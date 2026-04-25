import os
import hmac
import hashlib
import razorpay
from datetime import datetime, timezone, timedelta
from fastapi import APIRouter, Depends, HTTPException, Request
from pydantic import BaseModel
from services.supabase_client import get_client as get_db
from services.email import send_payment_confirmation
from auth import get_current_user

router = APIRouter(prefix="/payments", tags=["payments"])

RAZORPAY_KEY_ID     = os.getenv("RAZORPAY_KEY_ID", "")
RAZORPAY_KEY_SECRET = os.getenv("RAZORPAY_KEY_SECRET", "")

# Pricing in paise
PRO_MONTHLY_PAISE = 49900   # ₹499/mo
PRO_YEARLY_PAISE  = 349900  # ₹3,499/yr

PLAN_AMOUNTS = {
    "monthly": PRO_MONTHLY_PAISE,
    "yearly":  PRO_YEARLY_PAISE,
}


def _rz_client():
    return razorpay.Client(auth=(RAZORPAY_KEY_ID, RAZORPAY_KEY_SECRET))


# ── POST /payments/create-order ───────────────────────────────────────────────

class CreateOrderRequest(BaseModel):
    plan: str  # "monthly" | "yearly"

@router.post("/create-order")
def create_order(
    body: CreateOrderRequest,
    user_id: str = Depends(get_current_user),
):
    if body.plan not in PLAN_AMOUNTS:
        raise HTTPException(status_code=400, detail="Invalid plan. Use 'monthly' or 'yearly'.")

    amount = PLAN_AMOUNTS[body.plan]

    try:
        client = _rz_client()
        order = client.order.create({
            "amount":   amount,
            "currency": "INR",
            "notes": {
                "user_id": user_id,
                "plan":    body.plan,
            },
        })
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Razorpay order creation failed: {e}")

    return {
        "order_id":  order["id"],
        "amount":    order["amount"],
        "currency":  order["currency"],
        "key_id":    RAZORPAY_KEY_ID,
    }


# ── POST /payments/verify ─────────────────────────────────────────────────────

class VerifyPaymentRequest(BaseModel):
    razorpay_order_id:   str
    razorpay_payment_id: str
    razorpay_signature:  str
    plan: str  # "monthly" | "yearly"

@router.post("/verify")
def verify_payment(
    body: VerifyPaymentRequest,
    user_id: str = Depends(get_current_user),
):
    # Verify signature
    message = f"{body.razorpay_order_id}|{body.razorpay_payment_id}"
    expected = hmac.new(
        RAZORPAY_KEY_SECRET.encode("utf-8"),
        message.encode("utf-8"),
        hashlib.sha256,
    ).hexdigest()

    if expected != body.razorpay_signature:
        raise HTTPException(status_code=400, detail="Invalid payment signature.")

    # Upgrade user to Pro in Supabase
    now = datetime.now(timezone.utc)
    expires = now + (timedelta(days=365) if body.plan == "yearly" else timedelta(days=30))

    db = get_db()
    try:
        db.table("users").upsert({
            "id":             user_id,
            "is_pro":         True,
            "pro_plan":       body.plan,
            "pro_since":      now.isoformat(),
            "pro_expires_at": expires.isoformat(),
        }).execute()
    except Exception:
        db.table("users").update({
            "is_pro":         True,
            "pro_plan":       body.plan,
            "pro_since":      now.isoformat(),
            "pro_expires_at": expires.isoformat(),
        }).eq("id", user_id).execute()

    # Send confirmation email
    try:
        auth_user = db.auth.admin.get_user_by_id(user_id)
        if auth_user and auth_user.user:
            u = auth_user.user
            send_payment_confirmation(
                email=u.email,
                name=u.user_metadata.get("full_name", "") if u.user_metadata else "",
                expires_at=expires.strftime("%d %b %Y"),
            )
    except Exception:
        pass

    return {"success": True, "is_pro": True, "plan": body.plan}


# ── POST /payments/webhook ───────────────────────────────────────────────────
# Razorpay calls this server-side on payment.captured — backup to /verify

@router.post("/webhook")
async def razorpay_webhook(request: Request):
    body_bytes = await request.body()
    sig = request.headers.get("X-Razorpay-Signature", "")
    webhook_secret = os.getenv("RAZORPAY_WEBHOOK_SECRET", "")

    if webhook_secret:
        expected = hmac.new(
            webhook_secret.encode("utf-8"),
            body_bytes,
            hashlib.sha256,
        ).hexdigest()
        if not hmac.compare_digest(expected, sig):
            raise HTTPException(status_code=400, detail="Invalid webhook signature")

    import json
    payload = json.loads(body_bytes)
    event = payload.get("event")

    if event == "payment.captured":
        payment = payload["payload"]["payment"]["entity"]
        notes   = payment.get("notes", {})
        user_id = notes.get("user_id")
        plan    = notes.get("plan", "monthly")

        if user_id:
            now     = datetime.now(timezone.utc)
            expires = now + (timedelta(days=365) if plan == "yearly" else timedelta(days=30))
            db = get_db()
            try:
                db.table("users").upsert({
                    "id":             user_id,
                    "is_pro":         True,
                    "pro_plan":       plan,
                    "pro_since":      now.isoformat(),
                    "pro_expires_at": expires.isoformat(),
                }).execute()
            except Exception:
                db.table("users").update({
                    "is_pro":         True,
                    "pro_plan":       plan,
                    "pro_since":      now.isoformat(),
                    "pro_expires_at": expires.isoformat(),
                }).eq("id", user_id).execute()

    return {"status": "ok"}


# ── GET /payments/status ──────────────────────────────────────────────────────

@router.get("/status")
def payment_status(user_id: str = Depends(get_current_user)):
    db = get_db()
    result = db.table("users").select("is_pro, pro_plan, pro_since, pro_expires_at").eq("id", user_id).execute()
    if result.data:
        row = result.data[0]
        return {
            "is_pro":         row.get("is_pro", False),
            "pro_plan":       row.get("pro_plan"),
            "pro_since":      row.get("pro_since"),
            "pro_expires_at": row.get("pro_expires_at"),
        }
    return {"is_pro": False, "pro_plan": None, "pro_since": None, "pro_expires_at": None}
