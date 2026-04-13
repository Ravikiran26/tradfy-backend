import os
import hmac
import hashlib
import razorpay
from fastapi import APIRouter, Header, HTTPException
from pydantic import BaseModel
from services.supabase_client import get_db

router = APIRouter(prefix="/payments", tags=["payments"])

RAZORPAY_KEY_ID     = os.getenv("RAZORPAY_KEY_ID", "")
RAZORPAY_KEY_SECRET = os.getenv("RAZORPAY_KEY_SECRET", "")

# Pricing in paise (₹299 = 29900 paise)
EARLY_BIRD_MONTHLY_PAISE = 29900   # ₹299/mo
EARLY_BIRD_YEARLY_PAISE  = 249900  # ₹2,499/yr
REGULAR_MONTHLY_PAISE    = 49900   # ₹499/mo
REGULAR_YEARLY_PAISE     = 349900  # ₹3,499/yr

PLAN_AMOUNTS = {
    "monthly": EARLY_BIRD_MONTHLY_PAISE,
    "yearly":  EARLY_BIRD_YEARLY_PAISE,
}


def _rz_client():
    return razorpay.Client(auth=(RAZORPAY_KEY_ID, RAZORPAY_KEY_SECRET))


# ── POST /payments/create-order ───────────────────────────────────────────────

class CreateOrderRequest(BaseModel):
    plan: str  # "monthly" | "yearly"

@router.post("/create-order")
def create_order(
    body: CreateOrderRequest,
    x_user_id: str = Header(...),
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
                "user_id": x_user_id,
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
    x_user_id: str = Header(...),
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
    db = get_db()
    try:
        db.table("users").upsert({
            "id":         x_user_id,
            "is_pro":     True,
            "pro_plan":   body.plan,
            "pro_since":  "now()",
        }).execute()
    except Exception:
        # Fallback: try updating instead of upsert
        db.table("users").update({
            "is_pro":    True,
            "pro_plan":  body.plan,
            "pro_since": "now()",
        }).eq("id", x_user_id).execute()

    return {"success": True, "is_pro": True, "plan": body.plan}


# ── GET /payments/status ──────────────────────────────────────────────────────

@router.get("/status")
def payment_status(x_user_id: str = Header(...)):
    db = get_db()
    result = db.table("users").select("is_pro, pro_plan, pro_since").eq("id", x_user_id).execute()
    if result.data:
        row = result.data[0]
        return {
            "is_pro":    row.get("is_pro", False),
            "pro_plan":  row.get("pro_plan"),
            "pro_since": row.get("pro_since"),
        }
    return {"is_pro": False, "pro_plan": None, "pro_since": None}
