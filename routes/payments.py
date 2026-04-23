import os
import hmac
import hashlib
import razorpay
from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from services.supabase_client import get_client as get_db
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
    db = get_db()
    try:
        db.table("users").upsert({
            "id":         user_id,
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
        }).eq("id", user_id).execute()

    return {"success": True, "is_pro": True, "plan": body.plan}


# ── GET /payments/status ──────────────────────────────────────────────────────

@router.get("/status")
def payment_status(user_id: str = Depends(get_current_user)):
    db = get_db()
    result = db.table("users").select("is_pro, pro_plan, pro_since").eq("id", user_id).execute()
    if result.data:
        row = result.data[0]
        return {
            "is_pro":    row.get("is_pro", False),
            "pro_plan":  row.get("pro_plan"),
            "pro_since": row.get("pro_since"),
        }
    return {"is_pro": False, "pro_plan": None, "pro_since": None}
