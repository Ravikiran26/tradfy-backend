from fastapi import APIRouter, Header, HTTPException
from typing import Optional
from auth import _get_client
from services.email import send_welcome_email

router = APIRouter(prefix="/auth", tags=["auth"])


@router.post("/welcome")
async def trigger_welcome_email(authorization: Optional[str] = Header(default=None)):
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Authorization header required")

    token = authorization.removeprefix("Bearer ")
    try:
        client = _get_client()
        response = client.auth.get_user(token)
        if not response or not response.user:
            raise HTTPException(status_code=401, detail="Invalid token")
        user = response.user
        email = user.email or ""
        meta = user.user_metadata or {}
        name = meta.get("full_name") or meta.get("name") or ""
    except HTTPException:
        raise
    except Exception:
        raise HTTPException(status_code=401, detail="Invalid token")

    send_welcome_email(email, name)
    return {"ok": True}
