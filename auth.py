import os
import jwt
from fastapi import HTTPException, Header
from typing import Optional

SUPABASE_JWT_SECRET = os.getenv("SUPABASE_JWT_SECRET", "")


def get_current_user(authorization: Optional[str] = Header(default=None)) -> str:
    """
    FastAPI dependency — validates the Supabase JWT from the Authorization header
    and returns the authenticated user's ID.

    Add to any route:
        user_id: str = Depends(get_current_user)
    """
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Authorization header required")

    if not SUPABASE_JWT_SECRET:
        raise RuntimeError(
            "SUPABASE_JWT_SECRET environment variable is not set. "
            "Find it in Supabase Dashboard → Project Settings → API → JWT Settings."
        )

    token = authorization.removeprefix("Bearer ")
    try:
        payload = jwt.decode(
            token,
            SUPABASE_JWT_SECRET,
            algorithms=["HS256"],
            audience="authenticated",
        )
        user_id = payload.get("sub")
        if not user_id:
            raise HTTPException(status_code=401, detail="Invalid token: missing user ID")
        return user_id
    except jwt.ExpiredSignatureError:
        raise HTTPException(status_code=401, detail="Token has expired")
    except jwt.InvalidTokenError:
        raise HTTPException(status_code=401, detail="Invalid token")
