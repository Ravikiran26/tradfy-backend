from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel, EmailStr
from services.supabase_client import get_client
from rate_limit import limiter

router = APIRouter(prefix="/waitlist", tags=["waitlist"])


class WaitlistRequest(BaseModel):
    email: EmailStr


@router.post("")
@limiter.limit("5/minute")
def join_waitlist(request: Request, body: WaitlistRequest):
    """
    Save an email to the waitlist table.
    Requires a `waitlist` table in Supabase:
        CREATE TABLE waitlist (
            id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
            email text NOT NULL,
            created_at timestamptz DEFAULT now()
        );
    """
    db = get_client()
    try:
        db.table("waitlist").insert({"email": body.email}).execute()
    except Exception as e:
        err = str(e).lower()
        # Silently succeed on duplicate emails — user is already on the list
        if "duplicate" in err or "unique" in err:
            return {"success": True}
        raise HTTPException(status_code=500, detail="Could not save email. Try again later.")
    return {"success": True}
