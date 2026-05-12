from fastapi import APIRouter, Depends, File, Form, HTTPException, Request, UploadFile
from typing import Optional

from services.trade_parser import parse_broker_file, _best_header_read
from services.claude import diagnose_empty_file
from services.supabase_client import bulk_save_trades
from models import Trade
from auth import get_current_user
from rate_limit import limiter

router = APIRouter(prefix="/trades", tags=["trades"])

SUPPORTED_BROKERS = {"zerodha", "upstox", "groww", "dhan", "angelone", "auto"}
MAX_FILE_SIZE = 10 * 1024 * 1024  # 10 MB


@router.post("/import")
@limiter.limit("10/minute")
async def import_trades(
    request: Request,
    file: UploadFile = File(...),
    broker: str = Form(...),
    user_id: str = Depends(get_current_user),
):
    """
    Import trades from a broker CSV/Excel export.

    Supported brokers: zerodha, upstox, groww, dhan, angelone, auto
    Supported file formats: .csv, .xlsx, .xls

    The endpoint:
    - Parses the file using the broker-specific column mapping
    - FIFO-matches BUY/SELL legs into closed trades (for tradebook formats)
    - Unmatched BUY legs are saved as open positions
    - Bulk-inserts all parsed trades for the authenticated user

    Returns the count of imported trades and their IDs.
    """

    broker_lower = broker.strip().lower()
    if broker_lower not in SUPPORTED_BROKERS:
        raise HTTPException(
            status_code=400,
            detail=f"Unsupported broker '{broker}'. Supported: {', '.join(sorted(SUPPORTED_BROKERS))}",
        )

    filename = file.filename or ""
    ext = filename.rsplit(".", 1)[-1].lower() if "." in filename else ""
    if ext not in ("csv", "xlsx", "xls"):
        raise HTTPException(
            status_code=415,
            detail="Unsupported file type. Upload a .csv or .xlsx file.",
        )

    file_bytes = await file.read()
    if len(file_bytes) > MAX_FILE_SIZE:
        raise HTTPException(status_code=413, detail="File too large. Maximum size is 10 MB.")

    try:
        trades = parse_broker_file(file_bytes, filename, broker_lower)
    except ValueError as e:
        raise HTTPException(status_code=422, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=422, detail=f"Failed to parse file: {str(e)}")

    if not trades:
        try:
            df = _best_header_read(file_bytes, filename)
            columns = [str(c) for c in df.columns.tolist()]
            cols_lower = " ".join(columns).lower()
            sample_rows = df.head(5).fillna("").astype(str).to_dict(orient="records")

            # Fast keyword-based diagnosis (no API call needed)
            if any(k in cols_lower for k in ("running balance", "voucher", "debit", "credit")):
                message = (
                    "This looks like a ledger or account statement, not a trade report. "
                    "Please download your Trade History or P&L Report from your broker's "
                    "Reports section (not the Ledger/Statement section)."
                )
            elif any(k in cols_lower for k in ("opening balance", "closing balance", "fund")):
                message = (
                    "This appears to be a funds/account summary, not a trade file. "
                    "Please upload your broker's P&L Statement or Trade Book instead."
                )
            else:
                # Ask Claude for a specific diagnosis
                message = diagnose_empty_file(columns, sample_rows)
        except Exception:
            message = "No trades found in this file. Please upload your broker's P&L or Trade History report."
        raise HTTPException(status_code=422, detail=message)

    # Attach user_id to every trade
    for trade in trades:
        trade["user_id"] = user_id

    try:
        saved = bulk_save_trades(trades)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to save trades: {str(e)}")

    return {
        "imported": len(saved),
        "open_positions": sum(1 for t in saved if t.get("status") == "open"),
        "closed_trades": sum(1 for t in saved if t.get("status") == "closed"),
        "broker": broker_lower.capitalize(),
        "trades": [Trade(**t) for t in saved],
    }
