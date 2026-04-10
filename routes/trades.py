from fastapi import APIRouter, File, Form, UploadFile, HTTPException, Header
from typing import Optional
from datetime import datetime, timezone, date as date_cls

from services.market_data import get_market_context, get_swing_context, get_fundamentals
from models import (
    Trade, TradeStats, FeedbackResponse, MultiTradeResponse,
    TradeUpdate, ClosePositionRequest,
    OpenPosition, SwingPatterns, SectorStat,
    OptionsPatterns, TimeSlotStat,
)
from services.claude import (
    extract_trades_from_screenshot,
    extract_trade_from_screenshot,
    generate_trade_feedback,
    generate_entry_observation,
    generate_swing_feedback,
    generate_session_feedback,
    generate_trade_autopsy,
)
from services.supabase_client import (
    save_trade,
    bulk_save_trades,
    get_user_trades,
    get_trade_stats,
    get_user_trade_history,
    get_open_positions,
    get_trade_by_id,
    update_trade,
    close_position,
    get_sector_winrate,
    get_swing_patterns,
    get_options_patterns,
    get_expiry_stats,
    get_options_depth_stats,
    get_intraday_patterns,
    count_ai_analyses,
)

router = APIRouter(prefix="/trades", tags=["trades"])

ALLOWED_MEDIA_TYPES = {
    "image/jpeg": "image/jpeg",
    "image/jpg":  "image/jpeg",
    "image/png":  "image/png",
    "image/webp": "image/webp",
}
MAX_FILE_SIZE    = 10 * 1024 * 1024  # 10 MB
SWING_TYPES      = {"equity_swing", "futures_swing"}


# ── helpers ──────────────────────────────────────────────────────────────────

def _require_user(x_user_id: Optional[str]) -> str:
    if not x_user_id:
        raise HTTPException(status_code=401, detail="X-User-Id header required")
    return x_user_id


async def _read_image(file: UploadFile) -> tuple[bytes, str]:
    """Validate content-type and size; return (bytes, normalised media_type)."""
    content_type = file.content_type or ""
    if content_type not in ALLOWED_MEDIA_TYPES:
        raise HTTPException(
            status_code=415,
            detail=f"Unsupported file type '{content_type}'. Allowed: jpg, png, webp",
        )
    image_bytes = await file.read()
    if len(image_bytes) > MAX_FILE_SIZE:
        raise HTTPException(status_code=413, detail="File too large. Maximum size is 10 MB")
    return image_bytes, ALLOWED_MEDIA_TYPES[content_type]


def _days_since(created_at_str: str) -> int:
    dt = datetime.fromisoformat(created_at_str.replace("Z", "+00:00"))
    return (datetime.now(timezone.utc) - dt).days


def _calculate_pnl(open_trade: dict, exit_price: float) -> tuple[float, float]:
    """
    Compute realised P&L when closing a position.
    Returns (pnl_inr, pnl_percent).
    """
    entry    = float(open_trade.get("entry_price") or 0)
    qty      = int(open_trade.get("quantity") or 0)
    action   = (open_trade.get("action") or "buy").lower()
    overnight = float(open_trade.get("overnight_charges") or 0)

    if qty == 0 or entry == 0:
        return 0.0, 0.0

    if action == "buy":
        gross = (exit_price - entry) * qty
    else:                          # short position
        gross = (entry - exit_price) * qty

    pnl = round(gross - overnight, 2)
    pnl_pct = round((pnl / (entry * qty)) * 100, 4) if entry * qty else 0.0
    return pnl, pnl_pct


# ── POST /trades/upload ───────────────────────────────────────────────────────

@router.post("/upload")
async def upload_trade_screenshot(
    file: UploadFile = File(...),
    linked_trade_id: Optional[str] = Form(default=None),
    trade_type_override: Optional[str] = Form(default=None),
    x_user_id: Optional[str] = Header(default=None),
):
    """
    Accept a broker screenshot and:
    - Multi-trade P&L screen → extract all trades, bulk save, return session feedback
    - Intraday / options     → extract + full feedback + save
    - Swing OPEN             → extract + entry observation only + save (status=open)
    - Swing CLOSED + linked_trade_id → calculate P&L, holding_days, full swing feedback
    - Swing CLOSED + no linked_trade_id → save as standalone closed trade with full feedback
    trade_type_override: optional — if user explicitly selected the trade type on the frontend,
    use it instead of the AI-detected type (e.g. options_scalping, options_positional).
    """
    user_id = _require_user(x_user_id)
    image_bytes, media_type = await _read_image(file)

    # ── Extract all trades from screenshot ───────────────────────────────────
    try:
        all_trades = extract_trades_from_screenshot(image_bytes, media_type)
    except Exception as e:
        raise HTTPException(status_code=422, detail=f"Could not extract trade data: {str(e)}")

    if not all_trades:
        raise HTTPException(status_code=422, detail="No trade data found in the screenshot")

    # ── Multi-trade path: P&L summary screenshots ────────────────────────────
    if len(all_trades) > 1:
        try:
            feedback = generate_session_feedback(all_trades)
        except Exception:
            feedback = "Session logged. Feedback unavailable at this time."

        payloads = [{**t, "user_id": user_id, "ai_feedback": feedback} for t in all_trades]
        try:
            saved = bulk_save_trades(payloads)
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Failed to save trades: {str(e)}")

        return MultiTradeResponse(
            trades=[Trade(**t) for t in saved],
            feedback=feedback,
            count=len(saved),
        )

    # ── Single trade path (existing logic) ───────────────────────────────────
    trade_data = all_trades[0]
    # Apply user-selected trade type override (frontend knows better than AI for options sub-types)
    if trade_type_override:
        trade_data["trade_type"] = trade_type_override

    trade_type = trade_data.get("trade_type", "options_intraday")
    status     = trade_data.get("status", "closed")

    # ── Branch: swing open ───────────────────────────────────────────────────
    if trade_type in SWING_TYPES and status == "open":
        try:
            feedback = generate_entry_observation(trade_data)
        except Exception:
            feedback = "Entry logged. Feedback unavailable at this time."

        payload = {**trade_data, "user_id": user_id, "ai_feedback": feedback}
        try:
            saved = save_trade(payload)
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Failed to save trade: {str(e)}")

        return FeedbackResponse(trade=Trade(**saved), feedback=feedback)

    # ── Branch: swing closed + linked open position ───────────────────────────
    if trade_type in SWING_TYPES and status == "closed" and linked_trade_id:
        open_trade = get_trade_by_id(linked_trade_id, user_id)
        if not open_trade:
            raise HTTPException(
                status_code=404,
                detail="Linked open trade not found or does not belong to this user",
            )
        if open_trade.get("status") != "open":
            raise HTTPException(
                status_code=409,
                detail="Linked trade is not in open status",
            )

        exit_price = trade_data.get("exit_price") or trade_data.get("entry_price")
        if not exit_price:
            raise HTTPException(
                status_code=422,
                detail="Could not extract exit price from close screenshot",
            )

        sell_data = {
            "exit_price":        exit_price,
            "exit_date":         trade_data.get("trade_date"),
            "overnight_charges": trade_data.get("overnight_charges"),
        }
        try:
            updated = close_position(linked_trade_id, user_id, sell_data)
        except ValueError as e:
            raise HTTPException(status_code=409, detail=str(e))
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Failed to update open trade: {str(e)}")

        # Full swing feedback on the now-complete trade
        completed_trade_data = {**updated}
        try:
            user_history = get_user_trade_history(user_id)
            feedback = generate_swing_feedback(completed_trade_data, user_history)
        except Exception:
            feedback = "Feedback unavailable at this time."

        # Save the close screenshot as a linked record for audit trail
        close_payload = {
            **trade_data,
            "user_id":        user_id,
            "linked_trade_id": linked_trade_id,
            "status":          "closed",
            "pnl":             pnl,
            "pnl_percent":     pnl_pct,
            "holding_days":    holding_days,
            "closed_at":       now_iso,
            "ai_feedback":     feedback,
        }
        try:
            save_trade(close_payload)
        except Exception:
            pass  # audit record failure is non-fatal

        return FeedbackResponse(trade=Trade(**updated), feedback=feedback)

    # ── Branch: standalone closed trade (intraday or swing without link) ─────
    try:
        if trade_type in SWING_TYPES:
            user_history = get_user_trade_history(user_id)
            feedback = generate_swing_feedback(trade_data, user_history)
        else:
            # Fetch market context (VIX + Greeks) for options trades — non-blocking
            mkt_ctx = None
            try:
                mkt_ctx = get_market_context(
                    trade_data.get("symbol", ""),
                    trade_data.get("trade_date"),
                )
            except Exception:
                pass
            feedback = generate_trade_feedback(trade_data, market_context=mkt_ctx)
    except Exception:
        feedback = "Feedback unavailable at this time."

    payload = {**trade_data, "user_id": user_id, "ai_feedback": feedback}
    try:
        saved = save_trade(payload)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to save trade: {str(e)}")

    return FeedbackResponse(trade=Trade(**saved), feedback=feedback)


# ── POST /trades/{trade_id}/close-manual ─────────────────────────────────────

@router.post("/{trade_id}/close-manual", response_model=FeedbackResponse)
def close_trade_manual(
    trade_id: str,
    body: ClosePositionRequest,
    x_user_id: Optional[str] = Header(default=None),
):
    """
    Close an open position using a JSON body (no screenshot required).
    Useful when exit_price is already known (e.g. from order history).
    Calculates P&L and holding_days server-side, then generates swing feedback.
    """
    user_id = _require_user(x_user_id)

    sell_data = {
        "exit_price":        body.exit_price,
        "exit_date":         str(body.exit_date) if body.exit_date else None,
        "overnight_charges": body.overnight_charges,
    }
    try:
        updated = close_position(trade_id, user_id, sell_data)
    except ValueError as e:
        raise HTTPException(status_code=409, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to close trade: {str(e)}")

    try:
        user_history = get_user_trade_history(user_id)
        feedback = generate_swing_feedback(updated, user_history)
    except Exception:
        feedback = "Feedback unavailable at this time."

    updated["ai_feedback"] = feedback
    try:
        update_trade(trade_id, {"ai_feedback": feedback})
    except Exception:
        pass

    return FeedbackResponse(trade=Trade(**updated), feedback=feedback)


# ── GET /trades/open ──────────────────────────────────────────────────────────

@router.get("/open", response_model=list[OpenPosition])
def open_positions(x_user_id: Optional[str] = Header(default=None)):
    """Return all open swing positions with days_held calculated."""
    user_id = _require_user(x_user_id)
    try:
        rows = get_open_positions(user_id)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

    result = []
    for row in rows:
        days = _days_since(row["created_at"]) if row.get("created_at") else 0
        result.append(OpenPosition(trade=Trade(**row), days_held=days))
    return result


# ── POST /trades/{trade_id}/close ────────────────────────────────────────────

@router.post("/{trade_id}/close", response_model=FeedbackResponse)
async def close_trade_position(
    trade_id: str,
    file: UploadFile = File(...),
    x_user_id: Optional[str] = Header(default=None),
):
    """
    Accept a sell/exit screenshot for an open position.
    Matches to the open trade, calculates P&L + holding_days,
    updates the original record to closed, returns full swing feedback.
    """
    user_id = _require_user(x_user_id)

    open_trade = get_trade_by_id(trade_id, user_id)
    if not open_trade:
        raise HTTPException(status_code=404, detail="Trade not found")
    if open_trade.get("status") != "open":
        raise HTTPException(status_code=409, detail="Trade is not in open status")

    image_bytes, media_type = await _read_image(file)

    try:
        close_data = extract_trade_from_screenshot(image_bytes, media_type)
    except Exception as e:
        raise HTTPException(status_code=422, detail=f"Could not extract close data: {str(e)}")

    if not close_data:
        raise HTTPException(status_code=422, detail="No trade data found in close screenshot")

    exit_price = close_data.get("exit_price") or close_data.get("entry_price")
    if not exit_price:
        raise HTTPException(
            status_code=422,
            detail="Could not extract exit price from screenshot",
        )

    sell_data = {
        "exit_price":  exit_price,
        "exit_date":   close_data.get("trade_date"),
        "overnight_charges": close_data.get("overnight_charges"),
    }
    try:
        updated = close_position(trade_id, user_id, sell_data)
    except ValueError as e:
        raise HTTPException(status_code=409, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to close trade: {str(e)}")

    completed_trade_data = {**open_trade, **close_update}
    try:
        user_history = get_user_trade_history(user_id)
        feedback = generate_swing_feedback(completed_trade_data, user_history)
    except Exception:
        feedback = "Feedback unavailable at this time."

    updated["ai_feedback"] = feedback
    try:
        update_trade(trade_id, {"ai_feedback": feedback})
    except Exception:
        pass

    return FeedbackResponse(trade=Trade(**updated), feedback=feedback)


# ── GET /trades/patterns/swing ────────────────────────────────────────────────

@router.get("/patterns/swing", response_model=SwingPatterns)
def swing_patterns(x_user_id: Optional[str] = Header(default=None)):
    """
    Swing-specific pattern analysis:
    sector win rate, avg holding days winners vs losers,
    dead money positions (open > 2× avg winner hold time),
    panic sell count (closed < 2 days with a loss).
    """
    user_id = _require_user(x_user_id)
    try:
        data = get_swing_patterns(user_id)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

    sector_win_rate = {
        k: SectorStat(**v) for k, v in data["sector_win_rate"].items()
    }
    dead_money = [Trade(**r) for r in data["dead_money_positions"]]

    return SwingPatterns(
        sector_win_rate=sector_win_rate,
        avg_holding_days_winners=data["avg_holding_days_winners"],
        avg_holding_days_losers=data["avg_holding_days_losers"],
        dead_money_positions=dead_money,
        panic_sell_count=data["panic_sell_count"],
    )


# ── GET /trades/patterns/options ──────────────────────────────────────────────

@router.get("/patterns/options", response_model=OptionsPatterns)
def options_patterns(x_user_id: Optional[str] = Header(default=None)):
    """
    Options-intraday pattern analysis:
    expiry day (Thursday) win rate vs other days,
    time slot win rate by IST hour bucket (from upload time).
    """
    user_id = _require_user(x_user_id)
    try:
        data = get_options_patterns(user_id)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

    time_slot_wr = {
        k: TimeSlotStat(**v) for k, v in data["time_slot_win_rate"].items()
    }

    return OptionsPatterns(
        expiry_day_win_rate=data["expiry_day_win_rate"],
        non_expiry_win_rate=data["non_expiry_win_rate"],
        expiry_day_trades=data["expiry_day_trades"],
        time_slot_win_rate=time_slot_wr,
        total_options_trades=data["total_options_trades"],
    )


# ── GET /trades/patterns/expiry ──────────────────────────────────────────────

@router.get("/patterns/expiry")
def expiry_patterns(x_user_id: Optional[str] = Header(default=None)):
    """
    Expiry day intelligence:
    - Win rate + avg P&L for each day of week (Mon–Fri)
    - Thursday broken down by week-of-month (1st–4th)
    """
    user_id = _require_user(x_user_id)
    try:
        data = get_expiry_stats(user_id)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    return data


# ── GET /trades/patterns/intraday ────────────────────────────────────────────

@router.get("/patterns/intraday")
def intraday_patterns(x_user_id: Optional[str] = Header(default=None)):
    """Overtrading, revenge trading, and best underlying for intraday options traders."""
    user_id = _require_user(x_user_id)
    try:
        data = get_intraday_patterns(user_id)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    return data


# ── GET /trades/patterns/options-depth ───────────────────────────────────────

@router.get("/patterns/options-depth")
def options_depth_patterns(x_user_id: Optional[str] = Header(default=None)):
    """Strike selection (OTM/ATM/ITM) and hold-time patterns for options trades."""
    user_id = _require_user(x_user_id)
    try:
        data = get_options_depth_stats(user_id)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    return data


# ── GET /trades/patterns/insights ────────────────────────────────────────────

@router.get("/patterns/insights")
def pattern_insights(x_user_id: Optional[str] = Header(default=None)):
    """
    Compute AI-style pattern insights from all the user's closed trades.
    Returns up to 5 insight objects with title, body, severity, and icon.
    Requires at least 5 closed trades; returns empty list otherwise.
    """
    user_id = _require_user(x_user_id)
    trades = get_user_trades(user_id)
    closed = [t for t in trades if t.get("pnl") is not None and t.get("status") != "open"]

    if len(closed) < 5:
        return {"insights": [], "total_trades": len(closed), "ready": False}

    insights = []

    # ── 1. Avg win vs avg loss ──────────────────────────────────────────────
    wins   = [t["pnl"] for t in closed if (t.get("pnl") or 0) > 0]
    losses = [t["pnl"] for t in closed if (t.get("pnl") or 0) < 0]
    if wins and losses:
        avg_win  = sum(wins) / len(wins)
        avg_loss = abs(sum(losses) / len(losses))
        ratio    = avg_loss / avg_win if avg_win > 0 else 0
        if ratio > 1.3:
            insights.append({
                "type": "risk_reward",
                "icon": "⚖️",
                "severity": "warning",
                "title": "You exit winners too early",
                "body": (
                    f"Average win ₹{avg_win:,.0f} vs average loss ₹{avg_loss:,.0f} — "
                    f"your losses are {ratio:.1f}× bigger than your wins. "
                    "Let your winners run longer or tighten your stop-loss."
                ),
            })
        elif ratio < 0.75:
            insights.append({
                "type": "risk_reward",
                "icon": "✅",
                "severity": "positive",
                "title": "Excellent risk-reward discipline",
                "body": (
                    f"Average win ₹{avg_win:,.0f} vs average loss ₹{avg_loss:,.0f} — "
                    f"you cut losses well. Win rate just needs to stay above {100 / (1 + avg_win / avg_loss):.0f}% to be profitable."
                ),
            })

    # ── 2. Expiry day (options intraday) ────────────────────────────────────
    options_closed = [
        t for t in closed
        if t.get("trade_type") == "options_intraday" and t.get("trade_date")
    ]
    if options_closed:
        expiry_trades     = []
        non_expiry_trades = []
        for t in options_closed:
            try:
                d = date_cls.fromisoformat(str(t["trade_date"]))
                if d.weekday() == 3:          # Thursday
                    expiry_trades.append(t)
                else:
                    non_expiry_trades.append(t)
            except (ValueError, TypeError):
                pass

        if len(expiry_trades) >= 3:
            expiry_wr   = sum(1 for t in expiry_trades if (t.get("pnl") or 0) > 0) / len(expiry_trades) * 100
            expiry_avg  = sum(t.get("pnl") or 0 for t in expiry_trades) / len(expiry_trades)
            ne_wr_txt   = ""
            if len(non_expiry_trades) >= 2:
                ne_wr = sum(1 for t in non_expiry_trades if (t.get("pnl") or 0) > 0) / len(non_expiry_trades) * 100
                ne_wr_txt = f" Non-expiry days: {ne_wr:.0f}% win rate."
            if expiry_wr < 45:
                insights.append({
                    "type": "expiry_day",
                    "icon": "📅",
                    "severity": "warning",
                    "title": "Thursday expiry is costing you money",
                    "body": (
                        f"Expiry day win rate: {expiry_wr:.0f}% across {len(expiry_trades)} trades "
                        f"(avg ₹{expiry_avg:+,.0f} per trade).{ne_wr_txt} "
                        "Reduce position size or sit out expiry day entirely."
                    ),
                })
            elif expiry_wr >= 60:
                insights.append({
                    "type": "expiry_day",
                    "icon": "🎯",
                    "severity": "positive",
                    "title": "Expiry day is your edge",
                    "body": (
                        f"You win {expiry_wr:.0f}% on Thursday expiry ({len(expiry_trades)} trades, "
                        f"avg ₹{expiry_avg:+,.0f}).{ne_wr_txt} "
                        "Most traders lose on expiry day — you're doing the opposite."
                    ),
                })

    # ── 3. Sector edge (swing trades) ──────────────────────────────────────
    sector_map: dict = {}
    swing_closed = [
        t for t in closed
        if t.get("trade_type") in ("equity_swing", "futures_swing") and t.get("sector")
    ]
    for t in swing_closed:
        s = t["sector"]
        if s not in sector_map:
            sector_map[s] = {"wins": 0, "total": 0, "pnl": 0.0}
        sector_map[s]["total"] += 1
        sector_map[s]["pnl"] += t.get("pnl") or 0
        if (t.get("pnl") or 0) > 0:
            sector_map[s]["wins"] += 1

    valid_sectors = {s: v for s, v in sector_map.items() if v["total"] >= 3}
    if valid_sectors:
        overall_wr = sum(1 for t in closed if (t.get("pnl") or 0) > 0) / len(closed) * 100
        best  = max(valid_sectors.items(), key=lambda x: x[1]["wins"] / x[1]["total"])
        worst = min(valid_sectors.items(), key=lambda x: x[1]["wins"] / x[1]["total"])
        best_wr  = best[1]["wins"] / best[1]["total"] * 100
        worst_wr = worst[1]["wins"] / worst[1]["total"] * 100

        if best_wr > overall_wr + 15:
            insights.append({
                "type": "sector_edge",
                "icon": "🏢",
                "severity": "positive",
                "title": f"{best[0]} is your strongest sector",
                "body": (
                    f"{best[0]} trades win {best_wr:.0f}% vs your overall {overall_wr:.0f}% "
                    f"({best[1]['total']} trades, ₹{best[1]['pnl']:+,.0f} total P&L). "
                    "This is where your edge lives — allocate more capital here."
                ),
            })

        if worst[0] != best[0] and worst_wr < overall_wr - 20:
            insights.append({
                "type": "sector_avoid",
                "icon": "⚠️",
                "severity": "warning",
                "title": f"Stop trading {worst[0]}",
                "body": (
                    f"{worst[0]} trades win only {worst_wr:.0f}% "
                    f"({worst[1]['total']} trades, ₹{worst[1]['pnl']:+,.0f} total). "
                    "This sector is consistently draining your account."
                ),
            })

    # ── 4. Best time slot (options) ─────────────────────────────────────────
    try:
        opts_data = get_options_patterns(user_id)
        time_slots = opts_data.get("time_slot_win_rate", {})
        valid_slots = {k: v for k, v in time_slots.items() if v["total"] >= 3}
        if valid_slots:
            best_slot  = max(valid_slots.items(), key=lambda x: x[1]["win_rate"])
            worst_slot = min(valid_slots.items(), key=lambda x: x[1]["win_rate"])
            if best_slot[1]["win_rate"] >= 60:
                insights.append({
                    "type": "time_edge",
                    "icon": "⏰",
                    "severity": "positive",
                    "title": f"{best_slot[0]} IST is your best trading window",
                    "body": (
                        f"Trades between {best_slot[0]} IST win {best_slot[1]['win_rate']:.0f}% "
                        f"({best_slot[1]['total']} trades). "
                        + (
                            f"Contrast: {worst_slot[0]} IST only {worst_slot[1]['win_rate']:.0f}%. "
                            if worst_slot[0] != best_slot[0] and worst_slot[1]["total"] >= 3 else ""
                        )
                        + "Concentrate your trades in your best window."
                    ),
                })
    except Exception:
        pass

    # ── 5. Instrument type comparison ──────────────────────────────────────
    type_map: dict = {}
    for t in closed:
        tt = t.get("trade_type") or "options_intraday"
        if tt not in type_map:
            type_map[tt] = {"wins": 0, "total": 0}
        type_map[tt]["total"] += 1
        if (t.get("pnl") or 0) > 0:
            type_map[tt]["wins"] += 1

    labels = {
        "options_intraday": "Options intraday",
        "equity_swing":     "Equity swing",
        "futures_swing":    "Futures swing",
    }
    valid_types = {k: v for k, v in type_map.items() if v["total"] >= 3}
    if len(valid_types) >= 2:
        best_t  = max(valid_types.items(), key=lambda x: x[1]["wins"] / x[1]["total"])
        worst_t = min(valid_types.items(), key=lambda x: x[1]["wins"] / x[1]["total"])
        best_t_wr  = best_t[1]["wins"] / best_t[1]["total"] * 100
        worst_t_wr = worst_t[1]["wins"] / worst_t[1]["total"] * 100
        if best_t_wr - worst_t_wr > 20 and best_t[0] != worst_t[0]:
            insights.append({
                "type": "instrument_edge",
                "icon": "📊",
                "severity": "positive",
                "title": f"Focus on {labels.get(best_t[0], best_t[0])}",
                "body": (
                    f"{labels.get(best_t[0], best_t[0])}: {best_t_wr:.0f}% win rate vs "
                    f"{labels.get(worst_t[0], worst_t[0])}: {worst_t_wr:.0f}%. "
                    "Your results are significantly better in one instrument type — play your strengths."
                ),
            })

    return {
        "insights": insights[:5],
        "total_trades": len(closed),
        "ready": True,
    }


# ── GET /trades/{trade_id}/market-context ────────────────────────────────────

@router.get("/{trade_id}/market-context")
def trade_market_context(
    trade_id: str,
    x_user_id: Optional[str] = Header(default=None),
):
    """
    Return VIX, spot price, and Black-Scholes Greeks for an options trade
    at the time it was traded. Used by the frontend drawer for context display.
    Returns 404 if the trade is not an options instrument or data is unavailable.
    """
    user_id = _require_user(x_user_id)
    trade = get_trade_by_id(trade_id, user_id)
    if not trade:
        raise HTTPException(status_code=404, detail="Trade not found")

    symbol     = trade.get("symbol", "")
    trade_date = trade.get("trade_date")

    ctx = get_market_context(symbol, trade_date)
    if not ctx:
        raise HTTPException(
            status_code=404,
            detail="Market context unavailable — trade may not be an options instrument, or historical data not found.",
        )
    return ctx


# ── GET /trades/{trade_id}/swing-context ─────────────────────────────────────

@router.get("/{trade_id}/swing-context")
def trade_swing_context(
    trade_id: str,
    x_user_id: Optional[str] = Header(default=None),
):
    """
    Return price vs EMAs, 52-week range, trend, candlestick, VIX and NIFTY
    for a swing (equity/futures) trade at entry time.
    """
    user_id = _require_user(x_user_id)
    trade = get_trade_by_id(trade_id, user_id)
    if not trade:
        raise HTTPException(status_code=404, detail="Trade not found")

    instrument = trade.get("instrument_type", "")
    if instrument not in ("equity", "futures"):
        raise HTTPException(status_code=400, detail="Swing context only available for equity/futures trades.")

    symbol      = trade.get("symbol", "")
    trade_date  = trade.get("trade_date")
    entry_price = float(trade.get("entry_price") or 0)

    ctx = get_swing_context(symbol, trade_date, entry_price)
    if not ctx:
        raise HTTPException(
            status_code=404,
            detail="Swing context unavailable — historical data not found for this symbol/date.",
        )
    return ctx


# ── GET /trades/{trade_id}/fundamentals ──────────────────────────────────────

@router.get("/{trade_id}/fundamentals")
def trade_fundamentals(
    trade_id: str,
    x_user_id: Optional[str] = Header(default=None),
):
    """
    Return key fundamental data (P/E, P/B, EPS growth, ROE, D/E, market cap, etc.)
    for equity swing trades via yfinance.
    """
    user_id = _require_user(x_user_id)
    trade = get_trade_by_id(trade_id, user_id)
    if not trade:
        raise HTTPException(status_code=404, detail="Trade not found")

    if trade.get("instrument_type") not in ("equity", "futures"):
        raise HTTPException(status_code=400, detail="Fundamentals only available for equity/futures trades.")

    symbol = trade.get("symbol", "")
    data = get_fundamentals(symbol)
    if not data:
        raise HTTPException(status_code=404, detail=f"Fundamental data not available for {symbol}")
    return data


FREE_AI_LIMIT = 10  # free analyses per user


# ── GET /trades/usage ─────────────────────────────────────────────────────────

@router.get("/usage")
def get_usage(x_user_id: Optional[str] = Header(default=None)):
    """Return AI analysis usage count for the current user."""
    user_id = _require_user(x_user_id)
    used = count_ai_analyses(user_id)
    return {
        "ai_analyses_used":  used,
        "ai_analyses_limit": FREE_AI_LIMIT,
        "is_pro":            False,   # placeholder until payments are live
        "can_generate":      used < FREE_AI_LIMIT,
    }


# ── POST /trades/{trade_id}/generate-coaching ────────────────────────────────

@router.post("/{trade_id}/generate-coaching")
def generate_coaching(
    trade_id: str,
    x_user_id: Optional[str] = Header(default=None),
):
    """
    Generate AI coaching from stored trade data (no screenshot needed).
    Used for CSV-imported trades that have no ai_feedback yet.
    """
    user_id = _require_user(x_user_id)
    trade = get_trade_by_id(trade_id, user_id)
    if not trade:
        raise HTTPException(status_code=404, detail="Trade not found")

    # Gate: allow re-generating cached coaching for free; block new analyses over limit
    already_has_coaching = bool(trade.get("ai_feedback"))
    if not already_has_coaching:
        used = count_ai_analyses(user_id)
        if used >= FREE_AI_LIMIT:
            raise HTTPException(
                status_code=402,
                detail={
                    "code":    "UPGRADE_REQUIRED",
                    "message": f"You've used all {FREE_AI_LIMIT} free AI analyses. Upgrade to Pro for unlimited coaching.",
                    "used":    used,
                    "limit":   FREE_AI_LIMIT,
                }
            )

    trade_type = trade.get("trade_type", "options_intraday")
    market_ctx = None
    if trade.get("instrument_type") == "options":
        market_ctx = get_market_context(trade.get("symbol", ""), trade.get("trade_date"))

    try:
        if trade_type in SWING_TYPES:
            user_history = get_user_trade_history(user_id)
            swing_ctx = None
            fund_ctx = None
            try:
                from datetime import date as _date
                # For open positions, fetch live market data (today's price/EMAs)
                as_of = _date.today().isoformat() if trade.get("status") == "open" else None
                swing_ctx = get_swing_context(
                    trade.get("symbol", ""),
                    trade.get("trade_date"),
                    trade.get("entry_price"),
                    as_of_date=as_of,
                )
            except Exception:
                pass
            try:
                fund_ctx = get_fundamentals(trade.get("symbol", ""))
            except Exception:
                pass
            feedback = generate_swing_feedback(trade, user_history, swing_ctx, fund_ctx)
        else:
            feedback = generate_trade_feedback(trade, market_ctx)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Coaching generation failed: {str(e)}")

    # Persist to DB
    try:
        update_trade(trade_id, {"ai_feedback": feedback})
    except Exception:
        pass  # Return feedback even if save fails

    return {"feedback": feedback}


# ── GET /trades/{trade_id}/autopsy ───────────────────────────────────────────

@router.get("/{trade_id}/autopsy")
def trade_autopsy(
    trade_id: str,
    x_user_id: Optional[str] = Header(default=None),
):
    """
    Deep post-trade autopsy via Claude.
    Loss → entry failure, exit discipline, primary cause, risk lesson.
    Profit → trailing stop sim, exit timing, profit capture %, optimization tip.
    """
    user_id = _require_user(x_user_id)
    trade = get_trade_by_id(trade_id, user_id)
    if not trade:
        raise HTTPException(status_code=404, detail="Trade not found")

    if trade.get("status") == "open":
        raise HTTPException(status_code=400, detail="Autopsy only available for closed trades")

    market_ctx = None
    if trade.get("instrument_type") == "options":
        market_ctx = get_market_context(trade.get("symbol", ""), trade.get("trade_date"))

    try:
        analysis = generate_trade_autopsy(trade, market_ctx)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Autopsy failed: {str(e)}")

    return {"autopsy": analysis}


# ── GET /trades/my ────────────────────────────────────────────────────────────

@router.get("/my", response_model=list[Trade])
def my_trades(x_user_id: Optional[str] = Header(default=None)):
    """Return all trades for the authenticated user, newest first."""
    user_id = _require_user(x_user_id)
    try:
        trades = get_user_trades(user_id)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    return [Trade(**t) for t in trades]


# ── GET /trades/stats ─────────────────────────────────────────────────────────

@router.get("/stats", response_model=TradeStats)
def trade_stats(x_user_id: Optional[str] = Header(default=None)):
    """Return aggregated trading statistics for the authenticated user."""
    user_id = _require_user(x_user_id)
    try:
        stats = get_trade_stats(user_id)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    return TradeStats(**stats)
