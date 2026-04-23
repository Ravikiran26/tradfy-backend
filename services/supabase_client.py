import os
from datetime import date as date_cls, datetime, timezone
from supabase import create_client, Client
from typing import Optional

_supabase: Optional[Client] = None


def get_client() -> Client:
    global _supabase
    if _supabase is None:
        url = os.getenv("SUPABASE_URL")
        key = os.getenv("SUPABASE_SERVICE_KEY")
        if not url or not key:
            raise RuntimeError("SUPABASE_URL and SUPABASE_SERVICE_KEY must be set")
        _supabase = create_client(url, key)
    return _supabase


def save_trade(trade_dict: dict) -> dict:
    """Insert a trade row and return the created record."""
    db = get_client()
    # Remove None values so Supabase uses column defaults
    payload = {k: v for k, v in trade_dict.items() if v is not None}
    result = db.table("trades").insert(payload).execute()
    return result.data[0]


def bulk_save_trades(trade_list: list[dict]) -> list[dict]:
    """Insert multiple trades in a single request and return the created records."""
    if not trade_list:
        return []
    db = get_client()
    # Strip None values from each row
    payloads = [{k: v for k, v in t.items() if v is not None} for t in trade_list]
    result = db.table("trades").insert(payloads).execute()
    return result.data


def get_user_trades(user_id: str) -> list[dict]:
    """Fetch all trades for a user, newest first."""
    db = get_client()
    result = (
        db.table("trades")
        .select("*")
        .eq("user_id", user_id)
        .order("created_at", desc=True)
        .execute()
    )
    return result.data


def get_open_positions(user_id: str) -> list[dict]:
    """Return all open swing positions for a user, oldest first."""
    db = get_client()
    result = (
        db.table("trades")
        .select("*")
        .eq("user_id", user_id)
        .eq("status", "open")
        .order("created_at", desc=False)
        .execute()
    )
    return result.data


def get_trade_by_id(trade_id: str, user_id: str) -> Optional[dict]:
    """Fetch a single trade row, verifying it belongs to user_id."""
    db = get_client()
    result = (
        db.table("trades")
        .select("*")
        .eq("id", trade_id)
        .eq("user_id", user_id)
        .limit(1)
        .execute()
    )
    return result.data[0] if result.data else None


def update_trade(trade_id: str, update_dict: dict) -> dict:
    """Patch a trade row with the given fields and return the updated record."""
    db = get_client()
    payload = {k: v for k, v in update_dict.items() if v is not None}
    result = (
        db.table("trades")
        .update(payload)
        .eq("id", trade_id)
        .execute()
    )
    return result.data[0]


def close_position(trade_id: str, user_id: str, sell_data: dict) -> dict:
    """
    Close an open swing position.

    sell_data keys:
      exit_price        float  — required
      exit_date         str | date  — optional, defaults to today (IST)
      overnight_charges float  — optional, added to cost basis

    Computes:
      pnl          = (exit - entry) × qty  for BUY positions
                     (entry - exit) × qty  for SELL/short positions
                     minus overnight_charges
      pnl_percent  = pnl / (entry × qty) × 100
      holding_days = exit_date − trade_date  (if both dates available)

    Returns the updated trade record.
    Raises ValueError if trade is not found, not owned by user, or already closed.
    """
    open_trade = get_trade_by_id(trade_id, user_id)
    if not open_trade:
        raise ValueError(f"Trade {trade_id} not found for this user")
    if open_trade.get("status") != "open":
        raise ValueError(f"Trade {trade_id} is not open (status={open_trade.get('status')})")

    exit_price    = float(sell_data["exit_price"])
    entry_price   = float(open_trade.get("entry_price") or 0)
    qty           = int(open_trade.get("quantity") or 0)
    action        = (open_trade.get("action") or "buy").lower()
    oc            = float(sell_data.get("overnight_charges") or open_trade.get("overnight_charges") or 0)

    # P&L
    if qty > 0 and entry_price > 0:
        gross = (exit_price - entry_price) * qty if action == "buy" \
                else (entry_price - exit_price) * qty
        pnl        = round(gross - oc, 2)
        pnl_pct    = round(pnl / (entry_price * qty) * 100, 4)
    else:
        pnl, pnl_pct = 0.0, 0.0

    # holding_days from dates
    raw_exit_date  = sell_data.get("exit_date")
    raw_trade_date = open_trade.get("trade_date")
    holding_days: Optional[int] = None

    if raw_exit_date and raw_trade_date:
        exit_d  = date_cls.fromisoformat(str(raw_exit_date))
        entry_d = date_cls.fromisoformat(str(raw_trade_date))
        holding_days = (exit_d - entry_d).days
    elif raw_trade_date:
        # fall back to today minus trade_date
        today = datetime.now(timezone.utc).date()
        entry_d = date_cls.fromisoformat(str(raw_trade_date))
        holding_days = (today - entry_d).days

    exit_date_str = str(raw_exit_date) if raw_exit_date else str(datetime.now(timezone.utc).date())
    closed_at_iso = datetime.now(timezone.utc).isoformat()

    update_payload = {
        "status":       "closed",
        "exit_price":   exit_price,
        "pnl":          pnl,
        "pnl_percent":  pnl_pct,
        "closed_at":    closed_at_iso,
        "trade_date":   exit_date_str,   # overwrite with exit date so stats use close date
    }
    if holding_days is not None:
        update_payload["holding_days"] = holding_days

    return update_trade(trade_id, update_payload)


def get_sector_winrate(user_id: str) -> dict:
    """
    Return win rate per sector for all closed swing trades.

    Result shape:
      { "Banking": {"total": 5, "wins": 3, "win_rate": 60.0}, ... }
    """
    db = get_client()
    result = (
        db.table("trades")
        .select("sector, pnl")
        .eq("user_id", user_id)
        .eq("status", "closed")
        .in_("trade_type", ["equity_swing", "futures_swing"])
        .not_.is_("sector", "null")
        .not_.is_("pnl", "null")
        .execute()
    )

    stats: dict = {}
    for row in result.data:
        sector = row["sector"]
        if sector not in stats:
            stats[sector] = {"total": 0, "wins": 0, "win_rate": 0.0}
        stats[sector]["total"] += 1
        if row["pnl"] > 0:
            stats[sector]["wins"] += 1

    for s in stats:
        t = stats[s]["total"]
        stats[s]["win_rate"] = round(stats[s]["wins"] / t * 100, 1) if t else 0.0

    return stats


DEAD_MONEY_DAYS = 15  # open position held longer than this is flagged as dead money


def get_swing_patterns(user_id: str) -> dict:
    """
    Compute swing-specific patterns from closed and open swing trades.

    Returns:
      sector_win_rate          — per-sector {total, wins, win_rate}
      avg_holding_days_winners — avg holding_days for profitable closed trades
      avg_holding_days_losers  — avg holding_days for unprofitable closed trades
      dead_money_positions     — open trades held > DEAD_MONEY_DAYS (15 days)
      panic_sell_count         — closed trades with holding_days < 2 and pnl < 0
    """
    db = get_client()

    # All closed swing trades
    closed = (
        db.table("trades")
        .select("*")
        .eq("user_id", user_id)
        .eq("status", "closed")
        .in_("trade_type", ["equity_swing", "futures_swing"])
        .execute()
    ).data

    # All open swing trades
    open_pos = (
        db.table("trades")
        .select("*")
        .eq("user_id", user_id)
        .eq("status", "open")
        .in_("trade_type", ["equity_swing", "futures_swing"])
        .execute()
    ).data

    # Sector win rates — delegate to dedicated function
    sector_stats = get_sector_winrate(user_id)

    # Holding days stats (only trades with holding_days populated)
    winners = [r for r in closed if (r.get("pnl") or 0) > 0 and r.get("holding_days") is not None]
    losers  = [r for r in closed if (r.get("pnl") or 0) < 0 and r.get("holding_days") is not None]

    avg_hold_winners = (
        round(sum(r["holding_days"] for r in winners) / len(winners), 1) if winners else None
    )
    avg_hold_losers = (
        round(sum(r["holding_days"] for r in losers) / len(losers), 1) if losers else None
    )

    # Dead money: open positions held > DEAD_MONEY_DAYS days
    dead_money: list[dict] = []
    now = datetime.now(timezone.utc)
    for row in open_pos:
        created = row.get("created_at")
        if not created:
            continue
        dt = datetime.fromisoformat(created.replace("Z", "+00:00"))
        if (now - dt).days > DEAD_MONEY_DAYS:
            dead_money.append(row)

    # Panic sells: closed in < 2 days with a loss
    panic_count = sum(
        1 for r in closed
        if (r.get("holding_days") or 999) < 2 and (r.get("pnl") or 0) < 0
    )

    return {
        "sector_win_rate": sector_stats,
        "avg_holding_days_winners": avg_hold_winners,
        "avg_holding_days_losers": avg_hold_losers,
        "dead_money_positions": dead_money,
        "panic_sell_count": panic_count,
    }


def get_options_patterns(user_id: str) -> dict:
    """
    Compute options-intraday-specific patterns.

    Returns:
      expiry_day_win_rate    — win rate when trade_date is a Thursday (NSE weekly expiry)
      non_expiry_win_rate
      expiry_day_trades      — count
      time_slot_win_rate     — win rate by hour bucket (from created_at)
      total_options_trades
    """
    db = get_client()
    result = (
        db.table("trades")
        .select("pnl, trade_date, trade_time, created_at")
        .eq("user_id", user_id)
        .eq("trade_type", "options_intraday")
        .not_.is_("pnl", "null")
        .execute()
    )
    rows = result.data

    if not rows:
        return {
            "expiry_day_win_rate": None,
            "non_expiry_win_rate": None,
            "expiry_day_trades": 0,
            "time_slot_win_rate": {},
            "total_options_trades": 0,
        }

    # Expiry day = Thursday (weekday 3)
    expiry, non_expiry = [], []
    for row in rows:
        td = row.get("trade_date")
        if td:
            d = date_cls.fromisoformat(td) if isinstance(td, str) else td
            if d.weekday() == 3:  # Thursday
                expiry.append(row)
            else:
                non_expiry.append(row)

    def _wr(lst: list) -> Optional[float]:
        if not lst:
            return None
        wins = sum(1 for r in lst if (r.get("pnl") or 0) > 0)
        return round(wins / len(lst) * 100, 1)

    # Time slots from created_at hour (proxy for upload time)
    TIME_SLOTS = [
        ("09-10", 9), ("10-11", 10), ("11-12", 11),
        ("12-13", 12), ("13-14", 13), ("14-15", 14), ("15+", 15),
    ]
    slot_buckets: dict = {label: [] for label, _ in TIME_SLOTS}

    import re as _re
    for row in rows:
        # Prefer trade_time (actual execution time from broker screenshot, already IST)
        tt = row.get("trade_time")  # "HH:MM" string or None
        if tt:
            try:
                ist_hour = int(tt.split(":")[0])
            except (ValueError, IndexError):
                ist_hour = None
        else:
            # Fall back to created_at (upload time), convert UTC → IST
            ca = row.get("created_at")
            if not ca:
                continue
            ca_clean = ca.replace("Z", "+00:00")
            ca_clean = _re.sub(r'\.(\d{1,5})([+-])', lambda m: f".{m.group(1).ljust(6,'0')}{m.group(2)}", ca_clean)
            dt = datetime.fromisoformat(ca_clean)
            total_minutes = dt.hour * 60 + dt.minute + 330  # UTC → IST (+5:30)
            ist_hour = (total_minutes // 60) % 24

        if ist_hour is None:
            continue
        matched = "15+"
        for label, start_h in TIME_SLOTS[:-1]:
            if ist_hour == start_h:
                matched = label
                break
        slot_buckets[matched].append(row)

    time_slot_wr: dict = {}
    for label, bucket in slot_buckets.items():
        if not bucket:
            continue
        wins = sum(1 for r in bucket if (r.get("pnl") or 0) > 0)
        time_slot_wr[label] = {
            "total": len(bucket),
            "wins": wins,
            "win_rate": round(wins / len(bucket) * 100, 1),
        }

    return {
        "expiry_day_win_rate": _wr(expiry),
        "non_expiry_win_rate": _wr(non_expiry),
        "expiry_day_trades": len(expiry),
        "time_slot_win_rate": time_slot_wr,
        "total_options_trades": len(rows),
    }


def get_user_trade_history(user_id: str) -> dict:
    """
    Build a swing-trade history summary for the AI coaching prompt.
    Returns: total_swing_trades, swing_win_rate, overnight_total, overnight_wins,
             sector_stats {sector: {total, wins}}, avg_winner_pnl, avg_loser_pnl
    """
    db = get_client()
    result = (
        db.table("trades")
        .select("pnl, trade_type, status, sector")
        .eq("user_id", user_id)
        .in_("trade_type", ["equity_swing", "futures_swing"])
        .execute()
    )

    rows = result.data
    if not rows:
        return {}

    total_swing = len(rows)
    wins = [r for r in rows if (r.get("pnl") or 0) > 0]
    losses = [r for r in rows if (r.get("pnl") or 0) < 0]
    swing_win_rate = round((len(wins) / total_swing) * 100, 1) if total_swing else 0.0

    # Overnight = futures_swing trades (they carry overnight risk by definition)
    overnight_rows = [r for r in rows if r.get("trade_type") == "futures_swing"]
    overnight_wins = len([r for r in overnight_rows if (r.get("pnl") or 0) > 0])

    # Sector win rates
    sector_stats: dict = {}
    for row in rows:
        sector = row.get("sector")
        if not sector:
            continue
        if sector not in sector_stats:
            sector_stats[sector] = {"total": 0, "wins": 0}
        sector_stats[sector]["total"] += 1
        if (row.get("pnl") or 0) > 0:
            sector_stats[sector]["wins"] += 1

    avg_winner_pnl = round(sum(r["pnl"] for r in wins) / len(wins), 2) if wins else None
    avg_loser_pnl = round(sum(r["pnl"] for r in losses) / len(losses), 2) if losses else None

    return {
        "total_swing_trades": total_swing,
        "swing_win_rate": swing_win_rate,
        "overnight_total": len(overnight_rows),
        "overnight_wins": overnight_wins,
        "sector_stats": sector_stats,
        "avg_winner_pnl": avg_winner_pnl,
        "avg_loser_pnl": avg_loser_pnl,
    }


def count_ai_analyses(user_id: str) -> int:
    """Count trades that already have AI feedback (= coaching analyses used)."""
    db = get_client()
    result = (
        db.table("trades")
        .select("id", count="exact")
        .eq("user_id", user_id)
        .not_.is_("ai_feedback", "null")
        .execute()
    )
    return result.count or 0


def is_user_pro(user_id: str) -> bool:
    """Return True if the user has an active Pro subscription."""
    try:
        db = get_client()
        result = (
            db.table("users")
            .select("is_pro")
            .eq("id", user_id)
            .single()
            .execute()
        )
        return bool(result.data and result.data.get("is_pro"))
    except Exception:
        return False


def get_expiry_stats(user_id: str) -> dict:
    """
    Compute expiry intelligence for options intraday trades:
    - Win rate + avg P&L by day of week (Mon–Fri)
    - Thursday broken down by week-of-month (1st/2nd/3rd/4th)
    """
    db = get_client()
    result = (
        db.table("trades")
        .select("pnl, trade_date")
        .eq("user_id", user_id)
        .eq("trade_type", "options_intraday")
        .not_.is_("pnl", "null")
        .not_.is_("trade_date", "null")
        .execute()
    )
    rows = result.data
    if not rows:
        return {"day_stats": {}, "thursday_by_week": {}, "total_options_trades": 0}

    DAY_NAMES = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
    day_buckets: dict = {d: [] for d in DAY_NAMES[:5]}
    thu_week_buckets: dict = {"Week 1": [], "Week 2": [], "Week 3": [], "Week 4": []}

    for row in rows:
        try:
            d = date_cls.fromisoformat(str(row["trade_date"]))
        except (ValueError, TypeError):
            continue
        day_name = DAY_NAMES[d.weekday()]
        if day_name not in day_buckets:
            continue
        day_buckets[day_name].append(row["pnl"])

        # Thursday → week of month
        if d.weekday() == 3:
            # Find which Thursday of the month this is
            week_num = (d.day - 1) // 7 + 1
            key = f"Week {min(week_num, 4)}"
            thu_week_buckets[key].append(row["pnl"])

    def _summarise(pnls: list) -> dict:
        if not pnls:
            return None  # type: ignore[return-value]
        wins = sum(1 for p in pnls if p > 0)
        total_pnl = round(sum(pnls), 0)
        avg_pnl   = round(total_pnl / len(pnls), 0)
        return {
            "wins":      wins,
            "total":     len(pnls),
            "win_rate":  round(wins / len(pnls) * 100, 1),
            "avg_pnl":   avg_pnl,
            "total_pnl": total_pnl,
        }

    day_stats = {k: _summarise(v) for k, v in day_buckets.items() if v}
    thu_by_week = {k: _summarise(v) for k, v in thu_week_buckets.items() if v}

    # Best and worst day (by avg_pnl)
    best_day  = max(day_stats.items(), key=lambda x: x[1]["avg_pnl"])  if day_stats else None
    worst_day = min(day_stats.items(), key=lambda x: x[1]["avg_pnl"])  if day_stats else None

    return {
        "day_stats":              day_stats,
        "thursday_by_week":       thu_by_week,
        "total_options_trades":   len(rows),
        "best_day":               best_day[0]  if best_day  else None,
        "worst_day":              worst_day[0] if worst_day else None,
    }


def get_intraday_patterns(user_id: str) -> dict:
    """
    Compute intraday-specific behavioural patterns:
    1. Overtrading  — P&L and win rate by trades-per-day bucket (1-3 / 4-6 / 7+)
    2. Revenge trading — after a loss, did the next trade on the same day also lose?
    3. Best underlying — NIFTY vs BANKNIFTY vs SENSEX win rate and avg P&L
    """
    import re as _re

    db = get_client()
    result = (
        db.table("trades")
        .select("pnl, trade_date, created_at, symbol")
        .eq("user_id", user_id)
        .eq("trade_type", "options_intraday")
        .not_.is_("pnl", "null")
        .not_.is_("trade_date", "null")
        .order("created_at", desc=False)
        .execute()
    )
    rows = result.data
    if not rows:
        return {
            "overtrading": {},
            "revenge_trading": None,
            "best_underlying": {},
            "total_intraday_trades": 0,
        }

    # ── 1. Overtrading ────────────────────────────────────────────────────
    # Group by trade_date → compute bucket stats
    day_trades: dict = {}
    for row in rows:
        td  = str(row.get("trade_date", ""))
        pnl = row.get("pnl") or 0
        if not td:
            continue
        if td not in day_trades:
            day_trades[td] = []
        day_trades[td].append(pnl)

    BUCKETS = [("1–3 trades", 1, 3), ("4–6 trades", 4, 6), ("7+ trades", 7, 9999)]
    over_buckets: dict = {label: [] for label, _, _ in BUCKETS}

    for td, pnls in day_trades.items():
        n = len(pnls)
        for label, lo, hi in BUCKETS:
            if lo <= n <= hi:
                over_buckets[label].extend(pnls)
                break

    def _sum(pnls: list) -> Optional[dict]:
        if not pnls:
            return None
        wins      = sum(1 for p in pnls if p > 0)
        total_pnl = round(sum(pnls), 0)
        # avg per DAY for overtrading (not per trade)
        return {
            "wins":      wins,
            "total":     len(pnls),
            "win_rate":  round(wins / len(pnls) * 100, 1),
            "avg_pnl":   round(total_pnl / len(pnls), 0),
            "total_pnl": total_pnl,
        }

    overtrading = {k: _sum(v) for k, v in over_buckets.items() if v}

    # Most profitable bucket (by avg_pnl)
    best_bucket = max(overtrading.items(), key=lambda x: x[1]["avg_pnl"]) if overtrading else None
    worst_bucket = min(overtrading.items(), key=lambda x: x[1]["avg_pnl"]) if overtrading else None

    # ── 2. Revenge Trading ────────────────────────────────────────────────
    # Within each day (sorted by created_at), find trades immediately after a loss.
    # "Revenge trade" = trade placed after a loss on the same day.
    revenge_trades_pnl   = []   # P&L of the revenge trade itself
    non_revenge_pnl      = []   # all other trades for comparison
    revenge_count        = 0
    total_post_loss      = 0

    for td, pnls_in_day in day_trades.items():
        # pnls already in created_at order
        for i in range(1, len(pnls_in_day)):
            prev_pnl = pnls_in_day[i - 1]
            curr_pnl = pnls_in_day[i]
            if prev_pnl < 0:   # previous trade was a loss
                total_post_loss += 1
                revenge_trades_pnl.append(curr_pnl)
                if curr_pnl < 0:
                    revenge_count += 1
            else:
                non_revenge_pnl.append(curr_pnl)

    revenge_result = None
    if total_post_loss >= 3:
        revenge_loss_rate = round(revenge_count / total_post_loss * 100, 1)
        avg_revenge_pnl   = round(sum(revenge_trades_pnl) / len(revenge_trades_pnl), 0) if revenge_trades_pnl else 0
        total_revenge_damage = round(sum(p for p in revenge_trades_pnl if p < 0), 0)
        revenge_result = {
            "total_post_loss_trades":  total_post_loss,
            "loss_count":              revenge_count,
            "loss_rate":               revenge_loss_rate,
            "avg_pnl":                 avg_revenge_pnl,
            "total_damage":            total_revenge_damage,
            "is_problem":              revenge_loss_rate > 55,
        }

    # ── 3. Best Underlying ────────────────────────────────────────────────
    UNDERLYING_RE = _re.compile(
        r"(BANKNIFTY|FINNIFTY|MIDCPNIFTY|SENSEX|NIFTY|BSX)",
        _re.IGNORECASE,
    )
    UNDERLYING_LABELS = {
        "NIFTY":      "NIFTY 50",
        "BANKNIFTY":  "Bank Nifty",
        "FINNIFTY":   "Fin Nifty",
        "SENSEX":     "SENSEX",
        "BSX":        "SENSEX",
        "MIDCPNIFTY": "Midcap Nifty",
    }
    underlying_buckets: dict = {}

    for row in rows:
        symbol = (row.get("symbol") or "").upper()
        pnl    = row.get("pnl") or 0
        m = UNDERLYING_RE.search(symbol)
        if not m:
            continue
        key = m.group(1).upper()
        if key == "BSX":
            key = "SENSEX"
        if key not in underlying_buckets:
            underlying_buckets[key] = []
        underlying_buckets[key].append(pnl)

    best_underlying = {k: _sum(v) for k, v in underlying_buckets.items() if v and len(v) >= 2}

    return {
        "overtrading":            overtrading,
        "best_bucket":            best_bucket[0]  if best_bucket  else None,
        "worst_bucket":           worst_bucket[0] if worst_bucket else None,
        "revenge_trading":        revenge_result,
        "best_underlying":        best_underlying,
        "total_intraday_trades":  len(rows),
        "total_trading_days":     len(day_trades),
    }


def get_options_depth_stats(user_id: str) -> dict:
    """
    Compute strike type (OTM/ATM/ITM) and hold-time patterns for options trades.

    Strike classification: fetch actual underlying spot price on trade_date via
    yfinance (parallel, deduplicated by underlying+date). Compare strike vs spot
    to classify accurately:
      CE: ITM if strike < spot, OTM if strike > spot
      PE: ITM if strike > spot, OTM if strike < spot
      ATM band: ±0.5% of spot

    Hold time: upload time (created_at) vs market open (9:15 IST) as entry proxy.
    Buckets: <30min | 30-60min | 1-2hr | 2-3hr | 3hr+
    """
    import re as _re
    from concurrent.futures import ThreadPoolExecutor, as_completed

    db = get_client()
    result = (
        db.table("trades")
        .select("pnl, symbol, trade_date, created_at, entry_price, exit_price")
        .eq("user_id", user_id)
        .eq("trade_type", "options_intraday")
        .not_.is_("pnl", "null")
        .execute()
    )
    rows = result.data
    if not rows:
        return {
            "strike_stats": {},
            "hold_time_stats": {},
            "total_options_trades": 0,
        }

    # ── Parse symbols & collect unique (ticker, date) pairs to fetch ──────
    from services.market_data import parse_option_symbol, _TICKER_MAP, _yf_close

    SYMBOL_RE = _re.compile(r"^([A-Z&]+)\s+(\d+)\s+(CE|PE)$", _re.IGNORECASE)

    parsed_rows = []   # (pnl, underlying, strike, opt_type, trade_date, created_at)
    fetch_keys  = set()   # (yf_ticker, date_str)

    for row in rows:
        symbol = (row.get("symbol") or "").strip().upper()
        pnl    = row.get("pnl") or 0
        td     = row.get("trade_date")
        ca     = row.get("created_at")

        underlying, strike, opt_type = parse_option_symbol(symbol)
        if not underlying or not strike or not opt_type or not td:
            parsed_rows.append((pnl, None, None, None, td, ca))
            continue

        yf_ticker = _TICKER_MAP.get(underlying)
        if not yf_ticker:
            # Single-stock option — use underlying.NS
            yf_ticker = f"{underlying}.NS"

        fetch_keys.add((yf_ticker, str(td)))
        parsed_rows.append((pnl, underlying, strike, opt_type, td, ca))

    # ── Parallel spot price fetch (one call per unique ticker+date) ────────
    spot_cache: dict = {}   # (yf_ticker, date_str) → spot_price | None

    def _fetch(ticker: str, date_str: str):
        from datetime import datetime as _dt
        try:
            trade_dt = _dt.strptime(date_str, "%Y-%m-%d")
            price    = _yf_close(ticker, trade_dt)
            return (ticker, date_str), price
        except Exception:
            return (ticker, date_str), None

    if fetch_keys:
        with ThreadPoolExecutor(max_workers=8) as pool:
            futures = {pool.submit(_fetch, t, d): (t, d) for t, d in fetch_keys}
            for fut in as_completed(futures):
                key, price = fut.result()
                spot_cache[key] = price

    # ── Classify each trade ───────────────────────────────────────────────
    strike_buckets: dict = {"OTM": [], "ATM": [], "ITM": []}

    for pnl, underlying, strike, opt_type, td, ca in parsed_rows:
        if underlying is None:
            continue

        yf_ticker = _TICKER_MAP.get(underlying, f"{underlying}.NS")
        spot      = spot_cache.get((yf_ticker, str(td)))

        if spot is None or spot <= 0:
            # Fallback: ATM band based on entry premium thresholds
            continue

        atm_band = spot * 0.005   # ±0.5% of spot = ATM zone

        if opt_type == "CE":
            if strike < spot - atm_band:
                strike_buckets["ITM"].append(pnl)
            elif strike > spot + atm_band:
                strike_buckets["OTM"].append(pnl)
            else:
                strike_buckets["ATM"].append(pnl)
        else:  # PE
            if strike > spot + atm_band:
                strike_buckets["ITM"].append(pnl)
            elif strike < spot - atm_band:
                strike_buckets["OTM"].append(pnl)
            else:
                strike_buckets["ATM"].append(pnl)

    def _summarise(pnls: list) -> Optional[dict]:
        if not pnls:
            return None
        wins      = sum(1 for p in pnls if p > 0)
        total_pnl = round(sum(pnls), 0)
        avg_pnl   = round(total_pnl / len(pnls), 0)
        return {
            "wins":      wins,
            "total":     len(pnls),
            "win_rate":  round(wins / len(pnls) * 100, 1),
            "avg_pnl":   avg_pnl,
            "total_pnl": total_pnl,
        }

    strike_stats = {k: _summarise(v) for k, v in strike_buckets.items() if v}

    # ── Hold time analysis ────────────────────────────────────────────────
    # We don't have a trade_time column, so we use:
    #   entry proxy  = market open 9:15 IST on trade_date
    #   exit proxy   = created_at (upload time, typically after exit)
    # This gives an upper-bound hold duration bucket.
    HOLD_BUCKETS = [
        ("<30 min",   0,   30),
        ("30–60 min", 30,  60),
        ("1–2 hrs",   60,  120),
        ("2–3 hrs",   120, 180),
        ("3 hrs+",    180, 9999),
    ]
    hold_buckets: dict = {label: [] for label, _, _ in HOLD_BUCKETS}

    MARKET_OPEN_IST = 9 * 60 + 15   # 9:15 AM in minutes

    for row in rows:
        ca  = row.get("created_at")
        td  = row.get("trade_date")
        pnl = row.get("pnl") or 0
        if not ca or not td:
            continue
        try:
            ca_clean = ca.replace("Z", "+00:00")
            ca_clean = _re.sub(
                r'\.(\d{1,5})([+-])',
                lambda mx: f".{mx.group(1).ljust(6,'0')}{mx.group(2)}",
                ca_clean,
            )
            dt = datetime.fromisoformat(ca_clean)
            # Convert UTC → IST (+5:30 = 330 min)
            upload_ist = (dt.hour * 60 + dt.minute) + 330
            upload_ist_norm = upload_ist % (24 * 60)

            # If uploaded after market close (15:30+) or next day, skip — hold time unreliable
            if upload_ist_norm > 16 * 60 or upload_ist_norm < 9 * 60:
                continue

            hold_min = upload_ist_norm - MARKET_OPEN_IST
            if hold_min < 0 or hold_min > 400:
                continue

            for label, lo, hi in HOLD_BUCKETS:
                if lo <= hold_min < hi:
                    hold_buckets[label].append(pnl)
                    break
        except (ValueError, TypeError, AttributeError):
            continue

    hold_time_stats = {k: _summarise(v) for k, v in hold_buckets.items() if v}

    return {
        "strike_stats":         strike_stats,
        "hold_time_stats":      hold_time_stats,
        "total_options_trades": len(rows),
    }


def get_trade_stats(user_id: str) -> dict:
    """
    Compute win_rate, total_pnl, avg_profit, avg_loss, total_trades
    from the trades table for a given user.
    """
    db = get_client()
    result = (
        db.table("trades")
        .select("pnl")
        .eq("user_id", user_id)
        .not_.is_("pnl", "null")
        .execute()
    )

    trades = result.data
    total_trades = len(trades)

    if total_trades == 0:
        return {
            "total_trades": 0,
            "win_rate": 0.0,
            "total_pnl": 0.0,
            "avg_profit": 0.0,
            "avg_loss": 0.0,
        }

    pnls = [t["pnl"] for t in trades]
    profits = [p for p in pnls if p > 0]
    losses = [p for p in pnls if p < 0]

    win_rate = round((len(profits) / total_trades) * 100, 2)
    total_pnl = round(sum(pnls), 2)
    avg_profit = round(sum(profits) / len(profits), 2) if profits else 0.0
    avg_loss = round(sum(losses) / len(losses), 2) if losses else 0.0

    return {
        "total_trades": total_trades,
        "win_rate": win_rate,
        "total_pnl": total_pnl,
        "avg_profit": avg_profit,
        "avg_loss": avg_loss,
    }
