"""
Broker CSV/Excel import parsers for Zerodha, Upstox, Groww, Dhan, and Angel One.

Each parser returns a list of trade dicts ready for bulk DB insert.
Tradebook formats (individual buy/sell legs) are FIFO-matched into closed trades.
P&L statement formats (already matched) are mapped directly.
"""

import io
import re
import pandas as pd
from datetime import date, datetime
from collections import defaultdict
from typing import Optional, List


# ── File loading ──────────────────────────────────────────────────────────────

def _read_file_with_header_scan(file_bytes: bytes, filename: str) -> pd.DataFrame:
    """Try reading with header scan first (for files with metadata rows like Upstox P&L)."""
    header_row = _find_header_row(file_bytes, filename)
    if header_row is not None and header_row > 0:
        name = (filename or "").lower()
        if name.endswith(".xlsx") or name.endswith(".xls"):
            return pd.read_excel(io.BytesIO(file_bytes), skiprows=header_row)
    return _read_file(file_bytes, filename)


def _read_dhan_file(file_bytes: bytes, filename: str) -> pd.DataFrame:
    """
    For Dhan P&L reports: scan for the row containing 'Security Name'
    and use that as the header. Falls back to normal read for tradebook CSVs.
    """
    name = (filename or "").lower()
    if name.endswith(".xlsx") or name.endswith(".xls"):
        try:
            df_raw = pd.read_excel(io.BytesIO(file_bytes), header=None)
            for i, row in df_raw.iterrows():
                vals = [str(v).strip() for v in row if str(v).strip() not in ("", "nan")]
                if "Security Name" in vals:
                    return pd.read_excel(io.BytesIO(file_bytes), skiprows=i)
        except Exception:
            pass
    return _read_file(file_bytes, filename)


def _read_angelone_file(file_bytes: bytes, filename: str) -> pd.DataFrame:
    """
    Angel One P&L/Net-position reports have several metadata rows before the real header.
    Scan for a row that contains known Angel One column keywords and use it as the header.
    Falls back to plain read (for tradebook CSVs).
    """
    name = (filename or "").lower()
    if name.endswith(".xlsx") or name.endswith(".xls"):
        try:
            df_raw = pd.read_excel(io.BytesIO(file_bytes), header=None)
            for i, row in df_raw.iterrows():
                vals = [str(v).strip().lower() for v in row if str(v).strip() not in ("", "nan")]
                # Match Angel One P&L header row keywords
                if any(k in vals for k in ("net instrument", "scrip name", "symbol", "script name")):
                    if any(k in vals for k in ("buy qty", "sell qty", "net p&l", "p&l", "buy rate", "sell rate")):
                        return pd.read_excel(io.BytesIO(file_bytes), skiprows=i)
        except Exception:
            pass
    return _read_file(file_bytes, filename)


def parse_generic_csv(df: pd.DataFrame) -> list[dict]:
    """
    Fallback parser for unknown brokers.
    Sends column names + sample rows to Claude, gets back a field mapping,
    then normalizes every row into our standard trade dict.
    """
    from services.claude import infer_csv_mapping

    columns = [str(c) for c in df.columns.tolist()]
    sample_rows = df.head(3).fillna("").astype(str).to_dict(orient="records")

    mapping = infer_csv_mapping(columns, sample_rows)
    if not mapping:
        raise ValueError("Could not determine column mapping for this CSV. Please select your broker manually.")

    # Invert: standard_field -> original_col (skip nulls)
    field_to_col = {v: k for k, v in mapping.items() if v}

    def get(row, field):
        col = field_to_col.get(field)
        return row.get(col) if col else None

    def safe_float(val):
        try:
            return float(str(val).replace(",", "").replace("₹", "").strip())
        except (ValueError, TypeError):
            return None

    def parse_date(val) -> Optional[str]:
        if not val:
            return None
        for fmt in ("%d-%m-%Y", "%Y-%m-%d", "%d/%m/%Y", "%m/%d/%Y", "%d %b %Y", "%d-%b-%Y", "%Y/%m/%d"):
            try:
                return datetime.strptime(str(val).strip(), fmt).strftime("%Y-%m-%d")
            except ValueError:
                continue
        return None

    trades = []
    for _, row in df.iterrows():
        symbol = get(row, "symbol")
        if not symbol or str(symbol).strip() in ("", "nan", "None"):
            continue

        action_raw = str(get(row, "action") or "").lower().strip()
        action = "buy" if action_raw in ("buy", "b", "purchase") else "sell" if action_raw in ("sell", "s") else None

        pnl = safe_float(get(row, "pnl"))
        entry = safe_float(get(row, "entry_price"))
        exit_ = safe_float(get(row, "exit_price"))
        qty = safe_float(get(row, "quantity"))
        trade_date = parse_date(get(row, "trade_date"))

        # Infer instrument type from symbol
        # Options contracts always have a digit before CE/PE (e.g. NIFTY22600CE)
        sym_upper = str(symbol).upper()
        if re.search(r'\d(CE|PE)$', sym_upper):
            instrument_type = "options"
        elif "FUT" in sym_upper:
            instrument_type = "futures"
        else:
            instrument_type = "equity"

        trades.append({
            "symbol":          str(symbol).strip().upper(),
            "instrument_type": instrument_type,
            "action":          action,
            "quantity":        int(qty) if qty is not None else None,
            "entry_price":     entry,
            "exit_price":      exit_,
            "pnl":             pnl,
            "pnl_percent":     safe_float(get(row, "pnl_percent")),
            "trade_date":      trade_date,
            "trade_time":      str(get(row, "trade_time") or "").strip() or None,
            "status":          "closed" if pnl is not None else "open",
            "broker":          str(get(row, "broker") or "Unknown").strip(),
        })

    return trades


def parse_broker_file(file_bytes: bytes, filename: str, broker: str) -> list[dict]:
    """
    Main entry point.
    broker: 'zerodha' | 'upstox' | 'groww' | 'dhan' | 'angelone' | 'auto' (case-insensitive)
    Returns list of trade dicts ready for DB insert (no user_id yet).
    """
    df = _read_file(file_bytes, filename)

    broker_lower = broker.lower().strip()
    parsers = {
        "zerodha":   parse_zerodha,
        "upstox":    parse_upstox,
        "groww":     parse_groww,
        "dhan":      parse_dhan,
        "angelone":  parse_angelone,
    }

    if broker_lower not in parsers:
        # Try auto-detect from column names
        detected = _detect_broker(df)
        if detected:
            broker_lower = detected
        else:
            # Fall back to AI-powered generic parser
            return parse_generic_csv(df)

    # For Upstox, try P&L format first (files with metadata header rows)
    if broker_lower == "upstox":
        df_smart = _read_file_with_header_scan(file_bytes, filename)
        return parse_upstox(df_smart, file_bytes=file_bytes, filename=filename)

    # For Zerodha, try smart header scan first (handles P&L Excel with metadata rows)
    if broker_lower == "zerodha":
        df_smart = _read_file_with_header_scan(file_bytes, filename)
        return parse_zerodha(df_smart)

    # For Dhan, find the row containing "Security Name" (P&L report has metadata before it)
    if broker_lower == "dhan":
        df_smart = _read_dhan_file(file_bytes, filename)
        return parse_dhan(df_smart)

    # For Angel One, scan past metadata rows to find the real header
    if broker_lower == "angelone":
        df_smart = _read_angelone_file(file_bytes, filename)
        return parse_angelone(df_smart)

    return parsers[broker_lower](df)


def _read_file(file_bytes: bytes, filename: str) -> pd.DataFrame:
    name = (filename or "").lower()
    if name.endswith(".xlsx") or name.endswith(".xls"):
        return pd.read_excel(io.BytesIO(file_bytes))

    for encoding in ("utf-8", "utf-8-sig", "latin-1"):
        try:
            text = file_bytes.decode(encoding)
            return pd.read_csv(io.StringIO(text))
        except (UnicodeDecodeError, pd.errors.ParserError):
            continue

    raise ValueError("Could not read file. Ensure it is a valid CSV or Excel file.")


def _find_header_row(file_bytes: bytes, filename: str, min_cols: int = 5) -> Optional[int]:
    """
    Scan an Excel file row by row to find the first row that looks like a real header
    (has at least `min_cols` non-null, non-numeric string values).
    Returns the 0-based row index to use as `skiprows`, or None if not found.
    """
    name = (filename or "").lower()
    if not (name.endswith(".xlsx") or name.endswith(".xls")):
        return None
    try:
        df_raw = pd.read_excel(io.BytesIO(file_bytes), header=None)
        for i, row in df_raw.iterrows():
            str_vals = [v for v in row if isinstance(v, str) and v.strip()]
            if len(str_vals) >= min_cols:
                return int(i)
    except Exception:
        pass
    return None


# ── Auto-detection ────────────────────────────────────────────────────────────

def _detect_broker(df: pd.DataFrame) -> Optional[str]:
    cols_raw = " ".join(str(c) for c in df.columns).lower()

    if "order_execution_time" in cols_raw or (
        "trade_id" in cols_raw and "trade_type" in cols_raw
    ):
        return "zerodha"
    if "instrument name" in cols_raw or "instrument_name" in cols_raw:
        return "upstox"
    if "realised p&l" in cols_raw or "realised_p&l" in cols_raw or "realised pnl" in cols_raw:
        return "groww"
    if "trade no." in cols_raw or "trade no" in cols_raw or (
        "description" in cols_raw and "series" in cols_raw
    ):
        return "dhan"
    if "net instrument" in cols_raw or (
        "buy rate" in cols_raw and "sell rate" in cols_raw and "net p&l" in cols_raw
    ):
        return "angelone"
    return None


# ── Shared utilities ──────────────────────────────────────────────────────────

def _norm_cols(df: pd.DataFrame) -> pd.DataFrame:
    """Lowercase, strip, replace spaces/special chars with underscores."""
    df = df.copy()
    df.columns = (
        pd.Index(df.columns)
        .str.strip()
        .str.lower()
        .str.replace(r"[\s/\.]+", "_", regex=True)
        .str.replace(r"[^a-z0-9_]", "", regex=True)
    )
    return df


def _get_col(df: pd.DataFrame, candidates: list[str]) -> Optional[str]:
    """Return the first candidate column name that exists in df, else None."""
    for c in candidates:
        if c in df.columns:
            return c
    return None


def _parse_date(val) -> Optional[date]:
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return None
    if isinstance(val, datetime):
        return val.date()
    if isinstance(val, date):
        return val
    s = str(val).strip().split(" ")[0]  # take date portion only
    for fmt in ("%Y-%m-%d", "%d-%m-%Y", "%d/%m/%Y", "%m/%d/%Y", "%d-%b-%Y", "%Y/%m/%d"):
        try:
            return datetime.strptime(s, fmt).date()
        except ValueError:
            continue
    return None


def _clean_float(val) -> Optional[float]:
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return None
    try:
        return float(str(val).replace(",", "").replace("₹", "").replace(" ", "").strip())
    except (ValueError, TypeError):
        return None


def _detect_instrument(symbol: str, extra: str = "") -> str:
    extra_up = extra.upper()
    sym_up   = symbol.upper()

    # Extra field (segment/series like "CE", "PE", "FO", "OPT") takes priority
    if any(x in extra_up for x in ("CE", "PE", "CALL", "PUT", "OPT", "FNO", "F&O")):
        return "options"
    if any(x in extra_up for x in ("FUT", "FUTURE")):
        return "futures"

    # Symbol: CE/PE must be preceded by a digit (e.g. NIFTY22600CE not RELIANCE/PERSISTENT/PRICE)
    if re.search(r'\d(CE|PE)$', sym_up) or any(x in sym_up for x in ("CALL", "PUT")):
        return "options"
    if any(x in sym_up for x in ("FUT", "FUTURE")):
        return "futures"

    return "equity"


def _detect_trade_type(instrument_type: str, holding_days: int = 0) -> str:
    if instrument_type == "options":
        return "options_intraday"
    if instrument_type == "futures":
        return "futures_swing"
    return "equity_swing"


# ── FIFO buy/sell matching ────────────────────────────────────────────────────

def _match_pairs(raw_legs: list[dict]) -> list[dict]:
    """
    FIFO-match BUY and SELL legs from tradebook-format rows.

    Groups by symbol. Within each group sorts by date then matches each
    BUY against the earliest available SELL.

    Unmatched BUY legs become open positions.
    """
    by_symbol: dict = defaultdict(lambda: {"buys": [], "sells": []})
    for leg in raw_legs:
        sym = leg.get("symbol", "")
        if leg.get("action") == "buy":
            by_symbol[sym]["buys"].append({**leg, "quantity": leg.get("quantity") or 0})
        else:
            by_symbol[sym]["sells"].append({**leg, "quantity": leg.get("quantity") or 0})

    trades: list[dict] = []

    for sym, legs in by_symbol.items():
        buys  = sorted(legs["buys"],  key=lambda x: x.get("trade_date") or date.min)
        sells = sorted(legs["sells"], key=lambda x: x.get("trade_date") or date.min)

        bi = si = 0
        while bi < len(buys) and si < len(sells):
            buy  = buys[bi]
            sell = sells[si]

            qty = min(buy["quantity"], sell["quantity"])
            if qty <= 0:
                if buy["quantity"] <= 0:
                    bi += 1
                if sell["quantity"] <= 0:
                    si += 1
                continue

            entry_price = buy.get("entry_price")
            exit_price  = sell.get("entry_price")   # sell leg stores its price as entry_price

            pnl = pnl_percent = None
            if entry_price and exit_price and qty:
                pnl         = round((exit_price - entry_price) * qty, 2)
                denom       = entry_price * qty
                pnl_percent = round(pnl / denom * 100, 4) if denom else 0.0

            holding_days = 0
            if buy.get("trade_date") and sell.get("trade_date"):
                holding_days = (sell["trade_date"] - buy["trade_date"]).days

            instrument_type = buy.get("instrument_type", "equity")

            trades.append({
                "symbol":          sym,
                "instrument_type": instrument_type,
                "trade_type":      _detect_trade_type(instrument_type, holding_days),
                "action":          "buy",
                "status":          "closed",
                "quantity":        qty,
                "entry_price":     entry_price,
                "exit_price":      exit_price,
                "pnl":             pnl,
                "pnl_percent":     pnl_percent,
                "trade_date":      str(buy["trade_date"]) if buy.get("trade_date") else None,
                "closed_at":       str(sell["trade_date"]) if sell.get("trade_date") else None,
                "holding_days":    holding_days,
                "broker":          buy.get("broker"),
                "sector":          None,
                "ai_feedback":     None,
            })

            buy["quantity"]  -= qty
            sell["quantity"] -= qty
            if buy["quantity"]  <= 0:
                bi += 1
            if sell["quantity"] <= 0:
                si += 1

        # Remaining unmatched buys → open positions
        while bi < len(buys):
            buy = buys[bi]
            if buy["quantity"] > 0:
                instrument_type = buy.get("instrument_type", "equity")
                trades.append({
                    "symbol":          buy.get("symbol"),
                    "instrument_type": instrument_type,
                    "trade_type":      _detect_trade_type(instrument_type),
                    "action":          "buy",
                    "status":          "open",
                    "quantity":        buy["quantity"],
                    "entry_price":     buy.get("entry_price"),
                    "exit_price":      None,
                    "pnl":             None,
                    "pnl_percent":     None,
                    "trade_date":      str(buy["trade_date"]) if buy.get("trade_date") else None,
                    "closed_at":       None,
                    "holding_days":    None,
                    "broker":          buy.get("broker"),
                    "sector":          None,
                    "ai_feedback":     None,
                })
            bi += 1

    return trades


# ── Zerodha ───────────────────────────────────────────────────────────────────
#
# Kite Tradebook CSV columns:
#   trade_id, trade_type (BUY/SELL), instrument_type (EQ/FUT/CE/PE),
#   symbol, expiry, strike, option_type, quantity, price, order_execution_time
#
# Zerodha Tax P&L Excel (Console → Reports → Tax P&L):
#   symbol, isin, quantity, buy_value, sell_value, realized_pl,
#   realized_pl_pct, previous_closing_price, open_quantity, open_quantity_type,
#   open_value, unrealized_pl, unrealized_pl_pct


_ISIN_RE = re.compile(r'^[A-Z]{2}[A-Z0-9]{9}[0-9]$')


def _parse_zerodha_pnl(df: pd.DataFrame) -> list[dict]:
    """
    Parse Zerodha Tax P&L Statement format (equity & F&O).
    Each row is a symbol with closed trade summary + open position.
    No individual trade dates are available in this format.
    """
    sym_col   = _get_col(df, ["symbol", "scrip", "instrument"])
    qty_col   = _get_col(df, ["quantity", "qty"])
    buy_col   = _get_col(df, ["buy_value", "buy_amount"])
    sell_col  = _get_col(df, ["sell_value", "sell_amount"])
    pnl_col   = _get_col(df, ["realized_pl", "realized_p_l", "realised_pl", "realised_p_l", "p_l"])
    oqty_col  = _get_col(df, ["open_quantity", "open_qty"])
    oval_col  = _get_col(df, ["open_value"])

    trades: list[dict] = []

    for _, row in df.iterrows():
        if not sym_col:
            continue

        symbol = str(row.get(sym_col, "")).strip().upper()

        # Skip blank, header-echo, or ISIN-looking values in symbol column
        if not symbol or symbol in ("NAN", "SYMBOL", "") or _ISIN_RE.match(symbol):
            continue

        qty    = _clean_float(row.get(qty_col))    if qty_col   else None
        buy_v  = _clean_float(row.get(buy_col))    if buy_col   else None
        sell_v = _clean_float(row.get(sell_col))   if sell_col  else None
        pnl    = _clean_float(row.get(pnl_col))    if pnl_col   else None
        oqty   = _clean_float(row.get(oqty_col))   if oqty_col  else None
        oval   = _clean_float(row.get(oval_col))   if oval_col  else None

        instrument_type = _detect_instrument(symbol)

        # ── Closed trade (sold quantity > 0) ──
        if qty and qty > 0 and pnl is not None:
            entry = round(buy_v / qty, 4) if buy_v and qty else None
            exit_ = round(sell_v / qty, 4) if sell_v and qty else None
            pnl_pct = round(pnl / buy_v * 100, 4) if buy_v and buy_v != 0 else None
            trades.append({
                "symbol":          symbol,
                "instrument_type": instrument_type,
                "trade_type":      _detect_trade_type(instrument_type),
                "action":          "buy",
                "status":          "closed",
                "quantity":        int(qty),
                "entry_price":     entry,
                "exit_price":      exit_,
                "pnl":             round(pnl, 2),
                "pnl_percent":     pnl_pct,
                "trade_date":      None,
                "closed_at":       None,
                "holding_days":    None,
                "broker":          "Zerodha",
                "sector":          None,
                "ai_feedback":     None,
            })

        # ── Open position (still held) ──
        if oqty and oqty > 0:
            entry = round(oval / oqty, 4) if oval and oqty else None
            trades.append({
                "symbol":          symbol,
                "instrument_type": instrument_type,
                "trade_type":      _detect_trade_type(instrument_type),
                "action":          "buy",
                "status":          "open",
                "quantity":        int(oqty),
                "entry_price":     entry,
                "exit_price":      None,
                "pnl":             None,
                "pnl_percent":     None,
                "trade_date":      None,
                "closed_at":       None,
                "holding_days":    None,
                "broker":          "Zerodha",
                "sector":          None,
                "ai_feedback":     None,
            })

    return trades


def parse_zerodha(df: pd.DataFrame) -> list[dict]:
    df = _norm_cols(df)

    # ── Detect Zerodha Tax P&L format ──
    # Columns: symbol, isin, quantity, buy_value, sell_value, realized_pl,
    #           open_quantity, open_value, unrealized_pl
    pnl_indicators = {"buy_value", "sell_value", "realized_pl", "open_quantity", "open_value"}
    if pnl_indicators & set(df.columns):
        return _parse_zerodha_pnl(df)

    raw_legs: list[dict] = []
    for _, row in df.iterrows():
        action = str(row.get("trade_type", "")).strip().lower()
        if action not in ("buy", "sell"):
            continue

        inst_raw = str(row.get("instrument_type", "")).upper()
        if any(x in inst_raw for x in ("CE", "PE", "OPT")):
            instrument_type = "options"
        elif "FUT" in inst_raw:
            instrument_type = "futures"
        else:
            instrument_type = "equity"

        symbol = str(row.get("symbol", "")).strip().upper()
        if ":" in symbol:
            symbol = symbol.split(":")[-1]

        # Append strike + option_type for options readability
        if instrument_type == "options":
            option_type = str(row.get("option_type", "")).strip().upper()
            strike      = _clean_float(row.get("strike"))
            if strike and option_type:
                symbol = f"{symbol} {int(strike)}{option_type}"

        exec_time = str(row.get("order_execution_time", "")).strip()
        trade_date = _parse_date(exec_time)

        qty = int(_clean_float(row.get("quantity")) or 0)
        if qty <= 0:
            continue

        raw_legs.append({
            "symbol":          symbol,
            "instrument_type": instrument_type,
            "action":          action,
            "quantity":        qty,
            "entry_price":     _clean_float(row.get("price")),
            "trade_date":      trade_date,
            "broker":          "Zerodha",
        })

    return _match_pairs(raw_legs)


# ── Upstox ────────────────────────────────────────────────────────────────────
#
# Tradebook CSV columns (may vary across app versions):
#   Instrument Name, ISIN, Exchange, Segment, Product,
#   Trade Date, Trade Number, Order Number, Trade Time,
#   Buy/Sell, Quantity, Price, Trade Value

def parse_upstox(df: pd.DataFrame, file_bytes: bytes = b"", filename: str = "") -> list[dict]:
    df_norm = _norm_cols(df)

    # ── Detect Upstox Realized P&L format ──
    # Header: Scrip Name, Symbol, Scrip Opt, Qty, Buy Date, Buy Rate, Sell Date, Sell Rate, Total PL, Strike Price
    pnl_indicators = {"buy_rate", "sell_rate", "total_pl", "scrip_opt", "strike_price"}
    if pnl_indicators & set(df_norm.columns):
        return _parse_upstox_pnl(df_norm)

    # ── Fall back to tradebook format (individual buy/sell legs) ──
    name_col  = _get_col(df_norm, ["instrument_name", "symbol", "scrip", "name", "trading_symbol"])
    side_col  = _get_col(df_norm, ["buy_sell", "side", "trade_type", "transaction_type", "buysell"])
    qty_col   = _get_col(df_norm, ["quantity", "qty", "trade_quantity"])
    price_col = _get_col(df_norm, ["price", "trade_price", "avg_price"])
    date_col  = _get_col(df_norm, ["trade_date", "date", "order_date", "execution_date"])
    seg_col   = _get_col(df_norm, ["segment", "series", "instrument", "product"])

    raw_legs: list[dict] = []
    for _, row in df_norm.iterrows():
        if not name_col or not side_col:
            continue

        name = str(row.get(name_col, "")).strip().upper()
        if not name:
            continue

        side   = str(row.get(side_col, "")).strip().upper()
        action = "buy" if side.startswith("B") else "sell"

        segment         = str(row.get(seg_col, "")).upper() if seg_col else ""
        instrument_type = _detect_instrument(name, segment)
        symbol          = name.split(" ")[0]

        qty = int(_clean_float(row.get(qty_col)) or 0) if qty_col else 0
        if qty <= 0:
            continue

        raw_legs.append({
            "symbol":          symbol,
            "instrument_type": instrument_type,
            "action":          action,
            "quantity":        qty,
            "entry_price":     _clean_float(row.get(price_col)) if price_col else None,
            "trade_date":      _parse_date(row.get(date_col)) if date_col else None,
            "broker":          "Upstox",
        })

    return _match_pairs(raw_legs)


def _parse_upstox_pnl(df: pd.DataFrame) -> list[dict]:
    """
    Parse Upstox Realized P&L Excel report (FO/EQ).
    Columns: Scrip Name, Symbol, Scrip Opt, Qty, Buy Date, Buy Rate,
             Sell Date, Sell Rate, Days, Total PL, Strike Price
    """
    trades: list[dict] = []

    for _, row in df.iterrows():
        # Symbol: use 'symbol' col (cleaner), fall back to 'scrip_name'
        symbol_base = str(row.get("symbol", row.get("scrip_name", ""))).strip().upper()
        if not symbol_base or symbol_base == "NAN":
            continue

        scrip_opt  = str(row.get("scrip_opt", "")).strip().upper()
        strike     = _clean_float(row.get("strike_price"))
        qty        = int(_clean_float(row.get("qty")) or 0)
        buy_rate   = _clean_float(row.get("buy_rate"))
        sell_rate  = _clean_float(row.get("sell_rate"))
        total_pl   = _clean_float(row.get("total_pl"))
        buy_date   = _parse_date(row.get("buy_date"))
        sell_date  = _parse_date(row.get("sell_date"))
        days       = int(_clean_float(row.get("days")) or 0)

        if qty <= 0:
            continue

        # Build readable symbol: NIFTY 22600 CE
        if scrip_opt in ("CE", "PE") and strike:
            symbol = f"{symbol_base} {int(strike)} {scrip_opt}"
            instrument_type = "options"
        elif scrip_opt == "FUT":
            symbol = f"{symbol_base} FUT"
            instrument_type = "futures"
        else:
            symbol = symbol_base
            instrument_type = "equity"

        pnl_percent = None
        if total_pl is not None and buy_rate and qty:
            cost = buy_rate * qty
            pnl_percent = round(total_pl / cost * 100, 4) if cost else 0.0

        trades.append({
            "symbol":          symbol,
            "instrument_type": instrument_type,
            "trade_type":      _detect_trade_type(instrument_type, days),
            "action":          "buy",
            "status":          "closed",
            "quantity":        qty,
            "entry_price":     buy_rate,
            "exit_price":      sell_rate,
            "pnl":             total_pl,
            "pnl_percent":     pnl_percent,
            "trade_date":      str(buy_date) if buy_date else None,
            "closed_at":       str(sell_date) if sell_date else None,
            "holding_days":    days,
            "broker":          "Upstox",
            "sector":          None,
            "ai_feedback":     None,
        })

    return trades


# ── Groww ─────────────────────────────────────────────────────────────────────
#
# Two formats:
#
# (A) P&L statement (each row = closed trade):
#   Symbol, ISIN, Qty, Avg Buy Price, Avg Sell Price, Realised P&L, Trade Date
#
# (B) Transaction / order history (individual legs):
#   Date, Exchange, Symbol, Series, Trade Type, Qty, Price, Trade Value, ...

def parse_groww(df: pd.DataFrame) -> list[dict]:
    df = _norm_cols(df)
    cols = set(df.columns)

    pnl_indicators = {"realised_pl", "realised_pnl", "realised_p_l", "net_pnl", "realized_pnl", "pnl"}
    if cols & pnl_indicators:
        return _parse_groww_pnl(df)
    return _parse_groww_transactions(df)


def _parse_groww_pnl(df: pd.DataFrame) -> list[dict]:
    sym_col   = _get_col(df, ["symbol", "scrip_name", "name", "stock"])
    qty_col   = _get_col(df, ["qty", "quantity", "close_qty", "open_qty"])
    buy_col   = _get_col(df, ["avg_buy_price", "buy_price", "buy_avg", "open_price", "entry_price"])
    sell_col  = _get_col(df, ["avg_sell_price", "sell_price", "sell_avg", "close_price", "exit_price"])
    pnl_col   = _get_col(df, ["realised_pl", "realised_pnl", "realised_p_l", "net_pnl", "realized_pnl", "pnl"])
    date_col  = _get_col(df, ["trade_date", "sell_date", "date", "close_date"])

    trades: list[dict] = []
    for _, row in df.iterrows():
        symbol = str(row.get(sym_col, "")).strip().upper() if sym_col else ""
        if not symbol:
            continue

        qty         = int(_clean_float(row.get(qty_col)) or 0) if qty_col else 0
        entry_price = _clean_float(row.get(buy_col))  if buy_col  else None
        exit_price  = _clean_float(row.get(sell_col)) if sell_col else None
        pnl         = _clean_float(row.get(pnl_col))  if pnl_col  else None
        trade_date  = _parse_date(row.get(date_col))  if date_col else None

        pnl_percent = None
        if pnl is not None and entry_price and qty:
            denom = entry_price * qty
            pnl_percent = round(pnl / denom * 100, 4) if denom else 0.0

        instrument_type = _detect_instrument(symbol)

        trades.append({
            "symbol":          symbol,
            "instrument_type": instrument_type,
            "trade_type":      _detect_trade_type(instrument_type),
            "action":          "buy",
            "status":          "closed",
            "quantity":        qty,
            "entry_price":     entry_price,
            "exit_price":      exit_price,
            "pnl":             pnl,
            "pnl_percent":     pnl_percent,
            "trade_date":      str(trade_date) if trade_date else None,
            "closed_at":       None,
            "holding_days":    None,
            "broker":          "Groww",
            "sector":          None,
            "ai_feedback":     None,
        })

    return trades


def _parse_groww_transactions(df: pd.DataFrame) -> list[dict]:
    sym_col   = _get_col(df, ["symbol", "scrip_name", "stock_name", "name"])
    side_col  = _get_col(df, ["trade_type", "side", "buy_sell", "transaction_type", "type"])
    qty_col   = _get_col(df, ["qty", "quantity"])
    price_col = _get_col(df, ["price", "avg_price", "trade_price"])
    date_col  = _get_col(df, ["date", "trade_date", "order_date"])
    seg_col   = _get_col(df, ["series", "segment", "exchange"])

    raw_legs: list[dict] = []
    for _, row in df.iterrows():
        if not sym_col:
            continue

        symbol = str(row.get(sym_col, "")).strip().upper()
        if not symbol:
            continue

        side   = str(row.get(side_col, "")).strip().upper() if side_col else "B"
        action = "buy" if side.startswith("B") else "sell"

        segment         = str(row.get(seg_col, "")).upper() if seg_col else ""
        instrument_type = _detect_instrument(symbol, segment)

        qty = int(_clean_float(row.get(qty_col)) or 0) if qty_col else 0
        if qty <= 0:
            continue

        raw_legs.append({
            "symbol":          symbol,
            "instrument_type": instrument_type,
            "action":          action,
            "quantity":        qty,
            "entry_price":     _clean_float(row.get(price_col)) if price_col else None,
            "trade_date":      _parse_date(row.get(date_col)) if date_col else None,
            "broker":          "Groww",
        })

    return _match_pairs(raw_legs)


# ── Dhan ──────────────────────────────────────────────────────────────────────
#
# Trade History CSV columns:
#   Exchange, Symbol, Series, Trade Date, Trade Time,
#   Trade No., Order No., Description, Buy/Sell, Qty, Price, Trade Value

def parse_dhan(df: pd.DataFrame) -> list[dict]:
    df_norm = _norm_cols(df)

    # Detect Dhan P&L report format (Security Name, Buy Qty., Avg. Buy Price, Realised P&L …)
    pnl_indicators = {"security_name", "buy_qty_", "avg__buy_price", "realised_pl"}
    if pnl_indicators & set(df_norm.columns):
        return _parse_dhan_pnl(df_norm)

    # Tradebook CSV format (individual buy/sell legs)
    sym_col    = _get_col(df_norm, ["symbol", "scrip", "trading_symbol", "name"])
    side_col   = _get_col(df_norm, ["buy_sell", "side", "trade_type", "buysell", "b_s"])
    qty_col    = _get_col(df_norm, ["qty", "quantity"])
    price_col  = _get_col(df_norm, ["price", "trade_price"])
    date_col   = _get_col(df_norm, ["trade_date", "date", "order_date"])
    series_col = _get_col(df_norm, ["series", "segment", "instrument", "description"])

    raw_legs: list[dict] = []
    for _, row in df_norm.iterrows():
        if not sym_col:
            continue

        symbol = str(row.get(sym_col, "")).strip().upper()
        if not symbol:
            continue

        side   = str(row.get(side_col, "")).strip().upper() if side_col else "B"
        action = "buy" if side.startswith("B") else "sell"

        series          = str(row.get(series_col, "")).upper() if series_col else ""
        instrument_type = _detect_instrument(symbol, series)

        qty = int(_clean_float(row.get(qty_col)) or 0) if qty_col else 0
        if qty <= 0:
            continue

        raw_legs.append({
            "symbol":          symbol,
            "instrument_type": instrument_type,
            "action":          action,
            "quantity":        qty,
            "entry_price":     _clean_float(row.get(price_col)) if price_col else None,
            "trade_date":      _parse_date(row.get(date_col)) if date_col else None,
            "broker":          "Dhan",
        })

    return _match_pairs(raw_legs)


def _parse_dhan_pnl(df: pd.DataFrame) -> list[dict]:
    """
    Parse Dhan P&L Statement (PNL_REPORT.xls).
    Columns: Sr., Security Name, ISIN, Buy Qty., Avg. Buy Price, Buy Value,
             Sell Qty., Avg. Sell Price, Sell Value, Realised P&L, Realised P&L %,
             Open Qty., Open Avg. Price, Closing Rate, Unrealised P&L, Unrealised P&L %
    """
    sym_col      = _get_col(df, ["security_name", "scrip_name", "symbol", "name"])
    buy_qty_col  = _get_col(df, ["buy_qty_", "buy_qty", "buy_quantity"])
    buy_px_col   = _get_col(df, ["avg__buy_price", "avg_buy_price", "buy_price"])
    sell_qty_col = _get_col(df, ["sell_qty_", "sell_qty", "sell_quantity"])
    sell_px_col  = _get_col(df, ["avg__sell_price", "avg_sell_price", "sell_price"])
    pnl_col      = _get_col(df, ["realised_pl", "realised_pnl", "net_pnl"])
    pnl_pct_col  = _get_col(df, ["realised_pl_", "realised_pl_pct"])
    oqty_col     = _get_col(df, ["open_qty_", "open_qty", "open_quantity"])
    opx_col      = _get_col(df, ["open_avg__price", "open_avg_price", "open_price"])

    skip_names = {"equity", "futures and options", "commodities", "currency", "nan", ""}

    trades: list[dict] = []
    for _, row in df.iterrows():
        if not sym_col:
            continue

        symbol = str(row.get(sym_col, "")).strip().upper()
        if not symbol or symbol.lower() in skip_names:
            continue

        # Skip numeric-only rows (Sr. number only)
        try:
            float(symbol)
            continue
        except ValueError:
            pass

        buy_qty  = int(_clean_float(row.get(buy_qty_col))  or 0) if buy_qty_col  else 0
        buy_px   = _clean_float(row.get(buy_px_col))              if buy_px_col   else None
        sell_qty = int(_clean_float(row.get(sell_qty_col)) or 0) if sell_qty_col else 0
        sell_px  = _clean_float(row.get(sell_px_col))             if sell_px_col  else None
        pnl      = _clean_float(row.get(pnl_col))                 if pnl_col      else None
        pnl_pct  = _clean_float(row.get(pnl_pct_col))             if pnl_pct_col  else None
        oqty     = int(_clean_float(row.get(oqty_col))     or 0) if oqty_col     else 0
        opx      = _clean_float(row.get(opx_col))                 if opx_col      else None

        instrument_type = _detect_instrument(symbol)

        if sell_qty > 0 and pnl is not None:
            trades.append({
                "symbol":          symbol,
                "instrument_type": instrument_type,
                "trade_type":      _detect_trade_type(instrument_type),
                "action":          "buy",
                "status":          "closed",
                "quantity":        sell_qty,
                "entry_price":     buy_px,
                "exit_price":      sell_px,
                "pnl":             round(pnl, 2),
                "pnl_percent":     pnl_pct,
                "trade_date":      None,
                "closed_at":       None,
                "holding_days":    None,
                "broker":          "Dhan",
                "sector":          None,
                "ai_feedback":     None,
            })

        if oqty > 0:
            trades.append({
                "symbol":          symbol,
                "instrument_type": instrument_type,
                "trade_type":      _detect_trade_type(instrument_type),
                "action":          "buy",
                "status":          "open",
                "quantity":        oqty,
                "entry_price":     opx,
                "exit_price":      None,
                "pnl":             None,
                "pnl_percent":     None,
                "trade_date":      None,
                "closed_at":       None,
                "holding_days":    None,
                "broker":          "Dhan",
                "sector":          None,
                "ai_feedback":     None,
            })

    return trades


# ── Angel One ─────────────────────────────────────────────────────────────────
#
# Two report formats available from Angel One Smart Back Office:
#
# (A) Net Position / P&L Statement (Excel) — most useful:
#   Net Instrument | Buy Qty | Buy Rate | Sell Qty | Sell Rate | Buy Value | Sell Value | Net P&L
#   -- OR older back-office --
#   Scrip Name | Qty | Buy Avg | Sell Avg | Buy Value | Sell Value | P&L
#
# (B) Trade Book CSV (individual legs):
#   Symbol | Exchange | Segment | Trade Date | Trade Time | Buy/Sell | Qty | Price | Order No


def parse_angelone(df: pd.DataFrame) -> list[dict]:
    df_norm = _norm_cols(df)

    # Detect P&L / Net Position format
    pnl_indicators = {
        "net_p_l", "net_pl", "p_l", "realized_p_l", "realised_p_l",
        "net_p&l", "buy_rate", "sell_rate", "buy_avg", "sell_avg",
        "avg_buy_price", "avg_sell_price",
    }
    if pnl_indicators & set(df_norm.columns):
        return _parse_angelone_pnl(df_norm)

    # Fall back to trade book (individual buy/sell legs)
    return _parse_angelone_tradebook(df_norm)


def _parse_angelone_pnl(df: pd.DataFrame) -> list[dict]:
    """
    Angel One Net Position / P&L Statement.
    Handles both newer 'Net Instrument' format and older 'Scrip Name' format.
    """
    sym_col      = _get_col(df, ["net_instrument", "scrip_name", "script_name", "symbol", "instrument", "name"])
    buy_qty_col  = _get_col(df, ["buy_qty", "buy_quantity"])
    buy_px_col   = _get_col(df, ["buy_rate", "buy_avg", "avg_buy_price", "avg__buy_price"])
    sell_qty_col = _get_col(df, ["sell_qty", "sell_quantity"])
    sell_px_col  = _get_col(df, ["sell_rate", "sell_avg", "avg_sell_price", "avg__sell_price"])
    pnl_col      = _get_col(df, ["net_p_l", "net_pl", "p_l", "realized_p_l", "realised_p_l", "net_p&l", "pnl"])
    buy_date_col = _get_col(df, ["buy_date", "date", "trade_date"])
    sell_date_col = _get_col(df, ["sell_date"])
    oqty_col     = _get_col(df, ["open_qty", "net_qty", "open_quantity"])
    opx_col      = _get_col(df, ["open_avg", "open_price", "closing_rate", "ltp"])

    trades: list[dict] = []

    for _, row in df.iterrows():
        if not sym_col:
            continue

        symbol = str(row.get(sym_col, "")).strip().upper()
        if not symbol or symbol in ("NAN", "", "NET INSTRUMENT", "SCRIP NAME", "SYMBOL"):
            continue

        # Skip pure-number rows (serial numbers)
        try:
            float(symbol)
            continue
        except ValueError:
            pass

        buy_qty  = int(_clean_float(row.get(buy_qty_col))  or 0) if buy_qty_col  else 0
        buy_px   = _clean_float(row.get(buy_px_col))              if buy_px_col   else None
        sell_qty = int(_clean_float(row.get(sell_qty_col)) or 0) if sell_qty_col else 0
        sell_px  = _clean_float(row.get(sell_px_col))             if sell_px_col  else None
        pnl      = _clean_float(row.get(pnl_col))                 if pnl_col      else None
        buy_date = _parse_date(row.get(buy_date_col))             if buy_date_col else None
        sell_date = _parse_date(row.get(sell_date_col))           if sell_date_col else None
        oqty     = int(_clean_float(row.get(oqty_col))     or 0) if oqty_col     else 0
        opx      = _clean_float(row.get(opx_col))                 if opx_col      else None

        instrument_type = _detect_instrument(symbol)
        qty = sell_qty or buy_qty

        pnl_percent = None
        if pnl is not None and buy_px and qty:
            cost = buy_px * qty
            pnl_percent = round(pnl / cost * 100, 4) if cost else 0.0

        if sell_qty > 0 and pnl is not None:
            trades.append({
                "symbol":          symbol,
                "instrument_type": instrument_type,
                "trade_type":      _detect_trade_type(instrument_type),
                "action":          "buy",
                "status":          "closed",
                "quantity":        sell_qty,
                "entry_price":     buy_px,
                "exit_price":      sell_px,
                "pnl":             round(pnl, 2),
                "pnl_percent":     pnl_percent,
                "trade_date":      str(buy_date) if buy_date else None,
                "closed_at":       str(sell_date) if sell_date else None,
                "holding_days":    (sell_date - buy_date).days if buy_date and sell_date else None,
                "broker":          "Angel One",
                "sector":          None,
                "ai_feedback":     None,
            })

        if oqty > 0:
            trades.append({
                "symbol":          symbol,
                "instrument_type": instrument_type,
                "trade_type":      _detect_trade_type(instrument_type),
                "action":          "buy",
                "status":          "open",
                "quantity":        oqty,
                "entry_price":     opx,
                "exit_price":      None,
                "pnl":             None,
                "pnl_percent":     None,
                "trade_date":      str(buy_date) if buy_date else None,
                "closed_at":       None,
                "holding_days":    None,
                "broker":          "Angel One",
                "sector":          None,
                "ai_feedback":     None,
            })

    return trades


def _parse_angelone_tradebook(df: pd.DataFrame) -> list[dict]:
    """
    Angel One Trade Book CSV (individual buy/sell legs).
    Columns: Symbol, Exchange, Segment, Trade Date, Trade Time, Buy/Sell, Qty, Price
    """
    sym_col   = _get_col(df, ["symbol", "scrip_name", "script_name", "net_instrument", "instrument"])
    side_col  = _get_col(df, ["buy_sell", "buysell", "b_s", "side", "trade_type", "transaction_type"])
    qty_col   = _get_col(df, ["qty", "quantity", "trade_qty"])
    price_col = _get_col(df, ["price", "trade_price", "avg_price", "rate"])
    date_col  = _get_col(df, ["trade_date", "date", "order_date", "execution_date"])
    seg_col   = _get_col(df, ["segment", "series", "exchange", "instrument_type"])

    raw_legs: list[dict] = []
    for _, row in df.iterrows():
        if not sym_col:
            continue

        symbol = str(row.get(sym_col, "")).strip().upper()
        if not symbol or symbol in ("NAN", ""):
            continue

        side   = str(row.get(side_col, "")).strip().upper() if side_col else "B"
        action = "buy" if side.startswith("B") else "sell"

        segment         = str(row.get(seg_col, "")).upper() if seg_col else ""
        instrument_type = _detect_instrument(symbol, segment)

        qty = int(_clean_float(row.get(qty_col)) or 0) if qty_col else 0
        if qty <= 0:
            continue

        raw_legs.append({
            "symbol":          symbol,
            "instrument_type": instrument_type,
            "action":          action,
            "quantity":        qty,
            "entry_price":     _clean_float(row.get(price_col)) if price_col else None,
            "trade_date":      _parse_date(row.get(date_col)) if date_col else None,
            "broker":          "Angel One",
        })

    return _match_pairs(raw_legs)
