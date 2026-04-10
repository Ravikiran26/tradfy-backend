"""
market_data.py — Fetch India VIX + underlying spot, compute Black-Scholes Greeks.

Used to give options traders context on the volatility / theta environment
at the time of their trade.
"""

import re
import numpy as np
from datetime import datetime, timedelta
from typing import Optional
import warnings

warnings.filterwarnings("ignore")

try:
    import yfinance as yf
    _YF_AVAILABLE = True
except ImportError:
    _YF_AVAILABLE = False

try:
    from scipy.stats import norm
    _SCIPY_AVAILABLE = True
except ImportError:
    _SCIPY_AVAILABLE = False


# ── Symbol parsing ────────────────────────────────────────────────────────────

_TICKER_MAP = {
    "NIFTY":     "^NSEI",
    "BANKNIFTY": "^NSEBANK",
    "FINNIFTY":  "NIFTY_FIN_SERVICE.NS",
    "BSX":       "^BSESN",      # BSE SENSEX options
    "SENSEX":    "^BSESN",
    "MIDCPNIFTY":"^CNXMIDCAP",
}


def parse_option_symbol(symbol: str) -> tuple[Optional[str], Optional[int], Optional[str]]:
    """
    Extract (underlying, strike, option_type) from broker symbol strings.
    Handles formats like: 'NIFTY22600CE', 'NIFTY 22600 CE', 'BSX 75300 CE',
    'BANKNIFTY75300CE', 'NIFTY23600 CE'
    Returns (None, None, None) if pattern not recognised.
    """
    s = symbol.upper().replace(" ", "")
    # Match: known_underlying + digits + CE/PE
    m = re.search(
        r"(BANKNIFTY|FINNIFTY|MIDCPNIFTY|SENSEX|NIFTY|BSX)"
        r"(\d{4,6})"
        r"(CE|PE)",
        s,
    )
    if m:
        return m.group(1), int(m.group(2)), m.group(3)
    return None, None, None


# ── Expiry helpers ────────────────────────────────────────────────────────────

def _next_thursday(dt: datetime) -> datetime:
    """Return the nearest Thursday on or after dt (NSE weekly expiry day)."""
    days = (3 - dt.weekday()) % 7  # 3 = Thursday
    return dt + timedelta(days=days)


def _next_friday(dt: datetime) -> datetime:
    """Return nearest Friday on or after dt (BSE SENSEX weekly expiry day)."""
    days = (4 - dt.weekday()) % 7  # 4 = Friday
    return dt + timedelta(days=days)


def _dte(underlying: str, trade_dt: datetime) -> int:
    """Days to nearest weekly expiry from trade_dt."""
    if underlying in ("BSX", "SENSEX"):
        expiry = _next_friday(trade_dt)
    else:
        expiry = _next_thursday(trade_dt)
    dte = (expiry - trade_dt).days
    return max(dte, 0)


# ── Greeks (Black-Scholes) ────────────────────────────────────────────────────

def _bs_greeks(S: float, K: int, T: float, r: float, sigma: float, opt: str) -> dict:
    """
    Return Delta, Gamma, Theta (per day), Vega.
    Theta is in ₹ per lot (lot size 50 for NIFTY, 15 for BANKNIFTY, 10 for SENSEX).
    For raw per-unit values use lot_size=1.
    """
    if not _SCIPY_AVAILABLE or T <= 0 or S <= 0 or sigma <= 0:
        return {}

    d1 = (np.log(S / K) + (r + 0.5 * sigma ** 2) * T) / (sigma * np.sqrt(T))
    d2 = d1 - sigma * np.sqrt(T)

    gamma = float(norm.pdf(d1) / (S * sigma * np.sqrt(T)))
    vega  = float(S * norm.pdf(d1) * np.sqrt(T) / 100)  # per 1% IV move

    if opt == "CE":
        delta = float(norm.cdf(d1))
        theta = float(
            (-(S * norm.pdf(d1) * sigma) / (2 * np.sqrt(T))
             - r * K * np.exp(-r * T) * norm.cdf(d2)) / 365
        )
    else:
        delta = float(norm.cdf(d1) - 1)
        theta = float(
            (-(S * norm.pdf(d1) * sigma) / (2 * np.sqrt(T))
             + r * K * np.exp(-r * T) * norm.cdf(-d2)) / 365
        )

    return {
        "delta": round(delta, 4),
        "gamma": round(gamma, 6),
        "theta": round(theta, 2),   # ₹ per unit per day (negative for buyers)
        "vega":  round(vega, 2),
    }


# ── yfinance fetch ────────────────────────────────────────────────────────────

def _yf_close(ticker: str, trade_dt: datetime) -> Optional[float]:
    """
    Fetch closing price for ticker on trade_dt.
    Tries up to 5 calendar days forward to handle weekends / holidays.
    """
    if not _YF_AVAILABLE:
        return None
    try:
        import pandas as pd
        start = trade_dt.strftime("%Y-%m-%d")
        end   = (trade_dt + timedelta(days=5)).strftime("%Y-%m-%d")
        df    = yf.download(ticker, start=start, end=end, progress=False, auto_adjust=True)
        if df.empty:
            return None
        # Handle yfinance MultiIndex columns (ticker as level-1)
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = df.columns.droplevel(1)
        return float(df["Close"].iloc[0])
    except Exception:
        return None


# ── Candlestick pattern detection ────────────────────────────────────────────

def _fetch_ohlcv(ticker: str, trade_dt: datetime, lookback: int = 30) -> Optional[object]:
    """Fetch daily OHLCV for last `lookback` days ending on trade_dt (inclusive)."""
    if not _YF_AVAILABLE:
        return None
    try:
        import pandas as pd
        start = (trade_dt - timedelta(days=lookback + 5)).strftime("%Y-%m-%d")
        end   = (trade_dt + timedelta(days=2)).strftime("%Y-%m-%d")
        df    = yf.download(ticker, start=start, end=end, progress=False, auto_adjust=True)
        if df.empty:
            return None
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = df.columns.droplevel(1)
        # Keep only rows up to and including trade date
        df = df[df.index <= pd.Timestamp(trade_dt.date())]
        return df if len(df) >= 2 else None
    except Exception:
        return None


def _detect_candle_pattern(df) -> dict:
    """
    Detect candlestick pattern from the last 1-3 candles.
    Returns: { pattern, signal, description, prev_close, curr_open, curr_high, curr_low, curr_close, change_pct }
    """
    try:
        c = df.iloc[-1]   # today (trade date)
        p = df.iloc[-2]   # previous day

        o, h, l, cl = float(c["Open"]), float(c["High"]), float(c["Low"]), float(c["Close"])
        po, ph, pl, pcl = float(p["Open"]), float(p["High"]), float(p["Low"]), float(p["Close"])

        body       = abs(cl - o)
        total_range = h - l
        upper_wick  = h - max(o, cl)
        lower_wick  = min(o, cl) - l
        is_bullish  = cl > o
        change_pct  = round((cl - pcl) / pcl * 100, 2)

        prev_body     = abs(pcl - po)
        prev_bullish  = pcl > po

        pattern = "No Clear Pattern"
        signal  = "neutral"
        desc    = ""

        # ── Single-candle patterns ────────────────────────────────────────────
        if total_range > 0:
            body_ratio  = body / total_range
            upper_ratio = upper_wick / total_range if total_range else 0
            lower_ratio = lower_wick / total_range if total_range else 0

            # Doji
            if body_ratio < 0.1:
                pattern = "Doji"
                signal  = "neutral"
                desc    = "Market is indecisive — buyers and sellers balanced. Expect a directional move soon."

            # Marubozu (strong trend candle — body ≥ 85% of range)
            elif body_ratio >= 0.85:
                if is_bullish:
                    pattern = "Bullish Marubozu"
                    signal  = "bullish"
                    desc    = "Strong buying day — bulls in full control with no upper wick resistance."
                else:
                    pattern = "Bearish Marubozu"
                    signal  = "bearish"
                    desc    = "Strong selling day — bears dominated the full session."

            # Hammer (lower wick ≥ 2× body, small upper wick, appears in downtrend)
            elif lower_ratio >= 0.55 and upper_ratio < 0.15 and body_ratio < 0.35:
                pattern = "Hammer"
                signal  = "bullish"
                desc    = "Sellers pushed price down but buyers recovered strongly. Potential reversal signal."

            # Shooting Star (upper wick ≥ 2× body, small lower wick)
            elif upper_ratio >= 0.55 and lower_ratio < 0.15 and body_ratio < 0.35:
                pattern = "Shooting Star"
                signal  = "bearish"
                desc    = "Buyers pushed higher but sellers rejected the move. Bearish reversal warning."

            # Spinning Top (small body, significant wicks on both sides)
            elif body_ratio < 0.25 and upper_ratio > 0.25 and lower_ratio > 0.25:
                pattern = "Spinning Top"
                signal  = "neutral"
                desc    = "Indecision with both sides fighting — no clear winner. Wait for confirmation."

        # ── Two-candle patterns ───────────────────────────────────────────────
        if prev_body > 0 and pattern == "No Clear Pattern":
            # Bullish Engulfing
            if (not prev_bullish and is_bullish and
                    o <= pcl and cl >= po):
                pattern = "Bullish Engulfing"
                signal  = "bullish"
                desc    = "Today's bullish candle completely engulfs yesterday's bearish one — strong buying signal."

            # Bearish Engulfing
            elif (prev_bullish and not is_bullish and
                    o >= pcl and cl <= po):
                pattern = "Bearish Engulfing"
                signal  = "bearish"
                desc    = "Today's bearish candle completely engulfs yesterday's bullish one — strong selling signal."

            # Inside Bar
            elif h < ph and l > pl:
                pattern = "Inside Bar"
                signal  = "neutral"
                desc    = "Market consolidating inside yesterday's range — a breakout is building up."

            # Outside Bar
            elif h > ph and l < pl:
                pattern = "Outside Bar"
                signal  = "neutral"
                desc    = "Today's range exceeded yesterday's on both sides — volatile, directional move likely."

        # ── 3-candle: Morning / Evening Star ────────────────────────────────
        if len(df) >= 3 and pattern in ("No Clear Pattern", "Doji", "Spinning Top"):
            pp = df.iloc[-3]
            ppo, ppcl = float(pp["Open"]), float(pp["Close"])
            pp_bearish = ppcl < ppo

            # Morning Star (gap down, doji/small candle, gap up)
            if pp_bearish and is_bullish and body / total_range < 0.4:
                pattern = "Morning Star"
                signal  = "bullish"
                desc    = "3-candle bullish reversal — sellers exhausted, buyers stepping in."

            # Evening Star
            pp_bullish = ppcl > ppo
            if pp_bullish and not is_bullish and body / total_range < 0.4 and pattern not in ("Morning Star",):
                pattern = "Evening Star"
                signal  = "bearish"
                desc    = "3-candle bearish reversal — buyers exhausted, sellers taking control."

        # ── Fallback: describe the candle in plain English if no pattern matched ──
        if pattern == "No Clear Pattern" and total_range > 0:
            body_pct  = round(body / total_range * 100)
            uw_pct    = round(upper_wick / total_range * 100)
            lw_pct    = round(lower_wick / total_range * 100)

            if is_bullish:
                pattern = "Bullish Day"
                signal  = "bullish"
                if uw_pct > 40:
                    desc = (f"Buyers pushed price up +{change_pct}% but sellers created a long upper wick "
                            f"({uw_pct}% of range) — resistance overhead. Bullish but with caution.")
                elif lw_pct > 40:
                    desc = (f"Price dipped lower but recovered strongly to close +{change_pct}%. "
                            f"Long lower wick ({lw_pct}% of range) shows buyers absorbing selling pressure.")
                else:
                    desc = (f"Steady bullish day, +{change_pct}%. Body is {body_pct}% of the range — "
                            f"controlled buying with no major wicks.")
            else:
                pattern = "Bearish Day"
                signal  = "bearish"
                if lw_pct > 40:
                    desc = (f"Sellers pushed price down {change_pct}% but a long lower wick "
                            f"({lw_pct}% of range) shows buyers stepping in. Bearish but not strong.")
                elif uw_pct > 40:
                    desc = (f"Price attempted a rally but reversed to close {change_pct}%. "
                            f"Long upper wick ({uw_pct}% of range) — sellers rejected the highs.")
                else:
                    desc = (f"Clean bearish day, {change_pct}%. Body is {body_pct}% of the range — "
                            f"controlled selling throughout the session.")

        return {
            "pattern":     pattern,
            "signal":      signal,
            "description": desc,
            "open":        round(o, 2),
            "high":        round(h, 2),
            "low":         round(l, 2),
            "close":       round(cl, 2),
            "prev_close":  round(pcl, 2),
            "change_pct":  change_pct,
        }
    except Exception:
        return {}


def _detect_trend(df) -> dict:
    """
    Calculate EMA-20 and EMA-5, determine trend, key levels.
    """
    try:
        closes = df["Close"].astype(float)

        ema20 = float(closes.ewm(span=20, adjust=False).mean().iloc[-1])
        ema5  = float(closes.ewm(span=5,  adjust=False).mean().iloc[-1])
        curr  = float(closes.iloc[-1])

        if curr > ema20 and ema5 > ema20:
            trend       = "Uptrend"
            trend_note  = f"Price above both EMA-5 ({ema5:,.0f}) and EMA-20 ({ema20:,.0f}) — bullish structure."
            trend_signal = "bullish"
        elif curr < ema20 and ema5 < ema20:
            trend       = "Downtrend"
            trend_note  = f"Price below both EMA-5 ({ema5:,.0f}) and EMA-20 ({ema20:,.0f}) — bearish structure."
            trend_signal = "bearish"
        elif curr > ema20:
            trend       = "Above 20-EMA"
            trend_note  = f"Price above EMA-20 ({ema20:,.0f}) but mixed short-term signals."
            trend_signal = "neutral"
        else:
            trend       = "Below 20-EMA"
            trend_note  = f"Price below EMA-20 ({ema20:,.0f}) — short-term bearish bias."
            trend_signal = "bearish"

        # Key levels: prev day high/low
        prev_high = round(float(df["High"].iloc[-2]), 2)
        prev_low  = round(float(df["Low"].iloc[-2]),  2)

        # 20-day high/low
        high_20 = round(float(df["High"].tail(20).max()), 2)
        low_20  = round(float(df["Low"].tail(20).min()),  2)

        # Is price near a key level (within 0.3%)?
        key_level = None
        for level, label in [(prev_high, "Prev Day High"), (prev_low, "Prev Day Low"),
                              (high_20, "20-Day High"),    (low_20,   "20-Day Low")]:
            if abs(curr - level) / curr < 0.003:
                key_level = f"Near {label} ({level:,.0f})"
                break

        return {
            "trend":        trend,
            "trend_signal": trend_signal,
            "trend_note":   trend_note,
            "ema5":         round(ema5, 2),
            "ema20":        round(ema20, 2),
            "prev_high":    prev_high,
            "prev_low":     prev_low,
            "high_20":      high_20,
            "low_20":       low_20,
            "key_level":    key_level,
        }
    except Exception:
        return {}


def get_chart_context(underlying: str, trade_date) -> Optional[dict]:
    """
    Fetch OHLCV, detect candlestick pattern + trend for the underlying on trade_date.
    Returns None if data unavailable. Never raises.
    """
    if not _YF_AVAILABLE:
        return None
    try:
        dt = datetime.strptime(str(trade_date), "%Y-%m-%d") if trade_date else None
        if not dt:
            return None
    except (ValueError, TypeError):
        return None

    yf_ticker = _TICKER_MAP.get(underlying)
    if not yf_ticker:
        return None

    try:
        df = _fetch_ohlcv(yf_ticker, dt)
        if df is None:
            return None

        candle = _detect_candle_pattern(df)
        trend  = _detect_trend(df)

        if not candle or not trend:
            return None

        # Day-of-week context
        day_name = dt.strftime("%A")
        day_notes = {
            "Monday":    "Monday — gap risk from weekend news. IV often elevated at open.",
            "Tuesday":   "Tuesday — typically stable, good trend-following day.",
            "Wednesday": "Wednesday — mid-week, watch for pre-expiry positioning.",
            "Thursday":  "Thursday — NSE weekly expiry day. Gamma spikes, premium collapses fast.",
            "Friday":    "Friday — pre-weekend. Sellers favour this day; IV often sold off into close.",
        }
        day_note = day_notes.get(day_name, "")

        return {
            "underlying": underlying,
            "trade_date": str(trade_date),
            "day_of_week": day_name,
            "day_note":    day_note,
            **candle,
            **trend,
        }
    except Exception:
        return None


# ── Public API ────────────────────────────────────────────────────────────────

_VIX_RANGES = [
    (0,  12, "Extremely Low",  "Premiums crushed — IV near multi-year lows. Buyers paying very little but theta decay is rapid."),
    (12, 16, "Low",            "Low volatility — premiums decaying fast. Theta works hard against option buyers."),
    (16, 20, "Normal",         "Moderate volatility — fair premium pricing. Theta decay standard."),
    (20, 25, "Elevated",       "Elevated fear — decent premium, but expect sudden moves. Watch for whipsaws."),
    (25, 35, "High",           "High volatility — inflated premiums. Sellers have edge; buyers need strong directional move."),
    (35, 999,"Extreme",        "Panic levels — options massively overpriced. Premium spikes can reverse sharply."),
]


def _vix_context(vix: float) -> dict:
    for lo, hi, label, interpretation in _VIX_RANGES:
        if lo <= vix < hi:
            return {"label": label, "interpretation": interpretation}
    return {"label": "Unknown", "interpretation": ""}


def get_market_context(symbol: str, trade_date) -> Optional[dict]:
    """
    Main entry point.
    Returns a dict with VIX, spot, DTE, Greeks, and human-readable interpretations.
    Returns None if the symbol isn't an option or data is unavailable.
    Never raises — all errors are swallowed so trade saves are never blocked.
    """
    if not _YF_AVAILABLE or not _SCIPY_AVAILABLE:
        return None

    underlying, strike, opt_type = parse_option_symbol(symbol or "")
    if not underlying or not strike or not opt_type:
        return None

    try:
        dt = datetime.strptime(str(trade_date), "%Y-%m-%d") if trade_date else None
        if not dt:
            return None
    except (ValueError, TypeError):
        return None

    # ── Fetch spot + VIX ─────────────────────────────────────────────────────
    yf_ticker = _TICKER_MAP.get(underlying)
    if not yf_ticker:
        return None

    spot = _yf_close(yf_ticker,   dt)
    vix  = _yf_close("^INDIAVIX", dt)

    if spot is None or vix is None:
        return None

    # ── Greeks ───────────────────────────────────────────────────────────────
    dte    = _dte(underlying, dt)
    T      = max(dte / 365.0, 1 / 365.0)  # at least 1 day to avoid div-by-zero
    sigma  = vix / 100.0   # e.g. 14.5 → 0.145 annualised IV
    r      = 0.065         # India ~6.5% risk-free rate

    greeks = _bs_greeks(spot, strike, T, r, sigma, opt_type)

    # ── Moneyness ────────────────────────────────────────────────────────────
    diff_pct = (spot - strike) / strike * 100
    if opt_type == "CE":
        if spot > strike * 1.005:
            moneyness = "ITM"
        elif spot < strike * 0.995:
            moneyness = "OTM"
        else:
            moneyness = "ATM"
    else:
        if spot < strike * 0.995:
            moneyness = "ITM"
        elif spot > strike * 1.005:
            moneyness = "OTM"
        else:
            moneyness = "ATM"

    vix_ctx = _vix_context(vix)

    # Theta interpretation
    theta_val = greeks.get("theta", 0)
    gamma_val = greeks.get("gamma", 0)

    if dte == 0:
        theta_note = "Expiry day — theta is at maximum, premium collapses to intrinsic value only."
    elif dte <= 2:
        theta_note = f"Near expiry ({dte}d) — theta decay accelerating sharply. Gamma spike risk on small moves."
    elif dte <= 5:
        theta_note = f"Weekly expiry in {dte} days — theta erosion elevated. Time is working against you."
    else:
        theta_note = f"{dte} days to expiry — moderate theta. Time decay becomes critical inside 5 DTE."

    gamma_note = (
        "High gamma — small moves in underlying cause large premium swings. Double-edged near expiry."
        if gamma_val > 0.003 else
        "Low gamma — premium moves slowly relative to underlying. Directional move needed to profit."
    )

    # ── Chart context (candlestick + trend) ──────────────────────────────────
    chart = get_chart_context(underlying, trade_date) or {}

    return {
        "underlying":     underlying,
        "strike":         strike,
        "opt_type":       opt_type,
        "moneyness":      moneyness,
        "spot":           round(spot, 2),
        "vix":            round(vix, 2),
        "vix_label":      vix_ctx["label"],
        "vix_note":       vix_ctx["interpretation"],
        "dte":            dte,
        "theta_note":     theta_note,
        "gamma_note":     gamma_note,
        **greeks,
        # Chart context
        "candle_pattern":  chart.get("pattern"),
        "candle_signal":   chart.get("signal"),
        "candle_desc":     chart.get("description"),
        "candle_open":     chart.get("open"),
        "candle_high":     chart.get("high"),
        "candle_low":      chart.get("low"),
        "candle_close":    chart.get("close"),
        "candle_change":   chart.get("change_pct"),
        "trend":           chart.get("trend"),
        "trend_signal":    chart.get("trend_signal"),
        "trend_note":      chart.get("trend_note"),
        "ema5":            chart.get("ema5"),
        "ema20":           chart.get("ema20"),
        "prev_high":       chart.get("prev_high"),
        "prev_low":        chart.get("prev_low"),
        "key_level":       chart.get("key_level"),
        "day_of_week":     chart.get("day_of_week"),
        "day_note":        chart.get("day_note"),
    }


def get_swing_context(symbol: str, trade_date, entry_price: float = 0, as_of_date=None) -> Optional[dict]:
    """
    Fetch swing trade context for an equity/futures stock.
    Computes price vs EMA-20/50/200, 52-week range, candlestick, VIX, NIFTY trend.

    as_of_date: if provided, data is fetched up to this date instead of trade_date.
                Use today's date for open positions to get live market context.
    Returns None if data unavailable. Never raises.
    """
    if not _YF_AVAILABLE:
        return None

    try:
        dt = datetime.strptime(str(trade_date), "%Y-%m-%d") if trade_date else None
        if not dt:
            return None
    except (ValueError, TypeError):
        return None

    # For live/open-position coaching, use as_of_date as the data cutoff
    cutoff_dt = dt
    if as_of_date:
        try:
            cutoff_dt = datetime.strptime(str(as_of_date), "%Y-%m-%d")
        except (ValueError, TypeError):
            pass

    ticker = f"{symbol.upper()}.NS"

    try:
        import pandas as pd
        start = (dt - timedelta(days=280)).strftime("%Y-%m-%d")
        end   = (cutoff_dt + timedelta(days=2)).strftime("%Y-%m-%d")
        df    = yf.download(ticker, start=start, end=end, progress=False, auto_adjust=True)
        if df.empty:
            return None
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = df.columns.droplevel(1)
        df = df[df.index <= pd.Timestamp(cutoff_dt.date())]
        if len(df) < 5:
            return None
    except Exception:
        return None

    try:
        closes = df["Close"].astype(float)
        curr   = float(closes.iloc[-1])

        ema20  = float(closes.ewm(span=20,  adjust=False).mean().iloc[-1])
        ema50  = float(closes.ewm(span=50,  adjust=False).mean().iloc[-1]) if len(closes) >= 30  else None
        ema200 = float(closes.ewm(span=200, adjust=False).mean().iloc[-1]) if len(closes) >= 100 else None

        # 52-week range
        n_days      = min(252, len(df))
        high52      = float(df["High"].tail(n_days).max())
        low52       = float(df["Low"].tail(n_days).min())
        rng52       = high52 - low52
        pct_in_rng  = round((curr - low52) / rng52 * 100, 1) if rng52 > 0 else 50.0
        pct_52h     = round((curr - high52) / high52 * 100, 1)

        # Trend
        above = sum([
            1 if curr > ema20 else 0,
            1 if (ema50  and curr > ema50)  else 0,
            1 if (ema200 and curr > ema200) else 0,
        ])
        total_emas = 1 + (1 if ema50 else 0) + (1 if ema200 else 0)

        if above == total_emas:
            trend, trend_signal = "Strong Uptrend", "bullish"
            trend_note = f"Price above all EMAs — strong bullish structure."
        elif above >= total_emas // 2 + 1:
            trend, trend_signal = "Uptrend", "bullish"
            trend_note = "Price above most EMAs — medium-term bullish bias."
        elif above == 0:
            trend, trend_signal = "Downtrend", "bearish"
            trend_note = "Price below all EMAs — bearish structure. Risky long entry."
        else:
            trend, trend_signal = "Mixed", "neutral"
            trend_note = "Mixed EMA signals — trend not clearly defined."

        # Prev day levels
        prev_high = round(float(df["High"].iloc[-2]), 2) if len(df) >= 2 else None
        prev_low  = round(float(df["Low"].iloc[-2]),  2) if len(df) >= 2 else None

        # Entry quality
        if entry_price and entry_price > 0:
            if entry_price >= ema20 * 1.03:
                entry_note = "Bought >3% above EMA-20 — chasing strength, stop well below."
            elif entry_price > ema20:
                entry_note = "Bought just above EMA-20 — momentum entry with trend."
            elif entry_price >= ema20 * 0.97:
                entry_note = "Bought near EMA-20 — potential pullback entry close to dynamic support."
            else:
                entry_note = "Bought below EMA-20 — counter-trend entry; needs strong catalyst."
        else:
            entry_note = None

        # Candle
        candle = _detect_candle_pattern(df) if len(df) >= 2 else {}

        # VIX + NIFTY
        vix    = _yf_close("^INDIAVIX", dt)
        nifty  = _yf_close("^NSEI", dt)
        vix_ctx = _vix_context(vix) if vix else None

        nifty_trend = None
        if nifty:
            try:
                ndf = _fetch_ohlcv("^NSEI", dt, lookback=30)
                if ndf is not None and len(ndf) >= 5:
                    nc = ndf["Close"].astype(float)
                    n_ema20 = float(nc.ewm(span=20, adjust=False).mean().iloc[-1])
                    nifty_trend = "Uptrend" if float(nc.iloc[-1]) > n_ema20 else "Downtrend"
            except Exception:
                pass

        return {
            "symbol":        symbol.upper(),
            "trade_date":    str(trade_date),
            "curr_price":    round(curr, 2),
            "entry_price":   round(entry_price, 2) if entry_price else None,
            "entry_note":    entry_note,
            # Moving averages
            "ema20":         round(ema20, 2),
            "ema50":         round(ema50, 2)  if ema50  else None,
            "ema200":        round(ema200, 2) if ema200 else None,
            # 52-week
            "high52":        round(high52, 2),
            "low52":         round(low52, 2),
            "pct_in_range":  pct_in_rng,    # 0=at 52w low, 100=at 52w high
            "pct_from_52h":  pct_52h,
            # Trend
            "trend":         trend,
            "trend_signal":  trend_signal,
            "trend_note":    trend_note,
            # Prev levels
            "prev_high":     prev_high,
            "prev_low":      prev_low,
            # Candle
            "candle_pattern": candle.get("pattern"),
            "candle_signal":  candle.get("signal"),
            "candle_desc":    candle.get("description"),
            "candle_open":    candle.get("open"),
            "candle_high":    candle.get("high"),
            "candle_low":     candle.get("low"),
            "candle_close":   candle.get("close"),
            "candle_change":  candle.get("change_pct"),
            # Market
            "vix":           round(vix, 2)   if vix   else None,
            "vix_label":     vix_ctx["label"]          if vix_ctx else None,
            "vix_note":      vix_ctx["interpretation"] if vix_ctx else None,
            "nifty":         round(nifty, 2) if nifty else None,
            "nifty_trend":   nifty_trend,
        }
    except Exception:
        return None


def get_fundamentals(symbol: str) -> Optional[dict]:
    """
    Fetch key fundamental data for an NSE-listed equity via yfinance.
    Used for swing trade coaching context.
    Returns None if unavailable. Never raises.
    """
    if not _YF_AVAILABLE:
        return None
    try:
        ticker = yf.Ticker(f"{symbol.upper()}.NS")
        info = ticker.info
        if not info or info.get("quoteType") not in ("EQUITY", "ETF", None):
            # Try without exchange suffix
            ticker = yf.Ticker(symbol.upper())
            info = ticker.info
        if not info:
            return None

        def _round(v, d=2):
            return round(float(v), d) if v is not None else None

        market_cap = info.get("marketCap")
        if market_cap:
            if market_cap >= 2e11:   cap_label = "Large Cap (₹200Cr+)"
            elif market_cap >= 5e10: cap_label = "Mid Cap (₹50–200Cr)"
            else:                    cap_label = "Small Cap (<₹50Cr)"
        else:
            cap_label = None

        pe  = _round(info.get("trailingPE"), 1)
        fpe = _round(info.get("forwardPE"),  1)
        pb  = _round(info.get("priceToBook"), 2)
        ev_ebitda = _round(info.get("enterpriseToEbitda"), 1)
        roe = _round((info.get("returnOnEquity") or 0) * 100, 1) if info.get("returnOnEquity") else None
        roc = _round((info.get("returnOnAssets") or 0) * 100, 1) if info.get("returnOnAssets") else None
        d2e = _round(info.get("debtToEquity"), 2)
        rev_growth = _round((info.get("revenueGrowth") or 0) * 100, 1) if info.get("revenueGrowth") else None
        eps_growth = _round((info.get("earningsGrowth") or 0) * 100, 1) if info.get("earningsGrowth") else None
        eps        = _round(info.get("trailingEps"), 2)
        div_yield  = _round((info.get("dividendYield") or 0) * 100, 2) if info.get("dividendYield") else None
        beta       = _round(info.get("beta"), 2)

        sector   = info.get("sector") or info.get("sectorDisp")
        industry = info.get("industry") or info.get("industryDisp")
        name     = info.get("longName") or info.get("shortName")

        # 52W high/low directly from info (may differ slightly from OHLCV calculation)
        w52_high = _round(info.get("fiftyTwoWeekHigh"))
        w52_low  = _round(info.get("fiftyTwoWeekLow"))

        return {
            "name":         name,
            "sector":       sector,
            "industry":     industry,
            "market_cap":   market_cap,
            "cap_label":    cap_label,
            "pe":           pe,
            "forward_pe":   fpe,
            "pb":           pb,
            "ev_ebitda":    ev_ebitda,
            "eps":          eps,
            "eps_growth":   eps_growth,
            "rev_growth":   rev_growth,
            "roe":          roe,
            "roa":          roc,
            "debt_equity":  d2e,
            "div_yield":    div_yield,
            "beta":         beta,
            "w52_high":     w52_high,
            "w52_low":      w52_low,
        }
    except Exception:
        return None


def market_context_prompt_block(ctx: dict) -> str:
    """Format market context as a prompt block for Claude."""
    if not ctx:
        return ""

    lines = [
        "--- Market Context at Trade Time ---",
        f"India VIX: {ctx['vix']} ({ctx['vix_label']}) — {ctx['vix_note']}",
        f"Underlying Spot: {ctx['underlying']} @ ₹{ctx['spot']:,.2f}",
        f"Option: {ctx['strike']} {ctx['opt_type']} ({ctx['moneyness']}) | DTE: {ctx['dte']} days",
        f"Greeks → Delta: {ctx.get('delta', 'n/a')} | Gamma: {ctx.get('gamma', 'n/a')} "
        f"| Theta: ₹{ctx.get('theta', 'n/a')}/day | Vega: {ctx.get('vega', 'n/a')}",
        "",
        ctx["theta_note"],
    ]

    # Chart context block
    if ctx.get("candle_pattern"):
        lines += [
            "",
            "--- Chart Context (Daily) ---",
            f"Day: {ctx.get('day_of_week', '')} — {ctx.get('day_note', '')}",
            f"Candlestick: {ctx['candle_pattern']} ({ctx.get('candle_signal','').upper()}) — {ctx.get('candle_desc','')}",
            f"OHLC: O:{ctx.get('candle_open')} H:{ctx.get('candle_high')} L:{ctx.get('candle_low')} C:{ctx.get('candle_close')} ({ctx.get('candle_change','')}%)",
            f"Trend: {ctx.get('trend','')} — {ctx.get('trend_note','')}",
        ]
        if ctx.get("key_level"):
            lines.append(f"Key Level: {ctx['key_level']}")

    lines += [
        "",
        "Use the candlestick pattern and trend to assess whether the trader entered with or against the market structure.",
        "If they bought a CE on a bearish candle day in a downtrend, flag this as a counter-trend trade.",
        "If they bought a PE on a bearish candle day in a downtrend, note it as a trend-aligned trade.",
        "---",
    ]
    # Remove old duplicate return below
    return "\n".join(lines)

