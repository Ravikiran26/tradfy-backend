import anthropic
import base64
import json
import os
from typing import Optional

client = anthropic.Anthropic(api_key=os.getenv("ANTHROPIC_API_KEY"))

_EXTRACT_PROMPT = """You are a trade data extractor for Indian stock brokers.
Analyze this broker screenshot (Zerodha, Upstox, Angel One, Groww, Fyers, etc.) and extract ALL visible trades.

IMPORTANT: The screenshot may show ONE trade or MULTIPLE trades (e.g. a P&L summary screen).
Extract every trade you can see. Always return a JSON array, even for a single trade.

Return ONLY a valid JSON array where each element has these exact keys:
[
  {
    "symbol": "stock/contract symbol e.g. RELIANCE, NIFTY23DECFUT, NIFTY22600CE",
    "instrument_type": "equity | futures | options | currency | commodity",
    "trade_type": "detect automatically — see rules below",
    "action": "buy | sell",
    "status": "open | closed — see rules below",
    "quantity": integer or null,
    "entry_price": float or null,
    "exit_price": float or null,
    "pnl": float in INR or null,
    "pnl_percent": float as percentage or null,
    "trade_date": "YYYY-MM-DD" or null,
    "trade_time": "HH:MM" in 24h IST if visible on screenshot (order time, execution time), else null,
    "broker": "broker name or null",
    "sector": "IT | Banking | Pharma | Auto | FMCG | Energy | Metals | Telecom | Realty | null",
    "overnight_charges": float in INR or null
  }
]

trade_type detection rules (pick exactly one):
- "options_intraday"    — CE/PE suffix visible with MIS product type, same-day entry and exit
- "options_scalping"    — CE/PE suffix, MIS, and trade duration appears very short (seconds to a few minutes) — infer from time if visible
- "options_positional"  — CE/PE suffix with NRML product type, or options held overnight (exit date differs from entry date)
- "equity_swing"        — product type is CNC or Delivery, or stock with no expiry date visible
- "futures_swing"       — product type is NRML, or contract has FUT suffix with an expiry date
- default to "options_intraday" only if type cannot be determined and it looks like an intraday options trade

status rules:
- "open"   — if this is a buy order confirmation or a position still open
- "closed" — if this is a sell order, exit, or a completed P&L screen

For P&L summary screens (multiple trades listed):
- entry_price = Buy avg value shown
- exit_price  = Sell avg value shown
- pnl         = Gross P&L value shown (positive for profit, negative for loss)
- status      = "closed" for all completed P&L entries

sector detection rules (null if not identifiable):
- IT: Infosys, TCS, Wipro, HCL, Tech Mahindra, LTI, Mphasis, Coforge, Persistent
- Banking: HDFC Bank, ICICI Bank, SBI, Kotak, Axis, IndusInd, Bandhan, Federal, Nifty Bank, BankNifty, Bsx
- Pharma: Sun Pharma, Dr Reddy, Cipla, Divi's, Lupin, Biocon, Mankind, Torrent
- Auto: Maruti, Tata Motors, M&M, Bajaj Auto, Hero MotoCorp, Eicher, TVS
- FMCG: HUL, ITC, Nestle, Britannia, Dabur, Godrej Consumer, Marico, Colgate
- Energy: Reliance, ONGC, BPCL, IOC, NTPC, Power Grid, Adani Green, Tata Power
- Metals: Tata Steel, JSW, Hindalco, Vedanta, SAIL, NMDC, Coal India
- Telecom: Bharti Airtel, Jio Financial, Vodafone Idea
- Realty: DLF, Godrej Properties, Prestige, Sobha, Brigade
- For Nifty index options (NIFTY CE/PE): sector = null

General rules:
- Use null for any field not clearly visible
- pnl must be positive for profit, negative for loss
- Do not guess values not shown
- Return raw JSON array only, no markdown, no explanation"""


_OPTIONS_SCALPING_PROMPT = """You are a trading mentor coaching an Indian retail trader on quick options trades. Your reader has 1-2 years experience but no formal finance education.

BANNED WORDS — never use these: theta, gamma, delta, vega, Greeks, DTE, OTM, ITM, ATM, directional conviction, whipsaw, premium decay, volatility sensitivity, bearish structure
Instead say: "daily time decay" not theta | "days left before expiry" not DTE | "far from Nifty price" not OTM | "market was falling" not bearish structure

Trade data:
{trade_data}

{market_context}

Give exactly 3 insights with plain English titles like "You traded at the worst time of day" not "Open Auction Risk":
1. Did brokerage + STT (taxes on each trade) eat into the profit or make the loss worse? Use ₹ numbers.
2. Was the entry time risky? (First 15 min = very unpredictable. Last 30 min = options drop fast.)
3. Did they hold too long or exit at the right time?

After 3 insights add exactly:
🔴 Key Mistake: [one plain sentence]
✅ Do Better: [one simple action]
Under 130 words. End with: "⚠️ Not investment advice."
"""


_OPTIONS_POSITIONAL_PROMPT = """You are a trading mentor coaching an Indian retail trader who held an options position overnight. Your reader has 1-2 years experience, no formal finance education.

BANNED WORDS — never use: theta, gamma, delta, vega, Greeks, DTE, OTM, ITM, ATM, IV crush, premium decay, volatility sensitivity, directional conviction
Instead say: "options lose value every day even if market doesn't move" not theta | "days left before expiry" not DTE | "fear/volatility index" not VIX without explanation

Trade data:
{trade_data}

{market_context}

Give exactly 3 insights with plain English titles like "Options lose money just by sitting overnight" not "Theta Decay Analysis":
1. How much money was lost just from time passing (options shrink in value every day even if Nifty doesn't move)?
2. Did they buy when options were expensive (VIX — India's fear index — was high = options cost more)?
3. Did a small loss become big by holding overnight? Was the quantity too large for an overnight bet?

After 3 insights add exactly:
🔴 Key Mistake: [one plain sentence]
✅ Do Better: [one simple action]
Under 130 words. End with: "⚠️ Not investment advice."
"""


_FEEDBACK_PROMPT = """You are a trading mentor coaching an Indian retail trader. Your target reader is someone who has traded for 1-2 years but never studied finance formally.

STRICT LANGUAGE RULES — follow these exactly:
- BANNED words/phrases (do NOT use): theta, gamma, delta, vega, DTE, Greeks, directional conviction, whipsaw, volatility sensitivity, bearish structure, premium decay, OTM, ITM, ATM, delta swings
- If you need to reference theta, say: "the daily cost of holding this option (options lose value every day even if the market doesn't move)"
- If you need to reference DTE, say: "only X days left before this option expires worthless"
- If you need to reference OTM, say: "the strike price was far from where Nifty was trading"
- Insight titles must sound like a friend talking, not a textbook: "You fought the market direction" not "Counter-Trend Entry in Hostile Greeks Environment"

Trade data:
{trade_data}

{market_context}

Provide exactly 3 coaching insights. Pick the 3 most relevant:
- Was the market going against the trade direction?
- Was there only a day or two left before expiry (very risky)?
- Was the trade size too big with no stop loss?
- Did they hold too long or exit too early?
- Was VIX high (options were expensive to buy)?

Format:
- Number each insight (1. 2. 3.) with a short plain-English title
- Each insight: 2-3 simple sentences with actual numbers from the trade
- After the 3 insights, add exactly:
  🔴 Key Mistake: [one sentence, plain English, specific to this trade]
  ✅ Do Better: [one simple action for next time]
- Under 160 words total
- End with: "⚠️ Not investment advice."
"""


_ENTRY_OBSERVATION_PROMPT = """You are a trading coach reviewing an Indian trader's entry into a swing position.
This trade is still OPEN — do not comment on P&L or outcome.

Trade entry details:
{trade_data}

Provide exactly 2 brief entry observations:
1. One observation on entry context (price level, sector momentum, or instrument choice — based only on what is visible)
2. One observation on risk setup (position size relative to price, or missing information like stop loss)

Rules:
- Never say buy, sell, hold, or exit
- No price targets or predictions
- Under 60 words total
- Number each observation (1. 2.)
- End with exactly: "Position is open. No exit advice implied."
"""


_SWING_FEEDBACK_PROMPT = """You are a trading coach reviewing an Indian retail trader's swing trade. Your job is to give honest, clear feedback in plain simple English — like a knowledgeable friend, not a finance textbook.

Trade details:
{trade_data}

Market context at entry:
{swing_context}

Fundamental data:
{fundamentals}

Trader's historical performance:
{user_history}

LANGUAGE RULES — follow strictly:
- Write as if explaining to someone who has traded for 1 year but never studied finance
- NO jargon: say "the stock was falling" not "bearish structure"; say "options lose value daily" not "theta decay"; say "overbought" is fine but explain it
- Use ₹ numbers from the actual trade in every insight
- Keep sentences short — max 20 words each
- Give credit when the trade setup was actually good — not every trade is a mistake

YOUR JOB: Look at the data and pick the 3 most important things about THIS specific trade.

Choose 3 that matter most from:
- Position too large (₹ amount at risk vs their average)
- Entry was chased — bought too high above the 20-day moving average
- Stock was already falling when they entered
- Company fundamentals are weak — high debt or not making real profits
- This stock is small and hard to sell quickly in a panic
- Trader keeps losing in this sector (use their history if available)
- Held too long — open longer than their usual winning trades
- Good entry — price was near support, trend was intact (give credit)
- Well-sized position — risk was controlled (give credit)

Return ONLY a valid JSON object, no other text before or after:
{{
  "verdict": "HIGH RISK" or "BE CAREFUL" or "LOOKS CLEAN",
  "summary": "One plain sentence — the single most important thing about this trade (max 15 words)",
  "insights": [
    {{
      "severity": "critical" or "warning" or "positive",
      "title": "Short plain English title, 6 words max, like talking to a friend",
      "body": "2-3 short sentences. Use actual ₹ numbers and % from the trade. No jargon."
    }},
    {{
      "severity": "critical" or "warning" or "positive",
      "title": "...",
      "body": "..."
    }},
    {{
      "severity": "critical" or "warning" or "positive",
      "title": "...",
      "body": "..."
    }}
  ],
  "key_mistake": "The single biggest mistake — one sentence, plain English, specific to this trade",
  "do_better": "One concrete thing to do differently next time — simple and measurable"
}}

verdict guide:
- HIGH RISK: position sizing problem, broken trend at entry, or fundamentals are very weak
- BE CAREFUL: some concerns but not all bad — mixed signals
- LOOKS CLEAN: good entry, reasonable size, trend was intact"""


def extract_trades_from_screenshot(
    image_bytes: bytes, media_type: str
) -> list[dict]:
    """
    Send broker screenshot to Claude Vision and extract ALL visible trades.
    Always returns a list — single trade screenshots return a list of one.
    """
    image_b64 = base64.standard_b64encode(image_bytes).decode("utf-8")

    message = client.messages.create(
        model="claude-haiku-4-5-20251001",
        max_tokens=2048,
        messages=[
            {
                "role": "user",
                "content": [
                    {
                        "type": "image",
                        "source": {
                            "type": "base64",
                            "media_type": media_type,
                            "data": image_b64,
                        },
                    },
                    {
                        "type": "text",
                        "text": _EXTRACT_PROMPT,
                    },
                ],
            }
        ],
    )

    raw = message.content[0].text.strip()

    # Strip markdown code fences if present
    if raw.startswith("```"):
        raw = raw.split("```")[1]
        if raw.startswith("json"):
            raw = raw[4:]
    raw = raw.strip()

    parsed = json.loads(raw)

    # Normalise to always be a list
    if isinstance(parsed, dict):
        parsed = [parsed]
    elif not isinstance(parsed, list):
        parsed = []

    return [t for t in parsed if isinstance(t, dict) and t.get("symbol")]


# Keep old name as alias so existing callers don't break
def extract_trade_from_screenshot(image_bytes: bytes, media_type: str) -> Optional[dict]:
    trades = extract_trades_from_screenshot(image_bytes, media_type)
    return trades[0] if trades else {}


def _build_trade_summary(trade_data: dict, extra_labels: Optional[dict] = None) -> str:
    """Build a human-readable trade summary string for use in prompts."""
    field_labels = {
        "symbol": "Symbol",
        "instrument_type": "Instrument",
        "trade_type": "Trade Type",
        "action": "Action",
        "status": "Status",
        "quantity": "Quantity",
        "entry_price": "Entry Price (₹)",
        "exit_price": "Exit Price (₹)",
        "pnl": "P&L (₹)",
        "pnl_percent": "P&L %",
        "trade_date": "Trade Date",
        "broker": "Broker",
        "sector": "Sector",
        "overnight_charges": "Overnight Charges (₹)",
    }
    if extra_labels:
        field_labels.update(extra_labels)

    lines = []
    for key, label in field_labels.items():
        val = trade_data.get(key)
        if val is not None:
            lines.append(f"{label}: {val}")
    return "\n".join(lines) if lines else "No trade data available"


def _build_history_summary(user_history: dict) -> str:
    """Build a human-readable user history summary for swing feedback prompts."""
    if not user_history:
        return "No prior swing trade history available."

    lines = []
    total_swing = user_history.get("total_swing_trades", 0)

    if total_swing == 0:
        return "No prior swing trades logged — patterns cannot yet be identified."

    lines.append(f"Total swing trades logged: {total_swing}")
    lines.append(f"Swing win rate: {user_history.get('swing_win_rate', 0):.1f}%")

    overnight_total = user_history.get("overnight_total", 0)
    if overnight_total > 0:
        overnight_wins = user_history.get("overnight_wins", 0)
        overnight_wr = round((overnight_wins / overnight_total) * 100, 1)
        lines.append(f"Overnight/positional trades: {overnight_total} (win rate: {overnight_wr}%)")

    sector_stats = user_history.get("sector_stats", {})
    if sector_stats:
        sector_lines = []
        for sector, data in sector_stats.items():
            t = data.get("total", 0)
            w = data.get("wins", 0)
            if t > 0:
                sector_lines.append(f"{sector} ({w}/{t} wins)")
        if sector_lines:
            lines.append(f"Sector performance: {', '.join(sector_lines)}")

    avg_winner = user_history.get("avg_winner_pnl")
    avg_loser = user_history.get("avg_loser_pnl")
    if avg_winner is not None:
        lines.append(f"Avg winning trade P&L: ₹{avg_winner:,.0f}")
    if avg_loser is not None:
        lines.append(f"Avg losing trade P&L: ₹{avg_loser:,.0f}")

    return "\n".join(lines)


def generate_trade_feedback(trade_data: dict, market_context: Optional[dict] = None) -> str:
    """Send trade data to Claude and get 3 coaching insights. Routes by trade_type."""
    from services.market_data import market_context_prompt_block
    trade_summary = _build_trade_summary(trade_data)
    ctx_block = market_context_prompt_block(market_context) if market_context else ""

    trade_type = trade_data.get("trade_type", "options_intraday")
    if trade_type == "options_scalping":
        prompt = _OPTIONS_SCALPING_PROMPT.format(
            trade_data=trade_summary,
            market_context=ctx_block,
        )
    elif trade_type == "options_positional":
        prompt = _OPTIONS_POSITIONAL_PROMPT.format(
            trade_data=trade_summary,
            market_context=ctx_block,
        )
    else:
        prompt = _FEEDBACK_PROMPT.format(
            trade_data=trade_summary,
            market_context=ctx_block,
        )

    message = client.messages.create(
        model="claude-haiku-4-5-20251001",
        max_tokens=320,
        messages=[{"role": "user", "content": prompt}],
    )

    return message.content[0].text.strip()


def generate_entry_observation(trade_data: dict) -> str:
    """
    Generate 2 brief entry observations for a newly opened swing position.
    No P&L feedback — trade is still open.
    """
    trade_summary = _build_trade_summary(trade_data)

    message = client.messages.create(
        model="claude-haiku-4-5-20251001",
        max_tokens=160,
        messages=[
            {
                "role": "user",
                "content": _ENTRY_OBSERVATION_PROMPT.format(trade_data=trade_summary),
            }
        ],
    )

    return message.content[0].text.strip()


def generate_session_feedback(trades: list[dict]) -> str:
    """
    Generate a session-level coaching summary for a multi-trade upload.
    Covers overall session P&L, win rate, and 2-3 pattern observations.
    """
    total = len(trades)
    wins = sum(1 for t in trades if (t.get("pnl") or 0) > 0)
    total_pnl = sum(t.get("pnl") or 0 for t in trades)
    symbols = ", ".join(t.get("symbol", "?") for t in trades)

    summary = (
        f"Session: {total} trades logged — {symbols}\n"
        f"Winners: {wins}/{total}\n"
        f"Net P&L: ₹{total_pnl:,.2f}\n\n"
        "Individual trades:\n"
    )
    for t in trades:
        pnl = t.get("pnl")
        pnl_str = f"₹{pnl:+,.2f}" if pnl is not None else "P&L unknown"
        summary += f"- {t.get('symbol')}: {pnl_str} ({t.get('pnl_percent', '')}%)\n"

    prompt = (
        "You are a trading coach reviewing an Indian retail trader's session.\n\n"
        f"{summary}\n"
        "Give exactly 3 coaching observations about this session:\n"
        "1. One on trade selection or diversity of strikes/instruments\n"
        "2. One on session P&L management (did they cut winners early, hold losers, etc.)\n"
        "3. One on risk pattern (position sizing across trades, concentration risk)\n\n"
        "Rules: no buy/sell advice, number each point (1. 2. 3.).\n"
        "After the 3 observations, add exactly these two lines:\n"
        "🔴 Key Mistake: [the single biggest process mistake across this session]\n"
        "✅ Do Better: [one concrete change to improve the next session]\n"
        "Under 130 words total. End with: '⚠️ Not investment advice.'"
    )

    message = client.messages.create(
        model="claude-haiku-4-5-20251001",
        max_tokens=420,
        messages=[{"role": "user", "content": prompt}],
    )
    return message.content[0].text.strip()


def _build_swing_context_block(swing_ctx: Optional[dict]) -> str:
    """Format swing context dict into a prompt-ready block."""
    if not swing_ctx:
        return "No live market context available."

    lines = []

    curr = swing_ctx.get("curr_price")
    entry = swing_ctx.get("entry_price")
    if curr is not None:
        lines.append(f"Current price: ₹{curr:,.2f}")
    if entry is not None and curr is not None:
        chg = ((curr - entry) / entry) * 100
        sign = "+" if chg >= 0 else ""
        lines.append(f"Price vs entry: {sign}{chg:.1f}%")

    trend = swing_ctx.get("trend")
    trend_note = swing_ctx.get("trend_note")
    if trend:
        lines.append(f"Trend: {trend}" + (f" — {trend_note}" if trend_note else ""))

    ema20 = swing_ctx.get("ema20")
    ema50 = swing_ctx.get("ema50")
    ema200 = swing_ctx.get("ema200")
    if ema20 is not None:
        pos = "above" if (curr or 0) > ema20 else "below"
        lines.append(f"EMA-20: ₹{ema20:,.2f} (current price is {pos})")
    if ema50 is not None:
        pos = "above" if (curr or 0) > ema50 else "below"
        lines.append(f"EMA-50: ₹{ema50:,.2f} (current price is {pos})")
    if ema200 is not None:
        pos = "above" if (curr or 0) > ema200 else "below"
        lines.append(f"EMA-200: ₹{ema200:,.2f} (current price is {pos})")

    pct_range = swing_ctx.get("pct_in_range")
    high52 = swing_ctx.get("high52")
    low52 = swing_ctx.get("low52")
    pct_from_52h = swing_ctx.get("pct_from_52h")
    if pct_range is not None and high52 is not None and low52 is not None:
        lines.append(f"52-week range: ₹{low52:,.2f} – ₹{high52:,.2f} ({pct_range:.0f}% from low, {pct_from_52h:.1f}% from 52W high)")

    entry_note = swing_ctx.get("entry_note")
    if entry_note:
        lines.append(f"Entry quality: {entry_note}")

    candle = swing_ctx.get("candle_pattern")
    candle_sig = swing_ctx.get("candle_signal")
    candle_desc = swing_ctx.get("candle_desc")
    if candle:
        lines.append(f"Last candle: {candle} ({candle_sig})" + (f" — {candle_desc}" if candle_desc else ""))

    vix = swing_ctx.get("vix")
    vix_label = swing_ctx.get("vix_label")
    vix_note = swing_ctx.get("vix_note")
    if vix is not None:
        lines.append(f"VIX: {vix:.1f} ({vix_label})" + (f" — {vix_note}" if vix_note else ""))

    nifty = swing_ctx.get("nifty")
    nifty_trend = swing_ctx.get("nifty_trend")
    if nifty is not None:
        lines.append(f"NIFTY: {nifty:,.2f} (trend: {nifty_trend or 'unknown'})")

    return "\n".join(lines) if lines else "No market context available."


def _build_fundamentals_block(fund: Optional[dict]) -> str:
    """Format fundamentals dict into a prompt-ready block."""
    if not fund:
        return "No fundamental data available."

    def _fmt(v, suffix=""):
        return f"{v}{suffix}" if v is not None else "n/a"

    lines = []
    if fund.get("cap_label"):
        lines.append(f"Market cap: {fund['cap_label']}")
    if fund.get("sector"):
        lines.append(f"Sector: {fund['sector']}" + (f" | Industry: {fund['industry']}" if fund.get("industry") else ""))
    if fund.get("pe") is not None:
        fpe = f" (Fwd P/E: {fund['forward_pe']}x)" if fund.get("forward_pe") else ""
        lines.append(f"P/E (TTM): {fund['pe']}x{fpe}")
    if fund.get("pb") is not None:
        lines.append(f"P/B: {fund['pb']}x")
    if fund.get("ev_ebitda") is not None:
        lines.append(f"EV/EBITDA: {fund['ev_ebitda']}x")
    if fund.get("eps") is not None:
        lines.append(f"EPS (TTM): ₹{fund['eps']}" + (f" | EPS growth: {fund['eps_growth']}%" if fund.get("eps_growth") is not None else ""))
    if fund.get("rev_growth") is not None:
        lines.append(f"Revenue growth (YoY): {fund['rev_growth']}%")
    if fund.get("roe") is not None:
        lines.append(f"ROE: {fund['roe']}%")
    if fund.get("debt_equity") is not None:
        lines.append(f"Debt/Equity: {fund['debt_equity']}x")
    if fund.get("beta") is not None:
        lines.append(f"Beta: {fund['beta']} (vs NIFTY)")
    if fund.get("div_yield") is not None:
        lines.append(f"Dividend yield: {fund['div_yield']}%")

    return "\n".join(lines) if lines else "No fundamental data available."


def generate_swing_feedback(
    trade_data: dict,
    user_history: dict,
    swing_ctx: Optional[dict] = None,
    fundamentals: Optional[dict] = None,
) -> str:
    """
    Generate plain-English coaching for equity_swing or futures_swing trades.
    Returns a JSON string with verdict, 3 insights with severity, key mistake, and do better.
    Falls back to raw text if JSON parsing fails.
    """
    trade_summary = _build_trade_summary(trade_data)
    history_summary = _build_history_summary(user_history)
    ctx_block = _build_swing_context_block(swing_ctx)
    fund_block = _build_fundamentals_block(fundamentals)

    message = client.messages.create(
        model="claude-haiku-4-5-20251001",
        max_tokens=800,
        messages=[
            {
                "role": "user",
                "content": _SWING_FEEDBACK_PROMPT.format(
                    trade_data=trade_summary,
                    swing_context=ctx_block,
                    fundamentals=fund_block,
                    user_history=history_summary,
                ),
            }
        ],
    )

    raw = message.content[0].text.strip()

    # Strip markdown code fences if Claude wrapped the JSON
    if raw.startswith("```"):
        raw = raw.split("```")[1]
        if raw.startswith("json"):
            raw = raw[4:]
        raw = raw.strip()

    # Validate it's parseable JSON before storing — fallback to raw text if not
    try:
        parsed = json.loads(raw)
        # Ensure required fields exist
        if "verdict" in parsed and "insights" in parsed:
            return json.dumps(parsed, ensure_ascii=False)
    except (json.JSONDecodeError, TypeError):
        pass

    return raw


_AUTOPSY_LOSS_PROMPT = """You are a post-trade analyst doing a forensic review of a losing trade for an Indian retail trader.

Trade data:
{trade_data}

{market_context}

This trade resulted in a LOSS. Do not recommend any future trades or securities.
Analyze retrospectively what went wrong and what better execution of THIS trade could have looked like.

Provide exactly these 6 labeled lines (one line each, no paragraph breaks between them):

📍 Entry Timing: [was the entry price/timing poor given VIX, DTE, Greeks, or candle structure? What made entry high-risk?]
🎯 Better Entry: [retrospectively, what session condition or price zone would have given a lower-risk entry on this same trade? Reference candle OHLC or VIX if available. Backward-looking only.]
🚪 Exit Discipline: [did a small loss become large? At what approximate price/% loss should the exit have been taken to limit damage?]
🏃 Better Exit: [what was the earliest clear signal that the trade was failing? Reference price level or structure if visible.]
⚙️ Primary Failure: [one-line diagnosis — structure break, theta decay, IV crush, wrong direction, or poor sizing?]
🛡️ Risk Lesson: [one concrete process change for next time — stop loss %, position size, or environment filter]

Rules:
- Never say buy, sell, or give price targets for future trades
- All analysis must be backward-looking on this specific completed trade only
- Reference actual numbers (entry ₹, exit ₹, VIX, DTE, theta, P&L%, candle H/L) where available
- Under 160 words total
- No markdown, no # headers, no ** bold — plain text only
- End with: "⚠️ Not investment advice."
"""

_AUTOPSY_PROFIT_PROMPT = """You are a post-trade analyst reviewing a profitable trade for an Indian retail trader.

Trade data:
{trade_data}

{market_context}

This trade resulted in a PROFIT. Analyze retrospectively how entry and profit capture could have been optimized.

Provide exactly these 6 labeled lines (one line each, no paragraph breaks between them):

🎯 Entry Quality: [was the entry price/timing optimal? Reference VIX, DTE, candle structure at entry. Could a slightly different entry have improved the risk/reward?]
📈 Trailing Stop Analysis: [given entry ₹X and exit ₹Y — estimate what trailing stop % would have captured more profit. Calculate approximate extra ₹ if possible.]
⏰ Exit Timing: [was the exit too early or well-timed? What % of the available move was captured based on entry vs exit vs candle range?]
💰 Profit Capture: [how efficiently was the move captured? Did theta or delta work in the trader's favour during the hold?]
🏁 Better Exit: [retrospectively, what price level or signal would have indicated a better exit point to capture more of the move?]
⚡ Optimization: [one specific process improvement — scale out in parts, use trailing stop after X% gain, or hold through key level]

Rules:
- Never give price targets or recommendations for future trades
- All analysis is retrospective on this specific completed trade only
- Reference actual numbers (entry ₹, exit ₹, P&L%, VIX, DTE, candle H/L) where available
- Under 160 words total
- No markdown, no # headers, no ** bold — plain text only
- End with: "⚠️ Not investment advice."
"""


def generate_trade_autopsy(trade_data: dict, market_context: Optional[dict] = None) -> str:
    """
    Deep post-trade autopsy.
    Loss: entry failure, exit discipline, primary failure cause, risk lesson.
    Profit: trailing stop simulation, exit timing, profit capture %, optimization tip.
    """
    from services.market_data import market_context_prompt_block
    trade_summary = _build_trade_summary(trade_data)
    ctx_block = market_context_prompt_block(market_context) if market_context else ""

    pnl = trade_data.get("pnl") or 0
    prompt_template = _AUTOPSY_PROFIT_PROMPT if pnl >= 0 else _AUTOPSY_LOSS_PROMPT

    message = client.messages.create(
        model="claude-haiku-4-5-20251001",
        max_tokens=600,
        messages=[
            {
                "role": "user",
                "content": prompt_template.format(
                    trade_data=trade_summary,
                    market_context=ctx_block,
                ),
            }
        ],
    )
    return message.content[0].text.strip()
