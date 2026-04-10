from pydantic import BaseModel
from typing import Optional
from datetime import date, datetime
import uuid


class Trade(BaseModel):
    id: Optional[uuid.UUID] = None
    user_id: Optional[uuid.UUID] = None
    symbol: Optional[str] = None
    instrument_type: Optional[str] = None  # equity, futures, options, currency, commodity
    trade_type: Optional[str] = None       # options_intraday, equity_swing, futures_swing
    action: Optional[str] = None           # buy, sell
    status: Optional[str] = None           # open, closed
    quantity: Optional[int] = None
    entry_price: Optional[float] = None
    exit_price: Optional[float] = None
    pnl: Optional[float] = None
    pnl_percent: Optional[float] = None
    trade_date: Optional[date] = None
    trade_time: Optional[str] = None          # "HH:MM" IST execution time from screenshot
    broker: Optional[str] = None
    sector: Optional[str] = None           # IT, Banking, Pharma, Auto, FMCG, Energy, Metals, Telecom, Realty
    overnight_charges: Optional[float] = None
    linked_trade_id: Optional[uuid.UUID] = None  # open trade this close is linked to
    holding_days: Optional[int] = None           # calculated on close
    closed_at: Optional[datetime] = None
    ai_feedback: Optional[str] = None
    created_at: Optional[datetime] = None


class TradeStats(BaseModel):
    total_trades: int
    win_rate: float          # percentage 0-100
    total_pnl: float
    avg_profit: float        # avg P&L on winning trades
    avg_loss: float          # avg P&L on losing trades (negative value)


class FeedbackResponse(BaseModel):
    trade: Trade
    feedback: str


class MultiTradeResponse(BaseModel):
    trades: list[Trade]
    feedback: str
    count: int


class TradeUpdate(BaseModel):
    """
    Partial update payload for an existing trade.
    All fields are optional — only non-None values are written.
    """
    symbol: Optional[str] = None
    exit_price: Optional[float] = None
    pnl: Optional[float] = None
    pnl_percent: Optional[float] = None
    trade_date: Optional[date] = None
    status: Optional[str] = None
    sector: Optional[str] = None
    broker: Optional[str] = None
    quantity: Optional[int] = None
    overnight_charges: Optional[float] = None
    holding_days: Optional[int] = None
    closed_at: Optional[datetime] = None
    ai_feedback: Optional[str] = None


class ClosePositionRequest(BaseModel):
    """
    Body sent to POST /trades/{trade_id}/close (JSON path, not multipart).
    Use when the caller already knows exit_price without uploading a screenshot.
    For screenshot-based close, use the multipart upload endpoint instead.
    """
    exit_price: float
    exit_date: Optional[date] = None       # defaults to today if omitted
    overnight_charges: Optional[float] = None


class OpenPosition(BaseModel):
    """An open swing trade with days_held calculated server-side."""
    trade: Trade
    days_held: int


class SectorStat(BaseModel):
    total: int
    wins: int
    win_rate: float


class SwingPatterns(BaseModel):
    sector_win_rate: dict[str, SectorStat]   # keyed by sector name
    avg_holding_days_winners: Optional[float]
    avg_holding_days_losers: Optional[float]
    dead_money_positions: list[Trade]         # open positions held > 2× avg winner hold time
    panic_sell_count: int                     # closed in < 2 days with a loss


class TimeSlotStat(BaseModel):
    total: int
    wins: int
    win_rate: float


class OptionsPatterns(BaseModel):
    expiry_day_win_rate: Optional[float]      # win rate when trade_date is a Thursday
    non_expiry_win_rate: Optional[float]
    expiry_day_trades: int
    time_slot_win_rate: dict[str, TimeSlotStat]  # keyed by "HH-HH" slot, based on created_at hour
    total_options_trades: int
