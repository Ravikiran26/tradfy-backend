"""
Tests for broker CSV parsers and the AI-powered generic CSV fallback.

Covered:
- _detect_instrument: equity / options / futures for all symbol patterns
- _detect_broker: recognises Zerodha/Upstox/Dhan/Angel One/Groww columns
- parse_zerodha tradebook: equity swing, F&O options, futures (FIFO-matched pairs)
- parse_upstox P&L: options, equity
- parse_dhan P&L: options, equity, futures
- parse_generic_csv: full flow with mocked Claude (no real API call)
- parse_broker_file: routes correctly, falls through to generic on unknown broker
"""

import io
import csv
import json
import pytest
import pandas as pd
from unittest.mock import patch

import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from services.trade_parser import (
    _detect_broker,
    _detect_instrument,
    parse_zerodha,
    parse_generic_csv,
    parse_broker_file,
    _read_file,
    _norm_cols,
)


# ── Helpers ───────────────────────────────────────────────────────────────────

def _make_csv_bytes(rows: list[dict]) -> bytes:
    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=rows[0].keys())
    writer.writeheader()
    writer.writerows(rows)
    return buf.getvalue().encode()


def _df(rows: list[dict]) -> pd.DataFrame:
    return pd.DataFrame(rows)


# ── _detect_broker ────────────────────────────────────────────────────────────

class TestDetectBroker:

    def test_detects_zerodha_tradebook(self):
        df = _df([{"order_execution_time": "2024-01-01", "trade_id": "1", "trade_type": "buy", "symbol": "RELIANCE"}])
        assert _detect_broker(df) == "zerodha"

    def test_detects_upstox(self):
        df = _df([{"instrument name": "NIFTY CE", "buy_avg": 100, "sell_avg": 120}])
        assert _detect_broker(df) == "upstox"

    def test_detects_dhan(self):
        df = _df([{"trade no.": "T001", "description": "NIFTY", "series": "CE"}])
        assert _detect_broker(df) == "dhan"

    def test_detects_angelone(self):
        df = _df([{"net instrument": "NIFTY", "buy rate": 100, "sell rate": 120, "net p&l": 500}])
        assert _detect_broker(df) == "angelone"

    def test_detects_groww(self):
        df = _df([{"realised p&l": 500, "symbol": "INFY"}])
        assert _detect_broker(df) == "groww"

    def test_returns_none_for_unknown(self):
        df = _df([{"Script Name": "RELIANCE", "Buy Price": 2000, "Sell Price": 2100, "Profit": 100}])
        assert _detect_broker(df) is None


# ── parse_generic_csv (mocked Claude) ────────────────────────────────────────

# Simulates an unknown broker (e.g. ICICI Direct / HDFC Securities)
ICICI_ROWS = [
    {"Script Name": "RELIANCE",    "Transaction": "BUY",  "Qty": "10", "Buy Price": "2000", "Sell Price": "2100", "P&L": "1000",  "Trade Date": "15-01-2024"},
    {"Script Name": "NIFTY22600CE","Transaction": "BUY",  "Qty": "50", "Buy Price": "120",  "Sell Price": "85",   "P&L": "-1750", "Trade Date": "18-01-2024"},
    {"Script Name": "INFY",        "Transaction": "SELL", "Qty": "5",  "Buy Price": "1500", "Sell Price": "1480", "P&L": "-100",  "Trade Date": "20-01-2024"},
]

ICICI_MAPPING = {
    "Script Name":   "symbol",
    "Transaction":   "action",
    "Qty":           "quantity",
    "Buy Price":     "entry_price",
    "Sell Price":    "exit_price",
    "P&L":           "pnl",
    "Trade Date":    "trade_date",
}

HDFC_ROWS = [
    {"Security": "BANKNIFTY FUT", "Side": "B", "Volume": "25", "Avg Buy": "47500", "Avg Sell": "47800", "Realised PnL": "7500", "Date": "2024-03-14"},
    {"Security": "TCS",            "Side": "S", "Volume": "2",  "Avg Buy": "3900",  "Avg Sell": "3850",  "Realised PnL": "-100",  "Date": "2024-03-15"},
]

HDFC_MAPPING = {
    "Security":     "symbol",
    "Side":         "action",
    "Volume":       "quantity",
    "Avg Buy":      "entry_price",
    "Avg Sell":     "exit_price",
    "Realised PnL": "pnl",
    "Date":         "trade_date",
}


class TestParseGenericCsv:

    def test_icici_style_csv_basic_fields(self):
        df = _df(ICICI_ROWS)
        with patch("services.claude.infer_csv_mapping", return_value=ICICI_MAPPING):
            trades = parse_generic_csv(df)

        assert len(trades) == 3

        reliance = trades[0]
        assert reliance["symbol"] == "RELIANCE"
        assert reliance["entry_price"] == 2000.0
        assert reliance["exit_price"] == 2100.0
        assert reliance["pnl"] == 1000.0
        assert reliance["quantity"] == 10
        assert reliance["trade_date"] == "2024-01-15"
        assert reliance["status"] == "closed"
        assert reliance["instrument_type"] == "equity"

    def test_options_instrument_detected_from_ce_suffix(self):
        df = _df(ICICI_ROWS)
        with patch("services.claude.infer_csv_mapping", return_value=ICICI_MAPPING):
            trades = parse_generic_csv(df)

        nifty = trades[1]
        assert nifty["symbol"] == "NIFTY22600CE"
        assert nifty["instrument_type"] == "options"
        assert nifty["pnl"] == -1750.0

    def test_hdfc_style_csv(self):
        df = _df(HDFC_ROWS)
        with patch("services.claude.infer_csv_mapping", return_value=HDFC_MAPPING):
            trades = parse_generic_csv(df)

        assert len(trades) == 2

        bnf = trades[0]
        assert bnf["symbol"] == "BANKNIFTY FUT"
        assert bnf["instrument_type"] == "futures"
        assert bnf["pnl"] == 7500.0
        assert bnf["trade_date"] == "2024-03-14"

    def test_skips_rows_with_empty_symbol(self):
        rows = [
            {"Script": "RELIANCE", "P&L": "500"},
            {"Script": "",         "P&L": "200"},
            {"Script": None,       "P&L": "100"},
        ]
        mapping = {"Script": "symbol", "P&L": "pnl"}
        df = _df(rows)
        with patch("services.claude.infer_csv_mapping", return_value=mapping):
            trades = parse_generic_csv(df)
        assert len(trades) == 1
        assert trades[0]["symbol"] == "RELIANCE"

    def test_raises_if_mapping_empty(self):
        df = _df(ICICI_ROWS)
        with patch("services.claude.infer_csv_mapping", return_value={}):
            with pytest.raises(ValueError, match="Could not determine column mapping"):
                parse_generic_csv(df)

    def test_pnl_with_comma_formatting(self):
        rows = [{"Scrip": "RELIANCE", "Net P/L": "1,25,000", "Date": "01-01-2024"}]
        mapping = {"Scrip": "symbol", "Net P/L": "pnl", "Date": "trade_date"}
        df = _df(rows)
        with patch("services.claude.infer_csv_mapping", return_value=mapping):
            trades = parse_generic_csv(df)
        assert trades[0]["pnl"] == 125000.0

    def test_action_normalisation(self):
        rows = [
            {"Sym": "INFY", "Side": "B",    "PnL": "100"},
            {"Sym": "TCS",  "Side": "buy",  "PnL": "200"},
            {"Sym": "HDFC", "Side": "S",    "PnL": "-50"},
            {"Sym": "WIPRO","Side": "sell", "PnL": "-80"},
        ]
        mapping = {"Sym": "symbol", "Side": "action", "PnL": "pnl"}
        df = _df(rows)
        with patch("services.claude.infer_csv_mapping", return_value=mapping):
            trades = parse_generic_csv(df)
        assert trades[0]["action"] == "buy"
        assert trades[1]["action"] == "buy"
        assert trades[2]["action"] == "sell"
        assert trades[3]["action"] == "sell"

    def test_date_formats_parsed(self):
        formats = [
            ("15-01-2024", "2024-01-15"),
            ("2024-01-15", "2024-01-15"),
            ("15/01/2024", "2024-01-15"),
            ("15 Jan 2024", "2024-01-15"),
            ("15-Jan-2024", "2024-01-15"),
        ]
        for raw, expected in formats:
            rows = [{"Sym": "RELIANCE", "Date": raw, "PnL": "100"}]
            mapping = {"Sym": "symbol", "Date": "trade_date", "PnL": "pnl"}
            df = _df(rows)
            with patch("services.claude.infer_csv_mapping", return_value=mapping):
                trades = parse_generic_csv(df)
            assert trades[0]["trade_date"] == expected, f"Failed for format: {raw}"


# ── parse_broker_file routing ─────────────────────────────────────────────────

class TestParseBrokerFileRouting:

    def test_falls_through_to_generic_for_unknown_broker(self):
        csv_bytes = _make_csv_bytes(ICICI_ROWS)
        with patch("services.claude.infer_csv_mapping", return_value=ICICI_MAPPING):
            trades = parse_broker_file(csv_bytes, "icici_trades.csv", "icici")
        assert len(trades) == 3
        assert trades[0]["symbol"] == "RELIANCE"

    def test_falls_through_to_generic_for_auto_broker(self):
        csv_bytes = _make_csv_bytes(ICICI_ROWS)
        with patch("services.claude.infer_csv_mapping", return_value=ICICI_MAPPING):
            trades = parse_broker_file(csv_bytes, "trades.csv", "auto")
        assert len(trades) == 3

    def test_auto_detects_zerodha_from_columns(self):
        rows = [
            {
                "trade_id": "T001", "trade_type": "buy", "order_execution_time": "2024-01-15 09:30:00",
                "symbol": "RELIANCE EQ", "isin": "INE002A01018", "exchange": "NSE",
                "segment": "EQ", "series": "EQ", "trade_type_": "buy",
                "auction": "N", "quantity": "10", "price": "2000", "order_id": "ORD001",
                "order_type": "LIMIT",
            }
        ]
        csv_bytes = _make_csv_bytes(rows)
        # Should auto-detect zerodha and NOT call Claude at all
        with patch("services.claude.infer_csv_mapping") as mock_claude:
            try:
                parse_broker_file(csv_bytes, "zerodha.csv", "fyers")
            except Exception:
                pass  # parsing may fail on incomplete data — what matters is Claude wasn't called
            mock_claude.assert_not_called()


# ── _detect_instrument ────────────────────────────────────────────────────────

class TestDetectInstrument:

    # Equity — stock names that contain CE/PE letters must NOT be options
    def test_reliance_is_equity(self):
        assert _detect_instrument("RELIANCE") == "equity"

    def test_persistent_is_equity(self):
        # PERSISTENT contains "PE" and "SISTE" — should not be options
        assert _detect_instrument("PERSISTENT") == "equity"

    def test_price_like_symbols_are_equity(self):
        assert _detect_instrument("PRICOL") == "equity"

    # Options — must have digit before CE/PE
    def test_nifty_call_option(self):
        assert _detect_instrument("NIFTY22600CE") == "options"

    def test_nifty_put_option(self):
        assert _detect_instrument("NIFTY22400PE") == "options"

    def test_banknifty_option(self):
        assert _detect_instrument("BANKNIFTY48000CE") == "options"

    def test_sensex_option(self):
        assert _detect_instrument("SENSEX75300PE") == "options"

    # Options via extra/segment field
    def test_extra_ce_segment(self):
        assert _detect_instrument("NIFTY", "CE") == "options"

    def test_extra_pe_segment(self):
        assert _detect_instrument("BANKNIFTY", "PE") == "options"

    def test_extra_opt_segment(self):
        assert _detect_instrument("NIFTY", "OPT") == "options"

    # Futures
    def test_banknifty_future(self):
        assert _detect_instrument("BANKNIFTYFUT") == "futures"

    def test_fut_in_symbol(self):
        assert _detect_instrument("NIFTY 27 MAR FUT") == "futures"

    def test_extra_fut_segment(self):
        assert _detect_instrument("NIFTY", "FUT") == "futures"


# ── parse_zerodha tradebook ───────────────────────────────────────────────────

def _zerodha_leg(symbol, inst_type, action, qty, price, date="2024-01-15",
                 strike=None, option_type=None):
    return {
        "trade_id": "T001", "trade_type": action,
        "instrument_type": inst_type,
        "symbol": symbol, "expiry": "", "isin": "",
        "strike": strike or "", "option_type": option_type or "",
        "quantity": qty, "price": price,
        "order_execution_time": f"{date} 09:30:00",
        "exchange": "NSE", "segment": "NFO", "series": "",
        "auction": "N", "order_id": "O001", "order_type": "LIMIT",
    }


class TestParseZerodhaTradebook:

    def test_equity_swing_matched_pair(self):
        rows = [
            _zerodha_leg("RELIANCE EQ", "EQ", "buy",  10, 2000, "2024-01-10"),
            _zerodha_leg("RELIANCE EQ", "EQ", "sell", 10, 2150, "2024-01-15"),
        ]
        df = _norm_cols(_df(rows))
        trades = parse_zerodha(df)
        assert len(trades) == 1
        t = trades[0]
        assert "RELIANCE" in t["symbol"]
        assert t["instrument_type"] == "equity"
        assert t["entry_price"] == 2000.0
        assert t["exit_price"] == 2150.0
        assert t["pnl"] == pytest.approx(1500.0)
        assert t["status"] == "closed"
        assert t["broker"] == "Zerodha"

    def test_options_intraday_matched_pair(self):
        rows = [
            _zerodha_leg("NIFTY", "CE", "buy",  50, 120.0, "2024-01-18", 22600, "CE"),
            _zerodha_leg("NIFTY", "CE", "sell", 50, 85.0,  "2024-01-18", 22600, "CE"),
        ]
        df = _norm_cols(_df(rows))
        trades = parse_zerodha(df)
        assert len(trades) == 1
        t = trades[0]
        assert t["instrument_type"] == "options"
        assert t["pnl"] == pytest.approx(-1750.0)

    def test_futures_matched_pair(self):
        rows = [
            _zerodha_leg("BANKNIFTY", "FUT", "buy",  25, 47500, "2024-03-01"),
            _zerodha_leg("BANKNIFTY", "FUT", "sell", 25, 47800, "2024-03-14"),
        ]
        df = _norm_cols(_df(rows))
        trades = parse_zerodha(df)
        assert len(trades) == 1
        t = trades[0]
        assert t["instrument_type"] == "futures"
        assert t["pnl"] == pytest.approx(7500.0)

    def test_open_buy_leg_creates_no_closed_trade(self):
        rows = [
            _zerodha_leg("TCS EQ", "EQ", "buy", 5, 3900, "2024-03-15"),
        ]
        df = _norm_cols(_df(rows))
        trades = parse_zerodha(df)
        # Only a buy leg — no matched sell, so no closed trade
        assert all(t["status"] == "open" for t in trades)

    def test_multiple_symbols_matched_independently(self):
        rows = [
            _zerodha_leg("RELIANCE EQ", "EQ", "buy",  10, 2000),
            _zerodha_leg("INFY EQ",     "EQ", "buy",  5,  1500),
            _zerodha_leg("RELIANCE EQ", "EQ", "sell", 10, 2100),
            _zerodha_leg("INFY EQ",     "EQ", "sell", 5,  1450),
        ]
        df = _norm_cols(_df(rows))
        trades = parse_zerodha(df)
        closed = [t for t in trades if t["status"] == "closed"]
        assert len(closed) == 2
        symbols = {t["symbol"] for t in closed}
        assert "RELIANCE EQ" in symbols or any("RELIANCE" in s for s in symbols)


# ── parse_upstox P&L format ───────────────────────────────────────────────────

class TestParseUpstoxPnL:

    def _make_upstox_pnl_df(self, rows):
        df = _df(rows)
        return _norm_cols(df)

    def test_options_row(self):
        from services.trade_parser import parse_upstox
        rows = [{
            "Scrip Name": "NIFTY", "Symbol": "NIFTY22600CE", "Scrip Opt": "CE",
            "Qty": "50", "Buy Date": "18-01-2024", "Buy Rate": "120",
            "Sell Date": "18-01-2024", "Sell Rate": "85",
            "Total PL": "-1750", "Strike Price": "22600",
        }]
        df = self._make_upstox_pnl_df(rows)
        trades = parse_upstox(df)
        assert len(trades) == 1
        t = trades[0]
        assert t["instrument_type"] == "options"
        assert t["pnl"] == pytest.approx(-1750.0)
        assert t["entry_price"] == pytest.approx(120.0)
        assert t["exit_price"] == pytest.approx(85.0)
        assert t["broker"] == "Upstox"

    def test_equity_row(self):
        from services.trade_parser import parse_upstox
        rows = [{
            "Scrip Name": "RELIANCE", "Symbol": "RELIANCE", "Scrip Opt": "EQ",
            "Qty": "10", "Buy Date": "10-01-2024", "Buy Rate": "2000",
            "Sell Date": "15-01-2024", "Sell Rate": "2150",
            "Total PL": "1500", "Strike Price": "",
        }]
        df = self._make_upstox_pnl_df(rows)
        trades = parse_upstox(df)
        assert len(trades) == 1
        t = trades[0]
        assert t["instrument_type"] == "equity"
        assert t["pnl"] == pytest.approx(1500.0)


# ── parse_dhan ────────────────────────────────────────────────────────────────
# Dhan Trade History CSV: Exchange, Symbol, Series, Trade Date, Trade Time,
# Trade No., Order No., Description, Buy/Sell, Qty, Price, Trade Value

def _dhan_leg(symbol, series, side, qty, price, date="2024-01-18"):
    return {
        "Exchange": "NSE", "Symbol": symbol, "Series": series,
        "Trade Date": date, "Trade Time": "09:30:00",
        "Trade No.": "T001", "Order No.": "O001",
        "Description": symbol, "Buy/Sell": side,
        "Qty": qty, "Price": price, "Trade Value": qty * price,
    }


class TestParseDhan:

    def test_options_matched_pair(self):
        from services.trade_parser import parse_dhan
        rows = [
            _dhan_leg("NIFTY22600CE", "CE", "B", 50, 120.0),
            _dhan_leg("NIFTY22600CE", "CE", "S", 50, 85.0),
        ]
        df = _norm_cols(_df(rows))
        trades = parse_dhan(df)
        closed = [t for t in trades if t["status"] == "closed"]
        assert len(closed) >= 1
        t = closed[0]
        assert t["instrument_type"] == "options"
        assert t["pnl"] == pytest.approx(-1750.0)

    def test_equity_matched_pair(self):
        from services.trade_parser import parse_dhan
        rows = [
            _dhan_leg("RELIANCE", "EQ", "B", 10, 2000.0, "2024-01-10"),
            _dhan_leg("RELIANCE", "EQ", "S", 10, 2150.0, "2024-01-15"),
        ]
        df = _norm_cols(_df(rows))
        trades = parse_dhan(df)
        closed = [t for t in trades if t["status"] == "closed"]
        assert len(closed) >= 1
        t = closed[0]
        assert t["instrument_type"] == "equity"
        assert t["pnl"] == pytest.approx(1500.0)

    def test_futures_matched_pair(self):
        from services.trade_parser import parse_dhan
        rows = [
            _dhan_leg("BANKNIFTYFUT", "FUT", "B", 25, 47500.0, "2024-03-01"),
            _dhan_leg("BANKNIFTYFUT", "FUT", "S", 25, 47800.0, "2024-03-14"),
        ]
        df = _norm_cols(_df(rows))
        trades = parse_dhan(df)
        closed = [t for t in trades if t["status"] == "closed"]
        assert len(closed) >= 1
        t = closed[0]
        assert t["instrument_type"] == "futures"
        assert t["pnl"] == pytest.approx(7500.0)
