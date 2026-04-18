"""
Tests for analysis/dom_liquidity_shift.py

Covers: liquidity shift computation, adverse detection, DB query.
"""
import sqlite3
import pytest
from analysis.dom_liquidity_shift import (
    compute_liquidity_shift,
    detect_adverse_liquidity,
    detect_favorable_liquidity,
    query_recent_depth_snapshots,
    analyze_dom_liquidity,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_snapshot(run_id, bid_total, ask_total):
    return {
        "run_id": run_id,
        "event_utc": "2026-04-04T10:00:00Z",
        "bid_levels": [{"price": 2300.0, "size": bid_total, "level_index": 0}],
        "ask_levels": [{"price": 2301.0, "size": ask_total, "level_index": 0}],
        "bid_total_size": bid_total,
        "ask_total_size": ask_total,
    }


# ---------------------------------------------------------------------------
# 1. Liquidity Shift Computation
# ---------------------------------------------------------------------------

class TestComputeLiquidityShift:
    def test_bid_draining(self):
        snapshots = [
            _make_snapshot("run3", 50, 100),   # latest: bid dropped
            _make_snapshot("run2", 80, 100),
            _make_snapshot("run1", 100, 100),  # earliest: bid was 100
        ]
        shift = compute_liquidity_shift(snapshots)
        assert shift["bid_size_change_pct"] < 0
        assert shift["bid_wall_shift"] == "draining"
        assert shift["ask_wall_shift"] == "stable"

    def test_ask_building(self):
        snapshots = [
            _make_snapshot("run3", 100, 200),  # latest: ask doubled
            _make_snapshot("run2", 100, 150),
            _make_snapshot("run1", 100, 100),  # earliest
        ]
        shift = compute_liquidity_shift(snapshots)
        assert shift["ask_size_change_pct"] > 0
        assert shift["ask_wall_shift"] == "building"

    def test_stable_market(self):
        snapshots = [
            _make_snapshot("run2", 100, 100),
            _make_snapshot("run1", 105, 95),
        ]
        shift = compute_liquidity_shift(snapshots)
        assert shift["bid_wall_shift"] == "stable"
        assert shift["ask_wall_shift"] == "stable"

    def test_single_snapshot(self):
        shift = compute_liquidity_shift([_make_snapshot("run1", 100, 100)])
        assert shift["n_snapshots"] == 1
        assert shift["bid_wall_shift"] == "unknown"

    def test_empty_snapshots(self):
        shift = compute_liquidity_shift([])
        assert shift["n_snapshots"] == 0

    def test_imbalance_trend(self):
        snapshots = [
            _make_snapshot("run2", 60, 140),   # latest: bearish imbalance
            _make_snapshot("run1", 120, 80),   # earliest: bullish imbalance
        ]
        shift = compute_liquidity_shift(snapshots)
        assert shift["imbalance_trend"] < 0  # turned bearish


# ---------------------------------------------------------------------------
# 2. Adverse Liquidity Detection
# ---------------------------------------------------------------------------

class TestDetectAdverseLiquidity:
    def test_long_adverse_bid_draining(self):
        shift = {
            "bid_wall_shift": "draining",
            "ask_wall_shift": "building",
            "liquidity_score": -2,
            "imbalance_trend": -0.15,
        }
        result = detect_adverse_liquidity(shift, "long")
        assert result["is_adverse"] is True
        assert result["severity"] == "severe"
        assert result["adverse_score"] == 3
        assert "bid_support_draining" in result["reasons"]

    def test_short_adverse_ask_draining(self):
        shift = {
            "bid_wall_shift": "building",
            "ask_wall_shift": "draining",
            "liquidity_score": 2,
            "imbalance_trend": 0.15,
        }
        result = detect_adverse_liquidity(shift, "short")
        assert result["is_adverse"] is True
        assert result["severity"] == "severe"

    def test_no_adverse(self):
        shift = {
            "bid_wall_shift": "stable",
            "ask_wall_shift": "stable",
            "liquidity_score": 0,
            "imbalance_trend": 0.0,
        }
        result = detect_adverse_liquidity(shift, "long")
        assert result["is_adverse"] is False
        assert result["severity"] == "none"
        assert result["recommendation"] == "hold"

    def test_mild_adverse(self):
        shift = {
            "bid_wall_shift": "draining",
            "ask_wall_shift": "stable",
            "liquidity_score": -1,
            "imbalance_trend": 0.0,
        }
        result = detect_adverse_liquidity(shift, "long")
        assert result["is_adverse"] is True
        assert result["severity"] == "mild"
        assert result["recommendation"] == "tighten_stop"


# ---------------------------------------------------------------------------
# 2b. Favorable Liquidity Detection
# ---------------------------------------------------------------------------

class TestDetectFavorableLiquidity:
    def test_long_favorable_bid_building_ask_draining(self):
        shift = {
            "bid_wall_shift": "building",
            "ask_wall_shift": "draining",
            "liquidity_score": 2,
            "imbalance_trend": 0.15,
        }
        result = detect_favorable_liquidity(shift, "long")
        assert result["is_favorable"] is True
        assert result["strength"] == "strong"
        assert result["favorable_score"] == 3
        assert "bid_support_building" in result["reasons"]
        assert result["recommendation"] == "trail_wide"

    def test_short_favorable_ask_building_bid_draining(self):
        shift = {
            "bid_wall_shift": "draining",
            "ask_wall_shift": "building",
            "liquidity_score": -2,
            "imbalance_trend": -0.15,
        }
        result = detect_favorable_liquidity(shift, "short")
        assert result["is_favorable"] is True
        assert result["strength"] == "strong"
        assert result["favorable_score"] == 3

    def test_not_favorable_stable(self):
        shift = {
            "bid_wall_shift": "stable",
            "ask_wall_shift": "stable",
            "liquidity_score": 0,
            "imbalance_trend": 0.0,
        }
        result = detect_favorable_liquidity(shift, "long")
        assert result["is_favorable"] is False
        assert result["strength"] == "none"
        assert result["recommendation"] == "hold"

    def test_moderate_favorable(self):
        shift = {
            "bid_wall_shift": "building",
            "ask_wall_shift": "draining",
            "liquidity_score": 2,
            "imbalance_trend": 0.05,  # not strong enough for +1
        }
        result = detect_favorable_liquidity(shift, "long")
        assert result["is_favorable"] is True
        assert result["strength"] == "moderate"
        assert result["favorable_score"] == 2
        assert result["recommendation"] == "extend_tp"

    def test_mild_favorable_not_enough(self):
        shift = {
            "bid_wall_shift": "building",
            "ask_wall_shift": "stable",
            "liquidity_score": 1,
            "imbalance_trend": 0.0,
        }
        result = detect_favorable_liquidity(shift, "long")
        assert result["is_favorable"] is False
        assert result["strength"] == "mild"
        assert result["favorable_score"] == 1

    def test_full_pipeline_includes_favorable(self):
        """analyze_dom_liquidity returns favorable key."""
        conn = sqlite3.connect(":memory:")
        conn.row_factory = sqlite3.Row
        conn.execute("""
            CREATE TABLE ctrader_depth_quotes (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                run_id TEXT, account_id INTEGER, symbol_id INTEGER,
                symbol TEXT, quote_id INTEGER, side TEXT,
                price REAL, size REAL, level_index INTEGER,
                event_utc TEXT, event_ts REAL, raw_json TEXT DEFAULT '{}'
            )
        """)
        import time
        now_ts = time.time()
        # Run 1: balanced
        for side, price in [("bid", 2300.0), ("ask", 2301.0)]:
            conn.execute(
                "INSERT INTO ctrader_depth_quotes (run_id, symbol, side, price, size, level_index, event_utc, event_ts) VALUES (?,?,?,?,?,?,?,?)",
                ("run1", "BTCUSD", side, price, 100, 0, "2026-04-05", now_ts - 600),
            )
        # Run 2: bids building, asks draining (favorable for longs)
        conn.execute(
            "INSERT INTO ctrader_depth_quotes (run_id, symbol, side, price, size, level_index, event_utc, event_ts) VALUES (?,?,?,?,?,?,?,?)",
            ("run2", "BTCUSD", "bid", 2300.0, 200, 0, "2026-04-05", now_ts - 60),
        )
        conn.execute(
            "INSERT INTO ctrader_depth_quotes (run_id, symbol, side, price, size, level_index, event_utc, event_ts) VALUES (?,?,?,?,?,?,?,?)",
            ("run2", "BTCUSD", "ask", 2301.0, 50, 0, "2026-04-05", now_ts - 60),
        )
        conn.commit()
        result = analyze_dom_liquidity(conn, symbol="BTCUSD", direction="long", lookback_min=60)
        assert result["ok"] is True
        assert "favorable" in result
        assert result["favorable"]["is_favorable"] is True


# ---------------------------------------------------------------------------
# 3. DB Query (in-memory)
# ---------------------------------------------------------------------------

class TestQueryDepthSnapshots:
    def _setup_depth_db(self, conn):
        conn.execute("""
            CREATE TABLE ctrader_depth_quotes (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                run_id TEXT, account_id INTEGER, symbol_id INTEGER,
                symbol TEXT, quote_id INTEGER, side TEXT,
                price REAL, size REAL, level_index INTEGER,
                event_utc TEXT, event_ts REAL, raw_json TEXT DEFAULT '{}'
            )
        """)

    def test_query_with_data(self):
        conn = sqlite3.connect(":memory:")
        conn.row_factory = sqlite3.Row
        self._setup_depth_db(conn)
        import time
        now_ts = time.time()
        for i, run_id in enumerate(["run1", "run2"]):
            for side, price in [("bid", 2300.0), ("bid", 2299.0), ("ask", 2301.0), ("ask", 2302.0)]:
                conn.execute(
                    "INSERT INTO ctrader_depth_quotes (run_id, symbol, side, price, size, level_index, event_utc, event_ts) VALUES (?,?,?,?,?,?,?,?)",
                    (run_id, "XAUUSD", side, price, 100 + i * 50, 0, "2026-04-04", now_ts - (1 - i) * 300),
                )
        conn.commit()
        snapshots = query_recent_depth_snapshots(conn, symbol="XAUUSD", lookback_min=60)
        assert len(snapshots) == 2
        assert snapshots[0]["bid_total_size"] > 0
        assert snapshots[0]["ask_total_size"] > 0

    def test_query_empty_db(self):
        conn = sqlite3.connect(":memory:")
        conn.row_factory = sqlite3.Row
        snapshots = query_recent_depth_snapshots(conn, symbol="XAUUSD")
        assert snapshots == []

    def test_full_pipeline(self):
        conn = sqlite3.connect(":memory:")
        conn.row_factory = sqlite3.Row
        self._setup_depth_db(conn)
        import time
        now_ts = time.time()
        # Run 1: balanced
        for side, price in [("bid", 2300.0), ("ask", 2301.0)]:
            conn.execute(
                "INSERT INTO ctrader_depth_quotes (run_id, symbol, side, price, size, level_index, event_utc, event_ts) VALUES (?,?,?,?,?,?,?,?)",
                ("run1", "XAUUSD", side, price, 100, 0, "2026-04-04", now_ts - 600),
            )
        # Run 2: bids draining
        conn.execute(
            "INSERT INTO ctrader_depth_quotes (run_id, symbol, side, price, size, level_index, event_utc, event_ts) VALUES (?,?,?,?,?,?,?,?)",
            ("run2", "XAUUSD", "bid", 2300.0, 30, 0, "2026-04-04", now_ts - 60),
        )
        conn.execute(
            "INSERT INTO ctrader_depth_quotes (run_id, symbol, side, price, size, level_index, event_utc, event_ts) VALUES (?,?,?,?,?,?,?,?)",
            ("run2", "XAUUSD", "ask", 2301.0, 150, 0, "2026-04-04", now_ts - 60),
        )
        conn.commit()
        result = analyze_dom_liquidity(conn, symbol="XAUUSD", direction="long", lookback_min=60)
        assert result["ok"] is True
        assert result["adverse"]["is_adverse"] is True  # bid draining for longs
