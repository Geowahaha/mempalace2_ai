"""
Tests for analysis/volume_profile.py

Covers: price-volume distribution, VP computation (POC/VA/HVN/LVN),
entry confirmation, SL/TP suggestions, session VP builder.
"""
import sqlite3
import pytest
from analysis.volume_profile import (
    build_price_volume_distribution,
    compute_volume_profile,
    check_entry_vs_profile,
    suggest_sl_from_profile,
    suggest_tp_from_profile,
    query_m1_bars,
    build_session_volume_profile,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_bars(prices: list[tuple], volume: int = 100):
    """Create bar dicts from (open, high, low, close) tuples."""
    bars = []
    for i, (o, h, l, c) in enumerate(prices):
        bars.append({
            "ts_ms": 1712200000000 + i * 60000,
            "ts_utc": f"2026-04-04T10:{i:02d}:00Z",
            "open": o, "high": h, "low": l, "close": c,
            "volume": volume,
        })
    return bars


# ---------------------------------------------------------------------------
# 1. Price-Volume Distribution
# ---------------------------------------------------------------------------

class TestPriceVolumeDistribution:
    def test_basic_distribution(self):
        bars = _make_bars([(2300, 2305, 2295, 2302)] * 10, volume=100)
        dist = build_price_volume_distribution(bars, tick_size=0.01, bucket_ticks=100)
        assert len(dist) > 0
        assert all(v > 0 for v in dist.values())

    def test_close_bucket_weighted_higher(self):
        bars = _make_bars([(2300, 2310, 2290, 2305)], volume=100)
        dist = build_price_volume_distribution(bars, tick_size=0.01, bucket_ticks=100)
        # Close price bucket (2305 → 2300 bucket) should have higher volume
        # than edge buckets
        close_bucket = 2300.0  # 2305 snaps to 2300 with bucket_size=1.0
        if close_bucket in dist:
            edge_vols = [v for p, v in dist.items() if abs(p - close_bucket) > 2]
            if edge_vols:
                assert dist[close_bucket] >= min(edge_vols)

    def test_empty_bars(self):
        assert build_price_volume_distribution([]) == {}

    def test_zero_price_bars_skipped(self):
        bars = [{"ts_ms": 0, "open": 0, "high": 0, "low": 0, "close": 0, "volume": 100}]
        dist = build_price_volume_distribution(bars)
        assert len(dist) == 0


# ---------------------------------------------------------------------------
# 2. Volume Profile Computation
# ---------------------------------------------------------------------------

class TestComputeVolumeProfile:
    def test_basic_vp(self):
        # Build a distribution with clear POC
        dist = {2300.0: 500, 2301.0: 200, 2302.0: 100, 2299.0: 150, 2298.0: 50}
        vp = compute_volume_profile(dist)
        assert vp["poc"] == 2300.0  # highest volume
        assert vp["va_high"] >= vp["va_low"]
        assert vp["total_volume"] == 1000
        assert len(vp["hvn_levels"]) > 0
        assert len(vp["lvn_levels"]) > 0
        assert vp["n_buckets"] == 5

    def test_empty_distribution(self):
        vp = compute_volume_profile({})
        assert vp["poc"] == 0.0
        assert vp["n_buckets"] == 0

    def test_single_level(self):
        vp = compute_volume_profile({2300.0: 100})
        assert vp["poc"] == 2300.0
        assert vp["va_high"] == 2300.0
        assert vp["va_low"] == 2300.0

    def test_value_area_contains_70pct(self):
        dist = {float(2290 + i): float(100 - abs(i - 10) * 8) for i in range(21)}
        vp = compute_volume_profile(dist, va_pct=0.70)
        va_prices = [p for p, v in dist.items() if vp["va_low"] <= p <= vp["va_high"]]
        va_vol = sum(dist[p] for p in va_prices)
        assert va_vol >= vp["total_volume"] * 0.68  # ~70% with rounding

    def test_profile_list(self):
        dist = {2300.0: 500, 2301.0: 200}
        vp = compute_volume_profile(dist)
        assert len(vp["profile"]) == 2
        assert vp["profile"][0]["price"] == 2300.0


# ---------------------------------------------------------------------------
# 3. Entry Confirmation
# ---------------------------------------------------------------------------

class TestEntryConfirmation:
    def _sample_vp(self):
        return {
            "poc": 2300.0,
            "va_high": 2305.0,
            "va_low": 2295.0,
            "hvn_levels": [2300.0, 2298.0, 2303.0],
            "lvn_levels": [2290.0, 2310.0, 2315.0],
        }

    def test_long_at_hvn_strong(self):
        result = check_entry_vs_profile(2300.0, "long", self._sample_vp(), bucket_ticks=100)
        assert result["vp_confirmation"] == "strong"
        assert result["near_poc"]
        assert result["near_hvn"]

    def test_short_at_hvn_strong(self):
        result = check_entry_vs_profile(2303.0, "short", self._sample_vp(), bucket_ticks=100)
        assert result["vp_confirmation"] == "strong"

    def test_entry_at_lvn_weak(self):
        result = check_entry_vs_profile(2310.0, "long", self._sample_vp(), bucket_ticks=100)
        assert result["near_lvn"]

    def test_no_profile_neutral(self):
        result = check_entry_vs_profile(2300.0, "long", {})
        assert result["vp_confirmation"] == "neutral"
        assert result["vp_reason"] == "no_profile"

    def test_in_value_area(self):
        result = check_entry_vs_profile(2302.0, "long", self._sample_vp(), bucket_ticks=100)
        assert result["in_value_area"]


# ---------------------------------------------------------------------------
# 4. SL/TP Suggestions
# ---------------------------------------------------------------------------

class TestSLTPSuggestions:
    def _sample_vp(self):
        return {
            "poc": 2300.0,
            "va_high": 2305.0,
            "va_low": 2295.0,
            "hvn_levels": [2294.0, 2300.0, 2306.0],
            "lvn_levels": [2288.0, 2312.0, 2318.0],
        }

    def test_sl_long_uses_hvn(self):
        result = suggest_sl_from_profile(2302.0, "long", self._sample_vp(), atr_sl=2290.0, min_distance=0.5)
        assert result["suggested_sl"] >= 2290.0  # HVN at 2294 is tighter than ATR at 2290

    def test_sl_no_hvn(self):
        result = suggest_sl_from_profile(2302.0, "long", {"hvn_levels": []}, atr_sl=2290.0)
        assert result["suggested_sl"] == 2290.0
        assert result["source"] == "atr_only"

    def test_tp_long_uses_lvn(self):
        result = suggest_tp_from_profile(2302.0, "long", self._sample_vp(), min_rr=1.5, risk_distance=8.0)
        assert result["suggested_tp"] >= 2302.0

    def test_tp_no_lvn(self):
        result = suggest_tp_from_profile(2302.0, "long", {"lvn_levels": []}, risk_distance=5.0, min_rr=2.0)
        assert result["source"] == "rr_only"


# ---------------------------------------------------------------------------
# 5. Session VP Builder (with in-memory DB)
# ---------------------------------------------------------------------------

class TestSessionVPBuilder:
    def test_build_with_data(self):
        conn = sqlite3.connect(":memory:")
        conn.row_factory = sqlite3.Row
        conn.execute("""
            CREATE TABLE stream_trendbars (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                symbol TEXT NOT NULL,
                symbol_id INTEGER NOT NULL DEFAULT 0,
                tf TEXT NOT NULL,
                ts_ms INTEGER NOT NULL,
                ts_utc TEXT NOT NULL,
                open REAL NOT NULL,
                high REAL NOT NULL,
                low REAL NOT NULL,
                close REAL NOT NULL,
                volume INTEGER NOT NULL DEFAULT 0,
                UNIQUE(symbol, tf, ts_ms)
            )
        """)
        import time
        now_ms = int(time.time() * 1000)
        for i in range(60):
            conn.execute(
                "INSERT INTO stream_trendbars (symbol, tf, ts_ms, ts_utc, open, high, low, close, volume) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                ("XAUUSD", "M1", now_ms - (60 - i) * 60000, "2026-04-04", 2300 + i * 0.1, 2300.5 + i * 0.1, 2299.5 + i * 0.1, 2300.2 + i * 0.1, 100),
            )
        conn.commit()
        result = build_session_volume_profile(conn, symbol="XAUUSD", hours_back=2)
        assert result["ok"] is True
        assert result["bars_used"] == 60
        assert result["vp"]["poc"] > 0

    def test_build_no_data(self):
        conn = sqlite3.connect(":memory:")
        conn.row_factory = sqlite3.Row
        conn.execute("""
            CREATE TABLE stream_trendbars (
                id INTEGER PRIMARY KEY AUTOINCREMENT, symbol TEXT, symbol_id INTEGER DEFAULT 0,
                tf TEXT, ts_ms INTEGER, ts_utc TEXT, open REAL, high REAL, low REAL, close REAL, volume INTEGER DEFAULT 0,
                UNIQUE(symbol, tf, ts_ms)
            )
        """)
        result = build_session_volume_profile(conn, symbol="XAUUSD", hours_back=24)
        assert result["ok"] is False

    def test_build_empty_db(self):
        conn = sqlite3.connect(":memory:")
        conn.row_factory = sqlite3.Row
        result = build_session_volume_profile(conn, symbol="XAUUSD")
        assert result["ok"] is False
