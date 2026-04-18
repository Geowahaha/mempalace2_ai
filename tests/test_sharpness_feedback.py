"""
Tests for learning/sharpness_feedback.py

Covers:
- Sharpness extraction from request_json
- Sharpness-outcome correlation computation
- Weight auto-calibration recommendations
- Family performance decay detection
- Full report builder + formatter
"""
import json
import sqlite3
import pytest
from unittest.mock import patch

from learning.sharpness_feedback import (
    _extract_sharpness_from_meta,
    _point_biserial_correlation,
    query_resolved_trades_with_sharpness,
    compute_sharpness_correlation,
    compute_weight_recommendations,
    detect_family_decay,
    build_sharpness_feedback_report,
    format_sharpness_feedback_text,
    SHARPNESS_DIMENSIONS,
    DIMENSION_TO_WEIGHT_KEY,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_request_json(sharpness_score=60, band="normal", dims=None):
    """Build a request_json string embedding sharpness in xau_openapi_entry_router."""
    dim_defaults = {
        "momentum_quality": 55.0,
        "flow_persistence": 60.0,
        "absorption_quality": 50.0,
        "price_stability": 65.0,
        "positioning_quality": 58.0,
    }
    if dims:
        dim_defaults.update(dims)
    sharpness = {
        "sharpness_score": sharpness_score,
        "sharpness_band": band,
        "sharpness_reasons": ["test"],
        **dim_defaults,
    }
    return json.dumps({
        "raw_scores": {
            "xau_openapi_entry_router": {
                "sharpness": sharpness,
            }
        }
    })


def _setup_db(conn: sqlite3.Connection, trades: list[dict]):
    """Create tables and insert test trade data."""
    conn.execute("""
        CREATE TABLE IF NOT EXISTS execution_journal (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            direction TEXT,
            source TEXT,
            symbol TEXT DEFAULT 'XAUUSD',
            confidence REAL DEFAULT 0.0,
            request_json TEXT DEFAULT '{}'
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS ctrader_deals (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            journal_id INTEGER,
            outcome INTEGER,
            pnl_usd REAL,
            execution_utc TEXT,
            has_close_detail INTEGER DEFAULT 1
        )
    """)
    for t in trades:
        cur = conn.execute(
            "INSERT INTO execution_journal (direction, source, symbol, confidence, request_json) VALUES (?, ?, ?, ?, ?)",
            (
                t.get("direction", "long"),
                t.get("source", "scalp_xauusd:xau_scalp_pullback_limit:canary"),
                t.get("symbol", "XAUUSD"),
                t.get("confidence", 5.0),
                t.get("request_json", _make_request_json()),
            ),
        )
        jid = cur.lastrowid
        conn.execute(
            "INSERT INTO ctrader_deals (journal_id, outcome, pnl_usd, execution_utc, has_close_detail) VALUES (?, ?, ?, ?, 1)",
            (
                jid,
                t.get("outcome", 1),
                t.get("pnl_usd", 1.5),
                t.get("execution_utc", "2026-04-04 10:00:00"),
            ),
        )
    conn.commit()


# ---------------------------------------------------------------------------
# 1. Extraction tests
# ---------------------------------------------------------------------------

class TestExtractSharpness:
    def test_valid_extraction(self):
        rj = _make_request_json(sharpness_score=72, band="sharp")
        result = _extract_sharpness_from_meta(rj)
        assert result is not None
        assert result["sharpness_score"] == 72
        assert result["sharpness_band"] == "sharp"

    def test_missing_raw_scores(self):
        assert _extract_sharpness_from_meta(json.dumps({"foo": 1})) is None

    def test_missing_sharpness_key(self):
        rj = json.dumps({"raw_scores": {"xau_openapi_entry_router": {}}})
        assert _extract_sharpness_from_meta(rj) is None

    def test_invalid_json(self):
        assert _extract_sharpness_from_meta("not json") is None

    def test_empty_string(self):
        assert _extract_sharpness_from_meta("") is None

    def test_none_input(self):
        assert _extract_sharpness_from_meta(None) is None

    def test_missing_sharpness_score(self):
        rj = json.dumps({"raw_scores": {"xau_openapi_entry_router": {"sharpness": {"band": "normal"}}}})
        assert _extract_sharpness_from_meta(rj) is None


# ---------------------------------------------------------------------------
# 2. Point-biserial correlation tests
# ---------------------------------------------------------------------------

class TestPointBiserialCorrelation:
    def test_perfect_positive(self):
        continuous = [0.0, 0.0, 0.0, 100.0, 100.0, 100.0]
        binary = [0, 0, 0, 1, 1, 1]
        r = _point_biserial_correlation(continuous, binary)
        assert r > 0.8

    def test_perfect_negative(self):
        continuous = [100.0, 100.0, 100.0, 0.0, 0.0, 0.0]
        binary = [0, 0, 0, 1, 1, 1]
        r = _point_biserial_correlation(continuous, binary)
        assert r < -0.8

    def test_no_correlation(self):
        continuous = [50.0, 50.0, 50.0, 50.0]
        binary = [0, 1, 0, 1]
        r = _point_biserial_correlation(continuous, binary)
        assert abs(r) < 0.01

    def test_insufficient_data(self):
        assert _point_biserial_correlation([1.0, 2.0], [0, 1]) == 0.0

    def test_all_same_outcome(self):
        assert _point_biserial_correlation([1.0, 2.0, 3.0, 4.0], [1, 1, 1, 1]) == 0.0

    def test_bounds(self):
        r = _point_biserial_correlation([10.0, 20.0, 30.0, 40.0], [0, 0, 1, 1])
        assert -1.0 <= r <= 1.0


# ---------------------------------------------------------------------------
# 3. Query resolved trades
# ---------------------------------------------------------------------------

class TestQueryResolvedTrades:
    def test_query_with_sharpness(self):
        conn = sqlite3.connect(":memory:")
        conn.row_factory = sqlite3.Row
        _setup_db(conn, [
            {"outcome": 1, "pnl_usd": 2.0, "direction": "long", "request_json": _make_request_json(75, "sharp")},
            {"outcome": 0, "pnl_usd": -1.0, "direction": "short", "request_json": _make_request_json(25, "knife")},
        ])
        trades = query_resolved_trades_with_sharpness(conn, days=30, symbol="XAUUSD")
        assert len(trades) == 2
        scores = sorted([t["sharpness_score"] for t in trades])
        assert scores == [25, 75]

    def test_query_no_sharpness_data(self):
        conn = sqlite3.connect(":memory:")
        conn.row_factory = sqlite3.Row
        _setup_db(conn, [
            {"outcome": 1, "request_json": json.dumps({"raw_scores": {}})},
        ])
        trades = query_resolved_trades_with_sharpness(conn, days=30, symbol="XAUUSD")
        assert len(trades) == 0

    def test_query_empty_db(self):
        conn = sqlite3.connect(":memory:")
        conn.row_factory = sqlite3.Row
        trades = query_resolved_trades_with_sharpness(conn, days=30, symbol="XAUUSD")
        assert trades == []

    def test_query_wrong_symbol(self):
        conn = sqlite3.connect(":memory:")
        conn.row_factory = sqlite3.Row
        _setup_db(conn, [
            {"outcome": 1, "symbol": "BTCUSD", "request_json": _make_request_json()},
        ])
        trades = query_resolved_trades_with_sharpness(conn, days=30, symbol="XAUUSD")
        assert len(trades) == 0


# ---------------------------------------------------------------------------
# 4. Correlation computation
# ---------------------------------------------------------------------------

class TestComputeCorrelation:
    def test_correlation_basic(self):
        trades = [
            {"outcome": 1, "pnl_usd": 2.0, "sharpness_score": 80, "sharpness_band": "sharp",
             "momentum_quality": 80, "flow_persistence": 70, "absorption_quality": 60,
             "price_stability": 75, "positioning_quality": 85},
            {"outcome": 1, "pnl_usd": 1.5, "sharpness_score": 72, "sharpness_band": "sharp",
             "momentum_quality": 75, "flow_persistence": 65, "absorption_quality": 55,
             "price_stability": 70, "positioning_quality": 80},
            {"outcome": 0, "pnl_usd": -1.0, "sharpness_score": 30, "sharpness_band": "knife",
             "momentum_quality": 25, "flow_persistence": 30, "absorption_quality": 20,
             "price_stability": 35, "positioning_quality": 28},
            {"outcome": 0, "pnl_usd": -0.8, "sharpness_score": 28, "sharpness_band": "knife",
             "momentum_quality": 22, "flow_persistence": 28, "absorption_quality": 18,
             "price_stability": 30, "positioning_quality": 25},
        ]
        result = compute_sharpness_correlation(trades)
        assert result["n_trades"] == 4
        assert result["n_wins"] == 2
        assert result["composite"]["r"] > 0
        assert "sharp" in result["band_win_rates"]
        assert result["band_win_rates"]["sharp"]["win_rate"] == 1.0
        assert result["band_win_rates"]["knife"]["win_rate"] == 0.0
        for dim in SHARPNESS_DIMENSIONS:
            assert dim in result["per_dimension"]

    def test_correlation_empty(self):
        result = compute_sharpness_correlation([])
        assert result["n_trades"] == 0
        assert result["per_dimension"] == {}

    def test_band_win_rates_mixed(self):
        trades = [
            {"outcome": 1, "pnl_usd": 1.0, "sharpness_score": 55, "sharpness_band": "normal",
             "momentum_quality": 50, "flow_persistence": 50, "absorption_quality": 50,
             "price_stability": 50, "positioning_quality": 50},
            {"outcome": 0, "pnl_usd": -1.0, "sharpness_score": 55, "sharpness_band": "normal",
             "momentum_quality": 50, "flow_persistence": 50, "absorption_quality": 50,
             "price_stability": 50, "positioning_quality": 50},
            {"outcome": 1, "pnl_usd": 1.0, "sharpness_score": 55, "sharpness_band": "normal",
             "momentum_quality": 50, "flow_persistence": 50, "absorption_quality": 50,
             "price_stability": 50, "positioning_quality": 50},
            {"outcome": 0, "pnl_usd": -1.0, "sharpness_score": 55, "sharpness_band": "normal",
             "momentum_quality": 50, "flow_persistence": 50, "absorption_quality": 50,
             "price_stability": 50, "positioning_quality": 50},
        ]
        result = compute_sharpness_correlation(trades)
        assert result["band_win_rates"]["normal"]["win_rate"] == 0.5
        assert result["band_win_rates"]["normal"]["total"] == 4


# ---------------------------------------------------------------------------
# 5. Weight recommendations
# ---------------------------------------------------------------------------

class TestWeightRecommendations:
    def test_positive_correlation_increases_weight(self):
        correlation = {
            "n_trades": 20,
            "per_dimension": {
                "momentum_quality": {"r": 0.35},
                "flow_persistence": {"r": 0.10},
                "absorption_quality": {"r": -0.25},
                "price_stability": {"r": 0.02},
                "positioning_quality": {"r": 0.40},
            },
        }
        result = compute_weight_recommendations(correlation)
        assert result["apply"] is True
        recs = {r["dimension"]: r for r in result["recommendations"]}
        assert recs["momentum_quality"]["action"] == "increase"
        assert recs["absorption_quality"]["action"] == "decrease"
        assert recs["price_stability"]["action"] == "hold"
        assert recs["positioning_quality"]["recommended"] > recs["positioning_quality"]["current"]

    def test_insufficient_trades(self):
        correlation = {"n_trades": 3, "per_dimension": {}}
        result = compute_weight_recommendations(correlation, min_trades=10)
        assert result["apply"] is False
        assert "insufficient" in result["reason"]

    def test_all_weak_signals(self):
        correlation = {
            "n_trades": 20,
            "per_dimension": {dim: {"r": 0.01} for dim in SHARPNESS_DIMENSIONS},
        }
        result = compute_weight_recommendations(correlation)
        assert result["apply"] is False
        assert all(r["action"] == "hold" for r in result["recommendations"])

    def test_weight_clamping(self):
        correlation = {
            "n_trades": 20,
            "per_dimension": {
                "momentum_quality": {"r": 0.8},
                "flow_persistence": {"r": 0.0},
                "absorption_quality": {"r": 0.0},
                "price_stability": {"r": 0.0},
                "positioning_quality": {"r": 0.0},
            },
        }
        current = {"XAU_ENTRY_SHARPNESS_W_MOMENTUM": 1.95}
        result = compute_weight_recommendations(correlation, current_weights=current, max_weight=2.0)
        recs = {r["dimension"]: r for r in result["recommendations"]}
        assert recs["momentum_quality"]["recommended"] <= 2.0

    def test_custom_min_max_step(self):
        correlation = {
            "n_trades": 20,
            "per_dimension": {
                "momentum_quality": {"r": -0.5},
                "flow_persistence": {"r": 0.0},
                "absorption_quality": {"r": 0.0},
                "price_stability": {"r": 0.0},
                "positioning_quality": {"r": 0.0},
            },
        }
        current = {"XAU_ENTRY_SHARPNESS_W_MOMENTUM": 0.6}
        result = compute_weight_recommendations(
            correlation, current_weights=current, min_weight=0.5, max_step=0.05
        )
        recs = {r["dimension"]: r for r in result["recommendations"]}
        assert recs["momentum_quality"]["recommended"] >= 0.5


# ---------------------------------------------------------------------------
# 6. Family decay detection
# ---------------------------------------------------------------------------

class TestFamilyDecay:
    def _build_decay_trades(self, conn, family, recent_outcomes, baseline_outcomes):
        """Insert trades: recent first (newer), then baseline (older)."""
        _setup_db(conn, [])  # ensure tables exist
        source = f"scalp_xauusd:{family}:canary"
        base_time = "2026-04-04"
        idx = 0
        for outcome in recent_outcomes:
            idx += 1
            t = f"{base_time} {12 - idx // 60:02d}:{idx % 60:02d}:00"
            cur = conn.execute(
                "INSERT INTO execution_journal (direction, source, symbol, confidence, request_json) VALUES (?, ?, ?, ?, ?)",
                ("long", source, "XAUUSD", 5.0, "{}"),
            )
            conn.execute(
                "INSERT INTO ctrader_deals (journal_id, outcome, pnl_usd, execution_utc, has_close_detail) VALUES (?, ?, ?, ?, 1)",
                (cur.lastrowid, outcome, 1.0 if outcome else -1.0, t),
            )
        for outcome in baseline_outcomes:
            idx += 1
            t = f"2026-03-20 {12 - idx // 60:02d}:{idx % 60:02d}:00"
            cur = conn.execute(
                "INSERT INTO execution_journal (direction, source, symbol, confidence, request_json) VALUES (?, ?, ?, ?, ?)",
                ("long", source, "XAUUSD", 5.0, "{}"),
            )
            conn.execute(
                "INSERT INTO ctrader_deals (journal_id, outcome, pnl_usd, execution_utc, has_close_detail) VALUES (?, ?, ?, ?, 1)",
                (cur.lastrowid, outcome, 1.0 if outcome else -1.0, t),
            )
        conn.commit()

    def test_decay_detected(self):
        conn = sqlite3.connect(":memory:")
        conn.row_factory = sqlite3.Row
        # Recent: 3/10 wins = 30% WR, Baseline: 7/10 wins = 70% WR → decay = 40%
        self._build_decay_trades(conn, "xau_scalp_pullback_limit",
                                 [1, 0, 0, 0, 0, 0, 1, 0, 1, 0],
                                 [1, 1, 1, 1, 1, 0, 1, 0, 1, 0])
        result = detect_family_decay(conn, recent_trades=10, baseline_trades=10, decay_threshold=0.15, min_recent=4)
        assert len(result["families"]) > 0
        fam = result["families"][0]
        assert fam["decay"] > 0.15
        assert fam["action"] in ("reduce_risk", "alert", "pause")
        assert len(result["alerts"]) > 0

    def test_no_decay(self):
        conn = sqlite3.connect(":memory:")
        conn.row_factory = sqlite3.Row
        # Consistent 70% WR in both windows
        self._build_decay_trades(conn, "xau_scalp_pullback_limit",
                                 [1, 1, 1, 0, 1, 1, 1, 0, 0, 1],
                                 [1, 1, 1, 0, 1, 1, 1, 0, 0, 1])
        result = detect_family_decay(conn, recent_trades=10, baseline_trades=10, decay_threshold=0.15, min_recent=4)
        families_with_data = [f for f in result["families"] if f["action"] != "monitor"]
        for f in families_with_data:
            assert f["action"] == "ok"
        assert len(result["alerts"]) == 0

    def test_critical_wr_pause(self):
        conn = sqlite3.connect(":memory:")
        conn.row_factory = sqlite3.Row
        # Recent: 1/10 = 10% WR (critical), Baseline: 5/10 = 50%
        self._build_decay_trades(conn, "xau_scalp_rr",
                                 [0, 0, 0, 0, 0, 0, 1, 0, 0, 0],
                                 [1, 1, 1, 1, 1, 0, 0, 0, 0, 0])
        result = detect_family_decay(conn, recent_trades=10, baseline_trades=10, decay_threshold=0.15, min_recent=4)
        assert any(f["action"] in ("pause", "reduce_risk") for f in result["families"])

    def test_insufficient_data(self):
        conn = sqlite3.connect(":memory:")
        conn.row_factory = sqlite3.Row
        _setup_db(conn, [])
        result = detect_family_decay(conn, min_recent=6)
        assert result["families"] == []
        assert result["alerts"] == []

    def test_empty_db(self):
        conn = sqlite3.connect(":memory:")
        conn.row_factory = sqlite3.Row
        result = detect_family_decay(conn)
        assert result["families"] == []


# ---------------------------------------------------------------------------
# 7. Full report builder
# ---------------------------------------------------------------------------

class TestBuildReport:
    def test_full_report_with_data(self):
        conn = sqlite3.connect(":memory:")
        conn.row_factory = sqlite3.Row
        trades = []
        for i in range(15):
            score = 70 + i if i < 8 else 30 + i
            outcome = 1 if i < 8 else 0
            trades.append({
                "outcome": outcome,
                "pnl_usd": 2.0 if outcome else -1.0,
                "direction": "long",
                "request_json": _make_request_json(
                    score,
                    "sharp" if score >= 70 else "knife" if score < 30 else "normal",
                ),
            })
        _setup_db(conn, trades)
        report = build_sharpness_feedback_report(conn, days=30)
        assert report["ok"] is True
        assert "correlation" in report
        assert "calibration" in report
        assert "family_decay" in report
        assert "summary" in report
        summary = report["summary"]
        assert summary["n_trades_with_sharpness"] == 15

    def test_report_empty_db(self):
        conn = sqlite3.connect(":memory:")
        conn.row_factory = sqlite3.Row
        _setup_db(conn, [])
        report = build_sharpness_feedback_report(conn, days=30)
        assert report["ok"] is True
        assert report["summary"]["n_trades_with_sharpness"] == 0


# ---------------------------------------------------------------------------
# 8. Telegram formatter
# ---------------------------------------------------------------------------

class TestFormatReport:
    def test_format_with_data(self):
        report = {
            "ok": True,
            "summary": {"n_trades_with_sharpness": 20},
            "correlation": {
                "overall_win_rate": 0.65,
                "composite": {"r": 0.35, "mean_win": 68, "mean_loss": 42},
                "per_dimension": {
                    "momentum_quality": {"r": 0.30},
                    "flow_persistence": {"r": 0.20},
                    "absorption_quality": {"r": -0.10},
                    "price_stability": {"r": 0.15},
                    "positioning_quality": {"r": 0.25},
                },
                "band_win_rates": {
                    "knife": {"win_rate": 0.2, "total": 5},
                    "sharp": {"win_rate": 0.9, "total": 10},
                },
            },
            "calibration": {"apply": True, "recommendations": [{"action": "increase"}]},
            "family_decay": {"alerts": [
                {"family": "xau_scalp_pullback_limit", "recent_win_rate": 0.35,
                 "baseline_win_rate": 0.65, "action": "reduce_risk"},
            ]},
        }
        text = format_sharpness_feedback_text(report)
        assert "Sharpness Feedback Report" in text
        assert "Trades: 20" in text
        assert "r=0.350" in text
        assert "knife" in text
        assert "Calibration" in text
        assert "Decay" in text

    def test_format_no_trades(self):
        report = {"summary": {"n_trades_with_sharpness": 0}, "correlation": {}, "calibration": {}, "family_decay": {}}
        text = format_sharpness_feedback_text(report)
        assert "No trades with sharpness data" in text
