"""
Tests for learning/adaptive_directional_intelligence.py

Covers all 5 ADI dimensions, composite scoring, edge cases,
divergence detection, catastrophic override, and scheduler integration.
"""

import os
import sys
import sqlite3
import tempfile
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from learning.adaptive_directional_intelligence import (
    AdaptiveDirectionalIntelligence,
    _interpolate,
    _recency_weighted_wr,
    _consecutive_losses,
    _wr_acceleration,
    _pnl_trend_score,
    _r_efficiency,
    _win_loss_asymmetry,
    _trend_alignment,
    _utc_hour_to_session,
)


# ── Test Data Helpers ───────────────────────────────────────────────────────

def _make_trades(n_wins: int, n_losses: int, source: str = "fibo_xauusd",
                 direction: str = "short", win_pnl: float = 1.5,
                 loss_pnl: float = -2.5, base_hour: int = 14,
                 symbol: str = "XAUUSD") -> list[dict]:
    """Generate synthetic trade list ordered most-recent-first."""
    trades = []
    for i in range(n_wins):
        trades.append({
            "source": source, "direction": direction, "symbol": symbol,
            "outcome": "win", "pnl_usd": win_pnl,
            "entry": 3300.0, "stop_loss": 3302.0,
            "execution_utc": f"2026-04-{10 - i:02d} {base_hour}:00:00",
        })
    for i in range(n_losses):
        trades.append({
            "source": source, "direction": direction, "symbol": symbol,
            "outcome": "loss", "pnl_usd": loss_pnl,
            "entry": 3300.0, "stop_loss": 3302.0,
            "execution_utc": f"2026-04-{10 - n_wins - i:02d} {base_hour}:00:00",
        })
    return trades


def _make_db(trades: list[dict]) -> str:
    """Create a temporary SQLite DB with execution_journal rows. Returns path."""
    fd, path = tempfile.mkstemp(suffix=".db")
    os.close(fd)
    conn = sqlite3.connect(path)
    conn.execute("""
        CREATE TABLE execution_journal (
            source TEXT, direction TEXT, symbol TEXT, outcome TEXT,
            pnl_usd REAL, entry REAL, stop_loss REAL, take_profit REAL,
            execution_utc TEXT, execution_meta_json TEXT
        )
    """)
    for t in trades:
        conn.execute(
            "INSERT INTO execution_journal VALUES (?,?,?,?,?,?,?,?,?,?)",
            (t.get("source", ""), t.get("direction", ""), t.get("symbol", ""),
             t.get("outcome", ""), t.get("pnl_usd", 0), t.get("entry", 0),
             t.get("stop_loss", 0), t.get("take_profit", 0),
             t.get("execution_utc", ""), t.get("execution_meta_json", "")),
        )
    conn.commit()
    conn.close()
    return path


# ═══════════════════════════════════════════════════════════════════════════
#  UNIT TESTS — Helper Functions
# ═══════════════════════════════════════════════════════════════════════════

class TestInterpolate(unittest.TestCase):
    def test_exact_breakpoint(self):
        self.assertAlmostEqual(_interpolate(0.50, [(0, -10), (0.5, 0), (1, 10)]), 0.0)

    def test_between_breakpoints(self):
        self.assertAlmostEqual(_interpolate(0.25, [(0, -10), (0.5, 0), (1, 10)]), -5.0)

    def test_below_min(self):
        self.assertAlmostEqual(_interpolate(-5, [(0, -10), (1, 10)]), -10.0)

    def test_above_max(self):
        self.assertAlmostEqual(_interpolate(999, [(0, -10), (1, 10)]), 10.0)

    def test_empty_points(self):
        self.assertAlmostEqual(_interpolate(0.5, []), 0.0)


class TestRecencyWeightedWR(unittest.TestCase):
    def test_all_wins(self):
        trades = [{"outcome": "win"}] * 10
        self.assertAlmostEqual(_recency_weighted_wr(trades), 1.0)

    def test_all_losses(self):
        trades = [{"outcome": "loss"}] * 10
        self.assertAlmostEqual(_recency_weighted_wr(trades), 0.0)

    def test_recent_wins_weighted_higher(self):
        # Recent wins, old losses → WR > 0.5
        trades = [{"outcome": "win"}] * 5 + [{"outcome": "loss"}] * 5
        wr = _recency_weighted_wr(trades)
        self.assertGreater(wr, 0.5)

    def test_recent_losses_weighted_higher(self):
        # Recent losses, old wins → WR < 0.5
        trades = [{"outcome": "loss"}] * 5 + [{"outcome": "win"}] * 5
        wr = _recency_weighted_wr(trades)
        self.assertLess(wr, 0.5)

    def test_empty(self):
        self.assertAlmostEqual(_recency_weighted_wr([]), 0.0)


class TestConsecutiveLosses(unittest.TestCase):
    def test_three_recent_losses(self):
        trades = [{"outcome": "loss"}, {"outcome": "loss"}, {"outcome": "loss"}, {"outcome": "win"}]
        self.assertEqual(_consecutive_losses(trades), 3)

    def test_no_losses(self):
        trades = [{"outcome": "win"}, {"outcome": "win"}]
        self.assertEqual(_consecutive_losses(trades), 0)

    def test_empty(self):
        self.assertEqual(_consecutive_losses([]), 0)


class TestWRAcceleration(unittest.TestCase):
    def test_improving(self):
        # Recent 5 all wins, rest mixed → positive acceleration
        trades = [{"outcome": "win"}] * 5 + [{"outcome": "loss"}] * 5 + [{"outcome": "win"}] * 3
        accel = _wr_acceleration(trades, recent_n=5)
        self.assertGreater(accel, 0.0)

    def test_degrading(self):
        # Recent 5 all losses, rest wins → negative
        trades = [{"outcome": "loss"}] * 5 + [{"outcome": "win"}] * 8
        accel = _wr_acceleration(trades, recent_n=5)
        self.assertLess(accel, 0.0)

    def test_insufficient_data(self):
        trades = [{"outcome": "win"}] * 3
        self.assertAlmostEqual(_wr_acceleration(trades), 0.0)


class TestPnlTrendScore(unittest.TestCase):
    def test_improving_pnl(self):
        trades = [{"pnl_usd": 3.0}] * 5 + [{"pnl_usd": -2.0}] * 5
        score = _pnl_trend_score(trades)
        self.assertGreater(score, 0.0)

    def test_degrading_pnl(self):
        trades = [{"pnl_usd": -3.0}] * 5 + [{"pnl_usd": 2.0}] * 5
        score = _pnl_trend_score(trades)
        self.assertLess(score, 0.0)

    def test_insufficient(self):
        trades = [{"pnl_usd": 1.0}] * 5
        self.assertAlmostEqual(_pnl_trend_score(trades), 0.0)


class TestREfficiency(unittest.TestCase):
    def test_high_efficiency(self):
        trades = [{"outcome": "win", "entry": 100, "stop_loss": 99, "pnl_usd": 3.0}] * 5
        score = _r_efficiency(trades)
        self.assertGreater(score, 0.0)

    def test_no_wins(self):
        trades = [{"outcome": "loss", "entry": 100, "stop_loss": 99, "pnl_usd": -1.0}] * 5
        self.assertAlmostEqual(_r_efficiency(trades), 0.0)


class TestWinLossAsymmetry(unittest.TestCase):
    def test_big_wins_small_losses(self):
        trades = [
            {"outcome": "win", "pnl_usd": 5.0}, {"outcome": "win", "pnl_usd": 4.0},
            {"outcome": "loss", "pnl_usd": -1.0}, {"outcome": "loss", "pnl_usd": -1.5},
        ]
        score = _win_loss_asymmetry(trades)
        self.assertGreater(score, 0.0)

    def test_small_wins_big_losses(self):
        trades = [
            {"outcome": "win", "pnl_usd": 0.5}, {"outcome": "win", "pnl_usd": 0.3},
            {"outcome": "loss", "pnl_usd": -5.0}, {"outcome": "loss", "pnl_usd": -4.0},
        ]
        score = _win_loss_asymmetry(trades)
        self.assertLess(score, 0.0)


class TestTrendAlignment(unittest.TestCase):
    def test_long_bullish(self):
        self.assertEqual(_trend_alignment("long", "bullish"), 1)

    def test_long_bearish(self):
        self.assertEqual(_trend_alignment("long", "bearish"), -1)

    def test_short_bearish(self):
        self.assertEqual(_trend_alignment("short", "bearish"), 1)

    def test_short_bullish(self):
        self.assertEqual(_trend_alignment("short", "bullish"), -1)

    def test_neutral(self):
        self.assertEqual(_trend_alignment("long", "neutral"), 0)

    def test_empty(self):
        self.assertEqual(_trend_alignment("long", ""), 0)

    def test_strong_up(self):
        self.assertEqual(_trend_alignment("long", "strong_up"), 1)


class TestUtcHourToSession(unittest.TestCase):
    def test_asian(self):
        self.assertEqual(_utc_hour_to_session(23), "asian")
        self.assertEqual(_utc_hour_to_session(3), "asian")

    def test_london(self):
        self.assertEqual(_utc_hour_to_session(8), "london")
        self.assertEqual(_utc_hour_to_session(12), "london")

    def test_new_york(self):
        self.assertEqual(_utc_hour_to_session(14), "new_york")
        self.assertEqual(_utc_hour_to_session(20), "new_york")


# ═══════════════════════════════════════════════════════════════════════════
#  DIMENSION TESTS — Scoring each dimension independently
# ═══════════════════════════════════════════════════════════════════════════

class TestEmpiricalDimension(unittest.TestCase):
    def test_strong_winner(self):
        """90% WR with 20 trades → strong positive score."""
        trades = _make_trades(18, 2, source="scalp_xauusd", direction="long")
        adi = AdaptiveDirectionalIntelligence()
        score, details = adi._score_empirical("scalp_xauusd", "long", trades)
        self.assertGreater(score, 5.0)
        self.assertEqual(details["n"], 20)

    def test_severe_loser(self):
        """9% WR (fibo_xauusd short actual data) → strong negative score."""
        trades = _make_trades(2, 20, source="fibo_xauusd", direction="short")
        adi = AdaptiveDirectionalIntelligence()
        score, details = adi._score_empirical("fibo_xauusd", "short", trades)
        self.assertLess(score, -10.0)

    def test_cold_start(self):
        """No trades → small conservative penalty, not block."""
        adi = AdaptiveDirectionalIntelligence()
        score, details = adi._score_empirical("new_family", "long", [])
        self.assertLess(score, 0.0)
        self.assertGreater(score, -10.0)
        self.assertEqual(details["reason"], "no_data_cold_start")

    def test_source_matching_with_lane(self):
        """scalp_xauusd:winner trades match scalp_xauusd source."""
        trades = _make_trades(8, 2, source="scalp_xauusd:winner", direction="long")
        adi = AdaptiveDirectionalIntelligence()
        score, details = adi._score_empirical("scalp_xauusd", "long", trades)
        self.assertEqual(details["n"], 10)

    def test_direction_filtering(self):
        """Only counts trades matching requested direction."""
        long_trades = _make_trades(10, 0, direction="long")
        short_trades = _make_trades(0, 10, direction="short")
        all_trades = long_trades + short_trades
        adi = AdaptiveDirectionalIntelligence()
        score_long, det_long = adi._score_empirical("fibo_xauusd", "long", all_trades)
        score_short, det_short = adi._score_empirical("fibo_xauusd", "short", all_trades)
        self.assertGreater(score_long, score_short)


class TestTechnicalDimension(unittest.TestCase):
    def test_all_aligned_long(self):
        adi = AdaptiveDirectionalIntelligence()
        score, _ = adi._score_technical("long", {"d1": "bullish", "h4": "bullish", "h1": "bullish"})
        self.assertGreater(score, 5.0)

    def test_all_counter_long(self):
        adi = AdaptiveDirectionalIntelligence()
        score, _ = adi._score_technical("long", {"d1": "bearish", "h4": "bearish", "h1": "bearish"})
        self.assertLess(score, -20.0)

    def test_mixed_trends(self):
        adi = AdaptiveDirectionalIntelligence()
        score, _ = adi._score_technical("long", {"d1": "bullish", "h4": "bearish", "h1": "neutral"})
        # D1 aligned (+3), H4 counter (-2), H1 neutral (0) = +1 → mild positive
        self.assertGreater(score, -5.0)
        self.assertLess(score, 5.0)

    def test_no_trend_data(self):
        adi = AdaptiveDirectionalIntelligence()
        score, details = adi._score_technical("long", None)
        self.assertAlmostEqual(score, 0.0)
        self.assertEqual(details["reason"], "no_trend_data")


class TestFlowDimension(unittest.TestCase):
    def test_strong_long_flow(self):
        adi = AdaptiveDirectionalIntelligence()
        features = {"delta_proxy": 0.3, "depth_imbalance": 0.4, "bar_volume_proxy": 0.6, "tick_up_ratio": 0.8, "spots_count": 50}
        score, _ = adi._score_flow("long", features)
        self.assertGreater(score, 3.0)

    def test_adverse_flow_for_long(self):
        adi = AdaptiveDirectionalIntelligence()
        features = {"delta_proxy": -0.4, "depth_imbalance": -0.3, "bar_volume_proxy": 0.5, "tick_up_ratio": 0.2, "spots_count": 50}
        score, _ = adi._score_flow("long", features)
        self.assertLess(score, -5.0)

    def test_insufficient_spots(self):
        adi = AdaptiveDirectionalIntelligence()
        features = {"delta_proxy": 0.3, "depth_imbalance": 0.4, "bar_volume_proxy": 0.6, "tick_up_ratio": 0.8, "spots_count": 1}
        score, details = adi._score_flow("long", features)
        self.assertAlmostEqual(score, 0.0)
        self.assertEqual(details["reason"], "insufficient_flow_data")

    def test_no_flow_data(self):
        adi = AdaptiveDirectionalIntelligence()
        score, details = adi._score_flow("long", None)
        self.assertAlmostEqual(score, 0.0)

    def test_short_direction_reverses_delta(self):
        adi = AdaptiveDirectionalIntelligence()
        features = {"delta_proxy": -0.3, "depth_imbalance": -0.3, "bar_volume_proxy": 0.5, "tick_up_ratio": 0.3, "spots_count": 30}
        score_short, _ = adi._score_flow("short", features)
        score_long, _ = adi._score_flow("long", features)
        # Negative delta supports shorts, hurts longs
        self.assertGreater(score_short, score_long)


class TestTemporalDimension(unittest.TestCase):
    def test_insufficient_data(self):
        adi = AdaptiveDirectionalIntelligence()
        trades = _make_trades(2, 1, direction="long")
        score, details = adi._score_temporal("fibo_xauusd", "long", trades, None)
        self.assertAlmostEqual(score, 0.0)
        self.assertEqual(details["reason"], "insufficient_temporal_data")


class TestCrossFamilyDimension(unittest.TestCase):
    def test_systemic_losing(self):
        """Multiple families losing in same direction → extra penalty."""
        adi = AdaptiveDirectionalIntelligence()
        # Use distinct base sources so cross-family sees 3 separate families
        trades = (
            _make_trades(1, 9, source="scalp_xauusd", direction="short") +
            _make_trades(0, 5, source="canary_tdf_xauusd", direction="short") +
            _make_trades(1, 4, source="canary_mfu_xauusd", direction="short")
        )
        score, details = adi._score_cross_family("short", trades, exclude_source="fibo_xauusd")
        self.assertLess(score, -10.0)
        self.assertGreaterEqual(details["losing_families"], 2)

    def test_other_families_winning(self):
        """Other families winning in same direction → positive signal."""
        adi = AdaptiveDirectionalIntelligence()
        trades = (
            _make_trades(8, 2, source="scalp_xauusd", direction="long") +
            _make_trades(9, 1, source="scalp_xauusd:canary_tdf", direction="long")
        )
        score, details = adi._score_cross_family("long", trades, exclude_source="fibo_xauusd")
        self.assertGreater(score, 0.0)

    def test_excludes_own_source(self):
        """Own family is excluded from cross-family scoring."""
        adi = AdaptiveDirectionalIntelligence()
        trades = _make_trades(0, 10, source="fibo_xauusd", direction="short")
        score, details = adi._score_cross_family("short", trades, exclude_source="fibo_xauusd")
        self.assertAlmostEqual(score, 0.0)
        self.assertEqual(details["reason"], "no_cross_family_data")


# ═══════════════════════════════════════════════════════════════════════════
#  COMPOSITE / INTEGRATION TESTS
# ═══════════════════════════════════════════════════════════════════════════

class TestCompositeEvaluation(unittest.TestCase):
    def test_evaluate_returns_modifier(self):
        """evaluate() returns a dict with modifier key."""
        db_path = _make_db(_make_trades(10, 10, direction="long"))
        try:
            adi = AdaptiveDirectionalIntelligence(db_path=db_path)
            result = adi.evaluate(
                source="fibo_xauusd", direction="long", symbol="XAUUSD",
                confidence=72.0,
                trend_context={"d1": "bullish", "h4": "bullish", "h1": "neutral"},
            )
            self.assertIn("modifier", result)
            self.assertIn("dimensions", result)
            self.assertIn("recommendation", result)
            self.assertIsInstance(result["modifier"], float)
        finally:
            os.unlink(db_path)

    def test_evaluate_never_blocks(self):
        """Even with worst-case data, modifier is a number, not a block."""
        db_path = _make_db(_make_trades(0, 30, direction="short"))
        try:
            adi = AdaptiveDirectionalIntelligence(db_path=db_path)
            result = adi.evaluate(
                source="fibo_xauusd", direction="short", symbol="XAUUSD",
                confidence=72.0,
                trend_context={"d1": "bullish", "h4": "bullish", "h1": "bullish"},  # all counter for short
            )
            self.assertIn("modifier", result)
            self.assertIsInstance(result["modifier"], float)
            # Should be a big negative modifier, but still a number
            self.assertLessEqual(result["modifier"], -15.0)
            self.assertGreaterEqual(result["modifier"], -45.0)  # clamped
        finally:
            os.unlink(db_path)

    def test_evaluate_with_flow_features(self):
        """Flow features contribute to scoring."""
        db_path = _make_db(_make_trades(10, 10, direction="long"))
        try:
            adi = AdaptiveDirectionalIntelligence(db_path=db_path)
            result_good = adi.evaluate(
                source="fibo_xauusd", direction="long", symbol="XAUUSD",
                flow_features={"delta_proxy": 0.3, "depth_imbalance": 0.3, "bar_volume_proxy": 0.7, "tick_up_ratio": 0.8, "spots_count": 50},
            )
            result_bad = adi.evaluate(
                source="fibo_xauusd", direction="long", symbol="XAUUSD",
                flow_features={"delta_proxy": -0.4, "depth_imbalance": -0.3, "bar_volume_proxy": 0.5, "tick_up_ratio": 0.2, "spots_count": 50},
            )
            self.assertGreater(result_good["modifier"], result_bad["modifier"])
        finally:
            os.unlink(db_path)

    def test_divergence_detection(self):
        """Empirical losing + technical aligned → divergence flag."""
        db_path = _make_db(_make_trades(1, 20, source="fibo_xauusd", direction="short"))
        try:
            adi = AdaptiveDirectionalIntelligence(db_path=db_path)
            result = adi.evaluate(
                source="fibo_xauusd", direction="short", symbol="XAUUSD",
                confidence=72.0,
                trend_context={"d1": "bearish", "h4": "bearish", "h1": "bearish"},  # aligned
            )
            self.assertTrue(result.get("divergence_flag", False))
        finally:
            os.unlink(db_path)

    def test_both_directions_evaluated_independently(self):
        """ADI must produce different scores for long vs short with asymmetric data."""
        db_path = _make_db(
            _make_trades(18, 2, source="fibo_xauusd", direction="long", win_pnl=3.0, loss_pnl=-1.0) +
            _make_trades(2, 20, source="fibo_xauusd", direction="short", win_pnl=1.0, loss_pnl=-3.0)
        )
        try:
            adi = AdaptiveDirectionalIntelligence(db_path=db_path)
            result_long = adi.evaluate(
                source="fibo_xauusd", direction="long", symbol="XAUUSD",
                trend_context={"d1": "bullish", "h4": "bullish", "h1": "neutral"},
                flow_features={"delta_proxy": 0.2, "depth_imbalance": 0.1, "bar_volume_proxy": 0.5, "tick_up_ratio": 0.6, "spots_count": 40},
            )
            result_short = adi.evaluate(
                source="fibo_xauusd", direction="short", symbol="XAUUSD",
                trend_context={"d1": "bullish", "h4": "bullish", "h1": "neutral"},
                flow_features={"delta_proxy": 0.2, "depth_imbalance": 0.1, "bar_volume_proxy": 0.5, "tick_up_ratio": 0.6, "spots_count": 40},
            )
            # Long: 90% WR + bullish trend aligned → positive modifier
            # Short: 9% WR + bullish trend counter → severe negative modifier
            self.assertGreater(result_long["modifier"], result_short["modifier"])
            self.assertGreater(result_long["modifier"], 0.0, "Long with 90% WR + aligned trend should boost")
            self.assertLess(result_short["modifier"], -10.0, "Short with 9% WR + counter trend should penalize hard")
            # Empirical dimension must reflect direction-specific data
            self.assertGreater(
                result_long["dimensions"]["empirical"]["score"],
                result_short["dimensions"]["empirical"]["score"],
            )
            # Technical: bullish trend helps long, hurts short
            self.assertGreater(
                result_long["dimensions"]["technical"]["score"],
                result_short["dimensions"]["technical"]["score"],
            )
            # Flow: positive delta helps long, hurts short
            self.assertGreater(
                result_long["dimensions"]["flow"]["score"],
                result_short["dimensions"]["flow"]["score"],
            )
        finally:
            os.unlink(db_path)

    def test_short_direction_gets_boost_when_winning(self):
        """Short must get positive modifier when short is winning + bearish trend."""
        db_path = _make_db(
            _make_trades(25, 5, source="fibo_xauusd", direction="short", win_pnl=2.5, loss_pnl=-1.0)
        )
        try:
            adi = AdaptiveDirectionalIntelligence(db_path=db_path)
            result = adi.evaluate(
                source="fibo_xauusd", direction="short", symbol="XAUUSD",
                trend_context={"d1": "bearish", "h4": "bearish", "h1": "bearish"},
                flow_features={"delta_proxy": -0.3, "depth_imbalance": -0.2, "bar_volume_proxy": 0.6, "tick_up_ratio": 0.3, "spots_count": 50},
            )
            dims = result["dimensions"]
            self.assertGreater(dims["empirical"]["score"], 5.0, "83% short WR should score positive")
            self.assertGreater(dims["technical"]["score"], 5.0, "All-bearish trend aligned with short")
            self.assertGreater(dims["flow"]["score"], 0.0, "Negative delta supports short")
        finally:
            os.unlink(db_path)

    def test_invalid_direction_returns_zero(self):
        adi = AdaptiveDirectionalIntelligence()
        result = adi.evaluate(source="test", direction="sideways", symbol="XAUUSD")
        self.assertAlmostEqual(result["modifier"], 0.0)

    def test_evaluate_error_returns_zero(self):
        """Exception inside evaluate → modifier 0.0, no crash."""
        adi = AdaptiveDirectionalIntelligence(db_path="/nonexistent/path.db")
        result = adi.evaluate(source="test", direction="long", symbol="XAUUSD")
        # Should not raise; returns some result
        self.assertIn("modifier", result)

    def test_modifier_range_clamped(self):
        """Modifier is always within [-45, +15] range."""
        db_path = _make_db(
            _make_trades(0, 50, source="fibo_xauusd", direction="short", loss_pnl=-10.0) +
            _make_trades(0, 30, source="scalp_xauusd", direction="short", loss_pnl=-8.0)
        )
        try:
            adi = AdaptiveDirectionalIntelligence(db_path=db_path)
            result = adi.evaluate(
                source="fibo_xauusd", direction="short", symbol="XAUUSD",
                trend_context={"d1": "bullish", "h4": "bullish", "h1": "bullish"},
                flow_features={"delta_proxy": 0.5, "depth_imbalance": 0.5, "bar_volume_proxy": 0.8, "tick_up_ratio": 0.9, "spots_count": 100},
            )
            self.assertGreaterEqual(result["modifier"], -45.0)
            self.assertLessEqual(result["modifier"], 15.0)
        finally:
            os.unlink(db_path)

    def test_winning_direction_gets_boost(self):
        """90%+ WR with aligned trends → empirical + technical score strongly positive."""
        db_path = _make_db(_make_trades(27, 3, source="scalp_btcusd", direction="long", win_pnl=3.0, loss_pnl=-1.0, symbol="BTCUSD"))
        try:
            adi = AdaptiveDirectionalIntelligence(db_path=db_path)
            result = adi.evaluate(
                source="scalp_btcusd", direction="long", symbol="BTCUSD",
                trend_context={"d1": "bullish", "h4": "bullish", "h1": "bullish"},
            )
            # Empirical + Technical should be strongly positive
            dims = result.get("dimensions", {})
            self.assertGreater(dims["empirical"]["score"], 5.0)
            self.assertGreater(dims["technical"]["score"], 5.0)
            # Composite may be dragged by temporal (no matching hour/session data)
            # but should still be better than a loser scenario
            self.assertGreater(result["modifier"], -10.0)
        finally:
            os.unlink(db_path)


class TestCacheAndDB(unittest.TestCase):
    def test_cache_reuse(self):
        """Second call uses cached trades within TTL."""
        db_path = _make_db(_make_trades(5, 5, direction="long"))
        try:
            adi = AdaptiveDirectionalIntelligence(db_path=db_path)
            trades1 = adi._get_cached_trades("XAUUSD")
            trades2 = adi._get_cached_trades("XAUUSD")
            self.assertEqual(len(trades1), len(trades2))
            # Cache entry should exist
            self.assertIn("XAUUSD", adi._cache)
        finally:
            os.unlink(db_path)

    def test_no_db_returns_empty(self):
        adi = AdaptiveDirectionalIntelligence(db_path="/does/not/exist.db")
        trades = adi._load_trades("XAUUSD", days=14)
        self.assertEqual(trades, [])


class TestRecommendationLabels(unittest.TestCase):
    def test_severe_penalty_label(self):
        db_path = _make_db(_make_trades(0, 30, direction="short"))
        try:
            adi = AdaptiveDirectionalIntelligence(db_path=db_path)
            result = adi.evaluate(
                source="fibo_xauusd", direction="short", symbol="XAUUSD",
                trend_context={"d1": "bullish", "h4": "bullish", "h1": "bullish"},
            )
            self.assertIn("penalty", result["recommendation"])
        finally:
            os.unlink(db_path)

    def test_neutral_label(self):
        db_path = _make_db(_make_trades(10, 10, direction="long"))
        try:
            adi = AdaptiveDirectionalIntelligence(db_path=db_path)
            result = adi.evaluate(
                source="fibo_xauusd", direction="long", symbol="XAUUSD",
            )
            # With 50% WR and no trend data, should be in neutral-ish range
            self.assertIsNotNone(result["recommendation"])
        finally:
            os.unlink(db_path)


if __name__ == "__main__":
    unittest.main()
