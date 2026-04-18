"""
tests/test_entry_sharpness.py

Unit tests for analysis/entry_sharpness.py — deep data analytics features
and composite Entry Sharpness Score.
"""
import pytest
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from analysis.entry_sharpness import (
    tick_acceleration,
    adverse_flow_streak,
    depth_absorption_rate,
    micro_volatility,
    spread_trajectory,
    vwap_distance,
    tick_cluster_position,
    depth_imbalance_trend,
    compute_deep_features,
    compute_entry_sharpness_score,
)


# ── tick_acceleration ────────────────────────────────────────────────────────

class TestTickAcceleration:
    def test_accelerating_momentum(self):
        # Early ticks: flat; Late ticks: strong upward
        mids = [100.0, 100.0, 100.0, 100.1, 100.1, 100.1, 100.3, 100.5, 100.8]
        result = tick_acceleration(mids)
        assert result > 0, f"Expected positive for accelerating momentum, got {result}"

    def test_decaying_momentum(self):
        # Early ticks: strong upward; Late ticks: flat
        mids = [100.0, 100.3, 100.6, 100.7, 100.7, 100.7, 100.7, 100.7, 100.7]
        result = tick_acceleration(mids)
        assert result < 0, f"Expected negative for decaying momentum, got {result}"

    def test_insufficient_data(self):
        assert tick_acceleration([100.0, 101.0, 102.0]) == 0.0
        assert tick_acceleration([]) == 0.0

    def test_neutral_flat(self):
        mids = [100.0] * 12
        assert tick_acceleration(mids) == 0.0


# ── adverse_flow_streak ──────────────────────────────────────────────────────

class TestAdverseFlowStreak:
    def test_no_streak_alternating(self):
        deltas = [0.1, -0.1, 0.1, -0.1, 0.1, -0.1]
        result = adverse_flow_streak(deltas, "long")
        assert result <= 1.0 / 6.0 + 0.01, f"Expected low streak, got {result}"

    def test_pure_adverse_long(self):
        deltas = [-0.1, -0.2, -0.3, -0.1, -0.05]
        result = adverse_flow_streak(deltas, "long")
        assert result == 1.0, f"Expected 1.0 for all-adverse, got {result}"

    def test_pure_adverse_short(self):
        deltas = [0.1, 0.2, 0.3, 0.1, 0.05]
        result = adverse_flow_streak(deltas, "short")
        assert result == 1.0

    def test_direction_unknown_returns_max(self):
        deltas = [-0.1, -0.2, -0.3, 0.1, 0.05]
        result = adverse_flow_streak(deltas)
        # Should return max of both directions
        assert result > 0

    def test_insufficient_data(self):
        assert adverse_flow_streak([0.1, -0.1]) == 0.0
        assert adverse_flow_streak([]) == 0.0


# ── depth_absorption_rate ────────────────────────────────────────────────────

class TestDepthAbsorptionRate:
    def test_strong_refill(self):
        # Depth increases over time
        points = [
            (1000, 50.0, 40.0),
            (2000, 70.0, 55.0),
            (3000, 90.0, 70.0),
        ]
        result = depth_absorption_rate(points)
        assert result > 1.0, f"Expected >1.0 for strong refill, got {result}"

    def test_draining(self):
        # Depth decreases over time
        points = [
            (1000, 100.0, 100.0),
            (2000, 60.0, 50.0),
            (3000, 30.0, 20.0),
        ]
        result = depth_absorption_rate(points)
        assert result < 0.5, f"Expected <0.5 for draining, got {result}"

    def test_neutral_single_point(self):
        assert depth_absorption_rate([(1000, 50.0, 50.0)]) == 1.0

    def test_empty(self):
        assert depth_absorption_rate([]) == 1.0


# ── micro_volatility ─────────────────────────────────────────────────────────

class TestMicroVolatility:
    def test_calm_market(self):
        # Very small price changes
        mids = [3300.00, 3300.01, 3300.02, 3300.01, 3300.02]
        result = micro_volatility(mids)
        assert result < 0.01, f"Expected low vol for calm market, got {result}"

    def test_choppy_market(self):
        # Large alternating moves
        mids = [3300.0, 3310.0, 3290.0, 3315.0, 3285.0]
        result = micro_volatility(mids)
        assert result > 0.1, f"Expected high vol for choppy market, got {result}"

    def test_insufficient_data(self):
        assert micro_volatility([3300.0, 3301.0]) == 0.0
        assert micro_volatility([]) == 0.0


# ── spread_trajectory ────────────────────────────────────────────────────────

class TestSpreadTrajectory:
    def test_narrowing(self):
        # First half wide, second half narrow
        pcts = [0.002, 0.0022, 0.001, 0.0008]
        result = spread_trajectory(pcts)
        assert result < 0, f"Expected negative for narrowing, got {result}"

    def test_widening(self):
        # First half narrow, second half wide
        pcts = [0.0008, 0.001, 0.002, 0.0022]
        result = spread_trajectory(pcts)
        assert result > 0, f"Expected positive for widening, got {result}"

    def test_insufficient_data(self):
        assert spread_trajectory([0.001, 0.002]) == 0.0


# ── vwap_distance ────────────────────────────────────────────────────────────

class TestVwapDistance:
    def test_above_vwap(self):
        mids = [3300.0, 3301.0, 3302.0, 3305.0]
        result = vwap_distance(mids)
        assert result > 0, f"Expected positive for above VWAP, got {result}"

    def test_below_vwap(self):
        mids = [3305.0, 3302.0, 3301.0, 3300.0]
        result = vwap_distance(mids)
        assert result < 0, f"Expected negative for below VWAP, got {result}"

    def test_insufficient_data(self):
        assert vwap_distance([3300.0]) == 0.0


# ── tick_cluster_position ────────────────────────────────────────────────────

class TestTickClusterPosition:
    def test_accumulation_at_bottom(self):
        # Most ticks near the low
        mids = [100.0, 100.1, 100.0, 100.05, 100.0, 110.0]
        result = tick_cluster_position(mids)
        assert result < 0.3, f"Expected <0.3 for bottom cluster, got {result}"

    def test_distribution_at_top(self):
        # Most ticks near the high
        mids = [100.0, 110.0, 109.9, 110.0, 109.95, 110.0]
        result = tick_cluster_position(mids)
        assert result > 0.7, f"Expected >0.7 for top cluster, got {result}"

    def test_uniform(self):
        mids = [100.0, 102.5, 105.0, 107.5, 110.0]
        result = tick_cluster_position(mids)
        assert 0.4 <= result <= 0.6, f"Expected ~0.5 for uniform, got {result}"

    def test_flat_range(self):
        assert tick_cluster_position([100.0, 100.0, 100.0]) == 0.5

    def test_insufficient_data(self):
        assert tick_cluster_position([100.0, 101.0]) == 0.5


# ── depth_imbalance_trend ────────────────────────────────────────────────────

class TestDepthImbalanceTrend:
    def test_bids_strengthening(self):
        # Early: balanced; Late: bid-heavy
        points = [
            (1000, 50.0, 50.0),
            (2000, 55.0, 50.0),
            (3000, 60.0, 45.0),
            (4000, 70.0, 40.0),
            (5000, 80.0, 35.0),
            (6000, 90.0, 30.0),
        ]
        result = depth_imbalance_trend(points)
        assert result > 0, f"Expected positive for bid strengthening, got {result}"

    def test_asks_strengthening(self):
        # Early: bid-heavy; Late: ask-heavy
        points = [
            (1000, 90.0, 30.0),
            (2000, 80.0, 35.0),
            (3000, 70.0, 45.0),
            (4000, 50.0, 60.0),
            (5000, 40.0, 70.0),
            (6000, 30.0, 80.0),
        ]
        result = depth_imbalance_trend(points)
        assert result < 0, f"Expected negative for ask strengthening, got {result}"

    def test_insufficient_data(self):
        assert depth_imbalance_trend([(1000, 50.0, 50.0)]) == 0.0


# ── compute_deep_features ────────────────────────────────────────────────────

class TestComputeDeepFeatures:
    def test_returns_all_keys(self):
        mids = [3300.0 + i * 0.1 for i in range(20)]
        deltas = [mids[i] - mids[i - 1] for i in range(1, len(mids))]
        ts = list(range(1000, 1000 + len(mids)))
        spreads = [0.001] * len(mids)
        depth = [(t, 50.0, 45.0) for t in range(1000, 1010)]
        result = compute_deep_features(
            mids=mids, move_deltas=deltas, spot_ts=ts,
            spread_pcts=spreads, depth_points=depth,
        )
        expected_keys = {
            "tick_acceleration", "adverse_flow_streak", "depth_absorption_rate",
            "micro_volatility", "spread_trajectory", "vwap_distance",
            "tick_cluster_position", "depth_imbalance_trend",
        }
        assert expected_keys == set(result.keys())
        for k, v in result.items():
            assert isinstance(v, float), f"{k} should be float, got {type(v)}"

    def test_empty_data_returns_neutral(self):
        result = compute_deep_features(
            mids=[], move_deltas=[], spot_ts=[], spread_pcts=[], depth_points=[],
        )
        assert result["tick_acceleration"] == 0.0
        assert result["adverse_flow_streak"] == 0.0
        assert result["depth_absorption_rate"] == 1.0
        assert result["micro_volatility"] == 0.0
        assert result["tick_cluster_position"] == 0.5


# ── compute_entry_sharpness_score ────────────────────────────────────────────

class TestComputeEntrySharpnessScore:
    def _sharp_features(self) -> dict:
        """Features representing a strong, aligned long entry."""
        return {
            "delta_proxy": 0.15,
            "tick_up_ratio": 0.72,
            "rejection_ratio": 0.08,
            "depth_imbalance": 0.06,
            "depth_refill_shift": 0.05,
            "spread_expansion": 1.02,
            "bar_volume_proxy": 0.55,
            "tick_acceleration": 0.12,
            "adverse_flow_streak": 0.05,
            "depth_absorption_rate": 1.8,
            "micro_volatility": 0.005,
            "spread_trajectory": -0.0002,
            "vwap_distance": -0.01,
            "tick_cluster_position": 0.35,
            "depth_imbalance_trend": 0.06,
        }

    def _knife_features(self) -> dict:
        """Features representing a terrible knife entry (long direction)."""
        return {
            "delta_proxy": -0.18,
            "tick_up_ratio": 0.25,
            "rejection_ratio": 0.05,
            "depth_imbalance": -0.08,
            "depth_refill_shift": -0.06,
            "spread_expansion": 1.18,
            "bar_volume_proxy": 0.12,
            "tick_acceleration": -0.15,
            "adverse_flow_streak": 0.65,
            "depth_absorption_rate": 0.3,
            "micro_volatility": 0.04,
            "spread_trajectory": 0.0008,
            "vwap_distance": 0.05,
            "tick_cluster_position": 0.8,
            "depth_imbalance_trend": -0.08,
        }

    def test_sharp_scenario(self):
        result = compute_entry_sharpness_score(self._sharp_features(), "long")
        assert result["sharpness_score"] >= 70, f"Expected >=70 for sharp, got {result['sharpness_score']}"
        assert result["sharpness_band"] == "sharp"

    def test_knife_scenario(self):
        result = compute_entry_sharpness_score(self._knife_features(), "long")
        assert result["sharpness_score"] < 30, f"Expected <30 for knife, got {result['sharpness_score']}"
        assert result["sharpness_band"] == "knife"

    def test_neutral_scenario(self):
        features = {
            "delta_proxy": 0.02,
            "tick_up_ratio": 0.52,
            "rejection_ratio": 0.15,
            "depth_imbalance": 0.01,
            "depth_refill_shift": 0.0,
            "spread_expansion": 1.05,
            "bar_volume_proxy": 0.35,
            "tick_acceleration": 0.0,
            "adverse_flow_streak": 0.15,
            "depth_absorption_rate": 1.0,
            "micro_volatility": 0.012,
            "spread_trajectory": 0.0,
            "vwap_distance": 0.0,
            "tick_cluster_position": 0.5,
            "depth_imbalance_trend": 0.0,
        }
        result = compute_entry_sharpness_score(features, "long")
        assert 30 <= result["sharpness_score"] <= 70, f"Expected 30-70 for neutral, got {result['sharpness_score']}"
        assert result["sharpness_band"] in ("caution", "normal")

    def test_missing_deep_features_returns_mid_range(self):
        """When only existing features present (no deep), score should be reasonable."""
        features = {
            "delta_proxy": 0.05,
            "tick_up_ratio": 0.55,
            "rejection_ratio": 0.12,
            "depth_imbalance": 0.02,
            "depth_refill_shift": 0.01,
            "spread_expansion": 1.03,
            "bar_volume_proxy": 0.40,
        }
        result = compute_entry_sharpness_score(features, "long")
        assert 25 <= result["sharpness_score"] <= 80, f"Expected mid-range without deep features, got {result['sharpness_score']}"

    def test_direction_symmetry(self):
        """Same features, opposite directions produce different scores."""
        feat = self._sharp_features()
        long_result = compute_entry_sharpness_score(feat, "long")
        short_result = compute_entry_sharpness_score(feat, "short")
        # Features are aligned for long, so long should score higher than short
        assert long_result["sharpness_score"] > short_result["sharpness_score"], (
            f"Long={long_result['sharpness_score']} should be > Short={short_result['sharpness_score']}"
        )

    def test_return_dict_structure(self):
        result = compute_entry_sharpness_score(self._sharp_features(), "long")
        assert "sharpness_score" in result
        assert "sharpness_band" in result
        assert "momentum_quality" in result
        assert "flow_persistence" in result
        assert "absorption_quality" in result
        assert "price_stability" in result
        assert "positioning_quality" in result
        assert "sharpness_reasons" in result
        assert isinstance(result["sharpness_score"], int)
        assert isinstance(result["sharpness_band"], str)
        assert isinstance(result["sharpness_reasons"], list)

    def test_custom_weights(self):
        """Higher momentum weight should shift score when momentum is strong."""
        feat = self._sharp_features()
        default_result = compute_entry_sharpness_score(feat, "long")
        boosted = compute_entry_sharpness_score(
            feat, "long",
            weights={"momentum": 1.5, "flow": 1.0, "absorption": 1.0, "stability": 1.0, "positioning": 1.0},
        )
        # Boosted momentum should raise score (momentum is strong in sharp features)
        assert boosted["momentum_quality"] > default_result["momentum_quality"]

    def test_score_clamped_0_100(self):
        result = compute_entry_sharpness_score(self._sharp_features(), "long")
        assert 0 <= result["sharpness_score"] <= 100
        result2 = compute_entry_sharpness_score(self._knife_features(), "long")
        assert 0 <= result2["sharpness_score"] <= 100

    def test_empty_features(self):
        """Empty dict should not crash, returns neutral-ish."""
        result = compute_entry_sharpness_score({}, "long")
        assert 0 <= result["sharpness_score"] <= 100
        assert result["sharpness_band"] in ("knife", "caution", "normal", "sharp")
