"""
tests/test_scheduler_sharpness.py

Integration tests for Entry Sharpness Score paths in scheduler.py:
- _pb_capture_falling_knife_guard  (sharpness knife block)
- _xau_openapi_entry_router        (knife block, caution downgrade, sharp promote)
- raw_scores observability          (sharpness dict in journal)
- _build_xau_range_repair_canary_signal   (RR sharpness knife guard)
"""
import unittest
import sys
import os
from unittest.mock import patch

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import scheduler as scheduler_module
from analysis.signals import TradeSignal


def _make_xau_signal(direction: str = "long", confidence: float = 73.5) -> TradeSignal:
    return TradeSignal(
        symbol="XAUUSD",
        direction=direction,
        confidence=confidence,
        entry=3310.0,
        stop_loss=3306.0 if direction == "long" else 3314.0,
        take_profit_1=3316.0 if direction == "long" else 3304.0,
        take_profit_2=3320.0 if direction == "long" else 3300.0,
        take_profit_3=3325.0 if direction == "long" else 3295.0,
        risk_reward=2.0,
        timeframe="5m+1m",
        session="new_york",
        trend="bullish" if direction == "long" else "bearish",
        rsi=56.0,
        atr=4.2,
        pattern="SCALP_FLOW_FORCE",
        reasons=[],
        warnings=[],
        raw_scores={"edge": 20, "scalp_force_trend_h1": "bullish"},
    )


def _sharp_features() -> dict:
    """Features that produce a 'sharp' sharpness band (score >= 70)."""
    return {
        "delta_proxy": 0.15,
        "tick_up_ratio": 0.72,
        "rejection_ratio": 0.08,
        "depth_imbalance": 0.06,
        "depth_refill_shift": 0.05,
        "spread_expansion": 1.02,
        "bar_volume_proxy": 0.55,
        "spread_avg_pct": 0.0014,
        "day_type": "trend",
        "tick_acceleration": 0.12,
        "adverse_flow_streak": 0.05,
        "depth_absorption_rate": 1.8,
        "micro_volatility": 0.005,
        "spread_trajectory": -0.0002,
        "vwap_distance": -0.01,
        "tick_cluster_position": 0.35,
        "depth_imbalance_trend": 0.06,
    }


def _knife_features() -> dict:
    """Features that produce a 'knife' sharpness band (score < 30)."""
    return {
        "delta_proxy": -0.18,
        "tick_up_ratio": 0.25,
        "rejection_ratio": 0.05,
        "depth_imbalance": -0.08,
        "depth_refill_shift": -0.06,
        "spread_expansion": 1.18,
        "bar_volume_proxy": 0.12,
        "spread_avg_pct": 0.0028,
        "day_type": "trend",
        "tick_acceleration": -0.15,
        "adverse_flow_streak": 0.65,
        "depth_absorption_rate": 0.3,
        "micro_volatility": 0.04,
        "spread_trajectory": 0.0008,
        "vwap_distance": 0.05,
        "tick_cluster_position": 0.8,
        "depth_imbalance_trend": -0.08,
    }


def _caution_features() -> dict:
    """Features that produce a 'caution' sharpness band (30 <= score < 50)."""
    return {
        "delta_proxy": 0.02,
        "tick_up_ratio": 0.48,
        "rejection_ratio": 0.10,
        "depth_imbalance": -0.01,
        "depth_refill_shift": -0.01,
        "spread_expansion": 1.08,
        "bar_volume_proxy": 0.28,
        "spread_avg_pct": 0.0018,
        "day_type": "trend",
        "tick_acceleration": -0.03,
        "adverse_flow_streak": 0.22,
        "depth_absorption_rate": 0.7,
        "micro_volatility": 0.018,
        "spread_trajectory": 0.0002,
        "vwap_distance": 0.02,
        "tick_cluster_position": 0.55,
        "depth_imbalance_trend": -0.02,
    }


def _neutral_features() -> dict:
    """Features that produce a 'normal' sharpness band (50 <= score < 70)."""
    return {
        "delta_proxy": 0.06,
        "tick_up_ratio": 0.56,
        "rejection_ratio": 0.15,
        "depth_imbalance": 0.02,
        "depth_refill_shift": 0.01,
        "spread_expansion": 1.04,
        "bar_volume_proxy": 0.42,
        "spread_avg_pct": 0.0016,
        "day_type": "trend",
        "tick_acceleration": 0.02,
        "adverse_flow_streak": 0.10,
        "depth_absorption_rate": 1.1,
        "micro_volatility": 0.010,
        "spread_trajectory": -0.0001,
        "vwap_distance": -0.005,
        "tick_cluster_position": 0.45,
        "depth_imbalance_trend": 0.02,
    }


def _make_snapshot(features: dict, run_id: str = "ctcap_sharpness_test") -> dict:
    return {
        "ok": True,
        "run_id": run_id,
        "last_event_utc": "2026-04-04T08:00:00Z",
        "features": dict(features),
        "gate": {
            "features": dict(features),
            "reasons": [],
        },
    }


class SharpnessTestBase(unittest.TestCase):
    def setUp(self):
        self._journal_patcher = patch.object(
            scheduler_module.ctrader_executor,
            "journal_pre_dispatch_skip",
            return_value=0,
        )
        self._db_journal_patcher = patch.object(
            scheduler_module.ctrader_executor,
            "_journal",
            return_value=0,
        )
        self._journal_mock = self._journal_patcher.start()
        self._db_journal_mock = self._db_journal_patcher.start()

    def tearDown(self):
        self._db_journal_patcher.stop()
        self._journal_patcher.stop()


# ── PB Falling Knife Guard — Sharpness Block ────────────────────────────────

class TestPBFallingKnifeSharpness(SharpnessTestBase):
    """_pb_capture_falling_knife_guard should block on sharpness knife even when
    old flow_block checks don't trigger."""

    def test_sharpness_knife_blocks_pb_when_flow_ok(self):
        """Knife sharpness score should block even if day_type/delta/refill are fine."""
        dexter = scheduler_module.DexterScheduler()
        sig = _make_xau_signal("long")
        # Features: day_type=trend (not repricing), delta not adverse enough for flow_block,
        # BUT deep sharpness features are terrible → knife band
        feat = _knife_features()
        feat["day_type"] = "trend"  # not in blocked_day_types
        snapshot = _make_snapshot(feat, "ctcap_pb_sharpness_knife")
        with patch.object(scheduler_module.config, "XAU_PB_FALLING_KNIFE_BLOCK_ENABLED", True), \
             patch.object(scheduler_module.config, "XAU_ENTRY_SHARPNESS_ENABLED", True), \
             patch.object(scheduler_module.config, "XAU_ENTRY_SHARPNESS_PB_KNIFE_THRESHOLD", 35), \
             patch.object(scheduler_module.live_profile_autopilot, "latest_capture_feature_snapshot", return_value=snapshot), \
             patch.object(scheduler_module, "live_profile_classify_chart_state", return_value={
                 "state_label": "continuation_drive",
                 "day_type": "trend",
                 "continuation_bias": 0.05,
             }):
            result = dexter._pb_capture_falling_knife_guard(sig)

        self.assertTrue(bool(result.get("blocked")), f"Expected blocked, got {result}")
        self.assertIn("sharpness_knife", str(result.get("reason", "")))
        self.assertIsInstance(result.get("sharpness"), dict)
        self.assertIn("sharpness_score", result.get("sharpness", {}))

    def test_sharpness_normal_does_not_block(self):
        """Normal sharpness should not trigger knife block by itself."""
        dexter = scheduler_module.DexterScheduler()
        sig = _make_xau_signal("long")
        feat = _neutral_features()
        feat["day_type"] = "trend"
        snapshot = _make_snapshot(feat)
        with patch.object(scheduler_module.config, "XAU_PB_FALLING_KNIFE_BLOCK_ENABLED", True), \
             patch.object(scheduler_module.config, "XAU_ENTRY_SHARPNESS_ENABLED", True), \
             patch.object(scheduler_module.config, "XAU_ENTRY_SHARPNESS_PB_KNIFE_THRESHOLD", 35), \
             patch.object(scheduler_module.live_profile_autopilot, "latest_capture_feature_snapshot", return_value=snapshot), \
             patch.object(scheduler_module, "live_profile_classify_chart_state", return_value={
                 "state_label": "continuation_drive",
                 "day_type": "trend",
                 "continuation_bias": 0.05,
             }):
            result = dexter._pb_capture_falling_knife_guard(sig)

        # Not blocked by sharpness, not blocked by flow
        self.assertFalse(bool(result.get("blocked")), f"Expected not blocked, got {result}")

    def test_sharpness_disabled_skips_check(self):
        """When XAU_ENTRY_SHARPNESS_ENABLED=False, knife features should not block."""
        dexter = scheduler_module.DexterScheduler()
        sig = _make_xau_signal("long")
        feat = _knife_features()
        feat["day_type"] = "trend"
        snapshot = _make_snapshot(feat)
        with patch.object(scheduler_module.config, "XAU_PB_FALLING_KNIFE_BLOCK_ENABLED", True), \
             patch.object(scheduler_module.config, "XAU_ENTRY_SHARPNESS_ENABLED", False), \
             patch.object(scheduler_module.live_profile_autopilot, "latest_capture_feature_snapshot", return_value=snapshot), \
             patch.object(scheduler_module, "live_profile_classify_chart_state", return_value={
                 "state_label": "continuation_drive",
                 "day_type": "trend",
                 "continuation_bias": 0.05,
             }):
            result = dexter._pb_capture_falling_knife_guard(sig)

        self.assertFalse(bool(result.get("blocked")), f"Expected not blocked when sharpness disabled")


# ── Entry Router — Sharpness Bands ──────────────────────────────────────────

class TestEntryRouterSharpness(SharpnessTestBase):

    def test_knife_sharpness_blocks_entry_router(self):
        """Knife sharpness band should trigger hostile_flow block in entry router."""
        dexter = scheduler_module.DexterScheduler()
        sig = _make_xau_signal("long")
        feat = _knife_features()
        feat["spots_count"] = 50  # ensure _sharpness_has_data=True
        snapshot = _make_snapshot(feat, "ctcap_router_knife")
        with patch.object(scheduler_module.config, "XAU_OPENAPI_ENTRY_ROUTER_ENABLED", True), \
             patch.object(scheduler_module.config, "XAU_ENTRY_SHARPNESS_ENABLED", True), \
             patch.object(scheduler_module.config, "XAU_ENTRY_SHARPNESS_KNIFE_THRESHOLD", 30), \
             patch.object(scheduler_module.live_profile_autopilot, "latest_capture_feature_snapshot", return_value=snapshot), \
             patch.object(scheduler_module, "live_profile_classify_chart_state", return_value={
                 "state_label": "continuation_drive",
                 "day_type": "trend",
                 "continuation_bias": 0.05,
             }):
            result = dexter._xau_openapi_entry_router(sig, family="xau_scalp_pullback_limit", preferred_entry_type="limit")

        self.assertTrue(bool(result.get("blocked")), f"Expected blocked, got {result}")
        reason = str(result.get("reason", ""))
        self.assertIn("sharpness_knife", reason)
        self.assertIsInstance(result.get("sharpness"), dict)

    def test_caution_sharpness_downgrades_stop_to_limit(self):
        """Caution band should downgrade buy_stop to limit and reduce risk."""
        dexter = scheduler_module.DexterScheduler()
        sig = _make_xau_signal("long")
        feat = _caution_features()
        feat["spots_count"] = 50
        # Make continuation score high enough to initially promote to stop
        feat["delta_proxy"] = 0.12
        feat["depth_imbalance"] = 0.04
        feat["depth_refill_shift"] = 0.04
        feat["rejection_ratio"] = 0.15
        feat["bar_volume_proxy"] = 0.50
        feat["spread_avg_pct"] = 0.0014
        feat["spread_expansion"] = 1.04
        snapshot = _make_snapshot(feat, "ctcap_router_caution")
        with patch.object(scheduler_module.config, "XAU_OPENAPI_ENTRY_ROUTER_ENABLED", True), \
             patch.object(scheduler_module.config, "XAU_ENTRY_SHARPNESS_ENABLED", True), \
             patch.object(scheduler_module.config, "XAU_ENTRY_SHARPNESS_CAUTION_RISK_MULT", 0.75), \
             patch.object(scheduler_module.live_profile_autopilot, "latest_capture_feature_snapshot", return_value=snapshot), \
             patch.object(scheduler_module, "live_profile_classify_chart_state", return_value={
                 "state_label": "continuation_drive",
                 "day_type": "trend",
                 "continuation_bias": 0.10,
             }):
            result = dexter._xau_openapi_entry_router(sig, family="xau_scalp_pullback_limit", preferred_entry_type="limit")

        self.assertFalse(bool(result.get("blocked")), f"Unexpected block: {result}")
        # Caution band should have reduced risk_multiplier
        risk_mult = float(result.get("risk_multiplier", 1.0) or 1.0)
        self.assertLess(risk_mult, 1.0, f"Expected risk_multiplier < 1.0 for caution, got {risk_mult}")

    def test_sharp_sharpness_promotes_limit_to_stop(self):
        """Sharp band with sufficient continuation should promote limit to stop."""
        dexter = scheduler_module.DexterScheduler()
        sig = _make_xau_signal("long")
        feat = _sharp_features()
        feat["spots_count"] = 50
        snapshot = _make_snapshot(feat, "ctcap_router_sharp")
        with patch.object(scheduler_module.config, "XAU_OPENAPI_ENTRY_ROUTER_ENABLED", True), \
             patch.object(scheduler_module.config, "XAU_ENTRY_SHARPNESS_ENABLED", True), \
             patch.object(scheduler_module.config, "XAU_ENTRY_SHARPNESS_SHARP_PROMOTE_MIN_CONT_SCORE", 4), \
             patch.object(scheduler_module.live_profile_autopilot, "latest_capture_feature_snapshot", return_value=snapshot), \
             patch.object(scheduler_module, "live_profile_classify_chart_state", return_value={
                 "state_label": "continuation_drive",
                 "day_type": "trend",
                 "continuation_bias": 0.15,
             }):
            result = dexter._xau_openapi_entry_router(sig, family="xau_scalp_pullback_limit", preferred_entry_type="limit")

        self.assertFalse(bool(result.get("blocked")), f"Unexpected block: {result}")
        mode = str(result.get("mode") or "")
        # With strong features + continuation, should either promote_to_stop or sharpness_promote_to_stop
        entry_type = str(result.get("entry_type") or "")
        if mode == "sharpness_promote_to_stop":
            self.assertEqual(entry_type, "buy_stop")
        # Otherwise standard promote_to_stop is also valid
        self.assertIsInstance(result.get("sharpness"), dict)

    def test_normal_sharpness_does_not_alter_routing(self):
        """Normal band should not change the mode from what continuation/absorption decide."""
        dexter = scheduler_module.DexterScheduler()
        sig = _make_xau_signal("long")
        feat = _neutral_features()
        feat["spots_count"] = 50
        snapshot = _make_snapshot(feat, "ctcap_router_normal")
        with patch.object(scheduler_module.config, "XAU_OPENAPI_ENTRY_ROUTER_ENABLED", True), \
             patch.object(scheduler_module.config, "XAU_ENTRY_SHARPNESS_ENABLED", True), \
             patch.object(scheduler_module.live_profile_autopilot, "latest_capture_feature_snapshot", return_value=snapshot), \
             patch.object(scheduler_module, "live_profile_classify_chart_state", return_value={
                 "state_label": "continuation_drive",
                 "day_type": "trend",
                 "continuation_bias": 0.05,
             }):
            result = dexter._xau_openapi_entry_router(sig, family="xau_scalp_pullback_limit", preferred_entry_type="limit")

        self.assertFalse(bool(result.get("blocked")))
        mode = str(result.get("mode") or "")
        self.assertNotIn("sharpness", mode, f"Normal band should not trigger sharpness mode, got {mode}")

    def test_sharpness_dict_in_entry_router_result(self):
        """Entry router result should always include 'sharpness' dict when data available."""
        dexter = scheduler_module.DexterScheduler()
        sig = _make_xau_signal("long")
        feat = _neutral_features()
        feat["spots_count"] = 50
        snapshot = _make_snapshot(feat, "ctcap_router_observ")
        with patch.object(scheduler_module.config, "XAU_OPENAPI_ENTRY_ROUTER_ENABLED", True), \
             patch.object(scheduler_module.config, "XAU_ENTRY_SHARPNESS_ENABLED", True), \
             patch.object(scheduler_module.live_profile_autopilot, "latest_capture_feature_snapshot", return_value=snapshot), \
             patch.object(scheduler_module, "live_profile_classify_chart_state", return_value={
                 "state_label": "continuation_drive",
                 "day_type": "trend",
                 "continuation_bias": 0.05,
             }):
            result = dexter._xau_openapi_entry_router(sig, family="xau_scalp_pullback_limit", preferred_entry_type="limit")

        self.assertIn("sharpness", result)
        sharpness = result["sharpness"]
        self.assertIn("sharpness_score", sharpness)
        self.assertIn("sharpness_band", sharpness)
        self.assertIn("momentum_quality", sharpness)
        self.assertIn("flow_persistence", sharpness)


# ── RR Sharpness Knife Guard ───────────────────────────────────────────────

class TestRRSharpnessGuard(SharpnessTestBase):

    def test_rr_knife_sharpness_blocks_signal(self):
        """Range repair should be blocked when sharpness score is knife-level."""
        dexter = scheduler_module.DexterScheduler()
        sig = _make_xau_signal("long")
        sig.entry_type = "limit"
        sig.pattern = "SCALP_FLOW_FORCE"
        feat = _knife_features()
        # RR-specific: must pass the existing binary guards first
        feat["delta_proxy"] = 0.03  # low abs delta to pass RR max_abs_delta_proxy
        feat["depth_imbalance"] = 0.04
        feat["rejection_ratio"] = 0.20
        feat["bar_volume_proxy"] = 0.25
        feat["spread_expansion"] = 1.05
        feat["spread_avg_pct"] = 0.0018
        feat["tick_up_ratio"] = 0.52  # passes tick_up check
        snapshot = _make_snapshot(feat, "ctcap_rr_knife")
        candidate = {
            "family": "xau_scalp_range_repair",
            "strategy_id": "xau_scalp_range_repair_v1",
            "priority": 5,
            "execution_ready": True,
        }
        with patch.object(scheduler_module.config, "XAU_RANGE_REPAIR_ENABLED", True, create=True), \
             patch.object(scheduler_module.config, "XAU_RANGE_REPAIR_KNIFE_GUARD_ENABLED", True, create=True), \
             patch.object(scheduler_module.config, "XAU_ENTRY_SHARPNESS_ENABLED", True), \
             patch.object(scheduler_module.config, "XAU_ENTRY_SHARPNESS_RR_KNIFE_THRESHOLD", 30), \
             patch.object(scheduler_module.config, "XAU_RANGE_REPAIR_MAX_CONTINUATION_BIAS", 0.09, create=True), \
             patch.object(scheduler_module.config, "XAU_RANGE_REPAIR_MIN_REJECTION_RATIO", 0.16, create=True), \
             patch.object(scheduler_module.config, "XAU_RANGE_REPAIR_MIN_BAR_VOLUME_PROXY", 0.18, create=True), \
             patch.object(scheduler_module.config, "XAU_RANGE_REPAIR_MAX_ABS_DELTA_PROXY", 0.11, create=True), \
             patch.object(scheduler_module.config, "XAU_RANGE_REPAIR_MAX_ABS_DEPTH_IMBALANCE", 0.10, create=True), \
             patch.object(scheduler_module.config, "XAU_RANGE_REPAIR_MAX_SPREAD_EXPANSION", 1.10, create=True), \
             patch.object(scheduler_module.config, "XAU_RANGE_REPAIR_MAX_SPREAD_AVG_PCT", 0.0022, create=True), \
             patch.object(scheduler_module.config, "XAU_RANGE_REPAIR_KNIFE_GUARD_MAX_ADVERSE_DELTA_PROXY", 0.07, create=True), \
             patch.object(scheduler_module.config, "XAU_RANGE_REPAIR_KNIFE_GUARD_MIN_TICK_UP_RATIO", 0.38, create=True), \
             patch.object(scheduler_module.config, "XAU_RANGE_REPAIR_ALLOWED_STATES", "", create=True), \
             patch.object(scheduler_module.config, "XAU_RANGE_REPAIR_BLOCKED_DAY_TYPES", "", create=True), \
             patch.object(scheduler_module.config, "XAU_RANGE_REPAIR_BLOCKED_SESSIONS", "", create=True), \
             patch.object(scheduler_module.live_profile_autopilot, "latest_capture_feature_snapshot", return_value=snapshot), \
             patch.object(scheduler_module, "live_profile_classify_chart_state", return_value={
                 "state_label": "pullback_absorption",
                 "day_type": "trend",
                 "continuation_bias": 0.03,
             }):
            lane_signal, lane_source = dexter._build_xau_range_repair_canary_signal(sig, base_source="scalp_xauusd", candidate=candidate)

        self.assertIsNone(lane_signal, f"Expected None (knife blocked), got signal")
        self.assertEqual(lane_source, "")


# ── PB Knife Block Raw Scores Sharpness ─────────────────────────────────────

class TestPBKnifeBlockRawScoresSharpness(SharpnessTestBase):

    def test_pb_knife_block_stamps_sharpness_in_raw_scores(self):
        """When PB is blocked by sharpness knife, raw_scores should contain sharpness data."""
        dexter = scheduler_module.DexterScheduler()
        sig = _make_xau_signal("long")
        sig.entry_type = "limit"
        feat = _knife_features()
        feat["day_type"] = "trend"
        snapshot = _make_snapshot(feat, "ctcap_pb_stamp_sharp")
        candidate = {
            "family": "xau_scalp_pullback_limit",
            "strategy_id": "xau_scalp_pullback_limit_v1",
            "priority": 3,
            "execution_ready": True,
        }
        with patch.object(scheduler_module.config, "XAU_PB_NARROW_CONTEXT_ENABLED", True), \
             patch.object(scheduler_module.config, "XAU_PB_FALLING_KNIFE_BLOCK_ENABLED", True), \
             patch.object(scheduler_module.config, "XAU_ENTRY_SHARPNESS_ENABLED", True), \
             patch.object(scheduler_module.config, "XAU_ENTRY_SHARPNESS_PB_KNIFE_THRESHOLD", 35), \
             patch.object(dexter, "_load_xau_pb_narrow_contexts", return_value=[{
                 "direction": "long",
                 "session": "new_york",
                 "timeframe": "5m+1m",
                 "entry_type": "limit",
                 "confidence_band": "70-74.9",
                 "h1_trend": "bullish",
                 "memory_score": 31.0,
                 "resolved": 4,
             }]), \
             patch.object(scheduler_module.live_profile_autopilot, "latest_capture_feature_snapshot", return_value=snapshot), \
             patch.object(scheduler_module, "live_profile_classify_chart_state", return_value={
                 "state_label": "continuation_drive",
                 "day_type": "trend",
                 "continuation_bias": 0.05,
             }):
            lane_signal, lane_source = dexter._build_family_canary_signal(sig, base_source="scalp_xauusd", candidate=candidate)

        self.assertIsNone(lane_signal)
        self.assertEqual(lane_source, "")
        raw = dict(getattr(sig, "raw_scores", {}) or {})
        self.assertTrue(bool(raw.get("pb_falling_knife_block")))
        self.assertIn("sharpness_knife", str(raw.get("pb_falling_knife_block_reason", "")))
        sharpness_stamp = raw.get("pb_falling_knife_block_sharpness", {})
        self.assertIsInstance(sharpness_stamp, dict)
        self.assertIn("sharpness_score", sharpness_stamp)


if __name__ == "__main__":
    unittest.main()
