import json
import shutil
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import learning.strategy_lab_team as strategy_lab_team_module


class StrategyLabTeamAgentTests(unittest.TestCase):
    def setUp(self):
        self.tempdir = Path(tempfile.mkdtemp())
        self.report_dir = self.tempdir / "reports"
        self.runtime_dir = self.tempdir / "runtime"
        self.agent = strategy_lab_team_module.StrategyLabTeamAgent(
            report_dir=str(self.report_dir),
            runtime_dir=str(self.runtime_dir),
        )

    def tearDown(self):
        shutil.rmtree(self.tempdir, ignore_errors=True)

    def test_build_report_classifies_shadow_live_shadow_and_promotions(self):
        report = {
            "ok": True,
            "generated_at": "2026-03-19T04:00:00Z",
            "summary": {"spec_count": 4},
            "candidates": [
                {
                    "symbol": "XAUUSD",
                    "strategy_id": "xau_promote_v1",
                    "family": "xau_scalp_tick_depth_filter",
                    "router_score": 31.2,
                    "walk_forward_score": 24.5,
                    "execution_ready": True,
                    "status": "promotable",
                    "promotion_gate": {"eligible": True, "candidate_resolved": 21, "blockers": []},
                },
                {
                    "symbol": "XAUUSD",
                    "strategy_id": "xau_shadow_live_v1",
                    "family": "xau_scalp_microtrend_follow_up",
                    "router_score": 16.4,
                    "walk_forward_score": 13.1,
                    "execution_ready": True,
                    "status": "experimental",
                    "promotion_gate": {"eligible": False, "candidate_resolved": 9, "blockers": []},
                },
                {
                    "symbol": "XAUUSD",
                    "strategy_id": "xau_shadow_only_v1",
                    "family": "xau_scalp_failed_fade_follow_stop",
                    "router_score": 3.4,
                    "walk_forward_score": 2.8,
                    "execution_ready": True,
                    "status": "sample_collection",
                    "promotion_gate": {"eligible": False, "candidate_resolved": 2, "blockers": ["min_sample:2<4"]},
                },
                {
                    "symbol": "BTCUSD",
                    "strategy_id": "btc_blocked_v1",
                    "family": "btc_weekday_lob_momentum",
                    "router_score": -4.0,
                    "walk_forward_score": -2.0,
                    "execution_ready": True,
                    "status": "blocked",
                    "promotion_gate": {"eligible": False, "candidate_resolved": 5, "blockers": ["negative_score"]},
                },
            ],
        }
        with patch.object(strategy_lab_team_module.config, "STRATEGY_LAB_TEAM_TOPK", 5), \
             patch.object(strategy_lab_team_module.config, "STRATEGY_LAB_TEAM_LIVE_SHADOW_MIN_ROUTER_SCORE", 8.0), \
             patch.object(strategy_lab_team_module.config, "STRATEGY_LAB_TEAM_LIVE_SHADOW_MIN_RESOLVED", 4):
            built = self.agent.build_report(strategy_lab_report=report)

        self.assertTrue(built.get("ok"))
        state = json.loads(self.agent.state_path.read_text(encoding="utf-8"))
        xau = dict((state.get("symbols") or {}).get("XAUUSD") or {})
        self.assertEqual(str((xau.get("strategy_states") or {}).get("xau_promote_v1") or ""), "promotable")
        self.assertEqual(str((xau.get("strategy_states") or {}).get("xau_shadow_live_v1") or ""), "live_shadow")
        self.assertEqual(str((xau.get("strategy_states") or {}).get("xau_shadow_only_v1") or ""), "shadow")
        self.assertIn("xau_scalp_tick_depth_filter", list(xau.get("approved_families") or []))
        self.assertIn("xau_scalp_microtrend_follow_up", list(xau.get("live_shadow_families") or []))
        self.assertIn("xau_scalp_failed_fade_follow_stop", list(xau.get("shadow_families") or []))
        btc = dict((state.get("symbols") or {}).get("BTCUSD") or {})
        self.assertEqual(str((btc.get("family_states") or {}).get("btc_weekday_lob_momentum") or ""), "blocked")

    def test_xau_positive_blocked_candidate_stays_live_shadow_not_fully_blocked(self):
        report = {
            "ok": True,
            "generated_at": "2026-03-19T04:10:00Z",
            "summary": {"spec_count": 1},
            "candidates": [
                {
                    "symbol": "XAUUSD",
                    "strategy_id": "xau_flow_sidecar_v1",
                    "family": "xau_scalp_flow_short_sidecar",
                    "router_score": 43.8,
                    "walk_forward_score": 24.8,
                    "execution_ready": True,
                    "status": "blocked",
                    "promotion_gate": {"eligible": False, "candidate_resolved": 177, "blockers": ["promotion_capable=false"]},
                }
            ],
        }
        with patch.object(strategy_lab_team_module.config, "STRATEGY_LAB_TEAM_TOPK", 5), \
             patch.object(strategy_lab_team_module.config, "STRATEGY_LAB_TEAM_LIVE_SHADOW_MIN_ROUTER_SCORE", 8.0), \
             patch.object(strategy_lab_team_module.config, "STRATEGY_LAB_TEAM_LIVE_SHADOW_MIN_RESOLVED", 4):
            self.agent.build_report(strategy_lab_report=report)

        state = json.loads(self.agent.state_path.read_text(encoding="utf-8"))
        xau = dict((state.get("symbols") or {}).get("XAUUSD") or {})
        self.assertEqual(str((xau.get("strategy_states") or {}).get("xau_flow_sidecar_v1") or ""), "live_shadow")
        self.assertIn("xau_scalp_flow_short_sidecar", list(xau.get("live_shadow_families") or []))

    def test_xau_all_shadow_state_builds_recovery_queue(self):
        report = {
            "ok": True,
            "generated_at": "2026-03-19T04:20:00Z",
            "summary": {"spec_count": 2},
            "candidates": [
                {
                    "symbol": "XAUUSD",
                    "strategy_id": "xau_shadow_a_v1",
                    "family": "xau_scalp_tick_depth_filter",
                    "router_score": 6.8,
                    "walk_forward_score": 7.2,
                    "execution_ready": True,
                    "status": "sample_collection",
                    "promotion_gate": {"eligible": False, "candidate_resolved": 3, "blockers": ["min_sample:3<4"]},
                },
                {
                    "symbol": "XAUUSD",
                    "strategy_id": "xau_shadow_b_v1",
                    "family": "xau_scalp_microtrend_follow_up",
                    "router_score": 5.9,
                    "walk_forward_score": 6.1,
                    "execution_ready": True,
                    "status": "sample_collection",
                    "promotion_gate": {"eligible": False, "candidate_resolved": 2, "blockers": ["min_sample:2<4"]},
                },
            ],
        }
        with patch.object(strategy_lab_team_module.config, "STRATEGY_LAB_TEAM_TOPK", 5), \
             patch.object(strategy_lab_team_module.config, "STRATEGY_LAB_TEAM_LIVE_SHADOW_MIN_ROUTER_SCORE", 8.0), \
             patch.object(strategy_lab_team_module.config, "STRATEGY_LAB_TEAM_LIVE_SHADOW_MIN_RESOLVED", 4), \
             patch.object(strategy_lab_team_module.config, "STRATEGY_LAB_TEAM_RECOVERY_ENABLED", True), \
             patch.object(strategy_lab_team_module.config, "STRATEGY_LAB_TEAM_RECOVERY_TOPK", 2):
            self.agent.build_report(strategy_lab_report=report)

        state = json.loads(self.agent.state_path.read_text(encoding="utf-8"))
        xau = dict((state.get("symbols") or {}).get("XAUUSD") or {})
        self.assertEqual(int((xau.get("summary") or {}).get("recovery_count", 0) or 0), 2)
        self.assertIn("xau_scalp_tick_depth_filter", list(xau.get("recovery_families") or []))
        self.assertIn("xau_scalp_microtrend_follow_up", list(xau.get("recovery_families") or []))


if __name__ == "__main__":
    unittest.main()
