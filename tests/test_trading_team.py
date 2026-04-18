import json
import shutil
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import learning.trading_team as trading_team_module


class TradingTeamAgentTests(unittest.TestCase):
    def setUp(self):
        self.tempdir = Path(tempfile.mkdtemp())
        self.report_dir = self.tempdir / "reports"
        self.runtime_dir = self.tempdir / "runtime"
        self.agent = trading_team_module.TradingTeamAgent(
            report_dir=str(self.report_dir),
            runtime_dir=str(self.runtime_dir),
        )

    def tearDown(self):
        shutil.rmtree(self.tempdir, ignore_errors=True)

    def test_build_report_creates_advisory_runtime_state_from_manager_analysis(self):
        manager_report = {
            "ok": True,
            "generated_at": "2026-03-19T03:00:00Z",
            "summary": {"rows": 18},
            "opportunity_feed": {
                "symbols": {
                    "XAUUSD": {
                        "family_priority_map": {
                            "xau_scalp_flow_short_sidecar": 96.0,
                            "xau_scalp_microtrend_follow_up": 88.0,
                            "xau_scalp_pullback_limit": 72.0,
                        },
                        "priority_families": [
                            "xau_scalp_flow_short_sidecar",
                            "xau_scalp_microtrend_follow_up",
                            "xau_scalp_pullback_limit",
                        ],
                        "coaching": ["prefer short continuation follow-up"],
                    }
                }
            },
            "symbols": [
                {
                    "symbol": "XAUUSD",
                    "selected_family": "xau_scalp_pullback_limit",
                    "best_family_today": {
                        "key": ["xau_scalp_tick_depth_filter"],
                        "pnl_usd": 11.4,
                        "resolved": 4,
                        "win_rate": 0.75,
                    },
                    "family_leaderboard_today": [
                        {
                            "key": ["xau_scalp_tick_depth_filter"],
                            "pnl_usd": 11.4,
                            "resolved": 4,
                            "win_rate": 0.75,
                        },
                        {
                            "key": ["xau_scalp_microtrend_follow_up"],
                            "pnl_usd": 4.2,
                            "resolved": 3,
                            "win_rate": 0.67,
                        },
                    ],
                    "reason_memory_recommendations": {
                        "matched_count": 3,
                        "preferred_family": "xau_scalp_microtrend_follow_up",
                        "avoid_families": ["xau_scalp_pullback_limit"],
                        "family_scores": [
                            {"family": "xau_scalp_microtrend_follow_up", "score": 0.22},
                            {"family": "xau_scalp_pullback_limit", "score": -0.14},
                        ],
                    },
                    "parallel_family_recommendations": {
                        "active": True,
                        "allowed_families": [
                            "xau_scalp_flow_short_sidecar",
                            "xau_scalp_microtrend_follow_up",
                            "xau_scalp_tick_depth_filter",
                        ],
                        "max_same_direction_families": 3,
                    },
                    "hedge_lane_recommendations": {
                        "active": True,
                        "mode": "xau_manager_hedge_transition",
                        "reason": "short continuation hedge ready",
                        "allowed_families": ["xau_scalp_flow_short_sidecar"],
                        "max_per_symbol": 1,
                        "risk_multiplier": 0.65,
                    },
                    "opportunity_sidecar_recommendations": {
                        "active": True,
                        "mode": "xau_short_flow_sidecar",
                        "reason": "short continuation state",
                    },
                    "order_care_recommendations": {
                        "active": True,
                        "mode": "continuation_fail_fast",
                        "reason": "recent continuation losses failed to extend",
                        "allowed_sources": ["scalp_xauusd:fss:canary"],
                        "loss_count": 2,
                        "review_window": [{"source": "scalp_xauusd:fss:canary", "pnl_usd": -3.2}],
                        "overrides": {"close_score": 4, "trim_tp_r": 0.4},
                    },
                    "micro_regime_refresh": {
                        "active": True,
                        "dominant_direction": "short",
                        "state_label": "continuation_drive",
                    },
                    "cluster_loss_guard_recommendations": {
                        "active": True,
                        "blocked_direction": "long",
                        "losses": 3,
                        "resolved": 3,
                        "pnl_usd": -8.3,
                    },
                    "manager_findings": ["cluster-loss watch long", "follow the short continuation state"],
                    "recent_order_reviews": [{"family": "xau_scalp_flow_short_sidecar", "pnl_usd": -3.2}],
                    "open_positions": [],
                    "open_orders": [],
                }
            ],
        }
        with patch.object(trading_team_module.config, "get_persistent_canary_strategy_families", return_value={"xau_scalp_pullback_limit", "xau_scalp_breakout_stop"}), \
             patch.object(trading_team_module.config, "get_persistent_canary_experimental_families", return_value={"xau_scalp_tick_depth_filter", "xau_scalp_microtrend_follow_up", "xau_scalp_flow_short_sidecar"}), \
             patch.object(trading_team_module.config, "get_ctrader_xau_active_families", return_value={"xau_scalp_pullback_limit"}):
            report = self.agent.build_report(manager_report=manager_report)

        self.assertTrue(report.get("ok"))
        state = json.loads(self.agent.state_path.read_text(encoding="utf-8"))
        self.assertEqual(str((state.get("xau_family_routing") or {}).get("status") or ""), "active")
        self.assertEqual(str((state.get("xau_family_routing") or {}).get("primary_family") or ""), "xau_scalp_flow_short_sidecar")
        self.assertEqual(str((state.get("xau_parallel_families") or {}).get("status") or ""), "active")
        self.assertEqual(str((state.get("xau_hedge_transition") or {}).get("status") or ""), "active")
        self.assertEqual(str((state.get("xau_order_care") or {}).get("status") or ""), "active")
        self.assertNotIn("xau_cluster_loss_guard", state)
        self.assertEqual(str((state.get("xau_cluster_loss_watch") or {}).get("enforcement") or ""), "advisory_only")

    def test_build_report_incorporates_strategy_lab_promotions_and_probation(self):
        manager_report = {
            "ok": True,
            "generated_at": "2026-03-19T03:00:00Z",
            "summary": {"rows": 3},
            "opportunity_feed": {
                "symbols": {
                    "XAUUSD": {
                        "family_priority_map": {
                            "xau_scalp_pullback_limit": 72.0,
                        },
                        "coaching": ["manager feed sees only pullback for now"],
                    }
                }
            },
            "symbols": [
                {
                    "symbol": "XAUUSD",
                    "selected_family": "xau_scalp_pullback_limit",
                    "manager_findings": ["await better continuation evidence"],
                    "open_positions": [],
                    "open_orders": [],
                }
            ],
        }
        strategy_lab_state = {
            "status": "active",
            "generated_at": "2026-03-19T03:05:00Z",
            "summary": {"promotion_count": 1, "live_shadow_count": 1},
            "symbols": {
                "XAUUSD": {
                    "summary": {"promotion_count": 1, "live_shadow_count": 1, "blocked_count": 0},
                    "promotion_family_priority_map": {"xau_scalp_tick_depth_filter": 28.0},
                    "live_shadow_family_priority_map": {"xau_scalp_microtrend_follow_up": 18.0},
                    "execution_family_priority_map": {
                        "xau_scalp_tick_depth_filter": 28.0,
                        "xau_scalp_microtrend_follow_up": 18.0,
                    },
                    "promotion_queue": [{"strategy_id": "xau_td_v1", "family": "xau_scalp_tick_depth_filter"}],
                    "live_shadow_queue": [{"strategy_id": "xau_mfu_v1", "family": "xau_scalp_microtrend_follow_up"}],
                    "shadow_queue": [],
                }
            },
        }
        with patch.object(trading_team_module.config, "get_persistent_canary_strategy_families", return_value={"xau_scalp_pullback_limit"}), \
             patch.object(trading_team_module.config, "get_persistent_canary_experimental_families", return_value={"xau_scalp_tick_depth_filter", "xau_scalp_microtrend_follow_up"}), \
             patch.object(trading_team_module.config, "get_ctrader_xau_active_families", return_value={"xau_scalp_pullback_limit"}), \
             patch.object(trading_team_module.config, "TRADING_TEAM_STRATEGY_LAB_PROMOTION_SCORE_MULT", 0.5), \
             patch.object(trading_team_module.config, "TRADING_TEAM_STRATEGY_LAB_LIVE_SHADOW_SCORE_MULT", 0.2):
            report = self.agent.build_report(manager_report=manager_report, strategy_lab_state=strategy_lab_state)

        self.assertTrue(report.get("ok"))
        xau = dict((report.get("symbols") or {}).get("XAUUSD") or {})
        execution_desk = dict(xau.get("execution_desk") or {})
        strategy_lab_desk = dict(xau.get("strategy_lab_desk") or {})
        self.assertIn("xau_scalp_tick_depth_filter", list(execution_desk.get("ranked_families") or []))
        self.assertIn("xau_scalp_microtrend_follow_up", list(execution_desk.get("ranked_families") or []))
        self.assertTrue(list(strategy_lab_desk.get("promotion_ready") or []))
        self.assertTrue(list(strategy_lab_desk.get("live_shadow") or []))

    def test_build_report_uses_strategy_lab_recovery_feed_when_no_live_shadow(self):
        manager_report = {
            "ok": True,
            "generated_at": "2026-03-19T03:10:00Z",
            "summary": {"rows": 1},
            "opportunity_feed": {"symbols": {"XAUUSD": {"family_priority_map": {}, "coaching": []}}},
            "symbols": [
                {
                    "symbol": "XAUUSD",
                    "selected_family": "",
                    "manager_findings": ["lab must keep searching instead of flat blocking"],
                    "open_positions": [],
                    "open_orders": [],
                }
            ],
        }
        strategy_lab_state = {
            "status": "active",
            "generated_at": "2026-03-19T03:15:00Z",
            "summary": {"recovery_count": 2},
            "symbols": {
                "XAUUSD": {
                    "summary": {"recovery_count": 2, "promotion_count": 0, "live_shadow_count": 0, "blocked_count": 2},
                    "recovery_family_priority_map": {
                        "xau_scalp_tick_depth_filter": 11.0,
                        "xau_scalp_microtrend_follow_up": 9.5,
                    },
                    "execution_family_priority_map": {
                        "xau_scalp_tick_depth_filter": 11.0,
                        "xau_scalp_microtrend_follow_up": 9.5,
                    },
                    "recovery_queue": [
                        {"strategy_id": "xau_td_recovery_v1", "family": "xau_scalp_tick_depth_filter"},
                        {"strategy_id": "xau_mfu_recovery_v1", "family": "xau_scalp_microtrend_follow_up"},
                    ],
                }
            },
        }
        with patch.object(trading_team_module.config, "get_persistent_canary_strategy_families", return_value={"xau_scalp_pullback_limit"}), \
             patch.object(trading_team_module.config, "get_persistent_canary_experimental_families", return_value={"xau_scalp_tick_depth_filter", "xau_scalp_microtrend_follow_up"}), \
             patch.object(trading_team_module.config, "get_ctrader_xau_active_families", return_value={"xau_scalp_pullback_limit"}), \
             patch.object(trading_team_module.config, "TRADING_TEAM_STRATEGY_LAB_RECOVERY_SCORE_MULT", 0.2):
            report = self.agent.build_report(manager_report=manager_report, strategy_lab_state=strategy_lab_state)

        xau = dict((report.get("symbols") or {}).get("XAUUSD") or {})
        execution_desk = dict(xau.get("execution_desk") or {})
        strategy_lab_desk = dict(xau.get("strategy_lab_desk") or {})
        self.assertIn("xau_scalp_tick_depth_filter", list(execution_desk.get("ranked_families") or []))
        self.assertTrue(list(strategy_lab_desk.get("recovery") or []))

    def test_build_report_carries_active_xau_order_care_from_manager_state(self):
        manager_report = {
            "ok": True,
            "generated_at": "2026-03-19T03:16:00Z",
            "summary": {"rows": 1},
            "opportunity_feed": {"symbols": {"XAUUSD": {"family_priority_map": {"xau_scalp_pullback_limit": 72.0}, "coaching": []}}},
            "symbols": [
                {
                    "symbol": "XAUUSD",
                    "selected_family": "xau_scalp_pullback_limit",
                    "manager_findings": ["manager report did not emit a fresh order-care rec this cycle"],
                    "open_positions": [],
                    "open_orders": [],
                }
            ],
        }
        self.agent.manager_state_path.write_text(
            json.dumps(
                {
                    "xau_order_care": {
                        "status": "active",
                        "mode": "continuation_fail_fast",
                        "reason": "carry recent protection state",
                        "allowed_sources": ["scalp_xauusd:canary", "scalp_xauusd:td:canary"],
                        "loss_count": 2,
                        "review_window": [{"source": "scalp_xauusd:canary", "pnl_usd": -10.4}],
                        "desks": {
                            "fss_confirmation": {
                                "status": "active",
                                "mode": "continuation_fail_fast",
                                "allowed_sources": ["scalp_xauusd:fss:canary"],
                                "overrides": {"desk": "fss_confirmation"},
                            },
                            "limit_retest": {
                                "status": "active",
                                "mode": "retest_absorption_guard",
                                "allowed_sources": ["scalp_xauusd:canary", "scalp_xauusd:td:canary"],
                                "overrides": {"desk": "limit_retest"},
                            },
                        },
                        "overrides": {"be_trigger_r": 0.12, "be_lock_r": 0.01},
                    }
                },
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )
        with patch.object(trading_team_module.config, "get_persistent_canary_strategy_families", return_value={"xau_scalp_pullback_limit"}), \
             patch.object(trading_team_module.config, "get_persistent_canary_experimental_families", return_value={"xau_scalp_tick_depth_filter"}), \
             patch.object(trading_team_module.config, "get_ctrader_xau_active_families", return_value={"xau_scalp_pullback_limit"}):
            report = self.agent.build_report(manager_report=manager_report)

        self.assertTrue(report.get("ok"))
        state = json.loads(self.agent.state_path.read_text(encoding="utf-8"))
        order_care = dict(state.get("xau_order_care") or {})
        self.assertEqual(str(order_care.get("status") or ""), "active")
        self.assertIn("scalp_xauusd:canary", list(order_care.get("allowed_sources") or []))
        self.assertIn("fss_confirmation", dict(order_care.get("desks") or {}))

    def test_build_report_carries_xau_execution_directive_into_team_state(self):
        manager_report = {
            "ok": True,
            "generated_at": "2026-03-19T03:18:00Z",
            "summary": {"rows": 3},
            "opportunity_feed": {
                "symbols": {
                    "XAUUSD": {
                        "family_priority_map": {
                            "xau_scalp_microtrend": 96.0,
                            "xau_scalp_tick_depth_filter": 90.0,
                            "xau_scalp_flow_short_sidecar": 75.0,
                        },
                        "priority_families": [
                            "xau_scalp_microtrend",
                            "xau_scalp_tick_depth_filter",
                            "xau_scalp_flow_short_sidecar",
                        ],
                        "coaching": ["manager spotted disagreement and is shifting control"],
                    }
                }
            },
            "symbols": [
                {
                    "symbol": "XAUUSD",
                    "selected_family": "xau_scalp_microtrend",
                    "manager_findings": ["limit shorts were late while confirmation flow held"],
                    "execution_directive_recommendations": {
                        "active": True,
                        "mode": "family_disagreement_limit_pause",
                        "reason": "confirmation short held while short-limit lanes failed on the same run",
                        "blocked_direction": "short",
                        "blocked_entry_types": ["limit", "patience"],
                        "blocked_families": ["xau_scalp_microtrend", "xau_scalp_tick_depth_filter"],
                        "blocked_sources": ["scalp_xauusd:canary", "scalp_xauusd:td:canary"],
                        "preferred_families": ["xau_scalp_flow_short_sidecar"],
                        "preferred_sources": ["scalp_xauusd:fss:canary"],
                        "support_state": "fss_win",
                        "trigger_run_id": "run-disagree-1",
                        "pause_min": 20,
                        "remaining_min": 18.5,
                        "pause_until_utc": "2026-03-19T03:36:00Z",
                        "pair_risk_cap": {"enabled": True, "max_risk_usd": 3.0},
                        "coach_traders": [
                            "manager directive: pause short-limit xau_scalp_microtrend,xau_scalp_tick_depth_filter for 18.5m",
                            "manager directive: let scalp_xauusd:fss:canary lead confirmation shorts",
                        ],
                        "trader_assignments": [
                            {"source": "scalp_xauusd:fss:canary", "family": "xau_scalp_flow_short_sidecar", "task": "lead_confirmation_short", "status": "priority"},
                            {"source": "scalp_xauusd:canary", "family": "xau_scalp_microtrend", "task": "pause_short_limit", "status": "paused"},
                        ],
                    },
                    "open_positions": [],
                    "open_orders": [],
                }
            ],
        }
        with patch.object(trading_team_module.config, "get_persistent_canary_strategy_families", return_value={"xau_scalp_microtrend"}), \
             patch.object(trading_team_module.config, "get_persistent_canary_experimental_families", return_value={"xau_scalp_tick_depth_filter", "xau_scalp_flow_short_sidecar"}), \
             patch.object(trading_team_module.config, "get_ctrader_xau_active_families", return_value={"xau_scalp_microtrend", "xau_scalp_flow_short_sidecar"}):
            report = self.agent.build_report(manager_report=manager_report)

        self.assertTrue(report.get("ok"))
        xau = dict((report.get("symbols") or {}).get("XAUUSD") or {})
        execution_desk = dict(xau.get("execution_desk") or {})
        self.assertEqual(str(execution_desk.get("primary_family") or ""), "xau_scalp_flow_short_sidecar")
        self.assertEqual(list(execution_desk.get("support_all_families") or [])[0], "xau_scalp_flow_short_sidecar")
        state = json.loads(self.agent.state_path.read_text(encoding="utf-8"))
        directive = dict(state.get("xau_execution_directive") or {})
        self.assertEqual(str(directive.get("status") or ""), "active")
        self.assertIn("xau_scalp_flow_short_sidecar", list(directive.get("preferred_families") or []))
        self.assertIn("xau_scalp_microtrend", list(directive.get("blocked_families") or []))

    def test_build_report_carries_xau_regime_transition_into_team_state(self):
        manager_report = {
            "ok": True,
            "generated_at": "2026-03-19T03:19:00Z",
            "summary": {"rows": 2},
            "opportunity_feed": {
                "symbols": {
                    "XAUUSD": {
                        "family_priority_map": {
                            "xau_scalp_range_repair": 84.0,
                            "xau_scalp_microtrend": 70.0,
                        },
                        "priority_families": [
                            "xau_scalp_range_repair",
                            "xau_scalp_microtrend",
                        ],
                        "coaching": ["manager detected rebound/sideway transition"],
                    }
                }
            },
            "symbols": [
                {
                    "symbol": "XAUUSD",
                    "selected_family": "xau_scalp_microtrend",
                    "manager_findings": ["live short continuation degraded into reversal exhaustion"],
                    "regime_transition_recommendations": {
                        "active": True,
                        "mode": "live_range_transition_limit_pause",
                        "reason": "live short continuation degraded to reversal_exhaustion | day=trend rej=0.46 bias=-0.010",
                        "support_state": "range_repair_lead",
                        "current_side": "short",
                        "state_label": "reversal_exhaustion",
                        "opposite_state_label": "reversal_exhaustion",
                        "day_type": "trend",
                        "follow_up_plan": "wait_reversal_confirmation_then_probe",
                        "blocked_direction": "short",
                        "blocked_entry_types": ["limit", "patience"],
                        "blocked_families": ["xau_scalp_microtrend", "xau_scalp_tick_depth_filter"],
                        "blocked_sources": ["scalp_xauusd:canary", "scalp_xauusd:td:canary"],
                        "preferred_families": ["xau_scalp_range_repair"],
                        "preferred_sources": ["scalp_xauusd:rr:canary"],
                        "snapshot_run_id": "capture-range-1",
                        "snapshot_last_event_utc": "2026-03-19T03:18:30Z",
                        "snapshot_features": {"day_type": "trend", "rejection_ratio": 0.46},
                        "pressure": {"scores": {"short": 4.0, "long": 0.0}},
                        "hold_min": 12,
                        "remaining_min": 12.0,
                        "hold_until_utc": "2026-03-19T03:31:00Z",
                    },
                    "open_positions": [],
                    "open_orders": [],
                }
            ],
        }
        with patch.object(trading_team_module.config, "get_persistent_canary_strategy_families", return_value={"xau_scalp_microtrend"}), \
             patch.object(trading_team_module.config, "get_persistent_canary_experimental_families", return_value={"xau_scalp_range_repair"}), \
             patch.object(trading_team_module.config, "get_ctrader_xau_active_families", return_value={"xau_scalp_microtrend", "xau_scalp_range_repair"}):
            report = self.agent.build_report(manager_report=manager_report)

        self.assertTrue(report.get("ok"))
        xau = dict((report.get("symbols") or {}).get("XAUUSD") or {})
        bias_desk = dict(xau.get("bias_desk") or {})
        regime = dict(bias_desk.get("regime_transition") or {})
        self.assertEqual(str(regime.get("status") or ""), "active")
        self.assertEqual(str(regime.get("state_label") or ""), "reversal_exhaustion")
        state = json.loads(self.agent.state_path.read_text(encoding="utf-8"))
        runtime_regime = dict(state.get("xau_regime_transition") or {})
        self.assertEqual(str(runtime_regime.get("status") or ""), "active")
        self.assertIn("xau_scalp_range_repair", list(runtime_regime.get("preferred_families") or []))

    def test_build_report_uses_ct_experiment_live_edge_to_form_production_budget(self):
        manager_report = {
            "ok": True,
            "generated_at": "2026-03-19T03:20:00Z",
            "summary": {"rows": 6},
            "opportunity_feed": {
                "symbols": {
                    "XAUUSD": {
                        "family_priority_map": {
                            "xau_scalp_flow_short_sidecar": 96.0,
                            "xau_scalp_tick_depth_filter": 90.0,
                            "xau_scalp_microtrend_follow_up": 82.0,
                        },
                        "coaching": ["manager still prefers short continuation"],
                    }
                }
            },
            "symbols": [
                {
                    "symbol": "XAUUSD",
                    "selected_family": "xau_scalp_flow_short_sidecar",
                    "manager_findings": ["shift weights by realized edge, not only theory"],
                    "open_positions": [],
                    "open_orders": [],
                }
            ],
        }
        experiment_report = {
            "ok": True,
            "summary": {"tracked_sources": 4},
            "sources": [
                {
                    "symbol": "XAUUSD",
                    "source": "scalp_xauusd:td:canary",
                    "family": "xau_scalp_tick_depth_filter",
                    "closed_total": {"resolved": 5, "wins": 3, "losses": 2, "win_rate": 0.60, "pnl_usd": 6.0, "avg_pnl_usd": 1.2},
                },
                {
                    "symbol": "XAUUSD",
                    "source": "scalp_xauusd:fss:canary",
                    "family": "xau_scalp_flow_short_sidecar",
                    "closed_total": {"resolved": 6, "wins": 4, "losses": 2, "win_rate": 0.67, "pnl_usd": -5.5, "avg_pnl_usd": -0.92},
                },
                {
                    "symbol": "XAUUSD",
                    "source": "scalp_xauusd:mfu:canary",
                    "family": "xau_scalp_microtrend_follow_up",
                    "closed_total": {"resolved": 2, "wins": 2, "losses": 0, "win_rate": 1.0, "pnl_usd": 3.0, "avg_pnl_usd": 1.5},
                },
            ],
            "comparisons": {
                "xau_td_vs_pb_live": {"leader": "xau_scalp_tick_depth_filter"},
                "xau_mfu_vs_broad_microtrend_live": {"leader": "xau_scalp_microtrend_follow_up"},
                "xau_fss_vs_broad_microtrend_live": {"leader": "xau_scalp_microtrend"},
            },
        }
        with patch.object(trading_team_module.config, "get_persistent_canary_strategy_families", return_value={"xau_scalp_pullback_limit"}), \
             patch.object(trading_team_module.config, "get_persistent_canary_experimental_families", return_value={"xau_scalp_tick_depth_filter", "xau_scalp_microtrend_follow_up", "xau_scalp_flow_short_sidecar"}), \
             patch.object(trading_team_module.config, "get_ctrader_xau_active_families", return_value={"xau_scalp_pullback_limit"}), \
             patch.object(trading_team_module.config, "TRADING_TEAM_XAU_LIVE_EDGE_MIN_RESOLVED", 2), \
             patch.object(trading_team_module.config, "TRADING_TEAM_XAU_PRODUCTION_MAX_FAMILIES", 2), \
             patch.object(trading_team_module.config, "TRADING_TEAM_XAU_SAMPLING_MAX_FAMILIES", 2):
            report = self.agent.build_report(manager_report=manager_report, experiment_report=experiment_report)

        xau = dict((report.get("symbols") or {}).get("XAUUSD") or {})
        execution_desk = dict(xau.get("execution_desk") or {})
        state = json.loads(self.agent.state_path.read_text(encoding="utf-8"))
        budget = dict(state.get("xau_family_budget") or {})
        self.assertEqual(str(execution_desk.get("primary_family") or ""), "xau_scalp_tick_depth_filter")
        self.assertEqual(list(budget.get("production_families") or [])[:2], ["xau_scalp_tick_depth_filter", "xau_scalp_microtrend_follow_up"])
        self.assertIn("xau_scalp_flow_short_sidecar", list(budget.get("sampling_families") or []))
        self.assertLess(
            float(((budget.get("family_live_edge_map") or {}).get("xau_scalp_flow_short_sidecar") or {}).get("live_edge_score", 0.0) or 0.0),
            0.0,
        )


if __name__ == "__main__":
    unittest.main()
