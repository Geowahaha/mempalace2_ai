from __future__ import annotations

import json
import sqlite3
import tempfile
import time
import unittest
import gc
from contextlib import ExitStack
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch

from learning.live_profile_autopilot import (
    LiveProfileAutopilot,
    classify_xau_day_type,
    evaluate_xau_tick_depth_filter,
)


def _iso_now() -> str:
    from datetime import datetime, timezone

    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _init_neural_db(path: Path) -> None:
    with sqlite3.connect(str(path)) as conn:
        conn.execute(
            """
            CREATE TABLE neural_gate_decisions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                created_at TEXT NOT NULL,
                source TEXT,
                signal_symbol TEXT,
                broker_symbol TEXT,
                confidence REAL,
                neural_prob REAL,
                min_prob REAL,
                decision TEXT,
                decision_reason TEXT,
                outcome_type TEXT,
                outcome INTEGER,
                pnl_usd REAL,
                features_json TEXT,
                resolved INTEGER DEFAULT 1
            )
            """
        )


def _init_mt5_db(path: Path) -> None:
    with sqlite3.connect(str(path)) as conn:
        conn.execute(
            """
            CREATE TABLE mt5_execution_journal (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                created_at TEXT NOT NULL,
                source TEXT,
                signal_symbol TEXT,
                broker_symbol TEXT,
                resolved INTEGER DEFAULT 0,
                outcome INTEGER,
                pnl REAL
            )
            """
        )


def _init_ctrader_db(path: Path) -> None:
    with sqlite3.connect(str(path)) as conn:
        conn.execute(
            """
            CREATE TABLE execution_journal (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                created_utc TEXT NOT NULL,
                source TEXT,
                symbol TEXT,
                direction TEXT,
                entry_type TEXT,
                confidence REAL,
                entry REAL,
                stop_loss REAL,
                take_profit REAL,
                request_json TEXT,
                response_json TEXT DEFAULT '{}',
                execution_meta_json TEXT,
                status TEXT DEFAULT ''
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE ctrader_deals (
                deal_id INTEGER PRIMARY KEY,
                source TEXT,
                symbol TEXT,
                pnl_usd REAL,
                outcome INTEGER,
                has_close_detail INTEGER,
                journal_id INTEGER,
                execution_utc TEXT,
                raw_json TEXT
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE ctrader_spot_ticks (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                run_id TEXT,
                symbol TEXT,
                event_utc TEXT,
                event_ts INTEGER,
                bid REAL,
                ask REAL,
                spread REAL,
                spread_pct REAL,
                payload_json TEXT
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE ctrader_depth_quotes (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                run_id TEXT,
                symbol TEXT,
                event_utc TEXT,
                event_ts INTEGER,
                side TEXT,
                price REAL,
                size REAL,
                level_index INTEGER,
                payload_json TEXT
            )
            """
        )


class LiveProfileAutopilotTests(unittest.TestCase):
    def setUp(self):
        self._td = tempfile.TemporaryDirectory()
        self.base = Path(self._td.name)
        self.env_local = self.base / ".env.local"
        self.env_local.write_text(
            "\n".join(
                [
                    "CTRADER_ALLOWED_SOURCES=scalp_btcusd:winner,scalp_ethusd:winner,scalp_xauusd,xauusd_scheduled:winner",
                    "CTRADER_STORE_FEED_SOURCES=scalp_btcusd:winner,scalp_ethusd:winner,scalp_xauusd,xauusd_scheduled:winner",
                    "SCALPING_BTC_MIN_CONFIDENCE_WEEKEND=75",
                    "SCALPING_BTC_ALLOWED_SESSIONS_WEEKEND=new_york",
                    "SCALPING_ETH_MIN_CONFIDENCE_WEEKEND=76",
                    "SCALPING_ETH_ALLOWED_SESSIONS_WEEKEND=london,new_york,overlap",
                    "NEURAL_GATE_CANARY_MIN_CONFIDENCE=72",
                    "NEURAL_GATE_CANARY_FIXED_ALLOW_LOW=0.54",
                    "",
                ]
            ),
            encoding="utf-8",
        )
        self.report_dir = self.base / "reports"
        self.runtime_dir = self.base / "runtime"
        self.report_dir.mkdir(parents=True, exist_ok=True)
        self.runtime_dir.mkdir(parents=True, exist_ok=True)
        self.neural_db = self.base / "neural_gate_learning.db"
        self.mt5_db = self.base / "mt5_autopilot.db"
        self.ctrader_db = self.base / "ctrader_openapi.db"
        _init_neural_db(self.neural_db)
        _init_mt5_db(self.mt5_db)
        _init_ctrader_db(self.ctrader_db)
        self.engine = LiveProfileAutopilot(
            report_dir=str(self.report_dir),
            runtime_dir=str(self.runtime_dir),
            env_local_path=str(self.env_local),
            neural_gate_db_path=str(self.neural_db),
            mt5_db_path=str(self.mt5_db),
            ctrader_db_path=str(self.ctrader_db),
        )

    def tearDown(self):
        self.engine = None
        for _ in range(10):
            gc.collect()
            try:
                self._td.cleanup()
                break
            except PermissionError:
                time.sleep(0.05)

    def test_build_missed_opportunity_audit_report_recommends_canary_reinstate(self):
        now = _iso_now()
        with sqlite3.connect(str(self.neural_db)) as conn:
            rows = [
                (now, "scalp_xauusd", "XAUUSD", "XAUUSD", 71.0, 0.572, 0.58, "neural_block", "below_neural_min_prob", "shadow_counterfactual", 1, 40.0, json.dumps({"raw_scores": {"scalp_profile_session": "new_york"}}), 1),
                (now, "scalp_xauusd", "XAUUSD", "XAUUSD", 73.0, 0.575, 0.58, "neural_block", "below_neural_min_prob", "shadow_counterfactual", 1, 32.0, json.dumps({"raw_scores": {"scalp_profile_session": "new_york"}}), 1),
                (now, "scalp_xauusd", "XAUUSD", "XAUUSD", 72.0, 0.576, 0.58, "neural_block", "below_neural_min_prob", "shadow_counterfactual", 1, 35.0, json.dumps({"raw_scores": {"scalp_profile_session": "overlap"}}), 1),
                (now, "scalp_xauusd", "XAUUSD", "XAUUSD", 69.0, 0.574, 0.58, "neural_block", "below_neural_min_prob", "shadow_counterfactual", 0, -10.0, json.dumps({"raw_scores": {"scalp_profile_session": "overlap"}}), 1),
            ]
            conn.executemany(
                """
                INSERT INTO neural_gate_decisions(
                    created_at, source, signal_symbol, broker_symbol, confidence, neural_prob, min_prob,
                    decision, decision_reason, outcome_type, outcome, pnl_usd, features_json, resolved
                ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                rows,
            )

        with patch("learning.live_profile_autopilot.config.AUTO_APPLY_LIVE_PROFILE_MIN_SAMPLE", 4), \
             patch("learning.live_profile_autopilot.config.AUTO_APPLY_LIVE_PROFILE_MIN_WIN_RATE", 0.60), \
             patch("learning.live_profile_autopilot.config.NEURAL_GATE_CANARY_MIN_CONFIDENCE", 72.0), \
             patch("learning.live_profile_autopilot.config.NEURAL_GATE_CANARY_FIXED_ALLOW_LOW", 0.54):
            report = self.engine.build_missed_opportunity_audit_report(days=14)

        self.assertTrue(report["ok"])
        rec = next(x for x in report["recommendations"] if x["symbol"] == "XAUUSD")
        self.assertEqual(rec["action"], "canary_reinstate")
        self.assertEqual(float(rec["proposed_canary_min_confidence"]), 70.0)

    def test_auto_apply_live_profile_persists_changes_and_creates_active_bundle(self):
        winner_report = {
            "recommendations": [
                {"symbol": "BTCUSD", "recommended_live_mode": "winner_only"},
                {"symbol": "ETHUSD", "recommended_live_mode": "winner_only"},
                {"symbol": "XAUUSD", "recommended_live_mode": "scheduled_winner_only"},
            ],
            "symbols": [
                {"symbol": "BTCUSD", "model": {"resolved": 12}},
                {"symbol": "ETHUSD", "model": {"resolved": 10}},
                {"symbol": "XAUUSD", "model": {"resolved": 50}},
            ],
        }
        crypto_report = {
            "recommendations": [
                {"symbol": "BTCUSD", "profile_source": "weekend", "weekend_resolved": 8, "recommended_min_confidence": 74.0, "recommended_sessions": ["new_york", "london,new_york,overlap"]},
                {"symbol": "ETHUSD", "profile_source": "weekend", "weekend_resolved": 8, "recommended_min_confidence": 77.0, "recommended_sessions": ["london,new_york,overlap"]},
            ]
        }
        audit_report = {
            "recommendations": [
                {"symbol": "XAUUSD", "source": "scalp_xauusd", "action": "canary_reinstate", "resolved": 8, "proposed_canary_min_confidence": 70.0}
            ]
        }
        with patch("learning.live_profile_autopilot.config.AUTO_APPLY_LIVE_PROFILE_ENABLED", True), \
             patch("learning.live_profile_autopilot.config.AUTO_APPLY_LIVE_PROFILE_PERSIST_ENV", True), \
             patch("learning.live_profile_autopilot.config.AUTO_APPLY_LIVE_PROFILE_MIN_SAMPLE", 6), \
             patch("learning.live_profile_autopilot.config.CTRADER_ALLOWED_SOURCES", "scalp_btcusd:winner,scalp_ethusd:winner,scalp_xauusd,xauusd_scheduled:winner"), \
             patch("learning.live_profile_autopilot.config.CTRADER_STORE_FEED_SOURCES", "scalp_btcusd:winner,scalp_ethusd:winner,scalp_xauusd,xauusd_scheduled:winner"), \
             patch("learning.live_profile_autopilot.config.SCALPING_BTC_MIN_CONFIDENCE_WEEKEND", 75.0), \
             patch("learning.live_profile_autopilot.config.SCALPING_BTC_ALLOWED_SESSIONS_WEEKEND", "new_york"), \
             patch("learning.live_profile_autopilot.config.SCALPING_ETH_MIN_CONFIDENCE_WEEKEND", 76.0), \
             patch("learning.live_profile_autopilot.config.SCALPING_ETH_ALLOWED_SESSIONS_WEEKEND", "london,new_york,overlap"), \
             patch("learning.live_profile_autopilot.config.NEURAL_GATE_CANARY_MIN_CONFIDENCE", 72.0):
            out = self.engine.auto_apply_live_profile(
                winner_report=winner_report,
                crypto_report=crypto_report,
                audit_report=audit_report,
            )

        self.assertTrue(out["ok"])
        self.assertEqual(out["status"], "applied")
        self.assertIn("versions", out)
        self.assertIn("execution_scope", out)
        env_text = self.env_local.read_text(encoding="utf-8")
        self.assertIn("NEURAL_GATE_CANARY_MIN_CONFIDENCE=70", env_text)
        self.assertIn("SCALPING_BTC_MIN_CONFIDENCE_WEEKEND=74", env_text)

    def test_auto_apply_live_profile_preserves_xau_direct_sources_when_ctrader_canary_is_enabled(self):
        winner_report = {
            "recommendations": [
                {"symbol": "XAUUSD", "recommended_live_mode": "scheduled_winner_only"},
            ],
            "symbols": [
                {"symbol": "XAUUSD", "model": {"resolved": 50}},
            ],
        }
        with patch("learning.live_profile_autopilot.config.AUTO_APPLY_LIVE_PROFILE_ENABLED", True), \
             patch("learning.live_profile_autopilot.config.AUTO_APPLY_LIVE_PROFILE_PERSIST_ENV", True), \
             patch("learning.live_profile_autopilot.config.AUTO_APPLY_LIVE_PROFILE_MIN_SAMPLE", 6), \
             patch("learning.live_profile_autopilot.config.CTRADER_ALLOWED_SOURCES", "scalp_btcusd:winner,scalp_ethusd:winner,scalp_xauusd:winner,xauusd_scheduled:winner"), \
             patch("learning.live_profile_autopilot.config.CTRADER_STORE_FEED_SOURCES", "scalp_btcusd:winner,scalp_ethusd:winner,scalp_xauusd:winner,xauusd_scheduled:winner"), \
             patch("learning.live_profile_autopilot.config.PERSISTENT_CANARY_CTRADER_ENABLED", True), \
             patch("learning.live_profile_autopilot.config.get_persistent_canary_direct_allowed_sources", return_value={"scalp_xauusd", "xauusd_scheduled"}):
            out = self.engine.auto_apply_live_profile(
                winner_report=winner_report,
                crypto_report={"recommendations": []},
                audit_report={"recommendations": []},
            )

        self.assertTrue(out["ok"])
        env_text = self.env_local.read_text(encoding="utf-8")
        self.assertIn("CTRADER_ALLOWED_SOURCES=scalp_xauusd,scalp_xauusd:winner,xauusd_scheduled,xauusd_scheduled:winner", env_text)
        self.assertIn("CTRADER_STORE_FEED_SOURCES=scalp_xauusd,scalp_xauusd:winner,xauusd_scheduled,xauusd_scheduled:winner", env_text)

    def test_evaluate_xau_tick_depth_filter_passes_supportive_long_features(self):
        features = {
            "spots_count": 12,
            "depth_count": 80,
            "spread_avg_pct": 0.0016,
            "spread_expansion": 1.04,
            "depth_imbalance": -0.035,
            "depth_refill_shift": -0.08,
            "delta_proxy": -0.06,
            "bar_volume_proxy": 0.62,
            "rejection_ratio": 0.12,
        }
        with patch("learning.live_profile_autopilot.config.XAU_TICK_DEPTH_FILTER_ENABLED", True), \
             patch("learning.live_profile_autopilot.config.XAU_TICK_DEPTH_FILTER_MIN_SPOTS", 6), \
             patch("learning.live_profile_autopilot.config.XAU_TICK_DEPTH_FILTER_MIN_DEPTH_QUOTES", 40), \
             patch("learning.live_profile_autopilot.config.XAU_TICK_DEPTH_FILTER_MAX_SPREAD_PCT", 0.0022), \
             patch("learning.live_profile_autopilot.config.XAU_TICK_DEPTH_FILTER_MAX_SPREAD_EXPANSION", 1.12), \
             patch("learning.live_profile_autopilot.config.XAU_TICK_DEPTH_FILTER_LONG_MAX_IMBALANCE", -0.01), \
             patch("learning.live_profile_autopilot.config.XAU_TICK_DEPTH_FILTER_LONG_MAX_REFILL_SHIFT", -0.03), \
             patch("learning.live_profile_autopilot.config.XAU_TICK_DEPTH_FILTER_LONG_MAX_DELTA_PROXY", -0.01), \
             patch("learning.live_profile_autopilot.config.XAU_TICK_DEPTH_FILTER_MIN_BAR_VOLUME_PROXY", 0.35), \
             patch("learning.live_profile_autopilot.config.XAU_TICK_DEPTH_FILTER_MIN_GATE_SCORE", 3):
            gate = evaluate_xau_tick_depth_filter(features, "long", confidence=73.0)

        self.assertTrue(gate["pass"])
        self.assertGreaterEqual(int(gate["score"]), 3)
        self.assertEqual(list(gate.get("reasons") or []), [])

    def test_evaluate_xau_tick_depth_filter_allows_canary_sample_near_pass(self):
        features = {
            "spots_count": 10,
            "depth_count": 70,
            "spread_avg_pct": 0.0017,
            "spread_expansion": 1.03,
            "depth_imbalance": -0.018,
            "depth_refill_shift": -0.05,
            "delta_proxy": -0.03,
            "bar_volume_proxy": 0.58,
            "rejection_ratio": 0.11,
        }
        with patch("learning.live_profile_autopilot.config.XAU_TICK_DEPTH_FILTER_ENABLED", True), \
             patch("learning.live_profile_autopilot.config.XAU_TICK_DEPTH_FILTER_MIN_SPOTS", 6), \
             patch("learning.live_profile_autopilot.config.XAU_TICK_DEPTH_FILTER_MIN_DEPTH_QUOTES", 40), \
             patch("learning.live_profile_autopilot.config.XAU_TICK_DEPTH_FILTER_MAX_SPREAD_PCT", 0.0022), \
             patch("learning.live_profile_autopilot.config.XAU_TICK_DEPTH_FILTER_MAX_SPREAD_EXPANSION", 1.08), \
             patch("learning.live_profile_autopilot.config.XAU_TICK_DEPTH_FILTER_LONG_MAX_IMBALANCE", -0.02), \
             patch("learning.live_profile_autopilot.config.XAU_TICK_DEPTH_FILTER_LONG_MAX_REFILL_SHIFT", -0.08), \
             patch("learning.live_profile_autopilot.config.XAU_TICK_DEPTH_FILTER_LONG_MAX_DELTA_PROXY", -0.01), \
             patch("learning.live_profile_autopilot.config.XAU_TICK_DEPTH_FILTER_MIN_BAR_VOLUME_PROXY", 0.35), \
             patch("learning.live_profile_autopilot.config.XAU_TICK_DEPTH_FILTER_MIN_GATE_SCORE", 8), \
             patch("learning.live_profile_autopilot.config.XAU_TICK_DEPTH_FILTER_CANARY_SAMPLE_ENABLED", True), \
             patch("learning.live_profile_autopilot.config.XAU_TICK_DEPTH_FILTER_CANARY_SAMPLE_SCORE_DELTA", 1), \
             patch("learning.live_profile_autopilot.config.XAU_TICK_DEPTH_FILTER_CANARY_SAMPLE_MIN_CONFIDENCE", 73.0), \
             patch("learning.live_profile_autopilot.config.XAU_TICK_DEPTH_FILTER_CANARY_SAMPLE_MIN_BAR_VOLUME_PROXY", 0.45), \
             patch("learning.live_profile_autopilot.config.XAU_TICK_DEPTH_FILTER_CANARY_SAMPLE_MAX_SPREAD_EXPANSION", 1.05), \
             patch("learning.live_profile_autopilot.config.XAU_TICK_DEPTH_FILTER_CANARY_SAMPLE_RISK_MULTIPLIER", 0.7):
            gate = evaluate_xau_tick_depth_filter(features, "long", confidence=73.4)

        self.assertFalse(gate["pass"])
        self.assertTrue(gate["canary_sample_pass"])
        self.assertEqual(str(gate.get("sample_mode") or ""), "near_pass")
        self.assertAlmostEqual(float(gate.get("sample_risk_multiplier", 0.0) or 0.0), 0.7, places=4)

    def test_classify_xau_day_type_detects_panic_spread(self):
        out = classify_xau_day_type(
            {
                "spread_avg_pct": 0.0022,
                "spread_expansion": 1.21,
                "mid_drift_pct": 0.010,
                "delta_proxy": 0.18,
                "rejection_ratio": 0.06,
                "bar_volume_proxy": 0.72,
            }
        )
        self.assertEqual(str(out.get("day_type") or ""), "panic_spread")
        self.assertIn("one_way_liquidity_stress", list(out.get("reasons") or []))

    def test_classify_xau_day_type_detects_fast_expansion(self):
        out = classify_xau_day_type(
            {
                "spread_avg_pct": 0.0015,
                "spread_expansion": 1.28,
                "mid_drift_pct": 0.0035,
                "delta_proxy": 0.17,
                "rejection_ratio": 0.42,
                "bar_volume_proxy": 0.81,
            }
        )
        self.assertEqual(str(out.get("day_type") or ""), "fast_expansion")
        self.assertIn("tradable_liquidity_expansion", list(out.get("reasons") or []))

    def test_evaluate_xau_tick_depth_filter_relaxes_sample_gate_on_repricing_day(self):
        features = {
            "spots_count": 10,
            "depth_count": 70,
            "spread_avg_pct": 0.0017,
            "spread_expansion": 1.04,
            "depth_imbalance": 0.002,
            "depth_refill_shift": 0.01,
            "delta_proxy": 0.11,
            "mid_drift_pct": 0.014,
            "bar_volume_proxy": 0.46,
            "rejection_ratio": 0.16,
        }
        with ExitStack() as stack:
            stack.enter_context(patch("learning.live_profile_autopilot.config.XAU_TICK_DEPTH_FILTER_ENABLED", True))
            stack.enter_context(patch("learning.live_profile_autopilot.config.XAU_TICK_DEPTH_FILTER_MIN_SPOTS", 6))
            stack.enter_context(patch("learning.live_profile_autopilot.config.XAU_TICK_DEPTH_FILTER_MIN_DEPTH_QUOTES", 40))
            stack.enter_context(patch("learning.live_profile_autopilot.config.XAU_TICK_DEPTH_FILTER_MAX_SPREAD_PCT", 0.0022))
            stack.enter_context(patch("learning.live_profile_autopilot.config.XAU_TICK_DEPTH_FILTER_MAX_SPREAD_EXPANSION", 1.08))
            stack.enter_context(patch("learning.live_profile_autopilot.config.XAU_TICK_DEPTH_FILTER_SHORT_MIN_IMBALANCE", 0.005))
            stack.enter_context(patch("learning.live_profile_autopilot.config.XAU_TICK_DEPTH_FILTER_SHORT_MAX_REJECTION", 0.18))
            stack.enter_context(patch("learning.live_profile_autopilot.config.XAU_TICK_DEPTH_FILTER_SHORT_MIN_DELTA_PROXY", 0.12))
            stack.enter_context(patch("learning.live_profile_autopilot.config.XAU_TICK_DEPTH_FILTER_MIN_BAR_VOLUME_PROXY", 0.35))
            stack.enter_context(patch("learning.live_profile_autopilot.config.XAU_TICK_DEPTH_FILTER_MIN_GATE_SCORE", 8))
            stack.enter_context(patch("learning.live_profile_autopilot.config.XAU_TICK_DEPTH_DAY_TYPE_REPRICING_MIN_ABS_DRIFT_PCT", 0.010))
            stack.enter_context(patch("learning.live_profile_autopilot.config.XAU_TICK_DEPTH_DAY_TYPE_REPRICING_MIN_ABS_DELTA_PROXY", 0.08))
            stack.enter_context(patch("learning.live_profile_autopilot.config.XAU_TICK_DEPTH_DAY_TYPE_REPRICING_MIN_BAR_VOLUME_PROXY", 0.40))
            stack.enter_context(patch("learning.live_profile_autopilot.config.XAU_TICK_DEPTH_DAY_TYPE_REPRICING_MIN_REJECTION", 0.10))
            stack.enter_context(patch("learning.live_profile_autopilot.config.XAU_TICK_DEPTH_FILTER_CANARY_SAMPLE_ENABLED", True))
            stack.enter_context(patch("learning.live_profile_autopilot.config.XAU_TICK_DEPTH_FILTER_CANARY_SAMPLE_SCORE_DELTA", 1))
            stack.enter_context(patch("learning.live_profile_autopilot.config.XAU_TICK_DEPTH_FILTER_CANARY_SAMPLE_MIN_CONFIDENCE", 74.0))
            stack.enter_context(patch("learning.live_profile_autopilot.config.XAU_TICK_DEPTH_FILTER_CANARY_SAMPLE_MIN_BAR_VOLUME_PROXY", 0.45))
            stack.enter_context(patch("learning.live_profile_autopilot.config.XAU_TICK_DEPTH_FILTER_CANARY_SAMPLE_MAX_SPREAD_EXPANSION", 1.05))
            stack.enter_context(patch("learning.live_profile_autopilot.config.XAU_TICK_DEPTH_FILTER_CANARY_SAMPLE_RISK_MULTIPLIER", 0.7))
            stack.enter_context(patch("learning.live_profile_autopilot.config.XAU_TICK_DEPTH_FILTER_REPRICING_SAMPLE_SCORE_BONUS", 2))
            stack.enter_context(patch("learning.live_profile_autopilot.config.XAU_TICK_DEPTH_FILTER_REPRICING_SAMPLE_MIN_CONFIDENCE_DELTA", -1.0))
            stack.enter_context(patch("learning.live_profile_autopilot.config.XAU_TICK_DEPTH_FILTER_REPRICING_SAMPLE_MAX_SPREAD_EXPANSION_MULT", 1.0))
            stack.enter_context(patch("learning.live_profile_autopilot.config.XAU_TICK_DEPTH_FILTER_REPRICING_SAMPLE_MIN_BAR_VOLUME_MULT", 1.0))
            gate = evaluate_xau_tick_depth_filter(features, "short", confidence=73.2)

        self.assertEqual(str(gate.get("day_type") or ""), "repricing")
        self.assertFalse(gate["pass"])
        self.assertTrue(gate["canary_sample_pass"])
        self.assertEqual(str(gate.get("sample_mode") or ""), "repricing_near_pass")

    def test_evaluate_xau_tick_depth_filter_blocks_on_panic_spread_day(self):
        features = {
            "spots_count": 14,
            "depth_count": 90,
            "spread_avg_pct": 0.0028,
            "spread_expansion": 1.20,
            "depth_imbalance": -0.04,
            "depth_refill_shift": -0.09,
            "delta_proxy": -0.12,
            "mid_drift_pct": -0.015,
            "bar_volume_proxy": 0.76,
            "rejection_ratio": 0.09,
        }
        with patch("learning.live_profile_autopilot.config.XAU_TICK_DEPTH_FILTER_ENABLED", True), \
             patch("learning.live_profile_autopilot.config.XAU_TICK_DEPTH_FILTER_MIN_GATE_SCORE", 3), \
             patch("learning.live_profile_autopilot.config.XAU_TICK_DEPTH_FILTER_PANIC_SPREAD_BLOCK", True):
            gate = evaluate_xau_tick_depth_filter(features, "long", confidence=76.0)

        self.assertEqual(str(gate.get("day_type") or ""), "panic_spread")
        self.assertFalse(gate["pass"])
        self.assertFalse(gate["canary_sample_pass"])
        self.assertIn("panic_spread_day_block", list(gate.get("reasons") or []))

    def test_evaluate_xau_tick_depth_filter_allows_fast_expansion_near_pass(self):
        features = {
            "spots_count": 12,
            "depth_count": 82,
            "spread_avg_pct": 0.0016,
            "spread_expansion": 1.30,
            "depth_imbalance": 0.002,
            "depth_refill_shift": 0.01,
            "delta_proxy": 0.13,
            "mid_drift_pct": 0.0036,
            "bar_volume_proxy": 0.72,
            "rejection_ratio": 0.38,
        }
        with ExitStack() as stack:
            stack.enter_context(patch("learning.live_profile_autopilot.config.XAU_TICK_DEPTH_FILTER_ENABLED", True))
            stack.enter_context(patch("learning.live_profile_autopilot.config.XAU_TICK_DEPTH_FILTER_MIN_SPOTS", 6))
            stack.enter_context(patch("learning.live_profile_autopilot.config.XAU_TICK_DEPTH_FILTER_MIN_DEPTH_QUOTES", 40))
            stack.enter_context(patch("learning.live_profile_autopilot.config.XAU_TICK_DEPTH_FILTER_MAX_SPREAD_PCT", 0.0022))
            stack.enter_context(patch("learning.live_profile_autopilot.config.XAU_TICK_DEPTH_FILTER_MAX_SPREAD_EXPANSION", 1.08))
            stack.enter_context(patch("learning.live_profile_autopilot.config.XAU_TICK_DEPTH_FILTER_SHORT_MIN_IMBALANCE", 0.005))
            stack.enter_context(patch("learning.live_profile_autopilot.config.XAU_TICK_DEPTH_FILTER_SHORT_MAX_REJECTION", 0.20))
            stack.enter_context(patch("learning.live_profile_autopilot.config.XAU_TICK_DEPTH_FILTER_SHORT_MIN_DELTA_PROXY", 0.14))
            stack.enter_context(patch("learning.live_profile_autopilot.config.XAU_TICK_DEPTH_FILTER_MIN_BAR_VOLUME_PROXY", 0.35))
            stack.enter_context(patch("learning.live_profile_autopilot.config.XAU_TICK_DEPTH_FILTER_MIN_GATE_SCORE", 8))
            stack.enter_context(patch("learning.live_profile_autopilot.config.XAU_TICK_DEPTH_DAY_TYPE_FAST_EXPANSION_MIN_SPREAD_EXPANSION", 1.12))
            stack.enter_context(patch("learning.live_profile_autopilot.config.XAU_TICK_DEPTH_DAY_TYPE_FAST_EXPANSION_MAX_SPREAD_PCT", 0.0022))
            stack.enter_context(patch("learning.live_profile_autopilot.config.XAU_TICK_DEPTH_DAY_TYPE_FAST_EXPANSION_MIN_ABS_DRIFT_PCT", 0.0025))
            stack.enter_context(patch("learning.live_profile_autopilot.config.XAU_TICK_DEPTH_DAY_TYPE_FAST_EXPANSION_MIN_ABS_DELTA_PROXY", 0.10))
            stack.enter_context(patch("learning.live_profile_autopilot.config.XAU_TICK_DEPTH_DAY_TYPE_FAST_EXPANSION_MIN_BAR_VOLUME_PROXY", 0.45))
            stack.enter_context(patch("learning.live_profile_autopilot.config.XAU_TICK_DEPTH_DAY_TYPE_FAST_EXPANSION_MIN_REJECTION", 0.20))
            stack.enter_context(patch("learning.live_profile_autopilot.config.XAU_TICK_DEPTH_FILTER_CANARY_SAMPLE_ENABLED", True))
            stack.enter_context(patch("learning.live_profile_autopilot.config.XAU_TICK_DEPTH_FILTER_CANARY_SAMPLE_SCORE_DELTA", 1))
            stack.enter_context(patch("learning.live_profile_autopilot.config.XAU_TICK_DEPTH_FILTER_CANARY_SAMPLE_MIN_CONFIDENCE", 73.5))
            stack.enter_context(patch("learning.live_profile_autopilot.config.XAU_TICK_DEPTH_FILTER_CANARY_SAMPLE_MIN_BAR_VOLUME_PROXY", 0.45))
            stack.enter_context(patch("learning.live_profile_autopilot.config.XAU_TICK_DEPTH_FILTER_CANARY_SAMPLE_MAX_SPREAD_EXPANSION", 1.05))
            stack.enter_context(patch("learning.live_profile_autopilot.config.XAU_TICK_DEPTH_FILTER_FAST_EXPANSION_SAMPLE_SCORE_BONUS", 2))
            stack.enter_context(patch("learning.live_profile_autopilot.config.XAU_TICK_DEPTH_FILTER_FAST_EXPANSION_SAMPLE_MIN_CONFIDENCE_DELTA", -0.5))
            stack.enter_context(patch("learning.live_profile_autopilot.config.XAU_TICK_DEPTH_FILTER_FAST_EXPANSION_SAMPLE_MAX_SPREAD_EXPANSION_MULT", 1.35))
            stack.enter_context(patch("learning.live_profile_autopilot.config.XAU_TICK_DEPTH_FILTER_FAST_EXPANSION_SAMPLE_MIN_BAR_VOLUME_MULT", 1.0))
            gate = evaluate_xau_tick_depth_filter(features, "short", confidence=73.2)

        self.assertEqual(str(gate.get("day_type") or ""), "fast_expansion")
        self.assertFalse(gate["pass"])
        self.assertTrue(gate["canary_sample_pass"])
        self.assertEqual(str(gate.get("sample_mode") or ""), "fast_expansion_near_pass")

    def test_latest_capture_feature_snapshot_filters_rows_by_symbol_within_run(self):
        run_id = "ctcap_test_mixed"
        with sqlite3.connect(str(self.ctrader_db)) as conn:
            conn.executemany(
                """
                INSERT INTO ctrader_spot_ticks(run_id, symbol, event_utc, event_ts, bid, ask, spread, spread_pct, payload_json)
                VALUES(?, ?, ?, ?, ?, ?, ?, ?, '{}')
                """,
                [
                    (run_id, "XAUUSD", "2026-03-12T05:20:23Z", 1, 5146.35, 5146.40, 0.05, 0.00097),
                    (run_id, "XAUUSD", "2026-03-12T05:20:24Z", 2, 5146.30, 5146.42, 0.12, 0.00233),
                    (run_id, "ETHUSD", "2026-03-12T05:20:24Z", 3, 2022.03, 2024.93, 2.90, 0.14331),
                    (run_id, "BTCUSD", "2026-03-12T05:20:24Z", 4, 69363.99, 69375.99, 12.0, 0.01729),
                ],
            )
            conn.executemany(
                """
                INSERT INTO ctrader_depth_quotes(run_id, symbol, event_utc, event_ts, side, price, size, level_index, payload_json)
                VALUES(?, ?, ?, ?, ?, ?, ?, ?, '{}')
                """,
                [
                    (run_id, "XAUUSD", "2026-03-12T05:20:24Z", 1, "bid", 5146.35, 1.2, 0),
                    (run_id, "XAUUSD", "2026-03-12T05:20:24Z", 2, "ask", 5146.40, 1.0, 0),
                    (run_id, "ETHUSD", "2026-03-12T05:20:24Z", 3, "bid", 2022.03, 5.0, 0),
                    (run_id, "ETHUSD", "2026-03-12T05:20:24Z", 4, "ask", 2024.93, 5.0, 0),
                ],
            )

        with patch("learning.live_profile_autopilot._utc_now", return_value=datetime(2026, 3, 12, 5, 20, 30, tzinfo=timezone.utc)):
            snap = self.engine.latest_capture_feature_snapshot(symbol="XAUUSD", lookback_sec=240, direction="long", confidence=73.0)

        self.assertTrue(snap["ok"])
        self.assertEqual(snap["symbol"], "XAUUSD")
        self.assertEqual(snap["run_id"], run_id)
        self.assertEqual(int((snap.get("features") or {}).get("spots_count", 0) or 0), 2)
        self.assertLess(float((snap.get("features") or {}).get("spread_avg_pct", 1.0) or 1.0), 0.01)

    def test_build_xau_tick_depth_filter_report_filters_capture_rows_by_symbol_within_run(self):
        run_id = "ctcap_test_report_mixed"
        now_iso = "2026-03-12T05:30:00Z"
        with sqlite3.connect(str(self.ctrader_db)) as conn:
            conn.execute(
                """
                INSERT INTO execution_journal(
                    created_utc, source, symbol, direction, entry_type, confidence, entry, stop_loss, take_profit,
                    request_json, response_json, execution_meta_json, status
                ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, '{}', '{}', ?, 'closed')
                """,
                (
                    now_iso,
                    "scalp_xauusd:pb:canary",
                    "XAUUSD",
                    "long",
                    "limit",
                    73.0,
                    5146.30,
                    5145.80,
                    5147.20,
                    json.dumps({"market_capture": {"run_id": run_id}}),
                ),
            )
            journal_id = int(conn.execute("SELECT last_insert_rowid()").fetchone()[0])
            conn.execute(
                """
                INSERT INTO ctrader_deals(
                    deal_id, source, symbol, pnl_usd, outcome, has_close_detail, journal_id, execution_utc, raw_json
                ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (1, "scalp_xauusd:pb:canary", "XAUUSD", 2.5, 1, 1, journal_id, now_iso, json.dumps({"entryPrice": 5146.30})),
            )
            spot_rows = []
            for idx in range(6):
                bid = 5146.30 + (idx * 0.01)
                ask = bid + 0.05
                spot_rows.append((run_id, "XAUUSD", now_iso, idx + 1, bid, ask, 0.05, 0.00097))
            spot_rows.extend(
                [
                    (run_id, "ETHUSD", now_iso, 100, 2022.03, 2024.93, 2.90, 0.14331),
                    (run_id, "BTCUSD", now_iso, 101, 69363.99, 69375.99, 12.0, 0.01729),
                ]
            )
            conn.executemany(
                """
                INSERT INTO ctrader_spot_ticks(run_id, symbol, event_utc, event_ts, bid, ask, spread, spread_pct, payload_json)
                VALUES(?, ?, ?, ?, ?, ?, ?, ?, '{}')
                """,
                spot_rows,
            )
            depth_rows = []
            for idx in range(25):
                depth_rows.append((run_id, "XAUUSD", now_iso, idx * 2 + 1, "bid", 5146.25 - (idx * 0.01), 2.0, idx))
                depth_rows.append((run_id, "XAUUSD", now_iso, idx * 2 + 2, "ask", 5146.35 + (idx * 0.01), 1.0, idx))
            depth_rows.extend(
                [
                    (run_id, "ETHUSD", now_iso, 1000, "bid", 2022.03, 5.0, 0),
                    (run_id, "ETHUSD", now_iso, 1001, "ask", 2024.93, 5.0, 0),
                ]
            )
            conn.executemany(
                """
                INSERT INTO ctrader_depth_quotes(run_id, symbol, event_utc, event_ts, side, price, size, level_index, payload_json)
                VALUES(?, ?, ?, ?, ?, ?, ?, ?, '{}')
                """,
                depth_rows,
            )

        with ExitStack() as stack:
            stack.enter_context(patch("learning.live_profile_autopilot._utc_now", return_value=datetime(2026, 3, 12, 6, 0, 0, tzinfo=timezone.utc)))
            stack.enter_context(patch("learning.live_profile_autopilot.config.XAU_TICK_DEPTH_FILTER_MIN_SPOTS", 4))
            stack.enter_context(patch("learning.live_profile_autopilot.config.XAU_TICK_DEPTH_FILTER_MIN_DEPTH_QUOTES", 20))
            report = self.engine.build_xau_tick_depth_filter_report(days=1)

        self.assertTrue(report["ok"])
        family = next(row for row in report.get("families") or [] if row.get("family") == "xau_scalp_pullback_limit")
        self.assertEqual(int(((family.get("baseline") or {}).get("resolved", 0) or 0)), 1)
        self.assertLess(float(family.get("avg_filtered_spread_pct", 1.0) or 1.0), 0.01)

    def test_build_external_model_prior_library_report_includes_tick_depth_family(self):
        report = self.engine.build_external_model_prior_library_report()

        self.assertTrue(report["ok"])
        families = list(report.get("families") or [])
        td = next(row for row in families if row.get("family") == "xau_scalp_tick_depth_filter")
        self.assertEqual(td["symbol"], "XAUUSD")
        self.assertIn("DeepLOB", list(td.get("prior_models") or []))
        self.assertGreater(float(td.get("router_bonus", 0.0)), 0.0)

    def test_auto_apply_live_profile_rolls_back_after_bad_live_performance(self):
        state = {
            "version": 1,
            "active_bundle": {
                "id": "test",
                "status": "active",
                "applied_at": "2026-03-09T00:00:00Z",
                "changes": {"NEURAL_GATE_CANARY_MIN_CONFIDENCE": {"old": "72", "new": "70"}},
                "affected_symbols": ["XAUUSD"],
                "affected_sources": ["scalp_xauusd"],
                "reasons": ["test"],
            },
            "history": [],
        }
        self.engine.state_path.write_text(json.dumps(state), encoding="utf-8")
        with sqlite3.connect(str(self.mt5_db)) as conn:
            conn.executemany(
                """
                INSERT INTO mt5_execution_journal(created_at, source, signal_symbol, broker_symbol, resolved, outcome, pnl)
                VALUES(?,?,?,?,?,?,?)
                """,
                [
                    ("2026-03-09T00:10:00Z", "scalp_xauusd", "XAUUSD", "XAUUSD", 1, 0, -12.0),
                    ("2026-03-09T00:20:00Z", "scalp_xauusd", "XAUUSD", "XAUUSD", 1, 0, -9.0),
                    ("2026-03-09T00:30:00Z", "scalp_xauusd", "XAUUSD", "XAUUSD", 1, 0, -7.0),
                    ("2026-03-09T00:40:00Z", "scalp_xauusd", "XAUUSD", "XAUUSD", 1, 0, -6.0),
                ],
            )

        with patch("learning.live_profile_autopilot.config.AUTO_APPLY_LIVE_PROFILE_ENABLED", True), \
             patch("learning.live_profile_autopilot.config.AUTO_APPLY_LIVE_PROFILE_PERSIST_ENV", True), \
             patch("learning.live_profile_autopilot.config.AUTO_APPLY_LIVE_PROFILE_ROLLBACK_MIN_RESOLVED", 4), \
             patch("learning.live_profile_autopilot.config.AUTO_APPLY_LIVE_PROFILE_ROLLBACK_MAX_NET_LOSS_USD", -20.0), \
             patch("learning.live_profile_autopilot.config.AUTO_APPLY_LIVE_PROFILE_ROLLBACK_MIN_WIN_RATE", 0.40):
            out = self.engine.auto_apply_live_profile(winner_report={"recommendations": [], "symbols": []}, crypto_report={"recommendations": []}, audit_report={"recommendations": []})

        self.assertTrue(out["ok"])
        self.assertEqual(str((out.get("rollback") or {}).get("status")), "rolled_back")
        env_text = self.env_local.read_text(encoding="utf-8")
        self.assertIn("NEURAL_GATE_CANARY_MIN_CONFIDENCE=72", env_text)

    def test_build_canary_post_trade_audit_report_rolls_up_main_vs_canary(self):
        with sqlite3.connect(str(self.mt5_db)) as conn:
            conn.executemany(
                """
                INSERT INTO mt5_execution_journal(created_at, source, signal_symbol, broker_symbol, resolved, outcome, pnl)
                VALUES(?,?,?,?,?,?,?)
                """,
                [
                    ("2026-03-09T01:00:00Z", "scalp_xauusd:canary", "XAUUSD", "XAUUSD", 1, 0, -8.0),
                    ("2026-03-09T01:10:00Z", "scalp_xauusd", "XAUUSD", "XAUUSD", 1, 1, 6.0),
                ],
            )
        with sqlite3.connect(str(self.ctrader_db)) as conn:
            conn.executemany(
                """
                INSERT INTO ctrader_deals(deal_id, source, symbol, pnl_usd, outcome, has_close_detail, journal_id, execution_utc)
                VALUES(?,?,?,?,?,?,?,?)
                """,
                [
                    (1, "scalp_xauusd:canary", "XAUUSD", 12.0, 1, 1, 101, "2026-03-09T02:00:00Z"),
                    (2, "scalp_xauusd:canary", "XAUUSD", -4.0, 0, 1, 102, "2026-03-09T02:05:00Z"),
                    (3, "scalp_xauusd:canary", "XAUUSD", 9.0, 1, 1, 103, "2026-03-09T02:10:00Z"),
                    (4, "scalp_xauusd", "XAUUSD", -3.0, 0, 1, 104, "2026-03-09T02:15:00Z"),
                ],
            )

        with patch("learning.live_profile_autopilot.config.get_canary_post_trade_audit_milestones", return_value=[3, 5]), \
             patch("learning.live_profile_autopilot.config.AUTO_APPLY_LIVE_PROFILE_MIN_SAMPLE", 4):
            report = self.engine.build_canary_post_trade_audit_report(days=14)

        self.assertTrue(report["ok"])
        self.assertEqual(int((report.get("summary") or {}).get("total_canary_closed", 0)), 4)
        self.assertEqual(str(report.get("status")), "review_ready")
        self.assertTrue(any(int(x.get("milestone", 0) or 0) == 3 for x in list(report.get("milestone_events") or [])))
        xau = next(x for x in list(report.get("symbols") or []) if x["symbol"] == "XAUUSD")
        self.assertEqual(int(((xau.get("canary_total") or {}).get("resolved", 0) or 0)), 4)
        self.assertEqual(int(((xau.get("control_total") or {}).get("resolved", 0) or 0)), 2)
        self.assertEqual(int(((xau.get("control_cross_backend_total") or {}).get("resolved", 0) or 0)), 2)
        self.assertTrue(any(str(x.get("symbol")) == "XAUUSD" for x in list(report.get("recommendations") or [])))

    def test_auto_apply_live_profile_uses_canary_report_to_tune_symbols(self):
        canary_report = {
            "recommendations": [
                {"symbol": "XAUUSD", "key": "NEURAL_GATE_CANARY_MIN_CONFIDENCE", "proposed": 71.0, "resolved": 12, "action": "tighten_xau_canary"},
                {"symbol": "BTCUSD", "key": "SCALPING_BTC_MIN_CONFIDENCE_WEEKEND", "proposed": 74.0, "resolved": 8, "action": "loosen_btc_weekend_conf"},
                {"symbol": "ETHUSD", "key": "SCALPING_ETH_MIN_CONFIDENCE_WEEKEND", "proposed": 77.0, "resolved": 9, "action": "tighten_eth_weekend_conf"},
            ]
        }
        with patch("learning.live_profile_autopilot.config.AUTO_APPLY_LIVE_PROFILE_ENABLED", True), \
             patch("learning.live_profile_autopilot.config.AUTO_APPLY_LIVE_PROFILE_PERSIST_ENV", True), \
             patch("learning.live_profile_autopilot.config.AUTO_APPLY_LIVE_PROFILE_MIN_SAMPLE", 6), \
             patch("learning.live_profile_autopilot.config.CTRADER_ALLOWED_SOURCES", "scalp_btcusd:winner,scalp_ethusd:winner,xauusd_scheduled:winner"), \
             patch("learning.live_profile_autopilot.config.CTRADER_STORE_FEED_SOURCES", "scalp_btcusd:winner,scalp_ethusd:winner,xauusd_scheduled:winner"), \
             patch("learning.live_profile_autopilot.config.SCALPING_BTC_MIN_CONFIDENCE_WEEKEND", 75.0), \
             patch("learning.live_profile_autopilot.config.SCALPING_ETH_MIN_CONFIDENCE_WEEKEND", 76.0), \
             patch("learning.live_profile_autopilot.config.NEURAL_GATE_CANARY_MIN_CONFIDENCE", 70.0):
            out = self.engine.auto_apply_live_profile(
                winner_report={"recommendations": [], "symbols": []},
                crypto_report={"recommendations": []},
                audit_report={"recommendations": []},
                canary_report=canary_report,
            )

        self.assertTrue(out["ok"])
        self.assertEqual(out["status"], "applied")
        env_text = self.env_local.read_text(encoding="utf-8")
        self.assertIn("NEURAL_GATE_CANARY_MIN_CONFIDENCE=71", env_text)
        self.assertIn("SCALPING_BTC_MIN_CONFIDENCE_WEEKEND=74", env_text)
        self.assertIn("SCALPING_ETH_MIN_CONFIDENCE_WEEKEND=77", env_text)

    def test_build_canary_tuning_recommendations_respects_expanded_xau_ceiling(self):
        row = {
            "symbol": "XAUUSD",
            "canary_total": {"resolved": 10, "win_rate": 0.40, "pnl_usd": -5.0},
            "control_cross_backend_total": {"resolved": 10, "win_rate": 0.50, "pnl_usd": 0.0},
        }
        with patch("learning.live_profile_autopilot.config.AUTO_APPLY_LIVE_PROFILE_MIN_SAMPLE", 6), \
             patch("learning.live_profile_autopilot.config.NEURAL_GATE_CANARY_MIN_CONFIDENCE", 75.0), \
             patch("learning.live_profile_autopilot.config.AUTO_APPLY_XAU_CANARY_CONFIDENCE_MIN", 68.0), \
             patch("learning.live_profile_autopilot.config.AUTO_APPLY_XAU_CANARY_CONFIDENCE_MAX", 80.0):
            recs = self.engine._build_canary_tuning_recommendations([row])

        self.assertEqual(len(recs), 1)
        self.assertEqual(float(recs[0]["proposed"]), 76.0)

    def test_build_strategy_lab_and_mission_progress_reports(self):
        canary_report = {
            "summary": {"total_canary_closed": 12},
            "symbols": [
                {
                    "symbol": "XAUUSD",
                    "canary_total": {"resolved": 12, "win_rate": 0.42, "pnl_usd": -8.0},
                    "control_cross_backend_total": {"resolved": 10, "win_rate": 0.55, "pnl_usd": 3.0},
                    "control_total": {"resolved": 2, "win_rate": 0.5, "pnl_usd": 1.0},
                }
            ],
        }
        winner_report = {
            "symbols": [{"symbol": "XAUUSD", "recommended_live_mode": "scheduled_winner_only"}],
            "recommendations": [],
        }
        crypto_report = {
            "recommendations": [
                {"symbol": "BTCUSD", "recommended_min_confidence": 75.0, "recommended_sessions": ["new_york"]},
                {"symbol": "ETHUSD", "recommended_min_confidence": 76.0, "recommended_sessions": ["overlap"]},
            ]
        }
        strategy_lab = self.engine.build_strategy_lab_report(
            winner_report=winner_report,
            crypto_report=crypto_report,
            canary_report=canary_report,
        )
        progress = self.engine.build_mission_progress_report(
            winner_report=winner_report,
            crypto_report=crypto_report,
            audit_report={"summary": {"missed_positive_groups": 1}},
            canary_report=canary_report,
            auto_apply_report={"status": "waiting_active_canary"},
            strategy_lab_report=strategy_lab,
        )

        self.assertTrue(strategy_lab["ok"])
        self.assertIn("versions", strategy_lab)
        self.assertIn("execution_scope", strategy_lab)
        self.assertGreaterEqual(len(list(strategy_lab.get("candidates") or [])), 2)
        self.assertTrue(progress["ok"])
        self.assertIn("versions", progress)
        self.assertIn("execution_scope", progress)
        self.assertEqual(str((progress.get("summary") or {}).get("active_bundle_status")), "none")
        xau = next(x for x in list(progress.get("symbols") or []) if x["symbol"] == "XAUUSD")
        self.assertIsNotNone(xau.get("wr_gap_to_target"))

    def test_family_calibration_report_marks_xau_pullback_as_promotable_family(self):
        now = _iso_now()
        request_json = json.dumps(
            {
                "payload": {
                    "confidence": 78.0,
                    "session": "new_york",
                    "timeframe": "5m+1m",
                    "pattern": "PULLBACK_LIMIT",
                    "entry_type": "limit",
                }
            }
        )
        with sqlite3.connect(str(self.ctrader_db)) as conn:
            journal_rows = []
            deal_rows = []
            pnl_outcome = [(8.0, 1), (6.0, 1), (5.0, 1), (4.0, 1), (3.0, 1), (2.0, 1), (7.0, 1), (6.0, 1), (5.0, 1), (-2.0, 0), (-3.0, 0), (4.0, 1)]
            for idx, (pnl, outcome) in enumerate(pnl_outcome, start=11):
                journal_rows.append((idx, now, "scalp_xauusd:pb:canary", "XAUUSD", "limit", 78.0, request_json, "{}"))
                deal_rows.append((100 + idx, "scalp_xauusd:pb:canary", "XAUUSD", pnl, outcome, 1, idx, now))
            conn.executemany(
                """
                INSERT INTO execution_journal(id, created_utc, source, symbol, entry_type, confidence, request_json, execution_meta_json)
                VALUES(?,?,?,?,?,?,?,?)
                """,
                journal_rows,
            )
            conn.executemany(
                """
                INSERT INTO ctrader_deals(deal_id, source, symbol, pnl_usd, outcome, has_close_detail, journal_id, execution_utc)
                VALUES(?,?,?,?,?,?,?,?)
                """,
                deal_rows,
            )

        with patch("learning.live_profile_autopilot.config.CTRADER_PROFILE_VERSION", "ct_test"), \
             patch("learning.live_profile_autopilot.config.MISSION_STACK_VERSION", "mission_test"), \
             patch("learning.live_profile_autopilot.config.FAMILY_CALIBRATION_PRIOR_STRENGTH", 4.0):
            report = self.engine.build_family_calibration_report(days=21)

        self.assertTrue(report["ok"])
        row = next(x for x in list(report.get("families") or []) if x["symbol"] == "XAUUSD" and x["family"] == "xau_scalp_pullback_limit")
        self.assertGreater(float((row.get("overall") or {}).get("win_rate", 0.0)), 0.70)
        self.assertTrue(any(str(x.get("action")) == "promote_primary_family" for x in list(report.get("recommendations") or [])))

    def test_family_calibration_report_excludes_abnormal_fill_rows(self):
        now = _iso_now()
        pullback_request = json.dumps(
            {
                "payload": {
                    "direction": "long",
                    "confidence": 73.4,
                    "session": "asian",
                    "timeframe": "5m+1m",
                    "pattern": "SCALP_FLOW_FORCE|XAU_SCALP_PULLBACK_LIMIT",
                    "entry_type": "limit",
                }
            }
        )
        breakout_request = json.dumps(
            {
                "payload": {
                    "direction": "long",
                    "confidence": 72.0,
                    "session": "new_york",
                    "timeframe": "5m+1m",
                    "pattern": "SCALP_FLOW_FORCE|XAU_SCALP_BREAKOUT_STOP",
                    "entry_type": "buy_stop",
                }
            }
        )
        with sqlite3.connect(str(self.ctrader_db)) as conn:
            conn.executemany(
                """
                INSERT INTO execution_journal(
                    id, created_utc, source, symbol, direction, entry_type, confidence,
                    entry, stop_loss, take_profit, request_json, execution_meta_json
                )
                VALUES(?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                [
                    (31, now, "scalp_xauusd:pb:canary", "XAUUSD", "long", "limit", 73.4, 5160.10, 5157.10, 5164.20, pullback_request, "{}"),
                    (32, now, "scalp_xauusd:bs:canary", "XAUUSD", "long", "buy_stop", 72.0, 5114.7765, 5112.1000, 5117.5250, breakout_request, "{}"),
                ],
            )
            conn.executemany(
                """
                INSERT INTO ctrader_deals(
                    deal_id, source, symbol, pnl_usd, outcome, has_close_detail, journal_id, execution_utc, raw_json
                )
                VALUES(?,?,?,?,?,?,?,?,?)
                """,
                [
                    (301, "scalp_xauusd:pb:canary", "XAUUSD", 4.30, 1, 1, 31, now, json.dumps({"raw": {"closePositionDetail": {"entryPrice": 5160.10}}})),
                    (302, "scalp_xauusd:bs:canary", "XAUUSD", 35.98, 1, 1, 32, now, json.dumps({"raw": {"closePositionDetail": {"entryPrice": 5119.17}}})),
                ],
            )

        report = self.engine.build_family_calibration_report(days=21)
        self.assertTrue(report["ok"])
        self.assertEqual(int((report.get("summary") or {}).get("excluded_abnormal_rows", 0)), 1)
        families = {(row["symbol"], row["family"]): row for row in list(report.get("families") or [])}
        self.assertIn(("XAUUSD", "xau_scalp_pullback_limit"), families)
        self.assertNotIn(("XAUUSD", "xau_scalp_breakout_stop"), families)

    def test_ctrader_tick_depth_replay_report_prefers_pullback_family(self):
        now = _iso_now()
        request_json = json.dumps(
            {
                "payload": {
                    "confidence": 78.0,
                    "session": "new_york",
                    "timeframe": "5m+1m",
                    "pattern": "PULLBACK_LIMIT",
                    "entry_type": "limit",
                }
            }
        )
        with sqlite3.connect(str(self.ctrader_db)) as conn:
            conn.execute(
                """
                INSERT INTO execution_journal(id, created_utc, source, symbol, entry_type, confidence, request_json, execution_meta_json)
                VALUES(?,?,?,?,?,?,?,?)
                """,
                (21, now, "scalp_xauusd:pb:canary", "XAUUSD", "limit", 78.0, request_json, "{}"),
            )
            conn.execute(
                """
                INSERT INTO ctrader_deals(deal_id, source, symbol, pnl_usd, outcome, has_close_detail, journal_id, execution_utc)
                VALUES(?,?,?,?,?,?,?,?)
                """,
                (201, "scalp_xauusd:pb:canary", "XAUUSD", 7.0, 1, 1, 21, now),
            )
            conn.executemany(
                """
                INSERT INTO ctrader_spot_ticks(run_id, symbol, event_utc, event_ts, bid, ask, spread, spread_pct, payload_json)
                VALUES(?,?,?,?,?,?,?,?,?)
                """,
                [
                    ("run1", "XAUUSD", now, 1, 3000.0, 3000.3, 0.3, 0.01, "{}"),
                    ("run1", "XAUUSD", now, 2, 3000.4, 3000.7, 0.3, 0.01, "{}"),
                ],
            )
            conn.executemany(
                """
                INSERT INTO ctrader_depth_quotes(run_id, symbol, event_utc, event_ts, side, price, size, level_index, payload_json)
                VALUES(?,?,?,?,?,?,?,?,?)
                """,
                [
                    ("run1", "XAUUSD", now, 1, "bid", 3000.0, 8.0, 0, "{}"),
                    ("run1", "XAUUSD", now, 1, "ask", 3000.3, 4.0, 0, "{}"),
                ],
            )

        report = self.engine.build_ctrader_tick_depth_replay_report(days=7)
        self.assertTrue(report["ok"])
        row = next(x for x in list(report.get("families") or []) if x["symbol"] == "XAUUSD" and x["family"] == "xau_scalp_pullback_limit")
        self.assertEqual(int(row.get("orders", 0)), 1)
        self.assertGreater(float(row.get("avg_depth_imbalance", 0.0)), 0.0)
        self.assertTrue(any(str(x.get("action")) == "prefer_pullback_low_spread" for x in list(report.get("recommendations") or [])))

    def test_strategy_lab_builds_meta_policy_and_promotable_candidate(self):
        now = _iso_now()
        with sqlite3.connect(str(self.mt5_db)) as conn:
            conn.executemany(
                """
                INSERT INTO mt5_execution_journal(created_at, source, signal_symbol, broker_symbol, resolved, outcome, pnl)
                VALUES(?,?,?,?,?,?,?)
                """,
                [
                    (now, "xauusd_scheduled", "XAUUSD", "XAUUSD", 1, 1, 8.0),
                    (now, "xauusd_scheduled", "XAUUSD", "XAUUSD", 1, 1, 7.0),
                    (now, "xauusd_scheduled", "XAUUSD", "XAUUSD", 1, 1, 6.0),
                    (now, "xauusd_scheduled", "XAUUSD", "XAUUSD", 1, 1, 5.0),
                    (now, "xauusd_scheduled", "XAUUSD", "XAUUSD", 1, 1, 4.0),
                    (now, "xauusd_scheduled", "XAUUSD", "XAUUSD", 1, 0, -2.0),
                ],
            )

        winner_report = {
            "symbols": [
                {
                    "symbol": "XAUUSD",
                    "model": {"resolved": 40, "wins": 25, "losses": 15, "win_rate": 0.625, "pnl_usd": 120.0, "avg_pnl_usd": 3.0},
                    "recommended_live_mode": "scheduled_winner_only",
                    "entry_bias": "limit_priority",
                    "top_model_sessions": [{"session": "new_york", "resolved": 12, "wins": 8, "losses": 4, "win_rate": 0.6667, "pnl_usd": 40.0, "avg_pnl_usd": 3.3333}],
                    "top_model_conf_bands": [{"band": "75-79.9", "resolved": 10, "wins": 7, "losses": 3, "win_rate": 0.7, "pnl_usd": 35.0, "avg_pnl_usd": 3.5}],
                }
            ]
        }
        canary_report = {
            "symbols": [
                {
                    "symbol": "XAUUSD",
                    "canary_total": {"resolved": 8, "wins": 3, "losses": 5, "win_rate": 0.375, "pnl_usd": -6.0, "avg_pnl_usd": -0.75},
                    "control_cross_backend_total": {"resolved": 6, "wins": 3, "losses": 3, "win_rate": 0.5, "pnl_usd": 2.0, "avg_pnl_usd": 0.3333},
                    "control_total": {"resolved": 6, "wins": 3, "losses": 3, "win_rate": 0.5, "pnl_usd": 2.0, "avg_pnl_usd": 0.3333},
                }
            ]
        }

        with patch("learning.live_profile_autopilot.config.STRATEGY_GENERATOR_ENABLED", True), \
             patch("learning.live_profile_autopilot.config.STRATEGY_GENERATOR_MIN_SAMPLE", 1), \
             patch("learning.live_profile_autopilot.config.get_strategy_walk_forward_windows_days", return_value=[3, 7, 14]), \
             patch("learning.live_profile_autopilot.config.MT5_ENABLED", True), \
             patch("learning.live_profile_autopilot.config.MT5_EXECUTE_XAUUSD", True), \
             patch("learning.live_profile_autopilot.config.STRATEGY_PROMOTION_MIN_SAMPLE", 4), \
             patch("learning.live_profile_autopilot.config.STRATEGY_PROMOTION_MIN_WIN_RATE_EDGE", 0.02), \
             patch("learning.live_profile_autopilot.config.STRATEGY_PROMOTION_MIN_PNL_EDGE_USD", 0.0), \
             patch("learning.live_profile_autopilot.config.STRATEGY_PROMOTION_MIN_SCORE_EDGE", 0.1), \
             patch("learning.live_profile_autopilot.config.STRATEGY_PROMOTION_REQUIRE_POSITIVE_SCORE", True):
            report = self.engine.build_strategy_lab_report(
                winner_report=winner_report,
                crypto_report={"symbols": []},
                canary_report=canary_report,
                audit_report={"recommendations": []},
            )

        self.assertTrue(report["ok"])
        self.assertGreaterEqual(len(list(report.get("walk_forward_ranking") or [])), 1)
        xau_policy = next(x for x in list(((report.get("meta_policy") or {}).get("symbols") or [])) if x["symbol"] == "XAUUSD")
        self.assertEqual(str(xau_policy.get("selected_family")), "xau_scheduled_trend")
        self.assertIn(str(xau_policy.get("selected_regime")), {"trend_priority", "sample_collection"})

    def test_strategy_promotion_gate_allows_staged_pullback_with_configured_uncertainty(self):
        spec = {
            "symbol": "XAUUSD",
            "family": "xau_scalp_pullback_limit",
            "promotion_capable": True,
            "execution_ready": True,
            "proposed_overrides": {
                "CTRADER_XAU_PRIMARY_FAMILY": "xau_scalp_pullback_limit",
                "CTRADER_XAU_ACTIVE_FAMILIES": "xau_scalp_pullback_limit,xau_scalp_breakout_stop",
            },
        }
        ranking = {
            "total": {"resolved": 31, "win_rate": 0.6129, "pnl_usd": 30.62},
            "canary_total": {"resolved": 31, "win_rate": 0.6129, "pnl_usd": 30.62},
            "observed_walk_forward_score": 28.5408,
            "purged_observed_walk_forward_score": None,
        }
        baseline = {"resolved": 42, "win_rate": 0.4762, "pnl_usd": -54.6}
        calibration = {
            "deflated_sharpe_proxy": 1.1088,
            "max_drawdown_usd": 16.23,
            "calibrated_win_rate": 0.6129,
            "uncertainty_score": 0.5535,
        }

        with patch.object(self.engine, "_execution_scope", return_value={"backend_focus": "ctrader_only"}), \
             patch("learning.live_profile_autopilot.config.STRATEGY_PROMOTION_MIN_SAMPLE", 8), \
             patch("learning.live_profile_autopilot.config.STRATEGY_PROMOTION_MIN_WIN_RATE_EDGE", 0.02), \
             patch("learning.live_profile_autopilot.config.STRATEGY_PROMOTION_MIN_PNL_EDGE_USD", 0.0), \
             patch("learning.live_profile_autopilot.config.STRATEGY_PROMOTION_MIN_SCORE_EDGE", 0.5), \
             patch("learning.live_profile_autopilot.config.STRATEGY_PROMOTION_REQUIRE_POSITIVE_SCORE", True), \
             patch("learning.live_profile_autopilot.config.STRATEGY_PROMOTION_MIN_DSR", 0.0), \
             patch("learning.live_profile_autopilot.config.STRATEGY_PROMOTION_MAX_DD_USD", 35.0), \
             patch("learning.live_profile_autopilot.config.STRATEGY_PROMOTION_STAGED_MAX_UNCERTAINTY", 0.60):
            gate = self.engine._evaluate_strategy_promotion_gate(spec, ranking, baseline, calibration)

        self.assertTrue(gate["eligible"])
        self.assertTrue(gate["staged_canary_primary"])
        self.assertEqual(list(gate.get("blockers") or []), [])

    def test_recent_win_cluster_memory_prefers_asian_pullback_and_excludes_invalid_tp_repair(self):
        rows = [
            (
                "2026-03-10T01:35:37Z",
                "scalp_xauusd:pb:canary",
                "XAUUSD",
                "long",
                73.6,
                5160.5048,
                5156.3349,
                5164.2559,
                "limit",
                json.dumps(
                    {
                        "direction": "long",
                        "session": "asian",
                        "timeframe": "5m+1m",
                        "entry_type": "limit",
                        "pattern": "SCALP_FLOW_FORCE|XAU_SCALP_PULLBACK_LIMIT",
                        "raw_scores": {
                            "scalp_force_trend_h1": "bullish",
                            "winner_logic_scope": "side_session:long:asian",
                            "winner_logic_regime": "neutral",
                        },
                    }
                ),
                "{}",
            ),
            (
                "2026-03-10T01:50:31Z",
                "scalp_xauusd:pb:canary",
                "XAUUSD",
                "long",
                73.4,
                5167.8181,
                5163.7327,
                5171.4932,
                "limit",
                json.dumps(
                    {
                        "direction": "long",
                        "session": "asian",
                        "timeframe": "5m+1m",
                        "entry_type": "limit",
                        "pattern": "SCALP_FLOW_FORCE|XAU_SCALP_PULLBACK_LIMIT",
                        "raw_scores": {
                            "scalp_force_trend_h1": "bullish",
                            "winner_logic_scope": "side_session:long:asian",
                            "winner_logic_regime": "neutral",
                        },
                    }
                ),
                "{}",
            ),
            (
                "2026-03-09T18:41:05Z",
                "scalp_xauusd:bs:canary",
                "XAUUSD",
                "long",
                72.0,
                5114.7765,
                5112.0279,
                5117.5250,
                "buy_stop",
                json.dumps(
                    {
                        "direction": "long",
                        "session": "new_york",
                        "timeframe": "5m+1m",
                        "entry_type": "buy_stop",
                        "pattern": "SCALP_FLOW_FORCE|XAU_SCALP_BREAKOUT_STOP",
                        "raw_scores": {
                            "scalp_force_trend_h1": "bearish",
                            "winner_logic_scope": "side_session:long:new_york",
                            "winner_logic_regime": "weak",
                        },
                    }
                ),
                "{}",
            ),
        ]
        with sqlite3.connect(str(self.ctrader_db)) as conn:
            conn.executemany(
                """
                INSERT INTO execution_journal (
                    created_utc, source, symbol, direction, confidence, entry, stop_loss,
                    take_profit, entry_type, request_json, execution_meta_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                rows,
            )
            journal_ids = [row[0] for row in conn.execute("SELECT id FROM execution_journal ORDER BY id ASC").fetchall()]
            conn.executemany(
                """
                INSERT INTO ctrader_deals (
                    deal_id, source, symbol, pnl_usd, outcome, has_close_detail, journal_id, execution_utc, raw_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    (
                        1,
                        "scalp_xauusd:pb:canary",
                        "XAUUSD",
                        4.30,
                        1,
                        1,
                        journal_ids[0],
                        "2026-03-10T01:41:20Z",
                        json.dumps({"raw": {"closePositionDetail": {"entryPrice": 5160.44}}}),
                    ),
                    (
                        2,
                        "scalp_xauusd:pb:canary",
                        "XAUUSD",
                        4.25,
                        1,
                        1,
                        journal_ids[1],
                        "2026-03-10T01:52:51Z",
                        json.dumps({"raw": {"closePositionDetail": {"entryPrice": 5168.86}}}),
                    ),
                    (
                        3,
                        "scalp_xauusd:bs:canary",
                        "XAUUSD",
                        35.98,
                        1,
                        1,
                        journal_ids[2],
                        "2026-03-10T01:31:08Z",
                        json.dumps({"raw": {"closePositionDetail": {"entryPrice": 5119.17}}}),
                    ),
                ],
            )

        with patch("learning.live_profile_autopilot.config.RECENT_WIN_CLUSTER_LOOKBACK_HOURS", 12), \
             patch("learning.live_profile_autopilot.config.RECENT_WIN_CLUSTER_MIN_RESOLVED", 2), \
             patch("learning.live_profile_autopilot.config.RECENT_WIN_CLUSTER_MAX_HOLD_MIN", 45), \
             patch("learning.live_profile_autopilot._utc_now", return_value=datetime(2026, 3, 10, 3, 0, tzinfo=timezone.utc)):
            report = self.engine.build_recent_win_cluster_memory_report()

        self.assertTrue(report["ok"])
        self.assertEqual(int((report.get("summary") or {}).get("excluded_repair_like_rows", 0)), 1)
        top = dict((report.get("clusters") or [])[0])
        self.assertEqual(str(top.get("family")), "xau_scalp_pullback_limit")
        self.assertEqual(str(top.get("session")), "asian")
        self.assertEqual(str(top.get("direction")), "long")
        self.assertEqual(str(top.get("entry_type")), "limit")
        self.assertTrue(bool(top.get("memory_eligible")))

    def test_winner_memory_library_marks_market_beating_situation(self):
        rows = [
            (
                "2026-03-09T23:30:00Z",
                "scalp_xauusd:pb:canary",
                "XAUUSD",
                "long",
                73.2,
                5160.10,
                5164.20,
                "limit",
                json.dumps(
                    {
                        "direction": "long",
                        "session": "asian",
                        "timeframe": "5m+1m",
                        "entry_type": "limit",
                        "pattern": "SCALP_FLOW_FORCE|XAU_SCALP_PULLBACK_LIMIT",
                        "raw_scores": {
                            "scalp_force_trend_h1": "bullish",
                            "winner_logic_scope": "side_session:long:asian",
                            "winner_logic_regime": "neutral",
                        },
                    }
                ),
                "{}",
            ),
            (
                "2026-03-09T23:40:00Z",
                "scalp_xauusd:pb:canary",
                "XAUUSD",
                "long",
                73.4,
                5161.20,
                5165.40,
                "limit",
                json.dumps(
                    {
                        "direction": "long",
                        "session": "asian",
                        "timeframe": "5m+1m",
                        "entry_type": "limit",
                        "pattern": "SCALP_FLOW_FORCE|XAU_SCALP_PULLBACK_LIMIT",
                        "raw_scores": {
                            "scalp_force_trend_h1": "bullish",
                            "winner_logic_scope": "side_session:long:asian",
                            "winner_logic_regime": "neutral",
                        },
                    }
                ),
                "{}",
            ),
            (
                "2026-03-09T23:50:00Z",
                "scalp_xauusd:pb:canary",
                "XAUUSD",
                "long",
                73.1,
                5162.00,
                5166.10,
                "limit",
                json.dumps(
                    {
                        "direction": "long",
                        "session": "asian",
                        "timeframe": "5m+1m",
                        "entry_type": "limit",
                        "pattern": "SCALP_FLOW_FORCE|XAU_SCALP_PULLBACK_LIMIT",
                        "raw_scores": {
                            "scalp_force_trend_h1": "bullish",
                            "winner_logic_scope": "side_session:long:asian",
                            "winner_logic_regime": "neutral",
                        },
                    }
                ),
                "{}",
            ),
            (
                "2026-03-10T00:00:00Z",
                "scalp_xauusd:canary",
                "XAUUSD",
                "long",
                71.2,
                5158.50,
                5160.40,
                "limit",
                json.dumps(
                    {
                        "direction": "long",
                        "session": "asian",
                        "timeframe": "5m+1m",
                        "entry_type": "limit",
                        "pattern": "SCALP_FLOW_FORCE",
                        "raw_scores": {"scalp_force_trend_h1": "bullish"},
                    }
                ),
                "{}",
            ),
            (
                "2026-03-10T00:10:00Z",
                "scalp_xauusd:canary",
                "XAUUSD",
                "long",
                71.0,
                5159.20,
                5161.00,
                "limit",
                json.dumps(
                    {
                        "direction": "long",
                        "session": "asian",
                        "timeframe": "5m+1m",
                        "entry_type": "limit",
                        "pattern": "SCALP_FLOW_FORCE",
                        "raw_scores": {"scalp_force_trend_h1": "bullish"},
                    }
                ),
                "{}",
            ),
            (
                "2026-03-10T00:20:00Z",
                "scalp_xauusd:bs:canary",
                "XAUUSD",
                "long",
                72.0,
                5114.7765,
                5117.5250,
                "buy_stop",
                json.dumps(
                    {
                        "direction": "long",
                        "session": "new_york",
                        "timeframe": "5m+1m",
                        "entry_type": "buy_stop",
                        "pattern": "SCALP_FLOW_FORCE|XAU_SCALP_BREAKOUT_STOP",
                        "raw_scores": {"scalp_force_trend_h1": "bullish"},
                    }
                ),
                "{}",
            ),
        ]
        with sqlite3.connect(str(self.ctrader_db)) as conn:
            conn.executemany(
                """
                INSERT INTO execution_journal (
                    created_utc, source, symbol, direction, confidence, entry,
                    take_profit, entry_type, request_json, execution_meta_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                rows,
            )
            journal_ids = [row[0] for row in conn.execute("SELECT id FROM execution_journal ORDER BY id ASC").fetchall()]
            conn.executemany(
                """
                INSERT INTO ctrader_deals (
                    deal_id, source, symbol, pnl_usd, outcome, has_close_detail, journal_id, execution_utc, raw_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    (11, "scalp_xauusd:pb:canary", "XAUUSD", 4.1, 1, 1, journal_ids[0], "2026-03-09T23:34:00Z", json.dumps({"raw": {"closePositionDetail": {"entryPrice": 5160.10}}})),
                    (12, "scalp_xauusd:pb:canary", "XAUUSD", 4.2, 1, 1, journal_ids[1], "2026-03-09T23:44:00Z", json.dumps({"raw": {"closePositionDetail": {"entryPrice": 5161.20}}})),
                    (13, "scalp_xauusd:pb:canary", "XAUUSD", 4.3, 1, 1, journal_ids[2], "2026-03-09T23:54:00Z", json.dumps({"raw": {"closePositionDetail": {"entryPrice": 5162.00}}})),
                    (14, "scalp_xauusd:canary", "XAUUSD", -2.1, 0, 1, journal_ids[3], "2026-03-10T00:03:00Z", json.dumps({"raw": {"closePositionDetail": {"entryPrice": 5158.50}}})),
                    (15, "scalp_xauusd:canary", "XAUUSD", -1.7, 0, 1, journal_ids[4], "2026-03-10T00:13:00Z", json.dumps({"raw": {"closePositionDetail": {"entryPrice": 5159.20}}})),
                    (16, "scalp_xauusd:bs:canary", "XAUUSD", 35.98, 1, 1, journal_ids[5], "2026-03-10T00:25:00Z", json.dumps({"raw": {"closePositionDetail": {"entryPrice": 5119.17}}})),
                ],
            )

        with patch("learning.live_profile_autopilot.config.WINNER_MEMORY_LIBRARY_LOOKBACK_DAYS", 3), \
             patch("learning.live_profile_autopilot.config.WINNER_MEMORY_LIBRARY_MIN_RESOLVED", 3), \
             patch("learning.live_profile_autopilot.config.WINNER_MEMORY_LIBRARY_MIN_WIN_RATE", 0.60), \
             patch("learning.live_profile_autopilot._utc_now", return_value=datetime(2026, 3, 10, 3, 0, tzinfo=timezone.utc)):
            report = self.engine.build_winner_memory_library_report()

        self.assertTrue(report["ok"])
        self.assertEqual(int((report.get("summary") or {}).get("excluded_repair_like_rows", 0)), 1)
        self.assertEqual(int((report.get("summary") or {}).get("market_beating", 0)), 1)
        xau = dict((report.get("top_by_symbol") or {}).get("XAUUSD") or {})
        self.assertEqual(str(xau.get("family")), "xau_scalp_pullback_limit")
        self.assertTrue(bool(xau.get("market_beating")))
        self.assertEqual(str(xau.get("session")), "asian")

    def test_build_chart_state_memory_report_identifies_follow_up_state(self):
        now = datetime.now(timezone.utc)
        rows = [
            (
                (now - timedelta(days=1, minutes=12)).strftime("%Y-%m-%dT%H:%M:%SZ"),
                "scalp_xauusd:pb:canary",
                "XAUUSD",
                "long",
                73.2,
                5160.10,
                5156.40,
                5164.20,
                "limit",
                json.dumps(
                    {
                        "direction": "long",
                        "session": "asian",
                        "timeframe": "5m+1m",
                        "entry_type": "limit",
                        "pattern": "SCALP_FLOW_FORCE|XAU_SCALP_PULLBACK_LIMIT",
                        "raw_scores": {
                            "scalp_force_trend_h1": "bullish",
                            "m5_momentum": 1.8,
                        },
                    }
                ),
                json.dumps({"market_capture": {"run_id": "xau_cap_1"}}),
            ),
            (
                (now - timedelta(days=1, minutes=4)).strftime("%Y-%m-%dT%H:%M:%SZ"),
                "scalp_xauusd:pb:canary",
                "XAUUSD",
                "long",
                73.5,
                5161.20,
                5157.20,
                5165.40,
                "limit",
                json.dumps(
                    {
                        "direction": "long",
                        "session": "asian",
                        "timeframe": "5m+1m",
                        "entry_type": "limit",
                        "pattern": "SCALP_FLOW_FORCE|XAU_SCALP_PULLBACK_LIMIT",
                        "raw_scores": {
                            "scalp_force_trend_h1": "bullish",
                            "m5_momentum": 1.6,
                        },
                    }
                ),
                json.dumps({"market_capture": {"run_id": "xau_cap_2"}}),
            ),
        ]
        with sqlite3.connect(str(self.ctrader_db)) as conn:
            conn.executemany(
                """
                INSERT INTO execution_journal (
                    created_utc, source, symbol, direction, confidence, entry, stop_loss,
                    take_profit, entry_type, request_json, execution_meta_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                rows,
            )
            journal_ids = [row[0] for row in conn.execute("SELECT id FROM execution_journal ORDER BY id ASC").fetchall()]
            conn.executemany(
                """
                INSERT INTO ctrader_deals (
                    deal_id, source, symbol, pnl_usd, outcome, has_close_detail, journal_id, execution_utc, raw_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    (
                        31,
                        "scalp_xauusd:pb:canary",
                        "XAUUSD",
                        4.6,
                        1,
                        1,
                        journal_ids[0],
                        (now - timedelta(days=1, minutes=8)).strftime("%Y-%m-%dT%H:%M:%SZ"),
                        json.dumps({"raw": {"closePositionDetail": {"entryPrice": 5160.10}}}),
                    ),
                    (
                        32,
                        "scalp_xauusd:pb:canary",
                        "XAUUSD",
                        4.9,
                        1,
                        1,
                        journal_ids[1],
                        (now - timedelta(days=1)).strftime("%Y-%m-%dT%H:%M:%SZ"),
                        json.dumps({"raw": {"closePositionDetail": {"entryPrice": 5161.20}}}),
                    ),
                ],
            )
        capture_features = {
            "day_type": "trend",
            "delta_proxy": 0.15,
            "depth_imbalance": 0.08,
            "rejection_ratio": 0.12,
            "bar_volume_proxy": 0.67,
            "spread_expansion": 1.03,
        }
        with patch("learning.live_profile_autopilot.config.CHART_STATE_MEMORY_MIN_RESOLVED", 2, create=True), \
             patch.object(self.engine, "_load_market_capture_features", return_value=capture_features):
            report = self.engine.build_chart_state_memory_report(days=7)

        self.assertTrue(report["ok"])
        self.assertGreaterEqual(int((report.get("summary") or {}).get("follow_up_candidates", 0)), 1)
        xau = dict((report.get("top_by_symbol") or {}).get("XAUUSD") or {})
        self.assertEqual(str(xau.get("state_label") or ""), "continuation_drive")
        self.assertTrue(bool(xau.get("follow_up_candidate")))
        self.assertEqual(str(((xau.get("best_family") or {}).get("family") or "")), "xau_scalp_pullback_limit")

    def test_mission_progress_report_includes_asian_long_memory(self):
        strategy_lab_report = {
            "meta_policy": {"symbols": [{"symbol": "XAUUSD", "selected_family": "xau_scalp_microtrend", "selected_regime": "continuation"}]},
            "candidates": [],
            "recent_win_memory": {
                "clusters": [
                    {
                        "symbol": "XAUUSD",
                        "family": "xau_scalp_microtrend",
                        "direction": "long",
                        "session": "asian",
                        "timeframe": "5m+1m",
                        "entry_type": "limit",
                        "pattern": "SCALP_FLOW_FORCE",
                        "confidence_band": "70-74.9",
                        "wins": 5,
                        "resolved": 5,
                        "pnl_usd": 22.41,
                        "memory_eligible": True,
                    }
                ],
                "top_by_symbol": {
                    "XAUUSD": {
                        "symbol": "XAUUSD",
                        "family": "xau_scalp_microtrend",
                    }
                },
            },
            "winner_memory_library": {
                "top_by_symbol": {
                    "XAUUSD": {
                        "symbol": "XAUUSD",
                        "family": "xau_scalp_pullback_limit",
                        "session": "asian",
                        "direction": "long",
                        "stats": {"wins": 6, "resolved": 6, "pnl_usd": 26.46},
                    }
                }
            },
            "chart_state_memory": {
                "top_by_symbol": {
                    "XAUUSD": {
                        "symbol": "XAUUSD",
                        "state_label": "continuation_drive",
                        "best_family": {"family": "xau_scalp_pullback_limit"},
                        "follow_up_candidate": True,
                    }
                }
            },
        }
        report = self.engine.build_mission_progress_report(
            winner_report={"symbols": []},
            crypto_report={"recommendations": []},
            audit_report={"summary": {}},
            canary_report={"symbols": []},
            auto_apply_report={"status": "waiting_active_canary"},
            strategy_lab_report=strategy_lab_report,
        )
        xau = next(x for x in list(report.get("symbols") or []) if x.get("symbol") == "XAUUSD")
        asian = dict(xau.get("asian_long_memory") or {})
        self.assertEqual(str(asian.get("session")), "asian")
        self.assertEqual(str(asian.get("direction")), "long")
        self.assertEqual(str(asian.get("family")), "xau_scalp_microtrend")
        library = dict(xau.get("winner_memory_library") or {})
        self.assertEqual(str(library.get("family")), "xau_scalp_pullback_limit")
        chart_state = dict(xau.get("chart_state_memory") or {})
        self.assertEqual(str(chart_state.get("state_label") or ""), "continuation_drive")
        self.assertEqual(str(((chart_state.get("best_family") or {}).get("family") or "")), "xau_scalp_pullback_limit")

    def test_build_xau_direct_lane_report_aggregates_main_and_winner(self):
        now = datetime.now(timezone.utc)
        journal_main_ts = (now - timedelta(hours=2)).strftime("%Y-%m-%dT%H:%M:%SZ")
        journal_winner_ts = (now - timedelta(hours=1, minutes=55)).strftime("%Y-%m-%dT%H:%M:%SZ")
        deal_main_ts = (now - timedelta(hours=1, minutes=58)).strftime("%Y-%m-%dT%H:%M:%SZ")
        deal_winner_ts = (now - timedelta(hours=1, minutes=54)).strftime("%Y-%m-%dT%H:%M:%SZ")
        with sqlite3.connect(str(self.ctrader_db)) as conn:
            conn.executemany(
                """
                INSERT INTO execution_journal(
                    created_utc, source, symbol, direction, entry_type, confidence,
                    entry, stop_loss, take_profit, request_json, execution_meta_json, status
                ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                [
                    (
                        journal_main_ts,
                        "scalp_xauusd",
                        "XAUUSD",
                        "long",
                        "limit",
                        73.0,
                        5200.0,
                        5196.0,
                        5206.0,
                        json.dumps({"raw_scores": {"xau_multi_tf_snapshot": {"strict_alignment": "aligned_bullish"}}}),
                        "{}",
                        "accepted",
                    ),
                    (
                        journal_winner_ts,
                        "scalp_xauusd:winner",
                        "XAUUSD",
                        "long",
                        "stop",
                        74.0,
                        5201.0,
                        5197.0,
                        5207.0,
                        json.dumps({"raw_scores": {"xau_multi_tf_snapshot": {"strict_alignment": "aligned_bullish"}}}),
                        "{}",
                        "accepted",
                    ),
                ],
            )
            ids = [row[0] for row in conn.execute("SELECT id FROM execution_journal ORDER BY id ASC").fetchall()]
            conn.executemany(
                """
                INSERT INTO ctrader_deals(
                    deal_id, source, symbol, pnl_usd, outcome, has_close_detail, journal_id, execution_utc, raw_json
                ) VALUES(?,?,?,?,?,?,?,?,?)
                """,
                [
                    (1, "scalp_xauusd", "XAUUSD", 3.2, 1, 1, ids[0], deal_main_ts, "{}"),
                    (2, "scalp_xauusd:winner", "XAUUSD", -1.4, 0, 1, ids[1], deal_winner_ts, "{}"),
                ],
            )
            conn.commit()

        report = self.engine.build_xau_direct_lane_report(hours=72)
        self.assertTrue(report["ok"])
        self.assertEqual(int((report.get("summary") or {}).get("sent", 0)), 2)
        self.assertEqual(int((report.get("summary") or {}).get("resolved", 0)), 2)
        self.assertEqual(int(((report.get("sources") or {}).get("main") or {}).get("wins", 0)), 1)
        self.assertEqual(int(((report.get("sources") or {}).get("winner") or {}).get("losses", 0)), 1)
        self.assertEqual(int(((report.get("strict_alignment") or {}).get("aligned_bullish") or {}).get("resolved", 0)), 2)

    def test_build_ctrader_data_integrity_report_repairs_deal_context_from_journal(self):
        now = datetime.now(timezone.utc)
        journal_ts = (now - timedelta(hours=2)).strftime("%Y-%m-%dT%H:%M:%SZ")
        deal_ts = (now - timedelta(hours=1, minutes=55)).strftime("%Y-%m-%dT%H:%M:%SZ")
        request_payload = {
            "entry_type": "limit",
            "pattern": "SCALP_FLOW_FORCE",
            "timeframe": "5m+1m",
            "session": "london",
            "reasons": ["flow aligned", "fallback cadence"],
            "warnings": ["reduced risk"],
            "raw_scores": {
                "xau_multi_tf_snapshot": {
                    "strict_alignment": "aligned_bearish",
                    "strict_aligned_side": "short",
                }
            },
        }
        with sqlite3.connect(str(self.ctrader_db)) as conn:
            conn.execute(
                """
                INSERT INTO execution_journal(
                    created_utc, source, symbol, direction, entry_type, confidence,
                    entry, stop_loss, take_profit, request_json, execution_meta_json, status
                ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    journal_ts,
                    "scalp_xauusd:canary",
                    "XAUUSD",
                    "short",
                    "limit",
                    72.0,
                    4712.0,
                    4720.0,
                    4704.0,
                    json.dumps(request_payload),
                    "{}",
                    "closed",
                ),
            )
            journal_id = int(conn.execute("SELECT id FROM execution_journal LIMIT 1").fetchone()[0])
            conn.execute(
                """
                INSERT INTO ctrader_deals(
                    deal_id, source, symbol, pnl_usd, outcome, has_close_detail, journal_id, execution_utc, raw_json
                ) VALUES(?,?,?,?,?,?,?,?,?)
                """,
                (11, "scalp_xauusd:canary", "XAUUSD", 8.4, 1, 1, journal_id, deal_ts, "{}"),
            )
            conn.commit()

        report = self.engine.build_ctrader_data_integrity_report(days=30, repair=True)
        self.assertTrue(report["ok"])
        self.assertEqual(int((report.get("summary") or {}).get("deal_rows_repaired", 0)), 1)
        self.assertEqual(int((report.get("summary") or {}).get("deal_rows_remaining_missing", 0)), 0)
        with sqlite3.connect(str(self.ctrader_db)) as conn:
            raw_json = conn.execute("SELECT raw_json FROM ctrader_deals WHERE deal_id=11").fetchone()[0]
        raw = json.loads(raw_json)
        self.assertEqual(str(raw.get("family") or ""), "xau_scalp_microtrend")
        self.assertEqual(str(raw.get("strategy_family") or ""), "xau_scalp_microtrend")
        self.assertEqual(str(raw.get("entry_type") or ""), "limit")
        self.assertEqual(str(raw.get("session") or ""), "london")
        self.assertEqual(str(raw.get("timeframe") or ""), "5m+1m")
        self.assertEqual(str(raw.get("pattern") or ""), "SCALP_FLOW_FORCE")
        self.assertEqual(list(raw.get("reasons") or []), ["flow aligned", "fallback cadence"])
        self.assertEqual(list(raw.get("warnings") or []), ["reduced risk"])
        self.assertEqual(str(raw.get("strict_alignment") or ""), "aligned_bearish")
        self.assertEqual(str(raw.get("xau_mtf_aligned_side") or ""), "short")

    def test_auto_tune_xau_direct_lane_tightens_confidence_band(self):
        with patch("learning.live_profile_autopilot.config.XAU_DIRECT_LANE_AUTO_TUNE_ENABLED", True), \
             patch("learning.live_profile_autopilot.config.AUTO_APPLY_LIVE_PROFILE_PERSIST_ENV", True), \
             patch("learning.live_profile_autopilot.config.XAU_DIRECT_LANE_AUTO_TUNE_MIN_RESOLVED", 4), \
             patch("learning.live_profile_autopilot.config.XAU_DIRECT_LANE_AUTO_TUNE_TIGHTEN_MAX_WIN_RATE", 0.45), \
             patch("learning.live_profile_autopilot.config.XAU_DIRECT_LANE_AUTO_TUNE_TIGHTEN_MAX_PNL_USD", -2.0), \
             patch("learning.live_profile_autopilot.config.XAU_DIRECT_LANE_AUTO_TUNE_CONF_STEP", 0.5), \
             patch("learning.live_profile_autopilot.config.MT5_SCALP_XAU_LIVE_CONF_MIN", 72.0), \
             patch("learning.live_profile_autopilot.config.MT5_SCALP_XAU_LIVE_CONF_MAX", 75.0):
            out = self.engine.auto_tune_xau_direct_lane(
                report={
                    "ok": True,
                    "summary": {
                        "resolved": 4,
                        "win_rate": 0.25,
                        "pnl_usd": -6.0,
                        "fill_rate": 0.75,
                    },
                }
            )

        self.assertTrue(out["ok"])
        self.assertEqual(out["status"], "tightened")
        env_text = self.env_local.read_text(encoding="utf-8")
        self.assertIn("MT5_SCALP_XAU_LIVE_CONF_MIN=72.50", env_text)
        self.assertIn("MT5_SCALP_XAU_LIVE_CONF_MAX=74.50", env_text)

    def test_auto_apply_live_profile_applies_promotable_strategy_candidate(self):
        strategy_lab_report = {
            "promotable_candidates": [
                {
                    "symbol": "XAUUSD",
                    "strategy_id": "xau_scheduled_trend_v1",
                    "family": "xau_scheduled_trend",
                    "execution_ready": True,
                    "promotion_gate": {"eligible": True},
                    "walk_forward_score": 3.4,
                    "proposed_overrides": {
                        "MT5_XAU_SCHEDULED_LIVE_MIN_CONFIDENCE": "79",
                        "MT5_XAU_SCHEDULED_LIVE_SESSIONS": "new_york|overlap",
                    },
                }
            ]
        }
        with patch("learning.live_profile_autopilot.config.AUTO_APPLY_LIVE_PROFILE_ENABLED", True), \
             patch("learning.live_profile_autopilot.config.AUTO_APPLY_LIVE_PROFILE_PERSIST_ENV", True), \
             patch("learning.live_profile_autopilot.config.AUTO_APPLY_LIVE_PROFILE_MIN_SAMPLE", 6), \
             patch("learning.live_profile_autopilot.config.STRATEGY_PROMOTION_ENABLED", True):
            out = self.engine.auto_apply_live_profile(
                winner_report={"recommendations": [], "symbols": []},
                crypto_report={"recommendations": [], "symbols": []},
                audit_report={"recommendations": []},
                canary_report={"recommendations": []},
                strategy_lab_report=strategy_lab_report,
            )

        self.assertTrue(out["ok"])
        self.assertEqual(out["status"], "applied")
        self.assertTrue(list(out.get("strategy_promotions") or []))
        env_text = self.env_local.read_text(encoding="utf-8")
        self.assertIn("MT5_XAU_SCHEDULED_LIVE_MIN_CONFIDENCE=79", env_text)

    def test_build_ct_only_experiment_report_recommends_btc_promotion(self):
        now = datetime.now(timezone.utc)
        with sqlite3.connect(str(self.ctrader_db)) as conn:
            conn.executemany(
                """
                INSERT INTO execution_journal(
                    created_utc, source, symbol, direction, confidence, entry, stop_loss, take_profit, entry_type, request_json, execution_meta_json
                ) VALUES(?,?,?,?,?,?,?,?,?,?,?)
                """,
                [
                    ((now - timedelta(hours=6)).strftime("%Y-%m-%dT%H:%M:%SZ"), "scalp_btcusd:bwl:canary", "BTCUSD", "long", 71.0, 70000.0, 69800.0, 70200.0, "limit", "{}", "{}"),
                    ((now - timedelta(hours=5, minutes=55)).strftime("%Y-%m-%dT%H:%M:%SZ"), "scalp_btcusd:bwl:canary", "BTCUSD", "long", 72.0, 70020.0, 69820.0, 70220.0, "limit", "{}", "{}"),
                    ((now - timedelta(hours=5, minutes=50)).strftime("%Y-%m-%dT%H:%M:%SZ"), "scalp_btcusd:bwl:canary", "BTCUSD", "long", 73.0, 70040.0, 69840.0, 70240.0, "limit", "{}", "{}"),
                    ((now - timedelta(hours=5, minutes=45)).strftime("%Y-%m-%dT%H:%M:%SZ"), "scalp_btcusd:bwl:canary", "BTCUSD", "long", 74.0, 70060.0, 69860.0, 70260.0, "limit", "{}", "{}"),
                    ((now - timedelta(hours=5, minutes=40)).strftime("%Y-%m-%dT%H:%M:%SZ"), "scalp_xauusd:pb:canary", "XAUUSD", "long", 73.0, 5190.0, 5186.0, 5194.0, "limit", "{}", "{}"),
                    ((now - timedelta(hours=5, minutes=35)).strftime("%Y-%m-%dT%H:%M:%SZ"), "scalp_xauusd:td:canary", "XAUUSD", "long", 73.0, 5191.0, 5187.0, 5195.0, "limit", "{}", "{}"),
                ],
            )
            ids = [row[0] for row in conn.execute("SELECT id FROM execution_journal ORDER BY id ASC").fetchall()]
            conn.executemany(
                """
                INSERT INTO ctrader_deals(
                    deal_id, source, symbol, pnl_usd, outcome, has_close_detail, journal_id, execution_utc, raw_json
                ) VALUES(?,?,?,?,?,?,?,?,?)
                """,
                [
                    (1, "scalp_btcusd:bwl:canary", "BTCUSD", 1.5, 1, 1, ids[0], (now - timedelta(hours=5, minutes=59)).strftime("%Y-%m-%dT%H:%M:%SZ"), "{}"),
                    (2, "scalp_btcusd:bwl:canary", "BTCUSD", 1.2, 1, 1, ids[1], (now - timedelta(hours=5, minutes=54)).strftime("%Y-%m-%dT%H:%M:%SZ"), "{}"),
                    (3, "scalp_btcusd:bwl:canary", "BTCUSD", 1.1, 1, 1, ids[2], (now - timedelta(hours=5, minutes=49)).strftime("%Y-%m-%dT%H:%M:%SZ"), "{}"),
                    (4, "scalp_btcusd:bwl:canary", "BTCUSD", 1.4, 1, 1, ids[3], (now - timedelta(hours=5, minutes=44)).strftime("%Y-%m-%dT%H:%M:%SZ"), "{}"),
                    (5, "scalp_xauusd:pb:canary", "XAUUSD", 2.0, 1, 1, ids[4], (now - timedelta(hours=5, minutes=39)).strftime("%Y-%m-%dT%H:%M:%SZ"), "{}"),
                    (6, "scalp_xauusd:td:canary", "XAUUSD", 3.0, 1, 1, ids[5], (now - timedelta(hours=5, minutes=34)).strftime("%Y-%m-%dT%H:%M:%SZ"), "{}"),
                ],
            )
        with patch("learning.live_profile_autopilot.config.CT_ONLY_EXPERIMENT_REPORT_LOOKBACK_HOURS", 24), \
             patch("learning.live_profile_autopilot.config.BTC_WEEKDAY_LOB_PROMOTION_MIN_RESOLVED", 4), \
             patch("learning.live_profile_autopilot.config.BTC_WEEKDAY_LOB_PROMOTION_MIN_WIN_RATE", 0.55), \
             patch("learning.live_profile_autopilot.config.BTC_WEEKDAY_LOB_PROMOTION_MIN_PNL_USD", 1.0), \
             patch("learning.live_profile_autopilot.config.XAU_TD_VS_PB_COMPARE_MIN_RESOLVED", 1):
            report = self.engine.build_ct_only_experiment_report()
        self.assertTrue(report["ok"])
        recs = list(report.get("recommendations") or [])
        promote = next((x for x in recs if x.get("action") == "promote_btc_weekday_lob_narrow_live"), {})
        self.assertEqual(str(promote.get("family")), "btc_weekday_lob_momentum")
        compare = dict((report.get("comparisons") or {}).get("xau_td_vs_pb_live") or {})
        self.assertEqual(str(compare.get("leader")), "xau_scalp_tick_depth_filter")
        ff_compare = dict((report.get("comparisons") or {}).get("xau_ff_effectiveness") or {})
        self.assertEqual(int(ff_compare.get("launches", 0) or 0), 0)
        btc_row = next((x for x in list(report.get("sources") or []) if str(x.get("source")) == "scalp_btcusd:bwl:canary"), {})
        self.assertIn("HLOB", str(btc_row.get("external_prior_summary", "")))
        self.assertEqual(list(btc_row.get("external_prior_models") or []), ["HLOB", "EarnHFT", "DeepFolio"])

    def test_build_ct_only_experiment_report_tracks_failed_fade_follow_stop(self):
        now = datetime.now(timezone.utc)
        with sqlite3.connect(str(self.ctrader_db)) as conn:
            conn.executemany(
                """
                INSERT INTO execution_journal(
                    created_utc, source, symbol, direction, confidence, entry, stop_loss, take_profit, entry_type, request_json, response_json, execution_meta_json, status
                ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                [
                    ((now - timedelta(hours=4)).strftime("%Y-%m-%dT%H:%M:%SZ"), "scalp_xauusd:ff:canary", "XAUUSD", "long", 72.0, 5193.5, 5191.8, 5196.0, "buy_stop", "{}", "{}", "{}", "closed"),
                    ((now - timedelta(hours=3, minutes=55)).strftime("%Y-%m-%dT%H:%M:%SZ"), "scalp_xauusd:pb:canary", "XAUUSD", "short", 73.0, 5192.5, 5194.0, 5190.2, "limit", "{}", "{}", "{}", "canceled"),
                ],
            )
            ids = [row[0] for row in conn.execute("SELECT id FROM execution_journal ORDER BY id ASC").fetchall()]
            conn.execute(
                """
                INSERT INTO ctrader_deals(
                    deal_id, source, symbol, pnl_usd, outcome, has_close_detail, journal_id, execution_utc, raw_json
                ) VALUES(?,?,?,?,?,?,?,?,?)
                """,
                (101, "scalp_xauusd:ff:canary", "XAUUSD", 2.4, 1, 1, ids[0], (now - timedelta(hours=3, minutes=53)).strftime("%Y-%m-%dT%H:%M:%SZ"), "{}"),
            )
            conn.commit()
        with patch("learning.live_profile_autopilot.config.CT_ONLY_EXPERIMENT_REPORT_LOOKBACK_HOURS", 24):
            report = self.engine.build_ct_only_experiment_report()
        self.assertTrue(report["ok"])
        self.assertEqual(int((report.get("summary") or {}).get("xau_ff_resolved", 0) or 0), 1)
        ff_row = next((x for x in list(report.get("sources") or []) if str(x.get("source")) == "scalp_xauusd:ff:canary"), {})
        self.assertEqual(list(ff_row.get("external_prior_models") or []), ["DeepLOB", "EarnHFT", "BDLOB"])
        ff_compare = dict((report.get("comparisons") or {}).get("xau_ff_effectiveness") or {})
        self.assertEqual(int(ff_compare.get("launches", 0) or 0), 1)
        self.assertEqual(str(ff_compare.get("recommendation")), "collect_ff_sample")

    def test_build_ct_only_experiment_report_tracks_microtrend_follow_up(self):
        now = datetime.now(timezone.utc)
        with sqlite3.connect(str(self.ctrader_db)) as conn:
            conn.executemany(
                """
                INSERT INTO execution_journal(
                    created_utc, source, symbol, direction, confidence, entry, stop_loss, take_profit, entry_type, request_json, response_json, execution_meta_json, status
                ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                [
                    ((now - timedelta(hours=3)).strftime("%Y-%m-%dT%H:%M:%SZ"), "scalp_xauusd:mfu:canary", "XAUUSD", "short", 74.1, 5163.2, 5166.1, 5160.2, "sell_stop", "{}", "{}", "{}", "closed"),
                    ((now - timedelta(hours=2, minutes=55)).strftime("%Y-%m-%dT%H:%M:%SZ"), "scalp_xauusd:canary", "XAUUSD", "short", 74.0, 5163.4, 5166.3, 5160.0, "limit", "{}", "{}", "{}", "closed"),
                ],
            )
            ids = [row[0] for row in conn.execute("SELECT id FROM execution_journal ORDER BY id ASC").fetchall()]
            conn.executemany(
                """
                INSERT INTO ctrader_deals(
                    deal_id, source, symbol, pnl_usd, outcome, has_close_detail, journal_id, execution_utc, raw_json
                ) VALUES(?,?,?,?,?,?,?,?,?)
                """,
                [
                    (111, "scalp_xauusd:mfu:canary", "XAUUSD", 3.4, 1, 1, ids[0], (now - timedelta(hours=2, minutes=53)).strftime("%Y-%m-%dT%H:%M:%SZ"), "{}"),
                    (112, "scalp_xauusd:canary", "XAUUSD", 1.2, 1, 1, ids[1], (now - timedelta(hours=2, minutes=52)).strftime("%Y-%m-%dT%H:%M:%SZ"), "{}"),
                ],
            )
            conn.commit()
        with patch("learning.live_profile_autopilot.config.CT_ONLY_EXPERIMENT_REPORT_LOOKBACK_HOURS", 24):
            report = self.engine.build_ct_only_experiment_report()
        self.assertTrue(report["ok"])
        self.assertEqual(int((report.get("summary") or {}).get("xau_mfu_resolved", 0) or 0), 1)
        mfu_row = next((x for x in list(report.get("sources") or []) if str(x.get("source")) == "scalp_xauusd:mfu:canary"), {})
        self.assertEqual(list(mfu_row.get("external_prior_models") or []), ["DeepLOB", "EarnHFT", "BDLOB"])
        compare = dict((report.get("comparisons") or {}).get("xau_mfu_vs_broad_microtrend_live") or {})
        self.assertEqual(str(compare.get("leader")), "xau_scalp_microtrend_follow_up")

    def test_build_ct_only_experiment_report_tracks_flow_short_sidecar(self):
        now = datetime.now(timezone.utc)
        with sqlite3.connect(str(self.ctrader_db)) as conn:
            conn.executemany(
                """
                INSERT INTO execution_journal(
                    created_utc, source, symbol, direction, confidence, entry, stop_loss, take_profit, entry_type, request_json, response_json, execution_meta_json, status
                ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                [
                    ((now - timedelta(hours=2)).strftime("%Y-%m-%dT%H:%M:%SZ"), "scalp_xauusd:fss:canary", "XAUUSD", "short", 73.6, 5164.0, 5167.2, 5160.4, "sell_stop", "{}", "{}", "{}", "closed"),
                    ((now - timedelta(hours=1, minutes=55)).strftime("%Y-%m-%dT%H:%M:%SZ"), "scalp_xauusd:canary", "XAUUSD", "short", 73.4, 5163.4, 5166.5, 5160.3, "limit", "{}", "{}", "{}", "closed"),
                ],
            )
            ids = [row[0] for row in conn.execute("SELECT id FROM execution_journal ORDER BY id ASC").fetchall()]
            conn.executemany(
                """
                INSERT INTO ctrader_deals(
                    deal_id, source, symbol, pnl_usd, outcome, has_close_detail, journal_id, execution_utc, raw_json
                ) VALUES(?,?,?,?,?,?,?,?,?)
                """,
                [
                    (121, "scalp_xauusd:fss:canary", "XAUUSD", 4.1, 1, 1, ids[0], (now - timedelta(hours=1, minutes=54)).strftime("%Y-%m-%dT%H:%M:%SZ"), "{}"),
                    (122, "scalp_xauusd:canary", "XAUUSD", 1.3, 1, 1, ids[1], (now - timedelta(hours=1, minutes=53)).strftime("%Y-%m-%dT%H:%M:%SZ"), "{}"),
                ],
            )
            conn.commit()
        with patch("learning.live_profile_autopilot.config.CT_ONLY_EXPERIMENT_REPORT_LOOKBACK_HOURS", 24):
            report = self.engine.build_ct_only_experiment_report()
        self.assertTrue(report["ok"])
        self.assertEqual(int((report.get("summary") or {}).get("xau_fss_resolved", 0) or 0), 1)
        fss_row = next((x for x in list(report.get("sources") or []) if str(x.get("source")) == "scalp_xauusd:fss:canary"), {})
        self.assertEqual(list(fss_row.get("external_prior_models") or []), ["DeepLOB", "EarnHFT", "BDLOB"])
        compare = dict((report.get("comparisons") or {}).get("xau_fss_vs_broad_microtrend_live") or {})
        self.assertEqual(str(compare.get("leader")), "xau_scalp_flow_short_sidecar")

    def test_build_strategy_lab_report_applies_chart_state_router_bonus_to_microtrend_follow_up(self):
        chart_state_memory_report = {
            "states": [
                {
                    "symbol": "XAUUSD",
                    "state_label": "continuation_drive",
                    "day_type": "trend",
                    "direction": "short",
                    "session": "london,new_york,overlap",
                    "timeframe": "5m+1m",
                    "confidence_band": "70-74.9",
                    "h1_trend": "bearish",
                    "state_score": 24.0,
                    "follow_up_candidate": True,
                    "follow_up_plan": "follow_with_shallow_retest_or_break_stop",
                    "stats": {"resolved": 4, "wins": 4, "losses": 0, "pnl_usd": 16.61},
                    "best_family": {"family": "xau_scalp_microtrend"},
                }
            ]
        }
        strategy_lab = self.engine.build_strategy_lab_report(
            winner_report={"symbols": []},
            crypto_report={"recommendations": []},
            canary_report={"symbols": []},
            chart_state_memory=chart_state_memory_report,
        )
        self.assertTrue(strategy_lab["ok"])
        row = next((x for x in list(strategy_lab.get("candidates") or []) if str(x.get("family")) == "xau_scalp_microtrend_follow_up"), {})
        self.assertGreater(float(row.get("chart_state_router_bonus", 0.0) or 0.0), 0.0)
        self.assertEqual(str(((row.get("chart_state_router") or {}).get("state_label") or "")), "continuation_drive")

    def test_build_ct_only_watch_report_detects_first_samples_and_pb_demotion(self):
        now = _iso_now()
        with sqlite3.connect(str(self.ctrader_db)) as conn:
            conn.execute(
                """
                INSERT INTO execution_journal(
                    created_utc, source, symbol, direction, confidence, entry, stop_loss, take_profit, entry_type, request_json, response_json, execution_meta_json, status
                ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                (now, "scalp_xauusd:td:canary", "XAUUSD", "long", 74.0, 2500.0, 2498.0, 2503.0, "limit", "{}", "{}", "{}", "accepted"),
            )
            conn.commit()
        (self.report_dir / "ct_only_experiment_report.json").write_text(
            json.dumps(
                {
                    "ok": True,
                    "sources": [
                        {"source": "scalp_xauusd:td:canary", "latest_exec_utc": now, "closed_total": {"resolved": 0}},
                        {"source": "scalp_xauusd:ff:canary", "latest_exec_utc": "", "closed_total": {"resolved": 0}},
                    ],
                },
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )
        (self.report_dir / "trading_manager_report.json").write_text(
            json.dumps(
                {
                    "ok": True,
                    "symbols": [
                        {
                            "symbol": "XAUUSD",
                            "family_routing_recommendations": {
                                "mode": "scheduled_dominant_demote_pb",
                                "support_mode": "calibration_fallback",
                                "reason": "pb weak vs scheduled strong",
                            },
                        }
                    ],
                    "family_routing_apply": {"status": "already_active", "reason": "pb weak vs scheduled strong"},
                },
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )
        (self.runtime_dir / "trading_manager_state.json").write_text(
            json.dumps(
                {
                    "xau_family_routing": {
                        "status": "active",
                        "mode": "scheduled_dominant_demote_pb",
                        "applied_at": now,
                        "reason": "pb weak vs scheduled strong",
                    }
                },
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )

        report = self.engine.build_ct_only_watch_report()

        self.assertTrue(report["ok"])
        self.assertTrue(bool(((report.get("summary") or {}).get("td_first_execution_detected"))))
        self.assertFalse(bool(((report.get("summary") or {}).get("ff_first_execution_detected"))))
        self.assertTrue(bool(((report.get("summary") or {}).get("pb_demotion_applied"))))
        td_exec = dict((report.get("milestones") or {}).get("td_first_execution") or {})
        self.assertEqual(td_exec.get("source"), "scalp_xauusd:td:canary")
        self.assertEqual(td_exec.get("event_utc"), now)
        pb_demote = dict((report.get("milestones") or {}).get("pb_demotion_applied") or {})
        self.assertEqual(pb_demote.get("mode"), "scheduled_dominant_demote_pb")
        self.assertEqual(pb_demote.get("support_mode"), "calibration_fallback")

    def test_auto_apply_live_profile_promotes_btc_weekday_from_experiment_report(self):
        with patch("learning.live_profile_autopilot.config.AUTO_APPLY_LIVE_PROFILE_ENABLED", True), \
             patch("learning.live_profile_autopilot.config.AUTO_APPLY_LIVE_PROFILE_PERSIST_ENV", True), \
             patch("learning.live_profile_autopilot.config.STRATEGY_PROMOTION_ENABLED", True), \
             patch("learning.live_profile_autopilot.config.BTC_WEEKDAY_LOB_NARROW_LIVE_RISK_USD", 1.1), \
             patch("learning.live_profile_autopilot.config.get_persistent_canary_strategy_families", return_value={"xau_scalp_pullback_limit"}), \
             patch("learning.live_profile_autopilot.config.get_persistent_canary_experimental_families", return_value={"xau_scalp_tick_depth_filter", "xau_scalp_failed_fade_follow_stop", "xau_scalp_microtrend_follow_up", "btc_weekday_lob_momentum", "eth_weekday_overlap_probe"}):
            out = self.engine.auto_apply_live_profile(
                winner_report={"recommendations": [], "symbols": []},
                crypto_report={"recommendations": [], "symbols": []},
                audit_report={"recommendations": []},
                canary_report={"recommendations": []},
                strategy_lab_report={"promotable_candidates": []},
                experiment_report={
                    "recommendations": [
                        {
                            "symbol": "BTCUSD",
                            "family": "btc_weekday_lob_momentum",
                            "action": "promote_btc_weekday_lob_narrow_live",
                            "resolved": 4,
                            "win_rate": 1.0,
                            "pnl_usd": 5.2,
                        }
                    ]
                },
            )
        self.assertTrue(out["ok"])
        env_text = self.env_local.read_text(encoding="utf-8")
        self.assertIn("PERSISTENT_CANARY_STRATEGY_FAMILIES=btc_weekday_lob_momentum,xau_scalp_pullback_limit", env_text)
        self.assertIn("PERSISTENT_CANARY_EXPERIMENTAL_FAMILIES=eth_weekday_overlap_probe,xau_scalp_failed_fade_follow_stop,xau_scalp_flow_short_sidecar,xau_scalp_microtrend_follow_up,xau_scalp_tick_depth_filter", env_text)


if __name__ == "__main__":
    unittest.main()
