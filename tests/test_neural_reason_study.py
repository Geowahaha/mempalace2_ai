import json
import sqlite3
import tempfile
import unittest
import gc
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import learning.neural_brain as neural_module
from learning.neural_brain import NeuralBrain


def _iso_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _make_brain(base: Path) -> NeuralBrain:
    brain = NeuralBrain()
    brain.db_path = base / "signal_learning.db"
    brain.model_path = base / "neural_brain.npz"
    brain._model_cache = None
    brain._reason_study_cache = None
    brain._reason_study_cache_key = None
    brain._reason_study_cache_ts = 0.0
    brain._init_db()
    return brain


def _insert_signal_event(
    db_path: Path,
    *,
    outcome: int,
    pnl: float,
    reason: str,
    exit_state: str,
    source: str = "scalp_xauusd",
    symbol: str = "XAUUSD",
    direction: str = "long",
) -> None:
    now_iso = _iso_now()
    extra = {
        "reasons": [reason],
        "warnings": [],
        "raw_scores": {
            "entry_type": "limit",
            "scalp_family": "xau_scalp_pullback_limit",
            "gate_reasons": ["volume_light"],
        },
        "close_resolution": {"state": exit_state},
    }
    with sqlite3.connect(str(db_path)) as conn:
        conn.execute(
            """
            INSERT INTO signal_events (
                created_at, source, signal_symbol, broker_symbol, direction,
                confidence, risk_reward, rsi, atr, timeframe, entry, stop_loss,
                take_profit_1, take_profit_2, take_profit_3, pattern, session,
                score_long, score_short, score_edge, mt5_status, mt5_message,
                ticket, position_id, resolved, outcome, pnl, closed_at, extra_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1, ?, ?, ?, ?)
            """,
            (
                now_iso,
                source,
                symbol,
                symbol,
                direction,
                74.0,
                2.0,
                55.0,
                1.5,
                "5m",
                100.0,
                99.0,
                101.0,
                102.0,
                103.0,
                "OB_BOUNCE",
                "london",
                65.0,
                20.0,
                45.0,
                "ctrader_closed",
                "ctrader_reconciled_close",
                111,
                111,
                int(outcome),
                float(pnl),
                now_iso,
                json.dumps(extra, ensure_ascii=True),
            ),
        )


class NeuralReasonStudyTests(unittest.TestCase):
    def test_reason_study_boosts_matching_signal_without_model(self):
        with tempfile.TemporaryDirectory() as td:
            base = Path(td)
            brain = None
            try:
                brain = _make_brain(base)
                for _ in range(6):
                    _insert_signal_event(
                        brain.db_path,
                        outcome=1,
                        pnl=1.4,
                        reason="Fresh sweep rejection",
                        exit_state="tp2",
                    )
                for _ in range(2):
                    _insert_signal_event(
                        brain.db_path,
                        outcome=0,
                        pnl=-1.0,
                        reason="Late chase entry",
                        exit_state="sl",
                    )

                signal = SimpleNamespace(
                    symbol="XAUUSD",
                    direction="long",
                    confidence=72.0,
                    pattern="OB_BOUNCE",
                    session="london",
                    timeframe="5m",
                    entry_type="limit",
                    reasons=["Fresh sweep rejection"],
                    warnings=[],
                    raw_scores={"scalp_family": "xau_scalp_pullback_limit", "gate_reasons": ["volume_light"]},
                )

                with patch.object(neural_module.config, "NEURAL_BRAIN_ENABLED", True), \
                     patch.object(neural_module.config, "NEURAL_BRAIN_SOFT_ADJUST", True), \
                     patch.object(neural_module.config, "NEURAL_BRAIN_REASON_STUDY_ENABLED", True), \
                     patch.object(neural_module.config, "NEURAL_BRAIN_REASON_STUDY_LOOKBACK_DAYS", 30), \
                     patch.object(neural_module.config, "NEURAL_BRAIN_REASON_STUDY_MIN_RESOLVED", 3), \
                     patch.object(neural_module.config, "NEURAL_BRAIN_REASON_STUDY_WEIGHT", 0.30), \
                     patch.object(neural_module.config, "NEURAL_BRAIN_REASON_STUDY_MAX_DELTA", 4.0), \
                     patch.object(neural_module.config, "NEURAL_BRAIN_REASON_STUDY_CACHE_SEC", 1), \
                     patch.object(neural_module.config, "NEURAL_BRAIN_SOFT_ADJUST_WEIGHT", 0.35), \
                     patch.object(neural_module.config, "NEURAL_BRAIN_SOFT_ADJUST_MAX_DELTA", 8.0):
                    report = brain.build_reason_study_report(days=30, min_resolved=3)
                    self.assertTrue(report["ok"])
                    self.assertTrue(any(str(row.get("tag", "")).startswith("reason:fresh_sweep_rejection") for row in report["top_positive_tags"]))

                    reason_adj = brain.reason_confidence_adjustment(signal, source="scalp_xauusd")
                    self.assertTrue(reason_adj["applied"])
                    self.assertGreater(float(reason_adj["delta"]), 0.0)

                    conf_adj = brain.confidence_adjustment(signal, source="scalp_xauusd")
                    self.assertTrue(conf_adj["applied"])
                    self.assertGreater(float(conf_adj["delta"]), 0.0)
                    self.assertIsNone(conf_adj.get("prob"))
                    self.assertIn("reason_study", conf_adj)
            finally:
                brain = None
                gc.collect()


if __name__ == "__main__":
    unittest.main()
