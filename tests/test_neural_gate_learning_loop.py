import gc
import json
import sqlite3
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch

from learning.neural_gate_learning_loop import NeuralGateLearningLoop


def _iso(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _row(ts: datetime, *, p: float, decision: str, canary: bool, status: str, conf: float = 75.0) -> dict:
    return {
        "created_at": _iso(ts),
        "neural_prob": p,
        "confidence": conf,
        "force_mode": True,
        "decision": decision,
        "canary_applied": canary,
        "exec_status": status,
    }


class NeuralGateLearningLoopAutoStepTests(unittest.TestCase):
    def setUp(self):
        self.loop = NeuralGateLearningLoop()
        self.now = datetime.now(timezone.utc)

    def _rows_low_fill(self) -> list[dict]:
        rows: list[dict] = []
        # 18 eligible rows blocked by neural gate.
        for i in range(18):
            rows.append(
                _row(
                    self.now - timedelta(minutes=5 + i),
                    p=0.545,
                    decision="neural_block",
                    canary=False,
                    status="skipped",
                )
            )
        # 2 eligible rows passed via canary and filled.
        for i in range(2):
            rows.append(
                _row(
                    self.now - timedelta(minutes=2 + i),
                    p=0.55,
                    decision="allow",
                    canary=True,
                    status="filled",
                )
            )
        return rows

    def test_auto_step_down_on_low_fill_rate(self):
        rows = self._rows_low_fill()
        with patch.object(self.loop, "_previous_scope_config", return_value={}), \
             patch("learning.neural_gate_learning_loop.config.NEURAL_GATE_CANARY_AUTO_STEP_ENABLED", True), \
             patch("learning.neural_gate_learning_loop.config.NEURAL_GATE_CANARY_AUTO_STEP_LOOKBACK_HOURS", 6), \
             patch("learning.neural_gate_learning_loop.config.NEURAL_GATE_CANARY_AUTO_STEP_MIN_ELIGIBLE", 8), \
             patch("learning.neural_gate_learning_loop.config.NEURAL_GATE_CANARY_AUTO_STEP_TARGET_FILL_RATE", 0.20), \
             patch("learning.neural_gate_learning_loop.config.NEURAL_GATE_CANARY_AUTO_STEP_MIN_BLOCK_RATE", 0.40), \
             patch("learning.neural_gate_learning_loop.config.NEURAL_GATE_CANARY_AUTO_STEP_DOWN_STEP", 0.01), \
             patch("learning.neural_gate_learning_loop.config.NEURAL_GATE_CANARY_AUTO_STEP_MIN_ALLOW_LOW", 0.53), \
             patch("learning.neural_gate_learning_loop.config.NEURAL_GATE_CANARY_AUTO_STEP_VOLUME_CAP_STEP", 0.02), \
             patch("learning.neural_gate_learning_loop.config.NEURAL_GATE_CANARY_AUTO_STEP_MIN_VOLUME_CAP", 0.16), \
             patch("learning.neural_gate_learning_loop.config.NEURAL_GATE_CANARY_AUTO_STEP_COOLDOWN_MIN", 30), \
             patch("learning.neural_gate_learning_loop.config.NEURAL_GATE_CANARY_AUTO_STEP_REQUIRE_ACTIVE_SCOPE", True), \
             patch("learning.neural_gate_learning_loop.config.NEURAL_GATE_CANARY_AUTO_STEP_STOP_ON_TRIPWIRE", True):
            allow_after, cap_after, info = self.loop._auto_step_canary_scope(
                source="scalp_xauusd",
                symbol="XAUUSD",
                rows=rows,
                base_min=0.58,
                allow_low=0.54,
                low_floor=0.53,
                min_conf=70.0,
                require_force=True,
                volume_cap=0.20,
                scope_active=True,
                tripwire_triggered=False,
            )

        self.assertAlmostEqual(allow_after, 0.53, places=4)
        self.assertAlmostEqual(cap_after, 0.18, places=4)
        self.assertTrue(bool(info.get("applied", False)))
        self.assertEqual(str(info.get("action", "")), "step_down")
        self.assertEqual(str(info.get("reason", "")), "low_fill_rate")
        self.assertEqual((info.get("execution_window") or {}).get("eligible"), 20)
        self.assertAlmostEqual(float((info.get("execution_window") or {}).get("fill_rate") or 0.0), 0.10, places=4)

    def test_auto_step_hold_during_cooldown(self):
        rows = self._rows_low_fill()
        prev = {
            "allow_low": 0.53,
            "volume_multiplier_cap": 0.18,
            "auto_step": {"last_step_at": _iso(self.now - timedelta(minutes=10)), "steps_applied": 1},
        }
        with patch.object(self.loop, "_previous_scope_config", return_value=prev), \
             patch("learning.neural_gate_learning_loop.config.NEURAL_GATE_CANARY_AUTO_STEP_ENABLED", True), \
             patch("learning.neural_gate_learning_loop.config.NEURAL_GATE_CANARY_AUTO_STEP_LOOKBACK_HOURS", 6), \
             patch("learning.neural_gate_learning_loop.config.NEURAL_GATE_CANARY_AUTO_STEP_MIN_ELIGIBLE", 8), \
             patch("learning.neural_gate_learning_loop.config.NEURAL_GATE_CANARY_AUTO_STEP_TARGET_FILL_RATE", 0.20), \
             patch("learning.neural_gate_learning_loop.config.NEURAL_GATE_CANARY_AUTO_STEP_MIN_BLOCK_RATE", 0.40), \
             patch("learning.neural_gate_learning_loop.config.NEURAL_GATE_CANARY_AUTO_STEP_DOWN_STEP", 0.01), \
             patch("learning.neural_gate_learning_loop.config.NEURAL_GATE_CANARY_AUTO_STEP_MIN_ALLOW_LOW", 0.53), \
             patch("learning.neural_gate_learning_loop.config.NEURAL_GATE_CANARY_AUTO_STEP_VOLUME_CAP_STEP", 0.02), \
             patch("learning.neural_gate_learning_loop.config.NEURAL_GATE_CANARY_AUTO_STEP_MIN_VOLUME_CAP", 0.16), \
             patch("learning.neural_gate_learning_loop.config.NEURAL_GATE_CANARY_AUTO_STEP_COOLDOWN_MIN", 30), \
             patch("learning.neural_gate_learning_loop.config.NEURAL_GATE_CANARY_AUTO_STEP_REQUIRE_ACTIVE_SCOPE", True), \
             patch("learning.neural_gate_learning_loop.config.NEURAL_GATE_CANARY_AUTO_STEP_STOP_ON_TRIPWIRE", True):
            allow_after, cap_after, info = self.loop._auto_step_canary_scope(
                source="scalp_xauusd",
                symbol="XAUUSD",
                rows=rows,
                base_min=0.58,
                allow_low=0.54,
                low_floor=0.53,
                min_conf=70.0,
                require_force=True,
                volume_cap=0.20,
                scope_active=True,
                tripwire_triggered=False,
            )

        self.assertAlmostEqual(allow_after, 0.53, places=4)
        self.assertAlmostEqual(cap_after, 0.18, places=4)
        self.assertFalse(bool(info.get("applied", False)))
        self.assertEqual(str(info.get("reason", "")), "cooldown")
        self.assertFalse(bool(info.get("cooldown_ok", True)))


class NeuralGateExecColumnMigrationTests(unittest.TestCase):
    def test_renames_mt5_columns_to_exec_on_init(self):
        with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmp:
            ng = Path(tmp) / "neural_gate.db"
            with sqlite3.connect(str(ng)) as conn:
                conn.execute(
                    """
                    CREATE TABLE neural_gate_decisions (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        journal_id INTEGER UNIQUE NOT NULL,
                        created_at TEXT NOT NULL,
                        account_key TEXT,
                        source TEXT,
                        signal_symbol TEXT,
                        broker_symbol TEXT,
                        direction TEXT,
                        confidence REAL,
                        neural_prob REAL,
                        min_prob REAL,
                        min_prob_reason TEXT,
                        mt5_status TEXT,
                        mt5_message TEXT,
                        decision TEXT,
                        decision_reason TEXT,
                        entry REAL,
                        stop_loss REAL,
                        take_profit_2 REAL,
                        force_mode INTEGER,
                        m1_not_confirmed INTEGER,
                        canary_applied INTEGER,
                        features_json TEXT,
                        resolved INTEGER NOT NULL DEFAULT 0,
                        outcome_type TEXT,
                        outcome INTEGER,
                        outcome_label TEXT,
                        pnl_usd REAL,
                        outcome_ref TEXT,
                        weight REAL,
                        resolved_at TEXT
                    )
                    """
                )
                conn.commit()
            with patch("learning.neural_gate_learning_loop.config.NEURAL_GATE_LEARNING_DB_PATH", str(ng)), \
                 patch("learning.neural_gate_learning_loop.config.CTRADER_ENABLED", False):
                mig_loop = NeuralGateLearningLoop()
            del mig_loop
            gc.collect()
            with sqlite3.connect(str(ng)) as conn:
                cols = {r[1] for r in conn.execute("PRAGMA table_info(neural_gate_decisions)").fetchall()}
            self.assertIn("exec_status", cols)
            self.assertIn("exec_message", cols)
            self.assertNotIn("mt5_status", cols)
            self.assertNotIn("mt5_message", cols)


class NeuralGateCtraderJournalTests(unittest.TestCase):
    def test_sync_decisions_from_ctrader_and_resolve_closed(self):
        off = 1_000_000_000
        with tempfile.TemporaryDirectory() as tmp:
            ct = Path(tmp) / "ctrader.db"
            ng = Path(tmp) / "neural_gate.db"
            ts = datetime.now(timezone.utc).timestamp()
            req = json.dumps(
                {"raw_scores": {"neural_probability": 0.561, "mt5_neural_canary_applied": True}},
                separators=(",", ":"),
            )
            meta = json.dumps({"closed": {"pnl_usd": 2.25}}, separators=(",", ":"))
            conn = sqlite3.connect(str(ct))
            conn.execute(
                """
                CREATE TABLE execution_journal (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    created_ts REAL NOT NULL,
                    created_utc TEXT NOT NULL,
                    source TEXT, lane TEXT, symbol TEXT, direction TEXT, confidence REAL,
                    entry REAL, stop_loss REAL, take_profit REAL, entry_type TEXT,
                    dry_run INTEGER, account_id INTEGER, broker_symbol TEXT, volume REAL,
                    status TEXT, message TEXT, order_id INTEGER, position_id INTEGER, deal_id INTEGER,
                    signal_run_id TEXT, signal_run_no INTEGER, request_json TEXT, response_json TEXT, execution_meta_json TEXT
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE ctrader_deals (
                    deal_id INTEGER PRIMARY KEY,
                    journal_id INTEGER, pnl_usd REAL, has_close_detail INTEGER DEFAULT 0
                )
                """
            )
            conn.execute(
                """
                INSERT INTO execution_journal(
                    created_ts, created_utc, source, lane, symbol, direction, confidence,
                    entry, stop_loss, take_profit, entry_type, dry_run, account_id, broker_symbol, volume,
                    status, message, order_id, position_id, deal_id, signal_run_id, signal_run_no,
                    request_json, response_json, execution_meta_json
                ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    ts,
                    "2026-01-15T12:00:00Z",
                    "scalp_xauusd",
                    "",
                    "XAUUSD",
                    "long",
                    74.0,
                    2000.0,
                    1990.0,
                    2010.0,
                    "limit",
                    0,
                    99001,
                    "XAUUSD",
                    0.01,
                    "closed",
                    "ctrader closed win pnl=+2.25$",
                    0,
                    0,
                    0,
                    "",
                    0,
                    req,
                    "{}",
                    meta,
                ),
            )
            conn.commit()
            conn.close()

            with patch("learning.neural_gate_learning_loop.config.NEURAL_GATE_LEARNING_DB_PATH", str(ng)), \
                 patch("learning.neural_gate_learning_loop.config.CTRADER_DB_PATH", str(ct)), \
                 patch("learning.neural_gate_learning_loop.config.CTRADER_ENABLED", True), \
                 patch("learning.neural_gate_learning_loop.config.NEURAL_GATE_CTRADER_SYNC_ENABLED", True), \
                 patch("learning.neural_gate_learning_loop.config.NEURAL_GATE_JOURNAL_ID_CTRADER_OFFSET", off), \
                 patch("learning.neural_gate_learning_loop.config.NEURAL_GATE_LEARNING_ENABLED", True):
                loop = NeuralGateLearningLoop()
                sync = loop.sync_decisions_from_ctrader_journal(lookback_hours=48)
                self.assertTrue(sync.get("ok"))
                self.assertGreaterEqual(int(sync.get("inserted", 0)) + int(sync.get("updated", 0)), 1)
                res = loop.resolve_outcomes()
                self.assertTrue(res.get("ok"))
                self.assertGreaterEqual(int(res.get("resolved_real", 0)), 1)


if __name__ == "__main__":
    unittest.main()
