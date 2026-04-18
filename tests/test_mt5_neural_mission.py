import json
import sqlite3
import tempfile
import unittest
import gc
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from api.signal_store import SignalStore
import learning.mt5_neural_mission as mission_module
from learning.mt5_neural_mission import MT5NeuralMission


def _iso_now() -> str:
    from datetime import datetime, timezone

    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _init_learning_db(path: Path) -> None:
    with sqlite3.connect(str(path)) as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS signal_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                created_at TEXT,
                closed_at TEXT,
                source TEXT,
                signal_symbol TEXT,
                broker_symbol TEXT,
                resolved INTEGER DEFAULT 0,
                outcome INTEGER,
                pnl REAL,
                extra_json TEXT
            )
            """
        )


def _insert_learning_row(path: Path, symbol: str, outcome: int, pnl: float, prob: float, source: str = "xauusd") -> None:
    payload = {"raw_scores": {"neural_probability": float(prob)}}
    now_iso = _iso_now()
    with sqlite3.connect(str(path)) as conn:
        conn.execute(
            """
            INSERT INTO signal_events (
                created_at, closed_at, source, signal_symbol, broker_symbol,
                resolved, outcome, pnl, extra_json
            ) VALUES (?, ?, ?, ?, ?, 1, ?, ?, ?)
            """,
            (now_iso, now_iso, str(source or ""), symbol, symbol, int(outcome), float(pnl), json.dumps(payload)),
        )


def _store_signal_result(store: SignalStore, symbol: str, pnl_usd: float) -> None:
    sig = SimpleNamespace(
        symbol=symbol,
        direction="long",
        confidence=80.0,
        entry=100.0,
        stop_loss=99.0,
        take_profit_1=101.0,
        take_profit_2=102.0,
        take_profit_3=103.0,
        risk_reward=2.0,
        timeframe="1h",
        session="new_york",
        pattern="TEST",
        entry_type="market",
        sl_type="atr",
        tp_type="rr",
        sl_liquidity_mapped=False,
        liquidity_pools_count=0,
    )
    sid = int(store.store_signal(sig, source="mission_test"))
    outcome = "tp2_hit" if float(pnl_usd) > 0 else "sl_hit"
    store.update_outcome(
        sid,
        outcome=outcome,
        exit_price=100.0 + float(pnl_usd),
        pnl_pips=float(pnl_usd) * 10.0,
        pnl_usd=float(pnl_usd),
    )


class _StubNeural:
    def sync_outcomes_from_mt5(self, days: int = 90):
        return {"ok": True, "updated": 0, "closed_positions": 0, "days": days}

    def sync_signal_outcomes_from_market(self, days: int = 90, max_records: int = 200):
        return {"ok": True, "resolved": 0, "reviewed": 0, "pseudo_labeled": 0, "days": days, "max_records": max_records}

    def model_status(self):
        return {"available": True, "samples": 100, "val_accuracy": 0.6}

    def train_backprop(self, days: int = 120, min_samples: int = 30):
        return SimpleNamespace(
            ok=True,
            status="ok",
            message="training complete",
            samples=120,
            train_accuracy=0.66,
            val_accuracy=0.58,
            win_rate=0.56,
        )


class _StubSymbolBrain:
    def train_symbol(self, symbol: str, days: int = 120):
        return SimpleNamespace(
            ok=True,
            symbol_key=symbol,
            status="ok",
            message="training complete",
            samples=40,
            train_accuracy=0.69,
            val_accuracy=0.57,
            win_rate=0.55,
            feature_set="mixed",
        )


class MT5NeuralMissionTests(unittest.TestCase):
    def setUp(self):
        self._td = tempfile.TemporaryDirectory()
        self.base = Path(self._td.name)
        self.learning_db = self.base / "signal_learning.db"
        self.signal_db = self.base / "signal_history.db"
        self.report_dir = self.base / "reports"
        self.env_local = self.base / ".env.local"
        self.report_dir.mkdir(parents=True, exist_ok=True)
        self.env_local.write_text("MT5_ALLOW_SYMBOLS=XAUUSD,ETHUSD,BTCUSD,GBPUSD\n", encoding="utf-8")
        _init_learning_db(self.learning_db)
        self.store = SignalStore(db_path=str(self.signal_db))
        self.engine = MT5NeuralMission(
            signal_learning_db=str(self.learning_db),
            report_dir=str(self.report_dir),
            env_local_path=str(self.env_local),
            neural_engine=_StubNeural(),
            symbol_engine=_StubSymbolBrain(),
            signal_store_obj=self.store,
        )

    def tearDown(self):
        self.engine = None
        self.store = None
        gc.collect()
        self._td.cleanup()

    def test_recommends_higher_threshold_when_low_prob_losses_dominate(self):
        symbol = "XAUUSD"
        with patch.object(
            mission_module.config,
            "get_neural_min_prob_symbol_overrides",
            return_value={},
        ):
            base_prob = float(self.engine._base_min_prob_for_symbol(symbol))
            rows = [
                (0, -1.2, 0.40),
                (0, -1.0, 0.45),
                (0, -1.1, 0.55),
                (1, 1.0, 0.62),
                (1, 1.3, 0.68),
                (1, 1.5, 0.74),
            ]
            for outcome, pnl, prob in rows:
                _insert_learning_row(self.learning_db, symbol, outcome, pnl, prob)
                _store_signal_result(self.store, symbol, pnl)

            rpt = self.engine.run(
                symbols=[symbol],
                iterations=1,
                train_days=60,
                backtest_days=60,
                sync_days=60,
                target_win_rate=55.0,
                target_profit_factor=1.1,
                min_trades=3,
                apply_policy_draft=False,
            )

            self.assertTrue(rpt["ok"])
            rec = dict(rpt["final"]["recommendations"][symbol])
            self.assertGreaterEqual(float(rec["neural_min_prob"]), base_prob + 0.01)
            env_line = str(rpt["final"]["override_bundle"]["env_lines"]["NEURAL_BRAIN_MIN_PROB_SYMBOL_OVERRIDES"])
            self.assertIn("XAUUSD=", env_line)

    def test_load_learning_rows_excludes_experimental_lanes_by_default(self):
        symbol = "XAUUSD"
        _insert_learning_row(self.learning_db, symbol, 1, 1.0, 0.62, source="xauusd")
        _insert_learning_row(self.learning_db, symbol, 0, -1.0, 0.59, source="xauusd:winner")
        _insert_learning_row(self.learning_db, symbol, 1, 0.8, 0.61, source="scalp_xauusd:bypass")
        with patch.object(mission_module.config, "NEURAL_MISSION_INCLUDE_EXPERIMENTAL_LANES", False):
            rows = self.engine._load_learning_rows(symbol, days=30)
        self.assertEqual(len(rows), 1)
        self.assertEqual(str(rows[0].get("source")), "xauusd")

    def test_stops_early_when_all_target_symbols_pass(self):
        symbols = ["XAUUSD", "ETHUSD", "BTCUSD", "GBPUSD"]
        for sym in symbols:
            samples = [
                (1, 1.2, 0.62),
                (1, 1.1, 0.65),
                (1, 1.0, 0.67),
                (1, 1.4, 0.70),
                (1, 1.3, 0.73),
                (0, -0.5, 0.58),
            ]
            for outcome, pnl, prob in samples:
                _insert_learning_row(self.learning_db, sym, outcome, pnl, prob)
                _store_signal_result(self.store, sym, pnl)

        rpt = self.engine.run(
            symbols="XAUUSD,ETHUSD,BTCUSD,GBPUSD",
            iterations=3,
            train_days=90,
            backtest_days=90,
            sync_days=90,
            target_win_rate=55.0,
            target_profit_factor=1.1,
            min_trades=5,
            apply_policy_draft=False,
        )

        self.assertTrue(rpt["ok"])
        self.assertTrue(rpt["goal_met"])
        self.assertEqual(int(rpt["iterations_done"]), 1)
        self.assertTrue(Path(str(rpt["report_path"])).exists())

    def test_default_mission_notify_sends_only_final_report_once(self):
        symbol = "XAUUSD"
        samples = [
            (1, 1.0, 0.62),
            (0, -0.5, 0.48),
            (1, 1.2, 0.66),
        ]
        for outcome, pnl, prob in samples:
            _insert_learning_row(self.learning_db, symbol, outcome, pnl, prob)
            _store_signal_result(self.store, symbol, pnl)

        with patch.object(mission_module.config, "NEURAL_MISSION_NOTIFY_EACH_ITERATION", False), \
             patch.object(self.engine, "_notify_telegram") as notify_call:
            rpt = self.engine.run(
                symbols=[symbol],
                iterations=2,
                train_days=30,
                backtest_days=30,
                sync_days=30,
                target_win_rate=50.0,
                target_profit_factor=1.0,
                min_trades=3,
                apply_policy_draft=False,
            )

        self.assertTrue(rpt["ok"])
        self.assertEqual(notify_call.call_count, 1)

    def test_auto_add_allowlist_from_backtest_updates_env(self):
        symbol = "EURUSD"
        rows = [
            (1, 1.2, 0.62),
            (1, 1.1, 0.64),
            (1, 1.4, 0.66),
            (1, 1.0, 0.68),
            (0, -0.4, 0.60),
            (1, 1.3, 0.72),
        ]
        for outcome, pnl, prob in rows:
            _insert_learning_row(self.learning_db, symbol, outcome, pnl, prob)
            _store_signal_result(self.store, symbol, pnl)

        with patch.object(mission_module.config, "MT5_ALLOW_SYMBOLS", "XAUUSD,ETHUSD,BTCUSD,GBPUSD"), \
             patch.object(mission_module.config, "NEURAL_MISSION_AUTO_ALLOWLIST_ENABLED", True), \
             patch.object(mission_module.config, "NEURAL_MISSION_AUTO_ALLOWLIST_MIN_TRADES", 5), \
             patch.object(mission_module.config, "NEURAL_MISSION_AUTO_ALLOWLIST_MIN_WIN_RATE", 58.0), \
             patch.object(mission_module.config, "NEURAL_MISSION_AUTO_ALLOWLIST_MIN_PROFIT_FACTOR", 1.2), \
             patch.object(mission_module.config, "NEURAL_MISSION_AUTO_ALLOWLIST_MIN_NET_PNL", 0.0), \
             patch.object(mission_module.config, "NEURAL_MISSION_AUTO_ALLOWLIST_MAX_ADD_PER_CYCLE", 3), \
             patch.object(mission_module.config, "NEURAL_MISSION_AUTO_ALLOWLIST_PERSIST_ENV", True), \
             patch.object(self.engine, "_resolve_broker_symbol", return_value="EURUSD"), \
             patch.object(self.engine, "_apply_runtime_allow_symbols", return_value={"ok": True, "allow_count": 5}):
            rpt = self.engine.run(
                symbols=[symbol],
                iterations=1,
                train_days=30,
                backtest_days=30,
                sync_days=30,
                target_win_rate=55.0,
                target_profit_factor=1.1,
                min_trades=3,
                apply_policy_draft=False,
            )

        self.assertTrue(rpt["ok"])
        auto = dict(rpt["final"]["auto_allowlist"])
        self.assertEqual(str(auto.get("status")), "added")
        self.assertIn("EURUSD", list(auto.get("added_symbols", []) or []))
        env_line = str(rpt["final"]["override_bundle"]["env_lines"].get("MT5_ALLOW_SYMBOLS", ""))
        self.assertIn("EURUSD", env_line)
        env_text = self.env_local.read_text(encoding="utf-8")
        self.assertIn("MT5_ALLOW_SYMBOLS=", env_text)
        self.assertIn("EURUSD", env_text)

    def test_upsert_env_key_creates_backup_before_overwrite(self):
        env_path = self.base / ".env.backup.test"
        env_path.write_text("MT5_ALLOW_SYMBOLS=XAUUSD,BTCUSD\n", encoding="utf-8")
        with patch.object(mission_module.config, "NEURAL_MISSION_ENV_BACKUP_KEEP", 20):
            out = self.engine._upsert_env_key(env_path, "MT5_ALLOW_SYMBOLS", "XAUUSD,BTCUSD,ETHUSD")
        self.assertTrue(out.get("ok"))
        self.assertTrue(out.get("updated"))
        backup = str(out.get("backup_path", "") or "")
        self.assertTrue(bool(backup))
        self.assertTrue(Path(backup).exists())
        backup_text = Path(backup).read_text(encoding="utf-8")
        self.assertIn("MT5_ALLOW_SYMBOLS=XAUUSD,BTCUSD", backup_text)


if __name__ == "__main__":
    unittest.main()
