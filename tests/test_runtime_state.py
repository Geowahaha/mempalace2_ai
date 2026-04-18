from __future__ import annotations

import tempfile
import unittest
import json
from pathlib import Path

from trading_ai.core.execution import OpenPosition
from trading_ai.main import (
    _resolve_startup_positions,
    _seed_price_history_from_monitor,
    _should_store_execution_failure_note,
    _trade_failure_detail,
)
from trading_ai.core.runtime_state import (
    load_runtime_positions_state,
    load_shadow_runtime_positions_state,
    save_runtime_state,
)
from trading_ai.core.strategy import RiskManager


class RuntimeStateTests(unittest.TestCase):
    def test_save_and_load_shadow_runtime_state(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "runtime_state.json"
            risk = RiskManager(
                max_trades_per_session=5,
                max_consecutive_losses=3,
                neutral_rel_threshold=0.001,
            )
            real_position = OpenPosition(
                order_id="real_1",
                symbol="XAUUSD",
                side="BUY",
                volume=0.01,
                entry_price=3200.0,
                position_id="p_real_1",
                opened_ts=123.0,
            )
            shadow_position = OpenPosition(
                order_id="shadow_1",
                symbol="XAUUSD",
                side="SELL",
                volume=0.0025,
                entry_price=3198.0,
                position_id="p_shadow_1",
                opened_ts=456.0,
            )

            save_runtime_state(
                path,
                open_position=real_position,
                open_context={"strategy_key": "UP*HIGH*NY_trend_follow"},
                open_positions=[real_position],
                open_contexts=[{"strategy_key": "UP*HIGH*NY_trend_follow"}],
                shadow_open_position=shadow_position,
                shadow_open_context={"strategy_key": "DOWN*HIGH*ASIA_trend_follow", "shadow": True},
                shadow_open_positions=[shadow_position],
                shadow_open_contexts=[{"strategy_key": "DOWN*HIGH*ASIA_trend_follow", "shadow": True}],
                risk=risk,
            )

            positions, contexts, risk_state = load_runtime_positions_state(path)
            shadow_positions, shadow_contexts = load_shadow_runtime_positions_state(path)

            self.assertEqual(len(positions), 1)
            self.assertEqual(positions[0].position_id, "p_real_1")
            self.assertEqual(contexts[0]["strategy_key"], "UP*HIGH*NY_trend_follow")
            self.assertIn("trades_executed", risk_state)

            self.assertEqual(len(shadow_positions), 1)
            self.assertEqual(shadow_positions[0].position_id, "p_shadow_1")
            self.assertEqual(shadow_positions[0].side, "SELL")
            self.assertTrue(shadow_contexts[0]["shadow"])

    def test_startup_resolution_drops_stale_runtime_positions_when_broker_empty(self) -> None:
        restored = [
            OpenPosition(
                order_id="real_1",
                symbol="XAUUSD",
                side="BUY",
                volume=0.01,
                entry_price=3200.0,
                position_id="p_real_1",
                opened_ts=123.0,
            )
        ]
        positions, contexts, source = _resolve_startup_positions(
            restored_positions=restored,
            restored_contexts=[{"strategy_key": "UP*HIGH*NY_trend_follow"}],
            broker_positions=[],
            broker_reconcile_ok=True,
        )
        self.assertEqual(positions, [])
        self.assertEqual(contexts, [])
        self.assertEqual(source, "broker_empty")

    def test_startup_resolution_prefers_broker_positions(self) -> None:
        restored = [
            OpenPosition(
                order_id="real_1",
                symbol="XAUUSD",
                side="BUY",
                volume=0.01,
                entry_price=3200.0,
                position_id="p_real_1",
                opened_ts=123.0,
            )
        ]
        broker = [
            OpenPosition(
                order_id="real_2",
                symbol="XAUUSD",
                side="SELL",
                volume=0.01,
                entry_price=3195.0,
                position_id="p_real_2",
                opened_ts=456.0,
            )
        ]
        positions, contexts, source = _resolve_startup_positions(
            restored_positions=restored,
            restored_contexts=[{"strategy_key": "UP*HIGH*NY_trend_follow"}],
            broker_positions=broker,
            broker_reconcile_ok=True,
        )
        self.assertEqual(positions, broker)
        self.assertEqual(contexts, [])
        self.assertEqual(source, "broker")

    def test_seed_price_history_from_monitor_uses_recent_symbol_mids(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "monitor.ndjson"
            rows = [
                {"market": {"symbol": "EURUSD", "mid": 1.1}},
                {"market": {"symbol": "XAUUSD", "mid": 3200.0}},
                {"market": {"symbol": "XAUUSD", "mid": 3201.5}},
                {"market": {"symbol": "XAUUSD", "mid": 3203.25}},
            ]
            path.write_text("\n".join(json.dumps(row) for row in rows) + "\n", encoding="utf-8")
            mids = _seed_price_history_from_monitor(path, symbol="XAUUSD", limit=2)
            self.assertEqual(mids, [3201.5, 3203.25])

    def test_seed_price_history_from_monitor_skips_stale_rows(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "monitor.ndjson"
            rows = [
                {"market": {"symbol": "XAUUSD", "mid": 3200.0, "ts_unix": 100.0}},
                {"market": {"symbol": "XAUUSD", "mid": 3201.5, "ts_unix": 110.0}},
            ]
            path.write_text("\n".join(json.dumps(row) for row in rows) + "\n", encoding="utf-8")
            mids = _seed_price_history_from_monitor(
                path,
                symbol="XAUUSD",
                limit=5,
                max_age_sec=30.0,
                now_ts=200.0,
            )
            self.assertEqual(mids, [])

    def test_seed_price_history_from_monitor_keeps_recent_rows_with_age_filter(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "monitor.ndjson"
            rows = [
                {"market": {"symbol": "XAUUSD", "mid": 3190.0, "ts_unix": 120.0}},
                {"market": {"symbol": "XAUUSD", "mid": 3200.0, "ts_unix": 190.0}},
                {"market": {"symbol": "XAUUSD", "mid": 3201.5, "ts_unix": 195.0}},
            ]
            path.write_text("\n".join(json.dumps(row) for row in rows) + "\n", encoding="utf-8")
            mids = _seed_price_history_from_monitor(
                path,
                symbol="XAUUSD",
                limit=5,
                max_age_sec=20.0,
                now_ts=200.0,
            )
            self.assertEqual(mids, [3200.0, 3201.5])

    def test_market_closed_execution_not_stored_as_failure_note(self) -> None:
        raw = {
            "message": "Trading is not available: Market is closed.",
            "execution_meta": {"error_code": "MARKET_CLOSED"},
        }
        self.assertFalse(_should_store_execution_failure_note("rejected", raw))
        self.assertIn("MARKET_CLOSED", _trade_failure_detail("rejected", raw))

    def test_real_execution_rejection_still_stored(self) -> None:
        raw = {
            "message": "Order rejected because free margin is insufficient.",
            "execution_meta": {"error_code": "NOT_ENOUGH_MONEY"},
        }
        self.assertTrue(_should_store_execution_failure_note("rejected", raw))
        self.assertIn("NOT_ENOUGH_MONEY", _trade_failure_detail("rejected", raw))

    def test_skip_same_side_open_not_stored_as_failure_note(self) -> None:
        self.assertFalse(_should_store_execution_failure_note("skip_same_side_open", None))

    def test_comment_too_long_invalid_request_not_stored_as_failure_note(self) -> None:
        raw = {
            "message": "Field comment is too long",
            "execution_meta": {"error_code": "INVALID_REQUEST"},
        }
        self.assertFalse(_should_store_execution_failure_note("rejected", raw))


if __name__ == "__main__":
    unittest.main()
