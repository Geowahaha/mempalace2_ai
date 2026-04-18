from __future__ import annotations

import asyncio
from types import SimpleNamespace
import unittest

from trading_ai.integrations.ctrader_dexter_worker import _compact_broker_comment
from trading_ai.main import _reconcile_open_positions_from_broker


class _StubBroker:
    def __init__(self, result):
        self._result = result

    def _run_worker(self, mode, payload):
        assert mode == "reconcile"
        assert payload["account_id"] == 46945293
        return self._result


class ReconcileOpenPositionsTests(unittest.TestCase):
    def test_compact_broker_comment_keeps_short_safe_tag(self):
        text = "entry_override:opp=0.701:risk=0.484:edge=0.216|The market structure and trend direction are clearly downward"
        self.assertEqual(_compact_broker_comment(text), "entry_override")

    def test_converts_ctrader_raw_volume_back_to_lots(self):
        broker = _StubBroker(
            {
                "ok": True,
                "positions": [
                    {
                        "position_id": 604839402,
                        "symbol": "XAUUSD",
                        "direction": "long",
                        "volume": 100,
                        "entry_price": 4762.76,
                        "open_timestamp_ms": 1775839229868,
                    }
                ],
            }
        )
        settings = SimpleNamespace(
            ctrader_account_id="46945293",
            symbol="XAUUSD",
            ctrader_worker_volume_scale=100,
        )

        positions, ok = asyncio.run(_reconcile_open_positions_from_broker(broker, settings))

        self.assertTrue(ok)
        self.assertEqual(len(positions), 1)
        self.assertEqual(positions[0].symbol, "XAUUSD")
        self.assertEqual(positions[0].side, "BUY")
        self.assertEqual(positions[0].position_id, "604839402")
        self.assertEqual(positions[0].volume, 0.01)

    def test_skips_other_symbols(self):
        broker = _StubBroker(
            {
                "ok": True,
                "positions": [
                    {
                        "position_id": 1,
                        "symbol": "BTCUSD",
                        "direction": "long",
                        "volume": 100,
                        "entry_price": 1.0,
                        "open_timestamp_ms": 1,
                    }
                ],
            }
        )
        settings = SimpleNamespace(
            ctrader_account_id="46945293",
            symbol="XAUUSD",
            ctrader_worker_volume_scale=100,
        )

        positions, ok = asyncio.run(_reconcile_open_positions_from_broker(broker, settings))

        self.assertTrue(ok)
        self.assertEqual(positions, [])


if __name__ == "__main__":
    unittest.main()
