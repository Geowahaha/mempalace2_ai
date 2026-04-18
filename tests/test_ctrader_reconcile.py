from __future__ import annotations

import asyncio
from types import SimpleNamespace
import time
import unittest

from trading_ai.integrations.ctrader_dexter_worker import (
    CTraderDexterWorkerBroker,
    _compact_broker_comment,
    _worker_retry_attempts,
    _worker_retry_sleep_sec,
)
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

    def test_worker_retry_policy_keeps_capture_market_fast(self):
        self.assertEqual(_worker_retry_attempts("capture_market"), 2)
        self.assertEqual(_worker_retry_attempts("get_trendbars"), 3)
        self.assertEqual(_worker_retry_attempts("execute"), 1)
        self.assertLess(_worker_retry_sleep_sec("capture_market", 1), 1.0)

    def test_extract_latest_snapshot_uses_fetch_time_for_cache_ttl(self):
        broker = CTraderDexterWorkerBroker.__new__(CTraderDexterWorkerBroker)
        broker._quote_cache = {}
        payload = {
            "ok": True,
            "status": "captured_live",
            "environment": "demo",
            "spots": [
                {
                    "symbol": "BTCUSD",
                    "bid": 64000.0,
                    "ask": 64001.0,
                    "event_ts": 1741564801.0,
                }
            ],
        }
        before = time.time()
        snap = broker._extract_latest_snapshot("BTCUSD", payload)
        after = time.time()

        self.assertIsNotNone(snap)
        assert snap is not None
        self.assertGreaterEqual(snap.ts_unix, before - 0.5)
        self.assertLessEqual(snap.ts_unix, after + 0.5)
        self.assertEqual(snap.extra.get("source_event_ts"), 1741564801.0)

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
