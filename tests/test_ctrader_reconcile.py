from __future__ import annotations

import asyncio
from types import SimpleNamespace
import time
import unittest

from trading_ai.core.execution import MarketSnapshot
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

    def test_get_market_data_returns_soft_stale_cache_and_schedules_refresh(self):
        broker = CTraderDexterWorkerBroker.__new__(CTraderDexterWorkerBroker)
        broker._account_id = 46945293
        broker._quote_cache = {
            "BTCUSD": MarketSnapshot(
                symbol="BTCUSD",
                bid=64000.0,
                ask=64001.0,
                mid=64000.5,
                spread=1.0,
                ts_unix=time.time() - 5.0,
                extra={"venue": "test"},
            )
        }
        broker._reference_quote_cache = {}
        broker._quote_refresh_tasks = {}
        broker._settings = SimpleNamespace(
            ctrader_quote_cache_ttl_sec=2.0,
            ctrader_quote_soft_stale_ttl_sec=20.0,
            ctrader_quote_background_refresh_enabled=True,
            ctrader_capture_duration_sec=3,
            ctrader_capture_max_events=18,
        )
        broker._quote_source = lambda: "auto"
        broker._allow_paper_fallback = lambda: False
        broker._reference_quote_snapshot = lambda *_args, **_kwargs: None

        scheduled = []
        broker._schedule_quote_refresh = lambda token, reason="": scheduled.append((token, reason))

        snap = asyncio.run(broker.get_market_data("BTCUSD"))

        self.assertEqual(snap.symbol, "BTCUSD")
        self.assertTrue(bool(snap.extra.get("soft_stale_cache")))
        self.assertGreater(float(snap.extra.get("soft_stale_age_sec") or 0.0), 0.0)
        self.assertEqual(len(scheduled), 1)
        self.assertEqual(scheduled[0][0], "BTCUSD")

    def test_get_market_data_uses_capture_after_soft_stale_window(self):
        broker = CTraderDexterWorkerBroker.__new__(CTraderDexterWorkerBroker)
        broker._account_id = 46945293
        broker._quote_cache = {
            "BTCUSD": MarketSnapshot(
                symbol="BTCUSD",
                bid=64000.0,
                ask=64001.0,
                mid=64000.5,
                spread=1.0,
                ts_unix=time.time() - 40.0,
                extra={"venue": "test"},
            )
        }
        broker._reference_quote_cache = {}
        broker._quote_refresh_tasks = {}
        broker._settings = SimpleNamespace(
            ctrader_quote_cache_ttl_sec=2.0,
            ctrader_quote_soft_stale_ttl_sec=15.0,
            ctrader_quote_background_refresh_enabled=True,
            ctrader_capture_duration_sec=3,
            ctrader_capture_max_events=12,
        )
        broker._quote_source = lambda: "auto"
        broker._allow_paper_fallback = lambda: False
        broker._reference_quote_snapshot = lambda *_args, **_kwargs: None
        broker._schedule_quote_refresh = lambda *_args, **_kwargs: None

        calls = []

        def _run_worker(mode, payload):
            calls.append((mode, payload))
            return {
                "ok": True,
                "status": "captured_live",
                "environment": "demo",
                "spots": [{"symbol": "BTCUSD", "bid": 65000.0, "ask": 65001.0, "event_ts": 1741565801.0}],
            }

        broker._run_worker = _run_worker

        snap = asyncio.run(broker.get_market_data("BTCUSD"))

        self.assertEqual(snap.mid, 65000.5)
        self.assertFalse(bool(snap.extra.get("soft_stale_cache")))
        self.assertEqual(len(calls), 1)
        self.assertEqual(calls[0][0], "capture_market")
        self.assertEqual(int(calls[0][1]["max_events"]), 12)

    def test_get_market_data_soft_stale_uses_loop_interval_floor(self):
        broker = CTraderDexterWorkerBroker.__new__(CTraderDexterWorkerBroker)
        broker._account_id = 46945293
        broker._quote_cache = {
            "BTCUSD": MarketSnapshot(
                symbol="BTCUSD",
                bid=64000.0,
                ask=64001.0,
                mid=64000.5,
                spread=1.0,
                ts_unix=time.time() - 14.0,
                extra={"venue": "test"},
            )
        }
        broker._reference_quote_cache = {}
        broker._quote_refresh_tasks = {}
        broker._settings = SimpleNamespace(
            ctrader_quote_cache_ttl_sec=2.0,
            ctrader_quote_soft_stale_ttl_sec=6.0,
            ctrader_quote_background_refresh_enabled=True,
            ctrader_capture_duration_sec=3,
            ctrader_capture_max_events=12,
            loop_interval_sec=20.0,
        )
        broker._quote_source = lambda: "auto"
        broker._allow_paper_fallback = lambda: False
        broker._reference_quote_snapshot = lambda *_args, **_kwargs: None

        scheduled = []
        broker._schedule_quote_refresh = lambda token, reason="": scheduled.append((token, reason))

        def _run_worker(_mode, _payload):
            raise AssertionError("capture_market should not run while soft-stale loop floor applies")

        broker._run_worker = _run_worker

        snap = asyncio.run(broker.get_market_data("BTCUSD"))

        self.assertEqual(snap.symbol, "BTCUSD")
        self.assertTrue(bool(snap.extra.get("soft_stale_cache")))
        self.assertEqual(len(scheduled), 1)

    def test_get_recent_closes_seeds_quote_cache_when_empty(self):
        broker = CTraderDexterWorkerBroker.__new__(CTraderDexterWorkerBroker)
        broker._account_id = 46945293
        broker._quote_cache = {}
        broker._settings = SimpleNamespace(
            ctrader_reference_quote_spread=0.12,
        )

        def _run_worker(mode, payload):
            self.assertEqual(mode, "get_trendbars")
            self.assertEqual(payload["symbol"], "BTCUSD")
            return {
                "ok": True,
                "status": "ok",
                "bars": [
                    {"close": 64001.25},
                    {"close": 64002.5},
                ],
            }

        broker._run_worker = _run_worker

        closes = asyncio.run(broker.get_recent_closes("BTCUSD", count=2, timeframe="1m"))

        self.assertEqual(closes, [64001.25, 64002.5])
        self.assertIn("BTCUSD", broker._quote_cache)
        seeded = broker._quote_cache["BTCUSD"]
        self.assertAlmostEqual(seeded.mid, 64002.5)
        self.assertEqual(seeded.extra.get("venue"), "ctrader_trendbar_seed")

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
