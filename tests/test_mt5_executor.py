import unittest
import tempfile
from unittest.mock import patch, MagicMock
from types import SimpleNamespace

from analysis.signals import TradeSignal
from execution.mt5_executor import MT5Executor
from learning.mt5_adaptive_trade_planner import AdaptiveExecutionPlan


def make_signal(symbol: str = "XAUUSD", confidence: float = 80.0, direction: str = "long") -> TradeSignal:
    return TradeSignal(
        symbol=symbol,
        direction=direction,
        confidence=confidence,
        entry=100.0,
        stop_loss=99.0 if direction == "long" else 101.0,
        take_profit_1=101.0 if direction == "long" else 99.0,
        take_profit_2=102.0 if direction == "long" else 98.0,
        take_profit_3=103.0 if direction == "long" else 97.0,
        risk_reward=2.0,
        timeframe="1h",
        session="new_york",
        trend="bullish",
        rsi=55.0,
        atr=1.0,
        pattern="TEST",
        reasons=[],
        warnings=[],
        raw_scores={"edge": 20},
    )


class MT5ExecutorTests(unittest.TestCase):
    def setUp(self):
        self._td = tempfile.TemporaryDirectory()
        self._whitelist_path_patch = patch(
            "execution.mt5_executor.config.MT5_MICRO_WHITELIST_PATH",
            f"{self._td.name}\\mt5_micro_whitelist_test.json",
        )
        self._whitelist_path_patch.start()
        # Disable Tiger Risk Governor in legacy tests to preserve original behavior
        self._tiger_patch = patch("execution.mt5_executor.tiger_risk_governor", None)
        self._tiger_patch.start()
        self._store_patch = patch("execution.mt5_executor.signal_store", None)
        self._store_patch.start()
        self.exec = MT5Executor()
        # Keep tests independent from live .env allow/block symbol settings.
        self.exec._allow_symbols = set()
        self.exec._block_symbols = set()

    def tearDown(self):
        try:
            self._whitelist_path_patch.stop()
            self._tiger_patch.stop()
            self._store_patch.stop()
        finally:
            self._td.cleanup()

    def test_resolve_symbol_uses_explicit_mapping(self):
        self.exec._symbol_map = {"XAUUSD": "XAUUSDM"}
        self.exec._symbols_cache = ["XAUUSD", "XAUUSDM", "EURUSD"]
        self.exec._symbols_cache_ts = 9e9
        with patch.object(self.exec, "_ensure_connection", return_value=(True, "connected")):
            resolved = self.exec.resolve_symbol("XAUUSD")
        self.assertEqual(resolved, "XAUUSDM")

    def test_resolve_symbol_handles_usdt_pair(self):
        self.exec._symbol_map = {}
        self.exec._symbols_cache = ["BTCUSD", "ETHUSD"]
        self.exec._symbols_cache_ts = 9e9
        with patch.object(self.exec, "_ensure_connection", return_value=(True, "connected")):
            resolved = self.exec.resolve_symbol("BTC/USDT")
        self.assertEqual(resolved, "BTCUSD")

    def test_execute_signal_skips_when_disabled(self):
        sig = make_signal("XAUUSD")
        with patch("execution.mt5_executor.config.MT5_ENABLED", False):
            result = self.exec.execute_signal(sig, source="test")
        self.assertFalse(result.ok)
        self.assertEqual(result.status, "disabled")

    def test_execute_signal_respects_mt5_confidence_threshold(self):
        sig = make_signal("XAUUSD", confidence=60.0)
        with patch("execution.mt5_executor.config.MT5_ENABLED", True), \
             patch("execution.mt5_executor.config.MT5_MIN_SIGNAL_CONFIDENCE", 75):
            result = self.exec.execute_signal(sig, source="test")
        self.assertFalse(result.ok)
        self.assertEqual(result.status, "skipped")

    def test_suggest_symbol_map_generates_env_line(self):
        self.exec._symbol_map = {}
        self.exec._symbols_cache = ["XAUUSDm", "BTCUSD", "ETHUSD", "EURUSD"]
        self.exec._symbols_cache_ts = 9e9
        with patch.object(self.exec, "_ensure_connection", return_value=(True, "connected")):
            report = self.exec.suggest_symbol_map(
                signal_symbols=["XAUUSD", "BTC/USDT", "ETH/USDT", "EURUSD"]
            )
        self.assertTrue(report["connected"])
        self.assertEqual(report["suggested_map"]["XAUUSD"], "XAUUSDm")
        self.assertEqual(report["suggested_map"]["BTC/USDT"], "BTCUSD")
        self.assertEqual(report["suggested_map"]["ETH/USDT"], "ETHUSD")
        self.assertIn("EURUSD", report["passthrough"])
        self.assertIn("MT5_SYMBOL_MAP=", report["env_line"])
        self.assertEqual(report["unresolved"], [])

    def test_suggest_symbol_map_tracks_unresolved(self):
        self.exec._symbol_map = {}
        self.exec._symbols_cache = ["XAUUSDm"]
        self.exec._symbols_cache_ts = 9e9
        with patch.object(self.exec, "_ensure_connection", return_value=(True, "connected")):
            report = self.exec.suggest_symbol_map(signal_symbols=["DOGE/USDT"])
        self.assertEqual(report["resolved_count"], 0)
        self.assertIn("DOGE/USDT", report["unresolved"])

    def test_execute_signal_skips_on_margin_guard(self):
        sig = make_signal("XAUUSD", confidence=90.0, direction="long")
        sig.stop_loss = 5000.0
        sig.take_profit_2 = 5200.0

        fake_mt5 = MagicMock()
        fake_mt5.initialize.return_value = True
        fake_mt5.symbol_select.return_value = True
        fake_mt5.ORDER_TYPE_BUY = 0
        fake_mt5.ORDER_TYPE_SELL = 1
        fake_mt5.ORDER_TIME_GTC = 0
        fake_mt5.ORDER_FILLING_RETURN = 0
        fake_mt5.TRADE_ACTION_DEAL = 1
        fake_mt5.account_info.return_value = MagicMock(margin_free=3.41)
        fake_mt5.symbol_info.return_value = MagicMock(
            digits=2,
            point=0.01,
            trade_stops_level=1,
            volume_min=0.01,
            volume_max=100.0,
            volume_step=0.01,
            filling_mode=0,
        )
        fake_mt5.symbol_info_tick.return_value = MagicMock(ask=5100.0, bid=5099.9)
        fake_mt5.positions_get.return_value = []
        fake_mt5.order_calc_margin.return_value = 10.22  # > allowed

        self.exec._conn = MagicMock()
        self.exec._mt5 = fake_mt5
        self.exec._symbols_cache = ["XAUUSD"]
        self.exec._symbols_cache_ts = 9e9
        self.exec._symbol_map = {}

        with patch("execution.mt5_executor.config.MT5_ENABLED", True), \
             patch("execution.mt5_executor.config.MT5_DRY_RUN", False), \
             patch("execution.mt5_executor.config.MT5_MIN_SIGNAL_CONFIDENCE", 70), \
             patch("execution.mt5_executor.config.MT5_MICRO_MODE_ENABLED", False), \
             patch("execution.mt5_executor.config.MT5_MAX_MARGIN_USAGE_PCT", 35), \
             patch("execution.mt5_executor.config.MT5_MIN_FREE_MARGIN_AFTER_TRADE", 1):
            res = self.exec.execute_signal(sig, source="test")
        self.assertFalse(res.ok)
        self.assertEqual(res.status, "skipped")
        self.assertIn("margin guard", res.message.lower())

    def test_execute_signal_rejected_shows_retcode_hint(self):
        sig = make_signal("XAUUSD", confidence=90.0, direction="long")
        sig.stop_loss = 5000.0
        sig.take_profit_2 = 5200.0

        fake_mt5 = MagicMock()
        fake_mt5.initialize.return_value = True
        fake_mt5.symbol_select.return_value = True
        fake_mt5.ORDER_TYPE_BUY = 0
        fake_mt5.ORDER_TYPE_SELL = 1
        fake_mt5.ORDER_TIME_GTC = 0
        fake_mt5.ORDER_FILLING_RETURN = 0
        fake_mt5.TRADE_ACTION_DEAL = 1
        fake_mt5.TRADE_RETCODE_DONE = 10009
        fake_mt5.TRADE_RETCODE_PLACED = 10008
        fake_mt5.TRADE_RETCODE_DONE_PARTIAL = 10010
        fake_mt5.account_info.return_value = MagicMock(margin_free=1000.0)
        fake_mt5.symbol_info.return_value = MagicMock(
            digits=2,
            point=0.01,
            trade_stops_level=1,
            volume_min=0.01,
            volume_max=100.0,
            volume_step=0.01,
            filling_mode=0,
        )
        fake_mt5.symbol_info_tick.return_value = MagicMock(ask=5100.0, bid=5099.9)
        fake_mt5.positions_get.return_value = []
        fake_mt5.order_calc_margin.return_value = 10.0
        fake_mt5.order_send.return_value = MagicMock(retcode=10027, order=None, deal=None)

        self.exec._conn = MagicMock()
        self.exec._conn.root = object()
        self.exec._mt5 = fake_mt5
        self.exec._symbols_cache = ["XAUUSD"]
        self.exec._symbols_cache_ts = 9e9
        self.exec._symbol_map = {}

        with patch("execution.mt5_executor.config.MT5_ENABLED", True), \
             patch("execution.mt5_executor.config.MT5_DRY_RUN", False), \
             patch("execution.mt5_executor.config.MT5_MIN_SIGNAL_CONFIDENCE", 70), \
             patch("execution.mt5_executor.config.MT5_MAX_MARGIN_USAGE_PCT", 90), \
             patch("execution.mt5_executor.config.MT5_MIN_FREE_MARGIN_AFTER_TRADE", 1):
            res = self.exec.execute_signal(sig, source="test")
        self.assertFalse(res.ok)
        self.assertEqual(res.status, "rejected")
        self.assertIn("retcode=10027", res.message)
        self.assertIn("autotrading", res.message.lower())

    def test_execute_signal_supports_buy_stop_pending_order(self):
        sig = make_signal("XAUUSD", confidence=90.0, direction="long")
        sig.entry = 5102.0
        sig.stop_loss = 5098.0
        sig.take_profit_2 = 5108.0
        sig.entry_type = "buy_stop"

        seen = {}
        fake_mt5 = MagicMock()
        fake_mt5.initialize.return_value = True
        fake_mt5.symbol_select.return_value = True
        fake_mt5.ORDER_TYPE_BUY = 0
        fake_mt5.ORDER_TYPE_SELL = 1
        fake_mt5.ORDER_TYPE_BUY_LIMIT = 2
        fake_mt5.ORDER_TYPE_SELL_LIMIT = 3
        fake_mt5.ORDER_TYPE_BUY_STOP = 4
        fake_mt5.ORDER_TYPE_SELL_STOP = 5
        fake_mt5.ORDER_TIME_GTC = 0
        fake_mt5.ORDER_TIME_SPECIFIED = 1
        fake_mt5.ORDER_FILLING_RETURN = 0
        fake_mt5.TRADE_ACTION_DEAL = 1
        fake_mt5.TRADE_ACTION_PENDING = 5
        fake_mt5.TRADE_RETCODE_DONE = 10009
        fake_mt5.TRADE_RETCODE_PLACED = 10008
        fake_mt5.TRADE_RETCODE_DONE_PARTIAL = 10010
        fake_mt5.account_info.return_value = MagicMock(margin_free=1000.0, login=1, balance=1000.0, equity=1000.0)
        fake_mt5.symbol_info.return_value = MagicMock(
            digits=2,
            point=0.01,
            trade_stops_level=1,
            volume_min=0.01,
            volume_max=100.0,
            volume_step=0.01,
            filling_mode=0,
        )
        fake_mt5.symbol_info_tick.return_value = MagicMock(ask=5100.0, bid=5099.9)
        fake_mt5.positions_get.return_value = []
        fake_mt5.order_calc_margin.return_value = 10.0

        def _order_send(req):
            seen["request"] = dict(req)
            return MagicMock(retcode=10008, order=4321, deal=None, comment="ok")

        fake_mt5.order_send.side_effect = _order_send
        self.exec._conn = MagicMock()
        self.exec._conn.root = object()
        self.exec._mt5 = fake_mt5
        self.exec._symbols_cache = ["XAUUSD"]
        self.exec._symbols_cache_ts = 9e9
        self.exec._symbol_map = {}

        with patch("execution.mt5_executor.config.MT5_ENABLED", True), \
             patch("execution.mt5_executor.config.MT5_DRY_RUN", False), \
             patch("execution.mt5_executor.config.MT5_PENDING_ENTRY_ENABLED", True), \
             patch("execution.mt5_executor.config.MT5_MIN_SIGNAL_CONFIDENCE", 70), \
             patch("execution.mt5_executor.config.MT5_MAX_MARGIN_USAGE_PCT", 90), \
             patch("execution.mt5_executor.config.MT5_MIN_FREE_MARGIN_AFTER_TRADE", 1):
            res = self.exec.execute_signal(sig, source="scalp_xauusd:bs:canary")

        self.assertTrue(res.ok)
        self.assertEqual(res.status, "filled")
        self.assertEqual(int(seen["request"]["action"]), int(fake_mt5.TRADE_ACTION_PENDING))
        self.assertEqual(int(seen["request"]["type"]), int(fake_mt5.ORDER_TYPE_BUY_STOP))
        self.assertEqual(int(seen["request"]["type_time"]), int(fake_mt5.ORDER_TIME_SPECIFIED))
        self.assertIn("expiration", seen["request"])

    def test_closed_trades_snapshot_aggregates_recent_exit_deals(self):
        fake_mt5 = MagicMock()
        fake_mt5.account_info.return_value = MagicMock(login=123456, server="TEST-MT5")
        fake_mt5.DEAL_ENTRY_OUT = 1
        fake_mt5.DEAL_REASON_TP = 5
        fake_mt5.DEAL_REASON_SL = 4
        fake_mt5.DEAL_REASON_CLIENT = 0
        fake_mt5.DEAL_TYPE_SELL = 1
        fake_mt5.DEAL_TYPE_BUY = 0
        fake_mt5.history_deals_get.return_value = [
            SimpleNamespace(
                entry=1,
                reason=5,
                type=1,
                position_id=777,
                ticket=1001,
                symbol="ETHUSD",
                time=1700000000,
                profit=1.25,
                swap=0.0,
                commission=-0.05,
                price=2010.5,
                volume=0.1,
                comment="tp hit",
            ),
        ]
        self.exec._mt5 = fake_mt5
        self.exec._conn = MagicMock()
        self.exec._symbols_cache = ["ETHUSD"]
        self.exec._symbols_cache_ts = 9e9

        with patch.object(self.exec, "_ensure_connection", return_value=(True, "connected")):
            snap = self.exec.closed_trades_snapshot(signal_symbol="ETHUSD", hours=24, limit=5)

        self.assertTrue(snap["connected"])
        self.assertEqual(len(snap["closed_trades"]), 1)
        row = snap["closed_trades"][0]
        self.assertEqual(row["symbol"], "ETHUSD")
        self.assertEqual(row["reason"], "TP")
        self.assertAlmostEqual(float(row["pnl"]), 1.20, places=5)

    def test_closed_trades_snapshot_falls_back_to_timestamp_query_for_bridge(self):
        fake_mt5 = MagicMock()
        fake_mt5.account_info.return_value = MagicMock(login=123456, server="TEST-MT5")
        fake_mt5.DEAL_ENTRY_OUT = 1
        fake_mt5.DEAL_REASON_TP = 5
        fake_mt5.DEAL_TYPE_BUY = 0
        fake_mt5.DEAL_TYPE_SELL = 1

        deal = SimpleNamespace(
            entry=1,
            reason=5,
            type=0,
            position_id=888,
            ticket=2001,
            symbol="ETHUSD",
            time=1700000100,
            profit=2.85,
            swap=0.0,
            commission=0.0,
            price=1920.0,
            volume=0.1,
            comment="[tp 1920.15]",
        )

        def _history_deals_get(a, b):
            # Simulate bridge behavior: datetime calls return None, timestamp ints work.
            if isinstance(a, int) and isinstance(b, int):
                return (deal,)
            return None

        fake_mt5.history_deals_get.side_effect = _history_deals_get
        self.exec._mt5 = fake_mt5
        self.exec._conn = MagicMock()
        self.exec._symbols_cache = ["ETHUSD"]
        self.exec._symbols_cache_ts = 9e9

        with patch.object(self.exec, "_ensure_connection", return_value=(True, "connected")):
            snap = self.exec.closed_trades_snapshot(signal_symbol="ETHUSD", hours=24, limit=5)

        self.assertTrue(snap["connected"])
        self.assertEqual(snap.get("history_query_mode"), "ts_int")
        self.assertEqual(len(snap["closed_trades"]), 1)
        self.assertEqual(snap["closed_trades"][0]["reason"], "TP")

    def test_execute_signal_auto_downsizes_volume_when_affordable(self):
        sig = make_signal("ETH/USDT", confidence=90.0, direction="long")
        sig.stop_loss = 1900.0
        sig.take_profit_2 = 2100.0

        fake_mt5 = MagicMock()
        fake_mt5.initialize.return_value = True
        fake_mt5.symbol_select.return_value = True
        fake_mt5.ORDER_TYPE_BUY = 0
        fake_mt5.ORDER_TYPE_SELL = 1
        fake_mt5.ORDER_TIME_GTC = 0
        fake_mt5.ORDER_FILLING_RETURN = 0
        fake_mt5.TRADE_ACTION_DEAL = 1
        fake_mt5.TRADE_RETCODE_DONE = 10009
        fake_mt5.account_info.return_value = MagicMock(margin_free=10.0)
        fake_mt5.symbol_info.return_value = MagicMock(
            digits=2,
            point=0.01,
            trade_stops_level=1,
            volume_min=0.01,
            volume_max=100.0,
            volume_step=0.01,
            filling_mode=0,
        )
        fake_mt5.symbol_info_tick.return_value = MagicMock(ask=2000.0, bid=1999.9)
        fake_mt5.positions_get.return_value = []

        def _margin_calc(_otype, _sym, vol, _price):
            # linear margin: 0.01 lot => 1.0 margin
            return float(vol) * 100.0

        fake_mt5.order_calc_margin.side_effect = _margin_calc
        fake_mt5.order_send.return_value = MagicMock(retcode=10009, order=1234, deal=1235)

        self.exec._conn = MagicMock()
        self.exec._conn.root = object()
        self.exec._mt5 = fake_mt5
        self.exec._symbols_cache = ["ETHUSD"]
        self.exec._symbols_cache_ts = 9e9
        self.exec._symbol_map = {}

        with patch("execution.mt5_executor.config.MT5_ENABLED", True), \
             patch("execution.mt5_executor.config.MT5_DRY_RUN", False), \
             patch("execution.mt5_executor.config.MT5_LOT_SIZE", 0.10), \
             patch("execution.mt5_executor.config.MT5_MIN_SIGNAL_CONFIDENCE", 70), \
             patch("execution.mt5_executor.config.MT5_MAX_MARGIN_USAGE_PCT", 35), \
             patch("execution.mt5_executor.config.MT5_MIN_FREE_MARGIN_AFTER_TRADE", 1), \
             patch.object(self.exec, "_resolve_filled_position_id", return_value=999):
            res = self.exec.execute_signal(sig, source="test")

        self.assertTrue(res.ok)
        self.assertEqual(res.status, "filled")
        sent_req = fake_mt5.order_send.call_args.args[0]
        self.assertLess(float(sent_req["volume"]), 0.10)
        self.assertGreaterEqual(float(sent_req["volume"]), 0.01)

    def test_execute_signal_applies_adaptive_execution_plan(self):
        sig = make_signal("ETH/USDT", confidence=90.0, direction="long")
        sig.stop_loss = 1900.0
        sig.take_profit_2 = 2100.0
        sig.take_profit_1 = 2050.0
        sig.take_profit_3 = 2200.0

        fake_mt5 = MagicMock()
        fake_mt5.initialize.return_value = True
        fake_mt5.symbol_select.return_value = True
        fake_mt5.ORDER_TYPE_BUY = 0
        fake_mt5.ORDER_TYPE_SELL = 1
        fake_mt5.ORDER_TIME_GTC = 0
        fake_mt5.ORDER_FILLING_RETURN = 0
        fake_mt5.TRADE_ACTION_DEAL = 1
        fake_mt5.TRADE_RETCODE_DONE = 10009
        fake_mt5.account_info.return_value = MagicMock(login=123, margin_free=1000.0)
        fake_mt5.symbol_info.return_value = MagicMock(
            digits=2,
            point=0.01,
            trade_stops_level=1,
            volume_min=0.01,
            volume_max=100.0,
            volume_step=0.01,
            filling_mode=0,
        )
        fake_mt5.symbol_info_tick.return_value = MagicMock(ask=2000.0, bid=1999.9)
        fake_mt5.positions_get.return_value = []
        fake_mt5.order_calc_margin.return_value = 10.0
        fake_mt5.order_send.return_value = MagicMock(retcode=10009, order=1234, deal=1235)

        self.exec._conn = MagicMock()
        self.exec._conn.root = object()
        self.exec._mt5 = fake_mt5
        self.exec._symbols_cache = ["ETHUSD"]
        self.exec._symbols_cache_ts = 9e9
        self.exec._symbol_map = {}

        plan = AdaptiveExecutionPlan(
            ok=True,
            applied=True,
            reason="adaptive_applied",
            signal_symbol="ETH/USDT",
            broker_symbol="ETHUSD",
            account_key="TEST|123",
            rr_target=1.6,
            rr_base=2.0,
            stop_scale=0.9,
            size_multiplier=0.8,
            entry=2000.0,
            stop_loss=1910.0,
            take_profit_1=2090.0,
            take_profit_2=2144.0,
            take_profit_3=2234.0,
            factors={"samples": 12, "spread_pct": 0.005, "atr_pct": 1.75},
        )

        with patch("execution.mt5_executor.config.MT5_ENABLED", True), \
             patch("execution.mt5_executor.config.MT5_DRY_RUN", False), \
             patch("execution.mt5_executor.config.MT5_LOT_SIZE", 0.10), \
             patch("execution.mt5_executor.config.MT5_MIN_SIGNAL_CONFIDENCE", 70), \
             patch("execution.mt5_executor.config.MT5_MAX_MARGIN_USAGE_PCT", 90), \
             patch("execution.mt5_executor.config.MT5_MIN_FREE_MARGIN_AFTER_TRADE", 1), \
             patch("execution.mt5_executor.config.MT5_ADAPTIVE_EXECUTION_ENABLED", True), \
             patch.object(self.exec, "status", return_value={"account_server": "TEST"}), \
             patch("execution.mt5_executor.mt5_adaptive_trade_planner.plan_execution", return_value=plan), \
             patch.object(self.exec, "_resolve_filled_position_id", return_value=999):
            res = self.exec.execute_signal(sig, source="test", volume_multiplier=1.0)

        self.assertTrue(res.ok)
        self.assertEqual(res.status, "filled")
        self.assertIsInstance(res.execution_meta, dict)
        self.assertEqual(float(sig.risk_reward), 1.6)
        sent_req = fake_mt5.order_send.call_args.args[0]
        self.assertAlmostEqual(float(sent_req["sl"]), 1910.0, places=2)
        self.assertAlmostEqual(float(sent_req["tp"]), 2144.0, places=2)
        # lot 0.10 * sizex0.8 => 0.08 (then normalized by step stays 0.08)
        self.assertAlmostEqual(float(sent_req["volume"]), 0.08, places=2)

    def test_execute_limit_entry_keeps_original_entry_and_skips_adaptive_plan(self):
        sig = make_signal("XAUUSD", confidence=90.0, direction="short")
        sig.entry_type = "limit"
        sig.entry = 5114.98
        sig.stop_loss = 5118.36
        sig.take_profit_1 = 5111.85
        sig.take_profit_2 = 5110.80
        sig.take_profit_3 = 5109.41

        fake_mt5 = MagicMock()
        fake_mt5.initialize.return_value = True
        fake_mt5.symbol_select.return_value = True
        fake_mt5.ORDER_TYPE_BUY = 0
        fake_mt5.ORDER_TYPE_SELL = 1
        fake_mt5.ORDER_TYPE_BUY_LIMIT = 2
        fake_mt5.ORDER_TYPE_SELL_LIMIT = 3
        fake_mt5.ORDER_TIME_GTC = 0
        fake_mt5.ORDER_TIME_SPECIFIED = 1
        fake_mt5.ORDER_FILLING_RETURN = 0
        fake_mt5.TRADE_ACTION_DEAL = 1
        fake_mt5.TRADE_ACTION_PENDING = 5
        fake_mt5.TRADE_RETCODE_DONE = 10009
        fake_mt5.TRADE_RETCODE_PLACED = 10008
        fake_mt5.TRADE_RETCODE_DONE_PARTIAL = 10010
        fake_mt5.account_info.return_value = MagicMock(login=123, margin_free=1000.0)
        fake_mt5.symbol_info.return_value = MagicMock(
            digits=2,
            point=0.01,
            trade_stops_level=1,
            volume_min=0.01,
            volume_max=100.0,
            volume_step=0.01,
            filling_mode=0,
        )
        fake_mt5.symbol_info_tick.return_value = MagicMock(ask=5110.50, bid=5110.35)
        fake_mt5.positions_get.return_value = []
        fake_mt5.order_calc_margin.return_value = 10.0
        fake_mt5.order_send.return_value = MagicMock(retcode=10008, order=1234, deal=None)

        self.exec._conn = MagicMock()
        self.exec._conn.root = object()
        self.exec._mt5 = fake_mt5
        self.exec._symbols_cache = ["XAUUSD"]
        self.exec._symbols_cache_ts = 9e9
        self.exec._symbol_map = {}

        with patch("execution.mt5_executor.config.MT5_ENABLED", True), \
             patch("execution.mt5_executor.config.MT5_DRY_RUN", False), \
             patch("execution.mt5_executor.config.MT5_MIN_SIGNAL_CONFIDENCE", 70), \
             patch("execution.mt5_executor.config.MT5_MAX_MARGIN_USAGE_PCT", 90), \
             patch("execution.mt5_executor.config.MT5_MIN_FREE_MARGIN_AFTER_TRADE", 1), \
             patch("execution.mt5_executor.config.MT5_LIMIT_ENTRY_ENABLED", True), \
             patch("execution.mt5_executor.config.MT5_LIMIT_ADAPTIVE_EXITS_SIZE_ONLY", True), \
             patch("execution.mt5_executor.config.MT5_LIMIT_ENTRY_ALLOW_MARKET_FALLBACK", False), \
             patch("execution.mt5_executor.config.MT5_ADAPTIVE_EXECUTION_ENABLED", True), \
             patch("execution.mt5_executor.mt5_adaptive_trade_planner.plan_execution", return_value=AdaptiveExecutionPlan(ok=False, applied=False, reason="noop")) as plan_call:
            res = self.exec.execute_signal(sig, source="scalp_xauusd:bypass")

        self.assertTrue(res.ok)
        self.assertEqual(res.status, "filled")
        self.assertTrue(plan_call.called)
        sent_req = fake_mt5.order_send.call_args.args[0]
        self.assertEqual(int(sent_req["action"]), int(fake_mt5.TRADE_ACTION_PENDING))
        self.assertEqual(int(sent_req["type"]), int(fake_mt5.ORDER_TYPE_SELL_LIMIT))
        self.assertAlmostEqual(float(sent_req["price"]), 5114.98, places=2)
        self.assertAlmostEqual(float(sig.entry), 5114.98, places=2)

    def test_execute_limit_entry_strict_skips_when_market_crossed(self):
        sig = make_signal("XAUUSD", confidence=90.0, direction="short")
        sig.entry_type = "limit"
        sig.entry = 5099.00  # crossed below current bid -> invalid SELL LIMIT
        sig.stop_loss = 5102.00
        sig.take_profit_2 = 5095.00

        fake_mt5 = MagicMock()
        fake_mt5.initialize.return_value = True
        fake_mt5.symbol_select.return_value = True
        fake_mt5.ORDER_TYPE_BUY = 0
        fake_mt5.ORDER_TYPE_SELL = 1
        fake_mt5.ORDER_TYPE_BUY_LIMIT = 2
        fake_mt5.ORDER_TYPE_SELL_LIMIT = 3
        fake_mt5.ORDER_TIME_GTC = 0
        fake_mt5.ORDER_TIME_SPECIFIED = 1
        fake_mt5.ORDER_FILLING_RETURN = 0
        fake_mt5.TRADE_ACTION_DEAL = 1
        fake_mt5.TRADE_ACTION_PENDING = 5
        fake_mt5.account_info.return_value = MagicMock(login=123, margin_free=1000.0)
        fake_mt5.symbol_info.return_value = MagicMock(
            digits=2,
            point=0.01,
            trade_stops_level=1,
            volume_min=0.01,
            volume_max=100.0,
            volume_step=0.01,
            filling_mode=0,
        )
        fake_mt5.symbol_info_tick.return_value = MagicMock(ask=5110.50, bid=5110.35)
        fake_mt5.positions_get.return_value = []
        fake_mt5.order_calc_margin.return_value = 10.0

        self.exec._conn = MagicMock()
        self.exec._conn.root = object()
        self.exec._mt5 = fake_mt5
        self.exec._symbols_cache = ["XAUUSD"]
        self.exec._symbols_cache_ts = 9e9
        self.exec._symbol_map = {}

        with patch("execution.mt5_executor.config.MT5_ENABLED", True), \
             patch("execution.mt5_executor.config.MT5_DRY_RUN", False), \
             patch("execution.mt5_executor.config.MT5_MIN_SIGNAL_CONFIDENCE", 70), \
             patch("execution.mt5_executor.config.MT5_MAX_MARGIN_USAGE_PCT", 90), \
             patch("execution.mt5_executor.config.MT5_MIN_FREE_MARGIN_AFTER_TRADE", 1), \
             patch("execution.mt5_executor.config.MT5_LIMIT_ENTRY_ENABLED", True), \
             patch("execution.mt5_executor.config.MT5_LIMIT_ENTRY_ALLOW_MARKET_FALLBACK", False), \
             patch("execution.mt5_executor.config.MT5_ADAPTIVE_EXECUTION_ENABLED", False):
            res = self.exec.execute_signal(sig, source="scalp_xauusd:bypass")

        self.assertFalse(res.ok)
        self.assertEqual(res.status, "skipped")
        self.assertIn("strict limit", res.message.lower())
        self.assertFalse(fake_mt5.order_send.called)

    def test_limit_fallback_guard_blocks_when_confidence_low(self):
        sig = make_signal("XAUUSD", confidence=75.0, direction="short")
        sig.entry_type = "limit"
        sig.entry = 5099.0  # crossed for short
        sig.stop_loss = 5102.0
        sig.take_profit_2 = 5095.0
        sig.atr = 6.0

        fake_mt5 = MagicMock()
        fake_mt5.initialize.return_value = True
        fake_mt5.symbol_select.return_value = True
        fake_mt5.ORDER_TYPE_BUY = 0
        fake_mt5.ORDER_TYPE_SELL = 1
        fake_mt5.ORDER_TYPE_BUY_LIMIT = 2
        fake_mt5.ORDER_TYPE_SELL_LIMIT = 3
        fake_mt5.ORDER_TIME_GTC = 0
        fake_mt5.ORDER_TIME_SPECIFIED = 1
        fake_mt5.ORDER_FILLING_RETURN = 0
        fake_mt5.TRADE_ACTION_DEAL = 1
        fake_mt5.TRADE_ACTION_PENDING = 5
        fake_mt5.account_info.return_value = MagicMock(login=123, margin_free=1000.0)
        fake_mt5.symbol_info.return_value = MagicMock(
            digits=2, point=0.01, trade_stops_level=1, volume_min=0.01, volume_max=100.0, volume_step=0.01, filling_mode=0
        )
        fake_mt5.symbol_info_tick.return_value = MagicMock(ask=5110.50, bid=5110.35)
        fake_mt5.positions_get.return_value = []
        fake_mt5.order_calc_margin.return_value = 10.0

        self.exec._conn = MagicMock()
        self.exec._conn.root = object()
        self.exec._mt5 = fake_mt5
        self.exec._symbols_cache = ["XAUUSD"]
        self.exec._symbols_cache_ts = 9e9
        self.exec._symbol_map = {}

        with patch("execution.mt5_executor.config.MT5_ENABLED", True), \
             patch("execution.mt5_executor.config.MT5_DRY_RUN", False), \
             patch("execution.mt5_executor.config.MT5_MIN_SIGNAL_CONFIDENCE", 70), \
             patch("execution.mt5_executor.config.MT5_MAX_MARGIN_USAGE_PCT", 90), \
             patch("execution.mt5_executor.config.MT5_MIN_FREE_MARGIN_AFTER_TRADE", 1), \
             patch("execution.mt5_executor.config.MT5_LIMIT_ENTRY_ENABLED", True), \
             patch("execution.mt5_executor.config.MT5_LIMIT_ENTRY_ALLOW_MARKET_FALLBACK", True), \
             patch("execution.mt5_executor.config.MT5_LIMIT_FALLBACK_MIN_CONFIDENCE", 82.0), \
             patch("execution.mt5_executor.config.MT5_LIMIT_FALLBACK_MAX_SPREAD_PCT", 0.03), \
             patch("execution.mt5_executor.config.MT5_LIMIT_FALLBACK_MAX_SLIPPAGE_ATR", 0.20), \
             patch("execution.mt5_executor.config.MT5_ADAPTIVE_EXECUTION_ENABLED", False):
            res = self.exec.execute_signal(sig, source="scalp_xauusd:bypass")

        self.assertFalse(res.ok)
        self.assertEqual(res.status, "skipped")
        self.assertIn("fallback_guard", res.message.lower())
        self.assertFalse(fake_mt5.order_send.called)

    def test_limit_fallback_guard_allows_market_retry_when_quality_ok(self):
        sig = make_signal("XAUUSD", confidence=90.0, direction="short")
        sig.entry_type = "limit"
        sig.entry = 5110.20  # crossed for short (below bid=5110.35), but tiny slip
        sig.stop_loss = 5113.20
        sig.take_profit_2 = 5106.20
        sig.atr = 10.0

        fake_mt5 = MagicMock()
        fake_mt5.initialize.return_value = True
        fake_mt5.symbol_select.return_value = True
        fake_mt5.ORDER_TYPE_BUY = 0
        fake_mt5.ORDER_TYPE_SELL = 1
        fake_mt5.ORDER_TYPE_BUY_LIMIT = 2
        fake_mt5.ORDER_TYPE_SELL_LIMIT = 3
        fake_mt5.ORDER_TIME_GTC = 0
        fake_mt5.ORDER_TIME_SPECIFIED = 1
        fake_mt5.ORDER_FILLING_RETURN = 0
        fake_mt5.TRADE_ACTION_DEAL = 1
        fake_mt5.TRADE_ACTION_PENDING = 5
        fake_mt5.TRADE_RETCODE_DONE = 10009
        fake_mt5.TRADE_RETCODE_PLACED = 10008
        fake_mt5.TRADE_RETCODE_DONE_PARTIAL = 10010
        fake_mt5.account_info.return_value = MagicMock(login=123, margin_free=1000.0)
        fake_mt5.symbol_info.return_value = MagicMock(
            digits=2, point=0.01, trade_stops_level=1, volume_min=0.01, volume_max=100.0, volume_step=0.01, filling_mode=0
        )
        fake_mt5.symbol_info_tick.return_value = MagicMock(ask=5110.50, bid=5110.35)
        fake_mt5.positions_get.return_value = []
        fake_mt5.order_calc_margin.return_value = 10.0
        fake_mt5.order_send.return_value = MagicMock(retcode=10009, order=3333, deal=4444, price=5110.35)

        self.exec._conn = MagicMock()
        self.exec._conn.root = object()
        self.exec._mt5 = fake_mt5
        self.exec._symbols_cache = ["XAUUSD"]
        self.exec._symbols_cache_ts = 9e9
        self.exec._symbol_map = {}

        with patch("execution.mt5_executor.config.MT5_ENABLED", True), \
             patch("execution.mt5_executor.config.MT5_DRY_RUN", False), \
             patch("execution.mt5_executor.config.MT5_MIN_SIGNAL_CONFIDENCE", 70), \
             patch("execution.mt5_executor.config.MT5_MAX_MARGIN_USAGE_PCT", 90), \
             patch("execution.mt5_executor.config.MT5_MIN_FREE_MARGIN_AFTER_TRADE", 1), \
             patch("execution.mt5_executor.config.MT5_LIMIT_ENTRY_ENABLED", True), \
             patch("execution.mt5_executor.config.MT5_LIMIT_ENTRY_ALLOW_MARKET_FALLBACK", True), \
             patch("execution.mt5_executor.config.MT5_LIMIT_FALLBACK_MIN_CONFIDENCE", 82.0), \
             patch("execution.mt5_executor.config.MT5_LIMIT_FALLBACK_MAX_SPREAD_PCT", 0.03), \
             patch("execution.mt5_executor.config.MT5_LIMIT_FALLBACK_MAX_SLIPPAGE_ATR", 0.20), \
             patch("execution.mt5_executor.config.MT5_ADAPTIVE_EXECUTION_ENABLED", False), \
             patch.object(self.exec, "_resolve_filled_position_id", return_value=999):
            res = self.exec.execute_signal(sig, source="scalp_xauusd:bypass")

        self.assertTrue(res.ok)
        self.assertEqual(res.status, "filled")
        sent_req = fake_mt5.order_send.call_args.args[0]
        self.assertEqual(int(sent_req["action"]), int(fake_mt5.TRADE_ACTION_DEAL))
        self.assertEqual(int(sent_req["type"]), int(fake_mt5.ORDER_TYPE_SELL))

    def test_limit_fallback_respects_per_signal_override_disabled(self):
        sig = make_signal("XAUUSD", confidence=90.0, direction="short")
        sig.entry_type = "limit"
        sig.entry = 5099.0  # crossed for short (below bid), would normally fallback
        sig.stop_loss = 5102.0
        sig.take_profit_2 = 5095.0
        sig.atr = 8.0
        sig.raw_scores = {"mt5_limit_allow_market_fallback": False}

        fake_mt5 = MagicMock()
        fake_mt5.initialize.return_value = True
        fake_mt5.symbol_select.return_value = True
        fake_mt5.ORDER_TYPE_BUY = 0
        fake_mt5.ORDER_TYPE_SELL = 1
        fake_mt5.ORDER_TYPE_BUY_LIMIT = 2
        fake_mt5.ORDER_TYPE_SELL_LIMIT = 3
        fake_mt5.ORDER_TIME_GTC = 0
        fake_mt5.ORDER_TIME_SPECIFIED = 1
        fake_mt5.ORDER_FILLING_RETURN = 0
        fake_mt5.TRADE_ACTION_DEAL = 1
        fake_mt5.TRADE_ACTION_PENDING = 5
        fake_mt5.account_info.return_value = MagicMock(login=123, margin_free=1000.0)
        fake_mt5.symbol_info.return_value = MagicMock(
            digits=2, point=0.01, trade_stops_level=1, volume_min=0.01, volume_max=100.0, volume_step=0.01, filling_mode=0
        )
        fake_mt5.symbol_info_tick.return_value = MagicMock(ask=5110.50, bid=5110.35)
        fake_mt5.positions_get.return_value = []
        fake_mt5.order_calc_margin.return_value = 10.0

        self.exec._conn = MagicMock()
        self.exec._conn.root = object()
        self.exec._mt5 = fake_mt5
        self.exec._symbols_cache = ["XAUUSD"]
        self.exec._symbols_cache_ts = 9e9
        self.exec._symbol_map = {}

        with patch("execution.mt5_executor.config.MT5_ENABLED", True), \
             patch("execution.mt5_executor.config.MT5_DRY_RUN", False), \
             patch("execution.mt5_executor.config.MT5_MIN_SIGNAL_CONFIDENCE", 70), \
             patch("execution.mt5_executor.config.MT5_MAX_MARGIN_USAGE_PCT", 90), \
             patch("execution.mt5_executor.config.MT5_MIN_FREE_MARGIN_AFTER_TRADE", 1), \
             patch("execution.mt5_executor.config.MT5_LIMIT_ENTRY_ENABLED", True), \
             patch("execution.mt5_executor.config.MT5_LIMIT_ENTRY_ALLOW_MARKET_FALLBACK", True), \
             patch("execution.mt5_executor.config.MT5_ADAPTIVE_EXECUTION_ENABLED", False):
            res = self.exec.execute_signal(sig, source="scalp_xauusd:winner:bypass")

        self.assertFalse(res.ok)
        self.assertEqual(res.status, "skipped")
        self.assertIn("fallback_disabled", str(res.message).lower())
        self.assertFalse(fake_mt5.order_send.called)

    def test_preview_adaptive_execution_returns_plan_without_sending_order(self):
        sig = make_signal("ETH/USDT", confidence=90.0, direction="long")
        sig.stop_loss = 1900.0
        sig.take_profit_2 = 2100.0

        fake_mt5 = MagicMock()
        fake_mt5.symbol_select.return_value = True
        fake_mt5.ORDER_TYPE_BUY = 0
        fake_mt5.ORDER_TYPE_SELL = 1
        fake_mt5.account_info.return_value = MagicMock(login=123, balance=10.0, equity=10.0, margin_free=10.0)
        fake_mt5.symbol_info.return_value = MagicMock(
            digits=2, point=0.01, trade_stops_level=1,
            volume_min=0.01, volume_max=100.0, volume_step=0.01, filling_mode=0
        )
        fake_mt5.symbol_info_tick.return_value = MagicMock(ask=2000.0, bid=1999.9)
        fake_mt5.order_calc_margin.return_value = 1.0

        self.exec._mt5 = fake_mt5
        self.exec._conn = MagicMock()
        self.exec._symbols_cache = ["ETHUSD"]
        self.exec._symbols_cache_ts = 9e9
        self.exec._symbol_map = {}

        plan = AdaptiveExecutionPlan(
            ok=True, applied=True, reason="adaptive_applied",
            signal_symbol="ETH/USDT", broker_symbol="ETHUSD", account_key="TEST|123",
            rr_target=1.7, rr_base=2.0, stop_scale=0.95, size_multiplier=0.85,
            entry=2000.0, stop_loss=1910.0, take_profit_1=2090.0, take_profit_2=2153.0, take_profit_3=2243.0,
            factors={"samples": 7, "spread_pct": 0.005, "atr_pct": 1.2},
        )
        with patch("execution.mt5_executor.config.MT5_ENABLED", True), \
             patch("execution.mt5_executor.config.MT5_MIN_SIGNAL_CONFIDENCE", 70), \
             patch("execution.mt5_executor.config.MT5_LOT_SIZE", 0.10), \
             patch("execution.mt5_executor.config.MT5_MAX_MARGIN_USAGE_PCT", 90), \
             patch("execution.mt5_executor.config.MT5_MIN_FREE_MARGIN_AFTER_TRADE", 1), \
             patch.object(self.exec, "_ensure_connection", return_value=(True, "connected")), \
             patch.object(self.exec, "status", return_value={"account_server": "TEST"}), \
             patch("execution.mt5_executor.mt5_adaptive_trade_planner.plan_execution", return_value=plan):
            preview = self.exec.preview_adaptive_execution(sig, source="test")

        self.assertTrue(preview["ok"])
        self.assertEqual(preview["status"], "ok")
        self.assertEqual(preview["broker_symbol"], "ETHUSD")
        self.assertEqual(float(preview["execution"]["risk_reward"]), 1.7)
        # Preview should mirror execute-path volume normalization (0.085 -> 0.09 on 0.01 grid).
        self.assertAlmostEqual(float(preview["execution"]["fitted_volume"]), 0.09, places=2)
        self.assertFalse(fake_mt5.order_send.called)

    def test_execute_signal_micro_mode_skips_wide_spread(self):
        sig = make_signal("ETH/USDT", confidence=90.0, direction="long")
        sig.stop_loss = 1900.0
        sig.take_profit_2 = 2100.0

        fake_mt5 = MagicMock()
        fake_mt5.symbol_select.return_value = True
        fake_mt5.ORDER_TYPE_BUY = 0
        fake_mt5.ORDER_TYPE_SELL = 1
        fake_mt5.ORDER_TIME_GTC = 0
        fake_mt5.ORDER_FILLING_RETURN = 0
        fake_mt5.TRADE_ACTION_DEAL = 1
        fake_mt5.account_info.return_value = MagicMock(margin_free=100.0)
        fake_mt5.symbol_info.return_value = MagicMock(
            digits=2, point=0.01, trade_stops_level=1,
            volume_min=0.01, volume_max=100.0, volume_step=0.01, filling_mode=0
        )
        fake_mt5.symbol_info_tick.return_value = MagicMock(ask=2000.0, bid=1990.0)  # 0.5% spread
        fake_mt5.positions_get.return_value = []

        self.exec._mt5 = fake_mt5
        self.exec._conn = MagicMock()
        self.exec._symbols_cache = ["ETHUSD"]
        self.exec._symbols_cache_ts = 9e9

        with patch("execution.mt5_executor.config.MT5_ENABLED", True), \
             patch("execution.mt5_executor.config.MT5_MIN_SIGNAL_CONFIDENCE", 70), \
             patch("execution.mt5_executor.config.MT5_MICRO_MODE_ENABLED", True), \
             patch("execution.mt5_executor.config.MT5_MICRO_MAX_SPREAD_PCT", 0.15), \
             patch.object(self.exec, "_ensure_connection", return_value=(True, "connected")):
            res = self.exec.execute_signal(sig, source="test")

        self.assertFalse(res.ok)
        self.assertEqual(res.status, "micro_filtered")
        self.assertIn("spread filter", res.message.lower())

    def test_position_limits_micro_mode_single_position_only(self):
        fake_mt5 = MagicMock()
        fake_mt5.positions_get.side_effect = [[SimpleNamespace(ticket=1)], []]
        fake_mt5.ORDER_TYPE_BUY = 0
        fake_mt5.ORDER_TYPE_SELL = 1
        self.exec._mt5 = fake_mt5

        with patch("execution.mt5_executor.config.MT5_MAX_OPEN_POSITIONS", 5), \
             patch("execution.mt5_executor.config.MT5_MAX_POSITIONS_PER_SYMBOL", 5), \
             patch("execution.mt5_executor.config.MT5_MICRO_MODE_ENABLED", True), \
             patch("execution.mt5_executor.config.MT5_MICRO_SINGLE_POSITION_ONLY", True):
            ok, reason = self.exec._position_limits_ok("ETHUSD", "long")

        self.assertFalse(ok)
        self.assertIn("single open position", reason.lower())

    def test_position_limits_can_be_ignored_for_bypass_lane(self):
        fake_mt5 = MagicMock()
        fake_mt5.positions_get.return_value = [SimpleNamespace(ticket=1)]
        fake_mt5.ORDER_TYPE_BUY = 0
        fake_mt5.ORDER_TYPE_SELL = 1
        self.exec._mt5 = fake_mt5

        with patch("execution.mt5_executor.config.MT5_MAX_OPEN_POSITIONS", 1), \
             patch("execution.mt5_executor.config.MT5_MAX_POSITIONS_PER_SYMBOL", 1), \
             patch("execution.mt5_executor.config.MT5_MICRO_MODE_ENABLED", True), \
             patch("execution.mt5_executor.config.MT5_MICRO_SINGLE_POSITION_ONLY", True):
            ok, reason = self.exec._position_limits_ok("ETHUSD", "long", ignore_open_positions=True)

        self.assertTrue(ok)
        self.assertIn("bypass_ignore_open_positions", reason)

    def test_execute_signal_bypass_lane_uses_offset_magic_and_ignores_open_positions(self):
        sig = make_signal("XAUUSD", confidence=69.0, direction="long")
        sig.raw_scores.update(
            {
                "mt5_bypass_test_enabled": True,
                "mt5_bypass_skip_confidence": True,
                "mt5_bypass_ignore_open_positions": True,
                "mt5_bypass_magic_offset": 500,
                "signal_run_no": 77,
            }
        )

        fake_mt5 = MagicMock()
        fake_mt5.initialize.return_value = True
        fake_mt5.symbol_select.return_value = True
        fake_mt5.ORDER_TYPE_BUY = 0
        fake_mt5.ORDER_TYPE_SELL = 1
        fake_mt5.ORDER_TIME_GTC = 0
        fake_mt5.ORDER_FILLING_RETURN = 0
        fake_mt5.TRADE_ACTION_DEAL = 1
        fake_mt5.TRADE_RETCODE_DONE = 10009
        fake_mt5.account_info.return_value = MagicMock(margin_free=1000.0)
        fake_mt5.symbol_info.return_value = MagicMock(
            digits=2,
            point=0.01,
            trade_stops_level=1,
            volume_min=0.01,
            volume_max=100.0,
            volume_step=0.01,
            filling_mode=0,
        )
        fake_mt5.symbol_info_tick.return_value = MagicMock(ask=100.0, bid=99.9)
        fake_mt5.positions_get.return_value = [SimpleNamespace(ticket=1, magic=770100, comment="DEXTER:main:XAUUSD")]
        fake_mt5.order_calc_margin.return_value = 5.0
        fake_mt5.order_send.return_value = MagicMock(retcode=10009, order=9876, deal=9877)

        self.exec._conn = MagicMock()
        self.exec._conn.root = object()
        self.exec._mt5 = fake_mt5
        self.exec._symbols_cache = ["XAUUSD"]
        self.exec._symbols_cache_ts = 9e9
        self.exec._symbol_map = {}

        with patch("execution.mt5_executor.config.MT5_ENABLED", True), \
             patch("execution.mt5_executor.config.MT5_DRY_RUN", False), \
             patch("execution.mt5_executor.config.MT5_MIN_SIGNAL_CONFIDENCE", 75), \
             patch("execution.mt5_executor.config.MT5_BYPASS_TEST_ENABLED", True), \
             patch("execution.mt5_executor.config.MT5_BYPASS_TEST_MAGIC_OFFSET", 500), \
             patch("execution.mt5_executor.config.MT5_ADAPTIVE_EXECUTION_ENABLED", False), \
             patch("execution.mt5_executor.config.MT5_MAX_MARGIN_USAGE_PCT", 90), \
             patch("execution.mt5_executor.config.MT5_MIN_FREE_MARGIN_AFTER_TRADE", 1), \
             patch.object(self.exec, "_resolve_filled_position_id", return_value=555):
            res = self.exec.execute_signal(sig, source="scalp_xauusd:bypass")

        self.assertTrue(res.ok)
        sent_req = fake_mt5.order_send.call_args.args[0]
        self.assertEqual(int(sent_req.get("magic")), 770600)
        self.assertIn("BYPASS", str(sent_req.get("comment", "")).upper())

    def test_micro_whitelist_learner_records_and_reports_status(self):
        with tempfile.TemporaryDirectory() as td:
            with patch("execution.mt5_executor.config.MT5_MICRO_WHITELIST_PATH", f"{td}\\micro.json"):
                exec2 = MT5Executor()
            acct = SimpleNamespace(login=123, server="TEST", balance=6.26, equity=6.26, margin_free=6.26)
            ctx = exec2._micro_account_bucket_ctx(acct)
            with patch("execution.mt5_executor.config.MT5_MICRO_MODE_ENABLED", True), \
                 patch("execution.mt5_executor.config.MT5_MICRO_WHITELIST_LEARNER_ENABLED", True):
                exec2._micro_record_symbol_observation(ctx, "ETHUSD", signal_symbol="ETH/USDT", status="allow", reason="affordable", margin_required=0.09)
                exec2._micro_record_symbol_observation(ctx, "XAUUSD", signal_symbol="XAUUSD", status="deny_margin", reason="min lot too high", min_lot_margin=10.31)
                stat = exec2.micro_whitelist_status(acct)
                cached = exec2._micro_cached_deny(ctx, "XAUUSD")

            self.assertTrue(stat["enabled"])
            self.assertEqual(stat["total_symbols"], 2)
            self.assertEqual(stat["allowed"], 1)
            self.assertEqual(stat["denied"], 1)
            self.assertIsNotNone(cached)
            self.assertEqual(str(cached.get("status")), "deny_margin")

    def test_crypto_confidence_soft_filter_supports_mapped_symbol_overrides(self):
        sig = make_signal("ETH/USDT", confidence=73.5, direction="long")
        self.exec._symbols_cache = ["ETHUSD"]
        self.exec._symbols_cache_ts = 9e9

        with patch.object(self.exec, "resolve_symbol", return_value="ETHUSD"), \
             patch("execution.mt5_executor.config.MT5_CRYPTO_CONF_SOFT_FILTER_ENABLED", True), \
             patch("execution.mt5_executor.config.MT5_CRYPTO_CONF_SOFT_FILTER_BAND_PTS", 4.0), \
             patch("execution.mt5_executor.config.MT5_CRYPTO_CONF_SOFT_FILTER_MAX_SIZE_PENALTY", 0.25), \
             patch("execution.mt5_executor.config.get_mt5_crypto_conf_soft_filter_band_pts_symbol_overrides", return_value={"ETHUSD": 2.0}), \
             patch("execution.mt5_executor.config.get_mt5_crypto_conf_soft_filter_max_penalty_symbol_overrides", return_value={"ETHUSD": 0.5}):
            applied, info = self.exec._maybe_apply_fx_confidence_soft_filter(sig, source="crypto", min_conf=75.0)

        self.assertTrue(applied)
        self.assertTrue(info.get("applied"))
        self.assertEqual(info.get("reason"), "soft_size_penalty")
        self.assertAlmostEqual(float(info.get("size_multiplier")), 0.625, places=3)
        self.assertIn("mapped", str(info.get("band_pts_override_reason", "")))
        self.assertIn("mapped", str(info.get("max_penalty_override_reason", "")))


if __name__ == "__main__":
    unittest.main()
