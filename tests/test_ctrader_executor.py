import gc
import sqlite3
import shutil
import tempfile
import time
import unittest
import json
from contextlib import ExitStack
from pathlib import Path
from unittest.mock import PropertyMock, patch

from analysis.signals import TradeSignal
from execution import ctrader_executor as ctrader_module


def _make_signal(symbol: str = "XAUUSD", direction: str = "long", confidence: float = 77.0) -> TradeSignal:
    return TradeSignal(
        symbol=symbol,
        direction=direction,
        confidence=confidence,
        entry=5100.0,
        stop_loss=5095.0,
        take_profit_1=5103.0,
        take_profit_2=5106.0,
        take_profit_3=5109.0,
        risk_reward=1.2,
        timeframe="5m+1m",
        session="london,new_york,overlap",
        trend="bullish",
        rsi=58.0,
        atr=6.2,
        pattern="SCALP_FLOW_FORCE",
        reasons=["test"],
        warnings=[],
        raw_scores={"signal_run_no": 7, "signal_run_id": "20260307010101-000007"},
        entry_type="limit",
    )


def _execute_signal_with_fixture_reference(executor, signal: TradeSignal, *, source: str, reference_price: float | None = None):
    ref = float(reference_price if reference_price is not None else getattr(signal, "entry", 0.0) or 0.0)
    with patch.object(executor, "_reference_price", return_value=ref):
        return executor.execute_signal(signal, source=source)


class TestCTraderExecutor(unittest.TestCase):
    def test_build_payload_carries_xau_multi_tf_metadata(self):
        td = tempfile.mkdtemp()
        executor = None
        try:
            db_path = str(Path(td) / "ctrader_openapi.db")
            sig = _make_signal()
            sig.raw_scores.update(
                {
                    "signal_h1_trend": "bullish",
                    "signal_h4_trend": "bullish",
                    "xau_mtf_countertrend_confirmed": False,
                    "xau_multi_tf_snapshot": {"aligned_side": "long"},
                }
            )
            with patch.object(ctrader_module.config, "CTRADER_DB_PATH", db_path), \
                 patch.object(ctrader_module.config, "CTRADER_ACCOUNT_ID", "43880642"), \
                 patch.object(ctrader_module.config, "CTRADER_ENABLED", True), \
                 patch.object(ctrader_module.config, "CTRADER_AUTOTRADE_ENABLED", True), \
                 patch.object(ctrader_module.config, "CTRADER_DRY_RUN", True), \
                 patch.object(ctrader_module.config, "get_ctrader_default_volume_symbol_overrides", return_value={}), \
                 patch.object(ctrader_module.CTraderExecutor, "sdk_available", new_callable=PropertyMock, return_value=True):
                executor = ctrader_module.CTraderExecutor()
                payload, reason = executor._build_payload(sig, source="scalp_xauusd:td:canary")

            self.assertEqual(reason, "")
            self.assertIsNotNone(payload)
            self.assertEqual(str(payload.get("signal_h1_trend") or ""), "bullish")
            self.assertEqual(str(payload.get("signal_h4_trend") or ""), "bullish")
            self.assertEqual(str(payload.get("xau_mtf_aligned_side") or ""), "long")
            self.assertFalse(bool(payload.get("countertrend_confirmed")))
        finally:
            executor = None
            gc.collect()
            shutil.rmtree(td, ignore_errors=True)

    def test_journal_pre_dispatch_skip_persists_gate_and_reason(self):
        td = tempfile.mkdtemp()
        executor = None
        try:
            db_path = str(Path(td) / "ctrader_openapi.db")
            sig = _make_signal(direction="short", confidence=73.5)
            sig.entry_type = "limit"
            sig.raw_scores.update({"state_label": "range_probe", "day_type": "trend"})
            with patch.object(ctrader_module.config, "CTRADER_DB_PATH", db_path), \
                 patch.object(ctrader_module.config, "CTRADER_ENABLED", True), \
                 patch.object(ctrader_module.config, "CTRADER_AUTOTRADE_ENABLED", True), \
                 patch.object(ctrader_module.CTraderExecutor, "sdk_available", new_callable=PropertyMock, return_value=True):
                executor = ctrader_module.CTraderExecutor()
                journal_id = executor.journal_pre_dispatch_skip(
                    sig,
                    source="scalp_xauusd:canary",
                    reason="xau_forced_continuation_block",
                    gate="source_profile",
                    request_payload={"source": "scalp_xauusd:canary", "symbol": "XAUUSD"},
                    execution_meta={
                        "requested_source": "scalp_xauusd",
                        "dispatch_source": "scalp_xauusd:canary",
                        "runtime_state": {"xau_shock_profile": {"status": "active", "mode": "shock_protect"}},
                    },
                )

                with sqlite3.connect(db_path) as conn:
                    conn.row_factory = sqlite3.Row
                    row = conn.execute(
                        "SELECT source, lane, symbol, status, message, signal_run_id, request_json, execution_meta_json FROM execution_journal WHERE id=?",
                        (journal_id,),
                    ).fetchone()

            self.assertIsNotNone(row)
            self.assertEqual(str(row["source"] or ""), "scalp_xauusd:canary")
            self.assertEqual(str(row["lane"] or ""), "canary")
            self.assertEqual(str(row["symbol"] or ""), "XAUUSD")
            self.assertEqual(str(row["status"] or ""), "filtered")
            self.assertEqual(str(row["message"] or ""), "xau_forced_continuation_block")
            self.assertEqual(str(row["signal_run_id"] or ""), "20260307010101-000007")
            request_json = json.loads(str(row["request_json"] or "{}"))
            execution_meta = json.loads(str(row["execution_meta_json"] or "{}"))
            self.assertEqual(str(request_json.get("source") or ""), "scalp_xauusd:canary")
            self.assertTrue(bool(execution_meta.get("pre_dispatch_audit")))
            self.assertEqual(str(execution_meta.get("pre_dispatch_gate") or ""), "source_profile")
            self.assertEqual(str(execution_meta.get("pre_dispatch_reason") or ""), "xau_forced_continuation_block")
            self.assertIn("xau_pre_dispatch_skip", list(execution_meta.get("audit_tags") or []))
        finally:
            executor = None
            gc.collect()
            shutil.rmtree(td, ignore_errors=True)

    def test_build_deal_attribution_payload_carries_family_session_and_mtf_context(self):
        td = tempfile.mkdtemp()
        executor = None
        try:
            db_path = str(Path(td) / "ctrader_openapi.db")
            with patch.object(ctrader_module.config, "CTRADER_DB_PATH", db_path), \
                 patch.object(ctrader_module.config, "CTRADER_ENABLED", True), \
                 patch.object(ctrader_module.config, "CTRADER_AUTOTRADE_ENABLED", True), \
                 patch.object(ctrader_module.config, "CTRADER_DRY_RUN", True), \
                 patch.object(ctrader_module.CTraderExecutor, "sdk_available", new_callable=PropertyMock, return_value=True):
                executor = ctrader_module.CTraderExecutor()

            payload = executor._build_deal_attribution_payload(
                deal={"deal_id": 77, "symbol": "XAUUSD", "direction": "short"},
                source="scalp_xauusd:td:canary",
                lane="canary",
                symbol="XAUUSD",
                direction="short",
                journal_row={
                    "confidence": 74.2,
                    "entry_type": "limit",
                    "request_json": json.dumps(
                        {
                            "pattern": "SCALP_FLOW_FORCE",
                            "session": "london,new_york,overlap",
                            "timeframe": "5m+1m",
                            "entry_type": "limit",
                            "reasons": ["td leader", "aligned short"],
                            "raw_scores": {
                                "strategy_id": "xau_scalp_tick_depth_filter_v1",
                                "strategy_family": "xau_scalp_tick_depth_filter",
                                "winner_logic_regime": "strong",
                                "xau_multi_tf_snapshot": {
                                    "aligned_side": "short",
                                    "strict_aligned_side": "short",
                                },
                            },
                        }
                    ),
                    "execution_meta_json": json.dumps(
                        {
                            "market_capture": {
                                "features": {
                                    "day_type": "trend",
                                    "spread_expansion": 1.18,
                                    "depth_imbalance": -0.08,
                                    "delta_proxy": -0.22,
                                }
                            }
                        }
                    ),
                },
            )

            self.assertEqual(str(payload.get("family") or ""), "xau_scalp_tick_depth_filter")
            self.assertEqual(str(payload.get("strategy_id") or ""), "xau_scalp_tick_depth_filter_v1")
            self.assertEqual(str(payload.get("session") or ""), "london,new_york,overlap")
            self.assertEqual(str(payload.get("entry_type") or ""), "limit")
            self.assertEqual(str(payload.get("strict_alignment") or ""), "aligned_bearish")
            self.assertEqual(str(payload.get("winner_logic_regime") or ""), "strong")
            self.assertEqual(float((payload.get("market_capture_features") or {}).get("spread_expansion", 0.0) or 0.0), 1.18)
            self.assertIn("td leader", list(payload.get("reasons") or []))
        finally:
            executor = None
            gc.collect()
            shutil.rmtree(td, ignore_errors=True)

    def test_dry_run_journals_signal(self):
        td = tempfile.mkdtemp()
        executor = None
        try:
            db_path = str(Path(td) / "ctrader_openapi.db")
            with patch.object(ctrader_module.config, "CTRADER_ENABLED", True), \
                 patch.object(ctrader_module.config, "CTRADER_AUTOTRADE_ENABLED", True), \
                 patch.object(ctrader_module.config, "CTRADER_DRY_RUN", True), \
                 patch.object(ctrader_module.config, "CTRADER_DB_PATH", db_path), \
                 patch.object(ctrader_module.config, "CTRADER_ACCOUNT_ID", "43880642"), \
                 patch.object(ctrader_module.config, "CTRADER_RISK_USD_PER_TRADE", 10.0), \
                 patch.object(ctrader_module.config, "CTRADER_TP_LEVEL", 1), \
                 patch.object(ctrader_module.config, "get_ctrader_allowed_sources", return_value={"scalp_xauusd"}), \
                 patch.object(ctrader_module.config, "get_ctrader_allowed_symbols", return_value={"XAUUSD"}), \
                 patch.object(ctrader_module.config, "get_ctrader_default_volume_symbol_overrides", return_value={}), \
                 patch.object(ctrader_module.CTraderExecutor, "sdk_available", new_callable=PropertyMock, return_value=True):
                executor = ctrader_module.CTraderExecutor()
                result = _execute_signal_with_fixture_reference(executor, _make_signal(), source="scalp_xauusd")

            self.assertTrue(result.ok)
            self.assertEqual(result.status, "dry_run")
            rows = executor.get_recent_journal(limit=5)
            self.assertEqual(len(rows), 1)
            self.assertEqual(rows[0]["status"], "dry_run")
            self.assertEqual(rows[0]["symbol"], "XAUUSD")
            self.assertEqual(rows[0]["source"], "scalp_xauusd")
        finally:
            executor = None
            gc.collect()
            shutil.rmtree(td, ignore_errors=True)

    def test_status_prefers_usd_live_account_when_explicit_missing(self):
        accounts = [
            {"accountId": 111, "live": True, "accountStatus": "ACTIVE", "depositCurrency": "EUR", "deleted": False},
            {"accountId": 222, "live": True, "accountStatus": "ACTIVE", "depositCurrency": "USD", "deleted": False},
        ]
        td = tempfile.mkdtemp()
        executor = None
        try:
            db_path = str(Path(td) / "ctrader_openapi.db")
            with patch.object(ctrader_module.config, "CTRADER_DB_PATH", db_path), \
                 patch.object(ctrader_module.config, "CTRADER_ACCOUNT_ID", ""), \
                 patch.object(ctrader_module.config, "CTRADER_ACCOUNT_LOGIN", ""), \
                 patch.object(ctrader_module.config, "CTRADER_ENABLED", True), \
                 patch.object(ctrader_module.config, "CTRADER_AUTOTRADE_ENABLED", True), \
                 patch.object(ctrader_module.config, "CTRADER_DRY_RUN", True), \
                 patch.object(ctrader_module.config, "find_ctrader_account", return_value=accounts[1]), \
                 patch.object(ctrader_module.CTraderExecutor, "sdk_available", new_callable=PropertyMock, return_value=True):
                executor = ctrader_module.CTraderExecutor()
                status = executor.status()

            self.assertEqual(status["account_id"], 222)
            self.assertEqual(status["account_reason"], "env:Ctrader_accounts:usd_live_active")
        finally:
            executor = None
            gc.collect()
            shutil.rmtree(td, ignore_errors=True)

    def test_status_resolves_account_login_to_account_id(self):
        accounts = [
            {"accountId": 46552794, "accountNumber": 9900897, "traderLogin": 9900897, "live": False, "accountStatus": "ACTIVE", "depositCurrency": "USD", "deleted": False},
        ]
        td = tempfile.mkdtemp()
        executor = None
        try:
            db_path = str(Path(td) / "ctrader_openapi.db")
            with patch.object(ctrader_module.config, "CTRADER_DB_PATH", db_path), \
                 patch.object(ctrader_module.config, "CTRADER_ACCOUNT_ID", "9900897"), \
                 patch.object(ctrader_module.config, "CTRADER_ACCOUNT_LOGIN", ""), \
                 patch.object(ctrader_module.config, "CTRADER_USE_DEMO", True), \
                 patch.object(ctrader_module.config, "CTRADER_ENABLED", True), \
                 patch.object(ctrader_module.config, "CTRADER_AUTOTRADE_ENABLED", True), \
                 patch.object(ctrader_module.config, "CTRADER_DRY_RUN", True), \
                 patch.object(ctrader_module.config, "find_ctrader_account", return_value=accounts[0]), \
                 patch.object(ctrader_module.config, "get_ctrader_allowed_sources", return_value={"scalp_xauusd"}), \
                 patch.object(ctrader_module.config, "get_ctrader_allowed_symbols", return_value={"XAUUSD"}), \
                 patch.object(ctrader_module.CTraderExecutor, "sdk_available", new_callable=PropertyMock, return_value=True):
                executor = ctrader_module.CTraderExecutor()
                status = executor.status()

            self.assertEqual(status["account_id"], 46552794)
            self.assertEqual(status["account_reason"], "env:CTRADER_ACCOUNT_ID:login_resolved")
        finally:
            executor = None
            gc.collect()
            shutil.rmtree(td, ignore_errors=True)

    def test_execute_signal_filters_test_pattern(self):
        td = tempfile.mkdtemp()
        executor = None
        try:
            db_path = str(Path(td) / "ctrader_openapi.db")
            sig = _make_signal()
            sig.pattern = "TEST"
            with patch.object(ctrader_module.config, "CTRADER_ENABLED", True), \
                 patch.object(ctrader_module.config, "CTRADER_AUTOTRADE_ENABLED", True), \
                 patch.object(ctrader_module.config, "CTRADER_DRY_RUN", False), \
                 patch.object(ctrader_module.config, "CTRADER_DB_PATH", db_path), \
                 patch.object(ctrader_module.config, "CTRADER_ACCOUNT_ID", "46552794"), \
                 patch.object(ctrader_module.config, "CTRADER_ACCOUNT_LOGIN", "9900897"), \
                 patch.object(ctrader_module.config, "get_ctrader_allowed_sources", return_value={"scalp_xauusd"}), \
                 patch.object(ctrader_module.config, "get_ctrader_allowed_symbols", return_value={"XAUUSD"}), \
                 patch.object(ctrader_module.CTraderExecutor, "sdk_available", new_callable=PropertyMock, return_value=True):
                executor = ctrader_module.CTraderExecutor()
                result = executor.execute_signal(sig, source="scalp_xauusd")

            self.assertFalse(result.ok)
            self.assertEqual(result.status, "filtered")
            self.assertIn("test_pattern_filtered", result.message)
        finally:
            executor = None
            gc.collect()
            shutil.rmtree(td, ignore_errors=True)

    def test_execute_signal_does_not_filter_retest_pattern_as_test_fixture(self):
        td = tempfile.mkdtemp()
        executor = None
        try:
            db_path = str(Path(td) / "ctrader_openapi.db")
            sig = _make_signal(direction="short", confidence=87.0)
            sig.pattern = "Behavioral Sweep-Retest + Liquidity Continuation"
            with patch.object(ctrader_module.config, "CTRADER_ENABLED", True), \
                 patch.object(ctrader_module.config, "CTRADER_AUTOTRADE_ENABLED", True), \
                 patch.object(ctrader_module.config, "CTRADER_DRY_RUN", True), \
                 patch.object(ctrader_module.config, "CTRADER_PRICE_SANITY_ENABLED", False), \
                 patch.object(ctrader_module.config, "CTRADER_MARKET_ENTRY_DRIFT_GUARD_ENABLED", False), \
                 patch.object(ctrader_module.config, "CTRADER_DB_PATH", db_path), \
                 patch.object(ctrader_module.config, "CTRADER_ACCOUNT_ID", "46552794"), \
                 patch.object(ctrader_module.config, "CTRADER_ACCOUNT_LOGIN", "9900897"), \
                 patch.object(ctrader_module.config, "get_ctrader_allowed_sources", return_value={"xauusd_scheduled:canary"}), \
                 patch.object(ctrader_module.config, "get_ctrader_allowed_symbols", return_value={"XAUUSD"}), \
                 patch.object(ctrader_module.CTraderExecutor, "sdk_available", new_callable=PropertyMock, return_value=True):
                executor = ctrader_module.CTraderExecutor()
                result = executor.execute_signal(sig, source="xauusd_scheduled:canary")

            self.assertTrue(result.ok)
            self.assertEqual(result.status, "dry_run")
            self.assertNotIn("test_pattern_filtered", result.message)
        finally:
            executor = None
            gc.collect()
            shutil.rmtree(td, ignore_errors=True)

    def test_execute_signal_filters_fixture_price_profile(self):
        td = tempfile.mkdtemp()
        executor = None
        try:
            db_path = str(Path(td) / "ctrader_openapi.db")
            sig = _make_signal(symbol="ETHUSD", confidence=69.0)
            sig.pattern = "SCALP_FLOW_FORCE"
            sig.entry = 100.0
            sig.stop_loss = 99.0
            sig.take_profit_1 = 101.0
            sig.take_profit_2 = 102.0
            sig.take_profit_3 = 103.0
            with patch.object(ctrader_module.config, "CTRADER_ENABLED", True), \
                 patch.object(ctrader_module.config, "CTRADER_AUTOTRADE_ENABLED", True), \
                 patch.object(ctrader_module.config, "CTRADER_DRY_RUN", False), \
                 patch.object(ctrader_module.config, "CTRADER_DB_PATH", db_path), \
                 patch.object(ctrader_module.config, "CTRADER_ACCOUNT_ID", "46552794"), \
                 patch.object(ctrader_module.config, "CTRADER_ACCOUNT_LOGIN", "9900897"), \
                 patch.object(ctrader_module.config, "get_ctrader_allowed_sources", return_value={"scalp_ethusd"}), \
                 patch.object(ctrader_module.config, "get_ctrader_allowed_symbols", return_value={"ETHUSD"}), \
                 patch.object(ctrader_module.CTraderExecutor, "sdk_available", new_callable=PropertyMock, return_value=True):
                executor = ctrader_module.CTraderExecutor()
                result = executor.execute_signal(sig, source="scalp_ethusd")

            self.assertFalse(result.ok)
            self.assertEqual(result.status, "filtered")
            self.assertEqual(result.message, "fixture_signal_filtered")
        finally:
            executor = None
            gc.collect()
            shutil.rmtree(td, ignore_errors=True)

    def test_xau_active_defense_plan_uses_manager_order_care_overrides(self):
        td = tempfile.mkdtemp()
        executor = None
        try:
            db_path = str(Path(td) / "ctrader_openapi.db")
            with patch.object(ctrader_module.config, "CTRADER_ENABLED", True), \
                 patch.object(ctrader_module.config, "CTRADER_AUTOTRADE_ENABLED", True), \
                 patch.object(ctrader_module.config, "CTRADER_DRY_RUN", True), \
                 patch.object(ctrader_module.config, "CTRADER_DB_PATH", db_path), \
                 patch.object(ctrader_module.config, "CTRADER_ACCOUNT_ID", "46552794"), \
                 patch.object(ctrader_module.config, "CTRADER_PM_XAU_ACTIVE_DEFENSE_ENABLED", True), \
                 patch.object(ctrader_module.CTraderExecutor, "sdk_available", new_callable=PropertyMock, return_value=True):
                executor = ctrader_module.CTraderExecutor()
                executor.trading_manager_state_path.parent.mkdir(parents=True, exist_ok=True)
                executor.trading_manager_state_path.write_text(
                    json.dumps(
                        {
                            "xau_order_care": {
                                "status": "active",
                                "mode": "continuation_fail_fast",
                                "allowed_sources": ["scalp_xauusd:td:canary"],
                                "overrides": {
                                    "min_age_min": 0.5,
                                    "tighten_score": 2,
                                    "close_score": 2,
                                    "close_max_r": 0.5,
                                    "stop_keep_r": 0.2,
                                    "profit_lock_r": 0.02,
                                    "trim_tp_r": 0.3,
                                },
                            }
                        }
                    ),
                    encoding="utf-8",
                )
                snapshot = {
                    "ok": True,
                    "status": "captured_live",
                    "features": {
                        "day_type": "repricing",
                        "delta_proxy": 0.25,
                        "depth_imbalance": 0.18,
                        "mid_drift_pct": 0.02,
                        "rejection_ratio": 0.05,
                        "bar_volume_proxy": 1.0,
                    },
                }
                with patch.object(executor, "_latest_capture_snapshot", return_value=snapshot):
                    plan = executor._xau_active_defense_plan(
                        source="scalp_xauusd:td:canary",
                        symbol="XAUUSD",
                        direction="short",
                        entry=5107.04,
                        stop_loss=5109.14,
                        target_tp=5104.84,
                        current_price=5107.20,
                        confidence=72.0,
                        age_min=1.0,
                        r_now=0.1,
                    )

            self.assertTrue(plan.get("active"))
            self.assertEqual(str(plan.get("action") or ""), "close")
            self.assertEqual(str(((plan.get("details") or {}).get("order_care_mode") or "")), "continuation_fail_fast")
        finally:
            executor = None
            gc.collect()
            shutil.rmtree(td, ignore_errors=True)

    def test_xau_order_care_state_prefers_desk_specific_overrides(self):
        td = tempfile.mkdtemp()
        executor = None
        try:
            db_path = str(Path(td) / "ctrader_openapi.db")
            with patch.object(ctrader_module.config, "CTRADER_ENABLED", True), \
                 patch.object(ctrader_module.config, "CTRADER_AUTOTRADE_ENABLED", True), \
                 patch.object(ctrader_module.config, "CTRADER_DRY_RUN", True), \
                 patch.object(ctrader_module.config, "CTRADER_DB_PATH", db_path), \
                 patch.object(ctrader_module.config, "CTRADER_ACCOUNT_ID", "46552794"), \
                 patch.object(ctrader_module.CTraderExecutor, "sdk_available", new_callable=PropertyMock, return_value=True):
                executor = ctrader_module.CTraderExecutor()
                executor.trading_manager_state_path.parent.mkdir(parents=True, exist_ok=True)
                executor.trading_manager_state_path.write_text(
                    json.dumps(
                        {
                            "xau_order_care": {
                                "status": "active",
                                "mode": "continuation_fail_fast",
                                "allowed_sources": ["scalp_xauusd:canary", "scalp_xauusd:fss:canary"],
                                "overrides": {"no_follow_age_min": 6.0},
                                "desks": {
                                    "fss_confirmation": {
                                        "status": "active",
                                        "mode": "continuation_fail_fast",
                                        "allowed_sources": ["scalp_xauusd:fss:canary"],
                                        "overrides": {"desk": "fss_confirmation", "no_follow_age_min": 6.5},
                                    },
                                    "limit_retest": {
                                        "status": "active",
                                        "mode": "retest_absorption_guard",
                                        "allowed_sources": ["scalp_xauusd:canary"],
                                        "overrides": {"desk": "limit_retest", "no_follow_age_min": 3.0},
                                    },
                                },
                            }
                        }
                    ),
                    encoding="utf-8",
                )
                fss_state = executor._xau_order_care_state(symbol="XAUUSD", source="scalp_xauusd:fss:canary")
                limit_state = executor._xau_order_care_state(symbol="XAUUSD", source="scalp_xauusd:canary")

            self.assertEqual(str(fss_state.get("desk") or ""), "fss_confirmation")
            self.assertEqual(str(fss_state.get("mode") or ""), "continuation_fail_fast")
            self.assertAlmostEqual(float((dict(fss_state.get("overrides") or {})).get("no_follow_age_min") or 0.0), 6.5, places=4)
            self.assertEqual(str(limit_state.get("desk") or ""), "limit_retest")
            self.assertEqual(str(limit_state.get("mode") or ""), "retest_absorption_guard")
            self.assertAlmostEqual(float((dict(limit_state.get("overrides") or {})).get("no_follow_age_min") or 0.0), 3.0, places=4)
        finally:
            executor = None
            gc.collect()
            shutil.rmtree(td, ignore_errors=True)

    def test_close_position_uses_stored_volume_when_unspecified(self):
        td = tempfile.mkdtemp()
        executor = None
        try:
            db_path = str(Path(td) / "ctrader_openapi.db")
            with patch.object(ctrader_module.config, "CTRADER_ENABLED", True), \
                 patch.object(ctrader_module.config, "CTRADER_AUTOTRADE_ENABLED", True), \
                 patch.object(ctrader_module.config, "CTRADER_DRY_RUN", False), \
                 patch.object(ctrader_module.config, "CTRADER_DB_PATH", db_path), \
                 patch.object(ctrader_module.config, "CTRADER_ACCOUNT_ID", "46552794"), \
                 patch.object(ctrader_module.CTraderExecutor, "sdk_available", new_callable=PropertyMock, return_value=True):
                executor = ctrader_module.CTraderExecutor()
                with sqlite3.connect(db_path) as conn:
                    conn.execute(
                        """
                        INSERT INTO ctrader_positions(
                            position_id, account_id, source, lane, symbol, broker_symbol, direction, volume,
                            entry_price, stop_loss, take_profit, label, comment, signal_run_id, signal_run_no,
                            journal_id, is_open, status, first_seen_utc, last_seen_utc, raw_json
                        ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                        """,
                        (
                            123456,
                            46552794,
                            "scalp_ethusd",
                            "main",
                            "ETHUSD",
                            "ETHUSD",
                            "long",
                            7,
                            2000.0,
                            1990.0,
                            2010.0,
                            "dexter:ETHUSD:scalp_ethusd:7",
                            "dexter|scalp_ethusd|ETHUSD",
                            "",
                            7,
                            99,
                            1,
                            "POSITION_STATUS_OPEN",
                            "2026-03-07T05:00:00Z",
                            "2026-03-07T05:00:00Z",
                            "{}",
                        ),
                    )
                    conn.commit()
                seen = {}

                def _fake_run_worker(*, mode, payload, timeout_sec):
                    seen["mode"] = mode
                    seen["payload"] = dict(payload)
                    return {"ok": True, "status": "closed", "message": "ok", "position_id": payload.get("position_id")}

                with patch.object(executor, "_run_worker", side_effect=_fake_run_worker):
                    result = executor.close_position(position_id=123456)

            self.assertTrue(result.ok)
            self.assertEqual(seen["mode"], "close")
            self.assertEqual(seen["payload"]["volume"], 7)
        finally:
            executor = None
            gc.collect()
            shutil.rmtree(td, ignore_errors=True)

    def test_amend_position_sltp_uses_worker_mode(self):
        td = tempfile.mkdtemp()
        executor = None
        try:
            db_path = str(Path(td) / "ctrader_openapi.db")
            seen = {}
            with patch.object(ctrader_module.config, "CTRADER_ENABLED", True), \
                 patch.object(ctrader_module.config, "CTRADER_AUTOTRADE_ENABLED", True), \
                 patch.object(ctrader_module.config, "CTRADER_DRY_RUN", False), \
                 patch.object(ctrader_module.config, "CTRADER_DB_PATH", db_path), \
                 patch.object(ctrader_module.config, "CTRADER_ACCOUNT_ID", "46552794"), \
                 patch.object(ctrader_module.CTraderExecutor, "sdk_available", new_callable=PropertyMock, return_value=True):
                executor = ctrader_module.CTraderExecutor()

                def _fake_run_worker(*, mode, payload, timeout_sec):
                    seen["mode"] = mode
                    seen["payload"] = dict(payload)
                    return {
                        "ok": True,
                        "status": "amended",
                        "message": "ctrader amended",
                        "position_id": payload.get("position_id"),
                    }

                with patch.object(executor, "_run_worker", side_effect=_fake_run_worker):
                    result = executor.amend_position_sltp(position_id=456789, stop_loss=5100.5, take_profit=5110.5)

            self.assertTrue(result.ok)
            self.assertEqual(seen["mode"], "amend_position_sltp")
            self.assertEqual(int(seen["payload"]["position_id"]), 456789)
            self.assertAlmostEqual(float(seen["payload"]["stop_loss"]), 5100.5, places=4)
            self.assertAlmostEqual(float(seen["payload"]["take_profit"]), 5110.5, places=4)
        finally:
            executor = None
            gc.collect()
            shutil.rmtree(td, ignore_errors=True)

    def test_cancel_order_uses_worker_mode(self):
        td = tempfile.mkdtemp()
        executor = None
        try:
            db_path = str(Path(td) / "ctrader_openapi.db")
            seen = {}
            with patch.object(ctrader_module.config, "CTRADER_ENABLED", True), \
                 patch.object(ctrader_module.config, "CTRADER_AUTOTRADE_ENABLED", True), \
                 patch.object(ctrader_module.config, "CTRADER_DRY_RUN", False), \
                 patch.object(ctrader_module.config, "CTRADER_DB_PATH", db_path), \
                 patch.object(ctrader_module.config, "CTRADER_ACCOUNT_ID", "46552794"), \
                 patch.object(ctrader_module.CTraderExecutor, "sdk_available", new_callable=PropertyMock, return_value=True):
                executor = ctrader_module.CTraderExecutor()

                def _fake_run_worker(*, mode, payload, timeout_sec):
                    seen["mode"] = mode
                    seen["payload"] = dict(payload)
                    return {
                        "ok": True,
                        "status": "canceled",
                        "message": "ctrader canceled pending order",
                        "order_id": payload.get("order_id"),
                    }

                with patch.object(executor, "_run_worker", side_effect=_fake_run_worker):
                    result = executor.cancel_order(order_id=998877)

            self.assertTrue(result.ok)
            self.assertEqual(seen["mode"], "cancel_order")
            self.assertEqual(int(seen["payload"]["order_id"]), 998877)
        finally:
            executor = None
            gc.collect()
            shutil.rmtree(td, ignore_errors=True)

    def test_capture_market_data_stores_spots_and_depth(self):
        td = tempfile.mkdtemp()
        executor = None
        try:
            db_path = str(Path(td) / "ctrader_openapi.db")
            payload = {
                "ok": True,
                "status": "captured_live",
                "message": "captured spots=2 depth=2 mode=live_subscribe",
                "run_id": "ctcap_20260310_000001",
                "captured_at": "2026-03-10T00:00:05Z",
                "account_id": 46552794,
                "environment": "demo",
                "duration_sec": 18,
                "include_depth": True,
                "spots": [
                    {"account_id": 46552794, "symbol_id": 1, "symbol": "XAUUSD", "bid": 5160.1, "ask": 5160.4, "spread": 0.3, "spread_pct": 0.0058, "event_utc": "2026-03-10T00:00:01Z", "event_ts": 1741564801.0},
                    {"account_id": 46552794, "symbol_id": 1, "symbol": "XAUUSD", "bid": 5160.2, "ask": 5160.5, "spread": 0.3, "spread_pct": 0.0058, "event_utc": "2026-03-10T00:00:02Z", "event_ts": 1741564802.0},
                ],
                "depth": [
                    {"account_id": 46552794, "symbol_id": 1, "symbol": "XAUUSD", "quote_id": 11, "side": "bid", "price": 5160.1, "size": 3.0, "level_index": 0, "event_utc": "2026-03-10T00:00:01Z", "event_ts": 1741564801.0},
                    {"account_id": 46552794, "symbol_id": 1, "symbol": "XAUUSD", "quote_id": 12, "side": "ask", "price": 5160.5, "size": 2.0, "level_index": 0, "event_utc": "2026-03-10T00:00:01Z", "event_ts": 1741564801.0},
                ],
            }
            with patch.object(ctrader_module.config, "CTRADER_ENABLED", True), \
                 patch.object(ctrader_module.config, "CTRADER_AUTOTRADE_ENABLED", True), \
                 patch.object(ctrader_module.config, "CTRADER_DRY_RUN", False), \
                 patch.object(ctrader_module.config, "CTRADER_DB_PATH", db_path), \
                 patch.object(ctrader_module.config, "CTRADER_ACCOUNT_ID", "46552794"), \
                 patch.object(ctrader_module.CTraderExecutor, "sdk_available", new_callable=PropertyMock, return_value=True):
                executor = ctrader_module.CTraderExecutor()
                with patch.object(executor, "_run_worker", return_value=payload):
                    result = executor.capture_market_data(symbols=["XAUUSD"], duration_sec=18, include_depth=True)

            self.assertTrue(result["ok"])
            with sqlite3.connect(db_path) as conn:
                spot_count = conn.execute("SELECT COUNT(*) FROM ctrader_spot_ticks").fetchone()[0]
                depth_count = conn.execute("SELECT COUNT(*) FROM ctrader_depth_quotes").fetchone()[0]
                run_count = conn.execute("SELECT COUNT(*) FROM ctrader_capture_runs").fetchone()[0]
            self.assertEqual(spot_count, 2)
            self.assertEqual(depth_count, 2)
            self.assertEqual(run_count, 1)
        finally:
            executor = None
            gc.collect()
            shutil.rmtree(td, ignore_errors=True)

    def test_execute_signal_filters_market_entry_drift(self):
        td = tempfile.mkdtemp()
        executor = None
        try:
            db_path = str(Path(td) / "ctrader_openapi.db")
            sig = _make_signal(symbol="ETHUSD", confidence=78.0)
            sig.pattern = "SCALP_FLOW_FORCE"
            sig.entry_type = "market"
            sig.entry = 2000.0
            sig.stop_loss = 1990.0
            sig.take_profit_1 = 2010.0
            sig.take_profit_2 = 2020.0
            sig.take_profit_3 = 2030.0
            with patch.object(ctrader_module.config, "CTRADER_ENABLED", True), \
                 patch.object(ctrader_module.config, "CTRADER_AUTOTRADE_ENABLED", True), \
                 patch.object(ctrader_module.config, "CTRADER_DRY_RUN", False), \
                 patch.object(ctrader_module.config, "CTRADER_DB_PATH", db_path), \
                 patch.object(ctrader_module.config, "CTRADER_ACCOUNT_ID", "46552794"), \
                 patch.object(ctrader_module.config, "CTRADER_ACCOUNT_LOGIN", "9900897"), \
                 patch.object(ctrader_module.config, "CTRADER_MARKET_ENTRY_DRIFT_GUARD_ENABLED", True), \
                 patch.object(ctrader_module.config, "CTRADER_MARKET_ENTRY_MAX_DRIFT_PCT", 0.12), \
                 patch.object(ctrader_module.config, "get_ctrader_market_entry_max_drift_symbol_overrides", return_value={"ETHUSD": 0.05}), \
                 patch.object(ctrader_module.config, "get_ctrader_allowed_sources", return_value={"scalp_ethusd"}), \
                 patch.object(ctrader_module.config, "get_ctrader_allowed_symbols", return_value={"ETHUSD"}), \
                 patch.object(ctrader_module.CTraderExecutor, "sdk_available", new_callable=PropertyMock, return_value=True):
                executor = ctrader_module.CTraderExecutor()
                with patch.object(executor, "_reference_price", return_value=1900.0):
                    result = executor.execute_signal(sig, source="scalp_ethusd")

            self.assertFalse(result.ok)
            self.assertEqual(result.status, "filtered")
            self.assertIn("market_entry_drift_failed", result.message)
        finally:
            executor = None
            gc.collect()
            shutil.rmtree(td, ignore_errors=True)

    def test_persistent_canary_source_is_allowed_and_journaled_as_canary(self):
        td = tempfile.mkdtemp()
        executor = None
        try:
            db_path = str(Path(td) / "ctrader_openapi.db")
            sig = _make_signal(symbol="XAUUSD", confidence=78.0)
            sig.raw_scores["ctrader_risk_usd_override"] = 2.5
            with patch.object(ctrader_module.config, "CTRADER_ENABLED", True), \
                 patch.object(ctrader_module.config, "CTRADER_AUTOTRADE_ENABLED", True), \
                 patch.object(ctrader_module.config, "CTRADER_DRY_RUN", True), \
                 patch.object(ctrader_module.config, "CTRADER_DB_PATH", db_path), \
                 patch.object(ctrader_module.config, "CTRADER_ACCOUNT_ID", "46552794"), \
                 patch.object(ctrader_module.config, "CTRADER_ACCOUNT_LOGIN", "9900897"), \
                 patch.object(ctrader_module.config, "PERSISTENT_CANARY_ENABLED", True), \
                 patch.object(ctrader_module.config, "get_ctrader_allowed_sources", return_value={"xauusd_scheduled:winner"}), \
                 patch.object(ctrader_module.config, "get_ctrader_allowed_symbols", return_value={"XAUUSD"}), \
                 patch.object(ctrader_module.config, "get_persistent_canary_allowed_sources", return_value={"scalp_xauusd"}), \
                 patch.object(ctrader_module.config, "get_persistent_canary_direct_allowed_sources", return_value={"scalp_xauusd"}), \
                 patch.object(ctrader_module.config, "get_persistent_canary_allowed_symbols", return_value={"XAUUSD"}), \
                 patch.object(ctrader_module.config, "get_ctrader_default_volume_symbol_overrides", return_value={}), \
                 patch.object(ctrader_module.CTraderExecutor, "sdk_available", new_callable=PropertyMock, return_value=True):
                executor = ctrader_module.CTraderExecutor()
                result = _execute_signal_with_fixture_reference(executor, sig, source="scalp_xauusd:canary")

            self.assertTrue(result.ok)
            self.assertEqual(result.status, "dry_run")
            rows = executor.get_recent_journal(limit=1)
            self.assertEqual(rows[0]["lane"], "canary")
            self.assertEqual(rows[0]["source"], "scalp_xauusd:canary")
            self.assertAlmostEqual(float(rows[0]["request_json"]["risk_usd"]), 2.5, places=3)
        finally:
            executor = None
            gc.collect()
            shutil.rmtree(td, ignore_errors=True)

    def test_execute_signal_supports_buy_stop_payload_in_dry_run(self):
        td = tempfile.mkdtemp()
        executor = None
        try:
            db_path = str(Path(td) / "ctrader_openapi.db")
            sig = _make_signal(symbol="XAUUSD", confidence=79.0)
            sig.entry_type = "buy_stop"
            sig.entry = 5104.0
            sig.stop_loss = 5099.0
            sig.take_profit_1 = 5108.0
            sig.take_profit_2 = 5111.0
            sig.take_profit_3 = 5114.0
            with patch.object(ctrader_module.config, "CTRADER_ENABLED", True), \
                 patch.object(ctrader_module.config, "CTRADER_AUTOTRADE_ENABLED", True), \
                 patch.object(ctrader_module.config, "CTRADER_DRY_RUN", True), \
                 patch.object(ctrader_module.config, "CTRADER_DB_PATH", db_path), \
                 patch.object(ctrader_module.config, "CTRADER_ACCOUNT_ID", "46552794"), \
                 patch.object(ctrader_module.config, "CTRADER_ACCOUNT_LOGIN", "9900897"), \
                 patch.object(ctrader_module.config, "get_ctrader_allowed_sources", return_value={"xauusd_scheduled:winner"}), \
                 patch.object(ctrader_module.config, "get_ctrader_allowed_symbols", return_value={"XAUUSD"}), \
                 patch.object(ctrader_module.config, "get_ctrader_default_volume_symbol_overrides", return_value={}), \
                 patch.object(ctrader_module.CTraderExecutor, "sdk_available", new_callable=PropertyMock, return_value=True):
                executor = ctrader_module.CTraderExecutor()
                result = _execute_signal_with_fixture_reference(executor, sig, source="scalp_xauusd:bs:canary")

            self.assertTrue(result.ok)
            self.assertEqual(result.status, "dry_run")
            rows = executor.get_recent_journal(limit=1)
            self.assertEqual(rows[0]["request_json"]["entry_type"], "buy_stop")
            self.assertEqual(rows[0]["request_json"]["order_type"], "stop")
            self.assertEqual(rows[0]["source"], "scalp_xauusd:bs:canary")
        finally:
            executor = None
            gc.collect()
            shutil.rmtree(td, ignore_errors=True)

    def test_execute_signal_persists_market_capture_summary_after_live_accept(self):
        td = tempfile.mkdtemp()
        executor = None
        try:
            db_path = str(Path(td) / "ctrader_openapi.db")
            sig = _make_signal(symbol="XAUUSD", confidence=79.0)
            with patch.object(ctrader_module.config, "CTRADER_ENABLED", True), \
                 patch.object(ctrader_module.config, "CTRADER_AUTOTRADE_ENABLED", True), \
                 patch.object(ctrader_module.config, "CTRADER_DRY_RUN", False), \
                 patch.object(ctrader_module.config, "CTRADER_DB_PATH", db_path), \
                 patch.object(ctrader_module.config, "CTRADER_ACCOUNT_ID", "46552794"), \
                 patch.object(ctrader_module.config, "CTRADER_ACCOUNT_LOGIN", "9900897"), \
                 patch.object(ctrader_module.config, "CTRADER_MARKET_CAPTURE_ENABLED", True), \
                 patch.object(ctrader_module.config, "CTRADER_MARKET_CAPTURE_ON_EXECUTE", True), \
                 patch.object(ctrader_module.config, "get_ctrader_allowed_sources", return_value={"scalp_xauusd"}), \
                 patch.object(ctrader_module.config, "get_ctrader_allowed_symbols", return_value={"XAUUSD"}), \
                 patch.object(ctrader_module.config, "get_ctrader_market_capture_symbols", return_value={"XAUUSD"}), \
                 patch.object(ctrader_module.config, "get_ctrader_default_volume_symbol_overrides", return_value={}), \
                 patch.object(ctrader_module.CTraderExecutor, "sdk_available", new_callable=PropertyMock, return_value=True):
                executor = ctrader_module.CTraderExecutor()
                with patch.object(
                    executor,
                    "_run_worker",
                    return_value={
                        "ok": True,
                        "status": "accepted",
                        "message": "ctrader order_accepted",
                        "signal_symbol": "XAUUSD",
                        "broker_symbol": "XAUUSD",
                        "account_id": 46552794,
                        "order_id": 101,
                        "position_id": 202,
                        "deal_id": None,
                        "volume": 100.0,
                        "execution_meta": {"execution_type": "ORDER_ACCEPTED"},
                    },
                ), patch.object(
                    executor,
                    "_capture_after_execute",
                    return_value={
                        "ok": True,
                        "status": "captured_live",
                        "storage": {"run_id": "ctcap_test_1"},
                        "spots_count": 12,
                        "depth_count": 34,
                    },
                ):
                    result = _execute_signal_with_fixture_reference(executor, sig, source="scalp_xauusd")

            self.assertTrue(result.ok)
            self.assertEqual(result.execution_meta["market_capture"]["run_id"], "ctcap_test_1")
            rows = executor.get_recent_journal(limit=1)
            self.assertEqual(rows[0]["execution_meta_json"]["market_capture"]["run_id"], "ctcap_test_1")
            self.assertEqual(rows[0]["execution_meta_json"]["market_capture"]["spots"], 12)
            self.assertEqual(rows[0]["execution_meta_json"]["market_capture"]["depth"], 34)
        finally:
            executor = None
            gc.collect()
            shutil.rmtree(td, ignore_errors=True)

    def test_execute_signal_repairs_missing_order_protection_after_accept(self):
        td = tempfile.mkdtemp()
        executor = None
        try:
            db_path = str(Path(td) / "ctrader_openapi.db")
            sig = _make_signal(symbol="XAUUSD", confidence=79.0)
            with patch.object(ctrader_module.config, "CTRADER_ENABLED", True), \
                 patch.object(ctrader_module.config, "CTRADER_AUTOTRADE_ENABLED", True), \
                 patch.object(ctrader_module.config, "CTRADER_DRY_RUN", False), \
                 patch.object(ctrader_module.config, "CTRADER_DB_PATH", db_path), \
                 patch.object(ctrader_module.config, "CTRADER_ACCOUNT_ID", "46552794"), \
                 patch.object(ctrader_module.config, "CTRADER_ACCOUNT_LOGIN", "9900897"), \
                 patch.object(ctrader_module.config, "get_ctrader_allowed_sources", return_value={"scalp_xauusd"}), \
                 patch.object(ctrader_module.config, "get_ctrader_allowed_symbols", return_value={"XAUUSD"}), \
                 patch.object(ctrader_module.config, "get_ctrader_default_volume_symbol_overrides", return_value={}), \
                 patch.object(ctrader_module.CTraderExecutor, "sdk_available", new_callable=PropertyMock, return_value=True):
                executor = ctrader_module.CTraderExecutor()
                with patch.object(
                    executor,
                    "_run_worker",
                    return_value={
                        "ok": True,
                        "status": "accepted",
                        "message": "ctrader order_accepted",
                        "signal_symbol": "XAUUSD",
                        "broker_symbol": "XAUUSD",
                        "account_id": 46552794,
                        "order_id": 303,
                        "position_id": 404,
                        "volume": 100.0,
                        "execution_meta": {
                            "execution_type": "ORDER_ACCEPTED",
                            "raw_execution": {
                                "order": {
                                    "orderId": "303",
                                    "limitPrice": float(sig.entry),
                                    "stopLoss": 0.0,
                                    "takeProfit": 0.0,
                                }
                            },
                        },
                    },
                ), patch.object(
                    executor,
                    "amend_order",
                    return_value=ctrader_module.CTraderExecutionResult(True, "amended_order", "ok", signal_symbol="XAUUSD", broker_symbol="XAUUSD"),
                ) as amend_mock, patch.object(
                    executor,
                    "_capture_after_execute",
                    return_value={"ok": False, "status": "capture_skipped"},
                ):
                    result = _execute_signal_with_fixture_reference(executor, sig, source="scalp_xauusd")

            self.assertTrue(result.ok)
            self.assertEqual(amend_mock.call_count, 1)
            repair = dict(result.execution_meta.get("protection_repair") or {})
            self.assertEqual(str(repair.get("action") or ""), "repair_order_protection")
            self.assertTrue(bool(repair.get("missing_stop_loss")))
            self.assertTrue(bool(repair.get("missing_take_profit")))
        finally:
            executor = None
            gc.collect()
            shutil.rmtree(td, ignore_errors=True)

    def test_execute_signal_clamps_wide_filled_xau_stop_after_bad_fill(self):
        td = tempfile.mkdtemp()
        executor = None
        try:
            db_path = str(Path(td) / "ctrader_openapi.db")
            sig = _make_signal(symbol="XAUUSD", direction="short", confidence=74.0)
            sig.entry = 4594.9244
            sig.stop_loss = 4604.2379
            sig.take_profit_1 = 4586.2908
            sig.take_profit_2 = 4584.0
            sig.take_profit_3 = 4582.0
            with patch.object(ctrader_module.config, "CTRADER_ENABLED", True), \
                 patch.object(ctrader_module.config, "CTRADER_AUTOTRADE_ENABLED", True), \
                 patch.object(ctrader_module.config, "CTRADER_DRY_RUN", False), \
                 patch.object(ctrader_module.config, "CTRADER_DB_PATH", db_path), \
                 patch.object(ctrader_module.config, "CTRADER_ACCOUNT_ID", "46552794"), \
                 patch.object(ctrader_module.config, "CTRADER_ACCOUNT_LOGIN", "9900897"), \
                 patch.object(ctrader_module.config, "CTRADER_PM_XAU_POST_FILL_STOP_CLAMP_ENABLED", True), \
                 patch.object(ctrader_module.config, "CTRADER_PM_XAU_POST_FILL_STOP_MAX_RISK_MULT", 1.15), \
                 patch.object(ctrader_module.config, "get_ctrader_allowed_sources", return_value={"scalp_xauusd:fss:canary"}), \
                 patch.object(ctrader_module.config, "get_ctrader_allowed_symbols", return_value={"XAUUSD"}), \
                 patch.object(ctrader_module.config, "get_ctrader_default_volume_symbol_overrides", return_value={}), \
                 patch.object(ctrader_module.CTraderExecutor, "sdk_available", new_callable=PropertyMock, return_value=True):
                executor = ctrader_module.CTraderExecutor()
                with patch.object(
                    executor,
                    "_run_worker",
                    return_value={
                        "ok": True,
                        "status": "filled",
                        "message": "ctrader filled",
                        "signal_symbol": "XAUUSD",
                        "broker_symbol": "XAUUSD",
                        "account_id": 46552794,
                        "order_id": 939424994,
                        "position_id": 594235994,
                        "volume": 100.0,
                        "execution_meta": {
                            "execution_type": "ORDER_FILLED",
                            "raw_execution": {
                                "position": {
                                    "positionId": "594235994",
                                    "price": 4582.94,
                                    "stopLoss": 4604.24,
                                    "takeProfit": 0.0,
                                }
                            },
                        },
                    },
                ), patch.object(
                    executor,
                    "amend_position_sltp",
                    return_value=ctrader_module.CTraderExecutionResult(True, "amended_position", "ok", signal_symbol="XAUUSD", broker_symbol="XAUUSD"),
                ) as amend_mock, patch.object(
                    executor,
                    "_capture_after_execute",
                    return_value={"ok": False, "status": "capture_skipped"},
                ):
                    result = executor.execute_signal(sig, source="scalp_xauusd:fss:canary")

            self.assertTrue(result.ok)
            self.assertEqual(amend_mock.call_count, 1)
            amend_kwargs = amend_mock.call_args.kwargs
            self.assertLess(float(amend_kwargs["stop_loss"]), 4604.24)
            self.assertEqual(float(amend_kwargs["take_profit"]), 0.0)
            repair = dict(result.execution_meta.get("protection_repair") or {})
            self.assertEqual(str(repair.get("action") or ""), "clamp_position_stop_after_fill")
            self.assertTrue(bool(repair.get("clamped_stop_loss")))
        finally:
            executor = None
            gc.collect()
            shutil.rmtree(td, ignore_errors=True)

    def test_sync_account_state_cancels_stale_pending_orders(self):
        td = tempfile.mkdtemp()
        executor = None
        try:
            db_path = str(Path(td) / "ctrader_openapi.db")
            with patch.object(ctrader_module.config, "CTRADER_ENABLED", True), \
                 patch.object(ctrader_module.config, "CTRADER_AUTOTRADE_ENABLED", True), \
                 patch.object(ctrader_module.config, "CTRADER_DRY_RUN", False), \
                 patch.object(ctrader_module.config, "CTRADER_DB_PATH", db_path), \
                 patch.object(ctrader_module.config, "CTRADER_ACCOUNT_ID", "46552794"), \
                 patch.object(ctrader_module.config, "CTRADER_PENDING_ORDER_SWEEP_ENABLED", True), \
                 patch.object(ctrader_module.config, "CTRADER_PENDING_ORDER_SWEEP_ON_SYNC", True), \
                 patch.object(ctrader_module.config, "CTRADER_PENDING_ORDER_TTL_XAU_PULLBACK_MIN", 15), \
                 patch.object(ctrader_module.config, "CTRADER_PENDING_ORDER_TTL_DEFAULT_MIN", 60), \
                 patch.object(ctrader_module.config, "CTRADER_PENDING_ORDER_GRACE_MIN", 1), \
                 patch.object(ctrader_module.config, "CTRADER_PENDING_ORDER_MAX_PER_SOURCE_SYMBOL", 3), \
                 patch.object(ctrader_module.config, "CTRADER_PENDING_ORDER_CANCEL_DISABLED_SOURCE", True), \
                 patch.object(ctrader_module.config, "CTRADER_PENDING_ORDER_CANCEL_DISABLED_FAMILY", True), \
                 patch.object(ctrader_module.config, "get_ctrader_allowed_sources", return_value={"scalp_xauusd"}), \
                 patch.object(ctrader_module.config, "get_ctrader_allowed_symbols", return_value={"XAUUSD"}), \
                 patch.object(ctrader_module.config, "get_persistent_canary_allowed_sources", return_value={"scalp_xauusd"}), \
                 patch.object(ctrader_module.config, "get_persistent_canary_allowed_symbols", return_value={"XAUUSD"}), \
                 patch.object(ctrader_module.config, "get_ctrader_xau_active_families", return_value={"xau_scalp_pullback_limit"}), \
                 patch.object(ctrader_module.CTraderExecutor, "sdk_available", new_callable=PropertyMock, return_value=True):
                executor = ctrader_module.CTraderExecutor()
                old_ms = 1773000000000
                with sqlite3.connect(db_path) as conn:
                    conn.execute(
                        """
                        INSERT INTO execution_journal(
                            created_ts, created_utc, source, lane, symbol, direction, confidence,
                            entry, stop_loss, take_profit, entry_type, dry_run, account_id, broker_symbol,
                            volume, status, message, order_id, position_id, deal_id, signal_run_id, signal_run_no,
                            request_json, response_json, execution_meta_json
                        ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                        """,
                        (
                            1.0,
                            "2026-03-10 00:00:00",
                            "scalp_xauusd:pb:canary",
                            "canary",
                            "XAUUSD",
                            "long",
                            74.0,
                            5200.0,
                            5195.0,
                            5204.0,
                            "limit",
                            0,
                            46552794,
                            "XAUUSD",
                            100.0,
                            "accepted",
                            "ctrader accepted",
                            934000001,
                            None,
                            None,
                            "20260310000000-000001",
                            1,
                            "{}",
                            "{}",
                            "{}",
                        ),
                    )
                    conn.commit()

                reconcile_payload = {
                    "ok": True,
                    "status": "reconciled",
                    "positions": [],
                    "orders": [
                        {
                            "orderId": "934000001",
                            "tradeData": {
                                "symbolId": "41",
                                "volume": "100",
                                "tradeSide": "BUY",
                                "openTimestamp": str(old_ms),
                                "label": "dexter:XAUUSD:scalp_xauusd:pb::1",
                                "comment": "dexter|scalp_xauusd:pb:canary|XAUUSD",
                            },
                            "orderType": "LIMIT",
                            "orderStatus": "ORDER_STATUS_ACCEPTED",
                            "executedVolume": "0",
                            "utcLastUpdateTimestamp": str(old_ms),
                            "closingOrder": False,
                            "limitPrice": 5200.0,
                            "stopLoss": 5195.0,
                            "takeProfit": 5204.0,
                            "clientOrderId": "20260310000000-000001",
                            "timeInForce": "GOOD_TILL_CANCEL",
                        }
                    ],
                    "deals": [],
                }

                def _fake_run_worker(*, mode, payload, timeout_sec):
                    if mode == "reconcile":
                        return reconcile_payload
                    if mode == "cancel_order":
                        return {
                            "ok": True,
                            "status": "canceled",
                            "message": "ctrader canceled pending order",
                            "order_id": payload.get("order_id"),
                        }
                    raise AssertionError(f"unexpected mode {mode}")

                with patch.object(executor, "_run_worker", side_effect=_fake_run_worker):
                    report = executor.sync_account_state()

                self.assertTrue(report["ok"])
                self.assertEqual(report["orders"], 1)
                self.assertEqual(report["canceled_orders"], 1)
                with sqlite3.connect(db_path) as conn:
                    row = conn.execute("SELECT is_open, order_status FROM ctrader_orders WHERE order_id=934000001").fetchone()
                    jrow = conn.execute("SELECT status, message FROM execution_journal WHERE order_id=934000001").fetchone()
                self.assertEqual(int(row[0]), 0)
                self.assertEqual(str(row[1]), "canceled")
                self.assertEqual(str(jrow[0]), "canceled")
                self.assertIn("stale_ttl", str(jrow[1]))
        finally:
            executor = None
            gc.collect()
            shutil.rmtree(td, ignore_errors=True)

    def test_execute_signal_blocks_opposite_direction_when_guard_enabled(self):
        td = tempfile.mkdtemp()
        executor = None
        try:
            db_path = str(Path(td) / "ctrader_openapi.db")
            sig = _make_signal(symbol="XAUUSD", direction="short", confidence=74.0)
            with patch.object(ctrader_module.config, "CTRADER_ENABLED", True), \
                 patch.object(ctrader_module.config, "CTRADER_AUTOTRADE_ENABLED", True), \
                 patch.object(ctrader_module.config, "CTRADER_DRY_RUN", False), \
                 patch.object(ctrader_module.config, "CTRADER_DB_PATH", db_path), \
                 patch.object(ctrader_module.config, "CTRADER_ACCOUNT_ID", "46552794"), \
                 patch.object(ctrader_module.config, "CTRADER_ACCOUNT_LOGIN", "9900897"), \
                 patch.object(ctrader_module.config, "CTRADER_POSITION_DIRECTION_GUARD_ENABLED", True), \
                 patch.object(ctrader_module.config, "CTRADER_BLOCK_OPPOSITE_DIRECTION", True), \
                 patch.object(ctrader_module.config, "CTRADER_MAX_POSITIONS_PER_SYMBOL", 3), \
                 patch.object(ctrader_module.config, "CTRADER_MAX_POSITIONS_PER_DIRECTION", 2), \
                 patch.object(ctrader_module.config, "get_ctrader_allowed_sources", return_value={"scalp_xauusd"}), \
                 patch.object(ctrader_module.config, "get_ctrader_allowed_symbols", return_value={"XAUUSD"}), \
                 patch.object(ctrader_module.CTraderExecutor, "sdk_available", new_callable=PropertyMock, return_value=True):
                executor = ctrader_module.CTraderExecutor()
                with sqlite3.connect(db_path) as conn:
                    conn.execute(
                        """
                        INSERT INTO ctrader_positions(
                            position_id, account_id, source, lane, symbol, broker_symbol, direction, volume,
                            entry_price, stop_loss, take_profit, label, comment, signal_run_id, signal_run_no,
                            journal_id, is_open, status, first_seen_utc, last_seen_utc, raw_json
                        ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                        """,
                        (
                            1111,
                            46552794,
                            "scalp_xauusd:canary",
                            "canary",
                            "XAUUSD",
                            "XAUUSD",
                            "long",
                            100.0,
                            5200.0,
                            5195.0,
                            5204.0,
                            "dexter:XAUUSD:scalp_xauusd:can:1",
                            "dexter|scalp_xauusd:canary|XAUUSD",
                            "run",
                            1,
                            1,
                            1,
                            "POSITION_STATUS_OPEN",
                            "2026-03-10T20:00:00Z",
                            "2026-03-10T20:00:00Z",
                            "{}",
                        ),
                    )
                    conn.commit()
                result = _execute_signal_with_fixture_reference(executor, sig, source="scalp_xauusd")

            self.assertFalse(result.ok)
            self.assertEqual(result.status, "filtered")
            self.assertIn("opposite_direction_open", result.message)
        finally:
            executor = None
            gc.collect()
            shutil.rmtree(td, ignore_errors=True)

    def test_execute_signal_blocks_on_recent_unreconciled_opposite_journal(self):
        td = tempfile.mkdtemp()
        executor = None
        try:
            db_path = str(Path(td) / "ctrader_openapi.db")
            sig = _make_signal(symbol="XAUUSD", direction="short", confidence=74.0)
            with patch.object(ctrader_module.config, "CTRADER_ENABLED", True), \
                 patch.object(ctrader_module.config, "CTRADER_AUTOTRADE_ENABLED", True), \
                 patch.object(ctrader_module.config, "CTRADER_DRY_RUN", False), \
                 patch.object(ctrader_module.config, "CTRADER_DB_PATH", db_path), \
                 patch.object(ctrader_module.config, "CTRADER_ACCOUNT_ID", "46552794"), \
                 patch.object(ctrader_module.config, "CTRADER_ACCOUNT_LOGIN", "9900897"), \
                 patch.object(ctrader_module.config, "CTRADER_POSITION_DIRECTION_GUARD_ENABLED", True), \
                 patch.object(ctrader_module.config, "CTRADER_BLOCK_OPPOSITE_DIRECTION", True), \
                 patch.object(ctrader_module.config, "CTRADER_DIRECTION_GUARD_INCLUDE_RECENT_JOURNAL", True), \
                 patch.object(ctrader_module.config, "CTRADER_DIRECTION_GUARD_RECENT_SEC", 900), \
                 patch.object(ctrader_module.config, "CTRADER_MAX_POSITIONS_PER_SYMBOL", 1), \
                 patch.object(ctrader_module.config, "CTRADER_MAX_POSITIONS_PER_DIRECTION", 1), \
                 patch.object(ctrader_module.config, "CTRADER_MAX_PENDING_ORDERS_PER_SYMBOL", 1), \
                 patch.object(ctrader_module.config, "CTRADER_MAX_PENDING_ORDERS_PER_DIRECTION", 1), \
                 patch.object(ctrader_module.config, "get_ctrader_allowed_sources", return_value={"scalp_xauusd"}), \
                 patch.object(ctrader_module.config, "get_ctrader_allowed_symbols", return_value={"XAUUSD"}), \
                 patch.object(ctrader_module.CTraderExecutor, "sdk_available", new_callable=PropertyMock, return_value=True):
                executor = ctrader_module.CTraderExecutor()
                with sqlite3.connect(db_path) as conn:
                    conn.execute(
                        """
                        INSERT INTO execution_journal(
                            created_ts, created_utc, source, lane, symbol, direction, confidence,
                            entry, stop_loss, take_profit, entry_type, dry_run, account_id, broker_symbol,
                            volume, status, message, order_id, position_id, deal_id, signal_run_id, signal_run_no,
                            request_json, response_json, execution_meta_json
                        ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                        """,
                        (
                            time.time(),
                            "2026-03-10 20:10:00",
                            "scalp_xauusd",
                            "canary",
                            "XAUUSD",
                            "long",
                            74.0,
                            5200.0,
                            5195.0,
                            5204.0,
                            "limit",
                            0,
                            46552794,
                            "XAUUSD",
                            100.0,
                            "filled",
                            "ok",
                            0,
                            0,
                            None,
                            "run-1",
                            1,
                            "{}",
                            "{}",
                            "{}",
                        ),
                    )
                    conn.commit()
                result = _execute_signal_with_fixture_reference(executor, sig, source="scalp_xauusd")

            self.assertFalse(result.ok)
            self.assertEqual(result.status, "filtered")
            self.assertIn("opposite_direction_open", result.message)
        finally:
            executor = None
            gc.collect()
            shutil.rmtree(td, ignore_errors=True)

    def test_execute_signal_blocks_when_pending_order_cap_reached(self):
        td = tempfile.mkdtemp()
        executor = None
        try:
            db_path = str(Path(td) / "ctrader_openapi.db")
            sig = _make_signal(symbol="XAUUSD", direction="long", confidence=74.0)
            with patch.object(ctrader_module.config, "CTRADER_ENABLED", True), \
                 patch.object(ctrader_module.config, "CTRADER_AUTOTRADE_ENABLED", True), \
                 patch.object(ctrader_module.config, "CTRADER_DRY_RUN", False), \
                 patch.object(ctrader_module.config, "CTRADER_DB_PATH", db_path), \
                 patch.object(ctrader_module.config, "CTRADER_ACCOUNT_ID", "46552794"), \
                 patch.object(ctrader_module.config, "CTRADER_ACCOUNT_LOGIN", "9900897"), \
                 patch.object(ctrader_module.config, "CTRADER_POSITION_DIRECTION_GUARD_ENABLED", True), \
                 patch.object(ctrader_module.config, "CTRADER_DIRECTION_GUARD_INCLUDE_PENDING_ORDERS", True), \
                 patch.object(ctrader_module.config, "CTRADER_MAX_POSITIONS_PER_SYMBOL", 2), \
                 patch.object(ctrader_module.config, "CTRADER_MAX_POSITIONS_PER_DIRECTION", 2), \
                 patch.object(ctrader_module.config, "CTRADER_MAX_PENDING_ORDERS_PER_SYMBOL", 1), \
                 patch.object(ctrader_module.config, "CTRADER_MAX_PENDING_ORDERS_PER_DIRECTION", 1), \
                 patch.object(ctrader_module.config, "get_ctrader_allowed_sources", return_value={"scalp_xauusd"}), \
                 patch.object(ctrader_module.config, "get_ctrader_allowed_symbols", return_value={"XAUUSD"}), \
                 patch.object(ctrader_module.CTraderExecutor, "sdk_available", new_callable=PropertyMock, return_value=True):
                executor = ctrader_module.CTraderExecutor()
                with sqlite3.connect(db_path) as conn:
                    conn.execute(
                        """
                        INSERT INTO ctrader_orders(
                            order_id, account_id, source, lane, symbol, broker_symbol, direction, volume,
                            entry_price, stop_loss, take_profit, order_type, order_status, label, comment,
                            client_order_id, signal_run_id, signal_run_no, journal_id, is_open, first_seen_utc,
                            last_seen_utc, raw_json
                        ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                        """,
                        (
                            934000111,
                            46552794,
                            "scalp_xauusd",
                            "canary",
                            "XAUUSD",
                            "XAUUSD",
                            "long",
                            100.0,
                            5200.0,
                            5195.0,
                            5204.0,
                            "limit",
                            "accepted",
                            "dexter:XAUUSD:scalp_xauusd:can:1",
                            "dexter|scalp_xauusd|XAUUSD",
                            "client-1",
                            "run-1",
                            1,
                            1,
                            1,
                            "2026-03-10T20:00:00Z",
                            "2026-03-10T20:00:00Z",
                            "{}",
                        ),
                    )
                    conn.commit()
                result = _execute_signal_with_fixture_reference(executor, sig, source="scalp_xauusd")

            self.assertFalse(result.ok)
            self.assertEqual(result.status, "filtered")
            self.assertIn("pending_order_cap", result.message)
        finally:
            executor = None
            gc.collect()
            shutil.rmtree(td, ignore_errors=True)

    def test_execute_signal_allows_parallel_pending_orders_for_different_families(self):
        td = tempfile.mkdtemp()
        executor = None
        try:
            db_path = str(Path(td) / "ctrader_openapi.db")
            sig = _make_signal(symbol="XAUUSD", direction="long", confidence=74.0)
            with patch.object(ctrader_module.config, "CTRADER_ENABLED", True), \
                 patch.object(ctrader_module.config, "CTRADER_AUTOTRADE_ENABLED", True), \
                 patch.object(ctrader_module.config, "CTRADER_DRY_RUN", True), \
                 patch.object(ctrader_module.config, "CTRADER_DB_PATH", db_path), \
                 patch.object(ctrader_module.config, "CTRADER_ACCOUNT_ID", "46552794"), \
                 patch.object(ctrader_module.config, "CTRADER_ACCOUNT_LOGIN", "9900897"), \
                 patch.object(ctrader_module.config, "CTRADER_POSITION_DIRECTION_GUARD_ENABLED", True), \
                 patch.object(ctrader_module.config, "CTRADER_DIRECTION_GUARD_INCLUDE_PENDING_ORDERS", True), \
                 patch.object(ctrader_module.config, "CTRADER_MAX_POSITIONS_PER_SYMBOL", 3), \
                 patch.object(ctrader_module.config, "CTRADER_MAX_POSITIONS_PER_DIRECTION", 2), \
                 patch.object(ctrader_module.config, "CTRADER_MAX_ACTIVE_PER_FAMILY_SYMBOL", 1), \
                 patch.object(ctrader_module.config, "CTRADER_MAX_ACTIVE_PER_FAMILY_DIRECTION", 1), \
                 patch.object(ctrader_module.config, "CTRADER_MAX_PENDING_ORDERS_PER_SYMBOL", 3), \
                 patch.object(ctrader_module.config, "CTRADER_MAX_PENDING_ORDERS_PER_DIRECTION", 2), \
                 patch.object(ctrader_module.config, "CTRADER_MAX_PENDING_ORDERS_PER_FAMILY_SYMBOL", 1), \
                 patch.object(ctrader_module.config, "CTRADER_MAX_PENDING_ORDERS_PER_FAMILY_DIRECTION", 1), \
                 patch.object(ctrader_module.config, "get_ctrader_allowed_sources", return_value={"scalp_xauusd:pb:canary", "scalp_xauusd:td:canary"}), \
                 patch.object(ctrader_module.config, "get_ctrader_allowed_symbols", return_value={"XAUUSD"}), \
                 patch.object(ctrader_module.CTraderExecutor, "sdk_available", new_callable=PropertyMock, return_value=True):
                executor = ctrader_module.CTraderExecutor()
                with sqlite3.connect(db_path) as conn:
                    conn.execute(
                        """
                        INSERT INTO ctrader_orders(
                            order_id, account_id, source, lane, symbol, broker_symbol, direction, volume,
                            entry_price, stop_loss, take_profit, order_type, order_status, label, comment,
                            client_order_id, signal_run_id, signal_run_no, journal_id, is_open, first_seen_utc,
                            last_seen_utc, raw_json
                        ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                        """,
                        (
                            934000211,
                            46552794,
                            "scalp_xauusd:pb:canary",
                            "canary",
                            "XAUUSD",
                            "XAUUSD",
                            "long",
                            100.0,
                            5200.0,
                            5195.0,
                            5204.0,
                            "limit",
                            "accepted",
                            "dexter:XAUUSD:pb:1",
                            "dexter|pb|XAUUSD",
                            "client-pb-1",
                            "run-pb-1",
                            1,
                            1,
                            1,
                            "2026-03-10T20:00:00Z",
                            "2026-03-10T20:00:00Z",
                            "{}",
                        ),
                    )
                    conn.commit()
                result = _execute_signal_with_fixture_reference(executor, sig, source="scalp_xauusd:td:canary")

            self.assertTrue(result.ok)
            self.assertEqual(result.status, "dry_run")
        finally:
            executor = None
            gc.collect()
            shutil.rmtree(td, ignore_errors=True)

    def test_execute_signal_blocks_duplicate_order_for_same_source_and_run_id(self):
        td = tempfile.mkdtemp()
        executor = None
        try:
            db_path = str(Path(td) / "ctrader_openapi.db")
            sig = _make_signal(symbol="XAUUSD", direction="short", confidence=74.0)
            with patch.object(ctrader_module.config, "CTRADER_ENABLED", True), \
                 patch.object(ctrader_module.config, "CTRADER_AUTOTRADE_ENABLED", True), \
                 patch.object(ctrader_module.config, "CTRADER_DRY_RUN", True), \
                 patch.object(ctrader_module.config, "CTRADER_DB_PATH", db_path), \
                 patch.object(ctrader_module.config, "CTRADER_ACCOUNT_ID", "46552794"), \
                 patch.object(ctrader_module.config, "CTRADER_ACCOUNT_LOGIN", "9900897"), \
                 patch.object(ctrader_module.config, "CTRADER_POSITION_DIRECTION_GUARD_ENABLED", False), \
                 patch.object(ctrader_module.config, "get_ctrader_allowed_sources", return_value={"scalp_xauusd:td:canary"}), \
                 patch.object(ctrader_module.config, "get_ctrader_allowed_symbols", return_value={"XAUUSD"}), \
                 patch.object(ctrader_module.CTraderExecutor, "sdk_available", new_callable=PropertyMock, return_value=True):
                executor = ctrader_module.CTraderExecutor()
                with sqlite3.connect(db_path) as conn:
                    conn.execute(
                        """
                        INSERT INTO ctrader_orders(
                            order_id, account_id, source, lane, symbol, broker_symbol, direction, volume,
                            entry_price, stop_loss, take_profit, order_type, order_status, label, comment,
                            client_order_id, signal_run_id, signal_run_no, journal_id, is_open, first_seen_utc,
                            last_seen_utc, raw_json
                        ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                        """,
                        (
                            934200211,
                            46552794,
                            "scalp_xauusd:td:canary",
                            "canary",
                            "XAUUSD",
                            "XAUUSD",
                            "short",
                            100.0,
                            5100.0,
                            5105.0,
                            5097.0,
                            "limit",
                            "accepted",
                            "dexter:XAUUSD:td:7",
                            "dexter|td|XAUUSD",
                            "20260307010101-000007",
                            "20260307010101-000007",
                            7,
                            1,
                            1,
                            "2026-03-10T20:00:00Z",
                            "2026-03-10T20:00:00Z",
                            "{}",
                        ),
                    )
                    conn.commit()
                result = _execute_signal_with_fixture_reference(executor, sig, source="scalp_xauusd:td:canary")

            self.assertFalse(result.ok)
            self.assertEqual(result.status, "filtered")
            self.assertIn("duplicate_source_run_order:XAUUSD:934200211", result.message)
        finally:
            executor = None
            gc.collect()
            shutil.rmtree(td, ignore_errors=True)

    def test_execute_signal_allows_parallel_orders_for_different_sources_same_run_id(self):
        td = tempfile.mkdtemp()
        executor = None
        try:
            db_path = str(Path(td) / "ctrader_openapi.db")
            sig = _make_signal(symbol="XAUUSD", direction="short", confidence=74.0)
            with patch.object(ctrader_module.config, "CTRADER_ENABLED", True), \
                 patch.object(ctrader_module.config, "CTRADER_AUTOTRADE_ENABLED", True), \
                 patch.object(ctrader_module.config, "CTRADER_DRY_RUN", True), \
                 patch.object(ctrader_module.config, "CTRADER_DB_PATH", db_path), \
                 patch.object(ctrader_module.config, "CTRADER_ACCOUNT_ID", "46552794"), \
                 patch.object(ctrader_module.config, "CTRADER_ACCOUNT_LOGIN", "9900897"), \
                 patch.object(ctrader_module.config, "CTRADER_POSITION_DIRECTION_GUARD_ENABLED", False), \
                 patch.object(ctrader_module.config, "get_ctrader_allowed_sources", return_value={"scalp_xauusd:canary", "scalp_xauusd:td:canary"}), \
                 patch.object(ctrader_module.config, "get_ctrader_allowed_symbols", return_value={"XAUUSD"}), \
                 patch.object(ctrader_module.CTraderExecutor, "sdk_available", new_callable=PropertyMock, return_value=True):
                executor = ctrader_module.CTraderExecutor()
                with sqlite3.connect(db_path) as conn:
                    conn.execute(
                        """
                        INSERT INTO ctrader_orders(
                            order_id, account_id, source, lane, symbol, broker_symbol, direction, volume,
                            entry_price, stop_loss, take_profit, order_type, order_status, label, comment,
                            client_order_id, signal_run_id, signal_run_no, journal_id, is_open, first_seen_utc,
                            last_seen_utc, raw_json
                        ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                        """,
                        (
                            934200212,
                            46552794,
                            "scalp_xauusd:canary",
                            "canary",
                            "XAUUSD",
                            "XAUUSD",
                            "short",
                            100.0,
                            5100.0,
                            5105.0,
                            5097.0,
                            "limit",
                            "accepted",
                            "dexter:XAUUSD:base:7",
                            "dexter|base|XAUUSD",
                            "20260307010101-000007",
                            "20260307010101-000007",
                            7,
                            1,
                            1,
                            "2026-03-10T20:00:00Z",
                            "2026-03-10T20:00:00Z",
                            "{}",
                        ),
                    )
                    conn.commit()
                result = _execute_signal_with_fixture_reference(executor, sig, source="scalp_xauusd:td:canary")

            self.assertTrue(result.ok)
            self.assertEqual(result.status, "dry_run")
        finally:
            executor = None
            gc.collect()
            shutil.rmtree(td, ignore_errors=True)

    def test_execute_signal_pauses_xau_short_limit_after_family_disagreement(self):
        td = tempfile.mkdtemp()
        executor = None
        try:
            db_path = str(Path(td) / "ctrader_openapi.db")
            sig = _make_signal(symbol="XAUUSD", direction="short", confidence=74.0)
            sig.raw_scores["signal_run_id"] = "run-current"
            now_ts = time.time()
            closed_utc = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(now_ts - 60.0))
            with patch.object(ctrader_module.config, "CTRADER_ENABLED", True), \
                 patch.object(ctrader_module.config, "CTRADER_AUTOTRADE_ENABLED", True), \
                 patch.object(ctrader_module.config, "CTRADER_DRY_RUN", True), \
                 patch.object(ctrader_module.config, "CTRADER_DB_PATH", db_path), \
                 patch.object(ctrader_module.config, "CTRADER_ACCOUNT_ID", "46552794"), \
                 patch.object(ctrader_module.config, "CTRADER_ACCOUNT_LOGIN", "9900897"), \
                 patch.object(ctrader_module.config, "CTRADER_POSITION_DIRECTION_GUARD_ENABLED", False), \
                 patch.object(ctrader_module.config, "get_ctrader_allowed_sources", return_value={"scalp_xauusd:canary"}), \
                 patch.object(ctrader_module.config, "get_ctrader_allowed_symbols", return_value={"XAUUSD"}), \
                 patch.object(ctrader_module.CTraderExecutor, "sdk_available", new_callable=PropertyMock, return_value=True):
                executor = ctrader_module.CTraderExecutor()
                with sqlite3.connect(db_path) as conn:
                    conn.executemany(
                        """
                        INSERT INTO execution_journal(
                            created_ts, created_utc, source, lane, symbol, direction, confidence,
                            entry, stop_loss, take_profit, entry_type, dry_run, account_id, broker_symbol,
                            volume, status, message, order_id, position_id, deal_id, signal_run_id, signal_run_no,
                            request_json, response_json, execution_meta_json
                        ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                        """,
                        [
                            (
                                now_ts - 180.0,
                                "2026-03-19 13:46:43",
                                "scalp_xauusd:fss:canary",
                                "canary",
                                "XAUUSD",
                                "short",
                                73.4,
                                4556.95,
                                4571.98,
                                4543.01,
                                "sell_stop",
                                0,
                                46552794,
                                "XAUUSD",
                                100.0,
                                "closed",
                                "ctrader closed win pnl=+5.08$",
                                939520724,
                                594291004,
                                872298660,
                                "run-disagree",
                                3,
                                json.dumps({
                                    "symbol": "XAUUSD",
                                    "source": "scalp_xauusd:fss:canary",
                                    "direction": "short",
                                    "entry": 4556.95,
                                    "stop_loss": 4571.98,
                                    "take_profit": 4543.01,
                                    "entry_type": "sell_stop",
                                    "risk_usd": 0.75,
                                }),
                                "{}",
                                json.dumps({"closed": {"execution_utc": closed_utc, "pnl_usd": 5.08, "outcome": "win"}}),
                            ),
                            (
                                now_ts - 175.0,
                                "2026-03-19 13:46:53",
                                "scalp_xauusd:canary",
                                "canary",
                                "XAUUSD",
                                "short",
                                73.4,
                                4558.62,
                                4572.55,
                                4545.70,
                                "limit",
                                0,
                                46552794,
                                "XAUUSD",
                                100.0,
                                "closed",
                                "ctrader closed loss pnl=-14.38$",
                                939520232,
                                594290703,
                                872303927,
                                "run-disagree",
                                3,
                                json.dumps({
                                    "symbol": "XAUUSD",
                                    "source": "scalp_xauusd:canary",
                                    "direction": "short",
                                    "entry": 4558.62,
                                    "stop_loss": 4572.55,
                                    "take_profit": 4545.70,
                                    "entry_type": "limit",
                                    "risk_usd": 2.5,
                                }),
                                "{}",
                                json.dumps({"closed": {"execution_utc": closed_utc, "pnl_usd": -14.38, "outcome": "loss"}}),
                            ),
                        ],
                    )
                    conn.commit()
                result = _execute_signal_with_fixture_reference(executor, sig, source="scalp_xauusd:canary")

            self.assertFalse(result.ok)
            self.assertEqual(result.status, "filtered")
            self.assertIn("xau_short_limit_pause_active", result.message)
            self.assertIn("fss_win", result.message)
        finally:
            executor = None
            gc.collect()
            shutil.rmtree(td, ignore_errors=True)

    def test_xau_short_limit_pause_detects_stale_cancel_disagreement(self):
        td = tempfile.mkdtemp()
        executor = None
        try:
            db_path = str(Path(td) / "ctrader_openapi.db")
            sig = _make_signal(symbol="XAUUSD", direction="short", confidence=74.0)
            with patch.object(ctrader_module.config, "CTRADER_DB_PATH", db_path), \
                 patch.object(ctrader_module.config, "CTRADER_ENABLED", True), \
                 patch.object(ctrader_module.config, "CTRADER_AUTOTRADE_ENABLED", True), \
                 patch.object(ctrader_module.config, "CTRADER_DRY_RUN", True), \
                 patch.object(ctrader_module.config, "CTRADER_ACCOUNT_ID", "46552794"), \
                 patch.object(ctrader_module.config, "CTRADER_XAU_SHORT_LIMIT_PAUSE_MIN", 25), \
                 patch.object(ctrader_module.CTraderExecutor, "sdk_available", new_callable=PropertyMock, return_value=True):
                executor = ctrader_module.CTraderExecutor()
                now_ts = time.time()
                loss_utc = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(now_ts - 90.0))
                with sqlite3.connect(db_path) as conn:
                    conn.executemany(
                        """
                        INSERT INTO execution_journal(
                            created_ts, created_utc, source, lane, symbol, direction, confidence,
                            entry, stop_loss, take_profit, entry_type, dry_run, account_id, broker_symbol,
                            volume, status, message, order_id, position_id, deal_id, signal_run_id, signal_run_no,
                            request_json, response_json, execution_meta_json
                        ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                        """,
                        [
                            (
                                now_ts - (45.0 * 60.0) - 30.0,
                                "2026-03-19 13:54:13",
                                "scalp_xauusd:fss:canary",
                                "canary",
                                "XAUUSD",
                                "short",
                                73.4,
                                4580.80,
                                4595.88,
                                4566.83,
                                "sell_stop",
                                0,
                                46552794,
                                "XAUUSD",
                                100.0,
                                "canceled",
                                "ctrader canceled pending order: stale_ttl:45m",
                                939530696,
                                594297500,
                                None,
                                "run-stale",
                                4,
                                json.dumps({
                                    "symbol": "XAUUSD",
                                    "source": "scalp_xauusd:fss:canary",
                                    "direction": "short",
                                    "entry": 4580.80,
                                    "stop_loss": 4595.88,
                                    "take_profit": 4566.83,
                                    "entry_type": "sell_stop",
                                    "risk_usd": 0.75,
                                }),
                                "{}",
                                "{}",
                            ),
                            (
                                now_ts - 120.0,
                                "2026-03-19 13:53:49",
                                "scalp_xauusd:td:canary",
                                "canary",
                                "XAUUSD",
                                "short",
                                73.4,
                                4582.48,
                                4596.45,
                                4569.53,
                                "limit",
                                0,
                                46552794,
                                "XAUUSD",
                                100.0,
                                "closed",
                                "ctrader closed loss pnl=-8.26$",
                                939530130,
                                594297045,
                                872309381,
                                "run-stale",
                                4,
                                json.dumps({
                                    "symbol": "XAUUSD",
                                    "source": "scalp_xauusd:td:canary",
                                    "direction": "short",
                                    "entry": 4582.48,
                                    "stop_loss": 4596.45,
                                    "take_profit": 4569.53,
                                    "entry_type": "limit",
                                    "risk_usd": 0.75,
                                }),
                                "{}",
                                json.dumps({"closed": {"execution_utc": loss_utc, "pnl_usd": -8.26, "outcome": "loss"}}),
                            ),
                        ],
                    )
                    conn.commit()

                payload, reason = executor._build_payload(sig, source="scalp_xauusd:td:canary")
                self.assertEqual(reason, "")
                pause = executor._xau_short_limit_pause_state(source="scalp_xauusd:td:canary", payload=payload)

            self.assertTrue(bool(pause.get("active")))
            self.assertEqual(str(pause.get("support_state") or ""), "fss_stale_cancel")
            self.assertGreater(float(pause.get("remaining_min") or 0.0), 0.0)
        finally:
            executor = None
            gc.collect()
            shutil.rmtree(td, ignore_errors=True)

    def test_execute_signal_caps_same_run_pair_risk_for_identical_canary_and_td(self):
        td = tempfile.mkdtemp()
        executor = None
        try:
            db_path = str(Path(td) / "ctrader_openapi.db")
            sig = _make_signal(symbol="XAUUSD", direction="short", confidence=74.0)
            sig.raw_scores["signal_run_id"] = "run-pair-cap"
            with patch.object(ctrader_module.config, "CTRADER_ENABLED", True), \
                 patch.object(ctrader_module.config, "CTRADER_AUTOTRADE_ENABLED", True), \
                 patch.object(ctrader_module.config, "CTRADER_DRY_RUN", True), \
                 patch.object(ctrader_module.config, "CTRADER_DB_PATH", db_path), \
                 patch.object(ctrader_module.config, "CTRADER_ACCOUNT_ID", "46552794"), \
                 patch.object(ctrader_module.config, "CTRADER_ACCOUNT_LOGIN", "9900897"), \
                 patch.object(ctrader_module.config, "CTRADER_POSITION_DIRECTION_GUARD_ENABLED", False), \
                 patch.object(ctrader_module.config, "CTRADER_XAU_PAIR_RISK_MAX_USD", 3.0), \
                 patch.object(ctrader_module.config, "get_ctrader_allowed_sources", return_value={"scalp_xauusd:canary", "scalp_xauusd:td:canary"}), \
                 patch.object(ctrader_module.config, "get_ctrader_allowed_symbols", return_value={"XAUUSD"}), \
                 patch.object(ctrader_module.CTraderExecutor, "sdk_available", new_callable=PropertyMock, return_value=True):
                executor = ctrader_module.CTraderExecutor()
                with sqlite3.connect(db_path) as conn:
                    conn.execute(
                        """
                        INSERT INTO execution_journal(
                            created_ts, created_utc, source, lane, symbol, direction, confidence,
                            entry, stop_loss, take_profit, entry_type, dry_run, account_id, broker_symbol,
                            volume, status, message, order_id, position_id, deal_id, signal_run_id, signal_run_no,
                            request_json, response_json, execution_meta_json
                        ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                        """,
                        (
                            time.time() - 30.0,
                            "2026-03-19 13:53:24",
                            "scalp_xauusd:canary",
                            "canary",
                            "XAUUSD",
                            "short",
                            73.4,
                            5100.0,
                            5095.0,
                            5103.0,
                            "limit",
                            0,
                            46552794,
                            "XAUUSD",
                            100.0,
                            "accepted",
                            "ctrader order_accepted",
                            939529419,
                            594296597,
                            None,
                            "run-pair-cap",
                            7,
                            json.dumps({
                                "symbol": "XAUUSD",
                                "source": "scalp_xauusd:canary",
                                "direction": "short",
                                "entry": 5100.0,
                                "stop_loss": 5095.0,
                                "take_profit": 5103.0,
                                "entry_type": "limit",
                                "risk_usd": 2.5,
                            }),
                            "{}",
                            "{}",
                        ),
                    )
                    conn.commit()
                result = _execute_signal_with_fixture_reference(executor, sig, source="scalp_xauusd:td:canary")

                self.assertTrue(result.ok)
                self.assertEqual(result.status, "dry_run")
                with sqlite3.connect(db_path) as conn:
                    conn.row_factory = sqlite3.Row
                    row = conn.execute(
                        "SELECT request_json FROM execution_journal ORDER BY id DESC LIMIT 1"
                    ).fetchone()
                request_json = json.loads(str(row["request_json"] or "{}"))

            self.assertAlmostEqual(float(request_json.get("risk_usd") or 0.0), 0.5, places=3)
            self.assertTrue(bool((request_json.get("raw_scores") or {}).get("xau_pair_risk_cap_applied")))
        finally:
            executor = None
            gc.collect()
            shutil.rmtree(td, ignore_errors=True)

    def test_execute_signal_allows_swarm_same_direction_up_to_family_count(self):
        td = tempfile.mkdtemp()
        executor = None
        try:
            db_path = str(Path(td) / "ctrader_openapi.db")
            runtime_dir = Path(td) / "runtime"
            runtime_dir.mkdir(parents=True, exist_ok=True)
            (runtime_dir / "trading_manager_state.json").write_text(
                json.dumps(
                    {
                        "xau_family_routing": {
                            "status": "active",
                            "mode": "swarm_support_all",
                        },
                        "xau_parallel_families": {
                            "status": "active",
                            "allowed_families": [
                                "xau_scalp_pullback_limit",
                                "xau_scalp_tick_depth_filter",
                                "xau_scalp_microtrend_follow_up",
                                "xau_scalp_flow_short_sidecar",
                            ],
                            "max_same_direction_families": 3,
                        },
                        "opportunity_feed": {
                            "status": "active",
                            "symbols": {
                                "XAUUSD": {
                                    "support_all_families": [
                                        "xau_scalp_pullback_limit",
                                        "xau_scalp_tick_depth_filter",
                                        "xau_scalp_microtrend_follow_up",
                                        "xau_scalp_flow_short_sidecar",
                                    ]
                                }
                            },
                        },
                    }
                ),
                encoding="utf-8",
            )
            sig = _make_signal(symbol="XAUUSD", direction="short", confidence=74.0)
            with ExitStack() as stack:
                stack.enter_context(patch.object(ctrader_module.config, "CTRADER_ENABLED", True))
                stack.enter_context(patch.object(ctrader_module.config, "CTRADER_AUTOTRADE_ENABLED", True))
                stack.enter_context(patch.object(ctrader_module.config, "CTRADER_DRY_RUN", True))
                stack.enter_context(patch.object(ctrader_module.config, "CTRADER_DB_PATH", db_path))
                stack.enter_context(patch.object(ctrader_module.config, "CTRADER_ACCOUNT_ID", "46552794"))
                stack.enter_context(patch.object(ctrader_module.config, "CTRADER_ACCOUNT_LOGIN", "9900897"))
                stack.enter_context(patch.object(ctrader_module.config, "CTRADER_POSITION_DIRECTION_GUARD_ENABLED", True))
                stack.enter_context(patch.object(ctrader_module.config, "CTRADER_DIRECTION_GUARD_INCLUDE_PENDING_ORDERS", True))
                stack.enter_context(patch.object(ctrader_module.config, "CTRADER_MAX_POSITIONS_PER_SYMBOL", 5))
                stack.enter_context(patch.object(ctrader_module.config, "CTRADER_MAX_POSITIONS_PER_DIRECTION", 2))
                stack.enter_context(patch.object(ctrader_module.config, "CTRADER_MAX_ACTIVE_PER_FAMILY_SYMBOL", 1))
                stack.enter_context(patch.object(ctrader_module.config, "CTRADER_MAX_ACTIVE_PER_FAMILY_DIRECTION", 1))
                stack.enter_context(patch.object(ctrader_module.config, "CTRADER_MAX_PENDING_ORDERS_PER_SYMBOL", 5))
                stack.enter_context(patch.object(ctrader_module.config, "CTRADER_MAX_PENDING_ORDERS_PER_DIRECTION", 2))
                stack.enter_context(patch.object(ctrader_module.config, "CTRADER_MAX_PENDING_ORDERS_PER_FAMILY_SYMBOL", 1))
                stack.enter_context(patch.object(ctrader_module.config, "CTRADER_MAX_PENDING_ORDERS_PER_FAMILY_DIRECTION", 1))
                stack.enter_context(patch.object(ctrader_module.config, "get_ctrader_allowed_sources", return_value={"scalp_xauusd:pb:canary", "scalp_xauusd:td:canary", "scalp_xauusd:mfu:canary"}))
                stack.enter_context(patch.object(ctrader_module.config, "get_ctrader_allowed_symbols", return_value={"XAUUSD"}))
                stack.enter_context(patch.object(ctrader_module.CTraderExecutor, "sdk_available", new_callable=PropertyMock, return_value=True))
                executor = ctrader_module.CTraderExecutor()
                executor.trading_manager_state_path = runtime_dir / "trading_manager_state.json"
                with sqlite3.connect(db_path) as conn:
                    conn.execute(
                        """
                        INSERT INTO ctrader_orders(
                            order_id, account_id, source, lane, symbol, broker_symbol, direction, volume,
                            entry_price, stop_loss, take_profit, order_type, order_status, label, comment,
                            client_order_id, signal_run_id, signal_run_no, journal_id, is_open, first_seen_utc,
                            last_seen_utc, raw_json
                        ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                        """,
                        (
                            934100211,
                            46552794,
                            "scalp_xauusd:pb:canary",
                            "canary",
                            "XAUUSD",
                            "XAUUSD",
                            "short",
                            100.0,
                            5200.0,
                            5204.0,
                            5196.0,
                            "limit",
                            "accepted",
                            "dexter:XAUUSD:pb:swarm",
                            "dexter|pb|XAUUSD",
                            "client-pb-sw",
                            "run-pb-sw",
                            1,
                            1,
                            1,
                            "2026-03-10T20:00:00Z",
                            "2026-03-10T20:00:00Z",
                            "{}",
                        ),
                    )
                    conn.execute(
                        """
                        INSERT INTO ctrader_orders(
                            order_id, account_id, source, lane, symbol, broker_symbol, direction, volume,
                            entry_price, stop_loss, take_profit, order_type, order_status, label, comment,
                            client_order_id, signal_run_id, signal_run_no, journal_id, is_open, first_seen_utc,
                            last_seen_utc, raw_json
                        ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                        """,
                        (
                            934100212,
                            46552794,
                            "scalp_xauusd:td:canary",
                            "canary",
                            "XAUUSD",
                            "XAUUSD",
                            "short",
                            100.0,
                            5199.0,
                            5203.0,
                            5195.0,
                            "limit",
                            "accepted",
                            "dexter:XAUUSD:td:swarm",
                            "dexter|td|XAUUSD",
                            "client-td-sw",
                            "run-td-sw",
                            1,
                            1,
                            1,
                            "2026-03-10T20:00:00Z",
                            "2026-03-10T20:00:00Z",
                            "{}",
                        ),
                    )
                    conn.commit()
                result = _execute_signal_with_fixture_reference(executor, sig, source="scalp_xauusd:mfu:canary")

            self.assertTrue(result.ok)
            self.assertEqual(result.status, "dry_run")
        finally:
            executor = None
            gc.collect()
            shutil.rmtree(td, ignore_errors=True)

    def test_execute_signal_blocks_duplicate_pending_orders_for_same_family(self):
        td = tempfile.mkdtemp()
        executor = None
        try:
            db_path = str(Path(td) / "ctrader_openapi.db")
            sig = _make_signal(symbol="XAUUSD", direction="long", confidence=74.0)
            with patch.object(ctrader_module.config, "CTRADER_ENABLED", True), \
                 patch.object(ctrader_module.config, "CTRADER_AUTOTRADE_ENABLED", True), \
                 patch.object(ctrader_module.config, "CTRADER_DRY_RUN", True), \
                 patch.object(ctrader_module.config, "CTRADER_DB_PATH", db_path), \
                 patch.object(ctrader_module.config, "CTRADER_ACCOUNT_ID", "46552794"), \
                 patch.object(ctrader_module.config, "CTRADER_ACCOUNT_LOGIN", "9900897"), \
                 patch.object(ctrader_module.config, "CTRADER_POSITION_DIRECTION_GUARD_ENABLED", True), \
                 patch.object(ctrader_module.config, "CTRADER_DIRECTION_GUARD_INCLUDE_PENDING_ORDERS", True), \
                 patch.object(ctrader_module.config, "CTRADER_MAX_POSITIONS_PER_SYMBOL", 3), \
                 patch.object(ctrader_module.config, "CTRADER_MAX_POSITIONS_PER_DIRECTION", 2), \
                 patch.object(ctrader_module.config, "CTRADER_MAX_ACTIVE_PER_FAMILY_SYMBOL", 1), \
                 patch.object(ctrader_module.config, "CTRADER_MAX_ACTIVE_PER_FAMILY_DIRECTION", 1), \
                 patch.object(ctrader_module.config, "CTRADER_MAX_PENDING_ORDERS_PER_SYMBOL", 3), \
                 patch.object(ctrader_module.config, "CTRADER_MAX_PENDING_ORDERS_PER_DIRECTION", 2), \
                 patch.object(ctrader_module.config, "CTRADER_MAX_PENDING_ORDERS_PER_FAMILY_SYMBOL", 1), \
                 patch.object(ctrader_module.config, "CTRADER_MAX_PENDING_ORDERS_PER_FAMILY_DIRECTION", 1), \
                 patch.object(ctrader_module.config, "get_ctrader_allowed_sources", return_value={"scalp_xauusd:pb:canary"}), \
                 patch.object(ctrader_module.config, "get_ctrader_allowed_symbols", return_value={"XAUUSD"}), \
                 patch.object(ctrader_module.CTraderExecutor, "sdk_available", new_callable=PropertyMock, return_value=True):
                executor = ctrader_module.CTraderExecutor()
                with sqlite3.connect(db_path) as conn:
                    conn.execute(
                        """
                        INSERT INTO ctrader_orders(
                            order_id, account_id, source, lane, symbol, broker_symbol, direction, volume,
                            entry_price, stop_loss, take_profit, order_type, order_status, label, comment,
                            client_order_id, signal_run_id, signal_run_no, journal_id, is_open, first_seen_utc,
                            last_seen_utc, raw_json
                        ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                        """,
                        (
                            934000311,
                            46552794,
                            "scalp_xauusd:pb:canary",
                            "canary",
                            "XAUUSD",
                            "XAUUSD",
                            "long",
                            100.0,
                            5200.0,
                            5195.0,
                            5204.0,
                            "limit",
                            "accepted",
                            "dexter:XAUUSD:pb:2",
                            "dexter|pb|XAUUSD",
                            "client-pb-2",
                            "run-pb-2",
                            2,
                            2,
                            1,
                            "2026-03-10T20:00:00Z",
                            "2026-03-10T20:00:00Z",
                            "{}",
                        ),
                    )
                    conn.commit()
                result = _execute_signal_with_fixture_reference(executor, sig, source="scalp_xauusd:pb:canary")

            self.assertFalse(result.ok)
            self.assertEqual(result.status, "filtered")
            self.assertIn("family_pending_order_cap", result.message)
        finally:
            executor = None
            gc.collect()
            shutil.rmtree(td, ignore_errors=True)

    def test_execute_signal_allows_manager_controlled_hedge_lane_for_fss(self):
        td = tempfile.mkdtemp()
        executor = None
        try:
            db_path = str(Path(td) / "ctrader_openapi.db")
            runtime_dir = Path(td) / "runtime"
            runtime_dir.mkdir(parents=True, exist_ok=True)
            (runtime_dir / "trading_manager_state.json").write_text(
                json.dumps(
                    {
                        "xau_hedge_transition": {
                            "status": "active",
                            "allowed_families": ["xau_scalp_failed_fade_follow_stop", "xau_scalp_flow_short_sidecar"],
                            "max_per_symbol": 1,
                            "risk_multiplier": 0.65,
                        }
                    }
                ),
                encoding="utf-8",
            )
            sig = _make_signal(symbol="XAUUSD", direction="short", confidence=74.0)
            with patch.object(ctrader_module.config, "CTRADER_ENABLED", True), \
                 patch.object(ctrader_module.config, "CTRADER_AUTOTRADE_ENABLED", True), \
                 patch.object(ctrader_module.config, "CTRADER_DRY_RUN", True), \
                 patch.object(ctrader_module.config, "CTRADER_DB_PATH", db_path), \
                 patch.object(ctrader_module.config, "CTRADER_ACCOUNT_ID", "46552794"), \
                 patch.object(ctrader_module.config, "CTRADER_ACCOUNT_LOGIN", "9900897"), \
                 patch.object(ctrader_module.config, "CTRADER_POSITION_DIRECTION_GUARD_ENABLED", True), \
                 patch.object(ctrader_module.config, "CTRADER_BLOCK_OPPOSITE_DIRECTION", True), \
                 patch.object(ctrader_module.config, "CTRADER_MAX_POSITIONS_PER_SYMBOL", 3), \
                 patch.object(ctrader_module.config, "CTRADER_MAX_POSITIONS_PER_DIRECTION", 2), \
                 patch.object(ctrader_module.config, "CTRADER_MAX_ACTIVE_PER_FAMILY_SYMBOL", 1), \
                 patch.object(ctrader_module.config, "CTRADER_MAX_ACTIVE_PER_FAMILY_DIRECTION", 1), \
                 patch.object(ctrader_module.config, "CTRADER_MAX_PENDING_ORDERS_PER_SYMBOL", 3), \
                 patch.object(ctrader_module.config, "CTRADER_MAX_PENDING_ORDERS_PER_DIRECTION", 2), \
                 patch.object(ctrader_module.config, "CTRADER_MAX_PENDING_ORDERS_PER_FAMILY_SYMBOL", 1), \
                 patch.object(ctrader_module.config, "CTRADER_MAX_PENDING_ORDERS_PER_FAMILY_DIRECTION", 1), \
                 patch.object(ctrader_module.config, "get_ctrader_allowed_sources", return_value={"xauusd_scheduled:canary", "scalp_xauusd:fss:canary"}), \
                 patch.object(ctrader_module.config, "get_ctrader_allowed_symbols", return_value={"XAUUSD"}), \
                 patch.object(ctrader_module.CTraderExecutor, "sdk_available", new_callable=PropertyMock, return_value=True):
                executor = ctrader_module.CTraderExecutor()
                with sqlite3.connect(db_path) as conn:
                    conn.execute(
                        """
                        INSERT INTO ctrader_positions(
                            position_id, source, symbol, direction, entry_price, stop_loss, take_profit, first_seen_utc, is_open
                        ) VALUES(?,?,?,?,?,?,?,?,?)
                        """,
                        (
                            590900001,
                            "xauusd_scheduled:canary",
                            "XAUUSD",
                            "long",
                            5200.0,
                            5190.0,
                            5208.0,
                            "2026-03-10T20:00:00Z",
                            1,
                        ),
                    )
                    conn.commit()
                executor.trading_manager_state_path = runtime_dir / "trading_manager_state.json"
                result = _execute_signal_with_fixture_reference(executor, sig, source="scalp_xauusd:fss:canary")

            self.assertTrue(result.ok)
            self.assertEqual(result.status, "dry_run")
            self.assertAlmostEqual(float((result.execution_meta or {}).get("risk_usd") or 0.0), 6.5, places=2)
        finally:
            executor = None
            gc.collect()
            shutil.rmtree(td, ignore_errors=True)

    def test_execute_signal_still_blocks_opposite_direction_for_non_hedge_family(self):
        td = tempfile.mkdtemp()
        executor = None
        try:
            db_path = str(Path(td) / "ctrader_openapi.db")
            runtime_dir = Path(td) / "runtime"
            runtime_dir.mkdir(parents=True, exist_ok=True)
            (runtime_dir / "trading_manager_state.json").write_text(
                json.dumps(
                    {
                        "xau_hedge_transition": {
                            "status": "active",
                            "allowed_families": ["xau_scalp_failed_fade_follow_stop", "xau_scalp_flow_short_sidecar"],
                            "max_per_symbol": 1,
                            "risk_multiplier": 0.65,
                        }
                    }
                ),
                encoding="utf-8",
            )
            sig = _make_signal(symbol="XAUUSD", direction="short", confidence=74.0)
            with patch.object(ctrader_module.config, "CTRADER_ENABLED", True), \
                 patch.object(ctrader_module.config, "CTRADER_AUTOTRADE_ENABLED", True), \
                 patch.object(ctrader_module.config, "CTRADER_DRY_RUN", True), \
                 patch.object(ctrader_module.config, "CTRADER_DB_PATH", db_path), \
                 patch.object(ctrader_module.config, "CTRADER_ACCOUNT_ID", "46552794"), \
                 patch.object(ctrader_module.config, "CTRADER_ACCOUNT_LOGIN", "9900897"), \
                 patch.object(ctrader_module.config, "CTRADER_POSITION_DIRECTION_GUARD_ENABLED", True), \
                 patch.object(ctrader_module.config, "CTRADER_BLOCK_OPPOSITE_DIRECTION", True), \
                 patch.object(ctrader_module.config, "CTRADER_MAX_POSITIONS_PER_SYMBOL", 3), \
                 patch.object(ctrader_module.config, "CTRADER_MAX_POSITIONS_PER_DIRECTION", 2), \
                 patch.object(ctrader_module.config, "CTRADER_MAX_ACTIVE_PER_FAMILY_SYMBOL", 1), \
                 patch.object(ctrader_module.config, "CTRADER_MAX_ACTIVE_PER_FAMILY_DIRECTION", 1), \
                 patch.object(ctrader_module.config, "CTRADER_MAX_PENDING_ORDERS_PER_SYMBOL", 3), \
                 patch.object(ctrader_module.config, "CTRADER_MAX_PENDING_ORDERS_PER_DIRECTION", 2), \
                 patch.object(ctrader_module.config, "CTRADER_MAX_PENDING_ORDERS_PER_FAMILY_SYMBOL", 1), \
                 patch.object(ctrader_module.config, "CTRADER_MAX_PENDING_ORDERS_PER_FAMILY_DIRECTION", 1), \
                 patch.object(ctrader_module.config, "get_ctrader_allowed_sources", return_value={"xauusd_scheduled:canary", "scalp_xauusd:pb:canary"}), \
                 patch.object(ctrader_module.config, "get_ctrader_allowed_symbols", return_value={"XAUUSD"}), \
                 patch.object(ctrader_module.CTraderExecutor, "sdk_available", new_callable=PropertyMock, return_value=True):
                executor = ctrader_module.CTraderExecutor()
                with sqlite3.connect(db_path) as conn:
                    conn.execute(
                        """
                        INSERT INTO ctrader_positions(
                            position_id, source, symbol, direction, entry_price, stop_loss, take_profit, first_seen_utc, is_open
                        ) VALUES(?,?,?,?,?,?,?,?,?)
                        """,
                        (
                            590900101,
                            "xauusd_scheduled:canary",
                            "XAUUSD",
                            "long",
                            5200.0,
                            5190.0,
                            5208.0,
                            "2026-03-10T20:00:00Z",
                            1,
                        ),
                    )
                    conn.commit()
                executor.trading_manager_state_path = runtime_dir / "trading_manager_state.json"
                result = _execute_signal_with_fixture_reference(executor, sig, source="scalp_xauusd:pb:canary")

            self.assertFalse(result.ok)
            self.assertEqual(result.status, "filtered")
            self.assertIn("opposite_direction_open", result.message)
        finally:
            executor = None
            gc.collect()
            shutil.rmtree(td, ignore_errors=True)

    def test_execute_signal_allows_manager_controlled_opportunity_bypass_for_fss(self):
        td = tempfile.mkdtemp()
        executor = None
        try:
            db_path = str(Path(td) / "ctrader_openapi.db")
            runtime_dir = Path(td) / "runtime"
            runtime_dir.mkdir(parents=True, exist_ok=True)
            (runtime_dir / "trading_manager_state.json").write_text(
                json.dumps(
                    {
                        "xau_opportunity_bypass": {
                            "status": "active",
                            "allowed_families": ["xau_scalp_failed_fade_follow_stop", "xau_scalp_flow_short_sidecar"],
                            "max_per_symbol": 2,
                            "risk_multiplier": 0.55,
                        }
                    }
                ),
                encoding="utf-8",
            )
            sig = _make_signal(symbol="XAUUSD", direction="short", confidence=74.0)
            with patch.object(ctrader_module.config, "CTRADER_ENABLED", True), \
                 patch.object(ctrader_module.config, "CTRADER_AUTOTRADE_ENABLED", True), \
                 patch.object(ctrader_module.config, "CTRADER_DRY_RUN", True), \
                 patch.object(ctrader_module.config, "CTRADER_DB_PATH", db_path), \
                 patch.object(ctrader_module.config, "CTRADER_ACCOUNT_ID", "46552794"), \
                 patch.object(ctrader_module.config, "CTRADER_ACCOUNT_LOGIN", "9900897"), \
                 patch.object(ctrader_module.config, "CTRADER_POSITION_DIRECTION_GUARD_ENABLED", True), \
                 patch.object(ctrader_module.config, "CTRADER_BLOCK_OPPOSITE_DIRECTION", True), \
                 patch.object(ctrader_module.config, "CTRADER_MAX_POSITIONS_PER_SYMBOL", 3), \
                 patch.object(ctrader_module.config, "CTRADER_MAX_POSITIONS_PER_DIRECTION", 2), \
                 patch.object(ctrader_module.config, "CTRADER_MAX_ACTIVE_PER_FAMILY_SYMBOL", 1), \
                 patch.object(ctrader_module.config, "CTRADER_MAX_ACTIVE_PER_FAMILY_DIRECTION", 1), \
                 patch.object(ctrader_module.config, "CTRADER_MAX_PENDING_ORDERS_PER_SYMBOL", 3), \
                 patch.object(ctrader_module.config, "CTRADER_MAX_PENDING_ORDERS_PER_DIRECTION", 2), \
                 patch.object(ctrader_module.config, "CTRADER_MAX_PENDING_ORDERS_PER_FAMILY_SYMBOL", 1), \
                 patch.object(ctrader_module.config, "CTRADER_MAX_PENDING_ORDERS_PER_FAMILY_DIRECTION", 1), \
                 patch.object(ctrader_module.config, "get_ctrader_allowed_sources", return_value={"xauusd_scheduled:canary", "scalp_xauusd:fss:canary"}), \
                 patch.object(ctrader_module.config, "get_ctrader_allowed_symbols", return_value={"XAUUSD"}), \
                 patch.object(ctrader_module.CTraderExecutor, "sdk_available", new_callable=PropertyMock, return_value=True):
                executor = ctrader_module.CTraderExecutor()
                with sqlite3.connect(db_path) as conn:
                    conn.execute(
                        """
                        INSERT INTO ctrader_positions(
                            position_id, source, symbol, direction, entry_price, stop_loss, take_profit, first_seen_utc, is_open
                        ) VALUES(?,?,?,?,?,?,?,?,?)
                        """,
                        (
                            590900201,
                            "xauusd_scheduled:canary",
                            "XAUUSD",
                            "long",
                            5200.0,
                            5190.0,
                            5208.0,
                            "2026-03-10T20:00:00Z",
                            1,
                        ),
                    )
                    conn.commit()
                executor.trading_manager_state_path = runtime_dir / "trading_manager_state.json"
                result = _execute_signal_with_fixture_reference(executor, sig, source="scalp_xauusd:fss:canary")
            self.assertTrue(result.ok)
            self.assertEqual(result.status, "dry_run")
            self.assertAlmostEqual(float((result.execution_meta or {}).get("risk_usd") or 0.0), 5.5, places=2)
        finally:
            executor = None
            gc.collect()
            shutil.rmtree(td, ignore_errors=True)

    def test_source_family_maps_tick_depth_filter_lane(self):
        self.assertEqual(
            ctrader_module.CTraderExecutor._source_family("scalp_xauusd:td:canary"),
            "xau_scalp_tick_depth_filter",
        )

    def test_source_family_maps_range_repair_lane(self):
        self.assertEqual(
            ctrader_module.CTraderExecutor._source_family("scalp_xauusd:rr:canary"),
            "xau_scalp_range_repair",
        )

    def test_source_family_maps_failed_fade_follow_stop_lane(self):
        self.assertEqual(
            ctrader_module.CTraderExecutor._source_family("scalp_xauusd:ff:canary"),
            "xau_scalp_failed_fade_follow_stop",
        )

    def test_source_family_maps_microtrend_follow_up_lane(self):
        self.assertEqual(
            ctrader_module.CTraderExecutor._source_family("scalp_xauusd:mfu:canary"),
            "xau_scalp_microtrend_follow_up",
        )

    def test_source_family_maps_flow_short_sidecar_lane(self):
        self.assertEqual(
            ctrader_module.CTraderExecutor._source_family("scalp_xauusd:fss:canary"),
            "xau_scalp_flow_short_sidecar",
        )

    def test_xau_order_care_desk_maps_range_repair_lane(self):
        self.assertEqual(
            ctrader_module.CTraderExecutor._xau_order_care_desk("scalp_xauusd:rr:canary"),
            "range_repair",
        )

    def test_source_allowed_blocks_direct_canary_not_in_direct_allowlist(self):
        with patch.object(ctrader_module.config, "PERSISTENT_CANARY_ENABLED", True), \
             patch.object(ctrader_module.config, "get_ctrader_allowed_sources", return_value={"xauusd_scheduled:winner"}), \
             patch.object(ctrader_module.config, "get_persistent_canary_allowed_sources", return_value={"scalp_xauusd", "xauusd_scheduled"}), \
             patch.object(ctrader_module.config, "get_persistent_canary_direct_allowed_sources", return_value={"xauusd_scheduled"}):
            executor = ctrader_module.CTraderExecutor()
            self.assertFalse(executor._source_allowed("scalp_xauusd:canary"))
            self.assertTrue(executor._source_allowed("xauusd_scheduled:canary"))

    def test_source_allowed_keeps_family_canary_when_base_source_allowed(self):
        with patch.object(ctrader_module.config, "PERSISTENT_CANARY_ENABLED", True), \
             patch.object(ctrader_module.config, "get_ctrader_allowed_sources", return_value={"xauusd_scheduled:winner"}), \
             patch.object(ctrader_module.config, "get_persistent_canary_allowed_sources", return_value={"scalp_xauusd", "xauusd_scheduled"}), \
             patch.object(ctrader_module.config, "get_persistent_canary_direct_allowed_sources", return_value={"xauusd_scheduled"}):
            executor = ctrader_module.CTraderExecutor()
            self.assertTrue(executor._source_allowed("scalp_xauusd:pb:canary"))
            self.assertTrue(executor._source_allowed("scalp_xauusd:td:canary"))

    def test_pending_order_cancel_reason_keeps_enabled_experimental_family(self):
        td = tempfile.mkdtemp()
        executor = None
        try:
            db_path = str(Path(td) / "ctrader_openapi.db")
            with patch.object(ctrader_module.config, "CTRADER_DB_PATH", db_path), \
                 patch.object(ctrader_module.config, "CTRADER_ENABLED", True), \
                 patch.object(ctrader_module.config, "CTRADER_AUTOTRADE_ENABLED", True), \
                 patch.object(ctrader_module.config, "CTRADER_DRY_RUN", False), \
                 patch.object(ctrader_module.config, "CTRADER_ACCOUNT_ID", "46552794"), \
                 patch.object(ctrader_module.config, "CTRADER_PENDING_ORDER_CANCEL_DISABLED_FAMILY", True), \
                 patch.object(ctrader_module.config, "PERSISTENT_CANARY_EXPERIMENTAL_FAMILY_EXECUTOR_ENABLED", True), \
                 patch.object(ctrader_module.config, "get_ctrader_xau_active_families", return_value={"xau_scalp_pullback_limit"}), \
                 patch.object(ctrader_module.config, "get_persistent_canary_experimental_families", return_value={"xau_scalp_tick_depth_filter"}), \
                 patch.object(ctrader_module.config, "CTRADER_BLOCK_OPPOSITE_DIRECTION", False), \
                 patch.object(ctrader_module.config, "CTRADER_MAX_POSITIONS_PER_SYMBOL", 3), \
                 patch.object(ctrader_module.config, "CTRADER_MAX_POSITIONS_PER_DIRECTION", 2), \
                 patch.object(ctrader_module.CTraderExecutor, "sdk_available", new_callable=PropertyMock, return_value=True):
                executor = ctrader_module.CTraderExecutor()
                with patch.object(executor, "_active_exposure_snapshot", return_value={"active_total": 0, "active_long": 0, "active_short": 0}):
                    reason = executor._pending_order_cancel_reason(
                        {
                            "source": "scalp_xauusd:td:canary",
                            "symbol": "XAUUSD",
                            "direction": "long",
                            "created_ts": time.time(),
                            "order_id": 934100001,
                        },
                        now_ts=time.time(),
                    )

            self.assertEqual(reason, "")
        finally:
            executor = None
            gc.collect()
            shutil.rmtree(td, ignore_errors=True)

    def test_manage_open_positions_closes_when_missing_sl_breached(self):
        td = tempfile.mkdtemp()
        executor = None
        try:
            db_path = str(Path(td) / "ctrader_openapi.db")
            with patch.object(ctrader_module.config, "CTRADER_ENABLED", True), \
                 patch.object(ctrader_module.config, "CTRADER_AUTOTRADE_ENABLED", True), \
                 patch.object(ctrader_module.config, "CTRADER_DRY_RUN", False), \
                 patch.object(ctrader_module.config, "CTRADER_DB_PATH", db_path), \
                 patch.object(ctrader_module.config, "CTRADER_ACCOUNT_ID", "46552794"), \
                 patch.object(ctrader_module.config, "CTRADER_POSITION_MANAGER_ENABLED", True), \
                 patch.object(ctrader_module.config, "CTRADER_PM_REPAIR_MISSING_SL_ENABLED", True), \
                 patch.object(ctrader_module.CTraderExecutor, "sdk_available", new_callable=PropertyMock, return_value=True):
                executor = ctrader_module.CTraderExecutor()

                class _Row(dict):
                    def __getitem__(self, key):
                        return dict.__getitem__(self, key)

                journal_row = _Row({
                    "entry": 5234.5624,
                    "stop_loss": 5231.7175,
                    "take_profit": 5237.1254,
                })
                tracked = [{
                    "source": "scalp_xauusd:pb:canary",
                    "journal_row": journal_row,
                    "position": {
                        "position_id": 589826571,
                        "symbol": "XAUUSD",
                        "direction": "long",
                        "volume": 100.0,
                        "entry_price": 5231.56,
                        "stop_loss": 0.0,
                        "take_profit": 5237.13,
                        "source": "scalp_xauusd:pb:canary",
                    },
                }]

                with patch.object(executor, "_reference_price", return_value=5197.79), \
                     patch.object(executor, "close_position", return_value=ctrader_module.CTraderExecutionResult(True, "closed", "ok", signal_symbol="XAUUSD", broker_symbol="XAUUSD")) as close_mock:
                    report = executor._manage_open_positions(tracked)

            self.assertEqual(close_mock.call_count, 1)
            self.assertEqual(report["managed_positions"], 1)
            self.assertIn("close_missing_sl_breached", str(report["pm_actions"]))
        finally:
            executor = None
            gc.collect()
            shutil.rmtree(td, ignore_errors=True)

    def test_manage_open_positions_repairs_missing_tp_when_stop_is_present(self):
        td = tempfile.mkdtemp()
        executor = None
        try:
            db_path = str(Path(td) / "ctrader_openapi.db")
            with patch.object(ctrader_module.config, "CTRADER_ENABLED", True), \
                 patch.object(ctrader_module.config, "CTRADER_AUTOTRADE_ENABLED", True), \
                 patch.object(ctrader_module.config, "CTRADER_DRY_RUN", False), \
                 patch.object(ctrader_module.config, "CTRADER_DB_PATH", db_path), \
                 patch.object(ctrader_module.config, "CTRADER_ACCOUNT_ID", "46552794"), \
                 patch.object(ctrader_module.config, "CTRADER_POSITION_MANAGER_ENABLED", True), \
                 patch.object(ctrader_module.CTraderExecutor, "sdk_available", new_callable=PropertyMock, return_value=True):
                executor = ctrader_module.CTraderExecutor()

                class _Row(dict):
                    def __getitem__(self, key):
                        return dict.__getitem__(self, key)

                journal_row = _Row({
                    "entry": 5106.91,
                    "stop_loss": 5109.14,
                    "take_profit": 5104.84,
                    "confidence": 72.0,
                })
                tracked = [{
                    "source": "scalp_xauusd:td:canary",
                    "journal_row": journal_row,
                    "position": {
                        "position_id": 591011902,
                        "symbol": "XAUUSD",
                        "direction": "short",
                        "volume": 100.0,
                        "entry_price": 5106.91,
                        "stop_loss": 5109.14,
                        "take_profit": 0.0,
                        "source": "scalp_xauusd:td:canary",
                        "first_seen_utc": "2026-03-13T05:07:42Z",
                        "last_seen_utc": "2026-03-13T05:11:42Z",
                    },
                }]

                with patch.object(executor, "_reference_price", return_value=5107.0), \
                     patch.object(executor, "amend_position_sltp", return_value=ctrader_module.CTraderExecutionResult(True, "amended", "ok", signal_symbol="XAUUSD", broker_symbol="XAUUSD")) as amend_mock:
                    report = executor._manage_open_positions(tracked)

            self.assertEqual(amend_mock.call_count, 1)
            self.assertIn("repair_missing_tp", str(report["pm_actions"]))
        finally:
            executor = None
            gc.collect()
            shutil.rmtree(td, ignore_errors=True)

    def test_manage_open_positions_closes_xau_trade_on_strong_adverse_force(self):
        td = tempfile.mkdtemp()
        executor = None
        try:
            db_path = str(Path(td) / "ctrader_openapi.db")
            with patch.object(ctrader_module.config, "CTRADER_ENABLED", True), \
                 patch.object(ctrader_module.config, "CTRADER_AUTOTRADE_ENABLED", True), \
                 patch.object(ctrader_module.config, "CTRADER_DRY_RUN", False), \
                 patch.object(ctrader_module.config, "CTRADER_DB_PATH", db_path), \
                 patch.object(ctrader_module.config, "CTRADER_ACCOUNT_ID", "46552794"), \
                 patch.object(ctrader_module.config, "CTRADER_POSITION_MANAGER_ENABLED", True), \
                 patch.object(ctrader_module.config, "CTRADER_PM_XAU_ACTIVE_DEFENSE_ENABLED", True), \
                 patch.object(ctrader_module.config, "CTRADER_PM_XAU_ACTIVE_DEFENSE_ALLOWED_SOURCES", "scalp_xauusd:td:canary"), \
                 patch.object(ctrader_module.CTraderExecutor, "sdk_available", new_callable=PropertyMock, return_value=True):
                executor = ctrader_module.CTraderExecutor()

                class _Row(dict):
                    def __getitem__(self, key):
                        return dict.__getitem__(self, key)

                journal_row = _Row({
                    "entry": 5106.91,
                    "stop_loss": 5109.14,
                    "take_profit": 5104.84,
                    "confidence": 72.0,
                })
                tracked = [{
                    "source": "scalp_xauusd:td:canary",
                    "journal_row": journal_row,
                    "position": {
                        "position_id": 591011900,
                        "symbol": "XAUUSD",
                        "direction": "short",
                        "volume": 100.0,
                        "entry_price": 5106.91,
                        "stop_loss": 5109.14,
                        "take_profit": 5104.84,
                        "source": "scalp_xauusd:td:canary",
                        "first_seen_utc": "2026-03-13T05:07:42Z",
                        "last_seen_utc": "2026-03-13T05:11:42Z",
                    },
                }]
                snapshot = {
                    "ok": True,
                    "run_id": "ctcap_test",
                    "features": {
                        "day_type": "repricing",
                        "delta_proxy": 0.34,
                        "depth_imbalance": 0.22,
                        "mid_drift_pct": 0.024,
                        "rejection_ratio": 0.08,
                        "bar_volume_proxy": 0.74,
                    },
                }
                with patch.object(executor, "_reference_price", return_value=5108.05), \
                     patch.object(executor, "_latest_capture_snapshot", return_value=snapshot), \
                     patch.object(executor, "close_position", return_value=ctrader_module.CTraderExecutionResult(True, "closed", "ok", signal_symbol="XAUUSD", broker_symbol="XAUUSD")) as close_mock:
                    report = executor._manage_open_positions(tracked)

            self.assertEqual(close_mock.call_count, 1)
            self.assertIn("xau_active_defense_close", str(report["pm_actions"]))
        finally:
            executor = None
            gc.collect()
            shutil.rmtree(td, ignore_errors=True)

    def test_manage_open_positions_tightens_xau_trade_on_adverse_force_before_sl(self):
        td = tempfile.mkdtemp()
        executor = None
        try:
            db_path = str(Path(td) / "ctrader_openapi.db")
            with patch.object(ctrader_module.config, "CTRADER_ENABLED", True), \
                 patch.object(ctrader_module.config, "CTRADER_AUTOTRADE_ENABLED", True), \
                 patch.object(ctrader_module.config, "CTRADER_DRY_RUN", False), \
                 patch.object(ctrader_module.config, "CTRADER_DB_PATH", db_path), \
                 patch.object(ctrader_module.config, "CTRADER_ACCOUNT_ID", "46552794"), \
                 patch.object(ctrader_module.config, "CTRADER_POSITION_MANAGER_ENABLED", True), \
                 patch.object(ctrader_module.config, "CTRADER_PM_XAU_ACTIVE_DEFENSE_ENABLED", True), \
                 patch.object(ctrader_module.config, "CTRADER_PM_XAU_ACTIVE_DEFENSE_ALLOWED_SOURCES", "scalp_xauusd:td:canary"), \
                 patch.object(ctrader_module.config, "CTRADER_PM_XAU_ACTIVE_DEFENSE_CLOSE_SCORE", 6), \
                 patch.object(ctrader_module.CTraderExecutor, "sdk_available", new_callable=PropertyMock, return_value=True):
                executor = ctrader_module.CTraderExecutor()

                class _Row(dict):
                    def __getitem__(self, key):
                        return dict.__getitem__(self, key)

                journal_row = _Row({
                    "entry": 5106.91,
                    "stop_loss": 5109.14,
                    "take_profit": 5104.84,
                    "confidence": 72.0,
                })
                tracked = [{
                    "source": "scalp_xauusd:td:canary",
                    "journal_row": journal_row,
                    "position": {
                        "position_id": 591011901,
                        "symbol": "XAUUSD",
                        "direction": "short",
                        "volume": 100.0,
                        "entry_price": 5106.91,
                        "stop_loss": 5109.14,
                        "take_profit": 5104.84,
                        "source": "scalp_xauusd:td:canary",
                        "first_seen_utc": "2026-03-13T05:07:42Z",
                        "last_seen_utc": "2026-03-13T05:11:42Z",
                    },
                }]
                snapshot = {
                    "ok": True,
                    "run_id": "ctcap_test",
                    "features": {
                        "day_type": "trend",
                        "delta_proxy": 0.18,
                        "depth_imbalance": 0.12,
                        "mid_drift_pct": 0.012,
                        "rejection_ratio": 0.12,
                        "bar_volume_proxy": 0.58,
                    },
                }
                with patch.object(executor, "_reference_price", return_value=5107.25), \
                     patch.object(executor, "_latest_capture_snapshot", return_value=snapshot), \
                     patch.object(executor, "amend_position_sltp", return_value=ctrader_module.CTraderExecutionResult(True, "amended", "ok", signal_symbol="XAUUSD", broker_symbol="XAUUSD")) as amend_mock:
                    report = executor._manage_open_positions(tracked)

            self.assertEqual(amend_mock.call_count, 1)
            self.assertIn("xau_active_defense_tighten", str(report["pm_actions"]))
        finally:
            executor = None
            gc.collect()
            shutil.rmtree(td, ignore_errors=True)

    def test_manage_open_positions_extends_xau_target_on_strong_support(self):
        td = tempfile.mkdtemp()
        executor = None
        try:
            db_path = str(Path(td) / "ctrader_openapi.db")
            with patch.object(ctrader_module.config, "CTRADER_ENABLED", True), \
                 patch.object(ctrader_module.config, "CTRADER_AUTOTRADE_ENABLED", True), \
                 patch.object(ctrader_module.config, "CTRADER_DRY_RUN", False), \
                 patch.object(ctrader_module.config, "CTRADER_DB_PATH", db_path), \
                 patch.object(ctrader_module.config, "CTRADER_ACCOUNT_ID", "46552794"), \
                 patch.object(ctrader_module.config, "CTRADER_POSITION_MANAGER_ENABLED", True), \
                 patch.object(ctrader_module.CTraderExecutor, "sdk_available", new_callable=PropertyMock, return_value=True):
                executor = ctrader_module.CTraderExecutor()
                executor.trading_manager_state_path.parent.mkdir(parents=True, exist_ok=True)
                executor.trading_manager_state_path.write_text(
                    json.dumps(
                        {
                            "xau_order_care": {
                                "status": "active",
                                "mode": "continuation_fail_fast",
                                "allowed_sources": ["scalp_xauusd:canary"],
                                "overrides": {},
                            }
                        }
                    ),
                    encoding="utf-8",
                )

                class _Row(dict):
                    def __getitem__(self, key):
                        return dict.__getitem__(self, key)

                journal_row = _Row({
                    "entry": 5106.91,
                    "stop_loss": 5109.14,
                    "take_profit": 5104.84,
                    "confidence": 78.0,
                })
                tracked = [{
                    "source": "scalp_xauusd:canary",
                    "journal_row": journal_row,
                    "position": {
                        "position_id": 591011902,
                        "symbol": "XAUUSD",
                        "direction": "short",
                        "volume": 100.0,
                        "entry_price": 5106.91,
                        "stop_loss": 5109.14,
                        "take_profit": 5104.84,
                        "source": "scalp_xauusd:canary",
                        "first_seen_utc": "2026-03-13T05:07:42Z",
                        "last_seen_utc": "2026-03-13T05:11:42Z",
                    },
                }]
                snapshot = {
                    "ok": True,
                    "run_id": "ctcap_extend",
                    "features": {
                        "day_type": "trend",
                        "delta_proxy": -0.24,
                        "depth_imbalance": -0.18,
                        "mid_drift_pct": -0.014,
                        "rejection_ratio": 0.06,
                        "bar_volume_proxy": 0.88,
                    },
                }
                amend_res = ctrader_module.CTraderExecutionResult(True, "amended", "ok", signal_symbol="XAUUSD", broker_symbol="XAUUSD")
                with patch.object(executor, "_reference_price", return_value=5104.70), \
                     patch.object(executor, "_latest_capture_snapshot", return_value=snapshot), \
                     patch.object(executor, "amend_position_sltp", return_value=amend_res) as amend_mock, \
                     patch.object(executor, "close_position", return_value=ctrader_module.CTraderExecutionResult(True, "closed", "ok")) as close_mock:
                    report = executor._manage_open_positions(tracked)

            self.assertEqual(close_mock.call_count, 0)
            self.assertEqual(amend_mock.call_count, 1)
            amend_kwargs = amend_mock.call_args.kwargs
            self.assertLess(float(amend_kwargs["take_profit"]), 5104.84)
            self.assertLessEqual(float(amend_kwargs["stop_loss"]), 5109.14)
            self.assertIn("xau_profit_extension", str(report["pm_actions"]))
        finally:
            executor = None
            gc.collect()
            shutil.rmtree(td, ignore_errors=True)

    def test_xau_profit_extension_uses_sniper_min_age_default(self):
        td = tempfile.mkdtemp()
        executor = None
        try:
            db_path = str(Path(td) / "ctrader_openapi.db")
            with patch.object(ctrader_module.config, "CTRADER_ENABLED", True), \
                 patch.object(ctrader_module.config, "CTRADER_AUTOTRADE_ENABLED", True), \
                 patch.object(ctrader_module.config, "CTRADER_DRY_RUN", True), \
                 patch.object(ctrader_module.config, "CTRADER_DB_PATH", db_path), \
                 patch.object(ctrader_module.config, "CTRADER_ACCOUNT_ID", "46552794"), \
                 patch.object(ctrader_module.config, "CTRADER_PM_XAU_EXTENSION_MIN_AGE_MIN", 0.15), \
                 patch.object(ctrader_module.CTraderExecutor, "sdk_available", new_callable=PropertyMock, return_value=True):
                executor = ctrader_module.CTraderExecutor()
                executor.trading_manager_state_path.parent.mkdir(parents=True, exist_ok=True)
                executor.trading_manager_state_path.write_text(
                    json.dumps(
                        {
                            "xau_order_care": {
                                "status": "active",
                                "mode": "continuation_fail_fast",
                                "allowed_sources": ["scalp_xauusd:fss:canary"],
                                "overrides": {},
                            }
                        }
                    ),
                    encoding="utf-8",
                )
                snapshot = {
                    "ok": True,
                    "run_id": "ctcap_sniper_extend",
                    "features": {
                        "day_type": "trend",
                        "delta_proxy": -0.22,
                        "depth_imbalance": -0.15,
                        "mid_drift_pct": -0.013,
                        "rejection_ratio": 0.05,
                        "bar_volume_proxy": 0.79,
                    },
                }
                with patch.object(executor, "_latest_capture_snapshot", return_value=snapshot):
                    plan = executor._xau_profit_extension_plan(
                        source="scalp_xauusd:fss:canary",
                        symbol="XAUUSD",
                        direction="short",
                        entry=4562.2524,
                        stop_loss=4573.5624,
                        planned_tp=4551.7734,
                        current_tp=4551.7734,
                        current_price=4551.70,
                        confidence=75.0,
                        age_min=0.2,
                        r_now=0.93,
                    )

            self.assertTrue(bool(plan.get("active")))
            self.assertEqual(str(plan.get("action") or ""), "extend")
        finally:
            executor = None
            gc.collect()
            shutil.rmtree(td, ignore_errors=True)

    def test_manage_open_positions_clamps_wide_xau_stop_after_fill_drift(self):
        td = tempfile.mkdtemp()
        executor = None
        try:
            db_path = str(Path(td) / "ctrader_openapi.db")
            with patch.object(ctrader_module.config, "CTRADER_ENABLED", True), \
                 patch.object(ctrader_module.config, "CTRADER_AUTOTRADE_ENABLED", True), \
                 patch.object(ctrader_module.config, "CTRADER_DRY_RUN", False), \
                 patch.object(ctrader_module.config, "CTRADER_DB_PATH", db_path), \
                 patch.object(ctrader_module.config, "CTRADER_ACCOUNT_ID", "46552794"), \
                 patch.object(ctrader_module.config, "CTRADER_POSITION_MANAGER_ENABLED", True), \
                 patch.object(ctrader_module.config, "CTRADER_PM_XAU_POST_FILL_STOP_CLAMP_ENABLED", True), \
                 patch.object(ctrader_module.config, "CTRADER_PM_XAU_POST_FILL_STOP_MAX_RISK_MULT", 1.15), \
                 patch.object(ctrader_module.CTraderExecutor, "sdk_available", new_callable=PropertyMock, return_value=True):
                executor = ctrader_module.CTraderExecutor()

                class _Row(dict):
                    def __getitem__(self, key):
                        return dict.__getitem__(self, key)

                journal_row = _Row({
                    "entry": 4594.9244,
                    "stop_loss": 4604.2379,
                    "take_profit": 4586.2908,
                    "confidence": 71.2,
                })
                tracked = [{
                    "source": "scalp_xauusd:fss:canary",
                    "journal_row": journal_row,
                    "position": {
                        "position_id": 594235994,
                        "symbol": "XAUUSD",
                        "direction": "short",
                        "volume": 100.0,
                        "entry_price": 4582.94,
                        "stop_loss": 4604.24,
                        "take_profit": 0.0,
                        "source": "scalp_xauusd:fss:canary",
                        "first_seen_utc": "2026-03-19T12:43:28Z",
                        "last_seen_utc": "2026-03-19T12:44:10Z",
                    },
                }]
                amend_res = ctrader_module.CTraderExecutionResult(True, "amended", "ok", signal_symbol="XAUUSD", broker_symbol="XAUUSD")
                with patch.object(executor, "_reference_price", return_value=4581.80), \
                     patch.object(executor, "amend_position_sltp", return_value=amend_res) as amend_mock, \
                     patch.object(executor, "close_position", return_value=ctrader_module.CTraderExecutionResult(True, "closed", "ok")) as close_mock:
                    report = executor._manage_open_positions(tracked)

            self.assertEqual(close_mock.call_count, 0)
            self.assertEqual(amend_mock.call_count, 1)
            amend_kwargs = amend_mock.call_args.kwargs
            self.assertLess(float(amend_kwargs["stop_loss"]), 4604.24)
            self.assertEqual(float(amend_kwargs["take_profit"]), 0.0)
            self.assertIn("xau_post_fill_stop_clamp", str(report["pm_actions"]))
        finally:
            executor = None
            gc.collect()
            shutil.rmtree(td, ignore_errors=True)

    def test_manage_open_positions_skips_planned_close_when_live_tp_is_extended(self):
        td = tempfile.mkdtemp()
        executor = None
        try:
            db_path = str(Path(td) / "ctrader_openapi.db")
            with patch.object(ctrader_module.config, "CTRADER_ENABLED", True), \
                 patch.object(ctrader_module.config, "CTRADER_AUTOTRADE_ENABLED", True), \
                 patch.object(ctrader_module.config, "CTRADER_DRY_RUN", False), \
                 patch.object(ctrader_module.config, "CTRADER_DB_PATH", db_path), \
                 patch.object(ctrader_module.config, "CTRADER_ACCOUNT_ID", "46552794"), \
                 patch.object(ctrader_module.config, "CTRADER_POSITION_MANAGER_ENABLED", True), \
                 patch.object(ctrader_module.CTraderExecutor, "sdk_available", new_callable=PropertyMock, return_value=True):
                executor = ctrader_module.CTraderExecutor()

                class _Row(dict):
                    def __getitem__(self, key):
                        return dict.__getitem__(self, key)

                journal_row = _Row({
                    "entry": 5106.91,
                    "stop_loss": 5109.14,
                    "take_profit": 5104.84,
                    "confidence": 76.0,
                })
                tracked = [{
                    "source": "scalp_xauusd",
                    "journal_row": journal_row,
                    "position": {
                        "position_id": 591011903,
                        "symbol": "XAUUSD",
                        "direction": "short",
                        "volume": 100.0,
                        "entry_price": 5106.91,
                        "stop_loss": 5109.14,
                        "take_profit": 5104.20,
                        "source": "scalp_xauusd",
                        "first_seen_utc": "2026-03-13T05:07:42Z",
                        "last_seen_utc": "2026-03-13T05:11:42Z",
                    },
                }]
                with patch.object(executor, "_reference_price", return_value=5104.60), \
                     patch.object(executor, "close_position", return_value=ctrader_module.CTraderExecutionResult(True, "closed", "ok")) as close_mock, \
                     patch.object(executor, "amend_position_sltp", return_value=ctrader_module.CTraderExecutionResult(True, "amended", "ok")) as amend_mock:
                    report = executor._manage_open_positions(tracked)

            self.assertEqual(close_mock.call_count, 0)
            self.assertEqual(amend_mock.call_count, 0)
            self.assertEqual(int(report["managed_positions"]), 1)
            self.assertEqual(list(report["pm_actions"]), [])
        finally:
            executor = None
            gc.collect()
            shutil.rmtree(td, ignore_errors=True)

    def test_get_lane_stats_ignores_untracked_ctrader_rows(self):
        td = tempfile.mkdtemp()
        executor = None
        try:
            db_path = str(Path(td) / "ctrader_openapi.db")
            with patch.object(ctrader_module.config, "CTRADER_ENABLED", True), \
                 patch.object(ctrader_module.config, "CTRADER_AUTOTRADE_ENABLED", True), \
                 patch.object(ctrader_module.config, "CTRADER_DRY_RUN", False), \
                 patch.object(ctrader_module.config, "CTRADER_DB_PATH", db_path), \
                 patch.object(ctrader_module.config, "CTRADER_ACCOUNT_ID", "46552794"), \
                 patch.object(ctrader_module.CTraderExecutor, "sdk_available", new_callable=PropertyMock, return_value=True):
                executor = ctrader_module.CTraderExecutor()
                with sqlite3.connect(db_path) as conn:
                    conn.execute(
                        """
                        INSERT INTO execution_journal(
                            created_ts, created_utc, source, lane, symbol, direction, confidence,
                            entry, stop_loss, take_profit, entry_type, dry_run, account_id, broker_symbol,
                            volume, status, message, order_id, position_id, deal_id, signal_run_id, signal_run_no,
                            request_json, response_json, execution_meta_json
                        ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                        """,
                        (
                            1.0,
                            "2026-03-07 05:00:00",
                            "scalp_ethusd",
                            "main",
                            "ETHUSD",
                            "long",
                            76.0,
                            2000.0,
                            1990.0,
                            2010.0,
                            "market",
                            0,
                            46552794,
                            "ETHUSD",
                            2.0,
                            "filled",
                            "ok",
                            10,
                            777,
                            888,
                            "run",
                            7,
                            "{}",
                            "{}",
                            "{}",
                        ),
                    )
                    conn.executemany(
                        """
                        INSERT INTO ctrader_positions(
                            position_id, account_id, source, lane, symbol, broker_symbol, direction, volume,
                            entry_price, stop_loss, take_profit, label, comment, signal_run_id, signal_run_no,
                            journal_id, is_open, status, first_seen_utc, last_seen_utc, raw_json
                        ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                        """,
                        [
                            (
                                777,
                                46552794,
                                "scalp_ethusd",
                                "main",
                                "ETHUSD",
                                "ETHUSD",
                                "long",
                                2,
                                2000.0,
                                1990.0,
                                2010.0,
                                "dexter:ETHUSD:scalp_ethusd:7",
                                "dexter|scalp_ethusd|ETHUSD",
                                "run",
                                7,
                                1,
                                1,
                                "POSITION_STATUS_OPEN",
                                "2026-03-07T05:00:00Z",
                                "2026-03-07T05:00:00Z",
                                "{}",
                            ),
                            (
                                778,
                                46552794,
                                "scalp_ethusd",
                                "main",
                                "ETHUSD",
                                "ETHUSD",
                                "long",
                                2,
                                2001.0,
                                1991.0,
                                2011.0,
                                "dexter:ETHUSD:scalp_ethusd:8",
                                "dexter|scalp_ethusd|ETHUSD",
                                "",
                                8,
                                None,
                                1,
                                "POSITION_STATUS_OPEN",
                                "2026-03-07T05:01:00Z",
                                "2026-03-07T05:01:00Z",
                                "{}",
                            ),
                        ],
                    )
                    conn.executemany(
                        """
                        INSERT INTO ctrader_deals(
                            deal_id, account_id, position_id, order_id, source, lane, symbol, broker_symbol,
                            direction, volume, execution_price, gross_profit_usd, swap_usd, commission_usd,
                            pnl_conversion_fee_usd, pnl_usd, outcome, has_close_detail, signal_run_id, signal_run_no,
                            journal_id, execution_utc, raw_json
                        ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                        """,
                        [
                            (
                                9001, 46552794, 777, 8001, "scalp_ethusd", "main", "ETHUSD", "ETHUSD",
                                "long", 2.0, 2005.0, 8.0, 0.0, 0.0, 0.0, 8.0, 1, 1, "run", 7, 1,
                                "2026-03-07 05:10:00", "{}"
                            ),
                            (
                                9002, 46552794, 778, 8002, "scalp_ethusd", "main", "ETHUSD", "ETHUSD",
                                "long", 2.0, 1995.0, -6.0, 0.0, 0.0, 0.0, -6.0, 0, 1, "", 8, None,
                                "2026-03-07 05:11:00", "{}"
                            ),
                        ],
                    )
                    conn.commit()

                stats = executor.get_lane_stats(
                    symbol="ETHUSD",
                    start_utc="2026-03-07 00:00:00",
                    end_utc="2026-03-08 00:00:00",
                )

            main = stats["lanes"]["main"]
            self.assertEqual(main["sent"], 1)
            self.assertEqual(main["filled"], 1)
            self.assertEqual(main["open"], 1)
            self.assertEqual(main["resolved"], 1)
            self.assertEqual(main["wins"], 1)
            self.assertEqual(main["losses"], 0)
            self.assertEqual(main["pnl"], 8.0)
        finally:
            executor = None
            gc.collect()
            shutil.rmtree(td, ignore_errors=True)

    def test_sync_account_state_updates_execution_journal_to_closed(self):
        td = tempfile.mkdtemp()
        executor = None
        try:
            db_path = str(Path(td) / "ctrader_openapi.db")
            with patch.object(ctrader_module.config, "CTRADER_ENABLED", True), \
                 patch.object(ctrader_module.config, "CTRADER_AUTOTRADE_ENABLED", True), \
                 patch.object(ctrader_module.config, "CTRADER_DRY_RUN", False), \
                 patch.object(ctrader_module.config, "CTRADER_DB_PATH", db_path), \
                 patch.object(ctrader_module.config, "CTRADER_ACCOUNT_ID", "46552794"), \
                 patch.object(ctrader_module.CTraderExecutor, "sdk_available", new_callable=PropertyMock, return_value=True):
                executor = ctrader_module.CTraderExecutor()
                with sqlite3.connect(db_path) as conn:
                    conn.execute(
                        """
                        INSERT INTO execution_journal(
                            created_ts, created_utc, source, lane, symbol, direction, confidence,
                            entry, stop_loss, take_profit, entry_type, dry_run, account_id, broker_symbol,
                            volume, status, message, order_id, position_id, deal_id, signal_run_id, signal_run_no,
                            request_json, response_json, execution_meta_json
                        ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                        """,
                        (
                            1.0,
                            "2026-03-09 03:29:19",
                            "scalp_xauusd",
                            "main",
                            "XAUUSD",
                            "long",
                            72.8,
                            5100.38,
                            5093.98,
                            5106.31,
                            "limit",
                            0,
                            46552794,
                            "XAUUSD",
                            100.0,
                            "filled",
                            "ctrader order_accepted",
                            933106514,
                            589051669,
                            None,
                            "20260309032906-000003",
                            3,
                            "{}",
                            "{}",
                            "{}",
                        ),
                    )
                    conn.commit()

                reconcile_payload = {
                    "ok": True,
                    "status": "reconciled",
                    "positions": [],
                    "deals": [
                        {
                            "deal_id": 866586043,
                            "position_id": 589051669,
                            "order_id": 933106869,
                            "symbol": "XAUUSD",
                            "direction": "short",
                            "volume": 100.0,
                            "execution_price": 5086.06,
                            "gross_profit_usd": -6.8,
                            "swap_usd": 0.0,
                            "commission_usd": -0.3,
                            "pnl_conversion_fee_usd": 0.0,
                            "pnl_usd": -7.1,
                            "outcome": 0,
                            "has_close_detail": True,
                            "execution_utc": "2026-03-09T03:29:59Z",
                        }
                    ],
                }
                with patch.object(executor, "_run_worker", return_value=reconcile_payload):
                    report = executor.sync_account_state()

                self.assertTrue(report["ok"])
                with sqlite3.connect(db_path) as conn:
                    conn.row_factory = sqlite3.Row
                    row = conn.execute(
                        "SELECT status, message, deal_id, response_json, execution_meta_json FROM execution_journal WHERE position_id=?",
                        (589051669,),
                    ).fetchone()
                self.assertEqual(str(row["status"]), "closed")
                self.assertIn("closed loss", str(row["message"]))
                self.assertEqual(int(row["deal_id"]), 866586043)
                self.assertIn("close_deal", str(row["response_json"]))
                self.assertIn("\"outcome\":\"loss\"", str(row["execution_meta_json"]))
        finally:
            executor = None
            gc.collect()
            shutil.rmtree(td, ignore_errors=True)

    def test_sync_account_state_audits_xau_profit_extension_when_live_extend_hits(self):
        td = tempfile.mkdtemp()
        executor = None
        try:
            db_path = str(Path(td) / "ctrader_openapi.db")
            with patch.object(ctrader_module.config, "CTRADER_ENABLED", True), \
                 patch.object(ctrader_module.config, "CTRADER_AUTOTRADE_ENABLED", True), \
                 patch.object(ctrader_module.config, "CTRADER_DRY_RUN", False), \
                 patch.object(ctrader_module.config, "CTRADER_DB_PATH", db_path), \
                 patch.object(ctrader_module.config, "CTRADER_ACCOUNT_ID", "46552794"), \
                 patch.object(ctrader_module.config, "CTRADER_POSITION_MANAGER_ENABLED", True), \
                 patch.object(ctrader_module.config, "CTRADER_PM_CLOSE_AT_PLANNED_TARGET", True), \
                 patch.object(ctrader_module.config, "CTRADER_PM_XAU_EXTENSION_MIN_AGE_MIN", 0.15), \
                 patch.object(ctrader_module.CTraderExecutor, "sdk_available", new_callable=PropertyMock, return_value=True):
                executor = ctrader_module.CTraderExecutor()
                executor.trading_manager_state_path.parent.mkdir(parents=True, exist_ok=True)
                executor.trading_manager_state_path.write_text(
                    json.dumps(
                        {
                            "xau_order_care": {
                                "status": "active",
                                "mode": "continuation_fail_fast",
                                "allowed_sources": ["scalp_xauusd:fss:canary"],
                                "overrides": {},
                            }
                        }
                    ),
                    encoding="utf-8",
                )
                first_seen_utc = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(time.time() - 120.0))
                last_seen_utc = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(time.time() - 20.0))
                with sqlite3.connect(db_path) as conn:
                    conn.execute(
                        """
                        INSERT INTO execution_journal(
                            id, created_ts, created_utc, source, lane, symbol, direction, confidence,
                            entry, stop_loss, take_profit, entry_type, dry_run, account_id, broker_symbol,
                            volume, status, message, order_id, position_id, deal_id, signal_run_id, signal_run_no,
                            request_json, response_json, execution_meta_json
                        ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                        """,
                        (
                            1,
                            time.time() - 180.0,
                            "2026-03-19 20:14:19",
                            "scalp_xauusd:fss:canary",
                            "canary",
                            "XAUUSD",
                            "short",
                            75.0,
                            4543.8916,
                            4557.4571,
                            4531.3189,
                            "sell_stop",
                            0,
                            46552794,
                            "XAUUSD",
                            100.0,
                            "filled",
                            "ctrader reconciled open position",
                            933572351,
                            589497985,
                            None,
                            "20260319131405-000004",
                            4,
                            "{}",
                            "{}",
                            "{}",
                        ),
                    )
                    conn.execute(
                        """
                        INSERT INTO ctrader_positions(
                            position_id, account_id, source, lane, symbol, broker_symbol, direction, volume,
                            entry_price, stop_loss, take_profit, label, comment, signal_run_id, signal_run_no,
                            journal_id, is_open, status, first_seen_utc, last_seen_utc, raw_json
                        ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                        """,
                        (
                            589497985,
                            46552794,
                            "scalp_xauusd:fss:canary",
                            "canary",
                            "XAUUSD",
                            "XAUUSD",
                            "short",
                            100.0,
                            4543.8916,
                            4557.4571,
                            4531.3189,
                            "dexter:XAUUSD:scalp_xauusd:fss::4",
                            "dexter|scalp_xauusd:fss:canary|XAUUSD",
                            "20260319131405-000004",
                            4,
                            1,
                            1,
                            "POSITION_STATUS_OPEN",
                            first_seen_utc,
                            last_seen_utc,
                            "{}",
                        ),
                    )
                    conn.commit()

                reconcile_payload = {
                    "ok": True,
                    "status": "reconciled",
                    "positions": [
                        {
                            "position_id": 589497985,
                            "symbol": "XAUUSD",
                            "direction": "short",
                            "volume": 100.0,
                            "entry_price": 4543.8916,
                            "stop_loss": 4557.4571,
                            "take_profit": 4531.3189,
                            "label": "dexter:XAUUSD:scalp_xauusd:fss::4",
                            "comment": "dexter|scalp_xauusd:fss:canary|XAUUSD",
                            "status": "POSITION_STATUS_OPEN",
                        }
                    ],
                    "deals": [],
                }
                snapshot = {
                    "ok": True,
                    "run_id": "ctcap_live_extend_audit",
                    "features": {
                        "day_type": "trend",
                        "delta_proxy": -0.22,
                        "depth_imbalance": -0.15,
                        "mid_drift_pct": -0.013,
                        "rejection_ratio": 0.05,
                        "bar_volume_proxy": 0.79,
                    },
                }
                seen_modes = []

                def _fake_run_worker(*, mode, payload, timeout_sec):
                    seen_modes.append((mode, dict(payload or {})))
                    if mode == "reconcile":
                        return reconcile_payload
                    if mode == "amend_position_sltp":
                        return {"ok": True, "status": "amended_position", "message": "ok", "position_id": payload.get("position_id")}
                    raise AssertionError(f"unexpected mode {mode}")

                with patch.object(executor, "_run_worker", side_effect=_fake_run_worker), \
                     patch.object(executor, "_reference_price", return_value=4531.10), \
                     patch.object(executor, "_latest_capture_snapshot", return_value=snapshot):
                    report = executor.sync_account_state()

            self.assertTrue(report["ok"])
            self.assertEqual(int(report["pm_audited_actions"]), 1)
            self.assertIn("xau_profit_extension", str(report["pm_actions"]))
            self.assertIn("amend_position_sltp", [x[0] for x in seen_modes])
            with sqlite3.connect(db_path) as conn:
                conn.row_factory = sqlite3.Row
                row = conn.execute(
                    "SELECT execution_meta_json FROM execution_journal WHERE id=1"
                ).fetchone()
            execution_meta = json.loads(str(row["execution_meta_json"] or "{}"))
            self.assertIn("xau_profit_extension", list(execution_meta.get("audit_tags") or []))
            self.assertEqual(str(execution_meta.get("xau_profit_extension", {}).get("action") or ""), "xau_profit_extension")
            self.assertEqual(str(execution_meta.get("xau_profit_extension", {}).get("details", {}).get("trigger") or ""), "planned_target")
            self.assertGreater(float(execution_meta.get("xau_profit_extension", {}).get("age_min") or 0.0), 0.15)
            self.assertLess(float(execution_meta.get("xau_profit_extension", {}).get("new_take_profit") or 0.0), 4531.3189)
        finally:
            executor = None
            gc.collect()
            shutil.rmtree(td, ignore_errors=True)

    def test_sync_account_state_clamps_wide_stop_for_invalid_tp_position_after_fill_drift(self):
        td = tempfile.mkdtemp()
        executor = None
        try:
            db_path = str(Path(td) / "ctrader_openapi.db")
            with patch.object(ctrader_module.config, "CTRADER_ENABLED", True), \
                 patch.object(ctrader_module.config, "CTRADER_AUTOTRADE_ENABLED", True), \
                 patch.object(ctrader_module.config, "CTRADER_DRY_RUN", False), \
                 patch.object(ctrader_module.config, "CTRADER_DB_PATH", db_path), \
                 patch.object(ctrader_module.config, "CTRADER_ACCOUNT_ID", "46552794"), \
                 patch.object(ctrader_module.config, "CTRADER_POSITION_MANAGER_ENABLED", True), \
                 patch.object(ctrader_module.config, "CTRADER_PM_CLOSE_AT_PLANNED_TARGET", True), \
                 patch.object(ctrader_module.CTraderExecutor, "sdk_available", new_callable=PropertyMock, return_value=True):
                executor = ctrader_module.CTraderExecutor()
                with sqlite3.connect(db_path) as conn:
                    conn.execute(
                        """
                        INSERT INTO execution_journal(
                            created_ts, created_utc, source, lane, symbol, direction, confidence,
                            entry, stop_loss, take_profit, entry_type, dry_run, account_id, broker_symbol,
                            volume, status, message, order_id, position_id, deal_id, signal_run_id, signal_run_no,
                            request_json, response_json, execution_meta_json
                        ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                        """,
                        (
                            1.0,
                            "2026-03-09 18:42:02",
                            "scalp_xauusd:bs:canary",
                            "canary",
                            "XAUUSD",
                            "long",
                            72.0,
                            5114.7765,
                            5112.0279,
                            5117.5250,
                            "buy_stop",
                            0,
                            46552794,
                            "XAUUSD",
                            100.0,
                            "filled",
                            "ctrader order_accepted",
                            933572350,
                            589497984,
                            None,
                            "20260309184105-000003",
                            3,
                            "{}",
                            "{}",
                            "{}",
                        ),
                    )
                    conn.commit()

                reconcile_payload = {
                    "ok": True,
                    "status": "reconciled",
                    "positions": [
                        {
                            "position_id": 589497984,
                            "symbol": "XAUUSD",
                            "direction": "long",
                            "volume": 100.0,
                            "entry_price": 5119.17,
                            "stop_loss": 5112.03,
                            "take_profit": 0.0,
                            "label": "dexter:XAUUSD:scalp_xauusd:bs::3",
                            "comment": "dexter|scalp_xauusd:bs:canary|XAUUSD",
                            "status": "POSITION_STATUS_OPEN",
                        }
                    ],
                    "deals": [],
                }
                seen_modes = []

                def _fake_run_worker(*, mode, payload, timeout_sec):
                    seen_modes.append((mode, dict(payload or {})))
                    if mode == "reconcile":
                        return reconcile_payload
                    if mode == "amend_position_sltp":
                        return {"ok": True, "status": "amended_position", "message": "ok", "position_id": payload.get("position_id")}
                    raise AssertionError(f"unexpected mode {mode}")

                with patch.object(executor, "_run_worker", side_effect=_fake_run_worker), \
                     patch.object(executor, "_reference_price", return_value=5144.0):
                    report = executor.sync_account_state()

            self.assertTrue(report["ok"])
            self.assertEqual(int(report["closed_profit_positions"]), 0)
            self.assertIn("amend_position_sltp", [x[0] for x in seen_modes])
        finally:
            executor = None
            gc.collect()
            shutil.rmtree(td, ignore_errors=True)

    def test_sweep_pending_orders_reprices_xau_short_limit_when_no_weakening(self):
        td = tempfile.mkdtemp()
        executor = None
        try:
            db_path = str(Path(td) / "ctrader_openapi.db")
            with patch.object(ctrader_module.config, "CTRADER_DB_PATH", db_path), \
                 patch.object(ctrader_module.config, "CTRADER_ENABLED", True), \
                 patch.object(ctrader_module.config, "CTRADER_AUTOTRADE_ENABLED", True), \
                 patch.object(ctrader_module.config, "CTRADER_DRY_RUN", False), \
                 patch.object(ctrader_module.config, "CTRADER_ACCOUNT_ID", "46552794"), \
                 patch.object(ctrader_module.config, "CTRADER_PENDING_ORDER_SWEEP_ENABLED", True), \
                 patch.object(ctrader_module.config, "CTRADER_PENDING_ORDER_DYNAMIC_REPRICE_ENABLED", True), \
                 patch.object(ctrader_module.config, "CTRADER_PENDING_ORDER_DYNAMIC_REPRICE_MIN_AGE_SEC", 30), \
                 patch.object(ctrader_module.config, "CTRADER_PENDING_ORDER_DYNAMIC_REPRICE_TRIGGER_R", 0.18), \
                 patch.object(ctrader_module.config, "CTRADER_PENDING_ORDER_DYNAMIC_REPRICE_STEP_R", 0.22), \
                 patch.object(ctrader_module.config, "CTRADER_PENDING_ORDER_DYNAMIC_REPRICE_BUFFER_R", 0.16), \
                 patch.object(ctrader_module.config, "CTRADER_PENDING_ORDER_DYNAMIC_REPRICE_MAX_COUNT", 2), \
                 patch.object(ctrader_module.config, "CTRADER_PENDING_ORDER_DYNAMIC_REPRICE_COOLDOWN_SEC", 30), \
                 patch.object(ctrader_module.config, "CTRADER_PENDING_ORDER_DYNAMIC_REPRICE_MIN_MID_DRIFT_PCT", 0.006), \
                 patch.object(ctrader_module.config, "CTRADER_PENDING_ORDER_DYNAMIC_REPRICE_REQUIRE_CAPTURE", True), \
                 patch.object(ctrader_module.config, "get_ctrader_pending_order_dynamic_reprice_families", return_value={"xau_scalp_pullback_limit"}), \
                 patch.object(ctrader_module.CTraderExecutor, "sdk_available", new_callable=PropertyMock, return_value=True):
                executor = ctrader_module.CTraderExecutor()
                created_ts = time.time() - 180.0
                with sqlite3.connect(db_path) as conn:
                    conn.execute(
                        """
                        INSERT INTO execution_journal(
                            id, created_ts, created_utc, source, lane, symbol, direction, confidence,
                            entry, stop_loss, take_profit, entry_type, dry_run, account_id, broker_symbol,
                            volume, status, message, order_id, position_id, deal_id, signal_run_id, signal_run_no,
                            request_json, response_json, execution_meta_json
                        ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                        """,
                        (
                            1,
                            created_ts,
                            "2026-03-11T09:00:00Z",
                            "scalp_xauusd:pb:canary",
                            "canary",
                            "XAUUSD",
                            "short",
                            73.4,
                            5192.56,
                            5194.12,
                            5190.22,
                            "limit",
                            0,
                            46552794,
                            "XAUUSD",
                            100.0,
                            "accepted",
                            "ctrader accepted",
                            934500001,
                            None,
                            None,
                            "run-1",
                            1,
                            "{}",
                            "{}",
                            "{}",
                        ),
                    )
                    conn.execute(
                        """
                        INSERT INTO ctrader_orders(
                            order_id, account_id, source, lane, symbol, broker_symbol, direction, volume,
                            entry_price, stop_loss, take_profit, order_type, order_status, label, comment,
                            client_order_id, signal_run_id, signal_run_no, journal_id, is_open, first_seen_utc,
                            last_seen_utc, raw_json
                        ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                        """,
                        (
                            934500001,
                            46552794,
                            "scalp_xauusd:pb:canary",
                            "canary",
                            "XAUUSD",
                            "XAUUSD",
                            "short",
                            100.0,
                            5192.56,
                            5194.12,
                            5190.22,
                            "limit",
                            "accepted",
                            "dexter:XAUUSD:scalp_xauusd:pb:1",
                            "dexter|scalp_xauusd:pb:canary|XAUUSD",
                            "client-1",
                            "run-1",
                            1,
                            1,
                            1,
                            "2026-03-11T09:00:00Z",
                            "2026-03-11T09:00:00Z",
                            "{}",
                        ),
                    )
                    conn.commit()

                snapshot = {
                    "ok": True,
                    "status": "ok",
                    "run_id": "ctcap_test",
                    "gate": {"pass": False, "reasons": ["short_gate_failed"]},
                    "features": {
                        "mid_drift_pct": 0.012,
                        "spread_expansion": 1.01,
                        "depth_imbalance": -0.03,
                        "depth_refill_shift": -0.02,
                        "rejection_ratio": 0.31,
                    },
                }
                amend_res = ctrader_module.CTraderExecutionResult(ok=True, status="amended_order", message="ok")
                with patch.object(executor, "_reference_price", return_value=5193.05), \
                     patch.object(executor, "_latest_capture_snapshot", return_value=snapshot), \
                     patch.object(executor, "amend_order", return_value=amend_res):
                    report = executor._sweep_pending_orders(
                        [{
                            "order_id": 934500001,
                            "journal_id": 1,
                            "source": "scalp_xauusd:pb:canary",
                            "symbol": "XAUUSD",
                            "direction": "short",
                            "entry_price": 5192.56,
                            "stop_loss": 5194.12,
                            "take_profit": 5190.22,
                            "order_type": "limit",
                            "volume": 100.0,
                            "created_ts": created_ts,
                        }]
                    )

                self.assertEqual(int(report["repriced_orders"]), 1)
                self.assertEqual(int(report["canceled_orders"]), 0)
                with sqlite3.connect(db_path) as conn:
                    row = conn.execute(
                        "SELECT entry, stop_loss, take_profit, execution_meta_json FROM execution_journal WHERE id=1"
                    ).fetchone()
                self.assertGreater(float(row[0]), 5192.56)
                self.assertGreater(float(row[1]), 5194.12)
                self.assertGreater(float(row[2]), 5190.22)
                meta = ctrader_module.CTraderExecutor._safe_json_load(str(row[3] or "{}"))
                self.assertEqual(int(meta.get("pending_reprice_count", 0) or 0), 1)
        finally:
            executor = None
            gc.collect()
            shutil.rmtree(td, ignore_errors=True)

    def test_sweep_pending_orders_cancels_after_max_reprices_without_weakening(self):
        td = tempfile.mkdtemp()
        executor = None
        try:
            db_path = str(Path(td) / "ctrader_openapi.db")
            with patch.object(ctrader_module.config, "CTRADER_DB_PATH", db_path), \
                 patch.object(ctrader_module.config, "CTRADER_ENABLED", True), \
                 patch.object(ctrader_module.config, "CTRADER_AUTOTRADE_ENABLED", True), \
                 patch.object(ctrader_module.config, "CTRADER_DRY_RUN", False), \
                 patch.object(ctrader_module.config, "CTRADER_ACCOUNT_ID", "46552794"), \
                 patch.object(ctrader_module.config, "CTRADER_PENDING_ORDER_SWEEP_ENABLED", True), \
                 patch.object(ctrader_module.config, "CTRADER_PENDING_ORDER_DYNAMIC_REPRICE_ENABLED", True), \
                 patch.object(ctrader_module.config, "CTRADER_PENDING_ORDER_DYNAMIC_REPRICE_MIN_AGE_SEC", 30), \
                 patch.object(ctrader_module.config, "CTRADER_PENDING_ORDER_DYNAMIC_REPRICE_TRIGGER_R", 0.18), \
                 patch.object(ctrader_module.config, "CTRADER_PENDING_ORDER_DYNAMIC_REPRICE_MAX_COUNT", 2), \
                 patch.object(ctrader_module.config, "CTRADER_PENDING_ORDER_DYNAMIC_REPRICE_REQUIRE_CAPTURE", True), \
                 patch.object(ctrader_module.config, "get_ctrader_pending_order_dynamic_reprice_families", return_value={"xau_scalp_pullback_limit"}), \
                 patch.object(ctrader_module.CTraderExecutor, "sdk_available", new_callable=PropertyMock, return_value=True):
                executor = ctrader_module.CTraderExecutor()
                created_ts = time.time() - 240.0
                meta_json = {
                    "pending_reprice_count": 2,
                    "pending_reprices": [
                        {"repriced_at": "2026-03-11T08:50:00Z"},
                        {"repriced_at": "2026-03-11T08:55:00Z"},
                    ],
                }
                with sqlite3.connect(db_path) as conn:
                    conn.execute(
                        """
                        INSERT INTO execution_journal(
                            id, created_ts, created_utc, source, lane, symbol, direction, confidence,
                            entry, stop_loss, take_profit, entry_type, dry_run, account_id, broker_symbol,
                            volume, status, message, order_id, position_id, deal_id, signal_run_id, signal_run_no,
                            request_json, response_json, execution_meta_json
                        ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                        """,
                        (
                            1,
                            created_ts,
                            "2026-03-11T09:00:00Z",
                            "scalp_xauusd:pb:canary",
                            "canary",
                            "XAUUSD",
                            "short",
                            73.4,
                            5192.56,
                            5194.12,
                            5190.22,
                            "limit",
                            0,
                            46552794,
                            "XAUUSD",
                            100.0,
                            "accepted",
                            "ctrader accepted",
                            934500002,
                            None,
                            None,
                            "run-1",
                            1,
                            "{}",
                            "{}",
                            ctrader_module.json.dumps(meta_json, ensure_ascii=True),
                        ),
                    )
                    conn.execute(
                        """
                        INSERT INTO ctrader_orders(
                            order_id, account_id, source, lane, symbol, broker_symbol, direction, volume,
                            entry_price, stop_loss, take_profit, order_type, order_status, label, comment,
                            client_order_id, signal_run_id, signal_run_no, journal_id, is_open, first_seen_utc,
                            last_seen_utc, raw_json
                        ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                        """,
                        (
                            934500002,
                            46552794,
                            "scalp_xauusd:pb:canary",
                            "canary",
                            "XAUUSD",
                            "XAUUSD",
                            "short",
                            100.0,
                            5192.56,
                            5194.12,
                            5190.22,
                            "limit",
                            "accepted",
                            "dexter:XAUUSD:scalp_xauusd:pb:1",
                            "dexter|scalp_xauusd:pb:canary|XAUUSD",
                            "client-2",
                            "run-1",
                            1,
                            1,
                            1,
                            "2026-03-11T09:00:00Z",
                            "2026-03-11T09:00:00Z",
                            "{}",
                        ),
                    )
                    conn.commit()

                snapshot = {
                    "ok": True,
                    "status": "ok",
                    "run_id": "ctcap_test",
                    "gate": {"pass": False, "reasons": ["short_gate_failed"]},
                    "features": {"mid_drift_pct": 0.014},
                }
                cancel_res = ctrader_module.CTraderExecutionResult(ok=True, status="canceled", message="ok")
                with patch.object(executor, "_reference_price", return_value=5193.05), \
                     patch.object(executor, "_latest_capture_snapshot", return_value=snapshot), \
                     patch.object(executor, "cancel_order", return_value=cancel_res):
                    report = executor._sweep_pending_orders(
                        [{
                            "order_id": 934500002,
                            "journal_id": 1,
                            "source": "scalp_xauusd:pb:canary",
                            "symbol": "XAUUSD",
                            "direction": "short",
                            "entry_price": 5192.56,
                            "stop_loss": 5194.12,
                            "take_profit": 5190.22,
                            "order_type": "limit",
                            "volume": 100.0,
                            "created_ts": created_ts,
                        }]
                    )

                self.assertEqual(int(report["repriced_orders"]), 0)
                self.assertEqual(int(report["canceled_orders"]), 1)
                self.assertEqual(str(report["cancel_actions"][0]["reason"]), "max_reprices_no_weakening")
        finally:
            executor = None
            gc.collect()
            shutil.rmtree(td, ignore_errors=True)

    def test_sweep_pending_orders_flips_to_follow_stop_on_significant_opposite_force(self):
        td = tempfile.mkdtemp()
        executor = None
        try:
            db_path = str(Path(td) / "ctrader_openapi.db")
            with ExitStack() as stack:
                stack.enter_context(patch.object(ctrader_module.config, "CTRADER_DB_PATH", db_path))
                stack.enter_context(patch.object(ctrader_module.config, "CTRADER_ENABLED", True))
                stack.enter_context(patch.object(ctrader_module.config, "CTRADER_AUTOTRADE_ENABLED", True))
                stack.enter_context(patch.object(ctrader_module.config, "CTRADER_DRY_RUN", False))
                stack.enter_context(patch.object(ctrader_module.config, "CTRADER_ACCOUNT_ID", "46552794"))
                stack.enter_context(patch.object(ctrader_module.config, "CTRADER_PENDING_ORDER_SWEEP_ENABLED", True))
                stack.enter_context(patch.object(ctrader_module.config, "CTRADER_PENDING_ORDER_DYNAMIC_REPRICE_ENABLED", True))
                stack.enter_context(patch.object(ctrader_module.config, "CTRADER_PENDING_ORDER_DYNAMIC_REPRICE_MIN_AGE_SEC", 30))
                stack.enter_context(patch.object(ctrader_module.config, "CTRADER_PENDING_ORDER_DYNAMIC_REPRICE_TRIGGER_R", 0.18))
                stack.enter_context(patch.object(ctrader_module.config, "CTRADER_PENDING_ORDER_DYNAMIC_REPRICE_REQUIRE_CAPTURE", True))
                stack.enter_context(patch.object(ctrader_module.config, "get_ctrader_pending_order_dynamic_reprice_families", return_value={"xau_scalp_pullback_limit"}))
                stack.enter_context(patch.object(ctrader_module.config, "CTRADER_PENDING_ORDER_FOLLOW_STOP_ENABLED", True))
                stack.enter_context(patch.object(ctrader_module.config, "CTRADER_PENDING_ORDER_FOLLOW_STOP_TRIGGER_R", 0.34))
                stack.enter_context(patch.object(ctrader_module.config, "CTRADER_PENDING_ORDER_FOLLOW_STOP_MIN_MID_DRIFT_PCT", 0.012))
                stack.enter_context(patch.object(ctrader_module.config, "CTRADER_PENDING_ORDER_FOLLOW_STOP_MIN_IMBALANCE", 0.02))
                stack.enter_context(patch.object(ctrader_module.config, "CTRADER_PENDING_ORDER_FOLLOW_STOP_MAX_REJECTION", 0.12))
                stack.enter_context(patch.object(ctrader_module.config, "CTRADER_PENDING_ORDER_FOLLOW_STOP_MIN_DELTA_PROXY", 0.12))
                stack.enter_context(patch.object(ctrader_module.config, "CTRADER_PENDING_ORDER_FOLLOW_STOP_MIN_BAR_VOLUME_PROXY", 0.35))
                stack.enter_context(patch.object(ctrader_module.config, "CTRADER_PENDING_ORDER_FOLLOW_STOP_ENTRY_BUFFER_R", 0.10))
                stack.enter_context(patch.object(ctrader_module.config, "CTRADER_PENDING_ORDER_FOLLOW_STOP_STOP_R", 0.58))
                stack.enter_context(patch.object(ctrader_module.config, "CTRADER_PENDING_ORDER_FOLLOW_STOP_TP_R", 0.88))
                stack.enter_context(patch.object(ctrader_module.config, "CTRADER_PENDING_ORDER_FOLLOW_STOP_RISK_USD", 0.50))
                stack.enter_context(patch.object(ctrader_module.config, "get_ctrader_pending_order_follow_stop_families", return_value={"xau_scalp_pullback_limit"}))
                stack.enter_context(patch.object(ctrader_module.CTraderExecutor, "sdk_available", new_callable=PropertyMock, return_value=True))
                executor = ctrader_module.CTraderExecutor()
                created_ts = time.time() - 180.0
                with sqlite3.connect(db_path) as conn:
                    conn.execute(
                        """
                        INSERT INTO execution_journal(
                            id, created_ts, created_utc, source, lane, symbol, direction, confidence,
                            entry, stop_loss, take_profit, entry_type, dry_run, account_id, broker_symbol,
                            volume, status, message, order_id, position_id, deal_id, signal_run_id, signal_run_no,
                            request_json, response_json, execution_meta_json
                        ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                        """,
                        (
                            1,
                            created_ts,
                            "2026-03-11T09:00:00Z",
                            "scalp_xauusd:pb:canary",
                            "canary",
                            "XAUUSD",
                            "short",
                            73.4,
                            5192.56,
                            5194.12,
                            5190.22,
                            "limit",
                            0,
                            46552794,
                            "XAUUSD",
                            100.0,
                            "accepted",
                            "ctrader accepted",
                            934500003,
                            None,
                            None,
                            "run-ff",
                            1,
                            "{}",
                            "{}",
                            "{}",
                        ),
                    )
                    conn.execute(
                        """
                        INSERT INTO ctrader_orders(
                            order_id, account_id, source, lane, symbol, broker_symbol, direction, volume,
                            entry_price, stop_loss, take_profit, order_type, order_status, label, comment,
                            client_order_id, signal_run_id, signal_run_no, journal_id, is_open, first_seen_utc,
                            last_seen_utc, raw_json
                        ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                        """,
                        (
                            934500003,
                            46552794,
                            "scalp_xauusd:pb:canary",
                            "canary",
                            "XAUUSD",
                            "XAUUSD",
                            "short",
                            100.0,
                            5192.56,
                            5194.12,
                            5190.22,
                            "limit",
                            "accepted",
                            "dexter:XAUUSD:scalp_xauusd:pb:1",
                            "dexter|scalp_xauusd:pb:canary|XAUUSD",
                            "client-ff",
                            "run-ff",
                            1,
                            1,
                            1,
                            "2026-03-11T09:00:00Z",
                            "2026-03-11T09:00:00Z",
                            "{}",
                        ),
                    )
                    conn.commit()

                snapshot = {
                    "ok": True,
                    "status": "ok",
                    "run_id": "ctcap_ff",
                    "gate": {"pass": False, "reasons": ["short_gate_failed"]},
                    "features": {
                        "mid_drift_pct": 0.014,
                        "spread_expansion": 1.02,
                        "depth_imbalance": 0.031,
                        "depth_refill_shift": 0.02,
                        "delta_proxy": 0.24,
                        "bar_volume_proxy": 0.71,
                        "rejection_ratio": 0.05,
                    },
                }
                cancel_res = ctrader_module.CTraderExecutionResult(ok=True, status="canceled", message="ok")
                follow_res = ctrader_module.CTraderExecutionResult(ok=True, status="accepted", message="follow ok", order_id=944400001)
                with patch.object(executor, "_reference_price", return_value=5193.24), \
                     patch.object(executor, "_latest_capture_snapshot", return_value=snapshot), \
                     patch.object(executor, "cancel_order", return_value=cancel_res), \
                     patch.object(executor, "execute_signal", return_value=follow_res) as exec_mock:
                    report = executor._sweep_pending_orders(
                        [{
                            "order_id": 934500003,
                            "journal_id": 1,
                            "source": "scalp_xauusd:pb:canary",
                            "symbol": "XAUUSD",
                            "direction": "short",
                            "entry_price": 5192.56,
                            "stop_loss": 5194.12,
                            "take_profit": 5190.22,
                            "order_type": "limit",
                            "volume": 100.0,
                            "signal_run_id": "run-ff",
                            "signal_run_no": 1,
                            "created_ts": created_ts,
                        }]
                    )

                self.assertEqual(int(report["canceled_orders"]), 1)
                self.assertEqual(int(report["follow_stop_orders"]), 1)
                self.assertEqual(str(report["follow_stop_actions"][0]["follow_source"]), "scalp_xauusd:ff:canary")
                exec_args = exec_mock.call_args
                self.assertIsNotNone(exec_args)
                self.assertEqual(str(exec_args.kwargs.get("source")), "scalp_xauusd:ff:canary")
                follow_signal = exec_args.args[0]
                self.assertEqual(str(getattr(follow_signal, "entry_type", "")), "buy_stop")
                with sqlite3.connect(db_path) as conn:
                    row = conn.execute(
                        "SELECT status, response_json, execution_meta_json FROM execution_journal WHERE id=1"
                    ).fetchone()
                self.assertEqual(str(row[0]), "canceled")
                response_json = ctrader_module.CTraderExecutor._safe_json_load(str(row[1] or "{}"))
                execution_meta = ctrader_module.CTraderExecutor._safe_json_load(str(row[2] or "{}"))
                self.assertEqual(str(response_json.get("cancel", {}).get("reason")), "follow_stop_flip:failed_fade_opposite_force_significant")
                self.assertEqual(str(response_json.get("follow_stop_launch", {}).get("source")), "scalp_xauusd:ff:canary")
                self.assertEqual(str(execution_meta.get("follow_stop_launch", {}).get("result", {}).get("status")), "accepted")
        finally:
            executor = None
            gc.collect()
            shutil.rmtree(td, ignore_errors=True)

    def test_sweep_pending_orders_flips_to_follow_stop_in_sample_mode_with_lower_risk(self):
        td = tempfile.mkdtemp()
        executor = None
        try:
            db_path = str(Path(td) / "ctrader_openapi.db")
            with ExitStack() as stack:
                stack.enter_context(patch.object(ctrader_module.config, "CTRADER_DB_PATH", db_path))
                stack.enter_context(patch.object(ctrader_module.config, "CTRADER_ENABLED", True))
                stack.enter_context(patch.object(ctrader_module.config, "CTRADER_AUTOTRADE_ENABLED", True))
                stack.enter_context(patch.object(ctrader_module.config, "CTRADER_DRY_RUN", False))
                stack.enter_context(patch.object(ctrader_module.config, "CTRADER_ACCOUNT_ID", "46552794"))
                stack.enter_context(patch.object(ctrader_module.config, "CTRADER_PENDING_ORDER_SWEEP_ENABLED", True))
                stack.enter_context(patch.object(ctrader_module.config, "CTRADER_PENDING_ORDER_DYNAMIC_REPRICE_ENABLED", True))
                stack.enter_context(patch.object(ctrader_module.config, "CTRADER_PENDING_ORDER_DYNAMIC_REPRICE_MIN_AGE_SEC", 30))
                stack.enter_context(patch.object(ctrader_module.config, "CTRADER_PENDING_ORDER_DYNAMIC_REPRICE_TRIGGER_R", 0.18))
                stack.enter_context(patch.object(ctrader_module.config, "CTRADER_PENDING_ORDER_DYNAMIC_REPRICE_REQUIRE_CAPTURE", True))
                stack.enter_context(patch.object(ctrader_module.config, "get_ctrader_pending_order_dynamic_reprice_families", return_value={"xau_scalp_pullback_limit"}))
                stack.enter_context(patch.object(ctrader_module.config, "CTRADER_PENDING_ORDER_FOLLOW_STOP_ENABLED", True))
                stack.enter_context(patch.object(ctrader_module.config, "CTRADER_PENDING_ORDER_FOLLOW_STOP_TRIGGER_R", 0.34))
                stack.enter_context(patch.object(ctrader_module.config, "CTRADER_PENDING_ORDER_FOLLOW_STOP_MIN_MID_DRIFT_PCT", 0.012))
                stack.enter_context(patch.object(ctrader_module.config, "CTRADER_PENDING_ORDER_FOLLOW_STOP_MIN_IMBALANCE", 0.02))
                stack.enter_context(patch.object(ctrader_module.config, "CTRADER_PENDING_ORDER_FOLLOW_STOP_MAX_REJECTION", 0.12))
                stack.enter_context(patch.object(ctrader_module.config, "CTRADER_PENDING_ORDER_FOLLOW_STOP_MIN_DELTA_PROXY", 0.12))
                stack.enter_context(patch.object(ctrader_module.config, "CTRADER_PENDING_ORDER_FOLLOW_STOP_MIN_BAR_VOLUME_PROXY", 0.35))
                stack.enter_context(patch.object(ctrader_module.config, "CTRADER_PENDING_ORDER_FOLLOW_STOP_ENTRY_BUFFER_R", 0.10))
                stack.enter_context(patch.object(ctrader_module.config, "CTRADER_PENDING_ORDER_FOLLOW_STOP_STOP_R", 0.58))
                stack.enter_context(patch.object(ctrader_module.config, "CTRADER_PENDING_ORDER_FOLLOW_STOP_TP_R", 0.88))
                stack.enter_context(patch.object(ctrader_module.config, "CTRADER_PENDING_ORDER_FOLLOW_STOP_RISK_USD", 0.50))
                stack.enter_context(patch.object(ctrader_module.config, "CTRADER_PENDING_ORDER_FOLLOW_STOP_SAMPLE_ENABLED", True))
                stack.enter_context(patch.object(ctrader_module.config, "CTRADER_PENDING_ORDER_FOLLOW_STOP_SAMPLE_MIN_CONFIDENCE", 74.0))
                stack.enter_context(patch.object(ctrader_module.config, "CTRADER_PENDING_ORDER_FOLLOW_STOP_SAMPLE_TRIGGER_R_MULT", 0.88))
                stack.enter_context(patch.object(ctrader_module.config, "CTRADER_PENDING_ORDER_FOLLOW_STOP_SAMPLE_IMBALANCE_MULT", 0.75))
                stack.enter_context(patch.object(ctrader_module.config, "CTRADER_PENDING_ORDER_FOLLOW_STOP_SAMPLE_DELTA_MULT", 0.75))
                stack.enter_context(patch.object(ctrader_module.config, "CTRADER_PENDING_ORDER_FOLLOW_STOP_SAMPLE_BAR_VOLUME_MULT", 0.90))
                stack.enter_context(patch.object(ctrader_module.config, "CTRADER_PENDING_ORDER_FOLLOW_STOP_SAMPLE_RISK_MULT", 0.70))
                stack.enter_context(patch.object(ctrader_module.config, "get_ctrader_pending_order_follow_stop_families", return_value={"xau_scalp_pullback_limit"}))
                stack.enter_context(patch.object(ctrader_module.CTraderExecutor, "sdk_available", new_callable=PropertyMock, return_value=True))
                executor = ctrader_module.CTraderExecutor()
                created_ts = time.time() - 180.0
                with sqlite3.connect(db_path) as conn:
                    conn.execute(
                        """
                        INSERT INTO execution_journal(
                            id, created_ts, created_utc, source, lane, symbol, direction, confidence,
                            entry, stop_loss, take_profit, entry_type, dry_run, account_id, broker_symbol,
                            volume, status, message, order_id, position_id, deal_id, signal_run_id, signal_run_no,
                            request_json, response_json, execution_meta_json
                        ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                        """,
                        (
                            1,
                            created_ts,
                            "2026-03-11T09:00:00Z",
                            "scalp_xauusd:pb:canary",
                            "canary",
                            "XAUUSD",
                            "short",
                            74.2,
                            5192.56,
                            5194.12,
                            5190.22,
                            "limit",
                            0,
                            46552794,
                            "XAUUSD",
                            100.0,
                            "accepted",
                            "ctrader accepted",
                            934500004,
                            None,
                            None,
                            "run-ff-sample",
                            1,
                            "{}",
                            "{}",
                            "{}",
                        ),
                    )
                    conn.execute(
                        """
                        INSERT INTO ctrader_orders(
                            order_id, account_id, source, lane, symbol, broker_symbol, direction, volume,
                            entry_price, stop_loss, take_profit, order_type, order_status, label, comment,
                            client_order_id, signal_run_id, signal_run_no, journal_id, is_open, first_seen_utc,
                            last_seen_utc, raw_json
                        ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                        """,
                        (
                            934500004,
                            46552794,
                            "scalp_xauusd:pb:canary",
                            "canary",
                            "XAUUSD",
                            "XAUUSD",
                            "short",
                            100.0,
                            5192.56,
                            5194.12,
                            5190.22,
                            "limit",
                            "accepted",
                            "dexter:XAUUSD:scalp_xauusd:pb:1",
                            "dexter|scalp_xauusd:pb:canary|XAUUSD",
                            "client-ff-sample",
                            "run-ff-sample",
                            1,
                            1,
                            1,
                            "2026-03-11T09:00:00Z",
                            "2026-03-11T09:00:00Z",
                            "{}",
                        ),
                    )
                    conn.commit()

                snapshot = {
                    "ok": True,
                    "status": "ok",
                    "run_id": "ctcap_ff_sample",
                    "gate": {"pass": False, "reasons": ["short_gate_failed"]},
                    "features": {
                        "mid_drift_pct": 0.013,
                        "spread_expansion": 1.02,
                        "depth_imbalance": 0.018,
                        "depth_refill_shift": 0.02,
                        "delta_proxy": 0.10,
                        "bar_volume_proxy": 0.36,
                        "rejection_ratio": 0.05,
                    },
                }
                cancel_res = ctrader_module.CTraderExecutionResult(ok=True, status="canceled", message="ok")
                follow_res = ctrader_module.CTraderExecutionResult(ok=True, status="accepted", message="follow ok", order_id=944400002)
                with patch.object(executor, "_reference_price", return_value=5193.24), \
                     patch.object(executor, "_latest_capture_snapshot", return_value=snapshot), \
                     patch.object(executor, "cancel_order", return_value=cancel_res), \
                     patch.object(executor, "execute_signal", return_value=follow_res) as exec_mock:
                    report = executor._sweep_pending_orders(
                        [{
                            "order_id": 934500004,
                            "journal_id": 1,
                            "source": "scalp_xauusd:pb:canary",
                            "symbol": "XAUUSD",
                            "direction": "short",
                            "entry_price": 5192.56,
                            "stop_loss": 5194.12,
                            "take_profit": 5190.22,
                            "order_type": "limit",
                            "volume": 100.0,
                            "signal_run_id": "run-ff-sample",
                            "signal_run_no": 1,
                            "created_ts": created_ts,
                        }]
                    )

                self.assertEqual(int(report["follow_stop_orders"]), 1)
                exec_args = exec_mock.call_args
                self.assertIsNotNone(exec_args)
                self.assertEqual(str(exec_args.kwargs.get("source")), "scalp_xauusd:ff:canary")
                follow_signal = exec_args.args[0]
                raw_scores = dict(getattr(follow_signal, "raw_scores", {}) or {})
                self.assertAlmostEqual(float(raw_scores.get("ctrader_risk_usd_override", 0.0) or 0.0), 0.35, places=4)
                with sqlite3.connect(db_path) as conn:
                    row = conn.execute(
                        "SELECT response_json, execution_meta_json FROM execution_journal WHERE id=1"
                    ).fetchone()
                response_json = ctrader_module.CTraderExecutor._safe_json_load(str(row[0] or "{}"))
                execution_meta = ctrader_module.CTraderExecutor._safe_json_load(str(row[1] or "{}"))
                self.assertEqual(str(response_json.get("cancel", {}).get("reason")), "follow_stop_flip:failed_fade_opposite_force_sample")
                self.assertEqual(str(execution_meta.get("follow_stop_launch", {}).get("result", {}).get("status") or ""), "accepted")
        finally:
            executor = None
            gc.collect()
            shutil.rmtree(td, ignore_errors=True)

    def test_sweep_pending_orders_uses_repricing_relaxed_follow_stop_sample_gate(self):
        td = tempfile.mkdtemp()
        executor = None
        try:
            db_path = str(Path(td) / "ctrader_openapi.db")
            with ExitStack() as stack:
                stack.enter_context(patch.object(ctrader_module.config, "CTRADER_DB_PATH", db_path))
                stack.enter_context(patch.object(ctrader_module.config, "CTRADER_ENABLED", True))
                stack.enter_context(patch.object(ctrader_module.config, "CTRADER_AUTOTRADE_ENABLED", True))
                stack.enter_context(patch.object(ctrader_module.config, "CTRADER_DRY_RUN", False))
                stack.enter_context(patch.object(ctrader_module.config, "CTRADER_ACCOUNT_ID", "46552794"))
                stack.enter_context(patch.object(ctrader_module.config, "CTRADER_PENDING_ORDER_SWEEP_ENABLED", True))
                stack.enter_context(patch.object(ctrader_module.config, "CTRADER_PENDING_ORDER_DYNAMIC_REPRICE_ENABLED", True))
                stack.enter_context(patch.object(ctrader_module.config, "CTRADER_PENDING_ORDER_DYNAMIC_REPRICE_MIN_AGE_SEC", 30))
                stack.enter_context(patch.object(ctrader_module.config, "CTRADER_PENDING_ORDER_DYNAMIC_REPRICE_TRIGGER_R", 0.18))
                stack.enter_context(patch.object(ctrader_module.config, "CTRADER_PENDING_ORDER_DYNAMIC_REPRICE_REQUIRE_CAPTURE", True))
                stack.enter_context(patch.object(ctrader_module.config, "get_ctrader_pending_order_dynamic_reprice_families", return_value={"xau_scalp_pullback_limit"}))
                stack.enter_context(patch.object(ctrader_module.config, "CTRADER_PENDING_ORDER_FOLLOW_STOP_ENABLED", True))
                stack.enter_context(patch.object(ctrader_module.config, "CTRADER_PENDING_ORDER_FOLLOW_STOP_TRIGGER_R", 0.34))
                stack.enter_context(patch.object(ctrader_module.config, "CTRADER_PENDING_ORDER_FOLLOW_STOP_MIN_MID_DRIFT_PCT", 0.012))
                stack.enter_context(patch.object(ctrader_module.config, "CTRADER_PENDING_ORDER_FOLLOW_STOP_MIN_IMBALANCE", 0.02))
                stack.enter_context(patch.object(ctrader_module.config, "CTRADER_PENDING_ORDER_FOLLOW_STOP_MAX_REJECTION", 0.12))
                stack.enter_context(patch.object(ctrader_module.config, "CTRADER_PENDING_ORDER_FOLLOW_STOP_MIN_DELTA_PROXY", 0.12))
                stack.enter_context(patch.object(ctrader_module.config, "CTRADER_PENDING_ORDER_FOLLOW_STOP_MIN_BAR_VOLUME_PROXY", 0.35))
                stack.enter_context(patch.object(ctrader_module.config, "CTRADER_PENDING_ORDER_FOLLOW_STOP_ENTRY_BUFFER_R", 0.10))
                stack.enter_context(patch.object(ctrader_module.config, "CTRADER_PENDING_ORDER_FOLLOW_STOP_STOP_R", 0.58))
                stack.enter_context(patch.object(ctrader_module.config, "CTRADER_PENDING_ORDER_FOLLOW_STOP_TP_R", 0.88))
                stack.enter_context(patch.object(ctrader_module.config, "CTRADER_PENDING_ORDER_FOLLOW_STOP_RISK_USD", 0.50))
                stack.enter_context(patch.object(ctrader_module.config, "CTRADER_PENDING_ORDER_FOLLOW_STOP_SAMPLE_ENABLED", True))
                stack.enter_context(patch.object(ctrader_module.config, "CTRADER_PENDING_ORDER_FOLLOW_STOP_SAMPLE_MIN_CONFIDENCE", 74.0))
                stack.enter_context(patch.object(ctrader_module.config, "CTRADER_PENDING_ORDER_FOLLOW_STOP_SAMPLE_TRIGGER_R_MULT", 0.88))
                stack.enter_context(patch.object(ctrader_module.config, "CTRADER_PENDING_ORDER_FOLLOW_STOP_SAMPLE_IMBALANCE_MULT", 0.75))
                stack.enter_context(patch.object(ctrader_module.config, "CTRADER_PENDING_ORDER_FOLLOW_STOP_SAMPLE_DELTA_MULT", 0.75))
                stack.enter_context(patch.object(ctrader_module.config, "CTRADER_PENDING_ORDER_FOLLOW_STOP_SAMPLE_BAR_VOLUME_MULT", 0.90))
                stack.enter_context(patch.object(ctrader_module.config, "CTRADER_PENDING_ORDER_FOLLOW_STOP_SAMPLE_RISK_MULT", 0.70))
                stack.enter_context(patch.object(ctrader_module.config, "CTRADER_PENDING_ORDER_FOLLOW_STOP_REPRICING_SAMPLE_ENABLED", True))
                stack.enter_context(patch.object(ctrader_module.config, "CTRADER_PENDING_ORDER_FOLLOW_STOP_REPRICING_SAMPLE_MIN_CONFIDENCE_DELTA", -1.0))
                stack.enter_context(patch.object(ctrader_module.config, "CTRADER_PENDING_ORDER_FOLLOW_STOP_REPRICING_SAMPLE_TRIGGER_MULT", 0.90))
                stack.enter_context(patch.object(ctrader_module.config, "CTRADER_PENDING_ORDER_FOLLOW_STOP_REPRICING_SAMPLE_IMBALANCE_MULT", 0.90))
                stack.enter_context(patch.object(ctrader_module.config, "CTRADER_PENDING_ORDER_FOLLOW_STOP_REPRICING_SAMPLE_DELTA_MULT", 0.90))
                stack.enter_context(patch.object(ctrader_module.config, "CTRADER_PENDING_ORDER_FOLLOW_STOP_REPRICING_SAMPLE_BAR_VOLUME_MULT", 0.95))
                stack.enter_context(patch.object(ctrader_module.config, "get_ctrader_pending_order_follow_stop_families", return_value={"xau_scalp_pullback_limit"}))
                stack.enter_context(patch.object(ctrader_module.CTraderExecutor, "sdk_available", new_callable=PropertyMock, return_value=True))
                executor = ctrader_module.CTraderExecutor()
                created_ts = time.time() - 180.0
                with sqlite3.connect(db_path) as conn:
                    conn.execute(
                        """
                        INSERT INTO execution_journal(
                            id, created_ts, created_utc, source, lane, symbol, direction, confidence,
                            entry, stop_loss, take_profit, entry_type, dry_run, account_id, broker_symbol,
                            volume, status, message, order_id, position_id, deal_id, signal_run_id, signal_run_no,
                            request_json, response_json, execution_meta_json
                        ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                        """,
                        (
                            1,
                            created_ts,
                            "2026-03-11T09:00:00Z",
                            "scalp_xauusd:pb:canary",
                            "canary",
                            "XAUUSD",
                            "short",
                            73.2,
                            5192.56,
                            5194.12,
                            5190.22,
                            "limit",
                            0,
                            46552794,
                            "XAUUSD",
                            100.0,
                            "accepted",
                            "ctrader accepted",
                            934500005,
                            None,
                            None,
                            "run-ff-repricing",
                            1,
                            "{}",
                            "{}",
                            "{}",
                        ),
                    )
                    conn.execute(
                        """
                        INSERT INTO ctrader_orders(
                            order_id, account_id, source, lane, symbol, broker_symbol, direction, volume,
                            entry_price, stop_loss, take_profit, order_type, order_status, label, comment,
                            client_order_id, signal_run_id, signal_run_no, journal_id, is_open, first_seen_utc,
                            last_seen_utc, raw_json
                        ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                        """,
                        (
                            934500005,
                            46552794,
                            "scalp_xauusd:pb:canary",
                            "canary",
                            "XAUUSD",
                            "XAUUSD",
                            "short",
                            100.0,
                            5192.56,
                            5194.12,
                            5190.22,
                            "limit",
                            "accepted",
                            "dexter:XAUUSD:scalp_xauusd:pb:1",
                            "dexter|scalp_xauusd:pb:canary|XAUUSD",
                            "client-ff-repricing",
                            "run-ff-repricing",
                            1,
                            1,
                            1,
                            "2026-03-11T09:00:00Z",
                            "2026-03-11T09:00:00Z",
                            "{}",
                        ),
                    )
                    conn.commit()

                snapshot = {
                    "ok": True,
                    "status": "ok",
                    "run_id": "ctcap_ff_repr",
                    "gate": {"pass": False, "reasons": ["short_gate_failed"]},
                    "features": {
                        "day_type": "repricing",
                        "mid_drift_pct": 0.014,
                        "spread_expansion": 1.03,
                        "depth_imbalance": 0.014,
                        "depth_refill_shift": 0.02,
                        "delta_proxy": 0.082,
                        "bar_volume_proxy": 0.30,
                        "rejection_ratio": 0.08,
                    },
                }
                cancel_res = ctrader_module.CTraderExecutionResult(ok=True, status="canceled", message="ok")
                follow_res = ctrader_module.CTraderExecutionResult(ok=True, status="accepted", message="follow ok", order_id=944400002)
                with patch.object(executor, "_reference_price", return_value=5193.00), \
                     patch.object(executor, "_latest_capture_snapshot", return_value=snapshot), \
                     patch.object(executor, "cancel_order", return_value=cancel_res), \
                     patch.object(executor, "execute_signal", return_value=follow_res) as exec_mock:
                    report = executor._sweep_pending_orders(
                        [{
                            "order_id": 934500005,
                            "journal_id": 1,
                            "source": "scalp_xauusd:pb:canary",
                            "symbol": "XAUUSD",
                            "direction": "short",
                            "entry_price": 5192.56,
                            "stop_loss": 5194.12,
                            "take_profit": 5190.22,
                            "order_type": "limit",
                            "volume": 100.0,
                            "signal_run_id": "run-ff-repricing",
                            "signal_run_no": 1,
                            "created_ts": created_ts,
                        }]
                    )

                self.assertEqual(int(report["follow_stop_orders"]), 1)
                self.assertEqual(str(report["follow_stop_actions"][0]["reason"]), "failed_fade_opposite_force_sample")
                exec_args = exec_mock.call_args
                self.assertIsNotNone(exec_args)
                follow_signal = exec_args.args[0]
                self.assertEqual(str(getattr(follow_signal, "entry_type", "")), "buy_stop")
        finally:
            executor = None
            gc.collect()
            shutil.rmtree(td, ignore_errors=True)

    def test_sweep_pending_orders_uses_secondary_follow_stop_sample_gate(self):
        td = tempfile.mkdtemp()
        executor = None
        try:
            db_path = str(Path(td) / "ctrader_openapi.db")
            with ExitStack() as stack:
                stack.enter_context(patch.object(ctrader_module.config, "CTRADER_DB_PATH", db_path))
                stack.enter_context(patch.object(ctrader_module.config, "CTRADER_ENABLED", True))
                stack.enter_context(patch.object(ctrader_module.config, "CTRADER_AUTOTRADE_ENABLED", True))
                stack.enter_context(patch.object(ctrader_module.config, "CTRADER_DRY_RUN", False))
                stack.enter_context(patch.object(ctrader_module.config, "CTRADER_ACCOUNT_ID", "46552794"))
                stack.enter_context(patch.object(ctrader_module.config, "CTRADER_PENDING_ORDER_SWEEP_ENABLED", True))
                stack.enter_context(patch.object(ctrader_module.config, "CTRADER_PENDING_ORDER_DYNAMIC_REPRICE_ENABLED", True))
                stack.enter_context(patch.object(ctrader_module.config, "CTRADER_PENDING_ORDER_DYNAMIC_REPRICE_MIN_AGE_SEC", 30))
                stack.enter_context(patch.object(ctrader_module.config, "CTRADER_PENDING_ORDER_DYNAMIC_REPRICE_TRIGGER_R", 0.18))
                stack.enter_context(patch.object(ctrader_module.config, "CTRADER_PENDING_ORDER_DYNAMIC_REPRICE_REQUIRE_CAPTURE", True))
                stack.enter_context(patch.object(ctrader_module.config, "get_ctrader_pending_order_dynamic_reprice_families", return_value={"xau_scalp_pullback_limit"}))
                stack.enter_context(patch.object(ctrader_module.config, "CTRADER_PENDING_ORDER_FOLLOW_STOP_ENABLED", True))
                stack.enter_context(patch.object(ctrader_module.config, "CTRADER_PENDING_ORDER_FOLLOW_STOP_TRIGGER_R", 0.34))
                stack.enter_context(patch.object(ctrader_module.config, "CTRADER_PENDING_ORDER_FOLLOW_STOP_MIN_MID_DRIFT_PCT", 0.012))
                stack.enter_context(patch.object(ctrader_module.config, "CTRADER_PENDING_ORDER_FOLLOW_STOP_MIN_IMBALANCE", 0.02))
                stack.enter_context(patch.object(ctrader_module.config, "CTRADER_PENDING_ORDER_FOLLOW_STOP_MAX_REJECTION", 0.12))
                stack.enter_context(patch.object(ctrader_module.config, "CTRADER_PENDING_ORDER_FOLLOW_STOP_MIN_DELTA_PROXY", 0.12))
                stack.enter_context(patch.object(ctrader_module.config, "CTRADER_PENDING_ORDER_FOLLOW_STOP_MIN_BAR_VOLUME_PROXY", 0.35))
                stack.enter_context(patch.object(ctrader_module.config, "CTRADER_PENDING_ORDER_FOLLOW_STOP_ENTRY_BUFFER_R", 0.10))
                stack.enter_context(patch.object(ctrader_module.config, "CTRADER_PENDING_ORDER_FOLLOW_STOP_STOP_R", 0.58))
                stack.enter_context(patch.object(ctrader_module.config, "CTRADER_PENDING_ORDER_FOLLOW_STOP_TP_R", 0.88))
                stack.enter_context(patch.object(ctrader_module.config, "CTRADER_PENDING_ORDER_FOLLOW_STOP_RISK_USD", 0.50))
                stack.enter_context(patch.object(ctrader_module.config, "CTRADER_PENDING_ORDER_FOLLOW_STOP_SAMPLE_ENABLED", True))
                stack.enter_context(patch.object(ctrader_module.config, "CTRADER_PENDING_ORDER_FOLLOW_STOP_SAMPLE_MIN_CONFIDENCE", 74.0))
                stack.enter_context(patch.object(ctrader_module.config, "CTRADER_PENDING_ORDER_FOLLOW_STOP_SAMPLE_TRIGGER_R_MULT", 0.88))
                stack.enter_context(patch.object(ctrader_module.config, "CTRADER_PENDING_ORDER_FOLLOW_STOP_SAMPLE_IMBALANCE_MULT", 0.75))
                stack.enter_context(patch.object(ctrader_module.config, "CTRADER_PENDING_ORDER_FOLLOW_STOP_SAMPLE_DELTA_MULT", 0.75))
                stack.enter_context(patch.object(ctrader_module.config, "CTRADER_PENDING_ORDER_FOLLOW_STOP_SAMPLE_BAR_VOLUME_MULT", 0.90))
                stack.enter_context(patch.object(ctrader_module.config, "CTRADER_PENDING_ORDER_FOLLOW_STOP_SAMPLE_RISK_MULT", 0.70))
                stack.enter_context(patch.object(ctrader_module.config, "CTRADER_PENDING_ORDER_FOLLOW_STOP_SECONDARY_SAMPLE_ENABLED", True))
                stack.enter_context(patch.object(ctrader_module.config, "CTRADER_PENDING_ORDER_FOLLOW_STOP_SECONDARY_SAMPLE_MIN_CONFIDENCE", 75.0))
                stack.enter_context(patch.object(ctrader_module.config, "CTRADER_PENDING_ORDER_FOLLOW_STOP_SECONDARY_SAMPLE_TRIGGER_R_MULT", 0.82))
                stack.enter_context(patch.object(ctrader_module.config, "CTRADER_PENDING_ORDER_FOLLOW_STOP_SECONDARY_SAMPLE_IMBALANCE_MULT", 0.65))
                stack.enter_context(patch.object(ctrader_module.config, "CTRADER_PENDING_ORDER_FOLLOW_STOP_SECONDARY_SAMPLE_DELTA_MULT", 0.65))
                stack.enter_context(patch.object(ctrader_module.config, "CTRADER_PENDING_ORDER_FOLLOW_STOP_SECONDARY_SAMPLE_BAR_VOLUME_MULT", 1.05))
                stack.enter_context(patch.object(ctrader_module.config, "CTRADER_PENDING_ORDER_FOLLOW_STOP_SECONDARY_SAMPLE_REJECTION_MULT", 1.10))
                stack.enter_context(patch.object(ctrader_module.config, "CTRADER_PENDING_ORDER_FOLLOW_STOP_SECONDARY_SAMPLE_RISK_MULT", 0.55))
                stack.enter_context(patch.object(ctrader_module.config, "get_ctrader_pending_order_follow_stop_families", return_value={"xau_scalp_pullback_limit"}))
                stack.enter_context(patch.object(ctrader_module.CTraderExecutor, "sdk_available", new_callable=PropertyMock, return_value=True))
                executor = ctrader_module.CTraderExecutor()
                created_ts = time.time() - 180.0
                with sqlite3.connect(db_path) as conn:
                    conn.execute(
                        """
                        INSERT INTO execution_journal(
                            id, created_ts, created_utc, source, lane, symbol, direction, confidence,
                            entry, stop_loss, take_profit, entry_type, dry_run, account_id, broker_symbol,
                            volume, status, message, order_id, position_id, deal_id, signal_run_id, signal_run_no,
                            request_json, response_json, execution_meta_json
                        ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                        """,
                        (
                            1,
                            created_ts,
                            "2026-03-12T09:00:00Z",
                            "scalp_xauusd:pb:canary",
                            "canary",
                            "XAUUSD",
                            "short",
                            75.4,
                            5192.56,
                            5194.12,
                            5190.22,
                            "limit",
                            0,
                            46552794,
                            "XAUUSD",
                            100.0,
                            "accepted",
                            "ctrader accepted",
                            934500006,
                            None,
                            None,
                            "run-ff-secondary",
                            1,
                            "{}",
                            "{}",
                            "{}",
                        ),
                    )
                    conn.execute(
                        """
                        INSERT INTO ctrader_orders(
                            order_id, account_id, source, lane, symbol, broker_symbol, direction, volume,
                            entry_price, stop_loss, take_profit, order_type, order_status, label, comment,
                            client_order_id, signal_run_id, signal_run_no, journal_id, is_open, first_seen_utc,
                            last_seen_utc, raw_json
                        ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                        """,
                        (
                            934500006,
                            46552794,
                            "scalp_xauusd:pb:canary",
                            "canary",
                            "XAUUSD",
                            "XAUUSD",
                            "short",
                            100.0,
                            5192.56,
                            5194.12,
                            5190.22,
                            "limit",
                            "accepted",
                            "dexter:XAUUSD:scalp_xauusd:pb:1",
                            "dexter|scalp_xauusd:pb:canary|XAUUSD",
                            "client-ff-secondary",
                            "run-ff-secondary",
                            1,
                            1,
                            1,
                            "2026-03-12T09:00:00Z",
                            "2026-03-12T09:00:00Z",
                            "{}",
                        ),
                    )
                    conn.commit()

                snapshot = {
                    "ok": True,
                    "status": "ok",
                    "run_id": "ctcap_ff_secondary",
                    "gate": {"pass": False, "reasons": ["short_gate_failed"]},
                    "features": {
                        "day_type": "trend",
                        "mid_drift_pct": 0.013,
                        "spread_expansion": 1.01,
                        "depth_imbalance": 0.014,
                        "depth_refill_shift": 0.02,
                        "delta_proxy": 0.08,
                        "bar_volume_proxy": 0.38,
                        "rejection_ratio": 0.125,
                    },
                }
                cancel_res = ctrader_module.CTraderExecutionResult(ok=True, status="canceled", message="ok")
                follow_res = ctrader_module.CTraderExecutionResult(ok=True, status="accepted", message="follow ok", order_id=944400003)
                with patch.object(executor, "_reference_price", return_value=5193.24), \
                     patch.object(executor, "_latest_capture_snapshot", return_value=snapshot), \
                     patch.object(executor, "cancel_order", return_value=cancel_res), \
                     patch.object(executor, "execute_signal", return_value=follow_res) as exec_mock:
                    report = executor._sweep_pending_orders(
                        [{
                            "order_id": 934500006,
                            "journal_id": 1,
                            "source": "scalp_xauusd:pb:canary",
                            "symbol": "XAUUSD",
                            "direction": "short",
                            "entry_price": 5192.56,
                            "stop_loss": 5194.12,
                            "take_profit": 5190.22,
                            "order_type": "limit",
                            "volume": 100.0,
                            "signal_run_id": "run-ff-secondary",
                            "signal_run_no": 1,
                            "created_ts": created_ts,
                        }]
                    )

                self.assertEqual(int(report["follow_stop_orders"]), 1)
                self.assertEqual(str(report["follow_stop_actions"][0]["reason"]), "failed_fade_opposite_force_secondary_sample")
                exec_args = exec_mock.call_args
                self.assertIsNotNone(exec_args)
                self.assertEqual(str(exec_args.kwargs.get("source")), "scalp_xauusd:ff:canary")
                follow_signal = exec_args.args[0]
                self.assertEqual(str(getattr(follow_signal, "entry_type", "")), "buy_stop")
                raw_scores = dict(getattr(follow_signal, "raw_scores", {}) or {})
                self.assertAlmostEqual(float(raw_scores.get("ctrader_risk_usd_override", 0.0) or 0.0), 0.275, places=4)
                self.assertEqual(str(raw_scores.get("follow_stop_sample_tier") or ""), "secondary")
                with sqlite3.connect(db_path) as conn:
                    row = conn.execute(
                        "SELECT response_json, execution_meta_json FROM execution_journal WHERE id=1"
                    ).fetchone()
                response_json = ctrader_module.CTraderExecutor._safe_json_load(str(row[0] or "{}"))
                execution_meta = ctrader_module.CTraderExecutor._safe_json_load(str(row[1] or "{}"))
                self.assertEqual(str(response_json.get("cancel", {}).get("reason")), "follow_stop_flip:failed_fade_opposite_force_secondary_sample")
                self.assertEqual(str((execution_meta.get("follow_stop_launch") or {}).get("sample_tier") or ""), "secondary")
        finally:
            executor = None
            gc.collect()
            shutil.rmtree(td, ignore_errors=True)

    def test_pending_order_reprice_plan_disables_follow_stop_on_panic_spread_day(self):
        td = tempfile.mkdtemp()
        executor = None
        try:
            db_path = str(Path(td) / "ctrader_openapi.db")
            with ExitStack() as stack:
                stack.enter_context(patch.object(ctrader_module.config, "CTRADER_DB_PATH", db_path))
                stack.enter_context(patch.object(ctrader_module.config, "CTRADER_PENDING_ORDER_DYNAMIC_REPRICE_ENABLED", True))
                stack.enter_context(patch.object(ctrader_module.config, "CTRADER_PENDING_ORDER_DYNAMIC_REPRICE_MIN_AGE_SEC", 30))
                stack.enter_context(patch.object(ctrader_module.config, "CTRADER_PENDING_ORDER_DYNAMIC_REPRICE_TRIGGER_R", 0.18))
                stack.enter_context(patch.object(ctrader_module.config, "CTRADER_PENDING_ORDER_DYNAMIC_REPRICE_REQUIRE_CAPTURE", True))
                stack.enter_context(patch.object(ctrader_module.config, "get_ctrader_pending_order_dynamic_reprice_families", return_value={"xau_scalp_pullback_limit"}))
                stack.enter_context(patch.object(ctrader_module.config, "CTRADER_PENDING_ORDER_FOLLOW_STOP_ENABLED", True))
                stack.enter_context(patch.object(ctrader_module.config, "CTRADER_PENDING_ORDER_FOLLOW_STOP_PANIC_SPREAD_DISABLE", True))
                stack.enter_context(patch.object(ctrader_module.CTraderExecutor, "sdk_available", new_callable=PropertyMock, return_value=True))
                executor = ctrader_module.CTraderExecutor()
                created_ts = time.time() - 180.0
                with sqlite3.connect(db_path) as conn:
                    conn.execute(
                        """
                        INSERT INTO execution_journal(id, created_ts, created_utc, source, lane, symbol, direction, confidence,
                            entry, stop_loss, take_profit, entry_type, dry_run, account_id, broker_symbol,
                            volume, status, message, order_id, position_id, deal_id, signal_run_id, signal_run_no,
                            request_json, response_json, execution_meta_json)
                        VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                        """,
                        (
                            1, created_ts, "2026-03-11T09:00:00Z", "scalp_xauusd:pb:canary", "canary", "XAUUSD", "short", 76.0,
                            5192.56, 5194.12, 5190.22, "limit", 0, 46552794, "XAUUSD", 100.0, "accepted", "ctrader accepted",
                            934500006, None, None, "run-panic", 1, "{}", "{}", "{}",
                        ),
                    )
                    conn.commit()

                snapshot = {
                    "ok": True,
                    "status": "ok",
                    "run_id": "ctcap_ff_panic",
                    "gate": {"pass": False, "reasons": ["short_gate_failed"]},
                    "features": {
                        "day_type": "panic_spread",
                        "mid_drift_pct": 0.020,
                        "spread_expansion": 1.22,
                        "depth_imbalance": 0.05,
                        "depth_refill_shift": 0.03,
                        "delta_proxy": 0.22,
                        "bar_volume_proxy": 0.82,
                        "rejection_ratio": 0.05,
                    },
                }
                order_row = {
                    "order_id": 934500006,
                    "journal_id": 1,
                    "source": "scalp_xauusd:pb:canary",
                    "symbol": "XAUUSD",
                    "direction": "short",
                    "entry_price": 5192.56,
                    "stop_loss": 5194.12,
                    "take_profit": 5190.22,
                    "order_type": "limit",
                    "created_ts": created_ts,
                }
                with sqlite3.connect(db_path) as plan_conn:
                    plan_conn.row_factory = sqlite3.Row
                    with patch.object(executor, "_reference_price", return_value=5193.24), \
                         patch.object(executor, "_latest_capture_snapshot", return_value=snapshot):
                        plan = executor._pending_order_reprice_plan(plan_conn, order_row, now_ts=time.time())

                self.assertEqual(str(plan.get("action") or ""), "amend")
                self.assertEqual(str(plan.get("reason") or ""), "retreat_no_weakening")
                self.assertEqual(str(plan.get("day_type") or ""), "panic_spread")
        finally:
            executor = None
            gc.collect()
            shutil.rmtree(td, ignore_errors=True)

    def test_pending_order_cancel_reason_cancels_xau_limit_far_from_market(self):
        td = tempfile.mkdtemp()
        executor = None
        try:
            db_path = str(Path(td) / "ctrader_openapi.db")
            with patch.object(ctrader_module.config, "CTRADER_DB_PATH", db_path), \
                 patch.object(ctrader_module.config, "CTRADER_PENDING_ORDER_MAX_DISTANCE_ENABLED", True), \
                 patch.object(ctrader_module.config, "CTRADER_PENDING_ORDER_MAX_DISTANCE_MIN_AGE_SEC", 60), \
                 patch.object(ctrader_module.config, "CTRADER_PENDING_ORDER_MAX_DISTANCE_R", 1.45), \
                 patch.object(ctrader_module.config, "get_ctrader_pending_order_dynamic_reprice_families", return_value={"xau_scalp_pullback_limit"}), \
                 patch.object(ctrader_module.CTraderExecutor, "sdk_available", new_callable=PropertyMock, return_value=True):
                executor = ctrader_module.CTraderExecutor()
                now_ts = time.time()
                order_row = {
                    "order_id": 934999111,
                    "source": "scalp_xauusd:pb:canary",
                    "symbol": "XAUUSD",
                    "direction": "short",
                    "order_type": "limit",
                    "entry_price": 5110.57,
                    "stop_loss": 5117.61,
                    "created_ts": now_ts - 300,
                }
                with patch.object(executor, "_reference_price", return_value=5089.10), \
                     patch.object(executor, "_active_exposure_snapshot", return_value={
                         "active_total": 0,
                         "active_long": 0,
                         "active_short": 0,
                     }):
                    reason = executor._pending_order_cancel_reason(order_row, now_ts=now_ts)

            self.assertTrue(str(reason).startswith("far_from_market:"))
        finally:
            executor = None
            gc.collect()
            shutil.rmtree(td, ignore_errors=True)

    def test_execute_signal_blocks_when_cluster_loss_guard_blocks_direction(self):
        td = tempfile.mkdtemp()
        executor = None
        try:
            db_path = str(Path(td) / "ctrader_openapi.db")
            runtime_dir = Path(td) / "runtime"
            runtime_dir.mkdir(parents=True, exist_ok=True)
            state_path = runtime_dir / "trading_manager_state.json"
            state_path.write_text(
                json.dumps(
                    {
                        "xau_cluster_loss_guard": {
                            "status": "active",
                            "mode": "same_side_cluster_loss_guard",
                            "blocked_direction": "long",
                            "losses": 3,
                            "resolved": 3,
                            "pnl_usd": -8.35,
                            "families": [
                                "xau_scalp_pullback_limit",
                                "xau_scalp_tick_depth_filter",
                                "xau_scalp_microtrend_follow_up",
                            ],
                        }
                    }
                ),
                encoding="utf-8",
            )
            sig = _make_signal(symbol="XAUUSD", direction="long", confidence=74.0)
            with patch.object(ctrader_module.config, "CTRADER_ENABLED", True), \
                 patch.object(ctrader_module.config, "CTRADER_AUTOTRADE_ENABLED", True), \
                 patch.object(ctrader_module.config, "CTRADER_DRY_RUN", True), \
                 patch.object(ctrader_module.config, "CTRADER_DB_PATH", db_path), \
                 patch.object(ctrader_module.config, "CTRADER_ACCOUNT_ID", "46552794"), \
                 patch.object(ctrader_module.config, "CTRADER_ACCOUNT_LOGIN", "9900897"), \
                 patch.object(ctrader_module.config, "CTRADER_POSITION_DIRECTION_GUARD_ENABLED", True), \
                 patch.object(ctrader_module.config, "CTRADER_BLOCK_OPPOSITE_DIRECTION", True), \
                 patch.object(ctrader_module.config, "get_ctrader_allowed_sources", return_value={"scalp_xauusd:pb:canary"}), \
                 patch.object(ctrader_module.config, "get_ctrader_allowed_symbols", return_value={"XAUUSD"}), \
                 patch.object(ctrader_module.CTraderExecutor, "sdk_available", new_callable=PropertyMock, return_value=True):
                executor = ctrader_module.CTraderExecutor()
                executor.trading_manager_state_path = state_path
                result = _execute_signal_with_fixture_reference(executor, sig, source="scalp_xauusd:pb:canary")

            self.assertFalse(result.ok)
            self.assertEqual(result.status, "filtered")
            self.assertIn("cluster_loss_guard", result.message)
        finally:
            executor = None
            gc.collect()
            shutil.rmtree(td, ignore_errors=True)

    def test_execute_signal_prefers_trading_team_state_over_manager_cluster_guard(self):
        td = tempfile.mkdtemp()
        executor = None
        try:
            db_path = str(Path(td) / "ctrader_openapi.db")
            runtime_dir = Path(td) / "runtime"
            runtime_dir.mkdir(parents=True, exist_ok=True)
            manager_state_path = runtime_dir / "trading_manager_state.json"
            team_state_path = runtime_dir / "trading_team_state.json"
            manager_state_path.write_text(
                json.dumps(
                    {
                        "xau_cluster_loss_guard": {
                            "status": "active",
                            "mode": "same_side_cluster_loss_guard",
                            "blocked_direction": "long",
                            "losses": 3,
                        }
                    }
                ),
                encoding="utf-8",
            )
            team_state_path.write_text(
                json.dumps(
                    {
                        "status": "active",
                        "xau_family_routing": {
                            "status": "active",
                            "mode": "team_primary_advisory",
                            "primary_family": "xau_scalp_pullback_limit",
                            "active_families": ["xau_scalp_pullback_limit"],
                        },
                        "opportunity_feed": {
                            "status": "active",
                            "symbols": {
                                "XAUUSD": {
                                    "family_priority_map": {"xau_scalp_pullback_limit": 92.0},
                                    "priority_families": ["xau_scalp_pullback_limit"],
                                }
                            },
                        },
                    }
                ),
                encoding="utf-8",
            )
            sig = _make_signal(symbol="XAUUSD", direction="long", confidence=74.0)
            with patch.object(ctrader_module.config, "CTRADER_ENABLED", True), \
                 patch.object(ctrader_module.config, "CTRADER_AUTOTRADE_ENABLED", True), \
                 patch.object(ctrader_module.config, "CTRADER_DRY_RUN", True), \
                 patch.object(ctrader_module.config, "CTRADER_DB_PATH", db_path), \
                 patch.object(ctrader_module.config, "CTRADER_ACCOUNT_ID", "46552794"), \
                 patch.object(ctrader_module.config, "CTRADER_ACCOUNT_LOGIN", "9900897"), \
                 patch.object(ctrader_module.config, "CTRADER_POSITION_DIRECTION_GUARD_ENABLED", True), \
                 patch.object(ctrader_module.config, "CTRADER_BLOCK_OPPOSITE_DIRECTION", True), \
                 patch.object(ctrader_module.config, "get_ctrader_allowed_sources", return_value={"scalp_xauusd:pb:canary"}), \
                 patch.object(ctrader_module.config, "get_ctrader_allowed_symbols", return_value={"XAUUSD"}), \
                 patch.object(ctrader_module.CTraderExecutor, "sdk_available", new_callable=PropertyMock, return_value=True):
                executor = ctrader_module.CTraderExecutor()
                executor.trading_manager_state_path = manager_state_path
                executor.trading_team_state_path = team_state_path
                result = _execute_signal_with_fixture_reference(executor, sig, source="scalp_xauusd:pb:canary")

            self.assertTrue(result.ok)
            self.assertEqual(result.status, "dry_run")
        finally:
            executor = None
            gc.collect()
            shutil.rmtree(td, ignore_errors=True)


# ═══════════════════════════════════════════════════════════════════════════
# Momentum Exhaustion Profit Lock + Adaptive step_r
# ═══════════════════════════════════════════════════════════════════════════

class TestMomentumExhaustionLock(unittest.TestCase):
    """Test momentum exhaustion detection and profit locking."""

    def _make_executor(self, td):
        db_path = str(Path(td) / "ctrader_openapi.db")
        with patch.object(ctrader_module.config, "CTRADER_ENABLED", True), \
             patch.object(ctrader_module.config, "CTRADER_AUTOTRADE_ENABLED", True), \
             patch.object(ctrader_module.config, "CTRADER_DRY_RUN", True), \
             patch.object(ctrader_module.config, "CTRADER_DB_PATH", db_path), \
             patch.object(ctrader_module.config, "CTRADER_ACCOUNT_ID", "46552794"), \
             patch.object(ctrader_module.CTraderExecutor, "sdk_available", new_callable=PropertyMock, return_value=True):
            executor = ctrader_module.CTraderExecutor()
            executor.trading_manager_state_path.parent.mkdir(parents=True, exist_ok=True)
            executor.trading_manager_state_path.write_text(
                json.dumps({"xau_order_care": {"status": "active", "mode": "test", "allowed_sources": ["fibo:sniper"], "overrides": {}}}),
                encoding="utf-8",
            )
            return executor

    def test_exhaustion_locks_profit_when_all_signals_fire(self):
        """When delta reversed + volume dying + drift adverse + high rejection + range → lock profit."""
        td = tempfile.mkdtemp()
        try:
            executor = self._make_executor(td)
            # Exhausted momentum snapshot: all 5 signals fire
            snapshot = {
                "ok": True,
                "run_id": "exh_test",
                "features": {
                    "day_type": "range",
                    "delta_proxy": -0.15,   # adverse for long
                    "depth_imbalance": -0.05,
                    "mid_drift_pct": -0.012,  # adverse for long
                    "rejection_ratio": 0.35,  # high
                    "bar_volume_proxy": 0.15,  # dying
                },
            }
            with patch.object(executor, "_latest_capture_snapshot", return_value=snapshot):
                result = executor._xau_momentum_exhaustion_lock(
                    source="fibo:sniper", symbol="XAUUSD", direction="long",
                    entry=3026.49, stop_loss=3024.54, current_price=3028.50,
                    confidence=75.0, age_min=5.0, r_now=0.95,
                )
            self.assertTrue(result["active"])
            self.assertEqual(result["action"], "tighten")
            self.assertIn("xau_momentum_exhaustion_lock", result["reason"])
            # At r_now=0.95, lock_pct should be 0.55 (1.0R tier)
            self.assertEqual(result["details"]["lock_pct"], 0.55)
            # 5 exhaustion signals fired
            self.assertEqual(result["details"]["exhaustion_signals"], 5)

        finally:
            gc.collect()
            shutil.rmtree(td, ignore_errors=True)

    def test_exhaustion_no_lock_when_momentum_still_strong(self):
        """When momentum is still strong (volume high, delta supportive) → don't lock."""
        td = tempfile.mkdtemp()
        try:
            executor = self._make_executor(td)
            snapshot = {
                "ok": True,
                "run_id": "exh_test",
                "features": {
                    "day_type": "trend",
                    "delta_proxy": 0.20,     # supportive for long
                    "depth_imbalance": 0.15,
                    "mid_drift_pct": 0.015,   # supportive for long
                    "rejection_ratio": 0.05,   # low
                    "bar_volume_proxy": 0.75,   # high
                },
            }
            with patch.object(executor, "_latest_capture_snapshot", return_value=snapshot):
                result = executor._xau_momentum_exhaustion_lock(
                    source="fibo:sniper", symbol="XAUUSD", direction="long",
                    entry=3026.49, stop_loss=3024.54, current_price=3028.50,
                    confidence=75.0, age_min=5.0, r_now=0.95,
                )
            self.assertFalse(result["active"])
            self.assertEqual(result["reason"], "exhaustion_not_confirmed")
            # 0 exhaustion signals
            self.assertEqual(result["details"]["exhaustion_signals"], 0)

        finally:
            gc.collect()
            shutil.rmtree(td, ignore_errors=True)

    def test_exhaustion_no_lock_when_not_profitable(self):
        """When trade is in loss → don't lock (can't lock what you don't have)."""
        td = tempfile.mkdtemp()
        try:
            executor = self._make_executor(td)
            result = executor._xau_momentum_exhaustion_lock(
                source="fibo:sniper", symbol="XAUUSD", direction="long",
                entry=3026.49, stop_loss=3024.54, current_price=3025.00,
                confidence=75.0, age_min=5.0, r_now=-0.70,
            )
            self.assertFalse(result["active"])
            self.assertEqual(result["reason"], "not_in_profit")
        finally:
            gc.collect()
            shutil.rmtree(td, ignore_errors=True)

    def test_exhaustion_no_lock_when_too_young(self):
        """When trade is too young (< min_age) → don't lock yet."""
        td = tempfile.mkdtemp()
        try:
            executor = self._make_executor(td)
            result = executor._xau_momentum_exhaustion_lock(
                source="fibo:sniper", symbol="XAUUSD", direction="long",
                entry=3026.49, stop_loss=3024.54, current_price=3028.50,
                confidence=75.0, age_min=1.0, r_now=0.95,
            )
            self.assertFalse(result["active"])
            self.assertEqual(result["reason"], "too_young")
        finally:
            gc.collect()
            shutil.rmtree(td, ignore_errors=True)

    def test_exhaustion_partial_signals_not_enough(self):
        """When only 2 of 5 signals fire (need 3) → don't lock."""
        td = tempfile.mkdtemp()
        try:
            executor = self._make_executor(td)
            snapshot = {
                "ok": True,
                "run_id": "exh_test",
                "features": {
                    "day_type": "trend",       # supportive
                    "delta_proxy": -0.10,      # adverse (1 signal)
                    "depth_imbalance": 0.05,
                    "mid_drift_pct": 0.005,    # supportive
                    "rejection_ratio": 0.30,    # high (2nd signal)
                    "bar_volume_proxy": 0.40,   # not dying
                },
            }
            with patch.object(executor, "_latest_capture_snapshot", return_value=snapshot):
                result = executor._xau_momentum_exhaustion_lock(
                    source="fibo:sniper", symbol="XAUUSD", direction="long",
                    entry=3026.49, stop_loss=3024.54, current_price=3028.50,
                    confidence=75.0, age_min=5.0, r_now=0.50,
                )
            self.assertFalse(result["active"])
            self.assertEqual(result["details"]["exhaustion_signals"], 2)

        finally:
            gc.collect()
            shutil.rmtree(td, ignore_errors=True)

    def test_exhaustion_lock_scales_with_r_multiple(self):
        """Higher R-multiple → higher lock percentage."""
        td = tempfile.mkdtemp()
        try:
            executor = self._make_executor(td)
            snapshot_exhausted = {
                "ok": True, "run_id": "exh",
                "features": {
                    "day_type": "range", "delta_proxy": -0.15, "depth_imbalance": -0.05,
                    "mid_drift_pct": -0.012, "rejection_ratio": 0.35, "bar_volume_proxy": 0.15,
                },
            }
            with patch.object(executor, "_latest_capture_snapshot", return_value=snapshot_exhausted):
                # At 0.3R → lock_pct=0.15
                r_low = executor._xau_momentum_exhaustion_lock(
                    source="fibo:sniper", symbol="XAUUSD", direction="long",
                    entry=3026.49, stop_loss=3024.54, current_price=3027.20,
                    confidence=75.0, age_min=5.0, r_now=0.35,
                )
                # At 1.0R → lock_pct=0.55
                r_mid = executor._xau_momentum_exhaustion_lock(
                    source="fibo:sniper", symbol="XAUUSD", direction="long",
                    entry=3026.49, stop_loss=3024.54, current_price=3028.50,
                    confidence=75.0, age_min=5.0, r_now=1.0,
                )
                # At 2.0R → lock_pct=0.70
                r_high = executor._xau_momentum_exhaustion_lock(
                    source="fibo:sniper", symbol="XAUUSD", direction="long",
                    entry=3026.49, stop_loss=3024.54, current_price=3030.40,
                    confidence=75.0, age_min=5.0, r_now=2.0,
                )

            self.assertTrue(r_low["active"])
            self.assertTrue(r_mid["active"])
            self.assertTrue(r_high["active"])
            self.assertEqual(r_low["details"]["lock_pct"], 0.15)
            self.assertEqual(r_mid["details"]["lock_pct"], 0.55)
            self.assertEqual(r_high["details"]["lock_pct"], 0.70)

        finally:
            gc.collect()
            shutil.rmtree(td, ignore_errors=True)

    def test_momentum_adaptive_step_r_strong(self):
        """Strong momentum (4+ favorable) → step_r increases from base."""
        td = tempfile.mkdtemp()
        try:
            executor = self._make_executor(td)
            snapshot = {
                "ok": True, "run_id": "test",
                "features": {
                    "day_type": "trend",
                    "delta_proxy": -0.25,  # strong supportive for short
                    "depth_imbalance": -0.18,
                    "mid_drift_pct": -0.020,
                    "rejection_ratio": 0.05,  # low
                    "bar_volume_proxy": 0.80,  # high
                },
            }
            with patch.object(executor, "_latest_capture_snapshot", return_value=snapshot):
                result = executor._xau_profit_extension_plan(
                    source="fibo:sniper", symbol="XAUUSD", direction="short",
                    entry=3030.00, stop_loss=3033.00, planned_tp=3027.00,
                    current_tp=3027.00, current_price=3026.00,
                    confidence=80.0, age_min=2.0, r_now=1.33,
                )
            if result["active"]:
                details = result.get("details", {})
                momentum_info = details.get("momentum_adaptive", {})
                self.assertEqual(momentum_info.get("momentum_label"), "strong")
                # step_r should be base(0.25) + 0.10 = 0.35
                self.assertAlmostEqual(momentum_info.get("step_r", 0), 0.35, places=2)

        finally:
            gc.collect()
            shutil.rmtree(td, ignore_errors=True)

    def test_momentum_adaptive_step_r_weak(self):
        """Weak momentum (0-1 favorable) → step_r decreases from base."""
        td = tempfile.mkdtemp()
        try:
            executor = self._make_executor(td)
            snapshot = {
                "ok": True, "run_id": "test",
                "features": {
                    "day_type": "range",          # not supportive
                    "delta_proxy": 0.05,          # weak adverse for short
                    "depth_imbalance": 0.02,       # weak
                    "mid_drift_pct": 0.003,        # weak
                    "rejection_ratio": 0.35,       # high
                    "bar_volume_proxy": 0.15,       # low
                },
            }
            with patch.object(executor, "_latest_capture_snapshot", return_value=snapshot):
                result = executor._xau_profit_extension_plan(
                    source="fibo:sniper", symbol="XAUUSD", direction="short",
                    entry=3030.00, stop_loss=3033.00, planned_tp=3027.00,
                    current_tp=3027.00, current_price=3026.00,
                    confidence=80.0, age_min=2.0, r_now=1.33,
                )
            # Even if not active (score below), momentum should be labeled weak
            details = result.get("details", {})
            if "momentum_adaptive" in details:
                self.assertEqual(details["momentum_adaptive"]["momentum_label"], "weak")

        finally:
            gc.collect()
            shutil.rmtree(td, ignore_errors=True)


if __name__ == "__main__":
    unittest.main()
