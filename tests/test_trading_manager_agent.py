from __future__ import annotations

import json
import sqlite3
import tempfile
import unittest
import gc
from contextlib import ExitStack
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from learning.trading_manager_agent import TradingManagerAgent


def _init_ctrader_db(path: Path) -> None:
    with sqlite3.connect(str(path)) as conn:
        conn.execute(
            """
            CREATE TABLE execution_journal (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                created_ts REAL DEFAULT 0,
                created_utc TEXT NOT NULL,
                source TEXT,
                symbol TEXT,
                direction TEXT,
                entry_type TEXT,
                confidence REAL,
                entry REAL,
                stop_loss REAL,
                take_profit REAL,
                status TEXT DEFAULT '',
                request_json TEXT,
                response_json TEXT DEFAULT '{}',
                execution_meta_json TEXT
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
                event_ts REAL,
                bid REAL,
                ask REAL,
                spread REAL,
                spread_pct REAL,
                raw_json TEXT
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
                event_ts REAL,
                side TEXT,
                price REAL,
                size REAL,
                level_index INTEGER,
                raw_json TEXT
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE ctrader_positions (
                position_id INTEGER PRIMARY KEY,
                source TEXT,
                symbol TEXT,
                direction TEXT,
                entry_price REAL,
                stop_loss REAL,
                take_profit REAL,
                first_seen_utc TEXT,
                is_open INTEGER
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE ctrader_orders (
                order_id INTEGER PRIMARY KEY,
                source TEXT,
                symbol TEXT,
                direction TEXT,
                order_type TEXT,
                entry_price REAL,
                stop_loss REAL,
                take_profit REAL,
                first_seen_utc TEXT,
                is_open INTEGER
            )
            """
        )


class TradingManagerAgentTests(unittest.TestCase):
    def setUp(self):
        self._td = tempfile.TemporaryDirectory()
        self.base = Path(self._td.name)
        self.report_dir = self.base / "reports"
        self.report_dir.mkdir(parents=True, exist_ok=True)
        self.ctrader_db = self.base / "ctrader_openapi.db"
        _init_ctrader_db(self.ctrader_db)
        self.agent = TradingManagerAgent(report_dir=str(self.report_dir), ctrader_db_path=str(self.ctrader_db))
        self.agent.state_path = self.base / "trading_manager_state.json"
        self._utc_now_patch = patch(
            "learning.trading_manager_agent._utc_now",
            return_value=datetime(2026, 3, 11, 12, 0, 0, tzinfo=timezone.utc),
        )
        self._utc_now_patch.start()

    def tearDown(self):
        self._utc_now_patch.stop()
        self.agent = None
        gc.collect()
        try:
            self._td.cleanup()
        except PermissionError:
            pass

    def test_trading_manager_report_detects_shock_and_best_family(self):
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
                    (
                        1,
                        "2026-03-10T18:00:00Z",
                        "scalp_xauusd:pb:canary",
                        "XAUUSD",
                        "long",
                        "limit",
                        73.2,
                        5202.0,
                        5197.0,
                        5206.0,
                        json.dumps({"payload": {"direction": "long", "session": "asian", "timeframe": "5m+1m", "pattern": "PULLBACK_LIMIT", "entry_type": "limit"}}),
                        "{}",
                    ),
                    (
                        2,
                        "2026-03-10T18:02:00Z",
                        "xauusd_scheduled:canary",
                        "XAUUSD",
                        "short",
                        "limit",
                        76.0,
                        5198.0,
                        5205.0,
                        5188.0,
                        json.dumps({"payload": {"direction": "short", "session": "asian", "timeframe": "1h", "pattern": "Bearish OB + BOS", "entry_type": "limit"}}),
                        "{}",
                    ),
                    (
                        3,
                        "2026-03-10T18:05:00Z",
                        "scalp_xauusd:bs:canary",
                        "XAUUSD",
                        "long",
                        "buy_stop",
                        72.0,
                        5114.7765,
                        5112.1,
                        5117.525,
                        json.dumps({"payload": {"direction": "long", "session": "new_york", "timeframe": "5m+1m", "pattern": "BREAKOUT_STOP", "entry_type": "buy_stop"}}),
                        "{}",
                    ),
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
                    (101, "scalp_xauusd:pb:canary", "XAUUSD", -12.5, 0, 1, 1, "2026-03-10T18:10:00Z", json.dumps({"raw": {"closePositionDetail": {"entryPrice": 5202.0}}})),
                    (102, "xauusd_scheduled:canary", "XAUUSD", 18.4, 1, 1, 2, "2026-03-10T18:12:00Z", json.dumps({"raw": {"closePositionDetail": {"entryPrice": 5198.0}}})),
                    (103, "scalp_xauusd:bs:canary", "XAUUSD", 35.98, 1, 1, 3, "2026-03-10T18:15:00Z", json.dumps({"raw": {"closePositionDetail": {"entryPrice": 5119.17}}})),
                ],
            )
            conn.executemany(
                """
                INSERT INTO ctrader_spot_ticks(run_id, symbol, event_utc, event_ts, bid, ask, spread, spread_pct, raw_json)
                VALUES(?,?,?,?,?,?,?,?,?)
                """,
                [
                    ("run1", "XAUUSD", "2026-03-10T18:00:00Z", 1773165600.0, 5200.0, 5200.4, 0.4, 0.0077, "{}"),
                    ("run1", "XAUUSD", "2026-03-10T18:03:00Z", 1773165780.0, 5196.0, 5196.5, 0.5, 0.0096, "{}"),
                    ("run1", "XAUUSD", "2026-03-10T18:07:00Z", 1773166020.0, 5172.0, 5172.8, 0.8, 0.0155, "{}"),
                    ("run1", "XAUUSD", "2026-03-10T18:11:00Z", 1773166260.0, 5184.0, 5184.5, 0.5, 0.0096, "{}"),
                ],
            )
            conn.executemany(
                """
                INSERT INTO ctrader_depth_quotes(run_id, symbol, event_utc, event_ts, side, price, size, level_index, raw_json)
                VALUES(?,?,?,?,?,?,?,?,?)
                """,
                [
                    ("run1", "XAUUSD", "2026-03-10T18:02:00Z", 1773165720.0, "bid", 5198.0, 100.0, 0, "{}"),
                    ("run1", "XAUUSD", "2026-03-10T18:02:00Z", 1773165720.0, "ask", 5198.4, 450.0, 0, "{}"),
                    ("run1", "XAUUSD", "2026-03-10T18:06:00Z", 1773165960.0, "bid", 5173.0, 80.0, 0, "{}"),
                    ("run1", "XAUUSD", "2026-03-10T18:06:00Z", 1773165960.0, "ask", 5173.4, 520.0, 0, "{}"),
                ],
            )
            conn.execute(
                """
                INSERT INTO ctrader_positions(position_id, source, symbol, direction, entry_price, stop_loss, take_profit, first_seen_utc, is_open)
                VALUES(?,?,?,?,?,?,?,?,?)
                """,
                (901, "xauusd_scheduled:canary", "XAUUSD", "short", 5198.0, 5205.0, 5188.0, "2026-03-10T18:02:00Z", 1),
            )

        (self.report_dir / "mission_progress_report.json").write_text(
            json.dumps({"symbols": [{"symbol": "XAUUSD", "selected_family": "xau_scalp_pullback_limit", "selected_regime": "pullback_priority"}]}),
            encoding="utf-8",
        )
        (self.report_dir / "winner_memory_library_report.json").write_text(
            json.dumps({"top_by_symbol": {"XAUUSD": {"family": "xau_scheduled_trend", "session": "asian", "direction": "short"}}}),
            encoding="utf-8",
        )
        (self.report_dir / "family_calibration_report.json").write_text(
            json.dumps({"families": [{"symbol": "XAUUSD", "family": "xau_scheduled_trend", "overall": {"resolved": 10, "win_rate": 0.8, "pnl_usd": 50.0}}]}),
            encoding="utf-8",
        )

        macro_entry = {
            "headline_id": "h1",
            "title": "Reuters: Oil jumps as Middle East conflict escalates",
            "source": "Reuters",
            "published_utc": "2026-03-10T17:55:00Z",
            "score": 9,
            "themes": ["geopolitics", "oil_energy_shock"],
            "classification": "impact_developing",
            "classification_human": "Impact developing",
            "reaction_summary": "Gold and risk assets repriced lower on shock headlines.",
            "assets": {"XAUUSD": {"classification": "impact_developing", "classification_human": "Impact developing"}},
        }
        upcoming = [
            SimpleNamespace(
                event_id="ev1",
                title="US CPI",
                currency="USD",
                impact="high",
                time_utc=None,
                minutes_to_event=18,
            )
        ]
        headlines = [
            SimpleNamespace(
                headline_id="h1",
                title="Reuters: Oil jumps as Middle East conflict escalates",
                source="Reuters",
                published_utc=None,
                score=9,
                themes=["geopolitics", "oil_energy_shock"],
                impact_hint="Energy shock can reprice gold.",
            )
        ]

        with patch("learning.trading_manager_agent.config.TRADING_MANAGER_REPORT_TIMEZONE", "Asia/Bangkok"), \
             patch("learning.trading_manager_agent.config.TRADING_MANAGER_REPORT_LOOKBACK_HOURS", 24), \
             patch("learning.trading_manager_agent.config.TRADING_MANAGER_REPORT_SYMBOLS", "XAUUSD"), \
             patch("learning.trading_manager_agent.config.TRADING_MANAGER_EVENT_MIN_DROP_PCT", 0.30), \
             patch("learning.trading_manager_agent.config.TRADING_MANAGER_AUTO_TUNE_ENABLED", False, create=True), \
             patch("learning.trading_manager_agent.config.TRADING_MANAGER_AUTO_ROUTING_ENABLED", False, create=True), \
             patch("learning.trading_manager_agent.config.TRADING_MANAGER_XAU_SWARM_SAMPLING_ENABLED", False, create=True), \
             patch("learning.trading_manager_agent.config.get_persistent_canary_strategy_families", return_value={"xau_scalp_pullback_limit", "xau_scalp_breakout_stop"}), \
             patch("learning.trading_manager_agent.config.get_ctrader_xau_active_families", return_value={"xau_scalp_pullback_limit", "xau_scalp_breakout_stop"}), \
             patch("learning.trading_manager_agent.config.get_persistent_canary_experimental_families", return_value=set()), \
             patch.object(self.agent, "_current_value", side_effect=lambda key: {"CTRADER_XAU_PRIMARY_FAMILY": "xau_scalp_breakout_stop", "CTRADER_XAU_ACTIVE_FAMILIES": "xau_scalp_pullback_limit,xau_scalp_breakout_stop", "PERSISTENT_CANARY_STRATEGY_FAMILIES": "xau_scalp_pullback_limit,xau_scalp_breakout_stop"}.get(key, "")), \
             patch("learning.trading_manager_agent.macro_news.high_impact_headlines", return_value=headlines), \
             patch("learning.trading_manager_agent.macro_impact_tracker.sync", return_value={"ok": True}), \
             patch("learning.trading_manager_agent.macro_impact_tracker.build_report", return_value={"entries": [macro_entry]}), \
             patch("learning.trading_manager_agent.economic_calendar.next_events", return_value=upcoming):
            report = self.agent.build_report(hours=24)

        self.assertTrue(report["ok"])
        self.assertEqual(int((report.get("summary") or {}).get("abnormal_excluded", 0)), 1)
        xau = next(row for row in list(report.get("symbols") or []) if row.get("symbol") == "XAUUSD")
        shock = dict(xau.get("shock_event") or {})
        self.assertEqual(str(shock.get("shock_type")), "orderbook_sell_pressure")
        best = dict(xau.get("best_same_situation") or {})
        self.assertEqual(best, {})
        self.assertIn("sell pressure dominated the book", str(xau.get("shock_explanation") or ""))
        self.assertEqual(str((xau.get("macro_cause") or {}).get("source") or ""), "Reuters")
        self.assertEqual(int(((xau.get("upcoming_events") or [{}])[0]).get("minutes_to_event", 0) or 0), 18)
        actions = list(xau.get("manager_actions") or [])
        self.assertTrue(any(str(row.get("action")) == "block_countertrend_long_after_selloff" for row in actions))
        self.assertTrue(any(str(row.get("action")) == "tighten_countertrend_scalp_during_repricing_shock" for row in actions))
        self.assertTrue(any(str(row.get("action")) == "pre_event_freeze_countertrend" for row in actions))
        routing = dict(xau.get("family_routing_recommendations") or {})
        self.assertEqual(str(routing.get("mode") or ""), "pre_event_caution")
        self.assertEqual(str((routing.get("changes") or {}).get("CTRADER_XAU_PRIMARY_FAMILY") or ""), "xau_scalp_pullback_limit")

    def test_trading_manager_auto_family_routing_applies_post_event_leader(self):
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
                    (
                        1,
                        "2026-03-10T18:00:00Z",
                        "scalp_xauusd:pb:canary",
                        "XAUUSD",
                        "long",
                        "limit",
                        73.4,
                        5200.0,
                        5196.0,
                        5204.0,
                        json.dumps({"payload": {"direction": "long", "session": "new_york", "timeframe": "5m+1m", "pattern": "PULLBACK_LIMIT", "entry_type": "limit"}}),
                        "{}",
                    ),
                    (
                        2,
                        "2026-03-10T18:08:00Z",
                        "scalp_xauusd:pb:canary",
                        "XAUUSD",
                        "short",
                        "limit",
                        73.8,
                        5198.0,
                        5201.0,
                        5194.0,
                        json.dumps({"payload": {"direction": "short", "session": "new_york", "timeframe": "5m+1m", "pattern": "PULLBACK_LIMIT", "entry_type": "limit"}}),
                        "{}",
                    ),
                    (
                        3,
                        "2026-03-10T18:12:00Z",
                        "scalp_xauusd:pb:canary",
                        "XAUUSD",
                        "long",
                        "limit",
                        74.0,
                        5199.0,
                        5195.0,
                        5203.5,
                        json.dumps({"payload": {"direction": "long", "session": "new_york", "timeframe": "5m+1m", "pattern": "PULLBACK_LIMIT", "entry_type": "limit"}}),
                        "{}",
                    ),
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
                    (201, "scalp_xauusd:pb:canary", "XAUUSD", 3.20, 1, 1, 1, "2026-03-10T18:05:00Z", json.dumps({"raw": {"closePositionDetail": {"entryPrice": 5200.0}}})),
                    (202, "scalp_xauusd:pb:canary", "XAUUSD", 2.75, 1, 1, 2, "2026-03-10T18:10:00Z", json.dumps({"raw": {"closePositionDetail": {"entryPrice": 5198.0}}})),
                    (203, "scalp_xauusd:pb:canary", "XAUUSD", 1.90, 1, 1, 3, "2026-03-10T18:15:00Z", json.dumps({"raw": {"closePositionDetail": {"entryPrice": 5199.0}}})),
                ],
            )

        (self.report_dir / "mission_progress_report.json").write_text(
            json.dumps({"symbols": [{"symbol": "XAUUSD", "selected_family": "xau_scheduled_trend", "selected_regime": "trend_priority"}]}),
            encoding="utf-8",
        )
        (self.report_dir / "winner_memory_library_report.json").write_text(
            json.dumps({"top_by_symbol": {"XAUUSD": {"family": "xau_scalp_pullback_limit", "session": "new_york", "direction": "long"}}}),
            encoding="utf-8",
        )
        (self.report_dir / "family_calibration_report.json").write_text(
            json.dumps({"families": [{"symbol": "XAUUSD", "family": "xau_scheduled_trend", "overall": {"resolved": 6, "win_rate": 0.5, "pnl_usd": -6.0}}]}),
            encoding="utf-8",
        )

        with patch("learning.trading_manager_agent.config.TRADING_MANAGER_REPORT_TIMEZONE", "Asia/Bangkok"), \
             patch("learning.trading_manager_agent.config.TRADING_MANAGER_REPORT_LOOKBACK_HOURS", 24), \
             patch("learning.trading_manager_agent.config.TRADING_MANAGER_REPORT_SYMBOLS", "XAUUSD"), \
             patch("learning.trading_manager_agent.config.TRADING_MANAGER_AUTO_TUNE_ENABLED", False, create=True), \
             patch("learning.trading_manager_agent.config.TRADING_MANAGER_AUTO_ROUTING_ENABLED", True, create=True), \
             patch("learning.trading_manager_agent.config.TRADING_MANAGER_AUTO_ROUTING_PERSIST_ENV", False, create=True), \
             patch("learning.trading_manager_agent.config.TRADING_MANAGER_XAU_SWARM_SAMPLING_ENABLED", False, create=True), \
             patch("learning.trading_manager_agent.config.CTRADER_XAU_PRIMARY_FAMILY", "xau_scalp_breakout_stop", create=True), \
             patch("learning.trading_manager_agent.config.CTRADER_XAU_ACTIVE_FAMILIES", "xau_scalp_pullback_limit,xau_scalp_breakout_stop", create=True), \
             patch("learning.trading_manager_agent.config.PERSISTENT_CANARY_STRATEGY_FAMILIES", "xau_scalp_pullback_limit,xau_scalp_breakout_stop", create=True), \
             patch.object(self.agent, "_current_value", side_effect=lambda key: {"CTRADER_XAU_PRIMARY_FAMILY": "xau_scalp_breakout_stop", "CTRADER_XAU_ACTIVE_FAMILIES": "xau_scalp_pullback_limit,xau_scalp_breakout_stop", "PERSISTENT_CANARY_STRATEGY_FAMILIES": "xau_scalp_pullback_limit,xau_scalp_breakout_stop"}.get(key, "")), \
             patch.object(self.agent, "_apply_runtime_value"), \
             patch.object(self.agent, "_persist_env_value", return_value={"ok": True, "updated": False, "reason": "persist_disabled"}), \
             patch("learning.trading_manager_agent.macro_news.high_impact_headlines", return_value=[]), \
             patch("learning.trading_manager_agent.macro_impact_tracker.sync", return_value={"ok": True}), \
             patch("learning.trading_manager_agent.macro_impact_tracker.build_report", return_value={"entries": []}), \
             patch("learning.trading_manager_agent.economic_calendar.next_events", return_value=[]):
            report = self.agent.build_report(hours=24)

        xau = next(row for row in list(report.get("symbols") or []) if row.get("symbol") == "XAUUSD")
        routing = dict(xau.get("family_routing_recommendations") or {})
        self.assertEqual(str(routing.get("mode") or ""), "post_event_promote_today_leader")
        self.assertEqual(str((routing.get("changes") or {}).get("CTRADER_XAU_PRIMARY_FAMILY") or ""), "xau_scalp_pullback_limit")
        self.assertEqual(str((report.get("family_routing_apply") or {}).get("status") or ""), "applied")
        state = json.loads(self.agent.state_path.read_text(encoding="utf-8"))
        self.assertEqual(str(((state.get("xau_family_routing") or {}).get("status")) or ""), "active")

    def test_trading_manager_swarm_sampling_keeps_xau_family_set_broad(self):
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
                    (
                        11,
                        "2026-03-10T18:00:00Z",
                        "scalp_xauusd:pb:canary",
                        "XAUUSD",
                        "short",
                        "limit",
                        73.6,
                        5199.0,
                        5202.0,
                        5194.0,
                        json.dumps({"payload": {"direction": "short", "session": "new_york", "timeframe": "5m+1m", "pattern": "PULLBACK_LIMIT", "entry_type": "limit"}}),
                        "{}",
                    ),
                    (
                        12,
                        "2026-03-10T18:06:00Z",
                        "scalp_xauusd:pb:canary",
                        "XAUUSD",
                        "long",
                        "limit",
                        73.1,
                        5197.0,
                        5193.0,
                        5201.0,
                        json.dumps({"payload": {"direction": "long", "session": "new_york", "timeframe": "5m+1m", "pattern": "PULLBACK_LIMIT", "entry_type": "limit"}}),
                        "{}",
                    ),
                    (
                        13,
                        "2026-03-10T18:12:00Z",
                        "scalp_xauusd:pb:canary",
                        "XAUUSD",
                        "short",
                        "limit",
                        73.9,
                        5198.5,
                        5201.5,
                        5194.5,
                        json.dumps({"payload": {"direction": "short", "session": "new_york", "timeframe": "5m+1m", "pattern": "PULLBACK_LIMIT", "entry_type": "limit"}}),
                        "{}",
                    ),
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
                    (211, "scalp_xauusd:pb:canary", "XAUUSD", 4.20, 1, 1, 11, "2026-03-10T18:04:00Z", json.dumps({"raw": {"closePositionDetail": {"entryPrice": 5199.0}}})),
                    (212, "scalp_xauusd:pb:canary", "XAUUSD", 2.10, 1, 1, 12, "2026-03-10T18:10:00Z", json.dumps({"raw": {"closePositionDetail": {"entryPrice": 5197.0}}})),
                    (213, "scalp_xauusd:pb:canary", "XAUUSD", 1.95, 1, 1, 13, "2026-03-10T18:16:00Z", json.dumps({"raw": {"closePositionDetail": {"entryPrice": 5198.5}}})),
                ],
            )

        (self.report_dir / "mission_progress_report.json").write_text(
            json.dumps({"symbols": [{"symbol": "XAUUSD", "selected_family": "xau_scheduled_trend", "selected_regime": "trend_priority"}]}),
            encoding="utf-8",
        )
        (self.report_dir / "winner_memory_library_report.json").write_text(
            json.dumps({"top_by_symbol": {"XAUUSD": {"family": "xau_scalp_pullback_limit", "session": "new_york", "direction": "short"}}}),
            encoding="utf-8",
        )
        (self.report_dir / "family_calibration_report.json").write_text(
            json.dumps({"families": [{"symbol": "XAUUSD", "family": "xau_scheduled_trend", "overall": {"resolved": 8, "win_rate": 0.81, "pnl_usd": 32.0}}]}),
            encoding="utf-8",
        )
        (self.report_dir / "chart_state_memory_report.json").write_text(
            json.dumps(
                {
                    "states": [
                        {
                            "symbol": "XAUUSD",
                            "state_label": "continuation_drive",
                            "direction": "short",
                            "day_type": "trend",
                            "follow_up_candidate": True,
                            "state_score": 44.0,
                            "stats": {"resolved": 3, "pnl_usd": 8.1},
                            "best_family": {"family": "xau_scalp_tick_depth_filter"},
                        }
                    ]
                }
            ),
            encoding="utf-8",
        )

        swarm_families = "xau_scalp_pullback_limit,xau_scalp_tick_depth_filter,xau_scalp_microtrend_follow_up,xau_scalp_flow_short_sidecar,xau_scalp_failed_fade_follow_stop"
        with patch("learning.trading_manager_agent.config.TRADING_MANAGER_REPORT_TIMEZONE", "Asia/Bangkok"), \
             patch("learning.trading_manager_agent.config.TRADING_MANAGER_REPORT_LOOKBACK_HOURS", 24), \
             patch("learning.trading_manager_agent.config.TRADING_MANAGER_REPORT_SYMBOLS", "XAUUSD"), \
             patch("learning.trading_manager_agent.config.TRADING_MANAGER_AUTO_TUNE_ENABLED", False, create=True), \
             patch("learning.trading_manager_agent.config.TRADING_MANAGER_AUTO_ROUTING_ENABLED", True, create=True), \
             patch("learning.trading_manager_agent.config.TRADING_MANAGER_AUTO_ROUTING_PERSIST_ENV", False, create=True), \
             patch("learning.trading_manager_agent.config.TRADING_MANAGER_XAU_POST_EVENT_PROMOTE_MIN_RESOLVED", 2, create=True), \
             patch("learning.trading_manager_agent.config.TRADING_MANAGER_XAU_SWARM_SAMPLING_ENABLED", True, create=True), \
             patch("learning.trading_manager_agent.config.TRADING_MANAGER_XAU_SWARM_ACTIVE_FAMILIES", swarm_families, create=True), \
             patch("learning.trading_manager_agent.config.CTRADER_XAU_PRIMARY_FAMILY", "xau_scalp_pullback_limit", create=True), \
             patch("learning.trading_manager_agent.config.CTRADER_XAU_ACTIVE_FAMILIES", "xau_scalp_pullback_limit", create=True), \
             patch("learning.trading_manager_agent.config.PERSISTENT_CANARY_STRATEGY_FAMILIES", "xau_scalp_pullback_limit", create=True), \
             patch.object(self.agent, "_current_value", side_effect=lambda key: {"CTRADER_XAU_PRIMARY_FAMILY": "xau_scalp_pullback_limit", "CTRADER_XAU_ACTIVE_FAMILIES": "xau_scalp_pullback_limit", "PERSISTENT_CANARY_STRATEGY_FAMILIES": "xau_scalp_pullback_limit"}.get(key, "")), \
             patch.object(self.agent, "_apply_runtime_value"), \
             patch.object(self.agent, "_persist_env_value", return_value={"ok": True, "updated": False, "reason": "persist_disabled"}), \
             patch("learning.trading_manager_agent.macro_news.high_impact_headlines", return_value=[]), \
             patch("learning.trading_manager_agent.macro_impact_tracker.sync", return_value={"ok": True}), \
             patch("learning.trading_manager_agent.macro_impact_tracker.build_report", return_value={"entries": []}), \
             patch("learning.trading_manager_agent.economic_calendar.next_events", return_value=[]):
            report = self.agent.build_report(hours=24)

        xau = next(row for row in list(report.get("symbols") or []) if row.get("symbol") == "XAUUSD")
        parallel = dict(xau.get("parallel_family_recommendations") or {})
        routing = dict(xau.get("family_routing_recommendations") or {})
        self.assertEqual(str(parallel.get("mode") or ""), "state_parallel_same_direction")
        self.assertEqual(list(parallel.get("allowed_families") or []), swarm_families.split(","))
        self.assertEqual(str(routing.get("mode") or ""), "swarm_support_all")
        self.assertEqual(str((routing.get("changes") or {}).get("CTRADER_XAU_PRIMARY_FAMILY") or ""), "")
        self.assertEqual(str((routing.get("changes") or {}).get("CTRADER_XAU_ACTIVE_FAMILIES") or ""), swarm_families)

    def test_trading_manager_swarm_support_mode_does_not_flag_selected_family_lag(self):
        (self.report_dir / "mission_progress_report.json").write_text(
            json.dumps({"symbols": [{"symbol": "XAUUSD", "selected_family": "xau_scheduled_trend", "selected_regime": "trend_priority"}]}),
            encoding="utf-8",
        )
        (self.report_dir / "winner_memory_library_report.json").write_text(json.dumps({"top_by_symbol": {}}), encoding="utf-8")
        (self.report_dir / "family_calibration_report.json").write_text(json.dumps({"families": []}), encoding="utf-8")
        (self.report_dir / "chart_state_memory_report.json").write_text(
            json.dumps(
                {
                    "states": [
                        {
                            "symbol": "XAUUSD",
                            "state_label": "continuation_drive",
                            "direction": "short",
                            "day_type": "trend",
                            "follow_up_candidate": True,
                            "state_score": 42.0,
                            "stats": {"resolved": 4, "pnl_usd": 8.4},
                            "best_family": {"family": "xau_scalp_tick_depth_filter"},
                        }
                    ]
                }
            ),
            encoding="utf-8",
        )
        (self.report_dir / "ct_only_experiment_report.json").write_text(json.dumps({"sources": []}), encoding="utf-8")

        swarm_families = "xau_scalp_pullback_limit,xau_scalp_tick_depth_filter,xau_scalp_microtrend_follow_up"
        self.agent._save_state(
            {
                "opportunity_feed": {
                    "status": "active",
                    "symbols": {
                        "XAUUSD": {
                            "priority_families": swarm_families.split(","),
                            "support_all_families": swarm_families.split(","),
                        }
                    },
                }
            }
        )
        with patch("learning.trading_manager_agent.config.TRADING_MANAGER_REPORT_SYMBOLS", "XAUUSD"), \
             patch("learning.trading_manager_agent.config.TRADING_MANAGER_AUTO_TUNE_ENABLED", False, create=True), \
             patch("learning.trading_manager_agent.config.TRADING_MANAGER_AUTO_ROUTING_ENABLED", False, create=True), \
             patch("learning.trading_manager_agent.config.TRADING_MANAGER_XAU_SWARM_SAMPLING_ENABLED", True, create=True), \
             patch("learning.trading_manager_agent.config.TRADING_MANAGER_XAU_SWARM_ACTIVE_FAMILIES", swarm_families, create=True), \
             patch("learning.trading_manager_agent.macro_news.high_impact_headlines", return_value=[]), \
             patch("learning.trading_manager_agent.macro_impact_tracker.sync", return_value={"ok": True}), \
             patch("learning.trading_manager_agent.macro_impact_tracker.build_report", return_value={"entries": []}), \
             patch("learning.trading_manager_agent.economic_calendar.next_events", return_value=[]):
            report = self.agent.build_report(hours=24)

        xau = next(row for row in list(report.get("symbols") or []) if row.get("symbol") == "XAUUSD")
        no_trade = dict(xau.get("why_no_trade_diagnostics") or {})
        self.assertFalse(any("lags the current manager priority" in str(item or "") for item in list(no_trade.get("likely_blockers") or [])))
        self.assertTrue(any("support-all families" in str(item or "") for item in list(no_trade.get("coaching") or [])))

    def test_trading_manager_applies_parallel_and_hedge_lane_states(self):
        (self.report_dir / "mission_progress_report.json").write_text(
            json.dumps({"symbols": [{"symbol": "XAUUSD", "selected_family": "xau_scheduled_trend", "selected_regime": "trend_priority"}]}),
            encoding="utf-8",
        )
        (self.report_dir / "winner_memory_library_report.json").write_text(json.dumps({"top_by_symbol": {}}), encoding="utf-8")
        (self.report_dir / "family_calibration_report.json").write_text(json.dumps({"families": []}), encoding="utf-8")
        (self.report_dir / "chart_state_memory_report.json").write_text(
            json.dumps(
                {
                    "states": [
                        {
                            "symbol": "XAUUSD",
                            "state_label": "continuation_drive",
                            "direction": "short",
                            "follow_up_candidate": True,
                            "state_score": 49.6,
                            "stats": {"resolved": 4, "pnl_usd": 12.4},
                        }
                    ]
                }
            ),
            encoding="utf-8",
        )
        (self.report_dir / "ct_only_experiment_report.json").write_text(
            json.dumps(
                {
                    "sources": [
                        {"source": "scalp_xauusd:fss:canary", "closed_total": {"resolved": 0}},
                        {"source": "scalp_xauusd:ff:canary", "closed_total": {"resolved": 0}},
                    ]
                }
            ),
            encoding="utf-8",
        )

        with patch("learning.trading_manager_agent.config.TRADING_MANAGER_REPORT_TIMEZONE", "Asia/Bangkok"), \
             patch("learning.trading_manager_agent.config.TRADING_MANAGER_REPORT_LOOKBACK_HOURS", 24), \
             patch("learning.trading_manager_agent.config.TRADING_MANAGER_REPORT_SYMBOLS", "XAUUSD"), \
             patch("learning.trading_manager_agent.config.TRADING_MANAGER_AUTO_TUNE_ENABLED", False, create=True), \
             patch("learning.trading_manager_agent.config.TRADING_MANAGER_AUTO_ROUTING_ENABLED", True, create=True), \
             patch("learning.trading_manager_agent.config.TRADING_MANAGER_AUTO_ROUTING_PERSIST_ENV", False, create=True), \
             patch("learning.trading_manager_agent.config.TRADING_MANAGER_XAU_PARALLEL_FAMILIES_ENABLED", True, create=True), \
             patch("learning.trading_manager_agent.config.TRADING_MANAGER_XAU_HEDGE_LANE_ENABLED", True, create=True), \
             patch("learning.trading_manager_agent.config.get_persistent_canary_strategy_families", return_value={"xau_scalp_pullback_limit"}), \
             patch("learning.trading_manager_agent.config.get_ctrader_xau_active_families", return_value={"xau_scalp_pullback_limit"}), \
             patch("learning.trading_manager_agent.config.get_persistent_canary_experimental_families", return_value={"xau_scalp_flow_short_sidecar", "xau_scalp_failed_fade_follow_stop"}), \
             patch("learning.trading_manager_agent.macro_news.high_impact_headlines", return_value=[]), \
             patch("learning.trading_manager_agent.macro_impact_tracker.sync", return_value={"ok": True}), \
             patch("learning.trading_manager_agent.macro_impact_tracker.build_report", return_value={"entries": []}), \
             patch("learning.trading_manager_agent.economic_calendar.next_events", return_value=[]):
            report = self.agent.build_report(hours=24)

        xau = next(row for row in list(report.get("symbols") or []) if row.get("symbol") == "XAUUSD")
        self.assertEqual(str((report.get("parallel_family_apply") or {}).get("status") or ""), "applied")
        self.assertEqual(str((report.get("hedge_lane_apply") or {}).get("status") or ""), "applied")
        self.assertEqual(str(((xau.get("parallel_family_recommendations") or {}).get("mode")) or ""), "state_parallel_same_direction")
        self.assertEqual(str(((xau.get("hedge_lane_recommendations") or {}).get("mode")) or ""), "xau_manager_hedge_transition")
        state = json.loads(self.agent.state_path.read_text(encoding="utf-8"))
        self.assertEqual(str(((state.get("xau_parallel_families") or {}).get("status")) or ""), "active")
        self.assertEqual(str(((state.get("xau_hedge_transition") or {}).get("status")) or ""), "active")

    def test_trading_manager_applies_xau_opportunity_bypass_with_open_long_and_short_state(self):
        (self.report_dir / "mission_progress_report.json").write_text(
            json.dumps({"symbols": [{"symbol": "XAUUSD", "selected_family": "xau_scheduled_trend", "selected_regime": "trend_priority"}]}),
            encoding="utf-8",
        )
        (self.report_dir / "winner_memory_library_report.json").write_text(json.dumps({"top_by_symbol": {}}), encoding="utf-8")
        (self.report_dir / "family_calibration_report.json").write_text(json.dumps({"families": []}), encoding="utf-8")
        (self.report_dir / "chart_state_memory_report.json").write_text(
            json.dumps(
                {
                    "states": [
                        {
                            "symbol": "XAUUSD",
                            "state_label": "continuation_drive",
                            "direction": "short",
                            "day_type": "trend",
                            "follow_up_candidate": True,
                            "state_score": 41.0,
                            "stats": {"resolved": 4, "pnl_usd": 7.2},
                        }
                    ]
                }
            ),
            encoding="utf-8",
        )
        (self.report_dir / "ct_only_experiment_report.json").write_text(json.dumps({"sources": []}), encoding="utf-8")
        with sqlite3.connect(str(self.ctrader_db)) as conn:
            conn.execute(
                """
                INSERT INTO ctrader_positions(
                    position_id, source, symbol, direction, entry_price, stop_loss, take_profit, first_seen_utc, is_open
                ) VALUES(?,?,?,?,?,?,?,?,?)
                """,
                (99001, "scalp_xauusd:pb:canary", "XAUUSD", "long", 5200.0, 5190.0, 5208.0, "2026-03-10T20:00:00Z", 1),
            )
            conn.executemany(
                """
                INSERT INTO execution_journal(
                    id, created_ts, created_utc, source, symbol, direction, confidence, entry, stop_loss, take_profit, entry_type, status, request_json, response_json, execution_meta_json
                ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                [
                    (
                        6001, 1.0, "2026-03-10T20:10:00Z", "scalp_xauusd:td:canary", "XAUUSD", "long", 74.0,
                        5201.0, 5198.0, 5204.0, "limit", "closed",
                        json.dumps({"payload": {"direction": "long", "session": "new_york", "timeframe": "5m+1m", "pattern": "TD", "entry_type": "limit"}}),
                        "{}", "{}"
                    )
                ],
            )
            conn.execute(
                """
                INSERT INTO ctrader_deals(deal_id, source, symbol, pnl_usd, outcome, has_close_detail, journal_id, execution_utc, raw_json)
                VALUES(?,?,?,?,?,?,?,?,?)
                """,
                (7001, "scalp_xauusd:td:canary", "XAUUSD", -2.3, 0, 1, 6001, "2026-03-10T20:14:00Z", json.dumps({"raw": {"closePositionDetail": {"entryPrice": 5201.0}}})),
            )
            conn.commit()
        with ExitStack() as stack:
            stack.enter_context(patch("learning.trading_manager_agent.config.TRADING_MANAGER_REPORT_SYMBOLS", "XAUUSD"))
            stack.enter_context(patch("learning.trading_manager_agent.config.TRADING_MANAGER_AUTO_ROUTING_ENABLED", True, create=True))
            stack.enter_context(patch("learning.trading_manager_agent.config.TRADING_MANAGER_AUTO_ROUTING_PERSIST_ENV", False, create=True))
            stack.enter_context(patch("learning.trading_manager_agent.config.TRADING_MANAGER_XAU_OPPORTUNITY_BYPASS_ENABLED", True, create=True))
            stack.enter_context(patch("learning.trading_manager_agent.config.TRADING_MANAGER_XAU_OPPORTUNITY_BYPASS_MIN_STATE_SCORE", 28.0, create=True))
            stack.enter_context(patch("learning.trading_manager_agent.config.TRADING_MANAGER_XAU_OPPORTUNITY_BYPASS_ALLOWED_FAMILIES", "xau_scalp_failed_fade_follow_stop,xau_scalp_flow_short_sidecar", create=True))
            stack.enter_context(patch("learning.trading_manager_agent.config.TRADING_MANAGER_XAU_OPPORTUNITY_BYPASS_MAX_PER_SYMBOL", 2, create=True))
            stack.enter_context(patch("learning.trading_manager_agent.config.TRADING_MANAGER_XAU_OPPORTUNITY_BYPASS_RISK_MULTIPLIER", 0.55, create=True))
            stack.enter_context(patch("learning.trading_manager_agent.config.TRADING_MANAGER_XAU_OPPORTUNITY_BYPASS_MIN_VULNERABLE_REVIEWS", 1, create=True))
            stack.enter_context(patch("learning.trading_manager_agent.macro_news.high_impact_headlines", return_value=[]))
            stack.enter_context(patch("learning.trading_manager_agent.macro_impact_tracker.sync", return_value={"ok": True}))
            stack.enter_context(patch("learning.trading_manager_agent.macro_impact_tracker.build_report", return_value={"entries": []}))
            stack.enter_context(patch("learning.trading_manager_agent.economic_calendar.next_events", return_value=[]))
            report = self.agent.build_report(hours=24)
        xau = next(row for row in list(report.get("symbols") or []) if row.get("symbol") == "XAUUSD")
        bypass = dict(xau.get("opportunity_bypass_recommendations") or {})
        self.assertTrue(bool(bypass.get("active")))
        self.assertEqual(str((report.get("opportunity_bypass_apply") or {}).get("status") or ""), "applied")
        state = json.loads(self.agent.state_path.read_text(encoding="utf-8"))
        self.assertEqual(str(((state.get("xau_opportunity_bypass") or {}).get("status")) or ""), "active")

    def test_trading_manager_adds_recent_order_reviews_and_why_no_trade_diagnostics(self):
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
                    (
                        1,
                        "2026-03-11T18:00:00Z",
                        "xauusd_scheduled:canary",
                        "XAUUSD",
                        "long",
                        "market",
                        76.0,
                        5190.0,
                        5180.0,
                        5197.0,
                        json.dumps({"payload": {"direction": "long", "session": "london", "timeframe": "1h", "pattern": "Bullish FVG + ChoCH", "entry_type": "market"}}),
                        "{}",
                    ),
                    (
                        2,
                        "2026-03-11T18:10:00Z",
                        "scalp_xauusd:td:canary",
                        "XAUUSD",
                        "short",
                        "limit",
                        73.2,
                        5188.0,
                        5191.0,
                        5183.0,
                        json.dumps({"payload": {"direction": "short", "session": "new_york", "timeframe": "5m+1m", "pattern": "SCALP_FLOW_FORCE", "entry_type": "limit"}}),
                        "{}",
                    ),
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
                    (301, "xauusd_scheduled:canary", "XAUUSD", -6.5, 0, 1, 1, "2026-03-11T18:20:00Z", json.dumps({"raw": {"closePositionDetail": {"entryPrice": 5190.0}}})),
                    (302, "scalp_xauusd:td:canary", "XAUUSD", 4.2, 1, 1, 2, "2026-03-11T18:22:00Z", json.dumps({"raw": {"closePositionDetail": {"entryPrice": 5188.0}}})),
                ],
            )
        (self.report_dir / "mission_progress_report.json").write_text(
            json.dumps({"symbols": [{"symbol": "XAUUSD", "selected_family": "xau_scheduled_trend", "selected_regime": "trend_priority"}]}),
            encoding="utf-8",
        )
        (self.report_dir / "winner_memory_library_report.json").write_text(json.dumps({"top_by_symbol": {}}), encoding="utf-8")
        (self.report_dir / "family_calibration_report.json").write_text(json.dumps({"families": []}), encoding="utf-8")
        (self.report_dir / "chart_state_memory_report.json").write_text(
            json.dumps(
                {
                    "states": [
                        {
                            "symbol": "XAUUSD",
                            "state_label": "continuation_drive",
                            "direction": "short",
                            "day_type": "repricing",
                            "follow_up_candidate": True,
                            "state_score": 48.5,
                            "stats": {"resolved": 3, "pnl_usd": 9.7},
                            "best_family": {"family": "xau_scalp_tick_depth_filter"},
                        }
                    ]
                }
            ),
            encoding="utf-8",
        )
        (self.report_dir / "ct_only_experiment_report.json").write_text(
            json.dumps({"sources": [{"symbol": "XAUUSD", "source": "scalp_xauusd:td:canary", "closed_total": {"resolved": 0, "pnl_usd": 0.0}}]}),
            encoding="utf-8",
        )
        self.agent._save_state(
            {
                "opportunity_feed": {
                    "status": "active",
                    "symbols": {
                        "XAUUSD": {
                            "priority_families": ["xau_scalp_tick_depth_filter"],
                        }
                    },
                }
            }
        )
        with patch("learning.trading_manager_agent.config.TRADING_MANAGER_REPORT_SYMBOLS", "XAUUSD"), \
             patch("learning.trading_manager_agent.config.TRADING_MANAGER_AUTO_TUNE_ENABLED", False, create=True), \
             patch("learning.trading_manager_agent.config.TRADING_MANAGER_AUTO_ROUTING_ENABLED", False, create=True), \
             patch("learning.trading_manager_agent.config.TRADING_MANAGER_XAU_SWARM_SAMPLING_ENABLED", False, create=True), \
             patch("learning.trading_manager_agent.macro_news.high_impact_headlines", return_value=[]), \
             patch("learning.trading_manager_agent.macro_impact_tracker.sync", return_value={"ok": True}), \
             patch("learning.trading_manager_agent.macro_impact_tracker.build_report", return_value={"entries": []}), \
             patch("learning.trading_manager_agent.economic_calendar.next_events", return_value=[]):
            report = self.agent.build_report(hours=24)

        xau = next(row for row in list(report.get("symbols") or []) if row.get("symbol") == "XAUUSD")
        reviews = list(xau.get("recent_order_reviews") or [])
        self.assertGreaterEqual(len(reviews), 2)
        self.assertTrue(any("scheduled market entry paid up before retest confirmation" in str(row.get("diagnosis") or "") for row in reviews))
        no_trade = dict(xau.get("why_no_trade_diagnostics") or {})
        self.assertEqual(str(no_trade.get("status") or ""), "undertrading")
        self.assertTrue(any("lags the current manager priority" in str(item or "") for item in list(no_trade.get("likely_blockers") or [])))

    def test_trading_manager_parallel_budget_changes_with_day_type(self):
        (self.report_dir / "mission_progress_report.json").write_text(
            json.dumps({"symbols": [{"symbol": "XAUUSD", "selected_family": "xau_scheduled_trend", "selected_regime": "trend_priority"}]}),
            encoding="utf-8",
        )
        (self.report_dir / "winner_memory_library_report.json").write_text(json.dumps({"top_by_symbol": {}}), encoding="utf-8")
        (self.report_dir / "family_calibration_report.json").write_text(json.dumps({"families": []}), encoding="utf-8")
        (self.report_dir / "chart_state_memory_report.json").write_text(
            json.dumps(
                {
                    "states": [
                        {
                            "symbol": "XAUUSD",
                            "state_label": "panic_dislocation",
                            "direction": "short",
                            "day_type": "panic_spread",
                            "follow_up_candidate": True,
                            "state_score": 47.1,
                            "stats": {"resolved": 4, "pnl_usd": 8.0},
                        }
                    ]
                }
            ),
            encoding="utf-8",
        )
        (self.report_dir / "ct_only_experiment_report.json").write_text(json.dumps({"sources": []}), encoding="utf-8")

        with patch("learning.trading_manager_agent.config.TRADING_MANAGER_REPORT_SYMBOLS", "XAUUSD"), \
             patch("learning.trading_manager_agent.config.TRADING_MANAGER_AUTO_TUNE_ENABLED", False, create=True), \
             patch("learning.trading_manager_agent.config.TRADING_MANAGER_AUTO_ROUTING_ENABLED", True, create=True), \
             patch("learning.trading_manager_agent.config.TRADING_MANAGER_AUTO_ROUTING_PERSIST_ENV", False, create=True), \
             patch("learning.trading_manager_agent.config.TRADING_MANAGER_XAU_SWARM_SAMPLING_ENABLED", False, create=True), \
             patch("learning.trading_manager_agent.config.TRADING_MANAGER_XAU_PARALLEL_FAMILIES_PANIC_SPREAD_MAX_SAME_DIRECTION", 1, create=True), \
             patch("learning.trading_manager_agent.macro_news.high_impact_headlines", return_value=[]), \
             patch("learning.trading_manager_agent.macro_impact_tracker.sync", return_value={"ok": True}), \
             patch("learning.trading_manager_agent.macro_impact_tracker.build_report", return_value={"entries": []}), \
             patch("learning.trading_manager_agent.economic_calendar.next_events", return_value=[]):
            report = self.agent.build_report(hours=24)

        xau = next(row for row in list(report.get("symbols") or []) if row.get("symbol") == "XAUUSD")
        parallel = dict(xau.get("parallel_family_recommendations") or {})
        slot_budget = dict(parallel.get("slot_budget") or {})
        self.assertEqual(str(parallel.get("mode") or ""), "state_parallel_same_direction")
        self.assertEqual(str(slot_budget.get("day_type") or ""), "panic_spread")
        self.assertEqual(int(parallel.get("max_same_direction_families", 0) or 0), 1)

    def test_trading_manager_demotes_pb_when_scheduled_outperforms(self):
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
                    (
                        1,
                        "2026-03-11T09:00:00Z",
                        "scalp_xauusd:pb:canary",
                        "XAUUSD",
                        "long",
                        "limit",
                        72.8,
                        5200.0,
                        5195.0,
                        5204.0,
                        json.dumps({"payload": {"direction": "long", "session": "london,new_york,overlap", "timeframe": "5m+1m", "pattern": "PULLBACK_LIMIT", "entry_type": "limit"}}),
                        "{}",
                    ),
                    (
                        2,
                        "2026-03-11T09:04:00Z",
                        "scalp_xauusd:pb:canary",
                        "XAUUSD",
                        "short",
                        "limit",
                        73.2,
                        5198.0,
                        5202.0,
                        5193.0,
                        json.dumps({"payload": {"direction": "short", "session": "new_york", "timeframe": "5m+1m", "pattern": "PULLBACK_LIMIT", "entry_type": "limit"}}),
                        "{}",
                    ),
                    (
                        3,
                        "2026-03-11T09:08:00Z",
                        "scalp_xauusd:pb:canary",
                        "XAUUSD",
                        "long",
                        "limit",
                        72.6,
                        5197.0,
                        5192.0,
                        5201.0,
                        json.dumps({"payload": {"direction": "long", "session": "new_york", "timeframe": "5m+1m", "pattern": "PULLBACK_LIMIT", "entry_type": "limit"}}),
                        "{}",
                    ),
                    (
                        4,
                        "2026-03-11T09:12:00Z",
                        "xauusd_scheduled:canary",
                        "XAUUSD",
                        "long",
                        "limit",
                        76.5,
                        5188.0,
                        5178.0,
                        5204.0,
                        json.dumps({"payload": {"direction": "long", "session": "london", "timeframe": "1h", "pattern": "Bullish OB + BOS", "entry_type": "limit"}}),
                        "{}",
                    ),
                    (
                        5,
                        "2026-03-11T09:20:00Z",
                        "xauusd_scheduled:winner",
                        "XAUUSD",
                        "long",
                        "limit",
                        78.0,
                        5191.0,
                        5180.0,
                        5208.0,
                        json.dumps({"payload": {"direction": "long", "session": "london,new_york,overlap", "timeframe": "1h", "pattern": "Bullish OB + BOS", "entry_type": "limit"}}),
                        "{}",
                    ),
                    (
                        6,
                        "2026-03-11T09:30:00Z",
                        "xauusd_scheduled:canary",
                        "XAUUSD",
                        "long",
                        "limit",
                        77.1,
                        5190.0,
                        5181.0,
                        5206.0,
                        json.dumps({"payload": {"direction": "long", "session": "london", "timeframe": "1h", "pattern": "Bullish OB + BOS", "entry_type": "limit"}}),
                        "{}",
                    ),
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
                    (301, "scalp_xauusd:pb:canary", "XAUUSD", -4.10, 0, 1, 1, "2026-03-11T09:02:00Z", json.dumps({"raw": {"closePositionDetail": {"entryPrice": 5200.0}}})),
                    (302, "scalp_xauusd:pb:canary", "XAUUSD", -3.85, 0, 1, 2, "2026-03-11T09:06:00Z", json.dumps({"raw": {"closePositionDetail": {"entryPrice": 5198.0}}})),
                    (303, "scalp_xauusd:pb:canary", "XAUUSD", -4.55, 0, 1, 3, "2026-03-11T09:10:00Z", json.dumps({"raw": {"closePositionDetail": {"entryPrice": 5197.0}}})),
                    (304, "xauusd_scheduled:canary", "XAUUSD", 8.40, 1, 1, 4, "2026-03-11T09:16:00Z", json.dumps({"raw": {"closePositionDetail": {"entryPrice": 5188.0}}})),
                    (305, "xauusd_scheduled:winner", "XAUUSD", 12.35, 1, 1, 5, "2026-03-11T09:24:00Z", json.dumps({"raw": {"closePositionDetail": {"entryPrice": 5191.0}}})),
                    (306, "xauusd_scheduled:canary", "XAUUSD", 9.10, 1, 1, 6, "2026-03-11T09:36:00Z", json.dumps({"raw": {"closePositionDetail": {"entryPrice": 5190.0}}})),
                ],
            )

        (self.report_dir / "mission_progress_report.json").write_text(
            json.dumps({"symbols": [{"symbol": "XAUUSD", "selected_family": "xau_scheduled_trend", "selected_regime": "trend_priority"}]}),
            encoding="utf-8",
        )
        (self.report_dir / "winner_memory_library_report.json").write_text(
            json.dumps({"top_by_symbol": {"XAUUSD": {"family": "xau_scheduled_trend", "session": "london", "direction": "long"}}}),
            encoding="utf-8",
        )
        (self.report_dir / "family_calibration_report.json").write_text(
            json.dumps({"families": [{"symbol": "XAUUSD", "family": "xau_scheduled_trend", "overall": {"resolved": 6, "win_rate": 0.83, "pnl_usd": 22.0}}]}),
            encoding="utf-8",
        )

        with ExitStack() as stack:
            stack.enter_context(patch("learning.trading_manager_agent.config.TRADING_MANAGER_REPORT_TIMEZONE", "Asia/Bangkok"))
            stack.enter_context(patch("learning.trading_manager_agent.config.TRADING_MANAGER_REPORT_LOOKBACK_HOURS", 24))
            stack.enter_context(patch("learning.trading_manager_agent.config.TRADING_MANAGER_REPORT_SYMBOLS", "XAUUSD"))
            stack.enter_context(patch("learning.trading_manager_agent.config.TRADING_MANAGER_AUTO_TUNE_ENABLED", False, create=True))
            stack.enter_context(patch("learning.trading_manager_agent.config.TRADING_MANAGER_AUTO_ROUTING_ENABLED", True, create=True))
            stack.enter_context(patch("learning.trading_manager_agent.config.TRADING_MANAGER_AUTO_ROUTING_PERSIST_ENV", False, create=True))
            stack.enter_context(patch("learning.trading_manager_agent.config.TRADING_MANAGER_XAU_SWARM_SAMPLING_ENABLED", False, create=True))
            stack.enter_context(patch("learning.trading_manager_agent.config.TRADING_MANAGER_XAU_PB_DEMOTE_ENABLED", True, create=True))
            stack.enter_context(patch("learning.trading_manager_agent.config.TRADING_MANAGER_XAU_PB_DEMOTE_MIN_PB_RESOLVED", 3, create=True))
            stack.enter_context(patch("learning.trading_manager_agent.config.TRADING_MANAGER_XAU_PB_DEMOTE_MAX_PB_PNL_USD", -10.0, create=True))
            stack.enter_context(patch("learning.trading_manager_agent.config.TRADING_MANAGER_XAU_PB_DEMOTE_MIN_SCHEDULED_RESOLVED", 3, create=True))
            stack.enter_context(patch("learning.trading_manager_agent.config.TRADING_MANAGER_XAU_PB_DEMOTE_MIN_SCHEDULED_PNL_USD", 20.0, create=True))
            stack.enter_context(patch("learning.trading_manager_agent.config.TRADING_MANAGER_XAU_PB_DEMOTE_MIN_SCHEDULED_WIN_RATE", 0.60, create=True))
            stack.enter_context(patch("learning.trading_manager_agent.config.TRADING_MANAGER_XAU_PB_DEMOTE_PRIMARY_FAMILY", "xau_scalp_tick_depth_filter", create=True))
            stack.enter_context(patch("learning.trading_manager_agent.config.TRADING_MANAGER_XAU_PB_DEMOTE_ACTIVE_FAMILIES", "xau_scalp_tick_depth_filter,xau_scalp_failed_fade_follow_stop", create=True))
            stack.enter_context(patch("learning.trading_manager_agent.config.CTRADER_XAU_ACTIVE_FAMILIES", "xau_scalp_pullback_limit", create=True))
            stack.enter_context(patch("learning.trading_manager_agent.config.PERSISTENT_CANARY_STRATEGY_FAMILIES", "xau_scalp_pullback_limit,btc_weekday_lob_momentum", create=True))
            stack.enter_context(patch("learning.trading_manager_agent.config.PERSISTENT_CANARY_EXPERIMENTAL_FAMILIES", "xau_scalp_tick_depth_filter,xau_scalp_failed_fade_follow_stop", create=True))
            stack.enter_context(patch("learning.trading_manager_agent.config.get_persistent_canary_strategy_families", return_value={"xau_scalp_pullback_limit", "btc_weekday_lob_momentum"}))
            stack.enter_context(patch("learning.trading_manager_agent.config.get_ctrader_xau_active_families", return_value={"xau_scalp_pullback_limit"}))
            stack.enter_context(patch("learning.trading_manager_agent.config.get_persistent_canary_experimental_families", return_value={"xau_scalp_tick_depth_filter", "xau_scalp_failed_fade_follow_stop"}))
            stack.enter_context(patch.object(self.agent, "_current_value", side_effect=lambda key: {"CTRADER_XAU_PRIMARY_FAMILY": "xau_scalp_pullback_limit", "CTRADER_XAU_ACTIVE_FAMILIES": "xau_scalp_pullback_limit", "PERSISTENT_CANARY_STRATEGY_FAMILIES": "xau_scalp_pullback_limit,btc_weekday_lob_momentum"}.get(key, "")))
            stack.enter_context(patch.object(self.agent, "_apply_runtime_value"))
            stack.enter_context(patch.object(self.agent, "_persist_env_value", return_value={"ok": True, "updated": False, "reason": "persist_disabled"}))
            stack.enter_context(patch("learning.trading_manager_agent.macro_news.high_impact_headlines", return_value=[]))
            stack.enter_context(patch("learning.trading_manager_agent.macro_impact_tracker.sync", return_value={"ok": True}))
            stack.enter_context(patch("learning.trading_manager_agent.macro_impact_tracker.build_report", return_value={"entries": []}))
            stack.enter_context(patch("learning.trading_manager_agent.economic_calendar.next_events", return_value=[]))
            report = self.agent.build_report(hours=24)

        xau = next(row for row in list(report.get("symbols") or []) if row.get("symbol") == "XAUUSD")
        self.assertEqual(float((xau.get("pb_source_stats") or {}).get("pnl_usd", 0.0) or 0.0), -12.5)
        self.assertEqual(float((xau.get("scheduled_source_stats") or {}).get("pnl_usd", 0.0) or 0.0), 29.85)
        routing = dict(xau.get("family_routing_recommendations") or {})
        self.assertEqual(str(routing.get("mode") or ""), "scheduled_dominant_demote_pb")
        self.assertEqual(str((routing.get("changes") or {}).get("CTRADER_XAU_PRIMARY_FAMILY") or ""), "xau_scalp_tick_depth_filter")

    def test_trading_manager_demotes_pb_with_scheduled_calibration_fallback(self):
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
                    (
                        1,
                        "2026-03-11T09:00:00Z",
                        "scalp_xauusd:pb:canary",
                        "XAUUSD",
                        "long",
                        "limit",
                        72.8,
                        5200.0,
                        5195.0,
                        5204.0,
                        json.dumps({"payload": {"direction": "long", "session": "london,new_york,overlap", "timeframe": "5m+1m", "pattern": "PULLBACK_LIMIT", "entry_type": "limit"}}),
                        "{}",
                    ),
                    (
                        2,
                        "2026-03-11T09:04:00Z",
                        "scalp_xauusd:pb:canary",
                        "XAUUSD",
                        "short",
                        "limit",
                        73.2,
                        5198.0,
                        5202.0,
                        5193.0,
                        json.dumps({"payload": {"direction": "short", "session": "new_york", "timeframe": "5m+1m", "pattern": "PULLBACK_LIMIT", "entry_type": "limit"}}),
                        "{}",
                    ),
                    (
                        3,
                        "2026-03-11T09:08:00Z",
                        "scalp_xauusd:pb:canary",
                        "XAUUSD",
                        "long",
                        "limit",
                        72.6,
                        5197.0,
                        5192.0,
                        5201.0,
                        json.dumps({"payload": {"direction": "long", "session": "new_york", "timeframe": "5m+1m", "pattern": "PULLBACK_LIMIT", "entry_type": "limit"}}),
                        "{}",
                    ),
                    (
                        4,
                        "2026-03-11T09:12:00Z",
                        "xauusd_scheduled:canary",
                        "XAUUSD",
                        "long",
                        "limit",
                        76.5,
                        5188.0,
                        5178.0,
                        5204.0,
                        json.dumps({"payload": {"direction": "long", "session": "london", "timeframe": "1h", "pattern": "Bullish OB + BOS", "entry_type": "limit"}}),
                        "{}",
                    ),
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
                    (301, "scalp_xauusd:pb:canary", "XAUUSD", -5.20, 0, 1, 1, "2026-03-11T09:03:00Z", json.dumps({"raw": {"closePositionDetail": {"entryPrice": 5200.0}}})),
                    (302, "scalp_xauusd:pb:canary", "XAUUSD", -4.90, 0, 1, 2, "2026-03-11T09:07:00Z", json.dumps({"raw": {"closePositionDetail": {"entryPrice": 5198.0}}})),
                    (303, "scalp_xauusd:pb:canary", "XAUUSD", -3.80, 0, 1, 3, "2026-03-11T09:11:00Z", json.dumps({"raw": {"closePositionDetail": {"entryPrice": 5197.0}}})),
                    (304, "xauusd_scheduled:canary", "XAUUSD", -2.50, 0, 1, 4, "2026-03-11T09:18:00Z", json.dumps({"raw": {"closePositionDetail": {"entryPrice": 5188.0}}})),
                ],
            )

        (self.report_dir / "mission_progress_report.json").write_text(
            json.dumps({"symbols": [{"symbol": "XAUUSD", "selected_family": "xau_scalp_pullback_limit", "selected_regime": "pullback_priority"}]}),
            encoding="utf-8",
        )
        (self.report_dir / "winner_memory_library_report.json").write_text(
            json.dumps({"top_by_symbol": {"XAUUSD": {"family": "xau_scheduled_trend", "session": "london", "direction": "long"}}}),
            encoding="utf-8",
        )
        (self.report_dir / "family_calibration_report.json").write_text(
            json.dumps(
                {
                    "families": [
                        {
                            "symbol": "XAUUSD",
                            "family": "xau_scheduled_trend",
                            "overall": {"resolved": 28, "win_rate": 0.82, "pnl_usd": 145.0},
                        }
                    ]
                }
            ),
            encoding="utf-8",
        )

        with ExitStack() as stack:
            stack.enter_context(patch("learning.trading_manager_agent.config.TRADING_MANAGER_REPORT_TIMEZONE", "Asia/Bangkok"))
            stack.enter_context(patch("learning.trading_manager_agent.config.TRADING_MANAGER_REPORT_LOOKBACK_HOURS", 24))
            stack.enter_context(patch("learning.trading_manager_agent.config.TRADING_MANAGER_REPORT_SYMBOLS", "XAUUSD"))
            stack.enter_context(patch("learning.trading_manager_agent.config.TRADING_MANAGER_AUTO_TUNE_ENABLED", False, create=True))
            stack.enter_context(patch("learning.trading_manager_agent.config.TRADING_MANAGER_AUTO_ROUTING_ENABLED", True, create=True))
            stack.enter_context(patch("learning.trading_manager_agent.config.TRADING_MANAGER_AUTO_ROUTING_PERSIST_ENV", False, create=True))
            stack.enter_context(patch("learning.trading_manager_agent.config.TRADING_MANAGER_XAU_SWARM_SAMPLING_ENABLED", False, create=True))
            stack.enter_context(patch("learning.trading_manager_agent.config.TRADING_MANAGER_XAU_PB_DEMOTE_USE_CALIBRATION_FALLBACK", True, create=True))
            stack.enter_context(patch("learning.trading_manager_agent.config.TRADING_MANAGER_XAU_PB_DEMOTE_MIN_PB_RESOLVED", 3, create=True))
            stack.enter_context(patch("learning.trading_manager_agent.config.TRADING_MANAGER_XAU_PB_DEMOTE_MAX_PB_PNL_USD", -10.0, create=True))
            stack.enter_context(patch("learning.trading_manager_agent.config.TRADING_MANAGER_XAU_PB_DEMOTE_MIN_SCHEDULED_RESOLVED", 6, create=True))
            stack.enter_context(patch("learning.trading_manager_agent.config.TRADING_MANAGER_XAU_PB_DEMOTE_MIN_SCHEDULED_PNL_USD", 10.0, create=True))
            stack.enter_context(patch("learning.trading_manager_agent.config.TRADING_MANAGER_XAU_PB_DEMOTE_MIN_SCHEDULED_WIN_RATE", 0.60, create=True))
            stack.enter_context(patch("learning.trading_manager_agent.config.TRADING_MANAGER_XAU_PB_DEMOTE_MIN_SCHEDULED_CALIB_RESOLVED", 20, create=True))
            stack.enter_context(patch("learning.trading_manager_agent.config.TRADING_MANAGER_XAU_PB_DEMOTE_MIN_SCHEDULED_CALIB_PNL_USD", 40.0, create=True))
            stack.enter_context(patch("learning.trading_manager_agent.config.TRADING_MANAGER_XAU_PB_DEMOTE_MIN_SCHEDULED_CALIB_WIN_RATE", 0.72, create=True))
            stack.enter_context(patch("learning.trading_manager_agent.config.get_persistent_canary_strategy_families", return_value={"xau_scalp_pullback_limit", "xau_scalp_tick_depth_filter", "xau_scalp_failed_fade_follow_stop"}))
            stack.enter_context(patch("learning.trading_manager_agent.config.get_ctrader_xau_active_families", return_value={"xau_scalp_pullback_limit"}))
            stack.enter_context(patch("learning.trading_manager_agent.config.get_persistent_canary_experimental_families", return_value={"xau_scalp_tick_depth_filter", "xau_scalp_failed_fade_follow_stop"}))
            stack.enter_context(patch.object(self.agent, "_current_value", side_effect=lambda key: {"CTRADER_XAU_PRIMARY_FAMILY": "xau_scalp_pullback_limit", "CTRADER_XAU_ACTIVE_FAMILIES": "xau_scalp_pullback_limit"}.get(key, "")))
            stack.enter_context(patch.object(self.agent, "_apply_runtime_value"))
            stack.enter_context(patch.object(self.agent, "_persist_env_value", return_value={"ok": True, "updated": False, "reason": "persist_disabled"}))
            stack.enter_context(patch("learning.trading_manager_agent.macro_news.high_impact_headlines", return_value=[]))
            stack.enter_context(patch("learning.trading_manager_agent.macro_impact_tracker.sync", return_value={"ok": True}))
            stack.enter_context(patch("learning.trading_manager_agent.macro_impact_tracker.build_report", return_value={"entries": []}))
            stack.enter_context(patch("learning.trading_manager_agent.economic_calendar.next_events", return_value=[]))
            report = self.agent.build_report(hours=24)

        xau = next(row for row in list(report.get("symbols") or []) if row.get("symbol") == "XAUUSD")
        routing = dict(xau.get("family_routing_recommendations") or {})
        self.assertEqual(str(routing.get("mode") or ""), "scheduled_dominant_demote_pb")
        self.assertEqual(str(routing.get("support_mode") or ""), "calibration_fallback")

    def test_trading_manager_applies_xau_order_care_from_recent_losses(self):
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
                    (
                        1,
                        "2026-03-11T11:00:00Z",
                        "scalp_xauusd:td:canary",
                        "XAUUSD",
                        "long",
                        "limit",
                        60.1,
                        5108.45,
                        5106.29,
                        5110.39,
                        json.dumps({"payload": {"direction": "long", "session": "asian", "timeframe": "5m+1m", "pattern": "SCALP_FLOW_FORCE|XAU_SCALP_TICK_DEPTH_FILTER", "entry_type": "limit"}}),
                        "{}",
                    ),
                    (
                        2,
                        "2026-03-11T11:05:00Z",
                        "scalp_xauusd:td:canary",
                        "XAUUSD",
                        "short",
                        "limit",
                        72.0,
                        5106.91,
                        5109.14,
                        5104.84,
                        json.dumps({"payload": {"direction": "short", "session": "asian", "timeframe": "5m+1m", "pattern": "SCALP_FLOW_FORCE|XAU_SCALP_TICK_DEPTH_FILTER", "entry_type": "limit"}}),
                        "{}",
                    ),
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
                    (201, "scalp_xauusd:td:canary", "XAUUSD", -2.41, 0, 1, 1, "2026-03-11T11:08:00Z", "{}"),
                    (202, "scalp_xauusd:td:canary", "XAUUSD", -2.44, 0, 1, 2, "2026-03-11T11:12:00Z", "{}"),
                ],
            )
            conn.commit()

        with patch("learning.trading_manager_agent.config.TRADING_MANAGER_REPORT_SYMBOLS", "XAUUSD"), \
             patch("learning.trading_manager_agent.config.TRADING_MANAGER_AUTO_TUNE_ENABLED", False, create=True), \
             patch("learning.trading_manager_agent.config.TRADING_MANAGER_AUTO_ROUTING_ENABLED", True, create=True), \
             patch("learning.trading_manager_agent.config.TRADING_MANAGER_AUTO_ROUTING_PERSIST_ENV", False, create=True), \
             patch("learning.trading_manager_agent.config.TRADING_MANAGER_XAU_ORDER_CARE_CONTINUATION_NO_FOLLOW_AGE_MIN", 5.0, create=True), \
             patch("learning.trading_manager_agent.macro_news.high_impact_headlines", return_value=[]), \
             patch("learning.trading_manager_agent.macro_impact_tracker.sync", return_value={"ok": True}), \
             patch("learning.trading_manager_agent.macro_impact_tracker.build_report", return_value={"entries": []}), \
             patch("learning.trading_manager_agent.economic_calendar.next_events", return_value=[]):
            report = self.agent.build_report(hours=24)

        xau = next(row for row in list(report.get("symbols") or []) if row.get("symbol") == "XAUUSD")
        rec = dict(xau.get("order_care_recommendations") or {})
        apply_out = dict(report.get("order_care_apply") or {})
        state = self.agent._load_state()

        self.assertTrue(rec.get("active"))
        self.assertEqual(str(rec.get("mode") or ""), "continuation_fail_fast")
        self.assertEqual(str(apply_out.get("status") or ""), "applied")
        care_state = dict(state.get("xau_order_care") or {})
        self.assertEqual(str(care_state.get("mode") or ""), "continuation_fail_fast")
        self.assertEqual(str(care_state.get("status") or ""), "active")
        self.assertIn("scalp_xauusd:canary", list(care_state.get("allowed_sources") or []))
        self.assertIn("scalp_xauusd:td:canary", list(care_state.get("allowed_sources") or []))
        desks = dict(care_state.get("desks") or {})
        self.assertIn("fss_confirmation", desks)
        self.assertIn("limit_retest", desks)
        self.assertEqual(str(((desks.get("fss_confirmation") or {}).get("mode") or "")), "continuation_fail_fast")
        self.assertEqual(str(((desks.get("limit_retest") or {}).get("mode") or "")), "retest_absorption_guard")
        self.assertAlmostEqual(float((dict(care_state.get("overrides") or {})).get("no_follow_age_min") or 0.0), 5.0, places=6)
        self.assertGreaterEqual(int((dict(care_state.get("overrides") or {})).get("close_score") or 0), 4)

    def test_trading_manager_holds_recent_xau_order_care_within_min_active_window(self):
        self.agent._save_state(
            {
                "xau_order_care": {
                    "status": "active",
                    "mode": "continuation_fail_fast",
                    "reason": "hold for cooldown",
                    "allowed_sources": ["scalp_xauusd:canary", "scalp_xauusd:td:canary"],
                    "overrides": {"be_trigger_r": 0.12, "be_lock_r": 0.01},
                    "applied_at": "2026-03-11T11:55:00Z",
                }
            }
        )
        (self.report_dir / "mission_progress_report.json").write_text(json.dumps({"symbols": []}), encoding="utf-8")
        (self.report_dir / "winner_memory_library_report.json").write_text(json.dumps({"top_by_symbol": {}}), encoding="utf-8")
        (self.report_dir / "family_calibration_report.json").write_text(json.dumps({"families": []}), encoding="utf-8")
        (self.report_dir / "chart_state_memory_report.json").write_text(json.dumps({"states": []}), encoding="utf-8")
        (self.report_dir / "ct_only_experiment_report.json").write_text(json.dumps({"sources": []}), encoding="utf-8")

        with patch("learning.trading_manager_agent.config.TRADING_MANAGER_REPORT_SYMBOLS", "XAUUSD"), \
             patch("learning.trading_manager_agent.config.TRADING_MANAGER_AUTO_TUNE_ENABLED", False, create=True), \
             patch("learning.trading_manager_agent.config.TRADING_MANAGER_AUTO_ROUTING_ENABLED", True, create=True), \
             patch("learning.trading_manager_agent.config.TRADING_MANAGER_AUTO_ROUTING_PERSIST_ENV", False, create=True), \
             patch("learning.trading_manager_agent.config.TRADING_MANAGER_XAU_ORDER_CARE_MIN_ACTIVE_MIN", 45, create=True), \
             patch("learning.trading_manager_agent._utc_now", return_value=datetime(2026, 3, 11, 12, 20, tzinfo=timezone.utc)), \
             patch("learning.trading_manager_agent.macro_news.high_impact_headlines", return_value=[]), \
             patch("learning.trading_manager_agent.macro_impact_tracker.sync", return_value={"ok": True}), \
             patch("learning.trading_manager_agent.macro_impact_tracker.build_report", return_value={"entries": []}), \
             patch("learning.trading_manager_agent.economic_calendar.next_events", return_value=[]):
            report = self.agent.build_report(hours=24)

        apply_out = dict(report.get("order_care_apply") or {})
        state = self.agent._load_state()
        care_state = dict(state.get("xau_order_care") or {})

        self.assertEqual(str(apply_out.get("status") or ""), "held")
        self.assertEqual(str(care_state.get("status") or ""), "active")
        self.assertIn("scalp_xauusd:canary", list(care_state.get("allowed_sources") or []))

    def test_trading_manager_adds_flow_short_sidecar_to_experimental_set(self):
        (self.report_dir / "chart_state_memory_report.json").write_text(
            json.dumps(
                {
                    "states": [
                        {
                            "symbol": "XAUUSD",
                            "state_label": "continuation_drive",
                            "direction": "short",
                            "follow_up_candidate": True,
                            "state_score": 31.0,
                            "stats": {"resolved": 4, "pnl_usd": 12.4},
                        }
                    ]
                }
            ),
            encoding="utf-8",
        )
        (self.report_dir / "ct_only_experiment_report.json").write_text(
            json.dumps({"sources": [], "summary": {"xau_fss_resolved": 0}}),
            encoding="utf-8",
        )

        with ExitStack() as stack:
            stack.enter_context(patch("learning.trading_manager_agent.config.TRADING_MANAGER_REPORT_SYMBOLS", "XAUUSD"))
            stack.enter_context(patch("learning.trading_manager_agent.config.TRADING_MANAGER_AUTO_ROUTING_ENABLED", True, create=True))
            stack.enter_context(patch("learning.trading_manager_agent.config.TRADING_MANAGER_AUTO_ROUTING_PERSIST_ENV", False, create=True))
            stack.enter_context(patch("learning.trading_manager_agent.config.XAU_FLOW_SHORT_SIDECAR_ENABLED", True, create=True))
            stack.enter_context(patch("learning.trading_manager_agent.config.XAU_FLOW_SHORT_SIDECAR_MIN_RESOLVED", 3, create=True))
            stack.enter_context(patch("learning.trading_manager_agent.config.XAU_FLOW_SHORT_SIDECAR_MIN_STATE_SCORE", 28.0, create=True))
            stack.enter_context(patch("learning.trading_manager_agent.config.PERSISTENT_CANARY_EXPERIMENTAL_FAMILIES", "xau_scalp_tick_depth_filter,xau_scalp_failed_fade_follow_stop,xau_scalp_microtrend_follow_up", create=True))
            stack.enter_context(patch("learning.trading_manager_agent.config.get_persistent_canary_experimental_families", return_value={"xau_scalp_tick_depth_filter", "xau_scalp_failed_fade_follow_stop", "xau_scalp_microtrend_follow_up"}))
            stack.enter_context(patch.object(self.agent, "_current_value", side_effect=lambda key: {"PERSISTENT_CANARY_EXPERIMENTAL_FAMILIES": "xau_scalp_tick_depth_filter,xau_scalp_failed_fade_follow_stop,xau_scalp_microtrend_follow_up"}.get(key, "")))
            stack.enter_context(patch.object(self.agent, "_apply_runtime_value"))
            stack.enter_context(patch.object(self.agent, "_persist_env_value", return_value={"ok": True, "updated": False, "reason": "persist_disabled"}))
            stack.enter_context(patch("learning.trading_manager_agent.macro_news.high_impact_headlines", return_value=[]))
            stack.enter_context(patch("learning.trading_manager_agent.macro_impact_tracker.sync", return_value={"ok": True}))
            stack.enter_context(patch("learning.trading_manager_agent.macro_impact_tracker.build_report", return_value={"entries": []}))
            stack.enter_context(patch("learning.trading_manager_agent.economic_calendar.next_events", return_value=[]))
            report = self.agent.build_report(hours=24)

        xau = next(row for row in list(report.get("symbols") or []) if row.get("symbol") == "XAUUSD")
        rec = dict(xau.get("opportunity_sidecar_recommendations") or {})
        self.assertTrue(bool(rec.get("active")))
        self.assertIn("xau_scalp_flow_short_sidecar", str((rec.get("changes") or {}).get("PERSISTENT_CANARY_EXPERIMENTAL_FAMILIES") or ""))
        apply_row = dict(report.get("opportunity_sidecar_apply") or {})
        self.assertIn(str(apply_row.get("status") or ""), {"applied", "already_active"})

    def test_trading_manager_builds_opportunity_feed_and_saves_state(self):
        (self.report_dir / "mission_progress_report.json").write_text(
            json.dumps({"symbols": [{"symbol": "XAUUSD", "selected_family": "xau_scheduled_trend", "selected_regime": "trend_priority"}]}),
            encoding="utf-8",
        )
        (self.report_dir / "winner_memory_library_report.json").write_text(
            json.dumps({"top_by_symbol": {"XAUUSD": {"family": "xau_scheduled_trend", "session": "london,new_york,overlap", "direction": "short"}}}),
            encoding="utf-8",
        )
        (self.report_dir / "chart_state_memory_report.json").write_text(
            json.dumps(
                {
                    "states": [
                        {
                            "symbol": "XAUUSD",
                            "state_label": "continuation_drive",
                            "direction": "short",
                            "session": "london,new_york,overlap",
                            "follow_up_candidate": True,
                            "follow_up_plan": "follow_with_shallow_retest_or_break_stop",
                            "profitable_state": True,
                            "state_score": 49.6,
                            "best_family": {"family": "xau_scalp_flow_short_sidecar"},
                            "stats": {"resolved": 4, "pnl_usd": 12.5},
                        }
                    ]
                }
            ),
            encoding="utf-8",
        )
        (self.report_dir / "ct_only_experiment_report.json").write_text(
            json.dumps(
                {
                    "sources": [
                        {
                            "source": "scalp_xauusd:td:canary",
                            "symbol": "XAUUSD",
                            "closed_total": {"resolved": 5, "wins": 3, "losses": 2, "pnl_usd": 4.2},
                        }
                    ]
                }
            ),
            encoding="utf-8",
        )
        with ExitStack() as stack:
            stack.enter_context(patch("learning.trading_manager_agent.config.TRADING_MANAGER_REPORT_SYMBOLS", "XAUUSD"))
            stack.enter_context(patch("learning.trading_manager_agent.config.TRADING_MANAGER_OPPORTUNITY_FEED_ENABLED", True, create=True))
            stack.enter_context(patch("learning.trading_manager_agent.config.TRADING_MANAGER_OPPORTUNITY_FEED_TOPK", 3, create=True))
            stack.enter_context(patch("learning.trading_manager_agent.config.TRADING_MANAGER_OPPORTUNITY_FEED_MIN_STATE_SCORE", 24.0, create=True))
            stack.enter_context(patch("learning.trading_manager_agent.config.TRADING_MANAGER_OPPORTUNITY_FEED_MIN_RESOLVED", 2, create=True))
            stack.enter_context(patch("learning.trading_manager_agent.config.TRADING_MANAGER_AUTO_ROUTING_ENABLED", True, create=True))
            stack.enter_context(patch("learning.trading_manager_agent.config.TRADING_MANAGER_AUTO_ROUTING_PERSIST_ENV", False, create=True))
            stack.enter_context(patch("learning.trading_manager_agent.config.TRADING_MANAGER_XAU_SWARM_SAMPLING_ENABLED", False, create=True))
            stack.enter_context(patch.object(self.agent, "_current_value", side_effect=lambda key: {"CTRADER_XAU_PRIMARY_FAMILY": "xau_scalp_pullback_limit"}.get(key, "")))
            stack.enter_context(patch("learning.trading_manager_agent.macro_news.high_impact_headlines", return_value=[]))
            stack.enter_context(patch("learning.trading_manager_agent.macro_impact_tracker.sync", return_value={"ok": True}))
            stack.enter_context(patch("learning.trading_manager_agent.macro_impact_tracker.build_report", return_value={"entries": []}))
            stack.enter_context(patch("learning.trading_manager_agent.economic_calendar.next_events", return_value=[]))
            report = self.agent.build_report(hours=24)

        feed = dict(report.get("opportunity_feed") or {})
        xau_feed = dict((feed.get("symbols") or {}).get("XAUUSD") or {})
        self.assertTrue(bool(xau_feed.get("active")))
        self.assertIn("xau_scalp_flow_short_sidecar", dict(xau_feed.get("family_priority_map") or {}))
        self.assertIn("xau_scheduled_trend", dict(xau_feed.get("family_priority_map") or {}))
        state = json.loads(self.agent.state_path.read_text(encoding="utf-8"))
        self.assertEqual(str(((state.get("opportunity_feed") or {}).get("status")) or ""), "active")
        self.assertIn("XAUUSD", dict(((state.get("opportunity_feed") or {}).get("symbols")) or {}))

    def test_trading_manager_prioritizes_range_repair_for_range_probe_state(self):
        (self.report_dir / "mission_progress_report.json").write_text(
            json.dumps({"symbols": [{"symbol": "XAUUSD", "selected_family": "xau_scheduled_trend", "selected_regime": "range_probe"}]}),
            encoding="utf-8",
        )
        (self.report_dir / "winner_memory_library_report.json").write_text(
            json.dumps({"top_by_symbol": {}}),
            encoding="utf-8",
        )
        (self.report_dir / "chart_state_memory_report.json").write_text(
            json.dumps(
                {
                    "states": [
                        {
                            "symbol": "XAUUSD",
                            "state_label": "range_probe",
                            "direction": "long",
                            "session": "new_york",
                            "follow_up_candidate": True,
                            "follow_up_plan": "fade_range_edge_with_limit_only",
                            "profitable_state": True,
                            "state_score": 46.0,
                            "best_family": {"family": "xau_scalp_range_repair"},
                            "stats": {"resolved": 3, "pnl_usd": 5.4},
                        }
                    ]
                }
            ),
            encoding="utf-8",
        )
        (self.report_dir / "ct_only_experiment_report.json").write_text(
            json.dumps(
                {
                    "sources": [
                        {
                            "source": "scalp_xauusd:rr:canary",
                            "symbol": "XAUUSD",
                            "closed_total": {"resolved": 3, "wins": 2, "losses": 1, "pnl_usd": 3.1},
                        }
                    ]
                }
            ),
            encoding="utf-8",
        )
        with ExitStack() as stack:
            stack.enter_context(patch("learning.trading_manager_agent.config.TRADING_MANAGER_REPORT_SYMBOLS", "XAUUSD"))
            stack.enter_context(patch("learning.trading_manager_agent.config.TRADING_MANAGER_OPPORTUNITY_FEED_ENABLED", True, create=True))
            stack.enter_context(patch("learning.trading_manager_agent.config.TRADING_MANAGER_OPPORTUNITY_FEED_TOPK", 3, create=True))
            stack.enter_context(patch("learning.trading_manager_agent.config.TRADING_MANAGER_OPPORTUNITY_FEED_MIN_STATE_SCORE", 24.0, create=True))
            stack.enter_context(patch("learning.trading_manager_agent.config.TRADING_MANAGER_OPPORTUNITY_FEED_MIN_RESOLVED", 2, create=True))
            stack.enter_context(patch("learning.trading_manager_agent.config.TRADING_MANAGER_AUTO_ROUTING_ENABLED", True, create=True))
            stack.enter_context(patch("learning.trading_manager_agent.config.TRADING_MANAGER_AUTO_ROUTING_PERSIST_ENV", False, create=True))
            stack.enter_context(patch("learning.trading_manager_agent.config.TRADING_MANAGER_XAU_SWARM_SAMPLING_ENABLED", False, create=True))
            stack.enter_context(patch("learning.trading_manager_agent.macro_news.high_impact_headlines", return_value=[]))
            stack.enter_context(patch("learning.trading_manager_agent.macro_impact_tracker.sync", return_value={"ok": True}))
            stack.enter_context(patch("learning.trading_manager_agent.macro_impact_tracker.build_report", return_value={"entries": []}))
            stack.enter_context(patch("learning.trading_manager_agent.economic_calendar.next_events", return_value=[]))
            report = self.agent.build_report(hours=24)

        xau_feed = dict(((report.get("opportunity_feed") or {}).get("symbols") or {}).get("XAUUSD") or {})
        priority_families = list(xau_feed.get("priority_families") or [])
        self.assertTrue(priority_families)
        priority_map = dict(xau_feed.get("family_priority_map") or {})
        self.assertIn("xau_scalp_range_repair", priority_map)
        self.assertGreater(float(priority_map.get("xau_scalp_range_repair", 0.0) or 0.0), 90.0)
        self.assertIn("xau_scalp_range_repair", priority_families[:3])

    def test_range_repair_order_care_profile_disables_extension(self):
        profile = self.agent._order_care_profile("retest_absorption_guard", desk="range_repair")
        self.assertEqual(str(profile.get("desk") or ""), "range_repair")
        self.assertGreater(float(profile.get("extension_min_confidence", 0.0) or 0.0), 100.0)
        self.assertGreater(int(profile.get("extension_score", 0) or 0), 90)

    def test_xau_profile_recommendation_skips_pre_event_without_urgent_event(self):
        rec = self.agent._derive_xau_profile_recommendation(
            shock={"shock_type": "fast_selloff_repricing"},
            losses={"resolved": 4, "losses": 4, "pnl_usd": -12.5},
            best_same_situation={"family": "xau_scalp_tick_depth_filter", "pnl_usd": 8.4},
            upcoming_events=[],
        )
        self.assertEqual(rec, {})

    def test_why_no_trade_ignores_pre_event_blocker_for_xau_short_continuation(self):
        diag = self.agent._derive_why_no_trade_diagnostics(
            symbol="XAUUSD",
            symbol_closed=[],
            open_positions=[],
            open_orders=[],
            chart_state_memory={
                "states": [
                    {
                        "symbol": "XAUUSD",
                        "state_label": "continuation_drive",
                        "direction": "short",
                        "day_type": "trend",
                        "follow_up_candidate": True,
                        "state_score": 42.0,
                    }
                ]
            },
            experiment_report={"sources": []},
            opportunity_feed_symbol={},
            selected_family="xau_scalp_tick_depth_filter",
            manager_state={
                "xau_shock_profile": {
                    "status": "active",
                    "mode": "pre_event_caution",
                },
                "xau_parallel_families": {
                    "mode": "state_parallel_same_direction",
                    "max_same_direction_families": 3,
                    "allowed_families": ["xau_scalp_tick_depth_filter"],
                },
            },
        )
        blockers = list(diag.get("likely_blockers") or [])
        coaching = list(diag.get("coaching") or [])
        self.assertFalse(any("pre_event_caution" in str(item) for item in blockers))
        self.assertTrue(any("short continuation remains eligible" in str(item) for item in coaching))

    def test_why_no_trade_treats_shock_protect_as_caution_not_global_blocker(self):
        diag = self.agent._derive_why_no_trade_diagnostics(
            symbol="XAUUSD",
            symbol_closed=[],
            open_positions=[],
            open_orders=[],
            chart_state_memory={
                "states": [
                    {
                        "symbol": "XAUUSD",
                        "state_label": "continuation_drive",
                        "direction": "short",
                        "day_type": "trend",
                        "follow_up_candidate": True,
                        "state_score": 48.3,
                    }
                ]
            },
            experiment_report={"sources": []},
            opportunity_feed_symbol={},
            selected_family="xau_scalp_flow_short_sidecar",
            manager_state={
                "xau_shock_profile": {
                    "status": "active",
                    "mode": "shock_protect",
                },
                "xau_parallel_families": {
                    "mode": "state_parallel_same_direction",
                    "max_same_direction_families": 4,
                    "allowed_families": ["xau_scalp_flow_short_sidecar", "xau_scalp_range_repair"],
                },
            },
        )
        blockers = list(diag.get("likely_blockers") or [])
        coaching = list(diag.get("coaching") or [])
        self.assertFalse(any("shock_protect" in str(item) for item in blockers))
        self.assertTrue(any("shock_protect" in str(item) for item in coaching))
        self.assertTrue(any("short continuation remains eligible" in str(item) for item in coaching))

    def test_trading_manager_applies_micro_regime_and_cluster_loss_guard(self):
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
                    (
                        1,
                        "2026-03-11T11:49:00Z",
                        "scalp_xauusd:pb:canary",
                        "XAUUSD",
                        "long",
                        "limit",
                        72.1,
                        5108.50,
                        5105.90,
                        5111.40,
                        json.dumps({"payload": {"direction": "long", "session": "london", "timeframe": "5m+1m", "pattern": "SCALP_FLOW_FORCE", "entry_type": "limit"}}),
                        "{}",
                    ),
                    (
                        2,
                        "2026-03-11T11:53:00Z",
                        "scalp_xauusd:td:canary",
                        "XAUUSD",
                        "long",
                        "limit",
                        71.9,
                        5107.40,
                        5104.70,
                        5110.10,
                        json.dumps({"payload": {"direction": "long", "session": "london", "timeframe": "5m+1m", "pattern": "SCALP_FLOW_FORCE|XAU_SCALP_TICK_DEPTH_FILTER", "entry_type": "limit"}}),
                        "{}",
                    ),
                    (
                        3,
                        "2026-03-11T11:57:00Z",
                        "scalp_xauusd:mfu:canary",
                        "XAUUSD",
                        "long",
                        "buy_stop",
                        70.8,
                        5109.20,
                        5106.20,
                        5112.10,
                        json.dumps({"payload": {"direction": "long", "session": "london", "timeframe": "5m+1m", "pattern": "MICROTREND_FOLLOW_UP", "entry_type": "buy_stop"}}),
                        "{}",
                    ),
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
                    (301, "scalp_xauusd:pb:canary", "XAUUSD", -2.91, 0, 1, 1, "2026-03-11T11:51:00Z", "{}"),
                    (302, "scalp_xauusd:td:canary", "XAUUSD", -3.12, 0, 1, 2, "2026-03-11T11:56:00Z", "{}"),
                    (303, "scalp_xauusd:mfu:canary", "XAUUSD", -2.88, 0, 1, 3, "2026-03-11T11:59:00Z", "{}"),
                ],
            )
            conn.commit()

        (self.report_dir / "mission_progress_report.json").write_text(
            json.dumps({"symbols": [{"symbol": "XAUUSD", "selected_family": "xau_scalp_tick_depth_filter", "selected_regime": "trend_priority"}]}),
            encoding="utf-8",
        )
        (self.report_dir / "family_calibration_report.json").write_text(
            json.dumps({"families": [{"symbol": "XAUUSD", "family": "xau_scheduled_trend", "overall": {"resolved": 28, "win_rate": 0.82, "pnl_usd": 145.0}}]}),
            encoding="utf-8",
        )
        (self.report_dir / "winner_memory_library_report.json").write_text(json.dumps({"top_by_symbol": {}}), encoding="utf-8")
        (self.report_dir / "chart_state_memory_report.json").write_text(json.dumps({"states": []}), encoding="utf-8")
        (self.report_dir / "ct_only_experiment_report.json").write_text(json.dumps({"sources": []}), encoding="utf-8")

        with ExitStack() as stack:
            stack.enter_context(patch("learning.trading_manager_agent.config.TRADING_MANAGER_REPORT_SYMBOLS", "XAUUSD"))
            stack.enter_context(patch("learning.trading_manager_agent.config.TRADING_MANAGER_AUTO_TUNE_ENABLED", False, create=True))
            stack.enter_context(patch("learning.trading_manager_agent.config.TRADING_MANAGER_AUTO_ROUTING_ENABLED", True, create=True))
            stack.enter_context(patch("learning.trading_manager_agent.config.TRADING_MANAGER_AUTO_ROUTING_PERSIST_ENV", False, create=True))
            stack.enter_context(patch("learning.trading_manager_agent.config.TRADING_MANAGER_XAU_MICRO_REGIME_REFRESH_ENABLED", True, create=True))
            stack.enter_context(patch("learning.trading_manager_agent.config.TRADING_MANAGER_XAU_MICRO_REGIME_WINDOW_MIN", 12, create=True))
            stack.enter_context(patch("learning.trading_manager_agent.config.TRADING_MANAGER_XAU_MICRO_REGIME_MIN_RESOLVED", 3, create=True))
            stack.enter_context(patch("learning.trading_manager_agent.config.TRADING_MANAGER_XAU_CLUSTER_LOSS_GUARD_ENABLED", True, create=True))
            stack.enter_context(patch("learning.trading_manager_agent.config.TRADING_MANAGER_XAU_CLUSTER_LOSS_GUARD_WINDOW_MIN", 12, create=True))
            stack.enter_context(patch("learning.trading_manager_agent.config.TRADING_MANAGER_XAU_CLUSTER_LOSS_GUARD_MIN_RESOLVED", 3, create=True))
            stack.enter_context(patch("learning.trading_manager_agent.config.TRADING_MANAGER_XAU_CLUSTER_LOSS_GUARD_MIN_LOSSES", 2, create=True))
            stack.enter_context(patch("learning.trading_manager_agent.config.TRADING_MANAGER_XAU_CLUSTER_LOSS_GUARD_MIN_DISTINCT_FAMILIES", 2, create=True))
            stack.enter_context(patch("learning.trading_manager_agent.config.TRADING_MANAGER_XAU_CLUSTER_LOSS_GUARD_MAX_PNL_USD", -5.0, create=True))
            stack.enter_context(patch("learning.trading_manager_agent.macro_news.high_impact_headlines", return_value=[]))
            stack.enter_context(patch("learning.trading_manager_agent.macro_impact_tracker.sync", return_value={"ok": True}))
            stack.enter_context(patch("learning.trading_manager_agent.macro_impact_tracker.build_report", return_value={"entries": []}))
            stack.enter_context(patch("learning.trading_manager_agent.economic_calendar.next_events", return_value=[]))
            report = self.agent.build_report(hours=24)

        xau = next(row for row in list(report.get("symbols") or []) if row.get("symbol") == "XAUUSD")
        micro = dict(xau.get("micro_regime_refresh") or {})
        guard = dict(xau.get("cluster_loss_guard_recommendations") or {})
        self.assertTrue(bool(micro.get("active")))
        self.assertEqual(str(micro.get("dominant_direction") or ""), "long")
        self.assertEqual(str(micro.get("state_label") or ""), "long_cluster_loss")
        self.assertTrue(bool(guard.get("active")))
        self.assertEqual(str(guard.get("blocked_direction") or ""), "long")
        self.assertIn(str((report.get("cluster_loss_guard_apply") or {}).get("status") or ""), {"applied", "already_active"})

        state = self.agent._load_state()
        self.assertEqual(str(((state.get("xau_micro_regime") or {}).get("status")) or ""), "active")
        self.assertEqual(str(((state.get("xau_cluster_loss_guard") or {}).get("status")) or ""), "active")

    def test_trading_manager_applies_xau_execution_directive_from_family_disagreement(self):
        with sqlite3.connect(str(self.ctrader_db)) as conn:
            conn.executemany(
                """
                INSERT INTO execution_journal(
                    id, created_ts, created_utc, source, symbol, direction, entry_type,
                    confidence, entry, stop_loss, take_profit, status, request_json,
                    response_json, execution_meta_json
                )
                VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                [
                    (
                        1,
                        1741693560.0,
                        "2026-03-11T11:46:00Z",
                        "scalp_xauusd:fss:canary",
                        "XAUUSD",
                        "short",
                        "sell_stop",
                        73.4,
                        5108.0,
                        5112.0,
                        5100.0,
                        "closed",
                        json.dumps({"signal_run_id": "run-disagree-1", "payload": {"direction": "short", "entry_type": "sell_stop"}}),
                        "{}",
                        json.dumps({"closed": {"execution_utc": "2026-03-11T11:47:00Z", "pnl_usd": 5.08}}),
                    ),
                    (
                        2,
                        1741693565.0,
                        "2026-03-11T11:46:05Z",
                        "scalp_xauusd:canary",
                        "XAUUSD",
                        "short",
                        "limit",
                        72.6,
                        5107.8,
                        5112.1,
                        5100.2,
                        "closed",
                        json.dumps({"signal_run_id": "run-disagree-1", "payload": {"direction": "short", "entry_type": "limit"}}),
                        "{}",
                        json.dumps({"closed": {"execution_utc": "2026-03-11T11:48:00Z", "pnl_usd": -14.38}}),
                    ),
                    (
                        3,
                        1741693570.0,
                        "2026-03-11T11:46:10Z",
                        "scalp_xauusd:td:canary",
                        "XAUUSD",
                        "short",
                        "limit",
                        72.1,
                        5107.7,
                        5112.1,
                        5100.1,
                        "closed",
                        json.dumps({"signal_run_id": "run-disagree-1", "payload": {"direction": "short", "entry_type": "limit"}}),
                        "{}",
                        json.dumps({"closed": {"execution_utc": "2026-03-11T11:48:20Z", "pnl_usd": -8.26}}),
                    ),
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
                    (401, "scalp_xauusd:fss:canary", "XAUUSD", 5.08, 1, 1, 1, "2026-03-11T11:47:00Z", "{}"),
                    (402, "scalp_xauusd:canary", "XAUUSD", -14.38, 0, 1, 2, "2026-03-11T11:48:00Z", "{}"),
                    (403, "scalp_xauusd:td:canary", "XAUUSD", -8.26, 0, 1, 3, "2026-03-11T11:48:20Z", "{}"),
                ],
            )
            conn.commit()

        (self.report_dir / "mission_progress_report.json").write_text(
            json.dumps({"symbols": [{"symbol": "XAUUSD", "selected_family": "xau_scalp_microtrend", "selected_regime": "scalp_priority"}]}),
            encoding="utf-8",
        )
        (self.report_dir / "family_calibration_report.json").write_text(json.dumps({"families": []}), encoding="utf-8")
        (self.report_dir / "winner_memory_library_report.json").write_text(json.dumps({"top_by_symbol": {}}), encoding="utf-8")
        (self.report_dir / "chart_state_memory_report.json").write_text(json.dumps({"states": []}), encoding="utf-8")
        (self.report_dir / "ct_only_experiment_report.json").write_text(json.dumps({"sources": []}), encoding="utf-8")

        with ExitStack() as stack:
            stack.enter_context(patch("learning.trading_manager_agent.config.TRADING_MANAGER_REPORT_SYMBOLS", "XAUUSD"))
            stack.enter_context(patch("learning.trading_manager_agent.config.TRADING_MANAGER_AUTO_TUNE_ENABLED", False, create=True))
            stack.enter_context(patch("learning.trading_manager_agent.config.TRADING_MANAGER_AUTO_ROUTING_ENABLED", True, create=True))
            stack.enter_context(patch("learning.trading_manager_agent.config.TRADING_MANAGER_AUTO_ROUTING_PERSIST_ENV", False, create=True))
            stack.enter_context(patch("learning.trading_manager_agent.config.TRADING_MANAGER_XAU_EXECUTION_DIRECTIVE_ENABLED", True, create=True))
            stack.enter_context(patch("learning.trading_manager_agent.config.CTRADER_XAU_SHORT_LIMIT_PAUSE_ENABLED", True, create=True))
            stack.enter_context(patch("learning.trading_manager_agent.config.CTRADER_XAU_SHORT_LIMIT_PAUSE_FAMILIES", "xau_scalp_microtrend,xau_scalp_tick_depth_filter", create=True))
            stack.enter_context(patch("learning.trading_manager_agent.config.CTRADER_XAU_SHORT_LIMIT_PAUSE_MIN", 20, create=True))
            stack.enter_context(patch("learning.trading_manager_agent.config.CTRADER_XAU_SHORT_LIMIT_PAUSE_LOOKBACK_MIN", 95, create=True))
            stack.enter_context(patch("learning.trading_manager_agent.config.CTRADER_XAU_PAIR_RISK_CAP_ENABLED", True, create=True))
            stack.enter_context(patch("learning.trading_manager_agent.config.CTRADER_XAU_PAIR_RISK_MAX_USD", 3.0, create=True))
            stack.enter_context(patch("learning.trading_manager_agent.config.CTRADER_XAU_PAIR_RISK_MIN_USD", 0.15, create=True))
            stack.enter_context(patch("learning.trading_manager_agent.macro_news.high_impact_headlines", return_value=[]))
            stack.enter_context(patch("learning.trading_manager_agent.macro_impact_tracker.sync", return_value={"ok": True}))
            stack.enter_context(patch("learning.trading_manager_agent.macro_impact_tracker.build_report", return_value={"entries": []}))
            stack.enter_context(patch("learning.trading_manager_agent.economic_calendar.next_events", return_value=[]))
            report = self.agent.build_report(hours=24)

        xau = next(row for row in list(report.get("symbols") or []) if row.get("symbol") == "XAUUSD")
        directive = dict(xau.get("execution_directive_recommendations") or {})
        self.assertTrue(bool(directive.get("active")))
        self.assertEqual(str(directive.get("mode") or ""), "family_disagreement_limit_pause")
        self.assertIn("xau_scalp_flow_short_sidecar", list(directive.get("preferred_families") or []))
        self.assertIn("xau_scalp_microtrend", list(directive.get("blocked_families") or []))
        self.assertIn("scalp_xauusd:canary", list(directive.get("blocked_sources") or []))
        self.assertTrue(any("execution directive:" in str(item or "") for item in list(xau.get("manager_findings") or [])))
        self.assertIn(str((report.get("execution_directive_apply") or {}).get("status") or ""), {"applied", "already_active"})
        feed_symbol = dict(((report.get("opportunity_feed") or {}).get("symbols") or {}).get("XAUUSD") or {})
        priority_families = list(feed_symbol.get("priority_families") or [])
        self.assertTrue(priority_families)
        self.assertEqual(priority_families[0], "xau_scalp_flow_short_sidecar")

        state = self.agent._load_state()
        state_directive = dict(state.get("xau_execution_directive") or {})
        self.assertEqual(str(state_directive.get("status") or ""), "active")
        self.assertEqual(str(state_directive.get("trigger_run_id") or ""), "run-disagree-1")
        self.assertTrue(any(str(item.get("task") or "") == "lead_confirmation_short" for item in list(state_directive.get("trader_assignments") or [])))

    def test_trading_manager_applies_live_xau_regime_transition_before_cluster_losses(self):
        with sqlite3.connect(str(self.ctrader_db)) as conn:
            conn.execute(
                """
                INSERT INTO ctrader_orders(
                    order_id, source, symbol, direction, order_type, entry_price, stop_loss, take_profit, first_seen_utc, is_open
                )
                VALUES(?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    1,
                    "scalp_xauusd:canary",
                    "XAUUSD",
                    "short",
                    "limit",
                    5108.2,
                    5112.4,
                    5101.0,
                    "2026-03-11T11:58:00Z",
                    1,
                ),
            )
            conn.commit()

        (self.report_dir / "mission_progress_report.json").write_text(
            json.dumps({"symbols": [{"symbol": "XAUUSD", "selected_family": "xau_scalp_microtrend", "selected_regime": "scalp_priority"}]}),
            encoding="utf-8",
        )
        (self.report_dir / "family_calibration_report.json").write_text(json.dumps({"families": []}), encoding="utf-8")
        (self.report_dir / "winner_memory_library_report.json").write_text(json.dumps({"top_by_symbol": {}}), encoding="utf-8")
        (self.report_dir / "chart_state_memory_report.json").write_text(
            json.dumps(
                {
                    "states": [
                        {
                            "symbol": "XAUUSD",
                            "state_label": "continuation_drive",
                            "direction": "short",
                            "day_type": "trend",
                            "follow_up_candidate": True,
                            "state_score": 38.0,
                        }
                    ]
                }
            ),
            encoding="utf-8",
        )
        (self.report_dir / "ct_only_experiment_report.json").write_text(json.dumps({"sources": []}), encoding="utf-8")

        snapshot = {
            "ok": True,
            "status": "ok",
            "symbol": "XAUUSD",
            "run_id": "capture-range-1",
            "last_event_utc": "2026-03-11T11:59:20Z",
            "features": {
                "day_type": "trend",
                "rejection_ratio": 0.46,
                "bar_volume_proxy": 0.31,
                "delta_proxy": 0.01,
                "depth_imbalance": 0.0,
                "depth_refill_shift": 0.03,
                "spread_expansion": 1.03,
            },
        }

        with ExitStack() as stack:
            stack.enter_context(patch("learning.trading_manager_agent.config.TRADING_MANAGER_REPORT_SYMBOLS", "XAUUSD"))
            stack.enter_context(patch("learning.trading_manager_agent.config.TRADING_MANAGER_AUTO_TUNE_ENABLED", False, create=True))
            stack.enter_context(patch("learning.trading_manager_agent.config.TRADING_MANAGER_AUTO_ROUTING_ENABLED", True, create=True))
            stack.enter_context(patch("learning.trading_manager_agent.config.TRADING_MANAGER_XAU_REGIME_TRANSITION_ENABLED", True, create=True))
            stack.enter_context(patch("learning.trading_manager_agent.config.TRADING_MANAGER_XAU_REGIME_TRANSITION_LOOKBACK_SEC", 240, create=True))
            stack.enter_context(patch("learning.trading_manager_agent.config.TRADING_MANAGER_XAU_REGIME_TRANSITION_HOLD_MIN", 12, create=True))
            stack.enter_context(patch("learning.trading_manager_agent.config.TRADING_MANAGER_XAU_REGIME_TRANSITION_MIN_REJECTION_RATIO", 0.34, create=True))
            stack.enter_context(patch("learning.trading_manager_agent.config.TRADING_MANAGER_XAU_REGIME_TRANSITION_MAX_ABS_CONTINUATION_BIAS", 0.055, create=True))
            stack.enter_context(patch("learning.trading_manager_agent.config.TRADING_MANAGER_XAU_REGIME_TRANSITION_MIN_BAR_VOLUME_PROXY", 0.18, create=True))
            stack.enter_context(patch("learning.trading_manager_agent.config.TRADING_MANAGER_XAU_REGIME_TRANSITION_MIN_OPPOSITE_BIAS", 0.03, create=True))
            stack.enter_context(patch("learning.trading_manager_agent.config.TRADING_MANAGER_XAU_REGIME_TRANSITION_LIMIT_FAMILIES", "xau_scalp_microtrend,xau_scalp_tick_depth_filter", create=True))
            stack.enter_context(patch("learning.trading_manager_agent.config.TRADING_MANAGER_XAU_REGIME_TRANSITION_PREFERRED_FAMILIES", "xau_scalp_range_repair", create=True))
            stack.enter_context(patch("learning.trading_manager_agent.config.TRADING_MANAGER_XAU_REGIME_TRANSITION_PREFERRED_SOURCES", "scalp_xauusd:rr:canary", create=True))
            stack.enter_context(patch("learning.trading_manager_agent.config.TRADING_MANAGER_XAU_EXECUTION_DIRECTIVE_ENABLED", True, create=True))
            stack.enter_context(patch("learning.trading_manager_agent.config.CTRADER_XAU_SHORT_LIMIT_PAUSE_ENABLED", True, create=True))
            stack.enter_context(patch("learning.trading_manager_agent.config.CTRADER_XAU_PAIR_RISK_CAP_ENABLED", True, create=True))
            stack.enter_context(patch("learning.trading_manager_agent.config.CTRADER_XAU_PAIR_RISK_MAX_USD", 3.0, create=True))
            stack.enter_context(patch("learning.trading_manager_agent.config.CTRADER_XAU_PAIR_RISK_MIN_USD", 0.15, create=True))
            stack.enter_context(patch.object(self.agent, "_current_value", return_value=""))
            stack.enter_context(patch("learning.trading_manager_agent.live_profile_autopilot.latest_capture_feature_snapshot", return_value=snapshot))
            stack.enter_context(patch("learning.trading_manager_agent.macro_news.high_impact_headlines", return_value=[]))
            stack.enter_context(patch("learning.trading_manager_agent.macro_impact_tracker.sync", return_value={"ok": True}))
            stack.enter_context(patch("learning.trading_manager_agent.macro_impact_tracker.build_report", return_value={"entries": []}))
            stack.enter_context(patch("learning.trading_manager_agent.economic_calendar.next_events", return_value=[]))
            report = self.agent.build_report(hours=24)

        xau = next(row for row in list(report.get("symbols") or []) if row.get("symbol") == "XAUUSD")
        transition = dict(xau.get("regime_transition_recommendations") or {})
        directive = dict(xau.get("execution_directive_recommendations") or {})
        self.assertTrue(bool(transition.get("active")))
        self.assertEqual(str(transition.get("blocked_direction") or ""), "short")
        self.assertEqual(str(transition.get("state_label") or ""), "reversal_exhaustion")
        self.assertIn("xau_scalp_microtrend", list(transition.get("blocked_families") or []))
        self.assertIn("xau_scalp_range_repair", list(transition.get("preferred_families") or []))
        self.assertTrue(bool(directive.get("active")))
        self.assertEqual(str(directive.get("mode") or ""), "live_range_transition_limit_pause")
        self.assertIn("scalp_xauusd:canary", list(directive.get("blocked_sources") or []))
        self.assertIn(str((report.get("regime_transition_apply") or {}).get("status") or ""), {"applied", "already_active"})
        state = self.agent._load_state()
        self.assertEqual(str(((state.get("xau_regime_transition") or {}).get("status")) or ""), "active")
        self.assertEqual(str((((state.get("xau_regime_transition") or {}).get("preferred_families")) or [""])[0] or ""), "xau_scalp_range_repair")

    def test_trading_manager_skips_live_xau_regime_transition_when_continuation_still_confirmed(self):
        with sqlite3.connect(str(self.ctrader_db)) as conn:
            conn.execute(
                """
                INSERT INTO ctrader_orders(
                    order_id, source, symbol, direction, order_type, entry_price, stop_loss, take_profit, first_seen_utc, is_open
                )
                VALUES(?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    1,
                    "scalp_xauusd:canary",
                    "XAUUSD",
                    "short",
                    "limit",
                    5108.2,
                    5112.4,
                    5101.0,
                    "2026-03-11T11:58:00Z",
                    1,
                ),
            )
            conn.commit()

        (self.report_dir / "mission_progress_report.json").write_text(
            json.dumps({"symbols": [{"symbol": "XAUUSD", "selected_family": "xau_scalp_microtrend", "selected_regime": "scalp_priority"}]}),
            encoding="utf-8",
        )
        (self.report_dir / "family_calibration_report.json").write_text(json.dumps({"families": []}), encoding="utf-8")
        (self.report_dir / "winner_memory_library_report.json").write_text(json.dumps({"top_by_symbol": {}}), encoding="utf-8")
        (self.report_dir / "chart_state_memory_report.json").write_text(json.dumps({"states": []}), encoding="utf-8")
        (self.report_dir / "ct_only_experiment_report.json").write_text(json.dumps({"sources": []}), encoding="utf-8")

        snapshot = {
            "ok": True,
            "status": "ok",
            "symbol": "XAUUSD",
            "run_id": "capture-trend-1",
            "last_event_utc": "2026-03-11T11:59:20Z",
            "features": {
                "day_type": "trend",
                "rejection_ratio": 0.10,
                "bar_volume_proxy": 0.62,
                "delta_proxy": -0.18,
                "depth_imbalance": -0.05,
                "depth_refill_shift": -0.03,
                "spread_expansion": 1.04,
            },
        }

        with ExitStack() as stack:
            stack.enter_context(patch("learning.trading_manager_agent.config.TRADING_MANAGER_REPORT_SYMBOLS", "XAUUSD"))
            stack.enter_context(patch("learning.trading_manager_agent.config.TRADING_MANAGER_AUTO_TUNE_ENABLED", False, create=True))
            stack.enter_context(patch("learning.trading_manager_agent.config.TRADING_MANAGER_AUTO_ROUTING_ENABLED", True, create=True))
            stack.enter_context(patch("learning.trading_manager_agent.config.TRADING_MANAGER_XAU_REGIME_TRANSITION_ENABLED", True, create=True))
            stack.enter_context(patch.object(self.agent, "_current_value", return_value=""))
            stack.enter_context(patch("learning.trading_manager_agent.live_profile_autopilot.latest_capture_feature_snapshot", return_value=snapshot))
            stack.enter_context(patch("learning.trading_manager_agent.macro_news.high_impact_headlines", return_value=[]))
            stack.enter_context(patch("learning.trading_manager_agent.macro_impact_tracker.sync", return_value={"ok": True}))
            stack.enter_context(patch("learning.trading_manager_agent.macro_impact_tracker.build_report", return_value={"entries": []}))
            stack.enter_context(patch("learning.trading_manager_agent.economic_calendar.next_events", return_value=[]))
            report = self.agent.build_report(hours=24)

        xau = next(row for row in list(report.get("symbols") or []) if row.get("symbol") == "XAUUSD")
        self.assertFalse(bool((xau.get("regime_transition_recommendations") or {}).get("active")))
        self.assertFalse(bool((xau.get("execution_directive_recommendations") or {}).get("active")))

    def test_trading_manager_reason_memory_tightens_xau_canary_confidence(self):
        (self.report_dir / "mission_progress_report.json").write_text(
            json.dumps({"symbols": [{"symbol": "XAUUSD", "selected_family": "xau_scalp_pullback_limit", "selected_regime": "scalp_priority"}]}),
            encoding="utf-8",
        )
        (self.report_dir / "winner_memory_library_report.json").write_text(
            json.dumps({"top_by_symbol": {"XAUUSD": {"family": "xau_scalp_pullback_limit", "session": "new_york", "direction": "long"}}}),
            encoding="utf-8",
        )
        (self.report_dir / "family_calibration_report.json").write_text(json.dumps({"families": []}), encoding="utf-8")
        (self.report_dir / "chart_state_memory_report.json").write_text(json.dumps({"states": []}), encoding="utf-8")
        (self.report_dir / "ct_only_experiment_report.json").write_text(json.dumps({"sources": []}), encoding="utf-8")

        reason_study_report = {
            "ok": True,
            "status": "ok",
            "days": 120,
            "min_resolved": 8,
            "resolved_rows": 144,
            "tag_index": {
                "symbol:xauusd": {"tag": "symbol:xauusd", "resolved": 40, "eligible": True, "score": -0.32, "win_rate": 0.43, "avg_r": -0.19},
                "family:xau_scalp_pullback_limit": {"tag": "family:xau_scalp_pullback_limit", "resolved": 18, "eligible": True, "score": -0.44, "win_rate": 0.39, "avg_r": -0.24},
                "family:xau_scalp_breakout_stop": {"tag": "family:xau_scalp_breakout_stop", "resolved": 16, "eligible": True, "score": 0.21, "win_rate": 0.62, "avg_r": 0.11},
                "session:new_york": {"tag": "session:new_york", "resolved": 22, "eligible": True, "score": -0.17, "win_rate": 0.47, "avg_r": -0.08},
            },
        }

        with ExitStack() as stack:
            stack.enter_context(patch("learning.trading_manager_agent.config.TRADING_MANAGER_REPORT_SYMBOLS", "XAUUSD"))
            stack.enter_context(patch("learning.trading_manager_agent.config.TRADING_MANAGER_AUTO_TUNE_ENABLED", False, create=True))
            stack.enter_context(patch("learning.trading_manager_agent.config.TRADING_MANAGER_AUTO_ROUTING_ENABLED", False, create=True))
            stack.enter_context(patch("learning.trading_manager_agent.config.TRADING_MANAGER_XAU_REASON_MEMORY_ENABLED", True, create=True))
            stack.enter_context(patch("learning.trading_manager_agent.config.TRADING_MANAGER_XAU_REASON_MEMORY_AUTO_APPLY_ENABLED", True, create=True))
            stack.enter_context(patch("learning.trading_manager_agent.config.TRADING_MANAGER_XAU_REASON_MEMORY_AUTO_APPLY_PERSIST_ENV", False, create=True))
            stack.enter_context(patch("learning.trading_manager_agent.config.TRADING_MANAGER_XAU_REASON_MEMORY_MIN_ABS_SCORE", 0.08, create=True))
            stack.enter_context(patch("learning.trading_manager_agent.config.TRADING_MANAGER_XAU_REASON_MEMORY_CONFIDENCE_SCORE_MULT", 4.0, create=True))
            stack.enter_context(patch("learning.trading_manager_agent.config.TRADING_MANAGER_XAU_REASON_MEMORY_MAX_ABS_DELTA", 2.0, create=True))
            stack.enter_context(patch("learning.trading_manager_agent.config.AUTO_APPLY_XAU_CANARY_CONFIDENCE_MIN", 68.0, create=True))
            stack.enter_context(patch("learning.trading_manager_agent.config.AUTO_APPLY_XAU_CANARY_CONFIDENCE_MAX", 80.0, create=True))
            stack.enter_context(patch("learning.trading_manager_agent.config.NEURAL_GATE_CANARY_MIN_CONFIDENCE", 72.0, create=True))
            stack.enter_context(patch("learning.trading_manager_agent.config.get_persistent_canary_strategy_families", return_value={"xau_scalp_pullback_limit", "xau_scalp_breakout_stop"}))
            stack.enter_context(patch("learning.trading_manager_agent.config.get_ctrader_xau_active_families", return_value={"xau_scalp_pullback_limit", "xau_scalp_breakout_stop"}))
            stack.enter_context(patch("learning.trading_manager_agent.config.get_persistent_canary_experimental_families", return_value=set()))
            stack.enter_context(patch.object(self.agent, "_current_value", side_effect=lambda key: {"CTRADER_XAU_PRIMARY_FAMILY": "xau_scalp_pullback_limit", "CTRADER_XAU_ACTIVE_FAMILIES": "xau_scalp_pullback_limit,xau_scalp_breakout_stop", "NEURAL_GATE_CANARY_MIN_CONFIDENCE": "72"}.get(key, "")))
            runtime_apply = stack.enter_context(patch.object(self.agent, "_apply_runtime_value"))
            stack.enter_context(patch.object(self.agent, "_persist_env_value", return_value={"ok": True, "updated": False, "reason": "persist_disabled"}))
            stack.enter_context(patch("learning.trading_manager_agent.neural_brain.build_reason_study_report", return_value=reason_study_report))
            stack.enter_context(patch("learning.trading_manager_agent.macro_news.high_impact_headlines", return_value=[]))
            stack.enter_context(patch("learning.trading_manager_agent.macro_impact_tracker.sync", return_value={"ok": True}))
            stack.enter_context(patch("learning.trading_manager_agent.macro_impact_tracker.build_report", return_value={"entries": []}))
            stack.enter_context(patch("learning.trading_manager_agent.economic_calendar.next_events", return_value=[]))
            report = self.agent.build_report(hours=24)

        xau = next(row for row in list(report.get("symbols") or []) if row.get("symbol") == "XAUUSD")
        reason_memory = dict(xau.get("reason_memory_recommendations") or {})
        self.assertTrue(bool(reason_memory.get("active")))
        self.assertEqual(str(reason_memory.get("mode") or ""), "tighten_canary_from_reason_memory")
        self.assertAlmostEqual(float(reason_memory.get("proposed_canary_min_confidence", 0.0) or 0.0), 73.5, places=1)
        self.assertEqual(str((reason_memory.get("changes") or {}).get("NEURAL_GATE_CANARY_MIN_CONFIDENCE") or ""), "73.5")
        self.assertEqual(str((report.get("reason_memory_apply") or {}).get("status") or ""), "applied")
        runtime_apply.assert_called_with("NEURAL_GATE_CANARY_MIN_CONFIDENCE", "73.5")
        self.assertTrue(any("reason memory:" in str(item or "") for item in list(xau.get("manager_findings") or [])))

        state = self.agent._load_state()
        self.assertEqual(str(((state.get("xau_reason_memory") or {}).get("status")) or ""), "active")
        self.assertEqual(str((((state.get("xau_reason_memory") or {}).get("changes")) or {}).get("NEURAL_GATE_CANARY_MIN_CONFIDENCE") or ""), "73.5")


if __name__ == "__main__":
    unittest.main()
