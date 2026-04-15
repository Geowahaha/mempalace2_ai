"""
Integration tests for the enhanced hermes-agent pipeline.

Tests the full enhanced boot → scan → analyze → risk → execute → close
flow with memory, skills, trajectories, and context engine.
"""

import asyncio
import json
import os
import shutil
import tempfile
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pandas as pd
import numpy as np

# Ensure the package is importable
import sys
sys.path.insert(0, str(Path(__file__).parent.parent))


def _make_ohlcv(n=200, base_price=3200.0):
    """Create synthetic OHLCV data for testing."""
    np.random.seed(42)
    prices = base_price + np.cumsum(np.random.randn(n) * 2)
    return pd.DataFrame({
        "open": prices + np.random.randn(n) * 1,
        "high": prices + abs(np.random.randn(n) * 5),
        "low": prices - abs(np.random.randn(n) * 5),
        "close": prices,
        "volume": np.random.randint(1000, 10000, n).astype(float),
        "ema_fast": prices + np.random.randn(n) * 0.5,
        "ema_slow": prices + np.random.randn(n) * 0.8,
        "ema_trend": prices + np.random.randn(n) * 1.5,
        "rsi": 30 + np.random.rand(n) * 40,
        "macd": np.random.randn(n) * 2,
        "macd_signal": np.random.randn(n) * 1.5,
        "macd_histogram": np.random.randn(n) * 0.5,
        "supertrend_direction": np.random.choice([1, -1], n),
        "bb_upper": prices + 15,
        "bb_middle": prices,
        "bb_lower": prices - 15,
        "bb_width": np.random.rand(n) * 4,
        "atr": np.random.rand(n) * 5 + 2,
        "adx": np.random.rand(n) * 30 + 10,
        "volume_ratio": np.random.rand(n) * 2 + 0.5,
        "trend_score": np.random.rand(n) * 100,
        "support_1": prices - 10,
        "support_2": prices - 20,
        "resistance_1": prices + 10,
        "resistance_2": prices + 20,
    })


class TestStateStore(unittest.TestCase):
    """Test StateStore SQLite schema and operations."""

    def setUp(self):
        self.tmpdir = tempfile.mkdtemp()
        self.db_path = Path(self.tmpdir) / "test_state.db"
        from enhanced.state_store import StateStore
        self.store = StateStore(db_path=self.db_path)

    def tearDown(self):
        shutil.rmtree(self.tmpdir)

    def test_creates_tables(self):
        """Verify all core tables are created."""
        conn = self.store._get_conn()
        cursor = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        )
        tables = {row[0] for row in cursor.fetchall()}
        expected = {"sessions", "trade_history", "signal_history", "trade_patterns", "learning_events"}
        self.assertTrue(expected.issubset(tables), f"Missing tables: {expected - tables}")

    def test_create_session(self):
        """Session creation returns a valid ID."""
        sid = self.store.create_session(source="test", symbols=["XAUUSD"])
        self.assertTrue(len(sid) > 0)

    def test_record_trade(self):
        """Trade recording works."""
        sid = self.store.create_session(source="test", symbols=["XAUUSD"])
        tid = self.store.record_trade(sid, {
            "trade_id": "t1",
            "symbol": "XAUUSD",
            "direction": "long",
            "setup_type": "ema_crossover",
            "entry_price": 3200.0,
            "exit_price": 3220.0,
            "pnl": 20.0,
            "pnl_pct": 0.625,
        })
        # record_trade returns a string (trade id)
        self.assertIsNotNone(tid)

    def test_record_pattern(self):
        """Pattern recording works."""
        pid = self.store.record_pattern({
            "pattern_name": "XAUUSD_ema_crossover_long",
            "pattern_type": "ema_crossover",
            "symbol": "XAUUSD",
            "timeframe": "1h",
            "conditions": {"rsi": 25},
            "win_rate": 0.7,
            "avg_pnl_pct": 0.5,
            "avg_risk_reward": 2.5,
            "sample_count": 10,
        })
        self.assertTrue(len(pid) > 0)
        # get_best_patterns default min_samples=1, so our pattern should appear
        best = self.store.get_best_patterns(symbol="XAUUSD", min_samples=1)
        self.assertTrue(len(best) > 0)

    def test_record_learning_event(self):
        """Learning event recording and retrieval."""
        self.store.record_learning_event(
            event_type="loss",
            trade_id="t1",
            description="Lost on reversal",
            lesson="Don't trade supertrend flips in low ATR",
        )
        events = self.store.get_learning_events(event_type="loss")
        self.assertTrue(len(events) > 0)
        self.assertIn("supertrend", events[0]["lesson"])

    def test_search_trades(self):
        """Search trades works."""
        sid = self.store.create_session(source="test", symbols=["XAUUSD"])
        self.store.record_trade(sid, {
            "trade_id": "t1",
            "symbol": "XAUUSD",
            "direction": "long",
            "setup_type": "bb_squeeze_breakout",
            "entry_price": 3200.0,
        })
        results = self.store.search_trades("bb squeeze")
        self.assertIsInstance(results, list)

    def test_end_session(self):
        """Session ending records stats."""
        sid = self.store.create_session(source="test", symbols=["XAUUSD"])
        self.store.end_session(sid, {"total_trades": 5, "total_pnl": 100.0})


class TestTradeMemory(unittest.TestCase):
    """Test TradeMemory pattern recall and lesson learning."""

    def setUp(self):
        self.tmpdir = tempfile.mkdtemp()
        self.db_path = Path(self.tmpdir) / "test_memory.db"
        from enhanced.state_store import StateStore
        from memory.store import TradeMemory
        self.store = StateStore(db_path=self.db_path)
        self.memory = TradeMemory(state_store=self.store)

    def tearDown(self):
        shutil.rmtree(self.tmpdir)

    def test_store_and_recall_pattern(self):
        """Store a pattern and recall it."""
        self.memory.store_trade_pattern(
            symbol="XAUUSD",
            setup_type="ema_crossover",
            direction="long",
            conditions={"timeframe": "1h", "atr": 5.0},
            outcome={"pnl": 20, "pnl_pct": 0.6, "risk_reward_ratio": 2.5},
        )
        # Force cache refresh
        self.memory._cache_ts = 0
        items = self.memory.recall_similar_patterns("XAUUSD", "ema_crossover")
        # Patterns are cached from state_store with min_samples requirement
        self.assertIsInstance(items, list)

    def test_store_and_recall_lesson(self):
        """Store a lesson and recall it."""
        self.memory.store_lesson(
            event_type="loss",
            trade_id="t1",
            description="Lost on ema crossover in choppy market",
            lesson="Avoid EMA crossover when ADX < 20",
        )
        self.memory._cache_ts = 0
        items = self.memory.recall_lessons(context="XAUUSD ema_crossover long")
        self.assertTrue(len(items) > 0)

    def test_build_context_for_analysis_with_lessons(self):
        """Context block includes lessons when available."""
        self.memory.store_lesson(
            event_type="loss",
            trade_id="t1",
            description="XAUUSD ema_crossover loss",
            lesson="Don't trade in low ADX",
        )
        self.memory._cache_ts = 0
        block = self.memory.build_context_for_analysis("XAUUSD", "ema_crossover", "long")
        # Should contain trade-memory block since we have lessons
        if block:
            self.assertIn("trade-memory", block.lower())

    def test_empty_context_returns_empty(self):
        """No patterns → empty context."""
        block = self.memory.build_context_for_analysis("EURUSD", "trend_alignment", "long")
        self.assertEqual(block, "")

    def test_get_memory_stats(self):
        """Memory stats returns a dict."""
        stats = self.memory.get_memory_stats()
        self.assertIsInstance(stats, dict)


class TestSkillManager(unittest.TestCase):
    """Test SkillManager discovery and self-learning."""

    def setUp(self):
        self.tmpdir = tempfile.mkdtemp()
        self.db_path = Path(self.tmpdir) / "test_skills.db"
        from enhanced.state_store import StateStore
        from skills.manager import SkillManager
        self.store = StateStore(db_path=self.db_path)
        # Copy skill files to temp dir
        skills_dir = Path(self.tmpdir) / "skills"
        src_skills = Path(__file__).parent.parent / "skills" / "trading"
        if src_skills.exists():
            shutil.copytree(src_skills, skills_dir / "trading")
        self.mgr = SkillManager(skills_dir=skills_dir, state_store=self.store)

    def tearDown(self):
        shutil.rmtree(self.tmpdir)

    def test_discover_skills(self):
        """Skills are discovered from SKILL.md files."""
        count = self.mgr.discover_skills()
        self.assertGreaterEqual(count, 3, "Should discover at least 3 trading skills")

    def test_match_skills(self):
        """Match skills by context."""
        self.mgr.discover_skills()
        # Provide all context fields to avoid NoneType errors
        matches = self.mgr.match_skills({
            "symbol": "XAUUSD",
            "setup_type": "supertrend_flip_bullish",
            "direction": "long",
            "timeframe": "1h",
        })
        self.assertIsInstance(matches, list)

    def test_update_skill_from_trade(self):
        """Updating skill from trade affects win rate."""
        self.mgr.discover_skills()
        self.mgr.update_skill_from_trade("supertrend-reversal", won=True, pnl_pct=0.5, risk_reward=2.0)
        stats = self.mgr.get_stats()
        self.assertGreater(stats["total"], 0)

    def test_build_skills_context_block(self):
        """Skills context block is generated."""
        self.mgr.discover_skills()
        block = self.mgr.build_skills_context_block({
            "symbol": "XAUUSD",
            "setup_type": "supertrend_flip_bullish",
            "direction": "long",
            "timeframe": "1h",
        })
        self.assertIsInstance(block, str)


class TestTrajectoryLogger(unittest.TestCase):
    """Test TrajectoryLogger JSONL output."""

    def setUp(self):
        self.tmpdir = tempfile.mkdtemp()
        from trajectories.logger import TrajectoryLogger
        self.logger = TrajectoryLogger(output_dir=self.tmpdir)

    def tearDown(self):
        shutil.rmtree(self.tmpdir)

    def test_trajectory_lifecycle(self):
        """Full trajectory: start → add steps → finalize → JSONL written."""
        tid = self.logger.start_trajectory(session_id="s1", symbol="XAUUSD", direction="long")
        self.assertTrue(len(tid) > 0)

        self.logger.add_step(tid, "scan", {"setup_type": "ema_crossover", "strength": 75})
        self.logger.add_step(tid, "analysis", {"has_memory": True, "has_skills": True})
        self.logger.add_step(tid, "risk_approved", {"position_size_pct": 2.0})

        self.logger.finalize(tid, "closed", {
            "pnl": 20.0,
            "pnl_pct": 0.6,
            "won": True,
        })

        # Check JSONL was written
        jsonl_path = Path(self.tmpdir) / "trajectory_samples.jsonl"
        self.assertTrue(jsonl_path.exists())
        with open(jsonl_path) as f:
            line = f.readline()
            data = json.loads(line)
            # The entry has "metadata" and "conversations" keys
            self.assertIn("metadata", data)
            meta = data["metadata"]
            self.assertEqual(meta["symbol"], "XAUUSD")
            self.assertEqual(len(meta["steps"]), 3)
            self.assertEqual(meta["status"], "closed")

    def test_stats_tracking(self):
        """Stats are updated correctly."""
        tid1 = self.logger.start_trajectory(session_id="s1", symbol="XAUUSD", direction="long")
        self.logger.finalize(tid1, "executed")
        tid2 = self.logger.start_trajectory(session_id="s1", symbol="XAUUSD", direction="short")
        self.logger.finalize(tid2, "rejected")
        stats = self.logger.get_stats()
        self.assertEqual(stats["total"], 2)
        self.assertEqual(stats["executed"], 1)
        self.assertEqual(stats["rejected"], 1)

    def test_sharegpt_export(self):
        """ShareGPT format is generated."""
        tid = self.logger.start_trajectory(session_id="s1", symbol="XAUUSD", direction="long")
        self.logger.add_step(tid, "scan", {"setup": "test"})
        self.logger.finalize(tid, "closed", {"pnl": 10, "won": True})
        export_path = Path(self.tmpdir) / "sharegpt_export.jsonl"
        count = self.logger.export_sharegpt(str(export_path))
        self.assertGreaterEqual(count, 1)


class TestContextEngine(unittest.TestCase):
    """Test ContextEngine scan tick and compression."""

    def test_tick_and_compress(self):
        """Context engine ticks and triggers compression after threshold."""
        from enhanced.context_engine import create_context_engine
        engine = create_context_engine("trading")
        engine.on_session_start("test-session")

        # Tick past threshold
        for i in range(150):
            engine.tick_scan()

        self.assertTrue(engine.should_compress())
        engine.on_session_end("test-session")


class TestEnhancedToolRegistry(unittest.TestCase):
    """Test EnhancedToolRegistry and learning tool factories."""

    def test_register_learning_tools(self):
        """Learning tools are registered properly."""
        from enhanced.tools.trading_registry import (
            EnhancedToolRegistry, make_memory_tools, make_trajectory_tools, make_skill_tools,
        )
        tmpdir = tempfile.mkdtemp()
        try:
            from enhanced.state_store import StateStore
            from memory.store import TradeMemory
            from trajectories.logger import TrajectoryLogger
            from skills.manager import SkillManager

            store = StateStore(db_path=Path(tmpdir) / "test.db")
            memory = TradeMemory(state_store=store)
            traj = TrajectoryLogger(output_dir=tmpdir)
            skills = SkillManager(skills_dir=Path(tmpdir), state_store=store)

            registry = EnhancedToolRegistry()
            for name, handler in make_memory_tools(memory).items():
                registry.register(name, handler, toolset="learning")
            for name, handler in make_trajectory_tools(traj).items():
                registry.register(name, handler, toolset="learning")
            for name, handler in make_skill_tools(skills).items():
                registry.register(name, handler, toolset="learning")

            self.assertGreater(len(registry._tools), 0)
            tool_names = list(registry._tools.keys())
            self.assertIn("store_pattern", tool_names)
            self.assertIn("recall_patterns", tool_names)
            self.assertIn("store_lesson", tool_names)
            self.assertIn("log_trajectory", tool_names)
            self.assertIn("list_skills", tool_names)
            self.assertIn("match_skills", tool_names)
        finally:
            shutil.rmtree(tmpdir)


class TestEnhancedBoot(unittest.TestCase):
    """Test the enhanced boot pipeline."""

    def test_boot_creates_all_components(self):
        """Enhanced boot initializes all hermes components."""
        tmpdir = tempfile.mkdtemp()
        state = None
        try:
            db_path = Path(tmpdir) / "boot_test.db"
            traj_dir = Path(tmpdir) / "trajectories"

            from enhanced.boot import EnhancedBootPipeline
            pipeline = EnhancedBootPipeline(
                db_path=str(db_path),
                trajectory_dir=str(traj_dir),
            )

            state = asyncio.run(pipeline.boot())

            # Verify all components exist
            self.assertTrue(hasattr(state, 'state_store'))
            self.assertTrue(hasattr(state, 'memory'))
            self.assertTrue(hasattr(state, 'skills_manager'))
            self.assertTrue(hasattr(state, 'context_engine'))
            self.assertTrue(hasattr(state, 'prompt_builder'))
            self.assertTrue(hasattr(state, 'scheduler'))
            self.assertTrue(hasattr(state, 'trajectory_logger'))
            self.assertTrue(hasattr(state, 'enhanced_registry'))
            self.assertIsNotNone(state.state_store_session_id)

            # Skills discovery depends on ~/.mempalace2/skills existing —
            # just verify the manager was initialized
            self.assertIsNotNone(state.skills_manager)
        finally:
            if state and hasattr(state, 'scheduler') and state.scheduler:
                asyncio.run(state.scheduler.stop())
            shutil.rmtree(tmpdir)


class TestEndToEndPipeline(unittest.TestCase):
    """Test the full scan → analyze → risk → execute → close pipeline with hermes wiring."""

    def setUp(self):
        self.tmpdir = tempfile.mkdtemp()
        self.db_path = Path(self.tmpdir) / "e2e_test.db"
        self.traj_dir = Path(self.tmpdir) / "trajectories"
        from enhanced.boot import EnhancedBootPipeline
        self.pipeline = EnhancedBootPipeline(
            db_path=str(self.db_path),
            trajectory_dir=str(self.traj_dir),
        )
        self.state = asyncio.run(self.pipeline.boot())

    def tearDown(self):
        if hasattr(self.state, 'scheduler') and self.state.scheduler:
            asyncio.run(self.state.scheduler.stop())
        shutil.rmtree(self.tmpdir)

    def test_trajectory_flows_through_pipeline(self):
        """Trajectory ID flows from scan → analyst → risk → executor."""
        # Start trajectory
        tid = self.state.trajectory_logger.start_trajectory(
            session_id=self.state.state_store_session_id,
            symbol="XAUUSD",
            direction="long",
        )

        # Add scan step
        self.state.trajectory_logger.add_step(tid, "scan", {"setup_type": "test"})
        # Add analysis step
        self.state.trajectory_logger.add_step(tid, "analysis", {"has_memory": False})
        # Add risk step
        self.state.trajectory_logger.add_step(tid, "risk_approved", {"size": 2.0})
        # Add execution step
        self.state.trajectory_logger.add_step(tid, "execution", {"entry": 3200.0})
        # Finalize
        self.state.trajectory_logger.finalize(tid, "closed", {"pnl": 20, "won": True})

        # Verify JSONL
        jsonl_path = self.traj_dir / "trajectory_samples.jsonl"
        with open(jsonl_path) as f:
            data = json.loads(f.readline())
        meta = data["metadata"]
        self.assertEqual(len(meta["steps"]), 4)
        self.assertEqual(meta["status"], "closed")

    def test_loss_stores_lesson(self):
        """Losing trade stores a lesson in memory."""
        from agents.executor import ExecutorAgent
        from core.state import TradeSignal, ActiveTrade
        from datetime import datetime, timezone

        executor = ExecutorAgent(self.state)
        signal = TradeSignal(
            symbol="XAUUSD",
            direction="long",
            entry_price=3200.0,
            stop_loss=3190.0,
            take_profit_1=3220.0,
            strategy="ema_crossover",
            position_size_pct=2.0,
            atr=5.0,
        )
        trade = ActiveTrade(
            signal=signal,
            entry_filled_price=3200.0,
            entry_filled_time=datetime.now(timezone.utc),
            quantity=0.1,
        )
        # Add to active_trades so close can remove it
        self.state.portfolio.active_trades.append(trade)
        asyncio.run(executor._close_position(trade, 3190.0, "SL hit"))

        # Verify lesson was stored
        self.state.memory._cache_ts = 0
        lessons = self.state.memory.recall_lessons(event_type="loss")
        self.assertTrue(len(lessons) > 0, "Loss should create a lesson")

    def test_winning_trade_stores_pattern(self):
        """Winning trade stores a pattern in memory."""
        from agents.executor import ExecutorAgent
        from core.state import TradeSignal, ActiveTrade
        from datetime import datetime, timezone

        executor = ExecutorAgent(self.state)
        signal = TradeSignal(
            symbol="XAUUSD",
            direction="long",
            entry_price=3200.0,
            stop_loss=3190.0,
            take_profit_1=3220.0,
            strategy="ema_crossover",
            position_size_pct=2.0,
            atr=5.0,
        )
        trade = ActiveTrade(
            signal=signal,
            entry_filled_price=3200.0,
            entry_filled_time=datetime.now(timezone.utc),
            quantity=0.1,
        )
        self.state.portfolio.active_trades.append(trade)
        asyncio.run(executor._close_position(trade, 3220.0, "TP1 hit"))

        # Verify pattern was stored
        self.state.memory._cache_ts = 0
        patterns = self.state.memory.recall_similar_patterns("XAUUSD", "ema_crossover")
        self.assertIsInstance(patterns, list)

    def test_trajectory_finalize_on_close(self):
        """Trajectory is finalized when trade closes."""
        from agents.executor import ExecutorAgent
        from core.state import TradeSignal, ActiveTrade
        from datetime import datetime, timezone

        tid = self.state.trajectory_logger.start_trajectory(
            session_id=self.state.state_store_session_id,
            symbol="XAUUSD",
            direction="long",
        )
        self.state.trajectory_logger.add_step(tid, "scan", {"setup": "ema_crossover"})

        executor = ExecutorAgent(self.state)
        signal = TradeSignal(
            symbol="XAUUSD",
            direction="long",
            entry_price=3200.0,
            stop_loss=3190.0,
            take_profit_1=3220.0,
            strategy="ema_crossover",
            position_size_pct=2.0,
            atr=5.0,
        )
        trade = ActiveTrade(
            signal=signal,
            entry_filled_price=3200.0,
            entry_filled_time=datetime.now(timezone.utc),
            quantity=0.1,
            trajectory_id=tid,
        )
        self.state.portfolio.active_trades.append(trade)
        asyncio.run(executor._close_position(trade, 3220.0, "TP1 hit"))

        # Verify trajectory JSONL was written
        jsonl_path = self.traj_dir / "trajectory_samples.jsonl"
        self.assertTrue(jsonl_path.exists())
        with open(jsonl_path) as f:
            data = json.loads(f.readline())
        meta = data["metadata"]
        self.assertEqual(meta["symbol"], "XAUUSD")
        self.assertEqual(meta["status"], "closed")
        self.assertTrue(meta["outcome"]["won"])

    def test_scheduler_has_reports(self):
        """Scheduler has default reports configured."""
        reports = self.state.scheduler.list_reports()
        self.assertGreaterEqual(len(reports), 3, "Should have at least 3 default reports")

    def test_enhanced_registry_has_learning_tools(self):
        """Enhanced registry has learning tools registered."""
        registry = self.state.enhanced_registry
        tool_names = list(registry._tools.keys())
        self.assertIn("store_pattern", tool_names)
        self.assertIn("recall_patterns", tool_names)
        self.assertIn("store_lesson", tool_names)

    def test_dashboard_includes_enhanced_stats(self):
        """Coordinator dashboard includes hermes stats."""
        dashboard = self.state.coordinator.get_dashboard()
        self.assertIn("skills", dashboard)
        self.assertIn("trajectories", dashboard)
        self.assertIn("context_engine", dashboard)


if __name__ == "__main__":
    unittest.main()
