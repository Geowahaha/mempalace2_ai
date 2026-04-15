"""
Enhanced Boot Pipeline — extends core/boot.py with hermes-agent components.

Three-phase boot with hermes integration:
  Phase 1: Config + State Store + Memory init
  Phase 2: Agents + Skills + Context Engine + Prompt Builder
  Phase 3: Scheduler + Trajectory Logger + First scan

Extends the original BootPipeline without breaking it.
"""

import asyncio
import logging
import time
from pathlib import Path
from typing import Optional

from core.boot import BootPipeline, BootMetrics
from core.state import GlobalState, get_state

logger = logging.getLogger("mempalace2.enhanced.boot")


class EnhancedBootMetrics(BootMetrics):
    """Extended boot metrics with hermes component timings."""
    state_store_loaded_at: float = 0.0
    memory_loaded_at: float = 0.0
    skills_loaded_at: float = 0.0
    context_engine_loaded_at: float = 0.0
    scheduler_started_at: float = 0.0
    trajectory_logger_loaded_at: float = 0.0


class EnhancedBootPipeline(BootPipeline):
    """
    Boot pipeline with hermes-agent integration.

    Adds hermes components to the existing boot sequence:
      - StateStore (SQLite + FTS5)
      - TradeMemory (pattern recall + lesson learning)
      - SkillManager (self-improving skills)
      - ContextEngine (token budget management)
      - PromptBuilder (dynamic system prompts)
      - Scheduler (periodic reports)
      - TrajectoryLogger (decision logging)
    """

    def __init__(self, config_path: Optional[str] = None,
                 db_path: Optional[str] = None,
                 trajectory_dir: Optional[str] = None):
        super().__init__(config_path)
        self.metrics = EnhancedBootMetrics()
        self._db_path = Path(db_path) if db_path else None
        self._trajectory_dir = trajectory_dir or "trajectories/"

    async def boot(self) -> GlobalState:
        """Execute the enhanced boot pipeline."""
        self.metrics.start_time = time.monotonic()
        logger.info("=" * 60)
        logger.info("  MEMPALACE2 AI — ENHANCED BOOT (hermes-agent)")
        logger.info("=" * 60)

        # Phase 1: Environment, Config, State Store
        await self._phase_environment_enhanced()
        logger.info(f"  ✓ Phase 1: Config + State Store ({self._phase_time_ms(1):.0f}ms)")

        # Phase 2: Agents, Memory, Skills, Context Engine
        await self._phase_agents_enhanced()
        logger.info(f"  ✓ Phase 2: Agents + Memory + Skills ({self._phase_time_ms(2):.0f}ms)")

        # Phase 3: Scheduler, Trajectory Logger, First Scan
        await self._phase_tasks_enhanced()
        logger.info(f"  ✓ Phase 3: Scheduler + Trajectories ({self._phase_time_ms(3):.0f}ms)")

        logger.info("=" * 60)
        logger.info(f"  ENHANCED BOOT COMPLETE — {self.metrics.total_boot_ms:.0f}ms")
        self._log_component_status()
        logger.info("=" * 60)

        return self.state

    async def _phase_environment_enhanced(self):
        """Phase 1: Config + State Store initialization."""
        from config.settings import load_config
        config = load_config(self.config_path)

        self.state = GlobalState(config=config)
        self.state.boot_metrics = self.metrics
        get_state._instance = self.state

        # Initialize StateStore (hermes integration)
        from enhanced.state_store import StateStore
        self.state.state_store = StateStore(db_path=self._db_path)
        self.metrics.state_store_loaded_at = time.monotonic()

        # Create session in state store
        session_id = self.state.state_store.create_session(
            source="enhanced_boot",
            symbols=config.symbols,
            config={"exchange": config.exchanges.primary, "sandbox": config.exchanges.sandbox},
        )
        self.state.state_store_session_id = session_id

        logger.info(f"  StateStore: {self.state.state_store.db_path}")
        logger.info(f"  Session: {session_id}")

    async def _phase_agents_enhanced(self):
        """Phase 2: Agents + Memory + Skills + Context Engine + Prompt Builder."""
        from agents.coordinator import CoordinatorAgent
        from tools.registry import register_all_tools

        # Register tools (original)
        register_all_tools(self.state)

        # Initialize coordinator
        coordinator = CoordinatorAgent(self.state)
        await coordinator.initialize()
        self.state.coordinator = coordinator

        # ── Memory System ──
        from memory.store import TradeMemory
        self.state.memory = TradeMemory(state_store=self.state.state_store)
        self.metrics.memory_loaded_at = time.monotonic()

        # ── Skills Manager ──
        from skills.manager import SkillManager
        skills_dir = Path.home() / ".mempalace2" / "skills"
        self.state.skills_manager = SkillManager(
            skills_dir=skills_dir,
            state_store=self.state.state_store,
        )
        count = self.state.skills_manager.discover_skills()
        self.metrics.skills_loaded_at = time.monotonic()
        logger.info(f"  Skills: {count} discovered")

        # ── Context Engine ──
        from enhanced.context_engine import create_context_engine
        self.state.context_engine = create_context_engine("trading")
        self.state.context_engine.on_session_start(self.state.session_id)
        self.metrics.context_engine_loaded_at = time.monotonic()

        # ── Prompt Builder ──
        from enhanced.prompt_builder import PromptBuilder
        self.state.prompt_builder = PromptBuilder(
            memory=self.state.memory,
            skills_manager=self.state.skills_manager,
            state_store=self.state.state_store,
        )

        # ── Trajectory Logger ──
        from trajectories.logger import TrajectoryLogger
        self.state.trajectory_logger = TrajectoryLogger(
            output_dir=self._trajectory_dir,
        )
        self.metrics.trajectory_logger_loaded_at = time.monotonic()

        # ── Enhanced Tool Registry with learning tools ──
        from enhanced.tools.trading_registry import (
            EnhancedToolRegistry, make_memory_tools, make_trajectory_tools, make_skill_tools,
        )

        enhanced_registry = EnhancedToolRegistry()
        # Register learning tools from memory, trajectories, and skills
        for name, handler in make_memory_tools(self.state.memory).items():
            enhanced_registry.register(name, handler, toolset="learning")
        for name, handler in make_trajectory_tools(self.state.trajectory_logger).items():
            enhanced_registry.register(name, handler, toolset="learning")
        for name, handler in make_skill_tools(self.state.skills_manager).items():
            enhanced_registry.register(name, handler, toolset="learning")
        self.state.enhanced_registry = enhanced_registry

        logger.info(f"  Agents: {len(self.state.agents)} active")
        logger.info(f"  Tools:  {len(self.state.tools)} registered")
        logger.info(f"  Enhanced tools: {len(enhanced_registry._tools)} registered")

    async def _phase_tasks_enhanced(self):
        """Phase 3: Scheduler + Task System + First Scan."""
        from core.task import TaskManager
        from scheduler.reporter import Scheduler

        # Start task manager (original)
        self.state.task_manager = TaskManager(self.state)
        await self.state.task_manager.start()

        # Start scheduler (hermes integration)
        self.state.scheduler = Scheduler(
            state_store=self.state.state_store,
            memory=self.state.memory,
            skills_manager=self.state.skills_manager,
        )
        self.state.scheduler.setup_defaults()
        await self.state.scheduler.start()
        self.metrics.scheduler_started_at = time.monotonic()

        logger.info("  Task manager started")
        logger.info("  Scheduler started with 5 default reports")

    def _phase_time_ms(self, phase: int) -> float:
        """Calculate elapsed time for a phase in ms."""
        times = [
            self.metrics.start_time,
            self.metrics.state_store_loaded_at or self.metrics.config_loaded_at,
            self.metrics.agents_initialized_at or self.metrics.skills_loaded_at,
            self.metrics.fully_booted_at or self.metrics.scheduler_started_at,
        ]
        if phase < len(times) and times[phase] and times[phase - 1]:
            return (times[phase] - times[phase - 1]) * 1000
        return 0.0

    def _log_component_status(self):
        """Log status of all hermes components."""
        components = [
            ("StateStore", getattr(self.state, 'state_store', None)),
            ("Memory", getattr(self.state, 'memory', None)),
            ("SkillsManager", getattr(self.state, 'skills_manager', None)),
            ("ContextEngine", getattr(self.state, 'context_engine', None)),
            ("PromptBuilder", getattr(self.state, 'prompt_builder', None)),
            ("Scheduler", getattr(self.state, 'scheduler', None)),
            ("TrajectoryLogger", getattr(self.state, 'trajectory_logger', None)),
        ]
        for name, obj in components:
            status = "✓ loaded" if obj else "✗ not loaded"
            logger.info(f"  {name}: {status}")
