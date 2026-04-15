"""
Boot Sequence — Inspired by Claude Code's multi-phase boot pipeline.

Three nested layers:
  Layer 1: Environment prep, config loading, exchange validation
  Layer 2: Agent initialization, tool registration, state setup
  Layer 3: Task scheduler start, first market scan trigger
"""

import asyncio
import logging
import time
from dataclasses import dataclass, field
from typing import Optional

from core.state import GlobalState, get_state
from core.task import TaskManager

logger = logging.getLogger("mempalace2.boot")


@dataclass
class BootMetrics:
    """Track boot timing for each phase."""
    start_time: float = 0.0
    config_loaded_at: float = 0.0
    agents_initialized_at: float = 0.0
    tools_registered_at: float = 0.0
    first_scan_at: float = 0.0
    fully_booted_at: float = 0.0

    @property
    def total_boot_ms(self) -> float:
        if self.fully_booted_at and self.start_time:
            return (self.fully_booted_at - self.start_time) * 1000
        return 0.0


class BootPipeline:
    """
    Multi-phase boot sequence:
      Phase 1: Environment & Config
      Phase 2: Agent & Tool Init
      Phase 3: Task System & First Scan
    """

    def __init__(self, config_path: Optional[str] = None):
        self.config_path = config_path
        self.metrics = BootMetrics()
        self.state: Optional[GlobalState] = None

    async def boot(self) -> GlobalState:
        """Execute the full boot pipeline."""
        self.metrics.start_time = time.monotonic()
        logger.info("=" * 60)
        logger.info("  MEMPALACE2 AI — BOOT SEQUENCE")
        logger.info("=" * 60)

        # Phase 1: Environment & Config
        await self._phase_environment()
        self.metrics.config_loaded_at = time.monotonic()
        logger.info(f"  ✓ Phase 1: Config loaded ({self._phase_time(1):.0f}ms)")

        # Phase 2: Agents & Tools
        await self._phase_agents_tools()
        self.metrics.agents_initialized_at = time.monotonic()
        logger.info(f"  ✓ Phase 2: Agents & Tools ready ({self._phase_time(2):.0f}ms)")

        # Phase 3: Tasks & First Scan
        await self._phase_tasks()
        self.metrics.fully_booted_at = time.monotonic()
        logger.info(f"  ✓ Phase 3: Task system active ({self._phase_time(3):.0f}ms)")

        logger.info("=" * 60)
        logger.info(f"  BOOT COMPLETE — {self.metrics.total_boot_ms:.0f}ms")
        logger.info("=" * 60)

        return self.state

    async def _phase_environment(self):
        """Phase 1: Load config, validate exchanges, init state."""
        from config.settings import load_config
        config = load_config(self.config_path)

        self.state = GlobalState(config=config)
        self.state.boot_metrics = self.metrics
        get_state._instance = self.state

        logger.info(f"  Config: {config.exchanges.primary} @ {config.exchanges.sandbox and 'sandbox' or 'live'}")
        logger.info(f"  Risk: max_position={config.risk.max_position_pct}%, max_portfolio={config.risk.max_portfolio_risk_pct}%")

    async def _phase_agents_tools(self):
        """Phase 2: Initialize all agents and register tools."""
        from agents.coordinator import CoordinatorAgent
        from tools.registry import register_all_tools

        # Register tools first (agents depend on them)
        register_all_tools(self.state)

        # Initialize coordinator (creates sub-agents)
        coordinator = CoordinatorAgent(self.state)
        await coordinator.initialize()
        self.state.coordinator = coordinator

        logger.info(f"  Agents: {len(self.state.agents)} active")
        logger.info(f"  Tools:  {len(self.state.tools)} registered")

    async def _phase_tasks(self):
        """Phase 3: Start task manager and trigger first market scan."""
        self.state.task_manager = TaskManager(self.state)
        await self.state.task_manager.start()
        logger.info("  Task manager started")

    def _phase_time(self, phase: int) -> float:
        """Calculate elapsed time for a specific phase in ms."""
        times = [
            self.metrics.start_time,
            self.metrics.config_loaded_at,
            self.metrics.agents_initialized_at,
            self.metrics.tools_registered_at,
            self.metrics.fully_booted_at,
        ]
        if phase < len(times) and times[phase] and times[phase - 1]:
            return (times[phase] - times[phase - 1]) * 1000
        return 0.0
