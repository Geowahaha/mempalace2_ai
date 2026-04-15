"""
Coordinator Agent — Orchestrates all sub-agents and tools.

Inspired by Claude Code's Coordinator Mode:
  - Routes messages between agents
  - Manages the analysis pipeline
  - Provides dashboard/status
  - Handles emergency stops
"""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone
from typing import Dict, List, Optional

from agents.base import BaseAgent, AgentMessage
from agents.market_scanner import MarketScannerAgent
from agents.analyst import AnalystAgent
from agents.risk_manager import RiskManagerAgent
from agents.executor import ExecutorAgent
from agents.delegate import DelegateAgent

logger = logging.getLogger("mempalace2.agents.coordinator")


class CoordinatorAgent(BaseAgent):
    """
    Top-level orchestrator. Owns all sub-agents.

    Pipeline flow:
      Scanner → Analyst → RiskManager → Executor

    The Coordinator:
      - Initializes all sub-agents
      - Routes messages between them
      - Provides unified status dashboard
      - Handles emergency shutdown
    """

    name = "coordinator"
    role = "Orchestrate analysis pipeline and manage agents"

    def __init__(self, state):
        super().__init__(state)
        self.sub_agents: Dict[str, BaseAgent] = {}
        self._message_log: List[Dict] = []

    async def initialize(self):
        """Initialize coordinator and all sub-agents."""
        await super().initialize()

        # Create sub-agents
        self.sub_agents = {
            "market_scanner": MarketScannerAgent(self.state),
            "analyst": AnalystAgent(self.state),
            "risk_manager": RiskManagerAgent(self.state),
            "executor": ExecutorAgent(self.state),
            "delegate": DelegateAgent(self.state),
        }

        # Initialize each sub-agent
        for name, agent in self.sub_agents.items():
            await agent.initialize()

        logger.info(f"Coordinator initialized with {len(self.sub_agents)} sub-agents")

    async def handle_message(self, message: AgentMessage):
        """Coordinator handles status queries and commands."""
        if message.action == "status":
            return self.get_dashboard()
        elif message.action == "start":
            await self.start_pipeline()
        elif message.action == "stop":
            await self.stop_pipeline()
        elif message.action == "emergency_close":
            executor = self.sub_agents.get("executor")
            if executor:
                await executor.close_all_positions()

    async def route_message(self, message: AgentMessage):
        """Route a message to the appropriate sub-agent."""
        recipient = self.sub_agents.get(message.recipient)
        if recipient:
            self._message_log.append({
                "time": datetime.now(timezone.utc).isoformat(),
                "from": message.sender,
                "to": message.recipient,
                "action": message.action,
            })
            await recipient.receive(message)
        else:
            logger.warning(f"Unknown recipient: {message.recipient}")

    async def start_pipeline(self):
        """Start the full analysis pipeline."""
        logger.info("=" * 60)
        logger.info("  STARTING TRADING PIPELINE")
        logger.info("=" * 60)

        # Start scanner loop
        scanner = self.sub_agents.get("market_scanner")
        if scanner:
            await scanner.start_scan_loop()

        # Start executor position monitor
        executor = self.sub_agents.get("executor")
        if executor:
            await executor.start_position_monitor()

        logger.info("Pipeline started — scanning for opportunities...")

    async def stop_pipeline(self):
        """Stop all agents."""
        scanner = self.sub_agents.get("market_scanner")
        if scanner:
            await scanner.stop_scan_loop()

        logger.info("Pipeline stopped")

    async def shutdown(self):
        """Full shutdown."""
        await self.stop_pipeline()
        for agent in self.sub_agents.values():
            await agent.shutdown()
        await super().shutdown()

    def get_dashboard(self) -> Dict:
        """Get current system status for dashboard."""
        portfolio = self.state.portfolio
        risk_manager = self.sub_agents.get("risk_manager")

        dashboard = {
            "session_id": self.state.session_id,
            "uptime": str(datetime.now(timezone.utc) - self.state.start_time),
            "boot_time_ms": self.state.boot_metrics.total_boot_ms if self.state.boot_metrics else 0,
            "agents": {
                name: {"active": agent.is_active, "role": agent.role}
                for name, agent in self.sub_agents.items()
            },
            "portfolio": {
                "equity": portfolio.total_equity,
                "available": portfolio.available_balance,
                "margin_used": portfolio.margin_used,
                "open_positions": portfolio.open_positions,
                "total_risk_pct": f"{portfolio.total_risk_pct:.1f}%",
                "daily_pnl": f"${portfolio.daily_pnl:+.2f}",
                "total_pnl": f"${portfolio.total_pnl:+.2f}",
                "win_rate": f"{portfolio.win_rate:.1f}%",
                "max_drawdown": f"{portfolio.max_drawdown_pct:.1f}%",
            },
            "activity": {
                "total_analyses": self.state.total_analyses,
                "total_signals": self.state.total_signals,
                "total_trades": self.state.total_trades,
                "messages_routed": len(self._message_log),
            },
            "risk_manager": risk_manager.get_stats() if risk_manager else {},
            "task_stats": self.state.task_manager.get_stats() if self.state.task_manager else {},
            "symbols": self.state.config.symbols,
        }

        # ── hermes: Enhanced component stats ──
        if hasattr(self.state, 'skills_manager') and self.state.skills_manager:
            dashboard["skills"] = {
                "total": len(self.state.skills_manager._skills),
                "active": sum(1 for s in self.state.skills_manager._skills.values() if s.is_active),
            }
        if hasattr(self.state, 'trajectory_logger') and self.state.trajectory_logger:
            dashboard["trajectories"] = self.state.trajectory_logger._stats
        if hasattr(self.state, 'context_engine') and self.state.context_engine:
            dashboard["context_engine"] = {
                "scan_count": getattr(self.state.context_engine, '_scan_count', 0),
            }

        return dashboard
