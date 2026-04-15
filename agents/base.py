"""
Base Agent — Foundation for all specialized trading agents.

Inspired by Claude Code's agent system:
  - Each agent has a specialized role
  - Agents communicate through the coordinator
  - Tools are injected, not owned
"""

from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any, Dict, Optional, TYPE_CHECKING

if TYPE_CHECKING:
    from core.state import GlobalState

logger = logging.getLogger("mempalace2.agent")


@dataclass
class AgentMessage:
    """Message passed between agents."""
    sender: str
    recipient: str
    action: str  # "scan", "analyze", "validate", "execute", "alert"
    data: Dict[str, Any] = None
    priority: int = 0

    def __post_init__(self):
        if self.data is None:
            self.data = {}


class BaseAgent(ABC):
    """
    Base class for all trading agents.

    Each agent:
      - Has a unique name and role
      - Shares the global state
      - Can use tools via the registry
      - Communicates through message passing
    """

    name: str = "base"
    role: str = ""

    def __init__(self, state: "GlobalState"):
        self.state = state
        self.is_active = False
        self.message_queue: list[AgentMessage] = []

    async def initialize(self):
        """Called during boot phase 2."""
        self.is_active = True
        self.state.agents[self.name] = self
        logger.info(f"Agent initialized: {self.name} [{self.role}]")

    async def shutdown(self):
        """Clean shutdown."""
        self.is_active = False
        logger.info(f"Agent shutdown: {self.name}")

    async def receive(self, message: AgentMessage):
        """Receive a message from another agent."""
        self.message_queue.append(message)
        await self.handle_message(message)

    @abstractmethod
    async def handle_message(self, message: AgentMessage):
        """Process an incoming message. Override in subclasses."""
        ...

    def get_tool(self, name: str):
        """Get a registered tool by name."""
        return self.state.tools.get(name)

    async def send(self, recipient: str, action: str, data: Dict = None, priority: int = 0):
        """Send a message to another agent via coordinator."""
        msg = AgentMessage(
            sender=self.name,
            recipient=recipient,
            action=action,
            data=data or {},
            priority=priority,
        )
        if self.state.coordinator:
            await self.state.coordinator.route_message(msg)

    def __repr__(self):
        return f"<{self.__class__.__name__} name={self.name} active={self.is_active}>"
