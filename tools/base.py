"""
Tool System — Interface design, registration, routing, execution.

Inspired by Claude Code's Tool.ts / tools.ts pattern:
  - Structural type every tool must satisfy
  - buildTool() factory with safe defaults
  - Registry with filtering by context
"""

from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, Optional

logger = logging.getLogger("mempalace2.tool")


class ToolCategory(str, Enum):
    MARKET_DATA = "market_data"
    ANALYSIS = "analysis"
    RISK = "risk"
    EXECUTION = "execution"
    PORTFOLIO = "portfolio"


@dataclass
class ToolResult:
    """Result returned by any tool execution."""
    success: bool = True
    data: Any = None
    error: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)

    @staticmethod
    def ok(data: Any, **meta) -> "ToolResult":
        return ToolResult(success=True, data=data, metadata=meta)

    @staticmethod
    def fail(error: str, **meta) -> "ToolResult":
        return ToolResult(success=False, error=error, metadata=meta)


class Tool(ABC):
    """
    Base tool interface — every tool must satisfy this contract.

    Core properties:
      name:          Primary identifier
      category:      Tool category for filtering
      description:   What this tool does
      is_read_only:  Whether tool mutates state
      is_safe:       Whether tool can be run without approval
    """

    name: str = ""
    category: ToolCategory = ToolCategory.MARKET_DATA
    description: str = ""
    is_read_only: bool = True
    is_safe: bool = True

    @abstractmethod
    async def execute(self, **kwargs) -> ToolResult:
        """Execute the tool with given parameters."""
        ...

    def validate_input(self, **kwargs) -> Optional[str]:
        """Validate inputs. Returns error string or None."""
        return None


class ToolRegistry:
    """
    Tool registration and routing system.

    Maintains a registry of all available tools with:
      - Registration with duplicate detection
      - Category-based filtering
      - Safe execution with error handling
    """

    def __init__(self):
        self._tools: Dict[str, Tool] = {}

    def register(self, tool: Tool) -> None:
        """Register a tool. Raises on duplicate names."""
        if tool.name in self._tools:
            raise ValueError(f"Tool '{tool.name}' already registered")
        self._tools[tool.name] = tool
        logger.debug(f"Tool registered: {tool.name} [{tool.category.value}]")

    def get(self, name: str) -> Optional[Tool]:
        """Get a tool by name."""
        return self._tools.get(name)

    def get_by_category(self, category: ToolCategory) -> list[Tool]:
        """Get all tools in a category."""
        return [t for t in self._tools.values() if t.category == category]

    def list_all(self) -> list[str]:
        """List all registered tool names."""
        return list(self._tools.keys())

    async def execute(self, name: str, **kwargs) -> ToolResult:
        """Execute a tool by name with error handling."""
        tool = self._tools.get(name)
        if not tool:
            return ToolResult.fail(f"Tool '{name}' not found")

        # Validate
        error = tool.validate_input(**kwargs)
        if error:
            return ToolResult.fail(f"Validation error: {error}")

        try:
            return await tool.execute(**kwargs)
        except Exception as e:
            logger.error(f"Tool '{name}' execution failed: {e}")
            return ToolResult.fail(str(e))

    @property
    def count(self) -> int:
        return len(self._tools)
