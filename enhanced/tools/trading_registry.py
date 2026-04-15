"""
Enhanced Tool Registry — adapted from hermes-agent's tools/registry.py.

Extends the original tool registry with:
  - Toolsets: market_data, analysis, risk, execution, portfolio, learning
  - Risk-based tool gating (block execution when circuit breaker trips)
  - Tool metadata with schemas for LLM tool-use
  - Thread-safe registration and invocation

Key hermes-agent concepts adapted:
  - ToolEntry with schema, toolset, and handler
  - ToolRegistry with register/get/check
  - Toolset-based access control
"""

import json
import logging
import threading
import time
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional, Set

from tools.base import ToolRegistry as BaseToolRegistry

logger = logging.getLogger("mempalace2.enhanced.tools.registry")


# ── Toolset Definitions ─────────────────────────────────

TOOLSETS = {
    "market_data": {
        "description": "OHLCV data fetching, price feeds, tick data",
        "tools": ["get_ohlcv", "get_ticker", "get_orderbook"],
    },
    "analysis": {
        "description": "Technical analysis, indicator computation, pattern detection",
        "tools": ["compute_indicators", "detect_patterns", "analyze_multi_timeframe"],
    },
    "risk": {
        "description": "Position sizing, risk:reward, portfolio heat",
        "tools": ["calculate_position_size", "validate_risk", "check_portfolio_heat"],
    },
    "execution": {
        "description": "Order placement, trailing stops, position management",
        "tools": ["place_order", "modify_order", "close_position", "set_trailing_stop"],
    },
    "portfolio": {
        "description": "Portfolio state, P&L tracking, equity management",
        "tools": ["get_portfolio", "get_open_positions", "get_trade_history"],
    },
    "learning": {
        "description": "Memory, skills, trajectory logging — self-improvement tools",
        "tools": ["store_pattern", "recall_patterns", "store_lesson", "log_trajectory",
                  "list_skills", "match_skills"],
    },
}


@dataclass
class ToolEntry:
    """Enhanced tool entry with schema and toolset metadata."""
    name: str
    handler: Callable
    toolset: str = "general"
    description: str = ""
    schema: Dict = field(default_factory=dict)
    requires_live: bool = False  # True for execution tools
    enabled: bool = True
    call_count: int = 0
    last_called: Optional[float] = None
    total_ms: float = 0.0

    def to_schema(self) -> Dict:
        """Return OpenAI-compatible function schema for LLM tool-use."""
        return {
            "type": "function",
            "function": {
                "name": self.name,
                "description": self.description,
                "parameters": self.schema or {"type": "object", "properties": {}},
            },
        }


class EnhancedToolRegistry:
    """
    Enhanced tool registry with toolsets, schemas, and risk gating.

    Wraps the base registry and adds:
      - Toolset-based organization
      - Risk gating (block execution tools when circuit breaker active)
      - LLM-compatible schema export
      - Performance tracking
    """

    def __init__(self, base_registry: Optional[BaseToolRegistry] = None):
        self._base = base_registry
        self._tools: Dict[str, ToolEntry] = {}
        self._lock = threading.RLock()
        self._circuit_breaker_active = False
        self._blocked_toolsets: Set[str] = set()

    # ── Registration ────────────────────────────────────

    def register(self, name: str, handler: Callable,
                 toolset: str = "general", description: str = "",
                 schema: Dict = None, requires_live: bool = False):
        """Register an enhanced tool."""
        with self._lock:
            self._tools[name] = ToolEntry(
                name=name,
                handler=handler,
                toolset=toolset,
                description=description,
                schema=schema or {},
                requires_live=requires_live,
            )
            logger.debug(f"Registered tool: {name} [{toolset}]")

    def register_from_base(self):
        """Import tools from the base registry."""
        if not self._base:
            return
        for name in self._base.list_all():
            tool = self._base.get(name)
            if tool:
                # Map to toolset based on name patterns
                toolset = self._infer_toolset(name)
                self.register(
                    name=name,
                    handler=tool.execute if hasattr(tool, 'execute') else lambda x: x,
                    toolset=toolset,
                    description=getattr(tool, 'description', ''),
                )

    def _infer_toolset(self, name: str) -> str:
        """Infer toolset from tool name."""
        name_lower = name.lower()
        if "market" in name_lower or "ohlcv" in name_lower or "ticker" in name_lower:
            return "market_data"
        elif "indicator" in name_lower or "technical" in name_lower or "pattern" in name_lower:
            return "analysis"
        elif "risk" in name_lower or "position" in name_lower or "kelly" in name_lower:
            return "risk"
        elif "order" in name_lower or "execute" in name_lower or "trade" in name_lower:
            return "execution"
        elif "portfolio" in name_lower or "pnl" in name_lower or "equity" in name_lower:
            return "portfolio"
        elif "memory" in name_lower or "skill" in name_lower or "trajectory" in name_lower:
            return "learning"
        return "general"

    # ── Invocation ──────────────────────────────────────

    def get(self, name: str) -> Optional[ToolEntry]:
        """Get a tool entry by name."""
        with self._lock:
            return self._tools.get(name)

    def invoke(self, name: str, args: Dict[str, Any] = None) -> Any:
        """
        Invoke a tool by name with arguments.

        Respects circuit breaker and toolset blocking.
        """
        with self._lock:
            entry = self._tools.get(name)
            if not entry:
                return {"error": f"Tool '{name}' not found"}

            if not entry.enabled:
                return {"error": f"Tool '{name}' is disabled"}

            # Risk gating
            if self._circuit_breaker_active and entry.requires_live:
                return {
                    "error": "Circuit breaker active — execution tools blocked",
                    "suggestion": "Check risk state, or wait for circuit breaker reset",
                }

            if entry.toolset in self._blocked_toolsets:
                return {
                    "error": f"Toolset '{entry.toolset}' is blocked",
                    "suggestion": "Use analysis or learning tools instead",
                }

        # Execute outside lock
        start = time.monotonic()
        try:
            result = entry.handler(args or {})
            elapsed = (time.monotonic() - start) * 1000
            with self._lock:
                entry.call_count += 1
                entry.last_called = time.time()
                entry.total_ms += elapsed
            return result
        except Exception as e:
            logger.error(f"Tool '{name}' failed: {e}")
            return {"error": str(e)}

    # ── Risk Gating ─────────────────────────────────────

    def set_circuit_breaker(self, active: bool):
        """Enable/disable circuit breaker (blocks execution tools)."""
        with self._lock:
            self._circuit_breaker_active = active
            if active:
                logger.warning("Circuit breaker ACTIVATED — execution tools blocked")
            else:
                logger.info("Circuit breaker deactivated")

    def block_toolset(self, toolset: str, blocked: bool = True):
        """Block/unblock a toolset."""
        with self._lock:
            if blocked:
                self._blocked_toolsets.add(toolset)
            else:
                self._blocked_toolsets.discard(toolset)

    # ── Schema Export (for LLM tool-use) ────────────────

    def get_schemas(self, toolsets: List[str] = None) -> List[Dict]:
        """
        Get OpenAI-compatible function schemas for all enabled tools.

        Args:
            toolsets: If provided, only include tools from these toolsets.
        """
        with self._lock:
            schemas = []
            for entry in self._tools.values():
                if not entry.enabled:
                    continue
                if toolsets and entry.toolset not in toolsets:
                    continue
                if self._circuit_breaker_active and entry.requires_live:
                    continue
                if entry.toolset in self._blocked_toolsets:
                    continue
                schemas.append(entry.to_schema())
            return schemas

    def get_toolset_schemas(self, toolset: str) -> List[Dict]:
        """Get schemas for a specific toolset."""
        return self.get_schemas(toolsets=[toolset])

    # ── Listing & Stats ─────────────────────────────────

    def list_all(self) -> List[str]:
        """List all registered tool names."""
        with self._lock:
            return list(self._tools.keys())

    def list_by_toolset(self, toolset: str) -> List[str]:
        """List tools in a specific toolset."""
        with self._lock:
            return [name for name, entry in self._tools.items()
                    if entry.toolset == toolset]

    def get_stats(self) -> Dict[str, Any]:
        """Get registry statistics."""
        with self._lock:
            by_toolset = {}
            for entry in self._tools.values():
                if entry.toolset not in by_toolset:
                    by_toolset[entry.toolset] = {"count": 0, "calls": 0}
                by_toolset[entry.toolset]["count"] += 1
                by_toolset[entry.toolset]["calls"] += entry.call_count

            return {
                "total": len(self._tools),
                "enabled": sum(1 for e in self._tools.values() if e.enabled),
                "circuit_breaker": self._circuit_breaker_active,
                "blocked_toolsets": list(self._blocked_toolsets),
                "by_toolset": by_toolset,
                "toolsets_available": list(TOOLSETS.keys()),
            }


# ── Learning Tool Implementations ───────────────────────
# These are the actual handler functions for the "learning" toolset.

def make_memory_tools(memory_system) -> Dict[str, Callable]:
    """Create tool handlers backed by the TradeMemory system."""
    return {
        "store_pattern": lambda args: _store_pattern(memory_system, args),
        "recall_patterns": lambda args: _recall_patterns(memory_system, args),
        "store_lesson": lambda args: _store_lesson(memory_system, args),
    }


def make_trajectory_tools(trajectory_logger) -> Dict[str, Callable]:
    """Create tool handlers backed by the TrajectoryLogger."""
    return {
        "log_trajectory": lambda args: _log_trajectory(trajectory_logger, args),
    }


def make_skill_tools(skills_manager) -> Dict[str, Callable]:
    """Create tool handlers backed by the SkillManager."""
    return {
        "list_skills": lambda args: _list_skills(skills_manager, args),
        "match_skills": lambda args: _match_skills(skills_manager, args),
    }


def _store_pattern(memory, args):
    memory.store_trade_pattern(
        symbol=args.get("symbol", ""),
        setup_type=args.get("setup_type", ""),
        direction=args.get("direction", ""),
        conditions=args.get("conditions", {}),
        outcome=args.get("outcome", {}),
    )
    return {"success": True, "message": "Pattern stored"}


def _recall_patterns(memory, args):
    items = memory.recall_similar_patterns(
        symbol=args.get("symbol", ""),
        setup_type=args.get("setup_type", ""),
    )
    return {"patterns": [i.to_dict() for i in items]}


def _store_lesson(memory, args):
    memory.store_lesson(
        event_type=args.get("event_type", "insight"),
        trade_id=args.get("trade_id"),
        description=args.get("description", ""),
        lesson=args.get("lesson", ""),
        metadata=args.get("metadata"),
    )
    return {"success": True, "message": "Lesson stored"}


def _log_trajectory(logger_tool, args):
    tid = args.get("trajectory_id")
    if tid:
        logger_tool.add_step(tid, args.get("step_type", "unknown"), args.get("data", {}))
        return {"success": True, "trajectory_id": tid}
    return {"error": "No trajectory_id provided"}


def _list_skills(skills_mgr, args):
    min_wr = args.get("min_win_rate", 0.0)
    category = args.get("category")
    skills = skills_mgr.list_skills(category=category, min_win_rate=min_wr)
    return {"skills": [s.to_dict() for s in skills]}


def _match_skills(skills_mgr, args):
    matches = skills_mgr.match_skills(
        context=args.get("context", {}),
        min_score=args.get("min_score", 0.3),
    )
    return {"matches": [(e.to_dict(), score) for e, score in matches]}
