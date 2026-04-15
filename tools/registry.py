"""
Tool Registry — Registers all available tools.

Inspired by Claude Code's tools.ts / getAllBaseTools() pattern.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from tools.base import ToolRegistry
from tools.market_data import MarketDataTool
from tools.technical import TechnicalAnalysisTool
from tools.risk_engine import RiskEngineTool

if TYPE_CHECKING:
    from core.state import GlobalState

logger = logging.getLogger("mempalace2.registry")


def register_all_tools(state: "GlobalState") -> ToolRegistry:
    """
    Register all tools into the global state.

    Tools:
      market_data       — OHLCV data fetching (XAUUSD, forex)
      technical_analysis — Full indicator suite
      risk_engine       — Position sizing & risk validation
    """
    registry = ToolRegistry()

    # Market Data
    exchange_cfg = {
        "api_key": state.config.exchanges.api_key,
        "api_secret": state.config.exchanges.api_secret,
        "sandbox": state.config.exchanges.sandbox,
    }
    registry.register(MarketDataTool(exchange_client=exchange_cfg))

    # Technical Analysis
    registry.register(TechnicalAnalysisTool(config=state.config.analysis))

    # Risk Engine
    registry.register(RiskEngineTool(config=state.config.risk))

    # Store in state
    state.tools = {name: registry.get(name) for name in registry.list_all()}

    logger.info(f"Registered {registry.count} tools: {registry.list_all()}")
    return registry
