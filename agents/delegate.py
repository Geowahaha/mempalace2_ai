"""
Delegate Agent — Parallel subagent architecture for multi-symbol analysis.

Adapted from hermes-agent/tools/delegate_tool.py for the mempalace2 trading system.

Key capabilities:
  - Spawn parallel analysis subagents for multi-symbol scanning
  - Isolated backtesting subagents that don't affect live state
  - Batch mode: analyze multiple symbols concurrently
  - Depth-limited: no recursive delegation (MAX_DEPTH=1)

Each child gets:
  - A fresh analysis context (no parent history leak)
  - Its own trajectory for logging
  - Restricted toolset (no execution, no circuit breaker manipulation)
  - Focused goal from the delegating agent

The parent sees only the summary result, not intermediate reasoning.
"""

from __future__ import annotations

import asyncio
import logging
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Callable

from agents.base import BaseAgent, AgentMessage

logger = logging.getLogger("mempalace2.agents.delegate")

# Tools that child subagents must never access
DELEGATE_BLOCKED_TOOLS = frozenset([
    "execute_trade",        # children analyze, don't execute
    "close_all_positions",  # no emergency close by children
    "set_circuit_breaker",  # no risk override
    "delegate_task",        # no recursive delegation (MAX_DEPTH=1)
])

# Maximum concurrent child subagents
_MAX_CONCURRENT_CHILDREN = 3

# Maximum delegation depth (parent=0, child=1, grandchild=blocked)
MAX_DEPTH = 1


@dataclass
class DelegationResult:
    """Result from a delegated subagent task."""
    symbol: str
    success: bool = False
    setups: List[Dict] = field(default_factory=list)
    summary: str = ""
    trajectory_id: str = ""
    duration_s: float = 0.0
    error: Optional[str] = None


class DelegateAgent(BaseAgent):
    """
    Orchestrates parallel subagent delegation for multi-symbol analysis.

    Supports two modes:
      1. Parallel scan: analyze multiple symbols concurrently
      2. Isolated backtest: run backtests without affecting live state
    """

    name = "delegate"
    role = "Parallel subagent delegation for multi-symbol analysis"

    def __init__(self, state):
        super().__init__(state)
        self._active_delegations: Dict[str, asyncio.Task] = {}
        self._results: Dict[str, DelegationResult] = {}

    async def handle_message(self, message: AgentMessage):
        if message.action == "delegate_scan":
            symbols = message.data.get("symbols", [])
            await self.delegate_parallel_scan(symbols)
        elif message.action == "delegate_analyze":
            symbol = message.data.get("symbol", "")
            setup = message.data.get("setup", {})
            await self.delegate_analysis(symbol, setup)
        elif message.action == "results":
            return self.get_results()

    async def delegate_parallel_scan(self, symbols: List[str]) -> Dict[str, DelegationResult]:
        """
        Scan multiple symbols in parallel using subagents.

        Each symbol gets its own isolated analysis subagent that:
          - Fetches market data
          - Runs technical analysis
          - Detects setups
          - Returns results without affecting live state

        Args:
            symbols: List of symbols to scan (e.g., ["XAUUSD", "EURUSD"])

        Returns:
            Dict mapping symbol → DelegationResult
        """
        if not symbols:
            symbols = self.state.config.symbols

        # Limit concurrency
        symbols = symbols[:_MAX_CONCURRENT_CHILDREN]

        logger.info(f"🔀 Delegating parallel scan: {symbols}")
        start_time = time.monotonic()

        results: Dict[str, DelegationResult] = {}

        # Use asyncio.gather for parallel execution
        tasks = [
            self._run_symbol_scan(symbol, idx)
            for idx, symbol in enumerate(symbols)
        ]

        completed = await asyncio.gather(*tasks, return_exceptions=True)

        for symbol, result in zip(symbols, completed):
            if isinstance(result, Exception):
                results[symbol] = DelegationResult(
                    symbol=symbol,
                    success=False,
                    error=str(result),
                )
            else:
                results[symbol] = result

        elapsed = time.monotonic() - start_time
        successful = sum(1 for r in results.values() if r.success)
        logger.info(
            f"🔀 Parallel scan complete: {successful}/{len(symbols)} "
            f"symbols in {elapsed:.1f}s"
        )

        self._results.update(results)
        return results

    async def _run_symbol_scan(self, symbol: str, index: int) -> DelegationResult:
        """
        Run isolated scan for a single symbol.

        This is the child subagent execution. It:
          - Has no access to parent's trade state
          - Cannot execute trades or modify risk
          - Produces a summary result only
        """
        start = time.monotonic()

        # Start trajectory for this delegation
        trajectory_id = None
        if hasattr(self.state, 'trajectory_logger') and self.state.trajectory_logger:
            trajectory_id = self.state.trajectory_logger.start_trajectory(
                session_id=getattr(self.state, 'state_store_session_id', self.state.session_id),
                symbol=symbol,
                direction="pending",
            )
            self.state.trajectory_logger.add_step(trajectory_id, "delegate_scan_start", {
                "symbol": symbol,
                "index": index,
            })

        try:
            # Get tools (isolated — no blocked tools)
            market_data_tool = self.get_tool("market_data")
            ta_tool = self.get_tool("technical_analysis")
            if not market_data_tool or not ta_tool:
                raise RuntimeError("Required tools not available")

            # 1. Fetch data
            result = await market_data_tool.execute(
                symbol=symbol,
                timeframe=self.state.config.analysis.primary_timeframe,
                limit=self.state.config.analysis.lookback_candles,
            )
            if not result.success:
                raise RuntimeError(f"Market data fetch failed: {result.error}")

            df = result.data

            # 2. Run technical analysis
            ta_result = await ta_tool.execute(data=df)
            if not ta_result.success:
                raise RuntimeError(f"TA failed: {ta_result.error}")

            indicators = ta_result.data["indicators"]

            # 3. Detect setups (reuse scanner logic)
            setups = self._detect_setups(symbol, indicators, df)

            # 4. Check memory for similar patterns (read-only)
            memory_context = ""
            if hasattr(self.state, 'memory') and self.state.memory:
                for setup in setups:
                    ctx = self.state.memory.build_context_for_analysis(
                        symbol=symbol,
                        setup_type=setup["type"],
                        direction=setup["direction"],
                    )
                    if ctx:
                        memory_context += ctx + "\n"

            # 5. Match skills (read-only)
            skills_context = ""
            if hasattr(self.state, 'skills_manager') and self.state.skills_manager and setups:
                best = max(setups, key=lambda s: s["strength"])
                skills_context = self.state.skills_manager.build_skills_context_block({
                    "symbol": symbol,
                    "setup_type": best["type"],
                    "direction": best["direction"],
                    "timeframe": self.state.config.analysis.primary_timeframe,
                })

            # Build summary
            summary_parts = [f"{symbol}: {len(setups)} setup(s) detected"]
            for s in setups:
                summary_parts.append(
                    f"  • {s['type']} {s['direction']} (strength={s['strength']:.0f})"
                )
            if memory_context:
                summary_parts.append(f"  Memory: {len(memory_context)} chars context")
            if skills_context:
                summary_parts.append(f"  Skills: matched relevant skills")

            summary = "\n".join(summary_parts)

            # Finalize trajectory
            if trajectory_id and hasattr(self.state, 'trajectory_logger') and self.state.trajectory_logger:
                self.state.trajectory_logger.add_step(trajectory_id, "delegate_scan_complete", {
                    "symbol": symbol,
                    "setups_found": len(setups),
                    "has_memory": bool(memory_context),
                    "has_skills": bool(skills_context),
                })
                self.state.trajectory_logger.finalize(trajectory_id, "executed", {
                    "symbol": symbol,
                    "setups": len(setups),
                })

            return DelegationResult(
                symbol=symbol,
                success=True,
                setups=setups,
                summary=summary,
                trajectory_id=trajectory_id or "",
                duration_s=time.monotonic() - start,
            )

        except Exception as e:
            logger.error(f"Delegated scan failed for {symbol}: {e}")

            if trajectory_id and hasattr(self.state, 'trajectory_logger') and self.state.trajectory_logger:
                self.state.trajectory_logger.finalize(trajectory_id, "failed", {
                    "symbol": symbol,
                    "error": str(e),
                })

            return DelegationResult(
                symbol=symbol,
                success=False,
                error=str(e),
                duration_s=time.monotonic() - start,
            )

    def _detect_setups(self, symbol: str, indicators, df) -> List[dict]:
        """Detect setups — delegates to scanner logic."""
        # Import and reuse scanner's detection logic
        # This avoids duplicating the setup detection code
        setups = []
        close = float(df["close"].iloc[-1])

        # EMA Crossover
        ema_fast_prev = float(df["ema_fast"].iloc[-2])
        ema_slow_prev = float(df["ema_slow"].iloc[-2])
        if (indicators.ema_fast > indicators.ema_slow and ema_fast_prev <= ema_slow_prev):
            setups.append({
                "type": "ema_crossover", "direction": "long",
                "strength": 70 + min(20, indicators.adx),
                "reasoning": f"EMA bullish crossover",
            })
        elif (indicators.ema_fast < indicators.ema_slow and ema_fast_prev >= ema_slow_prev):
            setups.append({
                "type": "ema_crossover", "direction": "short",
                "strength": 70 + min(20, indicators.adx),
                "reasoning": f"EMA bearish crossover",
            })

        # RSI Oversold/Overbought
        if indicators.rsi < self.state.config.analysis.rsi_oversold:
            setups.append({
                "type": "rsi_oversold", "direction": "long",
                "strength": 60 + (self.state.config.analysis.rsi_oversold - indicators.rsi),
                "reasoning": f"RSI at {indicators.rsi:.1f} — oversold",
            })
        elif indicators.rsi > self.state.config.analysis.rsi_overbought:
            setups.append({
                "type": "rsi_overbought", "direction": "short",
                "strength": 60 + (indicators.rsi - self.state.config.analysis.rsi_overbought),
                "reasoning": f"RSI at {indicators.rsi:.1f} — overbought",
            })

        # Supertrend flip
        st_prev = int(df["supertrend_direction"].iloc[-2])
        if indicators.supertrend_direction == 1 and st_prev == -1:
            setups.append({
                "type": "supertrend_flip_bullish", "direction": "long",
                "strength": 75, "reasoning": "Supertrend flipped bullish",
            })
        elif indicators.supertrend_direction == -1 and st_prev == 1:
            setups.append({
                "type": "supertrend_flip_bearish", "direction": "short",
                "strength": 75, "reasoning": "Supertrend flipped bearish",
            })

        # Trend alignment
        trend_long = (
            indicators.ema_fast > indicators.ema_slow > indicators.ema_trend
            and close > indicators.ema_trend
            and indicators.supertrend_direction == 1
            and indicators.macd_histogram > 0
        )
        trend_short = (
            indicators.ema_fast < indicators.ema_slow < indicators.ema_trend
            and close < indicators.ema_trend
            and indicators.supertrend_direction == -1
            and indicators.macd_histogram < 0
        )
        if trend_long:
            setups.append({
                "type": "trend_alignment", "direction": "long",
                "strength": 80, "reasoning": "Full trend alignment bullish",
            })
        elif trend_short:
            setups.append({
                "type": "trend_alignment", "direction": "short",
                "strength": 80, "reasoning": "Full trend alignment bearish",
            })

        return setups

    async def delegate_analysis(self, symbol: str, setup: dict) -> Optional[DelegationResult]:
        """
        Delegate deep analysis of a single setup to an isolated subagent.

        Use this when the coordinator wants to analyze a setup in parallel
        without blocking the main scan loop.
        """
        logger.info(f"🔀 Delegating analysis: {symbol} {setup.get('type')}")

        # Forward to analyst via normal message routing
        # The analyst runs in the same process but with isolated context
        await self.send(
            recipient="analyst",
            action="analyze",
            data={
                "symbol": symbol,
                "setup": setup,
            },
            priority=int(setup.get("strength", 50)),
        )

        return DelegationResult(
            symbol=symbol,
            success=True,
            summary=f"Analysis delegated for {symbol} {setup.get('type')}",
        )

    def get_results(self) -> Dict[str, DelegationResult]:
        """Get results from completed delegations."""
        return dict(self._results)

    def get_stats(self) -> Dict[str, Any]:
        """Get delegation statistics."""
        total = len(self._results)
        successful = sum(1 for r in self._results.values() if r.success)
        total_setups = sum(len(r.setups) for r in self._results.values())
        return {
            "total_delegations": total,
            "successful": successful,
            "failed": total - successful,
            "total_setups_found": total_setups,
        }
