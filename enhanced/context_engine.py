"""
Context Compression Engine — adapted from hermes-agent's agent/context_engine.py.

Manages token budget for long-running trading sessions (hours/days).
Compresses old scan results and market data while preserving recent
trades and active positions.

Key hermes-agent concepts adapted:
  - ContextEngine ABC with pluggable implementations
  - should_compress() threshold checking
  - compress() for message list compaction
  - Token tracking from API responses

For the trading system, "context" means:
  - Market scan results (latest price, indicators)
  - Recent trade history
  - Active positions
  - Signal history
  - Memory (patterns + lessons)
"""

import json
import logging
import time
from abc import ABC, abstractmethod
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

logger = logging.getLogger("mempalace2.enhanced.context")


class ContextEngine(ABC):
    """
    Abstract base class for context management engines.

    A context engine controls how conversation/scanning context is
    managed when approaching capacity limits. Different engines can
    implement different strategies (summarization, DAG, etc.).
    """

    @property
    @abstractmethod
    def name(self) -> str:
        """Short identifier (e.g. 'compressor', 'trading_compressor')."""

    # Token/state tracking
    last_prompt_tokens: int = 0
    last_completion_tokens: int = 0
    last_total_tokens: int = 0
    threshold_tokens: int = 0
    context_length: int = 0
    compression_count: int = 0

    # Compaction parameters
    threshold_percent: float = 0.75
    protect_first_n: int = 3
    protect_last_n: int = 6

    @abstractmethod
    def update_from_response(self, usage: Dict[str, Any]) -> None:
        """Update token usage tracking from an API response."""

    @abstractmethod
    def should_compress(self, prompt_tokens: int = None) -> bool:
        """Return True if compression should fire."""

    @abstractmethod
    def compress(self, messages: List[Dict[str, Any]],
                 current_tokens: int = None) -> List[Dict[str, Any]]:
        """Compact the message list and return the new list."""

    def on_session_start(self, session_id: str, **kwargs) -> None:
        """Called when a trading session begins."""

    def on_session_end(self, session_id: str,
                       messages: List[Dict[str, Any]] = None) -> None:
        """Called when a trading session ends."""

    def on_session_reset(self) -> None:
        """Reset per-session state."""
        self.last_prompt_tokens = 0
        self.last_completion_tokens = 0
        self.last_total_tokens = 0
        self.compression_count = 0

    def get_status(self) -> Dict[str, Any]:
        """Return status dict for display/logging."""
        return {
            "name": self.name,
            "last_prompt_tokens": self.last_prompt_tokens,
            "threshold_tokens": self.threshold_tokens,
            "context_length": self.context_length,
            "usage_percent": (
                min(100, self.last_prompt_tokens / self.context_length * 100)
                if self.context_length else 0
            ),
            "compression_count": self.compression_count,
        }


class TradingContextCompressor(ContextEngine):
    """
    Context compressor specialized for trading sessions.

    Strategy:
      1. Always preserve: system prompt, active positions, recent trades (last N)
      2. Compress: old scan results, historical market data, completed trade details
      3. Summarize: old signals into aggregate stats

    This is the default engine for mempalace2 trading sessions.
    """

    def __init__(self, context_length: int = 128_000,
                 threshold_percent: float = 0.75,
                 protect_last_n: int = 10):
        self.context_length = context_length
        self.threshold_percent = threshold_percent
        self.threshold_tokens = int(context_length * threshold_percent)
        self.protect_last_n = protect_last_n
        self._session_id: Optional[str] = None
        self._scan_count = 0
        self._last_compress_time: float = 0

    @property
    def name(self) -> str:
        return "trading_compressor"

    def update_from_response(self, usage: Dict[str, Any]) -> None:
        """Track token usage from API responses."""
        self.last_prompt_tokens = usage.get("prompt_tokens", 0)
        self.last_completion_tokens = usage.get("completion_tokens", 0)
        self.last_total_tokens = usage.get("total_tokens", 0)

    def should_compress(self, prompt_tokens: int = None) -> bool:
        """Check if we should compress based on token threshold."""
        tokens = prompt_tokens or self.last_prompt_tokens
        if tokens >= self.threshold_tokens:
            return True
        # Also compress every N scans to prevent context drift
        if self._scan_count > 0 and self._scan_count % 50 == 0:
            return True
        return False

    def compress(self, messages: List[Dict[str, Any]],
                 current_tokens: int = None) -> List[Dict[str, Any]]:
        """
        Compress trading context messages.

        Strategy:
          1. Separate messages into categories
          2. Keep protected messages (first N, last N, active positions)
          3. Compress old scan results into summaries
          4. Drop redundant market data snapshots
        """
        self.compression_count += 1
        self._last_compress_time = time.time()

        if len(messages) <= self.protect_last_n + 3:
            return messages  # Not enough to compress

        # Separate into protected and compressible
        protected = messages[:self.protect_first_n] + messages[-self.protect_last_n:]
        middle = messages[self.protect_first_n:-self.protect_last_n]

        # Categorize middle messages
        scan_results = []
        trade_updates = []
        market_data = []
        other = []

        for msg in middle:
            content = msg.get("content", "")
            if isinstance(content, str):
                if "SCAN_RESULT" in content or "market scan" in content.lower():
                    scan_results.append(msg)
                elif "TRADE_UPDATE" in content or "position" in content.lower():
                    trade_updates.append(msg)
                elif "MARKET_DATA" in content or "ohlc" in content.lower():
                    market_data.append(msg)
                else:
                    other.append(msg)
            else:
                other.append(msg)

        # Compress: keep last 5 scan results, summarize the rest
        compressed_middle = []

        if len(scan_results) > 5:
            old_scans = scan_results[:-5]
            recent_scans = scan_results[-5:]

            # Summarize old scans
            summary = self._summarize_scans(old_scans)
            compressed_middle.append({
                "role": "system",
                "content": f"[Compressed] {len(old_scans)} earlier scans: {summary}",
            })
            compressed_middle.extend(recent_scans)
        else:
            compressed_middle.extend(scan_results)

        # Keep all trade updates (don't compress these)
        compressed_middle.extend(trade_updates)

        # Drop all but last 3 market data snapshots
        if len(market_data) > 3:
            compressed_middle.append({
                "role": "system",
                "content": f"[Compressed] {len(market_data) - 3} market data snapshots dropped",
            })
            compressed_middle.extend(market_data[-3:])
        else:
            compressed_middle.extend(market_data)

        # Keep other messages
        compressed_middle.extend(other)

        result = messages[:self.protect_first_n] + compressed_middle + messages[-self.protect_last_n:]

        logger.info(
            f"Compressed context: {len(messages)} → {len(result)} messages "
            f"(compression #{self.compression_count})"
        )
        return result

    def _summarize_scans(self, scans: List[Dict]) -> str:
        """Create a summary string from old scan results."""
        if not scans:
            return ""

        symbols_seen = set()
        setups_found = 0
        for msg in scans:
            content = msg.get("content", "")
            if isinstance(content, str):
                if "setup" in content.lower():
                    setups_found += 1
                # Extract symbol mentions
                for sym in ("XAUUSD", "EURUSD", "GBPUSD"):
                    if sym in content:
                        symbols_seen.add(sym)

        return (
            f"{len(scans)} scans across {', '.join(symbols_seen) or 'multiple symbols'}, "
            f"{setups_found} setups detected"
        )

    def on_session_start(self, session_id: str, **kwargs) -> None:
        """Initialize for a new trading session."""
        self._session_id = session_id
        self._scan_count = 0
        self.on_session_reset()

    def on_session_end(self, session_id: str,
                       messages: List[Dict[str, Any]] = None) -> None:
        """Log final stats for the session."""
        logger.info(
            f"Session {session_id} ended: {self.compression_count} compressions, "
            f"{self._scan_count} scans"
        )

    def tick_scan(self):
        """Call after each market scan to track scan count."""
        self._scan_count += 1


def create_context_engine(engine_type: str = "trading",
                          context_length: int = 128_000,
                          **kwargs) -> ContextEngine:
    """
    Factory function to create a context engine.

    Args:
        engine_type: "trading" (default), or a custom class
        context_length: Model context window size
    """
    if engine_type == "trading":
        return TradingContextCompressor(
            context_length=context_length, **kwargs
        )
    else:
        raise ValueError(f"Unknown context engine type: {engine_type}")
