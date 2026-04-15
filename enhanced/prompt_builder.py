"""
System Prompt Assembly — adapted from hermes-agent's agent/prompt_builder.py.

Builds dynamic system prompts for LLM-powered trading analysis.
Injects: trading memory, active skills, portfolio state, risk parameters.

Key hermes-agent concepts adapted:
  - build_system_prompt(): modular prompt assembly
  - Context file scanning with injection defense
  - _CONTEXT_THREAT_PATTERNS for prompt injection detection
  - Fenced context blocks (<trade-memory>, <trading-skills>)
"""

import json
import logging
import re
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Set

logger = logging.getLogger("mempalace2.enhanced.prompt")


# ── Prompt Injection Defense ─────────────────────────────
# Adapted from hermes-agent's _CONTEXT_THREAT_PATTERNS

_CONTEXT_THREAT_PATTERNS = [
    (r'ignore\s+(previous|all|above|prior)\s+instructions', "prompt_injection"),
    (r'do\s+not\s+tell\s+the\s+user', "deception_hide"),
    (r'system\s+prompt\s+override', "sys_prompt_override"),
    (r'disregard\s+(your|all|any)\s+(instructions|rules|guidelines)', "disregard_rules"),
    (r'act\s+as\s+(if|though)\s+you\s+(have\s+no|don\'t\s+have)\s+(restrictions|limits|rules)', "bypass_restrictions"),
    (r'<!--[^>]*(?:ignore|override|system|secret|hidden)[^>]*-->', "html_comment_injection"),
    (r'curl\s+[^\n]*\$\{?\w*(KEY|TOKEN|SECRET|PASSWORD|CREDENTIAL|API)', "exfil_curl"),
]


def scan_context_for_injection(content: str, source: str = "unknown") -> str:
    """
    Scan context content for prompt injection patterns.

    Returns sanitized content or a blocked warning if injection detected.
    Adapted from hermes-agent's _scan_context_content.
    """
    if not content:
        return content

    findings = []
    for pattern, pid in _CONTEXT_THREAT_PATTERNS:
        if re.search(pattern, content, re.IGNORECASE):
            findings.append(pid)

    if findings:
        logger.warning("Context source '%s' blocked: %s", source, ", ".join(findings))
        return (
            f"[BLOCKED: {source} contained potential prompt injection "
            f"({', '.join(findings)}). Content not loaded.]"
        )

    return content


# ── Prompt Building ──────────────────────────────────────

TRADING_IDENTITY = (
    "You are an intelligent XAUUSD (Gold) trading analyst. You analyze market data, "
    "recall relevant patterns from your trading memory, apply learned skills, and make "
    "trade decisions with proper risk management. You are self-improving: you learn from "
    "every trade and refine your strategies over time."
)

RISK_GUIDANCE = (
    "## Risk Management Rules\n"
    "- Never risk more than 2% of equity on a single trade\n"
    "- Always use stop losses — no exceptions\n"
    "- Minimum risk:reward ratio of 1.5:1 for entry\n"
    "- Maximum portfolio heat: 6% total risk across all positions\n"
    "- Circuit breaker: pause trading after 3 consecutive losses\n"
    "- Respect the Kelly Criterion for position sizing\n"
)

ANALYSIS_GUIDANCE = (
    "## Analysis Approach\n"
    "- Check your trading memory for similar past setups before analyzing\n"
    "- Apply relevant learned skills when conditions match\n"
    "- Show your reasoning step by step\n"
    "- Rate your confidence honestly (0-100)\n"
    "- If memory suggests a pattern has low win rate, reduce position or skip\n"
    "- Log what you learned from each analysis for future improvement\n"
)

MEMORY_GUIDANCE = (
    "## Memory Usage\n"
    "- You have persistent memory across sessions\n"
    "- Patterns: check recalled patterns (inside <trade-memory>) for historical win rates\n"
    "- Lessons: heed learned mistakes and heuristics\n"
    "- Skills: consult active skills (inside <trading-skills>) for proven strategies\n"
    "- After each trade, store new patterns and lessons for next time\n"
)

TRADING_TOOLS_GUIDANCE = (
    "## Tool Use\n"
    "- Use market_data tools to get current prices and indicators\n"
    "- Use technical analysis tools for deeper indicator computation\n"
    "- Use risk_engine for position sizing and risk:reward calculation\n"
    "- Use memory tools to store and recall patterns and lessons\n"
    "- Always verify data freshness before making decisions\n"
)


class PromptBuilder:
    """
    Builds dynamic system prompts for LLM-powered trading analysis.

    Assembles prompt sections modularly based on current state:
      1. Identity & risk rules (always)
      2. Portfolio state (if available)
      3. Trading memory context (if memory system available)
      4. Active skills context (if skills manager available)
      5. Market regime context (if available)
      6. Analysis and tool guidance (always)
    """

    def __init__(self, memory=None, skills_manager=None, state_store=None):
        self.memory = memory
        self.skills_manager = skills_manager
        self.state_store = state_store

    def build_system_prompt(self,
                            portfolio: Dict = None,
                            market_context: Dict = None,
                            session_context: Dict = None) -> str:
        """
        Build a complete system prompt for the trading analyst.

        Args:
            portfolio: Current portfolio state (equity, positions, P&L)
            market_context: Current market conditions (price, regime, volatility)
            session_context: Session metadata (session_id, scan count, etc.)
        """
        sections = []

        # 1. Identity
        sections.append(TRADING_IDENTITY)

        # 2. Portfolio state
        if portfolio:
            sections.append(self._build_portfolio_section(portfolio))

        # 3. Market regime
        if market_context:
            sections.append(self._build_market_section(market_context))

        # 4. Trading memory (patterns + lessons)
        if self.memory:
            memory_block = self.memory.build_system_prompt_block()
            if memory_block:
                sections.append(memory_block)

        # 5. Active skills
        if self.skills_manager and market_context:
            skills_block = self.skills_manager.build_skills_context_block(
                context=market_context
            )
            if skills_block:
                sections.append(skills_block)

        # 6. Risk & analysis guidance
        sections.append(RISK_GUIDANCE)
        sections.append(ANALYSIS_GUIDANCE)
        sections.append(MEMORY_GUIDANCE)
        sections.append(TRADING_TOOLS_GUIDANCE)

        # 7. Session context
        if session_context:
            sections.append(self._build_session_section(session_context))

        return "\n\n".join(sections)

    def build_analysis_context(self, symbol: str, setup_type: str,
                                direction: str, indicators: Dict = None,
                                market_context: Dict = None) -> str:
        """
        Build the context block for a specific analysis request.

        Combines: memory recall + skill matching + indicator data.
        Returns a fenced context block for injection into the prompt.
        """
        parts = []

        # Memory context
        if self.memory:
            memory_ctx = self.memory.build_context_for_analysis(
                symbol, setup_type, direction
            )
            if memory_ctx:
                parts.append(memory_ctx)

        # Skills context
        if self.skills_manager:
            ctx = {
                "symbol": symbol,
                "setup_type": setup_type,
                "direction": direction,
            }
            skills_ctx = self.skills_manager.build_skills_context_block(context=ctx)
            if skills_ctx:
                parts.append(skills_ctx)

        # Indicator snapshot
        if indicators:
            parts.append("<indicators>")
            parts.append(f"Symbol: {symbol} | Direction: {direction} | Setup: {setup_type}")
            for key, value in indicators.items():
                parts.append(f"  {key}: {value}")
            parts.append("</indicators>")

        # Market context
        if market_context:
            parts.append("<market-context>")
            for key, value in market_context.items():
                parts.append(f"  {key}: {value}")
            parts.append("</market-context>")

        if not parts:
            return ""

        return (
            "## Analysis Context\n"
            + "\n".join(parts)
        )

    def _build_portfolio_section(self, portfolio: Dict) -> str:
        """Build the portfolio state section."""
        lines = ["## Current Portfolio State"]
        lines.append(f"  Equity: ${portfolio.get('total_equity', 0):,.2f}")
        lines.append(f"  Available: ${portfolio.get('available_balance', 0):,.2f}")
        lines.append(f"  Open Positions: {portfolio.get('open_positions', 0)}")
        lines.append(f"  Total Risk: {portfolio.get('total_risk_pct', 0):.1f}%")
        lines.append(f"  Daily P&L: ${portfolio.get('daily_pnl', 0):,.2f}")
        lines.append(f"  Win Rate: {portfolio.get('win_rate', 0):.1%}")
        return "\n".join(lines)

    def _build_market_section(self, market_context: Dict) -> str:
        """Build the market regime section."""
        lines = ["## Market Conditions"]
        if "price" in market_context:
            lines.append(f"  Price: {market_context['price']}")
        if "regime" in market_context:
            lines.append(f"  Regime: {market_context['regime']}")
        if "volatility" in market_context:
            lines.append(f"  Volatility: {market_context['volatility']}")
        if "trend" in market_context:
            lines.append(f"  Trend: {market_context['trend']}")
        return "\n".join(lines)

    def _build_session_section(self, session: Dict) -> str:
        """Build session metadata section."""
        lines = ["## Session Info"]
        if "session_id" in session:
            lines.append(f"  Session: {session['session_id']}")
        if "scan_count" in session:
            lines.append(f"  Scans this session: {session['scan_count']}")
        if "duration" in session:
            lines.append(f"  Duration: {session['duration']}")
        return "\n".join(lines)

    def scan_and_sanitize(self, content: str, source: str = "user_input") -> str:
        """
        Sanitize content before injection into prompt.
        Uses hermes-agent's injection detection patterns.
        """
        return scan_context_for_injection(content, source)
