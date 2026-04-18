"""
openclaw/agents/base.py — BaseAgent interface for all Dexter Pro autonomous agents.

Every agent implements run(context) -> AgentResult.
Findings  = observations about current system state (read-only analysis).
Proposals = parameter change requests to route through PTS safety gate.
"""

import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

logger = logging.getLogger(__name__)


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


@dataclass
class AgentResult:
    agent_name: str
    status: str  # ok | skip | error
    findings: dict = field(default_factory=dict)
    proposals: list = field(default_factory=list)  # list[dict] — each proposal routed to PTS
    confidence: float = 0.0
    generated_at: str = field(default_factory=_utc_now_iso)
    error: str = ""

    def is_ok(self) -> bool:
        return self.status == "ok"

    def has_proposals(self) -> bool:
        return bool(self.proposals)


class BaseAgent:
    """Abstract base for all Dexter Pro autonomous agents."""

    name: str = "base"

    def run(self, context: dict) -> AgentResult:
        """Execute agent logic. Must be overridden by subclass."""
        raise NotImplementedError(f"{self.__class__.__name__}.run() not implemented")

    # ── helpers ──────────────────────────────────────────────────────────────

    def _ok(
        self,
        findings: dict | None = None,
        proposals: list | None = None,
        confidence: float = 0.0,
    ) -> AgentResult:
        return AgentResult(
            agent_name=self.name,
            status="ok",
            findings=findings or {},
            proposals=proposals or [],
            confidence=confidence,
        )

    def _skip(self, reason: str = "") -> AgentResult:
        return AgentResult(
            agent_name=self.name,
            status="skip",
            findings={"skip_reason": reason},
        )

    def _error(self, err: str = "") -> AgentResult:
        logger.warning("[%s] agent error: %s", self.name, err)
        return AgentResult(
            agent_name=self.name,
            status="error",
            error=err,
        )

    def _safe_run(self, context: dict) -> AgentResult:
        """Call run() with error guard — never crashes the conductor."""
        try:
            return self.run(context)
        except Exception as exc:
            return self._error(str(exc))
