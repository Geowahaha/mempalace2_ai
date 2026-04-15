"""
Trade Decision Trajectory Logger — adapted from hermes-agent's agent/trajectory.py.

Logs every trade decision as a structured trajectory in JSONL format for
fine-tuning data generation. Each trajectory captures the full decision
cycle: market scan → analysis → risk check → execution/close.

Key concepts adapted from hermes-agent:
  - save_trajectory(): JSONL append in ShareGPT format
  - convert_scratchpad_to_think(): reasoning block conversion
  - Trajectory metadata for provenance tracking

Usage:
    logger = TrajectoryLogger(output_dir="trajectories/")
    tid = logger.start_trajectory(session_id, symbol, direction)
    logger.add_step(tid, "scan", {"setup_type": "supertrend_flip", ...})
    logger.add_step(tid, "analysis", {"reasoning": "...", "confidence": 75})
    logger.add_step(tid, "risk_check", {"position_size": 0.02, "rr": 3.5})
    logger.finalize(tid, "executed", outcome={"pnl_pct": 1.2})

    # Export as ShareGPT for fine-tuning
    logger.export_sharegpt("training_data.jsonl")
"""

import json
import logging
import os
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

logger = logging.getLogger("mempalace2.trajectories")


def convert_scratchpad_to_think(content: str) -> str:
    """Convert <REASONING_SCRATCHPAD> tags to <think> tags for fine-tuning format."""
    if not content or "<REASONING_SCRATCHPAD>" not in content:
        return content
    return (
        content
        .replace("<REASONING_SCRATCHPAD>", "<think>")
        .replace("</REASONING_SCRATCHPAD>", "</think>")
    )


def has_incomplete_scratchpad(content: str) -> bool:
    """Check for opening <REASONING_SCRATCHPAD> without closing tag."""
    if not content:
        return False
    return (
        "<REASONING_SCRATCHPAD>" in content
        and "</REASONING_SCRATCHPAD>" not in content
    )


class Trajectory:
    """A single trade decision trajectory with step tracking."""

    def __init__(self, trajectory_id: str, session_id: str,
                 symbol: str, direction: str):
        self.id = trajectory_id
        self.session_id = session_id
        self.symbol = symbol
        self.direction = direction
        self.steps: List[Dict[str, Any]] = []
        self.start_time = time.time()
        self.end_time: Optional[float] = None
        self.status: str = "in_progress"  # in_progress | executed | rejected | failed
        self.outcome: Optional[Dict] = None

    def add_step(self, step_type: str, data: Dict[str, Any]):
        """Add a decision step to the trajectory."""
        step = {
            "type": step_type,
            "timestamp": time.time(),
            "timestamp_iso": datetime.now(timezone.utc).isoformat(),
            "data": data,
        }
        self.steps.append(step)

    def finalize(self, status: str, outcome: Dict = None):
        """Finalize the trajectory with outcome."""
        self.status = status
        self.end_time = time.time()
        self.outcome = outcome or {}

    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": self.id,
            "session_id": self.session_id,
            "symbol": self.symbol,
            "direction": self.direction,
            "steps": self.steps,
            "start_time": self.start_time,
            "start_time_iso": datetime.fromtimestamp(
                self.start_time, tz=timezone.utc
            ).isoformat(),
            "end_time": self.end_time,
            "duration_s": (self.end_time - self.start_time) if self.end_time else None,
            "status": self.status,
            "outcome": self.outcome,
        }

    def to_sharegpt(self) -> List[Dict[str, str]]:
        """
        Convert trajectory to ShareGPT conversation format for fine-tuning.

        ShareGPT format:
        [
            {"role": "system", "content": "..."},
            {"role": "user", "content": "..."},
            {"role": "assistant", "content": "..."}
        ]
        """
        conversations = []

        # System prompt: describe the trading agent role
        conversations.append({
            "role": "system",
            "content": (
                "You are a self-improving XAUUSD trading analyst. "
                "Analyze market data, recall relevant patterns from memory, "
                "apply learned skills, and make trade decisions with proper "
                "risk management. Show your reasoning step by step."
            ),
        })

        # Build user message from trajectory steps
        scan_data = {}
        analysis_data = {}
        for step in self.steps:
            if step["type"] == "scan":
                scan_data = step["data"]
            elif step["type"] == "analysis":
                analysis_data = step["data"]

        user_parts = [
            f"Symbol: {self.symbol}",
            f"Direction: {self.direction}",
            f"Setup: {scan_data.get('setup_type', 'unknown')}",
            f"Price: {scan_data.get('price', 'N/A')}",
            f"Indicators: {json.dumps(scan_data.get('indicators', {}))}",
        ]
        if scan_data.get("market_context"):
            user_parts.append(
                f"Market Context: {json.dumps(scan_data['market_context'])}"
            )
        if analysis_data.get("memory_context"):
            user_parts.append(f"Memory: {analysis_data['memory_context']}")

        conversations.append({
            "role": "user",
            "content": "\n".join(user_parts),
        })

        # Build assistant response from analysis + decision
        reasoning = ""
        decision = ""
        for step in self.steps:
            if step["type"] == "analysis":
                reasoning = step["data"].get("reasoning", "")
            elif step["type"] == "risk_check":
                decision = step["data"].get("reasoning", "")
                if step["data"].get("position_size"):
                    decision += f"\nPosition size: {step['data']['position_size']:.2%}"
            elif step["type"] == "decision":
                decision = step["data"].get("reasoning", "")

        # Convert any scratchpads
        reasoning = convert_scratchpad_to_think(reasoning)
        if reasoning and not reasoning.startswith("<think>"):
            reasoning = f"<think>{reasoning}"

        response_parts = []
        if reasoning:
            response_parts.append(reasoning)
        if decision:
            response_parts.append(decision)

        # Add outcome
        if self.outcome:
            outcome_str = json.dumps(self.outcome, default=str)
            response_parts.append(f"\nOutcome: {outcome_str}")

        conversations.append({
            "role": "assistant",
            "content": "\n\n".join(response_parts) if response_parts else "No trade.",
        })

        return conversations


class TrajectoryLogger:
    """
    Trade decision trajectory logger.

    Manages active trajectories, persists to JSONL, and exports
    ShareGPT format for fine-tuning pipelines.
    """

    def __init__(self, output_dir: str = "trajectories/",
                 model: str = "mempalace2-trading"):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.model = model
        self._active: Dict[str, Trajectory] = {}
        self._completed: List[str] = []
        self._stats = {
            "total": 0,
            "executed": 0,
            "rejected": 0,
            "failed": 0,
        }

    # ── Lifecycle ───────────────────────────────────────

    def start_trajectory(self, session_id: str, symbol: str,
                         direction: str) -> str:
        """Start a new trajectory. Returns trajectory_id."""
        tid = uuid.uuid4().hex[:12]
        self._active[tid] = Trajectory(tid, session_id, symbol, direction)
        self._stats["total"] += 1
        logger.debug(f"Trajectory started: {tid} ({symbol} {direction})")
        return tid

    def add_step(self, trajectory_id: str, step_type: str,
                 data: Dict[str, Any]):
        """Add a step to an active trajectory."""
        traj = self._active.get(trajectory_id)
        if not traj:
            logger.warning(f"Trajectory {trajectory_id} not found, skipping step")
            return
        traj.add_step(step_type, data)

    def finalize(self, trajectory_id: str, status: str,
                 outcome: Dict = None):
        """Finalize a trajectory and persist to JSONL."""
        traj = self._active.pop(trajectory_id, None)
        if not traj:
            logger.warning(f"Trajectory {trajectory_id} not found for finalization")
            return

        traj.finalize(status, outcome)
        completed = status in ("executed", "closed")

        # Update stats
        if status == "executed":
            self._stats["executed"] += 1
        elif status == "rejected":
            self._stats["rejected"] += 1
        else:
            self._stats["failed"] += 1

        # Save to JSONL
        filename = "trajectory_samples.jsonl" if completed else "failed_trajectories.jsonl"
        filepath = self.output_dir / filename

        entry = {
            "conversations": traj.to_sharegpt(),
            "metadata": traj.to_dict(),
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "model": self.model,
            "completed": completed,
        }

        try:
            with open(filepath, "a", encoding="utf-8") as f:
                f.write(json.dumps(entry, ensure_ascii=False) + "\n")
            self._completed.append(trajectory_id)
            logger.info(
                f"Trajectory {trajectory_id} saved → {filename} "
                f"({traj.status}, {len(traj.steps)} steps)"
            )
        except Exception as e:
            logger.warning(f"Failed to save trajectory {trajectory_id}: {e}")

    # ── Batch Export ────────────────────────────────────

    def export_sharegpt(self, output_file: str = None,
                        include_failed: bool = False) -> int:
        """
        Export all trajectories as a consolidated ShareGPT JSONL file.

        Returns the number of trajectories exported.
        """
        if output_file is None:
            output_file = str(self.output_dir / "sharegpt_export.jsonl")

        count = 0
        files_to_read = ["trajectory_samples.jsonl"]
        if include_failed:
            files_to_read.append("failed_trajectories.jsonl")

        with open(output_file, "w", encoding="utf-8") as out:
            for fname in files_to_read:
                filepath = self.output_dir / fname
                if not filepath.exists():
                    continue
                with open(filepath, "r", encoding="utf-8") as f:
                    for line in f:
                        line = line.strip()
                        if not line:
                            continue
                        try:
                            entry = json.loads(line)
                            sgpt = {
                                "conversations": entry.get("conversations", []),
                                "source": "mempalace2-trading",
                                "model": entry.get("model", self.model),
                            }
                            out.write(json.dumps(sgpt, ensure_ascii=False) + "\n")
                            count += 1
                        except json.JSONDecodeError:
                            continue

        logger.info(f"Exported {count} trajectories to {output_file}")
        return count

    # ── Stats ───────────────────────────────────────────

    def get_stats(self) -> Dict[str, Any]:
        """Get trajectory logging statistics."""
        return {
            **self._stats,
            "active": len(self._active),
            "completed": len(self._completed),
            "output_dir": str(self.output_dir),
        }

    def flush_active(self):
        """Force-finalize all active trajectories as 'failed'."""
        for tid in list(self._active.keys()):
            self.finalize(tid, "failed", {"reason": "session_ended"})
