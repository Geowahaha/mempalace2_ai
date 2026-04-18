from __future__ import annotations

import tempfile
import time
import unittest
from pathlib import Path

from trading_ai.core.memory import MemoryEngine, MemoryNote, MemoryRecord


class MemoryTests(unittest.TestCase):
    def test_recall_similar_trades_excludes_notes(self) -> None:
        tmp = tempfile.mkdtemp()
        memory = MemoryEngine(
            persist_path=Path(tmp) / "chroma",
            collection_name="test_trading_experiences",
        )
        features = {
            "symbol": "XAUUSD",
            "session": "NY",
            "volatility": "HIGH",
            "trend_direction": "DOWN",
        }
        memory.store_memory(
            MemoryRecord(
                market={"symbol": "XAUUSD"},
                features=features,
                decision={"action": "SELL", "confidence": 0.7, "reason": "trade journal"},
                result={"pnl": 1.25},
                score=1,
                created_ts=time.time(),
            )
        )
        memory.store_note(
            MemoryNote(
                title="Execution failure",
                content="Failed to execute SELL because market was closed.",
                wing="execution",
                hall="hall_events",
                room="execution:sell:xauusd",
                note_type="execution_failure",
                hall_type="hall_events",
                symbol="XAUUSD",
                session="NY",
                importance=0.9,
                source="execution_service",
            )
        )

        hits = memory.recall_similar_trades(features, symbol="XAUUSD", top_k=5)

        self.assertEqual(len(hits), 1)
        self.assertEqual(hits[0].metadata.get("memory_type"), "trade_journal")

    def test_wake_up_context_excludes_execution_failure_notes(self) -> None:
        tmp = tempfile.mkdtemp()
        memory = MemoryEngine(
            persist_path=Path(tmp) / "chroma",
            collection_name="test_trading_experiences",
        )
        memory.store_memory(
            MemoryRecord(
                market={"symbol": "XAUUSD"},
                features={
                    "symbol": "XAUUSD",
                    "session": "NY",
                    "volatility": "HIGH",
                    "trend_direction": "UP",
                },
                decision={"action": "BUY", "confidence": 0.72, "reason": "winner"},
                result={"pnl": 2.1},
                score=1,
                created_ts=time.time(),
            )
        )
        memory.store_note(
            MemoryNote(
                title="Execution failure",
                content="Failed to execute BUY for XAUUSD.",
                wing="execution",
                hall="hall_events",
                room="execution:buy:xauusd",
                note_type="execution_failure",
                hall_type="hall_events",
                symbol="XAUUSD",
                session="NY",
                importance=0.9,
                source="execution_service",
            )
        )

        context = memory.build_wake_up_context(symbol="XAUUSD", session="NY")

        self.assertIn("L1 market memory for XAUUSD in NY", context)
        self.assertNotIn("Execution failure", context)
        self.assertNotIn("execution_failure", context)


if __name__ == "__main__":
    unittest.main()
