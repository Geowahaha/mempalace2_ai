from __future__ import annotations

import argparse
import asyncio
import json
import logging
import sqlite3
import uuid
from collections import Counter, deque
from dataclasses import asdict, dataclass
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional
from zoneinfo import ZoneInfo

from trading_ai.config import Settings, load_settings
from trading_ai.core.agent import Decision, TradingAgent, apply_confidence_floor
from trading_ai.core.execution import (
    Action,
    Broker,
    CloseResult,
    ExecutionService,
    MarketSnapshot,
    OpenPosition,
    TradeResult,
)
from trading_ai.core.market_features import extract_features, infer_setup_tag
from trading_ai.core.memory import MemoryNote, MemoryRecord, RecallHit
from trading_ai.core.patterns import (
    PatternBook,
    apply_pattern_confidence_boost,
    build_pattern_analysis_for_prompt,
    passes_pattern_execution_gate,
)
from trading_ai.core.performance import PerformanceTracker
from trading_ai.core.position_manager import (
    assess_entry_candidate,
    evaluate_open_position,
    write_monitor_snapshot,
)
from trading_ai.core.self_improvement import SelfImprovementEngine
from trading_ai.core.skillbook import SkillBook, SkillMatch, build_team_brief
from trading_ai.core.strategy import RiskManager, evaluate_outcome
from trading_ai.core.strategy_evolution import StrategyRegistry, build_strategy_key
from trading_ai.main import _cap_trade_volume_for_exposure, _hard_market_filters, _journal_structured
from trading_ai.utils.logger import get_logger

log = get_logger(__name__)

UTC = timezone.utc
TIMEFRAME_SECONDS = {"5m": 300, "15m": 900, "1h": 3600}
SOURCE_PRIORITY = {"spot_ticks": 3, "depth_level0": 2, "candle_db": 1}
SOURCE_POLICIES = {"real_first", "real_only", "candle_only"}
FIXED_TZ_OFFSETS = {
    "UTC": timezone.utc,
    "Etc/UTC": timezone.utc,
    "Asia/Bangkok": timezone(timedelta(hours=7)),
}


@dataclass(slots=True)
class HistoricalBar:
    ts_unix: float
    ts_utc: str
    open: float
    high: float
    low: float
    close: float
    bid: float
    ask: float
    spread: float
    volume: float
    source: str

    def as_market_snapshot(self, symbol: str) -> MarketSnapshot:
        return MarketSnapshot(
            symbol=symbol,
            bid=self.bid,
            ask=self.ask,
            mid=self.close,
            spread=self.spread,
            ts_unix=self.ts_unix,
            extra={
                "venue": "historical_replay",
                "bar_source": self.source,
                "bar_open": self.open,
                "bar_high": self.high,
                "bar_low": self.low,
                "bar_close": self.close,
                "bar_volume": self.volume,
            },
        )


@dataclass(slots=True)
class GapSummary:
    start_utc: str
    end_utc: str
    missing_bars: int
    duration_minutes: float


@dataclass(slots=True)
class BacktestResult:
    run_dir: Path
    report_path: Path
    summary_path: Path
    report: Dict[str, Any]


class BacktestFallbackLLM:
    async def complete_json(
        self,
        *,
        system: str,
        user: str,
        temperature: float = 0.2,
        json_schema: Dict[str, Any] | None = None,
    ) -> Dict[str, Any]:
        raise RuntimeError("backtest_heuristic_mode")


class HistoricalBacktestBroker(Broker):
    def __init__(self, symbol: str, *, initial_equity: float) -> None:
        self._symbol = symbol
        self._current_bar: Optional[HistoricalBar] = None
        self._realized_pnl = 0.0
        self._initial_equity = float(initial_equity)

    def set_bar(self, bar: HistoricalBar) -> None:
        self._current_bar = bar

    def get_account_equity(self) -> float:
        return float(self._initial_equity + self._realized_pnl)

    def _require_bar(self) -> HistoricalBar:
        if self._current_bar is None:
            raise RuntimeError("HistoricalBacktestBroker has no active bar")
        return self._current_bar

    async def get_market_data(self, symbol: str) -> MarketSnapshot:
        if symbol != self._symbol:
            raise ValueError(f"Backtest broker only loaded for {self._symbol}, got {symbol}")
        return self._require_bar().as_market_snapshot(symbol)

    async def execute_trade(
        self,
        *,
        symbol: str,
        side: Action,
        volume: float,
        decision_reason: str,
        dry_run: bool,
    ) -> TradeResult:
        if side == "HOLD":
            raise ValueError("HOLD should not reach HistoricalBacktestBroker.execute_trade")
        bar = self._require_bar()
        entry = bar.ask if side == "BUY" else bar.bid
        order_id = f"bt_{uuid.uuid4().hex[:12]}"
        return TradeResult(
            order_id=order_id,
            symbol=symbol,
            side=side,
            volume=float(volume),
            entry_price=entry,
            executed=not dry_run,
            dry_run=bool(dry_run),
            message=f"historical_fill:{bar.source}",
            position_id=order_id,
            ts_unix=bar.ts_unix,
            raw_response={"reason": decision_reason, "bar_source": bar.source},
        )

    async def close_position(
        self,
        *,
        symbol: str,
        position: OpenPosition,
        reason: str,
        dry_run: bool,
    ) -> CloseResult:
        bar = self._require_bar()
        exit_price = bar.bid if position.side == "BUY" else bar.ask
        if not dry_run:
            sign = 1.0 if position.side == "BUY" else -1.0
            self._realized_pnl += sign * (exit_price - position.entry_price) * position.volume
        return CloseResult(
            symbol=symbol,
            side=position.side,
            volume=position.volume,
            exit_price=exit_price,
            closed=True,
            dry_run=bool(dry_run),
            message=f"historical_close:{bar.source}",
            position_id=position.position_id,
            ts_unix=bar.ts_unix,
            raw_response={"reason": reason, "bar_source": bar.source},
        )


class ReplayMemory:
    def __init__(self, *, score_weight: float = 0.35) -> None:
        self._score_weight = max(0.0, min(1.0, float(score_weight)))
        self._records: list[dict[str, Any]] = []
        self._notes: list[dict[str, Any]] = []

    def count(self) -> int:
        return len(self._records)

    def store_memory(self, record: MemoryRecord, *, extra_metadata: Optional[Dict[str, Any]] = None) -> str:
        doc_id = str(uuid.uuid4())
        features = dict(record.features or {})
        room = str(record.room or record.strategy_key or f"{record.setup_tag}:{features.get('trend_direction')}:{features.get('volatility')}")
        meta = {
            "id": doc_id,
            "symbol": str(record.market.get("symbol") or features.get("symbol") or ""),
            "session": str(features.get("session") or ""),
            "volatility": str(features.get("volatility") or ""),
            "trend_direction": str(features.get("trend_direction") or ""),
            "setup_tag": str(record.setup_tag or ""),
            "strategy_key": str(record.strategy_key or ""),
            "outcome_score": float(record.score),
            "confidence": float((record.decision or {}).get("confidence") or 0.0),
            "action": str((record.decision or {}).get("action") or ""),
            "created_ts": float(record.created_ts),
            "room": room,
            "memory_type": "trade_journal",
        }
        if extra_metadata:
            meta.update(dict(extra_metadata))
        self._records.append({"id": doc_id, "document": record.to_document(), "metadata": meta})
        return doc_id

    def store_note(self, note: MemoryNote) -> str:
        doc_id = str(uuid.uuid4())
        self._notes.append(
            {
                "id": doc_id,
                "title": note.title,
                "content": note.content,
                "room": note.room,
                "symbol": note.symbol,
                "session": note.session,
                "setup_tag": note.setup_tag,
                "strategy_key": note.strategy_key,
                "importance": float(note.importance),
                "note_type": note.note_type,
                "created_ts": float(note.created_ts),
            }
        )
        return doc_id

    def recall_similar_trades(
        self,
        features: Dict[str, Any],
        *,
        symbol: str,
        top_k: int = 5,
    ) -> List[RecallHit]:
        hits: List[RecallHit] = []
        session = str(features.get("session") or "")
        volatility = str(features.get("volatility") or "")
        trend = str(features.get("trend_direction") or "")
        for row in self._records:
            meta = dict(row["metadata"])
            if str(meta.get("symbol") or "") != symbol:
                continue
            similarity = 0.0
            if str(meta.get("session") or "") == session:
                similarity += 0.45
            if str(meta.get("volatility") or "") == volatility:
                similarity += 0.25
            if str(meta.get("trend_direction") or "") == trend:
                similarity += 0.25
            if str(meta.get("setup_tag") or "") == str(features.get("setup_tag") or ""):
                similarity += 0.05
            similarity = max(0.0, min(1.0, similarity))
            mem_score = float(meta.get("outcome_score") or 0.0)
            weighted = (1.0 - self._score_weight) * similarity + self._score_weight * ((mem_score + 1.0) / 2.0)
            hits.append(
                RecallHit(
                    id=str(row["id"]),
                    document=str(row["document"]),
                    similarity=similarity,
                    weighted_score=weighted,
                    metadata=meta,
                )
            )
        hits.sort(key=lambda item: item.weighted_score, reverse=True)
        return hits[:top_k]

    def build_wake_up_context(
        self,
        *,
        symbol: str,
        session: Optional[str] = None,
        top_k: int = 6,
        note_top_k: int = 6,
    ) -> str:
        rows = [
            row for row in self._records
            if str(row["metadata"].get("symbol") or "") == symbol
            and (session is None or str(row["metadata"].get("session") or "") == session)
        ]
        rows.sort(
            key=lambda row: (
                float(row["metadata"].get("outcome_score") or 0.0),
                float(row["metadata"].get("created_ts") or 0.0),
            ),
            reverse=True,
        )
        lines = [f"L1 market memory for {symbol}" + (f" in {session}" if session else "")]
        if not rows:
            lines.append("no stored trade journals yet.")
        else:
            for idx, row in enumerate(rows[:top_k], start=1):
                meta = dict(row["metadata"])
                lines.append(
                    f"{idx}. room={meta.get('room')} score={meta.get('outcome_score')} "
                    f"action={meta.get('action')} trend={meta.get('trend_direction')} vol={meta.get('volatility')}"
                )
        notes = [
            row for row in self._notes
            if (not str(row.get("symbol") or "") or str(row.get("symbol") or "") == symbol)
            and (session is None or not str(row.get("session") or "") or str(row.get("session") or "") == session)
        ]
        notes.sort(key=lambda row: (float(row.get("importance") or 0.0), float(row.get("created_ts") or 0.0)), reverse=True)
        for note in notes[:note_top_k]:
            lines.append(
                f"note[{note.get('note_type')}|{note.get('importance')}]: {note.get('title')} :: {str(note.get('content') or '')[:160]}"
            )
        return "\n".join(lines)

    def get_room_guardrail(
        self,
        *,
        symbol: str,
        session: str,
        setup_tag: str,
        trend_direction: str,
        volatility: str,
        strategy_key: str = "",
    ) -> Dict[str, Any]:
        room_name = str(strategy_key or f"{setup_tag}:{trend_direction}:{volatility}" or "room:unknown")
        rows = [
            row for row in self._records
            if str(row["metadata"].get("room") or "") == room_name
            and str(row["metadata"].get("symbol") or "") == symbol
            and (not session or str(row["metadata"].get("session") or "") == session)
        ]
        wins = sum(1 for row in rows if float(row["metadata"].get("outcome_score") or 0.0) > 0)
        losses = sum(1 for row in rows if float(row["metadata"].get("outcome_score") or 0.0) < 0)
        total = len(rows)
        avg_pnl = 0.0
        if rows:
            pnls = []
            for row in rows:
                try:
                    body = json.loads(str(row["document"]))
                    pnls.append(float(((body.get("result") or {}).get("pnl") or 0.0)))
                except Exception:
                    continue
            avg_pnl = sum(pnls) / len(pnls) if pnls else 0.0
        notes = [
            row for row in self._notes
            if str(row.get("room") or "") == room_name
            and (not str(row.get("symbol") or "") or str(row.get("symbol") or "") == symbol)
            and (not session or not str(row.get("session") or "") or str(row.get("session") or "") == session)
        ]
        notes.sort(key=lambda row: (float(row.get("importance") or 0.0), float(row.get("created_ts") or 0.0)), reverse=True)
        blocked = total >= 3 and losses >= 3 and wins == 0
        caution = total >= 2 and losses > wins and avg_pnl < 0 and not blocked
        winner = total >= 2 and wins > losses and avg_pnl > 0
        opportunity = any(str(note.get("note_type") or "") == "opportunity_candidate" for note in notes)
        confidence_delta = 0.0
        if winner:
            confidence_delta += 0.05
        if opportunity:
            confidence_delta += 0.03
        if caution:
            confidence_delta -= 0.06
        if blocked:
            confidence_delta -= 0.15
        return {
            "room": room_name,
            "blocked": blocked,
            "caution": caution,
            "confidence_delta": round(confidence_delta, 4),
            "winner_room": {"name": room_name, "count": total, "wins": wins, "avg_pnl": avg_pnl} if winner else None,
            "danger_room": {"name": room_name, "count": total, "losses": losses, "avg_pnl": avg_pnl} if caution else None,
            "anti_pattern_room": {"name": room_name, "count": total, "losses": losses} if blocked else None,
            "opportunity_room": {"name": room_name, "count": len(notes)} if opportunity else None,
            "supporting_notes": notes[:10],
        }


def _parse_timestamp(raw: Any) -> Optional[datetime]:
    text = str(raw or "").strip()
    if not text:
        return None
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        dt = datetime.fromisoformat(text)
    except ValueError:
        for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
            try:
                dt = datetime.strptime(text, fmt)
                break
            except ValueError:
                continue
        else:
            return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=UTC)
    return dt.astimezone(UTC)


def _iso_utc(dt: datetime) -> str:
    return dt.astimezone(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _bucket_floor(dt: datetime, timeframe_sec: int) -> datetime:
    ts = int(dt.timestamp())
    floored = ts - (ts % timeframe_sec)
    return datetime.fromtimestamp(floored, tz=UTC)


def _date_window_to_utc(
    start_day: str,
    end_day: str,
    tz_name: str,
) -> tuple[datetime, datetime, Dict[str, Any]]:
    try:
        zone = ZoneInfo(tz_name)
    except Exception:
        zone = FIXED_TZ_OFFSETS.get(tz_name)
        if zone is None:
            raise
    start_local = datetime.combine(date.fromisoformat(start_day), time.min, tzinfo=zone)
    end_local = datetime.combine(date.fromisoformat(end_day), time.min, tzinfo=zone) + timedelta(days=1)
    start_utc = start_local.astimezone(UTC)
    end_utc = end_local.astimezone(UTC)
    return start_utc, end_utc, {
        "timezone": tz_name,
        "start_local": start_local.isoformat(),
        "end_local_exclusive": end_local.isoformat(),
        "start_local_weekday": start_local.strftime("%A"),
        "end_local_weekday": (end_local - timedelta(days=1)).strftime("%A"),
        "start_utc": _iso_utc(start_utc),
        "end_utc_exclusive": _iso_utc(end_utc),
    }


def _aggregate_quote_rows(
    rows: Iterable[tuple[datetime, float, float]],
    *,
    timeframe_sec: int,
    source: str,
) -> list[HistoricalBar]:
    buckets: Dict[datetime, Dict[str, Any]] = {}
    for dt, bid, ask in rows:
        if float(bid) <= 0.0 or float(ask) <= 0.0 or float(ask) < float(bid):
            continue
        bucket = _bucket_floor(dt, timeframe_sec)
        mid = (float(bid) + float(ask)) / 2.0
        spread = max(0.0, float(ask) - float(bid))
        current = buckets.get(bucket)
        if current is None:
            buckets[bucket] = {
                "open": mid,
                "high": mid,
                "low": mid,
                "close": mid,
                "bid": float(bid),
                "ask": float(ask),
                "spread": spread,
                "volume": 1.0,
            }
            continue
        current["high"] = max(float(current["high"]), mid)
        current["low"] = min(float(current["low"]), mid)
        current["close"] = mid
        current["bid"] = float(bid)
        current["ask"] = float(ask)
        current["spread"] = spread
        current["volume"] = float(current["volume"]) + 1.0

    out: list[HistoricalBar] = []
    for bucket, item in sorted(buckets.items(), key=lambda pair: pair[0]):
        out.append(
            HistoricalBar(
                ts_unix=bucket.timestamp(),
                ts_utc=_iso_utc(bucket),
                open=float(item["open"]),
                high=float(item["high"]),
                low=float(item["low"]),
                close=float(item["close"]),
                bid=float(item["bid"]),
                ask=float(item["ask"]),
                spread=max(1e-9, float(item["spread"])),
                volume=float(item["volume"]),
                source=source,
            )
        )
    return out


def _load_spot_tick_bars(
    conn: sqlite3.Connection,
    *,
    symbol: str,
    start_utc: datetime,
    end_utc: datetime,
    timeframe_sec: int,
) -> list[HistoricalBar]:
    cur = conn.cursor()
    rows = cur.execute(
        """
        select event_utc, bid, ask
        from ctrader_spot_ticks
        where symbol = ?
          and event_utc >= ?
          and event_utc < ?
        order by event_utc asc
        """,
        (symbol, _iso_utc(start_utc), _iso_utc(end_utc)),
    ).fetchall()
    samples: list[tuple[datetime, float, float]] = []
    for raw_ts, bid, ask in rows:
        dt = _parse_timestamp(raw_ts)
        if dt is None:
            continue
        bid_f = float(bid)
        ask_f = float(ask)
        if bid_f <= 0.0 or ask_f <= 0.0 or ask_f < bid_f:
            continue
        samples.append((dt, bid_f, ask_f))
    return _aggregate_quote_rows(samples, timeframe_sec=timeframe_sec, source="spot_ticks")


def _load_depth_bars(
    conn: sqlite3.Connection,
    *,
    symbol: str,
    start_utc: datetime,
    end_utc: datetime,
    timeframe_sec: int,
) -> list[HistoricalBar]:
    cur = conn.cursor()
    rows = cur.execute(
        """
        select event_utc, side, price
        from ctrader_depth_quotes
        where symbol = ?
          and level_index = 0
          and event_utc >= ?
          and event_utc < ?
        order by event_utc asc
        """,
        (symbol, _iso_utc(start_utc), _iso_utc(end_utc)),
    ).fetchall()
    samples: list[tuple[datetime, float, float]] = []
    current_ts = ""
    bid: Optional[float] = None
    ask: Optional[float] = None
    for raw_ts, side, price in rows:
        ts_text = str(raw_ts or "")
        if current_ts and ts_text != current_ts:
            dt = _parse_timestamp(current_ts)
            if dt is not None and bid is not None and ask is not None and bid > 0.0 and ask > 0.0 and ask >= bid:
                samples.append((dt, float(bid), float(ask)))
            bid = None
            ask = None
        current_ts = ts_text
        if str(side).lower() == "bid":
            bid = float(price)
        elif str(side).lower() == "ask":
            ask = float(price)
    if current_ts:
        dt = _parse_timestamp(current_ts)
        if dt is not None and bid is not None and ask is not None and bid > 0.0 and ask > 0.0 and ask >= bid:
            samples.append((dt, float(bid), float(ask)))
    return _aggregate_quote_rows(samples, timeframe_sec=timeframe_sec, source="depth_level0")


def _load_candle_bars(
    conn: sqlite3.Connection,
    *,
    symbol: str,
    tf: str,
    start_utc: datetime,
    end_utc: datetime,
    synthetic_spread: float,
) -> list[HistoricalBar]:
    cur = conn.cursor()
    rows = cur.execute(
        """
        select ts, open, high, low, close, coalesce(volume, 0.0)
        from candles
        where symbol = ?
          and tf = ?
        order by ts asc
        """,
        (symbol, tf),
    ).fetchall()
    out: list[HistoricalBar] = []
    for raw_ts, opn, high, low, close, volume in rows:
        dt = _parse_timestamp(raw_ts)
        if dt is None:
            continue
        if not (start_utc <= dt < end_utc):
            continue
        close_f = float(close)
        spread = max(1e-9, float(synthetic_spread))
        out.append(
            HistoricalBar(
                ts_unix=dt.timestamp(),
                ts_utc=_iso_utc(dt),
                open=float(opn),
                high=float(high),
                low=float(low),
                close=close_f,
                bid=close_f - spread / 2.0,
                ask=close_f + spread / 2.0,
                spread=spread,
                volume=float(volume),
                source="candle_db",
            )
        )
    return out


def merge_bars(*bar_sets: Iterable[HistoricalBar]) -> list[HistoricalBar]:
    merged: Dict[str, HistoricalBar] = {}
    for bars in bar_sets:
        for bar in bars:
            current = merged.get(bar.ts_utc)
            if current is None or SOURCE_PRIORITY.get(bar.source, 0) > SOURCE_PRIORITY.get(current.source, 0):
                merged[bar.ts_utc] = bar
    return sorted(merged.values(), key=lambda item: item.ts_unix)


def summarize_gaps(
    bars: list[HistoricalBar],
    *,
    start_utc: datetime,
    end_utc: datetime,
    timeframe_sec: int,
) -> list[GapSummary]:
    if not bars:
        return [
            GapSummary(
                start_utc=_iso_utc(start_utc),
                end_utc=_iso_utc(end_utc),
                missing_bars=max(1, int((end_utc - start_utc).total_seconds() // timeframe_sec)),
                duration_minutes=max(0.0, (end_utc - start_utc).total_seconds() / 60.0),
            )
        ]

    marks: list[tuple[datetime, datetime]] = []
    first = datetime.fromtimestamp(bars[0].ts_unix, tz=UTC)
    if first > start_utc:
        marks.append((start_utc, first))

    for prev, curr in zip(bars, bars[1:]):
        prev_dt = datetime.fromtimestamp(prev.ts_unix, tz=UTC)
        curr_dt = datetime.fromtimestamp(curr.ts_unix, tz=UTC)
        if (curr_dt - prev_dt).total_seconds() > timeframe_sec:
            marks.append((prev_dt + timedelta(seconds=timeframe_sec), curr_dt))

    last = datetime.fromtimestamp(bars[-1].ts_unix, tz=UTC) + timedelta(seconds=timeframe_sec)
    if last < end_utc:
        marks.append((last, end_utc))

    out: list[GapSummary] = []
    for gap_start, gap_end in marks:
        seconds = max(0.0, (gap_end - gap_start).total_seconds())
        if seconds <= 0:
            continue
        missing = max(1, int(round(seconds / timeframe_sec)))
        out.append(
            GapSummary(
                start_utc=_iso_utc(gap_start),
                end_utc=_iso_utc(gap_end),
                missing_bars=missing,
                duration_minutes=round(seconds / 60.0, 2),
            )
        )
    return out


def build_historical_bars(
    *,
    dexter_root: Path | str,
    symbol: str,
    timeframe: str,
    start_utc: datetime,
    end_utc: datetime,
    synthetic_spread: float,
    source_policy: str = "real_only",
) -> tuple[list[HistoricalBar], Dict[str, Any]]:
    dexter_root = Path(dexter_root).resolve()
    timeframe_sec = TIMEFRAME_SECONDS[timeframe]
    source_policy = str(source_policy or "real_first").strip().lower()
    if source_policy not in SOURCE_POLICIES:
        raise ValueError(f"Unsupported source_policy={source_policy}; expected one of {sorted(SOURCE_POLICIES)}")
    candle_db = dexter_root / "backtest" / "candle_data.db"
    ctrader_db = dexter_root / "data" / "ctrader_openapi.db"

    candle_bars: list[HistoricalBar] = []
    spot_bars: list[HistoricalBar] = []
    depth_bars: list[HistoricalBar] = []

    if candle_db.is_file():
        with sqlite3.connect(f"file:{candle_db}?mode=ro", uri=True) as conn:
            candle_bars = _load_candle_bars(
                conn,
                symbol=symbol,
                tf=timeframe,
                start_utc=start_utc,
                end_utc=end_utc,
                synthetic_spread=synthetic_spread,
            )
    if ctrader_db.is_file():
        with sqlite3.connect(f"file:{ctrader_db}?mode=ro", uri=True) as conn:
            spot_bars = _load_spot_tick_bars(
                conn,
                symbol=symbol,
                start_utc=start_utc,
                end_utc=end_utc,
                timeframe_sec=timeframe_sec,
            )
            depth_bars = _load_depth_bars(
                conn,
                symbol=symbol,
                start_utc=start_utc,
                end_utc=end_utc,
                timeframe_sec=timeframe_sec,
            )

    if source_policy == "real_only":
        merged = merge_bars(depth_bars, spot_bars)
    elif source_policy == "candle_only":
        merged = merge_bars(candle_bars)
    else:
        merged = merge_bars(candle_bars, depth_bars, spot_bars)
    source_counts = Counter(bar.source for bar in merged)
    gaps = summarize_gaps(
        merged,
        start_utc=start_utc,
        end_utc=end_utc,
        timeframe_sec=timeframe_sec,
    )
    diagnostics = {
        "requested_start_utc": _iso_utc(start_utc),
        "requested_end_utc_exclusive": _iso_utc(end_utc),
        "timeframe": timeframe,
        "source_policy": source_policy,
        "raw_source_bars": {
            "candle_db": len(candle_bars),
            "spot_ticks": len(spot_bars),
            "depth_level0": len(depth_bars),
        },
        "merged_bar_count": len(merged),
        "merged_source_counts": dict(source_counts),
        "first_bar_utc": merged[0].ts_utc if merged else None,
        "last_bar_utc": merged[-1].ts_utc if merged else None,
        "gap_groups": [asdict(item) for item in gaps[:20]],
    }
    return merged, diagnostics


def _isolated_settings(
    base: Settings,
    *,
    run_dir: Path,
    symbol: Optional[str],
    enable_learning: bool,
) -> Settings:
    settings = base.model_copy(deep=True)
    if symbol:
        settings.symbol = str(symbol)
    settings.data_dir = run_dir.resolve()
    settings.runtime_state_path = (run_dir / "runtime_state.json").resolve()
    settings.strategy_registry_path = (run_dir / "strategy_registry.json").resolve()
    settings.chroma_path = (run_dir / "chroma").resolve()
    settings.skillbook_dir = (run_dir / "skills").resolve()
    settings.skillbook_index_path = (run_dir / "skillbook_index.json").resolve()
    settings.dry_run = False
    settings.live_execution_enabled = False
    settings.self_improvement_enabled = bool(enable_learning)
    settings.self_improvement_store_notes = bool(enable_learning)
    settings.performance_monitor_enabled = False
    settings.agent_team_enabled = bool(enable_learning)
    return settings


def _build_memory_for_backtest(settings: Settings, run_id: str) -> ReplayMemory:
    return ReplayMemory(score_weight=settings.memory_score_weight)


def _store_backtest_note(
    memory: ReplayMemory,
    *,
    settings: Settings,
    title: str,
    content: str,
    room: str,
    note_type: str,
    importance: float,
    session: str = "",
    setup_tag: str = "",
    strategy_key: str = "",
    tags: Optional[list[str]] = None,
) -> None:
    memory.store_note(
        MemoryNote(
            title=title,
            content=content,
            wing=f"symbol:{settings.symbol.lower()}",
            hall="hall_discoveries",
            room=room,
            note_type=note_type,
            hall_type="hall_discoveries",
            symbol=settings.symbol,
            session=session,
            setup_tag=setup_tag,
            strategy_key=strategy_key,
            importance=importance,
            source="backtest_replay",
            tags=list(tags or []),
        )
    )


def _strategy_state_payload(registry: StrategyRegistry, strategy_key: str) -> Optional[Dict[str, Any]]:
    if not strategy_key:
        return None
    stats = registry.get_stats(strategy_key)
    if stats is None:
        return None
    return {
        "trades": stats.trades,
        "wins": stats.wins,
        "losses": stats.losses,
        "total_profit": stats.total_profit,
        "score": stats.score,
        "ranking_score": stats.ranking_score,
        "active": stats.active,
        "lane_stage": stats.lane_stage,
        "pending_recommendation": stats.pending_recommendation,
        "shadow_trades": stats.shadow_trades,
        "shadow_wins": stats.shadow_wins,
        "shadow_losses": stats.shadow_losses,
        "shadow_total_profit": stats.shadow_total_profit,
    }


def _serialize_skill_match(match: SkillMatch) -> Dict[str, Any]:
    return {
        "skill_key": match.skill_key,
        "score": match.score,
        "title": match.title,
        "summary": match.summary,
        "stats": dict(match.stats),
        "fit_reasons": list(match.fit_reasons),
        "file_path": match.file_path,
    }


def _reason_bucket(reason: str) -> str:
    text = str(reason or "").strip()
    if not text:
        return "unknown"
    if text.startswith("pre_llm_hard_filter:"):
        return f"pre_llm_hard_filter:{text.split(':', 1)[1].split('|', 1)[0].split()[0]}"
    if text.startswith("low_confidence_floor("):
        return "low_confidence_floor"
    for marker, bucket in (
        ("|pattern_block:", "pattern_block"),
        ("|strategy_disabled:", "strategy_disabled"),
        ("|memory_guard:anti_pattern:", "memory_guard_anti_pattern"),
        ("|memory_guard:", "memory_guard"),
        ("|exposure_cap:", "exposure_cap"),
        ("|hard_filter:", "hard_filter"),
        ("|loss_streak_soft_gate:", "loss_streak_soft_gate"),
        ("skill_promotion:", "skill_promotion"),
        ("|skill_block:", "skill_block"),
        ("|skill_caution:", "skill_caution"),
        ("|skill_support:", "skill_support"),
        ("|pattern_soft_gate:", "pattern_soft_gate"),
        ("|strategy_soft_gate:", "strategy_soft_gate"),
    ):
        if marker in text or text.startswith(marker):
            return bucket
    if text == "risk_block_session":
        return "risk_block_session"
    if text.startswith("heuristic_fallback:"):
        bucket = text.split("|", 1)[0]
        bucket = bucket.split(" llm_error", 1)[0]
        bucket = bucket.split(" sample_len", 1)[0]
        return bucket
    return text[:96]


def _apply_skill_feedback(
    decision: Decision,
    *,
    anticipated_action: str,
    matches: List[SkillMatch],
    min_trade_confidence: float,
) -> tuple[Decision, Dict[str, Any]]:
    if anticipated_action not in ("BUY", "SELL") or not matches:
        return decision, {"applied": False}

    top = matches[0]
    stats = dict(top.stats)
    trades_seen = int(stats.get("trades_seen") or 0)
    wins = int(stats.get("wins") or 0)
    losses = int(stats.get("losses") or 0)
    win_rate = float(stats.get("win_rate") or 0.0)
    edge = float(stats.get("risk_adjusted_score") or 0.0)
    feedback = {
        "applied": False,
        "skill_key": top.skill_key,
        "fit": top.score,
        "edge": edge,
        "trades_seen": trades_seen,
        "win_rate": win_rate,
    }

    raw = dict(decision.raw)
    raw["skill_feedback"] = feedback

    if decision.action in ("BUY", "SELL"):
        if trades_seen >= 3 and losses >= wins + 2 and edge < -0.35 and top.score >= 4.0:
            feedback.update({"applied": True, "type": "block"})
            raw["skill_feedback"] = feedback
            return (
                Decision(
                    action="HOLD",
                    confidence=decision.confidence,
                    reason=f"{decision.reason}|skill_block:{top.skill_key}",
                    raw=raw,
                ),
                feedback,
            )

        delta = 0.0
        if trades_seen >= 2 and win_rate >= 0.55 and edge > 0.05:
            delta += min(0.08, 0.03 + (0.01 * min(trades_seen, 4)))
        elif trades_seen >= 2 and losses > wins and edge < -0.10:
            delta -= min(0.10, 0.04 + (0.02 * min(losses - wins, 3)))
        elif trades_seen == 1 and edge > 0.20:
            delta += 0.02
        elif trades_seen == 1 and edge < -0.90:
            delta -= 0.02

        if abs(delta) > 1e-9:
            feedback.update(
                {
                    "applied": True,
                    "type": "support" if delta > 0 else "caution",
                    "delta": round(delta, 4),
                }
            )
            raw["skill_feedback"] = feedback
            adjusted = Decision(
                action=decision.action,
                confidence=max(0.0, min(0.95, float(decision.confidence) + delta)),
                reason=f"{decision.reason}|{'skill_support' if delta > 0 else 'skill_caution'}:{top.skill_key}",
                raw=raw,
            )
            return apply_confidence_floor(adjusted, min_trade_confidence), feedback

        return Decision(
            action=decision.action,
            confidence=decision.confidence,
            reason=decision.reason,
            raw=raw,
        ), feedback

    promotable_hold = decision.reason.startswith("low_confidence_floor(") or "similar_memory_" in decision.reason
    if promotable_hold and trades_seen >= 3 and top.score >= 6.0 and win_rate >= 0.66 and edge > 0.20:
        feedback.update({"applied": True, "type": "promotion"})
        raw["skill_feedback"] = feedback
        promoted_conf = min(0.95, max(min_trade_confidence, 0.67 + min(0.06, 0.01 * trades_seen)))
        return (
            Decision(
                action=anticipated_action,
                confidence=promoted_conf,
                reason=f"skill_promotion:{top.skill_key}|{decision.reason}",
                raw=raw,
            ),
            feedback,
        )

    return Decision(
        action=decision.action,
        confidence=decision.confidence,
        reason=decision.reason,
        raw=raw,
    ), feedback


def _is_new_lane(strategy_state: Optional[Dict[str, Any]], settings: Settings) -> bool:
    trades = int((strategy_state or {}).get("trades") or 0)
    return trades < int(settings.soft_gate_new_lane_max_trades)


def _sync_registry_from_skill(registry: StrategyRegistry, skill: Optional[Dict[str, Any]]) -> None:
    if not skill:
        return
    stats = dict(skill.get("stats") or {})
    registry.sync_skill_feedback(
        str(skill.get("skill_key") or ""),
        risk_adjusted_score=float(stats.get("risk_adjusted_score") or 0.0),
        trades_seen=int(stats.get("trades_seen") or 0),
        win_rate=float(stats.get("win_rate") or 0.0),
    )


def _soften_pattern_block(
    decision: Decision,
    *,
    pat_reason: str,
    strategy_state: Optional[Dict[str, Any]],
    matches: List[SkillMatch],
    settings: Settings,
) -> tuple[Decision, bool]:
    if not settings.soft_gate_new_lane_enabled:
        return decision, False
    pending = str((strategy_state or {}).get("pending_recommendation") or "")
    allow = _is_new_lane(strategy_state, settings) or pending in {"probation_boost", "promote_from_shadow"}
    if not allow and matches:
        top = matches[0]
        allow = float(top.stats.get("risk_adjusted_score") or 0.0) > 0.15 and int(top.stats.get("trades_seen") or 0) >= 1
    if not allow:
        return decision, False
    if not (str(pat_reason).startswith("pattern_low_sample:") or str(pat_reason).startswith("pattern_unknown")):
        return decision, False
    raw = dict(decision.raw)
    raw["soft_gate"] = {"type": "pattern", "reason": pat_reason}
    softened = Decision(
        action=decision.action,
        confidence=max(0.0, min(0.95, float(decision.confidence) - float(settings.soft_gate_confidence_penalty))),
        reason=f"{decision.reason}|pattern_soft_gate:{pat_reason}",
        raw=raw,
    )
    return apply_confidence_floor(softened, float(settings.soft_gate_min_confidence)), True


def _soften_strategy_block(
    decision: Decision,
    *,
    strategy_key: str,
    strategy_state: Optional[Dict[str, Any]],
    matches: List[SkillMatch],
    settings: Settings,
) -> tuple[Decision, bool]:
    if not settings.soft_gate_new_lane_enabled:
        return decision, False
    pending = str((strategy_state or {}).get("pending_recommendation") or "")
    if pending in {"quarantine", "quarantine_shadow"}:
        return decision, False
    allow = _is_new_lane(strategy_state, settings) or pending in {"probation_boost", "promote_from_shadow"}
    if not allow and matches:
        top = matches[0]
        allow = float(top.stats.get("risk_adjusted_score") or 0.0) > 0.15 and int(top.stats.get("trades_seen") or 0) >= 1
    if not allow:
        return decision, False
    raw = dict(decision.raw)
    raw["soft_gate"] = {"type": "strategy", "strategy_key": strategy_key}
    softened = Decision(
        action=decision.action,
        confidence=max(0.0, min(0.95, float(decision.confidence) - float(settings.soft_gate_confidence_penalty))),
        reason=f"{decision.reason}|strategy_soft_gate:{strategy_key}",
        raw=raw,
    )
    return apply_confidence_floor(softened, float(settings.soft_gate_min_confidence)), True


def _loss_streak_override_payload(
    *,
    veto: Optional[str],
    anticipated_action: str,
    strategy_key: str,
    strategy_state: Optional[Dict[str, Any]],
    matches: List[SkillMatch],
    settings: Settings,
) -> Optional[Dict[str, Any]]:
    if not settings.loss_streak_override_enabled:
        return None
    if anticipated_action not in ("BUY", "SELL"):
        return None
    text = str(veto or "")
    if not text.startswith("loss_streak_"):
        return None

    state = dict(strategy_state or {})
    pending = str(state.get("pending_recommendation") or "")
    shadow_trades = int(state.get("shadow_trades") or 0)
    shadow_wins = int(state.get("shadow_wins") or 0)
    shadow_total_profit = float(state.get("shadow_total_profit") or 0.0)
    shadow_wr = (shadow_wins / float(shadow_trades)) if shadow_trades else 0.0

    top = matches[0] if matches else None
    stats = dict(top.stats) if top else {}
    trades_seen = int(stats.get("trades_seen") or 0)
    skill_wr = float(stats.get("win_rate") or 0.0)
    skill_edge = float(stats.get("risk_adjusted_score") or 0.0)
    skill_fit = float(top.score) if top else 0.0
    skill_key = str(top.skill_key) if top else strategy_key

    min_shadow_trades = int(settings.loss_streak_override_min_shadow_trades)
    min_shadow_wr = float(settings.loss_streak_override_min_shadow_win_rate)
    min_skill_trades = int(settings.loss_streak_override_min_skill_trades)
    min_skill_edge = float(settings.loss_streak_override_min_skill_edge)
    min_skill_wr = max(0.55, min_shadow_wr)

    if (
        pending == "promote_from_shadow"
        and shadow_trades >= min_shadow_trades
        and shadow_wr >= min_shadow_wr
        and shadow_total_profit > 0.0
    ):
        return {
            "applied": True,
            "type": "promote_from_shadow",
            "strategy_key": strategy_key,
            "skill_key": skill_key,
            "shadow_trades": shadow_trades,
            "shadow_win_rate": round(shadow_wr, 4),
            "shadow_total_profit": round(shadow_total_profit, 6),
        }

    if (
        pending == "probation_boost"
        and shadow_trades >= max(1, min_shadow_trades - 1)
        and shadow_wr >= min_shadow_wr
        and shadow_total_profit >= 0.0
    ):
        return {
            "applied": True,
            "type": "probation_boost",
            "strategy_key": strategy_key,
            "skill_key": skill_key,
            "shadow_trades": shadow_trades,
            "shadow_win_rate": round(shadow_wr, 4),
            "shadow_total_profit": round(shadow_total_profit, 6),
        }

    if (
        top is not None
        and trades_seen >= min_skill_trades
        and skill_wr >= min_skill_wr
        and skill_edge >= min_skill_edge
        and skill_fit >= 4.0
    ):
        return {
            "applied": True,
            "type": "skill_edge",
            "strategy_key": strategy_key,
            "skill_key": skill_key,
            "skill_trades_seen": trades_seen,
            "skill_win_rate": round(skill_wr, 4),
            "skill_edge": round(skill_edge, 6),
        }

    return None


def _apply_loss_streak_soft_gate(
    decision: Decision,
    *,
    override: Optional[Dict[str, Any]],
    settings: Settings,
    min_trade_confidence: float,
) -> Decision:
    if not override or decision.action not in ("BUY", "SELL"):
        return decision
    raw = dict(decision.raw)
    raw["loss_streak_override"] = dict(override)
    marker = f"{override.get('type')}:{override.get('skill_key') or override.get('strategy_key') or 'unknown'}"
    softened = Decision(
        action=decision.action,
        confidence=max(
            0.0,
            min(0.95, float(decision.confidence) - float(settings.loss_streak_override_confidence_penalty)),
        ),
        reason=f"{decision.reason}|loss_streak_soft_gate:{marker}",
        raw=raw,
    )
    return apply_confidence_floor(softened, min_trade_confidence)


def _eligible_shadow_probe_bucket(bucket: str) -> bool:
    text = str(bucket or "")
    if text.startswith("pre_llm_hard_filter:loss_streak_"):
        return True
    return text in {"pattern_block", "strategy_disabled", "low_confidence_floor"}


def _shadow_probe_volume(settings: Settings) -> float:
    requested = float(settings.default_volume) * float(settings.shadow_probe_volume_fraction)
    return max(1e-4, requested)


def _shadow_probe_market_ok(market: MarketSnapshot, features: Dict[str, Any]) -> bool:
    if float(market.bid) <= 0.0 or float(market.ask) <= 0.0 or float(market.mid) <= 0.0:
        return False
    return float(features.get("spread_pct") or 0.0) < 0.001


def _requested_trade_volume(decision: Decision, settings: Settings) -> float:
    reason = str(decision.reason or "")
    if (
        "|pattern_soft_gate:" in reason
        or "|strategy_soft_gate:" in reason
        or "|loss_streak_soft_gate:" in reason
    ):
        requested = float(settings.default_volume) * float(settings.probation_trade_volume_fraction)
        floor = max(1e-4, float(settings.risk_min_order_lot))
        return min(float(settings.default_volume), max(floor, requested))
    return float(settings.default_volume)


async def run_backtest(
    *,
    start_day: str,
    end_day: str,
    timezone_name: str,
    symbol: Optional[str],
    timeframe: str,
    dexter_root: Path | str,
    output_root: Optional[Path] = None,
    source_policy: str = "real_only",
    enable_learning: bool = False,
) -> BacktestResult:
    if timeframe not in TIMEFRAME_SECONDS:
        raise ValueError(f"Unsupported timeframe {timeframe}; expected one of {sorted(TIMEFRAME_SECONDS)}")

    logging.getLogger("trading_ai.core.execution").setLevel(logging.WARNING)
    logging.getLogger("trading_ai.core.agent").setLevel(logging.WARNING)

    base_settings = load_settings()
    start_utc, end_utc, window_meta = _date_window_to_utc(start_day, end_day, timezone_name)
    run_id = f"{(symbol or base_settings.symbol).lower()}_{start_day}_to_{end_day}_{uuid.uuid4().hex[:8]}"
    root = Path(output_root or (Path(base_settings.data_dir) / "backtests"))
    run_dir = (root / run_id).resolve()
    run_dir.mkdir(parents=True, exist_ok=True)

    settings = _isolated_settings(
        base_settings,
        run_dir=run_dir,
        symbol=symbol,
        enable_learning=enable_learning,
    )
    bars, coverage = build_historical_bars(
        dexter_root=Path(dexter_root).resolve(),
        symbol=settings.symbol,
        timeframe=timeframe,
        start_utc=start_utc,
        end_utc=end_utc,
        synthetic_spread=float(settings.ctrader_reference_quote_spread),
        source_policy=source_policy,
    )
    if not bars:
        raise RuntimeError(
            f"No historical bars found for {settings.symbol} between {window_meta['start_utc']} and {window_meta['end_utc_exclusive']}"
        )

    memory = _build_memory_for_backtest(settings, run_id)
    skillbook = SkillBook(
        root_dir=Path(settings.skillbook_dir),
        index_path=Path(settings.skillbook_index_path),
        max_evidence=settings.skillbook_max_evidence,
    )
    self_improvement = SelfImprovementEngine(
        skillbook=skillbook,
        memory=memory,
        llm=None,
        enabled=settings.self_improvement_enabled,
        store_notes=settings.self_improvement_store_notes,
    )
    broker = HistoricalBacktestBroker(
        settings.symbol,
        initial_equity=float(settings.risk_equity_fallback_usd),
    )
    execution = ExecutionService(broker)
    shadow_execution = ExecutionService(broker)
    agent = TradingAgent(BacktestFallbackLLM(), settings)
    risk = RiskManager(
        max_trades_per_session=settings.max_trades_per_session,
        max_consecutive_losses=settings.max_consecutive_losses,
        neutral_rel_threshold=settings.neutral_pnl_threshold,
    )
    perf = PerformanceTracker()
    pattern_book = PatternBook()
    registry = StrategyRegistry(Path(settings.strategy_registry_path))

    price_history: List[float] = []
    bars_window: deque[dict[str, float]] = deque(maxlen=max(20, int(settings.price_history_max)))
    open_contexts: List[Dict[str, Any]] = []
    shadow_open_contexts: List[Dict[str, Any]] = []
    base_decision_counts: Counter[str] = Counter()
    decision_counts: Counter[str] = Counter()
    base_reason_counts: Counter[str] = Counter()
    final_reason_counts: Counter[str] = Counter()
    blocker_counts: Counter[str] = Counter()
    skill_effect_counts: Counter[str] = Counter()
    skill_recall_counts: Counter[str] = Counter()
    shadow_probe_counts: Counter[str] = Counter()
    shadow_probe_blockers: Counter[str] = Counter()
    regime_counts: Dict[str, Counter[str]] = {
        "trend_direction": Counter(),
        "volatility": Counter(),
        "session": Counter(),
        "consolidation": Counter(),
        "anticipated_action": Counter(),
    }
    skill_update_counts: Counter[str] = Counter()
    trades: List[Dict[str, Any]] = []
    shadow_trades: List[Dict[str, Any]] = []
    shadow_perf = PerformanceTracker()
    try:
        local_tz = ZoneInfo(timezone_name)
    except Exception:
        local_tz = FIXED_TZ_OFFSETS.get(timezone_name, timezone.utc)
    daily_learning: Dict[str, Dict[str, Any]] = {}
    positive_skill_keys: set[str] = set()
    negative_skill_keys: set[str] = set()

    def _day_bucket(ts_unix: float) -> str:
        return datetime.fromtimestamp(ts_unix, tz=UTC).astimezone(local_tz).date().isoformat()

    def _daily(day_key: str) -> Dict[str, Any]:
        bucket = daily_learning.get(day_key)
        if bucket is None:
            bucket = {
                "day": day_key,
                "base_decisions": Counter(),
                "final_decisions": Counter(),
                "blockers": Counter(),
                "trades_closed": 0,
                "shadow_trades_closed": 0,
                "carryover_opportunity_entries": 0,
                "carryover_risk_avoids": 0,
                "carryover_unlocks": 0,
                "new_positive_keys": [],
                "new_negative_keys": [],
                "skills_at_open": sorted(positive_skill_keys),
                "risk_keys_at_open": sorted(negative_skill_keys),
            }
            daily_learning[day_key] = bucket
        return bucket

    def _note_skill_signal(day_key: str, skill: Optional[Dict[str, Any]]) -> None:
        if not skill:
            return
        skill_key = str(skill.get("skill_key") or "")
        if not skill_key:
            return
        stats = dict(skill.get("stats") or {})
        trades_seen = int(stats.get("trades_seen") or 0)
        edge = float(stats.get("risk_adjusted_score") or 0.0)
        win_rate = float(stats.get("win_rate") or 0.0)
        bucket = _daily(day_key)
        if trades_seen >= 2 and edge > 0.15 and win_rate >= 0.5 and skill_key not in positive_skill_keys:
            positive_skill_keys.add(skill_key)
            bucket["new_positive_keys"].append(skill_key)
        if trades_seen >= 2 and edge < -0.2 and skill_key not in negative_skill_keys:
            negative_skill_keys.add(skill_key)
            bucket["new_negative_keys"].append(skill_key)

    async def process_closed_trade(
        close_detail: Dict[str, Any],
        close_context: Optional[Dict[str, Any]],
        *,
        close_reason: str,
    ) -> None:
        pnl = float(close_detail["pnl"])
        entry_price = float(close_detail["entry_price"])
        exit_price = float(close_detail["exit_price"])
        volume = float(close_detail["volume"])
        side = str(close_detail["side"])
        closed_dt = _parse_timestamp(close_detail["closed_utc"])
        day_key = _day_bucket(closed_dt.timestamp() if closed_dt is not None else 0.0)
        notional = abs(entry_price * volume)
        tscore = evaluate_outcome(
            pnl,
            notional=notional,
            neutral_rel_threshold=settings.neutral_pnl_threshold,
        )
        score_int = int(tscore)

        if close_context is not None:
            strategy_key = str(close_context.get("strategy_key") or "")
            setup_tag = str(close_context.get("setup_tag") or "")
            features = dict(close_context.get("features") or {})
            decision = dict(close_context.get("decision") or {})
            record = MemoryRecord(
                market=dict(close_context.get("market") or {}),
                features=features,
                decision=decision,
                result={"pnl": pnl, "entry_price": entry_price, "exit_price": exit_price},
                score=score_int,
                setup_tag=setup_tag,
                strategy_key=strategy_key,
                journal=str(close_context.get("journal") or ""),
                tags=list(close_context.get("tags") or []),
            )
            memory.store_memory(record, extra_metadata={"trade_score": score_int, "strategy_key": strategy_key})
            pattern_book.append_closed_trade(
                features=features,
                setup_tag=setup_tag,
                score=score_int,
                pnl=pnl,
            )
            if strategy_key:
                registry.update_strategy(strategy_key, {"pnl": pnl, "score": score_int})

            strategy_state = _strategy_state_payload(registry, strategy_key)
            room_guard = memory.get_room_guardrail(
                symbol=settings.symbol,
                session=str(features.get("session") or ""),
                setup_tag=setup_tag,
                trend_direction=str(features.get("trend_direction") or ""),
                volatility=str(features.get("volatility") or ""),
                strategy_key=strategy_key,
            )

            confidence = float(decision.get("confidence") or 0.0)
            room = str(strategy_key or build_strategy_key(features, setup_tag))
            if score_int < 0 and confidence >= 0.75:
                _store_backtest_note(
                    memory,
                    settings=settings,
                    title="Overconfident loss",
                    content=(
                        f"Loss in room={room} confidence={confidence:.3f} pnl={pnl:.6f}. "
                        "Backtest replay flagged this as an anti-pattern candidate."
                    ),
                    room=room,
                    note_type="anti_pattern_candidate",
                    importance=0.9,
                    session=str(features.get("session") or ""),
                    setup_tag=setup_tag,
                    strategy_key=strategy_key,
                    tags=["anti-pattern", "overconfident-loss", "backtest"],
                )
            elif score_int > 0 and confidence < 0.55:
                _store_backtest_note(
                    memory,
                    settings=settings,
                    title="Underconfident win",
                    content=(
                        f"Win in room={room} confidence={confidence:.3f} pnl={pnl:.6f}. "
                        "Backtest replay flagged this as an opportunity candidate."
                    ),
                    room=room,
                    note_type="opportunity_candidate",
                    importance=0.78,
                    session=str(features.get("session") or ""),
                    setup_tag=setup_tag,
                    strategy_key=strategy_key,
                    tags=["opportunity", "underconfident-win", "backtest"],
                )

            trades.append(
                {
                    "opened_utc": close_context.get("opened_utc"),
                    "closed_utc": close_detail["closed_utc"],
                    "side": side,
                    "volume": volume,
                    "entry_price": entry_price,
                    "exit_price": exit_price,
                    "pnl": round(pnl, 6),
                    "score": score_int,
                    "setup_tag": setup_tag,
                    "strategy_key": strategy_key,
                    "decision_reason": str(decision.get("reason") or ""),
                    "decision_confidence": confidence,
                    "close_reason": close_reason,
                    "source_open": close_context.get("source"),
                    "source_close": close_detail.get("source"),
                }
            )

            if settings.self_improvement_enabled:
                skill = await self_improvement.learn_from_closed_trade(
                    close_context=close_context,
                    close_result={
                        "pnl": round(pnl, 6),
                        "entry_price": entry_price,
                        "exit_price": exit_price,
                        "close_reason": close_reason,
                        "closed_utc": close_detail["closed_utc"],
                        "side": side,
                        "volume": volume,
                    },
                    score=score_int,
                    strategy_state=strategy_state,
                    room_guard=room_guard,
                )
                if skill is not None:
                    skill_update_counts[str(skill.get("skill_key") or strategy_key or "general-learning")] += 1
                    _sync_registry_from_skill(registry, skill)
                    _note_skill_signal(day_key, skill)
        else:
            trades.append(
                {
                    "opened_utc": None,
                    "closed_utc": close_detail["closed_utc"],
                    "side": side,
                    "volume": volume,
                    "entry_price": entry_price,
                    "exit_price": exit_price,
                    "pnl": round(pnl, 6),
                    "score": score_int,
                    "setup_tag": "",
                    "strategy_key": "",
                    "decision_reason": "missing_context",
                    "decision_confidence": 0.0,
                    "close_reason": close_reason,
                    "source_open": None,
                    "source_close": close_detail.get("source"),
                }
            )

        risk.on_trade_result(tscore, pnl=pnl)
        perf.record_close(pnl, score=score_int)
        _daily(day_key)["trades_closed"] += 1

    async def process_shadow_close(
        close_detail: Dict[str, Any],
        close_context: Optional[Dict[str, Any]],
        *,
        close_reason: str,
    ) -> None:
        pnl = float(close_detail["pnl"])
        entry_price = float(close_detail["entry_price"])
        exit_price = float(close_detail["exit_price"])
        volume = float(close_detail["volume"])
        side = str(close_detail["side"])
        closed_dt = _parse_timestamp(close_detail["closed_utc"])
        day_key = _day_bucket(closed_dt.timestamp() if closed_dt is not None else 0.0)
        notional = abs(entry_price * volume)
        tscore = evaluate_outcome(
            pnl,
            notional=notional,
            neutral_rel_threshold=settings.neutral_pnl_threshold,
        )
        score_int = int(tscore)

        if close_context is None:
            shadow_trades.append(
                {
                    "opened_utc": None,
                    "closed_utc": close_detail["closed_utc"],
                    "side": side,
                    "volume": volume,
                    "entry_price": entry_price,
                    "exit_price": exit_price,
                    "pnl": round(pnl, 6),
                    "score": score_int,
                    "strategy_key": "",
                    "setup_tag": "",
                    "blocker_bucket": "unknown",
                    "shadow_probe": True,
                    "close_reason": close_reason,
                }
            )
            shadow_perf.record_close(pnl, score=score_int)
            return

        strategy_key = str(close_context.get("strategy_key") or "")
        setup_tag = str(close_context.get("setup_tag") or "")
        features = dict(close_context.get("features") or {})
        decision = dict(close_context.get("decision") or {})
        blocker_bucket = str(close_context.get("probe_blocker_bucket") or "unknown")
        blocker_reason = str(close_context.get("probe_blocker_reason") or "")

        record = MemoryRecord(
            market=dict(close_context.get("market") or {}),
            features=features,
            decision=decision,
            result={"pnl": pnl, "entry_price": entry_price, "exit_price": exit_price},
            score=score_int,
            setup_tag=setup_tag,
            strategy_key=strategy_key,
            journal=str(close_context.get("journal") or ""),
            tags=list(close_context.get("tags") or []) + ["shadow-probe"],
        )
        memory.store_memory(
            record,
            extra_metadata={
                "trade_score": score_int,
                "strategy_key": strategy_key,
                "memory_type": "shadow_probe",
                "probe_blocker_bucket": blocker_bucket,
            },
        )
        pattern_book.append_closed_trade(
            features=features,
            setup_tag=setup_tag,
            score=score_int,
            pnl=pnl,
        )
        if strategy_key:
            registry.record_shadow_probe(strategy_key, pnl=pnl, score=score_int)

        note_title = "Blocked opportunity confirmed" if score_int > 0 else "Blocker confirmed"
        note_type = "opportunity_candidate" if score_int > 0 else "anti_pattern_candidate"
        note_importance = 0.84 if score_int > 0 else 0.78
        _store_backtest_note(
            memory,
            settings=settings,
            title=note_title,
            content=(
                f"shadow_probe blocker={blocker_bucket} strategy={strategy_key or setup_tag} "
                f"side={side} pnl={pnl:.6f} close_reason={close_reason}. original_blocker={blocker_reason}"
            ),
            room=str(strategy_key or build_strategy_key(features, setup_tag)),
            note_type=note_type,
            importance=note_importance,
            session=str(features.get("session") or ""),
            setup_tag=setup_tag,
            strategy_key=strategy_key,
            tags=["shadow-probe", blocker_bucket, "blocked-opportunity" if score_int > 0 else "blocked-loss"],
        )

        strategy_state = _strategy_state_payload(registry, strategy_key)
        room_guard = memory.get_room_guardrail(
            symbol=settings.symbol,
            session=str(features.get("session") or ""),
            setup_tag=setup_tag,
            trend_direction=str(features.get("trend_direction") or ""),
            volatility=str(features.get("volatility") or ""),
            strategy_key=strategy_key,
        )
        skill = await self_improvement.learn_from_closed_trade(
            close_context=close_context,
            close_result={
                "pnl": round(pnl, 6),
                "entry_price": entry_price,
                "exit_price": exit_price,
                "close_reason": close_reason,
                "closed_utc": close_detail["closed_utc"],
                "side": side,
                "volume": volume,
                "shadow_probe": True,
                "probe_blocker_bucket": blocker_bucket,
            },
            score=score_int,
            strategy_state=strategy_state,
            room_guard=room_guard,
        )
        _sync_registry_from_skill(registry, skill)
        _note_skill_signal(day_key, skill)
        shadow_trades.append(
            {
                "opened_utc": close_context.get("opened_utc"),
                "closed_utc": close_detail["closed_utc"],
                "side": side,
                "volume": volume,
                "entry_price": entry_price,
                "exit_price": exit_price,
                "pnl": round(pnl, 6),
                "score": score_int,
                "setup_tag": setup_tag,
                "strategy_key": strategy_key,
                "decision_reason": str(decision.get("reason") or ""),
                "decision_confidence": float(decision.get("confidence") or 0.0),
                "blocker_bucket": blocker_bucket,
                "blocker_reason": blocker_reason,
                "shadow_probe": True,
                "close_reason": close_reason,
                "source_open": close_context.get("source"),
                "source_close": close_detail.get("source"),
            }
        )
        shadow_perf.record_close(pnl, score=score_int)
        _daily(day_key)["shadow_trades_closed"] += 1

    for bar in bars:
        broker.set_bar(bar)
        market = await execution.get_market_data(settings.symbol)
        day_key = _day_bucket(bar.ts_unix)
        day_bucket = _daily(day_key)
        price_history.append(float(market.mid))
        if len(price_history) > settings.price_history_max:
            price_history = price_history[-settings.price_history_max :]
        bars_window.append(
            {"open": bar.open, "high": bar.high, "low": bar.low, "close": bar.close}
        )

        md: Dict[str, Any] = {
            **market.as_prompt_dict(),
            "price_history": list(price_history),
            "bars": list(bars_window),
        }
        features = extract_features(md)
        regime_counts["trend_direction"][str(features.get("trend_direction") or "UNKNOWN")] += 1
        regime_counts["volatility"][str(features.get("volatility") or "UNKNOWN")] += 1
        regime_counts["session"][str(features.get("session") or "UNKNOWN")] += 1
        regime_counts["consolidation"]["true" if bool((features.get("structure") or {}).get("consolidation")) else "false"] += 1

        similar = memory.recall_similar_trades(
            features,
            symbol=settings.symbol,
            top_k=settings.similar_trades_top_k,
        ) if memory.count() > 0 else []

        risk_state = {
            "can_trade": risk.can_trade(),
            "halted": risk.halted,
            "consecutive_losses": risk.consecutive_losses,
            "max_consecutive_losses_halt": settings.max_consecutive_losses,
            "entry_loss_streak_block": settings.entry_loss_streak_block,
            "trades_executed_session": risk.trades_executed,
            "min_confidence_required": settings.min_trade_confidence,
        }
        patterns_live = pattern_book.patterns_dict()
        pattern_analysis = build_pattern_analysis_for_prompt(features, patterns_live)
        wake_up_context = memory.build_wake_up_context(
            symbol=settings.symbol,
            session=str(features.get("session") or ""),
            top_k=settings.memory_wakeup_top_k,
            note_top_k=settings.memory_note_top_k,
        )
        anticipated_action = (
            "BUY"
            if str(features.get("trend_direction") or "").upper() == "UP"
            else "SELL"
            if str(features.get("trend_direction") or "").upper() == "DOWN"
            else "HOLD"
        )
        regime_counts["anticipated_action"][anticipated_action] += 1
        anticipated_setup = infer_setup_tag(features, anticipated_action)
        anticipated_strategy_key = build_strategy_key(features, anticipated_setup)
        anticipated_room_guard = (
            memory.get_room_guardrail(
                symbol=settings.symbol,
                session=str(features.get("session") or ""),
                setup_tag=anticipated_setup,
                trend_direction=str(features.get("trend_direction") or ""),
                volatility=str(features.get("volatility") or ""),
                strategy_key=anticipated_strategy_key,
            )
            if settings.memory_room_guard_enabled
            else None
        )
        anticipated_state = _strategy_state_payload(registry, anticipated_strategy_key)
        skill_matches = skillbook.recall(
            symbol=settings.symbol,
            session=str(features.get("session") or ""),
            setup_tag=anticipated_setup,
            strategy_key=anticipated_strategy_key,
            room=anticipated_strategy_key,
            trend_direction=str(features.get("trend_direction") or ""),
            volatility=str(features.get("volatility") or ""),
            action=anticipated_action,
            top_k=settings.skill_recall_top_k,
        )
        if skill_matches:
            skill_recall_counts[skill_matches[0].skill_key] += 1
        active_skill_keys = [match.skill_key for match in skill_matches]
        skill_context = skillbook.render_prompt_context(skill_matches)
        team_brief = (
            build_team_brief(
                features=features,
                risk_state=risk_state,
                pattern_analysis=pattern_analysis,
                matches=skill_matches,
                strategy_state=anticipated_state,
                room_guard=anticipated_room_guard,
            )
            if settings.agent_team_enabled
            else {}
        )

        if settings.position_manager_enabled and execution.positions_for(settings.symbol):
            managed_positions = execution.positions_for(settings.symbol)
            managed_plans = []
            for idx, position in enumerate(managed_positions):
                close_context = open_contexts[idx] if idx < len(open_contexts) else None
                if close_context is None:
                    continue
                ctx_matches = skillbook.recall(
                    symbol=settings.symbol,
                    session=str(features.get("session") or ""),
                    setup_tag=str(close_context.get("setup_tag") or ""),
                    strategy_key=str(close_context.get("strategy_key") or ""),
                    room=str(close_context.get("strategy_key") or ""),
                    trend_direction=str(features.get("trend_direction") or ""),
                    volatility=str(features.get("volatility") or ""),
                    action=str((close_context.get("decision") or {}).get("action") or ""),
                    top_k=settings.skill_recall_top_k,
                )
                managed_plans.append(
                    evaluate_open_position(
                        position=position,
                        market=market,
                        features=features,
                        close_context=close_context,
                        matches=ctx_matches,
                        strategy_state=_strategy_state_payload(registry, str(close_context.get("strategy_key") or "")),
                        pattern_analysis=pattern_analysis,
                        settings=settings,
                    )
                )
            if any(plan.action == "CLOSE" for plan in managed_plans):
                close_reason = next(plan.reason for plan in managed_plans if plan.action == "CLOSE")
                manager_closes = await execution.close_positions(
                    symbol=settings.symbol,
                    reason=close_reason,
                    dry_run=False,
                )
                managed_contexts = list(open_contexts[: len(manager_closes)])
                for idx, close in enumerate(manager_closes):
                    close_context = managed_contexts[idx] if idx < len(managed_contexts) else None
                    await process_closed_trade(
                        {
                            "closed_utc": bar.ts_utc,
                            "side": close.side,
                            "volume": close.volume,
                            "entry_price": close.entry_price,
                            "exit_price": close.exit_price,
                            "pnl": close.pnl,
                            "source": bar.source,
                        },
                        close_context,
                        close_reason=close_reason,
                    )
                if manager_closes:
                    open_contexts = open_contexts[len(manager_closes) :]

        if settings.position_manager_enabled and shadow_execution.positions_for(settings.symbol):
            shadow_positions = shadow_execution.positions_for(settings.symbol)
            shadow_plans = []
            for idx, position in enumerate(shadow_positions):
                close_context = shadow_open_contexts[idx] if idx < len(shadow_open_contexts) else None
                if close_context is None:
                    continue
                ctx_matches = skillbook.recall(
                    symbol=settings.symbol,
                    session=str(features.get("session") or ""),
                    setup_tag=str(close_context.get("setup_tag") or ""),
                    strategy_key=str(close_context.get("strategy_key") or ""),
                    room=str(close_context.get("strategy_key") or ""),
                    trend_direction=str(features.get("trend_direction") or ""),
                    volatility=str(features.get("volatility") or ""),
                    action=str((close_context.get("decision") or {}).get("action") or ""),
                    top_k=settings.skill_recall_top_k,
                )
                shadow_plans.append(
                    evaluate_open_position(
                        position=position,
                        market=market,
                        features=features,
                        close_context=close_context,
                        matches=ctx_matches,
                        strategy_state=_strategy_state_payload(registry, str(close_context.get("strategy_key") or "")),
                        pattern_analysis=pattern_analysis,
                        settings=settings,
                    )
                )
            if any(plan.action == "CLOSE" for plan in shadow_plans):
                close_reason = next(plan.reason for plan in shadow_plans if plan.action == "CLOSE")
                shadow_closes = await shadow_execution.close_positions(
                    symbol=settings.symbol,
                    reason=close_reason,
                    dry_run=True,
                )
                shadow_contexts = list(shadow_open_contexts[: len(shadow_closes)])
                for idx, close in enumerate(shadow_closes):
                    close_context = shadow_contexts[idx] if idx < len(shadow_contexts) else None
                    await process_shadow_close(
                        {
                            "closed_utc": bar.ts_utc,
                            "side": close.side,
                            "volume": close.volume,
                            "entry_price": close.entry_price,
                            "exit_price": close.exit_price,
                            "pnl": close.pnl,
                            "source": bar.source,
                        },
                        close_context,
                        close_reason=close_reason,
                    )
                if shadow_closes:
                    shadow_open_contexts = shadow_open_contexts[len(shadow_closes) :]

        probe_candidate_decision: Optional[Decision] = None
        probe_candidate_bucket = ""
        probe_candidate_reason = ""
        pre_llm_veto = _hard_market_filters(features, risk, settings)
        loss_streak_override = _loss_streak_override_payload(
            veto=pre_llm_veto,
            anticipated_action=anticipated_action,
            strategy_key=anticipated_strategy_key,
            strategy_state=anticipated_state,
            matches=skill_matches,
            settings=settings,
        )
        if pre_llm_veto and not loss_streak_override:
            decision = Decision(
                action="HOLD",
                confidence=0.0,
                reason=f"pre_llm_hard_filter:{pre_llm_veto}",
                raw={"pre_llm_hard_filter": pre_llm_veto},
            )
            if settings.shadow_probe_enabled and str(pre_llm_veto).startswith("loss_streak_"):
                probe_candidate_decision = agent._heuristic_fallback_decision(
                    similar_trades=similar,
                    features=features,
                    risk_state=risk_state,
                    pattern_analysis=pattern_analysis,
                    error=RuntimeError("backtest_shadow_probe"),
                )
                if settings.self_improvement_enabled:
                    probe_candidate_decision, skill_feedback = _apply_skill_feedback(
                        probe_candidate_decision,
                        anticipated_action=anticipated_action,
                        matches=skill_matches,
                        min_trade_confidence=settings.shadow_probe_min_confidence,
                    )
                    if skill_feedback.get("applied"):
                        skill_effect_counts[f"shadow_{str(skill_feedback.get('type') or 'unknown')}"] += 1
                probe_candidate_bucket = _reason_bucket(decision.reason)
                probe_candidate_reason = decision.reason
        else:
            decision = agent._heuristic_fallback_decision(
                similar_trades=similar,
                features=features,
                risk_state=risk_state,
                pattern_analysis=pattern_analysis,
                error=RuntimeError("backtest_heuristic_mode"),
            )
            if settings.self_improvement_enabled:
                decision, skill_feedback = _apply_skill_feedback(
                    decision,
                    anticipated_action=anticipated_action,
                    matches=skill_matches,
                    min_trade_confidence=settings.min_trade_confidence,
                )
                if skill_feedback.get("applied"):
                    skill_effect_counts[str(skill_feedback.get("type") or "unknown")] += 1
            decision = _apply_loss_streak_soft_gate(
                decision,
                override=loss_streak_override,
                settings=settings,
                min_trade_confidence=float(settings.soft_gate_min_confidence),
            )

        base_decision = Decision(
            action=decision.action,
            confidence=decision.confidence,
            reason=decision.reason,
            raw=dict(decision.raw),
        )
        base_decision_counts[base_decision.action] += 1
        base_reason_counts[_reason_bucket(base_decision.reason)] += 1
        day_bucket["base_decisions"][base_decision.action] += 1

        veto = _hard_market_filters(features, risk, settings, decision.action)
        if loss_streak_override and str(veto).startswith("loss_streak_"):
            veto = None
        if veto and not decision.reason.startswith("pre_llm_hard_filter:"):
            decision = Decision(
                action="HOLD",
                confidence=0.0,
                reason=f"{decision.reason}|hard_filter:{veto}",
                raw=dict(decision.raw),
            )
            decision = apply_confidence_floor(decision, settings.min_trade_confidence)

        setup_eval = infer_setup_tag(features, decision.action) if decision.action in ("BUY", "SELL") else ""
        if decision.action in ("BUY", "SELL"):
            ok_pat, pat_reason, pat_stat = passes_pattern_execution_gate(
                features,
                patterns_live,
                setup_eval,
                min_win_rate=settings.pattern_min_win_rate,
                min_sample_size=settings.pattern_min_sample_size,
                strict_unknown=settings.pattern_gate_strict,
            )
            if not ok_pat:
                softened, softened_applied = _soften_pattern_block(
                    decision,
                    pat_reason=pat_reason,
                    strategy_state=_strategy_state_payload(registry, build_strategy_key(features, setup_eval)),
                    matches=skill_matches,
                    settings=settings,
                )
                if softened_applied:
                    decision = softened
                else:
                    decision = Decision(
                        action="HOLD",
                        confidence=decision.confidence,
                        reason=f"{decision.reason}|pattern_block:{pat_reason}",
                        raw=dict(decision.raw),
                    )
            else:
                new_conf, boosted = apply_pattern_confidence_boost(
                    decision.confidence,
                    pat_stat or {},
                    boost_min_win_rate=settings.pattern_boost_min_win_rate,
                    boost_min_sample=settings.pattern_boost_min_sample,
                    delta=settings.pattern_confidence_boost_delta,
                    cap=settings.pattern_confidence_cap,
                )
                if boosted:
                    decision = Decision(
                        action=decision.action,
                        confidence=new_conf,
                        reason=f"{decision.reason}|pattern_boost",
                        raw=dict(decision.raw),
                    )
                    decision = apply_confidence_floor(decision, settings.min_trade_confidence)

            if decision.action in ("BUY", "SELL"):
                strategy_key = build_strategy_key(features, setup_eval)
                if not registry.is_strategy_allowed(strategy_key):
                    softened, softened_applied = _soften_strategy_block(
                        decision,
                        strategy_key=strategy_key,
                        strategy_state=_strategy_state_payload(registry, strategy_key),
                        matches=skill_matches,
                        settings=settings,
                    )
                    if softened_applied:
                        decision = softened
                    else:
                        decision = Decision(
                            action="HOLD",
                            confidence=decision.confidence,
                            reason=f"{decision.reason}|strategy_disabled:{strategy_key}",
                            raw=dict(decision.raw),
                        )
                else:
                    evo_boost = registry.get_strategy_boost(strategy_key)
                    if evo_boost > 0.0:
                        decision = Decision(
                            action=decision.action,
                            confidence=min(0.95, decision.confidence + evo_boost),
                            reason=f"{decision.reason}|evolution_boost",
                            raw=dict(decision.raw),
                        )
                        decision = apply_confidence_floor(decision, settings.min_trade_confidence)

            if settings.memory_room_guard_enabled and decision.action in ("BUY", "SELL"):
                strategy_key = build_strategy_key(features, setup_eval)
                room_guard = memory.get_room_guardrail(
                    symbol=settings.symbol,
                    session=str(features.get("session") or ""),
                    setup_tag=setup_eval,
                    trend_direction=str(features.get("trend_direction") or ""),
                    volatility=str(features.get("volatility") or ""),
                    strategy_key=strategy_key,
                )
                raw = dict(decision.raw)
                raw["memory_room_guard"] = room_guard
                if room_guard.get("blocked") and settings.memory_room_guard_block_anti:
                    decision = Decision(
                        action="HOLD",
                        confidence=decision.confidence,
                        reason=f"{decision.reason}|memory_guard:anti_pattern:{room_guard.get('room')}",
                        raw=raw,
                    )
                else:
                    delta = float(room_guard.get("confidence_delta") or 0.0)
                    if delta != 0.0:
                        decision = Decision(
                            action=decision.action,
                            confidence=max(0.0, min(0.95, decision.confidence + delta)),
                            reason=f"{decision.reason}|memory_guard:{room_guard.get('room')}",
                            raw=raw,
                        )
                        decision = apply_confidence_floor(decision, settings.min_trade_confidence)

        if not risk.can_trade() and decision.action in ("BUY", "SELL"):
            decision = Decision(
                action="HOLD",
                confidence=0.0,
                reason="risk_block_session",
                raw=dict(decision.raw),
            )

        trade_volume = _requested_trade_volume(decision, settings)
        if decision.action in ("BUY", "SELL"):
            if not _shadow_probe_market_ok(market, features):
                decision = Decision(
                    action="HOLD",
                    confidence=0.0,
                    reason=f"{decision.reason}|market_quote_invalid",
                    raw=dict(decision.raw),
                )
                trade_volume = 0.0
            else:
                capped_volume, cap_reason = await _cap_trade_volume_for_exposure(
                    broker=broker,
                    execution=execution,
                    settings=settings,
                    symbol=settings.symbol,
                    action=decision.action,
                    requested_volume=trade_volume,
                    confidence=float(decision.confidence),
                )
                if capped_volume <= 0:
                    decision = Decision(
                        action="HOLD",
                        confidence=0.0,
                        reason=f"{decision.reason}|exposure_cap:{cap_reason}",
                        raw={**dict(decision.raw), "exposure_cap": cap_reason},
                    )
                    trade_volume = 0.0
                else:
                    trade_volume = capped_volume

        decision_counts[decision.action] += 1
        final_bucket = _reason_bucket(decision.reason)
        final_reason_counts[final_bucket] += 1
        day_bucket["final_decisions"][decision.action] += 1
        if base_decision.action in ("BUY", "SELL") and decision.action == "HOLD":
            blocker_counts[final_bucket] += 1
            day_bucket["blockers"][final_bucket] += 1
            if anticipated_strategy_key in day_bucket["risk_keys_at_open"]:
                day_bucket["carryover_risk_avoids"] += 1
        if decision.action in ("BUY", "SELL") and anticipated_strategy_key in day_bucket["skills_at_open"]:
            day_bucket["carryover_opportunity_entries"] += 1
        if (
            decision.action in ("BUY", "SELL")
            and anticipated_strategy_key in day_bucket["skills_at_open"]
            and (
                "skill_promotion:" in decision.reason
                or "|pattern_soft_gate:" in decision.reason
                or "|strategy_soft_gate:" in decision.reason
                or "|loss_streak_soft_gate:" in decision.reason
            )
        ):
            day_bucket["carryover_unlocks"] += 1
        outcome = await execution.execute_trade(
            symbol=settings.symbol,
            action=decision.action,
            volume=trade_volume,
            decision_reason=decision.reason,
            dry_run=False,
        )

        closed_positions = list(outcome.closes or ([] if outcome.close is None else [outcome.close]))
        if closed_positions:
            closed_contexts = list(open_contexts[: len(closed_positions)])
            for idx, close in enumerate(closed_positions):
                close_context = closed_contexts[idx] if idx < len(closed_contexts) else None
                await process_closed_trade(
                    {
                        "closed_utc": bar.ts_utc,
                        "side": close.side,
                        "volume": close.volume,
                        "entry_price": close.entry_price,
                        "exit_price": close.exit_price,
                        "pnl": close.pnl,
                        "source": bar.source,
                    },
                    close_context,
                    close_reason="flip_signal",
                )
            open_contexts = open_contexts[len(closed_positions) :]

        opened = decision.action in ("BUY", "SELL") and outcome.trade.message not in ("hold", "skip_same_side_open")
        if opened and outcome.trade.executed:
            tag = infer_setup_tag(features, decision.action)
            strategy_key = build_strategy_key(features, tag)
            open_contexts.append(
                {
                    "market": market.as_prompt_dict(),
                    "features": dict(features),
                    "decision": {
                        "action": decision.action,
                        "confidence": decision.confidence,
                        "reason": decision.reason,
                    },
                    "setup_tag": tag,
                    "strategy_key": strategy_key,
                    "created_ts": market.ts_unix,
                    "opened_utc": bar.ts_utc,
                    "source": bar.source,
                    "active_skill_keys": active_skill_keys,
                    "team_brief": team_brief,
                    "journal": _journal_structured(
                        market,
                        features,
                        decision,
                        settings,
                        tag,
                        strategy_key=strategy_key,
                    ),
                    "tags": [settings.symbol, tag, str(features.get("session") or ""), strategy_key],
                }
            )

        if settings.shadow_probe_enabled and decision.action == "HOLD":
            shadow_candidate = probe_candidate_decision
            shadow_bucket = probe_candidate_bucket
            shadow_reason = probe_candidate_reason
            if shadow_candidate is None and base_decision.action in ("BUY", "SELL") and _eligible_shadow_probe_bucket(final_bucket):
                shadow_candidate = base_decision
                shadow_bucket = final_bucket
                shadow_reason = decision.reason
            if (
                shadow_candidate is not None
                and shadow_candidate.action in ("BUY", "SELL")
                and float(shadow_candidate.confidence) >= float(settings.shadow_probe_min_confidence)
                and _shadow_probe_market_ok(market, features)
            ):
                existing_shadow = shadow_execution.open_position_for(settings.symbol)
                if existing_shadow is None or existing_shadow.side != shadow_candidate.action:
                    probe_volume = _shadow_probe_volume(settings)
                    shadow_outcome = await shadow_execution.execute_trade(
                        symbol=settings.symbol,
                        action=shadow_candidate.action,
                        volume=probe_volume,
                        decision_reason=f"shadow_probe:{shadow_bucket}:{shadow_reason}",
                        dry_run=True,
                    )
                    shadow_probe_counts[shadow_candidate.action] += 1
                    shadow_probe_blockers[shadow_bucket or "unknown"] += 1
                    shadow_closed_positions = list(
                        shadow_outcome.closes or ([] if shadow_outcome.close is None else [shadow_outcome.close])
                    )
                    if shadow_closed_positions:
                        closed_contexts = list(shadow_open_contexts[: len(shadow_closed_positions)])
                        for idx, close in enumerate(shadow_closed_positions):
                            close_context = closed_contexts[idx] if idx < len(closed_contexts) else None
                            await process_shadow_close(
                                {
                                    "closed_utc": bar.ts_utc,
                                    "side": close.side,
                                    "volume": close.volume,
                                    "entry_price": close.entry_price,
                                    "exit_price": close.exit_price,
                                    "pnl": close.pnl,
                                    "source": bar.source,
                                },
                                close_context,
                                close_reason="shadow_flip_signal",
                            )
                        shadow_open_contexts = shadow_open_contexts[len(shadow_closed_positions) :]

                    shadow_tag = infer_setup_tag(features, shadow_candidate.action)
                    shadow_key = build_strategy_key(features, shadow_tag)
                    shadow_open_contexts.append(
                        {
                            "market": market.as_prompt_dict(),
                            "features": dict(features),
                            "decision": {
                                "action": shadow_candidate.action,
                                "confidence": shadow_candidate.confidence,
                                "reason": shadow_candidate.reason,
                            },
                            "setup_tag": shadow_tag,
                            "strategy_key": shadow_key,
                            "created_ts": market.ts_unix,
                            "opened_utc": bar.ts_utc,
                            "source": bar.source,
                            "probe_blocker_bucket": shadow_bucket,
                            "probe_blocker_reason": shadow_reason,
                            "active_skill_keys": active_skill_keys,
                            "team_brief": team_brief,
                            "journal": _journal_structured(
                                market,
                                features,
                                shadow_candidate,
                                settings,
                                shadow_tag,
                                strategy_key=shadow_key,
                                extra={"probe_blocker_bucket": shadow_bucket, "probe_blocker_reason": shadow_reason},
                            ),
                            "tags": [settings.symbol, shadow_tag, str(features.get("session") or ""), shadow_key, "shadow-probe"],
                        }
                    )

    final_bar = bars[-1]
    broker.set_bar(final_bar)
    remaining_positions = execution.positions_for(settings.symbol)
    for idx, position in enumerate(remaining_positions):
        close_result = await broker.close_position(
            symbol=settings.symbol,
            position=position,
            reason="forced_end_of_backtest",
            dry_run=False,
        )
        sign = 1.0 if position.side == "BUY" else -1.0
        pnl = sign * (close_result.exit_price - position.entry_price) * position.volume
        close_context = open_contexts[idx] if idx < len(open_contexts) else None
        await process_closed_trade(
            {
                "closed_utc": _iso_utc(datetime.fromtimestamp(close_result.ts_unix, tz=UTC)),
                "side": position.side,
                "volume": position.volume,
                "entry_price": position.entry_price,
                "exit_price": close_result.exit_price,
                "pnl": pnl,
                "source": final_bar.source,
            },
            close_context,
            close_reason="forced_end",
        )

    remaining_shadow_positions = shadow_execution.positions_for(settings.symbol)
    for idx, position in enumerate(remaining_shadow_positions):
        close_result = await broker.close_position(
            symbol=settings.symbol,
            position=position,
            reason="forced_end_of_shadow_probe",
            dry_run=True,
        )
        sign = 1.0 if position.side == "BUY" else -1.0
        pnl = sign * (close_result.exit_price - position.entry_price) * position.volume
        close_context = shadow_open_contexts[idx] if idx < len(shadow_open_contexts) else None
        await process_shadow_close(
            {
                "closed_utc": _iso_utc(datetime.fromtimestamp(close_result.ts_unix, tz=UTC)),
                "side": position.side,
                "volume": position.volume,
                "entry_price": position.entry_price,
                "exit_price": close_result.exit_price,
                "pnl": pnl,
                "source": final_bar.source,
            },
            close_context,
            close_reason="shadow_forced_end",
        )

    skill_snapshot = skillbook.list_skills(symbol=settings.symbol, limit=50)
    mode = "heuristic_replay_learning" if settings.self_improvement_enabled else "heuristic_replay"
    report: Dict[str, Any] = {
        "run_id": run_id,
        "symbol": settings.symbol,
        "mode": mode,
        "window": window_meta,
        "coverage": coverage,
        "settings": {
            "default_volume": settings.default_volume,
            "min_trade_confidence": settings.min_trade_confidence,
            "pattern_min_win_rate": settings.pattern_min_win_rate,
            "pattern_min_sample_size": settings.pattern_min_sample_size,
            "entry_loss_streak_block": settings.entry_loss_streak_block,
            "risk_max_lot_per_1000_equity": settings.risk_max_lot_per_1000_equity,
            "risk_max_total_lot_per_symbol": settings.risk_max_total_lot_per_symbol,
            "memory_room_guard_enabled": settings.memory_room_guard_enabled,
            "self_improvement_enabled": settings.self_improvement_enabled,
            "agent_team_enabled": settings.agent_team_enabled,
            "skill_recall_top_k": settings.skill_recall_top_k,
            "source_policy": source_policy,
            "shadow_probe_enabled": settings.shadow_probe_enabled,
            "shadow_probe_volume_fraction": settings.shadow_probe_volume_fraction,
            "shadow_probe_min_confidence": settings.shadow_probe_min_confidence,
            "soft_gate_new_lane_enabled": settings.soft_gate_new_lane_enabled,
            "soft_gate_new_lane_max_trades": settings.soft_gate_new_lane_max_trades,
            "soft_gate_confidence_penalty": settings.soft_gate_confidence_penalty,
            "probation_trade_volume_fraction": settings.probation_trade_volume_fraction,
            "loss_streak_override_enabled": settings.loss_streak_override_enabled,
            "loss_streak_override_min_shadow_trades": settings.loss_streak_override_min_shadow_trades,
            "loss_streak_override_min_shadow_win_rate": settings.loss_streak_override_min_shadow_win_rate,
            "loss_streak_override_min_skill_trades": settings.loss_streak_override_min_skill_trades,
            "loss_streak_override_min_skill_edge": settings.loss_streak_override_min_skill_edge,
            "loss_streak_override_confidence_penalty": settings.loss_streak_override_confidence_penalty,
            "position_manager_enabled": settings.position_manager_enabled,
            "position_manager_max_hold_minutes": settings.position_manager_max_hold_minutes,
            "position_manager_tp_vol_multiplier": settings.position_manager_tp_vol_multiplier,
            "position_manager_sl_vol_multiplier": settings.position_manager_sl_vol_multiplier,
            "position_manager_risk_close_threshold": settings.position_manager_risk_close_threshold,
        },
        "diagnostics": {
            "base_decisions": dict(base_decision_counts),
            "base_reason_buckets": dict(base_reason_counts.most_common(20)),
            "final_reason_buckets": dict(final_reason_counts.most_common(20)),
            "blocker_buckets": dict(blocker_counts.most_common(20)),
            "skill_effects": dict(skill_effect_counts),
            "skill_recall_top": dict(skill_recall_counts.most_common(20)),
            "shadow_probe_counts": dict(shadow_probe_counts),
            "shadow_probe_blockers": dict(shadow_probe_blockers),
            "regime_counts": {name: dict(counter) for name, counter in regime_counts.items()},
        },
        "learning": {
            "enabled": settings.self_improvement_enabled,
            "skill_count": len(skill_snapshot),
            "skills_updated": int(sum(skill_update_counts.values())),
            "skill_update_counts": dict(skill_update_counts),
            "skills": [
                {
                    "skill_key": str(item.get("skill_key") or ""),
                    "title": str(item.get("title") or ""),
                    "summary": str(item.get("summary") or ""),
                    "stats": dict(item.get("stats") or {}),
                    "file_path": str(item.get("file_path") or ""),
                }
                for item in skill_snapshot[:6]
            ],
            "skill_files": [str(item.get("file_path") or "") for item in skill_snapshot if str(item.get("file_path") or "")],
        },
        "decisions": dict(decision_counts),
        "performance": perf.summary(),
        "shadow_performance": shadow_perf.summary(),
        "risk": risk.snapshot(),
        "strategy_registry_keys": len(registry.snapshot()),
        "memory_records": memory.count(),
        "daily_learning": [
            {
                **{
                    key: value
                    for key, value in bucket.items()
                    if key not in {"base_decisions", "final_decisions", "blockers"}
                },
                "base_decisions": dict(bucket["base_decisions"]),
                "final_decisions": dict(bucket["final_decisions"]),
                "blockers": dict(bucket["blockers"]),
            }
            for _, bucket in sorted(daily_learning.items())
        ],
        "trades": trades,
        "shadow_trades": shadow_trades,
    }

    report_path = run_dir / "report.json"
    summary_path = run_dir / "summary.md"
    report_path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")

    summary_lines = [
        f"# Backtest {run_id}",
        "",
        f"- Symbol: `{settings.symbol}`",
        f"- Mode: `{mode}`",
        f"- Source policy: `{source_policy}`",
        f"- Learning enabled: `{settings.self_improvement_enabled}`",
        f"- Local window: `{window_meta['start_local']}` to `{window_meta['end_local_exclusive']}` (exclusive end)",
        f"- UTC window: `{window_meta['start_utc']}` to `{window_meta['end_utc_exclusive']}` (exclusive end)",
        f"- Bars merged: `{coverage['merged_bar_count']}`",
        f"- Sources: `{json.dumps(coverage['merged_source_counts'], ensure_ascii=False)}`",
        f"- Performance: `{json.dumps(perf.summary(), ensure_ascii=False)}`",
        f"- Shadow performance: `{json.dumps(shadow_perf.summary(), ensure_ascii=False)}`",
        f"- Base decisions: `{json.dumps(dict(base_decision_counts), ensure_ascii=False)}`",
        f"- Decisions: `{json.dumps(dict(decision_counts), ensure_ascii=False)}`",
        f"- Trades closed: `{len(trades)}`",
        f"- Shadow probes closed: `{len(shadow_trades)}`",
        "",
        "## Top Blockers",
    ]
    if blocker_counts:
        for key, count in blocker_counts.most_common(8):
            summary_lines.append(f"- `{key}` => `{count}`")
    else:
        summary_lines.append("- none")
    summary_lines.extend(
        [
            "",
            "## Regime Mix",
            f"- Trend: `{json.dumps(dict(regime_counts['trend_direction']), ensure_ascii=False)}`",
            f"- Volatility: `{json.dumps(dict(regime_counts['volatility']), ensure_ascii=False)}`",
            f"- Consolidation: `{json.dumps(dict(regime_counts['consolidation']), ensure_ascii=False)}`",
            f"- Anticipated action: `{json.dumps(dict(regime_counts['anticipated_action']), ensure_ascii=False)}`",
            "",
            "## Learning",
            f"- Skills updated: `{int(sum(skill_update_counts.values()))}`",
            f"- Skill count: `{len(skill_snapshot)}`",
            f"- Skill effects: `{json.dumps(dict(skill_effect_counts), ensure_ascii=False)}`",
            f"- Shadow probe blockers: `{json.dumps(dict(shadow_probe_blockers), ensure_ascii=False)}`",
            "",
            "## Daily Carryover",
        ]
    )
    if daily_learning:
        for _, bucket in sorted(daily_learning.items()):
            summary_lines.append(
                f"- `{bucket['day']}` carryover_entries={bucket['carryover_opportunity_entries']} "
                f"carryover_unlocks={bucket['carryover_unlocks']} risk_avoids={bucket['carryover_risk_avoids']} "
                f"new_positive={len(bucket['new_positive_keys'])} new_negative={len(bucket['new_negative_keys'])}"
            )
    else:
        summary_lines.append("- none")
    summary_lines.extend(
        [
            "",
            "## Top Gaps",
        ]
    )
    top_gaps = coverage.get("gap_groups") or []
    if top_gaps:
        for item in top_gaps[:10]:
            summary_lines.append(
                f"- `{item['start_utc']}` -> `{item['end_utc']}` | missing_bars={item['missing_bars']} duration_min={item['duration_minutes']}"
            )
    else:
        summary_lines.append("- none")
    if skill_snapshot:
        summary_lines.extend(["", "## Top Skills"])
        for item in skill_snapshot[:5]:
            stats = dict(item.get("stats") or {})
            summary_lines.append(
                f"- `{item.get('skill_key')}` edge={stats.get('risk_adjusted_score')} trades={stats.get('trades_seen')} win_rate={stats.get('win_rate')}"
            )
    summary_path.write_text("\n".join(summary_lines) + "\n", encoding="utf-8")

    log.info(
        "Backtest complete run_id=%s bars=%s trades=%s equity=%s report=%s",
        run_id,
        coverage["merged_bar_count"],
        len(trades),
        report["performance"]["equity_last"],
        report_path,
    )
    return BacktestResult(run_dir=run_dir, report_path=report_path, summary_path=summary_path, report=report)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Replay Mempalac on historical Dexter data (read-only)")
    parser.add_argument("--start", required=True, help="Local start date YYYY-MM-DD (inclusive)")
    parser.add_argument("--end", required=True, help="Local end date YYYY-MM-DD (inclusive)")
    parser.add_argument("--timezone", default="Asia/Bangkok", help="Date interpretation timezone")
    parser.add_argument("--symbol", default=None, help="Override symbol (defaults to Settings.symbol)")
    parser.add_argument("--timeframe", default="5m", choices=sorted(TIMEFRAME_SECONDS), help="Replay bar size")
    parser.add_argument(
        "--dexter-root",
        default=r"D:\dexter_pro_v3_fixed\dexter_pro_v3_fixed",
        help="Read-only Dexter repo root that contains backtest/ and data/",
    )
    parser.add_argument(
        "--output-root",
        default=None,
        help="Directory for backtest artifacts (defaults to data/backtests/ under Mempalac data dir)",
    )
    parser.add_argument(
        "--source-policy",
        default="real_only",
        choices=sorted(SOURCE_POLICIES),
        help="Historical source policy: prefer real cTrader capture, require it, or use candle DB only.",
    )
    parser.add_argument(
        "--enable-learning",
        action=argparse.BooleanOptionalAction,
        default=False,
        help="Enable isolated Hermes-style self-improvement and skill recall during replay.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    result = asyncio.run(
        run_backtest(
            start_day=args.start,
            end_day=args.end,
            timezone_name=args.timezone,
            symbol=args.symbol,
            timeframe=args.timeframe,
            dexter_root=Path(args.dexter_root).resolve(),
            output_root=Path(args.output_root).resolve() if args.output_root else None,
            source_policy=args.source_policy,
            enable_learning=args.enable_learning,
        )
    )
    print(json.dumps(result.report, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
