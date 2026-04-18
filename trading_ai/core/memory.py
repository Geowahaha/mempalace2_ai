from __future__ import annotations

import json
import math
import time
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from time import sleep
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

from trading_ai.utils.logger import get_logger

log = get_logger(__name__)


@dataclass(slots=True)
class MemoryRecord:
    """
    Structured experience unit for Chroma.
    Document shape:
      { market, features, decision, result, score, setup_tag, strategy_key, journal, created_ts, tags }
    """

    market: Dict[str, Any]
    features: Dict[str, Any]
    decision: Dict[str, Any]
    result: Dict[str, Any]
    score: int
    setup_tag: str = "trend_follow"
    strategy_key: str = ""
    journal: str = ""
    wing: str = ""
    hall: str = ""
    room: str = ""
    created_ts: float = field(default_factory=time.time)
    tags: List[str] = field(default_factory=list)

    def to_document(self) -> str:
        body = {
            "market": self.market,
            "features": self.features,
            "decision": self.decision,
            "result": self.result,
            "score": int(self.score),
            "setup_tag": self.setup_tag,
            "strategy_key": self.strategy_key,
            "journal": self.journal,
            "wing": self.wing,
            "hall": self.hall,
            "room": self.room,
            "created_ts": self.created_ts,
            "tags": self.tags,
        }
        return json.dumps(body, ensure_ascii=False, sort_keys=True)


@dataclass(slots=True)
class MemoryNote:
    title: str
    content: str
    wing: str
    hall: str
    room: str
    note_type: str = "operator_note"
    hall_type: str = "hall_discoveries"
    symbol: str = ""
    session: str = ""
    setup_tag: str = ""
    strategy_key: str = ""
    importance: float = 0.5
    source: str = "manual"
    tags: List[str] = field(default_factory=list)
    created_ts: float = field(default_factory=time.time)

    def to_document(self) -> str:
        body = {
            "title": self.title,
            "content": self.content,
            "wing": self.wing,
            "hall": self.hall,
            "room": self.room,
            "note_type": self.note_type,
            "hall_type": self.hall_type,
            "symbol": self.symbol,
            "session": self.session,
            "setup_tag": self.setup_tag,
            "strategy_key": self.strategy_key,
            "importance": self.importance,
            "source": self.source,
            "tags": self.tags,
            "created_ts": self.created_ts,
        }
        return json.dumps(body, ensure_ascii=False, sort_keys=True)


@dataclass(slots=True)
class RecallHit:
    id: str
    document: str
    similarity: float
    weighted_score: float
    metadata: Dict[str, Any]


def _similarity_from_distance(d: float) -> float:
    if math.isnan(d):
        return 0.0
    return float(1.0 / (1.0 + max(d, 0.0)))


def _coerce_meta_value(v: Any) -> Any:
    if isinstance(v, (str, int, float, bool)) or v is None:
        return v
    return str(v)


def _slug(value: Any, *, default: str = "unknown") -> str:
    raw = str(value or "").strip().lower()
    if not raw:
        return default
    out: List[str] = []
    last_dash = False
    for ch in raw:
        if ch.isalnum():
            out.append(ch)
            last_dash = False
        elif not last_dash:
            out.append("-")
            last_dash = True
    return "".join(out).strip("-") or default


def _build_palace_taxonomy(record: MemoryRecord) -> Tuple[str, str, str]:
    symbol = _slug(record.market.get("symbol"), default="unknown-symbol")
    setup = _slug(record.setup_tag, default="unknown-setup")
    trend = _slug(record.features.get("trend_direction"), default="unknown-trend")
    volatility = _slug(record.features.get("volatility"), default="unknown-volatility")
    wing = record.wing or f"symbol:{symbol}"
    hall = record.hall or "hall_events"
    room = record.room or (record.strategy_key or f"{setup}:{trend}:{volatility}")
    return wing, hall, room


def _decode_document(text: str) -> Dict[str, Any]:
    try:
        body = json.loads(text)
    except Exception:
        body = {"journal": text}
    return body if isinstance(body, dict) else {"journal": text}


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _confidence_bucket(conf: float) -> str:
    if conf < 0.2:
        return "0.0-0.2"
    if conf < 0.4:
        return "0.2-0.4"
    if conf < 0.6:
        return "0.4-0.6"
    if conf < 0.8:
        return "0.6-0.8"
    return "0.8-1.0"


def _outcome_label(score: float) -> str:
    if score > 0:
        return "win"
    if score < 0:
        return "loss"
    return "neutral"


def _calibration_label(score: float, confidence: float) -> str:
    if score > 0 and confidence >= 0.75:
        return "confirmed_high_confidence_win"
    if score < 0 and confidence >= 0.75:
        return "overconfident_loss"
    if score > 0 and confidence < 0.55:
        return "underconfident_win"
    if score < 0 and confidence < 0.55:
        return "low_confidence_loss"
    if score == 0:
        return "neutral_outcome"
    return "uncertain_mixed"


def _memory_room_from_row(row: Dict[str, Any]) -> str:
    return str(row.get("room") or row.get("strategy_key") or "room:unknown")


def _mean(values: Iterable[float]) -> float:
    vals = [float(v) for v in values]
    return sum(vals) / float(len(vals)) if vals else 0.0


def _ordered_metric_rows(rows: List[Dict[str, Any]], *, key: str, limit: int = 12) -> List[Dict[str, Any]]:
    ordered = sorted(rows, key=lambda item: (-float(item.get(key) or 0.0), str(item.get("name") or "")))
    return ordered[:limit]


class MemoryEngine:
    """
    Vector memory with ChromaDB. Metadata supports filtering by session, volatility, trend.
    """

    def __init__(
        self,
        *,
        persist_path: Path,
        collection_name: str,
        score_weight: float = 0.35,
    ) -> None:
        self._persist_path = Path(persist_path)
        self._collection_name = collection_name
        self._score_weight = max(0.0, min(1.0, score_weight))
        self._collection = None
        self._client = None
        self._normalized_rows_cache: Optional[List[Dict[str, Any]]] = None
        self._normalized_rows_cache_count: int = -1
        self._init_chroma()

    def _init_chroma(self) -> None:
        try:
            import chromadb
            from chromadb.config import Settings as ChromaSettings
        except ImportError as exc:
            raise ImportError("chromadb is required: pip install chromadb") from exc

        self._persist_path.mkdir(parents=True, exist_ok=True)
        last_exc: Optional[Exception] = None
        for attempt in range(4):
            try:
                self._client = chromadb.PersistentClient(
                    path=str(self._persist_path),
                    settings=ChromaSettings(anonymized_telemetry=False),
                )
                self._collection = self._client.get_or_create_collection(
                    name=self._collection_name,
                    metadata={"hnsw:space": "cosine"},
                )
                break
            except Exception as exc:
                last_exc = exc
                msg = str(exc).lower()
                if "table collections already exists" not in msg or attempt == 3:
                    raise
                sleep(0.35 * (attempt + 1))
        if self._collection is None:
            assert last_exc is not None
            raise last_exc
        log.info("Memory: Chroma persistence at %s", self._persist_path)

    def _build_metadata(self, record: MemoryRecord, extra: Optional[Dict[str, Any]]) -> Dict[str, Any]:
        feat = record.features or {}
        sym = str(record.market.get("symbol") or feat.get("symbol") or "")
        wing, hall, room = _build_palace_taxonomy(record)
        confidence = float(record.decision.get("confidence") or 0.0)
        outcome_score = float(record.score)
        lane_key = (
            str(record.strategy_key or "").strip()
            or f"{sym}|{feat.get('session') or ''}|{record.setup_tag}|{room}"
        )
        meta: Dict[str, Any] = {
            "symbol": sym,
            "session": str(feat.get("session") or ""),
            "volatility": str(feat.get("volatility") or ""),
            "trend_direction": str(feat.get("trend_direction") or ""),
            "setup_tag": str(record.setup_tag or ""),
            "strategy_key": str(record.strategy_key or ""),
            "outcome_score": outcome_score,
            "confidence": confidence,
            "action": str(record.decision.get("action") or ""),
            "created_ts": float(record.created_ts),
            "wing": wing,
            "hall": hall,
            "room": room,
            "memory_type": "trade_journal",
            "hall_type": "hall_events",
            "confidence_bucket": _confidence_bucket(confidence),
            "outcome_label": _outcome_label(outcome_score),
            "calibration_label": _calibration_label(outcome_score, confidence),
            "lane_key": lane_key,
        }
        if isinstance(record.result.get("pnl"), (int, float)):
            meta["pnl"] = float(record.result["pnl"])
        if extra:
            for k, v in extra.items():
                cv = _coerce_meta_value(v)
                if isinstance(cv, (str, int, float, bool)):
                    meta[str(k)] = cv
        return meta

    def _build_note_metadata(self, note: MemoryNote) -> Dict[str, Any]:
        return {
            "symbol": str(note.symbol or ""),
            "session": str(note.session or ""),
            "volatility": "",
            "trend_direction": "",
            "setup_tag": str(note.setup_tag or ""),
            "strategy_key": str(note.strategy_key or ""),
            "outcome_score": 0.0,
            "confidence": 0.0,
            "action": "",
            "created_ts": float(note.created_ts),
            "wing": str(note.wing),
            "hall": str(note.hall),
            "room": str(note.room),
            "memory_type": str(note.note_type),
            "hall_type": str(note.hall_type),
            "importance": float(max(0.0, min(1.0, note.importance))),
            "source": str(note.source or "manual"),
            "lane_key": str(note.strategy_key or ""),
            "note_title": str(note.title or ""),
            "tags_csv": ",".join(str(tag) for tag in note.tags),
        }

    def _add_document(self, *, doc_id: str, document: str, metadata: Dict[str, Any]) -> str:
        self._collection.add(ids=[doc_id], documents=[document], metadatas=[metadata])
        self._normalized_rows_cache = None
        self._normalized_rows_cache_count = -1
        return doc_id

    def store_memory(self, record: MemoryRecord, *, extra_metadata: Optional[Dict[str, Any]] = None) -> str:
        doc_id = str(uuid.uuid4())
        doc = record.to_document()
        meta = self._build_metadata(record, extra_metadata)
        self._add_document(doc_id=doc_id, document=doc, metadata=meta)
        log.debug(
            "Stored memory id=%s session=%s vol=%s trend=%s score=%s",
            doc_id,
            meta.get("session"),
            meta.get("volatility"),
            meta.get("trend_direction"),
            record.score,
        )
        return doc_id

    def store_note(self, note: MemoryNote) -> str:
        doc_id = str(uuid.uuid4())
        self._add_document(
            doc_id=doc_id,
            document=note.to_document(),
            metadata=self._build_note_metadata(note),
        )
        log.debug(
            "Stored note id=%s hall=%s room=%s type=%s",
            doc_id,
            note.hall,
            note.room,
            note.note_type,
        )
        return doc_id

    def _query_weighted(
        self,
        query: str,
        *,
        top_k: int,
        where: Optional[Dict[str, Any]],
        total_count: Optional[int] = None,
    ) -> List[RecallHit]:
        total = int(total_count) if total_count is not None else int(self._collection.count())
        if total == 0:
            return []
        n_fetch = min(max(top_k * 3, top_k), total)
        try:
            res = self._collection.query(
                query_texts=[query],
                n_results=n_fetch,
                where=where,
                include=["documents", "metadatas", "distances"],
            )
        except Exception as exc:
            log.warning("Chroma query failed where=%s: %s", where, exc)
            raise
        ids: Sequence[str] = res.get("ids", [[]])[0] or []
        docs: Sequence[Optional[str]] = res.get("documents", [[]])[0] or []
        metas: Sequence[Optional[Dict[str, Any]]] = res.get("metadatas", [[]])[0] or []
        dists: Sequence[Optional[float]] = res.get("distances", [[]])[0] or []

        hits: List[RecallHit] = []
        for i, did in enumerate(ids):
            dist = float(dists[i]) if i < len(dists) and dists[i] is not None else 1.0
            sim = _similarity_from_distance(dist)
            meta = dict(metas[i] or {})
            mem_score = float(meta.get("outcome_score", 0.0))
            norm_mem = (mem_score + 1.0) / 2.0
            w = self._score_weight
            weighted = (1.0 - w) * sim + w * norm_mem
            doc_text = docs[i] or ""
            hits.append(
                RecallHit(
                    id=did,
                    document=doc_text,
                    similarity=sim,
                    weighted_score=weighted,
                    metadata=meta,
                )
            )
        hits.sort(key=lambda h: h.weighted_score, reverse=True)
        return hits[:top_k]

    def recall_memory(
        self,
        query: str,
        *,
        top_k: int = 8,
        where: Optional[Dict[str, Any]] = None,
    ) -> List[RecallHit]:
        return self._query_weighted(query, top_k=top_k, where=where)

    def recall_palace(
        self,
        query: str,
        *,
        top_k: int = 8,
        wing: Optional[str] = None,
        hall: Optional[str] = None,
        room: Optional[str] = None,
    ) -> List[RecallHit]:
        parts: List[Dict[str, Any]] = []
        if wing:
            parts.append({"wing": {"$eq": wing}})
        if hall:
            parts.append({"hall": {"$eq": hall}})
        if room:
            parts.append({"room": {"$eq": room}})
        if not parts:
            where = None
        elif len(parts) == 1:
            where = parts[0]
        else:
            where = {"$and": parts}
        return self.recall_memory(query, top_k=top_k, where=where)

    def recall_similar_trades(
        self,
        features: Dict[str, Any],
        *,
        symbol: str,
        top_k: int = 5,
    ) -> List[RecallHit]:
        """
        Prefer memories matching session, volatility band, and trend; widen filters if sparse.
        """
        sess = str(features.get("session") or "")
        vol = str(features.get("volatility") or "")
        trend = str(features.get("trend_direction") or "")
        query = f"{symbol} | {sess} | {vol} | {trend} | {json.dumps(features, sort_keys=True)}"

        trade_cond = {"memory_type": {"$eq": "trade_journal"}}
        sym_cond = {"symbol": {"$eq": symbol}}
        strategies: List[Optional[Dict[str, Any]]] = [
            {
                "$and": [
                    trade_cond,
                    sym_cond,
                    {"session": {"$eq": sess}},
                    {"volatility": {"$eq": vol}},
                    {"trend_direction": {"$eq": trend}},
                ]
            },
            {"$and": [trade_cond, sym_cond, {"session": {"$eq": sess}}, {"volatility": {"$eq": vol}}]},
            {"$and": [trade_cond, sym_cond, {"session": {"$eq": sess}}, {"trend_direction": {"$eq": trend}}]},
            {"$and": [trade_cond, sym_cond, {"session": {"$eq": sess}}]},
            {"$and": [trade_cond, sym_cond]},
            trade_cond,
        ]

        total = int(self._collection.count())
        if total <= 0:
            return []
        seen_ids: set[str] = set()
        merged: List[RecallHit] = []
        for w in strategies:
            try:
                hits = self._query_weighted(
                    query,
                    top_k=top_k,
                    where=w,
                    total_count=total,
                )
            except Exception:
                continue
            for h in hits:
                if h.id not in seen_ids:
                    seen_ids.add(h.id)
                    merged.append(h)
            if len(merged) >= top_k:
                break
        return merged[:top_k]

    def list_all_structured_experiences(self, *, limit: Optional[int] = None) -> List[Dict[str, Any]]:
        """Return raw Chroma rows (document JSON + metadata) for pattern aggregation."""
        try:
            kwargs: Dict[str, Any] = {"include": ["documents", "metadatas"]}
            if limit is not None:
                kwargs["limit"] = int(limit)
            batch = self._collection.get(**kwargs)
        except Exception as exc:
            log.warning("list_all_structured_experiences failed: %s", exc)
            return []
        ids = batch.get("ids") or []
        docs = batch.get("documents") or []
        metas = batch.get("metadatas") or []
        out: List[Dict[str, Any]] = []
        for i, doc_id in enumerate(ids):
            doc = docs[i] if i < len(docs) else None
            meta = metas[i] if i < len(metas) else {}
            if not doc:
                continue
            out.append({"id": doc_id, "document": str(doc), "metadata": dict(meta or {})})
        return out

    def list_notes(
        self,
        *,
        wing: Optional[str] = None,
        hall: Optional[str] = None,
        room: Optional[str] = None,
        hall_type: Optional[str] = None,
        note_type: Optional[str] = None,
        symbol: Optional[str] = None,
        session: Optional[str] = None,
        limit: int = 50,
    ) -> List[Dict[str, Any]]:
        rows = self._normalized_rows()
        out = [
            row
            for row in rows
            if row["memory_type"] != "trade_journal"
            and (wing is None or row["wing"] == wing)
            and (hall is None or row["hall"] == hall)
            and (room is None or row["room"] == room)
            and (hall_type is None or row["hall_type"] == hall_type)
            and (note_type is None or row["memory_type"] == note_type)
            and (symbol is None or row["symbol"] == symbol)
            and (session is None or row["session"] == session)
        ]
        out.sort(key=lambda row: (float(row["importance"]), float(row["created_ts"])), reverse=True)
        return out[: max(1, min(int(limit), 500))]

    def _normalized_rows(self) -> List[Dict[str, Any]]:
        try:
            current_count = int(self._collection.count())
        except Exception:
            current_count = -1
        if (
            self._normalized_rows_cache is not None
            and current_count >= 0
            and current_count == self._normalized_rows_cache_count
        ):
            return list(self._normalized_rows_cache)

        rows: List[Dict[str, Any]] = []
        for item in self.list_all_structured_experiences():
            meta = dict(item.get("metadata") or {})
            body = _decode_document(str(item.get("document") or ""))
            market = dict(body.get("market") or {})
            features = dict(body.get("features") or {})
            decision = dict(body.get("decision") or {})
            result = dict(body.get("result") or {})
            score = _safe_float(body.get("score", meta.get("outcome_score")), 0.0)
            confidence = _safe_float(decision.get("confidence", meta.get("confidence")), 0.0)
            pnl = _safe_float(result.get("pnl", meta.get("pnl")), 0.0)
            session = str(features.get("session") or meta.get("session") or "")
            symbol = str(market.get("symbol") or meta.get("symbol") or "")
            strategy_key = str(body.get("strategy_key") or meta.get("strategy_key") or "")
            setup_tag = str(body.get("setup_tag") or meta.get("setup_tag") or "")
            wing = str(meta.get("wing") or body.get("wing") or f"symbol:{_slug(symbol)}")
            hall = str(meta.get("hall") or body.get("hall") or f"session:{_slug(session)}")
            room = str(meta.get("room") or body.get("room") or strategy_key or "room:unknown")
            lane_key = str(meta.get("lane_key") or strategy_key or f"{symbol}|{session}|{setup_tag}|{room}")
            rows.append(
                {
                    "id": str(item.get("id") or ""),
                    "market": market,
                    "features": features,
                    "decision": decision,
                    "result": result,
                    "score": score,
                    "confidence": confidence,
                    "pnl": pnl,
                    "symbol": symbol,
                    "session": session,
                    "wing": wing,
                    "hall": hall,
                    "hall_type": str(meta.get("hall_type") or "hall_events"),
                    "memory_type": str(meta.get("memory_type") or "trade_journal"),
                    "room": room,
                    "strategy_key": strategy_key,
                    "setup_tag": setup_tag,
                    "action": str(decision.get("action") or meta.get("action") or ""),
                    "trend_direction": str(features.get("trend_direction") or meta.get("trend_direction") or ""),
                    "volatility": str(features.get("volatility") or meta.get("volatility") or ""),
                    "created_ts": _safe_float(meta.get("created_ts", body.get("created_ts")), 0.0),
                    "importance": _safe_float(meta.get("importance", body.get("importance")), 0.0),
                    "title": str(body.get("title") or meta.get("note_title") or ""),
                    "content": str(body.get("content") or ""),
                    "source": str(body.get("source") or meta.get("source") or ""),
                    "outcome_label": _outcome_label(score),
                    "confidence_bucket": _confidence_bucket(confidence),
                    "calibration_label": _calibration_label(score, confidence),
                    "lane_key": lane_key,
                    "journal": str(body.get("journal") or ""),
                    "tags": list(body.get("tags") or []),
                }
            )
        self._normalized_rows_cache = list(rows)
        self._normalized_rows_cache_count = current_count
        return rows

    def build_wake_up_context(
        self,
        *,
        symbol: str,
        session: Optional[str] = None,
        top_k: int = 6,
        note_top_k: int = 6,
    ) -> str:
        rows = [
            item
            for item in self._normalized_rows()
            if str(item.get("symbol") or "") == symbol and str(item.get("memory_type") or "") == "trade_journal"
        ]
        if session is not None:
            rows = [
                item
                for item in rows
                if str(item.get("session") or "") == session
            ]
        if not rows:
            target = f"{symbol}/{session}" if session else symbol
            base = [f"L1 market memory for {target}: no stored trade journals yet."]
        else:
            def sort_key(item: Dict[str, Any]) -> Tuple[float, float]:
                return (
                    float(item.get("score") or 0.0),
                    float(item.get("created_ts") or 0.0),
                )

            recent = sorted(rows, key=sort_key, reverse=True)[: max(top_k * 2, top_k)]
            wins = [item for item in recent if float(item.get("score") or 0.0) > 0.0]
            losses = [item for item in recent if float(item.get("score") or 0.0) < 0.0]
            chosen = (wins[: max(1, top_k // 2)] + losses[: max(1, top_k // 2)])[:top_k]
            if not chosen:
                chosen = recent[:top_k]

            base = [f"L1 market memory for {symbol}" + (f" in {session}" if session else "")]
            for idx, item in enumerate(chosen, start=1):
                decision = dict(item.get("decision") or {})
                features = dict(item.get("features") or {})
                reason = str(decision.get("reason") or item.get("journal") or "").replace("\n", " ").strip()
                if len(reason) > 180:
                    reason = reason[:177] + "..."
                base.append(
                    (
                        f"{idx}. room={item.get('room')} score={item.get('score')} "
                        f"action={decision.get('action')} pnl={item.get('pnl')} "
                        f"trend={features.get('trend_direction')} vol={features.get('volatility')} "
                        f"reason={reason or 'n/a'}"
                    )
                )

        notes = [
            row
            for row in self._normalized_rows()
            if row["memory_type"] != "trade_journal"
            and not str(row["memory_type"] or "").startswith("execution_")
            and (not str(row["symbol"]) or str(row["symbol"]) == symbol)
            and (session is None or not str(row["session"]) or str(row["session"]) == session)
        ]
        notes.sort(
            key=lambda row: (
                float(row.get("importance") or 0.0),
                1.0 if str(row.get("symbol") or "") == symbol else 0.0,
                1.0 if session and str(row.get("session") or "") == session else 0.0,
                float(row.get("created_ts") or 0.0),
            ),
            reverse=True,
        )
        notes = notes[: max(1, min(int(note_top_k), 12))]
        if notes:
            base.append("L1 palace notes")
            for item in notes[:6]:
                text = str(item.get("content") or item.get("journal") or "").replace("\n", " ").strip()
                if len(text) > 140:
                    text = text[:137] + "..."
                base.append(
                    f"- hall={item.get('hall')} room={item.get('room')} type={item.get('memory_type')} "
                    f"importance={item.get('importance')} note={text or item.get('title') or 'n/a'}"
                )
        return "\n".join(base)

    def get_taxonomy(self) -> Dict[str, Any]:
        total = 0
        wings: Dict[str, int] = {}
        halls: Dict[str, int] = {}
        rooms: Dict[str, int] = {}
        sessions: Dict[str, int] = {}
        hall_types: Dict[str, int] = {}
        memory_types: Dict[str, int] = {}
        for item in self._normalized_rows():
            total += 1
            wing = str(item.get("wing") or "wing:unknown")
            hall = str(item.get("hall") or "hall:unknown")
            room = str(item.get("room") or "room:unknown")
            session = str(item.get("session") or "session:unknown")
            hall_type = str(item.get("hall_type") or "hall:unknown")
            memory_type = str(item.get("memory_type") or "memory:unknown")
            wings[wing] = wings.get(wing, 0) + 1
            halls[hall] = halls.get(hall, 0) + 1
            rooms[room] = rooms.get(room, 0) + 1
            sessions[session] = sessions.get(session, 0) + 1
            hall_types[hall_type] = hall_types.get(hall_type, 0) + 1
            memory_types[memory_type] = memory_types.get(memory_type, 0) + 1
        return {
            "total_memories": total,
            "wings": _ordered_counts(wings),
            "halls": _ordered_counts(halls),
            "rooms": _ordered_counts(rooms),
            "sessions": _ordered_counts(sessions),
            "hall_types": _ordered_counts(hall_types),
            "memory_types": _ordered_counts(memory_types),
        }

    def get_memory_intelligence(self) -> Dict[str, Any]:
        rows = self._normalized_rows()
        if not rows:
            return {
                "summary": {
                    "total_trades": 0,
                    "wins": 0,
                    "losses": 0,
                    "neutrals": 0,
                    "note_memories": 0,
                    "avg_pnl": 0.0,
                    "avg_confidence": 0.0,
                    "avg_win_confidence": 0.0,
                    "avg_loss_confidence": 0.0,
                    "overconfident_losses": 0,
                    "underconfident_wins": 0,
                },
                "winner_rooms": [],
                "danger_rooms": [],
                "opportunity_rooms": [],
                "confidence_calibration": [],
                "lane_scoreboard": [],
                "anti_pattern_rooms": [],
                "promotion_pipeline": [],
                "note_memory_overview": [],
                "tunnels": [],
            }

        trade_rows = [row for row in rows if row["memory_type"] == "trade_journal"]
        note_rows = [row for row in rows if row["memory_type"] != "trade_journal"]

        wins = [row for row in trade_rows if row["score"] > 0]
        losses = [row for row in trade_rows if row["score"] < 0]
        neutrals = [row for row in trade_rows if row["score"] == 0]

        room_groups: Dict[str, List[Dict[str, Any]]] = {}
        lane_groups: Dict[str, List[Dict[str, Any]]] = {}
        confidence_groups: Dict[str, List[Dict[str, Any]]] = {}
        note_groups: Dict[str, List[Dict[str, Any]]] = {}

        for row in trade_rows:
            room_groups.setdefault(_memory_room_from_row(row), []).append(row)
            lane_groups.setdefault(str(row["lane_key"]), []).append(row)
            confidence_groups.setdefault(str(row["confidence_bucket"]), []).append(row)
        for row in note_rows:
            note_groups.setdefault(str(row["hall"]), []).append(row)

        room_rows: List[Dict[str, Any]] = []
        tunnel_rows: List[Dict[str, Any]] = []
        for room_name, items in room_groups.items():
            score_count = len(items)
            win_rows = [row for row in items if row["score"] > 0]
            loss_rows = [row for row in items if row["score"] < 0]
            win_rate = len(win_rows) / float(score_count) if score_count else 0.0
            loss_rate = len(loss_rows) / float(score_count) if score_count else 0.0
            avg_conf = _mean(row["confidence"] for row in items)
            avg_pnl = _mean(row["pnl"] for row in items)
            avg_win_conf = _mean(row["confidence"] for row in win_rows)
            avg_loss_conf = _mean(row["confidence"] for row in loss_rows)
            overconfident_losses = sum(1 for row in loss_rows if row["confidence"] >= 0.75)
            underconfident_wins = sum(1 for row in win_rows if row["confidence"] < 0.55)
            unique_wings = sorted({str(row["wing"]) for row in items})
            unique_halls = sorted({str(row["hall"]) for row in items})
            unique_sessions = sorted({str(row["session"]) for row in items if str(row["session"])})
            unique_symbols = sorted({str(row["symbol"]) for row in items if str(row["symbol"])})
            unique_strategies = sorted(
                {str(row["strategy_key"]) for row in items if str(row["strategy_key"])}
            )
            room_rows.append(
                {
                    "name": room_name,
                    "count": score_count,
                    "wins": len(win_rows),
                    "losses": len(loss_rows),
                    "win_rate": round(win_rate, 4),
                    "loss_rate": round(loss_rate, 4),
                    "avg_pnl": round(avg_pnl, 6),
                    "avg_confidence": round(avg_conf, 4),
                    "avg_win_confidence": round(avg_win_conf, 4),
                    "avg_loss_confidence": round(avg_loss_conf, 4),
                    "overconfident_losses": overconfident_losses,
                    "underconfident_wins": underconfident_wins,
                    "symbols": unique_symbols,
                    "sessions": unique_sessions,
                    "wings": unique_wings,
                    "halls": unique_halls,
                    "strategy_keys": unique_strategies,
                }
            )
            if len(unique_wings) > 1 or len(unique_halls) > 1 or len(unique_symbols) > 1:
                tunnel_rows.append(
                    {
                        "room": room_name,
                        "count": score_count,
                        "wings": unique_wings,
                        "halls": unique_halls,
                        "symbols": unique_symbols,
                        "sessions": unique_sessions,
                    }
                )

        winner_rooms = [
            row
            for row in room_rows
            if row["count"] >= 2 and row["wins"] >= 2 and row["win_rate"] >= 0.6
        ]
        danger_rooms = [
            row
            for row in room_rows
            if row["count"] >= 2
            and (row["loss_rate"] >= 0.6 or row["overconfident_losses"] >= 2 or row["avg_pnl"] < 0.0)
        ]
        opportunity_rooms = [
            row
            for row in room_rows
            if row["count"] >= 2
            and (
                (row["win_rate"] >= 0.6 and row["avg_confidence"] < 0.6)
                or (row["wins"] >= 2 and row["underconfident_wins"] >= 1)
            )
        ]
        anti_pattern_rooms = [
            row
            for row in room_rows
            if row["count"] >= 2
            and (
                row["loss_rate"] >= 0.7
                or row["overconfident_losses"] >= 2
                or (row["avg_pnl"] < 0.0 and row["avg_loss_confidence"] >= 0.65)
            )
        ]

        lane_rows: List[Dict[str, Any]] = []
        for lane_key, items in lane_groups.items():
            count = len(items)
            win_rows = [row for row in items if row["score"] > 0]
            loss_rows = [row for row in items if row["score"] < 0]
            wr = len(win_rows) / float(count) if count else 0.0
            pnl_sum = sum(float(row["pnl"]) for row in items)
            avg_conf = _mean(row["confidence"] for row in items)
            room_name = _memory_room_from_row(items[0])
            lane_rows.append(
                {
                    "name": lane_key,
                    "strategy_key": items[0]["strategy_key"],
                    "room": room_name,
                    "symbol": items[0]["symbol"],
                    "session": items[0]["session"],
                    "setup_tag": items[0]["setup_tag"],
                    "count": count,
                    "wins": len(win_rows),
                    "losses": len(loss_rows),
                    "win_rate": round(wr, 4),
                    "pnl_sum": round(pnl_sum, 6),
                    "avg_confidence": round(avg_conf, 4),
                    "ranking_hint": round(wr * pnl_sum, 6),
                }
            )

        promotion_rows: List[Dict[str, Any]] = []
        for lane in lane_rows:
            recommendation = "hold"
            if lane["count"] >= 15 and lane["win_rate"] >= 0.68 and lane["pnl_sum"] > 0.0:
                recommendation = "promote_to_live"
            elif lane["count"] >= 5 and lane["win_rate"] >= 0.62 and lane["pnl_sum"] > 0.0:
                recommendation = "promote_to_shadow"
            elif lane["count"] >= 5 and (lane["win_rate"] <= 0.42 or lane["pnl_sum"] < 0.0):
                recommendation = "demote_to_lab"
            promotion_rows.append({**lane, "recommendation": recommendation})

        calibration_rows: List[Dict[str, Any]] = []
        for bucket in ("0.0-0.2", "0.2-0.4", "0.4-0.6", "0.6-0.8", "0.8-1.0"):
            items = confidence_groups.get(bucket, [])
            if not items:
                continue
            win_rows = [row for row in items if row["score"] > 0]
            loss_rows = [row for row in items if row["score"] < 0]
            neutral_rows = [row for row in items if row["score"] == 0]
            calibration_rows.append(
                {
                    "bucket": bucket,
                    "count": len(items),
                    "wins": len(win_rows),
                    "losses": len(loss_rows),
                    "neutrals": len(neutral_rows),
                    "win_rate": round(len(win_rows) / float(len(items)), 4),
                    "avg_pnl": round(_mean(row["pnl"] for row in items), 6),
                    "avg_confidence": round(_mean(row["confidence"] for row in items), 4),
                }
            )

        note_overview: List[Dict[str, Any]] = []
        for hall_name, items in note_groups.items():
            note_overview.append(
                {
                    "name": hall_name,
                    "count": len(items),
                    "avg_importance": round(_mean(row["importance"] for row in items), 4),
                    "sources": sorted({str(row["source"]) for row in items if str(row["source"])}),
                    "rooms": sorted({str(row["room"]) for row in items if str(row["room"])}),
                }
            )

        return {
            "summary": {
                "total_trades": len(trade_rows),
                "wins": len(wins),
                "losses": len(losses),
                "neutrals": len(neutrals),
                "note_memories": len(note_rows),
                "avg_pnl": round(_mean(row["pnl"] for row in trade_rows), 6),
                "avg_confidence": round(_mean(row["confidence"] for row in trade_rows), 4),
                "avg_win_confidence": round(_mean(row["confidence"] for row in wins), 4),
                "avg_loss_confidence": round(_mean(row["confidence"] for row in losses), 4),
                "overconfident_losses": sum(1 for row in losses if row["confidence"] >= 0.75),
                "underconfident_wins": sum(1 for row in wins if row["confidence"] < 0.55),
            },
            "winner_rooms": _ordered_metric_rows(winner_rooms, key="win_rate"),
            "danger_rooms": _ordered_metric_rows(danger_rooms, key="loss_rate"),
            "opportunity_rooms": _ordered_metric_rows(opportunity_rooms, key="win_rate"),
            "anti_pattern_rooms": _ordered_metric_rows(anti_pattern_rooms, key="loss_rate"),
            "confidence_calibration": calibration_rows,
            "lane_scoreboard": _ordered_metric_rows(lane_rows, key="ranking_hint"),
            "promotion_pipeline": _ordered_metric_rows(
                [row for row in promotion_rows if row["recommendation"] != "hold"],
                key="ranking_hint",
            ),
            "note_memory_overview": _ordered_metric_rows(note_overview, key="count"),
            "tunnels": sorted(
                tunnel_rows,
                key=lambda item: (-int(item.get("count") or 0), str(item.get("room") or "")),
            )[:12],
        }

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
        room_name = str(strategy_key or f"{setup_tag}:{trend_direction}:{volatility}")
        room_name = room_name or "room:unknown"
        intel = self.get_memory_intelligence()
        winner_rooms = list(intel.get("winner_rooms") or [])
        danger_rooms = list(intel.get("danger_rooms") or [])
        anti_pattern_rooms = list(intel.get("anti_pattern_rooms") or [])
        opportunity_rooms = list(intel.get("opportunity_rooms") or [])
        note_rows = [
            row
            for row in self._normalized_rows()
            if row["memory_type"] != "trade_journal"
            and str(row.get("room") or "") == room_name
            and (not str(row.get("symbol") or "") or str(row.get("symbol") or "") == symbol)
            and (not session or not str(row.get("session") or "") or str(row.get("session") or "") == session)
        ]
        note_rows.sort(
            key=lambda row: (float(row.get("importance") or 0.0), float(row.get("created_ts") or 0.0)),
            reverse=True,
        )
        note_rows = note_rows[:10]

        winner = next((row for row in winner_rooms if str(row.get("name") or "") == room_name), None)
        danger = next((row for row in danger_rooms if str(row.get("name") or "") == room_name), None)
        anti = next((row for row in anti_pattern_rooms if str(row.get("name") or "") == room_name), None)
        opportunity = next((row for row in opportunity_rooms if str(row.get("name") or "") == room_name), None)

        blocked = anti is not None
        caution = danger is not None and anti is None
        confidence_delta = 0.0
        if winner is not None:
            confidence_delta += 0.05
        if opportunity is not None:
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
            "winner_room": winner,
            "danger_room": danger,
            "anti_pattern_room": anti,
            "opportunity_room": opportunity,
            "supporting_notes": note_rows,
        }

    def build_daily_analyst_packet(self) -> Dict[str, Any]:
        intel = self.get_memory_intelligence()
        return {
            "brief": self.build_daily_analyst_brief(),
            "intelligence": intel,
            "taxonomy": self.get_taxonomy(),
            "top_notes": self.list_notes(limit=20),
        }

    def build_daily_analyst_brief(self) -> str:
        intel = self.get_memory_intelligence()
        summary = dict(intel.get("summary") or {})
        lines = [
            "Daily trading memory brief",
            (
                f"trades={summary.get('total_trades', 0)} wins={summary.get('wins', 0)} "
                f"losses={summary.get('losses', 0)} neutrals={summary.get('neutrals', 0)} "
                f"avg_pnl={summary.get('avg_pnl', 0.0)} avg_conf={summary.get('avg_confidence', 0.0)}"
            ),
        ]

        def append_section(title: str, rows: List[Dict[str, Any]], fields: Sequence[str]) -> None:
            lines.append(title)
            if not rows:
                lines.append("  - none")
                return
            for item in rows[:5]:
                bits = [f"{field}={item.get(field)}" for field in fields]
                lines.append(f"  - {item.get('name', item.get('room', 'unknown'))}: " + " ".join(bits))

        append_section(
            "Winner rooms",
            list(intel.get("winner_rooms") or []),
            ("count", "win_rate", "avg_pnl", "avg_confidence"),
        )
        append_section(
            "Danger rooms",
            list(intel.get("danger_rooms") or []),
            ("count", "loss_rate", "avg_pnl", "overconfident_losses"),
        )
        append_section(
            "Opportunity rooms",
            list(intel.get("opportunity_rooms") or []),
            ("count", "win_rate", "avg_pnl", "underconfident_wins"),
        )
        append_section(
            "Anti-pattern rooms",
            list(intel.get("anti_pattern_rooms") or []),
            ("count", "loss_rate", "avg_pnl", "overconfident_losses"),
        )
        append_section(
            "Lane scoreboard",
            list(intel.get("lane_scoreboard") or []),
            ("count", "win_rate", "pnl_sum", "ranking_hint"),
        )
        append_section(
            "Promotion pipeline",
            list(intel.get("promotion_pipeline") or []),
            ("count", "win_rate", "pnl_sum", "recommendation"),
        )
        append_section(
            "Note halls",
            list(intel.get("note_memory_overview") or []),
            ("count", "avg_importance"),
        )
        lines.append("Confidence calibration")
        for item in list(intel.get("confidence_calibration") or [])[:5]:
            lines.append(
                f"  - bucket={item.get('bucket')} count={item.get('count')} "
                f"win_rate={item.get('win_rate')} avg_pnl={item.get('avg_pnl')}"
            )
        return "\n".join(lines)

    def query_winning_trades(self, *, top_k: int = 200) -> List[RecallHit]:
        """Semantic recall restricted to outcomes with outcome_score > 0 in metadata."""
        return self.recall_memory(
            "profitable structured trade outcome",
            top_k=top_k,
            where={"outcome_score": {"$gt": 0.0}},
        )

    def query_by_setup_tag(self, tag: str, *, top_k: int = 80) -> List[RecallHit]:
        return self.recall_memory(
            f"trade setup tag {tag}",
            top_k=top_k,
            where={"setup_tag": {"$eq": tag}},
        )

    def query_by_session(self, session: str, *, top_k: int = 80) -> List[RecallHit]:
        return self.recall_memory(
            f"trading session {session}",
            top_k=top_k,
            where={"session": {"$eq": session}},
        )

    def count(self) -> int:
        return int(self._collection.count())


def _ordered_counts(counts: Dict[str, int], limit: int = 20) -> List[Dict[str, Any]]:
    ordered = sorted(counts.items(), key=lambda kv: (-kv[1], kv[0]))
    return [{"name": name, "count": count} for name, count in ordered[:limit]]
