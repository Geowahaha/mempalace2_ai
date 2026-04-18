from __future__ import annotations

import asyncio
import json
from functools import lru_cache
from pathlib import Path
from typing import Any, Dict, List, Optional

from fastapi import FastAPI, HTTPException
from fastapi.responses import HTMLResponse
from pydantic import BaseModel, Field

from trading_ai.config import load_settings, memory_persist_path
from trading_ai.core.memory import MemoryEngine, MemoryNote, RecallHit
from trading_ai.core.skillbook import build_team_brief
from trading_ai.core.strategy_evolution import StrategyRegistry
from trading_ai.main import build_broker, build_skillbook

app = FastAPI(title="Mempalac Trading AI", version="0.1.0")
_ROOT = Path(__file__).resolve().parent.parent
_LOG_FILES = {
    "api_out": _ROOT / "logs" / "api.out.log",
    "api_err": _ROOT / "logs" / "api.err.log",
    "loop_out": _ROOT / "logs" / "demo-live-loop.out.log",
    "loop_err": _ROOT / "logs" / "demo-live-loop.err.log",
}


@lru_cache(maxsize=1)
def get_settings():
    return load_settings()


@lru_cache(maxsize=1)
def get_memory() -> MemoryEngine:
    settings = get_settings()
    return MemoryEngine(
        persist_path=memory_persist_path(settings),
        collection_name=settings.memory_collection,
        score_weight=settings.memory_score_weight,
    )


@lru_cache(maxsize=1)
def get_broker():
    return build_broker(get_settings())


@lru_cache(maxsize=1)
def get_skillbook():
    return build_skillbook(get_settings())


def get_registry() -> StrategyRegistry:
    return StrategyRegistry(Path(get_settings().strategy_registry_path))


class MemorySearchRequest(BaseModel):
    q: str = Field(..., min_length=1)
    top_k: int = Field(default=8, ge=1, le=50)
    wing: Optional[str] = None
    hall: Optional[str] = None
    room: Optional[str] = None


class MemorySearchResponse(BaseModel):
    hits: List[Dict[str, Any]]


class MemoryNoteCreateRequest(BaseModel):
    title: str = Field(..., min_length=1, max_length=200)
    content: str = Field(..., min_length=1)
    wing: str = Field(..., min_length=1)
    hall: str = Field(..., min_length=1)
    room: str = Field(..., min_length=1)
    note_type: str = Field(default="operator_note")
    hall_type: str = Field(default="hall_discoveries")
    symbol: Optional[str] = None
    session: Optional[str] = None
    setup_tag: Optional[str] = None
    strategy_key: Optional[str] = None
    importance: float = Field(default=0.5, ge=0.0, le=1.0)
    source: str = Field(default="manual")
    tags: List[str] = Field(default_factory=list)


class LaneStageUpdateRequest(BaseModel):
    strategy_key: str = Field(..., min_length=1)
    lane_stage: str = Field(..., min_length=1)


@app.get("/status")
async def status() -> Dict[str, Any]:
    settings = get_settings()
    memory = get_memory()
    intel = memory.get_memory_intelligence()
    summary = dict(intel.get("summary") or {})
    return {
        "instance_name": settings.instance_name,
        "memory_backend": settings.memory_backend,
        "chroma_path": str(memory_persist_path(settings)),
        "collection": settings.memory_collection,
        "memory_count": memory.count(),
        "trade_memory_count": summary.get("total_trades", 0),
        "note_memory_count": summary.get("note_memories", 0),
        "llm_provider": str(settings.llm_provider),
        "symbol": settings.symbol,
        "dry_run_default": settings.dry_run,
        "live_execution_enabled": settings.live_execution_enabled,
        "runtime_state_path": str(settings.runtime_state_path),
    }


@app.get("/broker/health")
async def broker_health() -> Dict[str, Any]:
    settings = get_settings()
    broker = get_broker()
    if not hasattr(broker, "_run_worker"):
        return {
            "ok": False,
            "status": "unsupported",
            "broker_type": type(broker).__name__,
        }
    return await asyncio.to_thread(
        broker._run_worker,
        "health",
        {"account_id": int(settings.ctrader_account_id)},
    )


@app.get("/broker/reconcile")
async def broker_reconcile(symbol: Optional[str] = None) -> Dict[str, Any]:
    settings = get_settings()
    broker = get_broker()
    if not hasattr(broker, "_run_worker"):
        return {
            "ok": False,
            "status": "unsupported",
            "broker_type": type(broker).__name__,
        }
    payload = {
        "account_id": int(settings.ctrader_account_id),
        "symbol": symbol or settings.symbol,
    }
    return await asyncio.to_thread(broker._run_worker, "reconcile", payload)


@app.post("/memory/search", response_model=MemorySearchResponse)
async def memory_search(req: MemorySearchRequest) -> MemorySearchResponse:
    try:
        hits: List[RecallHit] = get_memory().recall_palace(
            req.q,
            top_k=req.top_k,
            wing=req.wing,
            hall=req.hall,
            room=req.room,
        )
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc
    out = []
    for h in hits:
        out.append(
            {
                "id": h.id,
                "similarity": h.similarity,
                "weighted_score": h.weighted_score,
                "metadata": h.metadata,
                "document": h.document,
            }
        )
    return MemorySearchResponse(hits=out)


@app.get("/memory/wakeup")
async def memory_wakeup(
    symbol: Optional[str] = None,
    session: Optional[str] = None,
    top_k: Optional[int] = None,
) -> Dict[str, Any]:
    settings = get_settings()
    text = get_memory().build_wake_up_context(
        symbol=symbol or settings.symbol,
        session=session,
        top_k=top_k or settings.memory_wakeup_top_k,
        note_top_k=settings.memory_note_top_k,
    )
    return {"text": text}


@app.get("/memory/taxonomy")
async def memory_taxonomy() -> Dict[str, Any]:
    return get_memory().get_taxonomy()


@app.get("/memory/intelligence")
async def memory_intelligence() -> Dict[str, Any]:
    return get_memory().get_memory_intelligence()


@app.get("/memory/room-guard")
async def memory_room_guard(
    symbol: Optional[str] = None,
    session: Optional[str] = None,
    setup_tag: str = "trend_follow",
    trend_direction: str = "RANGE",
    volatility: str = "MEDIUM",
    strategy_key: str = "",
) -> Dict[str, Any]:
    settings = get_settings()
    return get_memory().get_room_guardrail(
        symbol=symbol or settings.symbol,
        session=session or "",
        setup_tag=setup_tag,
        trend_direction=trend_direction,
        volatility=volatility,
        strategy_key=strategy_key,
    )


@app.get("/memory/tunnel")
async def memory_tunnel(room: str) -> Dict[str, Any]:
    intel = get_memory().get_memory_intelligence()
    matches = [item for item in list(intel.get("tunnels") or []) if str(item.get("room") or "") == room]
    return {"items": matches}


@app.get("/positions/monitor")
async def positions_monitor() -> Dict[str, Any]:
    settings = get_settings()
    path = Path(settings.position_monitor_path)
    if not path.is_file():
        return {"ok": False, "status": "missing", "path": str(path)}
    try:
        return {"ok": True, "path": str(path), "snapshot": json.loads(path.read_text(encoding="utf-8"))}
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@app.get("/memory/notes")
async def memory_notes(
    wing: Optional[str] = None,
    hall: Optional[str] = None,
    room: Optional[str] = None,
    hall_type: Optional[str] = None,
    note_type: Optional[str] = None,
    symbol: Optional[str] = None,
    session: Optional[str] = None,
    limit: int = 50,
) -> Dict[str, Any]:
    return {
        "items": get_memory().list_notes(
            wing=wing,
            hall=hall,
            room=room,
            hall_type=hall_type,
            note_type=note_type,
            symbol=symbol,
            session=session,
            limit=limit,
        )
    }


@app.post("/memory/notes")
async def memory_notes_create(req: MemoryNoteCreateRequest) -> Dict[str, Any]:
    note = MemoryNote(
        title=req.title,
        content=req.content,
        wing=req.wing,
        hall=req.hall,
        room=req.room,
        note_type=req.note_type,
        hall_type=req.hall_type,
        symbol=req.symbol or "",
        session=req.session or "",
        setup_tag=req.setup_tag or "",
        strategy_key=req.strategy_key or "",
        importance=req.importance,
        source=req.source,
        tags=list(req.tags),
    )
    doc_id = get_memory().store_note(note)
    return {"ok": True, "id": doc_id}


@app.get("/memory/daily-brief")
async def memory_daily_brief() -> Dict[str, Any]:
    return {"text": get_memory().build_daily_analyst_brief()}


@app.get("/memory/analyst-packet")
async def memory_analyst_packet() -> Dict[str, Any]:
    packet = get_memory().build_daily_analyst_packet()
    packet["strategy_promotions"] = get_registry().promotion_snapshot()
    packet["skills"] = get_skillbook().list_skills(limit=20)
    return packet


@app.get("/strategy/promotions")
async def strategy_promotions() -> Dict[str, Any]:
    return {"items": get_registry().promotion_snapshot()}


@app.get("/skills")
async def skills(
    symbol: Optional[str] = None,
    session: Optional[str] = None,
    strategy_key: Optional[str] = None,
    limit: int = 50,
) -> Dict[str, Any]:
    return {
        "items": get_skillbook().list_skills(
            symbol=symbol,
            session=session,
            strategy_key=strategy_key,
            limit=limit,
        )
    }


@app.get("/skills/context")
async def skills_context(
    symbol: Optional[str] = None,
    session: Optional[str] = None,
    setup_tag: str = "trend_follow",
    strategy_key: str = "",
    room: str = "",
    trend_direction: str = "RANGE",
    volatility: str = "MEDIUM",
    action: str = "HOLD",
) -> Dict[str, Any]:
    settings = get_settings()
    effective_symbol = symbol or settings.symbol
    matches = get_skillbook().recall(
        symbol=effective_symbol,
        session=session or "",
        setup_tag=setup_tag,
        strategy_key=strategy_key,
        room=room or strategy_key,
        trend_direction=trend_direction,
        volatility=volatility,
        action=action,
        top_k=settings.skill_recall_top_k,
    )
    strategy_state = next(
        (
            row
            for row in get_registry().promotion_snapshot()
            if str(row.get("strategy_key") or "") == strategy_key
        ),
        None,
    )
    team_brief = build_team_brief(
        features={
            "session": session or "",
            "trend_direction": trend_direction,
            "volatility": volatility,
            "structure": {},
        },
        risk_state={"can_trade": True},
        pattern_analysis={},
        matches=matches,
        strategy_state=strategy_state,
        room_guard=None,
    )
    return {
        "items": [
            {
                "skill_key": item.skill_key,
                "score": item.score,
                "title": item.title,
                "summary": item.summary,
                "fit_reasons": item.fit_reasons,
                "stats": item.stats,
                "file_path": item.file_path,
            }
            for item in matches
        ],
        "prompt_context": get_skillbook().render_prompt_context(matches),
        "team_brief": team_brief,
    }


@app.post("/strategy/promotions/stage")
async def strategy_promotion_stage(req: LaneStageUpdateRequest) -> Dict[str, Any]:
    get_registry().set_lane_stage(req.strategy_key, req.lane_stage)
    return {"ok": True, "items": get_registry().promotion_snapshot()}


@app.get("/runtime/state")
async def runtime_state() -> Dict[str, Any]:
    path = Path(get_settings().runtime_state_path)
    if not path.is_file():
        return {"ok": False, "status": "missing", "path": str(path)}
    try:
        return {
            "ok": True,
            "status": "loaded",
            "path": str(path),
            "data": json.loads(path.read_text(encoding="utf-8")),
        }
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@app.get("/llm/failover")
async def llm_failover() -> Dict[str, Any]:
    path = Path(get_settings().llm_failover_runtime_path)
    if not path.is_file():
        return {"ok": False, "status": "missing", "path": str(path), "snapshots": {}}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc
    return {
        "ok": True,
        "status": "loaded",
        "path": str(path),
        "data": payload,
    }


@app.get("/logs/tail")
async def logs_tail(name: str = "loop_out", lines: int = 80) -> Dict[str, Any]:
    if name not in _LOG_FILES:
        raise HTTPException(status_code=400, detail=f"unknown log name: {name}")
    path = _LOG_FILES[name]
    if not path.is_file():
        return {"ok": False, "status": "missing", "path": str(path), "text": ""}
    take = max(1, min(int(lines), 400))
    content = path.read_text(encoding="utf-8", errors="replace").splitlines()
    return {
        "ok": True,
        "status": "loaded",
        "path": str(path),
        "text": "\n".join(content[-take:]),
    }


@app.get("/dashboard", response_class=HTMLResponse)
async def dashboard() -> str:
    return """
<!doctype html>
<html>
<head>
  <meta charset="utf-8" />
  <title>Mempalace Trader Monitor</title>
  <style>
    body { font-family: Consolas, monospace; background: #111827; color: #e5e7eb; margin: 0; padding: 24px; }
    h1 { margin-top: 0; }
    .grid { display: grid; grid-template-columns: repeat(2, minmax(320px, 1fr)); gap: 16px; }
    .card { background: #1f2937; border: 1px solid #374151; border-radius: 10px; padding: 16px; }
    pre { white-space: pre-wrap; word-break: break-word; margin: 0; }
    .meta { color: #93c5fd; margin-bottom: 12px; }
    a { color: #93c5fd; }
  </style>
</head>
<body>
  <h1>Mempalace Trader Monitor</h1>
  <div class="meta">
    refresh: 5s |
    endpoints:
    <a href="/status" target="_blank">/status</a>,
    <a href="/broker/health" target="_blank">/broker/health</a>,
    <a href="/broker/reconcile" target="_blank">/broker/reconcile</a>,
    <a href="/runtime/state" target="_blank">/runtime/state</a>,
    <a href="/llm/failover" target="_blank">/llm/failover</a>,
    <a href="/memory/taxonomy" target="_blank">/memory/taxonomy</a>,
    <a href="/memory/wakeup" target="_blank">/memory/wakeup</a>,
    <a href="/memory/intelligence" target="_blank">/memory/intelligence</a>,
    <a href="/memory/daily-brief" target="_blank">/memory/daily-brief</a>,
    <a href="/memory/analyst-packet" target="_blank">/memory/analyst-packet</a>,
    <a href="/memory/notes" target="_blank">/memory/notes</a>,
    <a href="/skills" target="_blank">/skills</a>,
    <a href="/skills/context" target="_blank">/skills/context</a>,
    <a href="/strategy/promotions" target="_blank">/strategy/promotions</a>
  </div>
  <div class="grid">
    <div class="card"><h3>Status</h3><pre id="status">loading...</pre></div>
    <div class="card"><h3>Broker Health</h3><pre id="health">loading...</pre></div>
    <div class="card"><h3>Reconcile</h3><pre id="reconcile">loading...</pre></div>
    <div class="card"><h3>Runtime State</h3><pre id="runtime">loading...</pre></div>
    <div class="card"><h3>LLM Failover</h3><pre id="llmfailover">loading...</pre></div>
    <div class="card"><h3>Memory Taxonomy</h3><pre id="taxonomy">loading...</pre></div>
    <div class="card"><h3>Memory Intelligence</h3><pre id="intelligence">loading...</pre></div>
    <div class="card" style="grid-column: 1 / -1;"><h3>Wake Up Context</h3><pre id="wakeup">loading...</pre></div>
    <div class="card" style="grid-column: 1 / -1;"><h3>Daily Analyst Brief</h3><pre id="dailybrief">loading...</pre></div>
    <div class="card" style="grid-column: 1 / -1;"><h3>Loop Log Tail</h3><pre id="looplog">loading...</pre></div>
  </div>
  <script>
    async function loadJson(path) {
      const res = await fetch(path);
      return await res.json();
    }
    async function refresh() {
      try {
        const [status, health, reconcile, runtime, llmfailover, taxonomy, intelligence, wakeup, dailybrief, looplog] = await Promise.all([
          loadJson('/status'),
          loadJson('/broker/health'),
          loadJson('/broker/reconcile'),
          loadJson('/runtime/state'),
          loadJson('/llm/failover'),
          loadJson('/memory/taxonomy'),
          loadJson('/memory/intelligence'),
          loadJson('/memory/wakeup'),
          loadJson('/memory/daily-brief'),
          loadJson('/logs/tail?name=loop_out&lines=60'),
        ]);
        document.getElementById('status').textContent = JSON.stringify(status, null, 2);
        document.getElementById('health').textContent = JSON.stringify(health, null, 2);
        document.getElementById('reconcile').textContent = JSON.stringify(reconcile, null, 2);
        document.getElementById('runtime').textContent = JSON.stringify(runtime, null, 2);
        document.getElementById('llmfailover').textContent = JSON.stringify(llmfailover, null, 2);
        document.getElementById('taxonomy').textContent = JSON.stringify(taxonomy, null, 2);
        document.getElementById('intelligence').textContent = JSON.stringify(intelligence, null, 2);
        document.getElementById('wakeup').textContent = wakeup.text || JSON.stringify(wakeup, null, 2);
        document.getElementById('dailybrief').textContent = dailybrief.text || JSON.stringify(dailybrief, null, 2);
        document.getElementById('looplog').textContent = looplog.text || JSON.stringify(looplog, null, 2);
      } catch (err) {
        document.getElementById('status').textContent = String(err);
      }
    }
    refresh();
    setInterval(refresh, 5000);
  </script>
</body>
</html>
"""


def main() -> None:
    import uvicorn

    s = get_settings()
    uvicorn.run(app, host=s.api_host, port=s.api_port, reload=False)


if __name__ == "__main__":
    main()
