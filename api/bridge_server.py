"""
api/bridge_server.py - Tiger Bridge API
HTTP + WebSocket server connecting Dexter Pro to the Web3 Dashboard.

Endpoints:
  GET  /api/signals/active   — Current active signals
  GET  /api/signals/history   — Past signals with outcomes
  GET  /api/performance       — Win rate, P&L, equity curve
  GET  /api/status            — System health
  WS   /ws/signals            — Real-time signal stream
"""
import asyncio
import json
import logging
import re
import time
from typing import Optional

logger = logging.getLogger(__name__)

from config import config

try:
    from aiohttp import web
except ImportError:
    web = None
    logger.warning("[BridgeAPI] aiohttp not installed - bridge API disabled")

try:
    from api.signal_store import signal_store
except Exception:
    signal_store = None

try:
    from api.scalp_signal_store import scalp_store
except Exception:
    scalp_store = None

try:
    from execution.tiger_risk_governor import tiger_risk_governor
except Exception:
    tiger_risk_governor = None

try:
    from api.report_store import report_store
except Exception:
    report_store = None

try:
    from execution.mt5_executor import mt5_executor
except Exception:
    mt5_executor = None

try:
    from execution.ctrader_executor import ctrader_executor
except Exception:
    ctrader_executor = None


class DexterBridgeServer:
    """Lightweight HTTP + WebSocket bridge server."""

    def __init__(self, host: str = "0.0.0.0", port: int = 8788):
        self.host = host
        self.port = port
        self._ws_clients: list = []
        self._app: Optional[web.Application] = None
        self._runner: Optional[web.AppRunner] = None
        self._start_time = time.time()

    def _json_response(self, data: dict, status: int = 200) -> web.Response:
        return web.json_response(data, status=status, headers={
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
            "Access-Control-Allow-Headers": "Content-Type, Authorization",
        })

    @staticmethod
    def _request_token(request: web.Request) -> str:
        auth = str(request.headers.get("Authorization", "") or "").strip()
        if auth.lower().startswith("bearer "):
            return auth[7:].strip()
        x_api_key = str(request.headers.get("X-API-Key", "") or "").strip()
        if x_api_key:
            return x_api_key
        return str(request.query.get("token", "") or "").strip()

    def _require_auth(self, request: web.Request) -> web.Response | None:
        expected = str(getattr(config, "DEXTER_BRIDGE_API_TOKEN", "") or "").strip()
        if not expected:
            return None
        provided = self._request_token(request)
        if provided == expected:
            return None
        return self._json_response(
            {
                "error": "Unauthorized",
                "message": "Missing or invalid bridge token",
            },
            401,
        )

    # ─── HTTP Endpoints ───────────────────────────────────────────────

    async def handle_active_signals(self, request: web.Request) -> web.Response:
        """GET /api/signals/active"""
        auth_err = self._require_auth(request)
        if auth_err is not None:
            return auth_err
        if signal_store is None:
            return self._json_response({"error": "signal store unavailable"}, 503)
        
        is_premium = bool(request.headers.get("Authorization") or request.query.get("token"))
        signals = signal_store.get_active_signals(limit=100)
        
        if not is_premium:
            # 15-minute delay for public/free tier
            now = time.time()
            signals = [s for s in signals if (now - s.get("timestamp", now)) > 900]
            
        signals = [self._normalize_signal_payload(s) for s in signals[:20]]
        return self._json_response({"signals": signals, "count": len(signals)})

    async def handle_signal_history(self, request: web.Request) -> web.Response:
        """GET /api/signals/history"""
        auth_err = self._require_auth(request)
        if auth_err is not None:
            return auth_err
        if signal_store is None:
            return self._json_response({"error": "signal store unavailable"}, 503)
        symbol_raw = str(request.query.get("symbol", "") or "").strip()
        symbol = self._normalize_scalping_symbol(symbol_raw) if symbol_raw else None
        limit = min(int(request.query.get("limit", "50")), 200)
        signals = signal_store.get_signal_history(symbol=symbol, limit=limit)
        signals = [self._normalize_signal_payload(s) for s in signals]
        return self._json_response({"signals": signals, "count": len(signals)})

    async def handle_performance(self, request: web.Request) -> web.Response:
        """GET /api/performance"""
        auth_err = self._require_auth(request)
        if auth_err is not None:
            return auth_err
        if signal_store is None:
            return self._json_response({"error": "signal store unavailable"}, 503)
        stats = signal_store.get_performance_stats()
        equity_curve = signal_store.get_equity_curve(initial_equity=15.0)
        return self._json_response({
            "stats": stats,
            "equity_curve": equity_curve[-50:],  # last 50 points
        })

    async def handle_status(self, request: web.Request) -> web.Response:
        """GET /api/status"""
        auth_err = self._require_auth(request)
        if auth_err is not None:
            return auth_err
        uptime = time.time() - self._start_time
        status = {
            "status": "online",
            "uptime_seconds": round(uptime),
            "ws_clients": len(self._ws_clients),
            "signal_store": signal_store is not None,
            "tiger_governor": tiger_risk_governor is not None,
        }
        if tiger_risk_governor is not None:
            try:
                status["risk_phase"] = tiger_risk_governor.status(15.0)
            except Exception:
                pass
        return self._json_response(status)

    async def handle_neural_status(self, request: web.Request) -> web.Response:
        """GET /api/neural/status"""
        auth_err = self._require_auth(request)
        if auth_err is not None:
            return auth_err
        try:
            from learning.neural_brain import neural_brain

            return self._json_response(
                {
                    "ok": True,
                    "model": neural_brain.model_status(),
                    "filter": neural_brain.execution_filter_status()
                    if bool(getattr(config, "NEURAL_BRAIN_ENABLED", False))
                    else {"ready": False, "reason": "disabled"},
                    "data": neural_brain.data_status(
                        days=max(1, int(getattr(config, "NEURAL_BRAIN_SYNC_DAYS", 120)))
                    ),
                }
            )
        except Exception as e:
            logger.error("[BridgeAPI] Neural status error: %s", e)
            return self._json_response({"ok": False, "error": str(e)}, 500)

    async def handle_scan_run(self, request: web.Request) -> web.Response:
        """POST /api/scan/run?task=xauusd"""
        auth_err = self._require_auth(request)
        if auth_err is not None:
            return auth_err

        task = str(request.query.get("task", "") or "").strip().lower()
        if not task:
            try:
                body = await request.json()
                task = str((body or {}).get("task", "") or "").strip().lower()
            except Exception:
                task = ""
        if not task:
            task = "all"

        alias_map = {
            "gold": "xauusd",
            "forex": "fx",
            "us_open_plan": "us_open",
            "monitor_us": "us_open_monitor",
            "scalp": "scalping",
            "scalp_signals": "scalping",
            "scalping_scan": "scalping",
        }
        task = alias_map.get(task, task)

        allowed = {
            "all",
            "xauusd",
            "crypto",
            "fx",
            "stocks",
            "thai",
            "thai_vi",
            "us",
            "us_open",
            "us_open_monitor",
            "overview",
            "calendar",
            "macro",
            "macro_report",
            "macro_weights",
            "vi",
            "vi_buffett",
            "vi_turnaround",
            "scalping",
        }
        if task not in allowed:
            return self._json_response(
                {
                    "ok": False,
                    "error": "invalid_task",
                    "allowed": sorted(allowed),
                },
                400,
            )

        try:
            from scheduler import scheduler

            started = scheduler.running
            result = scheduler.run_once(task)
            return self._json_response(
                {
                    "ok": True,
                    "task": task,
                    "scheduler_running": bool(started),
                    "result": result,
                }
            )
        except Exception as e:
            logger.error("[BridgeAPI] Scan run error task=%s err=%s", task, e)
            return self._json_response({"ok": False, "task": task, "error": str(e)}, 500)

    @staticmethod
    def _normalize_scalping_symbol(raw: str) -> str:
        token = str(raw or "").strip().upper().replace(" ", "")
        if not token:
            return ""
        alias = {
            "GOLD": "XAUUSD",
            "XAU": "XAUUSD",
            "XAU/USD": "XAUUSD",
            "ETH": "ETHUSD",
            "ETHUSDT": "ETHUSD",
            "ETH/USDT": "ETHUSD",
            "ETH/USD": "ETHUSD",
            "BTC": "BTCUSD",
            "BTCUSDT": "BTCUSD",
            "BTC/USDT": "BTCUSD",
            "BTC/USD": "BTCUSD",
        }
        return alias.get(token, token)

    @classmethod
    def _normalize_signal_payload(cls, row: dict) -> dict:
        payload = dict(row or {})
        payload["symbol"] = cls._normalize_scalping_symbol(str(payload.get("symbol", "") or ""))
        return payload

    async def handle_scalping_status(self, request: web.Request) -> web.Response:
        """GET /api/scalping/status"""
        auth_err = self._require_auth(request)
        if auth_err is not None:
            return auth_err
        try:
            return self._json_response(
                {
                    "ok": True,
                    "enabled": bool(getattr(config, "SCALPING_ENABLED", False)),
                    "symbols": sorted(list(config.get_scalping_symbols())),
                    "entry_tf": str(getattr(config, "SCALPING_ENTRY_TF", "5m")),
                    "trigger_tf": str(getattr(config, "SCALPING_M1_TRIGGER_TF", "1m")),
                    "scan_interval_sec": int(getattr(config, "SCALPING_SCAN_INTERVAL_SEC", 300) or 300),
                    "min_confidence": float(getattr(config, "SCALPING_MIN_CONFIDENCE", 70.0) or 70.0),
                    "execute_mt5": bool(getattr(config, "SCALPING_EXECUTE_MT5", True)),
                    "notify_telegram": bool(getattr(config, "SCALPING_NOTIFY_TELEGRAM", True)),
                    "close_timeout_min": int(getattr(config, "SCALPING_CLOSE_TIMEOUT_MIN", 35) or 35),
                }
            )
        except Exception as e:
            logger.error("[BridgeAPI] Scalping status error: %s", e)
            return self._json_response({"ok": False, "error": str(e)}, 500)

    async def handle_scalping_toggle(self, request: web.Request) -> web.Response:
        """POST /api/scalping/toggle?enabled=1&symbols=XAUUSD,ETHUSD,BTCUSD"""
        auth_err = self._require_auth(request)
        if auth_err is not None:
            return auth_err
        body: dict = {}
        try:
            if request.can_read_body:
                parsed = await request.json()
                if isinstance(parsed, dict):
                    body = parsed
        except Exception:
            body = {}
        enabled_raw = str(request.query.get("enabled", "") or "").strip().lower()
        if not enabled_raw:
            enabled_raw = str((body or {}).get("enabled", "") or "").strip().lower()
        if enabled_raw in {"1", "true", "yes", "on"}:
            enabled = True
        elif enabled_raw in {"0", "false", "no", "off"}:
            enabled = False
        else:
            return self._json_response(
                {"ok": False, "error": "invalid_enabled", "message": "Use enabled=1 or enabled=0"},
                400,
            )
        config.SCALPING_ENABLED = bool(enabled)

        symbols_raw = str(request.query.get("symbols", "") or "").strip()
        if not symbols_raw:
            symbols_raw = str((body or {}).get("symbols", "") or "").strip()
        if symbols_raw:
            parsed = set()
            for part in re.split(r"[\s,;]+", symbols_raw):
                sym = self._normalize_scalping_symbol(part)
                if sym in {"XAUUSD", "ETHUSD", "BTCUSD"}:
                    parsed.add(sym)
            if parsed:
                config.SCALPING_SYMBOLS = ",".join(sorted(parsed))
        return self._json_response(
            {
                "ok": True,
                "enabled": bool(getattr(config, "SCALPING_ENABLED", False)),
                "symbols": sorted(list(config.get_scalping_symbols())),
                "note": "Runtime-only toggle. Persist in .env.local for reboot durability.",
            }
        )

    async def handle_scalping_logic(self, request: web.Request) -> web.Response:
        """GET /api/scalping/logic?symbol=BTCUSD"""
        auth_err = self._require_auth(request)
        if auth_err is not None:
            return auth_err
        symbol = self._normalize_scalping_symbol(str(request.query.get("symbol", "") or "BTCUSD"))
        if symbol not in {"XAUUSD", "ETHUSD", "BTCUSD"}:
            return self._json_response(
                {"ok": False, "error": "invalid_symbol", "allowed": ["XAUUSD", "ETHUSD", "BTCUSD"]},
                400,
            )
        try:
            from scanners.scalping_scanner import scalping_scanner

            if symbol == "XAUUSD":
                row = scalping_scanner.scan_xauusd()
            elif symbol == "ETHUSD":
                row = scalping_scanner.scan_eth()
            else:
                row = scalping_scanner.scan_btc(require_enabled=False)
            signal = getattr(row, "signal", None)
            out = {
                "ok": True,
                "symbol": symbol,
                "source": str(getattr(row, "source", "")),
                "status": str(getattr(row, "status", "")),
                "reason": str(getattr(row, "reason", "")),
                "trigger": dict(getattr(row, "trigger", {}) or {}),
                "signal": None,
            }
            if signal is not None:
                out["signal"] = {
                    "direction": str(getattr(signal, "direction", "")),
                    "entry": float(getattr(signal, "entry", 0.0) or 0.0),
                    "stop_loss": float(getattr(signal, "stop_loss", 0.0) or 0.0),
                    "take_profit_1": float(getattr(signal, "take_profit_1", 0.0) or 0.0),
                    "take_profit_2": float(getattr(signal, "take_profit_2", 0.0) or 0.0),
                    "confidence": float(getattr(signal, "confidence", 0.0) or 0.0),
                    "timeframe": str(getattr(signal, "timeframe", "")),
                    "reasons": list(getattr(signal, "reasons", []) or [])[:8],
                }
            return self._json_response(out)
        except Exception as e:
            logger.error("[BridgeAPI] Scalping logic error symbol=%s err=%s", symbol, e)
            return self._json_response({"ok": False, "symbol": symbol, "error": str(e)}, 500)

    async def handle_risk_status(self, request: web.Request) -> web.Response:
        """GET /api/risk"""
        auth_err = self._require_auth(request)
        if auth_err is not None:
            return auth_err
        if tiger_risk_governor is None:
            return self._json_response({"error": "risk governor unavailable"}, 503)
        equity = float(request.query.get("equity", "15.0"))
        status = tiger_risk_governor.status(equity)
        return self._json_response(status)

    async def handle_positions(self, request: web.Request) -> web.Response:
        """GET /api/positions"""
        auth_err = self._require_auth(request)
        if auth_err is not None:
            return auth_err
        if mt5_executor is None:
            return self._json_response({"error": "mt5 executor unavailable"}, 503)
        try:
            positions = mt5_executor.open_positions_snapshot()
            return self._json_response({"positions": positions, "count": len(positions)})
        except Exception as e:
            logger.error(f"[BridgeAPI] Fetch positions error: {e}")
            return self._json_response({"error": "Failed to fetch positions"}, 500)

    async def handle_ctrader_status(self, request: web.Request) -> web.Response:
        """GET /api/ctrader/status"""
        auth_err = self._require_auth(request)
        if auth_err is not None:
            return auth_err
        if ctrader_executor is None:
            return self._json_response({"ok": False, "error": "ctrader executor unavailable"}, 503)
        live = str(request.query.get("live", "0") or "0").strip().lower() in {"1", "true", "yes", "on"}
        try:
            status = ctrader_executor.health_check(live=live)
            return self._json_response({"ok": True, "status": status})
        except Exception as e:
            logger.error("[BridgeAPI] cTrader status error: %s", e)
            return self._json_response({"ok": False, "error": str(e)}, 500)

    async def handle_ctrader_journal(self, request: web.Request) -> web.Response:
        """GET /api/ctrader/journal"""
        auth_err = self._require_auth(request)
        if auth_err is not None:
            return auth_err
        if ctrader_executor is None:
            return self._json_response({"ok": False, "error": "ctrader executor unavailable"}, 503)
        limit = min(max(int(request.query.get("limit", "25") or "25"), 1), 200)
        try:
            rows = ctrader_executor.get_recent_journal(limit=limit)
            return self._json_response({"ok": True, "count": len(rows), "rows": rows})
        except Exception as e:
            logger.error("[BridgeAPI] cTrader journal error: %s", e)
            return self._json_response({"ok": False, "error": str(e)}, 500)

    async def handle_ctrader_accounts(self, request: web.Request) -> web.Response:
        """GET /api/ctrader/accounts"""
        auth_err = self._require_auth(request)
        if auth_err is not None:
            return auth_err
        if ctrader_executor is None:
            return self._json_response({"ok": False, "error": "ctrader executor unavailable"}, 503)
        try:
            payload = ctrader_executor.list_accounts(live=True)
            return self._json_response({"ok": bool(payload.get("ok", False)), "accounts": payload})
        except Exception as e:
            logger.error("[BridgeAPI] cTrader accounts error: %s", e)
            return self._json_response({"ok": False, "error": str(e)}, 500)

    async def handle_ctrader_positions(self, request: web.Request) -> web.Response:
        """GET /api/ctrader/positions"""
        auth_err = self._require_auth(request)
        if auth_err is not None:
            return auth_err
        if ctrader_executor is None:
            return self._json_response({"ok": False, "error": "ctrader executor unavailable"}, 503)
        try:
            rows = ctrader_executor.get_open_positions()
            return self._json_response({"ok": True, "count": len(rows), "rows": rows})
        except Exception as e:
            logger.error("[BridgeAPI] cTrader positions error: %s", e)
            return self._json_response({"ok": False, "error": str(e)}, 500)

    async def handle_ctrader_deals(self, request: web.Request) -> web.Response:
        """GET /api/ctrader/deals"""
        auth_err = self._require_auth(request)
        if auth_err is not None:
            return auth_err
        if ctrader_executor is None:
            return self._json_response({"ok": False, "error": "ctrader executor unavailable"}, 503)
        limit = min(max(int(request.query.get("limit", "25") or "25"), 1), 200)
        try:
            rows = ctrader_executor.get_recent_deals(limit=limit)
            return self._json_response({"ok": True, "count": len(rows), "rows": rows})
        except Exception as e:
            logger.error("[BridgeAPI] cTrader deals error: %s", e)
            return self._json_response({"ok": False, "error": str(e)}, 500)

    async def handle_ctrader_reconcile(self, request: web.Request) -> web.Response:
        """POST /api/ctrader/reconcile"""
        auth_err = self._require_auth(request)
        if auth_err is not None:
            return auth_err
        if ctrader_executor is None:
            return self._json_response({"ok": False, "error": "ctrader executor unavailable"}, 503)
        lookback_hours = min(max(int(request.query.get("lookback_hours", "72") or "72"), 1), 720)
        try:
            report = ctrader_executor.sync_account_state(lookback_hours=lookback_hours)
            return self._json_response({"ok": bool(report.get("ok", False)), "report": report})
        except Exception as e:
            logger.error("[BridgeAPI] cTrader reconcile error: %s", e)
            return self._json_response({"ok": False, "error": str(e)}, 500)

    async def handle_ctrader_feed(self, request: web.Request) -> web.Response:
        """GET /api/ctrader/feed"""
        auth_err = self._require_auth(request)
        if auth_err is not None:
            return auth_err
        if not bool(getattr(config, "CTRADER_STORE_FEED_ENABLED", True)):
            return self._json_response({"ok": False, "error": "ctrader store feed disabled"}, 503)
        limit = min(max(int(request.query.get("limit", "20") or "20"), 1), 100)
        symbols_raw = str(request.query.get("symbols", "") or "").strip()
        symbols = set()
        if symbols_raw:
            for part in re.split(r"[\s,;]+", symbols_raw):
                mapped = self._normalize_scalping_symbol(part)
                if mapped:
                    symbols.add(mapped)
        allowed_sources = set(getattr(config, "get_ctrader_store_feed_sources", lambda: set())() or set())
        rows: list[dict] = []
        if signal_store is not None:
            try:
                for row in signal_store.get_active_signals(limit=limit * 2):
                    payload = self._normalize_signal_payload(row)
                    payload["_origin"] = "signal_store"
                    rows.append(payload)
            except Exception:
                logger.debug("[BridgeAPI] cTrader feed main signals unavailable", exc_info=True)
        if scalp_store is not None:
            try:
                for row in scalp_store.get_pending():
                    payload = dict(row or {})
                    payload["symbol"] = self._normalize_scalping_symbol(str(payload.get("symbol", "") or ""))
                    payload["_origin"] = "scalp_store"
                    rows.append(payload)
            except Exception:
                logger.debug("[BridgeAPI] cTrader feed scalp signals unavailable", exc_info=True)

        def _source_from_row(row: dict) -> str:
            source = str(row.get("source", "") or "").strip().lower()
            if source:
                return source
            setup_detail = row.get("setup_detail")
            if isinstance(setup_detail, str):
                try:
                    setup_detail = json.loads(setup_detail)
                except Exception:
                    setup_detail = {}
            if isinstance(setup_detail, dict):
                return str(setup_detail.get("source", "") or "").strip().lower()
            return ""

        normalized: list[dict] = []
        for row in rows:
            source = _source_from_row(row)
            symbol = self._normalize_scalping_symbol(str(row.get("symbol", "") or ""))
            if symbols and symbol not in symbols:
                continue
            if allowed_sources and source and source not in allowed_sources:
                continue
            setup_detail = row.get("setup_detail")
            if isinstance(setup_detail, str):
                try:
                    setup_detail = json.loads(setup_detail)
                except Exception:
                    setup_detail = {}
            item = {
                "symbol": symbol,
                "source": source,
                "direction": str(row.get("direction", "") or "").strip().lower(),
                "confidence": float(row.get("confidence", 0.0) or 0.0),
                "entry": float(row.get("entry", 0.0) or 0.0),
                "stop_loss": float(row.get("stop_loss", 0.0) or 0.0),
                "take_profit_1": float(row.get("take_profit_1", 0.0) or 0.0),
                "take_profit_2": float(row.get("take_profit_2", 0.0) or 0.0),
                "take_profit_3": float(row.get("take_profit_3", 0.0) or 0.0),
                "entry_type": str(row.get("entry_type", "") or ""),
                "timeframe": str(row.get("timeframe", "") or setup_detail.get("entry_tf", "")),
                "session": str(row.get("session", "") or ""),
                "pattern": str(row.get("pattern", "") or ""),
                "timestamp": float(row.get("timestamp", 0.0) or 0.0),
                "origin": str(row.get("_origin", "") or ""),
            }
            normalized.append(item)
        normalized.sort(key=lambda x: float(x.get("timestamp", 0.0) or 0.0), reverse=True)
        normalized = normalized[:limit]
        return self._json_response({"ok": True, "count": len(normalized), "signals": normalized})

    async def handle_reports(self, request: web.Request) -> web.Response:
        """GET /api/reports/{type}"""
        auth_err = self._require_auth(request)
        if auth_err is not None:
            return auth_err
        if report_store is None:
            return self._json_response({"error": "report store unavailable"}, 503)
        report_type = request.match_info.get("type", "unknown")
        data = report_store.get_report(report_type)
        if data is None:
            return self._json_response({"error": f"Report '{report_type}' not found or empty"}, 404)
        return self._json_response({"type": report_type, "data": data})

    async def handle_action_close_all(self, request: web.Request) -> web.Response:
        """POST /api/action/close_all"""
        auth_err = self._require_auth(request)
        if auth_err is not None:
            return auth_err
        if mt5_executor is None:
            return self._json_response({"error": "mt5 executor unavailable"}, 503)
        
        try:
            positions = mt5_executor.open_positions_snapshot()
            closed_count = 0
            errors = []
            
            for p in positions:
                ticket = p.get("ticket")
                vol = p.get("volume")
                ptype = p.get("type")
                sym = p.get("symbol")
                
                if not all([ticket, vol, ptype, sym]):
                    continue
                    
                # Close the position
                res = mt5_executor.close_position_partial(
                    broker_symbol=sym,
                    position_ticket=ticket,
                    position_type=ptype,
                    position_volume=vol,
                    close_volume=vol,
                    source="openclaw"
                )
                if getattr(res, "ok", False):
                    closed_count += 1
                else:
                    errors.append(f"Failed to close {sym} ({ticket}): {getattr(res, 'message', 'unknown error')}")
                    
            return self._json_response({
                "status": "success" if not errors else "partial_success",
                "closed": closed_count,
                "total": len(positions),
                "errors": errors
            })
        except Exception as e:
            logger.error(f"[BridgeAPI] Close all error: {e}")
            return self._json_response({"error": str(e)}, 500)

    async def handle_action_pause(self, request: web.Request) -> web.Response:
        """POST /api/action/pause"""
        auth_err = self._require_auth(request)
        if auth_err is not None:
            return auth_err
        import config
        config.config.MT5_AUTOPILOT_ENABLED = False
        return self._json_response({"status": "paused", "message": "MT5 Autopilot disabled."})

    async def handle_action_resume(self, request: web.Request) -> web.Response:
        """POST /api/action/resume"""
        auth_err = self._require_auth(request)
        if auth_err is not None:
            return auth_err
        import config
        config.config.MT5_AUTOPILOT_ENABLED = True
        return self._json_response({"status": "resumed", "message": "MT5 Autopilot enabled."})

    async def handle_cors_preflight(self, request: web.Request) -> web.Response:
        """OPTIONS handler for CORS preflight."""
        return web.Response(headers={
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
            "Access-Control-Allow-Headers": "Content-Type, Authorization",
        })

    # ─── WebSocket ────────────────────────────────────────────────────

    async def handle_ws(self, request: web.Request) -> web.WebSocketResponse:
        """WS /ws/signals — Real-time signal stream."""
        ws = web.WebSocketResponse()
        await ws.prepare(request)
        is_premium = bool(request.query.get("token"))
        client_data = {"ws": ws, "is_premium": is_premium}
        self._ws_clients.append(client_data)
        logger.info("[BridgeAPI] WebSocket client connected (%d total, premium: %s)", len(self._ws_clients), is_premium)

        try:
            await ws.send_json({"type": "connected", "message": "Tiger Bridge active"})
            async for msg in ws:
                if msg.type == web.WSMsgType.TEXT:
                    # Echo back pings
                    if msg.data == "ping":
                        await ws.send_json({"type": "pong"})
                elif msg.type == web.WSMsgType.ERROR:
                    break
        finally:
            self._ws_clients.remove(client_data)
            logger.info("[BridgeAPI] WebSocket client disconnected (%d remaining)", len(self._ws_clients))

        return ws

    async def broadcast_signal(self, signal_data: dict):
        """Broadcast a new signal to all connected WebSocket clients."""
        if not self._ws_clients:
            return
        msg = json.dumps({"type": "signal", "data": signal_data})
        dead = []
        for client in self._ws_clients:
            if not client["is_premium"]:
                continue  # Free tier does not get real-time WebSocket push
            ws = client["ws"]
            try:
                await ws.send_str(msg)
            except Exception:
                dead.append(client)
        for client in dead:
            self._ws_clients.remove(client)

    async def broadcast_performance(self):
        """Broadcast performance update to all connected clients."""
        if not self._ws_clients or signal_store is None:
            return
        stats = signal_store.get_performance_stats()
        msg = json.dumps({"type": "performance", "data": stats})
        dead = []
        for client in self._ws_clients:
            ws = client["ws"]
            try:
                await ws.send_str(msg)
            except Exception:
                dead.append(client)
        for client in dead:
            self._ws_clients.remove(client)

    # ─── Server Lifecycle ─────────────────────────────────────────────

    def create_app(self) -> "web.Application":
        """Create and configure the aiohttp application."""
        if web is None:
            raise ImportError("aiohttp is required for bridge API")

        app = web.Application()
        app.router.add_get("/api/signals/active", self.handle_active_signals)
        app.router.add_get("/api/signals/history", self.handle_signal_history)
        app.router.add_get("/api/performance", self.handle_performance)
        app.router.add_get("/api/status", self.handle_status)
        app.router.add_get("/api/neural/status", self.handle_neural_status)
        app.router.add_post("/api/scan/run", self.handle_scan_run)
        app.router.add_get("/api/scalping/status", self.handle_scalping_status)
        app.router.add_post("/api/scalping/toggle", self.handle_scalping_toggle)
        app.router.add_get("/api/scalping/logic", self.handle_scalping_logic)
        app.router.add_get("/api/risk", self.handle_risk_status)
        app.router.add_get("/api/positions", self.handle_positions)
        app.router.add_get("/api/ctrader/status", self.handle_ctrader_status)
        app.router.add_get("/api/ctrader/journal", self.handle_ctrader_journal)
        app.router.add_get("/api/ctrader/accounts", self.handle_ctrader_accounts)
        app.router.add_get("/api/ctrader/positions", self.handle_ctrader_positions)
        app.router.add_get("/api/ctrader/deals", self.handle_ctrader_deals)
        app.router.add_post("/api/ctrader/reconcile", self.handle_ctrader_reconcile)
        app.router.add_get("/api/ctrader/feed", self.handle_ctrader_feed)
        app.router.add_get("/api/reports/{type}", self.handle_reports)
        app.router.add_post("/api/action/close_all", self.handle_action_close_all)
        app.router.add_post("/api/action/pause", self.handle_action_pause)
        app.router.add_post("/api/action/resume", self.handle_action_resume)
        app.router.add_get("/ws/signals", self.handle_ws)

        # CORS preflight for all api routes
        app.router.add_route("OPTIONS", "/api/{tail:.*}", self.handle_cors_preflight)

        self._app = app
        return app

    async def start(self):
        """Start the bridge server."""
        app = self.create_app()
        self._runner = web.AppRunner(app)
        await self._runner.setup()
        site = web.TCPSite(self._runner, self.host, self.port)
        await site.start()
        logger.info("[BridgeAPI] Tiger Bridge API running on http://%s:%d", self.host, self.port)

    async def stop(self):
        """Stop the bridge server."""
        if self._runner:
            await self._runner.cleanup()
        logger.info("[BridgeAPI] Tiger Bridge API stopped")


# Singleton (lazy start)
bridge_server = DexterBridgeServer()
