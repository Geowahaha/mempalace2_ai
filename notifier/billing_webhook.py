"""
notifier/billing_webhook.py
Webhook server for automated plan upgrades after Stripe/PromptPay payments.
"""
from __future__ import annotations

import hashlib
import hmac
import json
import logging
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Optional
from urllib.parse import urlparse

from config import config
from notifier.access_control import access_manager

logger = logging.getLogger(__name__)


class BillingWebhookServer:
    def __init__(self):
        self.host = str(getattr(config, "BILLING_WEBHOOK_HOST", "0.0.0.0"))
        self.port = int(getattr(config, "BILLING_WEBHOOK_PORT", 8787))
        self.enabled = bool(getattr(config, "BILLING_ENABLED", False))
        self._server: Optional[ThreadingHTTPServer] = None
        self._thread: Optional[threading.Thread] = None

    @property
    def running(self) -> bool:
        return bool(self._thread and self._thread.is_alive())

    @staticmethod
    def _json_response(handler: BaseHTTPRequestHandler, status: int, payload: dict) -> None:
        body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        handler.send_response(int(status))
        handler.send_header("Content-Type", "application/json; charset=utf-8")
        handler.send_header("Content-Length", str(len(body)))
        handler.end_headers()
        handler.wfile.write(body)

    @staticmethod
    def _parse_json_bytes(raw_body: bytes) -> Optional[dict]:
        try:
            text = raw_body.decode("utf-8")
            obj = json.loads(text)
            if isinstance(obj, dict):
                return obj
        except Exception:
            return None
        return None

    @staticmethod
    def _safe_int(value, default: int = 0) -> int:
        try:
            return int(str(value).strip())
        except Exception:
            return int(default)

    @staticmethod
    def _parse_stripe_sig_header(header: str) -> tuple[Optional[int], list[str]]:
        ts = None
        sigs: list[str] = []
        for part in (header or "").split(","):
            part = part.strip()
            if "=" not in part:
                continue
            k, v = part.split("=", 1)
            k = k.strip()
            v = v.strip()
            if k == "t" and v.isdigit():
                ts = int(v)
            elif k == "v1" and v:
                sigs.append(v)
        return ts, sigs

    @staticmethod
    def verify_stripe_signature(raw_body: bytes, sig_header: str, secret: str, tolerance_sec: int = 300) -> tuple[bool, str]:
        if not secret:
            return False, "stripe secret missing"
        ts, sigs = BillingWebhookServer._parse_stripe_sig_header(sig_header or "")
        if ts is None or not sigs:
            return False, "invalid stripe signature header"
        now = int(time.time())
        if abs(now - int(ts)) > int(max(0, tolerance_sec)):
            return False, "stripe signature timestamp outside tolerance"
        signed_payload = f"{ts}.".encode("utf-8") + raw_body
        expected = hmac.new(secret.encode("utf-8"), signed_payload, hashlib.sha256).hexdigest()
        for candidate in sigs:
            if hmac.compare_digest(expected, candidate):
                return True, "ok"
        return False, "stripe signature mismatch"

    @staticmethod
    def verify_promptpay_signature(raw_body: bytes, signature: str, secret: str) -> tuple[bool, str]:
        if not secret:
            return False, "promptpay secret missing"
        if not signature:
            return False, "promptpay signature missing"
        expected = hmac.new(secret.encode("utf-8"), raw_body, hashlib.sha256).hexdigest()
        if hmac.compare_digest(expected.lower(), str(signature).strip().lower()):
            return True, "ok"
        return False, "promptpay signature mismatch"

    @staticmethod
    def _extract_metadata(obj: dict) -> dict:
        if not isinstance(obj, dict):
            return {}
        meta = obj.get("metadata")
        if isinstance(meta, dict):
            return {str(k): meta[k] for k in meta.keys()}
        return {}

    def _resolve_upgrade_context(
        self,
        metadata: dict,
        fallback_user_id: Optional[int] = None,
        fallback_plan: Optional[str] = None,
        fallback_days: Optional[int] = None,
    ) -> tuple[Optional[int], Optional[str], int]:
        md = metadata or {}
        uid_candidates = [
            md.get("telegram_user_id"),
            md.get("tg_user_id"),
            md.get("user_id"),
            fallback_user_id,
        ]
        user_id = None
        for c in uid_candidates:
            if c is None:
                continue
            txt = str(c).strip()
            if txt.lstrip("-").isdigit():
                user_id = int(txt)
                break

        plan_candidates = [
            md.get("plan"),
            md.get("tier"),
            fallback_plan,
            getattr(config, "BILLING_DEFAULT_PLAN", "b"),
        ]
        plan = None
        for c in plan_candidates:
            p = str(c or "").strip().lower()
            if p in {"trial", "a", "b", "c"}:
                plan = p
                break

        days_candidates = [
            md.get("days"),
            md.get("duration_days"),
            fallback_days,
            getattr(config, "BILLING_DEFAULT_DAYS", 30),
        ]
        days = 0
        for c in days_candidates:
            d = self._safe_int(c, default=0)
            if d > 0:
                days = d
                break
        if days <= 0:
            days = 30

        return user_id, plan, days

    def _apply_upgrade(
        self,
        provider: str,
        event_id: str,
        user_id: int,
        plan: str,
        days: int,
        amount: float = 0.0,
        currency: str = "",
        payload: Optional[dict] = None,
    ) -> tuple[int, dict]:
        try:
            res = access_manager.apply_payment_upgrade(
                provider=provider,
                event_id=event_id,
                user_id=user_id,
                plan=plan,
                days=days,
                amount=float(amount or 0.0),
                currency=str(currency or "").upper(),
                payload=payload or {},
                note=f"{provider}_webhook",
            )
            logger.info(
                "[Billing] %s event=%s user=%s plan=%s days=%s duplicate=%s applied=%s",
                provider,
                event_id,
                user_id,
                plan,
                days,
                bool(res.get("duplicate")),
                bool(res.get("applied")),
            )
            return 200, {
                "ok": True,
                "provider": provider,
                "event_id": event_id,
                "duplicate": bool(res.get("duplicate")),
                "applied": bool(res.get("applied")),
                "user_id": int(user_id),
                "plan": plan,
                "days": int(days),
            }
        except Exception as e:
            logger.error("[Billing] apply upgrade failed: %s", e, exc_info=True)
            return 500, {"ok": False, "error": f"apply failed: {e}"}

    def _handle_stripe_event(self, raw_body: bytes, headers: dict) -> tuple[int, dict]:
        if not bool(getattr(config, "STRIPE_ENABLED", True)):
            return 403, {"ok": False, "error": "stripe disabled"}

        secret = str(getattr(config, "STRIPE_WEBHOOK_SECRET", "") or "")
        tolerance = int(getattr(config, "STRIPE_SIGNATURE_TOLERANCE_SEC", 300))
        sig_header = str(headers.get("Stripe-Signature", "") or "")
        ok, why = self.verify_stripe_signature(raw_body, sig_header, secret, tolerance_sec=tolerance)
        if not ok:
            return 401, {"ok": False, "error": why}

        event = self._parse_json_bytes(raw_body)
        if not event:
            return 400, {"ok": False, "error": "invalid json"}

        event_id = str(event.get("id") or "")
        event_type = str(event.get("type") or "")
        data_obj = ((event.get("data") or {}).get("object") or {})
        metadata = self._extract_metadata(data_obj)
        if not event_id:
            event_id = hashlib.sha256(raw_body).hexdigest()

        paid = False
        if event_type == "checkout.session.completed":
            status = str(data_obj.get("payment_status", "")).lower()
            paid = status in {"paid", "no_payment_required"}
        elif event_type == "invoice.paid":
            paid = True
        elif event_type == "payment_intent.succeeded":
            paid = True
        else:
            return 200, {"ok": True, "ignored": True, "reason": f"event {event_type} not handled"}

        if not paid:
            return 200, {"ok": True, "ignored": True, "reason": "payment not completed"}

        fallback_uid = None
        client_ref = str(data_obj.get("client_reference_id") or "").strip()
        if client_ref.lstrip("-").isdigit():
            fallback_uid = int(client_ref)

        fallback_plan = None
        fallback_days = None
        price_map = config.get_stripe_price_plan_map()
        price_id = str(metadata.get("price_id") or data_obj.get("price") or "").strip()
        if price_id and price_id in price_map:
            fallback_plan, fallback_days = price_map[price_id]

        user_id, plan, days = self._resolve_upgrade_context(
            metadata,
            fallback_user_id=fallback_uid,
            fallback_plan=fallback_plan,
            fallback_days=fallback_days,
        )
        if user_id is None:
            return 400, {"ok": False, "error": "missing telegram user id metadata"}
        if not plan:
            return 400, {"ok": False, "error": "missing plan metadata"}

        amount_total = float((data_obj.get("amount_total") or 0) or 0) / 100.0
        currency = str(data_obj.get("currency") or "").upper()
        return self._apply_upgrade(
            provider="stripe",
            event_id=event_id,
            user_id=user_id,
            plan=plan,
            days=days,
            amount=amount_total,
            currency=currency,
            payload=event,
        )

    def _handle_promptpay_event(self, raw_body: bytes, headers: dict) -> tuple[int, dict]:
        if not bool(getattr(config, "PROMPTPAY_ENABLED", True)):
            return 403, {"ok": False, "error": "promptpay disabled"}

        require_sig = bool(getattr(config, "PROMPTPAY_REQUIRE_SIGNATURE", True))
        secret = str(getattr(config, "PROMPTPAY_WEBHOOK_SECRET", "") or "")
        header_name = str(getattr(config, "PROMPTPAY_SIGNATURE_HEADER", "X-PromptPay-Signature") or "X-PromptPay-Signature")
        signature = str(headers.get(header_name, "") or "")
        if require_sig:
            ok, why = self.verify_promptpay_signature(raw_body, signature, secret)
            if not ok:
                return 401, {"ok": False, "error": why}

        payload = self._parse_json_bytes(raw_body)
        if not payload:
            return 400, {"ok": False, "error": "invalid json"}

        event_id = str(
            payload.get("event_id")
            or payload.get("transaction_id")
            or payload.get("reference")
            or payload.get("id")
            or ""
        ).strip()
        if not event_id:
            event_id = hashlib.sha256(raw_body).hexdigest()

        status = str(payload.get("status") or payload.get("payment_status") or "").lower()
        if status and status not in {"paid", "success", "succeeded", "completed"}:
            return 200, {"ok": True, "ignored": True, "reason": f"status={status}"}

        metadata = {}
        md = payload.get("metadata")
        if isinstance(md, dict):
            metadata.update(md)
        # also allow flat payload fields
        for key in ("telegram_user_id", "tg_user_id", "user_id", "plan", "tier", "days", "duration_days"):
            if key in payload and key not in metadata:
                metadata[key] = payload.get(key)

        user_id, plan, days = self._resolve_upgrade_context(metadata)
        if user_id is None:
            return 400, {"ok": False, "error": "missing telegram user id"}
        if not plan:
            return 400, {"ok": False, "error": "missing plan"}

        amount = float(payload.get("amount") or 0.0)
        currency = str(payload.get("currency") or "THB").upper()
        return self._apply_upgrade(
            provider="promptpay",
            event_id=event_id,
            user_id=user_id,
            plan=plan,
            days=days,
            amount=amount,
            currency=currency,
            payload=payload,
        )

    def _handle_post(self, path: str, raw_body: bytes, headers: dict) -> tuple[int, dict]:
        if path == "/webhook/stripe":
            return self._handle_stripe_event(raw_body, headers)
        if path == "/webhook/promptpay":
            return self._handle_promptpay_event(raw_body, headers)
        return 404, {"ok": False, "error": "not found"}

    def _handler_factory(self):
        owner = self

        class _Handler(BaseHTTPRequestHandler):
            def log_message(self, fmt: str, *args):  # noqa: D401
                logger.info("[BillingWebhook] %s - %s", self.address_string(), fmt % args)

            def do_GET(self):  # noqa: N802
                path = urlparse(self.path).path
                if path == "/health":
                    BillingWebhookServer._json_response(
                        self,
                        200,
                        {
                            "ok": True,
                            "service": "billing_webhook",
                            "running": owner.running,
                            "stripe_enabled": bool(getattr(config, "STRIPE_ENABLED", True)),
                            "promptpay_enabled": bool(getattr(config, "PROMPTPAY_ENABLED", True)),
                        },
                    )
                    return
                BillingWebhookServer._json_response(self, 404, {"ok": False, "error": "not found"})

            def do_POST(self):  # noqa: N802
                path = urlparse(self.path).path
                content_length = int(self.headers.get("Content-Length", "0") or 0)
                raw_body = self.rfile.read(content_length) if content_length > 0 else b""
                headers = {k: v for k, v in self.headers.items()}
                status, payload = owner._handle_post(path, raw_body, headers)
                BillingWebhookServer._json_response(self, status, payload)

        return _Handler

    def start(self) -> bool:
        if not self.enabled:
            logger.info("[BillingWebhook] Disabled (BILLING_ENABLED=0)")
            return False
        if self.running:
            return True
        try:
            self._server = ThreadingHTTPServer((self.host, self.port), self._handler_factory())
            self._server.daemon_threads = True
            self._thread = threading.Thread(target=self._server.serve_forever, daemon=True, name="BillingWebhookServer")
            self._thread.start()
            logger.info("[BillingWebhook] Listening on http://%s:%s", self.host, self.port)
            return True
        except Exception as e:
            logger.error("[BillingWebhook] Start failed: %s", e, exc_info=True)
            self._server = None
            self._thread = None
            return False

    def stop(self) -> None:
        if self._server is None:
            return
        try:
            self._server.shutdown()
        except Exception:
            pass
        try:
            self._server.server_close()
        except Exception:
            pass
        self._server = None
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=5)
        self._thread = None
        logger.info("[BillingWebhook] Stopped")

    def status(self) -> dict:
        return {
            "enabled": bool(self.enabled),
            "running": bool(self.running),
            "host": self.host,
            "port": self.port,
            "stripe_enabled": bool(getattr(config, "STRIPE_ENABLED", True)),
            "promptpay_enabled": bool(getattr(config, "PROMPTPAY_ENABLED", True)),
        }

    def run_forever(self) -> None:
        if not self.start():
            return
        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            pass
        finally:
            self.stop()


billing_webhook_server = BillingWebhookServer()

