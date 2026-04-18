"""
notifier/billing_checkout.py
Stripe Checkout session creation for direct in-bot upgrade payments.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Optional

import requests

from config import config

logger = logging.getLogger(__name__)


@dataclass
class CheckoutSessionResult:
    ok: bool
    message: str
    url: str = ""
    session_id: str = ""
    plan: str = ""
    days: int = 0


class StripeCheckoutService:
    def __init__(self):
        self.endpoint = "https://api.stripe.com/v1/checkout/sessions"

    def enabled(self) -> bool:
        return bool(config.STRIPE_ENABLED and config.STRIPE_SECRET_KEY)

    def _resolve_plan(self, raw_plan: str) -> Optional[str]:
        p = str(raw_plan or "").strip().lower()
        if p in {"a", "b", "c"}:
            return p
        return None

    def _plan_days(self, plan: str) -> int:
        return int(config.get_plan_days_map().get(plan, max(1, int(config.BILLING_DEFAULT_DAYS))))

    def _plan_price_id(self, plan: str) -> str:
        return str(config.get_stripe_plan_price_ids().get(plan, "")).strip()

    def _plan_amount_cents(self, plan: str) -> int:
        return int(config.get_plan_price_cents_map().get(plan, 0))

    def create_checkout_session(self, user_id: int, plan: str) -> CheckoutSessionResult:
        if not self.enabled():
            return CheckoutSessionResult(False, "Stripe checkout not configured (STRIPE_ENABLED/STRIPE_SECRET_KEY).")

        p = self._resolve_plan(plan)
        if not p:
            return CheckoutSessionResult(False, "Invalid plan. Use: a, b, or c.")

        price_id = self._plan_price_id(p)
        success_url = str(config.STRIPE_CHECKOUT_SUCCESS_URL or "").strip()
        cancel_url = str(config.STRIPE_CHECKOUT_CANCEL_URL or "").strip()
        if not success_url or not cancel_url:
            return CheckoutSessionResult(False, "Missing STRIPE_CHECKOUT_SUCCESS_URL or STRIPE_CHECKOUT_CANCEL_URL.")

        days = self._plan_days(p)
        metadata = {
            "telegram_user_id": str(int(user_id)),
            "plan": p,
            "days": str(int(days)),
            "source": "telegram_bot_upgrade",
        }
        form = {
            "mode": "payment",
            "success_url": success_url,
            "cancel_url": cancel_url,
            "client_reference_id": str(int(user_id)),
            "metadata[telegram_user_id]": metadata["telegram_user_id"],
            "metadata[plan]": metadata["plan"],
            "metadata[days]": metadata["days"],
            "metadata[source]": metadata["source"],
        }
        if price_id:
            form["line_items[0][price]"] = price_id
            form["line_items[0][quantity]"] = "1"
        else:
            amount_cents = self._plan_amount_cents(p)
            currency = str(config.BILLING_CURRENCY or "usd").strip().lower()
            if amount_cents <= 0:
                return CheckoutSessionResult(
                    False,
                    f"Missing Stripe price id for plan {p.upper()} and BILLING_PRICE_{p.upper()}_CENTS is not set."
                )
            form["line_items[0][price_data][currency]"] = currency
            form["line_items[0][price_data][unit_amount]"] = str(int(amount_cents))
            form["line_items[0][price_data][product_data][name]"] = f"Dexter Pro Plan {p.upper()}"
            form["line_items[0][price_data][product_data][description]"] = f"{days} days access"
            form["line_items[0][quantity]"] = "1"

        try:
            resp = requests.post(
                self.endpoint,
                data=form,
                headers={"Authorization": f"Bearer {config.STRIPE_SECRET_KEY.strip()}"},
                timeout=20,
            )
            payload = resp.json()
        except Exception as e:
            return CheckoutSessionResult(False, f"Stripe request failed: {e}")

        if resp.status_code >= 400:
            err = ((payload or {}).get("error") or {}).get("message") or f"HTTP {resp.status_code}"
            logger.warning("[Checkout] Stripe create session failed: %s", err)
            return CheckoutSessionResult(False, f"Stripe error: {err}")

        session_url = str((payload or {}).get("url") or "")
        session_id = str((payload or {}).get("id") or "")
        if not session_url:
            return CheckoutSessionResult(False, "Stripe did not return checkout URL.")

        return CheckoutSessionResult(
            ok=True,
            message="Checkout session created",
            url=session_url,
            session_id=session_id,
            plan=p,
            days=days,
        )


stripe_checkout = StripeCheckoutService()
