import unittest
from unittest.mock import patch

from config import config
from notifier.billing_checkout import StripeCheckoutService


class _FakeResp:
    def __init__(self, status_code: int, payload: dict):
        self.status_code = status_code
        self._payload = payload

    def json(self):
        return self._payload


class BillingCheckoutTests(unittest.TestCase):
    def setUp(self):
        cls = type(config)
        self.old = {
            "STRIPE_ENABLED": cls.STRIPE_ENABLED,
            "STRIPE_SECRET_KEY": cls.STRIPE_SECRET_KEY,
            "STRIPE_PRICE_ID_A": cls.STRIPE_PRICE_ID_A,
            "STRIPE_PRICE_ID_B": cls.STRIPE_PRICE_ID_B,
            "STRIPE_PRICE_ID_C": cls.STRIPE_PRICE_ID_C,
            "STRIPE_CHECKOUT_SUCCESS_URL": cls.STRIPE_CHECKOUT_SUCCESS_URL,
            "STRIPE_CHECKOUT_CANCEL_URL": cls.STRIPE_CHECKOUT_CANCEL_URL,
            "BILLING_CURRENCY": cls.BILLING_CURRENCY,
            "BILLING_PRICE_A_CENTS": cls.BILLING_PRICE_A_CENTS,
            "BILLING_PRICE_B_CENTS": cls.BILLING_PRICE_B_CENTS,
            "BILLING_PRICE_C_CENTS": cls.BILLING_PRICE_C_CENTS,
            "BILLING_PLAN_DAYS_A": cls.BILLING_PLAN_DAYS_A,
            "BILLING_PLAN_DAYS_B": cls.BILLING_PLAN_DAYS_B,
            "BILLING_PLAN_DAYS_C": cls.BILLING_PLAN_DAYS_C,
        }
        cls.STRIPE_ENABLED = True
        cls.STRIPE_SECRET_KEY = "sk_test_123"
        cls.STRIPE_PRICE_ID_A = "price_A"
        cls.STRIPE_PRICE_ID_B = "price_B"
        cls.STRIPE_PRICE_ID_C = "price_C"
        cls.STRIPE_CHECKOUT_SUCCESS_URL = "https://example.com/success"
        cls.STRIPE_CHECKOUT_CANCEL_URL = "https://example.com/cancel"
        cls.BILLING_CURRENCY = "usd"
        cls.BILLING_PRICE_A_CENTS = 1900
        cls.BILLING_PRICE_B_CENTS = 4900
        cls.BILLING_PRICE_C_CENTS = 12900
        cls.BILLING_PLAN_DAYS_A = 30
        cls.BILLING_PLAN_DAYS_B = 30
        cls.BILLING_PLAN_DAYS_C = 90
        self.svc = StripeCheckoutService()

    def tearDown(self):
        cls = type(config)
        for key, value in self.old.items():
            setattr(cls, key, value)

    def test_create_checkout_session_success(self):
        with patch("notifier.billing_checkout.requests.post") as post:
            post.return_value = _FakeResp(
                200,
                {"id": "cs_test_1", "url": "https://checkout.stripe.com/pay/cs_test_1"},
            )
            res = self.svc.create_checkout_session(user_id=1585019324, plan="b")
        self.assertTrue(res.ok)
        self.assertEqual(res.plan, "b")
        self.assertEqual(res.days, 30)
        self.assertIn("checkout.stripe.com", res.url)

    def test_create_checkout_session_missing_price_id(self):
        type(config).STRIPE_PRICE_ID_B = ""
        type(config).BILLING_PRICE_B_CENTS = 4900
        with patch("notifier.billing_checkout.requests.post") as post:
            post.return_value = _FakeResp(
                200,
                {"id": "cs_test_2", "url": "https://checkout.stripe.com/pay/cs_test_2"},
            )
            res = self.svc.create_checkout_session(user_id=1, plan="b")
        self.assertTrue(res.ok)
        self.assertIn("checkout.stripe.com", res.url)

    def test_create_checkout_session_missing_price_and_cents_fails(self):
        type(config).STRIPE_PRICE_ID_B = ""
        type(config).BILLING_PRICE_B_CENTS = 0
        res = self.svc.create_checkout_session(user_id=1, plan="b")
        self.assertFalse(res.ok)
        self.assertIn("Missing Stripe price id", res.message)


if __name__ == "__main__":
    unittest.main()
