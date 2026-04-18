from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from copy_trade.accounts import FollowerAccount
import copy_trade.manager as manager_module
from copy_trade.manager import CopyTradeManager, CopyTradeResult


class _StubRegistry:
    def __init__(self, follower: FollowerAccount):
        self._follower = follower

    def get(self, account_id: str):
        if account_id == self._follower.account_id:
            return self._follower
        return None

    def list_accounts(self):
        return [self._follower]

    def update_trade_stats(self, _account_id: str) -> None:
        return None


class CopyTradeFollowTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.tmp_path = Path(self.tmp.name)
        self.follower = FollowerAccount(
            account_id="ct_9900",
            label="CopierA",
            broker="ctrader",
            ctrader_account_id=9900,
            enabled=True,
        )
        self.registry = _StubRegistry(self.follower)
        self._orig_registry = manager_module.account_registry
        manager_module.account_registry = self.registry
        self.manager = CopyTradeManager()
        self.manager.enabled = True
        self.manager.close_follow_enabled = True
        self.manager.protection_follow_enabled = True
        self.manager._links_path = self.tmp_path / "copy_trade_links.json"
        self.manager._follow_log_path = self.tmp_path / "copy_trade_follow_log.jsonl"
        self.manager._position_links = {}
        self.manager._order_links = {}
        self.manager._recent_close_events = {}

    def tearDown(self) -> None:
        manager_module.account_registry = self._orig_registry
        self.tmp.cleanup()

    def test_protection_follow_amends_follower_position(self) -> None:
        self.manager._position_links = {
            "123": [
                {
                    "follower_account_id": self.follower.account_id,
                    "follower_label": self.follower.label,
                    "broker": self.follower.broker,
                    "follower_position_id": 998877,
                    "follower_order_id": 0,
                }
            ]
        }
        calls: list[tuple[str, dict]] = []

        def _fake_run(**kwargs):
            calls.append((str(kwargs["mode"]), dict(kwargs["payload"])))
            return CopyTradeResult(
                account_id=self.follower.account_id,
                label=self.follower.label,
                broker=self.follower.broker,
                ok=True,
                status="amended",
                message="ok",
                position_id=kwargs["payload"].get("position_id"),
            )

        self.manager._run_ctrader_worker_mode = _fake_run  # type: ignore[method-assign]

        out = self.manager.sync_protection_follow(
            master_position_id=123,
            stop_loss=4861.2,
            take_profit=4870.8,
            reason="unit_test",
        )

        self.assertEqual(len(out), 1)
        self.assertTrue(out[0].ok)
        self.assertEqual(len(calls), 1)
        self.assertEqual(calls[0][0], "amend_position_sltp")
        self.assertEqual(calls[0][1].get("position_id"), 998877)

    def test_close_follow_removes_position_link_on_success(self) -> None:
        self.manager._position_links = {
            "555": [
                {
                    "follower_account_id": self.follower.account_id,
                    "follower_label": self.follower.label,
                    "broker": self.follower.broker,
                    "follower_position_id": 111222,
                    "follower_order_id": 0,
                }
            ]
        }
        calls: list[tuple[str, dict]] = []

        def _fake_run(**kwargs):
            calls.append((str(kwargs["mode"]), dict(kwargs["payload"])))
            return CopyTradeResult(
                account_id=self.follower.account_id,
                label=self.follower.label,
                broker=self.follower.broker,
                ok=True,
                status="closed",
                message="ok",
                position_id=kwargs["payload"].get("position_id"),
            )

        self.manager._run_ctrader_worker_mode = _fake_run  # type: ignore[method-assign]

        out = self.manager.enforce_close_follow(
            master_position_id=555,
            reason="unit_test",
            master_close_utc="2026-04-18T01:00:00Z",
        )

        self.assertEqual(len(out), 1)
        self.assertTrue(out[0].ok)
        self.assertEqual(len(calls), 1)
        self.assertEqual(calls[0][0], "close")
        self.assertNotIn("555", self.manager._position_links)


if __name__ == "__main__":
    unittest.main()
