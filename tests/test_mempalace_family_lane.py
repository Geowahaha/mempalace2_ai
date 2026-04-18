from __future__ import annotations

import unittest

from config import Config
from execution.ctrader_executor import CTraderExecutor


class MempalaceFamilyLaneTests(unittest.TestCase):
    def setUp(self) -> None:
        self._orig_enabled = Config.DEXTER_MEMPALACE_FAMILY_LANE_ENABLED
        self._orig_family = Config.DEXTER_MEMPALACE_FAMILY_NAME
        self._orig_tokens = Config.DEXTER_MEMPALACE_SOURCE_TOKENS
        self._orig_active = Config.CTRADER_XAU_ACTIVE_FAMILIES

    def tearDown(self) -> None:
        Config.DEXTER_MEMPALACE_FAMILY_LANE_ENABLED = self._orig_enabled
        Config.DEXTER_MEMPALACE_FAMILY_NAME = self._orig_family
        Config.DEXTER_MEMPALACE_SOURCE_TOKENS = self._orig_tokens
        Config.CTRADER_XAU_ACTIVE_FAMILIES = self._orig_active

    def test_ctrader_active_families_include_mempalace_when_enabled(self) -> None:
        Config.CTRADER_XAU_ACTIVE_FAMILIES = "xau_scalp_pullback_limit"
        Config.DEXTER_MEMPALACE_FAMILY_LANE_ENABLED = True
        Config.DEXTER_MEMPALACE_FAMILY_NAME = "xau_scalp_mempalace_lane"

        families = Config.get_ctrader_xau_active_families()

        self.assertIn("xau_scalp_pullback_limit", families)
        self.assertIn("xau_scalp_mempalace_lane", families)

    def test_source_family_maps_mempalace_token_when_enabled(self) -> None:
        Config.DEXTER_MEMPALACE_FAMILY_LANE_ENABLED = True
        Config.DEXTER_MEMPALACE_FAMILY_NAME = "xau_scalp_mempalace_lane"
        Config.DEXTER_MEMPALACE_SOURCE_TOKENS = "mempalace,atlas"

        family = CTraderExecutor._source_family("scalp_xauusd:mempalace:live")
        self.assertEqual(family, "xau_scalp_mempalace_lane")

    def test_source_family_ignores_mempalace_token_when_disabled(self) -> None:
        Config.DEXTER_MEMPALACE_FAMILY_LANE_ENABLED = False
        Config.DEXTER_MEMPALACE_SOURCE_TOKENS = "mempalace"

        family = CTraderExecutor._source_family("scalp_xauusd:mempalace:live")
        self.assertEqual(family, "xau_scalp_microtrend")


if __name__ == "__main__":
    unittest.main()
