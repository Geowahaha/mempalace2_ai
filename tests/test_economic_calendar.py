import unittest
from datetime import datetime, timedelta, timezone
from unittest.mock import patch
from zoneinfo import ZoneInfo

import scheduler as scheduler_module
from market.economic_calendar import EconomicCalendar, EconomicEvent


class EconomicCalendarTests(unittest.TestCase):
    def test_fetch_events_parses_and_filters_unsupported_times(self):
        cal = EconomicCalendar()
        ny_now = datetime.now(timezone.utc).astimezone(ZoneInfo("America/New_York"))
        date_txt = ny_now.strftime("%m-%d-%Y")
        time_txt = (ny_now + timedelta(minutes=50)).strftime("%I:%M%p").lstrip("0").lower()
        xml_text = f"""
<weeklyevents>
  <event>
    <title>US CPI m/m</title>
    <country>USD</country>
    <date>{date_txt}</date>
    <time>{time_txt}</time>
    <impact>High</impact>
    <forecast>0.3%</forecast>
    <previous>0.2%</previous>
    <url>https://example.com/cpi</url>
  </event>
  <event>
    <title>Holiday</title>
    <country>USD</country>
    <date>{date_txt}</date>
    <time>All Day</time>
    <impact>Low</impact>
  </event>
</weeklyevents>
""".strip()

        with patch.object(cal, "_download_feed", return_value=xml_text):
            events = cal.fetch_events(force=True)

        self.assertEqual(len(events), 1)
        self.assertEqual(events[0].currency, "USD")
        self.assertEqual(events[0].impact, "high")

    def test_scheduler_dedupes_calendar_alert_per_window(self):
        dexter = scheduler_module.DexterScheduler()
        ev = EconomicEvent(
            event_id="evt123",
            title="US NFP",
            currency="USD",
            impact="high",
            forecast="",
            previous="",
            actual="",
            source_url="",
            time_utc=datetime.now(timezone.utc) + timedelta(minutes=60),
        )

        with patch.object(scheduler_module.config, "ECON_CALENDAR_ENABLED", True), \
             patch.object(scheduler_module.config, "ECON_CALENDAR_MIN_IMPACT", "high"), \
             patch.object(scheduler_module.config, "ECON_ALERT_TOLERANCE_MIN", 3), \
             patch.object(scheduler_module.config, "get_econ_alert_windows", return_value=[60]), \
             patch.object(scheduler_module.config, "get_econ_alert_currencies", return_value={"USD"}), \
             patch.object(scheduler_module.economic_calendar, "upcoming_events", return_value=[ev]), \
             patch.object(scheduler_module.notifier, "send_economic_calendar_alert", return_value=True) as send_alert:
            dexter._run_economic_calendar_alerts()
            dexter._run_economic_calendar_alerts()

        self.assertEqual(send_alert.call_count, 1)


if __name__ == "__main__":
    unittest.main()

