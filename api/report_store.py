"""
api/report_store.py - Tiger Hunter Report Cache
Simple JSON-based storage to hold the latest VI stock data, US Open assistance,
and system status reports so the Web3 Dashboard can display them.
"""
import os
import json
import logging
from typing import Optional, Dict, Any

logger = logging.getLogger(__name__)

REPORTS_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data", "reports")

class ReportStore:
    def __init__(self, data_dir: str = REPORTS_DIR):
        self.data_dir = data_dir
        os.makedirs(self.data_dir, exist_ok=True)
    
    def _path(self, report_type: str) -> str:
        return os.path.join(self.data_dir, f"{report_type}.json")

    def save_report(self, report_type: str, data: Any) -> bool:
        """Cache the latest report payload as JSON."""
        try:
            path = self._path(report_type)
            import dataclasses
            def _custom_default(obj):
                if dataclasses.is_dataclass(obj):
                    return dataclasses.asdict(obj)
                return str(obj)
            
            with open(path, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, default=_custom_default)
            return True
        except Exception as e:
            logger.error(f"Failed to save {report_type} report: {e}")
            return False

    def get_report(self, report_type: str) -> Optional[Dict[str, Any]]:
        """Retrieve the latest cached report."""
        try:
            path = self._path(report_type)
            if not os.path.exists(path):
                return None
            with open(path, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception as e:
            logger.error(f"Failed to load {report_type} report: {e}")
            return None


# Singleton
report_store = ReportStore()
