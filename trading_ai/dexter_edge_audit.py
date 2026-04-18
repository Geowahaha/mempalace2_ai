"""Read-only Dexter edge auditor.

This module is intentionally one-way: it reads aggregate evidence from Dexter's
SQLite databases and writes a report inside the Mempalace repo. It never reads
Dexter .env files, never writes into Dexter, and never prints account IDs or
raw request/response payloads.
"""

from __future__ import annotations

import argparse
import json
import math
import re
import sqlite3
import subprocess
import xml.etree.ElementTree as ET
from collections import defaultdict
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Iterable, Sequence
from zipfile import ZipFile


DEFAULT_DEXTER_ROOT = Path(r"D:\dexter_pro_v3_fixed\dexter_pro_v3_fixed")
DEFAULT_OUTPUT = Path(r"D:\Mempalac_AI\reports\DEXTER_EDGE_AUDIT.md")
XLSX_NS = {"a": "http://schemas.openxmlformats.org/spreadsheetml/2006/main"}
SENSITIVE_ENV_RE = re.compile(
    r"(TOKEN|SECRET|PASSWORD|API[_-]?KEY|PRIVATE|CLIENT_ID|CLIENT_SECRET|ACCOUNT|LOGIN|AUTH|"
    r"(^|_)PASS($|_)|"
    r"TELEGRAM|WEBHOOK|CHAT_ID|ADMIN_ID|EMAIL)",
    re.IGNORECASE,
)
TRADING_ENV_RE = re.compile(
    r"(RISK|LOT|VOLUME|FIBO|XAU|BTC|ETH|CRYPTO|CANARY|LIVE|DRY|DEMO|CTRADER|OPENAPI|HOST|PORT|"
    r"MAX_|MIN_|ENABLE|DISABLE|SCALP|LANE|SESSION|TP|SL|STOP|TRAIL|KILL|GATE|THRESH|CONF|"
    r"SPREAD|NEWS|DRAWDOWN|LOSS|WINNER|DIRECT)",
    re.IGNORECASE,
)


@dataclass(frozen=True)
class DatabasePaths:
    ctrader: Path
    backtest: Path


def connect_readonly(path: Path) -> sqlite3.Connection:
    """Open a SQLite database in read-only mode."""
    if not path.exists():
        raise FileNotFoundError(f"Database not found: {path}")
    uri = f"file:{path.as_posix()}?mode=ro"
    conn = sqlite3.connect(uri, uri=True)
    conn.row_factory = sqlite3.Row
    return conn


def rows(conn: sqlite3.Connection, sql: str, params: Sequence[Any] = ()) -> list[dict[str, Any]]:
    return [dict(row) for row in conn.execute(sql, params).fetchall()]


def one(conn: sqlite3.Connection, sql: str, params: Sequence[Any] = ()) -> dict[str, Any]:
    row = conn.execute(sql, params).fetchone()
    return dict(row) if row else {}


def table_exists(conn: sqlite3.Connection, name: str) -> bool:
    found = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=? LIMIT 1",
        (name,),
    ).fetchone()
    return found is not None


def value(v: Any) -> str:
    if v is None:
        return ""
    if isinstance(v, float):
        if math.isnan(v) or math.isinf(v):
            return ""
        if abs(v) >= 100:
            return f"{v:,.2f}"
        return f"{v:.4f}".rstrip("0").rstrip(".")
    if isinstance(v, int):
        return f"{v:,}"
    text = str(v)
    return text.replace("|", "\\|").replace("\n", " ").strip()


def pct(v: Any) -> str:
    if v is None:
        return ""
    try:
        return f"{float(v) * 100:.1f}%"
    except (TypeError, ValueError):
        return ""


def usd(v: Any) -> str:
    if v is None:
        return ""
    try:
        return f"{float(v):,.2f}"
    except (TypeError, ValueError):
        return ""


def md_table(data: Iterable[dict[str, Any]], columns: Sequence[tuple[str, str, str]]) -> str:
    items = list(data)
    if not items:
        return "_No rows._\n"

    header = "| " + " | ".join(label for _, label, _ in columns) + " |"
    sep = "| " + " | ".join("---" for _ in columns) + " |"
    lines = [header, sep]
    for item in items:
        cells: list[str] = []
        for key, _, fmt in columns:
            cell = item.get(key)
            if fmt == "pct":
                cells.append(pct(cell))
            elif fmt == "usd":
                cells.append(usd(cell))
            else:
                cells.append(value(cell))
        lines.append("| " + " | ".join(cells) + " |")
    return "\n".join(lines) + "\n"


def to_float(v: Any) -> float:
    if v is None:
        return 0.0
    if isinstance(v, (int, float)):
        return float(v)
    text = str(v).strip().replace(",", "")
    if not text:
        return 0.0
    try:
        return float(text)
    except ValueError:
        return 0.0


def parse_export_time(text: Any) -> datetime | None:
    raw = str(text or "").strip()
    for fmt in ("%d/%m/%Y %H:%M:%S.%f", "%d/%m/%Y %H:%M:%S", "%Y-%m-%d %H:%M:%S"):
        try:
            return datetime.strptime(raw, fmt)
        except ValueError:
            continue
    return None


def col_to_index(cell_ref: str) -> int:
    letters = "".join(ch for ch in cell_ref if ch.isalpha())
    idx = 0
    for ch in letters:
        idx = idx * 26 + ord(ch.upper()) - 64
    return max(0, idx - 1)


def read_xlsx_records(path: Path) -> list[dict[str, str]]:
    """Read the first worksheet from a simple XLSX using only stdlib."""
    if not path.exists():
        raise FileNotFoundError(f"Trade export not found: {path}")

    with ZipFile(path) as zf:
        names = set(zf.namelist())
        shared: list[str] = []
        if "xl/sharedStrings.xml" in names:
            root = ET.fromstring(zf.read("xl/sharedStrings.xml"))
            for si in root.findall("a:si", XLSX_NS):
                text = "".join(
                    node.text or ""
                    for node in si.iter("{http://schemas.openxmlformats.org/spreadsheetml/2006/main}t")
                )
                shared.append(text)

        sheet_name = "xl/worksheets/sheet1.xml"
        if sheet_name not in names:
            candidates = sorted(name for name in names if name.startswith("xl/worksheets/sheet") and name.endswith(".xml"))
            if not candidates:
                return []
            sheet_name = candidates[0]

        root = ET.fromstring(zf.read(sheet_name))
        raw_rows: list[list[str]] = []
        for row in root.findall(".//a:sheetData/a:row", XLSX_NS):
            cells: dict[int, str] = {}
            for cell in row.findall("a:c", XLSX_NS):
                idx = col_to_index(cell.attrib.get("r", "A"))
                ctype = cell.attrib.get("t")
                vnode = cell.find("a:v", XLSX_NS)
                raw = vnode.text if vnode is not None else ""
                if ctype == "s" and raw:
                    val = shared[int(raw)] if int(raw) < len(shared) else ""
                else:
                    val = raw
                cells[idx] = val
            if cells:
                raw_rows.append([cells.get(i, "") for i in range(max(cells) + 1)])

    if not raw_rows:
        return []
    headers = [str(h).strip() for h in raw_rows[0]]
    records: list[dict[str, str]] = []
    for row in raw_rows[1:]:
        record = {headers[i]: (row[i] if i < len(row) else "") for i in range(len(headers))}
        if any(str(v).strip() for v in record.values()):
            records.append(record)
    return records


def export_trade_summary(path: Path | None) -> dict[str, Any] | None:
    if not path:
        return None

    records = read_xlsx_records(path)
    normalized: list[dict[str, Any]] = []
    for rec in records:
        pnl = to_float(rec.get("Net $"))
        close_time = parse_export_time(rec.get("Closing time"))
        normalized.append(
            {
                "symbol": str(rec.get("Symbol") or "").upper(),
                "direction": str(rec.get("Opening direction") or "").lower(),
                "close_time": close_time,
                "close_time_text": str(rec.get("Closing time") or ""),
                "quantity": to_float(rec.get("Closing Quantity")),
                "volume": to_float(rec.get("Closing volume")),
                "pnl_usd": pnl,
                "balance_usd": to_float(rec.get("Balance $")),
            }
        )

    summary = {
        "rows": len(normalized),
        "wins": sum(1 for r in normalized if r["pnl_usd"] > 0),
        "losses": sum(1 for r in normalized if r["pnl_usd"] < 0),
        "pnl_usd": sum(r["pnl_usd"] for r in normalized),
        "avg_pnl_usd": (sum(r["pnl_usd"] for r in normalized) / len(normalized)) if normalized else 0.0,
        "win_rate": (
            sum(1 for r in normalized if r["pnl_usd"] > 0)
            / max(sum(1 for r in normalized if r["pnl_usd"] != 0), 1)
        ),
        "first_close": min((r["close_time"] for r in normalized if r["close_time"]), default=None),
        "last_close": max((r["close_time"] for r in normalized if r["close_time"]), default=None),
    }

    by_symbol_direction: dict[tuple[str, str], dict[str, Any]] = defaultdict(
        lambda: {
            "trades": 0,
            "wins": 0,
            "losses": 0,
            "pnl_usd": 0.0,
            "quantity_sum": 0.0,
            "min_quantity": None,
            "max_quantity": None,
        }
    )
    by_day: dict[str, dict[str, Any]] = defaultdict(lambda: {"trades": 0, "wins": 0, "losses": 0, "pnl_usd": 0.0})
    by_hour: dict[str, dict[str, Any]] = defaultdict(lambda: {"trades": 0, "wins": 0, "losses": 0, "pnl_usd": 0.0})

    for rec in normalized:
        key = (rec["symbol"], rec["direction"])
        agg = by_symbol_direction[key]
        agg["trades"] += 1
        agg["wins"] += int(rec["pnl_usd"] > 0)
        agg["losses"] += int(rec["pnl_usd"] < 0)
        agg["pnl_usd"] += rec["pnl_usd"]
        agg["quantity_sum"] += rec["quantity"]
        agg["min_quantity"] = rec["quantity"] if agg["min_quantity"] is None else min(agg["min_quantity"], rec["quantity"])
        agg["max_quantity"] = rec["quantity"] if agg["max_quantity"] is None else max(agg["max_quantity"], rec["quantity"])

        if rec["close_time"]:
            day = rec["close_time"].strftime("%Y-%m-%d")
            hour = rec["close_time"].strftime("%Y-%m-%d %H:00")
        else:
            day = "unknown"
            hour = "unknown"
        for bucket, store in ((day, by_day), (hour, by_hour)):
            b = store[bucket]
            b["trades"] += 1
            b["wins"] += int(rec["pnl_usd"] > 0)
            b["losses"] += int(rec["pnl_usd"] < 0)
            b["pnl_usd"] += rec["pnl_usd"]

    symbol_rows: list[dict[str, Any]] = []
    for (symbol, direction), agg in by_symbol_direction.items():
        trades = int(agg["trades"])
        symbol_rows.append(
            {
                "symbol": symbol,
                "direction": direction,
                "trades": trades,
                "wins": agg["wins"],
                "losses": agg["losses"],
                "pnl_usd": agg["pnl_usd"],
                "avg_pnl_usd": agg["pnl_usd"] / max(trades, 1),
                "win_rate": agg["wins"] / max(agg["wins"] + agg["losses"], 1),
                "min_quantity": agg["min_quantity"],
                "avg_quantity": agg["quantity_sum"] / max(trades, 1),
                "max_quantity": agg["max_quantity"],
            }
        )

    day_rows = [
        {
            "period": day,
            "trades": agg["trades"],
            "wins": agg["wins"],
            "losses": agg["losses"],
            "pnl_usd": agg["pnl_usd"],
            "avg_pnl_usd": agg["pnl_usd"] / max(agg["trades"], 1),
            "win_rate": agg["wins"] / max(agg["wins"] + agg["losses"], 1),
        }
        for day, agg in by_day.items()
    ]
    hour_rows = [
        {
            "period": hour,
            "trades": agg["trades"],
            "wins": agg["wins"],
            "losses": agg["losses"],
            "pnl_usd": agg["pnl_usd"],
            "avg_pnl_usd": agg["pnl_usd"] / max(agg["trades"], 1),
            "win_rate": agg["wins"] / max(agg["wins"] + agg["losses"], 1),
        }
        for hour, agg in by_hour.items()
    ]

    balance_points = sorted(
        (rec for rec in normalized if rec["close_time"] and rec["balance_usd"]),
        key=lambda r: r["close_time"],
    )
    peak = None
    max_drawdown = 0.0
    for rec in balance_points:
        balance = float(rec["balance_usd"])
        peak = balance if peak is None else max(peak, balance)
        max_drawdown = min(max_drawdown, balance - peak)

    return {
        "summary": summary,
        "by_symbol_direction": sorted(symbol_rows, key=lambda r: r["pnl_usd"]),
        "by_day": sorted(day_rows, key=lambda r: r["period"]),
        "by_hour_worst": sorted(hour_rows, key=lambda r: r["pnl_usd"])[:15],
        "by_hour_best": sorted(hour_rows, key=lambda r: r["pnl_usd"], reverse=True)[:15],
        "max_drawdown_usd": max_drawdown,
    }


def git_timeline(dexter_root: Path) -> list[dict[str, Any]]:
    try:
        proc = subprocess.run(
            [
                "git",
                "-C",
                str(dexter_root),
                "log",
                "--date=iso-strict",
                "--pretty=format:%h|%cI|%s",
                "--max-count=30",
            ],
            check=False,
            capture_output=True,
            text=True,
            timeout=20,
        )
    except (OSError, subprocess.SubprocessError):
        return []

    if proc.returncode != 0:
        return []

    timeline: list[dict[str, Any]] = []
    for line in proc.stdout.splitlines():
        parts = line.split("|", 2)
        if len(parts) != 3:
            continue
        subject_lower = parts[2].lower()
        if "fibo" in subject_lower:
            area = "fibo"
        elif "btc" in subject_lower or "eth" in subject_lower or "multi-symbol" in subject_lower:
            area = "multi-symbol"
        elif "dom" in subject_lower:
            area = "dom"
        elif "sharpness" in subject_lower or "learning" in subject_lower:
            area = "learning"
        else:
            area = "general"
        timeline.append({"commit": parts[0], "commit_time": parts[1], "area": area, "subject": parts[2]})
    return timeline


def clean_env_value(raw: str) -> str:
    """Strip quotes and trailing inline comments without evaluating the value."""
    val = raw.strip()
    if " #" in val:
        val = val.split(" #", 1)[0].rstrip()
    if len(val) >= 2 and val[0] == val[-1] and val[0] in {"'", '"'}:
        val = val[1:-1]
    return val


def env_category(key: str) -> str:
    upper = key.upper()
    if "FIBO" in upper:
        return "fibo"
    if "BTC" in upper or "ETH" in upper or "CRYPTO" in upper:
        return "crypto"
    if "XAU" in upper or "GOLD" in upper:
        return "xau"
    if "CTRADER" in upper or "OPENAPI" in upper:
        return "ctrader"
    if "RISK" in upper or "LOT" in upper or "VOLUME" in upper:
        return "risk-sizing"
    if "CANARY" in upper or "LANE" in upper or "WINNER" in upper:
        return "lane-governance"
    if "CONF" in upper or "THRESH" in upper or "GATE" in upper:
        return "gates"
    return "general"


def sanitized_env_snapshot(dexter_root: Path, max_rows: int = 260) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    env_path = dexter_root / ".env.local"
    summary = {
        "env_present": env_path.exists(),
        "interesting_keys": 0,
        "redacted_keys": 0,
        "shown_keys": 0,
        "path_note": ".env.local read in sanitized mode; path and secret values intentionally omitted.",
    }
    if not env_path.exists():
        return [], summary

    parsed: list[dict[str, Any]] = []
    for line in env_path.read_text(encoding="utf-8", errors="replace").splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#") or "=" not in stripped:
            continue
        key, raw_value = stripped.split("=", 1)
        key = key.strip()
        if not key or not TRADING_ENV_RE.search(key):
            continue
        summary["interesting_keys"] += 1
        sensitive = bool(SENSITIVE_ENV_RE.search(key))
        if sensitive:
            summary["redacted_keys"] += 1
            shown_value = "<redacted>"
        else:
            shown_value = clean_env_value(raw_value)
        parsed.append({"category": env_category(key), "key": key, "value": shown_value})

    priority = {
        "fibo": 0,
        "crypto": 1,
        "xau": 2,
        "risk-sizing": 3,
        "ctrader": 4,
        "lane-governance": 5,
        "gates": 6,
        "general": 7,
    }
    parsed.sort(key=lambda row: (priority.get(str(row["category"]), 99), str(row["key"])))
    shown = parsed[:max_rows]
    summary["shown_keys"] = len(shown)
    return shown, summary


def db_paths(dexter_root: Path) -> DatabasePaths:
    return DatabasePaths(
        ctrader=dexter_root / "data" / "ctrader_openapi.db",
        backtest=dexter_root / "backtest" / "backtest_results.db",
    )


def inventory(conn: sqlite3.Connection) -> list[dict[str, Any]]:
    important = [
        "ctrader_deals",
        "execution_journal",
        "xau_family_canary_gate_journal",
        "xau_shadow_journal",
    ]
    out: list[dict[str, Any]] = []
    for table in important:
        if table_exists(conn, table):
            count = one(conn, f"SELECT COUNT(*) AS n FROM {table}").get("n", 0)
            out.append({"table": table, "rows": count})
    return out


def overall_deals(conn: sqlite3.Connection) -> dict[str, Any]:
    return one(
        conn,
        """
        SELECT
            COUNT(*) AS trades,
            SUM(CASE WHEN pnl_usd > 0 THEN 1 ELSE 0 END) AS wins,
            SUM(CASE WHEN pnl_usd < 0 THEN 1 ELSE 0 END) AS losses,
            SUM(pnl_usd) AS pnl_usd,
            AVG(pnl_usd) AS avg_pnl_usd,
            SUM(CASE WHEN pnl_usd > 0 THEN 1 ELSE 0 END) * 1.0 /
                NULLIF(SUM(CASE WHEN pnl_usd != 0 THEN 1 ELSE 0 END), 0) AS win_rate,
            MIN(execution_utc) AS first_execution_utc,
            MAX(execution_utc) AS latest_execution_utc
        FROM ctrader_deals
        """,
    )


def lane_stats(conn: sqlite3.Connection, order: str, limit: int = 20) -> list[dict[str, Any]]:
    if order not in {"best", "worst"}:
        raise ValueError("order must be best or worst")
    direction = "DESC" if order == "best" else "ASC"
    return rows(
        conn,
        f"""
        SELECT
            COALESCE(NULLIF(source, ''), '(blank)') AS source,
            COALESCE(NULLIF(lane, ''), '(blank)') AS lane,
            UPPER(COALESCE(NULLIF(symbol, ''), '(blank)')) AS symbol,
            LOWER(COALESCE(NULLIF(direction, ''), '(blank)')) AS direction,
            COUNT(*) AS trades,
            SUM(CASE WHEN pnl_usd > 0 THEN 1 ELSE 0 END) AS wins,
            SUM(CASE WHEN pnl_usd < 0 THEN 1 ELSE 0 END) AS losses,
            SUM(pnl_usd) AS pnl_usd,
            AVG(pnl_usd) AS avg_pnl_usd,
            SUM(CASE WHEN pnl_usd > 0 THEN 1 ELSE 0 END) * 1.0 /
                NULLIF(SUM(CASE WHEN pnl_usd != 0 THEN 1 ELSE 0 END), 0) AS win_rate,
            AVG(volume) AS avg_volume,
            MIN(execution_utc) AS first_utc,
            MAX(execution_utc) AS last_utc
        FROM ctrader_deals
        WHERE COALESCE(symbol, '') != ''
        GROUP BY source, lane, symbol, direction
        HAVING COUNT(*) >= 10
        ORDER BY pnl_usd {direction}, trades DESC
        LIMIT ?
        """,
        (limit,),
    )


def fibo_stats(conn: sqlite3.Connection) -> list[dict[str, Any]]:
    return rows(
        conn,
        """
        SELECT
            COALESCE(NULLIF(source, ''), '(blank)') AS source,
            COALESCE(NULLIF(lane, ''), '(blank)') AS lane,
            UPPER(symbol) AS symbol,
            LOWER(direction) AS direction,
            COUNT(*) AS trades,
            SUM(CASE WHEN pnl_usd > 0 THEN 1 ELSE 0 END) AS wins,
            SUM(CASE WHEN pnl_usd < 0 THEN 1 ELSE 0 END) AS losses,
            SUM(pnl_usd) AS pnl_usd,
            AVG(pnl_usd) AS avg_pnl_usd,
            SUM(CASE WHEN pnl_usd > 0 THEN 1 ELSE 0 END) * 1.0 /
                NULLIF(SUM(CASE WHEN pnl_usd != 0 THEN 1 ELSE 0 END), 0) AS win_rate,
            AVG(volume) AS avg_volume,
            MIN(execution_utc) AS first_utc,
            MAX(execution_utc) AS last_utc
        FROM ctrader_deals
        WHERE LOWER(COALESCE(source, '') || ' ' || COALESCE(lane, '')) LIKE '%fibo%'
        GROUP BY source, lane, symbol, direction
        ORDER BY pnl_usd ASC
        """,
    )


def first_fibo_execution(conn: sqlite3.Connection) -> str | None:
    found = one(
        conn,
        """
        SELECT MIN(execution_utc) AS first_fibo_utc
        FROM ctrader_deals
        WHERE LOWER(COALESCE(source, '') || ' ' || COALESCE(lane, '')) LIKE '%fibo%'
        """,
    ).get("first_fibo_utc")
    return str(found) if found else None


def xau_before_after_fibo(conn: sqlite3.Connection, first_fibo_utc: str | None) -> list[dict[str, Any]]:
    if not first_fibo_utc:
        return []
    return rows(
        conn,
        """
        SELECT
            CASE WHEN execution_utc < ? THEN 'before_first_fibo_deal' ELSE 'after_first_fibo_deal' END AS period,
            LOWER(direction) AS direction,
            COUNT(*) AS trades,
            SUM(CASE WHEN pnl_usd > 0 THEN 1 ELSE 0 END) AS wins,
            SUM(CASE WHEN pnl_usd < 0 THEN 1 ELSE 0 END) AS losses,
            SUM(pnl_usd) AS pnl_usd,
            AVG(pnl_usd) AS avg_pnl_usd,
            SUM(CASE WHEN pnl_usd > 0 THEN 1 ELSE 0 END) * 1.0 /
                NULLIF(SUM(CASE WHEN pnl_usd != 0 THEN 1 ELSE 0 END), 0) AS win_rate
        FROM ctrader_deals
        WHERE UPPER(symbol) = 'XAUUSD'
        GROUP BY period, direction
        ORDER BY period, direction
        """,
        (first_fibo_utc,),
    )


def btc_volume_by_lane(conn: sqlite3.Connection) -> list[dict[str, Any]]:
    return rows(
        conn,
        """
        SELECT
            COALESCE(NULLIF(source, ''), '(blank)') AS source,
            COALESCE(NULLIF(lane, ''), '(blank)') AS lane,
            LOWER(COALESCE(NULLIF(direction, ''), '(blank)')) AS direction,
            COUNT(*) AS trades,
            MIN(volume) AS min_volume,
            AVG(volume) AS avg_volume,
            MAX(volume) AS max_volume,
            SUM(pnl_usd) AS pnl_usd,
            AVG(pnl_usd) AS avg_pnl_usd,
            SUM(CASE WHEN pnl_usd > 0 THEN 1 ELSE 0 END) * 1.0 /
                NULLIF(SUM(CASE WHEN pnl_usd != 0 THEN 1 ELSE 0 END), 0) AS win_rate,
            MIN(execution_utc) AS first_utc,
            MAX(execution_utc) AS last_utc
        FROM ctrader_deals
        WHERE UPPER(symbol) = 'BTCUSD'
        GROUP BY source, lane, direction
        ORDER BY pnl_usd DESC, trades DESC
        """,
    )


def btc_journal_volume_by_day(conn: sqlite3.Connection) -> list[dict[str, Any]]:
    return rows(
        conn,
        """
        SELECT
            SUBSTR(created_utc, 1, 10) AS day_utc,
            COALESCE(NULLIF(source, ''), '(blank)') AS source,
            COALESCE(NULLIF(lane, ''), '(blank)') AS lane,
            LOWER(COALESCE(NULLIF(direction, ''), '(blank)')) AS direction,
            COALESCE(NULLIF(status, ''), '(blank)') AS status,
            COUNT(*) AS rows,
            MIN(volume) AS min_volume,
            AVG(volume) AS avg_volume,
            MAX(volume) AS max_volume,
            AVG(confidence) AS avg_confidence
        FROM execution_journal
        WHERE UPPER(symbol) = 'BTCUSD'
        GROUP BY day_utc, source, lane, direction, status
        ORDER BY day_utc DESC, rows DESC
        LIMIT 30
        """,
    )


def parse_json_obj(raw: Any) -> dict[str, Any]:
    try:
        data = json.loads(str(raw or "{}"))
    except Exception:
        return {}
    return data if isinstance(data, dict) else {}


def btc_sizing_diagnostics(conn: sqlite3.Connection, limit: int = 30) -> list[dict[str, Any]]:
    diagnostics: list[dict[str, Any]] = []
    recent = rows(
        conn,
        """
        SELECT
            id,
            created_utc,
            COALESCE(NULLIF(source, ''), '(blank)') AS source,
            COALESCE(NULLIF(lane, ''), '(blank)') AS lane,
            LOWER(COALESCE(NULLIF(direction, ''), '(blank)')) AS direction,
            volume,
            status,
            message,
            request_json,
            execution_meta_json
        FROM execution_journal
        WHERE UPPER(symbol) = 'BTCUSD'
        ORDER BY id DESC
        LIMIT ?
        """,
        (limit,),
    )
    for row in recent:
        req = parse_json_obj(row.get("request_json"))
        raw_scores = req.get("raw_scores") if isinstance(req.get("raw_scores"), dict) else {}
        meta = parse_json_obj(row.get("execution_meta_json"))
        volume_meta = meta.get("volume_meta")
        if not isinstance(volume_meta, dict):
            nested = meta.get("execution_meta") if isinstance(meta.get("execution_meta"), dict) else {}
            volume_meta = nested.get("volume_meta") if isinstance(nested.get("volume_meta"), dict) else {}
        diagnostics.append(
            {
                "created_utc": row.get("created_utc"),
                "source": row.get("source"),
                "lane": row.get("lane"),
                "direction": row.get("direction"),
                "status": row.get("status"),
                "journal_volume": row.get("volume"),
                "payload_risk_usd": req.get("risk_usd"),
                "payload_fixed_volume": req.get("fixed_volume"),
                "raw_risk_override": raw_scores.get("ctrader_risk_usd_override"),
                "volume_reason": volume_meta.get("reason"),
                "risk_price": volume_meta.get("risk_price"),
                "raw_volume": volume_meta.get("raw_volume"),
                "min_volume": volume_meta.get("min_volume"),
                "step_volume": volume_meta.get("step_volume"),
                "message": str(row.get("message") or "")[:90],
            }
        )
    return diagnostics


def execution_status(conn: sqlite3.Connection) -> list[dict[str, Any]]:
    return rows(
        conn,
        """
        SELECT
            COALESCE(NULLIF(status, ''), '(blank)') AS status,
            COALESCE(NULLIF(lane, ''), '(blank)') AS lane,
            COUNT(*) AS rows,
            AVG(confidence) AS avg_confidence,
            MIN(created_utc) AS first_utc,
            MAX(created_utc) AS last_utc
        FROM execution_journal
        GROUP BY status, lane
        ORDER BY rows DESC
        LIMIT 25
        """,
    )


def top_filter_reasons(conn: sqlite3.Connection) -> list[dict[str, Any]]:
    return rows(
        conn,
        """
        SELECT
            UPPER(COALESCE(NULLIF(symbol, ''), '(blank)')) AS symbol,
            LOWER(COALESCE(NULLIF(direction, ''), '(blank)')) AS direction,
            COALESCE(NULLIF(message, ''), '(blank)') AS reason,
            COUNT(*) AS rows,
            AVG(confidence) AS avg_confidence,
            MIN(created_utc) AS first_utc,
            MAX(created_utc) AS last_utc
        FROM execution_journal
        WHERE LOWER(COALESCE(status, '')) = 'filtered'
        GROUP BY symbol, direction, message
        ORDER BY rows DESC
        LIMIT 20
        """,
    )


def canary_gate_reasons(conn: sqlite3.Connection) -> list[dict[str, Any]]:
    if not table_exists(conn, "xau_family_canary_gate_journal"):
        return []
    return rows(
        conn,
        """
        SELECT
            COALESCE(NULLIF(family, ''), '(blank)') AS family,
            COALESCE(NULLIF(lane_source, ''), '(blank)') AS lane_source,
            COALESCE(NULLIF(gate_stage, ''), '(blank)') AS gate_stage,
            COALESCE(NULLIF(reason, ''), '(blank)') AS reason,
            LOWER(COALESCE(NULLIF(direction, ''), '(blank)')) AS direction,
            COUNT(*) AS rows,
            AVG(confidence) AS avg_confidence,
            AVG(neural_probability) AS avg_neural_probability,
            MIN(signal_utc) AS first_utc,
            MAX(signal_utc) AS last_utc
        FROM xau_family_canary_gate_journal
        GROUP BY family, lane_source, gate_stage, reason, direction
        ORDER BY rows DESC
        LIMIT 25
        """,
    )


def shadow_health(conn: sqlite3.Connection) -> dict[str, Any]:
    if not table_exists(conn, "xau_shadow_journal"):
        return {}
    return one(
        conn,
        """
        SELECT
            COUNT(*) AS rows,
            SUM(CASE WHEN resolved_utc IS NOT NULL AND resolved_utc != '' THEN 1 ELSE 0 END) AS resolved_rows,
            SUM(CASE WHEN shadow_outcome IS NOT NULL AND shadow_outcome != '' THEN 1 ELSE 0 END) AS outcome_rows,
            SUM(CASE WHEN shadow_pnl_rr IS NOT NULL THEN 1 ELSE 0 END) AS pnl_rows,
            AVG(shadow_pnl_rr) AS avg_pnl_rr,
            SUM(CASE WHEN shadow_pnl_rr > 0 THEN 1 ELSE 0 END) AS positive_rr,
            SUM(CASE WHEN shadow_pnl_rr < 0 THEN 1 ELSE 0 END) AS negative_rr,
            MIN(signal_utc) AS first_signal_utc,
            MAX(signal_utc) AS last_signal_utc
        FROM xau_shadow_journal
        """,
    )


def shadow_by_reason(conn: sqlite3.Connection) -> list[dict[str, Any]]:
    if not table_exists(conn, "xau_shadow_journal"):
        return []
    return rows(
        conn,
        """
        SELECT
            COALESCE(NULLIF(block_reason, ''), '(blank)') AS block_reason,
            LOWER(direction) AS direction,
            COUNT(*) AS rows,
            AVG(confidence) AS avg_confidence,
            AVG(shadow_pnl_rr) AS avg_pnl_rr,
            SUM(CASE WHEN shadow_pnl_rr > 0 THEN 1 ELSE 0 END) AS positive_rr,
            SUM(CASE WHEN shadow_pnl_rr < 0 THEN 1 ELSE 0 END) AS negative_rr
        FROM xau_shadow_journal
        GROUP BY block_reason, direction
        ORDER BY rows DESC
        LIMIT 20
        """,
    )


def top_backtests(conn: sqlite3.Connection) -> list[dict[str, Any]]:
    if not table_exists(conn, "backtest_runs"):
        return []
    return rows(
        conn,
        """
        SELECT
            run_name,
            COALESCE(NULLIF(strategy, ''), '(blank)') AS strategy,
            total_trades,
            win_rate,
            total_pnl_r,
            max_drawdown,
            profit_factor,
            start_date,
            end_date,
            created_at
        FROM backtest_runs
        WHERE total_trades >= 20
        ORDER BY profit_factor DESC, total_pnl_r DESC, total_trades DESC
        LIMIT 20
        """,
    )


def recommendations(best: list[dict[str, Any]], worst: list[dict[str, Any]], fibo: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []

    for row in best:
        trades = int(row.get("trades") or 0)
        pnl = float(row.get("pnl_usd") or 0)
        win = row.get("win_rate")
        win_rate = float(win) if win is not None else 0.0
        if trades >= 20 and pnl >= 20 and win_rate >= 0.58:
            out.append(
                {
                    "action": "PROTECT / candidate promote",
                    "source": row["source"],
                    "lane": row["lane"],
                    "symbol": row["symbol"],
                    "direction": row["direction"],
                    "evidence": f"{trades} trades, pnl {pnl:.2f}, win {win_rate * 100:.1f}%",
                    "safe_next_step": "Mempalace daily report only; do not auto-edit Dexter policy.",
                }
            )

    for row in worst:
        trades = int(row.get("trades") or 0)
        pnl = float(row.get("pnl_usd") or 0)
        avg = float(row.get("avg_pnl_usd") or 0)
        if trades >= 20 and pnl <= -20 and avg < 0:
            out.append(
                {
                    "action": "QUARANTINE candidate",
                    "source": row["source"],
                    "lane": row["lane"],
                    "symbol": row["symbol"],
                    "direction": row["direction"],
                    "evidence": f"{trades} trades, pnl {pnl:.2f}, avg {avg:.2f}",
                    "safe_next_step": "Require human approval before any Dexter gate/config patch.",
                }
            )

    for row in fibo:
        trades = int(row.get("trades") or 0)
        pnl = float(row.get("pnl_usd") or 0)
        direction = str(row.get("direction") or "")
        if trades >= 10 and pnl < 0 and direction == "short":
            out.insert(
                0,
                {
                    "action": "HIGH PRIORITY QUARANTINE",
                    "source": row["source"],
                    "lane": row["lane"],
                    "symbol": row["symbol"],
                    "direction": row["direction"],
                    "evidence": f"Fibo short asymmetry: {trades} trades, pnl {pnl:.2f}",
                    "safe_next_step": "Disable or hard-gate only after user approval; preserve fibo long until separately disproven.",
                },
            )

    seen: set[tuple[str, str, str, str, str]] = set()
    deduped: list[dict[str, Any]] = []
    for item in out:
        key = (
            item["action"],
            item["source"],
            item["lane"],
            item["symbol"],
            item["direction"],
        )
        if key not in seen:
            seen.add(key)
            deduped.append(item)
    return deduped[:30]


def build_report(
    dexter_root: Path,
    paths: DatabasePaths,
    trade_export: Path | None = None,
    include_env: bool = True,
    max_env_keys: int = 260,
) -> str:
    generated = datetime.now(UTC).strftime("%Y-%m-%d %H:%M:%S UTC")
    export_summary = export_trade_summary(trade_export) if trade_export else None
    timeline = git_timeline(dexter_root)
    env_rows: list[dict[str, Any]] = []
    env_summary: dict[str, Any] = {}
    if include_env:
        env_rows, env_summary = sanitized_env_snapshot(dexter_root, max_rows=max_env_keys)

    with connect_readonly(paths.ctrader) as cconn:
        inv = inventory(cconn)
        overall = overall_deals(cconn)
        best = lane_stats(cconn, "best")
        worst = lane_stats(cconn, "worst")
        fibo = fibo_stats(cconn)
        first_fibo = first_fibo_execution(cconn)
        xau_periods = xau_before_after_fibo(cconn, first_fibo)
        btc_lanes = btc_volume_by_lane(cconn)
        btc_daily = btc_journal_volume_by_day(cconn)
        btc_sizing = btc_sizing_diagnostics(cconn)
        statuses = execution_status(cconn)
        filters = top_filter_reasons(cconn)
        gates = canary_gate_reasons(cconn)
        shadow = shadow_health(cconn)
        shadow_reasons = shadow_by_reason(cconn)
        recs = recommendations(best, worst, fibo)

    backtests: list[dict[str, Any]] = []
    if paths.backtest.exists():
        with connect_readonly(paths.backtest) as bconn:
            backtests = top_backtests(bconn)

    sections: list[str] = []
    sections.append("# Dexter Edge Audit\n")
    sections.append(
        "This report is generated by Mempalace in read-only mode. "
        "It reads aggregate evidence from Dexter SQLite databases, optionally reads "
        "a sanitized `.env.local` trading-parameter snapshot, and optionally cross-checks "
        "a cTrader XLSX trade export. It redacts credential-like values, omits account IDs, "
        "does not store raw API payloads, and does not write to Dexter.\n"
    )
    sections.append(f"- Generated: `{generated}`")
    sections.append(f"- Dexter root scanned: `{dexter_root}`")
    sections.append(f"- cTrader DB: `{paths.ctrader}`")
    sections.append(f"- Backtest DB: `{paths.backtest}`")
    sections.append(f"- External cTrader export included: `{'yes' if export_summary else 'no'}`")
    sections.append(f"- Sanitized Dexter env included: `{'yes' if include_env else 'no'}`")
    sections.append("- Safety stance: Mempalace can recommend promote/demote/quarantine actions, but should not auto-patch Dexter live policy without human approval.\n")

    if env_summary:
        sections.append("## Sanitized Dexter Trading Config Snapshot\n")
        sections.append(md_table([env_summary], [
            ("env_present", "Env Present", ""),
            ("interesting_keys", "Trading Keys Seen", ""),
            ("redacted_keys", "Redacted Keys", ""),
            ("shown_keys", "Shown Keys", ""),
            ("path_note", "Note", ""),
        ]))
        sections.append(
            "This table is for explaining behavior such as BTC sizing, XAU gates, fibo filters, "
            "canary policy, and live/demo routing. Secret-like values are never shown.\n"
        )
        sections.append(md_table(env_rows, [
            ("category", "Category", ""),
            ("key", "Key", ""),
            ("value", "Sanitized Value", ""),
        ]))

    sections.append("## Dexter Recent Code Timeline\n")
    sections.append(
        "This is correlation evidence only. A trade export without strategy/order IDs cannot prove "
        "a commit caused a win or loss; it can only highlight time windows that deserve code review.\n"
    )
    sections.append(md_table(timeline, [
        ("commit", "Commit", ""),
        ("commit_time", "Commit Time", ""),
        ("area", "Area", ""),
        ("subject", "Subject", ""),
    ]))

    if export_summary:
        summary = dict(export_summary["summary"])
        for key in ("first_close", "last_close"):
            if summary.get(key):
                summary[key] = summary[key].strftime("%Y-%m-%d %H:%M:%S")
        summary["max_drawdown_usd"] = export_summary["max_drawdown_usd"]

        sections.append("## External cTrader Export Cross-Check\n")
        sections.append(
            "The export validates broker-side closed-trade results. It does not contain Dexter source/lane labels, "
            "so use it to verify symbol/direction/day performance and then correlate with Dexter journals.\n"
        )
        sections.append(md_table([summary], [
            ("rows", "Rows", ""),
            ("wins", "Wins", ""),
            ("losses", "Losses", ""),
            ("pnl_usd", "PnL USD", "usd"),
            ("avg_pnl_usd", "Avg PnL", "usd"),
            ("win_rate", "Win Rate", "pct"),
            ("max_drawdown_usd", "Max DD USD", "usd"),
            ("first_close", "First Close", ""),
            ("last_close", "Last Close", ""),
        ]))
        sections.append("### Export By Symbol And Direction\n")
        sections.append(md_table(export_summary["by_symbol_direction"], [
            ("symbol", "Symbol", ""),
            ("direction", "Direction", ""),
            ("trades", "Trades", ""),
            ("wins", "Wins", ""),
            ("losses", "Losses", ""),
            ("pnl_usd", "PnL USD", "usd"),
            ("avg_pnl_usd", "Avg PnL", "usd"),
            ("win_rate", "Win Rate", "pct"),
            ("min_quantity", "Min Qty", ""),
            ("avg_quantity", "Avg Qty", ""),
            ("max_quantity", "Max Qty", ""),
        ]))
        sections.append("### Export By Day\n")
        sections.append(md_table(export_summary["by_day"], [
            ("period", "Day", ""),
            ("trades", "Trades", ""),
            ("wins", "Wins", ""),
            ("losses", "Losses", ""),
            ("pnl_usd", "PnL USD", "usd"),
            ("avg_pnl_usd", "Avg PnL", "usd"),
            ("win_rate", "Win Rate", "pct"),
        ]))
        sections.append("### Worst Export Hours\n")
        sections.append(md_table(export_summary["by_hour_worst"], [
            ("period", "Hour", ""),
            ("trades", "Trades", ""),
            ("wins", "Wins", ""),
            ("losses", "Losses", ""),
            ("pnl_usd", "PnL USD", "usd"),
            ("avg_pnl_usd", "Avg PnL", "usd"),
            ("win_rate", "Win Rate", "pct"),
        ]))
        sections.append("### Best Export Hours\n")
        sections.append(md_table(export_summary["by_hour_best"], [
            ("period", "Hour", ""),
            ("trades", "Trades", ""),
            ("wins", "Wins", ""),
            ("losses", "Losses", ""),
            ("pnl_usd", "PnL USD", "usd"),
            ("avg_pnl_usd", "Avg PnL", "usd"),
            ("win_rate", "Win Rate", "pct"),
        ]))
        sections.append(
            "Initial use: treat symbol/direction export rows as broker-truth labels, then use Dexter DB lanes "
            "below to identify which strategy family likely produced each cluster. If export and DB disagree by "
            "direction, do not patch blindly; first reconcile account, source, and journal linkage.\n"
        )

    sections.append("## Database Inventory\n")
    sections.append(md_table(inv, [("table", "Table", ""), ("rows", "Rows", "")]))

    sections.append("## Overall Live Deal Evidence\n")
    sections.append(md_table([overall], [
        ("trades", "Trades", ""),
        ("wins", "Wins", ""),
        ("losses", "Losses", ""),
        ("pnl_usd", "PnL USD", "usd"),
        ("avg_pnl_usd", "Avg PnL", "usd"),
        ("win_rate", "Win Rate", "pct"),
        ("first_execution_utc", "First UTC", ""),
        ("latest_execution_utc", "Latest UTC", ""),
    ]))
    sections.append(
        "Interpretation: the whole Dexter book should not be treated as one edge. "
        "The evidence is lane-specific and direction-specific, so broad changes risk damaging good lanes while hiding bad ones.\n"
    )

    sections.append("## Winner Lanes To Protect\n")
    sections.append(md_table(best, [
        ("source", "Source", ""),
        ("lane", "Lane", ""),
        ("symbol", "Symbol", ""),
        ("direction", "Direction", ""),
        ("trades", "Trades", ""),
        ("pnl_usd", "PnL USD", "usd"),
        ("avg_pnl_usd", "Avg PnL", "usd"),
        ("win_rate", "Win Rate", "pct"),
        ("avg_volume", "Avg Volume", ""),
        ("last_utc", "Last UTC", ""),
    ]))

    sections.append("## Danger Lanes To Quarantine Or Review\n")
    sections.append(md_table(worst, [
        ("source", "Source", ""),
        ("lane", "Lane", ""),
        ("symbol", "Symbol", ""),
        ("direction", "Direction", ""),
        ("trades", "Trades", ""),
        ("pnl_usd", "PnL USD", "usd"),
        ("avg_pnl_usd", "Avg PnL", "usd"),
        ("win_rate", "Win Rate", "pct"),
        ("avg_volume", "Avg Volume", ""),
        ("last_utc", "Last UTC", ""),
    ]))

    sections.append("## Fibo Family Regression Check\n")
    sections.append(f"- First detected fibo deal UTC: `{first_fibo or 'not found'}`\n")
    sections.append(md_table(fibo, [
        ("source", "Source", ""),
        ("lane", "Lane", ""),
        ("symbol", "Symbol", ""),
        ("direction", "Direction", ""),
        ("trades", "Trades", ""),
        ("wins", "Wins", ""),
        ("losses", "Losses", ""),
        ("pnl_usd", "PnL USD", "usd"),
        ("avg_pnl_usd", "Avg PnL", "usd"),
        ("win_rate", "Win Rate", "pct"),
        ("avg_volume", "Avg Volume", ""),
        ("last_utc", "Last UTC", ""),
    ]))
    sections.append("### XAUUSD Before / After First Fibo Deal\n")
    sections.append(md_table(xau_periods, [
        ("period", "Period", ""),
        ("direction", "Direction", ""),
        ("trades", "Trades", ""),
        ("wins", "Wins", ""),
        ("losses", "Losses", ""),
        ("pnl_usd", "PnL USD", "usd"),
        ("avg_pnl_usd", "Avg PnL", "usd"),
        ("win_rate", "Win Rate", "pct"),
    ]))

    sections.append("## BTCUSD Lot / Volume Evidence\n")
    sections.append(
        "Volume is reported exactly as stored by Dexter. If the broker/executor stores lots directly, "
        "`0.05` means 0.05 lot; if it stores units, convert with Dexter's symbol rules before changing risk.\n"
    )
    sections.append(md_table(btc_lanes, [
        ("source", "Source", ""),
        ("lane", "Lane", ""),
        ("direction", "Direction", ""),
        ("trades", "Trades", ""),
        ("min_volume", "Min Volume", ""),
        ("avg_volume", "Avg Volume", ""),
        ("max_volume", "Max Volume", ""),
        ("pnl_usd", "PnL USD", "usd"),
        ("avg_pnl_usd", "Avg PnL", "usd"),
        ("win_rate", "Win Rate", "pct"),
        ("last_utc", "Last UTC", ""),
    ]))
    sections.append("### Recent BTCUSD Journal Volume By Day\n")
    sections.append(md_table(btc_daily, [
        ("day_utc", "Day UTC", ""),
        ("source", "Source", ""),
        ("lane", "Lane", ""),
        ("direction", "Direction", ""),
        ("status", "Status", ""),
        ("rows", "Rows", ""),
        ("min_volume", "Min Volume", ""),
        ("avg_volume", "Avg Volume", ""),
        ("max_volume", "Max Volume", ""),
        ("avg_confidence", "Avg Confidence", ""),
    ]))
    sections.append("### Recent BTCUSD Sizing Diagnostics\n")
    sections.append(
        "Key rule observed in Dexter worker: `fixed_volume` wins over risk-based sizing. "
        "When `payload_fixed_volume=5`, the worker sends volume 5 even if `payload_risk_usd` is lower. "
        "In cTrader export this commonly appears as quantity 0.05 for BTCUSD.\n"
    )
    sections.append(md_table(btc_sizing, [
        ("created_utc", "Created UTC", ""),
        ("source", "Source", ""),
        ("lane", "Lane", ""),
        ("direction", "Direction", ""),
        ("status", "Status", ""),
        ("journal_volume", "Journal Volume", ""),
        ("payload_fixed_volume", "Payload Fixed Volume", ""),
        ("payload_risk_usd", "Payload Risk USD", ""),
        ("raw_risk_override", "Raw Risk Override", ""),
        ("volume_reason", "Volume Reason", ""),
        ("risk_price", "Risk Price", ""),
        ("raw_volume", "Raw Volume", ""),
        ("message", "Message", ""),
    ]))

    sections.append("## Execution Status And Gate Reasons\n")
    sections.append(md_table(statuses, [
        ("status", "Status", ""),
        ("lane", "Lane", ""),
        ("rows", "Rows", ""),
        ("avg_confidence", "Avg Confidence", ""),
        ("first_utc", "First UTC", ""),
        ("last_utc", "Last UTC", ""),
    ]))
    sections.append("### Top Filter Reasons\n")
    sections.append(md_table(filters, [
        ("symbol", "Symbol", ""),
        ("direction", "Direction", ""),
        ("reason", "Reason", ""),
        ("rows", "Rows", ""),
        ("avg_confidence", "Avg Confidence", ""),
        ("last_utc", "Last UTC", ""),
    ]))
    sections.append("### XAU Family Canary Gate Journal\n")
    sections.append(md_table(gates, [
        ("family", "Family", ""),
        ("lane_source", "Lane Source", ""),
        ("gate_stage", "Gate Stage", ""),
        ("reason", "Reason", ""),
        ("direction", "Direction", ""),
        ("rows", "Rows", ""),
        ("avg_confidence", "Avg Confidence", ""),
        ("avg_neural_probability", "Avg Neural Probability", ""),
        ("last_utc", "Last UTC", ""),
    ]))

    sections.append("## Shadow Journal Health\n")
    sections.append(md_table([shadow], [
        ("rows", "Rows", ""),
        ("resolved_rows", "Resolved Rows", ""),
        ("outcome_rows", "Outcome Rows", ""),
        ("pnl_rows", "PnL Rows", ""),
        ("avg_pnl_rr", "Avg PnL RR", ""),
        ("positive_rr", "Positive RR", ""),
        ("negative_rr", "Negative RR", ""),
        ("first_signal_utc", "First Signal UTC", ""),
        ("last_signal_utc", "Last Signal UTC", ""),
    ]))
    sections.append(md_table(shadow_reasons, [
        ("block_reason", "Block Reason", ""),
        ("direction", "Direction", ""),
        ("rows", "Rows", ""),
        ("avg_confidence", "Avg Confidence", ""),
        ("avg_pnl_rr", "Avg PnL RR", ""),
        ("positive_rr", "Positive RR", ""),
        ("negative_rr", "Negative RR", ""),
    ]))

    sections.append("## Backtest Leaders\n")
    sections.append(md_table(backtests, [
        ("run_name", "Run Name", ""),
        ("strategy", "Strategy", ""),
        ("total_trades", "Trades", ""),
        ("win_rate", "Win Rate", ""),
        ("total_pnl_r", "PnL R", ""),
        ("max_drawdown", "Max DD", ""),
        ("profit_factor", "PF", ""),
        ("start_date", "Start", ""),
        ("end_date", "End", ""),
        ("created_at", "Created", ""),
    ]))

    sections.append("## Mempalace Recommendations\n")
    sections.append(md_table(recs, [
        ("action", "Action", ""),
        ("source", "Source", ""),
        ("lane", "Lane", ""),
        ("symbol", "Symbol", ""),
        ("direction", "Direction", ""),
        ("evidence", "Evidence", ""),
        ("safe_next_step", "Safe Next Step", ""),
    ]))
    sections.append(
        "Recommended integration model: keep Dexter as the executor and proven strategy host; "
        "use Mempalace as a read-only memory, evidence, and governance layer. Mempalace should create "
        "`winner rooms`, `danger rooms`, confidence-calibration notes, and daily promote/demote proposals. "
        "Dexter live gates should only change after explicit human approval and a rollback note.\n"
    )

    return "\n".join(sections).rstrip() + "\n"


def write_report(
    dexter_root: Path,
    output: Path,
    trade_export: Path | None = None,
    include_env: bool = True,
    max_env_keys: int = 260,
) -> Path:
    paths = db_paths(dexter_root)
    report = build_report(
        dexter_root,
        paths,
        trade_export=trade_export,
        include_env=include_env,
        max_env_keys=max_env_keys,
    )
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(report, encoding="utf-8", newline="\n")
    return output


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate a read-only Dexter edge audit report.")
    parser.add_argument("--dexter-root", type=Path, default=DEFAULT_DEXTER_ROOT)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--trade-export", type=Path, default=None, help="Optional cTrader XLSX trade export.")
    parser.add_argument(
        "--no-env",
        action="store_true",
        help="Do not read Dexter .env.local. By default a sanitized config snapshot is included.",
    )
    parser.add_argument("--max-env-keys", type=int, default=260)
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> None:
    args = parse_args(argv)
    path = write_report(
        args.dexter_root,
        args.output,
        trade_export=args.trade_export,
        include_env=not args.no_env,
        max_env_keys=args.max_env_keys,
    )
    print(f"Wrote read-only Dexter edge audit: {path}")


if __name__ == "__main__":
    main()
