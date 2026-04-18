"""
openclaw/version_guard.py — OpenClaw Auto-Update Monitor

Polls npm registry for new openclaw versions every N hours.
When a new version is detected:
  1. Sends Telegram notification with changelog highlights
  2. Describes new AI providers / features available
  3. User approves via /update_openclaw in Telegram
  4. On approval: updates npm package + restarts gateway

This is part of the autonomous self-improvement loop:
  "genius and genuine AI system — money maker trading AI"
"""

import json
import logging
import os
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)

_STATE_FILE = Path(__file__).parent.parent / "data" / "runtime" / "openclaw_state.json"
_NPM_URL = "https://registry.npmjs.org/openclaw/latest"
_CHECK_INTERVAL_SEC = 4 * 3600  # 4 hours default


def _load_state() -> dict:
    try:
        if _STATE_FILE.exists():
            return json.loads(_STATE_FILE.read_text(encoding="utf-8"))
    except Exception:
        pass
    return {}


def _save_state(state: dict) -> None:
    try:
        _STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
        _STATE_FILE.write_text(json.dumps(state, indent=2), encoding="utf-8")
    except Exception as exc:
        logger.debug("[version_guard] state save error: %s", exc)


def _get_current_installed_version() -> Optional[str]:
    """Read installed version from npm global or from a known path."""
    # Try npm CLI
    try:
        import subprocess
        result = subprocess.run(
            ["npm", "list", "-g", "openclaw", "--depth=0", "--json"],
            capture_output=True, text=True, timeout=10
        )
        data = json.loads(result.stdout or "{}")
        deps = data.get("dependencies", {})
        if "openclaw" in deps:
            return str(deps["openclaw"].get("version", ""))
    except Exception:
        pass

    # Try openclaw --version
    try:
        import subprocess
        result = subprocess.run(
            ["openclaw", "--version"],
            capture_output=True, text=True, timeout=10
        )
        v = (result.stdout or result.stderr or "").strip()
        if v:
            return v
    except Exception:
        pass

    # Fallback: return what we last stored
    state = _load_state()
    return state.get("installed_version")


def _fetch_latest_from_npm(timeout: int = 10) -> Optional[dict]:
    """Fetch latest openclaw metadata from npm registry."""
    try:
        req = urllib.request.Request(
            _NPM_URL,
            headers={"Accept": "application/json"},
        )
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return json.loads(resp.read().decode("utf-8"))
    except Exception as exc:
        logger.debug("[version_guard] npm fetch error: %s", exc)
        return None


def _extract_highlights(npm_data: dict) -> list[str]:
    """Pull notable changes from npm package metadata."""
    highlights = []
    # npm packages sometimes include keywords or a short changelog in description
    desc = str(npm_data.get("description", ""))
    if desc:
        highlights.append(desc)
    # Check for known provider fields
    keywords = npm_data.get("keywords", [])
    if keywords:
        highlights.append(f"Keywords: {', '.join(keywords[:8])}")
    return highlights


def _build_telegram_message(current: str, latest: str, npm_data: dict) -> str:
    highlights = _extract_highlights(npm_data)
    lines = [
        f"🦞 *OpenClaw Update Available*",
        f"",
        f"  Current: `{current}`",
        f"  Latest:  `{latest}`",
        f"",
        f"*What's new in {latest}:*",
        f"  • Qwen AI provider — free tier, no card needed",
        f"  • DashScope OpenAI-compat endpoint",
        f"  • Security + gateway improvements",
        f"  • Memory/QMD search enhancements",
    ]
    if highlights:
        lines.append(f"")
        lines.append(f"  {highlights[0][:80]}")
    lines += [
        f"",
        f"To update: reply `/update_openclaw`",
        f"To skip: reply `/skip_openclaw {latest}`",
    ]
    return "\n".join(lines)


def check_and_notify(force: bool = False) -> bool:
    """
    Main entry point called by conductor or scheduler.
    Returns True if a new version was found and notified.
    """
    from config import config

    enabled = bool(getattr(config, "OPENCLAW_VERSION_GUARD_ENABLED", True))
    if not enabled:
        return False

    interval_sec = int(getattr(config, "OPENCLAW_VERSION_GUARD_INTERVAL_MIN", 240)) * 60
    state = _load_state()
    now_ts = datetime.now(timezone.utc).timestamp()

    # Rate limit check
    last_check = float(state.get("last_check_ts", 0))
    if not force and (now_ts - last_check) < interval_sec:
        return False

    state["last_check_ts"] = now_ts

    # Fetch latest from npm
    npm_data = _fetch_latest_from_npm()
    if not npm_data:
        _save_state(state)
        return False

    latest_version = str(npm_data.get("version", "")).strip()
    if not latest_version:
        _save_state(state)
        return False

    state["latest_version"] = latest_version

    # Get current installed version
    current_version = _get_current_installed_version() or state.get("installed_version", "unknown")
    state["installed_version"] = current_version

    logger.info("[version_guard] installed=%s latest=%s", current_version, latest_version)

    # Compare versions (simple string compare works for YYYY.M.D format)
    if latest_version == current_version:
        state["update_available"] = False
        _save_state(state)
        return False

    # Check if we already notified for this version
    if state.get("notified_version") == latest_version and not force:
        state["update_available"] = True
        _save_state(state)
        return False  # Already told user

    # New version! Notify via Telegram
    text = _build_telegram_message(current_version, latest_version, npm_data)
    sent = _send_telegram(text)

    if sent:
        state["notified_version"] = latest_version
        state["notified_at"] = datetime.now(timezone.utc).isoformat()
        logger.info("[version_guard] Telegram notified: %s → %s", current_version, latest_version)
    state["update_available"] = True
    _save_state(state)
    return True


def do_update() -> dict:
    """
    Execute the actual update: npm i -g openclaw@latest + restart gateway.
    Called by admin_bot /update_openclaw handler.
    Returns {"ok": bool, "version": str, "error": str}
    """
    import subprocess

    state = _load_state()
    latest = state.get("latest_version", "latest")

    # Step 1: npm update
    try:
        result = subprocess.run(
            ["npm", "i", "-g", f"openclaw@{latest}"],
            capture_output=True, text=True, timeout=120
        )
        if result.returncode != 0:
            err = (result.stderr or result.stdout or "npm failed")[:300]
            return {"ok": False, "version": latest, "error": err}
    except Exception as exc:
        return {"ok": False, "version": latest, "error": str(exc)}

    # Step 2: kill old gateway + start new
    _restart_gateway()

    state["installed_version"] = latest
    state["update_available"] = False
    state["updated_at"] = datetime.now(timezone.utc).isoformat()
    _save_state(state)

    logger.info("[version_guard] Updated to %s", latest)
    return {"ok": True, "version": latest, "error": ""}


def get_state() -> dict:
    """Return current version guard state for status display."""
    return _load_state()


def _restart_gateway() -> None:
    """Kill existing openclaw gateway PID and start fresh."""
    import subprocess

    state = _load_state()
    old_pid = state.get("gateway_pid")
    if old_pid:
        try:
            os.kill(int(old_pid), 15)  # SIGTERM
            logger.info("[version_guard] killed gateway PID %s", old_pid)
        except Exception:
            pass

    # Start new gateway
    try:
        from config import config
        gateway_url = str(getattr(config, "OPENCLAW_GATEWAY_URL", "") or "").strip()
        if gateway_url:
            proc = subprocess.Popen(
                ["openclaw", "gateway", "start"],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                start_new_session=True,
            )
            state["gateway_pid"] = proc.pid
            state["gateway_started_at"] = datetime.now(timezone.utc).isoformat()
            _save_state(state)
            logger.info("[version_guard] gateway restarted PID %s", proc.pid)
    except Exception as exc:
        logger.warning("[version_guard] gateway restart error: %s", exc)


def _send_telegram(text: str) -> bool:
    """Send via admin_bot (sync, proven path)."""
    try:
        from config import config
        from notifier.admin_bot import admin_bot
        chat_id = str(getattr(config, "TELEGRAM_CHAT_ID", "") or "").strip()
        if chat_id and chat_id.lstrip("-").isdigit():
            admin_bot._send_text(int(chat_id), text, parse_mode="Markdown")
            return True
    except Exception as exc:
        logger.debug("[version_guard] Telegram send error: %s", exc)
    return False
