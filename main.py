"""
main.py - Dexter Pro CLI Entry Point
Rich terminal UI with multiple modes:
  - monitor : Run 24/7 with background scanning + Telegram alerts
  - scan     : Run one-time scan (gold / crypto / all)
  - research : Ask the Claude AI brain a financial question
  - overview : Print current market overview
  - status   : Show system status
"""
import sys
import os
import argparse
import logging
import time
from datetime import datetime, timezone
from pathlib import Path

# Make console output resilient on Windows code pages (e.g., cp1252).
for _stream in (sys.stdout, sys.stderr):
    try:
        if hasattr(_stream, "reconfigure"):
            _stream.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from rich.console import Console
from rich.table import Table
from rich.panel import Panel
from rich.text import Text
from rich.live import Live
from rich import box

from config import config

console = Console()

BANNER = r"""
  ____            _             ____
 |  _ \  _____  _| |_ ___ _ __|  _ \ _ __ ___
 | | | |/ _ \ \/ / __/ _ \ '__| |_) | '__/ _ \
 | |_| |  __/>  <| ||  __/ |  |  __/| | | (_) |
 |____/ \___/_/\_\\__\___|_|  |_|   |_|  \___/

 🦞  AI Trading Agent  |  XAUUSD + Crypto Sniper
"""


def setup_logging():
    level = getattr(logging, config.LOG_LEVEL.upper(), logging.INFO)
    logging.basicConfig(
        level=level,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%H:%M:%S",
    )
    # Suppress noisy third-party loggers
    for lib in ("httpx", "httpcore", "telegram", "ccxt", "yfinance", "urllib3"):
        logging.getLogger(lib).setLevel(logging.WARNING)


def check_config():
    missing = config.validate()
    if missing:
        console.print(Panel(
            f"[yellow]⚠️  Missing configuration:[/]\n" +
            "\n".join(f"  • {m}" for m in missing) +
            "\n\n[dim]Copy .env.example to .env and fill in your keys.[/]",
            title="Configuration Warning",
            border_style="yellow",
        ))
    return missing


def _owner_chat_id() -> int | None:
    raw = str(getattr(config, "TELEGRAM_CHAT_ID", "") or "").strip()
    if raw and raw.lstrip("-").isdigit():
        return int(raw)
    return None


def _monitor_lock_path() -> Path:
    return Path(__file__).resolve().parent / "data" / "runtime" / "monitor.lock"


def _pid_running(pid: int) -> bool:
    pid_i = int(pid or 0)
    if pid_i <= 0:
        return False
    if os.name == "nt":
        try:
            import subprocess
            proc = subprocess.run(
                ["tasklist", "/FI", f"PID eq {pid_i}", "/FO", "CSV", "/NH"],
                capture_output=True,
                text=True,
                timeout=5,
                check=False,
            )
            out = str(proc.stdout or "").strip()
            if (not out) or ("no tasks are running" in out.lower()):
                return False
            first = out.splitlines()[0].strip().strip('"')
            if not first or first.lower().startswith("info:"):
                return False
            parts = [x.strip().strip('"') for x in out.splitlines()[0].split('","')]
            if len(parts) < 2:
                return False
            try:
                return int(parts[1]) == pid_i
            except Exception:
                return False
        except Exception:
            return False
    try:
        os.kill(pid_i, 0)
        return True
    except Exception:
        return False


def _acquire_monitor_lock() -> Path | None:
    path = _monitor_lock_path()
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
    except Exception:
        return None
    pid = int(os.getpid())
    payload = f"{pid}\n{int(time.time())}\n"
    for _ in range(2):
        try:
            fd = os.open(str(path), os.O_CREAT | os.O_EXCL | os.O_WRONLY)
            with os.fdopen(fd, "w", encoding="utf-8") as f:
                f.write(payload)
            return path
        except FileExistsError:
            existing_pid = None
            try:
                lines = path.read_text(encoding="utf-8", errors="ignore").splitlines()
                if lines:
                    existing_pid = int((lines[0] or "").strip())
            except Exception:
                existing_pid = None
            if existing_pid and existing_pid != pid and _pid_running(existing_pid):
                return None
            try:
                path.unlink(missing_ok=True)
            except Exception:
                return None
        except Exception:
            return None
    return None


def _release_monitor_lock(path: Path | None) -> None:
    if path is None:
        return
    try:
        path.unlink(missing_ok=True)
    except Exception:
        pass


# ─── Commands ─────────────────────────────────────────────────────────────────

def cmd_monitor():
    """Start 24/7 monitoring mode with background scanning."""
    from scheduler import scheduler
    from notifier.telegram_bot import notifier
    from notifier.admin_bot import admin_bot
    from notifier.billing_webhook import billing_webhook_server
    from learning.signal_simulator import signal_simulator

    lock_path = _acquire_monitor_lock()
    if lock_path is None:
        console.print("[yellow]⚠️ Monitor already running (lock active). Skip duplicate start.[/]")
        return

    console.print(Panel(BANNER, border_style="cyan", title="[bold cyan]DEXTER PRO[/]"))
    console.print("\n[green]Starting 24/7 monitor mode...[/]\n")

    missing = check_config()
    if not config.has_any_ai_key():
        console.print(
            "[red]Cannot start without an AI key "
            "(GROQ_API_KEY / GEMINI_API_KEY / GEMINI_VERTEX_AI_API_KEY / ANTHROPIC_API_KEY)[/]"
        )
        sys.exit(1)

    try:
        telegram_disabled = bool(getattr(config, "MONITOR_DISABLE_TELEGRAM", False))
        if telegram_disabled:
            notifier.enabled = False
            admin_bot.enabled = False
            console.print("[yellow]Telegram runtime disabled for this monitor session.[/]")
        else:
            notifier.send_startup_message()
        # ── cTrader OpenAPI token health check ──
        if bool(getattr(config, "CTRADER_ENABLED", False)):
            try:
                from api.ctrader_token_manager import token_manager
                health = token_manager.health_check()
                if "critical" in str(health.get("status", "")):
                    console.print(f"[red]🔴 cTrader Token: {health.get('message', 'CRITICAL')}[/]")
                elif "refreshed" in str(health.get("status", "")):
                    console.print(f"[green]✅ cTrader Token refreshed at startup[/]")
                else:
                    console.print(f"[green]✅ cTrader Token OK[/]")
            except Exception as e:
                console.print(f"[yellow]⚠️ cTrader token check skipped: {e}[/]")

        if bool(getattr(config, "SIM_ENABLED", True)):
            signal_simulator.start()
        # ── Hermes self-improving loop ──
        try:
            from learning.hermes_loop import improvement_loop
            improvement_loop.start(interval_sec=300)
            console.print("[green]✅ Hermes self-improving loop started (5-min cycle)[/]")
        except Exception as e:
            console.print(f"[yellow]⚠️ Hermes loop start skipped: {e}[/]")
        scheduler.start()
        if not telegram_disabled:
            admin_bot.start()
        if config.BILLING_ENABLED and config.BILLING_AUTOSTART_IN_MONITOR:
            started = billing_webhook_server.start()
            if started:
                console.print(
                    f"[green]✅ Billing webhook active[/] [dim]({config.BILLING_WEBHOOK_HOST}:{config.BILLING_WEBHOOK_PORT})[/]"
                )
            else:
                console.print("[yellow]⚠️ Billing webhook failed to start (check logs/port).[/]")

        # Start the Tiger Bridge API server for Web3 Dashboard
        try:
            import asyncio
            from api.bridge_server import bridge_server
            loop = asyncio.new_event_loop()
            loop.run_until_complete(bridge_server.start())
            import threading
            threading.Thread(target=loop.run_forever, daemon=True).start()
            console.print(
                f"[green]✅ Bridge API active[/] [dim](http://{bridge_server.host}:{bridge_server.port})[/]"
            )
        except Exception as e:
            console.print(f"[yellow]⚠️ Bridge API failed to start: {e}[/]")

        try:
            shock = scheduler._xau_event_shock_state()
            console.print(
                "[cyan]XAU shock guard:[/] "
                f"active={bool(shock.get('active', False))} "
                f"kill={bool(shock.get('kill_switch', False))} "
                f"reason={str(shock.get('reason', '-'))}"
            )
        except Exception:
            pass

        console.print("[green]✅ Monitor mode active[/]")
        console.print("[dim]Press Ctrl+C to stop[/]\n")

        try:
            while True:
                now = datetime.now(timezone.utc).strftime("%H:%M:%S UTC")
                console.print(f"[dim]💓 {now} — Scanner heartbeat[/]", end="\r")
                time.sleep(60)
        except KeyboardInterrupt:
            console.print("\n[yellow]Shutting down...[/]")
            signal_simulator.stop()
            billing_webhook_server.stop()
            admin_bot.stop()
            scheduler.stop()
            console.print("[green]Goodbye! 🦞[/]")
    finally:
        _release_monitor_lock(lock_path)


def cmd_scan(target: str = "all"):
    """Run a one-time scan."""
    from scheduler import scheduler

    console.print(Panel(BANNER, border_style="cyan"))
    console.print(f"\n[cyan]Running one-time scan: {target}[/]\n")
    result = scheduler.run_once(target)
    if target in ("xauusd", "gold") and isinstance(result, dict):
        xres = result.get("xauusd", {})
        status = str(xres.get("status", "unknown"))
        if status == "sent_manual_bypass_cooldown":
            console.print("[yellow]XAUUSD: signal sent (manual cooldown bypass active).[/]")
        elif status == "sent":
            console.print("[green]XAUUSD: signal sent.[/]")
        elif status == "below_confidence":
            sig = xres.get("signal", {}) or {}
            conf = float(sig.get("confidence", 0))
            thr = float(xres.get("confidence_threshold", 0))
            console.print(f"[yellow]XAUUSD: below confidence ({conf:.1f}% < {thr:.1f}%).[/]")
        elif status == "cooldown_suppressed":
            rem = int(float((xres.get("cooldown", {}) or {}).get("remaining_sec", 0)))
            console.print(f"[yellow]XAUUSD: suppressed by cooldown ({rem}s remaining).[/]")


def cmd_research(question: str):
    """Run deep AI research on a financial question."""
    from agent.brain import get_brain

    console.print(Panel(BANNER, border_style="cyan"))
    console.print(f"\n[cyan]🧠 Researching: {question}[/]\n")

    brain = get_brain()
    iteration = 0

    for event in brain.research(question):
        if event.event_type == "thinking":
            if event.content.strip():
                console.print(f"\n[dim italic]💭 Thinking (iter {event.iteration}):[/]")
                console.print(f"[dim]{event.content[:300]}{'...' if len(event.content) > 300 else ''}[/]")

        elif event.event_type == "tool_call":
            console.print(f"\n[cyan]🔧 Tool: {event.tool_name}[/]")
            if event.tool_args:
                console.print(f"[dim]   Args: {event.tool_args}[/]")

        elif event.event_type == "tool_result":
            console.print(f"[green]   ✅ {event.tool_name} — done[/]")

        elif event.event_type == "answer":
            console.print("\n" + "═" * 60)
            console.print("[bold green]💡 RESEARCH ANSWER:[/]")
            console.print("═" * 60)
            console.print(event.content)
            console.print("═" * 60)

            # Also send to Telegram
            try:
                from notifier.telegram_bot import notifier
                notifier.send_research_answer(question, event.content, chat_id=_owner_chat_id())
            except Exception:
                pass

        elif event.event_type == "error":
            console.print(f"\n[red]❌ Error: {event.content}[/]")


def cmd_stocks(market: str = "all"):
    """Scan global stock markets."""
    from scanners.stock_scanner import stock_scanner
    from notifier.telegram_bot import notifier

    def _is_vi_market(m: str) -> bool:
        return str(m or "").lower() in {"vi", "thai_vi", "vi_buffett", "vi_turnaround"}

    def _print_vi_audit_breakdown(opps, title_suffix: str):
        if not opps:
            return
        audit = Table(
            title=f"VI Audit Breakdown — {title_suffix}",
            box=box.SIMPLE_HEAVY,
            style="magenta",
        )
        audit.add_column("#", width=3, style="bold white")
        audit.add_column("Symbol", style="bold yellow")
        audit.add_column("Profile", style="cyan")
        audit.add_column("VI", justify="right")
        audit.add_column("Buff", justify="right")
        audit.add_column("Turn", justify="right")
        audit.add_column("MOS", justify="right")
        audit.add_column("Band", style="green")
        audit.add_column("FCFY", justify="right")
        audit.add_column("O/Y", justify="right")
        audit.add_column("P/E", justify="right")
        audit.add_column("P/B", justify="right")
        audit.add_column("ROE", justify="right")
        audit.add_column("RevG", justify="right")

        for i, opp in enumerate(opps[:10], 1):
            raw = dict(getattr(opp.signal, "raw_scores", {}) or {})
            prof = str(raw.get("vi_primary_profile", "BLEND") or "BLEND")
            mos_pct = raw.get("vi_metric_mos_pct")
            fcf_yield = raw.get("vi_metric_fcf_yield")
            owner_yield = raw.get("vi_metric_owner_earnings_yield")
            pe = raw.get("vi_metric_pe")
            pb = raw.get("vi_metric_pb") or raw.get("vi_metric_price_to_book")
            roe = raw.get("vi_metric_roe") or raw.get("vi_metric_return_on_equity")
            revg = raw.get("vi_metric_revenue_growth")
            def _fp(v, mult=1.0, pct=False):
                try:
                    x=float(v)
                    if pct:
                        return f"{x*100:.1f}%"
                    return f"{x*mult:.1f}"
                except Exception:
                    return "-"
            audit.add_row(
                str(i),
                opp.signal.symbol,
                prof,
                _fp(raw.get("vi_total_score")),
                _fp(raw.get("vi_compounder_score")),
                _fp(raw.get("vi_turnaround_score")),
                _fp(mos_pct, pct=True),
                str(raw.get("vi_metric_mos_band", "-") or "-"),
                _fp(fcf_yield, pct=True),
                _fp(owner_yield, pct=True),
                _fp(pe),
                _fp(pb),
                _fp(roe, pct=True),
                _fp(revg, pct=True),
            )
        console.print(audit)

        console.print("\n[bold magenta]VI Reason Breakdown (Top candidates)[/]")
        for i, opp in enumerate(opps[:10], 1):
            raw = dict(getattr(opp.signal, "raw_scores", {}) or {})
            prof = str(raw.get("vi_primary_profile", "BLEND") or "BLEND")
            console.print(f"[bold]{i}. {opp.signal.symbol}[/] [dim]({prof})[/]")
            reasons = [str(x) for x in (raw.get("vi_reasons_detailed") or []) if str(x).strip()]
            if not reasons:
                console.print("  [dim]- no detailed reasons stored[/]")
                continue
            for r in reasons[:8]:
                console.print(f"  • {r}")

    console.print(Panel(BANNER, border_style="cyan"))
    console.print(f"\n[cyan]🌏 Scanning stocks: {market.upper()}[/]\n")

    market_map = {
        "all":      lambda: stock_scanner.scan_all_open_markets(),
        "us":       lambda: stock_scanner.scan_us(),
        "vi":       lambda: stock_scanner.scan_us_value_trend(top_n=max(3, int(config.VI_TOP_N))),
        "vi_buffett": lambda: stock_scanner.scan_us_value_trend_profile("BUFFETT", top_n=max(3, int(config.VI_TOP_N))),
        "vi_turnaround": lambda: stock_scanner.scan_us_value_trend_profile("TURNAROUND", top_n=max(3, int(config.VI_TOP_N))),
        "thai_vi":  lambda: stock_scanner.scan_thailand_value_trend(top_n=max(3, int(config.VI_TOP_N))),
        "thai":     lambda: stock_scanner.scan_thailand(),
        "priority": lambda: stock_scanner.scan_priority(),
        "uk":       lambda: stock_scanner.scan_group("UK_FTSE100"),
        "de":       lambda: stock_scanner.scan_group("DE_DAX40"),
        "jp":       lambda: stock_scanner.scan_group("JP_NIKKEI"),
        "hk":       lambda: stock_scanner.scan_group("HK_HANGSENG"),
        "sg":       lambda: stock_scanner.scan_group("SG_STI"),
        "in":       lambda: stock_scanner.scan_group("IN_NIFTY"),
        "au":       lambda: stock_scanner.scan_group("AU_ASX"),
    }
    mkey = market.lower()
    scanner_fn = market_map.get(mkey, market_map["all"])
    opps = scanner_fn()

    if not opps:
        console.print("[yellow]No qualifying signals found.[/]")
        return

    table = Table(
        title=f"Stock Opportunities — {market.upper()}",
        box=box.DOUBLE_EDGE, style="cyan"
    )
    table.add_column("#",        style="bold white", width=3)
    table.add_column("Symbol",   style="bold yellow")
    table.add_column("Market",   style="cyan")
    table.add_column("Dir",      style="bold")
    table.add_column("Setup",    style="magenta")
    table.add_column("Entry",    style="green")
    table.add_column("SL",       style="red")
    table.add_column("TP2",      style="green")
    table.add_column("R:R",      style="white")
    table.add_column("Conf%",    style="bold yellow")
    table.add_column("Vol",      style="dim")

    for i, opp in enumerate(opps[:10], 1):
        s = opp.signal
        dir_color = "green" if s.direction == "long" else "red"
        dir_text = f"[{dir_color}]{'▲ LONG' if s.direction == 'long' else '▼ SHORT'}[/]"
        table.add_row(
            str(i), s.symbol, opp.market, dir_text,
            opp.setup_type,
            str(s.entry), str(s.stop_loss), str(s.take_profit_2),
            f"1:{s.risk_reward}", f"{s.confidence:.0f}%",
            f"{opp.vol_vs_avg:.1f}x",
        )
    console.print(table)

    if _is_vi_market(mkey):
        _print_vi_audit_breakdown(opps, market.upper())

    # Send to Telegram
    try:
        label = market.upper()
        summary_ok = True
        if mkey == "vi":
            summary_ok = notifier.send_vi_stock_summary(opps, chat_id=_owner_chat_id(), feature_override="scan_vi")
        elif mkey == "vi_buffett":
            summary_ok = notifier.send_vi_stock_summary(opps, chat_id=_owner_chat_id(), region_label="🇺🇸 US VI BUFFETT", feature_override="scan_vi_buffett")
        elif mkey == "vi_turnaround":
            summary_ok = notifier.send_vi_stock_summary(opps, chat_id=_owner_chat_id(), region_label="🇺🇸 US VI TURNAROUND", feature_override="scan_vi_turnaround")
        elif mkey == "thai_vi":
            summary_ok = notifier.send_vi_stock_summary(opps, chat_id=_owner_chat_id(), region_label="🇹🇭 THAILAND", feature_override="scan_thai_vi")
        else:
            summary_ok = notifier.send_stock_scan_summary(opps, market_label=label, chat_id=_owner_chat_id())
        signal_ok = True
        if opps[0].signal.confidence >= 75:
            if mkey == "vi":
                signal_ok = notifier.send_stock_signal(opps[0], chat_id=_owner_chat_id(), feature_override="scan_vi")
            elif mkey == "vi_buffett":
                signal_ok = notifier.send_stock_signal(opps[0], chat_id=_owner_chat_id(), feature_override="scan_vi_buffett")
            elif mkey == "vi_turnaround":
                signal_ok = notifier.send_stock_signal(opps[0], chat_id=_owner_chat_id(), feature_override="scan_vi_turnaround")
            elif mkey == "thai_vi":
                signal_ok = notifier.send_stock_signal(opps[0], chat_id=_owner_chat_id(), feature_override="scan_thai_vi")
            else:
                signal_ok = notifier.send_stock_signal(opps[0], chat_id=_owner_chat_id())
        if summary_ok and signal_ok:
            console.print("[green]\n✅ Results sent to Telegram[/]")
        else:
            console.print("[yellow]\n⚠️ Telegram send failed (check chat ID/bot access)[/]")
    except Exception as e:
        console.print(f"[yellow]⚠️ Telegram: {e}[/]")


def cmd_markets():
    """Print global market hours status."""
    from scanners.stock_scanner import stock_scanner
    from notifier.telegram_bot import notifier
    from datetime import datetime, timezone

    console.print(Panel(BANNER, border_style="cyan"))
    overview = stock_scanner.get_market_overview()
    now = datetime.now(timezone.utc).strftime("%H:%M UTC")

    table = Table(
        title=f"Global Market Status — {now}",
        box=box.ROUNDED, style="cyan"
    )
    table.add_column("Market", style="bold white")
    table.add_column("Status", style="bold")
    table.add_column("Hours (UTC)", style="dim")
    table.add_column("TZ", style="dim")

    flags = {
        "US": "🇺🇸 US", "UK": "🇬🇧 UK", "DE": "🇩🇪 DE",
        "FR": "🇫🇷 FR", "JP": "🇯🇵 JP", "HK": "🇭🇰 HK",
        "TH": "🇹🇭 TH", "SG": "🇸🇬 SG", "IN": "🇮🇳 IN", "AU": "🇦🇺 AU",
    }

    for market, info in overview.get("all_markets", {}).items():
        flag_name = flags.get(market, f"🌏 {market}")
        status_text = "[green]● OPEN[/]" if info["open"] else "[red]● CLOSED[/]"
        table.add_row(flag_name, status_text, info["hours_utc"], info["tz"])

    console.print(table)
    try:
        sent = notifier.send_market_hours_overview(overview, chat_id=_owner_chat_id())
        if sent:
            console.print("[green]✅ Sent to Telegram[/]")
        else:
            console.print("[yellow]⚠️ Telegram send failed (check chat ID/bot access)[/]")
    except Exception as e:
        console.print(f"[yellow]⚠️ Telegram: {e}[/]")


def cmd_overview():
    """Print XAUUSD market overview."""
    from scanners.xauusd import xauusd_scanner
    from notifier.telegram_bot import notifier

    console.print(Panel(BANNER, border_style="cyan"))
    console.print("\n[cyan]📊 Fetching XAUUSD overview...[/]\n")

    overview = xauusd_scanner.get_market_overview()

    # Print to terminal
    table = Table(title="XAUUSD Market Overview", box=box.DOUBLE_EDGE, style="cyan")
    table.add_column("Metric", style="bold white")
    table.add_column("Value", style="yellow")

    price = overview.get("price", "N/A")
    table.add_row("Current Price", f"${price:,.2f}" if isinstance(price, float) else str(price))
    if overview.get("price_source"):
        table.add_row("Price Source", str(overview.get("price_source")))

    session = overview.get("session", {})
    table.add_row("Sessions", ", ".join(session.get("active_sessions", ["unknown"])))

    for tf, key in [("H1", "h1"), ("H4", "h4"), ("D1", "d1")]:
        data = overview.get(key, {})
        if data:
            trend = data.get("trend", "?")
            rsi = data.get("rsi", "?")
            trend_color = "green" if trend == "bullish" else ("red" if trend == "bearish" else "yellow")
            table.add_row(f"{tf} Trend", f"[{trend_color}]{trend}[/] | RSI: {rsi}")

    levels = overview.get("key_levels", {})
    if levels:
        table.add_row("Resistance", f"${levels.get('nearest_resistance', 'N/A'):.0f}")
        table.add_row("Support", f"${levels.get('nearest_support', 'N/A'):.0f}")
        if "asian_high" in levels:
            table.add_row("Asian Range", f"${levels.get('asian_low'):.2f} - ${levels.get('asian_high'):.2f}")

    smc = overview.get("h4_smc", {})
    if smc:
        bias = smc.get("bias", "neutral")
        bias_color = "green" if bias == "long" else ("red" if bias == "short" else "yellow")
        table.add_row("H4 SMC Bias", f"[{bias_color}]{bias.upper()}[/] ({int(smc.get('confidence',0)*100)}%)")

    console.print(table)

    # Send to Telegram too
    try:
        sent = notifier.send_xauusd_overview(overview, chat_id=_owner_chat_id())
        if sent:
            console.print("[green]\n✅ Overview sent to Telegram[/]")
        else:
            console.print("[yellow]\n⚠️ Telegram send failed (check chat ID/bot access)[/]")
    except Exception as e:
        console.print(f"[yellow]⚠️ Telegram send failed: {e}[/]")


def cmd_status():
    """Print system status."""
    from scanners.xauusd import xauusd_scanner
    from scanners.crypto_sniper import crypto_sniper
    from scanners.stock_scanner import stock_scanner
    from market.stock_universe import get_all_stocks
    from execution.mt5_executor import mt5_executor
    from learning.neural_brain import neural_brain
    from notifier.billing_webhook import billing_webhook_server

    console.print(Panel(BANNER, border_style="cyan"))

    table = Table(title="Dexter Pro — System Status", box=box.ROUNDED, style="cyan")
    table.add_column("Component", style="bold white")
    table.add_column("Status", style="green")
    table.add_column("Details")

    missing = config.validate()
    provider = config.resolve_ai_provider()
    api_status = "✅ OK" if provider != "none" else "❌ Missing"
    tg_status = "✅ OK" if config.TELEGRAM_BOT_TOKEN else "⚠️ Not set"

    table.add_row("AI Provider", api_status, f"{provider.upper()} | Model: {config.model_for_provider(provider)}")
    table.add_row("Telegram Bot", tg_status, "@mrgeon8n_bot")
    table.add_row("Exchange", "✅", f"{config.CRYPTO_EXCHANGE.upper()} | Top {config.TOP_COINS_COUNT} pairs")
    table.add_row("Min Confidence", "✅", f"Crypto/Gold: {config.MIN_SIGNAL_CONFIDENCE}% | Stocks: {config.STOCK_MIN_CONFIDENCE}%")
    table.add_row("XAUUSD Scan", "⏰", f"Every {config.XAUUSD_SCAN_INTERVAL//60}m")
    table.add_row("Crypto Scan", "⏰", f"Every {config.CRYPTO_SCAN_INTERVAL//60}m")
    table.add_row("Stock Scan", "⏰", f"Every {config.STOCK_SCAN_INTERVAL//60}m + market opens")
    mt5_state = mt5_executor.status()
    mt5_status = "✅" if (mt5_state.get("enabled") and mt5_state.get("connected")) else ("⚪" if mt5_state.get("enabled") else "⏸️")
    if mt5_state.get("enabled"):
        details = (
            f"{'DRY_RUN' if mt5_state.get('dry_run') else 'LIVE'} | "
            f"{mt5_state.get('host')}:{mt5_state.get('port')} | "
            f"Symbols: {mt5_state.get('symbols', 0)}"
        )
        if mt5_state.get("account_login"):
            details += f" | Login: {mt5_state.get('account_login')}"
        if mt5_state.get("error"):
            details += f" | {mt5_state.get('error')}"
    else:
        details = "Disabled (set MT5_ENABLED=1)"
    table.add_row("MT5 Execution", mt5_status, details)
    brain = neural_brain.model_status()
    brain_filter = neural_brain.execution_filter_status() if config.NEURAL_BRAIN_ENABLED else {"ready": False, "reason": "disabled"}
    if config.NEURAL_BRAIN_ENABLED:
        if brain.get("available"):
            brain_details = (
                f"Model ready | samples={brain.get('samples', 0)} | "
                f"val_acc={float(brain.get('val_accuracy', 0)) * 100:.1f}% | "
                f"filter={'ready' if brain_filter.get('ready') else brain_filter.get('reason', 'not_ready')}"
            )
            table.add_row("Neural Brain", "✅", brain_details)
        else:
            table.add_row(
                "Neural Brain",
                "⚪",
                (
                    f"Collecting labels (bootstrap>={config.NEURAL_BRAIN_BOOTSTRAP_MIN_SAMPLES}, "
                    f"target>={config.NEURAL_BRAIN_MIN_SAMPLES}) | "
                    f"feedback={'on' if config.SIGNAL_FEEDBACK_ENABLED else 'off'}"
                ),
            )
    else:
        table.add_row("Neural Brain", "⏸️", "Disabled (set NEURAL_BRAIN_ENABLED=1)")
    bill = billing_webhook_server.status()
    if bill.get("enabled"):
        bill_status = "✅" if bill.get("running") else "⚠️"
        bill_details = f"{bill.get('host')}:{bill.get('port')} | stripe={bill.get('stripe_enabled')} promptpay={bill.get('promptpay_enabled')}"
    else:
        bill_status = "⏸️"
        bill_details = "Disabled (set BILLING_ENABLED=1)"
    table.add_row("Billing Webhook", bill_status, bill_details)

    xstats = xauusd_scanner.get_stats()
    table.add_row("XAUUSD Scanner", "✅", f"Scans: {xstats['total_scans']} | Signals: {xstats['signals_generated']}")

    cstats = crypto_sniper.get_stats()
    table.add_row("Crypto Sniper", "✅", f"Scans: {cstats['total_scans']} | Signals: {cstats['total_signals']}")

    sstats = stock_scanner.get_stats()
    total_stocks = len(get_all_stocks())
    table.add_row(
        "Stock Scanner 🌍", "✅",
        f"Scans: {sstats['total_scans']} | Signals: {sstats['total_signals']} | {total_stocks} stocks | 11 markets"
    )

    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    table.add_row("Time", "🕐", now)
    console.print(table)


def cmd_mt5(
    action: str = "status",
    scope: str = "quick",
    days: int = 30,
    sync_days: int = 120,
    symbols: str = "XAUUSD,ETHUSD,BTCUSD,GBPUSD",
    iterations: int = 3,
    target_win_rate: float = 58.0,
    target_profit_factor: float = 1.2,
    min_trades: int = 12,
    continuous: bool = False,
    interval_min: int = 30,
    max_cycles: int = 0,
    key: str = "",
    value: str = "",
    symbol: str = "",
    pm_action: str = "",
    top: int = 8,
    draft: bool = False,
    ok_only: bool = False,
):
    """MT5 bridge diagnostics and symbol preview."""
    from execution.mt5_executor import mt5_executor

    console.print(Panel(BANNER, border_style="cyan"))
    action = (action or "status").lower()
    scope = (scope or "quick").lower()

    if action == "status":
        state = mt5_executor.status()
        table = Table(title="MT5 Bridge Status", box=box.ROUNDED, style="cyan")
        table.add_column("Field", style="bold white")
        table.add_column("Value", style="yellow")
        for key in (
            "enabled", "dry_run", "micro_mode", "micro_single_position_only", "position_limits_bot_only",
            "max_open_positions", "max_positions_per_symbol", "micro_max_spread_pct", "available", "connected",
            "host", "port", "account_login", "account_server", "balance", "equity", "margin_free", "currency", "leverage",
            "micro_learner_enabled", "micro_balance_bucket", "micro_whitelist_allowed", "micro_whitelist_denied", "micro_whitelist_total",
            "symbols", "error",
        ):
            table.add_row(key, str(state.get(key)))
        console.print(table)
        return

    if action == "symbols":
        symbols = mt5_executor.list_symbols(limit=100)
        if not symbols:
            console.print("[yellow]No broker symbols loaded (check bridge connection).[/]")
            return
        table = Table(title="MT5 Broker Symbols (Top 100)", box=box.ROUNDED, style="cyan")
        table.add_column("#", style="bold white", width=5)
        table.add_column("Symbol", style="yellow")
        for i, sym in enumerate(symbols, 1):
            table.add_row(str(i), sym)
        console.print(table)
        return

    if action == "bootstrap":
        report = mt5_executor.suggest_symbol_map(scope=scope)
        if not report.get("connected"):
            console.print(f"[red]MT5 bootstrap failed:[/] {report.get('error')}")
            return

        summary = Table(title=f"MT5 Symbol Bootstrap ({report.get('scope')})", box=box.ROUNDED, style="cyan")
        summary.add_column("Metric", style="bold white")
        summary.add_column("Value", style="yellow")
        summary.add_row("Broker symbols", str(report.get("broker_symbols", 0)))
        summary.add_row("Candidates", str(report.get("total_candidates", 0)))
        summary.add_row("Resolved", str(report.get("resolved_count", 0)))
        summary.add_row("Explicit map needed", str(len(report.get("suggested_map", {}))))
        summary.add_row("Passthrough", str(len(report.get("passthrough", []))))
        summary.add_row("Unresolved", str(len(report.get("unresolved", []))))
        console.print(summary)

        suggested_map = report.get("suggested_map", {})
        if suggested_map:
            table = Table(title="Suggested MT5 Symbol Map", box=box.ROUNDED, style="cyan")
            table.add_column("Signal Symbol", style="bold white")
            table.add_column("Broker Symbol", style="yellow")
            for sig, brk in suggested_map.items():
                table.add_row(sig, brk)
            console.print(table)
            console.print("\n[green]Suggested .env.local line:[/]")
            console.print(f"[bold]{report.get('env_line')}[/]")
        else:
            console.print("[green]No explicit mapping required for resolved candidates.[/]")

        unresolved = report.get("unresolved", [])
        if unresolved:
            preview = ", ".join(unresolved[:25])
            suffix = " ..." if len(unresolved) > 25 else ""
            console.print(f"[yellow]Unresolved symbols ({len(unresolved)}):[/] {preview}{suffix}")
        return

    if action == "backtest":
        from learning.mt5_backtester import mt5_backtester
        report = mt5_backtester.run(days=max(1, int(days)), sync_days=max(1, int(sync_days)))

        summary = Table(title=f"MT5 Backtest ({max(1, int(days))}d)", box=box.ROUNDED, style="cyan")
        summary.add_column("Metric", style="bold white")
        summary.add_column("Value", style="yellow")
        summary.add_row("status", str(report.get("status")))
        summary.add_row("trades", str(report.get("trades", 0)))
        summary.add_row("win_rate", f"{float(report.get('win_rate', 0)):.1f}%")
        summary.add_row("net_pnl", str(report.get("net_pnl", 0)))
        summary.add_row("profit_factor", str(report.get("profit_factor", 0)))
        sync = report.get("sync", {}) or {}
        summary.add_row("sync.updated", str(sync.get("updated", 0)))
        summary.add_row("sync.closed_positions", str(sync.get("closed_positions", 0)))
        feedback = report.get("feedback", {}) or {}
        summary.add_row("feedback.resolved", str(feedback.get("resolved", 0)))
        summary.add_row("feedback.pseudo_labeled", str(feedback.get("pseudo_labeled", 0)))
        summary.add_row("feedback.reviewed", str(feedback.get("reviewed", 0)))
        model = report.get("model", {}) or {}
        summary.add_row("model.available", str(model.get("available", False)))
        if model.get("available"):
            summary.add_row("model.samples", str(model.get("samples", 0)))
            summary.add_row("model.val_accuracy", f"{float(model.get('val_accuracy', 0)) * 100:.1f}%")
        console.print(summary)

        tops = list(report.get("top_symbols", []) or [])
        if tops:
            top_table = Table(title="Top Symbols", box=box.ROUNDED, style="cyan")
            top_table.add_column("#", style="bold white", width=4)
            top_table.add_column("Symbol", style="yellow")
            top_table.add_column("Trades", style="white")
            top_table.add_column("WinRate", style="green")
            top_table.add_column("NetPnL", style="magenta")
            for i, row in enumerate(tops[:10], 1):
                top_table.add_row(
                    str(i),
                    str(row.get("symbol", "-")),
                    str(row.get("trades", 0)),
                    f"{float(row.get('win_rate', 0)):.1f}%",
                    str(row.get("net_pnl", 0)),
                )
            console.print(top_table)
        return

    if action == "brain":
        from learning.neural_brain import neural_brain

        st = neural_brain.model_status()
        filter_state = neural_brain.execution_filter_status()
        ds = neural_brain.data_status(days=config.NEURAL_BRAIN_SYNC_DAYS)
        table = Table(title="Neural Brain Status", box=box.ROUNDED, style="cyan")
        table.add_column("Field", style="bold white")
        table.add_column("Value", style="yellow")
        table.add_row("enabled", str(config.NEURAL_BRAIN_ENABLED))
        table.add_row("auto_train", str(config.NEURAL_BRAIN_AUTO_TRAIN))
        table.add_row("execution_filter", str(config.NEURAL_BRAIN_EXECUTION_FILTER))
        table.add_row("min_prob", str(config.NEURAL_BRAIN_MIN_PROB))
        table.add_row("filter_ready", str(filter_state.get("ready", False)))
        table.add_row("filter_reason", str(filter_state.get("reason", "")))
        table.add_row("bootstrap_min_samples", str(config.NEURAL_BRAIN_BOOTSTRAP_MIN_SAMPLES))
        table.add_row("feedback_enabled", str(config.SIGNAL_FEEDBACK_ENABLED))
        table.add_row("pseudo_label_enabled", str(config.NEURAL_BRAIN_PSEUDO_LABEL_ENABLED))
        table.add_row("pseudo_label_min_h", str(config.NEURAL_BRAIN_PSEUDO_LABEL_MIN_HOURS))
        table.add_row("pseudo_label_min_abs_r", str(config.NEURAL_BRAIN_PSEUDO_LABEL_MIN_ABS_R))
        table.add_row("soft_adjust", str(config.NEURAL_BRAIN_SOFT_ADJUST))
        table.add_row("soft_weight", str(config.NEURAL_BRAIN_SOFT_ADJUST_WEIGHT))
        table.add_row("soft_max_delta", str(config.NEURAL_BRAIN_SOFT_ADJUST_MAX_DELTA))
        table.add_row("model_available", str(st.get("available", False)))
        table.add_row("data_days", str(ds.get("days", 0)))
        table.add_row("data_total_events", str(ds.get("total_events", 0)))
        table.add_row("data_labeled_events", str(ds.get("labeled_events", 0)))
        table.add_row("data_pending_feedback", str(ds.get("pending_feedback", 0)))
        table.add_row("data_pending_mt5", str(ds.get("pending_mt5", 0)))
        if st.get("available"):
            table.add_row("trained_at", str(st.get("trained_at", "")))
            table.add_row("samples", str(st.get("samples", 0)))
            table.add_row("train_accuracy", f"{float(st.get('train_accuracy', 0)) * 100:.1f}%")
            table.add_row("val_accuracy", f"{float(st.get('val_accuracy', 0)) * 100:.1f}%")
            table.add_row("baseline_win_rate", f"{float(st.get('win_rate', 0)) * 100:.1f}%")
            table.add_row("filter_min_samples", str(config.NEURAL_BRAIN_FILTER_MIN_SAMPLES))
            table.add_row("filter_min_val_acc", f"{float(config.NEURAL_BRAIN_FILTER_MIN_VAL_ACC) * 100:.1f}%")
        console.print(table)
        return

    if action == "autopilot":
        from learning.mt5_autopilot_core import mt5_autopilot_core

        st = mt5_autopilot_core.status()
        table = Table(title="MT5 Autopilot Core", box=box.ROUNDED, style="cyan")
        table.add_column("Field", style="bold white")
        table.add_column("Value", style="yellow")
        table.add_row("enabled", str(st.get("enabled", False)))
        table.add_row("db_path", str(st.get("db_path", "")))
        table.add_row("account_key", str(st.get("account_key", "")))
        gate = dict(st.get("risk_gate", {}) or {})
        table.add_row("risk_gate.allow", str(gate.get("allow", True)))
        table.add_row("risk_gate.status", str(gate.get("status", "")))
        table.add_row("risk_gate.reason", str(gate.get("reason", "")))
        snap = dict(st.get("risk_snapshot", {}) or {})
        for k in ("daily_realized_pnl", "daily_loss_abs", "consecutive_losses", "recent_rejections_1h", "open_positions", "pending_orders"):
            if k in snap:
                table.add_row(f"risk.{k}", str(snap.get(k)))
        journal = dict(st.get("journal", {}) or {})
        for k in ("total", "resolved", "open_forward_tests", "rejected_24h"):
            table.add_row(f"journal.{k}", str(journal.get(k, 0)))
        calib = dict(st.get("calibration", {}) or {})
        table.add_row("calib.labeled_7d", str(calib.get("labeled_7d", 0)))
        table.add_row("calib.win_rate_7d", f"{float(calib.get('win_rate_7d', 0.0) or 0.0) * 100:.1f}%")
        mae = calib.get("mae_7d")
        table.add_row("calib.mae_7d", "-" if mae is None else str(mae))
        console.print(table)
        return

    if action == "orchestrator":
        from learning.mt5_orchestrator import mt5_orchestrator

        st = mt5_orchestrator.status()
        table = Table(title="MT5 Orchestrator", box=box.ROUNDED, style="cyan")
        table.add_column("Field", style="bold white")
        table.add_column("Value", style="yellow")
        table.add_row("enabled", str(st.get("enabled", False)))
        table.add_row("db_path", str(st.get("db_path", "")))
        table.add_row("accounts_total", str(st.get("accounts_total", 0)))
        table.add_row("current_account_key", str(st.get("current_account_key", "")))
        ep = dict(st.get("execution_plan_preview", {}) or {})
        if ep:
            table.add_row("plan.allow", str(ep.get("allow")))
            table.add_row("plan.reason", str(ep.get("reason")))
            table.add_row("plan.canary_mode", str(ep.get("canary_mode")))
            table.add_row("plan.risk_multiplier", str(ep.get("risk_multiplier")))
            wf = dict(ep.get("walkforward", {}) or {})
            for k in ("train_trades", "forward_trades", "forward_win_rate", "forward_mae"):
                if k in wf:
                    table.add_row(f"wf.{k}", str(wf.get(k)))
        console.print(table)
        return

    if action == "walkforward":
        from learning.mt5_orchestrator import mt5_orchestrator
        from learning.mt5_walkforward import mt5_walkforward

        orch = mt5_orchestrator.status()
        acct = str(orch.get("current_account_key", "") or "")
        if not acct:
            console.print("[yellow]No connected MT5 account for walk-forward report.[/]")
            return
        rpt = mt5_walkforward.build_report(acct, train_days=config.MT5_WF_TRAIN_DAYS, forward_days=config.MT5_WF_FORWARD_DAYS)
        if not rpt.get("ok"):
            console.print(f"[red]Walk-forward report failed:[/] {rpt.get('error')}")
            return
        table = Table(title="MT5 Walk-Forward Validation", box=box.ROUNDED, style="cyan")
        table.add_column("Field", style="bold white")
        table.add_column("Value", style="yellow")
        table.add_row("account_key", acct)
        table.add_row("train_days", str(rpt.get("train_days")))
        table.add_row("forward_days", str(rpt.get("forward_days")))
        train = dict(rpt.get("train", {}) or {})
        fwd = dict(rpt.get("forward", {}) or {})
        canary = dict(rpt.get("canary", {}) or {})
        for prefix, block in (("train", train), ("forward", fwd)):
            table.add_row(f"{prefix}.trades", str(block.get("trades", 0)))
            table.add_row(f"{prefix}.win_rate", f"{float(block.get('win_rate', 0.0) or 0.0) * 100:.1f}%")
            table.add_row(f"{prefix}.net_pnl", str(block.get("net_pnl", 0.0)))
            table.add_row(f"{prefix}.mae", "-" if block.get("mae") is None else str(block.get("mae")))
        table.add_row("canary_mode", str(canary.get("canary_mode", True)))
        table.add_row("canary_pass", str(canary.get("canary_pass", False)))
        table.add_row("canary.reason", str(canary.get("reason", "")))
        table.add_row("risk_multiplier", str(canary.get("risk_multiplier", 1.0)))
        console.print(table)
        return

    if action == "manage":
        from learning.mt5_position_manager import mt5_position_manager

        if scope == "watch":
            sym_filter = str(key or "").strip().upper()
            rpt = mt5_position_manager.watch_snapshot(signal_symbol=sym_filter, limit=10)
            table = Table(title="MT5 Position Manager Watch", box=box.ROUNDED, style="cyan")
            table.add_column("Field", style="bold white")
            table.add_column("Value", style="yellow")
            for key in ("enabled", "ok", "account_key", "requested_symbol", "resolved_symbol", "positions", "watched", "error"):
                table.add_row(key, str(rpt.get(key)))
            console.print(table)
            rules = dict(rpt.get("rules", {}) or {})
            if rules:
                rt = Table(title="PM Rules", box=box.ROUNDED, style="cyan")
                rt.add_column("Rule", style="bold white")
                rt.add_column("Value", style="yellow")
                for k in sorted(rules.keys()):
                    rt.add_row(str(k), str(rules.get(k)))
                console.print(rt)
            entries = list(rpt.get("entries", []) or [])
            if entries:
                et = Table(title="Watched Positions", box=box.ROUNDED, style="cyan")
                et.add_column("Symbol", style="yellow")
                et.add_column("Ticket", style="white")
                et.add_column("Type", style="white")
                et.add_column("Vol", style="white")
                et.add_column("PnL", style="magenta")
                et.add_column("Age(m)", style="white")
                et.add_column("R now", style="green")
                et.add_column("Flags", style="cyan")
                for row in entries[:10]:
                    st_flags = dict((row.get("state") or {}))
                    nxt = dict((row.get("next_checks") or {}))
                    flags = []
                    if st_flags.get("breakeven_done"): flags.append("BE✓")
                    if st_flags.get("partial_done"): flags.append("PT✓")
                    if st_flags.get("time_stop_done"): flags.append("TS✓")
                    if st_flags.get("early_risk_done"): flags.append("ER✓")
                    if nxt.get("breakeven_ready"): flags.append("BE!")
                    if nxt.get("partial_ready"): flags.append("PT!")
                    if nxt.get("trail_ready"): flags.append("TR!")
                    if nxt.get("time_stop_ready"): flags.append("TS!")
                    if nxt.get("early_risk_ready"): flags.append("ER!")
                    if nxt.get("spread_spike_ready"): flags.append("SP!")
                    dist = dict((row.get("distances") or {}))
                    dist_txt = "-"
                    if dist:
                        dist_txt = (
                            f"BE {dist.get('to_be_trigger_r','-')}R/{dist.get('to_be_trigger_price','-')} | "
                            f"TP {dist.get('to_tp_price','-')} | SL {dist.get('to_sl_price','-')}"
                        )
                    et.add_row(
                        str(row.get("symbol")),
                        str(row.get("ticket")),
                        str(row.get("type")),
                        str(row.get("volume")),
                        str(row.get("profit")),
                        str(row.get("age_min")),
                        str(row.get("r_now")),
                        (", ".join(flags) or "-") + (f" | {dist_txt}" if dist_txt != "-" else ""),
                    )
                console.print(et)
            return

        rpt = mt5_position_manager.run_cycle(source="cli")
        table = Table(title="MT5 Position Manager", box=box.ROUNDED, style="cyan")
        table.add_column("Field", style="bold white")
        table.add_column("Value", style="yellow")
        for key in ("enabled", "ok", "account_key", "positions", "checked", "managed", "removed_states", "error"):
            table.add_row(key, str(rpt.get(key)))
        console.print(table)
        actions = list(rpt.get("actions", []) or [])
        if actions:
            at = Table(title="Actions", box=box.ROUNDED, style="cyan")
            at.add_column("Symbol", style="yellow")
            at.add_column("Ticket", style="white")
            at.add_column("Action", style="cyan")
            at.add_column("Status", style="magenta")
            at.add_column("Message", style="white")
            for a in actions[:10]:
                at.add_row(str(a.get("symbol")), str(a.get("ticket")), str(a.get("action")), str(a.get("status")), str(a.get("message")))
            console.print(at)
        return

    if action == "pm_learning":
        from learning.mt5_position_manager import mt5_position_manager

        lookback_days = max(1, int(days or 30))
        top_n = max(1, min(20, int(top or 8)))
        sync = True
        symbol_filter = str(symbol or "").strip()
        action_filter = str(pm_action or "").strip()
        scope_l = (scope or "").lower()
        if scope_l.startswith("top"):
            try:
                top_n = max(1, min(20, int(scope_l[3:])))
            except Exception:
                top_n = 8
        elif scope_l in {"nosync", "no-sync"}:
            sync = False
        rpt = mt5_position_manager.build_learning_report(
            days=lookback_days,
            top=top_n,
            sync=sync,
            symbol=symbol_filter,
            action=action_filter,
        )
        if bool(draft) and bool(rpt.get("ok")):
            try:
                from learning.mt5_orchestrator import mt5_orchestrator
                d = mt5_position_manager.build_policy_draft_from_learning_report(rpt)
                rpt["draft_result"] = mt5_orchestrator.save_current_account_policy_draft(d, source="mt5_pm_learning_cli")
            except Exception as e:
                rpt["draft_result"] = {"ok": False, "error": str(e)}
        table = Table(title=f"MT5 PM Learning ({lookback_days}d)", box=box.ROUNDED, style="cyan")
        table.add_column("Field", style="bold white")
        table.add_column("Value", style="yellow")
        for k in ("enabled", "learning_enabled", "ok", "account_key", "error"):
            table.add_row(k, str(rpt.get(k)))
        summ = dict(rpt.get("summary", {}) or {})
        filters = dict(rpt.get("filters", {}) or {})
        if filters.get("symbol"):
            table.add_row("filter.symbol", str(filters.get("symbol")))
        if filters.get("action"):
            table.add_row("filter.action", str(filters.get("action")))
        for k in ("total_actions", "resolved_actions", "unresolved_actions"):
            table.add_row(f"summary.{k}", str(summ.get(k, 0)))
        sync_r = dict(rpt.get("sync", {}) or {})
        if sync_r:
            table.add_row("sync.ok", str(sync_r.get("ok")))
            table.add_row("sync.updated", str(sync_r.get("updated", 0)))
            table.add_row("sync.closed_rows_seen", str(sync_r.get("closed_rows_seen", 0)))
            table.add_row("sync.query_mode", str(sync_r.get("history_query_mode", "")))
        dr = dict(rpt.get("draft_result", {}) or {})
        if dr:
            table.add_row("draft.saved", str(dr.get("ok", False)))
            table.add_row("draft.keys", ", ".join(list(dr.get("keys", []) or [])[:8]))
        console.print(table)
        acts = list(rpt.get("actions_overall", []) or [])
        if acts:
            at = Table(title="PM Action Effectiveness (Overall)", box=box.ROUNDED, style="cyan")
            for c in ("Action", "Samples", "Resolved", "Pos%", "Neg%", "TP%", "AvgPnL"):
                at.add_column(c, style="yellow" if c == "Action" else "white")
            for row in acts[:8]:
                at.add_row(
                    str(row.get("label")),
                    str(row.get("samples", 0)),
                    str(row.get("resolved", 0)),
                    ("-" if row.get("positive_rate") is None else f"{float(row.get('positive_rate'))*100:.0f}%"),
                    ("-" if row.get("negative_rate") is None else f"{float(row.get('negative_rate'))*100:.0f}%"),
                    ("-" if row.get("tp_rate") is None else f"{float(row.get('tp_rate'))*100:.0f}%"),
                    ("-" if row.get("avg_pnl") is None else str(row.get("avg_pnl"))),
                )
            console.print(at)
        syms = list(rpt.get("symbols", []) or [])
        if syms:
            st = Table(title=f"Top {min(len(syms), top_n)} Symbols", box=box.ROUNDED, style="cyan")
            for c in ("Symbol", "Samples", "Resolved", "Pos%", "Neg%", "Best", "Weak"):
                st.add_column(c, style="yellow" if c == "Symbol" else "white")
            for row in syms[:top_n]:
                best = dict(row.get("best_action") or {})
                weak = dict(row.get("weak_action") or {})
                st.add_row(
                    str(row.get("label")),
                    str(row.get("samples", 0)),
                    str(row.get("resolved", 0)),
                    ("-" if row.get("positive_rate") is None else f"{float(row.get('positive_rate'))*100:.0f}%"),
                    ("-" if row.get("negative_rate") is None else f"{float(row.get('negative_rate'))*100:.0f}%"),
                    (str(best.get("label")) if best else "-"),
                    (str(weak.get("label")) if weak else "-"),
                )
            console.print(st)
        recs = list(rpt.get("recommendations", []) or [])
        if recs:
            rt = Table(title="PM Threshold Recommendations (Bounded)", box=box.ROUNDED, style="cyan")
            for c in ("Action", "Key", "Current", "Suggested", "Dir", "Conf", "Samples"):
                rt.add_column(c, style="yellow" if c in {"Action", "Key"} else "white")
            for rec in recs[:6]:
                rt.add_row(
                    str(rec.get("action")),
                    str(rec.get("key")),
                    str(rec.get("current")),
                    str(rec.get("suggested")),
                    str(rec.get("direction")),
                    str(rec.get("confidence")),
                    str(rec.get("samples")),
                )
            console.print(rt)
        reg = list(rpt.get("recommendations_by_regime", []) or [])
        if reg:
            gt = Table(title="PM Recommendations by Regime", box=box.ROUNDED, style="cyan")
            for c in ("Regime", "Rows", "Resolved", "Top Recs"):
                gt.add_column(c, style="yellow" if c == "Regime" else "white")
            for b in reg[:6]:
                top_recs = []
                for rec in list(b.get("recommendations", []) or [])[:2]:
                    top_recs.append(f"{rec.get('key')} {rec.get('current')}→{rec.get('suggested')}")
                gt.add_row(
                    str(b.get("regime")),
                    str(b.get("rows", 0)),
                    str(b.get("resolved_rows", 0)),
                    " | ".join(top_recs) or "-",
                )
            console.print(gt)
        return

    if action == "affordable":
        from execution.mt5_executor import mt5_executor

        raw_scope = (scope or "all").lower().strip()
        ok_only = bool(ok_only)
        if raw_scope == "ok":
            ok_only = True
            cat = "all"
        else:
            cat = raw_scope
        if cat not in {"all", "crypto", "fx", "metal", "index"}:
            cat = "all"
        top_n = max(3, min(30, int(top or 12)))
        rpt = mt5_executor.affordable_symbols_snapshot(category=cat, limit=top_n, only_ok=ok_only)
        title_suffix = f"{cat}" + (" | ok-only" if ok_only else "")
        table = Table(title=f"MT5 Affordable Symbols ({title_suffix})", box=box.ROUNDED, style="cyan")
        table.add_column("Field", style="bold white")
        table.add_column("Value", style="yellow")
        for k in ("enabled", "available", "connected", "account_server", "account_login", "balance", "equity", "free_margin", "currency", "error"):
            table.add_row(k, str(rpt.get(k)))
        table.add_row("margin_budget_pct", str(rpt.get("margin_budget_pct")))
        table.add_row("allowed_margin", str(rpt.get("allowed_margin")))
        table.add_row("micro_max_spread_pct", str(rpt.get("micro_max_spread_pct")))
        summ = dict(rpt.get("summary", {}) or {})
        for k in ("broker_symbols", "recognized_candidates", "checked", "ok_now", "market_ok", "margin_ok", "spread_ok"):
            table.add_row(f"summary.{k}", str(summ.get(k, 0)))
        console.print(table)

        rows = list(rpt.get("rows", []) or [])
        if rows:
            rt = Table(title=f"Top {min(len(rows), top_n)} Affordable Candidates", box=box.ROUNDED, style="cyan")
            for c in ("Symbol", "Cat", "Status", "Margin(min)", "Spread%", "Policy", "Flags"):
                rt.add_column(c, style="yellow" if c == "Symbol" else "white")
            for r in rows[:top_n]:
                flags = []
                flags.append("m✓" if r.get("margin_ok") else "m✗")
                flags.append("s✓" if r.get("spread_ok") else "s✗")
                flags.append("p✓" if r.get("policy_ok") else "p✗")
                rt.add_row(
                    str(r.get("symbol")),
                    str(r.get("category")),
                    str(r.get("status")),
                    str(r.get("margin_min_lot")),
                    str(r.get("spread_pct")),
                    ("yes" if r.get("policy_ok") else "no"),
                    " ".join(flags),
                )
            console.print(rt)
        return

    if action == "exec_reasons":
        from learning.mt5_autopilot_core import mt5_autopilot_core

        hrs = max(1, min(24 * 14, int(days or 1) * 24))
        sym_filter = str(symbol or "").strip().upper()
        rpt = mt5_autopilot_core.execution_reasons_report(hours=hrs, symbol=sym_filter)
        table = Table(title=f"MT5 Execution Reasons ({hrs}h)", box=box.ROUNDED, style="cyan")
        table.add_column("Field", style="bold white")
        table.add_column("Value", style="yellow")
        for k in ("enabled", "ok", "account_key", "symbol", "hours", "message"):
            table.add_row(k, str(rpt.get(k, "")))
        summ = dict(rpt.get("summary", {}) or {})
        for k in ("total", "filled", "skipped", "guard_blocked", "errors"):
            table.add_row(f"summary.{k}", str(summ.get(k, 0)))
        console.print(table)

        bysym = list(rpt.get("by_symbol", []) or [])
        if bysym:
            st = Table(title="By Symbol", box=box.ROUNDED, style="cyan")
            for c in ("Symbol", "Sent", "Filled", "Skipped", "Blocked"):
                st.add_column(c, style="yellow" if c == "Symbol" else "white")
            for row in bysym[:10]:
                st.add_row(
                    str(row.get("symbol", "-")),
                    str(row.get("sent", 0)),
                    str(row.get("filled", 0)),
                    str(row.get("skipped", 0)),
                    str(row.get("guard_blocked", 0)),
                )
            console.print(st)

        reasons = list(rpt.get("reasons", []) or [])
        if reasons:
            rt = Table(title="Top Reasons", box=box.ROUNDED, style="cyan")
            rt.add_column("Count", style="yellow")
            rt.add_column("Status", style="white")
            rt.add_column("Message", style="white")
            for row in reasons[:12]:
                rt.add_row(str(row.get("count", 0)), str(row.get("status", "-")), str(row.get("message", "-")))
            console.print(rt)

        delta = dict(rpt.get("delta", {}) or {})
        if delta.get("enabled"):
            dt = Table(title="Exec Reasons Delta (Before vs After Patch Marker)", box=box.ROUNDED, style="cyan")
            dt.add_column("Field", style="bold white")
            dt.add_column("Value", style="yellow")
            dt.add_row("marker_utc", str(delta.get("marker_utc", "")))
            pre = dict(delta.get("pre", {}) or {})
            post = dict(delta.get("post", {}) or {})
            dt.add_row("before.total", str(pre.get("total", 0)))
            dt.add_row("after.total", str(post.get("total", 0)))
            console.print(dt)
            changes = list(delta.get("changes", []) or [])
            if changes:
                ct = Table(title="Delta by Reason Bucket", box=box.ROUNDED, style="cyan")
                for c in ("Bucket", "Before", "After", "Delta"):
                    ct.add_column(c, style="yellow" if c == "Bucket" else "white")
                ranked = sorted(changes, key=lambda r: abs(int(r.get("delta", 0) or 0)), reverse=True)
                for row in ranked[:10]:
                    ct.add_row(
                        str(row.get("bucket", "-")),
                        str(row.get("before", 0)),
                        str(row.get("after", 0)),
                        str(row.get("delta", 0)),
                    )
                console.print(ct)

        recs = list(rpt.get("recommendations", []) or [])
        if recs:
            rt2 = Table(title="Tuning Recommendations", box=box.ROUNDED, style="cyan")
            rt2.add_column("Prio", style="yellow")
            rt2.add_column("Code", style="white")
            rt2.add_column("Action", style="white")
            for row in recs[:8]:
                rt2.add_row(str(row.get("priority", "-")), str(row.get("code", "-")), str(row.get("action", "-")))
            console.print(rt2)

        samples = list(rpt.get("samples", []) or [])
        if samples:
            sm = Table(title="Recent Samples", box=box.ROUNDED, style="cyan")
            for c in ("UTC", "Signal", "Broker", "Status", "Message"):
                sm.add_column(c, style="yellow" if c in {"Signal", "Status"} else "white")
            for row in samples[:8]:
                msg = str(row.get("message", "-") or "-")
                conf = row.get("confidence")
                nprob = row.get("neural_prob")
                if conf is not None or nprob is not None:
                    extras = []
                    if conf is not None:
                        extras.append(f"conf={conf}")
                    if nprob is not None:
                        extras.append(f"p={nprob}")
                    msg = f"{msg} ({', '.join(extras)})"
                sm.add_row(
                    str(row.get("created_at", "-")),
                    str(row.get("signal_symbol", "-")),
                    str(row.get("broker_symbol", "-")),
                    str(row.get("status", "-")),
                    msg,
                )
            console.print(sm)
        return

    if action == "scalping_report":
        from learning.scalping_forward import scalping_forward_analyzer

        lookback_days = max(3, min(30, int(days or 7)))
        rpt = scalping_forward_analyzer.build_report(days=lookback_days)
        table = Table(title=f"Scalping Forward Report ({lookback_days}d)", box=box.ROUNDED, style="cyan")
        table.add_column("Field", style="bold white")
        table.add_column("Value", style="yellow")
        table.add_row("ok", str(rpt.get("ok", False)))
        table.add_row("db_path", str(rpt.get("db_path", "")))
        table.add_row("since_utc", str(rpt.get("since_utc", "")))
        table.add_row("rows", str(rpt.get("rows", 0)))
        if rpt.get("error"):
            table.add_row("error", str(rpt.get("error")))
        best = dict(rpt.get("best_pair") or {})
        if best:
            table.add_row("best_pair.symbol", str(best.get("symbol", "-")))
            table.add_row("best_pair.trades", str(best.get("trades", 0)))
            table.add_row("best_pair.win_rate", f"{float(best.get('win_rate', 0.0) or 0.0) * 100:.1f}%")
            table.add_row("best_pair.pnl_net_usd", str(best.get("pnl_net_usd", 0.0)))
        console.print(table)

        rows = list(rpt.get("pairs", []) or [])
        if rows:
            rt = Table(title="By Pair (Net After Real Costs)", box=box.ROUNDED, style="cyan")
            rt.add_column("Pair", style="yellow")
            rt.add_column("Trades", style="white")
            rt.add_column("WinRate", style="green")
            rt.add_column("NetUSD", style="magenta")
            rt.add_column("AvgUSD", style="white")
            rt.add_column("MDD", style="red")
            rt.add_column("AvgDur(min)", style="white")
            rt.add_column("PF", style="white")
            for row in rows[:10]:
                pf = row.get("profit_factor")
                rt.add_row(
                    str(row.get("symbol", "-")),
                    str(row.get("trades", 0)),
                    f"{float(row.get('win_rate', 0.0) or 0.0) * 100:.1f}%",
                    str(row.get("pnl_net_usd", 0.0)),
                    str(row.get("avg_net_usd", 0.0)),
                    str(row.get("max_drawdown_usd", 0.0)),
                    str(row.get("avg_duration_min", 0.0)),
                    ("-" if pf is None else str(pf)),
                )
            console.print(rt)
        return

    if action == "policy":
        from learning.mt5_orchestrator import mt5_orchestrator

        op = (scope or "show").lower()
        if op not in {"show", "set", "reset", "keys", "preset"}:
            op = "show"
        if op == "keys":
            specs = list(mt5_orchestrator.policy_key_specs() or [])
            table = Table(title="MT5 Policy Keys", box=box.ROUNDED, style="cyan")
            table.add_column("Key", style="bold white")
            table.add_column("Type", style="yellow")
            table.add_column("Default", style="white")
            table.add_column("Example", style="green")
            table.add_column("Description", style="magenta")
            for s in specs:
                table.add_row(
                    str(s.get("key", "")),
                    str(s.get("type", "")),
                    str(s.get("default", "")),
                    str(s.get("example", "")),
                    str(s.get("desc", "")),
                )
            console.print(table)
            presets = list(mt5_orchestrator.policy_presets() or [])
            if presets:
                pt = Table(title="MT5 Policy Presets", box=box.ROUNDED, style="cyan")
                pt.add_column("Preset", style="bold white")
                pt.add_column("Description", style="yellow")
                for p in presets:
                    pt.add_row(str(p.get("name", "")), str(p.get("desc", "")))
                console.print(pt)
            return
        if op == "preset":
            preset_name = str(key or value or "").strip()
            if not preset_name:
                console.print("[yellow]Missing preset name. Example: python main.py mt5 policy preset --key micro_safe[/]")
                return
            rep = mt5_orchestrator.apply_current_account_preset(preset_name)
            if not rep.get("ok"):
                console.print(f"[red]Apply preset failed:[/] {rep.get('message')}")
                return
            console.print(f"[green]MT5 policy preset applied[/] {rep.get('account_key')} | {rep.get('preset')}")
            return
        if op == "show":
            rep = mt5_orchestrator.current_account_policy()
            if not rep.get("ok"):
                console.print(f"[red]MT5 policy unavailable:[/] {rep.get('message')}")
                return
            pol = dict(rep.get("policy", {}) or {})
            table = Table(title="MT5 Per-Account Policy", box=box.ROUNDED, style="cyan")
            table.add_column("Field", style="bold white")
            table.add_column("Value", style="yellow")
            table.add_row("account_key", str(rep.get("account_key", "")))
            for k in sorted(pol.keys()):
                table.add_row(str(k), str(pol.get(k)))
            console.print(table)
            console.print("[dim]Set example: python main.py mt5 policy set --key canary_force --value false[/]")
            return
        if op == "reset":
            rep = mt5_orchestrator.reset_current_account_policy()
            if not rep.get("ok"):
                console.print(f"[red]Reset policy failed:[/] {rep.get('message')}")
                return
            console.print(f"[green]MT5 policy reset for {rep.get('account_key')}[/]")
            return
        if not str(key or "").strip():
            console.print("[yellow]Missing --key for mt5 policy set[/]")
            return
        rep = mt5_orchestrator.set_current_account_policy(str(key).strip(), value)
        if not rep.get("ok"):
            console.print(f"[red]Set policy failed:[/] {rep.get('message')}")
            return
        console.print(
            f"[green]MT5 policy updated[/] {rep.get('account_key')} | "
            f"{rep.get('updated_key')}={rep.get('updated_value')}"
        )
        return

    if action == "train":
        from learning.neural_brain import neural_brain

        sync = neural_brain.sync_outcomes_from_mt5(days=max(1, int(sync_days)))
        feedback = neural_brain.sync_signal_outcomes_from_market(
            days=max(1, int(sync_days)),
            max_records=max(50, int(config.NEURAL_BRAIN_SIGNAL_FEEDBACK_MAX_RECORDS)),
        )
        model = neural_brain.model_status()
        train_min_samples = int(config.NEURAL_BRAIN_MIN_SAMPLES)
        if not model.get("available"):
            train_min_samples = min(
                train_min_samples,
                max(10, int(config.NEURAL_BRAIN_BOOTSTRAP_MIN_SAMPLES)),
            )
        train = neural_brain.train_backprop(
            days=max(1, int(days)),
            min_samples=train_min_samples,
        )
        # ── Per-symbol training ───────────────────────────────────────────────
        try:
            from learning.symbol_neural_brain import symbol_neural_brain
            sym_results = symbol_neural_brain.train_all(days=max(1, int(days)))
        except Exception as _e_sym:
            sym_results = {}
            logger.debug("[SymbolBrain] train_all error: %s", _e_sym)
        table = Table(title="Neural Brain Train", box=box.ROUNDED, style="cyan")
        table.add_column("Field", style="bold white")
        table.add_column("Value", style="yellow")
        table.add_row("sync.ok", str(sync.get("ok", False)))
        table.add_row("sync.updated", str(sync.get("updated", 0)))
        table.add_row("sync.closed_positions", str(sync.get("closed_positions", 0)))
        table.add_row("feedback.ok", str(feedback.get("ok", False)))
        table.add_row("feedback.reviewed", str(feedback.get("reviewed", 0)))
        table.add_row("feedback.resolved", str(feedback.get("resolved", 0)))
        table.add_row("feedback.pseudo_labeled", str(feedback.get("pseudo_labeled", 0)))
        table.add_row("train.min_samples", str(train_min_samples))
        table.add_row("train.ok", str(train.ok))
        table.add_row("train.status", str(train.status))
        table.add_row("train.message", str(train.message))
        table.add_row("samples", str(train.samples))
        if train.ok:
            table.add_row("train_accuracy", f"{train.train_accuracy * 100:.1f}%")
            table.add_row("val_accuracy", f"{train.val_accuracy * 100:.1f}%")
            table.add_row("win_rate", f"{train.win_rate * 100:.1f}%")
        # Per-symbol summary
        sym_ok = [k for k, r in sym_results.items() if r.ok]
        sym_skip = [k for k, r in sym_results.items() if not r.ok]
        table.add_row("symbol_models.trained", str(len(sym_ok)))
        table.add_row("symbol_models.skipped", str(len(sym_skip)))
        if sym_ok:
            table.add_row("symbol_models.keys", ", ".join(sorted(sym_ok)[:8]))
        console.print(table)
        return

    if action == "mission":
        from learning.mt5_neural_mission import mt5_neural_mission

        cycle = 0
        while True:
            cycle += 1
            rpt = mt5_neural_mission.run(
                symbols=str(symbols or "XAUUSD,ETHUSD,BTCUSD,GBPUSD"),
                iterations=max(1, int(iterations)),
                train_days=max(1, int(days)),
                backtest_days=max(1, int(days)),
                sync_days=max(1, int(sync_days)),
                target_win_rate=float(target_win_rate),
                target_profit_factor=float(target_profit_factor),
                min_trades=max(3, int(min_trades)),
                apply_policy_draft=bool(draft),
            )

            summary = Table(title="MT5 Neural Mission", box=box.ROUNDED, style="cyan")
            summary.add_column("Field", style="bold white")
            summary.add_column("Value", style="yellow")
            summary.add_row("cycle", str(cycle))
            summary.add_row("ok", str(rpt.get("ok", False)))
            summary.add_row("goal_met", str(rpt.get("goal_met", False)))
            summary.add_row("iterations_done", str(rpt.get("iterations_done", 0)))
            summary.add_row("symbols", ", ".join(list(rpt.get("symbols", []) or [])))
            target = dict(rpt.get("target", {}) or {})
            summary.add_row("target.win_rate_pct", str(target.get("win_rate_pct", "")))
            summary.add_row("target.profit_factor", str(target.get("profit_factor", "")))
            summary.add_row("target.min_trades", str(target.get("min_trades", "")))
            summary.add_row("report_path", str(rpt.get("report_path", "")))
            dr = dict(rpt.get("policy_draft_result", {}) or {})
            if dr:
                summary.add_row("draft.saved", str(dr.get("ok", False)))
                summary.add_row("draft.account_key", str(dr.get("account_key", "")))
            console.print(summary)

            final = dict(rpt.get("final", {}) or {})
            recs = dict(final.get("recommendations", {}) or {})
            if recs:
                rt = Table(title="Per-Symbol Recommendations", box=box.ROUNDED, style="cyan")
                for c in ("Symbol", "Status", "Pass", "MinProb", "RiskMin", "RiskMax", "Canary", "TP/SL Profile"):
                    rt.add_column(c, style="yellow" if c == "Symbol" else "white")
                for sym in sorted(recs.keys()):
                    rec = dict(recs.get(sym, {}) or {})
                    tp = dict(rec.get("tp_sl_profile", {}) or {})
                    rt.add_row(
                        str(sym),
                        str(rec.get("status", "")),
                        str(rec.get("target_pass", False)),
                        str(rec.get("neural_min_prob", "")),
                        str(rec.get("risk_multiplier_min", "")),
                        str(rec.get("risk_multiplier_max", "")),
                        str(rec.get("canary_force", "")),
                        str(tp.get("profile", "balanced")),
                    )
                console.print(rt)

            auto_allow = dict(final.get("auto_allowlist", {}) or {})
            if auto_allow:
                at = Table(title="Auto-Allowlist From Backtest", box=box.ROUNDED, style="cyan")
                at.add_column("Field", style="bold white")
                at.add_column("Value", style="yellow")
                at.add_row("enabled", str(auto_allow.get("enabled", False)))
                at.add_row("status", str(auto_allow.get("status", "")))
                cr = dict(auto_allow.get("criteria", {}) or {})
                if cr:
                    at.add_row(
                        "criteria",
                        (
                            f"trades>={cr.get('min_trades','-')}, "
                            f"wr>={cr.get('min_win_rate','-')}%, "
                            f"pf>={cr.get('min_profit_factor','-')}, "
                            f"net>={cr.get('min_net_pnl','-')}, "
                            f"max_add={cr.get('max_add_per_cycle','-')}"
                        ),
                    )
                added = list(auto_allow.get("added_symbols", []) or [])
                at.add_row("added_symbols", ", ".join([str(x) for x in added]) if added else "-")
                console.print(at)

            ob = dict(final.get("override_bundle", {}) or {})
            env_lines = dict(ob.get("env_lines", {}) or {})
            if env_lines:
                et = Table(title="Suggested ENV Overrides", box=box.ROUNDED, style="cyan")
                et.add_column("Key", style="bold white")
                et.add_column("Value", style="yellow")
                for k in sorted(env_lines.keys()):
                    et.add_row(str(k), str(env_lines.get(k, "")))
                console.print(et)

            if not bool(continuous):
                break
            if bool(rpt.get("goal_met", False)):
                console.print("[green]Neural mission goal reached. Continuous loop stopped.[/]")
                break
            if int(max_cycles or 0) > 0 and cycle >= int(max_cycles):
                console.print("[yellow]Reached --max-cycles limit. Continuous loop stopped.[/]")
                break
            wait_min = max(1, int(interval_min or 30))
            console.print(f"[dim]Waiting {wait_min} minute(s) before next mission cycle...[/]")
            time.sleep(wait_min * 60)
        return


    console.print("[yellow]Unknown mt5 action. Use: status | symbols | bootstrap | backtest | brain | train | mission | autopilot | orchestrator | walkforward | manage | exec_reasons | scalping_report | policy[/]")


def cmd_billing(action: str = "status"):
    """Billing webhook server controls."""
    from notifier.billing_webhook import billing_webhook_server

    console.print(Panel(BANNER, border_style="cyan"))
    action = (action or "status").lower()

    if action == "status":
        state = billing_webhook_server.status()
        table = Table(title="Billing Webhook Status", box=box.ROUNDED, style="cyan")
        table.add_column("Field", style="bold white")
        table.add_column("Value", style="yellow")
        for key in ("enabled", "running", "host", "port", "stripe_enabled", "promptpay_enabled"):
            table.add_row(key, str(state.get(key)))
        table.add_row("stripe_path", "/webhook/stripe")
        table.add_row("promptpay_path", "/webhook/promptpay")
        table.add_row("health_path", "/health")
        console.print(table)
        return

    if action == "start":
        if not billing_webhook_server.start():
            console.print("[red]Billing webhook failed to start.[/]")
            return
        state = billing_webhook_server.status()
        console.print(
            f"[green]Billing webhook listening on {state.get('host')}:{state.get('port')}[/]\n"
            "[dim]Endpoints: /webhook/stripe, /webhook/promptpay, /health[/]\n"
            "[dim]Press Ctrl+C to stop[/]"
        )
        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            pass
        finally:
            billing_webhook_server.stop()
        return

    console.print("[yellow]Unknown billing action. Use: status | start[/]")


# ─── Main ─────────────────────────────────────────────────────────────────────


def cmd_setup():
    """Interactive setup wizard: validates config and auto-detects Telegram Chat ID."""
    import requests
    from pathlib import Path

    console.print(Panel(BANNER, border_style="cyan"))
    console.print("\n[bold cyan]⚙️  DEXTER PRO SETUP WIZARD[/]\n")

    env_path = Path(__file__).parent / ".env.local"

    # ── 1. Check .env.local exists ────────────────────────────────────────────
    if env_path.exists():
        console.print(f"[green]✅ .env.local found:[/] {env_path}")
    else:
        console.print(f"[red]❌ .env.local NOT found at {env_path}[/]")
        console.print("[yellow]   Create it from the template in the zip package.[/]")
        return

    # ── 2. Validate each config key ───────────────────────────────────────────
    console.print()
    table = Table(title="Config Validation", box=box.ROUNDED, style="cyan")
    table.add_column("Key",    style="bold white", width=30)
    table.add_column("Status", style="bold",       width=10)
    table.add_column("Value / Hint")

    checks = [
        ("GROQ_API_KEY",       config.GROQ_API_KEY,       "gsk_..."),
        ("GEMINI_API_KEY",     config.GEMINI_API_KEY,     "AIza..."),
        ("GEMINI_VERTEX_AI_API_KEY", config.GEMINI_VERTEX_AI_API_KEY, "AQ...."),
        ("ANTHROPIC_API_KEY",  config.ANTHROPIC_API_KEY,  "sk-ant-..."),
        ("TELEGRAM_BOT_TOKEN", config.TELEGRAM_BOT_TOKEN, "token"),
        ("TELEGRAM_CHAT_ID",   config.TELEGRAM_CHAT_ID,   "numeric ID"),
        ("CRYPTO_EXCHANGE",    config.CRYPTO_EXCHANGE,     ""),
        ("MIN_SIGNAL_CONFIDENCE", str(config.MIN_SIGNAL_CONFIDENCE), ""),
    ]
    ai_keys = {"GROQ_API_KEY", "GEMINI_API_KEY", "GEMINI_VERTEX_AI_API_KEY", "ANTHROPIC_API_KEY"}
    ai_any = bool(
        config.GROQ_API_KEY
        or config.GEMINI_API_KEY
        or config.GEMINI_VERTEX_AI_API_KEY
        or config.ANTHROPIC_API_KEY
    )
    for key, val, hint in checks:
        is_placeholder = (not val
                          or "your_" in val.lower()
                          or val == hint)
        if key in ai_keys and is_placeholder and ai_any:
            table.add_row(key, "[white]• OPTIONAL[/]",
                          "[dim]At least one AI provider key is already configured[/]")
            continue
        if is_placeholder:
            table.add_row(key, "[red]❌ MISSING[/]",
                          f"[dim]→ edit .env.local: {key}=...[/]")
        else:
            masked = (val[:8] + "..." + val[-4:]) if len(val) > 16 else val
            table.add_row(key, "[green]✅ OK[/]", f"[dim]{masked}[/]")
    console.print(table)

    # ── 3. Auto-detect Telegram Chat ID ──────────────────────────────────────
    console.print()
    bot_token = config.TELEGRAM_BOT_TOKEN
    chat_id_set = (config.TELEGRAM_CHAT_ID
                   and "your_" not in config.TELEGRAM_CHAT_ID.lower())

    if not chat_id_set and bot_token and "your_" not in bot_token.lower():
        console.print("[cyan]🔍 Auto-detecting your Telegram Chat ID...[/]")
        console.print("[dim]   → Send ANY message to @mrgeon8n_bot on Telegram first,[/]")
        console.print("[dim]     then press Enter here.[/]")
        input("   Press Enter after sending a message to the bot: ")

        try:
            url = f"https://api.telegram.org/bot{bot_token}/getUpdates"
            resp = requests.get(url, timeout=10)
            data = resp.json()
            if data.get("ok") and data.get("result"):
                last = data["result"][-1]
                chat = last.get("message", {}).get("chat", {})
                detected_id = str(chat.get("id", ""))
                username    = chat.get("username", "")
                first_name  = chat.get("first_name", "")
                if detected_id:
                    console.print(f"\n[bold green]🎉 Chat ID detected: {detected_id}[/]")
                    if username:
                        console.print(f"   Username: @{username}")
                    if first_name:
                        console.print(f"   Name: {first_name}")
                    console.print()

                    # Write it into .env.local automatically
                    env_text = env_path.read_text(encoding="utf-8")
                    env_text = env_text.replace(
                        "TELEGRAM_CHAT_ID=your_telegram_chat_id_here",
                        f"TELEGRAM_CHAT_ID={detected_id}",
                    )
                    env_path.write_text(env_text, encoding="utf-8")
                    console.print(
                        f"[green]✅ TELEGRAM_CHAT_ID={detected_id} saved to .env.local[/]"
                    )
                else:
                    console.print("[yellow]⚠️  Could not extract Chat ID from response.[/]")
                    console.print(f"[dim]   Raw: {data['result'][-1]}[/]")
            else:
                console.print("[yellow]⚠️  No messages found. Make sure you sent a message to @mrgeon8n_bot[/]")
                console.print(f"[dim]   Response: {data}[/]")
        except Exception as e:
            console.print(f"[red]❌ Failed to reach Telegram API: {e}[/]")
    elif chat_id_set:
        console.print(f"[green]✅ TELEGRAM_CHAT_ID already set: {config.TELEGRAM_CHAT_ID}[/]")

    # ── 4. Test Telegram connection ───────────────────────────────────────────
    console.print()
    final_chat = config.TELEGRAM_CHAT_ID
    if (final_chat and "your_" not in final_chat.lower()
            and bot_token and "your_" not in bot_token.lower()):
        console.print("[cyan]📨 Sending test message to Telegram...[/]")
        try:
            from notifier.telegram_bot import notifier
            # Re-read config after .env.local update
            from dotenv import load_dotenv
            load_dotenv(env_path, override=True)
            import os
            config.TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", final_chat)

            ok = notifier._send(
                "🦞 *DEXTER PRO* \u2014 Setup complete\\! Bot connected successfully\\."
            )
            if ok:
                console.print("[bold green]✅ Test message sent! Check your Telegram.[/]")
            else:
                console.print("[red]❌ Message failed — check your Chat ID in .env.local[/]")
        except Exception as e:
            console.print(f"[red]❌ Telegram test error: {e}[/]")
    else:
        console.print(
            "[yellow]⚠️  Skipping Telegram test — fill TELEGRAM_CHAT_ID in .env.local first[/]"
        )

    # ── 5. Summary ────────────────────────────────────────────────────────────
    console.print()
    console.print(Panel(
        "[bold green]Setup complete![/]\n\n"
        "Next steps:\n"
        "  1. Set at least one AI key: [bold]GROQ_API_KEY[/] or [bold]GEMINI_API_KEY[/] or [bold]GEMINI_VERTEX_AI_API_KEY[/] or [bold]ANTHROPIC_API_KEY[/]\n"
        "  2. Make sure [bold]TELEGRAM_CHAT_ID[/] is set in .env.local\n"
        "  3. Run: [bold cyan]python main.py monitor[/]",
        title="✅ Done",
        border_style="green",
    ))

def main():
    setup_logging()

    parser = argparse.ArgumentParser(
        description="🦞 Dexter Pro — AI Trading Agent | XAUUSD + Crypto Sniper",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python main.py monitor                        # Start 24/7 monitor
  python main.py scan all                       # One-time full scan
  python main.py scan gold                      # Scan gold only
  python main.py scan crypto                    # Scan crypto only
  python main.py scan fx                        # Scan FX majors only
  python main.py scan calendar                  # Upcoming economic events
  python main.py scan macro                     # Macro policy risk headlines
  python main.py scan macro_report              # Post-news impact tracker report
  python main.py scan macro_weights             # Adaptive macro theme weights
  python main.py scan vi                        # US value + trend candidates
  python main.py scan us_open                   # US open top-10 daytrade plan
  python main.py scan us_open_monitor           # US open smart monitor snapshot
  python main.py scan scalping                  # Dedicated scalping (M5+M1) XAUUSD/ETH
  python main.py research "Is gold bullish?"    # AI research
  python main.py overview                       # Gold market overview
  python main.py status                         # System status
  python main.py mt5 status                     # MT5 bridge status
  python main.py mt5 symbols                    # MT5 broker symbols preview
  python main.py mt5 bootstrap                  # Suggest MT5_SYMBOL_MAP from broker symbols
  python main.py mt5 backtest --days 30         # MT5 strategy backtest from logged outcomes
  python main.py mt5 train --days 120           # Train backprop neural model
  python main.py mt5 brain                      # Neural model status
  python main.py mt5 autopilot                  # MT5 risk governor + forward-test journal
  python main.py mt5 orchestrator               # Multi-account policy/canary status
  python main.py mt5 walkforward                # Walk-forward validation + canary
  python main.py mt5 manage                     # Run one position-manager cycle now
  python main.py mt5 pm_learning --days 30      # PM action effectiveness report
  python main.py mt5 affordable fx --top 10     # live affordable symbols for this account
  python main.py mt5 affordable ok --top 10     # only symbols passing margin+spread+policy
  python main.py mt5 exec_reasons --symbol ETHUSD --days 1   # why orders were skipped/filled
  python main.py mt5 scalping_report --days 7   # net-after-cost forward summary (scalping only)
  python main.py mt5 mission --symbols XAUUSD,ETHUSD,BTCUSD,GBPUSD --iterations 3 --days 120
  python main.py mt5 mission --continuous --interval-min 30 --max-cycles 0
  python main.py mt5 policy show                # Show per-account canary/risk policy
  python main.py mt5 policy set --key canary_force --value false
  python main.py mt5 policy reset               # Reset per-account policy to defaults
  python main.py billing status                 # Billing webhook status
  python main.py billing start                  # Start webhook server (Stripe/PromptPay)
        """,
    )

    subparsers = parser.add_subparsers(dest="command", help="Command to run")

    # monitor
    subparsers.add_parser("monitor", help="Start 24/7 monitoring with Telegram alerts")

    # scan
    scan_parser = subparsers.add_parser("scan", help="Run one-time market scan")
    scan_parser.add_argument(
        "target",
        nargs="?",
        default="all",
        choices=["all", "gold", "xauusd", "crypto", "fx", "stocks", "thai", "thai_vi", "us", "us_open", "us_open_monitor", "calendar", "macro", "macro_report", "macro_weights", "vi", "vi_buffett", "vi_turnaround", "scalp", "scalping"],
        help="What to scan",
    )

    # stocks
    stocks_parser = subparsers.add_parser("stocks", help="Scan global stock markets")
    stocks_parser.add_argument(
        "market",
        nargs="?",
        default="all",
        choices=["all", "us", "uk", "de", "jp", "hk", "thai", "thai_vi", "sg", "in", "au", "priority", "vi", "vi_buffett", "vi_turnaround"],
        help="Which market to scan",
    )

    # markets
    subparsers.add_parser("markets", help="Show global market hours status")

    # research
    research_parser = subparsers.add_parser("research", help="Deep AI financial research")
    research_parser.add_argument("question", nargs="+", help="Research question")

    # overview
    subparsers.add_parser("overview", help="XAUUSD market overview")

    # status
    subparsers.add_parser("status", help="System status check")

    # setup
    subparsers.add_parser("setup", help="Interactive setup: validate config & get Telegram Chat ID")

    # mt5
    mt5_parser = subparsers.add_parser("mt5", help="MT5 bridge diagnostics")
    mt5_parser.add_argument(
        "action",
        nargs="?",
        default="status",
        choices=["status", "symbols", "bootstrap", "backtest", "brain", "train", "mission", "autopilot", "orchestrator", "walkforward", "manage", "affordable", "exec_reasons", "scalping_report", "pm_learning", "policy"],
        help="MT5 diagnostic action",
    )
    mt5_parser.add_argument(
        "scope",
        nargs="?",
        default="quick",
        choices=["quick", "all", "show", "set", "reset", "watch", "keys", "preset", "nosync", "ok", "crypto", "fx", "metal", "index"],
        help="Bootstrap scope (used by: mt5 bootstrap)",
    )
    mt5_parser.add_argument(
        "--days",
        type=int,
        default=30,
        help="History window days for mt5 backtest/train",
    )
    mt5_parser.add_argument(
        "--sync-days",
        type=int,
        default=120,
        help="History window days for MT5 outcome sync",
    )
    mt5_parser.add_argument(
        "--symbols",
        default="XAUUSD,ETHUSD,BTCUSD,GBPUSD",
        help="Comma-separated symbols for 'mt5 mission'",
    )
    mt5_parser.add_argument(
        "--iterations",
        type=int,
        default=3,
        help="Max optimization iterations for 'mt5 mission'",
    )
    mt5_parser.add_argument(
        "--target-win-rate",
        type=float,
        default=58.0,
        help="Target win rate percent for 'mt5 mission'",
    )
    mt5_parser.add_argument(
        "--target-profit-factor",
        type=float,
        default=1.2,
        help="Target profit factor for 'mt5 mission'",
    )
    mt5_parser.add_argument(
        "--min-trades",
        type=int,
        default=12,
        help="Minimum completed trades per symbol for mission pass criteria",
    )
    mt5_parser.add_argument(
        "--continuous",
        action="store_true",
        help="For 'mt5 mission': keep running cycles until goal is met (or max-cycles reached)",
    )
    mt5_parser.add_argument(
        "--interval-min",
        type=int,
        default=30,
        help="Minutes between continuous mission cycles",
    )
    mt5_parser.add_argument(
        "--max-cycles",
        type=int,
        default=0,
        help="Optional safety cap for continuous mission cycles (0 = unlimited)",
    )
    mt5_parser.add_argument(
        "--key",
        default="",
        help="Policy key for 'mt5 policy set'",
    )
    mt5_parser.add_argument(
        "--value",
        default="",
        help="Policy value for 'mt5 policy set'",
    )
    mt5_parser.add_argument(
        "--symbol",
        default="",
        help="Filter symbol for 'mt5 pm_learning' (e.g. ETHUSD)",
    )
    mt5_parser.add_argument(
        "--pm-action",
        default="",
        help="Filter PM action for 'mt5 pm_learning' (e.g. trail_sl)",
    )
    mt5_parser.add_argument(
        "--top",
        type=int,
        default=8,
        help="Top rows for 'mt5 pm_learning' report",
    )
    mt5_parser.add_argument(
        "--ok-only",
        action="store_true",
        help="For 'mt5 affordable': show only symbols passing margin+spread+policy",
    )
    mt5_parser.add_argument(
        "--draft",
        action="store_true",
        help="Save PM learning recommendations as per-account policy draft (not applied)",
    )

    # billing
    billing_parser = subparsers.add_parser("billing", help="Billing webhook server")
    billing_parser.add_argument(
        "action",
        nargs="?",
        default="status",
        choices=["status", "start"],
        help="Billing action",
    )

    args = parser.parse_args()

    if args.command is None:
        parser.print_help()
        sys.exit(0)

    if args.command == "monitor":
        cmd_monitor()
    elif args.command == "scan":
        target = args.target
        if target in ("gold", "xauusd"):
            target = "xauusd"
        cmd_scan(target)
    elif args.command == "stocks":
        cmd_stocks(getattr(args, "market", "all"))
    elif args.command == "markets":
        cmd_markets()
    elif args.command == "research":
        question = " ".join(args.question)
        cmd_research(question)
    elif args.command == "overview":
        cmd_overview()
    elif args.command == "status":
        cmd_status()
    elif args.command == "setup":
        cmd_setup()
    elif args.command == "mt5":
        cmd_mt5(
            getattr(args, "action", "status"),
            getattr(args, "scope", "quick"),
            getattr(args, "days", 30),
            getattr(args, "sync_days", 120),
            getattr(args, "symbols", "XAUUSD,ETHUSD,BTCUSD,GBPUSD"),
            getattr(args, "iterations", 3),
            getattr(args, "target_win_rate", 58.0),
            getattr(args, "target_profit_factor", 1.2),
            getattr(args, "min_trades", 12),
            bool(getattr(args, "continuous", False)),
            getattr(args, "interval_min", 30),
            getattr(args, "max_cycles", 0),
            getattr(args, "key", ""),
            getattr(args, "value", ""),
            getattr(args, "symbol", ""),
            getattr(args, "pm_action", ""),
            getattr(args, "top", 8),
            bool(getattr(args, "draft", False)),
            bool(getattr(args, "ok_only", False)),
        )
    elif args.command == "billing":
        cmd_billing(getattr(args, "action", "status"))
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
