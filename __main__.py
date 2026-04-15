"""
Mempalace2 AI — Main entry point.

Usage:
  python -m mempalace2_ai                          # Boot with default config
  python -m mempalace2_ai --enhanced               # Boot with hermes-agent integration
  python -m mempalace2_ai --config config/settings.yaml
  python -m mempalace2_ai --symbols XAUUSD --timeframe 1h
  python -m mempalace2_ai --backtest --from 2024-01-01 --to 2025-01-01
  python -m mempalace2_ai --enhanced --trajectory-out trajectories/ --memory-db ~/.mempalace2/state.db
"""

import argparse
import asyncio
import json
import logging
import sys


def setup_logging(level: str = "INFO"):
    """Configure logging."""
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s │ %(name)-28s │ %(levelname)-5s │ %(message)s",
        datefmt="%H:%M:%S",
    )


async def main(args):
    """Main entry point."""
    setup_logging(args.log_level)
    logger = logging.getLogger("mempalace2")

    # Choose boot pipeline
    if args.enhanced:
        from enhanced.boot import EnhancedBootPipeline
        pipeline = EnhancedBootPipeline(
            config_path=args.config,
            db_path=args.memory_db,
            trajectory_dir=args.trajectory_out,
        )
        logger.info("Booting with hermes-agent integration (enhanced mode)")
    else:
        from core.boot import BootPipeline
        pipeline = BootPipeline(config_path=args.config)
        logger.info("Booting with standard pipeline")

    state = await pipeline.boot()

    # Override symbols if provided
    if args.symbols:
        state.config.symbols = args.symbols.split(",")

    # Start the trading pipeline
    await state.coordinator.start_pipeline()

    # Show dashboard
    dashboard = state.coordinator.get_dashboard()
    print("\n" + "=" * 70)
    print("  🏛️  MEMPALACE2 AI — TRADING SYSTEM ACTIVE")
    if args.enhanced:
        print("  ⚡ Enhanced Mode (hermes-agent integration)")
    print("=" * 70)
    print(f"  Session: {dashboard['session_id']}")
    print(f"  Symbols: {', '.join(dashboard['symbols'])}")
    print(f"  Agents:  {len(dashboard['agents'])} active")
    print(f"  Equity:  ${dashboard['portfolio']['equity']:,.2f}")

    # Enhanced mode stats
    if args.enhanced:
        _print_enhanced_stats(state)

    print("=" * 70)

    # Keep running
    try:
        logger.info("System running. Press Ctrl+C to stop.")
        status_interval = 0
        while True:
            await asyncio.sleep(10)
            status_interval += 1
            d = state.coordinator.get_dashboard()
            if d["activity"]["total_signals"] > 0:
                print(
                    f"\r  📊 Analyses: {d['activity']['total_analyses']} | "
                    f"Signals: {d['activity']['total_signals']} | "
                    f"Trades: {d['activity']['total_trades']} | "
                    f"P&L: {d['portfolio']['total_pnl']}",
                    end="",
                    flush=True,
                )
            # Periodic enhanced status (every 60s)
            if args.enhanced and status_interval % 6 == 0:
                _update_enhanced_stats(state)
    except KeyboardInterrupt:
        print("\n\nShutting down...")
        await _shutdown_enhanced(state, args.enhanced)
        logger.info("System stopped.")


def _print_enhanced_stats(state):
    """Print enhanced boot statistics."""
    parts = []

    if hasattr(state, 'state_store') and state.state_store:
        parts.append(f"  Memory DB: {state.state_store.db_path}")

    if hasattr(state, 'memory') and state.memory:
        stats = state.memory.get_memory_stats()
        parts.append(f"  Patterns: {stats.get('patterns_stored', 0)} | "
                      f"Lessons: {stats.get('lessons_stored', 0)}")

    if hasattr(state, 'skills_manager') and state.skills_manager:
        stats = state.skills_manager.get_stats()
        parts.append(f"  Skills: {stats.get('total', 0)} loaded")

    if hasattr(state, 'trajectory_logger') and state.trajectory_logger:
        stats = state.trajectory_logger.get_stats()
        parts.append(f"  Trajectories: {stats.get('total', 0)} logged")

    if hasattr(state, 'scheduler') and state.scheduler:
        reports = state.scheduler.list_reports()
        parts.append(f"  Scheduled Reports: {len(reports)}")

    if parts:
        print("  ── Enhanced Components ──")
        for p in parts:
            print(p)


def _update_enhanced_stats(state):
    """Update enhanced stats display."""
    parts = []
    if hasattr(state, 'memory') and state.memory:
        stats = state.memory.get_memory_stats()
        parts.append(f"Patterns: {stats.get('patterns_stored', 0)}")
    if hasattr(state, 'trajectory_logger') and state.trajectory_logger:
        stats = state.trajectory_logger.get_stats()
        parts.append(f"Trajectories: {stats.get('total', 0)}")
    if parts:
        logger.debug(f"Enhanced stats: {', '.join(parts)}")


async def _shutdown_enhanced(state, is_enhanced: bool):
    """Graceful shutdown with enhanced component cleanup."""
    if is_enhanced:
        # Stop scheduler
        if hasattr(state, 'scheduler') and state.scheduler:
            await state.scheduler.stop()
            logging.getLogger("mempalace2").info("Scheduler stopped")

        # Finalize active trajectories
        if hasattr(state, 'trajectory_logger') and state.trajectory_logger:
            state.trajectory_logger.flush_active()
            logging.getLogger("mempalace2").info(
                f"Trajectories flushed: {state.trajectory_logger.get_stats()}"
            )

        # End state store session
        if hasattr(state, 'state_store') and state.state_store:
            if hasattr(state, 'state_store_session_id'):
                stats = {}
                if hasattr(state, 'portfolio'):
                    stats = {
                        "total_trades": state.portfolio.open_positions + len(state.portfolio.closed_trades),
                        "total_pnl": state.portfolio.total_pnl,
                        "win_rate": state.portfolio.win_rate,
                        "max_drawdown_pct": state.portfolio.max_drawdown_pct,
                        "sharpe_ratio": state.portfolio.sharpe_ratio,
                    }
                state.state_store.end_session(state.state_store_session_id, stats)
            logging.getLogger("mempalace2").info("State store session ended")

        # Stop context engine
        if hasattr(state, 'context_engine') and state.context_engine:
            state.context_engine.on_session_end(state.session_id)

    # Standard shutdown
    await state.coordinator.shutdown()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Mempalace2 AI — Intelligent Multi-Agent Trading System"
    )
    parser.add_argument("--config", type=str, default="config/settings.yaml",
                        help="Path to config file")
    parser.add_argument("--symbols", type=str, default=None,
                        help="Comma-separated symbols (e.g., XAUUSD,EURUSD)")
    parser.add_argument("--timeframe", type=str, default="1h",
                        help="Primary timeframe")
    parser.add_argument("--log-level", type=str, default="INFO",
                        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
                        help="Log level")
    parser.add_argument("--backtest", action="store_true",
                        help="Run in backtest mode")
    parser.add_argument("--from", dest="date_from", type=str, default=None,
                        help="Backtest start date")
    parser.add_argument("--to", dest="date_to", type=str, default=None,
                        help="Backtest end date")
    parser.add_argument("--enhanced", action="store_true",
                        help="Enable hermes-agent integration (memory, skills, trajectories)")
    parser.add_argument("--memory-db", type=str, default=None,
                        help="Path to SQLite state store database")
    parser.add_argument("--trajectory-out", type=str, default="trajectories/",
                        help="Directory for trajectory JSONL output")

    args = parser.parse_args()
    asyncio.run(main(args))
