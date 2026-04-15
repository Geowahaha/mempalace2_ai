"""
Mempalace2 AI — Main entry point.

Usage:
  python -m mempalace2_ai                          # Boot with default config
  python -m mempalace2_ai --config config/settings.yaml
  python -m mempalace2_ai --symbols XAUUSD --timeframe 1h
  python -m mempalace2_ai --backtest --from 2024-01-01 --to 2025-01-01
"""

import argparse
import asyncio
import json
import logging
import sys

from core.boot import BootPipeline


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

    # Boot the system
    pipeline = BootPipeline(config_path=args.config)
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
    print("=" * 70)
    print(f"  Session: {dashboard['session_id']}")
    print(f"  Symbols: {', '.join(dashboard['symbols'])}")
    print(f"  Agents:  {len(dashboard['agents'])} active")
    print(f"  Equity:  ${dashboard['portfolio']['equity']:,.2f}")
    print("=" * 70)

    # Keep running
    try:
        logger.info("System running. Press Ctrl+C to stop.")
        while True:
            await asyncio.sleep(10)
            # Print periodic status
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
    except KeyboardInterrupt:
        print("\n\nShutting down...")
        await state.coordinator.shutdown()
        logger.info("System stopped.")


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

    args = parser.parse_args()
    asyncio.run(main(args))
