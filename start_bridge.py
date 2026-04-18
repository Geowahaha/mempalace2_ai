"""
start_bridge.py - Launch the Tiger Bridge API server
Run this to start the HTTP + WebSocket bridge on port 8787.
The Web3 dashboard connects to this server for live signals.
"""
import asyncio
import logging
import sys
import os

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
)
logger = logging.getLogger("TigerBridge")


async def main():
    from api.bridge_server import bridge_server

    logger.info("🐯 Starting Tiger Bridge API...")
    logger.info("   HTTP:  http://localhost:8788/api/status")
    logger.info("   WS:    ws://localhost:8788/ws/signals")
    logger.info("   Press Ctrl+C to stop")

    await bridge_server.start()

    # Keep running
    try:
        while True:
            await asyncio.sleep(3600)
    except (KeyboardInterrupt, asyncio.CancelledError):
        logger.info("🐯 Shutting down Tiger Bridge...")
        await bridge_server.stop()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n🐯 Tiger Bridge stopped.")
