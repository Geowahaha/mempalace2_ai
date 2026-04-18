---
name: dexter-pro-trading
description: >
  Dexter Pro — AI-powered trading agent for XAUUSD (Gold) and Crypto markets.
  Scans for high-probability sniper entries using Smart Money Concepts (SMC),
  multi-timeframe technical analysis, and Claude AI research.
  Sends signals, market overviews, and deep research answers via Telegram.
version: 1.0.0
author: Dexter Pro Team
---

# Dexter Pro Trading Agent — OpenClaw Skill

## Overview

Dexter Pro is a professional autonomous trading agent that runs on your machine 24/7 via OpenClaw.
It monitors **XAUUSD (Gold)** and the **Top 50 Crypto pairs** for high-probability trade setups,
delivers professional Telegram signals with entry/SL/TP/RR, and can answer any financial research
question using Claude AI with live market data.

## Setup Instructions

### 1. Install Dependencies
```bash
cd /path/to/dexter_pro
pip install -r requirements.txt
```

### 2. Configure Environment
```bash
cp .env.example .env
# Edit .env with your API keys:
# - ANTHROPIC_API_KEY (required)
# - TELEGRAM_BOT_TOKEN (required for alerts)
# - TELEGRAM_CHAT_ID (required for alerts)
# - BINANCE_API_KEY (optional - only needed for trade execution)
```

### 3. Get a Telegram Bot Token
1. Message @BotFather on Telegram
2. Send `/newbot` and follow prompts
3. Copy the token to `TELEGRAM_BOT_TOKEN`
4. Get your Chat ID: message @userinfobot or use `getUpdates` API

## Usage via OpenClaw

Once set up, you can control Dexter Pro directly from WhatsApp/Telegram by asking your OpenClaw:

```
"Start Dexter Pro in monitor mode"
→ Runs: python main.py monitor

"Scan gold for trading opportunities right now"
→ Runs: python main.py scan gold

"Scan all crypto for sniper entries"
→ Runs: python main.py scan crypto

"Run a full market scan"
→ Runs: python main.py scan all

"Show me the gold market overview"
→ Runs: python main.py overview

"Research: Is Bitcoin in a bull cycle?"
→ Runs: python main.py research "Is Bitcoin in a bull cycle?"

"What's the status of Dexter Pro?"
→ Runs: python main.py status
```

## Commands Reference

| Command | Description |
|---------|-------------|
| `python main.py monitor` | Start 24/7 auto-scanning with Telegram alerts |
| `python main.py scan all` | One-time full scan (gold + crypto) |
| `python main.py scan gold` | Scan XAUUSD only |
| `python main.py scan crypto` | Scan top 50 crypto pairs |
| `python main.py overview` | XAUUSD market overview |
| `python main.py research "question"` | Deep AI research |
| `python main.py status` | System status & stats |

## Signal Format

Dexter Pro sends signals in this format:

```
══════════════════════════════════════
⚡ DEXTER PRO SIGNAL 🔥🔥🔥
══════════════════════════════════════

Symbol:    XAUUSD
Direction: 🟢 LONG
Pattern:   Bullish OB + ChoCH + BOS
Timeframe: 1h
Session:   london, new_york

──────────────────────────────
📊 TRADE LEVELS
──────────────────────────────
🎯 Entry:     2645.50
🛑 Stop:      2632.80
✅ TP1 (1R):  2658.20
✅ TP2 (2R):  2670.90
🚀 TP3 (3R):  2683.60

⚖️ R:R Ratio: 1:2.5
──────────────────────────────
🎯 CONFIDENCE
██████████░  82%
══════════════════════════════════════
```

## Architecture

```
dexter_pro/
├── main.py                  # CLI entry point
├── config.py                # All configuration
├── scheduler.py             # Background scan scheduler
├── market/
│   └── data_fetcher.py     # XAUUSD (yfinance) + Crypto (CCXT) data
├── analysis/
│   ├── technical.py        # EMA, RSI, MACD, ATR, BB, Stoch, Pivots
│   ├── smc.py              # SMC: Order Blocks, FVG, BOS/ChoCH
│   └── signals.py          # Multi-factor signal scoring engine
├── scanners/
│   ├── xauusd.py           # Gold multi-TF scanner
│   └── crypto_sniper.py    # Crypto top-50 sniper scanner
├── agent/
│   └── brain.py            # Claude AI autonomous research agent
└── notifier/
    └── telegram_bot.py     # Professional Telegram signal delivery
```

## Key Features

### XAUUSD Scanner
- **Multi-timeframe**: D1 trend → H4 structure → H1 entry
- **Session awareness**: London open, NY session, overlap
- **Asian Range**: Breakout detection above/below Asian session H/L
- **Key levels**: Psychological levels ($50/$100 increments), pivot points
- **SMC**: Order blocks, FVGs, BOS/ChoCH on H4

### Crypto Sniper
- **Top 50 coins** by volume on Binance/Bybit
- **Setup types**: BB Squeeze Breakout, OB Bounce, FVG Fill, ChoCH Entry, Divergence
- **Funding rate** monitoring (contrarian extreme signals)
- **Parallel scanning** via ThreadPoolExecutor (8 workers)
- **Composite scoring** = confidence + volume boost + setup quality - funding penalty

### AI Brain (Claude-powered)
- Autonomous research with tool-calling loop (Dexter-style)
- Tools: get_xauusd_analysis, get_crypto_analysis, scan_xauusd, scan_crypto_market, get_current_price
- Max 12 iterations with self-validation
- Streaming events for real-time display

### Signal Scoring Factors
1. Higher-TF trend alignment (+20 pts)
2. Entry-TF trend (+10 pts)
3. RSI momentum / divergence (+8-15 pts)
4. MACD crossover / histogram (+6-12 pts)
5. Bollinger Band position (+10 pts)
6. Volume confirmation (+10 pts)
7. SMC: Order Block proximity (+12 pts)
8. SMC: FVG proximity (+8 pts)
9. SMC: BOS/ChoCH (+8-15 pts)
10. Session timing multiplier (×1.05-1.10)

### Risk Management
- Entry at signal close price
- Stop Loss: 1.5× ATR (adjusted to OB level if applicable)
- TP1: 1:1 R:R (partial exit)
- TP2: 1:2 R:R (main target)
- TP3: 1:3 R:R (runner)

## Data Sources

- **XAUUSD**: Yahoo Finance (GC=F gold futures, free, no API key needed)
- **Crypto**: CCXT with Binance public API (free, no API key for market data)
- **AI**: Anthropic Claude API (claude-sonnet-4-6)

## Disclaimer

Dexter Pro is for informational purposes only. It does not execute trades automatically
(unless you explicitly add order execution code). Always manage your own risk.
Trading involves significant risk of loss.
