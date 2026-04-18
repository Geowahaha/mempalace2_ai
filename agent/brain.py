"""
agent/brain.py - Multi-provider Autonomous Financial Research Agent
Provider order: Groq -> Gemini -> Anthropic -> OpenRouter
"""
import json
import logging
from datetime import datetime, timezone
from typing import Optional, Generator
from dataclasses import dataclass, field

import requests

try:
    import anthropic
except Exception:  # pragma: no cover - optional dependency runtime
    anthropic = None

from config import config
from market.data_fetcher import xauusd_provider, crypto_provider, session_manager
from analysis.technical import TechnicalAnalysis
from scanners.xauusd import xauusd_scanner
from scanners.crypto_sniper import crypto_sniper
from scanners.stock_scanner import stock_scanner, fetch_stock_ohlcv, detect_market
from scanners.fibo_advance import FiboAdvanceScanner as _FiboAdvanceScanner
_fibo_advance_scanner = _FiboAdvanceScanner()

logger = logging.getLogger(__name__)
ta = TechnicalAnalysis()

SYSTEM_PROMPT = """You are Dexter Pro — an elite AI trading research agent and financial analyst.
You are expert in:
- Technical Analysis (TA): EMA/SMA crossovers, RSI, MACD, Bollinger Bands, ATR
- Smart Money Concepts (SMC): Order Blocks, Fair Value Gaps, BOS/ChoCH, Liquidity
- Market Structure: Multi-timeframe analysis, session timing (London/NY/Asia)
- XAUUSD (Gold): Institutional flows, DXY correlation, geopolitical drivers
- Crypto Markets: Funding rates, liquidation levels, market cycles

Always provide:
- Clear directional bias (bullish/bearish/neutral)
- Specific levels (entry, SL, TP)
- Risk/reward ratio
- Key risks and invalidation levels
- Confidence percentage (0-100%)

Tone: Professional, precise, concise."""


TOOLS = [
    {
        "name": "get_xauusd_analysis",
        "description": "Get comprehensive multi-timeframe XAUUSD analysis.",
        "input_schema": {"type": "object", "properties": {"timeframe": {"type": "string"}}},
    },
    {
        "name": "get_crypto_analysis",
        "description": "Get technical analysis for a specific crypto pair.",
        "input_schema": {"type": "object", "properties": {"symbol": {"type": "string"}}},
    },
    {
        "name": "scan_xauusd",
        "description": "Run a live XAUUSD sniper scan.",
        "input_schema": {"type": "object", "properties": {}},
    },
    {
        "name": "scan_fibo_advance",
        "description": "Run the Fibonacci Advance dual-speed sniper scan (Sniper H4+H1 and Scout H1+M15).",
        "input_schema": {"type": "object", "properties": {}},
    },
    {
        "name": "scan_crypto_market",
        "description": "Scan top crypto market opportunities.",
        "input_schema": {"type": "object", "properties": {"top_n": {"type": "integer"}}},
    },
    {
        "name": "scan_stock_market",
        "description": "Scan global stock markets opportunities.",
        "input_schema": {"type": "object", "properties": {"market": {"type": "string"}, "top_n": {"type": "integer"}}},
    },
    {
        "name": "get_stock_analysis",
        "description": "Get detailed technical analysis for a stock ticker.",
        "input_schema": {"type": "object", "properties": {"symbol": {"type": "string"}}},
    },
    {
        "name": "get_global_market_status",
        "description": "Get open/closed status of global markets.",
        "input_schema": {"type": "object", "properties": {}},
    },
    {
        "name": "get_market_session",
        "description": "Get current market session info.",
        "input_schema": {"type": "object", "properties": {}},
    },
    {
        "name": "get_current_price",
        "description": "Get live price of XAUUSD or crypto pair.",
        "input_schema": {"type": "object", "properties": {"symbol": {"type": "string"}}},
    },
]


@dataclass
class AgentEvent:
    event_type: str          # thinking | tool_call | tool_result | answer | error
    content: str = ""
    tool_name: str = ""
    tool_args: dict = field(default_factory=dict)
    tool_result: dict = field(default_factory=dict)
    iteration: int = 0


class DexterBrain:
    """Autonomous financial research agent with provider fallback."""

    def __init__(self, max_iterations: int = 12):
        self.max_iterations = max_iterations
        self.provider_chain = self._build_provider_chain()
        if not self.provider_chain:
            raise ValueError(
                "No AI provider key found (set GROQ_API_KEY or GEMINI_API_KEY or GEMINI_VERTEX_AI_API_KEY or ANTHROPIC_API_KEY)"
            )
        self.provider = self.provider_chain[0]
        self.model = config.model_for_provider(self.provider)
        self.client = None

    def _build_provider_chain(self) -> list[str]:
        """Build provider order with preferred provider first, then fallbacks."""
        available: list[str] = []
        if config.GROQ_API_KEY:
            available.append("groq")
        if config.has_gemini_key():
            available.append("gemini")
        if config.ANTHROPIC_API_KEY:
            available.append("anthropic")
        if config.OPENROUTER_API_KEY:
            available.append("openrouter")

        pref = (config.AI_PROVIDER or "auto").strip().lower()
        if pref in ("groq", "gemini", "anthropic", "openrouter") and pref in available:
            return [pref] + [p for p in available if p != pref]
        return available

    def _ensure_anthropic_client(self) -> None:
        if anthropic is None:
            raise ValueError("anthropic package is not available")
        if self.client is None:
            self.client = anthropic.Anthropic(api_key=config.ANTHROPIC_API_KEY)

    def _set_active_provider(self, provider: str) -> None:
        self.provider = provider
        self.model = config.model_for_provider(provider)
        if provider == "anthropic":
            self._ensure_anthropic_client()

    # ─── Tool Execution (for Anthropic tool loop) ────────────────────────────
    def _execute_tool(self, tool_name: str, tool_input: dict) -> dict:
        try:
            if tool_name == "get_xauusd_analysis":
                return {"success": True, "data": xauusd_scanner.get_market_overview()}

            if tool_name == "get_crypto_analysis":
                symbol = tool_input["symbol"]
                tf = tool_input.get("timeframe", "1h")
                df = crypto_provider.fetch_ohlcv(symbol, tf, bars=200)
                if df is None:
                    return {"success": False, "error": f"No data for {symbol}"}
                return {"success": True, "symbol": symbol, "timeframe": tf, "analysis": ta.summary(df)}

            if tool_name == "scan_xauusd":
                signal = xauusd_scanner.scan()
                return {"success": True, "signal": signal.to_dict() if signal else None}

            if tool_name == "scan_fibo_advance":
                signal = _fibo_advance_scanner.scan()
                result = signal.to_dict() if signal else None
                if result:
                    result["mode"] = str((signal.raw_scores or {}).get("mode", "unknown"))
                diag = _fibo_advance_scanner.get_last_scan_diagnostics()
                return {"success": True, "signal": result, "diagnostics": diag}

            if tool_name == "scan_crypto_market":
                top_n = min(int(tool_input.get("top_n", 5)), 10)
                opps = crypto_sniper.get_top_n(top_n)
                return {
                    "success": True,
                    "count": len(opps),
                    "opportunities": [
                        {
                            "rank": i + 1,
                            "symbol": opp.signal.symbol,
                            "setup": opp.setup_type,
                            "direction": opp.signal.direction,
                            "confidence": opp.signal.confidence,
                            "entry": opp.signal.entry,
                            "sl": opp.signal.stop_loss,
                            "tp2": opp.signal.take_profit_2,
                        }
                        for i, opp in enumerate(opps)
                    ],
                }

            if tool_name == "scan_stock_market":
                market = tool_input.get("market", "all").upper()
                top_n = min(int(tool_input.get("top_n", 5)), 10)
                if market == "ALL":
                    opps = stock_scanner.scan_all_open_markets()
                elif market == "PRIORITY":
                    opps = stock_scanner.scan_priority()
                elif market == "TH":
                    opps = stock_scanner.scan_thailand()
                elif market == "US":
                    opps = stock_scanner.scan_us()
                else:
                    opps = stock_scanner.scan_priority()
                return {
                    "success": True,
                    "market": market,
                    "count": len(opps),
                    "opportunities": [
                        {
                            "rank": i + 1,
                            "symbol": o.signal.symbol,
                            "market": o.market,
                            "setup": o.setup_type,
                            "direction": o.signal.direction,
                            "confidence": o.signal.confidence,
                            "entry": o.signal.entry,
                            "sl": o.signal.stop_loss,
                            "tp2": o.signal.take_profit_2,
                        }
                        for i, o in enumerate(opps[:top_n])
                    ],
                }

            if tool_name == "get_stock_analysis":
                symbol = tool_input["symbol"]
                tf = tool_input.get("timeframe", "1h")
                df = fetch_stock_ohlcv(symbol, tf, bars=200)
                if df is None:
                    return {"success": False, "error": f"No data for {symbol}"}
                return {
                    "success": True,
                    "symbol": symbol,
                    "market": detect_market(symbol),
                    "timeframe": tf,
                    "analysis": ta.summary(df),
                }

            if tool_name == "get_global_market_status":
                return stock_scanner.get_market_overview()

            if tool_name == "get_market_session":
                return session_manager.get_session_info()

            if tool_name == "get_current_price":
                symbol = tool_input["symbol"]
                price = xauusd_provider.get_current_price() if symbol.upper() == "XAUUSD" else crypto_provider.get_current_price(symbol)
                if price is None:
                    return {"success": False, "error": f"Could not fetch price for {symbol}"}
                return {"success": True, "symbol": symbol, "price": price, "timestamp": datetime.now(timezone.utc).isoformat()}

            return {"success": False, "error": f"Unknown tool: {tool_name}"}
        except Exception as e:
            logger.error(f"Tool execution error [{tool_name}]: {e}", exc_info=True)
            return {"success": False, "error": str(e)}

    # ─── OpenAI-Compatible Providers (Groq / OpenRouter) ─────────────────────
    def _openai_compat_endpoint(self, provider: str) -> tuple[str, str]:
        if provider == "groq":
            return "https://api.groq.com/openai/v1/chat/completions", config.GROQ_API_KEY
        if provider == "openrouter":
            return "https://openrouter.ai/api/v1/chat/completions", config.OPENROUTER_API_KEY
        raise ValueError(f"Provider {provider} is not OpenAI-compatible path")

    def _chat_openai_compat(
        self,
        messages: list[dict],
        provider: str,
        max_tokens: int = 1400,
        temperature: float = 0.2,
    ) -> str:
        endpoint, api_key = self._openai_compat_endpoint(provider)
        payload = {
            "model": config.model_for_provider(provider),
            "messages": messages,
            "max_tokens": max_tokens,
            "temperature": temperature,
        }
        headers = {
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
        }
        resp = requests.post(endpoint, headers=headers, json=payload, timeout=90)
        if resp.status_code >= 400:
            detail = resp.text[:400]
            try:
                parsed = resp.json()
                detail = (
                    parsed.get("error", {}).get("message")
                    or parsed.get("error_description")
                    or parsed.get("message")
                    or detail
                )
            except Exception:
                pass
            raise RuntimeError(f"{provider.upper()} API error {resp.status_code}: {detail}")
        data = resp.json()
        return (data.get("choices", [{}])[0].get("message", {}).get("content") or "").strip()

    def _gemini_native_endpoint_and_key(self) -> tuple[str, str]:
        model = config.model_for_provider("gemini")
        mode = config.gemini_mode()
        if mode == "vertex":
            endpoint = f"https://aiplatform.googleapis.com/v1/publishers/google/models/{model}:generateContent"
            return endpoint, config.GEMINI_VERTEX_AI_API_KEY
        if mode == "direct":
            endpoint = f"https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent"
            return endpoint, config.GEMINI_API_KEY
        raise RuntimeError("Gemini provider selected but no Gemini key is configured")

    @staticmethod
    def _message_content_to_text(content: object) -> str:
        if isinstance(content, str):
            return content.strip()
        if isinstance(content, list):
            out: list[str] = []
            for item in content:
                if isinstance(item, str):
                    txt = item.strip()
                    if txt:
                        out.append(txt)
                    continue
                if isinstance(item, dict):
                    txt = str(
                        item.get("text")
                        or item.get("input_text")
                        or ""
                    ).strip()
                    if txt:
                        out.append(txt)
            return "\n".join(out).strip()
        if isinstance(content, dict):
            txt = str(content.get("text") or content.get("input_text") or "").strip()
            return txt
        if content is None:
            return ""
        return str(content).strip()

    def _chat_gemini_native(
        self,
        messages: list[dict],
        max_tokens: int = 1400,
        temperature: float = 0.2,
    ) -> str:
        endpoint, api_key = self._gemini_native_endpoint_and_key()
        system_parts: list[str] = []
        contents: list[dict] = []

        for msg in messages:
            role = str(msg.get("role", "user")).strip().lower()
            text = self._message_content_to_text(msg.get("content"))
            if not text:
                continue
            if role == "system":
                system_parts.append(text)
                continue
            gemini_role = "model" if role == "assistant" else "user"
            contents.append({
                "role": gemini_role,
                "parts": [{"text": text}],
            })

        if not contents:
            raise RuntimeError("Gemini payload is empty")

        payload: dict = {
            "contents": contents,
            "generationConfig": {
                "temperature": float(temperature),
                "maxOutputTokens": int(max_tokens),
            },
        }
        if system_parts:
            payload["systemInstruction"] = {
                "parts": [{"text": "\n\n".join(system_parts)[:20000]}],
            }

        resp = requests.post(
            endpoint,
            params={"key": api_key},
            headers={"Content-Type": "application/json"},
            json=payload,
            timeout=90,
        )
        if resp.status_code >= 400:
            detail = resp.text[:500]
            try:
                parsed = resp.json()
                detail = (
                    parsed.get("error", {}).get("message")
                    or parsed.get("error_description")
                    or parsed.get("message")
                    or detail
                )
            except Exception:
                pass
            raise RuntimeError(f"GEMINI API error {resp.status_code}: {detail}")

        data = resp.json()
        candidates = data.get("candidates") or []
        if not candidates:
            feedback = data.get("promptFeedback", {}) or {}
            block_reason = str(feedback.get("blockReason") or "").strip()
            block_msg = f" blocked: {block_reason}" if block_reason else ""
            raise RuntimeError(f"GEMINI returned no candidates{block_msg}")

        parts = ((candidates[0] or {}).get("content", {}) or {}).get("parts", []) or []
        text_chunks: list[str] = []
        for part in parts:
            if not isinstance(part, dict):
                continue
            txt = str(part.get("text") or "").strip()
            if txt:
                text_chunks.append(txt)
        answer = "\n".join(text_chunks).strip()
        if not answer:
            raise RuntimeError("GEMINI returned empty answer")
        return answer

    def _build_fast_context(self) -> dict:
        """Build compact live context for one-shot LLM research."""
        ctx: dict = {
            "provider": self.provider,
            "model": self.model,
            "session": session_manager.get_session_info(),
        }
        try:
            xau = xauusd_scanner.get_market_overview()
            ctx["xauusd"] = {
                "price": xau.get("price"),
                "price_source": xau.get("price_source"),
                "h1": {k: xau.get("h1", {}).get(k) for k in ("trend", "rsi", "macd_hist")},
                "h4": {k: xau.get("h4", {}).get(k) for k in ("trend", "rsi", "macd_hist")},
                "d1": {k: xau.get("d1", {}).get(k) for k in ("trend", "rsi", "macd_hist")},
                "levels": xau.get("key_levels", {}),
                "h4_smc": xau.get("h4_smc", {}),
            }
        except Exception as e:
            ctx["xauusd_error"] = str(e)[:200]

        try:
            crypto_opps = crypto_sniper.quick_scan()[:3]
            ctx["crypto_top"] = [
                {
                    "symbol": o.signal.symbol,
                    "direction": o.signal.direction,
                    "confidence": o.signal.confidence,
                    "entry": o.signal.entry,
                    "sl": o.signal.stop_loss,
                    "tp2": o.signal.take_profit_2,
                    "setup": o.setup_type,
                }
                for o in crypto_opps
            ]
        except Exception as e:
            ctx["crypto_error"] = str(e)[:200]

        try:
            stock_opps = stock_scanner.scan_priority()[:3]
            ctx["stocks_top"] = [
                {
                    "symbol": o.signal.symbol,
                    "market": o.market,
                    "direction": o.signal.direction,
                    "confidence": o.signal.confidence,
                    "entry": o.signal.entry,
                    "sl": o.signal.stop_loss,
                    "tp2": o.signal.take_profit_2,
                }
                for o in stock_opps
            ]
        except Exception as e:
            ctx["stocks_error"] = str(e)[:200]

        return ctx

    def _research_openai_compat(self, question: str, provider: str) -> str:
        ctx = self._build_fast_context()
        user_prompt = (
            f"Question: {question}\n\n"
            "Use this live context JSON and answer with directional bias, levels, risk, confidence.\n"
            "If confidence is low, say so clearly.\n\n"
            f"Context JSON:\n{json.dumps(ctx, default=str)[:12000]}"
        )
        messages = [
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": user_prompt},
        ]
        answer = self._chat_openai_compat(messages=messages, provider=provider)
        if not answer:
            raise RuntimeError(f"{provider.upper()} returned empty answer")
        return answer

    def _research_gemini(self, question: str) -> str:
        ctx = self._build_fast_context()
        user_prompt = (
            f"Question: {question}\n\n"
            "Use this live context JSON and answer with directional bias, levels, risk, confidence.\n"
            "If confidence is low, say so clearly.\n\n"
            f"Context JSON:\n{json.dumps(ctx, default=str)[:12000]}"
        )
        messages = [
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": user_prompt},
        ]
        return self._chat_gemini_native(messages=messages)

    # ─── Anthropic Tool Loop (legacy path) ────────────────────────────────────
    def _research_anthropic(self, question: str) -> Generator[AgentEvent, None, None]:
        messages = [{"role": "user", "content": question}]
        iteration = 0

        while iteration < self.max_iterations:
            iteration += 1
            try:
                response = self.client.messages.create(
                    model=self.model,
                    max_tokens=4096,
                    system=SYSTEM_PROMPT,
                    tools=TOOLS,
                    messages=messages,
                )
            except Exception as e:
                yield AgentEvent("error", content=f"API error: {e}", iteration=iteration)
                return

            for block in response.content:
                if hasattr(block, "text") and block.text:
                    yield AgentEvent("thinking", content=block.text, iteration=iteration)

            if response.stop_reason == "end_turn":
                final_text = "".join([block.text for block in response.content if hasattr(block, "text")])
                yield AgentEvent("answer", content=final_text, iteration=iteration)
                return

            if response.stop_reason == "tool_use":
                messages.append({"role": "assistant", "content": response.content})
                tool_results = []
                for block in response.content:
                    if block.type == "tool_use":
                        tool_name = block.name
                        tool_args = block.input
                        tool_use_id = block.id
                        yield AgentEvent("tool_call", tool_name=tool_name, tool_args=tool_args, iteration=iteration)
                        result = self._execute_tool(tool_name, tool_args)
                        yield AgentEvent("tool_result", tool_name=tool_name, tool_result=result, iteration=iteration)
                        tool_results.append({
                            "type": "tool_result",
                            "tool_use_id": tool_use_id,
                            "content": json.dumps(result, default=str),
                        })
                messages.append({"role": "user", "content": tool_results})
                continue
            break

        yield AgentEvent("error", content="Max iterations reached without final answer", iteration=iteration)

    # ─── Public API ────────────────────────────────────────────────────────────
    def research(self, question: str) -> Generator[AgentEvent, None, None]:
        errors: list[str] = []
        total = len(self.provider_chain)

        for idx, provider in enumerate(self.provider_chain, start=1):
            self._set_active_provider(provider)
            yield AgentEvent(
                "thinking",
                content=f"Using {provider.upper()} ({self.model}) [{idx}/{total}]",
                iteration=idx,
            )

            try:
                if provider in ("groq", "openrouter"):
                    yield AgentEvent("thinking", content="Collected live market context.", iteration=idx)
                    answer = self._research_openai_compat(question, provider)
                    yield AgentEvent("answer", content=answer, iteration=idx)
                    return

                if provider == "gemini":
                    yield AgentEvent("thinking", content="Collected live market context.", iteration=idx)
                    answer = self._research_gemini(question)
                    yield AgentEvent("answer", content=answer, iteration=idx)
                    return

                # Anthropic path (tool loop).
                provider_error = ""
                got_answer = False
                for event in self._research_anthropic(question):
                    if event.event_type == "error":
                        provider_error = event.content
                        break
                    yield event
                    if event.event_type == "answer":
                        got_answer = True
                if got_answer:
                    return
                raise RuntimeError(provider_error or "unknown anthropic error")

            except Exception as e:
                short_err = str(e).replace("\n", " ").strip()
                errors.append(f"{provider.upper()}: {short_err[:220]}")
                if idx < total:
                    next_provider = self.provider_chain[idx]
                    yield AgentEvent(
                        "thinking",
                        content=f"{provider.upper()} failed, falling back to {next_provider.upper()}.",
                        iteration=idx,
                    )
                    continue

                joined = " | ".join(errors) if errors else "Unknown error"
                yield AgentEvent("error", content=f"All AI providers failed: {joined}", iteration=idx)
                return

    def quick_answer(self, question: str) -> str:
        final_answer = ""
        for event in self.research(question):
            if event.event_type == "answer":
                final_answer = event.content
            elif event.event_type == "error":
                final_answer = f"❌ Error: {event.content}"
        return final_answer or "No answer generated."


_brain_instance: Optional[DexterBrain] = None


def get_brain() -> DexterBrain:
    global _brain_instance
    if _brain_instance is None:
        _brain_instance = DexterBrain()
    return _brain_instance
