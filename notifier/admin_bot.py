"""
notifier/admin_bot.py - Telegram admin command listener
Long-polling command bot for secure operational control.
"""
import difflib
import logging
import threading
import time
import json
import os
import re
import sqlite3
from datetime import datetime, timedelta, timezone
from typing import Optional

import requests

from config import config
from notifier.telegram_bot import notifier
from notifier.access_control import access_manager

logger = logging.getLogger(__name__)


class TelegramAdminBot:
    """Admin-only Telegram command listener (polling-based)."""

    def __init__(self):
        self.enabled = bool(config.TELEGRAM_BOT_TOKEN and config.TELEGRAM_CHAT_ID)
        self.running = False
        self._thread: Optional[threading.Thread] = None
        self._offset: Optional[int] = None
        self._username: str = ""
        self._admin_ids = config.get_admin_ids()
        self._startup_conflict_notified = False
        self._chat_lang: dict[int, str] = {}
        self._chat_lang_pref: dict[int, str] = {}
        self._chat_lang_counts: dict[int, dict[str, int]] = {}
        self._chat_lang_prompt_pending: dict[int, bool] = {}
        self._chat_lang_last_prompt_ts: dict[int, float] = {}
        self._chat_user_map: dict[int, int] = {}
        self._chat_mt5_context: dict[int, dict] = {}
        self._chat_pending_slots: dict[int, dict] = {}
        self._chat_pending_intent_confirm: dict[int, dict] = {}
        root_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        data_dir = os.path.join(root_dir, "data")
        self._intent_learning_file = os.path.join(data_dir, "intent_phrase_memory.json")
        self._intent_events_file = os.path.join(data_dir, "intent_events.jsonl")
        self._intent_learning_lock = threading.Lock()
        self._intent_phrase_memory: dict[str, dict] = {}
        self._load_intent_phrase_memory()

    def _api_get(self, method: str, params: Optional[dict] = None, timeout: int = 35) -> Optional[dict]:
        try:
            url = f"https://api.telegram.org/bot{config.TELEGRAM_BOT_TOKEN}/{method}"
            resp = requests.get(url, params=params or {}, timeout=timeout)
            data = resp.json()
            if not data.get("ok"):
                err_code = int(data.get("error_code", 0) or 0)
                desc = str(data.get("description", "") or "")
                if method == "getUpdates" and err_code == 409:
                    logger.error(
                        "[AdminBot] Telegram polling conflict (409): %s | "
                        "Another bot instance is already polling this token.",
                        desc or "Conflict",
                    )
                    if self.running:
                        self.running = False
                        logger.error(
                            "[AdminBot] Poll loop stopped due to duplicate instance conflict. "
                            "Keep only one monitor process per bot token."
                        )
                    if (not self._startup_conflict_notified) and config.TELEGRAM_CHAT_ID and config.TELEGRAM_CHAT_ID.lstrip("-").isdigit():
                        self._startup_conflict_notified = True
                        self._send_text(
                            int(config.TELEGRAM_CHAT_ID),
                            "Admin bot conflict detected (Telegram 409). "
                            "Another process is already polling this bot token.\n"
                            "Action: stop duplicate monitor and keep only one instance.",
                        )
                else:
                    logger.warning(f"[AdminBot] Telegram API {method} failed: {data}")
                return None
            return data
        except Exception as e:
            logger.warning(f"[AdminBot] API error {method}: {e}")
            return None

    def _detect_polling_conflict(self) -> tuple[bool, str]:
        """
        Detect duplicate long-polling instance before starting the admin poll loop.
        Telegram returns 409 for getUpdates when another poller is active.
        """
        for _ in range(2):
            try:
                url = f"https://api.telegram.org/bot{config.TELEGRAM_BOT_TOKEN}/getUpdates"
                params = {
                    "timeout": 0,
                    "limit": 1,
                    "allowed_updates": '["message"]',
                }
                if self._offset is not None:
                    params["offset"] = self._offset
                resp = requests.get(url, params=params, timeout=12)
                data = resp.json()
                if data.get("ok"):
                    time.sleep(0.4)
                    continue
                err_code = int(data.get("error_code", 0) or 0)
                desc = str(data.get("description", "") or "")
                desc_l = desc.lower()
                if err_code == 409 or "terminated by other getupdates request" in desc_l:
                    return True, desc or "Conflict: another getUpdates consumer is active"
                return False, desc
            except Exception as e:
                return False, f"probe_error:{e}"
        return False, ""

    def _api_post(self, method: str, payload: dict, timeout: int = 20) -> bool:
        try:
            url = f"https://api.telegram.org/bot{config.TELEGRAM_BOT_TOKEN}/{method}"
            resp = requests.post(url, json=payload, timeout=timeout)
            data = resp.json()
            return bool(data.get("ok"))
        except Exception as e:
            logger.warning(f"[AdminBot] API error {method}: {e}")
            return False

    def _send_text(self, chat_id: int, text: str) -> None:
        self._api_post("sendMessage", {
            "chat_id": chat_id,
            "text": text,
            "disable_web_page_preview": True,
        })

    def _load_intent_phrase_memory(self) -> None:
        path = str(getattr(self, "_intent_learning_file", "") or "").strip()
        if not path:
            return
        try:
            if not os.path.exists(path):
                self._intent_phrase_memory = {}
                return
            with open(path, "r", encoding="utf-8") as f:
                obj = json.load(f)
            phrases = obj.get("phrases", {}) if isinstance(obj, dict) else {}
            if not isinstance(phrases, dict):
                phrases = {}
            sanitized: dict[str, dict] = {}
            for k, v in phrases.items():
                key = str(k or "").strip()
                if not key or not isinstance(v, dict):
                    continue
                cmd = str(v.get("command") or "").strip().lower()
                args = str(v.get("args") or "").strip()
                if not cmd:
                    continue
                sanitized[key] = {
                    "command": cmd,
                    "args": args,
                    "count": int(v.get("count", 1) or 1),
                    "updated_at": float(v.get("updated_at", time.time()) or time.time()),
                    "source": str(v.get("source") or "memory"),
                }
            self._intent_phrase_memory = sanitized
        except Exception as e:
            logger.warning("[AdminBot] failed to load intent phrase memory: %s", e)
            self._intent_phrase_memory = {}

    def _save_intent_phrase_memory(self) -> None:
        path = str(getattr(self, "_intent_learning_file", "") or "").strip()
        if not path:
            return
        try:
            os.makedirs(os.path.dirname(path), exist_ok=True)
            payload = {
                "updated_at": time.time(),
                "phrases": self._intent_phrase_memory,
            }
            with open(path, "w", encoding="utf-8") as f:
                json.dump(payload, f, ensure_ascii=False, indent=2)
        except Exception as e:
            logger.warning("[AdminBot] failed to save intent phrase memory: %s", e)

    @staticmethod
    def _normalize_intent_phrase_key(text: str) -> str:
        raw = str(text or "").strip()
        if not raw:
            return ""
        q = TelegramAdminBot._normalize_intent_text(raw)
        q = re.sub(r"\s+", " ", q).strip()
        return q[:240]

    def _record_intent_event(
        self,
        chat_id: int,
        user_id: int,
        text: str,
        outcome: str,
        command: str = "",
        args: str = "",
        source: str = "heuristic",
    ) -> None:
        path = str(getattr(self, "_intent_events_file", "") or "").strip()
        if not path:
            return
        rec = {
            "ts": time.time(),
            "chat_id": int(chat_id),
            "user_id": int(user_id),
            "text": str(text or "")[:600],
            "normalized": self._normalize_intent_phrase_key(text),
            "outcome": str(outcome or "")[:48],
            "command": str(command or "").strip().lower(),
            "args": str(args or "").strip()[:240],
            "source": str(source or "heuristic")[:48],
        }
        try:
            os.makedirs(os.path.dirname(path), exist_ok=True)
            with open(path, "a", encoding="utf-8") as f:
                f.write(json.dumps(rec, ensure_ascii=False) + "\n")
        except Exception as e:
            logger.debug("[AdminBot] failed to append intent event: %s", e)

    def _remember_intent_phrase(
        self,
        text: str,
        command: str,
        args: str = "",
        source: str = "heuristic",
    ) -> None:
        key = self._normalize_intent_phrase_key(text)
        cmd = str(command or "").strip().lower()
        if not key or not cmd:
            return
        with self._intent_learning_lock:
            old = dict(self._intent_phrase_memory.get(key) or {})
            cnt = int(old.get("count", 0) or 0) + 1
            self._intent_phrase_memory[key] = {
                "command": cmd,
                "args": str(args or "").strip(),
                "count": cnt,
                "updated_at": time.time(),
                "source": str(source or "heuristic"),
            }
            self._save_intent_phrase_memory()

    def _lookup_learned_intent(self, text: str) -> Optional[tuple[str, str]]:
        key = self._normalize_intent_phrase_key(text)
        if not key:
            return None
        rec = self._intent_phrase_memory.get(key)
        if not rec:
            return None
        cmd = str(rec.get("command") or "").strip().lower()
        args = str(rec.get("args") or "").strip()
        if not cmd:
            return None
        return cmd, args

    @staticmethod
    def _detect_language(text: str) -> str:
        raw = str(text or "")
        if any("\u0E00" <= ch <= "\u0E7F" for ch in raw):
            return "th"
        q = raw.lower()
        if re.search(r"[äöüß]", q):
            return "de"
        de_hits = sum(
            1 for k in (
                " warum ", " bitte ", " und ", " oder ", "konto", "position", "offen",
                "status", "deutsch", "handel", "auftrag", "wie ", "ist ", "mein ",
            )
            if k in f" {q} "
        )
        if de_hits >= 2:
            return "de"
        return "en"

    def _remember_chat_lang(self, chat_id: int, text: str) -> str:
        lang = self._detect_language(text)
        try:
            cid = int(chat_id)
            self._chat_lang[cid] = lang
            counts = self._chat_lang_counts.setdefault(cid, {"th": 0, "en": 0, "de": 0})
            counts[lang] = int(counts.get(lang, 0) or 0) + 1
        except Exception:
            pass
        return lang

    def _lang_for_chat(self, chat_id: int) -> str:
        try:
            cid = int(chat_id)
            pref = self._chat_lang_pref.get(cid)
            if pref:
                return pref
            return self._chat_lang.get(cid, "en")
        except Exception:
            return "en"

    def _hydrate_chat_lang_preference(self, chat_id: int, user_id: Optional[int] = None) -> None:
        try:
            cid = int(chat_id)
        except Exception:
            return
        if cid in self._chat_lang_pref:
            return
        uid = None
        try:
            if user_id is not None:
                uid = int(user_id)
                self._chat_user_map[cid] = uid
            else:
                uid = self._chat_user_map.get(cid)
        except Exception:
            uid = None
        if not uid:
            return
        try:
            pref = access_manager.get_user_language_preference(uid)
        except Exception as e:
            logger.debug("[AdminBot] load language preference failed user=%s err=%s", uid, e)
            pref = None
        if pref in {"th", "en", "de"}:
            self._chat_lang_pref[cid] = pref
            self._chat_lang[cid] = pref

    def _set_chat_lang_preference(self, chat_id: int, lang: str, user_id: Optional[int] = None) -> None:
        try:
            cid = int(chat_id)
            l = (lang or "en").lower()
            if l not in {"th", "en", "de"}:
                return
            uid = None
            try:
                if user_id is not None:
                    uid = int(user_id)
                    self._chat_user_map[cid] = uid
                else:
                    uid = self._chat_user_map.get(cid)
            except Exception:
                uid = None
            self._chat_lang_pref[cid] = l
            self._chat_lang[cid] = l
            self._chat_lang_prompt_pending[cid] = False
            if uid:
                try:
                    access_manager.set_user_language_preference(
                        uid,
                        l,
                        metadata={"source": "admin_bot", "chat_id": cid},
                    )
                except Exception as e:
                    logger.warning("[AdminBot] save language preference failed user=%s err=%s", uid, e)
        except Exception:
            return

    @staticmethod
    def _lang_label(lang: str, ui_lang: str = "en") -> str:
        key = (lang or "en").lower()
        ui = (ui_lang or "en").lower()
        if ui == "th":
            labels = {"th": "ภาษาไทย", "en": "English", "de": "Deutsch"}
        elif ui == "de":
            labels = {"th": "Thai", "en": "English", "de": "Deutsch"}
        else:
            labels = {"th": "Thai", "en": "English", "de": "Deutsch"}
        return labels.get(key, "English")

    def _parse_language_preference(self, text: str, pending: bool = False) -> Optional[str]:
        raw = str(text or "").strip()
        if not raw:
            return None
        q = raw.lower().strip()

        direct_map = {
            "th": "th", "thai": "th", "ไทย": "th", "ภาษาไทย": "th",
            "en": "en", "eng": "en", "english": "en", "อังกฤษ": "en", "ภาษาอังกฤษ": "en",
            "de": "de", "german": "de", "deutsch": "de", "เยอรมัน": "de", "ภาษาเยอรมัน": "de",
        }
        if pending and q in direct_map:
            return direct_map[q]

        def any_in(*parts: str) -> bool:
            return any(p in q for p in parts)

        if (
            any_in("ตอบไทย", "ภาษาไทย", "คุยไทย", "พูดไทย", "thai please", "speak thai", "reply thai", "reply in thai", "use thai")
            or ("thai" in q and any_in("reply", "speak", "use", "lang", "language"))
        ):
            return "th"
        if (
            any_in("ตอบอังกฤษ", "ภาษาอังกฤษ", "คุยอังกฤษ", "พูดอังกฤษ", "english ok", "english is ok", "speak english", "reply in english", "use english")
            or ("english" in q and any_in("reply", "speak", "use", "ok", "language"))
        ):
            return "en"
        if (
            any_in("ตอบเยอรมัน", "ภาษาเยอรมัน", "คุยเยอรมัน", "deutsch bitte", "auf deutsch", "sprich deutsch", "reply in german", "speak german", "use german")
            or ("deutsch" in q and any_in("bitte", "auf", "reply", "speak"))
            or ("german" in q and any_in("reply", "speak", "use", "language"))
        ):
            return "de"
        return None

    def _language_pref_saved_text(self, lang: str) -> str:
        l = (lang or "en").lower()
        if l == "th":
            return (
                "รับทราบครับ จากนี้ผมจะตอบเป็นภาษาไทยเป็นหลัก\n"
                "ถ้าต้องการเปลี่ยนภาษา พิมพ์: English / Deutsch / ไทย"
            )
        if l == "de":
            return (
                "Verstanden. Ich antworte ab jetzt standardmäßig auf Deutsch.\n"
                "Zum Wechseln einfach senden: English / Thai / Deutsch"
            )
        return (
            "Understood. I will reply in English by default from now on.\n"
            "To switch later, send: Thai / Deutsch / English"
        )

    @staticmethod
    def _normalize_utc_offset_input(raw_text: str) -> Optional[str]:
        raw = str(raw_text or "").strip().lower()
        if not raw:
            return None
        aliases = {
            "bangkok": "+07:00",
            "bkk": "+07:00",
            "thai": "+07:00",
            "thailand": "+07:00",
            "asia/bangkok": "+07:00",
        }
        if raw in aliases:
            return aliases[raw]
        x = raw.upper()
        if x.startswith("UTC") or x.startswith("GMT"):
            x = x[3:].strip()
        x = x.replace(" ", "")
        if not x:
            return None
        if x[0] not in {"+", "-"} and x.isdigit():
            x = f"+{x}"
        if x and ":" not in x:
            sign = x[0] if x[0] in {"+", "-"} else "+"
            num = x[1:] if x[0] in {"+", "-"} else x
            if num.isdigit():
                x = f"{sign}{int(num):02d}:00"
        if len(x) == 6 and x[0] in {"+", "-"} and x[3] == ":":
            try:
                hh = int(x[1:3])
                mm = int(x[4:6])
                if 0 <= hh <= 14 and 0 <= mm < 60:
                    return x
            except Exception:
                return None
        return None

    @classmethod
    def _monitor_tz_for_user(cls, user_id: Optional[int]) -> tuple[timezone, str]:
        """Resolve monitor timezone from user's /tz preference; fallback to Bangkok."""
        offset = "+07:00"
        try:
            if user_id is not None:
                saved = access_manager.get_user_news_utc_offset(int(user_id))
                if saved:
                    offset = str(saved).strip().upper()
        except Exception:
            offset = "+07:00"
        if not (len(offset) == 6 and offset[0] in {"+", "-"} and offset[3] == ":"):
            offset = "+07:00"
        try:
            sign = 1 if offset[0] == "+" else -1
            hh = int(offset[1:3])
            mm = int(offset[4:6])
            if not (0 <= hh <= 14 and 0 <= mm < 60):
                raise ValueError("invalid offset")
            tz_obj = timezone(sign * timedelta(hours=hh, minutes=mm))
            return tz_obj, f"UTC{offset}"
        except Exception:
            return timezone(timedelta(hours=7)), "UTC+07:00"

    @classmethod
    def _monitor_local_time_text(cls, user_id: Optional[int]) -> str:
        tz_obj, tz_label = cls._monitor_tz_for_user(user_id)
        now_local = datetime.now(timezone.utc).astimezone(tz_obj)
        return f"{now_local.strftime('%H:%M')} {tz_label}"

    def _language_pref_offer_text(self, chat_id: int, ui_lang: str) -> str:
        counts = dict(self._chat_lang_counts.get(int(chat_id), {}) or {})
        seen = [(k, int(v or 0)) for k, v in counts.items() if int(v or 0) > 0]
        seen.sort(key=lambda x: (-x[1], x[0]))
        seen_labels = ", ".join(self._lang_label(k, ui_lang) for k, _ in seen[:3]) or self._lang_label("en", ui_lang)
        current = self._lang_label(self._chat_lang.get(int(chat_id), "en"), ui_lang)
        if (ui_lang or "en").lower() == "th":
            return (
                f"ผมสังเกตว่าคุณใช้ได้หลายภาษา ({seen_labels})\n"
                f"ตอนนี้ผมกำลังตอบตามข้อความล่าสุดเป็น {current}\n"
                "คุณโอเคไหม หรืออยากให้ผมคุยภาษาไหนเป็นหลัก?\n"
                "พิมพ์ตอบได้เลย: ไทย / English / Deutsch"
            )
        if (ui_lang or "en").lower() == "de":
            return (
                f"Ich sehe, dass du mehrere Sprachen nutzt ({seen_labels}).\n"
                f"Im Moment antworte ich nach der letzten Nachricht auf {current}.\n"
                "Welche Sprache bevorzugst du für meine Antworten?\n"
                "Antwort einfach mit: Thai / English / Deutsch"
            )
        return (
            f"I noticed you use multiple languages ({seen_labels}).\n"
            f"Right now I'm replying based on your latest message in {current}.\n"
            "Which language would you like me to use by default?\n"
            "Reply with: Thai / English / Deutsch"
        )

    def _maybe_offer_language_preference(self, chat_id: int, text: str, command: Optional[str], lang: str) -> None:
        raw = str(text or "").strip()
        if not raw:
            return
        if command and not (raw and not raw.startswith("/")):
            return
        try:
            cid = int(chat_id)
        except Exception:
            return
        if self._chat_lang_pref.get(cid):
            return
        if self._chat_lang_prompt_pending.get(cid):
            return
        counts = self._chat_lang_counts.get(cid, {})
        seen = [(k, int(v or 0)) for k, v in counts.items() if int(v or 0) > 0]
        if len(seen) < 2:
            return
        seen.sort(key=lambda x: (-x[1], x[0]))
        total = sum(v for _, v in seen)
        if total < 3:
            return
        if int(seen[1][1]) < 1:
            return
        now = time.time()
        last = float(self._chat_lang_last_prompt_ts.get(cid, 0.0) or 0.0)
        if (now - last) < 6 * 3600:
            return
        self._chat_lang_prompt_pending[cid] = True
        self._chat_lang_last_prompt_ts[cid] = now
        self._send_text(cid, self._language_pref_offer_text(cid, ui_lang=lang))

    @staticmethod
    def _lang_name(lang: str) -> str:
        m = {"th": "Thai", "de": "German", "en": "English"}
        return m.get((lang or "en").lower(), "English")

    def _tr(self, lang: str, key: str, **kwargs) -> str:
        lang = (lang or "en").lower()
        th = {
            "hello": "สวัสดีครับ ส่ง /help หรือถามเป็นภาษาธรรมชาติได้เลย",
            "checking_signal_reason": "กำลังตรวจเหตุผลของสัญญาณล่าสุด...",
            "xau_price_unavailable": "ตอนนี้ดึงราคา XAUUSD แบบเรียลไทม์ไม่ได้",
            "xau_price": "ราคา XAUUSD ล่าสุด: {price:.2f}",
            "running_gold": "กำลังสแกน XAUUSD...",
            "running_crypto": "กำลังสแกน Crypto...",
            "running_stocks": "กำลังสแกนหุ้น...",
            "running_thai": "กำลังสแกนหุ้นไทย SET50...",
            "running_us_open": "กำลังสแกนแผน US Open (Top 10)...",
            "running_us_monitor": "กำลังอัปเดต US Open Smart Monitor...",
            "running_vi": "กำลังสแกนหุ้น VI (Value + Trend)...",
            "running_symbol_scan": "กำลังสแกน/วิเคราะห์ {symbol}...",
            "checking_calendar": "กำลังตรวจข่าวเศรษฐกิจที่กำลังจะมา...",
            "checking_macro": "กำลังตรวจข่าวมหภาค/นโยบาย (Trump/Fed/น้ำมัน/สงคราม)...",
            "checking_macro_report": "กำลังวิเคราะห์ผลกระทบหลังข่าว (Post-News Impact)...",
            "checking_macro_weights": "กำลังดึงน้ำหนัก adaptive ของธีมข่าว macro...",
            "running_all": "กำลังรันสแกนทั้งหมด (Gold + Crypto + Stocks)...",
            "research_progress": "กำลังค้นคว้าให้ รอสักครู่...",
            "research_failed": "การค้นคว้าล้มเหลว: {err}",
            "no_answer": "ยังไม่พบคำตอบที่ชัดเจนในตอนนี้",
            "ai_credit_low": "เครดิต AI ใกล้หมดตอนนี้ แต่ยังใช้คำสั่ง scan/status/market ได้ตามปกติ",
            "mt5_query_progress": "กำลังตรวจสถานะ MT5 ของ {symbol}...",
            "mt5_query_none": "ไม่พบออเดอร์/โพสิชั่นเปิดอยู่สำหรับ {symbol} ใน MT5 ตอนนี้",
            "mt5_query_error": "ตรวจ MT5 ไม่สำเร็จ: {err}",
            "mt5_query_title": "สถานะ MT5 ของ {symbol}",
            "mt5_disconnected": "MT5 ไม่พร้อมใช้งาน/ไม่เชื่อมต่อ",
            "mt5_history_progress": "กำลังตรวจประวัติเทรด MT5 (ย้อนหลัง {hours}h){symbol_part}...",
            "mt5_history_none": "ไม่พบประวัติเทรดปิดใน MT5 ย้อนหลัง {hours}h{symbol_part}",
            "mt5_history_title": "ประวัติเทรดปิด MT5 ย้อนหลัง {hours}h{symbol_part}",
            "mt5_history_need_symbol": "ต้องการเช็ค history ของสัญลักษณ์ไหน? ส่งเช่น ETHUSD / XAUUSD หรือพิมพ์ all เพื่อดูทุกตัว (ค่าเริ่มต้นย้อนหลัง {hours}h)",
            "command_autocorrected": "เดาว่าคุณหมายถึง /{suggested} (จาก /{original}) กำลังดำเนินการให้...",
            "mt5_followup_no_context": "ยังไม่มีบริบทเทรด MT5 ก่อนหน้าในแชตนี้ กรุณาส่ง /mt5_status หรือระบุสัญลักษณ์ก่อน เช่น mt5 ETHUSD",
            "mt5_followup_checking": "กำลังติดตามสถานะเทรด MT5 ของ {symbol}...",
            "mt5_followup_no_open_trade": "ตอนนี้ไม่พบโพสิชั่น/ออเดอร์เปิดของ {symbol} แล้ว อาจปิดไปแล้ว",
            "mt5_followup_header": "ผู้ช่วยติดตามเทรด MT5 — {symbol}",
            "mt5_followup_auto_takeover_unavailable": "ยังไม่เปิดโหมดให้บอท takeover เพื่อจัดการ TP/SL อัตโนมัติในบทสนทนานี้",
            "mt5_followup_next_step": "หากต้องการให้ช่วยติดตามต่อ พิมพ์เช่น: 'เช็ค mt5 {symbol} อีกครั้ง' หรือ 'ช่วยวางแผน TP/SL สำหรับ {symbol}'",
            "ai_api_locked_trial": "โหมด Trial ปิด AI chat/research อัตโนมัติไว้เพื่อประหยัดเครดิต API\nยังใช้คำสั่งสแกน/สถานะ/MT5 ได้ตามแพ็กเกจ\nหากต้องการ AI chat ให้ใช้ /upgrade",
            "command_failed": "คำสั่งล้มเหลว: {err}",
            "specify_symbol": "กรุณาระบุสัญลักษณ์ เช่น AVGO, XAUUSD หรือ BTC/USDT",
        }
        de = {
            "hello": "Hallo. Sende /help oder frage in natürlicher Sprache.",
            "checking_signal_reason": "Prüfe die letzte Signal-Begründung...",
            "xau_price_unavailable": "Live-XAUUSD-Preis ist gerade nicht verfügbar.",
            "xau_price": "XAUUSD Live-Preis: {price:.2f}",
            "running_gold": "XAUUSD-Scan läuft...",
            "running_crypto": "Krypto-Scan läuft...",
            "running_stocks": "Aktien-Scan läuft...",
            "running_thai": "Thailand SET50-Scan läuft...",
            "running_us_open": "US-Open Daytrade-Plan (Top 10) läuft...",
            "running_us_monitor": "US-Open Smart Monitor läuft...",
            "running_vi": "US Value+Trend (VI)-Scan läuft...",
            "running_symbol_scan": "Scanne/analysiere {symbol}...",
            "checking_calendar": "Prüfe kommende Wirtschaftstermine...",
            "checking_macro": "Prüfe Makro-/Politik-Risiko-Schlagzeilen (Trump/Fed/Öl/Krieg)...",
            "checking_macro_report": "Analysiere Post-News-Marktreaktion (Post-News Impact)...",
            "checking_macro_weights": "Lade adaptive Makro-Themengewichte...",
            "running_all": "Vollscan läuft (Gold + Krypto + Aktien)...",
            "research_progress": "Recherche läuft. Bitte kurz warten...",
            "research_failed": "Recherche fehlgeschlagen: {err}",
            "no_answer": "Keine klare Antwort erzeugt.",
            "ai_credit_low": "AI-Recherche-Credits sind aktuell niedrig. Scan/Status/Market-Befehle funktionieren weiter.",
            "mt5_query_progress": "Prüfe MT5-Status für {symbol}...",
            "mt5_query_none": "Keine offenen Orders/Positionen für {symbol} im MT5 gefunden.",
            "mt5_query_error": "MT5-Prüfung fehlgeschlagen: {err}",
            "mt5_query_title": "MT5-Status für {symbol}",
            "mt5_disconnected": "MT5 ist nicht verfügbar / nicht verbunden.",
            "mt5_history_progress": "Prüfe MT5-Trade-Historie (letzte {hours}h){symbol_part}...",
            "mt5_history_none": "Keine geschlossenen MT5-Trades in den letzten {hours}h{symbol_part} gefunden.",
            "mt5_history_title": "MT5 Closed-Trade-Historie (letzte {hours}h){symbol_part}",
            "mt5_history_need_symbol": "Für welches Symbol soll ich die Historie prüfen? Sende z.B. ETHUSD / XAUUSD oder 'all' für alle (Standard {hours}h).",
            "command_autocorrected": "Ich nehme /{suggested} an (statt /{original}) und führe es aus...",
            "mt5_followup_no_context": "Kein vorheriger MT5-Trade-Kontext in diesem Chat. Bitte zuerst /mt5_status senden oder ein Symbol angeben (z.B. mt5 ETHUSD).",
            "mt5_followup_checking": "Prüfe MT5-Trade-Status für {symbol}...",
            "mt5_followup_no_open_trade": "Keine offene Position/Order für {symbol} gefunden. Der Trade wurde evtl. bereits geschlossen.",
            "mt5_followup_header": "MT5 Trade-Monitor Assistent — {symbol}",
            "mt5_followup_auto_takeover_unavailable": "Automatisches TP/SL-Takeover per Chat ist in diesem Build noch nicht aktiviert.",
            "mt5_followup_next_step": "Für weiteres Monitoring sende z.B.: 'prüfe mt5 {symbol} nochmal' oder 'TP/SL Plan für {symbol}'.",
            "ai_api_locked_trial": "Im Trial sind AI-Chat/Research-Funktionen zum Schutz des API-Credits deaktiviert.\nScan/Status/MT5-Befehle bleiben verfügbar.\nFür AI-Chat bitte /upgrade nutzen.",
            "command_failed": "Befehl fehlgeschlagen: {err}",
            "specify_symbol": "Bitte Symbol angeben, z.B. AVGO, XAUUSD oder BTC/USDT.",
        }
        en = {
            "hello": "Hello. Send /help or ask in plain language.",
            "checking_signal_reason": "Checking latest signal rationale...",
            "xau_price_unavailable": "Live XAUUSD price unavailable right now.",
            "xau_price": "XAUUSD live price: {price:.2f}",
            "running_gold": "Running XAUUSD scan...",
            "running_crypto": "Running crypto scan...",
            "running_stocks": "Running stock scan...",
            "running_thai": "Running Thailand SET50 scan...",
            "running_us_open": "Running US open day-trade plan (top 10)...",
            "running_us_monitor": "Running US open smart monitor update...",
            "running_vi": "Running US value + trend scanner...",
            "running_symbol_scan": "Scanning/analyzing {symbol}...",
            "checking_calendar": "Checking upcoming economic events...",
            "checking_macro": "Checking macro-policy risk headlines...",
            "checking_macro_report": "Analyzing post-news market impact (Post-News Impact)...",
            "checking_macro_weights": "Loading adaptive macro theme weights...",
            "running_all": "Running full scan (gold + crypto + stocks)...",
            "research_progress": "Research in progress. Please wait...",
            "research_failed": "Research failed: {err}",
            "no_answer": "No answer generated.",
            "ai_credit_low": "AI research credits are low right now. You can still use scan/status/market commands.",
            "mt5_query_progress": "Checking MT5 status for {symbol}...",
            "mt5_query_none": "No open orders/positions found for {symbol} in MT5 right now.",
            "mt5_query_error": "MT5 check failed: {err}",
            "mt5_query_title": "MT5 status for {symbol}",
            "mt5_disconnected": "MT5 is unavailable / not connected.",
            "mt5_history_progress": "Checking MT5 closed-trade history (last {hours}h){symbol_part}...",
            "mt5_history_none": "No closed MT5 trades found in the last {hours}h{symbol_part}.",
            "mt5_history_title": "MT5 closed-trade history (last {hours}h){symbol_part}",
            "mt5_history_need_symbol": "Which symbol should I check in MT5 history? Send e.g. ETHUSD / XAUUSD or 'all' for all symbols (default {hours}h).",
            "command_autocorrected": "Assuming /{suggested} (from /{original}) and running it...",
            "mt5_followup_no_context": "No prior MT5 trade context in this chat yet. Send /mt5_status first or specify a symbol (e.g. mt5 ETHUSD).",
            "mt5_followup_checking": "Checking MT5 trade monitor status for {symbol}...",
            "mt5_followup_no_open_trade": "No open position/order found for {symbol} now. It may already be closed.",
            "mt5_followup_header": "MT5 Trade Monitor Assistant — {symbol}",
            "mt5_followup_auto_takeover_unavailable": "Chat-based TP/SL auto-takeover is not enabled in this build yet.",
            "mt5_followup_next_step": "For continued monitoring, send for example: 'check mt5 {symbol} again' or 'TP/SL plan for {symbol}'.",
            "ai_api_locked_trial": "Trial mode disables AI chat/research to protect API credits.\nYou can still use scan/status/MT5 commands allowed by your plan.\nUse /upgrade for AI chat access.",
            "command_failed": "Command failed: {err}",
            "specify_symbol": "Please specify a symbol, e.g. AVGO, XAUUSD, BTC/USDT.",
        }
        table = {"th": th, "de": de}.get(lang, en)
        text = table.get(key) or en.get(key) or key
        try:
            return text.format(**kwargs)
        except Exception:
            return text

    def _send_text_localized(self, chat_id: int, key: str, lang: Optional[str] = None, **kwargs) -> None:
        self._send_text(chat_id, self._tr(lang or self._lang_for_chat(chat_id), key, **kwargs))

    def _research_prompt_with_language(self, question: str, lang: str) -> str:
        target = self._lang_name(lang)
        if (lang or "en").lower() == "en":
            return question
        return (
            f"Answer in {target}. Match the user's language naturally. "
            f"Keep trading terms/tickers unchanged when appropriate.\n\n"
            f"Question: {question}"
        )

    def _refresh_identity(self) -> None:
        data = self._api_get("getMe", timeout=20)
        if data and data.get("result"):
            self._username = data["result"].get("username", "")

    def _extract_command(self, text: str) -> tuple[Optional[str], str]:
        if not text or not text.startswith("/"):
            return None, ""
        parts = text.strip().split(maxsplit=1)
        cmd_raw = parts[0][1:]
        args = parts[1].strip() if len(parts) > 1 else ""
        if "@" in cmd_raw:
            cmd, suffix = cmd_raw.split("@", 1)
            if self._username and suffix.lower() != self._username.lower():
                return None, ""
            return cmd.lower(), args
        return cmd_raw.lower(), args

    @staticmethod
    def _known_commands() -> set[str]:
        return {
            "start", "help", "status",
            "scan_gold", "scan_crypto", "scan_fx", "scan_stocks", "scan_thai", "scan_thai_vi", "scan_us", "scan_us_open", "scan_vi", "scan_vi_buffett", "scan_vi_turnaround", "scan_all",
            "monitor_us",
            "scalping_status", "scalping_on", "scalping_off", "scalping_scan", "scalping_logic",
            "us_open_report",
            "us_open_dashboard",
            "us_open_guard_status",
            "signal_dashboard",
            "signal_monitor",
            "signal_filter", "show_only", "show_add", "show_clear", "show_all",
            "calendar", "macro", "tz", "timezone",
            "macro_report", "macro_weights", "markets", "gold_overview",
            "mt5_status", "mt5_history", "mt5_backtest", "mt5_train",
            "mt5_autopilot", "mt5_walkforward", "mt5_manage", "mt5_affordable", "mt5_exec_reasons", "mt5_pm_learning", "mt5_policy", "mt5_plan", "mt5_adaptive_explain",
            "run",
            "stock_mt5_filter",
            "plan", "upgrade", "research",
            "grant", "setplan", "revoke", "block", "admin_add", "admin_del", "admin_list", "user_list",
            "trials", "approve", "reject",
            "update_openclaw", "skip_openclaw", "openclaw_version",
            "ask", "chat", "q",
            "budget", "token_budget",
            "copy_status", "copy_add_ctrader", "copy_add_mt5", "copy_remove", "copy_pause", "copy_resume", "copy_log",
        }

    def _suggest_command(self, command: str) -> Optional[str]:
        raw = str(command or "").strip().lower()
        if not raw:
            return None
        aliases = {
            "mt5_ststus": "mt5_status",
            "mt5_statue": "mt5_status",
            "mt5_hisotry": "mt5_history",
            "mt5_histroy": "mt5_history",
            "mt5_autopliot": "mt5_autopilot",
            "mt5_walfroward": "mt5_walkforward",
            "mt5_walkfoward": "mt5_walkforward",
            "mt5_adaptiveexpalin": "mt5_adaptive_explain",
            "mt5_paln": "mt5_plan",
            "mt5_pm_learnig": "mt5_pm_learning",
            "mt5_pmlearn": "mt5_pm_learning",
            "mt5_affordble": "mt5_affordable",
            "mt5_exec_reason": "mt5_exec_reasons",
            "mt5_execreasons": "mt5_exec_reasons",
            "usopen_report": "us_open_report",
            "usopen_dashboard": "us_open_dashboard",
            "usopen_guard": "us_open_guard_status",
            "usopenguard": "us_open_guard_status",
            "us_open_guard": "us_open_guard_status",
            "signaldashboard": "signal_dashboard",
            "daily_signal_dashboard": "signal_dashboard",
            "signalmonitor": "signal_monitor",
            "monitor_signal": "signal_monitor",
            "dashboard_monitor": "signal_monitor",
            "runid": "run",
            "trace": "run",
            "traceid": "run",
            "calender": "calendar",
            "macro_weigths": "macro_weights",
            "macro_wights": "macro_weights",
            "stockmt5filter": "stock_mt5_filter",
            "stock_mt5filter": "stock_mt5_filter",
            "signalfilter": "signal_filter",
            "showonly": "show_only",
            "showadd": "show_add",
            "showclear": "show_clear",
            "showall": "show_all",
            "adminadd": "admin_add",
            "admindel": "admin_del",
            "adminremove": "admin_del",
            "adminlist": "admin_list",
            "userlist": "user_list",
            "users": "user_list",
            "knownusers": "user_list",
            "known_users": "user_list",
            "scanvibuffett": "scan_vi_buffett",
            "scanviturnaround": "scan_vi_turnaround",
            "scanfx": "scan_fx",
            "scanforex": "scan_fx",
            "scalpstatus": "scalping_status",
            "scalpingstatus": "scalping_status",
            "scalpon": "scalping_on",
            "scalpingon": "scalping_on",
            "scalpoff": "scalping_off",
            "scalpingoff": "scalping_off",
            "scalpscan": "scalping_scan",
            "scalp_logic": "scalping_logic",
            "scalpinglogic": "scalping_logic",
        }
        if raw in aliases:
            return aliases[raw]
        matches = difflib.get_close_matches(raw, sorted(self._known_commands()), n=1, cutoff=0.78)
        return matches[0] if matches else None

    @staticmethod
    def _contains_gold_token(q: str) -> bool:
        text = str(q or "").lower()
        return any(k in text for k in ("gold", "xau", "xauusd", "ทอง"))

    @staticmethod
    def _is_ambiguous_gold_stock_intent(q: str) -> bool:
        text = str(q or "").lower()
        if not TelegramAdminBot._contains_gold_token(text):
            return False
        stockish = (
            TelegramAdminBot._contains_vi_scan_token(text)
            or TelegramAdminBot._contains_us_market_token(text)
            or TelegramAdminBot._contains_thai_market_token(text)
            or any(k in text for k in ("stock", "stocks", "หุ้น", "market", "markets", "ตลาด"))
        )
        if not stockish:
            return False
        actionish = any(k in text for k in ("scan", "search", "find", "analy", "วิเคราะห์", "ค้นหา", "หา", "สแกน"))
        return actionish

    @staticmethod
    def _parse_signal_dashboard_args(text: str) -> dict:
        out = {
            "days": 1,
            "top": 5,
            "market_filter": None,
            "symbol_filter": None,
            "window_mode": "today",
            "compare": False,
            "left": None,
            "right": None,
        }
        raw = str(text or "").strip()
        if not raw:
            return out
        q = raw.lower().strip()

        # Natural-language window presets.
        if any(k in q for k in ("this month", "เดือนนี้", "เดือนนี", "mtd")):
            out["window_mode"] = "this_month"
            out["days"] = 30
        elif any(k in q for k in ("this week", "สัปดาห์นี้", "อาทิตย์นี้", "wtd")):
            out["window_mode"] = "this_week"
            out["days"] = 7
        elif any(k in q for k in ("yesterday", "เมื่อวาน", "yday")):
            out["window_mode"] = "yesterday"
            out["days"] = 1
        elif any(k in q for k in ("today", "วันนี้", "วันนี")):
            out["window_mode"] = "today"
            out["days"] = 1

        toks = [t for t in re.split(r"\s+", raw) if t]
        market_aliases = {
            "th": "thai", "thai": "thai", "thailand": "thai",
            "us": "us", "usa": "us", "america": "us", "american": "us", "เมกา": "us", "อเมริกา": "us",
            "crypto": "crypto", "coin": "crypto", "coins": "crypto",
            "gold": "gold", "xau": "gold", "xauusd": "gold", "ทอง": "gold",
            "global": "global", "world": "global", "ต่างประเทศ": "global", "ทั่วโลก": "global",
            "all": None,
        }
        compare_mode = any(str(t).lower() == 'compare' for t in toks)
        if compare_mode:
            out["compare"] = True
        i = 0
        while i < len(toks):
            tk = str(toks[i]).strip()
            tl = tk.lower()
            if tl in {"compare"}:
                i += 1
                continue
            if tl in {"today", "1d", "d1", "วันนี้", "วันนี"}:
                out["days"] = 1
                out["window_mode"] = "today"
            elif tl in {"yesterday", "yday", "เมื่อวาน"}:
                out["days"] = 1
                out["window_mode"] = "yesterday"
            elif tl in {"week", "thisweek", "this_week", "wtd", "สัปดาห์นี้", "อาทิตย์นี้"}:
                out["days"] = 7
                out["window_mode"] = "this_week"
            elif tl in {"month", "thismonth", "this_month", "mtd", "เดือนนี้"}:
                out["days"] = 30
                out["window_mode"] = "this_month"
            elif (m := re.fullmatch(r"(\d{1,2})d", tl)):
                out["days"] = max(1, min(30, int(m.group(1))))
                out["window_mode"] = "rolling_days"
            elif tl.isdigit():
                out["days"] = max(1, min(30, int(tl)))
                out["window_mode"] = "rolling_days"
            elif tl.startswith("top") and len(tl) > 3 and tl[3:].isdigit():
                out["top"] = max(1, min(20, int(tl[3:])))
            elif tl == "top" and (i + 1) < len(toks) and str(toks[i+1]).isdigit():
                out["top"] = max(1, min(20, int(toks[i+1])))
                i += 1
            elif tl == "vs" and compare_mode:
                pass
            elif tl in market_aliases:
                mkt = market_aliases[tl]
                if compare_mode:
                    if out["left"] is None:
                        out["left"] = mkt
                    elif out["right"] is None:
                        out["right"] = mkt
                else:
                    out["market_filter"] = mkt
                    if tl in {"gold", "xau", "xauusd", "ทอง"} and not out.get("symbol_filter"):
                        out["symbol_filter"] = "XAUUSD"
            elif not compare_mode:
                stop_words = {
                    "this", "that", "week", "month", "today", "yesterday", "top", "compare", "vs",
                    "dashboard", "signal", "signals", "all", "market", "markets",
                }
                if tl in stop_words:
                    i += 1
                    continue
                sym = TelegramAdminBot._normalize_dashboard_symbol(tk)
                if sym and sym not in {"TODAY", "YESTERDAY", "THISWEEK", "THISMONTH"}:
                    out["symbol_filter"] = sym
                    if sym == "XAUUSD" and not out.get("market_filter"):
                        out["market_filter"] = "gold"
                    elif sym.startswith("BTC") or sym.startswith("ETH"):
                        out["market_filter"] = out.get("market_filter") or "crypto"
            i += 1
        if out["compare"]:
            out["left"] = out["left"] or "us"
            out["right"] = out["right"] or "thai"
        return out

    @staticmethod
    def _normalize_dashboard_symbol(token: str) -> str:
        t = str(token or "").strip().upper().replace(" ", "")
        if not t:
            return ""
        alias = {
            "GOLD": "XAUUSD",
            "XAU": "XAUUSD",
            "XAUUSD": "XAUUSD",
            "ETH": "ETHUSD",
            "ETHUSD": "ETHUSD",
            "ETHUSDT": "ETHUSD",
            "ETH/USDT": "ETHUSD",
            "BTC": "BTCUSD",
            "BTCUSD": "BTCUSD",
            "BTCUSDT": "BTCUSD",
            "BTC/USDT": "BTCUSD",
        }
        if t in alias:
            return alias[t]
        compact = t.replace("/", "")
        if compact in alias:
            return alias[compact]
        if t.endswith("/USDT") and len(t) > 5:
            return f"{t[:-5]}USD"
        if t.endswith("USDT") and len(t) > 4:
            return f"{t[:-4]}USD"
        if re.fullmatch(r"[A-Z0-9._-]{3,20}", t):
            return t
        return ""

    @staticmethod
    def _parse_signal_monitor_args(text: str) -> dict:
        parsed = TelegramAdminBot._parse_signal_dashboard_args(text)
        raw = str(text or "").strip()
        stop_words = {
            "today", "yesterday", "this", "week", "month", "thisweek", "thismonth", "wtd", "mtd",
            "dashboard", "signal", "signals", "monitor", "top", "compare", "vs",
        }

        symbols: list[str] = []
        seen: set[str] = set()

        def _add_symbol(token: str) -> None:
            sym = TelegramAdminBot._normalize_dashboard_symbol(token)
            if not sym:
                return
            if sym in {"TODAY", "YESTERDAY", "THIS", "WEEK", "MONTH"}:
                return
            if sym in seen:
                return
            seen.add(sym)
            symbols.append(sym)

        for tk in [t for t in re.split(r"[\s,;|]+", raw) if t]:
            if str(tk).strip().lower() in stop_words:
                continue
            _add_symbol(tk)

        for alias in TelegramAdminBot._extract_signal_filter_symbols_from_text(raw):
            _add_symbol(alias)

        symbol_from_dash = str(parsed.get("symbol_filter") or "").strip().upper()
        if symbol_from_dash:
            _add_symbol(symbol_from_dash)

        market = str(parsed.get("market_filter") or "").strip().lower()
        if (not symbols) and market == "gold":
            symbols = ["XAUUSD"]

        out = {
            "symbol": symbols[0] if symbols else "",
            "symbols": symbols,
            "window_mode": str(parsed.get("window_mode") or "today").strip().lower(),
            "days": int(parsed.get("days", 1) or 1),
        }
        return out

    @staticmethod
    def _monitor_window_bounds(mode: str, days: int) -> tuple[float, float]:
        now = datetime.now(timezone.utc)
        day_start = datetime(now.year, now.month, now.day, tzinfo=timezone.utc)
        m = str(mode or "").strip().lower().replace("-", "_").replace(" ", "_")
        if m == "yesterday":
            start = day_start - timedelta(days=1)
            end = day_start
            return start.timestamp(), end.timestamp()
        if m == "this_week":
            start = day_start - timedelta(days=day_start.weekday())
            return start.timestamp(), now.timestamp()
        if m == "this_month":
            start = day_start.replace(day=1)
            return start.timestamp(), now.timestamp()
        if m == "rolling_days":
            span = max(1, min(30, int(days or 1)))
            start = now - timedelta(days=span)
            return start.timestamp(), now.timestamp()
        # default: today
        return day_start.timestamp(), now.timestamp()

    @staticmethod
    def _signal_monitor_status_label(status: str, lang: str = "en") -> str:
        ui = str(lang or "en").lower()
        table = {
            "no_signal": {
                "en": "No new signal this round",
                "th": "ยังไม่มีสัญญาณใหม่ในรอบนี้",
                "de": "Kein neues Signal in diesem Zyklus",
            },
            "no_setup": {
                "en": "No setup passed base engine",
                "th": "ยังไม่มี setup ผ่าน engine หลัก",
                "de": "Kein Setup hat den Basis-Filter bestanden",
            },
            "no_h1_data": {
                "en": "Missing H1 data",
                "th": "ข้อมูล H1 ยังไม่พอ",
                "de": "H1-Daten fehlen",
            },
            "trap_guard_blocked": {
                "en": "Blocked by XAU trap guard",
                "th": "ถูกบล็อกโดย XAU trap guard",
                "de": "Durch XAU-Trap-Guard blockiert",
            },
            "below_confidence": {
                "en": "Signal below confidence threshold",
                "th": "สัญญาณต่ำกว่า confidence threshold",
                "de": "Signal unter Confidence-Schwelle",
            },
            "cooldown_suppressed": {
                "en": "Signal suppressed by cooldown",
                "th": "สัญญาณถูก cooldown กดไว้",
                "de": "Signal durch Cooldown unterdrückt",
            },
            "ready": {
                "en": "New signal ready",
                "th": "มีสัญญาณใหม่พร้อมใช้งาน",
                "de": "Neues Signal bereit",
            },
            "sent": {
                "en": "Signal sent",
                "th": "ส่งสัญญาณแล้ว",
                "de": "Signal gesendet",
            },
            "sent_manual_bypass_cooldown": {
                "en": "Signal sent (manual cooldown bypass)",
                "th": "ส่งสัญญาณแล้ว (manual bypass cooldown)",
                "de": "Signal gesendet (manueller Cooldown-Bypass)",
            },
            "m1_rejected": {
                "en": "Trigger rejected by M1 filter",
                "th": "M1 filter ไม่ยืนยันทิศทาง",
                "de": "Trigger vom M1-Filter abgelehnt",
            },
            "regime_blocked": {
                "en": "Blocked by higher-timeframe regime guard",
                "th": "ถูกบล็อกโดย higher-timeframe regime guard",
                "de": "Durch Higher-Timeframe-Regime-Guard blockiert",
            },
            "disabled": {
                "en": "Scanner disabled",
                "th": "scanner ถูกปิดอยู่",
                "de": "Scanner deaktiviert",
            },
            "unsupported_symbol": {
                "en": "Unsupported monitor symbol",
                "th": "ยังไม่รองรับ monitor สำหรับสัญลักษณ์นี้",
                "de": "Dieses Symbol wird im Monitor nicht unterstützt",
            },
            "error": {
                "en": "Scan error",
                "th": "เกิดข้อผิดพลาดในการสแกน",
                "de": "Scan-Fehler",
            },
        }
        rec = table.get(str(status or "").strip().lower())
        if rec:
            return str(rec.get(ui) or rec.get("en"))
        return str(status or "unknown")

    @staticmethod
    def _fmt_monitor_profit(value: float) -> str:
        try:
            val = round(float(value), 2)
        except Exception:
            return str(value)
        if abs(val - round(val)) < 1e-9:
            return str(int(round(val)))
        return f"{val:.2f}".rstrip("0").rstrip(".")

    @classmethod
    def _format_monitor_perf_line(
        cls,
        label: str,
        count: int,
        wins: int,
        losses: int,
        profit_usd: float,
    ) -> str:
        ptxt = cls._fmt_monitor_profit(profit_usd)
        return f"{label} {int(count)} W{int(wins)}/L{int(losses)} Profit={ptxt}$"

    @staticmethod
    def _ts_to_iso_utc(ts: Optional[float]) -> str:
        if ts is None:
            return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        try:
            return datetime.fromtimestamp(float(ts), tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        except Exception:
            return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    @staticmethod
    def _monitor_symbol_aliases(symbol: str) -> tuple[str, ...]:
        token = str(symbol or "").strip().upper()
        aliases = {token}
        if token in {"XAU", "GOLD"}:
            aliases.add("XAUUSD")
        if token.endswith("USD") and len(token) > 3:
            base = token[:-3]
            aliases.add(f"{base}/USDT")
            aliases.add(f"{base}USDT")
        if token.endswith("USDT") and len(token) > 4:
            base = token[:-4]
            aliases.add(f"{base}/USDT")
            aliases.add(f"{base}USD")
        if token.endswith("/USDT") and len(token) > 6:
            base = token.split("/", 1)[0]
            aliases.add(f"{base}USD")
            aliases.add(f"{base}USDT")
        return tuple(sorted(a for a in aliases if a))

    @staticmethod
    def _mt5_lane_from_source(source: str) -> str:
        src = str(source or "").strip().lower()
        if not src:
            return "main"
        if ":bypass" in src or src.endswith("bypass"):
            return "bypass"
        lane_tag = str(getattr(config, "MT5_BEST_LANE_TAG", "winner") or "winner").strip().lower()
        if lane_tag and (f":{lane_tag}" in src or src == lane_tag):
            return "winner"
        return "main"

    def _load_mt5_exec_stats_filtered(self, symbol: str, start_ts: Optional[float], end_ts: Optional[float]) -> dict:
        lane_names = ("main", "winner", "bypass")

        def _empty_lane() -> dict:
            return {
                "sent": 0,
                "filled": 0,
                "skipped": 0,
                "guard_blocked": 0,
                "errors": 0,
                "fill_rate_pct": 0.0,
                "top_block_reason": "",
            }

        out = {
            "enabled": bool(getattr(config, "MT5_AUTOPILOT_ENABLED", False)),
            "available": False,
            "sent": 0,
            "filled": 0,
            "skipped": 0,
            "guard_blocked": 0,
            "errors": 0,
            "fill_rate_pct": 0.0,
            "top_block_reason": "",
            "lanes": {name: _empty_lane() for name in lane_names},
        }
        if not out["enabled"]:
            return out
        try:
            db_cfg = str(getattr(config, "MT5_AUTOPILOT_DB_PATH", "") or "").strip()
            db_path = db_cfg or os.path.join(
                os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                "data",
                "mt5_autopilot.db",
            )
            if not os.path.exists(db_path):
                return out
            aliases = self._monitor_symbol_aliases(symbol)
            if not aliases:
                return out
            since_iso = self._ts_to_iso_utc(start_ts)
            until_iso = self._ts_to_iso_utc(end_ts)
            account_key = ""
            try:
                from learning.mt5_autopilot_core import mt5_autopilot_core

                gate = mt5_autopilot_core.pre_trade_gate(signal=None, source="signal_monitor")
                account_key = str(getattr(gate, "account_key", "") or "").strip()
            except Exception:
                account_key = ""

            ph = ",".join(["?"] * len(aliases))
            where = [
                "created_at >= ?",
                "created_at < ?",
                f"(UPPER(COALESCE(signal_symbol,'')) IN ({ph}) OR UPPER(COALESCE(broker_symbol,'')) IN ({ph}))",
            ]
            params: list = [since_iso, until_iso]
            params.extend(list(aliases))
            params.extend(list(aliases))
            if account_key:
                where.append("account_key = ?")
                params.append(account_key)
            where_sql = " AND ".join(where)
            with sqlite3.connect(db_path) as conn:
                status_rows = conn.execute(
                    f"""
                    SELECT COALESCE(source,''), COALESCE(mt5_status,''), COUNT(*)
                      FROM mt5_execution_journal
                     WHERE {where_sql}
                     GROUP BY COALESCE(source,''), COALESCE(mt5_status,'')
                    """,
                    tuple(params),
                ).fetchall()
                lane_stats = {name: _empty_lane() for name in lane_names}
                for src, st, c in list(status_rows or []):
                    lane = self._mt5_lane_from_source(str(src or ""))
                    if lane not in lane_stats:
                        lane = "main"
                    count = int(c or 0)
                    ls = lane_stats[lane]
                    ls["sent"] += count
                    st_l = str(st or "").strip().lower()
                    if st_l in {"filled", "dry_run"}:
                        ls["filled"] += count
                    elif st_l == "skipped":
                        ls["skipped"] += count
                    elif st_l == "guard_blocked":
                        ls["guard_blocked"] += count
                    elif st_l in {"rejected", "error", "invalid_stops"}:
                        ls["errors"] += count

                for name in lane_names:
                    sent_lane = int(lane_stats[name]["sent"] or 0)
                    filled_lane = int(lane_stats[name]["filled"] or 0)
                    lane_stats[name]["fill_rate_pct"] = round((100.0 * filled_lane / sent_lane), 2) if sent_lane > 0 else 0.0

                sent = sum(int(lane_stats[name]["sent"] or 0) for name in lane_names)
                filled = sum(int(lane_stats[name]["filled"] or 0) for name in lane_names)
                skipped = sum(int(lane_stats[name]["skipped"] or 0) for name in lane_names)
                blocked = sum(int(lane_stats[name]["guard_blocked"] or 0) for name in lane_names)
                errors = sum(int(lane_stats[name]["errors"] or 0) for name in lane_names)
                out.update(
                    {
                        "available": True,
                        "sent": sent,
                        "filled": filled,
                        "skipped": skipped,
                        "guard_blocked": blocked,
                        "errors": errors,
                        "fill_rate_pct": round((100.0 * filled / sent), 2) if sent > 0 else 0.0,
                        "lanes": lane_stats,
                    }
                )
                blocked_rows = conn.execute(
                    f"""
                    SELECT COALESCE(source,''), COALESCE(mt5_message, ''), COUNT(*) AS c
                      FROM mt5_execution_journal
                     WHERE {where_sql}
                       AND mt5_status IN ('skipped','guard_blocked')
                     GROUP BY COALESCE(source,''), COALESCE(mt5_message,'')
                     ORDER BY c DESC, source ASC
                    """,
                    tuple(params),
                ).fetchall()
                best_overall_msg = ""
                best_overall_count = -1
                lane_top: dict[str, tuple[str, int]] = {name: ("", 0) for name in lane_names}
                for src, msg, c in list(blocked_rows or []):
                    count = int(c or 0)
                    msg_text = str(msg or "").strip()
                    lane = self._mt5_lane_from_source(str(src or ""))
                    if lane not in lane_top:
                        lane = "main"
                    prev_msg, prev_count = lane_top[lane]
                    if count > prev_count and msg_text:
                        lane_top[lane] = (msg_text, count)
                    if count > best_overall_count and msg_text:
                        best_overall_msg = msg_text
                        best_overall_count = count
                if best_overall_msg:
                    out["top_block_reason"] = best_overall_msg[:220]
                lanes_out = dict(out.get("lanes") or {})
                for name in lane_names:
                    if name not in lanes_out:
                        lanes_out[name] = _empty_lane()
                    top_msg = str(lane_top.get(name, ("", 0))[0] or "").strip()
                    if top_msg:
                        lanes_out[name]["top_block_reason"] = top_msg[:220]
                out["lanes"] = lanes_out
        except Exception:
            return out
        return out

    def _load_crypto_lane_stats_filtered(self, symbol: str, start_ts: Optional[float], end_ts: Optional[float]) -> dict:
        symbol_up = str(symbol or "").strip().upper()
        source_map = {
            "BTCUSD": {"main": "scalp_btcusd", "winner": "scalp_btcusd:winner"},
            "ETHUSD": {"main": "scalp_ethusd", "winner": "scalp_ethusd:winner"},
        }
        selected = dict(source_map.get(symbol_up) or {})

        def _bucket() -> dict:
            return {
                "sent": 0,
                "filled": 0,
                "resolved": 0,
                "wins": 0,
                "losses": 0,
                "pnl": 0.0,
                "fill_rate_pct": 0.0,
                "win_rate_pct": 0.0,
            }

        out = {
            "available": False,
            "symbol": symbol_up,
            "lanes": {name: _bucket() for name in ("main", "winner")},
        }
        if not selected:
            return out
        try:
            db_cfg = str(getattr(config, "MT5_AUTOPILOT_DB_PATH", "") or "").strip()
            db_path = db_cfg or os.path.join(
                os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                "data",
                "mt5_autopilot.db",
            )
            if not os.path.exists(db_path):
                return out
            since_iso = self._ts_to_iso_utc(start_ts)
            until_iso = self._ts_to_iso_utc(end_ts)
            account_key = ""
            try:
                from learning.mt5_autopilot_core import mt5_autopilot_core

                gate = mt5_autopilot_core.pre_trade_gate(signal=None, source="signal_monitor")
                account_key = str(getattr(gate, "account_key", "") or "").strip()
            except Exception:
                account_key = ""

            sources = [selected["main"], selected["winner"]]
            ph = ",".join(["?"] * len(sources))
            where = [
                "created_at >= ?",
                "created_at < ?",
                f"LOWER(COALESCE(source,'')) IN ({ph})",
            ]
            params: list = [since_iso, until_iso]
            params.extend([str(src).strip().lower() for src in sources])
            if account_key:
                where.append("account_key = ?")
                params.append(account_key)
            where_sql = " AND ".join(where)
            with sqlite3.connect(db_path) as conn:
                status_rows = conn.execute(
                    f"""
                    SELECT LOWER(COALESCE(source,'')), COALESCE(mt5_status,''), COUNT(*)
                      FROM mt5_execution_journal
                     WHERE {where_sql}
                     GROUP BY LOWER(COALESCE(source,'')), COALESCE(mt5_status,'')
                    """,
                    tuple(params),
                ).fetchall()
                resolved_rows = conn.execute(
                    f"""
                    SELECT LOWER(COALESCE(source,'')),
                           SUM(CASE WHEN resolved = 1 THEN 1 ELSE 0 END) AS resolved,
                           SUM(CASE WHEN resolved = 1 AND outcome = 1 THEN 1 ELSE 0 END) AS wins,
                           SUM(CASE WHEN resolved = 1 AND outcome = 0 THEN 1 ELSE 0 END) AS losses,
                           SUM(CASE WHEN resolved = 1 THEN COALESCE(pnl, 0.0) ELSE 0.0 END) AS pnl
                      FROM mt5_execution_journal
                     WHERE {where_sql}
                     GROUP BY LOWER(COALESCE(source,''))
                    """,
                    tuple(params),
                ).fetchall()
            lookup = {str(v).strip().lower(): k for k, v in selected.items()}
            lanes = {name: _bucket() for name in ("main", "winner")}
            for src, st, c in list(status_rows or []):
                lane = lookup.get(str(src or "").strip().lower())
                if not lane:
                    continue
                count = int(c or 0)
                bucket = lanes[lane]
                bucket["sent"] += count
                st_l = str(st or "").strip().lower()
                if st_l in {"filled", "dry_run"}:
                    bucket["filled"] += count
            for src, resolved, wins, losses, pnl in list(resolved_rows or []):
                lane = lookup.get(str(src or "").strip().lower())
                if not lane:
                    continue
                bucket = lanes[lane]
                bucket["resolved"] = int(resolved or 0)
                bucket["wins"] = int(wins or 0)
                bucket["losses"] = int(losses or 0)
                bucket["pnl"] = round(float(pnl or 0.0), 2)
            for lane in ("main", "winner"):
                bucket = lanes[lane]
                sent = int(bucket["sent"] or 0)
                filled = int(bucket["filled"] or 0)
                resolved = int(bucket["resolved"] or 0)
                wins = int(bucket["wins"] or 0)
                bucket["fill_rate_pct"] = round((100.0 * filled / sent), 2) if sent > 0 else 0.0
                bucket["win_rate_pct"] = round((100.0 * wins / resolved), 2) if resolved > 0 else 0.0
            out["available"] = True
            out["lanes"] = lanes
        except Exception:
            return out
        return out

    def _load_ctrader_lane_stats_filtered(self, symbol: str, start_ts: Optional[float], end_ts: Optional[float]) -> dict:
        symbol_up = str(symbol or "").strip().upper()
        out = {"available": False, "symbol": symbol_up, "lanes": {}}
        if symbol_up not in {"BTCUSD", "ETHUSD"}:
            return out
        try:
            from execution.ctrader_executor import ctrader_executor

            start_utc = self._ts_to_iso_utc(start_ts)
            end_utc = self._ts_to_iso_utc(end_ts)
            payload = ctrader_executor.get_lane_stats(symbol=symbol_up, start_utc=start_utc, end_utc=end_utc)
            if isinstance(payload, dict):
                return payload
        except Exception:
            return out
        return out

    def _build_signal_monitor_payload(self, symbol: str, window_mode: str = "today", days: int = 1) -> dict:
        from api.signal_store import signal_store
        from api.scalp_signal_store import scalp_store
        from scanners.xauusd import xauusd_scanner
        from scanners.scalping_scanner import scalping_scanner
        from scanners.fx_major_scanner import fx_major_scanner
        from market.data_fetcher import xauusd_provider, crypto_provider, fx_provider, session_manager

        symbol_up = str(symbol or "XAUUSD").strip().upper() or "XAUUSD"
        window = str(window_mode or "today").strip().lower()
        span_days = max(1, int(days or 1))
        session_info = session_manager.get_session_info() or {}
        active_sessions = ", ".join(list(session_info.get("active_sessions", []) or [])) or "unknown"

        payload = {
            "symbol": symbol_up,
            "window_mode": window,
            "days": span_days,
            "session": active_sessions,
            "status": "unknown",
            "unmet": [],
            "notes": [],
            "price": None,
            "confidence": None,
            "confidence_raw": None,
            "confidence_threshold": None,
            "tf": "",
        }

        if symbol_up == "XAUUSD":
            signal = xauusd_scanner.scan()
            diag = dict(xauusd_scanner.get_last_scan_diagnostics() or {})
            payload["status"] = str(diag.get("status") or "no_signal")
            payload["unmet"] = [str(x) for x in list(diag.get("unmet") or []) if str(x).strip()]
            payload["notes"] = [str(x) for x in list(diag.get("notes") or []) if str(x).strip()]
            fallback = dict(diag.get("fallback") or {})
            fb_reason = str(fallback.get("reason", "") or "").strip()
            if fb_reason:
                fb_note = f"fallback:{fb_reason}"
                if fb_note not in payload["notes"]:
                    payload["notes"].append(fb_note)
            price = diag.get("current_price")
            if price is None:
                price = xauusd_provider.get_current_price()
            payload["price"] = price

            threshold = float(getattr(config, "MIN_SIGNAL_CONFIDENCE", 70.0) or 70.0)
            payload["confidence_threshold"] = threshold
            if signal is not None:
                conf = float(getattr(signal, "confidence", 0.0) or 0.0)
                payload["confidence"] = conf
                raw_scores = dict(getattr(signal, "raw_scores", {}) or {})
                trend_tf = str(raw_scores.get("trend_tf") or "").strip().upper()
                structure_tf = str(raw_scores.get("structure_tf") or "").strip().upper()
                entry_tf = str(raw_scores.get("entry_tf") or "").strip().upper()
                if trend_tf and structure_tf and entry_tf:
                    payload["tf"] = f"{trend_tf}/{structure_tf}/{entry_tf}"
                raw_conf = raw_scores.get("confidence_pre_neural")
                if raw_conf is not None:
                    try:
                        payload["confidence_raw"] = float(raw_conf)
                    except Exception:
                        payload["confidence_raw"] = None
                if payload["price"] is None:
                    try:
                        payload["price"] = float(getattr(signal, "entry", 0.0) or 0.0)
                    except Exception:
                        payload["price"] = None
                payload["status"] = "ready" if conf >= threshold else "below_confidence"
            else:
                if payload["status"] in {"scan_started", "signal_eval"}:
                    payload["status"] = "no_signal"
                if payload["status"] == "no_signal" and not payload["unmet"]:
                    payload["unmet"] = ["base_setup"]
            if not payload["tf"]:
                fallback_diag = dict(diag.get("fallback_diag") or {})
                tf_map = dict(fallback_diag.get("timeframes") or {})
                trend_tf = str(tf_map.get("trend") or "").strip().upper()
                structure_tf = str(tf_map.get("structure") or "").strip().upper()
                entry_tf = str(tf_map.get("entry") or "").strip().upper()
                if trend_tf and structure_tf and entry_tf:
                    payload["tf"] = f"{trend_tf}/{structure_tf}/{entry_tf}"
        else:
            row = None
            market_symbol = ""
            fx_symbol = ""
            if symbol_up == "ETHUSD":
                row = scalping_scanner.scan_eth(require_enabled=False)
                market_symbol = str(getattr(config, "SCALPING_ETH_SYMBOL", "ETH/USDT") or "ETH/USDT").strip().upper()
            elif symbol_up == "BTCUSD":
                row = scalping_scanner.scan_btc(require_enabled=False)
                market_symbol = str(getattr(config, "SCALPING_BTC_SYMBOL", "BTC/USDT") or "BTC/USDT").strip().upper()
            elif symbol_up in {str(x).upper() for x in (config.get_fx_major_symbols() or [])}:
                fx_symbol = symbol_up
            else:
                payload["status"] = "unsupported_symbol"
                payload["notes"].append("supported_monitor_symbols:XAUUSD,ETHUSD,BTCUSD,+FX_majors")

            if row is not None:
                payload["status"] = str(getattr(row, "status", "unknown") or "unknown")
                reason = str(getattr(row, "reason", "") or "").strip()
                if reason:
                    payload["notes"].append(reason)
                trigger = dict(getattr(row, "trigger", {}) or {})
                fb = dict(trigger.get("fallback") or {})
                tf_map = dict(fb.get("timeframes") or {})
                trend_tf = str(tf_map.get("trend") or "").strip().upper()
                structure_tf = str(tf_map.get("structure") or "").strip().upper()
                entry_tf = str(tf_map.get("entry") or "").strip().upper()
                if trend_tf and structure_tf and entry_tf:
                    payload["tf"] = f"{trend_tf}/{structure_tf}/{entry_tf}"
                xau_unmet = [str(x) for x in list(trigger.get("xau_unmet") or []) if str(x).strip()]
                if xau_unmet:
                    payload["unmet"] = xau_unmet
                signal = getattr(row, "signal", None)
                if signal is not None:
                    try:
                        payload["price"] = float(getattr(signal, "entry", 0.0) or 0.0)
                    except Exception:
                        payload["price"] = None
                    payload["confidence"] = float(getattr(signal, "confidence", 0.0) or 0.0)
                    if not payload["tf"]:
                        raw_scores = dict(getattr(signal, "raw_scores", {}) or {})
                        trend_tf = str(raw_scores.get("trend_tf") or "").strip().upper()
                        structure_tf = str(raw_scores.get("structure_tf") or "").strip().upper()
                        entry_tf = str(raw_scores.get("entry_tf") or "").strip().upper()
                        if trend_tf and structure_tf and entry_tf:
                            payload["tf"] = f"{trend_tf}/{structure_tf}/{entry_tf}"
                        else:
                            scalp_entry_tf = str(raw_scores.get("scalping_entry_tf") or "").strip().upper()
                            scalp_trigger_tf = str(raw_scores.get("scalping_trigger_tf") or "").strip().upper()
                            if scalp_entry_tf and scalp_trigger_tf:
                                payload["tf"] = f"ENTRY {scalp_entry_tf} / TRIGGER {scalp_trigger_tf}"
                m = re.search(r"confidence<(\d+(?:\.\d+)?)", reason)
                if m:
                    try:
                        payload["confidence_threshold"] = float(m.group(1))
                    except Exception:
                        payload["confidence_threshold"] = None
                if payload["price"] is None and market_symbol:
                    payload["price"] = crypto_provider.get_current_price(market_symbol)
            elif fx_symbol:
                opps = fx_major_scanner.scan(symbols=[fx_symbol])
                if opps:
                    opp = None
                    for cand in opps:
                        sig_sym = str(getattr(getattr(cand, "signal", None), "symbol", "") or "").strip().upper()
                        if sig_sym == fx_symbol:
                            opp = cand
                            break
                    if opp is None:
                        opp = opps[0]
                    sig_fx = getattr(opp, "signal", None)
                    if sig_fx is not None:
                        conf_fx = float(getattr(sig_fx, "confidence", 0.0) or 0.0)
                        th_fx = float(getattr(config, "MT5_MIN_SIGNAL_CONFIDENCE_FX", getattr(config, "MIN_SIGNAL_CONFIDENCE", 70.0)) or 70.0)
                        payload["confidence"] = conf_fx
                        payload["confidence_threshold"] = th_fx
                        payload["status"] = "ready" if conf_fx >= th_fx else "below_confidence"
                        try:
                            payload["price"] = float(getattr(sig_fx, "entry", 0.0) or 0.0)
                        except Exception:
                            payload["price"] = fx_provider.get_current_price(fx_symbol)
                        raw_scores = dict(getattr(sig_fx, "raw_scores", {}) or {})
                        trend_tf = str(raw_scores.get("trend_tf") or "").strip().upper()
                        entry_tf = str(raw_scores.get("entry_tf") or "").strip().upper()
                        if trend_tf and entry_tf:
                            payload["tf"] = f"{trend_tf}/{entry_tf}"
                        else:
                            payload["tf"] = f"{str(getattr(config, 'FX_TREND_TF', '4h')).upper()}/{str(getattr(config, 'FX_ENTRY_TF', '1h')).upper()}"
                    if payload["status"] == "unknown":
                        payload["status"] = "no_signal"
                else:
                    payload["status"] = "no_signal"
                    try:
                        diag = dict(fx_major_scanner.get_last_scan_diagnostics() or {})
                        reject = dict(diag.get("reject_reasons", {}) or {})
                        top_reason = ""
                        top_count = 0
                        for k, v in reject.items():
                            vv = int(v or 0)
                            if vv > top_count:
                                top_reason = str(k)
                                top_count = vv
                        if top_reason:
                            payload["notes"].append(f"fx_reject:{top_reason}")
                    except Exception:
                        pass
                    if payload["price"] is None:
                        payload["price"] = fx_provider.get_current_price(fx_symbol)

        # De-duplicate diagnostics while preserving order to reduce noisy repeats.
        if payload.get("unmet"):
            payload["unmet"] = list(dict.fromkeys(str(x) for x in list(payload.get("unmet") or []) if str(x).strip()))
        if payload.get("notes"):
            payload["notes"] = list(dict.fromkeys(str(x) for x in list(payload.get("notes") or []) if str(x).strip()))

        start_ts, end_ts = self._monitor_window_bounds(window, span_days)
        payload["main_stats"] = signal_store.get_performance_stats_filtered(
            symbol=symbol_up,
            start_ts=start_ts,
            end_ts=end_ts,
        )
        payload["scalp_stats"] = scalp_store.get_stats_filtered(
            symbol=symbol_up,
            start_ts=start_ts,
            end_ts=end_ts,
            last_n=None,
        )
        payload["mt5_exec_stats"] = self._load_mt5_exec_stats_filtered(
            symbol=symbol_up,
            start_ts=start_ts,
            end_ts=end_ts,
        )
        if symbol_up in {"BTCUSD", "ETHUSD"}:
            payload["crypto_lane_stats"] = self._load_crypto_lane_stats_filtered(
                symbol=symbol_up,
                start_ts=start_ts,
                end_ts=end_ts,
            )
        if symbol_up in {"BTCUSD", "ETHUSD", "XAUUSD"}:
            payload["ctrader_lane_stats"] = self._load_ctrader_lane_stats_filtered(
                symbol=symbol_up,
                start_ts=start_ts,
                end_ts=end_ts,
            )
        return payload

    def _format_signal_monitor_text(self, payload: dict, lang: str = "en", chat_id: Optional[int] = None) -> str:
        ui = str(lang or "en").lower()

        def t(en: str, th: str, de: Optional[str] = None) -> str:
            if ui == "th":
                return th
            if ui == "de":
                return de or en
            return en

        symbol = str(payload.get("symbol") or "XAUUSD").strip().upper()
        status_key = str(payload.get("status") or "unknown")
        status_label = self._signal_monitor_status_label(status_key, lang=ui)
        price = payload.get("price")
        price_text = "-"
        if price is not None:
            try:
                price_text = f"${float(price):.2f}"
            except Exception:
                price_text = str(price)

        local_time = self._monitor_local_time_text(chat_id)
        lines = [
            f"[{price_text}] [{local_time}]",
            f"Price: {price_text}",
            f"Status: {status_label}",
        ]

        unmet = [str(x) for x in list(payload.get("unmet") or []) if str(x).strip()]
        if unmet:
            lines.append("Unmet: " + ", ".join(unmet[:5]))
        notes = [str(x) for x in list(payload.get("notes") or []) if str(x).strip()]
        if notes:
            lines.append("Notes: " + " | ".join(notes[:3]))

        main_stats = dict(payload.get("main_stats") or {})
        scalp_stats = dict(payload.get("scalp_stats") or {})
        main_total = int(main_stats.get("total_signals", 0) or 0)
        main_done = int(main_stats.get("completed_signals", 0) or 0)
        main_pending = max(0, main_total - main_done)
        scalp_done = int(scalp_stats.get("count", 0) or 0)
        scalp_total = int(scalp_stats.get("total_signals", scalp_done) or scalp_done)
        scalp_pending = max(0, int(scalp_stats.get("pending_count", scalp_total - scalp_done) or 0))
        signal_closed_label = t("Signal closed (model)", "Signal ปิดผล (model)", "Signal geschlossen (Modell)")
        scalp_closed_label = t("Scalp closed (model)", "Scalp ปิดผล (model)", "Scalp geschlossen (Modell)")
        lines.append(
            self._format_monitor_perf_line(
                signal_closed_label,
                main_done,
                int(main_stats.get("wins", 0) or 0),
                int(main_stats.get("losses", 0) or 0),
                float(main_stats.get("total_pnl_usd", 0.0) or 0.0),
            )
        )
        lines.append(
            self._format_monitor_perf_line(
                scalp_closed_label,
                scalp_done,
                int(scalp_stats.get("wins", 0) or 0),
                int(scalp_stats.get("losses", 0) or 0),
                float(scalp_stats.get("total_usd", 0.0) or 0.0),
            )
        )
        lines.append(
            f"{t('Track (model)', 'ติดตาม (model)', 'Tracking (Modell)')}: "
            f"Signal sent {main_total} (pending {main_pending}) | "
            f"Scalp sent {scalp_total} (pending {scalp_pending})"
        )
        mt5_stats = dict(payload.get("mt5_exec_stats") or {})
        if bool(mt5_stats.get("enabled")) and bool(mt5_stats.get("available")):
            lines.append(
                f"{t('MT5 exec', 'MT5 ส่งคำสั่งจริง', 'MT5 Ausführung')}: "
                f"sent {int(mt5_stats.get('sent', 0) or 0)} | "
                f"filled {int(mt5_stats.get('filled', 0) or 0)} | "
                f"skipped {int(mt5_stats.get('skipped', 0) or 0)} | "
                f"blocked {int(mt5_stats.get('guard_blocked', 0) or 0)} | "
                f"fill {float(mt5_stats.get('fill_rate_pct', 0.0) or 0.0):.1f}%"
            )
            top_block = str(mt5_stats.get("top_block_reason", "") or "").strip()
            if top_block:
                lines.append(f"{t('MT5 top block', 'MT5 เหตุผลบล็อกหลัก', 'MT5 Hauptblock')}: {top_block}")
            lane_stats = dict(mt5_stats.get("lanes") or {})
            lane_keys = [
                ("main", t("main", "main", "main")),
                ("winner", t("winner", "winner", "winner")),
                ("bypass", t("bypass", "bypass", "bypass")),
            ]
            lane_parts = []
            for key, label in lane_keys:
                ls = dict(lane_stats.get(key) or {})
                lane_parts.append(
                    f"{label} {int(ls.get('sent', 0) or 0)}/"
                    f"{int(ls.get('filled', 0) or 0)}/"
                    f"{int(ls.get('skipped', 0) or 0)}/"
                    f"{int(ls.get('guard_blocked', 0) or 0)}"
                )
            lines.append(
                f"{t('MT5 lanes', 'MT5 แยก lane', 'MT5 Lanes')}: "
                + " | ".join(lane_parts)
            )
        crypto_lane_stats = dict(payload.get("crypto_lane_stats") or {})
        if bool(crypto_lane_stats.get("available")) and symbol in {"BTCUSD", "ETHUSD"}:
            lanes = dict(crypto_lane_stats.get("lanes") or {})
            main_lane = dict(lanes.get("main") or {})
            winner_lane = dict(lanes.get("winner") or {})
            lines.append(
                f"{t('Crypto lanes', 'Crypto แยก lane', 'Krypto-Lanes')}: "
                f"main S{int(main_lane.get('sent', 0) or 0)} "
                f"F{int(main_lane.get('filled', 0) or 0)} "
                f"R{int(main_lane.get('resolved', 0) or 0)} "
                f"W{int(main_lane.get('wins', 0) or 0)}/L{int(main_lane.get('losses', 0) or 0)} "
                f"P={float(main_lane.get('pnl', 0.0) or 0.0):.2f}$ | "
                f"winner S{int(winner_lane.get('sent', 0) or 0)} "
                f"F{int(winner_lane.get('filled', 0) or 0)} "
                f"R{int(winner_lane.get('resolved', 0) or 0)} "
                f"W{int(winner_lane.get('wins', 0) or 0)}/L{int(winner_lane.get('losses', 0) or 0)} "
                f"P={float(winner_lane.get('pnl', 0.0) or 0.0):.2f}$"
            )
        ctrader_lane_stats = dict(payload.get("ctrader_lane_stats") or {})
        if bool(ctrader_lane_stats.get("available")) and symbol in {"BTCUSD", "ETHUSD", "XAUUSD"}:
            lanes = dict(ctrader_lane_stats.get("lanes") or {})
            main_lane = dict(lanes.get("main") or {})
            winner_lane = dict(lanes.get("winner") or {})
            lines.append(
                f"{t('cTrader lanes', 'cTrader แยก lane', 'cTrader-Lanes')}: "
                f"main S{int(main_lane.get('sent', 0) or 0)} "
                f"F{int(main_lane.get('filled', 0) or 0)} "
                f"O{int(main_lane.get('open', 0) or 0)} "
                f"R{int(main_lane.get('resolved', 0) or 0)} "
                f"W{int(main_lane.get('wins', 0) or 0)}/L{int(main_lane.get('losses', 0) or 0)} "
                f"P={float(main_lane.get('pnl', 0.0) or 0.0):.2f}$ | "
                f"winner S{int(winner_lane.get('sent', 0) or 0)} "
                f"F{int(winner_lane.get('filled', 0) or 0)} "
                f"O{int(winner_lane.get('open', 0) or 0)} "
                f"R{int(winner_lane.get('resolved', 0) or 0)} "
                f"W{int(winner_lane.get('wins', 0) or 0)}/L{int(winner_lane.get('losses', 0) or 0)} "
                f"P={float(winner_lane.get('pnl', 0.0) or 0.0):.2f}$"
            )
        return "\n".join(lines)

    @staticmethod
    def _signal_dashboard_window_label(mode: str, days: int, lang: str = "en") -> str:
        ui = str(lang or "en").lower()
        m = str(mode or "").strip().lower().replace("-", "_").replace(" ", "_")
        labels = {
            "today": {"en": "today", "th": "วันนี้", "de": "heute"},
            "yesterday": {"en": "yesterday", "th": "เมื่อวาน", "de": "gestern"},
            "this_week": {"en": "this week", "th": "สัปดาห์นี้", "de": "diese Woche"},
            "this_month": {"en": "this month", "th": "เดือนนี้", "de": "dieser Monat"},
            "rolling_days": {
                "en": f"last {int(days or 1)} days",
                "th": f"ย้อนหลัง {int(days or 1)} วัน",
                "de": f"letzte {int(days or 1)} Tage",
            },
        }
        rec = labels.get(m, labels["rolling_days"])
        return str(rec.get(ui) or rec.get("en"))

    @staticmethod
    def _signal_dashboard_market_label(market: str, lang: str = "en") -> str:
        ui = str(lang or "en").lower()
        m = str(market or "").strip().lower().replace("-", "_").replace(" ", "_")
        table = {
            "gold": {"en": "Gold", "th": "ทอง (Gold)", "de": "Gold"},
            "thai": {"en": "Thailand Stocks", "th": "หุ้นไทย", "de": "Thailand-Aktien"},
            "thai_stocks": {"en": "Thailand Stocks", "th": "หุ้นไทย", "de": "Thailand-Aktien"},
            "us": {"en": "US Stocks", "th": "หุ้นสหรัฐ", "de": "US-Aktien"},
            "us_stocks": {"en": "US Stocks", "th": "หุ้นสหรัฐ", "de": "US-Aktien"},
            "global": {"en": "Global Stocks", "th": "หุ้นต่างประเทศ", "de": "Globale Aktien"},
            "global_stocks": {"en": "Global Stocks", "th": "หุ้นต่างประเทศ", "de": "Globale Aktien"},
            "crypto": {"en": "Crypto", "th": "คริปโต (Crypto)", "de": "Krypto"},
            "other": {"en": "Other", "th": "อื่นๆ", "de": "Andere"},
        }
        rec = table.get(m)
        if rec:
            return str(rec.get(ui) or rec.get("en"))
        return str(market or "-")

    @staticmethod
    def _normalize_signal_filter_symbol(token: str) -> str:
        t = str(token or "").strip().upper().replace(" ", "")
        if not t:
            return ""
        alias = {
            "GOLD": "XAUUSD",
            "XAU": "XAUUSD",
            "BTCUSDT": "BTC/USDT",
            "ETHUSDT": "ETH/USDT",
        }
        if t in alias:
            return alias[t]
        if t.endswith("USDT") and "/" not in t and len(t) > 4:
            return f"{t[:-4]}/USDT"
        return t

    @classmethod
    def _parse_signal_filter_symbols(cls, raw: str) -> list[str]:
        out: list[str] = []
        seen: set[str] = set()
        for part in re.split(r"[,\s]+", str(raw or "").strip()):
            token = cls._normalize_signal_filter_symbol(part)
            if not token or token in {"ALL", "*"}:
                continue
            if token in seen:
                continue
            seen.add(token)
            out.append(token)
        return out

    @classmethod
    def _extract_signal_filter_symbols_from_text(cls, raw_text: str) -> list[str]:
        """
        Extract signal symbols from natural-language text without requiring strict command format.
        Example: "show only gold btc eth" -> ["XAUUSD", "BTC/USDT", "ETH/USDT"].
        """
        raw = str(raw_text or "").strip()
        q = raw.lower()
        out: list[str] = []
        seen: set[str] = set()

        def _add(sym: str) -> None:
            token = cls._normalize_signal_filter_symbol(sym)
            if not token or token in seen:
                return
            seen.add(token)
            out.append(token)

        def _looks_symbol(token: str) -> bool:
            t = str(token or "").strip().upper()
            if not t:
                return False
            return bool(re.fullmatch(r"[A-Z0-9][A-Z0-9/._-]{1,23}", t))

        # Keep exact symbol parsing support first (e.g., BTC/USDT, XAUUSD).
        for token in cls._parse_signal_filter_symbols(raw):
            if _looks_symbol(token):
                _add(token)

        # Human-friendly aliases.
        alias_keywords = {
            "XAUUSD": ("gold", "xau", "xauusd", "ทอง"),
            "BTC/USDT": ("btc", "bitcoin", "บิทคอยน์", "บิท"),
            "ETH/USDT": ("eth", "ethereum", "อีเธอเรียม"),
            "SOL/USDT": ("sol", "solana"),
            "XRP/USDT": ("xrp", "ripple"),
            "BNB/USDT": ("bnb",),
            "DOGE/USDT": ("doge", "dogecoin"),
            "ADA/USDT": ("ada", "cardano"),
        }

        def _kw_hit(keyword: str) -> bool:
            k = str(keyword or "").strip().lower()
            if not k:
                return False
            if re.search(r"[a-z0-9/]", k):
                return bool(re.search(rf"(^|[^a-z0-9/]){re.escape(k)}([^a-z0-9/]|$)", q))
            return k in q

        for sym, keys in alias_keywords.items():
            if any(_kw_hit(k) for k in keys):
                _add(sym)
        return out

    @staticmethod
    def _normalize_scalping_symbol(token: str) -> str:
        t = str(token or "").strip().upper().replace(" ", "")
        if not t:
            return ""
        alias = {
            "GOLD": "XAUUSD",
            "XAU": "XAUUSD",
            "XAUUSD": "XAUUSD",
            "ETH": "ETHUSD",
            "ETHUSD": "ETHUSD",
            "ETHUSDT": "ETHUSD",
            "ETH/USDT": "ETHUSD",
            "BTC": "BTCUSD",
            "BTCUSD": "BTCUSD",
            "BTCUSDT": "BTCUSD",
            "BTC/USDT": "BTCUSD",
        }
        return alias.get(t, "")

    @classmethod
    def _parse_scalping_symbols(cls, raw: str) -> list[str]:
        out: list[str] = []
        seen: set[str] = set()
        txt = str(raw or "").strip()
        for part in re.split(r"[\s,;|]+", txt):
            sym = cls._normalize_scalping_symbol(part)
            if not sym or sym in seen:
                continue
            seen.add(sym)
            out.append(sym)

        q = txt.lower()
        keyword_aliases = {
            "XAUUSD": ("gold", "xau", "ทอง"),
            "BTCUSD": ("btc", "bitcoin", "บิทคอยน์", "บิท"),
            "ETHUSD": ("eth", "ethereum", "อีเธอเรียม"),
        }
        for sym, keys in keyword_aliases.items():
            if sym in seen:
                continue
            if any(k in q for k in keys):
                seen.add(sym)
                out.append(sym)
        return out

    @classmethod
    def _infer_scalping_symbol_from_text(cls, raw: str, default: str = "") -> str:
        syms = cls._parse_scalping_symbols(raw)
        if syms:
            return syms[0]
        return cls._normalize_scalping_symbol(default) or ""

    @staticmethod
    def _contains_us_market_token(q: str) -> bool:
        text = str(q or "").lower()
        return bool(
            re.search(r"(^|[^a-z])us([^a-z]|$)", text)
            or any(k in text for k in ("u.s.", "usa", "อเมริกา"))
        )

    @staticmethod
    def _contains_thai_market_token(q: str) -> bool:
        text = str(q or "").lower()
        return bool(
            re.search(r"(^|[^a-z])th([^a-z]|$)", text)
            or any(k in text for k in ("thai", "set50", "หุ้นไทย", "ตลาดไทย"))
        )

    @staticmethod
    def _is_broad_market_scan_phrase(q: str) -> bool:
        text = str(q or "").lower()
        if not any(k in text for k in ("scan", "สแกน")):
            return False
        return any(k in text for k in ("stock", "stocks", "market", "markets", "หุ้น", "ตลาด"))

    @staticmethod
    def _contains_vi_scan_token(q: str) -> bool:
        text = str(q or "").lower()
        compact = text.strip()
        return bool(
            compact == "vi"
            or any(
                k in text
                for k in (
                    " vi",
                    "vi ",
                    "value",
                    "หุ้น vi",
                    "หุ้นคุณค่า",
                    "value+trend",
                    "value trend",
                    "หุ้นแนวโน้ม",
                    "แนวโน้ม",
                )
            )
        )

    @staticmethod
    def _contains_us_vi_token(q: str) -> bool:
        text = str(q or "").lower()
        return bool(
            TelegramAdminBot._contains_us_market_token(text)
            or any(
                k in text
                for k in (
                    "america",
                    "american",
                    "หุ้นอเมริกา",
                    "หุ้นอเมริกัน",
                    "หุ้นเมกา",
                    "ตลาดเมกา",
                    "ตลาดอเมริกา",
                    "เมกา",
                )
            )
        )

    @staticmethod
    def _contains_th_vi_token(q: str) -> bool:
        text = str(q or "").lower()
        return bool(
            TelegramAdminBot._contains_thai_market_token(text)
            or any(k in text for k in ("หุ้นไทย", "ตลาดไทย", "thai stock", "th stock", "set50"))
        )

    @staticmethod
    def _parse_vi_scan_intent(q: str) -> Optional[str]:
        text = str(q or "").lower().strip()
        if not TelegramAdminBot._contains_vi_scan_token(text):
            return None

        is_buffett = any(k in text for k in ("buffett", "บัฟเฟต", "บัพเฟต", "วอร์เรน", "compounder"))
        is_turnaround = any(
            k in text
            for k in (
                "turnaround",
                "re-rating",
                "rerating",
                "กลับตัว",
                "รีเรท",
                "หลายเท่า",
                "multi bagger",
                "multibagger",
            )
        )

        if TelegramAdminBot._contains_th_vi_token(text):
            return "scan_thai_vi"
        if TelegramAdminBot._contains_us_vi_token(text):
            if is_buffett:
                return "scan_vi_buffett"
            if is_turnaround:
                return "scan_vi_turnaround"
            return "scan_vi"

        if is_buffett:
            return "scan_vi_buffett"
        if is_turnaround:
            return "scan_vi_turnaround"
        return "scan_vi"

    @staticmethod
    def _parse_mt5_history_lookback_hours(text: str) -> int:
        q = str(text or "").lower()
        if any(k in q for k in ("last night", "เมื่อคืน", "เมื่อคืนนี้")):
            return 24
        if any(k in q for k in ("today", "วันนี้", "today's", "todays")):
            return 18
        m = re.search(r"\b(\d{1,3})\s*(h|hr|hrs|hour|hours)\b", q)
        if m:
            return max(1, min(24 * 30, int(m.group(1))))
        m = re.search(r"\b(\d{1,3})\s*(d|day|days)\b", q)
        if m:
            return max(1, min(24 * 30, int(m.group(1)) * 24))
        m = re.search(r"(\d{1,2})\s*(ชม|ชั่วโมง)", q)
        if m:
            return max(1, min(24 * 30, int(m.group(1))))
        m = re.search(r"(\d{1,2})\s*วัน", q)
        if m:
            return max(1, min(24 * 30, int(m.group(1)) * 24))
        return 24

    @staticmethod
    def _parse_run_trace_args(text: str) -> dict:
        raw = str(text or "").strip()
        token = raw.split(maxsplit=1)[0] if raw else ""
        token = str(token or "").strip().replace("#", "")
        out = {"valid": False, "raw": token, "run_tag": "", "run_id": "", "run_no": 0}
        if not token:
            return out

        t_up = token.upper()
        m_id = re.search(r"(20\d{12}-\d{1,8})", t_up)
        if m_id:
            out["run_id"] = str(m_id.group(1))
            out["valid"] = True
            m_num = re.search(r"-(\d{1,8})$", out["run_id"])
            if m_num:
                try:
                    out["run_no"] = int(m_num.group(1))
                except Exception:
                    out["run_no"] = 0

        m_tag = re.search(r"\bR(\d{1,8})\b", t_up)
        if m_tag:
            try:
                out["run_no"] = int(m_tag.group(1))
                out["valid"] = True
            except Exception:
                pass

        if (not out["valid"]) and t_up.isdigit():
            try:
                out["run_no"] = int(t_up)
                out["valid"] = True
            except Exception:
                pass

        if int(out.get("run_no", 0) or 0) > 0:
            out["run_tag"] = f"R{int(out['run_no']):06d}"
        return out

    @staticmethod
    def _safe_json_dict(raw: str) -> dict:
        if not raw:
            return {}
        try:
            obj = json.loads(raw)
            return obj if isinstance(obj, dict) else {}
        except Exception:
            return {}

    @staticmethod
    def _extract_trace_meta(extra_json: str, source: str = "") -> dict:
        obj = TelegramAdminBot._safe_json_dict(extra_json)
        raw = dict(obj.get("raw_scores", {}) or {})
        if not isinstance(raw, dict):
            raw = {}
        run_id = str(raw.get("signal_run_id") or obj.get("signal_run_id") or "").strip()
        run_tag = str(raw.get("signal_trace_tag") or obj.get("signal_trace_tag") or "").strip().upper()
        run_no = 0
        for cand in (raw.get("signal_run_no"), obj.get("signal_run_no")):
            try:
                if cand is not None:
                    run_no = int(cand)
                    if run_no > 0:
                        break
            except Exception:
                continue
        if run_no <= 0:
            m = re.search(r"-(\d{1,8})$", run_id)
            if m:
                try:
                    run_no = int(m.group(1))
                except Exception:
                    run_no = 0
        if not run_tag and run_no > 0:
            run_tag = f"R{run_no:06d}"
        if run_no <= 0 and run_tag:
            m = re.search(r"R(\d{1,8})", run_tag)
            if m:
                try:
                    run_no = int(m.group(1))
                except Exception:
                    run_no = 0

        bypass = False
        bypass_raw = raw.get("mt5_bypass_test_enabled", obj.get("mt5_bypass_test_enabled", False))
        if isinstance(bypass_raw, bool):
            bypass = bypass_raw
        else:
            bypass = str(bypass_raw).strip().lower() in {"1", "true", "yes", "on"}
        source_txt = str(source or obj.get("source") or raw.get("mt5_bypass_source") or "").lower()
        if ":bypass" in source_txt or "bypass" == str(source_txt).strip():
            bypass = True
        return {"run_id": run_id, "run_tag": run_tag, "run_no": int(run_no or 0), "bypass": bool(bypass)}

    @staticmethod
    def _trace_match(meta: dict, query: dict) -> bool:
        rid = str(query.get("run_id") or "").strip().upper()
        rtag = str(query.get("run_tag") or "").strip().upper()
        rno = int(query.get("run_no", 0) or 0)
        mid = str(meta.get("run_id") or "").strip().upper()
        mtag = str(meta.get("run_tag") or "").strip().upper()
        mno = int(meta.get("run_no", 0) or 0)
        if rid and mid and (rid == mid):
            return True
        if rtag and mtag and (rtag == mtag):
            return True
        if rno > 0 and mno > 0 and (rno == mno):
            return True
        return False

    @staticmethod
    def _fmt_trace_ts(ts_text: str) -> str:
        s = str(ts_text or "").strip()
        if not s:
            return "-"
        if "T" in s and s.endswith("Z"):
            return s.replace("T", " ").replace("Z", " UTC")
        if "T" in s:
            return s.replace("T", " ")
        return s

    @staticmethod
    def _outcome_label(outcome, pnl) -> str:
        try:
            if outcome is not None:
                ov = int(outcome)
                if ov > 0:
                    return "WIN"
                if ov < 0:
                    return "LOSS"
        except Exception:
            pass
        try:
            pv = float(pnl)
            if pv > 0:
                return "WIN"
            if pv < 0:
                return "LOSS"
        except Exception:
            pass
        return "PENDING"

    def _lookup_run_trace(self, query: dict) -> dict:
        from learning.neural_brain import neural_brain
        from learning.mt5_autopilot_core import mt5_autopilot_core

        report = {
            "ok": True,
            "query": dict(query or {}),
            "signal_rows": [],
            "journal_rows": [],
            "errors": [],
            "limits": {"signal_scan_limit": 2500, "journal_scan_limit": 3500},
            "db": {
                "signal_learning": str(getattr(neural_brain, "db_path", "") or ""),
                "mt5_autopilot": str(getattr(mt5_autopilot_core, "db_path", "") or ""),
            },
        }

        q = dict(query or {})
        if not bool(q.get("valid")):
            report["ok"] = False
            report["errors"].append("invalid_run_query")
            return report

        signal_path = str(report["db"].get("signal_learning") or "").strip()
        if signal_path and os.path.exists(signal_path):
            try:
                with sqlite3.connect(signal_path) as conn:
                    rows = conn.execute(
                        """
                        SELECT id, created_at, source, signal_symbol, broker_symbol, direction, confidence,
                               mt5_status, mt5_message, ticket, position_id, resolved, outcome, pnl, closed_at, extra_json
                        FROM signal_events
                        ORDER BY id DESC
                        LIMIT ?
                        """,
                        (int(report["limits"]["signal_scan_limit"]),),
                    ).fetchall()
                for row in rows:
                    meta = self._extract_trace_meta(str(row[15] or ""), source=str(row[2] or ""))
                    if not self._trace_match(meta, q):
                        continue
                    report["signal_rows"].append(
                        {
                            "id": int(row[0] or 0),
                            "created_at": str(row[1] or ""),
                            "source": str(row[2] or ""),
                            "signal_symbol": str(row[3] or ""),
                            "broker_symbol": str(row[4] or ""),
                            "direction": str(row[5] or ""),
                            "confidence": row[6],
                            "mt5_status": str(row[7] or ""),
                            "mt5_message": str(row[8] or ""),
                            "ticket": row[9],
                            "position_id": row[10],
                            "resolved": int(row[11] or 0),
                            "outcome": row[12],
                            "pnl": row[13],
                            "closed_at": str(row[14] or ""),
                            "bypass": bool(meta.get("bypass", False)),
                            "run_id": str(meta.get("run_id") or ""),
                            "run_tag": str(meta.get("run_tag") or ""),
                            "run_no": int(meta.get("run_no", 0) or 0),
                        }
                    )
            except Exception as e:
                report["errors"].append(f"signal_db_error:{e}")
        else:
            report["errors"].append("signal_db_missing")

        journal_path = str(report["db"].get("mt5_autopilot") or "").strip()
        if journal_path and os.path.exists(journal_path):
            try:
                with sqlite3.connect(journal_path) as conn:
                    rows = conn.execute(
                        """
                        SELECT id, created_at, source, signal_symbol, broker_symbol, direction, confidence,
                               mt5_status, mt5_message, ticket, position_id, resolved, outcome, pnl, close_reason, closed_at, extra_json
                        FROM mt5_execution_journal
                        ORDER BY id DESC
                        LIMIT ?
                        """,
                        (int(report["limits"]["journal_scan_limit"]),),
                    ).fetchall()
                for row in rows:
                    meta = self._extract_trace_meta(str(row[16] or ""), source=str(row[2] or ""))
                    if not self._trace_match(meta, q):
                        continue
                    report["journal_rows"].append(
                        {
                            "id": int(row[0] or 0),
                            "created_at": str(row[1] or ""),
                            "source": str(row[2] or ""),
                            "signal_symbol": str(row[3] or ""),
                            "broker_symbol": str(row[4] or ""),
                            "direction": str(row[5] or ""),
                            "confidence": row[6],
                            "mt5_status": str(row[7] or ""),
                            "mt5_message": str(row[8] or ""),
                            "ticket": row[9],
                            "position_id": row[10],
                            "resolved": int(row[11] or 0),
                            "outcome": row[12],
                            "pnl": row[13],
                            "close_reason": str(row[14] or ""),
                            "closed_at": str(row[15] or ""),
                            "bypass": bool(meta.get("bypass", False)),
                            "run_id": str(meta.get("run_id") or ""),
                            "run_tag": str(meta.get("run_tag") or ""),
                            "run_no": int(meta.get("run_no", 0) or 0),
                        }
                    )
            except Exception as e:
                report["errors"].append(f"journal_db_error:{e}")
        else:
            report["errors"].append("journal_db_missing")

        report["signal_rows"].sort(key=lambda x: (str(x.get("created_at") or ""), int(x.get("id", 0))))
        report["journal_rows"].sort(key=lambda x: (str(x.get("created_at") or ""), int(x.get("id", 0))))
        report["matched"] = int(len(report["signal_rows"]) + len(report["journal_rows"]))
        return report

    def _format_run_trace_report(self, report: dict, lang: str = "en") -> str:
        q = dict((report or {}).get("query") or {})
        run_tag = str(q.get("run_tag") or "").strip()
        run_id = str(q.get("run_id") or "").strip()
        base_key = run_tag or run_id or str(q.get("raw") or "-")
        if not bool((report or {}).get("ok", False)):
            return (
                "Run Trace\n"
                f"query={base_key}\n"
                "status=invalid_query\n"
                "usage=/run R000123  or  /run 20260306010101-000123"
            )

        sig_rows = list((report or {}).get("signal_rows") or [])
        j_rows = list((report or {}).get("journal_rows") or [])
        if not sig_rows and not j_rows:
            return (
                "Run Trace\n"
                f"query={base_key}\n"
                "status=not_found\n"
                "hint=try exact tag like R000123 or full run_id"
            )

        all_rows = sig_rows + j_rows
        bypass = any(bool(r.get("bypass")) or (":bypass" in str(r.get("source", "")).lower()) for r in all_rows)
        first = all_rows[0]
        symbol = str(first.get("signal_symbol") or first.get("broker_symbol") or "-")
        direction = str(first.get("direction") or "-").upper()

        lines = ["Run Trace"]
        lines.append(f"query={base_key}")
        if run_tag:
            lines.append(f"tag={run_tag}")
        if run_id:
            lines.append(f"run_id={run_id}")
        lines.append(f"symbol={symbol} direction={direction}")
        lines.append(f"lane={'BYPASS' if bypass else 'MAIN'}")
        lines.append(f"matches: signal_events={len(sig_rows)} mt5_journal={len(j_rows)}")
        lines.append("")

        lines.append("Signal Path:")
        for r in sig_rows[:8]:
            label = self._outcome_label(r.get("outcome"), r.get("pnl"))
            pnl = r.get("pnl")
            pnl_txt = "-" if pnl is None else f"{float(pnl):.2f}"
            lines.append(
                f"- [{self._fmt_trace_ts(r.get('created_at'))}] {r.get('mt5_status') or '-'} "
                f"src={r.get('source') or '-'} ticket={r.get('ticket') or '-'} pos={r.get('position_id') or '-'} "
                f"outcome={label} pnl={pnl_txt}"
                + (" [BYPASS]" if bool(r.get("bypass")) else "")
            )

        lines.append("")
        lines.append("MT5 Journal:")
        for r in j_rows[:8]:
            label = self._outcome_label(r.get("outcome"), r.get("pnl"))
            pnl = r.get("pnl")
            pnl_txt = "-" if pnl is None else f"{float(pnl):.2f}"
            lines.append(
                f"- [{self._fmt_trace_ts(r.get('created_at'))}] {r.get('mt5_status') or '-'} "
                f"src={r.get('source') or '-'} ticket={r.get('ticket') or '-'} pos={r.get('position_id') or '-'} "
                f"outcome={label} pnl={pnl_txt}"
                + (" [BYPASS]" if bool(r.get("bypass")) or (":bypass" in str(r.get("source", "")).lower()) else "")
            )

        model_resolved = [r for r in sig_rows if int(r.get("resolved", 0) or 0) == 1]
        mt5_resolved = [r for r in j_rows if int(r.get("resolved", 0) or 0) == 1]
        lines.append("")
        lines.append("Final Outcome:")
        if model_resolved:
            mr = model_resolved[-1]
            mr_pnl = mr.get("pnl")
            mr_pnl_txt = "-" if mr_pnl is None else f"{float(mr_pnl):.2f}"
            lines.append(
                f"- model: {self._outcome_label(mr.get('outcome'), mr.get('pnl'))} "
                f"pnl={mr_pnl_txt} closed={self._fmt_trace_ts(mr.get('closed_at'))}"
            )
        else:
            lines.append("- model: PENDING")
        if mt5_resolved:
            jr = mt5_resolved[-1]
            jr_pnl = jr.get("pnl")
            jr_pnl_txt = "-" if jr_pnl is None else f"{float(jr_pnl):.2f}"
            lines.append(
                f"- mt5: {self._outcome_label(jr.get('outcome'), jr.get('pnl'))} "
                f"pnl={jr_pnl_txt} closed={self._fmt_trace_ts(jr.get('closed_at'))}"
            )
        else:
            lines.append("- mt5: PENDING")

        errs = list((report or {}).get("errors") or [])
        if errs:
            lines.append("")
            lines.append("diag=" + " | ".join(str(x) for x in errs[:3]))
        return "\n".join(lines)[:3900]

    @staticmethod
    def _parse_mt5_pm_learning_args(text: str) -> dict:
        from learning.mt5_position_manager import mt5_position_manager

        out = {"days": 30, "top": 8, "sync": True, "symbol": "", "action": "", "save_draft": False}
        raw = str(text or "").strip()
        if not raw:
            return out

        toks = [t for t in raw.split() if t]
        i = 0
        while i < len(toks):
            tk = str(toks[i]).strip()
            tl = tk.lower()
            if tl in {"sync", "refresh"}:
                out["sync"] = True
            elif tl in {"nosync", "no-sync"}:
                out["sync"] = False
            elif tl in {"draft", "drafts"}:
                out["save_draft"] = True
            elif tl == "recommend" and (i + 1) < len(toks):
                nxt = str(toks[i + 1] or "").strip().lower()
                if nxt in {"draft", "drafts"}:
                    out["save_draft"] = True
                    i += 1
            elif tl.startswith("top") and len(tl) > 3:
                try:
                    out["top"] = max(1, min(20, int(tl[3:])))
                except Exception:
                    pass
            elif tl == "top" and (i + 1) < len(toks):
                try:
                    out["top"] = max(1, min(20, int(toks[i + 1])))
                    i += 1
                except Exception:
                    pass
            elif re.fullmatch(r"\d{1,3}d", tl):
                try:
                    out["days"] = max(1, min(365, int(tl[:-1])))
                except Exception:
                    pass
            elif tl in {"action", "act"} and (i + 1) < len(toks):
                norm = mt5_position_manager.normalize_learning_action_filter(toks[i + 1])
                if norm:
                    out["action"] = norm
                    i += 1
            elif tl.startswith("action=") or tl.startswith("act="):
                _, _, v = tk.partition("=")
                norm = mt5_position_manager.normalize_learning_action_filter(v)
                if norm:
                    out["action"] = norm
            elif tl in {"symbol", "sym"} and (i + 1) < len(toks):
                norm = mt5_position_manager.normalize_learning_symbol_filter(toks[i + 1])
                if norm:
                    out["symbol"] = norm
                    i += 1
            elif tl.startswith("symbol=") or tl.startswith("sym="):
                _, _, v = tk.partition("=")
                norm = mt5_position_manager.normalize_learning_symbol_filter(v)
                if norm:
                    out["symbol"] = norm
            else:
                parsed_num = False
                try:
                    v = int(tl)
                    if v <= 20:
                        out["top"] = max(1, min(20, v))
                    else:
                        out["days"] = max(1, min(365, v))
                    parsed_num = True
                except Exception:
                    parsed_num = False
                if not parsed_num and not out["symbol"]:
                    sym = mt5_position_manager.normalize_learning_symbol_filter(tk)
                    if sym and sym not in {"TOP", "SYNC", "REFRESH", "NOSYNC", "NO-SYNC", "ACTION"}:
                        out["symbol"] = sym
            i += 1
        return out

    @staticmethod
    def _parse_mt5_affordable_args(text: str) -> dict:
        out = {"category": "all", "top": 12, "only_ok": False}
        raw = str(text or "").strip()
        if not raw:
            return out
        toks = [t for t in raw.split() if t]
        for i, tk in enumerate(toks):
            tl = str(tk).strip().lower()
            if tl in {"ok", "pass", "eligible"}:
                out["only_ok"] = True
                continue
            if tl in {"all", "crypto", "fx", "forex", "metal", "metals", "index", "indices"}:
                cat = "fx" if tl == "forex" else ("metal" if tl == "metals" else ("index" if tl == "indices" else tl))
                out["category"] = cat
                continue
            if tl in {"top", "limit"} and i + 1 < len(toks):
                try:
                    out["top"] = max(3, min(30, int(toks[i + 1])))
                except Exception:
                    pass
                continue
            if tl.startswith("top") and len(tl) > 3:
                try:
                    out["top"] = max(3, min(30, int(tl[3:])))
                except Exception:
                    pass
                continue
            try:
                n = int(tl)
                out["top"] = max(3, min(30, n))
            except Exception:
                pass
        return out

    @staticmethod
    def _normalize_intent_text(text: str) -> str:
        raw = str(text or "")
        q = f" {raw.lower()} "
        replacements = {
            " calndar ": " calendar ",
            " calender ": " calendar ",
            " econimic ": " economic ",
            " histroy ": " history ",
            " hisotry ": " history ",
            " ststus ": " status ",
            " statua ": " status ",
            " chek ": " check ",
            " checl ": " check ",
            " chcek ": " check ",
            " chekck ": " check ",
            " reserch ": " research ",
            " trumph ": " trump ",
            " geopolotic ": " geopolitical ",
            " geopolitic ": " geopolitical ",
            " weigths ": " weights ",
            " wights ": " weights ",
            " เชค ": " เช็ค ",
            " เช็คค ": " เช็ค ",
            " ฮิสทอรี่ ": " history ",
        }
        for a, b in replacements.items():
            q = q.replace(a, b)
        return q.strip()

    def _set_pending_slot(self, chat_id: int, kind: str, payload: Optional[dict] = None) -> None:
        try:
            cid = int(chat_id)
        except Exception:
            return
        self._chat_pending_slots[cid] = {
            "kind": str(kind or ""),
            "payload": dict(payload or {}),
            "ts": time.time(),
        }

    def _clear_pending_slot(self, chat_id: int) -> None:
        try:
            self._chat_pending_slots.pop(int(chat_id), None)
        except Exception:
            return

    def _pending_slot(self, chat_id: int, max_age_sec: int = 900) -> Optional[dict]:
        try:
            rec = self._chat_pending_slots.get(int(chat_id))
        except Exception:
            return None
        if not rec:
            return None
        ts = float(rec.get("ts", 0.0) or 0.0)
        if ts <= 0 or (time.time() - ts) > max(60, int(max_age_sec)):
            self._clear_pending_slot(chat_id)
            return None
        return rec

    def _set_pending_intent_confirm(self, chat_id: int, command: str, args: str = "", source_text: str = "") -> None:
        try:
            cid = int(chat_id)
        except Exception:
            return
        self._chat_pending_intent_confirm[cid] = {
            "command": str(command or "").strip().lower(),
            "args": str(args or "").strip(),
            "source_text": str(source_text or "").strip()[:400],
            "ts": time.time(),
        }

    def _clear_pending_intent_confirm(self, chat_id: int) -> None:
        try:
            self._chat_pending_intent_confirm.pop(int(chat_id), None)
        except Exception:
            return

    def _pending_intent_confirm(self, chat_id: int, max_age_sec: int = 300) -> Optional[dict]:
        try:
            rec = self._chat_pending_intent_confirm.get(int(chat_id))
        except Exception:
            return None
        if not rec:
            return None
        ts = float(rec.get("ts", 0.0) or 0.0)
        if ts <= 0 or (time.time() - ts) > max(60, int(max_age_sec)):
            self._clear_pending_intent_confirm(chat_id)
            return None
        return rec

    @staticmethod
    def _parse_confirmation_answer(text: str) -> Optional[bool]:
        q = str(text or "").strip().lower()
        if not q:
            return None
        yes_tokens = {
            "y", "yes", "yeah", "yep", "ok", "okay", "sure", "do it", "confirm", "correct",
            "ใช่", "ใช่ครับ", "ใช่ค่ะ", "ถูกต้อง", "ตกลง", "โอเค", "เอาเลย", "ทำเลย",
            "ja", "jawohl", "genau", "richtig", "bestätigen",
        }
        no_tokens = {
            "n", "no", "nope", "not", "wrong", "cancel", "stop",
            "ไม่", "ไม่ใช่", "ไม่เอา", "ยกเลิก", "หยุด", "ไม่ถูก",
            "nein", "falsch", "abbrechen", "stopp",
        }
        compact = " ".join(q.split())
        if compact in yes_tokens:
            return True
        if compact in no_tokens:
            return False
        # Accept short Thai confirmations embedded in sentence.
        if any(x in compact for x in ("ใช่", "ถูกต้อง", "เอาเลย")):
            return True
        if any(x in compact for x in ("ไม่ใช่", "ไม่เอา", "ยกเลิก")):
            return False
        return None

    def _human_intent_label(self, command: str, args: str, lang: str = "en") -> str:
        cmd = str(command or "").strip().lower()
        arg = str(args or "").strip()
        if (lang or "en").lower() == "th":
            mapping = {
                "scan_gold": "สแกนทองคำ",
                "scan_crypto": "สแกนคริปโต",
                "scan_fx": "สแกนฟอเร็กซ์",
                "scan_stocks": "สแกนหุ้น",
                "scan_thai": "สแกนหุ้นไทย",
                "scan_us_open": "สแกนหุ้นสหรัฐช่วงเปิดตลาด",
                "scan_vi": "สแกนหุ้นแนว VI",
                "scan_all": "สแกนทุกตลาด",
                "signal_monitor": "ติดตามสถานะสัญญาณ",
                "signal_filter": "ดูสถานะตัวกรองสัญญาณ",
                "show_clear": "ล้างตัวกรองและแสดงทุกสัญญาณ",
            }
            if cmd == "show_only":
                return f"แสดงสัญญาณเฉพาะ {arg}" if arg else "ตั้งตัวกรองสัญญาณแบบเฉพาะ"
            if cmd == "show_add":
                return f"เพิ่มตัวกรองสัญญาณ: {arg}" if arg else "เพิ่มตัวกรองสัญญาณ"
            return mapping.get(cmd, f"ทำงานคำสั่ง {cmd}")
        if (lang or "en").lower() == "de":
            mapping = {
                "scan_gold": "Gold scannen",
                "scan_crypto": "Krypto scannen",
                "scan_fx": "FX scannen",
                "scan_stocks": "Aktien scannen",
                "scan_thai": "Thai-Aktien scannen",
                "scan_us_open": "US-Open-Aktien scannen",
                "scan_vi": "VI-Aktien scannen",
                "scan_all": "alle Märkte scannen",
                "signal_monitor": "Signal-Monitor anzeigen",
                "signal_filter": "Signalfilter-Status anzeigen",
                "show_clear": "Filter löschen und alle Signale anzeigen",
            }
            if cmd == "show_only":
                return f"nur diese Signale anzeigen: {arg}" if arg else "Signalfilter festlegen"
            if cmd == "show_add":
                return f"Signalfilter erweitern: {arg}" if arg else "Signalfilter erweitern"
            return mapping.get(cmd, f"Befehl ausführen: {cmd}")
        mapping = {
            "scan_gold": "scan gold",
            "scan_crypto": "scan crypto",
            "scan_fx": "scan FX",
            "scan_stocks": "scan stocks",
            "scan_thai": "scan Thai stocks",
            "scan_us_open": "scan US-open stocks",
            "scan_vi": "run VI stock scan",
            "scan_all": "scan all markets",
            "signal_monitor": "show signal monitor snapshot",
            "signal_filter": "show your signal filter status",
            "show_clear": "clear signal filter and show all signals",
        }
        if cmd == "show_only":
            return f"show only these signals: {arg}" if arg else "set a signal filter"
        if cmd == "show_add":
            return f"add these symbols to your filter: {arg}" if arg else "add symbols to signal filter"
        return mapping.get(cmd, f"run command {cmd}")

    def _intent_confirm_prompt(self, command: str, args: str, lang: str = "en") -> str:
        label = self._human_intent_label(command, args, lang=lang)
        if (lang or "en").lower() == "th":
            return (
                f"ขอยืนยันก่อนครับ: คุณต้องการให้ผม{label} ใช่ไหม?\n"
                "ตอบ: ใช่ / ไม่ใช่"
            )
        if (lang or "en").lower() == "de":
            return (
                f"Zur Bestätigung: Soll ich {label}?\n"
                "Antwort: ja / nein"
            )
        return (
            f"Quick confirmation: should I {label}?\n"
            "Reply: yes / no"
        )

    @staticmethod
    def _intent_rephrase_prompt(lang: str = "en") -> str:
        if (lang or "en").lower() == "th":
            return (
                "ได้เลยครับ บอกใหม่อีกครั้งแบบสั้นๆ ได้เลย เช่น\n"
                "• หาหุ้นไทย\n"
                "• แสดงแค่ทองคำ\n"
                "• แสดงเฉพาะ BTC ETH"
            )
        if (lang or "en").lower() == "de":
            return (
                "Okay. Formuliere es bitte kurz neu, zum Beispiel:\n"
                "• Thai-Aktien suchen\n"
                "• nur Gold anzeigen\n"
                "• nur BTC ETH anzeigen"
            )
        return (
            "Okay. Please rephrase briefly, for example:\n"
            "• find Thai stocks\n"
            "• show only gold\n"
            "• show only BTC ETH"
        )

    @staticmethod
    def _intent_missing_filter_symbols_prompt(lang: str = "en") -> str:
        if (lang or "en").lower() == "th":
            return "ต้องการให้แสดงเฉพาะอะไรครับ? เช่น ทองคำ, BTC ETH หรือพิมพ์ว่า แสดงทุกสัญญาณ"
        if (lang or "en").lower() == "de":
            return "Welche Signale soll ich zeigen? Z.B. Gold, BTC ETH oder 'alle Signale'."
        return "Which signals should I show? Example: gold, BTC ETH, or say 'show all signals'."

    def _try_handle_pending_slot(self, chat_id: int, user_id: int, text: str, is_admin: bool, lang: str) -> bool:
        rec = self._pending_slot(chat_id)
        if not rec:
            return False
        kind = str(rec.get("kind") or "")
        payload = dict(rec.get("payload") or {})
        if kind != "mt5_history_symbol":
            return False

        raw = str(text or "").strip()
        q = self._normalize_intent_text(raw)
        hours = int(payload.get("hours", 24) or 24)
        hours = self._parse_mt5_history_lookback_hours(q or raw) or hours
        if q in {"all", "ทั้งหมด", "ทุกตัว", "ทุกสัญลักษณ์"}:
            self._clear_pending_slot(chat_id)
            self._handle_admin_command(chat_id, user_id, "mt5_history", f"{hours}h", is_admin, lang=lang)
            return True

        symbol, _ = self._extract_symbol_and_side_hint(raw, mt5_hint=True)
        if not symbol:
            if lang == "th":
                self._send_text(chat_id, "ขอสัญลักษณ์ที่ต้องการตรวจ เช่น ETHUSD, XAUUSD หรือพิมพ์ all เพื่อดูทุกตัว")
            elif lang == "de":
                self._send_text(chat_id, "Bitte Symbol senden (z.B. ETHUSD, XAUUSD) oder 'all' für alle.")
            else:
                self._send_text(chat_id, "Please send a symbol (e.g. ETHUSD, XAUUSD) or 'all' for all symbols.")
            return True

        self._clear_pending_slot(chat_id)
        self._handle_admin_command(chat_id, user_id, "mt5_history", f"{symbol} {hours}h", is_admin, lang=lang)
        return True

    def _is_admin(self, msg: dict) -> bool:
        from_id = msg.get("from", {}).get("id")
        if from_id is None:
            return False
        try:
            uid = int(from_id)
        except Exception:
            return False
        if access_manager.is_admin_user(uid):
            return True
        # Owner fallback: explicit TELEGRAM_CHAT_ID match only.
        if config.TELEGRAM_CHAT_ID and str(config.TELEGRAM_CHAT_ID).lstrip("-").isdigit():
            return uid == int(config.TELEGRAM_CHAT_ID)
        return False

    def _help_text(self, lang: str = "en") -> str:
        if (lang or "en").lower() == "th":
            return (
                "คำสั่ง Dexter Admin\n"
                "/status\n"
                "/scan_gold\n"
                "/scan_crypto\n"
                "/scan_fx\n"
                "/scan_stocks\n"
                "/scan_thai\n"
                "/scan_thai_vi\n"
                "/scan_us\n"
                "/scan_us_open\n"
                "/scan_vi\n"
                "/scan_vi_buffett\n"
                "/scan_vi_turnaround\n"
                "/scalping_status\n"
                "/scalping_on [xauusd ethusd btcusd]\n"
                "/scalping_off\n"
                "/scalping_scan [xauusd|ethusd|btcusd]\n"
                "/scalping_logic [xauusd|ethusd|btcusd]\n"
                "/monitor_us\n"
                "/us_open_report\n"
                "/us_open_dashboard\n"
                "/us_open_guard_status\n"
                "/signal_dashboard [gold|XAUUSD|ETHUSD] [today|yesterday|this week|this month] [top5]\n"
                "/signal_monitor [gold|XAUUSD|ETHUSD|BTCUSD] [today|yesterday|this week|this month]\n"
                "/run <R000123|run_id>\n"
                "/signal_filter [status|only|add|clear]\n"
                "/show_only <gold|xauusd|btc eth ...>\n"
                "/show_add <symbol ...>\n"
                "/show_clear\n"
                "/calendar\n"
                "/macro [*|**|***]\n"
                "/macro_report [*|**|***] [24h]\n"
                "/macro_weights [refresh] [top10]\n"
                "/tz [UTC+7|+07:00|bangkok]\n"
                "/mt5_status [symbol]\n"
                "/mt5_affordable [ok] [crypto|fx|metal|index]\n/mt5_exec_reasons [symbol]\n"
                "/stock_mt5_filter [on|off|status]\n"
                "/mt5_history [symbol] [24h]\n"
                "/mt5_backtest\n"
                "/mt5_train\n"
                "/mt5_autopilot\n"
                "/mt5_walkforward\n"
                "/mt5_manage [watch]\n"
                "/mt5_pm_learning [30d] [top8] [action trail_sl] [symbol ETHUSD] [recommend draft]\n"
                "/mt5_plan <symbol>\n"
                "/mt5_policy [show|keys|preset|set|reset]\n"
                "/scan_all\n"
                "/markets\n"
                "/gold_overview\n"
                "/plan\n"
                "/upgrade [a|b|c]\n"
                "/research <คำถาม>\n\n"
                "คำสั่ง billing สำหรับแอดมิน:\n"
                "/grant <user_id> <trial|a|b|c> <days>\n"
                "/revoke <user_id>\n"
                "/user_list [keyword] [top20]\n"
                "/admin_add <user_id|@username>\n"
                "/admin_del <user_id|@username>\n"
                "/admin_list\n\n"
                "พิมพ์ภาษาธรรมชาติได้ เช่น\n"
                "- scan gold now\n"
                "- เช็คตลาดไหนเปิดอยู่\n"
                "- gold overview\n"
                "- ตอนนี้ทองราคาเท่าไร\n"
                "- เช็ค mt5 ETHUSD"
            )
        if (lang or "en").lower() == "de":
            return (
                "Dexter Admin-Befehle\n"
                "/status\n"
                "/scan_gold\n"
                "/scan_crypto\n"
                "/scan_fx\n"
                "/scan_stocks\n"
                "/scan_thai\n"
                "/scan_thai_vi\n"
                "/scan_us\n"
                "/scan_us_open\n"
                "/scan_vi\n"
                "/scan_vi_buffett\n"
                "/scan_vi_turnaround\n"
                "/scalping_status\n"
                "/scalping_on [xauusd ethusd btcusd]\n"
                "/scalping_off\n"
                "/scalping_scan [xauusd|ethusd|btcusd]\n"
                "/scalping_logic [xauusd|ethusd|btcusd]\n"
                "/monitor_us\n"
                "/us_open_report\n"
                "/us_open_dashboard\n"
                "/us_open_guard_status\n"
                "/signal_dashboard [gold|XAUUSD|ETHUSD] [today|yesterday|this week|this month] [top5]\n"
                "/signal_monitor [gold|XAUUSD|ETHUSD|BTCUSD] [today|yesterday|this week|this month]\n"
                "/run <R000123|run_id>\n"
                "/signal_filter [status|only|add|clear]\n"
                "/show_only <gold|xauusd|btc eth ...>\n"
                "/show_add <symbol ...>\n"
                "/show_clear\n"
                "/calendar\n"
                "/macro [*|**|***]\n"
                "/macro_report [*|**|***] [24h]\n"
                "/macro_weights [refresh] [top10]\n"
                "/tz [UTC+7|+07:00|bangkok]\n"
                "/mt5_status [symbol]\n"
                "/mt5_affordable [ok] [crypto|fx|metal|index]\n/mt5_exec_reasons [symbol]\n"
                "/stock_mt5_filter [on|off|status]\n"
                "/mt5_history [symbol] [24h]\n"
                "/mt5_backtest\n"
                "/mt5_train\n"
                "/mt5_autopilot\n"
                "/mt5_walkforward\n"
                "/mt5_manage [watch]\n"
                "/mt5_pm_learning [30d] [top8] [action trail_sl] [symbol ETHUSD] [recommend draft]\n"
                "/mt5_plan <symbol>\n"
                "/mt5_policy [show|keys|preset|set|reset]\n"
                "/scan_all\n"
                "/markets\n"
                "/gold_overview\n"
                "/plan\n"
                "/upgrade [a|b|c]\n"
                "/research <frage>\n\n"
                "Admin-Billingsteuerung:\n"
                "/grant <user_id> <trial|a|b|c> <days>\n"
                "/revoke <user_id>\n"
                "/user_list [keyword] [top20]\n"
                "/admin_add <user_id|@username>\n"
                "/admin_del <user_id|@username>\n"
                "/admin_list\n\n"
                "Natürliche Sprache funktioniert auch, z.B.\n"
                "- scan gold now\n"
                "- welche Märkte sind offen?\n"
                "- gold overview\n"
                "- prüfe mt5 ETHUSD"
            )
        return (
            "Dexter Admin Commands\n"
            "/status\n"
            "/scan_gold\n"
            "/scan_crypto\n"
            "/scan_fx\n"
            "/scan_stocks\n"
            "/scan_thai\n"
            "/scan_thai_vi\n"
            "/scan_us\n"
            "/scan_us_open\n"
            "/scan_vi\n"
            "/scalping_status\n"
            "/scalping_on [xauusd ethusd btcusd]\n"
            "/scalping_off\n"
            "/scalping_scan [xauusd|ethusd|btcusd]\n"
            "/scalping_logic [xauusd|ethusd|btcusd]\n"
            "/monitor_us\n"
            "/us_open_report\n"
            "/us_open_dashboard\n"
            "/us_open_guard_status\n"
            "/signal_dashboard [gold|XAUUSD|ETHUSD] [today|yesterday|this week|this month] [top5]\n"
            "/signal_monitor [gold|XAUUSD|ETHUSD|BTCUSD] [today|yesterday|this week|this month]\n"
            "/run <R000123|run_id>\n"
            "/signal_filter [status|only|add|clear]\n"
            "/show_only <gold|xauusd|btc eth ...>\n"
            "/show_add <symbol ...>\n"
            "/show_clear\n"
            "/calendar\n"
            "/macro [*|**|***]\n"
            "/macro_report [*|**|***] [24h]\n"
            "/macro_weights [refresh] [top10]\n"
            "/tz [UTC+7|+07:00|bangkok]\n"
            "/mt5_status [symbol]\n"
            "/mt5_affordable [ok] [crypto|fx|metal|index]\n/mt5_exec_reasons [symbol]\n"
            "/stock_mt5_filter [on|off|status]\n"
            "/mt5_history [symbol] [24h]\n"
                "/mt5_backtest\n"
                "/mt5_train\n"
            "/mt5_autopilot\n"
            "/mt5_walkforward\n"
            "/mt5_manage [watch]\n"
            "/mt5_pm_learning [30d] [top8] [action trail_sl] [symbol ETHUSD] [recommend draft]\n"
            "/mt5_plan <symbol>\n"
            "/mt5_policy [show|keys|preset|set|reset]\n"
                "/scan_all\n"
                "/markets\n"
                "/gold_overview\n"
            "/plan\n"
            "/upgrade [a|b|c]\n"
            "/research <question>\n\n"
            "Admin billing controls:\n"
            "/grant <user_id> <trial|a|b|c> <days>\n"
            "/revoke <user_id>\n"
                "/user_list [keyword] [top20]\n"
                "/admin_add <user_id|@username>\n"
                "/admin_del <user_id|@username>\n"
                "/admin_list\n\n"
            "You can also type normal language, e.g.\n"
            "- scan gold now\n"
            "- what markets are open?\n"
            "- gold overview\n"
            "- what's gold price now?\n"
            "- check mt5 ETHUSD"
        )

    def _format_xauusd_scan_status(self, result: dict) -> str:
        status = str((result or {}).get("status", "unknown"))
        session_info = (result or {}).get("session_info", {}) or {}
        sessions = session_info.get("active_sessions", []) or ["unknown"]
        session_text = ", ".join(sessions)
        weekend = bool((result or {}).get("weekend", False))
        threshold = float((result or {}).get("confidence_threshold", 0))
        signal = (result or {}).get("signal", {}) or {}
        cooldown = (result or {}).get("cooldown", {}) or {}

        lines = ["XAUUSD scan status"]
        lines.append(f"Session: {session_text}" + (" | Weekend/low-liquidity context" if weekend else ""))

        if status in ("sent", "sent_manual_bypass_cooldown"):
            lines.append("Result: SIGNAL SENT ✅")
            if signal:
                lines.append(
                    f"Signal: {str(signal.get('direction', '')).upper()} | "
                    f"Conf: {float(signal.get('confidence', 0)):.1f}% | "
                    f"Entry: {float(signal.get('entry', 0)):.2f}"
                )
            if status == "sent_manual_bypass_cooldown" and cooldown.get("reason") == "cooldown_active":
                lines.append(
                    "Cooldown: active but bypassed by manual command "
                    f"(remaining ~{int(float(cooldown.get('remaining_sec', 0)))}s)."
                )
            return "\n".join(lines)

        if status == "cooldown_suppressed":
            lines.append("Result: NO ALERT (cooldown anti-spam) ⏳")
            lines.append(
                f"Why: duplicate setup too close to last alert; remaining "
                f"~{int(float(cooldown.get('remaining_sec', 0)))}s."
            )
            if "price_delta" in cooldown and "move_threshold" in cooldown:
                lines.append(
                    f"Delta: {float(cooldown.get('price_delta', 0)):.2f} "
                    f"(needs >= {float(cooldown.get('move_threshold', 0)):.2f})"
                )
            lines.append("Not caused by economic-calendar/news filter (not enabled).")
            return "\n".join(lines)

        if status == "below_confidence":
            conf = float(signal.get("confidence", 0)) if signal else 0.0
            lines.append("Result: NO ALERT (below threshold)")
            lines.append(f"Confidence: {conf:.1f}% < required {threshold:.1f}%")
            lines.append("Not caused by cooldown, market-closed, or news filter.")
            return "\n".join(lines)

        if status == "no_signal":
            lines.append("Result: NO QUALIFYING SIGNAL right now.")
            lines.append(
                "Reason: current structure/momentum rules did not produce a valid setup."
            )
            lines.append("Not caused by cooldown or news filter.")
            return "\n".join(lines)

        if status == "error":
            lines.append(f"Result: ERROR ❌ {str((result or {}).get('error', 'unknown'))[:240]}")
            return "\n".join(lines)

        lines.append("Result: scan completed.")
        return "\n".join(lines)

    def _ai_intent_endpoint(self) -> tuple[Optional[str], Optional[str], Optional[str], Optional[str]]:
        provider = config.resolve_ai_provider()
        if provider == "groq" and config.GROQ_API_KEY:
            return (
                "groq_openai",
                "https://api.groq.com/openai/v1/chat/completions",
                config.GROQ_API_KEY,
                config.model_for_provider("groq"),
            )
        if provider == "gemini" and config.has_gemini_key():
            model = config.model_for_provider("gemini")
            if config.gemini_mode() == "vertex":
                return (
                    "gemini_native",
                    f"https://aiplatform.googleapis.com/v1/publishers/google/models/{model}:generateContent",
                    config.GEMINI_VERTEX_AI_API_KEY,
                    model,
                )
            return (
                "gemini_native",
                f"https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent",
                config.GEMINI_API_KEY,
                model,
            )
        return None, None, None, None

    def _infer_command_ai(self, text: str) -> Optional[tuple[str, str]]:
        """
        Use AI to classify multilingual user text into admin command intent.
        Returns (command, args) or None.
        """
        if not config.ADMIN_AI_INTENT_ENABLED:
            return None

        intent_mode, endpoint, api_key, model = self._ai_intent_endpoint()
        if not intent_mode or not endpoint or not api_key or not model:
            return None

        system = (
            "You classify a user message into ONE bot command.\n"
            "Output STRICT JSON only: {\"command\":\"...\",\"args\":\"...\"}\n"
            "Allowed command values: help,status,scan_gold,scan_crypto,scan_fx,scan_stocks,"
            "scan_thai,scan_thai_vi,scan_us_open,scan_vi,scan_vi_buffett,scan_vi_turnaround,monitor_us,us_open_guard_status,signal_dashboard,signal_monitor,signal_filter,show_only,show_add,show_clear,scalping_status,scalping_on,scalping_off,scalping_scan,scalping_logic,calendar,macro,macro_report,macro_weights,tz,mt5_status,mt5_affordable,mt5_exec_reasons,stock_mt5_filter,mt5_history,mt5_backtest,mt5_train,mt5_autopilot,mt5_walkforward,mt5_manage,mt5_pm_learning,mt5_plan,mt5_policy,run,scan_all,markets,gold_overview,plan,upgrade,research,none.\n"
            "If unclear, choose research.\n"
            "If command is research, put original user question in args."
        )
        try:
            if intent_mode == "groq_openai":
                payload = {
                    "model": model,
                    "messages": [
                        {"role": "system", "content": system},
                        {"role": "user", "content": text[:800]},
                    ],
                    "temperature": 0,
                    "max_tokens": 140,
                }
                headers = {
                    "Authorization": f"Bearer {api_key}",
                    "Content-Type": "application/json",
                }
                resp = requests.post(endpoint, headers=headers, json=payload, timeout=20)
                if resp.status_code >= 400:
                    return None
                data = resp.json()
                content = (data.get("choices", [{}])[0].get("message", {}).get("content") or "").strip()
            else:
                payload = {
                    "systemInstruction": {"parts": [{"text": system}]},
                    "contents": [
                        {
                            "role": "user",
                            "parts": [{"text": text[:800]}],
                        }
                    ],
                    "generationConfig": {
                        "temperature": 0,
                        "maxOutputTokens": 160,
                    },
                }
                resp = requests.post(
                    endpoint,
                    params={"key": api_key},
                    headers={"Content-Type": "application/json"},
                    json=payload,
                    timeout=20,
                )
                if resp.status_code >= 400:
                    return None
                data = resp.json()
                parts = ((data.get("candidates") or [{}])[0].get("content", {}) or {}).get("parts", []) or []
                texts: list[str] = []
                for part in parts:
                    if isinstance(part, dict):
                        t = str(part.get("text") or "").strip()
                        if t:
                            texts.append(t)
                content = "\n".join(texts).strip()

            if resp.status_code >= 400:
                return None
            if not content:
                return None
            start = content.find("{")
            end = content.rfind("}")
            if start == -1 or end == -1 or end <= start:
                return None
            obj = json.loads(content[start:end + 1])
            cmd = str(obj.get("command", "")).strip().lower()
            args = str(obj.get("args", "")).strip()
            allowed = {
                "help", "status", "scan_gold", "scan_crypto", "scan_fx", "scan_stocks", "scan_thai",
                "scan_us_open", "scan_thai_vi", "scan_vi", "scan_vi_buffett", "scan_vi_turnaround", "monitor_us", "us_open_guard_status",
                "signal_dashboard", "signal_monitor", "signal_filter", "show_only", "show_add", "show_clear",
                "scalping_status", "scalping_on", "scalping_off", "scalping_scan", "scalping_logic",
                "calendar", "macro", "macro_report", "macro_weights", "tz", "mt5_status", "mt5_affordable", "mt5_exec_reasons", "stock_mt5_filter", "mt5_history", "mt5_backtest", "mt5_train", "mt5_autopilot", "mt5_walkforward", "mt5_manage", "mt5_pm_learning", "mt5_plan", "mt5_policy", "scan_all", "markets",
                "gold_overview", "plan", "upgrade", "research", "none",
            }
            if cmd not in allowed or cmd == "none":
                return None
            if cmd == "research" and not args:
                args = text.strip()
            return cmd, args
        except Exception:
            return None

    def _extract_symbol_and_side_hint(self, text: str, mt5_hint: bool = False) -> tuple[Optional[str], Optional[str]]:
        raw = (text or "").strip()
        q = raw.lower()
        side = None
        if any(k in q for k in ("short", "sell", "bearish", "ชอร์ต", "ขาย")):
            side = "short"
        elif any(k in q for k in ("long", "buy", "bullish", "ลอง", "ซื้อ")):
            side = "long"

        if any(k in q for k in ("xau", "gold", "ทอง")):
            return "XAUUSD", side

        # Crypto-style symbol like BTC/USDT
        m = re.search(r"\b([A-Za-z0-9]{2,12}/USDT)\b", raw, flags=re.IGNORECASE)
        if m:
            return m.group(1).upper(), side

        # MT5-style symbols without slash (e.g., ETHUSD, EURUSD, US500, USTEC).
        if mt5_hint:
            m = re.search(
                r"\b("
                r"[A-Za-z]{3,6}(?:USD|USDT|EUR|JPY|GBP|CHF|AUD|NZD|CAD)"
                r"|US(?:30|100|500|2000)"
                r"|USTEC|UK100|JP225|DE40|GER40|SPX500|NAS100"
                r")\b",
                raw,
                flags=re.IGNORECASE,
            )
            if m:
                return m.group(1).upper(), side

        # Match known stock symbols from configured universe.
        try:
            from market.stock_universe import get_all_stocks
            known = {s.upper() for s in get_all_stocks()}
            tokens = re.findall(r"\b[A-Za-z][A-Za-z0-9\.\-]{1,10}\b", raw)
            for tok in tokens:
                up = tok.upper()
                if up in known:
                    return up, side
        except Exception:
            pass

        return None, side

    def _run_signal_explain_reply(self, chat_id: int, text: str, lang: str = "en") -> None:
        """Explain latest signal rationale for a specific symbol."""
        try:
            symbol, side_hint = self._extract_symbol_and_side_hint(text, mt5_hint=True)
            if not symbol:
                self._send_text_localized(chat_id, "specify_symbol", lang=lang)
                return

            # Accept MT5-style crypto symbols (ETHUSD) and normalize to scanner pair format (ETH/USDT).
            if "/" not in symbol and symbol.endswith("USD"):
                base = symbol[:-3].upper()
                crypto_bases = {
                    "BTC", "ETH", "SOL", "XRP", "DOGE", "ADA", "AVAX", "BNB", "LTC", "BCH",
                    "DOT", "LINK", "TRX", "UNI", "ATOM", "POL", "HBAR", "PEPE", "SHIB", "PAXG",
                }
                if base in crypto_bases:
                    symbol = f"{base}/USDT"

            if symbol == "XAUUSD":
                from scanners.xauusd import xauusd_scanner
                signal = xauusd_scanner.scan()
                if signal is None:
                    self._send_text(chat_id, "No active XAUUSD signal right now.")
                    return
                lines = [
                    f"XAUUSD signal explanation",
                    f"Direction: {signal.direction.upper()} | Confidence: {signal.confidence}%",
                    f"Entry: {signal.entry}  SL: {signal.stop_loss}  TP2: {signal.take_profit_2}",
                ]
                if side_hint and signal.direction != side_hint:
                    lines.append(f"Note: current direction is {signal.direction.upper()}, not {side_hint.upper()}.")
                lines.append("Reasons:")
                for r in signal.reasons[:6]:
                    lines.append(f"- {r}")
                if signal.warnings:
                    lines.append("Warnings:")
                    for w in signal.warnings[:3]:
                        lines.append(f"- {w}")
                self._send_text(chat_id, "\n".join(lines)[:3900])
                return

            if "/USDT" in symbol:
                from scanners.crypto_sniper import crypto_sniper
                opp = crypto_sniper.analyze_single(symbol)
                if opp is None:
                    self._send_text(chat_id, f"No active signal now for {symbol}.")
                    return
                signal = opp.signal
                lines = [
                    f"{symbol} signal explanation",
                    f"Direction: {signal.direction.upper()} | Confidence: {signal.confidence}%",
                    f"Setup: {opp.setup_type}",
                    f"Entry: {signal.entry}  SL: {signal.stop_loss}  TP2: {signal.take_profit_2}",
                ]
                if side_hint and signal.direction != side_hint:
                    lines.append(f"Note: current direction is {signal.direction.upper()}, not {side_hint.upper()}.")
                lines.append("Reasons:")
                for r in signal.reasons[:6]:
                    lines.append(f"- {r}")
                if signal.warnings:
                    lines.append("Warnings:")
                    for w in signal.warnings[:3]:
                        lines.append(f"- {w}")
                self._send_text(chat_id, "\n".join(lines)[:3900])
                return

            try:
                fx_majors = {str(x).upper() for x in config.get_fx_major_symbols()}
            except Exception:
                fx_majors = set()
            if ("/" not in symbol) and (symbol in fx_majors):
                from scanners.fx_major_scanner import fx_major_scanner
                opp = fx_major_scanner.analyze_single(symbol)
                if opp is None:
                    self._send_text(chat_id, f"No active signal now for {symbol}.")
                    return
                signal = opp.signal
                lines = [
                    f"{symbol} signal explanation",
                    f"Direction: {signal.direction.upper()} | Confidence: {signal.confidence}%",
                    f"Setup: {opp.setup_type} | VolRatio: {opp.vol_vs_avg}x | PairGroup: {opp.pair_group}",
                    f"Entry: {signal.entry}  SL: {signal.stop_loss}  TP2: {signal.take_profit_2}",
                ]
                if side_hint and signal.direction != side_hint:
                    lines.append(f"Note: current direction is {signal.direction.upper()}, not {side_hint.upper()}.")
                lines.append("Reasons:")
                for r in signal.reasons[:6]:
                    lines.append(f"- {r}")
                if signal.warnings:
                    lines.append("Warnings:")
                    for w in signal.warnings[:3]:
                        lines.append(f"- {w}")
                self._send_text(chat_id, "\n".join(lines)[:3900])
                return

            from scanners.stock_scanner import stock_scanner
            opp = stock_scanner._analyze_stock(symbol)  # uses current market data and live signal logic
            if opp is None:
                self._send_text(chat_id, f"No active signal now for {symbol}. It may have expired.")
                return
            signal = opp.signal
            lines = [
                f"{symbol} signal explanation",
                f"Direction: {signal.direction.upper()} | Confidence: {signal.confidence}%",
                f"Setup: {opp.setup_type} | VolRatio: {opp.vol_vs_avg}x | WinRate~ {round(opp.setup_win_rate*100,1)}%",
                f"Entry: {signal.entry}  SL: {signal.stop_loss}  TP2: {signal.take_profit_2}",
            ]
            if side_hint and signal.direction != side_hint:
                lines.append(f"Note: current direction is {signal.direction.upper()}, not {side_hint.upper()}.")
            lines.append("Reasons:")
            for r in signal.reasons[:6]:
                lines.append(f"- {r}")
            if signal.warnings:
                lines.append("Warnings:")
                for w in signal.warnings[:3]:
                    lines.append(f"- {w}")
            self._send_text(chat_id, "\n".join(lines)[:3900])
        except Exception as e:
            self._send_text(chat_id, f"Signal explanation failed: {str(e)[:200]}")

    def _run_research_reply(self, chat_id: int, question: str, lang: str = "en") -> None:
        """Run AI research in background and send response to Telegram."""
        try:
            from agent.brain import get_brain
            answer = get_brain().quick_answer(self._research_prompt_with_language(question, lang))
            if not answer:
                self._send_text_localized(chat_id, "no_answer", lang=lang)
                return
            if "credit balance is too low" in answer.lower():
                self._send_text_localized(chat_id, "ai_credit_low", lang=lang)
                return
            self._send_text(chat_id, answer[:3900])
        except Exception as e:
            self._send_text_localized(chat_id, "research_failed", lang=lang, err=str(e)[:200])

    def _format_mt5_symbol_snapshot(self, snap: dict, symbol: str, lang: str) -> str:
        if not snap.get("enabled", False):
            return self._tr(lang, "mt5_disconnected")
        if not snap.get("connected", False):
            err = str(snap.get("error") or "-")
            return f"{self._tr(lang, 'mt5_disconnected')}\nerror={err}"
        positions = list(snap.get("positions", []) or [])
        orders = list(snap.get("orders", []) or [])
        if not positions and not orders:
            return self._tr(lang, "mt5_query_none", symbol=symbol)

        title = self._tr(lang, "mt5_query_title", symbol=symbol)
        lines = [title]
        resolved = str(snap.get("resolved_symbol") or "")
        if resolved:
            lines.append(f"resolved={resolved}")
        lines.append(
            f"login={snap.get('account_login') or '-'} server={snap.get('account_server') or '-'} free_margin={snap.get('free_margin') or '-'}"
        )
        if positions:
            lines.append(f"open_positions={len(positions)}")
            for p in positions[:5]:
                lines.append(
                    f"- {p.get('symbol')} {str(p.get('type','')).upper()} vol={p.get('volume')} "
                    f"open={p.get('price_open')} now={p.get('price_current')} pnl={p.get('profit')} "
                    f"sl={p.get('sl')} tp={p.get('tp')} ticket={p.get('ticket')}"
                )
        if orders:
            lines.append(f"pending_orders={len(orders)}")
            for o in orders[:5]:
                lines.append(
                    f"- {o.get('symbol')} {str(o.get('type','')).upper()} vol={o.get('volume')} "
                    f"price={o.get('price_open')} sl={o.get('sl')} tp={o.get('tp')} ticket={o.get('ticket')}"
                )
        return "\n".join(lines)[:3900]

    def _format_mt5_closed_history_snapshot(self, snap: dict, lang: str = "en") -> str:
        if not snap.get("enabled", False):
            return self._tr(lang, "mt5_disconnected")
        if not snap.get("connected", False):
            err = str(snap.get("error") or "-")
            return f"{self._tr(lang, 'mt5_disconnected')}\nerror={err}"

        rows = list(snap.get("closed_trades", []) or [])
        hours = int(snap.get("hours", 24) or 24)
        req_symbol = str(snap.get("requested_symbol") or "").upper()
        resolved = str(snap.get("resolved_symbol") or "").upper()
        symbol_display = resolved or req_symbol
        symbol_part = f" ({symbol_display})" if symbol_display else ""
        qmode = str(snap.get("history_query_mode") or "").strip()
        err = str(snap.get("error") or "").strip()

        if not rows:
            base = self._tr(lang, "mt5_history_none", hours=hours, symbol_part=symbol_part)
            extras = []
            if qmode:
                extras.append(f"query_mode={qmode}")
            if err:
                extras.append(f"error={err}")
            return base if not extras else (base + "\n" + " | ".join(extras))

        title = self._tr(lang, "mt5_history_title", hours=hours, symbol_part=symbol_part)
        lines = [title]
        lines.append(f"login={snap.get('account_login') or '-'} server={snap.get('account_server') or '-'}")
        if qmode:
            lines.append(f"query_mode={qmode}")
        for r in rows[:8]:
            reason = str(r.get("reason") or "UNKNOWN").upper()
            pnl = r.get("pnl")
            pnl_txt = f"{float(pnl):.2f}" if pnl is not None else "-"
            symbol = str(r.get("symbol") or "-")
            closed_at = str(r.get("closed_at_utc") or "-")
            close_price = r.get("close_price")
            close_txt = str(close_price if close_price is not None else "-")
            vol_txt = str(r.get("volume") if r.get("volume") is not None else "-")
            lines.append(
                f"- {symbol} reason={reason} pnl={pnl_txt} vol={vol_txt} close={close_txt}"
            )
            lines.append(f"  closed_at={closed_at}")
        if rows:
            tp = sum(1 for r in rows if str(r.get("reason", "")).upper() == "TP")
            sl = sum(1 for r in rows if str(r.get("reason", "")).upper() == "SL")
            manual = sum(1 for r in rows if str(r.get("reason", "")).upper() in {"MANUAL", "EA"})
            lines.append(f"summary: TP={tp} SL={sl} manual/ea={manual} total={len(rows)}")
        return "\n".join(lines)[:3900]

    def _format_us_open_guard_status(self, snap: dict, lang: str = "en") -> str:
        s = dict(snap or {})
        if not s.get("ok", False):
            return f"US Open Guard Status\nerror={s.get('error','-')}"
        macro = dict(s.get("macro_freeze") or {})
        cb = dict(s.get("circuit_breaker") or {})
        mood = dict(s.get("mood_stop") or {})
        symcd = dict(s.get("symbol_cooldown") or {})

        def _state(flag, active):
            if not flag:
                return "DISABLED"
            return "ACTIVE" if active else "CLEAR"

        lines = ["US Open Guard Status"]
        lines.append(f"now={s.get('now_ny','-')} | {s.get('now_utc','-')}")
        lines.append(
            f"window: in_window={s.get('in_us_open_window')} premarket={s.get('premarket')} "
            f"elapsed_after_open={s.get('elapsed_after_open_min')}m"
        )
        lines.append(f"window_ny: {s.get('window_start_ny','-')} -> {s.get('window_end_ny','-')}")
        lines.append("")
        lines.append(f"macro_freeze: {_state(macro.get('enabled', False), macro.get('active', False))}")
        lines.append(f"reason: {macro.get('reason','-')}")
        if macro.get('headline'):
            lines.append(f"headline: {macro.get('headline')}")
        if macro.get('release_eta_min') is not None:
            lines.append(f"release_eta: ~{macro.get('release_eta_min')}m")
        lines.append("")
        lines.append(f"circuit_breaker: {_state(cb.get('enabled', False), cb.get('active', False))}")
        lines.append(f"reason: {cb.get('reason','-')}")
        if cb.get('release_eta_min') is not None:
            lines.append(f"release_eta: ~{cb.get('release_eta_min')}m")
        if cb.get('release_at_ny'):
            lines.append(f"release_at: {cb.get('release_at_ny')}")
        lines.append("")
        lines.append(f"mood_stop: {_state(mood.get('enabled', False), mood.get('active', False))}")
        lines.append(f"weak_cycles: {mood.get('weak_cycles',0)}/{mood.get('weak_cycles_to_stop',0)}")
        if mood.get('reason'):
            lines.append(f"reason: {mood.get('reason')}")
        if mood.get('release_at_ny'):
            lines.append(f"release_at: {mood.get('release_at_ny')}")
        lines.append("")
        lines.append(
            f"symbol_cooldown: enabled={symcd.get('enabled')} cooldown={symcd.get('cooldown_min')}m "
            f"tracked={symcd.get('tracked_symbols')} record_new_only={symcd.get('record_new_only')}"
        )
        return "\n".join(lines)[:3900]

    def _format_us_open_guard_status_compact(self, snap: dict, lang: str = "en") -> str:
        s = dict(snap or {})
        if not s.get("ok", False):
            return f"US Open Guard (compact)\nerror={s.get('error','-')}"
        macro = dict(s.get("macro_freeze") or {})
        cb = dict(s.get("circuit_breaker") or {})
        mood = dict(s.get("mood_stop") or {})
        symcd = dict(s.get("symbol_cooldown") or {})
        lines = ["US Open Guard (compact)"]
        window_label = "IN" if s.get('in_us_open_window') else "OUT"
        lines.append(
            f"window={window_label} "
            f"premarket={bool(s.get('premarket'))} t+={s.get('elapsed_after_open_min')}m"
        )
        macro_label = "ACTIVE" if macro.get('active') else "clear"
        lines.append(
            f"macro={macro_label}"
            + (f" (~{macro.get('release_eta_min')}m)" if macro.get("release_eta_min") is not None else "")
        )
        if macro.get("active") and macro.get("reason"):
            lines.append(f"macro_reason: {str(macro.get('reason'))[:140]}")
        cb_label = "ACTIVE" if cb.get('active') else "clear"
        lines.append(
            f"cb={cb_label}"
            + (f" (~{cb.get('release_eta_min')}m)" if cb.get("release_eta_min") is not None else "")
        )
        if cb.get("active") and cb.get("reason"):
            lines.append(f"cb_reason: {str(cb.get('reason'))[:140]}")
        mood_label = "ACTIVE" if mood.get('active') else "clear"
        lines.append(
            f"mood={mood_label} "
            f"weak={mood.get('weak_cycles',0)}/{mood.get('weak_cycles_to_stop',0)}"
        )
        lines.append(
            f"sym_cooldown={symcd.get('cooldown_min')}m tracked={symcd.get('tracked_symbols')} "
            f"new_only={symcd.get('record_new_only')}"
        )
        return "\n".join(lines)[:3900]

    def _format_us_open_guard_status_why(self, snap: dict, lang: str = "en") -> str:
        s = dict(snap or {})
        if not s.get("ok", False):
            return f"US Open Guard Status (why)\nerror={s.get('error','-')}"
        macro = dict(s.get("macro_freeze") or {})
        cb = dict(s.get("circuit_breaker") or {})
        mood = dict(s.get("mood_stop") or {})
        symcd = dict(s.get("symbol_cooldown") or {})
        lines = ["US Open Guard Status (why)"]
        lines.append(f"now={s.get('now_ny','-')} | {s.get('now_utc','-')}")
        lines.append(
            f"window={s.get('window_start_ny','-')} -> {s.get('window_end_ny','-')} "
            f"(in_window={s.get('in_us_open_window')}, premarket={s.get('premarket')}, t+={s.get('elapsed_after_open_min')}m)"
        )
        lines.append("")
        lines.append("macro-freeze logic:")
        lines.append(
            f"- enabled={macro.get('enabled')} min_score>={macro.get('min_score')} "
            f"max_age<={macro.get('max_age_min')}m priority_only={macro.get('priority_only')}"
        )
        macro_st = "ACTIVE" if macro.get('active') else "clear"
        lines.append(f"- status={macro_st} reason={macro.get('reason','-')}")
        if macro.get("headline"):
            lines.append(f"- headline={macro.get('headline')}")
        if macro.get("release_eta_min") is not None:
            lines.append(f"- release_eta≈{macro.get('release_eta_min')}m")
        lines.append("")
        lines.append("circuit-breaker logic:")
        lines.append(f"- enabled={cb.get('enabled')} check_start_after_open={cb.get('check_start_min')}m")
        cb_st = "ACTIVE" if cb.get('active') else "clear"
        lines.append(f"- status={cb_st} reason={cb.get('reason','-')}")
        if cb.get("release_eta_min") is not None:
            lines.append(f"- release_eta≈{cb.get('release_eta_min')}m")
        if cb.get("release_at_ny"):
            lines.append(f"- release_at={cb.get('release_at_ny')}")
        lines.append("")
        lines.append("mood-stop logic:")
        lines.append(f"- enabled={mood.get('enabled')} weak_cycles={mood.get('weak_cycles',0)}/{mood.get('weak_cycles_to_stop',0)}")
        mood_st = "ACTIVE" if mood.get('active') else "clear"
        lines.append(f"- status={mood_st} reason={mood.get('reason','-')}")
        if mood.get("release_at_ny"):
            lines.append(f"- release_at={mood.get('release_at_ny')}")
        lines.append("")
        lines.append("symbol alert control:")
        lines.append(
            f"- cooldown={symcd.get('cooldown_min')}m tracked_symbols={symcd.get('tracked_symbols')} "
            f"record_new_only={symcd.get('record_new_only')}"
        )
        qg = dict(s.get("quality_guard") or {})
        lines.append("")
        lines.append("quality-guard summary:")
        lines.append(
            f"- stats={qg.get('stats_status','-')} seg={qg.get('segment','-')} "
            f"segments_verdict={qg.get('segments_verdict','-')} cache_age={qg.get('cache_age_sec','-')}s"
        )
        if qg.get('last_diag_age_sec') is not None:
            lines.append(f"- last_diag_age={qg.get('last_diag_age_sec')}s")
        for stage in ("plan", "monitor"):
            d = dict(qg.get(f"last_{stage}") or {})
            if not d:
                continue
            lines.append(
                f"- {stage}: seg={d.get('segment','-')} in={d.get('input',0)} out={d.get('output',0)} "
                f"sym_block={d.get('symbol_loss_cap_blocked',0)} sym_recover={d.get('symbol_recovered',0)} "
                f"setup_block={d.get('setup_hard_blocked',0)} penalty={d.get('setup_penalized',0)} boost={d.get('setup_boosted',0)}"
            )
        capped = list(qg.get('capped_symbols') or [])
        if capped:
            top = ", ".join(f"{r.get('symbol')}({r.get('net_r')}R/{r.get('losses')}L)" for r in capped[:5])
            lines.append(f"- capped_symbols({len(capped)}): {top}")
        rec_state = dict(qg.get('recovery_state') or {})
        if rec_state:
            preview = []
            for sym, rec in sorted(rec_state.items(), key=lambda kv: float((kv[1] or {}).get('ts',0.0) or 0.0), reverse=True)[:5]:
                rr = dict(rec or {})
                preview.append(f"{sym}(count={rr.get('count',0)}, conf={rr.get('conf','-')}, vol={rr.get('vol','-')})")
            lines.append(f"- recovery_used({len(rec_state)}): " + "; ".join(preview))
        lines.append("")
        lines.append("Use /us_open_guard_status compact for mobile view.")
        return "\n".join(lines)[:3900]

    def _format_signal_dashboard_compare(self, a: dict, b: dict, lang: str = "en") -> str:
        ra = dict(a or {})
        rb = dict(b or {})
        sa = dict(ra.get("summary") or {})
        sb = dict(rb.get("summary") or {})
        sima = dict(ra.get("simulation") or {})
        simb = dict(rb.get("simulation") or {})
        ui = str(lang or "en").lower()
        la = self._signal_dashboard_market_label(
            str(ra.get("market_filter") or ra.get("market_filter_label") or "A"),
            lang=ui,
        )
        lb = self._signal_dashboard_market_label(
            str(rb.get("market_filter") or rb.get("market_filter_label") or "B"),
            lang=ui,
        )
        days = int(ra.get("days", rb.get("days", 1)) or 1)
        mode = str(ra.get("window_mode") or rb.get("window_mode") or "rolling_days")
        period = self._signal_dashboard_window_label(mode, days, lang=ui)

        def t(en: str, th: str, de: Optional[str] = None) -> str:
            if ui == "th":
                return th
            if ui == "de":
                return de or en
            return en

        lines = [t("Signal Dashboard Compare", "Signal Dashboard เปรียบเทียบ", "Signal-Dashboard Vergleich")]
        lines.append(f"{t('period', 'ช่วงเวลา', 'Zeitraum')}={period}")
        lines.append(f"A={la} | B={lb}")
        lines.append("")
        lines.append(f"{t('Metric', 'ตัวชี้วัด', 'Metrik'):<22} | A               | B")
        lines.append("-" * 54)
        def _row(name, va, vb):
            lines.append(f"{name:<22} | {str(va):<15} | {str(vb):<15}")
        _row(t("sent", "ส่ง", "gesendet"), sa.get("sent",0), sb.get("sent",0))
        _row(t("resolved", "ปิดผลแล้ว", "aufgelöst"), sa.get("resolved",0), sb.get("resolved",0))
        _row(t("pending", "ค้างอยู่", "offen"), sa.get("pending",0), sb.get("pending",0))
        _row(t("wins/losses", "ชนะ/แพ้", "Gewinn/Verlust"), f"{sa.get('wins',0)}/{sa.get('losses',0)}", f"{sb.get('wins',0)}/{sb.get('losses',0)}")
        _row("WR%", sa.get("win_rate",0.0), sb.get("win_rate",0.0))
        _row("netR", sa.get("net_r",0.0), sb.get("net_r",0.0))
        _row("pendingMarkR", sa.get("pending_mark_r",0.0), sb.get("pending_mark_r",0.0))
        _row(t("sim balance", "ยอดจำลอง", "Sim-Saldo"), sima.get("marked_balance", "-"), simb.get("marked_balance", "-"))
        best_a = list(ra.get("best_symbols") or [])
        best_b = list(rb.get("best_symbols") or [])
        lines.append("")
        if best_a:
            lines.append(f"{t('best', 'เด่นสุด', 'beste')} {la}: " + ", ".join([f"{x.get('symbol')}({x.get('session_r')})" for x in best_a[:3]]))
        if best_b:
            lines.append(f"{t('best', 'เด่นสุด', 'beste')} {lb}: " + ", ".join([f"{x.get('symbol')}({x.get('session_r')})" for x in best_b[:3]]))
        return "\n".join(lines)[:3900]

    def _format_mt5_affordable_snapshot(self, snap: dict, lang: str = "en") -> str:
        if not snap.get("enabled", False):
            return self._tr(lang, "mt5_disconnected")
        if not snap.get("connected", False):
            err = str(snap.get("error") or "-")
            return f"MT5 Affordable Symbols\nerror={err}"

        category = str(snap.get("category") or "all")
        only_ok = bool(snap.get("only_ok", False))
        summary = dict(snap.get("summary", {}) or {})
        rows = list(snap.get("rows", []) or [])
        if lang == "th":
            title = "MT5 คู่ที่เทรดได้ตามทุนตอนนี้"
        elif lang == "de":
            title = "MT5 Leistbare Symbole (Live)"
        else:
            title = "MT5 Affordable Symbols (Live)"
        lines = [title]
        lines.append(
            f"account={snap.get('account_server') or '-'}|{snap.get('account_login') or '-'} "
            f"category={category} filter={'ok_only' if only_ok else 'all'}"
        )
        lines.append(
            f"balance={snap.get('balance')} equity={snap.get('equity')} free_margin={snap.get('free_margin')} {snap.get('currency') or ''}".strip()
        )
        lines.append(
            f"budget={snap.get('margin_budget_pct')}% -> allowed_margin={snap.get('allowed_margin')} | min_free_after={snap.get('min_free_margin_after_trade')} | max_spread={snap.get('micro_max_spread_pct')}%"
        )
        sp = dict(snap.get("symbol_policy", {}) or {})
        lines.append(
            f"symbol_policy: allowlist_active={sp.get('allowlist_active')} allow={sp.get('allow_count')} block={sp.get('block_count')}"
        )
        lines.append(
            f"summary: ok_now={summary.get('ok_now',0)} market_ok={summary.get('market_ok',0)} margin_ok={summary.get('margin_ok',0)} spread_ok={summary.get('spread_ok',0)} checked={summary.get('checked',0)}"
        )
        by_cat = dict(summary.get("by_category", {}) or {})
        if by_cat:
            cat_parts = []
            for c in ("crypto", "fx", "metal", "index"):
                if c in by_cat:
                    b = dict(by_cat.get(c) or {})
                    cat_parts.append(f"{c}:{b.get('ok',0)}/{b.get('total',0)}")
            if cat_parts:
                lines.append("by_category: " + " | ".join(cat_parts))
        if not rows:
            if only_ok:
                lines.append("No symbols currently pass all conditions (margin + spread + policy).")
            else:
                lines.append("No affordable symbols found under current account constraints/policy.")
            return "\n".join(lines)[:3900]

        lines.append("top:")
        for r in rows[: max(1, int(snap.get("limit", 12) or 12))]:
            status = str(r.get("status") or "-")
            flags = []
            if r.get("margin_ok"):
                flags.append("margin✓")
            else:
                flags.append("margin✗")
            if r.get("spread_ok"):
                flags.append("spread✓")
            else:
                flags.append("spread✗")
            if r.get("policy_ok"):
                flags.append("policy✓")
            else:
                flags.append("policy✗")
            lines.append(
                f"- {r.get('symbol')} ({r.get('category')}) status={status} "
                f"m={r.get('margin_min_lot')} sp={r.get('spread_pct')}% minLot={r.get('vol_min')} "
                f"[{' '.join(flags)}]"
            )
        return "\n".join(lines)[:3900]

    def _format_mt5_pm_watch_snapshot(self, snap: dict, lang: str = "en") -> str:
        if not snap.get("enabled", False):
            return self._tr(lang, "mt5_disconnected")
        if not snap.get("ok", False):
            return f"MT5 Position Manager Watch\nerror={snap.get('error') or '-'}"
        lines = ["MT5 Position Manager Watch"]
        lines.append(f"account={snap.get('account_key') or '-'}")
        if snap.get("requested_symbol"):
            lines.append(f"requested={snap.get('requested_symbol')}")
        if snap.get("resolved_symbol"):
            lines.append(f"resolved={snap.get('resolved_symbol')}")
        lines.append(f"positions={snap.get('positions', 0)} watched={snap.get('watched', 0)}")
        rules = dict(snap.get("rules", {}) or {})
        if rules:
            lines.append(
                "rules: "
                f"BE@{rules.get('break_even_r')}R partial@{rules.get('partial_tp_r')}R "
                f"trail@{rules.get('trail_start_r')}R gap={rules.get('trail_gap_r')}R "
                f"time-stop={rules.get('time_stop_min')}m<{rules.get('time_stop_flat_r')}R"
            )
            if rules.get("early_risk_enabled") or rules.get("spread_spike_protect_enabled"):
                lines.append(
                    "risk-protect: "
                    f"early@{rules.get('early_risk_trigger_r')}R -> SL@{rules.get('early_risk_sl_r')}R "
                    f"(buf {rules.get('early_risk_buffer_r')}R) | "
                    f"spread>={rules.get('spread_spike_pct')}%"
                )
        entries = list(snap.get("entries", []) or [])
        if not entries:
            lines.append("No open positions in watch scope.")
            return "\n".join(lines)[:3900]
        for row in entries[:8]:
            st_flags = dict(row.get("state") or {})
            nxt = dict(row.get("next_checks") or {})
            flag_list = []
            if st_flags.get("breakeven_done"): flag_list.append("BE✓")
            if st_flags.get("partial_done"): flag_list.append("PT✓")
            if st_flags.get("time_stop_done"): flag_list.append("TS✓")
            if st_flags.get("early_risk_done"): flag_list.append("ER✓")
            if nxt.get("breakeven_ready"): flag_list.append("BE!")
            if nxt.get("partial_ready"): flag_list.append("PT!")
            if nxt.get("trail_ready"): flag_list.append("TR!")
            if nxt.get("time_stop_ready"): flag_list.append("TS!")
            if nxt.get("early_risk_ready"): flag_list.append("ER!")
            if nxt.get("spread_spike_ready"): flag_list.append("SP!")
            lines.append(
                f"- {row.get('symbol')} {str(row.get('type','')).upper()} ticket={row.get('ticket')} vol={row.get('volume')} "
                f"pnl={row.get('profit')} r={row.get('r_now')} age={row.get('age_min')}m"
            )
            lines.append(
                f"  open={row.get('price_open')} now={row.get('price_current')} sl={row.get('sl')} tp={row.get('tp')} "
                f"eligible={row.get('eligible')} metrics_valid={row.get('metrics_valid')} spread={row.get('spread_pct')}%"
            )
            if row.get("no_sl"):
                lines.append("  note=no_sl (PM BE/trail logic waits for an SL baseline)")
            if st_flags:
                lines.append(
                    "  state: "
                    f"BE={st_flags.get('breakeven_done')} partial={st_flags.get('partial_done')} time_stop={st_flags.get('time_stop_done')} early_risk={st_flags.get('early_risk_done')} "
                    f"last={st_flags.get('last_action') or '-'} @ {st_flags.get('last_action_at') or '-'}"
                )
            adp = dict(row.get("adaptive_pm") or {})
            adp_rules = dict(adp.get("rules") or {})
            adp_f = dict(adp.get("factors") or {})
            if adp_rules:
                lines.append(
                    "  pm-adapt: "
                    f"BE@{adp_rules.get('break_even_r')}R PT@{adp_rules.get('partial_tp_r')}R "
                    f"TR@{adp_rules.get('trail_start_r')}R gap={adp_rules.get('trail_gap_r')}R "
                    f"TS={adp_rules.get('time_stop_min')}m<{adp_rules.get('time_stop_flat_r')}R "
                    f"ER@{adp_rules.get('early_risk_trigger_r')}R"
                )
            if adp_f:
                lines.append(
                    "  pm-factors: "
                    f"family={adp_f.get('family')} samples={adp_f.get('samples')} wr={adp_f.get('win_rate')} "
                    f"mae={adp_f.get('mae')} spread={adp_f.get('spread_pct')}% bias={adp_f.get('protect_bias')}"
                )
            if flag_list:
                lines.append(f"  next: {', '.join(flag_list)}")
            dist = dict(row.get("distances") or {})
            if dist:
                parts = [
                    f"BE {dist.get('to_be_trigger_r','-')}R/{dist.get('to_be_trigger_price','-')}",
                    f"PT {dist.get('to_partial_r','-')}R",
                    f"TR {dist.get('to_trail_r','-')}R",
                    f"TP {dist.get('to_tp_price','-')}",
                    f"SL {dist.get('to_sl_price','-')}",
                ]
                if dist.get("to_early_risk_trigger_r") is not None:
                    parts.append(f"ER {dist.get('to_early_risk_trigger_r')}R")
                lines.append("  distance: " + " | ".join(parts))
        return "\n".join(lines)[:3900]

    def _format_mt5_policy_keys(self) -> str:
        from learning.mt5_orchestrator import mt5_orchestrator

        specs = list(mt5_orchestrator.policy_key_specs() or [])
        lines = ["MT5 Policy Keys", "Use: /mt5_policy set <key> <value>"]
        for s in specs:
            lines.append(
                f"- {s.get('key')} [{s.get('type')}] default={s.get('default')} | ex: {s.get('example')}"
            )
            lines.append(f"  {s.get('desc')}")
        presets = list(mt5_orchestrator.policy_presets() or [])
        if presets:
            lines.append("")
            lines.append("Presets:")
            for p in presets:
                lines.append(f"- {p.get('name')}: {p.get('desc')}")
            lines.append("Use: /mt5_policy preset micro_safe")
        return "\n".join(lines)[:3900]

    def _format_mt5_exec_reasons_report(self, report: dict, lang: str = "en") -> str:
        rep = dict(report or {})
        if not rep.get("ok", False):
            return f"MT5 Execution Reasons\nerror={rep.get('message','unknown')}"
        summ = dict(rep.get("summary") or {})
        reasons = list(rep.get("reasons") or [])
        bysym = list(rep.get("by_symbol") or [])
        samples = list(rep.get("samples") or [])
        delta = dict(rep.get("delta") or {})
        recs = list(rep.get("recommendations") or [])
        hours = int(rep.get("hours", 24) or 24)
        sym = str(rep.get("symbol") or "").upper()
        symbol_part = f" ({sym})" if sym else ""
        lines = [f"MT5 Execution Reasons{symbol_part}"]
        lines.append(f"lookback={hours}h account={rep.get('account_key') or '-'}")
        lines.append(
            f"total={summ.get('total',0)} filled={summ.get('filled',0)} skipped={summ.get('skipped',0)} "
            f"guard_blocked={summ.get('guard_blocked',0)} errors={summ.get('errors',0)}"
        )
        if reasons:
            lines.append("")
            lines.append("Top reasons:")
            for r in reasons[:10]:
                lines.append(f"- [{r.get('status')}] x{r.get('count')}  {r.get('message')}")
        if bysym:
            lines.append("")
            lines.append("By symbol:")
            for r in bysym[:8]:
                lines.append(
                    f"- {r.get('symbol')} sent={r.get('sent')} filled={r.get('filled')} "
                    f"skipped={r.get('skipped')} blocked={r.get('guard_blocked')}"
                )
        if delta.get("enabled"):
            pre = dict(delta.get("pre") or {})
            post = dict(delta.get("post") or {})
            lines.append("")
            lines.append("Delta (before/after patch):")
            lines.append(f"- marker={delta.get('marker_utc')}")
            lines.append(f"- total before={pre.get('total',0)} after={post.get('total',0)}")
            changes = list(delta.get("changes") or [])
            if changes:
                ranked = sorted(changes, key=lambda r: abs(int(r.get('delta',0) or 0)), reverse=True)
                for row in ranked[:5]:
                    lines.append(
                        f"- {row.get('bucket')}: {row.get('before',0)} -> {row.get('after',0)} (Δ {row.get('delta',0)})"
                    )
        if recs:
            lines.append("")
            lines.append("Recommendations:")
            for r in recs[:5]:
                pr = str(r.get('priority') or '-').upper()
                lines.append(f"- [{pr}] {r.get('code')}: {r.get('action')}")
        if samples:
            lines.append("")
            lines.append("Recent samples:")
            for s in samples[:5]:
                conf = s.get('confidence')
                nprob = s.get('neural_prob')
                conf_txt = '-' if conf is None else f"{float(conf):.1f}"
                np_txt = '-' if nprob is None else f"{float(nprob):.2f}"
                bsym = str(s.get('broker_symbol') or '-')
                ssym = str(s.get('signal_symbol') or '-')
                symtxt = bsym if (bsym and bsym != '-') else ssym
                lines.append(f"- {symtxt} [{s.get('status')}] conf={conf_txt} np={np_txt} :: {s.get('message')}")
        return "\n".join(lines)[:3900]

    def _format_mt5_pm_learning_report(self, report: dict, lang: str = "en") -> str:
        r = dict(report or {})
        if not r.get("enabled", False):
            return self._tr(lang, "mt5_disconnected")
        if not r.get("ok", False):
            return f"MT5 PM Learning Report\nerror={r.get('error') or '-'}"

        def _pct(v):
            if v is None:
                return "-"
            try:
                return f"{float(v) * 100:.0f}%"
            except Exception:
                return "-"

        summary = dict(r.get("summary", {}) or {})
        filters = dict(r.get("filters", {}) or {})
        lines = ["MT5 PM Learning Report"]
        lines.append(f"account={r.get('account_key') or '-'} lookback={r.get('days', 30)}d")
        filter_parts = []
        if filters.get("symbol"):
            filter_parts.append(f"symbol={filters.get('symbol')}")
        if filters.get("action"):
            filter_parts.append(f"action={filters.get('action')}")
        if filter_parts:
            lines.append("filter: " + " | ".join(filter_parts))
        if r.get("sync") is not None:
            s = dict(r.get("sync", {}) or {})
            if s.get("ok"):
                lines.append(
                    f"sync: updated={s.get('updated',0)} unresolved={s.get('still_unresolved',0)} closed={s.get('closed_rows_seen',0)} q={s.get('history_query_mode','-')}"
                )
            else:
                lines.append(f"sync: {s.get('error') or 'failed'}")
        lines.append(
            f"actions: total={summary.get('total_actions',0)} resolved={summary.get('resolved_actions',0)} unresolved={summary.get('unresolved_actions',0)}"
        )
        overall = list(r.get("actions_overall", []) or [])
        recs = list(r.get("recommendations", []) or [])
        recs_by_regime = list(r.get("recommendations_by_regime", []) or [])
        draft_result = dict(r.get("draft_result", {}) or {})
        if overall:
            lines.append("overall:")
            for row in overall[:6]:
                pos_txt = _pct(row.get("positive_rate"))
                neg_txt = _pct(row.get("negative_rate"))
                tp_txt = _pct(row.get("tp_rate"))
                avg_pnl = "-" if row.get("avg_pnl") is None else str(row.get("avg_pnl"))
                lines.append(
                    f"- {row.get('label')}: n={row.get('samples')} resolved={row.get('resolved')} "
                    f"pos={pos_txt} neg={neg_txt} tp={tp_txt} avg_pnl={avg_pnl}"
                )
        symbols = list(r.get("symbols", []) or [])
        if not symbols:
            lines.append("No PM learning samples yet. Let PM actions resolve first (TP/SL/manual close).")
            if recs:
                lines.append("recommendations:")
                for rec in recs[:4]:
                    lines.append(
                        f"- {rec.get('key')} ({rec.get('action')}): {rec.get('current')} -> {rec.get('suggested')} "
                        f"[{rec.get('direction')}, {rec.get('confidence')}, n={rec.get('samples')}]"
                    )
            return "\n".join(lines)[:3900]
        lines.append("by symbol:")
        for sym in symbols:
            pr_txt = _pct(sym.get("positive_rate"))
            nr_txt = _pct(sym.get("negative_rate"))
            avg_pnl = "-" if sym.get("avg_pnl") is None else str(sym.get("avg_pnl"))
            lines.append(
                f"- {sym.get('label')}: n={sym.get('samples')} resolved={sym.get('resolved')} "
                f"pos={pr_txt} neg={nr_txt} avg_pnl={avg_pnl}"
            )
            ba = dict(sym.get("best_action") or {})
            wa = dict(sym.get("weak_action") or {})
            if ba:
                lines.append(
                    f"  best: {ba.get('label')} pos={_pct(ba.get('positive_rate'))} n={ba.get('resolved')}"
                )
            if wa:
                lines.append(
                    f"  weak: {wa.get('label')} neg={_pct(wa.get('negative_rate'))} n={wa.get('resolved')}"
                )
            acts = list(sym.get("actions", []) or [])
            for a in acts[:3]:
                lines.append(
                    f"    · {a.get('label')}: n={a.get('samples')} r={a.get('resolved')} pos={_pct(a.get('positive_rate'))} sl={_pct(a.get('sl_rate'))}"
                )
        if recs:
            lines.append("recommendations:")
            for rec in recs[:5]:
                lines.append(
                    f"- {rec.get('key')} ({rec.get('action')}): {rec.get('current')} -> {rec.get('suggested')} "
                    f"[{rec.get('direction')}, {rec.get('confidence')}, n={rec.get('samples')}]"
                )
                reason = str(rec.get("reason") or "").strip()
                if reason:
                    lines.append(f"  {reason}")
        if recs_by_regime:
            lines.append("recommendations by regime:")
            for bucket in recs_by_regime[:6]:
                regime = str(bucket.get("regime") or "")
                lines.append(
                    f"- {regime}: rows={bucket.get('rows',0)} resolved={bucket.get('resolved_rows',0)}"
                )
                for rec in list(bucket.get("recommendations", []) or [])[:2]:
                    lines.append(
                        f"  · {rec.get('key')}: {rec.get('current')} -> {rec.get('suggested')} "
                        f"[{rec.get('direction')}, {rec.get('confidence')}, n={rec.get('samples')}]"
                    )
        if draft_result:
            if draft_result.get("ok"):
                keys = ",".join(list(draft_result.get("keys", []) or [])[:8]) or "-"
                regimes = ",".join(list(draft_result.get("regimes", []) or [])[:8]) or "-"
                lines.append("policy draft:")
                lines.append(
                    f"saved=yes keys={keys} regimes={regimes}"
                )
                lines.append("Not applied yet. Review with /mt5_policy show and apply manually if desired.")
            else:
                lines.append(f"policy draft: failed ({draft_result.get('message') or draft_result.get('error') or 'unknown'})")
        return "\n".join(lines)[:3900]

    def _load_live_signal_for_symbol(self, symbol: str):
        """Return (signal, meta) for symbol using current scanner logic, or (None, reason_meta)."""
        raw_symbol = str(symbol or "").strip().upper()
        if not raw_symbol:
            return None, {"reason": "missing_symbol"}

        symbol_norm = raw_symbol
        if symbol_norm == "GOLD":
            symbol_norm = "XAUUSD"
        if symbol_norm == "XAUUSD":
            from scanners.xauusd import xauusd_scanner
            sig = xauusd_scanner.scan()
            if sig is None:
                return None, {"reason": "no_active_signal", "symbol": "XAUUSD"}
            return sig, {"symbol": "XAUUSD", "kind": "xau", "setup": getattr(sig, "pattern", "")}

        if "/" not in symbol_norm and symbol_norm.endswith("USD"):
            base = symbol_norm[:-3]
            if base in {
                "BTC", "ETH", "SOL", "XRP", "DOGE", "ADA", "AVAX", "BNB", "LTC", "BCH",
                "DOT", "LINK", "TRX", "UNI", "ATOM", "POL", "HBAR", "PEPE", "SHIB", "PAXG",
            }:
                symbol_norm = f"{base}/USDT"
        if "/USDT" in symbol_norm:
            from scanners.crypto_sniper import crypto_sniper
            opp = crypto_sniper.analyze_single(symbol_norm)
            if opp is None:
                return None, {"reason": "no_active_signal", "symbol": symbol_norm}
            return opp.signal, {
                "symbol": symbol_norm,
                "kind": "crypto",
                "setup": getattr(opp, "setup_type", ""),
                "vol_vs_avg": getattr(opp, "vol_vs_avg", None),
                "setup_win_rate": getattr(opp, "setup_win_rate", None),
            }

        from scanners.stock_scanner import stock_scanner
        opp = stock_scanner._analyze_stock(symbol_norm)
        if opp is None:
            return None, {"reason": "no_active_signal", "symbol": symbol_norm}
        return opp.signal, {
            "symbol": symbol_norm,
            "kind": "stock",
            "setup": getattr(opp, "setup_type", ""),
            "vol_vs_avg": getattr(opp, "vol_vs_avg", None),
            "setup_win_rate": getattr(opp, "setup_win_rate", None),
            "quality": getattr(opp, "quality_tag", None),
        }

    def _format_mt5_adaptive_plan_preview(self, symbol: str, signal, preview: dict, wf_plan=None, lang: str = "en") -> str:
        p = dict(preview or {})
        base = dict(p.get("base", {}) or {})
        ex = dict(p.get("execution", {}) or {})
        ad = dict(p.get("adaptive", {}) or {})
        md = dict(p.get("margin", {}) or {})
        mk = dict(p.get("market", {}) or {})
        acct = dict(p.get("account", {}) or {})
        factors = dict(ad.get("factors") or {})
        wf = wf_plan

        title = "MT5 Adaptive Plan Preview"
        if lang == "th":
            title = "พรีวิวแผนเข้าเทรด MT5 (Adaptive)"
        elif lang == "de":
            title = "MT5 Adaptive-Plan Vorschau"
        lines = [title]
        lines.append(f"symbol={symbol} broker={p.get('broker_symbol') or '-'} status={p.get('status') or '-'}")
        if acct:
            lines.append(
                f"account={acct.get('account_key','-')} balance={acct.get('balance','-')} equity={acct.get('equity','-')} free_margin={acct.get('free_margin','-')}"
            )
        lines.append(
            f"signal: {str(getattr(signal,'direction','')).upper()} conf={getattr(signal,'confidence',0)}% pattern={getattr(signal,'pattern','')}"
        )
        lines.append(
            f"base: entry={base.get('entry')} sl={base.get('stop_loss')} tp2={base.get('take_profit_2')} rr={base.get('risk_reward')}"
        )
        lines.append(
            f"plan: entry={ex.get('entry')} sl={ex.get('stop_loss')} tp1={ex.get('take_profit_1')} tp2={ex.get('take_profit_2')} tp3={ex.get('take_profit_3')} rr={ex.get('risk_reward')}"
        )
        if ad:
            lines.append(
                f"adaptive: applied={ad.get('applied')} reason={ad.get('reason')} rr {ad.get('rr_base')}->{ad.get('rr_target')} "
                f"SLx{ad.get('stop_scale')} sizex{ad.get('size_multiplier')}"
            )
        if factors:
            lines.append(
                f"factors: family={factors.get('family')} samples={factors.get('samples')} win={factors.get('win_rate')} "
                f"mae={factors.get('mae')} spread={factors.get('spread_pct')}% atr={factors.get('atr_pct')}% session={factors.get('session')}"
            )
        if mk:
            lines.append(
                f"market: bid={mk.get('bid')} ask={mk.get('ask')} spread={mk.get('spread')} ({mk.get('spread_pct')}%)"
            )
        if wf is not None:
            lines.append(
                f"risk_gate: allow={getattr(wf,'allow', True)} reason={getattr(wf,'reason','-')} canary={getattr(wf,'canary_mode', True)} "
                f"risk_mult={getattr(wf,'risk_multiplier', 1.0)}"
            )
        lines.append(
            f"size: input_mult={ex.get('volume_multiplier_input')} final_mult={ex.get('volume_multiplier_final')} "
            f"desired_vol={ex.get('desired_volume')} fitted_vol={ex.get('fitted_volume')}"
        )
        lines.append(
            f"margin: free={md.get('free_margin')} required={md.get('required')} reason={md.get('fit_reason')}"
        )
        lines.append("This is a preview only. No order was sent.")
        return "\n".join(lines)[:3900]

    def _format_mt5_adaptive_plan_whatif(self, symbol: str, signal, previews: dict, wf_plan=None, lang: str = "en") -> str:
        title = "MT5 Adaptive Plan What-If"
        if lang == "th":
            title = "MT5 แผนเทรดแบบเปรียบเทียบ (What-if)"
        elif lang == "de":
            title = "MT5 Adaptive-Plan What-If"
        lines = [title]
        lines.append(f"symbol={symbol} direction={str(getattr(signal,'direction','')).upper()} conf={getattr(signal,'confidence',0)}%")
        if wf_plan is not None:
            lines.append(
                f"risk_gate: allow={getattr(wf_plan,'allow', True)} reason={getattr(wf_plan,'reason','-')} canary={getattr(wf_plan,'canary_mode', True)} risk_mult={getattr(wf_plan,'risk_multiplier', 1.0)}"
            )
        for scen in ("conservative", "balanced", "aggressive"):
            p = dict((previews or {}).get(scen, {}) or {})
            ex = dict(p.get("execution", {}) or {})
            ad = dict(p.get("adaptive", {}) or {})
            md = dict(p.get("margin", {}) or {})
            mk = dict(p.get("market", {}) or {})
            fac = dict(ad.get("factors") or {})
            if not bool(p.get("ok")):
                lines.append(f"[{scen.upper()}] failed: {p.get('reason') or p.get('error') or '-'}")
                continue
            lines.append(
                f"[{scen.upper()}] rr={ex.get('risk_reward')} vol={ex.get('fitted_volume')} sl={ex.get('stop_loss')} tp2={ex.get('take_profit_2')} margin={md.get('required')} ({md.get('fit_reason')})"
            )
            lines.append(
                f"  adaptive: reason={ad.get('reason')} sizex{ad.get('size_multiplier')} SLx{ad.get('stop_scale')} samples={fac.get('samples')} spread={mk.get('spread_pct')}% atr={fac.get('atr_pct')}%"
            )
        lines.append("Preview only. No order sent.")
        return "\n".join(lines)[:3900]

    def _plan_snapshot_safe(self, user_id: int, is_admin: bool) -> dict:
        try:
            return access_manager.plan_snapshot(int(user_id), is_admin=is_admin)
        except Exception:
            return {"user": {"plan": "trial", "status": "active"}, "features": [], "is_expired": False}

    def _ai_api_allowed(self, user_id: int, is_admin: bool) -> bool:
        # Policy: API-consuming AI features (intent parser/research fallback) are paid-only.
        if is_admin:
            return True
        snap = self._plan_snapshot_safe(user_id, is_admin=False)
        if bool(snap.get("is_expired", False)):
            return False
        plan = str((snap.get("user") or {}).get("plan", "trial")).lower()
        return plan in {"a", "b", "c"}

    def _remember_mt5_context(self, chat_id: int, snap: dict, requested_symbol: str = "") -> None:
        try:
            cid = int(chat_id)
        except Exception:
            return
        if not isinstance(snap, dict):
            return
        if not snap.get("connected"):
            return
        positions = list(snap.get("positions", []) or [])
        orders = list(snap.get("orders", []) or [])
        preferred = None
        if positions:
            preferred = dict(positions[0])
            kind = "position"
        elif orders:
            preferred = dict(orders[0])
            kind = "order"
        else:
            kind = "empty"
        symbol = str(requested_symbol or snap.get("resolved_symbol") or (preferred or {}).get("symbol") or "").upper()
        if not symbol and len(positions) == 1:
            symbol = str(positions[0].get("symbol") or "").upper()
        if not symbol and len(orders) == 1:
            symbol = str(orders[0].get("symbol") or "").upper()
        self._chat_mt5_context[cid] = {
            "ts": time.time(),
            "symbol": symbol,
            "requested_symbol": str(requested_symbol or "").upper(),
            "resolved_symbol": str(snap.get("resolved_symbol") or "").upper(),
            "positions_count": len(positions),
            "orders_count": len(orders),
            "kind": kind,
            "sample": preferred or {},
            "ambiguous": (len(positions) + len(orders)) > 1 and not requested_symbol,
        }

    def _get_recent_mt5_context(self, chat_id: int, max_age_sec: int = 1800) -> Optional[dict]:
        try:
            ctx = self._chat_mt5_context.get(int(chat_id))
        except Exception:
            return None
        if not ctx:
            return None
        ts = float(ctx.get("ts", 0.0) or 0.0)
        if ts <= 0:
            return None
        if (time.time() - ts) > max(60, int(max_age_sec)):
            return None
        return ctx

    @staticmethod
    def _is_trade_management_followup(q: str) -> bool:
        text = str(q or "").lower()
        manage_terms = (
            "monitor", "manage", "take over", "takeover", "trailing", "tp", "sl", "stop loss",
            "take profit", "exit plan", "manage this trade", "this trade", "this position",
            "ช่วย monitor", "ช่วยดู", "จัดการเทรด", "จัดการออเดอร์", "เทรดนี้", "โพสิชั่นนี้",
            "tp หรือ sl", "tp/sl", "ตั้ง tp", "ตั้ง sl", "เลื่อน sl", "take over to manage",
            "überwachen", "verwalten", "tp/sl", "stop loss", "take profit", "diese position",
        )
        return any(k in text for k in manage_terms)

    def _handle_mt5_trade_followup(self, chat_id: int, user_id: int, text: str, is_admin: bool, lang: str) -> bool:
        q = str(text or "").lower()
        if not self._is_trade_management_followup(q):
            return False

        symbol_hint, _ = self._extract_symbol_and_side_hint(text, mt5_hint=True)
        ctx = self._get_recent_mt5_context(chat_id)
        symbol = str(symbol_hint or (ctx or {}).get("symbol") or "").upper()
        if not symbol:
            self._send_text_localized(chat_id, "mt5_followup_no_context", lang=lang)
            return True

        self._send_text_localized(chat_id, "mt5_followup_checking", lang=lang, symbol=symbol)
        try:
            from execution.mt5_executor import mt5_executor
            snap = mt5_executor.open_positions_snapshot(signal_symbol=symbol, limit=5)
            self._remember_mt5_context(chat_id, snap, requested_symbol=symbol)
        except Exception as e:
            self._send_text_localized(chat_id, "mt5_query_error", lang=lang, err=str(e)[:180])
            return True

        if not snap.get("connected"):
            self._send_text(chat_id, self._format_mt5_symbol_snapshot(snap, symbol, lang))
            return True

        positions = list(snap.get("positions", []) or [])
        orders = list(snap.get("orders", []) or [])
        if not positions and not orders:
            self._send_text_localized(chat_id, "mt5_followup_no_open_trade", lang=lang, symbol=symbol)
            return True

        lines = [self._tr(lang, "mt5_followup_header", symbol=symbol)]
        if positions:
            p = positions[0]
            side = str(p.get("type", "")).upper()
            entry = p.get("price_open")
            now_p = p.get("price_current")
            pnl = p.get("profit")
            sl = p.get("sl")
            tp = p.get("tp")
            vol = p.get("volume")
            ticket = p.get("ticket")
            lines.append(
                f"{symbol} {side} vol={vol} ticket={ticket} pnl={pnl}"
            )
            lines.append(f"entry={entry} now={now_p} sl={sl} tp={tp}")
            try:
                entry_f = float(entry or 0)
                now_f = float(now_p or 0)
                sl_f = float(sl or 0)
                tp_f = float(tp or 0)
                if entry_f and now_f:
                    move = now_f - entry_f
                    if side == "SELL":
                        move = -move
                    lines.append(f"move_from_entry={move:.5f}")
                if not sl_f or sl_f == 0:
                    lines.append("warning=No SL set" if lang == "en" else ("คำเตือน=ยังไม่ได้ตั้ง SL" if lang == "th" else "Warnung=Kein SL gesetzt"))
                if not tp_f or tp_f == 0:
                    lines.append("warning=No TP set" if lang == "en" else ("คำเตือน=ยังไม่ได้ตั้ง TP" if lang == "th" else "Warnung=Kein TP gesetzt"))
            except Exception:
                pass
        else:
            o = orders[0]
            lines.append(
                f"pending {o.get('symbol')} {str(o.get('type','')).upper()} vol={o.get('volume')} "
                f"price={o.get('price_open')} sl={o.get('sl')} tp={o.get('tp')}"
            )

        lines.append(self._tr(lang, "mt5_followup_auto_takeover_unavailable"))
        lines.append(self._tr(lang, "mt5_followup_next_step", symbol=symbol))
        self._send_text(chat_id, "\n".join(lines)[:3900])
        return True

    def _format_plan_text(self, user_id: int, is_admin: bool, lang: str = "en") -> str:
        snap = access_manager.plan_snapshot(user_id, is_admin=is_admin)
        user = snap.get("user", {}) or {}
        plan = str(user.get("plan", "trial")).upper()
        status = str(user.get("status", "active")).upper()
        expires = user.get("expires_at") or "-"
        if (lang or "en").lower() == "th":
            if is_admin:
                return (
                    "แพ็กเกจของคุณ\n"
                    f"แพ็กเกจ: {plan}\n"
                    f"สถานะ: {status}\n"
                    "โควตา: ไม่จำกัด (admin)\n"
                    f"หมดอายุ: {expires}"
                )
            used = int(snap.get("used_today", 0) or 0)
            remaining = snap.get("remaining_today")
            limit = user.get("daily_cmd_limit")
            quota = "ไม่จำกัด" if limit is None else f"ใช้ {used}/{int(limit)} เหลือ {int(remaining or 0)} วันนี้"
            return (
                "แพ็กเกจของคุณ\n"
                f"แพ็กเกจ: {plan}\n"
                f"สถานะ: {status}\n"
                f"โควตา: {quota}\n"
                f"หมดอายุ: {expires}\n"
                "ใช้ /upgrade เพื่อดูแพ็กเกจแบบชำระเงิน"
            )
        if (lang or "en").lower() == "de":
            if is_admin:
                return (
                    "Dein Plan\n"
                    f"Plan: {plan}\n"
                    f"Status: {status}\n"
                    "Quota: Unbegrenzt (Admin)\n"
                    f"Läuft ab: {expires}"
                )
            used = int(snap.get("used_today", 0) or 0)
            remaining = snap.get("remaining_today")
            limit = user.get("daily_cmd_limit")
            quota = "Unbegrenzt" if limit is None else f"{used}/{int(limit)} genutzt, {int(remaining or 0)} heute übrig"
            return (
                "Dein Plan\n"
                f"Plan: {plan}\n"
                f"Status: {status}\n"
                f"Quota: {quota}\n"
                f"Läuft ab: {expires}\n"
                "Nutze /upgrade für kostenpflichtige Pläne."
            )
        if is_admin:
            return (
                "Your Plan\n"
                f"Plan: {plan}\n"
                f"Status: {status}\n"
                "Quota: Unlimited (admin)\n"
                f"Expires: {expires}"
            )
        used = int(snap.get("used_today", 0) or 0)
        remaining = snap.get("remaining_today")
        limit = user.get("daily_cmd_limit")
        if limit is None:
            quota = "Unlimited"
        else:
            quota = f"{used}/{int(limit)} used, {int(remaining or 0)} left today"
        return (
            "Your Plan\n"
            f"Plan: {plan}\n"
            f"Status: {status}\n"
            f"Quota: {quota}\n"
            f"Expires: {expires}\n"
            "Use /upgrade to view paid plans."
        )

    def _access_denied_text(self, reason: str, user_id: int, command: str, lang: str = "en") -> str:
        snap = access_manager.plan_snapshot(user_id, is_admin=False)
        user = snap.get("user", {}) or {}
        plan = str(user.get("plan", "trial")).upper()
        if (lang or "en").lower() == "th":
            if reason == "expired":
                return f"ใช้ /{command} ไม่ได้: แพ็กเกจหมดอายุแล้ว\nแพ็กเกจปัจจุบัน: {plan}\nใช้ /upgrade เพื่อต่ออายุ"
            if reason == "feature_locked":
                return f"ใช้ /{command} ไม่ได้: ฟีเจอร์นี้ไม่มีในแพ็กเกจ {plan}\nใช้ /upgrade เพื่ออัปเกรด"
            if reason == "daily_limit_reached":
                limit = user.get('daily_cmd_limit')
                return f"ใช้ครบโควตาแล้ว ({limit} คำสั่ง/วัน) สำหรับแพ็กเกจ {plan}\nลองใหม่พรุ่งนี้ (UTC) หรือใช้ /upgrade"
            if reason == "admin_only":
                return f"/{command} ใช้ได้เฉพาะแอดมิน"
            return f"ไม่อนุญาตให้ใช้ /{command}"
        if (lang or "en").lower() == "de":
            if reason == "expired":
                return f"Zugriff auf /{command} verweigert: Abo abgelaufen.\nAktueller Plan: {plan}\nMit /upgrade verlängern."
            if reason == "feature_locked":
                return f"Zugriff auf /{command} verweigert: Funktion nicht in Plan {plan} enthalten.\nMit /upgrade upgraden."
            if reason == "daily_limit_reached":
                limit = user.get('daily_cmd_limit')
                return f"Tageslimit erreicht ({limit} Befehle/Tag) für Plan {plan}.\nMorgen UTC erneut versuchen oder /upgrade nutzen."
            if reason == "admin_only":
                return f"/{command} ist nur für Admins."
            return f"Zugriff auf /{command} verweigert."
        if reason == "expired":
            return (
                f"Access denied for /{command}: subscription expired.\n"
                f"Current plan: {plan}\n"
                "Use /upgrade to renew."
            )
        if reason == "feature_locked":
            return (
                f"Access denied for /{command}: feature not included in {plan}.\n"
                "Use /upgrade to move to a higher plan."
            )
        if reason == "daily_limit_reached":
            limit = user.get("daily_cmd_limit")
            return (
                f"Daily limit reached ({limit} commands/day) for plan {plan}.\n"
                "Try again tomorrow UTC or use /upgrade."
            )
        if reason == "admin_only":
            return f"/{command} is admin-only."
        return f"Access denied for /{command}."

    def _resolve_local_intent(self, text: str, lang: str = "en") -> Optional[dict]:
        """
        Local (no external API) natural-language intent resolver.
        """
        msg = str(text or "").strip()
        if not msg:
            return None

        learned = self._lookup_learned_intent(msg)
        if learned:
            cmd, args = learned
            return {"mode": "run", "command": cmd, "args": args, "source": "memory"}

        q = self._normalize_intent_text(msg)
        q_pad = f" {q} "

        def has_any(*parts: str) -> bool:
            return any(p and p in q for p in parts)

        # ----- Scalping operation intents -----
        scalpingish = has_any(
            "scalp", "scalping", "สแกลป์", "สแคลป์", "สกัลป์", "สกาล์ป", "สแคลป", "สเกลป์",
        )
        if scalpingish:
            if has_any("status", "state", "mode", "สถานะ", "เปิดอยู่ไหม", "ทำงานไหม"):
                return {"mode": "run", "command": "scalping_status", "args": "", "source": "heuristic"}

            inferred_symbols = self._parse_scalping_symbols(msg)
            symbol_args = " ".join(inferred_symbols).strip()

            if has_any("on", "enable", "start", "เปิด", "ใช้งาน", "activate"):
                return {"mode": "run", "command": "scalping_on", "args": symbol_args, "source": "heuristic"}

            if has_any("off", "disable", "stop", "pause", "ปิด", "หยุด", "ยกเลิก"):
                return {"mode": "run", "command": "scalping_off", "args": "", "source": "heuristic"}

            if has_any("logic", "algorithm", "strategy", "entry", "tp", "sl", "วิเคราะห์", "ตรรกะ", "กลยุทธ์"):
                sym = self._infer_scalping_symbol_from_text(msg, default="BTCUSD")
                return {"mode": "run", "command": "scalping_logic", "args": sym, "source": "heuristic"}

            if has_any("scan", "run", "now", "ทันที", "ตอนนี้", "ลงมือ", "ทำเลย"):
                sym = self._infer_scalping_symbol_from_text(msg)
                return {"mode": "run", "command": "scalping_scan", "args": sym, "source": "heuristic"}

            return {"mode": "run", "command": "scalping_status", "args": "", "source": "heuristic"}

        # ----- Signal filter intents -----
        filterish = has_any(
            "show only", "only show", "show add", "show all", "signal filter", "filter signal",
            "กรองสัญญาณ", "ตัวกรอง", "แสดงแค่", "แสดงเฉพาะ", "ส่งเฉพาะ", "เอาแค่", "ขอแค่",
            "เฉพาะ", "filter",
        )
        if filterish:
            if has_any(
                "show all", "all signals", "clear filter", "reset filter",
                "แสดงทุกสัญญาณ", "แสดงทั้งหมด", "ล้างตัวกรอง", "ยกเลิกตัวกรอง",
            ):
                return {"mode": "run", "command": "show_clear", "args": "", "source": "heuristic"}

            add_mode = has_any("show add", "add symbol", "add filter", "เพิ่มสัญญาณ", "เพิ่มตัวกรอง", "เพิ่มคู่", "เพิ่มเหรียญ")
            status_mode = has_any("status", "สถานะ", "ตอนนี้", "ปัจจุบัน", "ตอนนี้เหลือ") and not (
                has_any("only", "แค่", "เฉพาะ", "add", "เพิ่ม")
            )
            if status_mode:
                return {"mode": "run", "command": "signal_filter", "args": "status", "source": "heuristic"}

            symbols = self._extract_signal_filter_symbols_from_text(msg)
            if symbols:
                cmd = "show_add" if add_mode else "show_only"
                return {"mode": "run", "command": cmd, "args": " ".join(symbols), "source": "heuristic"}
            return {"mode": "missing", "kind": "signal_filter_symbols"}

        # ----- Signal monitor snapshot intents -----
        monitorish = has_any(
            "signal monitor", "monitor signal", "monitor dashboard",
            "ติดตามสัญญาณ", "สถานะสัญญาณ", "สรุปสัญญาณ",
        ) or bool(
            re.search(
                r"(?i)\b(gold|xau|xauusd|eth|ethusd|ethusdt|btc|btcusd|btcusdt)\s+monitor\b",
                msg,
            )
        )
        if monitorish:
            args = re.sub(
                r"(?i)(signal\s*monitor|monitor\s*signal|monitor\s*dashboard|ติดตามสัญญาณ|สถานะสัญญาณ|สรุปสัญญาณ)",
                " ",
                msg,
            ).strip()
            if not args:
                args = msg.strip()
            return {"mode": "run", "command": "signal_monitor", "args": args, "source": "heuristic"}

        # ----- Signal dashboard intents -----
        dashboardish = has_any(
            "signal dashboard", "signaldashboard", "dashboard signal",
            "แดชบอร์ดสัญญาณ", "สรุปสัญญาณ", "dashboard สัญญาณ",
        )
        if dashboardish:
            args = re.sub(
                r"(?i)(signal\s*dashboard|dashboard\s*signal|signaldashboard|แดชบอร์ดสัญญาณ|สรุปสัญญาณ)",
                " ",
                msg,
            )
            return {"mode": "run", "command": "signal_dashboard", "args": args.strip(), "source": "heuristic"}

        scanish = has_any(
            "scan", "search", "find", "analy", "analyse", "analyze", "monitor",
            "สแกน", "หา", "ค้นหา", "วิเคราะห์", "เช็ค", "ดู",
        )
        logicish = has_any(
            "logic", "algorithm", "strategy", "entry", "tp", "sl",
            "วิเคราะห์", "ตรรกะ", "กลยุทธ์", "เข้าออก", "จุดเข้า", "จุดออก",
        )
        if logicish:
            sym = self._infer_scalping_symbol_from_text(msg)
            if sym:
                return {"mode": "run", "command": "scalping_logic", "args": sym, "source": "heuristic"}
        if not scanish:
            return None

        if has_any("scan all", "all markets", "ทุกตลาด", "ทั้งหมด"):
            return {"mode": "run", "command": "scan_all", "args": "", "source": "heuristic"}

        vi_cmd = self._parse_vi_scan_intent(q)
        if vi_cmd:
            return {"mode": "run", "command": vi_cmd, "args": "", "source": "heuristic"}

        if self._contains_gold_token(q):
            stockish = (
                self._contains_vi_scan_token(q)
                or self._contains_us_market_token(q)
                or self._contains_thai_market_token(q)
                or has_any("stock", "stocks", "หุ้น", "market", "markets", "ตลาด")
            )
            if stockish:
                return {"mode": "confirm", "command": "scan_gold", "args": "", "source": "heuristic"}
            return {"mode": "run", "command": "scan_gold", "args": "", "source": "heuristic"}

        if has_any("crypto", "coin", "coins", "คริปโต", "เหรียญ"):
            return {"mode": "run", "command": "scan_crypto", "args": "", "source": "heuristic"}

        if (
            re.search(r"(^|[^a-z])fx([^a-z]|$)", q)
            or has_any("forex", "ฟอเร็กซ์", "devisen", "ค่าเงิน")
        ):
            return {"mode": "run", "command": "scan_fx", "args": "", "source": "heuristic"}

        stockish = has_any("stock", "stocks", "หุ้น", "market", "markets", "ตลาด")
        if stockish:
            thai = self._contains_thai_market_token(q) or (" th stock " in q_pad)
            us = self._contains_us_market_token(q)
            if thai and us:
                return {"mode": "confirm", "command": "scan_thai", "args": "", "source": "heuristic"}
            if thai:
                return {"mode": "run", "command": "scan_thai", "args": "", "source": "heuristic"}
            if us:
                return {"mode": "run", "command": "scan_us_open", "args": "", "source": "heuristic"}
            return {"mode": "confirm", "command": "scan_stocks", "args": "", "source": "heuristic"}

        return None

    def _try_handle_pending_intent_confirm(self, chat_id: int, user_id: int, text: str, is_admin: bool, lang: str) -> bool:
        rec = self._pending_intent_confirm(chat_id)
        if not rec:
            return False

        cmd = str(rec.get("command") or "").strip().lower()
        args = str(rec.get("args") or "").strip()
        source_text = str(rec.get("source_text") or "").strip()

        answer = self._parse_confirmation_answer(text)
        if answer is True:
            self._clear_pending_intent_confirm(chat_id)
            self._handle_admin_command(chat_id, user_id, cmd, args, is_admin, lang=lang)
            if source_text:
                self._remember_intent_phrase(source_text, cmd, args, source="confirm_yes")
            self._record_intent_event(chat_id, user_id, source_text or text, "confirmed_run", cmd, args, source="confirm_yes")
            return True
        if answer is False:
            self._clear_pending_intent_confirm(chat_id)
            self._record_intent_event(chat_id, user_id, source_text or text, "confirm_rejected", cmd, args, source="confirm_no")
            self._send_text(chat_id, self._intent_rephrase_prompt(lang=lang))
            return True

        # User may answer with a corrected request.
        nxt = self._resolve_local_intent(text, lang=lang)
        if nxt:
            mode = str(nxt.get("mode") or "")
            if mode == "run":
                ncmd = str(nxt.get("command") or "").strip().lower()
                nargs = str(nxt.get("args") or "").strip()
                self._clear_pending_intent_confirm(chat_id)
                self._handle_admin_command(chat_id, user_id, ncmd, nargs, is_admin, lang=lang)
                self._remember_intent_phrase(text, ncmd, nargs, source="confirm_override")
                self._record_intent_event(chat_id, user_id, text, "confirm_override_run", ncmd, nargs, source=str(nxt.get("source") or "heuristic"))
                return True
            if mode == "confirm":
                ncmd = str(nxt.get("command") or "").strip().lower()
                nargs = str(nxt.get("args") or "").strip()
                self._set_pending_intent_confirm(chat_id, ncmd, nargs, source_text=text)
                self._record_intent_event(chat_id, user_id, text, "confirm_reask", ncmd, nargs, source=str(nxt.get("source") or "heuristic"))
                self._send_text(chat_id, self._intent_confirm_prompt(ncmd, nargs, lang=lang))
                return True
            if mode == "missing" and str(nxt.get("kind") or "") == "signal_filter_symbols":
                self._send_text(chat_id, self._intent_missing_filter_symbols_prompt(lang=lang))
                self._record_intent_event(chat_id, user_id, text, "missing_symbols", cmd, args, source="heuristic")
                return True

        self._send_text(chat_id, self._intent_confirm_prompt(cmd, args, lang=lang))
        return True

    def _handle_natural_language(self, chat_id: int, user_id: int, text: str, is_admin: bool, lang: str = "en") -> None:
        """Intent-style natural language command routing."""
        msg = (text or "").strip()
        if not msg:
            return
        q = self._normalize_intent_text(msg)

        if q in {"hi", "hello", "hey", "yo", "สวัสดี", "หวัดดี"}:
            self._send_text_localized(chat_id, "hello", lang=lang)
            return

        if any(k in q for k in ("help", "what can you do", "commands", "คำสั่ง", "ช่วยหน่อย", "ทำอะไรได้บ้าง")):
            self._send_text(chat_id, self._help_text(lang=lang))
            return

        local_intent = self._resolve_local_intent(msg, lang=lang)
        if local_intent:
            mode = str(local_intent.get("mode") or "")
            if mode == "run":
                cmd = str(local_intent.get("command") or "").strip().lower()
                args = str(local_intent.get("args") or "").strip()
                self._handle_admin_command(chat_id, user_id, cmd, args, is_admin, lang=lang)
                self._remember_intent_phrase(msg, cmd, args, source=str(local_intent.get("source") or "heuristic"))
                self._record_intent_event(
                    chat_id,
                    user_id,
                    msg,
                    "run",
                    cmd,
                    args,
                    source=str(local_intent.get("source") or "heuristic"),
                )
                return
            if mode == "confirm":
                cmd = str(local_intent.get("command") or "").strip().lower()
                args = str(local_intent.get("args") or "").strip()
                self._set_pending_intent_confirm(chat_id, cmd, args, source_text=msg)
                self._record_intent_event(
                    chat_id,
                    user_id,
                    msg,
                    "confirm_requested",
                    cmd,
                    args,
                    source=str(local_intent.get("source") or "heuristic"),
                )
                self._send_text(chat_id, self._intent_confirm_prompt(cmd, args, lang=lang))
                return
            if mode == "missing" and str(local_intent.get("kind") or "") == "signal_filter_symbols":
                self._record_intent_event(chat_id, user_id, msg, "missing_symbols", source="heuristic")
                self._send_text(chat_id, self._intent_missing_filter_symbols_prompt(lang=lang))
                return

        if any(k in q for k in ("show only", "signal filter", "filter signal", "show signal", "กรองสัญญาณ", "เลือกสัญญาณ", "show only")):
            tokens = self._parse_signal_filter_symbols(msg)
            if tokens:
                self._handle_admin_command(chat_id, user_id, "show_only", " ".join(tokens), is_admin, lang=lang)
            else:
                self._handle_admin_command(chat_id, user_id, "signal_filter", "status", is_admin, lang=lang)
            return

        # Signal explanation intent (e.g. "why AVGO short signal?")
        if any(k in q for k in ("why", "ทำไม", "เหตุผล", "warum", "por qué", "porque")) and any(
            k in q for k in ("signal", "setup", "trade", "short", "long", "สัญญาณ", "ชอร์ต", "ลอง")
        ):
            self._send_text_localized(chat_id, "checking_signal_reason", lang=lang)
            t = threading.Thread(
                target=self._run_signal_explain_reply,
                args=(chat_id, msg, lang),
                daemon=True,
                name="DexterSignalExplain",
            )
            t.start()
            return

        symbol_hint, _ = self._extract_symbol_and_side_hint(msg, mt5_hint=True)
        # Avoid false-positive "NOW" ticker when user means time adverb (e.g. "scan th stock now").
        if (
            str(symbol_hint or "").upper() == "NOW"
            and self._is_broad_market_scan_phrase(q)
            and (
                self._contains_us_market_token(q)
                or self._contains_thai_market_token(q)
                or any(k in q for k in ("stock", "stocks", "market", "markets", "หุ้น", "ตลาด"))
            )
        ):
            symbol_hint = None

        if ("scan" in q or "สแกน" in q) and symbol_hint:
            sym_up = str(symbol_hint or "").upper()
            self._send_text_localized(chat_id, "running_symbol_scan", lang=lang, symbol=sym_up)
            t = threading.Thread(
                target=self._run_signal_explain_reply,
                args=(chat_id, msg, lang),
                daemon=True,
                name="DexterSymbolScan",
            )
            t.start()
            return

        if symbol_hint and any(k in q for k in (
            "order", "orders", "position", "positions", "trade open", "open trade",
            "open order", "ออเดอร์", "คำสั่ง", "โพสิชั่น", "position", "offen", "auftrag",
            "ถืออยู่", "ค้างอยู่",
        )):
            self._handle_admin_command(chat_id, user_id, "mt5_status", symbol_hint, is_admin, lang=lang)
            return

        run_m = re.search(r"(20\d{12}-\d{1,8}|r\d{1,8})", q, flags=re.IGNORECASE)
        if run_m and any(k in q for k in ("run", "trace", "ย้อน", "รัน", "signal id", "run id", "run_id")):
            self._handle_admin_command(chat_id, user_id, "run", str(run_m.group(1)).upper(), is_admin, lang=lang)
            return

        # MT5-specific intents should be evaluated before generic "status".
        if any(k in q for k in ("mt5", "metatrader", "bridge status")):
            if any(k in q for k in ("affordable", "can trade", "tradable now", "ทุนนี้เทรดได้", "คู่ไหนเทรดได้", "leistbar", "handelbar")):
                arg = ""
                if any(k in q for k in ("crypto", "คริปโต", "krypto")):
                    arg = "crypto"
                elif any(k in q for k in ("fx", "forex", "ฟอเร็กซ์", "devisen")):
                    arg = "fx"
                elif any(k in q for k in ("metal", "gold", "ทอง", "metall")):
                    arg = "metal"
                elif any(k in q for k in ("index", "indices", "ดัชนี")):
                    arg = "index"
                self._handle_admin_command(chat_id, user_id, "mt5_affordable", arg, is_admin, lang=lang)
                return
            if any(k in q for k in ("autopilot", "auto pilot", "risk governor", "ออโต้ไพลอต", "ออโต้ pilot")):
                self._handle_admin_command(chat_id, user_id, "mt5_autopilot", "", is_admin, lang=lang)
                return
            if any(k in q for k in ("walkforward", "walk forward", "canary", "วอล์กฟอร์เวิร์ด", "แคนารี")):
                self._handle_admin_command(chat_id, user_id, "mt5_walkforward", "", is_admin, lang=lang)
                return
            if any(k in q for k in ("position manager", "manage position", "manage trade now", "trail stop", "break even", "position manage", "จัดการโพสิชั่น", "จัดการ position")):
                self._handle_admin_command(chat_id, user_id, "mt5_manage", "", is_admin, lang=lang)
                return
            if any(k in q for k in ("pm learning", "position manager learning", "pm report", "mt5 pm learning", "pm effectiveness", "ผล pm", "ประสิทธิภาพ pm")):
                self._handle_admin_command(chat_id, user_id, "mt5_pm_learning", "", is_admin, lang=lang)
                return
            if any(k in q for k in ("policy", "risk limit", "canary force", "ตั้งค่า mt5", "นโยบาย mt5")):
                self._handle_admin_command(chat_id, user_id, "mt5_policy", "", is_admin, lang=lang)
                return
            if any(k in q for k in ("mt5 plan", "adaptive explain", "trade plan", "แผน mt5", "อธิบายแผนเข้า", "rr/sl/tp ทำไม")):
                self._handle_admin_command(chat_id, user_id, "mt5_plan", msg, is_admin, lang=lang)
                return
            if any(k in q for k in (
                "history", "closed", "close trade", "trade history", "deal history",
                "tp or sl", "tp/sl", "ปิดออเดอร์", "ประวัติ", "ย้อนหลัง", "ปิดไป", "tp หรือ sl",
                "historie", "geschlossen", "verlauf",
            )):
                hours = self._parse_mt5_history_lookback_hours(msg)
                ctx = self._get_recent_mt5_context(chat_id)
                hist_symbol = str(symbol_hint or (ctx or {}).get("symbol") or "").upper()
                if not hist_symbol:
                    self._set_pending_slot(chat_id, "mt5_history_symbol", {"hours": hours})
                    self._send_text_localized(chat_id, "mt5_history_need_symbol", lang=lang, hours=hours)
                    return
                hist_args = " ".join(x for x in [hist_symbol, f"{hours}h"] if x).strip()
                self._handle_admin_command(chat_id, user_id, "mt5_history", hist_args, is_admin, lang=lang)
                return
            if any(k in q for k in ("backtest", "ย้อนหลัง", "ผลทดสอบ", "ประสิทธิภาพ")):
                self._handle_admin_command(chat_id, user_id, "mt5_backtest", "", is_admin, lang=lang)
                return
            if self._handle_mt5_trade_followup(chat_id, user_id, msg, is_admin, lang):
                return
            self._handle_admin_command(chat_id, user_id, "mt5_status", "", is_admin, lang=lang)
            return

        if any(k in q for k in ("history", "closed trade", "tp or sl", "tp/sl", "ประวัติ", "ย้อนหลัง", "tp หรือ sl")):
            ctx = self._get_recent_mt5_context(chat_id)
            if ctx and (ctx.get("symbol") or ctx.get("requested_symbol")):
                hours = self._parse_mt5_history_lookback_hours(msg)
                hist_symbol = str((ctx.get("symbol") or ctx.get("requested_symbol") or "")).upper()
                hist_args = " ".join(x for x in [hist_symbol, f"{hours}h"] if x).strip()
                self._handle_admin_command(chat_id, user_id, "mt5_history", hist_args, is_admin, lang=lang)
                return
            # Slot filling: ask only for missing symbol when user clearly asks for MT5 closed-trade outcome.
            if any(k in q for k in ("mt5", "metatrader", "trade", "position", "เทรด", "โพสิชั่น")):
                hours = self._parse_mt5_history_lookback_hours(msg)
                self._set_pending_slot(chat_id, "mt5_history_symbol", {"hours": hours})
                self._send_text_localized(chat_id, "mt5_history_need_symbol", lang=lang, hours=hours)
                return

        if self._handle_mt5_trade_followup(chat_id, user_id, msg, is_admin, lang):
            return

        if any(k in q for k in ("status", "health", "alive", "working", "สถานะ", "ยังทำงานไหม", "ออนไลน์")):
            self._handle_admin_command(chat_id, user_id, "status", "", is_admin, lang=lang)
            return

        if self._is_ambiguous_gold_stock_intent(q):
            self._set_pending_intent_confirm(chat_id, "scan_gold", "", source_text=msg)
            self._record_intent_event(chat_id, user_id, msg, "confirm_requested", "scan_gold", "", source="heuristic")
            self._send_text(chat_id, self._intent_confirm_prompt("scan_gold", "", lang=lang))
            return

        gold_actionish = any(
            k in q
            for k in (
                "scan",
                "สแกน",
                "search",
                "find",
                "analy",
                "analyse",
                "analyze",
                "วิเคราะห์",
                "ค้นหา",
                "หา",
            )
        )
        if self._contains_gold_token(q) and gold_actionish:
            self._handle_admin_command(chat_id, user_id, "scan_gold", "", is_admin, lang=lang)
            return
        if ("scan" in q or "สแกน" in q) and ("crypto" in q or "คริปโต" in q):
            self._handle_admin_command(chat_id, user_id, "scan_crypto", "", is_admin, lang=lang)
            return
        if ("scan" in q or "สแกน" in q) and (
            re.search(r"(^|[^a-z])fx([^a-z]|$)", q)
            or any(k in q for k in ("forex", "ฟอเร็กซ์", "devisen", "ค่าเงิน"))
        ):
            self._handle_admin_command(chat_id, user_id, "scan_fx", "", is_admin, lang=lang)
            return
        vi_cmd = self._parse_vi_scan_intent(q)
        if vi_cmd:
            self._handle_admin_command(chat_id, user_id, vi_cmd, "", is_admin, lang=lang)
            return
        if ("scan" in q or "สแกน" in q) and self._contains_thai_market_token(q):
            self._handle_admin_command(chat_id, user_id, "scan_thai", "", is_admin, lang=lang)
            return
        if any(k in q for k in ("guard status", "us guard", "us open guard", "circuit breaker", "macro freeze", "สถานะ guard", "สถานะ us open", "หยุด us open เพราะอะไร")):
            self._handle_admin_command(chat_id, user_id, "us_open_guard_status", "", is_admin, lang=lang)
            return
        if (
            "us open" in q
            or "ny open" in q
            or "monitor us" in q
            or "ตลาด us เปิด" in q
            or "ตลาดอเมริกาเปิด" in q
        ):
            self._handle_admin_command(chat_id, user_id, "monitor_us", "", is_admin, lang=lang)
            return
        if ("scan" in q or "สแกน" in q) and self._contains_us_market_token(q):
            self._handle_admin_command(chat_id, user_id, "scan_us_open", "", is_admin, lang=lang)
            return
        if ("scan" in q or "สแกน" in q) and ("stock" in q or "market" in q or "หุ้น" in q):
            self._handle_admin_command(chat_id, user_id, "scan_stocks", "", is_admin, lang=lang)
            return
        if any(k in q for k in ("value", "trending", "vi", "หุ้น vi", "หุ้นคุณค่า", "หุ้นแนวโน้ม")):
            self._handle_admin_command(chat_id, user_id, "scan_vi", "", is_admin, lang=lang)
            return
        if ("scan" in q or "สแกน" in q) and ("all" in q or "ทั้งหมด" in q or "ทุกตลาด" in q):
            self._handle_admin_command(chat_id, user_id, "scan_all", "", is_admin, lang=lang)
            return

        if ("gold" in q or "xau" in q or "ทอง" in q) and any(k in q for k in ("price", "quote", "current", "ราคา")):
            from market.data_fetcher import xauusd_provider
            price = xauusd_provider.get_current_price()
            if price is None:
                self._send_text_localized(chat_id, "xau_price_unavailable", lang=lang)
            else:
                self._send_text_localized(chat_id, "xau_price", lang=lang, price=price)
            return

        if any(k in q for k in ("market hours", "markets open", "which markets", "global market", "ตลาดเปิด", "ตลาดไหนเปิด")):
            self._handle_admin_command(chat_id, user_id, "markets", "", is_admin, lang=lang)
            return
        if any(k in q for k in ("timezone", "time zone", "utc+", "utc-", "gmt+", "gmt-", "bangkok time", "โซนเวลา", "เวลาไทย")):
            self._handle_admin_command(chat_id, user_id, "tz", msg, is_admin, lang=lang)
            return
        if any(k in q for k in ("calendar", "economic", "ข่าวเศรษฐกิจ", "ปฏิทิน")):
            self._handle_admin_command(chat_id, user_id, "calendar", "", is_admin, lang=lang)
            return
        if any(k in q for k in ("macro weights", "adaptive weights", "theme weights", "น้ำหนัก macro", "น้ำหนักข่าว", "ค่าน้ำหนักธีม")):
            self._handle_admin_command(chat_id, user_id, "macro_weights", msg, is_admin, lang=lang)
            return
        if any(k in q for k in ("macro report", "post-news impact", "post news impact", "impact report", "ผลกระทบข่าว", "ข่าวมีผลไหม", "macro impact")):
            self._handle_admin_command(chat_id, user_id, "macro_report", msg, is_admin, lang=lang)
            return
        if any(k in q for k in ("macro", "trump", "tariff", "policy risk", "ภูมิรัฐศาสตร์", "ผลกระทบทรัมป์")):
            self._handle_admin_command(chat_id, user_id, "macro", "", is_admin, lang=lang)
            return
        if "market" in q and "open" in q:
            self._handle_admin_command(chat_id, user_id, "markets", "", is_admin, lang=lang)
            return

        if any(k in q for k in ("gold overview", "xau overview", "overview gold", "overview xau", "ภาพรวมทอง")):
            self._handle_admin_command(chat_id, user_id, "gold_overview", "", is_admin, lang=lang)
            return

        if any(k in q for k in ("my plan", "subscription", "แพ็กเกจ", "แผน", "plan")):
            self._handle_admin_command(chat_id, user_id, "plan", "", is_admin, lang=lang)
            return

        if any(k in q for k in ("upgrade", "price", "pricing", "สมัคร", "ชำระ", "จ่าย")):
            self._handle_admin_command(chat_id, user_id, "upgrade", "", is_admin, lang=lang)
            return

        # Fallback: route to AI chat agent for free-form questions (admin only)
        self._record_intent_event(chat_id, user_id, msg, "unmapped", source="heuristic")
        if is_admin:
            try:
                from openclaw.chat_agent import ask as _ask
                self._send_text(chat_id, "🤔 กำลังวิเคราะห์...")
                answer = _ask(msg)
                self._send_text(chat_id, f"💬 {answer}")
            except Exception as exc:
                logger.debug("[admin_bot] AI chat fallback error: %s", exc)
                self._send_text(chat_id, self._intent_rephrase_prompt(lang=lang))
        else:
            if not self._ai_api_allowed(user_id, is_admin):
                self._send_text_localized(chat_id, "ai_api_locked_trial", lang=lang)
                return
            self._send_text(chat_id, self._intent_rephrase_prompt(lang=lang))

    def _handle_admin_command(self, chat_id: int, user_id: int, command: str, args: str, is_admin: bool, lang: str = "en") -> None:
        from scheduler import scheduler
        from scanners.xauusd import xauusd_scanner
        from scanners.crypto_sniper import crypto_sniper
        from scanners.stock_scanner import stock_scanner
        from market.stock_universe import get_all_stocks
        from market.data_fetcher import session_manager

        if command == "timezone":
            command = "tz"
        if command in {"mt5_hist", "mt5_deals"}:
            command = "mt5_history"
        if command in {"macroreport", "macro_impact", "macroimpact"}:
            command = "macro_report"
        if command in {"macroweights", "macro_weight", "macroweight"}:
            command = "macro_weights"
        if command in {"mt5_adaptive_explain", "mt5_adaptive", "mt5plan"}:
            command = "mt5_plan"
        if command in {"mt5pm_learning", "mt5_pmlearn"}:
            command = "mt5_pm_learning"
        if command in {"mt5affordable", "mt5_affordble"}:
            command = "mt5_affordable"
        if command in {"scalp_status", "scalpstat", "scalping_mode", "scalp_mode"}:
            command = "scalping_status"
        if command in {"scalp_on", "scalping_enable", "enable_scalping"}:
            command = "scalping_on"
        if command in {"scalp_off", "scalping_disable", "disable_scalping"}:
            command = "scalping_off"
        if command in {"scalp_scan", "scan_scalping"}:
            command = "scalping_scan"
        if command in {"scalp_logic", "scalping_algo", "scalping_algorithm"}:
            command = "scalping_logic"
        if command not in self._known_commands():
            suggestion = self._suggest_command(command)
            if suggestion and suggestion != command:
                self._send_text_localized(
                    chat_id,
                    "command_autocorrected",
                    lang=lang,
                    suggested=suggestion,
                    original=command,
                )
                command = suggestion

        decision = access_manager.check_and_consume(user_id, command, is_admin=is_admin)
        if not decision.allowed:
            self._send_text(chat_id, self._access_denied_text(decision.reason, user_id, command, lang=lang))
            return

        if command in ("start", "help"):
            self._send_text(chat_id, self._help_text(lang=lang))
            return

        if command == "plan":
            self._send_text(chat_id, self._format_plan_text(user_id, is_admin=is_admin, lang=lang))
            return

        if command == "scalping_status":
            enabled = bool(getattr(config, "SCALPING_ENABLED", False))
            symbols = sorted(list(config.get_scalping_symbols()))
            if lang == "th":
                self._send_text(
                    chat_id,
                    "Scalping Status\n"
                    f"enabled={enabled}\n"
                    f"symbols={', '.join(symbols) if symbols else '-'}\n"
                    f"entry_tf={getattr(config, 'SCALPING_ENTRY_TF', '5m')} trigger_tf={getattr(config, 'SCALPING_M1_TRIGGER_TF', '1m')}\n"
                    f"scan_interval={int(getattr(config, 'SCALPING_SCAN_INTERVAL_SEC', 300) or 300)}s min_conf={float(getattr(config, 'SCALPING_MIN_CONFIDENCE', 70.0) or 70.0):.1f}\n"
                    f"notify={bool(getattr(config, 'SCALPING_NOTIFY_TELEGRAM', True))} execute_mt5={bool(getattr(config, 'SCALPING_EXECUTE_MT5', True))}",
                )
            elif lang == "de":
                self._send_text(
                    chat_id,
                    "Scalping-Status\n"
                    f"enabled={enabled}\n"
                    f"symbols={', '.join(symbols) if symbols else '-'}\n"
                    f"entry_tf={getattr(config, 'SCALPING_ENTRY_TF', '5m')} trigger_tf={getattr(config, 'SCALPING_M1_TRIGGER_TF', '1m')}\n"
                    f"scan_interval={int(getattr(config, 'SCALPING_SCAN_INTERVAL_SEC', 300) or 300)}s min_conf={float(getattr(config, 'SCALPING_MIN_CONFIDENCE', 70.0) or 70.0):.1f}\n"
                    f"notify={bool(getattr(config, 'SCALPING_NOTIFY_TELEGRAM', True))} execute_mt5={bool(getattr(config, 'SCALPING_EXECUTE_MT5', True))}",
                )
            else:
                self._send_text(
                    chat_id,
                    "Scalping Status\n"
                    f"enabled={enabled}\n"
                    f"symbols={', '.join(symbols) if symbols else '-'}\n"
                    f"entry_tf={getattr(config, 'SCALPING_ENTRY_TF', '5m')} trigger_tf={getattr(config, 'SCALPING_M1_TRIGGER_TF', '1m')}\n"
                    f"scan_interval={int(getattr(config, 'SCALPING_SCAN_INTERVAL_SEC', 300) or 300)}s min_conf={float(getattr(config, 'SCALPING_MIN_CONFIDENCE', 70.0) or 70.0):.1f}\n"
                    f"notify={bool(getattr(config, 'SCALPING_NOTIFY_TELEGRAM', True))} execute_mt5={bool(getattr(config, 'SCALPING_EXECUTE_MT5', True))}",
                )
            return

        if command in {"scalping_on", "scalping_off"}:
            if not bool(is_admin):
                self._send_text(chat_id, self._access_denied_text("admin_only", user_id, command, lang=lang))
                return
            turn_on = command == "scalping_on"
            config.SCALPING_ENABLED = bool(turn_on)
            parsed_symbols = self._parse_scalping_symbols(args)
            if parsed_symbols:
                config.SCALPING_SYMBOLS = ",".join(parsed_symbols)
            symbols = sorted(list(config.get_scalping_symbols()))
            mode_txt = "ON" if turn_on else "OFF"
            if lang == "th":
                self._send_text(
                    chat_id,
                    f"Scalping mode: {mode_txt}\n"
                    f"symbols={', '.join(symbols) if symbols else '-'}\n"
                    "หมายเหตุ: ค่านี้เป็น runtime และจะรีเซ็ตหลังรีสตาร์ต ถ้าไม่บันทึกใน .env.local",
                )
            elif lang == "de":
                self._send_text(
                    chat_id,
                    f"Scalping-Modus: {mode_txt}\n"
                    f"symbols={', '.join(symbols) if symbols else '-'}\n"
                    "Hinweis: Runtime-Override; nach Neustart zurückgesetzt, wenn nicht in .env.local gespeichert.",
                )
            else:
                self._send_text(
                    chat_id,
                    f"Scalping mode: {mode_txt}\n"
                    f"symbols={', '.join(symbols) if symbols else '-'}\n"
                    "Note: this is a runtime override and resets after restart unless persisted in .env.local.",
                )
            return

        if command in {"scalping_scan", "scalping_logic"}:
            from scanners.scalping_scanner import scalping_scanner

            target_symbol = self._infer_scalping_symbol_from_text(args, default="BTCUSD" if command == "scalping_logic" else "")
            if command == "scalping_scan":
                if target_symbol in {"XAUUSD", "ETHUSD", "BTCUSD"}:
                    if target_symbol == "XAUUSD":
                        rows = [scalping_scanner.scan_xauusd(require_enabled=False)]
                    elif target_symbol == "ETHUSD":
                        rows = [scalping_scanner.scan_eth(require_enabled=False)]
                    else:
                        rows = [scalping_scanner.scan_btc(require_enabled=False)]
                else:
                    rpt = scheduler.run_once("scalping")
                    rows = []
                    for item in ((rpt or {}).get("scalping", {}) or {}).get("results", []):
                        rows.append(
                            {
                                "symbol": str(item.get("symbol", "")),
                                "status": str(item.get("status", "")),
                                "reason": str(item.get("reason", "")),
                                "signal_sent": bool(item.get("signal_sent")),
                                "executed_mt5": bool(item.get("executed_mt5")),
                            }
                        )
                    if not rows:
                        rows = [{"symbol": "-", "status": "no_rows", "reason": "scheduler_returned_empty"}]
                lines = ["Scalping Scan"]
                if target_symbol:
                    lines.append(f"target={target_symbol}")
                for row in rows:
                    if isinstance(row, dict):
                        lines.append(
                            f"- {row.get('symbol', '-')} | {row.get('status', '-')} | {row.get('reason', '-')}"
                            + (f" | sent={row.get('signal_sent')} exec={row.get('executed_mt5')}" if "signal_sent" in row else "")
                        )
                    else:
                        lines.append(
                            f"- {getattr(row, 'symbol', '-')} | {getattr(row, 'status', '-')} | {getattr(row, 'reason', '-')}"
                        )
                self._send_text(chat_id, "\n".join(lines))
                return

            # command == scalping_logic
            if target_symbol == "XAUUSD":
                row = scalping_scanner.scan_xauusd(require_enabled=False)
            elif target_symbol == "ETHUSD":
                row = scalping_scanner.scan_eth(require_enabled=False)
            else:
                target_symbol = "BTCUSD"
                row = scalping_scanner.scan_btc(require_enabled=False)
            sig = getattr(row, "signal", None)
            trigger = dict(getattr(row, "trigger", {}) or {})
            lines = [
                f"Scalping Logic ({target_symbol})",
                f"status={getattr(row, 'status', '-')}",
                f"reason={getattr(row, 'reason', '-')}",
            ]
            if sig is not None:
                lines.append(
                    f"signal={str(getattr(sig, 'direction', '')).upper()} conf={float(getattr(sig, 'confidence', 0.0) or 0.0):.1f}%"
                )
                lines.append(
                    f"entry={float(getattr(sig, 'entry', 0.0) or 0.0):.5f} "
                    f"sl={float(getattr(sig, 'stop_loss', 0.0) or 0.0):.5f} "
                    f"tp1={float(getattr(sig, 'take_profit_1', 0.0) or 0.0):.5f} "
                    f"tp2={float(getattr(sig, 'take_profit_2', 0.0) or 0.0):.5f}"
                )
                reasons = list(getattr(sig, "reasons", []) or [])
                if reasons:
                    lines.append("logic=" + "; ".join(str(x) for x in reasons[:3]))
            if trigger:
                lines.append(
                    "m1_trigger="
                    + ", ".join(
                        [
                            f"ok={trigger.get('ok')}",
                            f"reason={trigger.get('reason')}",
                            f"rsi={trigger.get('rsi14')}",
                            f"ema9={trigger.get('ema9')}",
                            f"ema21={trigger.get('ema21')}",
                        ]
                    )
                )
            self._send_text(chat_id, "\n".join(lines))
            return

        if command == "signal_monitor":
            parsed = self._parse_signal_monitor_args(args)
            symbols = [str(x or "").strip().upper() for x in list(parsed.get("symbols") or []) if str(x or "").strip()]
            if not symbols:
                try:
                    user_filter = access_manager.get_user_signal_symbol_filter(user_id)
                except Exception:
                    user_filter = []
                for item in user_filter:
                    sym = self._normalize_dashboard_symbol(str(item or ""))
                    if not sym:
                        continue
                    if sym in symbols:
                        continue
                    symbols.append(sym)
            if not symbols:
                symbols = ["XAUUSD"]

            window_mode = str(parsed.get("window_mode") or "today").strip().lower()
            days = int(parsed.get("days", 1) or 1)

            for symbol in symbols:
                payload = self._build_signal_monitor_payload(symbol=symbol, window_mode=window_mode, days=days)
                self._send_text(chat_id, self._format_signal_monitor_text(payload, lang=lang, chat_id=chat_id))
            return

        if command in {"signal_filter", "show_only", "show_add", "show_clear", "show_all"}:
            raw = str((args or "").strip())
            op = "status"
            payload = raw

            if command == "show_only":
                op = "only"
            elif command == "show_add":
                op = "add"
            elif command in {"show_clear", "show_all"}:
                op = "clear"
            elif raw:
                parts = raw.split(maxsplit=1)
                head = str(parts[0]).strip().lower()
                tail = str(parts[1]).strip() if len(parts) > 1 else ""
                if head in {"status", "show", "list", "get"}:
                    op = "status"
                    payload = ""
                elif head in {"only", "set", "replace"}:
                    op = "only"
                    payload = tail
                elif head in {"add", "append", "+"}:
                    op = "add"
                    payload = tail
                elif head in {"clear", "reset", "all", "*"}:
                    op = "clear"
                    payload = ""
                else:
                    op = "only"
                    payload = raw

            current = access_manager.get_user_signal_symbol_filter(user_id)
            if op == "status":
                if current:
                    if lang == "th":
                        self._send_text(chat_id, "ตัวกรองสัญญาณปัจจุบัน: " + ", ".join(current))
                    elif lang == "de":
                        self._send_text(chat_id, "Aktueller Signal-Filter: " + ", ".join(current))
                    else:
                        self._send_text(chat_id, "Current signal filter: " + ", ".join(current))
                else:
                    if lang == "th":
                        self._send_text(chat_id, "ตอนนี้แสดงทุกสัญญาณ (ยังไม่ได้ตั้ง filter)")
                    elif lang == "de":
                        self._send_text(chat_id, "Aktuell werden alle Signale angezeigt (kein Filter gesetzt).")
                    else:
                        self._send_text(chat_id, "Currently showing all signals (no filter set).")
                return

            if op == "clear":
                access_manager.set_user_signal_symbol_filter(user_id, [])
                if lang == "th":
                    self._send_text(chat_id, "ล้างตัวกรองแล้ว จากนี้จะแสดงทุกสัญญาณ")
                elif lang == "de":
                    self._send_text(chat_id, "Signal-Filter gelöscht. Ab jetzt werden alle Signale angezeigt.")
                else:
                    self._send_text(chat_id, "Signal filter cleared. You will now receive all signals.")
                return

            symbols = self._parse_signal_filter_symbols(payload)
            if not symbols:
                if lang == "th":
                    self._send_text(
                        chat_id,
                        "วิธีใช้:\n"
                        "/show_only gold\n"
                        "/show_only btc eth\n"
                        "/show_add xauusd\n"
                        "/show_clear\n"
                        "/signal_filter status",
                    )
                elif lang == "de":
                    self._send_text(
                        chat_id,
                        "Nutzung:\n"
                        "/show_only gold\n"
                        "/show_only btc eth\n"
                        "/show_add xauusd\n"
                        "/show_clear\n"
                        "/signal_filter status",
                    )
                else:
                    self._send_text(
                        chat_id,
                        "Usage:\n"
                        "/show_only gold\n"
                        "/show_only btc eth\n"
                        "/show_add xauusd\n"
                        "/show_clear\n"
                        "/signal_filter status",
                    )
                return

            if op == "add":
                merged = list(current or [])
                for sym in symbols:
                    if sym not in merged:
                        merged.append(sym)
                saved = access_manager.set_user_signal_symbol_filter(user_id, merged)
            else:
                saved = access_manager.set_user_signal_symbol_filter(user_id, symbols)

            if lang == "th":
                self._send_text(chat_id, "ตั้งค่าตัวกรองสัญญาณแล้ว: " + ", ".join(saved))
            elif lang == "de":
                self._send_text(chat_id, "Signal-Filter gespeichert: " + ", ".join(saved))
            else:
                self._send_text(chat_id, "Signal filter saved: " + ", ".join(saved))
            return

        if command in ("tz", "timezone"):
            raw = str((args or "").strip())
            if not raw:
                saved = None
                try:
                    saved = access_manager.get_user_news_utc_offset(user_id)
                except Exception:
                    saved = None
                current = saved or "UTC"
                if lang == "th":
                    self._send_text(
                        chat_id,
                        "โซนเวลาข่าวของคุณ\n"
                        f"ปัจจุบัน: {current}\n"
                        "ตั้งค่าได้ด้วย /tz +07:00 หรือ /tz bangkok\n"
                        "ล้างค่าใช้ /tz reset",
                    )
                elif lang == "de":
                    self._send_text(
                        chat_id,
                        "Deine News-Zeitzone\n"
                        f"Aktuell: {current}\n"
                        "Setze mit /tz +07:00 oder /tz bangkok\n"
                        "Zurücksetzen mit /tz reset",
                    )
                else:
                    self._send_text(
                        chat_id,
                        "Your news timezone\n"
                        f"Current: {current}\n"
                        "Set with /tz +07:00 or /tz bangkok\n"
                        "Reset with /tz reset",
                    )
                return
            if raw.lower() in {"reset", "clear", "default"}:
                try:
                    access_manager.set_user_news_utc_offset(user_id, None)
                except Exception:
                    pass
                if lang == "th":
                    self._send_text(chat_id, "รีเซ็ตโซนเวลาข่าวแล้ว (กลับไปใช้ UTC)")
                elif lang == "de":
                    self._send_text(chat_id, "News-Zeitzone zurückgesetzt (jetzt UTC)")
                else:
                    self._send_text(chat_id, "News timezone reset (now using UTC)")
                return

            normalized_tz = self._normalize_utc_offset_input(raw)
            if not normalized_tz:
                if lang == "th":
                    self._send_text(chat_id, "รูปแบบไม่ถูกต้อง ใช้ตัวอย่าง: /tz +07:00 หรือ /tz gmt+7 หรือ /tz bangkok")
                elif lang == "de":
                    self._send_text(chat_id, "Ungültiges Format. Beispiel: /tz +07:00 oder /tz gmt+7 oder /tz bangkok")
                else:
                    self._send_text(chat_id, "Invalid format. Example: /tz +07:00 or /tz gmt+7 or /tz bangkok")
                return
            saved = access_manager.set_user_news_utc_offset(user_id, normalized_tz)
            if lang == "th":
                self._send_text(chat_id, f"ตั้งค่าโซนเวลาข่าวเป็น {saved} แล้ว\nจากนี้ /calendar และ /macro จะแสดงเวลาตามโซนนี้")
            elif lang == "de":
                self._send_text(chat_id, f"News-Zeitzone auf {saved} gesetzt.\n/calendar und /macro zeigen jetzt Zeiten in dieser Zone.")
            else:
                self._send_text(chat_id, f"News timezone set to {saved}.\n/calendar and /macro will now show times in this timezone.")
            return

        if command == "upgrade":
            from notifier.billing_checkout import stripe_checkout

            parts = (args or "").split()
            selected = parts[0].strip().lower() if parts else ""
            if selected not in {"a", "b", "c"}:
                self._send_text(
                    chat_id,
                    access_manager.pricing_text()
                    + "\n\nBuy now:\n"
                    "/upgrade a\n"
                    "/upgrade b\n"
                    "/upgrade c"
                )
                return

            res = stripe_checkout.create_checkout_session(user_id=user_id, plan=selected)
            if not res.ok:
                self._send_text(chat_id, f"Checkout create failed: {res.message}\nUse /upgrade to view plans.")
                return

            self._send_text(
                chat_id,
                f"Plan {res.plan.upper()} checkout ready ({res.days} days).\n"
                f"Pay securely here:\n{res.url}\n\n"
                "After payment, your plan is upgraded automatically."
            )
            return

        if command == "status":
            xstats = xauusd_scanner.get_stats()
            cstats = crypto_sniper.get_stats()
            sstats = stock_scanner.get_stats()
            session = session_manager.get_session_info()
            if lang == "th":
                title = "สถานะ Dexter"
                session_lbl = "เซสชัน"
                universe_lbl = "จำนวนหุ้นในจักรวาล"
                x_lbl = "XAU scans/signals"
                c_lbl = "Crypto scans/signals"
                s_lbl = "Stocks scans/signals"
            elif lang == "de":
                title = "Dexter Status"
                session_lbl = "Session"
                universe_lbl = "Aktien-Universum"
                x_lbl = "XAU Scans/Signale"
                c_lbl = "Krypto Scans/Signale"
                s_lbl = "Aktien Scans/Signale"
            else:
                title = "Dexter Status"
                session_lbl = "Session"
                universe_lbl = "Universe stocks"
                x_lbl = "XAU scans/signals"
                c_lbl = "Crypto scans/signals"
                s_lbl = "Stocks scans/signals"
            text = (
                f"{title}\n"
                f"{session_lbl}: {', '.join(session.get('active_sessions', []))}\n"
                f"{x_lbl}: {xstats['total_scans']}/{xstats['signals_generated']}\n"
                f"{c_lbl}: {cstats['total_scans']}/{cstats['total_signals']}\n"
                f"{s_lbl}: {sstats['total_scans']}/{sstats['total_signals']}\n"
                f"{universe_lbl}: {len(get_all_stocks())}"
            )
            self._send_text(chat_id, text)
            return

        if command == "mt5_status":
            from execution.mt5_executor import mt5_executor
            from learning.mt5_autopilot_core import mt5_autopilot_core
            from learning.mt5_orchestrator import mt5_orchestrator
            symbol_arg = str((args or "").strip()).upper()
            if symbol_arg:
                self._send_text_localized(chat_id, "mt5_query_progress", lang=lang, symbol=symbol_arg)
                snap = mt5_executor.open_positions_snapshot(signal_symbol=symbol_arg, limit=10)
                self._remember_mt5_context(chat_id, snap, requested_symbol=symbol_arg)
                self._send_text(chat_id, self._format_mt5_symbol_snapshot(snap, symbol_arg, lang))
                return
            st = mt5_executor.status()
            auto = mt5_autopilot_core.status()
            orch = mt5_orchestrator.status()
            snap = mt5_executor.open_positions_snapshot(limit=5)
            self._remember_mt5_context(chat_id, snap, requested_symbol="")
            auto_risk = dict(auto.get("risk_gate", {}) or {})
            auto_journal = dict(auto.get("journal", {}) or {})
            auto_cal = dict(auto.get("calibration", {}) or {})
            if lang == "th":
                header = "สถานะ MT5 Bridge"
                pos_lbl = "โพสิชั่นเปิด"
                ord_lbl = "คำสั่งรอดำเนินการ"
            elif lang == "de":
                header = "MT5 Bridge Status"
                pos_lbl = "Offene Positionen"
                ord_lbl = "Pending Orders"
            else:
                header = "MT5 Bridge Status"
                pos_lbl = "Open positions"
                ord_lbl = "Pending orders"
            lines = [
                header,
                f"enabled={st.get('enabled')} dry_run={st.get('dry_run')} connected={st.get('connected')}",
                f"micro_mode={st.get('micro_mode')} learner={st.get('micro_learner_enabled')} spread_max_pct={st.get('micro_max_spread_pct')}",
                f"host={st.get('host')}:{st.get('port')} symbols={st.get('symbols')}",
                f"login={st.get('account_login')} server={st.get('account_server')}",
                f"balance={st.get('balance')} equity={st.get('equity')} free_margin={st.get('margin_free')} {st.get('currency') or ''}".strip(),
                f"micro_whitelist: bucket={st.get('micro_balance_bucket')} allow={st.get('micro_whitelist_allowed')} deny={st.get('micro_whitelist_denied')} total={st.get('micro_whitelist_total')}",
                f"autopilot_gate={auto_risk.get('status', '-')} allow={auto_risk.get('allow', True)}",
                f"autopilot_journal: total={auto_journal.get('total', 0)} open_ft={auto_journal.get('open_forward_tests', 0)} labeled7d={auto_cal.get('labeled_7d', 0)}",
                f"error={st.get('error') or '-'}",
            ]
            ep = dict(orch.get("execution_plan_preview", {}) or {})
            if ep:
                lines.append(
                    f"plan: canary={ep.get('canary_mode')} risk_mult={ep.get('risk_multiplier')} reason={ep.get('reason')}"
                )
            if snap.get("connected"):
                positions = list(snap.get("positions", []) or [])
                orders = list(snap.get("orders", []) or [])
                lines.append(f"{pos_lbl}={len(positions)} | {ord_lbl}={len(orders)}")
                for p in positions[:3]:
                    lines.append(
                        f"- {p.get('symbol')} {str(p.get('type','')).upper()} vol={p.get('volume')} "
                        f"open={p.get('price_open')} now={p.get('price_current')} pnl={p.get('profit')}"
                    )
                for o in orders[:2]:
                    lines.append(
                        f"- {o.get('symbol')} {str(o.get('type','')).upper()} vol={o.get('volume')} "
                        f"price={o.get('price_open')}"
                    )
            elif snap.get("error"):
                lines.append(f"snapshot_error={snap.get('error')}")
            self._send_text(chat_id, "\n".join(lines)[:3900])
            return

        if command == "mt5_affordable":
            from execution.mt5_executor import mt5_executor

            parsed = self._parse_mt5_affordable_args(args)
            category = str(parsed.get("category", "all") or "all")
            top_n = int(parsed.get("top", 12) or 12)
            if lang == "th":
                self._send_text(chat_id, f"กำลังเช็คคู่ที่เทรดได้ใน MT5 แบบสด ({category})...")
            elif lang == "de":
                self._send_text(chat_id, f"Prüfe live leistbare MT5-Symbole ({category})...")
            else:
                self._send_text(chat_id, f"Checking live MT5 affordable symbols ({category})...")
            only_ok = bool(parsed.get("only_ok", False))
            snap = mt5_executor.affordable_symbols_snapshot(category=category, limit=top_n, only_ok=only_ok)
            self._send_text(chat_id, self._format_mt5_affordable_snapshot(snap, lang=lang))
            return

        if command == "mt5_exec_reasons":
            from learning.mt5_autopilot_core import mt5_autopilot_core

            raw = str(args or "").strip()
            symbol_arg = ""
            hours = 24
            for tok in raw.split():
                t = str(tok).strip()
                if not t:
                    continue
                m_h = re.fullmatch(r"(\d{1,3})\s*h", t.lower())
                m_d = re.fullmatch(r"(\d{1,3})\s*d", t.lower())
                if m_h:
                    hours = max(1, min(24 * 14, int(m_h.group(1))))
                    continue
                if m_d:
                    hours = max(1, min(24 * 14, int(m_d.group(1)) * 24))
                    continue
                if t.isdigit():
                    hours = max(1, min(24 * 14, int(t)))
                    continue
                if not symbol_arg:
                    symbol_arg = t.upper()
            if lang == "th":
                self._send_text(chat_id, f"กำลังสรุปเหตุผล MT5 execute/skip ย้อนหลัง {hours}h" + (f" ({symbol_arg})" if symbol_arg else "") + "...")
            elif lang == "de":
                self._send_text(chat_id, f"Analysiere MT5 Execute/Skip-Gründe der letzten {hours}h" + (f" ({symbol_arg})" if symbol_arg else "") + "...")
            else:
                self._send_text(chat_id, f"Analyzing MT5 execute/skip reasons for last {hours}h" + (f" ({symbol_arg})" if symbol_arg else "") + "...")
            rep = mt5_autopilot_core.execution_reasons_report(hours=hours, symbol=symbol_arg)
            self._send_text(chat_id, self._format_mt5_exec_reasons_report(rep, lang=lang))
            return

        if command == "run":
            parsed = self._parse_run_trace_args(args)
            if not bool(parsed.get("valid")):
                self._send_text(
                    chat_id,
                    "Run Trace usage:\n"
                    "/run R000123\n"
                    "/run 20260306010101-000123",
                )
                return
            query_key = str(parsed.get("run_tag") or parsed.get("run_id") or parsed.get("raw") or "").strip()
            if lang == "th":
                self._send_text(chat_id, f"กำลัง trace เส้นทาง run={query_key} ...")
            elif lang == "de":
                self._send_text(chat_id, f"Trace wird geladen run={query_key} ...")
            else:
                self._send_text(chat_id, f"Loading run trace for {query_key} ...")
            rpt = self._lookup_run_trace(parsed)
            self._send_text(chat_id, self._format_run_trace_report(rpt, lang=lang))
            return

        if command == "stock_mt5_filter":
            from scanners.stock_scanner import stock_scanner

            raw = str(args or "").strip().lower()
            op = (raw.split()[0] if raw else "status")
            if op in {"status", "show", "get"}:
                st = stock_scanner.get_mt5_tradable_only()
                self._send_text(
                    chat_id,
                    "Stock MT5 Broker-Match Filter\n"
                    f"effective={st.get('effective')}\n"
                    f"runtime_override={st.get('runtime_override')}\n"
                    f"env_default={st.get('env_default')}\n"
                    "Use: /stock_mt5_filter on | off | reset",
                )
                return
            if op in {"on", "1", "true", "enable", "enabled"}:
                val = stock_scanner.set_mt5_tradable_only(True)
                self._send_text(
                    chat_id,
                    "Stock MT5 broker-match filter enabled (runtime).\n"
                    f"effective={val}\n"
                    "Note: runtime toggle resets after bot restart unless .env.local is updated.",
                )
                return
            if op in {"off", "0", "false", "disable", "disabled"}:
                val = stock_scanner.set_mt5_tradable_only(False)
                self._send_text(
                    chat_id,
                    "Stock MT5 broker-match filter disabled (runtime).\n"
                    f"effective={val}\n"
                    "Note: runtime toggle resets after bot restart unless .env.local is updated.",
                )
                return
            if op in {"reset", "default", "env"}:
                val = stock_scanner.set_mt5_tradable_only(None)
                st = stock_scanner.get_mt5_tradable_only()
                self._send_text(
                    chat_id,
                    "Stock MT5 broker-match filter reset to config default.\n"
                    f"effective={val} (env_default={st.get('env_default')})",
                )
                return
            self._send_text(chat_id, "Usage: /stock_mt5_filter [on|off|status|reset]")
            return

        if command == "mt5_history":
            from execution.mt5_executor import mt5_executor

            raw_args = str(args or "").strip()
            symbol_arg = ""
            hours = 24
            if raw_args:
                for tok in raw_args.split():
                    t = str(tok).strip()
                    if not t:
                        continue
                    m_h = re.fullmatch(r"(\d{1,3})\s*h", t.lower())
                    m_d = re.fullmatch(r"(\d{1,3})\s*d", t.lower())
                    if m_h:
                        hours = max(1, min(24 * 30, int(m_h.group(1))))
                        continue
                    if m_d:
                        hours = max(1, min(24 * 30, int(m_d.group(1)) * 24))
                        continue
                    if t.isdigit():
                        hours = max(1, min(24 * 30, int(t)))
                        continue
                    if not symbol_arg:
                        symbol_arg = t.upper()

            symbol_part = f" ({symbol_arg})" if symbol_arg else ""
            self._send_text_localized(chat_id, "mt5_history_progress", lang=lang, hours=hours, symbol_part=symbol_part)
            snap = mt5_executor.closed_trades_snapshot(signal_symbol=symbol_arg, hours=hours, limit=8)
            # Keep MT5 context fresh even when user inspects history.
            if symbol_arg:
                self._chat_mt5_context[int(chat_id)] = {
                    "ts": time.time(),
                    "symbol": symbol_arg,
                    "requested_symbol": symbol_arg,
                    "resolved_symbol": str(snap.get("resolved_symbol") or "").upper(),
                    "positions_count": 0,
                    "orders_count": 0,
                    "kind": "history",
                    "sample": (list(snap.get("closed_trades", []) or [])[:1] or [{}])[0],
                    "ambiguous": False,
                }
            self._send_text(chat_id, self._format_mt5_closed_history_snapshot(snap, lang=lang))
            return

        if command == "mt5_backtest":
            from learning.mt5_backtester import mt5_backtester
            report = mt5_backtester.run(days=30, sync_days=config.NEURAL_BRAIN_SYNC_DAYS)
            notifier.send_mt5_backtest_report(report, chat_id=chat_id)
            return

        if command == "mt5_train":
            from learning.neural_brain import neural_brain
            sync = neural_brain.sync_outcomes_from_mt5(days=config.NEURAL_BRAIN_SYNC_DAYS)
            train = neural_brain.train_backprop(
                days=config.NEURAL_BRAIN_SYNC_DAYS,
                min_samples=config.NEURAL_BRAIN_MIN_SAMPLES,
            )
            self._send_text(
                chat_id,
                (
                    "MT5 Neural Train\n"
                    f"sync_ok={sync.get('ok')} updated={sync.get('updated', 0)} closed_positions={sync.get('closed_positions', 0)}\n"
                    f"train_ok={train.ok} status={train.status}\n"
                    f"message={train.message}\n"
                    f"samples={train.samples} val_acc={train.val_accuracy * 100:.1f}%"
                ),
            )
            return

        if command == "mt5_autopilot":
            from learning.mt5_autopilot_core import mt5_autopilot_core

            st = mt5_autopilot_core.status()
            gate = dict(st.get("risk_gate", {}) or {})
            snap = dict(st.get("risk_snapshot", {}) or {})
            journal = dict(st.get("journal", {}) or {})
            calib = dict(st.get("calibration", {}) or {})
            if lang == "th":
                title = "MT5 Autopilot Core"
                lines = [
                    title,
                    f"enabled={st.get('enabled', False)} account={st.get('account_key', '-')}",
                    f"risk_gate: allow={gate.get('allow', True)} status={gate.get('status', '-')} reason={gate.get('reason', '-')}",
                    f"daily_realized_pnl={snap.get('daily_realized_pnl', '-')} loss_abs={snap.get('daily_loss_abs', '-')} loss_streak={snap.get('consecutive_losses', '-')}",
                    f"rejections_1h={snap.get('recent_rejections_1h', '-')} open_positions={snap.get('open_positions', '-')} pending_orders={snap.get('pending_orders', '-')}",
                    f"journal: total={journal.get('total', 0)} resolved={journal.get('resolved', 0)} open_ft={journal.get('open_forward_tests', 0)}",
                    f"calib: labeled7d={calib.get('labeled_7d', 0)} win_rate_7d={float(calib.get('win_rate_7d', 0.0) or 0.0)*100:.1f}% mae_7d={'-' if calib.get('mae_7d') is None else calib.get('mae_7d')}",
                ]
            elif lang == "de":
                lines = [
                    "MT5 Autopilot Core",
                    f"enabled={st.get('enabled', False)} account={st.get('account_key', '-')}",
                    f"risk_gate: allow={gate.get('allow', True)} status={gate.get('status', '-')} reason={gate.get('reason', '-')}",
                    f"daily_realized_pnl={snap.get('daily_realized_pnl', '-')} loss_abs={snap.get('daily_loss_abs', '-')} loss_streak={snap.get('consecutive_losses', '-')}",
                    f"rejections_1h={snap.get('recent_rejections_1h', '-')} open_positions={snap.get('open_positions', '-')} pending_orders={snap.get('pending_orders', '-')}",
                    f"journal: total={journal.get('total', 0)} resolved={journal.get('resolved', 0)} open_ft={journal.get('open_forward_tests', 0)}",
                    f"calib: labeled7d={calib.get('labeled_7d', 0)} win_rate_7d={float(calib.get('win_rate_7d', 0.0) or 0.0)*100:.1f}% mae_7d={'-' if calib.get('mae_7d') is None else calib.get('mae_7d')}",
                ]
            else:
                lines = [
                    "MT5 Autopilot Core",
                    f"enabled={st.get('enabled', False)} account={st.get('account_key', '-')}",
                    f"risk_gate: allow={gate.get('allow', True)} status={gate.get('status', '-')} reason={gate.get('reason', '-')}",
                    f"daily_realized_pnl={snap.get('daily_realized_pnl', '-')} loss_abs={snap.get('daily_loss_abs', '-')} loss_streak={snap.get('consecutive_losses', '-')}",
                    f"rejections_1h={snap.get('recent_rejections_1h', '-')} open_positions={snap.get('open_positions', '-')} pending_orders={snap.get('pending_orders', '-')}",
                    f"journal: total={journal.get('total', 0)} resolved={journal.get('resolved', 0)} open_ft={journal.get('open_forward_tests', 0)}",
                    f"calib: labeled7d={calib.get('labeled_7d', 0)} win_rate_7d={float(calib.get('win_rate_7d', 0.0) or 0.0)*100:.1f}% mae_7d={'-' if calib.get('mae_7d') is None else calib.get('mae_7d')}",
                ]
            self._send_text(chat_id, "\n".join(lines))
            return

        if command == "mt5_walkforward":
            from learning.mt5_orchestrator import mt5_orchestrator
            from learning.mt5_walkforward import mt5_walkforward

            orch = mt5_orchestrator.status()
            acct = str(orch.get("current_account_key", "") or "")
            if not acct:
                self._send_text_localized(chat_id, "mt5_disconnected", lang=lang)
                return
            rpt = mt5_walkforward.build_report(
                acct,
                train_days=max(7, int(getattr(config, "MT5_WF_TRAIN_DAYS", 30))),
                forward_days=max(1, int(getattr(config, "MT5_WF_FORWARD_DAYS", 7))),
            )
            if not rpt.get("ok"):
                self._send_text(chat_id, f"MT5 Walk-Forward failed: {rpt.get('error', 'unknown')}")
                return
            train = dict(rpt.get("train", {}) or {})
            fwd = dict(rpt.get("forward", {}) or {})
            canary = dict(rpt.get("canary", {}) or {})
            title = "MT5 Walk-Forward"
            if lang == "th":
                title = "MT5 Walk-Forward Validation"
            lines = [
                title,
                f"account={acct}",
                f"train: trades={train.get('trades', 0)} win={float(train.get('win_rate', 0.0) or 0.0)*100:.1f}% pnl={train.get('net_pnl', 0)} mae={'-' if train.get('mae') is None else train.get('mae')}",
                f"forward: trades={fwd.get('trades', 0)} win={float(fwd.get('win_rate', 0.0) or 0.0)*100:.1f}% pnl={fwd.get('net_pnl', 0)} mae={'-' if fwd.get('mae') is None else fwd.get('mae')}",
                f"canary_mode={canary.get('canary_mode', True)} canary_pass={canary.get('canary_pass', False)}",
                f"canary_reason={canary.get('reason', '-')} risk_multiplier={canary.get('risk_multiplier', 1.0)}",
            ]
            self._send_text(chat_id, "\n".join(lines))
            return

        if command in {"mt5_plan", "mt5_adaptive_explain"}:
            from execution.mt5_executor import mt5_executor
            from learning.mt5_orchestrator import mt5_orchestrator

            raw = str(args or "").strip()
            toks = [t for t in raw.split() if t]
            whatif = False
            keep = []
            for t in toks:
                tl = str(t).lower()
                if tl in {"--whatif", "whatif", "-w"}:
                    whatif = True
                    continue
                keep.append(t)
            sym_arg = " ".join(keep).strip()
            if not sym_arg:
                ctx = self._get_recent_mt5_context(chat_id)
                sym_arg = str((ctx or {}).get("symbol") or (ctx or {}).get("requested_symbol") or "").upper()
            if not sym_arg:
                self._send_text(chat_id, "Usage: /mt5_plan <symbol> [--whatif]\nExample: /mt5_plan ETHUSD --whatif")
                return
            sym_hint, _ = self._extract_symbol_and_side_hint(sym_arg, mt5_hint=True)
            sym = str(sym_hint or sym_arg).upper()
            if lang == "th":
                self._send_text(chat_id, f"กำลังสร้างพรีวิวแผนเข้าเทรด MT5 สำหรับ {sym}{' (what-if)' if whatif else ''}...")
            elif lang == "de":
                self._send_text(chat_id, f"Erstelle MT5-Planvorschau für {sym}{' (What-if)' if whatif else ''}...")
            else:
                self._send_text(chat_id, f"Building MT5 adaptive plan preview for {sym}{' (what-if)' if whatif else ''}...")

            signal, meta = self._load_live_signal_for_symbol(sym)
            if signal is None:
                msym = str((meta or {}).get("symbol") or sym)
                if lang == "th":
                    self._send_text(chat_id, f"ยังไม่มีสัญญาณ active สำหรับ {msym} ตอนนี้")
                elif lang == "de":
                    self._send_text(chat_id, f"Derzeit kein aktives Signal für {msym}.")
                else:
                    self._send_text(chat_id, f"No active signal for {msym} right now.")
                return

            plan = None
            risk_mult = None
            if bool(getattr(config, "MT5_AUTOPILOT_ENABLED", True)):
                try:
                    plan = mt5_orchestrator.pre_trade_plan(signal, source="telegram_plan_preview")
                    if getattr(plan, "allow", False):
                        risk_mult = float(getattr(plan, "risk_multiplier", 1.0) or 1.0)
                except Exception:
                    plan = None
            if whatif:
                previews = {}
                for scen in ("conservative", "balanced", "aggressive"):
                    previews[scen] = mt5_executor.preview_adaptive_execution(
                        signal,
                        source="telegram_plan_preview",
                        volume_multiplier=risk_mult,
                        scenario=scen,
                    )
                if not any(bool(dict(previews.get(k, {})).get("ok")) for k in previews):
                    p0 = dict(previews.get("balanced", {}) or {})
                    self._send_text(chat_id, f"MT5 plan preview failed: {p0.get('reason') or p0.get('error') or 'unknown'}")
                    return
                self._send_text(chat_id, self._format_mt5_adaptive_plan_whatif(sym, signal, previews, wf_plan=plan, lang=lang))
                return

            prev = mt5_executor.preview_adaptive_execution(signal, source="telegram_plan_preview", volume_multiplier=risk_mult, scenario="balanced")
            if not prev.get("ok"):
                self._send_text(chat_id, f"MT5 plan preview failed: {prev.get('reason') or prev.get('error') or 'unknown'}")
                return
            self._send_text(chat_id, self._format_mt5_adaptive_plan_preview(sym, signal, prev, wf_plan=plan, lang=lang))
            return

        if command == "mt5_pm_learning":
            from learning.mt5_position_manager import mt5_position_manager

            parsed = self._parse_mt5_pm_learning_args(args)
            days = int(parsed.get("days", 30) or 30)
            top = int(parsed.get("top", 8) or 8)
            do_sync = bool(parsed.get("sync", True))
            symbol_filter = str(parsed.get("symbol", "") or "")
            action_filter = str(parsed.get("action", "") or "")
            save_draft = bool(parsed.get("save_draft", False))
            if lang == "th":
                self._send_text(chat_id, "กำลังสรุปผลการทำงานของ MT5 Position Manager (learning report)...")
            elif lang == "de":
                self._send_text(chat_id, "Erstelle MT5 Position-Manager Learning-Report...")
            else:
                self._send_text(chat_id, "Building MT5 Position Manager learning report...")
            rpt = mt5_position_manager.build_learning_report(
                days=days,
                top=top,
                sync=do_sync,
                symbol=symbol_filter,
                action=action_filter,
            )
            if save_draft and rpt.get("ok"):
                try:
                    from learning.mt5_orchestrator import mt5_orchestrator
                    draft = mt5_position_manager.build_policy_draft_from_learning_report(rpt)
                    rpt["draft_result"] = mt5_orchestrator.save_current_account_policy_draft(draft, source="mt5_pm_learning")
                except Exception as e:
                    rpt["draft_result"] = {"ok": False, "error": str(e)}
            self._send_text(chat_id, self._format_mt5_pm_learning_report(rpt, lang=lang))
            return

        if command == "mt5_manage":
            from learning.mt5_position_manager import mt5_position_manager

            raw_args = str(args or "").strip()
            parts = raw_args.split()
            if parts and parts[0].lower() in {"watch", "status"}:
                symbol_arg = ""
                if len(parts) >= 2:
                    symbol_arg = str(parts[1] or "").strip().upper()
                rpt = mt5_position_manager.watch_snapshot(signal_symbol=symbol_arg, limit=8)
                self._send_text(chat_id, self._format_mt5_pm_watch_snapshot(rpt, lang=lang))
                return
            if lang == "th":
                self._send_text(chat_id, "กำลังรัน MT5 Position Manager (BE/partial/trail/time-stop)...")
            elif lang == "de":
                self._send_text(chat_id, "Starte MT5 Position Manager (BE/Teilgewinn/Trailing/Time-Stop)...")
            else:
                self._send_text(chat_id, "Running MT5 Position Manager (BE/partial/trail/time-stop)...")
            rpt = mt5_position_manager.run_cycle(source="telegram")
            if not rpt.get("ok"):
                self._send_text(chat_id, f"MT5 Position Manager failed: {rpt.get('error', 'unknown')}")
                return
            actions = list(rpt.get("actions", []) or [])
            if actions:
                notifier.send_mt5_position_manager_update(rpt, source="telegram", chat_id=chat_id)
                return
            if lang == "th":
                txt = (
                    "MT5 Position Manager\n"
                    f"account={rpt.get('account_key','-')}\n"
                    f"positions={rpt.get('positions',0)} checked={rpt.get('checked',0)} managed={rpt.get('managed',0)}\n"
                    "ยังไม่มี action ในรอบนี้"
                )
            elif lang == "de":
                txt = (
                    "MT5 Position Manager\n"
                    f"account={rpt.get('account_key','-')}\n"
                    f"positions={rpt.get('positions',0)} checked={rpt.get('checked',0)} managed={rpt.get('managed',0)}\n"
                    "Keine Aktion in diesem Lauf"
                )
            else:
                txt = (
                    "MT5 Position Manager\n"
                    f"account={rpt.get('account_key','-')}\n"
                    f"positions={rpt.get('positions',0)} checked={rpt.get('checked',0)} managed={rpt.get('managed',0)}\n"
                    "No actions this cycle"
                )
            self._send_text(chat_id, txt)
            return

        if command == "mt5_policy":
            from learning.mt5_orchestrator import mt5_orchestrator

            raw = str(args or "").strip()
            parts = raw.split()
            op = (parts[0].lower() if parts else "show")
            if op not in {"show", "set", "reset", "keys", "preset"}:
                op = "show"
                parts = raw.split() if raw else []

            if op == "keys":
                self._send_text(chat_id, self._format_mt5_policy_keys())
                return

            if op == "preset":
                preset_name = str(parts[1] if len(parts) > 1 else "").strip().lower()
                if not preset_name:
                    self._send_text(chat_id, "Usage: /mt5_policy preset <micro_safe|micro_aggressive>")
                    return
                rep = mt5_orchestrator.apply_current_account_preset(preset_name)
                if not rep.get("ok"):
                    self._send_text(chat_id, f"MT5 policy preset failed: {rep.get('message', 'unknown')}")
                    return
                self._send_text(
                    chat_id,
                    f"MT5 policy preset applied ({rep.get('account_key','-')})\n"
                    f"preset={rep.get('preset')}  {rep.get('preset_desc','')}",
                )
                return

            if op == "show":
                rep = mt5_orchestrator.current_account_policy()
                if not rep.get("ok"):
                    self._send_text(chat_id, f"MT5 policy unavailable: {rep.get('message', 'unknown')}")
                    return
                pol = dict(rep.get("policy", {}) or {})
                lines = [f"MT5 Policy ({rep.get('account_key','-')})"]
                for k in sorted(pol.keys()):
                    lines.append(f"{k}={pol.get(k)}")
                lines.append("Usage: /mt5_policy set <key> <value> | /mt5_policy keys | /mt5_policy preset <name> | /mt5_policy reset")
                self._send_text(chat_id, "\n".join(lines))
                return

            if op == "reset":
                rep = mt5_orchestrator.reset_current_account_policy()
                if not rep.get("ok"):
                    self._send_text(chat_id, f"MT5 policy reset failed: {rep.get('message', 'unknown')}")
                    return
                self._send_text(chat_id, f"MT5 policy reset for {rep.get('account_key','-')}")
                return

            # set
            key = ""
            value = ""
            rem = parts[1:]
            if rem:
                if "=" in rem[0] and rem[0].count("=") == 1:
                    key, value = rem[0].split("=", 1)
                    if len(rem) > 1 and not value:
                        value = " ".join(rem[1:])
                elif len(rem) >= 2:
                    key = rem[0]
                    value = " ".join(rem[1:])
            if not key:
                self._send_text(
                    chat_id,
                    "Usage: /mt5_policy set <key> <value>\n"
                    "Use /mt5_policy keys to see supported keys\n"
                    "Use /mt5_policy preset micro_safe for quick profile\n"
                    "Example: /mt5_policy set canary_force false\n"
                    "Example: /mt5_policy set daily_loss_limit_usd 0.8",
                )
                return
            rep = mt5_orchestrator.set_current_account_policy(str(key).strip(), value)
            if not rep.get("ok"):
                self._send_text(chat_id, f"MT5 policy set failed: {rep.get('message', 'unknown')}")
                return
            self._send_text(
                chat_id,
                f"MT5 policy updated ({rep.get('account_key','-')})\n"
                f"{rep.get('updated_key')}={rep.get('updated_value')}",
            )
            return

        if command == "scan_gold":
            self._send_text_localized(chat_id, "running_gold", lang=lang)
            run_result = scheduler.run_once("xauusd")
            xres = (run_result or {}).get("xauusd")
            if xres:
                self._send_text(chat_id, self._format_xauusd_scan_status(xres))
            return

        if command == "scan_crypto":
            self._send_text_localized(chat_id, "running_crypto", lang=lang)
            scheduler.run_once("crypto")
            return

        if command == "scan_fx":
            if lang == "th":
                self._send_text(chat_id, "กำลังสแกน FX majors (EURUSD/GBPUSD/USDJPY/AUDUSD/NZDUSD/USDCAD/USDCHF)...")
            elif lang == "de":
                self._send_text(chat_id, "Scanne FX-Majors (EURUSD/GBPUSD/USDJPY/AUDUSD/NZDUSD/USDCAD/USDCHF)...")
            else:
                self._send_text(chat_id, "Running FX major scan (EURUSD/GBPUSD/USDJPY/AUDUSD/NZDUSD/USDCAD/USDCHF)...")
            scheduler.run_once("fx")
            return

        if command == "scan_stocks":
            self._send_text_localized(chat_id, "running_stocks", lang=lang)
            scheduler.run_once("stocks")
            return

        if command == "scan_thai":
            self._send_text_localized(chat_id, "running_thai", lang=lang)
            scheduler.run_once("thai")
            return

        if command == "scan_thai_vi":
            if lang == "th":
                self._send_text(chat_id, "กำลังสแกน Thailand VI (value + trend, off-hours friendly)...")
            elif lang == "de":
                self._send_text(chat_id, "Scanne Thailand VI (Value + Trend, auch außerhalb der Marktzeiten)...")
            else:
                self._send_text(chat_id, "Running Thailand VI (value + trend, off-hours friendly)...")
            scheduler.run_once("thai_vi")
            return

        if command == "scan_us":
            self._send_text_localized(chat_id, "running_us_open", lang=lang)
            scheduler.run_once("us_open")
            return

        if command == "scan_us_open":
            self._send_text_localized(chat_id, "running_us_open", lang=lang)
            scheduler.run_once("us_open")
            return

        if command == "monitor_us":
            self._send_text_localized(chat_id, "running_us_monitor", lang=lang)
            scheduler.run_once("us_open_monitor")
            try:
                guard_snap = scheduler.get_us_open_guard_status()
                self._send_text(chat_id, self._format_us_open_guard_status_compact(guard_snap, lang=lang))
            except Exception:
                logger.debug("[AdminBot] monitor_us guard snapshot failed", exc_info=True)
            return

        if command == "us_open_guard_status":
            if lang == "th":
                self._send_text(chat_id, "กำลังตรวจ US Open guard status (macro-freeze / circuit-breaker / mood-stop)...")
            elif lang == "de":
                self._send_text(chat_id, "Prüfe US-Open Guard-Status (Macro-Freeze / Circuit-Breaker / Mood-Stop)...")
            else:
                self._send_text(chat_id, "Checking US-open guard status (macro-freeze / circuit-breaker / mood-stop)...")
            mode = str((args or '').strip().lower())
            snap = scheduler.get_us_open_guard_status()
            if mode == 'compact':
                txt = self._format_us_open_guard_status_compact(snap, lang=lang)
            elif mode == 'why':
                txt = self._format_us_open_guard_status_why(snap, lang=lang)
            else:
                txt = self._format_us_open_guard_status(snap, lang=lang)
            self._send_text(chat_id, txt)
            return

        if command == "us_open_report":
            from learning.neural_brain import neural_brain
            if lang == "th":
                self._send_text(chat_id, "กำลังสรุปผลสัญญาณ US Open ของวันนี้...")
            elif lang == "de":
                self._send_text(chat_id, "Erstelle heutigen US-Open Signal-Qualitätsreport...")
            else:
                self._send_text(chat_id, "Building today's US Open signal quality recap...")
            # Use dashboard-aligned session dataset so /us_open_report and /us_open_dashboard match.
            dash = neural_brain.us_open_trader_dashboard(risk_pct=1.0, start_balance=1000.0)
            if str(dash.get("status")) == "ok" and int((dash.get("summary") or {}).get("sent", 0) or 0) > 0:
                notifier.send_us_open_signal_quality_recap(dash, chat_id=chat_id)
                return
            # Legacy fallback (older source-filter summary) if dashboard has no rows.
            rpt = neural_brain.signal_feedback_report(days=1, source_contains="us_open")
            if int(rpt.get("sent", 0) or 0) == 0:
                if str(dash.get("status")) == "ok":
                    notifier.send_us_open_trader_dashboard(dash, chat_id=chat_id)
                    return

            notifier.send_us_open_signal_quality_recap(rpt, chat_id=chat_id)
            return

        if command == "signal_dashboard":
            from learning.neural_brain import neural_brain

            parsed = self._parse_signal_dashboard_args(args)
            days = int(parsed.get("days", 1) or 1)
            top_n = int(parsed.get("top", 5) or 5)
            market_filter = parsed.get("market_filter")
            symbol_filter = str(parsed.get("symbol_filter") or "").strip().upper() or None
            window_mode = str(parsed.get("window_mode") or "today").strip().lower()
            if parsed.get("compare"):
                left = str(parsed.get("left") or "us")
                right = str(parsed.get("right") or "thai")
                period = self._signal_dashboard_window_label(window_mode, days, lang=lang)
                left_label = self._signal_dashboard_market_label(left, lang=lang)
                right_label = self._signal_dashboard_market_label(right, lang=lang)
                if lang == "th":
                    self._send_text(chat_id, f"กำลังเทียบ Signal Dashboard: {left_label} vs {right_label} (ช่วง {period})...")
                elif lang == "de":
                    self._send_text(chat_id, f"Vergleiche Signal-Dashboard: {left_label} vs {right_label} (Zeitraum {period})...")
                else:
                    self._send_text(chat_id, f"Comparing signal dashboard: {left_label} vs {right_label} (period {period})...")
                ra = neural_brain.daily_signal_trader_dashboard(
                    days=days, risk_pct=1.0, start_balance=1000.0, market_filter=left, window_mode=window_mode
                )
                rb = neural_brain.daily_signal_trader_dashboard(
                    days=days, risk_pct=1.0, start_balance=1000.0, market_filter=right, window_mode=window_mode
                )
                self._send_text(chat_id, self._format_signal_dashboard_compare(ra, rb, lang=lang))
                return

            period = self._signal_dashboard_window_label(window_mode, days, lang=lang)
            market_label = self._signal_dashboard_market_label(str(market_filter or ""), lang=lang) if market_filter else ""
            if lang == "th":
                label = f" (ช่วง={period}"
                if market_filter:
                    label += f", ตลาด={market_label}"
                if symbol_filter:
                    label += f", คู่={symbol_filter}"
                if top_n:
                    label += f", top{top_n}"
                label += ")"
                self._send_text(chat_id, f"กำลังสร้าง Signal Dashboard{label}...")
            elif lang == "de":
                label = f" (Zeitraum={period}"
                if market_filter:
                    label += f", Markt={market_label}"
                if symbol_filter:
                    label += f", Symbol={symbol_filter}"
                if top_n:
                    label += f", top{top_n}"
                label += ")"
                self._send_text(chat_id, f"Erstelle Signal-Dashboard{label}...")
            else:
                label = f" (period={period}"
                if market_filter:
                    label += f", market={market_label}"
                if symbol_filter:
                    label += f", pair={symbol_filter}"
                if top_n:
                    label += f", top{top_n}"
                label += ")"
                self._send_text(chat_id, f"Building signal dashboard{label}...")
            rpt = neural_brain.daily_signal_trader_dashboard(
                days=days,
                risk_pct=1.0,
                start_balance=1000.0,
                market_filter=market_filter,
                symbol_filter=symbol_filter,
                window_mode=window_mode,
            )
            rpt["display_top_n"] = top_n
            notifier.send_signal_trader_dashboard(rpt, chat_id=chat_id, lang=lang)
            return

        if command == "us_open_dashboard":
            from learning.neural_brain import neural_brain
            if lang == "th":
                self._send_text(chat_id, "กำลังสร้าง US Open trader dashboard ของวันนี้...")
            elif lang == "de":
                self._send_text(chat_id, "Erstelle das heutige US-Open Trader-Dashboard...")
            else:
                self._send_text(chat_id, "Building today's US Open trader dashboard...")
            rpt = neural_brain.us_open_trader_dashboard(risk_pct=1.0, start_balance=1000.0)
            notifier.send_us_open_trader_dashboard(rpt, chat_id=chat_id)
            return

        if command == "scan_vi":
            self._send_text_localized(chat_id, "running_vi", lang=lang)
            scheduler.run_once("vi")
            return

        if command == "scan_vi_buffett":
            if lang == "th":
                self._send_text(chat_id, "กำลังสแกน US VI (Buffett-style compounders)...")
            elif lang == "de":
                self._send_text(chat_id, "Scanne US VI (Buffett-ähnliche Compounder)...")
            else:
                self._send_text(chat_id, "Running US VI (Buffett-style compounders)...")
            scheduler.run_once("vi_buffett")
            return

        if command == "scan_vi_turnaround":
            if lang == "th":
                self._send_text(chat_id, "กำลังสแกน US VI (turnaround / multi-bagger candidates)...")
            elif lang == "de":
                self._send_text(chat_id, "Scanne US VI (Turnaround / Multi-Bagger Kandidaten)...")
            else:
                self._send_text(chat_id, "Running US VI (turnaround / multi-bagger candidates)...")
            scheduler.run_once("vi_turnaround")
            return

        if command == "calendar":
            self._send_text_localized(chat_id, "checking_calendar", lang=lang)
            from market.economic_calendar import economic_calendar
            hours = max(6, int(getattr(config, "ECON_CALENDAR_LOOKAHEAD_HOURS", 24)))
            events = economic_calendar.next_events(
                hours=hours,
                limit=10,
                min_impact="medium",
                currencies=config.get_econ_alert_currencies(),
            )
            notifier.send_economic_calendar_snapshot(events, lookahead_hours=hours, chat_id=chat_id)
            return

        if command == "macro":
            self._send_text_localized(chat_id, "checking_macro", lang=lang)
            from market.macro_news import macro_news
            lookback_h = max(1, int(getattr(config, "MACRO_NEWS_LOOKBACK_HOURS", 24)))
            min_score = max(1, int(getattr(config, "MACRO_NEWS_MIN_SCORE", 6)))
            min_risk_stars = macro_news.score_to_stars(min_score)
            raw_filter = str((args or "").strip())
            if not raw_filter:
                try:
                    saved = access_manager.get_user_macro_risk_filter(user_id)
                except Exception:
                    saved = None
                if saved:
                    parsed_score = macro_news.stars_to_min_score(saved)
                    if parsed_score is not None:
                        min_score = int(parsed_score)
                        min_risk_stars = macro_news.score_to_stars(min_score)
            if raw_filter:
                if raw_filter.lower() in {"reset", "default", "clear"}:
                    try:
                        access_manager.set_user_macro_risk_filter(user_id, None)
                    except Exception:
                        pass
                    min_score = max(1, int(getattr(config, "MACRO_NEWS_MIN_SCORE", 6)))
                    min_risk_stars = macro_news.score_to_stars(min_score)
                    raw_filter = ""
                else:
                    parsed_score = macro_news.stars_to_min_score(raw_filter)
                    if parsed_score is None:
                        if lang == "th":
                            self._send_text(chat_id, "ใช้ได้เฉพาะ /macro *, /macro **, /macro ***  (หรือ /macro reset)")
                        elif lang == "de":
                            self._send_text(chat_id, "Verwendung: /macro *  |  /macro **  |  /macro ***  (oder /macro reset)")
                        else:
                            self._send_text(chat_id, "Usage: /macro *  |  /macro **  |  /macro ***  (or /macro reset)")
                        return
                    min_score = int(parsed_score)
                    min_risk_stars = macro_news.score_to_stars(min_score)
                    try:
                        access_manager.set_user_macro_risk_filter(user_id, min_risk_stars)
                    except Exception as e:
                        logger.warning("[AdminBot] save macro risk filter failed user=%s err=%s", user_id, e)
            heads = macro_news.high_impact_headlines(hours=lookback_h, min_score=min_score, limit=8)
            notifier.send_macro_news_snapshot(
                heads,
                lookback_hours=lookback_h,
                chat_id=chat_id,
                min_risk_stars=min_risk_stars,
            )
            return

        if command == "macro_report":
            self._send_text_localized(chat_id, "checking_macro_report", lang=lang)
            from market.macro_news import macro_news
            from market.macro_impact_tracker import macro_impact_tracker

            lookback_h = max(1, int(getattr(config, "MACRO_REPORT_DEFAULT_HOURS", 24)))
            min_score = max(1, int(getattr(config, "MACRO_NEWS_MIN_SCORE", 6)))
            min_risk_stars = macro_news.score_to_stars(min_score)
            raw_args = str((args or "").strip())

            # Default to saved /macro risk filter (per-user).
            if not raw_args:
                try:
                    saved = access_manager.get_user_macro_risk_filter(user_id)
                except Exception:
                    saved = None
                if saved:
                    parsed_score = macro_news.stars_to_min_score(saved)
                    if parsed_score is not None:
                        min_score = int(parsed_score)
                        min_risk_stars = macro_news.score_to_stars(min_score)

            if raw_args:
                parts = raw_args.split()
                explicit_star_pref = None
                for p in parts:
                    token = str(p).strip()
                    if not token:
                        continue
                    score_try = macro_news.stars_to_min_score(token)
                    if score_try is not None:
                        min_score = int(score_try)
                        min_risk_stars = macro_news.score_to_stars(min_score)
                        explicit_star_pref = min_risk_stars
                        continue
                    m_h = re.fullmatch(r"(\d{1,3})\s*h", token.lower())
                    m_d = re.fullmatch(r"(\d{1,3})\s*d", token.lower())
                    if m_h:
                        lookback_h = max(1, min(24 * 30, int(m_h.group(1))))
                        continue
                    if m_d:
                        lookback_h = max(1, min(24 * 30, int(m_d.group(1)) * 24))
                        continue
                    if token.isdigit():
                        lookback_h = max(1, min(24 * 30, int(token)))
                        continue
                if explicit_star_pref:
                    try:
                        access_manager.set_user_macro_risk_filter(user_id, explicit_star_pref)
                    except Exception as e:
                        logger.warning("[AdminBot] save macro risk filter from macro_report failed user=%s err=%s", user_id, e)

            # Lightweight sync before report so samples catch up.
            try:
                macro_impact_tracker.sync(
                    hours=max(lookback_h, int(getattr(config, "MACRO_IMPACT_TRACKER_LOOKBACK_HOURS", 72))),
                    min_score=max(1, int(getattr(config, "MACRO_IMPACT_TRACKER_MIN_SCORE", 5))),
                    limit=max(int(getattr(config, "MACRO_IMPACT_TRACKER_MAX_HEADLINES_PER_SYNC", 20)), int(getattr(config, "MACRO_REPORT_MAX_HEADLINES", 5))),
                )
            except Exception as e:
                logger.warning("[AdminBot] macro impact sync before report failed: %s", e)

            report = macro_impact_tracker.build_report(
                hours=lookback_h,
                min_score=min_score,
                min_risk_stars=min_risk_stars,
                limit=max(1, int(getattr(config, "MACRO_REPORT_MAX_HEADLINES", 5))),
            )
            notifier.send_macro_impact_report(report, chat_id=chat_id)
            return

        if command == "macro_weights":
            self._send_text_localized(chat_id, "checking_macro_weights", lang=lang)
            from market.macro_impact_tracker import macro_impact_tracker

            raw_args = str((args or "").strip())
            refresh_now = False
            top_n = max(1, int(getattr(config, "MACRO_WEIGHTS_DEFAULT_TOP", 8)))
            for part in raw_args.split():
                token = str(part or "").strip().lower()
                if not token:
                    continue
                if token in {"refresh", "recalc", "recompute", "update", "sync"}:
                    refresh_now = True
                    continue
                m_top = re.fullmatch(r"top\s*(\d{1,2})", token)
                if m_top:
                    top_n = max(1, min(20, int(m_top.group(1))))
                    continue
                if token.isdigit():
                    top_n = max(1, min(20, int(token)))
                    continue

            refresh_result = None
            if refresh_now:
                try:
                    refresh_result = macro_impact_tracker.refresh_adaptive_weights()
                except Exception as e:
                    logger.warning("[AdminBot] macro_weights refresh failed: %s", e)
                    refresh_result = {"ok": False, "status": "error", "error": str(e)}

            report = macro_impact_tracker.build_weights_report(limit=top_n)
            if refresh_result is not None:
                report = dict(report or {})
                report["refresh_result"] = refresh_result
            notifier.send_macro_weights_report(report, chat_id=chat_id)
            return

        if command == "scan_all":
            self._send_text_localized(chat_id, "running_all", lang=lang)
            scheduler.run_once("all")
            return

        if command == "markets":
            overview = stock_scanner.get_market_overview()
            notifier.send_market_hours_overview(overview, chat_id=chat_id)
            return

        if command == "gold_overview":
            overview = xauusd_scanner.get_market_overview()
            notifier.send_xauusd_overview(overview, chat_id=chat_id)
            return

        if command == "research":
            if not args:
                if lang == "th":
                    self._send_text(chat_id, "วิธีใช้: /research <คำถาม>")
                elif lang == "de":
                    self._send_text(chat_id, "Nutzung: /research <frage>")
                else:
                    self._send_text(chat_id, "Usage: /research <question>")
                return
            self._send_text_localized(chat_id, "research_progress", lang=lang)
            t = threading.Thread(
                target=self._run_research_reply,
                args=(chat_id, args, lang),
                daemon=True,
                name="DexterResearchReply",
            )
            t.start()
            return

        if command in ("grant", "setplan"):
            if not is_admin:
                self._send_text(chat_id, "/grant is admin-only.")
                return
            parts = (args or "").split()
            if len(parts) < 3:
                self._send_text(chat_id, "Usage: /grant <user_id> <trial|a|b|c> <days>")
                return
            uid_raw, plan, days_raw = parts[0], parts[1], parts[2]
            if not uid_raw.lstrip("-").isdigit() or not days_raw.isdigit():
                self._send_text(chat_id, "Invalid format. Example: /grant 123456789 b 30")
                return
            granted = access_manager.grant_plan(
                int(uid_raw),
                plan=plan.lower(),
                days=int(days_raw),
                status="active",
                note=f"granted_by:{user_id}",
            )
            self._send_text(
                chat_id,
                f"Granted plan {granted.get('plan','').upper()} to {uid_raw} "
                f"for {days_raw} days (expires {granted.get('expires_at','-')}).",
            )
            return

        if command in ("admin_add", "admin_del"):
            if not is_admin:
                self._send_text(chat_id, "/admin_add is admin-only.")
                return
            target_raw = (args or "").strip().split()[0] if (args or "").strip() else ""
            if not target_raw:
                self._send_text(chat_id, "Usage: /admin_add <user_id|@username>  |  /admin_del <user_id|@username>\nTip: /user_list [keyword]")
                return
            resolved = access_manager.resolve_known_telegram_user(target_raw)
            if not resolved:
                self._send_text(chat_id, f"User not found for {target_raw}. Try /user_list or /user_list <keyword> first.")
                return
            target_uid = int(resolved.get("user_id"))
            enabled = (command == "admin_add")
            note_suffix = f" target={target_raw}" if str(target_raw) else ""
            role = access_manager.set_admin_role(target_uid, enabled=enabled, note=f"{command}_by:{user_id}{note_suffix}")
            label = f"@{resolved.get('username')}" if resolved.get("username") else str(target_uid)
            name = " ".join(x for x in [str(resolved.get('first_name') or '').strip(), str(resolved.get('last_name') or '').strip()] if x).strip()
            extra = f" ({name})" if name else ""
            self._send_text(
                chat_id,
                f"Admin role for {label} [{target_uid}]{extra}: {str(role.get('status','-')).upper()} (source={role.get('source','db')}).",
            )
            return

        if command == "admin_list":
            if not is_admin:
                self._send_text(chat_id, "/admin_list is admin-only.")
                return
            rows = access_manager.list_admin_roles()
            if not rows:
                self._send_text(chat_id, "No admin roles found.")
                return
            lines = ["Admin roles:"]
            for r in rows[:50]:
                lines.append(
                    f"- {r.get('user_id')}  {str(r.get('status','')).upper()}  source={r.get('source','-')}"
                    + (f"  note={r.get('notes')}" if r.get('notes') else "")
                )
            self._send_text(chat_id, "\n".join(lines))
            return

        if command == "user_list":
            if not is_admin:
                self._send_text(chat_id, "/user_list is admin-only.")
                return
            raw_args = str(args or "").strip()
            limit = 20
            query = raw_args
            toks = [t for t in raw_args.split() if t]
            for i, tk in enumerate(toks):
                tl = tk.lower()
                if tl in {"top", "limit"} and i + 1 < len(toks) and toks[i + 1].isdigit():
                    limit = max(1, min(100, int(toks[i + 1])))
                    query = " ".join(t for j, t in enumerate(toks) if j not in {i, i + 1}).strip()
                    break
                if tl.startswith("top") and tl[3:].isdigit():
                    limit = max(1, min(100, int(tl[3:])))
                    query = " ".join(t for j, t in enumerate(toks) if j != i).strip()
                    break
            rows = access_manager.list_known_telegram_users(query=query, limit=limit)
            if not rows:
                self._send_text(chat_id, "No known users found. The bot only lists users who have already sent a message to this bot.")
                return
            hdr = f"Known users ({len(rows)} shown" + (f", filter={query}" if query else "") + "):"
            lines = [hdr]
            for r in rows:
                uid_row = r.get("user_id")
                uname = str(r.get("username") or "").strip()
                name = " ".join(x for x in [str(r.get('first_name') or '').strip(), str(r.get('last_name') or '').strip()] if x).strip()
                handle = f"@{uname}" if uname else "(no username)"
                plan = str(r.get("plan") or "").upper() or "-"
                pstatus = str(r.get("plan_status") or "")
                plan_txt = plan if not pstatus else f"{plan}/{pstatus}"
                seen = str(r.get("last_seen_at") or "-")
                lines.append(f"- {uid_row}  {handle}  {name or '-'}  plan={plan_txt}  seen={seen}")
            lines.append("Tip: /admin_add @username  or  /admin_add <user_id>")
            self._send_text(chat_id, "\n".join(lines))
            return

        # ── Parameter Trial Sandbox commands ────────────────────────────────
        if command in {"trials", "trial_list", "pts"}:
            if not is_admin:
                self._send_text(chat_id, "/trials is admin-only.")
                return
            try:
                from learning.live_profile_autopilot import live_profile_autopilot
                trials = live_profile_autopilot._load_trials()
            except Exception as e:
                self._send_text(chat_id, f"Error loading trials: {e}")
                return
            if not trials:
                self._send_text(chat_id, "No parameter trials found.")
                return
            lines = ["PARAMETER TRIALS", ""]
            status_icon = {"pending_bt": "⏳", "bt_running": "🔄", "bt_passed": "✅", "bt_failed": "❌", "applied": "✔️", "rejected": "🚫"}
            for t in trials[-10:]:
                tid = str(t.get("id") or "")
                status = str(t.get("status") or "")
                icon = status_icon.get(status, "•")
                param = str(t.get("param") or "")
                cur = str(t.get("current_value") or "")
                prop = str(t.get("proposed_value") or "")
                direction = str(t.get("direction") or "")
                created = str(t.get("created_at") or "")[:16]
                lines.append(f"{icon} [{status}] {created}")
                lines.append(f"   {param}: {cur} → {prop} ({direction})")
                if status == "bt_passed":
                    lines.append(f"   ✅ READY → /approve {tid}")
                elif status == "pending_bt":
                    lines.append(f"   ⏳ BT pending — waiting for shadow data")
                lines.append(f"   ID: {tid}")
                lines.append("")
            self._send_text(chat_id, "\n".join(lines).strip())
            return

        if command in {"approve", "approve_trial"}:
            if not is_admin:
                self._send_text(chat_id, "/approve is admin-only.")
                return
            trial_id = str(args or "").strip()
            if not trial_id:
                self._send_text(chat_id, "Usage: /approve <trial_id>\nGet IDs from /trials")
                return
            try:
                from learning.live_profile_autopilot import live_profile_autopilot
                result = live_profile_autopilot.apply_trial(trial_id)
            except Exception as e:
                self._send_text(chat_id, f"Error applying trial: {e}")
                return
            if bool(result.get("ok")):
                param = str(result.get("param") or "")
                value = str(result.get("value") or "")
                self._send_text(
                    chat_id,
                    f"✅ Trial applied successfully\n\n"
                    f"  {param} = {value}\n"
                    f"  Written to .env.local + runtime config\n"
                    f"  Trial ID: {trial_id}"
                )
            else:
                self._send_text(chat_id, f"❌ Apply failed: {result.get('error', 'unknown')}")
            return

        if command in {"reject", "reject_trial"}:
            if not is_admin:
                self._send_text(chat_id, "/reject is admin-only.")
                return
            trial_id = str(args or "").strip()
            if not trial_id:
                self._send_text(chat_id, "Usage: /reject <trial_id>")
                return
            try:
                from learning.live_profile_autopilot import live_profile_autopilot
                trials = live_profile_autopilot._load_trials()
                trial = next((t for t in trials if str(t.get("id") or "") == trial_id), None)
                if not trial:
                    self._send_text(chat_id, f"Trial not found: {trial_id}")
                    return
                trial["status"] = "rejected"
                trial["rejected_at"] = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
                live_profile_autopilot._save_trials(trials)
                self._send_text(
                    chat_id,
                    f"🚫 Trial rejected\n\n"
                    f"  {trial.get('param')}: {trial.get('current_value')} → {trial.get('proposed_value')}\n"
                    f"  Current value kept. Trial ID: {trial_id}"
                )
            except Exception as e:
                self._send_text(chat_id, f"Error rejecting trial: {e}")
            return

        if command in {"budget", "token_budget"}:
            try:
                from openclaw.token_budget import get_status as _budget_status
                st = _budget_status()
                month = st.get("month", "?")
                models = st.get("models", {})
                lines = [f"💰 *Qwen Token Budget — {month}*\n"]
                if not models:
                    lines.append("  ยังไม่มีการใช้งาน (เริ่มต้นใหม่)")
                for model, info in models.items():
                    used = info.get("used_tokens", 0)
                    budget = info.get("budget_tokens", 900_000)
                    pct = info.get("pct", 0)
                    calls = info.get("calls", 0)
                    bar = "🟢" if pct < 60 else "🟡" if pct < 80 else "🔴"
                    lines.append(f"{bar} *{model}*")
                    lines.append(f"  {used:,} / {budget:,} tokens ({pct}%)")
                    lines.append(f"  {calls} calls this month")
                    lines.append(f"  Switch to Groq at 95%")
                    lines.append("")
                lines.append("_Groq fallback = ฟรีไม่มี quota_")
                self._send_text(chat_id, "\n".join(lines), parse_mode="Markdown")
            except Exception as exc:
                self._send_text(chat_id, f"Budget error: {exc}")
            return

        if command == "copy_status":
            try:
                from copy_trade.manager import copy_trade_manager
                self._send_text(chat_id, copy_trade_manager.format_telegram_status(), parse_mode="Markdown")
            except Exception as exc:
                self._send_text(chat_id, f"CopyTrade error: {exc}")
            return

        if command == "copy_add_ctrader":
            if not is_admin:
                self._send_text(chat_id, "Admin only.")
                return
            parts = str(args or "").strip().split()
            if len(parts) < 2:
                self._send_text(
                    chat_id,
                    "Usage: /copy_add_ctrader <label> <account_id> [risk_mult] [max_risk_usd]\n"
                    "Example: /copy_add_ctrader MyAccount2 12345678 0.5 25",
                )
                return
            try:
                from copy_trade.accounts import account_registry as _ct_reg
                label = parts[0]
                ct_id = int(parts[1])
                risk_mult = float(parts[2]) if len(parts) > 2 else 1.0
                max_risk = float(parts[3]) if len(parts) > 3 else 50.0
                acc = _ct_reg.add_ctrader(label, ct_id, risk_multiplier=risk_mult, max_risk_usd=max_risk)
                self._send_text(
                    chat_id,
                    f"✅ Added cTrader follower:\n"
                    f"  Label: {acc.label}\n"
                    f"  Account: {acc.ctrader_account_id}\n"
                    f"  Risk: {acc.risk_multiplier}x (max ${acc.max_risk_usd})",
                )
            except Exception as exc:
                self._send_text(chat_id, f"Error: {exc}")
            return

        if command == "copy_add_mt5":
            if not is_admin:
                self._send_text(chat_id, "Admin only.")
                return
            parts = str(args or "").strip().split()
            if len(parts) < 2:
                self._send_text(
                    chat_id,
                    "Usage: /copy_add_mt5 <label> <login> [server] [risk_mult] [max_risk_usd]\n"
                    "Example: /copy_add_mt5 MyMT5 5001234 ICMarkets-Live 0.5 25",
                )
                return
            try:
                from copy_trade.accounts import account_registry as _ct_reg
                label = parts[0]
                login = int(parts[1])
                server = parts[2] if len(parts) > 2 else ""
                risk_mult = float(parts[3]) if len(parts) > 3 else 1.0
                max_risk = float(parts[4]) if len(parts) > 4 else 50.0
                acc = _ct_reg.add_mt5(label, mt5_login=login, mt5_server=server, risk_multiplier=risk_mult, max_risk_usd=max_risk)
                self._send_text(
                    chat_id,
                    f"✅ Added MT5 follower:\n"
                    f"  Label: {acc.label}\n"
                    f"  Login: {acc.mt5_login}\n"
                    f"  Server: {acc.mt5_server or 'default'}\n"
                    f"  Risk: {acc.risk_multiplier}x (max ${acc.max_risk_usd})",
                )
            except Exception as exc:
                self._send_text(chat_id, f"Error: {exc}")
            return

        if command == "copy_remove":
            if not is_admin:
                self._send_text(chat_id, "Admin only.")
                return
            account_id = str(args or "").strip()
            if not account_id:
                self._send_text(chat_id, "Usage: /copy_remove <account_id>\nUse /copy_status to see account IDs.")
                return
            try:
                from copy_trade.accounts import account_registry as _ct_reg
                if _ct_reg.remove(account_id):
                    self._send_text(chat_id, f"✅ Removed: {account_id}")
                else:
                    self._send_text(chat_id, f"❌ Not found: {account_id}")
            except Exception as exc:
                self._send_text(chat_id, f"Error: {exc}")
            return

        if command in ("copy_pause", "copy_resume"):
            if not is_admin:
                self._send_text(chat_id, "Admin only.")
                return
            account_id = str(args or "").strip()
            if not account_id:
                self._send_text(chat_id, f"Usage: /{command} <account_id>")
                return
            try:
                from copy_trade.accounts import account_registry as _ct_reg
                enabled = command == "copy_resume"
                if _ct_reg.set_enabled(account_id, enabled):
                    status = "resumed" if enabled else "paused"
                    self._send_text(chat_id, f"✅ {account_id} {status}")
                else:
                    self._send_text(chat_id, f"❌ Not found: {account_id}")
            except Exception as exc:
                self._send_text(chat_id, f"Error: {exc}")
            return

        if command == "copy_log":
            try:
                from copy_trade.manager import copy_trade_manager
                logs = copy_trade_manager.get_recent_log(10)
                if not logs:
                    self._send_text(chat_id, "No recent copy trade dispatches.")
                    return
                lines = ["📊 *Recent Copy Trades*\n"]
                for entry in reversed(logs):
                    sym = entry.get("symbol", "")
                    d = entry.get("direction", "")
                    ok = entry.get("success", 0)
                    fail = entry.get("failed", 0)
                    ts = entry.get("ts", "")
                    lines.append(f"  {ts} {sym} {d} | ✅{ok} ❌{fail}")
                self._send_text(chat_id, "\n".join(lines), parse_mode="Markdown")
            except Exception as exc:
                self._send_text(chat_id, f"Error: {exc}")
            return

        if command in {"ask", "chat", "q"}:
            question = str(args or "").strip()
            if not question:
                self._send_text(
                    chat_id,
                    "💬 ถามอะไรก็ได้เกี่ยวกับระบบ:\n\n"
                    "/ask วันนี้ระบบเป็นยังไง?\n"
                    "/ask which families are winning?\n"
                    "/ask should I approve the ETH trial?\n"
                    "/ask อธิบาย XAU regime ตอนนี้",
                )
                return
            # Show typing indicator via send_chat_action
            try:
                from config import config as _cfg
                _chat_id_str = str(getattr(_cfg, "TELEGRAM_CHAT_ID", "") or "").strip()
                if _chat_id_str:
                    self._api_post("sendChatAction", {"chat_id": int(_chat_id_str), "action": "typing"})
            except Exception:
                pass
            try:
                from openclaw.chat_agent import ask as _ask
                self._send_text(chat_id, f"🤔 กำลังวิเคราะห์...")
                answer = _ask(question)
                self._send_text(chat_id, f"💬 {answer}")
            except Exception as exc:
                self._send_text(chat_id, f"❌ Chat error: {exc}")
            return

        if command in {"openclaw_version", "openclaw_status"}:
            try:
                from openclaw.version_guard import get_state as _vg_state, check_and_notify as _vg_check
                state = _vg_state()
                installed = state.get("installed_version", "unknown")
                latest = state.get("latest_version", "unknown")
                update_available = bool(state.get("update_available"))
                notified_at = state.get("notified_at", "never")
                updated_at = state.get("updated_at", "never")
                lines = [
                    "🦞 *OpenClaw Version Status*",
                    f"  Installed: `{installed}`",
                    f"  Latest:    `{latest}`",
                    f"  Update available: {'✅ YES' if update_available else '✅ Up to date'}",
                    f"  Last notified: {notified_at[:16] if notified_at != 'never' else 'never'}",
                    f"  Last updated:  {updated_at[:16] if updated_at != 'never' else 'never'}",
                ]
                if update_available:
                    lines.append(f"\nSend /update_openclaw to upgrade to {latest}")
                self._send_text(chat_id, "\n".join(lines), parse_mode="Markdown")
            except Exception as exc:
                self._send_text(chat_id, f"Version guard error: {exc}")
            return

        if command in {"update_openclaw"}:
            if not is_admin:
                self._send_text(chat_id, "/update_openclaw is admin-only.")
                return
            try:
                from openclaw.version_guard import get_state as _vg_state, do_update as _vg_update
                state = _vg_state()
                latest = state.get("latest_version", "unknown")
                installed = state.get("installed_version", "unknown")
                if latest == installed and not state.get("update_available"):
                    self._send_text(chat_id, f"✅ Already at latest: `{installed}`", parse_mode="Markdown")
                    return
                self._send_text(chat_id, f"⏳ Updating openclaw `{installed}` → `{latest}`...", parse_mode="Markdown")
                result = _vg_update()
                if result["ok"]:
                    self._send_text(
                        chat_id,
                        f"✅ *OpenClaw updated to {result['version']}*\n"
                        f"Gateway restarted. Qwen + new features active.",
                        parse_mode="Markdown"
                    )
                else:
                    self._send_text(chat_id, f"❌ Update failed: {result.get('error', 'unknown')}")
            except Exception as exc:
                self._send_text(chat_id, f"Update error: {exc}")
            return

        if command in {"skip_openclaw"}:
            if not is_admin:
                self._send_text(chat_id, "/skip_openclaw is admin-only.")
                return
            version_to_skip = str(args or "").strip()
            try:
                from openclaw.version_guard import _load_state as _vg_load, _save_state as _vg_save
                state = _vg_load()
                state["notified_version"] = version_to_skip or state.get("latest_version", "")
                state["update_available"] = False
                _vg_save(state)
                self._send_text(chat_id, f"⏭ Skipped openclaw {version_to_skip}. Next update will notify again.")
            except Exception as exc:
                self._send_text(chat_id, f"Skip error: {exc}")
            return

        suggestion = self._suggest_command(command)
        if suggestion and suggestion != command:
            self._send_text_localized(
                chat_id,
                "command_autocorrected",
                lang=lang,
                suggested=suggestion,
                original=command,
            )
            self._handle_admin_command(chat_id, user_id, suggestion, args, is_admin, lang=lang)
            return
        self._send_text(chat_id, f"Unknown command: /{command}\nUse /help")

    def _handle_message(self, msg: dict) -> None:
        text = (msg.get("text") or "").strip()
        if not text:
            return
        command, args = self._extract_command(text)
        chat_id = msg.get("chat", {}).get("id")
        if chat_id is None:
            return

        from_id = msg.get("from", {}).get("id")
        if from_id is None:
            return
        is_admin = self._is_admin(msg)
        cid = int(chat_id)
        uid = int(from_id)
        self._chat_user_map[cid] = uid
        try:
            frm = msg.get("from", {}) or {}
            chat = msg.get("chat", {}) or {}
            access_manager.record_telegram_user_activity(
                user_id=uid,
                chat_id=cid,
                username=str(frm.get("username") or ""),
                first_name=str(frm.get("first_name") or ""),
                last_name=str(frm.get("last_name") or ""),
                is_bot=bool(frm.get("is_bot", False)),
                chat_type=str(chat.get("type") or ""),
            )
        except Exception as e:
            logger.debug("[AdminBot] record_telegram_user_activity failed uid=%s err=%s", uid, e)
        self._hydrate_chat_lang_preference(cid, uid)

        # Let users explicitly choose a preferred reply language (Thai/English/Deutsch).
        lang_source = (args or "").strip() if command else text
        pending_pref = bool(self._chat_lang_prompt_pending.get(cid))
        pref_choice = self._parse_language_preference(lang_source or text, pending=pending_pref)
        if pref_choice:
            self._set_chat_lang_preference(cid, pref_choice, user_id=uid)
            self._send_text(cid, self._language_pref_saved_text(pref_choice))
            low = (lang_source or text).strip().lower()
            has_other_intent = any(k in low for k in (
                "scan", "status", "mt5", "order", "position", "research", "plan", "upgrade",
                "สแกน", "สถานะ", "เช็ค", "ออเดอร์", "โพสิชั่น", "แพ็กเกจ", "คำสั่ง",
                "prüf", "status", "auftrag", "position",
            ))
            if (not command) and (not has_other_intent):
                return

        if command and (not (args or "").strip()):
            detected_lang = self._lang_for_chat(cid)
        else:
            detected_lang = self._remember_chat_lang(cid, lang_source or text)
        reply_lang = self._lang_for_chat(cid) or detected_lang

        if not command:
            if self._try_handle_pending_intent_confirm(cid, uid, text, is_admin, reply_lang):
                self._maybe_offer_language_preference(cid, text, None, reply_lang)
                return

        if not command:
            if self._try_handle_pending_slot(cid, uid, text, is_admin, reply_lang):
                self._maybe_offer_language_preference(cid, text, None, reply_lang)
                return

        if not command:
            self._handle_natural_language(cid, uid, text, is_admin, lang=reply_lang)
            self._maybe_offer_language_preference(cid, text, None, reply_lang)
            return

        try:
            self._handle_admin_command(cid, uid, command, args, is_admin, lang=reply_lang)
            self._maybe_offer_language_preference(cid, lang_source, command, reply_lang)
        except Exception as e:
            logger.error(f"[AdminBot] Command /{command} failed: {e}", exc_info=True)
            self._send_text_localized(cid, "command_failed", lang=reply_lang, err=str(e)[:200])

    def _poll_loop(self) -> None:
        logger.info("[AdminBot] Poll loop started")
        while self.running:
            data = self._api_get("getUpdates", params={
                "timeout": 25,
                "offset": self._offset,
                "allowed_updates": '["message"]',
            }, timeout=35)
            if not data:
                time.sleep(2)
                continue

            for upd in data.get("result", []):
                try:
                    self._offset = int(upd["update_id"]) + 1
                    msg = upd.get("message") or upd.get("edited_message")
                    if msg:
                        self._handle_message(msg)
                except Exception as e:
                    logger.warning(f"[AdminBot] Update handling error: {e}")

        logger.info("[AdminBot] Poll loop stopped")

    def _skip_historical_updates(self) -> None:
        """
        Advance offset past any queued historical updates so old commands are
        not replayed after process restart.
        """
        offset = self._offset
        latest_seen = None
        for _ in range(20):
            data = self._api_get(
                "getUpdates",
                params={"timeout": 0, "limit": 100, "offset": offset},
                timeout=10,
            )
            if not data:
                break
            updates = data.get("result", [])
            if not updates:
                break
            latest_seen = int(updates[-1]["update_id"]) + 1
            offset = latest_seen
            if len(updates) < 100:
                break
        if latest_seen is not None:
            self._offset = latest_seen

    def start(self) -> None:
        if not self.enabled:
            logger.info("[AdminBot] Disabled (missing Telegram config)")
            return
        if self.running:
            return
        self._refresh_identity()
        has_conflict, detail = self._detect_polling_conflict()
        if has_conflict:
            logger.error("[AdminBot] Duplicate bot instance detected at startup: %s", detail)
            logger.error(
                "[AdminBot] Poll loop NOT started. Stop the other 'python main.py monitor' "
                "process using this same Telegram bot token."
            )
            if (not self._startup_conflict_notified) and config.TELEGRAM_CHAT_ID and config.TELEGRAM_CHAT_ID.lstrip("-").isdigit():
                self._startup_conflict_notified = True
                self._send_text(
                    int(config.TELEGRAM_CHAT_ID),
                    "Admin bot startup blocked: duplicate bot instance detected (Telegram polling conflict 409).\n"
                    "Action: keep only one running monitor process for this bot token.",
                )
            return
        self._skip_historical_updates()
        if not self._admin_ids:
            logger.warning("[AdminBot] No admin IDs configured; fallback private-chat auth only")
        self.running = True
        self._thread = threading.Thread(target=self._poll_loop, daemon=True, name="DexterAdminBot")
        self._thread.start()
        logger.info("[AdminBot] Started")

    def stop(self) -> None:
        self.running = False
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=5)
        logger.info("[AdminBot] Stopped")


admin_bot = TelegramAdminBot()
