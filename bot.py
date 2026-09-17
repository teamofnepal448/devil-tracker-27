from __future__ import annotations


import asyncio
import json
import logging
import os
import re
import threading
import time
import uuid
from dataclasses import dataclass, asdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Literal

from quart import Quart, jsonify, request
from telethon import TelegramClient, errors, events
from telethon.sessions import StringSession
from telethon.tl.functions.channels import GetFullChannelRequest
from telethon.tl.functions.messages import CheckChatInviteRequest, GetDialogFiltersRequest
from telethon.tl.types import (
    ChatInvite,
    ChatInviteAlready,
    DialogFilter,
    InputMessagesFilterPinned,
    MessageEntityTextUrl,
    MessageEntityUrl,
)

try:
    from google import genai
    from google.genai import types as genai_types
except Exception:  # pragma: no cover - optional until dependency is installed
    genai = None
    genai_types = None

try:
    from pydantic import BaseModel, Field
except Exception:  # pragma: no cover
    BaseModel = None  # type: ignore
    Field = None  # type: ignore


# ============================================================
# CONFIGURATION (HARDCODED DEFAULTS ATTACHED)
# ============================================================

LOCAL_TZ = timezone(timedelta(hours=5, minutes=30))
APP_NAME = "Devil Cross-Promotion Engine + JARVIS V2"
SCHEMA_VERSION = 2


def env_bool(name: str, default: bool = False) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


def env_int(name: str, default: int = 0) -> int:
    raw = os.getenv(name)
    if raw is None or not raw.strip():
        return default
    try:
        return int(raw.strip())
    except ValueError as exc:
        raise RuntimeError(f"{name} must be an integer") from exc


def parse_admin_ids(raw: str) -> set[int]:
    result: set[int] = set()
    for part in (raw or "").split(","):
        part = part.strip()
        if not part:
            continue
        try:
            result.add(int(part))
        except ValueError:
            raise RuntimeError(f"Invalid Telegram admin ID: {part!r}")
    return result


# Credentials and Configuration attached directly
API_ID = env_int("API_ID", 36094172)
API_HASH = os.getenv("API_HASH", "ff6eee1bcccf82daea88c63c45b6b546").strip()
SESSION_STRING = os.getenv("SESSION_STRING", "").strip()
SESSION_NAME = os.getenv("SESSION_NAME", "devil_main_session")

GEMINI_API_KEY = os.getenv("GEMINI_API_KEY", "").strip()
GEMINI_MODEL = os.getenv("GEMINI_MODEL", "gemini-2.5-flash").strip()

TARGET_MAIN_CHANNEL = env_int("TARGET_MAIN_CHANNEL", 1716302260)
FOLDER_TARGET_NAME = os.getenv("FOLDER_TARGET_NAME", "RAN X CROXX").strip()

STATE_DIR = Path(os.getenv("STATE_DIR", "/data" if Path("/data").exists() else "."))
STATE_FILE = Path(os.getenv("STATE_FILE", str(STATE_DIR / "jarvis_state.json")))

JARVIS_WEB_TOKEN = os.getenv("JARVIS_WEB_TOKEN", "Jv9!kP7#xQ2@Lm8$Zr4").strip()
AUTHORIZED_ADMINS = parse_admin_ids(os.getenv("AUTHORIZED_ADMINS", "8520210719"))

ENGINE_ROUND_RELOAD_SECONDS = max(10, env_int("ENGINE_ROUND_RELOAD_SECONDS", 30))
DEFERRED_RECHECK_SECONDS = max(10, env_int("DEFERRED_RECHECK_SECONDS", 60))
MONITOR_SECONDS = max(30, env_int("MONITOR_SECONDS", 300))
MONITOR_POLL_SECONDS = max(5, env_int("MONITOR_POLL_SECONDS", 10))
SECONDARY_DELAY_MIN = max(0, env_int("SECONDARY_DELAY_MIN", 45))
SECONDARY_DELAY_MAX = max(SECONDARY_DELAY_MIN, env_int("SECONDARY_DELAY_MAX", 120))
POST_SETTLE_SECONDS = max(0, env_int("POST_SETTLE_SECONDS", 2))
IDLE_BACKOFF_SECONDS = max(5, env_int("IDLE_BACKOFF_SECONDS", 10))
SCHEDULER_TICK_SECONDS = max(1, env_int("SCHEDULER_TICK_SECONDS", 5))
MAX_SCHEDULE_SECONDS = max(3600, env_int("MAX_SCHEDULE_SECONDS", 7 * 86400))


if not API_ID or not API_HASH:
    raise RuntimeError("API_ID/API_HASH missing.")
if not FOLDER_TARGET_NAME:
    raise RuntimeError("FOLDER_TARGET_NAME is required.")
if not TARGET_MAIN_CHANNEL:
    raise RuntimeError("TARGET_MAIN_CHANNEL is required.")
if not JARVIS_WEB_TOKEN:
    raise RuntimeError("JARVIS_WEB_TOKEN is required for protected web APIs.")


# ============================================================
# LOGGING
# ============================================================

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
logger = logging.getLogger("jarvis")


def now_utc() -> datetime:
    return datetime.now(timezone.utc)


def now_local() -> datetime:
    return datetime.now(LOCAL_TZ)


def iso(dt: datetime | None) -> str | None:
    return dt.astimezone(timezone.utc).isoformat() if dt else None


def parse_iso(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(value)
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(timezone.utc)
    except ValueError:
        return None


def safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


# ============================================================
# APPLICATION / TELEGRAM
# ============================================================

app = Quart(__name__)

if SESSION_STRING:
    client = TelegramClient(StringSession(SESSION_STRING), API_ID, API_HASH)
else:
    session_path = STATE_DIR / SESSION_NAME
    client = TelegramClient(str(session_path), API_ID, API_HASH)


# ============================================================
# GLOBAL RUNTIME STATE
# ============================================================

CROSS_LOOP_RUNNING = False
LOOP_END_TIME: datetime | None = None
CHANNELS_QUEUE: list[int] = []
DEFERRED_QUEUE: dict[str, dict[str, Any]] = {}
PERMANENT_BAD_CHANNELS: set[int] = set()
CURRENT_SOURCE_MSGS: list[Any] = []
CURRENT_SOURCE_CHAT_ID: int | None = None
ME_ID: int | None = None
RUN_TASK: asyncio.Task | None = None
SCHEDULER_TASK: asyncio.Task | None = None
AI_BUSY = False
AI_LAST_DECISION: dict[str, Any] | None = None
AI_LAST_RESULT: str | None = None
STARTED_AT: datetime | None = None
LAST_ERROR: str | None = None

status_tracker: dict[str, Any] = {
    "total": 0,
    "completed": 0,
    "skipped": 0,
    "remaining": 0,
    "current_channel": "None",
    "timer_end": "None",
    "last_action": None,
}

LINK_RESOLVE_CACHE: dict[str, tuple[float, Any]] = {}

engine_lock = asyncio.Lock()
state_lock = asyncio.Lock()
ai_lock = asyncio.Lock()

file_lock = threading.RLock()


# ============================================================
# PERSISTENT STATE
# ============================================================

DEFAULT_STATE: dict[str, Any] = {
    "schema_version": SCHEMA_VERSION,
    "engine": {
        "running": False,
        "end_time": None,
    },
    "queue": [],
    "deferred_queue": {},
    "permanent_bad_channels": [],
    "source": {
        "chat_id": None,
        "message_ids": [],
    },
    "scheduler_jobs": [],
    "confirmations": {},
    "analytics": {},
    "runtime": {
        "total_processed": 0,
        "total_skipped": 0,
        "current_channel": "None",
        "last_error": None,
        "started_at": None,
    },
}


def deepcopy_json(data: Any) -> Any:
    return json.loads(json.dumps(data, ensure_ascii=False))


def normalize_state(raw: Any) -> dict[str, Any]:
    if not isinstance(raw, dict):
        return deepcopy_json(DEFAULT_STATE)

    if raw.get("schema_version") != SCHEMA_VERSION:
        analytics: dict[str, Any] = {}
        for key, value in raw.items():
            if key == "saved_queue_state":
                continue
            if isinstance(value, dict):
                analytics[key] = value
        migrated = deepcopy_json(DEFAULT_STATE)
        migrated["analytics"] = analytics
        old_queue = raw.get("saved_queue_state", [])
        migrated["queue"] = [safe_int(x) for x in old_queue if str(x).lstrip("-").isdigit()]
        return migrated

    state = deepcopy_json(DEFAULT_STATE)
    for key in state:
        if key in raw:
            state[key] = raw[key]

    if not isinstance(state["engine"], dict):
        state["engine"] = deepcopy_json(DEFAULT_STATE["engine"])
    if not isinstance(state["queue"], list):
        state["queue"] = []
    if not isinstance(state["deferred_queue"], dict):
        state["deferred_queue"] = {}
    if not isinstance(state["permanent_bad_channels"], list):
        state["permanent_bad_channels"] = []
    if not isinstance(state["source"], dict):
        state["source"] = deepcopy_json(DEFAULT_STATE["source"])
    if not isinstance(state["source"].get("message_ids"), list):
        state["source"]["message_ids"] = []
    if not isinstance(state["scheduler_jobs"], list):
        state["scheduler_jobs"] = []
    if not isinstance(state["confirmations"], dict):
        state["confirmations"] = {}
    if not isinstance(state["analytics"], dict):
        state["analytics"] = {}
    if not isinstance(state["runtime"], dict):
        state["runtime"] = deepcopy_json(DEFAULT_STATE["runtime"])

    return state


def load_state_sync() -> dict[str, Any]:
    STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
    with file_lock:
        if not STATE_FILE.exists():
            state = deepcopy_json(DEFAULT_STATE)
            save_state_sync(state)
            return state

        try:
            raw = json.loads(STATE_FILE.read_text(encoding="utf-8"))
            return normalize_state(raw)
        except Exception as primary_exc:
            logger.error("[STORAGE] State file is invalid: %s", primary_exc)
            backup = STATE_FILE.with_suffix(STATE_FILE.suffix + ".bak")
            if backup.exists():
                try:
                    raw = json.loads(backup.read_text(encoding="utf-8"))
                    logger.warning("[STORAGE] Recovered state from backup file")
                    return normalize_state(raw)
                except Exception as backup_exc:
                    logger.error("[STORAGE] Backup state is also invalid: %s", backup_exc)
            return deepcopy_json(DEFAULT_STATE)


def save_state_sync(state: dict[str, Any]) -> None:
    STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
    payload = normalize_state(state)
    temp_file = STATE_FILE.with_suffix(STATE_FILE.suffix + ".tmp")
    backup = STATE_FILE.with_suffix(STATE_FILE.suffix + ".bak")

    with file_lock:
        if STATE_FILE.exists():
            try:
                backup.write_bytes(STATE_FILE.read_bytes())
            except Exception:
                logger.warning("[STORAGE] Could not refresh state backup", exc_info=True)
        temp_file.write_text(
            json.dumps(payload, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        os.replace(temp_file, STATE_FILE)


def state_snapshot_sync() -> dict[str, Any]:
    return load_state_sync()


def persist_runtime_state() -> None:
    state = state_snapshot_sync()
    state["schema_version"] = SCHEMA_VERSION
    state["engine"]["running"] = CROSS_LOOP_RUNNING
    state["engine"]["end_time"] = iso(LOOP_END_TIME)
    state["queue"] = list(CHANNELS_QUEUE)
    state["deferred_queue"] = deepcopy_json(DEFERRED_QUEUE)
    state["permanent_bad_channels"] = sorted(PERMANENT_BAD_CHANNELS)
    state["source"] = {
        "chat_id": CURRENT_SOURCE_CHAT_ID,
        "message_ids": [getattr(m, "id", 0) for m in CURRENT_SOURCE_MSGS],
    }
    state["runtime"] = {
        "total_processed": status_tracker.get("completed", 0),
        "total_skipped": status_tracker.get("skipped", 0),
        "current_channel": status_tracker.get("current_channel", "None"),
        "last_error": LAST_ERROR,
        "started_at": iso(STARTED_AT),
    }
    save_state_sync(state)


def get_analytics_sync() -> dict[str, Any]:
    return load_state_sync().get("analytics", {})


def save_analytics_sync(analytics: dict[str, Any]) -> None:
    state = load_state_sync()
    state["analytics"] = analytics
    save_state_sync(state)


def update_analytics(channel_id: int, title: str, pending_request_delta: int) -> None:
    analytics = get_analytics_sync()
    key = str(channel_id)
    entry = analytics.get(key)
    if not isinstance(entry, dict):
        entry = {
            "title": title,
            "runs": 0,
            "total_joins": 0,
            "time_history": [],
        }
        analytics[key] = entry

    entry.setdefault("title", title)
    entry.setdefault("runs", 0)
    entry.setdefault("total_joins", 0)
    entry.setdefault("time_history", [])
    entry["title"] = title or entry["title"]
    entry["runs"] = safe_int(entry["runs"]) + 1
    entry["total_joins"] = safe_int(entry["total_joins"]) + max(0, pending_request_delta)
    entry["time_history"].append({
        "timestamp": now_local().strftime("%Y-%m-%d %H:%M:%S"),
        "hour": now_local().strftime("%I:%M %p"),
        "pending_request_delta": max(0, pending_request_delta),
    })
    entry["time_history"] = entry["time_history"][-200:]
    save_analytics_sync(analytics)


def load_runtime_from_state() -> None:
    global CROSS_LOOP_RUNNING, LOOP_END_TIME, CHANNELS_QUEUE
    global DEFERRED_QUEUE, PERMANENT_BAD_CHANNELS, CURRENT_SOURCE_CHAT_ID
    global STARTED_AT, LAST_ERROR

    state = load_state_sync()
    engine = state["engine"]
    CROSS_LOOP_RUNNING = bool(engine.get("running", False))
    LOOP_END_TIME = parse_iso(engine.get("end_time"))
    CHANNELS_QUEUE = [safe_int(x) for x in state["queue"] if safe_int(x) != 0]
    DEFERRED_QUEUE = state["deferred_queue"]
    PERMANENT_BAD_CHANNELS = {safe_int(x) for x in state["permanent_bad_channels"] if safe_int(x) != 0}
    CURRENT_SOURCE_CHAT_ID = safe_int(state["source"].get("chat_id"), 0) or None
    STARTED_AT = parse_iso(state["runtime"].get("started_at"))
    LAST_ERROR = state["runtime"].get("last_error")

    status_tracker["completed"] = safe_int(state["runtime"].get("total_processed"))
    status_tracker["skipped"] = safe_int(state["runtime"].get("total_skipped"))
    status_tracker["remaining"] = len(CHANNELS_QUEUE)
    status_tracker["current_channel"] = state["runtime"].get("current_channel", "None")
    status_tracker["timer_end"] = LOOP_END_TIME.strftime("%I:%M %p (%d-%b)") if LOOP_END_TIME else (
        "24/7 Unlimited Mode" if CROSS_LOOP_RUNNING else "None"
    )


async def save_runtime_state() -> None:
    async with state_lock:
        await asyncio.to_thread(persist_runtime_state)


# ============================================================
# TELEGRAM ERROR HANDLING
# ============================================================

class PermissionDenied(Exception):
    pass


class FloodWaitExceeded(Exception):
    pass


async def tg_call(func, *args, retries: int = 3, **kwargs):
    last_error: Exception | None = None
    for attempt in range(1, retries + 1):
        try:
            return await func(*args, **kwargs)
        except errors.FloodWaitError as exc:
            wait_seconds = max(0, int(exc.seconds))
            logger.warning(
                "[TELEGRAM] FloodWait: waiting exactly %ss (attempt %s/%s)",
                wait_seconds,
                attempt,
                retries,
            )
            await asyncio.sleep(wait_seconds)
            last_error = exc
            if attempt == retries:
                raise FloodWaitExceeded(
                    f"Telegram FloodWait persisted after {retries} attempts ({wait_seconds}s requested)."
                ) from exc
        except (
            errors.ChatAdminRequiredError,
            errors.ChannelPrivateError,
            errors.ChatWriteForbiddenError,
            errors.UserBannedInChannelError,
            errors.UserNotParticipantError,
        ) as exc:
            raise PermissionDenied(str(exc)) from exc
        except (errors.RPCError, OSError) as exc:
            last_error = exc
            if attempt == retries:
                raise
            logger.warning(
                "[TELEGRAM] RPC/connection error; retrying %s/%s: %s",
                attempt,
                retries,
                exc,
            )
            await asyncio.sleep(min(2 * attempt, 5))
    if last_error:
        raise last_error
    raise RuntimeError("Telegram call failed without an exception")


# ============================================================
# TELEGRAM HELPERS / LINK ENGINE
# ============================================================


def clean_url(url: str) -> str:
    if not url:
        return ""
    value = str(url).strip()
    if value.startswith("ps://"):
        value = "https://" + value[5:]
    elif value.startswith("tps://"):
        value = "https://" + value[6:]
    elif value.startswith("s://"):
        value = "https://" + value[4:]

    match = re.search(r"(?:https?://)?(?:www\.)?(?:t\.me|telegram\.me)/(.*)", value, re.I)
    if match:
        return f"https://t.me/{match.group(1)}"
    if value.startswith("@"):
        return f"https://t.me/{value[1:]}"
    return value


def extract_link_token(link: str) -> str:
    value = clean_url(link).rstrip("/")
    match = re.search(
        r"(?:t\.me|telegram\.me)/(?:\+|joinchat/|addlist/)?([\w\-]+)",
        value,
        re.I,
    )
    if match:
        return match.group(1).lower()
    if value.startswith("@"):
        return value[1:].lower()
    return value.lower()


def slice_utf16(text: str, offset: int, length: int) -> str:
    raw = text.encode("utf-16-le")
    start = max(0, offset) * 2
    end = start + max(0, length) * 2
    return raw[start:end].decode("utf-16-le", errors="ignore")


def extract_links_from_message(msg: Any) -> list[str]:
    links: list[str] = []
    if not msg:
        return links

    if getattr(msg, "reply_markup", None):
        try:
            for row in getattr(msg.reply_markup, "rows", []):
                for button in getattr(row, "buttons", []):
                    url = getattr(button, "url", None)
                    if url:
                        links.append(clean_url(url))
        except Exception:
            logger.debug("[LINK] Failed to inspect buttons", exc_info=True)

    raw_text = getattr(msg, "raw_text", "") or getattr(msg, "message", "") or ""
    for entity in getattr(msg, "entities", None) or []:
        try:
            if isinstance(entity, MessageEntityTextUrl):
                url = getattr(entity, "url", None)
                if url:
                    links.append(clean_url(url))
            elif isinstance(entity, MessageEntityUrl):
                value = slice_utf16(raw_text, entity.offset, entity.length)
                if value:
                    links.append(clean_url(value))
        except Exception:
            logger.debug("[LINK] Entity parsing failed", exc_info=True)

    if raw_text:
        pattern = (
            r"(?:https?://|ps://|tps://|s://)?(?:www\.)?"
            r"(?:t\.me|telegram\.me)/"
            r"(?:\+[\w\-]+|joinchat/[\w\-]+|addlist/[\w\-]+|[\w\-]+)"
        )
        links.extend(clean_url(x) for x in re.findall(pattern, raw_text, re.I))
        links.extend(f"https://t.me/{x}" for x in re.findall(r"(?<!\w)@([\w\-]+)", raw_text))

    unique: list[str] = []
    seen: set[str] = set()
    for link in links:
        token = extract_link_token(link)
        if token and token not in seen:
            seen.add(token)
            unique.append(clean_url(link))
    return unique


async def resolve_link_id(link: str) -> int | str:
    token = extract_link_token(link)
    if not token:
        return "UNKNOWN"

    cached = LINK_RESOLVE_CACHE.get(token)
    if cached and time.monotonic() - cached[0] < 900:
        return cached[1]

    resolved: int | str = "UNKNOWN"
    try:
        invite = re.search(r"(?:t\.me|telegram\.me)/(?:\+|joinchat/)([\w\-]+)", link, re.I)
        if invite:
            result = await tg_call(client, CheckChatInviteRequest, invite.group(1), retries=2)
            if isinstance(result, (ChatInviteAlready, ChatInvite)):
                chat = getattr(result, "chat", None)
                resolved = getattr(chat, "id", "UNKNOWN") if chat else "UNKNOWN"
        elif "addlist/" in link.lower():
            resolved = "UNKNOWN"
        else:
            entity = await tg_call(client.get_entity, link, retries=2)
            resolved = getattr(entity, "id", "UNKNOWN")
    except (PermissionDenied, FloodWaitExceeded, errors.RPCError):
        resolved = "UNKNOWN"
    except Exception:
        logger.debug("[LINK] Entity resolution failed for %s", token, exc_info=True)
        resolved = "UNKNOWN"

    if isinstance(resolved, int):
        resolved = abs(resolved)
    LINK_RESOLVE_CACHE[token] = (time.monotonic(), resolved)
    return resolved


@dataclass
class Eligibility:
    state: Literal["eligible", "defer", "blocked", "skip"]
    target_link: str | None = None
    reason: str = ""


async def verify_channel_eligibility(entity: Any, messages: list[Any], bio: str) -> Eligibility:
    current_id = abs(safe_int(getattr(entity, "id", 0)))
    blacklist = (
        "no link",
        "no cross",
        "admin remove",
        "cross off",
        "no promo",
        "link not allowed",
    )

    for msg in messages:
        text = (getattr(msg, "raw_text", "") or getattr(msg, "message", "") or "").lower()
        if any(word in text for word in blacklist):
            return Eligibility("blocked", reason="blacklist phrase")

    candidates: list[str] = []
    for msg in messages:
        candidates.extend(extract_links_from_message(msg))

    seen: set[str] = set()
    for link in candidates:
        token = extract_link_token(link)
        if not token or token in seen:
            continue
        seen.add(token)
        resolved_id = await resolve_link_id(link)
        if resolved_id == current_id:
            return Eligibility("defer", target_link=clean_url(link), reason="channel already contains its own link")
        if resolved_id == "UNKNOWN":
            return Eligibility("defer", reason="unresolved Telegram link")
        return Eligibility("blocked", reason="another Telegram link already present")

    if bio:
        dummy = type("Dummy", (), {"raw_text": bio, "message": bio, "reply_markup": None, "entities": None})()
        for link in extract_links_from_message(dummy):
            resolved_id = await resolve_link_id(link)
            if resolved_id == current_id:
                return Eligibility("defer", target_link=clean_url(link), reason="own link in bio")
            if resolved_id == "UNKNOWN":
                return Eligibility("defer", reason="unresolved Telegram link in bio")
            return Eligibility("blocked", reason="another Telegram link in bio")

    username = getattr(entity, "username", None)
    if username:
        return Eligibility("eligible", target_link=f"https://t.me/{username}")
    return Eligibility("skip", reason="channel has no public username")


async def get_folder_channels(folder_name: str) -> list[int]:
    result = await tg_call(client, GetDialogFiltersRequest, retries=2)
    filters = getattr(result, "filters", result) or []
    target = folder_name.strip().lower()
    excluded: set[int] = set()
    included: set[int] = set()

    for dialog_filter in filters:
        if not isinstance(dialog_filter, DialogFilter):
            continue
        title_obj = getattr(dialog_filter, "title", None)
        title = str(getattr(title_obj, "text", title_obj) or "").strip().lower()
        if title != target:
            continue

        for peer in list(getattr(dialog_filter, "exclude_peers", []) or []):
            cid = getattr(peer, "channel_id", None)
            if cid:
                excluded.add(int(cid))
        for peer in list(getattr(dialog_filter, "include_peers", []) or []):
            cid = getattr(peer, "channel_id", None)
            if cid:
                included.add(int(cid))
        for peer in list(getattr(dialog_filter, "pinned_peers", []) or []):
            cid = getattr(peer, "channel_id", None)
            if cid:
                included.add(int(cid))

    return sorted(included - excluded)


async def get_pending_requests(channel: Any) -> int | None:
    try:
        full = await tg_call(client, GetFullChannelRequest, channel, retries=2)
        if isinstance(full, str):
            return None
        value = getattr(full.full_chat, "requests_pending", None)
        return safe_int(value) if value is not None else 0
    except Exception as exc:
        logger.warning("[ANALYTICS] Pending request check failed: %s", exc)
        return None


# ============================================================
# QUEUE / DEFERRED QUEUE
# ============================================================


def add_deferred(channel_id: int, reason: str) -> None:
    key = str(channel_id)
    item = DEFERRED_QUEUE.get(key, {})
    DEFERRED_QUEUE[key] = {
        "channel_id": channel_id,
        "attempts": safe_int(item.get("attempts")) + 1,
        "reason": reason[:300],
        "retry_after": iso(now_utc() + timedelta(seconds=DEFERRED_RECHECK_SECONDS)),
        "updated_at": iso(now_utc()),
    }


def pop_due_deferred() -> list[int]:
    due: list[int] = []
    current = now_utc()
    for key, item in list(DEFERRED_QUEUE.items()):
        retry_after = parse_iso(item.get("retry_after")) if isinstance(item, dict) else None
        if retry_after is None or retry_after <= current:
            cid = safe_int(item.get("channel_id"), safe_int(key)) if isinstance(item, dict) else safe_int(key)
            if cid:
                due.append(cid)
            DEFERRED_QUEUE.pop(key, None)
    return due


def queue_unique(channels: list[int]) -> None:
    existing = set(CHANNELS_QUEUE)
    for channel_id in channels:
        if channel_id in existing or channel_id in PERMANENT_BAD_CHANNELS:
            continue
        if str(channel_id) in DEFERRED_QUEUE:
            continue
        if channel_id == abs(TARGET_MAIN_CHANNEL):
            continue
        CHANNELS_QUEUE.append(channel_id)
        existing.add(channel_id)


# ============================================================
# SOURCE MESSAGE RECOVERY
# ============================================================

async def recover_source_messages() -> bool:
    global CURRENT_SOURCE_MSGS, CURRENT_SOURCE_CHAT_ID
    state = load_state_sync()
    chat_id = safe_int(state["source"].get("chat_id"), 0) or None
    message_ids = [safe_int(x) for x in state["source"].get("message_ids", []) if safe_int(x)]
    if not chat_id or not message_ids:
        CURRENT_SOURCE_MSGS = []
        CURRENT_SOURCE_CHAT_ID = None
        return False

    try:
        messages = await tg_call(client.get_messages, chat_id, ids=message_ids, retries=2)
        if not isinstance(messages, list):
            messages = [messages] if messages else []
        messages = [m for m in messages if getattr(m, "id", None)]
        found_ids = {m.id for m in messages}
        if found_ids != set(message_ids):
            logger.warning("[SOURCE] Some saved source messages could not be recovered")
            return False
        CURRENT_SOURCE_CHAT_ID = chat_id
        CURRENT_SOURCE_MSGS = messages
        return True
    except Exception as exc:
        logger.error("[SOURCE] Recovery failed: %s", exc)
        return False


async def configure_source_from_event(event: Any) -> tuple[bool, str]:
    global CURRENT_SOURCE_MSGS, CURRENT_SOURCE_CHAT_ID
    if not event.is_reply:
        return False, "Reply to the source post to configure the source bundle."

    reply = await event.get_reply_message()
    if not reply:
        return False, "Could not load the replied source message."

    source = [reply]
    try:
        next_messages = await tg_call(
            client.get_messages,
            event.chat_id,
            min_id=reply.id,
            limit=2,
            reverse=True,
            retries=2,
        )
        if not isinstance(next_messages, list):
            next_messages = [next_messages] if next_messages else []
        for msg in next_messages:
            text = getattr(msg, "raw_text", "") or ""
            if text.strip().startswith("/"):
                continue
            if getattr(msg, "id", None):
                source.append(msg)
    except Exception as exc:
        logger.warning("[SOURCE] Could not load secondary source messages: %s", exc)

    CURRENT_SOURCE_MSGS = source
    CURRENT_SOURCE_CHAT_ID = safe_int(event.chat_id)
    await save_runtime_state()
    return True, f"Source configured with {len(source)} message(s)."


# ============================================================
# ENGINE OPERATIONS
# ============================================================


def format_timer() -> str:
    if LOOP_END_TIME:
        return LOOP_END_TIME.astimezone(LOCAL_TZ).strftime("%I:%M %p (%d-%b)")
    return "24/7 Unlimited Mode" if CROSS_LOOP_RUNNING else "None"


async def ensure_queue_loaded() -> tuple[bool, str]:
    global CHANNELS_QUEUE

    if CHANNELS_QUEUE:
        return True, "Saved queue loaded."

    channels = await get_folder_channels(FOLDER_TARGET_NAME)
    if not channels:
        return False, f"Folder '{FOLDER_TARGET_NAME}' has no eligible channels."
    queue_unique(channels)
    await save_runtime_state()
    return bool(CHANNELS_QUEUE), f"Loaded {len(CHANNELS_QUEUE)} channel(s)."


async def start_engine(duration_seconds: int | None = None, *, resume: bool = True) -> tuple[bool, str]:
    global CROSS_LOOP_RUNNING, LOOP_END_TIME, RUN_TASK, STARTED_AT

    async with engine_lock:
        if CROSS_LOOP_RUNNING:
            return False, "Engine is already running."
        if not CURRENT_SOURCE_MSGS:
            recovered = await recover_source_messages()
            if not recovered:
                return False, "No source messages are configured. Use /cross start with a reply to configure them."

        ok, message = await ensure_queue_loaded()
        if not ok:
            return False, message

        CROSS_LOOP_RUNNING = True
        STARTED_AT = now_utc()
        LOOP_END_TIME = now_utc() + timedelta(seconds=duration_seconds) if duration_seconds else None
        status_tracker["remaining"] = len(CHANNELS_QUEUE)
        status_tracker["timer_end"] = format_timer()
        status_tracker["last_action"] = "start"
        await save_runtime_state()

        if RUN_TASK and not RUN_TASK.done():
            return True, "Engine marked running; existing worker is active."
        RUN_TASK = asyncio.create_task(run_cross_loop(), name="cross-engine")
        return True, f"Engine started. Queue: {len(CHANNELS_QUEUE)}. Timer: {format_timer()}."


async def stop_engine(*, save: bool = True, clear_timer: bool = True) -> tuple[bool, str]:
    global CROSS_LOOP_RUNNING, LOOP_END_TIME, RUN_TASK, STARTED_AT

    async with engine_lock:
        CROSS_LOOP_RUNNING = False
        if clear_timer:
            LOOP_END_TIME = None
        STARTED_AT = None
        status_tracker["timer_end"] = format_timer()
        status_tracker["last_action"] = "stop"
        if save:
            await save_runtime_state()

        task = RUN_TASK
        RUN_TASK = None

    if task and not task.done():
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass
        except Exception as exc:
            logger.error("[ENGINE] Worker shutdown failed: %s", exc, exc_info=True)

    return True, "Engine stopped. Current state saved." if save else "Engine stopped."


async def reset_engine() -> tuple[bool, str]:
    global CHANNELS_QUEUE, DEFERRED_QUEUE, PERMANENT_BAD_CHANNELS, LOOP_END_TIME
    global CURRENT_SOURCE_MSGS, CURRENT_SOURCE_CHAT_ID, LAST_ERROR

    await stop_engine(save=False)
    CHANNELS_QUEUE = []
    DEFERRED_QUEUE = {}
    PERMANENT_BAD_CHANNELS.clear()
    LOOP_END_TIME = None
    LAST_ERROR = None
    status_tracker.update({
        "total": 0,
        "completed": 0,
        "skipped": 0,
        "remaining": 0,
        "current_channel": "None",
        "timer_end": "None",
        "last_action": "reset",
    })
    CURRENT_SOURCE_MSGS = []
    CURRENT_SOURCE_CHAT_ID = None
    await save_runtime_state()
    return True, "Engine reset completed. Queue, deferred queue, bad channels, and saved source were cleared."


# ============================================================
# ENGINE CORE
# ============================================================

async def scan_target_channel(channel_id: int) -> tuple[Any, Eligibility, str, list[Any]]:
    strict_id = int(f"-100{channel_id}" if not str(channel_id).startswith("-100") else channel_id)
    entity = await tg_call(client.get_entity, strict_id, retries=2)
    if not entity:
        raise RuntimeError("Target channel entity is unavailable")

    title = getattr(entity, "title", "Channel") or "Channel"
    messages: list[Any] = []
    async for msg in client.iter_messages(entity, limit=4):
        messages.append(msg)

    try:
        pinned = await tg_call(client.get_messages, entity, filter=InputMessagesFilterPinned(), limit=1, retries=2)
        if pinned and isinstance(pinned, list):
            messages.extend(pinned)
    except Exception:
        logger.debug("[ENGINE] Pinned-message scan failed for %s", title, exc_info=True)

    bio = ""
    try:
        full = await tg_call(client, GetFullChannelRequest, entity, retries=2)
        if not isinstance(full, str):
            bio = getattr(full.full_chat, "about", "") or ""
    except Exception:
        logger.debug("[ENGINE] Bio lookup failed for %s", title, exc_info=True)

    eligibility = await verify_channel_eligibility(entity, messages, bio)
    return entity, eligibility, title, messages


async def perform_cross_for_channel(channel_id: int) -> str:
    if not CURRENT_SOURCE_MSGS:
        raise RuntimeError("Source messages are not configured")

    entity, eligibility, title, _ = await scan_target_channel(channel_id)
    status_tracker["current_channel"] = title

    if eligibility.state == "defer":
        add_deferred(channel_id, eligibility.reason)
        return "deferred"
    if eligibility.state == "blocked":
        PERMANENT_BAD_CHANNELS.add(channel_id)
        status_tracker["skipped"] += 1
        return "blocked"
    if eligibility.state == "skip":
        status_tracker["skipped"] += 1
        return "skipped"
    if not eligibility.target_link:
        status_tracker["skipped"] += 1
        return "skipped"

    target_link = eligibility.target_link
    forwarded_ids: list[int] = []
    main_ids: list[int] = []
    secondary_task: asyncio.Task | None = None
    stop_secondary = asyncio.Event()

    before_requests = await get_pending_requests(TARGET_MAIN_CHANNEL)

    try:
        first = await tg_call(
            client.forward_messages,
            entity,
            CURRENT_SOURCE_MSGS[0],
            silent=False,
            retries=3,
        )
        if isinstance(first, list):
            first = first[0] if first else None
        first_id = getattr(first, "id", None)
        if not first_id:
            raise RuntimeError("Source message could not be forwarded")
        forwarded_ids.append(first_id)

        if POST_SETTLE_SECONDS:
            await asyncio.sleep(POST_SETTLE_SECONDS)

        drop_text = clean_url(target_link)
        main_message = await tg_call(
            client.send_message,
            TARGET_MAIN_CHANNEL,
            drop_text,
            silent=True,
            retries=3,
        )
        if not getattr(main_message, "id", None):
            raise RuntimeError("Main channel promo message was not created")
        main_ids.append(main_message.id)

        async def send_secondary() -> None:
            for msg in CURRENT_SOURCE_MSGS[1:]:
                delay = SECONDARY_DELAY_MIN
                if SECONDARY_DELAY_MAX > SECONDARY_DELAY_MIN:
                    delay = SECONDARY_DELAY_MIN + int(
                        (SECONDARY_DELAY_MAX - SECONDARY_DELAY_MIN) * (time.monotonic() % 1)
                    )
                if delay:
                    try:
                        await asyncio.wait_for(stop_secondary.wait(), timeout=delay)
                        return
                    except asyncio.TimeoutError:
                        pass

                if stop_secondary.is_set() or not CROSS_LOOP_RUNNING:
                    return

                check = await tg_call(client.get_messages, entity, ids=first_id, retries=2)
                if not check or getattr(check, "empty", False):
                    return

                try:
                    if msg.media:
                        sent = await tg_call(
                            client.send_message,
                            entity,
                            msg.message or "",
                            file=msg.media,
                            reply_to=first_id,
                            silent=False,
                            retries=3,
                        )
                    else:
                        sent = await tg_call(
                            client.send_message,
                            entity,
                            msg.message or "",
                            reply_to=first_id,
                            silent=False,
                            retries=3,
                        )
                    if getattr(sent, "id", None):
                        forwarded_ids.append(sent.id)
                except Exception as exc:
                    logger.error("[ENGINE] Secondary source post failed for %s: %s", title, exc)
                    return

        if len(CURRENT_SOURCE_MSGS) > 1:
            secondary_task = asyncio.create_task(send_secondary(), name=f"secondary-{channel_id}")

        started = time.monotonic()
        while CROSS_LOOP_RUNNING and (time.monotonic() - started) < MONITOR_SECONDS:
            await asyncio.sleep(MONITOR_POLL_SECONDS)

            check = await tg_call(client.get_messages, entity, ids=first_id, retries=2)
            if not check or getattr(check, "empty", False):
                break

            recent_main = await tg_call(client.get_messages, TARGET_MAIN_CHANNEL, limit=8, retries=2)
            if isinstance(recent_main, list):
                for message in recent_main:
                    if message.id in main_ids:
                        continue
                    for link in extract_links_from_message(message):
                        if extract_link_token(link) == extract_link_token(target_link):
                            main_ids.append(message.id)
                            break

    finally:
        stop_secondary.set()
        if secondary_task and not secondary_task.done():
            secondary_task.cancel()
            try:
                await secondary_task
            except asyncio.CancelledError:
                pass
            except Exception:
                logger.debug("[ENGINE] Secondary task cleanup error", exc_info=True)

        after_requests = await get_pending_requests(TARGET_MAIN_CHANNEL)
        if before_requests is not None and after_requests is not None:
            pending_delta = max(0, after_requests - before_requests)
            update_analytics(channel_id, title, pending_delta)

        if main_ids:
            try:
                await tg_call(client.delete_messages, TARGET_MAIN_CHANNEL, main_ids, retries=3)
            except Exception as exc:
                logger.error("[ENGINE] Main-message cleanup failed for %s: %s", title, exc)

        if forwarded_ids:
            try:
                await tg_call(client.delete_messages, entity, forwarded_ids, retries=3)
            except Exception as exc:
                logger.error("[ENGINE] Target-message cleanup failed for %s: %s", title, exc)

    return "completed"


async def replenish_queue_for_round() -> None:
    channels = await get_folder_channels(FOLDER_TARGET_NAME)
    if not channels:
        return
    queue_unique(channels)
    status_tracker["total"] = max(status_tracker.get("total", 0), len(CHANNELS_QUEUE))


async def run_cross_loop() -> None:
    global LAST_ERROR, CROSS_LOOP_RUNNING, LOOP_END_TIME

    logger.info("[ENGINE] Cross worker started")
    try:
        while CROSS_LOOP_RUNNING:
            if LOOP_END_TIME and now_utc() >= LOOP_END_TIME:
                logger.info("[ENGINE] Timer expired")
                CROSS_LOOP_RUNNING = False
                LOOP_END_TIME = None
                await save_runtime_state()
                break

            if not CHANNELS_QUEUE:
                due = pop_due_deferred()
                if due:
                    queue_unique(due)
                    await save_runtime_state()
                    if CHANNELS_QUEUE:
                        continue

                if DEFERRED_QUEUE:
                    await asyncio.sleep(DEFERRED_RECHECK_SECONDS)
                    continue

                try:
                    await replenish_queue_for_round()
                except Exception as exc:
                    LAST_ERROR = f"Folder scan failed: {exc}"
                    logger.error("[ENGINE] %s", LAST_ERROR, exc_info=True)
                    await save_runtime_state()
                    await asyncio.sleep(ENGINE_ROUND_RELOAD_SECONDS)
                    continue

                if not CHANNELS_QUEUE:
                    await asyncio.sleep(ENGINE_ROUND_RELOAD_SECONDS)
                    continue

            channel_id = CHANNELS_QUEUE[0]
            status_tracker["remaining"] = len(CHANNELS_QUEUE)

            try:
                result = await perform_cross_for_channel(channel_id)
                if CHANNELS_QUEUE and CHANNELS_QUEUE[0] == channel_id:
                    CHANNELS_QUEUE.pop(0)

                status_tracker["remaining"] = len(CHANNELS_QUEUE)
                if result == "completed":
                    status_tracker["completed"] += 1
                elif result in {"skipped", "blocked"}:
                    pass

                await save_runtime_state()
                await asyncio.sleep(IDLE_BACKOFF_SECONDS)
            except PermissionDenied as exc:
                PERMANENT_BAD_CHANNELS.add(channel_id)
                if CHANNELS_QUEUE and CHANNELS_QUEUE[0] == channel_id:
                    CHANNELS_QUEUE.pop(0)
                status_tracker["skipped"] += 1
                LAST_ERROR = f"Permission denied for {channel_id}: {exc}"
                logger.warning("[ENGINE] %s", LAST_ERROR)
                await save_runtime_state()
            except FloodWaitExceeded as exc:
                LAST_ERROR = str(exc)
                logger.error("[ENGINE] %s", LAST_ERROR)
                await save_runtime_state()
                CROSS_LOOP_RUNNING = False
                break
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                LAST_ERROR = f"Channel {channel_id}: {exc}"
                logger.error("[ENGINE] Recovered from channel error: %s", exc, exc_info=True)
                await save_runtime_state()
                await asyncio.sleep(IDLE_BACKOFF_SECONDS)
    except asyncio.CancelledError:
        await save_runtime_state()
        logger.info("[ENGINE] Cross worker cancelled safely")
        raise
    except Exception as exc:
        LAST_ERROR = f"Cross worker crashed: {exc}"
        logger.critical("[ENGINE] %s", LAST_ERROR, exc_info=True)
        CROSS_LOOP_RUNNING = False
        await save_runtime_state()
    finally:
        status_tracker["remaining"] = len(CHANNELS_QUEUE)
        logger.info("[ENGINE] Cross worker exited")


# ============================================================
# SCHEDULER
# ============================================================

@dataclass
class ScheduledJob:
    job_id: str
    action: str
    params: dict[str, Any]
    execute_at: str
    owner_id: int
    created_at: str
    status: str = "pending"


def load_jobs_sync() -> list[dict[str, Any]]:
    return list(load_state_sync().get("scheduler_jobs", []))


def save_jobs_sync(jobs: list[dict[str, Any]]) -> None:
    state = load_state_sync()
    state["scheduler_jobs"] = jobs[-500:]
    save_state_sync(state)


async def schedule_job(action: str, params: dict[str, Any], delay_seconds: int, owner_id: int) -> ScheduledJob:
    if delay_seconds < 0 or delay_seconds > MAX_SCHEDULE_SECONDS:
        raise ValueError(f"Delay must be between 0 and {MAX_SCHEDULE_SECONDS} seconds")

    execute_at = now_utc() + timedelta(seconds=delay_seconds)
    jobs = load_jobs_sync()

    for raw in jobs:
        if raw.get("status") not in {"pending", "running"}:
            continue
        if (
            raw.get("action") == action
            and safe_int(raw.get("owner_id")) == owner_id
            and raw.get("params") == params
            and abs((parse_iso(raw.get("execute_at")) or execute_at - timedelta(seconds=999999)).timestamp() - execute_at.timestamp()) < 5
        ):
            return ScheduledJob(**raw)

    job = ScheduledJob(
        job_id=uuid.uuid4().hex[:12],
        action=action,
        params=params,
        execute_at=iso(execute_at) or "",
        owner_id=owner_id,
        created_at=iso(now_utc()) or "",
        status="pending",
    )
    jobs.append(asdict(job))
    save_jobs_sync(jobs)
    return job


async def notify_user(user_id: int, text: str) -> None:
    try:
        await tg_call(client.send_message, user_id, text, retries=2)
    except Exception as exc:
        logger.warning("[SCHEDULER] Could not notify owner %s: %s", user_id, exc)


async def execute_scheduled_job(job: ScheduledJob) -> str:
    if job.action == "START_CROSS":
        delay = safe_int(job.params.get("duration_seconds"), 0) or None
        ok, message = await start_engine(delay)
        return message if ok else f"Start failed: {message}"
    if job.action == "STOP_CROSS":
        _, message = await stop_engine(save=True)
        return message
    return f"Unsupported scheduled action: {job.action}"


async def scheduler_loop() -> None:
    logger.info("[SCHEDULER] Started")
    while True:
        try:
            jobs = load_jobs_sync()
            changed = False
            now = now_utc()

            for raw in jobs:
                if raw.get("status") == "running":
                    raw["status"] = "pending"
                    changed = True

            for raw in jobs:
                if raw.get("status") != "pending":
                    continue
                execute_at = parse_iso(raw.get("execute_at"))
                if not execute_at or execute_at > now:
                    continue

                raw["status"] = "running"
                changed = True
                save_jobs_sync(jobs)

                job = ScheduledJob(**raw)
                try:
                    result = await execute_scheduled_job(job)
                    raw["status"] = "completed"
                    await notify_user(
                        job.owner_id,
                        f"🧠 JARVIS scheduled job `{job.job_id}` completed.\n\n{result}",
                    )
                except asyncio.CancelledError:
                    raw["status"] = "pending"
                    save_jobs_sync(jobs)
                    raise
                except Exception as exc:
                    raw["status"] = "failed"
                    raw["error"] = str(exc)[:1000]
                    logger.error("[SCHEDULER] Job %s failed: %s", job.job_id, exc, exc_info=True)
                    await notify_user(
                        job.owner_id,
                        f"❌ JARVIS scheduled job `{job.job_id}` failed: {exc}",
                    )
                changed = True
                save_jobs_sync(jobs)

            if changed:
                save_jobs_sync(jobs)
            await asyncio.sleep(SCHEDULER_TICK_SECONDS)
        except asyncio.CancelledError:
            logger.info("[SCHEDULER] Stopped")
            raise
        except Exception as exc:
            logger.error("[SCHEDULER] Loop error: %s", exc, exc_info=True)
            await asyncio.sleep(SCHEDULER_TICK_SECONDS)


# ============================================================
# JARVIS AI
# ============================================================

ALLOWED_ACTIONS = {
    "REPORT",
    "STATUS",
    "DIAGNOSTICS",
    "START_CROSS",
    "STOP_CROSS",
    "RESET_CROSS",
    "SCHEDULE_START",
    "SCHEDULE_STOP",
    "SAVE_STATE",
    "ASK_CONFIRMATION",
}


if BaseModel is not None:
    class AIPlan(BaseModel):
        action: Literal[
            "REPORT",
            "STATUS",
            "DIAGNOSTICS",
            "START_CROSS",
            "STOP_CROSS",
            "RESET_CROSS",
            "SCHEDULE_START",
            "SCHEDULE_STOP",
            "SAVE_STATE",
            "ASK_CONFIRMATION",
        ]
        reason: str = Field(default="", max_length=1000)
        message: str = Field(default="", max_length=2000)
        delay_seconds: int = Field(default=0, ge=0, le=604800)
        duration_seconds: int = Field(default=0, ge=0, le=604800)
else:
    AIPlan = None  # type: ignore


AI_SYSTEM_PROMPT = """
You are JARVIS, the planning layer of a Telegram automation service.

You do NOT have arbitrary code, shell, filesystem, or deployment access.
Return exactly one action from the allowlist below.
Never claim an action executed unless the application result says so.
Never suggest bypassing Telegram restrictions or FloodWaits.

Allowed actions:
REPORT
STATUS
DIAGNOSTICS
START_CROSS
STOP_CROSS
RESET_CROSS
SCHEDULE_START
SCHEDULE_STOP
SAVE_STATE
ASK_CONFIRMATION

Rules:
- 'cross start' -> START_CROSS.
- 'cross stop' -> STOP_CROSS.
- 'cross reset' -> RESET_CROSS.
- 'status' -> STATUS.
- 'diagnostics' -> DIAGNOSTICS.
- 'after N minutes/hours/days' for starting/stopping -> SCHEDULE_START or SCHEDULE_STOP.
- Put delays in delay_seconds.
- Do not invent missing configuration.
- For a clearly explicit user command, use the matching action directly.
- Use ASK_CONFIRMATION only when the user intent is ambiguous or requests an operation outside the allowlist.
"""


def parse_duration(text: str) -> int | None:
    if not text:
        return None
    matches = re.findall(
        r"(\d+(?:\.\d+)?)\s*(seconds?|secs?|s|minutes?|mins?|m|hours?|hrs?|h|days?|d)",
        text,
        re.I,
    )
    if not matches:
        return None

    total = 0.0
    for value, unit in matches:
        number = float(value)
        unit = unit.lower()
        if unit.startswith("s"):
            total += number
        elif unit.startswith("m"):
            total += number * 60
        elif unit.startswith("h"):
            total += number * 3600
        elif unit.startswith("d"):
            total += number * 86400
    return int(total) if total > 0 else None


def get_ai_snapshot() -> dict[str, Any]:
    jobs = load_jobs_sync()
    analytics = get_analytics_sync()
    return {
        "time": now_local().isoformat(),
        "engine_running": CROSS_LOOP_RUNNING,
        "timer_end": iso(LOOP_END_TIME),
        "queue_length": len(CHANNELS_QUEUE),
        "deferred_count": len(DEFERRED_QUEUE),
        "failed_count": len(PERMANENT_BAD_CHANNELS),
        "current_channel": status_tracker.get("current_channel"),
        "tracker": dict(status_tracker),
        "source_configured": bool(CURRENT_SOURCE_MSGS),
        "scheduler_jobs": [
            {
                "job_id": j.get("job_id"),
                "action": j.get("action"),
                "execute_at": j.get("execute_at"),
                "status": j.get("status"),
            }
            for j in jobs[-20:]
        ],
        "analytics_channels": len(analytics),
        "last_error": LAST_ERROR,
        "last_ai_decision": AI_LAST_DECISION,
        "last_ai_result": AI_LAST_RESULT,
    }


async def ask_gemini(instruction: str) -> dict[str, Any]:
    if not GEMINI_API_KEY:
        return {
            "action": "REPORT",
            "reason": "GEMINI_API_KEY is not configured.",
            "message": "Set GEMINI_API_KEY before using AI features.",
            "delay_seconds": 0,
            "duration_seconds": 0,
        }
    if genai is None or genai_types is None or AIPlan is None:
        return {
            "action": "REPORT",
            "reason": "Gemini SDK or Pydantic dependency is not installed.",
            "message": "Install requirements.txt before using AI features.",
            "delay_seconds": 0,
            "duration_seconds": 0,
        }

    prompt = (
        AI_SYSTEM_PROMPT
        + "\nUSER INSTRUCTION:\n"
        + instruction
        + "\n\nAPPLICATION SNAPSHOT:\n"
        + json.dumps(get_ai_snapshot(), ensure_ascii=False, default=str)
    )

    try:
        ai_client = genai.Client(api_key=GEMINI_API_KEY)
        response = await asyncio.to_thread(
            ai_client.models.generate_content,
            model=GEMINI_MODEL,
            contents=prompt,
            config=genai_types.GenerateContentConfig(
                response_mime_type="application/json",
                response_schema=AIPlan,
            ),
        )
        plan = AIPlan.model_validate_json(getattr(response, "text", ""))
        data = plan.model_dump()
        if data["action"] not in ALLOWED_ACTIONS:
            raise ValueError("Gemini returned a non-allowlisted action")
        data["delay_seconds"] = max(0, min(safe_int(data.get("delay_seconds")), MAX_SCHEDULE_SECONDS))
        data["duration_seconds"] = max(0, min(safe_int(data.get("duration_seconds")), MAX_SCHEDULE_SECONDS))
        return data
    except Exception as exc:
        logger.error("[GEMINI] Request/validation failed: %s", exc, exc_info=True)
        return {
            "action": "REPORT",
            "reason": f"Gemini request failed: {exc}",
            "message": "No automated action was executed.",
            "delay_seconds": 0,
            "duration_seconds": 0,
        }


async def deterministic_ai_plan(instruction: str) -> dict[str, Any] | None:
    text = instruction.strip().lower()
    duration = parse_duration(text)

    if re.search(r"\b(status|state)\b", text) and not re.search(r"start|stop|reset", text):
        return {"action": "STATUS", "reason": "Direct status command", "message": "", "delay_seconds": 0, "duration_seconds": 0}
    if re.search(r"\bdiagnostic(s)?\b|health check", text):
        return {"action": "DIAGNOSTICS", "reason": "Direct diagnostics command", "message": "", "delay_seconds": 0, "duration_seconds": 0}
    if re.search(r"\bcross\s+(reset|clear)\b|\breset\s+(queue|cross)\b", text):
        return {"action": "RESET_CROSS", "reason": "Direct reset command", "message": "", "delay_seconds": 0, "duration_seconds": 0}
    if re.search(r"\bcross\s+(stop|halt)\b|\bstop\s+(cross|engine)\b", text):
        if duration:
            return {"action": "SCHEDULE_STOP", "reason": "Delayed stop request", "message": "", "delay_seconds": duration, "duration_seconds": 0}
        return {"action": "STOP_CROSS", "reason": "Direct stop command", "message": "", "delay_seconds": 0, "duration_seconds": 0}
    if re.search(r"\bcross\s+(start|run)\b|\bstart\s+(cross|engine)\b", text) or text.startswith("start after"):
        if "after" in text and duration:
            return {"action": "SCHEDULE_START", "reason": "Delayed start request", "message": "", "delay_seconds": min(duration, MAX_SCHEDULE_SECONDS), "duration_seconds": 0}
        return {"action": "START_CROSS", "reason": "Direct start command", "message": "", "delay_seconds": 0, "duration_seconds": 0}
    if re.search(r"save\s+(state|queue)", text):
        return {"action": "SAVE_STATE", "reason": "Direct persistence command", "message": "", "delay_seconds": 0, "duration_seconds": 0}
    return None


async def ai_execute(plan: dict[str, Any], owner_id: int) -> str:
    global AI_LAST_RESULT
    action = str(plan.get("action", "REPORT")).upper()
    if action not in ALLOWED_ACTIONS:
        action = "REPORT"

    if action == "STATUS":
        result = format_status_text()
    elif action == "DIAGNOSTICS":
        result = await run_diagnostics()
    elif action == "START_CROSS":
        ok, message = await start_engine(plan.get("duration_seconds") or None)
        result = message
        if not ok:
            result = f"❌ {result}"
    elif action == "STOP_CROSS":
        _, result = await stop_engine(save=True)
    elif action == "RESET_CROSS":
        _, result = await reset_engine()
    elif action == "SAVE_STATE":
        await save_runtime_state()
        result = "State saved successfully."
    elif action == "SCHEDULE_START":
        job = await schedule_job(
            "START_CROSS",
            {"duration_seconds": safe_int(plan.get("duration_seconds"), 0)},
            safe_int(plan.get("delay_seconds"), 0),
            owner_id,
        )
        result = f"Scheduled START_CROSS as `{job.job_id}` for {job.execute_at}."
    elif action == "SCHEDULE_STOP":
        job = await schedule_job(
            "STOP_CROSS",
            {},
            safe_int(plan.get("delay_seconds"), 0),
            owner_id,
        )
        result = f"Scheduled STOP_CROSS as `{job.job_id}` for {job.execute_at}."
    elif action == "ASK_CONFIRMATION":
        confirmation_id = uuid.uuid4().hex[:10]
        state = load_state_sync()
        state["confirmations"][confirmation_id] = {
            "owner_id": owner_id,
            "plan": plan,
            "created_at": iso(now_utc()),
            "expires_at": iso(now_utc() + timedelta(minutes=10)),
        }
        save_state_sync(state)
        result = f"Confirmation required. Use `/ai confirm {confirmation_id}` within 10 minutes."
    else:
        result = plan.get("message") or plan.get("reason") or "No action needed."

    AI_LAST_RESULT = str(result)
    return str(result)


async def ai_process_instruction(instruction: str, owner_id: int) -> str:
    global AI_BUSY, AI_LAST_DECISION
    async with ai_lock:
        if AI_BUSY:
            return "🧠 JARVIS is already processing another request."
        AI_BUSY = True

    try:
        plan = await deterministic_ai_plan(instruction)
        if plan is None:
            plan = await ask_gemini(instruction)
        AI_LAST_DECISION = plan
        result = await ai_execute(plan, owner_id)
        return (
            f"🧠 **JARVIS**\n"
            f"• Action: `{plan.get('action')}`\n"
            f"• Reason: {plan.get('reason', 'N/A')}\n\n"
            f"🤖 **Result**\n{result}"
        )
    finally:
        async with ai_lock:
            AI_BUSY = False


# ============================================================
# STATUS / DIAGNOSTICS
# ============================================================


def format_status_text() -> str:
    uptime = "N/A"
    if STARTED_AT:
        elapsed = max(0, int((now_utc() - STARTED_AT).total_seconds()))
        uptime = str(timedelta(seconds=elapsed))

    jobs = load_jobs_sync()
    pending_jobs = sum(1 for j in jobs if j.get("status") == "pending")
    return (
        "📊 **JARVIS STATUS**\n\n"
        f"• Engine: **{'RUNNING' if CROSS_LOOP_RUNNING else 'IDLE'}**\n"
        f"• Queue: **{len(CHANNELS_QUEUE)}**\n"
        f"• Deferred: **{len(DEFERRED_QUEUE)}**\n"
        f"• Failed/blocked: **{len(PERMANENT_BAD_CHANNELS)}**\n"
        f"• Processed: **{status_tracker['completed']}**\n"
        f"• Skipped: **{status_tracker['skipped']}**\n"
        f"• Current: **{status_tracker['current_channel']}**\n"
        f"• Timer: **{format_timer()}**\n"
        f"• Scheduled pending: **{pending_jobs}**\n"
        f"• Uptime: **{uptime}**\n"
        f"• Last error: **{LAST_ERROR or 'None'}**"
    )


async def run_diagnostics() -> str:
    checks: list[tuple[str, bool, str]] = []

    checks.append(("Telegram connection", client.is_connected(), "connected" if client.is_connected() else "disconnected"))
    checks.append(("Gemini SDK", genai is not None and genai_types is not None, "installed" if genai else "missing"))
    checks.append(("Gemini key", bool(GEMINI_API_KEY), "configured" if GEMINI_API_KEY else "missing"))
    checks.append(("Source", bool(CURRENT_SOURCE_MSGS), "configured" if CURRENT_SOURCE_MSGS else "missing"))
    checks.append(("State file", STATE_FILE.parent.exists(), str(STATE_FILE)))
    checks.append(("Engine task", bool(RUN_TASK and not RUN_TASK.done()), "running" if RUN_TASK and not RUN_TASK.done() else "idle"))
    checks.append(("Scheduler task", bool(SCHEDULER_TASK and not SCHEDULER_TASK.done()), "running" if SCHEDULER_TASK and not SCHEDULER_TASK.done() else "idle"))

    if genai is not None and genai_types is not None and GEMINI_API_KEY and AIPlan is not None:
        try:
            ai_client = genai.Client(api_key=GEMINI_API_KEY)
            response = await asyncio.to_thread(
                ai_client.models.generate_content,
                model=GEMINI_MODEL,
                contents="Return a JSON object with action STATUS, reason 'diagnostic', message 'ok', delay_seconds 0, duration_seconds 0.",
                config=genai_types.GenerateContentConfig(
                    response_mime_type="application/json",
                    response_schema=AIPlan,
                ),
            )
            AIPlan.model_validate_json(getattr(response, "text", ""))
            checks.append(("Gemini API", True, f"{GEMINI_MODEL} responded"))
        except Exception as exc:
            checks.append(("Gemini API", False, str(exc)[:200]))
    else:
        checks.append(("Gemini API", False, "not configured"))

    lines = ["🩺 **JARVIS DIAGNOSTICS**"]
    for name, ok, detail in checks:
        lines.append(f"{'✅' if ok else '❌'} {name}: {detail}")
    lines.append(f"Queue={len(CHANNELS_QUEUE)}, Deferred={len(DEFERRED_QUEUE)}, Failed={len(PERMANENT_BAD_CHANNELS)}")
    return "\n".join(lines)


# ============================================================
# WEB AUTH
# ============================================================


def web_authorized() -> bool:
    header = request.headers.get("Authorization", "")
    if not header.startswith("Bearer "):
        return False
    token = header[7:].strip()
    return bool(token) and token == JARVIS_WEB_TOKEN


@app.route("/")
async def home():
    return jsonify({
        "status": "online",
        "engine": APP_NAME,
        "running": CROSS_LOOP_RUNNING,
        "ai_configured": bool(GEMINI_API_KEY),
    })


@app.route("/health")
async def health():
    return jsonify({"status": "ok", "telegram_connected": client.is_connected()})


@app.route("/api/status", methods=["GET"])
async def api_status():
    if not web_authorized():
        return jsonify({"status": "error", "message": "Unauthorized"}), 401
    return jsonify(get_ai_snapshot())


@app.route("/api/start", methods=["POST"])
async def api_start():
    if not web_authorized():
        return jsonify({"status": "error", "message": "Unauthorized"}), 401
    data = await request.get_json(silent=True) or {}
    duration = parse_duration(str(data.get("duration", ""))) if data.get("duration") else safe_int(data.get("duration_seconds"), 0) or None
    ok, message = await start_engine(duration)
    return jsonify({"status": "success" if ok else "error", "message": message, "queue_count": len(CHANNELS_QUEUE)})


@app.route("/api/stop", methods=["POST"])
async def api_stop():
    if not web_authorized():
        return jsonify({"status": "error", "message": "Unauthorized"}), 401
    _, message = await stop_engine(save=True)
    return jsonify({"status": "success", "message": message})


@app.route("/api/reset", methods=["POST"])
async def api_reset():
    if not web_authorized():
        return jsonify({"status": "error", "message": "Unauthorized"}), 401
    _, message = await reset_engine()
    return jsonify({"status": "success", "message": message})


@app.route("/api/diagnostics", methods=["GET"])
async def api_diagnostics():
    if not web_authorized():
        return jsonify({"status": "error", "message": "Unauthorized"}), 401
    return jsonify({"status": "success", "report": await run_diagnostics()})


# ============================================================
# TELEGRAM CONTROLLER
# ============================================================


def telegram_authorized(sender_id: int | None) -> bool:
    if not sender_id:
        return False
    if ME_ID is not None and sender_id == ME_ID:
        return True
    return sender_id in AUTHORIZED_ADMINS


async def handle_confirmation(event: Any, confirmation_id: str) -> None:
    state = load_state_sync()
    item = state["confirmations"].get(confirmation_id)
    if not item:
        await event.reply("❌ Confirmation not found or expired.")
        return

    if safe_int(item.get("owner_id")) != safe_int(event.sender_id):
        await event.reply("❌ This confirmation belongs to another authorized user.")
        return

    expires = parse_iso(item.get("expires_at"))
    if not expires or expires < now_utc():
        state["confirmations"].pop(confirmation_id, None)
        save_state_sync(state)
        await event.reply("⌛ Confirmation expired.")
        return

    state["confirmations"].pop(confirmation_id, None)
    save_state_sync(state)
    result = await ai_execute(item.get("plan", {}), safe_int(event.sender_id))
    await event.reply(result[:4000])


@client.on(events.NewMessage())
async def controller(event: Any) -> None:
    global ME_ID
    try:
        if ME_ID is None:
            me = await client.get_me()
            ME_ID = getattr(me, "id", None)
        if not telegram_authorized(event.sender_id):
            return
        text = (event.raw_text or "").strip()
        if not text:
            return
        lower = text.lower()

        if lower.startswith("/ai confirm "):
            await handle_confirmation(event, text.split(maxsplit=2)[2].strip())
            return

        if lower == "/ai" or lower.startswith("/ai "):
            instruction = text[3:].strip()
            if not instruction:
                await event.reply(
                    "🧠 **JARVIS ready**\n\n"
                    "`/ai status`\n"
                    "`/ai cross start`\n"
                    "`/ai cross stop`\n"
                    "`/ai cross reset`\n"
                    "`/ai diagnostics`\n"
                    "`/ai cross start after 1 hour`"
                )
                return
            result = await ai_process_instruction(instruction, safe_int(event.sender_id))
            await event.reply(result[:4000])
            return

        if lower.startswith("/cross start"):
            if CROSS_LOOP_RUNNING:
                await event.reply("⚠️ Engine is already running.")
                return
            ok, msg = await configure_source_from_event(event)
            if not ok:
                await event.reply(f"⚠️ {msg}")
                return
            duration_text = text[len("/cross start"):].strip()
            duration = parse_duration(duration_text)
            started, result = await start_engine(duration)
            await event.reply(("✅ " if started else "❌ ") + result)
            return

        if lower.startswith("/cross stop"):
            _, msg = await stop_engine(save=True)
            await event.reply("🛑 " + msg)
            return

        if lower.startswith("/cross reset"):
            _, msg = await reset_engine()
            await event.reply("🔄 " + msg)
            return

        if lower.startswith("/status"):
            await event.reply(format_status_text()[:4000])
            return

    except asyncio.CancelledError:
        raise
    except Exception as exc:
        global LAST_ERROR
        LAST_ERROR = str(exc)[:500]
        logger.error("[CONTROLLER] %s", exc, exc_info=True)
        try:
            await event.reply("❌ JARVIS controller error. Check diagnostics/logs.")
        except Exception:
            pass


# ============================================================
# QUART LIFECYCLE
# ============================================================

@app.before_serving
async def startup() -> None:
    global ME_ID, SCHEDULER_TASK

    load_runtime_from_state()

    if not client.is_connected():
        await client.start()

    me = await client.get_me()
    ME_ID = getattr(me, "id", None)
    logger.info("[STARTUP] Telegram connected as %s", ME_ID)

    await recover_source_messages()

    if SCHEDULER_TASK is None or SCHEDULER_TASK.done():
        SCHEDULER_TASK = asyncio.create_task(scheduler_loop(), name="jarvis-scheduler")

    if CROSS_LOOP_RUNNING:
        if CURRENT_SOURCE_MSGS:
            global RUN_TASK
            RUN_TASK = asyncio.create_task(run_cross_loop(), name="cross-engine")
            logger.info("[STARTUP] Resumed persisted engine state")
        else:
            CROSS_LOOP_RUNNING = False
            await save_runtime_state()
            logger.warning("[STARTUP] Persisted engine was running but source messages were unavailable; engine left stopped")


@app.after_serving
async def shutdown() -> None:
    global SCHEDULER_TASK
    try:
        await stop_engine(save=True)
    except Exception:
        logger.exception("[SHUTDOWN] Engine shutdown failed")

    if SCHEDULER_TASK and not SCHEDULER_TASK.done():
        SCHEDULER_TASK.cancel()
        try:
            await SCHEDULER_TASK
        except asyncio.CancelledError:
            pass
        except Exception:
            logger.exception("[SHUTDOWN] Scheduler shutdown failed")
    SCHEDULER_TASK = None

    try:
        if client.is_connected():
            await client.disconnect()
    except Exception:
        logger.exception("[SHUTDOWN] Telegram disconnect failed")


# ============================================================
# ENTRYPOINT
# ============================================================

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=env_int("PORT", 8000))
