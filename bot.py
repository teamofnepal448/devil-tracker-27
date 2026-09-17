from telethon import TelegramClient, events, errors
from telethon.sessions import StringSession
from telethon.tl.functions.messages import GetDialogFiltersRequest, CheckChatInviteRequest
from telethon.tl.functions.channels import GetFullChannelRequest
from telethon.tl.types import (
    DialogFilter, InputMessagesFilterPinned,
    MessageEntityTextUrl, MessageEntityUrl, ChatInvite, ChatInviteAlready
)
import asyncio
import os
import re
import random
import json
import time
from datetime import datetime, timedelta, timezone
from quart import Quart, jsonify, request

# ========================================================
# DEVIL ENGINE V7.0 — GEMINI MINI BRAIN
# ========================================================

LOCAL_TZ = timezone(timedelta(hours=5, minutes=30))

def get_local_now():
    return datetime.now(LOCAL_TZ)

app = Quart("devil_cross_app", root_path=".")

# --- Integrated Credentials ---
API_ID = int(os.environ.get("API_ID", 36094172))
API_HASH = os.environ.get("API_HASH", "ff6eee1bcccf82daea88c63c45b6b546")
SESSION_STRING = os.environ.get("SESSION_STRING")

GEMINI_API_KEY = os.environ.get(
    "GEMINI_API_KEY", 
    "AQ.Ab8RN6InYuWy8SqPS2qrcsu-r20aOM4SqWteebuFP8HtVMsM_A"
)
GEMINI_MODEL = os.environ.get("GEMINI_MODEL", "gemini-2.5-flash")

TARGET_MAIN_CHANNEL = int(os.environ.get("TARGET_MAIN_CHANNEL", -1002413253133))
FOLDER_TARGET_NAME = os.environ.get("FOLDER_TARGET_NAME", "RAN X CROXX")
DB_FILE_NAME = os.environ.get("DB_FILE_NAME", "devil_analytics_acc2.json")

DB_FILE = f"/data/{DB_FILE_NAME}" if os.path.exists("/data") else DB_FILE_NAME

if SESSION_STRING:
    client = TelegramClient(StringSession(SESSION_STRING.strip()), API_ID, API_HASH)
else:
    client = TelegramClient("devil_main_session_acc2", API_ID, API_HASH)

# ========================================================
# GLOBAL STATE
# ========================================================
CROSS_LOOP_RUNNING = False
LOOP_END_TIME = None
MEMORY_CACHE = {}
CHANNELS_QUEUE = []
PERMANENT_BAD_CHANNELS = set()
CURRENT_SOURCE_MSGS = []
ME_ID = None
RUN_TASK = None

status_tracker = {
    "total": 0,
    "completed": 0,
    "skipped": 0,
    "remaining": 0,
    "current_channel": "None",
    "timer_end": "None",
}

LINK_RESOLVE_CACHE = {}
AI_LAST_DECISION = None
AI_LAST_RESULT = None
AI_BUSY = False

# ========================================================
# SAFE TELEGRAM API
# ========================================================
async def safe_api_call(coro_func, *args, retries=3, **kwargs):
    attempt = 0
    while attempt < retries:
        try:
            return await coro_func(*args, **kwargs)
        except errors.FloodWaitError as e:
            attempt += 1
            wait_time = max(0, int(e.seconds))
            print(
                f"⚠️ Telegram FloodWait: sleeping exactly {wait_time}s "
                f"(attempt {attempt}/{retries})"
            )
            await asyncio.sleep(wait_time)
            if attempt >= retries:
                return None
        except (
            errors.ChatAdminRequiredError,
            errors.ChannelPrivateError,
            errors.ChatWriteForbiddenError,
            errors.UserBannedInChannelError,
        ):
            return "PERMISSION_ERROR"
        except Exception as e:
            print(f"⚠️ API Exception: {e}")
            return None
    return None

# ========================================================
# STORAGE
# ========================================================
def load_analytics():
    global MEMORY_CACHE
    if MEMORY_CACHE:
        return MEMORY_CACHE

    if os.path.exists(DB_FILE):
        try:
            with open(DB_FILE, "r", encoding="utf-8") as f:
                MEMORY_CACHE = json.load(f)
                return MEMORY_CACHE
        except Exception as e:
            print(f"⚠️ Analytics load failed: {e}")

    MEMORY_CACHE = {}
    return MEMORY_CACHE


def save_analytics(data):
    global MEMORY_CACHE
    MEMORY_CACHE = data

    try:
        folder = os.path.dirname(DB_FILE)
        if folder:
            os.makedirs(folder, exist_ok=True)

        temp_file = f"{DB_FILE}.tmp"
        with open(temp_file, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        os.replace(temp_file, DB_FILE)
    except Exception as e:
        print(f"⚠️ Analytics save failed: {e}")


def save_queue_state(queue_list):
    db = load_analytics()
    db["saved_queue_state"] = list(queue_list)
    save_analytics(db)


def get_saved_queue_state():
    db = load_analytics()
    value = db.get("saved_queue_state", [])
    return value if isinstance(value, list) else []


def update_joins_score(channel_id, channel_title, joins_gained):
    db = load_analytics()
    ch_key = str(channel_id)
    now = get_local_now()

    if ch_key not in db or not isinstance(db.get(ch_key), dict):
        db[ch_key] = {
            "title": channel_title,
            "total_joins": 0,
            "runs": 0,
            "time_history": [],
        }

    entry = db[ch_key]
    entry.setdefault("title", channel_title)
    entry.setdefault("total_joins", 0)
    entry.setdefault("runs", 0)
    entry.setdefault("time_history", [])

    joins_gained = max(0, int(joins_gained))
    entry["runs"] += 1
    entry["total_joins"] += joins_gained
    entry["time_history"].append({
        "timestamp": now.strftime("%Y-%m-%d %H:%M:%S"),
        "hour": now.strftime("%I:%M %p"),
        "joins": joins_gained,
    })

    entry["time_history"] = entry["time_history"][-200:]
    save_analytics(db)

# ========================================================
# TELEGRAM JOIN REQUEST ANALYTICS
# ========================================================
async def get_current_join_requests(target_channel):
    try:
        full_channel = await safe_api_call(
            client,
            GetFullChannelRequest,
            target_channel
        )
        if full_channel and full_channel != "PERMISSION_ERROR":
            pending = getattr(full_channel.full_chat, "requests_pending", None)
            return 0 if pending is None else pending
    except Exception as e:
        print(f"⚠️ Join request check failed: {e}")
    return None

# ========================================================
# LINK ENGINE
# ========================================================
def clean_and_repair_url(url):
    if not url:
        return ""

    url = str(url).strip()

    if url.startswith("ps://"):
        url = "htt" + url
    elif url.startswith("tps://"):
        url = "ht" + url
    elif url.startswith("s://"):
        url = "http" + url

    match = re.search(
        r"(?:https?://)?(?:www\.)?(?:t\.me|telegram\.me)/(.*)",
        url,
        re.IGNORECASE,
    )
    if match:
        return f"https://t.me/{match.group(1)}"

    if url.startswith("@"):
        return f"https://t.me/{url[1:]}"

    return url


def extract_link_token(link):
    if not link:
        return ""

    clean_link = clean_and_repair_url(link).rstrip("/")

    match = re.search(
        r"(?:t\.me|telegram\.me)/(?:\+|joinchat/|addlist/)?([\w\-]+)",
        clean_link,
        re.IGNORECASE,
    )
    if match:
        return match.group(1).lower()

    if clean_link.startswith("@"):
        return clean_link[1:].lower()

    return clean_link.lower()


def get_all_links_from_msg(msg):
    links = []
    if not msg:
        return links

    if getattr(msg, "reply_markup", None):
        try:
            for row in getattr(msg.reply_markup, "rows", []):
                for button in getattr(row, "buttons", []):
                    url = getattr(button, "url", None)
                    if url:
                        links.append(clean_and_repair_url(url))
        except Exception:
            pass

    raw_text = getattr(msg, "raw_text", "") or getattr(msg, "message", "") or ""

    if getattr(msg, "entities", None):
        for entity in msg.entities:
            if isinstance(entity, MessageEntityTextUrl):
                url = getattr(entity, "url", None)
                if url:
                    links.append(clean_and_repair_url(url))

            elif isinstance(entity, MessageEntityUrl):
                try:
                    offset = entity.offset
                    length = entity.length
                    value = raw_text[offset:offset + length]
                    if value:
                        links.append(clean_and_repair_url(value))
                except Exception:
                    pass

    if raw_text:
        tg_pattern = (
            r"(?:https?://|ps://|tps://|s://)?(?:www\.)?"
            r"(?:t\.me|telegram\.me)/"
            r"(?:\+[\w\-]+|joinchat/[\w\-]+|addlist/[\w\-]+|[\w\-]+)"
        )
        raw_matches = re.findall(tg_pattern, raw_text, re.IGNORECASE)
        mentions = re.findall(r"(?<!\w)@([\w\-]+)", raw_text)

        for match in raw_matches:
            links.append(clean_and_repair_url(match))
        for mention in mentions:
            links.append(f"https://t.me/{mention}")

    seen_tokens = set()
    unique_links = []

    for link in links:
        link = clean_and_repair_url(link)
        low = link.lower()

        if "t.me/" in low or "telegram.me/" in low or low.startswith("@"):
            token = extract_link_token(link)
            if token and token not in seen_tokens:
                seen_tokens.add(token)
                unique_links.append(link)

    return unique_links


def check_duplicate_link_in_msg(msg, target_link):
    target_token = extract_link_token(target_link)
    if not target_token:
        return False

    for link in get_all_links_from_msg(msg):
        if target_token == extract_link_token(link):
            return True

    return False


async def safe_resolve_entity_id(link):
    token = extract_link_token(link)
    if not token:
        return "UNKNOWN"

    if token in LINK_RESOLVE_CACHE:
        return LINK_RESOLVE_CACHE[token]

    resolved_id = "UNKNOWN"

    try:
        invite_match = re.search(
            r"(?:t\.me|telegram\.me)/(?:\+|joinchat/)([\w\-]+)",
            link,
            re.IGNORECASE,
        )

        if invite_match:
            invite_hash = invite_match.group(1)
            res = await safe_api_call(
                client,
                CheckChatInviteRequest,
                invite_hash
            )

            if isinstance(res, (ChatInviteAlready, ChatInvite)):
                chat = getattr(res, "chat", None)
                if chat:
                    resolved_id = getattr(chat, "id", "UNKNOWN")

        elif "addlist/" in link.lower():
            resolved_id = "UNKNOWN"

        else:
            resolved = await safe_api_call(client.get_entity, link)
            if resolved and resolved != "PERMISSION_ERROR":
                resolved_id = getattr(resolved, "id", "UNKNOWN")

    except Exception:
        resolved_id = "UNKNOWN"

    if isinstance(resolved_id, int):
        resolved_id = abs(resolved_id)

    LINK_RESOLVE_CACHE[token] = resolved_id
    return resolved_id


async def verify_and_extract_links(current_channel_entity, messages_list, bio_text=""):
    current_channel_id = abs(current_channel_entity.id)

    blacklist_words = [
        "no link",
        "no cross",
        "admin remove",
        "cross off",
        "no promo",
        "link not allowed",
    ]

    for msg in messages_list:
        raw_text = getattr(msg, "raw_text", "") or getattr(msg, "message", "") or ""
        if raw_text and any(word in raw_text.lower() for word in blacklist_words):
            return False, None

    candidate_links = []
    for msg in messages_list:
        candidate_links.extend(get_all_links_from_msg(msg))

    seen_tokens = set()
    unique_candidate_links = []

    for link in candidate_links:
        token = extract_link_token(link)
        if token and token not in seen_tokens:
            seen_tokens.add(token)
            unique_candidate_links.append(clean_and_repair_url(link))

    own_links = []

    for raw_link in unique_candidate_links:
        resolved_id = await safe_resolve_entity_id(raw_link)

        if resolved_id == "UNKNOWN":
            return False, None

        if resolved_id == current_channel_id:
            own_links.append(raw_link)
        else:
            return False, None

    if own_links:
        return True, own_links[0]

    if bio_text:
        dummy_msg = type(
            "DummyMsg",
            (),
            {
                "raw_text": bio_text,
                "message": bio_text,
                "reply_markup": None,
                "entities": None,
            },
        )()

        for link in get_all_links_from_msg(dummy_msg):
            resolved_id = await safe_resolve_entity_id(link)

            if resolved_id == current_channel_id:
                return True, clean_and_repair_url(link)

            if resolved_id != "UNKNOWN":
                return False, None

    username = getattr(current_channel_entity, "username", "")
    if username:
        return True, f"https://t.me/{username}"

    return True, "SKIP_DROP"

# ========================================================
# FOLDER SCANNER — CHANNELS ONLY
# ========================================================
async def get_folder_channels_safely(target_name):
    channel_ids = []

    try:
        result = await safe_api_call(client, GetDialogFiltersRequest())
        if not result or result == "PERMISSION_ERROR":
            return []

        target_clean = str(target_name).strip().lower()
        filters_list = getattr(result, "filters", result)

        for dialog_filter in filters_list:
            if not isinstance(dialog_filter, DialogFilter):
                continue

            title_obj = getattr(dialog_filter, "title", None)
            folder_title = str(
                getattr(title_obj, "text", title_obj) or ""
            ).strip()

            if folder_title.lower() != target_clean:
                continue

            for peer in getattr(dialog_filter, "include_peers", []):
                raw_id = getattr(peer, "channel_id", None)

                if raw_id:
                    channel_ids.append(int(raw_id))

    except Exception as e:
        print(f"⚠️ Folder scan failed: {e}")

    return list(dict.fromkeys(channel_ids))

# ========================================================
# TIMER
# ========================================================
def parse_duration(text_args):
    if not text_args:
        return None

    match = re.search(
        r"(\d+)\s*(hours?|hrs?|h|minutes?|mins?|min|m|days?|d)?",
        text_args,
        re.IGNORECASE,
    )

    if not match:
        return None

    val = int(match.group(1))
    unit = (match.group(2) or "h").lower()

    if unit.startswith("h"):
        return val * 3600
    if unit.startswith("m"):
        return val * 60
    if unit.startswith("d"):
        return val * 86400

    return val * 3600

# ========================================================
# AI MINI BRAIN
# ========================================================
try:
    from google import genai
except Exception:
    genai = None


AI_SYSTEM_PROMPT = """
You are the planning brain of a Telegram automation application.

Your job is to analyze the supplied application state, logs and user instruction
and return a SAFE, concise decision.

IMPORTANT:
- Do not invent facts.
- Do not claim you executed anything.
- Do not output Python or shell commands.
- Do not output arbitrary code.
- Use only the allowed command names below.
- If no action is needed, use REPORT.
- If an action could have irreversible or destructive consequences, use ASK_CONFIRM.
- For timing recommendations, use historical analytics if available and clearly
  distinguish a data-based recommendation from a guarantee.
- Respect Telegram API errors and FloodWaits; never suggest bypassing limits.

Allowed commands:
REPORT
PAUSE_ENGINE
RESUME_ENGINE
RETRY_CURRENT
STOP_ENGINE
SAVE_STATE
RECHECK_CURRENT
RUN_STATUS
ASK_CONFIRM

Return JSON only:
{
  "command": "...",
  "reason": "...",
  "message": "...",
  "wait_seconds": 0
}
"""


def get_ai_snapshot():
    db = load_analytics()

    analytics = {}
    for key, value in db.items():
        if key == "saved_queue_state":
            continue
        if isinstance(value, dict):
            analytics[key] = {
                "title": value.get("title", "Unknown"),
                "total_joins": value.get("total_joins", 0),
                "runs": value.get("runs", 0),
                "recent_history": value.get("time_history", [])[-20:],
            }

    return {
        "time": get_local_now().isoformat(),
        "engine_running": CROSS_LOOP_RUNNING,
        "timer_end": (
            LOOP_END_TIME.isoformat() if LOOP_END_TIME else None
        ),
        "queue_length": len(CHANNELS_QUEUE),
        "queue_preview": CHANNELS_QUEUE[:20],
        "permanent_bad_count": len(PERMANENT_BAD_CHANNELS),
        "current_channel": status_tracker.get("current_channel"),
        "tracker": dict(status_tracker),
        "analytics": analytics,
        "last_ai_decision": AI_LAST_DECISION,
        "last_ai_result": AI_LAST_RESULT,
    }


def extract_json_object(text):
    text = (text or "").strip()

    if text.startswith("```"):
        text = re.sub(r"^```(?:json)?\s*", "", text, flags=re.IGNORECASE)
        text = re.sub(r"\s*```$", "", text)

    start = text.find("{")
    end = text.rfind("}")

    if start == -1 or end == -1 or end <= start:
        return None

    try:
        return json.loads(text[start:end + 1])
    except Exception:
        return None


async def ask_gemini(instruction):
    if not GEMINI_API_KEY:
        return {
            "command": "REPORT",
            "reason": "GEMINI_API_KEY is not configured.",
            "message": "Set GEMINI_API_KEY before using /ai.",
            "wait_seconds": 0,
        }

    if genai is None:
        return {
            "command": "REPORT",
            "reason": "google-genai package is not installed.",
            "message": "Install the google-genai package.",
            "wait_seconds": 0,
        }

    snapshot = get_ai_snapshot()

    prompt = (
        AI_SYSTEM_PROMPT
        + "\n\nUSER INSTRUCTION:\n"
        + instruction
        + "\n\nAPPLICATION SNAPSHOT:\n"
        + json.dumps(snapshot, ensure_ascii=False, default=str)
    )

    try:
        ai_client = genai.Client(api_key=GEMINI_API_KEY)

        response = await asyncio.to_thread(
            ai_client.models.generate_content,
            model=GEMINI_MODEL,
            contents=prompt,
        )

        decision = extract_json_object(getattr(response, "text", ""))

        if not decision:
            return {
                "command": "REPORT",
                "reason": "AI returned an invalid decision format.",
                "message": "No action executed.",
                "wait_seconds": 0,
            }

        command = str(decision.get("command", "REPORT")).upper()

        if command not in {
            "REPORT",
            "PAUSE_ENGINE",
            "RESUME_ENGINE",
            "RETRY_CURRENT",
            "STOP_ENGINE",
            "SAVE_STATE",
            "RECHECK_CURRENT",
            "RUN_STATUS",
            "ASK_CONFIRM",
        }:
            command = "ASK_CONFIRM"

        return {
            "command": command,
            "reason": str(decision.get("reason", ""))[:1000],
            "message": str(decision.get("message", ""))[:2000],
            "wait_seconds": max(
                0,
                min(int(decision.get("wait_seconds", 0) or 0), 3600)
            ),
        }

    except Exception as e:
        print(f"⚠️ Gemini error: {e}")
        return {
            "command": "REPORT",
            "reason": f"Gemini request failed: {e}",
            "message": "No automated action executed.",
            "wait_seconds": 0,
        }

# ========================================================
# AI TOOL EXECUTOR
# ========================================================
async def execute_ai_command(decision):
    global CROSS_LOOP_RUNNING, LOOP_END_TIME, AI_LAST_RESULT

    command = decision.get("command", "REPORT").upper()

    if command == "PAUSE_ENGINE":
        CROSS_LOOP_RUNNING = False
        save_queue_state(CHANNELS_QUEUE)
        AI_LAST_RESULT = "Engine paused and queue saved."
        return AI_LAST_RESULT

    if command == "RESUME_ENGINE":
        if CROSS_LOOP_RUNNING:
            AI_LAST_RESULT = "Engine is already running."
            return AI_LAST_RESULT

        if not CURRENT_SOURCE_MSGS:
            AI_LAST_RESULT = (
                "Cannot resume: no source message is configured. "
                "Use /cross start with a reply first."
            )
            return AI_LAST_RESULT

        CROSS_LOOP_RUNNING = True
        if CHANNELS_QUEUE:
            start_cross_task(CURRENT_SOURCE_MSGS)
            AI_LAST_RESULT = "Engine resumed."
        else:
            AI_LAST_RESULT = "Engine marked active, but queue is empty."
        return AI_LAST_RESULT

    if command == "STOP_ENGINE":
        CROSS_LOOP_RUNNING = False
        LOOP_END_TIME = None
        save_queue_state(CHANNELS_QUEUE)
        AI_LAST_RESULT = "Engine stopped and queue saved."
        return AI_LAST_RESULT

    if command == "SAVE_STATE":
        save_queue_state(CHANNELS_QUEUE)
        AI_LAST_RESULT = "Queue state saved."
        return AI_LAST_RESULT

    if command == "RETRY_CURRENT":
        if not CHANNELS_QUEUE:
            AI_LAST_RESULT = "No current channel is available."
            return AI_LAST_RESULT

        CROSS_LOOP_RUNNING = True
        start_cross_task(CURRENT_SOURCE_MSGS)
        AI_LAST_RESULT = "Current queue processing resumed/retried."
        return AI_LAST_RESULT

    if command == "RECHECK_CURRENT":
        AI_LAST_RESULT = (
            f"Current queue head: "
            f"{CHANNELS_QUEUE[0] if CHANNELS_QUEUE else 'None'}"
        )
        return AI_LAST_RESULT

    if command == "RUN_STATUS":
        AI_LAST_RESULT = json.dumps(get_ai_snapshot(), default=str)[:3500]
        return AI_LAST_RESULT

    if command == "ASK_CONFIRM":
        AI_LAST_RESULT = (
            "AI requested confirmation before taking a potentially consequential action: "
            + decision.get("message", "")
        )
        return AI_LAST_RESULT

    AI_LAST_RESULT = decision.get("message") or decision.get("reason") or "No action."
    return AI_LAST_RESULT


async def ai_process_instruction(instruction):
    global AI_LAST_DECISION, AI_LAST_RESULT, AI_BUSY

    if AI_BUSY:
        return "🧠 AI is already processing another request."

    AI_BUSY = True
    try:
        decision = await ask_gemini(instruction)
        AI_LAST_DECISION = decision

        result = await execute_ai_command(decision)

        wait_seconds = decision.get("wait_seconds", 0)
        if wait_seconds:
            await asyncio.sleep(wait_seconds)

        return (
            "🧠 **AI DECISION**\n"
            f"• Command: `{decision.get('command')}`\n"
            f"• Reason: {decision.get('reason', 'N/A')}\n"
            f"• AI Message: {decision.get('message', 'N/A')}\n\n"
            f"🤖 **BOT RESULT**\n{result}"
        )
    finally:
        AI_BUSY = False

# ========================================================
# TASK MANAGEMENT
# ========================================================
def start_cross_task(source_msgs):
    global RUN_TASK

    if RUN_TASK and not RUN_TASK.done():
        return RUN_TASK

    RUN_TASK = asyncio.create_task(run_cross_loop(source_msgs))
    return RUN_TASK


async def stop_cross_task(save=True):
    global RUN_TASK, CROSS_LOOP_RUNNING, LOOP_END_TIME

    CROSS_LOOP_RUNNING = False
    LOOP_END_TIME = None

    if save:
        save_queue_state(CHANNELS_QUEUE)

    task = RUN_TASK
    RUN_TASK = None

    if task and not task.done():
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass
        except Exception as e:
            print(f"⚠️ Cross task shutdown error: {e}")

# ========================================================
# WEB LIFECYCLE
# ========================================================
@app.before_serving
async def startup_client():
    global ME_ID

    if not client.is_connected():
        await client.start()

    try:
        me = await client.get_me()
        if me:
            ME_ID = me.id
    except Exception as e:
        print(f"⚠️ Warning getting me entity: {e}")

    print("✅ Devil Engine V7.0 + Gemini Brain connected & ready.")


@app.after_serving
async def shutdown_client():
    try:
        await stop_cross_task(save=True)
    except Exception as e:
        print(f"⚠️ Shutdown task cleanup failed: {e}")

    try:
        if client.is_connected():
            await client.disconnect()
    except Exception:
        pass


@app.route("/")
async def home():
    return jsonify({
        "status": "online",
        "engine": "Devil Cross-Promotion Engine V7.0 + Gemini Brain",
        "is_running": CROSS_LOOP_RUNNING,
        "ai_configured": bool(GEMINI_API_KEY),
    })


@app.route("/api/status", methods=["GET"])
async def api_status():
    db = load_analytics()

    sorted_channels = [
        item for item in db.items()
        if item[0] != "saved_queue_state"
        and isinstance(item[1], dict)
    ]
    sorted_channels.sort(
        key=lambda x: x[1].get("total_joins", 0),
        reverse=True,
    )

    analytics_data = []

    for key, value in sorted_channels:
        analytics_data.append({
            "channel_id": key,
            "title": value.get("title", "Unknown"),
            "total_joins": value.get("total_joins", 0),
            "runs": value.get("runs", 0),
            "history": value.get("time_history", []),
        })

    return jsonify({
        "running": CROSS_LOOP_RUNNING,
        "tracker": status_tracker,
        "queue_length": len(CHANNELS_QUEUE),
        "ai_configured": bool(GEMINI_API_KEY),
        "last_ai_decision": AI_LAST_DECISION,
        "last_ai_result": AI_LAST_RESULT,
        "analytics": analytics_data,
    })


@app.route("/api/start", methods=["POST"])
async def api_start():
    global CROSS_LOOP_RUNNING, CHANNELS_QUEUE, LOOP_END_TIME

    if CROSS_LOOP_RUNNING:
        return jsonify({
            "status": "error",
            "message": "Engine is already running!"
        }), 400

    data = await request.get_json() or {}
    duration_str = data.get("duration", "")
    seconds = parse_duration(duration_str)

    if seconds:
        LOOP_END_TIME = get_local_now() + timedelta(seconds=seconds)
        status_tracker["timer_end"] = LOOP_END_TIME.strftime(
            "%I:%M %p (%d-%b)"
        )
    else:
        LOOP_END_TIME = None
        status_tracker["timer_end"] = "24/7 Unlimited Mode"

    saved_q = get_saved_queue_state()

    if saved_q:
        CHANNELS_QUEUE = saved_q
    else:
        channels = await get_folder_channels_safely(FOLDER_TARGET_NAME)
        if not channels:
            return jsonify({
                "status": "error",
                "message": f"Folder '{FOLDER_TARGET_NAME}' is empty or not found!"
            }), 400
        CHANNELS_QUEUE = list(channels)

    CROSS_LOOP_RUNNING = True

    status_tracker.update({
        "total": len(CHANNELS_QUEUE),
        "completed": 0,
        "skipped": 0,
        "remaining": len(CHANNELS_QUEUE),
        "current_channel": "None",
    })

    if not CURRENT_SOURCE_MSGS:
        CROSS_LOOP_RUNNING = False
        return jsonify({
            "status": "error",
            "message": "No source message configured. Use /cross start first."
        }), 400

    start_cross_task(CURRENT_SOURCE_MSGS)

    return jsonify({
        "status": "success",
        "message": "Cross loop started successfully!",
        "queue_count": len(CHANNELS_QUEUE),
    })


@app.route("/api/stop", methods=["POST"])
async def api_stop():
    await stop_cross_task(save=True)
    return jsonify({
        "status": "success",
        "message": "Loop stopped. Current progress saved."
    })


@app.route("/api/reset", methods=["POST"])
async def api_reset():
    global CHANNELS_QUEUE, PERMANENT_BAD_CHANNELS, LOOP_END_TIME

    await stop_cross_task(save=False)

    CHANNELS_QUEUE = []
    PERMANENT_BAD_CHANNELS.clear()
    LOOP_END_TIME = None
    save_queue_state([])

    status_tracker.update({
        "total": 0,
        "completed": 0,
        "skipped": 0,
        "remaining": 0,
        "current_channel": "None",
        "timer_end": "None",
    })

    return jsonify({
        "status": "success",
        "message": "Queue reset completed."
    })

# ========================================================
# TELEGRAM CONTROLLER
# ========================================================
@client.on(events.NewMessage())
async def controller(event):
    global CROSS_LOOP_RUNNING, CHANNELS_QUEUE, CURRENT_SOURCE_MSGS, LOOP_END_TIME, PERMANENT_BAD_CHANNELS, ME_ID

    if ME_ID is None:
        try:
            me = await client.get_me()
            if me:
                ME_ID = me.id
        except Exception:
            pass

    if ME_ID and event.sender_id != ME_ID and not event.out:
        return

    if not event.raw_text:
        return

    text = event.raw_text.strip()
    lower_text = text.lower()

    if lower_text.startswith("/ai"):
        instruction = text[3:].strip()

        if not instruction:
            await event.reply(
                "🧠 **AI Brain Ready**\n\n"
                "Example:\n"
                "`/ai system status check karo`\n"
                "`/ai current errors analyse karo`\n"
                "`/ai queue state analyse karo`"
            )
            return

        result = await ai_process_instruction(instruction)

        if len(result) > 4000:
            result = result[:3950] + "\n... (truncated)"

        await event.reply(result)
        return

    if lower_text.startswith("/cross start"):
        if not event.is_reply:
            await event.reply("⚠️ Reply to a post to set promo messages!")
            return

        if CROSS_LOOP_RUNNING:
            await event.reply("⚠️ Loop is already running!")
            return

        duration_args = text[12:].strip()
        duration_sec = parse_duration(duration_args)

        if duration_sec:
            LOOP_END_TIME = get_local_now() + timedelta(seconds=duration_sec)
            end_dt = LOOP_END_TIME.strftime("%I:%M %p (%d-%b)")
            status_tracker["timer_end"] = end_dt
            timer_msg = (
                f"⏱️ **Timer Set:** Active for `{duration_args}` "
                f"(Auto-Stop at {end_dt})"
            )
        else:
            LOOP_END_TIME = None
            status_tracker["timer_end"] = "24/7 Unlimited Mode"
            timer_msg = "♾️ **Timer:** Continuous Round-Robin Mode"

        reply_msg = await event.get_reply_message()
        source_msgs = [reply_msg]

        try:
            next_msgs = await safe_api_call(
                client.get_messages,
                event.chat_id,
                min_id=reply_msg.id,
                limit=2,
                reverse=True,
            )

            if next_msgs and isinstance(next_msgs, list):
                for msg in next_msgs:
                    if msg.raw_text and msg.raw_text.strip().lower().startswith("/"):
                        continue
                    source_msgs.append(msg)
        except Exception:
            pass

        CURRENT_SOURCE_MSGS = source_msgs

        saved_q = get_saved_queue_state()

        if saved_q:
            CHANNELS_QUEUE = saved_q
            CROSS_LOOP_RUNNING = True
            start_cross_task(source_msgs)
            await event.reply(
                f"🔄 **Resuming saved queue!** Queue size: "
                f"{len(CHANNELS_QUEUE)}\n{timer_msg}"
            )
            return

        channels = await get_folder_channels_safely(FOLDER_TARGET_NAME)

        if not channels:
            await event.reply(
                f"❌ Folder '{FOLDER_TARGET_NAME}' is empty!"
            )
            return

        CHANNELS_QUEUE = list(channels)
        CROSS_LOOP_RUNNING = True

        status_tracker.update({
            "total": len(CHANNELS_QUEUE),
            "completed": 0,
            "skipped": 0,
            "remaining": len(CHANNELS_QUEUE),
            "current_channel": "None",
        })

        start_cross_task(source_msgs)

        await event.reply(
            f"🚀 **Devil Engine V7.0 Active.** "
            f"Target channels: {len(CHANNELS_QUEUE)}\n{timer_msg}"
        )
        return

    if lower_text.startswith("/cross stop"):
        await stop_cross_task(save=True)
        await event.reply("🛑 Loop stopped & queue state saved.")
        return

    if lower_text.startswith("/cross reset"):
        await stop_cross_task(save=False)

        CHANNELS_QUEUE = []
        PERMANENT_BAD_CHANNELS.clear()
        LOOP_END_TIME = None
        save_queue_state([])

        status_tracker.update({
            "total": 0,
            "completed": 0,
            "skipped": 0,
            "remaining": 0,
            "current_channel": "None",
            "timer_end": "None",
        })

        await event.reply("🔄 Queue & Bad channel list reset completed!")
        return

    if lower_text.startswith("/status"):
        db = load_analytics()

        sorted_channels = [
            item for item in db.items()
            if item[0] != "saved_queue_state"
            and isinstance(item[1], dict)
        ]
        sorted_channels.sort(
            key=lambda x: x[1].get("total_joins", 0),
            reverse=True,
        )

        hot_list, cold_list = [], []

        for key, value in sorted_channels:
            history = value.get("time_history", [])
            time_log = ""

            if history:
                best_run = max(
                    history,
                    key=lambda x: x.get("joins", 0)
                )
                if best_run.get("joins", 0) > 0:
                    time_log = (
                        f" (Peak: +{best_run['joins']} "
                        f"at {best_run.get('hour', '?')})"
                    )

            display_text = (
                f"• {value.get('title', 'Unknown')} "
                f"+{value.get('total_joins', 0)} joins{time_log}"
            )

            if value.get("total_joins", 0) > 2:
                hot_list.append(display_text)
            else:
                cold_list.append(
                    f"• {value.get('title', 'Unknown')} "
                    f"{value.get('total_joins', 0)} join"
                )

        hot_display = "\n".join(hot_list) if hot_list else "No Hot Channels Yet."
        cold_display = "\n".join(cold_list) if cold_list else "No Cold Channels Yet."

        status_text = (
            "📊 **DEVIL ENGINE V7.0 STATUS**\n\n"
            f"• Engine Status: "
            f"{'⚡ RUNNING' if CROSS_LOOP_RUNNING else '💤 IDLE'}\n"
            f"• Mode: **{status_tracker.get('timer_end', 'None')}**\n"
            f"• Total Processed: {status_tracker['completed']}\n"
            f"• Permanently Skipped: {len(PERMANENT_BAD_CHANNELS)}\n"
            f"• Active Queue Remaining: {len(CHANNELS_QUEUE)}\n"
            f"• Current Focus: **{status_tracker['current_channel']}**\n"
            f"• AI Brain: **{'ONLINE' if GEMINI_API_KEY else 'NOT CONFIGURED'}**\n\n"
            f"🔥 **HOT ZONE ({len(hot_list)})**\n{hot_display}\n\n"
            f"❄️ **COLD ZONE ({len(cold_list)})**\n{cold_display}"
        )

        if len(status_text) > 4000:
            status_text = status_text[:3950] + "\n... (truncated)"

        await event.reply(status_text)
        return

# ========================================================
# CORE CROSS LOOP
# ========================================================
async def run_cross_loop(source_msgs):
    global CROSS_LOOP_RUNNING, CHANNELS_QUEUE, LOOP_END_TIME, PERMANENT_BAD_CHANNELS

    status_tracker.update({
        "total": len(CHANNELS_QUEUE) + status_tracker["completed"],
        "remaining": len(CHANNELS_QUEUE),
    })

    while CROSS_LOOP_RUNNING:
        try:
            if LOOP_END_TIME and get_local_now() >= LOOP_END_TIME:
                print("⏱️ Duration expired. Stopping cleanly.")
                CROSS_LOOP_RUNNING = False
                LOOP_END_TIME = None
                save_queue_state(CHANNELS_QUEUE)
                break

            if not CHANNELS_QUEUE:
                print(
                    f"🔄 Queue completed. Reloading folder "
                    f"'{FOLDER_TARGET_NAME}'..."
                )

                channels = await get_folder_channels_safely(FOLDER_TARGET_NAME)

                if channels:
                    CHANNELS_QUEUE = [
                        c for c in channels
                        if c not in PERMANENT_BAD_CHANNELS
                    ]
                    status_tracker["total"] += len(CHANNELS_QUEUE)
                    save_queue_state(CHANNELS_QUEUE)
                    await asyncio.sleep(15)
                else:
                    print("⚠️ Folder empty. Retrying scan in 30s...")
                    await asyncio.sleep(30)
                    continue

            if not CHANNELS_QUEUE:
                continue

            channel_id = CHANNELS_QUEUE[0]
            status_tracker["remaining"] = len(CHANNELS_QUEUE)

            def finalize_current_channel():
                if CHANNELS_QUEUE and CHANNELS_QUEUE[0] == channel_id:
                    CHANNELS_QUEUE.pop(0)
                    save_queue_state(CHANNELS_QUEUE)
                    status_tracker["remaining"] = len(CHANNELS_QUEUE)

            if channel_id in PERMANENT_BAD_CHANNELS:
                finalize_current_channel()
                continue

            strict_id = int(
                f"-100{channel_id}"
                if not str(channel_id).startswith("-100")
                else channel_id
            )

            if strict_id == int(TARGET_MAIN_CHANNEL):
                finalize_current_channel()
                continue

            real_entity = await safe_api_call(client.get_entity, strict_id)

            if real_entity in ("PERMISSION_ERROR", None):
                PERMANENT_BAD_CHANNELS.add(channel_id)
                status_tracker["skipped"] += 1
                status_tracker["completed"] += 1
                finalize_current_channel()
                continue

            ch_title = getattr(real_entity, "title", "Channel")
            status_tracker["current_channel"] = ch_title

            messages_to_scan = []

            try:
                async for last_msg in client.iter_messages(
                    real_entity,
                    limit=4
                ):
                    messages_to_scan.append(last_msg)

                pinned_msgs = await safe_api_call(
                    client.get_messages,
                    real_entity,
                    filter=InputMessagesFilterPinned(),
                    limit=1,
                )

                if pinned_msgs and isinstance(pinned_msgs, list):
                    messages_to_scan.extend(pinned_msgs)

            except Exception as e:
                print(f"⚠️ Message scan failed for {ch_title}: {e}")

            bio = ""

            try:
                full_channel = await safe_api_call(
                    client,
                    GetFullChannelRequest,
                    real_entity
                )

                if full_channel and full_channel != "PERMISSION_ERROR":
                    bio = getattr(full_channel.full_chat, "about", "") or ""

            except Exception:
                pass

            is_safe, target_link = await verify_and_extract_links(
                real_entity,
                messages_to_scan,
                bio_text=bio,
            )

            if not is_safe or not target_link or target_link == "SKIP_DROP":
                status_tracker["skipped"] += 1
                finalize_current_channel()
                continue

            fwd_ids = []
            first_fwd_id = None

            if source_msgs:
                fwd_msgs = await safe_api_call(
                    client.forward_messages,
                    real_entity,
                    source_msgs[0],
                    silent=False,
                )

                if fwd_msgs == "PERMISSION_ERROR":
                    PERMANENT_BAD_CHANNELS.add(channel_id)
                    status_tracker["skipped"] += 1
                    finalize_current_channel()
                    continue

                if fwd_msgs:
                    fwd = (
                        fwd_msgs[0]
                        if isinstance(fwd_msgs, list)
                        else fwd_msgs
                    )

                    if getattr(fwd, "id", None):
                        first_fwd_id = fwd.id
                        fwd_ids.append(first_fwd_id)

            if not first_fwd_id:
                status_tracker["skipped"] += 1
                finalize_current_channel()
                continue

            main_channel_msg_ids = []

            before_joins = await get_current_join_requests(
                TARGET_MAIN_CHANNEL
            )

            await asyncio.sleep(random.uniform(1.5, 3.5))

            drop_text = (
                target_link
                if target_link.startswith("http")
                else f"👉 {target_link}"
            )

            drop = await safe_api_call(
                client.send_message,
                TARGET_MAIN_CHANNEL,
                drop_text,
                silent=True,
            )

            if drop and getattr(drop, "id", None):
                main_channel_msg_ids.append(drop.id)

            stop_secondary_flag = asyncio.Event()

            async def send_secondary_posts_task():
                if len(source_msgs) <= 1:
                    return

                try:
                    for msg in source_msgs[1:]:
                        post_delay = random.randint(45, 120)
                        elapsed = 0

                        while elapsed < post_delay:
                            if (
                                stop_secondary_flag.is_set()
                                or not CROSS_LOOP_RUNNING
                            ):
                                return

                            await asyncio.sleep(2)
                            elapsed += 2

                        if (
                            stop_secondary_flag.is_set()
                            or not CROSS_LOOP_RUNNING
                        ):
                            return

                        chk = await safe_api_call(
                            client.get_messages,
                            real_entity,
                            ids=first_fwd_id,
                        )

                        if not chk or getattr(chk, "empty", False):
                            stop_secondary_flag.set()
                            return

                        if msg.media:
                            sec_fwd = await safe_api_call(
                                client.send_message,
                                real_entity,
                                msg.message or "",
                                file=msg.media,
                                reply_to=first_fwd_id,
                                silent=False,
                            )
                        else:
                            sec_fwd = await safe_api_call(
                                client.send_message,
                                real_entity,
                                msg.message or "",
                                reply_to=first_fwd_id,
                                silent=False,
                            )

                        if sec_fwd and getattr(sec_fwd, "id", None):
                            fwd_ids.append(sec_fwd.id)

                except asyncio.CancelledError:
                    raise
                except Exception as e:
                    print(f"⚠️ Secondary task recovered: {e}")

            sec_task = asyncio.create_task(
                send_secondary_posts_task()
            )

            start_monitor_time = asyncio.get_event_loop().time()
            total_wait_duration = 300

            while (
                asyncio.get_event_loop().time() - start_monitor_time
                < total_wait_duration
                and CROSS_LOOP_RUNNING
            ):
                await asyncio.sleep(10)

                chk_msg = await safe_api_call(
                    client.get_messages,
                    real_entity,
                    ids=first_fwd_id,
                )

                if not chk_msg or getattr(chk_msg, "empty", False):
                    break

                if target_link:
                    recent_main = await safe_api_call(
                        client.get_messages,
                        TARGET_MAIN_CHANNEL,
                        limit=8,
                    )

                    if recent_main and isinstance(recent_main, list):
                        for rm in recent_main:
                            if (
                                rm.id not in main_channel_msg_ids
                                and check_duplicate_link_in_msg(
                                    rm,
                                    target_link
                                )
                            ):
                                main_channel_msg_ids.append(rm.id)

            stop_secondary_flag.set()
            sec_task.cancel()

            try:
                await sec_task
            except asyncio.CancelledError:
                pass
            except Exception as e:
                print(f"⚠️ Secondary task cleanup error: {e}")

            after_joins = await get_current_join_requests(
                TARGET_MAIN_CHANNEL
            )

            if before_joins is not None and after_joins is not None:
                joins_gained = max(
                    0,
                    after_joins - before_joins
                )
                update_joins_score(
                    channel_id,
                    ch_title,
                    joins_gained
                )

            if main_channel_msg_ids:
                await safe_api_call(
                    client.delete_messages,
                    TARGET_MAIN_CHANNEL,
                    main_channel_msg_ids,
                )
                main_channel_msg_ids.clear()

            if fwd_ids:
                await safe_api_call(
                    client.delete_messages,
                    real_entity,
                    fwd_ids,
                )
                fwd_ids.clear()

            status_tracker["completed"] += 1
            finalize_current_channel()

            await asyncio.sleep(random.randint(5, 10))

        except asyncio.CancelledError:
            save_queue_state(CHANNELS_QUEUE)
            print("🛑 Cross loop task cancelled safely.")
            raise

        except Exception as global_err:
            print(
                "⚠️ Self-Healing Core: "
                f"Recovered from exception -> {global_err}"
            )
            save_queue_state(CHANNELS_QUEUE)
            await asyncio.sleep(5)
            continue

# ========================================================
# MAIN
# ========================================================
async def main():
    global ME_ID

    if not client.is_connected():
        await client.start()

    me = await client.get_me()

    if me:
        ME_ID = me.id

    print("✅ Devil Engine V7.0 + Gemini Brain online.")
    await client.run_until_disconnected()


if __name__ == "__main__":
    asyncio.run(main())
