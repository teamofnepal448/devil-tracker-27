from telethon import TelegramClient, events, errors
from telethon.sessions import StringSession
from telethon.tl.functions.messages import GetDialogFiltersRequest, CheckChatInviteRequest
from telethon.tl.functions.channels import GetFullChannelRequest
from telethon.tl.types import (
    DialogFilter, PeerChannel, InputMessagesFilterPinned, User, 
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
# 🚀 TIMEZONE CONFIGURATION (IST / NPT FIX)
# ========================================================
LOCAL_TZ = timezone(timedelta(hours=5, minutes=30))

def get_local_now():
    return datetime.now(LOCAL_TZ)

# ========================================================
# 🚀 QUART WEB APP INITIALIZATION (FOR HOSTING/RENDER)
# ========================================================
app = Quart("devil_cross_app", root_path=".")

# ========================================================
# 🔑 CONFIGURATION & ENVIRONMENT SETUP
# ========================================================
API_ID = int(os.environ.get("API_ID", 36094172))
API_HASH = os.environ.get("API_HASH", "ff6eee1bcccf82daea88c63c45b6b546")
SESSION_STRING = os.environ.get("SESSION_STRING", None)

TARGET_MAIN_CHANNEL = int(os.environ.get("TARGET_MAIN_CHANNEL", -1001716302260))
FOLDER_TARGET_NAME = os.environ.get("FOLDER_TARGET_NAME", "RAN X CROXX")
DB_FILE_NAME = os.environ.get("DB_FILE_NAME", "devil_analytics_acc2.json")

DB_FILE = f"/data/{DB_FILE_NAME}" if os.path.exists("/data") else DB_FILE_NAME

if 'client' not in globals() or client is None:
    if SESSION_STRING:
        client = TelegramClient(StringSession(SESSION_STRING.strip()), API_ID, API_HASH)
    else:
        client = TelegramClient("devil_main_session_acc2", API_ID, API_HASH)

CROSS_LOOP_RUNNING = False
LOOP_END_TIME = None  
MEMORY_CACHE = {}
CHANNELS_QUEUE = [] 
PERMANENT_BAD_CHANNELS = set()
CURRENT_SOURCE_MSGS = []
ME_ID = None  # Global User ID cache for fast command response

status_tracker = {
    "total": 0, "completed": 0, "skipped": 0, "remaining": 0, "current_channel": "None", "timer_end": "None"
}

LINK_RESOLVE_CACHE = {}

# ========================================================
# 🛡️ AUTOMATION SAFE API WRAPPER (UPGRADED FLOODWAIT)
# ========================================================
async def safe_api_call(coro_func, *args, retries=3, **kwargs):
    """Executes API calls safely with limited FloodWait retries & permission handling."""
    attempt = 0
    while attempt < retries:
        try:
            return await coro_func(*args, **kwargs)
        except errors.FloodWaitError as e:
            attempt += 1
            wait_time = e.seconds + 3
            print(f"⚠️ FloodWait Detected (Attempt {attempt}/{retries}): Sleeping for {wait_time}s...")
            if attempt >= retries:
                print("❌ Max FloodWait retries exceeded. Request safely aborted.")
                return None
            await asyncio.sleep(wait_time)
        except (errors.ChatAdminRequiredError, errors.ChannelPrivateError, errors.ChatWriteForbiddenError, errors.UserBannedInChannelError):
            return "PERMISSION_ERROR"
        except Exception as e:
            print(f"⚠️ API Exception Handled: {e}")
            return None
    return None

# ========================================================
# 💾 STORAGE & ANALYTICS PERSISTENCE ENGINE
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
        except Exception:
            pass
    return {}

def save_analytics(data):
    global MEMORY_CACHE
    MEMORY_CACHE = data
    try:
        temp_file = f"{DB_FILE}.tmp"
        with open(temp_file, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=4)
        os.replace(temp_file, DB_FILE)
    except Exception:
        pass

def save_queue_state(queue_list):
    db = load_analytics()
    db["saved_queue_state"] = queue_list
    save_analytics(db)

def get_saved_queue_state():
    db = load_analytics()
    return db.get("saved_queue_state", [])

def update_joins_score(channel_id, channel_title, joins_gained):
    db = load_analytics()
    ch_key = str(channel_id)
    now = get_local_now()
    current_time_str = now.strftime("%Y-%m-%d %H:%M:%S")
    current_hour = now.strftime("%I:%M %p")

    if ch_key not in db:
        db[ch_key] = {"title": channel_title, "total_joins": 0, "runs": 0, "time_history": []}

    if "time_history" not in db[ch_key]:
        db[ch_key]["time_history"] = []

    db[ch_key]["runs"] += 1
    db[ch_key]["total_joins"] += max(0, joins_gained)
    db[ch_key]["time_history"].append({
        "timestamp": current_time_str, "hour": current_hour, "joins": max(0, joins_gained)
    })
    save_analytics(db)

# ========================================================
# 📊 ANALYTICS IMPROVEMENT (SAFE JOIN REQUEST DETECTOR)
# ========================================================
async def get_current_join_requests(target_channel):
    try:
        full_channel = await safe_api_call(client, GetFullChannelRequest(target_channel))
        if full_channel and full_channel != "PERMISSION_ERROR":
            if hasattr(full_channel.full_chat, 'requests_pending'):
                return full_channel.full_chat.requests_pending if full_channel.full_chat.requests_pending is not None else 0
    except Exception as e:
        print(f"⚠️ Join request API check failed gracefully: {e}")
    return None

# ========================================================
# 🔗 LINK DETECTOR & SAFE RESOLVER ENGINE
# ========================================================
def clean_and_repair_url(url):
    if not url:
        return ""
    url = url.strip()
    match = re.search(r'(?:t\.me|telegram\.me)/(.*)', url, re.IGNORECASE)
    if match:
        return f"https://t.me/{match.group(1)}"
    if url.startswith('@'):
        return f"https://t.me/{url[1:]}"
    return url

def extract_link_token(link):
    if not link:
        return ""
    clean_link = clean_and_repair_url(link).rstrip('/')
    match = re.search(r'(?:t\.me|telegram\.me)/(?:\+|joinchat/|addlist/)?([\w\-]+)', clean_link, re.IGNORECASE)
    if match:
        return match.group(1).lower()
    if clean_link.startswith('@'):
        return clean_link[1:].lower()
    return clean_link.lower()

def get_all_links_from_msg(msg):
    links = []
    if not msg:
        return links

    if hasattr(msg, 'reply_markup') and msg.reply_markup:
        try:
            if hasattr(msg.reply_markup, 'rows'):
                for row in msg.reply_markup.rows:
                    for button in getattr(row, 'buttons', []):
                        if hasattr(button, 'url') and button.url:
                            links.append(clean_and_repair_url(button.url))
        except Exception:
            pass

    raw_text = getattr(msg, 'raw_text', '') or getattr(msg, 'message', '') or ''

    if hasattr(msg, 'entities') and msg.entities:
        for entity in msg.entities:
            if isinstance(entity, MessageEntityTextUrl) and getattr(entity, 'url', None):
                links.append(clean_and_repair_url(entity.url))

    if raw_text:
        tg_pattern = r'(?:https?://|ps://|tps://|s://)?(?:www\.)?(?:t\.me|telegram\.me)/(?:\+[\w\-]+|joinchat/[\w\-]+|addlist/[\w\-]+|[\w\-]+)'
        raw_matches = re.findall(tg_pattern, raw_text, re.IGNORECASE)
        mentions = re.findall(r'(?<!\w)@([\w\-]+)', raw_text)

        for match in raw_matches:
            links.append(clean_and_repair_url(match))
        for mention in mentions:
            links.append(f"https://t.me/{mention}")

    seen_tokens = set()
    unique_links = []
    for l in links:
        l_lower = l.lower()
        if 't.me/' in l_lower or 'telegram.me/' in l_lower or l_lower.startswith('@'):
            token = extract_link_token(l)
            if token and token not in seen_tokens:
                seen_tokens.add(token)
                unique_links.append(clean_and_repair_url(l))

    return unique_links

def check_duplicate_link_in_msg(msg, target_link):
    target_token = extract_link_token(target_link)
    if not target_token:
        return False

    extracted_links = get_all_links_from_msg(msg)
    for link in extracted_links:
        if target_token == extract_link_token(link):
            return True
    return False

async def safe_resolve_entity_id(link):
    token = extract_link_token(link)
    if not token:
        return 'UNKNOWN'

    if token in LINK_RESOLVE_CACHE:
        return LINK_RESOLVE_CACHE[token]

    resolved_id = 'UNKNOWN'

    try:
        invite_match = re.search(r'(?:t\.me|telegram\.me)/(?:\+|joinchat/)([\w\-]+)', link, re.IGNORECASE)
        if invite_match:
            invite_hash = invite_match.group(1)
            try:
                res = await safe_api_call(client, CheckChatInviteRequest(invite_hash))
                if isinstance(res, (ChatInviteAlready, ChatInvite)) and hasattr(res, 'chat') and res.chat:
                    resolved_id = getattr(res.chat, 'id', 'UNKNOWN')
            except Exception:
                resolved_id = 'UNKNOWN'
        elif 'addlist/' in link.lower():
            resolved_id = 'UNKNOWN'
        else:
            try:
                resolved = await safe_api_call(client.get_entity, link)
                if resolved and resolved != "PERMISSION_ERROR":
                    resolved_id = getattr(resolved, 'id', 'UNKNOWN')
            except Exception:
                resolved_id = 'UNKNOWN'

    except Exception:
        resolved_id = 'UNKNOWN'

    if isinstance(resolved_id, int):
        resolved_id = abs(resolved_id)

    LINK_RESOLVE_CACHE[token] = resolved_id
    return resolved_id

async def verify_and_extract_links(current_channel_entity, messages_list, bio_text=""):
    current_channel_id = abs(current_channel_entity.id)
    blacklist_words = ["no link", "no cross", "admin remove", "cross off", "no promo", "link not allowed"]

    for msg in messages_list:
        raw_text = getattr(msg, 'raw_text', '') or getattr(msg, 'message', '') or ''
        if raw_text and any(word in raw_text.lower() for word in blacklist_words):
            return False, None

    candidate_links = []
    for msg in messages_list:
        candidate_links.extend(get_all_links_from_msg(msg))

    seen_tokens = set()
    unique_candidate_links = []
    for l in candidate_links:
        tok = extract_link_token(l)
        if tok and tok not in seen_tokens:
            seen_tokens.add(tok)
            unique_candidate_links.append(clean_and_repair_url(l))

    own_extracted_links = []
    for raw_link in unique_candidate_links:
        resolved_id = await safe_resolve_entity_id(raw_link)
        if resolved_id == 'UNKNOWN':
            return False, None
        if resolved_id == current_channel_id:
            own_extracted_links.append(raw_link)
        else:
            return False, None

    if own_extracted_links:
        return True, own_extracted_links[0]

    if bio_text:
        dummy_msg = type('DummyMsg', (), {'raw_text': bio_text, 'message': bio_text, 'reply_markup': None, 'entities': None})()
        bio_links = get_all_links_from_msg(dummy_msg)
        for link in bio_links:
            resolved_id = await safe_resolve_entity_id(link)
            if resolved_id == current_channel_id:
                return True, clean_and_repair_url(link)
            elif resolved_id != 'UNKNOWN':
                return False, None

    current_username = getattr(current_channel_entity, 'username', '')
    if current_username:
        return True, f"https://t.me/{current_username}"

    if bio_text and len(bio_text.strip()) > 0:
        return True, clean_and_repair_url(bio_text.strip())

    return True, "SKIP_DROP"

# ========================================================
# 📁 FOLDER CHANNELS SCANNER (ROBUST FULL EXTRACTOR)
# ========================================================
async def get_folder_channels_safely(target_name):
    channel_ids = []
    try:
        result = await safe_api_call(client, GetDialogFiltersRequest())
        if not result or result == "PERMISSION_ERROR":
            return []
        
        target_clean = str(target_name).strip().lower()
        filters_list = result.filters if hasattr(result, 'filters') else result

        for dialog_filter in filters_list:
            if isinstance(dialog_filter, DialogFilter) and dialog_filter.title:
                folder_title = str(dialog_filter.title.text if hasattr(dialog_filter.title, 'text') else dialog_filter.title).strip()
                if folder_title.lower() == target_clean:
                    if hasattr(dialog_filter, 'include_peers'):
                        for peer in dialog_filter.include_peers:
                            raw_id = getattr(peer, 'channel_id', None) or getattr(peer, 'chat_id', None)
                            if raw_id:
                                channel_ids.append(raw_id)
    except Exception:
        pass
    
    return list(set(channel_ids))

# ========================================================
# ⏱️ TIME DURATION PARSER
# ========================================================
def parse_duration(text_args):
    if not text_args:
        return None
    match = re.search(r'(\d+)\s*(hour|hr|h|min|m|day|d)?', text_args, re.IGNORECASE)
    if not match:
        return None
    val = int(match.group(1))
    unit = (match.group(2) or 'h').lower()
    
    if unit in ['h', 'hour', 'hr', 'hours']:
        return val * 3600
    elif unit in ['m', 'min', 'minute', 'minutes']:
        return val * 60
    elif unit in ['d', 'day', 'days']:
        return val * 86400
    return val * 3600

# ========================================================
# 🌐 WEB REST API ENDPOINTS & LIFECYCLE HOOKS
# ========================================================
@app.before_serving
async def startup_client():
    """Ensure Telethon client starts automatically when hosted via ASGI/Quart."""
    global ME_ID
    if not client.is_connected():
        await client.start()
    try:
        me = await client.get_me()
        if me:
            ME_ID = me.id
    except Exception as e:
        print(f"⚠️ Warning getting me entity: {e}")
    print("✅ Devil Engine V6.0 SafeGuard connected & ready for commands.")

@app.route('/')
async def home():
    return jsonify({
        "status": "online",
        "engine": "Devil Cross-Promotion Engine V6.0 SafeGuard",
        "is_running": CROSS_LOOP_RUNNING
    })

@app.route('/api/status', methods=['GET'])
async def api_status():
    db = load_analytics()
    sorted_channels = [item for item in db.items() if item[0] != "saved_queue_state"]
    sorted_channels = sorted(sorted_channels, key=lambda x: x[1].get("total_joins", 0), reverse=True)

    analytics_data = []
    for k, v in sorted_channels:
        analytics_data.append({
            "channel_id": k,
            "title": v.get("title", "Unknown"),
            "total_joins": v.get("total_joins", 0),
            "runs": v.get("runs", 0),
            "history": v.get("time_history", [])
        })

    return jsonify({
        "running": CROSS_LOOP_RUNNING,
        "tracker": status_tracker,
        "queue_length": len(CHANNELS_QUEUE),
        "analytics": analytics_data
    })

@app.route('/api/start', methods=['POST'])
async def api_start():
    global CROSS_LOOP_RUNNING, CHANNELS_QUEUE, CURRENT_SOURCE_MSGS, LOOP_END_TIME
    if CROSS_LOOP_RUNNING:
        return jsonify({"status": "error", "message": "Engine is already running!"}), 400

    data = await request.get_json() or {}
    duration_str = data.get("duration", "")
    seconds = parse_duration(duration_str)
    
    if seconds:
        LOOP_END_TIME = get_local_now() + timedelta(seconds=seconds)
        status_tracker["timer_end"] = LOOP_END_TIME.strftime("%I:%M %p (%d-%b)")
    else:
        LOOP_END_TIME = None
        status_tracker["timer_end"] = "24/7 Unlimited Mode"

    CROSS_LOOP_RUNNING = True
    saved_q = get_saved_queue_state()

    if saved_q:
        CHANNELS_QUEUE = saved_q
    else:
        channels = await get_folder_channels_safely(FOLDER_TARGET_NAME)
        if not channels:
            CROSS_LOOP_RUNNING = False
            return jsonify({"status": "error", "message": f"Folder '{FOLDER_TARGET_NAME}' is empty or not found!"}), 400
        
        CHANNELS_QUEUE = list(channels)

    status_tracker.update({"total": len(CHANNELS_QUEUE), "completed": 0, "skipped": 0, "remaining": len(CHANNELS_QUEUE), "current_channel": "None"})
    
    asyncio.create_task(run_cross_loop(CURRENT_SOURCE_MSGS))
    return jsonify({"status": "success", "message": "Cross loop started successfully!", "queue_count": len(CHANNELS_QUEUE)})

@app.route('/api/stop', methods=['POST'])
async def api_stop():
    global CROSS_LOOP_RUNNING, LOOP_END_TIME
    CROSS_LOOP_RUNNING = False
    LOOP_END_TIME = None
    save_queue_state(CHANNELS_QUEUE)
    return jsonify({"status": "success", "message": "Loop stopped. Current progress saved."})

@app.route('/api/reset', methods=['POST'])
async def api_reset():
    global CROSS_LOOP_RUNNING, CHANNELS_QUEUE, LOOP_END_TIME, PERMANENT_BAD_CHANNELS
    CROSS_LOOP_RUNNING = False
    LOOP_END_TIME = None
    PERMANENT_BAD_CHANNELS.clear()
    save_queue_state([])
    CHANNELS_QUEUE = []
    status_tracker.update({"total": 0, "completed": 0, "skipped": 0, "remaining": 0, "current_channel": "None", "timer_end": "None"})
    return jsonify({"status": "success", "message": "Queue reset completed."})

# ========================================================
# 🤖 BOT COMMAND CONTROLLER (OPTIMIZED COMMAND HANDLER)
# ========================================================
@client.on(events.NewMessage())
async def controller(event):
    global CROSS_LOOP_RUNNING, CHANNELS_QUEUE, CURRENT_SOURCE_MSGS, LOOP_END_TIME, PERMANENT_BAD_CHANNELS, ME_ID
    
    # Ensure ME_ID is set without blocking calls
    if ME_ID is None:
        try:
            me = await client.get_me()
            if me:
                ME_ID = me.id
        except Exception:
            pass

    # Process command if it was sent by user or outgoing from this account
    if ME_ID and event.sender_id != ME_ID and not event.out:
        return

    if not event.raw_text:
        return
        
    text = event.raw_text.strip()
    lower_text = text.lower()

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
            timer_msg = f"⏱️ **Timer Set:** Active for `{duration_args}` (Auto-Stop at {end_dt})"
        else:
            LOOP_END_TIME = None
            status_tracker["timer_end"] = "24/7 Unlimited Mode"
            timer_msg = "♾️ **Timer:** Continuous Round-Robin Mode"

        reply_msg = await event.get_reply_message()
        CROSS_LOOP_RUNNING = True

        source_msgs = [reply_msg]
        try:
            next_msgs = await safe_api_call(client.get_messages, event.chat_id, min_id=reply_msg.id, limit=2, reverse=True)
            if next_msgs and isinstance(next_msgs, list):
                for m in next_msgs:
                    if m.raw_text and m.raw_text.strip().lower().startswith("/"):
                        continue
                    source_msgs.append(m)
        except Exception:
            pass

        CURRENT_SOURCE_MSGS = source_msgs

        saved_q = get_saved_queue_state()
        if saved_q:
            CHANNELS_QUEUE = saved_q
            await event.reply(f"🔄 **Resuming saved queue!** Queue size: {len(CHANNELS_QUEUE)}\n{timer_msg}")
        else:
            channels = await get_folder_channels_safely(FOLDER_TARGET_NAME)
            if not channels:
                await event.reply(f"❌ Folder '{FOLDER_TARGET_NAME}' is empty!")
                CROSS_LOOP_RUNNING = False
                return
            CHANNELS_QUEUE = list(channels)

            status_tracker.update({"total": len(CHANNELS_QUEUE), "completed": 0, "skipped": 0, "remaining": len(CHANNELS_QUEUE), "current_channel": "None"})
            await event.reply(f"🚀 **Devil Cross Engine V6.0 Active.** Target channels: {len(CHANNELS_QUEUE)}\n{timer_msg}")

        asyncio.create_task(run_cross_loop(source_msgs))

    elif lower_text.startswith("/cross stop"):
        CROSS_LOOP_RUNNING = False
        LOOP_END_TIME = None
        save_queue_state(CHANNELS_QUEUE)
        await event.reply("🛑 Loop stopped & queue state saved.")

    elif lower_text.startswith("/cross reset"):
        save_queue_state([])
        CHANNELS_QUEUE = []
        PERMANENT_BAD_CHANNELS.clear()
        CROSS_LOOP_RUNNING = False
        LOOP_END_TIME = None
        status_tracker.update({"total": 0, "completed": 0, "skipped": 0, "remaining": 0, "current_channel": "None", "timer_end": "None"})
        await event.reply("🔄 Queue & Bad channel list reset completed!")

    elif lower_text.startswith("/status"):
        db = load_analytics()
        sorted_channels = [item for item in db.items() if item[0] != "saved_queue_state"]
        sorted_channels = sorted(sorted_channels, key=lambda x: x[1].get("total_joins", 0), reverse=True)

        hot_list, cold_list = [], []
        for k, v in sorted_channels:
            history = v.get("time_history", [])
            time_log = ""
            if history:
                best_run = max(history, key=lambda x: x["joins"])
                if best_run["joins"] > 0:
                    time_log = f" (Peak: +{best_run['joins']} at {best_run['hour']})"

            display_text = f"• {v['title']} +{v['total_joins']} joins{time_log}"
            if v["total_joins"] > 2:
                hot_list.append(display_text)
            else:
                cold_list.append(f"• {v['title']} {v['total_joins']} join")

        hot_display = "\n".join(hot_list) if hot_list else "No Hot Channels Yet."
        cold_display = "\n".join(cold_list) if cold_list else "No Cold Channels Yet."

        status_text = (
            f"📊 **DEVIL ENGINE V6.0 STATUS**\n\n"
            f"• Engine Status: {'⚡ RUNNING' if CROSS_LOOP_RUNNING else '💤 IDLE'}\n"
            f"• Mode: **{status_tracker.get('timer_end', 'None')}**\n"
            f"• Total Processed: {status_tracker['completed']}\n"
            f"• Permanently Skipped: {len(PERMANENT_BAD_CHANNELS)}\n"
            f"• Active Queue Remaining: {status_tracker['remaining']}\n"
            f"• Current Focus: **{status_tracker['current_channel']}**\n\n"
            f"🔥 **HOT ZONE ({len(hot_list)})**\n{hot_display}\n\n"
            f"❄️ **COLD ZONE ({len(cold_list)})**\n{cold_display}"
        )
        
        if len(status_text) > 4000:
            status_text = status_text[:3950] + "\n\n... (Truncated due to Telegram message length limit)"

        await event.reply(status_text)

# ========================================================
# ⚡ CORE AUTOMATION LOOP ENGINE (UPGRADED QUEUE & SAFEGUARD)
# ========================================================
async def run_cross_loop(source_msgs):
    global CROSS_LOOP_RUNNING, status_tracker, CHANNELS_QUEUE, LOOP_END_TIME, PERMANENT_BAD_CHANNELS

    status_tracker.update({"total": len(CHANNELS_QUEUE) + status_tracker['completed'], "remaining": len(CHANNELS_QUEUE)})

    while CROSS_LOOP_RUNNING:
        try:
            if LOOP_END_TIME and get_local_now() >= LOOP_END_TIME:
                print("⏱️ Set duration expired! Stopping cross engine cleanly.")
                CROSS_LOOP_RUNNING = False
                LOOP_END_TIME = None
                save_queue_state(CHANNELS_QUEUE)
                break

            if not CHANNELS_QUEUE:
                print(f"🔄 Queue completed! Reloading folder '{FOLDER_TARGET_NAME}' channels...")
                channels = await get_folder_channels_safely(FOLDER_TARGET_NAME)
                if channels:
                    CHANNELS_QUEUE = [c for c in channels if c not in PERMANENT_BAD_CHANNELS]
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
                global CHANNELS_QUEUE
                if CHANNELS_QUEUE and CHANNELS_QUEUE[0] == channel_id:
                    CHANNELS_QUEUE.pop(0)
                    save_queue_state(CHANNELS_QUEUE)

            if channel_id in PERMANENT_BAD_CHANNELS:
                finalize_current_channel()
                continue

            strict_id = int(f"-100{channel_id}" if not str(channel_id).startswith("-100") else channel_id)
            if strict_id == int(TARGET_MAIN_CHANNEL):
                finalize_current_channel()
                continue

            real_entity = await safe_api_call(client.get_entity, strict_id)
            if real_entity == "PERMISSION_ERROR" or not real_entity:
                PERMANENT_BAD_CHANNELS.add(channel_id)
                status_tracker["skipped"] += 1
                status_tracker["completed"] += 1
                finalize_current_channel()
                continue

            ch_title = getattr(real_entity, 'title', 'Channel')
            status_tracker["current_channel"] = ch_title

            messages_to_scan = []
            try:
                async for last_msg in client.iter_messages(real_entity, limit=4):
                    messages_to_scan.append(last_msg)
                    
                pinned_msgs = await safe_api_call(client.get_messages, real_entity, filter=InputMessagesFilterPinned(), limit=1)
                if pinned_msgs and isinstance(pinned_msgs, list):
                    for pm in pinned_msgs:
                        messages_to_scan.append(pm)
            except Exception:
                pass

            bio = ""
            try:
                full_channel = await safe_api_call(client, GetFullChannelRequest(real_entity))
                if full_channel and full_channel != "PERMISSION_ERROR":
                    bio = full_channel.full_chat.about or ""
            except Exception:
                pass

            is_safe, target_link = await verify_and_extract_links(real_entity, messages_to_scan, bio_text=bio)

            if not is_safe or not target_link or target_link == "SKIP_DROP":
                status_tracker["skipped"] += 1
                finalize_current_channel()
                continue

            fwd_ids = []
            first_fwd_id = None

            if source_msgs:
                fwd_msgs = await safe_api_call(client.forward_messages, real_entity, source_msgs[0], silent=False)
                if fwd_msgs == "PERMISSION_ERROR":
                    print(f"🚫 Permission error for {ch_title}. Permanently skipping channel.")
                    PERMANENT_BAD_CHANNELS.add(channel_id)
                    status_tracker["skipped"] += 1
                    finalize_current_channel()
                    continue
                elif fwd_msgs:
                    fwd = fwd_msgs[0] if isinstance(fwd_msgs, list) else fwd_msgs
                    if hasattr(fwd, 'id') and fwd.id:
                        first_fwd_id = fwd.id
                        fwd_ids.append(first_fwd_id)

            if not first_fwd_id:
                status_tracker["skipped"] += 1
                finalize_current_channel()
                continue

            main_channel_msg_ids = []

            before_joins = await get_current_join_requests(TARGET_MAIN_CHANNEL)
            await asyncio.sleep(random.uniform(1.5, 3.5))

            if target_link:
                drop_text = target_link if not target_link.startswith("http") else f"👉 {target_link}"
                drop = await safe_api_call(client.send_message, TARGET_MAIN_CHANNEL, drop_text, silent=True)
                if drop and hasattr(drop, 'id'):
                    main_channel_msg_ids.append(drop.id)

            stop_secondary_flag = asyncio.Event()

            async def send_secondary_posts_task():
                if len(source_msgs) <= 1:
                    return
                for msg in source_msgs[1:]:
                    post_delay = random.randint(45, 120)
                    elapsed = 0
                    while elapsed < post_delay:
                        if stop_secondary_flag.is_set() or not CROSS_LOOP_RUNNING:
                            return
                        await asyncio.sleep(2)
                        elapsed += 2

                    if stop_secondary_flag.is_set() or not CROSS_LOOP_RUNNING:
                        return

                    chk = await safe_api_call(client.get_messages, real_entity, ids=first_fwd_id)
                    if not chk or getattr(chk, 'empty', False):
                        stop_secondary_flag.set()
                        return

                    if msg.media:
                        sec_fwd = await safe_api_call(client.send_message, real_entity, msg.message or "", file=msg.media, reply_to=first_fwd_id, silent=False)
                    else:
                        sec_fwd = await safe_api_call(client.send_message, real_entity, msg.message or "", reply_to=first_fwd_id, silent=False)
                    
                    if sec_fwd and hasattr(sec_fwd, 'id'):
                        fwd_ids.append(sec_fwd.id)

            sec_task = asyncio.create_task(send_secondary_posts_task())

            start_monitor_time = asyncio.get_event_loop().time()
            total_wait_duration = 300  # 5 minutes per channel

            while (asyncio.get_event_loop().time() - start_monitor_time) < total_wait_duration and CROSS_LOOP_RUNNING:
                await asyncio.sleep(10)

                chk_msg = await safe_api_call(client.get_messages, real_entity, ids=first_fwd_id)
                if not chk_msg or getattr(chk_msg, 'empty', False):
                    break

                if target_link:
                    recent_main = await safe_api_call(client.get_messages, TARGET_MAIN_CHANNEL, limit=8)
                    if recent_main and isinstance(recent_main, list):
                        for rm in recent_main:
                            if rm.id not in main_channel_msg_ids:
                                if check_duplicate_link_in_msg(rm, target_link):
                                    main_channel_msg_ids.append(rm.id)

            stop_secondary_flag.set()
            sec_task.cancel()

            after_joins = await get_current_join_requests(TARGET_MAIN_CHANNEL)
            if before_joins is not None and after_joins is not None:
                joins_gained = max(0, after_joins - before_joins)
                update_joins_score(channel_id, ch_title, joins_gained)

            if main_channel_msg_ids:
                await safe_api_call(client.delete_messages, TARGET_MAIN_CHANNEL, main_channel_msg_ids)
                main_channel_msg_ids.clear()

            if fwd_ids:
                await safe_api_call(client.delete_messages, real_entity, fwd_ids)
                fwd_ids.clear()

            status_tracker["completed"] += 1
            finalize_current_channel()
            await asyncio.sleep(random.randint(5, 10))

        except Exception as global_err:
            print(f"⚠️ Self-Healing Core: Recovered from exception -> {global_err}")
            await asyncio.sleep(5)
            continue

# ========================================================
# 🚀 DEVIL ENGINE PANEL ENTRY POINT
# ========================================================
async def main():
    global ME_ID
    if not client.is_connected():
        await client.start()
    me = await client.get_me()
    if me:
        ME_ID = me.id
    print("✅ Devil Cross Engine V6.0 SafeGuard online & operational.")
    await client.run_until_disconnected()

if __name__ == '__main__':
    asyncio.run(main())
