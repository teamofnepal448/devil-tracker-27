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
# 🚀 TIMEZONE CONFIGURATION (NPT / NEPAL TIME)
# ========================================================
LOCAL_TZ = timezone(timedelta(hours=5, minutes=45))

def get_local_now():
    return datetime.now(LOCAL_TZ)

# ========================================================
# 🚀 QUART WEB APP INITIALIZATION
# ========================================================
app = Quart("devil_cross_app", root_path=".")

# ========================================================
# 🔑 CONFIGURATION & ENVIRONMENT SETUP
# ========================================================
API_ID = int(os.environ.get("API_ID", 33372374))
API_HASH = os.environ.get("API_HASH", "1605f94e6ad4944f30662bde0327b31a")
SESSION_STRING = os.environ.get("SESSION_STRING", None)

TARGET_MAIN_CHANNEL = int(os.environ.get("TARGET_MAIN_CHANNEL", -1002413253133))
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
SKIPPED_QUEUE = []
PERMANENT_BAD_CHANNELS = set()
CURRENT_SOURCE_MSGS = []
ME_ID = None
CURRENT_ROUND = 1

CUSTOM_CROSS_MSG = None

status_tracker = {
    "total": 0, "completed": 0, "skipped": 0, "remaining": 0, "current_channel": "None", "timer_end": "None"
}

LINK_RESOLVE_CACHE = {}

# ========================================================
# 🛡️ AUTOMATION SAFE API WRAPPER
# ========================================================
async def safe_api_call(coro_func, *args, retries=3, **kwargs):
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
# 💾 STORAGE & ANALYTICS
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

def save_queue_state(queue_list, skipped_list=None):
    db = load_analytics()
    db["saved_queue_state"] = queue_list
    if skipped_list is not None:
        db["saved_skipped_queue"] = skipped_list
    save_analytics(db)

def get_saved_queue_state():
    db = load_analytics()
    return db.get("saved_queue_state", [])

def get_saved_skipped_queue():
    db = load_analytics()
    return db.get("saved_skipped_queue", [])

def save_custom_cross_msg(text):
    db = load_analytics()
    db["custom_cross_msg"] = text
    save_analytics(db)

def load_custom_cross_msg():
    db = load_analytics()
    return db.get("custom_cross_msg", None)

def update_joins_score(channel_id, channel_title, joins_gained):
    db = load_analytics()
    ch_key = str(channel_id)

    if ch_key not in db:
        db[ch_key] = {"title": channel_title, "total_joins": 0, "runs": 0}

    db[ch_key]["runs"] += 1
    db[ch_key]["total_joins"] += max(0, joins_gained)
    save_analytics(db)

# ========================================================
# 📊 JOIN REQUEST COUNT DETECTOR
# ========================================================
async def get_join_request_count(target_channel):
    try:
        full_channel = await safe_api_call(client, GetFullChannelRequest(target_channel))
        if full_channel and full_channel != "PERMISSION_ERROR":
            if hasattr(full_channel.full_chat, 'requests_pending'):
                val = full_channel.full_chat.requests_pending
                return val if val is not None else 0
    except Exception as e:
        print(f"⚠️ Join request count check failed: {e}")
    return None

# ========================================================
# 🔗 LINK DETECTOR ENGINE
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

# ========================================================
# 🚫 ADVANCED NO-LINK DETECTOR
# ========================================================
BLACKLIST_PHRASES = [
    "no link", "no links", "no cross", "no promo", "no promotion",
    "admin remove", "cross off", "link not allowed", "links not allowed",
    "don't cross", "dont cross", "not allowed", "no advertising",
    "no ads", "no advert", "no ad", "no join", "no join link",
    "stop cross", "stop link", "no marketing", "no spam",
    "link nahi", "link nhi", "no link hai", "cross nahi", "cross nhi",
    "promo nahi", "promo nhi", "link band", "cross band",
    "link mat", "cross mat", "link mt", "cross mt",
    "🚫 no link", "❌ no link", "⛔ no link", "no 🚫", "no ❌",
    "link 🚫", "link ❌", "cross 🚫", "cross ❌",
]

BLACKLIST_EMOJI_TOKENS = ["🚫", "❌", "⛔", "🚷", "🛑", "⭕", "❎"]

STICKER_NO_LINK_EMOJI_SETS = [
    {"🚫"}, {"❌"}, {"⛔"}, {"🚫", "🔗"}, {"🔗", "🚫"}, {"❌", "🔗"},
    {"🚫", "📢"}, {"⛔", "🔗"}, {"🛑"}, {"🚷"}
]

def detect_no_link_from_text(text):
    if not text:
        return False
    lower = text.lower().strip()
    
    for phrase in BLACKLIST_PHRASES:
        if phrase in lower:
            return True
    
    stripped = lower.replace(" ", "").replace("\n", "")
    for emo in BLACKLIST_EMOJI_TOKENS:
        if emo in stripped and len(stripped) <= 6:
            return True
    
    return False

def detect_no_link_from_sticker(msg):
    if not msg or not getattr(msg, 'sticker', None):
        return False
    
    sticker = msg.sticker
    
    sticker_emoji = getattr(sticker, 'emoji', None) or ""
    if sticker_emoji:
        for emo_set in STICKER_NO_LINK_EMOJI_SETS:
            if all(e in sticker_emoji for e in emo_set):
                return True
    
    alt_text = getattr(sticker, 'alt', None) or ""
    if alt_text and detect_no_link_from_text(alt_text):
        return True
    
    try:
        attributes = getattr(getattr(sticker, 'attributes', None), '__iter__', None)
        if attributes:
            for attr in sticker.attributes:
                alt = getattr(attr, 'alt', None) or ""
                if alt and detect_no_link_from_text(alt):
                    return True
    except Exception:
        pass
    
    return False

# ========================================================
# 🧹 PROMO NOISE FILTER
# ========================================================
PROMO_NOISE_PATTERNS = [
    r'^\s*r\s*[\.\-]?\s*rmv\s*$',
    r'^\s*rmv\s*$',
    r'^\s*next\s+link\s*$',
    r'^\s*hai\s+mt\s+dalo\s+link\s*$',
    r'^\s*hai\s+mat\s+dalo\s+link\s*$',
    r'^\s*link\s*$',
    r'^\s*\.\s*$',
    r'^\s*\.\.\s*$',
    r'^\s*\.\.\.\s*$',
    r'^\s*promo\s+time\s*$',
    r'^\s*toss\s+time\s*$',
    r'^\s*chk\s+time\s*$',
    r'^\s*check\s+time\s*$',
    r'^\s*toss\s*$',
    r'^\s*chk\s*$',
    r'^\s*rmv\s+link\s*$',
    r'^\s*remove\s*$',
    r'^\s*remove\s+link\s*$',
]

PROMO_NOISE_EMOJI_SUBSTRINGS = ["🗑", "❌", "🚫", "⛔"]

def is_promo_noise_message(msg):
    if not msg:
        return False

    raw_text = getattr(msg, 'raw_text', '') or getattr(msg, 'message', '') or ''
    if not raw_text:
        return False

    txt = raw_text.strip()
    if not txt:
        return False

    lower = txt.lower()

    for emo in PROMO_NOISE_EMOJI_SUBSTRINGS:
        if emo in txt and len(txt) <= 4:
            return True

    for pattern in PROMO_NOISE_PATTERNS:
        if re.match(pattern, lower):
            return True

    compact = re.sub(r'[\s\.\-_]', '', lower)
    if compact in ("rrmv", "rmv", "nextlink", "haimtdalolink", "haimatdalolink",
                   "promotime", "tosstime", "chktime", "checktime", "link", "remove"):
        return True

    return False

# ========================================================
# 🔥 STRICT LINK VERIFICATION
# ========================================================
async def verify_and_extract_links(current_channel_entity, messages_list, bio_text=""):
    current_channel_id = abs(current_channel_entity.id)
    current_username = (getattr(current_channel_entity, 'username', '') or '').lower().strip()

    for msg in messages_list:
        raw_text = getattr(msg, 'raw_text', '') or getattr(msg, 'message', '') or ''
        
        if raw_text and detect_no_link_from_text(raw_text):
            print(f"🚫 No-link TEXT detected: '{raw_text[:50]}'")
            return 'SKIP', None
        
        if getattr(msg, 'sticker', None) and detect_no_link_from_sticker(msg):
            print(f"🚫 No-link STICKER detected (emoji meta)")
            return 'SKIP', None
        
        if raw_text and len(raw_text.strip()) <= 6:
            stripped = raw_text.strip()
            for emo in BLACKLIST_EMOJI_TOKENS:
                if emo in stripped:
                    print(f"🚫 No-link EMOJI caption detected: '{stripped}'")
                    return 'SKIP', None

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

    own_link_found = None
    foreign_link_found = False

    for raw_link in unique_candidate_links:
        link_token = extract_link_token(raw_link)
        
        if current_username and link_token == current_username:
            own_link_found = raw_link
            break
        
        resolved_id = await safe_resolve_entity_id(raw_link)
        
        if resolved_id == 'UNKNOWN':
            continue
        
        if resolved_id == current_channel_id:
            own_link_found = raw_link
            break
        else:
            print(f"🚨 Foreign link detected: {raw_link}")
            foreign_link_found = True
    
    if own_link_found:
        return 'SAFE_LINK', own_link_found
    
    if foreign_link_found:
        print(f"🚫 Skipping: only foreign links found")
        return 'SKIP', None

    if bio_text:
        dummy_msg = type('DummyMsg', (), {'raw_text': bio_text, 'message': bio_text, 'reply_markup': None, 'entities': None})()
        bio_links = get_all_links_from_msg(dummy_msg)
        for link in bio_links:
            link_token = extract_link_token(link)
            if current_username and link_token == current_username:
                return 'SAFE_LINK', clean_and_repair_url(link)
            
            resolved_id = await safe_resolve_entity_id(link)
            if resolved_id == current_channel_id:
                return 'SAFE_LINK', clean_and_repair_url(link)
            elif resolved_id != 'UNKNOWN':
                print(f"🚫 Bio has foreign link only → SKIP")
                return 'SKIP', None

    if current_username:
        return 'SAFE_LINK', f"https://t.me/{current_username}"

    if bio_text and len(bio_text.strip()) > 0:
        return 'BIO_FALLBACK', bio_text.strip()

    return 'SKIP', None

# ========================================================
# 📁 FOLDER CHANNELS SCANNER
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
# 🌐 WEB REST API ENDPOINTS
# ========================================================
@app.before_serving
async def startup_client():
    global ME_ID, CUSTOM_CROSS_MSG
    if not client.is_connected():
        await client.start()
    try:
        me = await client.get_me()
        if me:
            ME_ID = me.id
    except Exception as e:
        print(f"⚠️️ Warning getting me entity: {e}")
    CUSTOM_CROSS_MSG = load_custom_cross_msg()
    print("✅ Devil Engine V7.8 SafeGuard+ connected & ready.")

@app.route('/')
async def home():
    return jsonify({
        "status": "online",
        "engine": "Devil Cross-Promotion Engine V7.8",
        "is_running": CROSS_LOOP_RUNNING,
        "round": CURRENT_ROUND,
        "active_queue": len(CHANNELS_QUEUE),
        "skipped_queue": len(SKIPPED_QUEUE)
    })

@app.route('/api/status', methods=['GET'])
async def api_status():
    db = load_analytics()
    skip_keys = {
        "saved_queue_state", "saved_skipped_queue", "custom_cross_msg",
        "global_seen_member_ids", "channel_seen_member_ids", 
        "main_channel_baseline_ids"
    }
    
    sorted_channels = []
    for k, v in db.items():
        if k in skip_keys:
            continue
        if not isinstance(v, dict):
            continue
        sorted_channels.append((k, v))
    
    sorted_channels = sorted(
        sorted_channels, 
        key=lambda x: x[1].get("total_joins", 0) if isinstance(x[1], dict) else 0, 
        reverse=True
    )

    analytics_data = []
    for k, v in sorted_channels:
        analytics_data.append({
            "channel_id": k,
            "title": v.get("title", "Unknown"),
            "total_joins": v.get("total_joins", 0),
            "runs": v.get("runs", 0)
        })

    return jsonify({
        "running": CROSS_LOOP_RUNNING,
        "round": CURRENT_ROUND,
        "tracker": status_tracker,
        "active_queue_length": len(CHANNELS_QUEUE),
        "skipped_queue_length": len(SKIPPED_QUEUE),
        "analytics": analytics_data
    })

@app.route('/api/start', methods=['POST'])
async def api_start():
    global CROSS_LOOP_RUNNING, CHANNELS_QUEUE, SKIPPED_QUEUE, CURRENT_SOURCE_MSGS, LOOP_END_TIME, CURRENT_ROUND
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
    saved_skip = get_saved_skipped_queue()

    if saved_q:
        CHANNELS_QUEUE = saved_q
        SKIPPED_QUEUE = saved_skip
    else:
        channels = await get_folder_channels_safely(FOLDER_TARGET_NAME)
        if not channels:
            CROSS_LOOP_RUNNING = False
            return jsonify({"status": "error", "message": f"Folder '{FOLDER_TARGET_NAME}' is empty or not found!"}), 400
        
        CHANNELS_QUEUE = list(channels)
        SKIPPED_QUEUE = []
        CURRENT_ROUND = 1

    status_tracker.update({"total": len(CHANNELS_QUEUE), "completed": 0, "skipped": 0, "remaining": len(CHANNELS_QUEUE), "current_channel": "None"})
    
    asyncio.create_task(run_cross_loop(CURRENT_SOURCE_MSGS))
    return jsonify({"status": "success", "message": "Cross loop started successfully!", "queue_count": len(CHANNELS_QUEUE)})

@app.route('/api/stop', methods=['POST'])
async def api_stop():
    global CROSS_LOOP_RUNNING, LOOP_END_TIME
    CROSS_LOOP_RUNNING = False
    LOOP_END_TIME = None
    save_queue_state(CHANNELS_QUEUE, SKIPPED_QUEUE)
    return jsonify({"status": "success", "message": "Loop stopped. Current progress saved."})

@app.route('/api/reset', methods=['POST'])
async def api_reset():
    global CROSS_LOOP_RUNNING, CHANNELS_QUEUE, SKIPPED_QUEUE, LOOP_END_TIME, PERMANENT_BAD_CHANNELS, CURRENT_ROUND
    CROSS_LOOP_RUNNING = False
    LOOP_END_TIME = None
    PERMANENT_BAD_CHANNELS.clear()
    save_queue_state([], [])
    CHANNELS_QUEUE = []
    SKIPPED_QUEUE = []
    CURRENT_ROUND = 1
    status_tracker.update({"total": 0, "completed": 0, "skipped": 0, "remaining": 0, "current_channel": "None", "timer_end": "None"})
    return jsonify({"status": "success", "message": "Queue reset completed."})

# ========================================================
# 🤖 BOT COMMAND CONTROLLER
# ========================================================
@client.on(events.NewMessage())
async def controller(event):
    global CROSS_LOOP_RUNNING, CHANNELS_QUEUE, SKIPPED_QUEUE, CURRENT_SOURCE_MSGS, LOOP_END_TIME, PERMANENT_BAD_CHANNELS, ME_ID, CURRENT_ROUND, CUSTOM_CROSS_MSG
    
    if ME_ID is None:
        try:
            me = await client.get_me()
            if me:
                ME_ID = me.id
        except Exception:
            pass

    if not event.out:
        return

    if not event.raw_text:
        return
        
    text = event.raw_text.strip()
    lower_text = text.lower()

    print(f"📨 Command received: {text[:60]}")

    async def safe_reply(msg_text):
        try:
            await client.send_message(event.chat_id, msg_text, reply_to=event.id)
            return True
        except Exception as e1:
            print(f"⚠️ Reply attempt failed: {e1}")
            try:
                await client.send_message(event.chat_id, msg_text)
                return True
            except Exception as e2:
                print(f"❌ Fallback send failed: {e2}")
                return False

    if lower_text.startswith("/cross msg set"):
        custom_text = text[len("/cross msg set"):].strip()
        if not custom_text:
            await safe_reply(
                "⚠️ **Usage:** `/cross msg set <your text>`\n"
                "Example: `/cross msg set Join our premium community 🚀`"
            )
            return
        CUSTOM_CROSS_MSG = custom_text
        save_custom_cross_msg(custom_text)
        await safe_reply(
            f"✅ **Custom Cross Message Saved!**\n\n"
            f"📝 **Text:** `{custom_text[:200]}`\n\n"
            f"Ab jab bhi link main channel me drop hoga, ye message uske reply me jayega."
        )
        return

    if lower_text.startswith("/cross msg clear"):
        CUSTOM_CROSS_MSG = None
        save_custom_cross_msg(None)
        await safe_reply("🗑️ **Custom Cross Message cleared!**")
        return

    if lower_text.startswith("/cross msg show"):
        if CUSTOM_CROSS_MSG:
            await safe_reply(f"📝 **Current Custom Cross Message:**\n\n`{CUSTOM_CROSS_MSG[:500]}`")
        else:
            await safe_reply("⚠️ **No custom cross message set.**\nUse: `/cross msg set <text>`")
        return

    if lower_text.startswith("/cross start"):
        if not event.is_reply:
            await safe_reply("⚠️ Reply to a post to set promo messages!")
            return
        if CROSS_LOOP_RUNNING:
            await safe_reply("⚠️ Loop is already running!")
            return

        duration_args = text[len("/cross start"):].strip()
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
        saved_skip = get_saved_skipped_queue()
        
        if saved_q:
            CHANNELS_QUEUE = saved_q
            SKIPPED_QUEUE = saved_skip
            await safe_reply(
                f"🔄 **Resuming saved queue!**\n"
                f"• Active: {len(CHANNELS_QUEUE)}\n"
                f"• Skipped: {len(SKIPPED_QUEUE)}\n"
                f"• Round: #{CURRENT_ROUND}\n"
                f"{timer_msg}"
            )
        else:
            channels = await get_folder_channels_safely(FOLDER_TARGET_NAME)
            if not channels:
                await safe_reply(f"❌ Folder '{FOLDER_TARGET_NAME}' is empty!")
                CROSS_LOOP_RUNNING = False
                return
            CHANNELS_QUEUE = list(channels)
            SKIPPED_QUEUE = []
            CURRENT_ROUND = 1

            status_tracker.update({"total": len(CHANNELS_QUEUE), "completed": 0, "skipped": 0, "remaining": len(CHANNELS_QUEUE), "current_channel": "None"})
            await safe_reply(
                f"🚀 **Devil Engine V7.8 Active.**\n"
                f"• Target channels: {len(CHANNELS_QUEUE)}\n"
                f"• Round: #1\n"
                f"{timer_msg}"
            )

        asyncio.create_task(run_cross_loop(source_msgs))
        return

    if lower_text.startswith("/cross stop"):
        CROSS_LOOP_RUNNING = False
        LOOP_END_TIME = None
        save_queue_state(CHANNELS_QUEUE, SKIPPED_QUEUE)
        await safe_reply(
            f"🛑 **Loop stopped & saved.**\n"
            f"• Active: {len(CHANNELS_QUEUE)}\n"
            f"• Skipped: {len(SKIPPED_QUEUE)}"
        )
        return

    if lower_text.startswith("/cross reset"):
        save_queue_state([], [])
        CHANNELS_QUEUE = []
        SKIPPED_QUEUE = []
        PERMANENT_BAD_CHANNELS.clear()
        CROSS_LOOP_RUNNING = False
        LOOP_END_TIME = None
        CURRENT_ROUND = 1
        status_tracker.update({"total": 0, "completed": 0, "skipped": 0, "remaining": 0, "current_channel": "None", "timer_end": "None"})
        await safe_reply("🔄 **Queue, Skipped list & Bad channels reset completed!**")
        return

    if lower_text.startswith("/status"):
        print("📊 Building status report...")
        
        try:
            db = load_analytics()
            
            skip_keys = {
                "saved_queue_state", "saved_skipped_queue", "custom_cross_msg",
                "global_seen_member_ids", "channel_seen_member_ids", 
                "main_channel_baseline_ids"
            }
            
            sorted_channels = []
            for k, v in db.items():
                if k in skip_keys:
                    continue
                if not isinstance(v, dict):
                    continue
                sorted_channels.append((k, v))
            
            sorted_channels = sorted(
                sorted_channels, 
                key=lambda x: x[1].get("total_joins", 0) if isinstance(x[1], dict) else 0, 
                reverse=True
            )

            hot_list, cold_list = [], []
            grand_total = 0
            for k, v in sorted_channels:
                if not isinstance(v, dict):
                    continue
                ch_joins = v.get("total_joins", 0)
                grand_total += ch_joins
                title = v.get("title", "Unknown")
                runs = v.get("runs", 0)
                display_text = f"• {title} +{ch_joins} joins ({runs} runs)"
                if ch_joins > 2:
                    hot_list.append(display_text)
                else:
                    cold_list.append(f"• {title} {ch_joins} join")

            hot_display = "\n".join(hot_list) if hot_list else "No Hot Channels Yet."
            cold_display = "\n".join(cold_list) if cold_list else "No Cold Channels Yet."

            msg_status = f"✅ Set ({len(CUSTOM_CROSS_MSG)} chars)" if CUSTOM_CROSS_MSG else "❌ Not Set"

            status_text = (
                f"📊 **DEVIL ENGINE V7.8 STATUS**\n\n"
                f"• Engine: {'⚡ RUNNING' if CROSS_LOOP_RUNNING else '💤 IDLE'}\n"
                f"• Round: **#{CURRENT_ROUND}**\n"
                f"• Mode: **{status_tracker.get('timer_end', 'None')}**\n"
                f"• Custom Msg: {msg_status}\n"
                f"• ✅ Completed: {status_tracker['completed']}\n"
                f"• ⏭️ Skipped (this round): {len(SKIPPED_QUEUE)}\n"
                f"• 🚫 Permanently Bad: {len(PERMANENT_BAD_CHANNELS)}\n"
                f"• 📋 Active Queue: {len(CHANNELS_QUEUE)}\n"
                f"• 🎯 Current: **{status_tracker['current_channel']}**\n\n"
                f"🎯 **JOIN TRACKING**\n"
                f"• Total Joins: **{grand_total}**\n\n"
                f"🔥 **HOT ({len(hot_list)})**\n{hot_display}\n\n"
                f"❄️ **COLD ({len(cold_list)})**\n{cold_display}"
            )
            
            if len(status_text) > 4000:
                status_text = status_text[:3950] + "\n\n... (Truncated)"

            print(f"📊 Status report built ({len(status_text)} chars), sending...")
            sent = await safe_reply(status_text)
            if sent:
                print("✅ Status sent successfully!")
            else:
                print("❌ Status send FAILED!")
        except Exception as e:
            print(f"❌ Status error: {e}")
            import traceback
            traceback.print_exc()
            await safe_reply(f"❌ **Status Error:** `{str(e)[:200]}`")
        return

# ========================================================
# 🧹 SMART CLEANUP — Owner's all messages + bot's own drop
# ========================================================
async def cleanup_owner_and_own_msgs(our_drop_msg_id, our_custom_reply_id, main_channel_msg_ids, snapshot_before_id):
    """
    SMART CLEANUP:
    
    1. Bot ke apne messages (drop + custom reply) → DELETE
    2. Owner ke SAARE messages jo cross window me aaye (snapshot ke baad) → DELETE
       - Chahe link ho ya text
       - Chahe reply ho ya attach ho
       - Chahe "Join now" ho ya kuch aur
    3. Purani posts → NEVER touch
    4. Bot ka dala hua duplicate link (agar owner ne wahi link dubara dala) → DELETE (bot apna delete kare)
    """
    deleted_main_ids = set()

    try:
        skip_ids = set(main_channel_msg_ids or [])
        if our_drop_msg_id:
            skip_ids.add(our_drop_msg_id)
        if our_custom_reply_id:
            skip_ids.add(our_custom_reply_id)

        # Scan messages after snapshot
        recent_main = []
        try:
            if snapshot_before_id:
                recent_main = await safe_api_call(
                    client.get_messages,
                    TARGET_MAIN_CHANNEL,
                    min_id=snapshot_before_id,
                    limit=50
                )
                if not isinstance(recent_main, list):
                    recent_main = []
            else:
                async for m in client.iter_messages(TARGET_MAIN_CHANNEL, limit=20):
                    recent_main.append(m)
        except Exception as e:
            print(f"⚠️ Main channel scan error: {e}")

        for m in recent_main:
            if not m:
                continue
            msg_id = getattr(m, 'id', None)
            if not msg_id:
                continue

            # Skip our own drop/custom IDs (handled separately)
            if msg_id in skip_ids:
                continue

            raw_text = getattr(m, 'raw_text', '') or getattr(m, 'message', '') or ''

            # Skip our own outgoing messages (bot ke bheje hue)
            if getattr(m, 'out', False):
                continue

            # 🎯 RULE 1: Owner ke SAARE messages delete karo (jo cross window me aaye)
            # Yani koi bhi non-bot message after snapshot_before_id
            # (Link ho, text ho, reply ho, attach ho — sab delete)
            
            # Bot's own drop link ke reply me aaya koi bhi message → delete
            reply_to = getattr(m, 'reply_to_msg_id', None)
            if reply_to and our_drop_msg_id and reply_to == our_drop_msg_id:
                deleted_main_ids.add(msg_id)
                print(f"🗑️ MAIN: Owner reply to our drop: '{raw_text[:40]}' (id={msg_id})")
                continue

            # Promo noise (R.RMV, Next Link, Link etc.)
            if is_promo_noise_message(m):
                deleted_main_ids.add(msg_id)
                print(f"🗑️ MAIN: Promo noise: '{raw_text[:40]}' (id={msg_id})")
                continue

            # No-link sticker
            if not raw_text and getattr(m, 'sticker', None):
                if detect_no_link_from_sticker(m):
                    deleted_main_ids.add(msg_id)
                    print(f"🗑️ MAIN: No-link sticker (id={msg_id})")
                    continue

            # 🎯 RULE 2: Owner ka koi bhi message (jo humne nahi bheja) → delete
            # Kyunki ye cross window me aaya = owner's promo activity
            deleted_main_ids.add(msg_id)
            print(f"🗑️ MAIN: Owner activity cleanup: '{raw_text[:40]}' (id={msg_id})")

        # Delete all collected IDs
        if deleted_main_ids:
            try:
                await safe_api_call(client.delete_messages, TARGET_MAIN_CHANNEL, list(deleted_main_ids))
                print(f"✅ Deleted {len(deleted_main_ids)} messages from MAIN channel")
            except Exception as e:
                print(f"⚠️ Main delete error: {e}")

    except Exception as e:
        print(f"⚠️ cleanup_owner_and_own_msgs error: {e}")

    return deleted_main_ids

# ========================================================
# ⚡ CORE AUTOMATION LOOP ENGINE
# ========================================================
async def run_cross_loop(source_msgs):
    global CROSS_LOOP_RUNNING, status_tracker, CHANNELS_QUEUE, SKIPPED_QUEUE, LOOP_END_TIME, PERMANENT_BAD_CHANNELS, CURRENT_ROUND, CUSTOM_CROSS_MSG

    status_tracker.update({"total": len(CHANNELS_QUEUE), "remaining": len(CHANNELS_QUEUE)})

    while CROSS_LOOP_RUNNING:
        try:
            if LOOP_END_TIME and get_local_now() >= LOOP_END_TIME:
                print("⏱️ Set duration expired! Stopping cross engine cleanly.")
                CROSS_LOOP_RUNNING = False
                LOOP_END_TIME = None
                save_queue_state(CHANNELS_QUEUE, SKIPPED_QUEUE)
                break

            if not CHANNELS_QUEUE:
                if SKIPPED_QUEUE:
                    print(f"🔄 Round #{CURRENT_ROUND} complete! Moving {len(SKIPPED_QUEUE)} skipped back to active...")
                    CHANNELS_QUEUE = list(SKIPPED_QUEUE)
                    SKIPPED_QUEUE = []
                    CURRENT_ROUND += 1
                    status_tracker["total"] = len(CHANNELS_QUEUE)
                    status_tracker["remaining"] = len(CHANNELS_QUEUE)
                    status_tracker["current_channel"] = "Round Refresh"
                    save_queue_state(CHANNELS_QUEUE, SKIPPED_QUEUE)
                    await asyncio.sleep(5)
                    continue
                else:
                    print(f"🔄 All covered! Reloading folder '{FOLDER_TARGET_NAME}'...")
                    channels = await get_folder_channels_safely(FOLDER_TARGET_NAME)
                    if channels:
                        CHANNELS_QUEUE = [c for c in channels if c not in PERMANENT_BAD_CHANNELS]
                        SKIPPED_QUEUE = []
                        CURRENT_ROUND = 1
                        status_tracker["total"] = len(CHANNELS_QUEUE)
                        status_tracker["remaining"] = len(CHANNELS_QUEUE)
                        status_tracker["current_channel"] = "Folder Reload"
                        save_queue_state(CHANNELS_QUEUE, SKIPPED_QUEUE)
                        await asyncio.sleep(10)
                        continue
                    else:
                        print("⚠️ Folder empty. Retry in 20s...")
                        await asyncio.sleep(20)
                        continue

            if not CHANNELS_QUEUE:
                await asyncio.sleep(3)
                continue

            channel_id = CHANNELS_QUEUE[0]
            status_tracker["remaining"] = len(CHANNELS_QUEUE)

            def pop_active_channel():
                global CHANNELS_QUEUE
                if CHANNELS_QUEUE and CHANNELS_QUEUE[0] == channel_id:
                    CHANNELS_QUEUE.pop(0)
                    save_queue_state(CHANNELS_QUEUE, SKIPPED_QUEUE)

            def move_to_skipped():
                global CHANNELS_QUEUE, SKIPPED_QUEUE
                if CHANNELS_QUEUE and CHANNELS_QUEUE[0] == channel_id:
                    CHANNELS_QUEUE.pop(0)
                    if channel_id not in SKIPPED_QUEUE:
                        SKIPPED_QUEUE.append(channel_id)
                    save_queue_state(CHANNELS_QUEUE, SKIPPED_QUEUE)

            if channel_id in PERMANENT_BAD_CHANNELS:
                pop_active_channel()
                continue

            strict_id = int(f"-100{channel_id}" if not str(channel_id).startswith("-100") else channel_id)
            if strict_id == int(TARGET_MAIN_CHANNEL):
                pop_active_channel()
                continue

            real_entity = await safe_api_call(client.get_entity, strict_id)
            if real_entity == "PERMISSION_ERROR" or not real_entity:
                PERMANENT_BAD_CHANNELS.add(channel_id)
                status_tracker["skipped"] += 1
                status_tracker["completed"] += 1
                pop_active_channel()
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

            verify_status, target_link = await verify_and_extract_links(real_entity, messages_to_scan, bio_text=bio)

            if verify_status == 'SKIP':
                print(f"⏭️ Skipped: {ch_title} → Skipped Queue")
                status_tracker["skipped"] += 1
                move_to_skipped()
                continue

            # ============================================================
            # 📝 BIO_FALLBACK
            # ============================================================
            if verify_status == 'BIO_FALLBACK':
                print(f"📝 Bio Fallback: {ch_title} → Main Channel")
                
                fwd_ids = []
                first_fwd_id = None

                snapshot_before_id = None
                try:
                    last_main = await safe_api_call(client.get_messages, TARGET_MAIN_CHANNEL, limit=1)
                    if last_main and isinstance(last_main, list) and len(last_main) > 0:
                        snapshot_before_id = getattr(last_main[0], 'id', None)
                except Exception:
                    pass

                before_joins = await get_join_request_count(TARGET_MAIN_CHANNEL)

                if source_msgs:
                    fwd_msgs = await safe_api_call(client.forward_messages, real_entity, source_msgs[0], silent=False)
                    if fwd_msgs == "PERMISSION_ERROR":
                        PERMANENT_BAD_CHANNELS.add(channel_id)
                        status_tracker["skipped"] += 1
                        pop_active_channel()
                        continue
                    elif fwd_msgs:
                        fwd = fwd_msgs[0] if isinstance(fwd_msgs, list) else fwd_msgs
                        if hasattr(fwd, 'id') and fwd.id:
                            first_fwd_id = fwd.id
                            fwd_ids.append(first_fwd_id)

                main_channel_msg_ids = []
                our_drop_msg_id = None
                our_custom_reply_id = None

                drop = await safe_api_call(client.send_message, TARGET_MAIN_CHANNEL, target_link, silent=True)
                if drop and hasattr(drop, 'id'):
                    our_drop_msg_id = drop.id
                    main_channel_msg_ids.append(drop.id)

                    if CUSTOM_CROSS_MSG:
                        reply_to_id = getattr(drop, 'id', None)
                        custom_reply = await safe_api_call(
                            client.send_message,
                            TARGET_MAIN_CHANNEL,
                            CUSTOM_CROSS_MSG,
                            reply_to=reply_to_id,
                            silent=True
                        )
                        if custom_reply and hasattr(custom_reply, 'id'):
                            our_custom_reply_id = custom_reply.id
                            main_channel_msg_ids.append(custom_reply.id)

                stop_secondary_flag = asyncio.Event()

                async def send_secondary_posts_task_bio():
                    if len(source_msgs) <= 1 or not first_fwd_id:
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

                sec_task = asyncio.create_task(send_secondary_posts_task_bio())

                start_monitor_time = asyncio.get_event_loop().time()
                total_wait_duration = 300

                while (asyncio.get_event_loop().time() - start_monitor_time) < total_wait_duration and CROSS_LOOP_RUNNING:
                    await asyncio.sleep(10)
                    if first_fwd_id:
                        chk_msg = await safe_api_call(client.get_messages, real_entity, ids=first_fwd_id)
                        if not chk_msg or getattr(chk_msg, 'empty', False):
                            break

                stop_secondary_flag.set()
                sec_task.cancel()

                # 🧹 SMART CLEANUP — owner's all msgs + our own
                await cleanup_owner_and_own_msgs(our_drop_msg_id, our_custom_reply_id, main_channel_msg_ids, snapshot_before_id)

                after_joins = await get_join_request_count(TARGET_MAIN_CHANNEL)
                if before_joins is not None and after_joins is not None:
                    joins_gained = max(0, after_joins - before_joins)
                    update_joins_score(channel_id, ch_title, joins_gained)
                    print(f"✅ {ch_title}: {joins_gained} new join requests")

                # Delete our own drop msgs
                if main_channel_msg_ids:
                    await safe_api_call(client.delete_messages, TARGET_MAIN_CHANNEL, main_channel_msg_ids)
                    main_channel_msg_ids.clear()

                # Delete our forwarded post from cross channel
                if fwd_ids:
                    await safe_api_call(client.delete_messages, real_entity, fwd_ids)
                    fwd_ids.clear()

                status_tracker["completed"] += 1
                pop_active_channel()
                await asyncio.sleep(random.randint(5, 10))
                continue

            # ============================================================
            # ✅ SAFE_LINK — Normal cross
            # ============================================================
            fwd_ids = []
            first_fwd_id = None

            snapshot_before_id = None
            try:
                last_main = await safe_api_call(client.get_messages, TARGET_MAIN_CHANNEL, limit=1)
                if last_main and isinstance(last_main, list) and len(last_main) > 0:
                    snapshot_before_id = getattr(last_main[0], 'id', None)
            except Exception:
                pass

            before_joins = await get_join_request_count(TARGET_MAIN_CHANNEL)

            if source_msgs:
                fwd_msgs = await safe_api_call(client.forward_messages, real_entity, source_msgs[0], silent=False)
                if fwd_msgs == "PERMISSION_ERROR":
                    PERMANENT_BAD_CHANNELS.add(channel_id)
                    status_tracker["skipped"] += 1
                    pop_active_channel()
                    continue
                elif fwd_msgs:
                    fwd = fwd_msgs[0] if isinstance(fwd_msgs, list) else fwd_msgs
                    if hasattr(fwd, 'id') and fwd.id:
                        first_fwd_id = fwd.id
                        fwd_ids.append(first_fwd_id)

            if not first_fwd_id:
                status_tracker["skipped"] += 1
                pop_active_channel()
                continue

            main_channel_msg_ids = []
            our_drop_msg_id = None
            our_custom_reply_id = None

            await asyncio.sleep(random.uniform(1.5, 3.5))

            if target_link:
                drop_text = target_link if not target_link.startswith("http") else f"👉 {target_link}"
                drop = await safe_api_call(client.send_message, TARGET_MAIN_CHANNEL, drop_text, silent=True)
                if drop and hasattr(drop, 'id'):
                    our_drop_msg_id = drop.id
                    main_channel_msg_ids.append(drop.id)

                    if CUSTOM_CROSS_MSG:
                        reply_to_id = getattr(drop, 'id', None)
                        custom_reply = await safe_api_call(
                            client.send_message,
                            TARGET_MAIN_CHANNEL,
                            CUSTOM_CROSS_MSG,
                            reply_to=reply_to_id,
                            silent=True
                        )
                        if custom_reply and hasattr(custom_reply, 'id'):
                            our_custom_reply_id = custom_reply.id
                            main_channel_msg_ids.append(custom_reply.id)

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
            total_wait_duration = 300

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

            # 🧹 SMART CLEANUP — owner's all msgs + our own
            await cleanup_owner_and_own_msgs(our_drop_msg_id, our_custom_reply_id, main_channel_msg_ids, snapshot_before_id)

            after_joins = await get_join_request_count(TARGET_MAIN_CHANNEL)
            if before_joins is not None and after_joins is not None:
                joins_gained = max(0, after_joins - before_joins)
                update_joins_score(channel_id, ch_title, joins_gained)
                print(f"✅ {ch_title}: {joins_gained} new join requests")

            # Delete our own drop msgs (bot ka dala hua link)
            if main_channel_msg_ids:
                await safe_api_call(client.delete_messages, TARGET_MAIN_CHANNEL, main_channel_msg_ids)
                main_channel_msg_ids.clear()

            if fwd_ids:
                await safe_api_call(client.delete_messages, real_entity, fwd_ids)
                fwd_ids.clear()

            status_tracker["completed"] += 1
            pop_active_channel()
            await asyncio.sleep(random.randint(5, 10))

        except Exception as global_err:
            print(f"⚠️ Self-Healing Core: Recovered from exception -> {global_err}")
            await asyncio.sleep(5)
            continue

# ========================================================
# 🚀 ENTRY POINT
# ========================================================
async def main():
    global ME_ID, CUSTOM_CROSS_MSG
    if not client.is_connected():
        await client.start()
    me = await client.get_me()
    if me:
        ME_ID = me.id
    CUSTOM_CROSS_MSG = load_custom_cross_msg()
    print("✅ Devil Cross Engine V7.8 SafeGuard+ online.")
    await client.run_until_disconnected()

if __name__ == '__main__':
    asyncio.run(main())
