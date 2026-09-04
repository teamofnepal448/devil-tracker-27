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
from datetime import datetime
from quart import Quart, jsonify, request

app = Quart(__name__)

# ========================================================
# CONFIGURATION & ENV VARIABLES
# ========================================================
API_ID = int(os.environ.get("API_ID", 36094172))
API_HASH = os.environ.get("API_HASH", "ff6eee1bcccf82daea88c63c45b6b546")
SESSION_STRING = os.environ.get("SESSION_STRING", None)

TARGET_MAIN_CHANNEL = int(os.environ.get("TARGET_MAIN_CHANNEL", -1002413253133))
FOLDER_TARGET_NAME = os.environ.get("FOLDER_TARGET_NAME", "RAN X CROXX")

DB_FILE_NAME = os.environ.get("DB_FILE_NAME", "devil_analytics_acc2.json")

if os.path.exists("/data"):
    DB_FILE = f"/data/{DB_FILE_NAME}"
else:
    DB_FILE = DB_FILE_NAME

if SESSION_STRING:
    client = TelegramClient(StringSession(SESSION_STRING.strip()), API_ID, API_HASH)
else:
    client = TelegramClient("devil_main_session_acc2", API_ID, API_HASH)

CROSS_LOOP_RUNNING = False
MEMORY_CACHE = {}
CHANNELS_QUEUE = [] 
CURRENT_SOURCE_MSGS = []

status_tracker = {
    "total": 0, "completed": 0, "skipped": 0, "remaining": 0, "current_channel": "None"
}

# ========================================================
# STORAGE SYSTEM WITH QUEUE PERSISTENCE
# ========================================================
def load_analytics():
    global MEMORY_CACHE
    if MEMORY_CACHE:
        return MEMORY_CACHE
    if os.path.exists(DB_FILE):
        try:
            with open(DB_FILE, "r") as f:
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
        with open(temp_file, "w") as f:
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
    current_time_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    current_hour = datetime.now().strftime("%I:%M %p")

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

async def get_current_join_requests(target_channel):
    try:
        full_channel = await client(GetFullChannelRequest(target_channel))
        if hasattr(full_channel.full_chat, 'requests_pending'):
            return full_channel.full_chat.requests_pending or 0
    except Exception:
        pass
    return 0

# ========================================================
# ADVANCED SAFE LINK RESOLVER & DETECTOR ENGINE
# ========================================================
def extract_link_token(link):
    """Extracts unique token from public or private links for duplicate checking."""
    if not link:
        return ""
    match = re.search(r'(?:t\.me|telegram\.me)/(?:\+|joinchat/|addlist/)?([\w\-]+)', link, re.IGNORECASE)
    return match.group(1).lower() if match else ""

def get_all_links_from_msg(msg):
    links = []
    if not msg:
        return links
        
    if hasattr(msg, 'reply_markup') and msg.reply_markup:
        try:
            if hasattr(msg.reply_markup, 'rows'):
                for row in msg.reply_markup.rows:
                    for button in row.buttons:
                        if hasattr(button, 'url') and button.url:
                            links.append(button.url.strip())
        except Exception:
            pass

    if hasattr(msg, 'entities') and msg.entities:
        for entity in msg.entities:
            if isinstance(entity, MessageEntityTextUrl) and getattr(entity, 'url', None):
                links.append(entity.url.strip())

    raw_text = getattr(msg, 'raw_text', '') or getattr(msg, 'message', '') or ''
    if raw_text:
        tg_pattern = r'(?:https?://)?(?:www\.)?(?:t\.me|telegram\.me)/(?:\+[\w\-]+|joinchat/[\w\-]+|addlist/[\w\-]+|[\w\-]+)'
        raw_matches = re.findall(tg_pattern, raw_text, re.IGNORECASE)
        mentions = re.findall(r'(?<!\w)@([\w\-]+)', raw_text)

        for match in raw_matches:
            clean_link = match if match.lower().startswith('http') else f"https://{match}"
            clean_link = re.sub(r'https?://(?:www\.)?telegram\.me/', 'https://t.me/', clean_link, flags=re.IGNORECASE)
            links.append(clean_link)

        for mention in mentions:
            links.append(f"https://t.me/{mention}")

    return list(set(links))

def check_duplicate_link_in_msg(msg, target_link):
    target_token = extract_link_token(target_link)
    if not target_token:
        return False
    
    extracted_links = get_all_links_from_msg(msg)
    for link in extracted_links:
        if target_token in extract_link_token(link):
            return True
    return False

async def safe_resolve_entity_id(link):
    """Safely resolves channel ID without crashing on private invite links (+ or joinchat)."""
    try:
        invite_match = re.search(r'(?:t\.me|telegram\.me)/(?:\+|joinchat/)([\w\-]+)', link, re.IGNORECASE)
        if invite_match:
            invite_hash = invite_match.group(1)
            res = await client(CheckChatInviteRequest(invite_hash))
            if isinstance(res, (ChatInviteAlready, ChatInvite)):
                return getattr(res.chat, 'id', None)
            return None

        if 'addlist/' in link.lower():
            return None

        resolved = await client.get_entity(link)
        if isinstance(resolved, User):
            return None
        return getattr(resolved, 'id', None)
    except Exception:
        return None

async def verify_and_extract_links(current_channel_entity, messages_list, bio_text=""):
    current_channel_id = current_channel_entity.id
    current_username = getattr(current_channel_entity, 'username', '')
    current_username_lower = current_username.lower().strip() if current_username else "___none___"

    blacklist_words = ["no link", "no cross", "admin remove", "cross off", "no promo", "link not allowed"]

    for msg in messages_list:
        raw_text = getattr(msg, 'raw_text', '') or getattr(msg, 'message', '') or ''
        if raw_text and any(word in raw_text.lower() for word in blacklist_words):
            return False, None

    candidate_links = []
    for msg in messages_list:
        candidate_links.extend(get_all_links_from_msg(msg))

    valid_extracted_link = None
    for raw_link in list(set(candidate_links)):
        link_lower = raw_link.lower().strip()

        if any(b in link_lower for b in ["devil", "titan", "bot"]) or current_username_lower in link_lower:
            continue

        resolved_id = await safe_resolve_entity_id(raw_link)
        if resolved_id:
            if resolved_id == current_channel_id:
                valid_extracted_link = raw_link
            else:
                return False, None

    if valid_extracted_link:
        return True, valid_extracted_link

    if bio_text:
        bio_links = get_all_links_from_msg(type('DummyMsg', (), {'raw_text': bio_text, 'reply_markup': None, 'entities': None})())
        for link in bio_links:
            resolved_id = await safe_resolve_entity_id(link)
            if resolved_id == current_channel_id:
                return True, link

    if current_username:
        return True, f"https://t.me/{current_username}"

    if bio_text and len(bio_text.strip()) > 0:
        return True, bio_text.strip()

    return True, "SKIP_DROP"

# ========================================================
# FOLDER CHANNELS SYSTEM
# ========================================================
async def get_folder_channels_safely(target_name):
    channel_ids = []
    try:
        result = await client(GetDialogFiltersRequest())
        target_clean = str(target_name).strip().lower()
        filters_list = result.filters if hasattr(result, 'filters') else result

        for dialog_filter in filters_list:
            if isinstance(dialog_filter, DialogFilter) and dialog_filter.title:
                folder_title = str(dialog_filter.title.text if hasattr(dialog_filter.title, 'text') else dialog_filter.title).strip()
                if folder_title.lower() == target_clean:
                    if hasattr(dialog_filter, 'include_peers'):
                        for peer in dialog_filter.include_peers:
                            raw_id = None
                            if hasattr(peer, 'channel_id'): raw_id = peer.channel_id
                            elif isinstance(peer, PeerChannel): raw_id = peer.channel_id
                            if raw_id:
                                channel_ids.append(raw_id)
    except Exception:
        pass
    
    return list(set(channel_ids))

# ========================================================
# WEB REST API ENDPOINTS
# ========================================================
@app.route('/')
async def home():
    return jsonify({
        "status": "online",
        "engine": "Official Cross-Promotion Automation Engine V5.2",
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
    global CROSS_LOOP_RUNNING, CHANNELS_QUEUE, CURRENT_SOURCE_MSGS
    if CROSS_LOOP_RUNNING:
        return jsonify({"status": "error", "message": "Engine is already running!"}), 400

    CROSS_LOOP_RUNNING = True
    saved_q = get_saved_queue_state()

    if saved_q:
        CHANNELS_QUEUE = saved_q
    else:
        channels = await get_folder_channels_safely(FOLDER_TARGET_NAME)
        if not channels:
            CROSS_LOOP_RUNNING = False
            return jsonify({"status": "error", "message": f"Folder '{FOLDER_TARGET_NAME}' is empty or not found!"}), 400
        
        random.shuffle(channels)
        db = load_analytics()
        channels.sort(key=lambda c: db.get(str(c), {}).get("total_joins", 0), reverse=True)
        CHANNELS_QUEUE = list(channels)

    status_tracker.update({"total": len(CHANNELS_QUEUE), "completed": 0, "skipped": 0, "remaining": len(CHANNELS_QUEUE), "current_channel": "None"})
    
    asyncio.get_event_loop().create_task(run_cross_loop(CURRENT_SOURCE_MSGS))
    return jsonify({"status": "success", "message": "Cross loop started successfully!", "queue_count": len(CHANNELS_QUEUE)})

@app.route('/api/stop', methods=['POST'])
async def api_stop():
    global CROSS_LOOP_RUNNING
    CROSS_LOOP_RUNNING = False
    save_queue_state(CHANNELS_QUEUE)
    return jsonify({"status": "success", "message": "Loop stopped. Current progress saved."})

@app.route('/api/reset', methods=['POST'])
async def api_reset():
    global CROSS_LOOP_RUNNING, CHANNELS_QUEUE
    CROSS_LOOP_RUNNING = False
    save_queue_state([])
    CHANNELS_QUEUE = []
    status_tracker.update({"total": 0, "completed": 0, "skipped": 0, "remaining": 0, "current_channel": "None"})
    return jsonify({"status": "success", "message": "Queue reset completed."})

# ========================================================
# BOT TELEGRAM COMMANDS HANDLER (FIXED CONTROLLER)
# ========================================================
@client.on(events.NewMessage())
async def controller(event):
    global CROSS_LOOP_RUNNING, CHANNELS_QUEUE, CURRENT_SOURCE_MSGS
    
    # Restrict commands to account owner/self
    me = await client.get_me()
    if event.sender_id != me.id and not event.out:
        return

    if not event.raw_text:
        return
        
    text = event.raw_text.strip().lower()

    if text.startswith("/cross start"):
        if not event.is_reply:
            await event.reply("⚠️ Reply to a post to set promo messages!")
            return
        if CROSS_LOOP_RUNNING:
            await event.reply("⚠️ Loop is already running!")
            return

        reply_msg = await event.get_reply_message()
        CROSS_LOOP_RUNNING = True

        source_msgs = [reply_msg]
        try:
            next_msgs = await client.get_messages(event.chat_id, min_id=reply_msg.id, limit=2, reverse=True)
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
            await event.reply(f"🔄 **Resuming saved state!** Remaining: {len(CHANNELS_QUEUE)} channels.")
        else:
            channels = await get_folder_channels_safely(FOLDER_TARGET_NAME)
            if not channels:
                await event.reply(f"❌ Folder '{FOLDER_TARGET_NAME}' is empty!")
                CROSS_LOOP_RUNNING = False
                return
            random.shuffle(channels)
            db = load_analytics()
            channels.sort(key=lambda c: db.get(str(c), {}).get("total_joins", 0), reverse=True)
            CHANNELS_QUEUE = list(channels)

            status_tracker.update({"total": len(CHANNELS_QUEUE), "completed": 0, "skipped": 0, "remaining": len(CHANNELS_QUEUE), "current_channel": "None"})
            await event.reply(f"🚀 **Multi-Stage Engine V5.2.** Processing {len(CHANNELS_QUEUE)} channels...")

        asyncio.get_event_loop().create_task(run_cross_loop(source_msgs))

    elif text.startswith("/cross stop"):
        CROSS_LOOP_RUNNING = False
        save_queue_state(CHANNELS_QUEUE)
        await event.reply("🛑 Loop stopped & queue saved.")

    elif text.startswith("/cross reset"):
        save_queue_state([])
        CHANNELS_QUEUE = []
        CROSS_LOOP_RUNNING = False
        status_tracker.update({"total": 0, "completed": 0, "skipped": 0, "remaining": 0, "current_channel": "None"})
        await event.reply("🔄 Queue Reset completed!")

    elif text.startswith("/status"):
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

        hot_display = "\n".join(hot_list[:15]) or "No Hot Channels Yet."
        cold_display = "\n".join(cold_list[:15]) or "No Cold Channels Yet."

        status_text = (
            f"📊 **DEVIL LIVE TRACKER STATUS**\n\n"
            f"• Engine: {'⚡ RUNNING' if CROSS_LOOP_RUNNING else '💤 IDLE'}\n"
            f"• Processed: {status_tracker['completed']} / {status_tracker['total']}\n"
            f"• Skipped: {status_tracker['skipped']}\n"
            f"• Remaining: {status_tracker['remaining']}\n"
            f"• Current Focus: **{status_tracker['current_channel']}**\n\n"
            f"🔥 **HOT ZONE**\n{hot_display}\n\n"
            f"❄️ **COLD ZONE**\n{cold_display}"
        )
        await event.reply(status_text)

# ========================================================
# CORE AUTOMATION ENGINE
# ========================================================
async def run_cross_loop(source_msgs):
    global CROSS_LOOP_RUNNING, status_tracker, CHANNELS_QUEUE

    status_tracker.update({"total": len(CHANNELS_QUEUE) + status_tracker['completed'], "remaining": len(CHANNELS_QUEUE)})
    retry_count = {}

    while CHANNELS_QUEUE and CROSS_LOOP_RUNNING:
        save_queue_state(CHANNELS_QUEUE) 

        channel_id = CHANNELS_QUEUE.pop(0)
        status_tracker["remaining"] = len(CHANNELS_QUEUE)

        try:
            strict_id = int(f"-100{channel_id}" if not str(channel_id).startswith("-100") else channel_id)
            if strict_id == int(TARGET_MAIN_CHANNEL):
                continue

            try:
                real_entity = await client.get_entity(strict_id)
            except ValueError:
                status_tracker["skipped"] += 1
                status_tracker["completed"] += 1
                continue

            ch_title = real_entity.title
            status_tracker["current_channel"] = ch_title

            messages_to_scan = []
            try:
                async for last_msg in client.iter_messages(real_entity, limit=4):
                    messages_to_scan.append(last_msg)
                    
                pinned_msgs = await client.get_messages(real_entity, filter=InputMessagesFilterPinned(), limit=1)
                for pm in pinned_msgs:
                    messages_to_scan.append(pm)
            except Exception:
                pass

            bio = ""
            try:
                full_channel = await client(GetFullChannelRequest(real_entity))
                bio = full_channel.full_chat.about or ""
            except Exception:
                pass

            is_safe, target_link = await verify_and_extract_links(real_entity, messages_to_scan, bio_text=bio)

            if not is_safe or not target_link or target_link == "SKIP_DROP":
                current_retries = retry_count.get(channel_id, 0)
                if current_retries < 2:
                    retry_count[channel_id] = current_retries + 1
                    CHANNELS_QUEUE.append(channel_id)
                else:
                    status_tracker["skipped"] += 1
                    status_tracker["completed"] += 1
                continue

            fwd_ids = []
            first_fwd_id = None

            if source_msgs:
                try:
                    fwd_msgs = await client.forward_messages(real_entity, source_msgs[0])
                    fwd = fwd_msgs[0] if isinstance(fwd_msgs, list) else fwd_msgs
                    if hasattr(fwd, 'id') and fwd.id:
                        first_fwd_id = fwd.id
                        fwd_ids.append(first_fwd_id)
                except Exception:
                    pass

            if not first_fwd_id:
                status_tracker["skipped"] += 1
                status_tracker["completed"] += 1
                continue

            before_joins = await get_current_join_requests(TARGET_MAIN_CHANNEL)
            await asyncio.sleep(random.uniform(1.5, 3.8))

            target_drop_ids = []
            bot_drop_id = None
            if target_link:
                drop_text = target_link if not target_link.startswith("http") else f"👉 {target_link}"
                drop = await client.send_message(TARGET_MAIN_CHANNEL, drop_text)
                if drop:
                    bot_drop_id = drop.id
                    target_drop_ids.append(drop.id)

            stop_secondary_flag = asyncio.Event()

            async def send_secondary_posts_task():
                if len(source_msgs) <= 1:
                    return
                for msg in source_msgs[1:]:
                    post_delay = random.randint(60, 180)
                    elapsed = 0
                    while elapsed < post_delay:
                        if stop_secondary_flag.is_set() or not CROSS_LOOP_RUNNING:
                            return
                        await asyncio.sleep(2)
                        elapsed += 2

                    if stop_secondary_flag.is_set() or not CROSS_LOOP_RUNNING:
                        return

                    try:
                        chk = await client.get_messages(real_entity, ids=first_fwd_id)
                        if not chk or getattr(chk, 'empty', False):
                            stop_secondary_flag.set()
                            return
                    except Exception:
                        stop_secondary_flag.set()
                        return

                    try:
                        if msg.media:
                            sec_fwd = await client.send_message(real_entity, msg.message or "", file=msg.media, reply_to=first_fwd_id)
                        else:
                            sec_fwd = await client.send_message(real_entity, msg.message or "", reply_to=first_fwd_id)
                        if sec_fwd:
                            fwd_ids.append(sec_fwd.id)
                    except Exception:
                        try:
                            fwd_msgs = await client.forward_messages(real_entity, msg)
                            sec_fwd = fwd_msgs[0] if isinstance(fwd_msgs, list) else fwd_msgs
                            if sec_fwd:
                                fwd_ids.append(sec_fwd.id)
                        except Exception:
                            pass

            sec_task = asyncio.create_task(send_secondary_posts_task())

            start_monitor_time = asyncio.get_event_loop().time()
            total_wait_duration = 300

            while (asyncio.get_event_loop().time() - start_monitor_time) < total_wait_duration and CROSS_LOOP_RUNNING:
                await asyncio.sleep(random.uniform(10, 15))

                try:
                    chk_msg = await client.get_messages(real_entity, ids=first_fwd_id)
                    if not chk_msg or getattr(chk_msg, 'empty', False):
                        break
                except Exception:
                    break

                if target_link:
                    try:
                        recent_main = await client.get_messages(TARGET_MAIN_CHANNEL, limit=5)
                        for rm in recent_main:
                            if rm.id not in target_drop_ids:
                                if check_duplicate_link_in_msg(rm, target_link):
                                    if bot_drop_id and bot_drop_id in target_drop_ids:
                                        await client.delete_messages(TARGET_MAIN_CHANNEL, bot_drop_id)
                                        target_drop_ids.remove(bot_drop_id)
                                        bot_drop_id = None
                                    target_drop_ids.append(rm.id)
                    except Exception:
                        pass

            stop_secondary_flag.set()
            sec_task.cancel()
            try:
                await sec_task
            except (asyncio.CancelledError, Exception):
                pass

            after_joins = await get_current_join_requests(TARGET_MAIN_CHANNEL)
            joins_gained = max(0, after_joins - before_joins)
            update_joins_score(channel_id, ch_title, joins_gained)

            for t_id in target_drop_ids:
                try:
                    await client.delete_messages(TARGET_MAIN_CHANNEL, t_id)
                except Exception:
                    pass

            for f_id in fwd_ids:
                try:
                    await client.delete_messages(real_entity, f_id)
                except Exception:
                    pass

            status_tracker["completed"] += 1
            await asyncio.sleep(random.randint(10, 30))

        except Exception:
            status_tracker["skipped"] += 1
            status_tracker["completed"] += 1
            await asyncio.sleep(5)

# ========================================================
# STARTUP HOOK & LIFECYCLE
# ========================================================
@app.before_serving
async def startup():
    print("🚀 Starting Telegram Client via Quart Lifecycle...")
    await client.start()
    print("✅ Telegram Client Connected Successfully!")

if __name__ == '__main__':
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port)
