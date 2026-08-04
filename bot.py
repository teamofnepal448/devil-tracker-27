from telethon import TelegramClient, events, errors
from telethon.sessions import StringSession
from telethon.tl.functions.messages import GetDialogFiltersRequest
from telethon.tl.functions.channels import GetFullChannelRequest
from telethon.tl.types import DialogFilter, PeerChannel, InputMessagesFilterPinned, User, MessageEntityTextUrl
import asyncio
import os
import re
import random
import json
from datetime import datetime
from quart import Quart

app = Quart(__name__)

@app.route('/')
async def home():
    return "Official IPL Titan Live Join-Tracker V5.2: Instant Dual-Drop Sync Active!"

# ========================================================
# CONFIGURATION
# ========================================================
api_id = 36094172
api_hash = "ff6eee1bcccf82daea88c63c45b6b546"
SESSION_STRING = os.environ.get("SESSION_STRING", None)
TARGET_MAIN_CHANNEL = -1002413253133 # DEVIL PREDICTION (Main)
FOLDER_TARGET_NAME = "RAN X CROXX"

if os.path.exists("/data"):
    DB_FILE = "/data/devil_analytics.json"
else:
    DB_FILE = "devil_analytics.json"

if SESSION_STRING:
    client = TelegramClient(StringSession(SESSION_STRING.strip()), api_id, api_hash)
else:
    client = TelegramClient("devil_main_session", api_id, api_hash)

CROSS_LOOP_RUNNING = False
MEMORY_CACHE = {}
CHANNELS_QUEUE = [] 
CROSS_SOURCE_MSGS = [] 

status_tracker = {
    "total": 0, "completed": 0, "skipped": 0, "remaining": 0, "current_channel": "None"
}

# ========================================================
# STORAGE SYSTEM
# ========================================================
def load_analytics():
    global MEMORY_CACHE
    if MEMORY_CACHE: return MEMORY_CACHE
    if os.path.exists(DB_FILE):
        try:
            with open(DB_FILE, "r") as f:
                MEMORY_CACHE = json.load(f)
                return MEMORY_CACHE
        except Exception: pass
    return {}

def save_analytics(data):
    global MEMORY_CACHE
    MEMORY_CACHE = data
    try:
        temp_file = f"{DB_FILE}.tmp"
        with open(temp_file, "w") as f:
            json.dump(data, f, indent=4)
        os.replace(temp_file, DB_FILE)
    except Exception: pass

def save_queue_state(queue_list):
    db = load_analytics()
    db["saved_queue_state"] = queue_list
    save_analytics(db)

def get_saved_queue_state():
    return load_analytics().get("saved_queue_state", [])

def update_joins_score(channel_id, channel_title, joins_gained):
    db = load_analytics()
    ch_key = str(channel_id)
    current_time_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    current_hour = datetime.now().strftime("%I:%M %p")

    if ch_key not in db:
        db[ch_key] = {"title": channel_title, "total_joins": 0, "runs": 0, "time_history": []}

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
    except: pass
    return 0

# ========================================================
# LINK EXTRACTION ENGINE
# ========================================================
async def verify_and_extract_links(current_channel_entity, messages_list, bio_text=""):
    current_username = getattr(current_channel_entity, 'username', '')
    blacklist_words = ["no link", "no cross", "admin remove", "cross off", "no promo"]
    post_text = (bio_text or "") + " "

    for msg in messages_list:
        if msg.raw_text:
            post_text += msg.raw_text + " "
            if any(word in msg.raw_text.lower() for word in blacklist_words):
                return False, None
        
        if msg.entities:
            for ent, txt in msg.get_entities_text():
                if isinstance(ent, MessageEntityTextUrl):
                    post_text += ent.url + " "

    all_links = re.findall(r'(?:https?://)?(?:t\.me|telegram\.me)/(?:joinchat/|addlist/|\+)?[\w\-]+', post_text)
    mentions = re.findall(r'@([\w\-]+)', post_text)
    for m in mentions:
        all_links.append(f"https://t.me/{m}")

    target_link = None
    for link in all_links:
        link_lower = link.lower()
        if "devil" in link_lower or "titan" in link_lower or "bot" in link_lower:
            continue
        
        if not link_lower.startswith("http"):
            link = "https://" + link.replace("@", "")
            
        target_link = link
        break

    if not target_link and current_username:
        target_link = f"https://t.me/{current_username}"
        
    if target_link:
        return True, target_link
        
    return True, "SKIP_DROP"

# ========================================================
# FOLDER CHANNELS SYSTEM
# ========================================================
async def get_folder_channels_safely(target_name, event):
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
                            if raw_id: channel_ids.append(raw_id)
    except: pass
    return list(set(channel_ids))

# ========================================================
# BOT COMMANDS HANDLER
# ========================================================
@client.on(events.NewMessage(chats='me'))
async def controller(event):
    global CROSS_LOOP_RUNNING, CHANNELS_QUEUE, CROSS_SOURCE_MSGS
    text = event.raw_text.strip().lower()

    if text == "/cross start":
        if not event.is_reply:
            await event.reply("⚠️ Main post par reply karke command do!")
            return
        if CROSS_LOOP_RUNNING:
            await event.reply("⚠️ Loop pehle se chal raha hai!")
            return

        reply_msg = await event.get_reply_message()
        
        CROSS_SOURCE_MSGS = [reply_msg]
        try:
            next_msgs = await client.get_messages(event.chat_id, min_id=reply_msg.id, limit=2, reverse=True)
            for m in next_msgs:
                CROSS_SOURCE_MSGS.append(m)
        except Exception as e:
            print("Multi-fetch error:", e)

        CROSS_LOOP_RUNNING = True
        saved_q = get_saved_queue_state()
        if saved_q:
            CHANNELS_QUEUE = saved_q
            await event.reply(f"🔄 **Purana state mila!** Remaining: {len(CHANNELS_QUEUE)} channels.")
        else:
            channels = await get_folder_channels_safely(FOLDER_TARGET_NAME, event)
            if not channels:
                await event.reply(f"❌ Folder '{FOLDER_TARGET_NAME}' khali mila!")
                CROSS_LOOP_RUNNING = False
                return
            random.shuffle(channels)
            db = load_analytics()
            channels.sort(key=lambda c: db.get(str(c), {}).get("total_joins", 0), reverse=True)
            CHANNELS_QUEUE = list(channels)

            status_tracker.update({"total": len(CHANNELS_QUEUE), "completed": 0, "skipped": 0, "remaining": len(CHANNELS_QUEUE)})
            await event.reply(f"🚀 **Multi-Cross Engine Enabled.** (Captured {len(CROSS_SOURCE_MSGS)} posts). Processing {len(CHANNELS_QUEUE)} channels...")

        asyncio.get_event_loop().create_task(run_cross_loop())

    elif text == "/cross stop":
        CROSS_LOOP_RUNNING = False
        save_queue_state(CHANNELS_QUEUE)
        await event.reply("🛑 Loop rok diya gaya hai.")

    elif text == "/status":
        msg = (
            f"📊 **Cross-Promo Live Status:**\n\n"
            f"🔹 **Total Channels in Folder:** {status_tracker['total']}\n"
            f"✅ **Completed Targets:** {status_tracker['completed']}\n"
            f"⏭ **Skipped (No Link/Blacklisted):** {status_tracker['skipped']}\n"
            f"⏳ **Remaining in Queue:** {status_tracker['remaining']}\n"
            f"📍 **Currently Processing:** {status_tracker['current_channel']}\n\n"
            f"🔄 **Engine Running:** {'Yes 🟢' if CROSS_LOOP_RUNNING else 'No 🔴'}"
        )
        await event.reply(msg)

# ========================================================
# CORE AUTOMATION ENGINE (INSTANT DUAL DROP)
# ========================================================
async def run_cross_loop():
    global CROSS_LOOP_RUNNING, status_tracker, CHANNELS_QUEUE, CROSS_SOURCE_MSGS
    retry_count = {}

    while CHANNELS_QUEUE and CROSS_LOOP_RUNNING:
        save_queue_state(CHANNELS_QUEUE) 
        channel_id = CHANNELS_QUEUE.pop(0)
        status_tracker["remaining"] = len(CHANNELS_QUEUE)

        try:
            strict_id = int(f"-100{channel_id}" if not str(channel_id).startswith("-100") else channel_id)
            if strict_id == int(TARGET_MAIN_CHANNEL): continue
            
            try:
                real_entity = await client.get_entity(strict_id)
            except ValueError:
                status_tracker["skipped"] += 1
                status_tracker["completed"] += 1
                continue

            status_tracker["current_channel"] = real_entity.title
            
            messages_to_scan = []
            async for last_msg in client.iter_messages(real_entity, limit=3):
                messages_to_scan.append(last_msg)
            
            bio = ""
            try:
                full_channel = await client(GetFullChannelRequest(real_entity))
                bio = full_channel.full_chat.about or ""
            except: pass

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

            before_joins = await get_current_join_requests(TARGET_MAIN_CHANNEL)
            
            # -----------------------------------------------------
            # FIX: INSTANT MULTI-FORWARD (NO 2-MIN TIMING GAP)
            # -----------------------------------------------------
            fwd_ids = []
            for msg_to_send in CROSS_SOURCE_MSGS:
                try:
                    fwd_msgs = await client.forward_messages(real_entity, msg_to_send)
                    fwd = fwd_msgs[0] if isinstance(fwd_msgs, list) else fwd_msgs
                    fwd_ids.append(fwd.id)
                    await asyncio.sleep(1) # Safety 1-sec gap for Telegram flood control
                except Exception as e:
                    print(f"Error forwarding multi-post part: {e}")

            # -----------------------------------------------------
            # FIX: INSTANT LINK DROP IN MAIN CHANNEL
            # -----------------------------------------------------
            drop = None
            if target_link:
                drop_text = target_link if not target_link.startswith("http") else f"👉 {target_link}"
                drop = await client.send_message(TARGET_MAIN_CHANNEL, drop_text)

            # -----------------------------------------------------
            # 5-MINUTE ANTI-CHEAT MONITORING LOOP
            # -----------------------------------------------------
            wait_time = 300
            check_interval = 15
            early_deleted = False

            for _ in range(int(wait_time / check_interval)):
                await asyncio.sleep(check_interval)
                if not CROSS_LOOP_RUNNING: break
                
                if fwd_ids:
                    try:
                        check_msgs = await client.get_messages(real_entity, ids=[fwd_ids[0]])
                        if not check_msgs or check_msgs[0] is None:
                            early_deleted = True
                            break
                    except Exception:
                        pass

            after_joins = await get_current_join_requests(TARGET_MAIN_CHANNEL)
            update_joins_score(channel_id, real_entity.title, after_joins - before_joins)

            if early_deleted:
                print(f"🚨 {real_entity.title} ne jaldi delete kiya! Apni link remove kar raha hu.")
            
            # Cleanup from both sides
            for f_id in fwd_ids:
                try: await client.delete_messages(real_entity, f_id)
                except: pass

            await asyncio.sleep(random.uniform(0.5, 1.5))

            if drop:
                try: await client.delete_messages(TARGET_MAIN_CHANNEL, drop.id)
                except: pass

            status_tracker["completed"] += 1
            if CHANNELS_QUEUE and CROSS_LOOP_RUNNING:
                sleep_time = random.randint(5, 10) if early_deleted else random.randint(20, 45)
                await asyncio.sleep(sleep_time)

        except errors.FloodWaitError as e:
            await asyncio.sleep(e.seconds + 5)
            CHANNELS_QUEUE.insert(0, channel_id) 
            continue
        except Exception:
            status_tracker["skipped"] += 1
            status_tracker["completed"] += 1
            continue

    if not CHANNELS_QUEUE: save_queue_state([]) 
    CROSS_LOOP_RUNNING = False
    await client.send_message('me', "✅ **Silent Automation Loop Completed!**")

@app.before_serving
async def startup(): await client.start()

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 7860))
    app.run(host="0.0.0.0", port=port)
        
