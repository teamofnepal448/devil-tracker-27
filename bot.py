"""
IPL Titan Live Join-Tracker V2.8 - Optimized Version with Local Database
Strict Third-Party Link Skipper Active & Full Analytics Restored
Optimized for 24/7 Render Cloud Deployment with Quart Port Binding Fix
"""

from telethon import TelegramClient, events, errors
from telethon.sessions import StringSession
from telethon.tl.functions.messages import GetDialogFiltersRequest
from telethon.tl.functions.channels import GetFullChannelRequest
from telethon.tl.types import (
    DialogFilter,
    PeerChannel,
    InputMessagesFilterPinned,
    MessageEntityTextUrl,
    MessageEntityUrl,
)
import asyncio
import os
import re
import random
import json
from dataclasses import dataclass
from typing import Optional, List, Tuple
from datetime import datetime

# Quart is used here for port binding as it is already in requirements.txt
from quart import Quart
import uvicorn

app = Quart(__name__)

@app.route("/")
async def read_root():
    return {"status": "running", "bot": "IPL Titan Live Join-Tracker V2.8"}

# ============================================================
# CONFIGURATION
# ============================================================
@dataclass(frozen=True)
class BotConfig:
    """Immutable bot configuration"""
    API_ID: int = 36094172
    API_HASH: str = "ff6eee1bcccf82daea88c63c45b6b546"
    TARGET_MAIN_CHANNEL: int = -1002413253133  # Strict Target Channel ID Updated!
    FOLDER_TARGET_NAME: str = "RAN X CROXX"
    FORWARD_DELAY_SECONDS: int = 300
    LOOP_DELAY_MIN: int = 18
    LOOP_DELAY_MAX: int = 35
    MAX_RETRIES: int = 2
    MESSAGES_TO_SCAN: int = 3
    # Persistent storage path for Render if using disk, else falls back to local
    DB_FILE: str = "/data/devil_analytics.json" if os.path.exists("/data") else "devil_analytics.json"

CONFIG = BotConfig()

# Compiled regex patterns for performance
TG_LINK_PATTERN = re.compile(
    r'(?:t\.me|telegram\.me)/(?:joinchat/|addlist/|\+)?[\w\-]+',
    re.IGNORECASE
)

BLOCKED_KEYWORDS = frozenset([
    "no link", "no cross", "admin remove", "cross off", "no promo"
])

SAFE_KEYWORDS = frozenset(["devil_prediction", "bot", "titan"])

# ============================================================
# LOCAL DATABASE & STORAGE SYSTEM
# ============================================================
MEMORY_CACHE = {}

def load_analytics() -> dict:
    global MEMORY_CACHE
    if MEMORY_CACHE:
        return MEMORY_CACHE
    if os.path.exists(CONFIG.DB_FILE):
        try:
            with open(CONFIG.DB_FILE, "r") as f:
                MEMORY_CACHE = json.load(f)
                return MEMORY_CACHE
        except Exception:
            pass
    return {}

def save_analytics(data: dict):
    global MEMORY_CACHE
    MEMORY_CACHE = data
    try:
        temp_file = f"{CONFIG.DB_FILE}.tmp"
        with open(temp_file, "w") as f:
            json.dump(data, f, indent=4)
        os.replace(temp_file, CONFIG.DB_FILE)
    except Exception:
        pass

def save_queue_state(queue_list: list):
    db = load_analytics()
    db["saved_queue_state"] = queue_list
    save_analytics(db)

def get_saved_queue_state() -> list:
    db = load_analytics()
    return db.get("saved_queue_state", [])

def update_joins_score(channel_id: int, channel_title: str, joins_gained: int):
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

# ============================================================
# STATUS TRACKER
# ============================================================
@dataclass
class StatusTracker:
    """Mutable status tracking"""
    total: int = 0
    completed: int = 0
    skipped: int = 0
    remaining: int = 0
    current_channel: str = "None"
    is_running: bool = False

status_tracker = StatusTracker()
channels_queue: List[int] = []
cross_loop_running = False

# ============================================================
# CLIENT SETUP
# ============================================================
def create_client() -> TelegramClient:
    session_string = os.environ.get("SESSION_STRING")
    if session_string:
        return TelegramClient(
            StringSession(session_string.strip()),
            CONFIG.API_ID,
            CONFIG.API_HASH
        )
    return TelegramClient("devil_main_session", CONFIG.API_ID, CONFIG.API_HASH)

client = create_client()

# ============================================================
# HELPER FUNCTIONS
# ============================================================
def is_third_party_link(url: str, self_username: str) -> bool:
    url_lower = url.lower()
    if self_username and self_username.lower() in url_lower:
        return False
    return not any(keyword in url_lower for keyword in SAFE_KEYWORDS)

def contains_blocked_keywords(text: str) -> bool:
    text_lower = text.lower()
    return any(keyword in text_lower for keyword in BLOCKED_KEYWORDS)

async def get_current_join_requests(channel_entity) -> int:
    try:
        full_channel = await client(GetFullChannelRequest(channel_entity))
        return getattr(full_channel.full_chat, 'requests_pending', 0) or 0
    except Exception:
        return 0

# ============================================================
# LINK EXTRACTION AND VALIDATION
# ============================================================
async def extract_links_from_message(msg_obj, self_username: str) -> Tuple[bool, Optional[str]]:
    if not msg_obj:
        return True, None
    
    if msg_obj.entities:
        for entity in msg_obj.entities:
            if isinstance(entity, MessageEntityTextUrl) and entity.url:
                if TG_LINK_PATTERN.search(entity.url):
                    if is_third_party_link(entity.url, self_username):
                        return False, None
                    return True, entity.url
            elif isinstance(entity, MessageEntityUrl) and msg_obj.message:
                extracted = msg_obj.message[entity.offset:entity.offset + entity.length]
                if TG_LINK_PATTERN.search(extracted):
                    if is_third_party_link(extracted, self_username):
                        return False, None
                    return True, f"https://{extracted}" if not extracted.startswith('http') else extracted
    
    if msg_obj.buttons:
        for row in msg_obj.buttons:
            for button in row:
                if button.url and TG_LINK_PATTERN.search(button.url):
                    if is_third_party_link(button.url, self_username):
                        return False, None
                    return True, button.url
    
    if msg_obj.message:
        matches = TG_LINK_PATTERN.findall(msg_obj.message)
        for match in matches:
            full_url = f"https://{match}"
            if is_third_party_link(full_url, self_username):
                return False, None
            return True, full_url
            
    return True, None

async def check_channel_safety(entity, username: str, msg_obj=None) -> Tuple[bool, Optional[str]]:
    try:
        try:
            pinned = await client.get_messages(entity, filter=InputMessagesFilterPinned(), limit=1)
            if pinned and pinned[0].message:
                if contains_blocked_keywords(pinned[0].message):
                    return False, None
        except Exception:
            pass

        full_channel = await client(GetFullChannelRequest(entity))
        bio = full_channel.full_chat.about or ""
        if contains_blocked_keywords(bio):
            return False, None
            
        self_user = username.lower().strip() if username else ""
        
        if msg_obj:
            is_safe, link = await extract_links_from_message(msg_obj, self_user)
            if not is_safe:
                return False, None
            if link:
                return True, link
                
        bio_links = TG_LINK_PATTERN.findall(bio)
        if bio_links:
            link = f"https://{bio_links[0]}"
            if is_third_party_link(link, self_user):
                return False, None
            return True, link
            
        if username:
            return True, f"https://t.me/{username}"
            
        return True, "SKIP_DROP"
    except Exception:
        return True, "SKIP_DROP"

# ============================================================
# FOLDER CHANNELS RETRIEVAL
# ============================================================
async def get_folder_channels(target_name: str) -> List[int]:
    channel_ids = []
    try:
        result = await client(GetDialogFiltersRequest())
        target_clean = target_name.strip().lower()
        filters_list = getattr(result, 'filters', result)
        
        for dialog_filter in filters_list:
            if not isinstance(dialog_filter, DialogFilter) or not dialog_filter.title:
                continue
            
            title = dialog_filter.title
            if hasattr(title, 'text'):
                title = title.text
                
            if str(title).strip().lower() != target_clean:
                continue
                
            if hasattr(dialog_filter, 'include_peers'):
                for peer in dialog_filter.include_peers:
                    raw_id = None
                    if hasattr(peer, 'channel_id'):
                        raw_id = peer.channel_id
                    elif isinstance(peer, PeerChannel):
                        raw_id = peer.channel_id
                    if raw_id:
                        channel_ids.append(raw_id)
    except Exception as e:
        print(f"Error getting folder channels: {e}")
    return list(set(channel_ids))

# ============================================================
# MAIN AUTOMATION LOOP
# ============================================================
async def run_cross_loop(source_msg, event):
    global cross_loop_running, channels_queue, status_tracker
    
    status_tracker.total = len(channels_queue) + status_tracker.completed
    status_tracker.remaining = len(channels_queue)
    retry_count: dict[int, int] = {}
    
    while channels_queue and cross_loop_running:
        save_queue_state(channels_queue)  # Progress Saved instantly!
        channel_id = channels_queue.pop(0)
        
        try:
            strict_id = int(f"-100{channel_id}" if not str(channel_id).startswith("-100") else channel_id)
            if strict_id == CONFIG.TARGET_MAIN_CHANNEL:
                continue
                
            entity = await client.get_entity(strict_id)
            title = entity.title
            username = getattr(entity, 'username', "")
            status_tracker.current_channel = title
            
            is_safe = True
            target_link = None
            
            async for msg in client.iter_messages(entity, limit=CONFIG.MESSAGES_TO_SCAN):
                safe_check, extracted = await check_channel_safety(entity, username, msg)
                if not safe_check:
                    is_safe = False
                    break
                if extracted and extracted != "SKIP_DROP" and not target_link:
                    target_link = extracted
                    
            if is_safe and not target_link:
                is_safe, target_link = await check_channel_safety(entity, username)
                
            if not is_safe:
                retries = retry_count.get(channel_id, 0)
                if retries < CONFIG.MAX_RETRIES:
                    retry_count[channel_id] = retries + 1
                    channels_queue.append(channel_id)
                    await asyncio.sleep(5)
                else:
                    status_tracker.skipped += 1
                continue
                
            # ===== FORWARD SECTION =====
            before_joins = await get_current_join_requests(CONFIG.TARGET_MAIN_CHANNEL)
            
            fwd_result = await client.forward_messages(entity, source_msg)
            fwd_msg = fwd_result[0] if isinstance(fwd_result, list) else fwd_result
            
            drop_msg = None
            if target_link and target_link != "SKIP_DROP":
                drop_msg = await client.send_message(CONFIG.TARGET_MAIN_CHANNEL, f"👉 {target_link}")
                
            await asyncio.sleep(CONFIG.FORWARD_DELAY_SECONDS)
            
            after_joins = await get_current_join_requests(CONFIG.TARGET_MAIN_CHANNEL)
            gained = after_joins - before_joins
            
            # Log analytics locally to database
            update_joins_score(channel_id, title, gained)
            print(f"[{title}] Gained: {gained} joins")
            
            try:
                await client.delete_messages(entity, fwd_msg.id)
            except Exception:
                pass
            if drop_msg:
                try:
                    await client.delete_messages(CONFIG.TARGET_MAIN_CHANNEL, drop_msg.id)
                except Exception:
                    pass
                    
            status_tracker.completed += 1
            status_tracker.remaining = len(channels_queue)
            
            if channels_queue and cross_loop_running:
                delay = random.randint(CONFIG.LOOP_DELAY_MIN, CONFIG.LOOP_DELAY_MAX)
                await asyncio.sleep(delay)
                
        except errors.FloodWaitError as e:
            print(f"FloodWait: Sleeping for {e.seconds}s")
            await asyncio.sleep(e.seconds + 5)
            channels_queue.insert(0, channel_id)
            continue
        except Exception as e:
            print(f"Error processing {channel_id}: {e}")
            status_tracker.skipped += 1
            continue
            
    if not channels_queue:
        save_queue_state([])
        
    cross_loop_running = False
    status_tracker.current_channel = "None"
    status_tracker.is_running = False
    await client.send_message('me', "✅ **Automation Loop Completed!**")

# ============================================================
# COMMAND HANDLERS
# ============================================================
@client.on(events.NewMessage(chats='me'))
async def controller(event):
    global cross_loop_running, channels_queue, status_tracker
    
    text = event.raw_text.strip().lower()
    
    if text == "/cross start":
        if not event.is_reply:
            await event.reply("⚠️ Reply to a post with this command!")
            return
            
        if cross_loop_running:
            await event.reply("⚠️ Loop is already running!")
            return
            
        reply_msg = await event.get_reply_message()
        cross_loop_running = True
        status_tracker.is_running = True
        
        saved_q = get_saved_queue_state()
        if saved_q:
            channels_queue = saved_q
            await event.reply(f"🔄 **Purana session mila!** Continuing with remaining {len(channels_queue)} channels.")
        else:
            channels = await get_folder_channels(CONFIG.FOLDER_TARGET_NAME)
            if not channels:
                await event.reply(f"❌ Folder '{CONFIG.FOLDER_TARGET_NAME}' is empty!")
                cross_loop_running = False
                status_tracker.is_running = False
                return
                
            random.shuffle(channels)
            db = load_analytics()
            # Hot zone order prioritization
            channels.sort(key=lambda c: db.get(str(c), {}).get("total_joins", 0), reverse=True)
            channels_queue = channels
            await event.reply(f"🚀 **Live Join-Tracker Started!**\nProcessing {len(channels_queue)} channels...")
            
        asyncio.create_task(run_cross_loop(reply_msg, event))
        
    elif text == "/cross stop":
        cross_loop_running = False
        status_tracker.is_running = False
        save_queue_state(channels_queue)
        await event.reply("🛑 Loop stopped. Current progress saved locally.")
        
    elif text == "/cross reset":
        save_queue_state([])
        channels_queue = []
        cross_loop_running = False
        status_tracker = StatusTracker()
        await event.reply("🔄 Queue reset! Next run will start fresh.")
        
    elif text == "/status":
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
            
            display_text = f"• {v['title']}  +{v['total_joins']} joins{time_log}"
            if v["total_joins"] > 2:
                hot_list.append(display_text)
            else:
                cold_list.append(f"• {v['title']}  {v['total_joins']} join")
                
        hot_display = "\n".join(hot_list[:5]) or "No Hot Channels Yet."
        cold_display = "\n".join(cold_list[:5]) or "No Cold Channels Yet."
        
        status_text = (
            f"📊 **DEVIL LIVE TRACKER STATUS**\n\n"
            f"• Engine: {'⚡ RUNNING' if cross_loop_running else '💤 IDLE'}\n"
            f"• Processed: {status_tracker.completed} / {status_tracker.total}\n"
            f"• Skipped: {status_tracker.skipped}\n"
            f"• Remaining: {status_tracker.remaining}\n"
            f"• Current Focus: **{status_tracker.current_channel}**\n\n"
            f"🔥 **HOT ZONE (Top Gainers + Best Time)**\n{hot_display}\n\n"
            f"❄️ **COLD ZONE (Dead Channels)**\n{cold_display}"
        )
        await event.reply(status_text)

# ============================================================
# MAIN ENTRY POINT (Render Compatible)
# ============================================================
async def run_telethon():
    print("🚀 IPL Titan Live Join-Tracker V2.8 Starting on Telethon...")
    await client.start()
    print("✅ Telethon client connected!")
    await client.run_until_disconnected()

async def main():
    # Render requirements ke liye parallel run system (Uvicorn + Telethon Client)
    port = int(os.environ.get("PORT", 8000))
    config = uvicorn.Config(app, host="0.0.0.0", port=port, log_level="warning")
    server = uvicorn.Server(config)
    
    # Telethon aur Quart Web App dono ko parallel chalayein
    await asyncio.gather(
        server.serve(),
        run_telethon()
    )

if __name__ == "__main__":
    asyncio.run(main())
    
