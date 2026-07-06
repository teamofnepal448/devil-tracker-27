from telethon import TelegramClient, events, errors
from telethon.sessions import StringSession
from telethon.tl.functions.messages import GetDialogFiltersRequest
from telethon.tl.functions.channels import GetFullChannelRequest
from telethon.tl.types import DialogFilter, PeerChannel, InputMessagesFilterPinned
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
    return "Official IPL Titan Live Join-Tracker V2.5: Persistent Engine & Time Analytics Active!"

# ========================================================
# CONFIGURATION (NEW TARGET ID APPLIED)
# ========================================================
api_id = 36094172
api_hash = "ff6eee1bcccf82daea88c63c45b6b546"

SESSION_STRING = os.environ.get("SESSION_STRING", None)
# Naya Target Channel ID strictly configured
TARGET_MAIN_CHANNEL = -1002413253133  

FOLDER_TARGET_NAME = "RAN X CROXX"

# Hugging Face Persistent Storage path check (Restart par data safe rahega)
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

status_tracker = {
    "total": 0, "completed": 0, "skipped": 0, "remaining": 0, "current_channel": "None"
}

# ========================================================
# ASYNC SAFE STORAGE SYSTEM (ANTI-DATA-LOSS)
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
        # Ensures atomic writes so file never gets corrupted
        temp_file = f"{DB_FILE}.tmp"
        with open(temp_file, "w") as f:
            json.dump(data, f, indent=4)
        os.replace(temp_file, DB_FILE)
    except Exception:
        pass

def update_joins_score(channel_id, channel_title, joins_gained):
    db = load_analytics()
    ch_key = str(channel_id)
    
    # Current timestamp for Timing Sense analysis
    current_time_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    current_hour = datetime.now().strftime("%I:%M %p") # Example: 04:00 PM
    
    if ch_key not in db:
        db[ch_key] = {
            "title": channel_title,
            "total_joins": 0,
            "runs": 0,
            "time_history": []
        }
    
    # Handle legacy databases gracefully
    if "time_history" not in db[ch_key]:
        db[ch_key]["time_history"] = []
        
    db[ch_key]["runs"] += 1
    db[ch_key]["total_joins"] += max(0, joins_gained)
    
    # Save current run analytics with time mapping
    db[ch_key]["time_history"].append({
        "timestamp": current_time_str,
        "hour": current_hour,
        "joins": max(0, joins_gained)
    })
    
    save_analytics(db)

# ========================================================
# LIVE JOIN REQUESTS COUNTER
# ========================================================
async def get_current_join_requests(target_channel):
    try:
        full_channel = await client(GetFullChannelRequest(target_channel))
        if hasattr(full_channel.full_chat, 'requests_pending'):
            return full_channel.full_chat.requests_pending or 0
    except Exception:
        pass
    return 0

# ========================================================
# ADVANCE TELEGRAM-ONLY LINK DETECTOR (ANTI-SKIP SYSTEM)
# ========================================================
async def check_and_extract_link(real_entity, channel_username):
    try:
        try:
            pinned_msgs = await client.get_messages(real_entity, filter=InputMessagesFilterPinned(), limit=1)
            if pinned_msgs and pinned_msgs[0].message:
                pin_text = pinned_msgs[0].message.lower()
                if any(word in pin_text for word in ["no link", "no cross", "admin remove", "cross off"]):
                    return False, None
        except Exception:
            pass

        full_channel = await client(GetFullChannelRequest(real_entity))
        bio = full_channel.full_chat.about or ""
        
        if any(word in bio.lower() for word in ["no link", "no cross", "admin remove", "no promo"]):
            return False, None

        self_user = channel_username.lower().strip() if channel_username else "____none____"
        tg_link_pattern = r'(?:t\.me|telegram\.me)/(?:joinchat/|addlist/|\+)?[\w\-]+'

        async for msg in client.iter_messages(real_entity, limit=2):
            if msg.message:
                found_links = re.findall(tg_link_pattern, msg.message.lower())
                for raw_link in found_links:
                    if self_user in raw_link:
                        continue
                    if "bot" in raw_link:
                        continue
                    return False, None

        extracted_links = re.findall(tg_link_pattern, bio)
        if extracted_links:
            return True, f"https://{extracted_links[0]}"
        else:
            if channel_username:
                return True, f"https://t.me/{channel_username}"
            else:
                return True, "SKIP_DROP"
    except Exception:
        return True, "SKIP_DROP"

# ========================================================
# SAFE FOLDER FILTER
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
                            
                            if raw_id:
                                channel_ids.append(raw_id)
    except Exception:
        pass
    return list(set(channel_ids))

# ========================================================
# CONTROLLER & STATUS DISPLAY WITH TIMING SENSE
# ========================================================
@client.on(events.NewMessage(chats='me'))
async def controller(event):
    global CROSS_LOOP_RUNNING
    text = event.raw_text.strip().lower()
    
    if text == "/cross start":
        if not event.is_reply:
            await event.reply("⚠️ Post par reply karke command do bhai!")
            return
        if CROSS_LOOP_RUNNING:
            await event.reply("⚠️ Loop pehle se chal raha hai!")
            return
            
        reply_msg = await event.get_reply_message()
        CROSS_LOOP_RUNNING = True
        asyncio.get_event_loop().create_task(run_cross_loop(reply_msg, event))
        
    elif text == "/cross stop":
        CROSS_LOOP_RUNNING = False
        await event.reply("🛑 Loop rok diya gaya hai.")

    elif text == "/status":
        db = load_analytics()
        sorted_channels = sorted(db.items(), key=lambda x: x[1].get("total_joins", 0), reverse=True)
        
        hot_list = []
        cold_list = []
        
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
            f"• Engine: {'⚡ RUNNING' if CROSS_LOOP_RUNNING else '💤 IDLE'}\n"
            f"• Processed: {status_tracker['completed']} / {status_tracker['total']}\n"
            f"• Current Focus: **{status_tracker['current_channel']}**\n\n"
            f"🔥 **HOT ZONE (Top Gainers + Best Time)**\n{hot_display}\n\n"
            f"❄️ **COLD ZONE (Dead Channels)**\n{cold_display}"
        )
        await event.reply(status_text)

# ========================================================
# DATA-DRIVEN TRACKING ENGINE
# ========================================================
async def run_cross_loop(source_msg, event):
    global CROSS_LOOP_RUNNING, status_tracker
    channels = await get_folder_channels_safely(FOLDER_TARGET_NAME, event)
    
    if not channels:
        await event.reply(f"❌ Folder '{FOLDER_TARGET_NAME}' khali mila!")
        CROSS_LOOP_RUNNING = False
        return

    random.shuffle(channels)
    db = load_analytics()
    channels.sort(key=lambda c: db.get(str(c), {}).get("total_joins", 0), reverse=True)

    status_tracker.update({"total": len(channels), "completed": 0, "skipped": 0, "remaining": len(channels)})
    await event.reply(f"🚀 **Live Join-Tracker Engine Enabled.** Processing {len(channels)} channels...")
    
    channels_queue = list(channels)
    retry_count = {}

    while channels_queue and CROSS_LOOP_RUNNING:
        channel_id = channels_queue.pop(0)
        
        try:
            strict_id = int(f"-100{channel_id}" if not str(channel_id).startswith("-100") else channel_id)
            if strict_id == int(TARGET_MAIN_CHANNEL):
                continue

            real_entity = await client.get_entity(strict_id)
            ch_title = real_entity.title
            status_tracker["current_channel"] = ch_title
            
            is_safe, target_link = await check_and_extract_link(real_entity, getattr(real_entity, 'username', ""))
            
            if not is_safe:
                current_retries = retry_count.get(channel_id, 0)
                if current_retries < 2:
                    retry_count[channel_id] = current_retries + 1
                    channels_queue.append(channel_id)
                    await asyncio.sleep(5)
                else:
                    status_tracker["skipped"] += 1
                continue

            # STEP 1: Get requests count before drop
            before_joins = await get_current_join_requests(TARGET_MAIN_CHANNEL)

            # STEP 2: Forward Post & Drop Link
            fwd_msgs = await client.forward_messages(real_entity, source_msg)
            fwd = fwd_msgs[0] if isinstance(fwd_msgs, list) else fwd_msgs
            
            drop = None
            if target_link and target_link != "SKIP_DROP":
                drop = await client.send_message(TARGET_MAIN_CHANNEL, f"👉 {target_link}")
            
            # STEP 3: Wait 5 minutes
            await asyncio.sleep(300)
            
            # STEP 4: Get requests count after drop
            after_joins = await get_current_join_requests(TARGET_MAIN_CHANNEL)
            
            # STEP 5: Score logging along with time mapping
            gained = after_joins - before_joins
            update_joins_score(channel_id, ch_title, gained)
            
            # Cleanup trace
            try: await client.delete_messages(real_entity, fwd.id)
            except: pass
            if drop:
                try: await client.delete_messages(TARGET_MAIN_CHANNEL, drop.id)
                except: pass

            status_tracker["completed"] += 1
            
            if channels_queue and CROSS_LOOP_RUNNING:
                await asyncio.sleep(random.randint(18, 35))
            
        except errors.FloodWaitError as e:
            await asyncio.sleep(e.seconds + 5)
            channels_queue.append(channel_id)
            continue
        except Exception:
            status_tracker["skipped"] += 1
            continue

    CROSS_LOOP_RUNNING = False
    status_tracker["current_channel"] = "None"
    await client.send_message('me', "✅ **Automation Loop completed! System clean.**")

@app.before_serving
async def startup():
    await client.start()

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 7860))
    app.run(host="0.0.0.0", port=port)
