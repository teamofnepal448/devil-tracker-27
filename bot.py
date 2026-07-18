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
    return "Official IPL Titan Live Join-Tracker V3.6: ID Resolution Engine Active!"

# ========================================================
# CONFIGURATION
# ========================================================
api_id = 36094172
api_hash = "ff6eee1bcccf82daea88c63c45b6b546"

SESSION_STRING = os.environ.get("SESSION_STRING", None)
TARGET_MAIN_CHANNEL = -1002413253133  
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
# DIRECT ID RESOLUTION DETECTOR (FOOLPROOF CLICK & COMPARE)
# ========================================================
async def verify_and_extract_links(current_channel_entity, messages_list, bio_text=""):
    """
    Post ya bio me se saare links nikal kar unhe 'click' (resolve) karega.
    Agar koi link kisi teesre bande ke channel par le jata hai -> (False, None, Reason)
    Agar link usi channel ka hai ya safe hai -> (True, Final_Link, Reason)
    """
    current_channel_id = current_channel_entity.id
    current_username = getattr(current_channel_entity, 'username', '')
    current_username_lower = current_username.lower().strip() if current_username else "___none___"

    # Pinned ya post me text blacklist check karne ke liye helper
    blacklist_words = ["no link", "no cross", "admin remove", "cross off", "no promo"]
    
    # 1. Extract all potential Telegram tokens/links from text
    all_text = bio_text + " "
    for msg in messages_list:
        if msg.message:
            all_text += msg.message + " "
            # Pinned Word check shortcut
            if any(word in msg.message.lower() for word in blacklist_words):
                return False, None, "❌ Blacklist word detected in channel text"

    # Find raw links and @usernames
    tg_links = re.findall(r'(?:t\.me|telegram\.me)/(?:joinchat/|addlist/|\+)?([\w\-]+)', all_text)
    mentions = re.findall(r'@([\w\-]+)', all_text)
    
    combined_tokens = list(set(tg_links + mentions))
    
    valid_extracted_link = None

    # 2. Click/Resolve each link to check destination ID
    for token in combined_tokens:
        token_clean = token.lower().strip()
        
        # Whitelist filters to avoid unnecessary API calls on manager/bot names
        if "devil" in token_clean or "titan" in token_clean or "bot" in token_clean or token_clean == current_username_lower:
            continue
            
        try:
            # Direct link resolution simulation (Simulating the click)
            resolved_entity = await client.get_entity(token)
            resolved_id = resolved_entity.id
            
            # AGE MATCH: Agar ID match nahi hui matlab teesra banda hai!
            if resolved_id != current_channel_id:
                return False, None, f"❌ Link/Mention points to third-party ID: {resolved_id} (@{token})"
            else:
                # Agar usi channel ka link hai to drop ke liye save kar lo
                if token in tg_links:
                    valid_extracted_link = f"https://t.me/{token}"
        except Exception:
            # Agar resolve nahi ho raha (invalid hash, external string, private group link without access),
            # toh use third-party dangerous nahi manenge, default safe chord denge.
            continue

    # 3. Final link assignment logic
    if valid_extracted_link:
        return True, valid_extracted_link, "✅ Verified Safe: Link points to the same channel"
        
    if current_username:
        return True, f"https://t.me/{current_username}", "ℹ️ Safe: Falling back to official channel username link"
        
    return True, "SKIP_DROP", "⚠️ Safe but no drop link could be fetched"

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
                            if raw_id:
                                channel_ids.append(raw_id)
    except Exception:
        pass
    return list(set(channel_ids))

# ========================================================
# BOT COMMANDS HANDLER
# ========================================================
@client.on(events.NewMessage(chats='me'))
async def controller(event):
    global CROSS_LOOP_RUNNING, CHANNELS_QUEUE
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
        
        saved_q = get_saved_queue_state()
        if saved_q:
            CHANNELS_QUEUE = saved_q
            await event.reply(f"🔄 **Purana state mila!** Wahi se continue kar raha hu. Remaining: {len(CHANNELS_QUEUE)} channels.")
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
            
            status_tracker.update({"total": len(CHANNELS_QUEUE), "completed": 0, "skipped": 0, "remaining": len(CHANNELS_QUEUE), "current_channel": "None"})
            await event.reply(f"🚀 **Live Join-Tracker Engine Enabled.** Processing {len(CHANNELS_QUEUE)} channels...")

        asyncio.get_event_loop().create_task(run_cross_loop(reply_msg, event))
        
    elif text == "/cross stop":
        CROSS_LOOP_RUNNING = False
        save_queue_state(CHANNELS_QUEUE)
        await event.reply("🛑 Loop rok diya gaya hai. Current progress save kar li gayi hai.")

    elif text == "/cross reset":
        save_queue_state([])
        CHANNELS_QUEUE = []
        CROSS_LOOP_RUNNING = False
        status_tracker.update({"total": 0, "completed": 0, "skipped": 0, "remaining": 0, "current_channel": "None"})
        await event.reply("🔄 Queue Reset completed!")

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
            f"• Engine: {'⚡ RUNNING' if CROSS_LOOP_RUNNING else '💤 IDLE'}\n"
            f"• Processed: {status_tracker['completed']} / {status_tracker['total']}\n"
            f"• Skipped: {status_tracker['skipped']}\n"
            f"• Remaining: {status_tracker['remaining']}\n"
            f"• Current Focus: **{status_tracker['current_channel']}**\n\n"
            f"🔥 **HOT ZONE (Top Gainers + Best Time)**\n{hot_display}\n\n"
            f"❄️ **COLD ZONE (Dead Channels)**\n{cold_display}"
        )
        await event.reply(status_text)

# ========================================================
# CORE AUTOMATION ENGINE 
# ========================================================
async def run_cross_loop(source_msg, event):
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
                await client.send_message('me', f"❌ [ENTITY ERROR SKIP] **ID: {channel_id}**\nReason: Channel not cached or requires manual access.")
                continue

            ch_title = real_entity.title
            status_tracker["current_channel"] = ch_title
            
            # 1. Fetch Pinned & Last 3 messages for structural ID scanning
            messages_to_scan = []
            try:
                async for last_msg in client.iter_messages(real_entity, limit=3):
                    messages_to_scan.append(last_msg)
                pinned_msgs = await client.get_messages(real_entity, filter=InputMessagesFilterPinned(), limit=1)
                if pinned_msgs:
                    messages_to_scan.append(pinned_msgs[0])
            except Exception:
                pass

            # 2. Extract Bio Text (Only for link collection, no bio skipping applied)
            bio = ""
            try:
                full_channel = await client(GetFullChannelRequest(real_entity))
                bio = full_channel.full_chat.about or ""
            except Exception:
                pass

            # 3. Verify Links via Instant ID Verification (Click simulation)
            is_safe, target_link, debug_reason = await verify_and_extract_links(real_entity, messages_to_scan, bio_text=bio)

            # EXECUTION SKIPPING AND RETRY MANAGEMENT
            if not is_safe or not target_link or target_link == "SKIP_DROP":
                current_retries = retry_count.get(channel_id, 0)
                if current_retries < 2:
                    retry_count[channel_id] = current_retries + 1
                    CHANNELS_QUEUE.append(channel_id)
                    await client.send_message('me', f"🔄 [RETRY {current_retries+1}/2] **{ch_title}**\nReason: {debug_reason}")
                else:
                    status_tracker["skipped"] += 1
                    status_tracker["completed"] += 1
                    await client.send_message('me', f"⏭️ [SKIPPED PERMANENTLY] **{ch_title}**\nReason: {debug_reason}")
                continue

            # ----- FORWARD SECTOR -----
            await client.send_message('me', f"📢 [PROCESSING] **{ch_title}**\nAction: Link Verified Safe (Points to Target Channel)\nLink to Drop: {target_link}")

            before_joins = await get_current_join_requests(TARGET_MAIN_CHANNEL)

            fwd_msgs = await client.forward_messages(real_entity, source_msg)
            fwd = fwd_msgs[0] if isinstance(fwd_msgs, list) else fwd_msgs
            
            drop = None
            if target_link:
                drop = await client.send_message(TARGET_MAIN_CHANNEL, f"👉 {target_link}")
            
            await asyncio.sleep(300)
            
            after_joins = await get_current_join_requests(TARGET_MAIN_CHANNEL)
            gained = after_joins - before_joins
            update_joins_score(channel_id, ch_title, gained)
            
            try: await client.delete_messages(real_entity, fwd.id)
            except: pass
            if drop:
                try: await client.delete_messages(TARGET_MAIN_CHANNEL, drop.id)
                except: pass

            status_tracker["completed"] += 1
            if CHANNELS_QUEUE and CROSS_LOOP_RUNNING:
                await asyncio.sleep(random.randint(18, 35))
            
        except errors.FloodWaitError as e:
            await client.send_message('me', f"⏳ [FLOOD WAIT] Waiting {e.seconds}s. Pushing channel back to queue.")
            await asyncio.sleep(e.seconds + 5)
            CHANNELS_QUEUE.insert(0, channel_id)  
            continue
        except Exception as e:
            status_tracker["skipped"] += 1
            status_tracker["completed"] += 1
            await client.send_message('me', f"❌ [ERROR SKIP] **Channel ID: {channel_id}**\nError: {str(e)}")
            continue

    if not CHANNELS_QUEUE:
        save_queue_state([])  

    CROSS_LOOP_RUNNING = False
    status_tracker["current_channel"] = "None"
    
    summary_text = (
        f"✅ **Automation Loop Processed Successfully!**\n\n"
        f"📊 **Final Summary:**\n"
        f"• Total Channels in Folder: {status_tracker['total']}\n"
        f"• Successfully Processed: {status_tracker['completed'] - status_tracker['skipped']}\n"
        f"• Skipped / Third-Party IDs: {status_tracker['skipped']}"
    )
    await client.send_message('me', summary_text)

@app.before_serving
async def startup():
    await client.start()

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 7860))
    app.run(host="0.0.0.0", port=port)
