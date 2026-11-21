import os
import re
import time
import imaplib
import email
import email.utils
import telebot
from decimal import Decimal
from email.header import decode_header, make_header
from telebot.types import InlineKeyboardMarkup, InlineKeyboardButton
from telebot.apihelper import ApiTelegramException
from pymongo import MongoClient
from datetime import datetime, timedelta
from apscheduler.schedulers.background import BackgroundScheduler
from concurrent.futures import ThreadPoolExecutor, as_completed
from flask import Flask
from threading import Thread, Lock

# --- RENDER KEEP-ALIVE SERVER ---
app = Flask('')
@app.route('/')
def home(): return "Bot is running and healthy!"

def run_web():
    port = int(os.environ.get("PORT", 5000))
    app.run(host='0.0.0.0', port=port)

def keep_alive():
    Thread(target=run_web).start()

# --- CONFIGURATION ---
BOT_TOKEN = os.getenv('BOT_TOKEN')
MONGO_URI = os.getenv('MONGO_URI')
ADMIN_ID = int(os.getenv('ADMIN_ID'))
UPI_ID = os.getenv('UPI_ID')
CONTACT_USERNAME = os.getenv('CONTACT_USERNAME')
GMAIL_ADDRESS = os.getenv('GMAIL_ADDRESS')
GMAIL_APP_PASSWORD = os.getenv('GMAIL_APP_PASSWORD')

COINS_PER_REFERRAL = 1
DEFAULT_MINUTES_PER_COIN = 2

# Broadcast speed settings
BROADCAST_WORKERS = 20
BROADCAST_MAX_PER_SECOND = 25

# Unsend settings (Telegram only lets bots delete messages for about 48 hours)
UNSEND_WINDOW_HOURS = 47

# Unique-amount settings: the buyer pays price + 0.01 ... price + 0.99
AMOUNT_SLOTS = 99
UNPAID_HOLD_MINUTES = 360
PROOF_HOLD_MINUTES = 1440

# Renewal / reminder settings
BONUS_WINDOW_HOURS = 24            # bonus time if they buy again within this many hours after expiry
DEFAULT_BONUS_PERCENT = 10         # change anytime with /setbonus
EXPIRY_REMINDER_MINUTES = 60       # reminder this long before expiry
ABANDON_REMINDER_MINUTES = 30      # reminder if the QR was shown but no payment proof was sent

bot = telebot.TeleBot(BOT_TOKEN)
client = MongoClient(MONGO_URI)
db = client['sub_management']
channels_col = db['channels']
users_col = db['users']
coins_col = db['coins']
all_users_col = db['all_users']
payments_col = db['payments']
bot_state_col = db['bot_state']
processed_emails_col = db['processed_emails']
broadcasts_col = db['broadcasts']
broadcast_msgs_col = db['broadcast_msgs']
expired_col = db['expired_subs']
coupons_col = db['coupons']

try:
    broadcasts_col.create_index("created_at", expireAfterSeconds=60 * 60 * 72)
    broadcast_msgs_col.create_index("created_at", expireAfterSeconds=60 * 60 * 72)
    broadcast_msgs_col.create_index("bid")
    expired_col.create_index("expired_at", expireAfterSeconds=60 * 60 * 72)
    coupons_col.create_index("code", unique=True)
except Exception as e:
    print(f"[Startup] Could not create indexes: {e}")

# Key = amount in PAISE (e.g. ₹100.01 -> 10001)
pending_payments = {}
_reserve_lock = Lock()
_last_offset = {}
user_coupons = {}   # user_id -> coupon code the user has applied

def is_admin(m):
    return m.from_user.id == ADMIN_ID

def _text_or_none(message):
    t = getattr(message, 'text', None)
    return t.strip() if t else None

# --- DISPLAY HELPERS ---

def duration_label(mins):
    """1440 -> '1 Day', 10080 -> '7 Days', 90 -> '1 Hour 30 Minutes'."""
    mins = int(mins)
    days, rem = divmod(mins, 1440)
    hours, minutes = divmod(rem, 60)
    parts = []
    if days:
        parts.append(f"{days} Day" + ("s" if days != 1 else ""))
    if hours:
        parts.append(f"{hours} Hour" + ("s" if hours != 1 else ""))
    if minutes:
        parts.append(f"{minutes} Minute" + ("s" if minutes != 1 else ""))
    return " ".join(parts) if parts else f"{mins} Minutes"

def fmt_amount(paise):
    """10001 -> '100.01'. Small numbers are old whole-rupee amounts from before the paise update."""
    paise = int(paise)
    if paise < 100:
        return str(paise)
    return f"{paise // 100}.{paise % 100:02d}"

def to_paise(amount):
    return int((Decimal(amount) * 100).to_integral_value())

def ist_str(dt):
    """Server time is UTC; show users Indian time."""
    return (dt + timedelta(hours=5, minutes=30)).strftime("%d %b %Y, %I:%M %p") + " IST"

def parse_duration(text):
    """'1d' -> 1440, '12h' -> 720, '90' -> 90 (minutes). Returns None if not understood."""
    t = (text or "").strip().lower().replace(" ", "")
    m = re.match(r'^(\d+)(m|min|mins|minute|minutes|h|hr|hrs|hour|hours|d|day|days)?$', t)
    if not m:
        return None
    n = int(m.group(1))
    unit = m.group(2) or "m"
    if unit.startswith("d"):
        n *= 1440
    elif unit.startswith("h"):
        n *= 60
    return n if n >= 1 else None

def price_from_amount(amount_key, fallback=0):
    """₹29.01 (2901 paise) -> 29. Old whole-rupee keys fall back to the given price."""
    try:
        if amount_key and int(amount_key) >= 100:
            return int(amount_key) // 100
    except Exception:
        pass
    try:
        return int(fallback)
    except Exception:
        return 0

def channel_name(ch_id):
    ch = channels_col.find_one({"channel_id": ch_id})
    return ch['name'] if ch else "the channel"

# --- PROCESSED EMAIL HELPERS (PERSISTENT IN MONGODB) ---

def is_email_processed(eid_str):
    return processed_emails_col.find_one({"_id": eid_str}) is not None

def mark_email_processed(eid_str):
    processed_emails_col.update_one(
        {"_id": eid_str},
        {"$set": {"processed_at": datetime.now()}},
        upsert=True
    )

def get_email_datetime(msg):
    """Parse the email's Date header into a naive local datetime. Returns None if unavailable."""
    date_str = msg.get('Date')
    if not date_str:
        return None
    try:
        dt = email.utils.parsedate_to_datetime(date_str)
        if dt.tzinfo is not None:
            dt = dt.astimezone().replace(tzinfo=None)
        return dt
    except Exception:
        return None

def decode_subject(msg):
    raw = msg.get('subject', '') or ''
    try:
        return str(make_header(decode_header(raw)))
    except Exception:
        return raw

# --- BOT STATE HELPERS ---

def get_minutes_per_coin():
    state = bot_state_col.find_one({"_id": "coin_value"})
    if state:
        return state.get("minutes_per_coin", DEFAULT_MINUTES_PER_COIN)
    return DEFAULT_MINUTES_PER_COIN

def set_minutes_per_coin(value):
    bot_state_col.update_one(
        {"_id": "coin_value"},
        {"$set": {"minutes_per_coin": value}},
        upsert=True
    )

def is_bot_paused():
    state = bot_state_col.find_one({"_id": "state"})
    return state and state.get("paused", False)

def get_pause_message():
    state = bot_state_col.find_one({"_id": "state"})
    return state.get("pause_message", "The bot is currently under maintenance. Please try again later.") if state else ""

def set_bot_paused(paused, message=""):
    bot_state_col.update_one(
        {"_id": "state"},
        {"$set": {"paused": paused, "pause_message": message}},
        upsert=True
    )

def get_bonus_percent():
    s = bot_state_col.find_one({"_id": "bonus"})
    if s and "percent" in s:
        return int(s["percent"])
    return DEFAULT_BONUS_PERCENT

# --- USER HELPERS ---

def track_user(user):
    today = datetime.now().strftime("%Y-%m-%d")
    all_users_col.update_one(
        {"user_id": user.id},
        {"$set": {
            "user_id": user.id,
            "name": user.first_name,
            "username": user.username or "",
        },
        "$setOnInsert": {"joined_date": today}},
        upsert=True
    )

def get_coin_data(user_id):
    data = coins_col.find_one({"user_id": user_id})
    if not data:
        coins_col.insert_one({"user_id": user_id, "coins": 0, "referred_users": []})
        data = coins_col.find_one({"user_id": user_id})
    return data

# --- COUPON HELPERS ---

def coupon_discount_text(c):
    return f"{c['value']}% off" if c['type'] == 'percent' else f"₹{c['value']} off"

def coupon_expiry_text(c):
    return ist_str(c['expires_at']) if c.get('expires_at') else "No expiry"

def coupon_problem(c, user_id):
    """Returns None if the coupon can be used by this user, otherwise the reason."""
    if not c:
        return "That coupon code doesn't exist."
    if c.get('expires_at') and datetime.now() > c['expires_at']:
        return "This coupon has expired."
    mu = c.get('max_uses', 0)
    if mu and c.get('used', 0) >= mu:
        return "This coupon has reached its usage limit."
    if user_id in c.get('used_by', []):
        return "You have already used this coupon."
    return None

def get_user_coupon(user_id):
    code = user_coupons.get(user_id)
    if not code:
        return None
    c = coupons_col.find_one({"code": code})
    if coupon_problem(c, user_id):
        user_coupons.pop(user_id, None)
        return None
    return c

def apply_coupon(price, c):
    price = int(price)
    if c['type'] == 'percent':
        new = int(price * (100 - c['value']) / 100 + 0.5)
    else:
        new = price - int(c['value'])
    return max(1, min(price, new))

# --- UNIQUE AMOUNT HANDLING (PAISE) ---

def owns_pending(amount_key, user_id):
    """True only if this amount is currently reserved by this same user."""
    entry = pending_payments.get(amount_key)
    return bool(entry and entry.get('user_id') == user_id)

def reserve_amount(price, user_id, user_name, ch_id, mins, coupon=None, orig_price=None):
    """Reserves price + 0.01 ... price + 0.99 for this buyer and returns it in paise."""
    base_price = int(price)
    base_paise = base_price * 100
    now = datetime.now()
    with _reserve_lock:
        for amt, d in list(pending_payments.items()):
            if (d.get('user_id') == user_id and d.get('ch_id') == int(ch_id)
                    and str(d.get('mins')) == str(mins) and int(d.get('price', -1)) == base_price):
                d['last_touch'] = now
                return amt

        chosen = None
        start = _last_offset.get(base_paise, 0)
        for i in range(1, AMOUNT_SLOTS + 1):
            n = ((start + i - 1) % AMOUNT_SLOTS) + 1
            key = base_paise + n
            if key not in pending_payments:
                chosen = key
                _last_offset[base_paise] = n
                break

        if chosen is None:
            unpaid = [(d.get('last_touch', d['created_at']), amt)
                      for amt, d in list(pending_payments.items())
                      if base_paise < amt <= base_paise + AMOUNT_SLOTS and not d.get('photo_file_id')]
            if unpaid:
                chosen = min(unpaid)[1]
                pending_payments.pop(chosen, None)

        if chosen is None:
            return None

        pending_payments[chosen] = {
            "user_id": user_id,
            "user_name": user_name,
            "ch_id": int(ch_id),
            "mins": mins,
            "price": base_price,
            "orig_price": int(orig_price) if orig_price is not None else base_price,
            "coupon": coupon,
            "reminded": False,
            "created_at": now,
            "last_touch": now,
            "timestamp": now,
            "photo_file_id": None,
            "admin_msg_id": None
        }
        return chosen

def cleanup_expired_payments():
    """Frees amounts that were reserved long ago and never completed."""
    now = datetime.now()
    for amount_key, data in list(pending_payments.items()):
        try:
            if data.get('photo_file_id'):
                age = (now - data['timestamp']).total_seconds()
                limit = PROOF_HOLD_MINUTES * 60
            else:
                age = (now - data.get('last_touch', data['created_at'])).total_seconds()
                limit = UNPAID_HOLD_MINUTES * 60
            if age > limit:
                pending_payments.pop(amount_key, None)
                print(f"[Cleanup] Released unused amount ₹{fmt_amount(amount_key)}")
        except Exception as e:
            print(f"[Cleanup] Error: {e}")

# --- FAST BROADCAST ENGINE ---

_rate_lock = Lock()
_last_send_time = [0.0]

def _throttle():
    """Spaces out sends so we never exceed BROADCAST_MAX_PER_SECOND."""
    interval = 1.0 / BROADCAST_MAX_PER_SECOND
    with _rate_lock:
        now = time.time()
        next_allowed = _last_send_time[0] + interval
        if next_allowed > now:
            time.sleep(next_allowed - now)
            now = next_allowed
        _last_send_time[0] = now

def _send_one(user_id, text, markup):
    """Sends one message. Returns (status, message_id).
    status is 'ok', 'dead' (blocked/deleted) or 'failed' (temporary)."""
    for attempt in range(3):
        try:
            _throttle()
            sent = bot.send_message(user_id, text, reply_markup=markup, parse_mode="Markdown")
            return "ok", sent.message_id
        except ApiTelegramException as e:
            code = e.error_code
            desc = str(e.description).lower()
            if code == 429:
                wait = 5
                try:
                    wait = int(e.result_json.get('parameters', {}).get('retry_after', 5))
                except Exception:
                    pass
                time.sleep(wait + 1)
                continue
            if code == 403:
                return "dead", None
            if code == 400 and "chat not found" in desc:
                return "dead", None
            if code == 400 and "parse" in desc:
                try:
                    _throttle()
                    sent = bot.send_message(user_id, text, reply_markup=markup)
                    return "ok", sent.message_id
                except ApiTelegramException as e2:
                    if e2.error_code == 403:
                        return "dead", None
                    return "failed", None
                except Exception:
                    return "failed", None
            return "failed", None
        except Exception:
            time.sleep(1)
            continue
    return "failed", None

def broadcast_to_all(message_text, extra_markup=None, status_msg_id=None, bid=None):
    """Sends to all users in parallel. Removes blocked/deleted users from the list.
    If bid is given, remembers every sent message so it can be unsent later.
    Returns (delivered, failed, removed)."""
    user_ids = [u['user_id'] for u in all_users_col.find({}, {"user_id": 1})]
    total = len(user_ids)
    delivered = 0
    failed = 0
    dead_ids = []
    sent_docs = []
    done = 0
    last_update = time.time()
    now = datetime.now()

    if total == 0:
        return 0, 0, 0

    with ThreadPoolExecutor(max_workers=BROADCAST_WORKERS) as executor:
        futures = {executor.submit(_send_one, uid, message_text, extra_markup): uid for uid in user_ids}
        for future in as_completed(futures):
            uid = futures[future]
            try:
                result, mid = future.result()
            except Exception:
                result, mid = "failed", None
            if result == "ok":
                delivered += 1
                if bid and mid:
                    sent_docs.append({"bid": bid, "chat_id": uid, "message_id": mid, "created_at": now})
            elif result == "dead":
                dead_ids.append(uid)
            else:
                failed += 1
            done += 1
            if status_msg_id and (time.time() - last_update) >= 4:
                last_update = time.time()
                try:
                    bot.edit_message_text(f"⏳ Sending... {done}/{total}", ADMIN_ID, status_msg_id)
                except Exception:
                    pass

    if sent_docs:
        try:
            for i in range(0, len(sent_docs), 1000):
                broadcast_msgs_col.insert_many(sent_docs[i:i + 1000])
        except Exception as e:
            print(f"[Broadcast] Could not save sent messages: {e}")

    removed = 0
    if dead_ids:
        try:
            result = all_users_col.delete_many({"user_id": {"$in": dead_ids}})
            removed = result.deleted_count
        except Exception as e:
            print(f"[Broadcast] Could not remove dead users: {e}")
    return delivered, failed, removed

def run_broadcast_job(text, markup, status_msg_id, title, footer="", label="Broadcast", preview=None):
    """Runs a broadcast in the background so the bot stays responsive."""
    def job():
        try:
            bid = str(int(time.time() * 1000))
            shown = (preview if preview is not None else text).replace("\n", " ").strip()
            broadcasts_col.insert_one({
                "_id": bid,
                "label": label,
                "preview": shown[:60],
                "created_at": datetime.now(),
                "delivered": 0,
                "deleted": False
            })
            delivered, failed, removed = broadcast_to_all(text, markup, status_msg_id, bid)
            broadcasts_col.update_one({"_id": bid}, {"$set": {"delivered": delivered}})
            report = (f"{title}\n\n"
                      f"📨 Delivered: *{delivered}*\n"
                      f"🗑 Removed (blocked/deleted): *{removed}*\n"
                      f"⚠️ Failed (temporary): *{failed}*")
            if footer:
                report += f"\n\n{footer}"
            if delivered:
                report += f"\n\n↩️ Sent by mistake? Use /unsend within {UNSEND_WINDOW_HOURS} hours."
            bot.send_message(ADMIN_ID, report, parse_mode="Markdown")
        except Exception as e:
            print(f"[Broadcast] Job error: {e}")
            try:
                bot.send_message(ADMIN_ID, f"❌ Broadcast error: {e}")
            except Exception:
                pass
    Thread(target=job, daemon=True).start()

# --- UNSEND BROADCAST ---

def _delete_one(chat_id, message_id):
    """Deletes one message. Returns 'deleted', 'gone' (already deleted / user left) or 'failed'."""
    for attempt in range(3):
        try:
            _throttle()
            bot.delete_message(chat_id, message_id)
            return "deleted"
        except ApiTelegramException as e:
            code = e.error_code
            desc = str(e.description).lower()
            if code == 429:
                wait = 5
                try:
                    wait = int(e.result_json.get('parameters', {}).get('retry_after', 5))
                except Exception:
                    pass
                time.sleep(wait + 1)
                continue
            if code == 403:
                return "gone"
            if code == 400 and ("not found" in desc or "can't be deleted" in desc or "message to delete" in desc):
                return "gone"
            return "failed"
        except Exception:
            time.sleep(1)
            continue
    return "failed"

def run_unsend_job(bid, status_msg_id):
    def job():
        try:
            docs = list(broadcast_msgs_col.find({"bid": bid}, {"chat_id": 1, "message_id": 1}))
            total = len(docs)
            deleted = 0
            gone = 0
            failed = 0
            done = 0
            last_update = time.time()
            if total == 0:
                bot.send_message(ADMIN_ID, "⚠️ No saved messages were found for that broadcast.")
                return
            with ThreadPoolExecutor(max_workers=BROADCAST_WORKERS) as executor:
                futures = [executor.submit(_delete_one, d['chat_id'], d['message_id']) for d in docs]
                for future in as_completed(futures):
                    try:
                        result = future.result()
                    except Exception:
                        result = "failed"
                    if result == "deleted":
                        deleted += 1
                    elif result == "gone":
                        gone += 1
                    else:
                        failed += 1
                    done += 1
                    if status_msg_id and (time.time() - last_update) >= 4:
                        last_update = time.time()
                        try:
                            bot.edit_message_text(f"⏳ Deleting... {done}/{total}", ADMIN_ID, status_msg_id)
                        except Exception:
                            pass
            broadcasts_col.update_one({"_id": bid}, {"$set": {"deleted": True}})
            broadcast_msgs_col.delete_many({"bid": bid})
            bot.send_message(ADMIN_ID,
                f"✅ *Unsend Complete!*\n\n"
                f"🗑 Deleted: *{deleted}*\n"
                f"👻 Already gone / user left: *{gone}*\n"
                f"⚠️ Failed: *{failed}*",
                parse_mode="Markdown")
        except Exception as e:
            print(f"[Unsend] Job error: {e}")
            try:
                bot.send_message(ADMIN_ID, f"❌ Unsend error: {e}")
            except Exception:
                pass
    Thread(target=job, daemon=True).start()

@bot.message_handler(commands=['unsend'], func=is_admin)
def unsend_start(message):
    cutoff = datetime.now() - timedelta(hours=UNSEND_WINDOW_HOURS)
    items = list(broadcasts_col.find(
        {"created_at": {"$gte": cutoff}, "deleted": {"$ne": True}, "delivered": {"$gt": 0}}
    ).sort("created_at", -1).limit(8))
    if not items:
        bot.send_message(ADMIN_ID,
            f"❌ No broadcasts found from the last {UNSEND_WINDOW_HOURS} hours that can be unsent.\n\n"
            f"_Broadcasts sent before this feature was added can't be unsent._",
            parse_mode="Markdown")
        return
    markup = InlineKeyboardMarkup()
    for b in items:
        label = f"{ist_str(b['created_at'])[:-4]} • {b.get('label', 'Broadcast')} • {b['preview'][:22]}"
        markup.add(InlineKeyboardButton(label[:60], callback_data=f"unsend_{b['_id']}"))
    bot.send_message(ADMIN_ID,
        "↩️ *Unsend a Broadcast*\n\nWhich message do you want to delete from users' chats?\n"
        f"_(Only the last {UNSEND_WINDOW_HOURS} hours can be unsent.)_",
        reply_markup=markup, parse_mode="Markdown")

@bot.callback_query_handler(func=lambda call: call.data.startswith('unsend_'))
def unsend_confirm(call):
    bid = call.data.split('_', 1)[1]
    bot.answer_callback_query(call.id)
    b = broadcasts_col.find_one({"_id": bid})
    if not b or b.get("deleted"):
        bot.send_message(ADMIN_ID, "❌ That broadcast is no longer available.")
        return
    markup = InlineKeyboardMarkup()
    markup.add(InlineKeyboardButton("✅ Yes, Delete It", callback_data=f"unsendyes_{bid}"))
    markup.add(InlineKeyboardButton("❌ Cancel", callback_data="unsendno"))
    bot.send_message(ADMIN_ID,
        f"⚠️ Delete this message from all users' chats?\n\n"
        f"📝 {b['preview']}\n"
        f"📨 Delivered to: {b.get('delivered', 0)} users\n\n"
        f"This cannot be undone.",
        reply_markup=markup)

@bot.callback_query_handler(func=lambda call: call.data.startswith('unsendyes_'))
def unsend_final(call):
    bid = call.data.split('_', 1)[1]
    bot.answer_callback_query(call.id)
    b = broadcasts_col.find_one({"_id": bid})
    if not b or b.get("deleted"):
        bot.send_message(ADMIN_ID, "❌ That broadcast is no longer available.")
        return
    try:
        bot.edit_message_text("⏳ Deleting the broadcast from all chats...",
                              call.message.chat.id, call.message.message_id)
    except Exception:
        pass
    run_unsend_job(bid, call.message.message_id)

@bot.callback_query_handler(func=lambda call: call.data == "unsendno")
def unsend_cancel(call):
    bot.answer_callback_query(call.id)
    try:
        bot.edit_message_text("❌ Cancelled. Nothing was deleted.",
                              call.message.chat.id, call.message.message_id)
    except Exception:
        pass

# --- CHANNEL LIST ---

def show_channel_list(chat_id):
    cursor = channels_col.find({"admin_id": ADMIN_ID})
    channels = list(cursor)
    if not channels:
        bot.send_message(chat_id, "❌ No channels are available right now. Please contact the admin.")
        return
    markup = InlineKeyboardMarkup()
    for ch in channels:
        markup.add(InlineKeyboardButton(f"📢 {ch['name']}", callback_data=f"viewchannel_{ch['channel_id']}"))
    markup.add(InlineKeyboardButton("📋 My Subscriptions", callback_data="mysubs"))
    bot.send_message(chat_id,
        "👋 *Welcome!*\n\nPlease select a channel below to view its plans:",
        reply_markup=markup, parse_mode="Markdown")

@bot.callback_query_handler(func=lambda call: call.data.startswith('viewchannel_'))
def view_channel(call):
    ch_id = int(call.data.split('_')[1])
    ch_data = channels_col.find_one({"channel_id": ch_id})
    bot.answer_callback_query(call.id)
    if not ch_data:
        bot.send_message(call.message.chat.id, "❌ Channel not found.")
        return
    show_plan_menu(call.message.chat.id, ch_id, ch_data)

@bot.callback_query_handler(func=lambda call: call.data == "channellist_back")
def channel_list_back(call):
    bot.answer_callback_query(call.id)
    show_channel_list(call.message.chat.id)

def show_plan_menu(chat_id, ch_id, ch_data):
    coupon = get_user_coupon(chat_id)
    markup = InlineKeyboardMarkup()
    for p_time, p_price in ch_data['plans'].items():
        label = duration_label(p_time)
        if coupon:
            btn = f"💳 {label} - ₹{p_price} ➜ ₹{apply_coupon(p_price, coupon)}"
        else:
            btn = f"💳 {label} - ₹{p_price}"
        markup.add(InlineKeyboardButton(btn, callback_data=f"select_{ch_id}_{p_time}"))
    if coupon:
        markup.add(InlineKeyboardButton(f"❌ Remove coupon {coupon['code']}", callback_data=f"couponremove_{ch_id}"))
    else:
        markup.add(InlineKeyboardButton("🎟 Have a Coupon Code?", callback_data=f"coupon_{ch_id}"))
    markup.add(InlineKeyboardButton("🎁 FREE SUBSCRIPTION", callback_data=f"referral_{ch_id}"))
    markup.add(InlineKeyboardButton("💰 Show My Coin Balance", callback_data=f"coinbalance_{ch_id}"))
    markup.add(InlineKeyboardButton("🎟 Redeem My Coins", callback_data=f"redeem_{ch_id}"))
    markup.add(InlineKeyboardButton("🔙 All Channels", callback_data="channellist_back"))
    markup.add(InlineKeyboardButton("📞 Contact Admin", url=f"https://t.me/{CONTACT_USERNAME}"))
    desc = ch_data.get("description", "")
    welcome_text = f"Welcome!\n\nYou are joining: *{ch_data['name']}*"
    if desc:
        welcome_text += f"\n\n📋 *About this channel:*\n{desc}"
    if coupon:
        welcome_text += f"\n\n🎟 Coupon applied: {coupon['code']} ({coupon_discount_text(coupon)})"
    welcome_text += "\n\nPlease select a subscription plan below:"
    bot.send_message(chat_id, welcome_text, reply_markup=markup, parse_mode="Markdown")

# --- MY SUBSCRIPTIONS ---

def show_my_subs(chat_id, user_id):
    now = datetime.now()
    subs = list(users_col.find({"user_id": user_id, "expiry": {"$gt": now.timestamp()}}))
    if not subs:
        bot.send_message(chat_id,
            "📋 You don't have any active subscriptions right now.\n\nUse /start to see the channels.")
        return
    lines = []
    markup = InlineKeyboardMarkup()
    for s in subs:
        exp = datetime.fromtimestamp(s['expiry'])
        left = max(1, int((exp - now).total_seconds() // 60))
        name = channel_name(s['channel_id'])
        lines.append(f"• {name}\n   ⏳ {duration_label(left)} left (until {ist_str(exp)})")
        markup.add(InlineKeyboardButton(f"🔄 Renew {name}"[:60], callback_data=f"viewchannel_{s['channel_id']}"))
    bot.send_message(chat_id,
        "📋 Your Subscriptions\n\n" + "\n\n".join(lines) +
        "\n\nRenewing adds time on top of what you have left.",
        reply_markup=markup)

@bot.message_handler(commands=['mysubs'])
def mysubs_cmd(message):
    show_my_subs(message.chat.id, message.from_user.id)

@bot.callback_query_handler(func=lambda call: call.data == "mysubs")
def mysubs_cb(call):
    bot.answer_callback_query(call.id)
    show_my_subs(call.message.chat.id, call.from_user.id)

# --- GMAIL IMAP ---

def extract_amount_from_text(text):
    """Returns the amount found in the text as a Decimal (keeps the paise), or None."""
    patterns = [
        r'(?:rs\.?|inr|₹)\s*(\d[\d,]*(?:\.\d{1,2})?)',
        r'(\d[\d,]*(?:\.\d{1,2})?)\s*(?:rs\.?|inr|₹)',
        r'amount.*?(\d[\d,]*(?:\.\d{1,2})?)',
        r'paid.*?(\d[\d,]*(?:\.\d{1,2})?)',
        r'debited.*?(\d[\d,]*(?:\.\d{1,2})?)',
        r'received.*?(\d[\d,]*(?:\.\d{1,2})?)',
        r'credited.*?(\d[\d,]*(?:\.\d{1,2})?)',
    ]
    text_lower = text.lower()
    for pattern in patterns:
        match = re.search(pattern, text_lower)
        if match:
            try:
                return Decimal(match.group(1).replace(',', ''))
            except Exception:
                continue
    return None

def check_gmail_payments():
    if not pending_payments:
        return
    print(f"[Gmail Check] Started. Pending payments: {[fmt_amount(k) for k in pending_payments.keys()]}")
    try:
        mail = imaplib.IMAP4_SSL('imap.gmail.com')
        mail.login(GMAIL_ADDRESS, GMAIL_APP_PASSWORD)
        mail.select('inbox')
        email_ids = []
        for term in ['payment', 'debited', 'credited', 'UPI', 'received', 'paid']:
            _, data = mail.search(None, f'(UNSEEN SUBJECT "{term}")')
            email_ids += data[0].split()
        email_ids = list(set(email_ids))
        print(f"[Gmail Check] Found {len(email_ids)} unseen matching emails")
        for eid in email_ids:
            eid_str = eid.decode() if isinstance(eid, bytes) else eid
            if is_email_processed(eid_str):
                continue
            _, msg_data = mail.fetch(eid, '(BODY.PEEK[])')
            msg = email.message_from_bytes(msg_data[0][1])
            subject = decode_subject(msg)
            body = ""
            if msg.is_multipart():
                for part in msg.walk():
                    if part.get_content_type() == 'text/plain':
                        try:
                            body = part.get_payload(decode=True).decode('utf-8', errors='ignore')
                            break
                        except Exception:
                            pass
            else:
                try:
                    body = msg.get_payload(decode=True).decode('utf-8', errors='ignore')
                except Exception:
                    pass
            amount = extract_amount_from_text(subject + " " + body)
            print(f"[Gmail Check] Email subject: '{subject}' -> extracted amount: {amount}")
            if amount is None:
                print(f"[Gmail Check] Could not extract amount, skipping this email")
                continue
            amount_key = to_paise(amount)
            entry_peek = pending_payments.get(amount_key)
            if entry_peek:
                # SECURITY CHECK: the email must have arrived AFTER the user picked the plan.
                email_dt = get_email_datetime(msg)
                pending_created = entry_peek.get('created_at', entry_peek['timestamp'])
                if email_dt is not None and email_dt < (pending_created - timedelta(minutes=5)):
                    print(f"[Gmail Check] Email for ₹{fmt_amount(amount_key)} is OLDER than the pending payment "
                          f"(email: {email_dt}, pending created: {pending_created}). Skipping.")
                    continue
                entry = pending_payments.pop(amount_key, None)
                if entry is None:
                    continue
                print(f"[Gmail Check] MATCH FOUND for ₹{fmt_amount(amount_key)}, auto-approving")
                mark_email_processed(eid_str)
                try:
                    mail.store(eid, '+FLAGS', '\\Seen')
                except Exception as e:
                    print(f"[Gmail Check] Could not mark as seen: {e}")
                auto_approve(
                    entry['user_id'], entry['ch_id'], int(entry['mins']),
                    entry.get('photo_file_id'), entry.get('user_name', 'Unknown'), entry['price'],
                    entry.get('admin_msg_id'), amount_key, entry.get('coupon')
                )
            else:
                print(f"[Gmail Check] Amount ₹{fmt_amount(amount_key)} extracted but no matching pending payment")
        mail.logout()
        print(f"[Gmail Check] Finished successfully")
    except Exception as e:
        print(f"Gmail IMAP error: {e}")

# --- GIVING ACCESS (RENEWALS ADD TIME, EARLY-RETURN BONUS) ---

def record_payment(u_id, user_name, ch_id, mins, price, amount_paise=None, coupon=None):
    today = datetime.now().strftime("%Y-%m-%d")
    doc = {
        "user_id": u_id,
        "user_name": user_name,
        "channel_id": ch_id,
        "mins": mins,
        "price": int(price),
        "date": today,
        "timestamp": datetime.now()
    }
    if amount_paise is not None:
        doc["amount_paise"] = int(amount_paise)
    if coupon:
        doc["coupon"] = coupon
    payments_col.insert_one(doc)

def grant_access(u_id, ch_id, mins, allow_bonus=True):
    """Gives or extends access.
    - Active subscription: the new time is ADDED to what is left.
    - Expired within BONUS_WINDOW_HOURS: bonus time is added (if allowed).
    Raises if the invite link can't be created (nothing is changed in that case)."""
    mins = int(mins)
    now = datetime.now()
    existing = users_col.find_one({"user_id": u_id, "channel_id": ch_id})
    start = now
    extended = False
    bonus_mins = 0
    if existing and existing.get('expiry', 0) > now.timestamp():
        start = datetime.fromtimestamp(existing['expiry'])
        extended = True
    elif allow_bonus:
        pct = get_bonus_percent()
        if pct > 0:
            rec = expired_col.find_one({"user_id": u_id, "channel_id": ch_id})
            if rec and (now - rec['expired_at']) <= timedelta(hours=BONUS_WINDOW_HOURS):
                bonus_mins = max(1, int(mins * pct / 100))
    total = mins + bonus_mins
    new_expiry = start + timedelta(minutes=total)
    link = bot.create_chat_invite_link(ch_id, member_limit=1, expire_date=int(new_expiry.timestamp()))
    remaining_min = (new_expiry - now).total_seconds() / 60
    users_col.update_one(
        {"user_id": u_id, "channel_id": ch_id},
        {"$set": {
            "expiry": new_expiry.timestamp(),
            "reminded": remaining_min <= EXPIRY_REMINDER_MINUTES,
            "kick_attempts": 0
        }},
        upsert=True
    )
    expired_col.delete_many({"user_id": u_id, "channel_id": ch_id})
    return {
        "link": link.invite_link,
        "new_expiry": new_expiry,
        "bonus_mins": bonus_mins,
        "extended": extended,
        "total_mins": total
    }

def access_message(title, mins, res, prefix=""):
    until = ist_str(res['new_expiry'])
    if res['extended']:
        return (f"🔄 Subscription Extended!\n\n"
                f"{prefix}"
                f"Added: {duration_label(mins)}\n"
                f"New expiry: {until}\n\n"
                f"You're already in the channel. If you ever left, use this link to re-join:\n{res['link']}")
    bonus_line = f"🎁 Bonus: +{duration_label(res['bonus_mins'])} free for coming back early!\n" if res['bonus_mins'] else ""
    return (f"{title}\n\n"
            f"{prefix}"
            f"Subscription: {duration_label(mins)}\n"
            f"{bonus_line}"
            f"Valid until: {until}\n\n"
            f"Join Link: {res['link']}\n\n"
            f"⚠️ This link stops working when your subscription ends.")

def purchase_notes(res, coupon):
    notes = ""
    if coupon:
        notes += f"Coupon: {coupon}\n"
    if res['extended']:
        notes += "🔄 Extended an active subscription\n"
    if res['bonus_mins']:
        notes += f"🎁 Early-return bonus: +{duration_label(res['bonus_mins'])}\n"
    return notes

def complete_purchase(u_id, ch_id, mins, user_name, price, amount_paise, coupon, title):
    """Shared by auto and manual approval: gives access, records the sale, counts the coupon, messages the buyer."""
    res = grant_access(u_id, ch_id, mins, allow_bonus=True)
    record_payment(u_id, user_name, ch_id, mins, price, amount_paise, coupon)
    if coupon:
        try:
            coupons_col.update_one({"code": coupon}, {"$inc": {"used": 1}, "$addToSet": {"used_by": u_id}})
        except Exception as e:
            print(f"[Coupon] Could not count usage: {e}")
        if user_coupons.get(u_id) == coupon:
            user_coupons.pop(u_id, None)
    bot.send_message(u_id, access_message(title, mins, res))
    return res

def auto_approve(u_id, ch_id, mins, photo_file_id, user_name, price, admin_msg_id=None, amount_paise=None, coupon=None):
    try:
        res = complete_purchase(u_id, ch_id, int(mins), user_name, price, amount_paise, coupon,
                                "🥳 Payment Verified & Approved!")
        paid_text = f"₹{price}" if amount_paise is None else f"₹{price} (paid ₹{fmt_amount(amount_paise)})"
        caption = (f"✅ Auto Verified by Bot\n\n"
                   f"User: {user_name} ({u_id})\n"
                   f"Plan: {duration_label(mins)}\n"
                   f"Amount: {paid_text}\n"
                   f"{purchase_notes(res, coupon)}\n"
                   f"No action needed — invite link already sent.")
        updated = False
        if admin_msg_id:
            try:
                bot.edit_message_caption(caption, chat_id=ADMIN_ID, message_id=admin_msg_id)
                updated = True
            except Exception as e:
                print(f"[Auto Approve] Could not edit admin message: {e}")
        if not updated:
            if photo_file_id:
                bot.send_photo(ADMIN_ID, photo_file_id, caption=caption)
            else:
                bot.send_message(ADMIN_ID, caption)
    except Exception as e:
        bot.send_message(ADMIN_ID, f"❌ Auto approval error for user {u_id}: {e}\n\nUse the Approve button on their proof to approve manually.")

# --- EMAIL STATS (READ ONLY) ---

@bot.message_handler(commands=['emailstats'], func=is_admin)
def email_stats(message):
    bot.send_message(ADMIN_ID, "🔍 Checking inbox... please wait.")
    try:
        mail = imaplib.IMAP4_SSL('imap.gmail.com')
        mail.login(GMAIL_ADDRESS, GMAIL_APP_PASSWORD)
        mail.select('inbox')

        _, total_data = mail.search(None, 'ALL')
        total_count = len(total_data[0].split())

        _, unseen_data = mail.search(None, 'UNSEEN')
        unseen_count = len(unseen_data[0].split())

        term_counts = {}
        matching_ids = set()
        for term in ['payment', 'debited', 'credited', 'UPI', 'received', 'paid']:
            _, data = mail.search(None, f'(UNSEEN SUBJECT "{term}")')
            ids = data[0].split()
            term_counts[term] = len(ids)
            matching_ids.update(ids)

        matched_unread_count = len(matching_ids)

        pending_processed_count = 0
        for eid in matching_ids:
            eid_str = eid.decode() if isinstance(eid, bytes) else eid
            if not is_email_processed(eid_str):
                pending_processed_count += 1

        mail.logout()

        total_processed_ever = processed_emails_col.count_documents({})

        term_lines = "\n".join([f"→ '{term}': {count}" for term, count in term_counts.items()])

        bot.send_message(ADMIN_ID,
            f"📧 *Email Inbox Stats*\n\n"
            f"━━━━━━━━━━━━━━━\n"
            f"📥 *Inbox Overview*\n"
            f"→ Total Emails: *{total_count}*\n"
            f"→ Unread Emails: *{unseen_count}*\n\n"
            f"━━━━━━━━━━━━━━━\n"
            f"🔎 *Unread Matching Payment Keywords*\n"
            f"{term_lines}\n"
            f"→ Unique Total: *{matched_unread_count}*\n\n"
            f"━━━━━━━━━━━━━━━\n"
            f"⏳ *Not Yet Used by Bot*\n"
            f"→ Awaiting a match: *{pending_processed_count}*\n\n"
            f"━━━━━━━━━━━━━━━\n"
            f"✅ *Bot History*\n"
            f"→ Total Emails Ever Used to Approve: *{total_processed_ever}*\n\n"
            f"_This command is read-only — nothing was changed or marked as read._",
            parse_mode="Markdown")
    except Exception as e:
        bot.send_message(ADMIN_ID, f"❌ Error checking email stats: {e}")

# --- START HANDLER ---

@bot.message_handler(commands=['start'])
def start_handler(message):
    user_id = message.from_user.id
    text = message.text.split()
    track_user(message.from_user)

    if is_bot_paused() and user_id != ADMIN_ID:
        bot.send_message(message.chat.id,
            f"🔴 *Bot is currently paused*\n\n"
            f"{get_pause_message()}\n\n"
            f"Please check back later. We apologize for the inconvenience.",
            parse_mode="Markdown")
        return

    if len(text) > 1:
        param = text[1]
        if param.startswith("ref_"):
            referrer_id = int(param.split("_")[1])
            if referrer_id != user_id:
                referrer_data = coins_col.find_one({"user_id": referrer_id})
                already_referred = referrer_data and user_id in referrer_data.get("referred_users", [])
                if not already_referred:
                    coins_col.update_one(
                        {"user_id": referrer_id},
                        {"$inc": {"coins": COINS_PER_REFERRAL}, "$push": {"referred_users": user_id}},
                        upsert=True
                    )
                    try:
                        bot.send_message(referrer_id,
                            f"🎉 Someone joined using your referral link!\n\n"
                            f"🪙 You earned 1 coin!\n"
                            f"Check your balance: tap 💰 Show My Coin Balance")
                    except Exception:
                        pass
            show_channel_list(message.chat.id)
            return
        else:
            try:
                ch_id = int(param)
                ch_data = channels_col.find_one({"channel_id": ch_id})
                if ch_data:
                    show_plan_menu(message.chat.id, ch_id, ch_data)
                    return
            except Exception:
                pass

    if user_id == ADMIN_ID:
        bot.send_message(message.chat.id,
            "✅ *Admin Panel Active!*\n\n"
            "/add — Add Channel & Prices\n"
            "/editprice — Edit Prices / Add / Remove Plans\n"
            "/delete — Delete a Channel\n"
            "/channels — Manage Existing Channels\n"
            "/setdesc — Set Channel Description\n"
            "/addcoupon — Create a Discount Coupon\n"
            "/coupons — View / Delete Coupons\n"
            "/setbonus — Early-Return Bonus Time\n"
            "/stats — View Full Bot Statistics\n"
            "/setcoin — Change Coin Value\n"
            "/broadcast — Broadcast Message to All Users\n"
            "/unsend — Delete a Recent Broadcast from Users' Chats\n"
            "/pause — Pause Bot & Notify Users\n"
            "/resume — Resume Bot & Notify Users\n"
            "/emailstats — View Gmail Inbox Stats (read-only)",
            parse_mode="Markdown")
    else:
        show_channel_list(message.chat.id)

# --- STATS ---

@bot.message_handler(commands=['stats'], func=is_admin)
def show_stats(message):
    try:
        today = datetime.now().strftime("%Y-%m-%d")
        now_ts = datetime.now().timestamp()
        total_users = all_users_col.count_documents({})
        new_today = all_users_col.count_documents({"joined_date": today})
        active_subs = users_col.count_documents({"expiry": {"$gt": now_ts}})
        total_subs_ever = payments_col.count_documents({})
        today_revenue_pipeline = [
            {"$match": {"date": today}},
            {"$group": {"_id": None, "total": {"$sum": "$price"}}}
        ]
        today_rev_result = list(payments_col.aggregate(today_revenue_pipeline))
        today_revenue = today_rev_result[0]['total'] if today_rev_result else 0
        total_revenue_pipeline = [{"$group": {"_id": None, "total": {"$sum": "$price"}}}]
        total_rev_result = list(payments_col.aggregate(total_revenue_pipeline))
        total_revenue = total_rev_result[0]['total'] if total_rev_result else 0
        this_month = datetime.now().strftime("%Y-%m")
        month_revenue_pipeline = [
            {"$match": {"date": {"$regex": f"^{this_month}"}}},
            {"$group": {"_id": None, "total": {"$sum": "$price"}}}
        ]
        month_rev_result = list(payments_col.aggregate(month_revenue_pipeline))
        month_revenue = month_rev_result[0]['total'] if month_rev_result else 0
        payments_today = payments_col.count_documents({"date": today})
        total_referrals = 0
        for doc in coins_col.find({}):
            total_referrals += len(doc.get("referred_users", []))
        coins_pipeline = [{"$group": {"_id": None, "total": {"$sum": "$coins"}}}]
        coins_result = list(coins_col.aggregate(coins_pipeline))
        total_coins = coins_result[0]['total'] if coins_result else 0
        total_channels = channels_col.count_documents({"admin_id": ADMIN_ID})
        state = "🟢 Running" if not is_bot_paused() else "🔴 Paused"
        mpc = get_minutes_per_coin()
        bot.send_message(ADMIN_ID,
            f"📊 *Full Bot Statistics*\n\n"
            f"━━━━━━━━━━━━━━━\n"
            f"👥 *Users*\n"
            f"→ Total Users: *{total_users}*\n"
            f"→ New Users Today: *{new_today}*\n\n"
            f"━━━━━━━━━━━━━━━\n"
            f"💰 *Revenue*\n"
            f"→ Today: *₹{today_revenue}*\n"
            f"→ This Month: *₹{month_revenue}*\n"
            f"→ All Time: *₹{total_revenue}*\n\n"
            f"━━━━━━━━━━━━━━━\n"
            f"💳 *Subscriptions*\n"
            f"→ Active Right Now: *{active_subs}*\n"
            f"→ Payments Today: *{payments_today}*\n"
            f"→ Total Payments Ever: *{total_subs_ever}*\n\n"
            f"━━━━━━━━━━━━━━━\n"
            f"🎁 *Referral System*\n"
            f"→ Total Referrals: *{total_referrals}*\n"
            f"→ Coins In Circulation: *{total_coins}*\n"
            f"→ Coin Value: *1 coin = {mpc} mins*\n\n"
            f"━━━━━━━━━━━━━━━\n"
            f"📢 *Channels*\n"
            f"→ Total Channels: *{total_channels}*\n\n"
            f"━━━━━━━━━━━━━━━\n"
            f"🤖 *Bot Status:* {state}",
            parse_mode="Markdown")
    except Exception as e:
        bot.send_message(ADMIN_ID, f"❌ Error fetching stats: {e}")

# --- SET COIN VALUE ---

@bot.message_handler(commands=['setcoin'], func=is_admin)
def set_coin_start(message):
    mpc = get_minutes_per_coin()
    msg = bot.send_message(ADMIN_ID,
        f"🪙 *Change Coin Value*\n\n"
        f"Current value: *1 coin = {mpc} minutes*\n\n"
        f"Send the new number of minutes per coin\n"
        f"(e.g. send *10* to make 1 coin = 10 minutes):",
        parse_mode="Markdown")
    bot.register_next_step_handler(msg, save_coin_value)

def save_coin_value(message):
    text = (message.text or "").strip()
    if not text.isdigit() or int(text) < 1:
        msg = bot.send_message(ADMIN_ID, "❌ Please send a valid number (e.g. 10).")
        bot.register_next_step_handler(msg, save_coin_value)
        return
    new_value = int(text)
    set_minutes_per_coin(new_value)
    bot.send_message(ADMIN_ID,
        f"✅ *Coin Value Updated!*\n\n"
        f"🪙 New value: *1 coin = {new_value} minutes*\n\n"
        f"This applies to all future redemptions immediately!",
        parse_mode="Markdown")

# --- EARLY-RETURN BONUS SETTING ---

@bot.message_handler(commands=['setbonus'], func=is_admin)
def setbonus_start(message):
    pct = get_bonus_percent()
    msg = bot.send_message(ADMIN_ID,
        f"🎁 Early-Return Bonus\n\n"
        f"Current: {pct}% extra time for people who buy again within {BONUS_WINDOW_HOURS} hours after their subscription expired.\n\n"
        f"Send a new percentage (0 turns it off, maximum 100):")
    bot.register_next_step_handler(msg, save_bonus)

def save_bonus(message):
    text = _text_or_none(message)
    if text and text.startswith('/'):
        bot.send_message(ADMIN_ID, "❌ Cancelled.")
        return
    if not text or not text.isdigit() or int(text) > 100:
        msg = bot.send_message(ADMIN_ID, "❌ Please send a number from 0 to 100.")
        bot.register_next_step_handler(msg, save_bonus)
        return
    bot_state_col.update_one({"_id": "bonus"}, {"$set": {"percent": int(text)}}, upsert=True)
    if int(text) == 0:
        bot.send_message(ADMIN_ID, "✅ Early-return bonus is now OFF.")
    else:
        bot.send_message(ADMIN_ID,
            f"✅ Early-return bonus set to {int(text)}%.\n\n"
            f"Example: someone buying 1 Day within {BONUS_WINDOW_HOURS} hours after expiry gets "
            f"{duration_label(max(1, int(1440 * int(text) / 100)))} extra free.")

# --- BROADCAST ---

@bot.message_handler(commands=['broadcast'], func=is_admin)
def broadcast_start(message):
    msg = bot.send_message(ADMIN_ID,
        "📢 *Broadcast Message*\n\n"
        "Send the message you want to broadcast to all users.\n\n"
        "You can use:\n"
        "→ *bold* for bold text\n"
        "→ _italic_ for italic\n"
        "→ Emojis for engagement 🔥\n\n"
        "Type your message now:",
        parse_mode="Markdown")
    bot.register_next_step_handler(msg, confirm_broadcast)

def confirm_broadcast(message):
    broadcast_text = message.text.strip()
    markup = InlineKeyboardMarkup()
    markup.add(InlineKeyboardButton("✅ Yes Send It", callback_data="confirmbc_yes"))
    markup.add(InlineKeyboardButton("❌ Cancel", callback_data="confirmbc_no"))
    bot_state_col.update_one(
        {"_id": "pending_broadcast"},
        {"$set": {"message": broadcast_text}},
        upsert=True
    )
    bot.send_message(ADMIN_ID,
        f"📢 *Preview of your broadcast:*\n\n"
        f"{broadcast_text}\n\n"
        f"━━━━━━━━━━━━━━━\n"
        f"Send this to all users?",
        reply_markup=markup, parse_mode="Markdown")

@bot.callback_query_handler(func=lambda call: call.data.startswith('confirmbc_'))
def handle_broadcast_confirm(call):
    action = call.data.split('_')[1]
    bot.answer_callback_query(call.id)
    if action == "no":
        bot.edit_message_text("❌ Broadcast cancelled.", call.message.chat.id, call.message.message_id)
        return
    pending = bot_state_col.find_one({"_id": "pending_broadcast"})
    if not pending:
        bot.send_message(ADMIN_ID, "❌ Broadcast message not found. Please try again.")
        return
    broadcast_text = pending['message']
    bot_state_col.delete_one({"_id": "pending_broadcast"})
    markup = InlineKeyboardMarkup()
    markup.add(InlineKeyboardButton("🚀 View Channels", callback_data="channellist_back"))
    full_message = f"📢 *Message from Admin*\n\n{broadcast_text}"
    bot.edit_message_text("⏳ Broadcast started... sending to all users.",
                          call.message.chat.id, call.message.message_id)
    run_broadcast_job(full_message, markup, call.message.message_id,
                      "✅ *Broadcast Complete!*",
                      label="Broadcast", preview=broadcast_text)

# --- SET DESCRIPTION ---

@bot.message_handler(commands=['setdesc'], func=is_admin)
def set_desc_start(message):
    cursor = channels_col.find({"admin_id": ADMIN_ID})
    channels = list(cursor)
    if not channels:
        bot.send_message(ADMIN_ID, "❌ No channels found. Use /add to add a channel first.")
        return
    markup = InlineKeyboardMarkup()
    for ch in channels:
        markup.add(InlineKeyboardButton(f"📢 {ch['name']}", callback_data=f"descselect_{ch['channel_id']}"))
    bot.send_message(ADMIN_ID, "Which channel do you want to set a description for?", reply_markup=markup)

@bot.callback_query_handler(func=lambda call: call.data.startswith('descselect_'))
def desc_channel_selected(call):
    ch_id = int(call.data.split('_')[1])
    ch_data = channels_col.find_one({"channel_id": ch_id})
    bot.answer_callback_query(call.id)
    msg = bot.send_message(ADMIN_ID,
        f"📝 Setting description for: *{ch_data['name']}*\n\n"
        f"Send the description text now.",
        parse_mode="Markdown")
    bot.register_next_step_handler(msg, save_description, ch_id)

def save_description(message, ch_id):
    desc = message.text.strip()
    channels_col.update_one({"channel_id": ch_id}, {"$set": {"description": desc}})
    ch_data = channels_col.find_one({"channel_id": ch_id})
    bot.send_message(ADMIN_ID,
        f"✅ Description saved for *{ch_data['name']}*!\n\n"
        f"📋 Preview:\n{desc}",
        parse_mode="Markdown")

# --- DELETE CHANNEL ---

@bot.message_handler(commands=['delete'], func=is_admin)
def delete_channel_start(message):
    cursor = channels_col.find({"admin_id": ADMIN_ID})
    channels = list(cursor)
    if not channels:
        bot.send_message(ADMIN_ID, "❌ No channels found to delete.")
        return
    markup = InlineKeyboardMarkup()
    for ch in channels:
        markup.add(InlineKeyboardButton(f"🗑 {ch['name']}", callback_data=f"delselect_{ch['channel_id']}"))
    bot.send_message(ADMIN_ID, "Which channel do you want to delete?", reply_markup=markup)

@bot.callback_query_handler(func=lambda call: call.data.startswith('delselect_'))
def delete_channel_confirm(call):
    ch_id = int(call.data.split('_')[1])
    ch_data = channels_col.find_one({"channel_id": ch_id})
    bot.answer_callback_query(call.id)
    if not ch_data:
        bot.send_message(ADMIN_ID, "❌ Channel not found.")
        return
    markup = InlineKeyboardMarkup()
    markup.add(InlineKeyboardButton("✅ Yes, Delete It", callback_data=f"delconfirm_{ch_id}"))
    markup.add(InlineKeyboardButton("❌ Cancel", callback_data="delcancel"))
    bot.send_message(ADMIN_ID,
        f"⚠️ *Are you sure you want to delete this channel?*\n\n"
        f"📢 Channel: *{ch_data['name']}*\n\n"
        f"This will remove it from the bot's channel list. New users won't be able to see or subscribe to it anymore.\n\n"
        f"_Note: Existing active subscribers will keep their access until it naturally expires — they will not be kicked out._\n\n"
        f"This cannot be undone.",
        reply_markup=markup, parse_mode="Markdown")

@bot.callback_query_handler(func=lambda call: call.data.startswith('delconfirm_'))
def delete_channel_final(call):
    ch_id = int(call.data.split('_')[1])
    ch_data = channels_col.find_one({"channel_id": ch_id})
    bot.answer_callback_query(call.id)
    if not ch_data:
        bot.send_message(ADMIN_ID, "❌ Channel not found.")
        return
    ch_name = ch_data['name']
    channels_col.delete_one({"channel_id": ch_id})
    bot.edit_message_text(
        f"✅ *Channel Deleted*\n\n"
        f"📢 *{ch_name}* has been removed from the bot.\n\n"
        f"It no longer appears in the channel list for new users.",
        call.message.chat.id, call.message.message_id, parse_mode="Markdown")

@bot.callback_query_handler(func=lambda call: call.data == "delcancel")
def delete_channel_cancel(call):
    bot.answer_callback_query(call.id)
    bot.edit_message_text("❌ Deletion cancelled. Channel was not removed.",
                          call.message.chat.id, call.message.message_id)

# --- PAUSE ---

@bot.message_handler(commands=['pause'], func=is_admin)
def pause_bot(message):
    if is_bot_paused():
        bot.send_message(ADMIN_ID, "⚠️ Bot is already paused. Use /resume to resume it.")
        return
    msg = bot.send_message(ADMIN_ID,
        "⏸ *Pause Bot*\n\n"
        "Please send the message you want to show to users while the bot is paused.",
        parse_mode="Markdown")
    bot.register_next_step_handler(msg, confirm_pause)

def confirm_pause(message):
    pause_msg = message.text.strip()
    set_bot_paused(True, pause_msg)
    broadcast_text = (f"🔴 *Bot Paused*\n\n"
                      f"{pause_msg}\n\n"
                      f"Please check back later.")
    status = bot.send_message(ADMIN_ID, "⏳ Broadcasting pause message to all users...")
    run_broadcast_job(broadcast_text, None, status.message_id,
                      "✅ *Bot Paused Successfully!*",
                      "Use /resume when ready.",
                      label="Pause notice", preview=pause_msg)

# --- RESUME ---

@bot.message_handler(commands=['resume'], func=is_admin)
def resume_bot(message):
    if not is_bot_paused():
        bot.send_message(ADMIN_ID, "⚠️ Bot is already running. Use /pause to pause it.")
        return
    msg = bot.send_message(ADMIN_ID,
        "▶️ *Resume Bot*\n\n"
        "Please send the message you want to broadcast to all users when resuming.",
        parse_mode="Markdown")
    bot.register_next_step_handler(msg, confirm_resume)

def confirm_resume(message):
    resume_msg = message.text.strip()
    set_bot_paused(False, "")
    markup = InlineKeyboardMarkup()
    markup.add(InlineKeyboardButton("🚀 View Channels", callback_data="channellist_back"))
    broadcast_text = (f"🟢 *Bot is Back Online!*\n\n"
                      f"{resume_msg}\n\n"
                      f"Tap the button below to join! 👇")
    status = bot.send_message(ADMIN_ID, "⏳ Broadcasting resume message to all users...")
    run_broadcast_job(broadcast_text, markup, status.message_id,
                      "✅ *Bot Resumed Successfully!*",
                      "Bot is now fully operational! ✅",
                      label="Resume notice", preview=resume_msg)

# --- ADMIN CHANNEL MANAGEMENT ---

@bot.message_handler(commands=['channels'], func=is_admin)
def list_channels(message):
    markup = InlineKeyboardMarkup()
    cursor = channels_col.find({"admin_id": ADMIN_ID})
    count = 0
    for ch in cursor:
        markup.add(InlineKeyboardButton(f"Channel: {ch['name']}", callback_data=f"manage_{ch['channel_id']}"))
        count += 1
    markup.add(InlineKeyboardButton("➕ Add New Channel", callback_data="add_new"))
    if count == 0:
        bot.send_message(ADMIN_ID, "No channels found. Click below to add one.", reply_markup=markup)
    else:
        bot.send_message(ADMIN_ID, "Your Managed Channels:", reply_markup=markup)

@bot.message_handler(commands=['add'], func=is_admin)
def add_channel_start(message):
    msg = bot.send_message(ADMIN_ID, "Please ensure the bot is an Admin in your channel, then FORWARD any message from that channel here.")
    bot.register_next_step_handler(msg, get_plans)

@bot.callback_query_handler(func=lambda call: call.data == "add_new")
def cb_add_new(call):
    bot.answer_callback_query(call.id)
    msg = bot.send_message(ADMIN_ID, "Please FORWARD any message from your channel here.")
    bot.register_next_step_handler(msg, get_plans)

def get_plans(message):
    if message.forward_from_chat:
        ch_id = message.forward_from_chat.id
        ch_name = message.forward_from_chat.title
        msg = bot.send_message(ADMIN_ID,
            f"Channel Detected: *{ch_name}* 🔥\n\nHow many plans do you want to add? (e.g. 2)",
            parse_mode="Markdown")
        bot.register_next_step_handler(msg, ask_plan_count, ch_id, ch_name)
    else:
        bot.send_message(ADMIN_ID, "❌ Error: Message was not forwarded. Use /add to try again.")

def ask_plan_count(message, ch_id, ch_name):
    text = message.text.strip()
    if not text.isdigit() or int(text) < 1:
        msg = bot.send_message(ADMIN_ID, "❌ Please send a valid number (e.g. 2).")
        bot.register_next_step_handler(msg, ask_plan_count, ch_id, ch_name)
        return
    plan_state = {"ch_id": ch_id, "ch_name": ch_name, "total": int(text), "current": 1, "plans": {}}
    ask_minutes(message, plan_state)

def ask_minutes(message, plan_state):
    msg = bot.send_message(ADMIN_ID,
        f"Plan {plan_state['current']}/{plan_state['total']}\n\nEnter duration in MINUTES (e.g. 1440 for 1 Day, 43200 for 30 Days):")
    bot.register_next_step_handler(msg, save_minutes, plan_state)

def save_minutes(message, plan_state):
    text = message.text.strip()
    if not text.isdigit() or int(text) < 1:
        msg = bot.send_message(ADMIN_ID, "❌ Please send a valid number of minutes.")
        bot.register_next_step_handler(msg, save_minutes, plan_state)
        return
    plan_state["temp_minutes"] = text
    msg = bot.send_message(ADMIN_ID, f"Got it: {text} minutes.\n\nNow enter the PRICE (numbers only, e.g. 99):")
    bot.register_next_step_handler(msg, save_price, plan_state)

def save_price(message, plan_state):
    text = message.text.strip()
    if not text.isdigit() or int(text) < 1:
        msg = bot.send_message(ADMIN_ID, "❌ Please send a valid price.")
        bot.register_next_step_handler(msg, save_price, plan_state)
        return
    minutes = plan_state.pop("temp_minutes")
    plan_state["plans"][minutes] = text
    if plan_state["current"] < plan_state["total"]:
        plan_state["current"] += 1
        ask_minutes(message, plan_state)
    else:
        finalize_channel(plan_state)

def finalize_channel(plan_state):
    try:
        ch_id = plan_state["ch_id"]
        ch_name = plan_state["ch_name"]
        plans_dict = plan_state["plans"]
        channels_col.update_one(
            {"channel_id": ch_id},
            {"$set": {"name": ch_name, "plans": plans_dict, "admin_id": ADMIN_ID}},
            upsert=True
        )
        summary_lines = []
        for mins, price in plans_dict.items():
            summary_lines.append(f"• {duration_label(mins)} — ₹{price}")
        summary = "\n".join(summary_lines)
        bot_username = bot.get_me().username
        bot.send_message(ADMIN_ID,
            f"✅ Setup Successful!\n\nPlans for *{ch_name}*:\n{summary}\n\n"
            f"Users can now access this channel via your main bot link:\n`https://t.me/{bot_username}`\n\n"
            f"Or this direct link:\n`https://t.me/{bot_username}?start={ch_id}`",
            parse_mode="Markdown")
    except Exception as e:
        bot.send_message(ADMIN_ID, f"❌ Something went wrong while saving: {e}")

# --- EDIT PRICES (NO FORWARDING NEEDED) ---

def show_plan_editor(ch_id):
    ch = channels_col.find_one({"channel_id": ch_id})
    if not ch:
        bot.send_message(ADMIN_ID, "❌ Channel not found.")
        return
    markup = InlineKeyboardMarkup()
    for mins, price in ch.get('plans', {}).items():
        markup.add(InlineKeyboardButton(f"{duration_label(mins)} — ₹{price}", callback_data=f"epplan_{ch_id}_{mins}"))
    markup.add(InlineKeyboardButton("➕ Add Plan", callback_data=f"epadd_{ch_id}"))
    markup.add(InlineKeyboardButton("✅ Done", callback_data="epdone"))
    bot.send_message(ADMIN_ID,
        f"✏️ Plans for {ch['name']}\n\nTap a plan to change its price or remove it.",
        reply_markup=markup)

@bot.message_handler(commands=['editprice'], func=is_admin)
def editprice_start(message):
    channels = list(channels_col.find({"admin_id": ADMIN_ID}))
    if not channels:
        bot.send_message(ADMIN_ID, "❌ No channels found. Use /add first.")
        return
    markup = InlineKeyboardMarkup()
    for ch in channels:
        markup.add(InlineKeyboardButton(f"📢 {ch['name']}", callback_data=f"epch_{ch['channel_id']}"))
    bot.send_message(ADMIN_ID, "Which channel's prices do you want to edit?", reply_markup=markup)

@bot.callback_query_handler(func=lambda call: call.data.startswith('epch_'))
def ep_channel(call):
    bot.answer_callback_query(call.id)
    if call.from_user.id != ADMIN_ID:
        return
    show_plan_editor(int(call.data.split('_')[1]))

@bot.callback_query_handler(func=lambda call: call.data.startswith('epplan_'))
def ep_plan(call):
    bot.answer_callback_query(call.id)
    if call.from_user.id != ADMIN_ID:
        return
    _, ch_id, mins = call.data.split('_')
    ch = channels_col.find_one({"channel_id": int(ch_id)})
    price = ch['plans'].get(mins) if ch else None
    if price is None:
        bot.send_message(ADMIN_ID, "❌ Plan not found.")
        return
    markup = InlineKeyboardMarkup()
    markup.add(InlineKeyboardButton("✏️ Change Price", callback_data=f"epprice_{ch_id}_{mins}"))
    markup.add(InlineKeyboardButton("🗑 Remove Plan", callback_data=f"epdel_{ch_id}_{mins}"))
    markup.add(InlineKeyboardButton("🔙 Back", callback_data=f"epch_{ch_id}"))
    bot.send_message(ADMIN_ID, f"Plan: {duration_label(mins)} — ₹{price}\n\nWhat do you want to do?", reply_markup=markup)

@bot.callback_query_handler(func=lambda call: call.data.startswith('epprice_'))
def ep_price_prompt(call):
    bot.answer_callback_query(call.id)
    if call.from_user.id != ADMIN_ID:
        return
    _, ch_id, mins = call.data.split('_')
    ch = channels_col.find_one({"channel_id": int(ch_id)})
    price = ch['plans'].get(mins) if ch else None
    if price is None:
        bot.send_message(ADMIN_ID, "❌ Plan not found.")
        return
    msg = bot.send_message(ADMIN_ID,
        f"Send the new price for {duration_label(mins)} (numbers only). Current price: ₹{price}\n\nSend /cancel to stop.")
    bot.register_next_step_handler(msg, save_edited_price, int(ch_id), mins)

def save_edited_price(message, ch_id, mins):
    text = _text_or_none(message)
    if text and text.startswith('/'):
        bot.send_message(ADMIN_ID, "❌ Cancelled.")
        return
    if not text or not text.isdigit() or int(text) < 1:
        msg = bot.send_message(ADMIN_ID, "❌ Please send a valid price (numbers only, e.g. 99).")
        bot.register_next_step_handler(msg, save_edited_price, ch_id, mins)
        return
    ch = channels_col.find_one({"channel_id": ch_id})
    old = ch['plans'].get(mins) if ch else None
    if old is None:
        bot.send_message(ADMIN_ID, "❌ Plan not found.")
        return
    channels_col.update_one({"channel_id": ch_id}, {"$set": {f"plans.{mins}": str(int(text))}})
    bot.send_message(ADMIN_ID,
        f"✅ Price updated: {duration_label(mins)}  ₹{old} ➜ ₹{int(text)}\n\nNew buyers see it right away.")
    show_plan_editor(ch_id)

@bot.callback_query_handler(func=lambda call: call.data.startswith('epdel_'))
def ep_delete_ask(call):
    bot.answer_callback_query(call.id)
    if call.from_user.id != ADMIN_ID:
        return
    _, ch_id, mins = call.data.split('_')
    markup = InlineKeyboardMarkup()
    markup.add(InlineKeyboardButton("✅ Yes, Remove", callback_data=f"epdelyes_{ch_id}_{mins}"))
    markup.add(InlineKeyboardButton("❌ No", callback_data=f"epch_{ch_id}"))
    bot.send_message(ADMIN_ID, f"Remove the {duration_label(mins)} plan?", reply_markup=markup)

@bot.callback_query_handler(func=lambda call: call.data.startswith('epdelyes_'))
def ep_delete_do(call):
    bot.answer_callback_query(call.id)
    if call.from_user.id != ADMIN_ID:
        return
    _, ch_id, mins = call.data.split('_')
    ch_id = int(ch_id)
    ch = channels_col.find_one({"channel_id": ch_id})
    if not ch or mins not in ch.get('plans', {}):
        bot.send_message(ADMIN_ID, "❌ Plan not found.")
        return
    if len(ch['plans']) <= 1:
        bot.send_message(ADMIN_ID, "❌ A channel needs at least one plan. Add another plan first, then remove this one.")
        show_plan_editor(ch_id)
        return
    channels_col.update_one({"channel_id": ch_id}, {"$unset": {f"plans.{mins}": ""}})
    bot.send_message(ADMIN_ID, f"✅ Removed the {duration_label(mins)} plan.")
    show_plan_editor(ch_id)

@bot.callback_query_handler(func=lambda call: call.data.startswith('epadd_'))
def ep_add_prompt(call):
    bot.answer_callback_query(call.id)
    if call.from_user.id != ADMIN_ID:
        return
    ch_id = int(call.data.split('_')[1])
    msg = bot.send_message(ADMIN_ID,
        "Send the plan duration. Examples: 1d, 7d, 30d, 12h — or minutes like 1440.\n\nSend /cancel to stop.")
    bot.register_next_step_handler(msg, add_plan_duration, ch_id)

def add_plan_duration(message, ch_id):
    text = _text_or_none(message)
    if text and text.startswith('/'):
        bot.send_message(ADMIN_ID, "❌ Cancelled.")
        return
    mins = parse_duration(text)
    if not mins:
        msg = bot.send_message(ADMIN_ID, "❌ I couldn't read that. Try something like 1d, 7d, 12h or 1440.")
        bot.register_next_step_handler(msg, add_plan_duration, ch_id)
        return
    ch = channels_col.find_one({"channel_id": ch_id})
    if not ch:
        bot.send_message(ADMIN_ID, "❌ Channel not found.")
        return
    if str(mins) in ch.get('plans', {}):
        bot.send_message(ADMIN_ID, f"⚠️ A {duration_label(mins)} plan already exists. Tap it in the list to change its price.")
        show_plan_editor(ch_id)
        return
    msg = bot.send_message(ADMIN_ID, f"Duration: {duration_label(mins)}\n\nNow send the price (numbers only):")
    bot.register_next_step_handler(msg, add_plan_price, ch_id, mins)

def add_plan_price(message, ch_id, mins):
    text = _text_or_none(message)
    if text and text.startswith('/'):
        bot.send_message(ADMIN_ID, "❌ Cancelled.")
        return
    if not text or not text.isdigit() or int(text) < 1:
        msg = bot.send_message(ADMIN_ID, "❌ Please send a valid price (numbers only, e.g. 99).")
        bot.register_next_step_handler(msg, add_plan_price, ch_id, mins)
        return
    channels_col.update_one({"channel_id": ch_id}, {"$set": {f"plans.{mins}": str(int(text))}})
    bot.send_message(ADMIN_ID, f"✅ Added: {duration_label(mins)} — ₹{int(text)}")
    show_plan_editor(ch_id)

@bot.callback_query_handler(func=lambda call: call.data == "epdone")
def ep_done(call):
    bot.answer_callback_query(call.id)
    try:
        bot.edit_message_text("✅ Done.", call.message.chat.id, call.message.message_id)
    except Exception:
        pass

# --- COUPONS (ADMIN) ---

@bot.message_handler(commands=['addcoupon'], func=is_admin)
def addcoupon_start(message):
    msg = bot.send_message(ADMIN_ID,
        "🎟 Create Coupon\n\nStep 1/4: Send the coupon code (3-20 letters/numbers), e.g. OFFER20\n\nSend /cancel to stop.")
    bot.register_next_step_handler(msg, coupon_step_code)

def coupon_step_code(message):
    text = _text_or_none(message)
    if text and text.startswith('/'):
        bot.send_message(ADMIN_ID, "❌ Cancelled.")
        return
    if not text or not re.match(r'^[A-Za-z0-9]{3,20}$', text):
        msg = bot.send_message(ADMIN_ID, "❌ Use 3-20 letters/numbers only (no spaces). Try again, or /cancel:")
        bot.register_next_step_handler(msg, coupon_step_code)
        return
    code = text.upper()
    if coupons_col.find_one({"code": code}):
        msg = bot.send_message(ADMIN_ID, "❌ That code already exists. Send a different code, or /cancel:")
        bot.register_next_step_handler(msg, coupon_step_code)
        return
    msg = bot.send_message(ADMIN_ID,
        "Step 2/4: Send the discount.\n\n• 20% → 20 percent off\n• 10 → flat ₹10 off")
    bot.register_next_step_handler(msg, coupon_step_discount, code)

def coupon_step_discount(message, code):
    text = _text_or_none(message)
    if text and text.startswith('/'):
        bot.send_message(ADMIN_ID, "❌ Cancelled.")
        return
    m = re.match(r'^(\d+)\s*(%)?$', text or "")
    if not m:
        msg = bot.send_message(ADMIN_ID, "❌ Send like 20% (percent) or 10 (flat ₹10). Try again, or /cancel:")
        bot.register_next_step_handler(msg, coupon_step_discount, code)
        return
    value = int(m.group(1))
    is_pct = bool(m.group(2))
    if value < 1 or (is_pct and value > 99):
        msg = bot.send_message(ADMIN_ID, "❌ Percent must be 1-99, and flat must be at least ₹1. Try again, or /cancel:")
        bot.register_next_step_handler(msg, coupon_step_discount, code)
        return
    ctype = 'percent' if is_pct else 'flat'
    msg = bot.send_message(ADMIN_ID,
        "Step 3/4: How long should the coupon be valid?\n\nExamples: 12h, 3d, 7d\nSend 0 for no expiry.")
    bot.register_next_step_handler(msg, coupon_step_expiry, code, ctype, value)

def coupon_step_expiry(message, code, ctype, value):
    text = _text_or_none(message)
    if text and text.startswith('/'):
        bot.send_message(ADMIN_ID, "❌ Cancelled.")
        return
    if text == "0":
        expires_at = None
    else:
        mins = parse_duration(text)
        if not mins or not any(ch.isalpha() for ch in (text or "")):
            msg = bot.send_message(ADMIN_ID, "❌ Use a unit, like 12h or 3d (or 0 for no expiry). Try again, or /cancel:")
            bot.register_next_step_handler(msg, coupon_step_expiry, code, ctype, value)
            return
        expires_at = datetime.now() + timedelta(minutes=mins)
    msg = bot.send_message(ADMIN_ID, "Step 4/4: Maximum total uses? Send a number, or 0 for unlimited.")
    bot.register_next_step_handler(msg, coupon_step_uses, code, ctype, value, expires_at)

def coupon_step_uses(message, code, ctype, value, expires_at):
    text = _text_or_none(message)
    if text and text.startswith('/'):
        bot.send_message(ADMIN_ID, "❌ Cancelled.")
        return
    if not text or not text.isdigit():
        msg = bot.send_message(ADMIN_ID, "❌ Send a number (0 for unlimited). Try again, or /cancel:")
        bot.register_next_step_handler(msg, coupon_step_uses, code, ctype, value, expires_at)
        return
    max_uses = int(text)
    try:
        coupons_col.insert_one({
            "code": code, "type": ctype, "value": value,
            "expires_at": expires_at, "max_uses": max_uses,
            "used": 0, "used_by": [], "created_at": datetime.now()
        })
    except Exception as e:
        bot.send_message(ADMIN_ID, f"❌ Could not save the coupon: {e}")
        return
    c = coupons_col.find_one({"code": code})
    bot.send_message(ADMIN_ID,
        f"✅ Coupon created!\n\n"
        f"Code: {code}\n"
        f"Discount: {coupon_discount_text(c)}\n"
        f"Expires: {coupon_expiry_text(c)}\n"
        f"Max uses: {max_uses if max_uses else 'Unlimited'}\n"
        f"Each person can use it once.\n\n"
        f"Share the code — buyers tap '🎟 Have a Coupon Code?' on the plans screen.")

@bot.message_handler(commands=['coupons'], func=is_admin)
def list_coupons(message):
    items = list(coupons_col.find({}).sort("created_at", -1).limit(20))
    if not items:
        bot.send_message(ADMIN_ID, "No coupons yet. Use /addcoupon to create one.")
        return
    now = datetime.now()
    lines = []
    markup = InlineKeyboardMarkup()
    for c in items:
        expired = c.get('expires_at') and now > c['expires_at']
        uses = f"{c.get('used', 0)}/{c['max_uses']}" if c.get('max_uses') else f"{c.get('used', 0)}/unlimited"
        status = " (EXPIRED)" if expired else ""
        lines.append(f"• {c['code']} — {coupon_discount_text(c)}{status}\n   Expires: {coupon_expiry_text(c)} | Used: {uses}")
        markup.add(InlineKeyboardButton(f"🗑 Delete {c['code']}", callback_data=f"delcoupon_{c['code']}"))
    bot.send_message(ADMIN_ID, "🎟 Coupons\n\n" + "\n\n".join(lines), reply_markup=markup)

@bot.callback_query_handler(func=lambda call: call.data.startswith('delcoupon_'))
def delete_coupon(call):
    bot.answer_callback_query(call.id)
    if call.from_user.id != ADMIN_ID:
        return
    code = call.data.split('_', 1)[1]
    coupons_col.delete_one({"code": code})
    bot.send_message(ADMIN_ID, f"🗑 Coupon {code} deleted.")

# --- COUPONS (BUYERS) ---

@bot.callback_query_handler(func=lambda call: call.data.startswith('coupon_'))
def coupon_prompt(call):
    ch_id = int(call.data.split('_')[1])
    bot.answer_callback_query(call.id)
    msg = bot.send_message(call.message.chat.id, "🎟 Send your coupon code:")
    bot.register_next_step_handler(msg, process_coupon_entry, ch_id)

def process_coupon_entry(message, ch_id):
    user_id = message.from_user.id
    text = _text_or_none(message)
    if not text or text.startswith('/'):
        bot.send_message(message.chat.id, "Coupon entry cancelled.")
    else:
        code = text.upper()
        c = coupons_col.find_one({"code": code})
        problem = coupon_problem(c, user_id)
        if problem:
            bot.send_message(message.chat.id, f"❌ {problem}")
        else:
            user_coupons[user_id] = code
            bot.send_message(message.chat.id,
                f"✅ Coupon {code} applied — {coupon_discount_text(c)}! The prices below are updated. 🎉")
    ch_data = channels_col.find_one({"channel_id": ch_id})
    if ch_data:
        show_plan_menu(message.chat.id, ch_id, ch_data)

@bot.callback_query_handler(func=lambda call: call.data.startswith('couponremove_'))
def coupon_remove(call):
    ch_id = int(call.data.split('_')[1])
    bot.answer_callback_query(call.id, "Coupon removed.")
    user_coupons.pop(call.from_user.id, None)
    ch_data = channels_col.find_one({"channel_id": ch_id})
    if ch_data:
        show_plan_menu(call.message.chat.id, ch_id, ch_data)

# --- REFERRAL ---

@bot.callback_query_handler(func=lambda call: call.data.startswith('referral_'))
def show_referral(call):
    ch_id = call.data.split('_')[1]
    user = call.from_user
    bot_username = bot.get_me().username
    ref_link = f"https://t.me/{bot_username}?start=ref_{user.id}"
    mpc = get_minutes_per_coin()
    markup = InlineKeyboardMarkup()
    markup.add(InlineKeyboardButton("🔙 Back to Plans", callback_data=f"backplans_{ch_id}"))
    bot.answer_callback_query(call.id)
    bot.send_message(call.message.chat.id,
        f"🎁 FREE SUBSCRIPTION\n\n"
        f"Your personal referral link:\n{ref_link}\n\n"
        f"📌 How it works:\n"
        f"→ Share your link with friends\n"
        f"→ When a new friend opens the bot using your link you earn 1 coin\n"
        f"→ Each friend can only give you 1 coin\n"
        f"→ No purchase needed — anyone can earn!\n\n"
        f"💰 What is a coin worth?\n"
        f"→ 1 coin = {mpc} minutes free access\n"
        f"→ 5 coins = {5*mpc} minutes\n"
        f"→ 30 coins = {30*mpc} minutes\n"
        f"→ 720 coins = {720*mpc} minutes 🔥",
        reply_markup=markup)

# --- COIN BALANCE ---

@bot.callback_query_handler(func=lambda call: call.data.startswith('coinbalance_'))
def show_coin_balance(call):
    ch_id = call.data.split('_')[1]
    user = call.from_user
    data = get_coin_data(user.id)
    coins = data.get('coins', 0)
    mpc = get_minutes_per_coin()
    worth_minutes = coins * mpc
    markup = InlineKeyboardMarkup()
    markup.add(InlineKeyboardButton("🎟 Redeem My Coins", callback_data=f"redeem_{ch_id}"))
    markup.add(InlineKeyboardButton("🔙 Back to Plans", callback_data=f"backplans_{ch_id}"))
    bot.answer_callback_query(call.id)
    bot.send_message(call.message.chat.id,
        f"💰 Your Coin Balance\n\n"
        f"🪙 Coins: {coins}\n"
        f"⏱ Worth: {worth_minutes} minutes of free access\n\n"
        f"📌 How to earn coins:\n"
        f"→ Share your referral link with friends\n"
        f"→ Each new friend who opens the bot = 1 coin\n"
        f"→ Each friend can only give you 1 coin\n\n"
        f"💡 Coin value:\n"
        f"→ 1 coin = {mpc} minutes\n"
        f"→ 5 coins = {5*mpc} minutes\n"
        f"→ 30 coins = {30*mpc} minutes\n"
        f"→ 720 coins = {720*mpc} minutes 🔥",
        reply_markup=markup)

# --- REDEEM COINS ---

@bot.callback_query_handler(func=lambda call: call.data.startswith('redeem_'))
def redeem_coins_start(call):
    ch_id = call.data.split('_')[1]
    user = call.from_user
    data = get_coin_data(user.id)
    coins = data.get('coins', 0)
    mpc = get_minutes_per_coin()
    worth_minutes = coins * mpc
    bot.answer_callback_query(call.id)
    if coins == 0:
        markup = InlineKeyboardMarkup()
        markup.add(InlineKeyboardButton("🎁 FREE SUBSCRIPTION", callback_data=f"referral_{ch_id}"))
        markup.add(InlineKeyboardButton("🔙 Back to Plans", callback_data=f"backplans_{ch_id}"))
        bot.send_message(call.message.chat.id,
            "🪙 You have 0 coins\n\n"
            "You don't have any coins yet!\n\n"
            "📌 Share your referral link with friends to earn coins and get free access!",
            reply_markup=markup)
        return
    markup = InlineKeyboardMarkup()
    markup.add(InlineKeyboardButton("🔙 Cancel", callback_data=f"backplans_{ch_id}"))
    msg = bot.send_message(call.message.chat.id,
        f"🎟 Redeem Your Coins\n\n"
        f"You currently have {coins} coins\n"
        f"Worth {worth_minutes} minutes of free access\n\n"
        f"📌 How redemption works:\n"
        f"→ Enter how many coins to redeem\n"
        f"→ Each coin = {mpc} minutes access\n"
        f"→ You'll get instant access to the channel\n"
        f"→ When time is up you'll be automatically removed\n\n"
        f"How many coins do you want to redeem? (max {coins})",
        reply_markup=markup)
    bot.register_next_step_handler(msg, process_redeem, ch_id, coins)

def process_redeem(message, ch_id, max_coins):
    user = message.from_user
    text = (message.text or "").strip()
    mpc = get_minutes_per_coin()
    markup = InlineKeyboardMarkup()
    markup.add(InlineKeyboardButton("🔙 Back to Plans", callback_data=f"backplans_{ch_id}"))
    if not text.isdigit() or int(text) < 1:
        msg = bot.send_message(message.chat.id,
            f"❌ Please send a valid number between 1 and {max_coins}.",
            reply_markup=markup)
        bot.register_next_step_handler(msg, process_redeem, ch_id, max_coins)
        return
    coins_to_use = int(text)
    if coins_to_use > max_coins:
        msg = bot.send_message(message.chat.id,
            f"❌ You only have {max_coins} coins. Please enter a number between 1 and {max_coins}.",
            reply_markup=markup)
        bot.register_next_step_handler(msg, process_redeem, ch_id, max_coins)
        return
    minutes = coins_to_use * mpc
    ch_id_int = int(ch_id)
    try:
        res = grant_access(user.id, ch_id_int, minutes, allow_bonus=False)
        coins_col.update_one({"user_id": user.id}, {"$inc": {"coins": -coins_to_use}})
        bot.send_message(message.chat.id,
            access_message("🥳 Access Granted!", minutes, res, prefix=f"🪙 Coins used: {coins_to_use}\n"))
        bot.send_message(ADMIN_ID,
            f"🪙 *Coin Redemption*\n\n"
            f"User: {user.first_name} (`{user.id}`)\n"
            f"Coins used: {coins_to_use}\n"
            f"Access: {duration_label(minutes)}",
            parse_mode="Markdown")
    except Exception as e:
        bot.send_message(message.chat.id, "❌ Something went wrong. Please contact admin.")
        bot.send_message(ADMIN_ID, f"❌ Redeem error for user {user.id}: {e}")

# --- BACK TO PLANS ---

@bot.callback_query_handler(func=lambda call: call.data.startswith('backplans_'))
def back_to_plans(call):
    ch_id = int(call.data.split('_')[1])
    ch_data = channels_col.find_one({"channel_id": ch_id})
    bot.answer_callback_query(call.id)
    if not ch_data:
        bot.send_message(call.message.chat.id, "❌ Channel not found. Please use the link again.")
        return
    show_plan_menu(call.message.chat.id, ch_id, ch_data)

# --- PAYMENT FLOW ---

@bot.callback_query_handler(func=lambda call: call.data.startswith('select_'))
def user_pays(call):
    _, ch_id, mins = call.data.split('_')
    ch_data = channels_col.find_one({"channel_id": int(ch_id)})
    plan_price = ch_data['plans'].get(mins) if ch_data else None
    if plan_price is None:
        bot.answer_callback_query(call.id, "This plan is no longer available.", show_alert=True)
        if ch_data:
            show_plan_menu(call.message.chat.id, int(ch_id), ch_data)
        return
    user_id = call.from_user.id
    orig_price = int(plan_price)
    coupon = get_user_coupon(user_id)
    final_price = apply_coupon(orig_price, coupon) if coupon else orig_price
    code = coupon['code'] if coupon else None
    unique_amount = reserve_amount(final_price, user_id, call.from_user.first_name, ch_id, mins, code, orig_price)
    if unique_amount is None:
        bot.answer_callback_query(call.id)
        bot.send_message(call.message.chat.id,
            "⏳ Many payments are in progress right now. Please try again in a minute.")
        return
    bot.answer_callback_query(call.id)
    amt_text = fmt_amount(unique_amount)
    qr_url = f"https://api.qrserver.com/v1/create-qr-code/?size=300x300&data=upi://pay?pa={UPI_ID}%26am={amt_text}%26cu=INR"
    markup = InlineKeyboardMarkup()
    markup.add(InlineKeyboardButton("✅ I Have Paid", callback_data=f"paid_{ch_id}_{mins}_{unique_amount}"))
    markup.add(InlineKeyboardButton("🔙 Choose Another Plan", callback_data=f"cancelpay_{ch_id}_{unique_amount}"))
    markup.add(InlineKeyboardButton("📞 Contact Admin", url=f"https://t.me/{CONTACT_USERNAME}"))
    if coupon:
        price_line = f"Price: ₹{orig_price} ➜ ₹{final_price} (🎟 {code})"
    else:
        price_line = f"Price: ₹{orig_price}"
    bot.send_photo(call.message.chat.id, qr_url,
        caption=(f"Plan: {duration_label(mins)}\n"
                 f"{price_line}\n"
                 f"UPI ID: {UPI_ID}\n\n"
                 f"⚠️ Please pay exactly ₹{amt_text} — this unique amount helps us verify your payment automatically. "
                 f"[ INSTANT BOT VERIFICATION AND ENTRY ]\n\n"
                 f"After paying tap 'I Have Paid' below."),
        reply_markup=markup)

@bot.callback_query_handler(func=lambda call: call.data.startswith('cancelpay_'))
def cancel_pay(call):
    """User backed out of the payment screen without paying: free the reserved amount."""
    parts = call.data.split('_')
    ch_id = int(parts[1])
    unique_amount = int(parts[2])
    entry = pending_payments.get(unique_amount)
    if entry and entry.get('user_id') == call.from_user.id and not entry.get('photo_file_id'):
        pending_payments.pop(unique_amount, None)
    bot.answer_callback_query(call.id)
    ch_data = channels_col.find_one({"channel_id": ch_id})
    if not ch_data:
        bot.send_message(call.message.chat.id, "❌ Channel not found. Please use the link again.")
        return
    show_plan_menu(call.message.chat.id, ch_id, ch_data)

@bot.callback_query_handler(func=lambda call: call.data.startswith('paid_'))
def request_proof(call):
    parts = call.data.split('_')
    ch_id = parts[1]
    mins = parts[2]
    unique_amount = int(parts[3])
    user_id = call.from_user.id

    if not owns_pending(unique_amount, user_id):
        bot.answer_callback_query(call.id)
        recent = payments_col.find_one({
            "user_id": user_id,
            "amount_paise": unique_amount,
            "timestamp": {"$gte": datetime.now() - timedelta(hours=24)}
        })
        if recent:
            bot.send_message(call.message.chat.id, "✅ Your payment was already verified! Check above for your invite link.")
            return
        # Late payer: take the screenshot and pass it to the admin for manual approval
        msg = bot.send_message(call.message.chat.id,
            "📸 Please send a screenshot of your payment as proof.\n\n"
            "The admin will verify it shortly.")
        bot.register_next_step_handler(msg, receive_late_proof, int(ch_id), mins, unique_amount)
        return

    pending_payments[unique_amount]['last_touch'] = datetime.now()
    bot.answer_callback_query(call.id)
    markup = InlineKeyboardMarkup()
    markup.add(InlineKeyboardButton("🔙 Cancel / Choose Another Plan", callback_data=f"cancelproof_{ch_id}_{unique_amount}"))
    msg = bot.send_message(call.message.chat.id,
        "📸 Please send a screenshot of your payment as proof.\n\n"
        "The bot will also try to verify automatically via email.",
        reply_markup=markup)
    bot.register_next_step_handler(msg, receive_proof, unique_amount)

@bot.callback_query_handler(func=lambda call: call.data.startswith('cancelproof_'))
def cancel_proof(call):
    parts = call.data.split('_')
    ch_id = int(parts[1])
    unique_amount = int(parts[2])
    if owns_pending(unique_amount, call.from_user.id):
        pending_payments.pop(unique_amount, None)
    bot.answer_callback_query(call.id, "Cancelled.")
    ch_data = channels_col.find_one({"channel_id": ch_id})
    if not ch_data:
        bot.send_message(call.message.chat.id, "❌ Channel not found. Please use the link again.")
        return
    show_plan_menu(call.message.chat.id, ch_id, ch_data)

def send_proof_to_admin(unique_amount, data):
    """Instantly send the screenshot to the admin with Approve/Reject buttons.
    Returns the admin message id (so it can be updated later), or None."""
    markup = InlineKeyboardMarkup()
    markup.add(InlineKeyboardButton("✅ Approve", callback_data=f"app_{data['user_id']}_{data['ch_id']}_{data['mins']}_{unique_amount}"))
    markup.add(InlineKeyboardButton("❌ Reject", callback_data=f"rej_{data['user_id']}_{unique_amount}"))
    coupon_line = f"Coupon: {data['coupon']}\n" if data.get('coupon') else ""
    caption = (f"🔔 New Payment Proof (auto-check running...)\n\n"
               f"User: {data['user_name']} ({data['user_id']})\n"
               f"Plan: {duration_label(data['mins'])}\n"
               f"Amount to match: ₹{fmt_amount(unique_amount)} (base ₹{data['price']})\n"
               f"{coupon_line}\n"
               f"If Gmail confirms it, this message updates automatically. "
               f"Otherwise verify the screenshot and tap Approve or Reject.")
    try:
        sent = bot.send_photo(ADMIN_ID, data['photo_file_id'], caption=caption, reply_markup=markup)
        return sent.message_id
    except Exception as e:
        print(f"[Admin Notify] Failed to send proof to admin: {e}")
        return None

def receive_proof(message, unique_amount):
    user = message.from_user
    if unique_amount not in pending_payments:
        bot.send_message(message.chat.id, "⚠️ This session was cancelled or already verified. Please select a plan again.")
        return
    if not message.photo:
        markup = InlineKeyboardMarkup()
        ch_id = pending_payments[unique_amount]['ch_id']
        markup.add(InlineKeyboardButton("🔙 Cancel / Choose Another Plan", callback_data=f"cancelproof_{ch_id}_{unique_amount}"))
        msg = bot.send_message(message.chat.id,
            "❌ That doesn't look like a photo. Please send a screenshot image of your payment.",
            reply_markup=markup)
        bot.register_next_step_handler(msg, receive_proof, unique_amount)
        return
    photo_file_id = message.photo[-1].file_id
    pending_payments[unique_amount]['photo_file_id'] = photo_file_id
    pending_payments[unique_amount]['timestamp'] = datetime.now()
    bot.send_message(message.chat.id,
        "⏳ Got your screenshot!\n\n"
        "The bot is verifying your payment automatically, and the admin has also been notified.\n\n"
        "✅ If verified → invite link arrives within 1 minute.\n"
        "👨‍💼 If not → admin will approve manually.\n\n"
        "Either way you're covered ❤️")
    admin_msg_id = send_proof_to_admin(unique_amount, pending_payments[unique_amount])
    if unique_amount in pending_payments:
        pending_payments[unique_amount]['admin_msg_id'] = admin_msg_id

def receive_late_proof(message, ch_id, mins, unique_amount):
    """A buyer whose payment reservation was released still sends proof: pass it to the admin."""
    user = message.from_user
    if not message.photo:
        msg = bot.send_message(message.chat.id,
            "❌ That doesn't look like a photo. Please send a screenshot image of your payment.")
        bot.register_next_step_handler(msg, receive_late_proof, ch_id, mins, unique_amount)
        return
    markup = InlineKeyboardMarkup()
    markup.add(InlineKeyboardButton("✅ Approve", callback_data=f"app_{user.id}_{ch_id}_{mins}_{unique_amount}"))
    markup.add(InlineKeyboardButton("❌ Reject", callback_data=f"rej_{user.id}_{unique_amount}"))
    caption = (f"🔔 Payment Proof (manual check needed)\n\n"
               f"User: {user.first_name} ({user.id})\n"
               f"Plan: {duration_label(mins)}\n"
               f"Amount asked: ₹{fmt_amount(unique_amount)}\n\n"
               f"This one is not auto-checked. Verify the screenshot and tap Approve or Reject.")
    try:
        bot.send_photo(ADMIN_ID, message.photo[-1].file_id, caption=caption, reply_markup=markup)
        bot.send_message(message.chat.id,
            "⏳ Got your screenshot!\n\n"
            "The admin will verify it shortly and send your invite link.\n\n"
            "Either way you're covered ❤️")
    except Exception as e:
        print(f"[Late Proof] Failed to notify admin: {e}")
        bot.send_message(message.chat.id,
            "⚠️ Something went wrong. Please contact the admin with your screenshot.",
            reply_markup=InlineKeyboardMarkup().add(
                InlineKeyboardButton("📞 Contact Admin", url=f"https://t.me/{CONTACT_USERNAME}")))

# --- MANUAL APPROVAL ---

@bot.callback_query_handler(func=lambda call: call.data.startswith('app_'))
def approve_now(call):
    parts = call.data.split('_')
    u_id, ch_id, mins = int(parts[1]), int(parts[2]), int(parts[3])
    amount_key = int(parts[4]) if len(parts) > 4 else None
    owns = amount_key is not None and owns_pending(amount_key, u_id)

    # Guard: if this payment is no longer pending, check whether it was already approved
    if amount_key is not None and not owns:
        recent = payments_col.find_one({
            "user_id": u_id,
            "amount_paise": amount_key,
            "timestamp": {"$gte": datetime.now() - timedelta(hours=24)}
        })
        if recent:
            bot.answer_callback_query(call.id, "Already approved (auto-verified or approved earlier).", show_alert=True)
            try:
                bot.edit_message_caption("✅ Already approved earlier.", call.message.chat.id, call.message.message_id)
            except Exception:
                pass
            return

    bot.answer_callback_query(call.id)
    entry = pending_payments.pop(amount_key, None) if owns else None
    try:
        if entry:
            name = entry.get('user_name', 'Unknown')
            price = entry.get('price', 0)
            coupon = entry.get('coupon')
        else:
            ch_data = channels_col.find_one({"channel_id": ch_id})
            plan_price = ch_data['plans'].get(str(mins)) if ch_data else 0
            price = price_from_amount(amount_key, plan_price)
            user_doc = all_users_col.find_one({"user_id": u_id})
            name = user_doc.get('name', 'Unknown') if user_doc else 'Unknown'
            coupon = None
        res = complete_purchase(u_id, ch_id, mins, name, price, amount_key, coupon, "🥳 Payment Approved!")
        bot.edit_message_caption(
            f"✅ Manually approved by Admin\nUser: {u_id} | Plan: {duration_label(mins)}\n{purchase_notes(res, coupon)}",
            call.message.chat.id, call.message.message_id)
    except Exception as e:
        if entry:
            pending_payments[amount_key] = entry
        bot.send_message(ADMIN_ID, f"❌ Error: {e}")

@bot.callback_query_handler(func=lambda call: call.data.startswith('rej_'))
def reject_payment(call):
    parts = call.data.split('_')
    u_id = int(parts[1])
    amount_key = int(parts[2]) if len(parts) > 2 else None
    owns = amount_key is not None and owns_pending(amount_key, u_id)

    # Guard: don't reject a payment that was already approved
    if amount_key is not None and not owns:
        recent = payments_col.find_one({
            "user_id": u_id,
            "amount_paise": amount_key,
            "timestamp": {"$gte": datetime.now() - timedelta(hours=24)}
        })
        if recent:
            bot.answer_callback_query(call.id, "This payment was already approved.", show_alert=True)
            try:
                bot.edit_message_caption("✅ Already approved earlier.", call.message.chat.id, call.message.message_id)
            except Exception:
                pass
            return

    bot.answer_callback_query(call.id)
    if owns:
        pending_payments.pop(amount_key, None)
    try:
        bot.send_message(u_id,
            "❌ Your payment could not be verified. Please contact admin if you believe this is a mistake.",
            reply_markup=InlineKeyboardMarkup().add(
                InlineKeyboardButton("📞 Contact Admin", url=f"https://t.me/{CONTACT_USERNAME}")))
    except Exception:
        pass
    try:
        bot.edit_message_caption(
            f"❌ Rejected by Admin\nUser: {u_id}",
            call.message.chat.id, call.message.message_id)
    except Exception:
        pass

@bot.callback_query_handler(func=lambda call: call.data.startswith('manage_'))
def manage_ch(call):
    ch_id = int(call.data.split('_')[1])
    ch_data = channels_col.find_one({"channel_id": ch_id})
    bot_username = bot.get_me().username
    link = f"https://t.me/{bot_username}?start={ch_id}"
    bot.edit_message_text(
        f"Settings for: *{ch_data['name']}*\n\nYour Link: `{link}`\n\nTo change prices, use /editprice.",
        call.message.chat.id, call.message.message_id, parse_mode="Markdown")

# --- EXPIRY KICKER ---

def kick_expired_users():
    now_ts = datetime.now().timestamp()
    expired = list(users_col.find({"expiry": {"$lte": now_ts}}))
    for rec in expired:
        ch_id, u_id = rec['channel_id'], rec['user_id']
        try:
            bot.ban_chat_member(ch_id, u_id)
            bot.unban_chat_member(ch_id, u_id)
        except Exception as e:
            attempts = rec.get('kick_attempts', 0) + 1
            print(f"[Kicker] Could not remove {u_id} from {ch_id} (try {attempts}): {e}")
            if attempts < 5:
                users_col.update_one({"_id": rec['_id']}, {"$set": {"kick_attempts": attempts}})
                continue
            try:
                bot.send_message(ADMIN_ID,
                    f"⚠️ Could not remove user {u_id} from {channel_name(ch_id)} after 5 tries.\n"
                    f"Check that the bot is still an admin there with permission to ban users.\nError: {e}")
            except Exception:
                pass
        expired_col.update_one(
            {"user_id": u_id, "channel_id": ch_id},
            {"$set": {"expired_at": datetime.now()}},
            upsert=True
        )
        users_col.delete_one({"_id": rec['_id']})
        try:
            pct = get_bonus_percent()
            bonus_line = (f"\n\n🎁 Renew within {BONUS_WINDOW_HOURS} hours and get {pct}% extra time FREE!") if pct > 0 else ""
            markup = InlineKeyboardMarkup()
            markup.add(InlineKeyboardButton("🔄 Renew Now", callback_data=f"viewchannel_{ch_id}"))
            markup.add(InlineKeyboardButton("🎟 Redeem My Coins", callback_data=f"redeem_{ch_id}"))
            bot.send_message(u_id,
                f"⚠️ Your subscription to {channel_name(ch_id)} has expired."
                f"{bonus_line}\n\nTap Renew to pick a plan, or use your coins for free access! 🪙",
                reply_markup=markup)
        except Exception:
            pass

# --- REMINDERS ---

def remind_expiring_users():
    """One reminder, about an hour before a subscription ends."""
    if is_bot_paused():
        return
    now = datetime.now()
    now_ts = now.timestamp()
    soon_ts = now_ts + EXPIRY_REMINDER_MINUTES * 60
    due = list(users_col.find({
        "expiry": {"$gt": now_ts, "$lte": soon_ts},
        "reminded": {"$ne": True}
    }))
    for u in due:
        try:
            users_col.update_one({"_id": u["_id"]}, {"$set": {"reminded": True}})
            left_min = max(1, int((u['expiry'] - now_ts) // 60))
            name = channel_name(u['channel_id'])
            pct = get_bonus_percent()
            text = (f"⏰ Your subscription ends soon!\n\n"
                    f"Channel: {name}\n"
                    f"Time left: about {duration_label(left_min)}\n\n"
                    f"Renew now and the new time is added on top of what you have left — nothing is lost. 🔄")
            markup = InlineKeyboardMarkup()
            markup.add(InlineKeyboardButton("🔄 Renew Now", callback_data=f"viewchannel_{u['channel_id']}"))
            markup.add(InlineKeyboardButton("🎟 Use My Coins", callback_data=f"redeem_{u['channel_id']}"))
            bot.send_message(u['user_id'], text, reply_markup=markup)
        except Exception as e:
            print(f"[Reminder] Could not remind {u.get('user_id')}: {e}")

def remind_abandoned_payments():
    """One gentle reminder for people who were shown the QR but never sent a payment proof."""
    if is_bot_paused():
        return
    now = datetime.now()
    for amount_key, d in list(pending_payments.items()):
        try:
            if d.get('photo_file_id') or d.get('reminded'):
                continue
            idle = (now - d.get('last_touch', d['created_at'])).total_seconds()
            if idle < ABANDON_REMINDER_MINUTES * 60:
                continue
            d['reminded'] = True
            name = channel_name(d['ch_id'])
            text = (f"👋 Still thinking about it?\n\n"
                    f"You were about to join {name} — {duration_label(d['mins'])} for ₹{d['price']}.\n"
                    f"Your payment amount ₹{fmt_amount(amount_key)} is still reserved for you.\n\n"
                    f"✅ Already paid? Tap \"I Have Paid\" and send your screenshot.\n"
                    f"💳 Haven't paid yet? Tap \"Show QR Again\".\n\n"
                    f"Need help? Contact the admin anytime.")
            markup = InlineKeyboardMarkup()
            markup.add(InlineKeyboardButton("✅ I Have Paid", callback_data=f"paid_{d['ch_id']}_{d['mins']}_{amount_key}"))
            markup.add(InlineKeyboardButton("🔄 Show QR Again", callback_data=f"select_{d['ch_id']}_{d['mins']}"))
            markup.add(InlineKeyboardButton("📞 Contact Admin", url=f"https://t.me/{CONTACT_USERNAME}"))
            bot.send_message(d['user_id'], text, reply_markup=markup)
        except Exception as e:
            print(f"[Abandoned Reminder] Error: {e}")

# --- STARTUP ---
if __name__ == '__main__':
    keep_alive()
    scheduler = BackgroundScheduler()
    scheduler.add_job(kick_expired_users, 'interval', minutes=1)
    scheduler.add_job(check_gmail_payments, 'interval', minutes=1)
    scheduler.add_job(remind_expiring_users, 'interval', minutes=1)
    scheduler.add_job(remind_abandoned_payments, 'interval', minutes=1)
    scheduler.add_job(cleanup_expired_payments, 'interval', minutes=5)
    scheduler.start()
    bot.remove_webhook()
    print("Bot is running...")
    bot.infinity_polling(timeout=20, long_polling_timeout=10)