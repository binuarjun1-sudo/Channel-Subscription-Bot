import os
import re
import time
import imaplib
import email
import email.utils
import telebot
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
BROADCAST_WORKERS = 20          # parallel senders
BROADCAST_MAX_PER_SECOND = 25   # stay under Telegram's ~30 msgs/sec limit

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

pending_payments = {}

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

def get_unique_amount(base_price):
    base = int(base_price)
    for offset in range(1, 50):
        unique = base + offset
        if unique not in pending_payments:
            return unique
    return base + 1

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
    """Sends one message. Returns 'ok', 'dead' (blocked/deleted), or 'failed' (temporary)."""
    for attempt in range(3):
        try:
            _throttle()
            bot.send_message(user_id, text, reply_markup=markup, parse_mode="Markdown")
            return "ok"
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
                return "dead"
            if code == 400 and "chat not found" in desc:
                return "dead"
            if code == 400 and "parse" in desc:
                # Formatting problem in the message: retry once as plain text
                try:
                    _throttle()
                    bot.send_message(user_id, text, reply_markup=markup)
                    return "ok"
                except ApiTelegramException as e2:
                    if e2.error_code == 403:
                        return "dead"
                    return "failed"
                except Exception:
                    return "failed"
            return "failed"
        except Exception:
            time.sleep(1)
            continue
    return "failed"

def broadcast_to_all(message_text, extra_markup=None, status_msg_id=None):
    """Sends to all users in parallel. Removes blocked/deleted users from the list.
    Returns (delivered, failed, removed)."""
    user_ids = [u['user_id'] for u in all_users_col.find({}, {"user_id": 1})]
    total = len(user_ids)
    delivered = 0
    failed = 0
    dead_ids = []
    done = 0
    last_update = time.time()

    if total == 0:
        return 0, 0, 0

    with ThreadPoolExecutor(max_workers=BROADCAST_WORKERS) as executor:
        futures = {executor.submit(_send_one, uid, message_text, extra_markup): uid for uid in user_ids}
        for future in as_completed(futures):
            uid = futures[future]
            try:
                result = future.result()
            except Exception:
                result = "failed"
            if result == "ok":
                delivered += 1
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

    removed = 0
    if dead_ids:
        try:
            result = all_users_col.delete_many({"user_id": {"$in": dead_ids}})
            removed = result.deleted_count
        except Exception as e:
            print(f"[Broadcast] Could not remove dead users: {e}")
    return delivered, failed, removed

def run_broadcast_job(text, markup, status_msg_id, title, footer=""):
    """Runs a broadcast in the background so the bot stays responsive."""
    def job():
        try:
            delivered, failed, removed = broadcast_to_all(text, markup, status_msg_id)
            report = (f"{title}\n\n"
                      f"📨 Delivered: *{delivered}*\n"
                      f"🗑 Removed (blocked/deleted): *{removed}*\n"
                      f"⚠️ Failed (temporary): *{failed}*")
            if footer:
                report += f"\n\n{footer}"
            bot.send_message(ADMIN_ID, report, parse_mode="Markdown")
        except Exception as e:
            print(f"[Broadcast] Job error: {e}")
            try:
                bot.send_message(ADMIN_ID, f"❌ Broadcast error: {e}")
            except Exception:
                pass
    Thread(target=job, daemon=True).start()

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
    markup = InlineKeyboardMarkup()
    for p_time, p_price in ch_data['plans'].items():
        label = f"{p_time} Min" if int(p_time) < 60 else f"{int(p_time)//1440} Days"
        markup.add(InlineKeyboardButton(f"💳 {label} - ₹{p_price}", callback_data=f"select_{ch_id}_{p_time}"))
    markup.add(InlineKeyboardButton("🎁 FREE SUBSCRIPTION", callback_data=f"referral_{ch_id}"))
    markup.add(InlineKeyboardButton("💰 Show My Coin Balance", callback_data=f"coinbalance_{ch_id}"))
    markup.add(InlineKeyboardButton("🎟 Redeem My Coins", callback_data=f"redeem_{ch_id}"))
    markup.add(InlineKeyboardButton("🔙 All Channels", callback_data="channellist_back"))
    markup.add(InlineKeyboardButton("📞 Contact Admin", url=f"https://t.me/{CONTACT_USERNAME}"))
    desc = ch_data.get("description", "")
    welcome_text = f"Welcome!\n\nYou are joining: *{ch_data['name']}*"
    if desc:
        welcome_text += f"\n\n📋 *About this channel:*\n{desc}"
    welcome_text += "\n\nPlease select a subscription plan below:"
    bot.send_message(chat_id, welcome_text, reply_markup=markup, parse_mode="Markdown")

# --- GMAIL IMAP ---

def extract_amount_from_text(text):
    patterns = [
        r'(?:rs\.?|inr|₹)\s*(\d+(?:\.\d{1,2})?)',
        r'(\d+(?:\.\d{1,2})?)\s*(?:rs\.?|inr|₹)',
        r'amount.*?(\d+(?:\.\d{1,2})?)',
        r'paid.*?(\d+(?:\.\d{1,2})?)',
        r'debited.*?(\d+(?:\.\d{1,2})?)',
        r'received.*?(\d+(?:\.\d{1,2})?)',
        r'credited.*?(\d+(?:\.\d{1,2})?)',
    ]
    text_lower = text.lower()
    for pattern in patterns:
        match = re.search(pattern, text_lower)
        if match:
            try:
                return float(match.group(1))
            except Exception:
                continue
    return None

def check_gmail_payments():
    if not pending_payments:
        return
    print(f"[Gmail Check] Started. Pending payments: {list(pending_payments.keys())}")
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
            subject = msg.get('subject', '')
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
            amount_key = int(amount)
            if amount_key in pending_payments:
                # SECURITY CHECK: the email must have arrived AFTER the user picked the plan
                # (not after the screenshot). Stops old emails from approving newer payments.
                email_dt = get_email_datetime(msg)
                entry_peek = pending_payments[amount_key]
                pending_created = entry_peek.get('created_at', entry_peek['timestamp'])
                if email_dt is not None and email_dt < (pending_created - timedelta(minutes=5)):
                    print(f"[Gmail Check] Email for amount {amount_key} is OLDER than the pending payment "
                          f"(email: {email_dt}, pending created: {pending_created}). Skipping.")
                    continue
                print(f"[Gmail Check] MATCH FOUND for amount {amount_key}, auto-approving")
                entry = pending_payments.pop(amount_key)
                mark_email_processed(eid_str)
                try:
                    mail.store(eid, '+FLAGS', '\\Seen')
                except Exception as e:
                    print(f"[Gmail Check] Could not mark as seen: {e}")
                auto_approve(
                    entry['user_id'], entry['ch_id'], int(entry['mins']),
                    entry.get('photo_file_id'), entry.get('user_name', 'Unknown'), entry['price'],
                    entry.get('admin_msg_id')
                )
            else:
                print(f"[Gmail Check] Amount {amount_key} extracted but no matching pending payment")
        mail.logout()
        print(f"[Gmail Check] Finished successfully")
    except Exception as e:
        print(f"Gmail IMAP error: {e}")

def record_payment(u_id, user_name, ch_id, mins, price):
    today = datetime.now().strftime("%Y-%m-%d")
    payments_col.insert_one({
        "user_id": u_id,
        "user_name": user_name,
        "channel_id": ch_id,
        "mins": mins,
        "price": int(price),
        "date": today,
        "timestamp": datetime.now()
    })

def auto_approve(u_id, ch_id, mins, photo_file_id, user_name, price, admin_msg_id=None):
    try:
        expiry_datetime = datetime.now() + timedelta(minutes=mins)
        expiry_ts = int(expiry_datetime.timestamp())
        link = bot.create_chat_invite_link(ch_id, member_limit=1, expire_date=expiry_ts)
        users_col.update_one(
            {"user_id": u_id, "channel_id": ch_id},
            {"$set": {"expiry": expiry_datetime.timestamp()}},
            upsert=True
        )
        record_payment(u_id, user_name, ch_id, mins, price)
        bot.send_message(u_id,
            f"🥳 Payment Verified & Approved!\n\n"
            f"Subscription: {mins} Minutes\n\n"
            f"Join Link: {link.invite_link}\n\n"
            f"⚠️ This link expires in {mins} minutes.")
        caption = (f"✅ Auto Verified by Bot\n\n"
                   f"User: {user_name} ({u_id})\n"
                   f"Plan: {mins} Mins\n"
                   f"Amount: ₹{price}\n\n"
                   f"No action needed — invite link already sent.")
        updated = False
        if admin_msg_id:
            try:
                # Update the message that was already sent to admin (buttons are removed)
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
        bot.send_message(ADMIN_ID, f"❌ Auto approval error for user {u_id}: {e}")

# --- EMAIL STATS (READ ONLY) ---

@bot.message_handler(commands=['emailstats'], func=lambda m: m.from_user.id == ADMIN_ID)
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
            "/add — Add/Edit Channel & Prices\n"
            "/delete — Delete a Channel\n"
            "/channels — Manage Existing Channels\n"
            "/setdesc — Set Channel Description\n"
            "/stats — View Full Bot Statistics\n"
            "/setcoin — Change Coin Value\n"
            "/broadcast — Broadcast Message to All Users\n"
            "/pause — Pause Bot & Notify Users\n"
            "/resume — Resume Bot & Notify Users\n"
            "/emailstats — View Gmail Inbox Stats (read-only)",
            parse_mode="Markdown")
    else:
        show_channel_list(message.chat.id)

# --- STATS ---

@bot.message_handler(commands=['stats'], func=lambda m: m.from_user.id == ADMIN_ID)
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

@bot.message_handler(commands=['setcoin'], func=lambda m: m.from_user.id == ADMIN_ID)
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
    text = message.text.strip()
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

# --- BROADCAST ---

@bot.message_handler(commands=['broadcast'], func=lambda m: m.from_user.id == ADMIN_ID)
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
                      "✅ *Broadcast Complete!*")

# --- SET DESCRIPTION ---

@bot.message_handler(commands=['setdesc'], func=lambda m: m.from_user.id == ADMIN_ID)
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

@bot.message_handler(commands=['delete'], func=lambda m: m.from_user.id == ADMIN_ID)
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

@bot.message_handler(commands=['pause'], func=lambda m: m.from_user.id == ADMIN_ID)
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
                      "Use /resume when ready.")

# --- RESUME ---

@bot.message_handler(commands=['resume'], func=lambda m: m.from_user.id == ADMIN_ID)
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
                      "Bot is now fully operational! ✅")

# --- ADMIN CHANNEL MANAGEMENT ---

@bot.message_handler(commands=['channels'], func=lambda m: m.from_user.id == ADMIN_ID)
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

@bot.message_handler(commands=['add'], func=lambda m: m.from_user.id == ADMIN_ID)
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
            label = f"{mins} Min" if int(mins) < 60 else f"{int(mins)//1440} Days"
            summary_lines.append(f"• {label} — ₹{price}")
        summary = "\n".join(summary_lines)
        bot_username = bot.get_me().username
        bot.send_message(ADMIN_ID,
            f"✅ Setup Successful!\n\nPlans for *{ch_name}*:\n{summary}\n\n"
            f"Users can now access this channel via your main bot link:\n`https://t.me/{bot_username}`\n\n"
            f"Or this direct link:\n`https://t.me/{bot_username}?start={ch_id}`",
            parse_mode="Markdown")
    except Exception as e:
        bot.send_message(ADMIN_ID, f"❌ Something went wrong while saving: {e}")

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
    text = message.text.strip()
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
        expiry_datetime = datetime.now() + timedelta(minutes=minutes)
        expiry_ts = int(expiry_datetime.timestamp())
        link = bot.create_chat_invite_link(ch_id_int, member_limit=1, expire_date=expiry_ts)
        coins_col.update_one({"user_id": user.id}, {"$inc": {"coins": -coins_to_use}})
        users_col.update_one(
            {"user_id": user.id, "channel_id": ch_id_int},
            {"$set": {"expiry": expiry_datetime.timestamp()}},
            upsert=True
        )
        bot.send_message(message.chat.id,
            f"🥳 Access Granted!\n\n"
            f"🪙 Coins used: {coins_to_use}\n"
            f"⏱ Access duration: {minutes} minutes\n\n"
            f"Join Link: {link.invite_link}\n\n"
            f"⚠️ You will be automatically removed after {minutes} minutes.")
        bot.send_message(ADMIN_ID,
            f"🪙 *Coin Redemption*\n\n"
            f"User: {user.first_name} (`{user.id}`)\n"
            f"Coins used: {coins_to_use}\n"
            f"Access: {minutes} minutes",
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
    price = ch_data['plans'][mins]
    unique_amount = get_unique_amount(price)
    qr_url = f"https://api.qrserver.com/v1/create-qr-code/?size=300x300&data=upi://pay?pa={UPI_ID}%26am={unique_amount}%26cu=INR"
    now = datetime.now()
    pending_payments[unique_amount] = {
        "user_id": call.from_user.id,
        "user_name": call.from_user.first_name,
        "ch_id": int(ch_id),
        "mins": mins,
        "price": price,
        "created_at": now,
        "timestamp": now,
        "photo_file_id": None,
        "admin_msg_id": None
    }
    markup = InlineKeyboardMarkup()
    markup.add(InlineKeyboardButton("✅ I Have Paid", callback_data=f"paid_{ch_id}_{mins}_{unique_amount}"))
    markup.add(InlineKeyboardButton("🔙 Choose Another Plan", callback_data=f"backplans_{ch_id}"))
    markup.add(InlineKeyboardButton("📞 Contact Admin", url=f"https://t.me/{CONTACT_USERNAME}"))
    bot.send_photo(call.message.chat.id, qr_url,
        caption=(f"Plan: {mins} Minutes\n"
                 f"Price: ₹{price}\n"
                 f"UPI ID: {UPI_ID}\n\n"
                 f"⚠️ Please pay exactly ₹{unique_amount} — this unique amount helps us verify your payment automatically.\n\n"
                 f"After paying tap 'I Have Paid' below."),
        reply_markup=markup)

@bot.callback_query_handler(func=lambda call: call.data.startswith('paid_'))
def request_proof(call):
    parts = call.data.split('_')
    ch_id = parts[1]
    unique_amount = int(parts[3])
    if unique_amount not in pending_payments:
        bot.answer_callback_query(call.id)
        bot.send_message(call.message.chat.id, "✅ Your payment was already verified! Check above for your invite link.")
        return
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
    caption = (f"🔔 New Payment Proof (auto-check running...)\n\n"
               f"User: {data['user_name']} ({data['user_id']})\n"
               f"Plan: {data['mins']} Mins\n"
               f"Amount to match: ₹{unique_amount} (base ₹{data['price']})\n\n"
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
    # Instantly notify the admin (auto-verification keeps running in the background)
    admin_msg_id = send_proof_to_admin(unique_amount, pending_payments[unique_amount])
    if unique_amount in pending_payments:
        pending_payments[unique_amount]['admin_msg_id'] = admin_msg_id

# --- MANUAL APPROVAL ---

@bot.callback_query_handler(func=lambda call: call.data.startswith('app_'))
def approve_now(call):
    parts = call.data.split('_')
    u_id, ch_id, mins = int(parts[1]), int(parts[2]), int(parts[3])
    amount_key = int(parts[4]) if len(parts) > 4 else None

    # Guard: if this payment is no longer pending, check whether it was already approved
    if amount_key is not None and amount_key not in pending_payments:
        recent = payments_col.find_one({
            "user_id": u_id,
            "channel_id": ch_id,
            "timestamp": {"$gte": datetime.now() - timedelta(minutes=30)}
        })
        if recent:
            bot.answer_callback_query(call.id, "Already approved (auto-verified or approved earlier).", show_alert=True)
            try:
                bot.edit_message_caption("✅ Already approved earlier.", call.message.chat.id, call.message.message_id)
            except Exception:
                pass
            return

    bot.answer_callback_query(call.id)
    try:
        expiry_datetime = datetime.now() + timedelta(minutes=mins)
        expiry_ts = int(expiry_datetime.timestamp())
        link = bot.create_chat_invite_link(ch_id, member_limit=1, expire_date=expiry_ts)
        users_col.update_one(
            {"user_id": u_id, "channel_id": ch_id},
            {"$set": {"expiry": expiry_datetime.timestamp()}},
            upsert=True
        )
        if amount_key and amount_key in pending_payments:
            entry = pending_payments.pop(amount_key)
            record_payment(u_id, entry.get('user_name', 'Unknown'), ch_id, mins, entry.get('price', 0))
        elif amount_key:
            # Bot restarted and lost the pending entry; still record the payment
            record_payment(u_id, "Unknown", ch_id, mins, amount_key)
        bot.send_message(u_id,
            f"🥳 Payment Approved!\n\n"
            f"Subscription: {mins} Minutes\n\n"
            f"Join Link: {link.invite_link}\n\n"
            f"⚠️ This link expires in {mins} minutes.")
        bot.edit_message_caption(
            f"✅ Manually approved by Admin\nUser: {u_id} | Plan: {mins} mins",
            call.message.chat.id, call.message.message_id)
    except Exception as e:
        bot.send_message(ADMIN_ID, f"❌ Error: {e}")

@bot.callback_query_handler(func=lambda call: call.data.startswith('rej_'))
def reject_payment(call):
    parts = call.data.split('_')
    u_id = int(parts[1])
    amount_key = int(parts[2]) if len(parts) > 2 else None

    # Guard: don't reject a payment that was already auto-approved
    if amount_key is not None and amount_key not in pending_payments:
        recent = payments_col.find_one({
            "user_id": u_id,
            "timestamp": {"$gte": datetime.now() - timedelta(minutes=30)}
        })
        if recent:
            bot.answer_callback_query(call.id, "This payment was already approved.", show_alert=True)
            try:
                bot.edit_message_caption("✅ Already approved earlier.", call.message.chat.id, call.message.message_id)
            except Exception:
                pass
            return

    bot.answer_callback_query(call.id)
    if amount_key:
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
        f"Settings for: *{ch_data['name']}*\n\nYour Link: `{link}`\n\nTo edit prices, use /add and forward a message from this channel again.",
        call.message.chat.id, call.message.message_id, parse_mode="Markdown")

# --- EXPIRY KICKER ---

def kick_expired_users():
    now = datetime.now().timestamp()
    expired_users = users_col.find({"expiry": {"$lte": now}})
    bot_username = bot.get_me().username
    for user in expired_users:
        try:
            bot.ban_chat_member(user['channel_id'], user['user_id'])
            bot.unban_chat_member(user['channel_id'], user['user_id'])
            markup = InlineKeyboardMarkup()
            markup.add(InlineKeyboardButton("🔄 Re-join / Renew", url=f"https://t.me/{bot_username}"))
            markup.add(InlineKeyboardButton("🎟 Redeem My Coins", callback_data=f"redeem_{user['channel_id']}"))
            bot.send_message(user['user_id'],
                "⚠️ Your subscription has expired.\n\n"
                "To join again tap Re-join or use your coins for free access! 🪙",
                reply_markup=markup)
            users_col.delete_one({"_id": user['_id']})
        except Exception:
            pass

# --- STARTUP ---
if __name__ == '__main__':
    keep_alive()
    scheduler = BackgroundScheduler()
    scheduler.add_job(kick_expired_users, 'interval', minutes=1)
    scheduler.add_job(check_gmail_payments, 'interval', minutes=1)
    scheduler.start()
    bot.remove_webhook()
    print("Bot is running...")
    bot.infinity_polling(timeout=20, long_polling_timeout=10)