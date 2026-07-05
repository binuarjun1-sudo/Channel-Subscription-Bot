 import os
import re
import imaplib
import email
import telebot
from telebot.types import InlineKeyboardMarkup, InlineKeyboardButton
from pymongo import MongoClient
from datetime import datetime, timedelta
from apscheduler.schedulers.background import BackgroundScheduler
from flask import Flask
from threading import Thread

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
MINUTES_PER_COIN = 2

bot = telebot.TeleBot(BOT_TOKEN)
client = MongoClient(MONGO_URI)
db = client['sub_management']
channels_col = db['channels']
users_col = db['users']
coins_col = db['coins']
all_users_col = db['all_users']
payments_col = db['payments']       # tracks every approved payment
bot_state_col = db['bot_state']     # stores pause/resume state and message

pending_payments = {}
processed_email_ids = set()

# --- BOT STATE HELPERS ---

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
    for offset in range(1, 10):
        unique = base + offset
        if unique not in pending_payments:
            return unique
    return base + 1

def broadcast_to_all(message_text, extra_markup=None):
    """Send a message to every user who has ever started the bot."""
    all_users = all_users_col.find({})
    success = 0
    failed = 0
    for user in all_users:
        try:
            bot.send_message(user['user_id'], message_text,
                             reply_markup=extra_markup, parse_mode="Markdown")
            success += 1
        except Exception:
            failed += 1
    return success, failed

def show_plan_menu(chat_id, ch_id, ch_data):
    markup = InlineKeyboardMarkup()
    for p_time, p_price in ch_data['plans'].items():
        label = f"{p_time} Min" if int(p_time) < 60 else f"{int(p_time)//1440} Days"
        markup.add(InlineKeyboardButton(f"💳 {label} - ₹{p_price}", callback_data=f"select_{ch_id}_{p_time}"))
    markup.add(InlineKeyboardButton("🎁 FREE SUBSCRIPTION", callback_data=f"referral_{ch_id}"))
    markup.add(InlineKeyboardButton("💰 Show My Coin Balance", callback_data=f"coinbalance_{ch_id}"))
    markup.add(InlineKeyboardButton("🎟 Redeem My Coins", callback_data=f"redeem_{ch_id}"))
    markup.add(InlineKeyboardButton("📞 Contact Admin", url=f"https://t.me/{CONTACT_USERNAME}"))

    # Show channel description if set
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
    try:
        mail = imaplib.IMAP4_SSL('imap.gmail.com')
        mail.login(GMAIL_ADDRESS, GMAIL_APP_PASSWORD)
        mail.select('inbox')
        email_ids = []
        for term in ['payment', 'debited', 'credited', 'UPI', 'received', 'paid']:
            _, data = mail.search(None, f'(UNSEEN SUBJECT "{term}")')
            email_ids += data[0].split()
        email_ids = list(set(email_ids))
        for eid in email_ids:
            eid_str = eid.decode() if isinstance(eid, bytes) else eid
            if eid_str in processed_email_ids:
                continue
            _, msg_data = mail.fetch(eid, '(RFC822)')
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
            if amount is None:
                continue
            amount_key = int(amount)
            if amount_key in pending_payments:
                entry = pending_payments.pop(amount_key)
                processed_email_ids.add(eid_str)
                if len(processed_email_ids) > 500:
                    processed_email_ids.clear()
                auto_approve(
                    entry['user_id'], entry['ch_id'], int(entry['mins']),
                    entry.get('photo_file_id'), entry.get('user_name', 'Unknown'), entry['price']
                )
        mail.logout()
    except Exception as e:
        print(f"Gmail IMAP error: {e}")

def check_fallback_payments():
    now = datetime.now()
    for amount_key, data in list(pending_payments.items()):
        if not data.get('photo_file_id'):
            continue
        if data.get('fallback_sent'):
            continue
        elapsed = (now - data['timestamp']).total_seconds()
        if elapsed >= 300:
            try:
                markup = InlineKeyboardMarkup()
                markup.add(InlineKeyboardButton("✅ Approve", callback_data=f"app_{data['user_id']}_{data['ch_id']}_{data['mins']}_{amount_key}"))
                markup.add(InlineKeyboardButton("❌ Reject", callback_data=f"rej_{data['user_id']}_{amount_key}"))
                caption = (f"⚠️ *Manual Verification Required*\n"
                           f"_(Bot could not auto-verify)_\n\n"
                           f"User: {data['user_name']} (`{data['user_id']}`)\n"
                           f"Plan: {data['mins']} Mins\n"
                           f"Amount: ₹{amount_key} (base ₹{data['price']})")
                bot.send_photo(ADMIN_ID, data['photo_file_id'], caption=caption, reply_markup=markup, parse_mode="Markdown")
                pending_payments[amount_key]['fallback_sent'] = True
            except Exception as e:
                print(f"Fallback error: {e}")

def record_payment(u_id, user_name, ch_id, mins, price):
    """Save every approved payment for revenue tracking."""
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

def auto_approve(u_id, ch_id, mins, photo_file_id, user_name, price):
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
            f"🥳 *Payment Verified & Approved!*\n\n"
            f"Subscription: {mins} Minutes\n\n"
            f"Join Link: {link.invite_link}\n\n"
            f"⚠️ This link expires in {mins} minutes.",
            parse_mode="Markdown")
        caption = (f"✅ *Auto Verified by Bot*\n\n"
                   f"User: {user_name} (`{u_id}`)\n"
                   f"Plan: {mins} Mins\n"
                   f"Amount: ₹{price}\n\n"
                   f"_No action needed — invite link already sent._")
        if photo_file_id:
            bot.send_photo(ADMIN_ID, photo_file_id, caption=caption, parse_mode="Markdown")
        else:
            bot.send_message(ADMIN_ID, caption, parse_mode="Markdown")
    except Exception as e:
        bot.send_message(ADMIN_ID, f"❌ Auto approval error for user {u_id}: {e}")

# --- START HANDLER ---

@bot.message_handler(commands=['start'])
def start_handler(message):
    user_id = message.from_user.id
    text = message.text.split()

    track_user(message.from_user)

    # If bot is paused, show pause message to non-admin users
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
                            f"🪙 You earned *1 coin!*\n"
                            f"Check your balance: tap 💰 Show My Coin Balance",
                            parse_mode="Markdown")
                    except Exception:
                        pass
            ch_data = channels_col.find_one({"admin_id": ADMIN_ID})
            if ch_data:
                show_plan_menu(message.chat.id, ch_data['channel_id'], ch_data)
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
            "/channels — Manage Existing Channels\n"
            "/setdesc — Set Channel Description\n"
            "/stats — View Full Bot Statistics\n"
            "/pause — Pause Bot & Notify Users\n"
            "/resume — Resume Bot & Notify Users",
            parse_mode="Markdown")
    else:
        bot.send_message(message.chat.id, "Welcome! To join a channel, please use the link provided by the Admin.")

# --- STATS COMMAND ---

@bot.message_handler(commands=['stats'], func=lambda m: m.from_user.id == ADMIN_ID)
def show_stats(message):
    try:
        today = datetime.now().strftime("%Y-%m-%d")
        now_ts = datetime.now().timestamp()

        # Users
        total_users = all_users_col.count_documents({})
        new_today = all_users_col.count_documents({"joined_date": today})

        # Subscribers
        active_subs = users_col.count_documents({"expiry": {"$gt": now_ts}})
        total_subs_ever = payments_col.count_documents({})

        # Revenue
        today_revenue_pipeline = [
            {"$match": {"date": today}},
            {"$group": {"_id": None, "total": {"$sum": "$price"}}}
        ]
        today_rev_result = list(payments_col.aggregate(today_revenue_pipeline))
        today_revenue = today_rev_result[0]['total'] if today_rev_result else 0

        total_revenue_pipeline = [
            {"$group": {"_id": None, "total": {"$sum": "$price"}}}
        ]
        total_rev_result = list(payments_col.aggregate(total_revenue_pipeline))
        total_revenue = total_rev_result[0]['total'] if total_rev_result else 0

        # This month revenue
        this_month = datetime.now().strftime("%Y-%m")
        month_revenue_pipeline = [
            {"$match": {"date": {"$regex": f"^{this_month}"}}},
            {"$group": {"_id": None, "total": {"$sum": "$price"}}}
        ]
        month_rev_result = list(payments_col.aggregate(month_revenue_pipeline))
        month_revenue = month_rev_result[0]['total'] if month_rev_result else 0

        # Payments today count
        payments_today = payments_col.count_documents({"date": today})

        # Referrals
        total_referrals = 0
        for doc in coins_col.find({}):
            total_referrals += len(doc.get("referred_users", []))

        # Coins
        coins_pipeline = [{"$group": {"_id": None, "total": {"$sum": "$coins"}}}]
        coins_result = list(coins_col.aggregate(coins_pipeline))
        total_coins = coins_result[0]['total'] if coins_result else 0

        # Channels
        total_channels = channels_col.count_documents({"admin_id": ADMIN_ID})

        # Bot state
        state = "🟢 Running" if not is_bot_paused() else "🔴 Paused"

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
            f"→ Coin Value: *1 coin = {MINUTES_PER_COIN} mins*\n\n"
            f"━━━━━━━━━━━━━━━\n"
            f"📢 *Channels*\n"
            f"→ Total Channels: *{total_channels}*\n\n"
            f"━━━━━━━━━━━━━━━\n"
            f"🤖 *Bot Status:* {state}",
            parse_mode="Markdown")

    except Exception as e:
        bot.send_message(ADMIN_ID, f"❌ Error fetching stats: {e}")

# --- SET DESCRIPTION COMMAND ---

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
        f"Send the description text now.\n"
        f"This will be shown to users when they open the bot link.",
        parse_mode="Markdown")
    bot.register_next_step_handler(msg, save_description, ch_id)

def save_description(message, ch_id):
    desc = message.text.strip()
    channels_col.update_one({"channel_id": ch_id}, {"$set": {"description": desc}})
    ch_data = channels_col.find_one({"channel_id": ch_id})
    bot.send_message(ADMIN_ID,
        f"✅ Description saved for *{ch_data['name']}*!\n\n"
        f"📋 Preview:\n{desc}\n\n"
        f"Users will now see this when they open your bot link.",
        parse_mode="Markdown")

# --- PAUSE COMMAND ---

@bot.message_handler(commands=['pause'], func=lambda m: m.from_user.id == ADMIN_ID)
def pause_bot(message):
    if is_bot_paused():
        bot.send_message(ADMIN_ID, "⚠️ Bot is already paused. Use /resume to resume it.")
        return
    msg = bot.send_message(ADMIN_ID,
        "⏸ *Pause Bot*\n\n"
        "Please send the message you want to show to users while the bot is paused.\n\n"
        "Example: _We are updating the bot. Back in 10 minutes!_",
        parse_mode="Markdown")
    bot.register_next_step_handler(msg, confirm_pause)

def confirm_pause(message):
    pause_msg = message.text.strip()
    set_bot_paused(True, pause_msg)

    # Broadcast pause message to all users
    broadcast_text = (f"🔴 *Bot Paused*\n\n"
                      f"{pause_msg}\n\n"
                      f"We apologize for the inconvenience. Please wait for the bot to resume.")

    bot.send_message(ADMIN_ID, "⏳ Broadcasting pause message to all users...")
    success, failed = broadcast_to_all(broadcast_text)

    bot.send_message(ADMIN_ID,
        f"✅ *Bot Paused Successfully!*\n\n"
        f"📨 Broadcast sent to *{success}* users\n"
        f"❌ Failed for *{failed}* users (they may have blocked the bot)\n\n"
        f"Use /resume when you're ready to bring the bot back online.",
        parse_mode="Markdown")

# --- RESUME COMMAND ---

@bot.message_handler(commands=['resume'], func=lambda m: m.from_user.id == ADMIN_ID)
def resume_bot(message):
    if not is_bot_paused():
        bot.send_message(ADMIN_ID, "⚠️ Bot is already running. Use /pause to pause it.")
        return
    msg = bot.send_message(ADMIN_ID,
        "▶️ *Resume Bot*\n\n"
        "Please send the message you want to broadcast to all users when resuming.\n\n"
        "Example: _Bot is back! New content added. Join now!_",
        parse_mode="Markdown")
    bot.register_next_step_handler(msg, confirm_resume)

def confirm_resume(message):
    resume_msg = message.text.strip()
    set_bot_paused(False, "")

    # Get channel link to include in broadcast
    ch_data = channels_col.find_one({"admin_id": ADMIN_ID})
    bot_username = bot.get_me().username

    markup = None
    if ch_data:
        ch_link = f"https://t.me/{bot_username}?start={ch_data['channel_id']}"
        markup = InlineKeyboardMarkup()
        markup.add(InlineKeyboardButton("🚀 Join Now!", url=ch_link))

    broadcast_text = (f"🟢 *Bot is Back Online!*\n\n"
                      f"{resume_msg}\n\n"
                      f"Tap the button below to join! 👇")

    bot.send_message(ADMIN_ID, "⏳ Broadcasting resume message to all users...")
    success, failed = broadcast_to_all(broadcast_text, markup)

    bot.send_message(ADMIN_ID,
        f"✅ *Bot Resumed Successfully!*\n\n"
        f"📨 Broadcast sent to *{success}* users\n"
        f"❌ Failed for *{failed}* users\n\n"
        f"Bot is now fully operational! ✅",
        parse_mode="Markdown")

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
            f"✅ Setup Successful!\n\nPlans for *{ch_name}*:\n{summary}\n\nInvite Link for users:\n`https://t.me/{bot_username}?start={ch_id}`",
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
    markup = InlineKeyboardMarkup()
    markup.add(InlineKeyboardButton("🔙 Back to Plans", callback_data=f"backplans_{ch_id}"))
    bot.answer_callback_query(call.id)
    bot.send_message(call.message.chat.id,
        f"🎁 *FREE SUBSCRIPTION*\n\n"
        f"Your personal referral link:\n`{ref_link}`\n\n"
        f"📌 *How it works:*\n"
        f"→ Share your link with friends\n"
        f"→ When a new friend opens the bot using your link you earn *1 coin*\n"
        f"→ Each friend can only give you 1 coin\n"
        f"→ No purchase needed — anyone can earn!\n\n"
        f"💰 *What is a coin worth?*\n"
        f"→ 1 coin = 2 minutes free access\n"
        f"→ 5 coins = 10 minutes\n"
        f"→ 30 coins = 1 hour\n"
        f"→ 720 coins = 1 full day 🔥",
        reply_markup=markup, parse_mode="Markdown")

# --- COIN BALANCE ---

@bot.callback_query_handler(func=lambda call: call.data.startswith('coinbalance_'))
def show_coin_balance(call):
    ch_id = call.data.split('_')[1]
    user = call.from_user
    data = get_coin_data(user.id)
    coins = data.get('coins', 0)
    worth_minutes = coins * MINUTES_PER_COIN
    markup = InlineKeyboardMarkup()
    markup.add(InlineKeyboardButton("🎟 Redeem My Coins", callback_data=f"redeem_{ch_id}"))
    markup.add(InlineKeyboardButton("🔙 Back to Plans", callback_data=f"backplans_{ch_id}"))
    bot.answer_callback_query(call.id)
    bot.send_message(call.message.chat.id,
        f"💰 *Your Coin Balance*\n\n"
        f"🪙 Coins: *{coins}*\n"
        f"⏱ Worth: *{worth_minutes} minutes* of free access\n\n"
        f"📌 *How to earn coins:*\n"
        f"→ Share your referral link with friends\n"
        f"→ Each new friend who opens the bot = 1 coin\n"
        f"→ Each friend can only give you 1 coin\n\n"
        f"💡 *Coin value:*\n"
        f"→ 1 coin = 2 minutes\n"
        f"→ 5 coins = 10 minutes\n"
        f"→ 30 coins = 1 hour\n"
        f"→ 720 coins = 1 full day 🔥",
        reply_markup=markup, parse_mode="Markdown")

# --- REDEEM COINS ---

@bot.callback_query_handler(func=lambda call: call.data.startswith('redeem_'))
def redeem_coins_start(call):
    ch_id = call.data.split('_')[1]
    user = call.from_user
    data = get_coin_data(user.id)
    coins = data.get('coins', 0)
    worth_minutes = coins * MINUTES_PER_COIN
    bot.answer_callback_query(call.id)
    if coins == 0:
        markup = InlineKeyboardMarkup()
        markup.add(InlineKeyboardButton("🎁 FREE SUBSCRIPTION", callback_data=f"referral_{ch_id}"))
        markup.add(InlineKeyboardButton("🔙 Back to Plans", callback_data=f"backplans_{ch_id}"))
        bot.send_message(call.message.chat.id,
            "🪙 *You have 0 coins*\n\n"
            "You don't have any coins yet!\n\n"
            "📌 Share your referral link with friends to earn coins and get free access!",
            reply_markup=markup, parse_mode="Markdown")
        return
    markup = InlineKeyboardMarkup()
    markup.add(InlineKeyboardButton("🔙 Cancel", callback_data=f"backplans_{ch_id}"))
    msg = bot.send_message(call.message.chat.id,
        f"🎟 *Redeem Your Coins*\n\n"
        f"You currently have *{coins} coins*\n"
        f"Worth *{worth_minutes} minutes* of free access\n\n"
        f"📌 *How redemption works:*\n"
        f"→ Enter how many coins to redeem\n"
        f"→ Each coin = 2 minutes access\n"
        f"→ You'll get instant access to the channel\n"
        f"→ When time is up you'll be automatically removed\n\n"
        f"How many coins do you want to redeem? (max {coins})",
        reply_markup=markup, parse_mode="Markdown")
    bot.register_next_step_handler(msg, process_redeem, ch_id, coins)

def process_redeem(message, ch_id, max_coins):
    user = message.from_user
    text = message.text.strip()
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
    minutes = coins_to_use * MINUTES_PER_COIN
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
            f"🥳 *Access Granted!*\n\n"
            f"🪙 Coins used: {coins_to_use}\n"
            f"⏱ Access duration: {minutes} minutes\n\n"
            f"Join Link: {link.invite_link}\n\n"
            f"⚠️ You will be automatically removed after {minutes} minutes.",
            parse_mode="Markdown")
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
    pending_payments[unique_amount] = {
        "user_id": call.from_user.id,
        "user_name": call.from_user.first_name,
        "ch_id": int(ch_id),
        "mins": mins,
        "price": price,
        "timestamp": datetime.now(),
        "photo_file_id": None,
        "fallback_sent": False
    }
    markup = InlineKeyboardMarkup()
    markup.add(InlineKeyboardButton("✅ I Have Paid", callback_data=f"paid_{ch_id}_{mins}_{unique_amount}"))
    markup.add(InlineKeyboardButton("🔙 Choose Another Plan", callback_data=f"backplans_{ch_id}"))
    markup.add(InlineKeyboardButton("📞 Contact Admin", url=f"https://t.me/{CONTACT_USERNAME}"))
    bot.send_photo(call.message.chat.id, qr_url,
        caption=(f"Plan: {mins} Minutes\n"
                 f"Price: ₹{price}\n"
                 f"UPI ID: `{UPI_ID}`\n\n"
                 f"⚠️ *Please pay exactly ₹{unique_amount}* — this unique amount helps us verify your payment automatically.\n\n"
                 f"After paying tap 'I Have Paid' below."),
        reply_markup=markup, parse_mode="Markdown")

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
        "⏳ *Got your screenshot!*\n\n"
        "The bot is now verifying your payment automatically.\n\n"
        "✅ If verified → invite link arrives within 1 minute.\n"
        "👨‍💼 If not → admin will review and approve manually.\n\n"
        "Either way you're covered ❤️",
        parse_mode="Markdown")

# --- MANUAL APPROVAL ---

@bot.callback_query_handler(func=lambda call: call.data.startswith('app_'))
def approve_now(call):
    parts = call.data.split('_')
    u_id, ch_id, mins = int(parts[1]), int(parts[2]), int(parts[3])
    amount_key = int(parts[4]) if len(parts) > 4 else None
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
        bot.send_message(u_id,
            f"🥳 *Payment Approved!*\n\n"
            f"Subscription: {mins} Minutes\n\n"
            f"Join Link: {link.invite_link}\n\n"
            f"⚠️ This link expires in {mins} minutes.",
            parse_mode="Markdown")
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
    if amount_key:
        pending_payments.pop(amount_key, None)
    try:
        bot.send_message(u_id,
            "❌ Your payment could not be verified. Please contact admin if you believe this is a mistake.",
            reply_markup=InlineKeyboardMarkup().add(
                InlineKeyboardButton("📞 Contact Admin", url=f"https://t.me/{CONTACT_USERNAME}")))
    except Exception:
        pass
    bot.edit_message_caption(
        f"❌ Rejected by Admin\nUser: {u_id}",
        call.message.chat.id, call.message.message_id)

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
            rejoin_url = f"https://t.me/{bot_username}?start={user['channel_id']}"
            markup = InlineKeyboardMarkup()
            markup.add(InlineKeyboardButton("🔄 Re-join / Renew", url=rejoin_url))
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
    scheduler.add_job(check_fallback_payments, 'interval', minutes=2)
    scheduler.start()
    bot.remove_webhook()
    print("Bot is running...")
    bot.infinity_polling(timeout=20, long_polling_timeout=10)
