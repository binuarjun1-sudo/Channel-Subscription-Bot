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

bot = telebot.TeleBot(BOT_TOKEN)
client = MongoClient(MONGO_URI)
db = client['sub_management']
channels_col = db['channels']
users_col = db['users']

# pending_payments key = unique_amount (int)
pending_payments = {}
processed_email_ids = set()

# --- IMAP GMAIL READER ---

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
    """Poll Gmail via IMAP every 1 minute to auto verify payments."""
    if not pending_payments:
        return

    try:
        mail = imaplib.IMAP4_SSL('imap.gmail.com')
        mail.login(GMAIL_ADDRESS, GMAIL_APP_PASSWORD)
        mail.select('inbox')

        # Search unseen payment emails
        _, data = mail.search(None, '(UNSEEN SUBJECT "payment")')
        email_ids = data[0].split()

        # Also search for other common UPI email subjects
        for search_term in ['debited', 'credited', 'UPI', 'received', 'paid']:
            _, data2 = mail.search(None, f'(UNSEEN SUBJECT "{search_term}")')
            email_ids += data2[0].split()

        # Remove duplicates
        email_ids = list(set(email_ids))

        for eid in email_ids:
            eid_str = eid.decode() if isinstance(eid, bytes) else eid

            if eid_str in processed_email_ids:
                continue

            _, msg_data = mail.fetch(eid, '(RFC822)')
            msg = email.message_from_bytes(msg_data[0][1])

            subject = msg.get('subject', '')

            # Get email body
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

            full_text = subject + " " + body
            amount = extract_amount_from_text(full_text)

            if amount is None:
                continue

            amount_key = int(amount)
            if amount_key in pending_payments:
                data_entry = pending_payments.pop(amount_key)
                processed_email_ids.add(eid_str)

                if len(processed_email_ids) > 500:
                    processed_email_ids.clear()

                auto_approve(
                    data_entry['user_id'],
                    data_entry['ch_id'],
                    int(data_entry['mins']),
                    data_entry.get('photo_file_id'),
                    data_entry.get('user_name', 'Unknown'),
                    data_entry['price']
                )

        mail.logout()

    except Exception as e:
        print(f"Gmail IMAP error: {e}")


def check_fallback_payments():
    """Every 2 mins — if unverified for 5+ mins, send to admin for manual approval."""
    now = datetime.now()
    for amount_key, data in list(pending_payments.items()):
        if not data.get('photo_file_id'):
            continue
        if data.get('fallback_sent'):
            continue

        elapsed = (now - data['timestamp']).total_seconds()
        if elapsed >= 300:  # 5 minutes
            try:
                markup = InlineKeyboardMarkup()
                markup.add(InlineKeyboardButton("✅ Approve", callback_data=f"app_{data['user_id']}_{data['ch_id']}_{data['mins']}_{amount_key}"))
                markup.add(InlineKeyboardButton("❌ Reject", callback_data=f"rej_{data['user_id']}_{amount_key}"))

                caption = (f"⚠️ *Manual Verification Required*\n"
                           f"_(Bot could not auto-verify this payment)_\n\n"
                           f"User: {data['user_name']} (`{data['user_id']}`)\n"
                           f"Plan: {data['mins']} Mins\n"
                           f"Amount: ₹{amount_key} (base ₹{data['price']})")

                bot.send_photo(ADMIN_ID, data['photo_file_id'],
                               caption=caption,
                               reply_markup=markup,
                               parse_mode="Markdown")

                pending_payments[amount_key]['fallback_sent'] = True

            except Exception as e:
                print(f"Fallback error: {e}")


def auto_approve(u_id, ch_id, mins, photo_file_id, user_name, price):
    """Auto approve after Gmail confirms payment."""
    try:
        expiry_datetime = datetime.now() + timedelta(minutes=mins)
        expiry_ts = int(expiry_datetime.timestamp())

        link = bot.create_chat_invite_link(ch_id, member_limit=1, expire_date=expiry_ts)
        users_col.update_one(
            {"user_id": u_id, "channel_id": ch_id},
            {"$set": {"expiry": expiry_datetime.timestamp()}},
            upsert=True
        )

        # Notify user
        bot.send_message(u_id,
            f"🥳 *Payment Verified & Approved!*\n\n"
            f"Subscription: {mins} Minutes\n\n"
            f"Join Link: {link.invite_link}\n\n"
            f"⚠️ This link expires in {mins} minutes.",
            parse_mode="Markdown")

        # Notify admin — marked as bot verified
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


def get_unique_amount(base_price):
    base = int(base_price)
    for offset in range(1, 10):
        unique = base + offset
        if unique not in pending_payments:
            return unique
    return base + 1

# --- ADMIN LOGIC ---

@bot.message_handler(commands=['start'])
def start_handler(message):
    user_id = message.from_user.id
    text = message.text.split()

    if len(text) > 1:
        try:
            ch_id = int(text[1])
            ch_data = channels_col.find_one({"channel_id": ch_id})
            if ch_data:
                markup = InlineKeyboardMarkup()
                for p_time, p_price in ch_data['plans'].items():
                    label = f"{p_time} Min" if int(p_time) < 60 else f"{int(p_time)//1440} Days"
                    markup.add(InlineKeyboardButton(f"💳 {label} - ₹{p_price}", callback_data=f"select_{ch_id}_{p_time}"))
                markup.add(InlineKeyboardButton("📞 Contact Admin", url=f"https://t.me/{CONTACT_USERNAME}"))
                bot.send_message(message.chat.id,
                    f"Welcome!\n\nYou are joining: *{ch_data['name']}*.\n\nPlease select a subscription plan below:",
                    reply_markup=markup, parse_mode="Markdown")
                return
        except Exception:
            pass

    if user_id == ADMIN_ID:
        bot.send_message(message.chat.id, "✅ Admin Panel Active!\n\n/add - Add/Edit Channel & Prices\n/channels - Manage Existing Channels")
    else:
        bot.send_message(message.chat.id, "Welcome! To join a channel, please use the link provided by the Admin.")

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

# --- USER: PAYMENT FLOW ---

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
    markup.add(InlineKeyboardButton("🔙 Choose Another Plan", callback_data=f"back_{ch_id}"))
    markup.add(InlineKeyboardButton("📞 Contact Admin", url=f"https://t.me/{CONTACT_USERNAME}"))

    bot.send_photo(call.message.chat.id, qr_url,
        caption=(f"Plan: {mins} Minutes\n"
                 f"Price: ₹{price}\n"
                 f"UPI ID: `{UPI_ID}`\n\n"
                 f"⚠️ *Please pay exactly ₹{unique_amount}* — this unique amount helps us verify your payment automatically.\n\n"
                 f"After paying tap 'I Have Paid' below."),
        reply_markup=markup, parse_mode="Markdown")


@bot.callback_query_handler(func=lambda call: call.data.startswith('back_'))
def go_back_to_plans(call):
    ch_id = int(call.data.split('_')[1])
    ch_data = channels_col.find_one({"channel_id": ch_id})
    bot.answer_callback_query(call.id)

    to_remove = [k for k, v in pending_payments.items() if v['user_id'] == call.from_user.id]
    for k in to_remove:
        pending_payments.pop(k, None)

    markup = InlineKeyboardMarkup()
    for p_time, p_price in ch_data['plans'].items():
        label = f"{p_time} Min" if int(p_time) < 60 else f"{int(p_time)//1440} Days"
        markup.add(InlineKeyboardButton(f"💳 {label} - ₹{p_price}", callback_data=f"select_{ch_id}_{p_time}"))
    markup.add(InlineKeyboardButton("📞 Contact Admin", url=f"https://t.me/{CONTACT_USERNAME}"))

    bot.send_message(call.message.chat.id,
        f"You are joining: *{ch_data['name']}*.\n\nPlease select a subscription plan below:",
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

    markup = InlineKeyboardMarkup()
    for p_time, p_price in ch_data['plans'].items():
        label = f"{p_time} Min" if int(p_time) < 60 else f"{int(p_time)//1440} Days"
        markup.add(InlineKeyboardButton(f"💳 {label} - ₹{p_price}", callback_data=f"select_{ch_id}_{p_time}"))
    markup.add(InlineKeyboardButton("📞 Contact Admin", url=f"https://t.me/{CONTACT_USERNAME}"))

    bot.send_message(call.message.chat.id,
        f"You are joining: *{ch_data['name']}*.\n\nPlease select a subscription plan below:",
        reply_markup=markup, parse_mode="Markdown")


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
        "👨‍💼 If not → admin will review your screenshot and approve manually.\n\n"
        "Either way you're covered ❤️",
        parse_mode="Markdown")


# --- MANUAL APPROVAL (fallback) ---

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
            pending_payments.pop(amount_key, None)

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
            markup = InlineKeyboardMarkup().add(
                InlineKeyboardButton("🔄 Re-join / Renew", url=rejoin_url))
            bot.send_message(user['user_id'],
                "⚠️ Your subscription has expired.\n\nTo join again or renew, please click the button below:",
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
