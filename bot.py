import os
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

# --- CONFIGURATION (Environment Variables) ---
BOT_TOKEN = os.getenv('BOT_TOKEN')
MONGO_URI = os.getenv('MONGO_URI')
ADMIN_ID = int(os.getenv('ADMIN_ID'))
UPI_ID = os.getenv('UPI_ID')
CONTACT_USERNAME = os.getenv('CONTACT_USERNAME')

bot = telebot.TeleBot(BOT_TOKEN)
client = MongoClient(MONGO_URI)
db = client['sub_management']
channels_col = db['channels']
users_col = db['users']

# In-memory store for pending payment proofs (cleared if bot restarts — that's fine,
# user just taps "I Have Paid" again)
pending_payments = {}  # { user_id: {"ch_id": int, "mins": str, "price": str} }

# --- ADMIN LOGIC ---

@bot.message_handler(commands=['start'])
def start_handler(message):
    user_id = message.from_user.id
    text = message.text.split()

    # User entry via Deep Link
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
        msg = bot.send_message(ADMIN_ID, "❌ Please send a valid number (e.g. 2). How many plans do you want to add?")
        bot.register_next_step_handler(msg, ask_plan_count, ch_id, ch_name)
        return

    plan_state = {
        "ch_id": ch_id,
        "ch_name": ch_name,
        "total": int(text),
        "current": 1,
        "plans": {}
    }
    ask_minutes(message, plan_state)

def ask_minutes(message, plan_state):
    msg = bot.send_message(ADMIN_ID,
        f"Plan {plan_state['current']}/{plan_state['total']}\n\nEnter duration in MINUTES (e.g. 1440 for 1 Day, 43200 for 30 Days):")
    bot.register_next_step_handler(msg, save_minutes, plan_state)

def save_minutes(message, plan_state):
    text = message.text.strip()
    if not text.isdigit() or int(text) < 1:
        msg = bot.send_message(ADMIN_ID, "❌ Please send a valid number of minutes (numbers only, e.g. 1440).")
        bot.register_next_step_handler(msg, save_minutes, plan_state)
        return

    plan_state["temp_minutes"] = text
    msg = bot.send_message(ADMIN_ID, f"Got it: {text} minutes.\n\nNow enter the PRICE for this plan (numbers only, e.g. 99):")
    bot.register_next_step_handler(msg, save_price, plan_state)

def save_price(message, plan_state):
    text = message.text.strip()
    if not text.isdigit() or int(text) < 1:
        msg = bot.send_message(ADMIN_ID, "❌ Please send a valid price (numbers only, e.g. 99).")
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

    qr_url = f"https://api.qrserver.com/v1/create-qr-code/?size=300x300&data=upi://pay?pa={UPI_ID}%26am={price}%26cu=INR"

    markup = InlineKeyboardMarkup()
    markup.add(InlineKeyboardButton("✅ I Have Paid", callback_data=f"paid_{ch_id}_{mins}"))
    markup.add(InlineKeyboardButton("📞 Contact Admin", url=f"https://t.me/{CONTACT_USERNAME}"))

    bot.send_photo(call.message.chat.id, qr_url,
                   caption=f"Plan: {mins} Minutes\nPrice: ₹{price}\nUPI ID: `{UPI_ID}`\n\nPlease complete the payment and click 'I Have Paid'.",
                   reply_markup=markup, parse_mode="Markdown")

@bot.callback_query_handler(func=lambda call: call.data.startswith('paid_'))
def request_proof(call):
    _, ch_id, mins = call.data.split('_')
    user = call.from_user
    ch_data = channels_col.find_one({"channel_id": int(ch_id)})
    price = ch_data['plans'][mins]

    pending_payments[user.id] = {"ch_id": int(ch_id), "mins": mins, "price": price}

    bot.answer_callback_query(call.id)

    markup = InlineKeyboardMarkup()
    markup.add(InlineKeyboardButton("🔙 Cancel / Choose Another Plan", callback_data=f"cancelproof_{ch_id}"))

    msg = bot.send_message(call.message.chat.id,
        "📸 Please send a screenshot of your payment as proof.\n\nJust send the photo here — it'll be forwarded to the admin for verification.",
        reply_markup=markup)
    bot.register_next_step_handler(msg, receive_proof)


@bot.callback_query_handler(func=lambda call: call.data.startswith('cancelproof_'))
def cancel_proof(call):
    user = call.from_user
    ch_id = int(call.data.split('_')[1])

    pending_payments.pop(user.id, None)
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


def receive_proof(message):
    user = message.from_user

    if user.id not in pending_payments:
        bot.send_message(message.chat.id, "⚠️ This payment request was cancelled or expired. Please select a plan again using the link.")
        return

    if not message.photo:
        markup = InlineKeyboardMarkup()
        markup.add(InlineKeyboardButton("🔙 Cancel / Choose Another Plan", callback_data=f"cancelproof_{pending_payments[user.id]['ch_id']}"))
        msg = bot.send_message(message.chat.id, "❌ That doesn't look like a photo. Please send a screenshot image of your payment.", reply_markup=markup)
        bot.register_next_step_handler(msg, receive_proof)
        return

    data = pending_payments.pop(user.id)
    ch_data = channels_col.find_one({"channel_id": data["ch_id"]})
    photo_file_id = message.photo[-1].file_id

    markup = InlineKeyboardMarkup()
    markup.add(InlineKeyboardButton("✅ Approve", callback_data=f"app_{user.id}_{data['ch_id']}_{data['mins']}"))
    markup.add(InlineKeyboardButton("❌ Reject", callback_data=f"rej_{user.id}"))

    caption = (f"🔔 *Payment Verification Required!*\n\n"
               f"User: {user.first_name} (`{user.id}`)\n"
               f"Channel: {ch_data['name']}\n"
               f"Plan: {data['mins']} Mins\n"
               f"Price: ₹{data['price']}")

    bot.send_photo(ADMIN_ID, photo_file_id, caption=caption, reply_markup=markup, parse_mode="Markdown")

    u_markup = InlineKeyboardMarkup().add(InlineKeyboardButton("📞 Contact Admin", url=f"https://t.me/{CONTACT_USERNAME}"))
    bot.send_message(message.chat.id, "✅ Your payment proof has been sent. Please wait for Admin approval.", reply_markup=u_markup)

# --- APPROVAL & EXPIRY ---

@bot.callback_query_handler(func=lambda call: call.data.startswith('app_'))
def approve_now(call):
    _, u_id, ch_id, mins = call.data.split('_')
    u_id, ch_id, mins = int(u_id), int(ch_id), int(mins)

    try:
        expiry_datetime = datetime.now() + timedelta(minutes=mins)
        expiry_ts = int(expiry_datetime.timestamp())

        link = bot.create_chat_invite_link(ch_id, member_limit=1, expire_date=expiry_ts)

        users_col.update_one({"user_id": u_id, "channel_id": ch_id}, {"$set": {"expiry": expiry_datetime.timestamp()}}, upsert=True)

        bot.send_message(u_id, f"🥳 *Payment Approved!*\n\nSubscription: {mins} Minutes\n\nJoin Link: {link.invite_link}\n\n⚠️ Note: This link and your access will expire in {mins} minutes.", parse_mode="Markdown")
        bot.edit_message_caption(f"✅ Approved user {u_id} for {mins} mins.", call.message.chat.id, call.message.message_id)

    except Exception as e:
        bot.send_message(ADMIN_ID, f"❌ Error: {e}")

@bot.callback_query_handler(func=lambda call: call.data.startswith('rej_'))
def reject_payment(call):
    u_id = int(call.data.split('_')[1])
    try:
        bot.send_message(u_id, "❌ Your payment could not be verified. Please contact admin if you believe this is a mistake.",
                          reply_markup=InlineKeyboardMarkup().add(InlineKeyboardButton("📞 Contact Admin", url=f"https://t.me/{CONTACT_USERNAME}")))
    except Exception:
        pass
    bot.edit_message_caption(f"❌ Rejected payment from user {u_id}.", call.message.chat.id, call.message.message_id)

@bot.callback_query_handler(func=lambda call: call.data.startswith('manage_'))
def manage_ch(call):
    ch_id = int(call.data.split('_')[1])
    ch_data = channels_col.find_one({"channel_id": ch_id})
    bot_username = bot.get_me().username
    link = f"https://t.me/{bot_username}?start={ch_id}"

    bot.edit_message_text(f"Settings for: *{ch_data['name']}*\n\nYour Link: `{link}`\n\nTo edit prices, use /add and forward a message from this channel again.",
                          call.message.chat.id, call.message.message_id, parse_mode="Markdown")

# Automate Kicking
def kick_expired_users():
    now = datetime.now().timestamp()
    expired_users = users_col.find({"expiry": {"$lte": now}})
    bot_username = bot.get_me().username

    for user in expired_users:
        try:
            bot.ban_chat_member(user['channel_id'], user['user_id'])
            bot.unban_chat_member(user['channel_id'], user['user_id'])

            rejoin_url = f"https://t.me/{bot_username}?start={user['channel_id']}"
            markup = InlineKeyboardMarkup().add(InlineKeyboardButton("🔄 Re-join / Renew", url=rejoin_url))

            bot.send_message(user['user_id'], "⚠️ Your subscription has expired.\n\nTo join again or renew, please click the button below:", reply_markup=markup)
            users_col.delete_one({"_id": user['_id']})
        except Exception:
            pass

# --- STARTUP ---
if __name__ == '__main__':
    keep_alive()
    scheduler = BackgroundScheduler()
    scheduler.add_job(kick_expired_users, 'interval', minutes=1)
    scheduler.start()
    bot.remove_webhook()
    print("Bot is running...")
    bot.infinity_polling(timeout=20, long_polling_timeout=10)
