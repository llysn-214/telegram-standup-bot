import logging
import sqlite3
import os
from datetime import datetime, timedelta
import pytz
from apscheduler.schedulers.background import BackgroundScheduler
from telegram import Update, InlineKeyboardMarkup, InlineKeyboardButton
from telegram.ext import (
    ApplicationBuilder, CommandHandler, ContextTypes, CallbackQueryHandler,
    MessageHandler, filters, ConversationHandler
)
from flask import Flask
import threading

BOT_TOKEN = os.environ.get("BOT_TOKEN")
PORT = int(os.environ.get('PORT', 10000))
PH_TZ = pytz.timezone('Asia/Manila')

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# DB setup (now includes topics table)
conn = sqlite3.connect('schedules.db', check_same_thread=False)
cur = conn.cursor()
cur.execute('''CREATE TABLE IF NOT EXISTS schedules (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    target_chat_id INTEGER,
    topic_id INTEGER,
    user_id INTEGER,
    message TEXT,
    run_at TEXT
)''')
cur.execute('''CREATE TABLE IF NOT EXISTS groups (
    chat_id INTEGER PRIMARY KEY,
    title TEXT
)''')
cur.execute('''CREATE TABLE IF NOT EXISTS topics (
    chat_id INTEGER,
    topic_id INTEGER,
    topic_name TEXT,
    PRIMARY KEY (chat_id, topic_id)
)''')
conn.commit()

scheduler = BackgroundScheduler()
scheduler.start()

# --- Flask HTTP server for Render/UptimeRobot ---
app_flask = Flask(__name__)

@app_flask.route("/")
def home():
    return "OK", 200

def run_flask():
    app_flask.run(host="0.0.0.0", port=PORT)

# --- Conversation states ---
CHOOSE_GROUP, CHOOSE_TOPIC, CHOOSE_TIME, WRITE_MSG, CONFIRM = range(5)

def register_group(chat):
    try:
        cur.execute("INSERT OR REPLACE INTO groups (chat_id, title) VALUES (?, ?)", (chat.id, chat.title or "Unnamed"))
        conn.commit()
    except Exception as e:
        logger.error(f"Failed to register group: {e}")

def register_topic(chat_id, topic_id, topic_name):
    try:
        cur.execute(
            "INSERT OR REPLACE INTO topics (chat_id, topic_id, topic_name) VALUES (?, ?, ?)",
            (chat_id, topic_id or 0, topic_name or "Main chat")
        )
        conn.commit()
    except Exception as e:
        logger.error(f"Failed to register topic: {e}")

def post_scheduled_message(target_chat_id, topic_id, message, schedule_id):
    try:
        app_ = ApplicationBuilder().token(BOT_TOKEN).build()
        logger.info(f"Posting scheduled message: {message} to chat {target_chat_id}, topic {topic_id}")
        if topic_id:
            app_.bot.send_message(chat_id=target_chat_id, text=message, message_thread_id=int(topic_id), parse_mode='MarkdownV2', disable_web_page_preview=False)
        else:
            app_.bot.send_message(chat_id=target_chat_id, text=message, parse_mode='MarkdownV2', disable_web_page_preview=False)
        cur.execute("DELETE FROM schedules WHERE id=?", (schedule_id,))
        conn.commit()
    except Exception as e:
        logger.error(f"Error posting scheduled message: {e}")

# --- Button-driven scheduling ---
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    keyboard = [[InlineKeyboardButton("📅 Schedule Message", callback_data="schedule_start")]]
    await update.message.reply_text("Welcome! What do you want to do?", reply_markup=InlineKeyboardMarkup(keyboard))

async def schedule_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    # List available groups
    cur.execute("SELECT chat_id, title FROM groups")
    groups = cur.fetchall()
    if not groups:
        await query.edit_message_text("No groups registered yet. Add me to groups as admin and use any command there first!")
        return ConversationHandler.END
    keyboard = [
        [InlineKeyboardButton(f"{title}", callback_data=f"group_{chat_id}")]
        for chat_id, title in groups
    ]
    await query.edit_message_text("Which group?", reply_markup=InlineKeyboardMarkup(keyboard))
    return CHOOSE_GROUP

async def choose_group(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    group_id = int(query.data.split("_")[1])
    context.user_data['target_chat_id'] = group_id

    # Fetch topics seen in this group
    cur.execute("SELECT topic_id, topic_name FROM topics WHERE chat_id = ?", (group_id,))
    topics = cur.fetchall()
    if not topics:
        keyboard = [
            [InlineKeyboardButton("Main chat (no topic)", callback_data="topic_0")]
        ]
        await query.edit_message_text(
            "No topics found yet for this group. The bot will learn topics as it sees them. For now, you can only use Main chat.",
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
        return CHOOSE_TOPIC

    keyboard = [
        [InlineKeyboardButton(topic_name, callback_data=f"topic_{topic_id}")]
        for topic_id, topic_name in topics
    ]
    await query.edit_message_text("Choose a topic (or main chat):", reply_markup=InlineKeyboardMarkup(keyboard))
    return CHOOSE_TOPIC

async def choose_topic(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    topic_id = int(query.data.split("_")[1])
    context.user_data['topic_id'] = topic_id if topic_id != 0 else None
    return await ask_time(update, context)

async def ask_time(update: Update, context: ContextTypes.DEFAULT_TYPE):
    now = datetime.now(PH_TZ)
    presets = [
        ("In 5 min", now + timedelta(minutes=5)),
        ("In 15 min", now + timedelta(minutes=15)),
        ("Tomorrow 9AM", (now + timedelta(days=1)).replace(hour=9, minute=0, second=0, microsecond=0)),
        ("Manual entry", None)
    ]
    keyboard = [
        [InlineKeyboardButton(label, callback_data=f"time_{dt.strftime('%Y-%m-%d %H:%M') if dt else 'manual'}")]
        for label, dt in presets
    ]
    msg = "Pick a time (Asia/Manila):"
    if getattr(update, "callback_query", None):
        await update.callback_query.edit_message_text(msg, reply_markup=InlineKeyboardMarkup(keyboard))
    else:
        await update.message.reply_text(msg, reply_markup=InlineKeyboardMarkup(keyboard))
    return CHOOSE_TIME

async def choose_time(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    data = query.data.replace("time_", "")
    if data == "manual":
        await query.edit_message_text("Reply with date and time in `YYYY-MM-DD HH:MM` format (24-hour, Asia/Manila).\n\nType /cancel to abort.", parse_mode='Markdown')
        return CHOOSE_TIME
    else:
        context.user_data['run_at'] = data
        await query.edit_message_text(f"Time set to: {data} (Asia/Manila)\nNow, please send your message.\n\nYou can mention users with @username and add links, emojis, etc.")
        return WRITE_MSG

async def set_manual_time(update: Update, context: ContextTypes.DEFAULT_TYPE):
    txt = update.message.text.strip()
    try:
        dt = datetime.strptime(txt, "%Y-%m-%d %H:%M")
        dt = PH_TZ.localize(dt)
        context.user_data['run_at'] = dt.strftime('%Y-%m-%d %H:%M')
        await update.message.reply_text("Time set! Now, please send your message text.\n(You can tag users or add links.)")
        return WRITE_MSG
    except Exception:
        await update.message.reply_text("Invalid format. Please use YYYY-MM-DD HH:MM (24hr), or type /cancel.")
        return CHOOSE_TIME

async def write_msg(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data['message'] = update.message.text
    group = context.user_data.get('target_chat_id')
    topic = context.user_data.get('topic_id')
    run_at = context.user_data.get('run_at')
    msg = f"Ready to schedule:\nGroup: `{group}`\n"
    if topic:
        cur.execute("SELECT topic_name FROM topics WHERE chat_id = ? AND topic_id = ?", (group, topic))
        tname = cur.fetchone()
        if tname:
            msg += f"Topic: `{tname[0]}`\n"
        else:
            msg += f"Topic ID: `{topic}`\n"
    msg += f"Time: `{run_at}` (Asia/Manila)\nMessage:\n\n{context.user_data['message']}\n\nConfirm?"
    keyboard = [
        [InlineKeyboardButton("✅ Confirm", callback_data="confirm_yes")],
        [InlineKeyboardButton("❌ Cancel", callback_data="confirm_no")]
    ]
    await update.message.reply_text(msg, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')
    return CONFIRM

async def confirm(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    if query.data == "confirm_yes":
        group = context.user_data['target_chat_id']
        topic = context.user_data.get('topic_id')
        run_at = context.user_data['run_at']
        message = context.user_data['message']
        user_id = query.from_user.id
        # Convert PH time to UTC for APScheduler (if needed)
        dt_ph = PH_TZ.localize(datetime.strptime(run_at, "%Y-%m-%d %H:%M"))
        dt_utc = dt_ph.astimezone(pytz.utc)

        cur.execute(
            "INSERT INTO schedules (target_chat_id, topic_id, user_id, message, run_at) VALUES (?, ?, ?, ?, ?)",
            (group, topic, user_id, message, dt_utc.isoformat())
        )
        conn.commit()
        schedule_id = cur.lastrowid

        scheduler.add_job(
            post_scheduled_message,
            'date',
            run_date=dt_utc,
            args=[group, topic, message, schedule_id],
            id=str(schedule_id)
        )
        await query.edit_message_text("✅ Scheduled!")
    else:
        await query.edit_message_text("Cancelled.")
    context.user_data.clear()
    return ConversationHandler.END

async def cancel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("Scheduling cancelled.")
    context.user_data.clear()
    return ConversationHandler.END

# --- Self-learning: Register topics on ANY message seen in a group or topic
async def register_chat_on_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_chat.type in ['group', 'supergroup']:
        register_group(update.effective_chat)
        topic_id = getattr(update.message, "message_thread_id", None)
        topic_name = None
        if topic_id:
            # Use the topic name if available (fallback to ID string)
            # For python-telegram-bot, topic name is only available on certain updates
            # We'll try to get it from the message's reply_to_message, otherwise just store the ID
            topic_name = None
            if hasattr(update.message, "forum_topic_created") and update.message.forum_topic_created:
                topic_name = update.message.forum_topic_created.name
            if not topic_name:
                # fallback to ID string
                topic_name = f"Topic {topic_id}"
        else:
            topic_id = 0
            topic_name = "Main chat"
        register_topic(update.effective_chat.id, topic_id, topic_name)

async def whereami(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    msg_thread_id = update.message.message_thread_id if update.message and update.message.message_thread_id else None
    msg = f"Chat ID: `{chat_id}`"
    if msg_thread_id:
        msg += f"\nTopic (Thread) ID: `{msg_thread_id}`"
    else:
        msg += "\n(Not in a topic/thread right now.)"
    await update.message.reply_text(msg, parse_mode='Markdown')
    
async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    help_text = (
        "*Available Commands:*\n\n"
        "/start – Begin scheduling a message with guided buttons\n"
        "/help – Show this help message\n"
        "/whereami – Show the group and topic/thread ID (useful for admins and troubleshooting)\n"
        "/cancel – Cancel the current operation (only when in scheduling flow)\n\n"
        "*How to register groups/topics:*\n"
        "- Add the bot as admin to your group (with permission to read messages)\n"
        "- Send any message in the main chat and in each topic; the bot will 'learn' all topics it sees\n\n"
        "*How to schedule:*\n"
        "1. DM or /start the bot\n"
        "2. Tap 'Schedule Message' and follow the button prompts\n"
        "3. Select group, topic, time (Asia/Manila), and type your message\n"
        "4. Confirm to schedule\n"
    )
    await update.message.reply_text(help_text, parse_mode="Markdown")

def run_telegram_bot():
    app_ = ApplicationBuilder().token(BOT_TOKEN).build()
    conv_handler = ConversationHandler(
        entry_points=[CommandHandler("start", start), CallbackQueryHandler(schedule_start, pattern="^schedule_start$")],
        states={
            CHOOSE_GROUP: [CallbackQueryHandler(choose_group, pattern="^group_")],
            CHOOSE_TOPIC: [CallbackQueryHandler(choose_topic, pattern="^topic_")],
            CHOOSE_TIME: [
                CallbackQueryHandler(choose_time, pattern="^time_"),
                MessageHandler(filters.TEXT & ~filters.COMMAND, set_manual_time)
            ],
            WRITE_MSG: [MessageHandler(filters.TEXT & ~filters.COMMAND, write_msg)],
            CONFIRM: [CallbackQueryHandler(confirm, pattern="^confirm_")]
        },
        fallbacks=[CommandHandler('cancel', cancel)],
        allow_reentry=True
    )
    app_.add_handler(conv_handler)
    app_.add_handler(CommandHandler("help", help_command))
    app_.add_handler(CommandHandler("whereami", whereami))
    app_.add_handler(MessageHandler(filters.ALL, register_chat_on_message))
    logger.info("Bot running...")
    app_.run_polling()

if __name__ == "__main__":
    threading.Thread(target=run_flask).start()
    run_telegram_bot()
