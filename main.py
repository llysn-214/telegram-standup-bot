import logging
import sqlite3
import os
import asyncio
import threading
from datetime import datetime, timedelta, time as dt_time
import pytz
from apscheduler.schedulers.background import BackgroundScheduler
from telegram import Update, InlineKeyboardMarkup, InlineKeyboardButton
from telegram.ext import (
    ApplicationBuilder, CommandHandler, ContextTypes, CallbackQueryHandler,
    MessageHandler, filters, ConversationHandler
)
from flask import Flask
from collections import defaultdict, deque

BOT_TOKEN = os.environ.get("BOT_TOKEN")
PORT = int(os.environ.get('PORT', 10000))
PH_TZ = pytz.timezone('Asia/Manila')

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# --- DB setup ---
conn = sqlite3.connect('schedules.db', check_same_thread=False)
cur = conn.cursor()
cur.execute('''CREATE TABLE IF NOT EXISTS schedules (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    target_chat_id INTEGER,
    topic_id INTEGER,
    user_id INTEGER,
    message TEXT,
    run_at TEXT,
    recurrence TEXT DEFAULT 'none',
    recurrence_data TEXT
)''')
# Migration if needed:
try:
    cur.execute("ALTER TABLE schedules ADD COLUMN recurrence TEXT DEFAULT 'none'")
except sqlite3.OperationalError:
    pass
try:
    cur.execute("ALTER TABLE schedules ADD COLUMN recurrence_data TEXT")
except sqlite3.OperationalError:
    pass

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

# --- Flood control queue ---
# For every scheduled second, a queue of jobs (FIFO), key: run_at string
scheduled_send_queues = defaultdict(deque)
scheduled_queue_locks = defaultdict(threading.Lock)

def queue_send_message(run_at, send_job):
    """Queue send_job (callable) for the given run_at datetime string (UTC ISO)."""
    key = run_at
    with scheduled_queue_locks[key]:
        scheduled_send_queues[key].append(send_job)
        # Only one sender should run for this second
        if len(scheduled_send_queues[key]) == 1:
            # Start the FIFO sender as a background thread
            threading.Thread(target=fifo_send_runner, args=(key,)).start()

def fifo_send_runner(key):
    """FIFO runner: send each job in queue, 3s apart, with retry, then clear."""
    while True:
        with scheduled_queue_locks[key]:
            if not scheduled_send_queues[key]:
                # Clean up lock and queue for memory
                del scheduled_send_queues[key]
                del scheduled_queue_locks[key]
                return
            send_job = scheduled_send_queues[key][0]
        # Call the send_job (includes its own retry)
        send_job()
        # Wait 3s before next in queue
        asyncio.run(asyncio.sleep(3))
        with scheduled_queue_locks[key]:
            if scheduled_send_queues[key]:
                scheduled_send_queues[key].popleft()

# --- Flask HTTP server for Render/UptimeRobot ---
app_flask = Flask(__name__)

@app_flask.route("/")
def home():
    return "OK", 200

def run_flask():
    app_flask.run(host="0.0.0.0", port=PORT)

# --- Conversation states ---
CHOOSE_GROUP, CHOOSE_TOPIC, CHOOSE_RECURRENCE, CHOOSE_DAY, CHOOSE_TIME, WRITE_MSG, CONFIRM = range(7)

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

def post_scheduled_message(target_chat_id, topic_id, message, schedule_id, recurrence="none"):
    # We queue up the send operation to guarantee FIFO and rate limiting
    cur.execute("SELECT run_at FROM schedules WHERE id=?", (schedule_id,))
    row = cur.fetchone()
    run_at = row[0] if row else datetime.utcnow().isoformat()

    def send_job():
        asyncio.run(_try_send_message_with_retry(target_chat_id, topic_id, message, schedule_id, recurrence))

    queue_send_message(run_at, send_job)

async def _try_send_message_with_retry(target_chat_id, topic_id, message, schedule_id, recurrence, tries=3):
    app_ = ApplicationBuilder().token(BOT_TOKEN).build()
    for attempt in range(1, tries + 1):
        try:
            if topic_id:
                await app_.bot.send_message(chat_id=target_chat_id, text=message, message_thread_id=int(topic_id), parse_mode='HTML', disable_web_page_preview=False)
            else:
                await app_.bot.send_message(chat_id=target_chat_id, text=message, parse_mode='HTML', disable_web_page_preview=False)
            logger.info(f"Sent message for schedule {schedule_id} (try {attempt})")
            if recurrence == "none":
                cur.execute("DELETE FROM schedules WHERE id=?", (schedule_id,))
                conn.commit()
                logger.info(f"One-time message: schedule {schedule_id} deleted from DB.")
            return
        except Exception as e:
            logger.error(f"Error sending schedule {schedule_id}, try {attempt}: {e}")
            if attempt < tries:
                await asyncio.sleep(3)
            else:
                logger.error(f"All retries failed for schedule {schedule_id}. Giving up.")

# --- Button-driven scheduling, unchanged ---
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    keyboard = [[InlineKeyboardButton("📅 Schedule Message", callback_data="schedule_start")]]
    await update.message.reply_text("Welcome! What do you want to do?", reply_markup=InlineKeyboardMarkup(keyboard))

async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    help_text = (
        "*Available Commands:*\n\n"
        "/start – Begin scheduling a message with guided buttons\n"
        "/help – Show this help message\n"
        "/myschedules – List and cancel your future scheduled messages\n"
        "/whereami – Show the group and topic/thread ID (useful for admins and troubleshooting)\n"
        "/cancel – Cancel the current operation (only when in scheduling flow)\n"
        "/topicname [Display Name] – Set a human-friendly name for the current topic (use in topic)\n\n"
        "*How to register groups/topics:*\n"
        "- Add the bot as admin to your group (with permission to read messages)\n"
        "- Send any message in the main chat and in each topic; the bot will 'learn' all topics it sees\n"
        "- Use /topicname in a topic to give it a readable name\n\n"
        "*How to schedule:*\n"
        "1. DM or /start the bot\n"
        "2. Tap 'Schedule Message' and follow the button prompts\n"
        "3. Select group, topic, recurrence, time (Asia/Manila), and type your message\n"
        "4. Confirm to schedule\n"
    )
    await update.message.reply_text(help_text, parse_mode="Markdown")

async def set_topic_name(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_chat.type not in ['group', 'supergroup']:
        await update.message.reply_text("This command must be used in a group/topic.")
        return
    topic_id = getattr(update.message, "message_thread_id", None)
    if topic_id is None:
        await update.message.reply_text("This command must be used *inside a topic* (not main chat).")
        return
    name = " ".join(context.args).strip()
    if not name:
        await update.message.reply_text("Usage: /topicname Actual Topic Name")
        return
    register_topic(update.effective_chat.id, topic_id, name)
    await update.message.reply_text(f"Topic name for ID {topic_id} set to: {name}")

# ... (THE REST OF THE CODE IS UNCHANGED from the last working version you had—see previous main.py for all the button-driven scheduling, recurring jobs, /myschedules, etc.)
# To avoid message limits, let me know if you want the complete working version pasted again, or if you only want to see flood control and retry upgrades for your copy-paste.

# For clarity: replace your old post_scheduled_message and async send logic with the ones above, add the FIFO queue code at the top, and everything else (all handlers, conversation logic, etc.) can stay the same.

def run_telegram_bot():
    app_ = ApplicationBuilder().token(BOT_TOKEN).build()
    # ... (handlers from your current working main.py)
    logger.info("Bot running...")
    app_.run_polling()

if __name__ == "__main__":
    threading.Thread(target=run_flask).start()
    run_telegram_bot()
