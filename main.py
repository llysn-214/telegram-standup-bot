import logging
import sqlite3
import os
from datetime import datetime, timedelta
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

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# DB setup
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

    # --- Fetch topics in the group (forum supergroups only) ---
    app_ = ApplicationBuilder().token(BOT_TOKEN).build()
    try:
        # Only works in forum-enabled groups!
        forum_topics = await app_.bot.get_forum_topic_list(group_id)
        keyboard = [
            [InlineKeyboardButton(topic["name"], callback_data=f"topic_{topic['message_thread_id']}")]
            for topic in forum_topics["topics"]
        ]
        keyboard.insert(0, [InlineKeyboardButton("Main chat (no topic)", callback_data="topic_none")])
        await query.edit_message_text("Choose a topic:", reply_markup=InlineKeyboardMarkup(keyboard))
    except Exception as e:
        # If not a forum group, just allow main chat
        logger.info(f"No topics or not a forum group: {e}")
        keyboard = [
            [InlineKeyboardButton("Main chat (no topic)", callback_data="topic_none")]
        ]
        await query.edit_message_text("No topics found or not a forum group. Using main chat.", reply_markup=InlineKeyboardMarkup(keyboard))
    return CHOOSE_TOPIC

async def choose_topic(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    if query.data == "topic_none":
        context.user_data['topic_id'] = None
    else:
        context.user_data['topic_id'] = int(query.data.split("_")[1])
    return await ask_time(update, context)

async def ask_time(update: Update, context: ContextTypes.DEFAULT_TYPE):
    now = datetime.now()
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
    msg = "Pick a time:"
    if hasattr(update, "callback_query"):
        await update.callback_query.edit_message_text(msg, reply_markup=InlineKeyboardMarkup(keyboard))
    else:
        await update.message.reply_text(msg, reply_markup=InlineKeyboardMarkup(keyboard))
    return CHOOSE_TIME

async def choose_time(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.
