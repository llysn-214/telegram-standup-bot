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

BOT_TOKEN = os.environ.get("BOT_TOKEN")

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
    # Let user pick topic (or skip)
    keyboard = [
        [InlineKeyboardButton("No topic (main chat)", callback_data="topic_none")],
        [InlineKeyboardButton("Enter topic ID manually", callback_data="topic_manual")]
    ]
    await query.edit_message_text("Select topic (or skip):", reply_markup=InlineKeyboardMarkup(keyboard))
    return CHOOSE_TOPIC

async def choose_topic(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    if query.data == "topic_none":
        context.user_data['topic_id'] = None
        return await ask_time(update, context)
    else:
        await query.edit_message_text("Please reply with the **topic/thread ID** (send as a number).")
        return CHOOSE_TOPIC

async def set_topic_id(update: Update, context: ContextTypes.DEFAULT_TYPE):
    topic_id = update.message.text.strip()
    if not topic_id.isdigit():
        await update.message.reply_text("Invalid topic ID. Please send a number.")
        return CHOOSE_TOPIC
    context.user_data['topic_id'] = int(topic_id)
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
    await query.answer()
    data = query.data.replace("time_", "")
    if data == "manual":
        await query.edit_message_text("Reply with date and time in `YYYY-MM-DD HH:MM` format (24-hour clock).", parse_mode='Markdown')
        return CHOOSE_TIME
    else:
        context.user_data['run_at'] = data
        await query.edit_message_text(f"Time set to: {data}\nNow, please send your message.\n\nYou can mention users with @username and add links, emojis, etc.")
        return WRITE_MSG

async def set_manual_time(update: Update, context: ContextTypes.DEFAULT_TYPE):
    txt = update.message.text.strip()
    try:
        _ = datetime.strptime(txt, "%Y-%m-%d %H:%M")
        context.user_data['run_at'] = txt
        await update.message.reply_text("Time set! Now, please send your message text.\n(You can tag users or add links.)")
        return WRITE_MSG
    except Exception:
        await update.message.reply_text("Invalid format. Please use YYYY-MM-DD HH:MM (24hr).")
        return CHOOSE_TIME

async def write_msg(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data['message'] = update.message.text
    group = context.user_data.get('target_chat_id')
    topic = context.user_data.get('topic_id')
    run_at = context.user_data.get('run_at')
    msg = f"Ready to schedule:\nGroup: `{group}`\n"
    if topic:
        msg += f"Topic ID: `{topic}`\n"
    msg += f"Time: `{run_at}`\nMessage:\n\n{context.user_data['message']}\n\nConfirm?"
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
        run_at_dt = datetime.strptime(run_at, "%Y-%m-%d %H:%M")

        cur.execute(
            "INSERT INTO schedules (target_chat_id, topic_id, user_id, message, run_at) VALUES (?, ?, ?, ?, ?)",
            (group, topic, user_id, message, run_at_dt.isoformat())
        )
        conn.commit()
        schedule_id = cur.lastrowid

        scheduler.add_job(
            post_scheduled_message,
            'date',
            run_date=run_at_dt,
            args=[group, topic, message, schedule_id],
            id=str(schedule_id)
        )
        await query.edit_message_text("✅ Scheduled!")
    else:
        await query.edit_message_text("Cancelled.")
    context.user_data.clear()
    return ConversationHandler.END

# --- Group registration (as before) ---
async def register_chat_on_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_chat.type in ['group', 'supergroup']:
        register_group(update.effective_chat)

# --- /whereami as before ---
async def whereami(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    msg_thread_id = update.message.message_thread_id if update.message and update.message.message_thread_id else None
    msg = f"Chat ID: `{chat_id}`"
    if msg_thread_id:
        msg += f"\nTopic (Thread) ID: `{msg_thread_id}`"
    else:
        msg += "\n(Not in a topic/thread right now.)"
    await update.message.reply_text(msg, parse_mode='Markdown')

def main():
    app_ = ApplicationBuilder().token(BOT_TOKEN).build()
    # --- Button-driven scheduler flow ---
    conv_handler = ConversationHandler(
        entry_points=[CommandHandler("start", start), CallbackQueryHandler(schedule_start, pattern="^schedule_start$")],
        states={
            CHOOSE_GROUP: [CallbackQueryHandler(choose_group, pattern="^group_")],
            CHOOSE_TOPIC: [
                CallbackQueryHandler(choose_topic, pattern="^topic_"),
                MessageHandler(filters.TEXT & ~filters.COMMAND, set_topic_id)
            ],
            CHOOSE_TIME: [
                CallbackQueryHandler(choose_time, pattern="^time_"),
                MessageHandler(filters.TEXT & ~filters.COMMAND, set_manual_time)
            ],
            WRITE_MSG: [MessageHandler(filters.TEXT & ~filters.COMMAND, write_msg)],
            CONFIRM: [CallbackQueryHandler(confirm, pattern="^confirm_")]
        },
        fallbacks=[],
        allow_reentry=True
    )
    app_.add_handler(conv_handler)
    app_.add_handler(CommandHandler("whereami", whereami))
    app_.add_handler(MessageHandler(filters.ALL, register_chat_on_message))
    logger.info("Bot running...")
    app_.run_polling()

if __name__ == "__main__":
    main()
