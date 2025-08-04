import logging
import sqlite3
import os
from datetime import datetime, timedelta, time as dt_time
import pytz
import asyncio
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

# --- DB setup (add recurrence columns if missing) ---
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
# If your table existed from before, ensure columns exist:
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

def send_scheduled_message(target_chat_id, topic_id, message):
    async def send():
        app_ = ApplicationBuilder().token(BOT_TOKEN).build()
        try:
            if topic_id:
                await app_.bot.send_message(chat_id=target_chat_id, text=message, message_thread_id=int(topic_id), parse_mode='HTML', disable_web_page_preview=False)
            else:
                await app_.bot.send_message(chat_id=target_chat_id, text=message, parse_mode='HTML', disable_web_page_preview=False)
        except Exception as e:
            logger.exception(f"Error posting scheduled message: {e}")
    asyncio.run(send())

def post_scheduled_message(target_chat_id, topic_id, message, schedule_id, recurrence="none"):
    logger.info(f"Trying to send: chat_id={target_chat_id}, topic_id={topic_id}, message='{message}', schedule_id={schedule_id}")
    send_scheduled_message(target_chat_id, topic_id, message)
    # For one-time, delete after send. For recurring, do not delete.
    if recurrence == "none":
        cur.execute("DELETE FROM schedules WHERE id=?", (schedule_id,))
        conn.commit()
        logger.info(f"Message sent and schedule {schedule_id} deleted.")

# --- Button-driven scheduling ---
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

# --- /myschedules and Cancel button
async def myschedules(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    cur.execute(
        "SELECT id, target_chat_id, topic_id, message, run_at, recurrence, recurrence_data FROM schedules WHERE user_id = ? ORDER BY run_at ASC",
        (user_id,)
    )
    schedules = cur.fetchall()
    if not schedules:
        await update.message.reply_text("You have no scheduled messages.")
        return

    for sched in schedules:
        schedule_id, chat_id, topic_id, msg, run_at, recurrence, recurrence_data = sched
        # Get group name
        cur.execute("SELECT title FROM groups WHERE chat_id = ?", (chat_id,))
        group_row = cur.fetchone()
        group_name = group_row[0] if group_row else str(chat_id)
        # Get topic name
        tname = None
        if topic_id:
            cur.execute("SELECT topic_name FROM topics WHERE chat_id = ? AND topic_id = ?", (chat_id, topic_id))
            trow = cur.fetchone()
            tname = trow[0] if trow else f"Topic {topic_id}"
        # Display time in PH
        dt = datetime.fromisoformat(run_at)
        dt_ph = dt.astimezone(PH_TZ)
        preview = msg[:60].replace('\n', ' ') + ("..." if len(msg) > 60 else "")
        text = f"<b>Group:</b> {group_name}\n"
        text += f"<b>Topic:</b> {tname if tname else 'Main chat'}\n"
        if recurrence == "weekly":
            weekday, at_time = recurrence_data.split(":")
            text += f"<b>Repeats:</b> Every {weekday} {at_time} Asia/Manila\n"
        else:
            text += f"<b>When:</b> {dt_ph.strftime('%Y-%m-%d %H:%M')} Asia/Manila\n"
        text += f"<b>Message:</b> <code>{preview}</code>"

        keyboard = [[InlineKeyboardButton("❌ Cancel", callback_data=f"cancel_{schedule_id}")]]
        await update.message.reply_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="HTML")

async def cancel_schedule(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    data = query.data
    if not data.startswith("cancel_"):
        return
    schedule_id = int(data.split("_")[1])
    # Remove job from APScheduler
    try:
        scheduler.remove_job(str(schedule_id))
    except Exception:
        pass  # Job may have already executed/been removed
    # Remove from DB
    cur.execute("DELETE FROM schedules WHERE id = ?", (schedule_id,))
    conn.commit()
    await query.edit_message_text("❌ Scheduled message cancelled.")

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

    # Choose recurrence type
    weekdays = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
    keyboard = [
        [InlineKeyboardButton("One time only", callback_data="recurr_none")]
    ] + [
        [InlineKeyboardButton(f"Repeat every {day}", callback_data=f"recurr_weekly_{day}")]
        for day in weekdays
    ]
    await query.edit_message_text(
        "Do you want this message to repeat?\n\nSelect a day to repeat weekly or pick 'One time only'.",
        reply_markup=InlineKeyboardMarkup(keyboard)
    )
    return CHOOSE_RECURRENCE

async def choose_recurrence(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    data = query.data
    if data == "recurr_none":
        context.user_data['recurrence'] = "none"
        return await ask_time(update, context)
    elif data.startswith("recurr_weekly_"):
        day = data.split("_")[-1]
        context.user_data['recurrence'] = "weekly"
        context.user_data['weekday'] = day
        await query.edit_message_text(f"Enter the time (24h, Asia/Manila) for every {day}, e.g. 13:00 for 1PM.")
        return CHOOSE_TIME

async def ask_time(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # This is for one-time schedule
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
    query = getattr(update, "callback_query", None)
    if query:
        await query.answer()
        data = query.data.replace("time_", "")
        if data == "manual":
            await query.edit_message_text("Reply with date and time in `YYYY-MM-DD HH:MM` format (24-hour, Asia/Manila).\n\nType /cancel to abort.", parse_mode='Markdown')
            return CHOOSE_TIME
        else:
            context.user_data['run_at'] = data
            await query.edit_message_text(f"Time set to: {data} (Asia/Manila)\nNow, please send your message.\n\nYou can mention users with @username and add links, emojis, etc.")
            return WRITE_MSG
    else:
        # For recurring, this is just HH:MM in Asia/Manila
        txt = update.message.text.strip()
        if context.user_data.get('recurrence', "none") == "weekly":
            # Validate HH:MM
            try:
                dt_time.fromisoformat(txt)
                context.user_data['recurr_time'] = txt
                await update.message.reply_text(f"Time set to {txt}. Now, please send your message text.")
                return WRITE_MSG
            except Exception:
                await update.message.reply_text("Invalid format. Use HH:MM (e.g. 13:00 for 1PM).")
                return CHOOSE_TIME
        else:
            # For one-time manual entry
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
    recurrence = context.user_data.get('recurrence', "none")
    msg = f"Ready to schedule:\nGroup: `{group}`\n"
    if topic:
        cur.execute("SELECT topic_name FROM topics WHERE chat_id = ? AND topic_id = ?", (group, topic))
        tname = cur.fetchone()
        if tname:
            msg += f"Topic: `{tname[0]}`\n"
        else:
            msg += f"Topic ID: `{topic}`\n"

    if recurrence == "weekly":
        weekday = context.user_data['weekday']
        at_time = context.user_data['recurr_time']
        msg += f"Repeats: Every {weekday} at {at_time} (Asia/Manila)\n"
    else:
        run_at = context.user_data.get('run_at')
        msg += f"Time: `{run_at}` (Asia/Manila)\n"
    msg += f"Message:\n\n{context.user_data['message']}\n\nConfirm?"
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
        message = context.user_data['message']
        user_id = query.from_user.id
        recurrence = context.user_data.get('recurrence', "none")

        if recurrence == "weekly":
            weekday = context.user_data['weekday']
            at_time = context.user_data['recurr_time']
            # For DB, store next run as run_at, recurrence="weekly", recurrence_data="Monday:13:00"
            # Schedule using APScheduler's cron (day_of_week=weekday, hour, minute)
            dt_now = datetime.now(PH_TZ)
            week_days = ['Monday','Tuesday','Wednesday','Thursday','Friday','Saturday','Sunday']
            day_idx = week_days.index(weekday)
            # Find next occurrence
            next_dt = dt_now
            while next_dt.weekday() != day_idx or next_dt.time() > dt_time.fromisoformat(at_time):
                next_dt += timedelta(days=1)
            next_dt = next_dt.replace(hour=int(at_time.split(":")[0]), minute=int(at_time.split(":")[1]), second=0, microsecond=0)
            dt_utc = PH_TZ.localize(next_dt).astimezone(pytz.utc)
            # Store in DB
            cur.execute(
                "INSERT INTO schedules (target_chat_id, topic_id, user_id, message, run_at, recurrence, recurrence_data) VALUES (?, ?, ?, ?, ?, ?, ?)",
                (group, topic, user_id, message, dt_utc.isoformat(), "weekly", f"{weekday}:{at_time}")
            )
            conn.commit()
            schedule_id = cur.lastrowid
            # Add APScheduler cron job
            scheduler.add_job(
                post_scheduled_message,
                'cron',
                day_of_week=weekday.lower(),
                hour=int(at_time.split(":")[0]),
                minute=int(at_time.split(":")[1]),
                args=[group, topic, message, schedule_id, "weekly"],
                id=str(schedule_id)
            )
            await query.edit_message_text(f"✅ Weekly recurring message scheduled for every {weekday} at {at_time} (Asia/Manila)!")
        else:
            run_at = context.user_data['run_at']
            dt_ph = PH_TZ.localize(datetime.strptime(run_at, "%Y-%m-%d %H:%M"))
            dt_utc = dt_ph.astimezone(pytz.utc)
            cur.execute(
                "INSERT INTO schedules (target_chat_id, topic_id, user_id, message, run_at, recurrence) VALUES (?, ?, ?, ?, ?, ?)",
                (group, topic, user_id, message, dt_utc.isoformat(), "none")
            )
            conn.commit()
            schedule_id = cur.lastrowid
            scheduler.add_job(
                post_scheduled_message,
                'date',
                run_date=dt_utc,
                args=[group, topic, message, schedule_id, "none"],
                id=str(schedule_id)
            )
            await query.edit_message_text("✅ One-time scheduled message set!")
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
            cur.execute("SELECT topic_name FROM topics WHERE chat_id = ? AND topic_id = ?", (update.effective_chat.id, topic_id))
            row = cur.fetchone()
            if row:
                topic_name = row[0]
            else:
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

def run_telegram_bot():
    app_ = ApplicationBuilder().token(BOT_TOKEN).build()
    conv_handler = ConversationHandler(
        entry_points=[CommandHandler("start", start), CallbackQueryHandler(schedule_start, pattern="^schedule_start$")],
        states={
            CHOOSE_GROUP: [CallbackQueryHandler(choose_group, pattern="^group_")],
            CHOOSE_TOPIC: [CallbackQueryHandler(choose_topic, pattern="^topic_")],
            CHOOSE_RECURRENCE: [CallbackQueryHandler(choose_recurrence, pattern="^recurr_")],
            CHOOSE_TIME: [
                CallbackQueryHandler(choose_time, pattern="^time_"),
                MessageHandler(filters.TEXT & ~filters.COMMAND, choose_time)
            ],
            WRITE_MSG: [MessageHandler(filters.TEXT & ~filters.COMMAND, write_msg)],
            CONFIRM: [CallbackQueryHandler(confirm, pattern="^confirm_")]
        },
        fallbacks=[CommandHandler('cancel', cancel)],
        allow_reentry=True
    )
    app_.add_handler(conv_handler)
    app_.add_handler(CommandHandler("help", help_command))
    app_.add_handler(CommandHandler("myschedules", myschedules))
    app_.add_handler(CommandHandler("whereami", whereami))
    app_.add_handler(CommandHandler("topicname", set_topic_name))
    app_.add_handler(CallbackQueryHandler(cancel_schedule, pattern="^cancel_"))
    app_.add_handler(MessageHandler(filters.ALL, register_chat_on_message))
    logger.info("Bot running...")
    app_.run_polling()

if __name__ == "__main__":
    threading.Thread(target=run_flask).start()
    run_telegram_bot()
