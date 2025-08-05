import logging
import sqlite3
import os
import asyncio
import threading
import json
from datetime import datetime, timedelta, time as dt_time
import pytz
from apscheduler.schedulers.background import BackgroundScheduler
from telegram import Update, InlineKeyboardMarkup, InlineKeyboardButton, MessageEntity
from telegram.ext import (
    ApplicationBuilder, CommandHandler, ContextTypes, CallbackQueryHandler,
    MessageHandler, filters, ConversationHandler
)
from flask import Flask
import html
import re

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
    recurrence_data TEXT,
    entities TEXT
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
cur.execute('''CREATE TABLE IF NOT EXISTS standup_tracking (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    schedule_id INTEGER,
    chat_id INTEGER,
    topic_id INTEGER,
    standup_message_id INTEGER,
    user_id INTEGER,
    username TEXT,
    done INTEGER DEFAULT 0,
    deadline TEXT
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

# --- GLOBAL MAIN ASYNCIO EVENT LOOP ---
main_asyncio_loop = None

# --- Async Flood Control Queue ---
send_queue = asyncio.Queue()

def safe_enqueue_send_job(send_job):
    global main_asyncio_loop
    try:
        loop = asyncio.get_running_loop()
        if loop and loop.is_running():
            asyncio.create_task(send_queue.put(send_job))
        else:
            if main_asyncio_loop and main_asyncio_loop.is_running():
                asyncio.run_coroutine_threadsafe(send_queue.put(send_job), main_asyncio_loop)
            else:
                raise RuntimeError("No running asyncio event loop available to enqueue jobs.")
    except RuntimeError:
        if main_asyncio_loop and main_asyncio_loop.is_running():
            asyncio.run_coroutine_threadsafe(send_queue.put(send_job), main_asyncio_loop)
        else:
            raise RuntimeError("No running asyncio event loop available to enqueue jobs.")

async def flood_control_worker():
    while True:
        send_job = await send_queue.get()
        try:
            await send_job()
        except Exception as e:
            logger.error(f"Flood control worker error: {e}")
        await asyncio.sleep(3)
        send_queue.task_done()

def post_scheduled_message(target_chat_id, topic_id, message, schedule_id, recurrence="none"):
    # Always use a fresh connection and cursor for jobs
    with sqlite3.connect('schedules.db') as conn_local:
        cur_local = conn_local.cursor()
        cur_local.execute("SELECT entities FROM schedules WHERE id=?", (schedule_id,))
        row = cur_local.fetchone()
        entities = None
        if row and row[0]:
            try:
                entities = [MessageEntity.de_json(e, None) for e in json.loads(row[0])]
            except Exception:
                entities = None

    async def send_job():
        app_ = ApplicationBuilder().token(BOT_TOKEN).build()
        if topic_id:
            msg_obj = await app_.bot.send_message(
                chat_id=target_chat_id,
                text=message,
                message_thread_id=int(topic_id),
                entities=entities if entities else None,
                disable_web_page_preview=False
            )
        else:
            msg_obj = await app_.bot.send_message(
                chat_id=target_chat_id,
                text=message,
                entities=entities if entities else None,
                disable_web_page_preview=False
            )
        logger.info(f"Sent message for schedule {schedule_id}")

        # Standup follow-up logic - always use a new conn/cursor
        usernames = set(re.findall(r'@(\w+)', message))
        deadline = (datetime.utcnow() + timedelta(hours=2)).isoformat()
        with sqlite3.connect('schedules.db') as conn_local2:
            cur_local2 = conn_local2.cursor()
            for uname in usernames:
                cur_local2.execute("INSERT INTO standup_tracking (schedule_id, chat_id, topic_id, standup_message_id, username, deadline) VALUES (?, ?, ?, ?, ?, ?)",
                    (schedule_id, target_chat_id, topic_id, msg_obj.message_id, uname.lower(), deadline))
            conn_local2.commit()
        if usernames:
            scheduler.add_job(
                followup_check_standups,
                'date',
                run_date=datetime.utcnow() + timedelta(hours=2),
                args=[schedule_id, target_chat_id, topic_id, msg_obj.message_id]
            )

        if recurrence == "none":
            with sqlite3.connect('schedules.db') as conn_local3:
                cur_local3 = conn_local3.cursor()
                cur_local3.execute("DELETE FROM schedules WHERE id=?", (schedule_id,))
                conn_local3.commit()

    safe_enqueue_send_job(send_job)

def followup_check_standups(schedule_id, chat_id, topic_id, standup_message_id):
    with sqlite3.connect('schedules.db') as conn_local:
        cur_local = conn_local.cursor()
        cur_local.execute("SELECT username FROM standup_tracking WHERE schedule_id=? AND chat_id=? AND topic_id=? AND standup_message_id=? AND done=0", 
                    (schedule_id, chat_id, topic_id, standup_message_id))
        users = [row[0] for row in cur_local.fetchall()]
        if users:
            mention_text = " ".join([f"@{uname}" for uname in users])
            msg = f"Hey there! 🌞 Just a gentle reminder to send in your standup when you get a chance. We appreciate your updates! 🚀\n{mention_text}"
            async def send_followup():
                app_ = ApplicationBuilder().token(BOT_TOKEN).build()
                await app_.bot.send_message(
                    chat_id=chat_id, 
                    text=msg,
                    message_thread_id=int(topic_id) if topic_id else None
                )
            safe_enqueue_send_job(send_followup)
        cur_local.execute("DELETE FROM standup_tracking WHERE schedule_id=? AND chat_id=? AND topic_id=? AND standup_message_id=?", 
                    (schedule_id, chat_id, topic_id, standup_message_id))
        conn_local.commit()

CHOOSE_GROUP, CHOOSE_TOPIC, CHOOSE_RECURRENCE, CHOOSE_TIME, WRITE_MSG, CONFIRM = range(6)

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

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    keyboard = [[InlineKeyboardButton("📅 Schedule Message", callback_data="schedule_start")]]
    await update.message.reply_text("Welcome! What do you want to do?", reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='HTML')

async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    help_text = (
        "<b>Available Commands:</b>\n\n"
        "/start – Begin scheduling a message with guided buttons\n"
        "/help – Show this help message\n"
        "/myschedules – List and cancel your future scheduled messages\n"
        "/whereami – Show the group and topic/thread ID (useful for admins and troubleshooting)\n"
        "/cancel – Cancel the current operation (only when in scheduling flow)\n"
        "/topicname [Display Name] – Set a human-friendly name for the current topic (use in topic)\n\n"
        "<b>How to register groups/topics:</b>\n"
        "- Add the bot as admin to your group (with permission to read messages)\n"
        "- Use /topicname in a topic to register it (the bot only schedules to named topics)\n"
        "- Use /topicname in a topic to give it a readable name\n\n"
        "<b>How to schedule:</b>\n"
        "1. DM or /start the bot\n"
        "2. Tap 'Schedule Message' and follow the button prompts\n"
        "3. Select group, topic, recurrence, time (Asia/Manila), and type your message\n"
        "4. Confirm to schedule\n"
    )
    await update.message.reply_text(help_text, parse_mode="HTML")

async def set_topic_name(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_chat.type not in ['group', 'supergroup']:
        await update.message.reply_text("This command must be used in a group/topic.", parse_mode='HTML')
        return
    topic_id = getattr(update.message, "message_thread_id", None)
    if topic_id is None:
        await update.message.reply_text("This command must be used <b>inside a topic</b> (not main chat).", parse_mode='HTML')
        return
    name = " ".join(context.args).strip()
    if not name:
        await update.message.reply_text("Usage: /topicname Actual Topic Name", parse_mode='HTML')
        return
    register_topic(update.effective_chat.id, topic_id, name)
    await update.message.reply_text(f"Topic name for ID {topic_id} set to: <b>{html.escape(name)}</b>", parse_mode='HTML')

async def myschedules(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    cur.execute(
        "SELECT id, target_chat_id, topic_id, message, run_at, recurrence, recurrence_data FROM schedules WHERE user_id = ? ORDER BY run_at ASC",
        (user_id,)
    )
    schedules = cur.fetchall()
    if not schedules:
        await update.message.reply_text("You have no scheduled messages.", parse_mode='HTML')
        return

    for sched in schedules:
        schedule_id, chat_id, topic_id, msg, run_at, recurrence, recurrence_data = sched
        cur.execute("SELECT title FROM groups WHERE chat_id = ?", (chat_id,))
        group_row = cur.fetchone()
        group_name = group_row[0] if group_row else str(chat_id)
        tname = None
        if topic_id:
            cur.execute("SELECT topic_name FROM topics WHERE chat_id = ? AND topic_id = ?", (chat_id, topic_id))
            trow = cur.fetchone()
            tname = trow[0] if trow else f"Topic {topic_id}"
        dt = datetime.fromisoformat(run_at)
        dt_ph = dt.astimezone(PH_TZ)
        preview = html.escape(msg[:60].replace('\n', ' ') + ("..." if len(msg) > 60 else ""))
        text = f"<b>Group:</b> {html.escape(group_name)}\n"
        text += f"<b>Topic:</b> {html.escape(tname) if tname else 'Main chat'}\n"
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
    try:
        scheduler.remove_job(str(schedule_id))
    except Exception:
        pass
    cur.execute("DELETE FROM schedules WHERE id = ?", (schedule_id,))
    conn.commit()
    await query.edit_message_text("❌ Scheduled message cancelled.", parse_mode='HTML')

async def schedule_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    cur.execute("SELECT chat_id, title FROM groups")
    groups = cur.fetchall()
    if not groups:
        await query.edit_message_text("No groups registered yet. Add me to groups as admin and use any command there first!", parse_mode='HTML')
        return ConversationHandler.END
    keyboard = [
        [InlineKeyboardButton(f"{title}", callback_data=f"group_{chat_id}")]
        for chat_id, title in groups
    ]
    await query.edit_message_text("Which group?", reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='HTML')
    return CHOOSE_GROUP

async def choose_group(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    group_id = int(query.data.split("_")[1])
    context.user_data['target_chat_id'] = group_id

    # Only include topics that have been named with /topicname and are not default "Topic ###"
    cur.execute("SELECT topic_id, topic_name FROM topics WHERE chat_id = ? AND topic_id != 0 AND topic_name != '' AND topic_name NOT LIKE 'Topic %'", (group_id,))
    topics = cur.fetchall()
    # Always allow Main chat
    topics.insert(0, (0, "Main chat"))
    if len(topics) == 1:  # only main chat
        keyboard = [
            [InlineKeyboardButton("Main chat (no topic)", callback_data="topic_0")]
        ]
        await query.edit_message_text(
            "No named topics found for this group. Use <code>/topicname [Display Name]</code> in a topic to register it for scheduling.",
            reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='HTML'
        )
        return CHOOSE_TOPIC

    keyboard = [
        [InlineKeyboardButton(topic_name, callback_data=f"topic_{topic_id}")]
        for topic_id, topic_name in topics
    ]
    await query.edit_message_text("Choose a topic (or main chat):", reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='HTML')
    return CHOOSE_TOPIC

async def choose_topic(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    topic_id = int(query.data.split("_")[1])
    context.user_data['topic_id'] = topic_id if topic_id != 0 else None
    weekdays = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
    keyboard = [
        [InlineKeyboardButton("One time only", callback_data="recurr_none")]
    ] + [
        [InlineKeyboardButton(f"Repeat every {day}", callback_data=f"recurr_weekly_{day}")]
        for day in weekdays
    ]
    await query.edit_message_text(
        "Do you want this message to repeat?\n\nSelect a day to repeat weekly or pick 'One time only'.",
        reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='HTML'
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
        await query.edit_message_text(f"Enter the time (24h, Asia/Manila) for every {day}, e.g. 13:00 for 1PM.", parse_mode='HTML')
        return CHOOSE_TIME

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
        await update.callback_query.edit_message_text(msg, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='HTML')
    else:
        await update.message.reply_text(msg, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='HTML')
    return CHOOSE_TIME

async def choose_time(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = getattr(update, "callback_query", None)
    if query:
        await query.answer()
        data = query.data.replace("time_", "")
        if data == "manual":
            await query.edit_message_text(
                "Reply with date and time in <code>YYYY-MM-DD HH:MM</code> format (24-hour, Asia/Manila).\n\nType /cancel to abort.", 
                parse_mode='HTML')
            return CHOOSE_TIME
        else:
            context.user_data['run_at'] = data
            await query.edit_message_text(
                f"Time set to: <code>{html.escape(data)}</code> (Asia/Manila)\nNow, please send your message.\n\nYou can use Telegram's rich text formatting (bold, italics, etc) and emojis!", 
                parse_mode='HTML')
            return WRITE_MSG
    else:
        txt = update.message.text.strip()
        if context.user_data.get('recurrence', "none") == "weekly":
            try:
                dt_time.fromisoformat(txt)
                context.user_data['recurr_time'] = txt
                await update.message.reply_text(
                    f"Time set to <code>{html.escape(txt)}</code>. Now, please send your message text.", 
                    parse_mode='HTML')
                return WRITE_MSG
            except Exception:
                await update.message.reply_text("Invalid format. Use <code>HH:MM</code> (e.g. 13:00 for 1PM).", parse_mode='HTML')
                return CHOOSE_TIME
        else:
            try:
                dt = datetime.strptime(txt, "%Y-%m-%d %H:%M")
                dt = PH_TZ.localize(dt)
                context.user_data['run_at'] = dt.strftime('%Y-%m-%d %H:%M')
                await update.message.reply_text(
                    "Time set! Now, please send your message text.\n(You can use Telegram's formatting.)", 
                    parse_mode='HTML')
                return WRITE_MSG
            except Exception:
                await update.message.reply_text("Invalid format. Please use <code>YYYY-MM-DD HH:MM</code> (24hr), or type /cancel.", parse_mode='HTML')
                return CHOOSE_TIME

async def write_msg(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data['message'] = update.message.text
    # Save formatting entities as JSON
    if update.message.entities:
        context.user_data['entities'] = json.dumps([e.to_dict() for e in update.message.entities])
    else:
        context.user_data['entities'] = None
    group = context.user_data.get('target_chat_id')
    topic = context.user_data.get('topic_id')
    recurrence = context.user_data.get('recurrence', "none")
    msg = f"Ready to schedule:\nGroup: <code>{group}</code>\n"
    if topic:
        cur.execute("SELECT topic_name FROM topics WHERE chat_id = ? AND topic_id = ?", (group, topic))
        tname = cur.fetchone()
        if tname:
            msg += f"Topic: <code>{html.escape(tname[0])}</code>\n"
        else:
            msg += f"Topic ID: <code>{topic}</code>\n"
    if recurrence == "weekly":
        weekday = context.user_data['weekday']
        at_time = context.user_data['recurr_time']
        msg += f"Repeats: Every {weekday} at {at_time} (Asia/Manila)\n"
    else:
        run_at = context.user_data.get('run_at')
        msg += f"Time: <code>{html.escape(run_at)}</code> (Asia/Manila)\n"
    msg += f"Message:\n<code>{html.escape(context.user_data['message'])}</code>\n\nConfirm?"
    keyboard = [
        [InlineKeyboardButton("✅ Confirm", callback_data="confirm_yes")],
        [InlineKeyboardButton("❌ Cancel", callback_data="confirm_no")]
    ]
    await update.message.reply_text(msg, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='HTML')
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
        entities = context.user_data.get('entities')
        if recurrence == "weekly":
            weekday = context.user_data['weekday']
            at_time = context.user_data['recurr_time']
            dt_now = datetime.now(PH_TZ)
            week_days = ['Monday','Tuesday','Wednesday','Thursday','Friday','Saturday','Sunday']
            day_idx = week_days.index(weekday)
            next_dt = dt_now
            while next_dt.weekday() != day_idx or next_dt.time() > dt_time.fromisoformat(at_time):
                next_dt += timedelta(days=1)
            next_dt = next_dt.replace(hour=int(at_time.split(":")[0]), minute=int(at_time.split(":")[1]), second=0, microsecond=0)
            dt_utc = PH_TZ.localize(next_dt).astimezone(pytz.utc)
            cur.execute(
                "INSERT INTO schedules (target_chat_id, topic_id, user_id, message, run_at, recurrence, recurrence_data, entities) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                (group, topic, user_id, message, dt_utc.isoformat(), "weekly", f"{weekday}:{at_time}", entities)
            )
            conn.commit()
            schedule_id = cur.lastrowid
            scheduler.add_job(
                post_scheduled_message,
                'cron',
                day_of_week=weekday.lower(),
                hour=int(at_time.split(":")[0]),
                minute=int(at_time.split(":")[1]),
                args=[group, topic, message, schedule_id, "weekly"],
                id=str(schedule_id)
            )
            await query.edit_message_text(f"✅ Weekly recurring message scheduled for every {weekday} at {at_time} (Asia/Manila)!", parse_mode='HTML')
        else:
            run_at = context.user_data['run_at']
            dt_ph = PH_TZ.localize(datetime.strptime(run_at, "%Y-%m-%d %H:%M"))
            dt_utc = dt_ph.astimezone(pytz.utc)
            cur.execute(
                "INSERT INTO schedules (target_chat_id, topic_id, user_id, message, run_at, recurrence, entities) VALUES (?, ?, ?, ?, ?, ?, ?)",
                (group, topic, user_id, message, dt_utc.isoformat(), "none", entities)
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
            await query.edit_message_text("✅ One-time scheduled message set!", parse_mode='HTML')
    else:
        await query.edit_message_text("Cancelled.", parse_mode='HTML')
    context.user_data.clear()
    return ConversationHandler.END

async def cancel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("Scheduling cancelled.", parse_mode='HTML')
    context.user_data.clear()
    return ConversationHandler.END

async def register_chat_on_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_chat.type in ['group', 'supergroup']:
        register_group(update.effective_chat)
        topic_id = getattr(update.message, "message_thread_id", None)
        # Only register "main chat" automatically
        if topic_id is None or topic_id == 0:
            register_topic(update.effective_chat.id, 0, "Main chat")

        # --- Standup reply tracking (must reply to bot's standup message) ---
        if getattr(update.message, "reply_to_message", None):
            replied_id = update.message.reply_to_message.message_id
            user = update.effective_user
            username = user.username.lower() if user.username else ""
            with sqlite3.connect('schedules.db') as conn_local:
                cur_local = conn_local.cursor()
                cur_local.execute(
                    "SELECT id FROM standup_tracking WHERE standup_message_id=? AND chat_id=? AND topic_id=? AND username=? AND done=0",
                    (replied_id, update.effective_chat.id, getattr(update.message, "message_thread_id", None), username)
                )
                row = cur_local.fetchone()
                if row:
                    cur_local.execute("UPDATE standup_tracking SET done=1 WHERE id=?", (row[0],))
                    conn_local.commit()
                    try:
                        # v21+: send_reaction, v20: fallback to "Thanks!"
                        send_react = getattr(context.bot, "send_reaction", None)
                        if send_react:
                            await send_react(
                                chat_id=update.effective_chat.id,
                                message_id=update.message.message_id,
                                emoji="👍"
                            )
                        else:
                            await update.message.reply_text("Thanks!")
                    except Exception as e:
                        logger.error(f"Failed to react or thank: {e}")

async def whereami(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    msg_thread_id = update.message.message_thread_id if update.message and update.message.message_thread_id else None
    msg = f"Chat ID: <code>{chat_id}</code>"
    if msg_thread_id:
        msg += f"\nTopic (Thread) ID: <code>{msg_thread_id}</code>"
    else:
        msg += "\n(Not in a topic/thread right now.)"
    await update.message.reply_text(msg, parse_mode='HTML')

def run_telegram_bot():
    global main_asyncio_loop
    app_ = ApplicationBuilder().token(BOT_TOKEN).build()
    main_asyncio_loop = asyncio.get_event_loop()
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
    main_asyncio_loop.create_task(flood_control_worker())
    logger.info("Bot running...")
    app_.run_polling()

if __name__ == "__main__":
    threading.Thread(target=run_flask).start()
    run_telegram_bot()
