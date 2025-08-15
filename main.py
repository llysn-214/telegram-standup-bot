import logging
import sqlite3
import os
import asyncio
import threading
import json
from datetime import datetime, timedelta, time as dt_time
import pytz
from apscheduler.schedulers.background import BackgroundScheduler
from telegram import Update, InlineKeyboardMarkup, InlineKeyboardButton, MessageEntity, Bot
from telegram.ext import (
    Application, CommandHandler, ContextTypes, CallbackQueryHandler,
    MessageHandler, filters, ConversationHandler, ChatMemberHandler
)
from flask import Flask
import html
import re
import shutil

# =========================
# Config
# =========================
BOT_TOKEN = os.environ.get("BOT_TOKEN")
PORT = int(os.environ.get('PORT', 10000))
PH_TZ = pytz.timezone('Asia/Manila')

DB_PATH = os.environ.get("DB_PATH", "schedules.db")
BACKUP_TMP_PATH = "/tmp/schedules-backup.db"

ADMIN_IDS = {int(x) for x in os.environ.get("ADMIN_IDS", "").split(",") if x.strip().isdigit()}

def is_admin(user_id: int) -> bool:
    return user_id in ADMIN_IDS or not ADMIN_IDS

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# =========================
# DB init
# =========================
conn = sqlite3.connect(DB_PATH, check_same_thread=False)
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

def reopen_db():
    global conn, cur
    try:
        conn.close()
    except Exception:
        pass
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    cur = conn.cursor()

# =========================
# APScheduler
# =========================
scheduler = BackgroundScheduler(
    timezone=PH_TZ,
    job_defaults={"misfire_grace_time": 3600, "coalesce": True, "max_instances": 1}
)
scheduler.start()

def clear_all_jobs():
    for job in scheduler.get_jobs():
        try:
            scheduler.remove_job(job.id)
        except Exception:
            pass

# =========================
# Flask (UptimeRobot ping)
# =========================
app_flask = Flask(__name__)

@app_flask.route("/")
def home():
    return "OK", 200

def run_flask():
    app_flask.run(host="0.0.0.0", port=PORT)

# =========================
# Global asyncio loop + flood control queue
# =========================
main_asyncio_loop = None
send_queue = asyncio.Queue()

def safe_enqueue_send_job(send_job):
    """Enqueue coroutine factory to be awaited by flood_control_worker."""
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

# =========================
# Helpers
# =========================
HTML_TAG_RE = re.compile(r'</?(b|strong|i|em|u|s|code|pre|a)[^>]*>', re.IGNORECASE)
def looks_like_html(s: str) -> bool:
    return bool(HTML_TAG_RE.search(s))

# =========================
# Job poster
# =========================
def post_scheduled_message(target_chat_id, topic_id, message, schedule_id, recurrence="none"):
    # Load entities fresh
    with sqlite3.connect(DB_PATH) as conn_local:
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
        bot = Bot(BOT_TOKEN)
        send_kwargs = dict(chat_id=target_chat_id, text=message, disable_web_page_preview=False)
        if topic_id:
            send_kwargs["message_thread_id"] = int(topic_id)
        if entities:
            send_kwargs["entities"] = entities
        elif looks_like_html(message):
            send_kwargs["parse_mode"] = 'HTML'

        msg_obj = await bot.send_message(**send_kwargs)
        logger.info(f"Sent message for schedule {schedule_id}")

        usernames = set(re.findall(r'@(\w+)', message))
        deadline = (datetime.now(pytz.utc) + timedelta(hours=2)).isoformat()
        with sqlite3.connect(DB_PATH) as conn_local2:
            cur_local2 = conn_local2.cursor()
            if usernames:
                for uname in usernames:
                    cur_local2.execute(
                        "INSERT INTO standup_tracking (schedule_id, chat_id, topic_id, standup_message_id, username, deadline) "
                        "VALUES (?, ?, ?, ?, ?, ?)",
                        (schedule_id, target_chat_id, topic_id, msg_obj.message_id, uname.lower(), deadline)
                    )
            else:
                cur_local2.execute(
                    "INSERT INTO standup_tracking (schedule_id, chat_id, topic_id, standup_message_id, username, deadline) "
                    "VALUES (?, ?, ?, ?, ?, ?)",
                    (schedule_id, target_chat_id, topic_id, msg_obj.message_id, '*', deadline)
                )
            conn_local2.commit()

        if usernames:
            scheduler.add_job(
                followup_check_standups,
                'date',
                run_date=datetime.now(pytz.utc) + timedelta(hours=2),
                args=[schedule_id, target_chat_id, topic_id, msg_obj.message_id]
            )

        if recurrence == "none":
            with sqlite3.connect(DB_PATH) as conn_local3:
                cur_local3 = conn_local3.cursor()
                cur_local3.execute("DELETE FROM schedules WHERE id=?", (schedule_id,))
                conn_local3.commit()

    safe_enqueue_send_job(send_job)

def followup_check_standups(schedule_id, chat_id, topic_id, standup_message_id):
    async def send_follow():
        bot = Bot(BOT_TOKEN)
        with sqlite3.connect(DB_PATH) as conn_local:
            cur_local = conn_local.cursor()
            cur_local.execute(
                "SELECT username FROM standup_tracking "
                "WHERE schedule_id=? AND chat_id=? AND topic_id=? AND standup_message_id=? "
                "AND done=0 AND username!='*'",
                (schedule_id, chat_id, topic_id, standup_message_id)
            )
            users = [row[0] for row in cur_local.fetchall()]
            if users:
                mention_text = " ".join([f"@{uname}" for uname in users])
                msg = f"Hey there! 🌞 Just a gentle reminder to send in your standup when you get a chance. 🚀\n{mention_text}"
                kwargs = dict(chat_id=chat_id, text=msg)
                if topic_id:
                    kwargs["message_thread_id"] = int(topic_id)
                await bot.send_message(**kwargs)
            cur_local.execute(
                "DELETE FROM standup_tracking WHERE schedule_id=? AND chat_id=? AND topic_id=? AND standup_message_id=?",
                (schedule_id, chat_id, topic_id, standup_message_id)
            )
            conn_local.commit()

    safe_enqueue_send_job(send_follow)

# =========================
# Rehydrate jobs
# =========================
def rehydrate_jobs():
    try:
        with sqlite3.connect(DB_PATH) as c:
            cur_r = c.cursor()
            cur_r.execute("""SELECT id, target_chat_id, topic_id, message, run_at, recurrence, recurrence_data
                             FROM schedules""")
            rows = cur_r.fetchall()

        weekday_abbrs = ['mon','tue','wed','thu','fri','sat','sun']
        week_days = ['Monday','Tuesday','Wednesday','Thursday','Friday','Saturday','Sunday']

        for (sid, chat_id, topic_id, message, run_at, recurrence, rdata) in rows:
            job_id = str(sid)
            try:
                scheduler.remove_job(job_id)
            except Exception:
                pass

            if recurrence == "weekly":
                parts = (rdata or "").split(":")
                weekday = parts[0] if parts else "Monday"
                at_time = ":".join(parts[1:]) if len(parts) > 1 else "09:00"
                hour, minute = map(int, at_time.split(":")[:2])
                wd_abbr = weekday_abbrs[week_days.index(weekday)] if weekday in week_days else 'mon'
                scheduler.add_job(
                    post_scheduled_message,
                    'cron',
                    day_of_week=wd_abbr,
                    hour=hour,
                    minute=minute,
                    args=[chat_id, topic_id, message, sid, "weekly"],
                    id=job_id,
                    replace_existing=True
                )
                logger.info(f"Rehydrated weekly #{sid} {wd_abbr} {hour:02d}:{minute:02d} PH")
            else:
                try:
                    dt_utc = datetime.fromisoformat(run_at)
                except Exception:
                    continue
                if dt_utc.tzinfo is None:
                    dt_utc = pytz.utc.localize(dt_utc)
                if dt_utc > datetime.now(pytz.utc):
                    scheduler.add_job(
                        post_scheduled_message,
                        'date',
                        run_date=dt_utc,
                        args=[chat_id, topic_id, message, sid, "none"],
                        id=job_id,
                        replace_existing=True
                    )
                    logger.info(f"Rehydrated one-time #{sid} -> {dt_utc.isoformat()}")
    except Exception as e:
        logger.error(f"Failed to rehydrate jobs: {e}")

# =========================
# Conversation states
# =========================
CHOOSE_GROUP, CHOOSE_TOPIC, CHOOSE_RECURRENCE, CHOOSE_TIME, CHOOSE_HOUR, CHOOSE_MIN, WRITE_MSG, CONFIRM = range(8)

# =========================
# Group/topic registration
# =========================
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

# =========================
# DM admin gate
# =========================
async def require_dm_admin(update: Update) -> bool:
    if not update.effective_chat or update.effective_chat.type != 'private':
        await update.message.reply_text("Please DM me to run this command.", parse_mode='HTML')
        return False
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("Sorry, you’re not allowed to run this.", parse_mode='HTML')
        return False
    return True

# =========================
# Auto-registration (no test posts)
# =========================
async def on_bot_membership(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat = update.effective_chat
    if chat and chat.type in ('group', 'supergroup'):
        register_group(chat)

# =========================
# DM Admin utilities
# =========================
async def listgroups_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await require_dm_admin(update): return
    cur.execute("SELECT chat_id, title FROM groups ORDER BY title COLLATE NOCASE")
    rows = cur.fetchall() if hasattr(cur, "fetchall") else []
    if not rows:
        await update.message.reply_text("No groups known yet. Add me to a group as admin; I’ll auto-register silently.", parse_mode='HTML')
        return
    lines = [f"- {html.escape(title)} (<code>{cid}</code>)" for cid, title in rows]
    await update.message.reply_text("Groups I know:\n" + "\n".join(lines), parse_mode='HTML')

async def listtopics_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await require_dm_admin(update): return
    if not context.args:
        await update.message.reply_text("Usage: /listtopics <group_id>", parse_mode='HTML')
        return
    try:
        gid = int(context.args[0])
    except Exception:
        await update.message.reply_text("Invalid group_id.", parse_mode='HTML')
        return
    cur.execute("SELECT topic_id, topic_name FROM topics WHERE chat_id=? ORDER BY topic_id", (gid,))
    rows = cur.fetchall()
    if not rows:
        await update.message.reply_text("No topics saved for that group.", parse_mode='HTML')
        return
    lines = []
    for tid, tname in rows:
        label = tname or (f"Topic #{tid}" if tid else "Main chat")
        lines.append(f"- {html.escape(label)} (<code>{tid}</code>)")
    await update.message.reply_text("Topics:\n" + "\n".join(lines), parse_mode='HTML')

async def addtopic_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await require_dm_admin(update): return
    if len(context.args) < 3:
        await update.message.reply_text("Usage: /addtopic <group_id> <topic_id> <Display Name...>", parse_mode='HTML')
        return
    try:
        gid = int(context.args[0]); tid = int(context.args[1])
    except Exception:
        await update.message.reply_text("group_id and topic_id must be numbers.", parse_mode='HTML')
        return
    name = " ".join(context.args[2:]).strip()
    if not name:
        await update.message.reply_text("Please provide a display name.", parse_mode='HTML')
        return
    cur.execute("SELECT 1 FROM groups WHERE chat_id=?", (gid,))
    if not cur.fetchone() and update.effective_chat and update.effective_chat.id == gid:
        register_group(update.effective_chat)
    register_topic(gid, tid, name)
    await update.message.reply_text(f"Saved topic <b>{html.escape(name)}</b> for group <code>{gid}</code> (topic_id <code>{tid}</code>).", parse_mode='HTML')

async def renametopic_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await require_dm_admin(update): return
    if len(context.args) < 3:
        await update.message.reply_text("Usage: /renametopic <group_id> <topic_id> <New Name...>", parse_mode='HTML')
        return
    try:
        gid = int(context.args[0]); tid = int(context.args[1])
    except Exception:
        await update.message.reply_text("group_id and topic_id must be numbers.", parse_mode='HTML')
        return
    name = " ".join(context.args[2:]).strip()
    if not name:
        await update.message.reply_text("Please provide a new name.", parse_mode='HTML')
        return
    register_topic(gid, tid, name)
    await update.message.reply_text(f"Renamed topic to <b>{html.escape(name)}</b> (group <code>{gid}</code>, topic <code>{tid}</code>).", parse_mode='HTML')

async def deltopic_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await require_dm_admin(update): return
    if len(context.args) < 2:
        await update.message.reply_text("Usage: /deltopic <group_id> <topic_id>", parse_mode='HTML')
        return
    try:
        gid = int(context.args[0]); tid = int(context.args[1])
    except Exception:
        await update.message.reply_text("group_id and topic_id must be numbers.", parse_mode='HTML')
        return
    cur.execute("DELETE FROM topics WHERE chat_id=? AND topic_id=?", (gid, tid))
    conn.commit()
    await update.message.reply_text(f"Deleted topic mapping for group <code>{gid}</code>, topic <code>{tid}</code>.", parse_mode='HTML')

async def listtopics_dm_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await require_dm_admin(update): return
    cur.execute("""
        SELECT g.chat_id, g.title, t.topic_id, t.topic_name
        FROM groups g
        LEFT JOIN topics t ON g.chat_id = t.chat_id
        ORDER BY g.title COLLATE NOCASE, t.topic_id
    """)
    rows = cur.fetchall()
    if not rows:
        await update.message.reply_text(
            "No groups/topics known yet. Add me to a group as admin; I’ll auto-register silently.",
            parse_mode='HTML'
        )
        return
    lines = []
    current_gid = None
    for gid, gtitle, tid, tname in rows:
        if gid != current_gid:
            if current_gid is not None:
                lines.append("")
            lines.append(f"<b>{html.escape(gtitle or str(gid))}</b>  <code>{gid}</code>")
            current_gid = gid
        if tid is None:
            lines.append("  • (no topics discovered yet)")
        else:
            label = tname or (f"Topic #{tid}" if tid else "Main chat")
            lines.append(f"  • {html.escape(label)}  <code>{tid}</code>")
    await update.message.reply_text("\n".join(lines), parse_mode='HTML')

# =========================
# Backup / Restore (DM admin only)
# =========================
async def backupdb_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await require_dm_admin(update): return
    try:
        await context.bot.send_document(
            chat_id=update.effective_user.id,
            document=open(DB_PATH, 'rb'),
            filename='schedules.db',
            caption='SQLite backup'
        )
        await update.message.reply_text("Backup sent to your DM ✅", parse_mode='HTML')
    except Exception as e:
        logger.error(f"Backup failed: {e}")
        await update.message.reply_text("Backup failed. Make sure you /start me in DM first.", parse_mode='HTML')

async def restoredb_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await require_dm_admin(update): return
    if not update.message.reply_to_message or not update.message.reply_to_message.document:
        await update.message.reply_text("Reply to a .db file with /restoredb.", parse_mode='HTML')
        return
    try:
        file = await context.bot.get_file(update.message.reply_to_message.document.file_id)
        await file.download_to_drive(BACKUP_TMP_PATH)
        clear_all_jobs()
        try:
            conn.close()
        except Exception:
            pass
        shutil.copyfile(BACKUP_TMP_PATH, DB_PATH)
        reopen_db()
        rehydrate_jobs()
        await update.message.reply_text("Restore complete and jobs rehydrated ✅", parse_mode='HTML')
    except Exception as e:
        logger.error(f"Restore failed: {e}")
        await update.message.reply_text("Restore failed. Check the file and try again.", parse_mode='HTML')

# =========================
# Commands & flow
# =========================
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    keyboard = [[InlineKeyboardButton("📅 Schedule Message", callback_data="schedule_start")]]
    await update.message.reply_text("Welcome! What do you want to do?", reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='HTML')

async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    help_text = (
        "<b>Available Commands:</b>\n\n"
        "/start – Guided scheduling with buttons\n"
        "/help – This help\n"
        "/myschedules – List & cancel your future scheduled messages\n"
        "/whereami – Show group & topic/thread ID\n"
        "/topicname [Display Name] – Set a friendly name for the current topic (run inside the topic)\n"
        "/topics – (in group) list known topics\n\n"
        "<b>DM Admin (private chat):</b>\n"
        "/listgroups, /listtopicsdm, /listtopics, /addtopic, /renametopic, /deltopic\n"
        "/backupdb – DM a copy of the database\n"
        "/restoredb – Reply to a .db file in DM to restore\n\n"
        "Timezone: Asia/Manila. Weekly schedules persist and auto-rehydrate."
    )
    await update.message.reply_text(help_text, parse_mode="HTML")

async def set_topic_name(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_chat.type not in ['group', 'supergroup']:
        await update.message.reply_text("Use this inside a group/topic.", parse_mode='HTML')
        return
    topic_id = getattr(update.message, "message_thread_id", None)
    if topic_id is None:
        await update.message.reply_text("Use this <b>inside a topic</b> (not main chat).", parse_mode='HTML')
        return
    name = " ".join(context.args).strip()
    if not name:
        await update.message.reply_text("Usage: /topicname Actual Topic Name", parse_mode='HTML')
        return
    register_group(update.effective_chat)  # ensure group exists
    register_topic(update.effective_chat.id, topic_id, name)
    await update.message.reply_text(f"Topic name for ID {topic_id} set to: <b>{html.escape(name)}</b>", parse_mode='HTML')

async def topics_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat = update.effective_chat
    if chat.type not in ['group', 'supergroup']:
        await update.message.reply_text("Use this inside a group.", parse_mode='HTML')
        return
    cur.execute("SELECT topic_id, topic_name FROM topics WHERE chat_id=? ORDER BY topic_id", (chat.id,))
    rows = cur.fetchall()
    if not rows:
        await update.message.reply_text("No topics known yet. Create/rename a topic or run /topicname inside one.", parse_mode='HTML')
        return
    lines = []
    for tid, tname in rows:
        display = tname or (f"Topic #{tid}" if tid else "Main chat")
        lines.append(f"- {html.escape(display)} (<code>{tid}</code>)")
    await update.message.reply_text("Known topics:\n" + "\n".join(lines), parse_mode='HTML')

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
        if dt.tzinfo is None:
            dt = pytz.utc.localize(dt)
        dt_ph = dt.astimezone(PH_TZ)
        preview = html.escape(msg[:60].replace('\n', ' ') + ("..." if len(msg) > 60 else ""))
        text = f"<b>Group:</b> {html.escape(group_name)}\n"
        text += f"<b>Topic:</b> {html.escape(tname) if tname else 'Main chat'}\n"
        if recurrence == "weekly":
            parts = (recurrence_data or "").split(":")
            weekday = parts[0]
            at_time = ":".join(parts[1:]) if len(parts) > 1 else ""
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

# =========================
# Schedule flow
# =========================
async def schedule_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    cur.execute("SELECT chat_id, title FROM groups ORDER BY title COLLATE NOCASE")
    groups = cur.fetchall()
    if not groups:
        await query.edit_message_text(
            "No groups registered yet. Add me to a group as admin; I’ll auto-register silently on add/topic events. "
            "You can also run /topicname inside a topic to register it.",
            parse_mode='HTML'
        )
        return ConversationHandler.END
    keyboard = [[InlineKeyboardButton(f"{title}", callback_data=f"group_{chat_id}")] for chat_id, title in groups]
    await query.edit_message_text("Which group?", reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='HTML')
    return CHOOSE_GROUP

async def choose_group(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    group_id = int(query.data.split("_")[1])
    context.user_data['target_chat_id'] = group_id
    cur.execute("SELECT topic_id, topic_name FROM topics WHERE chat_id = ?", (group_id,))
    topics = cur.fetchall()
    if not any(tid == 0 for tid, _ in topics):
        topics.insert(0, (0, "Main chat"))
    keyboard = []
    for topic_id, topic_name in topics:
        display = topic_name or (f"Topic #{topic_id}" if topic_id else "Main chat")
        keyboard.append([InlineKeyboardButton(display, callback_data=f"topic_{topic_id}")])
    await query.edit_message_text("Choose a topic (or main chat):", reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='HTML')
    return CHOOSE_TOPIC

async def choose_topic(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    if not query.data.startswith("topic_"):
        await query.edit_message_text("Please pick a topic again using the buttons.")
        return CHOOSE_TOPIC
    topic_id = int(query.data.split("_")[1])
    context.user_data['topic_id'] = topic_id if topic_id != 0 else None
    return await show_recurrence_menu(update, context)

async def show_recurrence_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    weekdays = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
    keyboard = [[InlineKeyboardButton("One time only", callback_data="recurr_none")]] + \
               [[InlineKeyboardButton(f"Repeat every {day}", callback_data=f"recurr_weekly_{day}")] for day in weekdays]
    await query.edit_message_text(
        "Do you want this message to repeat?\n\nSelect a day to repeat weekly or pick 'One time only'.",
        reply_markup=InlineKeyboardMarkup(keyboard),
        parse_mode='HTML'
    )
    return CHOOSE_RECURRENCE

async def choose_recurrence(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    data = query.data
    if data == "recurr_none":
        context.user_data['recurrence'] = "none"
        return await ask_time_one_time(update, context)
    elif data.startswith("recurr_weekly_"):
        day = data.split("_")[-1]
        context.user_data['recurrence'] = "weekly"
        context.user_data['weekday'] = day
        return await ask_hour(update, context, weekly=True)

async def ask_hour(update: Update, context: ContextTypes.DEFAULT_TYPE, weekly: bool):
    query = update.callback_query
    await query.answer()
    hours = [f"{h:02d}" for h in range(7, 21)]
    keyboard, row = [], []
    for i, h in enumerate(hours, start=1):
        row.append(InlineKeyboardButton(h, callback_data=f"hour_{h}_{'w' if weekly else 'o'}"))
        if i % 6 == 0:
            keyboard.append(row); row = []
    if row: keyboard.append(row)
    keyboard.append([InlineKeyboardButton("Back", callback_data="back_recurr")])
    await query.edit_message_text("Pick <b>hour</b> (Asia/Manila):", reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='HTML')
    return CHOOSE_HOUR

async def choose_hour(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    data = query.data
    if data == "back_recurr":
        return await show_recurrence_menu(update, context)
    _, hour_str, mode = data.split("_")
    context.user_data['picked_hour'] = hour_str
    minutes = ["00", "15", "30", "45"]
    keyboard = [[InlineKeyboardButton(m, callback_data=f"min_{m}_{mode}") for m in minutes]]
    keyboard.append([InlineKeyboardButton("Back", callback_data=f"back_hour_{mode}")])
    await query.edit_message_text(f"Hour: <b>{hour_str}</b>\nNow pick <b>minutes</b>:", reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='HTML')
    return CHOOSE_MIN

async def choose_min(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    data = query.data
    if data.startswith("back_hour_"):
        mode = data.split("_")[-1]
        return await ask_hour(update, context, weekly=(mode == "w"))
    _, min_str, mode = data.split("_")
    hour_str = context.user_data.get('picked_hour', "09")
    at_time = f"{hour_str}:{min_str}"
    if mode == "w":
        context.user_data['recurr_time'] = at_time
        await query.edit_message_text(
            f"Weekly time: <b>{at_time}</b> (Asia/Manila)\nNow, please send your message text.\nYou can use Telegram formatting & emojis.",
            parse_mode='HTML'
        )
        return WRITE_MSG
    else:
        now_ph = datetime.now(PH_TZ)
        candidate = now_ph.replace(hour=int(hour_str), minute=int(min_str), second=0, microsecond=0)
        if candidate <= now_ph:
            candidate = (now_ph + timedelta(days=1)).replace(hour=int(hour_str), minute=int(min_str), second=0, microsecond=0)
        context.user_data['run_at'] = candidate.strftime('%Y-%m-%d %H:%M')
        await query.edit_message_text(
            f"Time set to: <b>{context.user_data['run_at']}</b> (Asia/Manila)\nNow, please send your message text.",
            parse_mode='HTML'
        )
        return WRITE_MSG

async def ask_time_one_time(update: Update, context: ContextTypes.DEFAULT_TYPE):
    now = datetime.now(PH_TZ)
    presets = [
        ("In 5 min", now + timedelta(minutes=5)),
        ("In 15 min", now + timedelta(minutes=15)),
        ("Tomorrow 9AM", (now + timedelta(days=1)).replace(hour=9, minute=0, second=0, microsecond=0)),
        ("Pick hour…", None)
    ]
    keyboard = []
    for label, dtv in presets:
        if dtv:
            keyboard.append([InlineKeyboardButton(label, callback_data=f"time_{dtv.strftime('%Y-%m-%d %H:%M')}")])
        else:
            keyboard.append([InlineKeyboardButton(label, callback_data="time_hourpick")])
    await update.callback_query.edit_message_text("Pick a time (Asia/Manila):",
                                                  reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='HTML')
    return CHOOSE_TIME

async def choose_time(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = getattr(update, "callback_query", None)
    if query:
        await query.answer()
        data = query.data
        if data == "time_hourpick":
            return await ask_hour(update, context, weekly=False)
        if data.startswith("time_"):
            val = data.replace("time_", "")
            context.user_data['run_at'] = val
            await query.edit_message_text(
                f"Time set to: <code>{html.escape(val)}</code> (Asia/Manila)\nNow, please send your message.\n\nYou can use Telegram's rich text formatting and emojis!",
                parse_mode='HTML')
            return WRITE_MSG
        return CHOOSE_TIME
    else:
        txt = update.message.text.strip()
        try:
            dt = datetime.strptime(txt, "%Y-%m-%d %H:%M")
            dt = PH_TZ.localize(dt)
            context.user_data['run_at'] = dt.strftime('%Y-%m-%d %H:%M')
            await update.message.reply_text("Time set! Now, please send your message text.", parse_mode='HTML')
            return WRITE_MSG
        except Exception:
            await update.message.reply_text("Invalid format. Please use <code>YYYY-MM-DD HH:MM</code>.", parse_mode='HTML')
            return CHOOSE_TIME

async def write_msg(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data['message'] = update.message.text
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
        msg += f"Topic: <code>{html.escape(tname[0])}</code>\n" if tname else f"Topic ID: <code>{topic}</code>\n"
    if recurrence == "weekly":
        weekday = context.user_data['weekday']
        at_time = context.user_data['recurr_time']
        msg += f"Repeats: Every {weekday} at {at_time} (Asia/Manila)\n"
    else:
        run_at = context.user_data.get('run_at')
        msg += f"Time: <code>{html.escape(run_at)}</code> (Asia/Manila)\n"
    msg += f"Message:\n<code>{html.escape(context.user_data['message'])}</code>\n\nConfirm?"
    keyboard = [[InlineKeyboardButton("✅ Confirm", callback_data="confirm_yes")],
                [InlineKeyboardButton("❌ Cancel", callback_data="confirm_no")]]
    await update.message.reply_text(msg, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='HTML')
    return CONFIRM

async def confirm(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    if query.data != "confirm_yes":
        await query.edit_message_text("Cancelled.", parse_mode='HTML')
        context.user_data.clear()
        return ConversationHandler.END

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
        weekday_abbrs = ['mon','tue','wed','thu','fri','sat','sun']
        day_idx = week_days.index(weekday)

        at_time_obj = dt_time.fromisoformat(at_time)
        candidate_dt = dt_now.replace(hour=at_time_obj.hour, minute=at_time_obj.minute, second=0, microsecond=0)
        days_ahead = (day_idx - dt_now.weekday()) % 7
        if days_ahead == 0 and candidate_dt <= dt_now:
            days_ahead = 7
        if days_ahead != 0:
            candidate_dt += timedelta(days=days_ahead)

        dt_utc = candidate_dt.astimezone(pytz.utc)
        cur.execute(
            "INSERT INTO schedules (target_chat_id, topic_id, user_id, message, run_at, recurrence, recurrence_data, entities) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            (group, topic, user_id, message, dt_utc.isoformat(), "weekly", f"{weekday}:{at_time}", entities)
        )
        conn.commit()
        schedule_id = cur.lastrowid

        scheduler.add_job(
            post_scheduled_message,
            'cron',
            day_of_week=weekday_abbrs[day_idx],
            hour=int(at_time.split(":")[0]),
            minute=int(at_time.split(":")[1]),
            args=[group, topic, message, schedule_id, "weekly"],
            id=str(schedule_id)
        )
        logger.info(f"Scheduled weekly job #{schedule_id} {weekday} {at_time} PH for chat {group} topic {topic}")
        await query.edit_message_text(f"✅ Weekly recurring message scheduled for every {weekday} at {at_time} (Asia/Manila)!", parse_mode='HTML')

    else:
        run_at = context.user_data['run_at']
        dt_ph = PH_TZ.localize(datetime.strptime(run_at, "%Y-%m-%d %H:%M"))
        dt_utc = dt_ph.astimezone(pytz.utc)
        cur.execute(
            "INSERT INTO schedules (target_chat_id, topic_id, user_id, message, run_at, recurrence, entities) "
            "VALUES (?, ?, ?, ?, ?, ?, ?)",
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

    context.user_data.clear()
    return ConversationHandler.END

async def cancel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("Scheduling cancelled.", parse_mode='HTML')
    context.user_data.clear()
    return ConversationHandler.END

# =========================
# Topic discovery + Thanks (silent)
# =========================
async def on_topic_created(update: Update, context: ContextTypes.DEFAULT_TYPE):
    m = update.effective_message
    chat = update.effective_chat
    topic_id = getattr(m, "message_thread_id", None)
    if topic_id is None:
        return
    name = m.forum_topic_created.name if m.forum_topic_created else None
    topic_name = name or f"Topic #{topic_id}"
    register_group(chat)
    register_topic(chat.id, topic_id, topic_name)

async def on_topic_edited(update: Update, context: ContextTypes.DEFAULT_TYPE):
    m = update.effective_message
    chat = update.effective_chat
    topic_id = getattr(m, "message_thread_id", None)
    if topic_id is None:
        return
    name = m.forum_topic_edited.name if m.forum_topic_edited else None
    if not name:
        return
    register_group(chat)
    register_topic(chat.id, topic_id, name)

async def register_chat_on_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_chat.type in ['group', 'supergroup']:
        register_group(update.effective_chat)
        topic_id = getattr(update.message, "message_thread_id", None)
        if topic_id is None or topic_id == 0:
            register_topic(update.effective_chat.id, 0, "Main chat")
        else:
            try:
                cur.execute("SELECT 1 FROM topics WHERE chat_id=? AND topic_id=?", (update.effective_chat.id, topic_id))
                if not cur.fetchone():
                    register_topic(update.effective_chat.id, topic_id, f"Topic #{topic_id}")
            except Exception as e:
                logger.error(f"Topic auto-register failed: {e}")

        # Standup thanks logic (silent)
        user = update.effective_user
        username = (user.username or "").lower()
        msg_topic_id = getattr(update.message, "message_thread_id", None)

        with sqlite3.connect(DB_PATH) as conn_local:
            cur_local = conn_local.cursor()

            if getattr(update.message, "reply_to_message", None):
                replied_id = update.message.reply_to_message.message_id
                cur_local.execute(
                    "SELECT id, username FROM standup_tracking "
                    "WHERE standup_message_id=? AND chat_id=? AND topic_id=? AND done=0 "
                    "AND datetime(deadline) > datetime('now') "
                    "ORDER BY id DESC",
                    (replied_id, update.effective_chat.id, msg_topic_id)
                )
                rows = cur_local.fetchall()
                matched_id = None
                for rid, ruser in rows:
                    if ruser == '*' or (username and ruser == username):
                        matched_id = rid
                        break
                if matched_id:
                    cur_local.execute("SELECT username FROM standup_tracking WHERE id=?", (matched_id,))
                    ruser = cur_local.fetchone()[0]
                    if ruser != '*':
                        cur_local.execute("UPDATE standup_tracking SET done=1 WHERE id=?", (matched_id,))
                        conn_local.commit()
                    try:
                        ack = await context.bot.send_message(chat_id=update.effective_chat.id, text="👍 Thanks for your standup!")
                        scheduler.add_job(
                            lambda: asyncio.run_coroutine_threadsafe(
                                context.bot.delete_message(chat_id=ack.chat_id, message_id=ack.message_id),
                                main_asyncio_loop
                            ),
                            'date',
                            run_date=datetime.now(pytz.utc) + timedelta(seconds=6)
                        )
                    except Exception as e:
                        logger.error(f"Failed to send ephemeral thanks: {e}")
                return

            cur_local.execute(
                "SELECT id, standup_message_id, username FROM standup_tracking "
                "WHERE chat_id=? AND topic_id=? AND done=0 AND datetime(deadline) > datetime('now') "
                "ORDER BY id DESC LIMIT 50",
                (update.effective_chat.id, msg_topic_id)
            )
            candidates = cur_local.fetchall()
            for rid, standup_msg_id, ruser in candidates:
                if update.message.message_id <= standup_msg_id:
                    continue
                if (ruser and ruser != '*' and username and username == ruser):
                    cur_local.execute("UPDATE standup_tracking SET done=1 WHERE id=?", (rid,))
                    conn_local.commit()
                    try:
                        ack = await context.bot.send_message(chat_id=update.effective_chat.id, text="👍 Thanks for your standup!")
                        scheduler.add_job(
                            lambda: asyncio.run_coroutine_threadsafe(
                                context.bot.delete_message(chat_id=ack.chat_id, message_id=ack.message_id),
                                main_asyncio_loop
                            ),
                            'date',
                            run_date=datetime.now(pytz.utc) + timedelta(seconds=6)
                        )
                    except Exception as e:
                        logger.error(f"Failed to send ephemeral thanks: {e}")
                    break
                elif ruser == '*':
                    try:
                        ack = await context.bot.send_message(chat_id=update.effective_chat.id, text="👍 Thanks for your standup!")
                        scheduler.add_job(
                            lambda: asyncio.run_coroutine_threadsafe(
                                context.bot.delete_message(chat_id=ack.chat_id, message_id=ack.message_id),
                                main_asyncio_loop
                            ),
                            'date',
                            run_date=datetime.now(pytz.utc) + timedelta(seconds=6)
                        )
                    except Exception as e:
                        logger.error(f"Failed to send ephemeral thanks: {e}")
                    break

async def whereami(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    msg_thread_id = update.message.message_thread_id if update.message and update.message.message_thread_id else None
    msg = f"Chat ID: <code>{chat_id}</code>"
    if msg_thread_id:
        msg += f"\nTopic (Thread) ID: <code>{msg_thread_id}</code>"
    else:
        msg += "\n(Not in a topic/thread right now.)"
    await update.message.reply_text(msg, parse_mode='HTML')

# =========================
# Error handler
# =========================
async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
    logger.exception("Unhandled error", exc_info=context.error)

# =========================
# Start background tasks after app is running
# =========================
async def _start_background_tasks(context):
    global main_asyncio_loop
    main_asyncio_loop = asyncio.get_running_loop()
    context.application.create_task(flood_control_worker())
    rehydrate_jobs()

async def on_app_startup(app: Application):
    # Schedule background startup for t=0 so it's after Application starts
    app.job_queue.run_once(_start_background_tasks, when=0)

# =========================
# Bot setup & run
# =========================
def run_telegram_bot():
    app_ = Application.builder().token(BOT_TOKEN).post_init(on_app_startup).build()

    conv_handler = ConversationHandler(
        entry_points=[CommandHandler("start", start), CallbackQueryHandler(schedule_start, pattern="^schedule_start$")],
        states={
            CHOOSE_GROUP: [CallbackQueryHandler(choose_group, pattern="^group_")],
            CHOOSE_TOPIC: [CallbackQueryHandler(choose_topic, pattern="^topic_")],
            CHOOSE_RECURRENCE: [CallbackQueryHandler(choose_recurrence, pattern="^recurr_")],
            CHOOSE_TIME: [CallbackQueryHandler(choose_time, pattern="^(time_|time_hourpick)$")],
            CHOOSE_HOUR: [CallbackQueryHandler(choose_hour, pattern="^(hour_|back_recurr)$")],
            CHOOSE_MIN: [CallbackQueryHandler(choose_min, pattern="^(min_|back_hour_)")],
            WRITE_MSG: [MessageHandler(filters.TEXT & ~filters.COMMAND, write_msg)],
            CONFIRM: [CallbackQueryHandler(confirm, pattern="^confirm_")]
        },
        fallbacks=[CommandHandler('cancel', cancel)],
        allow_reentry=True,
    )

    # Conversation + also a standalone /start (extra safety)
    app_.add_handler(conv_handler)
    app_.add_handler(CommandHandler("start", start))

    app_.add_handler(CommandHandler("help", help_command))
    app_.add_handler(CommandHandler("myschedules", myschedules))
    app_.add_handler(CommandHandler("whereami", whereami))
    app_.add_handler(CommandHandler("topicname", set_topic_name))
    app_.add_handler(CommandHandler("topics", topics_cmd))
    app_.add_handler(CallbackQueryHandler(cancel_schedule, pattern="^cancel_"))

    # DM admin commands
    app_.add_handler(CommandHandler("listgroups", listgroups_cmd))
    app_.add_handler(CommandHandler("listtopics", listtopics_cmd))
    app_.add_handler(CommandHandler("addtopic", addtopic_cmd))
    app_.add_handler(CommandHandler("renametopic", renametopic_cmd))
    app_.add_handler(CommandHandler("deltopic", deltopic_cmd))
    app_.add_handler(CommandHandler("listtopicsdm", listtopics_dm_cmd))
    app_.add_handler(CommandHandler("mytopics", listtopics_dm_cmd))
    app_.add_handler(CommandHandler("backupdb", backupdb_cmd))
    app_.add_handler(CommandHandler("restoredb", restoredb_cmd))

    # Silent auto-registration
    app_.add_handler(ChatMemberHandler(on_bot_membership, ChatMemberHandler.MY_CHAT_MEMBER))
    app_.add_handler(MessageHandler(filters.StatusUpdate.FORUM_TOPIC_CREATED, on_topic_created))
    app_.add_handler(MessageHandler(filters.StatusUpdate.FORUM_TOPIC_EDITED, on_topic_edited))

    # Catch-all (silent discovery & thanks)
    app_.add_handler(MessageHandler(filters.ALL, register_chat_on_message))

    app_.add_error_handler(error_handler)

    logger.info("Bot running...")
    app_.run_polling()

if __name__ == "__main__":
    threading.Thread(target=run_flask, daemon=True).start()
    run_telegram_bot()
