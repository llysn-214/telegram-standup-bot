import logging
import sqlite3
import os
from datetime import datetime
from apscheduler.schedulers.background import BackgroundScheduler
from telegram import Update, Chat, Message
from telegram.ext import ApplicationBuilder, CommandHandler, ContextTypes

BOT_TOKEN = os.environ.get("BOT_TOKEN")

# --- DB SETUP ---
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

# --- LOGGING ---
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# --- SCHEDULER SETUP ---
scheduler = BackgroundScheduler()
scheduler.start()

# --- HELPER: Group Registry ---
def register_group(chat: Chat):
    try:
        cur.execute("INSERT OR REPLACE INTO groups (chat_id, title) VALUES (?, ?)", (chat.id, chat.title or "Unnamed"))
        conn.commit()
    except Exception as e:
        logger.error(f"Failed to register group: {e}")

# --- SCHEDULING FUNCTION ---
def post_scheduled_message(target_chat_id, topic_id, message, schedule_id):
    try:
        app_ = ApplicationBuilder().token(BOT_TOKEN).build()
        logger.info(f"Posting scheduled message: {message} to chat {target_chat_id}, topic {topic_id}")
        if topic_id:
            app_.bot.send_message(chat_id=target_chat_id, text=message, message_thread_id=int(topic_id))
        else:
            app_.bot.send_message(chat_id=target_chat_id, text=message)
        # Remove from DB
        cur.execute("DELETE FROM schedules WHERE id=?", (schedule_id,))
        conn.commit()
    except Exception as e:
        logger.error(f"Error posting scheduled message: {e}")

# --- /SCHEDULE COMMAND ---
async def schedule(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        args = context.args
        if len(args) < 4:
            await update.message.reply_text(
                "Usage:\n"
                "/schedule <group_id|group_name> [topic_id] YYYY-MM-DD HH:MM message"
            )
            return

        # Parse group, topic, date, and time
        group_ref = args[0]
        topic_id = None
        idx = 1

        # Try to parse topic ID if next argument is integer-like
        try:
            maybe_topic = int(args[1])
            topic_id = maybe_topic
            idx += 1
        except ValueError:
            topic_id = None

        date_str = args[idx]
        time_str = args[idx + 1]
        run_at_str = f"{date_str} {time_str}"
        run_at = datetime.strptime(run_at_str, "%Y-%m-%d %H:%M")
        message = ' '.join(args[idx + 2:])

        # Find the group chat_id by name or ID
        if group_ref.startswith('-'):
            target_chat_id = int(group_ref)
        else:
            cur.execute("SELECT chat_id FROM groups WHERE title LIKE ?", (group_ref,))
            res = cur.fetchone()
            if not res:
                await update.message.reply_text("Could not find group with that name. Try /groups to see available groups.")
                return
            target_chat_id = int(res[0])

        # Save to DB
        cur.execute(
            "INSERT INTO schedules (target_chat_id, topic_id, user_id, message, run_at) VALUES (?, ?, ?, ?, ?)",
            (target_chat_id, topic_id, update.effective_user.id, message, run_at.isoformat())
        )
        conn.commit()
        schedule_id = cur.lastrowid

        # Add to scheduler
        scheduler.add_job(
            post_scheduled_message,
            'date',
            run_date=run_at,
            args=[target_chat_id, topic_id, message, schedule_id],
            id=str(schedule_id)
        )

        info = f"Scheduled: \"{message}\" for {run_at.strftime('%Y-%m-%d %H:%M')}\nGroup: {group_ref}"
        if topic_id:
            info += f"\nTopic ID: {topic_id}"
        await update.message.reply_text(info)

    except Exception as e:
        logger.error(e)
        await update.message.reply_text("Error: Please check your format and try again.")

# --- /MYSCHEDULES COMMAND ---
async def myschedules(update: Update, context: ContextTypes.DEFAULT_TYPE):
    cur.execute(
        "SELECT id, target_chat_id, topic_id, message, run_at FROM schedules WHERE user_id=? ORDER BY run_at ASC",
        (update.effective_user.id,)
    )
    rows = cur.fetchall()
    if not rows:
        await update.message.reply_text("You have no scheduled messages.")
        return
    msg = "Your scheduled messages:\n"
    for row in rows:
        msg += f"- [{row[0]}] {row[3]} at {row[4]} (chat_id: {row[1]}"
        if row[2]:
            msg += f", topic_id: {row[2]}"
        msg += ")\n"
    await update.message.reply_text(msg)

# --- /GROUPS COMMAND ---
async def groups(update: Update, context: ContextTypes.DEFAULT_TYPE):
    cur.execute("SELECT chat_id, title FROM groups")
    rows = cur.fetchall()
    if not rows:
        await update.message.reply_text("No groups registered yet. Add this bot to your groups as admin, then send a message there to register.")
        return
    msg = "Groups:\n"
    for row in rows:
        msg += f"- {row[1]} (ID: {row[0]})\n"
    msg += "\nUse the chat_id or group name in /schedule. For topics, use topic_id as 2nd argument if needed."
    await update.message.reply_text(msg)

# --- /WHEREAMI COMMAND ---
async def whereami(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    msg_thread_id = update.message.message_thread_id if update.message and update.message.message_thread_id else None
    msg = f"Chat ID: `{chat_id}`"
    if msg_thread_id:
        msg += f"\nTopic (Thread) ID: `{msg_thread_id}`"
    else:
        msg += "\n(Not in a topic/thread right now.)"
    await update.message.reply_text(msg, parse_mode='Markdown')

# --- REGISTER GROUPS WHEN BOT SEES MESSAGES IN GROUPS ---
async def register_chat_on_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_chat.type in ['group', 'supergroup']:
        register_group(update.effective_chat)

def main():
    app_ = ApplicationBuilder().token(BOT_TOKEN).build()
    app_.add_handler(CommandHandler("schedule", schedule))
    app_.add_handler(CommandHandler("myschedules", myschedules))
    app_.add_handler(CommandHandler("groups", groups))
    app_.add_handler(CommandHandler("whereami", whereami))
    app_.add_handler(CommandHandler("start", register_chat_on_message))
    app_.add_handler(CommandHandler("help", register_chat_on_message))
    app_.add_handler(CommandHandler("register", register_chat_on_message))
    app_.add_handler(CommandHandler("test", register_chat_on_message))
    logger.info("Bot running...")
    app_.run_polling()

if __name__ == "__main__":
    main()
