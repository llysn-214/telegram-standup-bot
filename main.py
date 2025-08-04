import logging
import sqlite3
from datetime import datetime
from apscheduler.schedulers.background import BackgroundScheduler
from telegram import Update
from telegram.ext import ApplicationBuilder, CommandHandler, ContextTypes
import os

from flask import Flask
import threading

# --- FLASK SERVER FOR RENDER PINGING ---
app = Flask(__name__)

@app.route("/")
def home():
    return "OK", 200

def run_flask():
    app.run(host="0.0.0.0", port=10000)

# --- BOT TOKEN (from environment variable for security) ---
BOT_TOKEN = os.environ.get("BOT_TOKEN")

# --- DB SETUP ---
conn = sqlite3.connect('schedules.db', check_same_thread=False)
cur = conn.cursor()
cur.execute('''CREATE TABLE IF NOT EXISTS schedules (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    chat_id INTEGER,
    user_id INTEGER,
    message TEXT,
    run_at TEXT
)''')
conn.commit()

# --- LOGGING ---
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# --- SCHEDULER SETUP ---
scheduler = BackgroundScheduler()
scheduler.start()

# --- CORE FUNCTIONALITY ---

def post_scheduled_message(chat_id, message, schedule_id):
    try:
        # Needs new Application instance due to threading
        app_ = ApplicationBuilder().token(BOT_TOKEN).build()
        logger.info(f"Posting scheduled message: {message} to chat {chat_id}")
        app_.bot.send_message(chat_id=chat_id, text=message)
        # Remove from DB
        cur.execute("DELETE FROM schedules WHERE id=?", (schedule_id,))
        conn.commit()
    except Exception as e:
        logger.error(f"Error posting scheduled message: {e}")

async def schedule(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        args = context.args
        if len(args) < 3:
            await update.message.reply_text("Usage: /schedule YYYY-MM-DD HH:MM message")
            return

        # Parse date and time
        run_at_str = f"{args[0]} {args[1]}"
        run_at = datetime.strptime(run_at_str, "%Y-%m-%d %H:%M")

        # Get message
        message = ' '.join(args[2:])

        # Save to DB
        cur.execute(
            "INSERT INTO schedules (chat_id, user_id, message, run_at) VALUES (?, ?, ?, ?)",
            (update.effective_chat.id, update.effective_user.id, message, run_at.isoformat())
        )
        conn.commit()
        schedule_id = cur.lastrowid

        # Add to scheduler
        scheduler.add_job(
            post_scheduled_message,
            'date',
            run_date=run_at,
            args=[update.effective_chat.id, message, schedule_id],
            id=str(schedule_id)
        )

        await update.message.reply_text(f"Scheduled: \"{message}\" for {run_at.strftime('%Y-%m-%d %H:%M')}")

    except Exception as e:
        logger.error(e)
        await update.message.reply_text("Error: Please check your date/time format and try again.")

async def myschedules(update: Update, context: ContextTypes.DEFAULT_TYPE):
    cur.execute(
        "SELECT id, message, run_at FROM schedules WHERE user_id=? AND chat_id=? ORDER BY run_at ASC",
        (update.effective_user.id, update.effective_chat.id)
    )
    rows = cur.fetchall()
    if not rows:
        await update.message.reply_text("You have no scheduled messages.")
        return
    msg = "Your scheduled messages:\n"
    for row in rows:
        msg += f"- [{row[0]}] {row[1]} at {row[2]}\n"
    await update.message.reply_text(msg)

def main():
    app_ = ApplicationBuilder().token(BOT_TOKEN).build()
    app_.add_handler(CommandHandler("schedule", schedule))
    app_.add_handler(CommandHandler("myschedules", myschedules))
    logger.info("Bot running...")
    app_.run_polling()

if __name__ == "__main__":
    # Start Flask server in a separate thread for UptimeRobot
    threading.Thread(target=run_flask).start()
    main()
