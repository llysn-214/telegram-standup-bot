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

# Track app start for uptime
APP_STARTED_AT_UTC = datetime.now(pytz.utc)

def _fmt_uptime(start_utc: datetime) -> str:
    delta = datetime.now(pytz.utc) - start_utc
    days = delta.days
    hours, rem = divmod(delta.seconds, 3600)
    minutes, _ = divmod(rem, 60)
    parts = []
    if days: parts.append(f"{days}d")
    if hours: parts.append(f"{hours}h")
    if minutes or not parts: parts.append(f"{minutes}m")
    return " ".join(parts)

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
    run_at TEXT,                   -- ISO UTC
    recurrence TEXT DEFAULT 'none',-- 'none' or 'weekly'
    recurrence_data TEXT,          -- e.g. 'Monday:11:00'
    entities TEXT                  -- JSON saved entities
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
# Message date-line transformer & preload from env
# =========================
def _monday_of_week_ph(now_ph_dt: datetime) -> datetime:
    """Return the Monday (00:00) of the week for the given PH-local datetime."""
    monday_date = now_ph_dt.date() - timedelta(days=now_ph_dt.weekday())
    return datetime.combine(monday_date, datetime.min.time(), tzinfo=PH_TZ)

def _standup_monday_ph(now_ph_dt: datetime) -> datetime:
    """Return today's Monday if Monday, otherwise the upcoming Monday."""
    days_ahead = (0 - now_ph_dt.weekday()) % 7
    monday_date = now_ph_dt.date() + timedelta(days=days_ahead)
    return datetime.combine(monday_date, datetime.min.time(), tzinfo=PH_TZ)

def _format_month_d_year(d: datetime) -> str:
    return f"{d.strftime('%B')} {d.day}, {d.year}"

DATE_LINE_RE = re.compile(r'(?mi)^(Date:\s*)(.+)$')

def transform_message_date_line(original_text: str, now_ph_dt: datetime) -> tuple[str, bool]:
    monday = _standup_monday_ph(now_ph_dt)
    replacement = _format_month_d_year(monday)
    def _repl(m):
        return f"{m.group(1)}{replacement}"
    new_text, n = DATE_LINE_RE.subn(_repl, original_text or "")
    return new_text, (n > 0)

def render_scheduled_message(original_text: str, now_ph_dt: datetime | None = None) -> str:
    """Apply send-time substitutions while keeping DB templates stable."""
    now_ph_dt = now_ph_dt or datetime.now(PH_TZ)
    rendered, changed = transform_message_date_line(original_text or "", now_ph_dt)
    if changed:
        return rendered
    monday = _standup_monday_ph(now_ph_dt)
    return (original_text or "").replace("{{date}}", _format_month_d_year(monday))

def preload_from_env_topics():
    payload = os.environ.get("TOPIC_PRELOAD")
    if not payload:
        return
    try:
        data = json.loads(payload)
        if not isinstance(data, list):
            logger.error("TOPIC_PRELOAD must be a JSON array.")
            return
    except Exception as e:
        logger.error(f"Failed to parse TOPIC_PRELOAD JSON: {e}")
        return

    try:
        for entry in data:
            try:
                gid = int(entry.get("group_id"))
            except Exception:
                continue
            title = entry.get("title") or str(gid)
            try:
                cur.execute("INSERT OR REPLACE INTO groups (chat_id, title) VALUES (?, ?)", (gid, title))
            except Exception as e:
                logger.error(f"preload groups upsert failed: {e}")
            for t in entry.get("topics", []) or []:
                try:
                    tid = int(t.get("id"))
                    name = t.get("name") or f"Topic {tid}"
                    register_topic(gid, tid, name)
                except Exception as e:
                    logger.error(f"preload topic failed: {e}")
        conn.commit()
        logger.info("Preloaded groups/topics from TOPIC_PRELOAD ✅")
    except Exception as e:
        logger.error(f"preload_from_env_topics failed: {e}")

WEEKDAYS = ['Monday','Tuesday','Wednesday','Thursday','Friday','Saturday','Sunday']
WD_ABBR = ['mon','tue','wed','thu','fri','sat','sun']
WD_MAP = {w.lower(): w for w in WEEKDAYS}
WD_MAP.update({w[:3].lower(): w for w in WEEKDAYS})

def normalize_weekday(s: str) -> str | None:
    if not s: return None
    key = s.strip().lower()
    return WD_MAP.get(key)

def parse_hhmm(s: str) -> tuple[int,int] | None:
    try:
        t = dt_time.fromisoformat(s)
        return t.hour, t.minute
    except Exception:
        try:
            h, m = s.split(":")
            return int(h), int(m)
        except Exception:
            return None

STANDUP_WEEKDAY = "Monday"
STANDUP_GENERAL_TIME = "09:55"
STANDUP_TOPIC_TIME = "10:00"

STANDUP_GENERAL_MESSAGE = """Hello team!

Date: July 6, 2026

Please reply under your team's topic.

🗓 Priorities
What are your top 3 priorities this week?

🚀 Deliverables
What will be completed, launched, submitted, or moved forward by Friday?

🤝 Support Needed
What approvals, decisions, or help do you need?

🚧 Risks & Blockers
What's at risk of slipping, and why?"""

STANDUP_VERTICALS = [
    {
        "title": "🎥 Influence",
        "topic_names": ["Influence"],
        "tags": "@dima_influence @sofia_influence @julie_ph_influence",
        "body": """🗓 Priorities
Which influencers, campaigns, or negotiations are you actively driving this week?

🚀 Deliverables
Signed deals, content approvals, posts going live, campaign launches, or onboarding.

🤝 Support Needed
Approvals, budgets, assets, contracts, or coordination.

🚧 Risks & Blockers
What's preventing campaigns from moving?""",
    },
    {
        "title": "🤝 Affiliates & Agents",
        "topic_names": ["Aff&Agents", "Affiliates & Agents"],
        "tags": "@raz_marketing @brian_ph_aff @nina_ph_aff @trei_ph_aff",
        "body": """🗓 Priorities
Which affiliates, agents, or recruitment efforts are your focus this week?

🚀 Deliverables
New signings, onboarding, applications, payouts, reports, or optimizations.

🤝 Support Needed
Approvals, tracking, creatives, payouts, or technical support.

🚧 Risks & Blockers
What's slowing growth or partner activation?""",
    },
    {
        "title": "📈 Marketing",
        "topic_names": ["Marketing"],
        "tags": "@angelica_ph_marketing @mari_ph_marketing @toby_vn_marketing",
        "body": """🗓 Priorities
Which campaigns or growth initiatives are you executing this week?

🚀 Deliverables
Campaign launches, optimizations, reports, creatives, or experiments.

🤝 Support Needed
Budget, assets, approvals, or cross-team coordination.

🚧 Risks & Blockers
What's delaying execution or affecting performance?""",
    },
    {
        "title": "📱 Social",
        "topic_names": ["Social"],
        "tags": "@spuddy_ph_smm",
        "body": """🗓 Priorities
What content, campaigns, or platform initiatives are your focus this week?

🚀 Deliverables
Content published, calendars completed, reports, or major posts.

🤝 Support Needed
Assets, copy approval, or coordination.

🚧 Risks & Blockers
What's preventing content from going live?""",
    },
    {
        "title": "💬 Community",
        "topic_names": ["Community"],
        "tags": "@Cymon_PH_CommunityTL @raz_marketing @brian_ph_aff @raven_ph_community @ty_aff",
        "body": """🗓 Priorities
Which community initiatives, events, or engagement activities are you driving this week?

🚀 Deliverables
Events, activations, reports, feedback summaries, or community improvements.

🤝 Support Needed
Approvals, content, moderation support, or coordination.

🚧 Risks & Blockers
What's impacting community growth or engagement?""",
    },
    {
        "title": "🎨 Creatives",
        "topic_names": ["Creatives"],
        "tags": "@santi_ph_creatives @jom_ph_creatives",
        "body": """🗓 Priorities
Which creative requests are your priority this week?

🚀 Deliverables
Final assets, revisions, videos, copy, or production work.

🤝 Support Needed
Briefs, approvals, feedback, or references.

🚧 Risks & Blockers
What's delaying delivery?""",
    },
    {
        "title": "🤝 Partnerships",
        "topic_names": ["Partnerships"],
        "tags": "@frederich_ph_partnerships @angelica_ph_marketing",
        "body": """🗓 Priorities
Which partner opportunities or negotiations are you advancing this week?

🚀 Deliverables
Proposals, agreements, sponsorships, or partnership launches.

🤝 Support Needed
Approvals, pricing, legal, or materials.

🚧 Risks & Blockers
What's holding partner discussions or launches back?""",
    },
    {
        "title": "📋 Projects",
        "topic_names": ["Projects"],
        "tags": "@leo_ph_projects",
        "body": """🗓 Priorities
Which projects or workstreams are your main focus this week?

🚀 Deliverables
Milestones, launches, handoffs, or completed tasks.

🤝 Support Needed
Approvals, decisions, or dependency resolution.

🚧 Risks & Blockers
What's putting timelines at risk?""",
    },
]

def _standup_topic_message(vertical: dict) -> str:
    return (
        f"Hello {vertical['tags']}!\n\n"
        "Date: July 6, 2026\n\n"
        f"{vertical['title']}\n\n"
        f"{vertical['body']}\n\n"
        "---\n\n"
        "⚠️ Please reply to this message so I can tag this as submitted, thanks!"
    )

def _next_weekly_run_utc(weekday: str, hhmm: str) -> datetime:
    hour, minute = parse_hhmm(hhmm)
    now_ph = datetime.now(PH_TZ)
    day_idx = WEEKDAYS.index(weekday)
    candidate_dt = now_ph.replace(hour=hour, minute=minute, second=0, microsecond=0)
    days_ahead = (day_idx - now_ph.weekday()) % 7
    if days_ahead == 0 and candidate_dt <= now_ph:
        days_ahead = 7
    if days_ahead:
        candidate_dt += timedelta(days=days_ahead)
    return candidate_dt.astimezone(pytz.utc)

def _resolve_standup_chat_id(explicit_chat_id: int | None = None) -> int | None:
    if explicit_chat_id:
        return explicit_chat_id
    env_chat_id = os.environ.get("STANDUP_CHAT_ID")
    if env_chat_id and env_chat_id.strip().lstrip("-").isdigit():
        return int(env_chat_id)
    cur.execute("SELECT chat_id FROM groups ORDER BY title COLLATE NOCASE LIMIT 1")
    row = cur.fetchone()
    return int(row[0]) if row else None

def _topic_lookup(chat_id: int) -> dict[str, int]:
    cur.execute("SELECT topic_id, topic_name FROM topics WHERE chat_id=?", (chat_id,))
    return {(name or "").strip().lower(): int(tid) for tid, name in cur.fetchall()}

def _resolve_vertical_topic_id(vertical: dict, lookup: dict[str, int]) -> int | None:
    for name in vertical["topic_names"]:
        topic_id = lookup.get(name.strip().lower())
        if topic_id is not None:
            return topic_id
    return None

def _split_long_message(text: str, limit: int = 3900) -> list[str]:
    chunks = []
    current = ""
    for block in text.split("\n\n"):
        candidate = block if not current else f"{current}\n\n{block}"
        if len(candidate) > limit:
            if current:
                chunks.append(current)
            current = block
        else:
            current = candidate
    if current:
        chunks.append(current)
    return chunks

def _standup_targets(chat_id: int) -> tuple[list[dict], list[str]]:
    lookup = _topic_lookup(chat_id)
    missing = []
    targets = [
        {
            "label": "General",
            "topic_id": None,
            "hhmm": STANDUP_GENERAL_TIME,
            "message": STANDUP_GENERAL_MESSAGE,
        }
    ]
    for vertical in STANDUP_VERTICALS:
        topic_id = _resolve_vertical_topic_id(vertical, lookup)
        if topic_id is None:
            missing.append(", ".join(vertical["topic_names"]))
            continue
        targets.append(
            {
                "label": vertical["title"],
                "topic_id": topic_id,
                "hhmm": STANDUP_TOPIC_TIME,
                "message": _standup_topic_message(vertical),
            }
        )
    return targets, missing

def _replace_standup_schedule(target: dict, chat_id: int, user_id: int) -> dict:
    topic_id = target["topic_id"]
    hhmm = target["hhmm"]
    recurrence_data = f"{STANDUP_WEEKDAY}:{hhmm}"

    if topic_id is None:
        cur.execute(
            """SELECT id FROM schedules
               WHERE target_chat_id=? AND topic_id IS NULL
               AND recurrence='weekly' AND recurrence_data=?""",
            (chat_id, recurrence_data)
        )
    else:
        cur.execute(
            """SELECT id FROM schedules
               WHERE target_chat_id=? AND topic_id=?
               AND recurrence='weekly' AND recurrence_data=?""",
            (chat_id, topic_id, recurrence_data)
        )
    old_ids = [str(row[0]) for row in cur.fetchall()]
    for old_id in old_ids:
        try:
            scheduler.remove_job(old_id)
        except Exception:
            pass

    if topic_id is None:
        cur.execute(
            """DELETE FROM schedules
               WHERE target_chat_id=? AND topic_id IS NULL
               AND recurrence='weekly' AND recurrence_data=?""",
            (chat_id, recurrence_data)
        )
    else:
        cur.execute(
            """DELETE FROM schedules
               WHERE target_chat_id=? AND topic_id=?
               AND recurrence='weekly' AND recurrence_data=?""",
            (chat_id, topic_id, recurrence_data)
        )

    run_at_utc = _next_weekly_run_utc(STANDUP_WEEKDAY, hhmm)
    cur.execute(
        "INSERT INTO schedules (target_chat_id, topic_id, user_id, message, run_at, recurrence, recurrence_data, entities) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        (
            chat_id,
            topic_id,
            user_id,
            target["message"],
            run_at_utc.isoformat(),
            "weekly",
            recurrence_data,
            None,
        )
    )
    schedule_id = cur.lastrowid
    hour, minute = parse_hhmm(hhmm)
    scheduler.add_job(
        post_scheduled_message,
        'cron',
        day_of_week=WD_ABBR[WEEKDAYS.index(STANDUP_WEEKDAY)],
        hour=hour,
        minute=minute,
        args=[chat_id, topic_id, target["message"], schedule_id, "weekly"],
        id=str(schedule_id),
        replace_existing=True
    )
    return {**target, "schedule_id": schedule_id}

def _seed_standup_schedules(chat_id: int, user_id: int) -> tuple[list[dict], list[str]]:
    targets, missing = _standup_targets(chat_id)
    seeded = [_replace_standup_schedule(target, chat_id, user_id) for target in targets]
    conn.commit()
    return seeded, missing

def _parse_optional_chat_id_arg(args: list[str]) -> tuple[int | None, list[str]]:
    remaining = list(args or [])
    explicit_chat_id = None
    if remaining and remaining[0].lstrip("-").isdigit():
        explicit_chat_id = int(remaining.pop(0))
    return explicit_chat_id, remaining

def _is_group_chat(update: Update) -> bool:
    chat = update.effective_chat
    return bool(chat and chat.type in ('group', 'supergroup'))

async def block_if_group_non_admin(update: Update, context: ContextTypes.DEFAULT_TYPE, *, end_conv: bool = False):
    """
    If called from a group and the user is not in ADMIN_IDS, try deleting their command message (not bot messages)
    and block the action. Returns True or ConversationHandler.END when blocked.
    """
    if _is_group_chat(update) and not is_admin(update.effective_user.id):
        try:
            if update.effective_message and update.effective_message.from_user and \
               update.effective_message.from_user.id == update.effective_user.id:
                await context.bot.delete_message(
                    chat_id=update.effective_chat.id,
                    message_id=update.effective_message.message_id
                )
        except Exception:
            pass
        return ConversationHandler.END if end_conv else True
    return False

# =========================
# Job poster
# =========================
def post_scheduled_message(target_chat_id, topic_id, message, schedule_id, recurrence="none"):
    rendered_message = render_scheduled_message(message)

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
        # If the message is HTML, ignore any stored entities
        if looks_like_html(rendered_message):
            entities = None

    async def send_job():
        bot = Bot(BOT_TOKEN)
    
        send_kwargs = dict(
            chat_id=target_chat_id,
            text=rendered_message,
            disable_web_page_preview=False,
        )
    
        if topic_id:
            send_kwargs["message_thread_id"] = int(topic_id)
    
        # Fix A: message is stored as HTML, do NOT send entities
        send_kwargs["parse_mode"] = "HTML"
        send_kwargs.pop("entities", None)
    
        try:
            msg_obj = await bot.send_message(**send_kwargs)
        except Exception as e:
            # Safety net: if HTML is malformed, send plain text so it still fires
            logger.warning(f"HTML send failed for schedule {schedule_id}: {e}. Retrying as plain text.")
            send_kwargs.pop("parse_mode", None)
            msg_obj = await bot.send_message(**send_kwargs)
    
        logger.info(f"Sent message for schedule {schedule_id}")

        # Track standup replies window (2 hours)
        usernames = set(re.findall(r'@(\w+)', rendered_message))
        deadline = (datetime.now(pytz.utc) + timedelta(hours=2)).isoformat()
        if usernames:
            with sqlite3.connect(DB_PATH) as conn_local2:
                cur_local2 = conn_local2.cursor()
                for uname in usernames:
                    cur_local2.execute(
                        "INSERT INTO standup_tracking (schedule_id, chat_id, topic_id, standup_message_id, username, deadline) "
                        "VALUES (?, ?, ?, ?, ?, ?)",
                        (schedule_id, target_chat_id, topic_id, msg_obj.message_id, uname.lower(), deadline)
                    )
                # Always add a wildcard row to track "someone replied"
                cur_local2.execute(
                    "INSERT INTO standup_tracking (schedule_id, chat_id, topic_id, standup_message_id, username, deadline) "
                    "VALUES (?, ?, ?, ?, ?, ?)",
                    (schedule_id, target_chat_id, topic_id, msg_obj.message_id, '*', deadline)
                )
                conn_local2.commit()

            # Only tagged topic messages need follow-up checks.
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

            # Pending named users (not done)
            cur_local.execute(
                "SELECT username FROM standup_tracking "
                "WHERE schedule_id=? AND chat_id=? AND topic_id=? AND standup_message_id=? "
                "AND done=0 AND username!='*'",
                (schedule_id, chat_id, topic_id, standup_message_id)
            )
            users = [row[0] for row in cur_local.fetchall()]

            # Did anyone reply? (wildcard marked done when any reply observed)
            cur_local.execute(
                "SELECT done FROM standup_tracking "
                "WHERE schedule_id=? AND chat_id=? AND topic_id=? AND standup_message_id=? AND username='*' "
                "ORDER BY id DESC LIMIT 1",
                (schedule_id, chat_id, topic_id, standup_message_id)
            )
            wildcard_row = cur_local.fetchone()
            someone_replied = bool(wildcard_row and wildcard_row[0] == 1)

            if users:
                mention_text = " ".join([f"@{uname}" for uname in users])
                msg = f"⏰ Gentle reminder to send your standup when you can. {mention_text}"
                kwargs = dict(chat_id=chat_id, text=msg)
                if topic_id:
                    kwargs["message_thread_id"] = int(topic_id)
                await bot.send_message(**kwargs)
            elif not someone_replied:
                kwargs = dict(chat_id=chat_id, text="⏰ Gentle reminder: please send your standup when you can.")
                if topic_id:
                    kwargs["message_thread_id"] = int(topic_id)
                await bot.send_message(**kwargs)

            # Cleanup tracking rows for this standup
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
                wd_abbr = WD_ABBR[WEEKDAYS.index(weekday)] if weekday in WEEKDAYS else 'mon'
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
# Conversation states (buttons still available)
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
        await update.message.reply_text("Please DM me to run this command.")
        return False
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("Sorry, you’re not allowed to run this.")
        return False
    return True

# =========================
# Auto-registration (quiet) + DM admins group id
# =========================
async def on_bot_membership(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat = update.effective_chat
    if chat and chat.type in ('group', 'supergroup'):
        register_group(chat)
        for admin_id in ADMIN_IDS or []:
            try:
                await context.bot.send_message(
                    chat_id=admin_id,
                    text=f"📎 Bot added to {chat.title or chat.id}\nGroup ID: {chat.id}"
                )
            except Exception:
                pass

# =========================
# DM Admin utilities
# =========================
async def listgroups_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if await block_if_group_non_admin(update, context): return
    if not await require_dm_admin(update): return
    cur.execute("SELECT chat_id, title FROM groups ORDER BY title COLLATE NOCASE")
    rows = cur.fetchall() if hasattr(cur, "fetchall") else []
    if not rows:
        await update.message.reply_text("No groups known yet. Add me to a group as admin; I’ll auto-register silently.")
        return
    lines = [f"- {title} ({cid})" for cid, title in rows]
    await update.message.reply_text("Groups I know:\n" + "\n".join(lines))

async def addgroup_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if await block_if_group_non_admin(update, context): return
    if not await require_dm_admin(update): return
    if len(context.args) < 2:
        await update.message.reply_text("Usage: /addgroup <group_id> <Title…>"); return
    try:
        gid = int(context.args[0])
    except Exception:
        await update.message.reply_text("group_id must be a number."); return
    name = " ".join(context.args[1:]).strip() or str(gid)
    try:
        cur.execute("INSERT OR REPLACE INTO groups (chat_id, title) VALUES (?, ?)", (gid, name))
        conn.commit()
        await update.message.reply_text(f"Saved group {name} ({gid}).")
    except Exception as e:
        logger.error(f"/addgroup failed: {e}")
        await update.message.reply_text("Failed to save group.")

async def listtopics_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if await block_if_group_non_admin(update, context): return
    if not await require_dm_admin(update): return
    if not context.args:
        await update.message.reply_text("Usage: /listtopics <group_id>")
        return
    try:
        gid = int(context.args[0])
    except Exception:
        await update.message.reply_text("Invalid group_id.")
        return
    cur.execute("SELECT topic_id, topic_name FROM topics WHERE chat_id=? ORDER BY topic_id", (gid,))
    rows = cur.fetchall()
    if not rows:
        await update.message.reply_text("No topics saved for that group.")
        return
    lines = []
    for tid, tname in rows:
        label = tname or (f"Topic #{tid}" if tid else "Main chat")
        lines.append(f"- {label} ({tid})")
    await update.message.reply_text("Topics:\n" + "\n".join(lines))

async def addtopic_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if await block_if_group_non_admin(update, context): return
    if not await require_dm_admin(update): return
    if len(context.args) < 3:
        await update.message.reply_text("Usage: /addtopic <group_id> <topic_id> <Display Name…>")
        return
    try:
        gid = int(context.args[0]); tid = int(context.args[1])
    except Exception:
        await update.message.reply_text("group_id and topic_id must be numbers.")
        return
    name = " ".join(context.args[2:]).strip()
    if not name:
        await update.message.reply_text("Please provide a display name.")
        return
    cur.execute("INSERT OR IGNORE INTO groups (chat_id, title) VALUES (?, ?)", (gid, str(gid)))
    register_topic(gid, tid, name)
    await update.message.reply_text(f"Saved topic {name} for group {gid} (topic_id {tid}).")

async def bulkaddtopics_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if await block_if_group_non_admin(update, context): return
    if not await require_dm_admin(update): return
    if len(context.args) < 2:
        await update.message.reply_text(
            "Usage: /bulkaddtopics <group_id> <JSON or mapping>\n"
            "JSON example: [{\"id\":123,\"name\":\"Design\"},{\"id\":124,\"name\":\"Content\"}]\n"
            "Mapping: 123=Design;124=Content;200=Web"
        ); return
    try:
        gid = int(context.args[0])
    except Exception:
        await update.message.reply_text("group_id must be a number."); return

    payload = " ".join(context.args[1:]).strip()
    entries = []
    # Try JSON
    try:
        data = json.loads(payload)
        if isinstance(data, list):
            for item in data:
                tid = int(item.get("id"))
                name = str(item.get("name") or f"Topic {tid}")
                entries.append((tid, name))
    except Exception:
        # Try mapping: 123=Design;124=Content
        parts = [p for p in re.split(r'[;,\n]+', payload) if p.strip()]
        for p in parts:
            if "=" in p:
                k, v = p.split("=", 1)
                try:
                    entries.append((int(k.strip()), v.strip()))
                except Exception:
                    pass

    if not entries:
        await update.message.reply_text("Could not parse topics. Provide JSON list or mapping like 123=Design;124=Content"); return

    cur.execute("INSERT OR IGNORE INTO groups (chat_id, title) VALUES (?, ?)", (gid, str(gid)))
    for tid, name in entries:
        register_topic(gid, tid, name)

    await update.message.reply_text(f"Added/updated {len(entries)} topics for group {gid} ✅")

async def renametopic_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if await block_if_group_non_admin(update, context): return
    if not await require_dm_admin(update): return
    if len(context.args) < 3:
        await update.message.reply_text("Usage: /renametopic <group_id> <topic_id> <New Name…>")
        return
    try:
        gid = int(context.args[0]); tid = int(context.args[1])
    except Exception:
        await update.message.reply_text("group_id and topic_id must be numbers.")
        return
    name = " ".join(context.args[2:]).strip()
    if not name:
        await update.message.reply_text("Please provide a new name.")
        return
    register_topic(gid, tid, name)
    await update.message.reply_text(f"Renamed topic to {name} (group {gid}, topic {tid}).")

async def deltopic_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if await block_if_group_non_admin(update, context): return
    if not await require_dm_admin(update): return
    if len(context.args) < 2:
        await update.message.reply_text("Usage: /deltopic <group_id> <topic_id>")
        return
    try:
        gid = int(context.args[0]); tid = int(context.args[1])
    except Exception:
        await update.message.reply_text("group_id and topic_id must be numbers.")
        return
    cur.execute("DELETE FROM topics WHERE chat_id=? AND topic_id=?", (gid, tid))
    conn.commit()
    await update.message.reply_text(f"Deleted topic mapping for group {gid}, topic {tid}.")

async def listtopics_dm_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if await block_if_group_non_admin(update, context): return
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
            "No groups/topics known yet. Add me to a group or /addgroup, then /addtopic or /bulkaddtopics in DM."
        )
        return
    lines = []
    current_gid = None
    for gid, gtitle, tid, tname in rows:
        if gid != current_gid:
            if current_gid is not None:
                lines.append("")
            lines.append(f"{gtitle or gid}  ({gid})")
            current_gid = gid
        if tid is None:
            lines.append("  • (no topics added yet)")
        else:
            label = tname or (f"Topic #{tid}" if tid else "Main chat")
            lines.append(f"  • {label}  ({tid})")
    await update.message.reply_text("\n".join(lines))

# =========================
# Backup / Restore (DM admin only)
# =========================
async def backupdb_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if await block_if_group_non_admin(update, context): return
    if not await require_dm_admin(update): return
    try:
        await context.bot.send_document(
            chat_id=update.effective_user.id,
            document=open(DB_PATH, 'rb'),
            filename='schedules.db',
            caption='SQLite backup'
        )
        await update.message.reply_text("Backup sent to your DM ✅")
    except Exception as e:
        logger.error(f"Backup failed: {e}")
        await update.message.reply_text("Backup failed. Make sure you /start me in DM first.")

async def restoredb_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if await block_if_group_non_admin(update, context): return
    if not await require_dm_admin(update): return
    if not update.message.reply_to_message or not update.message.reply_to_message.document:
        await update.message.reply_text("Reply to a .db file with /restoredb.")
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
        await update.message.reply_text("Restore complete and jobs rehydrated ✅")
    except Exception as e:
        logger.error(f"Restore failed: {e}")
        await update.message.reply_text("Restore failed. Check the file and try again.")

# =========================
# Health (DM admin only)
# =========================
async def health_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if await block_if_group_non_admin(update, context): return
    if not await require_dm_admin(update): return

    try:
        cur.execute("SELECT COUNT(*) FROM groups"); groups_n = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM topics"); topics_n = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM schedules"); sched_n = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM standup_tracking WHERE done=0 AND datetime(deadline) > datetime('now')")
        open_windows = cur.fetchone()[0]
    except Exception:
        groups_n = topics_n = sched_n = open_windows = -1

    now_ph = datetime.now(PH_TZ)
    now_utc = datetime.now(pytz.utc)

    jobs = scheduler.get_jobs()
    next_lines = []
    for job in jobs[:10]:
        nrt = job.next_run_time
        if not nrt:
            continue
        ph = nrt.astimezone(PH_TZ).strftime("%Y-%m-%d %H:%M")
        utc = nrt.astimezone(pytz.utc).strftime("%Y-%m-%d %H:%M")

        label = ""
        try:
            sid = int(job.id)
        except Exception:
            sid = None
        if sid is not None:
            try:
                cur.execute("SELECT target_chat_id, topic_id, message, recurrence FROM schedules WHERE id=?", (sid,))
                row = cur.fetchone()
                if row:
                    g, t, msg, rec = row
                    prev = (msg or "").replace("\n", " ")
                    if len(prev) > 40:
                        prev = prev[:40] + "…"
                    label = f"chat {g}, topic {t or 0}, {rec}, “{prev}”"
            except Exception:
                pass

        next_lines.append(f"- id {job.id}: {ph} PH ({utc} UTC){(' — ' + label) if label else ''}")

    try:
        db_size = os.path.getsize(DB_PATH)
        db_mtime = datetime.fromtimestamp(os.path.getmtime(DB_PATH), tz=PH_TZ).strftime("%Y-%m-%d %H:%M")
    except Exception:
        db_size = -1
        db_mtime = "?"

    qsize = send_queue.qsize() if send_queue else 0

    text = (
        "🩺 Health Check\n"
        f"Now: {now_ph.strftime('%Y-%m-%d %H:%M:%S')} PH / {now_utc.strftime('%Y-%m-%d %H:%M:%S')} UTC\n"
        f"Uptime: {_fmt_uptime(APP_STARTED_AT_UTC)}\n"
        f"Groups: {groups_n} | Topics: {topics_n}\n"
        f"Schedules: {sched_n} | Open standup windows: {open_windows}\n"
        f"APScheduler jobs: {len(jobs)} | Send queue: {qsize}\n"
        f"DB: {DB_PATH} ({db_size} bytes, mtime PH {db_mtime})\n"
        "Next runs:\n" + ("\n".join(next_lines) if next_lines else "(none)")
    )
    await update.message.reply_text(text)

# =========================
# Standup template seeding
# =========================
async def seedstandups_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if await block_if_group_non_admin(update, context): return
    if not await require_dm_admin(update): return

    explicit_chat_id, remaining = _parse_optional_chat_id_arg(context.args)
    if remaining:
        await update.message.reply_text("Usage: /seedstandups [group_id]")
        return

    chat_id = _resolve_standup_chat_id(explicit_chat_id)
    if chat_id is None:
        await update.message.reply_text("No standup group found. Use /addgroup first, or set STANDUP_CHAT_ID.")
        return

    try:
        seeded, missing = _seed_standup_schedules(chat_id, update.effective_user.id)
        lines = [f"Seeded {len(seeded)} weekly standup schedules for chat {chat_id}:"]
        lines.extend([
            f"- {item['label']}: {STANDUP_WEEKDAY} {item['hhmm']} PH, topic {item['topic_id'] or 0}"
            for item in seeded
        ])
        if missing:
            lines.append("")
            lines.append("Missing topic mappings:")
            lines.extend([f"- {name}" for name in missing])
        await update.message.reply_text("\n".join(lines))
    except Exception as e:
        logger.exception("seedstandups_cmd failed", exc_info=e)
        await update.message.reply_text("Failed to seed standups. Check logs and topic mappings.")

async def previewstandups_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if await block_if_group_non_admin(update, context): return
    if not await require_dm_admin(update): return

    explicit_chat_id = None
    if context.args:
        try:
            explicit_chat_id = int(context.args[0])
        except Exception:
            await update.message.reply_text("Usage: /previewstandups [group_id]")
            return

    chat_id = _resolve_standup_chat_id(explicit_chat_id)
    lookup = _topic_lookup(chat_id) if chat_id is not None else {}

    messages = ["GENERAL\n\n" + render_scheduled_message(STANDUP_GENERAL_MESSAGE)]
    for vertical in STANDUP_VERTICALS:
        topic_id = _resolve_vertical_topic_id(vertical, lookup)
        label = f"{vertical['title']} (topic {topic_id if topic_id is not None else 'missing'})"
        messages.append(label + "\n\n" + render_scheduled_message(_standup_topic_message(vertical)))

    preview = "\n\n====================\n\n".join(messages)
    for chunk in _split_long_message(preview):
        await update.message.reply_text(chunk)

async def standuphelp_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if await block_if_group_non_admin(update, context): return
    if not await require_dm_admin(update): return

    text = (
        "Standup Bot Commands\n\n"
        "/previewstandups [group_id]\n"
        "Preview the General and per-topic standup messages in DM without sending to the group.\n\n"
        "/seedstandups [group_id]\n"
        "Create or refresh the weekly Monday schedules: General at 09:55 PH and topic prompts at 10:00 PH.\n\n"
        "/recoverstandups [group_id] [dryrun] [force]\n"
        "Recover after a Render restart or missed Monday send. Use dryrun to see what would happen. Use force to send General and all mapped topic prompts immediately.\n\n"
        "/health\n"
        "Show uptime, DB status, active schedules, and next run times.\n\n"
        "/listgroups\n"
        "Show groups the bot knows.\n\n"
        "/listtopics <group_id>\n"
        "Show saved topic mappings for a group.\n\n"
        "/addgroup <group_id> <title>\n"
        "Manually save a group.\n\n"
        "/addtopic <group_id> <topic_id> <name>\n"
        "Manually save a topic mapping.\n\n"
        "Typical flow after deploy/redeploy:\n"
        "1. /standuphelp\n"
        "2. /previewstandups\n"
        "3. /recoverstandups dryrun\n"
        "4. /seedstandups"
    )
    await update.message.reply_text(text)

async def recoverstandups_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if await block_if_group_non_admin(update, context): return
    if not await require_dm_admin(update): return

    explicit_chat_id, options = _parse_optional_chat_id_arg(context.args)
    option_set = {opt.lower() for opt in options}
    allowed_options = {"dryrun", "force"}
    invalid_options = option_set - allowed_options
    if invalid_options:
        await update.message.reply_text("Usage: /recoverstandups [group_id] [dryrun] [force]")
        return

    chat_id = _resolve_standup_chat_id(explicit_chat_id)
    if chat_id is None:
        await update.message.reply_text("No standup group found. Use /addgroup first, or set STANDUP_CHAT_ID.")
        return

    dryrun = "dryrun" in option_set
    force = "force" in option_set
    now_ph = datetime.now(PH_TZ)
    general_cutoff = dt_time.fromisoformat(STANDUP_GENERAL_TIME)
    topic_cutoff = dt_time.fromisoformat(STANDUP_TOPIC_TIME)

    try:
        seeded, missing = _seed_standup_schedules(chat_id, update.effective_user.id)
        due = []
        reason = ""

        if force:
            due = seeded
            reason = "force option: sending all standup messages now"
        elif now_ph.weekday() != WEEKDAYS.index(STANDUP_WEEKDAY):
            reason = f"today is {WEEKDAYS[now_ph.weekday()]}; schedules refreshed for next {STANDUP_WEEKDAY}"
        elif now_ph.time() < general_cutoff:
            reason = f"before {STANDUP_GENERAL_TIME} PH; schedules refreshed and will fire normally"
        elif now_ph.time() < topic_cutoff:
            due = [item for item in seeded if item["topic_id"] is None]
            reason = f"between {STANDUP_GENERAL_TIME} and {STANDUP_TOPIC_TIME} PH; catching up General only"
        else:
            due = seeded
            reason = f"after {STANDUP_TOPIC_TIME} PH; catching up General and all topic prompts now"

        if not dryrun:
            for item in due:
                post_scheduled_message(
                    chat_id,
                    item["topic_id"],
                    item["message"],
                    item["schedule_id"],
                    "weekly"
                )

        lines = [
            f"Recovery checked at {now_ph.strftime('%Y-%m-%d %H:%M')} PH.",
            f"Mode: {'dry run' if dryrun else 'live'}",
            f"Result: {reason}.",
            f"Seeded/refreshed schedules: {len(seeded)}",
        ]
        if due:
            lines.append("")
            lines.append("Messages " + ("that would send now:" if dryrun else "queued to send now:"))
            lines.extend([
                f"- {item['label']} -> topic {item['topic_id'] or 0}"
                for item in due
            ])
        else:
            lines.append("")
            lines.append("No immediate messages needed.")
        if missing:
            lines.append("")
            lines.append("Missing topic mappings:")
            lines.extend([f"- {name}" for name in missing])

        await update.message.reply_text("\n".join(lines))
    except Exception as e:
        logger.exception("recoverstandups_cmd failed", exc_info=e)
        await update.message.reply_text("Failed to recover standups. Check logs and topic mappings.")

# =========================
# COMMAND-ONLY SCHEDULING (DM or group for admins)
# =========================
async def weekly_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if await block_if_group_non_admin(update, context): return
    m = update.message
    is_dm = (update.effective_chat.type == 'private')

    try:
        if is_dm:
            # /weekly <group_id> <topic_id|0> <Weekday> <HH:MM> <message…>
            if len(context.args) < 5:
                await m.reply_text("Usage (DM): /weekly <group_id> <topic_id|0> <Weekday> <HH:MM> <message…>")
                return
            group_id = int(context.args[0])
            topic_id = int(context.args[1]) or None
            weekday = normalize_weekday(context.args[2])
            hhmm = parse_hhmm(context.args[3])
            message = " ".join(context.args[4:]).strip()
        else:
            # In group/topic (admin only): /weekly <Weekday> <HH:MM> <message…>
            if len(context.args) < 3:
                await m.reply_text("Usage: /weekly <Weekday> <HH:MM> <message…>")
                return
            group_id = update.effective_chat.id
            topic_id = getattr(m, "message_thread_id", None)
            weekday = normalize_weekday(context.args[0])
            hhmm = parse_hhmm(context.args[1])
            message = " ".join(context.args[2:]).strip()

        if not weekday:
            await m.reply_text("Invalid weekday. Example: Monday, Tue, fri"); return
        if not hhmm:
            await m.reply_text("Invalid time. Use HH:MM (24h). Example: 11:00"); return
        if not message:
            await m.reply_text("Please include the message text to send."); return

        hour, minute = hhmm
        dt_now = datetime.now(PH_TZ)
        day_idx = WEEKDAYS.index(weekday)
        candidate_dt = dt_now.replace(hour=hour, minute=minute, second=0, microsecond=0)
        days_ahead = (day_idx - dt_now.weekday()) % 7
        if days_ahead == 0 and candidate_dt <= dt_now:
            days_ahead = 7
        if days_ahead != 0:
            candidate_dt += timedelta(days=days_ahead)
        dt_utc = candidate_dt.astimezone(pytz.utc)

        cur.execute(
            "INSERT INTO schedules (target_chat_id, topic_id, user_id, message, run_at, recurrence, recurrence_data, entities) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            (group_id, topic_id, m.from_user.id, message, dt_utc.isoformat(), "weekly", f"{weekday}:{hour:02d}:{minute:02d}", None)
        )
        conn.commit()
        schedule_id = cur.lastrowid

        scheduler.add_job(
            post_scheduled_message,
            'cron',
            day_of_week=WD_ABBR[day_idx],
            hour=hour,
            minute=minute,
            args=[group_id, topic_id, message, schedule_id, "weekly"],
            id=str(schedule_id)
        )

        ack = await m.reply_text(f"✅ Weekly scheduled: every {weekday} {hour:02d}:{minute:02d} (Asia/Manila).")
        if not is_dm:
            try:
                scheduler.add_job(
                    lambda: asyncio.run_coroutine_threadsafe(
                        context.bot.delete_message(chat_id=ack.chat_id, message_id=ack.message_id),
                        main_asyncio_loop
                    ),
                    'date',
                    run_date=datetime.now(pytz.utc) + timedelta(seconds=6)
                )
                scheduler.add_job(
                    lambda: asyncio.run_coroutine_threadsafe(
                        context.bot.delete_message(chat_id=m.chat_id, message_id=m.message_id),
                        main_asyncio_loop
                    ),
                    'date',
                    run_date=datetime.now(pytz.utc) + timedelta(seconds=6)
                )
            except Exception:
                pass

    except Exception as e:
        logger.exception("weekly_cmd failed", exc_info=e)
        await m.reply_text("Sorry, failed to schedule. Check your arguments and try again.")

async def once_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if await block_if_group_non_admin(update, context): return
    m = update.message
    is_dm = (update.effective_chat.type == 'private')

    try:
        if is_dm:
            # /once <group_id> <topic_id|0> <YYYY-MM-DD> <HH:MM> <message…>
            if len(context.args) < 5:
                await m.reply_text("Usage (DM): /once <group_id> <topic_id|0> <YYYY-MM-DD> <HH:MM> <message…>")
                return
            group_id = int(context.args[0])
            topic_id = int(context.args[1]) or None
            date_str = context.args[2]
            time_str = context.args[3]
            message = " ".join(context.args[4:]).strip()
        else:
            # In group/topic (admin only): /once <YYYY-MM-DD> <HH:MM> <message…>
            if len(context.args) < 3:
                await m.reply_text("Usage: /once <YYYY-MM-DD> <HH:MM> <message…>")
                return
            group_id = update.effective_chat.id
            topic_id = getattr(m, "message_thread_id", None)
            date_str = context.args[0]
            time_str = context.args[1]
            message = " ".join(context.args[2:]).strip()

        try:
            dt_local = PH_TZ.localize(datetime.strptime(f"{date_str} {time_str}", "%Y-%m-%d %H:%M"))
        except Exception:
            await m.reply_text("Invalid date/time. Use YYYY-MM-DD HH:MM (24h)."); return
        if not message:
            await m.reply_text("Please include the message text to send."); return

        dt_utc = dt_local.astimezone(pytz.utc)
        cur.execute(
            "INSERT INTO schedules (target_chat_id, topic_id, user_id, message, run_at, recurrence, entities) "
            "VALUES (?, ?, ?, ?, ?, ?, ?)",
            (group_id, topic_id, m.from_user.id, message, dt_utc.isoformat(), "none", None)
        )
        conn.commit()
        schedule_id = cur.lastrowid

        scheduler.add_job(
            post_scheduled_message,
            'date',
            run_date=dt_utc,
            args=[group_id, topic_id, message, schedule_id, "none"],
            id=str(schedule_id)
        )

        ack = await m.reply_text("✅ One-time schedule set.")
        if not is_dm:
            try:
                scheduler.add_job(
                    lambda: asyncio.run_coroutine_threadsafe(
                        context.bot.delete_message(chat_id=ack.chat_id, message_id=ack.message_id),
                        main_asyncio_loop
                    ),
                    'date',
                    run_date=datetime.now(pytz.utc) + timedelta(seconds=6)
                )
                scheduler.add_job(
                    lambda: asyncio.run_coroutine_threadsafe(
                        context.bot.delete_message(chat_id=m.chat_id, message_id=m.message_id),
                        main_asyncio_loop
                    ),
                    'date',
                    run_date=datetime.now(pytz.utc) + timedelta(seconds=6)
                )
            except Exception:
                pass

    except Exception as e:
        logger.exception("once_cmd failed", exc_info=e)
        await m.reply_text("Sorry, failed to schedule. Check your arguments and try again.")

# =========================
# Buttons flow (optional; kept)
# =========================
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if await block_if_group_non_admin(update, context): return
    keyboard = [[InlineKeyboardButton("📅 Schedule Message", callback_data="schedule_start")]]
    await update.message.reply_text(
        "Welcome! Use /weekly and /once from DM to keep things private.\nYou can still use the button if you want.",
        reply_markup=InlineKeyboardMarkup(keyboard),
    )

async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if await block_if_group_non_admin(update, context): return
    help_text = (
        "<b>DM scheduling (recommended):</b>\n"
        "/weekly &lt;group_id&gt; &lt;topic_id|0&gt; Monday 11:00 Message…\n"
        "/once   &lt;group_id&gt; &lt;topic_id|0&gt; 2025-08-20 09:00 Message…\n\n"
        "<b>Provision via DM (no GC noise):</b>\n"
        "/addgroup &lt;group_id&gt; &lt;Title…&gt;\n"
        "/addtopic &lt;group_id&gt; &lt;topic_id&gt; &lt;Name…&gt;\n"
        "/bulkaddtopics &lt;group_id&gt; &lt;JSON or 123=Name;124=Name&gt;\n"
        "/listgroups, /listtopics &lt;group_id&gt;, /listtopicsdm\n\n"
        "<b>Other:</b>\n"
        "/myschedules, /backupdb, /restoredb, /health\n"
        "Timezone: Asia/Manila"
    )
    await update.message.reply_text(help_text, parse_mode="HTML")

async def set_topic_name(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if await block_if_group_non_admin(update, context): return
    if update.effective_chat.type not in ['group', 'supergroup']:
        await update.message.reply_text("Use this inside a group/topic.")
        return
    topic_id = getattr(update.message, "message_thread_id", None)
    if topic_id is None:
        await update.message.reply_text("Use this inside a topic (not main chat).")
        return
    name = " ".join(context.args).strip()
    if not name:
        await update.message.reply_text("Usage: /topicname Actual Topic Name")
        return
    register_group(update.effective_chat)
    register_topic(update.effective_chat.id, topic_id, name)
    await update.message.reply_text(f"Topic name for ID {topic_id} set to: {name}")

async def topics_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if await block_if_group_non_admin(update, context): return
    chat = update.effective_chat
    if chat.type not in ['group', 'supergroup']:
        await update.message.reply_text("Use this inside a group.")
        return
    cur.execute("SELECT topic_id, topic_name FROM topics WHERE chat_id=? ORDER BY topic_id", (chat.id,))
    rows = cur.fetchall()
    if not rows:
        await update.message.reply_text("No topics known yet. Use /addtopic via DM to provision silently.")
        return
    lines = []
    for tid, tname in rows:
        display = tname or (f"Topic #{tid}" if tid else "Main chat")
        lines.append(f"- {display} ({tid})")
    await update.message.reply_text("Known topics:\n" + "\n".join(lines))

async def myschedules(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if await block_if_group_non_admin(update, context): return
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
        preview = (msg[:60].replace('\n', ' ') + ("..." if len(msg) > 60 else ""))
        text = f"Group: {group_name}\n"
        text += f"Topic: {tname if tname else 'Main chat'}\n"
        if recurrence == "weekly":
            parts = (recurrence_data or "").split(":")
            weekday = parts[0]
            at_time = ":".join(parts[1:]) if len(parts) > 1 else ""
            text += f"Repeats: Every {weekday} {at_time} Asia/Manila\n"
        else:
            text += f"When: {dt_ph.strftime('%Y-%m-%d %H:%M')} Asia/Manila\n"
        text += f"Message: {preview}"
        keyboard = [[InlineKeyboardButton("❌ Cancel", callback_data=f"cancel_{schedule_id}")]]
        await update.message.reply_text(text, reply_markup=InlineKeyboardMarkup(keyboard))

async def cancel_schedule(update: Update, context: ContextTypes.DEFAULT_TYPE):
    r = await block_if_group_non_admin(update, context, end_conv=True)
    if r: return r
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
    await query.edit_message_text("❌ Scheduled message cancelled.")

# =========================
# Schedule flow (buttons; now 24h + 5-minute increments)
# =========================
async def schedule_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    r = await block_if_group_non_admin(update, context, end_conv=True)
    if r: return r
    query = update.callback_query
    await query.answer()
    cur.execute("SELECT chat_id, title FROM groups ORDER BY title COLLATE NOCASE")
    groups = cur.fetchall()
    if not groups:
        await query.edit_message_text(
            "No groups registered yet. Use /addgroup in DM or add me to a group (I’ll DM you the ID)."
        )
        return ConversationHandler.END
    keyboard = [[InlineKeyboardButton(f"{title}", callback_data=f"group_{chat_id}")] for chat_id, title in groups]
    await query.edit_message_text("Which group?", reply_markup=InlineKeyboardMarkup(keyboard))
    return CHOOSE_GROUP

async def choose_group(update: Update, context: ContextTypes.DEFAULT_TYPE):
    r = await block_if_group_non_admin(update, context, end_conv=True)
    if r: return r
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
    await query.edit_message_text("Choose a topic (or main chat):", reply_markup=InlineKeyboardMarkup(keyboard))
    return CHOOSE_TOPIC

async def choose_topic(update: Update, context: ContextTypes.DEFAULT_TYPE):
    r = await block_if_group_non_admin(update, context, end_conv=True)
    if r: return r
    query = update.callback_query
    await query.answer()
    if not query.data.startswith("topic_"):
        await query.edit_message_text("Please pick a topic again using the buttons.")
        return CHOOSE_TOPIC
    topic_id = int(query.data.split("_")[1])
    context.user_data['topic_id'] = topic_id if topic_id != 0 else None
    return await show_recurrence_menu(update, context)

async def show_recurrence_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    r = await block_if_group_non_admin(update, context, end_conv=True)
    if r: return r
    query = update.callback_query
    await query.answer()
    weekdays = WEEKDAYS
    keyboard = [[InlineKeyboardButton("One time only", callback_data="recurr_none")]] + \
               [[InlineKeyboardButton(f"Repeat every {day}", callback_data=f"recurr_weekly_{day}")] for day in weekdays]
    await query.edit_message_text(
        "Do you want this message to repeat?\n\nSelect a day to repeat weekly or pick 'One time only'.",
        reply_markup=InlineKeyboardMarkup(keyboard),
    )
    return CHOOSE_RECURRENCE

async def choose_recurrence(update: Update, context: ContextTypes.DEFAULT_TYPE):
    r = await block_if_group_non_admin(update, context, end_conv=True)
    if r: return r
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
    r = await block_if_group_non_admin(update, context, end_conv=True)
    if r: return r
    query = update.callback_query
    await query.answer()
    # 00–23 hours
    hours = [f"{h:02d}" for h in range(0, 24)]
    keyboard, row = [], []
    for i, h in enumerate(hours, start=1):
        row.append(InlineKeyboardButton(h, callback_data=f"hour_{h}_{'w' if weekly else 'o'}"))
        if i % 6 == 0:
            keyboard.append(row); row = []
    if row: keyboard.append(row)
    keyboard.append([InlineKeyboardButton("Back", callback_data="back_recurr")])
    await query.edit_message_text("Pick hour (Asia/Manila):", reply_markup=InlineKeyboardMarkup(keyboard))
    return CHOOSE_HOUR

async def choose_hour(update: Update, context: ContextTypes.DEFAULT_TYPE):
    r = await block_if_group_non_admin(update, context, end_conv=True)
    if r: return r
    query = update.callback_query
    await query.answer()
    data = query.data
    if data == "back_recurr":
        return await show_recurrence_menu(update, context)
    _, hour_str, mode = data.split("_")  # mode: 'w' weekly, 'o' one-time
    context.user_data['picked_hour'] = hour_str

    # minutes every 5: 00..55
    minutes = [f"{m:02d}" for m in range(0, 60, 5)]
    keyboard, row = [], []
    for i, m in enumerate(minutes, start=1):
        row.append(InlineKeyboardButton(m, callback_data=f"min_{m}_{mode}"))
        if i % 6 == 0:
            keyboard.append(row); row = []
    if row:
        keyboard.append(row)
    keyboard.append([InlineKeyboardButton("Back", callback_data=f"back_hour_{mode}")])

    await query.edit_message_text(f"Hour: {hour_str}\nNow pick minutes:", reply_markup=InlineKeyboardMarkup(keyboard))
    return CHOOSE_MIN

async def choose_min(update: Update, context: ContextTypes.DEFAULT_TYPE):
    r = await block_if_group_non_admin(update, context, end_conv=True)
    if r: return r
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
            f"Weekly time: {at_time} (Asia/Manila)\nNow, please send your message text.\nYou can use Telegram formatting & emojis."
        )
        return WRITE_MSG
    else:
        now_ph = datetime.now(PH_TZ)
        candidate = now_ph.replace(hour=int(hour_str), minute=int(min_str), second=0, microsecond=0)
        if candidate <= now_ph:
            candidate = (now_ph + timedelta(days=1)).replace(hour=int(hour_str), minute=int(min_str), second=0, microsecond=0)
        context.user_data['run_at'] = candidate.strftime('%Y-%m-%d %H:%M')
        await query.edit_message_text(
            f"Time set to: {context.user_data['run_at']} (Asia/Manila)\nNow, please send your message text."
        )
        return WRITE_MSG

async def ask_time_one_time(update: Update, context: ContextTypes.DEFAULT_TYPE):
    r = await block_if_group_non_admin(update, context, end_conv=True)
    if r: return r
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
                                                  reply_markup=InlineKeyboardMarkup(keyboard))
    return CHOOSE_TIME

async def choose_time(update: Update, context: ContextTypes.DEFAULT_TYPE):
    r = await block_if_group_non_admin(update, context, end_conv=True)
    if r: return r
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
                f"Time set to: {html.escape(val)} (Asia/Manila)\nNow, please send your message.\n\nYou can use Telegram's rich text formatting and emojis!"
            )
            return WRITE_MSG
        return CHOOSE_TIME
    else:
        txt = update.message.text.strip()
        try:
            dt = datetime.strptime(txt, "%Y-%m-%d %H:%M")
            dt = PH_TZ.localize(dt)
            context.user_data['run_at'] = dt.strftime('%Y-%m-%d %H:%M')
            await update.message.reply_text("Time set! Now, please send your message text.")
            return WRITE_MSG
        except Exception:
            await update.message.reply_text("Invalid format. Please use YYYY-MM-DD HH:MM.")
            return CHOOSE_TIME

async def write_msg(update: Update, context: ContextTypes.DEFAULT_TYPE):
    r = await block_if_group_non_admin(update, context, end_conv=True)
    if r: return r
    # Store HTML version to avoid entity/UTF-16 offset issues on send
    context.user_data['message'] = update.message.text_html or html.escape(update.message.text or "")
    
    # Keep a plain-text preview for the confirmation screen
    context.user_data['message_preview'] = update.message.text or ""
    
    # We no longer store entities
    context.user_data['entities'] = None
    group = context.user_data.get('target_chat_id')
    topic = context.user_data.get('topic_id')
    recurrence = context.user_data.get('recurrence', "none")
    msg = f"Ready to schedule:\nGroup: {group}\n"
    if topic:
        cur.execute("SELECT topic_name FROM topics WHERE chat_id = ? AND topic_id = ?", (group, topic))
        tname = cur.fetchone()
        msg += f"Topic: {tname[0]}\n" if tname else f"Topic ID: {topic}\n"
    if recurrence == "weekly":
        weekday = context.user_data['weekday']
        at_time = context.user_data['recurr_time']
        msg += f"Repeats: Every {weekday} at {at_time} (Asia/Manila)\n"
    else:
        run_at = context.user_data.get('run_at')
        msg += f"Time: {run_at} (Asia/Manila)\n"
    msg += f"Message:\n{context.user_data.get('message_preview','')}\n\nConfirm?"
    keyboard = [[InlineKeyboardButton("✅ Confirm", callback_data="confirm_yes")],
                [InlineKeyboardButton("❌ Cancel", callback_data="confirm_no")]]
    await update.message.reply_text(msg, reply_markup=InlineKeyboardMarkup(keyboard))
    return CONFIRM

async def confirm(update: Update, context: ContextTypes.DEFAULT_TYPE):
    r = await block_if_group_non_admin(update, context, end_conv=True)
    if r: return r
    query = update.callback_query
    await query.answer()
    if query.data != "confirm_yes":
        await query.edit_message_text("Cancelled.")
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
        day_idx = WEEKDAYS.index(weekday)

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
            day_of_week=WD_ABBR[day_idx],
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
        await query.edit_message_text("✅ One-time scheduled message set!")

    context.user_data.clear()
    return ConversationHandler.END

async def cancel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    r = await block_if_group_non_admin(update, context, end_conv=True)
    if r: return r
    await update.message.reply_text("Scheduling cancelled.")
    context.user_data.clear()
    return ConversationHandler.END

# =========================
# Topic discovery + Thanks (silent) + mark wildcard done on any reply
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
    # auto-register silently
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

        # Standup thanks + mark replies
        user = update.effective_user
        username = (user.username or "").lower()
        msg_topic_id = getattr(update.message, "message_thread_id", None)

        with sqlite3.connect(DB_PATH) as conn_local:
            cur_local = conn_local.cursor()

            # 1) Direct reply to the bot's standup message
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
                    # mark the matched row done
                    cur_local.execute("UPDATE standup_tracking SET done=1 WHERE id=?", (matched_id,))
                    # also mark wildcard row done
                    cur_local.execute(
                        "UPDATE standup_tracking SET done=1 "
                        "WHERE schedule_id=(SELECT schedule_id FROM standup_tracking WHERE id=?) "
                        "AND chat_id=? AND topic_id=? AND standup_message_id=? AND username='*'",
                        (matched_id, update.effective_chat.id, msg_topic_id, replied_id)
                    )
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
                return  # handled

            # 2) Not a reply: detect posts in the same topic after the standup message and before deadline
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
                    cur_local.execute(
                        "UPDATE standup_tracking SET done=1 "
                        "WHERE schedule_id=(SELECT schedule_id FROM standup_tracking WHERE id=?) "
                        "AND chat_id=? AND topic_id=? AND standup_message_id=? AND username='*'",
                        (rid, update.effective_chat.id, msg_topic_id, standup_msg_id)
                    )
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

async def whereami(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if await block_if_group_non_admin(update, context): return
    chat_id = update.effective_chat.id
    msg_thread_id = update.message.message_thread_id if update.message and update.message.message_thread_id else None
    msg = f"Chat ID: {chat_id}"
    if msg_thread_id:
        msg += f"\nTopic (Thread) ID: {msg_thread_id}"
    else:
        msg += "\n(Not in a topic/thread right now.)"
    await update.message.reply_text(msg)

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
    preload_from_env_topics()
    rehydrate_jobs()

async def on_app_startup(app: Application):
    app.job_queue.run_once(_start_background_tasks, when=0)

# =========================
# Bot setup & run
# =========================
def run_telegram_bot():
    app_ = Application.builder().token(BOT_TOKEN).post_init(on_app_startup).build()

    # Conversation (buttons flow)
    conv_handler = ConversationHandler(
        entry_points=[
            CommandHandler("start", start),
            CallbackQueryHandler(schedule_start, pattern="^schedule_start$")
        ],
        states={
            CHOOSE_GROUP: [CallbackQueryHandler(choose_group, pattern="^group_")],
            CHOOSE_TOPIC: [CallbackQueryHandler(choose_topic, pattern="^topic_")],
            CHOOSE_RECURRENCE: [CallbackQueryHandler(choose_recurrence, pattern="^recurr_")],
            CHOOSE_TIME: [CallbackQueryHandler(choose_time, pattern=r"^(time_.+|time_hourpick)$")],
            CHOOSE_HOUR: [CallbackQueryHandler(choose_hour, pattern=r"^(hour_.+|back_recurr)$")],
            CHOOSE_MIN: [CallbackQueryHandler(choose_min, pattern=r"^(min_.+|back_hour_.+)$")],
            WRITE_MSG: [MessageHandler(filters.TEXT & ~filters.COMMAND, write_msg)],
            CONFIRM: [CallbackQueryHandler(confirm, pattern="^confirm_")]
        },
        fallbacks=[CommandHandler('cancel', cancel)],
        allow_reentry=True,
    )

    # Conversation + core DM/group commands
    app_.add_handler(conv_handler)
    app_.add_handler(CommandHandler("start", start))
    app_.add_handler(CommandHandler("help", help_command))
    app_.add_handler(CommandHandler("myschedules", myschedules))
    app_.add_handler(CommandHandler("whereami", whereami))
    app_.add_handler(CommandHandler("topicname", set_topic_name))
    app_.add_handler(CommandHandler("topics", topics_cmd))

    # Command-only scheduling
    app_.add_handler(CommandHandler("weekly", weekly_cmd))
    app_.add_handler(CommandHandler("once", once_cmd))

    # Provision & admin (DM)
    app_.add_handler(CommandHandler("addgroup", addgroup_cmd))
    app_.add_handler(CommandHandler("addtopic", addtopic_cmd))
    app_.add_handler(CommandHandler("bulkaddtopics", bulkaddtopics_cmd))
    app_.add_handler(CommandHandler("listgroups", listgroups_cmd))
    app_.add_handler(CommandHandler("listtopics", listtopics_cmd))
    app_.add_handler(CommandHandler("listtopicsdm", listtopics_dm_cmd))
    app_.add_handler(CommandHandler("mytopics", listtopics_dm_cmd))  # alias
    app_.add_handler(CommandHandler("renametopic", renametopic_cmd))
    app_.add_handler(CommandHandler("deltopic", deltopic_cmd))
    app_.add_handler(CommandHandler("backupdb", backupdb_cmd))
    app_.add_handler(CommandHandler("restoredb", restoredb_cmd))
    app_.add_handler(CommandHandler("health", health_cmd))
    app_.add_handler(CommandHandler("seedstandups", seedstandups_cmd))
    app_.add_handler(CommandHandler("previewstandups", previewstandups_cmd))
    app_.add_handler(CommandHandler("standuphelp", standuphelp_cmd))
    app_.add_handler(CommandHandler("recoverstandups", recoverstandups_cmd))

    # Silent auto-registration & admin DM on add/remove
    app_.add_handler(ChatMemberHandler(on_bot_membership, ChatMemberHandler.MY_CHAT_MEMBER))

    # Topic discovery from system events
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
