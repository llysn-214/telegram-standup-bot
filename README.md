# Telegram Standup Bot

Python Telegram bot for weekly standup reminders in a main group chat and per-topic prompts.

## What This Version Does

- Sends the General standup reminder every Monday at `09:55` PHT.
- Sends per-topic standup prompts every Monday at `10:00` PHT.
- Auto-updates the visible `Date:` line every week.
- Keeps General chat untagged.
- Tags only the people assigned to each topic.
- Fixes the previous double-send bug.
- Adds recovery commands for Render free-tier restarts.

## Do I Still Edit The DB?

The bot still uses SQLite (`schedules.db`) for schedules, groups, topics, and standup tracking.

You should **not** edit the DB every week anymore. The approved standup templates live in `main.py`, and the recurring schedules are created/refreshed with bot commands:

```text
/seedstandups
/recoverstandups
```

You may still use the DB/admin commands if group/topic mappings change, but weekly date and prompt updates should no longer be manual DB work.

## Required Environment Variables

```env
BOT_TOKEN=your_telegram_bot_token
ADMIN_IDS=7711088876
STANDUP_CHAT_ID=-1002827060937
```

Recommended for Render free tier:

```env
TOPIC_PRELOAD=[{"group_id":-1002827060937,"title":"PH Standups","topics":[{"id":5,"name":"Marketing"},{"id":3,"name":"Influence"},{"id":7,"name":"Social"},{"id":4,"name":"Aff&Agents"},{"id":9,"name":"Community"},{"id":6,"name":"Creatives"},{"id":2,"name":"Partnerships"},{"id":1853,"name":"Projects"}]}]
```

Optional:

```env
PORT=10000
DB_PATH=schedules.db
```

Important: if using a local `.env` file, keep `TOPIC_PRELOAD` on one line.

## Local Setup

```powershell
cd "C:\Users\Leo\Documents\Codex\2026-05-31\files-mentioned-by-the-user-telegram\repo\telegram-standup-bot-main"
python -m venv .venv
.venv\Scripts\python.exe -m pip install -r requirements.txt
```

Set env vars in PowerShell:

```powershell
$env:BOT_TOKEN="your_real_bot_token"
$env:ADMIN_IDS="7711088876"
$env:STANDUP_CHAT_ID="-1002827060937"
$env:DB_PATH="C:\Users\Leo\Documents\Codex\2026-05-31\files-mentioned-by-the-user-telegram\local_schedules.db"
$env:TOPIC_PRELOAD='[{"group_id":-1002827060937,"title":"PH Standups","topics":[{"id":5,"name":"Marketing"},{"id":3,"name":"Influence"},{"id":7,"name":"Social"},{"id":4,"name":"Aff&Agents"},{"id":9,"name":"Community"},{"id":6,"name":"Creatives"},{"id":2,"name":"Partnerships"},{"id":1853,"name":"Projects"}]}]'
```

Run:

```powershell
.venv\Scripts\python.exe main.py
```

Keep Render turned off while running locally so two bot pollers do not compete.

## Render Setup

Use the same required environment variables on Render:

- `BOT_TOKEN`
- `ADMIN_IDS`
- `STANDUP_CHAT_ID`
- `TOPIC_PRELOAD`

Free Render services may sleep or restart, and their filesystem can be temporary. This version is designed so you can recover by redeploying and running:

```text
/recoverstandups
```

## Standup Commands

Run these in DM with the bot as an admin.

```text
/standuphelp
```

Shows the command guide.

```text
/previewstandups [group_id]
```

Previews the General and per-topic messages in DM. Does not send to the group.

```text
/seedstandups [group_id]
```

Creates or refreshes the weekly recurring schedules:

- General: Monday `09:55` PHT
- Topics: Monday `10:00` PHT

```text
/recoverstandups [group_id] [dryrun] [force]
```

Recovery command for Render restarts or missed Monday sends.

Behavior:

- Before Monday `09:55` PHT: refreshes schedules only.
- Monday `09:55-10:00` PHT: sends General only.
- Monday after `10:00` PHT: sends General and all mapped topic prompts.
- Other days: refreshes next Monday's schedules only.
- `dryrun`: shows what would happen without sending.
- `force`: sends General and all mapped topic prompts immediately.

Safe test examples:

```text
/recoverstandups dryrun
/recoverstandups dryrun force
```

Live examples:

```text
/seedstandups
/recoverstandups
/recoverstandups force
```

## Group And Topic Commands

```text
/listgroups
/addgroup <group_id> <title>
/listtopics <group_id>
/addtopic <group_id> <topic_id> <name>
/bulkaddtopics <group_id> <JSON or mapping>
```

## Health Check

```text
/health
```

Shows uptime, DB status, schedule count, queue size, and next scheduled runs.

## Current Topic Mapping

```text
Marketing: 5
Influence: 3
Social: 7
Aff&Agents: 4
Community: 9
Creatives: 6
Partnerships: 2
Projects: 1853
```

## Notes

- The Projects prompt currently tags only `@leo_ph_projects`.
- `@ryze_ph_projects` has been removed from Projects and Community.
- Use `.venv\Scripts\python.exe main.py` locally, not global `python main.py`.
