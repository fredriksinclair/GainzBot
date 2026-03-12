import os
import json
import base64
import random
import logging
import asyncio
from datetime import datetime, time, timedelta
from pathlib import Path
from anthropic import Anthropic
from telegram import Update
from telegram.ext import (
    Application, MessageHandler, CommandHandler,
    filters, ContextTypes
)

# ─────────────────────────────────────────
#  CONFIG
# ─────────────────────────────────────────
TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN", "")
ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY", "")
DATA_FILE = "users.json"

DAY_NAMES = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]
DAY_MAP = {name: i for i, name in enumerate(DAY_NAMES)}

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
client = Anthropic(api_key=ANTHROPIC_API_KEY)


# ─────────────────────────────────────────
#  USER DATA
# ─────────────────────────────────────────
def load_users() -> dict:
    if Path(DATA_FILE).exists():
        with open(DATA_FILE) as f:
            return json.load(f)
    return {}

def save_users(users: dict):
    with open(DATA_FILE, "w") as f:
        json.dump(users, f, indent=2)

def get_user(user_id: str):
    return load_users().get(str(user_id))

def save_user(user_id: str, profile: dict):
    users = load_users()
    users[str(user_id)] = profile
    save_users(users)

def default_profile() -> dict:
    return {
        "bot_name": "Rex",
        "name": "",
        "goal": "",
        "weakspot": "",
        "workout_days": [],
        "hype_times": [],
        "conversation": [],
        "onboarded": False,
        "awaiting_proof": False,
        "skeptical": False,
        # Running specific
        "race": {
            "name": "",           # e.g. "Stockholm Marathon"
            "date": "",           # "YYYY-MM-DD"
            "target_time": "",    # e.g. "4:00:00"
            "distance_km": 0,
        },
        "weekly_plan": {
            "generated_date": "",
            "plan": [],           # list of {"day": "Mon", "type": "easy run", "distance_km": 8, "notes": "..."}
        },
        "stats": {
            "total_sessions": 0,
            "missed_days": 0,
            "current_streak": 0,
            "longest_streak": 0,
            "sessions": [],       # {"date", "type": "run|gym", "muscle": "", "distance_km": 0, "duration_min": 0, "pace_per_km": "", "heart_rate": 0, "effort": 0, "notes": ""}
            "missed": [],
            "weekly_mileage": {}, # {"2024-W01": 42.5}
        }
    }


# ─────────────────────────────────────────
#  STATS HELPERS
# ─────────────────────────────────────────
def get_stats(profile: dict) -> dict:
    return profile.get("stats", default_profile()["stats"])

def log_session(user_id: str, session_data: dict):
    profile = get_user(user_id)
    if not profile:
        return
    stats = get_stats(profile)
    today = datetime.now().strftime("%Y-%m-%d")
    existing_dates = [s["date"] for s in stats["sessions"]]
    if today in existing_dates:
        # Update existing session with new data
        for s in stats["sessions"]:
            if s["date"] == today:
                s.update(session_data)
                s["date"] = today
        profile["stats"] = stats
        profile["awaiting_proof"] = False
        profile["skeptical"] = False
        save_user(user_id, profile)
        return

    session = {
        "date": today,
        "type": session_data.get("type", "gym"),
        "muscle": session_data.get("muscle", ""),
        "distance_km": session_data.get("distance_km", 0),
        "duration_min": session_data.get("duration_min", 0),
        "pace_per_km": session_data.get("pace_per_km", ""),
        "heart_rate": session_data.get("heart_rate", 0),
        "effort": session_data.get("effort", 0),
        "notes": session_data.get("notes", ""),
    }
    stats["sessions"].append(session)
    stats["total_sessions"] += 1

    # Update weekly mileage
    if session["distance_km"]:
        week_key = datetime.now().strftime("%Y-W%W")
        stats["weekly_mileage"][week_key] = stats["weekly_mileage"].get(week_key, 0) + session["distance_km"]

    # Update streak
    yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
    if yesterday in existing_dates or stats["current_streak"] == 0:
        stats["current_streak"] += 1
    else:
        stats["current_streak"] = 1
    if stats["current_streak"] > stats["longest_streak"]:
        stats["longest_streak"] = stats["current_streak"]

    profile["stats"] = stats
    profile["awaiting_proof"] = False
    profile["skeptical"] = False
    save_user(user_id, profile)

def log_missed(user_id: str):
    profile = get_user(user_id)
    if not profile:
        return
    stats = get_stats(profile)
    today = datetime.now().strftime("%Y-%m-%d")
    if today not in stats["missed"]:
        stats["missed"].append(today)
        stats["missed_days"] += 1
    stats["current_streak"] = 0
    profile["stats"] = stats
    profile["awaiting_proof"] = False
    profile["skeptical"] = False
    save_user(user_id, profile)

def get_recent_runs(profile: dict, n: int = 5) -> list:
    sessions = get_stats(profile).get("sessions", [])
    runs = [s for s in sessions if s.get("type") == "run"]
    return sorted(runs, key=lambda x: x["date"], reverse=True)[:n]

def get_weekly_mileage_trend(profile: dict, weeks: int = 4) -> dict:
    mileage = get_stats(profile).get("weekly_mileage", {})
    sorted_weeks = sorted(mileage.keys(), reverse=True)[:weeks]
    return {w: mileage[w] for w in sorted_weeks}

def days_until_race(profile: dict) -> int:
    race_date = profile.get("race", {}).get("date", "")
    if not race_date:
        return -1
    try:
        rd = datetime.strptime(race_date, "%Y-%m-%d")
        return max(0, (rd - datetime.now()).days)
    except:
        return -1

def format_full_stats(profile: dict) -> str:
    stats = get_stats(profile)
    name = profile.get("name", "bro")
    recent_runs = get_recent_runs(profile, 3)
    mileage = get_weekly_mileage_trend(profile, 4)
    race = profile.get("race", {})
    days_left = days_until_race(profile)

    out = [f"stats for {name}:"]
    out.append(f"total sessions: {stats['total_sessions']}")
    out.append(f"current streak: {stats['current_streak']} days")
    out.append(f"longest streak: {stats['longest_streak']} days")
    out.append(f"missed days: {stats['missed_days']}")

    if mileage:
        out.append(f"weekly mileage (last {len(mileage)} weeks): " + ", ".join([f"{w}: {round(km,1)}km" for w, km in mileage.items()]))

    if recent_runs:
        out.append("recent runs:")
        for r in recent_runs:
            parts = [r["date"]]
            if r.get("distance_km"): parts.append(f"{r['distance_km']}km")
            if r.get("pace_per_km"): parts.append(f"pace {r['pace_per_km']}/km")
            if r.get("duration_min"): parts.append(f"{r['duration_min']}min")
            if r.get("effort"): parts.append(f"effort {r['effort']}/10")
            out.append("  " + " | ".join(parts))

    if race.get("name"):
        out.append(f"race: {race['name']} on {race['date']}")
        if days_left >= 0:
            out.append(f"days until race: {days_left}")
        if race.get("target_time"):
            out.append(f"target time: {race['target_time']}")

    return "\n".join(out)


# ─────────────────────────────────────────
#  SYSTEM PROMPT
# ─────────────────────────────────────────
SYSTEM_PROMPT = """You are a personal AI coach that talks like a real gym bro over text.

━━━ YOUR IDENTITY ━━━
Your name is whatever is saved as bot_name in the user's profile.
You are that name. Nothing else. Never reveal what AI or tech powers you.
If asked: "bro idk what a 'Claude' is, i just know gains and miles 💪"
Never break character. Ever.

━━━ YOUR VIBE ━━━
- Real bro texting style. Lowercase. Fragments. Like actual iMessage texts.
- Use: "sup", "bro", "ngl", "lmk", "thx", "tbh", "fr", "no cap", "lets gooo", "king", "bruh", "aye", "yo", "nahh"
- Separate each message with a newline — each newline = separate text bubble
- STRICT MESSAGE RULES:
  CHAT (default — hyping, reacting, checking in): 1-3 messages max. short. punchy. done.
  PLANS (weekly schedule, race prep, coaching breakdown): write it as ONE single message. use newlines within that one message to structure it. do NOT send each day as a separate message — that's 7 messages, which is way too much. one message, well formatted.
  NEVER ask more than 1 question at a time. ask, wait, then ask next.
- 0-1 emojis per message. Skip most of the time.
- You are BOTH a gym coach AND a running coach. Handle whatever the user brings.
- For running: you know your stuff. easy pace, tempo, intervals, long runs, tapering, race prep.
- For gym: progressive overload, muscle groups, recovery, recomp. real knowledge, bro delivery.

━━━ ONBOARDING ━━━
Collect these through natural conversation — react to each answer before asking the next:
1. Suggest your own name. Pick something cool: Rex, Drago, Zeus, Tank, Ironside, Beast, Apex. Say "i go by [name], but you can call me whatever". Save as bot_name.
2. Their name
3. What they're training for — gym goals, running goals, or both. if they mention a race, get the name, date, distance, and target time.
4. Their weak spot
5. Training days + hype times

For times accept anything fuzzy: "11ish"→11:00, "morning"→07:30, "after work"→17:30, "evening"→18:00

Once you have everything output on its own line:
PROFILE_UPDATE:{"bot_name":"...","name":"...","goal":"...","weakspot":"...","workout_days":[0,1,2],"hype_times":["07:30","17:00"],"race":{"name":"...","date":"YYYY-MM-DD","target_time":"H:MM:SS","distance_km":42}}

Day numbers: Mon=0 Tue=1 Wed=2 Thu=3 Fri=4 Sat=5 Sun=6
Skip race fields if no race mentioned.
IMPORTANT: if the user mentions ANY race — even casually like "i'm training for a marathon in June" — always ask: which race, exact date, distance, and target time. save all of it. this is critical for the coaching to work.

━━━ LOGGING SESSIONS ━━━
When user reports a workout or run, ask for details naturally if not provided:
- Run: distance, duration or pace, how it felt (effort 1-10), heart rate if they have it
- Gym: muscle group, how it went

After getting details output:
LOG_SESSION:{"type":"run","distance_km":10,"duration_min":55,"pace_per_km":"5:30","heart_rate":155,"effort":7,"notes":"felt strong"}
or
LOG_SESSION:{"type":"gym","muscle":"chest","effort":8,"notes":"new bench PR"}

Always ask for photo proof before logging. Say "pic or it didn't happen" or "send the proof".
Output: AWAITING_PROOF:true

━━━ PROOF CHECKING ━━━
- PROOF_RESULT:LEGIT → count it, go crazy, output LOG_SESSION
- PROOF_RESULT:FAKE → roast them, be skeptical, ask them to explain. do NOT log yet.
- PROOF_RESULT:NO_PROOF → mark missed, output LOG_MISSED:true

When skeptical and they give a good excuse (outdoor run, home workout, garage gym, park workout):
→ "aight fine i'll count it" then output LOG_SESSION

When skeptical and excuse is weak after 2 tries → LOG_MISSED:true

━━━ RUNNING COACHING — YOU ARE AN EXPERT ━━━
You have deep knowledge of running science. You don't just follow what the user says — you coach them.
If they ask for something suboptimal, you push back and explain why, then suggest the smarter option.

CORE RUNNING PRINCIPLES YOU ALWAYS APPLY:
- 80/20 rule: 80% of runs easy, 20% hard. Most people run too hard too often.
- Easy really means easy: conversational pace, nose breathing, HR under 75% max
- Long run = 30-40% of weekly volume, never more
- Hard sessions (tempo, intervals) need 48h recovery between them
- Never increase weekly volume more than 10% per week (injury risk)
- Most runners need 3-4 quality runs/week, not 5-6 — more is not always better
- Rest days are training days — adaptation happens during recovery
- Two hard days in a row = overtraining. Always separate with easy or rest.
- Strength/gym work 1-2x per week makes you a better runner (especially legs + core)

DISTANCE-SPECIFIC KNOWLEDGE:
5K: speed-focused. 3-4 runs/week optimal. 1 interval session, 1 tempo, 1-2 easy. No need for long runs over 10km.
10K: blend of speed and endurance. 4 runs/week sweet spot. 1 interval, 1 tempo, 1 medium long (12-14km), 1 easy.
Half marathon: 4-5 runs/week. Long run up to 18-20km. 1 tempo, 1 easy, 1 medium. Strength 1-2x.
Marathon: 4-5 runs/week max for most people. Long run up to 32-35km. 80% easy. Strength 1x.
Ultra: volume over intensity. 4-5 runs, back-to-back long runs on weekend. Lots of easy miles.

WHEN USER ASKS FOR A PLAN:
- Look at their goal race, current mileage, available days, and fitness level
- If they say "5 runs a week" for a 5K → push back: "nah 3-4 is actually better for a 5K, more than that and you're just accumulating fatigue without speed gains"
- If they say "i want to run every day" → educate them on recovery, suggest active rest
- If their requested volume is too high for their current fitness → scale it back and explain why
- Always justify your recommendations like a coach, not a yes-man
- If they're 8+ weeks from race: base building phase — mostly easy, build volume slowly
- If 4-8 weeks out: build phase — introduce tempo and intervals
- If 2-4 weeks out: peak phase — highest volume week, then start taper
- If under 2 weeks out: taper — cut volume 40-60%, keep intensity, rest legs

PLAN FORMAT — one message, structured:
Mon: rest
Tue: easy 8km — keep it truly easy, you should be able to hold a full convo
Wed: tempo 6km — comfortably hard, 10k race pace
Thu: rest or easy 5km
Fri: intervals 5x1km @ 5k pace, 2min rest between
Sat: long run 18km — slow and steady, practice fuelling
Sun: rest

Then add 2-3 lines in bro style explaining the key sessions and why.

PACE COACHING:
- Compare recent run paces and call out trends
- If improving: hype it hard
- If slowing down: ask about sleep, stress, volume, nutrition — give real advice
- Suggest target paces for each session type based on their recent race pace or best effort
- Use McMillan-style pace zones: easy = race pace + 90-120s/km, tempo = 10k pace, intervals = 5k pace

━━━ SETTINGS CHANGES ━━━
Detect intent from natural speech and output only changed fields:
PROFILE_UPDATE:{"goal":"new goal"}

If they update race info: PROFILE_UPDATE:{"race":{"name":"...","date":"...","target_time":"...","distance_km":42}}

━━━ WEEKLY SUMMARY ━━━
When triggered, recap the week: sessions done, total km if running, streak, missed days.
Set tone for the week ahead. Reference race countdown if applicable.

━━━ CURRENT USER INFO ━━━
Injected below.
"""

def build_system_prompt(profile: dict) -> str:
    today = datetime.now().strftime("%A %d %B %Y")
    date_block = "━━━ TODAY'S DATE ━━━\n" + today + "\n\n━━━ CURRENT USER INFO ━━━"
    base = SYSTEM_PROMPT.replace("━━━ CURRENT USER INFO ━━━", date_block)
    if profile and profile.get("onboarded"):
        days_str = ", ".join([DAY_NAMES[d] for d in profile.get("workout_days", [])])
        stats = get_stats(profile)
        race = profile.get("race", {})
        days_left = days_until_race(profile)
        recent_runs = get_recent_runs(profile, 3)
        mileage = get_weekly_mileage_trend(profile, 4)

        base += f"""
━━━ THIS USER ━━━
Bot name (what they call you): {profile.get('bot_name', 'Rex')}
User name: {profile.get('name', '?')}
Goal: {profile.get('goal', '?')}
Weak spot: {profile.get('weakspot', '?')}
Training days: {days_str}
Hype times: {', '.join(profile.get('hype_times', []))}
Total sessions: {stats['total_sessions']}
Current streak: {stats['current_streak']} days
Longest streak: {stats['longest_streak']} days
Missed days: {stats['missed_days']}
"""
        if mileage:
            base += f"Weekly mileage trend: {', '.join([f'{w}: {round(km,1)}km' for w,km in mileage.items()])}\n"

        if recent_runs:
            base += "Recent runs:\n"
            for r in recent_runs:
                parts = [r["date"]]
                if r.get("distance_km"): parts.append(f"{r['distance_km']}km")
                if r.get("pace_per_km"): parts.append(f"{r['pace_per_km']}/km")
                if r.get("effort"): parts.append(f"effort {r['effort']}/10")
                base += "  " + " | ".join(parts) + "\n"

        if race.get("name") or race.get("date"):
            base += f"Race: {race.get('name','?')} | {race.get('date','?')} | {race.get('distance_km',0)}km | target: {race.get('target_time','?')}\n"
            if days_left >= 0:
                base += f"Days until race: {days_left}\n"
            elif race.get("date"):
                base += f"Race date has passed or invalid\n"

        wp = profile.get("weekly_plan", {})
        if wp.get("plan"):
            base += f"Current weekly plan (generated {wp.get('generated_date','?')}):\n"
            for day in wp["plan"]:
                km = f" {day['distance_km']}km" if day.get("distance_km") else ""
                base += f"  {day['day']}: {day['type']}{km} — {day.get('notes','')}\n"

    return base


# ─────────────────────────────────────────
#  RESPONSE PARSER
# ─────────────────────────────────────────
def parse_and_apply(user_id: str, reply: str) -> tuple:
    lines = reply.split("\n")
    clean_lines = []
    profile_updated = None

    for line in lines:
        s = line.strip()
        if s.startswith("PROFILE_UPDATE:"):
            try:
                data = json.loads(s[len("PROFILE_UPDATE:"):])
                profile = get_user(user_id) or default_profile()
                for k, v in data.items():
                    if k == "race" and isinstance(v, dict):
                        profile["race"] = {**profile.get("race", {}), **v}
                    else:
                        profile[k] = v
                if all([profile.get("name"), profile.get("goal"),
                        profile.get("weakspot"), profile.get("workout_days"),
                        profile.get("hype_times")]):
                    profile["onboarded"] = True
                save_user(user_id, profile)
                profile_updated = profile
            except Exception as e:
                logger.warning(f"PROFILE_UPDATE error: {e}")

        elif s.startswith("LOG_SESSION:"):
            try:
                data = json.loads(s[len("LOG_SESSION:"):])
                log_session(user_id, data)
            except Exception as e:
                logger.warning(f"LOG_SESSION error: {e}")

        elif s == "LOG_MISSED:true":
            log_missed(user_id)

        elif s == "AWAITING_PROOF:true":
            profile = get_user(user_id) or default_profile()
            profile["awaiting_proof"] = True
            save_user(user_id, profile)

        elif s.startswith("WEEKLY_PLAN:"):
            try:
                plan = json.loads(s[len("WEEKLY_PLAN:"):])
                profile = get_user(user_id) or default_profile()
                profile["weekly_plan"] = {
                    "generated_date": datetime.now().strftime("%Y-%m-%d"),
                    "plan": plan,
                }
                save_user(user_id, profile)
            except Exception as e:
                logger.warning(f"WEEKLY_PLAN error: {e}")

        else:
            clean_lines.append(line)

    clean_reply = "\n".join(clean_lines).strip()
    return clean_reply, profile_updated


# ─────────────────────────────────────────
#  IMAGE VERIFICATION
# ─────────────────────────────────────────
async def verify_gym_photo(photo_bytes: bytes) -> str:
    b64 = base64.standard_b64encode(photo_bytes).decode("utf-8")
    response = client.messages.create(
        model="claude-opus-4-5",
        max_tokens=100,
        messages=[{
            "role": "user",
            "content": [
                {"type": "image", "source": {"type": "base64", "media_type": "image/jpeg", "data": b64}},
                {"type": "text", "text": (
                    "Is this photo evidence of a workout or exercise? "
                    "Accept: gym equipment, weights, machines, someone running or exercising outdoors, "
                    "workout clothes in action, sweaty post-workout selfie, running path/track, "
                    "fitness class, home workout setup. "
                    "Reject: couch, bed, food, random selfie with no exercise context. "
                    "Reply with exactly one word: LEGIT or FAKE."
                )}
            ]
        }]
    )
    result = response.content[0].text.strip().upper()
    return "LEGIT" if "LEGIT" in result else "FAKE"


# ─────────────────────────────────────────
#  CLAUDE
# ─────────────────────────────────────────
async def get_bot_reply(user_id: str, user_message: str) -> tuple:
    profile = get_user(user_id) or default_profile()
    history = profile.get("conversation", [])
    history.append({"role": "user", "content": user_message})

    # Rough pre-check — estimate ~500 tokens per call
    if not check_rate_limit(user_id, estimate_cost(500, 200)):
        return "yo easy bro 😅 you've been going hard, give me a few mins to recover", None

    response = client.messages.create(
        model="claude-opus-4-5",
        max_tokens=600,
        system=build_system_prompt(profile),
        messages=history,
    )

    raw_reply = response.content[0].text
    # Track actual cost
    usage = response.usage
    actual_cost = estimate_cost(usage.input_tokens, usage.output_tokens)
    check_rate_limit(user_id, actual_cost - estimate_cost(500, 200))  # adjust for actual

    clean_reply, profile_updated = parse_and_apply(user_id, raw_reply)

    profile = get_user(user_id) or default_profile()
    history.append({"role": "assistant", "content": raw_reply})
    if len(history) > 50:
        history = history[-50:]
    profile["conversation"] = history
    save_user(user_id, profile)

    return clean_reply, profile_updated


# ─────────────────────────────────────────
#  TYPING + SENDING
# ─────────────────────────────────────────
# Tracks active sending tasks per user so we can cancel them instantly
user_send_tasks: dict = {}  # {user_id: asyncio.Task}

MAX_BUBBLES_CHAT = 3     # hype/chat mode — enforced by prompt, this is a safety fallback
MAX_COST_PER_HOUR = 0.50 # USD — rough limit
COST_PER_1K_INPUT = 0.003
COST_PER_1K_OUTPUT = 0.015
user_hourly_cost: dict = {}  # {user_id: [(timestamp, cost), ...]}

def estimate_cost(input_tokens: int, output_tokens: int) -> float:
    return (input_tokens / 1000 * COST_PER_1K_INPUT) + (output_tokens / 1000 * COST_PER_1K_OUTPUT)

def check_rate_limit(user_id: str, cost: float) -> bool:
    """Returns True if under limit, False if over."""
    now = datetime.now()
    hour_ago = now - timedelta(hours=1)
    history = user_hourly_cost.get(user_id, [])
    history = [(t, c) for t, c in history if t > hour_ago]
    total = sum(c for _, c in history)
    if total + cost > MAX_COST_PER_HOUR:
        return False
    history.append((now, cost))
    user_hourly_cost[user_id] = history
    return True

def split_into_messages(text: str) -> list:
    """
    Split on blank lines (double newline = new message).
    Single newlines stay within the same message.
    This means plans with single-line-per-day stay as ONE message.
    Only a blank separator line creates a new bubble.
    """
    sep = "\n\n"
    paragraphs = [p.strip() for p in text.split(sep) if p.strip()]
    return paragraphs if paragraphs else [text]

async def _send_chunks(bot, chat_id: int, chunks: list, reply_fn=None):
    """Actually sends chunks with typing delays. Designed to be cancellable."""
    for i, chunk in enumerate(chunks):
        await asyncio.sleep(random.uniform(0.5, 1.0))
        await bot.send_chat_action(chat_id=chat_id, action="typing")
        await asyncio.sleep(random.uniform(1.5, 3.0))
        if i == 0 and reply_fn:
            await reply_fn(chunk)
        else:
            await bot.send_message(chat_id=chat_id, text=chunk)


async def send_with_typing(bot, chat_id: int, text: str, reply_fn=None, user_id: str = None):
    chunks = split_into_messages(text)

    if user_id:
        # Cancel any in-progress send for this user immediately
        existing = user_send_tasks.get(user_id)
        if existing and not existing.done():
            existing.cancel()
            try:
                await existing
            except asyncio.CancelledError:
                pass

        # Wrap the send in a task so it can be cancelled if user replies mid-send
        task = asyncio.create_task(_send_chunks(bot, chat_id, chunks, reply_fn))
        user_send_tasks[user_id] = task
        try:
            await task
        except asyncio.CancelledError:
            logger.info(f"Send cancelled for {user_id} — user replied")
    else:
        await _send_chunks(bot, chat_id, chunks, reply_fn)


# ─────────────────────────────────────────
#  SCHEDULING
# ─────────────────────────────────────────
async def send_scheduled_hype(context: ContextTypes.DEFAULT_TYPE):
    user_id = context.job.data["user_id"]
    profile = get_user(user_id)
    if not profile or not profile.get("onboarded"):
        return
    if datetime.now().weekday() not in profile.get("workout_days", []):
        return

    hour = datetime.now().hour
    days_left = days_until_race(profile)
    race_context = f" they have a race in {days_left} days." if days_left > 0 else ""

    if hour < 10:
        trigger = f"morning of a training day.{race_context} send a short punchy morning hype."
    elif hour < 15:
        trigger = f"midday on a training day.{race_context} check if they trained yet."
    else:
        trigger = f"evening on a training day.{race_context} last chance, don't let them skip."

    reply, updated = await get_bot_reply(user_id, f"[SYSTEM: {trigger} short and punchy like a real text.]")
    if updated:
        await reschedule_user(user_id, updated, context.application)
    try:
        await send_with_typing(context.application.bot, int(user_id), reply)
    except Exception as e:
        logger.warning(f"Hype failed for {user_id}: {e}")


async def send_weekly_summary(context: ContextTypes.DEFAULT_TYPE):
    users = load_users()
    for user_id, profile in users.items():
        if not profile.get("onboarded"):
            continue
        days_left = days_until_race(profile)
        race_note = f" race is in {days_left} days." if days_left > 0 else ""
        prompt = (
            f"[SYSTEM: monday morning weekly summary.{race_note} "
            f"here are their stats: {format_full_stats(profile)}. "
            f"react in bro style — hype wins, call out misses, fire them up. "
            f"if they have a race coming suggest whether to increase or maintain mileage this week. "
            f"use newlines between messages.]"
        )
        reply, _ = await get_bot_reply(user_id, prompt)
        try:
            await send_with_typing(context.application.bot, int(user_id), reply)
        except Exception as e:
            logger.warning(f"Weekly summary failed for {user_id}: {e}")


async def reschedule_user(user_id: str, profile: dict, app: Application):
    job_queue = app.job_queue
    for job in job_queue.get_jobs_by_name(f"hype_{user_id}"):
        job.schedule_removal()
    for t in profile.get("hype_times", []):
        try:
            h, m = map(int, t.split(":"))
            offset = random.randint(-15, 15)
            total = max(0, min(h * 60 + m + offset, 23 * 60 + 59))
            job_queue.run_daily(
                send_scheduled_hype,
                time=time(hour=total // 60, minute=total % 60),
                name=f"hype_{user_id}",
                data={"user_id": user_id},
            )
        except Exception as e:
            logger.warning(f"Bad time {t}: {e}")
    logger.info(f"Scheduled hype for {user_id} at {profile.get('hype_times')}")


async def restore_all_jobs(app: Application):
    users = load_users()
    for user_id, profile in users.items():
        if profile.get("onboarded") and profile.get("hype_times"):
            await reschedule_user(user_id, profile, app)
    app.job_queue.run_daily(
        send_weekly_summary,
        time=time(hour=8, minute=0),
        days=(0,),
        name="weekly_summary",
        data={},
    )
    logger.info("Restored all jobs + weekly summary.")


# ─────────────────────────────────────────
#  HANDLERS
# ─────────────────────────────────────────
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = str(update.effective_user.id)
    profile = get_user(user_id)
    if profile and profile.get("onboarded"):
        reply, _ = await get_bot_reply(user_id, "[SYSTEM: user just reopened the chat. say a quick hey, short.]")
    else:
        save_user(user_id, default_profile())
        reply, _ = await get_bot_reply(user_id, "[SYSTEM: brand new user. introduce yourself and suggest your name. casual, short, like a first text.]")
    await send_with_typing(context.bot, update.effective_chat.id, reply, update.message.reply_text)


async def handle_photo(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = str(update.effective_user.id)
    profile = get_user(user_id)

    if not profile or not profile.get("awaiting_proof"):
        reply, _ = await get_bot_reply(user_id, "[SYSTEM: user sent a photo but we weren't expecting proof. react casually.]")
        await send_with_typing(context.bot, update.effective_chat.id, reply, update.message.reply_text)
        return

    photo = update.message.photo[-1]
    file = await context.bot.get_file(photo.file_id)
    photo_bytes = await file.download_as_bytearray()

    await context.bot.send_chat_action(chat_id=update.effective_chat.id, action="typing")
    result = await verify_gym_photo(bytes(photo_bytes))

    reply, updated = await get_bot_reply(user_id, f"PROOF_RESULT:{result}")
    await send_with_typing(context.bot, update.effective_chat.id, reply, update.message.reply_text)
    if updated:
        await reschedule_user(user_id, updated, context.application)


# Per-user message queues and worker tasks
user_queues: dict = {}      # {user_id: asyncio.Queue}
user_workers: dict = {}     # {user_id: asyncio.Task}

async def process_user_messages(user_id: str, app):
    """Single worker per user — processes one message at a time, skips stale ones."""
    queue = user_queues[user_id]
    while True:
        update, context = await queue.get()
        try:
            # Drain queue — if more messages waiting, skip straight to the latest
            while not queue.empty():
                update, context = queue.get_nowait()

            if not get_user(user_id):
                save_user(user_id, default_profile())

            profile = get_user(user_id)
            text = update.message.text

            if profile and profile.get("awaiting_proof"):
                no_proof = ["no", "nope", "don't have", "dont have", "no pic", "no photo", "can't", "cannot", "nahh", "nah", "nothing"]
                if any(s in text.lower() for s in no_proof):
                    text = "PROOF_RESULT:NO_PROOF"

            reply, updated_profile = await get_bot_reply(user_id, text)

            # Before sending — check if another message arrived while Claude was thinking
            if not queue.empty():
                continue  # skip this reply, process the newer message instead

            # Cancel any leftover send task
            existing = user_send_tasks.get(user_id)
            if existing and not existing.done():
                existing.cancel()
                try:
                    await existing
                except asyncio.CancelledError:
                    pass

            task = asyncio.create_task(
                _send_chunks(context.bot, update.effective_chat.id,
                             split_into_messages(reply), update.message.reply_text)
            )
            user_send_tasks[user_id] = task
            try:
                await task
            except asyncio.CancelledError:
                pass

            if updated_profile and updated_profile.get("hype_times"):
                await reschedule_user(user_id, updated_profile, app)

        except Exception as e:
            logger.error(f"Worker error for {user_id}: {e}")
        finally:
            queue.task_done()


async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = str(update.effective_user.id)

    # Cancel any in-progress send immediately
    existing = user_send_tasks.get(user_id)
    if existing and not existing.done():
        existing.cancel()

    # Put message in this user's queue
    if user_id not in user_queues:
        user_queues[user_id] = asyncio.Queue()

    await user_queues[user_id].put((update, context))

    # Start worker if not running
    worker = user_workers.get(user_id)
    if not worker or worker.done():
        user_workers[user_id] = asyncio.create_task(
            process_user_messages(user_id, context.application)
        )


# ─────────────────────────────────────────
#  MAIN
# ─────────────────────────────────────────
async def strava_connect(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Send user a Strava auth link."""
    user_id = str(update.effective_user.id)
    base_url = os.environ.get("RAILWAY_PUBLIC_DOMAIN", "http://localhost:8080")
    if not base_url.startswith("http"):
        base_url = "https://" + base_url
    auth_url = (
        f"https://www.strava.com/oauth/authorize"
        f"?client_id={STRAVA_CLIENT_ID}"
        f"&redirect_uri={base_url}/strava/auth"
        f"&response_type=code"
        f"&scope=activity:read_all"
        f"&state={user_id}"
        f"&approval_prompt=auto"
    )
    msg = "tap this to connect Strava:\n" + auth_url + "\n\nonce you approve it your runs sync automatically 💪"
    await update.message.reply_text(msg)


def main():
    if not ANTHROPIC_API_KEY:
        raise ValueError("Set ANTHROPIC_API_KEY as an environment variable.")
    if not TELEGRAM_TOKEN:
        raise ValueError("Set TELEGRAM_TOKEN as an environment variable.")

    import threading
    from aiohttp import web

    tg_app = Application.builder().token(TELEGRAM_TOKEN).build()
    tg_app.add_handler(CommandHandler("start", start))
    tg_app.add_handler(CommandHandler("strava", strava_connect))
    tg_app.add_handler(MessageHandler(filters.PHOTO, handle_photo))
    tg_app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))

    async def on_startup(app):
        await restore_all_jobs(app)
    tg_app.post_init = on_startup

    # Run aiohttp webhook server in same event loop
    async def run_both():
        webhook_app = start_webhook_server()
        runner = web.AppRunner(webhook_app)
        await runner.setup()
        port = int(os.environ.get("PORT", 8080))
        site = web.TCPSite(runner, "0.0.0.0", port)
        await site.start()
        logger.info(f"Strava webhook server running on port {port}")
        await tg_app.initialize()
        await tg_app.start()
        await tg_app.updater.start_polling(allowed_updates=Update.ALL_TYPES)
        logger.info("GAINZ BOT IS ALIVE. LFG 💪")
        # Run forever
        import signal
        stop = asyncio.Event()
        loop = asyncio.get_event_loop()
        loop.add_signal_handler(signal.SIGTERM, stop.set)
        loop.add_signal_handler(signal.SIGINT, stop.set)
        await stop.wait()
        await tg_app.updater.stop()
        await tg_app.stop()
        await tg_app.shutdown()
        await runner.cleanup()

    asyncio.run(run_both())


if __name__ == "__main__":
    main()


# ─────────────────────────────────────────
#  STRAVA WEBHOOK
# ─────────────────────────────────────────
from aiohttp import web

STRAVA_CLIENT_ID = os.environ.get("STRAVA_CLIENT_ID", "")
STRAVA_CLIENT_SECRET = os.environ.get("STRAVA_CLIENT_SECRET", "")
STRAVA_VERIFY_TOKEN = os.environ.get("STRAVA_VERIFY_TOKEN", "gainzbot_strava")

def find_user_by_strava_id(strava_athlete_id: int):
    """Find a user profile that has this Strava athlete ID linked."""
    users = load_users()
    for user_id, profile in users.items():
        if profile.get("strava_athlete_id") == strava_athlete_id:
            return user_id, profile
    return None, None

async def fetch_strava_activity(access_token: str, activity_id: int) -> dict:
    """Fetch full activity details from Strava API."""
    import aiohttp
    url = f"https://www.strava.com/api/v3/activities/{activity_id}"
    headers = {"Authorization": f"Bearer {access_token}"}
    async with aiohttp.ClientSession() as session:
        async with session.get(url, headers=headers) as resp:
            if resp.status == 200:
                return await resp.json()
    return {}

async def refresh_strava_token(user_id: str, profile: dict) -> str:
    """Refresh expired Strava access token using refresh token."""
    import aiohttp
    refresh_token = profile.get("strava_refresh_token", "")
    if not refresh_token:
        return ""
    url = "https://www.strava.com/oauth/token"
    data = {
        "client_id": STRAVA_CLIENT_ID,
        "client_secret": STRAVA_CLIENT_SECRET,
        "grant_type": "refresh_token",
        "refresh_token": refresh_token,
    }
    async with aiohttp.ClientSession() as session:
        async with session.post(url, data=data) as resp:
            if resp.status == 200:
                tokens = await resp.json()
                profile["strava_access_token"] = tokens["access_token"]
                profile["strava_refresh_token"] = tokens["refresh_token"]
                profile["strava_token_expires"] = tokens["expires_at"]
                save_user(user_id, profile)
                return tokens["access_token"]
    return ""

async def get_valid_strava_token(user_id: str, profile: dict) -> str:
    """Return a valid access token, refreshing if expired."""
    expires = profile.get("strava_token_expires", 0)
    if datetime.now().timestamp() >= expires - 60:
        return await refresh_strava_token(user_id, profile)
    return profile.get("strava_access_token", "")

async def handle_strava_webhook(request: web.Request) -> web.Response:
    """Handles both Strava webhook verification and activity events."""

    # GET = Strava verifying the webhook endpoint
    if request.method == "GET":
        params = request.rel_url.query
        logger.info(f"Strava webhook GET: {dict(params)}")
        verify_token = params.get("hub.verify_token", "")
        challenge = params.get("hub.challenge", "")
        # Accept if token matches OR if no token configured yet
        if verify_token == STRAVA_VERIFY_TOKEN or not STRAVA_VERIFY_TOKEN:
            logger.info(f"Strava webhook verified, challenge: {challenge}")
            return web.json_response({"hub.challenge": challenge})
        logger.warning(f"Strava verify token mismatch: got {verify_token}")
        return web.Response(status=200, text=challenge)  # return 200 either way

    # POST = actual activity event
    try:
        data = await request.json()
    except Exception:
        return web.Response(status=400)

    # Only care about activity creation events
    if data.get("object_type") != "activity" or data.get("aspect_type") != "create":
        return web.Response(status=200, text="ok")

    athlete_id = data.get("owner_id")
    activity_id = data.get("object_id")

    user_id, profile = find_user_by_strava_id(athlete_id)
    if not user_id:
        logger.info(f"Strava athlete {athlete_id} not linked to any user")
        return web.Response(status=200, text="ok")

    # Fetch full activity in background so webhook returns fast
    asyncio.create_task(process_strava_activity(user_id, profile, activity_id))
    return web.Response(status=200, text="ok")


async def process_strava_activity(user_id: str, profile: dict, activity_id: int):
    """Fetch activity, log it, and send hype message to user."""
    try:
        access_token = await get_valid_strava_token(user_id, profile)
        if not access_token:
            logger.warning(f"No valid Strava token for {user_id}")
            return

        activity = await fetch_strava_activity(access_token, activity_id)
        if not activity:
            return

        # Only process runs
        activity_type = activity.get("type", "").lower()
        if activity_type not in ("run", "virtualrun", "trailrun"):
            return

        # Parse activity data
        distance_km = round(activity.get("distance", 0) / 1000, 2)
        duration_min = round(activity.get("moving_time", 0) / 60, 1)
        avg_hr = activity.get("average_heartrate", 0)
        name = activity.get("name", "run")

        # Calculate pace
        pace_str = ""
        if distance_km > 0 and duration_min > 0:
            pace_sec_per_km = (duration_min * 60) / distance_km
            pace_min = int(pace_sec_per_km // 60)
            pace_sec = int(pace_sec_per_km % 60)
            pace_str = f"{pace_min}:{pace_sec:02d}"

        # Log the session
        session_data = {
            "type": "run",
            "distance_km": distance_km,
            "duration_min": duration_min,
            "pace_per_km": pace_str,
            "heart_rate": int(avg_hr) if avg_hr else 0,
            "effort": 0,
            "notes": f"via Strava: {name}",
        }
        log_session(user_id, session_data)

        # Build a message for Claude to react to
        details = f"{distance_km}km"
        if pace_str:
            details += f" at {pace_str}/km"
        if duration_min:
            details += f" in {duration_min}min"
        if avg_hr:
            details += f", avg HR {int(avg_hr)}"

        trigger = (
            f"[SYSTEM: user just finished a run via Strava. auto-logged. "
            f"activity: {details}. "
            f"react like a real bro — hype them up, comment on the pace/distance, "
            f"compare to their recent runs if relevant. short and punchy.]"
        )
        reply, _ = await get_bot_reply(user_id, trigger)

        # Send the hype message
        from telegram import Bot
        bot = Bot(token=TELEGRAM_TOKEN)
        await send_with_typing(bot, int(user_id), reply, user_id=user_id)

    except Exception as e:
        logger.error(f"Strava activity processing error for {user_id}: {e}")


async def handle_strava_auth(request: web.Request) -> web.Response:
    """
    OAuth callback — Strava redirects here after user authorises.
    URL: /strava/auth?code=XXX&state=TELEGRAM_USER_ID
    """
    import aiohttp
    code = request.rel_url.query.get("code", "")
    telegram_user_id = request.rel_url.query.get("state", "")

    if not code or not telegram_user_id:
        return web.Response(status=400, text="Missing code or state")

    # Exchange code for tokens
    url = "https://www.strava.com/oauth/token"
    data = {
        "client_id": STRAVA_CLIENT_ID,
        "client_secret": STRAVA_CLIENT_SECRET,
        "code": code,
        "grant_type": "authorization_code",
    }
    async with aiohttp.ClientSession() as session:
        async with session.post(url, data=data) as resp:
            if resp.status != 200:
                return web.Response(status=400, text="Strava auth failed")
            tokens = await resp.json()

    # Save Strava tokens to user profile
    profile = get_user(telegram_user_id) or default_profile()
    profile["strava_athlete_id"] = tokens["athlete"]["id"]
    profile["strava_access_token"] = tokens["access_token"]
    profile["strava_refresh_token"] = tokens["refresh_token"]
    profile["strava_token_expires"] = tokens["expires_at"]
    save_user(telegram_user_id, profile)

    logger.info(f"Strava linked for user {telegram_user_id}")

    # Notify user in Telegram
    try:
        from telegram import Bot
        bot = Bot(token=TELEGRAM_TOKEN)
        reply, _ = await get_bot_reply(
            telegram_user_id,
            "[SYSTEM: user just linked their Strava account. celebrate it, tell them their runs will now sync automatically. short and hype.]"
        )
        await send_with_typing(bot, int(telegram_user_id), reply, user_id=telegram_user_id)
    except Exception as e:
        logger.warning(f"Could not notify user after Strava auth: {e}")

    return web.Response(text="✅ Strava connected! Head back to Telegram.", content_type="text/html")


def start_webhook_server():
    """Start aiohttp server for Strava webhooks alongside the Telegram bot."""
    app = web.Application()
    app.router.add_get("/strava/webhook", handle_strava_webhook)
    app.router.add_post("/strava/webhook", handle_strava_webhook)
    app.router.add_get("/strava/auth", handle_strava_auth)
    return app
