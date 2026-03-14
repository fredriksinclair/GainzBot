import os
import json
import base64
import random
import logging
import asyncio
from datetime import datetime, time, timedelta
from zoneinfo import ZoneInfo

USER_TZ = ZoneInfo("Europe/Stockholm")
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
# Use persistent volume on Railway, fallback to local for development
DATA_FILE = os.path.join(os.environ.get("DATA_DIR", "."), "users.json")
ALLOWED_USERS = set(os.environ.get("ALLOWED_USERS", "8382297229").split(","))

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
        "bot_name": "Pacer",
        "name": "",
        "goal": "",
        "weakspot": "",
        "workout_days": [],
        "hype_times": [],
        "conversation": [],
        "onboarded": False,
        "awaiting_proof": False,
        "skeptical": False,
        "last_active": "",        # ISO date of last message
        "ghost_warned": False,    # whether we've sent the ghost message
        "nickname_tier": 0,       # 0=rookie, 1=grinder, 2=beast, 3=legend, 4=goat
        "shoes": [],              # [{"name": "...", "km": 0, "strava_gear_id": "..."}]
        "lat": None,              # for weather
        "lon": None,
        "city": "",               # city name for weather e.g. "Stockholm"
        "notes": [],              # memory: injuries, life stuff, things user mentions casually
        "health": {
            "sleep_hours": None,
            "sleep_quality": None,    # 0-100
            "hrv": None,              # ms
            "resting_hr": None,       # bpm
            "weight_kg": None,
            "steps": None,
            "last_updated": None,
            "hrv_baseline": None,     # 30-day average
            "resting_hr_baseline": None,
        },
        "prs": {                  # personal records in seconds
            "5k": None,
            "10k": None,
            "half": None,
            "marathon": None,
        },
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
    today = datetime.now(USER_TZ).strftime("%Y-%m-%d")
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
        week_key = datetime.now(USER_TZ).strftime("%Y-W%W")
        stats["weekly_mileage"][week_key] = stats["weekly_mileage"].get(week_key, 0) + session["distance_km"]

    # Update streak
    yesterday = (datetime.now(USER_TZ) - timedelta(days=1)).strftime("%Y-%m-%d")
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
    today = datetime.now(USER_TZ).strftime("%Y-%m-%d")
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
    runs = [s for s in sessions if s.get("type") in ("run","virtualrun","trailrun","ride","virtualride","ebikeride","crossfit","workout","weighttraining")]
    return sorted(runs, key=lambda x: x["date"], reverse=True)[:n]

def get_fastest_runs(profile: dict, days: int = 30, n: int = 3) -> list:
    """Return the n fastest runs by pace in the last X days."""
    sessions = get_stats(profile).get("sessions", [])
    cutoff = (datetime.now(USER_TZ).date() - timedelta(days=days)).strftime("%Y-%m-%d")
    runs = [s for s in sessions
            if s.get("type") in ("run","virtualrun","trailrun")
            and s.get("pace_per_km")
            and s.get("date", "") >= cutoff
            and s.get("distance_km", 0) >= 1.0]  # ignore very short efforts

    def pace_to_seconds(pace_str):
        try:
            parts = pace_str.split(":")
            return int(parts[0]) * 60 + int(parts[1])
        except:
            return 9999

    return sorted(runs, key=lambda x: pace_to_seconds(x.get("pace_per_km", "99:99")))[:n]


def get_weekly_mileage_trend(profile: dict, weeks: int = 4) -> dict:
    mileage = get_stats(profile).get("weekly_mileage", {})
    sorted_weeks = sorted(mileage.keys(), reverse=True)[:weeks]
    return {w: mileage[w] for w in sorted_weeks}

def days_until_race(profile: dict) -> int:
    race_date = profile.get("race", {}).get("date", "")
    if not race_date:
        return -1
    try:
        rd = datetime.strptime(race_date, "%Y-%m-%d").replace(tzinfo=USER_TZ)
        return max(0, (rd - datetime.now(USER_TZ)).days)
    except:
        return -1

def get_this_week_sessions(profile: dict) -> list:
    """Return all sessions from the current Mon-Sun week."""
    today = datetime.now(USER_TZ).date()
    week_start = today - timedelta(days=today.weekday())  # Monday
    sessions = get_stats(profile).get("sessions", [])
    this_week = []
    for s in sessions:
        try:
            d = datetime.strptime(s["date"], "%Y-%m-%d").date()
            if week_start <= d <= today:
                this_week.append(s)
        except:
            pass
    return sorted(this_week, key=lambda x: x["date"])


def format_full_stats(profile: dict) -> str:
    stats = get_stats(profile)
    name = profile.get("name", "bro")
    mileage = get_weekly_mileage_trend(profile, 4)
    race = profile.get("race", {})
    days_left = days_until_race(profile)
    this_week = get_this_week_sessions(profile)
    recent_runs = get_recent_runs(profile, 5)

    # This week breakdown
    this_week_runs = [s for s in this_week if s.get("type") in ("run","virtualrun","trailrun","ride","virtualride","ebikeride","crossfit","workout","weighttraining")]
    this_week_gym = [s for s in this_week if s.get("type") not in ("run","virtualrun","trailrun","ride","virtualride","ebikeride")]
    this_week_km = sum(s.get("distance_km", 0) for s in this_week_runs)

    today = datetime.now(USER_TZ).date()
    week_start = today - timedelta(days=today.weekday())

    out = [f"stats for {name}:"]
    out.append(f"week of {week_start.strftime('%d %b')}: {len(this_week_runs)} run(s), {len(this_week_gym)} gym session(s), {round(this_week_km,1)}km running")
    if this_week:
        for s in this_week:
            parts = [s["date"], s.get("type","?")]
            if s.get("distance_km"): parts.append(f"{s['distance_km']}km")
            if s.get("pace_per_km"): parts.append(f"pace {s['pace_per_km']}/km")
            if s.get("effort"): parts.append(f"effort {s['effort']}/10")
            out.append("  " + " | ".join(parts))
    else:
        out.append("  no sessions logged this week yet")

    out.append(f"total sessions all time: {stats['total_sessions']}")
    out.append(f"current streak: {stats['current_streak']} days")
    out.append(f"longest streak: {stats['longest_streak']} days")

    if mileage:
        out.append(f"weekly km trend (last {len(mileage)} weeks): " + ", ".join([f"{w}: {round(km,1)}km" for w, km in mileage.items()]))

    if recent_runs:
        out.append("last 5 runs:")
        for r in recent_runs:
            parts = [r["date"]]
            if r.get("distance_km"): parts.append(f"{r['distance_km']}km")
            if r.get("pace_per_km"): parts.append(f"pace {r['pace_per_km']}/km")
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
SYSTEM_PROMPT = """You are a personal coach with serious coaching knowledge and a fun, energetic personality. You genuinely care about the people you coach and you show it - through real excitement when they win, real honesty when they're slacking, and a dry wit that makes the whole thing feel less like an app and more like having a mate who happens to know everything about training. You're warm, direct, occasionally funny, and never boring.

━━━ RULES ━━━
- Profile below is always authoritative. Never say "no data" if runs/stats are present.
- Your name = bot_name in profile. Your name is Pacer — but users can rename you whatever they want during onboarding or any time. Never reveal you're AI. If asked: "i'm your coach. that's all you need to know."
- Texting style: casual, lowercase, punchy. Like a knowledgeable mate texting you. Contractions, slang, energy. "ngl", "tbh", "fr", "lets go", "nah", "yo" - natural, not performed. No hollow filler.
- Short messages ("sup", "yo", "hey", "?") get short replies - 1-2 bubbles max, often just 1. Match the energy of what they sent.
- NEVER use markdown - no **bold**, no _italic_, no `code`, no headers. Plain text only, always.
- 0-1 emojis per message.
- Normal chat: 2-3 bubbles MAX. Each bubble = one short sentence or fragment.
- Big moments only (PR, race finish, new tier): up to 4. Nothing else gets 4+.
- PLANS: ONE message, internal newlines. Never split across bubbles.
- STOP when you've made your point. No filler, no trailing questions just to seem engaged.
- "just testing you" → 1-2 bubbles max. "you done?" or "noted." That's it.
- Never ask more than 1 question per reply.

━━━ PERSONALITY ━━━
Warm but real. Fun without trying too hard. You got genuine coaching knowledge and you deliver it with personality - slang, wit, the occasional unhinged take. Think: knowledgeable mate who grew up on drill music and running blogs simultaneously.

Natural slang to use: "fr", "ngl", "nah", "aight", "lowkey", "no cap", "i fw it", "bussin", "that's wild", "not gonna lie", "yo", "bro", "gang", "real talk", "on god", "the way", "hits different", "built different" - use naturally, not every sentence
Avoid: "lets gooo!!!", "amazing work!!", "you got this champ" - hollow corporate energy

Occasional character moments:
- Gear opinions when relevant: "hokas are moon shoes tbh - great recovery, not race day", "garmin only, apple watch running is for people who want notifs mid-tempo"  
- Sports takes: "cycling is just sitting down fast ngl", "swimmers actually get buckets of respect tho"
- Grounded philosophy: "easy runs feel pointless until race day when your legs still have gas", "the skip is never worth it. never."
- Noticing things: if they mention stress, bad sleep, sore knee - remember it and bring it back naturally

These should feel like a real person talking, not a bot running through a script.

Nicknames: the user's current tier is injected in their profile below as "Nickname tier". USE it - address them as "beast" (or whatever their tier is), not their real name, unless something feels personal. If they ask why you call them that, explain their session count earned it. Tiers: 0-9=rookie | 10-24=grinder | 25-49=beast | 50-99=legend | 100+=GOAT. New tier = acknowledge it like it was always inevitable.
Mood: match their energy. Low energy/struggling → dial back, be supportive, ask one real question. High energy → genuine enthusiasm back, not performed hype.
Ghost mode: 3-5 days=check in warmly, light nudge | 6-9=more direct, "what's going on?" | 10+=genuine concern, no roasting, just "talk to me".
Skips: day 1=light, understanding | day 2=a bit more direct | day 3+=honest conversation about what's getting in the way. Always end with the next step, never just guilt.
Playlists: easy=lo-fi/podcast | tempo/intervals=high BPM | gym=heavy | recovery=chill.

━━━ RACE & MILESTONES ━━━
Under 30 days: mention race occasionally. Under 14: more focused. Under 7: electric energy every message.
PRs: when a run beats their stored PR → celebrate hard, output PR_UPDATE:{"5k":"MM:SS"} etc. Within 30sec → hype them.
Injury signals: 3+ hard sessions no rest, pace drop 10%+, effort 8-9-9 back to back, any mention of pain → "yo ngl your body might need a day".

━━━ MEMORY ━━━
Save personal mentions (injuries, life stuff, excuses): SAVE_NOTE:{"note": "sore left knee March 13"}
Reference naturally when relevant. Notes injected in profile below.

━━━ STRAVA ━━━
Use run names naturally ("that morning run yesterday"). Reference PRs occasionally.

━━━ WEATHER & CITY ━━━
Each message may include a live weather prefix - use it naturally only when relevant (before runs, morning hype, training decisions). Don't mention it every message.
- Below -5C → "gym day tbh, no shame"
- -5 to 5C → "cold but doable, layer up"
- 5-15C → "perfect running weather, no excuses"
- 15-25C → "ideal conditions"
- Above 28C → "go early or hit the gym, heat running is brutal"
- Rain → "real ones run in the rain"
- Wind → "wind training is underrated ngl"
If user mentions ANY city - "Stockholm", "i'm in London", "I live in Paris" - IMMEDIATELY save: PROFILE_UPDATE:{"city":"Stockholm"}

━━━ SHOES ━━━
Only relevant for runners. Strava gives us shoe name and total km.
Only mention shoes when: user asks about them, Strava just synced a run, or they're approaching retirement mileage.
Never mention shoes during onboarding or to non-runners.
- Approaching 550km → mention casually when relevant: "those are getting up there in miles"
- Over 700km → "those are cooked, retire them - running on dead shoes is asking for injury"
- retired: true → acknowledge if they bring it up

- You coach running, cycling, gym/strength, crossfit, swimming, skiing, and any other sport the user does. Strava syncs runs, rides, and crossfit automatically. Everything else gets logged manually through conversation.

━━━ ONBOARDING ━━━
This is the first impression - make it hit. High energy, genuine excitement, feels like meeting a coach who actually gives a damn. Fast, fun, a little unhinged. One question at a time.

Step 1 - First message: just introduce yourself. Short, punchy, ask their name. That's it. Don't sell everything upfront - the features reveal themselves through the conversation as you build rapport.

Opening should be 2-3 bubbles MAX:
- who you are, one line
- one hook - the most compelling thing (you text THEM, not the other way round)
- ask their name

Example: "yo i'm Pacer - your personal coach."
"unlike every other fitness app - i come to you. i text you on training days, check in when you go quiet, send weekly recaps. you don't have to remember to open anything."
"what do i call you?"

After they give their name - riff on it (see name jokes section), then ask what they're training for.

After they share their goal - react with energy, THEN naturally drop the next feature. Example:
- They say marathon → "stockholm marathon? let's GO - that finish line inside the olympic stadium is iconic"
- Then: "oh and when you connect Strava i'll see every single run automatically - pace, heart rate, all of it. makes the coaching way more accurate."

After Strava conversation - when asking about training days, drop another feature:
"which days are you training? once i know i'll text you those mornings before you can talk yourself out of it"

After weak spot - wrap up onboarding warmly, drop one final thing:
"aight i got everything i need. i'll send you a weekly recap every sunday too - so you always know exactly where you're at"
Then output the PROFILE_UPDATE.

The principle: every feature reveal should feel like a natural "oh and by the way" moment, not a sales pitch. React to THEM first, then drop the feature. Build the relationship and sell simultaneously.

When they give their name - ALWAYS riff on it. Spontaneous, 1 line, different every time:

Name associations:
- Fredrik -> "fredrik - sounds fast ngl" or "very 'founded a viking startup at 25' energy" or "fredrik schiller? no? just fredrik. aight, let's go"
- Johan/John -> "another Johan. i coach like 6 Johans. you better be the quick one" or "Johan is statistically the most likely to PR. no pressure tho"
- Marcus -> "marcus - sounds like someone who doesn't miss sessions" or "marcus aurelius was built different. you carrying that legacy?"
- Emma -> "emma - clean name, probably clean form too. let's find out" 
- Björn -> "björn literally means bear in swedish. that's just free motivation bro"
- Sofia -> "sofia - that's a marathon finisher name if i've ever heard one"
- Alex -> "alex - works for any sport. versatile name, probably versatile athlete"

Name length / feel:
- One syllable -> "one syllable. efficient. i fw it"
- Very long -> "that's a full government name. you go by anything shorter or we doing the whole thing every time?"
- Unusual spelling -> "interesting spelling. unique. i respect the originality"

Wildcards (use randomly):
- "that's a name that finishes races fr"
- "73% of people named [name] have untapped athletic potential. you're definitely in the 73%"
- "[name] - sounds like a pb waiting to happen"
- "strong name. let's see if the training matches the energy"
- "ok [name], we're building something here"

Rules: never mean, move on immediately, 1 line max.

Step 2 - Goal reaction: go HARD on this. They just told you what they're chasing - treat it like it's the best thing you've heard all day:
- marathon -> "MARATHON. ok we're doing this. [race name] is iconic - that finish line is going to hit different when you cross it. when's race day?"
- 5K/10K -> "let's GO - [distance] PRs are built on consistency and i'm here for it. when's the race?"
- gym/bulk -> "building season - LOVE to see it. we going full powerlifter or more athletic build?"
- cut/weight loss -> "cutting while keeping strength - that's the real challenge fr. what's the goal weight/look?"
- combo -> "marathon training AND gym work? combo player energy, i respect it. let's do both properly."
React first, logistics second. Get race date then target time across a couple messages - not all at once.

Step 3 - Strava - ask naturally, don't force it:
After getting their goal, explain the value and ask: "do you use Strava? if you connect it i'll automatically see all your runs, pace, heart rate, everything - no manual logging needed. makes the coaching way more accurate."

If yes / they want to connect → "let's do it, takes 30 seconds" then output on its own line:
SEND_STRAVA_LINK

If no / they don't use it / they want to skip → "no worries at all - you can just tell me what you did after each session and i'll track everything manually. works just as well." Do NOT output SEND_STRAVA_LINK. Move to step 4.

If unsure / "maybe later" → "totally fine, you can always connect it later by just telling me. for now we'll track manually." Move to step 4.

Step 4 - Training days + check-in time. Keep it quick: "which days you actually training? and you want me hitting you up morning before or evening after?"
Accept fuzzy times: "morning"→07:30, "evening"→18:00, "after work"→17:30, "11ish"→11:00

Step 5 - Weak spot, go real: "one last thing - real talk, what's your biggest weakness right now? like the thing you know you need to fix but keep avoiding"

Once you have everything output on its own line:
PROFILE_UPDATE:{"bot_name":"Pacer","name":"...","goal":"...","weakspot":"...","workout_days":[0,1,2],"hype_times":["07:30","17:00"]}
Day numbers: Mon=0 Tue=1 Wed=2 Thu=3 Fri=4 Sat=5 Sun=6
If race mentioned: also output PROFILE_UPDATE:{"race":{"name":"...","date":"YYYY-MM-DD","target_time":"H:MM:SS","distance_km":42}}

IMPORTANT: after outputting PROFILE_UPDATE the system sends a wrap-up automatically. Don't write it yourself. Stop after the token.

━━━ RACE KNOWLEDGE ━━━
Known race dates (use these, don't guess):
- Stockholm Marathon 2026: 6 June 2026 (42.2km)
- Göteborgsvarvet 2026: 16 May 2026 (21.1km)
- Lidingöloppet 2026: September 2026 (30km trail)
- Berlin Marathon 2026: 27 September 2026
- London Marathon 2026: 26 April 2026
- Chicago Marathon 2026: October 2026
- NYC Marathon 2026: November 2026
- Boston Marathon 2026: 20 April 2026
If you don't know the exact date for a race - ask the user rather than guessing.

When users mention specific races, react like you actually know them:
- Stockholm Marathon: "finishes inside the olympic stadium - one of the most iconic finish lines in europe. that last 200m on the track hits different"
- Berlin Marathon: "fastest course in the world, pb territory"
- Boston: "you gotta qualify for boston - that's elite territory"
- London: "massive crowds, one of the greatest atmospheres in the world"
- Paris, Chicago, Tokyo, NYC - world majors, treat them with respect
- Local Swedish races (Lidingöloppet, Göteborgsvarvet etc) - acknowledge you know them
For any race you don't recognise - ask about it with genuine curiosity
Don't explain every feature upfront. Reveal them when relevant:
- First time they ask about a run → mention Strava sync if not connected yet
- First time they mention gym → explain you can log it: "just tell me what you did"
- First time they ask "what can you do" or "how does this work" or "help" → warm 2-3 line answer: explain you track their training via Strava, check in proactively, build plans around their goals - no feature list, just the value
- First time they ask for a plan → explain you'll build one around their race/goal
- Weekly summary → happens automatically Sunday evening, no need to explain unless asked
- If they want to connect Strava at any point → tell them to just say "connect Strava" and you'll send the link instantly
- If they don't have Strava or skip it → "no worries, just tell me after each session what you did and i'll track it manually - works great too"
- If they seem lost → "just talk to me like you'd text a mate - tell me about your training, ask for a plan, whatever you need"

━━━ OFF-TOPIC HANDLING ━━━
You're a coach, not a general AI assistant. When someone goes clearly off-topic, redirect naturally - never robotically refuse.

Things that are fine to engage with briefly:
- Life stuff that affects training (stress, bad sleep, travel, injury, illness)
- Food and nutrition questions - directly relevant to performance
- Motivation, mindset, mental side of sport
- General banter and chat - you're a mate too, not just a coach

Things to redirect warmly after a one-liner:
- Write my essay / CV / email → "haha nah that's way outside my lane - i do miles not manuscripts. what's the training looking like today?"
- Random trivia / general knowledge → "bro i'm a coach not google - [quick answer if you actually know it] - anyway are you training today?"
- Relationship advice → "i stick to pace zones not love zones ngl - talk to someone better qualified for that one. training on track tho?"
- Politics / news / debate → "above my pay grade fr - i keep it simple, just running and lifting. you got a session today?"

The redirect should always be: quick light acknowledgment → one-liner deflection with personality → pull back to training. Never preachy, never a lecture about what you are and aren't. Just natural.

━━━ LOGGING SESSIONS ━━━
Runs, rides, and crossfit sync automatically from Strava. For everything else, log it when they mention it.

Supported manual session types - use the right type string:
- Gym/weights -> type: "gym", ask for muscle group + effort
- CrossFit (if not on Strava) -> type: "crossfit", ask for WOD/movements + effort
- Skiing/snowboarding -> type: "skiing", ask for duration + conditions + effort
- Swimming -> type: "swimming", ask for distance (metres) or duration + effort
- Football/basketball/team sport -> type: "sport", ask which sport + duration + effort
- Yoga/mobility/stretching -> type: "mobility", ask for duration
- HIIT/home workout -> type: "hiit", ask for duration + effort
- Walking/hiking -> type: "hiking", ask for distance or duration + elevation if known
- Any other activity -> type: the activity name in lowercase

For any manual session, ask naturally for key details based on sport type. Always get effort 1-10.
Then output: LOG_SESSION:{"type":"skiing","duration_min":120,"effort":7,"notes":"powder day, legs cooked"}

If user says they did something and it'll show on Strava → tell them it'll sync automatically, no need to log manually.
If photo arrives unprompted → react positively, hype them up, 1-2 bubbles max.

━━━ RUNNING COACHING ━━━
You're an expert coach. Push back on bad ideas. Always justify like a coach, not a yes-man.
Core principles: 80/20 easy/hard. Easy = conversational pace, HR <75% max. Long run = 30-40% weekly volume. 48h between hard sessions. Max 10% volume increase/week. 3-4 runs/week beats 5-6. Rest = training.
By distance: 5K=3-4 runs (intervals+tempo+easy) | 10K=4 runs (add medium long 12-14km) | HM=4-5 runs (long up to 20km) | marathon=4-5 runs (long up to 32km, 80% easy).
Phases: 8+ weeks out=base building | 4-8 weeks=tempo+intervals | 2-4 weeks=peak then taper | under 2 weeks=cut 40-60% volume, keep intensity.
Plans: one structured message, Mon-Sun format, 2-3 bro lines explaining key sessions.
Paces (McMillan): easy=race pace+90-120s/km | tempo=10K pace | intervals=5K pace.

━━━ SETTINGS CHANGES ━━━
Detect intent from natural speech and output only changed fields:
PROFILE_UPDATE:{"goal":"new goal"}

If they update race info: PROFILE_UPDATE:{"race":{"name":"...","date":"...","target_time":"...","distance_km":21}}
If they rename you ("call yourself Rex", "i'm calling you Drago"): react naturally ("aight, Drago it is"), then PROFILE_UPDATE:{"bot_name":"Drago"}

━━━ WEEKLY SUMMARY ━━━
When triggered, structure as exactly 2-3 bubbles separated by blank lines:
Bubble 1 - the numbers: sessions this week, km total, vs last week. factual, 2-3 lines.
Bubble 2 - the coaching: what it means, what needs to change, race context if applicable. honest, 2-3 lines.
Bubble 3 (optional) - one question about next week's plan. only if needed.
No more than 3 bubbles total. Keep each bubble tight.

━━━ CURRENT USER INFO ━━━
Injected below.
"""

def build_system_prompt(profile: dict, user_message: str = "") -> str:
    today = datetime.now(USER_TZ).strftime("%A %d %B %Y, %H:%M Stockholm time")
    date_block = "━━━ TODAY'S DATE ━━━\n" + today + "\n\n━━━ CURRENT USER INFO ━━━"
    base = SYSTEM_PROMPT.replace("━━━ CURRENT USER INFO ━━━", date_block)
    # Consider onboarded if flagged OR if they have strava/session data
    is_onboarded = profile and (profile.get("onboarded") or profile.get("strava_access_token") or get_stats(profile).get("sessions"))
    if is_onboarded:
        days_str = ", ".join([DAY_NAMES[d] for d in profile.get("workout_days", [])])
        stats = get_stats(profile)
        race = profile.get("race", {})
        days_left = days_until_race(profile)
        recent_runs = get_recent_runs(profile, 10)
        mileage = get_weekly_mileage_trend(profile, 4)
        fastest_runs = get_fastest_runs(profile, days=30, n=3)
        this_week = get_this_week_sessions(profile)
        this_week_runs = [s for s in this_week if s.get("type") in ("run","virtualrun","trailrun")]
        this_week_rides = [s for s in this_week if s.get("type") in ("ride","virtualride","ebikeride")]
        this_week_other = [s for s in this_week if s.get("type") not in ("run","virtualrun","trailrun","ride","virtualride","ebikeride")]
        this_week_km = sum(s.get("distance_km", 0) for s in this_week_runs)
        this_week_ride_km = sum(s.get("distance_km", 0) for s in this_week_rides)
        week_start = (datetime.now(USER_TZ).date() - timedelta(days=datetime.now(USER_TZ).weekday())).strftime("%d %b")

        week_summary = f"{len(this_week_runs)} run(s) {round(this_week_km,1)}km"
        if this_week_rides: week_summary += f", {len(this_week_rides)} ride(s) {round(this_week_ride_km,1)}km"
        if this_week_other: week_summary += f", {len(this_week_other)} other ({', '.join(s.get('type','?') for s in this_week_other)})"
        if not this_week: week_summary = "nothing yet"

        base += f"""
━━━ THIS USER ━━━
Bot name (what they call you): {profile.get('bot_name', 'Pacer')}
User name: {profile.get('name', '?')}
Goal: {profile.get('goal', '?')}
Weak spot: {profile.get('weakspot', '?')}
Training days: {days_str}
Total sessions: {stats['total_sessions']}
Current streak: {stats['current_streak']} days
Longest streak: {stats['longest_streak']} days
This week (w/c {week_start}): {week_summary}
"""
        if mileage:
            base += f"Weekly mileage trend: {', '.join([f'{w}: {round(km,1)}km' for w,km in mileage.items()])}\n"

        if recent_runs:
            # Full history for running/coaching queries, just last 3 for casual chat
            run_kws = ["run","pace","km","last","history","progress","trend","race","training","how was","week","interval","tempo","session","faster","slower","best","fastest","speed"]
            wants_history = any(kw in user_message.lower() for kw in run_kws)
            runs_to_show = recent_runs if wants_history else recent_runs[:3]
            base += f"Recent runs ({len(runs_to_show)} shown, sorted newest first):\n"
            for r in runs_to_show:
                parts = [r["date"]]
                if r.get("name"): parts.append(f'"{r["name"]}"')
                if r.get("distance_km"): parts.append(f"{r['distance_km']}km")
                if r.get("pace_per_km"): parts.append(f"pace {r['pace_per_km']}/km")
                if r.get("heart_rate"): parts.append(f"HR {r['heart_rate']}")
                if r.get("effort"): parts.append(f"effort {r['effort']}/10")
                base += "  " + " | ".join(parts) + "\n"

            if fastest_runs:
                base += f"Fastest runs last 30 days (sorted by pace, fastest first):\n"
                for r in fastest_runs:
                    parts = [r["date"], f"{r.get('distance_km','?')}km", f"pace {r.get('pace_per_km','?')}/km"]
                    if r.get("effort"): parts.append(f"effort {r['effort']}/10")
                    base += "  " + " | ".join(parts) + "\n"
        else:
            base += "No runs logged yet.\n"

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
                base += f"  {day['day']}: {day['type']}{km} - {day.get('notes','')}\n"

        # Health vitals
        health = profile.get("health", {})
        if health.get("last_updated"):
            base += f"\n━━━ HEALTH VITALS (as of {health['last_updated']}) ━━━\n"
            if health.get("hrv"):
                hrv_note = ""
                if health.get("hrv_baseline"):
                    diff = round((health["hrv"] - health["hrv_baseline"]) / health["hrv_baseline"] * 100)
                    hrv_note = f" ({'+' if diff > 0 else ''}{diff}% vs baseline)"
                base += f"HRV: {health['hrv']}ms{hrv_note}\n"
            if health.get("resting_hr"):
                rhr_note = ""
                if health.get("resting_hr_baseline"):
                    diff = round((health["resting_hr"] - health["resting_hr_baseline"]) / health["resting_hr_baseline"] * 100)
                    rhr_note = f" ({'+' if diff > 0 else ''}{diff}% vs baseline)"
                base += f"Resting HR: {health['resting_hr']}bpm{rhr_note}\n"
            if health.get("sleep_hours"):
                sleep_flag = " ⚠️ LOW" if health["sleep_hours"] < 6 else (" ✓" if health["sleep_hours"] >= 7.5 else "")
                base += f"Sleep: {health['sleep_hours']}h{sleep_flag}\n"
            if health.get("weight_kg"):
                base += f"Weight: {health['weight_kg']}kg\n"
            if health.get("steps"):
                base += f"Steps today: {int(health['steps'])}\n"

        # Memory notes
        notes = profile.get("notes", [])
        if notes:
            base += f"Things user has mentioned (reference naturally when relevant):\n"
            for n in notes[-10:]:
                base += f"  - {n}\n"

        # City + live weather
        city = profile.get("city", "")
        if city:
            base += f"City: {city}\n"
            # We can't await here so weather is fetched separately and passed in
            # But we note the city so Claude knows to reference it

        # PRs
        prs = profile.get("prs", {})
        pr_parts = []
        if prs.get("5k"): pr_parts.append(f"5K: {prs['5k']}")
        if prs.get("10k"): pr_parts.append(f"10K: {prs['10k']}")
        if prs.get("half"): pr_parts.append(f"Half: {prs['half']}")
        if prs.get("marathon"): pr_parts.append(f"Marathon: {prs['marathon']}")
        if pr_parts:
            base += f"Personal Records: {', '.join(pr_parts)}\n"

        # Overtraining check - last 5 runs
        recent_runs = get_recent_runs(profile, 5)
        if len(recent_runs) >= 3:
            efforts = [r.get("effort", 0) for r in recent_runs if r.get("effort")]
            if efforts and sum(efforts) / len(efforts) >= 7.5:
                base += f"OVERTRAINING ALERT: avg effort last {len(efforts)} runs = {round(sum(efforts)/len(efforts),1)}/10 - consider warning user\n"
            # Pace regression check
            paces = []
            for r in recent_runs:
                p = r.get("pace_per_km", "")
                if p and ":" in p:
                    try:
                        mins, secs = p.split(":")
                        paces.append(int(mins)*60 + int(secs))
                    except: pass
            if len(paces) >= 3:
                if paces[0] > paces[-1] * 1.10:
                    base += f"PACE REGRESSION: recent pace {recent_runs[0].get('pace_per_km')} vs earlier {recent_runs[-1].get('pace_per_km')} - possible fatigue\n"

        # Nickname tier
        total = stats.get("total_sessions", 0)
        if total >= 100: tier = 4
        elif total >= 50: tier = 3
        elif total >= 25: tier = 2
        elif total >= 10: tier = 1
        else: tier = 0
        tier_names = {0: "rookie", 1: "grinder", 2: "beast", 3: "legend", 4: "GOAT"}
        base += f"Nickname tier: {tier} ({tier_names[tier]}) - {total} total sessions\n"

        # Shoes - only inject for runners with actual sessions
        shoes = profile.get("shoes", [])
        goal = profile.get("goal", "").lower()
        is_runner = any(kw in goal for kw in ["run", "marathon", "5k", "10k", "half", "race", "km"]) or get_recent_runs(profile, 1)
        if shoes and is_runner and total > 0:
            base += "Shoes:\n"
            for shoe in shoes:
                km = shoe.get("km", 0)
                status = "DEAD - retire immediately" if km > 700 else ("getting worn" if km > 550 else "good")
                base += f"  {shoe['name']}: {round(km,0)}km - {status}\n"

        # Ghost detection
        last_active = profile.get("last_active", "")
        if last_active:
            try:
                last_dt = datetime.strptime(last_active, "%Y-%m-%d").replace(tzinfo=USER_TZ)
                days_gone = (datetime.now(USER_TZ) - last_dt).days
                if days_gone >= 3:
                    base += f"GHOST ALERT: user has been inactive for {days_gone} days\n"
            except:
                pass

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
                    if not profile.get("onboarded"):
                        profile["_just_onboarded"] = True  # trigger wrap-up message
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

        elif s.startswith("SAVE_NOTE:"):
            try:
                data = json.loads(s[len("SAVE_NOTE:"):])
                note = data.get("note", "").strip()
                if note:
                    profile = get_user(user_id) or default_profile()
                    notes = profile.get("notes", [])
                    timestamp = datetime.now(USER_TZ).strftime("%Y-%m-%d")
                    notes.append(f"{timestamp}: {note}")
                    profile["notes"] = notes[-20:]  # keep last 20
                    save_user(user_id, profile)
            except Exception as e:
                logger.warning(f"SAVE_NOTE error: {e}")

        elif s.startswith("PR_UPDATE:"):
            try:
                data = json.loads(s[len("PR_UPDATE:"):])
                profile = get_user(user_id) or default_profile()
                prs = profile.get("prs", {})
                prs.update(data)
                profile["prs"] = prs
                save_user(user_id, profile)
            except Exception as e:
                logger.warning(f"PR_UPDATE error: {e}")

        elif s.startswith("WEEKLY_PLAN:"):
            try:
                plan = json.loads(s[len("WEEKLY_PLAN:"):])
                profile = get_user(user_id) or default_profile()
                profile["weekly_plan"] = {
                    "generated_date": datetime.now(USER_TZ).strftime("%Y-%m-%d"),
                    "plan": plan,
                }
                save_user(user_id, profile)
            except Exception as e:
                logger.warning(f"WEEKLY_PLAN error: {e}")

        elif s == "SEND_STRAVA_LINK":
            # Signal to the caller to send the Strava auth link as a follow-up message
            logger.info(f"SEND_STRAVA_LINK token received for {user_id}")
            profile = get_user(user_id) or default_profile()
            profile["_send_strava_link"] = True
            save_user(user_id, profile)

        else:
            clean_lines.append(line)

    clean_reply = "\n".join(clean_lines).strip()
    return clean_reply, profile_updated


# ─────────────────────────────────────────
#  IMAGE VERIFICATION
# ─────────────────────────────────────────



# ─────────────────────────────────────────
#  CLAUDE
# ─────────────────────────────────────────
async def get_bot_reply(user_id: str, user_message: str) -> tuple:
    profile = get_user(user_id) or default_profile()
    history = profile.get("conversation", [])
    today_str = datetime.now(USER_TZ).strftime("%A %d %B %Y, %H:%M")
    stamped_message = f"[today is {today_str} Stockholm time] {user_message}"
    history.append({"role": "user", "content": stamped_message})

    # Daily message counter - soft limits
    count = increment_daily_count(user_id)
    usage_note = get_usage_modifier(user_id)

    # One-time gentle note when crossing soft limit
    warn = should_warn_usage(user_id)
    if warn:
        user_message = user_message + " [SYSTEM: user has been very active today - mention casually once that you're always here but they should get off their phone and train 😄]"

    # Smart model routing - Haiku for simple chat, Sonnet for coaching/analysis
    coaching_keywords = [
        "plan","training","pace","race","marathon","km","run","week","injury",
        "tired","sore","progress","improve","interval","tempo","long run","taper",
        "heart rate","shoes","nutrition","weight","trend","last","history",
        "how was","compare","faster","slower","recovery","effort","zone","session","schedule"
    ]
    last_msg = user_message.lower()
    is_coaching = any(kw in last_msg for kw in coaching_keywords) or len(last_msg) > 120
    model = "claude-sonnet-4-20250514" if is_coaching else "claude-haiku-4-5-20251001"

    system = build_system_prompt(profile, user_message) + usage_note

    loop = asyncio.get_event_loop()
    response = await loop.run_in_executor(None, lambda: client.messages.create(
        model=model,
        max_tokens=600,
        system=system,
        messages=history,
    ))

    raw_reply = response.content[0].text

    clean_reply, profile_updated = parse_and_apply(user_id, raw_reply)

    profile = get_user(user_id) or default_profile()
    history.append({"role": "assistant", "content": raw_reply})
    if len(history) > 20:
        history = history[-20:]
    profile["conversation"] = history
    save_user(user_id, profile)

    return clean_reply, profile_updated


# ─────────────────────────────────────────
#  TYPING + SENDING
# ─────────────────────────────────────────
# Tracks active sending tasks per user so we can cancel them instantly
user_send_tasks: dict = {}  # {user_id: asyncio.Task}

# Daily message tracking - soft limits, never hard stop
user_daily_messages: dict = {}  # {user_id: {"date": "YYYY-MM-DD", "count": 0, "warned": False}}

SOFT_LIMIT = 30   # normal below this
SLOW_LIMIT = 50   # add delay + one-time note between 30-50
# above 50: shorter responses, wrap up naturally

def get_daily_count(user_id: str) -> int:
    today = datetime.now(USER_TZ).strftime("%Y-%m-%d")
    entry = user_daily_messages.get(user_id, {})
    if entry.get("date") != today:
        user_daily_messages[user_id] = {"date": today, "count": 0, "warned": False}
        return 0
    return entry.get("count", 0)

def increment_daily_count(user_id: str) -> int:
    today = datetime.now(USER_TZ).strftime("%Y-%m-%d")
    entry = user_daily_messages.get(user_id, {})
    if entry.get("date") != today:
        entry = {"date": today, "count": 0, "warned": False}
    entry["count"] = entry.get("count", 0) + 1
    user_daily_messages[user_id] = entry
    return entry["count"]

def should_warn_usage(user_id: str) -> bool:
    """Returns True once when user crosses SOFT_LIMIT - never again that day."""
    entry = user_daily_messages.get(user_id, {})
    count = entry.get("count", 0)
    if count >= SOFT_LIMIT and not entry.get("warned", False):
        entry["warned"] = True
        user_daily_messages[user_id] = entry
        return True
    return False

def get_usage_modifier(user_id: str) -> str:
    """Returns a system note to inject based on usage level."""
    count = get_daily_count(user_id)
    if count > SLOW_LIMIT:
        return " [keep this response short and wrap up naturally - user has been very active today]"
    return ""

def estimate_cost(input_tokens: int, output_tokens: int) -> float:
    cost_per_1k_in = 0.003
    cost_per_1k_out = 0.015
    return (input_tokens / 1000 * cost_per_1k_in) + (output_tokens / 1000 * cost_per_1k_out)

def strip_markdown(text: str) -> str:
    """Remove markdown formatting that leaks into Telegram messages."""
    import re
    text = re.sub(r'\*\*(.+?)\*\*', r'\1', text)   # **bold**
    text = re.sub(r'\*(.+?)\*', r'\1', text)         # *italic*
    text = re.sub(r'__(.+?)__', r'\1', text)          # __bold__
    text = re.sub(r'_(.+?)_', r'\1', text)            # _italic_
    text = re.sub(r'`(.+?)`', r'\1', text)            # `code`
    text = re.sub(r'#{1,6}\s+', '', text)             # ## headers
    return text

def split_into_messages(text: str) -> list:
    """
    Split on blank lines (double newline = new message).
    Single newlines stay within the same message.
    Only a blank separator line creates a new bubble.
    """
    text = strip_markdown(text)
    sep = "\n\n"
    paragraphs = [p.strip() for p in text.split(sep) if p.strip()]
    return paragraphs if paragraphs else [text]

async def _send_chunks(bot, chat_id: int, chunks: list, reply_fn=None):
    """Sends chunks with natural typing delays. Designed to be cancellable."""
    for i, chunk in enumerate(chunks):
        # Show typing indicator
        await bot.send_chat_action(chat_id=chat_id, action="typing")
        # Typing duration: ~0.05s per char, capped between 0.4s and 2.5s
        typing_time = max(0.4, min(len(chunk) * 0.05, 2.5))
        await asyncio.sleep(typing_time)
        if i == 0 and reply_fn:
            await reply_fn(chunk)
        else:
            await bot.send_message(chat_id=chat_id, text=chunk)
        # Small gap between bubbles so they don't arrive simultaneously
        if i < len(chunks) - 1:
            await asyncio.sleep(0.3)


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
            logger.info(f"Send cancelled for {user_id} - user replied")
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
    if datetime.now(USER_TZ).weekday() not in profile.get("workout_days", []):
        return

    hour = datetime.now(USER_TZ).hour
    days_left = days_until_race(profile)
    race_context = f" they have a race in {days_left} days." if days_left > 0 else ""

    # Fetch weather by city or lat/lon
    weather_context = ""
    city = profile.get("city", "")
    lat = profile.get("lat")
    lon = profile.get("lon")
    if city:
        weather = await get_weather_by_city(city)
        if weather:
            weather_context = f" current weather in {city}: {weather}."
    elif lat and lon:
        weather = await get_weather(lat, lon)
        if weather:
            weather_context = f" current weather: {weather}."

    if hour < 10:
        trigger = f"morning of a training day.{race_context}{weather_context} send a short punchy morning hype. if weather is bad mention it but still push them."
    elif hour < 15:
        trigger = f"midday on a training day.{race_context}{weather_context} check if they trained yet."
    else:
        trigger = f"evening on a training day.{race_context}{weather_context} last chance, don't let them skip."

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
            f"[SYSTEM: sunday evening weekly summary.{race_note} "
            f"here are their stats: {format_full_stats(profile)}. "
            f"structure as 2-3 bubbles (blank line between each): "
            f"bubble 1 = the numbers (sessions, km vs last week), "
            f"bubble 2 = coaching take + focus for next week + race context if applicable, "
            f"bubble 3 = one question. keep each bubble tight.]"
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
        time=time(hour=19, minute=0, tzinfo=USER_TZ),
        days=(6,),
        name="weekly_summary",
        data={},
    )
    app.job_queue.run_daily(
        check_ghosts,
        time=time(hour=10, minute=0),
        name="ghost_checker",
        data={},
    )
    app.job_queue.run_daily(
        daily_shoe_sync,
        time=time(hour=3, minute=0),
        name="daily_shoe_sync",
        data={},
    )
    app.job_queue.run_daily(
        daily_run_sync,
        time=time(hour=3, minute=30),
        name="daily_run_sync",
        data={},
    )
    logger.info("Restored all jobs + weekly summary + ghost checker + shoe sync + run sync.")


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
    if user_id not in ALLOWED_USERS:
        return
    await context.bot.send_chat_action(chat_id=update.effective_chat.id, action="typing")
    reply, updated = await get_bot_reply(user_id, "[SYSTEM: user sent a workout photo unprompted. react positively, hype them up, keep it short - 1-2 bubbles max.]")
    await send_with_typing(context.bot, update.effective_chat.id, reply, update.message.reply_text, user_id=user_id)
    if updated:
        await reschedule_user(user_id, updated, context.application)


# Per-user message queues and worker tasks
user_queues: dict = {}      # {user_id: asyncio.Queue}
user_workers: dict = {}     # {user_id: asyncio.Task}

async def process_user_messages(user_id: str, app):
    """Single worker per user - processes one message at a time, skips stale ones."""
    queue = user_queues[user_id]
    while True:
        update, context = await queue.get()
        try:
            # Drain queue - if more messages waiting, skip straight to the latest
            while not queue.empty():
                update, context = queue.get_nowait()

            if not get_user(user_id):
                save_user(user_id, default_profile())

            profile = get_user(user_id)

            # Update last active
            profile['last_active'] = datetime.now(USER_TZ).strftime('%Y-%m-%d')
            save_user(user_id, profile)

            text = update.message.text

            # Detect city from message directly
            import re as _re
            city_match = _re.search(
                r"(?:i(?:'m| am) (?:in|based in|from)|i live in|based in|from|living in|i(?:'m| am) from|city is|located in|i(?:'m| am) at)\s+([A-Z][a-zA-Z]+(?:\s[A-Z][a-zA-Z]+)?)",
                text, _re.IGNORECASE
            )
            if city_match:
                detected_city = city_match.group(1).strip().title()
                if detected_city != profile.get('city'):
                    profile['city'] = detected_city
                    save_user(user_id, profile)
                    logger.info(f'Auto-detected city: {detected_city} for {user_id}')

            # Inject live weather as context prefix - invisible to user
            city = profile.get("city", "")
            weather_prefix = ""
            if city:
                weather = await get_weather_by_city(city)
                if weather:
                    weather_prefix = f"[SYSTEM CONTEXT - do NOT repeat this back: weather in {city} right now is {weather}. use this naturally only when relevant.]\n"
            text = weather_prefix + text

            # Natural language weekly summary trigger
            summary_phrases = [
                "weekly summary", "week summary", "how was my week", "my week",
                "recap", "week recap", "how did i do this week", "this week's stats",
                "show me my stats", "how's my training", "training summary"
            ]
            raw_text = update.message.text.lower()
            if any(p in raw_text for p in summary_phrases):
                days_left = days_until_race(profile)
                race_context = f" race in {days_left} days." if days_left > 0 else ""
                text = f"[SYSTEM: user asked for their weekly summary.{race_context} structure as 2-3 bubbles (blank line between each): bubble 1 = the numbers (sessions, km, vs last week), bubble 2 = coaching take + what to change, bubble 3 = one question. tight and direct.]"


            # Natural language Strava connect trigger — only fire on clear intent
            strava_phrases = ["connect strava", "link strava", "sync strava", "strava connect",
                              "how do i connect strava", "how do i link strava", "add strava",
                              "connect my strava", "link my strava", "yes strava", "yeah strava",
                              "i want to connect strava", "i use strava", "i have strava"]
            if any(p in raw_text for p in strava_phrases):
                auth_url = get_strava_auth_url(user_id)
                await send_with_typing(context.bot, update.effective_chat.id,
                    f"tap this to connect Strava 👇\n{auth_url}\n\nonce connected i'll automatically see all your runs, pace, heart rate, everything - no manual logging ever. takes 30 seconds.",
                    update.message.reply_text, user_id=user_id)
                continue

            # Natural language sync history trigger
            sync_phrases = ["sync history", "import runs", "sync my runs", "reimport",
                           "sync strava history", "update my runs"]
            if any(p in raw_text for p in sync_phrases):
                await send_with_typing(context.bot, update.effective_chat.id,
                    "syncing your Strava history now - give me a sec",
                    update.message.reply_text, user_id=user_id)
                await sync_strava_history(user_id, get_user(user_id))
                text = "[SYSTEM: just finished syncing user's Strava history. confirm it's done, mention how many runs came in if you know, keep it short.]"

            # Show typing immediately - before Claude even starts
            await context.bot.send_chat_action(chat_id=update.effective_chat.id, action="typing")

            reply, updated_profile = await get_bot_reply(user_id, text)

            # Before sending - check if another message arrived while Claude was thinking
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

            # Auto-send Strava link if Claude flagged it
            fresh_profile = get_user(user_id)
            if fresh_profile and fresh_profile.pop("_send_strava_link", False):
                save_user(user_id, fresh_profile)
                auth_url = get_strava_auth_url(user_id)
                logger.info(f"Sending Strava link to {user_id}: {auth_url[:50]}...")
                await asyncio.sleep(0.8)
                await context.bot.send_message(
                    chat_id=update.effective_chat.id,
                    text=f"tap this to connect Strava 👇\n{auth_url}\n\nonce connected i'll automatically see all your runs, pace, heart rate, everything - no manual logging ever. takes 30 seconds."
                )
            else:
                logger.info(f"No Strava link flag for {user_id}, _send_strava_link={fresh_profile.get('_send_strava_link') if fresh_profile else 'no profile'}")

            # Wrap-up message when onboarding just completed
            fresh_profile = get_user(user_id)
            if fresh_profile and fresh_profile.pop("_just_onboarded", False):
                save_user(user_id, fresh_profile)
                days = [DAY_NAMES[d] for d in fresh_profile.get("workout_days", [])]
                times = fresh_profile.get("hype_times", [])
                days_str = ", ".join(days) if days else "your training days"
                time_str = times[0] if times else "your scheduled time"
                race = fresh_profile.get("race", {})
                race_str = f"{race['name']} is {days_until_race(fresh_profile)} days away" if race.get("name") else ""
                bot_name = fresh_profile.get("bot_name", "Pacer")
                user_name = fresh_profile.get("name", "")
                await asyncio.sleep(1.2)
                wrap_trigger = (
                    f"[SYSTEM: onboarding just completed for {user_name}. send a punchy wrap-up across 3-4 short bubbles. "
                    f"cover these things naturally - not as a list: "
                    f"1) you're all set, here's what happens next: i'll text you on {days_str} at {time_str} "
                    f"2) weekly recap every sunday evening so they always know where they're at "
                    f"{f'3) race context: {race_str}, time to build toward it ' if race_str else ''}"
                    f"3) they can change ANYTHING just by telling you - their name, your name ({bot_name}), training days, goals, everything. just chat. "
                    f"4) end with one punchy line that makes them excited - something forward-looking and personal to their goal. "
                    f"make it feel like the start of something. warm, energetic, not corporate.]"
                )
                wrap_reply, _ = await get_bot_reply(user_id, wrap_trigger)
                await send_with_typing(context.bot, update.effective_chat.id, wrap_reply, user_id=user_id)

            if updated_profile and updated_profile.get("hype_times"):
                await reschedule_user(user_id, updated_profile, app)

        except Exception as e:
            logger.error(f"Worker error for {user_id}: {e}", exc_info=True)
            try:
                await context.bot.send_message(
                    chat_id=update.effective_chat.id,
                    text="something went wrong on my end - try again in a sec"
                )
            except Exception:
                pass
        finally:
            queue.task_done()


async def handle_location(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Save user location for weather updates."""
    user_id = str(update.effective_user.id)
    loc = update.message.location
    profile = get_user(user_id) or default_profile()
    profile["lat"] = loc.latitude
    profile["lon"] = loc.longitude
    save_user(user_id, profile)
    reply, _ = await get_bot_reply(user_id, "[SYSTEM: user just shared their location so we can send weather-aware hype messages. confirm it's saved, be brief and hype.]")
    await send_with_typing(context.bot, update.effective_chat.id, reply, update.message.reply_text, user_id=user_id)


async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = str(update.effective_user.id)
    if user_id not in ALLOWED_USERS:
        logger.info(f"Blocked unknown user {user_id}")
        return  # silently ignore

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

# ─────────────────────────────────────────
#  STRAVA WEBHOOK
# ─────────────────────────────────────────
from aiohttp import web

STRAVA_CLIENT_ID = os.environ.get("STRAVA_CLIENT_ID", "")

def get_strava_auth_url(user_id: str) -> str:
    base_url = os.environ.get("RAILWAY_PUBLIC_DOMAIN", "http://localhost:8080")
    if not base_url.startswith("http"):
        base_url = "https://" + base_url
    return (
        f"https://www.strava.com/oauth/authorize"
        f"?client_id={STRAVA_CLIENT_ID}"
        f"&redirect_uri={base_url}/strava/auth"
        f"&response_type=code"
        f"&scope=read_all,activity:read_all,profile:read_all"
        f"&state={user_id}"
        f"&approval_prompt=auto"
    )
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
    if datetime.now(USER_TZ).timestamp() >= expires - 60:
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
        if activity_type not in ("run", "virtualrun", "trailrun", "ride", "virtualride", "ebikeride", "crossfit", "workout", "weighttraining"):
            return

        # Sync all shoes from athlete profile
        asyncio.create_task(sync_strava_shoes(user_id, profile))

        # Sync shoe from Strava gear
        gear = activity.get("gear", {})
        if gear and gear.get("id"):
            profile = get_user(user_id)
            shoes = profile.get("shoes", [])
            gear_id = gear["id"]
            gear_name = gear.get("name", "Unknown shoe")
            gear_km = round((gear.get("converted_distance", 0)), 1)
            existing_ids = [s.get("strava_gear_id") for s in shoes]
            if gear_id not in existing_ids:
                shoes.append({"name": gear_name, "km": gear_km, "strava_gear_id": gear_id})
            else:
                for shoe in shoes:
                    if shoe.get("strava_gear_id") == gear_id:
                        shoe["km"] = gear_km
            profile["shoes"] = shoes
            save_user(user_id, profile)

        # Parse activity data
        distance_km = round(activity.get("distance", 0) / 1000, 2)
        duration_min = round(activity.get("elapsed_time", activity.get("moving_time", 0)) / 60, 1)
        avg_hr = activity.get("average_heartrate", 0)
        name = activity.get("name", "activity")
        is_ride = activity_type in ("ride", "virtualride", "ebikeride")
        is_gym = activity_type in ("crossfit", "workout", "weighttraining")

        # Calculate pace (runs) or speed (cycling) — gym has neither
        pace_str = ""
        if distance_km > 0 and duration_min > 0:
            if is_ride:
                speed_kmh = round(distance_km / (duration_min / 60), 1)
                pace_str = f"{speed_kmh}km/h"
            elif not is_gym:
                pace_sec_per_km = (duration_min * 60) / distance_km
                pace_min = int(pace_sec_per_km // 60)
                pace_sec = int(pace_sec_per_km % 60)
                pace_str = f"{pace_min}:{pace_sec:02d}"

        # Determine stored type
        if is_ride:
            stored_type = "ride"
        elif is_gym:
            stored_type = "crossfit" if activity_type == "crossfit" else "gym"
        else:
            stored_type = "run"

        # Log the session
        cadence = activity.get("average_cadence", 0)
        elevation = activity.get("total_elevation_gain", 0)
        suffer_score = activity.get("suffer_score", 0)
        max_hr = activity.get("max_heartrate", 0)

        run_name = activity.get("name", "")
        session_data = {
            "type": stored_type,
            "name": run_name,
            "distance_km": distance_km,
            "duration_min": duration_min,
            "pace_per_km": pace_str,
            "heart_rate": int(avg_hr) if avg_hr else 0,
            "max_heart_rate": int(max_hr) if max_hr else 0,
            "cadence": int(cadence) if cadence else 0,
            "elevation_m": round(elevation, 0) if elevation else 0,
            "suffer_score": suffer_score,
            "effort": min(10, round(suffer_score / 20)) if suffer_score else 0,
            "notes": f"via Strava: {name}",
        }
        log_session(user_id, session_data)

        # Check for PR - compare against stored PRs
        pr_context = ""
        prs = profile.get("prs", {})
        duration_sec = activity.get("moving_time", 0)
        if duration_sec > 0:
            dist = activity.get("distance", 0)
            # Check if it's close to a standard race distance (within 5%)
            race_distances = {"5k": 5000, "10k": 10000, "half": 21097, "marathon": 42195}
            for race_name, race_dist in race_distances.items():
                if abs(dist - race_dist) / race_dist < 0.05:
                    mins = duration_sec // 60
                    secs = duration_sec % 60
                    time_str = f"{mins}:{secs:02d}"
                    current_pr = prs.get(race_name)
                    if not current_pr:
                        pr_context = f" this was their first logged {race_name} time: {time_str} - treat it as a PR!"
                    else:
                        # Compare times
                        try:
                            pr_parts = current_pr.split(":")
                            pr_sec = int(pr_parts[0])*60 + int(pr_parts[1])
                            if duration_sec < pr_sec:
                                pr_context = f" NEW {race_name.upper()} PR: {time_str} (previous: {current_pr}) - go absolutely crazy!"
                            elif pr_sec - duration_sec < 30:
                                pr_context = f" they were {pr_sec - duration_sec} seconds off their {race_name} PR of {current_pr} - so close!"
                        except: pass
                    break

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
            f"activity: {details}.{pr_context} "
            f"react in your style - comment on the pace/distance flatly, "
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
    OAuth callback - Strava redirects here after user authorises.
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

    # Sync shoes + history immediately - await shoes so they're ready before we notify
    await sync_strava_shoes(telegram_user_id, profile)
    asyncio.create_task(sync_strava_history(telegram_user_id, profile))

    # Reload profile so notification includes fresh shoe + run data
    profile = get_user(telegram_user_id) or profile

    # Notify user in Telegram
    try:
        from telegram import Bot
        bot = Bot(token=TELEGRAM_TOKEN)
        shoes = profile.get("shoes", [])
        shoe_info = ", ".join([f"{s['name']} ({s['km']}km)" for s in shoes]) if shoes else "no shoes found"
        reply, _ = await get_bot_reply(
            telegram_user_id,
            f"[SYSTEM: user just linked Strava. shoes synced: {shoe_info}. runs will auto-sync going forward. celebrate it, mention their shoes if found, keep it short and hype.]"
        )
        await send_with_typing(bot, int(telegram_user_id), reply, user_id=telegram_user_id)
    except Exception as e:
        logger.warning(f"Could not notify user after Strava auth: {e}")

    return web.Response(text="✅ Strava connected! Head back to Telegram.", content_type="text/html")


# ─────────────────────────────────────────
#  WEATHER
# ─────────────────────────────────────────
async def get_weather_by_city(city: str) -> str:
    """Fetch weather using city name - no API key needed."""
    import aiohttp
    try:
        async with aiohttp.ClientSession() as session:
            geo_url = f"https://geocoding-api.open-meteo.com/v1/search?name={city}&count=1"
            async with session.get(geo_url) as resp:
                if resp.status != 200: return ""
                geo = await resp.json()
                results = geo.get("results", [])
                if not results: return ""
                lat = results[0]["latitude"]
                lon = results[0]["longitude"]
            wx_url = (
                f"https://api.open-meteo.com/v1/forecast"
                f"?latitude={lat}&longitude={lon}"
                f"&current=temperature_2m,weathercode,windspeed_10m,precipitation"
                f"&timezone=auto"
            )
            async with session.get(wx_url) as resp:
                if resp.status != 200: return ""
                data = await resp.json()
                curr = data.get("current", {})
                temp = curr.get("temperature_2m", "?")
                wind = curr.get("windspeed_10m", 0)
                precip = curr.get("precipitation", 0)
                code = curr.get("weathercode", 0)
                if code == 0: desc = "clear"
                elif code <= 3: desc = "partly cloudy"
                elif code <= 48: desc = "foggy"
                elif code <= 67: desc = "rainy"
                elif code <= 77: desc = "snowy"
                elif code <= 82: desc = "heavy rain"
                else: desc = "stormy"
                result = f"{int(temp)}C, {desc}"
                if wind > 25: result += f", very windy ({int(wind)}km/h)"
                elif wind > 15: result += ", a bit windy"
                if precip > 0: result += f", {precip}mm rain"
                return result
    except Exception as e:
        logger.warning(f"Weather fetch failed: {e}")
    return ""


async def get_weather(lat: float, lon: float) -> str:
    """Fetch weather by lat/lon fallback."""
    import aiohttp
    try:
        url = (
            f"https://api.open-meteo.com/v1/forecast"
            f"?latitude={lat}&longitude={lon}"
            f"&current=temperature_2m,weathercode,windspeed_10m,precipitation"
            f"&timezone=auto"
        )
        async with aiohttp.ClientSession() as session:
            async with session.get(url) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    curr = data.get("current", {})
                    temp = curr.get("temperature_2m", "?")
                    wind = curr.get("windspeed_10m", 0)
                    code = curr.get("weathercode", 0)
                    if code == 0: desc = "clear"
                    elif code <= 3: desc = "partly cloudy"
                    elif code <= 67: desc = "rainy"
                    elif code <= 77: desc = "snowy"
                    else: desc = "stormy"
                    result = f"{int(temp)}C, {desc}"
                    if wind > 15: result += ", windy"
                    return result
    except Exception as e:
        logger.warning(f"Weather fetch failed: {e}")
    return ""


# ─────────────────────────────────────────
#  GHOST CHECKER - runs daily
# ─────────────────────────────────────────
async def daily_shoe_sync(context: ContextTypes.DEFAULT_TYPE):
    """Sync shoes for all Strava-connected users every night."""
    users = load_users()
    for user_id, profile in users.items():
        if profile.get("strava_access_token"):
            await sync_strava_shoes(user_id, profile)


async def daily_run_sync(context: ContextTypes.DEFAULT_TYPE):
    """Sync recent runs for all Strava-connected users every night."""
    users = load_users()
    for user_id, profile in users.items():
        if profile.get("strava_access_token"):
            added = await sync_strava_history(user_id, profile, pages=1)  # last 30 runs
            if added:
                logger.info(f"Daily sync: added {added} new runs for {user_id}")


async def check_ghosts(context: ContextTypes.DEFAULT_TYPE):
    """Check for inactive users and send increasingly desperate messages."""
    users = load_users()
    for user_id, profile in users.items():
        if not profile.get("onboarded"):
            continue
        last_active = profile.get("last_active", "")
        if not last_active:
            continue
        try:
            last_dt = datetime.strptime(last_active, "%Y-%m-%d")
            days_gone = (datetime.now(USER_TZ) - last_dt).days
        except:
            continue

        if days_gone in (3, 7, 10):  # only ping on specific days, not every day
            if days_gone <= 5:
                trigger = f"[SYSTEM: user has ghosted for {days_gone} days. mild roast, ask where they've been. short.]"
            elif days_gone <= 9:
                trigger = f"[SYSTEM: user has ghosted for {days_gone} days. more dramatic, genuinely worried. short.]"
            else:
                trigger = f"[SYSTEM: user has ghosted for {days_gone} days. full breakdown. act personally hurt. guilt trip them lovingly. short.]"

            reply, _ = await get_bot_reply(user_id, trigger)
            try:
                await send_with_typing(context.application.bot, int(user_id), reply, user_id=user_id)
            except Exception as e:
                logger.warning(f"Ghost message failed for {user_id}: {e}")


async def sync_strava_history(user_id: str, profile: dict, pages: int = 3):
    """Fetch last ~90 runs from Strava and backfill session history."""
    import aiohttp
    profile = get_user(user_id) or profile
    access_token = await get_valid_strava_token(user_id, profile)
    if not access_token:
        return
    try:
        all_activities = []
        async with aiohttp.ClientSession() as session:
            for page in range(1, pages + 1):
                url = (
                    f"https://www.strava.com/api/v3/athlete/activities"
                    f"?per_page=30&page={page}"
                )
                headers = {"Authorization": f"Bearer {access_token}"}
                async with session.get(url, headers=headers) as resp:
                    body = await resp.json()
                    logger.info(f"Strava activities page {page} status: {resp.status}")
                    if resp.status != 200:
                        logger.warning(f"Strava activities error: {body}")
                        break
                    if not body:
                        break
                    all_activities.extend(body)

        profile = get_user(user_id) or profile
        stats = get_stats(profile)
        existing_dates = {s["date"] for s in stats["sessions"]}
        added = 0

        for act in all_activities:
            act_type = act.get("type", "").lower()
            if act_type not in ("run", "virtualrun", "trailrun", "ride", "virtualride", "ebikeride", "crossfit", "workout", "weighttraining"):
                continue

            date_str = act.get("start_date_local", "")[:10]
            if date_str in existing_dates:
                logger.info(f"Skipping {date_str} - already exists")
                continue

            distance_km = round(act.get("distance", 0) / 1000, 2)
            duration_min = round(act.get("elapsed_time", act.get("moving_time", 0)) / 60, 1)
            avg_hr = act.get("average_heartrate", 0)
            max_hr = act.get("max_heartrate", 0)
            cadence = act.get("average_cadence", 0)
            elevation = act.get("total_elevation_gain", 0)
            suffer_score = act.get("suffer_score", 0)
            is_ride = act_type in ("ride", "virtualride", "ebikeride")

            pace_str = ""
            if distance_km > 0 and duration_min > 0:
                if is_ride:
                    speed_kmh = round(distance_km / (duration_min / 60), 1)
                    pace_str = f"{speed_kmh}km/h"
                else:
                    pace_sec = (duration_min * 60) / distance_km
                    pace_str = f"{int(pace_sec//60)}:{int(pace_sec%60):02d}"

            run_name = act.get("name", "")
            session = {
                "date": date_str,
                "type": "ride" if is_ride else "run",
                "name": run_name,
                "muscle": "",
                "distance_km": distance_km,
                "duration_min": duration_min,
                "pace_per_km": pace_str,
                "heart_rate": int(avg_hr) if avg_hr else 0,
                "max_heart_rate": int(max_hr) if max_hr else 0,
                "cadence": int(cadence) if cadence else 0,
                "elevation_m": round(elevation, 0) if elevation else 0,
                "suffer_score": suffer_score,
                "effort": min(10, round(suffer_score / 20)) if suffer_score else 0,
                "notes": f"via Strava: {act.get('name', 'run')}",
            }
            stats["sessions"].append(session)
            stats["total_sessions"] += 1
            existing_dates.add(date_str)

            # Update weekly mileage
            if distance_km:
                try:
                    week_key = datetime.strptime(date_str, "%Y-%m-%d").strftime("%Y-W%W")
                    stats["weekly_mileage"][week_key] = stats["weekly_mileage"].get(week_key, 0) + distance_km
                except: pass

            added += 1

        # Sort sessions by date
        stats["sessions"].sort(key=lambda x: x["date"], reverse=True)
        stats["total_sessions"] = len([s for s in stats["sessions"]])
        profile["stats"] = stats
        save_user(user_id, profile)
        logger.info(f"Imported {added} historical runs for {user_id}")
        return added
    except Exception as e:
        logger.warning(f"History sync error for {user_id}: {e}")
        return 0


async def sync_strava_shoes(user_id: str, profile: dict):
    """Pull all shoes from Strava athlete profile."""
    import aiohttp
    # Always reload profile to get latest tokens
    profile = get_user(user_id) or profile
    access_token = await get_valid_strava_token(user_id, profile)
    if not access_token:
        logger.warning(f"No access token for shoe sync - user {user_id}")
        return
    try:
        url = "https://www.strava.com/api/v3/athlete"
        headers = {"Authorization": f"Bearer {access_token}"}
        async with aiohttp.ClientSession() as session:
            async with session.get(url, headers=headers) as resp:
                body = await resp.json()
                if resp.status != 200:
                    logger.warning(f"Strava athlete fetch failed: {resp.status} - {body}")
                    return
                # Log full athlete response keys for debugging
                gear_list = body.get("shoes", [])
                # Also check if gear is under a different key
                all_gear = body.get("gear", [])
                shoes = []
                for g in gear_list + all_gear:
                    name = g.get("name") or g.get("nickname") or g.get("description") or "Unnamed shoe"
                    km = round(g.get("converted_distance", g.get("distance", 0)), 1)
                    retired = g.get("retired", False)
                    shoes.append({
                        "name": name,
                        "km": km,
                        "strava_gear_id": g.get("id", ""),
                        "retired": retired,
                    })
                profile = get_user(user_id) or profile
                profile["shoes"] = shoes
                save_user(user_id, profile)
                if shoes or get_stats(profile).get('sessions'):
                    profile['onboarded'] = True
                    save_user(user_id, profile)
                logger.info(f"Synced {len(shoes)} shoes for {user_id}: {[s['name'] for s in shoes]}")
    except Exception as e:
        logger.warning(f"Shoe sync error for {user_id}: {e}")


async def handle_health(request: web.Request) -> web.Response:
    """Receives health data from Health Auto Export app."""
    try:
        data = await request.json()
        logger.info(f"Health webhook received: {list(data.keys()) if isinstance(data, dict) else type(data)}")

        # Health Auto Export sends data keyed by metric name
        # Find user by telegram ID in query params, or use first user if solo
        user_id = request.rel_url.query.get("user_id", "")
        if not user_id:
            users = load_users()
            if not users: return web.Response(status=200, text="ok")
            user_id = list(users.keys())[0]  # default to first user for solo use

        profile = get_user(user_id)
        if not profile: return web.Response(status=200, text="ok")

        health = profile.get("health", {})

        # Parse Health Auto Export format
        # It sends lists of datapoints per metric
        metrics = data.get("data", data)  # handle both formats

        def latest_value(metric_data):
            """Get most recent value from a list of datapoints."""
            if isinstance(metric_data, list) and metric_data:
                # Sort by date descending, take first
                try:
                    sorted_data = sorted(metric_data, key=lambda x: x.get("date", ""), reverse=True)
                    val = sorted_data[0].get("qty") or sorted_data[0].get("value")
                    return float(val) if val is not None else None
                except: pass
            elif isinstance(metric_data, (int, float)):
                return float(metric_data)
            return None

        # Map Health Auto Export metric names to our fields
        metric_map = {
            "sleep_analysis": "sleep_hours",
            "sleepAnalysis": "sleep_hours",
            "heart_rate_variability": "hrv",
            "heartRateVariability": "hrv",
            "hrv": "hrv",
            "resting_heart_rate": "resting_hr",
            "restingHeartRate": "resting_hr",
            "body_mass": "weight_kg",
            "bodyMass": "weight_kg",
            "weight": "weight_kg",
            "step_count": "steps",
            "stepCount": "steps",
            "steps": "steps",
        }

        updated = []
        for key, field in metric_map.items():
            if key in metrics:
                val = latest_value(metrics[key])
                if val is not None:
                    health[field] = round(val, 1)
                    updated.append(field)

        # Update baselines (rolling 30-day for HRV and resting HR)
        if health.get("hrv"):
            baseline = health.get("hrv_baseline") or health["hrv"]
            health["hrv_baseline"] = round(baseline * 0.9 + health["hrv"] * 0.1, 1)
        if health.get("resting_hr"):
            baseline = health.get("resting_hr_baseline") or health["resting_hr"]
            health["resting_hr_baseline"] = round(baseline * 0.9 + health["resting_hr"] * 0.1, 1)

        health["last_updated"] = datetime.now(USER_TZ).strftime("%Y-%m-%d %H:%M")
        profile["health"] = health
        save_user(user_id, profile)
        logger.info(f"Health data updated for {user_id}: {updated}")

        return web.Response(status=200, text="ok")
    except Exception as e:
        logger.warning(f"Health webhook error: {e}")
        return web.Response(status=200, text="ok")


async def check_health_alerts(user_id: str, profile: dict):
    """Send proactive health warnings if vitals look concerning."""
    health = profile.get("health", {})
    if not health.get("last_updated"): return

    alerts = []

    hrv = health.get("hrv")
    hrv_baseline = health.get("hrv_baseline")
    if hrv and hrv_baseline and hrv < hrv_baseline * 0.75:
        alerts.append(f"HRV is {hrv}ms vs your baseline of {hrv_baseline}ms - that's a 25%+ drop")

    rhr = health.get("resting_hr")
    rhr_baseline = health.get("resting_hr_baseline")
    if rhr and rhr_baseline and rhr > rhr_baseline * 1.07:
        alerts.append(f"resting HR is {rhr}bpm vs your baseline of {rhr_baseline}bpm - elevated")

    sleep = health.get("sleep_hours")
    if sleep and sleep < 6:
        alerts.append(f"only {sleep}h sleep last night")

    if not alerts: return

    # Check if today is a planned training day
    today_weekday = datetime.now(USER_TZ).weekday()
    is_training_day = today_weekday in profile.get("workout_days", [])

    alert_text = " + ".join(alerts)
    if is_training_day:
        trigger = f"[SYSTEM: health alert detected - {alert_text}. today is a planned training day. warn them to take it easy or consider rest. be direct but not dramatic. max 2 bubbles.]"
    else:
        trigger = f"[SYSTEM: health alert - {alert_text}. not a training day. mention it casually, suggest good recovery habits.]"

    try:
        from telegram import Bot
        bot = Bot(token=TELEGRAM_TOKEN)
        reply, _ = await get_bot_reply(user_id, trigger)
        for bubble in split_into_bubbles(reply):
            await bot.send_message(chat_id=int(user_id), text=bubble)
    except Exception as e:
        logger.warning(f"Health alert send failed: {e}")

def start_webhook_server():
    """Start aiohttp server for Strava webhooks alongside the Telegram bot."""
    app = web.Application()
    app.router.add_get("/", handle_health)
    app.router.add_get("/health", handle_health)
    app.router.add_post("/health", handle_health)
    app.router.add_get("/strava/webhook", handle_strava_webhook)
    app.router.add_post("/strava/webhook", handle_strava_webhook)
    app.router.add_get("/strava/auth", handle_strava_auth)
    return app


async def strava_history_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Manually import last 90 runs from Strava."""
    user_id = str(update.effective_user.id)
    profile = get_user(user_id)
    if not profile or not profile.get("strava_access_token"):
        await update.message.reply_text("connect Strava first with /strava")
        return
    await update.message.reply_text("importing your run history from Strava...")
    # Clear existing sessions so we don't skip anything
    profile = get_user(user_id) or profile
    profile.setdefault("stats", {})["sessions"] = []
    profile["stats"]["total_sessions"] = 0
    profile["stats"]["weekly_mileage"] = {}
    save_user(user_id, profile)
    added = await sync_strava_history(user_id, profile, pages=3)
    # Clear conversation history so Claude reads fresh profile data
    profile = get_user(user_id) or default_profile()
    profile["conversation"] = []
    save_user(user_id, profile)
    await update.message.reply_text(f"done - imported {added} runs. ask me about your runs now!")


async def strava_sync(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Manually trigger shoe + data sync from Strava."""
    user_id = str(update.effective_user.id)
    profile = get_user(user_id)
    if not profile or not profile.get("strava_access_token"):
        await update.message.reply_text("you haven't connected Strava yet - use /strava to link it")
        return
    await update.message.reply_text("syncing your Strava data...")
    await sync_strava_shoes(user_id, profile)
    profile = get_user(user_id)
    shoes = profile.get("shoes", [])
    if shoes:
        lines = ["got your shoes:"]
        for s in shoes:
            status = "retired" if s.get("retired") else ("⚠️ getting worn" if s["km"] > 550 else "good")
            lines.append(f"  {s['name']} - {s['km']}km ({status})")
        await update.message.reply_text("\n".join(lines))
    else:
        await update.message.reply_text("no shoes found on your Strava - make sure you have shoes added at strava.com/settings/gear")


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
        f"&scope=read_all,activity:read_all,profile:read_all"
        f"&state={user_id}"
        f"&approval_prompt=auto"
    )
    msg = "tap this to connect Strava:\n" + auth_url + "\n\nonce you approve it your runs sync automatically 💪"
    await update.message.reply_text(msg)


async def reset_me_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Wipe user profile completely - admin/testing only."""
    user_id = str(update.effective_user.id)
    if user_id not in ALLOWED_USERS:
        return
    # Clear profile from disk
    users = load_users()
    if user_id in users:
        del users[user_id]
        save_users(users)
    # Clear in-memory daily counter
    if user_id in user_daily_messages:
        del user_daily_messages[user_id]
    # Clear send tasks
    if user_id in user_send_tasks:
        task = user_send_tasks.pop(user_id)
        if not task.done():
            task.cancel()
    # Cancel worker so it doesn't process stale queue
    if user_id in user_workers:
        worker = user_workers.pop(user_id)
        if not worker.done():
            worker.cancel()
    if user_id in user_queues:
        del user_queues[user_id]
    await update.message.reply_text("done - fully wiped. send /start to begin fresh 👋")


async def test_hype_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = str(update.effective_user.id)
    if user_id not in ALLOWED_USERS:
        return
    profile = get_user(user_id)
    if not profile:
        await update.message.reply_text("no profile yet - say hi first")
        return
    hour = datetime.now(USER_TZ).hour
    days_left = days_until_race(profile)
    race_context = f" they have a race in {days_left} days." if days_left > 0 else ""
    city = profile.get("city", "")
    weather_context = ""
    if city:
        weather = await get_weather_by_city(city)
        if weather:
            weather_context = f" current weather in {city}: {weather}."
    if hour < 10:
        trigger = f"morning of a training day.{race_context}{weather_context} send a short morning message."
    elif hour < 15:
        trigger = f"midday on a training day.{race_context}{weather_context} check if they trained yet."
    else:
        trigger = f"evening on a training day.{race_context}{weather_context} last chance push."
    reply, _ = await get_bot_reply(user_id, f"[SYSTEM: {trigger} short and punchy like a real text.]")
    await send_with_typing(context.bot, update.effective_chat.id, reply, update.message.reply_text, user_id=user_id)


async def test_summary_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = str(update.effective_user.id)
    if user_id not in ALLOWED_USERS:
        return
    profile = get_user(user_id)
    if not profile:
        await update.message.reply_text("no profile yet")
        return
    days_left = days_until_race(profile)
    race_context = f" race in {days_left} days." if days_left > 0 else ""
    reply, _ = await get_bot_reply(user_id, f"[SYSTEM: weekly summary time.{race_context} structure as 2-3 bubbles (blank line between each): bubble 1 = the numbers (sessions, km, vs last week), bubble 2 = coaching take + what to change, bubble 3 = one question. tight and direct.]")
    await send_with_typing(context.bot, update.effective_chat.id, reply, update.message.reply_text, user_id=user_id)


def main():
    if not ANTHROPIC_API_KEY:
        raise ValueError("Set ANTHROPIC_API_KEY as an environment variable.")
    if not TELEGRAM_TOKEN:
        raise ValueError("Set TELEGRAM_TOKEN as an environment variable.")

    import threading
    from aiohttp import web

    tg_app = Application.builder().token(TELEGRAM_TOKEN).build()
    tg_app.add_handler(CommandHandler("start", start))
    tg_app.add_handler(CommandHandler("resetme", reset_me_cmd))           # internal testing only
    tg_app.add_handler(CommandHandler("testhype", test_hype_cmd))      # internal testing only
    tg_app.add_handler(CommandHandler("testsummary", test_summary_cmd)) # internal testing only
    tg_app.add_handler(MessageHandler(filters.PHOTO, handle_photo))
    tg_app.add_handler(MessageHandler(filters.LOCATION, handle_location))
    tg_app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))

    async def on_startup(app):
        await restore_all_jobs(app)
    tg_app.post_init = on_startup

    # Run aiohttp webhook server in same event loop
    async def run_both():
        webhook_app = start_webhook_server()
        runner = web.AppRunner(webhook_app)
        await runner.setup()
        port = int(os.environ.get("PORT", os.environ.get("RAILWAY_PORT", "8080")))
        logger.info(f"Starting webhook server on 0.0.0.0:{port}")
        site = web.TCPSite(runner, "0.0.0.0", port)
        await site.start()
        logger.info(f"Strava webhook server running on port {port}")
        await tg_app.initialize()
        await tg_app.start()
        # Drop any updates queued while old instance was running
        await tg_app.updater.start_polling(
            allowed_updates=Update.ALL_TYPES,
            drop_pending_updates=True,
        )
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
