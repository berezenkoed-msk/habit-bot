import asyncio
import os
import re
import random
import datetime as dt
from collections import defaultdict

import aiosqlite
from aiogram import Bot, Dispatcher, F
from aiogram.filters import Command
from aiogram.types import Message
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from aiohttp import web

# =============================
# НАСТРОЙКИ
# =============================
DB_PATH = "habits.db"

FREE_HABIT_LIMIT = 5
PRO_HABIT_LIMIT = 20

FREE_MAX_TIMES_PER_HABIT = 10
PRO_MAX_TIMES_PER_HABIT = 30

BOT_TOKEN = os.getenv("BOT_TOKEN")
ADMIN_ID = int(os.getenv("ADMIN_ID", "0"))
PORT = int(os.getenv("PORT", "10000"))

if not BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN is not set. Add BOT_TOKEN in Render environment variables.")

bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()
scheduler = AsyncIOScheduler()

# =============================
# ТЕКСТЫ (тон "хороший друг")
# =============================
START_TEXT = (
    "Привет! Я рядом, чтобы помочь тебе держать слово самому себе.\n"
    "Я напоминаю — ты отвечаешь *текстом*, как SMS.\n\n"
    "Как проходит чек-ин:\n"
    "— Я спрашиваю по очереди: «Сделал привычку …?»\n"
    "— Ты отвечаешь: *да* или *нет* (можно: «нет потому что …»)\n\n"
    "Команды (можно с / или без):\n"
    "• добавить — добавить привычку\n"
    "• список — список привычек\n"
    "• время — поменять времена привычки\n"
    "• удалить — удалить привычку\n"
    "• проверка — чек-ин прямо сейчас\n"
)

ASK_HABIT_TITLE = (
    "Ок, создаём привычку.\n"
    "Напиши её одним сообщением.\n"
    "Пример: «Вода» или «Чтение 10 минут»."
)

ASK_HABIT_TIMES = (
    "Теперь напиши время(а) напоминаний.\n"
    "Формат строго: *ЧЧ:ММ*.\n"
    "Можно несколько через запятую.\n"
    "Пример: `09:00,12:00,18:30`"
)

BAD_TIME_FORMAT = (
    "Чуть-чуть поправим формат 🙂\n"
    "Нужно строго *ЧЧ:ММ* (00:00–23:59).\n"
    "Пример: `07:00` или `21:30`.\n"
    "Если несколько — через запятую: `09:00,12:00,18:00`"
)

DONE_REPLIES = [
    "Есть ✅ Красавчик. Это маленькая победа, которая копится в большую.",
    "Засчитано ✅ Ты укрепляешь дисциплину. Спокойно, без пафоса — но мощно.",
    "Отлично ✅ Так и строится характер: сделал — и точка.",
    "Супер ✅ Это +1 к твоей надёжности перед самим собой.",
]

MISS_ACK_WITH_REASON = [
    "Принято. Спасибо за честность. Завтра вернём ритм без героизма.",
    "Ок. Не ругаем себя — настраиваем систему. Завтра станет проще.",
    "Понял. Такое бывает. Главное — ты не спрятался. Завтра берём реванш.",
]

ASK_REASON_TEXT = (
    "Ок. Скажи одной фразой: почему *нет*?\n"
    "Примеры: «устал», «забыл», «не было времени»."
)

# =============================
# ПАРСИНГ ВРЕМЕНИ ЧЧ:ММ
# =============================
TIME_RE = re.compile(r"^\d{2}:\d{2}$")

def is_valid_time(t: str) -> bool:
    t = t.strip()
    if not TIME_RE.match(t):
        return False
    hh, mm = t.split(":")
    h = int(hh)
    m = int(mm)
    return 0 <= h <= 23 and 0 <= m <= 59

def parse_times_csv(s: str) -> list[str] | None:
    parts = [p.strip() for p in s.split(",") if p.strip()]
    if not parts:
        return None
    for p in parts:
        if not is_valid_time(p):
            return None
    return sorted(set(parts))

# =============================
# БАЗА ДАННЫХ
# =============================
async def init_db():
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("""
        CREATE TABLE IF NOT EXISTS users(
            user_id INTEGER PRIMARY KEY,
            plan TEXT NOT NULL DEFAULT 'free',
            created_at TEXT NOT NULL,
            last_seen TEXT NOT NULL
        )""")

        await db.execute("""
        CREATE TABLE IF NOT EXISTS habits(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            title TEXT NOT NULL,
            is_active INTEGER NOT NULL DEFAULT 1
        )""")

        await db.execute("""
        CREATE TABLE IF NOT EXISTS habit_times(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            habit_id INTEGER NOT NULL,
            time TEXT NOT NULL
        )""")

        await db.execute("""
        CREATE TABLE IF NOT EXISTS checkins(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            habit_id INTEGER NOT NULL,
            day TEXT NOT NULL,
            time_slot TEXT NOT NULL,   -- HH:MM или 'manual'
            status TEXT NOT NULL DEFAULT 'pending', -- pending/done/miss
            reason TEXT
        )""")

        await db.execute("CREATE INDEX IF NOT EXISTS idx_checkins_uniq ON checkins(user_id, habit_id, day, time_slot)")
        await db.commit()

async def ensure_user(user_id: int):
    now = dt.datetime.now().isoformat(timespec="seconds")
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "INSERT OR IGNORE INTO users(user_id, plan, created_at, last_seen) VALUES(?,?,?,?)",
            (user_id, "free", now, now)
        )
        await db.execute("UPDATE users SET last_seen=? WHERE user_id=?", (now, user_id))
        await db.commit()

async def get_plan(user_id: int) -> str:
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute("SELECT plan FROM users WHERE user_id=?", (user_id,))
        row = await cur.fetchone()
        return row[0] if row else "free"

def habit_limit(plan: str) -> int:
    return PRO_HABIT_LIMIT if plan == "pro" else FREE_HABIT_LIMIT

def times_limit(plan: str) -> int:
    return PRO_MAX_TIMES_PER_HABIT if plan == "pro" else FREE_MAX_TIMES_PER_HABIT

async def count_habits(user_id: int) -> int:
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute("SELECT COUNT(*) FROM habits WHERE user_id=? AND is_active=1", (user_id,))
        (n,) = await cur.fetchone()
        return n

async def list_habits(user_id: int):
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute(
            "SELECT id, title FROM habits WHERE user_id=? AND is_active=1 ORDER BY id",
            (user_id,)
        )
        return await cur.fetchall()

async def get_habit_times(habit_id: int) -> list[str]:
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute("SELECT time FROM habit_times WHERE habit_id=? ORDER BY time", (habit_id,))
        rows = await cur.fetchall()
        return [r[0] for r in rows]

async def create_habit(user_id: int, title: str, times: list[str]) -> int:
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute(
            "INSERT INTO habits(user_id, title, is_active) VALUES(?,?,1)",
            (user_id, title)
        )
        habit_id = cur.lastrowid
        for t in times:
            await db.execute("INSERT INTO habit_times(habit_id, time) VALUES(?,?)", (habit_id, t))
        await db.commit()
        return habit_id

async def delete_habit(user_id: int, habit_id: int) -> bool:
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute("DELETE FROM habits WHERE user_id=? AND id=?", (user_id, habit_id))
        await db.execute("DELETE FROM habit_times WHERE habit_id=?", (habit_id,))
        await db.commit()
        return cur.rowcount > 0

async def replace_habit_times(user_id: int, habit_id: int, times: list[str]) -> bool:
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute("SELECT 1 FROM habits WHERE user_id=? AND id=? AND is_active=1", (user_id, habit_id))
        ok = await cur.fetchone()
        if not ok:
            return False
        await db.execute("DELETE FROM habit_times WHERE habit_id=?", (habit_id,))
        for t in times:
            await db.execute("INSERT INTO habit_times(habit_id, time) VALUES(?,?)", (habit_id, t))
        await db.commit()
        return True

async def ensure_checkin(user_id: int, habit_id: int, day: str, time_slot: str) -> bool:
    async with aiosqlite.connect(DB_PATH) as db:
        try:
            await db.execute(
                "INSERT INTO checkins(user_id, habit_id, day, time_slot, status) VALUES(?,?,?,?, 'pending')",
                (user_id, habit_id, day, time_slot)
            )
            await db.commit()
            return True
        except Exception:
            return False

async def get_checkin_id(user_id: int, habit_id: int, day: str, time_slot: str) -> int | None:
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute("""
            SELECT id FROM checkins
            WHERE user_id=? AND habit_id=? AND day=? AND time_slot=?
            LIMIT 1
        """, (user_id, habit_id, day, time_slot))
        row = await cur.fetchone()
        return row[0] if row else None

async def set_checkin_done(checkin_id: int):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("UPDATE checkins SET status='done', reason=NULL WHERE id=?", (checkin_id,))
        await db.commit()

async def set_checkin_miss(checkin_id: int, reason: str):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("UPDATE checkins SET status='miss', reason=? WHERE id=?", (reason, checkin_id))
        await db.commit()

# =============================
# СТЕЙТЫ (MVP)
# =============================
STATE: dict[int, dict] = {}  # добавление/удаление/время
SESSIONS: dict[int, dict] = {}  # чек-ин сессии: очередь привычек
WAIT_REASON: dict[int, int] = {}  # user_id -> checkin_id (ждём причину "нет")

YES_WORDS = {"да", "ага", "угу", "ok", "ок", "сделал", "выполнил", "готово", "✅", "yes"}
NO_WORDS = {"нет", "не", "неа", "пропустил", "не сделал", "не выполнил", "no"}

def norm(s: str) -> str:
    return (s or "").strip().lower()

def parse_yes_no(text: str):
    """
    Возвращает:
    ("yes", None) или ("no", reason_or_none) или None
    """
    t = norm(text)
    if not t:
        return None

    # "нет потому что ..." / "нет, потому что ..."
    if t.startswith("нет"):
        reason = None
        if "потому" in t:
            after = t.split("потому", 1)[1]
            after = after.replace("что", "", 1).strip(" :,-")
            reason = after if after else None
        elif "," in t:
            after = t.split(",", 1)[1].strip()
            reason = after if after else None
        return ("no", reason)

    # чистое "да"
    if t in YES_WORDS or t.startswith("да "):
        return ("yes", None)

    # варианты "не сделал"
    for w in ("не сделал", "не выполнил", "пропустил"):
        if t.startswith(w):
            # может быть причина после
            reason = None
            if "потому" in t:
                after = t.split("потому", 1)[1]
                after = after.replace("что", "", 1).strip(" :,-")
                reason = after if after else None
            elif "," in t:
                after = t.split(",", 1)[1].strip()
                reason = after if after else None
            return ("no", reason)

    if t in NO_WORDS:
        return ("no", None)

    return None

# =============================
# ЧЕК-ИН СЕССИЯ (Вариант Б)
# =============================
async def start_checkin_session(uid: int, habits: list[tuple[int, str]], time_slot: str):
    """
    Запускаем диалог чек-ина: спрашиваем по очереди.
    """
    if not habits:
        return

    day = dt.date.today().isoformat()

    # создаём pending checkins (чтобы потом закрывать их)
    for hid, _title in habits:
        await ensure_checkin(uid, hid, day, time_slot)

    SESSIONS[uid] = {
        "queue": habits,
        "idx": 0,
        "day": day,
        "time_slot": time_slot
    }
    await ask_next_habit(uid)

async def ask_next_habit(uid: int):
    sess = SESSIONS.get(uid)
    if not sess:
        return

    idx = sess["idx"]
    queue = sess["queue"]
    if idx >= len(queue):
        # сессия закончена
        SESSIONS.pop(uid, None)
        await bot.send_message(uid, "Чек-ин завершён ✅ Продолжаем спокойно и по плану.")
        return

    hid, title = queue[idx]
    text = (
        f"Сделал привычку: *{title}*?\n"
        "Ответь: *да* или *нет*\n"
        "Можно так: `нет потому что устал`"
    )
    await bot.send_message(uid, text, parse_mode="Markdown")

async def handle_session_answer(m: Message) -> bool:
    """
    Возвращает True если сообщение было обработано внутри сессии.
    """
    uid = m.from_user.id

    # Если ждём причину — это приоритет
    if uid in WAIT_REASON:
        checkin_id = WAIT_REASON.pop(uid)
        reason = m.text.strip()
        await set_checkin_miss(checkin_id, reason)
        await m.answer(random.choice(MISS_ACK_WITH_REASON))
        # продолжаем сессию
        if uid in SESSIONS:
            SESSIONS[uid]["idx"] += 1
            await ask_next_habit(uid)
        return True

    sess = SESSIONS.get(uid)
    if not sess:
        return False

    parsed = parse_yes_no(m.text)
    if not parsed:
        await m.answer("Я понял, что это ответ, но мне нужно просто: *да* или *нет* 🙂", parse_mode="Markdown")
        return True

    answer, reason = parsed
    idx = sess["idx"]
    hid, _title = sess["queue"][idx]
    day = sess["day"]
    time_slot = sess["time_slot"]

    checkin_id = await get_checkin_id(uid, hid, day, time_slot)
    if checkin_id is None:
        # на всякий случай
        await ensure_checkin(uid, hid, day, time_slot)
        checkin_id = await get_checkin_id(uid, hid, day, time_slot)

    if answer == "yes":
        if checkin_id is not None:
            await set_checkin_done(checkin_id)
        await m.answer(random.choice(DONE_REPLIES))
        sess["idx"] += 1
        await ask_next_habit(uid)
        return True

    # answer == "no"
    if reason:
        if checkin_id is not None:
            await set_checkin_miss(checkin_id, reason)
        await m.answer(random.choice(MISS_ACK_WITH_REASON))
        sess["idx"] += 1
        await ask_next_habit(uid)
        return True
    else:
        # просим причину
        if checkin_id is not None:
            WAIT_REASON[uid] = checkin_id
        await m.answer(ASK_REASON_TEXT)
        return True

# =============================
# НАПОМИНАНИЯ
# =============================
async def scheduler_tick():
    """
    Каждую минуту смотрим, какие привычки должны спроситься сейчас,
    и запускаем для каждого пользователя одну сессию.
    """
    now = dt.datetime.now()
    hhmm = now.strftime("%H:%M")

    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute("""
            SELECT h.user_id, h.id, h.title
            FROM habits h
            JOIN habit_times ht ON ht.habit_id = h.id
            WHERE h.is_active=1 AND ht.time=?
            ORDER BY h.user_id, h.id
        """, (hhmm,))
        rows = await cur.fetchall()

    grouped = defaultdict(list)
    for uid, hid, title in rows:
        grouped[int(uid)].append((int(hid), str(title)))

    for uid, habits in grouped.items():
        # если уже идёт сессия — не мешаем
        if uid in SESSIONS or uid in WAIT_REASON:
            continue
        await start_checkin_session(uid, habits, hhmm)

async def start_manual_checkin(uid: int):
    habits = await list_habits(uid)
    if not habits:
        await bot.send_message(uid, "Пока нет привычек. Напиши «добавить» — и сделаем первую.")
        return
    if uid in SESSIONS or uid in WAIT_REASON:
        await bot.send_message(uid, "Мы уже в процессе чек-ина 🙂 Ответь *да/нет* на текущий вопрос.", parse_mode="Markdown")
        return
    await start_checkin_session(uid, [(hid, title) for hid, title in habits], "manual")

# =============================
# КОМАНДЫ (и / и без /)
# =============================
def is_text_cmd(m: Message, cmd: str) -> bool:
    return norm(m.text) == cmd

@dp.message(Command("start"))
@dp.message(Command("старт"))
async def cmd_start(m: Message):
    await ensure_user(m.from_user.id)
    await m.answer(START_TEXT, parse_mode="Markdown")

@dp.message(Command("добавить"))
async def cmd_add_slash(m: Message):
    uid = m.from_user.id
    await ensure_user(uid)

    plan = await get_plan(uid)
    limit = habit_limit(plan)
    n = await count_habits(uid)
    if n >= limit:
        await m.answer(f"Лимит привычек: {limit}. В Pro можно больше.")
        return

    STATE[uid] = {"mode": "wait_title"}
    await m.answer(ASK_HABIT_TITLE)

@dp.message(Command("список"))
async def cmd_list_slash(m: Message):
    uid = m.from_user.id
    await ensure_user(uid)

    habits = await list_habits(uid)
    if not habits:
        await m.answer("Пока нет привычек. Напиши «добавить» — и сделаем первую.")
        return

    lines = ["Вот твои привычки:"]
    for hid, title in habits:
        times = await get_habit_times(hid)
        tline = ", ".join(times) if times else "—"
        lines.append(f"\n*{title}*\n⏰ {tline}")
    await m.answer("\n".join(lines), parse_mode="Markdown")

@dp.message(Command("удалить"))
async def cmd_delete_slash(m: Message):
    uid = m.from_user.id
    await ensure_user(uid)
    STATE[uid] = {"mode": "wait_delete_id"}
    await m.answer("Ок. Напиши *точное название привычки*, которую удалить.\nПример: `Вода`", parse_mode="Markdown")

@dp.message(Command("время"))
async def cmd_time_slash(m: Message):
    uid = m.from_user.id
    await ensure_user(uid)
    STATE[uid] = {"mode": "wait_time_change"}
    await m.answer(
        "Поменяем времена.\n"
        "Напиши так:\n"
        "`Вода 09:00,12:00,18:00`\n"
        "(название + времена через запятую, формат ЧЧ:ММ)",
        parse_mode="Markdown"
    )

@dp.message(Command("проверка"))
async def cmd_check_slash(m: Message):
    uid = m.from_user.id
    await ensure_user(uid)
    await start_manual_checkin(uid)

# Админ-статистика (если ADMIN_ID задан)
@dp.message(Command("статистика"))
async def cmd_stats(m: Message):
    if ADMIN_ID and m.from_user.id != ADMIN_ID:
        return
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute("SELECT COUNT(*) FROM users")
        (total,) = await cur.fetchone()

        today = dt.date.today().isoformat()
        cur = await db.execute("SELECT COUNT(*) FROM users WHERE substr(last_seen,1,10)=?", (today,))
        (active_today,) = await cur.fetchone()

        cur = await db.execute("SELECT COUNT(*) FROM users WHERE substr(created_at,1,10)=?", (today,))
        (new_today,) = await cur.fetchone()

    await m.answer(
        "📊 Статистика\n"
        f"👤 Всего пользователей: {total}\n"
        f"🟢 Активны сегодня: {active_today}\n"
        f"🆕 Новых сегодня: {new_today}"
    )

# =============================
# ТЕКСТОВОЙ РОУТЕР (включая команды без / и ответы да/нет)
# =============================
@dp.message(F.text)
async def text_router(m: Message):
    uid = m.from_user.id
    await ensure_user(uid)

    # 1) если идёт сессия — сначала пробуем обработать "да/нет"
    if await handle_session_answer(m):
        return

    # 2) текстовые команды без /
    t = norm(m.text)

    if t == "добавить":
        return await cmd_add_slash(m)
    if t == "список":
        return await cmd_list_slash(m)
    if t == "удалить":
        return await cmd_delete_slash(m)
    if t == "время":
        return await cmd_time_slash(m)
    if t == "проверка":
        return await cmd_check_slash(m)

    # 3) флоу добавления привычки
    st = STATE.get(uid, {})
    mode = st.get("mode")

    if mode == "wait_title":
        title = m.text.strip()
        if len(title) < 2:
            await m.answer("Слишком коротко. Напиши привычку понятнее 🙂")
            return

        # проверим лимит ещё раз
        plan = await get_plan(uid)
        limit = habit_limit(plan)
        n = await count_habits(uid)
        if n >= limit:
            STATE.pop(uid, None)
            await m.answer(f"Лимит привычек: {limit}. В Pro можно больше.")
            return

        STATE[uid] = {"mode": "wait_times", "title": title}
        await m.answer(ASK_HABIT_TIMES, parse_mode="Markdown")
        return

    if mode == "wait_times":
        title = st.get("title", "").strip()
        times = parse_times_csv(m.text)
        if not times:
            await m.answer(BAD_TIME_FORMAT, parse_mode="Markdown")
            return

        plan = await get_plan(uid)
        tlimit = times_limit(plan)
        if len(times) > tlimit:
            await m.answer(f"Слишком много времён: {len(times)}. Лимит твоего плана: {tlimit}.")
            return

        _hid = await create_habit(uid, title, times)
        STATE.pop(uid, None)
        await m.answer(
            f"Готово ✅ Привычка создана: *{title}*\n"
            f"⏰ {', '.join(times)}\n\n"
            "Когда придёт напоминание — я спрошу, а ты ответишь: *да* или *нет*.",
            parse_mode="Markdown"
        )
        return

    # 4) удаление по названию
    if mode == "wait_delete_id":
        title = m.text.strip()
        habits = await list_habits(uid)
        match = None
        for hid, htitle in habits:
            if htitle.strip().lower() == title.strip().lower():
                match = hid
                break
        STATE.pop(uid, None)
        if not match:
            await m.answer("Не нашёл привычку с таким названием. Проверь «список».")
            return
        ok = await delete_habit(uid, match)
        await m.answer("Удалил ✅" if ok else "Не получилось удалить. Проверь «список».")
        return

    # 5) смена времени по названию: "Вода 09:00,12:00"
    if mode == "wait_time_change":
        parts = m.text.strip().split(maxsplit=1)
        if len(parts) != 2:
            await m.answer("Формат: `Вода 09:00,12:00,18:00`", parse_mode="Markdown")
            return
        title = parts[0].strip()
        times = parse_times_csv(parts[1])
        if not times:
            await m.answer(BAD_TIME_FORMAT, parse_mode="Markdown")
            return

        habits = await list_habits(uid)
        habit_id = None
        for hid, htitle in habits:
            if htitle.strip().lower() == title.lower():
                habit_id = hid
                break

        if habit_id is None:
            STATE.pop(uid, None)
            await m.answer("Не нашёл привычку с таким названием. Проверь «список».")
            return

        plan = await get_plan(uid)
        tlimit = times_limit(plan)
        if len(times) > tlimit:
            await m.answer(f"Слишком много времён: {len(times)}. Лимит твоего плана: {tlimit}.")
            return

        ok = await replace_habit_times(uid, habit_id, times)
        STATE.pop(uid, None)
        await m.answer("Обновил ⏰" if ok else "Не получилось обновить. Проверь «список».")
        return

    # если это просто обычный текст — мягко молчим (чтобы бот не бесил)
    return

# =============================
# HEALTH SERVER (для Render)
# =============================
async def handle_health(_request):
    return web.Response(text="ok")

async def start_web_server():
    app = web.Application()
    app.router.add_get("/", handle_health)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", PORT)
    await site.start()

# =============================
# MAIN
# =============================
async def main():
    await init_db()

    scheduler.add_job(scheduler_tick, "cron", second=0)
    scheduler.start()

    await start_web_server()

    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())