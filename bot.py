import asyncio
import os
import re
import random
import datetime as dt

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

FREE_MAX_TIMES_PER_HABIT = 10   # можно снизить до 5, если хочешь
PRO_MAX_TIMES_PER_HABIT = 30

BOT_TOKEN = os.getenv("BOT_TOKEN")  # В Render добавишь в Environment
ADMIN_ID = int(os.getenv("ADMIN_ID", "0"))  # твой Telegram user_id (для /статистика и тестового включения Pro)

PORT = int(os.getenv("PORT", "10000"))  # Render любит, чтобы сервис слушал порт

# =============================
# ТЕКСТЫ (тон "хороший друг")
# =============================
START_TEXT = (
    "Привет! Я рядом, чтобы помочь тебе держать слово самому себе.\n"
    "Я напоминаю — ты отвечаешь *текстом*, как SMS.\n\n"
    "Команды:\n"
    "• /добавить — добавить привычку\n"
    "• /список — список привычек\n"
    "• /время — поменять времена привычки\n"
    "• /удалить — удалить привычку\n"
    "• /проверка — чек-ин сейчас\n\n"
    "Ответ на чек-ин:\n"
    "• \"12 выполнил\"\n"
    "• \"12 не выполнил потому что устал\""
)

ASK_HABIT_TITLE = (
    "Ок, создаём привычку.\n"
    "Напиши её одним сообщением.\n"
    "Пример: «Вода — 2 стакана» или «Пресс 10 минут»."
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

NEED_ID_FORMAT = (
    "Я понял мысль, но мне нужен номер привычки 🙂\n"
    "Пример: `12 выполнил` или `12 не выполнил потому что устал`."
)

DONE_REPLIES = [
    "Зафиксировал ✅ Красавчик. Ты укрепил привычку — это реально сила.",
    "Есть! Это +1 к дисциплине. Спокойно и по факту — так и строится результат.",
    "Сделано ✅ Ты сейчас управляешь днём, а не день тобой.",
    "Отлично. Маленькое «выполнил» каждый раз делает тебя надёжнее для самого себя.",
    "Засчитано ✅ Не настроение решает — привычка решает. И ты это показал.",
]

MISS_ACK_WITH_REASON = [
    "Принято. Спасибо за честность. Завтра вернём ритм без героизма.",
    "Ок, записал. Не ругаем себя — настраиваем систему. Завтра станет проще.",
    "Понял. Такое бывает. Главное — ты не спрятался. Завтра возьмём реванш.",
]

ASK_REASON_TEXT = (
    "Ок. Я закрываю отметку только после причины.\n"
    "Напиши одним сообщением: почему не сделал?\n"
    "Пример: «устал», «забыл», «не было времени»."
)

CHECKIN_TEMPLATE = (
    "Чек-ин по привычке *#{hid}: {title}*.\n"
    "Ответь текстом:\n"
    "• `{hid} выполнил`\n"
    "• `{hid} не выполнил потому что ...`"
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
    # Уникализируем и сортируем по времени
    uniq = sorted(set(parts))
    return uniq

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

        # checkins: запись факта, что мы спросили (pending) и потом закрыли done/miss
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
        # проверяем, что привычка принадлежит пользователю
        cur = await db.execute("SELECT 1 FROM habits WHERE user_id=? AND id=? AND is_active=1", (user_id, habit_id))
        ok = await cur.fetchone()
        if not ok:
            return False
        await db.execute("DELETE FROM habit_times WHERE habit_id=?", (habit_id,))
        for t in times:
            await db.execute("INSERT INTO habit_times(habit_id, time) VALUES(?,?)", (habit_id, t))
        await db.commit()
        return True

async def habit_title_for_user(user_id: int, habit_id: int) -> str | None:
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute("SELECT title FROM habits WHERE user_id=? AND id=? AND is_active=1", (user_id, habit_id))
        row = await cur.fetchone()
        return row[0] if row else None

async def ensure_checkin(user_id: int, habit_id: int, day: str, time_slot: str) -> bool:
    """
    Создаём checkin pending, если его ещё не было.
    Возвращает True если создали новый, False если уже существовал.
    """
    async with aiosqlite.connect(DB_PATH) as db:
        try:
            await db.execute(
                "INSERT INTO checkins(user_id, habit_id, day, time_slot, status) VALUES(?,?,?,?, 'pending')",
                (user_id, habit_id, day, time_slot)
            )
            await db.commit()
            return True
        except Exception:
            # конфликт уникального индекса или другое — считаем, что уже есть
            return False

async def get_latest_pending_checkin(user_id: int, habit_id: int, day: str) -> int | None:
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute("""
            SELECT id FROM checkins
            WHERE user_id=? AND habit_id=? AND day=? AND status='pending'
            ORDER BY time_slot DESC
            LIMIT 1
        """, (user_id, habit_id, day))
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
# СТЕЙТ ДИАЛОГА (MVP)
# =============================
STATE: dict[int, dict] = {}          # user_id -> {"mode": "...", ...}
WAIT_REASON: dict[int, int] = {}     # user_id -> checkin_id

# =============================
# БОТ
# =============================
if not BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN is not set. Add BOT_TOKEN in Render environment variables.")

bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()
scheduler = AsyncIOScheduler()

# =============================
# КОМАНДЫ (русские)
# =============================
@dp.message(Command("старт"))
@dp.message(Command("start"))
async def cmd_start(m: Message):
    await ensure_user(m.from_user.id)
    await m.answer(START_TEXT, parse_mode="Markdown")

@dp.message(Command("добавить"))
async def cmd_add(m: Message):
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
async def cmd_list(m: Message):
    uid = m.from_user.id
    await ensure_user(uid)

    habits = await list_habits(uid)
    if not habits:
        await m.answer("Пока нет привычек. Напиши /добавить — и сделаем первую.")
        return

    lines = ["Вот твои привычки:"]
    for hid, title in habits:
        times = await get_habit_times(hid)
        tline = ", ".join(times) if times else "—"
        lines.append(f"\n*#{hid}* {title}\n⏰ {tline}")
    await m.answer("\n".join(lines), parse_mode="Markdown")

@dp.message(Command("удалить"))
async def cmd_delete(m: Message):
    uid = m.from_user.id
    await ensure_user(uid)
    STATE[uid] = {"mode": "wait_delete_id"}
    await m.answer("Ок. Напиши номер привычки, которую удалить. Пример: `12`", parse_mode="Markdown")

@dp.message(Command("время"))
async def cmd_time(m: Message):
    uid = m.from_user.id
    await ensure_user(uid)
    STATE[uid] = {"mode": "wait_time_change"}
    await m.answer(
        "Поменяем времена.\n"
        "Напиши так:\n"
        "`12 09:00,12:00,18:00`\n"
        "(номер + времена через запятую, формат ЧЧ:ММ)",
        parse_mode="Markdown"
    )

@dp.message(Command("проверка"))
async def cmd_check(m: Message):
    uid = m.from_user.id
    await ensure_user(uid)
    await send_manual_checkins(uid)
    await m.answer("Ок. Я отправил чек-ин по всем привычкам.")

# Админ-команды (только для тебя)
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

        # новые за сегодня = created_at сегодня
        cur = await db.execute("SELECT COUNT(*) FROM users WHERE substr(created_at,1,10)=?", (today,))
        (new_today,) = await cur.fetchone()

    await m.answer(
        "📊 Статистика\n"
        f"👤 Всего пользователей: {total}\n"
        f"🟢 Активны сегодня: {active_today}\n"
        f"🆕 Новых сегодня: {new_today}"
    )

@dp.message(Command("setpro"))
async def cmd_setpro(m: Message):
    if ADMIN_ID and m.from_user.id != ADMIN_ID:
        return
    uid = m.from_user.id
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("UPDATE users SET plan='pro' WHERE user_id=?", (uid,))
        await db.commit()
    await m.answer("Готово. План: PRO (лимиты увеличены).")

@dp.message(Command("setfree"))
async def cmd_setfree(m: Message):
    if ADMIN_ID and m.from_user.id != ADMIN_ID:
        return
    uid = m.from_user.id
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("UPDATE users SET plan='free' WHERE user_id=?", (uid,))
        await db.commit()
    await m.answer("Ок. План: FREE.")

# =============================
# ЛОГИКА ЧЕК-ИНОВ
# =============================
async def send_checkin(uid: int, hid: int, title: str, time_slot: str):
    day = dt.date.today().isoformat()
    created = await ensure_checkin(uid, hid, day, time_slot)
    if not created:
        return  # уже отправляли этот слот
    text = CHECKIN_TEMPLATE.format(hid=hid, title=title)
    await bot.send_message(uid, text, parse_mode="Markdown")

async def send_manual_checkins(uid: int):
    habits = await list_habits(uid)
    for hid, title in habits:
        await send_checkin(uid, hid, title, "manual")

async def scheduler_tick():
    """Проверяем каждую минуту: есть ли привычки с таким временем."""
    now = dt.datetime.now()
    hhmm = now.strftime("%H:%M")
    day = dt.date.today().isoformat()

    async with aiosqlite.connect(DB_PATH) as db:
        # Получаем все совпадения времени
        cur = await db.execute("""
            SELECT h.user_id, h.id, h.title
            FROM habits h
            JOIN habit_times ht ON ht.habit_id = h.id
            WHERE h.is_active=1 AND ht.time=?
        """, (hhmm,))
        rows = await cur.fetchall()

    # Шлём чек-ин
    for uid, hid, title in rows:
        # ensure_user на всякий (если юзер уже есть — просто обновит last_seen в других местах)
        await send_checkin(uid, hid, title, hhmm)

# =============================
# ПАРСИНГ ОТВЕТОВ "12 выполнил / не выполнил ..."
# =============================
def parse_report(text: str):
    """
    Возвращает:
    ("done", habit_id) или ("miss", habit_id, reason_or_none) или None
    """
    t = text.strip().lower()
    m = re.match(r"^(\d+)\s+(.+)$", t)
    if not m:
        return None

    habit_id = int(m.group(1))
    rest = m.group(2).strip()

    done_words = ["выполнил", "сделал", "готово", "выполнено"]
    miss_words = ["не выполнил", "не сделал", "не выполнено", "пропустил"]

    if any(rest.startswith(w) for w in done_words):
        return ("done", habit_id)

    if any(rest.startswith(w) for w in miss_words):
        reason = None
        # "потому что ..."
        if "потому" in rest:
            after = rest.split("потому", 1)[1]
            after = after.replace("что", "", 1).strip(" :,-")
            reason = after if after else None
        # "не сделал, устал"
        elif "," in rest:
            after = rest.split(",", 1)[1].strip()
            reason = after if after else None
        return ("miss", habit_id, reason)

    return None

# =============================
# ТЕКСТОВОЙ РОУТЕР (основа всего)
# =============================
@dp.message(F.text)
async def text_router(m: Message):
    uid = m.from_user.id
    await ensure_user(uid)

    # 1) Если ждём причину после "не выполнил"
    if uid in WAIT_REASON:
        checkin_id = WAIT_REASON.pop(uid)
        reason = m.text.strip()
        await set_checkin_miss(checkin_id, reason)
        await m.answer(random.choice(MISS_ACK_WITH_REASON))
        return

    # 2) Флоу добавления привычки
    st = STATE.get(uid, {})
    mode = st.get("mode")

    if mode == "wait_title":
        title = m.text.strip()
        if len(title) < 2:
            await m.answer("Слишком коротко. Напиши привычку понятнее 🙂")
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
            await m.answer(f"Слишком много времён за раз: {len(times)}. Лимит для твоего плана: {tlimit}.")
            return

        hid = await create_habit(uid, title, times)
        STATE.pop(uid, None)
        await m.answer(
            f"Готово ✅ Привычка создана: *#{hid}* {title}\n"
            f"⏰ {', '.join(times)}\n\n"
            "Дальше — просто отвечай по номеру, когда я спрошу.",
            parse_mode="Markdown"
        )
        return

    # 3) Удаление привычки
    if mode == "wait_delete_id":
        try:
            hid = int(m.text.strip())
        except:
            await m.answer("Нужен номер. Пример: `12`", parse_mode="Markdown")
            return
        ok = await delete_habit(uid, hid)
        STATE.pop(uid, None)
        await m.answer("Удалил ✅" if ok else "Не нашёл такую привычку у тебя. Проверь /список")
        return

    # 4) Замена времён: "12 09:00,12:00"
    if mode == "wait_time_change":
        parts = m.text.strip().split(maxsplit=1)
        if len(parts) != 2:
            await m.answer("Формат: `12 09:00,12:00,18:00`", parse_mode="Markdown")
            return
        try:
            hid = int(parts[0])
        except:
            await m.answer("Первым должен быть номер привычки. Пример: `12 09:00,18:00`", parse_mode="Markdown")
            return
        times = parse_times_csv(parts[1])
        if not times:
            await m.answer(BAD_TIME_FORMAT, parse_mode="Markdown")
            return

        plan = await get_plan(uid)
        tlimit = times_limit(plan)
        if len(times) > tlimit:
            await m.answer(f"Слишком много времён: {len(times)}. Лимит твоего плана: {tlimit}.")
            return

        ok = await replace_habit_times(uid, hid, times)
        STATE.pop(uid, None)
        await m.answer("Обновил ⏰" if ok else "Не нашёл привычку. Проверь /список")
        return

    # 5) Отметка выполнения (главное)
    parsed = parse_report(m.text)
    if not parsed:
        # не мешаем обычной переписке — просто подскажем формат, если это похоже на попытку отчёта
        if re.match(r"^\d+\s*$", m.text.strip()):
            await m.answer(NEED_ID_FORMAT, parse_mode="Markdown")
        return

    day = dt.date.today().isoformat()

    if parsed[0] == "done":
        habit_id = parsed[1]
        title = await habit_title_for_user(uid, habit_id)
        if not title:
            await m.answer("Не вижу у тебя привычку с таким номером. Проверь /список.")
            return

        checkin_id = await get_latest_pending_checkin(uid, habit_id, day)
        # если нет pending (ответ “вне слота”), создадим manual и закроем
        if checkin_id is None:
            await ensure_checkin(uid, habit_id, day, "manual")
            checkin_id = await get_latest_pending_checkin(uid, habit_id, day)

        if checkin_id is not None:
            await set_checkin_done(checkin_id)

        await m.answer(random.choice(DONE_REPLIES))
        return

    if parsed[0] == "miss":
        habit_id, reason = parsed[1], parsed[2]
        title = await habit_title_for_user(uid, habit_id)
        if not title:
            await m.answer("Не вижу у тебя привычку с таким номером. Проверь /список.")
            return

        checkin_id = await get_latest_pending_checkin(uid, habit_id, day)
        if checkin_id is None:
            await ensure_checkin(uid, habit_id, day, "manual")
            checkin_id = await get_latest_pending_checkin(uid, habit_id, day)

        if checkin_id is None:
            await m.answer("Странно, не могу найти слот для отметки. Попробуй /проверка и ответь снова.")
            return

        if reason:
            await set_checkin_miss(checkin_id, reason)
            await m.answer(random.choice(MISS_ACK_WITH_REASON))
        else:
            WAIT_REASON[uid] = checkin_id
            await m.answer(ASK_REASON_TEXT)
        return

# =============================
# HEALTH SERVER (для Render Web Service)
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

    # Планировщик: каждую минуту проверяем времена
    scheduler.add_job(scheduler_tick, "cron", second=0)
    scheduler.start()

    # Сервер для Render
    await start_web_server()

    # Поллинг Telegram
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
