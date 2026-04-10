import csv
import logging
import os
import sqlite3
from datetime import datetime, timedelta, date, time
from pathlib import Path
from typing import Iterable

from openai import AsyncOpenAI
from telegram import (
    Update,
    ReplyKeyboardMarkup,
    ReplyKeyboardRemove,
    KeyboardButton,
)
from telegram.ext import (
    ApplicationBuilder,
    CommandHandler,
    MessageHandler,
    ConversationHandler,
    ContextTypes,
    filters,
)


logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)

(
    CHOOSING_ACTION,
    CHOOSING_DAY,
    CHOOSING_START_TIME,
    CHOOSING_DURATION,
    CONFIRM_BOOKING,
) = range(5)

WORKING_DAY_START_HOUR = 0  # круглосуточно: с 00:00
WORKING_DAY_END_HOUR = 24   # до 24:00 (используется только для расчётов)
MAX_DURATION_HOURS = 4

BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()

# ID владельца бота (Telegram user_id — только через окружение)
_admin_raw = os.getenv("RENT_BOT_ADMIN_ID", "").strip()
ADMIN_USER_ID = int(_admin_raw) if _admin_raw else 0

# DeepSeek — только из окружения (.env), без значений по умолчанию в коде
DEEPSEEK_API_KEY = os.getenv("DEEPSEEK_API_KEY", "").strip()
DEEPSEEK_BASE_URL = os.getenv("DEEPSEEK_BASE_URL", "https://api.deepseek.com").strip()
DEEPSEEK_MODEL = os.getenv("DEEPSEEK_MODEL", "deepseek-chat").strip()

BASE_DIR = Path(__file__).resolve().parent
DB_PATH = BASE_DIR / "rent_bot.db"
CSV_EXPORT_PATH = BASE_DIR / "bookings_export.csv"


def _get_db_connection() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def _init_db() -> None:
    conn = _get_db_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS users (
                user_id INTEGER PRIMARY KEY,
                hours   INTEGER NOT NULL DEFAULT 0
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS bookings (
                id       INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id  INTEGER NOT NULL,
                start_ts TEXT NOT NULL,
                end_ts   TEXT NOT NULL
            )
            """
        )
        conn.commit()
    finally:
        conn.close()


def _format_date(d: date) -> str:
    months = {
        1: "января",
        2: "февраля",
        3: "марта",
        4: "апреля",
        5: "мая",
        6: "июня",
        7: "июля",
        8: "августа",
        9: "сентября",
        10: "октября",
        11: "ноября",
        12: "декабря",
    }
    return f"{d.day} {months[d.month]} {d.year} г."


def _date_key(d: date) -> str:
    return d.strftime("%Y-%m-%d")


def _is_free(start_dt: datetime, end_dt: datetime) -> bool:
    conn = _get_db_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT 1
            FROM bookings
            WHERE NOT (end_ts <= ? OR start_ts >= ?)
            """,
            (start_dt.isoformat(), end_dt.isoformat()),
        )
        row = cur.fetchone()
        return row is None
    finally:
        conn.close()


def _add_booking(user_id: int, start_dt: datetime, end_dt: datetime) -> None:
    conn = _get_db_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO bookings (user_id, start_ts, end_ts) VALUES (?, ?, ?)",
            (user_id, start_dt.isoformat(), end_dt.isoformat()),
        )
        conn.commit()
    finally:
        conn.close()


def _format_user_bookings(user_id: int) -> str:
    conn = _get_db_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT start_ts, end_ts
            FROM bookings
            WHERE user_id = ?
            ORDER BY start_ts
            """,
            (user_id,),
        )
        rows = cur.fetchall()
    finally:
        conn.close()

    if not rows:
        return "У вас пока нет бронирований."

    lines: list[str] = []
    for r in rows:
        start_dt = datetime.fromisoformat(r["start_ts"])
        end_dt = datetime.fromisoformat(r["end_ts"])
        lines.append(
            f"{_format_date(start_dt.date())}: "
            f"{start_dt.strftime('%H:%M')}–{end_dt.strftime('%H:%M')}"
        )

    return "Ваши бронирования:\n\n" + "\n".join(f"• {line}" for line in lines)


def _get_user_hours(user_id: int) -> int:
    conn = _get_db_connection()
    try:
        cur = conn.cursor()
        cur.execute("SELECT hours FROM users WHERE user_id = ?", (user_id,))
        row = cur.fetchone()
        if row is None:
            return 0
        return int(row[0])
    finally:
        conn.close()


def _set_user_hours(user_id: int, hours: int) -> None:
    conn = _get_db_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO users (user_id, hours)
            VALUES (?, ?)
            ON CONFLICT(user_id) DO UPDATE SET hours = excluded.hours
            """,
            (user_id, hours),
        )
        conn.commit()
    finally:
        conn.close()


def _add_10_hours(user_id: int) -> int:
    current = _get_user_hours(user_id)
    new_value = current + 10
    _set_user_hours(user_id, new_value)
    return new_value


def _parse_time_15min(text: str) -> time | None:
    text = text.strip().replace(".", ":").replace(",", ":")
    if ":" not in text:
        return None
    hh, mm = text.split(":", 1)
    try:
        h = int(hh)
        m = int(mm)
    except ValueError:
        return None
    if not (0 <= h <= 23 and 0 <= m <= 59):
        return None
    if m % 15 != 0:
        return None
    return time(hour=h, minute=m)


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    user = update.effective_user
    logger.info("User %s started the bot", user.id if user else "?")

    keyboard = [
        [KeyboardButton("📅 Забронировать кабинет")],
        [KeyboardButton("📖 Мои бронирования"), KeyboardButton("🎫 Мой абонемент")],
        [KeyboardButton("👨‍💼 Администратор")],
    ]
    reply_markup = ReplyKeyboardMarkup(keyboard, resize_keyboard=True)
    await update.message.reply_text(
        "Здравствуйте! Это бот почасовой аренды кабинета.\n\n"
        "Выберите действие в меню ниже:",
        reply_markup=reply_markup,
    )
    return CHOOSING_ACTION


async def choose_action(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    text = (update.message.text or "").strip()
    user_id = update.effective_user.id

    if text == "📅 Забронировать кабинет":
        if _get_user_hours(user_id) <= 0:
            await update.message.reply_text(
                "У вас нет часов на абонементе.\n"
                "Сначала пополните абонемент (10 часов). Для теста можно отправить команду /buy10."
            )
            return CHOOSING_ACTION

        today = date.today()
        # ближайшие 30 дней, включая сегодня
        days = [today + timedelta(days=i) for i in range(30)]
        context.user_data["available_days"] = days

        keyboard = [[_format_date(d)] for d in days]
        keyboard.append([KeyboardButton("⬅ Назад")])

        await update.message.reply_text(
            "Выберите день бронирования:",
            reply_markup=ReplyKeyboardMarkup(keyboard, resize_keyboard=True),
        )
        return CHOOSING_DAY

    if text == "📖 Мои бронирования":
        msg = _format_user_bookings(user_id)
        await update.message.reply_text(msg)
        return CHOOSING_ACTION

    if text == "👨‍💼 Администратор":
        await update.message.reply_text(
            "Вы можете задать вопрос администратору.\n\n"
            "Напишите команду в одном сообщении:\n"
            "/administrator ваш вопрос\n\n"
            "Пример: /administrator можно ли завтра забронировать с 16:00 на 2 часа?"
        )
        return CHOOSING_ACTION

    if text == "🎫 Мой абонемент":
        hours = _get_user_hours(user_id)
        await update.message.reply_text(
            f"На вашем абонементе сейчас: {hours} ч.\n\n"
            "Для теста можно пополнить ещё на 10 часов командой /buy10."
        )
        return CHOOSING_ACTION

    if text == "⬅ Назад":
        return await start(update, context)

    # Любой другой текст в главном меню — считаем вопросом администратору,
    # если настроен DEEPSEEK_API_KEY.
    if DEEPSEEK_API_KEY:
        await update.message.reply_text("Администратор думает над ответом...")
        try:
            client = AsyncOpenAI(api_key=DEEPSEEK_API_KEY, base_url=DEEPSEEK_BASE_URL)
            response = await client.chat.completions.create(
                model=DEEPSEEK_MODEL,
                messages=[
                    {
                        "role": "system",
                        "content": (
                            "Ты — администратор кабинета почасовой аренды. "
                            "Пользователь пишет свободным текстом. "
                            "Отвечай кратко, дружелюбно и по делу. "
                            "Если тебя просят что-то забронировать, уточняй дату, время начала "
                            "и длительность брони, но сам бронь не создаёшь — только формулируешь, "
                            "что именно нужно забронировать, чтобы бот мог это сделать дальше."
                        ),
                    },
                    {
                        "role": "user",
                        "content": (
                            f"Пользователь с id {user_id} задаёт вопрос администратору:\n\n{text}"
                        ),
                    },
                ],
                max_tokens=600,
            )
            answer = (response.choices[0].message.content or "").strip()
            if not answer:
                await update.message.reply_text(
                    "Не получилось получить ответ от администратора. Попробуйте переформулировать вопрос."
                )
            else:
                await update.message.reply_text(answer)
        except Exception as e:
            logger.warning("DeepSeek administrator (from menu) error: %s", e)
            await update.message.reply_text(
                "Не удалось связаться с администратором. Попробуйте ещё раз позже."
            )
        return CHOOSING_ACTION

    await update.message.reply_text("Пожалуйста, воспользуйтесь кнопками меню.")
    return CHOOSING_ACTION


async def choose_day(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    text = (update.message.text or "").strip()
    if text == "⬅ Назад":
        return await start(update, context)

    days: list[date] = context.user_data.get("available_days", [])
    chosen_day: date | None = None
    for d in days:
        if _format_date(d) == text:
            chosen_day = d
            break

    if not chosen_day:
        await update.message.reply_text("Не понял день. Выберите из списка кнопок.")
        return CHOOSING_DAY

    context.user_data["chosen_day"] = chosen_day

    # Клавиатура с выбором времени (шаг 15 минут, круглосуточно)
    time_buttons: list[list[KeyboardButton]] = []
    row: list[KeyboardButton] = []
    current_dt = datetime.combine(chosen_day, time(hour=WORKING_DAY_START_HOUR, minute=0))
    end_dt_limit = datetime.combine(chosen_day + timedelta(days=1), time(0, 0))
    while current_dt < end_dt_limit:
        t = current_dt.time()
        row.append(KeyboardButton(t.strftime("%H:%M")))
        if len(row) == 4:
            time_buttons.append(row)
            row = []
        current_dt += timedelta(minutes=15)
    if row:
        time_buttons.append(row)
    time_buttons.append([KeyboardButton("⬅ Назад")])

    await update.message.reply_text(
        f"Выбрана дата: {_format_date(chosen_day)}.\n\n"
        "Теперь выберите время начала:",
        reply_markup=ReplyKeyboardMarkup(time_buttons, resize_keyboard=True),
    )

    return CHOOSING_START_TIME


async def choose_start_time(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    text = (update.message.text or "").strip()
    if text == "⬅ Назад":
        return await start(update, context)

    t = _parse_time_15min(text)
    if t is None:
        await update.message.reply_text(
            "Пожалуйста, введите время в формате ЧЧ:ММ с шагом 15 минут.\n"
            "Например: 18:00, 18:15, 18:30, 18:45."
        )
        return CHOOSING_START_TIME

    context.user_data["start_time"] = t

    max_possible = MAX_DURATION_HOURS
    durations = list(range(1, max_possible + 1))
    keyboard = [[KeyboardButton(f"{d} ч.")] for d in durations]
    keyboard.append([KeyboardButton("⬅ Назад")])

    await update.message.reply_text(
        "Выберите продолжительность бронирования (часы будут списаны с абонемента):",
        reply_markup=ReplyKeyboardMarkup(keyboard, resize_keyboard=True),
    )

    return CHOOSING_DURATION


async def choose_duration(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    text = (update.message.text or "").strip()
    user_id = update.effective_user.id

    if text == "⬅ Назад":
        return await start(update, context)

    try:
        duration_hours = int(text.split()[0])
    except (ValueError, IndexError):
        await update.message.reply_text("Пожалуйста, выберите продолжительность с кнопки.")
        return CHOOSING_DURATION

    current_hours = _get_user_hours(user_id)
    if duration_hours > current_hours:
        await update.message.reply_text(
            f"У вас только {current_hours} ч. на абонементе, а вы выбрали {duration_hours} ч.\n"
            "Выберите меньшую продолжительность или пополните абонемент."
        )
        return CHOOSING_DURATION

    day: date = context.user_data["chosen_day"]
    start_time: time = context.user_data["start_time"]

    start_dt = datetime.combine(day, start_time)
    end_dt = start_dt + timedelta(hours=duration_hours)
    dk = _date_key(day)  # пока используем только для совместимости логики дальше

    # не даём бронь, которая переходит на следующий день
    if end_dt.date() != day:
        await update.message.reply_text(
            "С этой продолжительностью бронь переходит на следующий день.\n"
            "Пока можно бронировать в пределах одних суток. Выберите меньшую длительность или другое время начала."
        )
        return CHOOSING_DURATION

    if not _is_free(start_dt, end_dt):
        await update.message.reply_text(
            "К сожалению, этот интервал уже занят. Попробуйте другое время."
        )
        return CHOOSING_START_TIME

    context.user_data["duration_hours"] = duration_hours
    context.user_data["start_dt"] = start_dt
    context.user_data["end_dt"] = end_dt

    text_confirm = (
        "Проверьте данные бронирования:\n\n"
        f"📅 Дата: {_format_date(day)}\n"
        f"⏰ Время: {start_dt.strftime('%H:%M')}–{end_dt.strftime('%H:%M')}\n"
        f"⌛ Будет списано с абонемента: {duration_hours} ч.\n\n"
        "Подтвердить?"
    )

    keyboard = [
        [KeyboardButton("✅ Подтвердить")],
        [KeyboardButton("❌ Отменить")],
        [KeyboardButton("⬅ Назад")],
    ]

    await update.message.reply_text(
        text_confirm,
        reply_markup=ReplyKeyboardMarkup(keyboard, resize_keyboard=True),
    )

    return CONFIRM_BOOKING


async def confirm_booking(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    text = (update.message.text or "").strip()
    user = update.effective_user
    user_id = user.id

    if text == "⬅ Назад":
        return await start(update, context)

    if text == "❌ Отменить":
        await update.message.reply_text(
            "Бронирование отменено.", reply_markup=ReplyKeyboardRemove()
        )
        return await start(update, context)

    if text != "✅ Подтвердить":
        await update.message.reply_text("Пожалуйста, выберите один из вариантов.")
        return CONFIRM_BOOKING

    start_dt: datetime = context.user_data["start_dt"]
    end_dt: datetime = context.user_data["end_dt"]
    duration_hours: int = context.user_data["duration_hours"]
    dk = _date_key(start_dt.date())

    if not _is_free(dk, start_dt, end_dt):
        await update.message.reply_text(
            "Пока вы подтверждали, этот слот уже заняли. Выберите другое время.",
            reply_markup=ReplyKeyboardRemove(),
        )
        return await start(update, context)

    current_hours = _get_user_hours(user_id)
    if duration_hours > current_hours:
        await update.message.reply_text(
            "Похоже, часы на абонементе уже израсходованы. Пополните абонемент."
        )
        return await start(update, context)

    remaining = current_hours - duration_hours
    _set_user_hours(user_id, remaining)

    _add_booking(user_id, start_dt, end_dt)

    await update.message.reply_text(
        "Готово! Ваше бронирование подтверждено ✅\n\n"
        f"📅 Дата: {_format_date(start_dt.date())}\n"
        f"⏰ Время: {start_dt.strftime('%H:%M')}–{end_dt.strftime('%H:%M')}\n"
        f"🎫 Осталось на абонементе: {remaining} ч.",
        reply_markup=ReplyKeyboardRemove(),
    )

    return await start(update, context)


async def cancel(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    await update.message.reply_text(
        "Диалог отменён. Чтобы начать заново, отправьте /start.",
        reply_markup=ReplyKeyboardRemove(),
    )
    return ConversationHandler.END


async def buy10(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user_id = update.effective_user.id
    total = _add_10_hours(user_id)
    await update.message.reply_text(
        f"Абонемент пополнен на 10 часов.\n"
        f"Теперь на вашем абонементе: {total} ч."
    )


def _require_admin(user_id: int) -> bool:
    return ADMIN_USER_ID and user_id == ADMIN_USER_ID


def _format_admin_bookings_for_day(target_date: date) -> str:
    start_dt = datetime.combine(target_date, time(0, 0))
    end_dt = start_dt + timedelta(days=1)

    conn = _get_db_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT b.user_id, b.start_ts, b.end_ts, u.hours
            FROM bookings b
            LEFT JOIN users u ON u.user_id = b.user_id
            WHERE NOT (b.end_ts <= ? OR b.start_ts >= ?)
            ORDER BY b.start_ts
            """,
            (start_dt.isoformat(), end_dt.isoformat()),
        )
        rows = cur.fetchall()
    finally:
        conn.close()

    if not rows:
        return f"На дату {_format_date(target_date)} броней нет."

    lines = [f"Бронирования на {_format_date(target_date)}:\n"]
    for r in rows:
        start_ts = datetime.fromisoformat(r["start_ts"])
        end_ts = datetime.fromisoformat(r["end_ts"])
        uid = r["user_id"]
        remaining_hours = r["hours"] if r["hours"] is not None else 0
        lines.append(
            f"• user_id {uid}: {start_ts.strftime('%H:%M')}–{end_ts.strftime('%H:%M')} "
            f"(осталось часов: {remaining_hours})"
        )

    return "\n".join(lines)


async def admin_today(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user_id = update.effective_user.id
    if not _require_admin(user_id):
        await update.message.reply_text("Эта команда доступна только владельцу бота.")
        return

    today = date.today()
    text = _format_admin_bookings_for_day(today)
    await update.message.reply_text(text)


async def admin_date(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user_id = update.effective_user.id
    if not _require_admin(user_id):
        await update.message.reply_text("Эта команда доступна только владельцу бота.")
        return

    if not context.args:
        await update.message.reply_text(
            "Укажите дату после команды.\n"
            "Примеры:\n"
            "/admin_date 25.03.2026\n"
            "/admin_date 2026-03-25"
        )
        return

    raw = context.args[0]
    parsed: date | None = None

    for fmt in ("%d.%m.%Y", "%Y-%m-%d"):
        try:
            parsed = datetime.strptime(raw, fmt).date()
            break
        except ValueError:
            continue

    if parsed is None:
        await update.message.reply_text(
            "Не удалось распознать дату. Используйте формат ДД.ММ.ГГГГ или ГГГГ-ММ-ДД."
        )
        return

    text = _format_admin_bookings_for_day(parsed)
    await update.message.reply_text(text)


def _format_admin_bookings_for_period(start_date: date, end_date: date, title: str) -> str:
    start_dt = datetime.combine(start_date, time(0, 0))
    end_dt = datetime.combine(end_date + timedelta(days=1), time(0, 0))

    conn = _get_db_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT b.user_id, b.start_ts, b.end_ts, u.hours
            FROM bookings b
            LEFT JOIN users u ON u.user_id = b.user_id
            WHERE NOT (b.end_ts <= ? OR b.start_ts >= ?)
            ORDER BY b.start_ts
            """,
            (start_dt.isoformat(), end_dt.isoformat()),
        )
        rows = cur.fetchall()
    finally:
        conn.close()

    if not rows:
        return f"На период {title} броней нет."

    lines = [f"Бронирования на период {title}:\n"]
    for r in rows:
        start_ts = datetime.fromisoformat(r["start_ts"])
        end_ts = datetime.fromisoformat(r["end_ts"])
        uid = r["user_id"]
        remaining_hours = r["hours"] if r["hours"] is not None else 0
        lines.append(
            f"• {start_ts.strftime('%d.%m.%Y')} "
            f"{start_ts.strftime('%H:%M')}–{end_ts.strftime('%H:%M')} "
            f"user_id {uid} (осталось часов: {remaining_hours})"
        )

    return "\n".join(lines)


async def admin_this_month(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user_id = update.effective_user.id
    if not _require_admin(user_id):
        await update.message.reply_text("Эта команда доступна только владельцу бота.")
        return

    today = date.today()
    start_date = today.replace(day=1)
    if today.month == 12:
        next_month_first = today.replace(year=today.year + 1, month=1, day=1)
    else:
        next_month_first = today.replace(month=today.month + 1, day=1)
    end_date = next_month_first - timedelta(days=1)

    title = f"{start_date.strftime('%m.%Y')}"
    text = _format_admin_bookings_for_period(start_date, end_date, title)
    await update.message.reply_text(text)


async def admin_next_month(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user_id = update.effective_user.id
    if not _require_admin(user_id):
        await update.message.reply_text("Эта команда доступна только владельцу бота.")
        return

    today = date.today()
    if today.month == 12:
        next_month_first = today.replace(year=today.year + 1, month=1, day=1)
    else:
        next_month_first = today.replace(month=today.month + 1, day=1)

    start_date = next_month_first
    if start_date.month == 12:
        after_next_first = start_date.replace(year=start_date.year + 1, month=1, day=1)
    else:
        after_next_first = start_date.replace(month=start_date.month + 1, day=1)
    end_date = after_next_first - timedelta(days=1)

    title = f"{start_date.strftime('%m.%Y')}"
    text = _format_admin_bookings_for_period(start_date, end_date, title)
    await update.message.reply_text(text)


def _calc_usage_stats_for_period(start_date: date, end_date: date) -> tuple[float, float]:
    """
    Возвращает (занято_часов, всего_часов) за период по датам включительно.
    """
    start_dt = datetime.combine(start_date, time(0, 0))
    end_dt = datetime.combine(end_date + timedelta(days=1), time(0, 0))

    conn = _get_db_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT start_ts, end_ts
            FROM bookings
            WHERE NOT (end_ts <= ? OR start_ts >= ?)
            """,
            (start_dt.isoformat(), end_dt.isoformat()),
        )
        rows = cur.fetchall()
    finally:
        conn.close()

    used_hours = 0.0
    for r in rows:
        s = datetime.fromisoformat(r["start_ts"])
        e = datetime.fromisoformat(r["end_ts"])
        used_hours += (e - s).total_seconds() / 3600

    days_count = (end_date - start_date).days + 1
    total_per_day = WORKING_DAY_END_HOUR - WORKING_DAY_START_HOUR
    total_hours = days_count * total_per_day
    return used_hours, total_hours


async def admin_stats_today(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user_id = update.effective_user.id
    if not _require_admin(user_id):
        await update.message.reply_text("Эта команда доступна только владельцу бота.")
        return

    today = date.today()
    used, total = _calc_usage_stats_for_period(today, today)
    percent = (used / total * 100) if total > 0 else 0.0
    await update.message.reply_text(
        f"Загруженность на сегодня ({_format_date(today)}):\n"
        f"Занято часов: {used:.1f} из {total:.1f} ({percent:.1f}%)."
    )


async def admin_stats_month(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user_id = update.effective_user.id
    if not _require_admin(user_id):
        await update.message.reply_text("Эта команда доступна только владельцу бота.")
        return

    today = date.today()
    start_date = today.replace(day=1)
    if today.month == 12:
        next_month_first = today.replace(year=today.year + 1, month=1, day=1)
    else:
        next_month_first = today.replace(month=today.month + 1, day=1)
    end_date = next_month_first - timedelta(days=1)

    used, total = _calc_usage_stats_for_period(start_date, end_date)
    percent = (used / total * 100) if total > 0 else 0.0
    await update.message.reply_text(
        f"Загруженность за месяц {start_date.strftime('%m.%Y')}:\n"
        f"Занято часов: {used:.1f} из {total:.1f} ({percent:.1f}%)."
    )


async def whoami(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user = update.effective_user
    await update.message.reply_text(
        f"Ваш Telegram user_id: {user.id}\n"
        f"username: @{user.username if user.username else '—'}"
    )


async def administrator(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    Чат с «администратором» на базе DeepSeek.
    Использование: /administrator ваш вопрос одним сообщением.
    """
    if not DEEPSEEK_API_KEY:
        await update.message.reply_text(
            "Функция администратора временно недоступна: не настроен DEEPSEEK_API_KEY."
        )
        return

    user = update.effective_user
    question = " ".join(context.args).strip()

    if not question:
        await update.message.reply_text(
            "Напишите команду так:\n"
            "/administrator ваш вопрос\n\n"
            "Пример: /administrator можно ли забронировать на завтра с 16:00 до 18:00?"
        )
        return

    await update.message.reply_text("Администратор думает над ответом...")

    try:
        client = AsyncOpenAI(api_key=DEEPSEEK_API_KEY, base_url=DEEPSEEK_BASE_URL)
        response = await client.chat.completions.create(
            model=DEEPSEEK_MODEL,
            messages=[
                {
                    "role": "system",
                    "content": (
                        "Ты — администратор кабинета почасовой аренды. "
                        "Отвечай кратко, дружелюбно и по делу. "
                        "Если тебя просят что-то забронировать, уточняй дату, время начала "
                        "и длительность брони, но сам бронь не создаёшь — только формулируешь, "
                        "что именно нужно забронировать, чтобы бот мог это сделать дальше."
                    ),
                },
                {
                    "role": "user",
                    "content": (
                        f"Пользователь с id {user.id} задаёт вопрос администратору:\n\n{question}"
                    ),
                },
            ],
            max_tokens=600,
        )
        answer = (response.choices[0].message.content or "").strip()
        if not answer:
            await update.message.reply_text(
                "Не получилось получить ответ от администратора. Попробуйте переформулировать вопрос."
            )
            return
        await update.message.reply_text(answer)
    except Exception as e:
        logger.warning("DeepSeek administrator error: %s", e)
        await update.message.reply_text(
            "Не удалось связаться с администратором. Попробуйте ещё раз позже."
        )


def _export_bookings_to_csv(rows: Iterable[sqlite3.Row], path: Path) -> None:
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["user_id", "start", "end", "hours"])
        for r in rows:
            start_dt = datetime.fromisoformat(r["start_ts"])
            end_dt = datetime.fromisoformat(r["end_ts"])
            duration_hours = (end_dt - start_dt).total_seconds() / 3600
            writer.writerow([r["user_id"], start_dt.isoformat(), end_dt.isoformat(), duration_hours])


async def export_csv(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user_id = update.effective_user.id
    if ADMIN_USER_ID and user_id != ADMIN_USER_ID:
        await update.message.reply_text("Эта команда доступна только владельцу бота.")
        return

    conn = _get_db_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT user_id, start_ts, end_ts
            FROM bookings
            ORDER BY start_ts
            """
        )
        rows = cur.fetchall()
    finally:
        conn.close()

    if not rows:
        await update.message.reply_text("Пока нет ни одной брони для экспорта.")
        return

    _export_bookings_to_csv(rows, CSV_EXPORT_PATH)

    await update.message.reply_document(
        document=CSV_EXPORT_PATH.open("rb"),
        filename=CSV_EXPORT_PATH.name,
        caption="Экспорт всех бронирований в CSV.\n"
        "Файл можно импортировать в Google Таблицы (Файл → Импорт).",
    )


def main() -> None:
    if not BOT_TOKEN or BOT_TOKEN == "PUT_YOUR_TOKEN_HERE":
        raise RuntimeError(
            "Не указан токен бота. Установите TELEGRAM_BOT_TOKEN или пропишите токен в BOT_TOKEN."
        )
    if not ADMIN_USER_ID:
        raise RuntimeError(
            "Укажите RENT_BOT_ADMIN_ID (ваш числовой Telegram user_id), например в .env."
        )
    if not DEEPSEEK_API_KEY:
        raise RuntimeError("Укажите DEEPSEEK_API_KEY в переменных окружения или .env.")

    _init_db()

    app = ApplicationBuilder().token(BOT_TOKEN).build()

    conv_handler = ConversationHandler(
        entry_points=[CommandHandler("start", start)],
        states={
            CHOOSING_ACTION: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, choose_action),
            ],
            CHOOSING_DAY: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, choose_day),
            ],
            CHOOSING_START_TIME: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, choose_start_time),
            ],
            CHOOSING_DURATION: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, choose_duration),
            ],
            CONFIRM_BOOKING: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, confirm_booking),
            ],
        },
        fallbacks=[CommandHandler("cancel", cancel)],
    )

    app.add_handler(conv_handler)
    app.add_handler(CommandHandler("buy10", buy10))
    app.add_handler(CommandHandler("export_csv", export_csv))
    app.add_handler(CommandHandler("admin_today", admin_today))
    app.add_handler(CommandHandler("admin_date", admin_date))
    app.add_handler(CommandHandler("admin_this_month", admin_this_month))
    app.add_handler(CommandHandler("admin_next_month", admin_next_month))
    app.add_handler(CommandHandler("admin_stats_today", admin_stats_today))
    app.add_handler(CommandHandler("admin_stats_month", admin_stats_month))
    app.add_handler(CommandHandler("whoami", whoami))
    app.add_handler(CommandHandler("administrator", administrator))

    logger.info("Rent bot with abonement started polling...")
    app.run_polling()


if __name__ == "__main__":
    main()

