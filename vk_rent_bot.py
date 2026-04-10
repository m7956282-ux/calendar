import logging
import os
import re
import sqlite3
import time
import json
import base64
from dataclasses import dataclass
from datetime import datetime, timedelta, date, time as dtime, timezone
from pathlib import Path
from collections import defaultdict
from typing import Dict, Optional
from urllib import error as urlerror
from urllib import request as urlrequest

import vk_api
from openai import (
    APIConnectionError,
    APIStatusError,
    APITimeoutError,
    OpenAI,
    RateLimitError,
)
from vk_api.keyboard import VkKeyboard, VkKeyboardColor
from vk_api.longpoll import VkLongPoll, VkEventType


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

BOT_VERSION = "2026-04-10-github-calendar-sync"


VK_TOKEN = os.getenv(
    "VK_RENT_BOT_TOKEN",
    "",
).strip()

BASE_DIR = Path(__file__).resolve().parent
DB_PATH = BASE_DIR / "rent_bot.db"  # общая БД с телеграм-ботом

WORKING_DAY_START_HOUR = 0
WORKING_DAY_END_HOUR = 24
MAX_DURATION_HOURS = 4

# VK админы (ваши VK ID)
ADMIN_VK_IDS = {
    int(os.getenv("VK_RENT_ADMIN_ID", "21476079")),  # Михаил
    164817756,  # Снежана Бурцева
}

# Кому пересылать скриншоты оплат (только Снежане)
PAYMENT_SCREENSHOT_ADMIN_ID = 164817756

# Кому отправлять заявки «Хочу бота» (id 21476079)
WANT_BOT_RECIPIENT_ID = 21476079

# DeepSeek / администратор
DEEPSEEK_API_KEY = os.getenv(
    "DEEPSEEK_API_KEY",
    "",
).strip()
DEEPSEEK_BASE_URL = os.getenv("DEEPSEEK_BASE_URL", "https://api.deepseek.com").strip()
DEEPSEEK_MODEL = os.getenv("DEEPSEEK_MODEL", "deepseek-chat").strip()
# Таймаут HTTP к API (сек.); при лагах на стороне DeepSeek можно поднять, напр. 120
DEEPSEEK_TIMEOUT_SECONDS = float(os.getenv("DEEPSEEK_TIMEOUT_SECONDS", "90"))

# GitHub calendar mirror (repo JSON for GitHub Pages)
CALENDAR_GH_OWNER = os.getenv("CALENDAR_GH_OWNER", "").strip()
CALENDAR_GH_REPO = os.getenv("CALENDAR_GH_REPO", "").strip()
CALENDAR_GH_BRANCH = os.getenv("CALENDAR_GH_BRANCH", "main").strip() or "main"
CALENDAR_GH_PATH = os.getenv("CALENDAR_GH_PATH", "data/bookings.json").strip() or "data/bookings.json"
CALENDAR_GH_TOKEN = os.getenv("CALENDAR_GH_TOKEN", "").strip()
CALENDAR_SYNC_PAST_DAYS = int(os.getenv("CALENDAR_SYNC_PAST_DAYS", "30"))
CALENDAR_SYNC_FUTURE_DAYS = int(os.getenv("CALENDAR_SYNC_FUTURE_DAYS", "180"))
CALENDAR_SYNC_TIMEOUT_SECONDS = float(os.getenv("CALENDAR_SYNC_TIMEOUT_SECONDS", "10"))


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
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS payments (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id     INTEGER NOT NULL,
                amount      INTEGER NOT NULL,
                hours_added INTEGER NOT NULL,
                created_at  TEXT NOT NULL
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS referrals (
                user_id     INTEGER PRIMARY KEY,   -- реферал
                referrer_id INTEGER NOT NULL,
                created_at  TEXT NOT NULL
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS user_welcome (
                user_id INTEGER PRIMARY KEY,
                welcomed_at TEXT NOT NULL
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS referral_payments (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                referrer_id INTEGER NOT NULL,
                user_id     INTEGER NOT NULL,
                payment_id  INTEGER NOT NULL,
                bonus_hours INTEGER NOT NULL,
                created_at  TEXT NOT NULL
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS awaiting_payment (
                user_id    INTEGER PRIMARY KEY,
                hours      REAL NOT NULL,
                updated_at TEXT NOT NULL
            )
            """
        )
        conn.commit()
        _ensure_balance_minutes_column(conn)
    finally:
        conn.close()


def _calendar_sync_enabled() -> bool:
    return bool(CALENDAR_GH_OWNER and CALENDAR_GH_REPO and CALENDAR_GH_TOKEN)


def _calendar_json_payload() -> dict:
    """
    Публичный JSON для GitHub Pages (без персональных данных пользователей).
    """
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    from_dt = now - timedelta(days=max(1, CALENDAR_SYNC_PAST_DAYS))
    to_dt = now + timedelta(days=max(1, CALENDAR_SYNC_FUTURE_DAYS))
    conn = _get_db_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT start_ts, end_ts
            FROM bookings
            WHERE NOT (end_ts < ? OR start_ts > ?)
            ORDER BY start_ts
            """,
            (from_dt.isoformat(), to_dt.isoformat()),
        )
        rows = cur.fetchall()
    finally:
        conn.close()
    bookings: list[dict] = []
    for r in rows:
        s = _parse_booking_ts(r["start_ts"])
        e = _parse_booking_ts(r["end_ts"])
        bookings.append(
            {
                "start_ts": s.isoformat(),
                "end_ts": e.isoformat(),
                "duration_minutes": int((e - s).total_seconds() // 60),
            }
        )
    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "timezone": "UTC",
        "source": "vk_rent_bot",
        "from_ts": from_dt.isoformat(),
        "to_ts": to_dt.isoformat(),
        "bookings": bookings,
    }


def _github_api_request(method: str, url: str, body: Optional[dict] = None) -> dict:
    headers = {
        "Accept": "application/vnd.github+json",
        "Authorization": f"Bearer {CALENDAR_GH_TOKEN}",
        "X-GitHub-Api-Version": "2022-11-28",
        "User-Agent": "vk-rent-bot-calendar-sync",
    }
    data = None
    if body is not None:
        data = json.dumps(body, ensure_ascii=False).encode("utf-8")
        headers["Content-Type"] = "application/json; charset=utf-8"
    req = urlrequest.Request(url=url, data=data, headers=headers, method=method)
    with urlrequest.urlopen(req, timeout=CALENDAR_SYNC_TIMEOUT_SECONDS) as resp:
        raw = resp.read().decode("utf-8")
        return json.loads(raw) if raw else {}


def _sync_calendar_json_to_github(reason: str = "") -> None:
    """
    Обновляет data/bookings.json в GitHub-репозитории.
    Вызывается после добавления/удаления брони.
    """
    if not _calendar_sync_enabled():
        return
    try:
        payload = _calendar_json_payload()
        json_bytes = json.dumps(payload, ensure_ascii=False, indent=2).encode("utf-8")
        content_b64 = base64.b64encode(json_bytes).decode("ascii")
        api_url = (
            f"https://api.github.com/repos/{CALENDAR_GH_OWNER}/{CALENDAR_GH_REPO}/contents/{CALENDAR_GH_PATH}"
        )
        sha = None
        try:
            existing = _github_api_request("GET", f"{api_url}?ref={CALENDAR_GH_BRANCH}")
            sha = existing.get("sha")
        except urlerror.HTTPError as e:
            if e.code != 404:
                raise
        commit_body = {
            "message": f"sync calendar: {reason or 'bookings update'}",
            "content": content_b64,
            "branch": CALENDAR_GH_BRANCH,
        }
        if sha:
            commit_body["sha"] = sha
        _github_api_request("PUT", api_url, commit_body)
        logger.info(
            "GitHub calendar sync ok (%s/%s, path=%s, reason=%s, items=%s)",
            CALENDAR_GH_OWNER,
            CALENDAR_GH_REPO,
            CALENDAR_GH_PATH,
            reason or "-",
            len(payload.get("bookings", [])),
        )
    except Exception as e:
        logger.warning("GitHub calendar sync failed (%s): %s", reason or "-", e)


def _ensure_balance_minutes_column(conn: sqlite3.Connection) -> None:
    """
    Дробные часы на абонементе (например 1,5 ч бронь): баланс в минутах.
    Колонка hours оставлена для совместимости (целые часы, floor), источник истины — balance_minutes.
    """
    cur = conn.cursor()
    cur.execute("PRAGMA table_info(users)")
    cols = {row[1] for row in cur.fetchall()}
    if "balance_minutes" not in cols:
        cur.execute(
            "ALTER TABLE users ADD COLUMN balance_minutes INTEGER NOT NULL DEFAULT 0"
        )
        cur.execute("UPDATE users SET balance_minutes = COALESCE(hours, 0) * 60")
        conn.commit()


# Сколько часов хранить «ожидание скрина оплаты» в БД после выбора тарифа (переживает перезапуск бота).
AWAITING_PAYMENT_TTL_HOURS = 72


def _clear_awaiting_payment(user_id: int) -> None:
    conn = _get_db_connection()
    try:
        conn.execute("DELETE FROM awaiting_payment WHERE user_id = ?", (user_id,))
        conn.commit()
    finally:
        conn.close()


def _save_awaiting_payment(user_id: int, hours: float) -> None:
    conn = _get_db_connection()
    try:
        conn.execute(
            """
            INSERT INTO awaiting_payment (user_id, hours, updated_at)
            VALUES (?, ?, ?)
            ON CONFLICT(user_id) DO UPDATE SET
                hours = excluded.hours,
                updated_at = excluded.updated_at
            """,
            (user_id, float(hours), datetime.now(timezone.utc).isoformat()),
        )
        conn.commit()
    finally:
        conn.close()


def _enter_await_payment(state: "UserState", user_id: int, hours: float) -> None:
    state.mode = "await_payment"
    state.pending_add_hours = float(hours)
    _save_awaiting_payment(user_id, float(hours))


def _restore_awaiting_payment_from_db(user_id: int, state: "UserState") -> None:
    """
    После перезапуска процесса STATES пустой: восстанавливаем режим ожидания скрина из БД.
    Не трогаем пользователя, который уже в другом режиме в памяти.
    """
    if state.mode != "idle":
        return
    conn = _get_db_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            "SELECT hours, updated_at FROM awaiting_payment WHERE user_id = ?",
            (user_id,),
        )
        row = cur.fetchone()
        if row is None:
            return
        hours = float(row["hours"])
        updated_raw = row["updated_at"]
        try:
            udt = _parse_booking_ts(str(updated_raw))
        except Exception:
            udt = datetime(1970, 1, 1, 0, 0, 0)
        now = datetime.now(timezone.utc).replace(tzinfo=None)
        if (now - udt).total_seconds() > AWAITING_PAYMENT_TTL_HOURS * 3600:
            cur.execute("DELETE FROM awaiting_payment WHERE user_id = ?", (user_id,))
            conn.commit()
            return
        state.mode = "await_payment"
        state.pending_add_hours = hours
    finally:
        conn.close()


def _format_hours_balance(h: float) -> str:
    """Человекочитаемый остаток/списание (в т.ч. 1,5 ч и 90 мин)."""
    if h < 0:
        h = 0.0
    if abs(h - round(h)) < 1e-6:
        return f"{int(round(h))} ч."
    whole = int(h)
    mins = int(round((h - whole) * 60))
    if mins == 0:
        return f"{whole} ч."
    if whole == 0:
        return f"{mins} мин"
    return f"{whole} ч {mins} мин"


def _parse_booking_ts(ts: str) -> datetime:
    """
    ISO-строка start_ts/end_ts из БД → наивный datetime в UTC.
    Нужно, чтобы не смешивать aware/naive при сравнении с «сейчас» и другими наивными датами.
    """
    if not ts:
        return datetime(1970, 1, 1, 0, 0, 0)
    t = str(ts).replace("Z", "+00:00")
    dt = datetime.fromisoformat(t)
    if dt.tzinfo is not None:
        return dt.astimezone(timezone.utc).replace(tzinfo=None)
    return dt


def _parse_vk_user_id_from_text(text: str) -> Optional[int]:
    """
    VK ID из ввода админа: только цифры, либо ссылка на профиль (vk.com/id…),
    как в списке «Участники». Если в сообщении много текста — ищем ссылку в любом месте.
    """
    if not text:
        return None
    s = text.strip()
    # ссылка в любом месте сообщения (копипаст списка участников + ссылка)
    m = re.search(
        r"(?:https?://)?(?:m\.)?vk\.(?:com|ru)/id(\d+)",
        s,
        re.IGNORECASE,
    )
    if m:
        return int(m.group(1))
    for line in s.splitlines():
        line = line.strip()
        if not line:
            continue
        if line.isdigit():
            return int(line)
        m = re.search(r"\bid(\d+)\b", line, re.IGNORECASE)
        if m:
            return int(m.group(1))
    return None


def _extract_vk_screen_name(text: str) -> Optional[str]:
    """
    Достаёт screen_name из ввода:
    - vk.com/screen_name
    - @screen_name
    - screen_name (целиком строка)
    """
    if not text:
        return None
    s = text.strip()
    if not s:
        return None

    # vk.com/<screen_name> (исключая id123); в никах VK бывают точка и дефис
    m = re.search(
        r"(?:https?://)?(?:m\.)?vk\.(?:com|ru)/([A-Za-z0-9_.-]{3,})",
        s,
        re.IGNORECASE,
    )
    if m:
        token = m.group(1).strip()
        if re.fullmatch(r"id\d+", token, re.IGNORECASE):
            return None
        return token

    # @screen_name
    m = re.fullmatch(r"@\s*([A-Za-z0-9_.-]{3,})", s)
    if m:
        return m.group(1).strip()

    # просто screen_name
    if re.fullmatch(r"[A-Za-z0-9_.-]{3,}", s) and not s.isdigit():
        if re.fullmatch(r"id\d+", s, re.IGNORECASE):
            return None
        return s
    return None


def _resolve_vk_user_id(vk, text: str) -> Optional[int]:
    """
    Универсальный разбор пользователя VK из ввода:
    - числовой id / id123 / ссылка на id
    - screen_name / @screen_name / ссылка vk.com/screen_name
    """
    uid = _parse_vk_user_id_from_text(text)
    if uid is not None:
        return uid

    screen_name = _extract_vk_screen_name(text)
    if not screen_name:
        return None
    try:
        users = vk.users.get(user_ids=screen_name)
        if users:
            return int(users[0]["id"])
    except Exception as e:
        logger.warning("Не удалось резолвить никнейм '%s' в VK ID: %s", screen_name, e)
    return None


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


def _is_free(start_dt: datetime, end_dt: datetime) -> bool:
    """
    Проверяет, свободен ли слот с учётом 15‑минутного запаса до и после
    предыдущих/следующих бронирований.

    То есть если бронь заканчивается в 12:00, следующую можно поставить
    не раньше 12:15, и наоборот.
    """
    buffer = timedelta(minutes=15)

    # Берём кандидатов из БД и проверяем конфликт в Python,
    # чтобы корректно обрабатывать границы (например, 19:00 -> 19:00 недопустимо).
    query_start = (start_dt - buffer).isoformat()
    query_end = (end_dt + buffer).isoformat()

    conn = _get_db_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT start_ts, end_ts
            FROM bookings
            WHERE NOT (end_ts <= ? OR start_ts >= ?)
            """,
            (query_start, query_end),
        )
        rows = cur.fetchall()
    finally:
        conn.close()

    for r in rows:
        existing_start = _parse_booking_ts(r["start_ts"])
        existing_end = _parse_booking_ts(r["end_ts"])
        # Конфликт, если интервалы пересекаются с учётом буфера
        # (строгое правило: следующий старт не раньше existing_end + buffer)
        if start_dt < existing_end + buffer and end_dt > existing_start - buffer:
            return False

    return True


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
    _sync_calendar_json_to_github("add_booking")


def _get_user_hours(user_id: int) -> float:
    conn = _get_db_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            "SELECT hours, balance_minutes FROM users WHERE user_id = ?",
            (user_id,),
        )
        row = cur.fetchone()
        if row is None:
            return 0.0
        try:
            bm = row["balance_minutes"]
        except (KeyError, IndexError):
            bm = None
        if bm is not None:
            return float(bm) / 60.0
        return float(row["hours"])
    finally:
        conn.close()


def _has_referrer_discount(user_id: int) -> bool:
    """
    Проверяет, имеет ли пользователь право на скидку 100 ₽
    при первой покупке абонемента на 10 часов:
    - есть записанный реферер
    - ещё не было реальных оплат (amount > 0)
    """
    conn = _get_db_connection()
    try:
        cur = conn.cursor()
        # есть ли реферер
        cur.execute(
            "SELECT referrer_id FROM referrals WHERE user_id = ?",
            (user_id,),
        )
        row = cur.fetchone()
        if row is None:
            return False
        # были ли уже реальные оплаты
        cur.execute(
            "SELECT 1 FROM payments WHERE user_id = ? AND amount > 0",
            (user_id,),
        )
        paid = cur.fetchone()
        return paid is None
    finally:
        conn.close()


def _referrer_exists_in_db(ref_id: int) -> bool:
    """Проверяем, что в базе уже есть хоть какая-то активность этого пользователя."""
    conn = _get_db_connection()
    try:
        cur = conn.cursor()
        # пользователи с часами
        cur.execute(
            """
            SELECT 1 FROM users WHERE user_id = ?
            UNION
            SELECT 1 FROM payments WHERE user_id = ?
            UNION
            SELECT 1 FROM referrals WHERE referrer_id = ? OR user_id = ?
            LIMIT 1
            """,
            (ref_id, ref_id, ref_id, ref_id),
        )
        return cur.fetchone() is not None
    finally:
        conn.close()


def _set_user_hours(user_id: int, hours: float) -> None:
    """Часы на абонементе; дробные значения (например 8,5 ч) хранятся как balance_minutes."""
    minutes = max(0, int(round(float(hours) * 60)))
    legacy_hours = minutes // 60
    conn = _get_db_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO users (user_id, hours, balance_minutes)
            VALUES (?, ?, ?)
            ON CONFLICT(user_id) DO UPDATE SET
                hours = excluded.hours,
                balance_minutes = excluded.balance_minutes
            """,
            (user_id, legacy_hours, minutes),
        )
        conn.commit()
    finally:
        conn.close()


def _add_10_hours(user_id: int) -> float:
    current = _get_user_hours(user_id)
    new_value = current + 10
    _set_user_hours(user_id, new_value)
    # фиксируем тестовое пополнение как payment с amount=0
    conn = _get_db_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO payments (user_id, amount, hours_added, created_at) VALUES (?, ?, ?, ?)",
            (user_id, 0, 10, datetime.now(timezone.utc).isoformat()),
        )
        conn.commit()
    finally:
        conn.close()
    return new_value


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

    lines = []
    for r in rows:
        start_dt = datetime.fromisoformat(r["start_ts"])
        end_dt = datetime.fromisoformat(r["end_ts"])
        lines.append(
            f"{_format_date(start_dt.date())}: "
            f"{start_dt.strftime('%H:%M')}–{end_dt.strftime('%H:%M')}"
        )
    return "Ваши бронирования:\n\n" + "\n".join(f"• {line}" for line in lines)


def _get_future_bookings(user_id: int) -> list[sqlite3.Row]:
    """Возвращает все будущие бронирования пользователя, отсортированные по времени начала."""
    now_iso = datetime.now(timezone.utc).isoformat()
    conn = _get_db_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT id, start_ts, end_ts
            FROM bookings
            WHERE user_id = ? AND end_ts >= ?
            ORDER BY start_ts
            """,
            (user_id, now_iso),
        )
        return cur.fetchall()
    finally:
        conn.close()


def _delete_booking(booking_id: int) -> None:
    conn = _get_db_connection()
    try:
        cur = conn.cursor()
        cur.execute("DELETE FROM bookings WHERE id = ?", (booking_id,))
        conn.commit()
    finally:
        conn.close()
    _sync_calendar_json_to_github("delete_booking")


def _get_booking_by_id(booking_id: int) -> Optional[sqlite3.Row]:
    conn = _get_db_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            "SELECT id, user_id, start_ts, end_ts FROM bookings WHERE id = ?",
            (booking_id,),
        )
        return cur.fetchone()
    finally:
        conn.close()


def _get_user_future_bookings(user_id: int) -> list[sqlite3.Row]:
    now_iso = datetime.now(timezone.utc).isoformat()
    conn = _get_db_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT id, start_ts, end_ts
            FROM bookings
            WHERE user_id = ? AND end_ts >= ?
            ORDER BY start_ts
            """,
            (user_id, now_iso),
        )
        return cur.fetchall()
    finally:
        conn.close()


def _touch_known_user(user_id: int) -> None:
    """Добавляет пользователя в users с 0 ч., если записи ещё нет — чтобы он учитывался в списке участников и рассылках."""
    conn = _get_db_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO users (user_id, hours, balance_minutes)
            VALUES (?, 0, 0)
            ON CONFLICT(user_id) DO NOTHING
            """,
            (user_id,),
        )
        conn.commit()
    finally:
        conn.close()


def _get_all_known_user_ids() -> list[int]:
    """Все user_id, которые встречались в системе (для рассылок и «участники»)."""
    conn = _get_db_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT user_id FROM users
            UNION
            SELECT user_id FROM bookings
            UNION
            SELECT user_id FROM payments
            UNION
            SELECT referrer_id AS user_id FROM referrals
            UNION
            SELECT user_id FROM referrals
            UNION
            SELECT user_id FROM user_welcome
            UNION
            SELECT user_id FROM referral_payments
            UNION
            SELECT referrer_id AS user_id FROM referral_payments
            """
        )
        return [int(r[0]) for r in cur.fetchall() if r[0] is not None]
    finally:
        conn.close()


def _broadcast_free_slot(vk, start_dt: datetime, end_dt: datetime, exclude_user_id: Optional[int] = None) -> None:
    """Рассылка об освобождении слота отключена.

    Раньше бот отправлял всем пользователям сообщение «Освободилось время для записи».
    Сейчас уведомления убраны, чтобы не спамить и не засорять чат.
    """
    return


def _send_admin_period_report(vk, user_id: int, start_date: date, end_date: date) -> None:
    """Отчёт по бронированиям за период (как в режиме admin_report_range)."""
    used, total = _calc_usage_stats_for_period(start_date, end_date)
    percent = (used / total * 100) if total > 0 else 0.0
    bookings_text = _format_admin_bookings_for_period(vk, start_date, end_date)
    report = (
        f"Отчет по бронированиям за период {start_date.strftime('%d.%m.%Y')} — "
        f"{end_date.strftime('%d.%m.%Y')}:\n\n"
        f"Занято часов: {used:.1f} из {total:.1f} ({percent:.1f}%).\n\n"
        f"{bookings_text}"
    )
    send_message(vk, user_id, report, keyboard=_main_keyboard_for(user_id))


def _parse_period_text(text: str) -> Optional[tuple[date, date]]:
    """Парсит период для админ-команды записей."""
    txt = text.strip().lower()
    today = date.today()
    if txt in ("сегодня", "today"):
        return today, today
    if txt in ("этот месяц", "текущий месяц"):
        start_date = today.replace(day=1)
        if today.month == 12:
            nm = today.replace(year=today.year + 1, month=1, day=1)
        else:
            nm = today.replace(month=today.month + 1, day=1)
        end_date = nm - timedelta(days=1)
        return start_date, end_date
    if "с " in txt and " по " in txt:
        try:
            part = txt.replace("с ", "", 1)
            left, right = part.split(" по ", 1)
            start_date = datetime.strptime(left.strip(), "%d.%m.%Y").date()
            end_date = datetime.strptime(right.strip(), "%d.%m.%Y").date()
            return start_date, end_date
        except Exception:
            return None
    return None


def _get_bookings_for_period(start_date: date, end_date: date) -> list[sqlite3.Row]:
    start_dt = datetime.combine(start_date, dtime(0, 0))
    end_dt = datetime.combine(end_date + timedelta(days=1), dtime(0, 0))
    conn = _get_db_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT id, user_id, start_ts, end_ts
            FROM bookings
            WHERE NOT (end_ts <= ? OR start_ts >= ?)
            ORDER BY start_ts
            """,
            (start_dt.isoformat(), end_dt.isoformat()),
        )
        return cur.fetchall()
    finally:
        conn.close()


def _parse_time_15(text: str) -> Optional[dtime]:
    """
    Парсит время с шагом 15 минут из свободного текста.
    Понимает варианты:
    - 10
    - 10 утра / 10 вечера / 10 дня / 10 ночи
    - 10:00, 10.00, 10,00, 10-00, 10\00
    - 18 15, 18:15, 18-15 и т.п.
    """
    txt = text.strip().lower()
    # Уберем слова про время суток
    is_pm = False
    for marker in ("утра", "ночи", "дня", "вечера"):
        if marker in txt:
            if marker in ("вечера", "дня"):
                is_pm = True
            txt = txt.replace(marker, "")
    txt = txt.strip()

    # Заменяем разные разделители на двоеточие
    for sep in ("\\", "-", ".", ",", " "):
        txt = txt.replace(sep, ":")

    # Если только часы (например, "10")
    if ":" not in txt:
        try:
            h = int(txt)
        except ValueError:
            return None
        m = 0
    else:
        hh, mm = txt.split(":", 1)
        try:
            h = int(hh) if hh else 0
            m = int(mm) if mm else 0
        except ValueError:
            return None

    # Учитываем "вечер" / "день"
    if is_pm and 1 <= h <= 11:
        h += 12

    if not (0 <= h <= 23 and 0 <= m <= 59):
        return None
    if m % 15 != 0:
        return None
    return dtime(hour=h, minute=m)


def _parse_human_date(text: str) -> Optional[date]:
    """
    Пытается распознать дату из свободного текста:
    - сегодня / завтра / послезавтра
    - ДД.ММ.ГГГГ
    - ДД.ММ.ГГ
    - ДД месяц [ГГГГ], например: 18 марта 2026, 5 мая
    """
    txt = text.strip().lower()
    today = date.today()

    if txt in ("сегодня", "today"):
        return today
    if txt in ("завтра", "tomorrow"):
        return today + timedelta(days=1)
    if txt in ("послезавтра",):
        return today + timedelta(days=2)

    # форматы с точками или пробелами: 18.03.2026, 18 03 26
    txt_dots = txt.replace(" ", ".")
    for fmt in ("%d.%m.%Y", "%d.%m.%y"):
        try:
            return datetime.strptime(txt_dots, fmt).date()
        except ValueError:
            pass

    # форматы с русскими месяцами
    months = {
        "январ": 1,
        "феврал": 2,
        "март": 3,
        "апрел": 4,
        "мая": 5,
        "май": 5,
        "июн": 6,
        "июл": 7,
        "август": 8,
        "сентябр": 9,
        "октябр": 10,
        "ноябр": 11,
        "декабр": 12,
    }
    parts = txt.replace(",", " ").split()
    if len(parts) >= 2:
        day_str, month_str = parts[0], parts[1]
        try:
            d = int(day_str)
        except ValueError:
            d = None
        if d is not None:
            m = None
            for key, num in months.items():
                if month_str.startswith(key):
                    m = num
                    break
            if m is not None:
                if len(parts) >= 3:
                    try:
                        y = int(parts[2])
                    except ValueError:
                        y = today.year
                else:
                    y = today.year
                try:
                    return date(y, m, d)
                except ValueError:
                    return None
    return None


def _find_nearest_free_interval(
    day: date, duration_hours: float, requested_start: datetime
) -> Optional[tuple[datetime, datetime]]:
    """
    Ищет ближайший свободный интервал той же длительности в этот же день,
    двигаясь по шагу 15 минут назад и вперед от запрошенного времени.
    Возвращает (start_dt, end_dt) или None.
    """
    step = timedelta(minutes=15)
    max_dt = datetime.combine(day, dtime(23, 45))
    min_dt = datetime.combine(day, dtime(0, 0))

    candidates: list[datetime] = []
    # Собираем точки во времени вокруг запроса.
    # ВАЖНО: сначала предлагаем время ПОСЛЕ (requested_start + 15 минут),
    # т.к. это самый частый кейс "впритык к окончанию прошлой брони".
    for i in range(1, 4 * 24):  # до суток в обе стороны по 15 минут
        later = requested_start + step * i
        earlier = requested_start - step * i
        if later <= max_dt:
            candidates.append(later)
        if earlier >= min_dt:
            candidates.append(earlier)

    for start_dt in candidates:
        end_dt = start_dt + timedelta(hours=float(duration_hours))
        if end_dt.date() != day:
            continue
        if _is_free(start_dt, end_dt):
            return start_dt, end_dt
    return None


@dataclass
class UserState:
    mode: str = (
        "idle"  # idle | choosing_day | choosing_time | choosing_duration
        # | confirm | admin_report_range | buy_choice | await_payment | cancel_select
        # | admin_cancel_ask_client | admin_add_hours_ask_client | admin_add_hours_ask_delta
        # | admin_remove_hours_ask_client | admin_remove_hours_ask_delta
    )
    chosen_day: Optional[date] = None
    start_time: Optional[dtime] = None
    duration_hours: Optional[float] = None  # 1.0 … 4.0, в т.ч. 1.5 и 2.5 ч
    start_dt: Optional[datetime] = None
    end_dt: Optional[datetime] = None
    pending_add_hours: Optional[float] = None  # ожидаемая оплата: 1, 1.5, 2, 2.5, 3 или 10 ч.
    # Если задан — отмена брони для этого VK ID (только админ, режим cancel_select)
    cancel_for_user_id: Optional[int] = None
    # Кому начислять часы в режиме admin_add_hours_ask_delta (только админ)
    admin_hours_target_id: Optional[int] = None


STATES: Dict[int, UserState] = {}
# История переписки с администратором для каждого пользователя (для контекста диалога)
ADMIN_HISTORY: Dict[int, list[dict]] = {}


def _clear_admin_chat_history(user_id: int) -> None:
    """Сброс контекста ИИ, чтобы старые ответы не противоречили актуальному балансу."""
    ADMIN_HISTORY.pop(user_id, None)


# Тексты кнопок в нижнем регистре (новые + старые подписи после переименования)
_BONUS_MENU_TEXTS_LOWER: frozenset[str] = frozenset(
    s.strip().lower()
    for s in (
        "👥 Бонусная система",
        "👥 Реферальная программа",
        "бонусная система",
        "реферальная программа",
    )
)
_ADMIN_BONUS_STATS_TEXTS_LOWER: frozenset[str] = frozenset(
    s.strip().lower()
    for s in (
        "📊 Бонусы по кодам",
        "📊 Реферальная статистика",
        "бонусы по кодам",
        "реферальная статистика",
    )
)


def _main_keyboard_base() -> VkKeyboard:
    kb = VkKeyboard(one_time=False)
    kb.add_button("📅 Забронировать кабинет", color=VkKeyboardColor.PRIMARY)
    kb.add_line()
    kb.add_button("📖 Мои бронирования", color=VkKeyboardColor.SECONDARY)
    kb.add_button("🎫 Мой абонемент", color=VkKeyboardColor.SECONDARY)
    kb.add_line()
    kb.add_button("📆 Свободные даты", color=VkKeyboardColor.POSITIVE)
    kb.add_line()
    kb.add_button("❌ Отменить бронирование", color=VkKeyboardColor.NEGATIVE)
    kb.add_line()
    kb.add_button("💳 Купить абонемент", color=VkKeyboardColor.PRIMARY)
    kb.add_line()
    kb.add_button("👥 Бонусная система", color=VkKeyboardColor.SECONDARY)
    kb.add_line()
    kb.add_button("💬 Вопрос Снежане", color=VkKeyboardColor.SECONDARY)
    kb.add_line()
    kb.add_button("🤖 Тоже хочу бота", color=VkKeyboardColor.SECONDARY)
    return kb


def _main_keyboard_for(user_id: int) -> VkKeyboard:
    """Главное меню; у администраторов дополнительно кнопка «Команды»."""
    kb = _main_keyboard_base()
    if user_id in ADMIN_VK_IDS:
        kb.add_line()
        kb.add_button("⚙️ Команды", color=VkKeyboardColor.SECONDARY)
    return kb


def _admin_keyboard() -> VkKeyboard:
    """Панель администратора: те же действия, что и текстовые команды (кроме «удалить запись …»)."""
    kb = VkKeyboard(one_time=False)
    kb.add_button("Баланс", color=VkKeyboardColor.PRIMARY)
    kb.add_button("Участники", color=VkKeyboardColor.PRIMARY)
    kb.add_line()
    kb.add_button("Отчет сегодня", color=VkKeyboardColor.SECONDARY)
    kb.add_button("Отчет этот месяц", color=VkKeyboardColor.SECONDARY)
    kb.add_line()
    kb.add_button("Записи сегодня", color=VkKeyboardColor.SECONDARY)
    kb.add_button("Записи этот месяц", color=VkKeyboardColor.SECONDARY)
    kb.add_line()
    kb.add_button("📊 Бонусы по кодам", color=VkKeyboardColor.POSITIVE)
    kb.add_button("Отменить бронирование клиента", color=VkKeyboardColor.NEGATIVE)
    kb.add_line()
    kb.add_button("➕ Добавить часы", color=VkKeyboardColor.PRIMARY)
    kb.add_button("➖ Удалить часы", color=VkKeyboardColor.NEGATIVE)
    kb.add_line()
    kb.add_button("⬅ Главное меню", color=VkKeyboardColor.SECONDARY)
    return kb


def _back_keyboard() -> VkKeyboard:
    kb = VkKeyboard(one_time=False)
    kb.add_button("⬅ Назад", color=VkKeyboardColor.NEGATIVE)
    return kb


def _times_keyboard(chosen_day: date) -> VkKeyboard:
    kb = VkKeyboard(one_time=False)
    current_dt = datetime.combine(chosen_day, dtime(hour=WORKING_DAY_START_HOUR, minute=0))
    end_dt_limit = datetime.combine(chosen_day + timedelta(days=1), dtime(0, 0))
    col = 0
    while current_dt < end_dt_limit:
        t = current_dt.time()
        kb.add_button(t.strftime("%H:%M"), color=VkKeyboardColor.PRIMARY)
        col += 1
        if col >= 4:
            kb.add_line()
            col = 0
        current_dt += timedelta(minutes=15)
    if col != 0:
        kb.add_line()
    kb.add_button("⬅ Назад", color=VkKeyboardColor.NEGATIVE)
    return kb


def _text_looks_like_booking_time(t: str) -> bool:
    """Отсечь фразы про оплату «1.5 часа» от контекста брони «в 14:30» / «18.03.2026»."""
    if re.search(r"\d{1,2}:\d{2}", t):
        return True
    if re.search(r"\d{1,2}\.\d{1,2}\.\d{4}", t):
        return True
    return any(word in t for word in ("\\", "утра", "вечера", "дня", "ночи"))


def _parse_duration_hours(text: str) -> Optional[float]:
    """Длительность брони: 1 ч., 1,5 часа, 2 ч., … (до MAX_DURATION_HOURS часов)."""
    t = text.strip().lower().replace("ё", "е")
    # 1,5 часа / 1,5 ч. / 90 мин (старый вариант кнопки)
    if re.match(r"^1[,.]5\s*(часа|часов|ч\.)", t) or re.match(r"^1[,.]5\s*$", t):
        return 1.5
    if re.match(r"^90\s*мин", t):
        return 1.5
    # 2,5 часа / 2,5 ч.
    if re.match(r"^2[,.]5\s*(часа|часов|ч\.)", t) or re.match(r"^2[,.]5\s*$", t):
        return 2.5
    m = re.match(r"^(\d+)\s*ч", t)
    if m:
        v = float(m.group(1))
        if 1 <= v <= MAX_DURATION_HOURS:
            return v
    return None


def _duration_keyboard() -> VkKeyboard:
    kb = VkKeyboard(one_time=False)
    kb.add_button("1 ч.", color=VkKeyboardColor.PRIMARY)
    kb.add_button("1,5 часа", color=VkKeyboardColor.PRIMARY)
    kb.add_line()
    kb.add_button("2 ч.", color=VkKeyboardColor.PRIMARY)
    kb.add_button("2,5 часа", color=VkKeyboardColor.PRIMARY)
    kb.add_line()
    kb.add_button("3 ч.", color=VkKeyboardColor.PRIMARY)
    kb.add_button("4 ч.", color=VkKeyboardColor.PRIMARY)
    kb.add_line()
    kb.add_button("⬅ Назад", color=VkKeyboardColor.NEGATIVE)
    return kb


def _confirm_keyboard(with_break_button: bool = False) -> VkKeyboard:
    kb = VkKeyboard(one_time=False)
    kb.add_button("подтвердить", color=VkKeyboardColor.POSITIVE)
    if with_break_button:
        kb.add_line()
        kb.add_button("Нужен перерыв 15 минут", color=VkKeyboardColor.SECONDARY)
    kb.add_line()
    kb.add_button("отмена", color=VkKeyboardColor.NEGATIVE)
    kb.add_line()
    kb.add_button("⬅ Назад", color=VkKeyboardColor.SECONDARY)
    return kb


def _buy_keyboard() -> VkKeyboard:
    kb = VkKeyboard(one_time=False)
    kb.add_button("1 час (300 ₽)", color=VkKeyboardColor.PRIMARY)
    kb.add_button("1,5 часа (450 ₽)", color=VkKeyboardColor.PRIMARY)
    kb.add_line()
    kb.add_button("2 часа (600 ₽)", color=VkKeyboardColor.PRIMARY)
    kb.add_button("2,5 часа (750 ₽)", color=VkKeyboardColor.PRIMARY)
    kb.add_line()
    kb.add_button("3 часа (900 ₽)", color=VkKeyboardColor.PRIMARY)
    kb.add_line()
    kb.add_button("10 часов (2500 ₽)", color=VkKeyboardColor.PRIMARY)
    kb.add_line()
    kb.add_button("⬅ Назад", color=VkKeyboardColor.NEGATIVE)
    return kb


def _payment_wait_keyboard() -> VkKeyboard:
    kb = VkKeyboard(one_time=False)
    kb.add_button("отмена", color=VkKeyboardColor.NEGATIVE)
    kb.add_line()
    kb.add_button("⬅ Назад", color=VkKeyboardColor.SECONDARY)
    return kb


def _calc_usage_stats_for_period(start_date: date, end_date: date) -> tuple[float, float]:
    """Возвращает (занято_часов, всего_часов) за период по датам включительно."""
    start_dt = datetime.combine(start_date, dtime(0, 0))
    end_dt = datetime.combine(end_date + timedelta(days=1), dtime(0, 0))

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


def _format_admin_bookings_for_period(vk, start_date: date, end_date: date) -> str:
    start_dt = datetime.combine(start_date, dtime(0, 0))
    end_dt = datetime.combine(end_date + timedelta(days=1), dtime(0, 0))

    conn = _get_db_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT user_id, start_ts, end_ts
            FROM bookings
            WHERE NOT (end_ts <= ? OR start_ts >= ?)
            ORDER BY start_ts
            """,
            (start_dt.isoformat(), end_dt.isoformat()),
        )
        rows = cur.fetchall()
    finally:
        conn.close()

    if not rows:
        return "За выбранный период бронирований нет."

    # Имена пользователей пачкой (чтобы показывать администратору не только id)
    uids = sorted({int(r["user_id"]) for r in rows})
    name_map: Dict[int, str] = {}
    if uids:
        try:
            vk_users = vk.users.get(user_ids=",".join(str(u) for u in uids))
            for u in vk_users:
                uid = int(u["id"])
                name = f'{u.get("first_name", "")} {u.get("last_name", "")}'.strip()
                name_map[uid] = name if name else f"user_id {uid}"
        except Exception as e:
            logger.warning("Не удалось получить имена пользователей для отчёта: %s", e)
            for uid in uids:
                name_map[uid] = _get_vk_name(vk, uid)

    lines = []
    for r in rows:
        s = datetime.fromisoformat(r["start_ts"])
        e = datetime.fromisoformat(r["end_ts"])
        uid = r["user_id"]
        uid_i = int(uid)
        name = name_map.get(uid_i, f"user_id {uid_i}")
        lines.append(
            f"{s.strftime('%d.%m.%Y')} {s.strftime('%H:%M')}-{e.strftime('%H:%M')} — {name} (id {uid_i})"
        )
    return "\n".join(lines)


def _busy_intervals_for_day(target_date: date) -> str:
    """Возвращает текст со списком занятых интервалов на выбранный день."""
    start_dt = datetime.combine(target_date, dtime(0, 0))
    end_dt = datetime.combine(target_date + timedelta(days=1), dtime(0, 0))

    conn = _get_db_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT start_ts, end_ts
            FROM bookings
            WHERE NOT (end_ts <= ? OR start_ts >= ?)
            ORDER BY start_ts
            """,
            (start_dt.isoformat(), end_dt.isoformat()),
        )
        rows = cur.fetchall()
    finally:
        conn.close()

    if not rows:
        return "На эту дату пока нет занятых интервалов."

    lines = []
    for r in rows:
        s = datetime.fromisoformat(r["start_ts"])
        e = datetime.fromisoformat(r["end_ts"])
        lines.append(f"{s.strftime('%H:%M')}-{e.strftime('%H:%M')}")

    return (
        f"На дату {_format_date(target_date)} уже заняты интервалы:\n"
        + ", ".join(lines)
    )


def _free_dates_summary(days_ahead: int = 14) -> str:
    today = date.today()
    lines = []
    all_full_free = True
    for i in range(days_ahead):
        d = today + timedelta(days=i)
        used, total = _calc_usage_stats_for_period(d, d)
        if total <= 0:
            continue
        if used >= total:
            continue  # день полностью занят

        # Собираем занятые интервалы для дня
        busy = _busy_intervals_for_day(d)

        percent = (used / total * 100)
        free_percent = 100 - percent
        if free_percent >= 95:
            # почти полностью свободен
            lines.append(
                f"{_format_date(d)} — свободен полностью (занято ничего или совсем немного)"
            )
        else:
            all_full_free = False
            lines.append(
                f"{_format_date(d)} — свободно примерно {free_percent:.0f}% времени.\n"
                f"{busy}"
            )
    if not lines:
        return "В ближайшие дни свободных слотов нет."
    # Если все дни фактически полностью свободны — не засоряем отчёт деталями
    if all_full_free:
        return "В ближайшие две недели все дни полностью свободны."
    return "Свободные даты на ближайшие две недели:\n\n" + "\n\n".join(lines)


def _format_admin_balances(vk) -> str:
    """Отчёт по пользователям: сколько часов, сколько оплат, имена."""
    conn = _get_db_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT
                u.user_id,
                COALESCE(SUM(p.hours_added), 0) AS total_hours_bought,
                COALESCE(SUM(p.amount), 0) AS total_amount,
                COUNT(p.id) AS payments_count
            FROM users u
            LEFT JOIN payments p ON u.user_id = p.user_id
            GROUP BY u.user_id
            ORDER BY u.user_id
            """
        )
        rows = cur.fetchall()
    finally:
        conn.close()

    if not rows:
        return "Пока нет ни одного пользователя с абонементом."

    # Подсчитываем суммарные часы из всех броней (прошлых и будущих)
    conn = _get_db_connection()
    bookings_hours: Dict[int, float] = {}
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT user_id, start_ts, end_ts
            FROM bookings
            """
        )
        for r in cur.fetchall():
            uid = r["user_id"]
            s = datetime.fromisoformat(r["start_ts"])
            e = datetime.fromisoformat(r["end_ts"])
            dur = (e - s).total_seconds() / 3600.0
            bookings_hours[uid] = bookings_hours.get(uid, 0.0) + dur
    finally:
        conn.close()

    # Получаем имена пользователей из VK
    user_ids = [str(r["user_id"]) for r in rows]
    name_map = {}
    try:
        vk_users = vk.users.get(user_ids=",".join(user_ids))
        for u in vk_users:
            name_map[u["id"]] = f'{u.get("first_name", "")} {u.get("last_name", "")}'.strip()
    except Exception as e:
        logger.warning("Не удалось получить имена пользователей VK: %s", e)

    lines = ["Отчет по пользователям:\n"]
    for r in rows:
        uid = r["user_id"]
        hours = _get_user_hours(uid)
        bought = r["total_hours_bought"] or 0
        amount = r["total_amount"] or 0
        cnt = r["payments_count"] or 0
        # если по данным оплат куплено 0 часов и нет сумм, попробуем оценить
        # историю покупок на основе всех бронирований + текущего остатка
        if cnt == 0 and amount == 0:
            ever_hours = bookings_hours.get(uid, 0) + hours
            bought = max(bought, ever_hours)
            # грубо оцениваем денежный эквивалент: максимум часов разбиваем на
            # пакеты по 10 часов (2500 ₽) и одиночные часы (300 ₽)
            tens = bought // 10
            ones = bought % 10
            amount = tens * 2500 + ones * 300
        effective_bought = bought
        display_name = name_map.get(uid, f"user_id {uid}")
        lines.append(
            f"{display_name} (id {uid}): сейчас {_format_hours_balance(hours)}, всего куплено {effective_bought} ч. "
            f"по {cnt} оплатам на сумму {amount} ₽"
        )
    return "\n".join(lines)


def _send_admin_participants(vk, admin_user_id: int) -> None:
    """Все участники бота: имя и кликабельная ссылка https://vk.com/id… в одном сообщении."""
    uids = sorted(set(_get_all_known_user_ids()))
    if not uids:
        send_message(
            vk,
            admin_user_id,
            "В базе пока нет участников (ни записей в users, бронях, оплатах, бонусах и т.д.).",
            keyboard=_main_keyboard_for(admin_user_id),
        )
        return

    name_map: Dict[int, str] = {}
    chunk_size = 500
    for i in range(0, len(uids), chunk_size):
        chunk = uids[i : i + chunk_size]
        try:
            vk_users = vk.users.get(user_ids=",".join(str(u) for u in chunk))
            for u in vk_users:
                uid = int(u["id"])
                nm = f'{u.get("first_name", "")} {u.get("last_name", "")}'.strip()
                name_map[uid] = nm if nm else f"id {uid}"
        except Exception as e:
            logger.warning("Не удалось получить имена участников (пачка): %s", e)
            for uid in chunk:
                if uid not in name_map:
                    name_map[uid] = _get_vk_name(vk, uid)

    lines = [f"Участники бота (всего {len(uids)}). Ссылки на профили нажимаются в VK:\n"]
    for idx, uid in enumerate(uids, start=1):
        nm = name_map.get(uid, f"user_id {uid}")
        link = f"https://vk.com/id{uid}"
        lines.append(f"{idx}. {nm} — {link}")

    def _send_long(text: str, header: str = "") -> None:
        body = (header + "\n\n" + text).strip() if header else text
        max_len = 3800
        if len(body) <= max_len:
            send_message(vk, admin_user_id, body, keyboard=_main_keyboard_for(admin_user_id))
            return
        parts: list[str] = []
        cur: list[str] = []
        cur_len = 0
        for line in body.split("\n"):
            if cur_len + len(line) + 1 > max_len and cur:
                parts.append("\n".join(cur))
                cur = [line]
                cur_len = len(line)
            else:
                cur.append(line)
                cur_len += len(line) + 1
        if cur:
            parts.append("\n".join(cur))
        for p in parts:
            send_message(vk, admin_user_id, p, keyboard=_main_keyboard_for(admin_user_id))

    _send_long("\n".join(lines[1:]), lines[0].strip())


def _format_iso_ts_short(ts: str) -> str:
    """Короткое отображение даты из ISO-строки БД."""
    if not ts:
        return "—"
    try:
        dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
        if dt.tzinfo:
            dt = dt.astimezone(timezone.utc).replace(tzinfo=None)
        return dt.strftime("%d.%m.%Y %H:%M")
    except Exception:
        return ts[:19] if len(ts) > 19 else ts


def _send_admin_referral_stats(vk, admin_user_id: int) -> None:
    """
    Отчёт для администратора: бонусная система (код пригласившего),
    привязки и начисления +1 ч за первую оплату 10 ч приглашённым.
    """
    conn = _get_db_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT user_id, referrer_id, created_at
            FROM referrals
            ORDER BY referrer_id, created_at
            """
        )
        ref_rows = cur.fetchall()
        cur.execute(
            """
            SELECT referrer_id, user_id, bonus_hours, created_at
            FROM referral_payments
            ORDER BY referrer_id, created_at
            """
        )
        pay_rows = cur.fetchall()
        cur.execute("SELECT COALESCE(SUM(bonus_hours), 0) FROM referral_payments")
        total_bonus = int(cur.fetchone()[0] or 0)
    finally:
        conn.close()

    bonus_by_pair: Dict[tuple[int, int], sqlite3.Row] = {}
    for row in pay_rows:
        bonus_by_pair[(int(row["referrer_id"]), int(row["user_id"]))] = row

    header = (
        "📊 Бонусная система — отчёт для администратора\n\n"
        "Правило простое: если приглашённый впервые купил абонемент на 10 ч, пригласивший получает +1 ч. "
        "Приглашённый до покупки ввёл код друга одной строкой: реф КОД. "
        "На первую покупку 10 ч у приглашённого скидка 100 ₽.\n\n"
        f"Всего начислено бонусных часов: {total_bonus}\n"
        f"Вводов кода (привязок): {len(ref_rows)}\n"
        f"Начислений бонуса (первая оплата 10 ч): {len(pay_rows)}\n"
    )

    uids: set[int] = set()
    for r in ref_rows:
        uids.add(int(r["user_id"]))
        uids.add(int(r["referrer_id"]))
    for r in pay_rows:
        uids.add(int(r["user_id"]))
        uids.add(int(r["referrer_id"]))

    name_map: Dict[int, str] = {}
    uids_sorted = sorted(uids)
    chunk_size = 500
    for i in range(0, len(uids_sorted), chunk_size):
        chunk = uids_sorted[i : i + chunk_size]
        try:
            vk_users = vk.users.get(user_ids=",".join(str(u) for u in chunk))
            for u in vk_users:
                uid = int(u["id"])
                nm = f'{u.get("first_name", "")} {u.get("last_name", "")}'.strip()
                name_map[uid] = nm if nm else f"id {uid}"
        except Exception as e:
            logger.warning("Не удалось получить имена для отчёта бонусов: %s", e)
            for uid in chunk:
                if uid not in name_map:
                    name_map[uid] = _get_vk_name(vk, uid)

    lines: list[str] = [header]

    if not ref_rows and not pay_rows:
        lines.append("\nВ базе пока нет записей по бонусной системе.")
        body = "".join(lines)
        send_message(vk, admin_user_id, body, keyboard=_admin_keyboard())
        return

    by_referrer: dict[int, list[sqlite3.Row]] = defaultdict(list)
    for r in ref_rows:
        by_referrer[int(r["referrer_id"])].append(r)

    lines.append("\n— Кто чей код ввёл —\n")
    for ref_id in sorted(by_referrer.keys()):
        ref_name = name_map.get(ref_id, f"id {ref_id}")
        lines.append(
            f"\n• Пригласил (его код вводили): {ref_name} (id {ref_id}) — https://vk.com/id{ref_id}\n"
        )
        for idx, row in enumerate(by_referrer[ref_id], start=1):
            uid = int(row["user_id"])
            rn = name_map.get(uid, f"id {uid}")
            linked = _format_iso_ts_short(str(row["created_at"]))
            key = (ref_id, uid)
            if key in bonus_by_pair:
                b = bonus_by_pair[key]
                bh = int(b["bonus_hours"])
                pay_when = _format_iso_ts_short(str(b["created_at"]))
                lines.append(
                    f"  {idx}) {rn} (id {uid}) — привязка {linked}; "
                    f"бонус пригласившему +{bh} ч (начислено {pay_when})\n"
                )
            else:
                lines.append(
                    f"  {idx}) {rn} (id {uid}) — привязка {linked}; "
                    f"бонус за первую оплату 10 ч ещё не начислялся\n"
                )

    # Редкий случай: есть начисление в referral_payments без строки в referrals
    ref_pairs = {(int(r["referrer_id"]), int(r["user_id"])) for r in ref_rows}
    orphan_pays = [
        r for r in pay_rows if (int(r["referrer_id"]), int(r["user_id"])) not in ref_pairs
    ]
    if orphan_pays:
        lines.append("\n— Начисления без найденной привязки (проверьте БД) —\n")
        for row in orphan_pays:
            rid = int(row["referrer_id"])
            uid = int(row["user_id"])
            rn = name_map.get(rid, f"id {rid}")
            un = name_map.get(uid, f"id {uid}")
            lines.append(
                f"• Пригласивший {rn} ({rid}) ← ввёл код {un} ({uid}): "
                f"+{int(row['bonus_hours'])} ч, {_format_iso_ts_short(str(row['created_at']))}\n"
            )

    body = "".join(lines)
    max_len = 3800
    if len(body) <= max_len:
        send_message(vk, admin_user_id, body, keyboard=_admin_keyboard())
        return
    parts: list[str] = []
    cur_part: list[str] = []
    cur_len = 0
    for line in body.split("\n"):
        if cur_len + len(line) + 1 > max_len and cur_part:
            parts.append("\n".join(cur_part))
            cur_part = [line]
            cur_len = len(line)
        else:
            cur_part.append(line)
            cur_len += len(line) + 1
    if cur_part:
        parts.append("\n".join(cur_part))
    for p in parts:
        send_message(vk, admin_user_id, p, keyboard=_admin_keyboard())


def _get_vk_name(vk, user_id: int) -> str:
    """Возвращает 'Имя Фамилия' по user_id или 'user_id N' при ошибке."""
    try:
        users = vk.users.get(user_ids=str(user_id))
        if users:
            u = users[0]
            name = f'{u.get("first_name", "")} {u.get("last_name", "")}'.strip()
            if name:
                return name
    except Exception as e:
        logger.warning("Не удалось получить имя VK для %s: %s", user_id, e)
    return f"user_id {user_id}"


def _hours_almost_equal(a: float, b: float) -> bool:
    return abs(float(a) - float(b)) < 1e-6


def _vk_load_full_message(vk, event) -> Optional[dict]:
    """
    Полное тело сообщения (в т.ч. attachments). Long Poll без preload часто не заполняет вложения —
    тогда один запрос messages.getById.
    """
    md = getattr(event, "message_data", None)
    if md:
        return md
    mid = getattr(event, "message_id", None)
    if not mid:
        return None
    for attempt in range(3):
        try:
            res = vk.messages.getById(message_ids=mid)
            items = res.get("items") or []
            if items:
                return items[0]
        except Exception as e:
            logger.warning(
                "messages.getById(%s) attempt %s failed: %s", mid, attempt + 1, e
            )
        if attempt < 2:
            time.sleep(0.35 * (attempt + 1))
    return None


def _vk_message_dict_has_payment_media(msg: Optional[dict]) -> bool:
    """Фото/видео/документ как подтверждение оплаты; учитываем ответы и пересланные сообщения."""
    if not msg or not isinstance(msg, dict):
        return False
    for a in msg.get("attachments") or []:
        t = a.get("type")
        if t in ("photo", "video"):
            return True
        if t == "doc":
            return True
    rep = msg.get("reply_message")
    if rep and _vk_message_dict_has_payment_media(rep):
        return True
    for fm in msg.get("fwd_messages") or []:
        if _vk_message_dict_has_payment_media(fm):
            return True
    return False


def _event_has_payment_attachment(vk, event) -> bool:
    if _vk_message_dict_has_payment_media(getattr(event, "message_data", None)):
        return True
    full = _vk_load_full_message(vk, event)
    return _vk_message_dict_has_payment_media(full)


def _payment_amount_for_hours(hours: float, user_id: int) -> int:
    """Ожидаемая сумма перевода в рублях (1 ч = 300, пакет 10 ч со скидкой как раньше)."""
    if _hours_almost_equal(hours, 1.0):
        return 300
    if _hours_almost_equal(hours, 1.5):
        return 450
    if _hours_almost_equal(hours, 2.0):
        return 600
    if _hours_almost_equal(hours, 2.5):
        return 750
    if _hours_almost_equal(hours, 3.0):
        return 900
    if _hours_almost_equal(hours, 10.0):
        return 2400 if _has_referrer_discount(user_id) else 2500
    return 0


def _admin_apply_hours_delta(
    vk,
    admin_user_id: int,
    target_id: int,
    delta: float,
    *,
    use_admin_keyboard: bool = False,
) -> None:
    """
    Ручное изменение часов на абонементе (как текст «часы USER ДЕЛЬТА»):
    новое значение = max(0, текущие + delta). Дельта может быть дробной (например 1.5).
    """
    kb = _admin_keyboard() if use_admin_keyboard else _main_keyboard_for(admin_user_id)
    if abs(float(delta)) < 1e-9:
        send_message(
            vk,
            admin_user_id,
            "Количество часов равно нулю, ничего не изменено.",
            keyboard=kb,
        )
        return
    current = _get_user_hours(target_id)
    new_value = max(0.0, current + float(delta))
    _set_user_hours(target_id, new_value)
    _clear_admin_chat_history(target_id)
    display_name = _get_vk_name(vk, target_id)
    send_message(
        vk,
        admin_user_id,
        f"Часы пользователя {display_name} (id {target_id}) изменены с {_format_hours_balance(current)} на {_format_hours_balance(new_value)}.",
        keyboard=kb,
    )
    try:
        if delta > 0:
            send_message(
                vk,
                target_id,
                f"Ваш абонемент был обновлен администратором. "
                f"Добавлено {_format_hours_balance(float(delta))} Теперь на вашем абонементе: {_format_hours_balance(new_value)}.",
            )
        else:
            send_message(
                vk,
                target_id,
                f"Ваш абонемент был обновлен администратором. "
                f"Списано {_format_hours_balance(float(-delta))} Теперь на вашем абонементе: {_format_hours_balance(new_value)}.",
            )
    except Exception:
        pass


def _deepseek_client() -> OpenAI:
    return OpenAI(
        api_key=DEEPSEEK_API_KEY,
        base_url=DEEPSEEK_BASE_URL,
        timeout=DEEPSEEK_TIMEOUT_SECONDS,
    )


def _is_deepseek_transient_error(exc: BaseException) -> bool:
    """Сеть, таймаут, rate limit или 5xx на стороне провайдера — имеет смысл одна повторная попытка."""
    if isinstance(exc, (APIConnectionError, APITimeoutError, RateLimitError)):
        return True
    if isinstance(exc, APIStatusError):
        code = getattr(exc, "status_code", None)
        try:
            return code is not None and int(code) >= 500
        except (TypeError, ValueError):
            return False
    return False


def _ask_administrator(question: str, user_id: int) -> Optional[str]:
    """Синхронный запрос к DeepSeek как к администратору."""
    if not DEEPSEEK_API_KEY:
        return None
    current_hours = _get_user_hours(user_id)
    history = ADMIN_HISTORY.setdefault(user_id, [])
    try:
        client = _deepseek_client()
        messages = [
            {
                "role": "system",
                "content": (
                    "Ты — Снежана, женщина-психолог и администратор кабинета почасовой аренды. "
                    "С тобой можно общаться на любые темы, как с обычным человеком: про жизнь, вопросы, мысли. "
                    "Отвечай естественно, по‑человечески, без официальщины и без эмодзи. "
                    "Всегда говори строго в женском роде. "
                    "Не используй слово 'понял' и не пиши в мужском роде. "
                    "Если нужно подтвердить понимание — пиши женские формы: 'я поняла', 'поняла вас', 'ок, поняла'. "
                    "Не начинай каждое сообщение с приветствия, просто отвечай по сути на то, что написал человек. "
                    "Никакого форматирования Markdown, не используй символ * и другие спецсимволы для выделения текста. "
                    "Если ответ получается длинным, разбивай на 2-4 абзаца, между абзацами пустая строка. "
                    "Если человек задаёт вопрос не про кабинет и не про бронирования — просто поговори с ним по‑человечески "
                    "и не навязывай аренду.\n\n"
                    "Если вопрос связан с арендой кабинета или бронированиями, используй правила ниже.\n\n"
                    "Как устроена оплата в этом боте (говори так и не выдумывай другое): "
                    "после выбора пакета в меню «Купить абонемент» человек переводит деньги и присылает сюда "
                    "скриншот или файл перевода одним сообщением — бот сам зачисляет часы на абонемент через несколько секунд. "
                    "Копия сообщения со скрином дополнительно уходит администратору для контроля, "
                    "но не говори, что часы появятся только после ручной проверки или что бот «не видит оплату» без твоего участия — это неверно.\n\n"
                    "Актуальный остаток часов в последнем блоке сообщения пользователя и во втором служебном системном сообщении ниже "
                    "всегда важнее любых предыдущих реплик в истории чата: если там больше нуля, нельзя писать, что на абонементе ноль часов.\n\n"
                    "Если часов достаточно для запрошенной брони, просто напиши, что всё ок и можно бронировать через кнопки меню, "
                    "но НЕ пиши сам, сколько часов спишется и сколько останется — этим занимается бот.\n"
                    "Если по актуальным данным часов нет или не хватает, объясни, что сначала нужно пополнить абонемент через кнопку "
                    "«Купить абонемент» и предложи варианты: абонемент на 10 часов за 2500 рублей или час за 300 рублей.\n\n"
                    "Если человек пишет, что уже оплатил или прислал скрин, а в актуальных данных часов всё ещё 0 — "
                    "не обещай ручного зачисления; скажи открыть «Мой абонемент» в меню, при необходимости выбрать тариф снова "
                    "и прислать скрин перевода фото или файлом одним сообщением.\n\n"
                    "ОТДЕЛЬНОЕ ВАЖНОЕ ПРАВИЛО ПРО ОПЛАТУ:\n"
                    "1) Если в сообщении явно говорится «на 1 час», «один час» или «продлить на 1 час» и НЕ указаны дата/время, "
                    "это значит, что человек хочет просто ДОКУПИТЬ 1 час, а не бронировать слот. "
                    "В этом случае НЕ предлагай бронирование и НЕ отправляй к кнопкам. "
                    "Твой ответ должен быть только про оплату, по смыслу: "
                    "«Тогда продлим на 1 час. Отправьте, пожалуйста, 300 рублей по номеру 89124566686 "
                    "и пришлите сюда скриншот перевода».\n"
                    "2) Если в сообщении явно говорится «на 10 часов», «абонемент на 10 часов» и НЕ указаны дата/время, "
                    "это значит, что человек хочет купить абонемент на 10 часов. "
                    "В этом случае тоже НЕ предлагай бронирование и НЕ отправляй к кнопкам. "
                    "Ответ должен быть только про оплату, по смыслу: "
                    "«Тогда оформим абонемент на 10 часов за 2500 рублей. Отправьте, пожалуйста, 2500 рублей "
                    "по номеру 89124566686 и пришлите сюда скриншот перевода».\n"
                    "3) В обоих случаях обязательно попроси прислать скриншот перевода в этот чат. "
                    "Сам бронь не создаёшь, не списываешь часы и не выдумываешь остаток — точный баланс показывает бот в меню «Мой абонемент»."
                ),
            }
        ]
        # добавляем историю диалога
        messages.extend(history)
        bal_line = _format_hours_balance(float(current_hours))
        messages.append(
            {
                "role": "system",
                "content": (
                    f"Служебно (приоритет над историей выше): пользователь id {user_id}, "
                    f"остаток на абонементе сейчас: {bal_line} "
                    "Если остаток больше нуля, не утверждай, что часов нет и не предлагай оплату как будто баланс пустой."
                ),
            }
        )
        # текущее сообщение пользователя
        user_block = (
            f"Пользователь VK с id {user_id} пишет:\n\n{question}\n\n"
            f"Актуально сейчас на абонементе этого пользователя: {bal_line}"
        )
        messages.append(
            {
                "role": "user",
                "content": user_block,
            }
        )
        resp = None
        last_exc: Optional[BaseException] = None
        for attempt in range(2):
            try:
                resp = client.chat.completions.create(
                    model=DEEPSEEK_MODEL,
                    messages=messages,
                    max_tokens=600,
                )
                break
            except Exception as e:
                last_exc = e
                if attempt == 0 and _is_deepseek_transient_error(e):
                    logger.warning("DeepSeek admin transient (VK), retry once: %s", e)
                    time.sleep(1.5)
                    continue
                raise
        if resp is None:
            raise last_exc  # type: ignore[misc]

        text = (resp.choices[0].message.content or "").strip()
        if not text:
            return None
        # обновляем историю: добавляем последнее сообщение пользователя и ответ ассистента,
        # ограничиваем историю, чтобы не разрасталась
        history.append({"role": "user", "content": user_block})
        history.append({"role": "assistant", "content": text})
        if len(history) > 20:
            # храним последние 20 сообщений (10 реплик туда‑сюда)
            del history[:-20]
        return text
    except Exception as e:
        logger.warning("DeepSeek admin error (VK): %s", e)
        return None


def send_message(vk, user_id: int, text: str, keyboard: Optional[VkKeyboard] = None) -> None:
    try:
        vk.messages.send(
            user_id=user_id,
            random_id=int(time.time() * 1000),
            message=text,
            keyboard=keyboard.get_keyboard() if keyboard is not None else None,
        )
    except vk_api.exceptions.ApiError as e:
        # VK [901]: нельзя отправлять сообщения этому пользователю (нет прав/не начинал чат).
        # Не считаем это фатальной ошибкой: пропускаем отправку и продолжаем работу.
        if "901" in str(e):
            logger.info("VK send_message skipped (no permission) for user_id=%s: %s", user_id, e)
            return
        raise


def _is_user_welcomed(user_id: int) -> bool:
    """
    True, если сегодня (в UTC) уже отправляли приветствие пользователю.
    """
    conn = _get_db_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            "SELECT welcomed_at FROM user_welcome WHERE user_id = ?",
            (user_id,),
        )
        row = cur.fetchone()
        if row is None:
            return False
        last_dt = datetime.fromisoformat(row["welcomed_at"])
        return last_dt.date() == datetime.now(timezone.utc).date()
    finally:
        conn.close()


def _mark_user_welcomed_if_absent(user_id: int) -> bool:
    """
    Возвращает True, если сегодня ещё не отмечали приветствие,
    и мы помечаем пользователя как "приветствованный сегодня".
    Используем upsert-логику, чтобы не было повторной генерации.
    """
    conn = _get_db_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            "SELECT welcomed_at FROM user_welcome WHERE user_id = ?",
            (user_id,),
        )
        row = cur.fetchone()
        today_utc = datetime.now(timezone.utc).date()
        if row is not None:
            last_dt = datetime.fromisoformat(row["welcomed_at"])
            if last_dt.date() == today_utc:
                return False

        if row is None:
            cur.execute(
                """
                INSERT INTO user_welcome (user_id, welcomed_at)
                VALUES (?, ?)
                """,
                (user_id, datetime.now(timezone.utc).isoformat()),
            )
        else:
            cur.execute(
                """
                UPDATE user_welcome SET welcomed_at = ?
                WHERE user_id = ?
                """,
                (datetime.now(timezone.utc).isoformat(), user_id),
            )
        conn.commit()
        return True
    finally:
        conn.close()


def _generate_snezana_welcome(vk, user_id: int) -> Optional[str]:
    if not DEEPSEEK_API_KEY:
        return None

    try:
        name = _get_vk_name(vk, user_id)
    except Exception:
        name = f"user_id {user_id}"

    try:
        client = _deepseek_client()
        messages = [
            {
                "role": "system",
                "content": (
                    "Ты — Снежана, психолог в первую очередь, а ещё сдаёшь свой кабинет в почасовую аренду. "
                    "Отвечай от первого лица, как живая реальная женщина. "
                    "Тон: дружелюбный, тёплый, немного игривый, шутки уместные и мягкие. "
                    "Без эмодзи. Максимально как человек. "
                    "Без Markdown: не используй символы '*' и другие спецсимволы для выделения. "
                    "Факты про твою личную жизнь (муж, дочка, вес, детали быта) используй только как фоновый контекст для тона в диалоге. "
                    "Не раскрывай и не перечисляй эти подробности в приветственном сообщении. "
                    "Текст должен быть именно приветственным сообщением при первом заходе в чат. "
                    "Обязательно упомяни, что: "
                    "1) В меню есть кнопки: '📅 забронировать кабинет', '📖 мои бронирования', '🎫 мой абонемент', "
                    "'📆 свободные даты', '❌ отменить бронирование', '💳 купить абонемент', '👥 бонусная система' "
                    "и как ими пользоваться очень кратко (1-2 фразы на пункт). "
                    "2) Бонусная система объясни максимально простыми словами: если человек привёл друга, друг до покупки один раз вводит код строкой «реф КОД»; "
                    "если друг впервые покупает абонемент на 10 часов, пригласивший получает бонус 1 час. Код — это VK ID или никнейм VK. "
                    "У приглашённого на первую покупку 10 ч скидка 100 рублей. "
                    "Упомяни, что всё фиксируется в системе. "
                    "3) Правило отмены брони: отменить можно в любой момент. При отмене за 24 ч и более до начала часы возвращаются на абонемент; при отмене менее чем за 24 ч часы не возвращаются. "
                    "4) Я (Снежана) прошу рекомендовать кабинет друзьям, если понравится. "
                    "5) Кнопки меню — основной способ работы с ботом (бронь, абонемент, отмена, бонусы). "
                    "В конце мягко предложи выбрать действие в меню или написать 'хочу забронировать' / 'нужно продлить'. "
                    "Формат: разбей текст на абзацы. 1-2 предложения в абзаце, между абзацами пустая строка. "
                    "Всегда говори в женском роде."
                ),
            },
            {
                "role": "user",
                "content": (
                    f"Сформируй приветственное сообщение для пользователя {name} (vk id {user_id})."
                ),
            },
        ]

        resp = None
        last_exc: Optional[BaseException] = None
        for attempt in range(2):
            try:
                resp = client.chat.completions.create(
                    model=DEEPSEEK_MODEL,
                    messages=messages,
                    max_tokens=650,
                )
                break
            except Exception as e:
                last_exc = e
                if attempt == 0 and _is_deepseek_transient_error(e):
                    logger.warning("DeepSeek welcome transient (VK), retry once: %s", e)
                    time.sleep(1.5)
                    continue
                raise
        if resp is None:
            raise last_exc  # type: ignore[misc]

        text = (resp.choices[0].message.content or "").strip()
        return text or None
    except Exception:
        return None


def handle_start(vk, user_id: int) -> None:
    _clear_awaiting_payment(user_id)
    _clear_admin_chat_history(user_id)
    STATES[user_id] = UserState(mode="idle")
    first_time = _mark_user_welcomed_if_absent(user_id)

    if first_time:
        text = (
            "Здравствуйте! Я Снежана, в первую очередь я психолог, а еще сдаю свой прекрасный кабинет хорошим людям в почасовую аренду.\n\n"
            "В меню ниже можно выбрать нужное: забронировать кабинет, посмотреть мои бронирования, "
            "проверить абонемент и свободные даты, а также отменить бронирование.\n\n"
            "Бонусная система простая.\n\n"
            "Приведи друга. Пусть он до покупки один раз напишет боту: реф ТВОЙ_КОД "
            "(код можно передать как VK ID или никнейм VK; ниже я отдельно пришлю удобную строку «реф:…»).\n\n"
            "Если друг впервые купит абонемент на 10 часов, ты получишь бонус: 1 час.\n"
            "Другу на эту первую покупку 10 ч — скидка 100 ₽.\n\n"
            "Отменить бронь можно в любой момент: за 24 ч и более до начала часы возвращаются, менее чем за 24 ч — не возвращаются. Если вам понравится — "
            "порекомендуйте кабинет друзьям.\n\n"
            "Выберите действие в меню ниже или напишите, например: «хочу забронировать»."
        )
        send_message(vk, user_id, text, keyboard=_main_keyboard_for(user_id))
        return

    # Повторный заход: коротко и без генерации
    send_message(
        vk,
        user_id,
        "Выберите действие в меню ниже:",
        keyboard=_main_keyboard_for(user_id),
    )


def handle_main_menu(vk, user_id: int, text_raw: str) -> None:
    state = STATES.setdefault(user_id, UserState())
    state.mode = "idle"
    raw = (text_raw or "").strip()
    text = raw.lower()

    # Админ: панель кнопок вместо текстовых команд
    if user_id in ADMIN_VK_IDS and text == "⚙️ команды":
        send_message(
            vk,
            user_id,
            "Панель администратора — кнопки ниже.\n"
            "Текстом по-прежнему: «удалить запись ID», «часы USER кол-во», "
            "«отчет» (период в ответе), «записи …» с датами.\n"
            "Кнопка «➕ Добавить часы» — пошагово: клиент и число часов.",
            keyboard=_admin_keyboard(),
        )
        return

    if user_id in ADMIN_VK_IDS and text == "⬅ главное меню":
        send_message(
            vk,
            user_id,
            "Выберите действие в меню ниже:",
            keyboard=_main_keyboard_for(user_id),
        )
        return

    if text == "📅 забронировать кабинет" or text == "📅 забронировать кабинет".lower():
        hours = _get_user_hours(user_id)
        if hours < 0.001:
            send_message(
                vk,
                user_id,
                "У вас нет часов на абонементе.\n\n"
                "На сколько часов хотите пополнить (1, 1,5, 2, 2,5, 3 или 10 ч)?\n"
                "Нажмите кнопку «Купить абонемент» внизу и выберите нужный вариант.",
                keyboard=_main_keyboard_for(user_id),
            )
            return
        state.mode = "choosing_day"
        send_message(
            vk,
            user_id,
            "Введите день бронирования.\n\n"
            "Примеры:\n"
            "• сегодня\n"
            "• завтра\n"
            "• 18.03.2026",
            keyboard=_back_keyboard(),
        )
        return

    if text == "📖 мои бронирования" or text == "мои бронирования":
        msg = _format_user_bookings(user_id)
        send_message(vk, user_id, msg, keyboard=_main_keyboard_for(user_id))
        return

    # Админ: отмена брони клиента (не своей)
    if user_id in ADMIN_VK_IDS and text == "отменить бронирование клиента":
        state.mode = "admin_cancel_ask_client"
        state.cancel_for_user_id = None
        send_message(
            vk,
            user_id,
            "Отмена бронирования клиента.\n\n"
            "Отправьте пользователя VK (ID, никнейм, @ник или ссылку), "
            "чьё бронирование нужно отменить.\n"
            "Примеры:\n"
            "21476079\n"
            "durov\n"
            "@durov\n"
            "https://vk.com/id21476079\n"
            "https://vk.com/durov",
            keyboard=_back_keyboard(),
        )
        return

    if user_id in ADMIN_VK_IDS and text == "➕ добавить часы":
        state.mode = "admin_add_hours_ask_client"
        state.admin_hours_target_id = None
        send_message(
            vk,
            user_id,
            "Добавить часы на абонемент пользователю.\n\n"
            "Шаг 1. Отправьте пользователя VK: ID, никнейм, @ник или ссылку.\n"
            "Примеры: 21476079, durov, @durov, https://vk.com/durov\n\n"
            "Шаг 2: бот попросит указать, сколько часов добавить "
            "(дробь через точку или запятую, например 1,5; можно отрицательное число).",
            keyboard=_back_keyboard(),
        )
        return

    if user_id in ADMIN_VK_IDS and text == "➖ удалить часы":
        state.mode = "admin_remove_hours_ask_client"
        state.admin_hours_target_id = None
        send_message(
            vk,
            user_id,
            "Удалить часы с абонемента пользователя.\n\n"
            "Шаг 1. Отправьте пользователя VK: ID, никнейм, @ник или ссылку.\n"
            "Примеры: 21476079, durov, @durov, https://vk.com/durov\n\n"
            "Шаг 2: бот попросит указать, сколько часов списать "
            "(только положительное число, можно дробное: 1,5).",
            keyboard=_back_keyboard(),
        )
        return

    if text == "❌ отменить бронирование" or text == "отменить бронирование":
        state.cancel_for_user_id = None
        future = _get_future_bookings(user_id)
        if not future:
            send_message(
                vk,
                user_id,
                "У вас нет будущих бронирований.\n\n"
                "Напоминаем: при отмене за 24 ч и более до начала часы возвращаются; менее чем за 24 ч — не возвращаются.",
                keyboard=_main_keyboard_for(user_id),
            )
            return
        state.mode = "cancel_select"
        lines = [
            "Ваши будущие бронирования.\n"
            "При отмене за 24 ч и более до начала часы возвращаются на абонемент; менее чем за 24 ч — не возвращаются."
        ]
        for idx, row in enumerate(future, start=1):
            s = datetime.fromisoformat(row["start_ts"])
            e = datetime.fromisoformat(row["end_ts"])
            lines.append(
                f"{idx}. {s.strftime('%d.%m.%Y')} {s.strftime('%H:%M')}-{e.strftime('%H:%M')}"
            )
        lines.append("\nОтправьте номер бронирования, которое хотите отменить.")
        send_message(
            vk,
            user_id,
            "\n".join(lines),
            keyboard=_back_keyboard(),
        )
        return

    if text == "📆 свободные даты" or text == "свободные даты":
        summary = _free_dates_summary(14)
        send_message(vk, user_id, summary, keyboard=_main_keyboard_for(user_id))
        return

    if text == "🎫 мой абонемент" or text == "мой абонемент":
        hours = _get_user_hours(user_id)
        send_message(
            vk,
            user_id,
            f"На вашем абонементе сейчас: {_format_hours_balance(hours)}\n\n"
            "Если хотите продлить, нажмите внизу кнопку «Купить абонемент» "
            "и выберите нужный вариант.",
            keyboard=_main_keyboard_for(user_id),
        )
        return

    if text == "💳 купить абонемент" or text == "купить абонемент":
        state.mode = "buy_choice"
        send_message(
            vk,
            user_id,
            "Купить абонемент.\n\n"
            "Если тебе дали код друга, до покупки один раз напиши боту:\n"
            "реф КОД\n"
            "например: реф 21476079  или  реф durov\n\n"
            "Потом выбери пакет на кнопках:\n\n"
            "1 ч — 300 ₽, 1,5 ч — 450 ₽, 2 ч — 600 ₽, 2,5 ч — 750 ₽, 3 ч — 900 ₽,\n"
            "10 ч — 2500 ₽ (если ввёл код до первой покупки 10 ч — 2400 ₽).",
            keyboard=_buy_keyboard(),
        )
        return

    if text in _BONUS_MENU_TEXTS_LOWER:
        send_message(
            vk,
            user_id,
            "Бонусная система — всё очень просто.\n\n"
            "Ты привёл друга.\n"
            "Друг до покупки один раз пишет боту твой код вот так:\n"
            "реф ТВОЙ_КОД\n\n"
            "Код можно передать как VK ID или никнейм VK (я пришлю одной строкой вариант с ID, чтобы было проще скопировать).\n\n"
            "Если друг впервые купит абонемент на 10 часов, ты получишь бонус: 1 час.\n\n"
            "Другу на эту первую покупку 10 ч скидка 100 ₽ (платит 2400 ₽, а не 2500 ₽).\n\n"
            "Перешли другу сообщение с кодом ниже или продиктуй ему номер.",
            keyboard=_main_keyboard_for(user_id),
        )
        # Отдельное короткое сообщение только с кодом, чтобы его было удобно пересылать
        send_message(
            vk,
            user_id,
            f"реф:{user_id}",
        )
        return

    if text == "🤖 тоже хочу бота" or text.lower() == "тоже хочу бота":
        send_message(
            vk,
            user_id,
            "Если вам нужен свой бот — напишите напрямую:\n"
            f"https://vk.com/id{WANT_BOT_RECIPIENT_ID}",
            keyboard=_main_keyboard_for(user_id),
        )
        return

    if text in ("💬 вопрос снежане", "вопрос снежане"):
        send_message(
            vk,
            user_id,
            "Напишите Снежане в личные сообщения:\n"
            "https://vk.com/id164817756",
            keyboard=_main_keyboard_for(user_id),
        )
        return

    if text == "пополнить":
        total = _add_10_hours(user_id)
        send_message(
            vk,
            user_id,
            f"Абонемент пополнен на 10 часов.\nТеперь на вашем абонементе: {_format_hours_balance(float(total))}",
            keyboard=_main_keyboard_for(user_id),
        )
        return

    # Админ: «отчет сегодня» / «отчет этот месяц» / «отчет с … по …» — сразу отчёт без диалога
    if user_id in ADMIN_VK_IDS:
        tl = text
        for prefix in ("отчет ", "отчёт "):
            if tl.startswith(prefix):
                rest = tl[len(prefix) :].strip()
                if rest:
                    parsed = _parse_period_text(rest)
                    if parsed:
                        _send_admin_period_report(vk, user_id, parsed[0], parsed[1])
                        return

    # Если админ пишет слово «отчет» / «отчёт» / «статистика» — запускаем диалог отчета.
    if user_id in ADMIN_VK_IDS and any(
        kw in text for kw in ("отчет", "отчёт", "статист")
    ):
        state.mode = "admin_report_range"
        send_message(
            vk,
            user_id,
            "По какому периоду сделать отчет по бронированиям?\n\n"
            "Примеры:\n"
            "• сегодня\n"
            "• этот месяц\n"
            "• с 01.04.2026 по 15.04.2026",
            keyboard=_back_keyboard(),
        )
        return

    # Отчет по пользователям и оплатам — по ключевым словам «баланс» / «оплаты»
    if user_id in ADMIN_VK_IDS and any(kw in text for kw in ("баланс", "оплат")):
        report = _format_admin_balances(vk)
        send_message(vk, user_id, report, keyboard=_main_keyboard_for(user_id))
        return

    if user_id in ADMIN_VK_IDS and text in (
        "синк календаря",
        "синхронизировать календарь",
        "sync calendar",
    ):
        if not _calendar_sync_enabled():
            send_message(
                vk,
                user_id,
                "Синк календаря в GitHub не настроен.\n"
                "Нужны переменные: CALENDAR_GH_OWNER, CALENDAR_GH_REPO, CALENDAR_GH_TOKEN.",
                keyboard=_main_keyboard_for(user_id),
            )
            return
        _sync_calendar_json_to_github("admin_manual_sync")
        send_message(
            vk,
            user_id,
            "Запустила ручную синхронизацию календаря в GitHub.",
            keyboard=_main_keyboard_for(user_id),
        )
        return

    # Список админ-команд (текстом) + панель кнопок
    if user_id in ADMIN_VK_IDS and any(kw in text for kw in ("команды", "/admin")):
        send_message(
            vk,
            user_id,
            "Административные команды:\n\n"
            "• баланс — отчет по пользователям (часы, оплаты)\n"
            "• отчет ... — отчет по бронированиям за период (например: отчет этот месяц)\n"
            "• записи ПЕРИОД — показать все записи за период, с именами и id\n"
            "  (пример: записи сегодня | записи этот месяц | записи с 01.04.2026 по 15.04.2026)\n"
            "• удалить запись BOOKING_ID — удалить запись клиента и вернуть часы\n"
            "  (пример: удалить запись 123)\n"
            "• часы USER КОЛ-ВО — вручную изменить часы пользователя\n"
            "  (пример: часы 21476079 5  или  часы durov 1,5)\n"
            "• ➖ удалить часы — пошагово списать часы у клиента\n"
            "• участники — все участники бота (имена и кликабельные ссылки vk.com/id…)\n"
            "• 📊 бонусы по кодам — кто чей код ввёл, начисленные бонусы\n"
            "• синк календаря — ручной пуш data/bookings.json в GitHub\n"
            "• ➕ добавить часы — ID, никнейм, @ник или ссылка, затем сколько часов добавить (дробь 1,5; или отрицательное — списать)\n"
            "• отменить бронирование клиента — ID, никнейм, @ник или ссылка, "
            "затем номер записи (за 24 ч и более часы возвращаются клиенту, менее 24 ч — нет)\n\n"
            "Остальные функции (бронь, свободные даты, покупка абонемента) доступны через кнопки меню.\n\n"
            "Ниже — те же действия кнопками; «⬅ Главное меню» вернёт обычное меню.",
            keyboard=_admin_keyboard(),
        )
        return

    # Админ: показать записи за период
    if user_id in ADMIN_VK_IDS and text.startswith("записи"):
        period_text = text[len("записи") :].strip()
        if not period_text:
            send_message(
                vk,
                user_id,
                "Укажите период.\n\nПримеры:\n• записи сегодня\n• записи этот месяц\n• записи с 01.04.2026 по 15.04.2026",
                keyboard=_main_keyboard_for(user_id),
            )
            return
        parsed = _parse_period_text(period_text)
        if parsed is None:
            send_message(
                vk,
                user_id,
                "Не удалось распознать период.\n\nПримеры:\n• записи сегодня\n• записи этот месяц\n• записи с 01.04.2026 по 15.04.2026",
                keyboard=_main_keyboard_for(user_id),
            )
            return
        start_date, end_date = parsed
        rows = _get_bookings_for_period(start_date, end_date)
        if not rows:
            send_message(
                vk,
                user_id,
                f"За период {start_date.strftime('%d.%m.%Y')} — {end_date.strftime('%d.%m.%Y')} записей нет.",
                keyboard=_main_keyboard_for(user_id),
            )
            return
        # Имена пользователей пачкой
        uids = sorted({int(r["user_id"]) for r in rows})
        name_map: Dict[int, str] = {}
        try:
            vk_users = vk.users.get(user_ids=",".join(str(u) for u in uids))
            for u in vk_users:
                uid = int(u["id"])
                name = f'{u.get("first_name", "")} {u.get("last_name", "")}'.strip()
                name_map[uid] = name if name else f"user_id {uid}"
        except Exception as e:
            logger.warning("Не удалось получить имена в команде 'записи': %s", e)
            for uid in uids:
                name_map[uid] = _get_vk_name(vk, uid)

        lines = [
            f"Записи за период {start_date.strftime('%d.%m.%Y')} — {end_date.strftime('%d.%m.%Y')}:\n"
        ]
        for b in rows:
            s = datetime.fromisoformat(b["start_ts"])
            e = datetime.fromisoformat(b["end_ts"])
            uid = int(b["user_id"])
            name = name_map.get(uid, "Без имени")
            lines.append(
                f"• {s.strftime('%d.%m.%Y')} {s.strftime('%H:%M')}-{e.strftime('%H:%M')} — "
                f"{name} (id {uid}) [запись id {b['id']}]"
            )
        send_message(vk, user_id, "\n".join(lines), keyboard=_main_keyboard_for(user_id))
        return

    # Админ: удалить запись клиента по id
    if user_id in ADMIN_VK_IDS and (text.startswith("удалить запись ") or text.startswith("удалить ")):
        parts = text.split()
        # ожидаем либо "удалить запись 123", либо "удалить 123"
        booking_id_str = parts[-1] if parts else ""
        try:
            booking_id = int(booking_id_str)
        except ValueError:
            send_message(
                vk,
                user_id,
                "Формат команды:\nудалить запись BOOKING_ID\nнапример: удалить запись 123",
                keyboard=_main_keyboard_for(user_id),
            )
            return
        row = _get_booking_by_id(booking_id)
        if row is None:
            send_message(
                vk,
                user_id,
                f"Запись с id {booking_id} не найдена.",
                keyboard=_main_keyboard_for(user_id),
            )
            return
        target_id = int(row["user_id"])
        s = datetime.fromisoformat(row["start_ts"])
        e = datetime.fromisoformat(row["end_ts"])
        dur = (e - s).total_seconds() / 3600.0
        _delete_booking(booking_id)
        # возвращаем часы клиенту
        current = _get_user_hours(target_id)
        new_value = current + dur
        _set_user_hours(target_id, new_value)

        display_name = _get_vk_name(vk, target_id)
        send_message(
            vk,
            user_id,
            "Запись удалена.\n"
            f"Клиент: {display_name} (id {target_id})\n"
            f"Когда: {s.strftime('%d.%m.%Y')} {s.strftime('%H:%M')}-{e.strftime('%H:%M')}\n"
            f"Возвращено: {_format_hours_balance(dur)}. Теперь у клиента {_format_hours_balance(new_value)}",
            keyboard=_main_keyboard_for(user_id),
        )
        # уведомим клиента
        try:
            send_message(
                vk,
                target_id,
                "Ваша запись была отменена администратором.\n"
                f"Дата: {s.strftime('%d.%m.%Y')}\n"
                f"Время: {s.strftime('%H:%M')}-{e.strftime('%H:%M')}\n\n"
                f"На абонемент возвращено {_format_hours_balance(dur)}. Теперь у вас {_format_hours_balance(new_value)}",
                keyboard=_main_keyboard_for(target_id),
            )
        except Exception:
            pass
        # уведомляем всех (кроме этого клиента), что освободилось время
        _broadcast_free_slot(vk, s, e, exclude_user_id=target_id)
        return

    # Ручное добавление часов администратором:
    # формат: "часы user кол_во", где user = id / id123 / @ник / ник / ссылка VK
    if user_id in ADMIN_VK_IDS and text.startswith("часы "):
        parts = text_norm.split()
        if len(parts) != 3:
            send_message(
                vk,
                user_id,
                "Формат команды для ручного добавления часов:\n"
                "часы USER КОЛИЧЕСТВО\n"
                "где USER: VK ID, никнейм, @ник или ссылка VK\n"
                "например: часы 21476079 5  или  часы durov 1,5",
                keyboard=_main_keyboard_for(user_id),
            )
            return
        try:
            target_id = _resolve_vk_user_id(vk, parts[1])
            if target_id is None:
                raise ValueError("bad user")
            delta_str = parts[2].replace(",", ".").replace("\u2212", "-")
            delta = float(delta_str)
        except ValueError:
            send_message(
                vk,
                user_id,
                "Не удалось распознать пользователя или количество часов. "
                "USER: VK ID, никнейм, @ник или ссылка VK. "
                "Часы: число (можно 1.5 или 1,5).",
                keyboard=_main_keyboard_for(user_id),
            )
            return
        _admin_apply_hours_delta(vk, user_id, target_id, delta, use_admin_keyboard=False)
        return

    # Реферальный код: показать свой и привязать чужой
    if "реф" in text and "код" in text and user_id in ADMIN_VK_IDS:
        # админ может смотреть свой код и код любого user_id, но для простоты показываем только свой
        send_message(
            vk,
            user_id,
            f"Ваш код для бонусов: {user_id}.\n"
            "Новый клиент один раз пишет боту: реф ваш_id_или_ник. "
            "Когда он впервые купит абонемент на 10 ч, вам добавят 1 ч бонусом.",
            keyboard=_main_keyboard_for(user_id),
        )
        return

    # Специальные случаи продления без ИИ — переводим в await_payment (иначе скрин в режиме idle не обрабатывается)
    tl = text.lower()
    slot_ctx = _text_looks_like_booking_time(text)

    if not slot_ctx and any(
        phrase in tl for phrase in ("на 10 часов", "10 часов", "абонемент на 10 часов")
    ):
        _enter_await_payment(state, user_id, 10.0)
        price = 2400 if _has_referrer_discount(user_id) else 2500
        send_message(
            vk,
            user_id,
            "Тогда оформим абонемент на 10 часов.\n"
            f"Отправьте, пожалуйста, {price} рублей по номеру 89124566686 (Альфа-банк) "
            "и пришлите сюда фото или файл со скриншотом перевода (не только текст). "
            "После оплаты добавим 10 часов на ваш абонемент.",
            keyboard=_payment_wait_keyboard(),
        )
        return

    if not slot_ctx and ("1,5" in tl or "1.5" in tl or "полтора час" in tl):
        _enter_await_payment(state, user_id, 1.5)
        send_message(
            vk,
            user_id,
            "Тогда продлим абонемент на 1,5 часа.\n"
            "Отправьте, пожалуйста, 450 рублей по номеру 89124566686 (Альфа-банк) "
            "и пришлите сюда фото или файл со скриншотом перевода (не только текст).",
            keyboard=_payment_wait_keyboard(),
        )
        return

    if not slot_ctx and ("2,5" in tl or "2.5" in tl or "два с половиной час" in tl):
        _enter_await_payment(state, user_id, 2.5)
        send_message(
            vk,
            user_id,
            "Тогда продлим абонемент на 2,5 часа.\n"
            "Отправьте, пожалуйста, 750 рублей по номеру 89124566686 (Альфа-банк) "
            "и пришлите сюда фото или файл со скриншотом перевода (не только текст).",
            keyboard=_payment_wait_keyboard(),
        )
        return

    if not slot_ctx and re.search(r"(?<!\d)3\s*час", tl):
        _enter_await_payment(state, user_id, 3.0)
        send_message(
            vk,
            user_id,
            "Тогда продлим абонемент на 3 часа.\n"
            "Отправьте, пожалуйста, 900 рублей по номеру 89124566686 (Альфа-банк) "
            "и пришлите сюда фото или файл со скриншотом перевода (не только текст).",
            keyboard=_payment_wait_keyboard(),
        )
        return

    if not slot_ctx and re.search(r"(?<!\d)2\s*час", tl):
        _enter_await_payment(state, user_id, 2.0)
        send_message(
            vk,
            user_id,
            "Тогда продлим абонемент на 2 часа.\n"
            "Отправьте, пожалуйста, 600 рублей по номеру 89124566686 (Альфа-банк) "
            "и пришлите сюда фото или файл со скриншотом перевода (не только текст).",
            keyboard=_payment_wait_keyboard(),
        )
        return

    if (
        not slot_ctx
        and any(phrase in text for phrase in ("на 1 час", "1 час", "один час"))
        and "1,5" not in tl
        and "1.5" not in tl
    ):
        _enter_await_payment(state, user_id, 1.0)
        send_message(
            vk,
            user_id,
            "Тогда продлим абонемент на 1 час.\n"
            "Отправьте, пожалуйста, 300 рублей по номеру 89124566686 (Альфа-банк) "
            "и пришлите сюда фото или файл со скриншотом перевода (не только текст).",
            keyboard=_payment_wait_keyboard(),
        )
        return

    # Уже оплатил / скрин — отвечаем балансом из БД без ИИ (иначе модель тянет старый контекст и врёт про «нет часов»).
    pay_hint = raw.lower()
    if any(
        w in pay_hint
        for w in (
            "оплатил",
            "оплатила",
            "оплат",
            "перевёл",
            "перевел",
            "перевела",
            "скрин",
            "скриншот",
            "чек ",
            "чек,",
            "зачисл",
            "пополнила",
            "пополнил",
            "отправила фото",
            "отправил фото",
            "вот перевод",
        )
    ):
        h = _get_user_hours(user_id)
        send_message(
            vk,
            user_id,
            "По данным бота сейчас на вашем абонементе: "
            f"{_format_hours_balance(h)}\n\n"
            "Если только что прислали скрин оплаты одним сообщением с фото или файлом после выбора тарифа в «Купить абонемент», "
            "часы должны зачислиться автоматически в течение нескольких секунд. "
            "Проверьте также кнопку «Мой абонемент».\n\n"
            "Если здесь по-прежнему 0 ч, выберите снова нужный пакет в «Купить абонемент» и пришлите скрин перевода снова одним сообщением.",
            keyboard=_main_keyboard_for(user_id),
        )
        return

    # Свободный текст: без DeepSeek, чтобы не путать пользователей.
    send_message(
        vk,
        user_id,
        "Для брони, оплаты и управления абонементом используйте кнопки меню ниже.\n\n"
        "Связаться со Снежаной: https://vk.com/id164817756",
        keyboard=_main_keyboard_for(user_id),
    )


def handle_message(vk, event) -> None:
    user_id = event.user_id
    _touch_known_user(user_id)
    text = event.text or ""
    text_norm = text.strip()
    state = STATES.setdefault(user_id, UserState())
    _restore_awaiting_payment_from_db(user_id, state)

    if text_norm.lower() in ("/start", "start", "старт", "начать"):
        handle_start(vk, user_id)
        return

    if text_norm == "⬅ Назад":
        handle_start(vk, user_id)
        return

    # Приветствие от лица Снежаны: один раз в сутки.
    # На админов не отвлекаем, чтобы не мешать командам.
    if (
        user_id not in ADMIN_VK_IDS
        and state.mode == "idle"
        and not _is_user_welcomed(user_id)
    ):
        handle_start(vk, user_id)
        return

    # Админ-команды должны работать из любого состояния
    lower_any = text_norm.lower().strip()
    if user_id in ADMIN_VK_IDS and lower_any in ("команды", "/admin", "админ"):
        STATES[user_id] = UserState(mode="idle")
        send_message(
            vk,
            user_id,
            "Административные команды:\n\n"
            "• команды или /admin — показать этот список\n"
            "• баланс — отчет по пользователям (часы, оплаты)\n"
            "• отчет ... — отчет по бронированиям за период (например: отчет этот месяц)\n"
            "• записи ПЕРИОД — все записи за период с именами и id\n"
            "  (пример: записи сегодня | записи этот месяц | записи с 01.04.2026 по 15.04.2026)\n"
            "• удалить запись BOOKING_ID — удалить запись клиента и вернуть часы\n"
            "  (пример: удалить запись 123)\n"
            "• часы USER КОЛ-ВО — вручную изменить часы пользователя\n"
            "  (пример: часы 21476079 5  или  часы durov 1,5)\n"
            "• ➖ удалить часы — пошагово списать часы у клиента\n"
            "• участники — все участники бота (имена и кликабельные ссылки vk.com/id…)\n"
            "• 📊 бонусы по кодам — кто чей код ввёл и какие были начисления\n"
            "• синк календаря — ручной пуш data/bookings.json в GitHub\n"
            "• ➕ добавить часы — пошагово начислить или списать часы у клиента (дробные часы можно)\n\n"
            "Остальные функции доступны через кнопки меню.\n\n"
            "Ниже — те же действия кнопками; «⬅ Главное меню» вернёт обычное меню.",
            keyboard=_admin_keyboard(),
        )
        return

    if user_id in ADMIN_VK_IDS and any(
        kw in lower_any for kw in ("участники", "список участников", "все участники")
    ):
        STATES[user_id] = UserState(mode="idle")
        _send_admin_participants(vk, user_id)
        return

    if user_id in ADMIN_VK_IDS and lower_any in _ADMIN_BONUS_STATS_TEXTS_LOWER:
        STATES[user_id] = UserState(mode="idle")
        _send_admin_referral_stats(vk, user_id)
        return

    if user_id in ADMIN_VK_IDS and lower_any == "➕ добавить часы":
        STATES[user_id] = UserState(
            mode="admin_add_hours_ask_client",
            admin_hours_target_id=None,
        )
        send_message(
            vk,
            user_id,
            "Добавить часы на абонемент пользователю.\n\n"
            "Шаг 1. Отправьте пользователя VK: ID, никнейм, @ник или ссылку.\n"
            "Примеры: 21476079, durov, @durov, https://vk.com/durov\n\n"
            "Шаг 2: бот попросит указать, сколько часов добавить "
            "(дробь через точку или запятую, например 1,5; можно отрицательное число).",
            keyboard=_back_keyboard(),
        )
        return

    if user_id in ADMIN_VK_IDS and lower_any == "➖ удалить часы":
        STATES[user_id] = UserState(
            mode="admin_remove_hours_ask_client",
            admin_hours_target_id=None,
        )
        send_message(
            vk,
            user_id,
            "Удалить часы с абонемента пользователя.\n\n"
            "Шаг 1. Отправьте пользователя VK: ID, никнейм, @ник или ссылку.\n"
            "Примеры: 21476079, durov, @durov, https://vk.com/durov\n\n"
            "Шаг 2: бот попросит указать, сколько часов списать "
            "(только положительное число, можно дробное: 1,5).",
            keyboard=_back_keyboard(),
        )
        return

    if state.mode == "buy_choice":
        lower = text_norm.lower()
        if lower in ("отмена", "назад"):
            STATES[user_id] = UserState(mode="idle")
            handle_start(vk, user_id)
            return
        # Порядок важен: сначала 10 ч и 1,5 ч, иначе «1» перехватит полтора часа.
        if lower.startswith("10") or "10 часов" in lower or lower.strip() == "10":
            _enter_await_payment(state, user_id, 10.0)
            has_discount = _has_referrer_discount(user_id)
            price = 2400 if has_discount else 2500
            send_message(
                vk,
                user_id,
                "Тогда оформим абонемент на 10 часов.\n"
                f"Отправьте, пожалуйста, {price} рублей по номеру 89124566686 (Альфа-банк) "
                "и пришлите сюда фото или файл со скриншотом перевода (не только текст). "
                "Как только оплата придёт, обновим ваш абонемент.",
                keyboard=_payment_wait_keyboard(),
            )
            return
        if "1,5" in lower or "1.5" in lower or "полтора" in lower:
            _enter_await_payment(state, user_id, 1.5)
            send_message(
                vk,
                user_id,
                "Тогда продлим абонемент на 1,5 часа.\n"
                "Отправьте, пожалуйста, 450 рублей по номеру 89124566686 (Альфа-банк) "
                "и пришлите сюда фото или файл со скриншотом перевода (не только текст). "
                "Как только оплата придёт, обновим ваш абонемент.",
                keyboard=_payment_wait_keyboard(),
            )
            return
        if "2,5" in lower or "2.5" in lower or (
            "750" in lower.replace(" ", "") and "час" in lower and "1,5" not in lower and "1.5" not in lower
        ):
            _enter_await_payment(state, user_id, 2.5)
            send_message(
                vk,
                user_id,
                "Тогда продлим абонемент на 2,5 часа.\n"
                "Отправьте, пожалуйста, 750 рублей по номеру 89124566686 (Альфа-банк) "
                "и пришлите сюда фото или файл со скриншотом перевода (не только текст). "
                "Как только оплата придёт, обновим ваш абонемент.",
                keyboard=_payment_wait_keyboard(),
            )
            return
        if "3 час" in lower or (lower.startswith("3") and "900" in lower.replace(" ", "")):
            _enter_await_payment(state, user_id, 3.0)
            send_message(
                vk,
                user_id,
                "Тогда продлим абонемент на 3 часа.\n"
                "Отправьте, пожалуйста, 900 рублей по номеру 89124566686 (Альфа-банк) "
                "и пришлите сюда фото или файл со скриншотом перевода (не только текст). "
                "Как только оплата придёт, обновим ваш абонемент.",
                keyboard=_payment_wait_keyboard(),
            )
            return
        if "2 час" in lower or (lower.startswith("2") and "600" in lower.replace(" ", "")):
            _enter_await_payment(state, user_id, 2.0)
            send_message(
                vk,
                user_id,
                "Тогда продлим абонемент на 2 часа.\n"
                "Отправьте, пожалуйста, 600 рублей по номеру 89124566686 (Альфа-банк) "
                "и пришлите сюда фото или файл со скриншотом перевода (не только текст). "
                "Как только оплата придёт, обновим ваш абонемент.",
                keyboard=_payment_wait_keyboard(),
            )
            return
        if "1 час" in lower or lower.strip() == "1" or (
            lower.startswith("1")
            and "300" in lower.replace(" ", "")
            and "1,5" not in lower
            and "1.5" not in lower
        ):
            _enter_await_payment(state, user_id, 1.0)
            send_message(
                vk,
                user_id,
                "Тогда продлим абонемент на 1 час.\n"
                "Отправьте, пожалуйста, 300 рублей по номеру 89124566686 (Альфа-банк) "
                "и пришлите сюда фото или файл со скриншотом перевода (не только текст). "
                "Как только оплата придёт, обновим ваш абонемент.",
                keyboard=_payment_wait_keyboard(),
            )
            return
        send_message(
            vk,
            user_id,
            "Пожалуйста, выберите вариант на клавиатуре: "
            "1 ч, 1,5 ч, 2 ч, 2,5 ч, 3 ч или 10 ч — либо нажмите «⬅ Назад».",
            keyboard=_buy_keyboard(),
        )
        return

    if state.mode == "await_payment":
        lower = text_norm.lower()
        if lower in ("отмена", "назад"):
            _clear_awaiting_payment(user_id)
            STATES[user_id] = UserState(mode="idle")
            send_message(
                vk,
                user_id,
                "Ок, отменил ожидание оплаты. Возвращаю в меню.",
                keyboard=_main_keyboard_for(user_id),
            )
            return
        # Любое входящее сообщение (чаще всего со скриншотом) считаем подтверждением оплаты.
        pending = float(state.pending_add_hours or 0)
        if pending <= 0:
            handle_start(vk, user_id)
            return
        # Long Poll часто не отдаёт вложения без preload / getById — проверяем полное сообщение
        if not _event_has_payment_attachment(vk, event):
            send_message(
                vk,
                user_id,
                "Нужно подтверждение оплаты картинкой или файлом.\n\n"
                "Отправьте фото скрина или документ (PDF или изображение) в этот чат. "
                "Один текст без вложения не засчитывается.\n\n"
                "Если фото уже отправляли, а бот снова просит — перешлите скрин ещё раз одним сообщением "
                "или нажмите «отмена» и выберите тариф снова.",
                keyboard=_payment_wait_keyboard(),
            )
            return
        try:
            vk.messages.send(
                user_id=PAYMENT_SCREENSHOT_ADMIN_ID,
                random_id=int(time.time() * 1000),
                message=f"Новая оплата от user_id {user_id}. Ниже оригинальное сообщение со скриншотом:",
                forward_messages=event.message_id,
            )
        except Exception as e:
            logger.warning("Не удалось переслать сообщение админу %s: %s", PAYMENT_SCREENSHOT_ADMIN_ID, e)
        send_message(
            vk,
            user_id,
            "Спасибо, проверяем оплату...",
        )
        time.sleep(5)
        current = _get_user_hours(user_id)
        new_value = current + pending
        _set_user_hours(user_id, new_value)
        amount = _payment_amount_for_hours(pending, user_id)
        if amount <= 0:
            amount = int(round(pending * 300))
        conn = _get_db_connection()
        try:
            cur = conn.cursor()
            cur.execute(
                "INSERT INTO payments (user_id, amount, hours_added, created_at) VALUES (?, ?, ?, ?)",
                (user_id, amount, pending, datetime.now(timezone.utc).isoformat()),
            )
            payment_id = cur.lastrowid
            conn.commit()
        finally:
            conn.close()

        ref_conn = None
        try:
            ref_conn = _get_db_connection()
            cur = ref_conn.cursor()
            cur.execute(
                "SELECT referrer_id FROM referrals WHERE user_id = ?",
                (user_id,),
            )
            row = cur.fetchone()
            if row is not None and _hours_almost_equal(pending, 10.0):
                referrer_id = row["referrer_id"]
                cur.execute(
                    "SELECT 1 FROM payments WHERE user_id = ? AND ABS(hours_added - 10) < 0.001 AND id != ?",
                    (user_id, payment_id),
                )
                had_prev_10 = cur.fetchone()
                if had_prev_10 is None:
                    cur.execute(
                        "SELECT 1 FROM referral_payments WHERE user_id = ?",
                        (user_id,),
                    )
                    already = cur.fetchone()
                    if already is None:
                        bonus_hours = 1
                        ref_hours = _get_user_hours(referrer_id)
                        _set_user_hours(referrer_id, ref_hours + bonus_hours)
                        cur.execute(
                            """
                            INSERT INTO referral_payments
                            (referrer_id, user_id, payment_id, bonus_hours, created_at)
                            VALUES (?, ?, ?, ?, ?)
                            """,
                            (
                                referrer_id,
                                user_id,
                                payment_id,
                                bonus_hours,
                                datetime.now(timezone.utc).isoformat(),
                            ),
                        )
                        ref_conn.commit()
                        send_message(
                            vk,
                            referrer_id,
                            "Ваш друг оплатил абонемент на 10 часов по вашему коду. "
                            "Вам начислен бонус: 1 час.",
                            keyboard=_main_keyboard_for(referrer_id),
                        )
                        for admin_id in ADMIN_VK_IDS:
                            send_message(
                                vk,
                                admin_id,
                                f"Бонус по коду: пригласивший {referrer_id}, "
                                f"покупатель {user_id}, сумма {amount} ₽, +1 ч.",
                            )
        except Exception as e:
            logger.warning("Ошибка при обработке бонуса по коду: %s", e)
        finally:
            if ref_conn is not None:
                try:
                    ref_conn.close()
                except Exception:
                    pass

        _clear_admin_chat_history(user_id)
        send_message(
            vk,
            user_id,
            f"Оплата подтверждена. Мы обновили ваш абонемент.\n"
            f"Теперь на вашем абонементе: {_format_hours_balance(new_value)}.\n\n"
            "Ключ от кабинета находится в мини‑сейфе на стене рядом с дверью.\n"
            "Код от сейфа: 2611.",
            keyboard=_main_keyboard_for(user_id),
        )
        _clear_awaiting_payment(user_id)
        STATES[user_id] = UserState(mode="idle")
        return

    if state.mode == "admin_add_hours_ask_client" and user_id in ADMIN_VK_IDS:
        client_id = _resolve_vk_user_id(vk, text_norm)
        if client_id is None:
            send_message(
                vk,
                user_id,
                "Не удалось распознать пользователя VK.\n"
                "Отправьте VK ID, никнейм, @ник или ссылку.\n"
                "Примеры: 21476079, durov, @durov, https://vk.com/durov\n\n"
                "Или нажмите «⬅ Назад».",
                keyboard=_back_keyboard(),
            )
            return
        display_name = _get_vk_name(vk, client_id)
        cur_h = _get_user_hours(client_id)
        state.admin_hours_target_id = client_id
        state.mode = "admin_add_hours_ask_delta"
        send_message(
            vk,
            user_id,
            f"Пользователь: {display_name} (id {client_id}).\n"
            f"Сейчас на абонементе: {_format_hours_balance(cur_h)}.\n\n"
            "Сколько часов добавить к балансу? Отправьте число (можно дробное: 1,5 или -0.5).\n"
            "Можно отрицательное число, чтобы уменьшить баланс.\n"
            "Примеры: 5  или  -2  или  1.5",
            keyboard=_back_keyboard(),
        )
        return

    if state.mode == "admin_add_hours_ask_delta" and user_id in ADMIN_VK_IDS:
        tid = state.admin_hours_target_id
        if tid is None:
            STATES[user_id] = UserState(mode="idle")
            send_message(
                vk,
                user_id,
                "Сессия сброшена. Начните снова: «➕ Добавить часы».",
                keyboard=_admin_keyboard(),
            )
            return
        raw = text_norm.strip().replace("\u2212", "-")
        raw = raw.replace(",", ".")
        raw = re.sub(r"\s+", "", raw)
        try:
            delta = float(raw)
        except ValueError:
            send_message(
                vk,
                user_id,
                "Нужно одно число часов, например: 5  или  -1  или  1.5\n"
                "Или нажмите «⬅ Назад».",
                keyboard=_back_keyboard(),
            )
            return
        if not (delta == delta) or abs(delta) > 1e6:
            send_message(
                vk,
                user_id,
                "Некорректное число. Введите разумное значение часов.",
                keyboard=_back_keyboard(),
            )
            return
        _admin_apply_hours_delta(vk, user_id, tid, delta, use_admin_keyboard=True)
        STATES[user_id] = UserState(mode="idle")
        return

    if state.mode == "admin_remove_hours_ask_client" and user_id in ADMIN_VK_IDS:
        client_id = _resolve_vk_user_id(vk, text_norm)
        if client_id is None:
            send_message(
                vk,
                user_id,
                "Не удалось распознать пользователя VK.\n"
                "Отправьте VK ID, никнейм, @ник или ссылку.\n"
                "Примеры: 21476079, durov, @durov, https://vk.com/durov\n\n"
                "Или нажмите «⬅ Назад».",
                keyboard=_back_keyboard(),
            )
            return
        display_name = _get_vk_name(vk, client_id)
        cur_h = _get_user_hours(client_id)
        state.admin_hours_target_id = client_id
        state.mode = "admin_remove_hours_ask_delta"
        send_message(
            vk,
            user_id,
            f"Пользователь: {display_name} (id {client_id}).\n"
            f"Сейчас на абонементе: {_format_hours_balance(cur_h)}.\n\n"
            "Сколько часов списать? Отправьте положительное число (можно дробное: 1,5).\n"
            "Примеры: 1  или  2.5",
            keyboard=_back_keyboard(),
        )
        return

    if state.mode == "admin_remove_hours_ask_delta" and user_id in ADMIN_VK_IDS:
        tid = state.admin_hours_target_id
        if tid is None:
            STATES[user_id] = UserState(mode="idle")
            send_message(
                vk,
                user_id,
                "Сессия сброшена. Начните снова: «➖ Удалить часы».",
                keyboard=_admin_keyboard(),
            )
            return
        raw = text_norm.strip().replace("\u2212", "-")
        raw = raw.replace(",", ".")
        raw = re.sub(r"\s+", "", raw)
        try:
            remove_hours = float(raw)
        except ValueError:
            send_message(
                vk,
                user_id,
                "Нужно одно число часов для списания, например: 1  или  2.5\n"
                "Или нажмите «⬅ Назад».",
                keyboard=_back_keyboard(),
            )
            return
        if not (remove_hours == remove_hours) or abs(remove_hours) > 1e6 or remove_hours <= 0:
            send_message(
                vk,
                user_id,
                "Введите положительное число часов для списания.",
                keyboard=_back_keyboard(),
            )
            return
        _admin_apply_hours_delta(vk, user_id, tid, -remove_hours, use_admin_keyboard=True)
        STATES[user_id] = UserState(mode="idle")
        return

    if state.mode == "admin_cancel_ask_client" and user_id in ADMIN_VK_IDS:
        client_id = _resolve_vk_user_id(vk, text_norm)
        if client_id is None:
            send_message(
                vk,
                user_id,
                "Не удалось распознать пользователя VK.\n"
                "Отправьте VK ID, никнейм, @ник или ссылку.\n"
                "Примеры: 21476079, durov, @durov, https://vk.com/durov\n\n"
                "Или нажмите «⬅ Назад».",
                keyboard=_back_keyboard(),
            )
            return
        future = _get_future_bookings(client_id)
        if not future:
            send_message(
                vk,
                user_id,
                f"У клиента id {client_id} нет будущих бронирований.",
                keyboard=_main_keyboard_for(user_id),
            )
            STATES[user_id] = UserState(mode="idle")
            return
        cn = _get_vk_name(vk, client_id)
        state.cancel_for_user_id = client_id
        state.mode = "cancel_select"
        lines = [
            f"Будущие бронирования клиента: {cn} (id {client_id}).\n"
            "При отмене за 24 ч и более до начала часы возвращаются на абонемент клиента; менее чем за 24 ч — не возвращаются."
        ]
        for idx, row in enumerate(future, start=1):
            s = datetime.fromisoformat(row["start_ts"])
            e = datetime.fromisoformat(row["end_ts"])
            lines.append(
                f"{idx}. {s.strftime('%d.%m.%Y')} {s.strftime('%H:%M')}-{e.strftime('%H:%M')}"
            )
        lines.append("\nОтправьте номер бронирования для отмены.")
        send_message(
            vk,
            user_id,
            "\n".join(lines),
            keyboard=_back_keyboard(),
        )
        return

    if state.mode == "cancel_select":
        target_uid = user_id
        if state.cancel_for_user_id is not None:
            if user_id not in ADMIN_VK_IDS:
                state.cancel_for_user_id = None
            else:
                target_uid = state.cancel_for_user_id
        admin_cancels_for_client = (
            state.cancel_for_user_id is not None
            and user_id in ADMIN_VK_IDS
            and target_uid != user_id
        )
        try:
            choice = int(text_norm.strip())
        except ValueError:
            send_message(
                vk,
                user_id,
                "Пожалуйста, отправьте номер бронирования, которое хотите отменить, "
                "или нажмите «⬅ Назад».",
                keyboard=_back_keyboard(),
            )
            return
        future = _get_future_bookings(target_uid)
        cancellable = future
        if not cancellable:
            send_message(
                vk,
                user_id,
                "Сейчас нет будущих бронирований для отмены.",
                keyboard=_main_keyboard_for(user_id),
            )
            STATES[user_id] = UserState(mode="idle")
            return
        if not (1 <= choice <= len(cancellable)):
            send_message(
                vk,
                user_id,
                "Такого номера нет среди бронирований, которые можно отменить. "
                "Попробуйте ещё раз или нажмите «⬅ Назад».",
                keyboard=_back_keyboard(),
            )
            return
        row = cancellable[choice - 1]
        _delete_booking(row["id"])
        s = _parse_booking_ts(row["start_ts"])
        e = _parse_booking_ts(row["end_ts"])
        # Наивное UTC «сейчас» — в одном стиле с s/e из _parse_booking_ts.
        now = datetime.now(timezone.utc).replace(tzinfo=None)
        at_least_24h = (s - now) >= timedelta(hours=24)
        if at_least_24h:
            duration_hours = (e - s).total_seconds() / 3600.0
            current = _get_user_hours(target_uid)
            new_value = current + duration_hours
            _set_user_hours(target_uid, new_value)
            dur_txt = _format_hours_balance(duration_hours)
            bal_txt = _format_hours_balance(new_value)
            if admin_cancels_for_client:
                try:
                    send_message(
                        vk,
                        target_uid,
                        "Администратор отменил ваше бронирование.\n"
                        f"Дата: {s.strftime('%d.%m.%Y')}\n"
                        f"Время: {s.strftime('%H:%M')}-{e.strftime('%H:%M')}\n\n"
                        f"На абонемент возвращено {dur_txt}\n"
                        f"Теперь на вашем абонементе: {bal_txt}",
                        keyboard=_main_keyboard_for(target_uid),
                    )
                except Exception:
                    pass
                send_message(
                    vk,
                    user_id,
                    "Бронирование клиента отменено.\n"
                    f"Клиент: id {target_uid}\n"
                    f"Дата: {s.strftime('%d.%m.%Y')}\n"
                    f"Время: {s.strftime('%H:%M')}-{e.strftime('%H:%M')}\n\n"
                    f"Клиенту на абонемент возвращено {dur_txt}.",
                    keyboard=_main_keyboard_for(user_id),
                )
            else:
                send_message(
                    vk,
                    user_id,
                    "Бронирование отменено.\n"
                    f"Дата: {s.strftime('%d.%m.%Y')}\n"
                    f"Время: {s.strftime('%H:%M')}-{e.strftime('%H:%M')}\n\n"
                    f"На абонемент возвращено {dur_txt}\n"
                    f"Теперь на вашем абонементе: {bal_txt}",
                    keyboard=_main_keyboard_for(user_id),
                )
        else:
            if admin_cancels_for_client:
                try:
                    send_message(
                        vk,
                        target_uid,
                        "Администратор отменил ваше бронирование.\n"
                        f"Дата: {s.strftime('%d.%m.%Y')}\n"
                        f"Время: {s.strftime('%H:%M')}-{e.strftime('%H:%M')}\n\n"
                        "При отмене менее чем за 24 часа до начала часы на абонемент не возвращаются.",
                        keyboard=_main_keyboard_for(target_uid),
                    )
                except Exception:
                    pass
                send_message(
                    vk,
                    user_id,
                    "Бронирование клиента отменено.\n"
                    f"Клиент: id {target_uid}\n"
                    f"Дата: {s.strftime('%d.%m.%Y')}\n"
                    f"Время: {s.strftime('%H:%M')}-{e.strftime('%H:%M')}\n\n"
                    "Часы не возвращались (менее 24 ч до начала).",
                    keyboard=_main_keyboard_for(user_id),
                )
            else:
                send_message(
                    vk,
                    user_id,
                    "Бронирование отменено.\n"
                    f"Дата: {s.strftime('%d.%m.%Y')}\n"
                    f"Время: {s.strftime('%H:%M')}-{e.strftime('%H:%M')}\n\n"
                    "При отмене менее чем за 24 часа до начала часы на абонемент не возвращаются.",
                    keyboard=_main_keyboard_for(user_id),
                )
        # уведомляем всех (кроме клиента, у которого отменили слот), что освободилось время
        _broadcast_free_slot(vk, s, e, exclude_user_id=target_uid)
        state.cancel_for_user_id = None
        STATES[user_id] = UserState(mode="idle")
        return

    if state.mode == "idle":
        # Код пригласившего (бонусная система): «реф 21476079» / «реф durov»
        # или длинный VK ID одной строкой
        lower = text_norm.lower()
        # Явно: «реф 123» / «123» при проверках ниже.
        # Сообщение из одних цифр — только если это похоже на VK ID (≥6 цифр)
        # или такой user_id уже есть в базе как клиент; иначе это часто «часы» / тариф —
        # не считаем рефералом и отдаём в handle_main_menu (покупка, фразы про оплату и т.д.).
        ref_id: Optional[int] = None
        m = re.match(r"^\s*реф\s*[: ]\s*(\S+)\s*$", text_norm, re.IGNORECASE)
        if m:
            ref_token = m.group(1).strip()
            ref_id = _resolve_vk_user_id(vk, ref_token)
        else:
            m_digits = re.fullmatch(r"\s*(\d+)\s*", lower)
            if m_digits:
                cand = int(m_digits.group(1))
                ds = m_digits.group(1)
                if len(ds) >= 6 or _referrer_exists_in_db(cand):
                    ref_id = cand

        if ref_id is not None:
            try:
                ref_id = int(ref_id)
            except ValueError:
                send_message(
                    vk,
                    user_id,
                    "Не разобрала команду. Напишите так: реф и код друга (например: реф 21476079 или реф durov).",
                    keyboard=_main_keyboard_for(user_id),
                )
                return
            if ref_id == user_id:
                send_message(
                    vk,
                    user_id,
                    "Нельзя указать самого себя — введите код того, кто вас пригласил.",
                    keyboard=_main_keyboard_for(user_id),
                )
                return
            if not _referrer_exists_in_db(ref_id):
                send_message(
                    vk,
                    user_id,
                    "Такого номера нет в нашей базе. Проверьте код или попросите друга открыть бота и написать вам строку из меню «Бонусная система».",
                    keyboard=_main_keyboard_for(user_id),
                )
                return
            try:
                conn = _get_db_connection()
                cur = conn.cursor()
                # если уже указан пригласивший, не переписываем
                cur.execute(
                    "SELECT referrer_id FROM referrals WHERE user_id = ?",
                    (user_id,),
                )
                if cur.fetchone() is None:
                    cur.execute(
                        "INSERT INTO referrals (user_id, referrer_id, created_at) VALUES (?, ?, ?)",
                        (user_id, ref_id, datetime.now(timezone.utc).isoformat()),
                    )
                    conn.commit()
                    send_message(
                        vk,
                        user_id,
                        f"Готово. Записала, что вас пригласил человек с номером {ref_id}. "
                        "Если вы впервые купите абонемент на 10 часов, ему начислят бонус 1 час (и у вас будет скидка на эту первую покупку 10 ч).",
                        keyboard=_main_keyboard_for(user_id),
                    )
                else:
                    send_message(
                        vk,
                        user_id,
                        "Код друга уже был введён ранее, поменять его нельзя.",
                        keyboard=_main_keyboard_for(user_id),
                    )
            except Exception as e:
                logger.warning("Ошибка при сохранении кода пригласившего: %s", e)
                send_message(
                    vk,
                    user_id,
                    "Не удалось сохранить код. Попробуйте позже.",
                    keyboard=_main_keyboard_for(user_id),
                )
            finally:
                try:
                    conn.close()
                except Exception:
                    pass
            return

        handle_main_menu(vk, user_id, text_norm)
        return

    if state.mode == "admin_report_range":
        txt = text_norm.lower()
        today = date.today()

        if txt in ("сегодня", "today"):
            start_date = end_date = today
        elif txt in ("этот месяц", "текущий месяц"):
            start_date = today.replace(day=1)
            if today.month == 12:
                nm = today.replace(year=today.year + 1, month=1, day=1)
            else:
                nm = today.replace(month=today.month + 1, day=1)
            end_date = nm - timedelta(days=1)
        elif "с " in txt and " по " in txt:
            try:
                part = txt.replace("с ", "", 1)
                left, right = part.split(" по ", 1)
                start_date = datetime.strptime(left.strip(), "%d.%m.%Y").date()
                end_date = datetime.strptime(right.strip(), "%d.%m.%Y").date()
            except Exception:
                send_message(
                    vk,
                    user_id,
                    "Не удалось распознать даты. Используйте формат: с 01.04.2026 по 15.04.2026",
                    keyboard=_back_keyboard(),
                )
                return
        else:
            send_message(
                vk,
                user_id,
                "Не понял период. Напишите «сегодня», «этот месяц» или, например:\n"
                "с 01.04.2026 по 15.04.2026",
                keyboard=_back_keyboard(),
            )
            return

        _send_admin_period_report(vk, user_id, start_date, end_date)
        STATES[user_id] = UserState(mode="idle")
        return

    if state.mode == "choosing_day":
        # Поддержка формата "завтра в 10:00" — сразу дата и время
        if " в " in text_norm:
            date_part, time_part = text_norm.split(" в ", 1)
            chosen = _parse_human_date(date_part)
            t = _parse_time_15(time_part)
            if chosen is not None and t is not None:
                state.chosen_day = chosen
                state.start_time = t
                state.mode = "choosing_duration"
                send_message(
                    vk,
                    user_id,
                    f"Выбраны дата и время: {_format_date(chosen)}, {t.strftime('%H:%M')}.\n\n"
                    "Теперь выберите продолжительность бронирования (часы будут списаны с абонемента):",
                    keyboard=_duration_keyboard(),
                )
                return

        chosen = _parse_human_date(text_norm)
        if chosen is None:
            send_message(
                vk,
                user_id,
                "Не удалось распознать дату. Можно писать, например:\n"
                "• сегодня\n"
                "• завтра\n"
                "• послезавтра\n"
                "• 18.03.2026\n"
                "• 5 мая\n"
                "• 5 мая 2026",
                keyboard=_back_keyboard(),
            )
            return
        state.chosen_day = chosen
        state.mode = "choosing_time"
        send_message(
            vk,
            user_id,
            f"Выбрана дата: {_format_date(chosen)}.\n\n"
            "Теперь напишите время начала бронирования в свободной форме.\n"
            "Например: 10, 10:00, 10 утра, 18 15, 18:15 и т.п.",
            keyboard=_back_keyboard(),
        )
        return

    if state.mode == "choosing_time":
        t = _parse_time_15(text_norm)
        if t is None:
            send_message(
                vk,
                user_id,
                "Не удалось распознать время.\n"
                "Можно писать, например: 10, 10:00, 10 утра, 18 15, 18:15.",
                keyboard=_back_keyboard(),
            )
            return
        state.start_time = t
        state.mode = "choosing_duration"
        send_message(
            vk,
            user_id,
            "Выберите продолжительность бронирования (часы будут списаны с абонемента):",
            keyboard=_duration_keyboard(),
        )
        return

    if state.mode == "choosing_duration":
        hours = _parse_duration_hours(text_norm)
        if hours is None:
            send_message(
                vk,
                user_id,
                "Пожалуйста, выберите продолжительность кнопкой: 1 ч., 1,5 часа, 2 ч., 2,5 часа и т.д.",
            )
            return
        current_hours = _get_user_hours(user_id)
        if hours > current_hours + 1e-9:
            send_message(
                vk,
                user_id,
                f"У вас только {_format_hours_balance(current_hours)} на абонементе, "
                f"а вы выбрали {_format_hours_balance(hours)}.\n"
                "Выберите меньшую продолжительность или пополните абонемент.",
            )
            return
        if state.chosen_day is None or state.start_time is None:
            handle_start(vk, user_id)
            return
        start_dt = datetime.combine(state.chosen_day, state.start_time)
        end_dt = start_dt + timedelta(hours=float(hours))
        if end_dt.date() != state.chosen_day:
            send_message(
                vk,
                user_id,
                "С этой продолжительностью бронь переходит на следующий день.\n"
                "Пока можно бронировать в пределах одних суток. Выберите меньшую длительность "
                "или другое время начала.",
            )
            return
        if not _is_free(start_dt, end_dt):
            busy_text = _busy_intervals_for_day(state.chosen_day)
            suggestion = _find_nearest_free_interval(
                state.chosen_day, hours, start_dt
            )
            suggestion_text = ""
            if suggestion is not None:
                alt_start, alt_end = suggestion
                suggestion_text = (
                    f"\n\nБлижайшее свободное время: "
                    f"{alt_start.strftime('%H:%M')}–{alt_end.strftime('%H:%M')}."
                )
            send_message(
                vk,
                user_id,
                "К сожалению, этот интервал уже занят.\n\n"
                + busy_text
                + suggestion_text
                + "\n\nНапишите другое время начала, которое вам удобно.",
                keyboard=_back_keyboard(),
            )
            state.mode = "choosing_time"
            return

        state.duration_hours = hours
        state.start_dt = start_dt
        state.end_dt = end_dt
        state.mode = "confirm"
        dh = state.duration_hours
        whole_hours = (
            dh is not None
            and dh >= 2
            and abs(float(dh) - int(round(float(dh)))) < 1e-9
        )
        send_message(
            vk,
            user_id,
            "Проверьте данные бронирования:\n\n"
            f"📅 Дата: {_format_date(state.chosen_day)}\n"
            f"⏰ Время: {start_dt.strftime('%H:%M')}–{end_dt.strftime('%H:%M')}\n"
            f"⌛ Будет списано с абонемента: {_format_hours_balance(hours)}\n\n"
            "Нажмите «подтвердить» или «Нужен перерыв 15 минут».",
            keyboard=_confirm_keyboard(with_break_button=whole_hours),
        )
        return

    if state.mode == "confirm":
        if (
            state.start_dt is None
            or state.end_dt is None
            or state.duration_hours is None
        ):
            handle_start(vk, user_id)
            return

        current_hours = _get_user_hours(user_id)
        if state.duration_hours > current_hours + 1e-9:
            send_message(
                vk,
                user_id,
                "Похоже, часы на абонементе уже израсходованы. Пополните абонемент.",
                keyboard=_main_keyboard_for(user_id),
            )
            handle_start(vk, user_id)
            return
        remaining = current_hours - float(state.duration_hours)

        # Кнопка «Нужен перерыв 15 минут» — N слотов по 1 часу, после каждого часа перерыв 15 мин.
        # Следующая бронь возможна только с (конец последнего слота + 15 мин) — обеспечивает _is_free.
        nh = int(round(float(state.duration_hours))) if state.duration_hours is not None else 0
        if (
            "перерыв" in text_norm.lower() and "15" in text_norm
            and state.duration_hours is not None
            and state.duration_hours >= 2
            and abs(float(state.duration_hours) - nh) < 1e-9
        ):
            slots = []
            for i in range(nh):
                slot_start = state.start_dt + timedelta(minutes=i * 75)  # i*(60+15)
                slot_end = slot_start + timedelta(hours=1)
                if slot_end.date() != state.chosen_day:
                    send_message(
                        vk,
                        user_id,
                        "С перерывами бронь выходит за пределы дня. Выберите другое время или «подтвердить» без перерыва.",
                        keyboard=_back_keyboard(),
                    )
                    state.mode = "choosing_time"
                    return
                slots.append((slot_start, slot_end))
            for slot_start, slot_end in slots:
                if not _is_free(slot_start, slot_end):
                    send_message(
                        vk,
                        user_id,
                        "К сожалению, один из слотов уже занят. Выберите другое время.",
                        keyboard=_back_keyboard(),
                    )
                    state.mode = "choosing_time"
                    return
            _set_user_hours(user_id, remaining)
            for slot_start, slot_end in slots:
                _add_booking(user_id, slot_start, slot_end)
            lines = [f"📅 Дата: {_format_date(state.start_dt.date())}\n"]
            for i, (slot_start, slot_end) in enumerate(slots, 1):
                lines.append(f"⏰ Часть {i}: {slot_start.strftime('%H:%M')}–{slot_end.strftime('%H:%M')}")
                if i < len(slots):
                    lines.append("⏸ Перерыв 15 мин")
            lines.append(f"\n🎫 Осталось на абонементе: {_format_hours_balance(remaining)}")
            send_message(
                vk,
                user_id,
                "Готово! Ваше бронирование с перерывом подтверждено ✅\n\n" + "\n".join(lines),
                keyboard=_main_keyboard_for(user_id),
            )
            STATES[user_id] = UserState(mode="idle")
            return

        if text_norm.lower() == "подтвердить":
            if not _is_free(state.start_dt, state.end_dt):
                busy_text = _busy_intervals_for_day(state.start_dt.date())
                suggestion = _find_nearest_free_interval(
                    state.start_dt.date(), float(state.duration_hours), state.start_dt
                )
                suggestion_text = ""
                if suggestion is not None:
                    alt_start, alt_end = suggestion
                    suggestion_text = (
                        f"\n\nБлижайшее свободное время: "
                        f"{alt_start.strftime('%H:%M')}–{alt_end.strftime('%H:%M')}."
                    )
                send_message(
                    vk,
                    user_id,
                    "К сожалению, этот интервал уже занят.\n\n"
                    + busy_text
                    + suggestion_text
                    + "\n\nНапишите другое время начала.",
                    keyboard=_back_keyboard(),
                )
                state.mode = "choosing_time"
                return
            _set_user_hours(user_id, remaining)
            _add_booking(user_id, state.start_dt, state.end_dt)
            send_message(
                vk,
                user_id,
                "Готово! Ваше бронирование подтверждено ✅\n\n"
                f"📅 Дата: {_format_date(state.start_dt.date())}\n"
                f"⏰ Время: {state.start_dt.strftime('%H:%M')}–{state.end_dt.strftime('%H:%M')}\n"
                f"🎫 Осталось на абонементе: {_format_hours_balance(remaining)}",
                keyboard=_main_keyboard_for(user_id),
            )
            STATES[user_id] = UserState(mode="idle")
            return

        if text_norm.lower() == "отмена":
            send_message(
                vk,
                user_id,
                "Бронирование отменено.",
                keyboard=_main_keyboard_for(user_id),
            )
            STATES[user_id] = UserState(mode="idle")
            return

        send_message(
            vk,
            user_id,
            "Нажмите «подтвердить» или «Нужен перерыв 15 минут», либо «отмена» / «⬅ Назад».",
            keyboard=_confirm_keyboard(
                with_break_button=(
                    state.duration_hours is not None
                    and state.duration_hours >= 2
                    and abs(float(state.duration_hours) - int(round(float(state.duration_hours)))) < 1e-9
                )
            ),
        )
        return

    # если состояние неожиданное — возвращаем в начало
    handle_start(vk, user_id)


def main() -> None:
    if not VK_TOKEN:
        raise RuntimeError("Не указан токен ВК бота. Установите VK_RENT_BOT_TOKEN.")

    _init_db()

    # Защита от "засыпания" longpoll: при сетевых ошибках переподключаемся.
    # systemd тоже перезапустит процесс, но лучше уметь восстанавливаться без падения.
    backoff = 5
    while True:
        try:
            session = vk_api.VkApi(token=VK_TOKEN)
            vk = session.get_api()
            # preload_messages: иначе Long Poll часто не присылает вложения — скрин оплаты не виден, бот зацикливается
            longpoll = VkLongPoll(session, preload_messages=True)

            logger.info("VK rent bot started (%s). Waiting for messages...", BOT_VERSION)
            backoff = 5

            for event in longpoll.listen():
                if event.type == VkEventType.MESSAGE_NEW and event.to_me:
                    try:
                        handle_message(vk, event)
                    except Exception as e:
                        logger.exception(
                            "Ошибка при обработке сообщения от %s: %s",
                            event.user_id,
                            e,
                        )
                        try:
                            send_message(
                                vk,
                                event.user_id,
                                "Произошла ошибка при обработке запроса. Попробуйте ещё раз команду /start.",
                            )
                        except Exception:
                            # Если сеть отвалилась — просто продолжим, переподключение сработает выше
                            pass
        except Exception as e:
            logger.warning("Longpoll reconnect due to error: %s", e)
            time.sleep(backoff)
            backoff = min(backoff * 2, 60)


if __name__ == "__main__":
    main()

