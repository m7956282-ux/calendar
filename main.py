import base64
import json
import logging
import os
import re
import sqlite3
from datetime import datetime, timedelta, time, timezone
from typing import Final, Dict, List, Optional, Tuple, Union

from openai import AsyncOpenAI
from telegram import Update, ReplyKeyboardMarkup, KeyboardButton, ReplyKeyboardRemove, InlineKeyboardMarkup, InlineKeyboardButton
from telegram.ext import (
    ApplicationBuilder,
    CallbackQueryHandler,
    CommandHandler,
    ContextTypes,
    MessageHandler,
    ConversationHandler,
    filters,
)

# Этапы диалога
ASK_NAME, ASK_BIRTHDATE, ASK_BIRTHTIME, CHECK_SUB_WEEK, CHECK_SUB_DAY, ASK_TAROT_TOPIC, ASK_COMPAT_TOPIC = range(7)

# Токен бота только из окружения (файл .env не коммитить; токен в чат/репозиторий не вставлять)
BOT_TOKEN: Final[str] = (os.getenv("BOT_TOKEN") or os.getenv("TELEGRAM_BOT_TOKEN") or "").strip()

# Запоминаем, какой гороскоп был отправлен для каждого чата и недели,
# чтобы в течение одной недели текст не менялся.
LAST_HOROSCOPE_INDEX: Dict[Tuple[int, str], int] = {}

# Запоминаем, какой гороскоп на день был отправлен для каждого чата и даты,
# чтобы в течение дня текст не менялся.
LAST_DAILY_HOROSCOPE_INDEX: Dict[Tuple[int, str], int] = {}

# Соляр на год: для каждого чата и года — один и тот же текст.
LAST_SOLAR_HOROSCOPE_INDEX: Dict[Tuple[int, str], int] = {}

# Кэш текстов, сгенерированных DeepSeek (один и тот же период — тот же текст).
CACHED_DEEPSEEK_WEEK: Dict[Tuple[int, str], str] = {}
CACHED_DEEPSEEK_DAY: Dict[Tuple[int, str], str] = {}
CACHED_DEEPSEEK_SOLAR: Dict[Tuple[int, str], str] = {}

# Ключ DeepSeek: задайте переменную окружения DEEPSEEK_API_KEY (в коде — запасной вариант)
DEEPSEEK_API_KEY: Final[str] = (os.getenv("DEEPSEEK_API_KEY") or "REDACTED_DEEPSEEK_KEY").strip()
DEEPSEEK_BASE_URL: Final[str] = "https://api.deepseek.com"
DEEPSEEK_MODEL: Final[str] = os.getenv("DEEPSEEK_MODEL", "deepseek-chat")
# Модель с поддержкой изображений для проверки скриншота (если есть)
DEEPSEEK_VISION_MODEL: Final[str] = os.getenv("DEEPSEEK_VISION_MODEL", "deepseek-chat")
TAROT_DELAY_MINUTES: int = 30
# Сколько последних пар «пользователь — Мария» передавать в API (история диалога)
MARIA_CHAT_HISTORY_LEN: int = 10  # 5 обменов
# Хранить в БД последних N сообщений на пользователя (поддержка диалога месяцами/год)
MARIA_PERSISTED_HISTORY_LEN: int = 30  # 15 обменов в SQLite
MARIA_HISTORY_DB: str = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "data", "maria_history.db"
)
# Канал Telegram для ежедневных постов и проверки подписки (бесплатные гороскопы)
# CHANNEL_ID в env: число 851438116, полный id -100851438116 или @username канала; пусто/0 = отключено
_raw_channel = (os.getenv("CHANNEL_ID") or "851438116").strip()
if not _raw_channel or _raw_channel == "0":
    CHANNEL_CHAT_ID: Optional[Union[int, str]] = None
elif _raw_channel.startswith("@"):
    CHANNEL_CHAT_ID = _raw_channel
elif _raw_channel.startswith("-"):
    CHANNEL_CHAT_ID = int(_raw_channel)
else:
    _num = int(_raw_channel)
    # В API Telegram каналы имеют id вида -100xxxxxxxxxx
    CHANNEL_CHAT_ID = -(100_000_000_000 + _num) if _num > 0 else _num
CHANNEL_USERNAME: Final[str] = os.getenv("CHANNEL_USERNAME", "astrolog_maria1")
MSK_TZ = timezone(timedelta(hours=3))

logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)

BACK_TO_MENU_BUTTON: Final[str] = "Вернуться в меню"


def _main_menu_keyboard() -> List[List[KeyboardButton]]:
    """Кнопки главного меню (гороскопы, соляр, Таро, натальная карта, чат, обо мне, помощь)."""
    return [
        [KeyboardButton("Гороскоп на неделю"), KeyboardButton("Гороскоп на день")],
        [KeyboardButton("Соляр на год"), KeyboardButton("Расклад Таро")],
        [KeyboardButton("Натальная карта"), KeyboardButton("Совместимость по зодиаку")],
        [KeyboardButton("Чат с Марией")],
        [KeyboardButton("Обо мне"), KeyboardButton("Помощь")],
    ]


def _maria_db_connect() -> sqlite3.Connection:
    """Подключение к БД истории чата с Марией."""
    os.makedirs(os.path.dirname(MARIA_HISTORY_DB) or ".", exist_ok=True)
    conn = sqlite3.connect(MARIA_HISTORY_DB)
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS maria_history (
            user_id INTEGER PRIMARY KEY,
            history_json TEXT NOT NULL DEFAULT '[]',
            updated_at TEXT NOT NULL
        )
        """
    )
    conn.commit()
    return conn


def get_maria_history(user_id: int) -> List[Dict[str, str]]:
    """Загрузить историю чата с Марией для пользователя (последние N сообщений)."""
    try:
        conn = _maria_db_connect()
        row = conn.execute(
            "SELECT history_json FROM maria_history WHERE user_id = ?", (user_id,)
        ).fetchone()
        conn.close()
        if row:
            data = json.loads(row[0])
            return data[-MARIA_PERSISTED_HISTORY_LEN:] if data else []
    except Exception as e:
        logger.warning("get_maria_history error: %s", e)
    return []


def set_maria_history(user_id: int, history: List[Dict[str, str]]) -> None:
    """Сохранить историю чата с Марией для пользователя."""
    try:
        to_save = history[-MARIA_PERSISTED_HISTORY_LEN:]
        conn = _maria_db_connect()
        conn.execute(
            """
            INSERT INTO maria_history (user_id, history_json, updated_at)
            VALUES (?, ?, ?)
            ON CONFLICT(user_id) DO UPDATE SET
                history_json = excluded.history_json,
                updated_at = excluded.updated_at
            """,
            (user_id, json.dumps(to_save, ensure_ascii=False), datetime.utcnow().isoformat()),
        )
        conn.commit()
        conn.close()
    except Exception as e:
        logger.warning("set_maria_history error: %s", e)


def get_zodiac_sign(birthdate: datetime) -> str:
    """Определение знака зодиака по дате рождения (тропический зодиак)."""
    day = birthdate.day
    month = birthdate.month

    if (month == 3 and day >= 21) or (month == 4 and day <= 19):
        return "Овен"
    if (month == 4 and day >= 20) or (month == 5 and day <= 20):
        return "Телец"
    if (month == 5 and day >= 21) or (month == 6 and day <= 20):
        return "Близнецы"
    if (month == 6 and day >= 21) or (month == 7 and day <= 22):
        return "Рак"
    if (month == 7 and day >= 23) or (month == 8 and day <= 22):
        return "Лев"
    if (month == 8 and day >= 23) or (month == 9 and day <= 22):
        return "Дева"
    if (month == 9 and day >= 23) or (month == 10 and day <= 22):
        return "Весы"
    if (month == 10 and day >= 23) or (month == 11 and day <= 21):
        return "Скорпион"
    if (month == 11 and day >= 22) or (month == 12 and day <= 21):
        return "Стрелец"
    if (month == 12 and day >= 22) or (month == 1 and day <= 19):
        return "Козерог"
    if (month == 1 and day >= 20) or (month == 2 and day <= 18):
        return "Водолей"
    return "Рыбы"


async def _generate_horoscope_with_deepseek(
    mode: str,
    name: str,
    sign: str,
    birthdate: datetime,
    birth_time: Optional[str] = None,
) -> Optional[str]:
    """
    Генерирует текст гороскопа через DeepSeek API.
    mode: "week" | "day" | "solar"
    Возвращает полный готовый текст сообщения или None при ошибке.
    """
    if not DEEPSEEK_API_KEY:
        return None

    period_desc = {
        "week": "на текущую неделю (с понедельника по воскресенье)",
        "day": "на сегодня",
        "solar": "соляр на текущий год (основные темы года от дня рождения до дня рождения)",
    }.get(mode, "на период")

    prompt = (
        f"Ты — астролог. Напиши персональный гороскоп {period_desc} для человека.\n\n"
        f"Имя: {name}. Знак зодиака: {sign}. Дата рождения: {birthdate.strftime('%d.%m.%Y')}."
    )
    if mode == "solar" and birth_time:
        prompt += f" Время рождения: {birth_time}."
    if mode == "day":
        prompt += (
            "\n\nТребования: пиши на русском, тёплым поддерживающим тоном. "
            "Объём основного текста — примерно 700–1200 символов (короче обычного недельного гороскопа). "
            "Используй эмодзи в меру (✨🔮💫 и т.п.). Не используй звёздочки для выделения (никаких ** в тексте). "
            "Сфокусируйся на сегодняшнем дне: настроении, ключевых подсказках и одной‑двух рекомендациях. "
            "Не пиши в конце блок «Важно помнить» и подписи — их добавит бот автоматически."
        )
    elif mode == "week":
        prompt += (
            "\n\nТребования: пиши на русском, тёплым поддерживающим тоном. "
            "Объём основного текста — примерно 2000–3000 символов. "
            "Сделай текст развёрнутым: общая энергия недели, отдельные блоки для отношений, работы/денег, "
            "личного состояния и советов. Используй эмодзи в меру (✨🔮💫 и т.п.). Не используй звёздочки для выделения (никаких ** в тексте). "
            "Не пиши в конце блок «Важно помнить» и подписи — их добавит бот автоматически."
        )
    else:
        prompt += (
            "\n\nТребования: пиши на русском, тёплым поддерживающим тоном. "
            "Объём основного текста — примерно 1500–2500 символов. "
            "Используй эмодзи в меру (✨🔮💫 и т.п.). Не используй звёздочки для выделения (никаких ** в тексте). "
            "Дай развёрнутые рекомендации: энергия периода, на что обратить внимание, "
            "советы по отношениям/здоровью/делам. Не пиши заголовки и подписи — только основной текст гороскопа."
        )

    try:
        client = AsyncOpenAI(
            api_key=DEEPSEEK_API_KEY,
            base_url=DEEPSEEK_BASE_URL,
        )
        response = await client.chat.completions.create(
            model=DEEPSEEK_MODEL,
            messages=[{"role": "user", "content": prompt}],
            max_tokens=2000,
        )
        body = (response.choices[0].message.content or "").strip()
        if not body:
            return None
        body = body.replace("**", "")  # убираем звёздочки выделения
        body = re.sub(r"^#+\s*", "", body, flags=re.MULTILINE)  # убираем ### ## # в начале строк

        # Собираем полное сообщение в том же формате, что и шаблоны
        if mode == "week":
            header = (
                "🌟 *Персональный гороскоп на неделю* 🌟\n\n"
                f"👤 Имя: {name}\n"
                f"♈️ Знак зодиака: {sign}\n"
                f"📅 Период: текущая неделя\n\n"
                "───────────────\n"
                "🔮 Общая энергия недели\n\n"
            )
            footer = (
                "\n\n───────────────\n"
                "💭 Важно помнить\n\n"
                "Гороскоп раскрывает вероятные тенденции, но более точную картину помогут увидеть разбор натальной "
                "карты, прогрессии и соляры. Заказать расклад Таро, разбор натальной карты и соляр на год можно в главном меню. ✨"
            )
        elif mode == "day":
            header = (
                "☀️ *Персональный гороскоп на день* ☀️\n\n"
                f"👤 Имя: {name}\n"
                f"♈️ Знак зодиака: {sign}\n"
                f"📅 Период: сегодня\n\n"
                "───────────────\n"
                "🔮 Энергия дня\n\n"
            )
            footer = (
                "\n\n───────────────\n"
                "💭 Важно помнить\n\n"
                "Гороскоп раскрывает вероятные тенденции, но более точную картину помогут увидеть разбор натальной "
                "карты, прогрессии и соляры. Заказать расклад Таро, разбор натальной карты и соляр на год можно в главном меню. ✨"
            )
        else:  # solar
            year_str = datetime.utcnow().strftime("%Y")
            header = (
                "🌅 *Соляр на год* 🌅\n\n"
                f"👤 Имя: {name}\n"
                f"♈️ Знак зодиака: {sign}\n"
                f"📅 Дата рождения: {birthdate.strftime('%d.%m.%Y')}\n"
            )
            if birth_time:
                header += f"🕐 Время рождения: {birth_time}\n"
            header += f"📅 Год соляра: {year_str}\n\n───────────────\n🔮 Общая картина года\n\n"
            footer = (
                "\n\n───────────────\n"
                "💭 Важно помнить\n\n"
                "Соляр описывает основные темы года от дня рождения до дня рождения. "
                "Для точной трактовки учитываются натальная карта, дом соляра и планеты. ✨"
            )

        return header + body + footer
    except Exception as e:
        logger.warning("DeepSeek API error: %s", e)
        return None


def _build_maria_system_prompt() -> str:
    return (
        "Ты — Мария, астролог. Отвечай тепло, по‑человечески и максимально «живым» тоном. От первого лица.\n\n"
        "Стиль: приветствие «Здравствуйте, я Мария» только в первом ответе (добавляется автоматически). В следующих сообщениях не представляйся и не повторяй одно и то же.\n\n"
        "Главное: сначала диалог и помощь человеку, потом — только если уместно — мягкое предложение услуги.\n"
        "- Не «продавай» в каждом ответе.\n"
        "- Задавай 1 уточняющий вопрос за раз.\n"
        "- Показывай эмпатию и сочувствие, можно лёгкие шутки по контексту (без токсичности).\n"
        "- Говори простыми словами, как человек в чате, без канцелярита.\n\n"
        "Услуги (упоминать только когда уместно): натальная карта, соляр на год, расклад Таро, совместимость по зодиаку.\n"
        "Если человек просит услугу напрямую или сам спрашивает про стоимость/как заказать — тогда дай условия оплаты: сумма, 30 минут, 89124566686 Альфа‑банк, скриншот в чат.\n\n"
        "Важно: не зацикливайся. Если в истории ты уже спрашивала тему/данные, и человек ответил — это его ответ. Не проси повторять.\n\n"
        "Тебе передаётся история переписки с этим клиентом (если есть). Опирайся на неё: помни прошлые темы и детали, чтобы диалог был непрерывным."
    )


async def _chat_with_deepseek(
    user_message: str,
    history: Optional[List[Dict[str, str]]] = None,
) -> Optional[str]:
    """Отправляет сообщение в DeepSeek от имени Марии. history — список {"role": "user"|"assistant", "content": "..."}."""
    if not DEEPSEEK_API_KEY or not user_message.strip():
        return None
    try:
        client = AsyncOpenAI(
            api_key=DEEPSEEK_API_KEY,
            base_url=DEEPSEEK_BASE_URL,
        )
        messages: List[Dict[str, str]] = [
            {"role": "system", "content": _build_maria_system_prompt()},
        ]
        if history:
            messages.extend(history[-MARIA_CHAT_HISTORY_LEN:])  # последние N пар в API
        messages.append({"role": "user", "content": user_message.strip()})
        response = await client.chat.completions.create(
            model=DEEPSEEK_MODEL,
            messages=messages,
            max_tokens=1500,
        )
        return (response.choices[0].message.content or "").strip()
    except Exception as e:
        logger.warning("DeepSeek chat error: %s", e)
        return None


async def _validate_payment_screenshot(image_bytes: bytes) -> Tuple[bool, str]:
    """
    Сейчас принимаем любой скриншот без проверки через нейросеть.
    Возвращаем (True, "") для любого непустого изображения.
    """
    if not image_bytes:
        return False, "Изображение не найдено. Отправьте скриншот ещё раз."
    return True, ""


async def _generate_tarot_reading(topic: str) -> Optional[str]:
    """Генерирует текст расклада Таро по теме через DeepSeek (от имени Марии)."""
    if not DEEPSEEK_API_KEY or not topic.strip():
        return None
    try:
        client = AsyncOpenAI(
            api_key=DEEPSEEK_API_KEY,
            base_url=DEEPSEEK_BASE_URL,
        )
        response = await client.chat.completions.create(
            model=DEEPSEEK_MODEL,
            messages=[
                {
                    "role": "system",
                    "content": (
                        "Ты — Мария, астролог. Ты делаешь расклад Таро и присылаешь клиенту готовый результат. "
                        "Напиши расклад на русском: кратко опиши, что показывают карты по теме клиента, "
                        "дай совет и поддержку. Объём 400–800 символов. От первого лица, тепло. "
                        "Не пиши названия карт, если не уверена — просто интерпретацию и совет."
                    ),
                },
                {
                    "role": "user",
                    "content": f"Тема/вопрос клиента для расклада Таро: {topic.strip()}",
                },
            ],
            max_tokens=1000,
        )
        return (response.choices[0].message.content or "").strip()
    except Exception as e:
        logger.warning("DeepSeek Tarot error: %s", e)
        return None


async def _generate_channel_daily_for_sign(sign: str) -> Optional[str]:
    """Короткий гороскоп на день для канала, для одного знака зодиака."""
    if not DEEPSEEK_API_KEY:
        return None
    try:
        client = AsyncOpenAI(
            api_key=DEEPSEEK_API_KEY,
            base_url=DEEPSEEK_BASE_URL,
        )
        response = await client.chat.completions.create(
            model=DEEPSEEK_MODEL,
            messages=[
                {
                    "role": "system",
                    "content": (
                        "Ты — астролог и пишешь ежедневный гороскоп для канала в Telegram.\n"
                        "Нужно по одному абзацу для каждого знака, без обращения по имени.\n"
                        "Тон тёплый и поддерживающий, без страшилок. Пиши кратко: 300–500 символов.\n"
                        "Фокус на сегодняшнем дне: настроение, ключевые подсказки и 1–2 конкретных рекомендации.\n"
                        "Добавляй больше эмодзи в текст (1–3 в каждом предложении там, где это уместно), но не превращай сообщение в сплошную череду символов."
                    ),
                },
                {
                    "role": "user",
                    "content": f"Короткий гороскоп на сегодня для знака {sign}.",
                },
            ],
            max_tokens=400,
        )
        return (response.choices[0].message.content or "").strip()
    except Exception as e:
        logger.warning("DeepSeek channel daily error (%s): %s", sign, e)
        return None


async def _send_daily_channel_post(context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    Ежедневный пост в канал: 12 коротких гороскопов по знакам в 8:00 по местному времени.
    Для каждого знака делается отдельное сообщение.
    """
    logger.info("Запуск ежедневного поста в канал (гороскопы)...")
    if not DEEPSEEK_API_KEY or not CHANNEL_CHAT_ID:
        logger.warning("Ежедневный пост (гороскопы) пропущен: не задан DEEPSEEK_API_KEY или CHANNEL_ID")
        return
    signs = [
        ("Овен", "♈"),
        ("Телец", "♉"),
        ("Близнецы", "♊"),
        ("Рак", "♋"),
        ("Лев", "♌"),
        ("Дева", "♍"),
        ("Весы", "♎"),
        ("Скорпион", "♏"),
        ("Стрелец", "♐"),
        ("Козерог", "♑"),
        ("Водолей", "♒"),
        ("Рыбы", "♓"),
    ]
    today = datetime.now(MSK_TZ).strftime("%d.%m.%Y")
    header = f"☀️ Гороскоп на сегодня\n\n📅 {today}"

    # Собираем все знаки в один или два больших поста, не превышая лимит Telegram.
    blocks: List[str] = []
    for sign_name, emoji in signs:
        text = await _generate_channel_daily_for_sign(sign_name)
        if not text:
            continue
        block = f"{emoji} {sign_name}\n\n{text}"
        blocks.append(block)

    if not blocks:
        logger.warning("Ежедневный пост (гороскопы): не удалось сгенерировать ни одного знака")
        return

    # Формируем 1–2 сообщения с учётом TELEGRAM_MESSAGE_MAX_LENGTH.
    messages: List[str] = []
    current = header
    for block in blocks:
        candidate = current + "\n\n" + block
        if len(candidate) <= TELEGRAM_MESSAGE_MAX_LENGTH:
            current = candidate
        else:
            messages.append(current)
            current = f"☀️ Гороскоп на сегодня (продолжение)\n\n{block}"
    messages.append(current)

    # Ограничимся максимум двумя сообщениями, чтобы не засорять канал.
    messages = messages[:2]

    for msg in messages:
        try:
            await context.bot.send_message(chat_id=CHANNEL_CHAT_ID, text=msg)
        except Exception as e:
            logger.warning("Failed to send daily post chunk: %s", e)
    logger.info("Ежедневный пост (гороскопы) завершён")


async def _generate_astrological_events_post() -> Optional[str]:
    """Генерирует пост про ближайшие астрологические события (обзор) через DeepSeek."""
    if not DEEPSEEK_API_KEY:
        return None
    now_msk = datetime.now(MSK_TZ).strftime("%d.%m.%Y %H:%M")
    try:
        client = AsyncOpenAI(
            api_key=DEEPSEEK_API_KEY,
            base_url=DEEPSEEK_BASE_URL,
        )
        response = await client.chat.completions.create(
            model=DEEPSEEK_MODEL,
            messages=[
                {
                    "role": "system",
                    "content": (
                        "Ты — астролог и пишешь пост в Telegram-канал про предстоящие астрологические события.\n"
                        "Формат: 4–7 пунктов списком (•), каждый пункт — событие + коротко что это значит и совет.\n"
                        "Объём: 1200–2200 символов. Тон: уверенно, тепло, без запугивания. Эмодзи используй щедро, по 1–3 на предложение там, где они помогают передать настроение.\n"
                        "Не используй Markdown-заголовки (#) и не используй **. Не добавляй ссылок."
                    ),
                },
                {
                    "role": "user",
                    "content": (
                        f"Сейчас {now_msk} (МСК). Напиши обзор ближайших астрологических событий на 3–5 дней вперёд "
                        "для широкой аудитории. Если не уверен в точных датах/времени — формулируй как тенденции "
                        "и не выдумывай точные градусы."
                    ),
                },
            ],
            max_tokens=1200,
        )
        text = (response.choices[0].message.content or "").strip()
        text = text.replace("**", "")
        text = re.sub(r"^#+\s*", "", text, flags=re.MULTILINE)
        return text
    except Exception as e:
        logger.warning("DeepSeek events post error: %s", e)
        return None


async def _send_astrological_events_post(context: ContextTypes.DEFAULT_TYPE) -> None:
    """Ежедневный пост в канал: ближайшие астрологические события."""
    logger.info("Запуск ежедневного поста (астро-события)...")
    if not DEEPSEEK_API_KEY or not CHANNEL_CHAT_ID:
        logger.warning("Ежедневный пост (астро-события) пропущен: не задан DEEPSEEK_API_KEY или CHANNEL_ID")
        return
    text = await _generate_astrological_events_post()
    if not text:
        logger.warning("Ежедневный пост (астро-события): не удалось сгенерировать текст")
        return
    header_date = datetime.now(MSK_TZ).strftime("%d.%m.%Y")
    msg = f"🌙 Астрологические события — ближайшие дни\n\n📅 {header_date}\n\n{text}"
    try:
        await context.bot.send_message(chat_id=CHANNEL_CHAT_ID, text=msg)
        logger.info("Ежедневный пост (астро-события) отправлен в канал")
    except Exception as e:
        logger.warning("Failed to send events post: %s", e)


# Команда для ручной отправки постов в канал (например, если бот запущен после 08:00 МСК)
ADMIN_TELEGRAM_ID: Final[int] = int(os.getenv("ADMIN_TELEGRAM_ID", "0"))


async def cmd_send_daily_posts(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Отправить ежедневные посты в канал сейчас (доступно только при заданном ADMIN_TELEGRAM_ID)."""
    if not ADMIN_TELEGRAM_ID:
        await update.message.reply_text("Ручная отправка постов отключена. Задайте ADMIN_TELEGRAM_ID на сервере.")
        return
    if not update.effective_user or update.effective_user.id != ADMIN_TELEGRAM_ID:
        await update.message.reply_text("Команда недоступна.")
        return
    if not CHANNEL_CHAT_ID:
        await update.message.reply_text("CHANNEL_ID не задан.")
        return
    await update.message.reply_text("Отправляю посты в канал...")
    try:
        await _send_daily_channel_post(context)
        await _send_astrological_events_post(context)
        await update.message.reply_text("Посты отправлены.")
    except Exception as e:
        logger.exception("Ошибка при ручной отправке постов: %s", e)
        await update.message.reply_text(f"Ошибка: {e}")


# Максимальная длина одного сообщения в Telegram
TELEGRAM_MESSAGE_MAX_LENGTH: int = 4096


async def _generate_solar_paid(order_data: str) -> Optional[str]:
    """Генерирует платный соляр на год через DeepSeek, объём около 10000 символов."""
    if not DEEPSEEK_API_KEY or not order_data.strip():
        return None
    try:
        client = AsyncOpenAI(
            api_key=DEEPSEEK_API_KEY,
            base_url=DEEPSEEK_BASE_URL,
        )
        response = await client.chat.completions.create(
            model=DEEPSEEK_MODEL,
            messages=[
                {
                    "role": "system",
                    "content": (
                        "Ты — Мария, астролог. Ты составляешь персональный соляр на год и присылаешь клиенту готовый результат. "
                        "Напиши развёрнутый соляр на русском по данным клиента (дата, время, место рождения — если указаны). "
                        "Объём текста: примерно 10000 символов. Структурируй: общая картина года, ключевые темы по сферам жизни "
                        "(отношения, карьера, здоровье, финансы, личный рост), рекомендации по месяцам или кварталам, важные даты и периоды. "
                        "Пиши тёплым поддерживающим тоном, от первого лица (как Мария). Используй эмодзи уместно (✨🌅🔮). "
                        "Не пиши заголовки вроде «Введение» — только содержательный текст соляра."
                    ),
                },
                {
                    "role": "user",
                    "content": f"Данные клиента для соляра на год: {order_data.strip()}",
                },
            ],
            max_tokens=4500,
        )
        return (response.choices[0].message.content or "").strip()
    except Exception as e:
        logger.warning("DeepSeek Solar paid error: %s", e)
        return None


async def _generate_natal_paid(order_data: str) -> Optional[str]:
    """Генерирует разбор натальной карты через DeepSeek по данным клиента."""
    if not DEEPSEEK_API_KEY or not order_data.strip():
        return None
    try:
        client = AsyncOpenAI(
            api_key=DEEPSEEK_API_KEY,
            base_url=DEEPSEEK_BASE_URL,
        )
        response = await client.chat.completions.create(
            model=DEEPSEEK_MODEL,
            messages=[
                {
                    "role": "system",
                    "content": (
                        "Ты — Мария, астролог. Ты составляешь разбор натальной карты и присылаешь клиенту готовый результат. "
                        "Напиши развёрнутый разбор на русском по данным клиента (дата, время рождения — если указаны). "
                        "Объём текста: примерно 3000–6000 символов. Опиши основные элементы натальной карты: знак и дом Солнца, Луны, "
                        "восходящий знак (если время есть), ключевые планеты и аспекты, сильные и слабые стороны, рекомендации. "
                        "Пиши тёплым поддерживающим тоном, от первого лица (как Мария). Используй эмодзи уместно (✨🔮💫), "
                        "но не используй звёздочки ** для выделения текста. "
                        "Не пиши заголовки вроде «Введение» — только содержательный текст разбора."
                    ),
                },
                {
                    "role": "user",
                    "content": f"Данные клиента для натальной карты: {order_data.strip()}",
                },
            ],
            max_tokens=3500,
        )
        text = (response.choices[0].message.content or "").strip()
        # Убираем возможные звёздочки и решётки Markdown, чтобы не было **Заголовков** и ### Заголовков
        text = text.replace("**", "")
        text = re.sub(r"^#+\s*", "", text, flags=re.MULTILINE)
        return text
    except Exception as e:
        logger.warning("DeepSeek Natal paid error: %s", e)
        return None


async def _generate_compat_paid(order_data: str) -> Optional[str]:
    """Генерирует разбор совместимости по зодиаку (пара) через DeepSeek."""
    if not DEEPSEEK_API_KEY or not order_data.strip():
        return None
    try:
        client = AsyncOpenAI(
            api_key=DEEPSEEK_API_KEY,
            base_url=DEEPSEEK_BASE_URL,
        )
        response = await client.chat.completions.create(
            model=DEEPSEEK_MODEL,
            messages=[
                {
                    "role": "system",
                    "content": (
                        "Ты — Мария, астролог. Ты разбираешь совместимость двух людей по их зодиаку и общим данным. "
                        "Напиши развёрнутый разбор на русском: общая динамика пары, сильные стороны, зоны роста, "
                        "советы по общению и поддержке друг друга. Объём: примерно 2000–4000 символов. "
                        "Пиши тёплым поддерживающим тоном, от первого лица (как Мария). Используй эмодзи уместно (💕✨🔮), "
                        "но не используй звёздочки ** для выделения текста."
                    ),
                },
                {
                    "role": "user",
                    "content": f"Данные для проверки совместимости: {order_data.strip()}",
                },
            ],
            max_tokens=2500,
        )
        text = (response.choices[0].message.content or "").strip()
        text = text.replace("**", "")
        text = re.sub(r"^#+\s*", "", text, flags=re.MULTILINE)
        return text
    except Exception as e:
        logger.warning("DeepSeek Compat paid error: %s", e)
        return None


def _split_long_message(text: str, max_len: int = TELEGRAM_MESSAGE_MAX_LENGTH) -> List[str]:
    """Разбивает длинный текст на части не больше max_len (разрез по абзацам или по предложениям)."""
    if len(text) <= max_len:
        return [text] if text.strip() else []
    parts: List[str] = []
    rest = text
    while rest:
        if len(rest) <= max_len:
            parts.append(rest.strip())
            break
        chunk = rest[:max_len]
        last_break = max(chunk.rfind("\n\n"), chunk.rfind("\n"), chunk.rfind(". "))
        if last_break > max_len // 2:
            chunk = rest[: last_break + 1]
            rest = rest[last_break + 1 :].lstrip()
        else:
            rest = rest[max_len:]
        parts.append(chunk.strip())
    return [p for p in parts if p]


def generate_weekly_horoscope(
    name: str, birthdate: datetime, last_index: Optional[int] = None
) -> tuple[str, int]:
    """Простая заглушка гороскопа на неделю.

    Возвращает текст и индекс использованного варианта,
    чтобы следующий раз можно было избежать повтора.
    """
    base_texts = [
        (
            "✨ В ближайшую неделю для вас открывается мягкий, но очень важный период переосмысления. "
            "Вы можете заметить, что многие привычные ситуации начинают проигрываться по-новому, словно "
            "жизнь предлагает вам альтернативный сценарий. Важно не спешить, а наблюдать: какие люди тянутся "
            "к вам, какие, наоборот, отдаляются, какие темы снова и снова всплывают в разговорах и мыслях. "
            "Вселенная словно подсказывает, где вы готовы к росту, а где пора расстаться со старыми "
            "ожиданиями. Постарайтесь в течение недели находить время для тишины: короткие прогулки, 🌙 "
            "медленные утренние ритуалы, вечерние размышления перед сном. Именно в такие моменты вы сможете "
            "услышать интуитивные ответы на вопросы, которые давно носите в себе. Не бойтесь отпускать то, "
            "что перестало откликаться — на это место уже готово прийти что‑то более живое и настоящее. 💫 "
            "Во второй половине недели могут проявиться новые идеи или люди, которые помогут вам мягко выйти "
            "из старых сценариев. Обращайте внимание на то, где вы чувствуете облегчение и вдохновение — это "
            "подсказки, в каком направлении лучше всего двигаться сейчас."
        ),
        (
            "🤝 Эта неделя несёт вам энергию обновления в сфере отношений и общения. Вы можете неожиданно "
            "получить приглашение, предложение о сотрудничестве или просто тёплый знак внимания от человека, "
            "от которого давно не было новостей. Старайтесь быть открыты диалогу, но при этом мягко "
            "отстаивать свои личные границы: не соглашайтесь на то, что вызывает внутреннее напряжение. "
            "Если в недавнем прошлом были недосказанности или обиды, сейчас подходящее время аккуратно "
            "прояснить ситуацию, не обвиняя и не оправдываясь, а просто честно говоря о своих чувствах. "
            "Также неделя благоприятна для обучения, чтения, расширения кругозора. 📚 Любая новая идея, "
            "которая появится в этот период, может стать отправной точкой для более масштабных перемен в "
            "будущем, поэтому записывайте инсайты и прислушивайтесь к тому, что особенно вас вдохновляет. 💡 "
            "Постарайтесь не застревать в старых обидах: неделя больше про обновление контактов, чем про "
            "возвращение к прошлым конфликтам. Новые знакомства могут оказаться важнее, чем кажется на первый взгляд."
        ),
        (
            "💼 В ближайшие дни внимание будет смещаться в сферу работы, самореализации и материальных вопросов. "
            "Возможно, вы почувствуете внутренний импульс навести порядок в делах, документах, финансах, "
            "расписании. Это хорошее время, чтобы структурировать свои задачи, разложить по полочкам планы и 📊 "
            "обновить отношение к деньгам. Постарайтесь не действовать из состояния спешки или тревоги: "
            "чем спокойнее и системнее вы подойдёте к текущим обязанностям, тем больше устойчивости получите "
            "в долгосрочной перспективе. Возможны небольшие проверки на ответственность — ситуации, в которых "
            "нужно будет сделать выбор между сиюминутным комфортом и тем, что полезно для вас в будущем. ⚖️ "
            "Выбирая второе, вы усиливаете ощущение внутренней опоры и постепенно выходите на новый уровень "
            "профессональной и личной зрелости. 🌟 В течение недели могут приходить идеи о смене формата работы "
            "или распределения нагрузки — не отвергайте их сразу, запишите и дайте себе время всё обдумать."
        ),
        (
            "💚 Неделя подталкивает вас глубже заняться темами здоровья, энергии и эмоционального баланса. "
            "Организм может давать тонкие сигналы: усталость, желание больше спать, смену аппетита или "
            "настроения. Важно не игнорировать эти подсказки, а мягко подстроить режим под реальные "
            "потребности тела. Подойдут лёгкие практики: растяжка, дыхательные упражнения, прогулки на 🚶‍♀️ "
            "свежем воздухе, отказ от лишней нагрузки там, где вы привыкли всё тащить на себе. Также в этот "
            "период полезно освободить пространство вокруг — разобрать вещи, удалить лишнюю информацию, "
            "отпустить то, что психологически «захламляет» личное поле. Чем честнее вы признаете свои "
            "чувства и усталость, тем быстрее вернётся ощущение ясности и внутренней силы. 🌈 "
            "Середина недели подойдёт для небольших изменений в привычках — не радикальных диет, а мягких шагов "
            "к более бережному отношению к себе."
        ),
        (
            "🎨 Эта неделя несёт творческую и вдохновляющую энергию, даже если ваша деятельность напрямую не "
            "связана с искусством. Вы можете неожиданно поймать сильное желание создать что‑то своё: начать "
            "проект, изменить интерьер, придумать новый формат работы или отдыха. Важно не откладывать эти ✨ "
            "импульсы «на потом» — сделайте хотя бы маленький шаг навстречу идее, которая зажигает. "
            "Обратите внимание на знаки: совпадения, фразы из книг или фильмов, случайные встречи. Через них 🔮 "
            "жизнь как будто разговаривает с вами и подтверждает выбранное направление. Не бойтесь проявлять "
            "свою индивидуальность ярче обычного: сейчас именно ваша уникальность может стать ключом к новым "
            "возможностям. Под конец недели вероятно ощущение лёгкого подъёма и веры в то, что вы действительно "
            "можете выстроить реальность ближе к своим внутренним мечтам и ценностям. 🌟 "
            "Если появится желание делиться своим творчеством с другими, неделя поддержит любые шаги в этом направлении."
        ),
    ]
    # Если на эту неделю уже есть сохранённый индекс — используем его,
    # чтобы текст не менялся до следующего понедельника.
    if last_index is not None:
        idx = last_index
    else:
        # Номер недели по понедельникам (%W) для «недели с понедельника»
        week_number = int(datetime.utcnow().strftime("%W"))
        number = birthdate.day + birthdate.month + birthdate.year + week_number
        idx = (len(name) + number) % len(base_texts)

    sign = get_zodiac_sign(birthdate)

    text = (
        "🌟 *Персональный гороскоп на неделю* 🌟\n\n"
        f"👤 Имя: {name}\n"
        f"♈️ Знак зодиака: {sign}\n"
        f"📅 Период: текущая неделя\n\n"
        "───────────────\n"
        "🔮 Общая энергия недели\n\n"
        f"{base_texts[idx]}\n\n"
        "───────────────\n"
        "💭 Важно помнить\n\n"
        "Гороскоп раскрывает вероятные тенденции, но более точную картину помогут увидеть разбор натальной "
        "карты, прогрессии и соляры. Заказать расклад Таро, разбор натальной карты и соляр на год можно в главном меню. ✨"
    )
    return text, idx


def generate_daily_horoscope(
    name: str, birthdate: datetime, last_index: Optional[int] = None
) -> tuple[str, int]:
    """Гороскоп на день с фиксацией по дате и чату."""
    base_texts = [
        (
            "Сегодня Вселенная мягко замедляет ваш ритм, чтобы вы смогли услышать себя яснее. "
            "Обратите внимание на первые мысли после пробуждения и на те ситуации, которые повторяются в "
            "течение дня — это подсказки, куда направить энергию. Хорошо подойдут дела, требующие "
            "внимательности и аккуратности, а вот резкие решения лучше отложить. Старайтесь не спорить с тем, "
            "что уже произошло, а мягко подстроиться и найти в происходящем для себя ресурсный смысл."
        ),
        (
            "День несёт активную, живую энергию общения. Возможны неожиданные звонки, переписки, встречи, "
            "которые напомнят вам, что вы не одни и поддержка ближе, чем кажется. Не бойтесь проявляться "
            "инициативно: написать первым, задать вопрос, предложить идею. Через разговоры вы можете увидеть "
            "ситуацию с другой стороны и почувствовать облегчение там, где раньше было напряжение."
        ),
        (
            "Сегодня хорошо наводить порядок в делах и пространстве. Даже если энергии кажется немного, "
            "маленькие шаги — разобранный ящик, выполненная одна важная задача, отложенный лишний запрос — "
            "дадут ощущение контроля и внутренней опоры. Полезно расставить приоритеты и честно признаться "
            "себе, какие обязанности действительно ваши, а что уже пора делегировать или отпустить."
        ),
        (
            "День подходит для заботы о теле и эмоциональном фоне. Вы можете острее чувствовать усталость "
            "или перепады настроения, и это сигнал не к самокритике, а к бережности. Подойдут тёплый душ, "
            "спокойная прогулка, вкусная простая еда, приятная музыка. Чем мягче вы отнесётесь к себе, тем "
            "быстрее вернётся ощущение ясности и внутренней устойчивости."
        ),
        (
            "Сегодняшняя энергия особенно поддерживает творчество и любые занятия, где вы можете проявить "
            "индивидуальность. Это может быть работа, хобби, стиль одежды или даже способ вести диалог. "
            "Позвольте себе сделать что‑то по‑своему, не оглядываясь на ожидания окружающих. Даже небольшой "
            "личный штрих поможет почувствовать радость и вдохновение."
        ),
    ]

    if last_index is not None:
        idx = last_index
    else:
        day_number = int(datetime.utcnow().strftime("%j"))  # номер дня в году
        number = birthdate.day + birthdate.month + birthdate.year + day_number
        idx = (len(name) + number) % len(base_texts)

    sign = get_zodiac_sign(birthdate)

    text = (
        "☀️ *Персональный гороскоп на день* ☀️\n\n"
        f"👤 Имя: {name}\n"
        f"♈️ Знак зодиака: {sign}\n"
        f"📅 Период: сегодня\n\n"
        "───────────────\n"
        "🔮 Энергия дня\n\n"
        f"{base_texts[idx]}\n\n"
        "───────────────\n"
        "💭 Важно помнить\n\n"
        "Гороскоп раскрывает вероятные тенденции, но более точную картину помогут увидеть разбор натальной "
        "карты, прогрессии и соляры. Заказать расклад Таро, разбор натальной карты и соляр на год можно в главном меню. ✨"
    )

    return text, idx


def _parse_birth_time(text: str) -> Optional[Tuple[int, int]]:
    """Парсит время в формате ЧЧ:ММ или ЧЧ.ММ. Возвращает (часы, минуты) или None."""
    text = text.strip().replace(".", ":").replace(",", ":")
    for sep in (":", "."):
        if sep in text:
            parts = text.split(sep, 1)
            if len(parts) != 2:
                return None
            try:
                h, m = int(parts[0].strip()), int(parts[1].strip())
                if 0 <= h <= 23 and 0 <= m <= 59:
                    return (h, m)
            except ValueError:
                pass
            return None
    return None


def _parse_birth_date(text: str) -> Optional[datetime]:
    """
    Парсит дату рождения в свободном формате.
    Допускает варианты:
    - 25.10.1988, 25-10-1988, 25/10/1988, 25 10 1988
    - 25.10.88, 25-10-88, 251088, 25 10 88
    - 25101988
    Важно: берём только цифры, разделители не обязательны.
    Год можно указывать двумя последними цифрами (88 → 1988, 05 → 2005).
    """
    digits = re.sub(r"\D", "", text)
    if len(digits) == 8:
        # ДДММГГГГ
        try:
            day = int(digits[0:2])
            month = int(digits[2:4])
            year = int(digits[4:8])
            return datetime(year, month, day)
        except ValueError:
            return None
    if len(digits) == 6:
        # ДДММГГ
        try:
            day = int(digits[0:2])
            month = int(digits[2:4])
            yy = int(digits[4:6])
            year = 1900 + yy if yy >= 30 else 2000 + yy
            return datetime(year, month, day)
        except ValueError:
            return None
    return None


def generate_solar_horoscope(
    name: str,
    birthdate: datetime,
    last_index: Optional[int] = None,
    birth_time: Optional[str] = None,
) -> tuple[str, int]:
    """Соляр на год: текст фиксируется по году для данного чата."""
    base_texts = [
        (
            "🌅 Этот год по соляру для вас — время переосмысления ценностей и целей. Вы можете почувствовать "
            "желание что-то завершить, отпустить старые сценарии и освободить место для нового. Важно не спешить "
            "с глобальными решениями, а наблюдать: какие люди и дела остаются в фокусе, а что само отдаляется. "
            "Солярный год благоприятствует внутренней работе, обучению, поездкам и расширению кругозора. "
            "Доверяйте интуиции в выборе направления — к концу года станет ясно, куда вы реально движетесь. 💫"
        ),
        (
            "🌅 В этом солярном году на первый план выходят отношения, партнёрство и диалог с миром. Возможны "
            "важные встречи, новые союзы, перезагрузка старых связей. Уделите внимание честности в общении: "
            "говорите о своих границах и желаниях, не дожидаясь накопления обид. Год хорош для совместных "
            "проектов, переговоров и всего, что связано с «мы». Одиночество может смениться ощущением опоры, "
            "если вы откроетесь диалогу и поддержке. 🤝"
        ),
        (
            "🌅 Солярный год несёт сильный акцент на карьеру, статус и материальную реализацию. Вы можете "
            "почувствовать желание больше зарабатывать, менять сферу или формат работы, структурировать финансы. "
            "Действуйте последовательно: маленькие шаги и дисциплина дадут больший результат, чем рывки. "
            "Возможны проверки на ответственность — ситуации, где нужно выбрать между комфортом и долгосрочной "
            "целью. Выбирая цель, вы закладываете фундамент на следующие годы. 💼"
        ),
        (
            "🌅 Этот год по соляру фокусирует внимание на здоровье, режиме и эмоциональном балансе. Организм "
            "и психика могут сигналить о необходимости отдыха, смены ритма, пересмотра привычек. Полезно "
            "ввести простые практики: сон, питание, движение, границы в общении. Год подходит для того, чтобы "
            "разобрать «завалы» — вещи, дела, отношения, которые тянут энергию вниз. Чем честнее вы признаете "
            "усталость и потребности, тем больше сил появится для новых целей. 💚"
        ),
        (
            "🌅 Солярный год открывает творческую и личную реализацию. Вы можете ощутить сильное желание "
            "творить, менять образ жизни, проявляться ярче. Это время для проектов «от души», хобби, смены имиджа "
            "или формата работы. Не бойтесь выделяться и делать по-своему — ваша уникальность становится "
            "ресурсом. Важно не разбрасываться, а выбрать 1–2 главных направления и вкладываться в них. "
            "К концу года вы увидите ощутимые плоды личного выбора. 🌟"
        ),
    ]

    current_year = datetime.utcnow().strftime("%Y")
    if last_index is not None:
        idx = last_index
    else:
        number = birthdate.day + birthdate.month + birthdate.year + int(current_year)
        idx = (len(name) + number) % len(base_texts)

    sign = get_zodiac_sign(birthdate)

    header_lines = [
        "🌅 *Соляр на год* 🌅\n\n",
        f"👤 Имя: {name}\n",
        f"♈️ Знак зодиака: {sign}\n",
        f"📅 Дата рождения: {birthdate.strftime('%d.%m.%Y')}\n",
    ]
    if birth_time:
        header_lines.append(f"🕐 Время рождения: {birth_time}\n")
    header_lines.append(f"📅 Год соляра: {current_year}\n\n")

    text = (
        "".join(header_lines)
        + "───────────────\n"
        + "🔮 Общая картина года\n\n"
        + f"{base_texts[idx]}\n\n"
        + "───────────────\n"
        + "💭 Важно помнить\n\n"
        + "Соляр описывает основные темы года от дня рождения до дня рождения. "
        + "Для точной трактовки учитываются натальная карта, дом соляра и планеты. ✨"
    )
    return text, idx


async def show_main_menu(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Команда /start — показывает главное меню."""
    try:
        user = update.effective_user
        chat_id = update.effective_chat.id if update.effective_chat else None
        logger.info("Пользователь %s (%s) вызвал /start", user.id if user else "?", user.first_name if user else "?")

        context.user_data["astrologer_chat"] = False

        keyboard = _main_menu_keyboard()
        reply_markup = ReplyKeyboardMarkup(keyboard, resize_keyboard=True)

        text = (
            "Вы написали Марии.\n\n"
            "Выберите услугу ниже — и мы отправим вам результат в течение 30 минут."
        )

        if update.message:
            await update.message.reply_text(text, reply_markup=reply_markup)
        elif chat_id is not None:
            await context.bot.send_message(chat_id=chat_id, text=text, reply_markup=reply_markup)
        else:
            logger.warning("/start: не удалось определить чат для ответа")
    except Exception as e:
        logger.exception("Ошибка в show_main_menu: %s", e)
        if update.message:
            await update.message.reply_text("Произошла ошибка при открытии меню. Попробуйте ещё раз или напишите /start.")
        elif update.effective_chat:
            await context.bot.send_message(update.effective_chat.id, "Произошла ошибка. Попробуйте /start.")


def _channel_mention() -> str:
    uname = CHANNEL_USERNAME if CHANNEL_USERNAME.startswith("@") else f"@{CHANNEL_USERNAME}"
    return f"https://t.me/{uname.lstrip('@')}"


async def _is_channel_subscriber(bot, user_id: int) -> bool:
    """Проверяет, подписан ли пользователь на канал (бот должен быть админом в канале)."""
    chat_id = f"@{CHANNEL_USERNAME}" if not CHANNEL_USERNAME.startswith("@") else CHANNEL_USERNAME
    try:
        member = await bot.get_chat_member(chat_id=chat_id, user_id=user_id)
        return member.status in ("member", "administrator", "creator")
    except Exception as e:
        # Если бот не админ в канале или Telegram не дал проверить статус,
        # не блокируем бесплатные гороскопы — лучше продолжить, чем сломать сценарий.
        logger.warning("get_chat_member error (allowing): %s", e)
        return True


async def check_subscribe_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Обработка нажатия «Проверить подписку»: проверяем канал и переходим к запросу имени или остаёмся на месте."""
    query = update.callback_query
    await query.answer()
    data = (query.data or "").strip()
    if data not in ("sub_week", "sub_day"):
        return ConversationHandler.END
    user_id = update.effective_user.id if update.effective_user else 0
    if not await _is_channel_subscriber(context.bot, user_id):
        again_markup = InlineKeyboardMarkup(
            [[InlineKeyboardButton("Проверить подписку", callback_data=data)]]
        )
        await query.edit_message_text(
            "Пожалуйста, подпишитесь на канал и нажмите «Проверить подписку» снова.\n\n"
            f"Канал: {_channel_mention()}",
            reply_markup=again_markup,
        )
        return CHECK_SUB_WEEK if data == "sub_week" else CHECK_SUB_DAY
    mode = "week" if data == "sub_week" else "day"
    context.user_data["horoscope_mode"] = mode
    if mode == "week":
        text = "Сейчас подготовим ваш персональный гороскоп на неделю.\n\nСначала напишите, пожалуйста, как вас зовут."
    else:
        text = "Сейчас подготовим ваш персональный гороскоп на сегодня.\n\nСначала напишите, пожалуйста, как вас зовут."
    await query.edit_message_text(text)
    return ASK_NAME


async def check_subscribe_via_message_week(
    update: Update, context: ContextTypes.DEFAULT_TYPE
) -> int:
    """Проверка подписки по текстовому сообщению (недельный гороскоп)."""
    user_id = update.effective_user.id if update.effective_user else 0
    if not await _is_channel_subscriber(context.bot, user_id):
        link = _channel_mention()
        await update.message.reply_text(
            "Пожалуйста, подпишитесь на канал и нажмите кнопку или напишите любое сообщение.\n\n"
            f"Канал: {link}"
        )
        return CHECK_SUB_WEEK
    context.user_data["horoscope_mode"] = "week"
    await update.message.reply_text(
        "Сейчас подготовим ваш персональный гороскоп на неделю.\n\n"
        "Сначала напишите, пожалуйста, как вас зовут."
    )
    return ASK_NAME


async def check_subscribe_via_message_day(
    update: Update, context: ContextTypes.DEFAULT_TYPE
) -> int:
    """Проверка подписки по текстовому сообщению (дневной гороскоп)."""
    user_id = update.effective_user.id if update.effective_user else 0
    if not await _is_channel_subscriber(context.bot, user_id):
        link = _channel_mention()
        await update.message.reply_text(
            "Пожалуйста, подпишитесь на канал и нажмите кнопку или напишите любое сообщение.\n\n"
            f"Канал: {link}"
        )
        return CHECK_SUB_DAY
    context.user_data["horoscope_mode"] = "day"
    await update.message.reply_text(
        "Сейчас подготовим ваш персональный гороскоп на сегодня.\n\n"
        "Сначала напишите, пожалуйста, как вас зовут."
    )
    return ASK_NAME


async def start_horoscope_dialog(
    update: Update, context: ContextTypes.DEFAULT_TYPE
) -> int:
    """Запуск диалога получения недельного гороскопа (команда /horoscope)."""
    context.user_data["horoscope_mode"] = "week"
    chat_id = update.effective_chat.id
    week_key = datetime.now(timezone.utc).strftime("%Y-%W")
    dict_key = (chat_id, week_key)
    cached_week = CACHED_DEEPSEEK_WEEK.get(dict_key)
    if cached_week:
        keyboard = [[KeyboardButton(BACK_TO_MENU_BUTTON)]]
        reply_markup = ReplyKeyboardMarkup(keyboard, resize_keyboard=True)
        await update.message.reply_text(cached_week, reply_markup=reply_markup)
        return ConversationHandler.END

    link = _channel_mention()
    text = (
        "Чтобы получить бесплатный гороскоп на неделю, подпишитесь на наш канал:\n\n"
        f"{link}\n\n"
        "После подписки нажмите кнопку «Проверить подписку» или напишите любое сообщение."
    )
    reply_markup = InlineKeyboardMarkup(
        [[InlineKeyboardButton("Проверить подписку", callback_data="sub_week")]]
    )
    await update.message.reply_text(text, reply_markup=reply_markup)
    return CHECK_SUB_WEEK


async def start_daily_horoscope_dialog(
    update: Update, context: ContextTypes.DEFAULT_TYPE
) -> int:
    """Запуск диалога получения дневного гороскопа (из меню или по команде)."""
    context.user_data["horoscope_mode"] = "day"
    chat_id = update.effective_chat.id
    day_key = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    dict_key = (chat_id, day_key)
    cached_day = CACHED_DEEPSEEK_DAY.get(dict_key)
    if cached_day:
        keyboard = [[KeyboardButton(BACK_TO_MENU_BUTTON)]]
        reply_markup = ReplyKeyboardMarkup(keyboard, resize_keyboard=True)
        await update.message.reply_text(cached_day, reply_markup=reply_markup)
        return ConversationHandler.END

    link = _channel_mention()
    text = (
        "Чтобы получить бесплатный гороскоп на день, подпишитесь на наш канал:\n\n"
        f"{link}\n\n"
        "После подписки нажмите кнопку «Проверить подписку» или напишите любое сообщение."
    )
    reply_markup = InlineKeyboardMarkup(
        [[InlineKeyboardButton("Проверить подписку", callback_data="sub_day")]]
    )
    await update.message.reply_text(text, reply_markup=reply_markup)
    return CHECK_SUB_DAY


async def start_solar_horoscope_dialog(
    update: Update, context: ContextTypes.DEFAULT_TYPE
) -> int:
    """Запуск диалога получения соляра на год."""
    context.user_data["horoscope_mode"] = "solar"
    reply_markup = ReplyKeyboardMarkup(_main_menu_keyboard(), resize_keyboard=True)
    await update.message.reply_text(
        "Сейчас подготовим ваш соляр на текущий год.\n\n"
        "Напишите, пожалуйста, как вас зовут.",
        reply_markup=reply_markup,
    )
    return ASK_NAME


async def start_natal_dialog(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Запуск диалога заказа натальной карты (имя → дата → время → оплата 990 ₽)."""
    context.user_data["horoscope_mode"] = "natal"
    reply_markup = ReplyKeyboardMarkup(_main_menu_keyboard(), resize_keyboard=True)
    await update.message.reply_text(
        "Сейчас подготовим разбор вашей натальной карты.\n\n"
        "Напишите, пожалуйста, как вас зовут.",
        reply_markup=reply_markup,
    )
    return ASK_NAME


async def start_tarot_dialog(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Запуск платного расклада Таро (990 ₽): один вопрос — оплата — скриншот — результат через 30 мин."""
    reply_markup = ReplyKeyboardMarkup(_main_menu_keyboard(), resize_keyboard=True)
    await update.message.reply_text(
        "Расклад Таро — 990 рублей.\n\n"
        "Напишите тему или вопрос для расклада (например: отношения, работа, решение).",
        reply_markup=reply_markup,
    )
    return ASK_TAROT_TOPIC


async def finish_tarot_topic(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Принимает тему Таро и переводит на оплату (скриншот → расклад через 30 мин)."""
    topic = (update.message.text or "").strip()
    if not topic:
        await update.message.reply_text("Напишите, пожалуйста, тему или вопрос для расклада Таро.")
        return ASK_TAROT_TOPIC

    # Если вместо темы нажали кнопку меню — переключаемся
    if topic == "Гороскоп на неделю":
        return await start_horoscope_dialog(update, context)
    if topic == "Гороскоп на день":
        return await start_daily_horoscope_dialog(update, context)
    if topic == "Соляр на год":
        return await start_solar_horoscope_dialog(update, context)
    if topic == "Расклад Таро":
        return await start_tarot_dialog(update, context)
    if topic == "Натальная карта":
        return await start_natal_dialog(update, context)
    if topic == "Совместимость по зодиаку":
        return await start_compat_dialog(update, context)
    if topic == "Чат с Марией":
        await start_astrologer_chat(update, context)
        return ConversationHandler.END
    if topic == BACK_TO_MENU_BUTTON:
        await show_main_menu(update, context)
        return ConversationHandler.END
    context.user_data["order_topic"] = topic
    context.user_data["order_type"] = "tarot"
    context.user_data["awaiting_screenshot"] = True
    payment_text = (
        "Расклад Таро — 990 рублей. Оплата: 89124566686 (Альфа-банк). "
        "Отправьте скриншот перевода в этот чат — расклад будет готов в течение 30 минут."
    )
    keyboard = _main_menu_keyboard()
    reply_markup = ReplyKeyboardMarkup(keyboard, resize_keyboard=True)
    await update.message.reply_text(payment_text, reply_markup=reply_markup)
    return ConversationHandler.END


async def start_compat_dialog(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Запуск платной проверки совместимости по зодиаку (200 ₽)."""
    reply_markup = ReplyKeyboardMarkup(_main_menu_keyboard(), resize_keyboard=True)
    await update.message.reply_text(
        "Проверка совместимости по зодиаку — 200 рублей.\n\n"
        "Напишите имена и даты рождения в свободной форме (например: Мария 25.10.1988 и Алексей 03.06.1990).",
        reply_markup=reply_markup,
    )
    return ASK_COMPAT_TOPIC


async def finish_compat_topic(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Принимает данные для совместимости и переводит на оплату 200 ₽ (скриншот → разбор через 30 мин)."""
    topic = (update.message.text or "").strip()
    if not topic:
        await update.message.reply_text(
            "Напишите, пожалуйста, имена и даты рождения в свободной форме для проверки совместимости."
        )
        return ASK_COMPAT_TOPIC

    # Если вместо данных нажали кнопку меню — переключаемся
    if topic == "Гороскоп на неделю":
        return await start_horoscope_dialog(update, context)
    if topic == "Гороскоп на день":
        return await start_daily_horoscope_dialog(update, context)
    if topic == "Соляр на год":
        return await start_solar_horoscope_dialog(update, context)
    if topic == "Расклад Таро":
        return await start_tarot_dialog(update, context)
    if topic == "Натальная карта":
        return await start_natal_dialog(update, context)
    if topic == "Совместимость по зодиаку":
        return await start_compat_dialog(update, context)
    if topic == "Чат с Марией":
        await start_astrologer_chat(update, context)
        return ConversationHandler.END
    if topic == BACK_TO_MENU_BUTTON:
        await show_main_menu(update, context)
        return ConversationHandler.END
    context.user_data["order_topic"] = topic
    context.user_data["order_type"] = "compat"
    context.user_data["awaiting_screenshot"] = True
    payment_text = (
        "Проверка совместимости по зодиаку — 200 рублей. Оплата: 89124566686 (Альфа-банк). "
        "Отправьте скриншот перевода в этот чат — разбор будет готов в течение 30 минут."
    )
    keyboard = _main_menu_keyboard()
    reply_markup = ReplyKeyboardMarkup(keyboard, resize_keyboard=True)
    await update.message.reply_text(payment_text, reply_markup=reply_markup)
    return ConversationHandler.END

async def ask_age(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    raw_name = (update.message.text or "").strip()
    if not raw_name:
        await update.message.reply_text("Пожалуйста, напишите ваше имя текстом.")
        return ASK_NAME

    # Если пользователь нажал кнопку меню вместо ввода имени — переключаемся на выбранный раздел
    if raw_name == "Гороскоп на неделю":
        return await start_horoscope_dialog(update, context)
    if raw_name == "Гороскоп на день":
        return await start_daily_horoscope_dialog(update, context)
    if raw_name == "Соляр на год":
        return await start_solar_horoscope_dialog(update, context)
    if raw_name == "Расклад Таро":
        return await start_tarot_dialog(update, context)
    if raw_name == "Натальная карта":
        return await start_natal_dialog(update, context)
    if raw_name == "Совместимость по зодиаку":
        return await start_compat_dialog(update, context)
    if raw_name == BACK_TO_MENU_BUTTON:
        await show_main_menu(update, context)
        return ConversationHandler.END

    # Проверяем, похоже ли это на нормальное имя
    # Разрешаем буквы, пробелы и дефис, без цифр и прочих символов
    # Дополнительно отсеиваем "служебные" слова и названия кнопок, которые люди иногда пишут вместо имени
    lowered = raw_name.lower()
    button_phrases = [
        "гороскоп на неделю",
        "гороскоп на день",
        "соляр на год",
        "расклад таро",
        "натальная карта",
        "совместимость по зодиаку",
        "чат с марией",
        "обо мне",
        "помощь",
        "вернуться в меню",
    ]
    service_words = {"гороскоп", "соляр", "таро", "совместимость", "неделя", "день", "чат", "меню"}

    bad_pattern = not re.match(r"^[A-Za-zА-Яа-яЁё][A-Za-zА-Яа-яЁё\-\s]{1,29}$", raw_name)
    bad_button = any(phrase in lowered for phrase in button_phrases)
    bad_word = any(word in lowered.split() for word in service_words)

    if bad_pattern or bad_button or bad_word:
        await update.message.reply_text(
            "Кажется, это не совсем имя. Для гороскопа нужно ваше настоящее имя, чтобы звёзды сложились правильно. "
            "Напишите, пожалуйста, как к вам лучше обращаться — только буквы, можно с пробелом или дефисом."
        )
        return ASK_NAME

    # Нормализуем имя: убираем лишние пробелы и делаем первую букву каждого слова заглавной
    normalized = " ".join(part.capitalize() for part in raw_name.split())
    context.user_data["name"] = normalized

    # Сообщение «Запишу вас как X» только если имя изменили и оно на кириллице (не показываем для латиницы, чтобы не акцентировать ошибки вроде Vfif)
    has_cyrillic = any(c in normalized for c in "абвгдеёжзийклмнопрстуфхцчшщъыьэюяАБВГДЕЁЖЗИЙКЛМНОПРСТУФХЦЧШЩЪЫЬЭЮЯ")
    if raw_name != normalized and has_cyrillic:
        await update.message.reply_text(
            f"Запишу вас как {normalized} — так будет красивее и правильнее."
        )

    await update.message.reply_text(
        f"Приятно познакомиться, {normalized}! Теперь напишите, пожалуйста, вашу дату рождения. "
        "Примеры: 25.10.1988, 25-10-88, 25 10 88, 251088."
    )
    return ASK_BIRTHDATE


async def about(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    text = (
        "✨ Меня зовут Мария Теплова.\n\n"
        "Я занимаюсь астрологией более пяти лет. Мой путь начался после серьёзной автомобильной аварии, "
        "после которой я провела время в коме. Когда я пришла в сознание, мир стал ощущаться иначе — "
        "я начала замечать тонкие знаки, совпадения и внутренние подсказки, которые невозможно было "
        "списать на случайность.\n\n"
        "С тех пор астрология стала для меня не просто инструментом прогноза, а языком, на котором "
        "говорит с нами жизнь. В своей работе я мягко помогаю увидеть ваши опоры, ключевые уроки и "
        "направления роста, чтобы движения вперёд были бережными и осознанными. 💫"
    )
    keyboard = [[KeyboardButton(BACK_TO_MENU_BUTTON)]]
    reply_markup = ReplyKeyboardMarkup(keyboard, resize_keyboard=True)
    await update.message.reply_text(text, reply_markup=reply_markup)


async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    text = (
        "❓ Как я могу помочь\n\n"
        "Гороскопы и соляр:\n"
        "- «🔮 Гороскоп на неделю» — персональный обзор энергии недели.\n"
        "- «☀️ Гороскоп на день» — краткий настрой на текущий день.\n"
        "- «🌅 Соляр на год» — подробный разбор тем года от дня рождения до дня рождения.\n"
        "- «🃏 Расклад Таро» — персональный разбор вашей ситуации или вопроса.\n"
        "- «💕 Совместимость по зодиаку» — разбор динамики пары по знакам и данным.\n\n"
        "После выбора раздела бот попросит ввести имя, дату рождения, а для соляра — ещё и время.\n"
        "Для углублённых разборов, вопросов про отношения, работу и личный путь можно написать Марии лично.\n\n"
        "Чтобы задать вопрос напрямую астрологу, нажмите кнопку «Помощь» ниже — откроется чат в Telegram."
    )
    inline = InlineKeyboardMarkup(
        [[InlineKeyboardButton("Помощь", url="https://t.me/mariaastrolog7")]]
    )
    keyboard = [[KeyboardButton(BACK_TO_MENU_BUTTON)]]
    reply_markup = ReplyKeyboardMarkup(keyboard, resize_keyboard=True)
    await update.message.reply_text(text, reply_markup=reply_markup)
    # Отдельным сообщением — кнопка-ссылка
    await update.message.reply_text(
        "Если хотите пообщаться напрямую, нажмите кнопку ниже 👇",
        reply_markup=inline,
    )


async def start_astrologer_chat(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Включение режима «Чат с Марией»: ответы от имени Марии через DeepSeek."""
    context.user_data["astrologer_chat"] = True
    user_id = update.effective_user.id if update.effective_user else 0
    # Загружаем сохранённую историю, чтобы бот «помнил» клиента между сессиями и годами
    persisted = get_maria_history(user_id)
    context.user_data["maria_chat_history"] = persisted
    # Приветствие «Здравствуйте, я Мария» только если это первый диалог (истории нет)
    context.user_data["maria_first_reply"] = len(persisted) == 0
    await update.message.reply_text("⏳ Подключаюсь к диалогу…")
    await update.message.reply_text("Я на связи. Напишите, что вас волнует — отвечу.")


async def astrologer_chat_reply(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Обработка сообщений в режиме «Чат с Марией»."""
    if not context.user_data.get("astrologer_chat"):
        return
    text = (update.message.text or "").strip()
    if not text:
        return
    if text in ("/выход", "/start"):
        context.user_data["astrologer_chat"] = False
        context.user_data.pop("maria_chat_history", None)
        await show_main_menu(update, context)
        return
    if not DEEPSEEK_API_KEY:
        await update.message.reply_text(
            "Мария временно недоступна. Напишите позже или выберите другой раздел."
        )
        return
    await update.message.reply_text("⏳ Печатает...")
    user_id = update.effective_user.id if update.effective_user else 0
    # Берём историю из сессии или подгружаем из БД (если бот перезапускался)
    history: List[Dict[str, str]] = context.user_data.get("maria_chat_history") or get_maria_history(user_id)
    reply = await _chat_with_deepseek(text, history)
    if reply:
        history.append({"role": "user", "content": text})
        history.append({"role": "assistant", "content": reply})
        context.user_data["maria_chat_history"] = history[-MARIA_CHAT_HISTORY_LEN:]
        # Сохраняем историю в БД, чтобы помнить клиента при следующих заходах и после перезапуска бота
        set_maria_history(user_id, history)
        if context.user_data.get("maria_first_reply"):
            context.user_data["maria_first_reply"] = False
            if not reply.strip().lower().startswith("здравствуйте"):
                reply = "Здравствуйте, я Мария. " + reply
        await update.message.reply_text(reply)
        if "990" in reply and ("89124566686" in reply or "оплатить" in reply.lower()):
            context.user_data["order_topic"] = text
            context.user_data["awaiting_screenshot"] = True
            # Соляр: в сообщении есть дата ДД.ММ.ГГГГ или слово «соляр»
            is_solar = bool(re.search(r"\d{1,2}\.\d{1,2}\.\d{4}", text)) or "соляр" in text.lower()
            context.user_data["order_type"] = "solar" if is_solar else "tarot"
    else:
        await update.message.reply_text(
            "Не удалось получить ответ. Проверьте подключение или попробуйте позже. Для выхода — /start."
        )


async def _send_paid_order_job(context: ContextTypes.DEFAULT_TYPE) -> None:
    """Отложенная отправка платного заказа (Таро или соляр) через 30 минут."""
    job = context.job
    if not job or not job.data:
        return
    chat_id = job.data.get("chat_id")
    topic = job.data.get("topic", "")
    order_type = job.data.get("order_type", "tarot")
    if not chat_id:
        return
    if order_type == "solar":
        text_body = await _generate_solar_paid(topic)
        if text_body:
            header = "🌅 Ваш соляр на год\n\n"
            full = header + text_body
            for part in _split_long_message(full):
                await context.bot.send_message(chat_id=chat_id, text=part)
        else:
            await context.bot.send_message(
                chat_id=chat_id,
                text="Соляр готовится. Если что-то пошло не так — напишите сюда, разберёмся.",
            )
    elif order_type == "natal":
        text_body = await _generate_natal_paid(topic)
        if text_body:
            header = "🪐 Ваша натальная карта\n\n"
            full = header + text_body
            for part in _split_long_message(full):
                await context.bot.send_message(chat_id=chat_id, text=part)
        else:
            await context.bot.send_message(
                chat_id=chat_id,
                text="Натальная карта готовится. Если что-то пошло не так — напишите сюда, разберёмся.",
            )
    elif order_type == "compat":
        text_body = await _generate_compat_paid(topic)
        if text_body:
            header = "💕 Совместимость по зодиаку\n\n"
            full = header + text_body
            for part in _split_long_message(full):
                await context.bot.send_message(chat_id=chat_id, text=part)
        else:
            await context.bot.send_message(
                chat_id=chat_id,
                text="Разбор совместимости готовится. Если что-то пошло не так — напишите сюда, разберёмся.",
            )
    else:
        reading = await _generate_tarot_reading(topic)
        if reading:
            await context.bot.send_message(
                chat_id=chat_id,
                text=f"🔮 Ваш расклад Таро\n\n{reading}",
            )
        else:
            await context.bot.send_message(
                chat_id=chat_id,
                text="Расклад готовится. Если что-то пошло не так — напишите сюда, разберёмся.",
            )


async def astrologer_chat_photo_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Обработка фото: скриншот оплаты (из чата с Марией или после кнопки «Соляр на год») → проверка → отложенная отправка."""
    in_maria_chat = context.user_data.get("astrologer_chat")
    awaiting = context.user_data.get("awaiting_screenshot") and context.user_data.get("order_topic")
    if not in_maria_chat and not awaiting:
        return
    if not context.user_data.get("awaiting_screenshot") or not context.user_data.get("order_topic"):
        await update.message.reply_text(
            "Спасибо за изображение. Если вы отправили скриншот об оплате — напишите, пожалуйста, "
            "какую услугу заказывали (Таро / натальная карта / соляр), и я пришлю результат."
        )
        return

    # Скачиваем фото и проверяем, что это скриншот перевода на 990+ ₽
    try:
        photo = update.message.photo[-1]
        tg_file = await context.bot.get_file(photo.file_id)
        image_bytes = await tg_file.download_as_bytearray()
    except Exception as e:
        logger.warning("Photo download error: %s", e)
        await update.message.reply_text(
            "Не удалось загрузить изображение. Отправьте скриншот ещё раз, пожалуйста."
        )
        return

    is_valid, reason = await _validate_payment_screenshot(bytes(image_bytes))
    if not is_valid:
        await update.message.reply_text("Пожалуйста, отправьте скриншот перевода.")
        return

    topic = context.user_data.pop("order_topic", "")
    order_type = context.user_data.pop("order_type", "tarot")
    context.user_data.pop("awaiting_screenshot", None)
    chat_id = update.effective_chat.id

    if order_type == "solar":
        await update.message.reply_text(
            "Скриншот получила, спасибо. Ваш соляр на год будет готов через 30 минут."
        )
    elif order_type == "natal":
        await update.message.reply_text(
            "Скриншот получила, спасибо. Ваша натальная карта будет готова через 30 минут."
        )
    elif order_type == "compat":
        await update.message.reply_text(
            "Скриншот получила, спасибо. Разбор совместимости будет готов через 30 минут."
        )
    else:
        await update.message.reply_text(
            "Скриншот получила, спасибо. Ваш расклад Таро будет готов через 30 минут."
        )

    delay_seconds = TAROT_DELAY_MINUTES * 60
    if context.job_queue:
        context.job_queue.run_once(
            _send_paid_order_job,
            when=delay_seconds,
            data={"chat_id": chat_id, "topic": topic, "order_type": order_type},
            name=f"order_{order_type}_{chat_id}_{datetime.utcnow().timestamp()}",
        )
    else:
        logger.warning("job_queue not available, sending order immediately")
        if order_type == "solar":
            text_body = await _generate_solar_paid(topic)
            if text_body:
                full = "🌅 Ваш соляр на год\n\n" + text_body
                for part in _split_long_message(full):
                    await update.message.reply_text(part)
            else:
                await update.message.reply_text("Соляр готовится. Если не придёт — напишите сюда.")
        elif order_type == "natal":
            text_body = await _generate_natal_paid(topic)
            if text_body:
                full = "🪐 Ваша натальная карта\n\n" + text_body
                for part in _split_long_message(full):
                    await update.message.reply_text(part)
            else:
                await update.message.reply_text("Натальная карта готовится. Если не придёт — напишите сюда.")
        elif order_type == "compat":
            text_body = await _generate_compat_paid(topic)
            if text_body:
                full = "💕 Совместимость по зодиаку\n\n" + text_body
                for part in _split_long_message(full):
                    await update.message.reply_text(part)
            else:
                await update.message.reply_text("Разбор совместимости готовится. Если не придёт — напишите сюда.")
        else:
            reading = await _generate_tarot_reading(topic)
            if reading:
                await update.message.reply_text(f"🔮 Ваш расклад Таро\n\n{reading}")
            else:
                await update.message.reply_text("Расклад готовится. Если не придёт — напишите сюда.")


async def schedule_horoscope(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    text = update.message.text.strip()
    name = context.user_data.get("name", "Дорогой друг")

    # Если пользователь нажал кнопку меню вместо даты — переключаемся на выбранный раздел
    if text == "Гороскоп на неделю":
        return await start_horoscope_dialog(update, context)
    if text == "Гороскоп на день":
        return await start_daily_horoscope_dialog(update, context)
    if text == "Соляр на год":
        return await start_solar_horoscope_dialog(update, context)
    if text == "Расклад Таро":
        return await start_tarot_dialog(update, context)
    if text == "Натальная карта":
        return await start_natal_dialog(update, context)
    if text == "Совместимость по зодиаку":
        return await start_compat_dialog(update, context)
    if text == BACK_TO_MENU_BUTTON:
        await show_main_menu(update, context)
        return ConversationHandler.END

    birthdate = _parse_birth_date(text)
    if birthdate is None:
        await update.message.reply_text(
            "Пожалуйста, введите дату рождения цифрами. Примеры: 25.10.1988, 25-10-88, 25 10 88, 251088."
        )
        return ASK_BIRTHDATE

    context.user_data["birthdate"] = birthdate.strftime("%d.%m.%Y")

    chat_id = update.effective_chat.id
    mode = context.user_data.get("horoscope_mode", "week")

    if mode == "solar" or mode == "natal":
        await update.message.reply_text(
            "Напишите, пожалуйста, время рождения в формате ЧЧ:ММ (например, 14:30). "
            "Если точное время неизвестно, укажите приблизительное."
        )
        return ASK_BIRTHTIME

    sign = get_zodiac_sign(birthdate)

    if mode == "day":
        day_key = datetime.utcnow().strftime("%Y-%m-%d")
        dict_key = (chat_id, day_key)
        horoscope_text = CACHED_DEEPSEEK_DAY.get(dict_key)
        if horoscope_text is None and DEEPSEEK_API_KEY:
            await update.message.reply_text("⏳ Готовлю персональный гороскоп на день...")
            horoscope_text = await _generate_horoscope_with_deepseek(
                "day", name, sign, birthdate
            )
            if horoscope_text is not None:
                CACHED_DEEPSEEK_DAY[dict_key] = horoscope_text
        if horoscope_text is None:
            last_index_for_day = LAST_DAILY_HOROSCOPE_INDEX.get(dict_key)
            horoscope_text, used_index = generate_daily_horoscope(
                name, birthdate, last_index_for_day
            )
            LAST_DAILY_HOROSCOPE_INDEX[dict_key] = used_index
            CACHED_DEEPSEEK_DAY[dict_key] = horoscope_text
    else:
        week_key = datetime.utcnow().strftime("%Y-%W")
        dict_key = (chat_id, week_key)
        horoscope_text = CACHED_DEEPSEEK_WEEK.get(dict_key)
        if horoscope_text is None and DEEPSEEK_API_KEY:
            await update.message.reply_text("⏳ Готовлю персональный гороскоп на неделю...")
            horoscope_text = await _generate_horoscope_with_deepseek(
                "week", name, sign, birthdate
            )
            if horoscope_text is not None:
                CACHED_DEEPSEEK_WEEK[dict_key] = horoscope_text
        if horoscope_text is None:
            # Если DeepSeek не ответил или ключ не задан — используем резервный текст
            last_index_for_week = LAST_HOROSCOPE_INDEX.get(dict_key)
            horoscope_text, used_index = generate_weekly_horoscope(
                name, birthdate, last_index_for_week
            )
            LAST_HOROSCOPE_INDEX[dict_key] = used_index
            CACHED_DEEPSEEK_WEEK[dict_key] = horoscope_text

    keyboard = [[KeyboardButton(BACK_TO_MENU_BUTTON)]]
    reply_markup = ReplyKeyboardMarkup(keyboard, resize_keyboard=True)
    try:
        await update.message.reply_text(horoscope_text, reply_markup=reply_markup)
    except Exception as e:
        logger.warning("Failed to send horoscope message: %s", e)

    return ConversationHandler.END


async def handle_birthtime(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """По времени рождения переводит на оплату соляра или натальной карты (по режиму)."""
    text = (update.message.text or "").strip()
    # Если пользователь нажал кнопку меню вместо времени — переключаемся
    if text == "Гороскоп на неделю":
        return await start_horoscope_dialog(update, context)
    if text == "Гороскоп на день":
        return await start_daily_horoscope_dialog(update, context)
    if text == "Соляр на год":
        return await start_solar_horoscope_dialog(update, context)
    if text == "Расклад Таро":
        return await start_tarot_dialog(update, context)
    if text == "Натальная карта":
        return await start_natal_dialog(update, context)
    if text == "Совместимость по зодиаку":
        return await start_compat_dialog(update, context)
    if text == BACK_TO_MENU_BUTTON:
        await show_main_menu(update, context)
        return ConversationHandler.END

    mode = context.user_data.get("horoscope_mode", "solar")
    if mode == "natal":
        return await finish_natal_with_time(update, context)
    return await finish_solar_with_time(update, context)


async def finish_solar_with_time(
    update: Update, context: ContextTypes.DEFAULT_TYPE
) -> int:
    """Принимает время рождения и переводит на оплату соляра 990 ₽ (скриншот → соляр через 30 мин)."""
    text = update.message.text.strip()
    parsed = _parse_birth_time(text)
    if parsed is None:
        await update.message.reply_text(
            "Пожалуйста, введите время в формате ЧЧ:ММ (например, 14:30 или 9:05)."
        )
        return ASK_BIRTHTIME

    hours, minutes = parsed
    birth_time_str = f"{hours:02d}:{minutes:02d}"
    context.user_data["birthtime"] = birth_time_str

    name = context.user_data.get("name", "Дорогой друг")
    birthdate_str = context.user_data.get("birthdate", "01.01.2000")

    order_topic = f"Имя: {name}. Дата рождения: {birthdate_str}. Время рождения: {birth_time_str}."
    context.user_data["order_topic"] = order_topic
    context.user_data["order_type"] = "solar"
    context.user_data["awaiting_screenshot"] = True

    payment_text = (
        "Соляр на год — 990 рублей. Оплата: 89124566686 (Альфа-банк). "
        "Отправьте скриншот перевода в этот чат — соляр будет готов в течение 30 минут."
    )
    keyboard = _main_menu_keyboard()
    reply_markup = ReplyKeyboardMarkup(keyboard, resize_keyboard=True)
    await update.message.reply_text(payment_text, reply_markup=reply_markup)
    return ConversationHandler.END


async def finish_natal_with_time(
    update: Update, context: ContextTypes.DEFAULT_TYPE
) -> int:
    """Принимает время рождения и переводит на оплату натальной карты 990 ₽ (скриншот → разбор через 30 мин)."""
    text = update.message.text.strip()
    parsed = _parse_birth_time(text)
    if parsed is None:
        await update.message.reply_text(
            "Пожалуйста, введите время в формате ЧЧ:ММ (например, 14:30 или 9:05)."
        )
        return ASK_BIRTHTIME

    hours, minutes = parsed
    birth_time_str = f"{hours:02d}:{minutes:02d}"
    context.user_data["birthtime"] = birth_time_str

    name = context.user_data.get("name", "Дорогой друг")
    birthdate_str = context.user_data.get("birthdate", "01.01.2000")

    order_topic = f"Имя: {name}. Дата рождения: {birthdate_str}. Время рождения: {birth_time_str}."
    context.user_data["order_topic"] = order_topic
    context.user_data["order_type"] = "natal"
    context.user_data["awaiting_screenshot"] = True

    payment_text = (
        "Натальная карта — 990 рублей. Оплата: 89124566686 (Альфа-банк). "
        "Отправьте скриншот перевода в этот чат — разбор будет готов в течение 30 минут."
    )
    keyboard = _main_menu_keyboard()
    reply_markup = ReplyKeyboardMarkup(keyboard, resize_keyboard=True)
    await update.message.reply_text(payment_text, reply_markup=reply_markup)
    return ConversationHandler.END


async def send_horoscope_job(context: ContextTypes.DEFAULT_TYPE) -> None:
    """Функция больше не используется, оставлена на будущее."""
    return


async def cancel(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    await update.message.reply_text("Диалог отменён. Если захотите начать снова — напишите /start.")
    return ConversationHandler.END


async def _start_astrologer_chat_from_conv(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Переключение в чат с Марией с завершением активного диалога."""
    await start_astrologer_chat(update, context)
    return ConversationHandler.END


async def _show_main_menu_from_conv(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Показ главного меню с завершением активного диалога."""
    await show_main_menu(update, context)
    return ConversationHandler.END


def main() -> None:
    if not BOT_TOKEN or BOT_TOKEN == "YOUR_BOT_TOKEN_HERE":
        raise RuntimeError(
            "Не указан токен бота. Установите TELEGRAM_BOT_TOKEN или BOT_TOKEN в окружении "
            "(удобно через файл .env в корне проекта; .env не коммитить)."
        )

    application = (
        ApplicationBuilder()
        .token(BOT_TOKEN)
        .connection_pool_size(8)
        .get_updates_connection_pool_size(4)
        .connect_timeout(30)
        .read_timeout(30)
        .write_timeout(30)
        .pool_timeout(60)
        .get_updates_pool_timeout(60)
        .build()
    )

    # Главное меню
    application.add_handler(CommandHandler("start", show_main_menu))

    # Отдельные команды
    application.add_handler(CommandHandler("about", about))
    application.add_handler(CommandHandler("help", help_command))
    application.add_handler(CommandHandler("send_daily_posts", cmd_send_daily_posts))
    application.add_handler(CommandHandler("day", start_daily_horoscope_dialog))
    application.add_handler(CommandHandler("solar", start_solar_horoscope_dialog))
    # Для надёжности регулярки используем текст без эмодзи
    application.add_handler(
        MessageHandler(filters.Regex("^Чат с Марией$"), start_astrologer_chat)
    )
    application.add_handler(
        MessageHandler(filters.Regex("^Обо мне$"), about)
    )
    application.add_handler(
        MessageHandler(filters.Regex("^Помощь$"), help_command)
    )
    application.add_handler(
        MessageHandler(filters.Regex(f"^{BACK_TO_MENU_BUTTON}$"), show_main_menu)
    )

    # Диалог получения гороскопа / соляра / Таро
    conv_handler = ConversationHandler(
        entry_points=[
            CommandHandler("horoscope", start_horoscope_dialog),
            MessageHandler(
                filters.Regex("^Гороскоп на неделю$"), start_horoscope_dialog
            ),
            MessageHandler(
                filters.Regex("^Гороскоп на день$"), start_daily_horoscope_dialog
            ),
            MessageHandler(
                filters.Regex("^Соляр на год$"), start_solar_horoscope_dialog
            ),
            MessageHandler(
                filters.Regex("^Расклад Таро$"), start_tarot_dialog
            ),
            MessageHandler(
                filters.Regex("^Совместимость по зодиаку$"), start_compat_dialog
            ),
            MessageHandler(
                filters.Regex("^Натальная карта$"), start_natal_dialog
            ),
        ],
        states={
            ASK_NAME: [MessageHandler(filters.TEXT & ~filters.COMMAND, ask_age)],
            ASK_BIRTHDATE: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, schedule_horoscope)
            ],
            ASK_BIRTHTIME: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, handle_birthtime)
            ],
            CHECK_SUB_WEEK: [
                CallbackQueryHandler(check_subscribe_callback, pattern="^sub_week$"),
                MessageHandler(filters.TEXT & ~filters.COMMAND, check_subscribe_via_message_week),
            ],
            CHECK_SUB_DAY: [
                CallbackQueryHandler(check_subscribe_callback, pattern="^sub_day$"),
                MessageHandler(filters.TEXT & ~filters.COMMAND, check_subscribe_via_message_day),
            ],
            ASK_TAROT_TOPIC: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, finish_tarot_topic),
            ],
            ASK_COMPAT_TOPIC: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, finish_compat_topic),
            ],
        },
        fallbacks=[
            CommandHandler("cancel", cancel),
            CommandHandler("start", show_main_menu),
            MessageHandler(filters.Regex("^Чат с Марией$"), _start_astrologer_chat_from_conv),
            MessageHandler(filters.Regex(f"^{BACK_TO_MENU_BUTTON}$"), _show_main_menu_from_conv),
        ],
    )

    application.add_handler(conv_handler)
    application.add_handler(MessageHandler(filters.TEXT, astrologer_chat_reply))
    application.add_handler(MessageHandler(filters.PHOTO, astrologer_chat_photo_handler))

    # Ежедневные посты в канал (время по МСК)
    if CHANNEL_CHAT_ID:
        application.job_queue.run_daily(
            _send_daily_channel_post,
            time=time(hour=8, minute=0, tzinfo=MSK_TZ),
            name="daily_channel_post",
        )
        application.job_queue.run_daily(
            _send_astrological_events_post,
            time=time(hour=8, minute=10, tzinfo=MSK_TZ),
            name="daily_events_post",
        )
        logger.info("Ежедневные посты в канал запланированы: 08:00 и 08:10 МСК")

    logger.info("Бот запущен. Ожидаю сообщения...")
    application.run_polling()


if __name__ == "__main__":
    main()

