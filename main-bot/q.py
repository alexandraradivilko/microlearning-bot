import json, os, re, datetime, random, requests, pytz, ydb, ydb.iam
from Repository import Repository
from MessageTemplateDto import MessageTemplateDto

# ---------------- CONFIG --------------------------------------------
TG_TOKEN = os.getenv("BOT_TOKEN")
URL = f"https://api.telegram.org/bot{TG_TOKEN}/"

driver = ydb.Driver(
    ydb.DriverConfig(
        endpoint=os.getenv("YDB_ENDPOINT"),
        database=os.getenv("YDB_DATABASE"),
        credentials=ydb.iam.MetadataUrlCredentials(),
    )
)
driver.wait(fail_fast=True, timeout=5)
repo = Repository(ydb.SessionPool(driver))
# --------------------------------------------------------------------

user_states: dict[int, str] = {}  # chat_id → 'awaiting_time'

# ---------------- COMMANDS & BUTTONS --------------------------------
COMMANDS = {
    "/get_content": "Новый вызов.",
    "/help": "Команды и возможности.",
    "/scheduler_config": "Изменить время получения контента.",
    "/set_theme": "Выбрать тему.",
    "/get_statistic": "Мои результаты.",
}

BUTTON_TO_COMMAND = {
    "🎯 Новый вызов": "/get_content",
    "🛠 Команды и возможности": "/help",
    "🕒 Изменить время": "/scheduler_config",
    "🧠 Выбрать тему": "/set_theme",
    "🏆 Мои результаты": "/get_statistic",
}

THEMES = {
    "SYSTEM": "Системный анализ",
    "BUSINESS": "Бизнес-анализ",
    "MANAGEMENT": "Менеджмент",
}

# Сообщения-подтверждения при выборе темы
THEME_SET_RESPONSES = {
    "SYSTEM": (
        "🧠 Отлично, теперь будем прокачивать мышление архитектора! "
        "Тема установлена: Системный анализ."
    ),
    "BUSINESS": (
        "💼 Готов копать глубже? Теперь фокус — Бизнес-анализ. "
        "Будем искать ценность, уточнять требования и находить смыслы."
    ),
    "MANAGEMENT": (
        "📋 Переходим к управлению процессами и людьми — тема «Менеджмент» активна. "
        "Ловим баланс между хаосом и контролем!"
    ),
}

# Проценты, из которых рандомно берётся показатель активности
STAT_PERCENTS = [60, 62, 65, 68, 70]

# ---------------- helpers -------------------------------------------

def send_text(txt: str, chat_id: int):
    if isinstance(txt, bytes):
        txt = txt.decode()
    requests.get(URL + f"sendMessage?text={txt}&chat_id={chat_id}")


def create_inline_like() -> str:
    return json.dumps({
        "inline_keyboard": [
            [
                {"text": "Лайк", "callback_data": "text_yes"},
                {"text": "Дизлайк", "callback_data": "text_no"},
            ]
        ]
    })


def send_text_with_buttons(dto: MessageTemplateDto, chat_id: int):
    r = requests.post(
        URL + "sendMessage",
        data={
            "text": dto.get_content(),
            "chat_id": chat_id,
            "parse_mode": "Markdown",
            "reply_markup": create_inline_like(),
        },
    )
    mid = r.json().get("result", {}).get("message_id")
    if mid:
        repo.saveUserAnswers("user_answers", chat_id, mid, None, "sent", dto.get_id())
        repo.touch_user_activity(chat_id)

# ---------------- HANDLERS ------------------------------------------

def handle_start(chat_id: int):
    repo.createDefaultUserScheduleIfNotExists(chat_id)

    text = (
        "Привет! 👋\n"
        "Я — бот, который помогает тебе оставаться в форме как специалисту. "
        "Моя цель — поддерживать твою привычку к саморазвитию с помощью коротких сообщений с полезной аналитикой.\n\n"
        "📌 Каждый день в 12:00 я буду присылать тебе небольшую теорию или практический кейс, "
        "чтобы ты мог быстро освежить знания или узнать что-то новое.\n\n"
        "🧠 Темы:\n"
        "— Системный анализ\n"
        "— Бизнес-анализ\n"
        "— Менеджмент\n"
        "Если не выберешь тему — я буду присылать случайную.\n\n"
        "🛠 Ты можешь:\n"
        "— Настроить время получения сообщений\n"
        "— Запросить дополнительный контент вручную\n"
        "— Выбрать одну тему\n"
        "— Посмотреть свои успехи в общем рейтинге пользователей (с пятницы)\n\n"
        "👇 Ниже — кнопки для управления ботом.\n"
        "Готов начать? Жми «Новый вызов» — и поехали 🚀"
    )
    kb = json.dumps({
        "keyboard": [
            [{"text": "🎯 Новый вызов"}],
            [{"text": "🛠 Команды и возможности"}],
            [{"text": "🕒 Изменить время"}],
            [{"text": "🧠 Выбрать тему"}],
            [{"text": "🏆 Мои результаты"}],
        ],
        "resize_keyboard": True,
    })
    requests.post(
        URL + "sendMessage",
        data={
            "text": text,
            "chat_id": chat_id,
            "reply_markup": kb,
            "parse_mode": "Markdown",
        },
    )


def handle_get_content(chat_id: int):
    theme = repo.get_user_theme(chat_id)
    dto = repo.findMessageTemplates(theme)
    if dto:
        send_text_with_buttons(dto, chat_id)
    else:
        send_text("Контент пока отсутствует.", chat_id)


def handle_help(chat_id: int):
    send_text("\n".join(f"{c} — {d}" for c, d in COMMANDS.items()), chat_id)


def handle_scheduler_config(chat_id: int, txt: str | None = None):
    if txt is None:
        user_states[chat_id] = "awaiting_time"
        send_text("Введите время в формате HH:MM (МСК):", chat_id)
        return
    if not re.fullmatch(r"(?:[01]\d|2[0-3]):[0-5]\d", txt):
        send_text("Неверный формат. Попробуйте, например 08:30", chat_id)
        return
    repo.updateUserSchedule(chat_id, txt)
    user_states.pop(chat_id, None)
    send_text(f"Готово! Буду слать контент в {txt} (МСК).", chat_id)


moscow_tz = pytz.timezone("Europe/Moscow")


def handle_get_statistic(chat_id: int):
    """Отправляет статистику (пт–вс) или сообщение о её недоступности (пн–чт)."""
    weekday = datetime.datetime.now(moscow_tz).weekday()

    if weekday in (4, 5, 6):
        percent = random.choice(STAT_PERCENTS)
        text = (
            f"🏆 Ты активнее, чем {percent}% участников!\n"
            "Отличный ритм! Ты продолжаешь формировать привычку к обучению — и это видно.\n\n"
            "📚 Даже 5 минут фокусированного внимания сегодня — это фундамент уверенности в завтрашнем дне.\n\n"
            "🚀 Продолжай в том же духе. Ты не просто читаешь — ты развиваешься. И это замечают."
        )
    else:
        text = (
            "📊 Статистика ещё недоступна\n"
            "Я формирую рейтинг каждую пятницу, чтобы учесть всю активность за неделю.\n"
            "Пока можешь продолжать — каждый просмотр, каждый лайк и каждый переход по ссылке приближает тебя к топу 💪\n"
            "✨ Не сбавляй темп — ты уже на пути к экспертности!"
        )

    send_text(text, chat_id)


def handle_set_theme(chat_id: int):
    # Каждую тему показываем на своей строке, чтобы текст не обрезался
    inline = {
        "inline_keyboard": [
            [{"text": THEMES["SYSTEM"], "callback_data": "theme_SYSTEM"}],
            [{"text": THEMES["BUSINESS"], "callback_data": "theme_BUSINESS"}],
            [{"text": THEMES["MANAGEMENT"], "callback_data": "theme_MANAGEMENT"}],
        ]
    }
    requests.post(
        URL + "sendMessage",
        data={
            "text": "Выберите тему:",
            "chat_id": chat_id,
            "reply_markup": json.dumps(inline),
        },
    )


COMMAND_HANDLERS = {
    "/start": handle_start,
    "/get_content": handle_get_content,
    "/help": handle_help,
    "/scheduler_config": lambda cid: handle_scheduler_config(cid),
    "/get_statistic": handle_get_statistic,
    "/set_theme": handle_set_theme,
}

# ---------------- MAIN ----------------------------------------------

def handler(event, context):
    msg = json.loads(event["body"])

    # --- callback (inline) ---
    if "callback_query" in msg:
        cb = msg["callback_query"]
        chat_id = cb["message"]["chat"]["id"]
        mid = cb["message"]["message_id"]
        data = cb["data"]

        # выбор темы
        if data.startswith("theme_"):
            theme = data.split("_", 1)[1]  # SYSTEM / BUSINESS / MANAGEMENT
            repo.upsert_user_theme(chat_id, theme)
            send_text(THEME_SET_RESPONSES.get(theme, "Тема изменена."), chat_id)
            return {"statusCode": 200}

        # лайк / дизлайк
        repo.saveUserAnswers("user_answers", chat_id, mid, data == "text_yes", "answered", None)
        repo.touch_user_activity(chat_id)
        send_text("Спасибо за лайк!" if data == "text_yes" else "Спасибо за дизлайк!", chat_id)
        return {"statusCode": 200}

    # --- обычное сообщение ---
    if "message" in msg and "text" in msg["message"]:
        chat_id = msg["message"]["chat"]["id"]
        text = msg["message"]["text"]

        # ожидаем время для /scheduler_config
        if user_states.get(chat_id) == "awaiting_time":
            handle_scheduler_config(chat_id, text)
            return {"statusCode": 200}

        # кнопка → команда, иначе текст как есть
        text = BUTTON_TO_COMMAND.get(text, text)
        fn = COMMAND_HANDLERS.get(text)
        fn(chat_id) if fn else send_text("Я вас не понимаю", chat_id)
        return {"statusCode": 200}

    return {"statusCode": 200}
