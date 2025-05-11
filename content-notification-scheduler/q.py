import json, os, datetime, pytz, requests, ydb, ydb.iam
from Repository import Repository
from MessageTemplateDto import MessageTemplateDto

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

def inline_like():
    return json.dumps({"inline_keyboard": [[
        {"text": "Лайк", "callback_data": "text_yes"},
        {"text": "Дизлайк", "callback_data": "text_no"},
    ]]})

def send_with_buttons(dto: MessageTemplateDto, chat_id: int):
    r = requests.post(
        URL + "sendMessage",
        data={
            "text": dto.get_content(),
            "chat_id": chat_id,
            "parse_mode": "Markdown",
            "reply_markup": inline_like(),
        },
    )
    mid = r.json().get("result", {}).get("message_id")
    if mid:
        repo.saveUserAnswers("user_answers", chat_id, mid, None, "sent", dto.get_id())

msk = pytz.timezone("Europe/Moscow")

def scheduler_handler(event, context):
    hhmm = datetime.datetime.now(msk).strftime("%H:%M")
    users = repo.getUsersForNotification(hhmm)

    for row in users:
        chat_id = row["user_id"]
        theme   = repo.get_user_theme(chat_id)
        dto     = repo.findMessageTemplates(theme)
        if dto:
            send_with_buttons(dto, chat_id)

    return {"statusCode": 200, "body": "done"}
