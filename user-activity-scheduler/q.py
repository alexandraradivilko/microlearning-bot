import os, json, datetime, pytz, requests, ydb, ydb.iam
from Repository import Repository

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


def send_push(chat_id: int, streak: int):
    texts = {
        1: (
            "🎉 Отличное начало! Первый день активности — это уже шаг вперёд.\n"
            "Продолжай — завтра ждёт новый вызов! 👏"
        ),
        3: (
            "🔥 Уже три дня подряд — так держать!\n"
            "Ты формируешь привычку, и мозг это чувствует. Продолжим завтра? 😉"
        ),
        7: (
            "🏆 Целая неделя без перерывов — ты в числе самых целеустремлённых!\n"
            "Это уже не случайность, а система. Вперёд — к следующей цели 💡"
        ),
    }
    text = texts.get(streak)
    if text:
        requests.get(URL + f"sendMessage?text={text}&chat_id={chat_id}")


def handler(event, context):
    # вызывается CRON'ом каждый день в 17:00 UTC (20:00 МСК)
    users = repo.get_users_for_activity_push()
    for row in users:
        chat_id = row["user_id"]
        streak = row["streak"]
        send_push(chat_id, streak)
        repo.mark_activity_notified(chat_id, streak)
    return {"statusCode": 200}
