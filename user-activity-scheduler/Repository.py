# Repository.py
# ---------------------------------
# Реализация доступа к YDB для Telegram-бота
# ---------------------------------

import uuid
import datetime
import random

import ydb
from MessageTemplateDto import MessageTemplateDto


class Repository:
    def __init__(self, pool: ydb.SessionPool):
        self._pool = pool

    # -----------------------------------------------------------------
    #  user_answers
    # -----------------------------------------------------------------
    def saveUserAnswers(
        self,
        tablename: str,
        chat_id: int,
        message_id,
        ans,
        event_type: str,
        message_template_id,
    ):
        """Сохраняет ответ пользователя в таблицу `tablename`."""

        msg_id_val = "NULL" if message_id is None else str(message_id)

        ans_val = (
            "NULL"
            if ans is None
            else "TRUE"
            if ans is True
            else "FALSE"
            if ans is False
            else f"'{str(ans)}'"
        )

        # ❗ без кавычек, потому что Int64 в таблице
        tmpl_val = "NULL" if message_template_id is None else str(message_template_id)

        sql = f"""
        INSERT INTO {tablename}
            (id, chat_id, message_id, answer,
             event_type, message_template_id, event_time)
        VALUES
            ('{uuid.uuid4()}',
             {chat_id},
             {msg_id_val},
             {ans_val},
             '{event_type}',
             {tmpl_val},
             '{datetime.datetime.utcnow()}');
        """

        try:
            return self._pool.retry_operation_sync(
                lambda s: s.transaction().execute(sql, commit_tx=True)
            )
        except Exception:
            # Залогируем запрос для отладки
            print("=== SQL FAILED ===")
            print(sql)
            print("==================")
            raise

    # -----------------------------------------------------------------
    #  Контент: message_templates
    # -----------------------------------------------------------------
    def findMessageTemplates(self, theme: str | None = None) -> MessageTemplateDto | None:
        """
        Возвращает случайный шаблон сообщения для заданной темы (theme_id).
        Если theme=None — выбирает из всех доступных.
        """

        where = f'WHERE theme_id = "{theme}"' if theme else ""

        # Сколько всего подходящих записей?
        cnt = self._pool.retry_operation_sync(
            lambda s: s.transaction().execute(
                f"SELECT COUNT(*) AS c FROM message_templates {where};",
                commit_tx=True,
            )
        )[0].rows[0]["c"]

        if cnt == 0:
            return None

        # Берём случайную запись
        off = random.randint(0, cnt - 1)
        row = self._pool.retry_operation_sync(
            lambda s: s.transaction().execute(
                f"""
                SELECT id, content, theme_id
                FROM message_templates
                {where}
                LIMIT 1 OFFSET {off};
                """,
                commit_tx=True,
            )
        )[0].rows[0]

        return MessageTemplateDto(row["content"], row["id"])

    # -----------------------------------------------------------------
    #  Расписание: user_schedule
    # -----------------------------------------------------------------
    def getUsersForNotification(self, hhmm: str):
        """
        Возвращает список пользователей, у которых
        поле notification_time равно hhmm (строка 'HH:MM').
        """
        res = self._pool.retry_operation_sync(
            lambda s: s.transaction().execute(
                f"""
                SELECT user_id
                FROM user_schedule
                WHERE notification_time = '{hhmm}';
                """,
                commit_tx=True,
            )
        )
        return res[0].rows

    # -----------------------------------------------------------------
    #  Активность пользователя
    # -----------------------------------------------------------------
    def touch_user_activity(self, chat_id: int):
        """
        Обновляет полосу (streak) активности пользователя.
        Вызывать при каждом факте активности.
        """
        today = datetime.date.today()

        # 1. Читаем текущую запись (если есть)
        select_sql = f"""
            SELECT last_active_date AS lad,
                   streak,
                   last_notified_day
            FROM user_activity
            WHERE user_id = {chat_id};
        """
        res = self._pool.retry_operation_sync(
            lambda s: s.transaction().execute(select_sql, commit_tx=True)
        )[0].rows

        if not res:
            # Нет записи – создаём с streak = 1
            self._pool.retry_operation_sync(
                lambda s: s.transaction().execute(
                    f"""
                    UPSERT INTO user_activity
                        (user_id, last_active_date, streak, last_notified_day)
                    VALUES
                        ({chat_id}, Date("{today}"), 1u, 0u);
                    """,
                    commit_tx=True,
                )
            )
            return

        # 2. Вычисляем новый streak
        row = res[0]
        lad: datetime.date = row["lad"]
        streak: int = int(row["streak"])
        lnd: int = int(row["last_notified_day"])

        if lad == today:
            return  # уже отмечен сегодня – ничего не меняем
        if lad == today - datetime.timedelta(days=1):
            streak += 1  # продолжаем полосу
        else:
            streak = 1  # был перерыв – начинаем заново
            lnd = 0     # обнуляем уведомление

        # 3. Сохраняем
        self._pool.retry_operation_sync(
            lambda s: s.transaction().execute(
                f"""
                UPSERT INTO user_activity
                    (user_id, last_active_date, streak, last_notified_day)
                VALUES
                    ({chat_id}, Date("{today}"), {streak}u, {lnd}u);
                """,
                commit_tx=True,
            )
        )

    def get_users_for_activity_push(self):
        """
        Выбирает пользователей, которых нужно поощрить пуш-сообщением
        за достижение полосы длиной 1, 3 или 7 дней.
        """
        sql = """
        SELECT user_id, streak
        FROM user_activity
        WHERE streak IN (1u, 3u, 7u)
          AND last_notified_day < streak         -- ещё не слали на эту длину
          AND last_active_date = CurrentUtcDate()  -- активен сегодня
        """
        res = self._pool.retry_operation_sync(
            lambda s: s.transaction().execute(sql, commit_tx=True)
        )
        return res[0].rows

    def mark_activity_notified(self, chat_id: int, streak: int):
        """Помечает, что пользователю уже отправили пуш за текущую длину streak."""
        self._pool.retry_operation_sync(
            lambda s: s.transaction().execute(
                f"""
                UPDATE user_activity
                SET last_notified_day = {streak}u
                WHERE user_id = {chat_id};
                """,
                commit_tx=True,
            )
        )

    # -----------------------------------------------------------------
    #  Предпочтения пользователя
    # -----------------------------------------------------------------
    THEMES = {
        "SYSTEM": "Системный анализ",
        "BUSINESS": "Бизнес-анализ",
        "DOCUMENTATION": "Документация",
    }

    def upsert_user_theme(self, chat_id: int, theme: str):
        """
        Сохраняет выбранную пользователем тему для рассылки.
        """
        self._pool.retry_operation_sync(
            lambda s: s.transaction().execute(
                f"""
                  UPSERT INTO user_preferences (user_id, theme_id)
                  VALUES ({chat_id}, "{theme}");
                  """,
                commit_tx=True,
            )
        )

    def get_user_theme(self, chat_id: int) -> str | None:
        """
        Возвращает тему, выбранную пользователем, либо None.
        """
        res = self._pool.retry_operation_sync(
            lambda s: s.transaction().execute(
                f"""
                  SELECT theme_id
                  FROM user_preferences
                  WHERE user_id = {chat_id};
                  """,
                commit_tx=True,
            )
        )[0].rows
        return res[0]["theme_id"] if res else None
