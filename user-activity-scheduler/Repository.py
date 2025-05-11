import uuid
import datetime
import random
import ydb
from MessageTemplateDto import MessageTemplateDto


class Repository:
    def __init__(self, pool: ydb.SessionPool):
        self._pool = pool

    # ---------- user_answers -----------------------------------------
    def saveUserAnswers(
        self, tablename, chat_id, message_id, ans, event_type, message_template_id
    ):
        ans_val = (
            "NULL" if ans is None else
            "TRUE" if ans is True else
            "FALSE" if ans is False else f"'{str(ans)}'"
        )
        tmpl_val = "NULL" if message_template_id is None else str(message_template_id)
        msg_id_val = "NULL" if message_id is None else str(message_id)

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
        self._pool.retry_operation_sync(
            lambda s: s.transaction().execute(sql, commit_tx=True)
        )

    # ---------- контент ---------------------------------------------
    def findMessageTemplates(self, theme: str | None = None):
        if theme:
            where = f'WHERE theme_id = "{theme}"'
        else:
            where = ""
        cnt = self._pool.retry_operation_sync(
            lambda s: s.transaction().execute(
                f"SELECT COUNT(*) AS c FROM message_templates {where};",
                commit_tx=True,
            )
        )[0].rows[0]["c"]
        if cnt == 0:
            return None
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

    # ---------- обновление времени ----------------------------------
    def updateUserSchedule(self, chat_id: int, hhmm: str):
        self._pool.retry_operation_sync(
            lambda s: s.transaction().execute(
                f"""
                UPSERT INTO user_schedule (user_id, notification_time)
                VALUES ({chat_id}, "{hhmm}");
                """,
                commit_tx=True,
            )
        )

    # ---------- пользователи для шедулера ---------------------------
    def getUsersForNotification(self, hhmm: str):
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

    # --- обновить/обнулить полосу -----------------------------------
    def touch_user_activity(self, chat_id: int):
        """Обновляет streak активности пользователя."""
        today = datetime.date.today()

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

        row = res[0]
        lad = row["lad"]                 # Date
        streak = int(row["streak"])
        lnd = int(row["last_notified_day"])

        if lad == today:
            return                      # уже отмечен сегодня
        if lad == today - datetime.timedelta(days=1):
            streak += 1                 # продолжаем полосу
        else:
            streak = 1                  # перерыв – начинаем заново
            lnd = 0                     # обнуляем уведомление

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

    # --- выбрать, кому слать напоминание ----------------------------
    def get_users_for_activity_push(self):
        sql = """
        SELECT user_id, streak
        FROM user_activity
        WHERE streak IN (1u,3u,7u)
          AND last_notified_day < streak
          AND last_active_date = Date(CurrentUtcDate())
        """
        res = self._pool.retry_operation_sync(
            lambda s: s.transaction().execute(sql, commit_tx=True)
        )
        return res[0].rows

    # --- отметить «уведомление отправлено» --------------------------
    def mark_activity_notified(self, chat_id: int, streak: int):
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

    THEMES = {
        "SYSTEM": "Системный анализ",
        "BUSINESS": "Бизнес-анализ",
        "MANAGEMENT": "Менеджмент",
    }

    # --- upsert / get -----------------------------------------------
    def upsert_user_theme(self, chat_id: int, theme: str):
        self._pool.retry_operation_sync(
            lambda s: s.transaction().execute(
                f"""
                  UPSERT INTO user_preferences (user_id, theme_id)
                  VALUES ({chat_id}, "{theme}");
                  """,
                commit_tx=True,
            )
        )

    def get_user_theme(self, chat_id: int):
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
        if not res:
            return None
        theme = res[0]["theme_id"]
        # ----- исправление: bytes → str ------------------------------
        if isinstance(theme, (bytes, bytearray)):
            theme = theme.decode()
        return theme

    # ---------- расписание (дефолт) ---------------------------------
    def createDefaultUserScheduleIfNotExists(self, chat_id: int):
        res = self._pool.retry_operation_sync(
            lambda s: s.transaction().execute(
                f"""
                SELECT COUNT(*) AS c
                FROM user_schedule
                WHERE user_id = {chat_id};
                """,
                commit_tx=True,
            )
        )
        if res[0].rows[0]["c"] == 0:
            self._pool.retry_operation_sync(
                lambda s: s.transaction().execute(
                    f"""
                    UPSERT INTO user_schedule (user_id)
                    VALUES ({chat_id});
                    """,
                    commit_tx=True,
                )
            )
