import sqlite3
import threading
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Optional


@dataclass
class LessonStartReminder:
    lesson_id: int
    chat_id: int
    school: str
    student_name: str
    lesson_start_dt: datetime
    lesson_end_dt: datetime


@dataclass
class LessonEndReminder:
    lesson_id: int
    chat_id: int
    student_name: str
    lesson_end_dt: datetime


@dataclass
class PostLessonAction:
    lesson_id: int
    chat_id: int
    school: str
    student_name: str
    lesson_start_dt: datetime
    lesson_end_dt: datetime


class Storage:
    def __init__(self, db_path: str = "bot_data.sqlite3") -> None:
        self.db_path = Path(db_path)
        self._lock = threading.Lock()
        self._init_db()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn

    def _init_db(self) -> None:
        with self._lock:
            with self._connect() as conn:
                conn.execute(
                    """
                    CREATE TABLE IF NOT EXISTS lessons (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        chat_id INTEGER NOT NULL,
                        school TEXT NOT NULL,
                        student_name TEXT NOT NULL,
                        lesson_dt TEXT NOT NULL,
                        lesson_end_dt TEXT NOT NULL,
                        reminder_dt TEXT NOT NULL,
                        end_reminder_dt TEXT NOT NULL,
                        reminded INTEGER NOT NULL DEFAULT 0,
                        end_reminded INTEGER NOT NULL DEFAULT 0,
                        created_at TEXT NOT NULL
                    )
                    """
                )
                lesson_columns = conn.execute("PRAGMA table_info(lessons)").fetchall()
                lesson_column_names = {str(row["name"]) for row in lesson_columns}
                if "lesson_end_dt" not in lesson_column_names:
                    conn.execute("ALTER TABLE lessons ADD COLUMN lesson_end_dt TEXT")
                if "end_reminder_dt" not in lesson_column_names:
                    conn.execute("ALTER TABLE lessons ADD COLUMN end_reminder_dt TEXT")
                if "end_reminded" not in lesson_column_names:
                    conn.execute("ALTER TABLE lessons ADD COLUMN end_reminded INTEGER NOT NULL DEFAULT 1")

                conn.execute(
                    """
                    CREATE TABLE IF NOT EXISTS lesson_reports (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        chat_id INTEGER NOT NULL,
                        full_name TEXT NOT NULL,
                        school TEXT NOT NULL,
                        payment TEXT NOT NULL,
                        payment_uah REAL NOT NULL DEFAULT 0,
                        created_at TEXT NOT NULL
                    )
                    """
                )
                conn.execute(
                    """
                    CREATE TABLE IF NOT EXISTS chats (
                        chat_id INTEGER PRIMARY KEY,
                        teacher_name TEXT NOT NULL DEFAULT '',
                        updated_at TEXT NOT NULL
                    )
                    """
                )
                conn.execute(
                    """
                    CREATE TABLE IF NOT EXISTS pending_report_notifications (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        chat_id INTEGER NOT NULL,
                        message_id INTEGER NOT NULL,
                        is_open INTEGER NOT NULL DEFAULT 1,
                        created_at TEXT NOT NULL
                    )
                    """
                )
                columns = conn.execute("PRAGMA table_info(lesson_reports)").fetchall()
                column_names = {str(row["name"]) for row in columns}
                if "payment_uah" not in column_names:
                    conn.execute(
                        "ALTER TABLE lesson_reports ADD COLUMN payment_uah REAL NOT NULL DEFAULT 0"
                    )
                conn.commit()

    def upsert_chat(self, chat_id: int, teacher_name: str) -> None:
        updated_at = datetime.now().isoformat(timespec="seconds")
        with self._lock:
            with self._connect() as conn:
                conn.execute(
                    """
                    INSERT INTO chats (chat_id, teacher_name, updated_at)
                    VALUES (?, ?, ?)
                    ON CONFLICT(chat_id) DO UPDATE SET
                        teacher_name = excluded.teacher_name,
                        updated_at = excluded.updated_at
                    """,
                    (chat_id, teacher_name, updated_at),
                )
                conn.commit()

    def list_chats(self) -> List[sqlite3.Row]:
        with self._lock:
            with self._connect() as conn:
                return conn.execute(
                    "SELECT chat_id, teacher_name FROM chats ORDER BY updated_at DESC"
                ).fetchall()

    def add_lesson(
        self,
        chat_id: int,
        school: str,
        student_name: str,
        lesson_start_dt: datetime,
        reminder_start_dt: datetime,
        lesson_end_dt: datetime,
        reminder_end_dt: datetime,
    ) -> int:
        created_at = datetime.now().isoformat(timespec="seconds")
        with self._lock:
            with self._connect() as conn:
                cur = conn.execute(
                    """
                    INSERT INTO lessons (
                        chat_id, school, student_name, lesson_dt, lesson_end_dt,
                        reminder_dt, end_reminder_dt, reminded, end_reminded, created_at
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, 0, 0, ?)
                    """,
                    (
                        chat_id,
                        school,
                        student_name,
                        lesson_start_dt.isoformat(timespec="seconds"),
                        lesson_end_dt.isoformat(timespec="seconds"),
                        reminder_start_dt.isoformat(timespec="seconds"),
                        reminder_end_dt.isoformat(timespec="seconds"),
                        created_at,
                    ),
                )
                conn.commit()
                return int(cur.lastrowid)

    def get_due_start_reminders(self, now: datetime) -> List[LessonStartReminder]:
        now_str = now.isoformat(timespec="seconds")
        with self._lock:
            with self._connect() as conn:
                rows = conn.execute(
                    """
                    SELECT id, chat_id, school, student_name, lesson_dt, lesson_end_dt
                    FROM lessons
                    WHERE reminded = 0 AND reminder_dt <= ?
                    ORDER BY lesson_dt ASC
                    """,
                    (now_str,),
                ).fetchall()

        reminders: List[LessonStartReminder] = []
        for row in rows:
            lesson_start_dt = datetime.fromisoformat(str(row["lesson_dt"]))
            lesson_end_raw = row["lesson_end_dt"]
            if lesson_end_raw:
                lesson_end_dt = datetime.fromisoformat(str(lesson_end_raw))
            else:
                lesson_end_dt = lesson_start_dt + timedelta(hours=1)
            reminders.append(
                LessonStartReminder(
                    lesson_id=int(row["id"]),
                    chat_id=int(row["chat_id"]),
                    school=str(row["school"]),
                    student_name=str(row["student_name"]),
                    lesson_start_dt=lesson_start_dt,
                    lesson_end_dt=lesson_end_dt,
                )
            )
        return reminders

    def get_due_end_reminders(self, now: datetime) -> List[LessonEndReminder]:
        now_str = now.isoformat(timespec="seconds")
        with self._lock:
            with self._connect() as conn:
                rows = conn.execute(
                    """
                    SELECT id, chat_id, student_name, lesson_end_dt
                    FROM lessons
                    WHERE end_reminded = 0
                      AND end_reminder_dt IS NOT NULL
                      AND end_reminder_dt <= ?
                    ORDER BY lesson_end_dt ASC
                    """,
                    (now_str,),
                ).fetchall()

        reminders: List[LessonEndReminder] = []
        for row in rows:
            lesson_end_raw = row["lesson_end_dt"]
            if not lesson_end_raw:
                continue
            reminders.append(
                LessonEndReminder(
                    lesson_id=int(row["id"]),
                    chat_id=int(row["chat_id"]),
                    student_name=str(row["student_name"]),
                    lesson_end_dt=datetime.fromisoformat(str(lesson_end_raw)),
                )
            )
        return reminders

    def get_due_post_lesson_actions(self, now: datetime) -> List[PostLessonAction]:
        threshold = (now - timedelta(minutes=5)).isoformat(timespec="seconds")
        with self._lock:
            with self._connect() as conn:
                rows = conn.execute(
                    """
                    SELECT id, chat_id, school, student_name, lesson_dt, lesson_end_dt
                    FROM lessons
                    WHERE lesson_end_dt IS NOT NULL
                      AND lesson_end_dt <= ?
                    ORDER BY lesson_end_dt ASC
                    """,
                    (threshold,),
                ).fetchall()

        actions: List[PostLessonAction] = []
        for row in rows:
            lesson_end_raw = row["lesson_end_dt"]
            if not lesson_end_raw:
                continue
            lesson_start_raw = row["lesson_dt"]
            if not lesson_start_raw:
                continue
            actions.append(
                PostLessonAction(
                    lesson_id=int(row["id"]),
                    chat_id=int(row["chat_id"]),
                    school=str(row["school"]),
                    student_name=str(row["student_name"]),
                    lesson_start_dt=datetime.fromisoformat(str(lesson_start_raw)),
                    lesson_end_dt=datetime.fromisoformat(str(lesson_end_raw)),
                )
            )
        return actions

    def mark_start_reminded(self, lesson_id: int) -> None:
        with self._lock:
            with self._connect() as conn:
                conn.execute("UPDATE lessons SET reminded = 1 WHERE id = ?", (lesson_id,))
                conn.commit()

    def mark_end_reminded(self, lesson_id: int) -> None:
        with self._lock:
            with self._connect() as conn:
                conn.execute("UPDATE lessons SET end_reminded = 1 WHERE id = ?", (lesson_id,))
                conn.commit()

    def delete_lesson_by_id(self, lesson_id: int) -> None:
        with self._lock:
            with self._connect() as conn:
                conn.execute("DELETE FROM lessons WHERE id = ?", (lesson_id,))
                conn.commit()

    def add_lesson_report(
        self,
        chat_id: int,
        full_name: str,
        school: str,
        payment: str,
        payment_uah: float,
    ) -> int:
        created_at = datetime.now().isoformat(timespec="seconds")
        with self._lock:
            with self._connect() as conn:
                cur = conn.execute(
                    """
                    INSERT INTO lesson_reports (chat_id, full_name, school, payment, payment_uah, created_at)
                    VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    (chat_id, full_name, school, payment, payment_uah, created_at),
                )
                conn.commit()
                return int(cur.lastrowid)

    def list_recent_lessons(self, chat_id: int, limit: int = 10) -> List[sqlite3.Row]:
        with self._lock:
            with self._connect() as conn:
                return conn.execute(
                    """
                    SELECT school, student_name, lesson_dt, lesson_end_dt, reminded, end_reminded
                    FROM lessons
                    WHERE chat_id = ?
                    ORDER BY lesson_dt DESC
                    LIMIT ?
                    """,
                    (chat_id, limit),
                ).fetchall()

    def list_recent_reports(self, chat_id: int, limit: int = 10) -> List[sqlite3.Row]:
        with self._lock:
            with self._connect() as conn:
                return conn.execute(
                    """
                    SELECT full_name, school, payment, payment_uah, created_at
                    FROM lesson_reports
                    WHERE chat_id = ?
                    ORDER BY created_at DESC
                    LIMIT ?
                    """,
                    (chat_id, limit),
                ).fetchall()

    def total_payment_uah(self, chat_id: int) -> float:
        with self._lock:
            with self._connect() as conn:
                row = conn.execute(
                    "SELECT COALESCE(SUM(payment_uah), 0) AS total FROM lesson_reports WHERE chat_id = ?",
                    (chat_id,),
                ).fetchone()
        return float(row["total"]) if row else 0.0

    def total_payment_uah_by_school(self, chat_id: int) -> List[sqlite3.Row]:
        with self._lock:
            with self._connect() as conn:
                return conn.execute(
                    """
                    SELECT school, COALESCE(SUM(payment_uah), 0) AS total
                    FROM lesson_reports
                    WHERE chat_id = ?
                    GROUP BY school
                    """,
                    (chat_id,),
                ).fetchall()

    def list_lessons_between(
        self,
        chat_id: int,
        start_dt: datetime,
        end_dt: datetime,
    ) -> List[sqlite3.Row]:
        with self._lock:
            with self._connect() as conn:
                return conn.execute(
                    """
                    SELECT school, student_name, lesson_dt, lesson_end_dt
                    FROM lessons
                    WHERE chat_id = ?
                      AND lesson_dt >= ?
                      AND lesson_dt < ?
                    ORDER BY lesson_dt ASC
                    """,
                    (
                        chat_id,
                        start_dt.isoformat(timespec="seconds"),
                        end_dt.isoformat(timespec="seconds"),
                    ),
                ).fetchall()

    def add_pending_report_notification(self, chat_id: int, message_id: int) -> None:
        created_at = datetime.now().isoformat(timespec="seconds")
        with self._lock:
            with self._connect() as conn:
                conn.execute(
                    """
                    INSERT INTO pending_report_notifications (chat_id, message_id, is_open, created_at)
                    VALUES (?, ?, 1, ?)
                    """,
                    (chat_id, message_id, created_at),
                )
                conn.commit()

    def consume_latest_pending_report_notification(self, chat_id: int) -> Optional[sqlite3.Row]:
        with self._lock:
            with self._connect() as conn:
                row = conn.execute(
                    """
                    SELECT id, message_id
                    FROM pending_report_notifications
                    WHERE chat_id = ? AND is_open = 1
                    ORDER BY created_at DESC
                    LIMIT 1
                    """,
                    (chat_id,),
                ).fetchone()
                if row:
                    conn.execute(
                        "UPDATE pending_report_notifications SET is_open = 0 WHERE id = ?",
                        (int(row["id"]),),
                    )
                    conn.commit()
                return row

    def delete_all_for_chat(self, chat_id: int) -> None:
        with self._lock:
            with self._connect() as conn:
                conn.execute("DELETE FROM lessons WHERE chat_id = ?", (chat_id,))
                conn.execute("DELETE FROM lesson_reports WHERE chat_id = ?", (chat_id,))
                conn.execute("DELETE FROM pending_report_notifications WHERE chat_id = ?", (chat_id,))
                conn.commit()
