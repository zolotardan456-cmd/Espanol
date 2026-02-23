import logging
import os
import calendar
import re
from html import escape
from datetime import datetime, timedelta, time as dt_time
from pathlib import Path
from typing import List, Optional, Any
from zoneinfo import ZoneInfo

from telegram import (
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    ReplyKeyboardMarkup,
    ReplyKeyboardRemove,
    Update,
)
from telegram.ext import (
    Application,
    CallbackQueryHandler,
    CommandHandler,
    ContextTypes,
    ConversationHandler,
    Defaults,
    MessageHandler,
    filters,
)
from tzlocal import get_localzone

from storage import Storage

logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", level=logging.INFO
)
logger = logging.getLogger(__name__)

BTN_LESSON = "Записать на урок"
BTN_REPORT = "Отчет о уроке"
BTN_ALL = "Все записи"
BTN_DELETE_ALL = "Удалить все записи"
BTN_EDIT = "Редактировать запись"
BTN_DELETE_ONE = "Удалить запись"
BTN_BACK = "Назад"
WEEKDAYS_RU = [
    "Понедельник",
    "Вторник",
    "Среда",
    "Четверг",
    "Пятница",
    "Суббота",
    "Воскресенье",
]
SCHOOLS = [
    "Yarko",
    "Uknow",
    "Shabadoo",
]

(
    LESSON_SCHOOL,
    LESSON_STUDENT,
    LESSON_DATE,
    LESSON_HOUR,
    LESSON_MINUTE,
    LESSON_END_HOUR,
    LESSON_END_MINUTE,
    EDIT_SELECT,
    DELETE_SELECT,
    REPORT_NAME,
    REPORT_SCHOOL,
    REPORT_PAYMENT,
) = range(12)


def resolve_db_path() -> str:
    explicit = os.getenv("DB_PATH")
    if explicit:
        return explicit.strip().strip('"').strip("'")
    if os.getenv("RAILWAY_ENVIRONMENT") or os.getenv("RAILWAY_PROJECT_ID"):
        return "/data/bot_data.sqlite3"
    return "bot_data.sqlite3"


def validate_runtime_storage_path(db_path: str) -> None:
    in_railway = bool(os.getenv("RAILWAY_ENVIRONMENT") or os.getenv("RAILWAY_PROJECT_ID"))
    if not in_railway:
        return

    db_path_obj = Path(db_path)
    db_path_str = str(db_path_obj)
    if not db_path_obj.is_absolute() or not db_path_str.startswith("/data/"):
        raise RuntimeError(
            "Для Railway задайте DB_PATH внутри /data, например: /data/bot_data.sqlite3. "
            f"Текущее значение DB_PATH: {db_path_str!r}"
        )
    if not Path("/data").exists():
        raise RuntimeError(
            "Не найден путь /data. Подключите Railway Volume и смонтируйте его в /data."
        )


DB_PATH = resolve_db_path()
validate_runtime_storage_path(DB_PATH)
storage = Storage(DB_PATH)


def get_app_timezone():
    tz_name = os.getenv("APP_TZ")
    if tz_name:
        try:
            return ZoneInfo(tz_name)
        except Exception:
            logger.warning("Некорректный APP_TZ=%s, использую системный часовой пояс", tz_name)
    return get_localzone()


def main_keyboard() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        [[BTN_LESSON, BTN_REPORT], [BTN_ALL, BTN_EDIT], [BTN_DELETE_ONE, BTN_DELETE_ALL]],
        resize_keyboard=True,
    )


def form_keyboard() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup([[BTN_BACK]], resize_keyboard=True)


def edit_menu_keyboard() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup([[BTN_DELETE_ONE, BTN_BACK]], resize_keyboard=True)


def teacher_name_from_update(update: Update) -> str:
    user = update.effective_user
    if user and user.first_name and user.first_name.strip():
        return user.first_name.strip()
    return "Анастасия"


def register_chat_from_update(update: Update) -> None:
    chat = update.effective_chat
    if not chat:
        return
    storage.upsert_chat(int(chat.id), teacher_name_from_update(update))


def get_registered_chat_ids(fallback_chat_id: Optional[int] = None) -> List[int]:
    ids = [int(row["chat_id"]) for row in storage.list_chats()]
    if fallback_chat_id is not None and fallback_chat_id not in ids:
        ids.append(fallback_chat_id)
    return ids


async def broadcast_to_registered(
    context: ContextTypes.DEFAULT_TYPE,
    text: str,
    *,
    fallback_chat_id: Optional[int] = None,
    parse_mode: Optional[str] = None,
    reply_markup: Optional[Any] = None,
    delete_after_seconds: Optional[int] = None,
    exclude_chat_id: Optional[int] = None,
) -> int:
    sent_count = 0
    for chat_id in get_registered_chat_ids(fallback_chat_id):
        if exclude_chat_id is not None and chat_id == exclude_chat_id:
            continue
        try:
            sent = await context.bot.send_message(
                chat_id=chat_id,
                text=text,
                parse_mode=parse_mode,
                reply_markup=reply_markup,
            )
            sent_count += 1
            if delete_after_seconds is not None:
                context.job_queue.run_once(
                    delete_message_job,
                    when=delete_after_seconds,
                    data={"chat_id": chat_id, "message_id": int(sent.message_id)},
                )
        except Exception as exc:
            logger.exception("Не удалось отправить рассылку для chat_id=%s: %s", chat_id, exc)
    return sent_count


def lesson_edit_keyboard(lessons: list) -> InlineKeyboardMarkup:
    rows = []
    for row in lessons[:30]:
        lesson_id = int(row["id"])
        start_dt = datetime.fromisoformat(str(row["lesson_dt"]))
        rows.append(
            [
                InlineKeyboardButton(
                    f"✏️ {row['student_name']} {start_dt.strftime('%d.%m %H:%M')}",
                    callback_data=f"edit_lesson:{lesson_id}",
                )
            ]
        )
    return InlineKeyboardMarkup(rows) if rows else InlineKeyboardMarkup([])


def lesson_delete_keyboard(lessons: list) -> InlineKeyboardMarkup:
    rows = []
    for row in lessons[:30]:
        lesson_id = int(row["id"])
        start_dt = datetime.fromisoformat(str(row["lesson_dt"]))
        rows.append(
            [
                InlineKeyboardButton(
                    f"🗑 {row['student_name']} {start_dt.strftime('%d.%m %H:%M')}",
                    callback_data=f"delete_lesson:{lesson_id}",
                )
            ]
        )
    return InlineKeyboardMarkup(rows) if rows else InlineKeyboardMarkup([])


def build_grouped_lessons_text(lessons: list) -> str:
    if not lessons:
        return "Уроки: нет записей"

    grouped = {}
    for row in lessons:
        start_dt = datetime.fromisoformat(str(row["lesson_dt"]))
        end_raw = row["lesson_end_dt"]
        if end_raw:
            end_dt = datetime.fromisoformat(str(end_raw))
        else:
            end_dt = start_dt + timedelta(hours=1)

        day_key = start_dt.date()
        grouped.setdefault(day_key, {})
        grouped[day_key].setdefault(str(row["school"]), [])
        grouped[day_key][str(row["school"])].append(
            {
                "student_name": str(row["student_name"]),
                "start_dt": start_dt,
                "end_dt": end_dt,
                "is_confirmed": bool(int(row["is_confirmed"])) if "is_confirmed" in row.keys() else False,
            }
        )

    lines = []
    for day in sorted(grouped.keys()):
        lines.append(f"{day.day}, {WEEKDAYS_RU[day.weekday()]}")
        lines.append("")

        day_schools = grouped[day]
        school_order = [s for s in SCHOOLS if s in day_schools] + [s for s in sorted(day_schools) if s not in SCHOOLS]
        for school in school_order:
            lines.append(escape(school))
            for lesson in day_schools[school]:
                row_text = (
                    f"{escape(lesson['student_name'])} "
                    f"{lesson['start_dt'].strftime('%H:%M')} - {lesson['end_dt'].strftime('%H:%M')}"
                )
                if lesson.get("is_confirmed"):
                    row_text = f"<s>{row_text}</s>"
                lines.append(row_text)
            lines.append("")

    return "\n".join(lines).strip()


async def delete_message_job(context: ContextTypes.DEFAULT_TYPE) -> None:
    data = context.job.data or {}
    chat_id = data.get("chat_id")
    message_id = data.get("message_id")
    if not chat_id or not message_id:
        return
    try:
        await context.bot.delete_message(chat_id=int(chat_id), message_id=int(message_id))
    except Exception:
        pass


def format_uah(amount: float) -> str:
    if float(amount).is_integer():
        return f"{int(amount)} грн"
    return f"{amount:.2f} грн"


def parse_payment_uah(raw: str) -> Optional[float]:
    value = raw.strip().lower().replace("грн", "").replace("uah", "")
    value = value.replace(",", ".").replace(" ", "")
    if not value:
        return None

    # Support "count*rate", "countxrate" and plain numeric value.
    match = re.fullmatch(r"(\d+(?:\.\d+)?)\s*[*xх]\s*(\d+(?:\.\d+)?)", value)
    if match:
        return float(match.group(1)) * float(match.group(2))

    if re.fullmatch(r"\d+(?:\.\d+)?", value):
        return float(value)

    return None


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    register_chat_from_update(update)
    await update.message.reply_text(
        "Выберите действие:",
        reply_markup=main_keyboard(),
    )


async def show_all(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    register_chat_from_update(update)
    lessons = storage.list_all_lessons_for_view()
    reports = storage.list_recent_reports(chat_id=None, limit=100)
    total_uah = storage.total_payment_uah(chat_id=None)
    totals_by_school_rows = storage.total_payment_uah_by_school(chat_id=None)
    totals_by_school = {str(row["school"]): float(row["total"]) for row in totals_by_school_rows}

    lessons_text = build_grouped_lessons_text(lessons)
    await update.message.reply_text(lessons_text, reply_markup=main_keyboard(), parse_mode="HTML")

    if reports:
        lines = ["\nОтчеты:"]
        for row in reports:
            created_at = datetime.fromisoformat(str(row["created_at"])).strftime("%d.%m.%Y %H:%M")
            lines.append(
                f"- {created_at} | {row['full_name']} | {row['school']} | оплата: {format_uah(float(row['payment_uah']))}"
            )
        lines.append("\nСумма по школам:")
        for school in SCHOOLS:
            lines.append(f"- {school}: {format_uah(totals_by_school.get(school, 0.0))}")
        lines.append(f"\nИтого оплата: {format_uah(total_uah)}")
        await update.message.reply_text("\n".join(lines), reply_markup=main_keyboard())
    else:
        await update.message.reply_text("Отчеты: нет записей", reply_markup=main_keyboard())


async def start_lesson(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    register_chat_from_update(update)
    context.user_data.pop("edit_lesson_id", None)
    await update.message.reply_text(
        "В какой школе будет проходить урок?",
        reply_markup=school_keyboard("school"),
    )
    await update.message.reply_text("Нажмите «Назад», чтобы выйти из заполнения.", reply_markup=form_keyboard())
    return LESSON_SCHOOL


async def need_school_button(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    await update.message.reply_text(
        "Выберите школу кнопкой ниже:",
        reply_markup=school_keyboard("school"),
    )
    return LESSON_SCHOOL


async def start_edit_menu(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    register_chat_from_update(update)
    lessons = storage.list_all_lessons_for_view()
    if not lessons:
        await update.message.reply_text("Нет записей для редактирования.", reply_markup=main_keyboard())
        return ConversationHandler.END

    await update.message.reply_text(
        "Выберите запись, которую хотите перезаписать:",
        reply_markup=lesson_edit_keyboard(lessons),
    )
    await update.message.reply_text(
        "Можно выбрать: «Удалить запись» или «Назад».",
        reply_markup=edit_menu_keyboard(),
    )
    return EDIT_SELECT


async def start_delete_menu(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    register_chat_from_update(update)
    lessons = storage.list_all_lessons_for_view()
    if not lessons:
        await update.message.reply_text("Нет записей для удаления.", reply_markup=main_keyboard())
        return ConversationHandler.END

    await update.message.reply_text(
        "Выберите запись, которую хотите удалить:",
        reply_markup=lesson_delete_keyboard(lessons),
    )
    await update.message.reply_text("Нажмите «Назад», чтобы выйти.", reply_markup=form_keyboard())
    return DELETE_SELECT


async def pick_edit_lesson(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    query = update.callback_query
    await query.answer()
    lesson_id = int(query.data.split(":")[1])
    lesson = storage.get_lesson_by_id(lesson_id)
    if not lesson:
        await query.message.reply_text("Запись не найдена.", reply_markup=main_keyboard())
        return ConversationHandler.END

    context.user_data["edit_lesson_id"] = lesson_id
    await query.message.reply_text(
        "Перезапишите урок по шагам.\nСначала выберите школу:",
        reply_markup=school_keyboard("school"),
    )
    return LESSON_SCHOOL


async def pick_delete_lesson(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    query = update.callback_query
    await query.answer()
    lesson_id = int(query.data.split(":")[1])
    lesson = storage.get_lesson_by_id(lesson_id)
    if not lesson:
        await query.message.reply_text("Запись не найдена.", reply_markup=main_keyboard())
        return ConversationHandler.END

    lesson_start_dt = datetime.fromisoformat(str(lesson["lesson_dt"]))
    lesson_end_raw = lesson["lesson_end_dt"]
    if lesson_end_raw:
        lesson_end_dt = datetime.fromisoformat(str(lesson_end_raw))
    else:
        lesson_end_dt = lesson_start_dt + timedelta(hours=1)

    # Закрываем и удаляем старые сообщения с напоминанием о заполнении отчета для этого урока.
    pending_notifications = storage.consume_open_pending_report_notifications_for_lesson(lesson_id)
    for notification in pending_notifications:
        try:
            await context.bot.delete_message(
                chat_id=int(notification["chat_id"]),
                message_id=int(notification["message_id"]),
            )
        except Exception:
            pass

    storage.delete_lesson_by_id(lesson_id)

    deleted_text = (
        "Запись удалена:\n"
        f"Школа: <b>{escape(str(lesson['school']))}</b>\n"
        f"Ученик: {escape(str(lesson['student_name']))}\n"
        f"Дата: {lesson_start_dt.strftime('%d.%m.%Y')}\n"
        f"Урок: {lesson_start_dt.strftime('%H:%M')} - {lesson_end_dt.strftime('%H:%M')}"
    )
    await query.edit_message_text(deleted_text, parse_mode="HTML")
    await query.message.reply_text("Готово.", reply_markup=main_keyboard())

    chat = update.effective_chat
    actor_chat_id = int(chat.id) if chat else None
    await broadcast_to_registered(
        context,
        deleted_text,
        fallback_chat_id=actor_chat_id,
        parse_mode="HTML",
        reply_markup=main_keyboard(),
        exclude_chat_id=actor_chat_id,
    )
    context.user_data.clear()
    return ConversationHandler.END


def school_keyboard(prefix: str) -> InlineKeyboardMarkup:
    rows = []
    for idx, school in enumerate(SCHOOLS):
        rows.append([InlineKeyboardButton(school, callback_data=f"{prefix}:{idx}")])
    return InlineKeyboardMarkup(rows)


def calendar_keyboard(year: int, month: int) -> InlineKeyboardMarkup:
    cal = calendar.monthcalendar(year, month)
    month_name = f"{calendar.month_name[month]} {year}"
    rows = [
        [
            InlineKeyboardButton("◀", callback_data=f"cal:prev:{year}:{month}"),
            InlineKeyboardButton(month_name, callback_data="cal:noop"),
            InlineKeyboardButton("▶", callback_data=f"cal:next:{year}:{month}"),
        ],
        [
            InlineKeyboardButton("Пн", callback_data="cal:noop"),
            InlineKeyboardButton("Вт", callback_data="cal:noop"),
            InlineKeyboardButton("Ср", callback_data="cal:noop"),
            InlineKeyboardButton("Чт", callback_data="cal:noop"),
            InlineKeyboardButton("Пт", callback_data="cal:noop"),
            InlineKeyboardButton("Сб", callback_data="cal:noop"),
            InlineKeyboardButton("Вс", callback_data="cal:noop"),
        ],
    ]
    for week in cal:
        week_row = []
        for day in week:
            if day == 0:
                week_row.append(InlineKeyboardButton("·", callback_data="cal:noop"))
            else:
                week_row.append(
                    InlineKeyboardButton(
                        str(day),
                        callback_data=f"cal:day:{year}:{month}:{day}",
                    )
                )
        rows.append(week_row)
    return InlineKeyboardMarkup(rows)


def hour_keyboard() -> InlineKeyboardMarkup:
    rows = []
    row = []
    for hour in range(24):
        row.append(InlineKeyboardButton(f"{hour:02d}", callback_data=f"timeh:{hour}"))
        if len(row) == 6:
            rows.append(row)
            row = []
    if row:
        rows.append(row)
    return InlineKeyboardMarkup(rows)


def minute_keyboard() -> InlineKeyboardMarkup:
    rows = []
    row = []
    for minute in range(0, 60, 5):
        row.append(InlineKeyboardButton(f"{minute:02d}", callback_data=f"timem:{minute}"))
        if len(row) == 6:
            rows.append(row)
            row = []
    if row:
        rows.append(row)
    rows.append([InlineKeyboardButton("Назад к часам", callback_data="timem:back")])
    return InlineKeyboardMarkup(rows)


async def lesson_school(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    query = update.callback_query
    await query.answer()
    idx = int(query.data.split(":")[1])
    context.user_data["lesson_school"] = SCHOOLS[idx]

    await query.message.reply_text("Введите имя ученика:", reply_markup=form_keyboard())
    return LESSON_STUDENT


async def lesson_student(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    context.user_data["lesson_student"] = update.message.text.strip()
    now = datetime.now()
    context.user_data["calendar_year"] = now.year
    context.user_data["calendar_month"] = now.month
    await update.message.reply_text(
        "Выберите дату урока в календаре:",
        reply_markup=calendar_keyboard(now.year, now.month),
    )
    return LESSON_DATE


async def lesson_date(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    query = update.callback_query
    await query.answer()
    data = query.data

    if data == "cal:noop":
        return LESSON_DATE

    parts = data.split(":")
    action = parts[1]

    if action in ("prev", "next"):
        year = int(parts[2])
        month = int(parts[3])
        if action == "prev":
            month -= 1
            if month == 0:
                month = 12
                year -= 1
        else:
            month += 1
            if month == 13:
                month = 1
                year += 1
        context.user_data["calendar_year"] = year
        context.user_data["calendar_month"] = month
        await query.edit_message_reply_markup(reply_markup=calendar_keyboard(year, month))
        return LESSON_DATE

    if action == "day":
        year = int(parts[2])
        month = int(parts[3])
        day = int(parts[4])
        selected_date = datetime(year, month, day).date()
        if selected_date < datetime.now().date():
            await query.answer("Нельзя выбрать прошедшую дату", show_alert=True)
            return LESSON_DATE

        context.user_data["lesson_date"] = selected_date.isoformat()
        await query.edit_message_text(
            f"Дата выбрана: {selected_date.strftime('%d.%m.%Y')}\nВыберите час:",
            reply_markup=hour_keyboard(),
        )
        return LESSON_HOUR

    return LESSON_DATE


async def need_date_button(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    now = datetime.now()
    await update.message.reply_text(
        "Выберите дату кликом в календаре:",
        reply_markup=calendar_keyboard(now.year, now.month),
    )
    return LESSON_DATE


async def lesson_hour(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    query = update.callback_query
    await query.answer()
    hour = int(query.data.split(":")[1])
    context.user_data["lesson_hour"] = hour
    selected_date = datetime.strptime(context.user_data["lesson_date"], "%Y-%m-%d").date()
    await query.edit_message_text(
        f"Дата: {selected_date.strftime('%d.%m.%Y')}\nЧас: {hour:02d}\nВыберите минуты:",
        reply_markup=minute_keyboard(),
    )
    return LESSON_MINUTE


async def need_hour_button(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    await update.message.reply_text("Выберите час кнопкой ниже:", reply_markup=hour_keyboard())
    return LESSON_HOUR


async def lesson_minute(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    query = update.callback_query
    await query.answer()
    minute_raw = query.data.split(":")[1]
    if minute_raw == "back":
        selected_date = datetime.strptime(context.user_data["lesson_date"], "%Y-%m-%d").date()
        await query.edit_message_text(
            f"Дата: {selected_date.strftime('%d.%m.%Y')}\nВыберите час:",
            reply_markup=hour_keyboard(),
        )
        return LESSON_HOUR

    minute = int(minute_raw)
    hour = int(context.user_data.get("lesson_hour", 0))
    start_time = datetime.strptime(f"{hour:02d}:{minute:02d}", "%H:%M").time()

    date_iso = context.user_data.get("lesson_date")
    if not date_iso:
        await query.message.reply_text("Дата не выбрана. Начните заново.", reply_markup=main_keyboard())
        context.user_data.clear()
        return ConversationHandler.END

    lesson_date_value = datetime.strptime(date_iso, "%Y-%m-%d").date()
    lesson_start_dt = datetime.combine(lesson_date_value, start_time)

    now = datetime.now()
    if lesson_start_dt <= now:
        await query.edit_message_text(
            "Дата и время должны быть в будущем.\nВыберите час заново:",
            reply_markup=hour_keyboard(),
        )
        return LESSON_HOUR

    context.user_data["lesson_start_dt"] = lesson_start_dt.isoformat(timespec="seconds")
    await query.edit_message_text(
        f"Время начала: {lesson_start_dt.strftime('%H:%M')}\nВыберите час окончания:",
        reply_markup=hour_keyboard(),
    )
    return LESSON_END_HOUR


async def need_minute_button(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    await update.message.reply_text("Выберите минуты кнопкой ниже:", reply_markup=minute_keyboard())
    return LESSON_MINUTE


async def lesson_end_hour(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    query = update.callback_query
    await query.answer()
    hour = int(query.data.split(":")[1])
    context.user_data["lesson_end_hour"] = hour
    lesson_start_dt = datetime.fromisoformat(context.user_data["lesson_start_dt"])
    await query.edit_message_text(
        f"Начало: {lesson_start_dt.strftime('%H:%M')}\nОкончание, час: {hour:02d}\nВыберите минуты окончания:",
        reply_markup=minute_keyboard(),
    )
    return LESSON_END_MINUTE


async def need_end_hour_button(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    await update.message.reply_text("Выберите час окончания кнопкой ниже:", reply_markup=hour_keyboard())
    return LESSON_END_HOUR


async def lesson_end_minute(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    query = update.callback_query
    await query.answer()
    minute_raw = query.data.split(":")[1]
    if minute_raw == "back":
        lesson_start_dt = datetime.fromisoformat(context.user_data["lesson_start_dt"])
        await query.edit_message_text(
            f"Начало: {lesson_start_dt.strftime('%H:%M')}\nВыберите час окончания:",
            reply_markup=hour_keyboard(),
        )
        return LESSON_END_HOUR

    lesson_start_iso = context.user_data.get("lesson_start_dt")
    if not lesson_start_iso:
        await query.message.reply_text("Не найдено время начала. Начните заново.", reply_markup=main_keyboard())
        context.user_data.clear()
        return ConversationHandler.END

    lesson_start_dt = datetime.fromisoformat(lesson_start_iso)
    end_hour = int(context.user_data.get("lesson_end_hour", 0))
    end_minute = int(minute_raw)
    lesson_end_dt = datetime.combine(
        lesson_start_dt.date(),
        datetime.strptime(f"{end_hour:02d}:{end_minute:02d}", "%H:%M").time(),
    )

    if lesson_end_dt <= lesson_start_dt:
        await query.edit_message_text(
            "Окончание должно быть позже начала.\nВыберите час окончания:",
            reply_markup=hour_keyboard(),
        )
        return LESSON_END_HOUR

    now = datetime.now()
    start_reminder_dt = lesson_start_dt - timedelta(minutes=30)
    if start_reminder_dt < now:
        start_reminder_dt = now

    end_reminder_dt = lesson_end_dt - timedelta(minutes=10)
    if end_reminder_dt < now:
        end_reminder_dt = now

    school = context.user_data.get("lesson_school", "")
    student = context.user_data.get("lesson_student", "")
    chat_id = update.effective_chat.id
    edit_lesson_id = context.user_data.get("edit_lesson_id")

    if edit_lesson_id:
        storage.update_lesson(
            lesson_id=int(edit_lesson_id),
            school=school,
            student_name=student,
            lesson_start_dt=lesson_start_dt,
            lesson_end_dt=lesson_end_dt,
            reminder_start_dt=start_reminder_dt,
            reminder_end_dt=end_reminder_dt,
        )
        header = "Запись обновлена."
    else:
        storage.add_lesson(
            chat_id=chat_id,
            school=school,
            student_name=student,
            lesson_start_dt=lesson_start_dt,
            reminder_start_dt=start_reminder_dt,
            lesson_end_dt=lesson_end_dt,
            reminder_end_dt=end_reminder_dt,
        )
        header = "Запись сохранена."

    await query.edit_message_text(
        f"{header}\n"
        f"Школа: <b>{escape(school)}</b>\n"
        f"Ученик: {student}\n"
        f"Дата: {lesson_start_dt.strftime('%d.%m.%Y')}\n"
        f"Урок: с {lesson_start_dt.strftime('%H:%M')} до {lesson_end_dt.strftime('%H:%M')}\n\n"
        "Напоминания:\n"
        "- за 30 минут до начала\n"
        "- за 10 минут до конца",
        parse_mode="HTML",
    )
    await query.message.reply_text("Готово.", reply_markup=main_keyboard())
    await broadcast_to_registered(
        context,
        (
            f"{header}\n"
            f"Школа: <b>{escape(school)}</b>\n"
            f"Ученик: {escape(student)}\n"
            f"Дата: {lesson_start_dt.strftime('%d.%m.%Y')}\n"
            f"Урок: с {lesson_start_dt.strftime('%H:%M')} до {lesson_end_dt.strftime('%H:%M')}"
        ),
        fallback_chat_id=chat_id,
        parse_mode="HTML",
        exclude_chat_id=chat_id,
    )
    context.user_data.clear()
    return ConversationHandler.END


async def need_end_minute_button(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    await update.message.reply_text("Выберите минуты окончания кнопкой ниже:", reply_markup=minute_keyboard())
    return LESSON_END_MINUTE


async def start_report(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    register_chat_from_update(update)
    context.user_data.pop("report_lesson_id", None)
    await update.message.reply_text(
        "Введите Имя Фамилию ученика:",
        reply_markup=form_keyboard(),
    )
    return REPORT_NAME


async def start_report_from_button(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    register_chat_from_update(update)
    query = update.callback_query
    await query.answer()
    lesson_id = None
    parts = query.data.split(":")
    if len(parts) == 2 and parts[1].isdigit():
        lesson_id = int(parts[1])
    context.user_data["report_lesson_id"] = lesson_id
    await query.message.reply_text(
        "Введите Имя Фамилию ученика:",
        reply_markup=form_keyboard(),
    )
    return REPORT_NAME


async def report_name(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    context.user_data["report_name"] = update.message.text.strip()
    await update.message.reply_text("Выберите школу:", reply_markup=school_keyboard("report_school"))
    return REPORT_SCHOOL


async def report_school(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    query = update.callback_query
    await query.answer()
    idx = int(query.data.split(":")[1])
    context.user_data["report_school"] = SCHOOLS[idx]
    await query.edit_message_text(f"Школа выбрана: <b>{escape(SCHOOLS[idx])}</b>", parse_mode="HTML")
    await query.message.reply_text(
        "Введите оплату в гривнах.\nПримеры: 500 или 2*350",
        reply_markup=form_keyboard(),
    )
    return REPORT_PAYMENT


async def need_report_school_button(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    await update.message.reply_text("Выберите школу кнопкой ниже:", reply_markup=school_keyboard("report_school"))
    return REPORT_SCHOOL


async def report_payment(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    payment_raw = update.message.text.strip()
    payment_uah = parse_payment_uah(payment_raw)
    if payment_uah is None:
        await update.message.reply_text("Неверный формат оплаты. Пример: 500 или 2*350")
        return REPORT_PAYMENT

    payment = format_uah(payment_uah)
    full_name = context.user_data.get("report_name", "")
    school = context.user_data.get("report_school", "")
    chat_id = update.effective_chat.id

    storage.add_lesson_report(chat_id, full_name, school, payment, payment_uah)
    report_lesson_id = context.user_data.get("report_lesson_id")
    if report_lesson_id:
        pending_notifications = storage.consume_open_pending_report_notifications_for_lesson(int(report_lesson_id))
    else:
        latest_notification = storage.consume_latest_open_pending_report_notification()
        pending_notifications = [latest_notification] if latest_notification else []
    for notification in pending_notifications:
        try:
            await context.bot.delete_message(
                chat_id=int(notification["chat_id"]),
                message_id=int(notification["message_id"]),
            )
        except Exception:
            pass
    total_uah = storage.total_payment_uah(chat_id=None)

    await update.message.reply_text(
        "Отчет сохранен:\n"
        f"Имя Фамилия: {full_name}\n"
        f"Школа: <b>{escape(school)}</b>\n"
        f"Оплата: {payment}\n"
        f"Общая сумма оплат: {format_uah(total_uah)}",
        reply_markup=main_keyboard(),
        parse_mode="HTML",
    )
    await broadcast_to_registered(
        context,
        (
            "Сохранен новый отчет:\n"
            f"Имя Фамилия: {escape(full_name)}\n"
            f"Школа: <b>{escape(school)}</b>\n"
            f"Оплата: {payment}\n"
            f"Общая сумма оплат: {format_uah(total_uah)}"
        ),
        fallback_chat_id=chat_id,
        parse_mode="HTML",
        exclude_chat_id=chat_id,
    )
    context.user_data.pop("report_lesson_id", None)
    context.user_data.clear()
    return ConversationHandler.END


async def delete_all_records(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    storage.delete_all_for_chat(chat_id=None)
    context.user_data.clear()
    return ConversationHandler.END


async def request_delete_all_confirmation(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    register_chat_from_update(update)
    await update.message.reply_text(
        "Вы уверены, что хотите удалить все записи?",
        reply_markup=InlineKeyboardMarkup(
            [
                [InlineKeyboardButton("Да, удалить", callback_data="delete_all:confirm")],
                [InlineKeyboardButton("Нет", callback_data="delete_all:cancel")],
            ]
        ),
    )
    return ConversationHandler.END


async def confirm_delete_all(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.answer()
    register_chat_from_update(update)
    chat = update.effective_chat
    requester_chat_id = int(chat.id) if chat else None
    await delete_all_records(update, context)
    await query.edit_message_text("Все записи удалены.")
    await context.bot.send_message(chat_id=update.effective_chat.id, text="Готово.", reply_markup=main_keyboard())
    await broadcast_to_registered(
        context,
        "Все записи и отчеты удалены.",
        fallback_chat_id=requester_chat_id,
        reply_markup=main_keyboard(),
        exclude_chat_id=requester_chat_id,
    )


async def cancel_delete_all(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.answer()
    await query.edit_message_text("Удаление отменено.")


async def go_back(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    register_chat_from_update(update)
    context.user_data.clear()
    await update.message.reply_text("Возврат в главное меню.", reply_markup=main_keyboard())
    return ConversationHandler.END


async def confirm_lesson(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.answer("Урок подтвержден")
    register_chat_from_update(update)
    parts = query.data.split(":")
    if len(parts) != 2 or not parts[1].isdigit():
        return
    lesson_id = int(parts[1])
    storage.mark_lesson_confirmed(lesson_id)
    lesson = storage.get_lesson_by_id(lesson_id)
    try:
        await query.edit_message_text("Урок подтвержден. Спасибо!")
    except Exception:
        pass
    chat = update.effective_chat
    actor_chat_id = int(chat.id) if chat else None
    if lesson:
        lesson_start_dt = datetime.fromisoformat(str(lesson["lesson_dt"]))
        lesson_end_raw = lesson["lesson_end_dt"]
        if lesson_end_raw:
            lesson_end_dt = datetime.fromisoformat(str(lesson_end_raw))
        else:
            lesson_end_dt = lesson_start_dt + timedelta(hours=1)
        text = (
            "Урок подтвержден:\n"
            f"Школа: <b>{escape(str(lesson['school']))}</b>\n"
            f"Ученик: {escape(str(lesson['student_name']))}\n"
            f"Дата: {lesson_start_dt.strftime('%d.%m.%Y')}\n"
            f"Урок: {lesson_start_dt.strftime('%H:%M')} - {lesson_end_dt.strftime('%H:%M')}"
        )
    else:
        text = f"Урок подтвержден (ID: {lesson_id})."
    await broadcast_to_registered(
        context,
        text,
        fallback_chat_id=actor_chat_id,
        parse_mode="HTML",
        exclude_chat_id=actor_chat_id,
    )


async def reject_media(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    try:
        if update.message:
            await update.message.delete()
    except Exception:
        pass


async def reject_free_text(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    try:
        if update.message:
            await update.message.delete()
    except Exception:
        pass


async def reject_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    try:
        if update.message:
            await update.message.delete()
    except Exception:
        pass


async def reminder_worker(context: ContextTypes.DEFAULT_TYPE) -> None:
    now = datetime.now()

    start_reminders = storage.get_due_start_reminders(now)
    for reminder in start_reminders:
        message = (
            "Напоминание: урок через 30 минут.\n\n"
            f"Школа: <b>{escape(reminder.school)}</b>\n"
            f"Имя ученика: {reminder.student_name}\n"
            f"Урок: с {reminder.lesson_start_dt.strftime('%H:%M')} до {reminder.lesson_end_dt.strftime('%H:%M')}\n"
            f"Дата: {reminder.lesson_start_dt.strftime('%d.%m.%Y')}"
        )
        sent_to_any = False
        for chat_id in get_registered_chat_ids(reminder.chat_id):
            try:
                sent = await context.bot.send_message(chat_id=chat_id, text=message, parse_mode="HTML")
                context.job_queue.run_once(
                    delete_message_job,
                    when=600,
                    data={"chat_id": chat_id, "message_id": int(sent.message_id)},
                )
                sent_to_any = True
            except Exception as exc:
                logger.exception(
                    "Не удалось отправить стартовое напоминание для lesson_id=%s chat_id=%s: %s",
                    reminder.lesson_id,
                    chat_id,
                    exc,
                )
        if sent_to_any:
            storage.mark_start_reminded(reminder.lesson_id)

    end_reminders = storage.get_due_end_reminders(now)
    for reminder in end_reminders:
        message = f"{reminder.student_name}, урок подходит к концу. Осталось 10 минут."
        sent_to_any = False
        for chat_id in get_registered_chat_ids(reminder.chat_id):
            try:
                sent = await context.bot.send_message(chat_id=chat_id, text=message)
                context.job_queue.run_once(
                    delete_message_job,
                    when=600,
                    data={"chat_id": chat_id, "message_id": int(sent.message_id)},
                )
                sent_to_any = True
            except Exception as exc:
                logger.exception(
                    "Не удалось отправить финальное напоминание для lesson_id=%s chat_id=%s: %s",
                    reminder.lesson_id,
                    chat_id,
                    exc,
                )
        if sent_to_any:
            storage.mark_end_reminded(reminder.lesson_id)

    post_actions = storage.get_due_post_lesson_actions(now)
    for action in post_actions:
        prompt = (
            "<b>Заполните отчет</b>\n"
            f"Урок завершен: {escape(action.student_name)}\n"
            f"Школа: <b>{escape(action.school)}</b>\n"
            f"Время: {action.lesson_start_dt.strftime('%H:%M')} - {action.lesson_end_dt.strftime('%H:%M')}"
        )
        sent_to_any = False
        for chat_id in get_registered_chat_ids(action.chat_id):
            try:
                sent = await context.bot.send_message(
                    chat_id=chat_id,
                    text=prompt,
                    parse_mode="HTML",
                    reply_markup=InlineKeyboardMarkup(
                        [
                            [InlineKeyboardButton("Заполнить отчет", callback_data=f"open_report:{action.lesson_id}")],
                            [InlineKeyboardButton("Подтвердить урок", callback_data=f"confirm_lesson:{action.lesson_id}")],
                        ]
                    ),
                )
                storage.add_pending_report_notification(chat_id, int(sent.message_id), lesson_id=action.lesson_id)
                sent_to_any = True
            except Exception as exc:
                logger.exception(
                    "Не удалось отправить уведомление об отчете для lesson_id=%s chat_id=%s: %s",
                    action.lesson_id,
                    chat_id,
                    exc,
                )
        if sent_to_any:
            storage.mark_post_notified(action.lesson_id)


async def morning_summary_worker(context: ContextTypes.DEFAULT_TYPE) -> None:
    now = datetime.now()
    day_start = datetime(now.year, now.month, now.day, 0, 0, 0)
    day_end = day_start + timedelta(days=1)

    chats = storage.list_chats()
    shared_lessons = storage.list_lessons_between(chat_id=None, start_dt=day_start, end_dt=day_end)
    lesson_count = len(shared_lessons)
    for chat in chats:
        chat_id = int(chat["chat_id"])
        teacher_name = str(chat["teacher_name"]).strip() or "Анастасия"
        if lesson_count == 0:
            text = f"Доброе утро, {teacher_name}. На сегодня у вас 0 уроков."
        else:
            lines = [f"Доброе утро, {teacher_name}. На сегодня у вас {lesson_count} урока(ов):"]
            for row in shared_lessons:
                start_dt = datetime.fromisoformat(str(row["lesson_dt"]))
                end_raw = row["lesson_end_dt"]
                if end_raw:
                    end_dt = datetime.fromisoformat(str(end_raw))
                else:
                    end_dt = start_dt + timedelta(hours=1)
                lines.append(
                    f"- {start_dt.strftime('%H:%M')} - {end_dt.strftime('%H:%M')} | "
                    f"{row['school']} | {row['student_name']}"
                )
            text = "\n".join(lines)

        try:
            await context.bot.send_message(chat_id=chat_id, text=text)
        except Exception as exc:
            logger.exception("Не удалось отправить утреннюю сводку для chat_id=%s: %s", chat_id, exc)


def build_app(token: str) -> Application:
    local_tz = get_app_timezone()
    app = (
        Application.builder()
        .token(token)
        .defaults(Defaults(tzinfo=local_tz))
        .connect_timeout(20)
        .read_timeout(20)
        .write_timeout(20)
        .pool_timeout(20)
        .build()
    )

    lesson_conv = ConversationHandler(
        entry_points=[
            MessageHandler(filters.Regex(f"^{BTN_LESSON}$"), start_lesson),
            MessageHandler(filters.Regex(f"^{BTN_EDIT}$"), start_edit_menu),
            MessageHandler(filters.Regex(f"^{BTN_DELETE_ONE}$"), start_delete_menu),
        ],
        states={
            LESSON_SCHOOL: [
                CallbackQueryHandler(lesson_school, pattern=r"^school:\d+$"),
            ],
            LESSON_STUDENT: [
                MessageHandler(filters.Regex(f"^{BTN_BACK}$"), go_back),
                MessageHandler(filters.TEXT & ~filters.COMMAND, lesson_student),
            ],
            LESSON_DATE: [
                CallbackQueryHandler(lesson_date, pattern=r"^cal:"),
            ],
            LESSON_HOUR: [
                CallbackQueryHandler(lesson_hour, pattern=r"^timeh:\d+$"),
            ],
            LESSON_MINUTE: [
                CallbackQueryHandler(lesson_minute, pattern=r"^timem:(\d+|back)$"),
            ],
            LESSON_END_HOUR: [
                CallbackQueryHandler(lesson_end_hour, pattern=r"^timeh:\d+$"),
            ],
            LESSON_END_MINUTE: [
                CallbackQueryHandler(lesson_end_minute, pattern=r"^timem:(\d+|back)$"),
            ],
            EDIT_SELECT: [
                CallbackQueryHandler(pick_edit_lesson, pattern=r"^edit_lesson:\d+$"),
                MessageHandler(filters.Regex(f"^{BTN_DELETE_ONE}$"), start_delete_menu),
            ],
            DELETE_SELECT: [
                CallbackQueryHandler(pick_delete_lesson, pattern=r"^delete_lesson:\d+$"),
            ],
        },
        fallbacks=[
            MessageHandler(filters.Regex(f"^{BTN_BACK}$"), go_back),
            MessageHandler(filters.Regex(f"^{BTN_DELETE_ALL}$"), request_delete_all_confirmation),
        ],
    )

    report_conv = ConversationHandler(
        entry_points=[
            MessageHandler(filters.Regex(f"^{BTN_REPORT}$"), start_report),
            CallbackQueryHandler(start_report_from_button, pattern=r"^open_report(?::\d+)?$"),
        ],
        states={
            REPORT_NAME: [
                MessageHandler(filters.Regex(f"^{BTN_BACK}$"), go_back),
                MessageHandler(filters.TEXT & ~filters.COMMAND, report_name),
            ],
            REPORT_SCHOOL: [
                CallbackQueryHandler(report_school, pattern=r"^report_school:\d+$"),
            ],
            REPORT_PAYMENT: [
                MessageHandler(filters.Regex(f"^{BTN_BACK}$"), go_back),
                MessageHandler(filters.TEXT & ~filters.COMMAND, report_payment),
            ],
        },
        fallbacks=[
            MessageHandler(filters.Regex(f"^{BTN_BACK}$"), go_back),
            MessageHandler(filters.Regex(f"^{BTN_DELETE_ALL}$"), request_delete_all_confirmation),
        ],
    )

    app.add_handler(CommandHandler("start", start))
    app.add_handler(MessageHandler(filters.Regex(f"^{BTN_ALL}$"), show_all))
    app.add_handler(MessageHandler(filters.Regex(f"^{BTN_DELETE_ALL}$"), request_delete_all_confirmation))
    app.add_handler(lesson_conv)
    app.add_handler(report_conv)
    app.add_handler(CallbackQueryHandler(confirm_delete_all, pattern=r"^delete_all:confirm$"))
    app.add_handler(CallbackQueryHandler(cancel_delete_all, pattern=r"^delete_all:cancel$"))
    app.add_handler(CallbackQueryHandler(confirm_lesson, pattern=r"^confirm_lesson:\d+$"))
    app.add_handler(MessageHandler(filters.Regex(f"^{BTN_BACK}$"), go_back))
    app.add_handler(MessageHandler(filters.COMMAND, reject_command))
    app.add_handler(
        MessageHandler(
            filters.PHOTO
            | filters.VIDEO
            | filters.Sticker.ALL
            | filters.Document.ALL
            | filters.VOICE
            | filters.AUDIO
            | filters.VIDEO_NOTE
            | filters.CONTACT
            | filters.LOCATION
            | filters.POLL,
            reject_media,
        )
    )
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, reject_free_text))

    app.job_queue.run_repeating(reminder_worker, interval=60, first=10)
    app.job_queue.run_daily(morning_summary_worker, time=dt_time(hour=9, minute=0))
    app.job_queue.run_daily(morning_summary_worker, time=dt_time(hour=9, minute=30))
    return app


if __name__ == "__main__":
    token = os.getenv("BOT_TOKEN")
    if not token:
        raise RuntimeError("Не задан BOT_TOKEN. Добавьте токен в переменную окружения.")

    application = build_app(token)
    application.run_polling(close_loop=False)
