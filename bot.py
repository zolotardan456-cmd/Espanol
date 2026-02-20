import logging
import os
import calendar
import re
from html import escape
from datetime import datetime, timedelta, time as dt_time
from typing import Optional
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
BTN_BACK = "Назад"
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
    REPORT_NAME,
    REPORT_SCHOOL,
    REPORT_PAYMENT,
) = range(10)

storage = Storage(os.getenv("DB_PATH", "bot_data.sqlite3"))


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
        [[BTN_LESSON, BTN_REPORT], [BTN_ALL, BTN_DELETE_ALL]],
        resize_keyboard=True,
    )


def form_keyboard() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup([[BTN_BACK]], resize_keyboard=True)


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
    chat_id = update.effective_chat.id
    lessons = storage.list_recent_lessons(chat_id)
    reports = storage.list_recent_reports(chat_id)
    total_uah = storage.total_payment_uah(chat_id)
    totals_by_school_rows = storage.total_payment_uah_by_school(chat_id)
    totals_by_school = {str(row["school"]): float(row["total"]) for row in totals_by_school_rows}

    lines = ["Последние данные:"]

    if lessons:
        lines.append("\nУроки:")
        for row in lessons:
            lesson_start_dt = datetime.fromisoformat(str(row["lesson_dt"]))
            lesson_end_raw = row["lesson_end_dt"]
            if lesson_end_raw:
                lesson_end_dt = datetime.fromisoformat(str(lesson_end_raw))
            else:
                lesson_end_dt = lesson_start_dt + timedelta(hours=1)
            reminded = "да" if int(row["reminded"]) else "нет"
            end_reminded = "да" if int(row["end_reminded"]) else "нет"
            lines.append(
                f"- {lesson_start_dt.strftime('%d.%m.%Y')} | {row['student_name']} | {row['school']} | "
                f"с {lesson_start_dt.strftime('%H:%M')} до {lesson_end_dt.strftime('%H:%M')} | "
                f"старт: {reminded}, конец: {end_reminded}"
            )
    else:
        lines.append("\nУроки: нет записей")

    if reports:
        lines.append("\nОтчеты:")
        for row in reports:
            created_at = datetime.fromisoformat(str(row["created_at"])).strftime("%d.%m.%Y %H:%M")
            lines.append(
                f"- {created_at} | {row['full_name']} | {row['school']} | оплата: {format_uah(float(row['payment_uah']))}"
            )
        lines.append("\nСумма по школам:")
        for school in SCHOOLS:
            lines.append(f"- {school}: {format_uah(totals_by_school.get(school, 0.0))}")
        lines.append(f"\nИтого оплата: {format_uah(total_uah)}")
    else:
        lines.append("\nОтчеты: нет записей")

    await update.message.reply_text("\n".join(lines), reply_markup=main_keyboard())


async def start_lesson(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    register_chat_from_update(update)
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

    storage.add_lesson(
        chat_id=chat_id,
        school=school,
        student_name=student,
        lesson_start_dt=lesson_start_dt,
        reminder_start_dt=start_reminder_dt,
        lesson_end_dt=lesson_end_dt,
        reminder_end_dt=end_reminder_dt,
    )

    await query.edit_message_text(
        "Запись сохранена.\n"
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
    context.user_data.clear()
    return ConversationHandler.END


async def need_end_minute_button(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    await update.message.reply_text("Выберите минуты окончания кнопкой ниже:", reply_markup=minute_keyboard())
    return LESSON_END_MINUTE


async def start_report(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    register_chat_from_update(update)
    await update.message.reply_text(
        "Введите Имя Фамилию ученика:",
        reply_markup=form_keyboard(),
    )
    return REPORT_NAME


async def start_report_from_button(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    register_chat_from_update(update)
    query = update.callback_query
    await query.answer()
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
    pending_notification = storage.consume_latest_pending_report_notification(chat_id)
    if pending_notification:
        try:
            await context.bot.delete_message(
                chat_id=chat_id,
                message_id=int(pending_notification["message_id"]),
            )
        except Exception:
            pass
    total_uah = storage.total_payment_uah(chat_id)

    await update.message.reply_text(
        "Отчет сохранен:\n"
        f"Имя Фамилия: {full_name}\n"
        f"Школа: <b>{escape(school)}</b>\n"
        f"Оплата: {payment}\n"
        f"Общая сумма оплат: {format_uah(total_uah)}",
        reply_markup=main_keyboard(),
        parse_mode="HTML",
    )
    context.user_data.clear()
    return ConversationHandler.END


async def delete_all_records(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    register_chat_from_update(update)
    chat_id = update.effective_chat.id
    storage.delete_all_for_chat(chat_id)
    context.user_data.clear()
    await update.message.reply_text("Все записи удалены.", reply_markup=main_keyboard())
    return ConversationHandler.END


async def go_back(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    register_chat_from_update(update)
    context.user_data.clear()
    await update.message.reply_text("Возврат в главное меню.", reply_markup=main_keyboard())
    return ConversationHandler.END


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
        try:
            await context.bot.send_message(chat_id=reminder.chat_id, text=message, parse_mode="HTML")
            storage.mark_start_reminded(reminder.lesson_id)
        except Exception as exc:
            logger.exception(
                "Не удалось отправить стартовое напоминание для lesson_id=%s: %s",
                reminder.lesson_id,
                exc,
            )

    end_reminders = storage.get_due_end_reminders(now)
    for reminder in end_reminders:
        message = f"{reminder.student_name}, урок подходит к концу. Осталось 10 минут."
        try:
            await context.bot.send_message(chat_id=reminder.chat_id, text=message)
            storage.mark_end_reminded(reminder.lesson_id)
        except Exception as exc:
            logger.exception(
                "Не удалось отправить финальное напоминание для lesson_id=%s: %s",
                reminder.lesson_id,
                exc,
            )

    post_actions = storage.get_due_post_lesson_actions(now)
    for action in post_actions:
        try:
            prompt = (
                "<b>Заполните отчет</b>\n"
                f"Урок завершен: {escape(action.student_name)}\n"
                f"Школа: <b>{escape(action.school)}</b>\n"
                f"Время: {action.lesson_start_dt.strftime('%H:%M')} - {action.lesson_end_dt.strftime('%H:%M')}"
            )
            sent = await context.bot.send_message(
                chat_id=action.chat_id,
                text=prompt,
                parse_mode="HTML",
                reply_markup=InlineKeyboardMarkup(
                    [[InlineKeyboardButton("Заполнить отчет", callback_data="open_report")]]
                ),
            )
            storage.add_pending_report_notification(action.chat_id, int(sent.message_id))
        except Exception as exc:
            logger.exception(
                "Не удалось отправить уведомление об отчете для lesson_id=%s: %s",
                action.lesson_id,
                exc,
            )
        finally:
            storage.delete_lesson_by_id(action.lesson_id)


async def morning_summary_worker(context: ContextTypes.DEFAULT_TYPE) -> None:
    now = datetime.now()
    day_start = datetime(now.year, now.month, now.day, 0, 0, 0)
    day_end = day_start + timedelta(days=1)

    chats = storage.list_chats()
    for chat in chats:
        chat_id = int(chat["chat_id"])
        teacher_name = str(chat["teacher_name"]).strip() or "Анастасия"
        lessons = storage.list_lessons_between(chat_id, day_start, day_end)
        lesson_count = len(lessons)

        if lesson_count == 0:
            text = f"Доброе утро, {teacher_name}. На сегодня у вас 0 уроков."
        else:
            lines = [f"Доброе утро, {teacher_name}. На сегодня у вас {lesson_count} урока(ов):"]
            for row in lessons:
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
        entry_points=[MessageHandler(filters.Regex(f"^{BTN_LESSON}$"), start_lesson)],
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
        },
        fallbacks=[
            MessageHandler(filters.Regex(f"^{BTN_BACK}$"), go_back),
            MessageHandler(filters.Regex(f"^{BTN_DELETE_ALL}$"), delete_all_records),
        ],
    )

    report_conv = ConversationHandler(
        entry_points=[
            MessageHandler(filters.Regex(f"^{BTN_REPORT}$"), start_report),
            CallbackQueryHandler(start_report_from_button, pattern=r"^open_report$"),
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
            MessageHandler(filters.Regex(f"^{BTN_DELETE_ALL}$"), delete_all_records),
        ],
    )

    app.add_handler(CommandHandler("start", start))
    app.add_handler(MessageHandler(filters.Regex(f"^{BTN_ALL}$"), show_all))
    app.add_handler(MessageHandler(filters.Regex(f"^{BTN_DELETE_ALL}$"), delete_all_records))
    app.add_handler(lesson_conv)
    app.add_handler(report_conv)
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
    return app


if __name__ == "__main__":
    token = os.getenv("BOT_TOKEN")
    if not token:
        raise RuntimeError("Не задан BOT_TOKEN. Добавьте токен в переменную окружения.")

    application = build_app(token)
    application.run_polling(close_loop=False)
