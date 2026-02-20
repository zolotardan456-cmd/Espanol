<<<<<<< HEAD
# Org
=======
# Telegram бот для уроков

Бот умеет:
- записывать на урок: школа (выбор кнопкой), имя ученика, дата (календарь), время начала и окончания;
- отправлять напоминание за 30 минут до начала и за 10 минут до конца урока;
- в 09:00 отправлять утреннюю сводку с количеством уроков и списком уроков на сегодня;
- через 5 минут после окончания урока удалять запись урока и присылать уведомление **Заполните отчет**;
- сохранять отчет о уроке: имя фамилия, школа, оплата (автоподсчет в гривнах);
- хранить все в одном месте (SQLite файл `bot_data.sqlite3`).

## Кнопки
- `Записать на урок`
- `Отчет о уроке`
- `Все записи`
- `Удалить все записи`
- `Назад` (появляется во время заполнения формы, возвращает в главное меню)

## Установка
```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## Запуск
1. Создайте бота через [@BotFather](https://t.me/BotFather) и получите токен.
2. Установите токен в переменную окружения:
```bash
export BOT_TOKEN="ВАШ_ТОКЕН"
```
3. Запустите:
```bash
python bot.py
```

## Railway (24/7)
1. Загрузите проект в GitHub и создайте новый проект в Railway из этого репозитория.
2. В `Variables` добавьте:
```bash
BOT_TOKEN=ВАШ_ТОКЕН
DB_PATH=/data/bot_data.sqlite3
APP_TZ=Europe/Kyiv
```
3. Добавьте `Volume` и примонтируйте его в `/data` (чтобы SQLite не терялась при деплоях).
4. Railway возьмет команду запуска из `railway.json` (`python bot.py`).
5. После деплоя проверьте логи сервиса в Railway.

## Автозапуск на Mac (без терминала)
1. Создайте файл `/Users/danzolotar/Documents/New project/.env`:
```bash
echo 'BOT_TOKEN="ВАШ_ТОКЕН"' > "/Users/danzolotar/Documents/New project/.env"
```
2. Подключите сервис:
```bash
launchctl bootstrap "gui/$(id -u)" "/Users/danzolotar/Documents/New project/com.lessonbot.plist"
launchctl enable "gui/$(id -u)/com.lessonbot"
launchctl kickstart -k "gui/$(id -u)/com.lessonbot"
```
3. Проверка:
```bash
launchctl print "gui/$(id -u)/com.lessonbot" | head
tail -n 50 "/Users/danzolotar/Documents/New project/bot.err.log"
```
Отключить:
```bash
launchctl bootout "gui/$(id -u)" "/Users/danzolotar/Documents/New project/com.lessonbot.plist"
```

## Дополнительно
- Команда `/start` показывает кнопки.
- Другие команды отключены, используйте кнопки.
- Кнопка `Удалить все записи` удаляет сразу все уроки и отчеты из базы для текущего чата.
- Школы фиксированные: `Yarko`, `Uknow`, `Shabadoo` (массив `SCHOOLS` в `bot.py`).
- Оплату можно вводить как `500` или `2*350`, бот автоматически посчитает сумму в грн.
- В `Все записи` показывается общий итог и отдельные суммы по каждой школе.
- После сохранения отчета бот удаляет последнее уведомление `Заполните отчет`.
- В уведомлении после урока есть кнопка `Заполнить отчет`, которая сразу открывает форму отчета.
>>>>>>> becee33 (Telegram bot: reminders, reports, railway deploy)
