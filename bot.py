import os
import asyncio
import logging
import time
from collections import defaultdict
from datetime import datetime, date, timedelta
from dotenv import load_dotenv

load_dotenv()

from telegram import Update, ReplyKeyboardMarkup, KeyboardButton
from telegram.ext import Application, MessageHandler, CommandHandler, filters, ContextTypes
from apscheduler.schedulers.asyncio import AsyncIOScheduler
import httpx
import pytz

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
logger = logging.getLogger(__name__)

TELEGRAM_TOKEN = os.environ["TELEGRAM_TOKEN"]
SUPABASE_URL   = os.environ["SUPABASE_URL"]
SUPABASE_KEY   = os.environ["SUPABASE_KEY"]
ADMIN_ID       = "539648155"
KZ_TZ          = pytz.timezone("Asia/Almaty")

HEADERS = {
    "apikey": SUPABASE_KEY,
    "Authorization": f"Bearer {SUPABASE_KEY}",
    "Content-Type": "application/json",
    "Prefer": "return=representation"
}

_last_msg: dict = defaultdict(float)

def is_rate_limited(telegram_id: str) -> bool:
    now = time.time()
    if now - _last_msg[telegram_id] < 1.5:
        return True
    _last_msg[telegram_id] = now
    return False

MENU = ReplyKeyboardMarkup([
    [KeyboardButton("🏠 Апартаменты"), KeyboardButton("📊 Статус")],
    [KeyboardButton("📅 Брони"), KeyboardButton("💰 Отчёт за месяц")],
    [KeyboardButton("➕ Добавить"), KeyboardButton("🤖 Команды")],
], resize_keyboard=True)

ADMIN_MENU = ReplyKeyboardMarkup([
    [KeyboardButton("🏠 Апартаменты"), KeyboardButton("📊 Статус")],
    [KeyboardButton("📅 Брони"), KeyboardButton("💰 Отчёт за месяц")],
    [KeyboardButton("➕ Добавить"), KeyboardButton("🤖 Команды")],
    [KeyboardButton("👑 Админ")],
], resize_keyboard=True)

def get_menu(telegram_id):
    return ADMIN_MENU if str(telegram_id) == ADMIN_ID else MENU

COMMANDS_TEXT = """🏠 Pater AI — всё что я умею:

🏠 Апартаменты:
"добавить 334" — добавить апартамент
"удалить 334" — удалить апартамент
"переименовать 334 в Люкс" — переименовать

🏃 Заезды:
"сдал 334 сутки 15000" — суточный заезд
"сдал 334 сутки 15000 01.04" — задним числом
"сдал 334 часовой 5000" — почасовой (2ч по умолчанию)
"сдал 334 часовой 5000 3ч" — почасовой на 3 часа
"выехал 334" — гость выехал досрочно

📅 Брони:
"забронировали 334 Айдар +77001234567 с 15 по 17 апреля"
"отменить бронь 334 22.04"

💰 Расходы:
"расход горничная 30000 общий" — общий расход
"расход горничная 30000 общий 15.03" — с датой
"расход 334 ремонт 50000" — расход по апартаменту
"расход 334 ремонт 50000 20.03" — с датой

📊 Отчёты:
"отчёт 334" — по конкретному апартаменту
"отчёт апрель" — за месяц
"статус" — кто занят сейчас

❌ Отмена:
"отмена" — удалить последнее действие (заезд или расход)"""

async def _get(url):
    async with httpx.AsyncClient(timeout=10.0) as client:
        r = await client.get(url, headers=HEADERS)
        data = r.json()
        return data if isinstance(data, list) else []

async def _post(url, payload):
    async with httpx.AsyncClient(timeout=10.0) as client:
        r = await client.post(url, headers=HEADERS, json=payload)
        data = r.json()
        return data if isinstance(data, list) else []

async def _patch(url, payload):
    async with httpx.AsyncClient(timeout=10.0) as client:
        r = await client.patch(url, headers=HEADERS, json=payload)
        return r.status_code < 300

async def _delete(url):
    async with httpx.AsyncClient(timeout=10.0) as client:
        r = await client.delete(url, headers=HEADERS)
        return r.status_code < 300

async def get_or_create_user(telegram_id, name):
    data = await _get(f"{SUPABASE_URL}/rest/v1/users?telegram_id=eq.{telegram_id}&select=id")
    if data:
        return data[0]["id"]
    result = await _post(f"{SUPABASE_URL}/rest/v1/users", {"telegram_id": telegram_id, "name": name})
    return result[0]["id"] if result else None

async def get_apartments(user_id):
    return await _get(f"{SUPABASE_URL}/rest/v1/apartments?user_id=eq.{user_id}&is_active=eq.true&select=id,name&order=name")

async def find_apartment(user_id, name_part):
    apts = await get_apartments(user_id)
    name_part = name_part.lower().strip()
    for a in apts:
        if a["name"].lower() == name_part:
            return a
    for a in apts:
        if name_part in a["name"].lower():
            return a
    return None

async def add_apartment(user_id, name):
    result = await _post(f"{SUPABASE_URL}/rest/v1/apartments", {"user_id": user_id, "name": name, "is_active": True})
    return result[0] if result else None

async def delete_apartment(user_id, name_part):
    apt = await find_apartment(user_id, name_part)
    if not apt:
        return False
    return await _patch(f"{SUPABASE_URL}/rest/v1/apartments?id=eq.{apt['id']}&user_id=eq.{user_id}", {"is_active": False})

async def rename_apartment(user_id, old_name, new_name):
    apt = await find_apartment(user_id, old_name)
    if not apt:
        return False
    return await _patch(f"{SUPABASE_URL}/rest/v1/apartments?id=eq.{apt['id']}&user_id=eq.{user_id}", {"name": new_name})

async def add_checkin(user_id, apt_id, amount, checkin_type, note="", checkin_date=None):
    return await _post(f"{SUPABASE_URL}/rest/v1/checkins", {
        "user_id": user_id, "apartment_id": apt_id, "amount": amount,
        "type": checkin_type, "note": note,
        "check_in": checkin_date or datetime.now().isoformat()
    })

async def checkout_apartment(user_id, apt_id):
    return await _patch(
        f"{SUPABASE_URL}/rest/v1/checkins?user_id=eq.{user_id}&apartment_id=eq.{apt_id}&check_out=is.null",
        {"check_out": datetime.now().isoformat()})

async def get_active_checkin(user_id, apt_id):
    data = await _get(f"{SUPABASE_URL}/rest/v1/checkins?user_id=eq.{user_id}&apartment_id=eq.{apt_id}&check_out=is.null&select=id,amount,type,check_in,note&order=check_in.desc&limit=1")
    return data[0] if data else None

async def add_booking(user_id, apt_id, guest_name, phone, check_in, check_out, amount=0):
    return await _post(f"{SUPABASE_URL}/rest/v1/bookings", {
        "user_id": user_id, "apartment_id": apt_id, "guest_name": guest_name,
        "phone": phone, "check_in": check_in, "check_out": check_out,
        "amount": amount, "status": "confirmed"
    })

async def get_bookings(user_id):
    today = date.today().isoformat()
    return await _get(f"{SUPABASE_URL}/rest/v1/bookings?user_id=eq.{user_id}&check_out=gte.{today}&status=eq.confirmed&select=id,guest_name,phone,check_in,check_out,amount,apartment_id&order=check_in")

async def add_expense(user_id, amount, category, comment, apt_id=None, is_shared=False, expense_date=None):
    payload = {
        "user_id": user_id, "apartment_id": apt_id, "amount": amount,
        "category": category, "comment": comment, "is_shared": is_shared
    }
    if expense_date:
        payload["created_at"] = expense_date
    return await _post(f"{SUPABASE_URL}/rest/v1/expenses", payload)

def get_logical_checkout(check_in_dt: datetime) -> datetime:
    """
    Суточный выезд:
    - Заехал 00:00–05:59 → выезд в ЭТОТ же день в 12:00
    - Заехал 06:00–23:59 → выезд на СЛЕДУЮЩИЙ день в 12:00
    """
    if check_in_dt.hour < 6:
        checkout_date = check_in_dt.date()
    else:
        checkout_date = check_in_dt.date() + timedelta(days=1)
    return datetime.combine(checkout_date, datetime.strptime("12:00", "%H:%M").time())

def get_hourly_checkout(check_in_dt: datetime, hours: int) -> datetime:
    """Почасовой выезд: заезд + количество часов"""
    return check_in_dt + timedelta(hours=hours)

async def get_status(user_id):
    apts = await get_apartments(user_id)
    if not apts:
        return "🏠 Апартаментов пока нет.\n\nДобавь: 'добавить 334'"
    lines = ["📊 Статус апартаментов:\n"]
    for apt in apts:
        active = await get_active_checkin(user_id, apt["id"])
        if active:
            check_in_dt = datetime.fromisoformat(active["check_in"])
            if active["type"] == "hourly":
                hours = 2
                try:
                    note = active.get("note", "")
                    if note and note.endswith("ч"):
                        hours = int(note.replace("ч", "").strip())
                except:
                    pass
                checkout_dt = get_hourly_checkout(check_in_dt, hours)
                lines.append(f"🔴 {apt['name']} — почасово до {checkout_dt.strftime('%H:%M')}")
            else:
                checkout_dt = get_logical_checkout(check_in_dt)
                lines.append(f"🔴 {apt['name']} — занят до {checkout_dt.strftime('%d.%m')} 12:00")
        else:
            lines.append(f"🟢 {apt['name']} — свободен")
    return "\n".join(lines)

async def get_monthly_report(user_id, year=None, month=None):
    now = datetime.now()
    year = year or now.year
    month = month or now.month
    start = f"{year}-{month:02d}-01T00:00:00"
    end = f"{year+1}-01-01T00:00:00" if month == 12 else f"{year}-{month+1:02d}-01T00:00:00"
    checkins = await _get(f"{SUPABASE_URL}/rest/v1/checkins?user_id=eq.{user_id}&check_in=gte.{start}&check_in=lt.{end}&select=amount,type,apartment_id&order=check_in")
    expenses = await _get(f"{SUPABASE_URL}/rest/v1/expenses?user_id=eq.{user_id}&created_at=gte.{start}&created_at=lt.{end}&select=amount,category,comment,apartment_id,is_shared")
    apts = await get_apartments(user_id)
    total_income  = sum(float(c["amount"]) for c in checkins)
    total_expense = sum(float(e["amount"]) for e in expenses)
    lines = [f"💰 Отчёт за {year}-{month:02d}\n"]
    lines.append(f"✅ Доходы: {total_income:,.0f} ₸")
    lines.append(f"❌ Расходы: {total_expense:,.0f} ₸")
    lines.append(f"💵 Прибыль: {total_income - total_expense:,.0f} ₸\n")
    if apts:
        lines.append("По апартаментам:")
        for apt in apts:
            apt_income = sum(float(c["amount"]) for c in checkins if c.get("apartment_id") == apt["id"])
            if apt_income > 0:
                lines.append(f"  🏠 {apt['name']}: {apt_income:,.0f} ₸")
    return "\n".join(lines)

async def get_apt_report(user_id, apt):
    now = datetime.now()
    start = f"{now.year}-{now.month:02d}-01T00:00:00"
    checkins = await _get(f"{SUPABASE_URL}/rest/v1/checkins?user_id=eq.{user_id}&apartment_id=eq.{apt['id']}&check_in=gte.{start}&select=amount,type,check_in&order=check_in.desc")
    expenses = await _get(f"{SUPABASE_URL}/rest/v1/expenses?user_id=eq.{user_id}&apartment_id=eq.{apt['id']}&created_at=gte.{start}&select=amount,comment")
    income  = sum(float(c["amount"]) for c in checkins)
    expense = sum(float(e["amount"]) for e in expenses)
    lines = [f"📊 {apt['name']} — отчёт за месяц\n"]
    lines.append(f"✅ Доходы: {income:,.0f} ₸")
    lines.append(f"❌ Расходы: {expense:,.0f} ₸")
    lines.append(f"💵 Прибыль: {income - expense:,.0f} ₸")
    lines.append(f"📝 Заездов: {len(checkins)}")
    return "\n".join(lines)

def parse_date(date_str):
    date_str = date_str.strip()
    for fmt in ["%d.%m", "%d.%m.%Y", "%d.%m.%y"]:
        try:
            d = datetime.strptime(date_str, fmt)
            if fmt == "%d.%m":
                d = d.replace(year=datetime.now().year)
            return d
        except ValueError:
            pass
    return None

def is_date_token(token):
    return "." in token and any(c.isdigit() for c in token)

def is_amount_token(token):
    cleaned = token.replace(",", "").replace(" ", "")
    return cleaned.isdigit() and len(cleaned) > 0

def parse_hours_token(token):
    """Парсит токен типа '2ч', '3ч' → возвращает int или None"""
    token = token.lower().strip()
    if token.endswith("ч") and token[:-1].isdigit():
        return int(token[:-1])
    return None

async def undo_last_action(user_id):
    """
    Удаляет последнее действие — заезд или расход,
    смотря что было записано позже по времени.
    """
    last_checkin = await _get(
        f"{SUPABASE_URL}/rest/v1/checkins?user_id=eq.{user_id}"
        f"&order=created_at.desc&limit=1&select=id,created_at,apartment_id"
    )
    last_expense = await _get(
        f"{SUPABASE_URL}/rest/v1/expenses?user_id=eq.{user_id}"
        f"&order=created_at.desc&limit=1&select=id,created_at,category,amount"
    )

    checkin_time = last_checkin[0]["created_at"] if last_checkin else None
    expense_time = last_expense[0]["created_at"] if last_expense else None

    # Сравниваем что было позже
    if checkin_time and expense_time:
        if checkin_time >= expense_time:
            await _delete(f"{SUPABASE_URL}/rest/v1/checkins?id=eq.{last_checkin[0]['id']}")
            return "заезд", None
        else:
            e = last_expense[0]
            await _delete(f"{SUPABASE_URL}/rest/v1/expenses?id=eq.{e['id']}")
            return "расход", f"{e['category']} {float(e['amount']):,.0f} ₸"
    elif checkin_time:
        await _delete(f"{SUPABASE_URL}/rest/v1/checkins?id=eq.{last_checkin[0]['id']}")
        return "заезд", None
    elif expense_time:
        e = last_expense[0]
        await _delete(f"{SUPABASE_URL}/rest/v1/expenses?id=eq.{e['id']}")
        return "расход", f"{e['category']} {float(e['amount']):,.0f} ₸"
    else:
        return None, None

async def auto_checkout_daily(app):
    """Автовыселение суточных заездов в 12:00"""
    now = datetime.now(KZ_TZ).replace(tzinfo=None)
    logger.info(f"⏰ Запуск автовыселения суточных в {now}")

    checkins = await _get(
        f"{SUPABASE_URL}/rest/v1/checkins"
        f"?check_out=is.null&type=eq.daily"
        f"&select=id,apartment_id,user_id,check_in"
    )

    for c in checkins:
        check_in_dt = datetime.fromisoformat(c["check_in"])
        expected_checkout = get_logical_checkout(check_in_dt)

        if now >= expected_checkout:
            await _patch(
                f"{SUPABASE_URL}/rest/v1/checkins?id=eq.{c['id']}",
                {"check_out": expected_checkout.isoformat()}
            )
            logger.info(f"✅ Автовыселение суточный: checkin_id={c['id']}")

            user = await _get(f"{SUPABASE_URL}/rest/v1/users?id=eq.{c['user_id']}&select=telegram_id")
            apt  = await _get(f"{SUPABASE_URL}/rest/v1/apartments?id=eq.{c['apartment_id']}&select=name")
            if user and apt:
                try:
                    await app.bot.send_message(
                        chat_id=user[0]["telegram_id"],
                        text=f"🟢 {apt[0]['name']} — автоматически освобождён (выезд 12:00)"
                    )
                except Exception as e:
                    logger.warning(f"Уведомление не отправлено: {e}")

async def auto_checkout_hourly(app):
    """Автовыселение почасовых заездов каждые 15 минут"""
    now = datetime.now(KZ_TZ).replace(tzinfo=None)

    checkins = await _get(
        f"{SUPABASE_URL}/rest/v1/checkins"
        f"?check_out=is.null&type=eq.hourly"
        f"&select=id,apartment_id,user_id,check_in,note"
    )

    for c in checkins:
        check_in_dt = datetime.fromisoformat(c["check_in"])
        hours = 2
        try:
            note = c.get("note", "")
            if note and note.endswith("ч"):
                hours = int(note.replace("ч", "").strip())
        except:
            pass

        expected_checkout = get_hourly_checkout(check_in_dt, hours)

        if now >= expected_checkout:
            await _patch(
                f"{SUPABASE_URL}/rest/v1/checkins?id=eq.{c['id']}",
                {"check_out": expected_checkout.isoformat()}
            )
            logger.info(f"✅ Автовыселение почасовой: checkin_id={c['id']}")

            user = await _get(f"{SUPABASE_URL}/rest/v1/users?id=eq.{c['user_id']}&select=telegram_id")
            apt  = await _get(f"{SUPABASE_URL}/rest/v1/apartments?id=eq.{c['apartment_id']}&select=name")
            if user and apt:
                try:
                    await app.bot.send_message(
                        chat_id=user[0]["telegram_id"],
                        text=f"🟢 {apt[0]['name']} — почасовой заезд завершён (через {hours}ч)"
                    )
                except Exception as e:
                    logger.warning(f"Уведомление не отправлено: {e}")

async def send_booking_reminders(app):
    tomorrow = (date.today() + timedelta(days=1)).isoformat()
    bookings = await _get(f"{SUPABASE_URL}/rest/v1/bookings?check_in=eq.{tomorrow}&status=eq.confirmed&select=user_id,guest_name,phone,check_in,check_out,apartment_id")
    for b in bookings:
        user = await _get(f"{SUPABASE_URL}/rest/v1/users?id=eq.{b['user_id']}&select=telegram_id")
        if not user:
            continue
        tid = user[0]["telegram_id"]
        apt = await _get(f"{SUPABASE_URL}/rest/v1/apartments?id=eq.{b['apartment_id']}&select=name")
        apt_name = apt[0]["name"] if apt else "?"
        try:
            await app.bot.send_message(chat_id=tid, text=(
                f"📅 Напоминание о заезде завтра!\n\n"
                f"🏠 {apt_name}\n👤 {b['guest_name']}\n📞 {b['phone']}\n"
                f"📆 {b['check_in']} → {b['check_out']}"))
        except Exception as e:
            logger.warning(f"Напоминание не отправлено {tid}: {e}")

async def handle_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    telegram_id = str(update.effective_user.id)
    name = update.effective_user.full_name
    await get_or_create_user(telegram_id, name)
    menu = get_menu(telegram_id)
    await update.message.reply_text(
        f"👋 Привет, {name}!\n\n"
        f"🏠 Я Pater AI — помогаю управлять апартаментами, мини-отелями и хостелами.\n\n"
        f"Начни с добавления объекта:\n'добавить 334'\n\n"
        f"Нажми 🤖 Команды чтобы увидеть всё что я умею.",
        reply_markup=menu)

async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    telegram_id = str(update.effective_user.id)
    name = update.effective_user.full_name
    if is_rate_limited(telegram_id):
        await update.message.reply_text("⏳ Подожди секунду...")
        return
    text = update.message.text.strip()
    text_lower = text.lower()
    menu = get_menu(telegram_id)
    user_id = await get_or_create_user(telegram_id, name)
    if user_id is None:
        await update.message.reply_text("Ошибка. Попробуй ещё раз.")
        return

    if text_lower in ["🤖 команды", "команды"]:
        await update.message.reply_text(COMMANDS_TEXT, reply_markup=menu); return

    if text_lower in ["🏠 апартаменты", "апартаменты", "мои апартаменты"]:
        apts = await get_apartments(user_id)
        if not apts:
            await update.message.reply_text("🏠 Апартаментов пока нет.\n\nДобавь: 'добавить 334'", reply_markup=menu)
        else:
            lines = ["🏠 Твои апартаменты:\n"] + [f"  • {a['name']}" for a in apts]
            await update.message.reply_text("\n".join(lines), reply_markup=menu)
        return

    if text_lower in ["📊 статус", "статус"]:
        await update.message.reply_text(await get_status(user_id), reply_markup=menu); return

    if text_lower in ["💰 отчёт за месяц", "отчёт за месяц", "отчет за месяц"]:
        await update.message.reply_text(await get_monthly_report(user_id), reply_markup=menu); return

    if text_lower in ["📅 брони", "брони"]:
        bookings = await get_bookings(user_id)
        if not bookings:
            await update.message.reply_text("📅 Активных броней нет.", reply_markup=menu)
        else:
            apts = await get_apartments(user_id)
            apt_map = {a["id"]: a["name"] for a in apts}
            lines = ["📅 Активные брони:\n"]
            for b in bookings:
                apt_name = apt_map.get(b["apartment_id"], "?")
                lines.append(f"🏠 {apt_name}\n  👤 {b['guest_name']} {b['phone']}\n  📆 {b['check_in']} → {b['check_out']}\n")
            await update.message.reply_text("\n".join(lines), reply_markup=menu)
        return

    if text_lower in ["➕ добавить", "добавить апартамент", "добавить объект"]:
        await update.message.reply_text("Напиши название:\n'добавить 334'", reply_markup=menu); return

    if text_lower.startswith("добавить "):
        apt_name = text[9:].strip()
        if not apt_name:
            await update.message.reply_text("Напиши название: 'добавить 334'", reply_markup=menu); return
        result = await add_apartment(user_id, apt_name)
        msg = f"✅ Апартамент '{apt_name}' добавлен!" if result else "Ошибка. Попробуй снова."
        await update.message.reply_text(msg, reply_markup=menu); return

    if text_lower.startswith("удалить "):
        apt_name = text[8:].strip()
        success = await delete_apartment(user_id, apt_name)
        msg = "✅ Апартамент удалён." if success else f"Апартамент '{apt_name}' не найден."
        await update.message.reply_text(msg, reply_markup=menu); return

    if text_lower.startswith("переименовать ") and " в " in text_lower:
        parts = text[14:].split(" в ", 1)
        if len(parts) == 2:
            old, new = parts[0].strip(), parts[1].strip()
            success = await rename_apartment(user_id, old, new)
            msg = f"✅ Переименовано в '{new}'" if success else f"Апартамент '{old}' не найден."
            await update.message.reply_text(msg, reply_markup=menu)
        return

    if text_lower.startswith("сдал "):
        raw = text[5:].strip()
        tokens = raw.split()
        if len(tokens) < 3:
            await update.message.reply_text("Формат: 'сдал 334 сутки 15000' или 'сдал 334 часовой 5000'", reply_markup=menu); return
        type_keywords = {"сутки", "суточный", "суточная", "часовой", "час", "часов", "часовая"}
        apt_tokens, rest_tokens, found_type = [], [], False
        for tok in tokens:
            if tok.lower() in type_keywords and not found_type:
                found_type = True; rest_tokens.append(tok)
            elif found_type:
                rest_tokens.append(tok)
            else:
                apt_tokens.append(tok)
        if not found_type:
            await update.message.reply_text("Укажи тип: 'сутки' или 'часовой'\nПример: 'сдал 334 сутки 15000'", reply_markup=menu); return
        apt_name = " ".join(apt_tokens).strip()
        apt = await find_apartment(user_id, apt_name)
        if not apt:
            await update.message.reply_text(f"Апартамент '{apt_name}' не найден.\n\nНажми 🏠 Апартаменты чтобы увидеть список.", reply_markup=menu); return

        checkin_type = "daily"
        for tok in rest_tokens:
            if tok.lower() in {"часовой", "час", "часов", "часовая"}:
                checkin_type = "hourly"; break

        amount, checkin_date, hours = 0, None, 2
        for tok in rest_tokens:
            if tok.lower() in type_keywords:
                continue
            h = parse_hours_token(tok)
            if h:
                hours = h; continue
            if is_date_token(tok):
                d = parse_date(tok)
                if d:
                    checkin_date = d.isoformat(); continue
            if is_amount_token(tok):
                val = float(tok.replace(",", ""))
                if val > amount:
                    amount = val

        if amount <= 0:
            await update.message.reply_text("Укажи сумму.\nПример: 'сдал 334 сутки 15000'", reply_markup=menu); return

        note = f"{hours}ч" if checkin_type == "hourly" else ""
        check_in_dt = datetime.fromisoformat(checkin_date) if checkin_date else datetime.now()

        await add_checkin(user_id, apt["id"], amount, checkin_type, note=note,
                          checkin_date=checkin_date or check_in_dt.isoformat())

        type_text = "почасово" if checkin_type == "hourly" else "суточный"
        date_text = f" ({checkin_date[:10]})" if checkin_date else ""

        if checkin_type == "daily":
            checkout_dt = get_logical_checkout(check_in_dt)
            checkout_info = f"\n🕐 Выезд: {checkout_dt.strftime('%d.%m')} в 12:00"
        else:
            checkout_dt = get_hourly_checkout(check_in_dt, hours)
            checkout_info = f"\n🕐 Выезд: {checkout_dt.strftime('%H:%M')} (через {hours}ч)"

        await update.message.reply_text(
            f"✅ Записан заезд{date_text}!\n\n🏠 {apt['name']}\n💰 {amount:,.0f} ₸ — {type_text}{checkout_info}",
            reply_markup=menu
        ); return

    if text_lower.startswith("выехал "):
        apt_name = text[7:].strip()
        apt = await find_apartment(user_id, apt_name)
        if not apt:
            await update.message.reply_text(f"Апартамент '{apt_name}' не найден.", reply_markup=menu); return
        active = await get_active_checkin(user_id, apt["id"])
        if not active:
            await update.message.reply_text(f"🟢 {apt['name']} уже свободен.", reply_markup=menu); return
        await checkout_apartment(user_id, apt["id"])
        await update.message.reply_text(f"✅ Гость выехал!\n\n🏠 {apt['name']} — теперь свободен 🟢", reply_markup=menu); return

    if text_lower.startswith("отменить бронь "):
        parts = text[15:].strip().split()
        apt_name = parts[0] if parts else ""
        apt = await find_apartment(user_id, apt_name)
        if not apt:
            await update.message.reply_text(f"Апартамент '{apt_name}' не найден.", reply_markup=menu); return
        cancel_date = None
        if len(parts) > 1:
            d = parse_date(parts[1])
            if d:
                cancel_date = d.date().isoformat()
        if cancel_date:
            bookings = await _get(f"{SUPABASE_URL}/rest/v1/bookings?user_id=eq.{user_id}&apartment_id=eq.{apt['id']}&check_in=eq.{cancel_date}&status=eq.confirmed&select=id")
        else:
            bookings = await _get(f"{SUPABASE_URL}/rest/v1/bookings?user_id=eq.{user_id}&apartment_id=eq.{apt['id']}&status=eq.confirmed&select=id&order=check_in.desc&limit=1")
        if not bookings:
            await update.message.reply_text("Бронь не найдена.", reply_markup=menu); return
        await _patch(f"{SUPABASE_URL}/rest/v1/bookings?id=eq.{bookings[0]['id']}", {"status": "cancelled"})
        date_text = f" на {cancel_date}" if cancel_date else ""
        await update.message.reply_text(f"✅ Бронь{date_text} для {apt['name']} отменена.", reply_markup=menu); return

    if text_lower.startswith("забронировали ") or text_lower.startswith("бронь "):
        parts = text.split()
        if len(parts) < 6:
            await update.message.reply_text("Формат:\n'забронировали 334 Айдар +77001234567 с 15 по 17 апреля'", reply_markup=menu); return
        apt = await find_apartment(user_id, parts[1])
        if not apt:
            await update.message.reply_text(f"Апартамент '{parts[1]}' не найден.", reply_markup=menu); return
        guest_name = parts[2] if len(parts) > 2 else "Гость"
        phone = parts[3] if len(parts) > 3 else ""
        check_in_date  = date.today().isoformat()
        check_out_date = (date.today() + timedelta(days=1)).isoformat()
        for i, p in enumerate(parts):
            if p.lower() in ["с", "от"] and i + 1 < len(parts):
                d = parse_date(parts[i+1])
                if d: check_in_date = d.date().isoformat()
            if p.lower() in ["по", "до"] and i + 1 < len(parts):
                d = parse_date(parts[i+1])
                if d: check_out_date = d.date().isoformat()
        await add_booking(user_id, apt["id"], guest_name, phone, check_in_date, check_out_date)
        await update.message.reply_text(f"📅 Бронь записана!\n\n🏠 {apt['name']}\n👤 {guest_name}\n📞 {phone}\n📆 {check_in_date} → {check_out_date}", reply_markup=menu); return

    if text_lower.startswith("расход "):
        parts = text[7:].strip().split()
        if len(parts) < 2:
            await update.message.reply_text(
                "Формат:\n'расход горничная 30000 общий'\n'расход горничная 30000 общий 15.03'\n'расход 334 ремонт 50000'\n'расход 334 ремонт 50000 20.03'",
                reply_markup=menu); return

        is_shared = "общий" in text_lower or "общ" in text_lower

        amount = 0
        expense_date = None
        for p in parts:
            if is_date_token(p):
                d = parse_date(p)
                if d:
                    expense_date = d.isoformat()
                continue
            if is_amount_token(p):
                try:
                    val = float(p.replace(",", ""))
                    if val > amount:
                        amount = val
                except ValueError:
                    pass

        if amount <= 0:
            await update.message.reply_text("Укажи сумму.", reply_markup=menu); return

        apt = None
        if not is_shared:
            apt = await find_apartment(user_id, parts[0])

        category = parts[0] if not apt else (parts[1] if len(parts) > 1 else "прочее")

        await add_expense(
            user_id, amount, category, " ".join(parts),
            apt_id=apt["id"] if apt else None,
            is_shared=is_shared,
            expense_date=expense_date
        )

        apt_text = f"\n🏠 {apt['name']}" if apt else "\n📦 Общий расход"
        date_text = f"\n📅 Дата: {expense_date[:10]}" if expense_date else ""
        await update.message.reply_text(
            f"❌ Расход записан!{apt_text}\n💰 {amount:,.0f} ₸ — {category}{date_text}",
            reply_markup=menu); return

    if text_lower.startswith("отчёт ") or text_lower.startswith("отчет "):
        query = text.split(" ", 1)[1].strip()
        months = {"январь":1,"февраль":2,"март":3,"апрель":4,"май":5,"июнь":6,"июль":7,"август":8,"сентябрь":9,"октябрь":10,"ноябрь":11,"декабрь":12}
        if query.lower() in months:
            await update.message.reply_text(await get_monthly_report(user_id, month=months[query.lower()]), reply_markup=menu)
        else:
            apt = await find_apartment(user_id, query)
            if apt:
                await update.message.reply_text(await get_apt_report(user_id, apt), reply_markup=menu)
            else:
                await update.message.reply_text(f"Апартамент '{query}' не найден.", reply_markup=menu)
        return

    if text_lower in ["отмена", "отменить"]:
        action_type, detail = await undo_last_action(user_id)
        if action_type == "заезд":
            await update.message.reply_text("↩️ Последний заезд удалён.", reply_markup=menu)
        elif action_type == "расход":
            await update.message.reply_text(f"↩️ Последний расход удалён: {detail}", reply_markup=menu)
        else:
            await update.message.reply_text("Нечего отменять.", reply_markup=menu)
        return

    if str(telegram_id) == ADMIN_ID and text_lower in ["👑 админ", "админ"]:
        users = await _get(f"{SUPABASE_URL}/rest/v1/users?select=id,name,telegram_id")
        lines = [f"👑 Pater AI — пользователи\n\n👥 Всего: {len(users)}\n"]
        for u in users:
            lines.append(f"• {u.get('name','?')} ({u.get('telegram_id','')})")
        await update.message.reply_text("\n".join(lines), reply_markup=menu); return

    await update.message.reply_text("Не понял. Нажми 🤖 Команды чтобы увидеть все доступные команды.", reply_markup=menu)

async def start():
    app = Application.builder().token(TELEGRAM_TOKEN).build()
    app.add_handler(CommandHandler("start", handle_start))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))

    scheduler = AsyncIOScheduler(timezone=KZ_TZ)
    scheduler.add_job(send_booking_reminders, "cron", hour=9, minute=0, args=[app])
    scheduler.add_job(auto_checkout_daily, "cron", hour=12, minute=0, args=[app])
    scheduler.add_job(auto_checkout_hourly, "interval", minutes=15, args=[app])
    scheduler.start()

    logger.info("✅ Pater AI запущен!")
    async with app:
        await app.start()
        await app.updater.start_polling()
        await asyncio.Event().wait()

if __name__ == "__main__":
    asyncio.run(start())