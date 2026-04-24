import os
import json
import asyncio
import logging
import time
from collections import defaultdict
from datetime import datetime, date, timedelta, timezone
from dotenv import load_dotenv

load_dotenv()

from telegram import Update, ReplyKeyboardMarkup, KeyboardButton, InlineKeyboardMarkup, InlineKeyboardButton
from telegram.ext import Application, MessageHandler, CommandHandler, CallbackQueryHandler, filters, ContextTypes
from apscheduler.schedulers.asyncio import AsyncIOScheduler
import httpx
import pytz

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
logger = logging.getLogger(__name__)

TELEGRAM_TOKEN = os.environ["TELEGRAM_TOKEN"]
SUPABASE_URL   = os.environ["SUPABASE_URL"]
SUPABASE_KEY   = os.environ["SUPABASE_KEY"]
GROQ_API_KEY   = os.environ["GROQ_API_KEY"]
ADMIN_ID       = "539648155"
CONTACT        = "@GPTttp"
KZ_TZ          = pytz.timezone("Asia/Almaty")

HEADERS = {
    "apikey": SUPABASE_KEY,
    "Authorization": f"Bearer {SUPABASE_KEY}",
    "Content-Type": "application/json",
    "Prefer": "return=representation"
}

# ─── ПЛАНЫ ПОДПИСОК ──────────────────────────────────────────────

PLANS = {
    "trial":     {"days": 7,   "price": 0,      "label": "🎁 Пробный (7 дней)"},
    "monthly":   {"days": 30,  "price": 7990,   "label": "📅 1 месяц — 7,990 ₸"},
    "quarterly": {"days": 90,  "price": 19990,  "label": "📆 3 месяца — 19,990 ₸"},
    "yearly":    {"days": 365, "price": 69990,  "label": "🗓 1 год — 69,990 ₸"},
}

def utcnow() -> datetime:
    return datetime.now(timezone.utc)

def now_kz() -> datetime:
    return datetime.now(KZ_TZ).replace(tzinfo=None)

_last_msg: dict = defaultdict(float)

def is_rate_limited(telegram_id: str) -> bool:
    now = time.time()
    if now - _last_msg[telegram_id] < 1.5:
        return True
    _last_msg[telegram_id] = now
    return False

# ─── МЕНЮ ────────────────────────────────────────────────────────

MENU = ReplyKeyboardMarkup([
    [KeyboardButton("🏠 Апартаменты"), KeyboardButton("📊 Статус")],
    [KeyboardButton("📅 Брони"), KeyboardButton("💰 Отчёт за месяц")],
    [KeyboardButton("➕ Добавить"), KeyboardButton("🤖 Команды")],
    [KeyboardButton("📋 Подписка")],
], resize_keyboard=True)

ADMIN_MENU = ReplyKeyboardMarkup([
    [KeyboardButton("🏠 Апартаменты"), KeyboardButton("📊 Статус")],
    [KeyboardButton("📅 Брони"), KeyboardButton("💰 Отчёт за месяц")],
    [KeyboardButton("➕ Добавить"), KeyboardButton("🤖 Команды")],
    [KeyboardButton("📋 Подписка"), KeyboardButton("👑 Админ")],
], resize_keyboard=True)

def get_menu(telegram_id):
    return ADMIN_MENU if str(telegram_id) == ADMIN_ID else MENU

def make_approval_keyboard(telegram_id: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("📅 Месяц",    callback_data=f"grant:{telegram_id}:monthly"),
            InlineKeyboardButton("📆 3 месяца", callback_data=f"grant:{telegram_id}:quarterly"),
            InlineKeyboardButton("🗓 Год",       callback_data=f"grant:{telegram_id}:yearly"),
        ],
        [InlineKeyboardButton("🔒 Не продлевать", callback_data=f"revoke:{telegram_id}")]
    ])

COMMANDS_TEXT = """🏠 Pater AI — всё что я умею:

🏠 Апартаменты:
"добавить 334" — добавить апартамент
"удалить 334" — удалить апартамент
"переименовать 334 в Люкс" — переименовать

🏃 Заезды (слова можно в любом порядке):
"сдал 334 сутки 15000"
"сдал 334 часовой 5000"
"сдал 334 часовой 5000 3ч" — на 3 часа
"сдал 334 сутки 15000 01.04" — задним числом
"выехал 334" — гость выехал досрочно

📅 Брони:
"забронировали 334 Айдар +77001234567 с 15 по 17 апреля"
"отменить бронь 334 22.04"

💰 Расходы:
"расход горничная 30000 общий"
"расход горничная 30000 общий 15.03"
"расход 334 ремонт 50000"
"расход 334 ремонт 50000 20.03"

📊 Отчёты:
"отчёт 334" — по конкретному апартаменту
"отчёт апрель" — за месяц
"статус" — кто занят сейчас

❌ Отмена:
"отмена" — удалить последнее действие"""

# ─── HTTP HELPERS ─────────────────────────────────────────────────

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

# ─── ПОДПИСКИ ────────────────────────────────────────────────────

async def get_subscription(telegram_id: str) -> dict | None:
    data = await _get(
        f"{SUPABASE_URL}/rest/v1/subscriptions?telegram_id=eq.{telegram_id}&select=*&limit=1"
    )
    return data[0] if data else None

async def has_access(telegram_id: str) -> bool:
    if str(telegram_id) == ADMIN_ID:
        return True
    sub = await get_subscription(telegram_id)
    if not sub:
        return False
    if not sub.get("is_active"):
        return False
    expires = datetime.fromisoformat(sub["expires_at"].replace("Z", "+00:00"))
    return expires > utcnow()

async def grant_subscription(telegram_id: str, plan: str, granted_by: str,
                              full_name: str = None, notes: str = None) -> dict:
    if plan not in PLANS:
        raise ValueError(f"Неизвестный тариф '{plan}'")
    days = PLANS[plan]["days"]
    now = utcnow()
    existing = await get_subscription(telegram_id)
    if existing:
        current_exp = datetime.fromisoformat(existing["expires_at"].replace("Z", "+00:00"))
        base = max(current_exp, now)
        new_expires = (base + timedelta(days=days)).isoformat()
        await _patch(
            f"{SUPABASE_URL}/rest/v1/subscriptions?telegram_id=eq.{telegram_id}",
            {
                "plan": plan,
                "expires_at": new_expires,
                "is_active": True,
                "granted_by": granted_by,
                "notes": notes,
                "updated_at": now.isoformat()
            }
        )
    else:
        new_expires = (now + timedelta(days=days)).isoformat()
        await _post(f"{SUPABASE_URL}/rest/v1/subscriptions", {
            "telegram_id": telegram_id,
            "full_name": full_name,
            "plan": plan,
            "started_at": now.isoformat(),
            "expires_at": new_expires,
            "is_active": True,
            "granted_by": granted_by,
            "notes": notes
        })
    return {"plan": plan, "expires_at": new_expires, "days": days}

async def revoke_subscription(telegram_id: str):
    await _patch(
        f"{SUPABASE_URL}/rest/v1/subscriptions?telegram_id=eq.{telegram_id}",
        {"is_active": False, "updated_at": utcnow().isoformat()}
    )

async def get_subscription_stats() -> dict:
    now_str = utcnow().isoformat()
    all_subs = await _get(f"{SUPABASE_URL}/rest/v1/subscriptions?select=plan,is_active,expires_at")
    active = [s for s in all_subs if s.get("is_active") and s.get("expires_at", "") > now_str]
    return {
        "active": len(active),
        "trial":  sum(1 for s in active if s.get("plan") == "trial"),
        "paid":   sum(1 for s in active if s.get("plan") != "trial"),
        "total":  len(all_subs)
    }

# ─── ПОЛЬЗОВАТЕЛИ ────────────────────────────────────────────────

async def get_or_create_user(telegram_id, name):
    data = await _get(f"{SUPABASE_URL}/rest/v1/users?telegram_id=eq.{telegram_id}&select=id")
    if data:
        return data[0]["id"]
    result = await _post(f"{SUPABASE_URL}/rest/v1/users", {"telegram_id": telegram_id, "name": name})
    return result[0]["id"] if result else None

# ─── АПАРТАМЕНТЫ ─────────────────────────────────────────────────

async def get_apartments(user_id):
    return await _get(f"{SUPABASE_URL}/rest/v1/apartments?user_id=eq.{user_id}&is_active=eq.true&select=id,name&order=name")

async def find_apartment(user_id, name_part):
    """
    Только точное совпадение по первому слову номера.
    "3" → только "3 кв", никогда не "23" или "334".
    """
    apts = await get_apartments(user_id)
    name_part = name_part.lower().strip()
    for a in apts:
        if a["name"].lower() == name_part:
            return a
    for a in apts:
        apt_number = a["name"].lower().split()[0]
        query_number = name_part.split()[0]
        if apt_number == query_number:
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

# ─── ЗАЕЗДЫ ──────────────────────────────────────────────────────

async def add_checkin(user_id, apt_id, amount, checkin_type, note="", checkin_date=None):
    return await _post(f"{SUPABASE_URL}/rest/v1/checkins", {
        "user_id": user_id, "apartment_id": apt_id, "amount": amount,
        "type": checkin_type, "note": note,
        "check_in": checkin_date or now_kz().isoformat()
    })

async def close_previous_checkin(user_id, apt_id, new_check_in_dt: datetime):
    existing = await get_active_checkin(user_id, apt_id)
    if existing:
        close_time = (new_check_in_dt - timedelta(minutes=1)).isoformat()
        await _patch(
            f"{SUPABASE_URL}/rest/v1/checkins?id=eq.{existing['id']}",
            {"check_out": close_time}
        )

async def checkout_apartment(user_id, apt_id):
    return await _patch(
        f"{SUPABASE_URL}/rest/v1/checkins?user_id=eq.{user_id}&apartment_id=eq.{apt_id}&check_out=is.null",
        {"check_out": now_kz().isoformat()})

async def get_active_checkin(user_id, apt_id):
    data = await _get(
        f"{SUPABASE_URL}/rest/v1/checkins"
        f"?user_id=eq.{user_id}&apartment_id=eq.{apt_id}"
        f"&check_out=is.null&select=id,amount,type,check_in,note"
        f"&order=check_in.desc&limit=1"
    )
    return data[0] if data else None

# ─── БРОНИ ───────────────────────────────────────────────────────

async def add_booking(user_id, apt_id, guest_name, phone, check_in, check_out, amount=0):
    return await _post(f"{SUPABASE_URL}/rest/v1/bookings", {
        "user_id": user_id, "apartment_id": apt_id, "guest_name": guest_name,
        "phone": phone, "check_in": check_in, "check_out": check_out,
        "amount": amount, "status": "confirmed"
    })

async def get_bookings(user_id):
    today = now_kz().date().isoformat()
    return await _get(f"{SUPABASE_URL}/rest/v1/bookings?user_id=eq.{user_id}&check_out=gte.{today}&status=eq.confirmed&select=id,guest_name,phone,check_in,check_out,amount,apartment_id&order=check_in")

# ─── РАСХОДЫ ─────────────────────────────────────────────────────

async def add_expense(user_id, amount, category, comment, apt_id=None, is_shared=False, expense_date=None):
    payload = {
        "user_id": user_id, "apartment_id": apt_id, "amount": amount,
        "category": category, "comment": comment, "is_shared": is_shared
    }
    if expense_date:
        payload["created_at"] = expense_date
    return await _post(f"{SUPABASE_URL}/rest/v1/expenses", payload)

# ─── ЛОГИКА ВЫЕЗДА ───────────────────────────────────────────────

def get_logical_checkout(check_in_dt: datetime) -> datetime:
    if check_in_dt.hour < 6:
        checkout_date = check_in_dt.date()
    else:
        checkout_date = check_in_dt.date() + timedelta(days=1)
    return datetime.combine(checkout_date, datetime.strptime("12:00", "%H:%M").time())

def get_hourly_checkout(check_in_dt: datetime, hours: int) -> datetime:
    return check_in_dt + timedelta(hours=hours)

def get_hours_from_note(note: str) -> int:
    """Извлечь количество часов из note. Всегда возвращает число, по умолчанию 2."""
    try:
        if note and note.strip().endswith("ч"):
            h = int(note.strip().replace("ч", "").strip())
            if h > 0:
                return h
    except (ValueError, AttributeError):
        pass
    return 2

# ─── СТАТУС ──────────────────────────────────────────────────────

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
                hours = get_hours_from_note(active.get("note", ""))
                checkout_dt = get_hourly_checkout(check_in_dt, hours)
                lines.append(f"🔴 {apt['name']} — почасово до {checkout_dt.strftime('%H:%M')}")
            else:
                checkout_dt = get_logical_checkout(check_in_dt)
                lines.append(f"🔴 {apt['name']} — занят до {checkout_dt.strftime('%d.%m')} 12:00")
        else:
            lines.append(f"🟢 {apt['name']} — свободен")
    return "\n".join(lines)

# ─── ОТЧЁТЫ ──────────────────────────────────────────────────────

async def get_monthly_report(user_id, year=None, month=None):
    now = now_kz()
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
    now = now_kz()
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

# ─── ПАРСИНГ ─────────────────────────────────────────────────────

def parse_date(date_str):
    date_str = date_str.strip()
    for fmt in ["%d.%m", "%d.%m.%Y", "%d.%m.%y"]:
        try:
            d = datetime.strptime(date_str, fmt)
            if fmt == "%d.%m":
                d = d.replace(year=now_kz().year)
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
    token = token.lower().strip()
    if token.endswith("ч") and token[:-1].isdigit():
        return int(token[:-1])
    return None

# ─── GROQ ПАРСЕР ДЛЯ "СДАЛ" ─────────────────────────────────────

async def parse_sdal_with_groq(text: str, apt_names: list[str]) -> dict | None:
    """
    Использует Groq чтобы разобрать команду сдал в любом порядке слов.
    Возвращает словарь с: apt_name, checkin_type, amount, hours, date
    или None если не удалось разобрать.
    """
    url = "https://api.groq.com/openai/v1/chat/completions"
    headers_g = {"Authorization": f"Bearer {GROQ_API_KEY}", "Content-Type": "application/json"}

    apts_str = ", ".join(apt_names) if apt_names else "нет апартаментов"

    prompt = f"""Разбери команду заезда в апартамент. Слова могут быть в любом порядке.

Сообщение: "{text}"

Список апартаментов пользователя: {apts_str}

Правила:
- apt_name: название апартамента из списка (первое слово номера должно совпадать)
- checkin_type: "daily" если сутки/суточный, "hourly" если часовой/час/почасово
- amount: сумма в тенге (число)
- hours: количество часов для почасового (если не указано явно — 2), для суточного null
- date: дата в формате YYYY-MM-DD если указана, иначе null

Верни только JSON без пояснений:
{{"apt_name": "название", "checkin_type": "daily/hourly", "amount": число, "hours": число_или_null, "date": "YYYY-MM-DD или null"}}

Если не можешь определить апартамент или сумму — верни {{"error": "причина"}}"""

    body = {
        "model": "llama-3.3-70b-versatile",
        "messages": [{"role": "user", "content": prompt}],
        "temperature": 0.1
    }

    try:
        async with httpx.AsyncClient(timeout=15.0) as client:
            r = await client.post(url, headers=headers_g, json=body)
            content = r.json()["choices"][0]["message"]["content"].strip()
        if "```" in content:
            content = content.split("```")[1]
            if content.startswith("json"):
                content = content[4:]
        return json.loads(content.strip())
    except Exception as e:
        logger.error(f"Groq parse error: {e}")
        return None

# ─── ОТМЕНА ──────────────────────────────────────────────────────

async def undo_last_action(user_id):
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

# ─── ПЛАНИРОВЩИК ─────────────────────────────────────────────────

async def check_subscriptions(app):
    now = utcnow()
    now_str = now.isoformat()

    # Истёкшие
    expired = await _get(
        f"{SUPABASE_URL}/rest/v1/subscriptions?is_active=eq.true&expires_at=lt.{now_str}&select=telegram_id,full_name,plan"
    )
    for sub in expired:
        tid = sub["telegram_id"]
        await _patch(
            f"{SUPABASE_URL}/rest/v1/subscriptions?telegram_id=eq.{tid}",
            {"is_active": False, "updated_at": now_str}
        )
        try:
            await app.bot.send_message(chat_id=tid, text=(
                "⏰ *Ваш пробный период закончился*\n\n"
                "Для продолжения выберите тариф:\n"
                "📅 1 месяц — 7,990 ₸\n"
                "📆 3 месяца — 19,990 ₸\n"
                "🗓 1 год — 69,990 ₸\n\n"
                f"Оплата: {CONTACT}"), parse_mode="Markdown")
        except Exception as e:
            logger.warning(f"Не удалось уведомить {tid}: {e}")
        try:
            await app.bot.send_message(chat_id=ADMIN_ID, text=(
                f"⏰ *Подписка истекла*\n\n"
                f"👤 {sub.get('full_name', tid)}\n🆔 `{tid}`\n"
                f"📦 {PLANS.get(sub['plan'], {}).get('label', sub['plan'])}\n\nПродлить?"),
                parse_mode="Markdown",
                reply_markup=make_approval_keyboard(tid))
        except Exception as e:
            logger.warning(f"Не удалось уведомить админа: {e}")

    # Предупреждения за 1 и 3 дня
    for days_before in [3, 1]:
        t_start = (now + timedelta(days=days_before-1)).replace(hour=0, minute=0, second=0, microsecond=0)
        t_end   = (now + timedelta(days=days_before)).replace(hour=0, minute=0, second=0, microsecond=0)
        warnings = await _get(
            f"{SUPABASE_URL}/rest/v1/subscriptions?is_active=eq.true"
            f"&expires_at=gte.{t_start.isoformat()}&expires_at=lt.{t_end.isoformat()}&select=telegram_id,expires_at"
        )
        for sub in warnings:
            try:
                exp = datetime.fromisoformat(sub["expires_at"].replace("Z", "+00:00"))
                emoji = "⚠️" if days_before == 1 else "📢"
                await app.bot.send_message(chat_id=sub["telegram_id"], text=(
                    f"{emoji} *Подписка истекает через {days_before} {'день' if days_before==1 else 'дня'}*\n\n"
                    f"До: *{exp.strftime('%d.%m.%Y')}*\n\nПродлить: {CONTACT}"),
                    parse_mode="Markdown")
            except Exception as e:
                logger.warning(f"Предупреждение не отправлено: {e}")

async def auto_checkout_daily(app):
    """Автовыселение суточных в 12:00."""
    now = now_kz()
    logger.info(f"⏰ Автовыселение суточных в {now}")
    users = await _get(f"{SUPABASE_URL}/rest/v1/users?select=id,telegram_id")
    for user in users:
        user_id = user["id"]
        telegram_id = user["telegram_id"]
        apts = await get_apartments(user_id)
        for apt in apts:
            active = await get_active_checkin(user_id, apt["id"])
            if not active or active["type"] != "daily":
                continue
            check_in_dt = datetime.fromisoformat(active["check_in"])
            expected_checkout = get_logical_checkout(check_in_dt)
            if now >= expected_checkout:
                await _patch(
                    f"{SUPABASE_URL}/rest/v1/checkins?id=eq.{active['id']}",
                    {"check_out": expected_checkout.isoformat()}
                )
                logger.info(f"✅ Выселен: {apt['name']} пользователь {telegram_id}")
                try:
                    await app.bot.send_message(
                        chat_id=telegram_id,
                        text=f"🟢 {apt['name']} — автоматически освобождён (выезд 12:00)"
                    )
                except Exception as e:
                    logger.warning(f"Уведомление не отправлено: {e}")

async def auto_checkout_hourly(app):
    """
    Автовыселение почасовых каждые 15 минут.
    Часы берутся из note через get_hours_from_note — всегда корректно.
    """
    now = now_kz()
    users = await _get(f"{SUPABASE_URL}/rest/v1/users?select=id,telegram_id")
    for user in users:
        user_id = user["id"]
        telegram_id = user["telegram_id"]
        apts = await get_apartments(user_id)
        for apt in apts:
            active = await get_active_checkin(user_id, apt["id"])
            if not active or active["type"] != "hourly":
                continue
            check_in_dt = datetime.fromisoformat(active["check_in"])
            hours = get_hours_from_note(active.get("note", ""))
            expected_checkout = get_hourly_checkout(check_in_dt, hours)
            if now >= expected_checkout:
                await _patch(
                    f"{SUPABASE_URL}/rest/v1/checkins?id=eq.{active['id']}",
                    {"check_out": expected_checkout.isoformat()}
                )
                logger.info(f"✅ Почасовой выселен: {apt['name']} пользователь {telegram_id}")
                try:
                    await app.bot.send_message(
                        chat_id=telegram_id,
                        text=f"🟢 {apt['name']} — почасовой заезд завершён (через {hours}ч)"
                    )
                except Exception as e:
                    logger.warning(f"Уведомление не отправлено: {e}")

async def send_booking_reminders(app):
    tomorrow = (now_kz().date() + timedelta(days=1)).isoformat()
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

# ─── CALLBACK ────────────────────────────────────────────────────

async def handle_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    if str(query.from_user.id) != ADMIN_ID:
        await query.answer("Нет доступа.", show_alert=True)
        return
    parts = query.data.split(":")
    if parts[0] == "grant":
        target_id, plan = parts[1], parts[2]
        user_info = await _get(f"{SUPABASE_URL}/rest/v1/users?telegram_id=eq.{target_id}&select=name")
        name = user_info[0]["name"] if user_info else target_id
        result = await grant_subscription(target_id, plan, ADMIN_ID, full_name=name)
        exp = datetime.fromisoformat(result["expires_at"].replace("Z", "+00:00"))
        plan_label = PLANS[plan]["label"]
        await query.edit_message_text(
            f"✅ *Выдано!*\n\n👤 {name} (`{target_id}`)\n📦 {plan_label}\n📅 До: *{exp.strftime('%d.%m.%Y')}*",
            parse_mode="Markdown")
        try:
            await context.bot.send_message(
                chat_id=target_id,
                text=f"✅ *Подписка активирована!*\n\n📦 {plan_label}\n📅 До: *{exp.strftime('%d.%m.%Y')}*\n\nНажми /start!",
                parse_mode="Markdown")
        except Exception as e:
            logger.warning(f"Не удалось уведомить {target_id}: {e}")
    elif parts[0] == "revoke":
        target_id = parts[1]
        await revoke_subscription(target_id)
        user_info = await _get(f"{SUPABASE_URL}/rest/v1/users?telegram_id=eq.{target_id}&select=name")
        name = user_info[0]["name"] if user_info else target_id
        await query.edit_message_text(f"🔒 *Не продлено*\n\n👤 {name} (`{target_id}`)", parse_mode="Markdown")
        try:
            await context.bot.send_message(chat_id=target_id, text=f"⛔ Доступ приостановлен.\n\nПо вопросам: {CONTACT}")
        except Exception as e:
            logger.warning(f"Не удалось уведомить {target_id}: {e}")

# ─── START ────────────────────────────────────────────────────────

async def handle_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    telegram_id = str(update.effective_user.id)
    name = update.effective_user.full_name
    username = update.effective_user.username or ""
    await get_or_create_user(telegram_id, name)
    menu = get_menu(telegram_id)

    if str(telegram_id) == ADMIN_ID:
        await update.message.reply_text(
            f"👋 Привет, {name}!\n\n🏠 Я Pater AI — помогаю управлять апартаментами, мини-отелями и хостелами.\n\nНажми 🤖 Команды чтобы увидеть всё что я умею.",
            reply_markup=menu)
        return

    existing_sub = await get_subscription(telegram_id)

    if existing_sub and existing_sub.get("is_active"):
        expires = datetime.fromisoformat(existing_sub["expires_at"].replace("Z", "+00:00"))
        if expires > utcnow():
            await update.message.reply_text(
                f"👋 Привет, {name}!\n\n🏠 Я Pater AI — помогаю управлять апартаментами, мини-отелями и хостелами.\n\nНажми 🤖 Команды чтобы увидеть всё что я умею.",
                reply_markup=menu)
            return

    if existing_sub and existing_sub.get("plan") == "trial":
        await update.message.reply_text(
            f"👋 Привет, {name}!\n\n⏰ Ваш пробный период завершён.\n\nДля продолжения: {CONTACT}\n\nНапишите 'подписка' чтобы проверить статус.")
        return

    result = await grant_subscription(telegram_id, "trial", "auto", full_name=name)
    exp = datetime.fromisoformat(result["expires_at"].replace("Z", "+00:00"))
    await update.message.reply_text(
        f"👋 Привет, {name}!\n\n"
        f"🎁 Вам активирован *пробный период на 7 дней*\n"
        f"📅 До: *{exp.strftime('%d.%m.%Y')}*\n\n"
        "🏠 Я помогаю управлять апартаментами, мини-отелями и хостелами.\n\n"
        "Начни с добавления объекта:\n'добавить 334'\n\n"
        "_Нажми_ 🤖 _Команды чтобы увидеть всё что я умею_",
        parse_mode="Markdown", reply_markup=menu)

    username_line = f"@{username}" if username else "нет username"
    try:
        await context.bot.send_message(chat_id=ADMIN_ID, text=(
            f"🆕 *Новый пользователь!*\n\n"
            f"👤 {name}\n🔗 {username_line}\n🆔 `{telegram_id}`\n\n"
            f"🎁 Пробный период (7 дней)\n"
            f"📅 До: *{exp.strftime('%d.%m.%Y')}*\n\nЧто делаем после пробного?"),
            parse_mode="Markdown",
            reply_markup=make_approval_keyboard(telegram_id))
    except Exception as e:
        logger.warning(f"Не удалось уведомить админа: {e}")

# ─── MESSAGE HANDLER ──────────────────────────────────────────────

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

    # ─── Подписка — доступна всем ───
    if text_lower in ["📋 подписка", "подписка", "/mysub"]:
        sub = await get_subscription(telegram_id)
        if not sub:
            await update.message.reply_text(f"❌ У вас нет подписки.\n\nДля подключения: {CONTACT}")
        else:
            exp = datetime.fromisoformat(sub["expires_at"].replace("Z", "+00:00"))
            days_left = (exp - utcnow()).days
            is_active = sub["is_active"] and exp > utcnow()
            plan_label = PLANS.get(sub["plan"], {}).get("label", sub["plan"])
            status = f"✅ Активна • осталось *{max(days_left,0)} дн.*" if is_active else "🔴 Истекла"
            await update.message.reply_text(
                f"📋 *Ваша подписка*\n\n📦 {plan_label}\n📅 До: *{exp.strftime('%d.%m.%Y')}*\n🔘 {status}\n\nПродление: {CONTACT}",
                parse_mode="Markdown", reply_markup=menu)
        return

    # ─── Проверка доступа ───
    if not await has_access(telegram_id):
        await update.message.reply_text(
            f"🔒 Доступ закрыт.\n\nДля продолжения: {CONTACT}\n\nНапишите 'подписка' чтобы проверить статус.")
        return

    # ─── Команды ───
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

    # ─── СДАЛ — через Groq парсер ───
    if text_lower.startswith("сдал "):
        apts = await get_apartments(user_id)
        apt_names = [a["name"] for a in apts]

        parsed = await parse_sdal_with_groq(text, apt_names)

        if not parsed or "error" in parsed:
            err = parsed.get("error", "не удалось разобрать") if parsed else "ошибка соединения"
            await update.message.reply_text(
                f"Не понял команду: {err}\n\nПример:\n'сдал 334 сутки 15000'\n'сдал 334 часовой 5000'",
                reply_markup=menu); return

        apt_name = parsed.get("apt_name", "")
        apt = await find_apartment(user_id, apt_name)
        if not apt:
            await update.message.reply_text(
                f"Апартамент '{apt_name}' не найден.\n\nНажми 🏠 Апартаменты чтобы увидеть список.",
                reply_markup=menu); return

        checkin_type = parsed.get("checkin_type", "daily")
        amount = float(parsed.get("amount", 0))
        hours = int(parsed.get("hours") or 2)
        date_str = parsed.get("date")

        if amount <= 0:
            await update.message.reply_text("Укажи сумму.\nПример: 'сдал 334 сутки 15000'", reply_markup=menu); return

        # note всегда сохраняем с часами для почасовых
        note = f"{hours}ч" if checkin_type == "hourly" else ""

        check_in_dt = datetime.fromisoformat(date_str + "T" + now_kz().strftime("%H:%M:%S")) if date_str else now_kz()

        await close_previous_checkin(user_id, apt["id"], check_in_dt)
        await add_checkin(user_id, apt["id"], amount, checkin_type, note=note,
                          checkin_date=check_in_dt.isoformat())

        type_text = "почасово" if checkin_type == "hourly" else "суточный"
        date_text = f" ({date_str})" if date_str else ""

        if checkin_type == "daily":
            checkout_dt = get_logical_checkout(check_in_dt)
            checkout_info = f"\n🕐 Выезд: {checkout_dt.strftime('%d.%m')} в 12:00"
        else:
            checkout_dt = get_hourly_checkout(check_in_dt, hours)
            checkout_info = f"\n🕐 Выезд: {checkout_dt.strftime('%H:%M')} (через {hours}ч)"

        await update.message.reply_text(
            f"✅ Записан заезд{date_text}!\n\n🏠 {apt['name']}\n💰 {amount:,.0f} ₸ — {type_text}{checkout_info}",
            reply_markup=menu); return

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
        check_in_date  = now_kz().date().isoformat()
        check_out_date = (now_kz().date() + timedelta(days=1)).isoformat()
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
                "Формат:\n'расход горничная 30000 общий'\n'расход 334 ремонт 50000'",
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
        if apt:
            apt_number = apt["name"].split()[0].lower()
            category = "прочее"
            for p in parts:
                if p.lower() == apt_number: continue
                if is_amount_token(p) or is_date_token(p): continue
                if p.lower() in ["общий", "общ"]: continue
                category = p; break
        else:
            category = parts[0]
        await add_expense(user_id, amount, category, " ".join(parts),
            apt_id=apt["id"] if apt else None, is_shared=is_shared, expense_date=expense_date)
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

    # ─── Админ ───
    if str(telegram_id) == ADMIN_ID:
        if text_lower in ["👑 админ", "админ"]:
            users = await _get(f"{SUPABASE_URL}/rest/v1/users?select=id,name,telegram_id")
            s = await get_subscription_stats()
            lines = [
                f"👑 Pater AI — Админ панель\n",
                f"👥 Пользователей: {len(users)}",
                f"🟢 Активных подписок: {s['active']}",
                f"🎁 Пробных: {s['trial']}",
                f"💳 Платных: {s['paid']}\n",
                "━━━━━━━━━━━━━━━━━━━━"
            ]
            for u in users:
                sub = await get_subscription(str(u.get("telegram_id", "")))
                if sub:
                    exp = datetime.fromisoformat(sub["expires_at"].replace("Z", "+00:00"))
                    is_active = sub["is_active"] and exp > utcnow()
                    plan_label = PLANS.get(sub["plan"], {}).get("label", sub["plan"]) if is_active else "истекла"
                    lines.append(f"\n{'✅' if is_active else '🔒'} {u.get('name','?')} ({u.get('telegram_id','')})")
                    lines.append(f"  📦 {plan_label} • до {exp.strftime('%d.%m.%Y')}")
                else:
                    lines.append(f"\n❌ {u.get('name','?')} ({u.get('telegram_id','')})")
            await update.message.reply_text("\n".join(lines), reply_markup=menu); return

        if text_lower.startswith("grant "):
            parts = text.split()
            if len(parts) < 3:
                await update.message.reply_text("Формат: `grant <id> <план>`", parse_mode="Markdown"); return
            try:
                result = await grant_subscription(parts[1], parts[2].lower(), ADMIN_ID)
                exp = datetime.fromisoformat(result["expires_at"].replace("Z", "+00:00"))
                await update.message.reply_text(
                    f"✅ Выдано `{parts[1]}` — {PLANS[parts[2].lower()]['label']}\n📅 До {exp.strftime('%d.%m.%Y')}",
                    parse_mode="Markdown", reply_markup=menu)
                try:
                    await context.bot.send_message(
                        chat_id=parts[1],
                        text=f"✅ Подписка активирована!\n📦 {PLANS[parts[2].lower()]['label']}\n📅 До {exp.strftime('%d.%m.%Y')}\n\nНажми /start!")
                except Exception as e:
                    logger.warning(f"Не удалось уведомить {parts[1]}: {e}")
            except ValueError as e:
                await update.message.reply_text(f"❌ {e}", reply_markup=menu)
            return

        if text_lower.startswith("revoke "):
            tid = text.split()[1]
            await revoke_subscription(tid)
            await update.message.reply_text(f"🔒 Отозвано у `{tid}`", parse_mode="Markdown", reply_markup=menu)
            try:
                await context.bot.send_message(chat_id=tid, text=f"⛔ Доступ приостановлен.\n{CONTACT}")
            except Exception as e:
                logger.warning(f"Не удалось уведомить {tid}: {e}")
            return

    await update.message.reply_text("Не понял. Нажми 🤖 Команды чтобы увидеть все доступные команды.", reply_markup=menu)

# ─── ЗАПУСК ───────────────────────────────────────────────────────

async def start():
    app = Application.builder().token(TELEGRAM_TOKEN).build()
    app.add_handler(CommandHandler("start", handle_start))
    app.add_handler(CallbackQueryHandler(handle_callback))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))

    scheduler = AsyncIOScheduler(timezone=KZ_TZ)
    scheduler.add_job(send_booking_reminders, "cron", hour=9,  minute=0,  args=[app])
    scheduler.add_job(auto_checkout_daily,    "cron", hour=12, minute=0,  args=[app])
    scheduler.add_job(auto_checkout_hourly,   "interval", minutes=15,     args=[app])
    scheduler.add_job(check_subscriptions,    "cron", hour=9,  minute=5,  args=[app])
    scheduler.start()

    logger.info("✅ Pater AI запущен!")
    async with app:
        await app.start()
        await app.updater.start_polling()
        await asyncio.Event().wait()

if __name__ == "__main__":
    asyncio.run(start())