"""Velqo Shop — bitta jarayon: aiohttp API + statik Mini App + aiogram bot (long polling)."""
import asyncio, hashlib, html, hmac, json, logging, math, os, re, time
from collections import deque
from datetime import datetime, timezone
from decimal import Decimal
from urllib.parse import parse_qsl

import asyncpg
from aiohttp import web
from . import receipt_check as rc
from aiogram.exceptions import TelegramRetryAfter
from aiogram import Bot, Dispatcher, F
from aiogram.filters import Command, CommandStart
from aiogram.types import (BufferedInputFile, CallbackQuery, InlineKeyboardButton,
                           InlineKeyboardMarkup, Message, WebAppInfo)

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("velqo")
BOT_TOKEN = os.environ["BOT_TOKEN"]
DB_URL = os.environ["DATABASE_URL"]
ADMINS = {int(x) for x in os.getenv("ADMIN_IDS", "").split(",") if x.strip()}
STAFF = set()


def admin_ids(): return ADMINS | STAFF


WEBAPP_URL = os.getenv("WEBAPP_URL", "")
PORT = int(os.getenv("PORT", "8080"))
DEV = os.getenv("DEV_MODE") == "1"
STATIC = os.getenv("STATIC_DIR", os.path.join(os.path.dirname(__file__), "..", "..", "webapp"))

DEFAULTS = {"usd_rate": 12650, "star_usd": 0.015, "margin_pct": 10, "min_stars": 50,
            "max_stars": 50000, "star_packs": [50, 100, 250, 500, 1000],
            "premium_prices": {"3": 1499, "6": 2699, "12": 4699},
            "card_number": "", "card_holder": "", "topup_min": 10000, "topup_max": 5000000,
            "force_sub": [], "maintenance": False, "welcome_text": "", "help": {"admins": [], "channels": [], "support": []},
            "ref_percent": 5, "receipt_check": {"mode": "assist", "auto_approve_max": 100000, "max_age_minutes": 60, "auto_reject": False, "require_card_match": False}}
SCHEMA = """
CREATE TABLE IF NOT EXISTS users(id BIGINT PRIMARY KEY, first_name TEXT, username TEXT, photo_url TEXT,
  lang TEXT NOT NULL DEFAULT 'uz', balance BIGINT NOT NULL DEFAULT 0 CHECK(balance>=0),
  is_blocked BOOLEAN NOT NULL DEFAULT false, created_at TIMESTAMPTZ NOT NULL DEFAULT now(), last_seen TIMESTAMPTZ NOT NULL DEFAULT now());
CREATE TABLE IF NOT EXISTS orders(id SERIAL PRIMARY KEY, user_id BIGINT NOT NULL REFERENCES users(id), kind TEXT NOT NULL,
  qty INT NOT NULL, price BIGINT NOT NULL, recipient TEXT NOT NULL, status TEXT NOT NULL DEFAULT 'pending',
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(), done_at TIMESTAMPTZ, note TEXT);
CREATE TABLE IF NOT EXISTS topups(id SERIAL PRIMARY KEY, user_id BIGINT NOT NULL REFERENCES users(id), amount BIGINT NOT NULL,
  rate BIGINT NOT NULL, credit BIGINT NOT NULL, status TEXT NOT NULL DEFAULT 'pending', receipt BYTEA, mime TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(), decided_at TIMESTAMPTZ, note TEXT);
CREATE TABLE IF NOT EXISTS balance_log(id SERIAL PRIMARY KEY, user_id BIGINT NOT NULL, delta BIGINT NOT NULL,
  balance_after BIGINT NOT NULL, reason TEXT, created_at TIMESTAMPTZ NOT NULL DEFAULT now());
CREATE TABLE IF NOT EXISTS settings(key TEXT PRIMARY KEY, value JSONB NOT NULL);
ALTER TABLE topups ADD COLUMN IF NOT EXISTS receipt_sha256 TEXT;
ALTER TABLE topups ADD COLUMN IF NOT EXISTS receipt_dhash TEXT;
ALTER TABLE topups ADD COLUMN IF NOT EXISTS check_json JSONB;
ALTER TABLE topups ADD COLUMN IF NOT EXISTS decided_auto BOOLEAN NOT NULL DEFAULT false;
ALTER TABLE topups ADD COLUMN IF NOT EXISTS decided_by BIGINT;
CREATE TABLE IF NOT EXISTS admins(id BIGINT PRIMARY KEY, added_by BIGINT, created_at TIMESTAMPTZ NOT NULL DEFAULT now());
CREATE TABLE IF NOT EXISTS audit(id SERIAL PRIMARY KEY, admin_id BIGINT NOT NULL, action TEXT NOT NULL, detail JSONB, created_at TIMESTAMPTZ NOT NULL DEFAULT now());
CREATE TABLE IF NOT EXISTS broadcasts(id SERIAL PRIMARY KEY, admin_id BIGINT, text TEXT NOT NULL, total INT NOT NULL DEFAULT 0, sent INT NOT NULL DEFAULT 0,
  failed INT NOT NULL DEFAULT 0, done BOOLEAN NOT NULL DEFAULT false, created_at TIMESTAMPTZ NOT NULL DEFAULT now());
ALTER TABLE users ADD COLUMN IF NOT EXISTS invited_by BIGINT;
ALTER TABLE users ADD COLUMN IF NOT EXISTS ref_earned BIGINT NOT NULL DEFAULT 0;
CREATE TABLE IF NOT EXISTS promos(code TEXT PRIMARY KEY, amount BIGINT NOT NULL CHECK(amount>0), max_uses INT NOT NULL DEFAULT 0, expires_at TIMESTAMPTZ,
  active BOOLEAN NOT NULL DEFAULT true, created_at TIMESTAMPTZ NOT NULL DEFAULT now());
CREATE TABLE IF NOT EXISTS promo_uses(code TEXT NOT NULL REFERENCES promos(code) ON DELETE CASCADE, user_id BIGINT NOT NULL,
  used_at TIMESTAMPTZ NOT NULL DEFAULT now(), PRIMARY KEY(code,user_id));
"""
P: asyncpg.Pool
BOT: Bot


class ApiErr(Exception):
    def __init__(self, status, code, msg, **extra):
        self.status, self.code, self.msg, self.extra = status, code, msg, extra


def usd(c): return f"${c / 100:.2f}"
def som(n): return f"{n:,}".replace(",", " ") + " so'm"


_SC = None


def drop_cache():
    global _SC
    _SC = None


async def get_settings():
    global _SC
    if _SC and time.time() - _SC[0] < 3: return dict(_SC[1])
    s = await _load_settings(); _SC = (time.time(), s)
    return dict(s)


async def _load_settings():
    s = dict(DEFAULTS)
    s.update({r["key"]: json.loads(r["value"]) for r in await P.fetch("SELECT key,value FROM settings")})
    s["receipt_check"] = {**DEFAULTS["receipt_check"], **s["receipt_check"]}
    s["help"] = {**DEFAULTS["help"], **s["help"]}
    return s


def stars_price(qty, s):  # sent, yuqoriga yaxlitlanadi
    return math.ceil(Decimal(qty) * Decimal(str(s["star_usd"])) * (1 + Decimal(str(s["margin_pct"])) / 100) * 100)


def check_init(raw):
    d = dict(parse_qsl(raw, keep_blank_values=True))
    h = d.pop("hash", None)
    if not h: return None
    msg = "\n".join(f"{k}={v}" for k, v in sorted(d.items()))
    key = hmac.new(b"WebAppData", BOT_TOKEN.encode(), hashlib.sha256).digest()
    if not hmac.compare_digest(hmac.new(key, msg.encode(), hashlib.sha256).hexdigest(), h): return None
    if time.time() - int(d.get("auth_date", 0)) > 86400 or "user" not in d: return None
    return json.loads(d["user"])


# ---------- atomik qarorlar (API ham, bot tugmalari ham shu funksiyalarni ishlatadi) ----------
async def decide_order(oid, act, note=""):
    async with P.acquire() as c, c.transaction():
        if act == "done":
            o = await c.fetchrow("UPDATE orders SET status='done',done_at=now() WHERE id=$1 AND status='pending' RETURNING *", oid)
        else:
            o = await c.fetchrow("UPDATE orders SET status='cancelled',note=$2,done_at=now() WHERE id=$1 AND status='pending' RETURNING *", oid, note)
            if o:
                b = await c.fetchval("UPDATE users SET balance=balance+$1 WHERE id=$2 RETURNING balance", o["price"], o["user_id"])
                await c.execute("INSERT INTO balance_log(user_id,delta,balance_after,reason) VALUES($1,$2,$3,'order_refund')", o["user_id"], o["price"], b)
    return o


async def _decide_topup(tid, ok, amount=None, note="", by=0):
    async with P.acquire() as c, c.transaction():
        t = await c.fetchrow("SELECT id,user_id,amount,rate,credit FROM topups WHERE id=$1 AND status='pending' FOR UPDATE", tid)
        if not t: return None
        if not ok:
            await c.execute("UPDATE topups SET status='rejected',decided_at=now(),note=$2,decided_by=$3 WHERE id=$1", tid, note, by)
            return {**dict(t), "ok": False, "note": note}
        credit = (amount * 100 // t["rate"]) if amount else t["credit"]  # topup kursi bilan
        await c.execute("UPDATE topups SET status='approved',decided_at=now(),credit=$2,decided_by=$3 WHERE id=$1", tid, credit, by)
        b = await c.fetchval("UPDATE users SET balance=balance+$1 WHERE id=$2 RETURNING balance", credit, t["user_id"])
        await c.execute("INSERT INTO balance_log(user_id,delta,balance_after,reason) VALUES($1,$2,$3,'topup')", t["user_id"], credit, b)
        ref, inv = None, await c.fetchval("SELECT invited_by FROM users WHERE id=$1", t["user_id"])
        pct = json.loads(await c.fetchval("SELECT value FROM settings WHERE key='ref_percent'") or str(DEFAULTS["ref_percent"]))
        bonus = int(credit * pct // 100)  # tasdiq tranzaksiyasi ichida, har topupga bir marta
        if inv and bonus > 0:
            ib = await c.fetchval("UPDATE users SET balance=balance+$1,ref_earned=ref_earned+$1 WHERE id=$2 RETURNING balance", bonus, inv)
            if ib is not None:
                await c.execute("INSERT INTO balance_log(user_id,delta,balance_after,reason) VALUES($1,$2,$3,'referral')", inv, bonus, ib)
                ref = (inv, bonus)
        return {**dict(t), "ok": True, "credit": credit, "balance": b, "ref": ref}


async def decide_topup(*a, **k):
    r = await _decide_topup(*a, **k)
    if r and r.get("ref"): spawn(tell_u(r["ref"][0], "ref_bonus", bonus=usd(r["ref"][1])))
    return r


async def tell(uid, text, **kw):
    try: await BOT.send_message(uid, text, **kw)
    except Exception as e: log.info("send to %s failed: %s", uid, e)  # bloklagan bo'lsa davom etamiz


LANGI = {"uz": 0, "ru": 1, "en": 2}
RT = {"duplicate_receipt": ("takroriy chek", "повторный чек", "duplicate receipt"), "amount_mismatch": ("summa mos emas", "сумма не совпадает", "amount mismatch"),
      "amount_not_found": ("summa topilmadi", "сумма не найдена", "amount not found"), "card_mismatch": ("karta mos emas", "карта не совпадает", "card mismatch"),
      "card_not_found": ("karta topilmadi", "карта не найдена", "card not found"), "too_old": ("chek eskirgan", "чек устарел", "receipt is too old"),
      "future_date": ("sana kelajakda", "дата в будущем", "date in the future"), "date_not_found": ("sana topilmadi", "дата не найдена", "date not found"),
      "ocr_failed": ("OCR xatosi", "ошибка OCR", "OCR error"), "low_quality": ("rasm sifati past", "низкое качество фото", "low image quality"),
      "manual_reject": ("chek tasdiqlanmadi", "чек не подтверждён", "receipt not approved")}


def reason_txt(note, lang):
    return ", ".join(RT[c][LANGI[lang]] if c in RT else c for c in (x.strip() for x in str(note or "").split(",")) if c)


BT = {
    "uz": {"welcome": "✦ Velqo Shop'ga xush kelibsiz!\nTelegram Stars va Premium — tez va qulay.", "open_btn": "Do'konni ochish", "sub_need": "Davom etish uchun kanallarga obuna bo'ling:",
           "sub_btn": "✅ Tekshirish", "sub_ok": "✅ Rahmat! Do'konni ochishingiz mumkin.", "sub_no": "Hali barcha kanallarga obuna bo'lmagansiz",
           "ord_done": "✅ Buyurtma #{id} bajarildi. Rahmat!", "ord_cancel": "✖ Buyurtma #{id} bekor qilindi. {price} balansingizga qaytarildi.",
           "top_ok": "✅ Balans to'ldirildi: +{credit}\nYangi balans: {balance}", "top_bad": "✖ Chek #{id} rad etildi. Sabab: {reason}", "ref_bonus": "🎁 Referal bonus: +{bonus}",
           "ref_info": "🎁 Referal dasturi: taklif qilingan do'stingiz har safar balansini to'ldirganda {pct}% bonus olasiz.\n\nHavolangiz:\n{link}\n\nTaklif qilinganlar: {n}\nIshlangan: {earned}",
           "bal_adj": "Balansingiz o'zgartirildi: {delta}. Yangi balans: {balance}"},
    "ru": {"welcome": "✦ Добро пожаловать в Velqo Shop!\nTelegram Stars и Premium — быстро и удобно.", "open_btn": "Открыть магазин", "sub_need": "Чтобы продолжить, подпишитесь на каналы:",
           "sub_btn": "✅ Проверить", "sub_ok": "✅ Спасибо! Можно открыть магазин.", "sub_no": "Вы подписались не на все каналы",
           "ord_done": "✅ Заказ #{id} выполнен. Спасибо!", "ord_cancel": "✖ Заказ #{id} отменён. {price} возвращено на ваш баланс.",
           "top_ok": "✅ Баланс пополнен: +{credit}\nНовый баланс: {balance}", "top_bad": "✖ Чек #{id} отклонён. Причина: {reason}", "ref_bonus": "🎁 Реферальный бонус: +{bonus}",
           "ref_info": "🎁 Реферальная программа: вы получаете {pct}% бонуса каждый раз, когда приглашённый друг пополняет баланс.\n\nВаша ссылка:\n{link}\n\nПриглашено: {n}\nЗаработано: {earned}",
           "bal_adj": "Ваш баланс изменён: {delta}. Новый баланс: {balance}"},
    "en": {"welcome": "✦ Welcome to Velqo Shop!\nTelegram Stars and Premium — fast and simple.", "open_btn": "Open shop", "sub_need": "To continue, subscribe to these channels:",
           "sub_btn": "✅ Check", "sub_ok": "✅ Thanks! You can open the shop now.", "sub_no": "You have not subscribed to all channels yet",
           "ord_done": "✅ Order #{id} is complete. Thank you!", "ord_cancel": "✖ Order #{id} was cancelled. {price} was returned to your balance.",
           "top_ok": "✅ Balance topped up: +{credit}\nNew balance: {balance}", "top_bad": "✖ Receipt #{id} was rejected. Reason: {reason}", "ref_bonus": "🎁 Referral bonus: +{bonus}",
           "ref_info": "🎁 Referral program: you earn {pct}% every time a friend you invited tops up.\n\nYour link:\n{link}\n\nInvited: {n}\nEarned: {earned}",
           "bal_adj": "Your balance was adjusted: {delta}. New balance: {balance}"}}


async def lang_of(u):
    row = await P.fetchval("SELECT lang FROM users WHERE id=$1", u.id)
    return row or (u.language_code if getattr(u, "language_code", None) in ("ru", "en") else "uz")


async def tell_u(uid, key, **kw):
    lang = await P.fetchval("SELECT lang FROM users WHERE id=$1", uid) or "uz"
    if "reason" in kw: kw["reason"] = reason_txt(kw["reason"], lang)
    await tell(uid, BT[lang][key].format(**kw))


async def notify_order(o):
    await tell_u(o["user_id"], "ord_done" if o["status"] == "done" else "ord_cancel", id=o["id"], price=usd(o["price"]))


async def notify_topup(r, tid):
    if r["ok"]: await tell_u(r["user_id"], "top_ok", credit=usd(r["credit"]), balance=usd(r["balance"]))
    else: await tell_u(r["user_id"], "top_bad", id=tid, reason=r["note"])


def admin_kb(kind, i, a, b):
    return InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text=a, callback_data=f"{kind}:ok:{i}"),
                                                  InlineKeyboardButton(text=b, callback_data=f"{kind}:no:{i}")]])


# ---------- API ----------
@web.middleware
async def mw(req, handler):
    try:
        if req.path.startswith("/api/"):
            raw, u = req.headers.get("X-Init-Data") or req.query.get("init", ""), None
            if raw: u = check_init(raw)
            elif DEV and (req.headers.get("X-Dev-User") or req.query.get("dev")): u = {"id": int(req.headers.get("X-Dev-User") or req.query["dev"]), "first_name": "Dev", "username": "dev_user"}
            if not u: raise ApiErr(401, "unauthorized", "Kirish rad etildi. Mini App'ni Telegram orqali oching.")
            lang = u.get("language_code", "uz")
            row = await P.fetchrow(
                "INSERT INTO users(id,first_name,username,photo_url,lang) VALUES($1,$2,$3,$4,$5) ON CONFLICT(id) DO UPDATE "
                "SET first_name=$2,username=$3,photo_url=$4,last_seen=now() RETURNING *",
                u["id"], u.get("first_name"), u.get("username"), u.get("photo_url"), lang if lang in ("ru", "en") else "uz")
            if row["is_blocked"]: raise ApiErr(403, "blocked", "Hisobingiz bloklangan.")
            req["u"] = row
            if row["id"] not in admin_ids():
                s = await get_settings()
                if s["maintenance"]: raise ApiErr(503, "maintenance", "Texnik ishlar olib borilmoqda. Birozdan so'ng qaytib keling.")
                now, q = time.time(), HITS.setdefault(row["id"], deque())
                while q and now - q[0] > 10: q.popleft()
                if len(q) >= 30: raise ApiErr(429, "rate_limited", "Juda ko'p so'rov. Biroz kuting.", retry_after=10)
                q.append(now)
                if req.path not in ("/api/bootstrap", "/api/check-sub") and req.method != "PATCH" and await missing_subs(row["id"], s):
                    raise ApiErr(403, "sub_required", "Avval kanallarga obuna bo'ling.")
        return await handler(req)
    except ApiErr as e:
        return web.json_response({"error": e.code, "message": e.msg, **e.extra}, status=e.status)
    except web.HTTPException: raise
    except Exception:
        log.exception("unhandled")
        return web.json_response({"error": "server_error", "message": "Server xatosi. Birozdan so'ng urinib ko'ring."}, status=500)


async def bootstrap(req):
    u, s = req["u"], await get_settings()
    miss = [] if u["id"] in admin_ids() else await missing_subs(u["id"], s)
    return web.json_response({"user": {"id": u["id"], "first_name": u["first_name"], "username": u["username"], "lang": u["lang"],
                                       "balance": u["balance"], "is_admin": u["id"] in admin_ids(), "is_super": u["id"] in ADMINS},
        "settings": {"min_stars": s["min_stars"], "max_stars": s["max_stars"], "usd_rate": s["usd_rate"],
                     "star_packs": [{"qty": q, "price": stars_price(q, s)} for q in s["star_packs"]],
                     "premium_prices": s["premium_prices"], "card_number": s["card_number"], "card_holder": s["card_holder"],
                     "topup_min": s["topup_min"], "topup_max": s["topup_max"], "help": s["help"]},
        "sub": sub_out(miss)})


async def price_of(kind, qty, s):
    if kind == "stars":
        if not isinstance(qty, int) or not s["min_stars"] <= qty <= s["max_stars"]:
            raise ApiErr(400, "invalid_input", f"Stars soni {s['min_stars']} dan {s['max_stars']} gacha bo'lsin.")
        return stars_price(qty, s)
    if kind == "premium" and str(qty) in s["premium_prices"]: return int(s["premium_prices"][str(qty)])
    raise ApiErr(400, "invalid_input", "Noto'g'ri buyurtma turi yoki miqdori.")


async def quote(req):
    b = await req.json()
    return web.json_response({"price": await price_of(b.get("kind"), b.get("qty"), await get_settings())})


async def create_order(req):
    u, b, s = req["u"], await req.json(), await get_settings()
    price = await price_of(b.get("kind"), b.get("qty"), s)
    rc = b.get("recipient") or {}
    if rc.get("type") == "other":
        name = str(rc.get("username", "")).strip()
        if not re.fullmatch(r"@?[A-Za-z0-9_]{5,32}", name): raise ApiErr(400, "invalid_input", "Username 5–32 ta harf, raqam yoki _ bo'lsin.")
        name = name.lstrip("@")
    else:
        name = u["username"]
        if not name: raise ApiErr(400, "username_required", "Avval Telegram sozlamalarida username o'rnating.")
    async with P.acquire() as c, c.transaction():
        bal = await c.fetchval("UPDATE users SET balance=balance-$1 WHERE id=$2 AND balance>=$1 RETURNING balance", price, u["id"])
        if bal is None:
            have = await c.fetchval("SELECT balance FROM users WHERE id=$1", u["id"])
            raise ApiErr(402, "insufficient_balance", f"Yetishmayotgan summa: {usd(price - have)}", need=price - have, balance=have)
        o = await c.fetchrow("INSERT INTO orders(user_id,kind,qty,price,recipient) VALUES($1,$2,$3,$4,$5) RETURNING *", u["id"], b["kind"], b["qty"], price, name)
        await c.execute("INSERT INTO balance_log(user_id,delta,balance_after,reason) VALUES($1,$2,$3,'order')", u["id"], -price, bal)
    what = f"{o['qty']} Stars" if o["kind"] == "stars" else f"Premium {o['qty']} oy"
    for a in admin_ids():
        await tell(a, f"🆕 Buyurtma #{o['id']}\n{what} → @{name}\nNarx: {usd(price)}\nMijoz: {u['first_name']} (id {u['id']})",
                   reply_markup=admin_kb("ord", o["id"], "✅ Bajarildi", "✖ Bekor"))
    return web.json_response({"order": {"id": o["id"], "status": "pending"}, "balance": bal}, status=201)


def rows(rs): return web.json_response({"items": [{k: (v.isoformat() if hasattr(v, "isoformat") else v) for k, v in dict(r).items()} for r in rs], "total": len(rs)})


async def list_orders(req):
    return rows(await P.fetch("SELECT id,kind,qty,price,recipient,status,created_at,done_at,note FROM orders WHERE user_id=$1 ORDER BY id DESC LIMIT 50", req["u"]["id"]))


async def list_topups(req):
    return rows(await P.fetch("SELECT id,amount,credit,rate,status,created_at,decided_at,note FROM topups WHERE user_id=$1 ORDER BY id DESC LIMIT 50", req["u"]["id"]))


def is_image(d):
    return d[:2] == b"\xff\xd8" or d[:8] == b"\x89PNG\r\n\x1a\n" or (d[:4] == b"RIFF" and d[8:12] == b"WEBP")


async def create_topup(req):
    u, s = req["u"], await get_settings()
    amount, data, mime = 0, b"", ""
    async for part in await req.multipart():
        if part.name == "amount":
            try: amount = int((await part.text()).strip())
            except ValueError: pass
        elif part.name == "receipt":
            mime, data = part.headers.get("Content-Type", ""), bytes(await part.read(decode=False))
    if not s["topup_min"] <= amount <= s["topup_max"]:
        raise ApiErr(400, "invalid_input", f"Summa {som(s['topup_min'])} dan {som(s['topup_max'])} gacha bo'lsin.")
    if mime not in ("image/jpeg", "image/png", "image/webp") or not data or len(data) > 5 * 1024 * 1024 or not is_image(data):
        raise ApiErr(400, "invalid_input", "Chek rasmi JPG, PNG yoki WEBP bo'lsin va 5 MB dan oshmasin.")
    credit = amount * 100 // s["usd_rate"]
    if credit < 1: raise ApiErr(400, "invalid_input", "Kurs bo'yicha bu summa 1 sentdan kam.")
    if await P.fetchval("SELECT count(*) FROM topups WHERE user_id=$1 AND status='pending'", u["id"]) >= 3:
        raise ApiErr(429, "limit_reached", "Kutilayotgan 3 ta so'rovingiz bor. Avval ular ko'rib chiqilishini kuting.")
    sha = hashlib.sha256(data).hexdigest()
    try: dh = await asyncio.get_running_loop().run_in_executor(None, rc.dhash, data)
    except Exception: dh = None
    t = await P.fetchrow("INSERT INTO topups(user_id,amount,rate,credit,receipt,mime,receipt_sha256,receipt_dhash) VALUES($1,$2,$3,$4,$5,$6,$7,$8) RETURNING id",
                         u["id"], amount, s["usd_rate"], credit, data, mime, sha, dh)
    spawn(run_check(t["id"]))  # javob darrov; tekshiruv va adminga xabar fonda
    return web.json_response({"topup": {"id": t["id"], "status": "pending"}}, status=201)


async def patch_me(req):
    lang = (await req.json()).get("lang")
    if lang not in ("uz", "ru", "en"): raise ApiErr(400, "invalid_input", "Til noto'g'ri.")
    await P.execute("UPDATE users SET lang=$1 WHERE id=$2", lang, req["u"]["id"])
    return web.json_response({"lang": lang})


# ---------- Chek tekshiruvi (fon vazifasi) ----------
TASKS = set()
RC_UZ = {"duplicate_receipt": "takroriy chek", "amount_mismatch": "summa mos emas", "amount_not_found": "summa topilmadi", "card_mismatch": "karta mos emas",
         "card_not_found": "karta topilmadi", "too_old": "chek eskirgan", "future_date": "sana kelajakda", "date_not_found": "sana topilmadi",
         "ocr_failed": "OCR xatosi", "low_quality": "rasm sifati past"}


def spawn(coro):
    t = asyncio.create_task(coro); TASKS.add(t); t.add_done_callback(TASKS.discard)


async def is_dup(tid, sha, dh):
    if await P.fetchval("SELECT 1 FROM topups WHERE id<$1 AND status<>'rejected' AND receipt_sha256=$2 LIMIT 1", tid, sha): return True
    if dh:
        for r in await P.fetch("SELECT receipt_dhash FROM topups WHERE id<$1 AND status<>'rejected' AND receipt_dhash IS NOT NULL ORDER BY id DESC LIMIT 3000", tid):
            if rc.hamming(r[0], dh) <= 5: return True
    return False


async def analyze_topup(t, s):
    cfg = s["receipt_check"]
    if cfg["mode"] == "off": return {"verdict": "off", "reasons": [], "found": {}}
    dup = await is_dup(t["id"], t["receipt_sha256"], t["receipt_dhash"])
    res = await asyncio.get_running_loop().run_in_executor(None, rc.analyze, bytes(t["receipt"]), t["amount"], s["card_number"], cfg, dup)
    await P.execute("UPDATE topups SET check_json=$2::jsonb WHERE id=$1", t["id"], json.dumps(res))
    return res


def chk_line(res):
    v = {"pass": "🟢 Mos", "review": "🟡 Shubhali", "fail": "🔴 Mos emas"}.get(res["verdict"])
    if not v: return ""
    f = res.get("found", {})
    return "\n" + v + (": " + ", ".join(RC_UZ.get(x, x) for x in res["reasons"]) if res["reasons"] else "") + (f" (chekda: {som(f['amount'])})" if f.get("amount") else "")


async def run_check(tid):
    try:
        t = await P.fetchrow("SELECT t.*,u.first_name FROM topups t JOIN users u ON u.id=t.user_id WHERE t.id=$1", tid)
        if not t or t["status"] != "pending": return
        s = await get_settings()
        try: res = await analyze_topup(t, s)
        except Exception:
            log.exception("check failed"); res = {"verdict": "review", "reasons": ["ocr_failed"], "found": {}}
        cfg, r, auto = s["receipt_check"], None, None
        if cfg["mode"] == "auto":
            if res["verdict"] == "fail" and cfg["auto_reject"]:
                auto = False; r = await decide_topup(tid, False, note=",".join(res["reasons"]))
            elif res["verdict"] == "pass" and t["amount"] <= cfg["auto_approve_max"]:
                auto = True; r = await decide_topup(tid, True)
        if r:
            await P.execute("UPDATE topups SET decided_auto=true WHERE id=$1", tid)
            await audit(0, "topup.auto_approve" if auto else "topup.auto_reject", {"id": tid})
            await notify_topup(r, tid)
        cap = f"🧾 Chek #{tid}\n{t['first_name']} (id {t['user_id']})\n{som(t['amount'])} → {usd(t['credit'])}" + chk_line(res)
        if r: cap += "\n🤖 Avtomatik " + ("tasdiqlandi" if auto else "rad etildi")
        for a in admin_ids():
            try:
                await BOT.send_photo(a, BufferedInputFile(bytes(t["receipt"]), "receipt.jpg"), caption=cap,
                                     reply_markup=None if r else admin_kb("top", tid, "✅ Tasdiqlash", "✖ Rad etish"))
            except Exception as e: log.info("admin notify failed: %s", e)
    except Exception:
        log.exception("run_check %s", tid)


async def a_recheck(req):
    tid = int(req.match_info["id"])
    t = await P.fetchrow("SELECT * FROM topups WHERE id=$1", tid)
    if not t: raise ApiErr(404, "not_found", "Chek topilmadi")
    s = await get_settings()
    if s["receipt_check"]["mode"] == "off": s["receipt_check"] = {**s["receipt_check"], "mode": "assist"}
    return web.json_response(await analyze_topup(t, s))


# ---------- Referal va promo ----------
BOT_USER = ""
CODE_RX = re.compile(r"[A-Z0-9_]{3,32}")


async def referral(req):
    u, s = req["u"], await get_settings()
    inv = await P.fetchval("SELECT count(*) FROM users WHERE invited_by=$1", u["id"])
    lb = await P.fetch("SELECT u.first_name AS name, count(i.id) AS invited, u.ref_earned AS earned FROM users u JOIN users i ON i.invited_by=u.id "
                       "GROUP BY u.id ORDER BY u.ref_earned DESC, count(i.id) DESC LIMIT 10")
    return web.json_response({"percent": s["ref_percent"], "invited": inv, "earned": u["ref_earned"],
                              "link": f"https://t.me/{BOT_USER}?start=ref{u['id']}", "leaderboard": [dict(r) for r in lb]})


async def redeem(req):
    u, code = req["u"], str((await req.json()).get("code", "")).strip().upper()
    if not CODE_RX.fullmatch(code): raise ApiErr(404, "promo_not_found", "Promo kod topilmadi")
    async with P.acquire() as c, c.transaction():
        p = await c.fetchrow("SELECT * FROM promos WHERE code=$1 FOR UPDATE", code)
        if not p or not p["active"]: raise ApiErr(404, "promo_not_found", "Promo kod topilmadi")
        if p["expires_at"] and p["expires_at"] < datetime.now(timezone.utc): raise ApiErr(410, "promo_expired", "Promo kod muddati tugagan")
        if p["max_uses"] and await c.fetchval("SELECT count(*) FROM promo_uses WHERE code=$1", code) >= p["max_uses"]:
            raise ApiErr(409, "promo_exhausted", "Promo kod limiti tugagan")
        if not await c.fetchval("INSERT INTO promo_uses(code,user_id) VALUES($1,$2) ON CONFLICT DO NOTHING RETURNING 1", code, u["id"]):
            raise ApiErr(409, "promo_used", "Siz bu kodni allaqachon ishlatgansiz")
        b = await c.fetchval("UPDATE users SET balance=balance+$1 WHERE id=$2 RETURNING balance", p["amount"], u["id"])
        await c.execute("INSERT INTO balance_log(user_id,delta,balance_after,reason) VALUES($1,$2,$3,$4)", u["id"], p["amount"], b, "promo:" + code)
    return web.json_response({"amount": p["amount"], "balance": b})


async def a_promos(req):
    return rows(await P.fetch("SELECT p.code,p.amount,p.max_uses,p.expires_at,p.active,(SELECT count(*) FROM promo_uses WHERE code=p.code) AS used FROM promos p ORDER BY p.created_at DESC LIMIT 100"))


async def a_promo_new(req):
    b = await req.json(); code = str(b.get("code", "")).strip().upper()
    amt, mx, days = b.get("amount"), b.get("max_uses", 0), b.get("days")
    if not CODE_RX.fullmatch(code) or not isinstance(amt, int) or amt <= 0 or not isinstance(mx, int) or mx < 0 or (days is not None and (not isinstance(days, int) or days <= 0)):
        raise ApiErr(400, "invalid_input", "Promo ma'lumotlari noto'g'ri")
    if not await P.fetchval("INSERT INTO promos(code,amount,max_uses,expires_at) VALUES($1,$2,$3,CASE WHEN $4::int IS NULL THEN NULL ELSE now()+make_interval(days=>$4::int) END) "
                            "ON CONFLICT DO NOTHING RETURNING 1", code, amt, mx, days):
        raise ApiErr(409, "conflict", "Bu kod allaqachon mavjud")
    return web.json_response({"code": code}, status=201)


async def a_promo_patch(req):
    if not await P.fetchval("UPDATE promos SET active=$2 WHERE code=$1 RETURNING 1", req.match_info["code"].upper(), bool((await req.json()).get("active"))):
        raise ApiErr(404, "not_found", "Promo topilmadi")
    return web.json_response({"ok": True})


async def a_promo_del(req):
    if not await P.fetchval("DELETE FROM promos WHERE code=$1 RETURNING 1", req.match_info["code"].upper()): raise ApiErr(404, "not_found", "Promo topilmadi")
    return web.json_response({"ok": True})


async def a_promo_uses(req):
    return rows(await P.fetch("SELECT pu.user_id,u.first_name,u.username,pu.used_at FROM promo_uses pu JOIN users u ON u.id=pu.user_id WHERE pu.code=$1 ORDER BY pu.used_at DESC LIMIT 200", req.match_info["code"].upper()))


async def ref_link(u, arg):
    """/start ref<id>: faqat yangi (<1 soat, buyurtma va topupsiz) foydalanuvchi; o'zini taklif qila olmaydi."""
    if not arg.startswith("ref") or not arg[3:].isdigit() or int(arg[3:]) == u.id: return
    await P.execute("INSERT INTO users(id,first_name,username) VALUES($1,$2,$3) ON CONFLICT DO NOTHING", u.id, u.first_name, u.username)
    await P.fetchval("UPDATE users SET invited_by=$2 WHERE id=$1 AND invited_by IS NULL AND created_at>now()-interval '1 hour' "
                     "AND NOT EXISTS(SELECT 1 FROM orders WHERE user_id=$1) AND NOT EXISTS(SELECT 1 FROM topups WHERE user_id=$1) "
                     "AND EXISTS(SELECT 1 FROM users WHERE id=$2) RETURNING 1", u.id, int(arg[3:]))


# ---------- Obuna, xodimlar, audit, broadcast ----------
HITS, SUB_CACHE = {}, {}


async def missing_subs(uid, s, fresh=False):
    chans = s["force_sub"]
    if not chans: return []
    hit = SUB_CACHE.get(uid)
    if hit and not fresh and time.time() - hit[0] < 60: return hit[1]
    miss = []
    for c in chans:
        try:
            if (await BOT.get_chat_member(c["chat"], uid)).status in ("left", "kicked"): miss.append(c)
        except Exception as e: log.warning("obuna tekshiruvi %s: %s", c["chat"], e)  # bot kanalda admin emas -> bloklamaymiz
    SUB_CACHE[uid] = (time.time(), miss)
    return miss


def sub_out(miss): return {"required": bool(miss), "channels": [{"title": c["title"], "url": c["url"]} for c in miss]}


async def check_sub(req):
    return web.json_response(sub_out(await missing_subs(req["u"]["id"], await get_settings(), True)))


async def audit(aid, action, detail=None):
    try: await P.execute("INSERT INTO audit(admin_id,action,detail) VALUES($1,$2,$3::jsonb)", aid, action, json.dumps(detail, default=str))
    except Exception: log.exception("audit")


def sup(h):
    async def w(req):
        if req["u"]["id"] not in ADMINS: raise ApiErr(403, "forbidden", "Faqat super admin")
        return await h(req)
    return adm(w)


async def a_audit(req):
    rs = await P.fetch("SELECT id,admin_id,action,detail,created_at FROM audit ORDER BY id DESC LIMIT 100")
    return rows([{**dict(r), "detail": json.loads(r["detail"]) if r["detail"] else None} for r in rs])


async def a_staff(req):
    ids = sorted(ADMINS) + sorted(STAFF - ADMINS)
    names = {r["id"]: r for r in await P.fetch("SELECT id,first_name,username FROM users WHERE id=ANY($1)", ids)}
    return web.json_response({"items": [{"id": i, "role": "super" if i in ADMINS else "staff", "env": i in ADMINS,
                                         "name": (names.get(i) or {}).get("first_name"), "username": (names.get(i) or {}).get("username")} for i in ids], "total": len(ids)})


async def a_staff_add(req):
    i = (await req.json()).get("id")
    if not isinstance(i, int) or i <= 0: raise ApiErr(400, "invalid_input", "Telegram ID noto'g'ri")
    await P.execute("INSERT INTO admins(id,added_by) VALUES($1,$2) ON CONFLICT DO NOTHING", i, req["u"]["id"]); STAFF.add(i)
    return web.json_response({"ok": True}, status=201)


async def a_staff_del(req):
    i = int(req.match_info["id"])
    if i in ADMINS: raise ApiErr(400, "invalid_input", "Env adminni o'chirib bo'lmaydi")
    await P.execute("DELETE FROM admins WHERE id=$1", i); STAFF.discard(i)
    return web.json_response({"ok": True})


def clean_html(t):
    t = html.escape(t, quote=True)
    t = re.sub(r"&lt;(/?)(b|i|code)&gt;", r"<\1\2>", t)
    t = re.sub(r"&lt;a href=&quot;(https?://(?:[^\s&<>\"]|&amp;)+)&quot;&gt;", r'<a href="\1">', t)
    return t.replace("&lt;/a&gt;", "</a>")


async def run_broadcast(bid, ids, text, btn):
    kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text=btn[0], url=btn[1])]]) if btn else None
    sent = failed = 0
    try:
        for n, uid in enumerate(ids, 1):
            try:
                try: await BOT.send_message(uid, text, parse_mode="HTML", reply_markup=kb)
                except TelegramRetryAfter as e:
                    await asyncio.sleep(e.retry_after); await BOT.send_message(uid, text, parse_mode="HTML", reply_markup=kb)
                sent += 1
            except Exception: failed += 1  # botni bloklaganlar — xato yutiladi
            if n % 20 == 0: await P.execute("UPDATE broadcasts SET sent=$2,failed=$3 WHERE id=$1", bid, sent, failed)
            await asyncio.sleep(0.05)  # <= 20 xabar/s
    finally:
        await P.execute("UPDATE broadcasts SET sent=$2,failed=$3,done=true WHERE id=$1", bid, sent, failed)


async def a_bc_new(req):
    b = await req.json(); text = clean_html(str(b.get("text", "")).strip())
    if not text or len(text) > 3500: raise ApiErr(400, "invalid_input", "Xabar matni bo'sh yoki juda uzun")
    bt, bu, btn = str(b.get("button_text", "")).strip(), str(b.get("button_url", "")).strip(), None
    if bt or bu:
        if not bt or not re.fullmatch(r"https?://\S+", bu): raise ApiErr(400, "invalid_input", "Tugma matni va to'g'ri URL kiriting")
        btn = (bt[:60], bu)
    ids = [r["id"] for r in await P.fetch("SELECT id FROM users WHERE NOT is_blocked")]
    bid = await P.fetchval("INSERT INTO broadcasts(admin_id,text,total) VALUES($1,$2,$3) RETURNING id", req["u"]["id"], text, len(ids))
    spawn(run_broadcast(bid, ids, text, btn))
    return web.json_response({"id": bid, "total": len(ids)}, status=201)


async def a_bc_get(req):
    r = await P.fetchrow("SELECT id,total,sent,failed,done FROM broadcasts WHERE id=$1", int(req.match_info["id"]))
    if not r: raise ApiErr(404, "not_found", "Topilmadi")
    return web.json_response(dict(r))


# ---------- Admin API ----------
def adm(h):
    async def w(req):
        if req["u"]["id"] not in admin_ids(): raise ApiErr(403, "forbidden", "Ruxsat yo'q")
        r = await h(req)
        if req.method != "GET":
            try: body = await req.json()
            except Exception: body = None
            await audit(req["u"]["id"], f"{req.method} {req.path}", body)
        return r
    return w


def valid_setting(k, v):
    d = DEFAULTS.get(k)
    if d is None: raise ApiErr(400, "invalid_input", f"Noma'lum kalit: {k}")
    ok = isinstance(v, bool) if isinstance(d, bool) else ((isinstance(v, (int, float)) and not isinstance(v, bool)) if isinstance(d, (int, float)) else isinstance(v, type(d)))
    if ok and k == "help": ok = all(isinstance(x, list) and all(isinstance(i, str) for i in x) for x in v.values())
    if ok and k in ("usd_rate", "star_usd", "topup_min", "min_stars") and v <= 0: ok = False
    if ok and k == "margin_pct" and not 0 <= v <= 500: ok = False
    if ok and k == "ref_percent" and not 0 <= v <= 50: ok = False
    if ok and k == "premium_prices": ok = all(str(m).isdigit() and isinstance(x, int) and x > 0 for m, x in v.items())
    if ok and k == "receipt_check":
        ok = v.get("mode") in ("off", "assist", "auto") and all(isinstance(v.get(x), int) and v[x] > 0 for x in ("auto_approve_max", "max_age_minutes"))
    if ok and k == "star_packs": ok = all(isinstance(x, int) and x > 0 for x in v)
    if not ok: raise ApiErr(400, "invalid_input", f"{k} qiymati noto'g'ri")


async def a_stats(req):
    f = P.fetchval
    chart = await P.fetch("SELECT d::date AS day, COALESCE((SELECT sum(price) FROM orders WHERE status='done' AND done_at::date=d::date),0)::bigint AS sum "
                          "FROM generate_series(now()-interval '6 days', now(), interval '1 day') d ORDER BY d")
    return web.json_response({
        "revenue_today": int(await f("SELECT COALESCE(sum(price),0) FROM orders WHERE status='done' AND done_at::date=now()::date")),
        "revenue_total": int(await f("SELECT COALESCE(sum(price),0) FROM orders WHERE status='done'")),
        "orders_total": await f("SELECT count(*) FROM orders"), "orders_pending": await f("SELECT count(*) FROM orders WHERE status='pending'"),
        "topups_pending": await f("SELECT count(*) FROM topups WHERE status='pending'"),
        "users_total": await f("SELECT count(*) FROM users"), "users_today": await f("SELECT count(*) FROM users WHERE created_at::date=now()::date"),
        "chart": [{"day": r["day"].isoformat(), "sum": int(r["sum"])} for r in chart]})


async def a_orders(req):
    st = req.query.get("status", "pending")
    return rows(await P.fetch("SELECT o.id,o.kind,o.qty,o.price,o.recipient,o.status,o.created_at,u.first_name,u.username FROM orders o JOIN users u ON u.id=o.user_id "
                              "WHERE ($1='all' OR o.status=$1) ORDER BY o.id DESC LIMIT 100", st))


async def a_order_act(req):
    i, act = int(req.match_info["id"]), req.match_info["act"]
    if act not in ("complete", "cancel"): raise ApiErr(404, "not_found", "Topilmadi")
    note = str((await req.json()).get("note", ""))[:200] if act == "cancel" else ""
    o = await decide_order(i, "done" if act == "complete" else "cancel", note or "Admin tomonidan bekor qilindi")
    if not o: raise ApiErr(409, "conflict", "Buyurtma allaqachon hal qilingan")
    await notify_order(o)
    return web.json_response({"ok": True})


async def a_topups(req):
    st = req.query.get("status", "pending")
    rs = await P.fetch("SELECT t.id,t.amount,t.credit,t.rate,t.status,t.created_at,t.note,t.decided_auto,t.check_json AS chk,u.first_name,u.username FROM topups t JOIN users u ON u.id=t.user_id "
                       "WHERE ($1='all' OR t.status=$1) ORDER BY t.id DESC LIMIT 100", st)
    return rows([{**dict(r), "chk": json.loads(r["chk"]) if r["chk"] else None} for r in rs])


async def a_receipt(req):
    r = await P.fetchrow("SELECT receipt,mime FROM topups WHERE id=$1", int(req.match_info["id"]))
    if not r: raise ApiErr(404, "not_found", "Chek topilmadi")
    return web.Response(body=bytes(r["receipt"]), content_type=r["mime"], headers={"Cache-Control": "private, max-age=300"})


async def a_topup_act(req):
    i, act, b = int(req.match_info["id"]), req.match_info["act"], await req.json()
    if act not in ("approve", "reject"): raise ApiErr(404, "not_found", "Topilmadi")
    amount = b.get("amount")
    if amount is not None and (not isinstance(amount, int) or amount <= 0): raise ApiErr(400, "invalid_input", "Summa noto'g'ri")
    r = await decide_topup(i, act == "approve", amount, str(b.get("note", "manual_reject"))[:200], req["u"]["id"])
    if not r: raise ApiErr(409, "conflict", "So'rov allaqachon hal qilingan")
    await notify_topup(r, i)
    return web.json_response({"ok": True})


async def a_users(req):
    q = req.query.get("q", "").strip().lstrip("@")
    return rows(await P.fetch("SELECT u.id,u.first_name,u.username,u.balance,u.is_blocked,(SELECT count(*) FROM orders WHERE user_id=u.id) AS orders FROM users u "
                              "WHERE $1='' OR u.first_name ILIKE '%'||$1||'%' OR u.username ILIKE '%'||$1||'%' OR u.id::text=$1 ORDER BY u.last_seen DESC LIMIT 50", q))


async def a_block(req):
    r = await P.fetchval("UPDATE users SET is_blocked=NOT is_blocked WHERE id=$1 RETURNING is_blocked", int(req.match_info["id"]))
    if r is None: raise ApiErr(404, "not_found", "Foydalanuvchi topilmadi")
    return web.json_response({"is_blocked": r})


async def a_balance(req):
    uid, b = int(req.match_info["id"]), await req.json()
    d = b.get("delta")
    if not isinstance(d, int) or d == 0: raise ApiErr(400, "invalid_input", "Summa noto'g'ri")
    async with P.acquire() as c, c.transaction():
        bal = await c.fetchval("UPDATE users SET balance=balance+$1 WHERE id=$2 AND balance+$1>=0 RETURNING balance", d, uid)
        if bal is None: raise ApiErr(400, "invalid_input", "Balans minusga tushmaydi yoki foydalanuvchi topilmadi")
        await c.execute("INSERT INTO balance_log(user_id,delta,balance_after,reason) VALUES($1,$2,$3,$4)", uid, d, bal, "admin: " + str(b.get("note", ""))[:100])
    await tell_u(uid, "bal_adj", delta=("+" if d > 0 else "-") + usd(abs(d)), balance=usd(bal))
    return web.json_response({"balance": bal})


async def a_settings_get(req):
    return web.json_response(await get_settings())


async def a_settings_put(req):
    b = await req.json()
    if not isinstance(b, dict) or not b: raise ApiErr(400, "invalid_input", "Bo'sh so'rov")
    for k, v in b.items(): valid_setting(k, v)
    async with P.acquire() as c, c.transaction():
        for k, v in b.items():
            await c.execute("INSERT INTO settings(key,value) VALUES($1,$2::jsonb) ON CONFLICT(key) DO UPDATE SET value=$2::jsonb", k, json.dumps(v))
    drop_cache()
    return web.json_response(await get_settings())


async def index(req):
    return web.FileResponse(os.path.join(STATIC, "index.html"), headers={"Cache-Control": "no-cache"})


# ---------- Bot ----------
dp = Dispatcher()


def open_kb(path="", text="Do'konni ochish"):
    return InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text=text, web_app=WebAppInfo(url=WEBAPP_URL + path))]])


@dp.message(CommandStart())
async def start(m: Message):
    parts = (m.text or "").split(maxsplit=1)
    await ref_link(m.from_user, parts[1] if len(parts) > 1 else "")
    s = await get_settings()
    miss = [] if m.from_user.id in admin_ids() else await missing_subs(m.from_user.id, s)
    L = await lang_of(m.from_user)
    if miss: return await m.answer(BT[L]["sub_need"], reply_markup=sub_kb(miss, L))
    await m.answer(s["welcome_text"] or BT[L]["welcome"], reply_markup=open_kb(text=BT[L]["open_btn"]))


def sub_kb(miss, L="uz"):
    k = [[InlineKeyboardButton(text=c["title"], url=c["url"])] for c in miss]
    return InlineKeyboardMarkup(inline_keyboard=k + [[InlineKeyboardButton(text=BT[L]["sub_btn"], callback_data="sub:check")]])


@dp.callback_query(F.data == "sub:check")
async def sub_check(q: CallbackQuery):
    L = await lang_of(q.from_user)
    if await missing_subs(q.from_user.id, await get_settings(), True): return await q.answer(BT[L]["sub_no"], show_alert=True)
    await q.message.answer(BT[L]["sub_ok"], reply_markup=open_kb(text=BT[L]["open_btn"])); await q.answer()


async def save_channels(chs):
    await P.execute("INSERT INTO settings(key,value) VALUES('force_sub',$1::jsonb) ON CONFLICT(key) DO UPDATE SET value=$1::jsonb", json.dumps(chs))
    drop_cache(); SUB_CACHE.clear()


@dp.message(Command("channels"))
async def channels_cmd(m: Message):
    if m.from_user.id not in admin_ids(): return
    c = (await get_settings())["force_sub"]
    await m.answer("Majburiy obuna kanallari:\n" + ("\n".join(f"• {x['chat']} — {x['title']}" for x in c) if c else "yo'q") + "\n\n/addchannel @kanal [sarlavha]\n/delchannel @kanal")


@dp.message(Command("addchannel"))
async def addch(m: Message):
    if m.from_user.id not in admin_ids(): return
    p = (m.text or "").split(maxsplit=2)
    if len(p) < 2 or not re.fullmatch(r"@[A-Za-z0-9_]{5,32}", p[1]): return await m.answer("Ishlatish: /addchannel @kanal [sarlavha]")
    try: ok = (await BOT.get_chat_member(p[1], (await BOT.me()).id)).status in ("administrator", "creator")
    except Exception: ok = False
    if not ok: return await m.answer("Bot bu kanalda admin bo'lishi shart.")
    c = [x for x in (await get_settings())["force_sub"] if x["chat"].lower() != p[1].lower()]
    c.append({"chat": p[1], "title": p[2] if len(p) > 2 else p[1], "url": "https://t.me/" + p[1][1:]})
    await save_channels(c); await m.answer("✅ Qo'shildi")


@dp.message(Command("delchannel"))
async def delch(m: Message):
    if m.from_user.id not in admin_ids(): return
    p = (m.text or "").split()
    if len(p) < 2: return await m.answer("Ishlatish: /delchannel @kanal")
    await save_channels([x for x in (await get_settings())["force_sub"] if x["chat"].lower() != p[1].lower()]); await m.answer("✅ O'chirildi")


@dp.message(Command("ref"))
async def ref_cmd(m: Message):
    s = await get_settings()
    n = await P.fetchval("SELECT count(*) FROM users WHERE invited_by=$1", m.from_user.id)
    e = await P.fetchval("SELECT ref_earned FROM users WHERE id=$1", m.from_user.id) or 0
    L = await lang_of(m.from_user)
    await m.answer(BT[L]["ref_info"].format(pct=s["ref_percent"], link=f"https://t.me/{BOT_USER}?start=ref{m.from_user.id}", n=n, earned=usd(e)))


@dp.message(Command("admin"))
async def admin_cmd(m: Message):
    if m.from_user.id in admin_ids(): await m.answer("🛠 Admin panel", reply_markup=open_kb("/#/admin", "Admin panel"))


@dp.message(Command("pending"))
async def pending(m: Message):
    if m.from_user.id not in admin_ids(): return
    os_ = await P.fetch("SELECT o.*,u.first_name FROM orders o JOIN users u ON u.id=o.user_id WHERE o.status='pending' ORDER BY o.id LIMIT 10")
    ts = await P.fetch("SELECT t.id,t.user_id,t.amount,t.credit,t.receipt,t.check_json,u.first_name FROM topups t JOIN users u ON u.id=t.user_id WHERE t.status='pending' ORDER BY t.id LIMIT 10")
    if not os_ and not ts: return await m.answer("✅ Kutilayotgan buyurtma va cheklar yo'q")
    for o in os_:
        what = f"{o['qty']} Stars" if o["kind"] == "stars" else f"Premium {o['qty']} oy"
        await m.answer(f"🆕 Buyurtma #{o['id']}\n{what} → @{o['recipient']}\nNarx: {usd(o['price'])}\nMijoz: {o['first_name']} (id {o['user_id']})",
                       reply_markup=admin_kb("ord", o["id"], "✅ Bajarildi", "✖ Bekor"))
    for t in ts:
        res = json.loads(t["check_json"]) if t["check_json"] else {"verdict": "off", "reasons": [], "found": {}}
        await m.answer_photo(BufferedInputFile(bytes(t["receipt"]), "receipt.jpg"), reply_markup=admin_kb("top", t["id"], "✅ Tasdiqlash", "✖ Rad etish"),
                             caption=f"🧾 Chek #{t['id']}\n{t['first_name']} (id {t['user_id']})\n{som(t['amount'])} → {usd(t['credit'])}" + chk_line(res))


@dp.message(Command("set"))  # /set usd_rate 12650  |  /set card_number "8600 ..."
async def set_cmd(m: Message):
    if m.from_user.id not in admin_ids(): return
    parts = (m.text or "").split(maxsplit=2)
    if len(parts) < 3 or parts[1] not in DEFAULTS:
        return await m.answer("Ishlatish: /set <kalit> <qiymat>\nKalitlar: " + ", ".join(DEFAULTS))
    try: val = json.loads(parts[2])
    except ValueError: val = parts[2]
    if type(val) is not type(DEFAULTS[parts[1]]) and not (isinstance(val, (int, float)) and isinstance(DEFAULTS[parts[1]], (int, float))):
        return await m.answer("Qiymat turi noto'g'ri.")
    await P.execute("INSERT INTO settings(key,value) VALUES($1,$2::jsonb) ON CONFLICT(key) DO UPDATE SET value=$2::jsonb", parts[1], json.dumps(val))
    drop_cache()
    await m.answer(f"✅ {parts[1]} = {val}")


async def mark(q: CallbackQuery, status):
    try:
        if q.message.photo: await q.message.edit_caption(caption=(q.message.caption or "") + f"\n\n{status}")
        else: await q.message.edit_text((q.message.text or "") + f"\n\n{status}")
    except Exception: pass


@dp.callback_query(F.data.regexp(r"^(ord|top):(ok|no):\d+$"))
async def decide(q: CallbackQuery):
    if q.from_user.id not in admin_ids(): return await q.answer("Ruxsat yo'q", show_alert=True)
    kind, act, i = q.data.split(":"); i = int(i)
    if kind == "ord":
        o = await decide_order(i, "done" if act == "ok" else "cancel", "Admin tomonidan bekor qilindi")
        if not o: return await q.answer("Allaqachon hal qilingan")
        await audit(q.from_user.id, f"bot:ord:{act}:{i}")
        await notify_order(o)
        await mark(q, "✅ Bajarildi" if act == "ok" else "✖ Bekor qilindi")
    else:
        r = await decide_topup(i, act == "ok", note="manual_reject", by=q.from_user.id)
        if not r: return await q.answer("Allaqachon hal qilingan")
        await audit(q.from_user.id, f"bot:top:{act}:{i}")
        await notify_topup(r, i)
        await mark(q, "✅ Tasdiqlandi" if r["ok"] else "✖ Rad etildi")
    await q.answer()


async def init_schema():
    async with P.acquire() as c:
        await c.execute("SELECT pg_advisory_lock(7101)")
        try: await c.execute(SCHEMA)
        finally: await c.execute("SELECT pg_advisory_unlock(7101)")


def make_app():
    app = web.Application(middlewares=[mw], client_max_size=8 * 1024 ** 2)
    app.add_routes([web.get("/healthz", lambda r: web.Response(text="ok")),
                    web.get("/api/bootstrap", bootstrap), web.post("/api/quote", quote),
                    web.post("/api/orders", create_order), web.get("/api/orders", list_orders),
                    web.post("/api/topups", create_topup), web.get("/api/topups", list_topups),
                    web.patch("/api/me", patch_me),
                    web.get("/api/admin/stats", adm(a_stats)), web.get("/api/admin/orders", adm(a_orders)),
                    web.post("/api/admin/orders/{id}/{act}", adm(a_order_act)), web.get("/api/admin/topups", adm(a_topups)),
                    web.get("/api/admin/topups/{id}/receipt", adm(a_receipt)), web.post("/api/admin/topups/{id}/{act}", adm(a_topup_act)),
                    web.post("/api/admin/topups/{id}/recheck", adm(a_recheck)),
                    web.post("/api/check-sub", check_sub), web.get("/api/referral", referral),
                    web.get("/api/admin/audit", adm(a_audit)), web.get("/api/admin/staff", sup(a_staff)), web.post("/api/admin/staff", sup(a_staff_add)),
                    web.delete("/api/admin/staff/{id}", sup(a_staff_del)), web.post("/api/admin/broadcast", adm(a_bc_new)), web.get("/api/admin/broadcast/{id}", adm(a_bc_get)), web.post("/api/promo/redeem", redeem),
                    web.get("/api/admin/promos", adm(a_promos)), web.post("/api/admin/promos", adm(a_promo_new)),
                    web.patch("/api/admin/promos/{code}", adm(a_promo_patch)), web.delete("/api/admin/promos/{code}", adm(a_promo_del)),
                    web.get("/api/admin/promos/{code}/redemptions", adm(a_promo_uses)),
                    web.get("/api/admin/users", adm(a_users)), web.post("/api/admin/users/{id}/block", adm(a_block)),
                    web.post("/api/admin/users/{id}/balance", adm(a_balance)),
                    web.get("/api/admin/settings", adm(a_settings_get)), web.put("/api/admin/settings", adm(a_settings_put)), web.get("/{tail:.*}", index)])
    return app


async def main():
    global P, BOT, BOT_USER
    P = await asyncpg.create_pool(DB_URL)
    await init_schema()
    STAFF.update(r["id"] for r in await P.fetch("SELECT id FROM admins"))
    BOT = Bot(BOT_TOKEN)
    BOT_USER = (await BOT.get_me()).username
    for r in await P.fetch("SELECT id FROM topups WHERE status='pending' AND check_json IS NULL"): spawn(run_check(r["id"]))  # qayta ishga tushganda
    app = make_app()
    runner = web.AppRunner(app); await runner.setup()
    await web.TCPSite(runner, "0.0.0.0", PORT).start()
    log.info("Velqo Shop ishga tushdi: port %s", PORT)
    try: await dp.start_polling(BOT)
    finally:
        await runner.cleanup(); await BOT.session.close(); await P.close()


if __name__ == "__main__":
    asyncio.run(main())
