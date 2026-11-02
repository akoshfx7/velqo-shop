"""Haqiqiy PostgreSQL bilan: TEST_DATABASE_URL=postgresql://... python -m pytest -q  (bo'lmasa o'tkazib yuboriladi). BAZA TOZALANADI — alohida test bazasini bering."""
import asyncio, os, sqlite3, tempfile
import pytest

URL = os.getenv("TEST_DATABASE_URL")
os.environ.update(BOT_TOKEN="123:test", DATABASE_URL=URL or "postgresql://x/x", ADMIN_IDS="1", DEV_MODE="1")
asyncpg = pytest.importorskip("asyncpg")
from aiohttp.test_utils import TestClient, TestServer
from app import main


class FakeBot:
    def __init__(s): s.sent = []
    async def send_message(s, uid, text, **kw): s.sent.append((uid, text))
    async def send_photo(s, uid, photo, **kw): s.sent.append((uid, kw.get("caption", "")))
    async def get_chat_member(s, *a): raise RuntimeError("no")


def case(f):
    def w():
        async def go():
            main.P, main.BOT = await asyncpg.create_pool(URL), FakeBot()
            await main.init_schema()
            await main.P.execute("TRUNCATE users,orders,topups,balance_log,settings,promos,promo_uses,admins,audit,broadcasts RESTART IDENTITY CASCADE")
            main.drop_cache(); main.SUB_CACHE.clear()
            try: await f()
            finally: await asyncio.sleep(0.05); await main.P.close()
        asyncio.run(go())
    w.__name__ = f.__name__
    return pytest.mark.skipif(not URL, reason="TEST_DATABASE_URL yo'q")(w)


H = lambda u: {"X-Dev-User": str(u)}
ORDER = {"kind": "stars", "qty": 50, "recipient": {"type": "self"}}  # 50 Stars = 83 sent
bal = lambda u: main.P.fetchval("SELECT balance FROM users WHERE id=$1", u)
stat = lambda t: main.P.fetchval("SELECT status FROM topups WHERE id=$1", t)


async def mkuser(i, balance=0, by=None):
    await main.P.execute("INSERT INTO users(id,first_name,username,balance,invited_by) VALUES($1,$2,$3,$4,$5)", i, f"U{i}", f"user_{i}", balance, by)


async def topup(uid, amount=50000, rate=12650):
    return await main.P.fetchval("INSERT INTO topups(user_id,amount,rate,credit,receipt,mime) VALUES($1,$2,$3,$4,$5,'image/png') RETURNING id", uid, amount, rate, amount * 100 // rate, b"x")


async def client():
    c = TestClient(TestServer(main.make_app())); await c.start_server(); return c


@case
async def test_parallel_orders_never_negative():
    await mkuser(101, 83 * 3); c = await client()
    rs = await asyncio.gather(*[c.post("/api/orders", json=ORDER, headers=H(101)) for _ in range(10)])
    st = [r.status for r in rs]
    assert st.count(201) == 3 and st.count(402) == 7 and await bal(101) == 0
    await c.close()


@case
async def test_cancel_refunds_once_and_done_is_final():
    await mkuser(101, 166); c = await client()
    o1 = (await (await c.post("/api/orders", json=ORDER, headers=H(101))).json())["order"]["id"]
    o2 = (await (await c.post("/api/orders", json=ORDER, headers=H(101))).json())["order"]["id"]
    res = await asyncio.gather(main.decide_order(o1, "cancel", "x"), main.decide_order(o1, "cancel", "x"))
    assert sum(1 for r in res if r) == 1 and await bal(101) == 83
    assert await main.decide_order(o2, "done") and await main.decide_order(o2, "cancel", "x") is None and await bal(101) == 83
    await c.close()


@case
async def test_topup_triple_approve_and_conversion():
    await mkuser(101); tid = await topup(101)  # 50 000 so'm @ 12 650 = 395 sent
    res = [r for r in await asyncio.gather(*[main.decide_topup(tid, True) for _ in range(3)]) if r]
    assert len(res) == 1 and res[0]["credit"] == 395 and await bal(101) == 395


@case
async def test_decisions_idempotent():
    await mkuser(101); tid = await topup(101)
    assert await main.decide_topup(tid, False, note="x") and await main.decide_topup(tid, False) is None and await main.decide_topup(tid, True) is None
    assert await bal(101) == 0


@case
async def test_referral_bonus_once_and_notified():
    await mkuser(5); await mkuser(6, by=5); tid = await topup(6)
    await main.decide_topup(tid, True); await main.decide_topup(tid, True); await asyncio.sleep(0.1)
    assert await bal(5) == 19 and await main.P.fetchval("SELECT ref_earned FROM users WHERE id=5") == 19  # 395 * 5%
    assert [u for u, _ in main.BOT.sent].count(5) == 1


@case
async def test_promo_once_and_limit():
    await mkuser(101); await mkuser(102); c = await client()
    await main.P.execute("INSERT INTO promos(code,amount,max_uses) VALUES('GIFT',100,1)")
    assert (await c.post("/api/promo/redeem", json={"code": "gift"}, headers=H(101))).status == 200
    assert (await c.post("/api/promo/redeem", json={"code": "GIFT"}, headers=H(101))).status == 409
    assert (await c.post("/api/promo/redeem", json={"code": "GIFT"}, headers=H(102))).status == 409
    assert await bal(101) == 100 and await bal(102) == 0
    await c.close()


@case
async def test_blocked_and_maintenance():
    await mkuser(101); await mkuser(102); c = await client()
    await main.P.execute("UPDATE users SET is_blocked=true WHERE id=101")
    assert (await c.get("/api/bootstrap", headers=H(101))).status == 403
    await main.P.execute("INSERT INTO settings VALUES('maintenance','true')"); main.drop_cache()
    assert (await c.get("/api/bootstrap", headers=H(102))).status == 503
    assert (await c.get("/api/bootstrap", headers=H(1))).status == 200  # admin
    await c.close()


@case
async def test_ocr_auto_modes():
    orig = main.rc.analyze
    try:
        await mkuser(101)
        cfg = {"mode": "auto", "auto_approve_max": 100000, "max_age_minutes": 60, "auto_reject": True, "require_card_match": False}
        await main.P.execute("INSERT INTO settings VALUES('receipt_check',$1::jsonb)", __import__("json").dumps(cfg)); main.drop_cache()
        main.rc.analyze = lambda *a: {"verdict": "pass", "reasons": [], "found": {}}
        small, big = await topup(101, 50000), await topup(101, 200000)
        await main.run_check(small); await main.run_check(big)
        assert await stat(small) == "approved" and await stat(big) == "pending"
        assert await main.P.fetchval("SELECT decided_auto FROM topups WHERE id=$1", small)
        main.rc.analyze = lambda *a: {"verdict": "fail", "reasons": ["amount_mismatch"], "found": {}}
        bad = await topup(101, 50000); await main.run_check(bad)
        assert await stat(bad) == "rejected" and await main.P.fetchval("SELECT note FROM topups WHERE id=$1", bad) == "amount_mismatch"
    finally: main.rc.analyze = orig


@case
async def test_import_old_sqlite_idempotent_and_dry_run():
    from tools import import_old_sqlite as imp
    f = os.path.join(tempfile.mkdtemp(), "old.db"); db = sqlite3.connect(f)
    db.execute("CREATE TABLE users(id INTEGER, first_name TEXT, username TEXT, balance REAL)")
    db.execute("INSERT INTO users VALUES(7,'A','a_user',1.65)"); db.commit(); db.close()
    assert (await imp.run(f, True, URL))["new"] == 1 and await main.P.fetchval("SELECT count(*) FROM users") == 0
    assert (await imp.run(f, False, URL))["new"] == 1 and await bal(7) == 165
    assert (await imp.run(f, False, URL))["new"] == 0 and await bal(7) == 165
