"""NFT free-mint auto buyer V2 for Ink + OpenSea Stream.

Flow: OpenSea discovery -> OpenSea metadata -> X filter -> on-chain SeaDrop validation
-> eth_call simulation -> gas/balance check -> send -> SQLite state.
"""
import asyncio
import json
import logging
import os
import sqlite3
import time
from collections import defaultdict
from datetime import datetime, timezone

import requests
import websockets
from dotenv import load_dotenv

from buyer import get_web3, get_onchain_phase_info, attempt_purchase_single_wallet
from twitter_checker import get_twitter_username_from_opensea, is_valid_twitter_account, get_last_lookup_status

load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger("auto-buyer")

OPENSEA_API_KEY = os.environ.get("OPENSEA_API_KEY", "")
BOT_ENABLED = os.environ.get("BOT_ENABLED", "false").lower() == "true"
PRIVATE_KEYS = [x.strip() for x in os.environ.get("PRIVATE_KEYS", "").split(",") if x.strip()]
WALLETS = [x.strip() for x in os.environ.get("WALLETS", "").split(",") if x.strip()]
TELEGRAM_BOT_TOKENS = [x.strip() for x in os.environ.get("TELEGRAM_BOT_TOKENS", "").split(",") if x.strip()]
TELEGRAM_CHAT_IDS = [x.strip() for x in os.environ.get("TELEGRAM_CHAT_IDS", "").split(",") if x.strip()]

if not OPENSEA_API_KEY:
    raise ValueError("OPENSEA_API_KEY غير مضبوط")
if not (len(PRIVATE_KEYS) == len(WALLETS) == len(TELEGRAM_BOT_TOKENS) == len(TELEGRAM_CHAT_IDS)):
    raise ValueError("أعداد PRIVATE_KEYS/WALLETS/TELEGRAM_BOT_TOKENS/TELEGRAM_CHAT_IDS غير متطابقة")

INK_RPC_URL = os.environ.get("INK_RPC_URL", "https://rpc-gel.inkonchain.com/")
MAX_GAS_FEE_USD = float(os.environ.get("MAX_GAS_FEE_USD", "0.05"))
MAX_BUY_QTY = min(20, max(1, int(os.environ.get("MAX_BUY_QTY", "20"))))
ETH_PRICE_USD = float(os.environ.get("ETH_PRICE_USD", "3000"))
MAX_PARALLEL_DISCOVERY = int(os.environ.get("MAX_PARALLEL_DISCOVERY", "8"))
POLL_INTERVAL = float(os.environ.get("POLL_INTERVAL", "2"))
DROP_CACHE_SECONDS = 0.75
X_LOG_COOLDOWN_SECONDS = 600
X_RETRY_SECONDS = 30
DB_PATH = os.environ.get("STATE_DB", "nft_bot.sqlite3")

STREAM_URL = f"wss://stream.openseabeta.com/socket/websocket?token={OPENSEA_API_KEY}&vsn=2.0.0"
DROPS_API_BASE = "https://api.opensea.io/api/v2/drops"
CHAIN_KEY = "ink"
W3 = get_web3(INK_RPC_URL)

WALLETS_DATA = [{"wallet": WALLETS[i], "private_key": PRIVATE_KEYS[i], "bot_token": TELEGRAM_BOT_TOKENS[i], "chat_id": TELEGRAM_CHAT_IDS[i]} for i in range(len(WALLETS))]
wallet_locks = {w["wallet"].lower(): asyncio.Lock() for w in WALLETS_DATA}
semaphore = asyncio.Semaphore(MAX_PARALLEL_DISCOVERY)
state_lock = asyncio.Lock()

_drop_cache = {}
_inflight = set()
_watchlist = {}
_recent_events = {}
_x_retry = {}
_x_last_log = {}
_send_queue = asyncio.Queue()

stats = defaultdict(int)
stats["started"] = time.time()


def db():
    c = sqlite3.connect(DB_PATH, timeout=15)
    c.execute("PRAGMA journal_mode=WAL")
    c.execute("CREATE TABLE IF NOT EXISTS purchases (slug TEXT, wallet TEXT, tx_hash TEXT, status TEXT, quantity INTEGER, created_at INTEGER, PRIMARY KEY(slug,wallet))")
    c.execute("CREATE TABLE IF NOT EXISTS seen (slug TEXT PRIMARY KEY, first_seen INTEGER, last_seen INTEGER)")
    c.commit()
    return c


def mark_seen(slug):
    c = db(); now = int(time.time())
    c.execute("INSERT INTO seen(slug,first_seen,last_seen) VALUES(?,?,?) ON CONFLICT(slug) DO UPDATE SET last_seen=excluded.last_seen", (slug, now, now)); c.commit(); c.close()


def already_purchased(slug, wallet):
    c = db(); row = c.execute("SELECT status FROM purchases WHERE slug=? AND wallet=?", (slug, wallet.lower())).fetchone(); c.close()
    return bool(row and row[0] == "success")


def save_purchase(slug, wallet, tx_hash, status, quantity):
    c = db(); c.execute("INSERT INTO purchases(slug,wallet,tx_hash,status,quantity,created_at) VALUES(?,?,?,?,?,?) ON CONFLICT(slug,wallet) DO UPDATE SET tx_hash=excluded.tx_hash,status=excluded.status,quantity=excluded.quantity,created_at=excluded.created_at", (slug, wallet.lower(), tx_hash or "", status, quantity or 0, int(time.time()))); c.commit(); c.close()


def fetch_drop_detail(slug):
    cached = _drop_cache.get(slug)
    if cached and time.time() - cached[1] < DROP_CACHE_SECONDS:
        return True, cached[0]
    try:
        r = requests.get(f"{DROPS_API_BASE}/{slug}", headers={"x-api-key": OPENSEA_API_KEY}, timeout=5)
        stats["api_calls"] += 1
        if r.status_code == 200:
            detail = r.json(); _drop_cache[slug] = (detail, time.time()); return True, detail
        if r.status_code == 404:
            return False, None
        return None, None
    except Exception as e:
        log.warning("Drops API %s: %s", slug, e); return None, None


def parse_time(s):
    if not s: return None
    try: return datetime.fromisoformat(str(s).replace("Z", "+00:00"))
    except Exception: return None


def free_stage_active(detail):
    stage = detail.get("active_stage") or {}
    try: price = int(stage.get("price", 0))
    except Exception: price = 0
    # OpenSea is discovery only. The buyer re-checks exact price on-chain.
    return stage, price == 0


def get_contract(detail):
    return detail.get("contract_address") or detail.get("contract") or ""


def remaining_supply(detail):
    try:
        mx = int(detail.get("max_supply") or 0)
        total = int(detail.get("total_supply") or 0)
        # If OpenSea does not expose supply, do not artificially reduce the
        # quantity to 1; the on-chain wallet limit remains authoritative.
        return max(0, mx-total) if mx else MAX_BUY_QTY
    except Exception:
        return MAX_BUY_QTY


def telegram_enqueue(text):
    for w in WALLETS_DATA:
        _send_queue.put_nowait((w["bot_token"], w["chat_id"], text))

async def telegram_sender():
    while True:
        token, chat, text = await _send_queue.get()
        try:
            await asyncio.to_thread(requests.post, f"https://api.telegram.org/bot{token}/sendMessage", data={"chat_id": chat, "text": text, "parse_mode": "HTML", "disable_web_page_preview": True}, timeout=10)
        except Exception as e:
            stats["telegram_errors"] += 1; log.warning("Telegram: %s", e)
        finally:
            _send_queue.task_done()

async def validate_x(slug):
    """OpenSea-only X check. 429/timeouts are temporary, never negative."""
    username = await asyncio.to_thread(get_twitter_username_from_opensea, slug, OPENSEA_API_KEY)
    status = get_last_lookup_status(slug)
    if username and is_valid_twitter_account(username):
        return True, username
    if status == "temporary":
        return None, None
    return False, None


async def buy_for_wallet(slug, detail, wallet_item, phase):
    wallet = wallet_item["wallet"]
    async with wallet_locks[wallet.lower()]:
        if already_purchased(slug, wallet):
            return {"success": False, "reason": "already_bought", "wallet": wallet}
        stats["purchase_attempts"] += 1
        result = await asyncio.to_thread(
            attempt_purchase_single_wallet, W3, wallet_item["private_key"], wallet,
            get_contract(detail), 0, phase["maxTotalMintableByWallet"], remaining_supply(detail),
            ETH_PRICE_USD, MAX_GAS_FEE_USD, MAX_BUY_QTY
        )
        if result.get("success"):
            save_purchase(slug, wallet, result["tx_hash"], "success", result.get("quantity", 0)); stats["mints_purchased"] += result.get("quantity", 0)
            telegram_enqueue(f"✅ <b>Free Mint ناجح</b>\n📦 {detail.get('collection_name') or slug}\n👛 <code>{wallet[:6]}...{wallet[-4:]}</code>\n🔢 الكمية: {result.get('quantity')}\n⛽ الغاز: ${result.get('gas_fee_usd',0):.6f}\n🔗 <code>{result['tx_hash']}</code>")
        elif result.get("pending"):
            save_purchase(slug, wallet, result.get("tx_hash"), "pending", result.get("quantity", 0))
        return result

async def attempt_mint(slug, detail):
    contract = get_contract(detail)
    if not contract:
        return
    phase = await asyncio.to_thread(get_onchain_phase_info, W3, contract)
    if not phase or phase["mintPrice"] != 0 or not phase["is_active"]:
        return
    # Use on-chain wallet limit; quantity is bounded again in buyer.py.
    tasks = [buy_for_wallet(slug, detail, w, phase) for w in WALLETS_DATA if not already_purchased(slug, w["wallet"])]
    if tasks:
        results = await asyncio.gather(*tasks, return_exceptions=True)
        if any(isinstance(r, dict) and r.get("success") for r in results):
            _watchlist[slug] = detail

async def evaluate(slug):
    async with semaphore:
        if slug in _inflight: return
        _inflight.add(slug)
        try:
            found, detail = await asyncio.to_thread(fetch_drop_detail, slug)
            if not found or not detail: return
            mark_seen(slug); stats["mints_detected"] += 1
            retry_at = _x_retry.get(slug, 0)
            if retry_at and time.time() < retry_at:
                return
            x_ok, username = await validate_x(slug)
            x_status = get_last_lookup_status(slug)
            now = time.time()
            if x_ok is None:
                retry_at = now + X_RETRY_SECONDS
                _x_retry[slug] = max(_x_retry.get(slug, 0), retry_at)
                if now - _x_last_log.get(slug, 0) >= 30:
                    log.warning("⏳ %s: OpenSea X lookup temporary failure; will retry", slug)
                    _x_last_log[slug] = now
                return
            if not x_ok:
                _x_retry.pop(slug, None)
                # Do not spam logs when Stream emits repeated events.
                if now - _x_last_log.get(slug, 0) >= X_LOG_COOLDOWN_SECONDS:
                    log.info("❌ %s: no X account listed by OpenSea", slug)
                    _x_last_log[slug] = now
                return
            _x_retry.pop(slug, None)
            _x_last_log.pop(slug, None)
            stage, free = free_stage_active(detail)
            if free:
                await attempt_mint(slug, detail)
            else:
                _watchlist[slug] = detail
        except Exception as e:
            stats["errors"] += 1; log.exception("evaluate %s: %s", slug, e)
        finally:
            _inflight.discard(slug)

async def watch_loop():
    while True:
        await asyncio.sleep(POLL_INTERVAL)
        for slug in list(_watchlist):
            if slug in _inflight: continue
            found, detail = await asyncio.to_thread(fetch_drop_detail, slug)
            if not found or not detail: continue
            _watchlist[slug] = detail
            if _x_retry.get(slug, 0) <= time.time():
                await evaluate(slug)

async def listen_opensea():
    ref = 0
    events = ["item_transferred", "item_listed", "collection_created", "item_sold", "item_metadata_updated"]
    while True:
        try:
            async with websockets.connect(STREAM_URL, ping_interval=20, ping_timeout=10, open_timeout=15, close_timeout=5) as ws:
                join = str(ref); ref += 1
                await ws.send(json.dumps([join, join, "collection:*", "phx_join", {}]))
                for ev in events:
                    r = str(ref); ref += 1
                    await ws.send(json.dumps([r, r, f"collection:*:{ev}", "phx_join", {}]))
                log.info("✅ OpenSea Stream connected")
                while True:
                    raw = await ws.recv()
                    try: msg = json.loads(raw)
                    except Exception: continue
                    if not isinstance(msg, list) or len(msg) < 5: continue
                    topic, event, payload = msg[2], msg[3], msg[4] or {}
                    if event in {"phx_reply", "phx_error"}: continue
                    p = payload.get("payload") or {}
                    col = p.get("collection") or {}
                    item = p.get("item") or {}
                    slug = col.get("slug") or (item.get("collection") or {}).get("slug")
                    chain = (item.get("chain") or {}).get("name") or (col.get("chain") or {}).get("name")
                    if not slug or str(chain).lower() != "ink": continue
                    now = time.time()
                    if now - _recent_events.get(slug, 0) < 1: continue
                    _recent_events[slug] = now
                    asyncio.create_task(evaluate(slug))
        except (asyncio.CancelledError, KeyboardInterrupt): raise
        except Exception as e:
            stats["errors"] += 1; log.warning("Stream disconnected: %s; reconnecting", e); await asyncio.sleep(2)

async def startup():
    db().close()
    log.info(
        "🚀 NFT bot V2 started | wallets=%s | max gas=$%.4f | qty=auto (cap=%s)",
        len(WALLETS_DATA), MAX_GAS_FEE_USD, MAX_BUY_QTY
    )
    telegram_enqueue(
        "🚀 <b>NFT Bot V2 بدأ</b>\n"
        "🔗 Ink\n"
        "💰 Free mint only\n"
        "⛽ حد الغاز: $%.4f\n"
        "🔢 الكمية: تلقائية (حد أقصى %s)\n"
        "🐦 X: OpenSea فقط" % (MAX_GAS_FEE_USD, MAX_BUY_QTY)
    )


async def run():
    if not BOT_ENABLED:
        log.warning("BOT_ENABLED=false — no purchases will run")
        await telegram_sender()
        return
    await startup()
    await asyncio.gather(listen_opensea(), watch_loop(), telegram_sender())

if __name__ == "__main__":
    asyncio.run(run())
