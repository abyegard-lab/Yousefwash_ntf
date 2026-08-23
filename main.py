"""
النظام الكامل — شراء المينتات المجانية على Ink
"""

import asyncio
import json
import logging
import os
import time
from datetime import datetime, timezone, timedelta

import requests
import websockets
from dotenv import load_dotenv

from buyer import (
    get_web3,
    attempt_purchase_single_wallet,
    get_onchain_public_price_wei,
    get_wallet_lock,
)

load_dotenv()

OPENSEA_API_KEY = os.environ["OPENSEA_API_KEY"]
BOT_ENABLED = os.environ.get("BOT_ENABLED", "false").lower() == "true"

# المحافظ
PRIVATE_KEYS = [k.strip() for k in os.environ.get("PRIVATE_KEYS", "").split(",") if k.strip()]
WALLETS = [w.strip() for w in os.environ.get("WALLETS", "").split(",") if w.strip()]
TELEGRAM_BOT_TOKENS = [t.strip() for t in os.environ.get("TELEGRAM_BOT_TOKENS", "").split(",") if t.strip()]
TELEGRAM_CHAT_IDS = [c.strip() for c in os.environ.get("TELEGRAM_CHAT_IDS", "").split(",") if c.strip()]

if not (len(PRIVATE_KEYS) == len(WALLETS) == len(TELEGRAM_BOT_TOKENS) == len(TELEGRAM_CHAT_IDS)):
    raise ValueError("أعداد المفاتيح، المحافظ، توكنات البوتات، و Chat IDs غير متطابقة!")

WALLETS_DATA = []
for i in range(len(WALLETS)):
    WALLETS_DATA.append({
        "wallet": WALLETS[i],
        "private_key": PRIVATE_KEYS[i],
        "bot_token": TELEGRAM_BOT_TOKENS[i],
        "chat_id": TELEGRAM_CHAT_IDS[i],
    })

INK_RPC_URL = os.environ.get("INK_RPC_URL", "https://rpc-gel.inkonchain.com")
STREAM_URL = f"wss://stream.openseabeta.com/socket/websocket?token={OPENSEA_API_KEY}&vsn=2.0.0"
DROPS_API_BASE = "https://api.opensea.io/api/v2/drops"

ZERO_ADDRESS = "0x0000000000000000000000000000000000000000"
LOCAL_TZ = timezone(timedelta(hours=3))

HEARTBEAT_INTERVAL = 20
RECV_TIMEOUT = 5
FREE_PRICE_THRESHOLD_USD = 0.01
WATCH_POLL_INTERVAL_SECONDS = 15

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("auto-buyer")

CHAIN_CONFIGS = {
    "ink": {
        "stream_chain_name": "ink",
        "rpc_url": INK_RPC_URL,
        "max_gas_fee_usd": 0.05,
        "chain_label": "Ink",
    },
}

W3_INSTANCE = get_web3(INK_RPC_URL)
STREAM_NAME_TO_CHAIN_KEY = {cfg["stream_chain_name"]: key for key, cfg in CHAIN_CONFIGS.items()}

# إحصائيات بسيطة
successful_mints: dict[str, set[str]] = {}
watchlist: dict[str, dict] = {}
in_flight: set[str] = set()
rejected_cooldown: dict[str, float] = {}
total_purchases = 0
total_gas_fee = 0.0

# ============================================
# رسائل البوت
# ============================================

send_queue: "asyncio.Queue[dict]" = asyncio.Queue()

def enqueue_message(bot_token: str, chat_id: str, text: str):
    send_queue.put_nowait({
        "bot_token": bot_token,
        "chat_id": chat_id,
        "text": text
    })

def broadcast_message(text: str):
    for w in WALLETS_DATA:
        enqueue_message(w["bot_token"], w["chat_id"], text)

# ============================================
# رسائل التشغيل والشراء فقط
# ============================================

def build_startup_message() -> str:
    now = datetime.now(LOCAL_TZ).strftime("%Y-%m-%d %H:%M:%S")
    return (
        f"🚀 <b>تم تشغيل نظام الشراء التلقائي</b>\n\n"
        f"📅 {now}\n"
        f"💰 {len(WALLETS_DATA)} محفظة\n"
        f"🔗 Ink\n"
        f"🆓 المينتات المجانية فقط\n"
        f"✅ جاهز..."
    )

def build_purchase_message(detail: dict, result: dict) -> str:
    name = detail.get("collection_name") or detail.get("collection_slug") or "Unknown"
    url = detail.get("opensea_url", f"https://opensea.io/collection/{detail.get('collection_slug', '')}")
    w_short = result['wallet'][:6] + "..." + result['wallet'][-4:]
    tx_short = result['tx_hash'][:10]
    
    return (
        f"✅ <b>تم الشراء!</b>\n\n"
        f"🖼️ {name}\n"
        f"💰 {w_short}\n"
        f"🔢 {result['quantity']}\n"
        f"⛽ ${result['gas_fee_usd']:.4f}\n"
        f"🔗 <code>{tx_short}...</code>\n"
        f"🌐 <a href='{url}'>OpenSea</a>"
    )

# ============================================
# الوظائف الأساسية
# ============================================

async def telegram_sender():
    while True:
        msg = await send_queue.get()
        try:
            api = f"https://api.telegram.org/bot{msg['bot_token']}"
            await asyncio.to_thread(
                requests.post,
                f"{api}/sendMessage",
                data={
                    "chat_id": msg["chat_id"],
                    "text": msg["text"],
                    "parse_mode": "HTML",
                    "disable_web_page_preview": True,
                },
                timeout=10,
            )
        except Exception as e:
            log.error(f"خطأ تليجرام: {e}")
        send_queue.task_done()
        await asyncio.sleep(0.1)

def get_eth_price_usd() -> float:
    try:
        resp = requests.get(
            "https://api.coingecko.com/api/v3/simple/price?ids=ethereum&vs_currencies=usd",
            timeout=8,
        )
        return resp.json()["ethereum"]["usd"]
    except:
        return 3000.0

def fetch_drop_detail(slug: str):
    try:
        resp = requests.get(
            f"{DROPS_API_BASE}/{slug}",
            headers={"x-api-key": OPENSEA_API_KEY},
            timeout=10,
        )
        if resp.status_code == 200:
            return True, resp.json()
        return False, None
    except:
        return None, None

def parse_iso(ts: str):
    try:
        return datetime.fromisoformat(ts.replace("Z", "+00:00"))
    except:
        return None

def started_today_local(stage: dict) -> bool:
    start = parse_iso(stage.get("start_time", ""))
    if not start:
        return False
    return start.astimezone(LOCAL_TZ).date() == datetime.now(LOCAL_TZ).date()

def is_free(price_wei: int, eth_price_usd: float) -> bool:
    if price_wei == 0:
        return True
    return (price_wei / 1e18) * eth_price_usd < FREE_PRICE_THRESHOLD_USD

def is_in_cooldown(slug: str) -> bool:
    ts = rejected_cooldown.get(slug)
    if ts is None:
        return False
    if time.time() - ts >= 120:
        rejected_cooldown.pop(slug, None)
        return False
    return True

def mark_rejected(slug: str):
    rejected_cooldown[slug] = time.time()

# ============================================
# الشراء
# ============================================

async def purchase_task(item, slug, contract_address, price_wei, max_per_wallet, remaining, eth_price_usd):
    global total_purchases, total_gas_fee
    
    wallet_addr = item["wallet"]
    pk = item["private_key"]
    bot_token = item["bot_token"]
    chat_id = item["chat_id"]

    lock = get_wallet_lock(wallet_addr)
    async with lock:
        if wallet_addr in successful_mints.get(slug, set()):
            return

        res = await asyncio.to_thread(
            attempt_purchase_single_wallet,
            W3_INSTANCE, pk, wallet_addr,
            contract_address, price_wei, max_per_wallet, remaining,
            eth_price_usd, 0.05,
        )

        if res.get("success"):
            if slug not in successful_mints:
                successful_mints[slug] = set()
            successful_mints[slug].add(wallet_addr)
            
            total_purchases += 1
            total_gas_fee += res.get("gas_fee_usd", 0)
            
            # ✅ إشعار شراء ناجح فقط
            msg = build_purchase_message(item.get("detail", {}), res)
            enqueue_message(bot_token, chat_id, msg)
            
            log.info(f"✅ شراء: {wallet_addr[:8]}... {res['quantity']} | غاز: ${res['gas_fee_usd']:.4f}")

        return res

async def try_buy_now(slug: str, detail: dict):
    stage = detail.get("active_stage")
    if not stage:
        return

    max_supply = int(detail.get("max_supply") or 0)
    total_supply = int(detail.get("total_supply") or 0)
    remaining = max_supply - total_supply
    if remaining <= 0:
        return

    contract_address = detail.get("contract_address")
    if not contract_address:
        return

    eth_price_usd = get_eth_price_usd()
    onchain_price = await asyncio.to_thread(get_onchain_public_price_wei, W3_INSTANCE, contract_address)
    price_wei = onchain_price if onchain_price is not None else int(stage.get("price", "0"))

    if not is_free(price_wei, eth_price_usd):
        return

    max_per_wallet = int(stage.get("max_total_mintable_by_wallet") or 0)
    if max_per_wallet <= 0:
        max_per_wallet = 1

    already_bought = successful_mints.get(slug, set())
    pending = [item for item in WALLETS_DATA if item["wallet"] not in already_bought]

    if not pending:
        return

    for item in pending:
        item["detail"] = detail

    tasks = [
        purchase_task(
            item, slug, contract_address,
            price_wei, max_per_wallet, remaining, eth_price_usd
        )
        for item in pending
    ]

    await asyncio.gather(*tasks)

# ============================================
# المراقبة
# ============================================

async def evaluate_new_mint(slug: str):
    if (
        slug in watchlist
        or slug in in_flight
        or is_in_cooldown(slug)
        or len(successful_mints.get(slug, set())) >= len(WALLETS_DATA)
    ):
        return

    in_flight.add(slug)
    try:
        found, detail = await asyncio.to_thread(fetch_drop_detail, slug)
        if not found or not detail or not detail.get("is_minting"):
            return

        stage = detail.get("active_stage")
        if not stage or not started_today_local(stage):
            return

        contract_address = detail.get("contract_address")
        if not contract_address:
            return

        eth_price_usd = get_eth_price_usd()
        onchain_price = await asyncio.to_thread(get_onchain_public_price_wei, W3_INSTANCE, contract_address)
        price_wei = onchain_price if onchain_price is not None else int(stage.get("price", "0"))

        if not is_free(price_wei, eth_price_usd):
            mark_rejected(slug)
            return

        log.info(f"✅ مينت مجاني: {slug}")
        await try_buy_now(slug, detail)

        if len(successful_mints.get(slug, set())) < len(WALLETS_DATA):
            watchlist[slug] = {"detail": detail}

    except Exception as e:
        log.error(f"خطأ: {e}")
    finally:
        in_flight.discard(slug)

async def watch_loop():
    while True:
        await asyncio.sleep(WATCH_POLL_INTERVAL_SECONDS)
        for slug in list(watchlist.keys()):
            if slug in in_flight:
                continue
            if len(successful_mints.get(slug, set())) >= len(WALLETS_DATA):
                watchlist.pop(slug, None)
                continue
            await evaluate_new_mint(slug)

async def listen_opensea():
    msg_ref = 0
    while True:
        try:
            async with websockets.connect(STREAM_URL, ping_interval=None, open_timeout=15) as ws:
                log.info("✅ متصل بـ OpenSea")
                await ws.send(json.dumps([str(msg_ref), str(msg_ref), "collection:*", "phx_join", {}]))
                msg_ref += 1
                last_heartbeat = time.time()

                while True:
                    if time.time() - last_heartbeat > HEARTBEAT_INTERVAL:
                        await ws.send(json.dumps([None, str(msg_ref), "phoenix", "heartbeat", {}]))
                        msg_ref += 1
                        last_heartbeat = time.time()

                    try:
                        raw = await asyncio.wait_for(ws.recv(), timeout=RECV_TIMEOUT)
                    except asyncio.TimeoutError:
                        continue

                    try:
                        parsed = json.loads(raw)
                    except:
                        continue

                    if isinstance(parsed, list) and len(parsed) == 5:
                        _, _, _, event_name, payload_wrapper = parsed
                    else:
                        continue

                    if event_name != "item_transferred":
                        continue

                    payload = (payload_wrapper or {}).get("payload") or {}
                    item = payload.get("item", {}) or {}
                    chain_name = (item.get("chain", {}) or {}).get("name", "")

                    if STREAM_NAME_TO_CHAIN_KEY.get(chain_name) != "ink":
                        continue

                    from_address = ((payload.get("from_account") or {}).get("address", "") or "").lower()
                    if from_address != ZERO_ADDRESS:
                        continue

                    slug = (payload.get("collection", {}) or {}).get("slug", "")
                    if slug:
                        asyncio.create_task(evaluate_new_mint(slug))

        except Exception as e:
            log.warning(f"إعادة اتصال: {e}")
            await asyncio.sleep(3)

# ============================================
# التشغيل
# ============================================

async def run():
    if not BOT_ENABLED:
        log.warning("🔴 BOT_ENABLED=false")
        return

    broadcast_message(build_startup_message())
    await asyncio.gather(listen_opensea(), watch_loop(), telegram_sender())

def main():
    while True:
        try:
            asyncio.run(run())
        except KeyboardInterrupt:
            log.info("🛑 إيقاف")
            break
        except Exception as e:
            log.critical(f"💥 خطأ: {e}")
            time.sleep(5)

if __name__ == "__main__":
    main()
