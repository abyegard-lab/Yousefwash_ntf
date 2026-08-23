"""
النظام الكامل — محافظ متعددة مع بوت تيليجرام خاص لكل محفظة
يدعم شبكة Ink فقط - المينتات المجانية فقط
"""

import asyncio
import json
import logging
import os
import sys
import time
from datetime import datetime, timezone, timedelta

import requests
import websockets
from dotenv import load_dotenv

from buyer import (
    get_web3,
    attempt_purchase_single_wallet,
    get_onchain_public_price_wei,
    get_max_per_wallet,
    get_wallet_lock,
    is_free_mint,
)

load_dotenv()

# ============================================
# إعدادات السجلات
# ============================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%H:%M:%S",
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)
log = logging.getLogger("auto-buyer")

# ============================================
# قراءة المتغيرات من .env
# ============================================
OPENSEA_API_KEY = os.environ.get("OPENSEA_API_KEY")
if not OPENSEA_API_KEY:
    log.error("❌ OPENSEA_API_KEY غير موجود في .env")
    sys.exit(1)

BOT_ENABLED = os.environ.get("BOT_ENABLED", "true").lower() == "true"

# تفكيك المحافظ والمفاتيح وإعدادات التيليجرام
PRIVATE_KEYS = [k.strip() for k in os.environ.get("PRIVATE_KEYS", "").split(",") if k.strip()]
WALLETS = [w.strip() for w in os.environ.get("WALLETS", "").split(",") if w.strip()]
TELEGRAM_BOT_TOKENS = [t.strip() for t in os.environ.get("TELEGRAM_BOT_TOKENS", "").split(",") if t.strip()]
TELEGRAM_CHAT_IDS = [c.strip() for c in os.environ.get("TELEGRAM_CHAT_IDS", "").split(",") if c.strip()]

if not (len(PRIVATE_KEYS) == len(WALLETS) == len(TELEGRAM_BOT_TOKENS) == len(TELEGRAM_CHAT_IDS)):
    raise ValueError("أعداد المفاتيح، المحافظ، توكنات البوتات، و Chat IDs غير متطابقة في ملف .env!")

# إنشاء هيكلية المحافظ
WALLETS_DATA = []
for i in range(len(WALLETS)):
    WALLETS_DATA.append({
        "wallet": WALLETS[i],
        "private_key": PRIVATE_KEYS[i],
        "bot_token": TELEGRAM_BOT_TOKENS[i],
        "chat_id": TELEGRAM_CHAT_IDS[i],
    })

log.info(f"💰 تم تحميل {len(WALLETS_DATA)} محافظ")

# ============================================
# إعدادات الشبكة - Ink فقط
# ============================================
INK_RPC_URL = os.environ.get("INK_RPC_URL", "https://rpc-gel.inkonchain.com")

STREAM_URL = f"wss://stream.openseabeta.com/socket/websocket?token={OPENSEA_API_KEY}&vsn=2.0.0"
DROPS_API_BASE = "https://api.opensea.io/api/v2/drops"

ZERO_ADDRESS = "0x0000000000000000000000000000000000000000"
LOCAL_TZ = timezone(timedelta(hours=3))

HEARTBEAT_INTERVAL = 20
RECV_TIMEOUT = 5
FREE_PRICE_THRESHOLD_USD = 0.01
WATCH_POLL_INTERVAL_SECONDS = 15
MAX_GAS_FEE_USD = 0.05

# تكوين Ink فقط
CHAIN_CONFIGS = {
    "ink": {
        "stream_chain_name": "ink",
        "rpc_url": INK_RPC_URL,
        "max_gas_fee_usd": MAX_GAS_FEE_USD,
        "chain_label": "Ink",
    },
}

W3_INSTANCE = get_web3(INK_RPC_URL)
if not W3_INSTANCE.is_connected():
    log.error("❌ فشل الاتصال بـ RPC")
    sys.exit(1)
log.info(f"✅ متصل بالشبكة، الكتلة: {W3_INSTANCE.eth.block_number}")

STREAM_NAME_TO_CHAIN_KEY = {cfg["stream_chain_name"]: key for key, cfg in CHAIN_CONFIGS.items()}

# تتبع المحافظ التي اشترت بنجاح: slug -> set(wallet_address)
successful_mints: dict[str, set[str]] = {}
watchlist: dict[str, dict] = {}
in_flight: set[str] = set()

# تبريد مؤقت للمجموعات التي رُفضت
REJECTION_COOLDOWN_SECONDS = 120
rejected_cooldown: dict[str, float] = {}

# إحصائيات
total_purchases = 0
total_gas_fee = 0.0
start_time = time.time()
detected_mints = 0

_eth_price_cache = {"value": None, "ts": 0}

# ============================================
# الوظائف المساعدة
# ============================================

def is_in_cooldown(slug: str) -> bool:
    ts = rejected_cooldown.get(slug)
    if ts is None:
        return False
    if time.time() - ts >= REJECTION_COOLDOWN_SECONDS:
        rejected_cooldown.pop(slug, None)
        return False
    return True

def mark_rejected(slug: str):
    rejected_cooldown[slug] = time.time()

def get_eth_price_usd() -> float:
    now = time.time()
    if _eth_price_cache["value"] and (now - _eth_price_cache["ts"] < 300):
        return _eth_price_cache["value"]
    try:
        resp = requests.get(
            "https://api.coingecko.com/api/v3/simple/price?ids=ethereum&vs_currencies=usd",
            timeout=8,
        )
        price = resp.json()["ethereum"]["usd"]
        _eth_price_cache["value"] = price
        _eth_price_cache["ts"] = now
        return price
    except Exception as e:
        log.warning(f"[السعر] تعذر جلب سعر ETH: {e}")
        return _eth_price_cache["value"] or 3000.0

def fetch_drop_detail(slug: str):
    try:
        resp = requests.get(
            f"{DROPS_API_BASE}/{slug}",
            headers={"x-api-key": OPENSEA_API_KEY},
            timeout=10,
        )
        if resp.status_code == 200:
            return True, resp.json()
        if resp.status_code == 404:
            return False, None
        return None, None
    except Exception as e:
        log.warning(f"[Drops API] خطأ: {e}")
        return None, None

def parse_iso(ts: str):
    try:
        return datetime.fromisoformat(ts.replace("Z", "+00:00"))
    except Exception:
        return None

def started_today_local(stage: dict) -> bool:
    start = parse_iso(stage.get("start_time", ""))
    if not start:
        return False
    return start.astimezone(LOCAL_TZ).date() == datetime.now(LOCAL_TZ).date()

def stage_has_ended(stage: dict) -> bool:
    end = parse_iso(stage.get("end_time", ""))
    if not end:
        return False
    return datetime.now(timezone.utc) > end

# ============================================
# إدارة رسائل التيليجرام
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

async def telegram_sender():
    while True:
        msg = await send_queue.get()
        try:
            telegram_api = f"https://api.telegram.org/bot{msg['bot_token']}"
            await asyncio.to_thread(
                requests.post,
                f"{telegram_api}/sendMessage",
                data={"chat_id": msg["chat_id"], "text": msg["text"], "parse_mode": "HTML"},
                timeout=10,
            )
        except Exception as e:
            log.error(f"خطأ إرسال تليجرام للبوت ({msg['bot_token'][:10]}...): {e}")
        send_queue.task_done()
        await asyncio.sleep(0.1)

# ============================================
# رسائل البوت
# ============================================

def build_startup_message() -> str:
    now = datetime.now(LOCAL_TZ).strftime("%Y-%m-%d %H:%M:%S")
    return (
        f"🚀 <b>تم تشغيل نظام الشراء التلقائي - Ink</b>\n\n"
        f"📅 الوقت: {now}\n"
        f"💰 عدد المحافظ: {len(WALLETS_DATA)}\n"
        f"🔗 الشبكة: Ink\n"
        f"🆓 الوضع: المينتات المجانية فقط\n"
        f"⛽ حد الغاز: ${MAX_GAS_FEE_USD}\n"
        f"✅ جاهز لرصد المينتات المجانية..."
    )

def build_purchase_success_message(detail: dict, result: dict, chain_key: str) -> str:
    name = detail.get("collection_name") or detail.get("collection_slug") or "Unknown"
    url = detail.get("opensea_url", f"https://opensea.io/collection/{detail.get('collection_slug', '')}")
    chain_label = CHAIN_CONFIGS[chain_key]["chain_label"]
    w_short = result['wallet'][:6] + "..." + result['wallet'][-4:]
    tx_short = result['tx_hash'][:10]
    
    return (
        f"✅ <b>تم الشراء بنجاح!</b> 🎉\n\n"
        f"🟣 الشبكة: {chain_label}\n"
        f"💰 المحفظة: <code>{w_short}</code>\n"
        f"🖼️ المجموعة: <b>{name}</b>\n"
        f"🔢 الكمية: {result['quantity']}\n"
        f"⛽ رسوم الغاز: ${result['gas_fee_usd']:.4f}\n"
        f"🔗 المعاملة: <code>{tx_short}...</code>\n"
        f"🌐 <a href='{url}'>عرض على OpenSea</a>"
    )

def build_detection_message(detail: dict) -> str:
    name = detail.get("collection_name") or detail.get("collection_slug") or "Unknown"
    url = detail.get("opensea_url", f"https://opensea.io/collection/{detail.get('collection_slug', '')}")
    return (
        f"🔍 <b>تم اكتشاف مينت مجاني!</b>\n\n"
        f"🖼️ <b>{name}</b>\n"
        f"🌐 <a href='{url}'>عرض على OpenSea</a>\n\n"
        f"⏳ جاري الشراء لجميع المحافظ..."
    )

# ============================================
# وظائف الشراء
# ============================================

async def purchase_task_for_wallet(
    item, slug, contract_address, price_wei, max_per_wallet, remaining, eth_price_usd
):
    global total_purchases, total_gas_fee
    
    wallet_addr = item["wallet"]
    pk = item["private_key"]
    bot_token = item["bot_token"]
    chat_id = item["chat_id"]

    lock = get_wallet_lock(wallet_addr)
    async with lock:
        if wallet_addr in successful_mints.get(slug, set()):
            return {"success": False, "wallet": wallet_addr, "reason": "already_bought"}

        res = await asyncio.to_thread(
            attempt_purchase_single_wallet,
            W3_INSTANCE, pk, wallet_addr,
            contract_address, price_wei, max_per_wallet, remaining,
            eth_price_usd, MAX_GAS_FEE_USD,
        )

        if res.get("success"):
            if slug not in successful_mints:
                successful_mints[slug] = set()
            successful_mints[slug].add(wallet_addr)
            
            total_purchases += 1
            total_gas_fee += res.get("gas_fee_usd", 0)
            
            msg = build_purchase_success_message(item.get("current_detail", {}), res, "ink")
            enqueue_message(bot_token, chat_id, msg)
            
            log.info(f"✅ شراء ناجح: {wallet_addr[:8]}... كمية: {res['quantity']} | غاز: ${res['gas_fee_usd']:.4f}")

        return res


async def try_buy_now_multi_wallet(slug: str, detail: dict) -> list[dict] | None:
    stage = detail.get("active_stage")
    if not stage:
        return None

    max_supply = int(detail.get("max_supply") or 0)
    total_supply = int(detail.get("total_supply") or 0)
    remaining = max_supply - total_supply
    if remaining <= 0:
        return [{"success": False, "reason": "sold_out"}]

    contract_address = detail.get("contract_address")
    if not contract_address:
        return [{"success": False, "reason": "no_contract_address"}]

    eth_price_usd = get_eth_price_usd()

    # جلب السعر من العقد
    onchain_price = await asyncio.to_thread(get_onchain_public_price_wei, W3_INSTANCE, contract_address)
    price_wei = onchain_price if onchain_price is not None else int(stage.get("price", "0"))

    # التحقق من المجانية
    if not is_free_mint(price_wei, eth_price_usd):
        log.info(f"⏭️ '{slug}': ليس مجانياً - تخطي")
        return None

    # جلب الحد الأقصى من العقد
    max_per_wallet = await asyncio.to_thread(get_max_per_wallet, W3_INSTANCE, contract_address)
    if max_per_wallet is None or max_per_wallet <= 0:
        max_per_wallet = int(stage.get("max_total_mintable_by_wallet") or 5)

    already_bought_wallets = successful_mints.get(slug, set())
    pending_items = [item for item in WALLETS_DATA if item["wallet"] not in already_bought_wallets]

    if not pending_items:
        return [{"success": False, "reason": "all_wallets_completed"}]

    log.info(f"🔄 شراء {slug} لـ {len(pending_items)} محافظ")

    for item in pending_items:
        item["current_detail"] = detail

    tasks = [
        purchase_task_for_wallet(
            item, slug, contract_address,
            price_wei, max_per_wallet, remaining, eth_price_usd
        )
        for item in pending_items
    ]

    results = await asyncio.gather(*tasks)
    
    success_count = sum(1 for r in results if r and r.get("success"))
    log.info(f"📊 {slug}: نجح {success_count}/{len(pending_items)}")
    
    return list(results)


# ============================================
# تقييم المينتات وإدارة المراقبة
# ============================================

async def evaluate_new_mint(slug: str):
    global detected_mints
    
    if (
        len(successful_mints.get(slug, set())) >= len(WALLETS_DATA)
        or slug in watchlist
        or slug in in_flight
        or is_in_cooldown(slug)
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

        if not is_free_mint(price_wei, eth_price_usd):
            price_usd = (price_wei / 1e18) * eth_price_usd
            log.info(f"⏭️ '{slug}': ليس مجانياً (${price_usd:.6f})")
            mark_rejected(slug)
            return

        # ✅ اكتشاف مينت مجاني
        detected_mints += 1
        log.info(f"✅ '{slug}': مينت مجاني نشط — بدء الشراء!")

        # إرسال إشعار اكتشاف
        msg = build_detection_message(detail)
        broadcast_message(msg)

        results = await try_buy_now_multi_wallet(slug, detail)

        if results is None:
            watchlist[slug] = {"detail": detail}
            return

        if len(successful_mints.get(slug, set())) < len(WALLETS_DATA):
            watchlist[slug] = {"detail": detail}

    except Exception as e:
        log.error(f"خطأ بتقييم '{slug}': {e}")
    finally:
        in_flight.discard(slug)


async def watch_loop():
    last_status_time = time.time()
    
    while True:
        await asyncio.sleep(WATCH_POLL_INTERVAL_SECONDS)
        
        # عرض حالة كل 5 دقائق
        if time.time() - last_status_time >= 300:
            uptime = int(time.time() - start_time)
            hours = uptime // 3600
            minutes = (uptime % 3600) // 60
            
            log.info("=" * 50)
            log.info(f"📊 تقرير الحالة - {hours}h {minutes}m")
            log.info(f"💰 المحافظ: {len(WALLETS_DATA)}")
            log.info(f"✅ مشتريات ناجحة: {total_purchases}")
            log.info(f"⛽ إجمالي رسوم الغاز: ${total_gas_fee:.4f}")
            log.info(f"🔍 مينتات مكتشفة: {detected_mints}")
            log.info(f"👀 تحت المراقبة: {len(watchlist)}")
            log.info("=" * 50)
            last_status_time = time.time()
        
        if not watchlist:
            continue

        for slug in list(watchlist.keys()):
            if slug in in_flight or len(successful_mints.get(slug, set())) >= len(WALLETS_DATA):
                watchlist.pop(slug, None)
                continue

            entry = watchlist.get(slug)
            if not entry:
                continue

            in_flight.add(slug)
            try:
                found, fresh_detail = await asyncio.to_thread(fetch_drop_detail, slug)

                if not found or not fresh_detail or not fresh_detail.get("is_minting"):
                    watchlist.pop(slug, None)
                    continue

                stage = fresh_detail.get("active_stage")
                if not stage or (stage_has_ended(stage) and not fresh_detail.get("next_stage")):
                    watchlist.pop(slug, None)
                    continue

                results = await try_buy_now_multi_wallet(slug, fresh_detail)

                if results is None:
                    watchlist[slug] = {"detail": fresh_detail}
                    continue

                if len(successful_mints.get(slug, set())) >= len(WALLETS_DATA):
                    watchlist.pop(slug, None)
                else:
                    watchlist[slug] = {"detail": fresh_detail}

            except Exception as e:
                log.error(f"خطأ بدورة مراقبة '{slug}': {e}")
            finally:
                in_flight.discard(slug)


async def listen_opensea():
    msg_ref = 0
    processed_slugs: dict[str, float] = {}
    MIN_INTERVAL = 3
    
    while True:
        try:
            log.info("🔄 محاولة الاتصال بـ OpenSea Stream...")
            async with websockets.connect(STREAM_URL, ping_interval=None, open_timeout=15) as ws:
                log.info(f"✅ متصل بـ OpenSea Stream — يراقب {len(WALLETS_DATA)} محافظ على Ink.")
                join_ref = str(msg_ref)
                await ws.send(json.dumps([join_ref, join_ref, "collection:*", "phx_join", {}]))
                msg_ref += 1
                last_heartbeat = time.time()

                while True:
                    if time.time() - last_heartbeat > HEARTBEAT_INTERVAL:
                        hb_ref = str(msg_ref)
                        await ws.send(json.dumps([None, hb_ref, "phoenix", "heartbeat", {}]))
                        msg_ref += 1
                        last_heartbeat = time.time()

                    try:
                        raw = await asyncio.wait_for(ws.recv(), timeout=RECV_TIMEOUT)
                    except asyncio.TimeoutError:
                        continue

                    try:
                        parsed = json.loads(raw)
                    except json.JSONDecodeError:
                        continue

                    if isinstance(parsed, list) and len(parsed) == 5:
                        _jref, _ref, _topic, event_name, payload_wrapper = parsed
                    else:
                        continue

                    if event_name != "item_transferred":
                        continue

                    payload = (payload_wrapper or {}).get("payload") or {}
                    item = payload.get("item", {}) or {}
                    stream_chain_name = (item.get("chain", {}) or {}).get("name", "")

                    # ✅ قبول فقط Ink
                    chain_key = STREAM_NAME_TO_CHAIN_KEY.get(stream_chain_name)
                    if chain_key != "ink":
                        continue

                    from_address = ((payload.get("from_account") or {}).get("address", "") or "").lower()
                    if from_address != ZERO_ADDRESS:
                        continue

                    slug = (payload.get("collection", {}) or {}).get("slug", "")
                    if not slug:
                        continue

                    # ✅ منع التكرار
                    now = time.time()
                    if slug in processed_slugs and now - processed_slugs[slug] < MIN_INTERVAL:
                        continue
                    processed_slugs[slug] = now

                    log.info(f"📩 استقبال حدث لـ {slug}")
                    asyncio.create_task(evaluate_new_mint(slug))

        except (websockets.ConnectionClosed, OSError, asyncio.TimeoutError) as e:
            log.warning(f"⚠️ انقطع الاتصال ({e}). إعادة الاتصال...")
            await asyncio.sleep(3)
        except Exception as e:
            log.error(f"❌ خطأ غير متوقع: {e}")
            await asyncio.sleep(5)


async def run():
    if not BOT_ENABLED:
        log.warning("🔴 BOT_ENABLED=false")
        broadcast_message("🔴 البوت شغّال لكن بوضع الإيقاف (BOT_ENABLED=false).")
        await telegram_sender()
        return

    log.info("=" * 50)
    log.info("🚀 بدء تشغيل نظام الشراء التلقائي - Ink")
    log.info(f"💰 عدد المحافظ: {len(WALLETS_DATA)}")
    log.info(f"🔗 RPC: {INK_RPC_URL}")
    log.info(f"🆓 الوضع: المينتات المجانية فقط")
    log.info("=" * 50)

    broadcast_message(build_startup_message())
    await asyncio.gather(listen_opensea(), watch_loop(), telegram_sender())


def main():
    backoff = 2
    while True:
        try:
            asyncio.run(run())
        except KeyboardInterrupt:
            log.info("🛑 تم الإيقاف يدويًا.")
            break
        except Exception as e:
            log.critical(f"💥 توقف غير متوقع: {e}.")
            time.sleep(backoff)
            backoff = min(backoff * 2, 30)
            continue
        else:
            break


if __name__ == "__main__":
    main()
