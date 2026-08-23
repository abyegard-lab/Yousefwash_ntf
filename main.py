# main.py - نسخة شاملة للاكتشاف (تلتقط كل المينتات)
"""
النظام الكامل — اكتشاف شامل لجميع المينتات على Ink
"""

import asyncio
import json
import logging
import os
import time
from datetime import datetime, timezone, timedelta
from collections import defaultdict

import requests
import websockets
from dotenv import load_dotenv

from buyer import (
    get_web3,
    attempt_purchase_single_wallet,
    get_onchain_public_price_wei,
    get_wallet_lock,
)
from twitter_checker import get_twitter_username_from_opensea

load_dotenv()

OPENSEA_API_KEY = os.environ["OPENSEA_API_KEY"]
BOT_ENABLED = os.environ.get("BOT_ENABLED", "false").lower() == "true"

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

# RPC مجاني لشبكة Ink
INK_RPC_URL = os.environ.get("INK_RPC_URL", "https://rpc-gel.inkonchain.com/")

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

# تكوين شبكة Ink فقط
CHAIN_CONFIGS = {
    "ink": {
        "stream_chain_name": "ink",
        "rpc_url": INK_RPC_URL,
        "max_gas_fee_usd": 0.50,
    },
}

W3_INSTANCES = {key: get_web3(cfg["rpc_url"]) for key, cfg in CHAIN_CONFIGS.items()}
STREAM_NAME_TO_CHAIN_KEY = {cfg["stream_chain_name"]: key for key, cfg in CHAIN_CONFIGS.items()}

# تتبع المحافظ التي اشترت بنجاح
successful_mints: dict[str, set[str]] = {}
watchlist: dict[str, dict] = {}
in_flight: set[str] = set()

REJECTION_COOLDOWN_SECONDS = 120
rejected_cooldown: dict[str, float] = {}

paid_mints_tracking: dict[str, dict] = {}
discovered_mints: set[str] = set()

# ==================== إعدادات السرعة ====================

DROP_CACHE_DURATION = 5
TWITTER_CACHE_DURATION = 600
MAX_PARALLEL_TASKS = 10

semaphore = asyncio.Semaphore(MAX_PARALLEL_TASKS)

# استخدام Session
_session = None

def get_session():
    global _session
    if _session is None:
        _session = requests.Session()
        _session.headers.update({"x-api-key": OPENSEA_API_KEY})
    return _session

# ==================== إحصائيات البوت ====================

bot_stats = {
    "start_time": time.time(),
    "mints_detected": 0,
    "mints_purchased": 0,
    "purchase_attempts": 0,
    "api_calls": 0,
    "conversions_detected": 0,
    "errors": 0,
    "total_gas_spent": 0.0,
    "mints_per_chain": defaultdict(int),
    "wallets_used": set(),
    "telegram_messages_sent": 0,
    "telegram_errors": 0
}

def get_uptime() -> str:
    seconds = int(time.time() - bot_stats["start_time"])
    hours = seconds // 3600
    minutes = (seconds % 3600) // 60
    seconds = seconds % 60
    return f"{hours}h {minutes}m {seconds}s"

# ==================== التخزين المؤقت ====================

_twitter_cache = {}
_drop_cache = {}

def get_cached_twitter(slug: str):
    if slug in _twitter_cache:
        username, timestamp = _twitter_cache[slug]
        if time.time() - timestamp < TWITTER_CACHE_DURATION:
            return username
    return None

def set_cached_twitter(slug: str, username):
    _twitter_cache[slug] = (username, time.time())

def get_cached_drop(slug: str):
    if slug in _drop_cache:
        detail, timestamp = _drop_cache[slug]
        if time.time() - timestamp < DROP_CACHE_DURATION:
            return detail
    return None

def set_cached_drop(slug: str, detail):
    _drop_cache[slug] = (detail, time.time())

# ==================== الوظائف الأساسية ====================

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

_eth_price_cache = {"value": None, "ts": 0}

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
        bot_stats["api_calls"] += 1
        return price
    except Exception as e:
        log.warning(f"[السعر] تعذر جلب سعر ETH: {e}")
        return _eth_price_cache["value"] or 3000.0

def fetch_drop_detail(slug: str):
    cached = get_cached_drop(slug)
    if cached is not None:
        return True, cached
    
    try:
        session = get_session()
        resp = session.get(
            f"{DROPS_API_BASE}/{slug}",
            timeout=5,
        )
        bot_stats["api_calls"] += 1
        if resp.status_code == 200:
            detail = resp.json()
            set_cached_drop(slug, detail)
            return True, detail
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

def stage_has_ended(stage: dict) -> bool:
    end = parse_iso(stage.get("end_time", ""))
    if not end:
        return False
    return datetime.now(timezone.utc) > end

def is_free_or_negligible(price_wei: int, eth_price_usd: float) -> bool:
    price_usd = (price_wei / 1e18) * eth_price_usd
    return price_usd < FREE_PRICE_THRESHOLD_USD

# ==================== تحليل المراحل ====================

def analyze_mint_stages(slug: str, detail: dict) -> dict:
    stage = detail.get("active_stage")
    price_wei = int(stage.get("price", "0")) if stage else 0
    eth_price_usd = get_eth_price_usd()
    is_free = is_free_or_negligible(price_wei, eth_price_usd)
    
    # التحقق من وجود مرحلة مجانية قادمة
    has_upcoming_free = False
    next_stages = detail.get("next_stages", [])
    for next_stage in next_stages:
        next_price = int(next_stage.get("price", "0"))
        if is_free_or_negligible(next_price, eth_price_usd):
            has_upcoming_free = True
            break
    
    result = {
        "slug": slug,
        "name": detail.get("collection_name") or slug,
        "is_free": is_free,
        "has_upcoming_free": has_upcoming_free,
        "status": "currently_free" if is_free else "paid_only",
        "is_worth_watching": is_free or has_upcoming_free,
        "price_wei": price_wei,
        "price_usd": (price_wei / 1e18) * eth_price_usd
    }
    
    return result

# ==================== رسائل التيليجرام ====================

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
            bot_token = msg.get("bot_token")
            chat_id = msg.get("chat_id")
            text = msg.get("text", "")
            
            if not bot_token or not chat_id:
                send_queue.task_done()
                continue
            
            telegram_api = f"https://api.telegram.org/bot{bot_token}"
            response = await asyncio.to_thread(
                requests.post,
                f"{telegram_api}/sendMessage",
                data={
                    "chat_id": chat_id, 
                    "text": text, 
                    "parse_mode": "HTML",
                    "disable_web_page_preview": True
                },
                timeout=15,
            )
            
            if response.status_code == 200:
                bot_stats["telegram_messages_sent"] += 1
            else:
                bot_stats["telegram_errors"] += 1
                log.error(f"❌ فشل إرسال تليجرام: {response.status_code}")
                
        except Exception as e:
            bot_stats["telegram_errors"] += 1
            log.error(f"❌ خطأ في إرسال تليجرام: {e}")
        finally:
            send_queue.task_done()
            await asyncio.sleep(0.1)

async def test_telegram():
    log.info("📤 جاري اختبار إرسال رسائل التيليجرام...")
    
    for i, w in enumerate(WALLETS_DATA):
        try:
            bot_token = w["bot_token"]
            chat_id = w["chat_id"]
            
            test_msg = f"🧪 <b>رسالة اختبار</b>\n\nالبوت #{i+1}\nالمحفظة: {w['wallet'][:8]}...\nالشبكة: Ink\nالوقت: {datetime.now().strftime('%H:%M:%S')}"
            
            telegram_api = f"https://api.telegram.org/bot{bot_token}"
            resp = requests.post(
                f"{telegram_api}/sendMessage",
                data={"chat_id": chat_id, "text": test_msg, "parse_mode": "HTML"},
                timeout=10,
            )
            
            if resp.status_code == 200:
                log.info(f"✅ تم إرسال رسالة اختبار للبوت #{i+1}")
                bot_stats["telegram_messages_sent"] += 1
            else:
                log.error(f"❌ فشل إرسال رسالة اختبار للبوت #{i+1}: {resp.status_code}")
                bot_stats["telegram_errors"] += 1
                
        except Exception as e:
            log.error(f"❌ خطأ في اختبار البوت #{i+1}: {e}")
            bot_stats["telegram_errors"] += 1

# ==================== رسائل الإشعارات ====================

def build_startup_message() -> str:
    wallet_count = len(WALLETS_DATA)
    return (
        f"🚀 <b>تم تشغيل البوت بنجاح!</b>\n\n"
        f"📊 عدد المحافظ: {wallet_count}\n"
        f"🔗 الشبكة: Ink\n"
        f"⚡ الوضع: اكتشاف شامل (كل الأحداث)\n"
        f"💰 المينتات: مجانية فقط\n"
        f"🔄 جارٍ المراقبة..."
    )

def build_purchase_message(detail: dict, result: dict, chain_key: str) -> str:
    name = detail.get("collection_name") or detail.get("collection_slug")
    url = detail.get("opensea_url", "")
    w_short = result['wallet'][:6] + "..." + result['wallet'][-4:]
    
    bot_stats["mints_purchased"] += 1
    bot_stats["total_gas_spent"] += result.get('gas_fee_usd', 0)
    bot_stats["mints_per_chain"][chain_key] += 1
    bot_stats["wallets_used"].add(result['wallet'])
    
    return (
        f"✅ <b>تم الشراء بنجاح!</b>\n\n"
        f"📦 المجموعة: <b>{name}</b>\n"
        f"🔗 الشبكة: Ink\n"
        f"👛 المحفظة: <code>{w_short}</code>\n"
        f"🔢 الكمية: {result['quantity']}\n"
        f"⛽ رسوم الغاز: ${result['gas_fee_usd']:.4f}\n"
        f"🔗 المعاملة: <code>{result['tx_hash'][:10]}...</code>\n"
        f"🌐 <a href='{url}'>عرض على OpenSea</a>"
    )

# ---------------------------------------------------------------------------
# الشراء
# ---------------------------------------------------------------------------

async def purchase_task_for_wallet(
    w3, item, slug, contract_address, price_wei, max_per_wallet, remaining, eth_price_usd, max_gas_fee_usd
):
    wallet_addr = item["wallet"]
    pk = item["private_key"]
    bot_token = item["bot_token"]
    chat_id = item["chat_id"]

    lock = get_wallet_lock(wallet_addr)
    async with lock:
        if wallet_addr in successful_mints.get(slug, set()):
            return {"success": False, "wallet": wallet_addr, "reason": "already_bought"}

        bot_stats["purchase_attempts"] += 1
        
        res = await asyncio.to_thread(
            attempt_purchase_single_wallet,
            w3, pk, wallet_addr,
            contract_address, price_wei, max_per_wallet, remaining,
            eth_price_usd, max_gas_fee_usd,
        )

        if res.get("success"):
            if slug not in successful_mints:
                successful_mints[slug] = set()
            successful_mints[slug].add(wallet_addr)
            
            msg = build_purchase_message(item.get("current_detail", {}), res, item.get("chain_key", ""))
            enqueue_message(bot_token, chat_id, msg)
            log.info(f"✅ شراء ناجح للمحفظة {wallet_addr[:8]}... - {slug}")
        else:
            log.warning(f"❌ فشل شراء للمحفظة {wallet_addr[:8]}... - {slug}: {res.get('reason', 'unknown')}")

        return res

async def try_buy_now_multi_wallet(slug: str, chain_key: str, detail: dict):
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

    w3 = W3_INSTANCES[chain_key]
    eth_price_usd = get_eth_price_usd()

    onchain_price = await asyncio.to_thread(get_onchain_public_price_wei, w3, contract_address)
    price_wei = onchain_price if onchain_price is not None else int(stage.get("price", "0"))

    # شرط المجانية فقط
    if not is_free_or_negligible(price_wei, eth_price_usd):
        log.info(f"💰 '{slug}' مدفوع - تجاهل (${(price_wei/1e18)*eth_price_usd:.4f})")
        return None

    max_per_wallet_raw = stage.get("max_total_mintable_by_wallet") or stage.get("max_per_wallet")
    max_per_wallet = int(max_per_wallet_raw) if max_per_wallet_raw is not None else None
    max_gas_fee_usd = CHAIN_CONFIGS[chain_key]["max_gas_fee_usd"]

    already_bought_wallets = successful_mints.get(slug, set())
    pending_items = [item for item in WALLETS_DATA if item["wallet"] not in already_bought_wallets]

    if not pending_items:
        return [{"success": False, "reason": "all_wallets_completed"}]

    for item in pending_items:
        item["current_detail"] = detail
        item["chain_key"] = chain_key

    tasks = [
        purchase_task_for_wallet(
            w3, item, slug, contract_address,
            price_wei, max_per_wallet, remaining, eth_price_usd, max_gas_fee_usd
        )
        for item in pending_items
    ]

    results = await asyncio.gather(*tasks)
    return list(results)

# ---------------------------------------------------------------------------
# تقييم المينتات - شامل
# ---------------------------------------------------------------------------

async def evaluate_new_mint_fast(slug: str, chain_key: str):
    """تقييم شامل للمينتات - يلتقط كل المينتات"""
    async with semaphore:
        if slug in successful_mints and len(successful_mints[slug]) >= len(WALLETS_DATA):
            return
        
        if slug in in_flight:
            return
        
        if is_in_cooldown(slug):
            return

        in_flight.add(slug)
        try:
            log.info(f"🔍 تقييم المينت: {slug}")
            
            found, detail = await asyncio.to_thread(fetch_drop_detail, slug)
            if not found or not detail:
                log.warning(f"❌ لا توجد تفاصيل لـ {slug}")
                in_flight.discard(slug)
                return

            if not detail.get("is_minting"):
                log.info(f"⏸️ {slug} ليس في حالة minting")
                in_flight.discard(slug)
                return

            stage = detail.get("active_stage")
            if not stage:
                log.warning(f"❌ لا توجد مرحلة نشطة لـ {slug}")
                in_flight.discard(slug)
                return

            # تحليل المراحل
            analysis = analyze_mint_stages(slug, detail)
            
            if slug not in discovered_mints:
                log.info(f"🆕 مينت جديد: {slug} [Ink] - {'مجاني' if analysis['is_free'] else 'مدفوع'}")
                discovered_mints.add(slug)

            bot_stats["mints_detected"] += 1

            # ✅ التحقق من وجود حساب X
            twitter_username = get_cached_twitter(slug)
            if twitter_username is None:
                try:
                    twitter_username = await asyncio.to_thread(
                        get_twitter_username_from_opensea, 
                        slug, 
                        OPENSEA_API_KEY
                    )
                    set_cached_twitter(slug, twitter_username)
                except Exception as e:
                    log.debug(f"خطأ في جلب تويتر لـ {slug}: {e}")
                    twitter_username = None
            
            # رفض إذا لم يكن لديه حساب X
            if twitter_username is None:
                log.info(f"❌ '{slug}' مرفوض - لا يوجد حساب X")
                mark_rejected(slug)
                in_flight.discard(slug)
                return

            log.info(f"✅ '{slug}' يوجد حساب X: @{twitter_username}")

            # ✅ إذا كان المينت مجاني → شراء فوري
            if analysis['is_free']:
                log.info(f"🟢 '{slug}' مجاني - جارٍ الشراء...")
                results = await try_buy_now_multi_wallet(slug, chain_key, detail)
                
                if results is None:
                    watchlist[slug] = {"chain_key": chain_key, "detail": detail}
                elif len(successful_mints.get(slug, set())) < len(WALLETS_DATA):
                    watchlist[slug] = {"chain_key": chain_key, "detail": detail}
            
            # ✅ إذا كان مدفوع ولكن سيصبح مجاني → تتبع
            elif analysis['has_upcoming_free']:
                log.info(f"⏳ '{slug}' سيكون مجانياً قريباً - جارٍ التتبع")
                paid_mints_tracking[slug] = {
                    "chain_key": chain_key,
                    "detail": detail,
                    "first_seen": time.time(),
                    "last_check": time.time(),
                    "check_count": 0,
                    "analysis": analysis
                }
            
            # ✅ مدفوع فقط → تجاهل
            else:
                log.info(f"💰 '{slug}' مدفوع فقط - تم تجاهله (${analysis['price_usd']:.4f})")
                mark_rejected(slug)

        except Exception as e:
            log.error(f"خطأ بتقييم '{slug}': {e}")
            bot_stats["errors"] += 1
        finally:
            in_flight.discard(slug)

# ---------------------------------------------------------------------------
# فحص المينتات المدفوعة
# ---------------------------------------------------------------------------

async def scan_paid_mints():
    while True:
        try:
            await asyncio.sleep(10)
            
            if not paid_mints_tracking:
                continue
            
            for slug, data in list(paid_mints_tracking.items()):
                data['check_count'] = data.get('check_count', 0) + 1
                data['last_check'] = time.time()
                
                if slug in successful_mints and len(successful_mints[slug]) >= len(WALLETS_DATA):
                    paid_mints_tracking.pop(slug, None)
                    continue
                
                found, fresh_detail = await asyncio.to_thread(fetch_drop_detail, slug)
                if not found or not fresh_detail or not fresh_detail.get("is_minting"):
                    paid_mints_tracking.pop(slug, None)
                    continue
                
                stage = fresh_detail.get("active_stage")
                if not stage:
                    continue
                
                chain_key = data.get("chain_key", "ink")
                w3 = W3_INSTANCES[chain_key]
                eth_price_usd = get_eth_price_usd()
                contract_address = fresh_detail.get("contract_address")
                
                if contract_address:
                    onchain_price = await asyncio.to_thread(get_onchain_public_price_wei, w3, contract_address)
                    price_wei = onchain_price if onchain_price is not None else int(stage.get("price", "0"))
                    
                    if is_free_or_negligible(price_wei, eth_price_usd):
                        wait_time = time.time() - data.get('first_seen', time.time())
                        log.info(f"🔄 '{slug}' أصبح مجانياً بعد {wait_time:.0f} ثانية!")
                        paid_mints_tracking.pop(slug, None)
                        asyncio.create_task(evaluate_new_mint_fast(slug, chain_key))
                        
        except Exception as e:
            log.error(f"خطأ في فحص المينتات المدفوعة: {e}")
            bot_stats["errors"] += 1
            await asyncio.sleep(5)

# ---------------------------------------------------------------------------
# watch_loop
# ---------------------------------------------------------------------------

async def watch_loop():
    while True:
        await asyncio.sleep(WATCH_POLL_INTERVAL_SECONDS)
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
                chain_key = entry["chain_key"]
                found, fresh_detail = await asyncio.to_thread(fetch_drop_detail, slug)

                if not found or not fresh_detail or not fresh_detail.get("is_minting"):
                    watchlist.pop(slug, None)
                    continue

                stage = fresh_detail.get("active_stage")
                if not stage or (stage_has_ended(stage) and not fresh_detail.get("next_stage")):
                    watchlist.pop(slug, None)
                    continue

                results = await try_buy_now_multi_wallet(slug, chain_key, fresh_detail)

                if results is None:
                    watchlist[slug] = {"chain_key": chain_key, "detail": fresh_detail}
                    continue

                if len(successful_mints.get(slug, set())) >= len(WALLETS_DATA):
                    watchlist.pop(slug, None)
                else:
                    watchlist[slug] = {"chain_key": chain_key, "detail": fresh_detail}

            except Exception as e:
                log.error(f"خطأ بدورة مراقبة '{slug}': {e}")
                bot_stats["errors"] += 1
            finally:
                in_flight.discard(slug)

# ---------------------------------------------------------------------------
# الاستماع الشامل إلى OpenSea
# ---------------------------------------------------------------------------

async def listen_opensea_fast():
    msg_ref = 0
    recent_mints = {}
    RECENT_WINDOW = 2
    
    while True:
        try:
            async with websockets.connect(STREAM_URL, ping_interval=None, open_timeout=15) as ws:
                log.info(f"🚀 متصل بـ OpenSea Stream — اكتشاف شامل لـ {len(WALLETS_DATA)} محافظ على Ink.")
                log.info(f"📋 يستمع لجميع الأحداث: transferred, listed, created, received, sold")
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

                    # ✅ قبول جميع أنواع الأحداث للاكتشاف الشامل
                    if event_name not in [
                        "item_transferred", 
                        "item_listed", 
                        "collection_created", 
                        "item_received", 
                        "item_sold",
                        "item_metadata_updated",
                        "collection_updated"
                    ]:
                        continue

                    payload = (payload_wrapper or {}).get("payload") or {}
                    item = payload.get("item", {}) or {}
                    stream_chain_name = (item.get("chain", {}) or {}).get("name", "")

                    chain_key = STREAM_NAME_TO_CHAIN_KEY.get(stream_chain_name)
                    if chain_key is None:
                        continue

                    # فقط Ink
                    if chain_key != "ink":
                        continue

                    slug = (payload.get("collection", {}) or {}).get("slug", "")
                    if not slug:
                        continue

                    # منع التكرار
                    now = time.time()
                    if slug in recent_mints:
                        if now - recent_mints[slug] < RECENT_WINDOW:
                            continue
                    recent_mints[slug] = now

                    # تنظيف الذاكرة
                    for s in list(recent_mints.keys()):
                        if now - recent_mints[s] > 60:
                            del recent_mints[s]

                    log.info(f"📨 حدث: {event_name} → {slug}")
                    asyncio.create_task(evaluate_new_mint_fast(slug, chain_key))

        except (websockets.ConnectionClosed, OSError, asyncio.TimeoutError) as e:
            log.warning(f"⚠️ انقطع الاتصال ({e}). إعادة الاتصال خلال 2 ثانية...")
            await asyncio.sleep(2)
        except Exception as e:
            log.error(f"❌ خطأ غير متوقع: {e}")
            bot_stats["errors"] += 1
            await asyncio.sleep(3)

# ==================== التشغيل الرئيسي ====================

async def run():
    if not BOT_ENABLED:
        log.warning("🔴 BOT_ENABLED=false")
        broadcast_message("🔴 البوت في وضع الإيقاف (BOT_ENABLED=false).")
        await telegram_sender()
        return

    broadcast_message(build_startup_message())
    log.info("🚀 تم تشغيل البوت بنجاح - اكتشاف شامل!")
    
    await asyncio.sleep(3)
    await test_telegram()
    
    await asyncio.gather(
        listen_opensea_fast(),
        scan_paid_mints(),
        watch_loop(),
        telegram_sender()
    )

def main():
    backoff = 2
    while True:
        try:
            asyncio.run(run())
        except KeyboardInterrupt:
            log.info("تم الإيقاف يدويًا.")
            break
        except Exception as e:
            log.critical(f"توقف غير متوقع: {e}.")
            bot_stats["errors"] += 1
            time.sleep(backoff)
            backoff = min(backoff * 2, 30)
            continue
        else:
            break

if __name__ == "__main__":
    main()
