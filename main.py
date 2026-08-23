"""
النظام الكامل — 10 محافظ، لكل محفظة بوت تيليجرام خاص بها
شبكة Ink - المينتات المجانية فقط
"""

import asyncio
import json
import logging
import os
import time
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Any

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

# رابط RPC الخاص بـ Ink
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

# تكوين شبكة Ink فقط
CHAIN_CONFIGS = {
    "ink": {
        "stream_chain_name": "ink",
        "rpc_url": INK_RPC_URL,
        "max_gas_fee_usd": 0.05,
        "chain_label": "Ink",
        "explorer_url": "https://explorer.inkonchain.com",
    },
}

W3_INSTANCE = get_web3(INK_RPC_URL)
STREAM_NAME_TO_CHAIN_KEY = {cfg["stream_chain_name"]: key for key, cfg in CHAIN_CONFIGS.items()}

# ============================================
# إحصائيات البوت
# ============================================
class BotStats:
    def __init__(self):
        self.start_time = time.time()
        self.successful_purchases = 0
        self.attempted_purchases = 0
        self.failed_purchases = 0
        self.total_gas_fee_usd = 0.0
        self.api_requests = 0
        self.telegram_messages = 0
        self.errors = 0
        self.detected_mints = 0
        self.purchases_by_wallet = {}  # wallet -> count
        self.purchases_by_collection = {}  # slug -> count
        
    def add_success(self, wallet: str, gas_fee: float, collection: str = ""):
        self.successful_purchases += 1
        self.total_gas_fee_usd += gas_fee
        self.purchases_by_wallet[wallet] = self.purchases_by_wallet.get(wallet, 0) + 1
        if collection:
            self.purchases_by_collection[collection] = self.purchases_by_collection.get(collection, 0) + 1
    
    def add_attempt(self):
        self.attempted_purchases += 1
    
    def add_failure(self):
        self.failed_purchases += 1
    
    def add_api_request(self):
        self.api_requests += 1
    
    def add_telegram_message(self):
        self.telegram_messages += 1
    
    def add_error(self):
        self.errors += 1
    
    def add_detected_mint(self):
        self.detected_mints += 1
    
    def get_uptime(self) -> str:
        elapsed = time.time() - self.start_time
        hours = int(elapsed // 3600)
        minutes = int((elapsed % 3600) // 60)
        seconds = int(elapsed % 60)
        return f"{hours}h {minutes}m {seconds}s"
    
    def get_success_rate(self) -> float:
        if self.attempted_purchases == 0:
            return 0.0
        return (self.successful_purchases / self.attempted_purchases) * 100

stats = BotStats()

# تتبع المحافظ التي اشترت بنجاح
successful_mints: dict[str, set[str]] = {}
watchlist: dict[str, dict] = {}
in_flight: set[str] = set()

# تبريد مؤقت للمجموعات التي رُفضت
REJECTION_COOLDOWN_SECONDS = 120
rejected_cooldown: dict[str, float] = {}

# ============================================
# وظائف مساعدة
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
        stats.add_api_request()
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
        stats.add_api_request()
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

def is_free_or_negligible(price_wei: int, eth_price_usd: float) -> bool:
    price_usd = (price_wei / 1e18) * eth_price_usd
    return price_usd < FREE_PRICE_THRESHOLD_USD

# ============================================
# رسائل البوت المحسنة
# ============================================

send_queue: "asyncio.Queue[dict]" = asyncio.Queue()

def enqueue_message(bot_token: str, chat_id: str, text: str):
    send_queue.put_nowait({
        "bot_token": bot_token,
        "chat_id": chat_id,
        "text": text
    })
    stats.add_telegram_message()

def broadcast_message(text: str):
    for w in WALLETS_DATA:
        enqueue_message(w["bot_token"], w["chat_id"], text)

# ============================================
# رسائل البوت المحسنة (مثل الصورة)
# ============================================

def build_startup_message() -> str:
    """رسالة بدء التشغيل"""
    now = datetime.now(LOCAL_TZ).strftime("%Y-%m-%d %H:%M:%S")
    return (
        f"🚀 <b>تم تشغيل نظام الشراء التلقائي</b>\n\n"
        f"📅 <b>التاريخ:</b> {now}\n"
        f"💰 <b>عدد المحافظ:</b> {len(WALLETS_DATA)}\n"
        f"🔗 <b>الشبكة:</b> Ink\n"
        f"🆓 <b>الوضع:</b> المينتات المجانية فقط\n"
        f"⛽ <b>حد رسوم الغاز:</b> $0.05\n"
        f"📊 <b>الحد الأدنى للشراء:</b> 10\n"
        f"\n✅ جاهز لرصد المينتات المجانية..."
    )

def build_purchase_success_message(detail: dict, result: dict, chain_key: str) -> str:
    """رسالة شراء ناجحة - مثل الصورة"""
    name = detail.get("collection_name") or detail.get("collection_slug") or "Unknown"
    url = detail.get("opensea_url", f"https://opensea.io/collection/{detail.get('collection_slug', '')}")
    chain_label = CHAIN_CONFIGS[chain_key]["chain_label"]
    w_short = result['wallet'][:6] + "..." + result['wallet'][-4:]
    tx_short = result['tx_hash'][:10]
    
    # حساب سعر المينت بالدولار
    price_usd = 0.0  # مجاني
    
    return (
        f"✅ <b>تم الشراء بنجاح!</b> 🎉\n\n"
        f"🔗 <b>الشبكة:</b> {chain_label}\n"
        f"💰 <b>المحفظة:</b> <code>{w_short}</code>\n"
        f"🖼️ <b>المجموعة:</b> {name}\n"
        f"🔢 <b>الكمية:</b> {result['quantity']}\n"
        f"⛽ <b>رسوم الغاز:</b> ${result['gas_fee_usd']:.4f}\n"
        f"🔗 <b>المعاملة:</b> <code>{tx_short}...</code>\n"
        f"🌐 <a href='{url}'>عرض على OpenSea</a>"
    )

def build_status_report() -> str:
    """تقرير حالة البوت - مثل الصورة"""
    uptime = stats.get_uptime()
    success_rate = stats.get_success_rate()
    
    # ترتيب المحافظ حسب عدد المشتريات
    top_wallets = sorted(stats.purchases_by_wallet.items(), key=lambda x: x[1], reverse=True)[:5]
    wallets_text = "\n".join([f"  • <code>{w[:6]}...{w[-4:]}</code>: {c}" for w, c in top_wallets]) if top_wallets else "  • لا توجد مشتريات بعد"
    
    # ترتيب المجموعات حسب عدد المشتريات
    top_collections = sorted(stats.purchases_by_collection.items(), key=lambda x: x[1], reverse=True)[:5]
    collections_text = "\n".join([f"  • {c}: {count}" for c, count in top_collections]) if top_collections else "  • لا توجد مجموعات بعد"
    
    return (
        f"📊 <b>تقرير حالة البوت</b>\n\n"
        f"⏱️ <b>وقت التشغيل:</b> {uptime}\n"
        f"💰 <b>عدد المحافظ:</b> {len(WALLETS_DATA)}\n\n"
        f"📈 <b>الإحصائيات:</b>\n"
        f"  • ✅ عمليات شراء ناجحة: {stats.successful_purchases}\n"
        f"  • 🔄 محاولات الشراء: {stats.attempted_purchases}\n"
        f"  • 📊 معدل النجاح: {success_rate:.1f}%\n"
        f"  • 🔍 مينتات مكتشفة: {stats.detected_mints}\n"
        f"  • ❌ الأخطاء: {stats.errors}\n\n"
        f"⛽ <b>إجمالي رسوم الغاز:</b> ${stats.total_gas_fee_usd:.4f}\n"
        f"📡 <b>طلبات API:</b> {stats.api_requests:,}\n"
        f"💬 <b>رسائل تليجرام:</b> {stats.telegram_messages}\n\n"
        f"🏆 <b>أكثر المحافظ نشاطاً:</b>\n{wallets_text}\n\n"
        f"🎯 <b>أكثر المجموعات شراءً:</b>\n{collections_text}"
    )

def build_purchase_failure_message(slug: str, reason: str, details: dict = None) -> str:
    """رسالة فشل الشراء"""
    name = details.get("collection_name") or details.get("collection_slug") or slug if details else slug
    
    reason_map = {
        "not_free_mint": "❌ المينت ليس مجانياً",
        "balance_too_low": "💰 رصيد غير كافٍ",
        "gas_too_high": "⛽ رسوم الغاز مرتفعة جداً",
        "insufficient_funds": "💰 رصيد غير كافٍ لتغطية الغاز",
        "sold_out": "❌ نفدت الكمية",
        "no_fee_recipient": "❌ لا يوجد مستلم رسوم",
        "contract_error": "⚠️ خطأ في العقد الذكي",
        "tx_error": "⚠️ خطأ في المعاملة",
        "already_bought": "✅ تم الشراء مسبقاً",
        "all_wallets_completed": "✅ جميع المحافظ اكتملت",
    }
    
    reason_text = reason_map.get(reason, f"⚠️ {reason}")
    
    return (
        f"❌ <b>فشل الشراء</b>\n\n"
        f"🖼️ <b>المجموعة:</b> {name}\n"
        f"📝 <b>السبب:</b> {reason_text}\n"
        f"🔗 <a href='https://opensea.io/collection/{slug}'>عرض على OpenSea</a>"
    )

# ============================================
# وظائف الشراء
# ============================================

async def telegram_sender():
    while True:
        msg = await send_queue.get()
        try:
            telegram_api = f"https://api.telegram.org/bot{msg['bot_token']}"
            await asyncio.to_thread(
                requests.post,
                f"{telegram_api}/sendMessage",
                data={
                    "chat_id": msg["chat_id"], 
                    "text": msg["text"], 
                    "parse_mode": "HTML",
                    "disable_web_page_preview": False,
                },
                timeout=10,
            )
        except Exception as e:
            log.error(f"خطأ إرسال تليجرام للبوت ({msg['bot_token'][:10]}...): {e}")
            stats.add_error()
        send_queue.task_done()
        await asyncio.sleep(0.1)

async def purchase_task_for_wallet(
    item, slug, contract_address, price_wei, max_per_wallet, remaining, eth_price_usd, max_gas_fee_usd
):
    wallet_addr = item["wallet"]
    pk = item["private_key"]
    bot_token = item["bot_token"]
    chat_id = item["chat_id"]

    lock = get_wallet_lock(wallet_addr)
    async with lock:
        if wallet_addr in successful_mints.get(slug, set()):
            return {"success": False, "wallet": wallet_addr, "reason": "already_bought"}

        stats.add_attempt()
        
        res = await asyncio.to_thread(
            attempt_purchase_single_wallet,
            W3_INSTANCE, pk, wallet_addr,
            contract_address, price_wei, max_per_wallet, remaining,
            eth_price_usd, max_gas_fee_usd,
        )

        if res.get("success"):
            if slug not in successful_mints:
                successful_mints[slug] = set()
            successful_mints[slug].add(wallet_addr)
            
            # تحديث الإحصائيات
            collection_name = item.get("current_detail", {}).get("collection_name") or slug
            stats.add_success(wallet_addr, res.get("gas_fee_usd", 0), collection_name)
            
            # إرسال رسالة نجاح
            msg = build_purchase_success_message(item.get("current_detail", {}), res, "ink")
            enqueue_message(bot_token, chat_id, msg)
        else:
            stats.add_failure()
            # إرسال رسالة فشل للمحاولات الفاشلة (باستثناء الأسباب المتوقعة)
            reason = res.get("reason", "unknown")
            if reason not in ["already_bought", "all_wallets_completed", "not_free_mint"]:
                fail_msg = build_purchase_failure_message(
                    slug, reason, item.get("current_detail", {})
                )
                enqueue_message(bot_token, chat_id, fail_msg)

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

    onchain_price = await asyncio.to_thread(get_onchain_public_price_wei, W3_INSTANCE, contract_address)
    price_wei = onchain_price if onchain_price is not None else int(stage.get("price", "0"))

    if not is_free_or_negligible(price_wei, eth_price_usd):
        log.info(f"⏭️ '{slug}': ليس مجانياً - تم التخطي")
        return None

    max_per_wallet_raw = stage.get("max_total_mintable_by_wallet") or stage.get("max_per_wallet")
    max_per_wallet = int(max_per_wallet_raw) if max_per_wallet_raw is not None else None
    max_gas_fee_usd = CHAIN_CONFIGS["ink"]["max_gas_fee_usd"]

    already_bought_wallets = successful_mints.get(slug, set())
    pending_items = [item for item in WALLETS_DATA if item["wallet"] not in already_bought_wallets]

    if not pending_items:
        return [{"success": False, "reason": "all_wallets_completed"}]

    for item in pending_items:
        item["current_detail"] = detail

    tasks = [
        purchase_task_for_wallet(
            item, slug, contract_address,
            price_wei, max_per_wallet, remaining, eth_price_usd, max_gas_fee_usd
        )
        for item in pending_items
    ]

    results = await asyncio.gather(*tasks)
    return list(results)

# ============================================
# المراقبة والاستماع
# ============================================

async def evaluate_new_mint(slug: str):
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
        eth_price_usd = get_eth_price_usd()
        
        if contract_address:
            onchain_price = await asyncio.to_thread(get_onchain_public_price_wei, W3_INSTANCE, contract_address)
            price_wei = onchain_price if onchain_price is not None else int(stage.get("price", "0"))
            
            if not is_free_or_negligible(price_wei, eth_price_usd):
                log.info(f"⏭️ '{slug}': ليس مجانياً - تم التخطي")
                mark_rejected(slug)
                return

        stats.add_detected_mint()
        log.info(f"✅ '{slug}': مينت مجاني نشط اليوم — المتابعة للشراء.")

        # إرسال إشعار باكتشاف المينت
        name = detail.get("collection_name") or slug
        for w in WALLETS_DATA:
            enqueue_message(
                w["bot_token"], 
                w["chat_id"],
                f"🔍 <b>تم اكتشاف مينت مجاني!</b>\n\n"
                f"🖼️ <b>المجموعة:</b> {name}\n"
                f"🔗 <a href='https://opensea.io/collection/{slug}'>عرض على OpenSea</a>\n\n"
                f"⏳ جاري الشراء لجميع المحافظ..."
            )

        results = await try_buy_now_multi_wallet(slug, detail)

        if results is None:
            watchlist[slug] = {"detail": detail}
            return

        if len(successful_mints.get(slug, set())) < len(WALLETS_DATA):
            watchlist[slug] = {"detail": detail}

    except Exception as e:
        log.error(f"خطأ بتقييم '{slug}': {e}")
        stats.add_error()
    finally:
        in_flight.discard(slug)

async def watch_loop():
    """حلقة مراقبة المينتات النشطة"""
    last_status_time = time.time()
    
    while True:
        await asyncio.sleep(WATCH_POLL_INTERVAL_SECONDS)
        
        # إرسال تقرير الحالة كل ساعة
        if time.time() - last_status_time >= 3600:  # كل ساعة
            broadcast_message(build_status_report())
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

                contract_address = fresh_detail.get("contract_address")
                eth_price_usd = get_eth_price_usd()
                
                if contract_address:
                    onchain_price = await asyncio.to_thread(get_onchain_public_price_wei, W3_INSTANCE, contract_address)
                    price_wei = onchain_price if onchain_price is not None else int(stage.get("price", "0"))
                    
                    if not is_free_or_negligible(price_wei, eth_price_usd):
                        log.info(f"⏭️ '{slug}': تغير السعر إلى مدفوع - إزالة من المراقبة")
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
                stats.add_error()
            finally:
                in_flight.discard(slug)

async def listen_opensea():
    """الاستماع لتدفق OpenSea"""
    msg_ref = 0
    while True:
        try:
            async with websockets.connect(STREAM_URL, ping_interval=None, open_timeout=15) as ws:
                log.info(f"✅ متصل بـ OpenSea Stream — يراقب المينتات المجانية على Ink لـ {len(WALLETS_DATA)} محافظ.")
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

                    chain_key = STREAM_NAME_TO_CHAIN_KEY.get(stream_chain_name)
                    if chain_key != "ink":
                        continue

                    from_address = ((payload.get("from_account") or {}).get("address", "") or "").lower()
                    if from_address != ZERO_ADDRESS:
                        continue

                    slug = (payload.get("collection", {}) or {}).get("slug", "")
                    if not slug:
                        continue

                    asyncio.create_task(evaluate_new_mint(slug))

        except (websockets.ConnectionClosed, OSError, asyncio.TimeoutError) as e:
            log.warning(f"⚠️ انقطع الاتصال ({e}). إعادة الاتصال...")
            await asyncio.sleep(3)
        except Exception as e:
            log.error(f"❌ خطأ غير متوقع: {e}.")
            stats.add_error()
            await asyncio.sleep(5)

async def run():
    if not BOT_ENABLED:
        log.warning("🔴 BOT_ENABLED=false")
        broadcast_message("🔴 البوت شغّال لكن بوضع الإيقاف (BOT_ENABLED=false).")
        await telegram_sender()
        return

    # إرسال رسالة البدء
    broadcast_message(build_startup_message())
    
    # إرسال تقرير الحالة الأولي بعد 10 ثوان
    await asyncio.sleep(10)
    broadcast_message(build_status_report())
    
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
