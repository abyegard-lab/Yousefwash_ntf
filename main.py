"""
main.py - النظام الكامل مع إشعارات محددة في التيليجرام
"""

import asyncio
import json
import logging
import os
import time
from datetime import datetime, timezone, timedelta
from collections import defaultdict
from typing import Optional, Dict, Set

import requests
import websockets
from dotenv import load_dotenv

from buyer import (
    get_web3,
    attempt_purchase_single_wallet,
    get_onchain_public_price_wei,
    get_onchain_phase_info,
    get_wallet_lock,
    get_wallet_balance_usd,
    estimate_gas_fee_usd,
)

from twitter_checker import get_twitter_username_from_opensea

load_dotenv()

# ==================== الإعدادات ====================

OPENSEA_API_KEY = os.environ["OPENSEA_API_KEY"]
BOT_ENABLED = os.environ.get("BOT_ENABLED", "false").lower() == "true"

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

INK_RPC_URL = os.environ.get("INK_RPC_URL", "https://rpc-gel.inkonchain.com/")
STREAM_URL = f"wss://stream.openseabeta.com/socket/websocket?token={OPENSEA_API_KEY}&vsn=2.0.0"
DROPS_API_BASE = "https://api.opensea.io/api/v2/drops"

# ==================== إعدادات متقدمة ====================

FREE_PRICE_THRESHOLD_USD = 0.0001
SLEEP_CHECK_INTERVAL = 0.5
MAX_GAS_FEE_USD = 0.50
MIN_BALANCE_RESERVE_USD = 0.10

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("auto-buyer")

# ==================== تكوين الشبكات ====================

CHAIN_CONFIGS = {
    "ink": {
        "stream_chain_name": "ink",
        "rpc_url": INK_RPC_URL,
        "max_gas_fee_usd": MAX_GAS_FEE_USD,
        "chain_id": 57073,
    },
}

W3_INSTANCES = {key: get_web3(cfg["rpc_url"]) for key, cfg in CHAIN_CONFIGS.items()}
STREAM_NAME_TO_CHAIN_KEY = {cfg["stream_chain_name"]: key for key, cfg in CHAIN_CONFIGS.items()}

# ==================== تخزين البيانات ====================

class MintTracker:
    """تتبع شامل للمينتات"""
    
    def __init__(self, slug: str, detail: dict):
        self.slug = slug
        self.detail = detail
        self.name = detail.get("collection_name") or slug
        self.contract_address = detail.get("contract_address")
        self.chain_key = "ink"
        
        self.all_stages = []
        self.free_stages = []
        self.is_sleeping = False
        self.has_purchased = False
        self.purchased_wallets = set()
        self.first_seen = time.time()
        self.twitter_username = None
        
        self.onchain_info = None
        self._fetch_onchain_info()
        self._analyze_stages(detail)
    
    def _fetch_onchain_info(self):
        if not self.contract_address:
            return
        try:
            w3 = W3_INSTANCES[self.chain_key]
            self.onchain_info = get_onchain_phase_info(w3, self.contract_address)
        except Exception as e:
            log.debug(f"فشل جلب معلومات السلسلة لـ {self.name}: {e}")
    
    def _analyze_stages(self, detail: dict):
        eth_price_usd = get_eth_price_usd()
        self.all_stages = []
        self.free_stages = []
        
        current = detail.get("active_stage")
        if current:
            stage_info = self._parse_stage(current, "active", eth_price_usd)
            self.all_stages.append(stage_info)
            if stage_info["is_free"]:
                self.free_stages.append(stage_info)
        
        for stage in detail.get("next_stages", []):
            stage_info = self._parse_stage(stage, "upcoming", eth_price_usd)
            self.all_stages.append(stage_info)
            if stage_info["is_free"]:
                self.free_stages.append(stage_info)
    
    def _parse_stage(self, stage: dict, status: str, eth_price_usd: float) -> dict:
        price_wei = int(stage.get("price", "0"))
        price_usd = (price_wei / 1e18) * eth_price_usd
        is_free = price_usd < FREE_PRICE_THRESHOLD_USD
        
        return {
            "name": stage.get("type", "unknown"),
            "price_wei": price_wei,
            "price_usd": price_usd,
            "is_free": is_free,
            "status": status,
            "start_time": stage.get("start_time"),
            "end_time": stage.get("end_time"),
            "max_per_wallet": stage.get("max_per_wallet"),
        }
    
    def has_free_active_stage(self) -> bool:
        if self.onchain_info and self.onchain_info.get("is_active"):
            price_wei = self.onchain_info.get("mintPrice", 0)
            eth_price_usd = get_eth_price_usd()
            if price_wei == 0 or (price_wei / 1e18) * eth_price_usd < FREE_PRICE_THRESHOLD_USD:
                return True
        
        for stage in self.all_stages:
            if stage["is_free"] and stage["status"] == "active":
                return True
        return False
    
    def get_current_free_stage(self) -> Optional[dict]:
        if self.onchain_info and self.onchain_info.get("is_active"):
            price_wei = self.onchain_info.get("mintPrice", 0)
            eth_price_usd = get_eth_price_usd()
            if price_wei == 0 or (price_wei / 1e18) * eth_price_usd < FREE_PRICE_THRESHOLD_USD:
                return {
                    "name": "public",
                    "price_wei": price_wei,
                    "price_usd": (price_wei / 1e18) * eth_price_usd,
                    "is_free": True,
                    "status": "active",
                    "max_per_wallet": self.onchain_info.get("maxTotalMintableByWallet")
                }
        
        for stage in self.all_stages:
            if stage["is_free"] and stage["status"] == "active":
                return stage
        return None
    
    def go_to_sleep(self):
        self.is_sleeping = True
        log.info(f"💤 {self.name}: دخل في وضع النوم")
    
    def wake_up(self):
        self.is_sleeping = False
        log.info(f"⏰ {self.name}: استيقظ!")

# ==================== التخزين المؤقت ====================

_mint_trackers: Dict[str, MintTracker] = {}
_successful_mints: Dict[str, Set[str]] = {}
_in_flight: Set[str] = set()
_rejected_cooldown: Dict[str, float] = {}
_twitter_cache = {}
_drop_cache = {}

# ✅ إحصائيات البوت
bot_stats = {
    "start_time": time.time(),
    "mints_detected": 0,
    "mints_purchased": 0,
    "purchase_attempts": 0,
    "errors": 0,
    "wallets_used": set(),
}

# ==================== الوظائف الأساسية ====================

def get_session():
    session = requests.Session()
    session.headers.update({"x-api-key": OPENSEA_API_KEY})
    return session

def get_eth_price_usd() -> float:
    try:
        resp = requests.get(
            "https://api.coingecko.com/api/v3/simple/price?ids=ethereum&vs_currencies=usd",
            timeout=8,
        )
        return resp.json()["ethereum"]["usd"]
    except Exception:
        return 3000.0

def is_in_cooldown(slug: str) -> bool:
    ts = _rejected_cooldown.get(slug)
    if ts is None:
        return False
    if time.time() - ts >= 120:
        _rejected_cooldown.pop(slug, None)
        return False
    return True

def mark_rejected(slug: str):
    _rejected_cooldown[slug] = time.time()

def get_cached_twitter(slug: str):
    if slug in _twitter_cache:
        username, timestamp = _twitter_cache[slug]
        if time.time() - timestamp < 600:
            return username
    return None

def set_cached_twitter(slug: str, username):
    _twitter_cache[slug] = (username, time.time())

async def fetch_drop_detail_async(slug: str):
    return await asyncio.to_thread(fetch_drop_detail_sync, slug)

def fetch_drop_detail_sync(slug: str):
    try:
        session = get_session()
        resp = session.get(f"{DROPS_API_BASE}/{slug}", timeout=5)
        if resp.status_code == 200:
            return True, resp.json()
        return False, None
    except Exception as e:
        log.warning(f"[Drops API] خطأ: {e}")
        return None, None

def is_free_or_negligible(price_wei: int, eth_price_usd: float) -> bool:
    if price_wei == 0:
        return True
    price_usd = (price_wei / 1e18) * eth_price_usd
    return price_usd < FREE_PRICE_THRESHOLD_USD

# ==================== إشعارات التيليجرام ====================

send_queue = asyncio.Queue()

def enqueue_message(bot_token: str, chat_id: str, text: str):
    """إضافة رسالة إلى قائمة الانتظار"""
    send_queue.put_nowait({"bot_token": bot_token, "chat_id": chat_id, "text": text})

def broadcast_message(text: str):
    """إرسال رسالة إلى جميع المحافظ"""
    for w in WALLETS_DATA:
        enqueue_message(w["bot_token"], w["chat_id"], text)

async def telegram_sender():
    """معالج إرسال رسائل التيليجرام"""
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
            
            if response.status_code != 200:
                log.error(f"❌ فشل إرسال تليجرام: {response.status_code}")
                
        except Exception as e:
            log.error(f"❌ خطأ في إرسال تليجرام: {e}")
        finally:
            send_queue.task_done()
            await asyncio.sleep(0.1)

# ==================== رسائل التيليجرام المحددة ====================

def send_startup_message():
    """✅ إشعار بدء التشغيل"""
    wallet_count = len(WALLETS_DATA)
    uptime = get_uptime()
    
    msg = (
        f"🚀 <b>تم تشغيل البوت!</b>\n\n"
        f"📊 عدد المحافظ: <b>{wallet_count}</b>\n"
        f"🔗 الشبكة: <b>Ink</b>\n"
        f"⚡ الحالة: <b>جاهز</b>\n"
        f"⏰ وقت التشغيل: <b>{uptime}</b>\n"
        f"🔄 الوضع: <b>اكتشاف تلقائي</b>\n\n"
        f"💡 في انتظار المينتات المجانية..."
    )
    
    broadcast_message(msg)
    log.info("📤 تم إرسال إشعار بدء التشغيل")

def send_test_message():
    """✅ إشعار اختبار الاتصال"""
    wallet_count = len(WALLETS_DATA)
    current_time = datetime.now().strftime('%H:%M:%S')
    
    msg = (
        f"🧪 <b>اختبار الاتصال</b>\n\n"
        f"✅ البوت يعمل بشكل صحيح\n"
        f"📊 عدد المحافظ: <b>{wallet_count}</b>\n"
        f"🕐 الوقت: <b>{current_time}</b>\n"
        f"🔗 الشبكة: <b>Ink</b>\n\n"
        f"📡 في انتظار المينتات الجديدة..."
    )
    
    broadcast_message(msg)
    log.info("📤 تم إرسال إشعار الاختبار")

def send_purchase_message(slug: str, result: dict, tracker: MintTracker):
    """💰 إشعار الشراء الناجح"""
    name = tracker.name if tracker else slug
    wallet = result.get('wallet', 'unknown')
    wallet_short = wallet[:8] + "..." + wallet[-6:] if len(wallet) > 14 else wallet
    tx_hash = result.get('tx_hash', '')
    tx_short = tx_hash[:10] + "..." if tx_hash else 'غير متاح'
    quantity = result.get('quantity', 1)
    gas_fee = result.get('gas_fee_usd', 0)
    
    # ✅ إحصائيات إضافية
    total_purchased = len(_successful_mints.get(slug, set()))
    total_wallets = len(WALLETS_DATA)
    
    msg = (
        f"💰 <b>تم الشراء بنجاح!</b>\n\n"
        f"📦 المجموعة: <b>{name}</b>\n"
        f"👛 المحفظة: <code>{wallet_short}</code>\n"
        f"🔢 الكمية: <b>{quantity}</b>\n"
        f"⛽ رسوم الغاز: <b>${gas_fee:.4f}</b>\n"
        f"🔗 المعاملة: <code>{tx_short}</code>\n"
        f"📊 المحافظ المشترية: <b>{total_purchased}/{total_wallets}</b>\n"
        f"🔗 الشبكة: <b>Ink</b>\n"
        f"🕐 الوقت: <b>{datetime.now().strftime('%H:%M:%S')}</b>"
    )
    
    broadcast_message(msg)
    log.info(f"📤 تم إرسال إشعار الشراء لـ {slug}")

def send_error_message(slug: str, error: str):
    """⚠️ إشعار الخطأ (للحالات الهامة فقط)"""
    # ✅ فقط للأخطاء المهمة (نفاذ الكمية، مشكلة في الرصيد)
    important_errors = ["sold_out", "insufficient_funds", "gas_too_high"]
    
    # ✅ التحقق مما إذا كان الخطأ مهماً
    is_important = any(err in error.lower() for err in important_errors)
    
    if not is_important:
        return
    
    msg = (
        f"⚠️ <b>تنبيه</b>\n\n"
        f"📦 المجموعة: <b>{slug}</b>\n"
        f"❌ المشكلة: <code>{error}</code>\n"
        f"🕐 الوقت: <b>{datetime.now().strftime('%H:%M:%S')}</b>"
    )
    
    broadcast_message(msg)
    log.info(f"📤 تم إرسال إشعار الخطأ لـ {slug}")

def get_uptime() -> str:
    """الحصول على مدة التشغيل"""
    seconds = int(time.time() - bot_stats["start_time"])
    hours = seconds // 3600
    minutes = (seconds % 3600) // 60
    seconds = seconds % 60
    return f"{hours}h {minutes}m {seconds}s"

# ==================== نظام الشراء ====================

async def purchase_task_for_wallet(
    w3, item, slug, contract_address, price_wei, max_per_wallet, remaining, eth_price_usd, max_gas_fee_usd
):
    wallet_addr = item["wallet"]
    pk = item["private_key"]

    lock = get_wallet_lock(wallet_addr)
    async with lock:
        if wallet_addr in _successful_mints.get(slug, set()):
            return {"success": False, "wallet": wallet_addr, "reason": "already_bought"}

        log.info(f"🔫 محاولة شراء للمحفظة {wallet_addr[:8]}... - {slug}")
        
        res = await asyncio.to_thread(
            attempt_purchase_single_wallet,
            w3, pk, wallet_addr,
            contract_address, price_wei, max_per_wallet, remaining,
            eth_price_usd, max_gas_fee_usd,
        )

        if res.get("success"):
            if slug not in _successful_mints:
                _successful_mints[slug] = set()
            _successful_mints[slug].add(wallet_addr)
            
            bot_stats["mints_purchased"] += 1
            bot_stats["wallets_used"].add(wallet_addr)
            
            # ✅ إرسال إشعار الشراء
            tracker = _mint_trackers.get(slug)
            if tracker:
                send_purchase_message(slug, res, tracker)
            
            log.info(f"✅ شراء ناجح للمحفظة {wallet_addr[:8]}... - {slug}")
        else:
            reason = res.get('reason', 'unknown')
            log.warning(f"❌ فشل شراء للمحفظة {wallet_addr[:8]}... - {slug}: {reason}")
            
            # ✅ إرسال إشعار خطأ للمشاكل المهمة
            send_error_message(slug, reason)

        return res

async def try_buy_now_multi_wallet(slug: str, chain_key: str, detail: dict, price_wei: int = None):
    """محاولة الشراء من جميع المحافظ"""
    
    tracker = _mint_trackers.get(slug)
    if not tracker:
        return None
    
    free_stage = tracker.get_current_free_stage()
    if not free_stage:
        return None
    
    contract_address = detail.get("contract_address")
    if not contract_address:
        return [{"success": False, "reason": "no_contract_address"}]

    w3 = W3_INSTANCES[chain_key]
    eth_price_usd = get_eth_price_usd()
    
    if price_wei is None:
        price_wei = free_stage.get("price_wei", 0)
        if price_wei == 0:
            onchain_price = await asyncio.to_thread(get_onchain_public_price_wei, w3, contract_address)
            price_wei = onchain_price if onchain_price is not None else 0
    
    if not is_free_or_negligible(price_wei, eth_price_usd):
        log.info(f"💰 '{slug}' مدفوع - تجاهل")
        return None

    max_per_wallet = free_stage.get("max_per_wallet")
    max_gas_fee_usd = CHAIN_CONFIGS[chain_key]["max_gas_fee_usd"]

    already_bought = _successful_mints.get(slug, set())
    pending_items = [item for item in WALLETS_DATA if item["wallet"] not in already_bought]

    if not pending_items:
        tracker.has_purchased = True
        return [{"success": False, "reason": "all_wallets_completed"}]

    log.info(f"🛒 بدء الشراء لـ {slug} - {len(pending_items)} محافظ")

    tasks = [
        purchase_task_for_wallet(
            w3, item, slug, contract_address,
            price_wei, max_per_wallet, 1,
            eth_price_usd, max_gas_fee_usd
        )
        for item in pending_items
    ]

    results = await asyncio.gather(*tasks)
    
    if len(_successful_mints.get(slug, set())) >= len(WALLETS_DATA):
        tracker.has_purchased = True
    
    return results

# ==================== مراقبة المينتات ====================

async def monitor_mint(slug: str):
    """مراقبة مينت جديد"""
    if slug in _in_flight:
        return
    
    if slug in _successful_mints and len(_successful_mints[slug]) >= len(WALLETS_DATA):
        return
    
    if is_in_cooldown(slug):
        return
    
    _in_flight.add(slug)
    try:
        found, detail = await fetch_drop_detail_async(slug)
        if not found or not detail:
            _in_flight.discard(slug)
            return
        
        # ✅ التحقق من Twitter (دون إرسال إشعار)
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
                log.debug(f"خطأ في جلب تويتر: {e}")
                twitter_username = None
        
        if twitter_username is None:
            log.info(f"❌ '{slug}' مرفوض - لا يوجد حساب X")
            mark_rejected(slug)
            _in_flight.discard(slug)
            return
        
        # ✅ إنشاء متتبع
        tracker = MintTracker(slug, detail)
        tracker.twitter_username = twitter_username
        _mint_trackers[slug] = tracker
        
        log.info(f"✅ '{slug}' يوجد حساب X: @{twitter_username}")
        
        # ✅ التحقق من المرحلة المجانية
        if tracker.has_free_active_stage():
            log.info(f"🟢 {slug}: مرحلة مجانية نشطة! شراء فوري...")
            await try_buy_now_multi_wallet(slug, "ink", detail)
        else:
            log.info(f"💤 {slug}: لا توجد مرحلة مجانية. النوم...")
            tracker.go_to_sleep()
    
    except Exception as e:
        log.error(f"خطأ في مراقبة {slug}: {e}")
    finally:
        _in_flight.discard(slug)

# ==================== مدير النوم ====================

async def sleep_manager():
    """يراقب المينتات النائمة ويوقظها عند ظهور مرحلة مجانية"""
    log.info("💤 مدير النوم يعمل...")
    
    while True:
        try:
            await asyncio.sleep(SLEEP_CHECK_INTERVAL)
            
            for slug, tracker in _mint_trackers.items():
                if not tracker.is_sleeping:
                    continue
                
                if tracker.has_purchased:
                    continue
                
                if slug in _in_flight:
                    continue
                
                # ✅ تحديث المعلومات من السلسلة
                if tracker.contract_address:
                    try:
                        w3 = W3_INSTANCES[tracker.chain_key]
                        onchain_info = get_onchain_phase_info(w3, tracker.contract_address)
                        if onchain_info:
                            tracker.onchain_info = onchain_info
                    except Exception as e:
                        log.debug(f"فشل تحديث معلومات السلسلة لـ {slug}: {e}")
                
                # ✅ التحقق من المرحلة المجانية
                if tracker.has_free_active_stage():
                    log.info(f"🔥 {slug}: مرحلة مجانية! استيقاظ!")
                    tracker.wake_up()
                    
                    # ✅ جلب تفاصيل جديدة
                    found, fresh_detail = await fetch_drop_detail_async(slug)
                    if found and fresh_detail:
                        tracker.detail = fresh_detail
                        tracker._analyze_stages(fresh_detail)
                    
                    # ✅ شراء فوري
                    await try_buy_now_multi_wallet(slug, "ink", tracker.detail)
                    
                    # ✅ إذا لم يتم الشراء، نعيد النوم
                    if not tracker.has_purchased:
                        tracker.go_to_sleep()
        
        except Exception as e:
            log.error(f"خطأ في مدير النوم: {e}")
            await asyncio.sleep(1)

# ==================== الاستماع إلى OpenSea ====================

async def listen_opensea():
    """الاستماع إلى أحداث OpenSea"""
    while True:
        try:
            log.info("🔄 محاولة الاتصال بـ OpenSea Stream...")
            
            async with websockets.connect(STREAM_URL, ping_interval=None, open_timeout=15) as ws:
                log.info("✅ تم الاتصال بـ OpenSea Stream!")
                
                msg_ref = 0
                join_ref = str(msg_ref)
                subscribe_msg = json.dumps([join_ref, join_ref, "collection:*", "phx_join", {}])
                await ws.send(subscribe_msg)
                log.info("📡 تم الاشتراك في جميع المجموعات")
                msg_ref += 1
                
                for event in ["item_transferred", "item_listed", "collection_created", "item_sold", "item_metadata_updated"]:
                    event_ref = str(msg_ref)
                    event_msg = json.dumps([event_ref, event_ref, f"collection:*:{event}", "phx_join", {}])
                    await ws.send(event_msg)
                    msg_ref += 1
                
                last_heartbeat = time.time()
                
                while True:
                    if time.time() - last_heartbeat > 20:
                        hb_ref = str(msg_ref)
                        await ws.send(json.dumps([None, hb_ref, "phoenix", "heartbeat", {}]))
                        msg_ref += 1
                        last_heartbeat = time.time()
                    
                    try:
                        raw = await asyncio.wait_for(ws.recv(), timeout=5)
                    except asyncio.TimeoutError:
                        continue
                    except websockets.ConnectionClosed as e:
                        log.warning(f"⚠️ تم إغلاق الاتصال: {e}")
                        break
                    
                    try:
                        parsed = json.loads(raw)
                    except json.JSONDecodeError:
                        continue
                    
                    if isinstance(parsed, list) and len(parsed) >= 4:
                        _, _, _, event_name = parsed[:4]
                        payload_wrapper = parsed[4] if len(parsed) > 4 else {}
                        payload = (payload_wrapper or {}).get("payload") or {}
                        
                        collection_info = payload.get("collection", {}) or {}
                        item = payload.get("item", {}) or {}
                        
                        slug = collection_info.get("slug", "")
                        if not slug:
                            collection_from_item = item.get("collection", {}) or {}
                            slug = collection_from_item.get("slug", "")
                        
                        if not slug:
                            continue
                        
                        chain_info = item.get("chain", {}) or {}
                        stream_chain_name = chain_info.get("name", "")
                        if not stream_chain_name:
                            chain_info = collection_info.get("chain", {}) or {}
                            stream_chain_name = chain_info.get("name", "")
                        
                        chain_key = STREAM_NAME_TO_CHAIN_KEY.get(stream_chain_name)
                        if chain_key != "ink":
                            continue
                        
                        log.info(f"🎯 اكتشاف مينت: {slug} ({event_name})")
                        asyncio.create_task(monitor_mint(slug))
                    
        except Exception as e:
            log.error(f"❌ خطأ في الاتصال: {e}")
            await asyncio.sleep(3)

# ==================== التشغيل الرئيسي ====================

async def run():
    if not BOT_ENABLED:
        log.warning("🔴 BOT_ENABLED=false")
        # ✅ إشعار الإيقاف
        broadcast_message("🔴 البوت في وضع الإيقاف (BOT_ENABLED=false)")
        await telegram_sender()
        return

    # ✅ إشعار بدء التشغيل
    send_startup_message()
    
    # ✅ تأخير صغير قبل اختبار الاتصال
    await asyncio.sleep(2)
    
    # ✅ إشعار اختبار الاتصال
    send_test_message()
    
    log.info("🚀 تشغيل البوت مع دعم SeaDrop!")
    log.info(f"📊 {len(WALLETS_DATA)} محفظة")
    log.info(f"⏰ فحص كل {SLEEP_CHECK_INTERVAL} ثانية")
    
    # ✅ تشغيل المهام
    await asyncio.gather(
        listen_opensea(),
        sleep_manager(),
        telegram_sender()
    )

def main():
    while True:
        try:
            asyncio.run(run())
        except KeyboardInterrupt:
            log.info("تم الإيقاف يدوياً.")
            broadcast_message("🛑 تم إيقاف البوت يدوياً")
            break
        except Exception as e:
            log.critical(f"توقف غير متوقع: {e}")
            # ✅ إشعار الخطأ الحرج
            broadcast_message(f"❌ توقف غير متوقع: {str(e)[:100]}")
            time.sleep(5)

if __name__ == "__main__":
    main()
