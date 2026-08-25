"""
main.py - النظام الكامل مع إشعارات محدودة (بدء + شراء فقط)
"""

import asyncio
import json
import logging
import os
import time
from datetime import datetime
from typing import Optional, Dict, Set, List

import requests
import websockets
from dotenv import load_dotenv

from buyer import (
    get_web3,
    attempt_purchase_single_wallet,
    get_onchain_public_price_wei,
    get_onchain_phase_info,
    get_wallet_lock,
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

# ✅ إعدادات متقدمة
FREE_PRICE_THRESHOLD_USD = 0.0001
SLEEP_CHECK_INTERVAL = 0.5
MAX_GAS_FEE_USD = 0.50

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

_mint_trackers: Dict[str, dict] = {}
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

# ==================== التيليجرام ====================

class TelegramSender:
    """نظام إرسال رسائل التيليجرام"""
    
    def __init__(self):
        self.send_queue = asyncio.Queue()
        self.is_running = False
        self.bot_tokens = TELEGRAM_BOT_TOKENS
        self.chat_ids = TELEGRAM_CHAT_IDS
        self.last_message_time = 0
        self.min_interval = 0.1
        self.messages_sent = 0
        self.errors = 0
    
    async def start(self):
        if self.is_running:
            return
        self.is_running = True
        asyncio.create_task(self._sender_loop())
        log.info("📨 تم بدء نظام إرسال التيليجرام")
    
    async def send_to_all(self, text: str):
        """إرسال رسالة لجميع المحافظ"""
        for i in range(len(self.bot_tokens)):
            await self.send_queue.put({
                "bot_token": self.bot_tokens[i],
                "chat_id": self.chat_ids[i],
                "text": text,
                "timestamp": time.time()
            })
    
    async def _sender_loop(self):
        """حلقة معالجة الرسائل"""
        while True:
            try:
                msg = await self.send_queue.get()
                
                bot_token = msg.get("bot_token")
                chat_id = msg.get("chat_id")
                text = msg.get("text", "")
                
                if not bot_token or not chat_id or not text:
                    self.send_queue.task_done()
                    continue
                
                current_time = time.time()
                time_since_last = current_time - self.last_message_time
                if time_since_last < self.min_interval:
                    await asyncio.sleep(self.min_interval - time_since_last)
                
                success = await self._send_message(bot_token, chat_id, text)
                
                if success:
                    self.messages_sent += 1
                    log.debug(f"✅ تم إرسال رسالة (#{self.messages_sent})")
                else:
                    self.errors += 1
                
                self.last_message_time = time.time()
                self.send_queue.task_done()
                
            except Exception as e:
                log.error(f"❌ خطأ في حلقة الإرسال: {e}")
                self.send_queue.task_done()
                await asyncio.sleep(1)
    
    async def _send_message(self, bot_token: str, chat_id: str, text: str) -> bool:
        """إرسال رسالة واحدة مع إعادة المحاولة"""
        max_retries = 3
        
        for attempt in range(max_retries):
            try:
                telegram_api = f"https://api.telegram.org/bot{bot_token}"
                
                response = await asyncio.to_thread(
                    requests.post,
                    f"{telegram_api}/sendMessage",
                    data={
                        "chat_id": chat_id,
                        "text": text,
                        "parse_mode": "HTML",
                        "disable_web_page_preview": False
                    },
                    timeout=10,
                )
                
                if response.status_code == 200:
                    return True
                else:
                    log.warning(f"⚠️ محاولة {attempt+1} فشلت: {response.status_code}")
                    
            except Exception as e:
                log.warning(f"⚠️ محاولة {attempt+1} فشلت: {e}")
            
            if attempt < max_retries - 1:
                await asyncio.sleep(1 * (attempt + 1))
        
        return False

telegram = TelegramSender()

# ==================== دوال مساعدة ====================

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

def is_free_or_negligible(price_wei: int, eth_price_usd: float) -> bool:
    if price_wei == 0:
        return True
    price_usd = (price_wei / 1e18) * eth_price_usd
    return price_usd < FREE_PRICE_THRESHOLD_USD

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

def get_collection_url(slug: str) -> str:
    """جلب رابط المجموعة على OpenSea"""
    return f"https://opensea.io/collection/{slug}"

def get_available_quantity(detail: dict) -> int:
    """جلب الكمية المتاحة للشراء من OpenSea"""
    try:
        max_supply = int(detail.get("max_supply") or 0)
        total_supply = int(detail.get("total_supply") or 0)
        remaining = max(0, max_supply - total_supply)
        
        active_stage = detail.get("active_stage", {})
        max_per_wallet = active_stage.get("max_total_mintable_by_wallet") or active_stage.get("max_per_wallet")
        if max_per_wallet:
            max_per_wallet = int(max_per_wallet)
        
        if max_per_wallet:
            available = min(remaining, max_per_wallet)
        else:
            available = remaining
        
        return available
        
    except Exception as e:
        log.error(f"❌ خطأ في جلب الكمية: {e}")
        return 1

def get_uptime() -> str:
    """الحصول على مدة التشغيل"""
    seconds = int(time.time() - bot_stats["start_time"])
    hours = seconds // 3600
    minutes = (seconds % 3600) // 60
    seconds = seconds % 60
    return f"{hours}h {minutes}m {seconds}s"

# ==================== ✅ تحليل المراحل (للإستخدام الداخلي فقط) ====================

def analyze_all_stages(detail: dict) -> dict:
    """تحليل شامل لجميع مراحل المينت (للاستخدام الداخلي)"""
    eth_price_usd = get_eth_price_usd()
    
    stages = {
        "active": None,
        "upcoming": [],
        "past": [],
        "free_stages": [],
        "paid_stages": [],
        "has_free_active": False,
        "has_free_upcoming": False,
        "next_free_stage": None,
        "total_stages": 0,
        "stage_types": {
            "team": [],
            "allowlist": [],
            "public": [],
            "free": [],
            "presale": [],
            "early_access": []
        }
    }
    
    # المرحلة الحالية
    current = detail.get("active_stage")
    if current:
        stage_info = parse_stage(current, "active", eth_price_usd)
        stages["active"] = stage_info
        stages["total_stages"] += 1
        
        stage_type = stage_info["type"]
        if stage_type in stages["stage_types"]:
            stages["stage_types"][stage_type].append(stage_info)
        
        if stage_info["is_free"]:
            stages["free_stages"].append(stage_info)
            stages["has_free_active"] = True
        else:
            stages["paid_stages"].append(stage_info)
    
    # المراحل القادمة
    next_stages = detail.get("next_stages", [])
    for stage in next_stages:
        stage_info = parse_stage(stage, "upcoming", eth_price_usd)
        stages["upcoming"].append(stage_info)
        stages["total_stages"] += 1
        
        stage_type = stage_info["type"]
        if stage_type in stages["stage_types"]:
            stages["stage_types"][stage_type].append(stage_info)
        
        if stage_info["is_free"]:
            stages["free_stages"].append(stage_info)
            stages["has_free_upcoming"] = True
            if not stages["next_free_stage"]:
                stages["next_free_stage"] = stage_info
        else:
            stages["paid_stages"].append(stage_info)
    
    # المراحل السابقة
    past_stages = detail.get("past_stages", [])
    for stage in past_stages:
        stage_info = parse_stage(stage, "past", eth_price_usd)
        stages["past"].append(stage_info)
        stages["total_stages"] += 1
    
    return stages

def parse_stage(stage: dict, status: str, eth_price_usd: float) -> dict:
    """تحليل مرحلة واحدة"""
    price_wei = int(stage.get("price", "0"))
    price_usd = (price_wei / 1e18) * eth_price_usd
    is_free = price_usd < FREE_PRICE_THRESHOLD_USD or price_wei == 0
    
    stage_type = stage.get("type", "unknown").lower()
    
    if is_free:
        stage_type = "free"
    elif stage_type in ["public", "open"]:
        stage_type = "public"
    elif stage_type in ["allowlist", "whitelist"]:
        stage_type = "allowlist"
    elif stage_type in ["team", "creator"]:
        stage_type = "team"
    elif stage_type in ["presale", "pre_sale"]:
        stage_type = "presale"
    elif stage_type in ["early_access", "early"]:
        stage_type = "early_access"
    
    return {
        "type": stage_type,
        "price_wei": price_wei,
        "price_usd": price_usd,
        "is_free": is_free,
        "status": status,
        "start_time": stage.get("start_time"),
        "end_time": stage.get("end_time"),
        "max_per_wallet": stage.get("max_per_wallet"),
        "max_total_mintable_by_wallet": stage.get("max_total_mintable_by_wallet"),
        "is_eligible": not (stage_type in ["team", "allowlist", "presale", "early_access"]),
        "requires_verification": stage_type in ["team", "allowlist", "presale", "early_access"]
    }

# ==================== ✅ رسائل التيليجرام المحدودة ====================

async def send_startup_message():
    """🚀 إشعار بدء التشغيل فقط"""
    wallet_count = len(WALLETS_DATA)
    uptime = get_uptime()
    
    msg = (
        f"🚀 <b>تم تشغيل البوت!</b>\n\n"
        f"📊 عدد المحافظ: <b>{wallet_count}</b>\n"
        f"🔗 الشبكة: <b>Ink</b>\n"
        f"⚡ الحالة: <b>جاهز</b>\n"
        f"🔄 الوضع: <b>اكتشاف تلقائي</b>\n"
        f"⏰ وقت التشغيل: <b>{uptime}</b>\n\n"
        f"💡 في انتظار المينتات المجانية..."
    )
    
    await telegram.send_to_all(msg)
    log.info("📤 تم إرسال إشعار بدء التشغيل")

async def send_purchase_message(slug: str, result: dict):
    """💰 إشعار الشراء الناجح فقط"""
    wallet = result.get('wallet', 'unknown')
    wallet_short = wallet[:8] + "..." + wallet[-6:] if len(wallet) > 14 else wallet
    tx_hash = result.get('tx_hash', '')
    tx_short = tx_hash[:10] + "..." if tx_hash else 'غير متاح'
    quantity = result.get('quantity', 1)
    gas_fee = result.get('gas_fee_usd', 0)
    
    tracker = _mint_trackers.get(slug, {})
    name = tracker.get('name', slug)
    collection_url = get_collection_url(slug)
    
    total_purchased = len(_successful_mints.get(slug, set()))
    total_wallets = len(WALLETS_DATA)
    
    # ✅ تحديث الإحصائيات
    bot_stats["mints_purchased"] += 1
    bot_stats["wallets_used"].add(wallet)
    
    msg = (
        f"💰 <b>تم الشراء بنجاح!</b>\n\n"
        f"📦 المجموعة: <b>{name}</b>\n"
        f"🌐 <a href='{collection_url}'>عرض المجموعة</a>\n"
        f"👛 المحفظة: <code>{wallet_short}</code>\n"
        f"🔢 الكمية: <b>{quantity}</b> (دفعة واحدة)\n"
        f"⛽ رسوم الغاز: <b>${gas_fee:.4f}</b>\n"
        f"🔗 المعاملة: <code>{tx_short}</code>\n"
        f"📊 المحافظ المشترية: <b>{total_purchased}/{total_wallets}</b>\n"
        f"🕐 الوقت: <b>{datetime.now().strftime('%H:%M:%S')}</b>"
    )
    
    await telegram.send_to_all(msg)
    log.info(f"📤 تم إرسال إشعار الشراء لـ {slug}")

# ❌ إلغاء جميع الإشعارات الأخرى
# - لا يوجد إشعار اكتشاف المينت
# - لا يوجد إشعار اختبار
# - لا يوجد إشعار خطأ
# - لا يوجد إشعار مراحل

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

        tracker = _mint_trackers.get(slug, {})
        detail = tracker.get('detail', {})
        
        available_quantity = get_available_quantity(detail)
        
        quantity = min(
            available_quantity,
            max_per_wallet if max_per_wallet else 999,
            remaining
        )
        quantity = max(1, quantity)
        
        log.info(f"🔫 محاولة شراء {quantity} من المحفظة {wallet_addr[:8]}... - {slug}")
        
        res = await asyncio.to_thread(
            attempt_purchase_single_wallet,
            w3, pk, wallet_addr,
            contract_address, price_wei, max_per_wallet, remaining,
            eth_price_usd, max_gas_fee_usd,
            quantity
        )

        if res.get("success"):
            if slug not in _successful_mints:
                _successful_mints[slug] = set()
            _successful_mints[slug].add(wallet_addr)
            
            # ✅ إرسال إشعار الشراء فقط
            await send_purchase_message(slug, res)
            
            log.info(f"✅ شراء {quantity} بنجاح من المحفظة {wallet_addr[:8]}... - {slug}")
        else:
            reason = res.get('reason', 'unknown')
            log.warning(f"❌ فشل شراء من المحفظة {wallet_addr[:8]}... - {slug}: {reason}")

        return res

async def try_buy_now_multi_wallet(slug: str, chain_key: str, detail: dict, price_wei: int = None):
    """محاولة الشراء من جميع المحافظ"""
    
    tracker = _mint_trackers.get(slug)
    if not tracker:
        return None
    
    contract_address = detail.get("contract_address")
    if not contract_address:
        return None
    
    w3 = W3_INSTANCES[chain_key]
    eth_price_usd = get_eth_price_usd()
    
    if price_wei is None:
        onchain_price = await asyncio.to_thread(get_onchain_public_price_wei, w3, contract_address)
        if onchain_price:
            price_wei = onchain_price
    
    if price_wei is None:
        price_wei = 0
    
    if not is_free_or_negligible(price_wei, eth_price_usd):
        log.info(f"💰 '{slug}' مدفوع - تجاهل")
        return None
    
    active_stage = detail.get("active_stage", {})
    max_per_wallet = active_stage.get("max_total_mintable_by_wallet") or active_stage.get("max_per_wallet")
    if max_per_wallet:
        max_per_wallet = int(max_per_wallet)
    
    max_supply = int(detail.get("max_supply") or 0)
    total_supply = int(detail.get("total_supply") or 0)
    remaining = max(0, max_supply - total_supply)
    
    if remaining <= 0:
        log.info(f"❌ '{slug}' نفذت الكمية")
        return None
    
    available_quantity = get_available_quantity(detail)
    
    if available_quantity <= 0:
        log.info(f"❌ '{slug}' لا توجد كمية متاحة")
        return None
    
    max_gas_fee_usd = CHAIN_CONFIGS[chain_key]["max_gas_fee_usd"]
    
    already_bought = _successful_mints.get(slug, set())
    pending_items = [item for item in WALLETS_DATA if item["wallet"] not in already_bought]
    
    if not pending_items:
        log.info(f"✅ {slug}: جميع المحافظ اشتريت")
        tracker['has_purchased'] = True
        return None
    
    log.info(f"🛒 بدء الشراء لـ {slug} - {len(pending_items)} محافظ")
    log.info(f"📊 الكمية لكل محفظة: {available_quantity}")
    
    tasks = [
        purchase_task_for_wallet(
            w3, item, slug, contract_address,
            price_wei, max_per_wallet, remaining,
            eth_price_usd, max_gas_fee_usd
        )
        for item in pending_items
    ]
    
    await asyncio.gather(*tasks)
    
    if len(_successful_mints.get(slug, set())) >= len(WALLETS_DATA):
        tracker['has_purchased'] = True
    
    return None

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
        
        # ✅ التحقق من Twitter
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
        
        # ✅ تحليل المراحل (داخلي فقط)
        stages = analyze_all_stages(detail)
        
        # ✅ تخزين معلومات المينت
        _mint_trackers[slug] = {
            'name': detail.get('collection_name') or slug,
            'detail': detail,
            'twitter': twitter_username,
            'has_purchased': False,
            'first_seen': time.time(),
            'stages': stages
        }
        
        log.info(f"✅ '{slug}' يوجد حساب X: @{twitter_username}")
        log.info(f"📊 عدد المراحل: {stages['total_stages']}")
        
        # ❌ لا يتم إرسال إشعار اكتشاف المينت
        
        # ✅ التحقق من وجود مرحلة مجانية نشطة
        if stages["has_free_active"]:
            log.info(f"🟢 {slug}: مرحلة مجانية نشطة! شراء فوري...")
            await try_buy_now_multi_wallet(slug, "ink", detail)
        else:
            log.info(f"💤 {slug}: لا توجد مرحلة مجانية نشطة. في الانتظار...")
    
    except Exception as e:
        log.error(f"خطأ في مراقبة {slug}: {e}")
    finally:
        _in_flight.discard(slug)

# ==================== مدير المراحل ====================

async def stage_sleep_manager():
    """يراقب المراحل القادمة ويوقظ عند بدء المرحلة المجانية"""
    log.info("💤 مدير مراقبة المراحل يعمل...")
    
    while True:
        try:
            await asyncio.sleep(SLEEP_CHECK_INTERVAL)
            
            for slug, tracker in _mint_trackers.items():
                if tracker.get('has_purchased', False):
                    continue
                
                if slug in _in_flight:
                    continue
                
                # ✅ جلب تفاصيل جديدة
                found, fresh_detail = await fetch_drop_detail_async(slug)
                if not found or not fresh_detail:
                    continue
                
                # ✅ تحليل المراحل
                stages = analyze_all_stages(fresh_detail)
                
                # ✅ تحديث المراحل
                tracker['stages'] = stages
                tracker['detail'] = fresh_detail
                
                # ✅ إذا أصبحت هناك مرحلة مجانية نشطة
                if stages["has_free_active"] and not tracker.get('has_purchased', False):
                    log.info(f"🔥 {slug}: أصبحت المرحلة المجانية نشطة! شراء فوري!")
                    await try_buy_now_multi_wallet(slug, "ink", fresh_detail)
                
        except Exception as e:
            log.error(f"خطأ في مدير المراحل: {e}")
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
                msg_ref += 1
                
                for event in ["item_transferred", "item_listed", "collection_created", "item_sold"]:
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
        # ✅ فقط إشعار الإيقاف
        await telegram.send_to_all("🔴 البوت في وضع الإيقاف (BOT_ENABLED=false)")
        await telegram.start()
        await asyncio.Event().wait()
        return
    
    # ✅ بدء التيليجرام
    await telegram.start()
    
    # ✅ إرسال إشعار بدء التشغيل فقط
    await send_startup_message()
    
    log.info("🚀 تشغيل البوت!")
    log.info(f"📊 {len(WALLETS_DATA)} محفظة")
    log.info(f"🔄 وضع اكتشاف المراحل (بدون إشعارات)")
    
    # ✅ تشغيل المهام
    await asyncio.gather(
        listen_opensea(),
        stage_sleep_manager(),
        telegram.send_queue.join(),
    )

def main():
    while True:
        try:
            asyncio.run(run())
        except KeyboardInterrupt:
            log.info("تم الإيقاف يدوياً.")
            break
        except Exception as e:
            log.critical(f"توقف غير متوقع: {e}")
            time.sleep(5)

if __name__ == "__main__":
    main()
