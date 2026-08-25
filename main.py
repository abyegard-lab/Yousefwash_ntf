"""
main.py - النظام الكامل مع إشعارات التيليجرام (نسخة مُصلحة)
"""

import asyncio
import json
import logging
import os
import time
from datetime import datetime
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

# ==================== التيليجرام - نظام مُصلح ====================

class TelegramSender:
    """نظام إرسال رسائل التيليجرام"""
    
    def __init__(self):
        self.send_queue = asyncio.Queue()
        self.is_running = False
        self.bot_tokens = TELEGRAM_BOT_TOKENS
        self.chat_ids = TELEGRAM_CHAT_IDS
        self.last_message_time = 0
        self.min_interval = 0.1  # 100ms بين الرسائل
        
        # ✅ إحصائيات
        self.messages_sent = 0
        self.errors = 0
    
    async def start(self):
        """بدء تشغيل نظام الإرسال"""
        if self.is_running:
            return
        
        self.is_running = True
        asyncio.create_task(self._sender_loop())
        log.info("📨 تم بدء نظام إرسال التيليجرام")
    
    async def send(self, text: str, bot_token: str = None, chat_id: str = None):
        """إضافة رسالة إلى قائمة الانتظار"""
        if not bot_token or not chat_id:
            # ✅ إرسال لجميع المحافظ
            for i in range(len(self.bot_tokens)):
                await self.send_to_specific(
                    text, 
                    self.bot_tokens[i], 
                    self.chat_ids[i]
                )
            return
        
        await self.send_to_specific(text, bot_token, chat_id)
    
    async def send_to_specific(self, text: str, bot_token: str, chat_id: str):
        """إضافة رسالة لقائمة الانتظار لمحفظة محددة"""
        await self.send_queue.put({
            "bot_token": bot_token,
            "chat_id": chat_id,
            "text": text,
            "timestamp": time.time()
        })
        log.debug(f"📨 تم إضافة رسالة للقائمة: {text[:50]}...")
    
    async def send_to_all(self, text: str):
        """إرسال رسالة لجميع المحافظ"""
        for i in range(len(self.bot_tokens)):
            await self.send_to_specific(
                text,
                self.bot_tokens[i],
                self.chat_ids[i]
            )
    
    async def _sender_loop(self):
        """حلقة معالجة الرسائل"""
        log.info("🔄 بدء حلقة إرسال التيليجرام...")
        
        while True:
            try:
                # ✅ انتظار رسالة من القائمة
                msg = await self.send_queue.get()
                
                bot_token = msg.get("bot_token")
                chat_id = msg.get("chat_id")
                text = msg.get("text", "")
                
                if not bot_token or not chat_id or not text:
                    self.send_queue.task_done()
                    continue
                
                # ✅ التحقق من التباعد بين الرسائل
                current_time = time.time()
                time_since_last = current_time - self.last_message_time
                if time_since_last < self.min_interval:
                    await asyncio.sleep(self.min_interval - time_since_last)
                
                # ✅ إرسال الرسالة
                success = await self._send_message(bot_token, chat_id, text)
                
                if success:
                    self.messages_sent += 1
                    log.info(f"✅ تم إرسال رسالة (#{self.messages_sent})")
                else:
                    self.errors += 1
                    log.error(f"❌ فشل إرسال رسالة (#{self.errors})")
                
                self.last_message_time = time.time()
                self.send_queue.task_done()
                
            except asyncio.CancelledError:
                log.info("⏹️ تم إيقاف حلقة الإرسال")
                break
            except Exception as e:
                log.error(f"❌ خطأ في حلقة الإرسال: {e}")
                self.send_queue.task_done()
                await asyncio.sleep(1)
    
    async def _send_message(self, bot_token: str, chat_id: str, text: str) -> bool:
        """إرسال رسالة واحدة مع إعادة المحاولة"""
        max_retries = 3
        retry_delay = 1
        
        for attempt in range(max_retries):
            try:
                telegram_api = f"https://api.telegram.org/bot{bot_token}"
                
                # ✅ استخدام asyncio.to_thread لتجنب حظر الحلقة
                response = await asyncio.to_thread(
                    requests.post,
                    f"{telegram_api}/sendMessage",
                    data={
                        "chat_id": chat_id,
                        "text": text,
                        "parse_mode": "HTML",
                        "disable_web_page_preview": True
                    },
                    timeout=10,
                )
                
                if response.status_code == 200:
                    log.debug(f"✅ تم الإرسال: {text[:30]}...")
                    return True
                else:
                    log.warning(f"⚠️ محاولة {attempt+1} فشلت: {response.status_code} - {response.text[:100]}")
                    
            except Exception as e:
                log.warning(f"⚠️ محاولة {attempt+1} فشلت: {e}")
            
            # ✅ انتظار قبل إعادة المحاولة
            if attempt < max_retries - 1:
                await asyncio.sleep(retry_delay * (attempt + 1))
        
        return False
    
    async def test_connection(self):
        """اختبار الاتصال بالتيليجرام"""
        log.info("🧪 اختبار اتصال التيليجرام...")
        
        test_msg = (
            f"🧪 <b>اختبار الاتصال</b>\n\n"
            f"✅ البوت يعمل\n"
            f"🕐 {datetime.now().strftime('%H:%M:%S')}\n"
            f"📊 عدد المحافظ: {len(self.bot_tokens)}"
        )
        
        await self.send_to_all(test_msg)
        log.info("📤 تم إرسال رسالة الاختبار")

# ==================== إنشاء كائن التيليجرام ====================

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

# ==================== رسائل التيليجرام ====================

async def send_startup_message():
    """🚀 إشعار بدء التشغيل"""
    msg = (
        f"🚀 <b>تم تشغيل البوت!</b>\n\n"
        f"📊 عدد المحافظ: <b>{len(WALLETS_DATA)}</b>\n"
        f"🔗 الشبكة: <b>Ink</b>\n"
        f"⚡ الحالة: <b>جاهز للعمل</b>\n"
        f"🔄 الوضع: <b>اكتشاف تلقائي</b>\n\n"
        f"💡 في انتظار المينتات المجانية..."
    )
    
    await telegram.send_to_all(msg)
    log.info("📤 تم إرسال إشعار بدء التشغيل")

async def send_test_message():
    """🧪 إشعار اختبار"""
    msg = (
        f"✅ <b>البوت يعمل بشكل صحيح</b>\n\n"
        f"📊 عدد المحافظ: <b>{len(WALLETS_DATA)}</b>\n"
        f"🕐 الوقت: <b>{datetime.now().strftime('%H:%M:%S')}</b>\n"
        f"🔗 الشبكة: <b>Ink</b>\n\n"
        f"📡 جاهز لاكتشاف المينتات..."
    )
    
    await telegram.send_to_all(msg)
    log.info("📤 تم إرسال إشعار الاختبار")

async def send_purchase_message(slug: str, result: dict):
    """💰 إشعار الشراء"""
    wallet = result.get('wallet', 'unknown')
    wallet_short = wallet[:8] + "..." + wallet[-6:] if len(wallet) > 14 else wallet
    tx_hash = result.get('tx_hash', '')
    tx_short = tx_hash[:10] + "..." if tx_hash else 'غير متاح'
    quantity = result.get('quantity', 1)
    gas_fee = result.get('gas_fee_usd', 0)
    
    tracker = _mint_trackers.get(slug, {})
    name = tracker.get('name', slug)
    
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
        f"🕐 الوقت: <b>{datetime.now().strftime('%H:%M:%S')}</b>"
    )
    
    await telegram.send_to_all(msg)
    log.info(f"📤 تم إرسال إشعار الشراء لـ {slug}")

async def send_error_message(slug: str, error: str):
    """⚠️ إشعار الخطأ المهم"""
    important_errors = ["sold_out", "insufficient_funds", "gas_too_high", "balance_too_low"]
    
    is_important = any(err in error.lower() for err in important_errors)
    if not is_important:
        return
    
    tracker = _mint_trackers.get(slug, {})
    name = tracker.get('name', slug)
    
    msg = (
        f"⚠️ <b>تنبيه</b>\n\n"
        f"📦 المجموعة: <b>{name}</b>\n"
        f"❌ المشكلة: <code>{error}</code>\n"
        f"🕐 الوقت: <b>{datetime.now().strftime('%H:%M:%S')}</b>"
    )
    
    await telegram.send_to_all(msg)
    log.info(f"📤 تم إرسال إشعار الخطأ لـ {slug}")

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
            
            # ✅ إرسال إشعار الشراء
            await send_purchase_message(slug, res)
            
            log.info(f"✅ شراء ناجح للمحفظة {wallet_addr[:8]}... - {slug}")
        else:
            reason = res.get('reason', 'unknown')
            log.warning(f"❌ فشل شراء للمحفظة {wallet_addr[:8]}... - {slug}: {reason}")
            
            # ✅ إرسال إشعار خطأ للمشاكل المهمة
            await send_error_message(slug, reason)

        return res

async def try_buy_now_multi_wallet(slug: str, chain_key: str, detail: dict, price_wei: int = None):
    """محاولة الشراء من جميع المحافظ"""
    
    # ✅ التحقق من وجود مرحلة مجانية
    tracker = _mint_trackers.get(slug)
    if not tracker:
        return None
    
    # ✅ جلب معلومات السلسلة
    contract_address = detail.get("contract_address")
    if not contract_address:
        return None
    
    w3 = W3_INSTANCES[chain_key]
    eth_price_usd = get_eth_price_usd()
    
    # ✅ جلب السعر من السلسلة
    if price_wei is None:
        onchain_price = await asyncio.to_thread(get_onchain_public_price_wei, w3, contract_address)
        if onchain_price:
            price_wei = onchain_price
    
    if price_wei is None:
        price_wei = 0
    
    # ✅ التحقق من المجانية
    if not is_free_or_negligible(price_wei, eth_price_usd):
        log.info(f"💰 '{slug}' مدفوع - تجاهل (${(price_wei/1e18)*eth_price_usd:.4f})")
        return None
    
    # ✅ تحديد الكمية
    max_per_wallet = None
    max_gas_fee_usd = CHAIN_CONFIGS[chain_key]["max_gas_fee_usd"]
    
    already_bought = _successful_mints.get(slug, set())
    pending_items = [item for item in WALLETS_DATA if item["wallet"] not in already_bought]
    
    if not pending_items:
        log.info(f"✅ {slug}: جميع المحافظ اشتريت")
        tracker['has_purchased'] = True
        return None
    
    log.info(f"🛒 بدء الشراء لـ {slug} - {len(pending_items)} محافظ")
    
    tasks = [
        purchase_task_for_wallet(
            w3, item, slug, contract_address,
            price_wei, max_per_wallet, 1,
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
        
        # ✅ تخزين معلومات المينت
        _mint_trackers[slug] = {
            'name': detail.get('collection_name') or slug,
            'detail': detail,
            'twitter': twitter_username,
            'has_purchased': False,
            'first_seen': time.time()
        }
        
        log.info(f"✅ '{slug}' يوجد حساب X: @{twitter_username}")
        
        # ✅ محاولة الشراء فوراً
        await try_buy_now_multi_wallet(slug, "ink", detail)
    
    except Exception as e:
        log.error(f"خطأ في مراقبة {slug}: {e}")
    finally:
        _in_flight.discard(slug)

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
        await telegram.send_to_all("🔴 البوت في وضع الإيقاف (BOT_ENABLED=false)")
        await telegram.start()
        await asyncio.Event().wait()  # انتظار إلى الأبد
        return
    
    # ✅ بدء نظام التيليجرام أولاً
    await telegram.start()
    
    # ✅ إرسال رسائل البدء
    await send_startup_message()
    await asyncio.sleep(2)
    await send_test_message()
    
    log.info("🚀 تشغيل البوت مع دعم SeaDrop!")
    log.info(f"📊 {len(WALLETS_DATA)} محفظة")
    log.info(f"⏰ فحص كل {SLEEP_CHECK_INTERVAL} ثانية")
    
    # ✅ تشغيل المهام
    await asyncio.gather(
        listen_opensea(),
        telegram.send_queue.join(),  # انتظار معالجة جميع الرسائل
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
