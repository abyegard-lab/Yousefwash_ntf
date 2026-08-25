# main.py - نسخة مبسطة ومُصححة

import asyncio
import json
import logging
import os
import time
from datetime import datetime, timezone
from collections import defaultdict

import requests
import websockets
from dotenv import load_dotenv

from buyer import (
    get_web3,
    attempt_purchase_single_wallet,
    get_onchain_public_price_wei,
    get_wallet_lock,
    is_free_or_negligible,
)
from twitter_checker import get_twitter_username_from_opensea

load_dotenv()

# ==================== قراءة الإعدادات ====================

OPENSEA_API_KEY = os.environ.get("OPENSEA_API_KEY", "")
BOT_ENABLED = os.environ.get("BOT_ENABLED", "false").lower() == "true"

PRIVATE_KEYS = [k.strip() for k in os.environ.get("PRIVATE_KEYS", "").split(",") if k.strip()]
WALLETS = [w.strip() for w in os.environ.get("WALLETS", "").split(",") if w.strip()]
TELEGRAM_BOT_TOKENS = [t.strip() for t in os.environ.get("TELEGRAM_BOT_TOKENS", "").split(",") if t.strip()]
TELEGRAM_CHAT_IDS = [c.strip() for c in os.environ.get("TELEGRAM_CHAT_IDS", "").split(",") if c.strip()]

if not OPENSEA_API_KEY:
    raise ValueError("OPENSEA_API_KEY غير مضبوط في ملف .env!")

if not PRIVATE_KEYS:
    raise ValueError("PRIVATE_KEYS غير مضبوط في ملف .env!")

if len(PRIVATE_KEYS) != len(WALLETS):
    raise ValueError("عدد PRIVATE_KEYS لا يساوي عدد WALLETS!")

WALLETS_DATA = []
for i in range(len(WALLETS)):
    WALLETS_DATA.append({
        "wallet": WALLETS[i],
        "private_key": PRIVATE_KEYS[i],
        "bot_token": TELEGRAM_BOT_TOKENS[i] if i < len(TELEGRAM_BOT_TOKENS) else "",
        "chat_id": TELEGRAM_CHAT_IDS[i] if i < len(TELEGRAM_CHAT_IDS) else "",
    })

INK_RPC_URL = os.environ.get("INK_RPC_URL", "https://rpc-gel.inkonchain.com/")

STREAM_URL = f"wss://stream.openseabeta.com/socket/websocket?token={OPENSEA_API_KEY}&vsn=2.0.0"
DROPS_API_BASE = "https://api.opensea.io/api/v2/drops"

# ==================== إعدادات ====================

FREE_PRICE_THRESHOLD_USD = 0.0001
SCAN_INTERVAL = 1  # ثانية
CHECK_INTERVAL = 1  # ثانية

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("auto-buyer")

CHAIN_CONFIGS = {
    "ink": {
        "rpc_url": INK_RPC_URL,
        "max_gas_fee_usd": 0.50,
    },
}

W3_INSTANCES = {key: get_web3(cfg["rpc_url"]) for key, cfg in CHAIN_CONFIGS.items()}

# ==================== التتبع ====================

successful_mints: dict[str, set[str]] = {}
discovered_mints: set[str] = set()
tracked_mints: dict[str, dict] = {}
in_flight: set[str] = set()

# ==================== التخزين المؤقت ====================

_drop_cache = {}
_twitter_cache = {}
_eth_price_cache = {"value": None, "ts": 0}

def get_cached_drop(slug: str):
    if slug in _drop_cache:
        detail, timestamp = _drop_cache[slug]
        if time.time() - timestamp < 5:
            return detail
    return None

def set_cached_drop(slug: str, detail):
    _drop_cache[slug] = (detail, time.time())

def get_cached_twitter(slug: str):
    if slug in _twitter_cache:
        username, timestamp = _twitter_cache[slug]
        if time.time() - timestamp < 600:
            return username
    return None

def set_cached_twitter(slug: str, username):
    _twitter_cache[slug] = (username, time.time())

def get_eth_price_usd() -> float:
    now = time.time()
    if _eth_price_cache["value"] and (now - _eth_price_cache["ts"] < 60):
        return _eth_price_cache["value"]
    try:
        resp = requests.get(
            "https://api.coingecko.com/api/v3/simple/price?ids=ethereum&vs_currencies=usd",
            timeout=5,
        )
        price = resp.json()["ethereum"]["usd"]
        _eth_price_cache["value"] = price
        _eth_price_cache["ts"] = now
        return price
    except Exception as e:
        log.warning(f"[السعر] خطأ: {e}")
        return _eth_price_cache["value"] or 3000.0

# ==================== API ====================

_session = None

def get_session():
    global _session
    if _session is None:
        _session = requests.Session()
        _session.headers.update({"x-api-key": OPENSEA_API_KEY})
    return _session

def fetch_drop_detail(slug: str):
    """جلب تفاصيل المينت"""
    cached = get_cached_drop(slug)
    if cached is not None:
        return cached
    
    try:
        session = get_session()
        resp = session.get(f"{DROPS_API_BASE}/{slug}", timeout=5)
        
        if resp.status_code == 200:
            detail = resp.json()
            set_cached_drop(slug, detail)
            return detail
        else:
            log.warning(f"[API] HTTP {resp.status_code} لـ {slug}")
            return None
    except Exception as e:
        log.warning(f"[API] خطأ لـ {slug}: {e}")
        return None

# ==================== تحليل المراحل ====================

def get_free_stage(detail: dict) -> dict:
    """الحصول على المرحلة المجانية من التفاصيل"""
    eth_price_usd = get_eth_price_usd()
    
    # 1. فحص المرحلة الحالية
    current = detail.get("active_stage")
    if current:
        try:
            price_wei = int(current.get("price", "0"))
        except:
            price_wei = 0
        
        # التحقق من السعر على السلسلة
        contract_address = detail.get("contract_address")
        if contract_address:
            w3 = W3_INSTANCES.get("ink")
            if w3:
                onchain_price = get_onchain_public_price_wei(w3, contract_address)
                if onchain_price is not None:
                    price_wei = onchain_price
        
        if is_free_or_negligible(price_wei, eth_price_usd):
            stage_type = current.get("type", "public").lower()
            if stage_type in ["public", "free", "open"] or price_wei == 0:
                return {
                    "price_wei": price_wei,
                    "max_per_wallet": current.get("max_per_wallet"),
                    "status": "active",
                    "name": stage_type,
                }
    
    # 2. فحص المراحل القادمة
    next_stages = detail.get("next_stages", [])
    for stage in next_stages:
        try:
            price_wei = int(stage.get("price", "0"))
        except:
            price_wei = 0
        
        stage_type = stage.get("type", "unknown").lower()
        
        if is_free_or_negligible(price_wei, eth_price_usd):
            if stage_type in ["public", "free", "open"] or price_wei == 0:
                start_time = stage.get("start_time")
                if start_time:
                    try:
                        start_dt = datetime.fromisoformat(start_time.replace("Z", "+00:00"))
                        time_until = (start_dt - datetime.now(timezone.utc)).total_seconds()
                        if time_until < 10:  # قريباً
                            return {
                                "price_wei": price_wei,
                                "max_per_wallet": stage.get("max_per_wallet"),
                                "status": "imminent",
                                "name": stage_type,
                                "start_time": start_time,
                            }
                    except:
                        pass
    
    return None

# ==================== دوال الشراء ====================

async def buy_for_wallet(w3, wallet_data, slug, contract_address, price_wei, max_per_wallet, remaining, eth_price_usd):
    """شراء لمحفظة واحدة"""
    wallet_addr = wallet_data["wallet"]
    pk = wallet_data["private_key"]
    
    lock = get_wallet_lock(wallet_addr)
    async with lock:
        if wallet_addr in successful_mints.get(slug, set()):
            return {"success": False, "reason": "already_bought"}
        
        log.info(f"🔫 شراء للمحفظة {wallet_addr[:8]}...")
        
        result = await asyncio.to_thread(
            attempt_purchase_single_wallet,
            w3, pk, wallet_addr,
            contract_address, price_wei, max_per_wallet, remaining,
            eth_price_usd, 0.50,
        )
        
        if result.get("success"):
            if slug not in successful_mints:
                successful_mints[slug] = set()
            successful_mints[slug].add(wallet_addr)
            log.info(f"✅ شراء ناجح للمحفظة {wallet_addr[:8]}...")
            
            # إرسال رسالة تليجرام
            bot_token = wallet_data.get("bot_token")
            chat_id = wallet_data.get("chat_id")
            if bot_token and chat_id:
                try:
                    name = detail.get("collection_name") or slug
                    msg = f"✅ تم الشراء!\n📦 {name}\n👛 {wallet_addr[:6]}...\n🔢 {result.get('quantity', 1)} قطعة"
                    await asyncio.to_thread(
                        requests.post,
                        f"https://api.telegram.org/bot{bot_token}/sendMessage",
                        data={"chat_id": chat_id, "text": msg},
                        timeout=5,
                    )
                except:
                    pass
        else:
            log.warning(f"❌ فشل شراء للمحفظة {wallet_addr[:8]}...: {result.get('reason', 'unknown')}")
        
        return result

async def try_buy(slug: str, chain_key: str, detail: dict):
    """محاولة الشراء"""
    
    # الحصول على المرحلة المجانية
    free_stage = get_free_stage(detail)
    if not free_stage:
        log.info(f"⏳ '{slug}' لا توجد مرحلة مجانية")
        return None
    
    if free_stage["status"] == "imminent":
        log.info(f"⏳ '{slug}' مرحلة مجانية قادمة - انتظار")
        return None
    
    price_wei = free_stage["price_wei"]
    max_per_wallet = free_stage.get("max_per_wallet")
    
    # فحص الكمية المتبقية
    max_supply = int(detail.get("max_supply") or 0)
    total_supply = int(detail.get("total_supply") or 0)
    remaining = max_supply - total_supply
    
    if remaining <= 0:
        log.info(f"❌ '{slug}' نفدت الكمية")
        return None
    
    contract_address = detail.get("contract_address")
    if not contract_address:
        log.warning(f"⚠️ '{slug}' لا يوجد عنوان عقد")
        return None
    
    w3 = W3_INSTANCES.get(chain_key)
    if not w3:
        return None
    
    eth_price_usd = get_eth_price_usd()
    
    # التحقق من المجانية
    if not is_free_or_negligible(price_wei, eth_price_usd):
        price_usd = (price_wei / 1e18) * eth_price_usd
        log.info(f"💰 '{slug}' مدفوع - ${price_usd:.6f}")
        return None
    
    # المحافظ التي لم تشترِ بعد
    bought_wallets = successful_mints.get(slug, set())
    pending_wallets = [w for w in WALLETS_DATA if w["wallet"] not in bought_wallets]
    
    if not pending_wallets:
        log.info(f"✅ جميع المحافظ اشتريت لـ '{slug}'")
        return None
    
    log.info(f"🛒 شراء {slug} لـ {len(pending_wallets)} محافظ")
    
    # شراء لكل محفظة
    tasks = [
        buy_for_wallet(w3, w, slug, contract_address, price_wei, max_per_wallet, remaining, eth_price_usd)
        for w in pending_wallets
    ]
    
    results = await asyncio.gather(*tasks)
    success_count = sum(1 for r in results if r.get("success"))
    log.info(f"📊 تم شراء {success_count} من {len(results)} لمحفظة لـ '{slug}'")
    
    return results

# ==================== معالجة المينتات ====================

async def process_mint(slug: str, chain_key: str):
    """معالجة مينت جديد"""
    
    if slug in in_flight:
        return
    
    if slug in successful_mints and len(successful_mints[slug]) >= len(WALLETS_DATA):
        return
    
    in_flight.add(slug)
    
    try:
        log.info(f"🔍 معالجة: {slug}")
        
        # جلب التفاصيل
        detail = await asyncio.to_thread(fetch_drop_detail, slug)
        if not detail:
            log.warning(f"❌ لا توجد تفاصيل لـ {slug}")
            in_flight.discard(slug)
            return
        
        # تسجيل المينت
        if slug not in discovered_mints:
            discovered_mints.add(slug)
            log.info(f"🆕 مينت جديد: {slug}")
        
        # التحقق من Twitter
        twitter_username = get_cached_twitter(slug)
        if twitter_username is None:
            try:
                twitter_username = await asyncio.to_thread(
                    get_twitter_username_from_opensea,
                    slug,
                    OPENSEA_API_KEY
                )
                set_cached_twitter(slug, twitter_username)
            except:
                pass
        
        if twitter_username is None:
            log.info(f"❌ '{slug}' لا يوجد حساب X")
            in_flight.discard(slug)
            return
        
        # محاولة الشراء
        await try_buy(slug, chain_key, detail)
        
    except Exception as e:
        log.error(f"خطأ في معالجة '{slug}': {e}")
    finally:
        in_flight.discard(slug)

# ==================== الفحص الدوري ====================

async def periodic_scan():
    """فحص دوري للمينتات المكتشفة"""
    while True:
        await asyncio.sleep(CHECK_INTERVAL)
        
        for slug in list(discovered_mints):
            if slug in in_flight:
                continue
            if slug in successful_mints and len(successful_mints[slug]) >= len(WALLETS_DATA):
                continue
            asyncio.create_task(process_mint(slug, "ink"))

# ==================== الاستماع إلى OpenSea ====================

async def listen_opensea():
    """الاستماع إلى أحداث OpenSea"""
    reconnect_attempts = 0
    
    while True:
        try:
            log.info(f"🔄 الاتصال بـ OpenSea... (محاولة #{reconnect_attempts + 1})")
            
            async with websockets.connect(STREAM_URL, ping_interval=None, open_timeout=10) as ws:
                log.info("✅ تم الاتصال بـ OpenSea")
                log.info(f"🚀 مراقبة {len(WALLETS_DATA)} محافظ")
                
                # الاشتراك
                msg_ref = 0
                join_msg = json.dumps([str(msg_ref), str(msg_ref), "collection:*", "phx_join", {}])
                await ws.send(join_msg)
                msg_ref += 1
                
                for event in ["item_transferred", "item_listed", "collection_created"]:
                    event_msg = json.dumps([str(msg_ref), str(msg_ref), f"collection:*:{event}", "phx_join", {}])
                    await ws.send(event_msg)
                    msg_ref += 1
                
                last_heartbeat = time.time()
                events_count = 0
                
                while True:
                    # Heartbeat
                    if time.time() - last_heartbeat > 20:
                        await ws.send(json.dumps([None, str(msg_ref), "phoenix", "heartbeat", {}]))
                        msg_ref += 1
                        last_heartbeat = time.time()
                    
                    try:
                        raw = await asyncio.wait_for(ws.recv(), timeout=5)
                    except asyncio.TimeoutError:
                        continue
                    except websockets.ConnectionClosed:
                        break
                    
                    try:
                        parsed = json.loads(raw)
                    except:
                        continue
                    
                    if isinstance(parsed, list) and len(parsed) >= 4:
                        event_name = parsed[3]
                        events_count += 1
                        
                        if events_count <= 5:
                            log.info(f"📨 حدث #{events_count}: {event_name}")
                        
                        # استخراج slug
                        payload = parsed[4] if len(parsed) > 4 else {}
                        payload = payload.get("payload", {}) if isinstance(payload, dict) else {}
                        
                        collection = payload.get("collection", {})
                        if isinstance(collection, dict):
                            slug = collection.get("slug")
                        else:
                            slug = None
                        
                        if not slug:
                            item = payload.get("item", {})
                            if isinstance(item, dict):
                                collection = item.get("collection", {})
                                if isinstance(collection, dict):
                                    slug = collection.get("slug")
                        
                        if slug:
                            log.info(f"🎯 اكتشاف: {slug}")
                            asyncio.create_task(process_mint(slug, "ink"))
                    
            reconnect_attempts = 0
            
        except Exception as e:
            reconnect_attempts += 1
            log.warning(f"⚠️ انقطع الاتصال: {e}")
            await asyncio.sleep(2)

# ==================== التشغيل ====================

async def run():
    if not BOT_ENABLED:
        log.warning("🔴 BOT_ENABLED=false")
        return
    
    log.info("🚀 تم تشغيل البوت!")
    log.info(f"📊 عدد المحافظ: {len(WALLETS_DATA)}")
    log.info("💰 المينتات: مجانية فقط")
    
    await asyncio.gather(
        listen_opensea(),
        periodic_scan(),
    )

def main():
    while True:
        try:
            asyncio.run(run())
        except KeyboardInterrupt:
            log.info("تم الإيقاف يدوياً.")
            break
        except Exception as e:
            log.critical(f"توقف: {e}")
            import traceback
            traceback.print_exc()
            time.sleep(5)

if __name__ == "__main__":
    main()
