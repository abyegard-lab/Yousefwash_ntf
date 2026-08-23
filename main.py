"""
النظام الرئيسي - شراء المينتات المجانية على Ink
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
)

load_dotenv()

# ============================================
# إعدادات السجلات - تظهر في log فقط
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

INK_RPC_URL = os.environ.get("INK_RPC_URL", "https://rpc-gel.inkonchain.com")
BOT_ENABLED = os.environ.get("BOT_ENABLED", "true").lower() == "true"

# المحافظ
PRIVATE_KEYS = [k.strip() for k in os.environ.get("PRIVATE_KEYS", "").split(",") if k.strip()]
WALLETS = [w.strip() for w in os.environ.get("WALLETS", "").split(",") if w.strip()]

if not PRIVATE_KEYS or not WALLETS:
    log.error("❌ لا توجد محافظ في .env")
    sys.exit(1)

if len(PRIVATE_KEYS) != len(WALLETS):
    log.error(f"❌ عدد المفاتيح ({len(PRIVATE_KEYS)}) لا يتطابق مع عدد المحافظ ({len(WALLETS)})")
    sys.exit(1)

WALLETS_DATA = []
for i in range(len(WALLETS)):
    WALLETS_DATA.append({
        "wallet": WALLETS[i],
        "private_key": PRIVATE_KEYS[i],
    })

log.info(f"💰 تم تحميل {len(WALLETS_DATA)} محافظ")

# ============================================
# إعدادات الشبكة
# ============================================
STREAM_URL = f"wss://stream.openseabeta.com/socket/websocket?token={OPENSEA_API_KEY}&vsn=2.0.0"
DROPS_API_BASE = "https://api.opensea.io/api/v2/drops"

ZERO_ADDRESS = "0x0000000000000000000000000000000000000000"
LOCAL_TZ = timezone(timedelta(hours=3))

HEARTBEAT_INTERVAL = 20
RECV_TIMEOUT = 5
WATCH_POLL_INTERVAL_SECONDS = 15
MAX_GAS_FEE_USD = 0.05

# ============================================
# المتغيرات العامة
# ============================================
W3_INSTANCE = get_web3(INK_RPC_URL)
successful_mints: dict[str, set[str]] = {}
watchlist: dict[str, dict] = {}
in_flight: set[str] = set()
rejected_cooldown: dict[str, float] = {}

total_purchases = 0
total_gas_fee = 0.0
start_time = time.time()

_eth_price_cache = {"value": None, "ts": 0}

# ============================================
# الوظائف المساعدة
# ============================================

def get_eth_price_usd() -> float:
    """جلب سعر ETH من CoinGecko"""
    global _eth_price_cache
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
        log.info(f"💰 سعر ETH: ${price}")
        return price
    except Exception as e:
        log.warning(f"⚠️ تعذر جلب سعر ETH: {e}")
        return _eth_price_cache["value"] or 3000.0


def fetch_drop_detail(slug: str):
    """جلب تفاصيل المينت من OpenSea API"""
    try:
        resp = requests.get(
            f"{DROPS_API_BASE}/{slug}",
            headers={"x-api-key": OPENSEA_API_KEY},
            timeout=10,
        )
        if resp.status_code == 200:
            log.info(f"✅ تفاصيل {slug}: تم الجلب")
            return True, resp.json()
        elif resp.status_code == 404:
            log.warning(f"⚠️ {slug}: غير موجود")
            return False, None
        else:
            log.warning(f"⚠️ {slug}: HTTP {resp.status_code}")
            return None, None
    except Exception as e:
        log.error(f"❌ خطأ في جلب {slug}: {e}")
        return None, None


def parse_iso(ts: str):
    try:
        return datetime.fromisoformat(ts.replace("Z", "+00:00"))
    except Exception:
        return None


def started_today_local(stage: dict) -> bool:
    """التحقق من أن المينت بدأ اليوم"""
    start = parse_iso(stage.get("start_time", ""))
    if not start:
        return False
    today = datetime.now(LOCAL_TZ).date()
    start_date = start.astimezone(LOCAL_TZ).date()
    return start_date == today


def is_free(price_wei: int, eth_price_usd: float) -> bool:
    """التحقق من أن المينت مجاني"""
    if price_wei == 0:
        return True
    price_usd = (price_wei / 1e18) * eth_price_usd
    return price_usd < 0.01


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
    log.info(f"⏸️ {slug}: تم وضعه في التبريد لمدة 120 ثانية")


# ============================================
# وظائف الشراء
# ============================================

async def purchase_task(item, slug, contract_address, price_wei, max_per_wallet, remaining, eth_price_usd):
    """مهمة الشراء لمحفظة واحدة"""
    global total_purchases, total_gas_fee
    
    wallet_addr = item["wallet"]
    pk = item["private_key"]

    lock = get_wallet_lock(wallet_addr)
    async with lock:
        # التحقق من أن المحفظة لم تشترِ بالفعل
        if wallet_addr in successful_mints.get(slug, set()):
            log.info(f"⏭️ {wallet_addr[:8]}... تم الشراء مسبقاً لـ {slug}")
            return

        log.info(f"🔄 {wallet_addr[:8]}... محاولة شراء {slug}")

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
            
            log.info(f"✅✅✅ شراء ناجح! {wallet_addr[:8]}... كمية: {res['quantity']} | غاز: ${res['gas_fee_usd']:.4f}")
            log.info(f"🔗 Hash: {res['tx_hash'][:16]}...")
        else:
            reason = res.get("reason", "unknown")
            log.warning(f"❌ {wallet_addr[:8]}... فشل الشراء: {reason}")

        return res


async def try_buy_now(slug: str, detail: dict):
    """محاولة الشراء لجميع المحافظ"""
    stage = detail.get("active_stage")
    if not stage:
        log.warning(f"⚠️ {slug}: لا يوجد stage نشط")
        return

    # التحقق من الكمية المتبقية
    max_supply = int(detail.get("max_supply") or 0)
    total_supply = int(detail.get("total_supply") or 0)
    remaining = max_supply - total_supply
    
    log.info(f"📊 {slug}: المتبقي {remaining} من {max_supply}")
    
    if remaining <= 0:
        log.warning(f"⚠️ {slug}: نفدت الكمية")
        return

    # جلب عنوان العقد
    contract_address = detail.get("contract_address")
    if not contract_address:
        log.error(f"❌ {slug}: لا يوجد عنوان عقد")
        return

    # جلب السعر من العقد
    eth_price_usd = get_eth_price_usd()
    onchain_price = await asyncio.to_thread(get_onchain_public_price_wei, W3_INSTANCE, contract_address)
    price_wei = onchain_price if onchain_price is not None else int(stage.get("price", "0"))

    # التحقق من المجانية
    if not is_free(price_wei, eth_price_usd):
        log.info(f"⏭️ {slug}: ليس مجانياً - تم التخطي")
        return

    # جلب الحد الأقصى لكل محفظة من العقد
    max_per_wallet = await asyncio.to_thread(get_max_per_wallet, W3_INSTANCE, contract_address)
    if max_per_wallet is None or max_per_wallet <= 0:
        max_per_wallet = int(stage.get("max_total_mintable_by_wallet") or 1)
    
    log.info(f"📊 {slug}: الحد الأقصى لكل محفظة: {max_per_wallet}")

    # المحافظ التي لم تشترِ بعد
    already_bought = successful_mints.get(slug, set())
    pending = [item for item in WALLETS_DATA if item["wallet"] not in already_bought]

    if not pending:
        log.info(f"✅ {slug}: جميع المحافظ اكتملت")
        return

    log.info(f"🔄 {slug}: محاولة الشراء لـ {len(pending)} محافظ")

    # تنفيذ الشراء
    tasks = [
        purchase_task(
            item, slug, contract_address,
            price_wei, max_per_wallet, remaining, eth_price_usd
        )
        for item in pending
    ]

    results = await asyncio.gather(*tasks)
    
    # إحصاء النتائج
    success_count = sum(1 for r in results if r and r.get("success"))
    log.info(f"📊 {slug}: نجح {success_count}/{len(pending)} محافظ")


# ============================================
# المراقبة
# ============================================

async def evaluate_new_mint(slug: str):
    """تقييم مينت جديد"""
    # التحقق من عدم المعالجة مسبقاً
    if slug in watchlist or slug in in_flight or is_in_cooldown(slug):
        return
    
    if len(successful_mints.get(slug, set())) >= len(WALLETS_DATA):
        log.info(f"✅ {slug}: جميع المحافظ اشتريت")
        return

    in_flight.add(slug)
    log.info(f"🔍 تقييم {slug}...")
    
    try:
        found, detail = await asyncio.to_thread(fetch_drop_detail, slug)
        if not found or not detail:
            log.warning(f"⚠️ {slug}: لا توجد تفاصيل")
            return

        if not detail.get("is_minting"):
            log.info(f"⏭️ {slug}: ليس في حالة minting")
            return

        stage = detail.get("active_stage")
        if not stage:
            log.info(f"⏭️ {slug}: لا يوجد stage نشط")
            return

        if not started_today_local(stage):
            log.info(f"⏭️ {slug}: لم يبدأ اليوم")
            return

        contract_address = detail.get("contract_address")
        if not contract_address:
            log.info(f"⏭️ {slug}: لا يوجد عنوان عقد")
            return

        # التحقق من السعر
        eth_price_usd = get_eth_price_usd()
        onchain_price = await asyncio.to_thread(get_onchain_public_price_wei, W3_INSTANCE, contract_address)
        price_wei = onchain_price if onchain_price is not None else int(stage.get("price", "0"))

        if not is_free(price_wei, eth_price_usd):
            log.info(f"⏭️ {slug}: ليس مجانياً - تم التخطي")
            mark_rejected(slug)
            return

        log.info(f"✅✅ {slug}: مينت مجاني نشط - بدء الشراء!")
        await try_buy_now(slug, detail)

        # إذا لم تشترِ جميع المحافظ، ضع في قائمة المراقبة
        if len(successful_mints.get(slug, set())) < len(WALLETS_DATA):
            watchlist[slug] = {"detail": detail}
            log.info(f"👀 {slug}: تمت إضافته للمراقبة")

    except Exception as e:
        log.error(f"❌ خطأ في تقييم {slug}: {e}")
    finally:
        in_flight.discard(slug)


async def watch_loop():
    """حلقة مراقبة المينتات النشطة"""
    last_status_time = time.time()
    
    while True:
        await asyncio.sleep(WATCH_POLL_INTERVAL_SECONDS)
        
        # عرض حالة كل 5 دقائق
        if time.time() - last_status_time >= 300:
            uptime = int(time.time() - start_time)
            hours = uptime // 3600
            minutes = (uptime % 3600) // 60
            
            log.info("=" * 50)
            log.info(f"📊 تقرير الحالة - وقت التشغيل: {hours}h {minutes}m")
            log.info(f"💰 المحافظ: {len(WALLETS_DATA)}")
            log.info(f"✅ عمليات شراء ناجحة: {total_purchases}")
            log.info(f"⛽ إجمالي رسوم الغاز: ${total_gas_fee:.4f}")
            log.info(f"👀 مينتات تحت المراقبة: {len(watchlist)}")
            log.info("=" * 50)
            last_status_time = time.time()
        
        if not watchlist:
            continue

        for slug in list(watchlist.keys()):
            if slug in in_flight:
                continue
                
            if len(successful_mints.get(slug, set())) >= len(WALLETS_DATA):
                log.info(f"✅ {slug}: جميع المحافظ اشتريت - إزالة من المراقبة")
                watchlist.pop(slug, None)
                continue
            
            log.info(f"🔄 مراقبة {slug}...")
            await evaluate_new_mint(slug)


async def listen_opensea():
    """الاستماع لتدفق OpenSea"""
    msg_ref = 0
    while True:
        try:
            log.info("🔄 محاولة الاتصال بـ OpenSea Stream...")
            async with websockets.connect(STREAM_URL, ping_interval=None, open_timeout=15) as ws:
                log.info("✅ متصل بـ OpenSea Stream")
                
                join_ref = str(msg_ref)
                await ws.send(json.dumps([join_ref, join_ref, "collection:*", "phx_join", {}]))
                msg_ref += 1
                last_heartbeat = time.time()

                while True:
                    # إرسال heartbeat
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

                    if not isinstance(parsed, list) or len(parsed) != 5:
                        continue

                    _, _, _, event_name, payload_wrapper = parsed
                    
                    if event_name != "item_transferred":
                        continue

                    payload = (payload_wrapper or {}).get("payload") or {}
                    item = payload.get("item", {}) or {}
                    chain_name = (item.get("chain", {}) or {}).get("name", "")

                    # قبول فقط Ink
                    if chain_name != "ink":
                        continue

                    from_address = ((payload.get("from_account") or {}).get("address", "") or "").lower()
                    if from_address != ZERO_ADDRESS:
                        continue

                    slug = (payload.get("collection", {}) or {}).get("slug", "")
                    if slug:
                        log.info(f"📩 استقبال حدث لـ {slug}")
                        asyncio.create_task(evaluate_new_mint(slug))

        except websockets.ConnectionClosed as e:
            log.warning(f"⚠️ انقطع الاتصال: {e}. إعادة الاتصال...")
        except Exception as e:
            log.error(f"❌ خطأ في الاتصال: {e}. إعادة الاتصال...")
        
        await asyncio.sleep(5)


# ============================================
# التشغيل الرئيسي
# ============================================

async def run():
    """تشغيل النظام"""
    log.info("=" * 50)
    log.info("🚀 بدء تشغيل نظام الشراء التلقائي - Ink")
    log.info(f"💰 عدد المحافظ: {len(WALLETS_DATA)}")
    log.info(f"🔗 RPC: {INK_RPC_URL}")
    log.info(f"🆓 الوضع: المينتات المجانية فقط")
    log.info("=" * 50)
    
    if not BOT_ENABLED:
        log.warning("⚠️ BOT_ENABLED=false - البوت في وضع الإيقاف")
        return
    
    # تشغيل المهام
    await asyncio.gather(
        listen_opensea(),
        watch_loop(),
    )


def main():
    """النقطة الرئيسية"""
    try:
        asyncio.run(run())
    except KeyboardInterrupt:
        log.info("🛑 تم الإيقاف يدوياً")
    except Exception as e:
        log.error(f"💥 خطأ غير متوقع: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()
