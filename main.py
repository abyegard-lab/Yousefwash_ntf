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
# إعدادات السجلات - مستوى DEBUG لرؤية كل شيء
# ============================================
logging.basicConfig(
    level=logging.DEBUG,  # ✅ DEBUG لرؤية كل التفاصيل
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

last_evaluation_time: dict[str, float] = {}
EVALUATION_COOLDOWN = 10

total_purchases = 0
total_gas_fee = 0.0
start_time = time.time()

_eth_price_cache = {"value": None, "ts": 0}

# ============================================
# الوظائف المساعدة
# ============================================

def get_eth_price_usd() -> float:
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
    log.debug(f"🔍 جلب تفاصيل {slug}...")
    try:
        resp = requests.get(
            f"{DROPS_API_BASE}/{slug}",
            headers={"x-api-key": OPENSEA_API_KEY},
            timeout=10,
        )
        log.debug(f"📡 استجابة {slug}: HTTP {resp.status_code}")
        
        if resp.status_code == 200:
            data = resp.json()
            log.debug(f"✅ تفاصيل {slug}: is_minting={data.get('is_minting')}")
            return True, data
        elif resp.status_code == 404:
            log.warning(f"⚠️ {slug}: غير موجود (404)")
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
    start = parse_iso(stage.get("start_time", ""))
    if not start:
        log.debug(f"⏭️ لا يوجد وقت بداية")
        return False
    today = datetime.now(LOCAL_TZ).date()
    start_date = start.astimezone(LOCAL_TZ).date()
    result = start_date == today
    log.debug(f"📅 بداية المينت: {start_date}, اليوم: {today} → {result}")
    return result


def is_free(price_wei: int, eth_price_usd: float) -> bool:
    if price_wei == 0:
        log.debug(f"💰 السعر: 0 wei → مجاني ✅")
        return True
    price_usd = (price_wei / 1e18) * eth_price_usd
    is_free = price_usd < 0.01
    log.debug(f"💰 السعر: {price_wei} wei (${price_usd:.6f}) → {'مجاني ✅' if is_free else 'مدفوع ❌'}")
    return is_free


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
    log.info(f"⏸️ {slug}: تبريد 120 ثانية")

# ============================================
# وظائف الشراء
# ============================================

async def purchase_task(item, slug, contract_address, price_wei, max_per_wallet, remaining, eth_price_usd):
    global total_purchases, total_gas_fee
    
    wallet_addr = item["wallet"]
    pk = item["private_key"]

    lock = get_wallet_lock(wallet_addr)
    async with lock:
        if wallet_addr in successful_mints.get(slug, set()):
            log.info(f"⏭️ {wallet_addr[:8]}... سبق الشراء لـ {slug}")
            return

        log.info(f"🔄 {wallet_addr[:8]}... شراء {slug} | كمية: {max_per_wallet}")

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
            log.warning(f"❌ {wallet_addr[:8]}... فشل: {reason}")

        return res


async def try_buy_now(slug: str, detail: dict):
    log.info(f"🔄 بدء محاولة الشراء لـ {slug}")
    
    stage = detail.get("active_stage")
    if not stage:
        log.warning(f"⚠️ {slug}: لا يوجد stage")
        return

    # عرض تفاصيل الـ stage
    log.info(f"📋 Stage: {stage}")
    log.info(f"📋 start_time: {stage.get('start_time')}")
    log.info(f"📋 end_time: {stage.get('end_time')}")
    log.info(f"📋 max_total_mintable_by_wallet: {stage.get('max_total_mintable_by_wallet')}")

    max_supply = int(detail.get("max_supply") or 0)
    total_supply = int(detail.get("total_supply") or 0)
    remaining = max_supply - total_supply
    
    log.info(f"📊 {slug}: max_supply={max_supply}, total_supply={total_supply}, remaining={remaining}")
    
    if remaining <= 0:
        log.warning(f"⚠️ {slug}: نفدت الكمية")
        return

    contract_address = detail.get("contract_address")
    log.info(f"📋 contract_address: {contract_address}")
    
    if not contract_address:
        log.error(f"❌ {slug}: لا يوجد عنوان عقد")
        return

    eth_price_usd = get_eth_price_usd()
    
    # جلب السعر من العقد
    log.info(f"🔄 جلب السعر من العقد {contract_address[:8]}...")
    onchain_price = await asyncio.to_thread(get_onchain_public_price_wei, W3_INSTANCE, contract_address)
    price_wei = onchain_price if onchain_price is not None else int(stage.get("price", "0"))
    log.info(f"💰 السعر: {price_wei} wei")

    if not is_free(price_wei, eth_price_usd):
        log.info(f"⏭️ {slug}: ليس مجانياً - تخطي")
        return

    # جلب الحد الأقصى من العقد
    log.info(f"🔄 جلب max_per_wallet من العقد...")
    max_per_wallet = await asyncio.to_thread(get_max_per_wallet, W3_INSTANCE, contract_address)
    if max_per_wallet is None or max_per_wallet <= 0:
        max_per_wallet = int(stage.get("max_total_mintable_by_wallet") or 1)
    
    log.info(f"📊 {slug}: حد المحفظة: {max_per_wallet}")

    already_bought = successful_mints.get(slug, set())
    pending = [item for item in WALLETS_DATA if item["wallet"] not in already_bought]

    if not pending:
        log.info(f"✅ {slug}: جميع المحافظ اكتملت")
        return

    log.info(f"🔄 {slug}: شراء لـ {len(pending)} محافظ: {[w['wallet'][:8] for w in pending]}")

    tasks = [
        purchase_task(
            item, slug, contract_address,
            price_wei, max_per_wallet, remaining, eth_price_usd
        )
        for item in pending
    ]

    results = await asyncio.gather(*tasks)
    success_count = sum(1 for r in results if r and r.get("success"))
    log.info(f"📊 {slug}: نجح {success_count}/{len(pending)}")


# ============================================
# المراقبة
# ============================================

async def evaluate_new_mint(slug: str):
    """تقييم مينت جديد - مع منع التكرار"""
    
    log.info(f"🔍 بدء تقييم {slug}...")
    
    # منع التكرار
    now = time.time()
    last_eval = last_evaluation_time.get(slug, 0)
    if now - last_eval < EVALUATION_COOLDOWN:
        log.info(f"⏭️ {slug}: تقييم متكرر ({now - last_eval:.1f}s) - تخطي")
        return
    last_evaluation_time[slug] = now
    
    # التحقق من الشروط
    if slug in watchlist:
        log.info(f"⏭️ {slug}: موجود في قائمة المراقبة")
        return
    
    if slug in in_flight:
        log.info(f"⏭️ {slug}: قيد المعالجة")
        return
    
    if is_in_cooldown(slug):
        log.info(f"⏭️ {slug}: في فترة التبريد")
        return
    
    if len(successful_mints.get(slug, set())) >= len(WALLETS_DATA):
        log.info(f"✅ {slug}: جميع المحافظ اكتملت")
        return

    in_flight.add(slug)
    log.info(f"🔍 تقييم {slug}...")
    
    try:
        found, detail = await asyncio.to_thread(fetch_drop_detail, slug)
        
        if not found:
            log.warning(f"⚠️ {slug}: غير موجود")
            mark_rejected(slug)
            return
            
        if not detail:
            log.warning(f"⚠️ {slug}: لا توجد تفاصيل")
            mark_rejected(slug)
            return

        # عرض تفاصيل المينت
        log.info(f"📋 {slug} - is_minting: {detail.get('is_minting')}")
        log.info(f"📋 {slug} - collection_name: {detail.get('collection_name')}")
        log.info(f"📋 {slug} - contract_address: {detail.get('contract_address')}")

        if not detail.get("is_minting"):
            log.info(f"⏭️ {slug}: ليس في حالة minting")
            return

        stage = detail.get("active_stage")
        if not stage:
            log.info(f"⏭️ {slug}: لا يوجد stage")
            return

        log.info(f"📋 {slug} - stage: start={stage.get('start_time')}, end={stage.get('end_time')}")

        if not started_today_local(stage):
            log.info(f"⏭️ {slug}: لم يبدأ اليوم")
            return

        contract_address = detail.get("contract_address")
        if not contract_address:
            log.info(f"⏭️ {slug}: لا يوجد عنوان عقد")
            return

        eth_price_usd = get_eth_price_usd()
        onchain_price = await asyncio.to_thread(get_onchain_public_price_wei, W3_INSTANCE, contract_address)
        price_wei = onchain_price if onchain_price is not None else int(stage.get("price", "0"))

        if not is_free(price_wei, eth_price_usd):
            log.info(f"⏭️ {slug}: ليس مجانياً - تخطي")
            mark_rejected(slug)
            return

        log.info(f"✅✅ {slug}: مينت مجاني نشط - بدء الشراء!")
        await try_buy_now(slug, detail)

        if len(successful_mints.get(slug, set())) < len(WALLETS_DATA):
            watchlist[slug] = {"detail": detail}
            log.info(f"👀 {slug}: تمت إضافته للمراقبة")

    except Exception as e:
        log.error(f"❌ خطأ في تقييم {slug}: {e}")
        import traceback
        traceback.print_exc()
    finally:
        in_flight.discard(slug)


async def watch_loop():
    """حلقة مراقبة المينتات النشطة"""
    last_status_time = time.time()
    status_interval = 300
    
    while True:
        await asyncio.sleep(WATCH_POLL_INTERVAL_SECONDS)
        
        if time.time() - last_status_time >= status_interval:
            uptime = int(time.time() - start_time)
            hours = uptime // 3600
            minutes = (uptime % 3600) // 60
            
            log.info("=" * 50)
            log.info(f"📊 تقرير الحالة - {hours}h {minutes}m")
            log.info(f"💰 المحافظ: {len(WALLETS_DATA)}")
            log.info(f"✅ شراء ناجح: {total_purchases}")
            log.info(f"⛽ رسوم الغاز: ${total_gas_fee:.4f}")
            log.info(f"👀 تحت المراقبة: {len(watchlist)}")
            log.info(f"📌 المينتات الناجحة: {list(successful_mints.keys())}")
            log.info("=" * 50)
            last_status_time = time.time()
        
        if not watchlist:
            continue

        for slug in list(watchlist.keys()):
            if slug in in_flight:
                continue
                
            if len(successful_mints.get(slug, set())) >= len(WALLETS_DATA):
                log.info(f"✅ {slug}: جميع المحافظ اكتملت - إزالة")
                watchlist.pop(slug, None)
                continue
            
            now = time.time()
            last_eval = last_evaluation_time.get(slug, 0)
            if now - last_eval < EVALUATION_COOLDOWN:
                continue
            
            await evaluate_new_mint(slug)


async def listen_opensea():
    """الاستماع لتدفق OpenSea"""
    msg_ref = 0
    processed_slugs: dict[str, float] = {}
    MIN_INTERVAL = 3  # زيادة إلى 3 ثواني
    
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

                    if chain_name != "ink":
                        continue

                    from_address = ((payload.get("from_account") or {}).get("address", "") or "").lower()
                    if from_address != ZERO_ADDRESS:
                        continue

                    slug = (payload.get("collection", {}) or {}).get("slug", "")
                    if not slug:
                        continue

                    # منع التكرار
                    now = time.time()
                    if slug in processed_slugs and now - processed_slugs[slug] < MIN_INTERVAL:
                        continue
                    processed_slugs[slug] = now

                    log.info(f"📩 استقبال {slug} - بدء التقييم")
                    
                    # ✅ معالجة مباشرة بدون تأخير
                    asyncio.create_task(evaluate_new_mint(slug))

        except websockets.ConnectionClosed as e:
            log.warning(f"⚠️ انقطع الاتصال: {e}")
        except Exception as e:
            log.error(f"❌ خطأ: {e}")
        
        log.info("🔄 إعادة الاتصال بعد 5 ثوان...")
        await asyncio.sleep(5)


# ============================================
# التشغيل الرئيسي
# ============================================

async def run():
    log.info("=" * 50)
    log.info("🚀 بدء تشغيل نظام الشراء التلقائي - Ink")
    log.info(f"💰 عدد المحافظ: {len(WALLETS_DATA)}")
    log.info(f"🔗 RPC: {INK_RPC_URL}")
    log.info(f"🆓 الوضع: المينتات المجانية فقط")
    log.info("=" * 50)
    
    if not BOT_ENABLED:
        log.warning("⚠️ BOT_ENABLED=false")
        return
    
    await asyncio.gather(
        listen_opensea(),
        watch_loop(),
    )


def main():
    try:
        asyncio.run(run())
    except KeyboardInterrupt:
        log.info("🛑 إيقاف")
    except Exception as e:
        log.error(f"💥 خطأ: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()
