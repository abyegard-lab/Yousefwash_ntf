"""
NFT Auto Mint Bot - Main
OpenSea Stream discovery + Polling fallback, multi-wallet parallel mint.
Supports both WebSocket and Polling modes with Telegram notifications.
"""
import os, asyncio, logging, json, time, sqlite3, threading
from pathlib import Path
from typing import Optional, List, Dict, Any
import requests
from dotenv import load_dotenv
from web3 import Web3
from buyer import attempt_purchase

load_dotenv()
logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%H:%M:%S"
)
log = logging.getLogger("main")

# =============================
# قراءة المتغيرات من .env
# =============================

# المتغيرات الأساسية
OPENSEA_API_KEY = os.getenv("OPENSEA_API_KEY", "")
BOT_ENABLED = os.getenv("BOT_ENABLED", "true").lower() == "true"
MAX_GAS_FEE_USD = float(os.getenv("MAX_GAS_FEE_USD", "0.05"))
MAX_BUY_QTY = int(os.getenv("MAX_BUY_QTY", "20"))
MAX_PARALLEL_DISCOVERY = int(os.getenv("MAX_PARALLEL_DISCOVERY", "8"))
POLL_INTERVAL = int(os.getenv("POLL_INTERVAL", "2"))

# الشبكة (يدعم كلا الاسمين)
CHAIN_RPC = os.getenv("INK_RPC_URL", os.getenv("RPC_URL", ""))
if not CHAIN_RPC:
    raise RuntimeError("RPC_URL or INK_RPC_URL is required")

# معرف الشبكة (يُستنتج تلقائياً)
CHAIN_ID = int(os.getenv("CHAIN_ID", "0") or 0)

# قاعدة البيانات
DB_PATH = os.getenv("STATE_DB", os.getenv("DB_PATH", "nft_bot.sqlite3"))

# =============================
# قراءة المحافظ (يدعم كلا الطريقتين)
# =============================

WALLETS = []

# الطريقة الجديدة: قوائم مفصولة بفواصل
private_keys_list = os.getenv("PRIVATE_KEYS", "").split(",")
wallets_list = os.getenv("WALLETS", "").split(",")

for pk, addr in zip(private_keys_list, wallets_list):
    pk = pk.strip()
    addr = addr.strip()
    if pk and addr:
        WALLETS.append({
            "private_key": pk,
            "wallet": Web3.to_checksum_address(addr)
        })

# الطريقة القديمة: WALLET_X_ADDRESS + WALLET_X_PRIVATE_KEY (للتوافق)
if not WALLETS:
    for i in range(1, 21):
        pk = os.getenv(f"WALLET_{i}_PRIVATE_KEY")
        address = os.getenv(f"WALLET_{i}_ADDRESS")
        if pk and address:
            WALLETS.append({
                "private_key": pk,
                "wallet": Web3.to_checksum_address(address)
            })

if not WALLETS:
    log.warning("⚠️ لم يتم العثور على أي محفظة! تأكد من إعداد WALLETS/PRIVATE_KEYS")

# =============================
# إعدادات التيليجرام
# =============================

TELEGRAM_BOT_TOKENS = os.getenv("TELEGRAM_BOT_TOKENS", "").split(",")
TELEGRAM_CHAT_IDS = os.getenv("TELEGRAM_CHAT_IDS", "").split(",")
TELEGRAM_ENABLED = bool(TELEGRAM_BOT_TOKENS and TELEGRAM_CHAT_IDS and TELEGRAM_BOT_TOKENS[0])

# =============================
# الاتصال بـ Web3
# =============================

W3 = Web3(Web3.HTTPProvider(CHAIN_RPC, request_kwargs={"timeout": 8}))
if not W3.is_connected():
    raise RuntimeError(f"RPC_URL is not reachable: {CHAIN_RPC}")

if not CHAIN_ID:
    CHAIN_ID = int(W3.eth.chain_id)
elif int(W3.eth.chain_id) != CHAIN_ID:
    log.warning(f"⚠️ Chain ID mismatch: RPC={W3.eth.chain_id}, expected={CHAIN_ID}")

# =============================
# المتغيرات العامة
# =============================

db_lock = threading.Lock()
wallet_locks = {w["wallet"]: asyncio.Lock() for w in WALLETS}
x_cache = {}
x_inflight = {}
slug_cooldown = {}
purchase_seen = set()
processed_slugs = set()  # لتتبع المجموعات التي تمت معالجتها

# =============================
# دوال قاعدة البيانات
# =============================

def db():
    con = sqlite3.connect(DB_PATH, timeout=30)
    con.execute("""CREATE TABLE IF NOT EXISTS purchases(
        slug TEXT, wallet TEXT, tx_hash TEXT, quantity INTEGER, status TEXT,
        created INTEGER, PRIMARY KEY(slug, wallet)
    )""")
    # جدول للمجموعات المعالجة (لمنع التكرار في وضع Polling)
    con.execute("""CREATE TABLE IF NOT EXISTS processed_slugs(
        slug TEXT PRIMARY KEY, processed_at INTEGER
    )""")
    con.commit()
    return con

def was_purchased(slug: str, wallet: str) -> bool:
    with db_lock:
        con = db()
        row = con.execute(
            "SELECT 1 FROM purchases WHERE slug=? AND wallet=? AND status IN ('sent','success','pending')",
            (slug, wallet)
        ).fetchone()
        con.close()
        return bool(row)

def save_purchase(slug: str, wallet: str, tx_hash: str, quantity: int, status: str):
    with db_lock:
        con = db()
        con.execute(
            "INSERT OR REPLACE INTO purchases (slug, wallet, tx_hash, quantity, status, created) VALUES(?,?,?,?,?,?)",
            (slug, wallet, tx_hash, quantity, status, int(time.time()))
        )
        con.commit()
        con.close()

def mark_slug_processed(slug: str):
    with db_lock:
        con = db()
        con.execute(
            "INSERT OR REPLACE INTO processed_slugs (slug, processed_at) VALUES(?,?)",
            (slug, int(time.time()))
        )
        con.commit()
        con.close()

def is_slug_processed(slug: str) -> bool:
    with db_lock:
        con = db()
        row = con.execute("SELECT 1 FROM processed_slugs WHERE slug=?", (slug,)).fetchone()
        con.close()
        return bool(row)

# =============================
# دوال التيليجرام
# =============================

def send_telegram_message(message: str):
    """إرسال رسالة إلى تيليجرام"""
    if not TELEGRAM_ENABLED:
        return
    
    for token, chat_id in zip(TELEGRAM_BOT_TOKENS, TELEGRAM_CHAT_IDS):
        if not token or not chat_id:
            continue
        try:
            url = f"https://api.telegram.org/bot{token}/sendMessage"
            payload = {
                "chat_id": chat_id.strip(),
                "text": message[:4096],  # حد تيليجرام
                "parse_mode": "HTML"
            }
            requests.post(url, json=payload, timeout=5)
        except Exception as e:
            log.warning(f"فشل إرسال رسالة تيليجرام: {e}")

def notify_purchase(slug: str, wallet: str, tx_hash: str, quantity: int, status: str):
    """إرسال إشعار شراء"""
    if status == "success":
        msg = f"""✅ <b>شراء ناجح!</b>
📦 المجموعة: <code>{slug}</code>
👛 المحفظة: <code>{wallet[:10]}...{wallet[-6:]}</code>
🔢 الكمية: {quantity}
🔗 المعاملة: <code>{tx_hash[:20]}...</code>
🕐 الوقت: {time.strftime('%H:%M:%S')}"""
    else:
        msg = f"""❌ <b>فشل الشراء</b>
📦 المجموعة: <code>{slug}</code>
👛 المحفظة: <code>{wallet[:10]}...{wallet[-6:]}</code>
📊 الحالة: {status}
🕐 الوقت: {time.strftime('%H:%M:%S')}"""
    
    send_telegram_message(msg)

# =============================
# دوال OpenSea API
# =============================

def os_headers():
    return {"X-API-KEY": OPENSEA_API_KEY, "Accept": "application/json"} if OPENSEA_API_KEY else {"Accept": "application/json"}

def get_os_collection(slug: str):
    """جلب معلومات المجموعة من OpenSea"""
    url = f"https://api.opensea.io/api/v2/collections/{slug}"
    try:
        r = requests.get(url, headers=os_headers(), timeout=5)
        if r.status_code == 429:
            return None, "429"
        if r.status_code >= 500:
            return None, str(r.status_code)
        if r.status_code != 200:
            return None, str(r.status_code)
        return r.json(), "ok"
    except requests.RequestException as e:
        return None, "timeout"

def opensea_x(slug: str):
    """البحث عن حساب تويتر/X من OpenSea"""
    now = time.time()
    cached = x_cache.get(slug)
    if cached and cached["expires"] > now:
        return cached["value"], cached["temporary"]
    if slug in x_inflight:
        return None, True

    data, status = get_os_collection(slug)
    if status != "ok":
        log.warning(f"⏳ OpenSea X lookup temporary {status} for {slug}")
        x_inflight[slug] = now
        return None, True

    x = None
    socials = (data or {}).get("socials") or {}
    for key in ("twitter", "x"):
        value = socials.get(key)
        if value:
            x = value
            break
    if not x:
        for key in ("twitter_username", "x_username"):
            value = (data or {}).get(key)
            if value:
                x = value
                break

    x_cache[slug] = {"value": x, "temporary": False, "expires": now + 3600}  # TTL ساعة
    x_inflight.pop(slug, None)
    return x, False

def extract_contract(detail: dict) -> Optional[str]:
    """استخراج عنوان العقد من تفاصيل المجموعة"""
    for key in ("contract_address", "contract", "address"):
        v = detail.get(key)
        if v:
            return v
    return None

def get_remaining_supply(detail: dict) -> int:
    """حساب الكمية المتبقية"""
    remaining = int(detail.get("remaining_supply") or detail.get("remaining") or 0)
    if remaining <= 0:
        max_supply = int(detail.get("max_supply") or 0)
        total = int(detail.get("total_supply") or 0)
        remaining = max(0, max_supply - total)
    return remaining or MAX_BUY_QTY

# =============================
# دوال الشراء الأساسية
# =============================

async def one_wallet(slug: str, contract: str, remaining: int, wallet_data: dict):
    """تنفيذ عملية شراء من محفظة واحدة"""
    wallet = wallet_data["wallet"]
    async with wallet_locks[wallet]:
        if was_purchased(slug, wallet):
            return
        
        try:
            res = await asyncio.to_thread(
                attempt_purchase,
                W3,
                wallet_data["private_key"],
                wallet,
                contract,
                3000,  # ETH_PRICE_USD (يمكن جلبها من API)
                MAX_GAS_FEE_USD,
                remaining,
                MAX_BUY_QTY,
                CHAIN_ID
            )
            
            status = "success" if res.get("success") else ("pending" if res.get("pending") else "failed")
            if res.get("tx_hash"):
                save_purchase(slug, wallet, res["tx_hash"], int(res.get("quantity", 0)), status)
                notify_purchase(slug, wallet, res["tx_hash"], int(res.get("quantity", 0)), status)
            
            if res.get("success"):
                log.info(f"✅ {slug} | wallet={wallet[:10]} | quantity={res['quantity']} | tx={res['tx_hash']}")
            elif res.get("reason") not in ("not_free", "not_started", "phase_ended"):
                log.warning(f"❌ {slug} | wallet={wallet[:10]} | {res.get('reason')}")
                
        except Exception as e:
            log.exception(f"buyer error {slug}/{wallet[:10]}: {e}")

async def process_slug(slug: str, detail: dict):
    """معالجة مجموعة جديدة"""
    # منع التكرار
    now = time.time()
    if now < slug_cooldown.get(slug, 0):
        return
    if is_slug_processed(slug):
        return
    
    slug_cooldown[slug] = now + 5
    
    # البحث عن تويتر
    x, temporary = opensea_x(slug)
    if temporary:
        slug_cooldown[slug] = now + 30
        log.warning(f"⏳ {slug}: OpenSea X lookup temporary failure; will retry")
        return
    if not x:
        log.info(f"❌ {slug}: no X account listed by OpenSea")
        slug_cooldown[slug] = now + 3600
        return
    
    # استخراج العقد
    contract = extract_contract(detail)
    if not contract:
        log.warning(f"❌ {slug}: no contract address")
        return
    
    # حساب الكمية المتبقية
    remaining = get_remaining_supply(detail)
    log.info(f"🎯 {slug}: X={x} | starting on-chain validation | remaining={remaining}")
    
    # تنفيذ الشراء من جميع المحافظ (مع التحكم في التوازي)
    semaphore = asyncio.Semaphore(MAX_PARALLEL_DISCOVERY)
    
    async def limited_wallet(w):
        async with semaphore:
            await one_wallet(slug, contract, remaining, w)
    
    await asyncio.gather(*(limited_wallet(w) for w in WALLETS))
    
    # تسجيل المجموعة كمعالجة
    mark_slug_processed(slug)

# =============================
# وضع WebSocket (الاتصال المباشر)
# =============================

async def stream_loop():
    """الاتصال بـ WebSocket الخاص بـ OpenSea"""
    try:
        import websockets
    except ImportError:
        log.error("❌ websockets package missing. Install: pip install websockets")
        return
    
    while BOT_ENABLED:
        try:
            headers = {"X-API-KEY": OPENSEA_API_KEY} if OPENSEA_API_KEY else {}
            async with websockets.connect(
                "wss://stream.openseabeta.com/socket/websocket",
                additional_headers=headers,
                ping_interval=20,
                ping_timeout=20,
                max_size=4_000_000
            ) as ws:
                log.info("✅ OpenSea Stream connected")
                await ws.send(json.dumps({
                    "topic": "collection:*",
                    "event": "phx_join",
                    "payload": {
                        "config": {"include_erc721": True, "include_erc1155": True},
                        "access_token": OPENSEA_API_KEY
                    },
                    "ref": "1"
                }))
                
                while BOT_ENABLED:
                    try:
                        raw = await asyncio.wait_for(ws.recv(), timeout=90)
                        msg = json.loads(raw)
                        payload = msg.get("payload") or {}
                        data = payload.get("data") or payload
                        slug = data.get("collection_slug") or data.get("collection") or data.get("slug")
                        if slug:
                            asyncio.create_task(process_slug(str(slug), data))
                    except asyncio.TimeoutError:
                        continue
                        
        except Exception as e:
            log.warning(f"Stream disconnected: {e}; reconnecting in 3s")
            await asyncio.sleep(3)

# =============================
# وضع Polling (الفحص الدوري)
# =============================

async def polling_loop():
    """الفحص الدوري للمجموعات الجديدة"""
    log.info("🔄 Starting polling mode...")
    
    # قائمة بالمجموعات الشهيرة أو المطلوبة (يمكن تعديلها)
    popular_slugs = [
        "azuki", "clonex", "otherdeed", "boredapeyachtclub",
        "mutant-ape-yacht-club", "doodles-official", "meebits"
    ]
    
    while BOT_ENABLED:
        try:
            for slug in popular_slugs:
                if not is_slug_processed(slug):
                    # جلب تفاصيل المجموعة
                    data, status = get_os_collection(slug)
                    if status == "ok" and data:
                        await process_slug(slug, data)
                await asyncio.sleep(0.5)  # بين كل مجموعة
            
            await asyncio.sleep(POLL_INTERVAL)
            
        except Exception as e:
            log.error(f"Polling error: {e}")
            await asyncio.sleep(POLL_INTERVAL * 2)

# =============================
# الوظيفة الرئيسية
# =============================

async def main():
    log.info("🚀 NFT Bot Ready")
    log.info(f"📊 Wallets: {len(WALLETS)}")
    log.info(f"💰 Max Gas: ${MAX_GAS_FEE_USD}")
    log.info(f"📦 Max Buy Qty: {MAX_BUY_QTY}")
    log.info(f"⚡ Parallel: {MAX_PARALLEL_DISCOVERY}")
    log.info(f"📱 Telegram: {'✅ Enabled' if TELEGRAM_ENABLED else '❌ Disabled'}")
    
    if TELEGRAM_ENABLED:
        send_telegram_message("🚀 <b>NFT Bot Started</b>\nBot is now active and monitoring for new collections!")
    
    if not WALLETS:
        log.warning("⚠️ No wallets configured! Please add wallets to .env")
        return
    
    if not BOT_ENABLED:
        log.info("⏸️ Bot is disabled (BOT_ENABLED=false)")
        return
    
    # تشغيل كلا الوضعين معاً
    tasks = []
    
    # WebSocket (إذا كان متاحاً)
    try:
        import websockets
        tasks.append(asyncio.create_task(stream_loop()))
        log.info("🌊 WebSocket mode enabled")
    except ImportError:
        log.warning("⚠️ WebSocket not available, using polling mode only")
    
    # Polling (كاحتياطي أو بديل)
    tasks.append(asyncio.create_task(polling_loop()))
    log.info("🔄 Polling mode enabled")
    
    await asyncio.gather(*tasks)

if __name__ == "__main__":
    asyncio.run(main())
