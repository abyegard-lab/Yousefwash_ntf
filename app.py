"""
NFT LISTING SYSTEM - الإصدار النهائي مع التحكم في رسوم الغاز
يدعم Robinhood Chain
يعمل مع EIP-1559 لحل مشاكل الغاز
"""

import asyncio
import logging
import os
import time
from collections import defaultdict
from datetime import datetime
from typing import Optional, Dict, List

import aiohttp
from web3 import Web3
from dotenv import load_dotenv
from eth_account import Account

load_dotenv()

# ============================================================
# ENV
# ============================================================

OPENSEA_API_KEY = os.getenv("OPENSEA_API_KEY", "").strip()
PRIVATE_KEY = os.getenv("PRIVATE_KEY", "").strip()
WALLET_ADDRESS = os.getenv("WALLET_ADDRESS", "").strip()
ETHEREUM_RPC_URL = os.getenv("ETHEREUM_RPC_URL", "").strip()
ROBINHOOD_RPC_URL = os.getenv("ROBINHOOD_RPC_URL", "").strip()
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "").strip()

# ============================================================
# SETTINGS
# ============================================================

READ_DELAY = 0.5
WRITE_DELAY = 3
ETH_PRICE_USD = 3000
CYCLE_INTERVAL = 300

# ✅ السعر الافتراضي إذا لم يوجد سعر في السوق: 5 دولار
DEFAULT_PRICE_USD = 5.0
DEFAULT_PRICE_ETH = DEFAULT_PRICE_USD / ETH_PRICE_USD  # ≈ 0.001667 ETH

# ✅ حد أقصى لرسوم الغاز: 0.03 دولار
MAX_GAS_FEE_USD = 0.03

# ============================================================
# LOGGING
# ============================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

log = logging.getLogger("nft-lister")

# ============================================================
# CHAINS
# ============================================================

CHAINS = {
    "ethereum": {
        "name": "Ethereum",
        "api_chain": "ethereum",
        "rpc": ETHEREUM_RPC_URL,
        "chain_id": 1,
        "currency": "ETH",
        "seaport": "0x00000000006c3852cbEf3e08E8dF289169EdE581",
        "enabled": False,
    },
    "robinhood": {
        "name": "Robinhood",
        "api_chain": "robinhood",
        "rpc": ROBINHOOD_RPC_URL,
        "chain_id": 4663,
        "currency": "ETH",
        "seaport": "0x0000000000000068F116a894984e2DB1123eB395",
        "enabled": True,
    },
}

# ✅ conduit key الصحيح لـ OpenSea (Seaport 1.5)
OPENSEA_CONDUIT_KEY = "0x61159fefdfada89302ed55f8b9e89e2d67d8258712b3a3f89aa88525877f1d5e"

# ✅ ConduitController موجود على نفس العنوان في جميع الشبكات (CREATE2)
CONDUIT_CONTROLLER = "0x00000000F9490004C11Cef243f5400493c00Ad63"

# كاش عنوان Conduit لتجنب طلبه مرتين
conduit_address_cache: Dict[str, str] = {}

ENABLED_CHAINS = [
    chain
    for chain, config in CHAINS.items()
    if config.get("enabled", False) and config["rpc"]
]

if not ENABLED_CHAINS:
    raise SystemExit("❌ لا توجد شبكة مفعلة")

# ============================================================
# CACHE & STATS
# ============================================================

approval_cache = {}
floor_price_cache = {}
processed_nfts = set()

stats = {
    "total": 0,
    "collections": 0,
    "processed": 0,
    "listed": 0,
    "failed": 0,
    "current_collection": "",
}

# ============================================================
# TELEGRAM
# ============================================================

telegram_queue = asyncio.Queue()

def telegram(message: str, is_error: bool = False):
    if TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID:
        if is_error:
            message = f"⚠️ <b>خطأ</b>\n{message}"
        telegram_queue.put_nowait(message)

async def telegram_worker():
    if not (TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID):
        return

    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"

    async with aiohttp.ClientSession() as session:
        while True:
            try:
                message = await telegram_queue.get()
                data = {
                    "chat_id": TELEGRAM_CHAT_ID,
                    "text": message,
                    "parse_mode": "HTML",
                }
                async with session.post(url, data=data, timeout=15) as response:
                    if response.status == 429:
                        await asyncio.sleep(5)
                        telegram_queue.put_nowait(message)
                    elif response.status != 200:
                        log.warning(f"Telegram HTTP {response.status}")
                await asyncio.sleep(1.5)
            except Exception as e:
                log.error(f"Telegram error: {e}")
                await asyncio.sleep(5)

# ============================================================
# WEB3
# ============================================================

def get_web3(chain: str) -> Optional[Web3]:
    config = CHAINS.get(chain)
    if not config or not config["rpc"]:
        return None

    try:
        provider = Web3.HTTPProvider(
            config["rpc"],
            request_kwargs={"timeout": 30}
        )
        client = Web3(provider)
        if client.is_connected():
            return client
    except Exception as e:
        log.error(f"{chain} RPC: {e}")
    return None

def checksum(address: str) -> Optional[str]:
    try:
        return Web3.to_checksum_address(address)
    except Exception:
        return None

def api_headers():
    return {
        "x-api-key": OPENSEA_API_KEY,
        "Content-Type": "application/json",
    }

# ============================================================
# FETCH NFTs
# ============================================================

async def fetch_chain_nfts(session: aiohttp.ClientSession, chain: str) -> List[Dict]:
    result = []
    config = CHAINS[chain]
    cursor = None
    url = f"https://api.opensea.io/api/v2/chain/{config['api_chain']}/account/{WALLET_ADDRESS}/nfts"

    while True:
        params = {"limit": 200}
        if cursor:
            params["next"] = cursor

        try:
            async with session.get(
                url,
                headers=api_headers(),
                params=params,
                timeout=30,
            ) as response:
                if response.status == 429:
                    log.warning(f"⚠️ {config['name']}: Rate limit")
                    await asyncio.sleep(10)
                    continue

                if response.status != 200:
                    log.error(f"❌ {config['name']}: HTTP {response.status}")
                    break

                data = await response.json()
                for nft in data.get("nfts", []):
                    contract = nft.get("contract")
                    token_id = nft.get("identifier")
                    if not contract or token_id is None:
                        continue

                    collection_raw = nft.get("collection")
                    if isinstance(collection_raw, dict):
                        collection_slug = collection_raw.get("slug") or "unknown"
                    elif isinstance(collection_raw, str) and collection_raw:
                        # ✅ OpenSea API أحياناً يرجع slug مباشرة كـ string
                        collection_slug = collection_raw
                    else:
                        collection_slug = "unknown"

                    result.append({
                        "chain": chain,
                        "contract": contract,
                        "token_id": str(token_id),
                        "name": nft.get("name") or f"#{token_id}",
                        "image_url": nft.get("image_url", ""),
                        "collection": collection_slug,
                    })

                cursor = data.get("next")
                if not cursor:
                    break

                await asyncio.sleep(READ_DELAY)

        except asyncio.TimeoutError:
            log.error(f"❌ {config['name']}: انتهت المهلة في جلب NFTs")
            break
        except Exception as e:
            log.error(f"❌ Fetch {chain}: {e}")
            break

    return result

async def fetch_all_nfts(session):
    all_nfts = []
    for chain in ENABLED_CHAINS:
        log.info(f"📥 جلب NFTs من {CHAINS[chain]['name']}")
        nfts = await fetch_chain_nfts(session, chain)
        log.info(f"   → {len(nfts)} NFT")
        all_nfts.extend(nfts)
    return all_nfts

def group_collections(nfts: List[Dict]):
    groups = defaultdict(list)
    for nft in nfts:
        key = (nft["chain"], nft["collection"])
        groups[key].append(nft)
    return groups

# حد أقصى للسعر الواحد (لا يعقل تسعير NFT بمليار دولار)
MAX_LISTING_PRICE_USD = 500.0

# كاش لمعلومات السعر: {slug: {"price_usdg": float, "price_eth": float, "is_usd": bool}}
collection_price_info_cache: Dict[str, Dict] = {}

async def get_collection_floor_price(session, collection_slug: str, api_chain: str) -> Dict:
    """
    جلب سعر السوق لـ Collection مع معلومات العملة.
    يُرجع: {"price_eth": float, "price_usd": float, "is_usd_currency": bool, "has_floor_price": bool}
    """
    no_price_info = {"price_eth": 0, "price_usd": 0, "is_usd_currency": False, "has_floor_price": False}

    if not collection_slug or collection_slug == "unknown":
        return no_price_info

    if collection_slug in collection_price_info_cache:
        return collection_price_info_cache[collection_slug]

    url = f"https://api.opensea.io/api/v2/collections/{collection_slug}/stats"
    try:
        async with session.get(url, headers=api_headers(), timeout=15) as response:
            if response.status == 200:
                data = await response.json()
                total_stats = data.get("total", {})
                floor = total_stats.get("floor_price", 0)
                symbol = str(total_stats.get("floor_price_symbol", "ETH")).upper()

                if floor and float(floor) > 0:
                    floor_val = float(floor)
                    is_usd = "USD" in symbol or symbol in ["DAI"]

                    if is_usd:
                        price_usd = floor_val
                        price_eth = floor_val / ETH_PRICE_USD
                    else:
                        price_eth = floor_val
                        price_usd = floor_val * ETH_PRICE_USD

                    # ✅ سقف السعر: إذا كان السعر وهمياً (أكثر من الحد الأقصى) → لا يوجد سعر حقيقي
                    if price_usd > MAX_LISTING_PRICE_USD:
                        log.warning(f"   ⚠️ سعر السوق لـ {collection_slug} وهمي ({price_usd:.2f}$) → لا يوجد سعر حقيقي")
                        info = no_price_info
                    else:
                        info = {"price_eth": price_eth, "price_usd": price_usd, "is_usd_currency": is_usd, "has_floor_price": True}
                        log.info(f"   📈 سعر السوق لـ {collection_slug}: {price_usd:.2f}$ = {price_eth:.6f} ETH (العملة: {symbol})")

                    collection_price_info_cache[collection_slug] = info
                    return info
            else:
                log.warning(f"   ⚠️ لم يتم العثور على سعر السوق لـ {collection_slug} ({response.status})")
    except Exception as e:
        log.warning(f"   ⚠️ فشل جلب سعر السوق لـ {collection_slug}: {e}")

    # لا يوجد سعر في السوق
    collection_price_info_cache[collection_slug] = no_price_info
    return no_price_info

# ============================================================
# ✅ CONDUIT - جلب عنوان Conduit من الـ blockchain
# ============================================================

async def get_conduit_address(chain: str) -> Optional[str]:
    """جلب عنوان Conduit الفعلي من ConduitController على الـ blockchain"""
    if chain in conduit_address_cache:
        return conduit_address_cache[chain]

    try:
        client = get_web3(chain)
        if not client:
            return None

        controller_abi = [{
            "inputs": [{"internalType": "bytes32", "name": "conduitKey", "type": "bytes32"}],
            "name": "getConduit",
            "outputs": [
                {"internalType": "address", "name": "conduit", "type": "address"},
                {"internalType": "bool", "name": "exists", "type": "bool"},
            ],
            "stateMutability": "view",
            "type": "function",
        }]

        controller = client.eth.contract(
            address=checksum(CONDUIT_CONTROLLER),
            abi=controller_abi,
        )

        key_bytes = bytes.fromhex(OPENSEA_CONDUIT_KEY.replace("0x", ""))
        conduit_addr, exists = controller.functions.getConduit(key_bytes).call()

        if exists and conduit_addr and conduit_addr != "0x0000000000000000000000000000000000000000":
            result = checksum(conduit_addr)
            conduit_address_cache[chain] = result
            log.info(f"   🔗 Conduit address ({chain}): {result}")
            return result
        else:
            log.warning(f"   ⚠️ Conduit غير موجود لـ conduitKey على {chain}")
            return None
    except Exception as e:
        log.warning(f"   ⚠️ فشل جلب Conduit من Controller: {e}")
        return None


async def ensure_approval(nft: Dict):
    chain = nft["chain"]
    contract_address = checksum(nft["contract"])
    owner = checksum(WALLET_ADDRESS)

    # ✅ جلب عنوان Conduit الفعلي من ConduitController على الـ blockchain
    operator = await get_conduit_address(chain)
    if not operator:
        # fallback للـ seaport إذا فشل جلب Conduit
        operator = checksum(CHAINS[chain]["seaport"])
        log.warning(f"   ⚠️ يتم استخدام Seaport بدل Conduit (fallback)")

    if not contract_address:
        return False, "Contract غير صالح"

    cache_key = f"{chain}:{contract_address}:{operator}"
    if cache_key in approval_cache:
        return approval_cache[cache_key], "من الكاش"

    client = get_web3(chain)
    if not client:
        return False, "فشل الاتصال بـ RPC"

    try:
        balance_wei = client.eth.get_balance(owner)
        balance_eth = balance_wei / 1e18
        log.info(f"💰 رصيد المحفظة: {balance_eth:.4f} {CHAINS[chain]['currency']}")
        if balance_wei == 0:
            return False, "الرصيد صفر"
    except Exception as e:
        return False, f"فشل قراءة الرصيد: {str(e)[:50]}"

    abi = [
        {
            "constant": True,
            "inputs": [{"name": "_owner", "type": "address"}, {"name": "_operator", "type": "address"}],
            "name": "isApprovedForAll",
            "outputs": [{"name": "", "type": "bool"}],
            "type": "function",
        },
        {
            "constant": False,
            "inputs": [{"name": "_operator", "type": "address"}, {"name": "_approved", "type": "bool"}],
            "name": "setApprovalForAll",
            "outputs": [],
            "type": "function",
        },
    ]

    try:
        contract = client.eth.contract(address=contract_address, abi=abi)

        try:
            approved = contract.functions.isApprovedForAll(owner, operator).call()
            if approved:
                approval_cache[cache_key] = True
                log.info(f"   ✅ موافق مسبقاً")
                return True, "موافق مسبقاً"
        except Exception as e:
            return False, f"فشل التحقق من الموافقة: {str(e)[:50]}"

        # ✅ EIP-1559
        nonce = client.eth.get_transaction_count(owner)
        latest_block = client.eth.get_block('latest')
        base_fee = latest_block.get('baseFeePerGas', 0)
        max_priority_fee = client.eth.max_priority_fee
        max_fee = int(base_fee * 1.5) + max_priority_fee

        tx = contract.functions.setApprovalForAll(operator, True).build_transaction({
            "from": owner,
            "nonce": nonce,
            "maxFeePerGas": max_fee,
            "maxPriorityFeePerGas": max_priority_fee,
            "type": 2,
        })

        try:
            estimated_gas = client.eth.estimate_gas(tx)
            tx["gas"] = int(estimated_gas * 1.2)
        except Exception as e:
            tx["gas"] = 150000

        # ✅ حساب رسوم الغاز
        gas_eth = tx["gas"] * max_fee / 1e18
        gas_usd = gas_eth * ETH_PRICE_USD
        log.info(f"   ⛽ رسوم الموافقة: ${gas_usd:.4f} ({gas_eth:.6f} ETH)")

        # ✅ التحقق من حد رسوم الغاز (0.03 دولار)
        if gas_usd > MAX_GAS_FEE_USD:
            log.warning(f"   ⚠️ رسوم الغاز مرتفعة: ${gas_usd:.4f} > ${MAX_GAS_FEE_USD:.2f}")
            telegram(
                f"⚠️ <b>رسوم غاز مرتفعة</b>\n"
                f"🖼️ {nft.get('name', 'NFT')}\n"
                f"⛽ ${gas_usd:.4f} > ${MAX_GAS_FEE_USD:.2f}",
                is_error=True
            )
            return False, f"رسوم الغاز مرتفعة: ${gas_usd:.4f}"

        if balance_wei < tx["gas"] * max_fee:
            return False, f"الرصيد غير كافٍ"

        signed = client.eth.account.sign_transaction(tx, PRIVATE_KEY)
        raw_tx = getattr(signed, "raw_transaction", None)
        if raw_tx is None:
            raw_tx = getattr(signed, "rawTransaction")

        tx_hash = client.eth.send_raw_transaction(raw_tx)
        log.info(f"   ⛽ جاري تأكيد الموافقة: {tx_hash.hex()[:10]}...")

        receipt = client.eth.wait_for_transaction_receipt(tx_hash, timeout=180)
        success = receipt.status == 1
        approval_cache[cache_key] = success

        if success:
            log.info(f"   ✅ تمت الموافقة (${gas_usd:.4f})")
            return True, f"تمت الموافقة (${gas_usd:.4f})"
        else:
            return False, "فشلت معاملة Approval"

    except Exception as e:
        return False, str(e)[:200]

# ============================================================
# ✅ CREATE LISTING - يعمل مع Robinhood
# ============================================================
async def get_seaport_counter(chain: str, owner: str) -> int:
    """جلب counter الحقيقي من عقد Seaport على الـ blockchain"""
    try:
        client = get_web3(chain)
        if not client:
            return 0
        seaport_abi = [{
            "inputs": [{"internalType": "address", "name": "offerer", "type": "address"}],
            "name": "getCounter",
            "outputs": [{"internalType": "uint256", "name": "counter", "type": "uint256"}],
            "stateMutability": "view",
            "type": "function",
        }]
        seaport = client.eth.contract(
            address=checksum(CHAINS[chain]["seaport"]),
            abi=seaport_abi,
        )
        counter = seaport.functions.getCounter(owner).call()
        log.info(f"   🔢 Seaport counter: {counter}")
        return counter
    except Exception as e:
        log.warning(f"   ⚠️ فشل جلب counter، سيُستخدم 0: {e}")
        return 0


async def create_listing(session, nft, price_eth: float, is_usd_currency: bool = False, price_usd: float = None):
    chain = nft["chain"]
    config = CHAINS[chain]
    api_chain = config["api_chain"]
    chain_id = config["chain_id"]
    seaport_address = checksum(config["seaport"])

    # عنوان عملة ERC20 المطلوب من المجموعة (USDG على Robinhood)
    currency_address = checksum("0x5fc5360d0400a0fd4f2af552add042d716f1d168")
    # ✅ USDG يستخدم 6 خانات عشرية (مثل USDC/USDT) وليس 18
    USDG_DECIMALS = 6

    # zone الصحيح لهذه الشبكة
    zone_address_str = "0x000056f7000000ece9003ca63978907a00ffd100"
    zone_address = checksum(zone_address_str)

    token_id_int = int(nft["token_id"])
    contract_address = checksum(nft["contract"])
    owner = checksum(WALLET_ADDRESS)

    # ✅ حساب price_wei بالـ USDG (6 خانات عشرية دائماً لأن عملة Robinhood هي USDG)
    # السعر بالدولار مقرّب لـ 2 خانات عشرية كحد أقصى (شرط OpenSea)
    if is_usd_currency and price_usd is not None:
        listing_price_usd = round(price_usd, 2)
    elif price_eth and price_eth > 0:
        # تحويل ETH → USD → USDG
        listing_price_usd = round(price_eth * ETH_PRICE_USD, 2)
    else:
        listing_price_usd = round(DEFAULT_PRICE_USD, 2)

    price_wei = int(listing_price_usd * (10 ** USDG_DECIMALS))

    # رسوم OpenSea = 100 نقطة أساس (1%)
    opensea_fee_basis_points = 100
    opensea_fee_amount = int(price_wei * opensea_fee_basis_points / 10000)
    owner_amount = price_wei - opensea_fee_amount

    # ✅ جلب counter الحقيقي من الـ chain (لصحة التوقيع)
    counter = await get_seaport_counter(chain, owner)

    now = int(time.time())
    salt_val = int(time.time() * 1000)

    start_time_int = now
    end_time_int = now + 86400

    # ERC20 itemType = 1
    erc20_item_type = 1

    conduit_key_hex = OPENSEA_CONDUIT_KEY
    conduit_key_bytes = bytes.fromhex(conduit_key_hex.replace("0x", ""))
    zone_hash_bytes = bytes(32)

    # بناء parameters للتوقيع EIP-712
    sign_parameters = {
        "offerer": owner,
        "zone": zone_address,
        "zoneHash": zone_hash_bytes,
        "startTime": start_time_int,
        "endTime": end_time_int,
        "orderType": 2,
        "salt": salt_val,
        "conduitKey": conduit_key_bytes,
        "counter": counter,
        "offer": [
            {
                "itemType": 2,
                "token": contract_address,
                "identifierOrCriteria": token_id_int,
                "startAmount": 1,
                "endAmount": 1,
            }
        ],
        "consideration": [
            {
                "itemType": erc20_item_type,
                "token": currency_address,
                "identifierOrCriteria": 0,
                "startAmount": owner_amount,
                "endAmount": owner_amount,
                "recipient": owner,
            },
            {
                "itemType": erc20_item_type,
                "token": currency_address,
                "identifierOrCriteria": 0,
                "startAmount": opensea_fee_amount,
                "endAmount": opensea_fee_amount,
                "recipient": checksum("0x0000a26b00c1F0DF003000390027140000fAa719"),
            },
        ],
    }

    # التوقيع EIP-712 (Seaport v1.6)
    try:
        account = Account.from_key(PRIVATE_KEY)

        domain = {
            "name": "Seaport",
            "version": "1.6",  # ✅ Seaport 1.6 على عقد 0x0000000000000068F116a894984e2DB1123eB395
            "chainId": chain_id,
            "verifyingContract": seaport_address,
        }

        types = {
            "OrderComponents": [
                {"name": "offerer", "type": "address"},
                {"name": "zone", "type": "address"},
                {"name": "offer", "type": "OfferItem[]"},
                {"name": "consideration", "type": "ConsiderationItem[]"},
                {"name": "orderType", "type": "uint8"},
                {"name": "startTime", "type": "uint256"},
                {"name": "endTime", "type": "uint256"},
                {"name": "zoneHash", "type": "bytes32"},
                {"name": "salt", "type": "uint256"},
                {"name": "conduitKey", "type": "bytes32"},
                {"name": "counter", "type": "uint256"},
            ],
            "OfferItem": [
                {"name": "itemType", "type": "uint8"},
                {"name": "token", "type": "address"},
                {"name": "identifierOrCriteria", "type": "uint256"},
                {"name": "startAmount", "type": "uint256"},
                {"name": "endAmount", "type": "uint256"},
            ],
            "ConsiderationItem": [
                {"name": "itemType", "type": "uint8"},
                {"name": "token", "type": "address"},
                {"name": "identifierOrCriteria", "type": "uint256"},
                {"name": "startAmount", "type": "uint256"},
                {"name": "endAmount", "type": "uint256"},
                {"name": "recipient", "type": "address"},
            ],
        }

        signed = account.sign_typed_data(domain, types, sign_parameters)
        signature = signed.signature.hex()
        if not signature.startswith("0x"):
            signature = "0x" + signature

    except Exception as e:
        return False, f"فشل التوقيع: {e}"

    # ✅ Payload للـ API (الحقول strings للأرقام الكبيرة)
    api_parameters = {
        "offerer": owner,
        "zone": zone_address_str,
        "zoneHash": "0x0000000000000000000000000000000000000000000000000000000000000000",
        "startTime": str(start_time_int),
        "endTime": str(end_time_int),
        "orderType": 2,
        "salt": str(salt_val),
        "conduitKey": conduit_key_hex,
        "counter": str(counter),
        "offer": [
            {
                "itemType": 2,
                "token": contract_address,
                "identifierOrCriteria": str(token_id_int),
                "startAmount": "1",
                "endAmount": "1",
            }
        ],
        "consideration": [
            {
                "itemType": erc20_item_type,
                "token": currency_address,
                "identifierOrCriteria": "0",
                "startAmount": str(owner_amount),
                "endAmount": str(owner_amount),
                "recipient": owner,
            },
            {
                "itemType": erc20_item_type,
                "token": currency_address,
                "identifierOrCriteria": "0",
                "startAmount": str(opensea_fee_amount),
                "endAmount": str(opensea_fee_amount),
                "recipient": checksum("0x0000a26b00c1F0DF003000390027140000fAa719"),
            },
        ],
        "totalOriginalConsiderationItems": 2,  # 👈 إجباري لـ OpenSea API
    }

    url = f"https://api.opensea.io/api/v2/orders/{api_chain}/seaport/listings"

    payload = {
        "parameters": api_parameters,
        "protocol_address": seaport_address,
        "signature": signature,
    }

    try:
        async with session.post(url, headers=api_headers(), json=payload, timeout=30) as response:
            data = await response.json()
            if response.status in (200, 201):
                log.info(f"   ✅ تم العرض بنجاح")
                return True, "تم العرض بنجاح"
            else:
                log.error(f"   ❌ OpenSea {response.status}: {data}")
                return False, f"OpenSea {response.status}: {data}"
    except Exception as e:
        log.error(f"   ❌ خطأ في الإرسال: {e}")
        return False, str(e)

# ============================================================
# ✅ التحقق من وجود عرض سابق للـ NFT
# ============================================================

async def is_already_listed(session, chain: str, contract: str, token_id: str) -> bool:
    """التحقق من وجود عرض نشط للـ NFT على OpenSea"""
    api_chain = CHAINS[chain]["api_chain"]
    url = f"https://api.opensea.io/api/v2/orders/{api_chain}/seaport/listings"
    params = {
        "asset_contract_address": contract,
        "token_ids": token_id,
        "order_by": "created_date",
        "order_direction": "desc",
        "limit": 1,
    }
    try:
        async with session.get(url, headers=api_headers(), params=params, timeout=10) as resp:
            if resp.status == 200:
                data = await resp.json()
                orders = data.get("orders", [])
                if orders:
                    # التحقق من أن العرض نشط وليس منتهي
                    order = orders[0]
                    if order.get("order_hash") and not order.get("cancelled") and not order.get("finalized"):
                        return True
    except Exception as e:
        log.warning(f"   ⚠️ فشل التحقق من وجود عرض سابق: {e}")
    return False

# ============================================================
# ✅ PROCESS ONE NFT
# ============================================================

async def process_nft(session, nft):
    key = f"{nft['chain']}:{nft['contract']}:{nft['token_id']}"
    if key in processed_nfts:
        return True, "تمت معالجته مسبقاً"

    stats["processed"] += 1
    nft_name = nft.get('name', 'بدون اسم')
    log.info(f"   🖼️ NFT: {nft_name} | #{nft['token_id']}")

    # ✅ جلب سعر السوق (الحد الأدنى)
    api_chain = CHAINS[nft["chain"]]["api_chain"]
    collection_slug = nft.get("collection", "")
    
    price_info = await get_collection_floor_price(session, collection_slug, api_chain)

    # ✅ إذا لم يوجد سعر في السوق → تخطّي هذا المنتج
    if not price_info["has_floor_price"]:
        log.info(f"   ⏭️ لا يوجد سعر في السوق → تخطّي")
        processed_nfts.add(key)
        return True, "لا يوجد سعر في السوق"

    price_eth = price_info["price_eth"]
    price_usd = price_info["price_usd"]
    is_usd_currency = price_info["is_usd_currency"]
    log.info(f"   💰 السعر: {price_usd:.2f}$ = {price_eth:.6f} ETH {'(USDG)' if is_usd_currency else '(ETH)'}")

    approved, approval_msg = await ensure_approval(nft)
    if not approved:
        stats["failed"] += 1
        log.warning(f"   ❌ Approval: {approval_msg}")
        telegram(
            f"❌ <b>فشل الموافقة</b>\n"
            f"🖼️ {nft_name}\n"
            f"⚠️ {approval_msg}",
            is_error=True
        )
        return False, approval_msg

    log.info(f"   ✅ Approval: {approval_msg}")

    ok, result = await create_listing(session, nft, price_eth=price_eth, is_usd_currency=is_usd_currency, price_usd=price_usd)
    if not ok:
        stats["failed"] += 1
        log.error(f"   ❌ Listing: {result}")
        telegram(
            f"❌ <b>فشل العرض</b>\n"
            f"🖼️ {nft_name}\n"
            f"⚠️ {result}",
            is_error=True
        )
        return False, result

    stats["listed"] += 1
    processed_nfts.add(key)

    log.info(f"   ✅ تم عرض NFT بسعر ${price_usd:.2f}")
    telegram(
        f"✅ <b>تم عرض NFT</b>\n"
        f"🖼️ {nft_name}\n"
        f"💰 ${price_usd:.2f} ({price_eth:.6f} ETH)"
    )

    return True, result

# ============================================================
# PROCESS ONE COLLECTION
# ============================================================

async def process_collection(
    session,
    chain,
    collection,
    nfts,
    index,
    total,
):
    stats["current_collection"] = collection

    log.info("")
    log.info("=" * 60)
    log.info(f"📌 COLLECTION {index}/{total}")
    log.info(f"🗂️ {collection}")
    log.info(f"📡 {CHAINS[chain]['name']}")
    log.info(f"📦 عدد NFTs: {len(nfts)}")
    log.info("=" * 60)

    telegram(
        f"📌 <b>بدء Collection</b>\n"
        f"🗂️ {collection}\n"
        f"📡 {CHAINS[chain]['name']}\n"
        f"📦 NFTs: {len(nfts)}\n"
        f"📊 {index}/{total}"
    )

    collection_success = 0
    collection_failed = 0

    for number, nft in enumerate(nfts, 1):
        log.info(f"📍 Collection {index}/{total} | NFT {number}/{len(nfts)}")
        success, _ = await process_nft(session, nft)

        if success:
            collection_success += 1
        else:
            collection_failed += 1

        await asyncio.sleep(WRITE_DELAY)

    log.info("")
    log.info("-" * 60)
    log.info(f"✅ انتهت Collection: {collection}")
    log.info(f"📦 الإجمالي: {len(nfts)}")
    log.info(f"✅ النجاح: {collection_success}")
    log.info(f"❌ الفشل: {collection_failed}")
    log.info("-" * 60)

    telegram(
        f"✅ <b>انتهت Collection</b>\n"
        f"🗂️ {collection}\n"
        f"📦 {len(nfts)}\n"
        f"✅ {collection_success}\n"
        f"❌ {collection_failed}"
    )

# ============================================================
# REPORT
# ============================================================

def final_report(elapsed):
    return (
        f"📊 <b>التقرير النهائي</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"📦 NFTs: {stats['total']}\n"
        f"🔄 تمت المعالجة: {stats['processed']}\n"
        f"✅ تم العرض: {stats['listed']}\n"
        f"❌ فشل: {stats['failed']}\n"
        f"⏱️ الوقت: {elapsed / 60:.1f} دقيقة"
    )

# ============================================================
# CYCLE
# ============================================================

async def run_cycle():
    start_time = time.time()

    stats.update({
        "total": 0,
        "collections": 0,
        "processed": 0,
        "listed": 0,
        "failed": 0,
        "current_collection": "",
    })

    log.info("")
    log.info("#" * 60)
    log.info("🚀 بدء دورة جديدة")
    log.info(f"📅 {datetime.now()}")
    log.info(f"💰 سعر البيع: ${DEFAULT_PRICE_ETH * ETH_PRICE_USD:.4f}")
    log.info(f"⛽ حد رسوم الغاز: ${MAX_GAS_FEE_USD:.2f}")
    log.info("#" * 60)

    telegram("🚀 <b>بدء دورة جديدة</b>")

    async with aiohttp.ClientSession() as session:
        nfts = await fetch_all_nfts(session)
        stats["total"] = len(nfts)

        if not nfts:
            log.info("ℹ️ لا توجد NFTs")
            telegram("ℹ️ <b>لا توجد NFTs</b>")
            return

        groups = group_collections(nfts)
        stats["collections"] = len(groups)

        log.info(f"📦 NFTs: {len(nfts)}")
        log.info(f"🗂️ Collections: {len(groups)}")

        telegram(
            f"📦 <b>تم العثور على NFTs</b>\n"
            f"NFTs: {len(nfts)}\n"
            f"Collections: {len(groups)}"
        )

        for index, (group_key, collection_nfts) in enumerate(groups.items(), 1):
            chain, collection = group_key

            await process_collection(
                session=session,
                chain=chain,
                collection=collection,
                nfts=collection_nfts,
                index=index,
                total=len(groups),
            )

            await asyncio.sleep(2)

    elapsed = time.time() - start_time
    report = final_report(elapsed)
    log.info("\n" + report)
    telegram(report)

# ============================================================
# MAIN
# ============================================================

async def main_loop():
    telegram("🤖 <b>NFT Lister</b>\n🟢 تم التشغيل")

    while True:
        try:
            await run_cycle()
            log.info("\n✅ انتهت جميع Collections")
            log.info("⏳ انتظار 24 ساعة...")
            telegram("🏁 <b>انتهت جميع Collections</b>\n⏳ الدورة القادمة بعد 24 ساعة.")
            await asyncio.sleep(CYCLE_INTERVAL)
        except Exception as e:
            log.exception("💥 خطأ في الدورة")
            telegram(f"⚠️ <b>خطأ</b>\n{str(e)[:200]}", is_error=True)
            await asyncio.sleep(300)

async def run():
    if not OPENSEA_API_KEY:
        log.error("❌ OPENSEA_API_KEY غير موجود")
        return
    if not PRIVATE_KEY:
        log.error("❌ PRIVATE_KEY غير موجود")
        return
    if not WALLET_ADDRESS:
        log.error("❌ WALLET_ADDRESS غير موجود")
        return

    log.info("🚀 تشغيل النظام")
    log.info("📡 الشبكات: " + ", ".join(CHAINS[c]["name"] for c in ENABLED_CHAINS))

    await asyncio.gather(main_loop(), telegram_worker())

def main():
    try:
        asyncio.run(run())
    except KeyboardInterrupt:
        log.info("🛑 تم إيقاف النظام")
    except Exception as e:
        log.exception(f"💥 خطأ: {e}")

if __name__ == "__main__":
    main()
