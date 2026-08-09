#!/usr/bin/env python3
"""
NFT LISTING SYSTEM - دورة كل 5 دقائق (قابلة للتخصيص)
----------------------------------------------------
- هيكلة OOP مع فصل المسؤوليات
- دعم متعدد السلاسل مع جلب متوازي
- نظام كاش متقدم مع TTL
- إعادة محاولة ذكية مع backoff
- تحكم عبر متغيرات البيئة ووسائط سطر الأوامر
- معالجة صحيحة لتجاوز الحد (429) مع حد أقصى للمحاولات
- يدعم Robinhood Chain مع USDG و EIP-1559
- دورة تشغيل قصيرة (افتراضي 5 دقائق) مع إمكانية تعديلها
"""

import asyncio
import logging
import os
import time
import argparse
from collections import defaultdict
from datetime import datetime
from typing import Optional, Dict, List, Tuple, Any, Callable
from dataclasses import dataclass
from functools import wraps

import aiohttp
from web3 import Web3
from dotenv import load_dotenv
from eth_account import Account
from eth_account.messages import encode_typed_data

load_dotenv()

# ============================================================
# التهيئة الأساسية
# ============================================================

OPENSEA_API_KEY = os.getenv("OPENSEA_API_KEY", "").strip()
PRIVATE_KEY = os.getenv("PRIVATE_KEY", "").strip()
WALLET_ADDRESS = os.getenv("WALLET_ADDRESS", "").strip()
ETHEREUM_RPC_URL = os.getenv("ETHEREUM_RPC_URL", "").strip()
ROBINHOOD_RPC_URL = os.getenv("ROBINHOOD_RPC_URL", "").strip()
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "").strip()

# ============================================================
# الإعدادات (قابلة للتخصيص عبر البيئة)
# ============================================================
DEFAULT_PRICE_USD = float(os.getenv("DEFAULT_PRICE_USD", "5.0"))
MAX_GAS_FEE_USD = float(os.getenv("MAX_GAS_FEE_USD", "0.03"))
MAX_LISTING_PRICE_USD = float(os.getenv("MAX_LISTING_PRICE_USD", "1_000_000_000.0"))
CYCLE_INTERVAL_MINUTES = float(os.getenv("CYCLE_INTERVAL_MINUTES", "5"))  # 🔥 التغيير الأساسي: دورة كل 5 دقائق
RUN_ONCE = os.getenv("RUN_ONCE", "false").lower() in ("true", "1", "yes")
READ_DELAY = float(os.getenv("READ_DELAY", "0.5"))
WRITE_DELAY = float(os.getenv("WRITE_DELAY", "3"))
CACHE_TTL_SECONDS = int(os.getenv("CACHE_TTL_SECONDS", "300"))  # 5 دقائق (مناسب للدورة القصيرة)

# ============================================================
# ثوابت الشبكات
# ============================================================
OPENSEA_CONDUIT_KEY = "0x61159fefdfada89302ed55f8b9e89e2d67d8258712b3a3f89aa88525877f1d5e"
CONDUIT_CONTROLLER = "0x00000000F9490004C11Cef243f5400493c00Ad63"

# ============================================================
# إعداد التسجيل
# ============================================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("nft-lister")

# ============================================================
# أدوات مساعدة
# ============================================================

def checksum(address: str) -> Optional[str]:
    try:
        return Web3.to_checksum_address(address)
    except Exception:
        return None

def api_headers() -> Dict[str, str]:
    return {
        "x-api-key": OPENSEA_API_KEY,
        "Content-Type": "application/json",
    }

def retry_with_backoff(max_retries: int = 3, base_delay: float = 1.0, backoff: float = 2.0):
    def decorator(func: Callable):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            delay = base_delay
            for attempt in range(max_retries):
                try:
                    return await func(*args, **kwargs)
                except Exception as e:
                    if attempt == max_retries - 1:
                        raise
                    log.warning(f"⚠️ إعادة محاولة {func.__name__} ({attempt+1}/{max_retries}) بعد {delay:.1f} ثانية: {e}")
                    await asyncio.sleep(delay)
                    delay *= backoff
            return None
        return wrapper
    return decorator

# ============================================================
# إدارة التكوين والسلاسل
# ============================================================

@dataclass
class ChainConfig:
    name: str
    api_chain: str
    rpc: str
    chain_id: int
    currency: str
    seaport: str
    enabled: bool = True
    currency_decimals: int = 6
    currency_address: str = "0x5fc5360d0400a0fd4f2af552add042d716f1d168"
    zone_address: str = "0x000056f7000000ece9003ca63978907a00ffd100"

class ChainManager:
    _chains: Dict[str, ChainConfig] = {}
    _web3_clients: Dict[str, Web3] = {}
    
    @classmethod
    def load_chains(cls):
        cls._chains = {
            "ethereum": ChainConfig(
                name="Ethereum",
                api_chain="ethereum",
                rpc=ETHEREUM_RPC_URL,
                chain_id=1,
                currency="ETH",
                seaport="0x00000000006c3852cbEf3e08E8dF289169EdE581",
                enabled=False,
            ),
            "robinhood": ChainConfig(
                name="Robinhood",
                api_chain="robinhood",
                rpc=ROBINHOOD_RPC_URL,
                chain_id=4663,
                currency="ETH",
                seaport="0x0000000000000068F116a894984e2DB1123eB395",
                enabled=True,
            ),
        }
    
    @classmethod
    def get_enabled_chains(cls) -> List[str]:
        return [key for key, cfg in cls._chains.items() if cfg.enabled and cfg.rpc]
    
    @classmethod
    def get_config(cls, chain: str) -> Optional[ChainConfig]:
        return cls._chains.get(chain)
    
    @classmethod
    def get_web3(cls, chain: str) -> Optional[Web3]:
        if chain not in cls._web3_clients:
            cfg = cls.get_config(chain)
            if not cfg or not cfg.rpc:
                return None
            try:
                provider = Web3.HTTPProvider(cfg.rpc, request_kwargs={"timeout": 30})
                client = Web3(provider)
                if client.is_connected():
                    cls._web3_clients[chain] = client
                else:
                    log.error(f"❌ فشل الاتصال بـ {cfg.name} RPC")
                    return None
            except Exception as e:
                log.error(f"❌ خطأ Web3 لـ {chain}: {e}")
                return None
        return cls._web3_clients.get(chain)

ChainManager.load_chains()
ENABLED_CHAINS = ChainManager.get_enabled_chains()
if not ENABLED_CHAINS:
    raise SystemExit("❌ لا توجد شبكة مفعلة")

# ============================================================
# نظام الكاش مع TTL
# ============================================================

class TTLDict:
    def __init__(self, ttl: int = CACHE_TTL_SECONDS):
        self._data: Dict[str, Tuple[Any, float]] = {}
        self._ttl = ttl
    
    def get(self, key: str) -> Optional[Any]:
        entry = self._data.get(key)
        if entry:
            value, timestamp = entry
            if time.time() - timestamp < self._ttl:
                return value
            else:
                del self._data[key]
        return None
    
    def set(self, key: str, value: Any):
        self._data[key] = (value, time.time())
    
    def clear(self):
        self._data.clear()
    
    def __contains__(self, key: str) -> bool:
        return self.get(key) is not None

# ============================================================
# إدارة الأسعار
# ============================================================

class PriceManager:
    _eth_price_cache: Dict[str, Tuple[float, float]] = {}
    _floor_cache: TTLDict = TTLDict()
    
    @classmethod
    @retry_with_backoff(max_retries=2)
    async def fetch_eth_price(cls, session: aiohttp.ClientSession) -> float:
        now = time.time()
        if "eth" in cls._eth_price_cache:
            price, ts = cls._eth_price_cache["eth"]
            if now - ts < 300:
                return price
        
        url = "https://api.coingecko.com/api/v3/simple/price?ids=ethereum&vs_currencies=usd"
        try:
            async with session.get(url, timeout=10) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    price = data.get("ethereum", {}).get("usd", 0)
                    if price > 0:
                        cls._eth_price_cache["eth"] = (price, now)
                        return price
        except Exception as e:
            log.warning(f"فشل جلب سعر ETH: {e}")
        if "eth" in cls._eth_price_cache:
            return cls._eth_price_cache["eth"][0]
        return 3000.0
    
    @classmethod
    @retry_with_backoff(max_retries=2)
    async def get_collection_floor_price(cls, session: aiohttp.ClientSession,
                                         collection_slug: str, api_chain: str,
                                         eth_price: float) -> Dict:
        no_price = {"price_eth": 0, "price_usd": 0, "is_usd_currency": False, "has_floor_price": False}
        if not collection_slug or collection_slug == "unknown":
            return no_price
        
        cache_key = f"{api_chain}:{collection_slug}"
        cached = cls._floor_cache.get(cache_key)
        if cached is not None:
            return cached
        
        url = f"https://api.opensea.io/api/v2/collections/{collection_slug}/stats"
        try:
            async with session.get(url, headers=api_headers(), timeout=15) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    total_stats = data.get("total", {})
                    floor = total_stats.get("floor_price", 0)
                    symbol = str(total_stats.get("floor_price_symbol", "ETH")).upper()
                    
                    if floor and float(floor) > 0:
                        floor_val = float(floor)
                        is_usd = any(x in symbol for x in ["USD", "DAI", "USDC", "USDT"])
                        if is_usd:
                            price_usd = floor_val
                            price_eth = floor_val / eth_price
                        else:
                            price_eth = floor_val
                            price_usd = floor_val * eth_price
                        
                        if price_usd > MAX_LISTING_PRICE_USD:
                            log.warning(f"⚠️ سعر {collection_slug} وهمي ({price_usd:.2f}$)")
                            info = no_price
                        else:
                            info = {
                                "price_eth": price_eth,
                                "price_usd": price_usd,
                                "is_usd_currency": is_usd,
                                "has_floor_price": True,
                            }
                            log.info(f"📈 سعر {collection_slug}: {price_usd:.2f}$ = {price_eth:.6f} ETH (العملة: {symbol})")
                        cls._floor_cache.set(cache_key, info)
                        return info
                else:
                    log.warning(f"⚠️ لم يُعثر على سعر لـ {collection_slug} (HTTP {resp.status})")
        except Exception as e:
            log.warning(f"⚠️ فشل جلب سعر {collection_slug}: {e}")
        
        cls._floor_cache.set(cache_key, no_price)
        return no_price

# ============================================================
# جلب NFTs (مع حد لمحاولات 429)
# ============================================================

class NFTFetcher:
    @classmethod
    @retry_with_backoff(max_retries=3)
    async def fetch_chain_nfts(cls, session: aiohttp.ClientSession, chain: str) -> List[Dict]:
        config = ChainManager.get_config(chain)
        if not config:
            return []
        
        result = []
        cursor = None
        url = f"https://api.opensea.io/api/v2/chain/{config.api_chain}/account/{WALLET_ADDRESS}/nfts"
        rate_limit_attempts = 0
        MAX_RATE_LIMIT_ATTEMPTS = 5
        
        while True:
            params = {"limit": 200}
            if cursor:
                params["next"] = cursor
            
            try:
                async with session.get(url, headers=api_headers(), params=params, timeout=30) as resp:
                    if resp.status == 429:
                        rate_limit_attempts += 1
                        if rate_limit_attempts >= MAX_RATE_LIMIT_ATTEMPTS:
                            log.error(f"❌ تجاوز الحد الأقصى لمحاولات 429 لـ {config.name}، توقف جلب NFTs")
                            return result
                        log.warning(f"⚠️ تجاوز الحد لـ {config.name}، المحاولة {rate_limit_attempts}/{MAX_RATE_LIMIT_ATTEMPTS}، انتظار 10 ثوانٍ")
                        await asyncio.sleep(10)
                        continue
                    
                    rate_limit_attempts = 0
                    
                    if resp.status != 200:
                        log.error(f"❌ {config.name} HTTP {resp.status}")
                        break
                    
                    data = await resp.json()
                    for nft in data.get("nfts", []):
                        contract = nft.get("contract")
                        token_id = nft.get("identifier")
                        if not contract or token_id is None:
                            continue
                        collection_raw = nft.get("collection")
                        if isinstance(collection_raw, dict):
                            collection_slug = collection_raw.get("slug") or "unknown"
                        elif isinstance(collection_raw, str):
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
                log.error(f"❌ انتهت المهلة لجلب NFTs من {config.name}")
                break
            except Exception as e:
                log.error(f"❌ خطأ في جلب {config.name}: {e}")
                break
        
        return result
    
    @classmethod
    async def fetch_all_chains(cls, session: aiohttp.ClientSession) -> List[Dict]:
        tasks = [cls.fetch_chain_nfts(session, chain) for chain in ENABLED_CHAINS]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        all_nfts = []
        for chain, res in zip(ENABLED_CHAINS, results):
            if isinstance(res, Exception):
                log.error(f"❌ فشل جلب {chain}: {res}")
            else:
                log.info(f"📥 {ChainManager.get_config(chain).name}: {len(res)} NFT")
                all_nfts.extend(res)
        return all_nfts

# ============================================================
# عقد Conduit
# ============================================================

class ConduitManager:
    _conduit_cache: Dict[str, str] = {}
    
    @classmethod
    @retry_with_backoff(max_retries=2)
    async def get_conduit_address(cls, chain: str) -> Optional[str]:
        if chain in cls._conduit_cache:
            return cls._conduit_cache[chain]
        
        client = ChainManager.get_web3(chain)
        if not client:
            return None
        
        controller_abi = [{
            "inputs": [{"internalType": "bytes32", "name": "conduitKey", "type": "bytes32"}],
            "name": "getConduit",
            "outputs": [{"internalType": "address", "name": "conduit", "type": "address"},
                        {"internalType": "bool", "name": "exists", "type": "bool"}],
            "stateMutability": "view",
            "type": "function",
        }]
        try:
            controller = client.eth.contract(
                address=checksum(CONDUIT_CONTROLLER),
                abi=controller_abi,
            )
            key_bytes = bytes.fromhex(OPENSEA_CONDUIT_KEY.replace("0x", ""))
            conduit_addr, exists = controller.functions.getConduit(key_bytes).call()
            if exists and conduit_addr and conduit_addr != "0x0000000000000000000000000000000000000000":
                addr = checksum(conduit_addr)
                cls._conduit_cache[chain] = addr
                log.info(f"🔗 Conduit ({chain}): {addr}")
                return addr
            else:
                log.warning(f"⚠️ Conduit غير موجود لـ {chain}")
                return None
        except Exception as e:
            log.warning(f"⚠️ فشل جلب Conduit لـ {chain}: {e}")
            return None

# ============================================================
# إدارة العروض
# ============================================================

class ListingManager:
    _approval_cache: Dict[str, bool] = {}
    _processed_nfts: set = set()
    
    @classmethod
    async def ensure_approval(cls, nft: Dict) -> Tuple[bool, str]:
        chain = nft["chain"]
        contract = checksum(nft["contract"])
        owner = checksum(WALLET_ADDRESS)
        
        operator = await ConduitManager.get_conduit_address(chain)
        if not operator:
            return False, "فشل الحصول على Conduit (لا يُسمح باستخدام Seaport كبديل)"
        if not contract:
            return False, "عنوان العقد غير صالح"
        
        cache_key = f"{chain}:{contract}:{operator}"
        if cache_key in cls._approval_cache:
            return cls._approval_cache[cache_key], "من الكاش"
        
        client = ChainManager.get_web3(chain)
        if not client:
            return False, "فشل الاتصال بـ RPC"
        
        try:
            balance_wei = client.eth.get_balance(owner)
            balance_eth = balance_wei / 1e18
            log.info(f"💰 الرصيد: {balance_eth:.4f} {ChainManager.get_config(chain).currency}")
            if balance_wei == 0:
                return False, "الرصيد صفر"
        except Exception as e:
            return False, f"فشل قراءة الرصيد: {e}"
        
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
            contract_inst = client.eth.contract(address=contract, abi=abi)
            try:
                approved = contract_inst.functions.isApprovedForAll(owner, operator).call()
                if approved:
                    cls._approval_cache[cache_key] = True
                    log.info("✅ موافق مسبقاً")
                    return True, "موافق مسبقاً"
            except Exception as e:
                return False, f"فشل التحقق من الموافقة: {e}"
            
            nonce = client.eth.get_transaction_count(owner)
            latest_block = client.eth.get_block('latest')
            base_fee = latest_block.get('baseFeePerGas', 0)
            max_priority_fee = client.eth.max_priority_fee
            max_fee = int(base_fee * 1.5) + max_priority_fee
            
            tx = contract_inst.functions.setApprovalForAll(operator, True).build_transaction({
                "from": owner,
                "nonce": nonce,
                "maxFeePerGas": max_fee,
                "maxPriorityFeePerGas": max_priority_fee,
                "type": 2,
            })
            try:
                estimated = client.eth.estimate_gas(tx)
                tx["gas"] = int(estimated * 1.2)
            except:
                tx["gas"] = 150000
            
            async with aiohttp.ClientSession() as session:
                eth_price = await PriceManager.fetch_eth_price(session)
            gas_eth = tx["gas"] * max_fee / 1e18
            gas_usd = gas_eth * eth_price
            log.info(f"⛽ رسوم الموافقة: ${gas_usd:.4f} ({gas_eth:.6f} ETH)")
            
            if gas_usd > MAX_GAS_FEE_USD:
                log.warning(f"⚠️ رسوم الغاز مرتفعة: ${gas_usd:.4f} > ${MAX_GAS_FEE_USD}")
                return False, f"رسوم الغاز مرتفعة: ${gas_usd:.4f}"
            
            if balance_wei < tx["gas"] * max_fee:
                return False, "الرصيد غير كافٍ"
            
            signed = client.eth.account.sign_transaction(tx, PRIVATE_KEY)
            raw_tx = getattr(signed, "raw_transaction", signed.rawTransaction)
            tx_hash = client.eth.send_raw_transaction(raw_tx)
            log.info(f"⛽ جاري تأكيد الموافقة: {tx_hash.hex()[:10]}...")
            receipt = client.eth.wait_for_transaction_receipt(tx_hash, timeout=180)
            success = receipt.status == 1
            cls._approval_cache[cache_key] = success
            if success:
                log.info(f"✅ تمت الموافقة (${gas_usd:.4f})")
                return True, f"تمت الموافقة (${gas_usd:.4f})"
            else:
                return False, "فشلت معاملة Approval"
        except Exception as e:
            return False, str(e)[:200]
    
    @classmethod
    async def is_already_listed(cls, session: aiohttp.ClientSession, chain: str,
                                 contract: str, token_id: str) -> bool:
        config = ChainManager.get_config(chain)
        if not config:
            return False
        url = f"https://api.opensea.io/api/v2/orders/{config.api_chain}/seaport/listings"
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
                        order = orders[0]
                        if order.get("order_hash") and not order.get("cancelled") and not order.get("finalized"):
                            return True
        except Exception as e:
            log.warning(f"⚠️ فشل التحقق من العرض السابق: {e}")
        return False
    
    @classmethod
    async def get_seaport_counter(cls, chain: str, owner: str) -> int:
        client = ChainManager.get_web3(chain)
        if not client:
            return 0
        config = ChainManager.get_config(chain)
        if not config:
            return 0
        abi = [{
            "inputs": [{"internalType": "address", "name": "offerer", "type": "address"}],
            "name": "getCounter",
            "outputs": [{"internalType": "uint256", "name": "counter", "type": "uint256"}],
            "stateMutability": "view",
            "type": "function",
        }]
        try:
            seaport = client.eth.contract(address=checksum(config.seaport), abi=abi)
            counter = seaport.functions.getCounter(owner).call()
            log.info(f"🔢 Seaport counter: {counter}")
            return counter
        except Exception as e:
            log.warning(f"⚠️ فشل جلب counter: {e}")
            return 0
    
    @classmethod
    async def create_listing(cls, session: aiohttp.ClientSession, nft: Dict,
                              price_eth: float, is_usd: bool, price_usd: float) -> Tuple[bool, str]:
        chain = nft["chain"]
        config = ChainManager.get_config(chain)
        if not config:
            return False, "تكوين السلسلة غير موجود"
        
        owner = checksum(WALLET_ADDRESS)
        contract = checksum(nft["contract"])
        token_id = int(nft["token_id"])
        
        if is_usd and price_usd is not None:
            listing_price_usd = round(price_usd, 2)
        elif price_eth > 0:
            eth_price = await PriceManager.fetch_eth_price(session)
            listing_price_usd = round(price_eth * eth_price, 2)
        else:
            listing_price_usd = round(DEFAULT_PRICE_USD, 2)
        
        price_wei = int(listing_price_usd * (10 ** config.currency_decimals))
        opensea_fee = int(price_wei * 100 / 10000)
        owner_amount = price_wei - opensea_fee
        
        counter = await cls.get_seaport_counter(chain, owner)
        now = int(time.time())
        salt = int(time.time() * 1000)
        start = now
        end = now + 86400
        
        currency_addr = checksum(config.currency_address)
        zone_addr = checksum(config.zone_address)
        seaport_addr = checksum(config.seaport)
        
        domain = {
            "name": "Seaport",
            "version": "1.6",
            "chainId": config.chain_id,
            "verifyingContract": seaport_addr,
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
        message = {
            "offerer": owner,
            "zone": zone_addr,
            "zoneHash": "0x" + "0" * 64,
            "startTime": start,
            "endTime": end,
            "orderType": 2,
            "salt": salt,
            "conduitKey": OPENSEA_CONDUIT_KEY,
            "counter": counter,
            "offer": [
                {
                    "itemType": 2,
                    "token": contract,
                    "identifierOrCriteria": token_id,
                    "startAmount": 1,
                    "endAmount": 1,
                }
            ],
            "consideration": [
                {
                    "itemType": 1,
                    "token": currency_addr,
                    "identifierOrCriteria": 0,
                    "startAmount": owner_amount,
                    "endAmount": owner_amount,
                    "recipient": owner,
                },
                {
                    "itemType": 1,
                    "token": currency_addr,
                    "identifierOrCriteria": 0,
                    "startAmount": opensea_fee,
                    "endAmount": opensea_fee,
                    "recipient": checksum("0x0000a26b00c1F0DF003000390027140000fAa719"),
                },
            ],
        }
        
        try:
            account = Account.from_key(PRIVATE_KEY)
            encoded = encode_typed_data(domain, types, message)
            signed = account.sign_message(encoded)
            signature = signed.signature.hex()
            if not signature.startswith("0x"):
                signature = "0x" + signature
        except Exception as e:
            return False, f"فشل التوقيع: {e}"
        
        api_message = {
            "offerer": message["offerer"],
            "zone": zone_addr,
            "zoneHash": message["zoneHash"],
            "startTime": str(message["startTime"]),
            "endTime": str(message["endTime"]),
            "orderType": message["orderType"],
            "salt": str(message["salt"]),
            "conduitKey": message["conduitKey"],
            "counter": str(message["counter"]),
            "offer": [
                {
                    "itemType": offer["itemType"],
                    "token": offer["token"],
                    "identifierOrCriteria": str(offer["identifierOrCriteria"]),
                    "startAmount": str(offer["startAmount"]),
                    "endAmount": str(offer["endAmount"]),
                }
                for offer in message["offer"]
            ],
            "consideration": [
                {
                    "itemType": c["itemType"],
                    "token": c["token"],
                    "identifierOrCriteria": str(c["identifierOrCriteria"]),
                    "startAmount": str(c["startAmount"]),
                    "endAmount": str(c["endAmount"]),
                    "recipient": c["recipient"],
                }
                for c in message["consideration"]
            ],
            "totalOriginalConsiderationItems": len(message["consideration"]),
        }
        
        url = f"https://api.opensea.io/api/v2/orders/{config.api_chain}/seaport/listings"
        payload = {
            "parameters": api_message,
            "protocol_address": seaport_addr,
            "signature": signature,
        }
        
        try:
            async with session.post(url, headers=api_headers(), json=payload, timeout=30) as resp:
                data = await resp.json()
                if resp.status in (200, 201):
                    log.info("✅ تم العرض بنجاح")
                    return True, "تم العرض بنجاح"
                else:
                    log.error(f"❌ OpenSea {resp.status}: {data}")
                    return False, f"OpenSea {resp.status}: {data}"
        except Exception as e:
            log.error(f"❌ خطأ في الإرسال: {e}")
            return False, str(e)

# ============================================================
# معالجة NFTs
# ============================================================

class NFTProcessor:
    _processed_nfts: set = set()
    _stats: Dict[str, int] = defaultdict(int)
    
    @classmethod
    def reset_stats(cls):
        cls._stats.clear()
        cls._stats.update(total=0, collections=0, processed=0, listed=0, failed=0)
    
    @classmethod
    async def process_one(cls, session: aiohttp.ClientSession, nft: Dict) -> Tuple[bool, str]:
        key = f"{nft['chain']}:{nft['contract']}:{nft['token_id']}"
        if key in cls._processed_nfts:
            return True, "تمت معالجته مسبقاً"
        
        cls._stats["processed"] += 1
        name = nft.get('name', 'بدون اسم')
        log.info(f"🖼️ NFT: {name} | #{nft['token_id']}")
        
        existing = await ListingManager.is_already_listed(session, nft["chain"], nft["contract"], nft["token_id"])
        if existing:
            log.info("⏭️ يوجد عرض نشط بالفعل -> تخطّي")
            cls._processed_nfts.add(key)
            return True, "موجود بالفعل"
        
        config = ChainManager.get_config(nft["chain"])
        eth_price = await PriceManager.fetch_eth_price(session)
        price_info = await PriceManager.get_collection_floor_price(
            session, nft.get("collection", ""), config.api_chain, eth_price
        )
        if not price_info["has_floor_price"]:
            log.info("⏭️ لا يوجد سعر في السوق -> تخطّي")
            cls._processed_nfts.add(key)
            return True, "لا يوجد سعر"
        
        price_eth = price_info["price_eth"]
        price_usd = price_info["price_usd"]
        is_usd = price_info["is_usd_currency"]
        log.info(f"💰 السعر: {price_usd:.2f}$ = {price_eth:.6f} ETH")
        
        approved, msg = await ListingManager.ensure_approval(nft)
        if not approved:
            cls._stats["failed"] += 1
            log.warning(f"❌ Approval: {msg}")
            return False, msg
        
        log.info(f"✅ Approval: {msg}")
        
        ok, result = await ListingManager.create_listing(session, nft, price_eth, is_usd, price_usd)
        if not ok:
            cls._stats["failed"] += 1
            log.error(f"❌ Listing: {result}")
            return False, result
        
        cls._stats["listed"] += 1
        cls._processed_nfts.add(key)
        log.info(f"✅ تم عرض NFT بسعر ${price_usd:.2f}")
        return True, "تم العرض"
    
    @classmethod
    async def process_collection(cls, session: aiohttp.ClientSession, chain: str,
                                 collection: str, nfts: List[Dict],
                                 index: int, total: int) -> None:
        log.info("")
        log.info("=" * 60)
        log.info(f"📌 COLLECTION {index}/{total} - {collection} ({ChainManager.get_config(chain).name})")
        log.info(f"📦 عدد NFTs: {len(nfts)}")
        log.info("=" * 60)
        
        success = 0
        for i, nft in enumerate(nfts, 1):
            log.info(f"📍 NFT {i}/{len(nfts)}")
            ok, _ = await cls.process_one(session, nft)
            if ok:
                success += 1
            await asyncio.sleep(WRITE_DELAY)
        
        log.info(f"✅ انتهت {collection}: نجاح {success}/{len(nfts)}")

# ============================================================
# حلقة التشغيل الرئيسية (الدورة كل 5 دقائق)
# ============================================================

class MainLoop:
    _telegram_queue: asyncio.Queue = asyncio.Queue()
    
    @classmethod
    def send_telegram(cls, message: str, is_error: bool = False):
        if TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID:
            if is_error:
                message = f"⚠️ <b>خطأ</b>\n{message}"
            cls._telegram_queue.put_nowait(message)
    
    @classmethod
    async def telegram_worker(cls):
        if not (TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID):
            return
        url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
        async with aiohttp.ClientSession() as session:
            while True:
                try:
                    msg = await cls._telegram_queue.get()
                    data = {"chat_id": TELEGRAM_CHAT_ID, "text": msg, "parse_mode": "HTML"}
                    async with session.post(url, data=data, timeout=15) as resp:
                        if resp.status == 429:
                            await asyncio.sleep(5)
                            cls._telegram_queue.put_nowait(msg)
                        elif resp.status != 200:
                            log.warning(f"Telegram HTTP {resp.status}")
                    await asyncio.sleep(1.5)
                except Exception as e:
                    log.error(f"Telegram error: {e}")
                    await asyncio.sleep(5)
    
    @classmethod
    async def run_cycle(cls):
        start = time.time()
        log.info("")
        log.info("#" * 60)
        log.info("🚀 بدء دورة جديدة")
        log.info(f"📅 {datetime.now()}")
        log.info("#" * 60)
        cls.send_telegram("🚀 <b>بدء دورة جديدة</b>")
        
        async with aiohttp.ClientSession() as session:
            eth_price = await PriceManager.fetch_eth_price(session)
            log.info(f"💰 سعر ETH: ${eth_price:.2f}")
            
            nfts = await NFTFetcher.fetch_all_chains(session)
            if not nfts:
                log.info("ℹ️ لا توجد NFTs")
                cls.send_telegram("ℹ️ <b>لا توجد NFTs</b>")
                return
            
            groups = defaultdict(list)
            for nft in nfts:
                groups[(nft["chain"], nft["collection"])].append(nft)
            
            total_nfts = len(nfts)
            total_collections = len(groups)
            NFTProcessor.reset_stats()
            NFTProcessor._stats["total"] = total_nfts
            NFTProcessor._stats["collections"] = total_collections
            log.info(f"📦 NFTs: {total_nfts}, 🗂️ Collections: {total_collections}")
            cls.send_telegram(f"📦 <b>تم العثور على NFTs</b>\nNFTs: {total_nfts}\nCollections: {total_collections}")
            
            for idx, ((chain, collection), items) in enumerate(groups.items(), 1):
                await NFTProcessor.process_collection(session, chain, collection, items, idx, total_collections)
                await asyncio.sleep(2)
        
        elapsed = time.time() - start
        report = (
            f"📊 <b>التقرير النهائي</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━\n"
            f"📦 NFTs: {NFTProcessor._stats['total']}\n"
            f"🔄 تمت المعالجة: {NFTProcessor._stats['processed']}\n"
            f"✅ تم العرض: {NFTProcessor._stats['listed']}\n"
            f"❌ فشل: {NFTProcessor._stats['failed']}\n"
            f"⏱️ الوقت: {elapsed / 60:.1f} دقيقة"
        )
        log.info("\n" + report)
        cls.send_telegram(report)
    
    @classmethod
    async def main_loop(cls):
        cls.send_telegram("🤖 <b>NFT Lister</b>\n🟢 تم التشغيل (الدورة كل 5 دقائق)")
        cycle = 0
        while True:
            cycle += 1
            log.info(f"🔄 الدورة رقم {cycle}")
            try:
                await cls.run_cycle()
                log.info("✅ انتهت جميع Collections")
                if RUN_ONCE or CYCLE_INTERVAL_MINUTES <= 0:
                    log.info("🛑 التوقف (RUN_ONCE أو CYCLE_INTERVAL_MINUTES=0)")
                    cls.send_telegram("🏁 <b>انتهى التشغيل</b> (دورة واحدة)")
                    break
                wait_seconds = int(CYCLE_INTERVAL_MINUTES * 60)
                log.info(f"⏳ انتظار {CYCLE_INTERVAL_MINUTES} دقيقة...")
                cls.send_telegram(f"🏁 انتهت الدورة {cycle}\n⏳ الدورة القادمة بعد {CYCLE_INTERVAL_MINUTES} دقيقة.")
                await asyncio.sleep(wait_seconds)
            except Exception as e:
                log.exception("💥 خطأ في الدورة")
                cls.send_telegram(f"⚠️ <b>خطأ</b>\n{str(e)[:200]}", is_error=True)
                if RUN_ONCE:
                    break
                await asyncio.sleep(60)  # انتظار دقيقة قبل إعادة المحاولة

# ============================================================
# نقطة الدخول
# ============================================================

async def run():
    if not OPENSEA_API_KEY or not PRIVATE_KEY or not WALLET_ADDRESS:
        log.error("❌ تأكد من وجود OPENSEA_API_KEY, PRIVATE_KEY, WALLET_ADDRESS")
        return
    
    log.info("🚀 تشغيل النظام المحسن (دورة كل 5 دقائق)")
    log.info(f"📡 الشبكات: {', '.join(ChainManager.get_config(c).name for c in ENABLED_CHAINS)}")
    log.info(f"⏱️ مدة الدورة: {CYCLE_INTERVAL_MINUTES} دقيقة")
    log.info(f"🔄 تشغيل لمرة واحدة: {RUN_ONCE}")
    
    await asyncio.gather(
        MainLoop.main_loop(),
        MainLoop.telegram_worker(),
    )

def main():
    parser = argparse.ArgumentParser(description="NFT Lister System - دورة قصيرة")
    parser.add_argument("--run-once", action="store_true", help="تشغيل دورة واحدة فقط")
    parser.add_argument("--interval", type=float, help="مدة الدورة بالدقائق")
    args = parser.parse_args()
    
    global RUN_ONCE, CYCLE_INTERVAL_MINUTES
    if args.run_once:
        RUN_ONCE = True
    if args.interval is not None:
        CYCLE_INTERVAL_MINUTES = args.interval
    
    try:
        asyncio.run(run())
    except KeyboardInterrupt:
        log.info("🛑 تم إيقاف النظام")
    except Exception as e:
        log.exception(f"💥 خطأ: {e}")

if __name__ == "__main__":
    main()
