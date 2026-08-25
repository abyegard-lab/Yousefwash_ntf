"""
محرك الشراء التلقائي المتعدد المحافظ عبر عقد SeaDrop.
نسخة محسنة مع نظام ذكي لإعادة المحاولة وتحليل الأخطاء
"""

import asyncio
import logging
import time
import random
from web3 import Web3
from web3.exceptions import ContractLogicError, TransactionNotFound
from web3.middleware import ExtraDataToPOAMiddleware

log = logging.getLogger("buyer")

SEADROP_ADDRESS = Web3.to_checksum_address("0x00005EA00Ac477B1030CE78506496e8C2dE24bf5")
ZERO_ADDRESS = Web3.to_checksum_address("0x0000000000000000000000000000000000000000")

SEADROP_ABI = [
    {
        "inputs": [
            {"name": "nftContract", "type": "address"},
            {"name": "feeRecipient", "type": "address"},
            {"name": "minterIfNotPayer", "type": "address"},
            {"name": "quantity", "type": "uint256"},
        ],
        "name": "mintPublic",
        "outputs": [],
        "stateMutability": "payable",
        "type": "function",
    },
    {
        "inputs": [{"name": "nftContract", "type": "address"}],
        "name": "getAllowedFeeRecipients",
        "outputs": [{"name": "", "type": "address[]"}],
        "stateMutability": "view",
        "type": "function",
    },
    {
        "inputs": [{"name": "nftContract", "type": "address"}],
        "name": "getPublicDrop",
        "outputs": [{
            "components": [
                {"name": "mintPrice", "type": "uint80"},
                {"name": "startTime", "type": "uint48"},
                {"name": "endTime", "type": "uint48"},
                {"name": "maxTotalMintableByWallet", "type": "uint16"},
                {"name": "feeBps", "type": "uint16"},
                {"name": "restrictFeeRecipients", "type": "bool"},
            ],
            "name": "",
            "type": "tuple",
        }],
        "stateMutability": "view",
        "type": "function",
    },
]

# ==================== الإعدادات ====================

MIN_BALANCE_RESERVE_USD = 0.10
FEW_THRESHOLD = 20
LIMITED_BUY_QTY = 15
GAS_LIMIT_SAFETY_MARGIN = 1.2
FREE_PRICE_THRESHOLD_USD = 0.01  # ✅ زيادة العتبة

MAX_RETRY_ATTEMPTS = 3
BASE_RETRY_DELAY = 0.5
MAX_RETRY_DELAY = 8

# ==================== التخزين العالمي ====================

prepared_transactions = {}
wallet_locks = {}

# ==================== دوال مساعدة ====================

def get_wallet_lock(wallet_address: str) -> asyncio.Lock:
    addr = wallet_address.lower()
    if addr not in wallet_locks:
        wallet_locks[addr] = asyncio.Lock()
    return wallet_locks[addr]

def get_web3(rpc_url: str) -> Web3:
    w3 = Web3(Web3.HTTPProvider(rpc_url))
    try:
        w3.middleware_onion.inject(ExtraDataToPOAMiddleware, layer=0)
    except (ValueError, TypeError):
        pass
    return w3

def get_wallet_balance_usd(w3: Web3, wallet_address: str, eth_price_usd: float) -> float:
    try:
        checksum_wallet = Web3.to_checksum_address(wallet_address)
        balance_wei = w3.eth.get_balance(checksum_wallet)
        return (balance_wei / 1e18) * eth_price_usd
    except Exception as e:
        log.error(f"[الرصيد] تعذر القراءة للمحفظة {wallet_address[:8]}...: {e}")
        return 0.0

def estimate_gas_fee_usd(w3: Web3, eth_price_usd: float, gas_units: int = 150_000) -> float:
    try:
        gas_price_wei = w3.eth.gas_price
        fee_eth = (gas_price_wei * gas_units) / 1e18
        return fee_eth * eth_price_usd
    except Exception as e:
        log.warning(f"[الغاز] تعذر التقدير: {e}")
        return float("inf")

def get_fee_recipient(w3: Web3, nft_contract: str):
    try:
        seadrop = w3.eth.contract(address=SEADROP_ADDRESS, abi=SEADROP_ABI)
        recipients = seadrop.functions.getAllowedFeeRecipients(
            Web3.to_checksum_address(nft_contract)
        ).call()
        if not recipients:
            return None
        return Web3.to_checksum_address(recipients[0])
    except Exception as e:
        log.error(f"[عنوان الرسوم] خطأ استعلام للعقد {nft_contract[:8]}...: {e}")
        return None

def decide_quantity(max_per_wallet, remaining_supply: int) -> int:
    if max_per_wallet is None:
        qty = 1
    elif max_per_wallet <= FEW_THRESHOLD:
        qty = max_per_wallet
    else:
        qty = LIMITED_BUY_QTY
    return max(1, min(qty, remaining_supply))

def get_onchain_public_price_wei(w3: Web3, nft_contract: str):
    try:
        seadrop = w3.eth.contract(address=SEADROP_ADDRESS, abi=SEADROP_ABI)
        public_drop = seadrop.functions.getPublicDrop(
            Web3.to_checksum_address(nft_contract)
        ).call()
        return int(public_drop[0])
    except Exception as e:
        log.warning(f"[سعر on-chain] تعذر القراءة للعقد {nft_contract[:8]}...: {e}")
        return None

def is_free_or_negligible(price_wei: int, eth_price_usd: float) -> bool:
    """تحديد إذا كان السعر مجانياً أو رمزياً"""
    if price_wei is None:
        return False
    if price_wei == 0:
        return True
    price_usd = (price_wei / 1e18) * eth_price_usd
    return price_usd < FREE_PRICE_THRESHOLD_USD

# ==================== تحليل الأخطاء ====================

def analyze_error(error: Exception) -> dict:
    error_str = str(error).lower()
    
    error_patterns = {
        "insufficient_funds": {
            "keywords": ["insufficient funds", "insufficient balance", "not enough funds"],
            "category": "insufficient_funds",
            "retryable": False,
        },
        "sold_out": {
            "keywords": ["sold out", "max supply", "no tokens left"],
            "category": "fatal",
            "retryable": False,
        },
        "already_minted": {
            "keywords": ["already minted", "already claimed", "max per wallet"],
            "category": "fatal",
            "retryable": False,
        },
        "nonce_too_low": {
            "keywords": ["nonce too low", "nonce already used"],
            "category": "retryable",
            "retryable": True,
        },
        "gas_too_low": {
            "keywords": ["gas too low", "gas price too low"],
            "category": "retryable",
            "retryable": True,
        },
        "network_error": {
            "keywords": ["connection", "timeout", "network", "429"],
            "category": "retryable",
            "retryable": True,
        },
        "phase_not_active": {
            "keywords": ["phase not active", "not started", "not yet started"],
            "category": "fatal",
            "retryable": False,
        },
    }
    
    for error_name, error_info in error_patterns.items():
        for keyword in error_info["keywords"]:
            if keyword in error_str:
                return {
                    "reason": error_name,
                    "category": error_info["category"],
                    "retryable": error_info.get("retryable", False),
                    "message": error_str[:200],
                }
    
    return {
        "reason": "unknown_error",
        "category": "unknown",
        "retryable": True,
        "message": error_str[:200],
    }

def calculate_retry_delay(attempt: int) -> float:
    delay = BASE_RETRY_DELAY * (1.5 ** attempt)
    jitter = random.uniform(0, 0.3)
    return min(delay + jitter, MAX_RETRY_DELAY)

def wait_for_transaction_receipt(w3: Web3, tx_hash: str, timeout: int = 30) -> dict:
    start_time = time.time()
    
    while time.time() - start_time < timeout:
        try:
            receipt = w3.eth.get_transaction_receipt(tx_hash)
            if receipt is not None:
                return {
                    "success": receipt.status == 1,
                    "receipt": receipt,
                    "gas_used": receipt.gasUsed,
                    "block_number": receipt.blockNumber
                }
        except TransactionNotFound:
            pass
        except Exception as e:
            log.warning(f"[الاستلام] خطأ: {e}")
        
        time.sleep(0.5)
    
    return {
        "success": False,
        "receipt": None,
        "error": "timeout"
    }

# ==================== تحضير المعاملات ====================

def prepare_transaction_for_wallet(
    w3: Web3,
    wallet_address: str,
    nft_contract: str,
    price_wei: int,
    max_per_wallet,
    remaining_supply: int,
    chain_id: int,
) -> dict:
    try:
        checksum_wallet = Web3.to_checksum_address(wallet_address)
        checksum_contract = Web3.to_checksum_address(nft_contract)
        
        fee_recipient = get_fee_recipient(w3, checksum_contract)
        if not fee_recipient:
            fee_recipient = ZERO_ADDRESS
        
        quantity = decide_quantity(max_per_wallet, remaining_supply)
        total_value = price_wei * quantity
        
        contract = w3.eth.contract(address=SEADROP_ADDRESS, abi=SEADROP_ABI)
        nonce = w3.eth.get_transaction_count(checksum_wallet, "pending")
        
        tx = contract.functions.mintPublic(
            checksum_contract,
            fee_recipient,
            ZERO_ADDRESS,
            quantity,
        ).build_transaction({
            "from": checksum_wallet,
            "value": total_value,
            "nonce": nonce,
            "chainId": chain_id,
        })
        
        try:
            estimated_gas = w3.eth.estimate_gas(tx)
            tx["gas"] = int(estimated_gas * GAS_LIMIT_SAFETY_MARGIN)
        except Exception as e:
            log.warning(f"⚠️ فشل تقدير الغاز للمحفظة {wallet_address[:8]}...: {e}")
            tx["gas"] = 200_000
        
        return {
            "success": True,
            "tx": tx,
            "quantity": quantity,
            "total_value": total_value,
        }
        
    except Exception as e:
        log.error(f"❌ فشل تحضير المعاملة للمحفظة {wallet_address[:8]}...: {e}")
        return {
            "success": False,
            "error": str(e),
            "wallet": wallet_address
        }

def prepare_all_transactions(
    slug: str,
    chain_key: str,
    w3: Web3,
    nft_contract: str,
    price_wei: int,
    max_per_wallet,
    remaining_supply: int,
    wallets_data: list,
) -> dict:
    global prepared_transactions
    
    chain_id = w3.eth.chain_id
    results = {}
    
    for wallet_data in wallets_data:
        wallet_addr = wallet_data["wallet"]
        
        if slug in prepared_transactions and wallet_addr in prepared_transactions.get(slug, {}):
            continue
        
        result = prepare_transaction_for_wallet(
            w3,
            wallet_addr,
            nft_contract,
            price_wei,
            max_per_wallet,
            remaining_supply,
            chain_id,
        )
        
        if result["success"]:
            if slug not in prepared_transactions:
                prepared_transactions[slug] = {}
            
            prepared_transactions[slug][wallet_addr] = {
                "tx": result["tx"],
                "quantity": result["quantity"],
                "total_value": result["total_value"],
                "wallet_data": wallet_data,
                "chain_key": chain_key
            }
            
            results[wallet_addr] = {
                "success": True,
                "quantity": result["quantity"]
            }
            
            log.info(f"📝 معاملة محضرة للمحفظة {wallet_addr[:8]}...: {result['quantity']} قطعة")
    
    return results

def get_prepared_transaction(slug: str, wallet_address: str, w3: Web3 = None) -> dict:
    global prepared_transactions
    
    if slug not in prepared_transactions:
        return None
    
    if wallet_address not in prepared_transactions[slug]:
        return None
    
    prep = prepared_transactions[slug][wallet_address]
    tx = prep["tx"].copy()
    
    if w3:
        try:
            tx["nonce"] = w3.eth.get_transaction_count(wallet_address, "pending")
        except:
            pass
        
        try:
            tx["gasPrice"] = w3.eth.gas_price
        except:
            pass
    
    return {
        "tx": tx,
        "quantity": prep["quantity"],
        "total_value": prep["total_value"],
        "wallet_data": prep["wallet_data"],
        "chain_key": prep.get("chain_key", "ink")
    }

def clear_prepared_transactions(slug: str = None, wallet_address: str = None):
    global prepared_transactions
    
    if slug is None:
        prepared_transactions = {}
        return
    
    if slug in prepared_transactions:
        if wallet_address is None:
            del prepared_transactions[slug]
        elif wallet_address in prepared_transactions[slug]:
            del prepared_transactions[slug][wallet_address]
            if not prepared_transactions[slug]:
                del prepared_transactions[slug]

# ==================== دوال الشراء ====================

def attempt_purchase_with_prepared_tx(
    w3: Web3,
    private_key: str,
    wallet_address: str,
    slug: str,
    eth_price_usd: float,
    max_gas_fee_usd: float,
) -> dict:
    try:
        checksum_wallet = Web3.to_checksum_address(wallet_address)
    except Exception as e:
        return {"success": False, "wallet": wallet_address, "reason": "invalid_address", "error": str(e)}
    
    prep = get_prepared_transaction(slug, wallet_address, w3)
    if not prep:
        return {"success": False, "wallet": wallet_address, "reason": "no_prepared_tx"}
    
    tx = prep["tx"]
    quantity = prep["quantity"]
    
    balance_usd = get_wallet_balance_usd(w3, checksum_wallet, eth_price_usd)
    if balance_usd < MIN_BALANCE_RESERVE_USD:
        return {"success": False, "wallet": checksum_wallet, "reason": "balance_too_low"}
    
    try:
        gas_price = w3.eth.gas_price
        gas_fee_usd = (tx["gas"] * gas_price / 1e18) * eth_price_usd
        
        if gas_fee_usd > max_gas_fee_usd:
            return {"success": False, "wallet": checksum_wallet, "reason": "gas_too_high"}
        
        total_cost_wei = prep["total_value"] + (tx["gas"] * gas_price)
        wallet_balance_wei = w3.eth.get_balance(checksum_wallet)
        
        if wallet_balance_wei < total_cost_wei:
            return {"success": False, "wallet": checksum_wallet, "reason": "insufficient_funds"}
    except Exception as e:
        return {"success": False, "wallet": checksum_wallet, "reason": "pre_send_check_failed", "error": str(e)}
    
    for attempt in range(MAX_RETRY_ATTEMPTS):
        try:
            log.info(f"📤 محاولة {attempt + 1}/{MAX_RETRY_ATTEMPTS} لإرسال المعاملة المحضرة")
            
            if attempt > 0:
                tx["nonce"] = w3.eth.get_transaction_count(checksum_wallet, "pending")
            
            signed = w3.eth.account.sign_transaction(tx, private_key=private_key)
            tx_hash = w3.eth.send_raw_transaction(signed.raw_transaction)
            
            log.info(f"✅ تم إرسال المعاملة: {tx_hash.hex()[:10]}...")
            
            receipt_result = wait_for_transaction_receipt(w3, tx_hash.hex())
            
            if receipt_result["success"]:
                log.info(f"✅ شراء ناجح! كمية: {quantity}")
                
                clear_prepared_transactions(slug, wallet_address)
                
                return {
                    "success": True,
                    "wallet": checksum_wallet,
                    "tx_hash": tx_hash.hex(),
                    "quantity": quantity,
                    "gas_fee_usd": gas_fee_usd,
                    "total_value_wei": prep["total_value"],
                    "used_prepared": True,
                }
            else:
                if attempt < MAX_RETRY_ATTEMPTS - 1:
                    time.sleep(calculate_retry_delay(attempt))
                    continue
                else:
                    return {"success": False, "wallet": checksum_wallet, "reason": "transaction_failed"}
            
        except Exception as e:
            error_analysis = analyze_error(e)
            
            if not error_analysis.get("retryable", False):
                return {"success": False, "wallet": checksum_wallet, "reason": error_analysis["reason"]}
            
            if attempt < MAX_RETRY_ATTEMPTS - 1:
                time.sleep(calculate_retry_delay(attempt))
                continue
            else:
                return {"success": False, "wallet": checksum_wallet, "reason": error_analysis["reason"]}
    
    return {"success": False, "wallet": checksum_wallet, "reason": "max_retries_exceeded"}

def attempt_purchase_single_wallet(
    w3: Web3,
    private_key: str,
    wallet_address: str,
    nft_contract: str,
    price_wei_per_token: int,
    max_per_wallet,
    remaining_supply: int,
    eth_price_usd: float,
    max_gas_fee_usd: float,
) -> dict:
    try:
        checksum_wallet = Web3.to_checksum_address(wallet_address)
        checksum_contract = Web3.to_checksum_address(nft_contract)
    except Exception as e:
        return {"success": False, "wallet": wallet_address, "reason": "invalid_address"}

    balance_usd = get_wallet_balance_usd(w3, checksum_wallet, eth_price_usd)
    if balance_usd < MIN_BALANCE_RESERVE_USD:
        return {"success": False, "wallet": checksum_wallet, "reason": "balance_too_low"}

    gas_fee_usd = estimate_gas_fee_usd(w3, eth_price_usd)
    if gas_fee_usd > max_gas_fee_usd:
        return {"success": False, "wallet": checksum_wallet, "reason": "gas_too_high"}

    fee_recipient = get_fee_recipient(w3, checksum_contract) or ZERO_ADDRESS

    quantity = decide_quantity(max_per_wallet, remaining_supply)
    total_value = price_wei_per_token * quantity
    
    log.info(f"💰 شراء {quantity} من {checksum_contract[:8]}... للمحفظة {checksum_wallet[:8]}...")

    try:
        contract = w3.eth.contract(address=SEADROP_ADDRESS, abi=SEADROP_ABI)
        nonce = w3.eth.get_transaction_count(checksum_wallet, "pending")
        
        tx = contract.functions.mintPublic(
            checksum_contract,
            fee_recipient,
            ZERO_ADDRESS,
            quantity,
        ).build_transaction({
            "from": checksum_wallet,
            "value": total_value,
            "nonce": nonce,
            "chainId": w3.eth.chain_id,
        })
    except Exception as e:
        return {"success": False, "wallet": checksum_wallet, "reason": "build_tx_failed", "error": str(e)}

    try:
        estimated_gas = w3.eth.estimate_gas(tx)
        tx["gas"] = int(estimated_gas * GAS_LIMIT_SAFETY_MARGIN)
    except ContractLogicError as e:
        error_analysis = analyze_error(e)
        return {"success": False, "wallet": checksum_wallet, "reason": f"contract_reverted", "error": str(e)}
    except Exception as e:
        return {"success": False, "wallet": checksum_wallet, "reason": "gas_estimation_failed"}

    try:
        gas_price = w3.eth.gas_price
        actual_gas_fee_usd = (tx["gas"] * gas_price / 1e18) * eth_price_usd
        
        if actual_gas_fee_usd > max_gas_fee_usd:
            return {"success": False, "wallet": checksum_wallet, "reason": "gas_too_high_actual"}
        
        total_cost_wei = total_value + (tx["gas"] * gas_price)
        wallet_balance_wei = w3.eth.get_balance(checksum_wallet)
        
        if wallet_balance_wei < total_cost_wei:
            return {"success": False, "wallet": checksum_wallet, "reason": "insufficient_funds"}
    except Exception as e:
        return {"success": False, "wallet": checksum_wallet, "reason": "pre_send_check_failed"}

    for attempt in range(MAX_RETRY_ATTEMPTS):
        try:
            log.info(f"📤 محاولة {attempt + 1}/{MAX_RETRY_ATTEMPTS}")
            
            if attempt > 0:
                tx["nonce"] = w3.eth.get_transaction_count(checksum_wallet, "pending")
            
            signed = w3.eth.account.sign_transaction(tx, private_key=private_key)
            tx_hash = w3.eth.send_raw_transaction(signed.raw_transaction)
            
            log.info(f"✅ تم الإرسال: {tx_hash.hex()[:10]}...")
            
            receipt_result = wait_for_transaction_receipt(w3, tx_hash.hex())
            
            if receipt_result["success"]:
                log.info(f"✅ شراء ناجح! كمية: {quantity}")
                return {
                    "success": True,
                    "wallet": checksum_wallet,
                    "tx_hash": tx_hash.hex(),
                    "quantity": quantity,
                    "gas_fee_usd": actual_gas_fee_usd,
                    "total_value_wei": total_value,
                    "used_prepared": False,
                }
            else:
                if attempt < MAX_RETRY_ATTEMPTS - 1:
                    time.sleep(calculate_retry_delay(attempt))
                    continue
                else:
                    return {"success": False, "wallet": checksum_wallet, "reason": "transaction_failed"}
            
        except ContractLogicError as e:
            error_analysis = analyze_error(e)
            
            if not error_analysis.get("retryable", False):
                return {"success": False, "wallet": checksum_wallet, "reason": error_analysis["reason"]}
            
            if attempt < MAX_RETRY_ATTEMPTS - 1:
                time.sleep(calculate_retry_delay(attempt))
                continue
            else:
                return {"success": False, "wallet": checksum_wallet, "reason": error_analysis["reason"]}
            
        except Exception as e:
            error_analysis = analyze_error(e)
            
            if not error_analysis.get("retryable", False):
                return {"success": False, "wallet": checksum_wallet, "reason": error_analysis["reason"]}
            
            if attempt < MAX_RETRY_ATTEMPTS - 1:
                time.sleep(calculate_retry_delay(attempt))
                continue
            else:
                return {"success": False, "wallet": checksum_wallet, "reason": error_analysis["reason"]}
    
    return {"success": False, "wallet": checksum_wallet, "reason": "max_retries_exceeded"}

def get_onchain_phase_info(w3: Web3, nft_contract: str) -> dict:
    try:
        seadrop = w3.eth.contract(address=SEADROP_ADDRESS, abi=SEADROP_ABI)
        public_drop = seadrop.functions.getPublicDrop(
            Web3.to_checksum_address(nft_contract)
        ).call()
        
        current_time = int(time.time())
        
        return {
            "mintPrice": int(public_drop[0]),
            "startTime": int(public_drop[1]),
            "endTime": int(public_drop[2]),
            "maxTotalMintableByWallet": int(public_drop[3]),
            "feeBps": int(public_drop[4]),
            "restrictFeeRecipients": bool(public_drop[5]),
            "is_active": int(public_drop[1]) <= current_time <= int(public_drop[2]),
            "time_until_start": max(0, int(public_drop[1]) - current_time),
            "time_until_end": max(0, int(public_drop[2]) - current_time)
        }
    except Exception as e:
        log.warning(f"[المرحلة] تعذر القراءة: {e}")
        return None
