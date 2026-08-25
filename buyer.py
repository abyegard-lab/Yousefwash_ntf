"""
buyer.py - محرك الشراء التلقائي المتعدد المحافظ عبر عقد SeaDrop.
نسخة محسنة مع سحب الكمية كاملة دفعة واحدة
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

# ✅ الإعدادات المتقدمة
MIN_BALANCE_RESERVE_USD = 0.10
GAS_LIMIT_SAFETY_MARGIN = 1.2
MAX_RETRY_ATTEMPTS = 5
BASE_RETRY_DELAY = 1
MAX_RETRY_DELAY = 10

# ✅ تصنيف الأخطاء
ERROR_CATEGORIES = {
    "fatal": ["sold_out", "already_minted", "invalid_address", "contract_paused"],
    "retryable": ["nonce_issue", "gas_issue", "network_error", "timeout"],
    "insufficient_funds": ["insufficient_funds", "balance_too_low"],
    "contract_reverted": ["contract_reverted", "not_eligible", "phase_not_active"],
}

wallet_locks = {}

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

def get_onchain_phase_info(w3: Web3, nft_contract: str) -> dict:
    """
    جلب معلومات المرحلة الحالية من العقد مباشرة
    """
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
        log.warning(f"[المرحلة] تعذر القراءة للعقد {nft_contract[:8]}...: {e}")
        return None

def analyze_error(error: Exception) -> dict:
    """
    تحليل ذكي للأخطاء مع تصنيفها وتحديد ما إذا كانت قابلة لإعادة المحاولة
    """
    error_str = str(error).lower()
    
    error_patterns = {
        "insufficient_funds": {
            "keywords": ["insufficient funds", "insufficient balance", "not enough funds", "insufficient eth"],
            "category": "insufficient_funds",
            "retryable": False,
            "action": "top_up_wallet"
        },
        "gas_too_low": {
            "keywords": ["gas too low", "gas price too low", "underpriced"],
            "category": "retryable",
            "retryable": True,
            "action": "increase_gas"
        },
        "out_of_gas": {
            "keywords": ["out of gas", "gas limit exceeded", "gas required exceeds"],
            "category": "retryable",
            "retryable": True,
            "action": "increase_gas_limit"
        },
        "nonce_too_low": {
            "keywords": ["nonce too low", "nonce already used", "replacement transaction underpriced"],
            "category": "retryable",
            "retryable": True,
            "action": "refresh_nonce"
        },
        "nonce_too_high": {
            "keywords": ["nonce too high", "nonce gap"],
            "category": "retryable",
            "retryable": True,
            "action": "refresh_nonce"
        },
        "sold_out": {
            "keywords": ["sold out", "max supply", "no tokens left", "exceeds total supply", "all tokens minted"],
            "category": "fatal",
            "retryable": False,
            "action": "stop"
        },
        "already_minted": {
            "keywords": ["already minted", "already claimed", "max per wallet", "wallet limit", "exceeds max"],
            "category": "fatal",
            "retryable": False,
            "action": "stop"
        },
        "not_eligible": {
            "keywords": ["not eligible", "not allowed", "not whitelisted", "not in allowlist", "unauthorized"],
            "category": "fatal",
            "retryable": False,
            "action": "stop"
        },
        "phase_not_active": {
            "keywords": ["phase not active", "not started", "not yet started", "ended", "phase ended"],
            "category": "fatal",
            "retryable": False,
            "action": "wait_for_phase"
        },
        "contract_paused": {
            "keywords": ["paused", "contract paused", "minting paused"],
            "category": "fatal",
            "retryable": False,
            "action": "wait_for_unpause"
        },
        "network_error": {
            "keywords": ["connection", "timeout", "network", "temporarily unavailable", "429", "rate limit"],
            "category": "retryable",
            "retryable": True,
            "action": "retry_with_backoff"
        },
        "rpc_error": {
            "keywords": ["rpc error", "internal error", "server error", "502", "503", "504"],
            "category": "retryable",
            "retryable": True,
            "action": "switch_rpc"
        },
        "contract_reverted": {
            "keywords": ["execution reverted", "revert", "vm exception"],
            "category": "contract_reverted",
            "retryable": True,
            "action": "analyze_revert_reason"
        },
    }
    
    for error_name, error_info in error_patterns.items():
        for keyword in error_info["keywords"]:
            if keyword in error_str:
                return {
                    "reason": error_name,
                    "category": error_info["category"],
                    "retryable": error_info["retryable"],
                    "action": error_info["action"],
                    "message": error_str[:200],
                    "original_error": error
                }
    
    return {
        "reason": "unknown_error",
        "category": "unknown",
        "retryable": True,
        "action": "retry_with_backoff",
        "message": error_str[:200],
        "original_error": error
    }

def calculate_retry_delay(attempt: int, error_analysis: dict) -> float:
    """حساب تأخير ذكي بناءً على نوع الخطأ وعدد المحاولات"""
    base_delay = BASE_RETRY_DELAY
    
    if error_analysis["reason"] in ["network_error", "rpc_error", "timeout"]:
        base_delay = 2
    
    if error_analysis["reason"] in ["nonce_too_low", "nonce_too_high"]:
        base_delay = 0.5
    
    exponential_delay = base_delay * (2 ** attempt)
    jitter = random.uniform(0, 0.5)
    
    return min(exponential_delay + jitter, MAX_RETRY_DELAY)

def wait_for_transaction_receipt(w3: Web3, tx_hash: str, timeout: int = 60) -> dict:
    """انتظار تأكيد المعاملة مع مهلة زمنية"""
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
            log.warning(f"[الاستلام] خطأ في انتظار الاستلام: {e}")
        
        time.sleep(1)
    
    return {
        "success": False,
        "receipt": None,
        "error": "timeout"
    }

# ✅ دالة الشراء المحسنة مع سحب الكمية كاملة
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
    quantity: int = 1,  # ✅ الكمية المطلوبة
) -> dict:
    """
    محاولة الشراء مع سحب الكمية كاملة دفعة واحدة
    """
    try:
        checksum_wallet = Web3.to_checksum_address(wallet_address)
        checksum_contract = Web3.to_checksum_address(nft_contract)
    except Exception as e:
        log.error(f"[عنوان غير صالح] للمحفظة {wallet_address[:8]}...: {e}")
        return {"success": False, "wallet": wallet_address, "reason": "invalid_address", "error": str(e)}

    # ✅ التحقق من الكمية
    if quantity < 1:
        quantity = 1
    
    # ✅ التأكد من عدم تجاوز الكمية المتبقية
    if remaining_supply > 0 and quantity > remaining_supply:
        log.info(f"📊 تعديل الكمية من {quantity} إلى {remaining_supply} (المتبقية)")
        quantity = remaining_supply
    
    # ✅ التأكد من عدم تجاوز الحد الأقصى للمحفظة
    if max_per_wallet and quantity > max_per_wallet:
        log.info(f"📊 تعديل الكمية من {quantity} إلى {max_per_wallet} (حد المحفظة)")
        quantity = max_per_wallet
    
    log.info(f"📊 الكمية النهائية للشراء: {quantity}")

    # ✅ فحص الرصيد
    balance_usd = get_wallet_balance_usd(w3, checksum_wallet, eth_price_usd)
    if balance_usd < MIN_BALANCE_RESERVE_USD:
        log.warning(f"⚠️ رصيد منخفض للمحفظة {checksum_wallet[:8]}...: ${balance_usd:.4f}")
        return {"success": False, "wallet": checksum_wallet, "reason": "balance_too_low", "balance_usd": balance_usd}

    # ✅ فحص رسوم الغاز
    gas_fee_usd = estimate_gas_fee_usd(w3, eth_price_usd)
    if gas_fee_usd > max_gas_fee_usd:
        log.warning(f"⚠️ رسوم غاز مرتفعة للمحفظة {checksum_wallet[:8]}...: ${gas_fee_usd:.4f}")
        return {"success": False, "wallet": checksum_wallet, "reason": "gas_too_high", "gas_fee_usd": gas_fee_usd}

    # ✅ الحصول على مستفيد الرسوم
    fee_recipient = get_fee_recipient(w3, checksum_contract)
    if not fee_recipient:
        log.warning(f"⚠️ لا يوجد مستفيد للعقد {checksum_contract[:8]}... - استخدام عنوان الصفر")
        fee_recipient = ZERO_ADDRESS

    # ✅ حساب القيمة الإجمالية
    total_value = price_wei_per_token * quantity
    
    log.info(f"💰 محاولة شراء {quantity} من {checksum_contract[:8]}... للمحفظة {checksum_wallet[:8]}...")

    # ✅ بناء المعاملة
    try:
        contract = w3.eth.contract(address=SEADROP_ADDRESS, abi=SEADROP_ABI)
        nonce = w3.eth.get_transaction_count(checksum_wallet, "pending")
        
        tx = contract.functions.mintPublic(
            checksum_contract,
            fee_recipient,
            ZERO_ADDRESS,
            quantity,  # ✅ سحب الكمية كاملة دفعة واحدة
        ).build_transaction({
            "from": checksum_wallet,
            "value": total_value,
            "nonce": nonce,
            "chainId": w3.eth.chain_id,
        })
    except Exception as e:
        error_analysis = analyze_error(e)
        log.error(f"❌ فشل بناء المعاملة للمحفظة {checksum_wallet[:8]}...: {error_analysis['reason']}")
        return {"success": False, "wallet": checksum_wallet, "reason": "build_tx_failed", "error": str(e), "analysis": error_analysis}

    # ✅ تقدير الغاز مع تحليل الأخطاء
    try:
        estimated_gas = w3.eth.estimate_gas(tx)
        tx["gas"] = int(estimated_gas * GAS_LIMIT_SAFETY_MARGIN)
        log.info(f"📊 الغاز المقدر: {estimated_gas} → مع هامش: {tx['gas']}")
    except ContractLogicError as e:
        error_analysis = analyze_error(e)
        log.error(f"❌ فشل تقدير الغاز (منطق العقد) للمحفظة {checksum_wallet[:8]}...: {error_analysis['reason']}")
        
        if error_analysis["reason"] in ["phase_not_active", "not_eligible"]:
            return {"success": False, "wallet": checksum_wallet, "reason": f"contract_reverted_{error_analysis['reason']}", "error": str(e), "analysis": error_analysis}
        
        return {"success": False, "wallet": checksum_wallet, "reason": f"contract_reverted_{error_analysis['reason']}", "error": str(e), "analysis": error_analysis}
    except Exception as e:
        error_analysis = analyze_error(e)
        log.error(f"❌ فشل تقدير الغاز للمحفظة {checksum_wallet[:8]}...: {error_analysis['reason']}")
        return {"success": False, "wallet": checksum_wallet, "reason": "gas_estimation_failed", "error": str(e), "analysis": error_analysis}

    # ✅ التحقق من التكلفة
    try:
        gas_price = w3.eth.gas_price
        actual_gas_fee_usd = (tx["gas"] * gas_price / 1e18) * eth_price_usd
        
        if actual_gas_fee_usd > max_gas_fee_usd:
            log.warning(f"⚠️ رسوم غاز فعلية مرتفعة للمحفظة {checksum_wallet[:8]}...: ${actual_gas_fee_usd:.4f}")
            return {"success": False, "wallet": checksum_wallet, "reason": "gas_too_high_actual", "gas_fee_usd": actual_gas_fee_usd}
        
        total_cost_wei = total_value + (tx["gas"] * gas_price)
        wallet_balance_wei = w3.eth.get_balance(checksum_wallet)
        
        if wallet_balance_wei < total_cost_wei:
            log.warning(f"⚠️ رصيد غير كافٍ للمحفظة {checksum_wallet[:8]}...")
            return {"success": False, "wallet": checksum_wallet, "reason": "insufficient_funds_for_total_cost"}
    except Exception as e:
        error_analysis = analyze_error(e)
        log.error(f"❌ فشل التحقق من التكلفة للمحفظة {checksum_wallet[:8]}...: {error_analysis['reason']}")
        return {"success": False, "wallet": checksum_wallet, "reason": "pre_send_check_failed", "error": str(e), "analysis": error_analysis}

    # ✅ نظام إعادة المحاولة الذكي
    for attempt in range(MAX_RETRY_ATTEMPTS):
        try:
            log.info(f"📤 محاولة {attempt + 1}/{MAX_RETRY_ATTEMPTS} لإرسال المعاملة للمحفظة {checksum_wallet[:8]}...")
            
            signed = w3.eth.account.sign_transaction(tx, private_key=private_key)
            tx_hash = w3.eth.send_raw_transaction(signed.raw_transaction)
            
            log.info(f"✅ تم إرسال المعاملة للمحفظة {checksum_wallet[:8]}...: {tx_hash.hex()}")
            
            # ✅ انتظار تأكيد المعاملة
            receipt_result = wait_for_transaction_receipt(w3, tx_hash.hex())
            
            if receipt_result["success"]:
                log.info(f"✅ شراء {quantity} بنجاح للمحفظة {checksum_wallet[:8]}...: {tx_hash.hex()}")
                return {
                    "success": True,
                    "wallet": checksum_wallet,
                    "tx_hash": tx_hash.hex(),
                    "quantity": quantity,
                    "gas_fee_usd": actual_gas_fee_usd,
                    "total_value_wei": total_value,
                    "attempt": attempt + 1,
                    "gas_used": receipt_result.get("gas_used"),
                    "block_number": receipt_result.get("block_number")
                }
            else:
                log.warning(f"⚠️ المعاملة فشلت على السلسلة للمحفظة {checksum_wallet[:8]}...")
                
                if attempt < MAX_RETRY_ATTEMPTS - 1:
                    nonce = w3.eth.get_transaction_count(checksum_wallet, "pending")
                    tx["nonce"] = nonce
                    delay = calculate_retry_delay(attempt, {"reason": "transaction_failed", "retryable": True})
                    time.sleep(delay)
                    continue
                else:
                    return {"success": False, "wallet": checksum_wallet, "reason": "transaction_failed", "error": "Transaction reverted on chain"}
            
        except ContractLogicError as e:
            error_analysis = analyze_error(e)
            log.warning(f"⚠️ محاولة {attempt + 1} فشلت (منطق العقد) للمحفظة {checksum_wallet[:8]}...: {error_analysis['reason']}")
            
            if error_analysis["category"] == "fatal":
                if error_analysis["reason"] in ["sold_out", "already_minted"]:
                    return {"success": False, "wallet": checksum_wallet, "reason": error_analysis["reason"], "error": str(e), "analysis": error_analysis}
            
            if not error_analysis["retryable"]:
                return {"success": False, "wallet": checksum_wallet, "reason": f"contract_reverted_{error_analysis['reason']}", "error": str(e), "analysis": error_analysis}
            
            if attempt == MAX_RETRY_ATTEMPTS - 1:
                return {"success": False, "wallet": checksum_wallet, "reason": f"contract_reverted_{error_analysis['reason']}", "error": str(e), "analysis": error_analysis}
            
            delay = calculate_retry_delay(attempt, error_analysis)
            time.sleep(delay)
            
            try:
                nonce = w3.eth.get_transaction_count(checksum_wallet, "pending")
                tx["nonce"] = nonce
            except:
                pass
            
        except Exception as e:
            error_analysis = analyze_error(e)
            log.warning(f"⚠️ محاولة {attempt + 1} فشلت للمحفظة {checksum_wallet[:8]}...: {error_analysis['reason']}")
            
            if not error_analysis["retryable"]:
                return {"success": False, "wallet": checksum_wallet, "reason": error_analysis["reason"], "error": str(e), "analysis": error_analysis}
            
            if attempt == MAX_RETRY_ATTEMPTS - 1:
                return {"success": False, "wallet": checksum_wallet, "reason": error_analysis["reason"], "error": str(e), "analysis": error_analysis}
            
            delay = calculate_retry_delay(attempt, error_analysis)
            time.sleep(delay)
            
            if error_analysis["reason"] in ["nonce_too_low", "nonce_too_high"]:
                try:
                    nonce = w3.eth.get_transaction_count(checksum_wallet, "pending")
                    tx["nonce"] = nonce
                except:
                    pass
    
    return {
        "success": False,
        "wallet": checksum_wallet,
        "reason": "max_retries_exceeded",
        "max_attempts": MAX_RETRY_ATTEMPTS
    }
