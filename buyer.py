"""
محرك الشراء التلقائي المتعدد المحافظ عبر عقد SeaDrop.
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

# الإعدادات
MIN_BALANCE_RESERVE_USD = 0.10
GAS_LIMIT_SAFETY_MARGIN = 1.2
FREE_PRICE_THRESHOLD_USD = 0.01
MAX_RETRY_ATTEMPTS = 3

# التخزين العالمي
prepared_transactions = {}
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
        log.error(f"[الرصيد] خطأ: {e}")
        return 0.0

def get_fee_recipient(w3: Web3, nft_contract: str):
    try:
        seadrop = w3.eth.contract(address=SEADROP_ADDRESS, abi=SEADROP_ABI)
        recipients = seadrop.functions.getAllowedFeeRecipients(
            Web3.to_checksum_address(nft_contract)
        ).call()
        if not recipients:
            return ZERO_ADDRESS
        return Web3.to_checksum_address(recipients[0])
    except Exception as e:
        log.error(f"[عنوان الرسوم] خطأ: {e}")
        return ZERO_ADDRESS

def get_onchain_public_price_wei(w3: Web3, nft_contract: str):
    try:
        seadrop = w3.eth.contract(address=SEADROP_ADDRESS, abi=SEADROP_ABI)
        public_drop = seadrop.functions.getPublicDrop(
            Web3.to_checksum_address(nft_contract)
        ).call()
        return int(public_drop[0])
    except Exception as e:
        log.warning(f"[سعر on-chain] خطأ: {e}")
        return None

def is_free_or_negligible(price_wei: int, eth_price_usd: float) -> bool:
    """تحديد إذا كان السعر مجانياً"""
    if price_wei is None:
        return False
    if price_wei == 0:
        return True
    price_usd = (price_wei / 1e18) * eth_price_usd
    return price_usd < FREE_PRICE_THRESHOLD_USD

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
                }
        except TransactionNotFound:
            pass
        except Exception as e:
            log.warning(f"[الاستلام] خطأ: {e}")
        time.sleep(0.5)
    return {"success": False, "error": "timeout"}

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
    """محاولة شراء لمحفظة واحدة"""
    
    try:
        checksum_wallet = Web3.to_checksum_address(wallet_address)
        checksum_contract = Web3.to_checksum_address(nft_contract)
    except Exception as e:
        return {"success": False, "reason": "invalid_address", "error": str(e)}

    # فحص الرصيد
    balance_usd = get_wallet_balance_usd(w3, checksum_wallet, eth_price_usd)
    if balance_usd < MIN_BALANCE_RESERVE_USD:
        return {"success": False, "reason": "balance_too_low", "balance_usd": balance_usd}

    # تحديد الكمية
    quantity = 1
    if max_per_wallet and max_per_wallet > 1:
        quantity = min(max_per_wallet, remaining_supply)
    
    total_value = price_wei_per_token * quantity
    
    log.info(f"💰 شراء {quantity} قطعة بسعر {price_wei_per_token} wei")

    # بناء المعاملة
    try:
        contract = w3.eth.contract(address=SEADROP_ADDRESS, abi=SEADROP_ABI)
        nonce = w3.eth.get_transaction_count(checksum_wallet, "pending")
        fee_recipient = get_fee_recipient(w3, checksum_contract)
        
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
        return {"success": False, "reason": "build_tx_failed", "error": str(e)}

    # تقدير الغاز
    try:
        estimated_gas = w3.eth.estimate_gas(tx)
        tx["gas"] = int(estimated_gas * GAS_LIMIT_SAFETY_MARGIN)
        log.info(f"📊 الغاز المقدر: {estimated_gas} → {tx['gas']}")
    except Exception as e:
        return {"success": False, "reason": "gas_estimation_failed", "error": str(e)}

    # فحص التكلفة النهائية
    try:
        gas_price = w3.eth.gas_price
        gas_fee_usd = (tx["gas"] * gas_price / 1e18) * eth_price_usd
        
        if gas_fee_usd > max_gas_fee_usd:
            return {"success": False, "reason": "gas_too_high", "gas_fee_usd": gas_fee_usd}
        
        total_cost_wei = total_value + (tx["gas"] * gas_price)
        wallet_balance_wei = w3.eth.get_balance(checksum_wallet)
        
        if wallet_balance_wei < total_cost_wei:
            return {"success": False, "reason": "insufficient_funds"}
    except Exception as e:
        return {"success": False, "reason": "pre_send_check_failed", "error": str(e)}

    # إرسال المعاملة مع إعادة المحاولة
    for attempt in range(MAX_RETRY_ATTEMPTS):
        try:
            log.info(f"📤 محاولة {attempt + 1}/{MAX_RETRY_ATTEMPTS}")
            
            if attempt > 0:
                tx["nonce"] = w3.eth.get_transaction_count(checksum_wallet, "pending")
            
            signed = w3.eth.account.sign_transaction(tx, private_key=private_key)
            tx_hash = w3.eth.send_raw_transaction(signed.raw_transaction)
            
            log.info(f"✅ تم الإرسال: {tx_hash.hex()[:10]}...")
            
            receipt = wait_for_transaction_receipt(w3, tx_hash.hex())
            
            if receipt["success"]:
                log.info(f"✅ شراء ناجح! كمية: {quantity}")
                return {
                    "success": True,
                    "wallet": checksum_wallet,
                    "tx_hash": tx_hash.hex(),
                    "quantity": quantity,
                    "gas_fee_usd": gas_fee_usd,
                }
            else:
                if attempt < MAX_RETRY_ATTEMPTS - 1:
                    time.sleep(0.5 * (attempt + 1))
                    continue
                else:
                    return {"success": False, "reason": "transaction_failed"}
            
        except ContractLogicError as e:
            error_str = str(e).lower()
            if "sold out" in error_str:
                return {"success": False, "reason": "sold_out"}
            if "already minted" in error_str or "max per wallet" in error_str:
                return {"success": False, "reason": "already_minted"}
            if attempt < MAX_RETRY_ATTEMPTS - 1:
                time.sleep(0.5 * (attempt + 1))
                continue
            else:
                return {"success": False, "reason": "contract_reverted", "error": str(e)}
            
        except Exception as e:
            if attempt < MAX_RETRY_ATTEMPTS - 1:
                time.sleep(0.5 * (attempt + 1))
                continue
            else:
                return {"success": False, "reason": "unknown_error", "error": str(e)}
    
    return {"success": False, "reason": "max_retries_exceeded"}
