"""
محرك الشراء التلقائي المتعدد المحافظ عبر عقد SeaDrop.
"""

import asyncio
import logging
import time
from web3 import Web3
from web3.exceptions import ContractLogicError
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

MIN_BALANCE_RESERVE_USD = 0.10
FEW_THRESHOLD = 20
LIMITED_BUY_QTY = 15
GAS_LIMIT_SAFETY_MARGIN = 1.2

MAX_RETRY_ATTEMPTS = 3
RETRY_DELAY_SECONDS = 2

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

def decide_quantity(max_per_wallet, remaining_supply: int) -> int:
    if max_per_wallet is None:
        qty = 5
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

def analyze_error(error: Exception) -> dict:
    error_str = str(error).lower()
    
    reasons = {
        "insufficient_funds": ["insufficient funds", "insufficient balance", "not enough funds"],
        "gas_issue": ["gas", "out of gas", "gas limit"],
        "nonce_issue": ["nonce", "transaction with the same nonce"],
        "contract_reverted": ["execution reverted", "revert", "vm exception"],
        "sold_out": ["sold out", "max supply", "no tokens left"],
        "already_minted": ["already minted", "already claimed", "max per wallet"],
    }
    
    for reason, keywords in reasons.items():
        for keyword in keywords:
            if keyword in error_str:
                return {"reason": reason, "message": error_str}
    
    return {"reason": "unknown_error", "message": error_str}

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
        log.error(f"[عنوان غير صالح] للمحفظة {wallet_address[:8]}...: {e}")
        return {"success": False, "wallet": wallet_address, "reason": "invalid_address", "error": str(e)}

    balance_usd = get_wallet_balance_usd(w3, checksum_wallet, eth_price_usd)
    if balance_usd < MIN_BALANCE_RESERVE_USD:
        log.warning(f"⚠️ رصيد منخفض للمحفظة {checksum_wallet[:8]}...: ${balance_usd:.4f}")
        return {"success": False, "wallet": checksum_wallet, "reason": "balance_too_low", "balance_usd": balance_usd}

    gas_fee_usd = estimate_gas_fee_usd(w3, eth_price_usd)
    if gas_fee_usd > max_gas_fee_usd:
        log.warning(f"⚠️ رسوم غاز مرتفعة للمحفظة {checksum_wallet[:8]}...: ${gas_fee_usd:.4f}")
        return {"success": False, "wallet": checksum_wallet, "reason": "gas_too_high", "gas_fee_usd": gas_fee_usd}

    fee_recipient = get_fee_recipient(w3, checksum_contract)
    if not fee_recipient:
        log.warning(f"⚠️ لا يوجد مستفيد للعقد {checksum_contract[:8]}... - استخدام عنوان الصفر")
        fee_recipient = ZERO_ADDRESS

    quantity = decide_quantity(max_per_wallet, remaining_supply)
    total_value = price_wei_per_token * quantity
    
    log.info(f"💰 محاولة شراء {quantity} من {checksum_contract[:8]}... للمحفظة {checksum_wallet[:8]}...")

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
        log.error(f"❌ فشل بناء المعاملة للمحفظة {checksum_wallet[:8]}...: {e}")
        return {"success": False, "wallet": checksum_wallet, "reason": "build_tx_failed", "error": str(e)}

    try:
        estimated_gas = w3.eth.estimate_gas(tx)
        tx["gas"] = int(estimated_gas * GAS_LIMIT_SAFETY_MARGIN)
        log.info(f"📊 الغاز المقدر: {estimated_gas} → مع هامش: {tx['gas']}")
    except ContractLogicError as e:
        error_analysis = analyze_error(e)
        log.error(f"❌ فشل تقدير الغاز (منطق العقد) للمحفظة {checksum_wallet[:8]}...: {error_analysis['reason']}")
        return {"success": False, "wallet": checksum_wallet, "reason": f"contract_reverted_{error_analysis['reason']}", "error": str(e)}
    except Exception as e:
        log.error(f"❌ فشل تقدير الغاز للمحفظة {checksum_wallet[:8]}...: {e}")
        return {"success": False, "wallet": checksum_wallet, "reason": "gas_estimation_failed", "error": str(e)}

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
        log.error(f"❌ فشل التحقق من التكلفة للمحفظة {checksum_wallet[:8]}...: {e}")
        return {"success": False, "wallet": checksum_wallet, "reason": "pre_send_check_failed", "error": str(e)}

    for attempt in range(MAX_RETRY_ATTEMPTS):
        try:
            log.info(f"📤 محاولة {attempt + 1}/{MAX_RETRY_ATTEMPTS} لإرسال المعاملة للمحفظة {checksum_wallet[:8]}...")
            
            signed = w3.eth.account.sign_transaction(tx, private_key=private_key)
            tx_hash = w3.eth.send_raw_transaction(signed.raw_transaction)
            
            log.info(f"✅ شراء ناجح للمحفظة {checksum_wallet[:8]}...: {tx_hash.hex()} — كمية: {quantity}")
            return {
                "success": True,
                "wallet": checksum_wallet,
                "tx_hash": tx_hash.hex(),
                "quantity": quantity,
                "gas_fee_usd": actual_gas_fee_usd,
                "total_value_wei": total_value,
                "attempt": attempt + 1
            }
            
        except ContractLogicError as e:
            error_analysis = analyze_error(e)
            log.warning(f"⚠️ محاولة {attempt + 1} فشلت (منطق العقد) للمحفظة {checksum_wallet[:8]}...: {error_analysis['reason']}")
            
            if error_analysis['reason'] in ['sold_out', 'already_minted']:
                return {"success": False, "wallet": checksum_wallet, "reason": error_analysis['reason'], "error": str(e)}
            
            if attempt == MAX_RETRY_ATTEMPTS - 1:
                return {"success": False, "wallet": checksum_wallet, "reason": f"contract_reverted_{error_analysis['reason']}", "error": str(e)}
            
            time.sleep(RETRY_DELAY_SECONDS)
            
        except Exception as e:
            error_analysis = analyze_error(e)
            log.warning(f"⚠️ محاولة {attempt + 1} فشلت للمحفظة {checksum_wallet[:8]}...: {error_analysis['reason']}")
            
            if attempt == MAX_RETRY_ATTEMPTS - 1:
                return {"success": False, "wallet": checksum_wallet, "reason": error_analysis['reason'], "error": str(e)}
            
            time.sleep(RETRY_DELAY_SECONDS * (attempt + 1))
    
    return {"success": False, "wallet": checksum_wallet, "reason": "max_retries_exceeded", "max_attempts": MAX_RETRY_ATTEMPTS}
