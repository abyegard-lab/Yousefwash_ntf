"""
محرك الشراء التلقائي المتعدد المحافظ عبر عقد SeaDrop - شبكة Ink
"""

import asyncio
import logging
from web3 import Web3
from web3.exceptions import ContractLogicError

log = logging.getLogger("buyer")

# عنوان عقد SeaDrop على شبكة Ink
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

# إعدادات خاصة بـ Ink
MIN_BALANCE_RESERVE_USD = 0.05
FEW_THRESHOLD = 20
LIMITED_BUY_QTY = 15
MIN_BUY_QTY = 10  # ✅ الحد الأدنى للشراء = 10
GAS_LIMIT_SAFETY_MARGIN = 1.2
FREE_PRICE_THRESHOLD_USD = 0.01

# قفل خاص لكل محفظة
wallet_locks = {}

def get_wallet_lock(wallet_address: str) -> asyncio.Lock:
    addr = wallet_address.lower()
    if addr not in wallet_locks:
        wallet_locks[addr] = asyncio.Lock()
    return wallet_locks[addr]


def get_web3(rpc_url: str) -> Web3:
    return Web3(Web3.HTTPProvider(rpc_url, request_kwargs={'timeout': 30}))


def get_wallet_balance_usd(w3: Web3, wallet_address: str, eth_price_usd: float) -> float:
    try:
        checksum_wallet = Web3.to_checksum_address(wallet_address)
        balance_wei = w3.eth.get_balance(checksum_wallet)
        return (balance_wei / 1e18) * eth_price_usd
    except Exception as e:
        log.error(f"[الرصيد] تعذر القراءة للمحفظة {wallet_address[:8]}...: {e}")
        return 0.0


def estimate_gas_fee_usd(w3: Web3, eth_price_usd: float, gas_units: int = 250_000) -> float:
    """تقدير رسوم الغاز - زيادة الوحدات لتغطية الكمية الأكبر"""
    try:
        gas_price_wei = w3.eth.gas_price
        fee_eth = (gas_price_wei * gas_units) / 1e18
        return fee_eth * eth_price_usd
    except Exception as e:
        log.warning(f"[الغاز] تعذر التقدير: {e}")
        return float("inf")


def get_fee_recipient(w3: Web3, nft_contract: str) -> str | None:
    try:
        seadrop = w3.eth.contract(address=SEADROP_ADDRESS, abi=SEADROP_ABI)
        recipients = seadrop.functions.getAllowedFeeRecipients(
            Web3.to_checksum_address(nft_contract)
        ).call()
        if not recipients:
            return None
        return Web3.to_checksum_address(recipients[0])
    except Exception as e:
        log.error(f"[عنوان الرسوم] خطأ استعلام: {e}")
        return None


def decide_quantity(max_per_wallet: int | None, remaining_supply: int) -> int:
    """
    تحديد الكمية المناسبة للشراء
    - الحد الأدنى 10 للمينتات المجانية
    """
    # ✅ الحد الأدنى 10
    if max_per_wallet is None:
        qty = MIN_BUY_QTY  # 10
    elif max_per_wallet <= FEW_THRESHOLD:
        qty = max_per_wallet
        # إذا كان الحد الأقصى أقل من 10، نشتري الحد الأقصى
        if qty < MIN_BUY_QTY:
            qty = min(max_per_wallet, remaining_supply)
    else:
        qty = min(LIMITED_BUY_QTY, remaining_supply)
        # التأكد من أن الكمية لا تقل عن 10
        if qty < MIN_BUY_QTY:
            qty = min(MIN_BUY_QTY, remaining_supply)
    
    # التأكد من أن الكمية بين 10 والحد الأقصى المتاح
    final_qty = max(MIN_BUY_QTY, min(qty, remaining_supply))
    
    log.info(f"[تحديد الكمية] الحد الأقصى: {max_per_wallet}, المتبقي: {remaining_supply} → الكمية: {final_qty}")
    return final_qty


def get_onchain_public_price_wei(w3: Web3, nft_contract: str) -> int | None:
    try:
        seadrop = w3.eth.contract(address=SEADROP_ADDRESS, abi=SEADROP_ABI)
        public_drop = seadrop.functions.getPublicDrop(
            Web3.to_checksum_address(nft_contract)
        ).call()
        return int(public_drop[0])
    except Exception as e:
        log.warning(f"[سعر on-chain] تعذر القراءة: {e}")
        return None


def is_free_mint(price_wei: int, eth_price_usd: float) -> bool:
    if price_wei == 0:
        return True
    price_usd = (price_wei / 1e18) * eth_price_usd
    return price_usd < FREE_PRICE_THRESHOLD_USD


def attempt_purchase_single_wallet(
    w3: Web3,
    private_key: str,
    wallet_address: str,
    nft_contract: str,
    price_wei_per_token: int,
    max_per_wallet: int | None,
    remaining_supply: int,
    eth_price_usd: float,
    max_gas_fee_usd: float,
) -> dict:
    try:
        checksum_wallet = Web3.to_checksum_address(wallet_address)
        checksum_contract = Web3.to_checksum_address(nft_contract)
    except Exception as e:
        return {"success": False, "wallet": wallet_address, "reason": "invalid_address", "error": str(e)}

    # التحقق من أن المينت مجاني
    if not is_free_mint(price_wei_per_token, eth_price_usd):
        return {
            "success": False, 
            "wallet": checksum_wallet, 
            "reason": "not_free_mint", 
            "price_usd": (price_wei_per_token / 1e18) * eth_price_usd
        }

    # التحقق من الرصيد
    balance_usd = get_wallet_balance_usd(w3, checksum_wallet, eth_price_usd)
    if balance_usd < MIN_BALANCE_RESERVE_USD:
        return {"success": False, "wallet": checksum_wallet, "reason": "balance_too_low", "balance_usd": balance_usd}

    # جلب مستلم الرسوم
    fee_recipient = get_fee_recipient(w3, checksum_contract)
    if not fee_recipient:
        return {"success": False, "wallet": checksum_wallet, "reason": "no_fee_recipient"}

    # ✅ تحديد الكمية (الحد الأدنى 10)
    quantity = decide_quantity(max_per_wallet, remaining_supply)
    total_value = price_wei_per_token * quantity  # = 0 للمجاني

    log.info(f"[محاولة شراء] {checksum_wallet[:8]}... كمية: {quantity}, السعر: {(price_wei_per_token/1e18)*eth_price_usd:.6f}$")

    try:
        contract = w3.eth.contract(address=SEADROP_ADDRESS, abi=SEADROP_ABI)
        nonce = w3.eth.get_transaction_count(checksum_wallet, "pending")

        # بناء المعاملة
        tx_data = {
            "from": checksum_wallet,
            "value": total_value,
            "nonce": nonce,
            "chainId": w3.eth.chain_id,
            "gas": 300000,  # ✅ زيادة الغاز الافتراضي للكميات الكبيرة
            "gasPrice": w3.eth.gas_price,
        }

        # بناء المعاملة عبر contract
        tx = contract.functions.mintPublic(
            checksum_contract,
            Web3.to_checksum_address(fee_recipient),
            ZERO_ADDRESS,
            quantity,
        ).build_transaction(tx_data)

        # محاولة تقدير الغاز الفعلي
        try:
            estimated_gas = w3.eth.estimate_gas(tx)
            tx["gas"] = int(estimated_gas * GAS_LIMIT_SAFETY_MARGIN)
            log.info(f"[تقدير الغاز] {checksum_wallet[:8]}...: {estimated_gas} → {tx['gas']}")
        except ContractLogicError as e:
            log.warning(f"[محاكاة فاشلة] {checksum_wallet[:8]}...: {e}")
            tx["gas"] = 300000  # ✅ قيمة افتراضية للكميات الكبيرة
        except Exception as e:
            log.warning(f"[تقدير الغاز] {checksum_wallet[:8]}...: {e}")
            tx["gas"] = 300000

        # التحقق من رسوم الغاز
        actual_gas_fee_usd = (tx["gas"] * tx["gasPrice"] / 1e18) * eth_price_usd
        if actual_gas_fee_usd > max_gas_fee_usd:
            return {"success": False, "wallet": checksum_wallet, "reason": "gas_too_high", "gas_fee_usd": actual_gas_fee_usd}

        # التحقق من الرصيد الكافي
        total_cost_wei = total_value + (tx["gas"] * tx["gasPrice"])
        wallet_balance_wei = w3.eth.get_balance(checksum_wallet)
        if wallet_balance_wei < total_cost_wei:
            return {"success": False, "wallet": checksum_wallet, "reason": "insufficient_funds"}

        # توقيع وإرسال المعاملة
        signed = w3.eth.account.sign_transaction(tx, private_key=private_key)
        tx_hash = w3.eth.send_raw_transaction(signed.raw_transaction)

        log.info(f"[✅ شراء ناجح - {checksum_wallet[:8]}] {tx_hash.hex()} — كمية: {quantity}")
        return {
            "success": True,
            "wallet": checksum_wallet,
            "tx_hash": tx_hash.hex(),
            "quantity": quantity,
            "gas_fee_usd": actual_gas_fee_usd,
            "total_value_wei": total_value,
        }

    except ContractLogicError as e:
        log.error(f"[خطأ منطق العقد - {checksum_wallet[:8]}] {e}")
        return {"success": False, "wallet": checksum_wallet, "reason": "contract_error", "error": str(e)}
    except Exception as e:
        log.error(f"[خطأ إرسال - {checksum_wallet[:8]}] {type(e).__name__}: {e}")
        return {"success": False, "wallet": checksum_wallet, "reason": "tx_error", "error": str(e)}
