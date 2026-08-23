"""
محرك الشراء التلقائي المتعدد المحافظ عبر عقد SeaDrop - شبكة Ink
"""

import asyncio
import logging
from web3 import Web3
from web3.exceptions import ContractLogicError

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

MIN_BALANCE_RESERVE_USD = 0.05
FREE_PRICE_THRESHOLD_USD = 0.01
GAS_LIMIT_SAFETY_MARGIN = 1.2

wallet_locks = {}

def get_wallet_lock(wallet_address: str) -> asyncio.Lock:
    addr = wallet_address.lower()
    if addr not in wallet_locks:
        wallet_locks[addr] = asyncio.Lock()
    return wallet_locks[addr]


def get_web3(rpc_url: str) -> Web3:
    return Web3(Web3.HTTPProvider(rpc_url, request_kwargs={'timeout': 60}))


def get_wallet_balance_usd(w3: Web3, wallet_address: str, eth_price_usd: float) -> float:
    try:
        checksum_wallet = Web3.to_checksum_address(wallet_address)
        balance_wei = w3.eth.get_balance(checksum_wallet)
        return (balance_wei / 1e18) * eth_price_usd
    except Exception as e:
        log.error(f"[الرصيد] تعذر القراءة للمحفظة {wallet_address[:8]}...: {e}")
        return 0.0


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
    max_per_wallet: int,
    remaining_supply: int,
    eth_price_usd: float,
    max_gas_fee_usd: float,
) -> dict:
    """
    محاولة الشراء بمحفظة واحدة
    - الكمية = max_per_wallet (من العقد)
    - الغاز = تقدير من العقد
    """
    try:
        checksum_wallet = Web3.to_checksum_address(wallet_address)
        checksum_contract = Web3.to_checksum_address(nft_contract)
    except Exception as e:
        return {"success": False, "wallet": wallet_address, "reason": "invalid_address", "error": str(e)}

    # ✅ التحقق من المجانية
    if not is_free_mint(price_wei_per_token, eth_price_usd):
        return {
            "success": False,
            "wallet": checksum_wallet,
            "reason": "not_free_mint",
        }

    # ✅ التحقق من الرصيد
    balance_usd = get_wallet_balance_usd(w3, checksum_wallet, eth_price_usd)
    if balance_usd < MIN_BALANCE_RESERVE_USD:
        return {"success": False, "wallet": checksum_wallet, "reason": "balance_too_low"}

    # ✅ جلب مستلم الرسوم
    fee_recipient = get_fee_recipient(w3, checksum_contract)
    if not fee_recipient:
        return {"success": False, "wallet": checksum_wallet, "reason": "no_fee_recipient"}

    # ✅ الكمية = من العقد (max_per_wallet)
    quantity = min(max_per_wallet, remaining_supply)
    if quantity <= 0:
        return {"success": False, "wallet": checksum_wallet, "reason": "no_quantity"}

    total_value = price_wei_per_token * quantity  # = 0 للمجاني

    log.info(f"[شراء] {checksum_wallet[:8]}... كمية: {quantity} (من العقد)")

    try:
        contract = w3.eth.contract(address=SEADROP_ADDRESS, abi=SEADROP_ABI)
        nonce = w3.eth.get_transaction_count(checksum_wallet, "pending")
        gas_price = w3.eth.gas_price

        # ✅ بناء المعاملة
        tx = contract.functions.mintPublic(
            checksum_contract,
            Web3.to_checksum_address(fee_recipient),
            ZERO_ADDRESS,
            quantity,
        ).build_transaction({
            "from": checksum_wallet,
            "value": total_value,
            "nonce": nonce,
            "chainId": w3.eth.chain_id,
            "gasPrice": gas_price,
        })

        # ✅ تقدير الغاز من العقد
        try:
            estimated_gas = w3.eth.estimate_gas(tx)
            tx["gas"] = int(estimated_gas * GAS_LIMIT_SAFETY_MARGIN)
            log.info(f"[غاز] {checksum_wallet[:8]}...: {estimated_gas} → {tx['gas']}")
        except Exception as e:
            log.warning(f"[تقدير الغاز] {checksum_wallet[:8]}...: {e}")
            tx["gas"] = 300000  # قيمة افتراضية

        # ✅ التحقق من رسوم الغاز
        actual_gas_fee_usd = (tx["gas"] * tx["gasPrice"] / 1e18) * eth_price_usd
        if actual_gas_fee_usd > max_gas_fee_usd:
            return {
                "success": False,
                "wallet": checksum_wallet,
                "reason": "gas_too_high",
                "gas_fee_usd": actual_gas_fee_usd
            }

        # ✅ التحقق من الرصيد الكافي
        total_cost_wei = total_value + (tx["gas"] * tx["gasPrice"])
        wallet_balance_wei = w3.eth.get_balance(checksum_wallet)
        if wallet_balance_wei < total_cost_wei:
            return {
                "success": False,
                "wallet": checksum_wallet,
                "reason": "insufficient_funds"
            }

        # ✅ توقيع وإرسال
        signed = w3.eth.account.sign_transaction(tx, private_key=private_key)
        tx_hash = w3.eth.send_raw_transaction(signed.raw_transaction)

        log.info(f"[✅ نجاح - {checksum_wallet[:8]}] كمية: {quantity}, غاز: ${actual_gas_fee_usd:.4f}")
        
        return {
            "success": True,
            "wallet": checksum_wallet,
            "tx_hash": tx_hash.hex(),
            "quantity": quantity,
            "gas_fee_usd": actual_gas_fee_usd,
        }

    except ContractLogicError as e:
        error_msg = str(e)
        if "0xedc01273" in error_msg:
            return {
                "success": False,
                "wallet": checksum_wallet,
                "reason": "max_mint_exceeded",
                "error": "تجاوز الحد الأقصى للشراء"
            }
        return {
            "success": False,
            "wallet": checksum_wallet,
            "reason": "contract_error",
            "error": error_msg[:200]
        }
    except Exception as e:
        log.error(f"[خطأ - {checksum_wallet[:8]}] {type(e).__name__}: {e}")
        return {
            "success": False,
            "wallet": checksum_wallet,
            "reason": "tx_error",
            "error": str(e)[:200]
        }
