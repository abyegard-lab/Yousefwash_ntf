"""
محرك الشراء التلقائي المتعدد المحافظ عبر عقد SeaDrop - شبكة Ink
"""

import asyncio
import logging
import traceback
from web3 import Web3
from web3.exceptions import ContractLogicError, ContractPanicError

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
MIN_BUY_QTY = 10
GAS_LIMIT_SAFETY_MARGIN = 1.2
FREE_PRICE_THRESHOLD_USD = 0.01
MAX_GAS_LIMIT = 500000  # حد أقصى للغاز

# قفل خاص لكل محفظة
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


def estimate_gas_fee_usd(w3: Web3, eth_price_usd: float, gas_units: int = 300_000) -> float:
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
    if max_per_wallet is None:
        qty = MIN_BUY_QTY
    elif max_per_wallet <= FEW_THRESHOLD:
        qty = max_per_wallet
        if qty < MIN_BUY_QTY:
            qty = min(max_per_wallet, remaining_supply)
    else:
        qty = min(LIMITED_BUY_QTY, remaining_supply)
        if qty < MIN_BUY_QTY:
            qty = min(MIN_BUY_QTY, remaining_supply)
    
    final_qty = max(1, min(qty, remaining_supply))
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

    # تحديد الكمية
    quantity = decide_quantity(max_per_wallet, remaining_supply)
    total_value = price_wei_per_token * quantity

    log.info(f"[محاولة شراء] {checksum_wallet[:8]}... كمية: {quantity}, السعر: {(price_wei_per_token/1e18)*eth_price_usd:.8f}$")

    try:
        contract = w3.eth.contract(address=SEADROP_ADDRESS, abi=SEADROP_ABI)
        nonce = w3.eth.get_transaction_count(checksum_wallet, "pending")
        gas_price = w3.eth.gas_price

        log.info(f"[معاملة] nonce: {nonce}, gas_price: {gas_price/1e9:.2f} Gwei")

        # بناء المعاملة
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

        # محاولة تقدير الغاز الفعلي
        estimated_gas = None
        try:
            estimated_gas = w3.eth.estimate_gas(tx)
            tx["gas"] = int(estimated_gas * GAS_LIMIT_SAFETY_MARGIN)
            log.info(f"[تقدير الغاز] {checksum_wallet[:8]}...: {estimated_gas} → {tx['gas']}")
        except ContractPanicError as e:
            # خطأ في العقد (panic)
            error_msg = str(e)
            log.error(f"[خطأ عقد - Panic] {checksum_wallet[:8]}...: {error_msg[:200]}")
            
            # محاولة فهم الخطأ
            if "reverted" in error_msg.lower():
                return {
                    "success": False, 
                    "wallet": checksum_wallet, 
                    "reason": "contract_reverted",
                    "error": error_msg[:300]
                }
            else:
                tx["gas"] = 300000
                log.warning(f"[استخدام غاز افتراضي] {checksum_wallet[:8]}...: 300000")
                
        except ContractLogicError as e:
            # خطأ منطق العقد
            error_msg = str(e)
            log.error(f"[خطأ عقد - Logic] {checksum_wallet[:8]}...: {error_msg[:200]}")
            
            # التحقق من أسباب محددة
            if "out of gas" in error_msg.lower():
                tx["gas"] = 400000
                log.info(f"[زيادة الغاز] {checksum_wallet[:8]}...: 400000")
            elif "execution reverted" in error_msg.lower():
                return {
                    "success": False, 
                    "wallet": checksum_wallet, 
                    "reason": "execution_reverted",
                    "error": "العقد رفض المعاملة - قد يكون المينت انتهى أو الكمية غير متاحة"
                }
            else:
                tx["gas"] = 300000
                log.warning(f"[استخدام غاز افتراضي] {checksum_wallet[:8]}...: 300000")
                
        except Exception as e:
            # أي خطأ آخر في التقدير
            log.warning(f"[تقدير الغاز - خطأ] {checksum_wallet[:8]}...: {type(e).__name__}: {str(e)[:200]}")
            tx["gas"] = 300000
            log.info(f"[استخدام غاز افتراضي] {checksum_wallet[:8]}...: 300000")

        # التأكد من أن الغاز ضمن الحدود
        if tx.get("gas", 0) > MAX_GAS_LIMIT:
            log.warning(f"[تجاوز حد الغاز] {checksum_wallet[:8]}...: {tx['gas']} → تخفيض إلى {MAX_GAS_LIMIT}")
            tx["gas"] = MAX_GAS_LIMIT

        # التحقق من رسوم الغاز
        actual_gas_fee_usd = (tx["gas"] * tx["gasPrice"] / 1e18) * eth_price_usd
        if actual_gas_fee_usd > max_gas_fee_usd:
            return {
                "success": False, 
                "wallet": checksum_wallet, 
                "reason": "gas_too_high", 
                "gas_fee_usd": actual_gas_fee_usd,
                "max_gas_fee_usd": max_gas_fee_usd
            }

        # التحقق من الرصيد الكافي
        total_cost_wei = total_value + (tx["gas"] * tx["gasPrice"])
        wallet_balance_wei = w3.eth.get_balance(checksum_wallet)
        
        log.info(f"[الرصيد] {checksum_wallet[:8]}...: {wallet_balance_wei/1e18:.6f} ETH, التكلفة: {total_cost_wei/1e18:.6f} ETH")
        
        if wallet_balance_wei < total_cost_wei:
            return {
                "success": False, 
                "wallet": checksum_wallet, 
                "reason": "insufficient_funds",
                "balance_eth": wallet_balance_wei/1e18,
                "cost_eth": total_cost_wei/1e18
            }

        # توقيع وإرسال المعاملة
        log.info(f"[توقيع المعاملة] {checksum_wallet[:8]}...")
        signed = w3.eth.account.sign_transaction(tx, private_key=private_key)
        
        log.info(f"[إرسال المعاملة] {checksum_wallet[:8]}...")
        tx_hash = w3.eth.send_raw_transaction(signed.raw_transaction)

        log.info(f"[✅ شراء ناجح - {checksum_wallet[:8]}] {tx_hash.hex()} — كمية: {quantity}")
        return {
            "success": True,
            "wallet": checksum_wallet,
            "tx_hash": tx_hash.hex(),
            "quantity": quantity,
            "gas_fee_usd": actual_gas_fee_usd,
            "total_value_wei": total_value,
            "gas_used": tx["gas"],
        }

    except ContractPanicError as e:
        log.error(f"[خطأ عقد - Panic] {checksum_wallet[:8]}...: {str(e)[:300]}")
        return {
            "success": False, 
            "wallet": checksum_wallet, 
            "reason": "contract_panic",
            "error": str(e)[:300]
        }
        
    except ContractLogicError as e:
        log.error(f"[خطأ عقد - Logic] {checksum_wallet[:8]}...: {str(e)[:300]}")
        return {
            "success": False, 
            "wallet": checksum_wallet, 
            "reason": "contract_logic_error",
            "error": str(e)[:300]
        }
        
    except Exception as e:
        log.error(f"[خطأ إرسال - {checksum_wallet[:8]}] {type(e).__name__}: {str(e)[:300]}")
        log.debug(traceback.format_exc())
        return {
            "success": False, 
            "wallet": checksum_wallet, 
            "reason": "tx_error",
            "error_type": type(e).__name__,
            "error": str(e)[:300]
        }
