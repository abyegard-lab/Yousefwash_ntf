"""
محرك الشراء التلقائي للمينتات المجانية - شبكة Ink
"""

import asyncio
import logging
from web3 import Web3
from web3.exceptions import ContractLogicError

log = logging.getLogger("buyer")

# عنوان عقد SeaDrop
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
MIN_BALANCE_RESERVE_USD = 0.05
FREE_PRICE_THRESHOLD_USD = 0.01
GAS_LIMIT_SAFETY_MARGIN = 1.2
MAX_GAS_LIMIT = 500000

# أقفال المحافظ
wallet_locks = {}

def get_wallet_lock(wallet_address: str) -> asyncio.Lock:
    addr = wallet_address.lower()
    if addr not in wallet_locks:
        wallet_locks[addr] = asyncio.Lock()
    return wallet_locks[addr]


def get_web3(rpc_url: str) -> Web3:
    """إنشاء اتصال Web3"""
    log.info(f"🔄 الاتصال بـ RPC: {rpc_url}")
    w3 = Web3(Web3.HTTPProvider(rpc_url, request_kwargs={'timeout': 60}))
    if w3.is_connected():
        log.info(f"✅ متصل بالشبكة، الكتلة: {w3.eth.block_number}")
    else:
        log.error("❌ فشل الاتصال بالشبكة")
    return w3


def get_wallet_balance_usd(w3: Web3, wallet_address: str, eth_price_usd: float) -> float:
    """جلب رصيد المحفظة بالدولار"""
    try:
        checksum_wallet = Web3.to_checksum_address(wallet_address)
        balance_wei = w3.eth.get_balance(checksum_wallet)
        balance_eth = balance_wei / 1e18
        log.info(f"💰 {wallet_address[:8]}... الرصيد: {balance_eth:.6f} ETH (${balance_eth * eth_price_usd:.4f})")
        return balance_eth * eth_price_usd
    except Exception as e:
        log.error(f"❌ [الرصيد] {wallet_address[:8]}...: {e}")
        return 0.0


def get_fee_recipient(w3: Web3, nft_contract: str) -> str | None:
    """جلب مستلم الرسوم من العقد"""
    try:
        seadrop = w3.eth.contract(address=SEADROP_ADDRESS, abi=SEADROP_ABI)
        recipients = seadrop.functions.getAllowedFeeRecipients(
            Web3.to_checksum_address(nft_contract)
        ).call()
        if not recipients:
            log.warning(f"⚠️ لا يوجد مستلم رسوم للعقد {nft_contract[:8]}...")
            return None
        fee_recipient = Web3.to_checksum_address(recipients[0])
        log.info(f"✅ مستلم الرسوم: {fee_recipient[:8]}...")
        return fee_recipient
    except Exception as e:
        log.error(f"❌ [عنوان الرسوم] {e}")
        return None


def get_onchain_public_price_wei(w3: Web3, nft_contract: str) -> int | None:
    """جلب السعر العام من السلسلة"""
    try:
        seadrop = w3.eth.contract(address=SEADROP_ADDRESS, abi=SEADROP_ABI)
        public_drop = seadrop.functions.getPublicDrop(
            Web3.to_checksum_address(nft_contract)
        ).call()
        price_wei = int(public_drop[0])
        log.info(f"💰 السعر من العقد: {price_wei} wei")
        return price_wei
    except Exception as e:
        log.error(f"❌ [سعر on-chain] {e}")
        return None


def get_max_per_wallet(w3: Web3, nft_contract: str) -> int | None:
    """جلب الحد الأقصى للشراء لكل محفظة من العقد"""
    try:
        seadrop = w3.eth.contract(address=SEADROP_ADDRESS, abi=SEADROP_ABI)
        public_drop = seadrop.functions.getPublicDrop(
            Web3.to_checksum_address(nft_contract)
        ).call()
        max_per_wallet = int(public_drop[3])  # maxTotalMintableByWallet
        log.info(f"📊 الحد الأقصى لكل محفظة: {max_per_wallet}")
        return max_per_wallet
    except Exception as e:
        log.error(f"❌ [max per wallet] {e}")
        return None


def is_free_mint(price_wei: int, eth_price_usd: float) -> bool:
    """التحقق من أن المينت مجاني"""
    if price_wei == 0:
        return True
    price_usd = (price_wei / 1e18) * eth_price_usd
    is_free = price_usd < FREE_PRICE_THRESHOLD_USD
    log.info(f"💰 سعر المينت: ${price_usd:.6f} - {'مجاني ✅' if is_free else 'مدفوع ❌'}")
    return is_free


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
    """
    log.info(f"🔄 بدء محاولة شراء للمحفظة {wallet_address[:8]}...")
    
    try:
        checksum_wallet = Web3.to_checksum_address(wallet_address)
        checksum_contract = Web3.to_checksum_address(nft_contract)
    except Exception as e:
        log.error(f"❌ عنوان غير صالح: {e}")
        return {"success": False, "wallet": wallet_address, "reason": "invalid_address"}

    # 1. التحقق من أن المينت مجاني
    if not is_free_mint(price_wei_per_token, eth_price_usd):
        log.warning(f"⏭️ {checksum_wallet[:8]}... المينت ليس مجانياً")
        return {"success": False, "wallet": checksum_wallet, "reason": "not_free_mint"}

    # 2. التحقق من الرصيد
    balance_usd = get_wallet_balance_usd(w3, checksum_wallet, eth_price_usd)
    if balance_usd < MIN_BALANCE_RESERVE_USD:
        log.warning(f"⚠️ {checksum_wallet[:8]}... رصيد منخفض: ${balance_usd:.4f}")
        return {"success": False, "wallet": checksum_wallet, "reason": "balance_too_low"}

    # 3. جلب مستلم الرسوم
    fee_recipient = get_fee_recipient(w3, checksum_contract)
    if not fee_recipient:
        log.error(f"❌ {checksum_wallet[:8]}... لا يوجد مستلم رسوم")
        return {"success": False, "wallet": checksum_wallet, "reason": "no_fee_recipient"}

    # 4. تحديد الكمية من العقد
    quantity = min(max_per_wallet, remaining_supply)
    if quantity <= 0:
        log.warning(f"⚠️ {checksum_wallet[:8]}... كمية غير متاحة (max: {max_per_wallet}, متبقي: {remaining_supply})")
        return {"success": False, "wallet": checksum_wallet, "reason": "no_quantity"}

    total_value = price_wei_per_token * quantity  # = 0 للمجاني
    log.info(f"📊 {checksum_wallet[:8]}... الكمية: {quantity} (من العقد)")

    try:
        # 5. بناء المعاملة
        contract = w3.eth.contract(address=SEADROP_ADDRESS, abi=SEADROP_ABI)
        nonce = w3.eth.get_transaction_count(checksum_wallet, "pending")
        gas_price = w3.eth.gas_price
        
        log.info(f"📝 {checksum_wallet[:8]}... nonce: {nonce}, gas_price: {gas_price/1e9:.2f} Gwei")

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

        # 6. تقدير الغاز
        try:
            estimated_gas = w3.eth.estimate_gas(tx)
            tx["gas"] = int(estimated_gas * GAS_LIMIT_SAFETY_MARGIN)
            log.info(f"⛽ {checksum_wallet[:8]}... تقدير الغاز: {estimated_gas} → {tx['gas']}")
        except ContractLogicError as e:
            error_msg = str(e)
            log.error(f"❌ {checksum_wallet[:8]}... خطأ في تقدير الغاز: {error_msg[:200]}")
            
            # التحقق من أسباب محددة
            if "0xedc01273" in error_msg:
                return {"success": False, "wallet": checksum_wallet, "reason": "max_mint_exceeded"}
            elif "execution reverted" in error_msg.lower():
                return {"success": False, "wallet": checksum_wallet, "reason": "execution_reverted"}
            else:
                # استخدام قيمة افتراضية
                tx["gas"] = 300000
                log.info(f"⛽ {checksum_wallet[:8]}... استخدام غاز افتراضي: 300000")
        except Exception as e:
            log.warning(f"⚠️ {checksum_wallet[:8]}... خطأ في تقدير الغاز: {e}")
            tx["gas"] = 300000

        # التأكد من أن الغاز ضمن الحدود
        if tx.get("gas", 0) > MAX_GAS_LIMIT:
            log.warning(f"⚠️ {checksum_wallet[:8]}... تخفيض الغاز من {tx['gas']} إلى {MAX_GAS_LIMIT}")
            tx["gas"] = MAX_GAS_LIMIT

        # 7. التحقق من رسوم الغاز
        actual_gas_fee_usd = (tx["gas"] * tx["gasPrice"] / 1e18) * eth_price_usd
        log.info(f"⛽ {checksum_wallet[:8]}... رسوم الغاز المتوقعة: ${actual_gas_fee_usd:.4f}")
        
        if actual_gas_fee_usd > max_gas_fee_usd:
            log.warning(f"⚠️ {checksum_wallet[:8]}... رسوم الغاز مرتفعة: ${actual_gas_fee_usd:.4f} > ${max_gas_fee_usd:.4f}")
            return {"success": False, "wallet": checksum_wallet, "reason": "gas_too_high"}

        # 8. التحقق من الرصيد الكافي
        total_cost_wei = total_value + (tx["gas"] * tx["gasPrice"])
        wallet_balance_wei = w3.eth.get_balance(checksum_wallet)
        
        log.info(f"💰 {checksum_wallet[:8]}... الرصيد: {wallet_balance_wei/1e18:.6f} ETH, التكلفة: {total_cost_wei/1e18:.6f} ETH")
        
        if wallet_balance_wei < total_cost_wei:
            log.warning(f"⚠️ {checksum_wallet[:8]}... رصيد غير كافٍ")
            return {"success": False, "wallet": checksum_wallet, "reason": "insufficient_funds"}

        # 9. توقيع وإرسال المعاملة
        log.info(f"✍️ {checksum_wallet[:8]}... توقيع المعاملة...")
        signed = w3.eth.account.sign_transaction(tx, private_key=private_key)
        
        log.info(f"📤 {checksum_wallet[:8]}... إرسال المعاملة...")
        tx_hash = w3.eth.send_raw_transaction(signed.raw_transaction)
        tx_hash_hex = tx_hash.hex()

        log.info(f"✅ {checksum_wallet[:8]}... شراء ناجح! كمية: {quantity}, Hash: {tx_hash_hex[:16]}...")
        log.info(f"⛽ {checksum_wallet[:8]}... رسوم الغاز الفعلية: ${actual_gas_fee_usd:.4f}")
        
        return {
            "success": True,
            "wallet": checksum_wallet,
            "tx_hash": tx_hash_hex,
            "quantity": quantity,
            "gas_fee_usd": actual_gas_fee_usd,
        }

    except ContractLogicError as e:
        error_msg = str(e)
        log.error(f"❌ {checksum_wallet[:8]}... خطأ في العقد: {error_msg[:300]}")
        
        if "0xedc01273" in error_msg:
            return {"success": False, "wallet": checksum_wallet, "reason": "max_mint_exceeded"}
        else:
            return {"success": False, "wallet": checksum_wallet, "reason": "contract_error", "error": error_msg[:200]}
            
    except Exception as e:
        log.error(f"❌ {checksum_wallet[:8]}... خطأ غير متوقع: {type(e).__name__}: {str(e)[:200]}")
        return {"success": False, "wallet": checksum_wallet, "reason": "tx_error", "error": str(e)[:200]}
