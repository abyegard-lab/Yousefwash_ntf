"""
NFT Auto Mint Bot - Buyer Engine
SeaDrop public mint, multi-wallet, one transaction per wallet.
"""
import logging, time, re
from dataclasses import dataclass
from typing import Optional, Any
from web3 import Web3
from web3.exceptions import ContractLogicError

log = logging.getLogger("buyer")

SEADROP_ADDRESS = Web3.to_checksum_address(
    "0x00005EA00Ac477B1030CE78506496e8C2dE24bf5"
)
ZERO_ADDRESS = Web3.to_checksum_address("0x0000000000000000000000000000000000000000")

ABI = [
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
        "type": "function"
    },
    {
        "inputs": [{"name": "nftContract", "type": "address"}],
        "name": "getAllowedFeeRecipients",
        "outputs": [{"name": "", "type": "address[]"}],
        "stateMutability": "view",
        "type": "function"
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
                {"name": "maxTotalMintable", "type": "uint16"},
                {"name": "feeBPS", "type": "uint16"},
                {"name": "restrictFeeRecipients", "type": "bool"},
            ],
            "name": "",
            "type": "tuple"
        }],
        "stateMutability": "view",
        "type": "function"
    },
]

@dataclass
class MintResult:
    success: bool
    wallet: str
    quantity: int = 0
    tx_hash: Optional[str] = None
    gas_fee_usd: float = 0.0
    reason: str = ""
    error: str = ""
    pending: bool = False

def _tuple_drop(v):
    names = ["mintPrice", "startTime", "endTime", "maxTotalMintableByWallet",
             "maxTotalMintable", "feeBPS", "restrictFeeRecipients"]
    if hasattr(v, "_asdict"):
        d = v._asdict()
        return {k: d.get(k) for k in names}
    if isinstance(v, dict):
        return {k: v.get(k) for k in names}
    return {k: v[i] for i, k in enumerate(names)}

def get_public_drop(w3: Web3, nft_contract: str) -> dict:
    """جلب تفاصيل المينت من العقد"""
    c = w3.eth.contract(address=SEADROP_ADDRESS, abi=ABI)
    return _tuple_drop(c.functions.getPublicDrop(Web3.to_checksum_address(nft_contract)).call())

def choose_quantity(drop: dict, remaining_supply: int, cap: int = 20) -> int:
    """اختيار الكمية المناسبة للشراء"""
    wallet_limit = int(drop.get("maxTotalMintableByWallet") or 0)
    candidates = [cap]
    if wallet_limit > 0:
        candidates.append(wallet_limit)
    if remaining_supply > 0:
        candidates.append(remaining_supply)
    return max(1, min(candidates))

def get_fee_recipient(w3: Web3, nft_contract: str, drop: dict) -> Optional[str]:
    """الحصول على مستلم الرسوم"""
    c = w3.eth.contract(address=SEADROP_ADDRESS, abi=ABI)
    try:
        allowed = c.functions.getAllowedFeeRecipients(
            Web3.to_checksum_address(nft_contract)
        ).call()
    except Exception as e:
        if bool(drop.get("restrictFeeRecipients")):
            raise RuntimeError(f"cannot read allowed fee recipients: {e}")
        return ZERO_ADDRESS
    if bool(drop.get("restrictFeeRecipients")):
        if not allowed:
            raise RuntimeError("fee recipients are restricted but list is empty")
        return Web3.to_checksum_address(allowed[0])
    return ZERO_ADDRESS

def estimate_fee_usd(w3: Web3, tx: dict, eth_price_usd: float) -> tuple[float, int]:
    """تقدير رسوم الغاز بالدولار"""
    try:
        gas = int(w3.eth.estimate_gas(tx))
    except Exception:
        gas = 21000
    
    try:
        gas_price = int(w3.eth.gas_price)
    except Exception:
        gas_price = int(tx.get("gasPrice", 0))
    
    fee_eth = gas * gas_price / 1e18
    fee_usd = fee_eth * eth_price_usd
    return fee_usd, gas

def attempt_purchase(
    w3: Web3,
    private_key: str,
    wallet: str,
    nft_contract: str,
    eth_price_usd: float,
    max_gas_fee_usd: float,
    remaining_supply: int,
    max_qty: int = 20,
    chain_id: Optional[int] = None
) -> dict:
    """
    محاولة شراء NFT من عقد SeaDrop
    """
    wallet = Web3.to_checksum_address(wallet)
    nft_contract = Web3.to_checksum_address(nft_contract)
    
    try:
        drop = get_public_drop(w3, nft_contract)
    except Exception as e:
        return MintResult(False, wallet, reason=f"drop_fetch_failed: {str(e)[:100]}").__dict__

    now = int(time.time())
    
    # التحقق من السعر (نشتري فقط المجاني)
    if int(drop["mintPrice"]) != 0:
        return MintResult(False, wallet, reason="not_free").__dict__
    
    # التحقق من الوقت
    if now < int(drop["startTime"]):
        return MintResult(False, wallet, reason="not_started").__dict__
    if int(drop["endTime"]) and now > int(drop["endTime"]):
        return MintResult(False, wallet, reason="phase_ended").__dict__

    # اختيار الكمية
    quantity = choose_quantity(drop, remaining_supply, max_qty)
    
    try:
        fee_recipient = get_fee_recipient(w3, nft_contract, drop)
    except Exception as e:
        return MintResult(False, wallet, quantity, reason=f"fee_recipient_failed: {str(e)[:100]}").__dict__

    contract = w3.eth.contract(address=SEADROP_ADDRESS, abi=ABI)
    total_value = int(drop["mintPrice"]) * quantity

    try:
        nonce = int(w3.eth.get_transaction_count(wallet, "pending"))
    except Exception as e:
        return MintResult(False, wallet, quantity, reason=f"nonce_failed: {str(e)[:100]}").__dict__

    tx_base = {
        "from": wallet,
        "value": total_value,
        "nonce": nonce,
        "chainId": int(chain_id if chain_id is not None else w3.eth.chain_id),
    }

    call_args = [nft_contract, fee_recipient, wallet, quantity]
    
    # محاكاة المعاملة
    try:
        contract.functions.mintPublic(*call_args).call(tx_base)
    except ContractLogicError as e:
        return MintResult(False, wallet, quantity, reason=f"contract_revert: {str(e)[:100]}").__dict__
    except Exception as e:
        return MintResult(False, wallet, quantity, reason=f"simulation_failed: {str(e)[:100]}").__dict__

    # تقدير رسوم الغاز
    try:
        gas_fee_usd, gas = estimate_fee_usd(w3, tx_base, eth_price_usd)
    except Exception as e:
        return MintResult(False, wallet, quantity, reason=f"gas_estimate_failed: {str(e)[:100]}").__dict__
    
    if gas_fee_usd > max_gas_fee_usd:
        return MintResult(False, wallet, quantity, reason="gas_too_high",
                          gas_fee_usd=gas_fee_usd).__dict__

    # بناء المعاملة
    try:
        tx = contract.functions.mintPublic(*call_args).build_transaction(tx_base)
    except Exception as e:
        return MintResult(False, wallet, quantity, reason=f"build_tx_failed: {str(e)[:100]}").__dict__
    
    tx["gas"] = max(21000, int(gas * 1.05))
    
    try:
        tx["gasPrice"] = int(w3.eth.gas_price)
    except Exception:
        pass

    # التحقق من الرصيد
    try:
        balance = int(w3.eth.get_balance(wallet))
    except Exception as e:
        return MintResult(False, wallet, quantity, reason=f"balance_check_failed: {str(e)[:100]}").__dict__
    
    max_cost = int(tx.get("value", 0)) + int(tx["gas"]) * int(tx.get("gasPrice", 0))
    if balance < max_cost:
        return MintResult(False, wallet, quantity, reason="insufficient_balance",
                          gas_fee_usd=gas_fee_usd).__dict__

    # توقيع وإرسال المعاملة
    try:
        signed = w3.eth.account.sign_transaction(tx, private_key)
        tx_hash = w3.eth.send_raw_transaction(signed.raw_transaction)
        h = tx_hash.hex()
    except Exception as e:
        return MintResult(False, wallet, quantity, reason=f"send_tx_failed: {str(e)[:100]}").__dict__

    log.info(f"🚀 sent ONE tx | wallet={wallet[:10]} | quantity={quantity} | tx={h[:20]}...")

    # انتظار تأكيد المعاملة
    try:
        receipt = w3.eth.wait_for_transaction_receipt(h, timeout=90, poll_latency=1)
    except Exception as e:
        return MintResult(False, wallet, quantity, tx_hash=h, gas_fee_usd=gas_fee_usd,
                          reason="pending", error=str(e), pending=True).__dict__

    if int(receipt.status) == 1:
        gas_used = int(receipt.gasUsed)
        try:
            gas_price = int(tx.get("gasPrice", w3.eth.gas_price))
        except Exception:
            gas_price = 0
        actual_fee = gas_used * gas_price / 1e18 * eth_price_usd
        return MintResult(True, wallet, quantity, h, actual_fee, "success").__dict__

    return MintResult(False, wallet, quantity, h, gas_fee_usd,
                      "transaction_reverted", "receipt.status=0").__dict__
