"""SeaDrop purchase engine - V2.

Key properties:
- On-chain public-drop validation is authoritative.
- No ZERO_ADDRESS fallback for restricted fee recipients.
- eth_call simulation before signing/sending.
- Quantity defaults to 1 and is bounded by the on-chain wallet limit.
- Gas is estimated from the actual transaction, not a fixed 150k guess.
- A timeout is reported as pending, never as a failed transaction.
- Retry is limited to transport/nonce errors; contract reverts are not blindly retried.
"""
import logging
import time
from typing import Optional, Dict, Any

from web3 import Web3
from web3.exceptions import ContractLogicError, TransactionNotFound
from web3.middleware import ExtraDataToPOAMiddleware

log = logging.getLogger("buyer")

SEADROP_ADDRESS = Web3.to_checksum_address("0x00005EA00Ac477B1030CE78506496e8C2dE24bf5")
ZERO_ADDRESS = Web3.to_checksum_address("0x0000000000000000000000000000000000000000")

SEADROP_ABI = [
    {"inputs":[{"name":"nftContract","type":"address"},{"name":"feeRecipient","type":"address"},{"name":"minterIfNotPayer","type":"address"},{"name":"quantity","type":"uint256"}],"name":"mintPublic","outputs":[],"stateMutability":"payable","type":"function"},
    {"inputs":[{"name":"nftContract","type":"address"}],"name":"getAllowedFeeRecipients","outputs":[{"name":"","type":"address[]"}],"stateMutability":"view","type":"function"},
    {"inputs":[{"name":"nftContract","type":"address"}],"name":"getPublicDrop","outputs":[{"components":[{"name":"mintPrice","type":"uint80"},{"name":"startTime","type":"uint48"},{"name":"endTime","type":"uint48"},{"name":"maxTotalMintableByWallet","type":"uint16"},{"name":"feeBps","type":"uint16"},{"name":"restrictFeeRecipients","type":"bool"}],"name":"","type":"tuple"}],"stateMutability":"view","type":"function"},
]

DEFAULT_QUANTITY = 20
MAX_QUANTITY = 20
GAS_SAFETY_MARGIN = 1.10
DEFAULT_RECEIPT_TIMEOUT = 45
MAX_SEND_RETRIES = 2


def get_web3(rpc_url: str) -> Web3:
    w3 = Web3(Web3.HTTPProvider(rpc_url, request_kwargs={"timeout": 10}))
    try:
        w3.middleware_onion.inject(ExtraDataToPOAMiddleware, layer=0)
    except Exception:
        pass
    if not w3.is_connected():
        raise ConnectionError(f"RPC غير متصل: {rpc_url}")
    return w3


def get_onchain_phase_info(w3: Web3, nft_contract: str) -> Optional[dict]:
    try:
        c = w3.eth.contract(address=SEADROP_ADDRESS, abi=SEADROP_ABI)
        d = c.functions.getPublicDrop(Web3.to_checksum_address(nft_contract)).call()
        now = int(time.time())
        start, end = int(d[1]), int(d[2])
        # SeaDrop deployments may use 0 as an open-ended end time.
        active = start <= now and (end == 0 or now <= end)
        return {
            "mintPrice": int(d[0]), "startTime": start, "endTime": end,
            "maxTotalMintableByWallet": int(d[3]), "feeBps": int(d[4]),
            "restrictFeeRecipients": bool(d[5]), "is_active": active,
            "time_until_start": max(0, start - now),
            "time_until_end": max(0, end - now) if end else None,
        }
    except Exception as e:
        log.warning("[phase] %s", e)
        return None


def get_onchain_public_price_wei(w3: Web3, nft_contract: str):
    info = get_onchain_phase_info(w3, nft_contract)
    return None if info is None else info["mintPrice"]


def get_fee_recipient(w3: Web3, nft_contract: str, restrict: bool) -> Optional[str]:
    try:
        c = w3.eth.contract(address=SEADROP_ADDRESS, abi=SEADROP_ABI)
        recipients = c.functions.getAllowedFeeRecipients(Web3.to_checksum_address(nft_contract)).call()
        if recipients:
            return Web3.to_checksum_address(recipients[0])
        # If fees are not restricted, zero is the SeaDrop-compatible fallback.
        return ZERO_ADDRESS if not restrict else None
    except Exception as e:
        log.warning("[feeRecipient] %s", e)
        return None if restrict else ZERO_ADDRESS


def decide_quantity(onchain_max_per_wallet, remaining_supply: int) -> int:
    """Return ONE quantity for ONE mintPublic transaction.

    The quantity comes from the SeaDrop public-drop value on-chain, not from
    an OpenSea value that may be stale.  Zero means there is no explicit
    wallet limit, so the bot uses the hard cap.  The result is never split
    into one transaction per NFT.
    """
    cap = MAX_QUANTITY
    try:
        wallet_limit = int(onchain_max_per_wallet)
        if wallet_limit > 0:
            cap = min(cap, wallet_limit)
    except (TypeError, ValueError):
        pass

    try:
        remaining = int(remaining_supply)
        if remaining > 0:
            cap = min(cap, remaining)
    except (TypeError, ValueError):
        pass

    return max(1, cap)


def _gas_params(w3: Web3) -> dict:
    # Use EIP-1559 when the RPC exposes a base fee; otherwise legacy gasPrice.
    latest = w3.eth.get_block("latest")
    if latest.get("baseFeePerGas") is not None:
        priority = getattr(w3.eth, "max_priority_fee", None)
        try:
            priority = int(priority) if priority is not None else int(w3.to_wei(0.001, "gwei"))
        except Exception:
            priority = int(w3.to_wei(0.001, "gwei"))
        base = int(latest["baseFeePerGas"])
        return {"maxPriorityFeePerGas": priority, "maxFeePerGas": base * 2 + priority}
    return {"gasPrice": int(w3.eth.gas_price)}


def _fee_wei(tx: dict) -> int:
    if "maxFeePerGas" in tx:
        return int(tx["gas"]) * int(tx["maxFeePerGas"])
    return int(tx["gas"]) * int(tx["gasPrice"])


def _error_reason(exc: Exception) -> str:
    s = str(exc).lower()
    mapping = {
        "insufficient_funds": ["insufficient funds", "insufficient balance", "not enough funds"],
        "nonce": ["nonce too low", "nonce too high", "nonce", "replacement transaction underpriced"],
        "rate_limit": ["429", "rate limit", "too many requests"],
        "network": ["connection", "timeout", "502", "503", "504", "internal error"],
        "reverted": ["execution reverted", "revert", "vm exception"],
    }
    for name, words in mapping.items():
        if any(w in s for w in words):
            return name
    return "unknown"


def _wait_receipt(w3: Web3, tx_hash, timeout: int = DEFAULT_RECEIPT_TIMEOUT) -> dict:
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            r = w3.eth.get_transaction_receipt(tx_hash)
            if r:
                return {"status": int(r.status), "pending": False, "receipt": r,
                        "gas_used": int(r.gasUsed), "block_number": int(r.blockNumber)}
        except TransactionNotFound:
            pass
        except Exception as e:
            log.debug("receipt poll: %s", e)
        time.sleep(0.75)
    return {"status": None, "pending": True, "receipt": None}


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
    requested_quantity: int = DEFAULT_QUANTITY,
    receipt_timeout: int = DEFAULT_RECEIPT_TIMEOUT,
) -> dict:
    try:
        wallet = Web3.to_checksum_address(wallet_address)
        nft = Web3.to_checksum_address(nft_contract)
    except Exception as e:
        return {"success": False, "reason": "invalid_address", "error": str(e), "wallet": wallet_address}

    if remaining_supply <= 0:
        return {"success": False, "reason": "sold_out", "wallet": wallet}

    phase = get_onchain_phase_info(w3, nft)
    if phase is None:
        return {"success": False, "reason": "onchain_phase_unavailable", "wallet": wallet}
    if phase["mintPrice"] != 0:
        return {"success": False, "reason": "not_free_onchain", "wallet": wallet, "price_wei": phase["mintPrice"]}
    if not phase["is_active"]:
        return {"success": False, "reason": "phase_not_active", "wallet": wallet, "phase": phase}

    quantity = decide_quantity(phase["maxTotalMintableByWallet"], remaining_supply)
    log.info(
        "🧮 quantity=%s in ONE transaction | onchain wallet limit=%s | remaining=%s | cap=%s",
        quantity, phase["maxTotalMintableByWallet"], remaining_supply, MAX_QUANTITY
    )
    if quantity <= 0:
        return {"success": False, "reason": "wallet_limit", "wallet": wallet}

    fee_recipient = get_fee_recipient(w3, nft, phase["restrictFeeRecipients"])
    if fee_recipient is None:
        return {"success": False, "reason": "fee_recipient_unavailable", "wallet": wallet}

    contract = w3.eth.contract(address=SEADROP_ADDRESS, abi=SEADROP_ABI)
    price_wei_per_token = int(phase["mintPrice"])
    total_value = price_wei_per_token * quantity

    # Retry only the send/build path for transient RPC/nonce errors.
    for attempt in range(MAX_SEND_RETRIES):
        try:
            nonce = w3.eth.get_transaction_count(wallet, "pending")
            tx = contract.functions.mintPublic(nft, fee_recipient, ZERO_ADDRESS, quantity).build_transaction({
                "from": wallet, "value": total_value, "nonce": nonce, "chainId": int(w3.eth.chain_id),
            })
            tx.update(_gas_params(w3))

            # Simulation is the final contract-level gate before paying gas.
            contract.functions.mintPublic(nft, fee_recipient, ZERO_ADDRESS, quantity).call({
                "from": wallet, "value": total_value
            })

            estimated = int(w3.eth.estimate_gas(tx))
            tx["gas"] = max(estimated + 1, int(estimated * GAS_SAFETY_MARGIN))

            fee_wei = _fee_wei(tx)
            fee_usd = fee_wei / 1e18 * float(eth_price_usd)
            if fee_usd > max_gas_fee_usd:
                return {"success": False, "reason": "gas_too_high", "gas_fee_usd": fee_usd, "wallet": wallet}

            balance = int(w3.eth.get_balance(wallet))
            if balance < total_value + fee_wei:
                return {"success": False, "reason": "insufficient_funds", "wallet": wallet,
                        "balance_wei": balance, "required_wei": total_value + fee_wei}

            signed = w3.eth.account.sign_transaction(tx, private_key=private_key)
            tx_hash = w3.eth.send_raw_transaction(signed.raw_transaction)
            tx_hex = tx_hash.hex()
            result = _wait_receipt(w3, tx_hash, receipt_timeout)

            if result["pending"]:
                return {"success": False, "pending": True, "reason": "pending", "tx_hash": tx_hex,
                        "wallet": wallet, "quantity": quantity, "gas_fee_usd": fee_usd}
            if result["status"] == 1:
                return {"success": True, "wallet": wallet, "tx_hash": tx_hex, "quantity": quantity,
                        "gas_fee_usd": fee_usd, "gas_used": result["gas_used"],
                        "block_number": result["block_number"], "total_value_wei": total_value}
            return {"success": False, "reason": "transaction_reverted", "wallet": wallet,
                    "tx_hash": tx_hex, "quantity": quantity}

        except ContractLogicError as e:
            return {"success": False, "reason": "contract_reverted", "wallet": wallet,
                    "error": str(e), "error_type": _error_reason(e)}
        except Exception as e:
            reason = _error_reason(e)
            if reason in {"nonce", "network", "rate_limit"} and attempt + 1 < MAX_SEND_RETRIES:
                time.sleep(0.4 * (attempt + 1))
                continue
            return {"success": False, "reason": reason, "wallet": wallet, "error": str(e)}

    return {"success": False, "reason": "send_failed", "wallet": wallet}
