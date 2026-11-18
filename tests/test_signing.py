"""EIP-712 hashing and the signer must match eth-account byte for byte."""

import asyncio
import time
from decimal import Decimal

import pytest
from eip712_reference import Reference
from eth_account import Account
from eth_account.messages import encode_typed_data

from grvt._generated.signing import EIP712_ORDER
from grvt._runtime import GrvtError, Runtime
from grvt._runtime.signing import Domain, PrivateKeySigner, Signature, StructType, grvt_domain, scaled, to_int

KEY = "0x" + "11" * 32

# Shape the generator will emit for the Order signed type.
ORDER_TYPES = {
    "Order": [
        {"name": "subAccountID", "type": "uint64"},
        {"name": "isMarket", "type": "bool"},
        {"name": "timeInForce", "type": "uint8"},
        {"name": "postOnly", "type": "bool"},
        {"name": "reduceOnly", "type": "bool"},
        {"name": "legs", "type": "OrderLeg[]"},
        {"name": "nonce", "type": "uint32"},
        {"name": "expiration", "type": "int64"},
    ],
    "OrderLeg": [
        {"name": "assetID", "type": "uint256"},
        {"name": "contractSize", "type": "uint64"},
        {"name": "limitPrice", "type": "uint64"},
        {"name": "isBuyingContract", "type": "bool"},
    ],
}
ORDER = EIP712_ORDER  # generated hasher
ORDER_MSG = {
    "subAccountID": 8289849667772468,
    "isMarket": False,
    "timeInForce": 1,
    "postOnly": False,
    "reduceOnly": False,
    "legs": [
        {
            "assetID": 0x030501,
            "contractSize": scaled("0.01", 9),
            "limitPrice": scaled("65000.1", 9),
            "isBuyingContract": True,
        }
    ],
    "nonce": 1234567,
    "expiration": 1730800479321350000,
}

# Every primitive kind the reference encoder supports, to keep the test reference honest.
ALL_TYPES = {
    "Mixed": [
        {"name": "who", "type": "address"},
        {"name": "label", "type": "string"},
        {"name": "blob", "type": "bytes"},
        {"name": "tag", "type": "bytes32"},
        {"name": "delta", "type": "int256"},
        {"name": "small", "type": "int8"},
        {"name": "ids", "type": "uint32[]"},
        {"name": "inner", "type": "Inner"},
        {"name": "inners", "type": "Inner[]"},
    ],
    "Inner": [{"name": "flag", "type": "bool"}, {"name": "note", "type": "string"}],
}
MIXED_MSG = {
    "who": "0x" + "ab" * 20,
    "label": "hello",
    "blob": "0x0102",
    "tag": "0x" + "cd" * 32,
    "delta": -12345678901234567890,
    "small": -5,
    "ids": [1, 2, 3],
    "inner": {"flag": True, "note": "x"},
    "inners": [{"flag": False, "note": ""}, {"flag": True, "note": "y"}],
}


def eth_sign(domain: Domain, struct: StructType, msg: dict):
    full = struct.to_typed_data(domain, msg)
    signable = encode_typed_data(full_message=full)
    return Account.sign_message(signable, KEY), signable


@pytest.mark.parametrize(
    "struct,msg", [(ORDER, ORDER_MSG), (Reference("Mixed", ALL_TYPES).as_struct_type(), MIXED_MSG)]
)
def test_matches_eth_account(struct, msg):
    domain = grvt_domain(326)
    expected, signable = eth_sign(domain, struct, msg)
    assert struct.digest(domain, msg) == expected.message_hash
    sig = asyncio.run(PrivateKeySigner(KEY).sign_typed(domain, struct, msg))
    assert sig.signer == Account.from_key(KEY).address.lower()
    assert int(sig.r, 16) == expected.r and int(sig.s, 16) == expected.s and sig.v == expected.v
    assert Account.recover_message(signable, vrs=(sig.v, int(sig.r, 16), int(sig.s, 16))).lower() == sig.signer


def test_domain_depends_on_chain_id():
    assert grvt_domain(325).separator() != grvt_domain(326).separator()
    assert ORDER.digest(grvt_domain(325), ORDER_MSG) != ORDER.digest(grvt_domain(326), ORDER_MSG)


def test_type_hash_checked_at_import():
    good = ORDER.type_hash
    assert StructType("Order", ORDER_TYPES, good, hasher=ORDER.hash_struct).type_hash == good
    with pytest.raises(ValueError, match="type hash mismatch"):
        StructType("Order", ORDER_TYPES, "0x" + "00" * 32, hasher=ORDER.hash_struct)


def test_generated_order_types_match():
    assert ORDER.types == ORDER_TYPES


class EthAccountSigner:
    """A wallet-style signer: signs the typed data itself (eth-account), never sees our digest."""

    def __init__(self, key: str, address: str | None = None):
        self._key = key
        self.address = address or Account.from_key(key).address.lower()

    async def sign_typed(self, domain, primary_type, message):
        s = Account.sign_message(encode_typed_data(full_message=primary_type.to_typed_data(domain, message)), self._key)
        return Signature(self.address, f"0x{s.r:064x}", f"0x{s.s:064x}", s.v)


async def test_external_signer_is_checked_by_recovery():
    rt = Runtime("testnet", signer=EthAccountSigner(KEY))
    sig = await rt.sign(ORDER, ORDER_MSG)
    local = PrivateKeySigner(KEY).sign_digest(ORDER.digest(rt.domain, ORDER_MSG))
    assert sig == local
    await rt.close()


async def test_external_signer_wrong_signature_refused():
    liar = EthAccountSigner("0x" + "22" * 32, address=PrivateKeySigner(KEY).address)
    rt = Runtime("testnet", signer=liar)
    with pytest.raises(GrvtError, match="invalid signature: it recovers to"):
        await rt.sign(ORDER, ORDER_MSG)
    await rt.close()


def test_check_signature_rule():
    from grvt._runtime.signing.signer import check_signature

    key = PrivateKeySigner(KEY)
    digest = ORDER.digest(grvt_domain(326), ORDER_MSG)
    good = key.sign_digest(digest)
    assert check_signature(digest, good, key.address) is None
    assert check_signature(digest, Signature("", good.r, good.s, good.v), key.address) is None  # signer optional
    n = 0xFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFEBAAEDCE6AF48A03BBFD25E8CD0364141
    high = Signature(good.signer, good.r, f"0x{n - int(good.s, 16):064x}", 55 - good.v)  # same point, high s
    assert check_signature(digest, high, key.address) == "s is not low-s"
    wrong = Signature("0x" + "11" * 20, good.r, good.s, good.v)
    assert "signer field" in (check_signature(digest, wrong, key.address) or "")


def test_range_checks():
    bad = dict(ORDER_MSG, nonce=2**32)
    with pytest.raises(ValueError, match="uint32"):
        ORDER.digest(grvt_domain(326), bad)


def test_transforms():
    assert scaled("65000.1", 9) == 65000100000000
    assert scaled(Decimal("0.001"), 9) == 1_000_000
    with pytest.raises(ValueError, match="decimal places"):
        scaled("0.0000000001", 9)  # over-precision is rejected, never rounded
    assert to_int("0x030501") == 0x030501 and to_int("123") == 123


def test_signing_speed_report(capsys):
    signer = PrivateKeySigner(KEY)
    domain = grvt_domain(326)
    n = 2000
    t0 = time.perf_counter()
    for _ in range(n):
        signer.sign_digest(ORDER.digest(domain, ORDER_MSG))
    ours = (time.perf_counter() - t0) / n * 1e6
    t0 = time.perf_counter()
    for _ in range(200):
        eth_sign(domain, ORDER, ORDER_MSG)
    theirs = (time.perf_counter() - t0) / 200 * 1e6
    with capsys.disabled():
        print(f"\n  sign one Order (hash + secp256k1): {ours:.1f}us; eth-account: {theirs:.1f}us")
    assert ours < theirs
