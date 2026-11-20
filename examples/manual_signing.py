from decimal import Decimal

from coincurve import PrivateKey
from Crypto.Hash import keccak


def keccak256(data: bytes) -> bytes:
    return keccak.new(digest_bits=256, data=data).digest()


def word(n: int) -> bytes:
    """One EIP-712 32-byte word. Negative ints are two's complement."""
    return (n % 2**256).to_bytes(32, "big")


def scaled(value: str, decimals: int) -> int:
    """Decimal string -> integer. More fractional digits than `decimals` is an error, never rounded."""
    n = Decimal(value).scaleb(decimals)
    if n != n.to_integral_value():
        raise ValueError(f"{value} has more than {decimals} decimals")
    return int(n)


# Step 1: the request you want to send (signature without r, s, v yet).
request = {
    "order": {
        "sub_account_id": "8289849667772468",
        "is_market": False,
        "time_in_force": "GOOD_TILL_TIME",
        "post_only": True,
        "reduce_only": False,
        "legs": [{"instrument": "BTC_USDT_Perp", "size": "0.01", "limit_price": "60000", "is_buying_asset": True}],
        # expiration: unix nanoseconds. The SDKs use now + 1 day (at most 30 days; 180 for take-profit and
        # stop-loss orders); this signing test case uses 2100-01-01 so its digest never changes.
        "signature": {
            "signer": "0x1267c6181aee40f5ddbd70c56ecf4613117326b5",
            "expiration": "4102444800000000000",
            "nonce": 1001,
            "chain_id": "326",
        },
        "metadata": {"client_order_id": "1001"},
    }
}

# Step 2: instrument metadata, from get_instrument / get_all_instruments.
instruments = {"BTC_USDT_Perp": {"instrument_hash": "0x030501", "base_decimals": 9}}
TIME_IN_FORCE = {"GOOD_TILL_TIME": 1, "ALL_OR_NONE": 2, "IMMEDIATE_OR_CANCEL": 3, "FILL_OR_KILL": 4}

# Step 3: map wire fields to the EIP-712 message.
o, sig = request["order"], request["order"]["signature"]
legs = [
    {
        "assetID": int(instruments[leg["instrument"]]["instrument_hash"], 16),
        "contractSize": scaled(leg["size"], instruments[leg["instrument"]]["base_decimals"]),
        "limitPrice": scaled(leg.get("limit_price", "0"), 9),
        "isBuyingContract": leg["is_buying_asset"],
    }
    for leg in o["legs"]
]
message = {
    "subAccountID": int(o["sub_account_id"]),
    "isMarket": o.get("is_market", False),
    "timeInForce": TIME_IN_FORCE[o["time_in_force"]],
    "postOnly": o.get("post_only", False),
    "reduceOnly": o.get("reduce_only", False),
    "legs": legs,
    "nonce": sig["nonce"],
    "expiration": int(sig["expiration"]),
}

# Step 4: hash. structHash = keccak(typeHash || encoded fields); an array is the keccak of its element hashes.
LEG_TYPE = b"OrderLeg(uint256 assetID,uint64 contractSize,uint64 limitPrice,bool isBuyingContract)"
ORDER_TYPE = (
    b"Order(uint64 subAccountID,bool isMarket,uint8 timeInForce,bool postOnly,bool reduceOnly,"
    b"OrderLeg[] legs,uint32 nonce,int64 expiration)"
) + LEG_TYPE


def leg_hash(leg: dict) -> bytes:
    return keccak256(
        keccak256(LEG_TYPE)
        + word(leg["assetID"])
        + word(leg["contractSize"])
        + word(leg["limitPrice"])
        + word(leg["isBuyingContract"])
    )


struct_hash = keccak256(
    keccak256(ORDER_TYPE)
    + word(message["subAccountID"])
    + word(message["isMarket"])
    + word(message["timeInForce"])
    + word(message["postOnly"])
    + word(message["reduceOnly"])
    + keccak256(b"".join(leg_hash(leg) for leg in message["legs"]))
    + word(message["nonce"])
    + word(message["expiration"])
)

chain_id = int(sig["chain_id"]) or 326  # "0" means the environment's chain ID
domain_separator = keccak256(
    keccak256(b"EIP712Domain(string name,string version,uint256 chainId)")
    + keccak256(b"GRVT Exchange")
    + keccak256(b"0")
    + word(chain_id)
)
digest = keccak256(b"\x19\x01" + domain_separator + struct_hash)
print("domain_separator 0x" + domain_separator.hex())
print("struct_hash      0x" + struct_hash.hex())
print("digest           0x" + digest.hex())

# Step 5: sign the digest (secp256k1, low-s) and fill in r, s, v.
key = PrivateKey(bytes.fromhex("bfe25ff07bd38cb3fbc28a2a296d41aabd68a78c929d257935906489fc9c6d89"))  # public TEST key
raw = key.sign_recoverable(digest, hasher=None)  # 65 bytes: r || s || recovery id
sig["r"] = "0x" + raw[:32].hex()
sig["s"] = "0x" + raw[32:64].hex()
sig["v"] = raw[64] + 27
print(sig)

assert digest.hex() == "7f8fecb8997024f85a5a9a44730f79d2cef290688d6a022d8e98e95ccc1f98c9"
assert sig["r"] == "0x62b6d0047530e701aa06d4b7de9dca76916219b8561424bd246a519bdc31bf7d"
assert sig["s"] == "0x2447b83565fa6cac90c607e5c4a3f96cda386f92d7fb51f45dea12c2f8e09904"
assert sig["v"] == 27
