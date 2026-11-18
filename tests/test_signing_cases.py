"""Replay every server-produced signing case in sdk/scenarios/signing.json through the GENERATED sign functions."""

import pytest
from shared_data import load_shared

from grvt._generated import models as m
from grvt._generated import signing
from grvt._runtime import FixedClock, OrderOutOfBounds, PrivateKeySigner, Runtime, codec

SIGNING = load_shared("scenarios/signing.json")
KEY = SIGNING["test_key"]

CONTRACT = load_shared("contract/contract.json")

# signed call id -> (request type, generated sign function), for every call the contract signs
BINDINGS = {
    r["id"]: (getattr(m, r["request"] + "Input"), getattr(signing, "sign_" + r["id"].replace(".", "_")))
    for r in CONTRACT["rpcs"] + CONTRACT["edge"]["routes"]
    if r.get("signs")
}


class RecordingSigner(PrivateKeySigner):
    """Captures what the generated code asked to sign, then signs for real."""

    calls: list

    def __init__(self, key):
        super().__init__(key)
        self.calls = []

    async def sign_typed(self, domain, primary_type, message):
        self.calls.append((domain, primary_type, message))
        return await super().sign_typed(domain, primary_type, message)


def runtime_for(vec, signer):
    rt = Runtime("testnet", signer=signer, clock=FixedClock(0))
    meta = vec["inputs"]["metadata"]
    for inst in meta.get("instruments", []):
        rt.metadata.put("instrument", inst["instrument"], inst)
    for cur in meta.get("currencies", []):
        rt.metadata.put("currency", cur["symbol"], cur)
    if "main_account_id" in meta:
        rt.metadata.put("main_account", "", meta["main_account_id"])
    return rt


@pytest.mark.parametrize("vec", SIGNING["cases"], ids=lambda v: v["name"])
async def test_signing_case(vec):
    req_type, sign = BINDINGS[vec["rpc"]]
    signer = RecordingSigner(KEY["private_key"])
    assert signer.address == KEY["address"]
    rt = runtime_for(vec, signer)
    assert rt.env.chain_id == vec["inputs"]["env_chain_id"]
    req = codec.convert(vec["inputs"]["wire_request"], req_type)

    if vec["expect"] == "reject":
        with pytest.raises(ValueError, match="decimal places"):
            await sign(rt, req)
        assert signer.calls == []  # refused before signing
        await rt.close()
        return

    signed = await sign(rt, req)
    wire = codec.to_builtins(signed)
    if "elements" in vec:  # per-element binding: one signature per list element, in order
        assert len(signer.calls) == len(vec["elements"])
        for (domain, struct, message), el in zip(signer.calls, vec["elements"], strict=True):
            steps = el["steps"]
            assert struct.name == el["signed_type"]
            assert "0x" + domain.separator().hex() == steps["domain_separator"]
            assert "0x" + struct.hash_struct(message).hex() == steps["struct_hash"]
            assert "0x" + struct.digest(domain, message).hex() == steps["digest"]
            list_name, index = el["path"].rstrip("]").split("[")
            sig = wire[list_name][int(index)]["signature"]
            assert (sig["r"], sig["s"], sig["v"]) == (steps["r"], steps["s"], steps["v"])
            assert sig == steps["signature"]
        assert wire == vec["signed_request"]
        await rt.close()
        return

    steps = vec["steps"]
    domain, struct, message = signer.calls[0]
    assert struct.name == vec["signed_type"]
    assert "0x" + domain.separator().hex() == steps["domain_separator"]
    assert "0x" + struct.hash_struct(message).hex() == steps["struct_hash"]
    assert "0x" + struct.digest(domain, message).hex() == steps["digest"]
    sig_path = ["order", "signature"] if vec["rpc"] == "order.create_order" else ["signature"]
    sig = wire
    for k in sig_path:
        sig = sig[k]
    assert (sig["r"], sig["s"], sig["v"]) == (steps["r"], steps["s"], steps["v"])
    assert sig == steps["signature"]
    assert wire == steps["signed_request"]
    await rt.close()


def test_every_signed_call_has_a_signing_case():
    assert {v["rpc"] for v in SIGNING["cases"]} == set(BINDINGS)


@pytest.mark.parametrize("vec", SIGNING["order_checks"], ids=lambda v: v["name"])
async def test_order_check(vec):
    """Every SDK must reach the same verdict, nearest values and message on the shared order-check cases."""
    signer = RecordingSigner(KEY["private_key"])
    rt = Runtime("testnet", signer=signer, clock=FixedClock(0))
    for inst in vec["instruments"]:
        rt.metadata.put("instrument", inst["instrument"], inst)
    try:
        if vec["expect"] not in ("pass", "reject"):
            with pytest.raises(ValueError):
                req = m.ApiCreateOrderRequestInput(order=codec.convert(vec["order"], m.OrderInput))
                await signing.sign_order_create_order(rt, req)
            assert signer.calls == []
            return
        req = m.ApiCreateOrderRequestInput(order=codec.convert(vec["order"], m.OrderInput))
        if vec["expect"] == "pass":
            await signing.sign_order_create_order(rt, req)
            assert len(signer.calls) == 1
        elif vec["expect"] == "reject":
            with pytest.raises(OrderOutOfBounds) as info:
                await signing.sign_order_create_order(rt, req)
            e, want = info.value, vec["rejection"]
            got = {k: getattr(e, k) for k in want if k != "message"}
            assert got == {k: v for k, v in want.items() if k != "message"}
            assert str(e) == want["message"]
            assert signer.calls == []
        else:
            with pytest.raises(ValueError) as info:
                await signing.sign_order_create_order(rt, req)
            assert not isinstance(info.value, OrderOutOfBounds)
            assert signer.calls == []
    finally:
        await rt.close()


@pytest.mark.parametrize(
    "vec", [v for v in SIGNING["order_checks"] if v["expect"] == "reject"], ids=lambda v: v["name"]
)
async def test_order_checks_off_signs_a_rejected_order(vec):
    """check_orders=False leaves the limits to the exchange: an order the check would refuse is signed."""
    signer = RecordingSigner(KEY["private_key"])
    rt = Runtime("testnet", signer=signer, clock=FixedClock(0), check_orders=False)
    for inst in vec["instruments"]:
        rt.metadata.put("instrument", inst["instrument"], inst)
    try:
        if vec["expect"] not in ("pass", "reject"):
            with pytest.raises(ValueError):
                req = m.ApiCreateOrderRequestInput(order=codec.convert(vec["order"], m.OrderInput))
                await signing.sign_order_create_order(rt, req)
            assert signer.calls == []
            return
        req = m.ApiCreateOrderRequestInput(order=codec.convert(vec["order"], m.OrderInput))
        await signing.sign_order_create_order(rt, req)
        assert len(signer.calls) == 1
    finally:
        await rt.close()
