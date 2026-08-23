from __future__ import annotations

from dataclasses import dataclass
import hashlib
import json


MODEL_ID = "P4-RUNTIME-EXIT-PRICE-IMPACT-0001"
SCHEMA_VERSION = "P4REPI-0.1"
MECHANICS = "SOL_NATIVE_CONSTANT_PRODUCT_TOKEN_INPUT"
INVENTORY_POLICY = "FLOOR_SOL_NOTIONAL_DIV_ENTRY_EXECUTION_PRICE"
ROUTER_ROUNDING = "CEIL_TO_INTEGER_BPS"
QUOTE_POLICY = "UNAVAILABLE_WITHOUT_CAUSAL_SOL_TO_QUOTE_INVENTORY"
EVIDENCE_ID = "PHASE4-3B1D0-2026-08-23"
ENTRY_IMPACT_MODEL_ID = "P4-RUNTIME-PRICE-IMPACT-0001"

_CONFIG = {
    "model_id": MODEL_ID,
    "schema_version": SCHEMA_VERSION,
    "mechanics": MECHANICS,
    "inventory_policy": INVENTORY_POLICY,
    "router_rounding": ROUTER_ROUNDING,
    "quote_policy": QUOTE_POLICY,
    "evidence_id": EVIDENCE_ID,
    "entry_impact_model_id": ENTRY_IMPACT_MODEL_ID,
}

MODEL_FINGERPRINT = hashlib.sha256(
    json.dumps(_CONFIG, sort_keys=True, separators=(",", ":")).encode("utf-8")
).hexdigest()


@dataclass(frozen=True, slots=True)
class RuntimeExitPriceImpact:
    schema_version: str
    model_id: str
    model_fingerprint: str
    available: bool
    price_identity: str
    filled_size_lamports: int
    entry_price_numerator_raw: int
    entry_price_denominator_raw: int
    derived_token_input_raw: int | None
    inventory_remainder_numerator: int | None
    inventory_remainder_denominator: int | None
    current_virtual_token_reserve_raw: int | None
    exact_impact_bps_numerator: int | None
    exact_impact_bps_denominator: int | None
    router_price_impact_bps: int | None
    mechanics: str
    inventory_policy: str
    rounding: str
    reason_code: str


class RuntimeExitPriceImpactV01:
    """Versioned Phase-4 SOL_NATIVE exit curve-impact provider.

    Inventory reconstruction
    ------------------------
    Phase-4 PaperPosition v0.1 persists the SOL notional and the exact rational
    simulated entry execution price, but not a separate token-raw quantity.
    For SOL_NATIVE, where price is lamports per token-raw unit:

        exact_token_inventory = filled_size_lamports / entry_execution_price
                              = filled_size_lamports * price_den / price_num

    v0.1 floors this rational quantity to an integer token-raw amount.  That
    fails conservatively: the paper executor can never sell more raw tokens
    than the persisted entry notional can support.  The sub-token-raw remainder
    is retained explicitly for audit.

    Exit constant-product mechanics
    -------------------------------
    Immediately before our hypothetical sell, let:

        y = current virtual token reserve
        q = paper token input being sold

    With x*y=k, selling q tokens changes token reserve to y+q and average
    execution price relative to pre-trade spot is:

        average / spot = y / (y + q)

    Therefore adverse curve impact is:

        impact = 1 - y/(y+q) = q/(y+q)

    Exact basis-point impact:

        q * 10_000 / (y + q)

    PaperCostModelV02 accepts integer bps, so v0.1 rounds UP.  This never
    understates fractional adverse exit impact.

    Fees remain separate in PaperCostModelV02.  This provider does not invent
    venue fees, priority fees, tips, interface fees, or quote conversions.
    """

    schema_version = SCHEMA_VERSION
    model_id = MODEL_ID
    model_fingerprint = MODEL_FINGERPRINT

    @staticmethod
    def _ceil_div(numerator: int, denominator: int) -> int:
        if numerator < 0:
            raise ValueError("numerator must be >= 0")
        if denominator <= 0:
            raise ValueError("denominator must be > 0")
        return (numerator + denominator - 1) // denominator

    @classmethod
    def for_position(
        cls,
        *,
        price_identity: str,
        filled_size_lamports: int,
        entry_price_numerator_raw: int,
        entry_price_denominator_raw: int,
        current_virtual_token_reserve_raw: int | None,
    ) -> RuntimeExitPriceImpact:
        if filled_size_lamports <= 0:
            raise ValueError("filled_size_lamports must be > 0")
        if entry_price_numerator_raw <= 0 or entry_price_denominator_raw <= 0:
            raise ValueError("entry price numerator/denominator must be > 0")

        identity = str(price_identity)

        if identity != "SOL_NATIVE":
            return RuntimeExitPriceImpact(
                schema_version=SCHEMA_VERSION,
                model_id=MODEL_ID,
                model_fingerprint=MODEL_FINGERPRINT,
                available=False,
                price_identity=identity,
                filled_size_lamports=int(filled_size_lamports),
                entry_price_numerator_raw=int(entry_price_numerator_raw),
                entry_price_denominator_raw=int(entry_price_denominator_raw),
                derived_token_input_raw=None,
                inventory_remainder_numerator=None,
                inventory_remainder_denominator=None,
                current_virtual_token_reserve_raw=(
                    None if current_virtual_token_reserve_raw is None
                    else int(current_virtual_token_reserve_raw)
                ),
                exact_impact_bps_numerator=None,
                exact_impact_bps_denominator=None,
                router_price_impact_bps=None,
                mechanics="UNSUPPORTED_EXIT_PRICE_IDENTITY",
                inventory_policy=INVENTORY_POLICY,
                rounding=ROUTER_ROUNDING,
                reason_code=(
                    "QUOTE_EXIT_INVENTORY_UNAVAILABLE"
                    if identity.startswith("QUOTE:")
                    else "UNSUPPORTED_PRICE_IDENTITY"
                ),
            )

        inventory_numerator = int(filled_size_lamports) * int(entry_price_denominator_raw)
        inventory_denominator = int(entry_price_numerator_raw)
        token_input_raw, remainder = divmod(inventory_numerator, inventory_denominator)

        if token_input_raw <= 0:
            return RuntimeExitPriceImpact(
                schema_version=SCHEMA_VERSION,
                model_id=MODEL_ID,
                model_fingerprint=MODEL_FINGERPRINT,
                available=False,
                price_identity=identity,
                filled_size_lamports=int(filled_size_lamports),
                entry_price_numerator_raw=int(entry_price_numerator_raw),
                entry_price_denominator_raw=int(entry_price_denominator_raw),
                derived_token_input_raw=int(token_input_raw),
                inventory_remainder_numerator=int(remainder),
                inventory_remainder_denominator=inventory_denominator,
                current_virtual_token_reserve_raw=(
                    None if current_virtual_token_reserve_raw is None
                    else int(current_virtual_token_reserve_raw)
                ),
                exact_impact_bps_numerator=None,
                exact_impact_bps_denominator=None,
                router_price_impact_bps=None,
                mechanics=MECHANICS,
                inventory_policy=INVENTORY_POLICY,
                rounding=ROUTER_ROUNDING,
                reason_code="DERIVED_TOKEN_INPUT_ZERO",
            )

        if current_virtual_token_reserve_raw is None:
            return RuntimeExitPriceImpact(
                schema_version=SCHEMA_VERSION,
                model_id=MODEL_ID,
                model_fingerprint=MODEL_FINGERPRINT,
                available=False,
                price_identity=identity,
                filled_size_lamports=int(filled_size_lamports),
                entry_price_numerator_raw=int(entry_price_numerator_raw),
                entry_price_denominator_raw=int(entry_price_denominator_raw),
                derived_token_input_raw=int(token_input_raw),
                inventory_remainder_numerator=int(remainder),
                inventory_remainder_denominator=inventory_denominator,
                current_virtual_token_reserve_raw=None,
                exact_impact_bps_numerator=None,
                exact_impact_bps_denominator=None,
                router_price_impact_bps=None,
                mechanics=MECHANICS,
                inventory_policy=INVENTORY_POLICY,
                rounding=ROUTER_ROUNDING,
                reason_code="TOKEN_RESERVE_UNAVAILABLE",
            )

        reserve = int(current_virtual_token_reserve_raw)
        if reserve <= 0:
            raise ValueError("current_virtual_token_reserve_raw must be > 0")

        numerator = int(token_input_raw) * 10_000
        denominator = reserve + int(token_input_raw)
        impact_bps = cls._ceil_div(numerator, denominator)

        if not (0 <= impact_bps < 10_000):
            raise RuntimeError("derived exit impact must be in [0, 10000)")

        return RuntimeExitPriceImpact(
            schema_version=SCHEMA_VERSION,
            model_id=MODEL_ID,
            model_fingerprint=MODEL_FINGERPRINT,
            available=True,
            price_identity=identity,
            filled_size_lamports=int(filled_size_lamports),
            entry_price_numerator_raw=int(entry_price_numerator_raw),
            entry_price_denominator_raw=int(entry_price_denominator_raw),
            derived_token_input_raw=int(token_input_raw),
            inventory_remainder_numerator=int(remainder),
            inventory_remainder_denominator=inventory_denominator,
            current_virtual_token_reserve_raw=reserve,
            exact_impact_bps_numerator=numerator,
            exact_impact_bps_denominator=denominator,
            router_price_impact_bps=impact_bps,
            mechanics=MECHANICS,
            inventory_policy=INVENTORY_POLICY,
            rounding=ROUTER_ROUNDING,
            reason_code="SOL_CONSTANT_PRODUCT_EXIT_IMPACT",
        )
