from __future__ import annotations

from dataclasses import dataclass
import hashlib
import json


MODEL_ID = "P4-RUNTIME-PRICE-IMPACT-0001"
SCHEMA_VERSION = "P4RPI-0.1"
MECHANICS = "SOL_NATIVE_CONSTANT_PRODUCT_GROSS_INPUT"
ROUTER_ROUNDING = "CEIL_TO_INTEGER_BPS"
QUOTE_POLICY = "UNAVAILABLE_WITHOUT_CAUSAL_SOL_TO_QUOTE_INPUT"

# Evidence comes from Phase 4.3B1D0 real-data mechanics probe.
EVIDENCE_ID = "PHASE4-3B1D0-2026-08-23"

_CONFIG = {
    "model_id": MODEL_ID,
    "schema_version": SCHEMA_VERSION,
    "mechanics": MECHANICS,
    "router_rounding": ROUTER_ROUNDING,
    "quote_policy": QUOTE_POLICY,
    "evidence_id": EVIDENCE_ID,
}

MODEL_FINGERPRINT = hashlib.sha256(
    json.dumps(
        _CONFIG,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
).hexdigest()


@dataclass(frozen=True, slots=True)
class RuntimePriceImpact:
    schema_version: str
    model_id: str
    model_fingerprint: str
    available: bool
    price_identity: str
    requested_size_lamports: int
    reserve_input_raw: int | None
    exact_impact_bps_numerator: int | None
    exact_impact_bps_denominator: int | None
    router_price_impact_bps: int | None
    mechanics: str
    rounding: str
    reason_code: str


class RuntimePriceImpactV01:
    """
    Versioned Phase-4 runtime price-impact provider.

    SOL_NATIVE
    ----------
    Empirically-supported Pump constant-product entry mechanics:

        pre-trade virtual SOL reserve = x
        requested SOL curve input     = q

    Spot price before our order is proportional to x/y.
    Under x*y=k, average execution price for q is proportional to (x+q)/y.

        average / spot - 1 = q/x

    Therefore the exact curve impact in basis points is:

        q * 10_000 / x

    The Phase-4 router requires integer bps. We deliberately round UP so the
    paper model never understates fractional curve impact.

    Fees are NOT subtracted from q here. Phase 4.3B1D0 showed that the gross
    reported SOL trade amount reconstructed observed Pump reserve mechanics
    better than an amount-minus-event-fee alternative. Explicit venue/priority/
    builder/base fees remain separate in PaperCostModelV02.

    QUOTE
    -----
    The locked paper reference size is SOL-denominated. A QUOTE:<mint> market
    needs the quote-raw amount that 0.10 SOL would causally buy at that instant.
    This provider refuses to invent that conversion.
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
    def for_entry(
        cls,
        *,
        price_identity: str,
        requested_size_lamports: int,
        virtual_sol_reserve_lamports: int | None,
        virtual_quote_reserve_raw: int | None = None,
        causal_quote_input_raw: int | None = None,
    ) -> RuntimePriceImpact:
        if requested_size_lamports <= 0:
            raise ValueError("requested_size_lamports must be > 0")

        identity = str(price_identity)

        if identity == "SOL_NATIVE":
            if virtual_sol_reserve_lamports is None:
                return RuntimePriceImpact(
                    schema_version=SCHEMA_VERSION,
                    model_id=MODEL_ID,
                    model_fingerprint=MODEL_FINGERPRINT,
                    available=False,
                    price_identity=identity,
                    requested_size_lamports=requested_size_lamports,
                    reserve_input_raw=None,
                    exact_impact_bps_numerator=None,
                    exact_impact_bps_denominator=None,
                    router_price_impact_bps=None,
                    mechanics=MECHANICS,
                    rounding=ROUTER_ROUNDING,
                    reason_code="SOL_RESERVE_UNAVAILABLE",
                )

            x = int(virtual_sol_reserve_lamports)
            if x <= 0:
                raise ValueError("virtual_sol_reserve_lamports must be > 0")

            numerator = int(requested_size_lamports) * 10_000
            denominator = x
            impact_bps = cls._ceil_div(numerator, denominator)

            return RuntimePriceImpact(
                schema_version=SCHEMA_VERSION,
                model_id=MODEL_ID,
                model_fingerprint=MODEL_FINGERPRINT,
                available=True,
                price_identity=identity,
                requested_size_lamports=int(requested_size_lamports),
                reserve_input_raw=x,
                exact_impact_bps_numerator=numerator,
                exact_impact_bps_denominator=denominator,
                router_price_impact_bps=impact_bps,
                mechanics=MECHANICS,
                rounding=ROUTER_ROUNDING,
                reason_code="SOL_CONSTANT_PRODUCT_IMPACT",
            )

        if identity.startswith("QUOTE:"):
            if causal_quote_input_raw is None:
                return RuntimePriceImpact(
                    schema_version=SCHEMA_VERSION,
                    model_id=MODEL_ID,
                    model_fingerprint=MODEL_FINGERPRINT,
                    available=False,
                    price_identity=identity,
                    requested_size_lamports=int(requested_size_lamports),
                    reserve_input_raw=(
                        None
                        if virtual_quote_reserve_raw is None
                        else int(virtual_quote_reserve_raw)
                    ),
                    exact_impact_bps_numerator=None,
                    exact_impact_bps_denominator=None,
                    router_price_impact_bps=None,
                    mechanics="QUOTE_CONSTANT_PRODUCT_REQUIRES_CAUSAL_INPUT",
                    rounding=ROUTER_ROUNDING,
                    reason_code="QUOTE_INPUT_CONVERSION_UNAVAILABLE",
                )

            if virtual_quote_reserve_raw is None:
                return RuntimePriceImpact(
                    schema_version=SCHEMA_VERSION,
                    model_id=MODEL_ID,
                    model_fingerprint=MODEL_FINGERPRINT,
                    available=False,
                    price_identity=identity,
                    requested_size_lamports=int(requested_size_lamports),
                    reserve_input_raw=None,
                    exact_impact_bps_numerator=None,
                    exact_impact_bps_denominator=None,
                    router_price_impact_bps=None,
                    mechanics="QUOTE_CONSTANT_PRODUCT_REQUIRES_CAUSAL_INPUT",
                    rounding=ROUTER_ROUNDING,
                    reason_code="QUOTE_RESERVE_UNAVAILABLE",
                )

            q = int(causal_quote_input_raw)
            x = int(virtual_quote_reserve_raw)
            if q <= 0:
                raise ValueError("causal_quote_input_raw must be > 0")
            if x <= 0:
                raise ValueError("virtual_quote_reserve_raw must be > 0")

            numerator = q * 10_000
            denominator = x

            return RuntimePriceImpact(
                schema_version=SCHEMA_VERSION,
                model_id=MODEL_ID,
                model_fingerprint=MODEL_FINGERPRINT,
                available=True,
                price_identity=identity,
                requested_size_lamports=int(requested_size_lamports),
                reserve_input_raw=x,
                exact_impact_bps_numerator=numerator,
                exact_impact_bps_denominator=denominator,
                router_price_impact_bps=cls._ceil_div(numerator, denominator),
                mechanics="QUOTE_CONSTANT_PRODUCT_CAUSAL_INPUT",
                rounding=ROUTER_ROUNDING,
                reason_code="QUOTE_CONSTANT_PRODUCT_IMPACT",
            )

        return RuntimePriceImpact(
            schema_version=SCHEMA_VERSION,
            model_id=MODEL_ID,
            model_fingerprint=MODEL_FINGERPRINT,
            available=False,
            price_identity=identity,
            requested_size_lamports=int(requested_size_lamports),
            reserve_input_raw=None,
            exact_impact_bps_numerator=None,
            exact_impact_bps_denominator=None,
            router_price_impact_bps=None,
            mechanics="UNSUPPORTED_PRICE_IDENTITY",
            rounding=ROUTER_ROUNDING,
            reason_code="UNSUPPORTED_PRICE_IDENTITY",
        )
