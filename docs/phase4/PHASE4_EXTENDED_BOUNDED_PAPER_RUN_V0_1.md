# Phase 4 Extended Bounded Paper Run v0.1

Status: IMPLEMENTATION PASS / CHECKPOINTED BEFORE REAL EXTENDED ACCEPTANCE RUN

Task: MEME-P4-T022

Purpose:
Extend the accepted/frozen Phase-4 V0.5 paper runtime beyond the historical
1h..6h run-control envelope without changing trading, source, strategy,
entry, exit, cost, sizing, accounting, or T021 observability semantics.

## Runtime contract

- Accepted V0.5 trading runtime remains protected and unchanged.
- Accepted T021 external observability wrapper remains protected and unchanged.
- New bounded duration range: 3,600..604,800 seconds (1h..168h / 7 days).
- Convenience CLI: --duration-hours.
- Reference extended runs:
  - 12h = 43,200 seconds
  - 24h = 86,400 seconds
  - 48h = 172,800 seconds
- Default duration remains inherited from accepted V0.5.
- Unbounded/continuous-until-manual-stop mode is NOT introduced by T022.

## Identities

V0.6 runtime model:
P4-CONTINUOUS-FIRSTPULLBACK-MULTIHOUR-RUN-0006

V0.6 runtime fingerprint:
434045e916e4be7e463a81ea17db487cc2b777af8d5f8874cf4b9b6b84013c9c

V0.6 runtime file SHA256:
8bf11f86f333220dd8e8617021123d99566186f9ba90b9afaaa7b3fd731caf61

Extended observability fingerprint:
689a8f53f475dad1d26d1f4e1b8cd681b0a49b79281fa0bb4e89b6bb3bbdbb79

Extended observability wrapper SHA256:
5f7bcfa1de4216f1e939a59295ec26d93fda968632ee1cec1de95232ece595d2

T022 selftest SHA256:
0a4a988c162ef5ef2f46938c4e8355449ea68fe817b6979105e15ea12715bebb

## Protected accepted baselines

V0.5 harness SHA256:
22d4c0fa40e53e62fb305c75a832f6a633b496a8f10382d75353444e1392db37

T021 observability wrapper SHA256:
6f517b63220ae19b2edde282aea580ae42a18794c73291115a92a4e0b850518a

Both remained byte-identical during T022 validation.

## Validation evidence

- T022 extended-duration selftest: 14/14 PASS
- 1h, 6h, 12h, 24h, 48h, and 7d duration parsing: PASS
- 24h CLI resolves exactly to 86,400 seconds: PASS
- fail-closed lower/upper duration bounds: PASS
- exact duration-boundary run-control semantics: PASS
- V0.6 -> V0.5 delegation/restoration: PASS
- extended wrapper -> T021 observability delegation/restoration: PASS
- deep full delegation probe: 8/8 PASS
- T021 terminal-finalization regression: 14/14 PASS
- T017 external-observability regression: 28/28 PASS
- accepted V0.5/T021 files unchanged: PASS

No long-duration runtime was simulated by sleeping/waiting.

## Acceptance state

T022 implementation and deterministic validation: PASS.

The first real extended-duration acceptance run is intentionally the 24-hour
paper run. T022 is not considered real extended-runtime ACCEPTED until that
run is reviewed.

No profitability claim is made by T022.
No trading semantics were intentionally changed.
