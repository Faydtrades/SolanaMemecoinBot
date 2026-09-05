# POST-24H-ANALYSIS-001 — Outcome Studies and Counterfactual Exit Matrix

Status: IMPLEMENTED_PENDING_PROJECT_REVIEW

## Scope and interpretation locks

This is hypothesis generation from one contiguous ~24h IN-SAMPLE regime. The 275-policy matrix creates multiple-comparison and selection risk. Shortlisted policies require Phase 6 out-of-sample validation. No parameter is approved for live trading.

FINAL-A and FINAL-B remain frozen and unmodified and share the same physical entries. ACTUAL SENS-C uses a different accepted 423-entry denominator. The research cohort contains 635 distinct physical entries/mints.

Unresolved executions are not converted to zero-PnL closed trades. Market outcome marks are causal last-known fresh marks without interpolation. Horizon availability is a research measure and is not canonical Phase-4 CoverageStatus.

## Frozen proofs

- Cohort: 635 / `167dd087ab516996b8cc174a5a55cc1b7b3d9f7b86970ac01f598cd82c121f52`
- T001 replay digest: `3ee403382569ac1f041da4798e3e15818ec5b4c1d60c0c4a42b3c24e9c96b61a`
- T002A deterministic digest: `48d20d23b1249bf8d743efd6847ddc4f66ef2c6a5251f0fadbc08542f47594e6`
- T002B deterministic analysis digest: `bf53efc7700c3902c2edec7d1cf153f273ca4d9aed3fdaf6f85bbdbba82f1874`

## Policy matrix

- Registry: 275 policies
- Replay rows: 174625
- All new research policies use ENTRY_FILL_CLOCK and keep thresholds anchored to the frozen Phase-4 rule reference.
- Equity/drawdown metrics are explicitly REALIZED_CLOSED_EQUITY, not causal mark-to-market drawdown.

## Entry-aligned mark availability

- 5s: 464/635 available (0.730708661417)
- 10s: 551/635 available (0.867716535433)
- 15s: 582/635 available (0.916535433071)
- 20s: 600/635 available (0.944881889764)
- 30s: 615/635 available (0.968503937008)
- 45s: 624/635 available (0.982677165354)
- 60s: 625/635 available (0.984251968504)
- 90s: 627/635 available (0.987401574803)
- 120s: 629/635 available (0.990551181102)
- 180s: 630/635 available (0.992125984252)
- 300s: 630/635 available (0.992125984252)

## MFE / MAE context

- 5s: MFE p50 286 bps; MAE p50 -190 bps; available 464/635
- 15s: MFE p50 485 bps; MAE p50 -666 bps; available 582/635
- 60s: MFE p50 1210 bps; MAE p50 -1574 bps; available 625/635
- 300s: MFE p50 2175 bps; MAE p50 -3249 bps; available 630/635

## ACTUAL_24H baselines

| Track | Denominator | Filled | Unresolved | Net PnL |
|---|---:|---:|---:|---:|
| FINAL-A | 635 | 607 | 28 | -2002529903 |
| FINAL-B | 635 | 606 | 29 | -1950013953 |
| SENS-C | 423 | 395 | 28 | -746595144 |

## Post-ACTUAL-exit raw-market marks

- 5s: 1155/1608 available; raw-market return p50 1 bps
- 10s: 1370/1608 available; raw-market return p50 0 bps
- 15s: 1456/1608 available; raw-market return p50 -42 bps
- 30s: 1533/1608 available; raw-market return p50 -187 bps
- 45s: 1556/1608 available; raw-market return p50 -259 bps
- 60s: 1564/1608 available; raw-market return p50 -259 bps

## Matrix context

The highest in-sample net-PnL policies are shown only as context, not as a winner ranking. Positive in-sample net PnL does not override the displayed execution-coverage rate or the locked shortlist gate.
- TP20_T120S: net 241677162; fill rate 0.869291338583; unresolved rate 0.130708661417
- TP25_T120S: net 97315520; fill rate 0.866141732283; unresolved rate 0.133858267717
- TP40_T120S: net -283739600; fill rate 0.856692913386; unresolved rate 0.143307086614
- TP15_T120S: net -434571608; fill rate 0.875590551181; unresolved rate 0.124409448819
- TP30_T120S: net -442645276; fill rate 0.858267716535; unresolved rate 0.141732283465
- TP10_T120S: net -529709206; fill rate 0.899212598425; unresolved rate 0.100787401575
- TP50_T120S: net -641446579; fill rate 0.850393700787; unresolved rate 0.149606299213
- TP20_T60S: net -822417411; fill rate 0.913385826772; unresolved rate 0.086614173228
- TIME_T60S: net -872476077; fill rate 0.902362204724; unresolved rate 0.097637795276
- TP25_T60S: net -902796780; fill rate 0.910236220472; unresolved rate 0.089763779528

Sold-too-early cells are descriptive only. The highest-rate non-small cell was FINAL-B / TRAIL / 60s / 1000bps: 48/82 (0.585365853659).

## Pareto-style shortlist

| Order | Candidate | Family | Fill rate | Net PnL | Profit factor | Drawdown |
|---:|---|---|---:|---:|---:|---:|
| 1 | TP20_T60S | TP_TIMEOUT | 0.913385826772 | -822417411 | 0.906413255673 | 972379110 |
| 2 | TP10_T30S | TP_TIMEOUT | 0.946456692913 | -1228859683 | 0.824156863837 | 1296812157 |
| 3 | TP10_T60S | TP_TIMEOUT | 0.925984251969 | -1224671492 | 0.845008839909 | 1273324451 |
| 4 | TRAIL_A15_G3_T30S | TRAILING_TIMEOUT | 0.937007874016 | -1178109990 | 0.851713046563 | 1382807661 |
| 5 | TIME_T60S | TIMEOUT_ONLY | 0.902362204724 | -872476077 | 0.920801584618 | 1362220064 |
| 6 | TRAIL_A10_G3_T30S | TRAILING_TIMEOUT | 0.940157480315 | -1207204327 | 0.846918947566 | 1448009800 |
| 7 | TP20_T30S | TP_TIMEOUT | 0.940157480315 | -1291508218 | 0.830807941865 | 1334081069 |
| 8 | TP10_SL30_T30S | TP_SL_TIMEOUT | 0.952755905512 | -1414308180 | 0.781716492602 | 1433967851 |
| 9 | TIME_T45S | TIMEOUT_ONLY | 0.929133858268 | -1078733416 | 0.897252337057 | 1653677083 |
| 10 | TP40_T30S | TP_TIMEOUT | 0.941732283465 | -1459265094 | 0.825984119360 | 1486450493 |

All listed policies are labeled IN_SAMPLE_CANDIDATE. None is a winner or a live-policy selection.

## Small-cell rule

Segment and sold-too-early cells with N < 50 are marked small_cell=true and must not support shortlist selection or strong conclusions.
