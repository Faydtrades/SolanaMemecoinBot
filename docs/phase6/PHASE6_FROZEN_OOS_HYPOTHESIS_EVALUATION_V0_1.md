# Phase-6 Frozen OOS Hypothesis Evaluation v0.1

## Status and scope

`MEME-P6-T001` implements the deterministic evaluation harness only. The
72-hour OOS run has **not** been started. This component is research-only: it
has no signer, wallet, send, broadcast, custody, network, RPC, collector, or
live-order capability. It does not alter Phase-4 or Phase-5 semantics.

Model: `P6-FROZEN-OOS-HYPOTHESIS-EVALUATION-0001`

Protocol: `P6-OOS-PROTOCOL-0001`

Model/protocol fingerprint:
`0cd06a3269c997aaa01a0e1a796e9a15088e9af7b3a4137930a5cc7beb9cdb87`

Policy-set SHA256:
`c4a2a277af1d1c7e1e8316e258b2a6da2abd302cd31d2498e4e7a45d08b2582c`

The policy registry is immutable at runtime, its expected SHA256 is included
inside the protocol fingerprint, and import fails closed if any policy payload
drifts.

## Frozen candidate and control

`H1_SIMPLE_TIME_TP10_T30` evaluates membership exclusively from
`CandidateSignal.signal_observed_at` after strict UTC validation. The interval
is `[00:00:00, 06:00:00)`. Entry/fill/ready timestamps and adverse slippage
cannot affect membership. Its exit is TP +10%, otherwise timeout 30 seconds
from the physical entry fill clock, delegated to the accepted Phase-4
counterfactual replay and execution helpers.

`CTRL_ALL_TP10_T30` evaluates the same frozen CandidateSignal cohort without
the time filter and uses the identical exit contract. The policy ID and cohort
gate are the only intentional definition differences.

## Diagnostic firewall

`H2_TIME_OR_Q4_TP20_T30`, `CTRL_Q4_TP20_T30`, and
`H3_EXCLUSION_TRAIL10_GB3_T30` use the persisted execution-outcome attribute
`entry_adverse_slippage_bps`. They are structurally classified
`DIAGNOSTIC_ONLY_NON_CAUSAL_ENTRY_FILTER`, have `promotion_eligible=false`, and
can never receive a paper or live promotion disposition.

The Q boundaries are immutable in-sample protocol inputs, not OOS estimates:
p25=0, p50=61, p75=585 bps. Q4 is strictly above p75. The named H3 exclusion
removes Q3 (`p50 < value <= p75`). Missing adverse-slippage evidence never
qualifies for a diagnostic outcome filter.

Frozen FINAL-A and FINAL-B retain their accepted Phase-4 signal-clock exit
definitions and are reference-only. `CTRL_ALL_TP20_T30` is also reference-only.

## Window and source binding

Every run requires explicit start/end UTC timestamps, source start/end rowid,
expected source-database SHA256, and expected paper-database SHA256. The window
must span at least 259,200 seconds (72 contiguous hours). The accepted source
model ID and fingerprint and this protocol fingerprint are part of the window
fingerprint.

Candidate rows are also bound to FirstPullback strategy v1.1, accepted locked
selection SHA256
`408657c1d6dc39435b61e01b368dac69796481864b7462efd150a2d4e1b0daa1`,
and one of its four accepted parameter-set IDs. The accepted continuous
FirstPullback binding fingerprint is
`c2f90d610ab774831af6a52b4a24e948b4bf0727d69457fee63311b5a159feca`.

The harness never chooses or ranks windows. Source and paper databases are
opened with SQLite URI `mode=ro` plus `PRAGMA query_only=ON`; non-empty WAL or
journal sidecars fail closed. File hashes are verified before reading, after
reading, and before atomic publication of artifacts. Source batching is
bounded before normalization so no row above the frozen end cursor is
inspected.

Each output directory must be new and have a `phase6` path component. Artifacts
are assembled in an owned staging directory and published only after final
input-hash verification. This prevents a failed run from leaving a complete-
looking result directory.

## Metrics and gates

For each policy the harness reports the required cohort, entry, exit-fill,
rejection, cost, realized PnL, expectancy, outcome, holding-time, exit-reason,
closed-equity drawdown, and complete-UTC-day metrics. Unresolved exits remain
unresolved and are never converted to zero-PnL fills. Their incurred entry
costs remain visible; realized net PnL and expectancy use closed trades only.

Complete-day PnL is attributed by the frozen causal event clock,
`CandidateSignal.signal_observed_at UTC`. Partial boundary days are excluded.

All gates live in one versioned protocol definition. A hard execution or
profitability failure produces `NO_GO`. Positive economics with insufficient
duration, sample, or daily robustness produces
`CONDITIONAL_POSITIVE_BUT_INSUFFICIENT_ROBUSTNESS`. Only H1 can produce
`GO_TO_PAPER_GATE`; no result has a live-promotion path. A positive net result
with zero realized closed-equity drawdown has an infinite ratio and passes the
ratio gate; non-positive net with zero drawdown fails the net gate.
Gate comparisons use exact integer fractions; 12-decimal strings are report
representations only and cannot round a failing boundary into a pass.

The harness reports whether H1 reaches 150 eligible entries. It does not extend
a window. Any extension decision is external and may use only duration, sample
count, and technical completeness—not PnL.

## Deterministic artifacts

A real frozen run emits:

- `protocol_manifest.json`
- `source_window_manifest.json`
- `policy_definitions.json`
- `per_trade_evaluation_rows.json`
- `policy_metrics.json`
- `daily_metrics.json`
- `gate_evaluation.json`
- `final_disposition.json`
- `artifact_manifest.json`

The artifact manifest binds every payload SHA256 and a canonical run digest.
Candidate and trade ordering is canonical, so caller input ordering cannot
change the result.

## Invocation

The production evaluator requires every binding value explicitly:

```text
.codex_venv\Scripts\python.exe scripts\phase6_frozen_oos_hypothesis_evaluation_v0_1.py \
  --paper-db <frozen-paper.sqlite3> \
  --source-db <frozen-source.sqlite3> \
  --output-dir data\research\phase6\<immutable-run-id> \
  --window-start <UTC-ISO8601> \
  --window-end <UTC-ISO8601> \
  --source-start-after-rowid <integer> \
  --source-end-rowid <integer> \
  --expected-source-db-sha256 <sha256> \
  --expected-paper-db-sha256 <sha256>
```

Supplying the values does not authorize a run. A separate reviewed task must
freeze the actual OOS input window and authorize execution.
