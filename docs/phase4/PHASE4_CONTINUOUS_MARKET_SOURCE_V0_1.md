# Phase 4 Continuous Market Source v0.1

## Scope

This foundation reads bounded `pump_events` batches for a later continuous
paper-runner binding. It performs no strategy evaluation, candidate arbitration,
timer delivery, paper-state mutation, collector control, RPC, or network access.

FIRSTPULLBACK LIVE BINDING:
**NOT YET IMPLEMENTED**

CONTINUOUS LIVE PAPER:
**NOT YET VALIDATED**

## Source identity

- Model ID: `P4-CONTINUOUS-MARKET-SOURCE-0001`
- Schema: `phase4_continuous_market_source_v0.1`
- Model fingerprint:
  `47cdd010c76c3530f0d3d181e6545d9e50ce40508005a37eda850efe290dceda`

The model fingerprint binds the accepted Phase-2 read-only adapter,
quote-aware adapter, quote-aware price engine, rowid polling contract, explicit
launch anchor, Phase-2-normalized fresh-launch qualification, gap behavior,
unavailable-price skips, and batch bounds.

Each adapter instance exposes a deterministic `source_identity` derived from
the model fingerprint, a stable database identity, and
`start_after_p1_rowid`. The default database identity is the resolved production
path. Tests and future deployments may inject a stable identity without
changing normalization semantics.

## Production database safety

The adapter opens SQLite with an explicit file URI containing `mode=ro`, then
enables `PRAGMA query_only=ON`. It never creates schema and contains no
production INSERT, UPDATE, DELETE, or DDL path. Schema validation uses only
`sqlite_master`, `PRAGMA table_info`, and a zero-row rowid query through the
accepted Phase-2 adapter.

The isolated self-test opens a production-column-compatible temporary fixture
through the same connection path and proves that an attempted UPDATE is
rejected. It never performs that write probe against production.

## Rowid provenance and batches

Normal polling is the indexed SQLite rowid range:

```sql
SELECT rowid AS p1_rowid, *
FROM pump_events
WHERE rowid > ?
ORDER BY rowid ASC
LIMIT ?
```

The batch size must be between 1 and 10,000 rows. Output records preserve strict
ascending `p1_rowid`, and accepted Phase-2 `ingest_seq` must equal that rowid.
The batch reports the requested cursor and highest fetched rowid, but the
adapter has no mutable consumed cursor and performs no durable cursor commit.
Fetching therefore cannot itself acknowledge a row.

## Launch-session anchor

`start_after_p1_rowid` is mandatory and immutable for an adapter instance.

- Rows at or below the anchor are historical.
- A mint becomes session-eligible only when a `LAUNCH` strictly after the
  anchor passes `Phase1QuoteAwareAdapterV01.row_to_event`, normalizes as an
  accepted `LAUNCH`, and is not `GAP_RECOVERY`.
- That launch row and later rows for the mint may be emitted.
- Rows for mints without a post-anchor launch are deterministically skipped as
  `MINT_NOT_SESSION_ELIGIBLE`.
- A pre-anchor launch never activates historical backfill.
- A raw, unsupported, malformed, or gap-recovery launch never activates a
  mint, even when a later fresh BUY is otherwise valid.

On restart, the adapter selects raw `LAUNCH` candidates only from the bounded
rowid interval `(start_after_p1_rowid, last_read_rowid]`, then applies the same
accepted Phase-2 normalization and non-gap qualification used for launches in
the current batch. The next batch is read strictly after the caller-supplied
last rowid. The same database contents, anchor, and last-read rowid reproduce
the same records, skips, fingerprints, and order.

`_eligible_launches_through()` is called for every `fetch_batch()`. It queries
all raw `LAUNCH` candidates between the fixed session anchor and the supplied
cursor and normalizes each candidate again. The query is rowid-bounded and can
use the existing event-type/rowid access paths, so it is not an unbounded full
table poll; however, repeated polling rescans a growing post-anchor launch
prefix. This correction deliberately does not redesign cursor/session state.
Project review should decide whether T004 must replace the repeated prefix scan
with explicitly supplied or durable launch-eligibility state.

This foundation does not choose whether a fresh production session should
anchor at the current maximum rowid. That policy belongs to the later binding
task.

## Accepted normalization and price semantics

The source delegates row normalization to
`Phase1QuoteAwareAdapterV01.row_to_event`, which in turn uses
`Phase1ReadOnlyAdapterV01`. It does not define a second raw-row normalizer.

The immutable output binds:

- production `p1_rowid` and post-anchor launch rowid;
- normalized event key, mint, event type, event time, and observation time;
- exact Phase-2 ingest sequence and ingestion source;
- price identity and raw numerator/denominator reserves from
  `QuoteAwarePriceEngineV01.point`;
- the current virtual token reserve;
- explicit gap-recovery provenance;
- source model, model fingerprint, source identity, and content fingerprint.

Timestamps are timezone-aware UTC. Raw reserve identities remain positive
integers; no float conversion is used. `SOL_NATIVE` uses virtual SOL reserves
as numerator and virtual token reserves as denominator. Quote-denominated
points retain their accepted `QUOTE:<mint>` identity.

If the accepted Phase-2 price engine cannot produce a positive numerator and
token-reserve denominator, the row is skipped as
`PRICE_POINT_UNAVAILABLE:<identity>` rather than receiving fabricated values.
Other accepted Phase-2 normalization failures retain their deterministic reason
codes in the batch skip details and counters.

## Gap behavior

Rows normalized by Phase-2 as `GAP_RECOVERY` remain explicitly labeled. The
source does not relabel them as fresh flow or assert that they are tradable.
The later binding must continue to honor that flag. An accepted gap-recovery
`LAUNCH` is audited as `GAP_RECOVERY_LAUNCH_NOT_SESSION_ELIGIBLE` and cannot
establish session eligibility. Gap-recovery observations after an already
qualified fresh launch retain their explicit provenance and may be emitted for
downstream fail-closed handling.

The locked Phase-2 `BOT_TRUTH_SOURCE_PREFIXES` currently accepts
`GAP_RECONCILIATION_V0_3_3`, while the current v0.3.4 collector writes newly
recovered rows as `GAP_RECONCILIATION_V0_3_4`. Because this task may not modify
the accepted Phase-2 adapter, v0.3.4-labeled recovery rows are deterministically
skipped as `UNSUPPORTED_NON_BOT_TRUTH_SOURCE`. Project review must decide a
separate accepted compatibility change before those rows can be admitted.

## Validation

`scripts/phase4_continuous_market_source_selftest_v0_1.py` uses temporary
SQLite only. It proves the A-U task contract, missing-schema rejection,
`query_only` write rejection, launch eligibility, deterministic skips, exact
integer price identities, restart replay, content-fingerprint sensitivity,
SQLite `quick_check=ok`, and repeated canonical digest equality.

The C1 regressions additionally prove that neither unsupported v0.3.4 recovery
launches nor accepted `GAP_RECOVERY` launches activate a mint, that a genuine
fresh live launch still activates its mint, and that restart reconstruction
neither loses a genuine launch nor resurrects either rejected launch class.

A bounded production read probe also validated the required schema and read a
20-row recent range using the same read-only path. It did not start a collector,
perform a full count/scan or `quick_check`, or write production data.

## Deliberately not implemented

- FirstPullback feature/state evaluation;
- role ordering or candidate arbitration;
- candidate or terminal-skip source events;
- strategy expiry timers;
- exit clock ticks;
- binding to `ContinuousPaperRunnerV01`;
- durable paper cursor state;
- continuous live paper execution;
- collector, RPC, WebSocket, wallet, signing, or live orders.
