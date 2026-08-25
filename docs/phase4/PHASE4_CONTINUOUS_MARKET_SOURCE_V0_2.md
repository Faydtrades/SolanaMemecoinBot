# Phase 4 Continuous Market Source v0.2

## Status

`IMPLEMENTED_PENDING_PROJECT_REVIEW`

Model: `P4-CONTINUOUS-MARKET-SOURCE-0002`

Fingerprint:
`9cb094f52bf4b4fe28dc4828b1d9a52a3350cde664c7da5a489fa84e9a4085a9`

## Purpose

V0.2 preserves the accepted V0.1 normalization result while removing the
growing launch-prefix reconstruction from the normal monotonic polling path.
V0.1 already used a fresh SQLite `mode=ro`, `query_only=ON` connection for
each latest-row and fetch operation. A stale long-lived SQLite snapshot was
therefore ruled out; the performance defect was repeated reconstruction from
the session anchor through the current cursor.

## Incremental launch state

The source owns a rebuildable, non-authoritative in-memory mapping from mint
to its first qualifying post-anchor fresh launch rowid. The paper binding's
durable cursor remains authoritative. No cache state is written to the source
database, paper database, or filesystem.

- First fetch or restart: rebuild once from the session anchor through the
  requested cursor.
- Normal monotonic fetch: fetch the next batch without a prefix scan and
  advance a working launch map as rows are normalized in rowid order.
- Forward cursor jump: catch up only the missing interval.
- Rewind or retry: rebuild through the requested cursor so future launch state
  cannot contaminate replay.
- Failure: publish the working cache only after normalization succeeds.

Fresh launch eligibility, gap-recovery exclusion, row ordering, skip reasons,
reserve/price identities, UTC timestamps, and `ingest_seq = p1 rowid` remain
semantically equivalent to V0.1. Only the versioned source identity changes.

## Continuity query

V0.2 provides a bounded read-only health query over
`(last_health_p1_rowid, latest_p1_rowid]`, returning rowid and
`inserted_at_utc`. It never rescans the session prefix. Each call uses a new
read-only/query-only connection.

## Deterministic instrumentation

The source reports:

- full launch rebuild queries;
- incremental launch catch-up queries;
- normal monotonic fetches without rebuild;
- rewind rebuild queries;
- source-health probe count; and
- source-health rows scanned.

The isolated long-session test uses 5,000 rows and 1,000 five-row monotonic
fetches. It requires one or fewer full rebuilds and no growing-prefix rebuild
after initialization; acceptance is query-count based rather than dependent
on machine timing.

## Safety boundary

This component is paper-only. It changes no strategy, candidate, exit,
deadline, latency, cost, price-impact, accounting, observability, trade-size,
or collector semantics. No production database or live service was accessed
during implementation testing.

NO PRODUCTION/LIVE VALIDATION PERFORMED.
