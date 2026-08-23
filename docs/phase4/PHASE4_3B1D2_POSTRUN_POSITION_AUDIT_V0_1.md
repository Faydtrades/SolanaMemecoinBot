# Phase 4.3B1D2 Postrun Position/Track Audit v0.1

The first B1D2 live paper fill passed every execution check except:

`position_tracks_match_lock`

This audit does not re-run the market or strategy.

It opens the most recent B1D2 paper SQLite database query-only/read-only and
verifies:

- exactly three filled paper orders;
- exactly three open paper positions;
- exact membership of FINAL-A / FINAL-B / SENS-C;
- one-to-one `paper_order_id` linkage;
- linked order/position track equality;
- linked mint equality;
- linked filled-size equality;
- linked entry-price equality;
- SQLite quick_check;
- whether the original audit had exactly one failed check.

If all lifecycle linkage is correct and only persisted/list ordering differs,
the original B1D2 `position_tracks_match_lock` failure is classified as an
ordering-only validator false negative.

Run:

```powershell
python scripts\phase4_3b1d2_postrun_position_audit_v0_1.py
```

No collector, network, production DB, wallet, or paper DB writes are used.
