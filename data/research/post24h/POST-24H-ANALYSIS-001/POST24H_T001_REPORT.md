# POST24H-T001 Frozen Replay Foundation

Result: exact frozen-policy behavior and lamport accounting reproduced.

- Model: `P4-POST24H-FROZEN-REPLAY-FOUNDATION-0001`
- Model fingerprint: `9ca0932c03c0a1a662f552c93273bcd69a46b035bdd068f8e6214b0a5d2d5607`
- Accepted run: `15f8b07e-1b9b-464f-a822-01e3fbb8333e`
- Physical entries: `635`
- Expected accepted cohort digest: `167dd087ab516996b8cc174a5a55cc1b7b3d9f7b86970ac01f598cd82c121f52`
- Computed accepted cohort digest: `167dd087ab516996b8cc174a5a55cc1b7b3d9f7b86970ac01f598cd82c121f52`
- Accepted cohort digest exact match: `true`
- Transparent projection digest: `95655ca886f209015d41e9cc724fb3d6cacfc379c4d51ce598e1da796b47e583`
- Source range: `rowid > 766710 AND rowid <= 1163003`
- Intent matches: `1693`
- Deterministic rerun: `EXACT`

The accepted cohort digest is recomputed from the exact project-review context/route join, field projection, row ordering, JSON serialization, newline joining, UTF-8 encoding, and SHA256 recipe.

| Track | Positions | Filled | Pending | Exit reasons | Gross execution PnL | Entry costs | Exit costs | Net PnL |
|---|---:|---:|---:|---|---:|---:|---:|---:|
| FINAL-A | 635 | 607 | 28 | TAKE_PROFIT=170, FALLBACK=437 | 867281418 | 1429485000 | 1440326321 | -2002529903 |
| FINAL-B | 635 | 606 | 29 | TRAIL=83, FALLBACK=523 | 915692495 | 1427130000 | 1438576448 | -1950013953 |
| SENS-C | 423 | 395 | 28 | TAKE_PROFIT=24, FALLBACK=371 | 1127954477 | 930225000 | 944324621 | -746595144 |

No entries were regenerated, no unresolved exit was filled by assumption, and no source row beyond the frozen watermark was used.
