# Phase 4.2B — Cost Baseline v0.1

Baseline ID: `P4-COST-BASELINE-0001`

This baseline is locked ex ante for Phase-4 paper trading. It was not selected from DEV-FREEZE-0002 PnL outcomes.

## Source-backed values

- Default Pump early-lifecycle venue fee: 1.25% (125 bps). Pump's current fee documentation states 1.25% on bonding-curve trades for SOL and USDC, and 1.25% in the lowest canonical PumpSwap tier.
- Solana base fee: 5,000 lamports per signature.
- Manual Terminal reference captured from user screenshots on 2026-08-23: Priority 0.0001 SOL, Tip 0.001 SOL, Buy slippage cap 20%, Sell slippage cap 50%, MEV Off.

## Bot research assumptions (not externally measured facts)

- Priority-fee policy: use a live observed value when available; otherwise fallback to 0.0001 SOL.
- Builder tip: 0.001 SOL per transaction as a conservative fast-route paper overhead.
- Buy slippage cap: 15%.
- Sell slippage cap: 20%.
- Entry latency: 500 ms.
- Exit latency: 500 ms.
- Bot interface fee: 0% for the direct-bot baseline.
- Price impact: must be supplied from runtime market/venue state; no fixed impact haircut is invented in this baseline.

The slippage caps are rejection thresholds only. They are not subtracted from every fill as a fixed PnL cost.

If live Phase-4 measurement shows an assumption is unrealistic, create a new versioned baseline with the measurement evidence. Do not silently edit or retune this baseline based on paper PnL.
