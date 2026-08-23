# Solana / Pump WebSocket Subscription Diagnostic v0.1

Purpose: isolate the live source after Phase 4.3B1B saw zero new production `pump_events`
and no raw JSONL file was created.

This test does not use the collector or production DB.

It performs three independent checks:

1. HTTP RPC (`getVersion`, `getSlot`)
2. WebSocket `slotSubscribe`
3. WebSocket `logsSubscribe` filtered to the locked Pump program ID

Locked Pump program ID:

`6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P`

Run with no collector running:

```powershell
python scripts\solana_pump_ws_subscription_diagnostic_v0_1.py
```

Expected runtime is normally under one minute. Pump logs are allowed up to 30 seconds.
