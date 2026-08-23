# Solana Memecoin Trading Bot — Phase 1 v0.1

This is the minimal Phase 1 scaffold.

## Locked minimal stack

- Python 3.12.x
- `solana-py` 0.40.2
- `asyncio` for asynchronous event processing
- Solana HTTP RPC + WebSocket
- Public Solana mainnet RPC for the first read-only connection test
- SQLite + raw append-only files will be introduced with the Data Recorder
- Git for source control
- Local Windows PC for initial development

No wallet, seed phrase, private key, or trading permission is used in this scaffold.

## First checkpoint

From PowerShell in this folder:

```powershell
py -3.12 -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
pip install -r requirements.txt
python scripts\connection_test.py
```

Expected final line:

```text
RESULT: PASS
```

If PowerShell blocks venv activation, do not weaken system-wide policy. For the current terminal session only:

```powershell
Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass
.\.venv\Scripts\Activate.ps1
```

## Safety

Never paste a seed phrase or private key into this project or ChatGPT. Phase 1 is read-only and does not need wallet access.
