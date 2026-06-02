# v6-orchestrator

Centralized video encoding orchestration across multiple distributed workers.

## Architecture

Coordinator (Hermes) → SSH push → Workers (Gorilla, SolidVPS, PVE14, PVE24)

## Structure

```
gorilla/worker.py          — Worker code (Gorilla, Intel VAAPI)
solidvps/worker.py         — Worker code (SolidVPS, NVIDIA NVENC)
pve14/worker.py            — Worker code (PVE14, Intel VAAPI)
pve24/worker.py            — Worker code (PVE24, Intel VAAPI)
coordinator.py             — Main orchestrator loop
orchestrator-report.py     — Status report generator
shared/bdrom_ripper.py     — MakeMKV BDROM extraction
shared/makemkv-renew.sh    — MakeMKV beta key auto-renewal
config.template.yaml       — Config template (NO passwords)
```

## Deploy

Each server has its own `config.yaml` (with passwords, not committed).

To deploy a worker update:
```bash
cd /opt/v6-orchestrator
scp worker.py root@SERVER:/opt/v6-orchestrator/worker.py
```

## Git workflow

When editing a worker on a server:
```bash
cd /opt/v6-orchestrator
cp /path/to/server/worker.py ./<server>/worker.py
git add <server>/worker.py
git commit -m "fix: <description>"
git push
```

## Credentials

Server passwords in `~/.hermes/server-creds.env` — never committed.
