# Agent Notes — 2026-04-16 Client Readiness Pass

## Production Release

- Live app: `https://hr-hunter.hyvelabs.tech`
- Active release path: `/srv/hr-hunter/releases/20260416T101500Z-client-ready-final`
- Previous rollback release: `/srv/hr-hunter/releases/20260416T092614Z-c654cce-ceo-order`
- Runtime service: `/etc/systemd/system/hr-hunter.service`
- Current service command: `/srv/hr-hunter/.venv/bin/python -m hr_hunter.cli serve --host 127.0.0.1 --port 8765 --workers 2`
- State source: Postgres from `/srv/hr-hunter/shared/env/hr-hunter.env`
- State backup before run pruning: `/srv/hr-hunter/backups/20260416T094559Z-pre-client-run-prune-live-env`

## What Changed

- CEO retrieval now prioritizes peer-company executive queries before generic broad CEO queries.
- Peer-company matching now handles compound company strings such as parenthetical parent brands.
- Malformed generic company fragments like `CEO`, `Furniture`, and year-like strings are treated as weak company identities.
- Transformer finalization verifies a wider scored tranche before choosing the final requested count.
- Final candidate ordering is bucket-aware: `Verified`, then `Needs Review`, then `Rejected`.
- Production now runs two Uvicorn workers so a long transformer job does not monopolize all health/status/UI traffic.
- Startup transformer warmup is disabled by default in production to avoid every worker loading the model at boot.
- Added `scripts/prune_project_runs.py` for operator run-history cleanup after backup.
- Live Hunt company paste is now split on both the client and server side, so pasted target-company and similar-company blobs break into real company entries instead of one malformed chip.
- Hunt wording is clearer live:
  - `Target Geography` -> `Where is the role based?`
  - `Must Current Companies` -> `Candidates must currently work at`
  - `Peer Companies` -> `Similar companies to search (optional)`
- ETA for new long transformer runs is now stage-aware and reliability-gated. The UI stays honest and shows an updating state until planning/retrieval/rerank/verifying have enough signal for a real countdown.

## Live Validation Results

- Health: external `/healthz` passed.
- Concurrency smoke: 20 concurrent `/healthz` probes passed, max latency under 1 second.
- Projects visible: `5`.
- Each visible project now has 1-2 saved runs.
- Results load for each latest project run.
- CSV export returns real 300-row candidate CSV files.
- Transformer default confirmed in `/app-config`.

Latest project runs after cleanup:

- CEO Test: `chief-executive-officer-(ceo)-9530e9dd`, `300 / 34 verified / 266 review / 0 reject`, `554s`.
- UAE Supply Chain Manager: `supply-chain-manager-e424bd18`, `300 / 212 verified / 88 review / 0 reject`, `186s`.
- Project Architect Test: `project-architect-07ac2f33`, `300 / 259 verified / 41 review / 0 reject`, `330s`.
- Senior Accountant Test: `senior-accountant-8c860221`, `300 / 167 verified / 133 review / 0 reject`.
- AI Engineer Test: `ai-engineer-baae73bf`, `300 / 73 verified / 227 review / 0 reject`.

Additional live runs after the client-ready pass:

- CEO - Marina Homes: `ceo-dcdc6591`, `587 / 437 verified / 115 review / 35 reject`, `732s`.
- Head of HR - hold co: `1000 / 114 verified / 886 review / 0 reject`, `399s`.

## Client Positioning

- Best showcase families: Supply Chain / Logistics and Project Architect / Architecture.
- CEO is now demo-usable as a pilot family, with verified executives ordered first, but should still be positioned as public-evidence constrained.
- AI Engineer is usable for broad sourcing, but do not promise high strict-verified yield yet.

## Rollback

```bash
sudo ln -sfn /srv/hr-hunter/releases/20260416T092614Z-c654cce-ceo-order /srv/hr-hunter/current
sudo cp /srv/hr-hunter/current/ops/systemd/hr-hunter.service /etc/systemd/system/hr-hunter.service
sudo systemctl daemon-reload
sudo systemctl restart hr-hunter
curl -fsS http://127.0.0.1:8765/healthz
```
