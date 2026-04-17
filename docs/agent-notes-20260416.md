# Agent Notes - 2026-04-16 / 2026-04-17 Live Alignment Pass

## Production Release

- Live app: `https://hr-hunter.hyvelabs.tech`
- Active release path: `/srv/hr-hunter/releases/20260416T101500Z-client-ready-final`
- Previous rollback release: `/srv/hr-hunter/releases/20260416T092614Z-c654cce-ceo-order`
- Runtime service: `/etc/systemd/system/hr-hunter.service`
- State source: Postgres from `/srv/hr-hunter/shared/env/hr-hunter.env`
- State backup before run pruning: `/srv/hr-hunter/backups/20260416T094559Z-pre-client-run-prune-live-env`

## What Changed

- CEO retrieval was tightened to prioritize better executive company and title evidence.
- Company paste in Hunt now splits target and similar-company blobs into real chips on the client and server.
- Live Hunt wording now uses:
  - `Where is the role based?`
  - `Target Companies`
  - `Similar Companies (optional)`
  - `Exclude Companies`
  - `Exclude Titles`
- ETA for new long transformer runs is now stage-aware and stays in an updating state until the estimate is trustworthy.
- Results/Candidates/History now prefer the project `latest_run_id` instead of stale older saved runs.
- Reject reasons now come from the real verifier diagnostics rather than a generic reject fallback.
- Strict exact-title handling was fixed so variants like `Head Of Hr` and `Head of HR` normalize and verify correctly.
- Parent/child company handling was tightened so child-brand verification requires explicit child-brand evidence.
- Candidate-detail selection was stabilized so the clicked row and selected detail pane stay aligned more reliably.

## Live Validation Results

- Health: external `/healthz` passed.
- Projects visible: `5`.
- Visible projects still have 1-2 saved runs each after pruning.
- Results load for each latest project run.
- CSV export returns real CSV output.
- Transformer remains the default backend in `/app-config`.

Latest project runs:

- CEO Test: `chief-executive-officer-(ceo)-9530e9dd`, `300 / 34 verified / 266 review / 0 reject`, `554s`
- UAE Supply Chain Manager: `supply-chain-manager-e424bd18`, `300 / 212 verified / 88 review / 0 reject`, `186s`
- Project Architect Test: `project-architect-07ac2f33`, `300 / 259 verified / 41 review / 0 reject`, `330s`
- Senior Accountant Test: `senior-accountant-8c860221`, `300 / 167 verified / 133 review / 0 reject`
- AI Engineer Test: `ai-engineer-baae73bf`, `300 / 73 verified / 227 review / 0 reject`

Additional later live runs on the same release path:

- CEO - Marina Homes, broad targeted pilot: `ceo-dcdc6591`, `587 / 437 verified / 115 review / 35 reject`, `732s`
- Head of HR - hold co, exact-title/reject-reason correction: `head-of-hr-e03e3a06`, `1000 / 131 verified / 775 review / 94 reject`, `643s`

## Important Functional Notes

- Exact-title normalization now matters materially for strict matching. This specifically fixed false rejects like:
  - `Head Of Hr | HSBC | United Arab Emirates`
- Older saved runs may still contain stale or weaker extracted evidence, especially around location and parent/child company presentation.
- New runs benefit from the latest reject-reason path and latest-run selection fixes; old runs do not rewrite themselves.

## Client Positioning

- Best showcase families: Supply Chain / Logistics and Project Architect / Architecture.
- Head of HR is improved and now behaves more defensibly on exact-title strictness, but it is still a pilot family.
- CEO remains demo-usable as a pilot family, but should still be positioned as public-evidence constrained.

## Rollback

```bash
sudo ln -sfn /srv/hr-hunter/releases/20260416T092614Z-c654cce-ceo-order /srv/hr-hunter/current
sudo cp /srv/hr-hunter/current/ops/systemd/hr-hunter.service /etc/systemd/system/hr-hunter.service
sudo systemctl daemon-reload
sudo systemctl restart hr-hunter
curl -fsS http://127.0.0.1:8765/healthz
```
