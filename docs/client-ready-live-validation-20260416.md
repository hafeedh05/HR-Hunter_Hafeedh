# Client-Ready Live Validation - 2026-04-16

This note records the production validation for the transformer-first HR Hunter release plus the later 2026-04-17 live alignment fixes applied on the same release path.

## Live Deployment

- Live app: `https://hr-hunter.hyvelabs.tech`
- Active release path: `/srv/hr-hunter/releases/20260416T101500Z-client-ready-final`
- Previous rollback release path: `/srv/hr-hunter/releases/20260416T092614Z-c654cce-ceo-order`
- Health endpoint: `https://hr-hunter.hyvelabs.tech/healthz`
- Health result: `{"status":"ok"}`
- Active backend: `transformer_v2`
- Latest state backup before run pruning: `/srv/hr-hunter/backups/20260416T094559Z-pre-client-run-prune-live-env`

## What Changed In The Live Alignment Pass

- Preserved `transformer_v2` as the canonical search backend.
- Improved company/name sanitation so malformed company fragments are less likely to be treated as strong evidence.
- Added company-paste splitting for target and similar-company fields.
- Renamed Hunt wording for clarity:
  - `Where is the role based?`
  - `Target Companies`
  - `Similar Companies (optional)`
  - `Exclude Companies`
  - `Exclude Titles`
- Latest-run selection now prefers the project `latest_run_id`.
- Candidate detail now follows the clicked row more reliably.
- Reject reasons now surface the real verifier diagnostics.
- Exact-title normalization was tightened so strict matching treats normalized equivalents consistently.
- Parent/child company handling is tighter so child-brand verification requires explicit child-brand evidence.
- ETA is stage-aware for new runs and stays in an honest updating state until the countdown is trustworthy.

## Live Smoke Checks

- `/healthz` works externally.
- Admin session API works.
- Project list loads.
- Validation projects are visible.
- Latest runs attach to the correct projects after refresh.
- Results report loads for the latest run.
- CSV export returns real CSV content.
- Progress for long searches now shows a more honest stage-aware status.

## Fresh Live Benchmarks

### UAE Supply Chain Manager

- project id: `project_6f9ec43faae9`
- run id: `supply-chain-manager-e424bd18`
- backend: `transformer_v2`
- requested: `300`
- returned: `300`
- verified: `212`
- needs review: `88`
- rejected: `0`
- query count: `73`
- raw found: `889`
- unique after dedupe: `434`
- job elapsed: `186s`
- saved report runtime: `182s`

### Project Architect

- project id: `project_340170c8a1d0`
- run id: `project-architect-07ac2f33`
- backend: `transformer_v2`
- requested: `300`
- returned: `300`
- verified: `259`
- needs review: `41`
- rejected: `0`
- query count: `135`
- raw found: `2242`
- unique after dedupe: `1272`
- job elapsed: `330s`

### CEO Test

- project id: `project_7c51c5b5a240`
- run id: `chief-executive-officer-(ceo)-9530e9dd`
- backend: `transformer_v2`
- requested: `300`
- returned: `300`
- verified: `34`
- needs review: `266`
- rejected: `0`
- query count: `212`
- raw found: `3737`
- unique after dedupe: `505`
- job elapsed: `554s`

### CEO - Marina Homes (broad targeted luxury retail pilot)

- project id: `project_7b0143fa2546`
- run id: `ceo-dcdc6591`
- backend: `transformer_v2`
- requested: `600`
- returned: `587`
- verified: `437`
- needs review: `115`
- rejected: `35`
- query count: `330`
- raw found: `3981`
- unique after dedupe: `587`
- job elapsed: `732s`

### Head of HR - hold co (latest exact-title correction)

- project id: `project_eb72b39b177e`
- run id: `head-of-hr-e03e3a06`
- backend: `transformer_v2`
- requested: `1000`
- returned: `1000`
- verified: `131`
- needs review: `775`
- rejected: `94`
- query count: `189`
- raw found: `2804`
- unique after dedupe: `1100`
- job elapsed: `643s`
- note: exact `Head Of Hr | HSBC | United Arab Emirates` candidates now verify correctly instead of false-rejecting on misleading title-scope diagnostics

## Current Honest Product Position

Safe to demo now:

- Supply Chain / Logistics
- Project Architect / Architecture
- Interior Design
- Digital Marketing
- Senior Accountant / Accounting

Pilot-only:

- CEO / executive search as a public-evidence-constrained pilot family
- Head of HR / HR leadership
- broader Finance / Accounting
- Legal / Compliance
- Sustainability / ESG
- General Operations

Do not oversell yet:

- AI / technical search when the client expects high strict-verified yield
- Healthcare / clinical
- Government / public sector
- Aviation / maritime

## Rollback

If the current release misbehaves:

```bash
sudo ln -sfn /srv/hr-hunter/releases/20260416T092614Z-c654cce-ceo-order /srv/hr-hunter/current
sudo systemctl restart hr-hunter
curl -fsS http://127.0.0.1:8765/healthz
```
