# HR Hunter Production Handoff

This document is the current deploy handoff for the live transformer-first HR Hunter release.

- Repo: `https://github.com/hafeedh05/HR-Hunter_Hafeedh.git`
- Live app: `https://hr-hunter.hyvelabs.tech`
- Validation note: `docs/client-ready-live-validation-20260416.md`

## Current Live Release

- Active release path: `/srv/hr-hunter/releases/20260416T101500Z-client-ready-final`
- Previous rollback release: `/srv/hr-hunter/releases/20260416T092614Z-c654cce-ceo-order`
- Health check: `https://hr-hunter.hyvelabs.tech/healthz` returns `{"status":"ok"}`
- Production serving is currently in a stability-first single-worker mode after later live verifier/search patches on the same release path
- Transformer startup warmup remains disabled in production
- Latest pre-prune backup: `/srv/hr-hunter/backups/20260416T094559Z-pre-client-run-prune-live-env`

## What The Release Includes

- `transformer_v2` as the canonical engine
- classic fallback kept internal only
- truthful runtime and progress data persisted into saved run summaries
- company-paste splitting for live Hunt target and similar-company fields
- live Hunt wording now uses:
  - `Where is the role based?`
  - `Target Companies`
  - `Similar Companies (optional)`
  - `Exclude Companies`
  - `Exclude Titles`
- stage-aware ETA semantics for long transformer runs
- latest-run selection fixes so Results/Candidates/History prefer the project `latest_run_id`
- reject reasons now come from real verifier diagnostics instead of a generic fallback
- strict exact-title normalization fixes
- tighter parent/child company handling so child-brand verification requires explicit child-brand evidence
- candidate-detail selection fixes so the clicked row and detail pane stay aligned more reliably

## Fresh Live Validation

Supply Chain Manager:

- run id: `supply-chain-manager-e424bd18`
- backend: `transformer_v2`
- returned: `300`
- verified / review / reject: `212 / 88 / 0`
- query count: `73`
- raw / unique: `889 / 434`
- job elapsed: `186s`

Project Architect:

- run id: `project-architect-07ac2f33`
- backend: `transformer_v2`
- returned: `300`
- verified / review / reject: `259 / 41 / 0`
- query count: `135`
- raw / unique: `2242 / 1272`
- job elapsed: `330s`

CEO Test:

- run id: `chief-executive-officer-(ceo)-9530e9dd`
- backend: `transformer_v2`
- returned: `300`
- verified / review / reject: `34 / 266 / 0`
- query count: `212`
- raw / unique: `3737 / 505`
- job elapsed: `554s`

CEO - Marina Homes, broad targeted pilot:

- project id: `project_7b0143fa2546`
- run id: `ceo-dcdc6591`
- backend: `transformer_v2`
- requested / returned: `600 / 587`
- verified / review / reject: `437 / 115 / 35`
- raw / unique / query count: `3981 / 587 / 330`
- job elapsed: `732s`

Head of HR - hold co, latest exact-title/reject-reason correction:

- project id: `project_eb72b39b177e`
- run id: `head-of-hr-e03e3a06`
- backend: `transformer_v2`
- requested / returned: `1000 / 1000`
- verified / review / reject: `131 / 775 / 94`
- raw / unique / query count: `2804 / 1100 / 189`
- job elapsed: `643s`
- exact `Head Of Hr | HSBC | United Arab Emirates` candidates now verify correctly instead of false-rejecting on misleading strict-title diagnostics

## Current Local Reference Baseline

Use these saved local app-project results as the reference baseline for deployment verification:

- Supply Chain Manager: `300 returned / 182 verified / 118 review / 0 reject`
- AI Engineer: `300 returned / 78 verified / 222 review / 0 reject`
- Chief Executive Officer (CEO): `300 returned / 36 verified / 264 review / 0 reject`
- Project Architect: `300 returned / 136 verified / 164 review / 0 reject`
- Senior Accountant: `300 returned / 182 verified / 118 review / 0 reject`

Use `docs/local-transformer-validation-20260415.md` for the exact local hunt briefs.

## Safe Client Positioning

Safe families to position now:

- Supply Chain / Logistics
- Digital Marketing
- Interior Design
- Architecture / Project Architect
- Senior Accountant / Accounting

Pilot-only families:

- Finance / Accounting outside the validated accountant path
- HR / Talent Acquisition
- Legal / Compliance
- Sustainability / ESG
- General Operations
- Executive / CEO when positioned honestly as public-evidence constrained sourcing
- Head of HR / HR leadership

Weak families not to oversell:

- AI / Data / Software for strict verified promises
- Healthcare / Doctors
- Pharma / Clinical
- Government / Public Sector
- Aviation / Maritime

## Critical Runtime Assumptions

- `transformer_v2` is the normal user path
- classic fallback should not appear as a normal user-facing mode
- `SCRAPINGBEE_API_KEY` must be present
- runtime environment must have:
  - `transformers`
  - `torch`
- state storage must be verified before deploy:
  - Postgres if production uses it
  - or the configured SQLite path if intentionally running local-style

## Production Storage Reality

Production is mixed-storage today:

- `Cloud SQL / Postgres` stores structured app state such as users, sessions, projects, runs, and latest-run attachments
- the `VM` still stores runtime and file-based assets such as releases, `.venv`, logs, backups, CSV/JSON artifacts, and caches
- SQLite feedback can still exist on the VM unless explicitly migrated

Do not describe production as "everything on Cloud SQL."

## Known Deploy Gotchas

- old saved projects can still expose stale UI state if the browser is not hard refreshed after asset bumps
- old saved run ETAs remain historical; only new runs benefit from the stage-aware ETA behavior
- operators should not push code back to GitHub from deployment work unless explicitly asked

## Branch And Merge Flow

1. Commit and push changes from local repo into GitHub
2. Merge into `main`
3. Deploy merged `main` to GCP
4. Smoke test on a safe family and confirm health remains responsive during or after long searches

## Pre-Deploy Verification

Run these locally before pushing:

```powershell
cd "C:\Users\abdul\Desktop\HR Hunter\HR Hunter Clone"
pytest -q tests\test_recruiter_app.py tests\test_transformer_query_planner.py tests\test_transformer_verifier.py tests\test_transformer_extraction.py
node --check "UI\app.js"
git diff --check
```

Then run the app locally and smoke test:

```powershell
cd "C:\Users\abdul\Desktop\HR Hunter\HR Hunter Clone"
$env:PYTHONPATH = "C:\Users\abdul\Desktop\HR Hunter\HR Hunter Clone\src"
.\.venv\Scripts\python.exe -m hr_hunter.cli serve --host 127.0.0.1 --port 8765
```

Check:

- login works
- Hunt loads without JS errors
- search starts
- progress updates
- results attach to the correct project
- old saved projects still open Results/Candidates
- CSV download works
- Candidates tab renders clean names and companies
