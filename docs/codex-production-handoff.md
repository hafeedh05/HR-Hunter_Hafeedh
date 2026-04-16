# HR Hunter Client-Safe Release Handoff

This document is the deploy handoff for the current release cut from:

- Repo: `https://github.com/hafeedh05/HR-Hunter_Hafeedh.git`
- Current operator workspace: `/Users/rabiyashaikh/Downloads/HR_Hunter`
- Live validation note: `docs/client-ready-live-validation-20260416.md`

## Current Live Release

- Live app: `https://hr-hunter.hyvelabs.tech`
- Final release path: `/srv/hr-hunter/releases/20260416T101500Z-client-ready-final`
- Previous rollback release: `/srv/hr-hunter/releases/20260416T092614Z-c654cce-ceo-order`
- Health check: `https://hr-hunter.hyvelabs.tech/healthz` returns `{"status":"ok"}`
- Frontend assets: `20260416etaquality1`
- Production serving: `HR_HUNTER_WEB_WORKERS=2`
- Transformer startup warmup: `HR_HUNTER_WARM_TRANSFORMER_ON_STARTUP=0`
- Latest pre-prune backup: `/srv/hr-hunter/backups/20260416T094559Z-pre-client-run-prune-live-env`

### Fresh Live Validation

Supply Chain Manager is the latest fresh 300-candidate post-deploy validation run:

- run id: `supply-chain-manager-e424bd18`
- backend: `transformer_v2`
- returned: `300`
- verified / review / reject: `212 / 88 / 0`
- query count: `73`
- raw / unique: `889 / 434`
- job elapsed: `186s`
- saved report runtime: `182s`
- transformer pipeline elapsed: `37s`
- target runtime baseline: `900s`
- CSV export: real CSV confirmed

Project Architect quality validation from the same quality path:

- run id: `project-architect-07ac2f33`
- backend: `transformer_v2`
- returned: `300`
- verified / review / reject: `259 / 41 / 0`
- query count: `135`
- raw / unique: `2242 / 1272`
- job elapsed: `330s`

CEO pilot validation after the executive search/order pass:

- run id: `chief-executive-officer-(ceo)-9530e9dd`
- backend: `transformer_v2`
- returned: `300`
- verified / review / reject: `34 / 266 / 0`
- query count: `212`
- raw / unique: `3737 / 505`
- job elapsed: `554s`
- ordering: all verified candidates appear before review candidates.

Large targeted CEO pilot after company parsing and executive-retrieval cleanup:

- project id: `project_7b0143fa2546`
- run id: `ceo-dcdc6591`
- backend: `transformer_v2`
- requested / returned: `600 / 587`
- verified / review / reject: `437 / 115 / 35`
- raw / unique / query count: `3981 / 587 / 330`
- job elapsed: `732s`

Head of HR family-correction validation:

- project id: `project_eb72b39b177e`
- backend: `transformer_v2`
- requested / returned: `1000 / 1000`
- verified / review / reject: `114 / 886 / 0`
- raw / unique / query count: `2868 / 1000 / 189`
- job elapsed: `399s`
- family mapping: `hr_talent`

Run-history cleanup after backup:

- visible projects: `5`
- saved runs per project: `1-2`

CEO is still constrained and should not be used as the main client go/no-go demo family. The latest live saved CEO run returned `300 / 16 verified / 284 review / 0 reject`, with diagnostics pointing to weak company/industry signals for a narrow executive brief.

## Release Decision For Today

- Canonical engine: `transformer_v2`
- Fallback engine: classic HR Hunter path, internal fallback only
- UI shell remains the same:
  - TOTP login
  - Projects
  - Hunt
  - Results
  - Candidates
  - History
  - Settings
  - Admin
- Hunt Brief remains familiar. This release does not redesign it.

## What This Release Includes

- transformer-first search path is the default
- existing taxonomy in `src/hr_hunter_transformer/taxonomy_data.yaml`
- existing family query profiles in `src/hr_hunter_transformer/query_profiles.py`
- existing transformer verifier in `src/hr_hunter_transformer/verifier.py`
- candidate/name/company sanitation in UI and CSV export
- truthful progress/status improvements
- CSV export that downloads as a real CSV file
- feedback page language cleaned up for client comprehension
- startup compatibility fix for workspace state loading
- legacy saved-run compatibility fix so older project runs still load
- job-wall-clock runtime persisted into saved summaries and telemetry
- product target runtime baseline restored (`300 candidates -> 900s`)
- cache-busted frontend assets for the latest Results/Candidates runtime fixes
- stricter malformed company-fragment filtering and regional profile-host location handling
- CEO/executive peer-company query priority and fuzzy peer-company matching
- verification-aware final ordering from a wider scored tranche
- two-worker web serving to keep health/status/UI responsive during long transformer searches
- run-prune operator utility under `scripts/prune_project_runs.py`
- live Hunt company-paste splitting for target-company and similar-company fields
- clearer live Hunt labels:
  - `Where is the role based?`
  - `Candidates must currently work at`
  - `Similar companies to search (optional)`
- stage-aware ETA semantics for new long transformer runs so the UI does not invent countdowns during planning or early retrieval

## Current Transformer App Baseline

Use these saved local app-project results as the reference baseline for deployment verification and post-deploy comparisons:

- Supply Chain Manager: `300 returned / 182 verified / 118 review / 0 reject`
- AI Engineer: `300 returned / 78 verified / 222 review / 0 reject`
- Chief Executive Officer (CEO): `300 returned / 36 verified / 264 review / 0 reject`
- Project Architect: `300 returned / 136 verified / 164 review / 0 reject`
- Senior Accountant: `300 returned / 182 verified / 118 review / 0 reject`

Post-deploy validation should try to land in the same general band on equivalent briefs. Exact matching is not required.

Use the exact local brief definitions here:

- `docs/local-transformer-validation-20260415.md`

## What This Release Does Not Claim

- full family-complete performance
- equal strength across every role family
- universal verified-yield quality for executive / AI / clinical / government roles
- full benchmark coverage across all families

## Safe Client Positioning

### Safe Families To Position Now

- Supply Chain / Logistics
- Digital Marketing
- Interior Design
- Architecture / Project Architect
- CEO / executive search when positioned as pilot/public-evidence constrained

### Pilot-Only Families

- Finance / Accounting
- HR / Talent Acquisition
- Legal / Compliance
- Sustainability / ESG
- General Operations

### Weak Families Not To Oversell

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
- transformer config must inherit the same secret-resolution behavior as the main app
- state storage must be verified before deploy:
  - Postgres if production uses it
  - or the configured SQLite path if intentionally running local-style

## Production Storage Reality

Production is mixed-storage today:

- `Cloud SQL / Postgres` stores structured app state such as users, sessions, projects, runs, and latest-run attachments
- the `VM` still stores runtime and file-based assets such as release folders, `.venv`, logs, backups, CSV/JSON artifacts, and caches
- SQLite feedback can still exist on the VM unless explicitly migrated

Do not describe production as “everything on Cloud SQL.”

## Known Deploy Gotchas

- previous production deployment needed a hotfix because transformer config was not picking up the same secret-loading path as the main app
- previous production deployment needed a hotfix because old saved runs still contained deprecated scope-era fields in report JSON
- old saved projects can appear to have no Results/Candidates if that compatibility path breaks
- operators should not push code back to GitHub from deployment work unless explicitly asked
- old saved run ETAs remain historical; only new runs get the fixed stage-aware ETA behavior

## Branch And Merge Flow

1. Commit and push changes from local repo into GitHub
2. Merge into `main`
3. Deploy merged `main` to GCP
4. Smoke test on a safe family and confirm health remains responsive during/after long searches

## Pre-Deploy Verification

Run these locally before pushing:

```powershell
cd "C:\Users\abdul\Desktop\HR Hunter\HR Hunter Clone"
.\.venv\Scripts\python.exe -m pytest -q tests\test_api.py tests\test_state.py tests\test_verifier.py tests\test_transformer_query_planner.py tests\test_transformer_verifier.py tests\test_transformer_extraction.py
node --check "UI\app.js"
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
- Candidates tab renders clean names/companies

## GCP Deploy Flow

On the VM:

1. Pull latest merged `main` from GitHub
2. Create a new release directory
3. Sync the repo into the release directory
4. Reuse or update the app venv safely
5. Point the service to the new release
6. Restart the service

Recommended stable runtime layout:

- service `WorkingDirectory` should be `/srv/hr-hunter/current`
- service `PYTHONPATH` should be `/srv/hr-hunter/current/src`
- service `ExecStart` should use a shared venv at `/srv/hr-hunter/.venv`
- deploy should only repoint `/srv/hr-hunter/current` and restart the service
- old releases should be pruned automatically after deploy confidence is high

The repo includes systemd templates for this under:

- `ops/systemd/hr-hunter.service`
- `ops/systemd/hr-hunter-maintenance.service`
- `ops/systemd/hr-hunter-maintenance.timer`
- `ops/systemd/hr-hunter-backup.service`
- `ops/systemd/hr-hunter-backup.timer`
- `ops/systemd/hr-hunter-healthcheck.service`
- `ops/systemd/hr-hunter-healthcheck.timer`
- `ops/systemd/hr-hunter-restore-drill.service`
- `ops/systemd/hr-hunter-restore-drill.timer`

The repo also includes operator scripts for safer cutovers:

- `ops/deploy/deploy_release.sh`
- `ops/deploy/rollback_release.sh`

Example service commands:

```bash
sudo systemctl daemon-reload
sudo systemctl restart hr-hunter
sudo systemctl status hr-hunter --no-pager
```

## Post-Deploy Smoke Tests

```bash
curl -f http://127.0.0.1:8765/healthz
curl -f https://hr-hunter.hyvelabs.tech/healthz
journalctl -u hr-hunter -n 100 --no-pager
```

In browser:

- sign in
- verify old projects are visible
- open an old project and confirm Results load
- open Candidates and confirm candidate rows load
- run a Supply Chain search
- verify progress moves
- verify latest run lands on the right project
- verify CSV download opens a real CSV

## Extended Validation If Explicitly Requested

If asked to do a deeper validation sweep after deploy:

1. back up the current production DB/state first
2. verify the current live project list and only delete old project records if explicitly instructed after backup is confirmed
3. create or refresh these transformer-mode test projects:
   - Supply Chain Manager
   - AI Engineer
   - Chief Executive Officer (CEO)
   - Project Architect
   - Senior Accountant
4. use the exact briefs from `docs/local-transformer-validation-20260415.md`
5. run all five and record:
   - runtime
   - returned
   - verified
   - needs review
   - rejected
   - execution backend
6. compare those numbers against the local app baseline above

### Important Comparison Rule

- similar results are acceptable
- exact numerical matching is not required
- backend must be `transformer_v2`
- candidate target should still land at `300`
- verified/review mix should be in the same general range

## Rollback

If deploy is bad:

1. repoint the service to the previous release path
2. restart `hr-hunter`
3. verify `/healthz`
4. keep the failed release on disk for inspection

Do not delete the previous release until rollback confidence is high.

## Runtime Retention

The VM should not grow forever. Use `hr-hunter runtime-maintenance` with a daily timer.

Recommended defaults:

- keep `8` releases including the active release
- keep `30` backup directories
- only prune backups older than `14` days
- prune orphaned JSON or CSV artifacts older than `45` days
- vacuum systemd journals to `14` days
- clean apt cache after maintenance

This is designed to keep rollback headroom while preventing release and artifact sprawl.

## Backup And Monitoring

The repo now includes two more operator commands:

- `hr-hunter runtime-backup`
- `hr-hunter runtime-healthcheck`
- `hr-hunter runtime-restore-drill`

Recommended production setup:

- run `runtime-backup` daily
- upload the resulting archive to a private GCS bucket with `HR_HUNTER_BACKUP_GCS_URI`
- run `runtime-healthcheck` every `15` minutes and persist JSON snapshots under `/srv/hr-hunter/shared/monitoring/health`
- set `HR_HUNTER_ALERT_WEBHOOK_URL` to a real webhook destination so unhealthy or recovered runtime states are pushed automatically
- optional custom headers can be passed with `HR_HUNTER_ALERT_WEBHOOK_HEADERS_JSON`
- run `runtime-restore-drill` weekly and keep extracted validation output under `/srv/hr-hunter/shared/monitoring/restore-drills`
- treat non-zero `runtime-healthcheck` or `runtime-restore-drill` exits as an operator-grade failure, not a cosmetic warning

The backup command currently captures:

- current release metadata
- systemd and Caddy config snapshots
- a redacted env snapshot
- the authoritative workspace DB snapshot
- the feedback DB when present
- a manifest of referenced run artifacts

The restore drill validates:

- latest local or `gs://` backup archive selection
- archive extraction into an isolated drill folder
- workspace DB readability:
  - `pg_restore -l` for Postgres backups
  - `PRAGMA integrity_check` for SQLite backups
- feedback SQLite integrity when a feedback snapshot exists

## Transformer Warm Start

Transformer scoring should not cold-load on every search. The app now warms the transformer runtime in the background at startup and exposes runtime cache status in:

- `/app-config`
- `/app/ops`

This reduces the worst cold-start penalty and makes it obvious whether the live process actually has a warmed transformer pipeline cached.

## Adaptive Transformer Retrieval

The ScrapingBee transformer retriever now behaves more like an optimizer loop instead of a fixed firehose:

- it prioritizes exact-title and strongest person-profile queries before weaker adjacent-title leakage
- it runs retrieval in batches instead of launching the entire query plan at once
- dense families can stop early once the raw evidence pool is already strong and recent batches have plateaued
- hard families can automatically spend extra page budget on the strongest exact-title probes when early yield is weak

This keeps retrieval more honest and more scalable:

- reports preserve planned query count versus executed query count
- dense roles waste fewer requests on low-yield tail queries
- hard roles widen more intentionally instead of stalling in a brittle first pass

## Do Not Change In This Release

- do not redesign the UI
- do not redesign the Hunt Brief
- do not expose classic fallback as a normal setting
- do not claim full role-family coverage
- do not begin major verifier redesign in the release branch
- do not push code back to GitHub during deploy unless explicitly asked
