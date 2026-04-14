# HR Hunter Client-Safe Release Handoff

This document is the deploy handoff for the current release cut from:

- Repo: `https://github.com/hafeedh05/HR-Hunter_Hafeedh.git`
- Local source workspace: `C:\Users\abdul\Desktop\HR Hunter\HR Hunter Clone`

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

### Pilot-Only Families

- Finance / Accounting
- HR / Talent Acquisition
- Legal / Compliance
- Sustainability / ESG
- General Operations

### Weak Families Not To Oversell

- AI / Data / Software for strict verified promises
- Executive / CEO
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

## Branch And Merge Flow

1. Commit and push changes from local repo into GitHub
2. Merge into `main`
3. Deploy merged `main` to GCP
4. Smoke test on a safe family

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

## Rollback

If deploy is bad:

1. repoint the service to the previous release path
2. restart `hr-hunter`
3. verify `/healthz`
4. keep the failed release on disk for inspection

Do not delete the previous release until rollback confidence is high.

## Do Not Change In This Release

- do not redesign the UI
- do not redesign the Hunt Brief
- do not expose classic fallback as a normal setting
- do not claim full role-family coverage
- do not begin major verifier redesign in the release branch
- do not push code back to GitHub during deploy unless explicitly asked
