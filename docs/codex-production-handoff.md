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

- Transformer-first search path is the default
- Existing taxonomy in `src/hr_hunter_transformer/taxonomy_data.yaml`
- Existing family query profiles in `src/hr_hunter_transformer/query_profiles.py`
- Existing transformer verifier in `src/hr_hunter_transformer/verifier.py`
- Candidate/name/company sanitation in UI and CSV export
- Truthful progress/status improvements
- CSV export that downloads as a real CSV file
- Feedback page language cleaned up for client comprehension

## What This Release Does Not Claim

- Full family-complete performance
- Equal strength across every role family
- Universal verified-yield quality for executive / AI / clinical / government roles
- Full benchmark coverage across all families

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
- state storage must be verified before deploy:
  - Postgres if production uses it
  - or the configured SQLite path if intentionally running local-style

## Branch And Merge Flow

1. Create a release branch from the current local state:
   - `codex/release-cut-transformer-gcp`
2. Commit only deploy-safe work:
   - transformer default behavior
   - UI clarity/sanitization
   - progress truthfulness
   - export fixes
   - handoff docs
3. Push branch to `origin`
4. Open PR into `main`
5. Merge after smoke checks pass
6. Deploy merged `main` to GCP

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
.\.venv\Scripts\python.exe -m hr_hunter.api
```

Check:

- login works
- Hunt loads without JS errors
- search starts
- progress updates
- results attach to the correct project
- CSV download works
- Candidates tab renders clean names/companies

## GCP Deploy Flow

On the VM:

1. Pull latest merged `main` from GitHub
2. Create a new release directory
3. Sync the repo into the release directory
4. Activate/install dependencies in the app venv
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
- open existing project
- run Supply Chain search
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

- Do not redesign the UI
- Do not redesign the Hunt Brief
- Do not expose classic fallback as a normal setting
- Do not claim full role-family coverage
- Do not begin major verifier redesign in the release branch
