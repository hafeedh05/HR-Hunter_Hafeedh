# HR Hunter Team README

Canonical deploy handoff: [docs/codex-production-handoff.md](C:/Users/abdul/Desktop/HR%20Hunter/HR%20Hunter%20Clone/docs/codex-production-handoff.md)

## Current Team Reality

HR Hunter is now a transformer-first recruiter sourcing app with a stable recruiter UI shell and a client-safe release cut prepared for deployment from GitHub to GCP.

This is the working team position:

- canonical engine: `transformer_v2`
- classic engine: fallback only
- UI stays familiar:
  - Projects
  - Hunt
  - Results
  - Candidates
  - Feedback
  - History
  - Settings
  - Admin
- TOTP login stays the same
- Hunt Brief stays familiar for this release

## What We Shipped In This Release Cut

- transformer-first search path committed into the repo
- taxonomy and family query profile files added
- transformer verifier, telemetry, ranking, and export flow added
- candidate name/company cleanup in UI and CSV export
- CSV download fixed to return real CSV files
- feedback page wording simplified for user understanding
- production deploy handoff updated for GitHub-to-GCP deployment
- workspace-state startup compatibility fix
- legacy saved-run compatibility fix for Results/Candidates loading

## Current Verification Baseline

Use these local transformer benchmark numbers as the current reference when validating another environment:

- Supply Chain Manager: `282 returned / 105 verified / 164 review / 13 reject` in `32.26s`
- AI Engineer: `300 returned / 11 verified / 289 review / 0 reject` in `146.38s`
- Chief Executive Officer (CEO): `300 returned / 10 verified / 290 review / 0 reject` in `165.69s`

If another environment performs materially worse than these on the same brief, treat it as a deployment/config/runtime issue until proven otherwise.

## What Was Wrong In The Previous Deploy Cycle

- production transformer search initially failed because transformer config was not inheriting the main app secret-resolution behavior, so ScrapingBee was missing in transformer mode
- old saved run JSON files could fail to open because deprecated scope-era fields were still being deserialized directly into `CandidateProfile`
- deploy notes incorrectly suggested `python -m hr_hunter.api` as the startup command; the correct app startup path is the CLI serve command
- a deploy operator response included git update directives even though deploy work should not push code unless explicitly requested

## Safe Internal/Product Scope

### Safe families right now

- Supply Chain / Logistics
- Digital Marketing
- Interior Design
- Architecture / Project Architect

### Pilot-only families

- Finance / Accounting
- HR / Talent Acquisition
- Legal / Compliance
- Sustainability / ESG
- General Operations

### Weak families not to oversell

- AI / Data / Software for strict verified-yield promises
- Executive / CEO
- Healthcare / Doctors
- Pharma / Clinical
- Government / Public Sector
- Aviation / Maritime

## Storage Reality

Production is mixed-storage today:

- `Cloud SQL / Postgres` is the source of truth for structured app state
- the `VM` still holds runtime assets and file-based artifacts such as releases, `.venv`, logs, backups, caches, and feedback SQLite unless migrated

Do not describe production as “fully on Cloud SQL.”

## Operating Rules For The Team

- do not present the product as universal coverage for every role family
- do not expose classic fallback as a normal user-facing mode
- do not redesign the UI before this deploy
- do not redesign the Hunt Brief before this deploy
- do not start a large verifier rewrite on the release branch
- do not commit or push back to GitHub from a deployment task unless explicitly asked

## Practical Team Workflow

1. make changes in the repo
2. validate locally
3. push to GitHub
4. deploy from GitHub to GCP
5. smoke test on a safe family
6. roll back fast if health or search behavior is broken

## Validation Baseline

Before deploy, the minimum checks are:

- app starts with:
  - `uv run hr-hunter serve --host 127.0.0.1 --port 8765`
- login works
- Hunt loads cleanly
- search starts
- progress updates
- results attach to the correct project
- old saved project runs still open
- CSV export downloads correctly
- Candidates tab shows clean names and companies

## Reference

- deploy handoff: `docs/codex-production-handoff.md`
- repo: `https://github.com/hafeedh05/HR-Hunter_Hafeedh.git`
