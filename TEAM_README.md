# HR Hunter Team README

Canonical deploy handoff: [docs/codex-production-handoff.md](docs/codex-production-handoff.md)
Local validation note: [docs/local-transformer-validation-20260415.md](docs/local-transformer-validation-20260415.md)
Live validation note: [docs/client-ready-live-validation-20260416.md](docs/client-ready-live-validation-20260416.md)

## Current Team Reality

HR Hunter is a transformer-first recruiter sourcing app with a stable recruiter UI shell and a live production deployment on [hr-hunter.hyvelabs.tech](https://hr-hunter.hyvelabs.tech).

Current working position:

- canonical engine: `transformer_v2`
- classic engine: fallback only
- UI shell remains:
  - Projects
  - Hunt
  - Results
  - Candidates
  - Feedback
  - History
  - Settings
  - Admin
- TOTP login remains unchanged
- Hunt Brief remains familiar, but the live wording is now clearer

## What The Repo Now Reflects

- transformer-first search path committed into the repo
- taxonomy and family query profile files added
- transformer verifier, telemetry, ranking, and export flow added
- candidate name/company cleanup in UI and CSV export
- CSV download fixed to return real CSV files
- feedback page wording simplified for user understanding
- startup compatibility fix for workspace state loading
- legacy saved-run compatibility fix for Results/Candidates loading
- runtime/progress truth fix so report summaries preserve job wall-clock runtime, transformer pipeline runtime, and product target runtime
- extraction/company-quality pass for malformed company fragments and regional profile-host location evidence
- company-paste splitting for target and similar-company fields in the live Hunt brief
- live Hunt wording now uses:
  - `Where is the role based?`
  - `Target Companies`
  - `Similar Companies (optional)`
  - `Exclude Companies`
  - `Exclude Titles`
- stage-aware ETA reliability for long transformer jobs so the UI stays in an honest updating state until the estimate is trustworthy
- latest-run binding fixes so Results/Candidates/History follow the project `latest_run_id`
- reject reasons now use real verifier diagnostics instead of a generic fallback
- strict exact-title normalization fixes so variants like `Head Of Hr` and `Head of HR` verify consistently
- parent/child company handling is tighter: child-brand verification requires explicit child-brand evidence

## Latest Live Validation Snapshot

Live release path:

- `/srv/hr-hunter/releases/20260416T101500Z-client-ready-final`

Verified live on [hr-hunter.hyvelabs.tech](https://hr-hunter.hyvelabs.tech):

- `/healthz` returns healthy
- admin session API works
- project list loads
- latest Supply Chain run attaches to the correct project
- Results and report summary load with truthful runtime fields
- CSV export returns a real candidate CSV
- visible project run history is pruned to 1-2 runs per project after a Postgres-backed state backup
- later 2026-04-17 live-fix passes stayed on the same release path and patched:
  - latest-run selection
  - candidate-detail consistency
  - reject reasons
  - exact-title strict matching
  - parent/child company handling

Latest validated runs:

- Supply Chain Manager
  - `supply-chain-manager-e424bd18`
  - `300 / 212 verified / 88 review / 0 reject`
  - `889 raw / 434 unique / 73 queries`
  - `186s`
- Project Architect
  - `project-architect-07ac2f33`
  - `300 / 259 verified / 41 review / 0 reject`
  - `2242 raw / 1272 unique / 135 queries`
  - `330s`
- CEO Test
  - `chief-executive-officer-(ceo)-9530e9dd`
  - `300 / 34 verified / 266 review / 0 reject`
  - `3737 raw / 505 unique / 212 queries`
  - `554s`
- CEO - Marina Homes, broad targeted pilot
  - `ceo-dcdc6591`
  - `587 / 437 verified / 115 review / 35 reject`
  - `3981 raw / 587 unique / 330 queries`
  - `732s`
- Head of HR - hold co, latest exact-title/reject-reason correction
  - `head-of-hr-e03e3a06`
  - `1000 / 131 verified / 775 review / 94 reject`
  - `2804 raw / 1100 unique / 189 queries`
  - `643s`
  - exact `Head Of Hr | HSBC | United Arab Emirates` candidates now verify correctly instead of false-rejecting on misleading strict-title diagnostics

## Current Verification Baseline

Use these current local app-project baselines as the reference when validating another environment:

- Supply Chain Manager: `300 returned / 182 verified / 118 review / 0 reject`
- AI Engineer: `300 returned / 78 verified / 222 review / 0 reject`
- Chief Executive Officer (CEO): `300 returned / 36 verified / 264 review / 0 reject`
- Project Architect: `300 returned / 136 verified / 164 review / 0 reject`
- Senior Accountant: `300 returned / 182 verified / 118 review / 0 reject`

If another environment performs materially worse than these on the same briefs, treat it as a deployment/config/runtime issue until proven otherwise.

## Safe Internal/Product Scope

Safe families right now:

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
- Data / BI / Analytics
- Product / Program / Government / Research
- Executive / CEO when demoed honestly as public-evidence constrained sourcing
- Head of HR / HR leadership

Weak families not to oversell:

- Healthcare / Doctors
- Pharma / Clinical
- Government / Public Sector
- Aviation / Maritime

## Storage Reality

Production is mixed-storage today:

- `Cloud SQL / Postgres` is the source of truth for structured app state
- the `VM` still holds runtime assets and file-based artifacts such as releases, `.venv`, logs, backups, caches, and feedback SQLite unless migrated

Do not describe production as "fully on Cloud SQL."

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

## Latest Local Quality Pass

The current repo also includes:

- family-history-aware query expansion
- family-history-aware verifier thresholds
- stricter extraction/company sanitation
- app-level retrieval widening for large requests like `300`
- strict exact-title/company/location work that is now being aligned with the live environment

## Reference

- deploy handoff: `docs/codex-production-handoff.md`
- repo: `https://github.com/hafeedh05/HR-Hunter_Hafeedh.git`
