# HR Hunter Team README

Canonical deploy handoff: [docs/codex-production-handoff.md](docs/codex-production-handoff.md)
Local validation note: [docs/local-transformer-validation-20260415.md](docs/local-transformer-validation-20260415.md)
Live validation note: [docs/client-ready-live-validation-20260416.md](docs/client-ready-live-validation-20260416.md)

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
- runtime/progress truth fix so report summaries preserve job wall-clock runtime, transformer pipeline runtime, and product target runtime
- asset cache bump so live browsers pull the latest app bundle
- extraction/company-quality pass for malformed company fragments and regional profile-host location evidence

## Latest Live Client-Ready Validation

Final live release path:

- `/srv/hr-hunter/releases/20260416T094357Z-c6c79d7-timeout-safe`

Verified live on `https://hr-hunter.hyvelabs.tech`:

- `/healthz` returns healthy
- app is running with two Uvicorn workers to reduce timeout risk during long searches
- admin session API works
- five validation projects load
- latest Supply Chain run attaches to the correct project
- Results and report summary load with truthful runtime fields
- CSV export returns a real candidate CSV
- visible project run history is pruned to 1-2 runs per project after a Postgres-backed state backup

Latest fresh Supply Chain run:

- run id: `supply-chain-manager-e424bd18`
- backend: `transformer_v2`
- returned: `300`
- verified/review/reject: `212 / 88 / 0`
- raw/unique/query count: `889 / 434 / 73`
- job elapsed: `186s`
- saved report runtime: `182s`
- target runtime baseline: `900s`

Latest Project Architect quality validation from the same quality code path:

- run id: `project-architect-07ac2f33`
- backend: `transformer_v2`
- returned: `300`
- verified/review/reject: `259 / 41 / 0`
- raw/unique/query count: `2242 / 1272 / 135`
- job elapsed: `330s`

Latest CEO pilot validation:

- run id: `chief-executive-officer-(ceo)-9530e9dd`
- backend: `transformer_v2`
- returned: `300`
- verified/review/reject: `34 / 266 / 0`
- raw/unique/query count: `3737 / 505 / 212`
- job elapsed: `554s`
- result ordering: verified candidates appear before review candidates

## Current Verification Baseline

Use these current local app-project baselines as the reference when validating another environment:

- Supply Chain Manager: `300 returned / 182 verified / 118 review / 0 reject`
- AI Engineer: `300 returned / 78 verified / 222 review / 0 reject`
- Chief Executive Officer (CEO): `300 returned / 36 verified / 264 review / 0 reject`
- Project Architect: `300 returned / 136 verified / 164 review / 0 reject`
- Senior Accountant: `300 returned / 182 verified / 118 review / 0 reject`

If another environment performs materially worse than these on the same briefs, treat it as a deployment/config/runtime issue until proven otherwise.

Use `docs/local-transformer-validation-20260415.md` for the exact hunt briefs, run IDs, and comparison notes.

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

- HR / Talent Acquisition
- Legal / Compliance
- Sustainability / ESG
- General Operations
- Data / BI / Analytics
- Product / Program / Government / Research
- Executive / CEO when demoed honestly as public-evidence constrained sourcing

### Weak families not to oversell

- Healthcare / Doctors
- Pharma / Clinical
- Government / Public Sector
- Aviation / Maritime

Accounting and architecture improved locally in this quality pass, but they should still be validated from saved runs instead of being sold as universal strengths.

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

## Latest Local Quality Pass

The latest local-only transformer quality pass added:

- family-history-aware query expansion
- family-history-aware verifier thresholds
- stricter extraction/company sanitation
- app-level retrieval widening for large requests like `300`
- a local family sweep across all `31` families to identify weak families

The app project list was then reduced to the five validation projects used for deployment comparison.

## Reference

- deploy handoff: `docs/codex-production-handoff.md`
- repo: `https://github.com/hafeedh05/HR-Hunter_Hafeedh.git`
