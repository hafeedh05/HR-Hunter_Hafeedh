# HR Hunter

HR Hunter is a recruiter-facing sourcing app with a familiar project workflow and a transformer-first search engine.

- Repo: [https://github.com/hafeedh05/HR-Hunter_Hafeedh.git](https://github.com/hafeedh05/HR-Hunter_Hafeedh.git)
- Deploy handoff: [docs/codex-production-handoff.md](C:/Users/abdul/Desktop/HR%20Hunter/HR%20Hunter%20Clone/docs/codex-production-handoff.md)

## Current Release Reality

This repo is cut for a client-safe transformer-first release.

- Canonical engine: `transformer_v2`
- Classic engine: fallback only
- UI stays familiar:
  - Projects
  - Hunt
  - Results
  - Candidates
  - Feedback
  - History
  - Settings
  - Admin
- Hunt Brief stays familiar and is not redesigned in this release

This release does not claim equal strength across every job family.

## Safe Usage Scope Today

### Strong families

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

- AI / Data / Software for strict verified promises
- Executive / CEO
- Healthcare / Doctors
- Pharma / Clinical
- Government / Public Sector
- Aviation / Maritime

## Product Overview

HR Hunter is built around a project-first recruiter workflow:

1. Sign in with an invite-only TOTP account.
2. Create or open a project.
3. Fill the Hunt Brief with titles, geography, companies, years, keywords, and JD context.
4. Run search in the background.
5. Review candidates and results inside the same project.
6. Export CSV and JSON reports.
7. Capture recruiter feedback and use it to improve ranking over time.

## Core Features

- Project-based recruiter workspace
- TOTP-only login flow
- Admin-managed recruiter accounts
- Hunt Brief with:
  - target titles
  - years and tolerance
  - countries, continents, cities, radius
  - current companies and peer companies
  - must-have and nice-to-have keywords
  - typed JD and uploaded JD support
  - recruiter notes
- Background search jobs
- Results, Candidates, Feedback, History, Settings, Admin
- Transformer-first ranking and verification
- CSV and JSON exports
- Recruiter feedback capture
- Learned ranking support from recruiter feedback
- SQLite local support and Postgres-compatible deployment path

## Tech Stack

- Python 3.10+
- FastAPI
- Uvicorn
- SQLite for local state by default
- Postgres-compatible database path for deployment
- Hugging Face Transformers for local transformer inference
- LightGBM + scikit-learn for learned ranking
- ScrapingBee for retrieval
- Vanilla HTML/CSS/JS frontend

## Run Locally

```bash
uv sync --extra dev --extra api --extra reranker --extra ranker
uv run hr-hunter serve --host 127.0.0.1 --port 8765
```

Open:

- [http://127.0.0.1:8765](http://127.0.0.1:8765)

## Important Runtime Variables

- `SCRAPINGBEE_API_KEY`
- `HR_HUNTER_OUTPUT_DIR`
- `HR_HUNTER_STATE_DB`
- `HR_HUNTER_FEEDBACK_DB`
- `HR_HUNTER_RANKER_MODEL_DIR`
- `HR_HUNTER_DATABASE_URL`
- TOTP/login env vars if using fixed-login bootstrap

## Local Validation

```bash
uv run pytest -q tests/test_api.py tests/test_state.py tests/test_verifier.py tests/test_transformer_query_planner.py tests/test_transformer_verifier.py tests/test_transformer_extraction.py
node --check UI/app.js
```

## Production Storage Reality

The current production shape is mixed:

- `Cloud SQL / Postgres` holds structured app state such as users, sessions, projects, runs, and latest-run attachments
- the `VM` still holds runtime and file-based assets such as code releases, `.venv`, logs, backups, CSV/JSON artifacts, caches, and SQLite feedback unless explicitly migrated

Do not assume that moving to Postgres alone removes VM disk pressure.

## Today's Release Notes

This release includes the following practical changes:

- transformer-first search path committed into the main repo
- taxonomy and transformer query profile files added
- transformer verifier and telemetry wiring added
- candidate name/company sanitation improved in UI and CSV export
- CSV download behavior fixed
- feedback page wording cleaned up for client readability
- deployment handoff updated for GitHub-to-GCP release flow
- startup compatibility fix for workspace state loading
- legacy report compatibility fix so older saved project runs still open in Results/Candidates

## Current Laptop Benchmark Baseline

These are the latest local transformer-first benchmark numbers that should be treated as the working baseline to match or improve:

- Supply Chain Manager: `282 returned / 105 verified / 164 review / 13 reject` in `32.26s`
- AI Engineer: `300 returned / 11 verified / 289 review / 0 reject` in `146.38s`
- Chief Executive Officer (CEO): `300 returned / 10 verified / 290 review / 0 reject` in `165.69s`

These numbers are not universal guarantees. They are the current reference points for teammate verification and production smoke comparison.

## Known Production Gotchas

- transformer mode must inherit the same secret resolution path as the main app, especially for `SCRAPINGBEE_API_KEY`
- old saved run JSON files may still contain deprecated scope-era fields; current code keeps them backward-compatible at load time
- safe smoke tests should use Supply Chain / Logistics or Architecture / Interior Design, not CEO or AI as the main go/no-go
- deploy operators should not commit or push back to GitHub unless explicitly asked

## Notes

- Do not present this release as universal coverage for every role family.
- Keep classic fallback internal.
- Use the safe families above for demos and early client positioning.
