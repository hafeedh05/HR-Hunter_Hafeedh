# HR Hunter

HR Hunter is a recruiter-facing sourcing app with a familiar project workflow and a transformer-first search engine.

- Repo: [https://github.com/hafeedh05/HR-Hunter_Hafeedh.git](https://github.com/hafeedh05/HR-Hunter_Hafeedh.git)
- Deploy handoff: [docs/codex-production-handoff.md](C:/Users/abdul/Desktop/HR%20Hunter/HR%20Hunter%20Clone/docs/codex-production-handoff.md)

## Current Release Reality

This repo is now cut for a **client-safe transformer-first release**.

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

This release does **not** claim equal strength across every job family.

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

## Today’s Release Notes

This release branch includes the following practical changes:

- transformer-first search path committed into the main repo
- taxonomy and transformer query profile files added
- transformer verifier and telemetry wiring added
- candidate name/company sanitation improved in UI and CSV export
- CSV download behavior fixed
- feedback page wording cleaned up for client readability
- deployment handoff updated for GitHub-to-GCP release flow

## Notes

- Do not present this release as universal coverage for every role family.
- Keep classic fallback internal.
- Use the safe families above for demos and early client positioning.
