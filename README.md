# HR Hunter

HR Hunter is a recruiter-focused search and ranking platform for building project-based candidate pipelines. It combines structured hunt briefs, public-web sourcing, semantic ranking, recruiter feedback, and a full local web app into one workflow.

This repository now includes:

- a FastAPI backend
- a full recruiter UI
- TOTP-based sign-in
- project and run history
- background search jobs
- candidate review and feedback capture
- optional semantic reranking
- optional learned ranking from recruiter feedback
- local SQLite support and a Postgres/Cloud SQL-ready database path

## Product Overview

HR Hunter is built around a project-first workflow:

1. Sign in with a recruiter account using a 6-digit authenticator code.
2. Create or open a project for a role or mandate.
3. Build the hunt brief with titles, years, location, companies, employment status, JD, and anchors.
4. Run search in the background without freezing the UI.
5. Review candidates, feedback, and run history inside the same project.
6. Log recruiter feedback and train a ranking model from it.

The app supports multiple recruiters, admin-only controls, recruiter account provisioning, and project history across runs.

## Core Features

- Project-based recruiter workspace
- TOTP-only login flow
- Admin-managed recruiter accounts
- Hunt brief builder for:
  - target titles
  - years and tolerance
  - countries, continents, cities, and radius
  - target companies and company match mode
  - employment status mode
  - must-have and nice-to-have keywords
  - typed job descriptions and recruiter notes
  - JD uploads from `pdf`, `doc`, `docx`, `txt`, `md`, and `rtf`
  - automatic structured JD breakdown from uploaded files
  - manual JD breakdown from typed descriptions
  - ranking anchors
- Separate tabs for:
  - Projects
  - Hunt
  - Results
  - Candidates
  - Feedback
  - History
  - Settings
- Background search jobs with retry and failure handling
- Candidate registry and cross-search memory
- Recruiter feedback logging
- Learned ranking with LightGBM
- Semantic reranking support
- CSV and JSON report exports
- Optional remote sourcing bridge
- Secret Manager-aware runtime configuration

## Ranking Model

HR Hunter keeps verification labels honest and score-based.

Score bands:

- `70.00 - 100.00` = `Verified`
- `50.00 - 69.99` = `Needs Review`
- `0.00 - 49.99` = `Rejected`

The stack can combine:

- heuristic scoring
- semantic reranking
- learned ranking from recruiter feedback

The semantic reranker is optional and currently uses:

- `BAAI/bge-reranker-v2-m3`

The learned ranker is optional and currently uses:

- `LightGBM LambdaRank`

## Tech Stack

- Python 3.10+
- FastAPI
- Uvicorn
- SQLite for local state and feedback by default
- Postgres-compatible database layer for Cloud SQL readiness
- Hugging Face Transformers for semantic reranking
- LightGBM + scikit-learn for learned ranking
- Google Cloud Secret Manager support

## Install

Base install:

```bash
uv sync --extra dev
```

If you want the local web app and API:

```bash
uv sync --extra dev --extra api
```

If you want semantic reranking:

```bash
uv sync --extra reranker
```

If you want learned ranking:

```bash
uv sync --extra ranker
```

If you want all major runtime features:

```bash
uv sync --extra dev --extra api --extra reranker --extra ranker
```

## Run the App

Start the local app:

```bash
uv run hr-hunter serve --host 127.0.0.1 --port 8765
```

Then open:

- `http://127.0.0.1:8765`

JD handling in the app:

- Uploading a JD file automatically runs structured JD breakdown.
- Typed JD text can be broken down with the `Break Down JD` button.
- If both an uploaded file and typed text are present, the uploaded file is used as the primary JD source and the typed text is treated as optional recruiter notes.

## CLI Commands

Run a brief:

```bash
uv run hr-hunter search --brief examples/search_briefs/senior_data_analyst_uae.yaml --providers scrapingbee_google --limit 100
```

Verify an existing report:

```bash
uv run hr-hunter verify --brief examples/search_briefs/senior_data_analyst_uae.yaml --report output/search/<run_id>.json --limit 50
```

Run a search matrix:

```bash
uv run hr-hunter matrix-search --matrix examples/matrices/sr_product_lead_ai_jan26_ireland_fmcg.yaml --limit 180 --verify-top 80
```

Log recruiter feedback:

```bash
uv run hr-hunter feedback-log --report output/search/<run_id>.json --candidate "<candidate name>" --recruiter-id recruiter_1 --action shortlist
```

Export training rows:

```bash
uv run hr-hunter feedback-export --output output/feedback/training_rows.json
```

Train the learned ranker:

```bash
uv run hr-hunter train-ranker --feedback-db output/feedback/hr_hunter_feedback.db --model-dir output/models/ranker/latest
```

## Environment

Create a local `.env` file from `.env.example` if you want local secrets and overrides.

Important runtime variables include:

- `SCRAPINGBEE_API_KEY`
- `HR_HUNTER_OUTPUT_DIR`
- `HR_HUNTER_FEEDBACK_DB`
- `HR_HUNTER_RANKER_MODEL_DIR`
- `HR_HUNTER_STATE_DB`
- `HR_HUNTER_DATABASE_URL`
- `HR_HUNTER_SECRET_ENV_FILES`
- `HR_HUNTER_USE_SECRET_MANAGER`
- `HR_HUNTER_GCP_PROJECT`
- `GOOGLE_CLOUD_PROJECT`
- `SCRAPINGBEE_API_KEY_SECRET_NAME`

## Secret Manager Support

The runtime can load secrets from Google Cloud Secret Manager instead of hardcoding them into the repo or a local `.env`.

Example environment setup:

```bash
export HR_HUNTER_USE_SECRET_MANAGER=true
export GOOGLE_CLOUD_PROJECT=<your-gcp-project>
export SCRAPINGBEE_API_KEY_SECRET_NAME=hr-hunter-scrapingbee-api-key
```

## Database Support

Local development uses SQLite by default for:

- workspace state
- project history
- recruiter accounts
- feedback storage

The codebase now also supports a shared Postgres-compatible connection path through:

- `HR_HUNTER_DATABASE_URL`

That makes the app ready for Cloud SQL-style deployments without forcing local development off SQLite.

## Remote Sourcing Support

HR Hunter can also call a remote sourcing backend server-to-server when configured. The local app can still fall back to local ranking/report handling around that remote response.

Relevant runtime controls live in:

- `src/hr_hunter/remote.py`
- `src/hr_hunter/api.py`

## Outputs

Search runs write recruiter-facing artifacts under the output directory, including:

- JSON report
- CSV export
- feedback DB
- state DB
- learned-ranker model files

Typical output folders:

- `output/search`
- `output/feedback`
- `output/state`
- `output/models/ranker`

## Current Direction

This repository has moved well beyond the earlier retrieval-only workflow. It now behaves as a recruiter product with:

- project memory
- candidate memory
- admin-managed access
- background jobs
- recruiter review loops
- learned ranking

It is designed to keep getting better as recruiter feedback accumulates.

## Notes

- The UI and backend evolve together in this repo.
- Verification labels are not faked to satisfy quotas.
- Recruiter feedback improves ranking quality over time, especially once the learned ranker is trained.
- Requested candidate counts are best-effort and depend on source coverage, dedupe, and quality filtering.
