# HR Hunter

PDL-first executive search retrieval with a scraper fallback and a hard QC pass.

This project is built for one thing: finding candidate lists that are fast, explainable, and good enough for retained search workflows instead of dumping a pile of low-signal leads.

## What It Does

- Ingests a `.docx` brief plus structured overrides.
- Compiles the mandate into search slices instead of firing one giant sloppy query.
- Runs provider adapters in order of trust:
  - `pdl`
  - `scrapingbee_google`
  - `mock`
- Scores and verifies every profile against title, company, geography, sector, and seniority.
- Runs a second-pass public-web verifier to corroborate title, company, and location with non-LinkedIn evidence.
- Exports a ranked JSON report plus CSV.
- Supports dry runs so query quality can be reviewed before spending credits.

## Current Status

Working now:

- DOCX ingestion
- Search-brief loading from YAML
- PDL query planning and request execution
- ScrapingBee Google result harvesting
- Heuristic scoring and verification
- Public evidence collection and corroboration
- JSON and CSV exports
- Dry-run mode with compiled queries

Assumed, not live-verified in this repo yet:

- `PDL_API_KEY` or `PEOPLEDATALABS_API_KEY`
- `SCRAPINGBEE_API_KEY`

Blocked right now:

- No provider credentials were present in the local environment, so live API execution could not be verified in this run.

## Install

```bash
uv sync --extra dev
```

If you want the optional API surface:

```bash
uv sync --extra dev --extra api
```

## Environment

Create a local `.env` file from `.env.example`.

```bash
cp .env.example .env
```

Supported variables:

- `PDL_API_KEY`
- `PEOPLEDATALABS_API_KEY`
- `SCRAPINGBEE_API_KEY`
- `HR_HUNTER_OUTPUT_DIR`
- `HR_HUNTER_SECRET_ENV_FILES`

If the key lives in a runtime env file on a VM instead of a local `.env`, set:

```bash
export HR_HUNTER_SECRET_ENV_FILES=/etc/reap/reap.env
```

The loader parses plain `KEY=value` files directly, so it can read runtime env files that are not shell-safe to `source`.

## Run

Dry run against the supplied brief:

```bash
uv run hr-hunter search \
  --brief examples/search_briefs/sr_product_lead_ai_jan26.yaml \
  --dry-run
```

Live run with PDL first and ScrapingBee fallback:

```bash
uv run hr-hunter search \
  --brief examples/search_briefs/sr_product_lead_ai_jan26.yaml \
  --providers pdl,scrapingbee_google \
  --limit 150
```

Live run with automated public-web verification of the top 50 candidates:

```bash
uv run hr-hunter search \
  --brief examples/search_briefs/sr_product_lead_ai_jan26.yaml \
  --providers scrapingbee_google \
  --limit 150 \
  --verify-top 50
```

Verify an existing report after retrieval:

```bash
uv run hr-hunter verify \
  --brief examples/search_briefs/sr_product_lead_ai_jan26.yaml \
  --report output/search/<run_id>.json \
  --limit 50
```

The command writes:

- `output/search/<run_id>.json`
- `output/search/<run_id>.csv`

## Search Strategy

This is intentionally not a single-query toy.

1. Parse the mandate.
2. Break the target company list into slices.
3. Run high-precision current-company queries first.
4. Run broader title-family queries second.
5. Merge and dedupe.
6. Verify by title fit, company fit, distance to search center, sector keywords, and years of experience.
7. Optionally run a public-evidence pass that searches non-LinkedIn web sources for corroboration.
8. Label each profile:
   - `verified`
   - `review`
   - `reject`

## Why This Shape

PDL is the best structured retrieval layer in the stack you described, but it still needs discipline:

- default rate limit is 10 requests per minute according to the official Person Search API reference
- billing is per record returned
- Preview Search exists, so you can inspect coverage before pulling full result sets

ScrapingBee is useful as a fallback for public-web recovery because its Google API supports fast "light requests" that are cheaper and faster, but it is still web search, not a first-class people graph. It should fill gaps, not pretend to replace PDL.

## Example Brief

The included sample brief is based on:

- role: Global Product Manager, Adult Incontinence
- location: Ireland, within 60 miles of Drogheda
- source companies: P&G, Unilever, J&J, Colgate-Palmolive, Beiersdorf, DCC, McBride, Trona, Kinvara, Voya, The Handmade Soap Company, Pestle & Mortar, Bellamianta, Human+Kind
- target titles:
  - Global Product Manager
  - Senior Product Manager
  - Product Marketing Director
  - Head of Product Marketing
  - Global Product Director
  - Product Portfolio Manager
  - Senior Product Marketing Manager
  - Category Manager

## Notes

- This version is retrieval-first. It does not waste money on LLM adjudication in the hot path.
- The second-pass verifier is code-first. If you add an LLM later, use it after evidence collection, not instead of evidence collection.
- If you want a service wrapper, the engine is already structured for it. The CLI is just the fastest operational surface.
