# HR Hunter Team README

## Purpose

HR Hunter is the internal talent-sourcing product we are building to help our team find, grade, review, and operationalize candidate search workflows.

The goal is not to build another generic lead scraper. The goal is to build a controlled internal system that:

- takes structured recruiter input
- runs targeted candidate discovery scripts
- uses open-source AI agents to plan and refine searches
- verifies candidates with public evidence
- grades them consistently
- shows the results in a reviewable UI
- supports a clean local -> PR -> validate -> deploy to GCP workflow

## Product Direction

We are building an internal HR Hunter application that should accept the following recruiter inputs:

- `Country / countries`
  - multiple countries can be selected
  - optional city or cities inside those countries
  - optional radius around a selected city
- `Preferred job title / job titles`
  - multiple titles can be entered
- `Preferred company / companies`
  - multiple companies can be entered
- `Job description upload`
  - recruiter uploads the JD
  - the system parses it and uses it to refine search logic, role fit, required signals, and grading

The current working example in this repo is FMCG-heavy and Ireland-focused, but the product should be reusable for other mandates and other countries as well.

## Product Goals

Primary goals:

- help recruiters produce better candidate lists faster
- make search logic transparent instead of black-box
- preserve search history and reviewer decisions
- prevent the same candidate from being surfaced repeatedly unless the user wants that
- keep verification strong enough that approved candidates are defensible

Non-goals for the first versions:

- public self-serve signup
- consumer-grade social product features
- password-based auth
- one-click "AI decides everything" candidate approval
- replacing human review entirely

## Core Search Anchors

The search system should be driven by explicit anchors selected by the user after they enter the brief.

Anchors are the highest-priority filters and scoring signals. Examples:

- location
- current company
- target company history
- current title family
- relevant experience family
- sector / industry
- seniority

For the current FMCG search shape, the main anchors are:

- Ireland location
- relevant FMCG experience
- current or target-company alignment
- relevant title family

Users should be able to mark anchors as:

- `critical`
- `important`
- `nice to have`

This matters because the search logic and grading should behave differently depending on what is locked in. A mandate where location is critical should not behave the same way as a mandate where location is only preferred.

## Search Philosophy

We want this built around scripts and open-source AI agents, not a single opaque black box.

That means:

- use scripts to run controlled search lanes
- use open-source AI agents to help with planning, parsing, evidence collection, and operator workflows
- keep the evidence and grading logic inspectable
- keep a hard verification layer so agent output does not become truth on its own

The system should support multiple search lanes in parallel. Examples:

- `current-target-company-strict`
  - candidates who currently work at the preferred companies and currently hold relevant roles
- `current-target-company-adjacent`
  - candidates at the preferred companies whose current roles are adjacent but still relevant
- `history-target-company-relevant-role`
  - candidates who previously worked at target companies but currently remain relevant for the mandate
- `location-first-fmcg`
  - candidates strongly anchored on the location and sector, even if the company match is broader
- `location-recovery`
  - candidates with strong role/company fit where location evidence needs to be recovered from public sources

Each lane should:

- use different query logic
- avoid re-searching the same candidates
- avoid reusing equivalent provider queries
- write explainable outputs
- feed a single deduped review surface

## Recommended Script Lanes

The implementation should explicitly support separate scripts or strategy lanes, each with a clear purpose.

Suggested lanes:

- `current_target_company_exact`
  - current company is in the preferred list
  - current role is an exact or near-exact role match
- `current_target_company_adjacent`
  - current company is in the preferred list
  - current role is adjacent but still relevant
- `current_target_company_location_first`
  - location anchor is strict
  - company still matters, but the lane prioritizes Ireland evidence first
- `historical_target_company_recovery`
  - previous target-company experience is present
  - current role still relevant
  - feeds mostly into `Needs Review`, not direct approval
- `location_recovery_probe`
  - strong role/company fit but weak location evidence
  - tries to recover Ireland or city-level evidence
- `role_recovery_probe`
  - strong company/location fit but weak or noisy title evidence
  - tries to confirm the actual current remit

Each lane should have:

- a query cap
- a verification cap
- dedupe against prior runs
- its own audit label so we know where the candidate came from

## Public-Only Search Model

Right now the public-web workflow uses ScrapingBee as a search layer and public evidence recovery layer.

We need to create search scripts that use ScrapingBee intelligently, not expensively and not randomly.

That means:

- high-precision query slices first
- broader recovery queries second
- evidence fetches only for top-ranked candidates
- dedupe before verification
- avoid re-querying previously seen candidates and previously seen search fingerprints

ScrapingBee is the search infrastructure, not the product logic. The value must live in our scripts, scoring, verification, and workflow design.

## Grading Schema

The grading schema is fixed:

- `70.00 - 100.00` = `Verified`
- `50.00 - 69.99` = `Needs Review`
- `0.00 - 49.99` = `Rejected`

The UI and exports must always respect those bands.

Alongside strict grading, we also use a second reporting layer in the underlying engine:

- `strict_verified`
- `search_qualified`
- `weak`

This helps us separate:

- what is safe to approve immediately
- what is promising but still needs a human
- what should be dropped

## Scoring Dimensions

The team should assume that scoring is multi-factor and should stay multi-factor.

Core scoring dimensions:

- `experience_fit`
- `company_fit`
- `title_fit`
- `industry_fit`
- `location_fit`
- `skill_fit`
- `verification_fit`
- `company_size_fit`

Additional precision dimensions already aligned with the current direction:

- `current_function_fit`
- `location_precision`
- `current_fmcg_fit`
- `source_quality`
- `evidence_freshness`

Operational fields that should be exposed in outputs:

- `qualification_tier`
- `cap_reasons`
- `disqualifier_reasons`
- `matched_title_family`
- `location_precision_bucket`
- `current_role_proof_count`

The point is not just to generate a number. The point is to explain why a number was earned and why a candidate was capped or downgraded.

## How Verification Should Work

The system should not approve candidates just because a search result looks good.

The verification layer must check:

- does the person currently appear to work there
- is the current role actually relevant
- is the location real and current
- is the sector / FMCG fit real
- is the title family a real match or just a noisy nearby title

Important rule:

- current role proof matters more than historical experience

Examples of what should push a candidate down:

- role is historical, not current
- company is historical, not current
- country-only signal with no usable locality when locality matters
- off-function current role
- tech PM or unrelated product title when the search is for FMCG brand / product / category work

Verification should be layered:

1. retrieval
2. local dedupe
3. cheap local scoring
4. targeted public evidence collection for the strongest candidates
5. current-role / current-company / location proof checks
6. final status assignment

Agents may help summarize evidence, but agents should not directly decide approval without the hard rule checks.

## UI Requirements

We want a UI for internal team use only.

Requirements:

- access is invite-only
- we create the account for the user
- there is no password
- login uses Google Authenticator-compatible OTP / TOTP
- only allowlisted users that we provision can get in

The safest implementation shape is:

- invite-only account provisioning by us
- no public signup
- no local password login
- Google Authenticator or another TOTP-compatible authenticator app for sign-in / MFA
- a user identity model based on invited email addresses plus TOTP enrollment

Important clarification:

- this should not default to public Google OAuth sign-in
- the requirement is private, invite-only, passwordless access with Google Authenticator-style verification

The UI should support:

- form inputs for countries, cities, radius, titles, companies, JD upload
- anchor selection after the brief is entered
- a run / re-run workflow
- candidate tables with clear rationale
- human review actions
- historical memory so we do not lose prior work for the same mandate

Recommended screens:

- `Login`
- `Mandates`
- `Create Mandate`
- `Run Search`
- `Candidates`
- `Candidate Detail`
- `History / Prior Runs`
- `Admin / Access Management`

## Candidate Review Surface

The UI should display three table states, effectively like three sheets:

- `Approved`
- `Needs Review`
- `Rejected`

Each candidate row should show at least:

- candidate name
- current title
- current company
- location
- score
- strict status
- qualification tier
- why they matched
- what anchors they matched
- what evidence was used
- whether the role/company/location are current or only historical
- notes from the verifier
- reviewer notes

For `Needs Review`, the user should be able to:

- approve
- reject
- hold
- add notes
- assign ownership

The review state should then feed the relevant final table without removing the audit trail.

Recommended row fields for the UI table:

- candidate name
- current title
- current company
- current location
- score
- strict status
- qualification tier
- anchor matches
- why approved / why review / why rejected
- evidence summary
- source links
- cap reasons
- disqualifier reasons
- review owner
- review notes
- last updated time

Recommended candidate detail drawer or page:

- profile summary
- current-role evidence
- current-company evidence
- location evidence
- historical experience
- scorer notes
- verifier notes
- raw source links
- reviewer actions and audit log

## Google Sheets / Table Behavior

Even once the full UI exists, the Google Sheet workflow is still useful as an ops surface.

The expected behavior is:

- `Approved` shows final positive candidates
- `Needs Review` shows candidates waiting on human action
- `Rejected` shows dropped candidates
- human approval or rejection from `Needs Review` should feed the final destination state
- audit history should be preserved
- duplicates should be prevented across active tables

Important:

- the system should not erase historical candidate records for the same mandate
- it should preserve reviewer decisions
- it should maintain dedupe across runs and across sheet syncs

## Candidate Lifecycle

Expected lifecycle:

1. user creates a mandate
2. user enters countries, cities, radius, titles, companies, JD
3. user selects anchors and priority levels
4. system compiles search lanes
5. search scripts execute
6. results dedupe into a candidate registry
7. scoring runs
8. top candidates get evidence collection
9. candidates land in `Approved`, `Needs Review`, or `Rejected`
10. human reviewers adjust the final shortlist
11. results and decisions stay attached to the mandate history

## Candidate Registry And Memory

We should preserve an internal candidate registry so the system can remember:

- candidate identity
- previous searches where the candidate appeared
- previous scores
- previous reviewer decisions
- prior source URLs
- prior location / company / title evidence

This is important because:

- we do not want to burn budget rediscovering the same people blindly
- we do not want to lose shortlist history
- we do want to reuse prior evidence when it is still valid

## Proposed Data Model

At a minimum, the application will likely need records for:

- `users`
- `allowed_users`
- `mandates`
- `search_inputs`
- `search_runs`
- `search_strategies`
- `candidate_registry`
- `candidate_run_results`
- `candidate_evidence`
- `review_decisions`
- `audit_events`

We should store enough information to answer:

- where did this candidate come from
- why was this candidate scored this way
- who approved or rejected this candidate
- what evidence existed at the time
- when was the candidate last re-verified

## What “Smart Search” Means Here

We should not rely on one query or one prompt.

Smart search in this system means:

- break the brief into multiple search lanes
- search current employer matches before broader title-family searches
- use adjacent title families carefully
- expand only within the mandate's actual logic
- recover missing evidence with follow-up probes
- dedupe across all runs
- retain candidate history across searches

For example, if the recruiter asks for FMCG product or brand leadership in Ireland, the system should search nearby role families such as:

- brand manager
- senior brand manager
- product marketing director
- category manager
- category development manager
- shopper marketing manager
- trade marketing manager
- customer marketing manager
- portfolio manager
- innovation manager
- commercialization roles

It should not drift into generic tech product management just because the word `product` exists.

## Current Repo Scope

The current repository already contains:

- a CLI search flow
- a public-web search path
- a matrix search workflow for multiple search strategies
- scoring and verification logic
- Google Sheets sync support
- a local GCP MCP for working with the VM
- scripts for remote search execution

The next major product step is to turn the current workflow into a usable internal UI while keeping the verification and deployment discipline intact.

## Open-Source AI Agent Role

We explicitly want to do this with scripts and open-source AI agents.

Suggested agent responsibilities:

- parse uploaded job descriptions into structured search hints
- propose title families and adjacent role families
- propose search query variants
- summarize evidence collected by the scraper
- draft reviewer notes
- help classify whether missing evidence should trigger a recovery probe

Agents should not be trusted to:

- bypass hard scoring thresholds
- invent current employment proof
- overwrite reviewer decisions
- silently promote weak candidates

## Local Development Workflow

### 1. Clone and branch

```bash
git checkout main
git pull origin main
git checkout -b codex/<feature-name>
```

### 2. Install dependencies

Recommended:

```bash
uv sync --extra dev --extra api --extra mcp --extra sheets
```

If `uv` is not available:

```bash
python3 -m venv .venv
.venv/bin/python -m pip install --upgrade pip setuptools wheel
.venv/bin/python -m pip install -e ".[dev,api,mcp,sheets]"
```

### 3. Create local env

```bash
cp .env.example .env
```

Expected variables include:

- `SCRAPINGBEE_API_KEY`
- `PDL_API_KEY` or `PEOPLEDATALABS_API_KEY`
- `HR_HUNTER_OUTPUT_DIR`
- `HR_HUNTER_SECRET_ENV_FILES`

### 4. Run local checks

Run tests:

```bash
uv run pytest tests
```

Run a dry-run search:

```bash
uv run hr-hunter search \
  --brief examples/search_briefs/sr_product_lead_ai_jan26.yaml \
  --dry-run
```

Run a matrix dry-run:

```bash
uv run hr-hunter matrix-search \
  --matrix examples/matrices/sr_product_lead_ai_jan26_ireland_fmcg.yaml \
  --dry-run
```

Optional local API:

```bash
uv run uvicorn hr_hunter.api:create_app --factory --reload
```

## GitHub Workflow

Every meaningful change should follow this path:

1. create a branch
2. implement the change
3. run tests locally
4. run at least one smoke check relevant to the change
5. push the branch
6. open a PR
7. validate the PR
8. merge to `main`
9. test the merged result
10. deploy to GCP

Minimum PR checklist:

- tests pass
- docs updated if behavior changed
- no obvious regressions in scoring or verification
- if search logic changed, run a capped smoke test
- if Sheets logic changed, verify row placement and dedupe behavior
- if deployment logic changed, verify the GCP path still works

Suggested PR template content:

- summary of what changed
- product behavior impact
- test evidence
- smoke test evidence
- rollout notes
- GCP impact

## GCP Workflow

We deploy and test on our existing GCP VM after local validation.

Authenticate locally:

```bash
gcloud auth login
gcloud auth application-default login
gcloud config set project <PROJECT_ID>
```

Optional:

```bash
gcloud config set compute/zone <ZONE>
```

If using the local GCP MCP from this repo:

```bash
uv sync --extra mcp
./scripts/install_codex_global_mcp.sh
```

To run the remote search helper directly on the VM:

```bash
./scripts/run_remote_search.sh <brief-path> [limit] [verify-top]
```

The VM deployment path writes a runnable wrapper under:

```bash
~/deployments/<workspace-name>/run-hr-hunter.sh
```

Recommended release path:

1. local development
2. local validation
3. GitHub PR
4. PR review and merge
5. merged smoke test
6. deploy to GCP VM
7. run capped production-like test
8. inspect outputs and Sheets sync

## Secrets And Security

Expected secret handling:

- local development: `.env`
- shared cloud runtime: GCP Secret Manager or controlled runtime env files
- VM runtime: mounted or managed env file

Do not:

- commit secrets
- paste secrets into docs
- hardcode provider keys into scripts
- bypass access controls for convenience

## Team Working Rules

- do not hardcode secrets in code or docs
- use GCP Secret Manager or runtime env files for API keys
- do not remove historical candidate data for the same mandate unless there is a deliberate cleanup decision
- do not overwrite reviewer actions silently
- do not trust agent output without evidence
- do not promote candidates whose current company, current role, or location is not defensible

## Recommended Build Priorities

1. internal auth model
   - invite-only
   - Google Authenticator-based access
   - no passwords
2. brief intake UI
   - countries, cities, radius, titles, companies, JD upload, anchor selection
3. run orchestration
   - scripted search lanes
   - agent-assisted planning
   - dedupe and verification
4. review UI
   - Approved / Needs Review / Rejected
   - reviewer actions and audit trail
5. deployment pipeline
   - local validation
   - PR checks
   - merge to main
   - deploy to GCP

## Suggested MVP Acceptance Criteria

We should consider the first real internal version successful if it can:

- create a mandate from structured recruiter input
- parse an uploaded JD
- let the user define anchors
- run multiple search scripts for one mandate
- dedupe candidates across lanes and across history
- verify current company / role / location with public evidence
- grade into `Approved`, `Needs Review`, and `Rejected`
- allow human review actions
- preserve reviewer history
- ship through the local -> PR -> validate -> GCP workflow cleanly

## Bottom Line

We are building an internal, script-driven, AI-assisted HR Hunter product that:

- accepts a structured talent brief
- searches intelligently with public-web tooling like ScrapingBee
- uses open-source AI agents where they help
- keeps hard verification and grading in place
- gives the team a clear review UI
- and follows a disciplined local -> PR -> validate -> merge -> GCP workflow

The standard we want is simple:

- fast to run
- hard to fool
- easy to review
- safe to deploy
