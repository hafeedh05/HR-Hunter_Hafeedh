# HR Hunter Team README

Canonical deploy handoff: [docs/codex-production-handoff.md](C:/Users/abdul/Desktop/HR%20Hunter/HR%20Hunter%20Clone/docs/codex-production-handoff.md)

## Current Team Reality

HR Hunter is now a **transformer-first recruiter sourcing app** with a stable recruiter UI shell and a client-safe release cut prepared for deployment from GitHub to GCP.

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

## Operating Rules For The Team

- do not present the product as universal coverage for every role family
- do not expose classic fallback as a normal user-facing mode
- do not redesign the UI before this deploy
- do not redesign the Hunt Brief before this deploy
- do not start a large verifier rewrite on the release branch

## Practical Team Workflow

1. make changes in the repo
2. validate locally
3. push to GitHub
4. deploy from GitHub to GCP
5. smoke test on a safe family
6. roll back fast if health or search behavior is broken

## Validation Baseline

Before deploy, the minimum checks are:

- app starts
- login works
- Hunt loads cleanly
- search starts
- progress updates
- results attach to the correct project
- CSV export downloads correctly
- Candidates tab shows clean names and companies

## Reference

- deploy handoff: `docs/codex-production-handoff.md`
- repo: `https://github.com/hafeedh05/HR-Hunter_Hafeedh.git`
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
