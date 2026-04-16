# Client-Ready Live Validation — 2026-04-16

This note records the production validation for the client-ready transformer-first HR Hunter cut.

## Live Deployment

- Live app: `https://hr-hunter.hyvelabs.tech`
- Final release path: `/srv/hr-hunter/releases/20260416T053644Z-client-ready-v3`
- Previous release path: `/srv/hr-hunter/releases/20260416T053015Z-client-ready-v2`
- Health endpoint: `https://hr-hunter.hyvelabs.tech/healthz`
- Health result: `{"status":"ok"}`
- Frontend asset version: `20260416clientready2`
- Active backend: `transformer_v2`

## What Changed In This Client-Ready Pass

- Preserved `transformer_v2` as the canonical search backend.
- Improved public-evidence extraction for regional profile hosts such as `ae.linkedin.com`.
- Improved company/name sanitation so malformed company fragments are less likely to be treated as strong verified evidence.
- Added runtime truth fields to saved reports:
  - `runtime_seconds`
  - `wall_clock_seconds`
  - `job_elapsed_seconds`
  - `pipeline_elapsed_seconds`
  - `target_runtime_seconds`
  - `runtime_display_source`
- Restored the product runtime target baseline:
  - `300 candidates -> 900s`
  - `200 candidates -> 600s`
  - `100 candidates -> 300s`
  - `60 candidates -> 180s`
  - `50 candidates -> 150s`
- Updated Results UI fallback timing so saved reports still display truthful runtime when the completed job object is not present.
- Bumped frontend asset cache keys so browsers fetch the latest JS/CSS.

## Live Smoke Checks

- `/healthz` works externally.
- Admin session API works.
- Project list loads.
- The five validation projects are visible.
- Supply Chain latest run attaches to `project_6f9ec43faae9`.
- Results report loads for the latest Supply Chain run.
- CSV export returns a real CSV with candidate rows.
- Progress during the fresh Supply Chain run moved through:
  - `query_planning`
  - `queries_planned`
  - `retrieval_running`
  - `entity_resolution`
  - `scoring`
  - `completed`
- Progress target runtime showed `900s` for the 300-candidate run.

Browser automation note: Playwright MCP was blocked in this local desktop environment by a read-only `/.playwright-mcp` path, so the final smoke was completed through live HTTP/API checks plus server logs instead of an interactive browser run.

## Fresh Live Benchmark

### UAE Supply Chain Manager

- project id: `project_6f9ec43faae9`
- run id: `supply-chain-manager-e424bd18`
- backend: `transformer_v2`
- requested: `300`
- returned: `300`
- verified: `212`
- needs review: `88`
- rejected: `0`
- query count: `73`
- raw found: `889`
- unique after dedupe: `434`
- job elapsed: `186s`
- saved report runtime: `182s`
- transformer pipeline elapsed: `37s`
- target runtime: `900s`
- CSV export: passed
- diagnostics: healthy yield; remaining medium issue is weak company/industry signals for `125` candidates.

Top quality notes from the first page:

- `Hammad Ul Haq`, Supply Planning Manager, Haleon — verified.
- `Khalid Abdulrahman`, Demand Planning Manager, ENOC — verified.
- `Shams Alhusseini`, Supply Planning Manager, Ecolab — verified.
- `Ahmad Maarouf`, Supply Chain Manager, Unilever — verified.
- `Reshma Shaikh`, Supply Chain Manager, Wipro Consumer Care And Lighting — verified.
- `Simon Rose`, Supply Chain Manager, Unilever — verified.
- `Abdelkrim Benosman`, Supply Planning Manager, NESPRESSO — verified.
- `Mohamed Abdalla`, Supply Chain Manager, Emitac Healthcare Solutions — verified.
- `Fawzy Fagbemi`, Supply Chain Manager, Al Islami Foods — verified.
- `Shubham Mukhopadhyay`, Supply Chain Manager, Kwality Global — verified.

### Project Architect

- project id: `project_340170c8a1d0`
- run id: `project-architect-07ac2f33`
- backend: `transformer_v2`
- requested: `300`
- returned: `300`
- verified: `259`
- needs review: `41`
- rejected: `0`
- query count: `135`
- raw found: `2242`
- unique after dedupe: `1272`
- job elapsed: `330s`
- diagnostics: healthy yield.

This run was completed before the final runtime-target metadata polish. The quality path is the same; the final v3 release only changed the asset cache key after that.

## Current Honest Product Position

Safe to demo now:

- Supply Chain / Logistics
- Project Architect / Architecture
- Interior Design
- Digital Marketing
- Senior Accountant / Accounting with validation caveat

Do not oversell yet:

- CEO / executive search
- AI / technical search when the client expects high strict-verified yield
- Healthcare / clinical
- Government / public sector
- Aviation / maritime

Latest saved CEO run is still weak:

- returned: `300`
- verified: `16`
- needs review: `284`
- rejected: `0`
- main diagnostic: weak company / industry signals for a narrow executive brief.

## Rollback

If the current release misbehaves:

```bash
sudo ln -sfn /srv/hr-hunter/releases/20260416T053015Z-client-ready-v2 /srv/hr-hunter/current
sudo systemctl restart hr-hunter
curl -fsS http://127.0.0.1:8765/healthz
```

The older quality-pass release is also available:

```bash
sudo ln -sfn /srv/hr-hunter/releases/20260416T051649Z-client-ready /srv/hr-hunter/current
sudo systemctl restart hr-hunter
curl -fsS http://127.0.0.1:8765/healthz
```
