# HR Hunter Production Handoff

This is the clean pickup doc for the next Codex or operator working on the live HR Hunter stack.

## Start Here

1. Use GitHub `main` as the source of truth.
2. Read this file before touching prod.
3. Inspect prod read-only first.
4. Create a rollback path before changing anything.
5. Benchmark honestly before claiming improvement.

Do not use Abdul's local machine as the source of truth.

## Current Live State

- Repo: `https://github.com/hafeedh05/HR-Hunter_Hafeedh`
- Live domain: `https://hr-hunter.hyvelabs.tech`
- Health endpoint: `https://hr-hunter.hyvelabs.tech/healthz`
- Deployed commit: `f1afe6881c94ca104ded58e9a49938f08becb8cb`
- Active release path: `/srv/hr-hunter/releases/20260412T170922Z-f1afe68`
- Previous release path: `/srv/hr-hunter/releases/20260412T162229Z-2dad9ff`
- Backup path for current deploy: `/srv/hr-hunter/backups/20260412T170922Z-pre-f1afe68`

GitHub `main` and `codex/prod-handoff-fix-20260410` both point to `f1afe68`.

## Production Topology

- Reverse proxy: Caddy
- Caddy config: `/etc/caddy/Caddyfile`
- App service: `hr-hunter.service`
- Service file: `/etc/systemd/system/hr-hunter.service`
- App bind: `127.0.0.1:8765`
- Health path on app: `/healthz`
- VM shape observed during takeover:
  - `2 vCPU`
  - `~2 GB RAM`

## Important Runtime Note

The app config surfaced a SQLite state path during inspection, but the live process environment was using Postgres. Treat storage configuration carefully and verify the active runtime path before making assumptions about checkpointing or state migration.

## What The Latest Patch Changed

Commit `f1afe68` tightened product truthfulness and parser quality across roles:

- telemetry no longer shows fake verified/review/reject movement during retrieval
- verification counters now reflect only candidates actually checked so far
- exec title matching now handles:
  - `CEO`
  - `Chief Executive Officer`
  - `Managing Director`
  - `President`
  - `Group CEO`
  - `Country CEO`
  - `General Manager`
- obvious company-page / non-person parser junk is penalized harder
- frontend JS asset was cache-busted so the live UI actually picks up the patch

Files changed in `f1afe68`:

- `UI/index.html`
- `src/hr_hunter/api.py`
- `src/hr_hunter/features.py`
- `src/hr_hunter/output.py`
- `src/hr_hunter/verifier.py`
- `tests/test_output.py`
- `tests/test_scoring.py`
- `tests/test_verifier.py`

Local validation on the deployment worktree:

- `143 passed, 1 skipped`

## Last Honest Benchmark Set

These were the last full live benchmarks taken before the telemetry/parser patch. They are still the correct baseline for current product quality.

### Marina Home CEO / 300

- runtime: `735s`
- queries: `36/36`
- raw found: `364`
- unique after dedupe: `364`
- reranked: `180`
- finalized: `57`
- verified: `2`
- review: `0`
- rejected: `55`
- in scope: `2`
- precise in scope: `2`
- diagnostics:
  - `market_scarcity`
  - `title_mismatch`
  - `filters_too_loose`

Blunt read: the market did not honestly support `100+ verified` on this strict brief with public-web evidence. The top 20 still showed parser/company-page junk before the latest patch, and the true in-scope executive pool was tiny.

### Digital Marketing Manager / Dubai / 100

- runtime: `481s`
- queries: `38/38`
- raw found: `243`
- unique after dedupe: `243`
- reranked: `200`
- finalized: `100`
- verified: `3`
- review: `18`
- rejected: `79`
- in scope: `47`
- precise in scope: `44`
- diagnostics:
  - `title_mismatch`
  - `weak_company_or_industry_signals`
  - `filters_too_loose`

Blunt read: top-of-list quality was materially better than overall verified yield. The system was finding same-title / same-market candidates, but company parsing and evidence quality were still dragging verification down.

### Data Analyst / UAE / 80

- runtime: `544s`
- queries: `34/34`
- raw found: `242`
- unique after dedupe: `242`
- reranked: `160`
- finalized: `80`
- verified: `13`
- review: `10`
- rejected: `57`
- in scope: `28`
- precise in scope: `22`
- diagnostics:
  - `title_mismatch`
  - `filters_too_loose`

Blunt read: this role shape behaved much better than the CEO case and is a healthier benchmark for common-role product quality.

## What Is Actually Working

- prod is healthy
- GitHub and prod are aligned
- background jobs run and persist
- progress polling is live via `/app/jobs/{job_id}`
- in-scope reporting exists in the UI and API
- common-role shortlist quality is better than earlier builds
- the final telemetry patch was smoke-tested live after deploy

## What Is Still Not Good Enough

- CEO strict verified yield is still low
- public-web evidence is the bottleneck for strict executive verification
- company/page parsing still needs continued cleanup
- storage/checkpoint runtime needs a cleaner single-source explanation
- the product still needs stronger scope-first orchestration and top-N in-scope verification as the default operating model

## Recommended Next Moves

1. Make `In Scope` the operating target, not just a reporting counter.
   - fill same-title + same-market candidates first
   - stop broadening once the in-scope pool is full
   - widen only when in-scope growth stalls

2. Verify the strongest in-scope tranche first.
   - do not burn verification budget on obvious off-scope rejects
   - keep `In Scope`, `Verifying`, `Verified`, `Needs Review`, and `Rejected` separate and truthful

3. Split company intent in the brief.
   - `Must currently work at these companies`
   - `Peer companies to source from`

4. Keep improving parser and evidence quality.
   - harder person-page filtering
   - stronger current-company extraction
   - contradiction detection across sources
   - heavier weighting for official leadership pages, LinkedIn, conference bios, and credible appointment pages

## Safe Pickup Flow For The Next Codex

1. Pull GitHub `main`.
2. Read this file.
3. Inspect prod read-only:
   - `systemctl show hr-hunter`
   - `/etc/systemd/system/hr-hunter.service`
   - `/etc/caddy/Caddyfile`
   - active release symlink/path
   - health endpoint
   - recent job logs
4. Confirm rollback path exists before any mutation.
5. Run one honest benchmark before changing logic.
6. Ship only after:
   - live health is green
   - prod smoke is verified
   - GitHub `main` matches deployed commit

## Rollback

To roll back from `f1afe68`:

1. point `hr-hunter.service` back to `/srv/hr-hunter/releases/20260412T162229Z-2dad9ff`
2. run:

```bash
sudo systemctl daemon-reload
sudo systemctl restart hr-hunter
```

If needed, restore from `/srv/hr-hunter/backups/20260412T170922Z-pre-f1afe68`.
