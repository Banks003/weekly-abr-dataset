# Architecture & data flow

> **Scope:** comprehensive register of where data lives, what writes it,
> what reads it, how it grows, and what's between current state and the
> target. Single source of truth for "how it all fits together" and
> "what's left to do." Issues in the GitHub tracker derive from §8 and
> §9 of this document — when an item closes, the row is removed here
> and the issue closed; when a new gap is identified, this doc is
> updated and an issue filed to track the work.
>
> **Last reconciled:** 2026-05-08, against the live R2 bucket inventory
> (#17) and the codebase at `claude/investigate-iceberg-duplicates-So7zi`
> (commits `058018e`, `499c32b`).

## 1. Component map

```
                          ┌──────────────────────┐
                          │  data.gov.au CKAN    │
                          │  ABR bulk XML extract│
                          └──────────┬───────────┘
                                     │ weekly
                                     ▼
            ┌────────────────────────────────────────┐
            │  GitHub Actions: refresh.yml           │
            │  cron: Sun 04:00 UTC                   │
            │  + workflow_dispatch (manual)          │
            └────────────────┬───────────────────────┘
                             │ runs
                             ▼
            ┌────────────────────────────────────────┐
            │  abr-extract run                       │
            │  src/abr_extract/cli.py                │
            └────┬────────────────┬─────────────────┘
                 │                │
        writes  ▼                ▼ writes
       ┌────────────────┐   ┌────────────────────────┐
       │ Iceberg via    │   │ R2 via boto3 S3 API    │
       │ R2 Data Catalog│   │ (publish.py)           │
       │ REST           │   └────┬───────────────────┘
       └────────┬───────┘        │
                │                │
                ▼                ▼
            ┌──────────────────────────────────────┐
            │  R2 bucket: weekly-abr-dataset       │
            │  ─ __r2_data_catalog/  (iceberg)     │
            │  ─ manifest.json                     │
            │  ─ iceberg-snapshot.json (MISSING)   │
            │  ─ abns/index.html      (frontend)   │
            │  ─ abn-*.parquet, *.sqlite           │
            │       (origin off-repo, see #13)     │
            └────────┬─────────────────────────────┘
                     │ public via gazetteer.au
                     ▼
            ┌─────────────────────────────────┐
            │  Frontend: gazetteer.au         │
            │  ─ abns/index.html              │
            │     (HTML + DuckDB-WASM)        │
            │  ─ reads iceberg-snapshot.json  │
            │     to discover live data files │
            └─────────────────────────────────┘

       ┌────────────────────────────────────────┐
       │ Cloudflare Worker (worker/)            │
       │ ─ /v1/* RESTful routes                 │
       │ ─ currently placeholder query layer    │
       │ ─ deployed via wrangler deploy         │
       └────────────────────────────────────────┘
```

## 2. Storage inventory (R2)

Live inventory captured 2026-05-08 — see #17 for the screenshot trail.

**Bucket:** `weekly-abr-dataset` · Standard storage · public access enabled · **total 11.12 GB**.

### 2.1 Root keys

| Key pattern | Size each | Count | Total | Last write | Producer | Consumer | Status |
|---|---|---:|---:|---|---|---|---|
| `manifest.json` | 1.08 KB | 1 | 1 KB | weekly | `abr-extract run` (`publish.py:plan_uploads`) | Pipeline idempotency; frontend; future API | Live |
| `manifest-{date}.json` | 1.08 KB | 1/wk | 1 KB/wk | weekly | same | Audit trail | Live |
| `iceberg-snapshot.json` | ~ KB | **0** | **0** | **never** | should be `abr-extract run` | Frontend (data file URLs) | **MISSING — #18** |
| `iceberg-snapshot-{date}.json` | ~ KB | **0** | **0** | **never** | same | Audit trail | **MISSING — #18** |
| `abn-main-{date}.parquet` | 772 MB | 1 | 772 MB | 2026-05-07 22:23 | **off-repo** | None known | **Origin unverified — #13** |
| `abn-main-latest.parquet` | 772 MB | 1 | 772 MB | 2026-05-07 22:23 | off-repo | README; consumer-facing | **Origin unverified — #13** |
| `abn-trading-names-{date}.parquet` | 199 MB | 1 | 199 MB | 22:24 | off-repo | None known | **Origin unverified — #13** |
| `abn-trading-names-latest.parquet` | 199 MB | 1 | 199 MB | 22:24 | off-repo | README; consumer-facing | **Origin unverified — #13** |
| `abn-dgr-{date}.parquet` | 1 MB | 1 | 1 MB | 22:24 | off-repo | None known | **Origin unverified — #13** |
| `abn-dgr-latest.parquet` | 1 MB | 1 | 1 MB | 22:24 | off-repo | README; consumer-facing | **Origin unverified — #13** |
| `abr-extract-{date}.sqlite` | 4.25 GB | 1 | 4.25 GB | 22:26 | off-repo | None known | **Origin unverified — #13** |
| `abr-extract-latest.sqlite` | 4.25 GB | 1 | 4.25 GB | 22:27 | off-repo | README; consumer-facing | **Origin unverified — #13** |

### 2.2 `abns/`

| Key | Size | Producer | Consumer |
|---|---:|---|---|
| `abns/index.html` | 26 KB | `scripts/deploy_frontend.ps1` (manual) | `gazetteer.au/abns/...` |
| `robots.txt` | <1 KB | same | bots |

### 2.3 `__r2_data_catalog/` — Iceberg warehouse

R2 Data Catalog managed warehouse. Layout: `<namespace-uuid>/<table-uuid>/{data,metadata}/`. Two namespaces, three tables each.

**Namespace 1 — `abr`** (UUID `019e02fb-f7b0-...`)

| Table (logical) | Snapshots | Data files | Metadata files | Total size |
|---|---:|---|---:|---:|
| `abn_main_history` | 1 | 11 × ~50 MB | 4 (2× metadata.json + 1× m0.avro + 1× snap.avro) | 547 MB |
| `abn_trading_names_history` | 1 | not drilled | not drilled | not drilled |
| `abn_dgr_history` | 1 | not drilled | not drilled | not drilled |

**Namespace 2 — `abr_test`** (UUID `019e030b-7d14-...`) — created by truncated `--max-records` runs, never torn down

| Table (logical) | Snapshots | Data files | Metadata files | Total size |
|---|---:|---|---:|---:|
| `abn_main_history` | 1 | 1 × 1.67 MB | 4 | 1.67 MB |
| `abn_trading_names_history` | 1 | not drilled | not drilled | not drilled |
| `abn_dgr_history` | 1 | not drilled | not drilled | not drilled |

## 3. Data flow per weekly refresh

Wall-clock per stage is **not measured** in this repo today. The new
diagnostic instrumentation (§9 group A item 2) makes per-stage
timing visible in workflow logs from its first run forward; this
section will be updated with measured numbers once that lands.

```
GitHub Actions (cron Sun 04:00 UTC, or manual workflow_dispatch)
   │
   ▼
[1] catalog.fetch_catalog                     network only, no R2 write
   │
   ▼
[2] publish.read_remote_manifest              R2 GET manifest.json
   │  → if extract_time matches, exit early (idempotent)
   ▼
[3] download.download_all                     HTTPS pull of 2 zips
   │  writes ./work/abr_extract_*.zip
   ▼
[4] parallel.parse_zips_parallel              4 workers
   │  writes ./work/shards/part-*.parquet
   │
   ▼
[4b] parallel.merge_shards_to_parquet         pyarrow.concat_tables
   │  writes ./work/abn_main.parquet, trading, dgr
   │  (LOCAL to runner; not uploaded by current pipeline)
   │
   ▼
[5] _run_iceberg_step                         3 tables sequentially
   │  for each in (main, trading, dgr):
   │    if iceberg empty: bootstrap_history_table_arrow
   │    else:             update_history_table_arrow (DuckDB SCD2)
   │    prune_iceberg_snapshots(keep=2)
   │  writes __r2_data_catalog/<ns>/<table>/{data,metadata}/...
   ▼
[6] search_index.build_search_index           DuckDB join over main+trading
   │  writes ./work/abn-search.parquet
   ▼
[7] parallel.build_sqlite_from_parquet        sqlite3.executemany loop
   │  writes ./work/abr.sqlite
   │  (LOCAL to runner; not uploaded by current pipeline)
   ▼
[8] _build_iceberg_snapshot_manifest          reads iceberg metadata
   │  writes ./work/iceberg-snapshot.json
   ▼
[9] write.build_manifest
   │  writes ./work/manifest.json
   ▼
[10] publish.upload_all + search-index upload
      writes manifest.json + manifest-{date}.json
            + iceberg-snapshot.json + iceberg-snapshot-{date}.json
            + abn-search-latest.parquet + abn-search-{date}.parquet
      to R2 bucket root
```

**Out of pipeline scope but observed on R2:** the static `abn-*.parquet` and `abr-extract-*.sqlite` keys at root. No code path in this repo writes them — origin tracked in #13.

## 4. Producer / consumer matrix

| Artifact | Producer | Cadence | Consumer | Read cadence |
|---|---|---|---|---|
| `manifest.json` (root) | `abr-extract run` | weekly | Pipeline idempotency check; frontend; future API | per pipeline run + per page load |
| `manifest-{date}.json` (root) | same | weekly | Audit only | rare |
| `iceberg-snapshot.json` (root) | `abr-extract run` (when working) | weekly | Frontend `index.html` (DuckDB-WASM URL list) | per page load |
| `__r2_data_catalog/<ns>/<table>/data/*.parquet` | `abr-extract run` via pyiceberg | weekly (one new generation per overwrite) | Frontend DuckDB-WASM `read_parquet(...)` | per query |
| `__r2_data_catalog/<ns>/<table>/metadata/*` | same | weekly | pyiceberg readers (next refresh's prev-state read) | weekly |
| `abns/index.html` | `scripts/deploy_frontend.ps1` (manual) | on demand | `gazetteer.au` users | continuously |
| `robots.txt` (root) | same | on demand | bots | continuously |
| `abn-*.parquet` (root) | **off-repo** (likely manual `wrangler r2 object put`) | unclear | README advertised; consumer-facing | unknown (no telemetry) |
| `abr-extract-*.sqlite` (root) | **off-repo** | unclear | README advertised | unknown |

## 5. Update cadence

| Trigger | Code path | Side effects |
|---|---|---|
| Cron Sun 04:00 UTC | `refresh.yml` scheduled run | Full pipeline (steps 1–9 above) |
| Manual `workflow_dispatch` | `refresh.yml` with optional `--force` and `--max-records` | Full pipeline; `--max-records` writes to `abr_test` namespace |
| Manual `pwsh ./scripts/deploy_frontend.ps1` | `wrangler r2 object put` × 2 | Updates `abns/index.html` + `robots.txt` |
| Manual `wrangler deploy` (in `worker/`) | Cloudflare Workers deploy | Updates the Worker that serves `gazetteer.au` routes |
| Off-repo manual upload | Unidentified | Updates `abn-*` / `abr-extract-*` keys at root |

## 6. Growth model

Single measured datum from the 2026-05-08 R2 inventory: **bucket total
11.12 GB**. Anything that scales over time (per-week deltas, multi-month
projections) requires repeated measurements that don't exist yet. This
section records what's measured; growth-rate analysis is deferred until
two consecutive successful refreshes give a real delta.

### 6.1 Static dumps at root

Eight keys, total measured **~10.44 GB** (~94% of bucket):

| Item | Per dated copy | Per latest copy |
|---|---:|---:|
| `abr-extract-*.sqlite` | 4.25 GB | 4.25 GB |
| `abn-main-*.parquet` | 0.77 GB | 0.77 GB |
| `abn-trading-names-*.parquet` | 0.20 GB | 0.20 GB |
| `abn-dgr-*.parquet` | 0.001 GB | 0.001 GB |

Whether these grow week-on-week depends on the upload origin (still
unverified — #13). Growth-rate projection deferred until origin is
identified and at least two consecutive runs are observed.

### 6.2 Iceberg warehouse (`__r2_data_catalog/`)

Total ~680 MB measured. `abn_main_history` in namespace 1: 547 MB
across 11 data files + ~16 KB metadata. The other two tables in
namespace 1 and all three tables in namespace 2 (`abr_test`) have
not been drilled.

`prune_iceberg_snapshots(keep=2)` (this PR) bounds the *snapshot log*
size — each table retains at most 2 snapshots after each refresh.
Whether the *data file* footprint stays bounded depends on cleanup of
orphan files (uncommitted SIGKILL artefacts + expired snapshots' files),
which PyIceberg 0.11 does not do automatically. Tracked in #13.

Per-week growth requires measurement after the next refresh.

### 6.3 Manifest history at root

`manifest-{date}.json` measured at 1.08 KB. Negligible at any cadence.

### 6.4 `abr_test` namespace

Total measured 1.67 MB for the one drilled table. If truncated runs
continue without explicit teardown the namespace accumulates one new
snapshot per run; bounded by `prune_iceberg_snapshots(keep=2)` once a
third run lands.

## 7. Target steady state

| Region | What's in it |
|---|---|
| `__r2_data_catalog/` (`abr` ns only) | 2 retained snapshots × 3 tables (per `prune_iceberg_snapshots(keep=2)`). Data file orphans cleaned periodically (#13). |
| `manifest.json` + dated history | Audit trail |
| `iceberg-snapshot.json` + dated history | Audit + frontend pointer |
| `abn-search-latest.parquet` + dated copy | Frontend search index (#22) |
| `abns/index.html` | Frontend |
| `abn-*-{date}.parquet` + `abr-extract-{date}.sqlite` (one archival date) | One frozen v1 dump for back-compat |
| `abn-*-latest.*` | Decision pending — either deleted (with README patch) or copy-from the kept dated set |
| `abr_test` namespace | Torn down between test runs |

Concrete target *size* requires measuring after the next clean refresh
+ #13 cleanup pass. Inventory will be re-reconciled then.

## 8. Gap analysis (current → target)

| Gap | Current | Target | Blocker | Issue |
|---|---|---|---|---|
| `iceberg-snapshot.json` exists at root | Missing | Present, refreshed weekly | Silent pipeline failure or off-pipeline write — needs diagnosis | #18 (depends on #20) |
| Static dumps controlled by pipeline | Off-repo, opaque | Either zero (drop, README patch) or one archival + republished by pipeline | Origin unverified | #13 (depends on owner local check) |
| Snapshot metadata bounded | Unbounded growth | `keep=2` per table | Code change | **In this PR** |
| Snapshot data file orphans cleaned | Stay forever | Periodic S3-list-and-diff cleanup | PyIceberg has no built-in; need homegrown | #13 |
| `abr_test` namespace lingers | Yes | Tear down post-test | Code change in CLI | New issue (file from #21) |
| Failed pipeline runs are diagnosable | Silent SIGKILL, no traceback | Stage marker + dmesg + artifact | Code change | #20 |
| SCD2 path validated against real data | Synthetic only | Whole or sliced real-data validation | Need next weekly refresh + diff script | #19 |
| SQLite materialisation is fast | `sqlite3.executemany` Python loop | DuckDB `sqlite` extension | Code change | #2 |
| LICENSE detectable on GitHub | No `LICENSE` file | Standard MIT `LICENSE` at root | One-shot file add | #16 |
| ABN/ACN cells link out | Plain text | ABR Lookup + OpenCorporates anchors | Frontend change | #14 |
| Frontend search latency | 4-way OR-ILIKE + trading sub-query, ~5 GB scanned worst case | One ILIKE on precomputed `abn-search.parquet`, ~1/5 the bytes | Rebase + merge `frontend-search-index` branch | #22 |
| Stale merged branches | 6 lingering on remote | Deleted | One-shot `git push origin --delete` per §12 | (no issue — owner action) |

## 9. Action plan

This collapses #21's sequencing into actionable groups.

**Group A — unblock everything else (do first)**

1. Merge the active PR. Pruning is a no-op against current state, no risk.
2. **#20** — diagnostic instrumentation. Without this every future failure is opaque, including the things we're trying to investigate.

**Group B — diagnose what we don't yet understand**

3. With #20 in place, investigate **#18** (missing snapshot manifest) on the next refresh run.
4. Owner runs local-machine check for the off-repo upload script (per #13 comment); reports findings.

**Group C — execute cleanup (one-shot, after Groups A+B)**

5. Decide `*-latest` URL fate (delete + README patch, or republish in pipeline).
6. Execute #13 cleanup procedure with revised paths from #17. Likely recovers ~10 GB.
7. File the `abr_test` teardown issue if not done; small mitigation.

**Group D — durable improvements (any order, no hard dependencies)**

8. **#19** — real-data SCD2 validation, after the next weekly refresh lands.
9. **#16** — LICENSE file once the active PR is on `main`.
10. **#2** (M7) — SQLite via DuckDB. Existing branch `m7-duckdb-sqlite` (`10ef4ef`); review then merge.
11. **#22** — Frontend search index. Existing branch `frontend-search-index` (`d91e021`); rebase post-PR-merge then review.
12. **#14** — ABN/ACN deep links. Frontend-only.
13. **Branch cleanup** — once #21 reflects only live work, delete the six merged branches per §12.

## 11. Search performance

The frontend's UX is dominated by per-keystroke search latency against
the parquet datasets. End-to-end timing is not measured in this repo.
This section describes the *shape* of current vs. post-search-index
queries; quantitative comparison waits on measurement post-#22.

### 11.1 Current shape (master)

`frontend/index.html` issues, on every keystroke, a query of roughly the
form:

```sql
SELECT … FROM read_parquet([..main data files..]) main
WHERE valid_to IS NULL
  AND (
    main_name                  ILIKE ?
    OR individual_family_name  ILIKE ?
    OR individual_given_names  ILIKE ?
    OR abn IN (
      SELECT abn FROM read_parquet([..trading data files..])
      WHERE valid_to IS NULL AND name ILIKE ?
    )
  )
LIMIT …
```

Four ILIKE comparisons. Two parquet relations scanned. Per-row
`LOWER()` coercion. The trading sub-query is re-evaluated each
keystroke regardless of column pruning.

### 11.2 Post-#22 shape

After the search index lands the same UX query becomes:

```sql
SELECT … FROM read_parquet('abn-search-latest.parquet')
WHERE search_text ILIKE ?
LIMIT …
```

One ILIKE on a denormalised, pre-lowercased column. One parquet
relation scanned (the search index, which carries only display
columns). The trading sub-query is gone — trading names are folded
into `search_text` at index-build time.

Profile expand on a row still hits `abn_main_history` for the full
record — search index is for the *list* view, not for replacing main as
the source of truth.

Comparative timing requires browser-side instrumentation; out of scope
for this iteration.

### 11.3 Beyond #22

Out of scope. Don't speculate; measure first, then revisit.

## 12. Branch inventory + cleanup recommendations

Live remote branches as of 2026-05-08, with what to do with each.

| Branch | Latest commit | Status | Recommendation |
|---|---|---|---|
| `master` | `3a77689` | Default | **Keep.** |
| `claude/investigate-iceberg-duplicates-So7zi` | `499c32b` | Active PR (this branch) | **Keep until merged.** |
| `frontend-search-index` | `d91e021` | WIP — search index work, tracked in #22 | **Keep.** Has unmerged code; rebase onto post-PR `master` and merge per #22. |
| `m7-duckdb-sqlite` | `10ef4ef` | WIP — SQLite-via-DuckDB work, tracked in #2 | **Keep.** Has unmerged code; needs review per #2. |
| `add-robots-txt` | `fab2002` | Merged via PR #6 | **Safe to delete.** |
| `m6-parallel-parse` | `cf95625` | Merged via PR #5 | **Safe to delete.** |
| `iceberg-snapshot-manifest` | `f6189ce` | Merged via PR #8 | **Safe to delete** — verify the SHA matches `master` history first if you want to be belt-and-braces. |
| `hotfix/iceberg-headroom` | `3d1fa07` | Merged via PR #10 | **Safe to delete.** |
| `hotfix/iceberg-arrow-bootstrap` | `c743634` | Merged via PR #11 | **Safe to delete.** |
| `hotfix/iceberg-oneshot-overwrite` | `609f8a9` | Merged via PR #12 | **Safe to delete.** |

Six branches recommended for deletion. None protected, none have unmerged work.

```bash
# All six in one go (run locally; this needs network access to push deletes):
for b in add-robots-txt m6-parallel-parse iceberg-snapshot-manifest \
         hotfix/iceberg-headroom hotfix/iceberg-arrow-bootstrap \
         hotfix/iceberg-oneshot-overwrite; do
  git push origin --delete "$b"
done
```

## 13. Maintenance of this document

- This is a **point-in-time** reconciliation. It will drift.
- Re-reconcile against R2 inventory whenever bucket size moves materially or a new infra issue is filed.
- Update the "Status" column in §2.1 when issues close.
- Bump §6 projections if cadence or data shape changes (e.g. ABR doubles in size, refresh frequency changes).
- **Issues flow from this doc.** The GitHub issue board (#21) is a slim mirror of the open rows in §8 and §9. When an item closes here, close the corresponding issue. When a new gap is identified, file an issue *and* add a row here in the same change.
- Where inference was used (no measurement available) sections are marked. Replace with measured numbers as they become available.
