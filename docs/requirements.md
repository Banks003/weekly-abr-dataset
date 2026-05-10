# Product requirements

> **Status:** draft. Captures outcomes the new product must enable, not
> implementation. Items tagged `[Must] / [Should] / [Could] / [Won't]`
> follow MoSCoW. Open questions at the bottom must be resolved before
> the architecture lane is locked — see `architecture.md` for the
> current system inventory.
>
> **Drafted:** 2026-05-10. To be revised once the open questions are
> answered.

## Personas

- **P1 — Web visitor.** Lands on gazetteer.au. Wants to find a specific Australian business or browse a slice of them.
- **P2 — Analyst / researcher.** Wants to answer questions about ABNs in aggregate ("how many new businesses in NSW in 2024Q1") without writing code if possible.
- **P3 — Developer / scriptable user.** Wants to pull ABN data programmatically — locally via CLI for one-off jobs, or via HTTP for integration.
- **P4 — Maintainer.** Wants to run the pipeline weekly without it falling over and without storage costs creeping.

## Outcomes

### O1 — Find a specific business

- **R1.1 [Must]** P1 can find an ABN by typing part of a name. Matches against business name, trading names, and individual/sole-trader names.
- **R1.2 [Must]** P1 can paste a full 11-digit ABN and get a direct profile in one step.
- **R1.3 [Must]** Search results return within ~200ms per keystroke for typical queries (substring match) on the frontend, so typing feels live.
- **R1.4 [Must]** P1 can filter results by state, current ABN status (active/cancelled), and entity type.
- **R1.5 [Should]** P1 can deep-link to an ABN profile page that's shareable and indexable.

### O2 — See everything publicly known about one ABN

- **R2.1 [Must]** Profile shows: legal name, entity type, state/postcode, ABN status + from-date, ACN/ASIC number if present, GST status + from-date, all trading names, all DGR endorsements with dates.
- **R2.2 [Must]** Profile clearly shows the as-of date (which weekly extract this came from).
- **R2.3 [Should]** Profile can optionally include fields that aren't in the bulk extract — name history, trading-name effective-from/to dates, GST to-date — fetched live from the ABR's official JSON service when requested.
- **R2.4 [Could]** Profile shows what changed since the previous weekly snapshot (e.g. "trading name X added on 2026-04-14"). *(This is the requirement that makes or breaks the case for Iceberg — see open questions.)*

### O3 — Understand trends over time

- **R3.1 [Must]** P2 can see new ABN registrations broken down by state and time bucket (month/year), since a chosen start date.
- **R3.2 [Must]** Same for cancellations.
- **R3.3 [Must]** P2 can see current active-ABN counts by state and by entity type.
- **R3.4 [Should]** P2 can filter trend series (e.g. "registrations in NSW for entity type PRV by month").
- **R3.5 [Should]** Trend results render as charts in the frontend, not just raw tables.
- **R3.6 [Could]** P2 can see week-over-week change feeds — what attributes flipped on which ABNs (name, state, status). *(Same Iceberg-justifying call as R2.4.)*
- **R3.7 [Won't, for now]** Sub-weekly trends. Source data is weekly; we don't manufacture intra-week resolution we don't have.

### O4 — Pull ABN data programmatically

- **R4.1 [Must]** P3 can install a CLI and run name search, profile lookup, and trend queries from the terminal.
- **R4.2 [Must]** CLI returns structured output (JSON by default) suitable for piping into other tools.
- **R4.3 [Must]** The same query shape works against either local cached data or a remote endpoint — same arguments, same JSON output. (This is the "scales from CLI to API" promise.)
- **R4.4 [Must]** CLI can optionally trigger live ABR-API enrichment for one ABN (R2.3), without needing the bulk extract.
- **R4.5 [Should]** CLI supports caching the working dataset locally so repeat queries are fast and don't re-download.
- **R4.6 [Should]** P3 can download the latest published current-state file (one parquet) for their own analytics, with a stable URL.
- **R4.7 [Could]** P3 can download trend pre-aggregations as standalone files.

### O5 — Hit it as an HTTP API

- **R5.1 [Must]** Read-only HTTP endpoints exposing the same operations as the CLI: name search, ABN profile, trend series.
- **R5.2 [Must]** Endpoint responses match the CLI's JSON shape exactly. One contract, two transports.
- **R5.3 [Must]** Anonymous, public, rate-limited only (no auth required for read).
- **R5.4 [Should]** Cacheable at the edge (Cloudflare cache); responses include sensible cache headers.
- **R5.5 [Could]** "Profile as of date X" — historical lookups for one ABN. *(Iceberg-justifying — see open questions.)*
- **R5.6 [Won't, for now]** Write/submission endpoints. No user-generated content.
- **R5.7 [Won't, for now]** Authenticated tiers, paid plans, API keys.

### O6 — Trust the data

- **R6.1 [Must]** Every published artifact identifies which weekly extract it derives from.
- **R6.2 [Must]** Source attribution (CC-BY ABR) visible to end users on the frontend.
- **R6.3 [Must]** Refresh cadence visible: "data as of YYYY-MM-DD".
- **R6.4 [Should]** Audit history accessible: a user can ask "what did this ABN look like 4 weeks ago" *(if R2.4/R3.6/R5.5 are in)*.
- **R6.5 [Should]** Schema documented in one place that's the source of truth (not duplicated in README and code).

### O7 — Run the pipeline reliably

- **R7.1 [Must]** Pipeline runs unattended weekly. No manual steps in the steady state.
- **R7.2 [Must]** Idempotent: if the source extract hasn't changed, re-running is a no-op (no wasted writes, no contract churn).
- **R7.3 [Must]** A failed run produces enough diagnostics to fix without re-running blind.
- **R7.4 [Must]** Storage cost stays bounded as weeks accumulate (no unbounded snapshot growth).
- **R7.5 [Should]** Local-development mode lets a contributor run the full pipeline against truncated data without touching production storage.
- **R7.6 [Should]** All published artifacts are reproducible: same source extract → same outputs.

## Non-functional

- **N1 [Must]** Free / cheap to run. Targets the Cloudflare R2 + Workers free tier where possible. *(Assumption — confirm budget.)*
- **N2 [Must]** Public read, anonymous, unauthenticated.
- **N3 [Must]** Mobile-friendly frontend.
- **N4 [Must]** Stable URLs for known artifacts and API endpoints. Breaking changes go through deprecation, not stealth removal.
- **N5 [Should]** Sub-second cold-start for API endpoints (cache-friendly, small bundles).
- **N6 [Should]** No lock-in: the canonical data is in open formats (parquet, JSON) on object storage, queryable by anyone with DuckDB.
- **N7 [Won't]** SLAs, uptime guarantees. Best-effort.

## Explicitly out of scope

- Submissions, corrections, user accounts.
- Joining ABN data with other datasets (Companies House, ASIC details beyond ACN, geocoding, etc.).
- Real-time / sub-weekly freshness.
- Enterprise features: SSO, audit logs for users, multi-tenant.
- Data validation / quality checks beyond what the source provides.
- Replacing the official ABR Lookup as a system of record.

## Open questions

These materially change the architecture and must be answered before the lane is locked.

1. **Row-level history — is it required?**
   R2.4, R3.6, R5.5. If any of these are Must or Should, Iceberg earns its keep as the snapshot archive. If all are Could/Won't, Iceberg is overkill and we can do this with retained dated parquets. **Single biggest decision.**

2. **Trends in the frontend — charts or just numbers?**
   R3.5 says charts. Charts mean we ship a visualization library and a tiny JSON feed; numbers means the trend data is just an API response.

3. **History horizon.**
   If we keep weekly history, how far back? 6 months, 2 years, forever? Drives storage cost and the snapshot-pruning policy.

4. **CLI installation surface.**
   `pip install` / `uv add`? Homebrew? Standalone binary? Determines packaging investment.

5. **Worker hosting commitment.**
   Cloudflare Workers stays as the API host? That constrains the query engine choice (no DuckDB-WASM, hyparquet or Containers). Or are we open to a Python backend on Fly / Railway / Render for the API?

6. **Frontend scope.**
   Search + profile + trend charts only, or also things like saved searches, watchlists, exports? Determines whether the frontend is static-HTML or something heavier.

7. **Definition of "better than the existing ABR website".**
   Better at search latency? Better at filters? Better at bulk export? Better at trends? Worth picking the 2-3 dimensions to compete on so we don't try to win everywhere.

Questions 1, 2, and 5 are the lane-deciding ones. The rest is sizing.
