from __future__ import annotations

import json
import os
import uuid
import zipfile
from datetime import UTC, date, datetime
from pathlib import Path

import click

from .catalog import fetch_catalog
from .download import download_all
from .history import (
    apply_update_dgr,
    apply_update_main,
    apply_update_trading,
    bootstrap_dgr,
    bootstrap_main,
    bootstrap_trading,
)
from .iceberg_io import (
    ABN_DGR_HISTORY_SCHEMA,
    ABN_MAIN_HISTORY_SCHEMA,
    ABN_TRADING_HISTORY_SCHEMA,
    build_snapshot_manifest,
    connect_r2_catalog,
    ensure_history_tables,
    load_iceberg_settings_from_env,
    update_history_table,
)
from .parse import parse_records
from .publish import (
    R2Settings,
    is_extract_already_published,
    make_s3_client,
    plan_uploads,
    read_remote_manifest,
    upload_all,
)
from .write import WriterBundle, WriterPaths, build_manifest, write_manifest


@click.group()
def main() -> None:
    pass


@main.command()
@click.option(
    "--output-dir",
    default="./work",
    help="Directory for downloaded zips and intermediate files.",
)
@click.option(
    "--skip-publish",
    is_flag=True,
    help="Build artefacts locally without uploading to R2.",
)
@click.option(
    "--skip-iceberg",
    is_flag=True,
    help="Skip the Iceberg history step (still publishes snapshot parquets).",
)
@click.option("--force", is_flag=True, help="Run even if the source extract is unchanged.")
@click.option(
    "--max-records",
    type=int,
    default=None,
    help="Truncate parse after N main records — full pipeline test mode. "
    "Implies test-isolated Iceberg namespace (abr_test) and skips R2 "
    "snapshot uploads so production artefacts are never overwritten.",
)
def run(
    output_dir: str,
    skip_publish: bool,
    skip_iceberg: bool,
    force: bool,
    max_records: int | None,
) -> None:
    """Run the full pipeline: fetch -> download -> parse -> write -> publish."""
    work_dir = Path(output_dir)
    work_dir.mkdir(parents=True, exist_ok=True)
    truncated = max_records is not None and max_records > 0

    catalog = fetch_catalog()
    extract_time_iso = catalog.extract_last_modified.isoformat()
    extract_date = catalog.extract_last_modified.date().isoformat()
    click.echo(f"Catalog: extract {extract_time_iso}")
    if truncated:
        click.echo(
            f"TRUNCATED RUN: max_records={max_records}; iceberg namespace=abr_test; "
            f"R2 snapshot uploads skipped."
        )

    settings: R2Settings | None = None
    s3 = None
    if not skip_publish and not truncated:
        settings = _load_r2_settings()
        s3 = make_s3_client(settings)
        remote_manifest = read_remote_manifest(s3, settings.bucket)
        if not force and is_extract_already_published(remote_manifest, extract_time_iso):
            click.echo("Remote manifest already at this extract — nothing to do.")
            return

    click.echo("Downloading source zips...")
    download_results = download_all(catalog, work_dir)

    paths = WriterPaths(
        main_parquet=work_dir / "abn_main.parquet",
        trading_parquet=work_dir / "abn_trading_names.parquet",
        dgr_parquet=work_dir / "abn_dgr.parquet",
        sqlite_db=work_dir / "abr.sqlite",
    )

    click.echo("Parsing and writing artefacts...")
    started = datetime.now(UTC)
    done_early = False
    with WriterBundle(paths) as wb:
        for dr in download_results:
            if done_early:
                break
            with zipfile.ZipFile(dr.path) as z:
                for info in z.infolist():
                    if done_early:
                        break
                    if not info.filename.endswith(".xml"):
                        continue
                    click.echo(f"  parsing {info.filename}...")
                    with z.open(info) as f:
                        for record in parse_records(f):
                            wb.write(record)
                            if truncated and wb.counts.main >= max_records:
                                done_early = True
                                break

    counts = wb.counts
    click.echo(
        f"  rows: main={counts.main:,} trading={counts.trading:,} dgr={counts.dgr:,}"
    )

    iceberg_summary: dict | None = None
    iceberg_ns = "abr_test" if truncated else "abr"
    if not skip_iceberg:
        iceberg_summary = _run_iceberg_step(
            paths,
            extract_date_iso=extract_date,
            namespace=iceberg_ns,
        )
        click.echo(f"Iceberg history step ({iceberg_ns}): {iceberg_summary}")

    iceberg_snapshot_manifest: dict | None = None
    if not skip_iceberg:
        # M10 — publish iceberg-snapshot.json so the frontend (and CLI
        # via query.py) can read directly from Iceberg data files.
        iceberg_bucket = (
            settings.bucket if settings is not None else os.environ.get(
                "R2_BUCKET", "weekly-abr-dataset"
            )
        )
        iceberg_snapshot_manifest = _build_iceberg_snapshot_manifest(
            namespace=iceberg_ns,
            generated_at=started.isoformat().replace("+00:00", "Z"),
            extract_time=extract_time_iso,
            bucket=iceberg_bucket,
        )
        # Always write the local copy so we can inspect runs that
        # skipped publish.
        snap_path = work_dir / "iceberg-snapshot.json"
        snap_path.write_text(
            json.dumps(iceberg_snapshot_manifest, indent=2, sort_keys=True),
            encoding="utf-8",
        )
        click.echo(f"Iceberg snapshot manifest: {snap_path}")

    manifest = build_manifest(
        paths,
        counts,
        extract_time=extract_time_iso,
        pipeline_run_id=str(uuid.uuid4()),
        generated_at=started.isoformat().replace("+00:00", "Z"),
    )
    if iceberg_summary is not None:
        manifest["iceberg"] = iceberg_summary
        manifest["iceberg_namespace"] = iceberg_ns
    if iceberg_snapshot_manifest is not None:
        manifest["iceberg_snapshot_url"] = f"{public_base_url_for(settings)}/iceberg-snapshot.json"
    if truncated:
        manifest["truncated"] = True
        manifest["max_records"] = max_records
    manifest_path = work_dir / "manifest.json"
    write_manifest(manifest, manifest_path)
    click.echo(f"Manifest: {manifest_path}")

    if skip_publish or truncated:
        reason = "--skip-publish set" if skip_publish else "truncated run"
        click.echo(f"{reason}; not uploading snapshot artefacts to R2.")
        return

    assert settings is not None and s3 is not None
    click.echo(f"Uploading to R2 bucket '{settings.bucket}'...")
    plans = plan_uploads(work_dir, extract_date=extract_date)
    upload_all(s3, settings.bucket, plans)
    if iceberg_snapshot_manifest is not None:
        snap_body = json.dumps(
            iceberg_snapshot_manifest, indent=2, sort_keys=True
        ).encode("utf-8")
        for key in (
            "iceberg-snapshot.json",
            f"iceberg-snapshot-{extract_date}.json",
        ):
            s3.put_object(
                Bucket=settings.bucket,
                Key=key,
                Body=snap_body,
                ContentType="application/json",
            )
        click.echo(
            f"Uploaded iceberg-snapshot.json + dated copy to "
            f"{public_base_url_for(settings)}/iceberg-snapshot.json"
        )
    click.echo(f"Uploaded {len(plans)} files (each as -latest and -{extract_date}).")


def _run_iceberg_step(
    paths: WriterPaths, *, extract_date_iso: str, namespace: str = "abr"
) -> dict:
    """Apply SCD2 update to the three Iceberg history tables on R2 Data Catalog.

    Reads each just-written snapshot parquet, reconciles against the prior
    history (or bootstraps if the table is empty), writes back. Returns a
    summary dict per relation for the manifest.
    """
    import polars as pl

    extract_date = date.fromisoformat(extract_date_iso)

    iceberg_settings = load_iceberg_settings_from_env()
    catalog = connect_r2_catalog(iceberg_settings)
    tables = ensure_history_tables(catalog, namespace=namespace)

    summary: dict = {}
    for table_key, parquet_path, schema, bootstrap_fn, apply_fn in (
        (
            "abn_main_history",
            paths.main_parquet,
            ABN_MAIN_HISTORY_SCHEMA,
            bootstrap_main,
            apply_update_main,
        ),
        (
            "abn_trading_names_history",
            paths.trading_parquet,
            ABN_TRADING_HISTORY_SCHEMA,
            bootstrap_trading,
            apply_update_trading,
        ),
        (
            "abn_dgr_history",
            paths.dgr_parquet,
            ABN_DGR_HISTORY_SCHEMA,
            bootstrap_dgr,
            apply_update_dgr,
        ),
    ):
        snapshot_df = pl.read_parquet(parquet_path)
        summary[table_key] = update_history_table(
            tables[table_key],
            snapshot_df,
            extract_date,
            bootstrap_fn=bootstrap_fn,
            apply_fn=apply_fn,
            schema=schema,
        )
    return summary


def _build_iceberg_snapshot_manifest(
    *,
    namespace: str,
    generated_at: str,
    extract_time: str,
    bucket: str,
    public_base_url: str = "https://gazetteer.au",
) -> dict:
    """Build the iceberg-snapshot.json payload from the live R2 Data Catalog.

    Re-connects to the catalog (cheap, env-driven) so this can run after
    ``_run_iceberg_step`` without sharing in-memory state.
    """
    iceberg_settings = load_iceberg_settings_from_env()
    catalog = connect_r2_catalog(iceberg_settings)
    tables = ensure_history_tables(catalog, namespace=namespace)
    return build_snapshot_manifest(
        tables,
        namespace=namespace,
        generated_at=generated_at,
        extract_time=extract_time,
        bucket=bucket,
        public_base_url=public_base_url,
    )


def _load_r2_settings() -> R2Settings:
    missing = [
        v
        for v in ("R2_ACCOUNT_ID", "R2_ACCESS_KEY_ID", "R2_SECRET_ACCESS_KEY", "R2_BUCKET")
        if not os.environ.get(v)
    ]
    if missing:
        raise click.ClickException(
            f"Missing required env vars: {', '.join(missing)}. "
            "Set them or pass --skip-publish."
        )
    return R2Settings(
        account_id=os.environ["R2_ACCOUNT_ID"],
        access_key_id=os.environ["R2_ACCESS_KEY_ID"],
        secret_access_key=os.environ["R2_SECRET_ACCESS_KEY"],
        bucket=os.environ["R2_BUCKET"],
    )


@main.command()
@click.option("--output-dir", default="./work", help="Where to save the zip files.")
def download(output_dir: str) -> None:
    """Download the current ABR bulk-extract zips to <output_dir>."""
    catalog = fetch_catalog()
    results = download_all(catalog, Path(output_dir))
    for r in results:
        click.echo(
            f"{r.path.name}: {r.bytes_written / 1_000_000:.1f} MB "
            f"in {r.elapsed_seconds:.1f}s, sha256={r.sha256[:16]}..."
        )


@main.command(name="search")
@click.argument("query")
@click.option("--limit", default=20, help="Max results")
@click.option(
    "--in",
    "search_in",
    type=click.Choice(["all", "main", "trading", "individual"]),
    default="all",
)
@click.option(
    "--format",
    "output_format",
    type=click.Choice(["json", "table"]),
    default="table",
)
@click.option(
    "--source",
    default=None,
    help="Override the dataset URL or path (default: live R2)",
)
@click.option(
    "--cache-dir",
    default=None,
    help="Local directory to cache downloaded parquets (faster repeat queries)",
)
def search_cmd(
    query: str,
    limit: int,
    search_in: str,
    output_format: str,
    source: str | None,
    cache_dir: str | None,
) -> None:
    """Search for ABNs by name (main, trading or individual)."""
    import json as _json

    from .query import DEFAULT_SOURCE
    from .query import search as _search

    rows = _search(
        query,
        limit=limit,
        search_in=search_in,
        source=source or DEFAULT_SOURCE,
        cache_dir=cache_dir,
    )
    if output_format == "json":
        click.echo(_json.dumps(rows, indent=2, default=str))
        return
    if not rows:
        click.echo("(no matches)")
        return
    cols = list(rows[0].keys())
    click.echo("  " + " | ".join(cols))
    click.echo("  " + "-+-".join("-" * len(c) for c in cols))
    for r in rows:
        cells = [str(r[c]) if r[c] is not None else "" for c in cols]
        click.echo("  " + " | ".join(cells))


@main.command(name="profile")
@click.argument("abn")
@click.option(
    "--format",
    "output_format",
    type=click.Choice(["json", "table"]),
    default="json",
)
@click.option("--source", default=None, help="Override the dataset URL or path")
@click.option(
    "--cache-dir",
    default=None,
    help="Local directory to cache downloaded parquets (faster repeat queries)",
)
@click.option(
    "--enrich-live",
    is_flag=True,
    help="Also call the official ABR JSON API for fields not in the bulk extract "
    "(needs ABR_API_GUID env var)",
)
def profile_cmd(
    abn: str,
    output_format: str,
    source: str | None,
    cache_dir: str | None,
    enrich_live: bool,
) -> None:
    """Fetch the full profile for one ABN."""
    import json as _json

    from .query import DEFAULT_SOURCE
    from .query import enrich_profile_live as _enrich
    from .query import profile as _profile

    p = _profile(abn, source=source or DEFAULT_SOURCE, cache_dir=cache_dir)
    if p is None:
        raise click.ClickException(f"ABN {abn} not found")
    if enrich_live:
        guid = os.environ.get("ABR_API_GUID")
        if not guid:
            raise click.ClickException(
                "ABR_API_GUID env var is required for --enrich-live"
            )
        p["live"] = _enrich(abn, guid=guid)
    if output_format == "json":
        click.echo(_json.dumps(p, indent=2, default=str))
        return
    click.echo(f"ABN:                  {p['abn']}")
    click.echo(f"Main name:            {p['main_name']}")
    if p["individual"]:
        i = p["individual"]
        full = " ".join(
            [s for s in (i.get("title"), i.get("given_names"), i.get("family_name")) if s]
        )
        click.echo(f"Individual:           {full}")
    click.echo(f"Entity type:          {p['entity_type']['text']} ({p['entity_type']['code']})")
    state = p["address"]["state"] or "-"
    postcode = p["address"]["postcode"] or "-"
    click.echo(f"Address:              {state} {postcode}")
    click.echo(f"ABN status:           {p['abn_status']['status']} from {p['abn_status']['from']}")
    if p["gst"]:
        click.echo(f"GST:                  {p['gst']['status']} from {p['gst']['from']}")
    click.echo(f"\nTrading / business names ({len(p['trading_names'])}):")
    for t in p["trading_names"]:
        click.echo(f"  [{t['type']}] {t['name']}")
    click.echo(f"\nDGR registrations ({len(p['dgrs'])}):")
    for d in p["dgrs"]:
        name = d["name"] or "(applies to main entity)"
        click.echo(f"  {d['from']}  [{d['status'] or '—'}]  {name}")


@main.command(name="trends")
@click.argument(
    "metric",
    type=click.Choice(
        ["registrations", "cancellations", "by_state", "by_entity_type"]
    ),
)
@click.option("--since", default="2020-01-01", help="Lower bound on dates (ISO)")
@click.option(
    "--by",
    "group_by",
    type=click.Choice(["month", "year"]),
    default="month",
)
@click.option(
    "--format",
    "output_format",
    type=click.Choice(["json", "csv", "table"]),
    default="table",
)
@click.option("--source", default=None, help="Override the dataset URL or path")
@click.option(
    "--cache-dir",
    default=None,
    help="Local directory to cache downloaded parquets (faster repeat queries)",
)
def trends_cmd(
    metric: str,
    since: str,
    group_by: str,
    output_format: str,
    source: str | None,
    cache_dir: str | None,
) -> None:
    """Pre-baked aggregations: registrations, cancellations, by_state, by_entity_type."""
    import csv as _csv
    import io as _io
    import json as _json

    from .query import DEFAULT_SOURCE
    from .query import trends as _trends

    rows = _trends(
        metric,
        since=since,
        group_by=group_by,
        source=source or DEFAULT_SOURCE,
        cache_dir=cache_dir,
    )
    if output_format == "json":
        click.echo(_json.dumps(rows, indent=2, default=str))
        return
    if output_format == "csv":
        buf = _io.StringIO()
        if rows:
            writer = _csv.DictWriter(buf, fieldnames=list(rows[0].keys()))
            writer.writeheader()
            writer.writerows(rows)
        click.echo(buf.getvalue().rstrip())
        return
    if not rows:
        click.echo("(no rows)")
        return
    cols = list(rows[0].keys())
    click.echo("  " + " | ".join(cols))
    click.echo("  " + "-+-".join("-" * len(c) for c in cols))
    for r in rows:
        cells = [str(r[c]) if r[c] is not None else "" for c in cols]
        click.echo("  " + " | ".join(cells))


@main.command()
def check() -> None:
    """Print the current ABR bulk-extract catalog metadata."""
    catalog = fetch_catalog()
    click.echo(f"Package: {catalog.package_id}")
    click.echo(f"Extract last modified: {catalog.extract_last_modified.isoformat()}")
    total = sum(z.size_bytes for z in catalog.zips)
    click.echo(f"Zip resources: {len(catalog.zips)} ({total / 1_000_000:.1f} MB total)")
    for z in catalog.zips:
        click.echo(
            f"  - {z.name}: {z.size_bytes / 1_000_000:.1f} MB, "
            f"modified {z.last_modified.isoformat()}"
        )
        click.echo(f"    {z.url}")
    if catalog.schema_xsd_url:
        click.echo(f"Schema: {catalog.schema_xsd_url}")
    if catalog.readme_pdf_url:
        click.echo(f"Readme: {catalog.readme_pdf_url}")


if __name__ == "__main__":
    main()


def public_base_url_for(settings: R2Settings | None) -> str:
    """Public CDN base URL where the bucket contents are served.

    Currently always gazetteer.au; lifted into a function so it can be
    overridden later via env var if we ever fork the deploy.
    """
    return os.environ.get("PUBLIC_BASE_URL", "https://gazetteer.au")
