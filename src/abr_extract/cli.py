from __future__ import annotations

import os
import uuid
import zipfile
from datetime import UTC, datetime
from pathlib import Path

import click

from .catalog import fetch_catalog
from .download import download_all
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
@click.option("--force", is_flag=True, help="Run even if the source extract is unchanged.")
def run(output_dir: str, skip_publish: bool, force: bool) -> None:
    """Run the full pipeline: fetch -> download -> parse -> write -> publish."""
    work_dir = Path(output_dir)
    work_dir.mkdir(parents=True, exist_ok=True)

    catalog = fetch_catalog()
    extract_time_iso = catalog.extract_last_modified.isoformat()
    extract_date = catalog.extract_last_modified.date().isoformat()
    click.echo(f"Catalog: extract {extract_time_iso}")

    settings: R2Settings | None = None
    s3 = None
    if not skip_publish:
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
    with WriterBundle(paths) as wb:
        for dr in download_results:
            with zipfile.ZipFile(dr.path) as z:
                for info in z.infolist():
                    if not info.filename.endswith(".xml"):
                        continue
                    click.echo(f"  parsing {info.filename}...")
                    with z.open(info) as f:
                        for record in parse_records(f):
                            wb.write(record)

    counts = wb.counts
    click.echo(
        f"  rows: main={counts.main:,} trading={counts.trading:,} dgr={counts.dgr:,}"
    )

    manifest = build_manifest(
        paths,
        counts,
        extract_time=extract_time_iso,
        pipeline_run_id=str(uuid.uuid4()),
        generated_at=started.isoformat().replace("+00:00", "Z"),
    )
    manifest_path = work_dir / "manifest.json"
    write_manifest(manifest, manifest_path)
    click.echo(f"Manifest: {manifest_path}")

    if skip_publish:
        click.echo("--skip-publish set; not uploading.")
        return

    assert settings is not None and s3 is not None
    click.echo(f"Uploading to R2 bucket '{settings.bucket}'...")
    plans = plan_uploads(work_dir, extract_date=extract_date)
    upload_all(s3, settings.bucket, plans)
    click.echo(f"Uploaded {len(plans)} files (each as -latest and -{extract_date}).")


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
