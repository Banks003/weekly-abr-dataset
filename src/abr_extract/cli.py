from __future__ import annotations

import click

from .catalog import fetch_catalog


@click.group()
def main() -> None:
    pass


@main.command()
@click.option("--output-dir", default="./work", help="Directory for downloaded zips and intermediate files.")
@click.option("--skip-publish", is_flag=True, help="Build artefacts locally without uploading to R2.")
@click.option("--force", is_flag=True, help="Run even if the source extract is unchanged.")
def run(output_dir: str, skip_publish: bool, force: bool) -> None:
    raise NotImplementedError("Pipeline not yet implemented")


@main.command()
def check() -> None:
    """Print the current ABR bulk-extract catalog metadata."""
    catalog = fetch_catalog()
    click.echo(f"Package: {catalog.package_id}")
    click.echo(f"Extract last modified: {catalog.extract_last_modified.isoformat()}")
    total = sum(z.size_bytes for z in catalog.zips)
    click.echo(f"Zip resources: {len(catalog.zips)} ({total / 1_000_000:.1f} MB total)")
    for z in catalog.zips:
        click.echo(f"  - {z.name}: {z.size_bytes / 1_000_000:.1f} MB, modified {z.last_modified.isoformat()}")
        click.echo(f"    {z.url}")
    if catalog.schema_xsd_url:
        click.echo(f"Schema: {catalog.schema_xsd_url}")
    if catalog.readme_pdf_url:
        click.echo(f"Readme: {catalog.readme_pdf_url}")


if __name__ == "__main__":
    main()
