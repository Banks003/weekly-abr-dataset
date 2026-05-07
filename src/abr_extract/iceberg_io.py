"""Iceberg + R2 Data Catalog wiring.

Production target: R2 Data Catalog (REST catalog + R2 object storage). Local
tests use PyIceberg's SQL catalog backed by SQLite + a temp directory, so
the SCD2 logic and Iceberg writes are tested without touching R2.

The history.py module produces the full reconciled SCD2 DataFrame each
week. This module's write_history overwrites the corresponding Iceberg
table with that DataFrame — simple and correct. Iceberg snapshots provide
the table-level time-travel; the SCD2 columns provide row-level history.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import polars as pl
from pyiceberg.catalog import Catalog, load_catalog
from pyiceberg.exceptions import NoSuchTableError
from pyiceberg.partitioning import UNPARTITIONED_PARTITION_SPEC
from pyiceberg.schema import Schema
from pyiceberg.table import Table
from pyiceberg.types import (
    DateType,
    NestedField,
    StringType,
)

# Schemas mirror schema.py's pyarrow schemas plus SCD2 columns.

ABN_MAIN_HISTORY_SCHEMA = Schema(
    NestedField(1, "abn", StringType(), required=True),
    NestedField(2, "abn_status", StringType()),
    NestedField(3, "abn_status_from_date", DateType()),
    NestedField(4, "record_last_updated", DateType()),
    NestedField(5, "replaced", StringType()),
    NestedField(6, "entity_type_ind", StringType()),
    NestedField(7, "entity_type_text", StringType()),
    NestedField(8, "entity_kind", StringType()),
    NestedField(9, "main_name", StringType()),
    NestedField(10, "main_name_type", StringType()),
    NestedField(11, "individual_title", StringType()),
    NestedField(12, "individual_given_names", StringType()),
    NestedField(13, "individual_family_name", StringType()),
    NestedField(14, "individual_name_type", StringType()),
    NestedField(15, "state", StringType()),
    NestedField(16, "postcode", StringType()),
    NestedField(17, "asic_number", StringType()),
    NestedField(18, "asic_number_type", StringType()),
    NestedField(19, "gst_status", StringType()),
    NestedField(20, "gst_status_from_date", DateType()),
    NestedField(21, "valid_from", DateType(), required=True),
    NestedField(22, "valid_to", DateType()),
    NestedField(23, "snapshot_observed_date", DateType(), required=True),
)

ABN_TRADING_HISTORY_SCHEMA = Schema(
    NestedField(1, "abn", StringType(), required=True),
    NestedField(2, "name", StringType(), required=True),
    NestedField(3, "name_type", StringType()),
    NestedField(4, "valid_from", DateType(), required=True),
    NestedField(5, "valid_to", DateType()),
    NestedField(6, "snapshot_observed_date", DateType(), required=True),
)

ABN_DGR_HISTORY_SCHEMA = Schema(
    NestedField(1, "abn", StringType(), required=True),
    NestedField(2, "dgr_status_from_date", DateType()),
    NestedField(3, "dgr_status", StringType()),
    NestedField(4, "dgr_name", StringType()),
    NestedField(5, "valid_from", DateType(), required=True),
    NestedField(6, "valid_to", DateType()),
    NestedField(7, "snapshot_observed_date", DateType(), required=True),
)


TABLE_SCHEMAS: dict[str, Schema] = {
    "abn_main_history": ABN_MAIN_HISTORY_SCHEMA,
    "abn_trading_names_history": ABN_TRADING_HISTORY_SCHEMA,
    "abn_dgr_history": ABN_DGR_HISTORY_SCHEMA,
}


@dataclass
class IcebergSettings:
    catalog_uri: str
    warehouse: str
    token: str | None
    s3_endpoint: str
    s3_access_key_id: str
    s3_secret_access_key: str
    namespace: str = "abr"


def load_iceberg_settings_from_env() -> IcebergSettings:
    required = (
        "R2_ACCOUNT_ID",
        "R2_CATALOG_ACCESS_ID_KEY",
        "R2_CATALOG_SECRET_ACCESS_KEY",
    )
    missing = [v for v in required if not os.environ.get(v)]
    if missing:
        raise RuntimeError(f"Missing required env vars: {', '.join(missing)}")

    account = os.environ["R2_ACCOUNT_ID"]
    bucket = os.environ.get("R2_BUCKET", "weekly-abr-dataset")
    return IcebergSettings(
        catalog_uri=os.environ.get(
            "R2_CATALOG_URI",
            f"https://catalog.cloudflarestorage.com/{account}/{bucket}",
        ),
        warehouse=os.environ.get("R2_CATALOG_WAREHOUSE", f"{account}_{bucket}"),
        token=os.environ.get("R2_CATALOG_TOKEN"),
        s3_endpoint=f"https://{account}.r2.cloudflarestorage.com",
        s3_access_key_id=os.environ["R2_CATALOG_ACCESS_ID_KEY"],
        s3_secret_access_key=os.environ["R2_CATALOG_SECRET_ACCESS_KEY"],
    )


def connect_r2_catalog(settings: IcebergSettings) -> Catalog:
    config: dict[str, Any] = {
        "type": "rest",
        "uri": settings.catalog_uri,
        "warehouse": settings.warehouse,
        "s3.endpoint": settings.s3_endpoint,
        "s3.access-key-id": settings.s3_access_key_id,
        "s3.secret-access-key": settings.s3_secret_access_key,
        "s3.region": "auto",
    }
    if settings.token:
        config["token"] = settings.token
    return load_catalog("abr", **config)


def connect_local_catalog(warehouse_dir: Path) -> Catalog:
    """SQLite-backed catalog with a filesystem warehouse — for tests only."""
    warehouse_dir.mkdir(parents=True, exist_ok=True)
    return load_catalog(
        "abr_local",
        **{
            "type": "sql",
            "uri": f"sqlite:///{warehouse_dir / 'catalog.db'}",
            "warehouse": f"file://{warehouse_dir.resolve()}",
        },
    )


def ensure_namespace(catalog: Catalog, namespace: str) -> None:
    if (namespace,) not in catalog.list_namespaces():
        catalog.create_namespace(namespace)


def ensure_table(catalog: Catalog, namespace: str, name: str, schema: Schema) -> Table:
    full = f"{namespace}.{name}"
    try:
        return catalog.load_table(full)
    except NoSuchTableError:
        return catalog.create_table(
            full,
            schema=schema,
            partition_spec=UNPARTITIONED_PARTITION_SPEC,
        )


def write_history(table: Table, df: pl.DataFrame, *, schema: Schema) -> None:
    """Replace the table contents with df.

    Simple semantics: overwrite the whole table each weekly run. The SCD2
    logic in history.py produces the full reconciled DataFrame, so this is
    correct. Iceberg snapshot history gives us coarse table-level time
    travel; the valid_from / valid_to columns give row-level history.
    """
    arrow_schema = schema.as_arrow()
    arrow = df.to_arrow().cast(arrow_schema)
    table.overwrite(arrow)


def ensure_history_tables(catalog: Catalog, namespace: str = "abr") -> dict[str, Table]:
    ensure_namespace(catalog, namespace)
    return {
        name: ensure_table(catalog, namespace, name, schema)
        for name, schema in TABLE_SCHEMAS.items()
    }


def update_history_table(
    table: Table,
    snapshot_df: pl.DataFrame,
    extract_date,
    *,
    bootstrap_fn,
    apply_fn,
    schema: Schema,
) -> dict:
    """Read prev history from Iceberg, apply bootstrap or SCD2 update, write back.

    Returns a small summary dict for the manifest.
    """
    table.refresh()
    try:
        prev = pl.from_arrow(table.scan().to_arrow())
    except Exception:
        prev = pl.DataFrame()

    if prev.height == 0:
        history = bootstrap_fn(snapshot_df, extract_date)
    else:
        history = apply_fn(prev, snapshot_df, extract_date)

    write_history(table, history, schema=schema)

    open_rows = history.filter(pl.col("valid_to").is_null()).height
    return {
        "total_rows": history.height,
        "open_rows": open_rows,
        "closed_rows": history.height - open_rows,
        "bootstrap": prev.height == 0,
    }


# Iceberg snapshot manifest (M10) -----------------------------------------------

def _s3_to_public_url(s3_path: str, bucket: str, public_base_url: str) -> str:
    """Translate an Iceberg data-file ``s3://<bucket>/...`` path to the public CDN URL."""
    s3_prefix = f"s3://{bucket}/"
    if s3_path.startswith(s3_prefix):
        return public_base_url.rstrip("/") + "/" + s3_path[len(s3_prefix):]
    return s3_path


def build_snapshot_manifest(
    tables: dict[str, Table],
    *,
    namespace: str,
    generated_at: str,
    extract_time: str,
    bucket: str,
    public_base_url: str,
) -> dict:
    """Build the ``iceberg-snapshot.json`` payload for a set of history tables.

    For each table this enumerates the live data files via
    ``table.scan().plan_files()``, translates ``s3://`` paths to public URLs,
    sums the per-file ``record_count`` for a row total, and records the
    current snapshot id.

    The frontend consumes this so it can read DuckDB-WASM ``read_parquet``
    against the live Iceberg data files. ``current_view_filter`` tells the
    UI which SCD2 clause to apply for the "currently valid" view of each
    history relation (``valid_to IS NULL``).
    """
    out: dict = {
        "generated_at": generated_at,
        "iceberg_namespace": namespace,
        "extract_time": extract_time,
        "tables": {},
    }
    for table_name, table in tables.items():
        table.refresh()
        snap = table.current_snapshot()
        snap_id = snap.snapshot_id if snap is not None else None

        plan = list(table.scan().plan_files())
        urls: list[str] = []
        row_count = 0
        for task in plan:
            urls.append(_s3_to_public_url(task.file.file_path, bucket, public_base_url))
            row_count += int(task.file.record_count or 0)

        out["tables"][table_name] = {
            "snapshot_id": snap_id,
            "data_files": urls,
            "row_count": row_count,
            "current_view_filter": "valid_to IS NULL",
        }
    return out
