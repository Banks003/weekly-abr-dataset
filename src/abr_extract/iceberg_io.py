"""Iceberg + R2 Data Catalog wiring.

Production target: R2 Data Catalog (REST catalog + R2 object storage). Local
tests use PyIceberg's SQL catalog backed by SQLite + a temp directory, so
the overwrite + snapshot management are tested without touching R2.

Each weekly refresh does a single ``table.overwrite`` per relation. The
table content is the *current state* of the ABR extract — no SCD2 columns,
no row-level history. Iceberg's own snapshot history (one snapshot per
weekly overwrite, retained per ``prune_iceberg_snapshots(keep=...)``) is
the time-travel mechanism: ``SELECT * FROM table FOR SYSTEM_VERSION AS OF
<snapshot_id>`` answers "what did this look like on date X" at
table-granularity, which covers the use cases that the dropped SCD2
columns used to cover at row-granularity.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

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

# Schemas mirror schema.py's pyarrow schemas — same columns, no SCD2.
# The "_history" suffix on the table names is preserved (Iceberg snapshot
# history *is* the history at this point).

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
)

ABN_TRADING_HISTORY_SCHEMA = Schema(
    NestedField(1, "abn", StringType(), required=True),
    NestedField(2, "name", StringType(), required=True),
    NestedField(3, "name_type", StringType()),
)

ABN_DGR_HISTORY_SCHEMA = Schema(
    NestedField(1, "abn", StringType(), required=True),
    NestedField(2, "dgr_status_from_date", DateType()),
    NestedField(3, "dgr_status", StringType()),
    NestedField(4, "dgr_name", StringType()),
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


def _schemas_match(existing: Schema, target: Schema) -> bool:
    """True if both schemas have the same set of (name, field type, required)."""
    e = {(f.name, str(f.field_type), f.required) for f in existing.fields}
    t = {(f.name, str(f.field_type), f.required) for f in target.fields}
    return e == t


def ensure_table(catalog: Catalog, namespace: str, name: str, schema: Schema) -> Table:
    """Load or create the named table; drop and recreate on schema mismatch.

    The schema-mismatch detection makes the SCD2 → current-state migration
    automatic on first run. An existing ``abn_main_history`` table from
    the SCD2 era will have ``valid_from`` / ``valid_to`` /
    ``snapshot_observed_date`` fields that the new schema lacks —
    ``_schemas_match`` returns False, the catalog drops the old table,
    and a fresh table is created with the new schema. Iceberg snapshot
    history starts fresh from the next overwrite.
    """
    full = f"{namespace}.{name}"
    try:
        existing = catalog.load_table(full)
    except NoSuchTableError:
        return catalog.create_table(
            full, schema=schema, partition_spec=UNPARTITIONED_PARTITION_SPEC
        )
    if _schemas_match(existing.schema(), schema):
        return existing
    catalog.drop_table(full)
    return catalog.create_table(
        full, schema=schema, partition_spec=UNPARTITIONED_PARTITION_SPEC
    )


def ensure_history_tables(catalog: Catalog, namespace: str = "abr") -> dict[str, Table]:
    ensure_namespace(catalog, namespace)
    return {
        name: ensure_table(catalog, namespace, name, schema)
        for name, schema in TABLE_SCHEMAS.items()
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
    """Build the ``iceberg-snapshot.json`` payload for a set of tables.

    For each table this enumerates the live data files via
    ``table.scan().plan_files()``, translates ``s3://`` paths to public URLs,
    sums the per-file ``record_count`` for a row total, and records the
    current snapshot id.

    The frontend consumes this so it can read DuckDB-WASM ``read_parquet``
    against the live Iceberg data files. Tables are now current-state-only
    (no SCD2), so no per-table SCD2 view filter is needed.
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
        }
    return out


# Overwrite path -------------------------------------------------------------

def overwrite_table_from_parquet(
    table: Table,
    parquet_path,
) -> dict:
    """Overwrite an Iceberg table with the contents of a parquet file.

    Reads the parquet to a pyarrow Table (cheaper than the polars
    round-trip — pyarrow holds the column-oriented buffers near the
    parquet native format), casts to the iceberg arrow schema, and calls
    ``table.overwrite`` exactly once. Single iceberg snapshot per call.

    Memory peaks at roughly the size of the parquet decompressed
    (~1-2 GB for the main relation).
    """
    import pyarrow.parquet as pq

    arrow_table = pq.read_table(parquet_path)
    n = arrow_table.num_rows
    arrow_table = arrow_table.cast(table.schema().as_arrow())
    table.overwrite(arrow_table)
    return {"rows_written": n}


# Snapshot pruning ----------------------------------------------------------
#
# Iceberg's ``table.overwrite`` writes a fresh snapshot pointing at new
# data files; the previous snapshot's data files stay on R2 indefinitely
# unless we explicitly expire and clean them up.
#
# ``expire_snapshots`` here is a *metadata* operation: it removes expired
# snapshots from the table's snapshot log so they're no longer reachable
# via time-travel. Physically deleting the orphaned data files from R2
# requires a separate listing pass (PyIceberg 0.11 has no built-in
# orphan-file remover); see the cleanup tracker. Running
# ``expire_snapshots`` on every refresh keeps the snapshot log bounded.
#
# Now that Iceberg snapshots *are* the time-travel mechanism (no SCD2),
# we keep more of them — the default has been bumped to 26 (six months
# of weekly snapshots).

DEFAULT_SNAPSHOT_KEEP = 26


def prune_iceberg_snapshots(
    table: Table,
    *,
    keep: int = DEFAULT_SNAPSHOT_KEEP,
    older_than: timedelta | None = None,
) -> dict:
    """Expire snapshots beyond the most-recent ``keep`` from the table metadata.

    Always-protected snapshots (the current branch HEAD) are skipped by
    PyIceberg's expire-snapshots machinery, so we don't need to filter
    them out explicitly. ``older_than`` adds a grace period: a snapshot
    is only eligible for expiry if it is also older than the supplied
    duration (defaults to no grace, i.e. expire immediately once we have
    ``keep`` newer snapshots).
    """
    table.refresh()
    snapshots = list(table.metadata.snapshots)
    if len(snapshots) <= keep:
        return {"expired": 0, "kept": len(snapshots)}

    snapshots.sort(key=lambda s: s.timestamp_ms, reverse=True)
    candidates = snapshots[keep:]

    if older_than is not None:
        cutoff_ms = int(
            (datetime.now(tz=UTC) - older_than).timestamp() * 1000
        )
        candidates = [s for s in candidates if s.timestamp_ms < cutoff_ms]

    if not candidates:
        return {"expired": 0, "kept": len(snapshots)}

    expirer = table.maintenance.expire_snapshots()
    for snap in candidates:
        try:
            expirer = expirer.by_id(snap.snapshot_id)
        except ValueError:
            # Protected (branch HEAD / tag) — silently skip; the maintenance
            # API would refuse to expire these anyway.
            continue
    expirer.commit()

    return {"expired": len(candidates), "kept": len(snapshots) - len(candidates)}
