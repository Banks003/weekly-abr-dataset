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
from datetime import UTC, datetime, timedelta
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


# Bootstrap (empty-table) path ----------------------------------------------

def bootstrap_history_table_arrow(
    table: Table,
    parquet_path,
    extract_date,
    *,
    record_last_updated_column: str | None = "record_last_updated",
) -> dict:
    """One-shot bootstrap an Iceberg history table from a snapshot Parquet.

    Reads the whole snapshot parquet to a pyarrow Table (cheaper than the
    polars round-trip — pyarrow holds the column-oriented buffers near
    the parquet native format, while polars copies them into its own
    representation, roughly doubling memory). Adds the three SCD2 columns
    via zero-copy ``append_column`` calls, casts once to the iceberg
    arrow schema, and calls ``table.overwrite`` exactly once — a single
    iceberg snapshot, no per-batch R2 commit overhead.

    Memory peaks at roughly the size of the snapshot parquet decompressed
    (~1-2GB for the main relation), which is half the polars path's peak
    and well within the 16GB ubuntu-latest runner''s headroom.
    """
    import pyarrow as pa
    import pyarrow.compute as pc
    import pyarrow.parquet as pq

    arrow_table = pq.read_table(parquet_path)
    n = arrow_table.num_rows

    extract_date_array = pa.array([extract_date] * n, type=pa.date32())
    null_date_array = pa.nulls(n, type=pa.date32())

    if (
        record_last_updated_column
        and record_last_updated_column in arrow_table.column_names
    ):
        valid_from = pc.coalesce(
            arrow_table.column(record_last_updated_column),
            extract_date_array,
        )
    else:
        valid_from = extract_date_array

    arrow_table = arrow_table.append_column("valid_from", valid_from)
    arrow_table = arrow_table.append_column("valid_to", null_date_array)
    arrow_table = arrow_table.append_column(
        "snapshot_observed_date", extract_date_array
    )
    arrow_table = arrow_table.cast(table.schema().as_arrow())

    table.overwrite(arrow_table)

    return {"mode": "bootstrap", "rows_written": n, "batches": 1}


# Arrow + DuckDB SCD2 update path -------------------------------------------


def _scd2_update_sql(
    *,
    base_columns: list[str],
    identity: list[str],
    content_columns: list[str],
) -> str:
    """Build the SCD2 reconciliation SQL for one history table.

    Inputs registered in the DuckDB connection: ``prev`` (full prior
    history with SCD2 columns) and ``snapshot`` (the new weekly snapshot,
    base columns only). Bind parameters in order: ``valid_to`` for closed
    rows, ``valid_from`` and ``snapshot_observed_date`` for new versions.
    """
    base_select = ", ".join(base_columns)
    id_join = " AND ".join(f'po."{c}" = n."{c}"' for c in identity)
    id_using = ", ".join(f'"{c}"' for c in identity)
    id_first = f'"{identity[0]}"'

    if content_columns:
        content_eq = " AND ".join(
            f'po."{c}" IS NOT DISTINCT FROM n."{c}"' for c in content_columns
        )
    else:
        # Pure set-membership tables (trading_names): identity match is
        # always content-equal.
        content_eq = "TRUE"

    return f"""
    WITH
    prev_open AS (SELECT * FROM prev WHERE valid_to IS NULL),
    prev_closed AS (SELECT * FROM prev WHERE valid_to IS NOT NULL),
    key_action AS (
      SELECT
        {", ".join(f'coalesce(po."{c}", n."{c}") AS "{c}"' for c in identity)},
        CASE
          WHEN po.{id_first} IS NULL THEN 'added'
          WHEN n.{id_first} IS NULL THEN 'removed'
          WHEN ({content_eq}) THEN 'unchanged'
          ELSE 'changed'
        END AS _action
      FROM prev_open po
      FULL OUTER JOIN snapshot n ON {id_join}
    )
    SELECT * FROM prev_closed
    UNION ALL BY NAME
    SELECT po.* FROM prev_open po
      JOIN key_action ka USING ({id_using})
      WHERE ka._action = 'unchanged'
    UNION ALL BY NAME
    SELECT po.* REPLACE (CAST(? AS DATE) AS valid_to)
      FROM prev_open po
      JOIN key_action ka USING ({id_using})
      WHERE ka._action IN ('changed', 'removed')
    UNION ALL BY NAME
    SELECT
      {base_select},
      CAST(? AS DATE) AS valid_from,
      CAST(NULL AS DATE) AS valid_to,
      CAST(? AS DATE) AS snapshot_observed_date
    FROM snapshot n
      JOIN key_action ka USING ({id_using})
      WHERE ka._action IN ('changed', 'added')
    """


def update_history_table_arrow(
    table: Table,
    snapshot_parquet_path,
    extract_date,
    *,
    base_columns: list[str],
    identity: list[str],
    content_columns: list[str],
    duckdb_memory_limit: str = "8GB",
    duckdb_temp_dir: str | None = None,
) -> dict:
    """SCD2 update via Arrow + DuckDB. No Polars round-trip.

    Reads prior history from the Iceberg table as an Arrow record-batch
    reader, reads the new snapshot parquet directly with DuckDB, executes
    the SCD2 reconciliation SQL, and writes the result back via
    ``table.overwrite``.
    """
    import duckdb

    table.refresh()
    prev_reader = table.scan().to_arrow_batch_reader()

    con = duckdb.connect(":memory:")
    try:
        con.execute(f"PRAGMA memory_limit='{duckdb_memory_limit}'")
        if duckdb_temp_dir:
            con.execute(f"PRAGMA temp_directory='{duckdb_temp_dir}'")
        con.register("prev", prev_reader)
        # Materialise the snapshot via DuckDB's native parquet reader so the
        # SCD2 query can reference it twice (key_action + final SELECT)
        # without re-decoding the file.
        con.execute(
            "CREATE TEMP TABLE snapshot AS "
            "SELECT * FROM read_parquet(?)",
            [str(snapshot_parquet_path)],
        )

        sql = _scd2_update_sql(
            base_columns=base_columns,
            identity=identity,
            content_columns=content_columns,
        )
        result = con.execute(sql, [extract_date, extract_date, extract_date]).to_arrow_table()
    finally:
        con.close()

    arrow = result.cast(table.schema().as_arrow())
    n = arrow.num_rows
    table.overwrite(arrow)

    return {"mode": "update", "rows_written": n}


# Snapshot pruning ----------------------------------------------------------
#
# Iceberg's ``table.overwrite`` writes a fresh snapshot pointing at new
# data files; the previous snapshot's data files stay on R2 indefinitely
# unless we explicitly expire and clean them up. Without pruning, R2
# storage grows by ~770MB (main) + smaller (trading + dgr) every week
# even though the live row count is constant.
#
# ``expire_snapshots`` here is a *metadata* operation: it removes expired
# snapshots from the table's snapshot log so they're no longer reachable
# via time-travel. Physically deleting the orphaned data files from R2
# requires a separate listing pass (PyIceberg 0.11 has no built-in
# orphan-file remover); see docs in the cleanup issue for the one-shot
# script. Running ``expire_snapshots`` on every refresh keeps the
# snapshot log bounded so future orphan-file passes don't have to chase
# a runaway list.

DEFAULT_SNAPSHOT_KEEP = 2


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
