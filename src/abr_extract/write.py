"""Streaming Parquet writer (per-table) and a DuckDB-driven SQLite materialiser.

WriterBundle is now Parquet-only: each call to ``write(record)`` appends to
three streaming pyarrow ParquetWriters. The SQLite mirror is no longer
written inline; it''s materialised as a separate post-parse step via
:func:`materialize_sqlite`, which uses DuckDB''s ``sqlite_scanner`` extension
to ``INSERT INTO ... SELECT * FROM read_parquet(...)`` at near-disk speed.

Why the split: ``sqlite3.executemany`` over 20M rows in Python is slow
(language-boundary overhead, single-threaded). DuckDB writes via the SQLite
C API in batches with no per-row Python round-trip. The on-disk shape of
the resulting database is identical: same three tables, same columns, same
indices, dates as ISO ``YYYY-MM-DD`` strings.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from pathlib import Path
from types import TracebackType
from typing import IO

import pyarrow as pa
import pyarrow.parquet as pq

from .parse import RawRecord
from .schema import (
    ABN_DGR_SCHEMA,
    ABN_MAIN_SCHEMA,
    ABN_TRADING_NAMES_SCHEMA,
    SQLITE_DDL,
    record_to_dgr_rows,
    record_to_main_row,
    record_to_trading_rows,
)


@dataclass
class WriterPaths:
    main_parquet: Path
    trading_parquet: Path
    dgr_parquet: Path
    sqlite_db: Path


@dataclass
class WriteCounts:
    main: int = 0
    trading: int = 0
    dgr: int = 0


class _ParquetBatchWriter:
    def __init__(self, path: Path, schema: pa.Schema, compression: str = "snappy"):
        self.path = path
        self.schema = schema
        self._writer = pq.ParquetWriter(path, schema, compression=compression)
        self._rows_written = 0

    def write(self, rows: list[dict]) -> None:
        if not rows:
            return
        table = pa.Table.from_pylist(rows, schema=self.schema)
        self._writer.write_table(table)
        self._rows_written += table.num_rows

    @property
    def rows_written(self) -> int:
        return self._rows_written

    def close(self) -> None:
        self._writer.close()


class WriterBundle:
    """Parquet-only streaming writer.

    Composes the three per-table ParquetWriters so a single pass over the
    parser populates all three relations. SQLite is no longer written
    inline -- call :func:`materialize_sqlite` once at the end of the pipeline.
    """

    def __init__(self, paths: WriterPaths, batch_size: int = 50_000):
        self.paths = paths
        self.batch_size = batch_size

        paths.main_parquet.parent.mkdir(parents=True, exist_ok=True)

        self._main_writer = _ParquetBatchWriter(paths.main_parquet, ABN_MAIN_SCHEMA)
        self._trading_writer = _ParquetBatchWriter(paths.trading_parquet, ABN_TRADING_NAMES_SCHEMA)
        self._dgr_writer = _ParquetBatchWriter(paths.dgr_parquet, ABN_DGR_SCHEMA)

        self._main_buffer: list[dict] = []
        self._trading_buffer: list[dict] = []
        self._dgr_buffer: list[dict] = []
        self.counts = WriteCounts()

    def __enter__(self) -> WriterBundle:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        self.close()

    def write(self, record: RawRecord) -> None:
        self._main_buffer.append(record_to_main_row(record))
        self._trading_buffer.extend(record_to_trading_rows(record))
        self._dgr_buffer.extend(record_to_dgr_rows(record))
        if len(self._main_buffer) >= self.batch_size:
            self._flush()

    def _flush(self) -> None:
        if self._main_buffer:
            self._main_writer.write(self._main_buffer)
            self.counts.main += len(self._main_buffer)
            self._main_buffer.clear()

        if self._trading_buffer:
            self._trading_writer.write(self._trading_buffer)
            self.counts.trading += len(self._trading_buffer)
            self._trading_buffer.clear()

        if self._dgr_buffer:
            self._dgr_writer.write(self._dgr_buffer)
            self.counts.dgr += len(self._dgr_buffer)
            self._dgr_buffer.clear()

    def close(self) -> None:
        self._flush()
        self._main_writer.close()
        self._trading_writer.close()
        self._dgr_writer.close()


# SQLite materialisation -----------------------------------------------------

# DuckDB selects with explicit casts for date columns so they land in
# SQLite as ISO YYYY-MM-DD strings (SQLite has no native DATE type; the
# rest of the codebase -- see query.py -- assumes dates are stored as text).
_MAIN_SELECT = """
SELECT
    abn, abn_status,
    CAST(abn_status_from_date AS VARCHAR) AS abn_status_from_date,
    CAST(record_last_updated   AS VARCHAR) AS record_last_updated,
    replaced, entity_type_ind, entity_type_text, entity_kind,
    main_name, main_name_type,
    individual_title, individual_given_names,
    individual_family_name, individual_name_type,
    state, postcode, asic_number, asic_number_type,
    gst_status,
    CAST(gst_status_from_date AS VARCHAR) AS gst_status_from_date
FROM read_parquet(?)
"""

_TRADING_SELECT = """
SELECT abn, name, name_type
FROM read_parquet(?)
"""

_DGR_SELECT = """
SELECT
    abn,
    CAST(dgr_status_from_date AS VARCHAR) AS dgr_status_from_date,
    dgr_status, dgr_name
FROM read_parquet(?)
"""


def materialize_sqlite(
    main_parquet: Path,
    trading_parquet: Path,
    dgr_parquet: Path,
    sqlite_path: Path,
) -> None:
    """Build the canonical SQLite mirror from the merged Parquet outputs.

    Strategy: pre-create the SQLite schema (DDL with PRIMARY KEY and
    indices), then have DuckDB ``INSERT INTO ... SELECT * FROM
    read_parquet(...)`` via the ``sqlite_scanner`` extension. DuckDB
    does the heavy lifting in C, no per-row Python round-trip -- typically
    > 10x faster than ``sqlite3.executemany`` over 20M rows.

    DuckDB''s CTAS doesn''t preserve constraints, so we always create the
    schema upfront and INSERT into it. That keeps the PRIMARY KEY on
    ``abn_main.abn`` and the three secondary indices.

    If ``sqlite_path`` already exists it''s truncated first so the build is
    repeatable.
    """
    import sqlite3

    import duckdb

    sqlite_path.parent.mkdir(parents=True, exist_ok=True)
    if sqlite_path.exists():
        sqlite_path.unlink()

    # 1) Pre-create the schema with PK + indices.
    conn = sqlite3.connect(sqlite_path)
    try:
        for stmt in SQLITE_DDL:
            conn.execute(stmt)
        conn.commit()
    finally:
        conn.close()

    # 2) Bulk-insert from Parquet via DuckDB''s sqlite_scanner.
    duck = duckdb.connect()
    try:
        duck.execute("INSTALL sqlite_scanner")
        duck.execute("LOAD sqlite_scanner")
        # ATTACH path is interpolated (no SQL placeholder support) -- sqlite_path
        # comes from the pipeline, never from untrusted input.
        duck.execute(f"ATTACH '{sqlite_path.as_posix()}' AS out (TYPE SQLITE)")

        duck.execute(
            f"INSERT INTO out.abn_main {_MAIN_SELECT}",
            [str(main_parquet)],
        )
        duck.execute(
            f"INSERT INTO out.abn_trading_names (abn, name, name_type) {_TRADING_SELECT}",
            [str(trading_parquet)],
        )
        duck.execute(
            f"INSERT INTO out.abn_dgr "
            f"(abn, dgr_status_from_date, dgr_status, dgr_name) {_DGR_SELECT}",
            [str(dgr_parquet)],
        )
    finally:
        duck.close()


# Manifest builder -----------------------------------------------------------


def sha256_of(path: Path, chunk: int = 1 << 20) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        while True:
            buf = f.read(chunk)
            if not buf:
                break
            h.update(buf)
    return h.hexdigest()


def build_manifest(
    paths: WriterPaths,
    counts: WriteCounts,
    extract_time: str,
    pipeline_run_id: str,
    generated_at: str,
) -> dict:
    """Build the manifest metadata.

    Iceberg owns the data; the manifest no longer enumerates snapshot
    files (none are uploaded). Row counts are kept since they''re useful
    operational telemetry. Iceberg-specific details are added to the
    manifest by the caller once the Iceberg step has run.
    """
    return {
        "pipeline_run_id": pipeline_run_id,
        "generated_at": generated_at,
        "extract_time": extract_time,
        "row_counts": {
            "abn_main": counts.main,
            "abn_trading_names": counts.trading,
            "abn_dgr": counts.dgr,
        },
    }


def write_manifest(manifest: dict, path: Path) -> None:
    path.write_text(json.dumps(manifest, indent=2, sort_keys=True), encoding="utf-8")


def write_manifest_to(manifest: dict, fp: IO[str]) -> None:
    json.dump(manifest, fp, indent=2, sort_keys=True)
