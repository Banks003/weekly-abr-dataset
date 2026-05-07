"""Streaming writers for Parquet (per-table) and SQLite (one file, three tables).

Writers accept RawRecord instances one at a time and flush in batches. The
WriterBundle composes all four outputs so a single pass over the parser
populates everything.
"""

from __future__ import annotations

import hashlib
import json
import sqlite3
from dataclasses import dataclass
from datetime import date
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


def _sqlite_value(value):
    if isinstance(value, date):
        return value.isoformat()
    return value


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
    """Single-pass writer composing 3 Parquet files + a SQLite database."""

    def __init__(self, paths: WriterPaths, batch_size: int = 50_000):
        self.paths = paths
        self.batch_size = batch_size

        paths.main_parquet.parent.mkdir(parents=True, exist_ok=True)

        self._main_writer = _ParquetBatchWriter(paths.main_parquet, ABN_MAIN_SCHEMA)
        self._trading_writer = _ParquetBatchWriter(paths.trading_parquet, ABN_TRADING_NAMES_SCHEMA)
        self._dgr_writer = _ParquetBatchWriter(paths.dgr_parquet, ABN_DGR_SCHEMA)

        self._sql_conn = sqlite3.connect(paths.sqlite_db)
        for stmt in SQLITE_DDL:
            self._sql_conn.execute(stmt)
        self._sql_conn.commit()

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
            self._sql_conn.executemany(
                _sqlite_main_insert(),
                [_main_row_tuple(r) for r in self._main_buffer],
            )
            self.counts.main += len(self._main_buffer)
            self._main_buffer.clear()

        if self._trading_buffer:
            self._trading_writer.write(self._trading_buffer)
            self._sql_conn.executemany(
                "INSERT INTO abn_trading_names (abn, name, name_type) VALUES (?, ?, ?)",
                [(r["abn"], r["name"], r["name_type"]) for r in self._trading_buffer],
            )
            self.counts.trading += len(self._trading_buffer)
            self._trading_buffer.clear()

        if self._dgr_buffer:
            self._dgr_writer.write(self._dgr_buffer)
            self._sql_conn.executemany(
                "INSERT INTO abn_dgr (abn, dgr_status_from_date, dgr_status, dgr_name) "
                "VALUES (?, ?, ?, ?)",
                [
                    (
                        r["abn"],
                        _sqlite_value(r["dgr_status_from_date"]),
                        r["dgr_status"],
                        r["dgr_name"],
                    )
                    for r in self._dgr_buffer
                ],
            )
            self.counts.dgr += len(self._dgr_buffer)
            self._dgr_buffer.clear()

        self._sql_conn.commit()

    def close(self) -> None:
        self._flush()
        self._main_writer.close()
        self._trading_writer.close()
        self._dgr_writer.close()
        self._sql_conn.close()


def _main_row_tuple(r: dict) -> tuple:
    return (
        r["abn"],
        r["abn_status"],
        _sqlite_value(r["abn_status_from_date"]),
        _sqlite_value(r["record_last_updated"]),
        r["replaced"],
        r["entity_type_ind"],
        r["entity_type_text"],
        r["entity_kind"],
        r["main_name"],
        r["main_name_type"],
        r["individual_title"],
        r["individual_given_names"],
        r["individual_family_name"],
        r["individual_name_type"],
        r["state"],
        r["postcode"],
        r["asic_number"],
        r["asic_number_type"],
        r["gst_status"],
        _sqlite_value(r["gst_status_from_date"]),
    )


def _sqlite_main_insert() -> str:
    cols = [name for name in ABN_MAIN_SCHEMA.names]
    placeholders = ", ".join(["?"] * len(cols))
    return f"INSERT OR REPLACE INTO abn_main ({', '.join(cols)}) VALUES ({placeholders})"


# Manifest builder ---------------------------------------------------------

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
    files = []
    for label, p in (
        ("abn_main_parquet", paths.main_parquet),
        ("abn_trading_names_parquet", paths.trading_parquet),
        ("abn_dgr_parquet", paths.dgr_parquet),
        ("sqlite", paths.sqlite_db),
    ):
        files.append(
            {
                "label": label,
                "filename": p.name,
                "size_bytes": p.stat().st_size,
                "sha256": sha256_of(p),
            }
        )
    return {
        "pipeline_run_id": pipeline_run_id,
        "generated_at": generated_at,
        "extract_time": extract_time,
        "row_counts": {
            "abn_main": counts.main,
            "abn_trading_names": counts.trading,
            "abn_dgr": counts.dgr,
        },
        "files": files,
    }


def write_manifest(manifest: dict, path: Path) -> None:
    path.write_text(json.dumps(manifest, indent=2, sort_keys=True), encoding="utf-8")


def write_manifest_to(manifest: dict, fp: IO[str]) -> None:
    json.dump(manifest, fp, indent=2, sort_keys=True)
