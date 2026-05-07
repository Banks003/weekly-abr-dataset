"""Parallel parse: a ProcessPoolExecutor fans out one worker per inner XML
file in the source zips.

The bulk extract is two zips of ten inner XML files apiece (each ~70-90 MB
gzipped, ~600 MB inflated). The 20 inner files are independent — there are
no cross-file references — so they parse cleanly in parallel.

Each worker opens the source zip independently (zipfile is read-only;
multiple readers per file are fine), parses its assigned inner file with
the existing streaming `parse_records`, and writes three per-worker
Parquet shards: `part-{NN}-abn-main.parquet`, `part-{NN}-abn-trading-names.parquet`,
`part-{NN}-abn-dgr.parquet`. After all workers finish, a single-process
merge step concats the shards into the canonical three Parquet files.

Row counts match the sequential WriterBundle output exactly; row contents
match after sorting by ABN. Order within a relation is not guaranteed
across runs — workers complete in non-deterministic order, and no global
ABN sort is enforced.
"""

from __future__ import annotations

import zipfile
from collections.abc import Callable
from concurrent.futures import ProcessPoolExecutor, as_completed
from dataclasses import dataclass, field
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from .parse import parse_records
from .schema import (
    ABN_DGR_SCHEMA,
    ABN_MAIN_SCHEMA,
    ABN_TRADING_NAMES_SCHEMA,
    record_to_dgr_rows,
    record_to_main_row,
    record_to_trading_rows,
)
from .write import WriteCounts

DEFAULT_WORKERS = 4
DEFAULT_BATCH_SIZE = 50_000


@dataclass(frozen=True)
class ShardPaths:
    main: Path
    trading: Path
    dgr: Path


@dataclass(frozen=True)
class _MemberJob:
    """Serialisable description of one worker's task.

    Paths are stored as strings so the dataclass round-trips cleanly across
    the ProcessPoolExecutor pickle boundary.
    """

    zip_path: str
    inner_filename: str
    shard_dir: str
    shard_id: int


@dataclass
class ShardResult:
    shard_id: int
    zip_path: str
    inner_filename: str
    paths: ShardPaths
    counts: WriteCounts = field(default_factory=WriteCounts)


def _shard_paths(shard_dir: Path, shard_id: int) -> ShardPaths:
    return ShardPaths(
        main=shard_dir / f"part-{shard_id:02d}-abn-main.parquet",
        trading=shard_dir / f"part-{shard_id:02d}-abn-trading-names.parquet",
        dgr=shard_dir / f"part-{shard_id:02d}-abn-dgr.parquet",
    )


def _parse_member_to_shards(job: _MemberJob) -> ShardResult:
    """Worker entry point: parse one inner XML, write three Parquet shards."""
    shard_dir = Path(job.shard_dir)
    shard_dir.mkdir(parents=True, exist_ok=True)
    paths = _shard_paths(shard_dir, job.shard_id)

    main_writer = pq.ParquetWriter(paths.main, ABN_MAIN_SCHEMA, compression="snappy")
    trading_writer = pq.ParquetWriter(
        paths.trading, ABN_TRADING_NAMES_SCHEMA, compression="snappy"
    )
    dgr_writer = pq.ParquetWriter(paths.dgr, ABN_DGR_SCHEMA, compression="snappy")
    counts = WriteCounts()

    main_buf: list[dict] = []
    trading_buf: list[dict] = []
    dgr_buf: list[dict] = []

    def _flush_main() -> None:
        if not main_buf:
            return
        main_writer.write_table(pa.Table.from_pylist(main_buf, schema=ABN_MAIN_SCHEMA))
        counts.main += len(main_buf)
        main_buf.clear()

    def _flush_trading() -> None:
        if not trading_buf:
            return
        trading_writer.write_table(
            pa.Table.from_pylist(trading_buf, schema=ABN_TRADING_NAMES_SCHEMA)
        )
        counts.trading += len(trading_buf)
        trading_buf.clear()

    def _flush_dgr() -> None:
        if not dgr_buf:
            return
        dgr_writer.write_table(pa.Table.from_pylist(dgr_buf, schema=ABN_DGR_SCHEMA))
        counts.dgr += len(dgr_buf)
        dgr_buf.clear()

    try:
        with zipfile.ZipFile(job.zip_path) as z, z.open(job.inner_filename) as f:
            for record in parse_records(f):
                main_buf.append(record_to_main_row(record))
                trading_buf.extend(record_to_trading_rows(record))
                dgr_buf.extend(record_to_dgr_rows(record))
                if len(main_buf) >= DEFAULT_BATCH_SIZE:
                    _flush_main()
                    _flush_trading()
                    _flush_dgr()
        _flush_main()
        _flush_trading()
        _flush_dgr()
    finally:
        main_writer.close()
        trading_writer.close()
        dgr_writer.close()

    return ShardResult(
        shard_id=job.shard_id,
        zip_path=job.zip_path,
        inner_filename=job.inner_filename,
        paths=paths,
        counts=counts,
    )


def _enumerate_jobs(zip_paths: list[Path], shard_dir: Path) -> list[_MemberJob]:
    jobs: list[_MemberJob] = []
    next_id = 0
    for zip_path in zip_paths:
        with zipfile.ZipFile(zip_path) as z:
            for info in sorted(z.infolist(), key=lambda i: i.filename):
                if not info.filename.endswith(".xml"):
                    continue
                jobs.append(
                    _MemberJob(
                        zip_path=str(zip_path),
                        inner_filename=info.filename,
                        shard_dir=str(shard_dir),
                        shard_id=next_id,
                    )
                )
                next_id += 1
    return jobs


def parse_zips_parallel(
    zip_paths: list[Path],
    shard_dir: Path,
    *,
    workers: int = DEFAULT_WORKERS,
    progress: Callable[[ShardResult], None] | None = None,
) -> list[ShardResult]:
    """Fan out parsing of every inner XML file across `workers` processes.

    Returns one ShardResult per inner file, sorted by shard_id (which mirrors
    inner-file enumeration order across the input zips). Total row counts
    summed across results match what the sequential WriterBundle would
    produce on the same input.
    """
    shard_dir.mkdir(parents=True, exist_ok=True)
    jobs = _enumerate_jobs(zip_paths, shard_dir)
    if not jobs:
        return []

    results: list[ShardResult] = []
    # workers=1 short-circuit makes unit-testing easy and avoids the small
    # but real overhead of spawning a subprocess for trivial inputs.
    if workers <= 1 or len(jobs) == 1:
        for job in jobs:
            r = _parse_member_to_shards(job)
            results.append(r)
            if progress is not None:
                progress(r)
        return results

    with ProcessPoolExecutor(max_workers=workers) as ex:
        futures = {ex.submit(_parse_member_to_shards, j): j for j in jobs}
        for fut in as_completed(futures):
            r = fut.result()
            results.append(r)
            if progress is not None:
                progress(r)

    results.sort(key=lambda r: r.shard_id)
    return results


def merge_shards_to_parquet(
    shard_paths: list[Path], dest: Path, schema: pa.Schema
) -> int:
    """Concat per-worker Parquet shards into a single canonical output.

    `pyarrow.concat_tables` requires the shards share an exact schema,
    which they do — every worker writes against the same module-level
    schema constant. Returns the merged row count.

    With zero shards we still emit a valid Parquet file with the correct
    schema, so downstream readers don't need to special-case "table is
    missing".
    """
    dest.parent.mkdir(parents=True, exist_ok=True)
    if not shard_paths:
        empty = pa.Table.from_pylist([], schema=schema)
        with pq.ParquetWriter(dest, schema, compression="snappy") as w:
            w.write_table(empty)
        return 0

    tables = [pq.read_table(p, schema=schema) for p in shard_paths]
    merged = pa.concat_tables(tables)
    with pq.ParquetWriter(dest, schema, compression="snappy") as w:
        w.write_table(merged)
    return merged.num_rows


# SQLite materialisation has moved to abr_extract.write.materialize_sqlite,
# which uses DuckDB's sqlite_scanner extension instead of sqlite3.executemany
# for ~10x throughput over the merged Parquet outputs.
