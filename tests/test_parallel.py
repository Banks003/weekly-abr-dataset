"""Tests for the parallel parse pipeline (M6).

We verify two things against the sequential WriterBundle reference:
1. Row counts match exactly across all three relations.
2. After sort-by-ABN the merged Parquet content is identical (schema and
   values), per the issue's acceptance criteria.

SQLite materialisation from the merged Parquet is covered separately in
test_write.py via the M7 ``materialize_sqlite`` flow.

The parallel module also exposes a single-process short-circuit
(workers <= 1) which lets these tests run without spawning subprocesses,
avoiding fixture-pickling complications.
"""

from __future__ import annotations

import zipfile
from pathlib import Path

import pyarrow.parquet as pq
import pytest

from abr_extract.parallel import (
    merge_shards_to_parquet,
    parse_zips_parallel,
)
from abr_extract.parse import parse_records
from abr_extract.schema import (
    ABN_DGR_SCHEMA,
    ABN_MAIN_SCHEMA,
    ABN_TRADING_NAMES_SCHEMA,
)
from abr_extract.write import WriterBundle, WriterPaths

SAMPLE_XML = Path(__file__).parent / "fixtures" / "sample_records.xml"


def _make_zip(path: Path, members: list[tuple[str, bytes]]) -> Path:
    with zipfile.ZipFile(path, "w", zipfile.ZIP_DEFLATED) as z:
        for name, data in members:
            z.writestr(name, data)
    return path


def _run_sequential(zip_paths: list[Path], paths: WriterPaths) -> None:
    with WriterBundle(paths) as wb:
        for zp in zip_paths:
            with zipfile.ZipFile(zp) as zf:
                for info in zf.infolist():
                    if not info.filename.endswith(".xml"):
                        continue
                    with zf.open(info) as f:
                        for record in parse_records(f):
                            wb.write(record)


def _seq_paths(root: Path) -> WriterPaths:
    root.mkdir(parents=True, exist_ok=True)
    return WriterPaths(
        main_parquet=root / "abn_main.parquet",
        trading_parquet=root / "abn_trading_names.parquet",
        dgr_parquet=root / "abn_dgr.parquet",
        sqlite_db=root / "abr.sqlite",
    )


@pytest.fixture
def sample_zips(tmp_path: Path) -> list[Path]:
    """Two zips, three inner XML files total — enough to exercise the
    multi-zip enumeration and the merge step across more than one shard."""
    data = SAMPLE_XML.read_bytes()
    return [
        _make_zip(
            tmp_path / "src1.zip",
            [("public01.xml", data), ("public02.xml", data)],
        ),
        _make_zip(tmp_path / "src2.zip", [("public03.xml", data)]),
    ]


def test_parallel_row_counts_and_content_match_sequential(
    tmp_path: Path, sample_zips: list[Path]
) -> None:
    seq = _seq_paths(tmp_path / "seq")
    _run_sequential(sample_zips, seq)
    seq_main = pq.read_table(seq.main_parquet)
    seq_trading = pq.read_table(seq.trading_parquet)
    seq_dgr = pq.read_table(seq.dgr_parquet)

    par_root = tmp_path / "par"
    par_root.mkdir()
    shard_dir = par_root / "shards"

    # workers=1 to keep the test in-process — pickling the fixture path
    # across a real subprocess pool isn't necessary to prove the
    # invariants we care about; the spawn path is exercised in CI's
    # full-pipeline runs.
    results = parse_zips_parallel(sample_zips, shard_dir, workers=1)
    assert [r.shard_id for r in results] == [0, 1, 2]

    par_main_path = par_root / "abn_main.parquet"
    par_trading_path = par_root / "abn_trading_names.parquet"
    par_dgr_path = par_root / "abn_dgr.parquet"

    main_rows = merge_shards_to_parquet(
        [r.paths.main for r in results], par_main_path, ABN_MAIN_SCHEMA
    )
    trading_rows = merge_shards_to_parquet(
        [r.paths.trading for r in results], par_trading_path, ABN_TRADING_NAMES_SCHEMA
    )
    dgr_rows = merge_shards_to_parquet(
        [r.paths.dgr for r in results], par_dgr_path, ABN_DGR_SCHEMA
    )

    par_main = pq.read_table(par_main_path)
    par_trading = pq.read_table(par_trading_path)
    par_dgr = pq.read_table(par_dgr_path)

    # Row counts match between sequential and parallel.
    assert par_main.num_rows == seq_main.num_rows == main_rows
    assert par_trading.num_rows == seq_trading.num_rows == trading_rows
    assert par_dgr.num_rows == seq_dgr.num_rows == dgr_rows

    # Schemas are identical (modulo metadata).
    assert par_main.schema.equals(ABN_MAIN_SCHEMA, check_metadata=False)
    assert par_trading.schema.equals(ABN_TRADING_NAMES_SCHEMA, check_metadata=False)
    assert par_dgr.schema.equals(ABN_DGR_SCHEMA, check_metadata=False)

    # After sort-by-ABN the content is identical (acceptance criterion).
    assert par_main.sort_by([("abn", "ascending")]).equals(
        seq_main.sort_by([("abn", "ascending")])
    )
    # Trading and DGR have multi-row groupings per ABN, so sort by all
    # columns to get a stable, comparable ordering.
    trading_keys = [("abn", "ascending"), ("name_type", "ascending"), ("name", "ascending")]
    assert par_trading.sort_by(trading_keys).equals(seq_trading.sort_by(trading_keys))
    dgr_keys = [
        ("abn", "ascending"),
        ("dgr_status_from_date", "ascending"),
        ("dgr_status", "ascending"),
    ]
    assert par_dgr.sort_by(dgr_keys).equals(seq_dgr.sort_by(dgr_keys))


def test_merge_shards_with_zero_inputs_still_writes_valid_parquet(tmp_path: Path) -> None:
    dest = tmp_path / "empty.parquet"
    rows = merge_shards_to_parquet([], dest, ABN_MAIN_SCHEMA)
    assert rows == 0
    assert pq.read_table(dest).schema.equals(ABN_MAIN_SCHEMA, check_metadata=False)
    assert pq.read_table(dest).num_rows == 0
