from __future__ import annotations

import json
import sqlite3
from datetime import date
from pathlib import Path

import polars as pl
import pyarrow.parquet as pq

from abr_extract.parse import parse_records
from abr_extract.schema import parse_yyyymmdd
from abr_extract.write import (
    WriterBundle,
    WriterPaths,
    build_manifest,
    materialize_sqlite,
    write_manifest,
)

FIXTURE = Path(__file__).parent / "fixtures" / "sample_records.xml"


def _paths(tmp_path: Path) -> WriterPaths:
    return WriterPaths(
        main_parquet=tmp_path / "abn_main.parquet",
        trading_parquet=tmp_path / "abn_trading_names.parquet",
        dgr_parquet=tmp_path / "abn_dgr.parquet",
        sqlite_db=tmp_path / "abr.sqlite",
    )


def _run(tmp_path: Path, batch_size: int = 50) -> WriterPaths:
    """Stream the fixture through WriterBundle (Parquet-only)."""
    paths = _paths(tmp_path)
    with WriterBundle(paths, batch_size=batch_size) as wb:
        for rec in parse_records(FIXTURE):
            wb.write(rec)
    return paths


def _run_with_sqlite(tmp_path: Path, batch_size: int = 50) -> WriterPaths:
    """Stream the fixture through WriterBundle, then materialise SQLite."""
    paths = _run(tmp_path, batch_size=batch_size)
    materialize_sqlite(
        paths.main_parquet, paths.trading_parquet, paths.dgr_parquet, paths.sqlite_db
    )
    return paths


def test_parse_yyyymmdd_handles_sentinel():
    assert parse_yyyymmdd("19000101") == date(1900, 1, 1)
    assert parse_yyyymmdd("20000701") == date(2000, 7, 1)


def test_parse_yyyymmdd_handles_garbage():
    assert parse_yyyymmdd(None) is None
    assert parse_yyyymmdd("") is None
    assert parse_yyyymmdd("abcd1234") is None
    assert parse_yyyymmdd("20139999") is None
    assert parse_yyyymmdd("2024010") is None


def test_main_parquet_row_count_matches_fixture(tmp_path: Path):
    paths = _run(tmp_path)
    table = pq.read_table(paths.main_parquet)
    assert table.num_rows == 9


def test_main_parquet_columns_have_expected_types(tmp_path: Path):
    paths = _run(tmp_path)
    df = pl.read_parquet(paths.main_parquet)
    assert df.schema["abn"] == pl.String
    assert df.schema["abn_status_from_date"] == pl.Date
    assert df.schema["record_last_updated"] == pl.Date
    assert df.schema["gst_status_from_date"] == pl.Date


def test_main_parquet_specific_record_values(tmp_path: Path):
    paths = _run(tmp_path)
    df = pl.read_parquet(paths.main_parquet)
    row = df.filter(pl.col("abn") == "11000000948").to_dicts()[0]
    assert row["abn_status"] == "ACT"
    assert row["abn_status_from_date"] == date(1999, 11, 1)
    assert row["main_name"] == "QBE INSURANCE (INTERNATIONAL) LTD"
    assert row["state"] == "NSW"
    assert row["gst_status"] == "ACT"


def test_individual_record_given_names_joined(tmp_path: Path):
    paths = _run(tmp_path)
    df = pl.read_parquet(paths.main_parquet)
    row = df.filter(pl.col("abn") == "11001572575").to_dicts()[0]
    assert row["entity_kind"] == "legal"
    assert row["individual_given_names"] == "WILLIAM RICHARD"
    assert row["individual_title"] == "MR"
    assert row["main_name"] is None


def test_empty_state_is_null_not_empty_string(tmp_path: Path):
    paths = _run(tmp_path)
    df = pl.read_parquet(paths.main_parquet)
    row = df.filter(pl.col("abn") == "11003615606").to_dicts()[0]
    assert row["state"] is None
    assert row["postcode"] == "0000"


def test_gst_non_sentinel_date_round_trips(tmp_path: Path):
    paths = _run(tmp_path)
    df = pl.read_parquet(paths.main_parquet)
    row = df.filter(pl.col("abn") == "11000009496").to_dicts()[0]
    assert row["gst_status"] == "NON"
    assert row["gst_status_from_date"] == date(1900, 1, 1)


def test_trading_names_parquet_cardinality(tmp_path: Path):
    paths = _run(tmp_path)
    df = pl.read_parquet(paths.trading_parquet)
    snp = df.filter(pl.col("abn") == "11000013098")
    assert snp.height == 11
    assert "TRD" in snp["name_type"].to_list()
    assert "BN" in snp["name_type"].to_list()


def test_dgr_parquet_captures_optional_fields(tmp_path: Path):
    paths = _run(tmp_path)
    df = pl.read_parquet(paths.dgr_parquet)
    smbc = df.filter(pl.col("abn") == "11000047950").to_dicts()
    assert len(smbc) == 2
    self_closing = next(d for d in smbc if d["dgr_name"] is None)
    assert self_closing["dgr_status"] == "ACT"
    assert self_closing["dgr_status_from_date"] == date(2006, 3, 1)
    with_name = next(d for d in smbc if d["dgr_name"] is not None)
    assert with_name["dgr_status"] is None
    assert "BUILDING & MAINTENANCE FUND" in with_name["dgr_name"]


# SQLite materialisation (M7 — DuckDB sqlite_scanner) -----------------------


def test_writer_bundle_does_not_write_sqlite(tmp_path: Path):
    """WriterBundle is Parquet-only after M7 — sqlite_db path is just a
    field on WriterPaths, not opened during streaming writes."""
    paths = _run(tmp_path)
    assert not paths.sqlite_db.exists()


def test_sqlite_main_table_row_count(tmp_path: Path):
    paths = _run_with_sqlite(tmp_path)
    conn = sqlite3.connect(paths.sqlite_db)
    n = conn.execute("SELECT COUNT(*) FROM abn_main").fetchone()[0]
    assert n == 9
    conn.close()


def test_sqlite_dates_stored_as_iso_strings(tmp_path: Path):
    """DuckDB DATE -> SQLite TEXT must round-trip as ISO YYYY-MM-DD —
    the rest of the codebase (query.py) assumes string-typed dates."""
    paths = _run_with_sqlite(tmp_path)
    conn = sqlite3.connect(paths.sqlite_db)
    row = conn.execute(
        "SELECT abn_status_from_date, record_last_updated, gst_status_from_date "
        "FROM abn_main WHERE abn = ?",
        ("11000000948",),
    ).fetchone()
    assert row[0] == "1999-11-01"
    # record_last_updated and gst_status_from_date should also be ISO strings
    # (or None) — verify the storage class is TEXT, not REAL/INTEGER.
    for value in row:
        if value is not None:
            assert isinstance(value, str)
    # The 1900-01-01 GST sentinel must round-trip too.
    sentinel_row = conn.execute(
        "SELECT gst_status_from_date FROM abn_main WHERE abn = ?",
        ("11000009496",),
    ).fetchone()
    assert sentinel_row[0] == "1900-01-01"
    conn.close()


def test_sqlite_trading_and_dgr_have_indexed_abn_lookups(tmp_path: Path):
    paths = _run_with_sqlite(tmp_path)
    conn = sqlite3.connect(paths.sqlite_db)
    n_trading = conn.execute(
        "SELECT COUNT(*) FROM abn_trading_names WHERE abn = ?",
        ("11000013098",),
    ).fetchone()[0]
    assert n_trading == 11
    n_dgr = conn.execute(
        "SELECT COUNT(*) FROM abn_dgr WHERE abn = ?", ("11000047950",)
    ).fetchone()[0]
    assert n_dgr == 2
    conn.close()


def test_sqlite_schema_preserves_pk_and_indices(tmp_path: Path):
    """DuckDB CTAS doesn't preserve constraints, so materialize_sqlite
    pre-creates the schema with DDL before INSERT. Verify all constraints
    survive the round-trip."""
    paths = _run_with_sqlite(tmp_path)
    conn = sqlite3.connect(paths.sqlite_db)
    try:
        # PRIMARY KEY on abn_main.abn
        info = conn.execute("PRAGMA table_info(abn_main)").fetchall()
        abn_col = next(c for c in info if c[1] == "abn")
        assert abn_col[5] == 1, "abn_main.abn must be PRIMARY KEY"

        # Indices: idx_trading_names_abn, idx_dgr_abn, idx_main_state
        indices = {
            row[0]
            for row in conn.execute(
                "SELECT name FROM sqlite_master "
                "WHERE type='index' AND name LIKE 'idx_%'"
            )
        }
        assert indices == {"idx_trading_names_abn", "idx_dgr_abn", "idx_main_state"}
    finally:
        conn.close()


def test_sqlite_materialize_overwrites_existing_db(tmp_path: Path):
    """Calling materialize_sqlite a second time should produce the same
    output, not append duplicates."""
    paths = _run_with_sqlite(tmp_path)
    materialize_sqlite(
        paths.main_parquet, paths.trading_parquet, paths.dgr_parquet, paths.sqlite_db
    )
    conn = sqlite3.connect(paths.sqlite_db)
    try:
        n = conn.execute("SELECT COUNT(*) FROM abn_main").fetchone()[0]
        assert n == 9
    finally:
        conn.close()


def test_batched_writes_match_unbatched(tmp_path: Path):
    paths_a = _paths(tmp_path / "batched")
    paths_b = _paths(tmp_path / "unbatched")
    with WriterBundle(paths_a, batch_size=2) as wb:
        for rec in parse_records(FIXTURE):
            wb.write(rec)
    with WriterBundle(paths_b, batch_size=10_000) as wb:
        for rec in parse_records(FIXTURE):
            wb.write(rec)
    df_a = pl.read_parquet(paths_a.main_parquet).sort("abn")
    df_b = pl.read_parquet(paths_b.main_parquet).sort("abn")
    assert df_a.equals(df_b)


def test_manifest_has_run_metadata_and_row_counts(tmp_path: Path):
    paths = _run(tmp_path)
    manifest = build_manifest(
        paths,
        counts_proxy(paths),
        extract_time="2026-05-06T12:23:33",
        pipeline_run_id="run-test",
        generated_at="2026-05-07T00:00:00Z",
    )
    write_manifest(manifest, tmp_path / "manifest.json")
    written = json.loads((tmp_path / "manifest.json").read_text())
    assert written["pipeline_run_id"] == "run-test"
    assert written["generated_at"] == "2026-05-07T00:00:00Z"
    assert written["extract_time"] == "2026-05-06T12:23:33"
    assert written["row_counts"]["abn_main"] == 9
    # Files array is gone — Iceberg owns the dataset; manifest is just metadata.
    assert "files" not in written


def counts_proxy(paths: WriterPaths):
    """Compute counts from on-disk Parquet, for manifest test convenience."""
    from abr_extract.write import WriteCounts
    return WriteCounts(
        main=pq.read_table(paths.main_parquet).num_rows,
        trading=pq.read_table(paths.trading_parquet).num_rows,
        dgr=pq.read_table(paths.dgr_parquet).num_rows,
    )
