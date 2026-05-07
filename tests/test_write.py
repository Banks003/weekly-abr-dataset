from __future__ import annotations

import json
import sqlite3
from datetime import date
from pathlib import Path

import polars as pl
import pyarrow.parquet as pq

from abr_extract.parse import parse_records
from abr_extract.schema import parse_yyyymmdd
from abr_extract.write import WriterBundle, WriterPaths, build_manifest, write_manifest

FIXTURE = Path(__file__).parent / "fixtures" / "sample_records.xml"


def _paths(tmp_path: Path) -> WriterPaths:
    return WriterPaths(
        main_parquet=tmp_path / "abn_main.parquet",
        trading_parquet=tmp_path / "abn_trading_names.parquet",
        dgr_parquet=tmp_path / "abn_dgr.parquet",
        sqlite_db=tmp_path / "abr.sqlite",
    )


def _run(tmp_path: Path, batch_size: int = 50) -> WriterPaths:
    paths = _paths(tmp_path)
    with WriterBundle(paths, batch_size=batch_size) as wb:
        for rec in parse_records(FIXTURE):
            wb.write(rec)
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


def test_sqlite_main_table_row_count(tmp_path: Path):
    paths = _run(tmp_path)
    conn = sqlite3.connect(paths.sqlite_db)
    n = conn.execute("SELECT COUNT(*) FROM abn_main").fetchone()[0]
    assert n == 9
    conn.close()


def test_sqlite_dates_stored_as_iso_strings(tmp_path: Path):
    paths = _run(tmp_path)
    conn = sqlite3.connect(paths.sqlite_db)
    row = conn.execute(
        "SELECT abn_status_from_date FROM abn_main WHERE abn = ?",
        ("11000000948",),
    ).fetchone()
    assert row[0] == "1999-11-01"
    conn.close()


def test_sqlite_trading_and_dgr_have_indexed_abn_lookups(tmp_path: Path):
    paths = _run(tmp_path)
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


def test_manifest_includes_sha256_and_row_counts(tmp_path: Path):
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
    assert written["extract_time"] == "2026-05-06T12:23:33"
    assert written["row_counts"]["abn_main"] == 9
    file_labels = {f["label"] for f in written["files"]}
    expected = {"abn_main_parquet", "abn_trading_names_parquet", "abn_dgr_parquet", "sqlite"}
    assert expected == file_labels
    for f in written["files"]:
        assert len(f["sha256"]) == 64
        assert f["size_bytes"] > 0


def counts_proxy(paths: WriterPaths):
    """Compute counts from on-disk Parquet, for manifest test convenience."""
    from abr_extract.write import WriteCounts
    return WriteCounts(
        main=pq.read_table(paths.main_parquet).num_rows,
        trading=pq.read_table(paths.trading_parquet).num_rows,
        dgr=pq.read_table(paths.dgr_parquet).num_rows,
    )
