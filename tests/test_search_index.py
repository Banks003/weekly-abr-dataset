"""Tests for the frontend search index Parquet."""

from __future__ import annotations

from pathlib import Path

import polars as pl

from abr_extract.parse import parse_records
from abr_extract.search_index import ABN_SEARCH_SCHEMA, build_search_index
from abr_extract.write import WriterBundle, WriterPaths

FIXTURE = Path(__file__).parent / "fixtures" / "sample_records.xml"


def _build_canonical(tmp_path: Path) -> WriterPaths:
    paths = WriterPaths(
        main_parquet=tmp_path / "abn_main.parquet",
        trading_parquet=tmp_path / "abn_trading_names.parquet",
        dgr_parquet=tmp_path / "abn_dgr.parquet",
        sqlite_db=tmp_path / "abr.sqlite",
    )
    with WriterBundle(paths) as wb:
        for rec in parse_records(FIXTURE):
            wb.write(rec)
    return paths


def test_search_index_row_count_matches_main(tmp_path: Path):
    paths = _build_canonical(tmp_path)
    dest = tmp_path / "abn-search.parquet"
    rows = build_search_index(paths.main_parquet, paths.trading_parquet, dest)
    main_rows = pl.read_parquet(paths.main_parquet).height
    assert rows == main_rows


def test_search_index_schema_matches_module_schema(tmp_path: Path):
    paths = _build_canonical(tmp_path)
    dest = tmp_path / "abn-search.parquet"
    build_search_index(paths.main_parquet, paths.trading_parquet, dest)
    import pyarrow.parquet as pq
    assert pq.read_table(dest).schema.equals(ABN_SEARCH_SCHEMA, check_metadata=False)


def test_search_text_includes_main_name_lowercased(tmp_path: Path):
    paths = _build_canonical(tmp_path)
    dest = tmp_path / "abn-search.parquet"
    build_search_index(paths.main_parquet, paths.trading_parquet, dest)
    df = pl.read_parquet(dest)
    qbe = df.filter(pl.col("abn") == "11000000948").to_dicts()[0]
    assert qbe["name"] == "QBE INSURANCE (INTERNATIONAL) LTD"
    # search_text should contain the lowercased main name
    assert "qbe insurance (international) ltd" in qbe["search_text"]


def test_search_text_includes_individual_names_for_legal_entities(tmp_path: Path):
    paths = _build_canonical(tmp_path)
    dest = tmp_path / "abn-search.parquet"
    build_search_index(paths.main_parquet, paths.trading_parquet, dest)
    df = pl.read_parquet(dest)
    legal = df.filter(pl.col("abn") == "11001572575").to_dicts()[0]
    # entity_kind=legal means main_name is null; display name comes from
    # title + given + family.
    assert legal["entity_kind"] == "legal"
    assert legal["name"] is not None
    assert "william" in legal["search_text"]
    assert "richard" in legal["search_text"]


def test_search_text_includes_trading_names(tmp_path: Path):
    """The fixture's ABN 11000013098 has 11 trading names — they must all
    show up in the search_text so a user typing any one of them matches."""
    paths = _build_canonical(tmp_path)
    dest = tmp_path / "abn-search.parquet"
    build_search_index(paths.main_parquet, paths.trading_parquet, dest)
    df = pl.read_parquet(dest)
    trading_df = pl.read_parquet(paths.trading_parquet)

    snp_search = df.filter(pl.col("abn") == "11000013098").to_dicts()[0]
    snp_trading_names = trading_df.filter(
        pl.col("abn") == "11000013098"
    ).get_column("name").to_list()
    assert len(snp_trading_names) == 11

    # Every trading name's lowercased form must appear in the search_text.
    for name in snp_trading_names:
        assert name.lower() in snp_search["search_text"], name


def test_search_index_handles_abn_with_no_trading_names(tmp_path: Path):
    """Most ABNs don't have trading names; the LEFT JOIN should still produce
    one search row per ABN with a non-null search_text."""
    paths = _build_canonical(tmp_path)
    dest = tmp_path / "abn-search.parquet"
    build_search_index(paths.main_parquet, paths.trading_parquet, dest)
    df = pl.read_parquet(dest)
    qbe = df.filter(pl.col("abn") == "11000000948").to_dicts()[0]
    assert qbe["search_text"] is not None
    assert qbe["search_text"].strip() != ""
