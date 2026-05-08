from __future__ import annotations

from datetime import date
from pathlib import Path

import polars as pl
import pytest

from abr_extract.history import (
    MAIN_CONTENT_COLUMNS,
    MAIN_IDENTITY,
    TRADING_CONTENT_COLUMNS,
    TRADING_IDENTITY,
    apply_update_main,
    apply_update_trading,
    bootstrap_main,
    bootstrap_trading,
)
from abr_extract.iceberg_io import (
    ABN_MAIN_HISTORY_SCHEMA,
    ABN_TRADING_HISTORY_SCHEMA,
    TABLE_SCHEMAS,
    bootstrap_history_table_arrow,
    connect_local_catalog,
    ensure_history_tables,
    prune_iceberg_snapshots,
    update_history_table_arrow,
    write_history,
)
from abr_extract.schema import (
    ABN_MAIN_SCHEMA,
    ABN_TRADING_NAMES_SCHEMA,
)


@pytest.fixture
def catalog(tmp_path: Path):
    return connect_local_catalog(tmp_path / "warehouse")


@pytest.fixture
def tables(catalog):
    return ensure_history_tables(catalog, namespace="abr_test")


def _main_row(**overrides) -> dict:
    base = {
        "abn": "11111111111",
        "abn_status": "ACT",
        "abn_status_from_date": date(2020, 1, 1),
        "record_last_updated": date(2024, 6, 1),
        "replaced": "N",
        "entity_type_ind": "PRV",
        "entity_type_text": "Australian Private Company",
        "entity_kind": "main",
        "main_name": "ACME PTY LTD",
        "main_name_type": "MN",
        "individual_title": None,
        "individual_given_names": None,
        "individual_family_name": None,
        "individual_name_type": None,
        "state": "NSW",
        "postcode": "2000",
        "asic_number": "123456789",
        "asic_number_type": "undetermined",
        "gst_status": "ACT",
        "gst_status_from_date": date(2020, 1, 1),
    }
    base.update(overrides)
    return base


# --- catalog & table lifecycle ------------------------------------------


def test_ensure_history_tables_creates_all_three(catalog):
    tables = ensure_history_tables(catalog, namespace="abr_test")
    assert set(tables.keys()) == {
        "abn_main_history",
        "abn_trading_names_history",
        "abn_dgr_history",
    }


def test_ensure_history_tables_is_idempotent(catalog):
    first = ensure_history_tables(catalog, namespace="abr_test")
    second = ensure_history_tables(catalog, namespace="abr_test")
    for name in first:
        assert first[name].name() == second[name].name()


# --- write + read round-trip --------------------------------------------


def test_write_and_read_main_history_roundtrip(tables):
    snapshot = pl.DataFrame([_main_row()])
    history = bootstrap_main(snapshot, date(2026, 5, 6))
    write_history(tables["abn_main_history"], history, schema=ABN_MAIN_HISTORY_SCHEMA)

    table = tables["abn_main_history"]
    table.refresh()
    df = pl.from_arrow(table.scan().to_arrow())
    assert df.height == 1
    assert df["abn"].to_list() == ["11111111111"]
    assert df["valid_to"].to_list() == [None]


def test_overwrite_replaces_all_rows(tables):
    history_a = bootstrap_main(pl.DataFrame([_main_row(abn="11111111111")]), date(2026, 5, 6))
    history_b = bootstrap_main(pl.DataFrame([_main_row(abn="22222222222")]), date(2026, 5, 13))

    write_history(tables["abn_main_history"], history_a, schema=ABN_MAIN_HISTORY_SCHEMA)
    write_history(tables["abn_main_history"], history_b, schema=ABN_MAIN_HISTORY_SCHEMA)

    table = tables["abn_main_history"]
    table.refresh()
    df = pl.from_arrow(table.scan().to_arrow())
    assert df["abn"].to_list() == ["22222222222"]


# --- multi-week update through Iceberg ----------------------------------


def test_two_week_update_through_iceberg_yields_closed_and_open_rows(tables):
    week1 = pl.DataFrame([_main_row(state="NSW")])
    history = bootstrap_main(week1, date(2026, 5, 6))
    write_history(tables["abn_main_history"], history, schema=ABN_MAIN_HISTORY_SCHEMA)

    table = tables["abn_main_history"]
    table.refresh()
    persisted = pl.from_arrow(table.scan().to_arrow())

    week2 = pl.DataFrame([_main_row(state="VIC")])
    history2 = apply_update_main(persisted, week2, date(2026, 5, 13))
    write_history(tables["abn_main_history"], history2, schema=ABN_MAIN_HISTORY_SCHEMA)

    table.refresh()
    df = pl.from_arrow(table.scan().to_arrow())
    assert df.height == 2
    closed = df.filter(pl.col("valid_to").is_not_null()).to_dicts()
    open_rows = df.filter(pl.col("valid_to").is_null()).to_dicts()
    assert len(closed) == 1
    assert closed[0]["state"] == "NSW"
    assert closed[0]["valid_to"] == date(2026, 5, 13)
    assert len(open_rows) == 1
    assert open_rows[0]["state"] == "VIC"


def test_trading_names_history_set_membership_through_iceberg(tables):
    week1 = pl.DataFrame(
        [
            {"abn": "11111111111", "name": "ACME", "name_type": "BN"},
            {"abn": "11111111111", "name": "ACME PRODUCTS", "name_type": "BN"},
        ]
    )
    history = bootstrap_trading(week1, date(2026, 5, 6))
    write_history(
        tables["abn_trading_names_history"], history, schema=ABN_TRADING_HISTORY_SCHEMA
    )

    table = tables["abn_trading_names_history"]
    table.refresh()
    persisted = pl.from_arrow(table.scan().to_arrow())

    week2 = pl.DataFrame(
        [
            {"abn": "11111111111", "name": "ACME", "name_type": "BN"},
            {"abn": "11111111111", "name": "ACME GLOBAL", "name_type": "BN"},
        ]
    )
    history2 = apply_update_trading(persisted, week2, date(2026, 5, 13))
    write_history(
        tables["abn_trading_names_history"], history2, schema=ABN_TRADING_HISTORY_SCHEMA
    )

    table.refresh()
    df = pl.from_arrow(table.scan().to_arrow()).sort(["name", "valid_from"])
    rows = df.to_dicts()
    names_seen = {r["name"] for r in rows}
    assert names_seen == {"ACME", "ACME PRODUCTS", "ACME GLOBAL"}
    closed = [r for r in rows if r["valid_to"] is not None]
    assert len(closed) == 1
    assert closed[0]["name"] == "ACME PRODUCTS"


# --- schema invariants --------------------------------------------------


def test_main_history_schema_includes_scd2_columns():
    fields = {f.name for f in ABN_MAIN_HISTORY_SCHEMA.fields}
    assert "valid_from" in fields
    assert "valid_to" in fields
    assert "snapshot_observed_date" in fields


def test_table_schemas_dict_is_exhaustive():
    assert set(TABLE_SCHEMAS.keys()) == {
        "abn_main_history",
        "abn_trading_names_history",
        "abn_dgr_history",
    }


# Snapshot manifest (M10) ---------------------------------------------------

from abr_extract.iceberg_io import (  # noqa: E402
    _s3_to_public_url,
    build_snapshot_manifest,
)


def test_s3_to_public_url_translates_warehouse_paths():
    out = _s3_to_public_url(
        "s3://weekly-abr-dataset/iceberg/abr/abn_main_history/data/00000-0.parquet",
        bucket="weekly-abr-dataset",
        public_base_url="https://gazetteer.au",
    )
    assert out == (
        "https://gazetteer.au/iceberg/abr/abn_main_history/data/00000-0.parquet"
    )


def test_s3_to_public_url_passes_through_unrelated_paths():
    out = _s3_to_public_url(
        "https://example.com/some.parquet",
        bucket="weekly-abr-dataset",
        public_base_url="https://gazetteer.au",
    )
    assert out == "https://example.com/some.parquet"


def test_build_snapshot_manifest_lists_tables_with_data_files(tables):
    """After bootstrapping a history table, the manifest should list its
    data file URLs, snapshot id, and a non-zero row count."""
    main = pl.DataFrame([_main_row()], schema={
        "abn": pl.Utf8, "abn_status": pl.Utf8, "abn_status_from_date": pl.Date,
        "record_last_updated": pl.Date, "replaced": pl.Utf8,
        "entity_type_ind": pl.Utf8, "entity_type_text": pl.Utf8,
        "entity_kind": pl.Utf8, "main_name": pl.Utf8, "main_name_type": pl.Utf8,
        "individual_title": pl.Utf8, "individual_given_names": pl.Utf8,
        "individual_family_name": pl.Utf8, "individual_name_type": pl.Utf8,
        "state": pl.Utf8, "postcode": pl.Utf8, "asic_number": pl.Utf8,
        "asic_number_type": pl.Utf8, "gst_status": pl.Utf8,
        "gst_status_from_date": pl.Date,
    })
    history = bootstrap_main(main, date(2026, 5, 5))
    write_history(tables["abn_main_history"], history, schema=ABN_MAIN_HISTORY_SCHEMA)

    out = build_snapshot_manifest(
        tables,
        namespace="abr_test",
        generated_at="2026-05-05T04:15:00Z",
        extract_time="2026-05-05T03:00:00Z",
        bucket="weekly-abr-dataset",
        public_base_url="https://gazetteer.au",
    )

    assert out["iceberg_namespace"] == "abr_test"
    assert out["generated_at"] == "2026-05-05T04:15:00Z"
    assert out["extract_time"] == "2026-05-05T03:00:00Z"
    main_block = out["tables"]["abn_main_history"]
    assert main_block["row_count"] == 1
    assert main_block["snapshot_id"] is not None
    assert main_block["current_view_filter"] == "valid_to IS NULL"
    assert len(main_block["data_files"]) >= 1
    # Local catalog uses file:// URLs — no s3:// prefix to translate.
    # In production the URLs would start with https://gazetteer.au/...
    for url in main_block["data_files"]:
        assert url.endswith(".parquet")

    # Tables that were never written to should still appear, with no data files.
    trading_block = out["tables"]["abn_trading_names_history"]
    assert trading_block["row_count"] == 0
    assert trading_block["data_files"] == []


# Arrow + DuckDB SCD2 update path -------------------------------------------
#
# TEMPORARY: synthetic-data scaffolding.
#
# These tests use small in-memory rows to validate the SCD2 mechanics
# (unchanged / changed / added / removed / set-membership) of
# update_history_table_arrow against a local SQLite-backed iceberg catalog.
# They cover the *correctness* side of the rewrite.
#
# The *real* validation is diffing the next weekly refresh's iceberg table
# against the prior week — either whole or sliced — and confirming row
# counts, open/closed transitions, and content equality match expectations.
# Track the real-data validation in the follow-up issue; remove or shrink
# this section once that lands.


def _write_main_snapshot_parquet(rows: list[dict], path: Path) -> Path:
    import pyarrow as pa
    import pyarrow.parquet as pq

    pq.write_table(pa.Table.from_pylist(rows, schema=ABN_MAIN_SCHEMA), path)
    return path


def _write_trading_snapshot_parquet(rows: list[dict], path: Path) -> Path:
    import pyarrow as pa
    import pyarrow.parquet as pq

    pq.write_table(pa.Table.from_pylist(rows, schema=ABN_TRADING_NAMES_SCHEMA), path)
    return path


def test_update_history_table_arrow_unchanged_keeps_open_rows(tables, tmp_path):
    snapshot = _main_row()
    parquet_path = _write_main_snapshot_parquet([snapshot], tmp_path / "snap1.parquet")
    bootstrap_history_table_arrow(
        tables["abn_main_history"], parquet_path, date(2026, 5, 6)
    )

    parquet_path2 = _write_main_snapshot_parquet([snapshot], tmp_path / "snap2.parquet")
    summary = update_history_table_arrow(
        tables["abn_main_history"],
        parquet_path2,
        date(2026, 5, 13),
        base_columns=list(ABN_MAIN_SCHEMA.names),
        identity=list(MAIN_IDENTITY),
        content_columns=list(MAIN_CONTENT_COLUMNS),
    )
    assert summary["mode"] == "update"
    assert summary["rows_written"] == 1

    table = tables["abn_main_history"]
    table.refresh()
    df = pl.from_arrow(table.scan().to_arrow())
    assert df.height == 1
    assert df["valid_to"].to_list() == [None]


def test_update_history_table_arrow_changed_field_closes_old_opens_new(tables, tmp_path):
    bootstrap_history_table_arrow(
        tables["abn_main_history"],
        _write_main_snapshot_parquet([_main_row(state="NSW")], tmp_path / "w1.parquet"),
        date(2026, 5, 6),
    )

    update_history_table_arrow(
        tables["abn_main_history"],
        _write_main_snapshot_parquet([_main_row(state="VIC")], tmp_path / "w2.parquet"),
        date(2026, 5, 13),
        base_columns=list(ABN_MAIN_SCHEMA.names),
        identity=list(MAIN_IDENTITY),
        content_columns=list(MAIN_CONTENT_COLUMNS),
    )

    table = tables["abn_main_history"]
    table.refresh()
    df = pl.from_arrow(table.scan().to_arrow())
    assert df.height == 2
    closed = df.filter(pl.col("valid_to").is_not_null()).to_dicts()
    open_rows = df.filter(pl.col("valid_to").is_null()).to_dicts()
    assert len(closed) == 1 and closed[0]["state"] == "NSW"
    assert closed[0]["valid_to"] == date(2026, 5, 13)
    assert len(open_rows) == 1 and open_rows[0]["state"] == "VIC"
    assert open_rows[0]["valid_from"] == date(2026, 5, 13)


def test_update_history_table_arrow_added_and_removed(tables, tmp_path):
    bootstrap_history_table_arrow(
        tables["abn_main_history"],
        _write_main_snapshot_parquet(
            [_main_row(abn="11111111111"), _main_row(abn="22222222222")],
            tmp_path / "w1.parquet",
        ),
        date(2026, 5, 6),
    )

    update_history_table_arrow(
        tables["abn_main_history"],
        _write_main_snapshot_parquet(
            [_main_row(abn="11111111111"), _main_row(abn="33333333333")],
            tmp_path / "w2.parquet",
        ),
        date(2026, 5, 13),
        base_columns=list(ABN_MAIN_SCHEMA.names),
        identity=list(MAIN_IDENTITY),
        content_columns=list(MAIN_CONTENT_COLUMNS),
    )

    table = tables["abn_main_history"]
    table.refresh()
    df = pl.from_arrow(table.scan().to_arrow()).sort("abn")
    rows = df.to_dicts()
    by_abn = {r["abn"]: r for r in rows}
    assert by_abn["11111111111"]["valid_to"] is None
    assert by_abn["22222222222"]["valid_to"] == date(2026, 5, 13)
    assert by_abn["33333333333"]["valid_to"] is None
    assert by_abn["33333333333"]["valid_from"] == date(2026, 5, 13)


def test_update_history_table_arrow_trading_set_membership(tables, tmp_path):
    bootstrap_history_table_arrow(
        tables["abn_trading_names_history"],
        _write_trading_snapshot_parquet(
            [
                {"abn": "11111111111", "name": "ACME", "name_type": "BN"},
                {"abn": "11111111111", "name": "ACME PRODUCTS", "name_type": "BN"},
            ],
            tmp_path / "t1.parquet",
        ),
        date(2026, 5, 6),
        record_last_updated_column=None,
    )

    update_history_table_arrow(
        tables["abn_trading_names_history"],
        _write_trading_snapshot_parquet(
            [
                {"abn": "11111111111", "name": "ACME", "name_type": "BN"},
                {"abn": "11111111111", "name": "ACME GLOBAL", "name_type": "BN"},
            ],
            tmp_path / "t2.parquet",
        ),
        date(2026, 5, 13),
        base_columns=list(ABN_TRADING_NAMES_SCHEMA.names),
        identity=list(TRADING_IDENTITY),
        content_columns=list(TRADING_CONTENT_COLUMNS),
    )

    table = tables["abn_trading_names_history"]
    table.refresh()
    df = pl.from_arrow(table.scan().to_arrow()).sort(["name", "valid_from"])
    rows = df.to_dicts()
    names_seen = {r["name"] for r in rows}
    assert names_seen == {"ACME", "ACME PRODUCTS", "ACME GLOBAL"}
    closed = [r for r in rows if r["valid_to"] is not None]
    assert len(closed) == 1
    assert closed[0]["name"] == "ACME PRODUCTS"
    assert closed[0]["valid_to"] == date(2026, 5, 13)


# Snapshot pruning ----------------------------------------------------------


def test_prune_iceberg_snapshots_keeps_recent_n(tables, tmp_path):
    """After multiple weekly overwrites, pruning should leave only ``keep`` snapshots."""
    table = tables["abn_main_history"]
    weeks = [
        (date(2026, 5, 6), "NSW"),
        (date(2026, 5, 13), "VIC"),
        (date(2026, 5, 20), "QLD"),
        (date(2026, 5, 27), "WA"),
    ]
    for i, (extract_date, st) in enumerate(weeks):
        snapshot = pl.DataFrame([_main_row(state=st)])
        history = bootstrap_main(snapshot, extract_date) if i == 0 else (
            apply_update_main(
                pl.from_arrow(table.scan().to_arrow()), snapshot, extract_date
            )
        )
        write_history(table, history, schema=ABN_MAIN_HISTORY_SCHEMA)
        table.refresh()

    pre_count = len(list(table.metadata.snapshots))
    assert pre_count >= 4

    summary = prune_iceberg_snapshots(table, keep=2)
    assert summary["expired"] == pre_count - 2

    table.refresh()
    assert len(list(table.metadata.snapshots)) == 2


def test_prune_iceberg_snapshots_noop_when_under_threshold(tables, tmp_path):
    """A table with fewer snapshots than ``keep`` is left untouched."""
    table = tables["abn_main_history"]
    snapshot = pl.DataFrame([_main_row()])
    history = bootstrap_main(snapshot, date(2026, 5, 6))
    write_history(table, history, schema=ABN_MAIN_HISTORY_SCHEMA)
    table.refresh()

    summary = prune_iceberg_snapshots(table, keep=5)
    assert summary["expired"] == 0
    assert summary["kept"] == 1
