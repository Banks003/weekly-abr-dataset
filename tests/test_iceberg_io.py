from __future__ import annotations

from datetime import date
from pathlib import Path

import polars as pl
import pytest

from abr_extract.history import (
    apply_update_main,
    apply_update_trading,
    bootstrap_main,
    bootstrap_trading,
)
from abr_extract.iceberg_io import (
    ABN_MAIN_HISTORY_SCHEMA,
    ABN_TRADING_HISTORY_SCHEMA,
    TABLE_SCHEMAS,
    connect_local_catalog,
    ensure_history_tables,
    write_history,
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
