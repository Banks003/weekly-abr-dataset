from __future__ import annotations

import json
from datetime import date
from pathlib import Path

import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from abr_extract.diff import DiffInputs, build_diff, diff_summary, empty_diff
from abr_extract.schema import (
    ABN_DGR_SCHEMA,
    ABN_MAIN_SCHEMA,
    ABN_TRADING_NAMES_SCHEMA,
)


def _write_parquet(path: Path, schema: pa.Schema, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    table = pa.Table.from_pylist(rows, schema=schema)
    pq.write_table(table, path)


def _main_row(**overrides) -> dict:
    base = {col: None for col in ABN_MAIN_SCHEMA.names}
    base["abn"] = "11111111111"
    base["abn_status"] = "ACT"
    base["abn_status_from_date"] = date(2020, 1, 1)
    base["record_last_updated"] = date(2024, 1, 1)
    base["replaced"] = "N"
    base["entity_type_ind"] = "PRV"
    base["entity_type_text"] = "Australian Private Company"
    base["entity_kind"] = "main"
    base["main_name"] = "ACME PTY LTD"
    base["main_name_type"] = "MN"
    base["state"] = "NSW"
    base["postcode"] = "2000"
    base["gst_status"] = "ACT"
    base["gst_status_from_date"] = date(2020, 1, 1)
    base.update(overrides)
    return base


@pytest.fixture
def synthetic(tmp_path: Path) -> DiffInputs:
    prev_dir = tmp_path / "prev"
    new_dir = tmp_path / "new"
    prev_dir.mkdir()
    new_dir.mkdir()

    prev_main = [
        _main_row(abn="11111111111", main_name="ACME PTY LTD"),
        _main_row(abn="22222222222", main_name="ACME OLD NAME PTY LTD"),
        _main_row(
            abn="33333333333",
            entity_kind="legal",
            main_name=None,
            main_name_type=None,
            individual_title="MR",
            individual_given_names="JOHN",
            individual_family_name="SMITH",
            entity_type_ind="IND",
        ),
        _main_row(abn="44444444444", main_name="GOING TO CANCEL PTY LTD"),
        _main_row(abn="55555555555", main_name="MOVING STATE PTY LTD", state="NSW"),
        _main_row(abn="66666666666", main_name="REMOVED COMPANY PTY LTD"),
    ]
    new_main = [
        _main_row(abn="11111111111", main_name="ACME PTY LTD"),  # unchanged
        _main_row(abn="22222222222", main_name="ACME REBRAND PTY LTD"),  # name change
        _main_row(  # individual name change
            abn="33333333333",
            entity_kind="legal",
            main_name=None,
            main_name_type=None,
            individual_title="MS",
            individual_given_names="JANE",
            individual_family_name="SMITH",
            entity_type_ind="IND",
        ),
        _main_row(abn="44444444444", main_name="GOING TO CANCEL PTY LTD", abn_status="CAN"),
        _main_row(abn="55555555555", main_name="MOVING STATE PTY LTD", state="VIC"),
        # 66666... removed
        _main_row(abn="77777777777", main_name="NEWLY ADDED PTY LTD"),  # added
    ]

    prev_trading = [
        {"abn": "11111111111", "name": "ACME", "name_type": "TRD"},
        {"abn": "11111111111", "name": "ACME PRODUCTS", "name_type": "BN"},
    ]
    new_trading = [
        {"abn": "11111111111", "name": "ACME", "name_type": "TRD"},
        # ACME PRODUCTS removed
        {"abn": "11111111111", "name": "ACME GLOBAL", "name_type": "BN"},  # added
    ]

    dgr_a = {
        "abn": "55555555555",
        "dgr_status_from_date": date(2010, 1, 1),
        "dgr_status": "ACT",
        "dgr_name": None,
    }
    dgr_b = {
        "abn": "55555555555",
        "dgr_status_from_date": date(2024, 6, 1),
        "dgr_status": "ACT",
        "dgr_name": "FUND XYZ",
    }
    prev_dgr = [dgr_a]
    new_dgr = [dgr_a, dgr_b]

    _write_parquet(prev_dir / "abn_main.parquet", ABN_MAIN_SCHEMA, prev_main)
    _write_parquet(new_dir / "abn_main.parquet", ABN_MAIN_SCHEMA, new_main)
    _write_parquet(prev_dir / "abn_trading_names.parquet", ABN_TRADING_NAMES_SCHEMA, prev_trading)
    _write_parquet(new_dir / "abn_trading_names.parquet", ABN_TRADING_NAMES_SCHEMA, new_trading)
    _write_parquet(prev_dir / "abn_dgr.parquet", ABN_DGR_SCHEMA, prev_dgr)
    _write_parquet(new_dir / "abn_dgr.parquet", ABN_DGR_SCHEMA, new_dgr)

    return DiffInputs(
        prev_main=prev_dir / "abn_main.parquet",
        prev_trading=prev_dir / "abn_trading_names.parquet",
        prev_dgr=prev_dir / "abn_dgr.parquet",
        new_main=new_dir / "abn_main.parquet",
        new_trading=new_dir / "abn_trading_names.parquet",
        new_dgr=new_dir / "abn_dgr.parquet",
    )


@pytest.fixture
def diff(synthetic: DiffInputs) -> pl.DataFrame:
    return build_diff(
        synthetic,
        prev_extract_date=date(2026, 4, 29),
        new_extract_date=date(2026, 5, 6),
    )


def _by_change(diff: pl.DataFrame, change_type: str) -> list[dict]:
    return diff.filter(pl.col("change_type") == change_type).to_dicts()


def test_added_change_type(diff):
    added = _by_change(diff, "added")
    abns = {r["abn"] for r in added}
    assert abns == {"77777777777"}


def test_removed_change_type(diff):
    removed = _by_change(diff, "removed")
    abns = {r["abn"] for r in removed}
    assert abns == {"66666666666"}


def test_main_name_change(diff):
    rows = _by_change(diff, "main_name")
    assert len(rows) == 1
    r = rows[0]
    assert r["abn"] == "22222222222"
    assert r["prev_value"] == "ACME OLD NAME PTY LTD"
    assert r["new_value"] == "ACME REBRAND PTY LTD"


def test_abn_status_change(diff):
    rows = _by_change(diff, "abn_status")
    assert len(rows) == 1
    assert rows[0]["abn"] == "44444444444"
    assert rows[0]["prev_value"] == "ACT"
    assert rows[0]["new_value"] == "CAN"


def test_state_change(diff):
    rows = _by_change(diff, "state")
    assert len(rows) == 1
    assert rows[0]["abn"] == "55555555555"
    assert rows[0]["prev_value"] == "NSW"
    assert rows[0]["new_value"] == "VIC"


def test_individual_name_change_json_encoded(diff):
    rows = _by_change(diff, "individual_name")
    assert len(rows) == 1
    r = rows[0]
    assert r["abn"] == "33333333333"
    prev = json.loads(r["prev_value"])
    new = json.loads(r["new_value"])
    assert prev["individual_title"] == "MR"
    assert new["individual_title"] == "MS"
    assert prev["individual_given_names"] == "JOHN"
    assert new["individual_given_names"] == "JANE"


def test_trading_name_added(diff):
    added = _by_change(diff, "trading_name_added")
    assert len(added) == 1
    r = added[0]
    assert r["abn"] == "11111111111"
    payload = json.loads(r["new_value"])
    assert payload["name"] == "ACME GLOBAL"
    assert payload["name_type"] == "BN"


def test_trading_name_removed(diff):
    removed = _by_change(diff, "trading_name_removed")
    assert len(removed) == 1
    payload = json.loads(removed[0]["prev_value"])
    assert payload["name"] == "ACME PRODUCTS"


def test_dgr_added_picks_up_new_dgr_per_date(diff):
    added = _by_change(diff, "dgr_added")
    assert len(added) == 1
    payload = json.loads(added[0]["new_value"])
    assert payload["dgr_name"] == "FUND XYZ"
    assert payload["dgr_status_from_date"] == "2024-06-01"


def test_dgr_unchanged_not_in_diff(diff):
    removed = _by_change(diff, "dgr_removed")
    assert removed == []


def test_extract_dates_stamped_on_every_row(diff):
    assert diff.height > 0
    assert (diff["prev_extract_date"] == date(2026, 4, 29)).all()
    assert (diff["new_extract_date"] == date(2026, 5, 6)).all()


def test_summary_row_counts(diff):
    summary = diff_summary(diff)
    assert summary["added"] == 1
    assert summary["removed"] == 1
    assert summary["main_name"] == 1
    assert summary["abn_status"] == 1
    assert summary["state"] == 1
    assert summary["individual_name"] == 1
    assert summary["trading_name_added"] == 1
    assert summary["trading_name_removed"] == 1
    assert summary["dgr_added"] == 1


def test_unchanged_abn_produces_no_main_rows(diff):
    rows = diff.filter(pl.col("abn") == "11111111111")
    # ACME has trading-name changes but no main-table changes
    main_change_types = {
        "added", "removed", "abn_status", "gst_status", "state",
        "postcode", "main_name", "entity_type_ind", "individual_name",
    }
    assert rows.filter(pl.col("change_type").is_in(main_change_types)).height == 0


def test_diff_dataframe_has_canonical_schema(diff):
    expected = {
        "abn",
        "change_type",
        "prev_value",
        "new_value",
        "prev_extract_date",
        "new_extract_date",
    }
    assert set(diff.columns) == expected


def test_empty_diff_has_canonical_schema():
    df = empty_diff(None, date(2026, 5, 6))
    assert df.height == 0
    assert "change_type" in df.columns
