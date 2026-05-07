from __future__ import annotations

from datetime import date
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from abr_extract.query import profile, search, trends
from abr_extract.schema import (
    ABN_DGR_SCHEMA,
    ABN_MAIN_SCHEMA,
    ABN_TRADING_NAMES_SCHEMA,
)


def _main_row(**overrides) -> dict:
    base = {col: None for col in ABN_MAIN_SCHEMA.names}
    base["abn"] = "11000000001"
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


def _write(path: Path, schema: pa.Schema, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(pa.Table.from_pylist(rows, schema=schema), path)


@pytest.fixture
def fixture_source(tmp_path: Path) -> str:
    """Build a tiny source directory with three parquet files."""
    main_rows = [
        _main_row(
            abn="11000000001",
            main_name="QANTAS AIRWAYS LIMITED",
            entity_type_ind="PUB",
            entity_type_text="Australian Public Company",
            abn_status="ACT",
            abn_status_from_date=date(1999, 11, 1),
        ),
        _main_row(
            abn="11000000002",
            main_name="ACME PTY LTD",
            abn_status="ACT",
            abn_status_from_date=date(2024, 6, 1),
        ),
        _main_row(
            abn="11000000003",
            main_name=None,
            entity_kind="legal",
            entity_type_ind="IND",
            entity_type_text="Individual/Sole Trader",
            individual_title="MR",
            individual_given_names="JOHN",
            individual_family_name="SMITH",
            individual_name_type="LGL",
            state="VIC",
            postcode="3000",
            abn_status="CAN",
            abn_status_from_date=date(2024, 3, 1),
        ),
        _main_row(
            abn="11000000004",
            main_name="OLD COMPANY PTY LTD",
            abn_status="CAN",
            abn_status_from_date=date(2023, 1, 1),
            state="QLD",
            postcode="4000",
        ),
    ]
    trading = [
        {"abn": "11000000001", "name": "QANTAS", "name_type": "BN"},
        {"abn": "11000000001", "name": "QANTAS WINE", "name_type": "BN"},
        {"abn": "11000000002", "name": "ACME GLOBAL", "name_type": "BN"},
    ]
    dgr = [
        {
            "abn": "11000000001",
            "dgr_status_from_date": date(2010, 1, 1),
            "dgr_status": "ACT",
            "dgr_name": "QANTAS FOUNDATION",
        }
    ]
    _write(tmp_path / "abn-main-latest.parquet", ABN_MAIN_SCHEMA, main_rows)
    _write(tmp_path / "abn-trading-names-latest.parquet", ABN_TRADING_NAMES_SCHEMA, trading)
    _write(tmp_path / "abn-dgr-latest.parquet", ABN_DGR_SCHEMA, dgr)
    return str(tmp_path)


# --- search --------------------------------------------------------------


def test_search_main_name_substring(fixture_source):
    rows = search("qantas", source=fixture_source)
    abns = {r["abn"] for r in rows}
    assert "11000000001" in abns


def test_search_individual_finds_legal_entity(fixture_source):
    rows = search("smith", search_in="individual", source=fixture_source)
    abns = {r["abn"] for r in rows}
    assert "11000000003" in abns
    assert all(r["matched_in"] == "individual" for r in rows)


def test_search_trading_finds_via_business_name(fixture_source):
    rows = search("acme global", search_in="trading", source=fixture_source)
    abns = {r["abn"] for r in rows}
    assert "11000000002" in abns
    assert all(r["matched_in"] == "trading" for r in rows)


def test_search_all_unions_main_and_trading(fixture_source):
    rows = search("acme", source=fixture_source)
    abns = {r["abn"] for r in rows}
    # ACME PTY LTD's main name AND ACME GLOBAL trading
    assert "11000000002" in abns


def test_search_respects_limit(fixture_source):
    rows = search("a", limit=2, source=fixture_source)
    assert len(rows) <= 2


# --- profile -------------------------------------------------------------


def test_profile_returns_full_nested_dict(fixture_source):
    p = profile("11000000001", source=fixture_source)
    assert p is not None
    assert p["abn"] == "11000000001"
    assert p["main_name"] == "QANTAS AIRWAYS LIMITED"
    assert p["entity_type"]["code"] == "PUB"
    assert p["address"]["state"] == "NSW"
    assert p["abn_status"]["status"] == "ACT"
    assert p["gst"]["status"] == "ACT"
    assert len(p["trading_names"]) == 2
    assert {t["name"] for t in p["trading_names"]} == {"QANTAS", "QANTAS WINE"}
    assert len(p["dgrs"]) == 1
    assert p["dgrs"][0]["name"] == "QANTAS FOUNDATION"


def test_profile_missing_returns_none(fixture_source):
    p = profile("99999999999", source=fixture_source)
    assert p is None


def test_profile_individual_block_for_legal_entity(fixture_source):
    p = profile("11000000003", source=fixture_source)
    assert p is not None
    assert p["main_name"] is None
    assert p["individual"] == {
        "title": "MR",
        "given_names": "JOHN",
        "family_name": "SMITH",
        "name_type": "LGL",
    }


def test_profile_individual_block_none_for_main_entity(fixture_source):
    p = profile("11000000001", source=fixture_source)
    assert p["individual"] is None


def test_profile_dates_are_iso_strings(fixture_source):
    p = profile("11000000001", source=fixture_source)
    assert p["abn_status"]["from"] == "1999-11-01"
    assert p["dgrs"][0]["from"] == "2010-01-01"


# --- trends --------------------------------------------------------------


def test_trends_registrations_by_month(fixture_source):
    rows = trends("registrations", since="2024-01-01", group_by="month", source=fixture_source)
    months = {r["month"] for r in rows}
    assert "2024-06" in months
    n_2024_06 = next(r["n"] for r in rows if r["month"] == "2024-06")
    assert n_2024_06 == 1


def test_trends_cancellations_filters_status(fixture_source):
    rows = trends("cancellations", since="2020-01-01", group_by="year", source=fixture_source)
    total = sum(r["n"] for r in rows)
    assert total == 2  # the IND record + OLD COMPANY


def test_trends_by_state_only_active(fixture_source):
    rows = trends("by_state", source=fixture_source)
    states = {r["state"] for r in rows}
    # OLD COMPANY (cancelled) and IND (cancelled) should be excluded
    assert "QLD" not in states
    assert "VIC" not in states
    assert "NSW" in states


def test_trends_by_entity_type_returns_code_and_text(fixture_source):
    rows = trends("by_entity_type", source=fixture_source)
    codes = {r["code"] for r in rows}
    # Only active ABNs counted
    assert "PUB" in codes
    assert "PRV" in codes
    assert "IND" not in codes  # cancelled in fixture


def test_trends_invalid_metric_raises():
    with pytest.raises(ValueError, match="Unknown metric"):
        trends("not_a_metric")
