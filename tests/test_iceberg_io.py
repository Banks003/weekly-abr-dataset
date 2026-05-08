from __future__ import annotations

from datetime import date
from pathlib import Path

import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from abr_extract.iceberg_io import (
    ABN_MAIN_HISTORY_SCHEMA,
    TABLE_SCHEMAS,
    _s3_to_public_url,
    build_snapshot_manifest,
    connect_local_catalog,
    ensure_history_tables,
    overwrite_table_from_parquet,
    prune_iceberg_snapshots,
)
from abr_extract.schema import ABN_MAIN_SCHEMA


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


def _write_main_parquet(rows: list[dict], path: Path) -> Path:
    pq.write_table(pa.Table.from_pylist(rows, schema=ABN_MAIN_SCHEMA), path)
    return path


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


def test_ensure_history_tables_drops_and_recreates_on_schema_mismatch(catalog):
    """An old SCD2-shaped table should be dropped + recreated with the new schema.

    Simulates the migration: a table exists with extra ``valid_from`` /
    ``valid_to`` / ``snapshot_observed_date`` columns. ``ensure_table``
    should detect the mismatch via ``_schemas_match`` and drop+recreate.
    """
    from pyiceberg.partitioning import UNPARTITIONED_PARTITION_SPEC
    from pyiceberg.schema import Schema
    from pyiceberg.types import DateType, NestedField, StringType

    old_schema = Schema(
        NestedField(1, "abn", StringType(), required=True),
        NestedField(2, "abn_status", StringType()),
        NestedField(3, "valid_from", DateType(), required=True),
        NestedField(4, "valid_to", DateType()),
    )
    catalog.create_namespace_if_not_exists("abr_test")
    catalog.create_table(
        "abr_test.abn_main_history",
        schema=old_schema,
        partition_spec=UNPARTITIONED_PARTITION_SPEC,
    )

    # ensure_history_tables should detect the mismatch and recreate
    tables = ensure_history_tables(catalog, namespace="abr_test")
    new_fields = {f.name for f in tables["abn_main_history"].schema().fields}
    # SCD2 columns gone, full main schema present
    assert "valid_from" not in new_fields
    assert "valid_to" not in new_fields
    assert "snapshot_observed_date" not in new_fields
    assert "main_name" in new_fields
    assert "abn_status_from_date" in new_fields


# --- overwrite + read round-trip ----------------------------------------


def test_overwrite_table_from_parquet_writes_rows(tables, tmp_path):
    parquet_path = _write_main_parquet([_main_row()], tmp_path / "w1.parquet")
    summary = overwrite_table_from_parquet(tables["abn_main_history"], parquet_path)
    assert summary["rows_written"] == 1

    table = tables["abn_main_history"]
    table.refresh()
    df = pl.from_arrow(table.scan().to_arrow())
    assert df.height == 1
    assert df["abn"].to_list() == ["11111111111"]
    assert df["main_name"].to_list() == ["ACME PTY LTD"]


def test_overwrite_replaces_all_rows(tables, tmp_path):
    p1 = _write_main_parquet([_main_row(abn="11111111111")], tmp_path / "w1.parquet")
    p2 = _write_main_parquet([_main_row(abn="22222222222")], tmp_path / "w2.parquet")

    overwrite_table_from_parquet(tables["abn_main_history"], p1)
    overwrite_table_from_parquet(tables["abn_main_history"], p2)

    table = tables["abn_main_history"]
    table.refresh()
    df = pl.from_arrow(table.scan().to_arrow())
    assert df["abn"].to_list() == ["22222222222"]


def test_overwrite_creates_snapshots(tables, tmp_path):
    """Each weekly overwrite contributes to Iceberg's snapshot history.

    PyIceberg's ``table.overwrite`` is implemented as DELETE + APPEND
    in two snapshots per call (except the first, where DELETE is a
    no-op skipped because there's nothing to remove). The exact ratio
    is an implementation detail of pyiceberg; what matters for our
    time-travel use is that the snapshot count climbs monotonically
    so consumers can read older states.
    """
    table = tables["abn_main_history"]
    counts = []
    for i in range(3):
        overwrite_table_from_parquet(
            table,
            _write_main_parquet([_main_row(state=f"S{i}")], tmp_path / f"w{i}.parquet"),
        )
        table.refresh()
        counts.append(len(list(table.metadata.snapshots)))
    # Strictly increasing.
    assert counts == sorted(set(counts)) and len(counts) == len(set(counts))
    # At least one snapshot per overwrite, possibly more (DELETE+APPEND pair).
    assert counts[-1] >= 3


# --- schema invariants --------------------------------------------------


def test_main_history_schema_excludes_scd2_columns():
    """SCD2 columns must NOT be in the post-migration schema."""
    fields = {f.name for f in ABN_MAIN_HISTORY_SCHEMA.fields}
    assert "valid_from" not in fields
    assert "valid_to" not in fields
    assert "snapshot_observed_date" not in fields


def test_table_schemas_dict_is_exhaustive():
    assert set(TABLE_SCHEMAS.keys()) == {
        "abn_main_history",
        "abn_trading_names_history",
        "abn_dgr_history",
    }


# --- snapshot manifest ---------------------------------------------------


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


def test_build_snapshot_manifest_lists_tables_with_data_files(tables, tmp_path):
    overwrite_table_from_parquet(
        tables["abn_main_history"],
        _write_main_parquet([_main_row()], tmp_path / "w1.parquet"),
    )

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
    main_block = out["tables"]["abn_main_history"]
    assert main_block["row_count"] == 1
    assert main_block["snapshot_id"] is not None
    # No more SCD2 view filter — table is current-state-only
    assert "current_view_filter" not in main_block
    assert len(main_block["data_files"]) >= 1
    for url in main_block["data_files"]:
        assert url.endswith(".parquet")

    # Tables that were never written to should still appear, with no data files.
    trading_block = out["tables"]["abn_trading_names_history"]
    assert trading_block["row_count"] == 0
    assert trading_block["data_files"] == []


# --- snapshot pruning ----------------------------------------------------


def test_prune_iceberg_snapshots_keeps_recent_n(tables, tmp_path):
    """After multiple weekly overwrites, pruning should leave only ``keep`` snapshots."""
    table = tables["abn_main_history"]
    for i in range(4):
        overwrite_table_from_parquet(
            table,
            _write_main_parquet([_main_row(state=f"S{i}")], tmp_path / f"w{i}.parquet"),
        )
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
    overwrite_table_from_parquet(
        table, _write_main_parquet([_main_row()], tmp_path / "w1.parquet")
    )
    table.refresh()

    summary = prune_iceberg_snapshots(table, keep=5)
    assert summary["expired"] == 0
    assert summary["kept"] == 1
