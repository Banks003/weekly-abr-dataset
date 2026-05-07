"""Slowly Changing Dimension Type 2 reconciliation, storage-agnostic.

Operates on Polars DataFrames. Storage to Iceberg / R2 Data Catalog happens
in iceberg_io.py — this module is pure logic for testability.

The SCD2 contract:
- Each row has valid_from (date) and valid_to (date | null).
- A row with valid_to = null is "currently true" (the open version).
- When content changes for an entity, the open row is closed (valid_to set
  to the new extract date) and a new row is appended with valid_from = new
  extract date, valid_to = null.
- snapshot_observed_date records when our pipeline first saw this version.
  Same as valid_from for non-bootstrap updates; for bootstrap rows it equals
  the bootstrap date even though valid_from may be earlier (taken from ABR's
  record_last_updated signal).

For child tables (trading_names, dgr) the identity is the row's compound
key, not just the abn. Adding/removing a trading name produces an
add/close pair against the (abn, name, name_type) tuple.
"""

from __future__ import annotations

from datetime import date

import polars as pl

SCD2_COLUMNS = ("valid_from", "valid_to", "snapshot_observed_date")

# For each table, the columns whose values define "content equality" — a
# change in any of these for the same identity triggers a close-and-append.
# valid_from / valid_to / snapshot_observed_date are SCD2 metadata, never
# part of content equality.

MAIN_IDENTITY = ("abn",)
MAIN_CONTENT_COLUMNS = (
    "abn_status",
    "abn_status_from_date",
    "record_last_updated",
    "replaced",
    "entity_type_ind",
    "entity_type_text",
    "entity_kind",
    "main_name",
    "main_name_type",
    "individual_title",
    "individual_given_names",
    "individual_family_name",
    "individual_name_type",
    "state",
    "postcode",
    "asic_number",
    "asic_number_type",
    "gst_status",
    "gst_status_from_date",
)

TRADING_IDENTITY = ("abn", "name", "name_type")
TRADING_CONTENT_COLUMNS = ()  # no content beyond identity — pure set membership

DGR_IDENTITY = ("abn", "dgr_status_from_date")
DGR_CONTENT_COLUMNS = ("dgr_status", "dgr_name")


def _add_scd2_columns(df: pl.DataFrame, valid_from: date, observed: date) -> pl.DataFrame:
    return df.with_columns(
        pl.lit(valid_from, dtype=pl.Date).alias("valid_from"),
        pl.lit(None, dtype=pl.Date).alias("valid_to"),
        pl.lit(observed, dtype=pl.Date).alias("snapshot_observed_date"),
    )


def bootstrap_main(snapshot: pl.DataFrame, extract_date: date) -> pl.DataFrame:
    """Initial SCD2 frame from the first-ever main snapshot.

    Uses ABR's own record_last_updated as valid_from where present so the
    bootstrap row has at least a hint of when the current value started
    being true; falls back to extract_date when null.
    """
    return snapshot.with_columns(
        pl.coalesce(pl.col("record_last_updated"), pl.lit(extract_date)).alias("valid_from"),
        pl.lit(None, dtype=pl.Date).alias("valid_to"),
        pl.lit(extract_date, dtype=pl.Date).alias("snapshot_observed_date"),
    )


def bootstrap_trading(snapshot: pl.DataFrame, extract_date: date) -> pl.DataFrame:
    return _add_scd2_columns(snapshot, valid_from=extract_date, observed=extract_date)


def bootstrap_dgr(snapshot: pl.DataFrame, extract_date: date) -> pl.DataFrame:
    return _add_scd2_columns(snapshot, valid_from=extract_date, observed=extract_date)


def apply_update_main(
    prev_history: pl.DataFrame,
    new_snapshot: pl.DataFrame,
    extract_date: date,
) -> pl.DataFrame:
    return _apply_update(
        prev_history,
        new_snapshot,
        extract_date,
        identity=list(MAIN_IDENTITY),
        content_columns=list(MAIN_CONTENT_COLUMNS),
    )


def apply_update_trading(
    prev_history: pl.DataFrame,
    new_snapshot: pl.DataFrame,
    extract_date: date,
) -> pl.DataFrame:
    return _apply_update(
        prev_history,
        new_snapshot,
        extract_date,
        identity=list(TRADING_IDENTITY),
        content_columns=list(TRADING_CONTENT_COLUMNS),
    )


def apply_update_dgr(
    prev_history: pl.DataFrame,
    new_snapshot: pl.DataFrame,
    extract_date: date,
) -> pl.DataFrame:
    return _apply_update(
        prev_history,
        new_snapshot,
        extract_date,
        identity=list(DGR_IDENTITY),
        content_columns=list(DGR_CONTENT_COLUMNS),
    )


def _apply_update(
    prev_history: pl.DataFrame,
    new_snapshot: pl.DataFrame,
    extract_date: date,
    *,
    identity: list[str],
    content_columns: list[str],
) -> pl.DataFrame:
    """Generic SCD2 reconciliation.

    1. Already-closed rows pass through unchanged.
    2. Open rows whose identity-content matches the new snapshot pass through.
    3. Open rows whose content changed get valid_to stamped (closed).
    4. Open rows whose identity disappeared from new get valid_to stamped.
    5. New identities and changed identities get a fresh open row appended.
    """
    closed_prev = prev_history.filter(pl.col("valid_to").is_not_null())
    open_prev = prev_history.filter(pl.col("valid_to").is_null())

    if open_prev.height == 0:
        # First update after a snapshot with zero rows — treat all of new as added.
        new_open = _add_scd2_columns(new_snapshot, extract_date, extract_date)
        return pl.concat([closed_prev, new_open], how="vertical_relaxed")

    open_prev_keyed = open_prev.with_columns(_content_hash(open_prev, content_columns).alias("_h"))
    new_keyed = new_snapshot.with_columns(_content_hash(new_snapshot, content_columns).alias("_h"))

    joined = open_prev_keyed.join(
        new_keyed.select([*identity, "_h"]),
        on=identity,
        how="full",
        coalesce=True,
        suffix="__new",
    )

    unchanged_keys = joined.filter(
        (pl.col("_h").is_not_null())
        & (pl.col("_h__new").is_not_null())
        & (pl.col("_h") == pl.col("_h__new"))
    ).select(identity)

    changed_keys = joined.filter(
        (pl.col("_h").is_not_null())
        & (pl.col("_h__new").is_not_null())
        & (pl.col("_h") != pl.col("_h__new"))
    ).select(identity)

    removed_keys = joined.filter(
        (pl.col("_h").is_not_null()) & (pl.col("_h__new").is_null())
    ).select(identity)

    added_keys = joined.filter(
        (pl.col("_h").is_null()) & (pl.col("_h__new").is_not_null())
    ).select(identity)

    keep_open = open_prev.join(unchanged_keys, on=identity, how="semi")

    to_close_keys = pl.concat([changed_keys, removed_keys], how="vertical_relaxed")
    closed_now = open_prev.join(to_close_keys, on=identity, how="semi").with_columns(
        pl.lit(extract_date, dtype=pl.Date).alias("valid_to")
    )

    new_versions_keys = pl.concat([changed_keys, added_keys], how="vertical_relaxed")
    new_versions = new_snapshot.join(new_versions_keys, on=identity, how="semi").pipe(
        _add_scd2_columns, valid_from=extract_date, observed=extract_date
    )

    return pl.concat(
        [closed_prev, keep_open, closed_now, new_versions],
        how="vertical_relaxed",
    )


def _content_hash(df: pl.DataFrame, content_columns: list[str]) -> pl.Expr:
    """Stable content hash for change-detection.

    For pure set-membership tables (TRADING_IDENTITY) content_columns is empty
    and every row hashes to the same constant — so the join collapses to a
    pure presence test.
    """
    if not content_columns:
        return pl.lit("__set__", dtype=pl.String)
    cast = [pl.col(c).cast(pl.String).fill_null("__null__") for c in content_columns]
    return pl.concat_str(cast, separator="")


def history_summary(history: pl.DataFrame) -> dict:
    if history.height == 0:
        return {"total_rows": 0, "open_rows": 0, "closed_rows": 0}
    open_n = history.filter(pl.col("valid_to").is_null()).height
    return {
        "total_rows": history.height,
        "open_rows": open_n,
        "closed_rows": history.height - open_n,
    }
