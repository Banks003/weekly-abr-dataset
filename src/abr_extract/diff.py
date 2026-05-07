"""Weekly diff between two extracts.

Output shape is narrow-long: one row per (abn, change_type). A single ABN
with three field changes produces three rows. This makes per-change-type
filtering a one-liner ("WHERE change_type = 'added'").

Inputs are paths to two sets of parquet files (prev and new). Output is a
single Polars DataFrame matching DIFF_SCHEMA, ready to write_parquet.

When prev is missing (first run), this module returns an empty DataFrame
with the correct schema rather than raising.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import date
from pathlib import Path

import polars as pl
import pyarrow as pa

DIFF_SCHEMA = pa.schema(
    [
        ("abn", pa.string()),
        ("change_type", pa.string()),
        ("prev_value", pa.string()),
        ("new_value", pa.string()),
        ("prev_extract_date", pa.date32()),
        ("new_extract_date", pa.date32()),
    ]
)

POLARS_DIFF_SCHEMA = {
    "abn": pl.String,
    "change_type": pl.String,
    "prev_value": pl.String,
    "new_value": pl.String,
    "prev_extract_date": pl.Date,
    "new_extract_date": pl.Date,
}

# Scalar fields that produce one diff row per change.
SCALAR_FIELDS: tuple[str, ...] = (
    "abn_status",
    "gst_status",
    "state",
    "postcode",
    "main_name",
    "entity_type_ind",
)


@dataclass(frozen=True)
class DiffInputs:
    prev_main: Path
    prev_trading: Path
    prev_dgr: Path
    new_main: Path
    new_trading: Path
    new_dgr: Path


def empty_diff(prev_extract_date: date | None, new_extract_date: date) -> pl.DataFrame:
    return pl.DataFrame(schema=POLARS_DIFF_SCHEMA)


def build_diff(
    inputs: DiffInputs,
    *,
    prev_extract_date: date,
    new_extract_date: date,
) -> pl.DataFrame:
    prev_main = pl.read_parquet(inputs.prev_main)
    new_main = pl.read_parquet(inputs.new_main)

    rows: list[pl.DataFrame] = []
    rows.append(_diff_added_removed(prev_main, new_main, prev_extract_date, new_extract_date))
    rows.append(_diff_scalar_fields(prev_main, new_main, prev_extract_date, new_extract_date))
    rows.append(_diff_individual_name(prev_main, new_main, prev_extract_date, new_extract_date))

    prev_trading = pl.read_parquet(inputs.prev_trading)
    new_trading = pl.read_parquet(inputs.new_trading)
    rows.append(
        _diff_set_table(
            prev_trading,
            new_trading,
            key_cols=("abn", "name", "name_type"),
            payload_cols=("name", "name_type"),
            added_change_type="trading_name_added",
            removed_change_type="trading_name_removed",
            prev_extract_date=prev_extract_date,
            new_extract_date=new_extract_date,
        )
    )

    prev_dgr = pl.read_parquet(inputs.prev_dgr)
    new_dgr = pl.read_parquet(inputs.new_dgr)
    rows.append(
        _diff_set_table(
            prev_dgr,
            new_dgr,
            key_cols=("abn", "dgr_status_from_date"),
            payload_cols=("dgr_status_from_date", "dgr_status", "dgr_name"),
            added_change_type="dgr_added",
            removed_change_type="dgr_removed",
            prev_extract_date=prev_extract_date,
            new_extract_date=new_extract_date,
        )
    )

    out = pl.concat([df for df in rows if df.height > 0], how="vertical_relaxed")
    if out.height == 0:
        return empty_diff(prev_extract_date, new_extract_date)
    return out.sort(["abn", "change_type"])


def _diff_added_removed(
    prev: pl.DataFrame,
    new: pl.DataFrame,
    prev_date: date,
    new_date: date,
) -> pl.DataFrame:
    prev_abns = prev.select("abn")
    new_abns = new.select("abn")
    added = new_abns.join(prev_abns, on="abn", how="anti").with_columns(
        pl.lit("added").alias("change_type"),
        pl.lit(None, dtype=pl.String).alias("prev_value"),
        pl.lit(None, dtype=pl.String).alias("new_value"),
    )
    removed = prev_abns.join(new_abns, on="abn", how="anti").with_columns(
        pl.lit("removed").alias("change_type"),
        pl.lit(None, dtype=pl.String).alias("prev_value"),
        pl.lit(None, dtype=pl.String).alias("new_value"),
    )
    df = pl.concat([added, removed], how="vertical_relaxed")
    return _stamp_dates(df, prev_date, new_date)


def _diff_scalar_fields(
    prev: pl.DataFrame,
    new: pl.DataFrame,
    prev_date: date,
    new_date: date,
) -> pl.DataFrame:
    join = prev.join(new, on="abn", how="inner", suffix="__new")
    parts: list[pl.DataFrame] = []
    for field in SCALAR_FIELDS:
        prev_col = pl.col(field)
        new_col = pl.col(f"{field}__new")
        changed = join.filter(
            (prev_col != new_col)
            | (prev_col.is_null() & new_col.is_not_null())
            | (prev_col.is_not_null() & new_col.is_null())
        ).select(
            pl.col("abn"),
            pl.lit(field).alias("change_type"),
            prev_col.cast(pl.String).alias("prev_value"),
            new_col.cast(pl.String).alias("new_value"),
        )
        if changed.height > 0:
            parts.append(changed)
    if not parts:
        return _empty_with_dates(prev_date, new_date)
    df = pl.concat(parts, how="vertical_relaxed")
    return _stamp_dates(df, prev_date, new_date)


def _diff_individual_name(
    prev: pl.DataFrame,
    new: pl.DataFrame,
    prev_date: date,
    new_date: date,
) -> pl.DataFrame:
    join = prev.join(new, on="abn", how="inner", suffix="__new")
    fields = ("individual_title", "individual_given_names", "individual_family_name")
    cond = None
    for f in fields:
        c = (pl.col(f) != pl.col(f"{f}__new")) | (
            pl.col(f).is_null() ^ pl.col(f"{f}__new").is_null()
        )
        cond = c if cond is None else (cond | c)

    changed = join.filter(cond)
    if changed.height == 0:
        return _empty_with_dates(prev_date, new_date)

    rows = changed.select(
        "abn",
        *[pl.col(f) for f in fields],
        *[pl.col(f"{f}__new") for f in fields],
    ).to_dicts()

    out_rows = []
    for r in rows:
        prev_obj = {f: r[f] for f in fields}
        new_obj = {f: r[f"{f}__new"] for f in fields}
        out_rows.append(
            {
                "abn": r["abn"],
                "change_type": "individual_name",
                "prev_value": json.dumps(prev_obj, sort_keys=True),
                "new_value": json.dumps(new_obj, sort_keys=True),
            }
        )
    df = pl.DataFrame(out_rows)
    return _stamp_dates(df, prev_date, new_date)


def _diff_set_table(
    prev: pl.DataFrame,
    new: pl.DataFrame,
    *,
    key_cols: tuple[str, ...],
    payload_cols: tuple[str, ...],
    added_change_type: str,
    removed_change_type: str,
    prev_extract_date: date,
    new_extract_date: date,
) -> pl.DataFrame:
    added = new.join(prev, on=list(key_cols), how="anti")
    removed = prev.join(new, on=list(key_cols), how="anti")

    def _row_payload(df: pl.DataFrame) -> pl.Series:
        records = df.select(list(payload_cols)).to_dicts()
        return pl.Series(
            "payload",
            [json.dumps({k: _jsonable(v) for k, v in r.items()}, sort_keys=True) for r in records],
            dtype=pl.String,
        )

    parts: list[pl.DataFrame] = []
    if added.height > 0:
        df = added.select("abn").with_columns(
            pl.lit(added_change_type).alias("change_type"),
            pl.lit(None, dtype=pl.String).alias("prev_value"),
            _row_payload(added).alias("new_value"),
        )
        parts.append(df)
    if removed.height > 0:
        df = removed.select("abn").with_columns(
            pl.lit(removed_change_type).alias("change_type"),
            _row_payload(removed).alias("prev_value"),
            pl.lit(None, dtype=pl.String).alias("new_value"),
        )
        parts.append(df)
    if not parts:
        return _empty_with_dates(prev_extract_date, new_extract_date)
    combined = pl.concat(parts, how="vertical_relaxed")
    return _stamp_dates(combined, prev_extract_date, new_extract_date)


def _stamp_dates(df: pl.DataFrame, prev_date: date, new_date: date) -> pl.DataFrame:
    return df.with_columns(
        pl.lit(prev_date, dtype=pl.Date).alias("prev_extract_date"),
        pl.lit(new_date, dtype=pl.Date).alias("new_extract_date"),
    ).select(
        "abn", "change_type", "prev_value", "new_value", "prev_extract_date", "new_extract_date"
    )


def _empty_with_dates(prev_date: date, new_date: date) -> pl.DataFrame:
    return pl.DataFrame(schema=POLARS_DIFF_SCHEMA)


def _jsonable(value):
    if isinstance(value, date):
        return value.isoformat()
    return value


def diff_summary(diff: pl.DataFrame) -> dict[str, int]:
    """Row counts grouped by change_type, for the manifest."""
    if diff.height == 0:
        return {}
    grouped = diff.group_by("change_type").agg(pl.len().alias("n")).to_dicts()
    return {row["change_type"]: row["n"] for row in grouped}
