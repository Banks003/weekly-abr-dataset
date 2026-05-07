"""Output schemas for the three relations and helpers for type conversion.

Schema shape (three-table normalisation, snake_case naming) follows
iangow/abn_lookup — see the README for full credits. The Parquet writers
are pyarrow-based for true streaming append support (Polars can't append
to a single Parquet file). SQLite uses standard SQL.
"""

from __future__ import annotations

from datetime import date

import pyarrow as pa

from .parse import RawRecord

# pyarrow schemas — used by the streaming Parquet writers.

ABN_MAIN_SCHEMA = pa.schema(
    [
        ("abn", pa.string()),
        ("abn_status", pa.string()),  # ACT | CAN
        ("abn_status_from_date", pa.date32()),
        ("record_last_updated", pa.date32()),
        ("replaced", pa.string()),  # Y | N
        ("entity_type_ind", pa.string()),
        ("entity_type_text", pa.string()),
        ("entity_kind", pa.string()),  # main | legal
        ("main_name", pa.string()),
        ("main_name_type", pa.string()),
        ("individual_title", pa.string()),
        ("individual_given_names", pa.string()),
        ("individual_family_name", pa.string()),
        ("individual_name_type", pa.string()),
        ("state", pa.string()),
        ("postcode", pa.string()),
        ("asic_number", pa.string()),
        ("asic_number_type", pa.string()),
        ("gst_status", pa.string()),
        ("gst_status_from_date", pa.date32()),
    ]
)

ABN_TRADING_NAMES_SCHEMA = pa.schema(
    [
        ("abn", pa.string()),
        ("name", pa.string()),
        ("name_type", pa.string()),
    ]
)

ABN_DGR_SCHEMA = pa.schema(
    [
        ("abn", pa.string()),
        ("dgr_status_from_date", pa.date32()),
        ("dgr_status", pa.string()),
        ("dgr_name", pa.string()),
    ]
)


# SQLite DDL — same shape, dates stored as ISO strings (SQLite lacks a date type).

SQLITE_DDL = [
    """
    CREATE TABLE IF NOT EXISTS abn_main (
        abn TEXT PRIMARY KEY,
        abn_status TEXT,
        abn_status_from_date TEXT,
        record_last_updated TEXT,
        replaced TEXT,
        entity_type_ind TEXT,
        entity_type_text TEXT,
        entity_kind TEXT,
        main_name TEXT,
        main_name_type TEXT,
        individual_title TEXT,
        individual_given_names TEXT,
        individual_family_name TEXT,
        individual_name_type TEXT,
        state TEXT,
        postcode TEXT,
        asic_number TEXT,
        asic_number_type TEXT,
        gst_status TEXT,
        gst_status_from_date TEXT
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS abn_trading_names (
        abn TEXT NOT NULL,
        name TEXT NOT NULL,
        name_type TEXT
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS abn_dgr (
        abn TEXT NOT NULL,
        dgr_status_from_date TEXT,
        dgr_status TEXT,
        dgr_name TEXT
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_trading_names_abn ON abn_trading_names(abn)",
    "CREATE INDEX IF NOT EXISTS idx_dgr_abn ON abn_dgr(abn)",
    "CREATE INDEX IF NOT EXISTS idx_main_state ON abn_main(state)",
]


def parse_yyyymmdd(s: str | None) -> date | None:
    """Parse YYYYMMDD into a date. Empty/None/malformed returns None.

    The sentinel 19000101 used by the ABR for "not real" GST dates is preserved
    as 1900-01-01; downstream queries can filter on gst_status='NON'.
    """
    if not s:
        return None
    s = s.strip()
    if len(s) != 8 or not s.isdigit():
        return None
    try:
        return date(int(s[0:4]), int(s[4:6]), int(s[6:8]))
    except ValueError:
        return None


def record_to_main_row(r: RawRecord) -> dict:
    given = " ".join(r.individual_given_names) if r.individual_given_names else None
    return {
        "abn": r.abn,
        "abn_status": r.abn_status or None,
        "abn_status_from_date": parse_yyyymmdd(r.abn_status_from_date),
        "record_last_updated": parse_yyyymmdd(r.record_last_updated),
        "replaced": r.replaced or None,
        "entity_type_ind": r.entity_type_ind or None,
        "entity_type_text": r.entity_type_text or None,
        "entity_kind": r.entity_kind,
        "main_name": r.main_name,
        "main_name_type": r.main_name_type,
        "individual_title": r.individual_title,
        "individual_given_names": given,
        "individual_family_name": r.individual_family_name,
        "individual_name_type": r.individual_name_type,
        "state": r.state,
        "postcode": r.postcode,
        "asic_number": r.asic_number,
        "asic_number_type": r.asic_number_type,
        "gst_status": r.gst_status,
        "gst_status_from_date": parse_yyyymmdd(r.gst_status_from_date),
    }


def record_to_trading_rows(r: RawRecord) -> list[dict]:
    return [
        {"abn": r.abn, "name": tn.name, "name_type": tn.name_type or None}
        for tn in r.trading_names
    ]


def record_to_dgr_rows(r: RawRecord) -> list[dict]:
    return [
        {
            "abn": r.abn,
            "dgr_status_from_date": parse_yyyymmdd(d.status_from_date),
            "dgr_status": d.status,
            "dgr_name": d.name,
        }
        for d in r.dgrs
    ]
