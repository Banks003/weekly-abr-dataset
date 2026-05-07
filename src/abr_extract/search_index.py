"""Compact search-index Parquet for the frontend filter.

The full ``abn_main_history`` and ``abn_trading_names_history`` Iceberg
tables together carry ~28M rows and ~5GB of uncompressed data. Doing
browser-side ILIKE across both relations (current frontend behaviour:
four ILIKEs OR'd against ``main_name``, the three individual-name
columns, and a sub-query into trading names) is the single biggest
cause of search latency — even with DuckDB-WASM column pruning the
trading-names sub-query has to fetch and scan the trading parquet on
every keystroke.

This module builds ``abn-search.parquet``, a per-ABN denormalised view
containing only the fields the search UI actually needs, plus a single
pre-lowercased ``search_text`` column that concatenates every name a
user might type — main name, individual full name, and all trading
names — into one string. The frontend reads this index for the filter
(``WHERE search_text ILIKE ?``) and joins back to ``abn_main_history``
only for the visible page if it needs columns the index doesn't carry.

The result is one ILIKE on a much smaller, denormalised relation in
place of four ORs across two relations — typically a ~5-10x latency
drop on partial-name searches.
"""

from __future__ import annotations

from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

ABN_SEARCH_SCHEMA = pa.schema(
    [
        ("abn", pa.string()),
        # Display columns — enough to render a row in the table without
        # joining back to ``abn_main_history``.
        ("name", pa.string()),
        ("entity_kind", pa.string()),
        ("entity_type_ind", pa.string()),
        ("entity_type_text", pa.string()),
        ("state", pa.string()),
        ("postcode", pa.string()),
        ("abn_status", pa.string()),
        ("gst_status", pa.string()),
        ("asic_number", pa.string()),
        # Lowercased concat of every searchable name. ILIKE-ed by the UI.
        ("search_text", pa.string()),
    ]
)


_BUILD_SQL = """
WITH trading_concat AS (
    SELECT abn, string_agg(LOWER(name), ' ') AS trading_lower
    FROM read_parquet(?)
    GROUP BY abn
)
SELECT
    m.abn,
    COALESCE(
        m.main_name,
        TRIM(CONCAT_WS(
            ' ',
            m.individual_title,
            m.individual_given_names,
            m.individual_family_name
        ))
    ) AS name,
    m.entity_kind,
    m.entity_type_ind,
    m.entity_type_text,
    m.state,
    m.postcode,
    m.abn_status,
    m.gst_status,
    m.asic_number,
    LOWER(
        CONCAT_WS(
            ' ',
            m.main_name,
            m.individual_given_names,
            m.individual_family_name,
            t.trading_lower
        )
    ) AS search_text
FROM read_parquet(?) m
LEFT JOIN trading_concat t ON t.abn = m.abn
"""


def build_search_index(
    main_parquet: Path,
    trading_parquet: Path,
    dest: Path,
) -> int:
    """Build ``abn-search.parquet`` from the merged main + trading parquets.

    Uses DuckDB to do the join + name-concat in one set-based pass; iterating
    in Python over 20M rows would be the slow path. Returns the row count
    written.
    """
    import duckdb

    dest.parent.mkdir(parents=True, exist_ok=True)
    if dest.exists():
        dest.unlink()

    duck = duckdb.connect()
    try:
        # Build the index in-memory then write — gives us full schema control
        # and lets us round-trip through the canonical pyarrow schema, so
        # downstream readers (frontend DuckDB-WASM) see the column types
        # they expect.
        rel = duck.execute(_BUILD_SQL, [str(trading_parquet), str(main_parquet)])
        table = rel.to_arrow_table()
        # Project / cast into the canonical schema. ``cast(strict=False)``
        # tolerates NULLs vs. our nullable string columns; without this,
        # DuckDB's strict types occasionally trip up the cast.
        # PyArrow Table.cast(target_schema) coerces type-compatible columns.
        casted = table.cast(ABN_SEARCH_SCHEMA)
        with pq.ParquetWriter(dest, ABN_SEARCH_SCHEMA, compression="snappy") as w:
            w.write_table(casted)
    finally:
        duck.close()

    return pq.read_metadata(dest).num_rows
