"""Demo: query the live R2 dataset via DuckDB without downloading the parquet.

DuckDB ranges over the parquet footer + matched row groups, so even a 773 MB
file responds in seconds for selective queries. The same SQL works against
local parquet paths once you have the file locally.
"""

from __future__ import annotations

import time

import duckdb
import polars as pl

MAIN_URL = "https://pub-24e45cc5c0384bf1b367427d9777b0a8.r2.dev/abn-main-latest.parquet"
TRADING_URL = "https://pub-24e45cc5c0384bf1b367427d9777b0a8.r2.dev/abn-trading-names-latest.parquet"


def section(title: str) -> None:
    print(f"\n{'=' * 70}\n  {title}\n{'=' * 70}")


def run_query(con: duckdb.DuckDBPyConnection, query: str, label: str) -> None:
    section(label)
    print(f"SQL:\n{query.strip()}\n")
    t0 = time.monotonic()
    arrow = con.execute(query).to_arrow_table()
    elapsed = time.monotonic() - t0
    columns = arrow.column_names
    print(f"Result ({elapsed:.1f}s, {arrow.num_rows} rows):")
    print("  " + " | ".join(columns))
    print("  " + "-+-".join("-" * len(c) for c in columns))
    rows = arrow.to_pylist()
    for row in rows:
        cells = [str(row[c]) if row[c] is not None else "" for c in columns]
        print("  " + " | ".join(cells))


def main() -> None:
    con = duckdb.connect()
    con.execute("INSTALL httpfs; LOAD httpfs")

    run_query(
        con,
        f"""
        SELECT abn, main_name, entity_type_text, state, postcode, abn_status, gst_status
        FROM read_parquet('{MAIN_URL}')
        WHERE main_name ILIKE '%qantas%' AND abn_status = 'ACT'
        ORDER BY main_name
        LIMIT 10
        """,
        "1. Active companies with 'qantas' in the main name",
    )

    run_query(
        con,
        f"""
        SELECT abn, main_name, individual_given_names, individual_family_name,
               entity_type_text, state, abn_status
        FROM read_parquet('{MAIN_URL}')
        WHERE abn = '11000000948'
        """,
        "2. Lookup by exact ABN (QBE Insurance, from our fixtures)",
    )

    run_query(
        con,
        f"""
        SELECT entity_type_text, COUNT(*) AS n
        FROM read_parquet('{MAIN_URL}')
        WHERE abn_status = 'ACT'
        GROUP BY entity_type_text
        ORDER BY n DESC
        LIMIT 10
        """,
        "3. Top 10 active entity types in Australia",
    )

    run_query(
        con,
        f"""
        SELECT m.abn, m.main_name, COUNT(t.name) AS trading_names
        FROM read_parquet('{MAIN_URL}') AS m
        JOIN read_parquet('{TRADING_URL}') AS t USING (abn)
        WHERE m.abn_status = 'ACT' AND m.main_name IS NOT NULL
        GROUP BY m.abn, m.main_name
        HAVING COUNT(t.name) > 30
        ORDER BY trading_names DESC
        LIMIT 5
        """,
        "4. Active companies with 30+ trading/business names (operational complexity proxy)",
    )

    run_query(
        con,
        f"""
        SELECT state, COUNT(*) AS active_abns
        FROM read_parquet('{MAIN_URL}')
        WHERE abn_status = 'ACT' AND state IS NOT NULL
        GROUP BY state
        ORDER BY active_abns DESC
        """,
        "5. Active ABNs by state",
    )


if __name__ == "__main__":
    main()
