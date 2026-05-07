"""Query functions for the published dataset.

Backed by DuckDB-over-Parquet so the same code works against:
- Live R2 (default): https://pub-24e45cc5c0384bf1b367427d9777b0a8.r2.dev
- Local files: pass a directory path containing abn-main-latest.parquet etc.

CLI commands wrap these. Worker reuses the same logic via a JS port (later).

Output shape for profiles is the OpenCorporates-style nested dict:
embedded `trading_names` and `dgr` arrays per ABN.

enrich_profile_live() optionally calls the official ABR JSON web service
to add fields the bulk extract doesn't carry (name history, trading-name
effective-from/to dates, GST to-date). Needs an ABR_API_GUID.
"""

from __future__ import annotations

import json as _json
from pathlib import Path
from typing import Any

import duckdb
import httpx

DEFAULT_SOURCE = "https://gazetteer.au"
ABR_JSON_URL = "https://abr.business.gov.au/json/AbnDetails.aspx"

PARQUET_FILES = {
    "main": "abn-main-latest.parquet",
    "trading": "abn-trading-names-latest.parquet",
    "dgr": "abn-dgr-latest.parquet",
}


def _resolve_paths(source: str, cache_dir: str | Path | None = None) -> dict[str, str]:
    """source: HTTPS base URL or a local directory containing the parquets.

    If cache_dir is given, files are downloaded once into it (if missing) and
    queries hit the local paths. Subsequent calls reuse the cached files.
    """
    base = source.rstrip("/")
    if cache_dir is None:
        return {key: f"{base}/{name}" for key, name in PARQUET_FILES.items()}

    cache = Path(cache_dir)
    cache.mkdir(parents=True, exist_ok=True)
    resolved: dict[str, str] = {}
    for key, name in PARQUET_FILES.items():
        local = cache / name
        if not local.exists() or local.stat().st_size == 0:
            _download_into(f"{base}/{name}", local)
        resolved[key] = str(local)
    return resolved


def _download_into(url: str, dest: Path) -> None:
    """Stream a remote file into dest atomically (.part rename on success)."""
    part = dest.with_suffix(dest.suffix + ".part")
    timeout = httpx.Timeout(30.0, read=300.0)
    with (
        httpx.Client(timeout=timeout, follow_redirects=True) as client,
        client.stream("GET", url) as resp,
    ):
        resp.raise_for_status()
        with part.open("wb") as f:
            for chunk in resp.iter_bytes(1 << 20):
                f.write(chunk)
    part.replace(dest)


def _connect(source: str) -> duckdb.DuckDBPyConnection:
    con = duckdb.connect()
    if source.startswith("http"):
        con.execute("INSTALL httpfs; LOAD httpfs")
    return con


def search(
    query: str,
    *,
    limit: int = 20,
    search_in: str = "all",
    source: str = DEFAULT_SOURCE,
    cache_dir: str | Path | None = None,
) -> list[dict[str, Any]]:
    """Search for ABNs by name.

    search_in: "main" (main_name only), "trading" (trading names),
               "individual" (sole-trader names), or "all" (all three).
    """
    paths = _resolve_paths(source, cache_dir)
    con = _connect("file" if cache_dir else source)
    pattern = f"%{query}%"

    parts: list[str] = []
    if search_in in ("all", "main"):
        parts.append(
            f"""
            SELECT abn, main_name AS matched_name, 'main' AS matched_in,
                   entity_type_text, state, postcode, abn_status, gst_status
            FROM read_parquet('{paths["main"]}')
            WHERE main_name ILIKE ?
            """
        )
    if search_in in ("all", "individual"):
        parts.append(
            f"""
            SELECT abn,
                   CONCAT_WS(' ',
                       individual_title,
                       individual_given_names,
                       individual_family_name) AS matched_name,
                   'individual' AS matched_in,
                   entity_type_text, state, postcode, abn_status, gst_status
            FROM read_parquet('{paths["main"]}')
            WHERE individual_family_name ILIKE ?
               OR individual_given_names ILIKE ?
            """
        )
    if search_in in ("all", "trading"):
        parts.append(
            f"""
            SELECT m.abn, t.name AS matched_name, 'trading' AS matched_in,
                   m.entity_type_text, m.state, m.postcode, m.abn_status, m.gst_status
            FROM read_parquet('{paths["trading"]}') t
            JOIN read_parquet('{paths["main"]}') m USING (abn)
            WHERE t.name ILIKE ?
            """
        )

    sql = f"""
    SELECT * FROM (
        {' UNION ALL '.join(parts)}
    )
    ORDER BY matched_name
    LIMIT ?
    """

    n_main = 1 if search_in in ("all", "main") else 0
    n_individual = 2 if search_in in ("all", "individual") else 0
    n_trading = 1 if search_in in ("all", "trading") else 0
    params = [pattern] * (n_main + n_individual + n_trading) + [limit]

    rows = con.execute(sql, params).to_arrow_table().to_pylist()
    return rows


def profile(
    abn: str,
    *,
    source: str = DEFAULT_SOURCE,
    cache_dir: str | Path | None = None,
) -> dict[str, Any] | None:
    """Full nested profile for one ABN. Returns None if not found."""
    paths = _resolve_paths(source, cache_dir)
    con = _connect("file" if cache_dir else source)

    main_rows = (
        con.execute(
            f"SELECT * FROM read_parquet('{paths['main']}') WHERE abn = ?",
            [abn],
        )
        .to_arrow_table()
        .to_pylist()
    )
    if not main_rows:
        return None
    m = main_rows[0]

    trading = (
        con.execute(
            f"SELECT name, name_type FROM read_parquet('{paths['trading']}') "
            "WHERE abn = ? ORDER BY name_type, name",
            [abn],
        )
        .to_arrow_table()
        .to_pylist()
    )
    dgr = (
        con.execute(
            f"SELECT dgr_status_from_date AS \"from\", dgr_status AS status, "
            f"dgr_name AS name FROM read_parquet('{paths['dgr']}') WHERE abn = ?",
            [abn],
        )
        .to_arrow_table()
        .to_pylist()
    )

    return {
        "abn": m["abn"],
        "main_name": m["main_name"],
        "individual": _individual_block(m),
        "entity_type": {"code": m["entity_type_ind"], "text": m["entity_type_text"]},
        "address": {"state": m["state"], "postcode": m["postcode"]},
        "abn_status": {"status": m["abn_status"], "from": _iso(m["abn_status_from_date"])},
        "gst": (
            {"status": m["gst_status"], "from": _iso(m["gst_status_from_date"])}
            if m["gst_status"]
            else None
        ),
        "asic_number": m["asic_number"],
        "asic_number_type": m["asic_number_type"],
        "record_last_updated": _iso(m["record_last_updated"]),
        "replaced": m["replaced"],
        "trading_names": [{"name": t["name"], "type": t["name_type"]} for t in trading],
        "dgrs": [
            {"name": d["name"], "status": d["status"], "from": _iso(d["from"])} for d in dgr
        ],
    }


def trends(
    metric: str,
    *,
    since: str = "2020-01-01",
    group_by: str = "month",
    source: str = DEFAULT_SOURCE,
    cache_dir: str | Path | None = None,
) -> list[dict[str, Any]]:
    """Pre-baked aggregations.

    metric:
      - registrations:   ABNs by abn_status_from_date
      - cancellations:   cancelled ABNs by abn_status_from_date (status='CAN')
      - by_state:        active ABN counts by state (no time grouping)
      - by_entity_type:  active ABN counts by entity type (no time grouping)
    """
    paths = _resolve_paths(source, cache_dir)
    con = _connect("file" if cache_dir else source)

    if metric in ("registrations", "cancellations"):
        date_format = "%Y-%m" if group_by == "month" else "%Y"
        bucket_label = "month" if group_by == "month" else "year"
        status_filter = "abn_status = 'CAN'" if metric == "cancellations" else "TRUE"
        sql = f"""
            SELECT strftime(abn_status_from_date, '{date_format}') AS {bucket_label},
                   COUNT(*) AS n
            FROM read_parquet('{paths["main"]}')
            WHERE abn_status_from_date >= ?
              AND {status_filter}
              AND abn_status_from_date IS NOT NULL
            GROUP BY 1
            ORDER BY 1
        """
        return con.execute(sql, [since]).to_arrow_table().to_pylist()

    if metric == "by_state":
        sql = f"""
            SELECT state, COUNT(*) AS n
            FROM read_parquet('{paths["main"]}')
            WHERE abn_status = 'ACT' AND state IS NOT NULL
            GROUP BY state ORDER BY n DESC
        """
        return con.execute(sql).to_arrow_table().to_pylist()

    if metric == "by_entity_type":
        sql = f"""
            SELECT entity_type_ind AS code, entity_type_text AS text, COUNT(*) AS n
            FROM read_parquet('{paths["main"]}')
            WHERE abn_status = 'ACT'
            GROUP BY 1, 2 ORDER BY n DESC
        """
        return con.execute(sql).to_arrow_table().to_pylist()

    raise ValueError(f"Unknown metric: {metric}")


def enrich_profile_live(
    abn: str,
    *,
    guid: str,
    client: httpx.Client | None = None,
) -> dict[str, Any]:
    """Call the official ABR JSON web service for fields not in the bulk extract.

    Returns:
      {
        "name_history":          [{name, type, from, to}, ...],
        "trading_names_history": [{name, type, from, to}, ...],
        "gst_to":                str | None,
        "address":               {state, postcode, from},
        "raw":                   <full upstream payload, for debugging>,
      }

    The service sometimes returns JSONP wrapped in `callback(...)`; we strip it.
    """
    own_client = client is None
    if own_client:
        client = httpx.Client(timeout=15.0)
    try:
        resp = client.get(ABR_JSON_URL, params={"abn": abn, "guid": guid})
        resp.raise_for_status()
        text = resp.text.strip()
        if text.startswith("callback("):
            text = text[len("callback("):-1]
        payload = _json.loads(text)
    finally:
        if own_client:
            client.close()

    return {
        "name_history": _name_history_from(payload),
        "trading_names_history": _trading_history_from(payload),
        "gst_to": _gst_to_from(payload),
        "address": _address_from(payload),
        "raw": payload,
    }


def _name_history_from(payload: dict) -> list[dict]:
    out = []
    for n in payload.get("HistoricalNames", []) or []:
        out.append(
            {
                "name": n.get("OrganisationName") or n.get("FullName"),
                "type": n.get("OrganisationNameType") or n.get("NameType"),
                "from": n.get("EffectiveFrom"),
                "to": n.get("EffectiveTo"),
            }
        )
    return out


def _trading_history_from(payload: dict) -> list[dict]:
    out = []
    for n in payload.get("BusinessName", []) or []:
        out.append(
            {
                "name": n.get("OrganisationName"),
                "type": "BN",
                "from": n.get("EffectiveFrom"),
                "to": n.get("EffectiveTo"),
            }
        )
    return out


def _gst_to_from(payload: dict) -> str | None:
    return payload.get("GstStatusToDate")


def _address_from(payload: dict) -> dict | None:
    state = payload.get("AddressState")
    postcode = payload.get("AddressPostcode")
    eff_from = payload.get("AddressDate")
    if not (state or postcode):
        return None
    return {"state": state, "postcode": postcode, "from": eff_from}


def _individual_block(m: dict) -> dict | None:
    if m["entity_kind"] != "legal" or not m["individual_family_name"]:
        return None
    return {
        "title": m["individual_title"],
        "given_names": m["individual_given_names"],
        "family_name": m["individual_family_name"],
        "name_type": m["individual_name_type"],
    }


def _iso(d) -> str | None:
    if d is None:
        return None
    return d.isoformat()
