"""Tests for the local-cache code path of query._resolve_paths.

We don't actually download from the live R2; the test serves a tiny parquet
over a local httpx MockTransport-backed pseudo-server pattern instead. The
production behaviour we care about (download once, reuse on second call)
is validated by counting downloads.
"""

from __future__ import annotations

from datetime import date
from pathlib import Path
from unittest.mock import patch

import pyarrow as pa
import pyarrow.parquet as pq

from abr_extract import query as q
from abr_extract.schema import (
    ABN_DGR_SCHEMA,
    ABN_MAIN_SCHEMA,
    ABN_TRADING_NAMES_SCHEMA,
)


def _write_tiny_parquets(dir_: Path) -> None:
    dir_.mkdir(parents=True, exist_ok=True)
    main_row = {col: None for col in ABN_MAIN_SCHEMA.names}
    main_row.update(
        {
            "abn": "11000000001",
            "abn_status": "ACT",
            "abn_status_from_date": date(2020, 1, 1),
            "record_last_updated": date(2024, 1, 1),
            "replaced": "N",
            "entity_type_ind": "PRV",
            "entity_type_text": "Australian Private Company",
            "entity_kind": "main",
            "main_name": "ACME PTY LTD",
            "main_name_type": "MN",
            "state": "NSW",
            "postcode": "2000",
            "gst_status": "ACT",
            "gst_status_from_date": date(2020, 1, 1),
        }
    )
    pq.write_table(
        pa.Table.from_pylist([main_row], schema=ABN_MAIN_SCHEMA),
        dir_ / "abn-main-latest.parquet",
    )
    pq.write_table(
        pa.Table.from_pylist([], schema=ABN_TRADING_NAMES_SCHEMA),
        dir_ / "abn-trading-names-latest.parquet",
    )
    pq.write_table(
        pa.Table.from_pylist([], schema=ABN_DGR_SCHEMA),
        dir_ / "abn-dgr-latest.parquet",
    )


def test_resolve_paths_remote_when_no_cache(tmp_path):
    paths = q._resolve_paths("https://gazetteer.au", cache_dir=None)
    assert all(v.startswith("https://gazetteer.au/") for v in paths.values())


def test_resolve_paths_returns_local_when_cache_populated(tmp_path):
    cache = tmp_path / "cache"
    _write_tiny_parquets(cache)
    paths = q._resolve_paths("https://example.invalid", cache_dir=cache)
    for v in paths.values():
        assert Path(v).exists()
    assert not any(v.startswith("http") for v in paths.values())


def test_resolve_paths_downloads_when_cache_empty(tmp_path):
    cache = tmp_path / "cache"
    seed = tmp_path / "seed"
    _write_tiny_parquets(seed)

    download_calls: list[str] = []

    def fake_download(url: str, dest: Path) -> None:
        download_calls.append(url)
        # Pretend we fetched the URL by copying from the seed dir
        filename = url.rsplit("/", 1)[1]
        dest.write_bytes((seed / filename).read_bytes())

    with patch.object(q, "_download_into", side_effect=fake_download):
        paths = q._resolve_paths("https://example.invalid", cache_dir=cache)

    assert len(download_calls) == 3
    assert all(u.startswith("https://example.invalid/") for u in download_calls)
    for v in paths.values():
        assert Path(v).exists()


def test_resolve_paths_skips_download_on_second_call(tmp_path):
    cache = tmp_path / "cache"
    seed = tmp_path / "seed"
    _write_tiny_parquets(seed)

    download_calls: list[str] = []

    def fake_download(url: str, dest: Path) -> None:
        download_calls.append(url)
        filename = url.rsplit("/", 1)[1]
        dest.write_bytes((seed / filename).read_bytes())

    with patch.object(q, "_download_into", side_effect=fake_download):
        q._resolve_paths("https://example.invalid", cache_dir=cache)
        q._resolve_paths("https://example.invalid", cache_dir=cache)

    assert len(download_calls) == 3  # only the first call downloads


def test_query_search_uses_cache_dir(tmp_path):
    cache = tmp_path / "cache"
    _write_tiny_parquets(cache)
    rows = q.search("acme", source="https://example.invalid", cache_dir=cache)
    assert rows
    assert rows[0]["abn"] == "11000000001"


def test_query_profile_uses_cache_dir(tmp_path):
    cache = tmp_path / "cache"
    _write_tiny_parquets(cache)
    p = q.profile("11000000001", source="https://example.invalid", cache_dir=cache)
    assert p is not None
    assert p["main_name"] == "ACME PTY LTD"
