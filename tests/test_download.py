from __future__ import annotations

import hashlib
from datetime import datetime
from pathlib import Path

import httpx
import pytest

from abr_extract.catalog import ZipResource
from abr_extract.download import DownloadError, download_zip


def _resource(url: str = "https://example/test.zip", size: int = 1024) -> ZipResource:
    return ZipResource(
        resource_id="test-id",
        name="Test",
        url=url,
        last_modified=datetime(2026, 5, 7),
        size_bytes=size,
    )


def _client_serving(payload: bytes, *, content_length: int | str | None = "match") -> httpx.Client:
    if content_length == "match":
        headers = {"content-length": str(len(payload))}
    elif content_length is None:
        headers = {}
    else:
        headers = {"content-length": str(content_length)}

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, content=payload, headers=headers)

    return httpx.Client(transport=httpx.MockTransport(handler))


def test_download_writes_file_and_computes_sha256(tmp_path: Path) -> None:
    payload = b"hello world" * 100
    resource = _resource(size=len(payload))
    client = _client_serving(payload)

    result = download_zip(resource, tmp_path, client=client, show_progress=False)

    assert result.path.exists()
    assert result.path.read_bytes() == payload
    assert result.bytes_written == len(payload)
    assert result.sha256 == hashlib.sha256(payload).hexdigest()
    assert not (tmp_path / "test.zip.part").exists()


def test_download_filename_from_url(tmp_path: Path) -> None:
    payload = b"x" * 50
    resource = _resource(url="https://example/path/public_split_1_10.zip", size=len(payload))
    client = _client_serving(payload)

    result = download_zip(resource, tmp_path, client=client, show_progress=False)
    assert result.path.name == "public_split_1_10.zip"


def test_download_rejects_size_mismatch_in_content_length(tmp_path: Path) -> None:
    payload = b"x" * 100
    resource = _resource(size=999)  # catalog says 999, server will send 100
    client = _client_serving(payload)

    with pytest.raises(DownloadError, match="Content-Length"):
        download_zip(resource, tmp_path, client=client, show_progress=False)
    assert not list(tmp_path.iterdir())


def test_download_cleans_up_part_file_on_error(tmp_path: Path) -> None:
    resource = _resource(size=10)

    def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(500, content=b"server error")

    client = httpx.Client(transport=httpx.MockTransport(handler))

    with pytest.raises(httpx.HTTPStatusError):
        download_zip(resource, tmp_path, client=client, show_progress=False)
    assert not list(tmp_path.iterdir())


def test_download_works_without_content_length_header(tmp_path: Path) -> None:
    payload = b"streamed-without-length" * 10
    resource = _resource(size=len(payload))
    client = _client_serving(payload, content_length=None)

    result = download_zip(resource, tmp_path, client=client, show_progress=False)
    assert result.bytes_written == len(payload)
    assert result.sha256 == hashlib.sha256(payload).hexdigest()
