from __future__ import annotations

import hashlib
import time
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import urlsplit

import httpx
from tqdm import tqdm

from .catalog import USER_AGENT, Catalog, ZipResource

CHUNK_SIZE = 1 << 20  # 1 MiB


class DownloadError(RuntimeError):
    pass


@dataclass(frozen=True)
class DownloadResult:
    resource_id: str
    path: Path
    bytes_written: int
    sha256: str
    elapsed_seconds: float


def _filename_for(resource: ZipResource) -> str:
    base = Path(urlsplit(resource.url).path).name
    return base or f"{resource.resource_id}.zip"


def download_zip(
    resource: ZipResource,
    output_dir: Path,
    *,
    client: httpx.Client | None = None,
    show_progress: bool = True,
) -> DownloadResult:
    output_dir.mkdir(parents=True, exist_ok=True)
    final_path = output_dir / _filename_for(resource)
    part_path = final_path.with_suffix(final_path.suffix + ".part")

    own_client = client is None
    if own_client:
        client = httpx.Client(headers={"User-Agent": USER_AGENT}, timeout=httpx.Timeout(30.0, read=300.0))

    digest = hashlib.sha256()
    bytes_written = 0
    started = time.monotonic()

    try:
        with client.stream("GET", resource.url) as response:
            response.raise_for_status()
            content_length = response.headers.get("content-length")
            expected = int(content_length) if content_length else None
            if expected is not None and expected != resource.size_bytes:
                raise DownloadError(
                    f"Content-Length {expected} != catalog size {resource.size_bytes} for {resource.name}"
                )

            progress = (
                tqdm(
                    total=expected,
                    unit="B",
                    unit_scale=True,
                    unit_divisor=1024,
                    desc=resource.name,
                    leave=False,
                )
                if show_progress
                else None
            )
            try:
                with part_path.open("wb") as f:
                    for chunk in response.iter_bytes(CHUNK_SIZE):
                        f.write(chunk)
                        digest.update(chunk)
                        bytes_written += len(chunk)
                        if progress is not None:
                            progress.update(len(chunk))
            finally:
                if progress is not None:
                    progress.close()

        if expected is not None and bytes_written != expected:
            raise DownloadError(
                f"Wrote {bytes_written} bytes but Content-Length was {expected} for {resource.name}"
            )

        part_path.replace(final_path)
    except Exception:
        if part_path.exists():
            part_path.unlink(missing_ok=True)
        raise
    finally:
        if own_client:
            client.close()

    return DownloadResult(
        resource_id=resource.resource_id,
        path=final_path,
        bytes_written=bytes_written,
        sha256=digest.hexdigest(),
        elapsed_seconds=time.monotonic() - started,
    )


def download_all(
    catalog: Catalog,
    output_dir: Path,
    *,
    client: httpx.Client | None = None,
    show_progress: bool = True,
) -> list[DownloadResult]:
    own_client = client is None
    if own_client:
        client = httpx.Client(headers={"User-Agent": USER_AGENT}, timeout=httpx.Timeout(30.0, read=300.0))
    try:
        return [
            download_zip(z, output_dir, client=client, show_progress=show_progress)
            for z in catalog.zips
        ]
    finally:
        if own_client:
            client.close()
