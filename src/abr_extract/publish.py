"""R2 upload via the S3-compatible API, plus idempotency via a remote manifest.

R2's endpoint is `https://<account_id>.r2.cloudflarestorage.com`. boto3 talks to
it as if it were S3; the only thing we need is to point the endpoint URL at R2.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import boto3
from botocore.client import Config


@dataclass
class R2Settings:
    account_id: str
    access_key_id: str
    secret_access_key: str
    bucket: str
    region: str = "auto"

    @property
    def endpoint_url(self) -> str:
        return f"https://{self.account_id}.r2.cloudflarestorage.com"


def make_s3_client(settings: R2Settings, *, endpoint_url_override: str | None = None) -> Any:
    return boto3.client(
        "s3",
        endpoint_url=endpoint_url_override or settings.endpoint_url,
        aws_access_key_id=settings.access_key_id,
        aws_secret_access_key=settings.secret_access_key,
        region_name=settings.region,
        config=Config(signature_version="s3v4", retries={"max_attempts": 5, "mode": "standard"}),
    )


CONTENT_TYPES = {
    ".parquet": "application/vnd.apache.parquet",
    ".sqlite": "application/vnd.sqlite3",
    ".json": "application/json",
}


def _content_type_for(path: Path) -> str:
    return CONTENT_TYPES.get(path.suffix.lower(), "application/octet-stream")


@dataclass
class UploadPlan:
    """A single file gets uploaded to two keys: stable -latest, and dated."""

    local_path: Path
    latest_key: str
    dated_key: str


def plan_uploads(
    work_dir: Path,
    *,
    extract_date: str,
) -> list[UploadPlan]:
    """Build the list of files to upload from a work_dir produced by WriterBundle.

    extract_date is the ISO date (YYYY-MM-DD) that names the archive copy.
    """
    plans = []
    for local_name, stem in (
        ("abn_main.parquet", "abn-main"),
        ("abn_trading_names.parquet", "abn-trading-names"),
        ("abn_dgr.parquet", "abn-dgr"),
        ("abr.sqlite", "abr-extract"),
    ):
        local = work_dir / local_name
        if not local.exists():
            continue
        ext = local.suffix
        plans.append(
            UploadPlan(
                local_path=local,
                latest_key=f"{stem}-latest{ext}",
                dated_key=f"{stem}-{extract_date}{ext}",
            )
        )
    manifest = work_dir / "manifest.json"
    if manifest.exists():
        plans.append(
            UploadPlan(
                local_path=manifest,
                latest_key="manifest.json",
                dated_key=f"manifest-{extract_date}.json",
            )
        )
    return plans


def upload_plan(s3, bucket: str, plan: UploadPlan) -> None:
    """Upload one local file to both its dated and latest keys.

    Order matters for atomicity: dated first (immutable archive), then -latest
    (stable URL flips after archive is durable). If -latest upload fails, the
    archive copy is still good and a re-run will recover.
    """
    extra = {"ContentType": _content_type_for(plan.local_path)}
    s3.upload_file(str(plan.local_path), bucket, plan.dated_key, ExtraArgs=extra)
    s3.upload_file(str(plan.local_path), bucket, plan.latest_key, ExtraArgs=extra)


def upload_all(s3, bucket: str, plans: list[UploadPlan]) -> None:
    for plan in plans:
        upload_plan(s3, bucket, plan)


def read_remote_manifest(s3, bucket: str) -> dict | None:
    """Return the current published manifest or None if it doesn't exist yet."""
    try:
        body = s3.get_object(Bucket=bucket, Key="manifest.json")["Body"].read()
    except s3.exceptions.NoSuchKey:
        return None
    except Exception as exc:  # botocore wraps "key not found" inconsistently
        if "NoSuchKey" in str(exc) or "404" in str(exc):
            return None
        raise
    return json.loads(body.decode("utf-8"))


def is_extract_already_published(remote_manifest: dict | None, catalog_extract_time: str) -> bool:
    """Compare published manifest's extract_time to the catalog's; True if same."""
    if not remote_manifest:
        return False
    return remote_manifest.get("extract_time") == catalog_extract_time
