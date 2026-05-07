from __future__ import annotations

import json
from pathlib import Path

import boto3
import pytest
from moto import mock_aws

from abr_extract.publish import (
    R2Settings,
    is_extract_already_published,
    plan_uploads,
    read_remote_manifest,
    upload_all,
)


@pytest.fixture
def aws_env(monkeypatch):
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "test")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "test")
    monkeypatch.setenv("AWS_DEFAULT_REGION", "us-east-1")


@pytest.fixture
def settings():
    return R2Settings(
        account_id="test-account",
        access_key_id="test",
        secret_access_key="test",
        bucket="test-bucket",
    )


def _moto_endpoint() -> str:
    # moto's mock S3 uses the standard AWS endpoint internally; we override
    # the R2 endpoint for tests so requests stay in moto.
    return "https://s3.us-east-1.amazonaws.com"


def _setup_bucket() -> boto3.client:
    s3 = boto3.client("s3", region_name="us-east-1")
    s3.create_bucket(Bucket="test-bucket")
    return s3


def test_plan_uploads_handles_missing_files(tmp_path: Path):
    plans = plan_uploads(tmp_path, extract_date="2026-05-06")
    assert plans == []


def test_plan_uploads_emits_manifest_only(tmp_path: Path):
    # Snapshot files are intentionally ignored — Iceberg owns the dataset.
    for name in ("abn_main.parquet", "abn_dgr.parquet", "abr.sqlite", "manifest.json"):
        (tmp_path / name).write_bytes(b"x")
    plans = plan_uploads(tmp_path, extract_date="2026-05-06")
    keys = {(p.latest_key, p.dated_key) for p in plans}
    assert keys == {("manifest.json", "manifest-2026-05-06.json")}


def test_plan_uploads_emits_nothing_when_manifest_missing(tmp_path: Path):
    (tmp_path / "abn_main.parquet").write_bytes(b"x")
    plans = plan_uploads(tmp_path, extract_date="2026-05-06")
    assert plans == []


@mock_aws
def test_upload_all_writes_manifest_in_two_keys(tmp_path: Path, aws_env, settings):
    (tmp_path / "manifest.json").write_text('{"extract_time": "2026-05-06T12:23:33"}')

    s3 = _setup_bucket()
    plans = plan_uploads(tmp_path, extract_date="2026-05-06")
    upload_all(s3, settings.bucket, plans)

    listing = s3.list_objects_v2(Bucket=settings.bucket)
    keys = {obj["Key"] for obj in listing["Contents"]}
    assert keys == {"manifest.json", "manifest-2026-05-06.json"}


@mock_aws
def test_upload_all_sets_manifest_content_type(tmp_path: Path, aws_env, settings):
    (tmp_path / "manifest.json").write_text("{}")
    s3 = _setup_bucket()
    plans = plan_uploads(tmp_path, extract_date="2026-05-06")
    upload_all(s3, settings.bucket, plans)
    head_json = s3.head_object(Bucket=settings.bucket, Key="manifest.json")
    assert head_json["ContentType"] == "application/json"


@mock_aws
def test_read_remote_manifest_when_missing_returns_none(aws_env, settings):
    s3 = _setup_bucket()
    assert read_remote_manifest(s3, settings.bucket) is None


@mock_aws
def test_read_remote_manifest_returns_payload(aws_env, settings):
    s3 = _setup_bucket()
    body = json.dumps({"extract_time": "2026-05-06T12:23:33"})
    s3.put_object(Bucket=settings.bucket, Key="manifest.json", Body=body.encode())
    manifest = read_remote_manifest(s3, settings.bucket)
    assert manifest == {"extract_time": "2026-05-06T12:23:33"}


def test_is_extract_already_published_true_when_match():
    assert is_extract_already_published(
        {"extract_time": "2026-05-06T12:23:33"},
        "2026-05-06T12:23:33",
    )


def test_is_extract_already_published_false_when_different():
    assert not is_extract_already_published(
        {"extract_time": "2026-04-29T12:23:33"},
        "2026-05-06T12:23:33",
    )


def test_is_extract_already_published_false_when_missing_manifest():
    assert not is_extract_already_published(None, "2026-05-06T12:23:33")


def test_make_s3_client_endpoint_url_includes_account(settings):
    assert settings.endpoint_url == "https://test-account.r2.cloudflarestorage.com"
