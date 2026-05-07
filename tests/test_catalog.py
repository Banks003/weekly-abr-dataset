from __future__ import annotations

from datetime import datetime
from unittest.mock import MagicMock

from abr_extract.catalog import Catalog, ZipResource, fetch_catalog


CKAN_FIXTURE = {
    "success": True,
    "result": {
        "id": "5bd7fcab-e315-42cb-8daf-50b7efc2027e",
        "resources": [
            {
                "id": "f3f8f8a1-590c-4d87-8285-806b093c2c69",
                "name": "Bulk Extract Schema",
                "format": "XML",
                "url": "https://example/bulkextract.xsd",
                "last_modified": "2023-01-19T00:00:00",
                "size": "12866",
            },
            {
                "id": "3b975e4f",
                "name": "ABN Lookup Bulk Extract Readme",
                "format": "PDF",
                "url": "https://example/readme.pdf",
                "last_modified": "2025-07-02T01:59:12.855327",
                "size": "153761",
            },
            {
                "id": "0ae4d427",
                "name": "ABN Bulk Extract Part 1",
                "format": "ZIP",
                "url": "https://example/part1.zip",
                "last_modified": "2026-05-05T22:48:12.779125",
                "size": "490205762",
            },
            {
                "id": "635fcb95",
                "name": "ABN Bulk Extract Part 2",
                "format": "ZIP",
                "url": "https://example/part2.zip",
                "last_modified": "2026-05-05T23:04:45.808033",
                "size": "489984727",
            },
        ],
    },
}


def _mock_client_returning(payload: dict) -> MagicMock:
    response = MagicMock()
    response.json.return_value = payload
    response.raise_for_status.return_value = None
    client = MagicMock()
    client.get.return_value = response
    return client


def test_fetch_catalog_extracts_zips_and_schema() -> None:
    client = _mock_client_returning(CKAN_FIXTURE)
    catalog = fetch_catalog(client=client)

    assert isinstance(catalog, Catalog)
    assert catalog.package_id == "5bd7fcab-e315-42cb-8daf-50b7efc2027e"
    assert len(catalog.zips) == 2
    assert all(isinstance(z, ZipResource) for z in catalog.zips)
    assert catalog.schema_xsd_url == "https://example/bulkextract.xsd"
    assert catalog.readme_pdf_url == "https://example/readme.pdf"


def test_zips_sorted_by_name() -> None:
    client = _mock_client_returning(CKAN_FIXTURE)
    catalog = fetch_catalog(client=client)
    assert catalog.zips[0].name.endswith("Part 1")
    assert catalog.zips[1].name.endswith("Part 2")


def test_extract_last_modified_is_max_of_zips() -> None:
    client = _mock_client_returning(CKAN_FIXTURE)
    catalog = fetch_catalog(client=client)
    assert catalog.extract_last_modified == datetime.fromisoformat(
        "2026-05-05T23:04:45.808033"
    )
