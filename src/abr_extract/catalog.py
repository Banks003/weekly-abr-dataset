from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

import httpx

CKAN_PACKAGE_URL = "https://data.gov.au/data/api/3/action/package_show?id=abn-bulk-extract"
USER_AGENT = "weekly-abr-dataset/0.1 (+https://github.com/Banks003/weekly-abr-dataset)"


@dataclass(frozen=True)
class ZipResource:
    resource_id: str
    name: str
    url: str
    last_modified: datetime
    size_bytes: int


@dataclass(frozen=True)
class Catalog:
    package_id: str
    zips: tuple[ZipResource, ...]
    schema_xsd_url: str | None
    readme_pdf_url: str | None

    @property
    def extract_last_modified(self) -> datetime:
        return max(z.last_modified for z in self.zips)


def fetch_catalog(client: httpx.Client | None = None) -> Catalog:
    own_client = client is None
    if own_client:
        client = httpx.Client(headers={"User-Agent": USER_AGENT}, timeout=30.0)
    try:
        resp = client.get(CKAN_PACKAGE_URL)
        resp.raise_for_status()
        body = resp.json()
    finally:
        if own_client:
            client.close()

    if not body.get("success"):
        raise RuntimeError(f"CKAN request unsuccessful: {body!r}")

    result = body["result"]
    package_id = result["id"]
    resources = result["resources"]

    zips: list[ZipResource] = []
    schema_url: str | None = None
    readme_url: str | None = None

    for r in resources:
        fmt = (r.get("format") or "").upper()
        if fmt == "ZIP":
            zips.append(
                ZipResource(
                    resource_id=r["id"],
                    name=r["name"],
                    url=r["url"],
                    last_modified=_parse_ckan_time(r["last_modified"]),
                    size_bytes=int(r["size"]),
                )
            )
        elif fmt == "XML" and "schema" in (r.get("name") or "").lower():
            schema_url = r["url"]
        elif fmt == "PDF":
            readme_url = r["url"]

    if not zips:
        raise RuntimeError("CKAN response had no ZIP resources")

    zips.sort(key=lambda z: z.name)
    return Catalog(
        package_id=package_id,
        zips=tuple(zips),
        schema_xsd_url=schema_url,
        readme_pdf_url=readme_url,
    )


def _parse_ckan_time(s: str) -> datetime:
    # CKAN omits timezone; values are UTC by convention.
    return datetime.fromisoformat(s.rstrip("Z"))
