from __future__ import annotations

import json

import httpx

from abr_extract.query import enrich_profile_live


def _client_serving(payload: str | dict) -> httpx.Client:
    body = payload if isinstance(payload, str) else json.dumps(payload)

    def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, text=body, headers={"content-type": "application/json"})

    return httpx.Client(transport=httpx.MockTransport(handler))


def test_enrich_parses_pure_json():
    payload = {
        "Abn": "16009661901",
        "EntityName": "QANTAS AIRWAYS LIMITED",
        "AddressState": "NSW",
        "AddressPostcode": "2020",
        "AddressDate": "2024-06-01",
        "GstStatusToDate": "2025-12-31",
        "HistoricalNames": [
            {
                "OrganisationName": "QANTAS OLD NAME LTD",
                "OrganisationNameType": "MN",
                "EffectiveFrom": "1985-01-01",
                "EffectiveTo": "1999-11-01",
            }
        ],
        "BusinessName": [
            {
                "OrganisationName": "QANTAS",
                "EffectiveFrom": "2000-01-01",
                "EffectiveTo": None,
            }
        ],
    }
    client = _client_serving(payload)
    result = enrich_profile_live("16009661901", guid="test-guid", client=client)
    assert result["address"] == {"state": "NSW", "postcode": "2020", "from": "2024-06-01"}
    assert result["gst_to"] == "2025-12-31"
    assert len(result["name_history"]) == 1
    assert result["name_history"][0]["name"] == "QANTAS OLD NAME LTD"
    assert len(result["trading_names_history"]) == 1
    assert result["trading_names_history"][0]["name"] == "QANTAS"


def test_enrich_strips_jsonp_callback_wrapper():
    payload_json = json.dumps({"Abn": "11000000001", "AddressState": "VIC"})
    client = _client_serving(f"callback({payload_json})")
    result = enrich_profile_live("11000000001", guid="g", client=client)
    assert result["address"]["state"] == "VIC"


def test_enrich_handles_missing_optional_fields():
    payload = {"Abn": "11000000001"}  # minimal response
    client = _client_serving(payload)
    result = enrich_profile_live("11000000001", guid="g", client=client)
    assert result["name_history"] == []
    assert result["trading_names_history"] == []
    assert result["gst_to"] is None
    assert result["address"] is None


def test_enrich_raw_payload_passed_through():
    payload = {"Abn": "11000000001", "Custom": "preserved"}
    client = _client_serving(payload)
    result = enrich_profile_live("11000000001", guid="g", client=client)
    assert result["raw"] == payload
