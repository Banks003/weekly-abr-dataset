from __future__ import annotations

from datetime import datetime
from pathlib import Path

import pytest

from abr_extract.parse import (
    ParseError,
    parse_records,
    read_transfer_info,
)

FIXTURE = Path(__file__).parent / "fixtures" / "sample_records.xml"


@pytest.fixture(scope="module")
def records():
    return list(parse_records(FIXTURE))


@pytest.fixture(scope="module")
def by_abn(records):
    return {r.abn: r for r in records}


def test_record_count_matches_fixture(records):
    assert len(records) == 9


def test_transfer_info_parses():
    info = read_transfer_info(FIXTURE)
    assert info.file_sequence_number == 1
    assert info.record_count == 9
    assert info.extract_time == datetime(2026, 5, 6, 12, 23, 33)
    assert info.error == "none"


def test_main_entity_record(by_abn):
    r = by_abn["11000000948"]
    assert r.abn_status == "ACT"
    assert r.abn_status_from_date == "19991101"
    assert r.record_last_updated == "20180216"
    assert r.replaced == "N"
    assert r.entity_type_ind == "PUB"
    assert r.entity_type_text == "Australian Public Company"
    assert r.entity_kind == "main"
    assert r.main_name == "QBE INSURANCE (INTERNATIONAL) LTD"
    assert r.main_name_type == "MN"
    assert r.state == "NSW"
    assert r.postcode == "2000"
    assert r.asic_number == "000000948"
    assert r.asic_number_type == "undetermined"
    assert r.gst_status == "ACT"
    assert r.gst_status_from_date == "20000701"
    assert r.individual_given_names == ()
    assert r.individual_family_name is None


def test_legal_entity_record_with_two_given_names(by_abn):
    r = by_abn["11001032430"]
    assert r.entity_kind == "legal"
    assert r.entity_type_ind == "IND"
    assert r.individual_name_type == "LGL"
    assert r.individual_title is None
    assert r.individual_given_names == ("JOHN", "MCLEOD")
    assert r.individual_family_name == "DUFF"
    assert r.state == "WA"
    assert r.postcode == "6084"
    assert r.main_name is None


def test_legal_entity_record_with_title(by_abn):
    r = by_abn["11001572575"]
    assert r.entity_kind == "legal"
    assert r.individual_title == "MR"
    assert r.individual_given_names == ("WILLIAM", "RICHARD")
    assert r.individual_family_name == "LITT"


def test_empty_state_becomes_none(by_abn):
    r = by_abn["11003615606"]
    assert r.state is None
    assert r.postcode == "0000"


def test_empty_postcode_becomes_none(by_abn):
    r = by_abn["11005538314"]
    assert r.state is None
    assert r.postcode is None


def test_gst_non_with_sentinel_date_preserved(by_abn):
    r = by_abn["11000009496"]
    assert r.gst_status == "NON"
    assert r.gst_status_from_date == "19000101"


def test_trading_names_extracted_in_order_with_types(by_abn):
    r = by_abn["11000013098"]
    types_seen = [tn.name_type for tn in r.trading_names]
    assert types_seen[0] == "TRD"
    assert "BN" in types_seen
    assert any(tn.name == "SNP SECURITY" for tn in r.trading_names)
    assert len(r.trading_names) == 11


def test_dgr_with_and_without_name(by_abn):
    r = by_abn["11000047950"]
    assert len(r.dgrs) == 2
    self_closing = [d for d in r.dgrs if d.name is None]
    with_name = [d for d in r.dgrs if d.name is not None]
    assert len(self_closing) == 1
    assert len(with_name) == 1
    assert self_closing[0].status == "ACT"
    assert self_closing[0].status_from_date == "20060301"
    assert with_name[0].status is None
    assert "BUILDING & MAINTENANCE FUND" in with_name[0].name


def test_records_with_no_dgr(by_abn):
    r = by_abn["11000000948"]
    assert r.dgrs == ()


def test_xml_entity_decoded_in_names(by_abn):
    # The fixture has &amp; in MainEntity name; lxml should decode it.
    r = by_abn["11000013098"]
    assert "&" in r.main_name
    assert "&amp;" not in r.main_name


def test_parse_records_streams_lazily():
    # Confirm the function returns an iterator, not a list
    iter_obj = parse_records(FIXTURE)
    assert hasattr(iter_obj, "__next__")
    first = next(iter_obj)
    assert first.abn == "11000000948"


def test_parse_error_on_missing_abn(tmp_path: Path):
    bad = tmp_path / "bad.xml"
    bad.write_text(
        "<Transfer error='none'>"
        "<TransferInfo>"
        "<FileSequenceNumber>1</FileSequenceNumber>"
        "<RecordCount>1</RecordCount>"
        "<ExtractTime>2026-05-06T12:23:33</ExtractTime>"
        "</TransferInfo>"
        "<ABR recordLastUpdatedDate='20240101' replaced='N'></ABR>"
        "</Transfer>"
    )
    with pytest.raises(ParseError, match="missing ABN"):
        list(parse_records(bad))
