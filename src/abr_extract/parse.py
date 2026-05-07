from __future__ import annotations

from collections.abc import Iterator
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import IO

from lxml import etree


@dataclass(frozen=True)
class TradingName:
    name: str
    name_type: str  # MN, TRD, OTN, BN, LGL, DGR


@dataclass(frozen=True)
class DgrEntry:
    status_from_date: str  # YYYYMMDD
    status: str | None
    name: str | None  # NonIndividualName/NonIndividualNameText if present


@dataclass(frozen=True)
class RawRecord:
    abn: str
    abn_status: str  # "ACT" | "CAN"
    abn_status_from_date: str  # YYYYMMDD
    record_last_updated: str  # YYYYMMDD
    replaced: str  # "Y" | "N"
    entity_type_ind: str  # 3-letter code
    entity_type_text: str
    entity_kind: str  # "main" | "legal"
    main_name: str | None
    main_name_type: str | None
    individual_title: str | None
    individual_given_names: tuple[str, ...]
    individual_family_name: str | None
    individual_name_type: str | None
    state: str | None
    postcode: str | None
    asic_number: str | None
    asic_number_type: str | None
    gst_status: str | None
    gst_status_from_date: str | None
    trading_names: tuple[TradingName, ...]
    dgrs: tuple[DgrEntry, ...]


@dataclass(frozen=True)
class TransferInfo:
    file_sequence_number: int
    record_count: int
    extract_time: datetime
    error: str  # value of Transfer/@error; "none" in healthy files


class ParseError(RuntimeError):
    pass


def _text_or_none(elem: etree._Element | None) -> str | None:
    if elem is None:
        return None
    text = elem.text
    if text is None:
        return None
    stripped = text.strip()
    return stripped or None


def _parse_abr_record(abr: etree._Element) -> RawRecord:
    abn_el = abr.find("ABN")
    if abn_el is None or not abn_el.text:
        raise ParseError(f"ABR record missing ABN: {etree.tostring(abr)[:200]!r}")
    abn = abn_el.text.strip()

    abn_status = abn_el.get("status", "")
    abn_status_from_date = abn_el.get("ABNStatusFromDate", "")

    record_last_updated = abr.get("recordLastUpdatedDate", "")
    replaced = abr.get("replaced", "")

    et_el = abr.find("EntityType")
    et_ind_el = et_el.find("EntityTypeInd") if et_el is not None else None
    et_text_el = et_el.find("EntityTypeText") if et_el is not None else None
    entity_type_ind = _text_or_none(et_ind_el) or ""
    entity_type_text = _text_or_none(et_text_el) or ""

    main_el = abr.find("MainEntity")
    legal_el = abr.find("LegalEntity")

    main_name: str | None = None
    main_name_type: str | None = None
    individual_title: str | None = None
    individual_given_names: tuple[str, ...] = ()
    individual_family_name: str | None = None
    individual_name_type: str | None = None
    entity_kind: str

    if main_el is not None:
        entity_kind = "main"
        nin = main_el.find("NonIndividualName")
        if nin is not None:
            main_name_type = nin.get("type")
            main_name = _text_or_none(nin.find("NonIndividualNameText"))
        addr_root = main_el
    elif legal_el is not None:
        entity_kind = "legal"
        ind = legal_el.find("IndividualName")
        if ind is not None:
            individual_name_type = ind.get("type")
            individual_title = _text_or_none(ind.find("NameTitle"))
            individual_given_names = tuple(
                t for t in (_text_or_none(g) for g in ind.findall("GivenName")) if t is not None
            )
            individual_family_name = _text_or_none(ind.find("FamilyName"))
        addr_root = legal_el
    else:
        raise ParseError(f"ABR record {abn} has neither MainEntity nor LegalEntity")

    state = _text_or_none(addr_root.find("BusinessAddress/AddressDetails/State"))
    postcode = _text_or_none(addr_root.find("BusinessAddress/AddressDetails/Postcode"))

    asic_el = abr.find("ASICNumber")
    asic_number = _text_or_none(asic_el)
    asic_number_type = asic_el.get("ASICNumberType") if asic_el is not None else None

    gst_el = abr.find("GST")
    gst_status = gst_el.get("status") if gst_el is not None else None
    gst_status_from_date = gst_el.get("GSTStatusFromDate") if gst_el is not None else None

    trading_names: list[TradingName] = []
    for other in abr.findall("OtherEntity"):
        nin = other.find("NonIndividualName")
        if nin is None:
            continue
        text = _text_or_none(nin.find("NonIndividualNameText"))
        if text is None:
            continue
        trading_names.append(TradingName(name=text, name_type=nin.get("type", "")))

    dgrs: list[DgrEntry] = []
    for d in abr.findall("DGR"):
        nin = d.find("NonIndividualName")
        dgr_name = _text_or_none(nin.find("NonIndividualNameText")) if nin is not None else None
        dgrs.append(
            DgrEntry(
                status_from_date=d.get("DGRStatusFromDate", ""),
                status=d.get("status"),
                name=dgr_name,
            )
        )

    return RawRecord(
        abn=abn,
        abn_status=abn_status,
        abn_status_from_date=abn_status_from_date,
        record_last_updated=record_last_updated,
        replaced=replaced,
        entity_type_ind=entity_type_ind,
        entity_type_text=entity_type_text,
        entity_kind=entity_kind,
        main_name=main_name,
        main_name_type=main_name_type,
        individual_title=individual_title,
        individual_given_names=individual_given_names,
        individual_family_name=individual_family_name,
        individual_name_type=individual_name_type,
        state=state,
        postcode=postcode,
        asic_number=asic_number,
        asic_number_type=asic_number_type,
        gst_status=gst_status,
        gst_status_from_date=gst_status_from_date,
        trading_names=tuple(trading_names),
        dgrs=tuple(dgrs),
    )


def parse_records(source: Path | str | IO[bytes]) -> Iterator[RawRecord]:
    """Stream-parse an ABR XML file, yielding one RawRecord per <ABR> element.

    Memory bounded: each ABR element is cleared after processing, and so are
    its preceding siblings on the root element.

    Uses recover=True to tolerate the buffer-boundary parse quirks that lxml's
    strict mode sometimes raises on the ABR feed (the data round-trips through
    stdlib ElementTree without complaint). The caller can inspect lxml's error
    log if any genuine errors arose.

    The streaming + flatten approach is downstream of joelkoen/simple-abns;
    we use lxml.iterparse with tag filtering instead of his event-based
    path-stack pattern, but the philosophy is the same.
    """
    context = etree.iterparse(source, events=("end",), tag="ABR", recover=True)
    for _event, elem in context:
        record = _parse_abr_record(elem)
        yield record
        elem.clear()
        # Drop preceding siblings so the root element's children list doesn't
        # accumulate ~10M cleared shells. Keep elem in place — the parser
        # tracks it, and removing it from the tree mid-stream confuses lxml.
        while elem.getprevious() is not None:
            del elem.getparent()[0]
    del context


def read_transfer_info(source: Path | str | IO[bytes]) -> TransferInfo:
    """Read just the TransferInfo header without parsing records."""
    context = etree.iterparse(source, events=("end",), tag="TransferInfo", recover=True)
    for _event, elem in context:
        seq = int(elem.findtext("FileSequenceNumber") or "0")
        count = int(elem.findtext("RecordCount") or "0")
        extract_time_text = (elem.findtext("ExtractTime") or "").strip()
        extract_time = datetime.fromisoformat(extract_time_text)
        root = elem.getparent()
        error = root.get("error", "") if root is not None else ""
        return TransferInfo(
            file_sequence_number=seq,
            record_count=count,
            extract_time=extract_time,
            error=error,
        )
    raise ParseError("No TransferInfo element found")
