"""Extract a diverse, hand-curated sample of ABR records into tests/fixtures/.

Streams one of the XML files inside the bulk-extract zip, picks records
matching specific shape categories, and writes a single fixture XML wrapping
each picked record so tests can stay self-contained.
"""

from __future__ import annotations

import zipfile
from collections.abc import Iterator
from pathlib import Path
from xml.etree import ElementTree as ET

ZIP_PATH = Path("work/public_split_1_10.zip")
INNER_XML = "20260506_Public01.xml"
OUTPUT = Path("tests/fixtures/sample_records.xml")
MAX_RECORDS_TO_SCAN = 200_000


def iter_abr_elements(stream) -> Iterator[ET.Element]:
    parser = ET.XMLPullParser(["end"])
    while True:
        chunk = stream.read(1 << 16)
        if not chunk:
            parser.close()
            for event, elem in parser.read_events():
                if elem.tag == "ABR":
                    yield elem
            return
        parser.feed(chunk)
        for event, elem in parser.read_events():
            if elem.tag == "ABR":
                yield elem
                elem.clear()


def categorise(rec: ET.Element) -> set[str]:
    cats: set[str] = set()
    abn_el = rec.find("ABN")
    if abn_el is not None:
        cats.add(f"abn_{abn_el.get('status', '?')}")
    if rec.get("replaced") == "Y":
        cats.add("replaced_yes")
    if rec.find("LegalEntity") is not None:
        cats.add("legal_entity")
        ind = rec.find("LegalEntity/IndividualName")
        if ind is not None:
            given_names = ind.findall("GivenName")
            if len(given_names) >= 2:
                cats.add("two_given_names")
            if ind.find("NameTitle") is not None:
                cats.add("name_title")
    if rec.find("MainEntity") is not None:
        cats.add("main_entity")
    dgrs = rec.findall("DGR")
    if dgrs:
        cats.add("has_dgr")
        if len(dgrs) >= 2:
            cats.add("multiple_dgr")
        if any(d.find("NonIndividualName") is not None for d in dgrs):
            cats.add("dgr_with_name")
    others = rec.findall("OtherEntity")
    if len(others) >= 4:
        cats.add("many_trading_names")
    elif len(others) >= 1:
        cats.add("some_trading_names")
    if rec.find("GST") is None:
        cats.add("no_gst")
    else:
        gst_status = rec.find("GST").get("status")
        cats.add(f"gst_{gst_status}")
    state_el = rec.find(".//State")
    if state_el is not None and not (state_el.text or "").strip():
        cats.add("empty_state")
    pc_el = rec.find(".//Postcode")
    if pc_el is not None and not (pc_el.text or "").strip():
        cats.add("empty_postcode")
    return cats


WANTED = {
    "abn_ACT",
    "abn_CAN",
    "main_entity",
    "legal_entity",
    "two_given_names",
    "name_title",
    "has_dgr",
    "multiple_dgr",
    "dgr_with_name",
    "many_trading_names",
    "some_trading_names",
    "gst_ACT",
    "gst_CAN",
    "gst_NON",
    "no_gst",
    "empty_state",
    "empty_postcode",
    "replaced_yes",
}


def main() -> None:
    OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    picked: dict[str, ET.Element] = {}

    print(f"Streaming {INNER_XML} from {ZIP_PATH}...")
    with zipfile.ZipFile(ZIP_PATH) as z, z.open(INNER_XML) as f:
        for i, rec in enumerate(iter_abr_elements(f)):
            if i >= MAX_RECORDS_TO_SCAN:
                break
            cats = categorise(rec)
            new = cats - picked.keys() & WANTED
            if new:
                rec_copy = ET.fromstring(ET.tostring(rec))
                for cat in new:
                    picked.setdefault(cat, rec_copy)
                if WANTED <= picked.keys():
                    print(f"All categories filled at record {i}.")
                    break

    seen_abns: set[str] = set()
    unique_records: list[tuple[str, ET.Element]] = []
    for cat, rec in picked.items():
        abn = rec.findtext("ABN") or "?"
        if abn not in seen_abns:
            seen_abns.add(abn)
            unique_records.append((abn, rec))

    print(f"\nCategory coverage:")
    for cat in sorted(WANTED):
        if cat in picked:
            abn = picked[cat].findtext("ABN") or "?"
            print(f"  {cat:25} -> ABN {abn}")
        else:
            print(f"  {cat:25} -> MISSING")

    transfer = ET.Element("Transfer", attrib={"error": "none"})
    info = ET.SubElement(transfer, "TransferInfo")
    ET.SubElement(info, "FileSequenceNumber").text = "1"
    ET.SubElement(info, "RecordCount").text = str(len(unique_records))
    ET.SubElement(info, "ExtractTime").text = "2026-05-06T12:23:33"
    for abn, rec in unique_records:
        transfer.append(rec)

    ET.indent(transfer, space="  ")
    tree = ET.ElementTree(transfer)
    tree.write(OUTPUT, encoding="utf-8", xml_declaration=True)
    print(f"\nWrote {len(unique_records)} unique records to {OUTPUT}")


if __name__ == "__main__":
    main()
