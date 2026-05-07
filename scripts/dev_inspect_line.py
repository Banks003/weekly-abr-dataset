"""Find the offending character lxml strict mode rejects."""

from __future__ import annotations

from pathlib import Path

from lxml import etree

PATH = Path("work/extracted_public01.xml")


def parse_with_recover_and_log() -> None:
    context = etree.iterparse(str(PATH), events=("end",), tag="ABR", recover=True)
    n = 0
    for _event, elem in context:
        n += 1
        elem.clear()
        if n >= 30_000:
            break
    print(f"Parsed {n} records with recover=True")
    print(f"Error log size: {len(context.error_log)}")
    for entry in context.error_log:
        print(f"  line {entry.line} col {entry.column}: {entry.message}")


def dump_bytes_around(target_offset: int, window: int = 200) -> None:
    print(f"\nBytes around offset {target_offset}:")
    with PATH.open("rb") as f:
        f.seek(max(0, target_offset - window))
        data = f.read(2 * window)
        for i in range(0, len(data), 80):
            chunk = data[i : i + 80]
            offset = max(0, target_offset - window) + i
            print(f"  [{offset:>10}] {chunk!r}")
            for j, b in enumerate(chunk):
                if b > 127 or (b < 32 and b not in (9, 10, 13)):
                    print(f"           ^ byte {offset+j}: 0x{b:02x}")


if __name__ == "__main__":
    parse_with_recover_and_log()
