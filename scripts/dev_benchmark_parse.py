"""Benchmark parse_records on a real ABR XML file from inside the zip."""

from __future__ import annotations

import os
import time
import zipfile

import psutil

from abr_extract.parse import parse_records, read_transfer_info

ZIP_PATH = "work/public_split_1_10.zip"
INNER = "20260506_Public01.xml"
LIMIT = 100_000


def main() -> None:
    proc = psutil.Process(os.getpid())
    rss_start = proc.memory_info().rss

    with zipfile.ZipFile(ZIP_PATH) as z:
        with z.open(INNER) as f:
            info = read_transfer_info(f)
            print(f"Header: file={info.file_sequence_number} count={info.record_count} extract={info.extract_time} error={info.error}")

        with z.open(INNER) as f:
            t0 = time.monotonic()
            n = 0
            kind_counts = {"main": 0, "legal": 0}
            tn_total = 0
            dgr_total = 0
            replaced_y = 0
            for rec in parse_records(f):
                kind_counts[rec.entity_kind] += 1
                tn_total += len(rec.trading_names)
                dgr_total += len(rec.dgrs)
                if rec.replaced == "Y":
                    replaced_y += 1
                n += 1
                if n >= LIMIT:
                    break
            elapsed = time.monotonic() - t0

    rss_end = proc.memory_info().rss

    print(f"\nParsed {n:,} records in {elapsed:.1f}s ({n/elapsed:,.0f} rec/s)")
    print(f"  main:  {kind_counts['main']:,}")
    print(f"  legal: {kind_counts['legal']:,}")
    print(f"  trading_names total: {tn_total:,}")
    print(f"  dgrs total: {dgr_total:,}")
    print(f"  replaced='Y' count: {replaced_y}")
    print(f"\nRSS: {(rss_start)/1_000_000:.1f} MB -> {rss_end/1_000_000:.1f} MB (delta {(rss_end-rss_start)/1_000_000:+.1f} MB)")


if __name__ == "__main__":
    main()
