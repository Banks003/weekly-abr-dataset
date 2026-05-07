from pathlib import Path

from abr_extract.catalog import fetch_catalog
from abr_extract.download import download_zip


def main() -> None:
    catalog = fetch_catalog()
    part1 = catalog.zips[0]
    print(f"Downloading {part1.name} ({part1.size_bytes / 1_000_000:.1f} MB)...")
    result = download_zip(part1, Path("./work"), show_progress=False)
    print(f"Done: {result.path} ({result.bytes_written / 1_000_000:.1f} MB) in {result.elapsed_seconds:.1f}s")
    print(f"sha256={result.sha256}")


if __name__ == "__main__":
    main()
