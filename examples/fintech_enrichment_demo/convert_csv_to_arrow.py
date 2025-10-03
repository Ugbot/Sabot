#!/usr/bin/env python3
"""
Convert large CSV files to Arrow IPC format for 10-100x faster loading.

Arrow IPC (Feather v2) benefits:
- Memory-mapped zero-copy reading
- Columnar format (load only needed columns)
- No parsing overhead (binary format)
- 10-100x faster than CSV for large files

Usage:
    python convert_csv_to_arrow.py
"""

import time
from pathlib import Path
import pyarrow as pa
import pyarrow.csv as pa_csv
import pyarrow.feather as feather

DATA_DIR = Path(__file__).parent

CSV_FILES = [
    "master_security_10m.csv",
    "synthetic_inventory.csv",
    "trax_trades_1m.csv"
]

def convert_csv_to_arrow(csv_path: Path) -> Path:
    """
    Convert CSV to Arrow IPC format.

    Returns path to created .arrow file
    """
    arrow_path = csv_path.with_suffix('.arrow')

    if arrow_path.exists():
        print(f"✓ {arrow_path.name} already exists, skipping")
        return arrow_path

    print(f"\n{'='*60}")
    print(f"Converting: {csv_path.name}")
    print(f"Size: {csv_path.stat().st_size / 1024 / 1024:.1f} MB")
    print(f"{'='*60}")

    start = time.perf_counter()

    # Read CSV with all threads
    print("Reading CSV...")
    read_options = pa_csv.ReadOptions(
        use_threads=True,
        block_size=64 * 1024 * 1024  # 64MB blocks
    )

    table = pa_csv.read_csv(
        csv_path,
        read_options=read_options
    )

    csv_time = time.perf_counter() - start
    print(f"  CSV loaded: {table.num_rows:,} rows in {csv_time:.2f}s")

    # Write Arrow IPC format (Feather v2)
    print("Writing Arrow IPC...")
    write_start = time.perf_counter()

    feather.write_feather(
        table,
        arrow_path,
        compression='zstd',  # Fast compression
        compression_level=3  # Light compression for speed
    )

    write_time = time.perf_counter() - write_start
    total_time = time.perf_counter() - start

    arrow_size = arrow_path.stat().st_size / 1024 / 1024
    csv_size = csv_path.stat().st_size / 1024 / 1024
    compression_ratio = (1 - arrow_size / csv_size) * 100

    print(f"  Arrow written: {arrow_size:.1f} MB in {write_time:.2f}s")
    print(f"\n✅ Conversion complete!")
    print(f"  Total time: {total_time:.2f}s")
    print(f"  Size reduction: {compression_ratio:.1f}%")
    print(f"  Output: {arrow_path}")

    return arrow_path


def main():
    """Convert all CSV files to Arrow format."""
    print("\n" + "="*60)
    print("CSV → Arrow IPC Converter")
    print("="*60)

    converted = []

    for csv_file in CSV_FILES:
        csv_path = DATA_DIR / csv_file

        if not csv_path.exists():
            print(f"⚠️  {csv_file} not found, skipping")
            continue

        arrow_path = convert_csv_to_arrow(csv_path)
        converted.append(arrow_path)

    print("\n" + "="*60)
    print(f"✅ Converted {len(converted)} files to Arrow format")
    print("="*60)

    print("\nTo use Arrow files instead of CSV:")
    print("  export SABOT_USE_ARROW=1")
    print("  ./run_demo.sh --securities 10000000 --quotes 1200000")

    print("\nExpected speedup: 10-100x faster loading! 🚀")


if __name__ == '__main__':
    main()
