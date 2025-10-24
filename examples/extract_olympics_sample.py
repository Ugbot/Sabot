#!/usr/bin/env python3
"""
Extract Sample from Olympics Dataset

Creates smaller N-Triples samples from the full Olympics dataset
for testing and development purposes.

Usage:
    # Extract first 10K triples
    python extract_olympics_sample.py --size 10000 --output olympics_sample.nt

    # Extract athletes from specific year
    python extract_olympics_sample.py --filter "2012" --output olympics_2012.nt

    # Extract specific sport
    python extract_olympics_sample.py --filter "Swimming" --output olympics_swimming.nt
"""

import sys
import argparse
import lzma
from pathlib import Path
from typing import Optional

# Add sabot to path
sabot_root = Path(__file__).parent.parent
sys.path.insert(0, str(sabot_root))

OLYMPICS_DATA = sabot_root / "vendor" / "qlever" / "examples" / "olympics.nt.xz"


def extract_sample(input_file: Path,
                   output_file: Path,
                   size: Optional[int] = None,
                   filter_text: Optional[str] = None):
    """
    Extract sample triples from Olympics dataset.

    Args:
        input_file: Input N-Triples file (may be compressed)
        output_file: Output N-Triples file
        size: Maximum number of triples to extract
        filter_text: Only include triples containing this text
    """
    print(f"Extracting sample from: {input_file}")
    print(f"Output file: {output_file}")

    if size:
        print(f"Max triples: {size:,}")

    if filter_text:
        print(f"Filter: Lines containing '{filter_text}'")

    # Determine opener
    if input_file.suffix == '.xz':
        open_func = lzma.open
    else:
        open_func = open

    count = 0
    filtered = 0

    with open_func(input_file, 'rt', encoding='utf-8') as infile:
        with open(output_file, 'w', encoding='utf-8') as outfile:

            for line in infile:
                # Apply filter if specified
                if filter_text and filter_text not in line:
                    filtered += 1
                    continue

                # Write triple
                outfile.write(line)
                count += 1

                # Progress
                if count % 1000 == 0:
                    print(f"  Extracted {count:,} triples...", end='\r')

                # Check size limit
                if size and count >= size:
                    break

    print(f"\n✓ Extracted {count:,} triples")

    if filter_text:
        print(f"  Filtered out: {filtered:,} triples")

    # Show file size
    size_mb = output_file.stat().st_size / (1024 * 1024)
    print(f"  Output size: {size_mb:.2f} MB")


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description='Extract sample from Olympics dataset',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Extract first 10K triples
  python extract_olympics_sample.py --size 10000 --output olympics_10k.nt

  # Extract 2012 Olympics
  python extract_olympics_sample.py --filter "2012" --size 50000 --output olympics_2012.nt

  # Extract Swimming events
  python extract_olympics_sample.py --filter "Swimming" --output olympics_swimming.nt

  # Extract athletes only (by filtering foaf:Person)
  python extract_olympics_sample.py --filter "foaf/0.1/Person" --output olympics_athletes.nt
        """
    )

    parser.add_argument(
        '--input',
        type=Path,
        default=OLYMPICS_DATA,
        help='Input N-Triples file (default: Olympics dataset from QLever)'
    )

    parser.add_argument(
        '--output',
        type=Path,
        required=True,
        help='Output N-Triples file'
    )

    parser.add_argument(
        '--size',
        type=int,
        help='Maximum number of triples to extract'
    )

    parser.add_argument(
        '--filter',
        type=str,
        help='Only include triples containing this text'
    )

    args = parser.parse_args()

    # Validate input
    if not args.input.exists():
        print(f"Error: Input file not found: {args.input}")
        print("\nExpected location: vendor/qlever/examples/olympics.nt.xz")
        print("Make sure QLever submodule is initialized.")
        sys.exit(1)

    # Check output doesn't exist
    if args.output.exists():
        response = input(f"Output file {args.output} exists. Overwrite? [y/N] ")
        if response.lower() != 'y':
            print("Aborted.")
            sys.exit(0)

    # Extract
    try:
        extract_sample(
            args.input,
            args.output,
            size=args.size,
            filter_text=args.filter
        )

        print(f"\n✅ Sample extracted successfully!")
        print(f"\nYou can now use this with:")
        print(f"  from sabot.rdf_loader import load_ntriples")
        print(f"  store, count = load_ntriples('{args.output}')")

    except KeyboardInterrupt:
        print("\n\n⚠️  Extraction interrupted")
        if args.output.exists():
            args.output.unlink()
            print(f"Removed incomplete file: {args.output}")
        sys.exit(1)

    except Exception as e:
        print(f"\n❌ Error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
