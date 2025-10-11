#!/usr/bin/env python3
"""
Test vectorized queries against original implementations for correctness.

This script verifies that the optimized vectorized queries produce
identical results to the original Python loop-based implementations.
"""
import sys
import os
from pathlib import Path

# Add Sabot to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

# Set library path for Arrow
os.environ['DYLD_LIBRARY_PATH'] = '/Users/bengamble/Sabot/vendor/arrow/cpp/build/install/lib'

from data_loader import load_all_data
import queries as orig
import queries_vectorized as vec
from sabot import cyarrow as ca


def compare_tables(table1: ca.Table, table2: ca.Table, query_name: str) -> bool:
    """Compare two Arrow tables for equality."""

    # Check schema
    if table1.schema != table2.schema:
        print(f"❌ {query_name}: Schema mismatch")
        print(f"   Original: {table1.schema}")
        print(f"   Vectorized: {table2.schema}")
        return False

    # Check row count
    if table1.num_rows != table2.num_rows:
        print(f"❌ {query_name}: Row count mismatch")
        print(f"   Original: {table1.num_rows} rows")
        print(f"   Vectorized: {table2.num_rows} rows")
        return False

    # Convert to Python for comparison (easier than comparing Arrow arrays)
    dict1 = table1.to_pydict()
    dict2 = table2.to_pydict()

    # Check column names
    if set(dict1.keys()) != set(dict2.keys()):
        print(f"❌ {query_name}: Column name mismatch")
        print(f"   Original: {sorted(dict1.keys())}")
        print(f"   Vectorized: {sorted(dict2.keys())}")
        return False

    # Check values for each column (with tolerance for floating point)
    for col_name in dict1.keys():
        vals1 = dict1[col_name]
        vals2 = dict2[col_name]

        # For numeric columns, check with tolerance
        if table1.column(col_name).type in [ca.float32(), ca.float64()]:
            for i, (v1, v2) in enumerate(zip(vals1, vals2)):
                if abs(v1 - v2) > 1e-6:
                    print(f"❌ {query_name}: Value mismatch in column '{col_name}' at row {i}")
                    print(f"   Original: {v1}")
                    print(f"   Vectorized: {v2}")
                    return False
        else:
            # Exact comparison for non-float types
            if vals1 != vals2:
                print(f"❌ {query_name}: Value mismatch in column '{col_name}'")
                # Find first difference
                for i, (v1, v2) in enumerate(zip(vals1, vals2)):
                    if v1 != v2:
                        print(f"   First difference at row {i}:")
                        print(f"   Original: {v1}")
                        print(f"   Vectorized: {v2}")
                        break
                return False

    print(f"✅ {query_name}: Results match!")
    return True


def main():
    print("="*70)
    print("TESTING VECTORIZED QUERIES FOR CORRECTNESS")
    print("="*70)
    print()

    # Load data
    data_dir = Path(__file__).parent / "reference" / "data" / "output"

    if not data_dir.exists():
        print(f"❌ Error: Data directory not found: {data_dir}")
        print("   Run: cd reference/data && bash ../generate_data.sh 100000")
        return 1

    print("Loading data...")
    data = load_all_data(data_dir)
    print(f"✅ Loaded {data['person_nodes'].num_rows:,} persons")
    print(f"✅ Loaded {data['follows_edges'].num_rows:,} follows edges")
    print()

    # Extract tables for convenience
    vertices = data['person_nodes']
    follows = data['follows_edges']
    lives_in = data['lives_in_edges']
    has_interest = data['has_interest_edges']
    cities = data['city_nodes']
    states = data['state_nodes']
    interests = data['interest_nodes']

    # Test results
    all_passed = True

    # Test Query 3
    print("[1/4] Testing Query 3: Lowest avg age cities...")
    try:
        result_orig = orig.query3_lowest_avg_age_cities(vertices, lives_in, cities, country="United States")
        result_vec = vec.query3_lowest_avg_age_cities(vertices, lives_in, cities, country="United States")

        passed = compare_tables(result_orig, result_vec, "Query 3")
        all_passed = all_passed and passed
    except Exception as e:
        print(f"❌ Query 3 failed with error: {e}")
        import traceback
        traceback.print_exc()
        all_passed = False
    print()

    # Test Query 4
    print("[2/4] Testing Query 4: Persons aged 30-40 by country...")
    try:
        result_orig = orig.query4_persons_by_age_country(vertices, lives_in, cities, age_lower=30, age_upper=40)
        result_vec = vec.query4_persons_by_age_country(vertices, lives_in, cities, age_lower=30, age_upper=40)

        passed = compare_tables(result_orig, result_vec, "Query 4")
        all_passed = all_passed and passed
    except Exception as e:
        print(f"❌ Query 4 failed with error: {e}")
        import traceback
        traceback.print_exc()
        all_passed = False
    print()

    # Test Query 6
    print("[3/4] Testing Query 6: City with most women interested in tennis...")
    try:
        result_orig = orig.query6_city_with_most_interest_gender(
            vertices, has_interest, lives_in, cities, interests,
            gender="female", interest="tennis"
        )
        result_vec = vec.query6_city_with_most_interest_gender(
            vertices, has_interest, lives_in, cities, interests,
            gender="female", interest="tennis"
        )

        passed = compare_tables(result_orig, result_vec, "Query 6")
        all_passed = all_passed and passed
    except Exception as e:
        print(f"❌ Query 6 failed with error: {e}")
        import traceback
        traceback.print_exc()
        all_passed = False
    print()

    # Test Query 9
    print("[4/4] Testing Query 9: Filtered 2-hop paths...")
    try:
        result_orig = orig.query9_filtered_paths(vertices, follows, age_1=50, age_2=25)
        result_vec = vec.query9_filtered_paths(vertices, follows, age_1=50, age_2=25)

        passed = compare_tables(result_orig, result_vec, "Query 9")
        all_passed = all_passed and passed
    except Exception as e:
        print(f"❌ Query 9 failed with error: {e}")
        import traceback
        traceback.print_exc()
        all_passed = False
    print()

    # Summary
    print("="*70)
    print("SUMMARY")
    print("="*70)
    if all_passed:
        print("✅ All vectorized queries produce identical results!")
        print("   Ready to integrate into benchmark runner.")
        return 0
    else:
        print("❌ Some queries have mismatches - need to fix before benchmarking")
        return 1


if __name__ == "__main__":
    sys.exit(main())
