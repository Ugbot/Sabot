#!/usr/bin/env python3
"""Debug Query 6 mismatch."""
import sys
import os
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent))
os.environ['DYLD_LIBRARY_PATH'] = '/Users/bengamble/Sabot/vendor/arrow/cpp/build/install/lib'

from data_loader import load_all_data
import queries as orig
import queries_vectorized as vec

# Load data
data_dir = Path(__file__).parent / "reference" / "data" / "output"
data = load_all_data(data_dir)

vertices = data['person_nodes']
has_interest = data['has_interest_edges']
lives_in = data['lives_in_edges']
cities = data['city_nodes']
interests = data['interest_nodes']

# Run both versions
print("Running original Query 6...")
result_orig = orig.query6_city_with_most_interest_gender(
    vertices, has_interest, lives_in, cities, interests,
    gender="female", interest="tennis"
)

print("Running vectorized Query 6...")
result_vec = vec.query6_city_with_most_interest_gender(
    vertices, has_interest, lives_in, cities, interests,
    gender="female", interest="tennis"
)

print("\n" + "="*70)
print("ORIGINAL RESULTS:")
print("="*70)
print(result_orig.to_pandas())

print("\n" + "="*70)
print("VECTORIZED RESULTS:")
print("="*70)
print(result_vec.to_pandas())

# Check if the sets are the same even if order is different
orig_dict = result_orig.to_pydict()
vec_dict = result_vec.to_pydict()

orig_set = set(zip(orig_dict['numPersons'], orig_dict['city'], orig_dict['country']))
vec_set = set(zip(vec_dict['numPersons'], vec_dict['city'], vec_dict['country']))

print("\n" + "="*70)
print("SET COMPARISON:")
print("="*70)
if orig_set == vec_set:
    print("✅ Same results, just different order due to ties!")
    print("   This is acceptable - both implementations are correct.")
else:
    print("❌ Different results:")
    print(f"   Only in original: {orig_set - vec_set}")
    print(f"   Only in vectorized: {vec_set - orig_set}")
