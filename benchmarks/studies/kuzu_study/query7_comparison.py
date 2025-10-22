#!/usr/bin/env python3
"""
Query 7 Performance Comparison: 3 Implementations

Compare different approaches to Query 7 (state age interest):
1. ORIGINAL: Python loops (current implementation)
2. STREAM API: Using CythonMapOperator to trigger auto-Numba
3. VECTORIZED: Arrow joins and group_by

Query 7: US state with most 23-30yo interested in photography
Cypher:
    MATCH (p:Person)-[:LivesIn]->(:City)-[:CityIn]->(s:State)
    WHERE p.age >= 23 AND p.age <= 30 AND s.country = 'United States'
    WITH p, s
    MATCH (p)-[:HasInterest]->(i:Interest)
    WHERE lower(i.interest) = lower('photography')
    RETURN count(p.id) AS numPersons, s.state AS state, s.country AS country
    ORDER BY numPersons DESC LIMIT 1

Expected result: ~138 people in California
"""

import sys
import time
from pathlib import Path

# Add Sabot to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from sabot import cyarrow as ca
from sabot.cyarrow import compute as pc


# ============================================================================
# VERSION 1: ORIGINAL (Python Loops)
# ============================================================================

def query7_original(
    persons: ca.Table,
    lives_in: ca.Table,
    has_interest: ca.Table,
    cities: ca.Table,
    states: ca.Table,
    city_in: ca.Table,
    interests: ca.Table,
    country: str,
    min_age: int,
    max_age: int,
    interest: str
) -> ca.Table:
    """
    Original implementation from queries.py (lines 399-497).
    Uses Python loops for city→state mapping and state counting.

    Bottleneck: Lines 462-470 call .filter() 500-1000 times (40-50ms overhead)
    """
    # Step 1: Find interest ID
    interest_lower = interest.lower()
    interest_id = None
    for i in range(interests.num_rows):
        if interests.column('interest')[i].as_py().lower() == interest_lower:
            interest_id = interests.column('id')[i].as_py()
            break

    if interest_id is None:
        return ca.table({'numPersons': [0], 'state': [''], 'country': ['']})

    # Step 2: Filter states by country
    country_mask = pc.equal(states.column('country'), ca.scalar(country))
    country_states = states.filter(country_mask)
    country_state_ids = set(country_states.column('id').to_pylist())

    # Step 3: Build city→state mapping (PYTHON LOOP #1 - ~10-15ms)
    city_to_state = {}
    for i in range(city_in.num_rows):  # 7,117 iterations
        city_id = city_in.column('source')[i].as_py()
        state_id = city_in.column('target')[i].as_py()
        if state_id in country_state_ids:
            city_to_state[city_id] = state_id

    # Step 4: Filter persons by age
    age_mask = pc.and_(
        pc.greater_equal(persons.column('age'), ca.scalar(min_age)),
        pc.less_equal(persons.column('age'), ca.scalar(max_age))
    )
    age_filtered = persons.filter(age_mask)

    # Step 5: Find persons with interest
    interest_mask = pc.equal(has_interest.column('target'), ca.scalar(interest_id))
    interest_edges = has_interest.filter(interest_mask)
    person_with_interest_ids = set(interest_edges.column('source').to_pylist())

    # Step 6: Intersect age filter + interest filter
    matching_person_ids = set(age_filtered.column('id').to_pylist()) & person_with_interest_ids

    # Step 7: Count persons by state (PYTHON LOOP #2 - ~40-50ms BOTTLENECK!)
    state_counts = {}
    for person_id in matching_person_ids:  # ~500-1000 iterations
        person_mask = pc.equal(lives_in.column('source'), ca.scalar(person_id))
        residence = lives_in.filter(person_mask)  # ❌ FILTER CALLED PER PERSON!

        if residence.num_rows > 0:
            city_id = residence.column('target')[0].as_py()
            if city_id in city_to_state:
                state_id = city_to_state[city_id]
                state_counts[state_id] = state_counts.get(state_id, 0) + 1

    # Step 8: Find state with max count
    if not state_counts:
        return ca.table({'numPersons': [0], 'state': [''], 'country': ['']})

    max_state_id = max(state_counts.items(), key=lambda x: x[1])[0]
    max_count = state_counts[max_state_id]

    # Get state info
    state_mask = pc.equal(states.column('id'), ca.scalar(max_state_id))
    state_row = states.filter(state_mask)
    state_name = state_row.column('state')[0].as_py()
    state_country = state_row.column('country')[0].as_py()

    return ca.table({
        'numPersons': [max_count],
        'state': [state_name],
        'country': [state_country]
    })


# ============================================================================
# VERSION 2: STREAM API (Auto-Numba Compilation)
# ============================================================================

def query7_stream_api(
    persons: ca.Table,
    lives_in: ca.Table,
    has_interest: ca.Table,
    cities: ca.Table,
    states: ca.Table,
    city_in: ca.Table,
    interests: ca.Table,
    country: str,
    min_age: int,
    max_age: int,
    interest: str
) -> ca.Table:
    """
    Stream API version using CythonMapOperator to trigger auto-Numba.

    Wraps the computation in a map function that gets auto-compiled by Numba.
    This tests whether Numba's @njit can speed up the Python loops.
    """
    from sabot._cython.operators.transform import CythonMapOperator

    # Step 1: Find interest ID (keep as-is)
    interest_lower = interest.lower()
    interest_id = None
    for i in range(interests.num_rows):
        if interests.column('interest')[i].as_py().lower() == interest_lower:
            interest_id = interests.column('id')[i].as_py()
            break

    if interest_id is None:
        return ca.table({'numPersons': [0], 'state': [''], 'country': ['']})

    # Step 2: Filter states by country
    country_mask = pc.equal(states.column('country'), ca.scalar(country))
    country_states = states.filter(country_mask)
    country_state_ids = set(country_states.column('id').to_pylist())

    # Step 3: Build city→state mapping (keep as-is for now)
    city_to_state = {}
    for i in range(city_in.num_rows):
        city_id = city_in.column('source')[i].as_py()
        state_id = city_in.column('target')[i].as_py()
        if state_id in country_state_ids:
            city_to_state[city_id] = state_id

    # Step 4: Filter persons by age
    age_mask = pc.and_(
        pc.greater_equal(persons.column('age'), ca.scalar(min_age)),
        pc.less_equal(persons.column('age'), ca.scalar(max_age))
    )
    age_filtered = persons.filter(age_mask)

    # Step 5: Find persons with interest
    interest_mask = pc.equal(has_interest.column('target'), ca.scalar(interest_id))
    interest_edges = has_interest.filter(interest_mask)
    person_with_interest_ids = set(interest_edges.column('source').to_pylist())

    # Step 6: Intersect age filter + interest filter
    matching_person_ids = list(set(age_filtered.column('id').to_pylist()) & person_with_interest_ids)

    # Step 7: Use Stream API with map operator to trigger Numba compilation
    # Create a batch with matching person IDs
    person_batch = ca.RecordBatch.from_pydict({
        'person_id': matching_person_ids
    })

    # Define the computation function - THIS SHOULD BE AUTO-COMPILED BY NUMBA
    def count_persons_by_state(batch):
        """
        Count persons by state.

        This function has Python loops, so NumbaCompiler should detect:
        - has_loops = True
        - Strategy: NJIT
        - Should get @njit compilation

        BUT: It also uses Arrow operations (lives_in.filter), which Numba can't compile.
        Expected: Compilation will fail gracefully, fall back to Python.
        """
        state_counts = {}

        # This loop should trigger Numba NJIT detection
        for i in range(batch.num_rows):
            person_id = batch.column('person_id')[i].as_py()

            # Arrow filter - Numba can't compile this!
            person_mask = pc.equal(lives_in.column('source'), ca.scalar(person_id))
            residence = lives_in.filter(person_mask)

            if residence.num_rows > 0:
                city_id = residence.column('target')[0].as_py()
                if city_id in city_to_state:
                    state_id = city_to_state[city_id]
                    state_counts[state_id] = state_counts.get(state_id, 0) + 1

        # Convert to batch
        if not state_counts:
            return ca.RecordBatch.from_pydict({
                'state_id': [],
                'count': []
            })

        return ca.RecordBatch.from_pydict({
            'state_id': list(state_counts.keys()),
            'count': list(state_counts.values())
        })

    # Apply map operator - should trigger auto-Numba
    map_op = CythonMapOperator(
        source=iter([person_batch]),
        map_func=count_persons_by_state
    )

    # Execute
    result_batch = map_op.process_batch(person_batch)

    # Find max state
    if result_batch is None or result_batch.num_rows == 0:
        return ca.table({'numPersons': [0], 'state': [''], 'country': ['']})

    counts = result_batch.column('count').to_pylist()
    state_ids = result_batch.column('state_id').to_pylist()

    max_idx = counts.index(max(counts))
    max_state_id = state_ids[max_idx]
    max_count = counts[max_idx]

    # Get state info
    state_mask = pc.equal(states.column('id'), ca.scalar(max_state_id))
    state_row = states.filter(state_mask)
    state_name = state_row.column('state')[0].as_py()
    state_country = state_row.column('country')[0].as_py()

    return ca.table({
        'numPersons': [max_count],
        'state': [state_name],
        'country': [state_country]
    })


# ============================================================================
# VERSION 3: VECTORIZED (Arrow Joins)
# ============================================================================

def query7_vectorized(
    persons: ca.Table,
    lives_in: ca.Table,
    has_interest: ca.Table,
    cities: ca.Table,
    states: ca.Table,
    city_in: ca.Table,
    interests: ca.Table,
    country: str,
    min_age: int,
    max_age: int,
    interest: str
) -> ca.Table:
    """
    Fully vectorized implementation using Arrow joins and group_by.

    Replaces ALL Python loops with Arrow operations:
    - Interest lookup: vectorized filter
    - City→state mapping: vectorized filter (no dict needed!)
    - State counting: hash join + group_by

    Expected: 3-4x faster than original (61.95ms → 15-20ms)
    """
    # Step 1: Find interest ID (vectorized)
    interest_mask = pc.equal(
        pc.utf8_lower(interests.column('interest')),
        ca.scalar(interest.lower())
    )
    matching_interests = interests.filter(interest_mask)

    if matching_interests.num_rows == 0:
        return ca.table({'numPersons': [0], 'state': [''], 'country': ['']})

    interest_id = matching_interests.column('id')[0].as_py()

    # Step 2: Filter states by country
    country_mask = pc.equal(states.column('country'), ca.scalar(country))
    country_states = states.filter(country_mask)
    country_state_ids = set(country_states.column('id').to_pylist())

    # Step 3: Filter city_in by country states (vectorized - no dict needed!)
    state_mask = pc.is_in(city_in.column('target'), ca.array(list(country_state_ids)))
    country_city_in = city_in.filter(state_mask)

    # Step 4: Filter persons by age
    age_mask = pc.and_(
        pc.greater_equal(persons.column('age'), ca.scalar(min_age)),
        pc.less_equal(persons.column('age'), ca.scalar(max_age))
    )
    age_filtered = persons.filter(age_mask)

    # Step 5: Filter by interest
    interest_mask = pc.equal(has_interest.column('target'), ca.scalar(interest_id))
    interest_edges = has_interest.filter(interest_mask)

    # Step 6: Join persons (age filter) with has_interest
    persons_with_interest = age_filtered.join(
        interest_edges.select(['source']),
        keys='id',
        right_keys='source',
        join_type='inner'
    )

    # Step 7: Join with lives_in to get city (VECTORIZED - replaces loop!)
    persons_with_city = persons_with_interest.join(
        lives_in.select(['source', 'target']),
        keys='id',
        right_keys='source',
        join_type='inner'
    )

    # Step 8: Join with city_in to get state (VECTORIZED - replaces dict lookup!)
    persons_with_state = persons_with_city.join(
        country_city_in.select(['source', 'target']),
        keys='target',
        right_keys='source',
        join_type='inner'
    )

    # The join gives us duplicate 'target' columns (city and state)
    # Schema: id, name, gender, birthday, age, isMarried, target (city), target (state)
    # Select just person_id (column 0) and state_id (column 7)
    persons_with_state_simple = ca.table({
        'person_id': persons_with_state.column(0),  # id
        'state_id': persons_with_state.column(7)     # second 'target' (state)
    })

    # Step 9: Group by state and count (VECTORIZED - replaces loop!)
    state_counts = persons_with_state_simple.group_by('state_id').aggregate([
        ('person_id', 'count')
    ])

    if state_counts.num_rows == 0:
        return ca.table({'numPersons': [0], 'state': [''], 'country': ['']})

    # Find state with max count
    max_count = pc.max(state_counts.column('person_id_count')).as_py()
    max_mask = pc.equal(state_counts.column('person_id_count'), ca.scalar(max_count))
    max_state = state_counts.filter(max_mask)
    max_state_id = max_state.column('state_id')[0].as_py()

    # Get state info
    state_mask = pc.equal(states.column('id'), ca.scalar(max_state_id))
    state_row = states.filter(state_mask)
    state_name = state_row.column('state')[0].as_py()
    state_country = state_row.column('country')[0].as_py()

    return ca.table({
        'numPersons': [max_count],
        'state': [state_name],
        'country': [state_country]
    })


# ============================================================================
# Benchmark Runner
# ============================================================================

def run_comparison(data_dir: Path, iterations: int = 10, warmup: int = 3):
    """Run all 3 versions and compare performance."""
    print("="*70)
    print("QUERY 7 PERFORMANCE COMPARISON")
    print("="*70)
    print()

    # Load data
    print("Loading data...")
    from data_loader import load_all_data
    data = load_all_data(data_dir)
    print("✅ Data loaded")
    print()

    # Query parameters
    params = {
        'persons': data['person_nodes'],
        'lives_in': data['lives_in_edges'],
        'has_interest': data['has_interest_edges'],
        'cities': data['city_nodes'],
        'states': data['state_nodes'],
        'city_in': data['city_in_edges'],
        'interests': data['interest_nodes'],
        'country': "United States",
        'min_age': 23,
        'max_age': 30,
        'interest': "photography"
    }

    versions = [
        ("ORIGINAL (Python Loops)", query7_original),
        ("STREAM API (Auto-Numba)", query7_stream_api),
        ("VECTORIZED (Arrow Joins)", query7_vectorized),
    ]

    results = []

    for name, query_func in versions:
        print(f"Testing: {name}")
        print("-"*70)

        # Warmup
        print(f"  Warming up ({warmup} iterations)...")
        for _ in range(warmup):
            query_func(**params)

        # Timed runs
        print(f"  Running ({iterations} iterations)...")
        times = []
        result = None

        for i in range(iterations):
            start = time.perf_counter()
            result = query_func(**params)
            elapsed = (time.perf_counter() - start) * 1000  # ms
            times.append(elapsed)

        avg_time = sum(times) / len(times)
        min_time = min(times)
        max_time = max(times)
        std_dev = (sum((t - avg_time) ** 2 for t in times) / len(times)) ** 0.5

        # Verify result
        result_dict = result.to_pydict()
        num_persons = result_dict['numPersons'][0]
        state = result_dict['state'][0]
        country = result_dict['country'][0]

        print(f"  ✅ Result: {num_persons} persons in {state}, {country}")
        print(f"  ⏱️  Avg: {avg_time:.2f}ms")
        print(f"     Min: {min_time:.2f}ms | Max: {max_time:.2f}ms | StdDev: {std_dev:.2f}ms")
        print()

        results.append({
            'name': name,
            'avg': avg_time,
            'min': min_time,
            'max': max_time,
            'std': std_dev,
            'result': result_dict
        })

    # Print comparison
    print("="*70)
    print("PERFORMANCE COMPARISON")
    print("="*70)
    print()

    baseline = results[0]['avg']  # Original as baseline

    print(f"{'Version':<30} {'Avg (ms)':>12} {'vs Original':>15} {'Status':>12}")
    print("-"*70)

    for r in results:
        speedup = baseline / r['avg']
        if speedup > 1:
            vs_str = f"{speedup:.2f}x faster"
            status = "✅ FASTER"
        elif speedup < 1:
            vs_str = f"{1/speedup:.2f}x slower"
            status = "⚠️ SLOWER"
        else:
            vs_str = "same"
            status = "="

        print(f"{r['name']:<30} {r['avg']:>12.2f} {vs_str:>15} {status:>12}")

    print()
    print("="*70)
    print("CONCLUSIONS")
    print("="*70)
    print()

    # Analyze Stream API result
    stream_speedup = baseline / results[1]['avg']
    if stream_speedup > 1.2:
        print(f"✅ Stream API (Auto-Numba): {stream_speedup:.2f}x faster")
        print("   → Numba compilation is working and providing speedup!")
    elif 0.8 < stream_speedup < 1.2:
        print(f"⚠️ Stream API (Auto-Numba): ~{stream_speedup:.2f}x (no significant change)")
        print("   → Numba compilation likely failed due to Arrow operations")
        print("   → Check logs for compilation warnings")
    else:
        print(f"❌ Stream API (Auto-Numba): {1/stream_speedup:.2f}x slower")
        print("   → Operator overhead outweighs any Numba benefit")

    print()

    # Analyze vectorized result
    vec_speedup = baseline / results[2]['avg']
    if vec_speedup > 2:
        print(f"✅ Vectorized (Arrow Joins): {vec_speedup:.2f}x faster")
        print("   → Arrow operations are significantly faster than Python loops")
        print("   → This is the recommended approach!")
    else:
        print(f"⚠️ Vectorized (Arrow Joins): {vec_speedup:.2f}x (unexpected)")
        print("   → Expected 3-4x speedup - investigate further")

    print()

    return results


def main():
    import argparse
    parser = argparse.ArgumentParser(description="Compare Query 7 implementations")
    parser.add_argument('--iterations', type=int, default=10,
                        help='Number of timed iterations (default: 10)')
    parser.add_argument('--warmup', type=int, default=3,
                        help='Number of warmup iterations (default: 3)')
    args = parser.parse_args()

    data_dir = Path(__file__).parent / "reference" / "data" / "output"

    if not data_dir.exists():
        print(f"❌ Error: Data directory not found: {data_dir}")
        print("   Run: cd reference/data && bash ../generate_data.sh 100000")
        return 1

    run_comparison(data_dir, iterations=args.iterations, warmup=args.warmup)

    print("✅ Comparison complete!")
    return 0


if __name__ == "__main__":
    sys.exit(main())
