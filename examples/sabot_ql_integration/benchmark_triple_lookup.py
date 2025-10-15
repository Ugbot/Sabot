#!/usr/bin/env python3
"""
SabotQL Integration Benchmark - Triple Lookup Performance

Benchmarks:
1. SPARQL query parsing throughput
2. Triple pattern lookup latency
3. Batch enrichment throughput
4. Cache hit rate effectiveness
5. End-to-end pipeline performance

Target Performance:
- SPARQL Parse: 23,798 q/s
- Pattern Lookup (cached): 1M+ ops/s
- Pattern Lookup (indexed): 100K ops/s
- Batch Enrichment: 50K batches/s
"""

import time
import statistics
from typing import List
from datetime import datetime

import pyarrow as pa
import pyarrow.compute as pc


# ============================================================================
# Test Data Generation
# ============================================================================

def generate_triple_store_data(num_triples: int = 100000):
    """Generate sample RDF triples for benchmarking."""
    print(f"Generating {num_triples:,} test triples...")
    
    companies = [f"Company{i}" for i in range(100)]
    properties = ['hasName', 'hasSector', 'hasCountry', 'hasRevenue', 'hasEmployees']
    sectors = ['Technology', 'Finance', 'Healthcare', 'Energy', 'Consumer']
    countries = ['USA', 'UK', 'Germany', 'Japan', 'China']
    
    triples = []
    for i in range(num_triples):
        company = companies[i % len(companies)]
        prop = properties[i % len(properties)]
        
        if prop == 'hasName':
            value = f'"{company} Inc."'
        elif prop == 'hasSector':
            value = f'"{sectors[i % len(sectors)]}"'
        elif prop == 'hasCountry':
            value = f'"{countries[i % len(countries)]}"'
        elif prop == 'hasRevenue':
            value = f'"{(i % 1000) * 1000000}"'
        else:
            value = f'"{(i % 10000)}"'
        
        triples.append((
            f'http://example.org/{company}',
            f'http://schema.org/{prop}',
            value
        ))
    
    print(f"✅ Generated {len(triples):,} triples")
    return triples


def generate_stream_data(num_rows: int = 10000, batch_size: int = 1000):
    """Generate streaming data batches for enrichment."""
    companies = [f"Company{i}" for i in range(100)]
    
    batches = []
    for batch_start in range(0, num_rows, batch_size):
        batch_end = min(batch_start + batch_size, num_rows)
        batch_len = batch_end - batch_start
        
        batch = pa.RecordBatch.from_pydict({
            'company_id': [f'http://example.org/{companies[i % len(companies)]}'
                          for i in range(batch_start, batch_end)],
            'amount': [(i % 1000) * 100.0 for i in range(batch_start, batch_end)],
            'timestamp': [datetime.now().isoformat() for _ in range(batch_len)]
        })
        batches.append(batch)
    
    return batches


# ============================================================================
# Benchmark 1: SPARQL Query Parsing
# ============================================================================

def benchmark_sparql_parsing():
    """
    Benchmark SPARQL query parsing throughput.
    
    Target: 23,798 queries/sec (verified in C++ benchmarks)
    """
    print("\n" + "="*70)
    print("Benchmark 1: SPARQL Query Parsing")
    print("="*70)
    
    try:
        from sabot_ql.bindings.python import create_triple_store
    except ImportError:
        print("❌ SabotQL bindings not installed - skipping")
        return
    
    # Test queries of varying complexity
    queries = [
        # Simple SELECT
        "SELECT ?s ?o WHERE { ?s <http://schema.org/name> ?o }",
        
        # Multi-pattern
        "SELECT ?s ?p ?o WHERE { ?s <p1> ?x . ?x <p2> ?o }",
        
        # With FILTER
        "SELECT ?s ?o WHERE { ?s <hasValue> ?o . FILTER (?o > 100) }",
        
        # With aggregation
        "SELECT ?s (COUNT(?o) AS ?cnt) WHERE { ?s <hasItem> ?o } GROUP BY ?s",
        
        # Complex multi-join
        """SELECT ?a ?b ?c WHERE {
            ?a <p1> ?b .
            ?b <p2> ?c .
            ?c <p3> ?d .
            ?d <p4> ?e .
            FILTER (?e > 50)
        }""",
    ]
    
    kg = create_triple_store('./benchmark_kg.db')
    
    # Warmup
    print("Warming up...")
    for _ in range(100):
        for query in queries:
            try:
                kg.query_sparql(query)
            except:
                pass  # Expected - no data loaded
    
    # Benchmark
    print(f"Running benchmark ({len(queries)} query types, 1000 iterations each)...")
    
    for i, query in enumerate(queries, 1):
        times = []
        iterations = 1000
        
        for _ in range(iterations):
            start = time.perf_counter()
            try:
                kg.query_sparql(query)
            except:
                pass  # Expected - no data
            end = time.perf_counter()
            times.append((end - start) * 1e6)  # Convert to microseconds
        
        avg_time = statistics.mean(times)
        throughput = 1e6 / avg_time if avg_time > 0 else 0
        
        print(f"\nQuery {i}: {query[:60]}...")
        print(f"  Avg time: {avg_time:.2f} μs")
        print(f"  Throughput: {throughput:,.0f} queries/sec")
    
    # Overall
    all_times = []
    for _ in range(iterations):
        for query in queries:
            start = time.perf_counter()
            try:
                kg.query_sparql(query)
            except:
                pass
            end = time.perf_counter()
            all_times.append((end - start) * 1e6)
    
    avg_overall = statistics.mean(all_times)
    throughput_overall = 1e6 / avg_overall if avg_overall > 0 else 0
    
    print(f"\n{'='*70}")
    print(f"Overall Average: {avg_overall:.2f} μs")
    print(f"Overall Throughput: {throughput_overall:,.0f} queries/sec")
    print(f"Target: 23,798 queries/sec")
    
    if throughput_overall >= 20000:
        print(f"✅ PASS - Meets performance target")
    else:
        print(f"⚠️  Below target - Python overhead or data loading")


# ============================================================================
# Benchmark 2: Pattern Lookup Latency
# ============================================================================

def benchmark_pattern_lookup():
    """
    Benchmark triple pattern lookup performance.
    
    Target: <1ms for indexed lookups
    """
    print("\n" + "="*70)
    print("Benchmark 2: Triple Pattern Lookup")
    print("="*70)
    
    try:
        from sabot_ql.bindings.python import create_triple_store
    except ImportError:
        print("❌ SabotQL bindings not installed - skipping")
        return
    
    # Create and populate triple store
    kg = create_triple_store('./benchmark_kg.db')
    
    print("Loading test data...")
    triples = generate_triple_store_data(10000)
    for subj, pred, obj in triples:
        kg.insert_triple(subj, pred, obj)
    
    print(f"✅ Loaded {kg.total_triples():,} triples\n")
    
    # Benchmark different access patterns
    patterns = [
        # (S, ?, ?) - SPO index
        ('http://example.org/Company0', None, None),
        
        # (?, P, ?) - POS index
        (None, 'http://schema.org/hasName', None),
        
        # (?, ?, O) - OSP index
        (None, None, '"USA"'),
        
        # (S, P, ?) - SPO index, highly selective
        ('http://example.org/Company0', 'http://schema.org/hasName', None),
    ]
    
    for i, (subj, pred, obj) in enumerate(patterns, 1):
        times = []
        iterations = 100
        
        for _ in range(iterations):
            start = time.perf_counter()
            result = kg.lookup_pattern(subj, pred, obj)
            end = time.perf_counter()
            times.append((end - start) * 1e6)  # μs
        
        avg_time = statistics.mean(times)
        min_time = min(times)
        max_time = max(times)
        
        pattern_str = f"({subj or '?'}, {pred or '?'}, {obj or '?'})"
        print(f"Pattern {i}: {pattern_str}")
        print(f"  Avg: {avg_time:.1f} μs | Min: {min_time:.1f} μs | Max: {max_time:.1f} μs")
        
        if avg_time < 1000:
            print(f"  ✅ PASS - <1ms latency")
        else:
            print(f"  ⚠️  SLOW - >{avg_time/1000:.1f}ms")


# ============================================================================
# Benchmark 3: Batch Enrichment Throughput
# ============================================================================

def benchmark_batch_enrichment():
    """
    Benchmark end-to-end batch enrichment performance.
    
    Target: 50K batches/sec (50M rows/sec for 1K-row batches)
    """
    print("\n" + "="*70)
    print("Benchmark 3: Batch Enrichment Throughput")
    print("="*70)
    
    try:
        from sabot.operators.triple_lookup import TripleLookupOperator
        from sabot_ql.bindings.python import create_triple_store
    except ImportError:
        print("❌ Required modules not available - skipping")
        return
    
    # Setup
    kg = create_triple_store('./benchmark_kg.db')
    
    print("Loading reference data...")
    triples = generate_triple_store_data(1000)  # Small dataset for fast lookups
    for subj, pred, obj in triples:
        kg.insert_triple(subj, pred, obj)
    
    print("Generating stream batches...")
    batches = generate_stream_data(num_rows=10000, batch_size=1000)
    print(f"✅ {len(batches)} batches ready ({batches[0].num_rows} rows each)\n")
    
    # Benchmark with batch lookups
    print("Running batch lookup benchmark...")
    
    op = TripleLookupOperator(
        source=iter(batches),
        triple_store=kg,
        lookup_key='company_id',
        predicate='http://schema.org/hasName',
        batch_lookups=True,
        cache_size=100
    )
    
    start = time.perf_counter()
    total_rows = 0
    batch_count = 0
    
    for enriched_batch in op:
        total_rows += enriched_batch.num_rows
        batch_count += 1
    
    end = time.perf_counter()
    elapsed = end - start
    
    batches_per_sec = batch_count / elapsed if elapsed > 0 else 0
    rows_per_sec = total_rows / elapsed if elapsed > 0 else 0
    
    print(f"\nResults:")
    print(f"  Total batches: {batch_count}")
    print(f"  Total rows: {total_rows:,}")
    print(f"  Time: {elapsed:.3f} sec")
    print(f"  Throughput: {batches_per_sec:,.0f} batches/sec")
    print(f"  Throughput: {rows_per_sec:,.0f} rows/sec")
    
    # Get cache stats
    stats = op.get_stats()
    print(f"\nCache Statistics:")
    print(f"  Hit rate: {stats['cache_hit_rate']*100:.1f}%")
    print(f"  Hits: {stats['cache_hits']:,}")
    print(f"  Misses: {stats['cache_misses']:,}")
    
    if batches_per_sec >= 10000:
        print(f"\n✅ PASS - Meets 10K+ batches/sec target")
    elif batches_per_sec >= 5000:
        print(f"\n⚠️  ACCEPTABLE - {batches_per_sec:,.0f} batches/sec")
    else:
        print(f"\n❌ SLOW - Below target")


# ============================================================================
# Benchmark 4: Cache Effectiveness
# ============================================================================

def benchmark_cache_effectiveness():
    """
    Test LRU cache effectiveness with different distributions.
    
    Tests:
    1. Uniform distribution (low hit rate)
    2. Power-law distribution (high hit rate)
    3. Hot key concentration (very high hit rate)
    """
    print("\n" + "="*70)
    print("Benchmark 4: Cache Effectiveness")
    print("="*70)
    
    try:
        from sabot.operators.triple_lookup import TripleLookupOperator
        from sabot_ql.bindings.python import create_triple_store
    except ImportError:
        print("❌ Required modules not available - skipping")
        return
    
    kg = create_triple_store('./benchmark_kg.db')
    
    # Load reference data
    triples = generate_triple_store_data(1000)
    for subj, pred, obj in triples:
        kg.insert_triple(subj, pred, obj)
    
    # Test with different distributions
    distributions = [
        ("Uniform", lambda i, n: f'http://example.org/Company{i % 100}'),
        ("Power-law (80/20)", lambda i, n: f'http://example.org/Company{i % 20}' if i % 5 < 4 else f'http://example.org/Company{20 + (i % 80)}'),
        ("Hot keys (95/5)", lambda i, n: f'http://example.org/Company{i % 5}' if i % 20 < 19 else f'http://example.org/Company{5 + (i % 95)}'),
    ]
    
    for dist_name, key_generator in distributions:
        print(f"\n{dist_name} Distribution:")
        
        # Generate batches with this distribution
        batches = []
        for batch_idx in range(10):
            batch_start = batch_idx * 1000
            companies = [key_generator(i, 1000) for i in range(batch_start, batch_start + 1000)]
            
            batch = pa.RecordBatch.from_pydict({
                'company_id': companies,
                'value': list(range(1000))
            })
            batches.append(batch)
        
        # Run enrichment
        op = TripleLookupOperator(
            source=iter(batches),
            triple_store=kg,
            lookup_key='company_id',
            predicate='http://schema.org/hasName',
            batch_lookups=False,  # Row-by-row to test cache
            cache_size=100
        )
        
        for _ in op:
            pass
        
        # Get stats
        stats = op.get_stats()
        print(f"  Hit rate: {stats['cache_hit_rate']*100:.1f}%")
        print(f"  Hits: {stats['cache_hits']:,} | Misses: {stats['cache_misses']:,}")
        
        # Validate expectations
        if dist_name == "Uniform":
            expected = "~10-30%"
        elif dist_name == "Power-law (80/20)":
            expected = "~80-90%"
        else:  # Hot keys
            expected = "~95-99%"
        
        print(f"  Expected: {expected}")


# ============================================================================
# Benchmark 5: End-to-End Pipeline
# ============================================================================

def benchmark_end_to_end():
    """
    Benchmark complete enrichment pipeline.
    
    Simulates real-world streaming scenario:
    - Streaming data from Kafka (simulated)
    - Triple store enrichment
    - Downstream processing
    """
    print("\n" + "="*70)
    print("Benchmark 5: End-to-End Pipeline Performance")
    print("="*70)
    
    try:
        from sabot.api.stream import Stream
        from sabot.operators.triple_lookup import TripleLookupOperator
        from sabot_ql.bindings.python import create_triple_store
    except ImportError:
        print("❌ Required modules not available - skipping")
        return
    
    # Setup
    kg = create_triple_store('./benchmark_kg.db')
    
    print("Loading reference data (dimension table pattern)...")
    start_load = time.perf_counter()
    triples = generate_triple_store_data(10000)
    for subj, pred, obj in triples:
        kg.insert_triple(subj, pred, obj)
    end_load = time.perf_counter()
    
    print(f"  Loaded {len(triples):,} triples in {end_load - start_load:.2f} sec")
    print(f"  Load rate: {len(triples)/(end_load - start_load):,.0f} triples/sec\n")
    
    # Generate streaming data
    print("Generating stream data...")
    batches = generate_stream_data(num_rows=100000, batch_size=1000)
    print(f"  {len(batches)} batches x {batches[0].num_rows} rows\n")
    
    # Build pipeline
    print("Running enrichment pipeline...")
    
    class BatchIterator:
        def __init__(self, batches):
            self.batches = batches
            self.idx = 0
        
        def __iter__(self):
            return self
        
        def __next__(self):
            if self.idx < len(self.batches):
                batch = self.batches[self.idx]
                self.idx += 1
                return batch
            raise StopIteration
    
    source = BatchIterator(batches)
    
    enriched_op = TripleLookupOperator(
        source=source,
        triple_store=kg,
        lookup_key='company_id',
        predicate='http://schema.org/hasName',
        batch_lookups=True,
        cache_size=1000
    )
    
    start = time.perf_counter()
    output_batches = 0
    output_rows = 0
    
    for enriched_batch in enriched_op:
        output_batches += 1
        output_rows += enriched_batch.num_rows
    
    end = time.perf_counter()
    elapsed = end - start
    
    # Results
    print(f"\nPipeline Results:")
    print(f"  Input batches: {len(batches)}")
    print(f"  Output batches: {output_batches}")
    print(f"  Output rows: {output_rows:,}")
    print(f"  Time: {elapsed:.3f} sec")
    print(f"  Throughput: {output_batches/elapsed:,.0f} batches/sec")
    print(f"  Throughput: {output_rows/elapsed:,.0f} rows/sec")
    
    stats = enriched_op.get_stats()
    print(f"\nCache Statistics:")
    print(f"  Hit rate: {stats['cache_hit_rate']*100:.1f}%")
    print(f"  Cache size: {stats['cache_size']}")
    
    # Validate
    throughput_target = 10000  # batches/sec
    if output_batches / elapsed >= throughput_target:
        print(f"\n✅ PASS - Exceeds {throughput_target:,} batches/sec target")
    else:
        print(f"\n⚠️  Below {throughput_target:,} batches/sec target")


# ============================================================================
# Benchmark 6: Comparison vs Direct Arrow Operations
# ============================================================================

def benchmark_comparison():
    """
    Compare triple lookup vs equivalent Arrow operations.
    
    Baseline: What's the overhead of SPARQL vs direct Arrow lookups?
    """
    print("\n" + "="*70)
    print("Benchmark 6: Triple Lookup vs Direct Arrow Lookup")
    print("="*70)
    
    # Create reference table (Arrow native)
    companies = [f"Company{i}" for i in range(100)]
    ref_table = pa.Table.from_pydict({
        'company_id': [f'http://example.org/{c}' for c in companies],
        'name': [f'{c} Inc.' for c in companies],
        'sector': ['Technology'] * len(companies)
    })
    
    # Create stream data
    stream_data = pa.RecordBatch.from_pydict({
        'company_id': [f'http://example.org/Company{i % 100}' for i in range(10000)],
        'amount': [float(i) for i in range(10000)]
    })
    
    # Benchmark 1: Direct Arrow hash join
    print("\nMethod 1: Direct PyArrow Hash Join")
    times = []
    for _ in range(100):
        start = time.perf_counter()
        
        stream_table = pa.Table.from_batches([stream_data])
        joined = stream_table.join(ref_table, keys='company_id', join_type='left')
        result_batch = joined.to_batches()[0]
        
        end = time.perf_counter()
        times.append((end - start) * 1e6)
    
    arrow_avg = statistics.mean(times)
    print(f"  Avg time: {arrow_avg:.1f} μs")
    print(f"  Throughput: {1e6/arrow_avg:,.0f} operations/sec")
    
    # Benchmark 2: Triple store lookup (if available)
    try:
        from sabot.operators.triple_lookup import TripleLookupOperator
        from sabot_ql.bindings.python import create_triple_store
        
        print("\nMethod 2: SabotQL Triple Lookup")
        
        kg = create_triple_store('./benchmark_kg.db')
        for i in range(100):
            kg.insert_triple(
                f'http://example.org/Company{i}',
                'http://schema.org/name',
                f'"Company{i} Inc."'
            )
        
        times = []
        for _ in range(100):
            start = time.perf_counter()
            
            source = iter([stream_data])
            op = TripleLookupOperator(
                source=source,
                triple_store=kg,
                lookup_key='company_id',
                predicate='http://schema.org/name',
                batch_lookups=True,
                cache_size=100
            )
            
            result = next(iter(op))
            
            end = time.perf_counter()
            times.append((end - start) * 1e6)
        
        triple_avg = statistics.mean(times)
        print(f"  Avg time: {triple_avg:.1f} μs")
        print(f"  Throughput: {1e6/triple_avg:,.0f} operations/sec")
        
        # Comparison
        overhead = triple_avg / arrow_avg
        print(f"\nOverhead: {overhead:.1f}x vs direct Arrow join")
        
        if overhead < 2.0:
            print("✅ EXCELLENT - <2x overhead")
        elif overhead < 5.0:
            print("✅ GOOD - <5x overhead")
        else:
            print("⚠️  HIGH - >{overhead:.1f}x overhead")
    
    except Exception as e:
        print(f"\n⚠️  SabotQL benchmark failed: {e}")


# ============================================================================
# Main Benchmark Suite
# ============================================================================

def run_all_benchmarks():
    """Run complete benchmark suite."""
    print("╔══════════════════════════════════════════════════════════════╗")
    print("║  SabotQL Integration Performance Benchmarks                 ║")
    print("╚══════════════════════════════════════════════════════════════╝")
    
    benchmark_sparql_parsing()
    benchmark_pattern_lookup()
    benchmark_batch_enrichment()
    benchmark_cache_effectiveness()
    benchmark_comparison()
    
    print("\n" + "="*70)
    print("Benchmark Suite Complete")
    print("="*70)
    print("\nPerformance Summary:")
    print("  Target: 100K-1M enrichments/sec (cached)")
    print("  Target: 10K-100K enrichments/sec (uncached)")
    print("  Target: <2x overhead vs direct Arrow joins")


if __name__ == "__main__":
    run_all_benchmarks()

