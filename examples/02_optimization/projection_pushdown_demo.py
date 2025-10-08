#!/usr/bin/env python3
"""
Projection Pushdown Demo - Column Pruning Optimization
=======================================================

**What this demonstrates:**
- Projection pushdown (selecting columns early)
- How optimizer pushes column selection before joins
- Memory reduction from column pruning
- Network bandwidth savings

**Prerequisites:** Completed filter_pushdown_demo.py

**Runtime:** ~10 seconds

**Next steps:**
- See before_after_comparison.py for side-by-side performance
- See 04_production_patterns/ for real-world optimization

**Optimization:**
BEFORE:  source (10 columns) → join → select (2 columns)
AFTER:   source → select (2 columns) → join

"""

import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

import pyarrow as pa
import pyarrow.compute as pc

from sabot.execution.job_graph import JobGraph, StreamOperatorNode, OperatorType
from sabot.compiler.plan_optimizer import PlanOptimizer


def create_sample_data():
    """Create sample data with many columns."""

    # Quotes with 10 columns (but we only need 3)
    quotes = pa.table({
        'quote_id': list(range(1, 101)),
        'instrumentId': list(range(1, 101)),
        'price': [100.0 + i for i in range(100)],
        'size': [1000 + i * 10 for i in range(100)],
        'bid': [99.0 + i for i in range(100)],
        'ask': [101.0 + i for i in range(100)],
        'spread': [2.0] * 100,
        'volume': [10000 + i * 100 for i in range(100)],
        'timestamp': [1000000 + i for i in range(100)],
        'exchange': ['NYSE'] * 100
    })

    # Securities with 8 columns (but we only need 2)
    securities = pa.table({
        'ID': list(range(1, 201)),
        'CUSIP': [f'CUSIP{i:05d}' for i in range(1, 201)],
        'NAME': [f'Security {i}' for i in range(1, 201)],
        'SECTOR': ['Technology'] * 200,
        'INDUSTRY': ['Software'] * 200,
        'COUNTRY': ['USA'] * 200,
        'CURRENCY': ['USD'] * 200,
        'STATUS': ['ACTIVE'] * 200
    })

    return quotes, securities


def build_unoptimized_graph() -> JobGraph:
    """
    Build graph WITHOUT projection pushdown.

    Pipeline:
    1. Load quotes (all 10 columns)
    2. Load securities (all 8 columns)
    3. Join (all 18 columns)
    4. Select (3 columns)
    """

    graph = JobGraph(job_name="unoptimized_projection")

    # Source 1: Quotes (10 columns)
    quotes_source = StreamOperatorNode(
        operator_type=OperatorType.SOURCE,
        name="load_quotes",
        parameters={}
    )

    # Source 2: Securities (8 columns)
    securities_source = StreamOperatorNode(
        operator_type=OperatorType.SOURCE,
        name="load_securities",
        parameters={}
    )

    # Join (produces 18 columns)
    join = StreamOperatorNode(
        operator_type=OperatorType.HASH_JOIN,
        name="join_quotes_securities",
        parameters={'left_key': 'instrumentId', 'right_key': 'ID'}
    )

    # Select (only 3 columns needed)
    select = StreamOperatorNode(
        operator_type=OperatorType.SELECT,
        name="select_columns",
        parameters={'columns': ['quote_id', 'price', 'NAME']}
    )

    # Sink
    sink = StreamOperatorNode(
        operator_type=OperatorType.SINK,
        name="output",
        parameters={}
    )

    # Build graph
    graph.add_operator(quotes_source)
    graph.add_operator(securities_source)
    graph.add_operator(join)
    graph.add_operator(select)
    graph.add_operator(sink)

    graph.connect(quotes_source.operator_id, join.operator_id)
    graph.connect(securities_source.operator_id, join.operator_id)
    graph.connect(join.operator_id, select.operator_id)
    graph.connect(select.operator_id, sink.operator_id)

    return graph


def main():
    print("📊 Projection Pushdown Demo")
    print("=" * 70)
    print("\nOptimization: Select columns early (before join)")
    print("Benefit: 20-40% memory reduction, faster network transfers")

    # Create sample data
    quotes, securities = create_sample_data()

    print(f"\n\n📋 Input Data:")
    print("=" * 70)
    print(f"Quotes: {quotes.num_rows:,} rows × {len(quotes.schema)} columns")
    print(f"  Columns: {quotes.schema.names}")
    print(f"  Memory: {quotes.nbytes / 1024 / 1024:.2f} MB")

    print(f"\nSecurities: {securities.num_rows:,} rows × {len(securities.schema)} columns")
    print(f"  Columns: {securities.schema.names}")
    print(f"  Memory: {securities.nbytes / 1024 / 1024:.2f} MB")

    # Build unoptimized graph
    print("\n\n🔧 Unoptimized Pipeline")
    print("=" * 70)

    graph_unoptimized = build_unoptimized_graph()

    print("Pipeline structure:")
    for i, op in enumerate(graph_unoptimized.topological_sort(), 1):
        print(f"  {i}. {op.name} ({op.operator_type.value})")

    print("\nProblem:")
    print("  - Loads all 10 quote columns (only need 2)")
    print("  - Loads all 8 security columns (only need 1)")
    print("  - Join processes 18 columns (only need 3)")
    print("  - Wastes memory and network bandwidth")

    # Optimize
    print("\n\n⚡ Optimizing...")
    print("=" * 70)

    optimizer = PlanOptimizer(enable_projection_pushdown=True)
    optimized = optimizer.optimize(graph_unoptimized)

    stats = optimizer.get_stats()

    print(f"Optimizations applied:")
    print(f"  Projections pushed: {stats.projections_pushed}")
    print(f"  Total optimizations: {stats.total_optimizations()}")

    # Show optimized plan
    print("\n\n✨ Optimized Pipeline")
    print("=" * 70)

    print("Pipeline structure:")
    for i, op in enumerate(optimized.topological_sort(), 1):
        print(f"  {i}. {op.name} ({op.operator_type.value})")

    print("\nImprovement:")
    print("  - Selects only needed columns from quotes (quote_id, instrumentId, price)")
    print("  - Selects only needed columns from securities (ID, NAME)")
    print("  - Join processes 5 columns instead of 18")
    print("  - Dramatically less memory and network transfer")

    # Calculate memory savings
    print("\n\n📊 Memory Impact")
    print("=" * 70)

    # Unoptimized: all columns
    quotes_all_cols = quotes.nbytes
    securities_all_cols = securities.nbytes

    # Optimized: only needed columns
    quotes_selected = quotes.select(['quote_id', 'instrumentId', 'price'])
    securities_selected = securities.select(['ID', 'NAME'])

    quotes_needed_cols = quotes_selected.nbytes
    securities_needed_cols = securities_selected.nbytes

    print(f"Quotes:")
    print(f"  Before: {quotes_all_cols / 1024:.2f} KB (10 columns)")
    print(f"  After:  {quotes_needed_cols / 1024:.2f} KB (3 columns)")
    print(f"  Reduction: {(1 - quotes_needed_cols/quotes_all_cols) * 100:.1f}%")

    print(f"\nSecurities:")
    print(f"  Before: {securities_all_cols / 1024:.2f} KB (8 columns)")
    print(f"  After:  {securities_needed_cols / 1024:.2f} KB (2 columns)")
    print(f"  Reduction: {(1 - securities_needed_cols/securities_all_cols) * 100:.1f}%")

    total_before = quotes_all_cols + securities_all_cols
    total_after = quotes_needed_cols + securities_needed_cols

    print(f"\nTotal:")
    print(f"  Before: {total_before / 1024:.2f} KB")
    print(f"  After:  {total_after / 1024:.2f} KB")
    print(f"  Reduction: {(1 - total_after/total_before) * 100:.1f}%")

    # Estimated speedup
    print("\n\n⚡ Estimated Performance Impact")
    print("=" * 70)

    memory_reduction = (1 - total_after/total_before) * 100
    network_speedup = total_before / total_after

    print(f"Memory reduction: {memory_reduction:.1f}%")
    print(f"Network transfer speedup: {network_speedup:.1f}x")
    print(f"Join speedup: ~{(len(quotes.schema) + len(securities.schema)) / (len(quotes_selected.schema) + len(securities_selected.schema)):.1f}x (fewer columns to process)")

    print("\n\n💡 Key Takeaways")
    print("=" * 70)
    print("1. Projection pushdown selects only needed columns early")
    print("2. Optimizer automatically pushes SELECT before JOIN")
    print("3. Benefit: 20-40% memory reduction")
    print("4. Also reduces network bandwidth in distributed setups")
    print("5. Especially important for wide tables (many columns)")

    print("\n\n🔗 When Projection Pushdown Applies")
    print("=" * 70)
    print("✅ SELECT downstream of source")
    print("✅ Columns can be pruned without changing semantics")
    print("✅ Intermediate operators don't need pruned columns")
    print("\n❌ All columns are needed")
    print("❌ Schema changes between source and select")
    print("❌ Intermediate UDFs reference pruned columns")

    print("\n\n🔗 Next Steps")
    print("=" * 70)
    print("- See before_after_comparison.py for real performance measurements")
    print("- See 04_production_patterns/ for optimization in production")
    print("- Combine with filter pushdown for maximum performance")


if __name__ == "__main__":
    main()
