#!/usr/bin/env python3
"""
Local Join - Join Two Data Sources
====================================

**What this demonstrates:**
- HASH_JOIN operator
- Multi-source pipelines
- Stream enrichment pattern
- Local execution with joins

**Prerequisites:**
- None! Runs completely locally

**Runtime:** ~2 seconds

**Next steps:**
- ../02_optimization/filter_pushdown_demo.py - See join optimization
- ../04_production_patterns/stream_enrichment/ - Production pattern

Run:
    python examples/00_quickstart/local_join.py
"""

import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

import pyarrow as pa
from sabot.execution.job_graph import JobGraph, StreamOperatorNode, OperatorType
from sabot._c.arrow_core import hash_join_batches


class LocalExecutor:
    """Simple local executor with join support."""

    def execute_pipeline(self, graph: JobGraph, input_data: dict):
        """Execute JobGraph locally."""
        print("Executing pipeline locally...")
        print()

        results = {}

        for op in graph.topological_sort():
            if op.operator_type == OperatorType.SOURCE:
                table_name = op.parameters.get('table_name')
                data = input_data.get(table_name)
                results[op.operator_id] = data
                print(f"[{op.name}] Loaded {data.num_rows:,} rows, {data.num_columns} columns")

            elif op.operator_type == OperatorType.HASH_JOIN:
                # Get both inputs
                left_id, right_id = op.upstream_ids
                left_data = results[left_id]
                right_data = results[right_id]

                # Perform join using Sabot's native hash join
                left_batch = left_data.to_batches()[0]
                right_batch = right_data.to_batches()[0]

                joined_batch = hash_join_batches(
                    left_batch, right_batch,
                    op.parameters['left_key'],
                    op.parameters['right_key'],
                    join_type='inner'
                )

                joined = pa.Table.from_batches([joined_batch])
                results[op.operator_id] = joined

                print(f"[{op.name}] Joined: {left_data.num_rows:,} × {right_data.num_rows:,} → {joined.num_rows:,} rows")

            elif op.operator_type == OperatorType.SINK:
                upstream_id = op.upstream_ids[0]
                data = results[upstream_id]
                results[op.operator_id] = data
                print(f"[{op.name}] Output: {data.num_rows:,} rows")

        sinks = graph.get_sinks()
        return results[sinks[0].operator_id] if sinks else None


def main():
    print("="*70)
    print("Local Join - Join Two Data Sources")
    print("="*70)
    print()

    # Step 1: Generate test data
    print("Step 1: Generate Test Data")
    print("-"*70)

    # Quotes (stream data)
    quotes = pa.table({
        'instrumentId': [1, 5, 10, 3, 7, 2, 8, 4, 6, 9],
        'price': [100.5, 200.3, 150.0, 300.2, 250.1, 180.5, 90.3, 120.0, 105.5, 151.0],
        'size': [100, 200, 150, 300, 250, 180, 90, 120, 105, 151]
    })

    # Securities (reference data)
    securities = pa.table({
        'ID': list(range(20)),
        'CUSIP': [f'CUSIP{i:04d}' for i in range(20)],
        'NAME': [f'Security {i}' for i in range(20)]
    })

    print(f"✅ Quotes: {quotes.num_rows} rows")
    print(f"✅ Securities: {securities.num_rows} rows")
    print()

    # Step 2: Build JobGraph
    print("Step 2: Build JobGraph with Join")
    print("-"*70)

    graph = JobGraph(job_name="local_join")

    # Source 1: Quotes
    source_quotes = StreamOperatorNode(
        operator_type=OperatorType.SOURCE,
        name="load_quotes",
        parameters={'table_name': 'quotes'}
    )
    graph.add_operator(source_quotes)

    # Source 2: Securities
    source_securities = StreamOperatorNode(
        operator_type=OperatorType.SOURCE,
        name="load_securities",
        parameters={'table_name': 'securities'}
    )
    graph.add_operator(source_securities)

    # Join: Enrich quotes with security details
    join_op = StreamOperatorNode(
        operator_type=OperatorType.HASH_JOIN,
        name="enrich_quotes",
        parameters={
            'left_key': 'instrumentId',
            'right_key': 'ID'
        }
    )
    graph.add_operator(join_op)

    # Sink
    sink = StreamOperatorNode(
        operator_type=OperatorType.SINK,
        name="output_enriched",
        parameters={}
    )
    graph.add_operator(sink)

    # Connect pipeline
    graph.connect(source_quotes.operator_id, join_op.operator_id)
    graph.connect(source_securities.operator_id, join_op.operator_id)
    graph.connect(join_op.operator_id, sink.operator_id)

    print(f"✅ Built pipeline:")
    print(f"   Quotes    ↘")
    print(f"               Join → Sink")
    print(f"   Securities ↗")
    print()

    # Step 3: Execute
    print("Step 3: Execute Pipeline")
    print("-"*70)

    executor = LocalExecutor()
    result = executor.execute_pipeline(graph, {
        'quotes': quotes,
        'securities': securities
    })
    print()

    # Step 4: Show results
    print("Step 4: Results")
    print("-"*70)

    print(f"Enriched data: {result.num_rows} rows, {result.num_columns} columns")
    print()
    print("Sample output:")
    print(result.to_pandas().head(5))
    print()

    # Step 5: Summary
    print("="*70)
    print("Success!")
    print("="*70)
    print()
    print("Pipeline executed:")
    print(f"  1. Loaded {quotes.num_rows} quotes")
    print(f"  2. Loaded {securities.num_rows} securities")
    print(f"  3. Joined on instrumentId=ID")
    print(f"  4. Output {result.num_rows} enriched rows")
    print()
    print("Key concepts:")
    print("  • HASH_JOIN - Inner join on key columns")
    print("  • Multi-source - Multiple data inputs")
    print("  • Stream enrichment - Enrich streaming data with reference tables")
    print("  • Native join - Uses Arrow C++ SIMD (830M rows/sec)")
    print()
    print("Next steps:")
    print("  • ../02_optimization/filter_pushdown_demo.py - Join optimization")
    print("  • ../03_distributed_basics/two_agents_simple.py - Distributed join")
    print("  • ../04_production_patterns/stream_enrichment/ - Production pattern")


if __name__ == '__main__':
    main()
