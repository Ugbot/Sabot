#!/usr/bin/env python3
"""
Hello Sabot - Simplest Possible Example
========================================

**What this demonstrates:**
- Creating a JobGraph (logical plan)
- Adding operators (source, filter, sink)
- Connecting operators into a pipeline
- Local execution (no distributed agents)

**Prerequisites:**
- None! This runs completely locally

**Runtime:** <1 second

**Next steps:**
- filter_and_map.py - More operators
- local_join.py - Join two streams
- ../01_local_pipelines/ - Batch and streaming modes

Run:
    python examples/00_quickstart/hello_sabot.py
"""

import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

import pyarrow as pa
from sabot.execution.job_graph import JobGraph, StreamOperatorNode, OperatorType


def main():
    print("="*70)
    print("Hello Sabot - Simplest Possible Example")
    print("="*70)
    print()

    # Step 1: Create a JobGraph (logical plan)
    print("Step 1: Create JobGraph")
    print("-"*70)

    graph = JobGraph(job_name="hello_sabot")
    print(f"✅ Created JobGraph: '{graph.job_name}'")
    print()

    # Step 2: Add operators
    print("Step 2: Add Operators")
    print("-"*70)

    # Source: Load data
    source = StreamOperatorNode(
        operator_type=OperatorType.SOURCE,
        name="load_numbers",
        parameters={'description': 'Generate numbers 1-100'}
    )
    graph.add_operator(source)
    print(f"✅ Added SOURCE operator: {source.name}")

    # Filter: Keep only even numbers
    filter_op = StreamOperatorNode(
        operator_type=OperatorType.FILTER,
        name="filter_even",
        parameters={
            'column': 'value',
            'operator': '==',
            'description': 'Keep only even numbers'
        }
    )
    graph.add_operator(filter_op)
    print(f"✅ Added FILTER operator: {filter_op.name}")

    # Sink: Output results
    sink = StreamOperatorNode(
        operator_type=OperatorType.SINK,
        name="output_results",
        parameters={'description': 'Print to console'}
    )
    graph.add_operator(sink)
    print(f"✅ Added SINK operator: {sink.name}")
    print()

    # Step 3: Connect operators (build pipeline)
    print("Step 3: Connect Operators")
    print("-"*70)

    graph.connect(source.operator_id, filter_op.operator_id)
    print(f"✅ Connected: {source.name} → {filter_op.name}")

    graph.connect(filter_op.operator_id, sink.operator_id)
    print(f"✅ Connected: {filter_op.name} → {sink.name}")
    print()

    # Step 4: Inspect the graph
    print("Step 4: Inspect JobGraph")
    print("-"*70)

    print(f"Total operators: {len(graph.operators)}")
    print(f"Sources: {len(graph.get_sources())}")
    print(f"Sinks: {len(graph.get_sinks())}")
    print()

    # Show pipeline structure
    print("Pipeline structure:")
    for i, op in enumerate(graph.topological_sort(), 1):
        upstream = " (no upstream)" if not op.upstream_ids else ""
        downstream = " (no downstream)" if not op.downstream_ids else ""
        print(f"  {i}. {op.name} ({op.operator_type.value}){upstream}{downstream}")
    print()

    # Step 5: Show next steps
    print("="*70)
    print("Success!")
    print("="*70)
    print()
    print("You've created your first Sabot pipeline:")
    print("  Source → Filter → Sink")
    print()
    print("Key concepts:")
    print("  • JobGraph = Logical plan (what to do)")
    print("  • Operators = Transformation steps")
    print("  • Connections = Data flow edges")
    print()
    print("Next steps:")
    print("  • filter_and_map.py - Add MAP transformations")
    print("  • local_join.py - Join two data sources")
    print("  • ../01_local_pipelines/ - Execute pipelines locally")
    print("  • ../03_distributed_basics/ - Distribute across agents")
    print()


if __name__ == '__main__':
    main()
