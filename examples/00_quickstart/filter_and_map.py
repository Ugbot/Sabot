#!/usr/bin/env python3
"""
Filter and Map - Basic Operators with Local Execution
=======================================================

**What this demonstrates:**
- FILTER operator (remove rows)
- MAP operator (transform data)
- SELECT operator (project columns)
- Local execution with real data

**Prerequisites:**
- None! Runs completely locally with generated data

**Runtime:** ~2 seconds

**Next steps:**
- local_join.py - Join two data sources
- ../01_local_pipelines/batch_processing.py - Full batch pipeline

Run:
    python examples/00_quickstart/filter_and_map.py
"""

import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

import pyarrow as pa
import pyarrow.compute as pc
from sabot.execution.job_graph import JobGraph, StreamOperatorNode, OperatorType


class LocalExecutor:
    """
    Simple local executor (no agents, no distribution).

    This is a simplified executor for demonstration purposes.
    Production uses JobManager + Agents for distributed execution.
    """

    def execute_pipeline(self, graph: JobGraph, input_data: dict):
        """Execute JobGraph locally with input data."""
        print("Executing pipeline locally...")
        print()

        results = {}

        # Execute in topological order
        for op in graph.topological_sort():
            if op.operator_type == OperatorType.SOURCE:
                # Load data
                table_name = op.parameters.get('table_name')
                data = input_data.get(table_name)
                results[op.operator_id] = data
                print(f"[{op.name}] Loaded {data.num_rows:,} rows, {data.num_columns} columns")

            elif op.operator_type == OperatorType.FILTER:
                # Get input from upstream
                upstream_id = op.upstream_ids[0]
                data = results[upstream_id]

                # Apply filter
                column = op.parameters['column']
                value = op.parameters['value']
                operator = op.parameters.get('operator', '>')

                if operator == '>':
                    mask = pc.greater(data[column], value)
                elif operator == '<':
                    mask = pc.less(data[column], value)
                elif operator == '>=':
                    mask = pc.greater_equal(data[column], value)
                elif operator == '==':
                    mask = pc.equal(data[column], value)

                filtered = data.filter(mask)
                results[op.operator_id] = filtered

                selectivity = (filtered.num_rows / data.num_rows * 100) if data.num_rows > 0 else 0
                print(f"[{op.name}] Filtered: {data.num_rows:,} → {filtered.num_rows:,} rows ({selectivity:.1f}% retained)")

            elif op.operator_type == OperatorType.MAP:
                # Get input from upstream
                upstream_id = op.upstream_ids[0]
                data = results[upstream_id]

                # Apply transformation (simplified - would use UDF in production)
                transform = op.parameters.get('transform')
                if transform == 'double_amount':
                    new_amount = pc.multiply(data['amount'], 2.0)
                    data = data.append_column('amount_doubled', new_amount)
                elif transform == 'add_tax':
                    tax_amount = pc.multiply(data['amount'], 0.1)  # 10% tax
                    data = data.append_column('tax', tax_amount)

                results[op.operator_id] = data
                print(f"[{op.name}] Transformed: {data.num_rows:,} rows, added columns")

            elif op.operator_type == OperatorType.SELECT:
                # Get input from upstream
                upstream_id = op.upstream_ids[0]
                data = results[upstream_id]

                # Project columns
                columns = op.parameters['columns']
                selected = data.select(columns)
                results[op.operator_id] = selected

                print(f"[{op.name}] Selected {len(columns)} columns: {', '.join(columns)}")

            elif op.operator_type == OperatorType.SINK:
                # Get final result
                upstream_id = op.upstream_ids[0]
                data = results[upstream_id]
                results[op.operator_id] = data
                print(f"[{op.name}] Output: {data.num_rows:,} rows")

        # Return final result
        sinks = graph.get_sinks()
        if sinks:
            return results[sinks[0].operator_id]
        return None


def main():
    print("="*70)
    print("Filter and Map - Basic Operators")
    print("="*70)
    print()

    # Step 1: Generate test data
    print("Step 1: Generate Test Data")
    print("-"*70)

    data = pa.table({
        'id': range(1000),
        'amount': [100.0 + (i % 500) for i in range(1000)],
        'category': ['A' if i % 2 == 0 else 'B' for i in range(1000)],
    })

    print(f"✅ Generated 1,000 rows with columns: {', '.join(data.column_names)}")
    print(f"   Amount range: ${min(data['amount'].to_pylist()):.2f} - ${max(data['amount'].to_pylist()):.2f}")
    print()

    # Step 2: Build JobGraph
    print("Step 2: Build JobGraph")
    print("-"*70)

    graph = JobGraph(job_name="filter_and_map")

    # Source
    source = StreamOperatorNode(
        operator_type=OperatorType.SOURCE,
        name="load_transactions",
        parameters={'table_name': 'transactions'}
    )
    graph.add_operator(source)

    # Filter: amount > 300
    filter_op = StreamOperatorNode(
        operator_type=OperatorType.FILTER,
        name="filter_high_value",
        parameters={
            'column': 'amount',
            'value': 300.0,
            'operator': '>'
        }
    )
    graph.add_operator(filter_op)

    # Map: Add tax column
    map_op = StreamOperatorNode(
        operator_type=OperatorType.MAP,
        name="calculate_tax",
        parameters={'transform': 'add_tax'}
    )
    graph.add_operator(map_op)

    # Select: Project specific columns
    select_op = StreamOperatorNode(
        operator_type=OperatorType.SELECT,
        name="select_columns",
        parameters={'columns': ['id', 'amount', 'tax', 'category']}
    )
    graph.add_operator(select_op)

    # Sink
    sink = StreamOperatorNode(
        operator_type=OperatorType.SINK,
        name="output_results",
        parameters={}
    )
    graph.add_operator(sink)

    # Connect pipeline
    graph.connect(source.operator_id, filter_op.operator_id)
    graph.connect(filter_op.operator_id, map_op.operator_id)
    graph.connect(map_op.operator_id, select_op.operator_id)
    graph.connect(select_op.operator_id, sink.operator_id)

    print(f"✅ Built pipeline with {len(graph.operators)} operators:")
    print(f"   Source → Filter → Map → Select → Sink")
    print()

    # Step 3: Execute locally
    print("Step 3: Execute Pipeline Locally")
    print("-"*70)

    executor = LocalExecutor()
    result = executor.execute_pipeline(graph, {'transactions': data})
    print()

    # Step 4: Show results
    print("Step 4: Results")
    print("-"*70)

    print(f"Final output: {result.num_rows} rows, {result.num_columns} columns")
    print()
    print("Sample output (first 5 rows):")
    print(result.to_pandas().head(5))
    print()

    # Step 5: Summary
    print("="*70)
    print("Success!")
    print("="*70)
    print()
    print("Pipeline executed:")
    print("  1. Loaded 1,000 transactions")
    print("  2. Filtered to high-value (amount > 300)")
    print("  3. Calculated 10% tax")
    print("  4. Selected 4 columns")
    print(f"  5. Output {result.num_rows} rows")
    print()
    print("Key concepts:")
    print("  • FILTER - Remove rows based on condition")
    print("  • MAP - Transform data (add columns, calculate)")
    print("  • SELECT - Project specific columns")
    print("  • Local execution - Run without distributed agents")
    print()
    print("Next steps:")
    print("  • local_join.py - Join two data sources")
    print("  • ../02_optimization/ - See filter pushdown optimization")
    print("  • ../03_distributed_basics/ - Distribute across agents")


if __name__ == '__main__':
    main()
