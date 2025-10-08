#!/usr/bin/env python3
"""
Batch Processing Example - Parquet to Parquet Pipeline
=======================================================

**What this demonstrates:**
- Batch processing (not streaming)
- Reading from Parquet files
- Applying transformations (filter, map, select)
- Writing results to Parquet
- Local execution (no distributed agents)

**Prerequisites:** Completed 00_quickstart

**Runtime:** ~5 seconds

**Next steps:**
- Try streaming_simulation.py for simulated streaming
- See 02_optimization/ for automatic optimization

**Pattern:**
Read Parquet → Filter → Map → Select → Write Parquet

"""

import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.compute as pc
import tempfile
from pathlib import Path

from sabot.execution.job_graph import JobGraph, StreamOperatorNode, OperatorType


def create_sample_data():
    """Create sample transaction data."""
    data = {
        'transaction_id': list(range(1, 1001)),
        'amount': [100.0 + i * 0.5 for i in range(1000)],
        'category': ['A', 'B', 'C'] * 333 + ['A'],
        'timestamp': [f'2025-10-08T{i % 24:02d}:00:00' for i in range(1000)]
    }
    return pa.table(data)


class BatchProcessor:
    """Simple batch processor (executes pipeline locally)."""

    def __init__(self, input_path: str, output_path: str):
        self.input_path = input_path
        self.output_path = output_path

    def execute_pipeline(self, graph: JobGraph):
        """Execute pipeline locally."""

        print("📊 Batch Processing Pipeline")
        print("=" * 50)

        # Read input
        print(f"\n1. Reading from: {self.input_path}")
        input_table = pq.read_table(self.input_path)
        print(f"   Loaded {input_table.num_rows:,} rows, {len(input_table.schema)} columns")

        # Process operators in topological order
        results = {}
        current_data = input_table

        for op in graph.topological_sort():
            if op.operator_type == OperatorType.SOURCE:
                results[op.operator_id] = current_data

            elif op.operator_type == OperatorType.FILTER:
                # Get upstream data
                upstream_id = graph.get_upstream_operators(op.operator_id)[0]
                data = results[upstream_id]

                # Apply filter
                column = op.parameters['column']
                value = op.parameters['value']
                operator = op.parameters['operator']

                if operator == '>':
                    mask = pc.greater(data[column], value)
                elif operator == '==':
                    mask = pc.equal(data[column], value)
                else:
                    raise ValueError(f"Unsupported operator: {operator}")

                filtered = data.filter(mask)
                results[op.operator_id] = filtered

                print(f"\n2. {op.name}: {data.num_rows:,} → {filtered.num_rows:,} rows")
                print(f"   Filter: {column} {operator} {value}")

            elif op.operator_type == OperatorType.MAP:
                # Get upstream data
                upstream_id = graph.get_upstream_operators(op.operator_id)[0]
                data = results[upstream_id]

                # Apply map transformation
                operation = op.parameters['operation']

                if operation == 'add_tax':
                    # Calculate 10% tax
                    tax = pc.multiply(data['amount'], 0.10)
                    data = data.append_column('tax', tax)

                results[op.operator_id] = data
                print(f"\n3. {op.name}: Added 'tax' column")

            elif op.operator_type == OperatorType.SELECT:
                # Get upstream data
                upstream_id = graph.get_upstream_operators(op.operator_id)[0]
                data = results[upstream_id]

                # Select columns
                columns = op.parameters['columns']
                selected = data.select(columns)
                results[op.operator_id] = selected

                print(f"\n4. {op.name}: Selected {len(columns)} columns")
                print(f"   Columns: {columns}")

            elif op.operator_type == OperatorType.SINK:
                # Get upstream data
                upstream_id = graph.get_upstream_operators(op.operator_id)[0]
                data = results[upstream_id]

                # Write to output
                pq.write_table(data, self.output_path)
                results[op.operator_id] = data

                print(f"\n5. {op.name}: Wrote {data.num_rows:,} rows to {self.output_path}")

        # Final result
        final_op_id = graph.topological_sort()[-1].operator_id
        final_result = results[final_op_id]

        print("\n" + "=" * 50)
        print("✅ Pipeline completed successfully!")
        print(f"\nFinal output: {final_result.num_rows:,} rows, {len(final_result.schema)} columns")
        print(f"Output schema: {final_result.schema}")

        return final_result


def main():
    # Create temporary directory for demo
    temp_dir = tempfile.mkdtemp()
    input_path = Path(temp_dir) / "input.parquet"
    output_path = Path(temp_dir) / "output.parquet"

    print("🔧 Setup")
    print("=" * 50)
    print(f"Input file: {input_path}")
    print(f"Output file: {output_path}")

    # Create sample data
    sample_data = create_sample_data()
    pq.write_table(sample_data, input_path)
    print(f"\n✅ Created sample data: {sample_data.num_rows:,} rows")

    # Build JobGraph
    print("\n\n📋 Building JobGraph")
    print("=" * 50)

    graph = JobGraph(job_name="batch_processing")

    # Source
    source = StreamOperatorNode(
        operator_type=OperatorType.SOURCE,
        name="read_transactions",
        parameters={'path': str(input_path)}
    )

    # Filter: amount > 500
    filter_op = StreamOperatorNode(
        operator_type=OperatorType.FILTER,
        name="filter_high_value",
        parameters={'column': 'amount', 'value': 500.0, 'operator': '>'}
    )

    # Map: add tax column
    map_op = StreamOperatorNode(
        operator_type=OperatorType.MAP,
        name="add_tax",
        parameters={'operation': 'add_tax'}
    )

    # Select: choose columns
    select_op = StreamOperatorNode(
        operator_type=OperatorType.SELECT,
        name="select_columns",
        parameters={'columns': ['transaction_id', 'amount', 'tax', 'category']}
    )

    # Sink
    sink = StreamOperatorNode(
        operator_type=OperatorType.SINK,
        name="write_results",
        parameters={'path': str(output_path)}
    )

    # Build graph
    graph.add_operator(source)
    graph.add_operator(filter_op)
    graph.add_operator(map_op)
    graph.add_operator(select_op)
    graph.add_operator(sink)

    graph.connect(source.operator_id, filter_op.operator_id)
    graph.connect(filter_op.operator_id, map_op.operator_id)
    graph.connect(map_op.operator_id, select_op.operator_id)
    graph.connect(select_op.operator_id, sink.operator_id)

    print("✅ JobGraph created with 5 operators:")
    for i, op in enumerate(graph.topological_sort(), 1):
        print(f"   {i}. {op.name} ({op.operator_type.value})")

    # Execute pipeline
    print("\n")
    processor = BatchProcessor(str(input_path), str(output_path))
    result = processor.execute_pipeline(graph)

    # Show sample output
    print("\n\n📄 Sample Output (first 5 rows):")
    print("=" * 50)
    print(result.slice(0, 5).to_pandas().to_string(index=False))

    # Cleanup
    print("\n\n🧹 Cleanup")
    print("=" * 50)
    print(f"Files created in: {temp_dir}")
    print("(Files will be cleaned up automatically)")


if __name__ == "__main__":
    main()
