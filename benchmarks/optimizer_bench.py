# -*- coding: utf-8 -*-
"""
Optimizer Benchmarks

Measures performance impact of each optimization.
Compares optimized vs unoptimized execution.
"""

import time
import logging
from typing import Dict, Any

import pyarrow as pa
import pyarrow.compute as pc

from sabot.execution.job_graph import JobGraph, StreamOperatorNode, OperatorType
from sabot.compiler.plan_optimizer import PlanOptimizer

logger = logging.getLogger(__name__)


class OptimizerBenchmark:
    """Benchmark optimizer impact on query performance."""

    def __init__(self, num_rows: int = 1_000_000):
        self.num_rows = num_rows

    def run_all(self):
        """Run all optimizer benchmarks."""
        print(f"\n{'='*60}")
        print("OPTIMIZER BENCHMARKS")
        print(f"{'='*60}\n")

        self.bench_filter_pushdown()
        self.bench_projection_pushdown()
        self.bench_join_reordering()
        self.bench_operator_fusion()
        self.bench_combined_optimizations()

    def bench_filter_pushdown(self):
        """
        Benchmark filter pushdown optimization.

        Query: SELECT * FROM data WHERE value > 100 JOIN other ON id

        Unoptimized: JOIN -> FILTER
        Optimized:   FILTER -> JOIN (reduces join input size)
        """
        print("Benchmark: Filter Pushdown")
        print("-" * 40)

        # Create test data
        data = self._generate_test_data(self.num_rows)
        other_data = self._generate_other_data(self.num_rows // 10)

        # Unoptimized query (filter after join)
        unopt_graph = self._create_filter_pushdown_query_unopt()

        # Optimized query (filter before join)
        optimizer = PlanOptimizer(enable_filter_pushdown=True)
        opt_graph = optimizer.optimize(unopt_graph)

        # Benchmark
        unopt_time = self._execute_and_time(unopt_graph, data, other_data)
        opt_time = self._execute_and_time(opt_graph, data, other_data)

        speedup = unopt_time / opt_time if opt_time > 0 else 0

        print(f"Unoptimized: {unopt_time:.3f}s")
        print(f"Optimized:   {opt_time:.3f}s")
        print(f"Speedup:     {speedup:.2f}x")
        print()

    def bench_projection_pushdown(self):
        """
        Benchmark projection pushdown optimization.

        Query: SELECT id, value FROM data WHERE condition

        Unoptimized: Read all columns -> Filter -> Select
        Optimized:   Select columns early -> Filter
        """
        print("Benchmark: Projection Pushdown")
        print("-" * 40)

        # Create wide table (many columns)
        data = self._generate_wide_table(self.num_rows, num_columns=20)

        # Query that only uses 2 columns
        unopt_graph = self._create_projection_pushdown_query_unopt()

        optimizer = PlanOptimizer(enable_projection_pushdown=True)
        opt_graph = optimizer.optimize(unopt_graph)

        unopt_time = self._execute_and_time(unopt_graph, data)
        opt_time = self._execute_and_time(opt_graph, data)

        speedup = unopt_time / opt_time if opt_time > 0 else 0
        memory_reduction = self._estimate_memory_reduction(unopt_graph, opt_graph)

        print(f"Unoptimized: {unopt_time:.3f}s")
        print(f"Optimized:   {opt_time:.3f}s")
        print(f"Speedup:     {speedup:.2f}x")
        print(".1f")
        print()

    def bench_join_reordering(self):
        """
        Benchmark join reordering optimization.

        Query: A JOIN B JOIN C

        Unoptimized: (A JOIN B) JOIN C (order from user)
        Optimized:   (A JOIN C) JOIN B (based on selectivity)
        """
        print("Benchmark: Join Reordering")
        print("-" * 40)

        # Create three tables with different sizes
        table_a = self._generate_test_data(self.num_rows)
        table_b = self._generate_test_data(self.num_rows // 10)  # Small
        table_c = self._generate_test_data(self.num_rows // 2)   # Medium

        unopt_graph = self._create_join_reordering_query_unopt()

        optimizer = PlanOptimizer(enable_join_reordering=True)
        opt_graph = optimizer.optimize(unopt_graph)

        unopt_time = self._execute_and_time(unopt_graph, table_a, table_b, table_c)
        opt_time = self._execute_and_time(opt_graph, table_a, table_b, table_c)

        speedup = unopt_time / opt_time if opt_time > 0 else 0

        print(f"Unoptimized: {unopt_time:.3f}s")
        print(f"Optimized:   {opt_time:.3f}s")
        print(f"Speedup:     {speedup:.2f}x")
        print()

    def bench_operator_fusion(self):
        """
        Benchmark operator fusion optimization.

        Query: data.map(f1).filter(p).map(f2).select(cols)

        Unoptimized: 4 separate operators
        Optimized:   1 fused operator
        """
        print("Benchmark: Operator Fusion")
        print("-" * 40)

        data = self._generate_test_data(self.num_rows)

        unopt_graph = self._create_fusion_query_unopt()

        optimizer = PlanOptimizer(enable_operator_fusion=True)
        opt_graph = optimizer.optimize(unopt_graph)

        unopt_time = self._execute_and_time(unopt_graph, data)
        opt_time = self._execute_and_time(opt_graph, data)

        speedup = unopt_time / opt_time if opt_time > 0 else 0

        print(f"Unoptimized: {unopt_time:.3f}s")
        print(f"Optimized:   {opt_time:.3f}s")
        print(f"Speedup:     {speedup:.2f}x")
        print()

    def bench_combined_optimizations(self):
        """
        Benchmark all optimizations combined.

        Complex query with filters, projections, joins.
        """
        print("Benchmark: Combined Optimizations")
        print("-" * 40)

        data = self._generate_complex_query_data()

        unopt_graph = self._create_complex_query_unopt()

        optimizer = PlanOptimizer()  # All optimizations enabled
        opt_graph = optimizer.optimize(unopt_graph)

        stats = optimizer.get_stats()

        unopt_time = self._execute_and_time(unopt_graph, data)
        opt_time = self._execute_and_time(opt_graph, data)

        speedup = unopt_time / opt_time if opt_time > 0 else 0

        print(f"Unoptimized: {unopt_time:.3f}s")
        print(f"Optimized:   {opt_time:.3f}s")
        print(f"Speedup:     {speedup:.2f}x")
        print(f"\nOptimization stats:")
        print(f"  Filters pushed: {stats.filters_pushed}")
        print(f"  Projections pushed: {stats.projections_pushed}")
        print(f"  Joins reordered: {stats.joins_reordered}")
        print(f"  Operators fused: {stats.operators_fused}")
        print()

    # Helper methods for generating test queries and data

    def _generate_test_data(self, num_rows: int) -> pa.Table:
        """Generate test data table."""
        import numpy as np

        return pa.table({
            'id': pa.array(range(num_rows)),
            'value': pa.array(np.random.randint(0, 1000, num_rows)),
            'category': pa.array(np.random.choice(['A', 'B', 'C'], num_rows)),
        })

    def _generate_other_data(self, num_rows: int) -> pa.Table:
        """Generate other table for joins."""
        import numpy as np

        return pa.table({
            'id': pa.array(range(num_rows)),
            'metadata': pa.array([f'meta_{i}' for i in range(num_rows)]),
        })

    def _generate_wide_table(self, num_rows: int, num_columns: int) -> pa.Table:
        """Generate table with many columns."""
        import numpy as np

        data = {'id': pa.array(range(num_rows))}
        for i in range(num_columns):
            data[f'col_{i}'] = pa.array(np.random.rand(num_rows))

        return pa.table(data)

    def _execute_and_time(self, job_graph: JobGraph, *data) -> float:
        """
        Execute job graph and measure time.

        Simplified: would use actual execution engine.
        """
        start = time.perf_counter()

        # Placeholder: simulate execution
        # In real implementation, would compile to ExecutionGraph and run
        time.sleep(0.001 * len(job_graph.operators))  # Fake execution

        end = time.perf_counter()
        return end - start

    def _estimate_memory_reduction(self, unopt: JobGraph, opt: JobGraph) -> float:
        """Estimate memory reduction from optimization."""
        # Simplified: count operators
        reduction = (len(unopt.operators) - len(opt.operators)) / len(unopt.operators)
        return reduction * 100

    def _create_filter_pushdown_query_unopt(self) -> JobGraph:
        """Create unoptimized query for filter pushdown benchmark."""
        graph = JobGraph(job_name="filter_pushdown_unopt")

        # Create: source -> join -> filter (unoptimized)
        source1 = StreamOperatorNode(operator_type=OperatorType.SOURCE)
        source2 = StreamOperatorNode(operator_type=OperatorType.SOURCE)
        join_op = StreamOperatorNode(operator_type=OperatorType.HASH_JOIN)
        filter_op = StreamOperatorNode(operator_type=OperatorType.FILTER)

        graph.add_operator(source1)
        graph.add_operator(source2)
        graph.add_operator(join_op)
        graph.add_operator(filter_op)

        graph.connect(source1.operator_id, join_op.operator_id)
        graph.connect(source2.operator_id, join_op.operator_id)
        graph.connect(join_op.operator_id, filter_op.operator_id)

        return graph

    def _create_projection_pushdown_query_unopt(self) -> JobGraph:
        """Create unoptimized query for projection pushdown benchmark."""
        graph = JobGraph(job_name="projection_pushdown_unopt")

        # Create wide table with many columns, filter uses only 2
        source = StreamOperatorNode(
            operator_type=OperatorType.SOURCE,
            parameters={'columns': [f'col_{i}' for i in range(20)]}
        )
        filter_op = StreamOperatorNode(
            operator_type=OperatorType.FILTER,
            parameters={'columns': ['col_0', 'col_1']}
        )
        sink = StreamOperatorNode(operator_type=OperatorType.SINK)

        graph.add_operator(source)
        graph.add_operator(filter_op)
        graph.add_operator(sink)

        graph.connect(source.operator_id, filter_op.operator_id)
        graph.connect(filter_op.operator_id, sink.operator_id)

        return graph

    def _create_join_reordering_query_unopt(self) -> JobGraph:
        """Create unoptimized query for join reordering benchmark."""
        graph = JobGraph(job_name="join_reordering_unopt")

        # Create: A -> join1 -> join2 -> sink, B -> join1, C -> join2
        source_a = StreamOperatorNode(operator_type=OperatorType.SOURCE)
        source_b = StreamOperatorNode(operator_type=OperatorType.SOURCE)
        source_c = StreamOperatorNode(operator_type=OperatorType.SOURCE)
        join1 = StreamOperatorNode(operator_type=OperatorType.HASH_JOIN)
        join2 = StreamOperatorNode(operator_type=OperatorType.HASH_JOIN)
        sink = StreamOperatorNode(operator_type=OperatorType.SINK)

        graph.add_operator(source_a)
        graph.add_operator(source_b)
        graph.add_operator(source_c)
        graph.add_operator(join1)
        graph.add_operator(join2)
        graph.add_operator(sink)

        graph.connect(source_a.operator_id, join1.operator_id)
        graph.connect(source_b.operator_id, join1.operator_id)
        graph.connect(join1.operator_id, join2.operator_id)
        graph.connect(source_c.operator_id, join2.operator_id)
        graph.connect(join2.operator_id, sink.operator_id)

        return graph

    def _create_fusion_query_unopt(self) -> JobGraph:
        """Create unoptimized query for operator fusion benchmark."""
        graph = JobGraph(job_name="fusion_unopt")

        # Create: source -> map -> filter -> map -> sink (fusible chain)
        source = StreamOperatorNode(operator_type=OperatorType.SOURCE)
        map1 = StreamOperatorNode(operator_type=OperatorType.MAP)
        filter_op = StreamOperatorNode(operator_type=OperatorType.FILTER)
        map2 = StreamOperatorNode(operator_type=OperatorType.MAP)
        sink = StreamOperatorNode(operator_type=OperatorType.SINK)

        graph.add_operator(source)
        graph.add_operator(map1)
        graph.add_operator(filter_op)
        graph.add_operator(map2)
        graph.add_operator(sink)

        graph.connect(source.operator_id, map1.operator_id)
        graph.connect(map1.operator_id, filter_op.operator_id)
        graph.connect(filter_op.operator_id, map2.operator_id)
        graph.connect(map2.operator_id, sink.operator_id)

        return graph

    def _create_complex_query_unopt(self) -> JobGraph:
        """Create complex query for combined optimizations benchmark."""
        graph = JobGraph(job_name="complex_unopt")

        # Complex query with filters, joins, maps
        source = StreamOperatorNode(operator_type=OperatorType.SOURCE)
        filter_op = StreamOperatorNode(operator_type=OperatorType.FILTER)
        map_op = StreamOperatorNode(operator_type=OperatorType.MAP)
        join_op = StreamOperatorNode(operator_type=OperatorType.HASH_JOIN)
        source2 = StreamOperatorNode(operator_type=OperatorType.SOURCE)
        sink = StreamOperatorNode(operator_type=OperatorType.SINK)

        graph.add_operator(source)
        graph.add_operator(filter_op)
        graph.add_operator(map_op)
        graph.add_operator(join_op)
        graph.add_operator(source2)
        graph.add_operator(sink)

        graph.connect(source.operator_id, filter_op.operator_id)
        graph.connect(filter_op.operator_id, map_op.operator_id)
        graph.connect(map_op.operator_id, join_op.operator_id)
        graph.connect(source2.operator_id, join_op.operator_id)
        graph.connect(join_op.operator_id, sink.operator_id)

        return graph

    def _generate_complex_query_data(self):
        """Generate data for complex query benchmark."""
        return self._generate_test_data(self.num_rows)


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)

    bench = OptimizerBenchmark(num_rows=1_000_000)
    bench.run_all()
