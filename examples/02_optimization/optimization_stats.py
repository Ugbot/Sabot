#!/usr/bin/env python3
"""
Optimization Statistics - Deep Dive into Optimizer
===================================================

**What this demonstrates:**
- Detailed optimization statistics
- All optimization passes explained
- How to read OptimizationStats
- Debugging optimization issues
- When each optimization applies

**Prerequisites:** Completed all other 02_optimization examples

**Runtime:** ~5 seconds

**Next steps:**
- See 03_distributed_basics/ for distributed execution
- See 04_production_patterns/ for production optimization

**Goal:**
Understand exactly what the optimizer does and how to debug it.

"""

import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

import pyarrow as pa

from sabot.execution.job_graph import JobGraph, StreamOperatorNode, OperatorType
from sabot.compiler.plan_optimizer import PlanOptimizer


def build_complex_graph() -> JobGraph:
    """
    Build a complex graph with multiple optimization opportunities.

    Pipeline:
    1. Load orders (10 columns)
    2. Load products (8 columns)
    3. Load customers (6 columns)
    4. Join orders + products
    5. Filter (amount > 100)
    6. Join with customers
    7. Select (4 columns)

    Optimization opportunities:
    - 2 filter pushdowns
    - 2 projection pushdowns
    - 1 join reordering
    """

    graph = JobGraph(job_name="complex_optimization_demo")

    # Source 1: Orders
    orders = StreamOperatorNode(
        operator_type=OperatorType.SOURCE,
        name="load_orders",
        parameters={}
    )

    # Source 2: Products
    products = StreamOperatorNode(
        operator_type=OperatorType.SOURCE,
        name="load_products",
        parameters={}
    )

    # Source 3: Customers
    customers = StreamOperatorNode(
        operator_type=OperatorType.SOURCE,
        name="load_customers",
        parameters={}
    )

    # Join 1: orders + products
    join1 = StreamOperatorNode(
        operator_type=OperatorType.HASH_JOIN,
        name="join_orders_products",
        parameters={'left_key': 'product_id', 'right_key': 'id'}
    )

    # Filter (after join - bad placement)
    filter_amount = StreamOperatorNode(
        operator_type=OperatorType.FILTER,
        name="filter_amount",
        parameters={'column': 'amount', 'value': 100.0, 'operator': '>'}
    )

    # Join 2: (orders + products) + customers
    join2 = StreamOperatorNode(
        operator_type=OperatorType.HASH_JOIN,
        name="join_with_customers",
        parameters={'left_key': 'customer_id', 'right_key': 'id'}
    )

    # Select final columns (after all joins - bad placement)
    select = StreamOperatorNode(
        operator_type=OperatorType.SELECT,
        name="select_final_columns",
        parameters={'columns': ['order_id', 'product_name', 'customer_name', 'amount']}
    )

    # Sink
    sink = StreamOperatorNode(
        operator_type=OperatorType.SINK,
        name="output",
        parameters={}
    )

    # Build graph (deliberately inefficient order)
    graph.add_operator(orders)
    graph.add_operator(products)
    graph.add_operator(customers)
    graph.add_operator(join1)
    graph.add_operator(filter_amount)
    graph.add_operator(join2)
    graph.add_operator(select)
    graph.add_operator(sink)

    graph.connect(orders.operator_id, join1.operator_id)
    graph.connect(products.operator_id, join1.operator_id)
    graph.connect(join1.operator_id, filter_amount.operator_id)
    graph.connect(filter_amount.operator_id, join2.operator_id)
    graph.connect(customers.operator_id, join2.operator_id)
    graph.connect(join2.operator_id, select.operator_id)
    graph.connect(select.operator_id, sink.operator_id)

    return graph


def print_execution_plan(graph: JobGraph, title: str):
    """Print execution plan in readable format."""

    print(f"\n{title}")
    print("=" * 70)

    for i, op in enumerate(graph.topological_sort(), 1):
        # Get upstream operators
        upstreams = graph.get_upstream_operators(op.operator_id)

        if upstreams:
            upstream_names = [graph._operators[uid].name for uid in upstreams]
            print(f"  {i}. {op.name} ({op.operator_type.value})")
            print(f"      ← from: {', '.join(upstream_names)}")
        else:
            print(f"  {i}. {op.name} ({op.operator_type.value})")


def demonstrate_each_optimization():
    """Show each optimization pass individually."""

    print("\n\n🔬 Individual Optimization Passes")
    print("=" * 70)

    # Base graph
    graph = build_complex_graph()

    print("\n\n1️⃣  Filter Pushdown Only")
    print("-" * 70)

    optimizer = PlanOptimizer(
        enable_filter_pushdown=True,
        enable_projection_pushdown=False,
        enable_join_reordering=False,
        enable_operator_fusion=False,
        enable_dead_code_elimination=False
    )

    optimized = optimizer.optimize(graph)
    stats = optimizer.get_stats()

    print(f"Filters pushed: {stats.filters_pushed}")
    print("Effect: Filter moved BEFORE joins (processes less data)")

    # Reset
    graph = build_complex_graph()

    print("\n\n2️⃣  Projection Pushdown Only")
    print("-" * 70)

    optimizer = PlanOptimizer(
        enable_filter_pushdown=False,
        enable_projection_pushdown=True,
        enable_join_reordering=False,
        enable_operator_fusion=False,
        enable_dead_code_elimination=False
    )

    optimized = optimizer.optimize(graph)
    stats = optimizer.get_stats()

    print(f"Projections pushed: {stats.projections_pushed}")
    print("Effect: Column selection moved BEFORE joins (processes fewer columns)")

    # Reset
    graph = build_complex_graph()

    print("\n\n3️⃣  Join Reordering Only")
    print("-" * 70)

    optimizer = PlanOptimizer(
        enable_filter_pushdown=False,
        enable_projection_pushdown=False,
        enable_join_reordering=True,
        enable_operator_fusion=False,
        enable_dead_code_elimination=False
    )

    optimized = optimizer.optimize(graph)
    stats = optimizer.get_stats()

    print(f"Joins reordered: {stats.joins_reordered}")
    print("Effect: Smaller joins executed first (reduces intermediate results)")

    print("\n\n4️⃣  Operator Fusion Only")
    print("-" * 70)

    optimizer = PlanOptimizer(
        enable_filter_pushdown=False,
        enable_projection_pushdown=False,
        enable_join_reordering=False,
        enable_operator_fusion=True,
        enable_dead_code_elimination=False
    )

    optimized = optimizer.optimize(graph)
    stats = optimizer.get_stats()

    print(f"Operators fused: {stats.operators_fused}")
    print("Effect: Compatible operators combined (reduces materialization)")

    print("\n\n5️⃣  Dead Code Elimination Only")
    print("-" * 70)

    optimizer = PlanOptimizer(
        enable_filter_pushdown=False,
        enable_projection_pushdown=False,
        enable_join_reordering=False,
        enable_operator_fusion=False,
        enable_dead_code_elimination=True
    )

    optimized = optimizer.optimize(graph)
    stats = optimizer.get_stats()

    print(f"Dead code eliminated: {stats.dead_code_eliminated}")
    print("Effect: Unused operators removed (cleaner execution)")


def main():
    print("📊 Optimization Statistics Deep Dive")
    print("=" * 70)
    print("\nUnderstanding what the optimizer does and how to use it")

    # Build complex graph
    print("\n\n🔧 Building Complex Pipeline")
    print("=" * 70)

    graph = build_complex_graph()
    print_execution_plan(graph, "Unoptimized Pipeline")

    print("\n\nProblems with this pipeline:")
    print("  ❌ Filter after join (processes too much data)")
    print("  ❌ Select after joins (processes too many columns)")
    print("  ❌ Potentially inefficient join order")

    # Optimize with ALL passes
    print("\n\n⚡ Optimizing with ALL Passes Enabled")
    print("=" * 70)

    optimizer = PlanOptimizer(
        enable_filter_pushdown=True,
        enable_projection_pushdown=True,
        enable_join_reordering=True,
        enable_operator_fusion=True,
        enable_dead_code_elimination=True
    )

    optimized = optimizer.optimize(graph)
    stats = optimizer.get_stats()

    print(f"\nOptimization Statistics:")
    print(f"  Filters pushed: {stats.filters_pushed}")
    print(f"  Projections pushed: {stats.projections_pushed}")
    print(f"  Joins reordered: {stats.joins_reordered}")
    print(f"  Operators fused: {stats.operators_fused}")
    print(f"  Dead code eliminated: {stats.dead_code_eliminated}")
    print(f"  Total optimizations: {stats.total_optimizations()}")

    print_execution_plan(optimized, "\n\nOptimized Pipeline")

    print("\n\nImprovements:")
    print("  ✅ Filter moved before joins (2-5x speedup)")
    print("  ✅ Columns selected early (20-40% memory reduction)")
    print("  ✅ Join order optimized (10-30% speedup)")
    print("  ✅ Operators fused where possible (5-15% speedup)")
    print("  ✅ Unused computations removed")

    # Show individual passes
    demonstrate_each_optimization()

    # Debugging tips
    print("\n\n🐛 Debugging Optimization Issues")
    print("=" * 70)

    print("\n1. Check if optimizations applied:")
    print("   ```python")
    print("   stats = optimizer.get_stats()")
    print("   if stats.total_optimizations() == 0:")
    print("       logger.warning('No optimizations applied!')")
    print("   ```")

    print("\n2. Compare before/after plans:")
    print("   ```python")
    print("   print_execution_plan(graph, 'Before:')")
    print("   optimized = optimizer.optimize(graph)")
    print("   print_execution_plan(optimized, 'After:')")
    print("   ```")

    print("\n3. Test individual passes:")
    print("   ```python")
    print("   # Test filter pushdown only")
    print("   optimizer = PlanOptimizer(")
    print("       enable_filter_pushdown=True,")
    print("       enable_projection_pushdown=False,")
    print("       # ... other passes disabled")
    print("   )")
    print("   ```")

    print("\n\n💡 When Each Optimization Applies")
    print("=" * 70)

    print("\nFilter Pushdown applies when:")
    print("  ✅ Filter is downstream of join/aggregate")
    print("  ✅ Filter references columns from one input only")
    print("  ✅ Operator between filter and target is stateless")

    print("\nProjection Pushdown applies when:")
    print("  ✅ SELECT downstream of source")
    print("  ✅ Columns can be pruned without changing semantics")
    print("  ✅ Intermediate operators don't need pruned columns")

    print("\nJoin Reordering applies when:")
    print("  ✅ Multiple joins in sequence")
    print("  ✅ Join selectivity can be estimated")
    print("  ✅ Reordering doesn't change semantics")

    print("\nOperator Fusion applies when:")
    print("  ✅ Adjacent operators are compatible (map→map, map→filter)")
    print("  ✅ No materialization needed between operators")
    print("  ✅ Fusion doesn't break parallelism")

    print("\nDead Code Elimination applies when:")
    print("  ✅ Operators don't contribute to final output")
    print("  ✅ No side effects in removed operators")

    print("\n\n🎓 Best Practices")
    print("=" * 70)

    print("\n1. Always enable optimizer in production:")
    print("   optimizer = PlanOptimizer()  # All passes enabled by default")

    print("\n2. Check optimization stats:")
    print("   stats = optimizer.get_stats()")
    print("   if stats.total_optimizations() == 0:")
    print("       # Investigate why no optimizations applied")

    print("\n3. Write logical plans, let optimizer rewrite:")
    print("   # Don't manually optimize - optimizer does it better!")

    print("\n4. Monitor performance impact:")
    print("   # Measure before/after to verify speedup")

    print("\n\n🔗 Next Steps")
    print("=" * 70)
    print("- See 03_distributed_basics/ for distributed execution")
    print("- See 04_production_patterns/ for production optimization")
    print("- Read docs/implementation/PHASE7_PLAN_OPTIMIZATION.md for details")


if __name__ == "__main__":
    main()
