#!/usr/bin/env python3
"""
Integration test for PlanOptimizer with JobManager.

Tests that the optimizer integrates properly with the job submission workflow.
"""

import json
import logging
from sabot.execution.job_graph import JobGraph, StreamOperatorNode, OperatorType
from sabot.compiler.plan_optimizer import PlanOptimizer

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def test_optimizer_integration():
    """Test that optimizer works with JobGraph serialization."""

    # Create a simple job graph
    graph = JobGraph(job_name="optimizer_test")

    # Create operators: source -> filter -> map -> sink
    source = StreamOperatorNode(operator_type=OperatorType.SOURCE)
    filter_op = StreamOperatorNode(
        operator_type=OperatorType.FILTER,
        parameters={'columns': ['value']}
    )
    map_op = StreamOperatorNode(operator_type=OperatorType.MAP)
    sink = StreamOperatorNode(operator_type=OperatorType.SINK)

    graph.add_operator(source)
    graph.add_operator(filter_op)
    graph.add_operator(map_op)
    graph.add_operator(sink)

    graph.connect(source.operator_id, filter_op.operator_id)
    graph.connect(filter_op.operator_id, map_op.operator_id)
    graph.connect(map_op.operator_id, sink.operator_id)

    print(f"Original graph: {len(graph.operators)} operators")

    # Serialize to JSON (like JobManager does)
    original_json = json.dumps(graph.to_dict())
    print(f"Serialized size: {len(original_json)} chars")

    # Deserialize (like JobManager does)
    restored_graph = JobGraph.from_dict(json.loads(original_json))
    print(f"Restored graph: {len(restored_graph.operators)} operators")

    # Apply optimization (like JobManager does)
    optimizer = PlanOptimizer()
    optimized_graph = optimizer.optimize(restored_graph)

    print(f"Optimized graph: {len(optimized_graph.operators)} operators")
    print(f"Optimization stats: {optimizer.get_stats()}")

    # Serialize optimized graph
    optimized_json = json.dumps(optimized_graph.to_dict())
    print(f"Optimized serialized size: {len(optimized_json)} chars")

    print("✓ Optimizer integration test passed!")


def test_dead_code_elimination():
    """Test dead code elimination specifically."""

    graph = JobGraph(job_name="dead_code_test")

    # Create connected pipeline: source -> map -> sink
    source = StreamOperatorNode(operator_type=OperatorType.SOURCE)
    map_op = StreamOperatorNode(operator_type=OperatorType.MAP)
    sink = StreamOperatorNode(operator_type=OperatorType.SINK)

    # Create dead code: unconnected operators
    dead_source = StreamOperatorNode(operator_type=OperatorType.SOURCE, name="dead_source")
    dead_map = StreamOperatorNode(operator_type=OperatorType.MAP, name="dead_map")

    graph.add_operator(source)
    graph.add_operator(map_op)
    graph.add_operator(sink)
    graph.add_operator(dead_source)
    graph.add_operator(dead_map)

    # Connect main pipeline
    graph.connect(source.operator_id, map_op.operator_id)
    graph.connect(map_op.operator_id, sink.operator_id)
    # Dead operators remain unconnected

    print(f"Before optimization: {len(graph.operators)} operators")

    optimizer = PlanOptimizer()
    optimized = optimizer.optimize(graph)

    print(f"After optimization: {len(optimized.operators)} operators")
    print(f"Dead code eliminated: {optimizer.stats.dead_code_eliminated}")

    assert len(optimized.operators) == 3, "Should eliminate 2 dead operators"
    assert optimizer.stats.dead_code_eliminated == 2, "Should count eliminated operators"

    print("✓ Dead code elimination test passed!")


if __name__ == '__main__':
    test_optimizer_integration()
    test_dead_code_elimination()
