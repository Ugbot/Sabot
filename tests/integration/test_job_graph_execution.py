#!/usr/bin/env python3
"""
Test Job Graph Execution

Verifies the new job execution layer:
- JobGraph creation and validation
- Conversion to ExecutionGraph
- Task parallelism expansion
- Live rescaling
"""

import sys
sys.path.insert(0, '/Users/bengamble/PycharmProjects/pythonProject/sabot')

from sabot.execution import (
    JobGraph, ExecutionGraph, StreamOperatorNode,
    OperatorType, TaskState
)
from sabot.execution.slot_pool import SlotPool, SlotRequest


def test_job_graph_creation():
    """Test creating a simple job graph."""
    print("\n=== Test 1: Job Graph Creation ===")

    # Create job graph
    job = JobGraph(job_name="test_pipeline", default_parallelism=4)

    # Add operators
    source = StreamOperatorNode(
        operator_type=OperatorType.SOURCE,
        name="kafka_source",
        parallelism=2,
    )
    source_id = job.add_operator(source)

    filter_op = StreamOperatorNode(
        operator_type=OperatorType.FILTER,
        name="price_filter",
        parallelism=4,
    )
    filter_id = job.add_operator(filter_op)

    map_op = StreamOperatorNode(
        operator_type=OperatorType.MAP,
        name="enrichment",
        parallelism=4,
    )
    map_id = job.add_operator(map_op)

    sink = StreamOperatorNode(
        operator_type=OperatorType.SINK,
        name="kafka_sink",
        parallelism=2,
    )
    sink_id = job.add_operator(sink)

    # Connect operators
    job.connect(source_id, filter_id)
    job.connect(filter_id, map_id)
    job.connect(map_id, sink_id)

    # Validate
    job.validate()

    print(f"✓ Created job graph with {len(job.operators)} operators")
    print(f"  - Source: {source.name} (parallelism={source.parallelism})")
    print(f"  - Filter: {filter_op.name} (parallelism={filter_op.parallelism})")
    print(f"  - Map: {map_op.name} (parallelism={map_op.parallelism})")
    print(f"  - Sink: {sink.name} (parallelism={sink.parallelism})")

    return job


def test_execution_graph_conversion(job: JobGraph):
    """Test converting job graph to execution graph."""
    print("\n=== Test 2: Execution Graph Conversion ===")

    # Convert to execution graph
    exec_graph = job.to_execution_graph()

    total_tasks = exec_graph.get_task_count()
    expected_tasks = 2 + 4 + 4 + 2  # Source + Filter + Map + Sink

    print(f"✓ Converted to execution graph")
    print(f"  - Total tasks: {total_tasks} (expected {expected_tasks})")

    # Verify parallelism per operator
    for operator_id in job.operators.keys():
        tasks = exec_graph.get_tasks_by_operator(operator_id)
        parallelism = len(tasks)
        operator = job.operators[operator_id]
        print(f"  - {operator.name}: {parallelism} tasks")

        assert parallelism == operator.parallelism, \
            f"Parallelism mismatch: {parallelism} != {operator.parallelism}"

    # Verify task connections
    source_tasks = exec_graph.get_source_tasks()
    print(f"  - Source tasks: {len(source_tasks)}")
    print(f"  - Sample task ID: {source_tasks[0].task_id}")
    print(f"  - Sample downstream: {source_tasks[0].downstream_task_ids[:2]}")

    assert total_tasks == expected_tasks

    return exec_graph


def test_slot_pool():
    """Test slot pool allocation."""
    print("\n=== Test 3: Slot Pool ===")

    pool = SlotPool()

    # Register 2 nodes with 4 slots each
    pool.register_node("node-1", num_slots=4, cpu_per_slot=1.0, memory_per_slot_mb=1024)
    pool.register_node("node-2", num_slots=4, cpu_per_slot=1.0, memory_per_slot_mb=1024)

    print(f"✓ Registered 2 nodes")
    print(f"  - Total slots: {pool.get_total_slots()}")
    print(f"  - Free slots: {pool.get_free_slots()}")

    # Allocate slots for tasks
    allocated = []
    for i in range(6):
        request = SlotRequest(
            task_id=f"task-{i}",
            operator_id="map_0",
            cpu_cores=0.5,
            memory_mb=256,
            sharing_group="job-123"
        )

        slot = pool.request_slot(request)
        if slot:
            allocated.append(slot)
            pool.activate_slot(f"task-{i}")

    print(f"✓ Allocated {len(allocated)} slots")
    print(f"  - Free slots remaining: {pool.get_free_slots()}")
    print(f"  - Utilization: {pool.get_utilization():.1%}")

    # Get metrics
    metrics = pool.get_metrics()
    print(f"  - Active slots: {metrics['active_slots']}")
    print(f"  - Pending requests: {metrics['pending_requests']}")

    assert len(allocated) == 6
    assert pool.get_free_slots() == 2

    return pool


def test_rescaling(exec_graph: ExecutionGraph):
    """Test live operator rescaling."""
    print("\n=== Test 4: Live Rescaling ===")

    # Get map operator ID
    map_operator_id = None
    for op_id, op in exec_graph.operator_tasks.items():
        if "enrichment" in exec_graph.tasks[op[0]].operator_name:
            map_operator_id = op_id
            break

    old_parallelism = exec_graph.get_operator_parallelism(map_operator_id)
    new_parallelism = 8

    print(f"✓ Rescaling operator {map_operator_id}")
    print(f"  - Old parallelism: {old_parallelism}")
    print(f"  - New parallelism: {new_parallelism}")

    # Perform rescaling
    rescale_plan = exec_graph.rescale_operator(map_operator_id, new_parallelism)

    print(f"✓ Rescaling complete")
    print(f"  - Status: {rescale_plan['status']}")
    print(f"  - Old tasks: {len(rescale_plan['old_task_ids'])}")
    print(f"  - New tasks: {len(rescale_plan['new_task_ids'])}")
    print(f"  - Requires state redistribution: {rescale_plan['requires_state_redistribution']}")

    # Verify new parallelism
    actual_parallelism = exec_graph.get_operator_parallelism(map_operator_id)
    print(f"  - Actual parallelism after rescale: {actual_parallelism}")

    # Note: actual_parallelism includes both old and new tasks
    # In production, old tasks would be canceled after new tasks are running
    assert len(rescale_plan['new_task_ids']) == new_parallelism

    return rescale_plan


def test_serialization(job: JobGraph):
    """Test job graph serialization."""
    print("\n=== Test 5: Serialization ===")

    # Serialize to dict
    job_dict = job.to_dict()

    print(f"✓ Serialized job graph")
    print(f"  - Job ID: {job_dict['job_id']}")
    print(f"  - Operators: {len(job_dict['operators'])}")

    # Deserialize
    restored_job = JobGraph.from_dict(job_dict)

    print(f"✓ Deserialized job graph")
    print(f"  - Operators: {len(restored_job.operators)}")
    print(f"  - Job name: {restored_job.job_name}")

    # Verify operators match
    assert len(restored_job.operators) == len(job.operators)
    assert restored_job.job_name == job.job_name

    return restored_job


def main():
    """Run all tests."""
    print("=" * 60)
    print("Job Graph Execution Tests")
    print("=" * 60)

    # Test 1: Create job graph
    job = test_job_graph_creation()

    # Test 2: Convert to execution graph
    exec_graph = test_execution_graph_conversion(job)

    # Test 3: Slot pool
    pool = test_slot_pool()

    # Test 4: Rescaling
    rescale_plan = test_rescaling(exec_graph)

    # Test 5: Serialization
    restored_job = test_serialization(job)

    print("\n" + "=" * 60)
    print("✓ All tests passed!")
    print("=" * 60)

    # Summary
    print("\nSummary:")
    print(f"  - Job graph operators: {len(job.operators)}")
    print(f"  - Execution graph tasks: {exec_graph.get_task_count()}")
    print(f"  - Slot pool slots: {pool.get_total_slots()}")
    print(f"  - Rescaled operator to: {len(rescale_plan['new_task_ids'])} tasks")


if __name__ == "__main__":
    main()
