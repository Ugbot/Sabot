#!/usr/bin/env python3
"""
Standalone test for distributed SQL execution.
Tests the full flow without pytest dependencies.
"""

import asyncio
import sys
import traceback

# Setup path
sys.path.insert(0, '/Users/bengamble/Sabot')

def test_distributed_plan_creation():
    """Test creating a DistributedQueryPlan from dict."""
    print("=" * 60)
    print("Test: DistributedQueryPlan creation from dict")
    print("=" * 60)

    from sabot.sql.distributed_plan import (
        DistributedQueryPlan,
        ExecutionStage,
        OperatorSpec,
    )

    # Create a simple filter plan
    plan_dict = {
        'sql': 'SELECT * FROM orders WHERE amount > 100',
        'stages': [
            {
                'stage_id': 'stage_0',
                'operators': [
                    {
                        'type': 'TableScan',
                        'name': 'Scan orders',
                        'table_name': 'orders',
                        'projected_columns': ['id', 'customer_id', 'amount', 'status'],
                        'estimated_cardinality': 10
                    },
                    {
                        'type': 'Filter',
                        'name': 'Filter amount > 100',
                        'filter_expression': 'amount > 100',
                        'estimated_cardinality': 5
                    }
                ],
                'input_shuffle_ids': [],
                'parallelism': 2,
                'is_source': True,
                'is_sink': True,
                'estimated_rows': 5
            }
        ],
        'shuffles': {},
        'execution_waves': [['stage_0']],
        'output_column_names': ['id', 'customer_id', 'amount', 'status'],
        'output_column_types': ['int64', 'int64', 'double', 'string'],
        'requires_shuffle': False,
        'total_parallelism': 2,
        'estimated_total_rows': 5
    }

    plan = DistributedQueryPlan.from_dict(plan_dict)

    # Assertions
    assert plan.sql == 'SELECT * FROM orders WHERE amount > 100', "SQL mismatch"
    assert len(plan.stages) == 1, f"Expected 1 stage, got {len(plan.stages)}"
    assert plan.stages[0].stage_id == 'stage_0', "Stage ID mismatch"
    assert plan.stages[0].is_source, "Expected source stage"
    assert plan.stages[0].is_sink, "Expected sink stage"
    assert len(plan.stages[0].operators) == 2, f"Expected 2 operators, got {len(plan.stages[0].operators)}"

    print(f"  ✓ Created plan with {len(plan.stages)} stages")
    print(f"  ✓ Stage 0 has {len(plan.stages[0].operators)} operators")

    # Print explain string
    explain = plan.to_explain_string()
    print(f"\n{explain}")

    print("\n✓ PASSED: DistributedQueryPlan creation\n")
    return True


def test_aggregate_plan():
    """Test creating an aggregate plan with shuffle."""
    print("=" * 60)
    print("Test: Aggregate plan with shuffle")
    print("=" * 60)

    from sabot.sql.distributed_plan import DistributedQueryPlan

    plan_dict = {
        'sql': 'SELECT customer_id, SUM(amount) FROM orders GROUP BY customer_id',
        'stages': [
            {
                'stage_id': 'stage_0',
                'operators': [
                    {
                        'type': 'TableScan',
                        'name': 'Scan orders',
                        'table_name': 'orders',
                        'projected_columns': ['customer_id', 'amount'],
                        'estimated_cardinality': 10
                    }
                ],
                'input_shuffle_ids': [],
                'output_shuffle_id': 'shuffle_0',
                'parallelism': 2,
                'is_source': True,
                'is_sink': False,
                'estimated_rows': 10
            },
            {
                'stage_id': 'stage_1',
                'operators': [
                    {
                        'type': 'Aggregate',
                        'name': 'GroupBy customer_id',
                        'group_by_keys': ['customer_id'],
                        'aggregate_functions': ['SUM'],
                        'aggregate_columns': ['amount'],
                        'estimated_cardinality': 5
                    }
                ],
                'input_shuffle_ids': ['shuffle_0'],
                'parallelism': 2,
                'is_source': False,
                'is_sink': True,
                'estimated_rows': 5,
                'dependency_stage_ids': ['stage_0']
            }
        ],
        'shuffles': {
            'shuffle_0': {
                'shuffle_id': 'shuffle_0',
                'type': 'HASH',
                'partition_keys': ['customer_id'],
                'num_partitions': 2,
                'producer_stage_id': 'stage_0',
                'consumer_stage_id': 'stage_1',
                'column_names': ['customer_id', 'amount'],
                'column_types': ['int64', 'double']
            }
        },
        'execution_waves': [['stage_0'], ['stage_1']],
        'output_column_names': ['customer_id', 'SUM_amount'],
        'output_column_types': ['int64', 'double'],
        'requires_shuffle': True,
        'total_parallelism': 2,
        'estimated_total_rows': 5
    }

    plan = DistributedQueryPlan.from_dict(plan_dict)

    # Assertions
    assert len(plan.stages) == 2, f"Expected 2 stages, got {len(plan.stages)}"
    assert len(plan.shuffles) == 1, f"Expected 1 shuffle, got {len(plan.shuffles)}"
    assert plan.requires_shuffle, "Expected requires_shuffle=True"
    assert len(plan.execution_waves) == 2, f"Expected 2 waves, got {len(plan.execution_waves)}"

    # Get source and sink stages
    sources = plan.get_source_stages()
    sink = plan.get_sink_stage()

    assert len(sources) == 1, f"Expected 1 source stage, got {len(sources)}"
    assert sources[0].stage_id == 'stage_0', "Wrong source stage"
    assert sink is not None, "Expected sink stage"
    assert sink.stage_id == 'stage_1', "Wrong sink stage"

    print(f"  ✓ Created plan with {len(plan.stages)} stages")
    print(f"  ✓ {len(plan.shuffles)} shuffles configured")
    print(f"  ✓ {len(plan.execution_waves)} execution waves")
    print(f"  ✓ Source: {sources[0].stage_id}, Sink: {sink.stage_id}")

    print("\n✓ PASSED: Aggregate plan with shuffle\n")
    return True


async def test_stage_scheduler_filter():
    """Test executing a simple filter plan."""
    print("=" * 60)
    print("Test: StageScheduler filter execution")
    print("=" * 60)

    from sabot import cyarrow as ca
    from sabot.sql.distributed_plan import DistributedQueryPlan, OperatorSpec
    from sabot.sql.stage_scheduler import StageScheduler

    # Create test data
    orders = ca.table({
        'id': [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
        'customer_id': [101, 102, 101, 103, 102, 101, 104, 103, 102, 105],
        'amount': [150.0, 50.0, 200.0, 75.0, 300.0, 25.0, 180.0, 90.0, 250.0, 60.0],
        'status': ['completed', 'pending', 'completed', 'completed', 'pending',
                   'cancelled', 'completed', 'pending', 'completed', 'completed']
    })

    print(f"  Input table: {orders.num_rows} rows")

    table_registry = {'orders': orders}

    # Create plan
    plan_dict = {
        'sql': 'SELECT * FROM orders WHERE amount > 100',
        'stages': [
            {
                'stage_id': 'stage_0',
                'operators': [
                    {
                        'type': 'TableScan',
                        'name': 'Scan orders',
                        'table_name': 'orders',
                        'projected_columns': ['id', 'customer_id', 'amount', 'status'],
                        'estimated_cardinality': 10
                    },
                    {
                        'type': 'Filter',
                        'name': 'Filter amount > 100',
                        'filter_expression': 'amount > 100',
                        'estimated_cardinality': 5
                    }
                ],
                'input_shuffle_ids': [],
                'parallelism': 2,
                'is_source': True,
                'is_sink': True,
                'estimated_rows': 5
            }
        ],
        'shuffles': {},
        'execution_waves': [['stage_0']],
        'output_column_names': ['id', 'customer_id', 'amount', 'status'],
        'output_column_types': ['int64', 'int64', 'double', 'string'],
        'requires_shuffle': False,
        'total_parallelism': 2,
        'estimated_total_rows': 5
    }

    plan = DistributedQueryPlan.from_dict(plan_dict)

    # Simple operator builder for testing
    def simple_operator_builder(spec, input_data):
        if spec.type == 'TableScan':
            return None  # Handled by scheduler
        elif spec.type == 'Filter':
            def filter_func(table):
                import pyarrow.compute as pc
                mask = pc.greater(table.column('amount'), 100)
                return table.filter(mask)
            return filter_func
        return None

    # Execute
    scheduler = StageScheduler(
        table_registry=table_registry,
        operator_builder=simple_operator_builder,
        shuffle_coordinator=None
    )

    result = await scheduler.execute(plan)

    # Verify result
    assert result.num_rows > 0, "Expected non-empty result"
    amounts = result.column('amount').to_pylist()
    assert all(a > 100 for a in amounts), f"Filter failed: amounts = {amounts}"

    print(f"  ✓ Filter executed: {result.num_rows} rows returned")
    print(f"  ✓ All amounts > 100: {amounts}")

    print("\n✓ PASSED: StageScheduler filter execution\n")
    return True


async def test_shuffle_coordinator():
    """Test shuffle coordinator send/receive."""
    print("=" * 60)
    print("Test: ShuffleCoordinator")
    print("=" * 60)

    from sabot import cyarrow as ca
    from sabot.sql.distributed_plan import ShuffleSpec
    from sabot.sql.stage_scheduler import ShuffleCoordinator

    # Create test data
    data = ca.table({
        'customer_id': [1, 2, 1, 2],
        'amount': [100.0, 200.0, 150.0, 250.0]
    })

    # Create shuffle spec
    shuffles = {
        'shuffle_0': ShuffleSpec(
            shuffle_id='shuffle_0',
            type='HASH',
            partition_keys=['customer_id'],
            num_partitions=2,
            producer_stage_id='stage_0',
            consumer_stage_id='stage_1'
        )
    }

    coord = ShuffleCoordinator(shuffles)

    assert len(coord.buffers) == 2, f"Expected 2 partition buffers, got {len(coord.buffers)}"
    print(f"  ✓ Created coordinator with {len(coord.buffers)} partition buffers")

    # Send data
    await coord.send('shuffle_0', 0, data)
    print(f"  ✓ Sent {data.num_rows} rows to shuffle")

    # Receive from each partition
    p0_data = await coord.receive('shuffle_0', 0)
    p1_data = await coord.receive('shuffle_0', 1)

    total_rows = p0_data.num_rows + p1_data.num_rows
    assert total_rows == 4, f"Expected 4 total rows, got {total_rows}"

    print(f"  ✓ Partition 0: {p0_data.num_rows} rows")
    print(f"  ✓ Partition 1: {p1_data.num_rows} rows")
    print(f"  ✓ Total: {total_rows} rows (expected 4)")

    print("\n✓ PASSED: ShuffleCoordinator\n")
    return True


def main():
    """Run all tests."""
    print("\n" + "=" * 60)
    print("DISTRIBUTED SQL INTEGRATION TESTS")
    print("=" * 60 + "\n")

    results = []

    # Synchronous tests
    try:
        results.append(("DistributedQueryPlan creation", test_distributed_plan_creation()))
    except Exception as e:
        print(f"✗ FAILED: DistributedQueryPlan creation\n  {e}")
        traceback.print_exc()
        results.append(("DistributedQueryPlan creation", False))

    try:
        results.append(("Aggregate plan with shuffle", test_aggregate_plan()))
    except Exception as e:
        print(f"✗ FAILED: Aggregate plan with shuffle\n  {e}")
        traceback.print_exc()
        results.append(("Aggregate plan with shuffle", False))

    # Async tests
    async def run_async_tests():
        async_results = []

        try:
            async_results.append(("StageScheduler filter", await test_stage_scheduler_filter()))
        except Exception as e:
            print(f"✗ FAILED: StageScheduler filter\n  {e}")
            traceback.print_exc()
            async_results.append(("StageScheduler filter", False))

        try:
            async_results.append(("ShuffleCoordinator", await test_shuffle_coordinator()))
        except Exception as e:
            print(f"✗ FAILED: ShuffleCoordinator\n  {e}")
            traceback.print_exc()
            async_results.append(("ShuffleCoordinator", False))

        return async_results

    results.extend(asyncio.run(run_async_tests()))

    # Summary
    print("=" * 60)
    print("TEST SUMMARY")
    print("=" * 60)

    passed = sum(1 for _, r in results if r)
    total = len(results)

    for name, passed_test in results:
        status = "✓ PASSED" if passed_test else "✗ FAILED"
        print(f"  {status}: {name}")

    print(f"\n{passed}/{total} tests passed")

    if passed == total:
        print("\n✓ ALL TESTS PASSED\n")
        return 0
    else:
        print(f"\n✗ {total - passed} TESTS FAILED\n")
        return 1


if __name__ == '__main__':
    sys.exit(main())
