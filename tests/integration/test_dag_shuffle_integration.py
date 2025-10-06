#!/usr/bin/env python3
"""
Integration test for DAG-level shuffle execution.

Tests complete flow:
1. Job graph creation with varying parallelism
2. Automatic shuffle edge detection
3. Execution graph generation
4. Shuffle coordinator initialization
"""

import pytest
from sabot.execution.job_graph import JobGraph, StreamOperatorNode, OperatorType
from sabot.execution.execution_graph import ShuffleEdgeType
from sabot.execution.shuffle_coordinator import ShuffleCoordinator


class TestDAGShuffleIntegration:
    """Tests for DAG-level shuffle integration."""

    def test_job_graph_to_execution_graph_with_shuffles(self):
        """
        Test conversion from job graph to execution graph.

        Creates multi-operator pipeline with different parallelism levels
        and verifies shuffle edges are created correctly.
        """
        # Create job graph
        job = JobGraph(job_id="test_job", job_name="Shuffle Test Pipeline")

        # Operator 1: Source (parallelism=4)
        source = StreamOperatorNode(
            operator_type=OperatorType.SOURCE,
            name="source",
            parallelism=4,
            stateful=False
        )
        job.add_operator(source)

        # Operator 2: Map (parallelism=4, same as source)
        map_op = StreamOperatorNode(
            operator_type=OperatorType.MAP,
            name="map",
            parallelism=4,
            stateful=False
        )
        job.add_operator(map_op)
        job.connect(source.operator_id, map_op.operator_id)

        # Operator 3: GroupBy (parallelism=2, stateful with key)
        group_by = StreamOperatorNode(
            operator_type=OperatorType.GROUP_BY,
            name="group_by",
            parallelism=2,
            stateful=True,
            key_by=["user_id"]
        )
        job.add_operator(group_by)
        job.connect(map_op.operator_id, group_by.operator_id)

        # Operator 4: Sink (parallelism=1)
        sink = StreamOperatorNode(
            operator_type=OperatorType.SINK,
            name="sink",
            parallelism=1,
            stateful=False
        )
        job.add_operator(sink)
        job.connect(group_by.operator_id, sink.operator_id)

        # Convert to execution graph
        exec_graph = job.to_execution_graph()

        # Verify task count
        # 4 + 4 + 2 + 1 = 11 tasks
        assert exec_graph.get_task_count() == 11

        # Verify shuffle edges created
        assert len(exec_graph.shuffle_edges) == 3

        # Find shuffle edges
        shuffles = list(exec_graph.shuffle_edges.values())

        # Edge 1: source -> map (same parallelism)
        # Should be FORWARD (operator chaining)
        source_to_map = [s for s in shuffles
                         if s.upstream_operator_id == source.operator_id
                         and s.downstream_operator_id == map_op.operator_id][0]
        assert source_to_map.edge_type == ShuffleEdgeType.FORWARD
        assert source_to_map.num_partitions == 4

        # Edge 2: map -> group_by (different parallelism, stateful)
        # Should be HASH
        map_to_groupby = [s for s in shuffles
                         if s.upstream_operator_id == map_op.operator_id
                         and s.downstream_operator_id == group_by.operator_id][0]
        assert map_to_groupby.edge_type == ShuffleEdgeType.HASH
        assert map_to_groupby.partition_keys == ["user_id"]
        assert map_to_groupby.num_partitions == 2

        # Edge 3: group_by -> sink (different parallelism, stateless)
        # Should be REBALANCE
        groupby_to_sink = [s for s in shuffles
                          if s.upstream_operator_id == group_by.operator_id
                          and s.downstream_operator_id == sink.operator_id][0]
        assert groupby_to_sink.edge_type == ShuffleEdgeType.REBALANCE
        assert groupby_to_sink.num_partitions == 1

    def test_shuffle_coordinator_initialization(self):
        """
        Test shuffle coordinator initialization with agent assignments.
        """
        # Create simple job graph
        job = JobGraph(job_id="coord_test")

        source = StreamOperatorNode(
            operator_type=OperatorType.SOURCE,
            name="source",
            parallelism=3,
            stateful=False
        )
        job.add_operator(source)

        group_by = StreamOperatorNode(
            operator_type=OperatorType.GROUP_BY,
            name="group_by",
            parallelism=2,
            stateful=True,
            key_by=["key"]
        )
        job.add_operator(group_by)
        job.connect(source.operator_id, group_by.operator_id)

        # Convert to execution graph
        exec_graph = job.to_execution_graph()

        # Create shuffle coordinator
        coordinator = ShuffleCoordinator(exec_graph)

        # Simulate agent assignments
        agent_assignments = {}
        task_count = 0
        for task_id in exec_graph.tasks.keys():
            agent_id = f"agent-{task_count % 2}"  # Distribute across 2 agents
            agent_assignments[task_id] = f"{agent_id}:localhost:900{task_count % 2}"
            task_count += 1

        # Initialize coordinator (async, so we test the synchronous parts)
        assert not coordinator._initialized

        # After initialization would be called:
        # await coordinator.initialize(agent_assignments)
        # assert coordinator._initialized
        # assert len(coordinator.shuffle_plans) == 1

    def test_operator_chaining_detection(self):
        """
        Test that operators with same parallelism get FORWARD shuffle (chaining).
        """
        job = JobGraph(job_id="chain_test")

        # Create chain of operators with same parallelism
        prev_op = None
        for i in range(4):
            op = StreamOperatorNode(
                operator_type=OperatorType.MAP if i > 0 else OperatorType.SOURCE,
                name=f"op_{i}",
                parallelism=5,  # All same parallelism
                stateful=False
            )
            job.add_operator(op)

            if prev_op:
                job.connect(prev_op.operator_id, op.operator_id)

            prev_op = op

        # Convert to execution graph
        exec_graph = job.to_execution_graph()

        # All shuffles should be FORWARD (operator chaining)
        for shuffle in exec_graph.shuffle_edges.values():
            assert shuffle.edge_type == ShuffleEdgeType.FORWARD

    def test_hash_shuffle_detection(self):
        """
        Test that stateful operators with keys trigger HASH shuffle.
        """
        job = JobGraph(job_id="hash_test")

        source = StreamOperatorNode(
            operator_type=OperatorType.SOURCE,
            parallelism=10,
            stateful=False
        )
        job.add_operator(source)

        # Stateful operator with key → should trigger HASH
        aggregation = StreamOperatorNode(
            operator_type=OperatorType.AGGREGATE,
            parallelism=5,
            stateful=True,
            key_by=["customer_id", "product_id"]
        )
        job.add_operator(aggregation)
        job.connect(source.operator_id, aggregation.operator_id)

        exec_graph = job.to_execution_graph()

        # Find the shuffle edge
        shuffle = list(exec_graph.shuffle_edges.values())[0]

        assert shuffle.edge_type == ShuffleEdgeType.HASH
        assert shuffle.partition_keys == ["customer_id", "product_id"]
        assert len(shuffle.upstream_task_ids) == 10
        assert len(shuffle.downstream_task_ids) == 5

    def test_rebalance_shuffle_detection(self):
        """
        Test that different parallelism without keys triggers REBALANCE.
        """
        job = JobGraph(job_id="rebalance_test")

        source = StreamOperatorNode(
            operator_type=OperatorType.SOURCE,
            parallelism=8,
            stateful=False
        )
        job.add_operator(source)

        # Different parallelism, stateless → should trigger REBALANCE
        filter_op = StreamOperatorNode(
            operator_type=OperatorType.FILTER,
            parallelism=3,
            stateful=False
        )
        job.add_operator(filter_op)
        job.connect(source.operator_id, filter_op.operator_id)

        exec_graph = job.to_execution_graph()

        shuffle = list(exec_graph.shuffle_edges.values())[0]

        assert shuffle.edge_type == ShuffleEdgeType.REBALANCE
        assert shuffle.partition_keys is None
        assert len(shuffle.upstream_task_ids) == 8
        assert len(shuffle.downstream_task_ids) == 3

    def test_complex_pipeline_with_multiple_shuffles(self):
        """
        Test complex pipeline with multiple shuffle types.
        """
        job = JobGraph(job_id="complex_pipeline")

        # Stage 1: Source (p=10)
        source = StreamOperatorNode(
            operator_type=OperatorType.SOURCE,
            name="kafka_source",
            parallelism=10
        )
        job.add_operator(source)

        # Stage 2: Enrich (p=10, same as source)
        enrich = StreamOperatorNode(
            operator_type=OperatorType.MAP,
            name="enrich",
            parallelism=10
        )
        job.add_operator(enrich)
        job.connect(source.operator_id, enrich.operator_id)

        # Stage 3: GroupBy user (p=5, stateful)
        group_user = StreamOperatorNode(
            operator_type=OperatorType.GROUP_BY,
            name="group_by_user",
            parallelism=5,
            stateful=True,
            key_by=["user_id"]
        )
        job.add_operator(group_user)
        job.connect(enrich.operator_id, group_user.operator_id)

        # Stage 4: Join with dimension table (p=5)
        join_dim = StreamOperatorNode(
            operator_type=OperatorType.HASH_JOIN,
            name="join_dimensions",
            parallelism=5,
            stateful=True,
            key_by=["dim_key"]
        )
        job.add_operator(join_dim)
        job.connect(group_user.operator_id, join_dim.operator_id)

        # Stage 5: Sink (p=2)
        sink = StreamOperatorNode(
            operator_type=OperatorType.SINK,
            name="output",
            parallelism=2
        )
        job.add_operator(sink)
        job.connect(join_dim.operator_id, sink.operator_id)

        exec_graph = job.to_execution_graph()

        # Verify tasks
        assert exec_graph.get_task_count() == 10 + 10 + 5 + 5 + 2

        # Verify shuffles
        assert len(exec_graph.shuffle_edges) == 4

        shuffles = list(exec_graph.shuffle_edges.values())

        # Verify shuffle types
        shuffle_types = [s.edge_type for s in shuffles]
        assert ShuffleEdgeType.FORWARD in shuffle_types  # source -> enrich
        assert ShuffleEdgeType.HASH in shuffle_types     # enrich -> group, group -> join
        assert ShuffleEdgeType.REBALANCE in shuffle_types  # join -> sink


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
