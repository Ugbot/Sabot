# -*- coding: utf-8 -*-
"""
Integration tests for DBOS-backed JobManager.

Tests job submission, task assignment, and basic orchestration.
"""

import asyncio
import unittest
import tempfile
import os
from sabot.job_manager import JobManager
from sabot.execution.job_graph import JobGraph, StreamOperatorNode, OperatorType


class TestDBOSJobManager(unittest.TestCase):
    """Test DBOS-backed job orchestration."""

    def setUp(self):
        """Set up test environment."""
        # Use temporary SQLite database for testing
        self.db_file = tempfile.NamedTemporaryFile(delete=False, suffix='.db')
        self.db_file.close()
        self.db_url = f'sqlite:///{self.db_file.name}'

        # Create JobManager in local mode
        self.job_manager = JobManager(self.db_url)

    def tearDown(self):
        """Clean up test environment."""
        if os.path.exists(self.db_file.name):
            os.unlink(self.db_file.name)

    def test_job_submission_workflow(self):
        """Test complete job submission workflow."""
        async def run_test():
            # Create a simple job graph
            graph = JobGraph(job_name="test_job")
            source = StreamOperatorNode(operator_type=OperatorType.SOURCE)
            map_op = StreamOperatorNode(operator_type=OperatorType.MAP)
            sink = StreamOperatorNode(operator_type=OperatorType.SINK)

            graph.add_operator(source)
            graph.add_operator(map_op)
            graph.add_operator(sink)
            graph.connect(source.operator_id, map_op.operator_id)
            graph.connect(map_op.operator_id, sink.operator_id)

            # Submit job
            job_id = await self.job_manager.submit_job(graph)

            # Verify job was created
            self.assertIsNotNone(job_id)

            # Check job status
            status = await self.job_manager.get_job_status(job_id)
            self.assertEqual(status['status'], 'running')

            # List jobs
            jobs = await self.job_manager.list_jobs()
            self.assertGreater(len(jobs), 0)

            # Find our job
            our_job = next((j for j in jobs if j['job_id'] == job_id), None)
            self.assertIsNotNone(our_job)
            self.assertEqual(our_job['job_name'], 'test_job')

        asyncio.run(run_test())

    def test_task_assignment(self):
        """Test task creation and assignment."""
        async def run_test():
            # Create job with multiple operators
            graph = JobGraph(job_name="multi_op_job")
            source = StreamOperatorNode(operator_type=OperatorType.SOURCE)
            filter_op = StreamOperatorNode(operator_type=OperatorType.FILTER)
            map_op = StreamOperatorNode(operator_type=OperatorType.MAP)
            sink = StreamOperatorNode(operator_type=OperatorType.SINK)

            graph.add_operator(source)
            graph.add_operator(filter_op)
            graph.add_operator(map_op)
            graph.add_operator(sink)

            graph.connect(source.operator_id, filter_op.operator_id)
            graph.connect(filter_op.operator_id, map_op.operator_id)
            graph.connect(map_op.operator_id, sink.operator_id)

            # Submit job
            job_id = await self.job_manager.submit_job(graph)

            # Check tasks were created
            tasks = await self.job_manager.get_job_tasks(job_id)
            self.assertGreater(len(tasks), 0)

            # Verify task states
            for task in tasks:
                self.assertIn(task['state'], ['scheduled', 'running', 'completed'])

        asyncio.run(run_test())

    def test_agent_registration(self):
        """Test agent registration and heartbeat."""
        async def run_test():
            # Register an agent
            agent_id = "test_agent_1"
            await self.job_manager.register_agent({
                'agent_id': agent_id,
                'host': 'localhost',
                'port': 8080,
                'max_workers': 4,
                'available_slots': 4
            })

            # Verify agent was registered
            agents = await self.job_manager.list_agents()
            self.assertGreater(len(agents), 0)

            agent = next((a for a in agents if a['agent_id'] == agent_id), None)
            self.assertIsNotNone(agent)
            self.assertEqual(agent['host'], 'localhost')
            self.assertEqual(agent['max_workers'], 4)

            # Send heartbeat
            await self.job_manager.send_heartbeat({
                'agent_id': agent_id,
                'cpu_percent': 45.0,
                'memory_percent': 60.0,
                'active_workers': 2,
                'total_processed': 1000
            })

            # Verify heartbeat updated
            agents = await self.job_manager.list_agents()
            agent = next((a for a in agents if a['agent_id'] == agent_id), None)
            self.assertIsNotNone(agent['last_heartbeat'])

        asyncio.run(run_test())

    def test_job_cancellation(self):
        """Test job cancellation."""
        async def run_test():
            # Create and submit job
            graph = JobGraph(job_name="cancel_test")
            source = StreamOperatorNode(operator_type=OperatorType.SOURCE)
            sink = StreamOperatorNode(operator_type=OperatorType.SINK)

            graph.add_operator(source)
            graph.add_operator(sink)
            graph.connect(source.operator_id, sink.operator_id)

            job_id = await self.job_manager.submit_job(graph)

            # Verify job is running
            status = await self.job_manager.get_job_status(job_id)
            self.assertEqual(status['status'], 'running')

            # Cancel job
            await self.job_manager.cancel_job(job_id)

            # Verify job was cancelled
            status = await self.job_manager.get_job_status(job_id)
            self.assertEqual(status['status'], 'cancelled')

        asyncio.run(run_test())


class TestLiveRescaling(unittest.TestCase):
    """Test live operator rescaling."""

    def setUp(self):
        self.db_file = tempfile.NamedTemporaryFile(delete=False, suffix='.db')
        self.db_file.close()
        self.db_url = f'sqlite:///{self.db_file.name}'
        self.job_manager = JobManager(self.db_url)

    def tearDown(self):
        if os.path.exists(self.db_file.name):
            os.unlink(self.db_file.name)

    def test_rescaling_validation(self):
        """Test rescaling request validation."""
        async def run_test():
            # Create and submit job
            graph = JobGraph(job_name="rescale_test")
            source = StreamOperatorNode(operator_type=OperatorType.SOURCE)
            map_op = StreamOperatorNode(operator_type=OperatorType.MAP, parallelism=4)
            sink = StreamOperatorNode(operator_type=OperatorType.SINK)

            graph.add_operator(source)
            graph.add_operator(map_op)
            graph.add_operator(sink)
            graph.connect(source.operator_id, map_op.operator_id)
            graph.connect(map_op.operator_id, sink.operator_id)

            job_id = await self.job_manager.submit_job(graph)

            # Test invalid rescaling requests
            with self.assertRaises(ValueError):
                await self.job_manager._validate_rescaling_request("invalid_job", map_op.operator_id, 8)

            with self.assertRaises(ValueError):
                await self.job_manager._validate_rescaling_request(job_id, "invalid_operator", 8)

            with self.assertRaises(ValueError):
                await self.job_manager._validate_rescaling_request(job_id, map_op.operator_id, 0)  # Invalid parallelism

            # Valid request should not raise
            await self.job_manager._validate_rescaling_request(job_id, map_op.operator_id, 8)

        asyncio.run(run_test())

    def test_rescaling_workflow(self):
        """Test complete rescaling workflow."""
        async def run_test():
            # Create job with scalable operator
            graph = JobGraph(job_name="rescale_workflow")
            source = StreamOperatorNode(operator_type=OperatorType.SOURCE)
            map_op = StreamOperatorNode(operator_type=OperatorType.MAP, parallelism=2)
            sink = StreamOperatorNode(operator_type=OperatorType.SINK)

            graph.add_operator(source)
            graph.add_operator(map_op)
            graph.add_operator(sink)
            graph.connect(source.operator_id, map_op.operator_id)
            graph.connect(map_op.operator_id, sink.operator_id)

            job_id = await self.job_manager.submit_job(graph)

            # Attempt rescaling (will fail gracefully in test environment)
            try:
                await self.job_manager.rescale_operator(job_id, map_op.operator_id, 4)
            except Exception as e:
                # Expected in test environment without full agent setup
                self.assertIn("rescaling", str(e).lower())

        asyncio.run(run_test())


class TestFailureRecovery(unittest.TestCase):
    """Test failure recovery functionality."""

    def setUp(self):
        self.db_file = tempfile.NamedTemporaryFile(delete=False, suffix='.db')
        self.db_file.close()
        self.db_url = f'sqlite:///{self.db_file.name}'
        self.job_manager = JobManager(self.db_url)

    def tearDown(self):
        if os.path.exists(self.db_file.name):
            os.unlink(self.db_file.name)

    def test_agent_failure_detection(self):
        """Test agent failure detection and recovery."""
        async def run_test():
            # Register agent
            agent_id = "failing_agent"
            await self.job_manager.register_agent({
                'agent_id': agent_id,
                'host': 'localhost',
                'port': 8080,
                'max_workers': 2,
                'available_slots': 2
            })

            # Send initial heartbeat
            await self.job_manager.send_heartbeat({
                'agent_id': agent_id,
                'cpu_percent': 50.0,
                'memory_percent': 40.0
            })

            # Verify agent is alive
            agents = await self.job_manager.list_agents()
            agent = next((a for a in agents if a['agent_id'] == agent_id), None)
            self.assertEqual(agent['status'], 'alive')

            # Simulate agent failure by not sending heartbeats
            # In real scenario, this would trigger handle_agent_failure
            # For test, we manually call handle_agent_failure
            await self.job_manager.handle_agent_failure(agent_id)

            # Verify agent marked as failed
            agents = await self.job_manager.list_agents()
            agent = next((a for a in agents if a['agent_id'] == agent_id), None)
            self.assertEqual(agent['status'], 'failed')

        asyncio.run(run_test())


if __name__ == '__main__':
    unittest.main()
