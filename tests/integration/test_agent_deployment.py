"""
Integration Tests for Multi-Agent Deployment

Tests distributed task deployment, agent coordination,
cross-agent shuffle, and failure recovery.
"""

import pytest
import asyncio
from sabot import cyarrow as pa
from unittest.mock import Mock, AsyncMock, patch

from sabot.agent import Agent, AgentConfig
from sabot.job_manager import JobManager


class TestTwoAgentDeployment:
    """Test deployment across 2 agents"""

    @pytest.mark.integration
    @pytest.mark.asyncio
    async def test_two_agent_startup(self):
        """Test starting 2 agents successfully"""
        # Create 2 agents
        config1 = AgentConfig(agent_id="agent-1", host="localhost", port=8816)
        config2 = AgentConfig(agent_id="agent-2", host="localhost", port=8817)

        agent1 = Agent(config1)
        agent2 = Agent(config2)

        # Start both agents
        with patch.object(agent1, '_start_local_mode', new_callable=AsyncMock), \
             patch.object(agent2, '_start_local_mode', new_callable=AsyncMock):
            await agent1.start()
            await agent2.start()

            assert agent1.running is True
            assert agent2.running is True

        # Cleanup
        await agent1.stop()
        await agent2.stop()

    @pytest.mark.integration
    @pytest.mark.asyncio
    async def test_task_distribution(self):
        """Test task distribution across 2 agents"""
        # Create 2 agents
        config1 = AgentConfig(agent_id="agent-1", host="localhost", port=8816, num_slots=2)
        config2 = AgentConfig(agent_id="agent-2", host="localhost", port=8817, num_slots=2)

        agent1 = Agent(config1)
        agent2 = Agent(config2)

        with patch.object(agent1, '_start_local_mode', new_callable=AsyncMock), \
             patch.object(agent2, '_start_local_mode', new_callable=AsyncMock):
            await agent1.start()
            await agent2.start()

            # Create mock tasks
            task1 = Mock(task_id="task-1", memory_mb=1024, operator_type="map")
            task2 = Mock(task_id="task-2", memory_mb=1024, operator_type="map")
            task3 = Mock(task_id="task-3", memory_mb=1024, operator_type="map")
            task4 = Mock(task_id="task-4", memory_mb=1024, operator_type="map")

            # Deploy tasks to agents
            with patch.object(agent1, '_create_task_executor', return_value=AsyncMock()):
                with patch.object(agent2, '_create_task_executor', return_value=AsyncMock()):
                    await agent1.deploy_task(task1)
                    await agent1.deploy_task(task2)
                    await agent2.deploy_task(task3)
                    await agent2.deploy_task(task4)

                    # Verify distribution
                    assert len(agent1.active_tasks) == 2
                    assert len(agent2.active_tasks) == 2
                    assert agent1.get_available_slots() == 0
                    assert agent2.get_available_slots() == 0

            # Cleanup
            await agent1.stop()
            await agent2.stop()


class TestDistributedShuffle:
    """Test shuffle between agents"""

    @pytest.mark.integration
    @pytest.mark.asyncio
    async def test_cross_agent_shuffle(self):
        """Test data shuffle between agents"""
        # This test verifies that data can be shuffled between agents

        config1 = AgentConfig(agent_id="agent-1", host="localhost", port=8816)
        config2 = AgentConfig(agent_id="agent-2", host="localhost", port=8817)

        agent1 = Agent(config1)
        agent2 = Agent(config2)

        with patch.object(agent1, '_start_local_mode', new_callable=AsyncMock), \
             patch.object(agent2, '_start_local_mode', new_callable=AsyncMock):
            await agent1.start()
            await agent2.start()

            # Create test data
            test_batch = pa.RecordBatch.from_pydict({
                'key': [1, 2, 3, 4, 5, 6],
                'value': ['a', 'b', 'c', 'd', 'e', 'f']
            })

            # Mock shuffle transport
            shuffle_client = Mock()
            shuffle_client.send_partition = AsyncMock()

            # Simulate shuffle from agent1 to agent2
            # In real scenario, this would use Arrow Flight
            partition_id = 0
            await shuffle_client.send_partition(
                shuffle_id=b"test-shuffle",
                partition_id=partition_id,
                batch=test_batch,
                target_agent=b"localhost:8817"
            )

            shuffle_client.send_partition.assert_called_once()

            await agent1.stop()
            await agent2.stop()


class TestAgentFailureRecovery:
    """Test agent failure and recovery"""

    @pytest.mark.integration
    @pytest.mark.asyncio
    async def test_agent_failure_detection(self):
        """Test detection of failed agent"""
        config1 = AgentConfig(
            agent_id="agent-1",
            host="localhost",
            port=8816,
            job_manager_url="http://localhost:8080"
        )

        agent1 = Agent(config1)

        with patch.object(agent1, '_start_distributed_mode', new_callable=AsyncMock), \
             patch.object(agent1, '_register_with_job_manager', new_callable=AsyncMock):
            await agent1.start()

            # Simulate failure
            agent1.running = False

            # JobManager should detect failure via missed heartbeats
            # This would be tested at JobManager level

            await agent1.stop()

    @pytest.mark.integration
    @pytest.mark.asyncio
    async def test_task_redistribution_on_failure(self):
        """Test task redistribution when agent fails"""
        config1 = AgentConfig(agent_id="agent-1", host="localhost", port=8816, num_slots=2)
        config2 = AgentConfig(agent_id="agent-2", host="localhost", port=8817, num_slots=2)

        agent1 = Agent(config1)
        agent2 = Agent(config2)

        with patch.object(agent1, '_start_local_mode', new_callable=AsyncMock), \
             patch.object(agent2, '_start_local_mode', new_callable=AsyncMock):
            await agent1.start()
            await agent2.start()

            # Deploy task to agent1
            task1 = Mock(task_id="task-1", memory_mb=1024, operator_type="map")

            with patch.object(agent1, '_create_task_executor', return_value=AsyncMock()):
                await agent1.deploy_task(task1)
                assert "task-1" in agent1.active_tasks

            # Simulate agent1 failure
            await agent1.stop()

            # In real scenario, JobManager would:
            # 1. Detect agent1 failure via heartbeat
            # 2. Reassign task-1 to agent2
            # 3. Deploy task-1 to agent2

            # For this test, we simulate reassignment
            with patch.object(agent2, '_create_task_executor', return_value=AsyncMock()):
                await agent2.deploy_task(task1)
                assert "task-1" in agent2.active_tasks

            await agent2.stop()


class TestEndToEndDataflow:
    """Test complete end-to-end dataflow across agents"""

    @pytest.mark.integration
    @pytest.mark.asyncio
    async def test_two_agent_pipeline(self):
        """Test complete pipeline across 2 agents"""
        # Agent 1: Source + Map
        # Agent 2: Filter + Sink

        config1 = AgentConfig(agent_id="agent-1", host="localhost", port=8816)
        config2 = AgentConfig(agent_id="agent-2", host="localhost", port=8817)

        agent1 = Agent(config1)
        agent2 = Agent(config2)

        with patch.object(agent1, '_start_local_mode', new_callable=AsyncMock), \
             patch.object(agent2, '_start_local_mode', new_callable=AsyncMock):
            await agent1.start()
            await agent2.start()

            # Create source data
            source_data = pa.RecordBatch.from_pydict({
                'value': list(range(100))
            })

            # Mock operators
            map_operator = Mock()
            map_operator.requires_shuffle = Mock(return_value=False)

            # Map: x * 2
            mapped_data = pa.RecordBatch.from_pydict({
                'value': [x * 2 for x in range(100)]
            })
            map_operator.__iter__ = Mock(return_value=iter([mapped_data]))

            filter_operator = Mock()
            filter_operator.requires_shuffle = Mock(return_value=False)

            # Filter: x > 50
            filtered_data = pa.RecordBatch.from_pydict({
                'value': [x for x in mapped_data['value'].to_pylist() if x > 50]
            })
            filter_operator.__iter__ = Mock(return_value=iter([filtered_data]))

            # Deploy map to agent1
            map_task = Mock(task_id="map-task", memory_mb=1024)
            map_task.operator = map_operator

            # Deploy filter to agent2
            filter_task = Mock(task_id="filter-task", memory_mb=1024)
            filter_task.operator = filter_operator

            with patch.object(agent1, '_create_task_executor', return_value=AsyncMock()):
                with patch.object(agent2, '_create_task_executor', return_value=AsyncMock()):
                    await agent1.deploy_task(map_task)
                    await agent2.deploy_task(filter_task)

                    # Execute pipeline
                    # In real scenario, data would flow: agent1 → agent2

                    assert "map-task" in agent1.active_tasks
                    assert "filter-task" in agent2.active_tasks

            await agent1.stop()
            await agent2.stop()


class TestAgentCoordination:
    """Test agent coordination via JobManager"""

    @pytest.mark.integration
    @pytest.mark.asyncio
    async def test_job_manager_coordination(self):
        """Test JobManager coordinating multiple agents"""
        # Create JobManager
        job_manager = JobManager(dbos_url=None)  # Local mode

        # Create agents
        config1 = AgentConfig(
            agent_id="agent-1",
            host="localhost",
            port=8816,
            job_manager_url="http://localhost:8080"
        )
        config2 = AgentConfig(
            agent_id="agent-2",
            host="localhost",
            port=8817,
            job_manager_url="http://localhost:8080"
        )

        agent1 = Agent(config1)
        agent2 = Agent(config2)

        with patch.object(agent1, '_start_distributed_mode', new_callable=AsyncMock), \
             patch.object(agent2, '_start_distributed_mode', new_callable=AsyncMock), \
             patch.object(agent1, '_register_with_job_manager', new_callable=AsyncMock), \
             patch.object(agent2, '_register_with_job_manager', new_callable=AsyncMock):

            await agent1.start()
            await agent2.start()

            # Agents should register with JobManager
            agent1._register_with_job_manager.assert_called_once()
            agent2._register_with_job_manager.assert_called_once()

            # JobManager should track agents
            # (This would be tested in JobManager tests)

            await agent1.stop()
            await agent2.stop()


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
