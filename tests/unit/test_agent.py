"""
Unit Tests for Agent Worker Node

Tests agent initialization, slot management, task execution,
and health monitoring functionality.
"""

import pytest
import asyncio
from unittest.mock import Mock, AsyncMock, patch, MagicMock
from dataclasses import dataclass

from sabot.agent import Agent, AgentConfig, Slot, TaskExecutor


class TestAgentConfig:
    """Test AgentConfig dataclass"""

    def test_default_config(self):
        """Test default configuration values"""
        config = AgentConfig(agent_id="test-agent")

        assert config.agent_id == "test-agent"
        assert config.host == "0.0.0.0"
        assert config.port == 8816
        assert config.num_slots == 8
        assert config.workers_per_slot == 4
        assert config.memory_mb == 16384
        assert config.cpu_cores == 8
        assert config.job_manager_url is None

    def test_custom_config(self):
        """Test custom configuration values"""
        config = AgentConfig(
            agent_id="custom-agent",
            host="localhost",
            port=9000,
            num_slots=4,
            workers_per_slot=2,
            memory_mb=8192,
            cpu_cores=4,
            job_manager_url="http://localhost:8080"
        )

        assert config.agent_id == "custom-agent"
        assert config.host == "localhost"
        assert config.port == 9000
        assert config.num_slots == 4
        assert config.job_manager_url == "http://localhost:8080"


class TestSlot:
    """Test Slot class for task execution"""

    def test_slot_initialization(self):
        """Test slot initialization"""
        slot = Slot(slot_id=0, memory_mb=2048)

        assert slot.slot_id == 0
        assert slot.memory_mb == 2048
        assert slot.assigned_task is None
        assert slot.is_free() is True

    def test_slot_assignment(self):
        """Test task assignment to slot"""
        slot = Slot(slot_id=0, memory_mb=2048)

        # Mock task
        task = Mock()
        task.task_id = "task-123"
        task.memory_mb = 1024

        # Assign task
        slot.assign(task)

        assert slot.assigned_task == "task-123"
        assert slot.is_free() is False

    def test_slot_release(self):
        """Test task release from slot"""
        slot = Slot(slot_id=0, memory_mb=2048)

        # Mock task
        task = Mock()
        task.task_id = "task-123"
        task.memory_mb = 1024

        # Assign and release
        slot.assign(task)
        assert slot.is_free() is False

        slot.release()
        assert slot.is_free() is True
        assert slot.assigned_task is None

    def test_slot_can_fit(self):
        """Test slot capacity check"""
        slot = Slot(slot_id=0, memory_mb=2048)

        # Task that fits
        small_task = Mock()
        small_task.memory_mb = 1024
        assert slot.can_fit(small_task) is True

        # Task that doesn't fit
        large_task = Mock()
        large_task.memory_mb = 4096
        assert slot.can_fit(large_task) is False


class TestAgent:
    """Test Agent worker node class"""

    @pytest.mark.asyncio
    async def test_agent_initialization(self):
        """Test agent initialization"""
        config = AgentConfig(agent_id="test-agent", num_slots=4)
        agent = Agent(config)

        assert agent.agent_id == "test-agent"
        assert agent.config == config
        assert len(agent.slots) == 4
        assert all(slot.is_free() for slot in agent.slots.values())

    @pytest.mark.asyncio
    async def test_agent_start_local_mode(self):
        """Test agent start in local mode"""
        config = AgentConfig(
            agent_id="test-agent",
            num_slots=2,
            job_manager_url=None  # Local mode
        )
        agent = Agent(config)

        with patch.object(agent, '_start_local_mode', new_callable=AsyncMock) as mock_start:
            await agent.start()
            mock_start.assert_called_once()

    @pytest.mark.asyncio
    async def test_agent_start_distributed_mode(self):
        """Test agent start in distributed mode"""
        config = AgentConfig(
            agent_id="test-agent",
            num_slots=2,
            job_manager_url="http://localhost:8080"  # Distributed mode
        )
        agent = Agent(config)

        with patch.object(agent, '_start_distributed_mode', new_callable=AsyncMock) as mock_start, \
             patch.object(agent, '_register_with_job_manager', new_callable=AsyncMock):
            await agent.start()
            mock_start.assert_called_once()

    @pytest.mark.asyncio
    async def test_slot_allocation(self):
        """Test slot allocation for tasks"""
        config = AgentConfig(agent_id="test-agent", num_slots=2)
        agent = Agent(config)

        # Mock task
        task = Mock()
        task.task_id = "task-123"
        task.memory_mb = 1024

        # Allocate slot
        slot = agent._allocate_slot(task)

        assert slot is not None
        assert slot.assigned_task == "task-123"
        assert len(agent._get_free_slots()) == 1

    @pytest.mark.asyncio
    async def test_slot_allocation_full_capacity(self):
        """Test slot allocation when agent is at full capacity"""
        config = AgentConfig(agent_id="test-agent", num_slots=2)
        agent = Agent(config)

        # Fill all slots
        task1 = Mock(task_id="task-1", memory_mb=1024)
        task2 = Mock(task_id="task-2", memory_mb=1024)
        task3 = Mock(task_id="task-3", memory_mb=1024)

        slot1 = agent._allocate_slot(task1)
        slot2 = agent._allocate_slot(task2)

        assert slot1 is not None
        assert slot2 is not None

        # Try to allocate when full
        slot3 = agent._allocate_slot(task3)
        assert slot3 is None

    @pytest.mark.asyncio
    async def test_task_deployment(self):
        """Test task deployment to agent"""
        config = AgentConfig(agent_id="test-agent", num_slots=2)
        agent = Agent(config)

        # Mock task
        task = Mock()
        task.task_id = "task-123"
        task.memory_mb = 1024
        task.operator_type = "map"

        # Mock task executor creation
        with patch.object(agent, '_create_task_executor', return_value=Mock(spec=TaskExecutor)) as mock_create:
            with patch.object(agent, '_allocate_slot', return_value=Slot(0, 2048)) as mock_alloc:
                await agent.deploy_task(task)

                mock_alloc.assert_called_once_with(task)
                mock_create.assert_called_once()
                assert task.task_id in agent.active_tasks

    @pytest.mark.asyncio
    async def test_task_execution_start(self):
        """Test starting task execution"""
        config = AgentConfig(agent_id="test-agent", num_slots=2)
        agent = Agent(config)

        # Mock task and executor
        task = Mock(task_id="task-123", memory_mb=1024)
        executor = AsyncMock(spec=TaskExecutor)

        agent.active_tasks["task-123"] = executor

        # Start execution
        await agent.start_task("task-123")

        executor.start.assert_called_once()

    @pytest.mark.asyncio
    async def test_task_execution_stop(self):
        """Test stopping task execution"""
        config = AgentConfig(agent_id="test-agent", num_slots=2)
        agent = Agent(config)

        # Mock task and executor
        task = Mock(task_id="task-123", memory_mb=1024)
        executor = AsyncMock(spec=TaskExecutor)

        agent.active_tasks["task-123"] = executor

        # Stop execution
        await agent.stop_task("task-123")

        executor.stop.assert_called_once()

    @pytest.mark.asyncio
    async def test_slot_release_on_task_completion(self):
        """Test slot is released when task completes"""
        config = AgentConfig(agent_id="test-agent", num_slots=2)
        agent = Agent(config)

        # Mock task
        task = Mock(task_id="task-123", memory_mb=1024)

        # Allocate slot
        slot = agent._allocate_slot(task)
        assert slot.is_free() is False

        # Complete task
        await agent._on_task_completed(task.task_id)

        # Verify slot is released
        assert slot.is_free() is True
        assert len(agent._get_free_slots()) == 2

    @pytest.mark.asyncio
    async def test_heartbeat_reporting(self):
        """Test heartbeat reporting to JobManager"""
        config = AgentConfig(
            agent_id="test-agent",
            num_slots=2,
            job_manager_url="http://localhost:8080"
        )
        agent = Agent(config)

        with patch('aiohttp.ClientSession.post', new_callable=AsyncMock) as mock_post:
            mock_post.return_value.__aenter__.return_value.status = 200

            await agent._send_heartbeat()

            # Verify heartbeat was sent
            mock_post.assert_called_once()
            call_args = mock_post.call_args
            assert '/heartbeat' in str(call_args)

    @pytest.mark.asyncio
    async def test_agent_capacity_management(self):
        """Test agent capacity management"""
        config = AgentConfig(agent_id="test-agent", num_slots=4)
        agent = Agent(config)

        # Initial capacity
        assert agent.get_available_slots() == 4

        # Deploy tasks
        task1 = Mock(task_id="task-1", memory_mb=1024)
        task2 = Mock(task_id="task-2", memory_mb=1024)

        with patch.object(agent, '_create_task_executor', return_value=Mock(spec=TaskExecutor)):
            await agent.deploy_task(task1)
            await agent.deploy_task(task2)

        # Verify capacity reduced
        assert agent.get_available_slots() == 2

    @pytest.mark.asyncio
    async def test_agent_shutdown(self):
        """Test graceful agent shutdown"""
        config = AgentConfig(agent_id="test-agent", num_slots=2)
        agent = Agent(config)

        # Add mock tasks
        executor1 = AsyncMock(spec=TaskExecutor)
        executor2 = AsyncMock(spec=TaskExecutor)
        agent.active_tasks = {"task-1": executor1, "task-2": executor2}

        # Shutdown
        await agent.stop()

        # Verify all tasks stopped
        executor1.stop.assert_called_once()
        executor2.stop.assert_called_once()
        assert len(agent.active_tasks) == 0


class TestAgentIntegration:
    """Integration tests for agent functionality"""

    @pytest.mark.asyncio
    async def test_agent_lifecycle(self):
        """Test complete agent lifecycle"""
        config = AgentConfig(agent_id="test-agent", num_slots=2)
        agent = Agent(config)

        # Start agent
        with patch.object(agent, '_start_local_mode', new_callable=AsyncMock):
            await agent.start()
            assert agent.running is True

        # Deploy task
        task = Mock(task_id="task-123", memory_mb=1024, operator_type="map")

        with patch.object(agent, '_create_task_executor', return_value=AsyncMock(spec=TaskExecutor)):
            await agent.deploy_task(task)
            assert task.task_id in agent.active_tasks

        # Stop agent
        await agent.stop()
        assert agent.running is False
        assert len(agent.active_tasks) == 0


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
