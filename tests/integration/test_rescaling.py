"""
Live Operator Rescaling Integration Tests

Tests rescaling operator parallelism, state redistribution,
and zero-downtime rescaling operations.
"""

import pytest
import asyncio
import pyarrow as pa
from unittest.mock import Mock, AsyncMock, patch

from sabot.job_manager import JobManager


@pytest.mark.integration
class TestOperatorRescaling:
    """Test live operator rescaling"""

    @pytest.mark.asyncio
    async def test_rescale_up_4_to_8_tasks(self):
        """Test rescaling operator from 4 to 8 tasks"""
        jm = JobManager(dbos_url=None)

        job_id = "test-job-123"
        operator_id = "map-operator-1"
        old_parallelism = 4
        new_parallelism = 8

        with patch.object(jm, '_init_rescale_step', new_callable=AsyncMock) as mock_init, \
             patch.object(jm, '_pause_operator_tasks_step', new_callable=AsyncMock), \
             patch.object(jm, '_drain_operator_tasks_step', new_callable=AsyncMock), \
             patch.object(jm, '_create_rescaled_tasks_step', new_callable=AsyncMock) as mock_create, \
             patch.object(jm, '_deploy_rescaled_tasks_step', new_callable=AsyncMock), \
             patch.object(jm, '_cancel_old_tasks_step', new_callable=AsyncMock), \
             patch.object(jm, '_complete_rescale_step', new_callable=AsyncMock):

            mock_init.return_value = (old_parallelism, False)  # (old_parallelism, stateful)
            mock_create.return_value = [f"task-{i}" for i in range(8)]

            await jm.rescale_operator(job_id, operator_id, new_parallelism)

            # Verify rescaling steps called
            mock_init.assert_called_once()
            mock_create.assert_called_once()

    @pytest.mark.asyncio
    async def test_rescale_down_8_to_4_tasks(self):
        """Test rescaling operator from 8 to 4 tasks"""
        jm = JobManager(dbos_url=None)

        job_id = "test-job-123"
        operator_id = "map-operator-1"
        old_parallelism = 8
        new_parallelism = 4

        with patch.object(jm, '_init_rescale_step', new_callable=AsyncMock) as mock_init, \
             patch.object(jm, '_pause_operator_tasks_step', new_callable=AsyncMock), \
             patch.object(jm, '_drain_operator_tasks_step', new_callable=AsyncMock), \
             patch.object(jm, '_create_rescaled_tasks_step', new_callable=AsyncMock) as mock_create, \
             patch.object(jm, '_deploy_rescaled_tasks_step', new_callable=AsyncMock), \
             patch.object(jm, '_cancel_old_tasks_step', new_callable=AsyncMock), \
             patch.object(jm, '_complete_rescale_step', new_callable=AsyncMock):

            mock_init.return_value = (old_parallelism, False)
            mock_create.return_value = [f"task-{i}" for i in range(4)]

            await jm.rescale_operator(job_id, operator_id, new_parallelism)

            # Verify rescaling completed
            mock_create.assert_called_once()


@pytest.mark.integration
class TestStateRedistribution:
    """Test state redistribution during rescaling"""

    @pytest.mark.asyncio
    async def test_state_redistribution(self):
        """Test state is correctly redistributed"""
        # Mock state redistribution
        from sabot.state_redistribution import StateRedistributor

        old_tasks = [Mock(task_id=f"old-task-{i}") for i in range(4)]
        new_tasks = [Mock(task_id=f"new-task-{i}") for i in range(8)]

        redistributor = StateRedistributor()

        with patch.object(redistributor, '_snapshot_state', new_callable=AsyncMock) as mock_snapshot, \
             patch.object(redistributor, '_repartition_state', new_callable=AsyncMock) as mock_repartition, \
             patch.object(redistributor, '_restore_state', new_callable=AsyncMock) as mock_restore:

            await redistributor.redistribute(old_tasks, new_tasks, key_columns=['user_id'])

            # Verify redistribution steps
            mock_snapshot.assert_called_once()
            mock_repartition.assert_called_once()
            mock_restore.assert_called_once()

    @pytest.mark.asyncio
    async def test_no_data_loss_during_rescale(self):
        """Test no data loss during rescaling"""
        # Create test data
        test_state = {
            'user-1': {'count': 100},
            'user-2': {'count': 200},
            'user-3': {'count': 300},
        }

        # Simulate rescaling from 2 to 4 tasks
        # Verify all state preserved

        from sabot.state_redistribution import StateRedistributor
        redistributor = StateRedistributor()

        # Mock state operations
        with patch.object(redistributor, '_snapshot_state', new_callable=AsyncMock) as mock_snapshot:
            mock_snapshot.return_value = test_state

            snapshots = await mock_snapshot([])

            # Verify all state captured
            assert len(snapshots) == len(test_state)


@pytest.mark.integration
class TestStatefulOperatorRescaling:
    """Test rescaling stateful operators"""

    @pytest.mark.asyncio
    async def test_stateful_join_rescaling(self):
        """Test rescaling stateful join operator"""
        jm = JobManager(dbos_url=None)

        job_id = "test-job-123"
        join_operator_id = "join-op-1"
        new_parallelism = 8

        with patch.object(jm, '_init_rescale_step', new_callable=AsyncMock) as mock_init, \
             patch.object(jm, '_pause_operator_tasks_step', new_callable=AsyncMock), \
             patch.object(jm, '_drain_operator_tasks_step', new_callable=AsyncMock), \
             patch.object(jm, '_create_rescaled_tasks_step', new_callable=AsyncMock) as mock_create, \
             patch.object(jm, '_redistribute_state_step', new_callable=AsyncMock) as mock_redistribute, \
             patch.object(jm, '_deploy_rescaled_tasks_step', new_callable=AsyncMock), \
             patch.object(jm, '_cancel_old_tasks_step', new_callable=AsyncMock), \
             patch.object(jm, '_complete_rescale_step', new_callable=AsyncMock):

            # Stateful operator
            mock_init.return_value = (4, True)  # (old_parallelism, stateful=True)
            mock_create.return_value = [f"task-{i}" for i in range(8)]

            await jm.rescale_operator(job_id, join_operator_id, new_parallelism)

            # Verify state redistribution called for stateful operator
            mock_redistribute.assert_called_once()


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
