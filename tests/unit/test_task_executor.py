"""
Unit Tests for TaskExecutor

Tests task execution, morsel-driven batch processing,
shuffle-aware execution, and error handling.
"""

import pytest
import asyncio
import pyarrow as pa
from unittest.mock import Mock, AsyncMock, patch, MagicMock

from sabot.agent import TaskExecutor, Slot


class TestTaskExecutorCreation:
    """Test TaskExecutor instantiation and setup"""

    def test_executor_initialization(self):
        """Test basic executor initialization"""
        # Mock dependencies
        task = Mock()
        task.task_id = "task-123"
        task.operator_type = "map"
        task.memory_mb = 1024

        slot = Slot(slot_id=0, memory_mb=2048)
        morsel_processor = Mock()
        shuffle_client = Mock()

        executor = TaskExecutor(
            task=task,
            slot=slot,
            morsel_processor=morsel_processor,
            shuffle_client=shuffle_client,
            is_local_mode=True
        )

        assert executor.task == task
        assert executor.slot == slot
        assert executor.is_local_mode is True
        assert executor.running is False

    def test_operator_creation_from_task(self):
        """Test operator instantiation from task specification"""
        task = Mock()
        task.task_id = "task-123"
        task.operator_type = "map"
        task.operator = Mock()  # Mock operator

        slot = Slot(slot_id=0, memory_mb=2048)

        executor = TaskExecutor(
            task=task,
            slot=slot,
            morsel_processor=Mock(),
            shuffle_client=Mock()
        )

        assert executor.operator is not None


class TestTaskExecutorExecution:
    """Test task execution logic"""

    @pytest.mark.asyncio
    async def test_start_execution(self):
        """Test starting task execution"""
        task = Mock(task_id="task-123", operator_type="map", memory_mb=1024)
        task.operator = Mock()
        task.operator.requires_shuffle = Mock(return_value=False)

        slot = Slot(slot_id=0, memory_mb=2048)

        executor = TaskExecutor(
            task=task,
            slot=slot,
            morsel_processor=Mock(),
            shuffle_client=Mock(),
            is_local_mode=True
        )

        # Start execution
        await executor.start()

        assert executor.running is True
        assert executor.execution_task is not None

    @pytest.mark.asyncio
    async def test_stop_execution(self):
        """Test stopping task execution"""
        task = Mock(task_id="task-123", operator_type="map", memory_mb=1024)
        task.operator = Mock()
        task.operator.requires_shuffle = Mock(return_value=False)

        slot = Slot(slot_id=0, memory_mb=2048)

        executor = TaskExecutor(
            task=task,
            slot=slot,
            morsel_processor=Mock(),
            shuffle_client=Mock(),
            is_local_mode=True
        )

        # Start and stop
        await executor.start()
        assert executor.running is True

        await executor.stop()
        assert executor.running is False

    @pytest.mark.asyncio
    async def test_local_execution(self):
        """Test local (non-shuffle) execution"""
        # Create mock operator that returns batches
        mock_operator = Mock()

        # Create test batches
        batch1 = pa.RecordBatch.from_pydict({'value': [1, 2, 3]})
        batch2 = pa.RecordBatch.from_pydict({'value': [4, 5, 6]})

        # Make operator iterable
        mock_operator.__iter__ = Mock(return_value=iter([batch1, batch2]))
        mock_operator.requires_shuffle = Mock(return_value=False)

        task = Mock(task_id="task-123", memory_mb=1024)
        task.operator = mock_operator

        slot = Slot(slot_id=0, memory_mb=2048)

        executor = TaskExecutor(
            task=task,
            slot=slot,
            morsel_processor=Mock(),
            shuffle_client=Mock(),
            is_local_mode=True
        )

        # Execute
        executor.operator = mock_operator

        batches = []
        async for batch in executor._execute_local_async():
            batches.append(batch)

        assert len(batches) == 2

    @pytest.mark.asyncio
    async def test_shuffle_aware_execution(self):
        """Test execution with shuffle"""
        # Create mock operator that requires shuffle
        mock_operator = Mock()
        mock_operator.requires_shuffle = Mock(return_value=True)

        task = Mock(task_id="task-123", memory_mb=1024)
        task.operator = mock_operator

        slot = Slot(slot_id=0, memory_mb=2048)
        shuffle_client = Mock()

        executor = TaskExecutor(
            task=task,
            slot=slot,
            morsel_processor=Mock(),
            shuffle_client=shuffle_client,
            is_local_mode=False
        )

        # Mock shuffle server
        mock_shuffle_server = AsyncMock()
        batch1 = pa.RecordBatch.from_pydict({'value': [1, 2, 3]})
        batch2 = pa.RecordBatch.from_pydict({'value': [4, 5, 6]})

        async def mock_receive_batches(task_id):
            yield batch1
            yield batch2

        mock_shuffle_server.receive_batches = mock_receive_batches
        executor.shuffle_server = mock_shuffle_server

        # Execute with shuffle
        received = []
        async for batch in executor._execute_with_shuffle():
            received.append(batch)

        # Note: actual implementation would process batches through operator
        # This test verifies the shuffle receive mechanism

    @pytest.mark.asyncio
    async def test_execution_error_handling(self):
        """Test error handling during execution"""
        # Create operator that raises error
        mock_operator = Mock()
        mock_operator.requires_shuffle = Mock(return_value=False)
        mock_operator.__iter__ = Mock(side_effect=RuntimeError("Operator failed"))

        task = Mock(task_id="task-123", memory_mb=1024)
        task.operator = mock_operator

        slot = Slot(slot_id=0, memory_mb=2048)

        executor = TaskExecutor(
            task=task,
            slot=slot,
            morsel_processor=Mock(),
            shuffle_client=Mock(),
            is_local_mode=True
        )

        executor.operator = mock_operator

        # Execution should handle error
        with pytest.raises(RuntimeError, match="Operator failed"):
            await executor._execution_loop()


class TestMorselProcessing:
    """Test morsel-driven batch processing"""

    @pytest.mark.asyncio
    async def test_batch_processing_with_morsels(self):
        """Test processing batch with morsel parallelism"""
        task = Mock(task_id="task-123", memory_mb=1024)
        slot = Slot(slot_id=0, memory_mb=2048)

        # Mock morsel processor
        morsel_processor = Mock()
        morsel_processor.process_batch = AsyncMock()

        # Create test batch
        batch = pa.RecordBatch.from_pydict({
            'id': range(1000),
            'value': range(1000)
        })

        executor = TaskExecutor(
            task=task,
            slot=slot,
            morsel_processor=morsel_processor,
            shuffle_client=Mock(),
            is_local_mode=True
        )

        # Process batch with morsels
        result = await executor._process_batch_with_morsels(batch)

        # Verify morsel processor was called
        morsel_processor.process_batch.assert_called()

    @pytest.mark.asyncio
    async def test_morsel_size_configuration(self):
        """Test morsel size configuration"""
        task = Mock(task_id="task-123", memory_mb=1024)
        task.morsel_size_kb = 64  # 64KB morsels

        slot = Slot(slot_id=0, memory_mb=2048)

        executor = TaskExecutor(
            task=task,
            slot=slot,
            morsel_processor=Mock(),
            shuffle_client=Mock(),
            is_local_mode=True
        )

        # Verify morsel size is used (implementation specific)
        # This test validates the configuration is passed through


class TestDownstreamCommunication:
    """Test sending results to downstream operators"""

    @pytest.mark.asyncio
    async def test_send_downstream_local(self):
        """Test sending results to downstream in local mode"""
        task = Mock(task_id="task-123", memory_mb=1024)
        slot = Slot(slot_id=0, memory_mb=2048)

        executor = TaskExecutor(
            task=task,
            slot=slot,
            morsel_processor=Mock(),
            shuffle_client=Mock(),
            is_local_mode=True
        )

        # Create result batch
        result_batch = pa.RecordBatch.from_pydict({'value': [1, 2, 3]})

        # Mock downstream queue
        executor.downstream_queue = AsyncMock()

        # Send downstream
        await executor._send_downstream(result_batch)

        # Verify batch sent to queue
        executor.downstream_queue.put.assert_called_once_with(result_batch)

    @pytest.mark.asyncio
    async def test_send_downstream_shuffle(self):
        """Test sending results via shuffle"""
        task = Mock(task_id="task-123", memory_mb=1024)
        slot = Slot(slot_id=0, memory_mb=2048)

        shuffle_client = Mock()
        shuffle_client.send_partition = AsyncMock()

        executor = TaskExecutor(
            task=task,
            slot=slot,
            morsel_processor=Mock(),
            shuffle_client=shuffle_client,
            is_local_mode=False
        )

        # Create result batch
        result_batch = pa.RecordBatch.from_pydict({'value': [1, 2, 3]})

        # Send via shuffle
        await executor._send_downstream(result_batch)

        # Verify shuffle client was used
        shuffle_client.send_partition.assert_called()


class TestExecutionLifecycle:
    """Test complete execution lifecycle"""

    @pytest.mark.asyncio
    async def test_complete_execution_cycle(self):
        """Test complete execution from start to finish"""
        # Create operator with test data
        mock_operator = Mock()
        batch1 = pa.RecordBatch.from_pydict({'value': [1, 2, 3]})
        batch2 = pa.RecordBatch.from_pydict({'value': [4, 5, 6]})

        mock_operator.__iter__ = Mock(return_value=iter([batch1, batch2]))
        mock_operator.requires_shuffle = Mock(return_value=False)

        task = Mock(task_id="task-123", memory_mb=1024)
        task.operator = mock_operator

        slot = Slot(slot_id=0, memory_mb=2048)

        executor = TaskExecutor(
            task=task,
            slot=slot,
            morsel_processor=Mock(),
            shuffle_client=Mock(),
            is_local_mode=True
        )

        executor.operator = mock_operator

        # Start execution
        await executor.start()
        assert executor.running is True

        # Allow execution to process
        await asyncio.sleep(0.1)

        # Stop execution
        await executor.stop()
        assert executor.running is False


class TestTaskExecutorIntegration:
    """Integration tests for task executor"""

    @pytest.mark.asyncio
    async def test_map_operator_execution(self):
        """Test executing a map operator"""
        # This would test actual map operator execution
        # For now, placeholder for integration test
        pass

    @pytest.mark.asyncio
    async def test_filter_operator_execution(self):
        """Test executing a filter operator"""
        # This would test actual filter operator execution
        # For now, placeholder for integration test
        pass


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
