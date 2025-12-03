"""
Stage Scheduler for Distributed Query Execution

Orchestrates execution of query stages across distributed agents,
managing shuffle barriers and collecting results.
"""

import asyncio
import logging
from typing import Dict, List, Optional, Any, Callable, Awaitable
from dataclasses import dataclass, field
from enum import Enum

from sabot import cyarrow as ca
from sabot.sql.distributed_plan import (
    DistributedQueryPlan,
    ExecutionStage,
    ShuffleSpec,
    OperatorSpec,
)

logger = logging.getLogger(__name__)


class StageState(Enum):
    """State of a stage in execution."""
    PENDING = "pending"
    RUNNING = "running"
    SHUFFLE_WAIT = "shuffle_wait"
    COMPLETED = "completed"
    FAILED = "failed"


@dataclass
class AgentTask:
    """A task assigned to an agent for stage execution."""
    task_id: str
    stage_id: str
    partition_id: int
    operators: List[OperatorSpec]
    input_shuffle_ids: List[str]
    output_shuffle_id: Optional[str]
    parallelism: int

    # Runtime state
    state: StageState = StageState.PENDING
    result: Optional[ca.Table] = None
    error: Optional[str] = None


@dataclass
class StageExecution:
    """Tracks execution of a single stage."""
    stage: ExecutionStage
    tasks: List[AgentTask] = field(default_factory=list)
    state: StageState = StageState.PENDING
    started_at: Optional[float] = None
    completed_at: Optional[float] = None
    error: Optional[str] = None


class StageScheduler:
    """
    Scheduler for wave-based distributed query execution.

    Manages the execution of query stages in waves, where stages
    within a wave can execute in parallel, but waves execute sequentially.

    Execution Model:
    - Wave 0: Source stages (scan tables, send to shuffle)
    - Wave 1: Intermediate stages (receive shuffle, process, send to shuffle)
    - Wave N: Sink stage (receive shuffle, produce final result)

    Each stage is divided into tasks based on parallelism,
    with one task per partition.
    """

    def __init__(
        self,
        table_registry: Dict[str, ca.Table],
        operator_builder: Callable[[OperatorSpec, ca.Table], Any],
        shuffle_coordinator: Optional['ShuffleCoordinator'] = None,
    ):
        """
        Initialize the stage scheduler.

        Args:
            table_registry: Registered tables by name
            operator_builder: Function to build operators from specs
            shuffle_coordinator: Coordinator for shuffle operations
        """
        self.table_registry = table_registry
        self.operator_builder = operator_builder
        self.shuffle_coordinator = shuffle_coordinator

        # Execution state
        self.plan: Optional[DistributedQueryPlan] = None
        self.stage_executions: Dict[str, StageExecution] = {}
        self.wave_index: int = 0
        self.final_result: Optional[ca.Table] = None

    async def execute(self, plan: DistributedQueryPlan) -> ca.Table:
        """
        Execute a distributed query plan.

        Args:
            plan: Distributed query plan from StagePartitioner

        Returns:
            Final result as Arrow table
        """
        self.plan = plan
        self.wave_index = 0
        self.stage_executions = {}
        self.final_result = None

        logger.info(f"Starting execution of plan with {len(plan.stages)} stages, "
                    f"{len(plan.execution_waves)} waves")

        # Initialize stage executions
        for stage in plan.stages:
            self.stage_executions[stage.stage_id] = StageExecution(stage=stage)

        # Execute waves sequentially
        for wave_idx, wave_stage_ids in enumerate(plan.execution_waves):
            self.wave_index = wave_idx
            logger.info(f"Executing wave {wave_idx}: {wave_stage_ids}")

            # Execute stages in wave concurrently
            tasks = [
                self._execute_stage(stage_id)
                for stage_id in wave_stage_ids
            ]
            await asyncio.gather(*tasks)

            # Check for failures
            for stage_id in wave_stage_ids:
                execution = self.stage_executions[stage_id]
                if execution.state == StageState.FAILED:
                    raise RuntimeError(
                        f"Stage {stage_id} failed: {execution.error}"
                    )

            logger.info(f"Wave {wave_idx} completed")

        # Get final result from sink stage
        sink = plan.get_sink_stage()
        if sink:
            execution = self.stage_executions[sink.stage_id]
            if execution.tasks:
                # Combine results from all sink tasks
                results = [t.result for t in execution.tasks if t.result is not None]
                if results:
                    self.final_result = ca.concat_tables(results)

        if self.final_result is None:
            # Return empty table with expected schema
            schema = ca.schema([
                ca.field(name, ca.string())  # Default to string for unknown types
                for name in plan.output_column_names
            ])
            self.final_result = ca.table({}, schema=schema)

        logger.info(f"Execution completed: {self.final_result.num_rows} rows")
        return self.final_result

    async def _execute_stage(self, stage_id: str) -> None:
        """Execute a single stage."""
        import time

        execution = self.stage_executions[stage_id]
        stage = execution.stage
        execution.state = StageState.RUNNING
        execution.started_at = time.time()

        logger.info(f"Starting stage {stage_id}: {len(stage.operators)} operators, "
                    f"parallelism={stage.parallelism}")

        try:
            # Create tasks for each partition
            for partition_id in range(stage.parallelism):
                task = AgentTask(
                    task_id=f"{stage_id}_p{partition_id}",
                    stage_id=stage_id,
                    partition_id=partition_id,
                    operators=stage.operators,
                    input_shuffle_ids=stage.input_shuffle_ids,
                    output_shuffle_id=stage.output_shuffle_id,
                    parallelism=stage.parallelism,
                )
                execution.tasks.append(task)

            # Execute tasks concurrently
            task_futures = [
                self._execute_task(task, stage)
                for task in execution.tasks
            ]
            await asyncio.gather(*task_futures)

            # Check for task failures
            failed_tasks = [t for t in execution.tasks if t.state == StageState.FAILED]
            if failed_tasks:
                execution.state = StageState.FAILED
                execution.error = f"{len(failed_tasks)} tasks failed"
            else:
                execution.state = StageState.COMPLETED

        except Exception as e:
            execution.state = StageState.FAILED
            execution.error = str(e)
            logger.exception(f"Stage {stage_id} failed: {e}")

        execution.completed_at = time.time()
        duration = execution.completed_at - execution.started_at
        logger.info(f"Stage {stage_id} {execution.state.value} in {duration:.3f}s")

    async def _execute_task(self, task: AgentTask, stage: ExecutionStage) -> None:
        """Execute a single task within a stage."""
        task.state = StageState.RUNNING

        try:
            # Get input data
            if stage.is_source:
                # Source stage: get data from table
                input_data = await self._get_source_data(task, stage)
                shuffle_inputs = {'primary': input_data}
            else:
                # Non-source: get data from shuffle (returns dict of tables)
                shuffle_inputs = await self._receive_shuffle_data(task, stage)
                input_data = shuffle_inputs.get('primary', ca.table({}))

            # Build and execute operator pipeline
            result = await self._execute_operator_pipeline(
                task.operators, input_data, task.partition_id, shuffle_inputs
            )

            # Send output
            if stage.output_shuffle_id:
                # Send to shuffle
                await self._send_to_shuffle(result, stage.output_shuffle_id, task)
            else:
                # Store result (for sink stage)
                task.result = result

            task.state = StageState.COMPLETED

        except Exception as e:
            task.state = StageState.FAILED
            task.error = str(e)
            logger.exception(f"Task {task.task_id} failed: {e}")

    async def _get_source_data(
        self,
        task: AgentTask,
        stage: ExecutionStage
    ) -> ca.Table:
        """Get source data for a scan task."""
        # Find the TableScan operator
        for op in stage.operators:
            if op.type == "TableScan":
                table_name = op.table_name
                if table_name in self.table_registry:
                    table = self.table_registry[table_name]

                    # Partition the table for this task
                    total_rows = table.num_rows
                    rows_per_partition = total_rows // task.parallelism
                    start_row = task.partition_id * rows_per_partition

                    if task.partition_id == task.parallelism - 1:
                        # Last partition gets remaining rows
                        end_row = total_rows
                    else:
                        end_row = start_row + rows_per_partition

                    # Slice the table
                    return table.slice(start_row, end_row - start_row)
                else:
                    raise ValueError(f"Table not found: {table_name}")

        # No TableScan operator - return empty table
        return ca.table({})

    async def _receive_shuffle_data(
        self,
        task: AgentTask,
        stage: ExecutionStage
    ) -> Dict[str, ca.Table]:
        """
        Receive data from shuffle inputs.

        Returns a dict mapping shuffle_id to table for multi-input operators (joins).
        For single input, also includes 'primary' key for backwards compatibility.
        """
        if not self.shuffle_coordinator:
            logger.warning("No shuffle coordinator - returning empty tables")
            return {'primary': ca.table({})}

        # Receive from all input shuffles
        shuffle_tables: Dict[str, ca.Table] = {}
        for shuffle_id in stage.input_shuffle_ids:
            data = await self.shuffle_coordinator.receive(
                shuffle_id, task.partition_id
            )
            shuffle_tables[shuffle_id] = data

        if not shuffle_tables:
            return {'primary': ca.table({})}
        elif len(shuffle_tables) == 1:
            # Single input - set as primary
            shuffle_id = list(shuffle_tables.keys())[0]
            shuffle_tables['primary'] = shuffle_tables[shuffle_id]
            return shuffle_tables
        else:
            # Multiple inputs (for joins) - order them as left/right based on order in input_shuffle_ids
            if len(stage.input_shuffle_ids) >= 2:
                shuffle_tables['left'] = shuffle_tables.get(stage.input_shuffle_ids[0], ca.table({}))
                shuffle_tables['right'] = shuffle_tables.get(stage.input_shuffle_ids[1], ca.table({}))
            # Set primary to left for backwards compatibility
            shuffle_tables['primary'] = shuffle_tables.get('left', ca.table({}))
            return shuffle_tables

    async def _execute_operator_pipeline(
        self,
        operators: List[OperatorSpec],
        input_data: ca.Table,
        partition_id: int,
        shuffle_inputs: Optional[Dict[str, ca.Table]] = None
    ) -> ca.Table:
        """
        Execute a pipeline of operators on input data.

        Args:
            operators: List of operator specs to execute
            input_data: Primary input data (left side for joins)
            partition_id: Partition being processed
            shuffle_inputs: Dict of all shuffle inputs for multi-input operators (joins)
        """
        result = input_data
        shuffle_inputs = shuffle_inputs or {'primary': input_data}

        for op_spec in operators:
            if op_spec.type == "TableScan":
                # Already handled in _get_source_data
                continue

            # Build operator
            operator = self.operator_builder(op_spec, result)
            if operator is None:
                continue

            # Handle multi-input operators (joins, set operations, time series joins)
            multi_input_ops = {
                "HashJoin", "Union", "Intersect", "Except",
                "SemiJoin", "AntiJoin", "CrossJoin",
                "AsofJoin", "IntervalJoin"  # Time series joins
            }

            if op_spec.type in multi_input_ops:
                # These operators need both left and right tables
                left_table = shuffle_inputs.get('left', result)
                right_table = shuffle_inputs.get('right', ca.table({}))

                # Call operator with both tables
                if callable(operator):
                    try:
                        # The operator func expects (left_table, right_table)
                        result = operator(left_table, right_table)
                    except TypeError:
                        # Fallback: maybe operator only expects single input
                        result = operator(left_table)
                elif hasattr(operator, 'execute'):
                    # If operator has execute method, try to pass both
                    result = await self._execute_join_operator(operator, left_table, right_table)
                else:
                    logger.warning(f"{op_spec.type} operator not callable: {type(operator)}")
                    result = left_table
            else:
                # Single-input operator
                result = await self._execute_single_operator(operator, result)

        return result

    async def _execute_join_operator(
        self,
        operator: Any,
        left_table: ca.Table,
        right_table: ca.Table
    ) -> ca.Table:
        """Execute a join operator with left and right inputs."""
        # Check for CythonHashJoinOperator pattern
        if hasattr(operator, 'set_inputs'):
            operator.set_inputs(left_table, right_table)
            if hasattr(operator, 'execute_async'):
                return await operator.execute_async()
            else:
                loop = asyncio.get_event_loop()
                return await loop.run_in_executor(None, operator.execute)
        elif hasattr(operator, 'execute') and callable(operator.execute):
            # Try passing both tables
            loop = asyncio.get_event_loop()
            try:
                return await loop.run_in_executor(None, operator.execute, left_table, right_table)
            except TypeError:
                return await loop.run_in_executor(None, operator.execute, left_table)
        else:
            return left_table

    async def _execute_single_operator(
        self,
        operator: Any,
        input_data: ca.Table
    ) -> ca.Table:
        """Execute a single operator."""
        # Check if operator has async execute method
        if hasattr(operator, 'execute_async'):
            return await operator.execute_async(input_data)
        elif hasattr(operator, 'execute'):
            # Wrap sync execution in asyncio
            loop = asyncio.get_event_loop()
            return await loop.run_in_executor(
                None, operator.execute, input_data
            )
        elif callable(operator):
            return operator(input_data)
        else:
            # Passthrough if operator is not executable
            return input_data

    async def _send_to_shuffle(
        self,
        data: ca.Table,
        shuffle_id: str,
        task: AgentTask
    ) -> None:
        """Send data to shuffle output."""
        if not self.shuffle_coordinator:
            logger.warning("No shuffle coordinator - data not sent")
            return

        await self.shuffle_coordinator.send(
            shuffle_id, task.partition_id, data
        )


class ShuffleCoordinator:
    """
    Coordinator for shuffle operations between stages.

    Manages the lifecycle of shuffles:
    1. Registration (before execution)
    2. Send/Receive (during execution)
    3. Cleanup (after execution)
    """

    def __init__(self, shuffles: Dict[str, ShuffleSpec]):
        """
        Initialize shuffle coordinator.

        Args:
            shuffles: Shuffle specifications from the plan
        """
        self.shuffles = shuffles
        # In-memory shuffle buffers (for local execution)
        # Key: (shuffle_id, partition_id), Value: list of batches
        self.buffers: Dict[tuple, List[ca.Table]] = {}
        # Events for coordinating producers and consumers
        self.ready_events: Dict[str, asyncio.Event] = {}

        # Initialize buffers and events
        for shuffle_id, spec in shuffles.items():
            for partition_id in range(spec.num_partitions):
                self.buffers[(shuffle_id, partition_id)] = []
            self.ready_events[shuffle_id] = asyncio.Event()

    async def send(
        self,
        shuffle_id: str,
        producer_partition: int,
        data: ca.Table
    ) -> None:
        """
        Send data to a shuffle.

        Partitions the data and adds to appropriate buffers.
        """
        spec = self.shuffles.get(shuffle_id)
        if not spec:
            raise ValueError(f"Unknown shuffle: {shuffle_id}")

        # Hash partition the data
        partitioned = self._hash_partition(
            data, spec.partition_keys, spec.num_partitions
        )

        # Add to buffers
        for partition_id, partition_data in enumerate(partitioned):
            if partition_data.num_rows > 0:
                self.buffers[(shuffle_id, partition_id)].append(partition_data)

        logger.debug(f"Sent {data.num_rows} rows to shuffle {shuffle_id} "
                     f"from producer partition {producer_partition}")

    async def receive(
        self,
        shuffle_id: str,
        consumer_partition: int
    ) -> ca.Table:
        """
        Receive data from a shuffle for a specific partition.
        """
        # Wait for shuffle to be ready (all producers done)
        # In a real implementation, this would wait for all producers
        # For now, just get what's available

        key = (shuffle_id, consumer_partition)
        batches = self.buffers.get(key, [])

        if not batches:
            # Return empty table
            return ca.table({})

        # Combine all batches
        result = ca.concat_tables(batches)
        logger.debug(f"Received {result.num_rows} rows from shuffle {shuffle_id} "
                     f"for consumer partition {consumer_partition}")
        return result

    def _hash_partition(
        self,
        data: ca.Table,
        partition_keys: List[str],
        num_partitions: int
    ) -> List[ca.Table]:
        """Hash partition data by keys."""
        if num_partitions == 1:
            return [data]

        if not partition_keys or data.num_rows == 0:
            # No keys or empty - send all to partition 0
            empty = [ca.table({}, schema=data.schema) for _ in range(num_partitions)]
            empty[0] = data
            return empty

        # Use Cython hash partitioner with partition_table method
        try:
            from sabot._cython.shuffle.hash_partitioner import ArrowHashPartitioner

            partitioner = ArrowHashPartitioner(partition_keys, num_partitions)
            return partitioner.partition_table(data)

        except Exception as e:
            logger.debug(f"Cython partitioner unavailable: {e}, using fallback")
            return self._arrow_hash_partition(data, partition_keys, num_partitions)

    def _arrow_hash_partition(
        self,
        data: ca.Table,
        partition_keys: List[str],
        num_partitions: int
    ) -> List[ca.Table]:
        """Hash partition using Arrow compute directly."""
        import pyarrow.compute as pc
        import pyarrow as pa

        # Get the partition key columns
        key_cols = []
        for key in partition_keys:
            if key in data.column_names:
                key_cols.append(data.column(key))

        if not key_cols:
            # No valid keys - put all in partition 0
            empty = [ca.table({}, schema=data.schema) for _ in range(num_partitions)]
            empty[0] = data
            return empty

        # Compute partition ID for each row using Python hash
        # This is slower but works reliably
        partition_ids = []
        for row_idx in range(data.num_rows):
            # Hash the key values for this row
            hash_val = 0
            for col in key_cols:
                val = col[row_idx].as_py()
                hash_val ^= hash(val) if val is not None else 0
            partition_ids.append(hash_val % num_partitions)

        # Convert to Arrow array
        partition_id_array = pa.array(partition_ids, type=pa.int32())

        # Split into partitions
        result = []
        for i in range(num_partitions):
            mask = pc.equal(partition_id_array, i)
            partition = data.filter(mask)
            result.append(partition)

        return result

    def _round_robin_partition(
        self,
        data: ca.Table,
        num_partitions: int
    ) -> List[ca.Table]:
        """Round-robin partition data."""
        rows_per_partition = data.num_rows // num_partitions
        partitions = []

        for i in range(num_partitions):
            start = i * rows_per_partition
            if i == num_partitions - 1:
                # Last partition gets remaining rows
                partition = data.slice(start, data.num_rows - start)
            else:
                partition = data.slice(start, rows_per_partition)
            partitions.append(partition)

        return partitions

    def mark_shuffle_ready(self, shuffle_id: str) -> None:
        """Mark a shuffle as ready (all producers done)."""
        if shuffle_id in self.ready_events:
            self.ready_events[shuffle_id].set()

    def reset(self) -> None:
        """Reset coordinator for new execution."""
        for key in self.buffers:
            self.buffers[key] = []
        for event in self.ready_events.values():
            event.clear()
