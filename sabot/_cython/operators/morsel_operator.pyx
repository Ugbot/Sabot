# cython: language_level=3, boundscheck=False, wraparound=False, cdivision=True, nonecheck=False
"""
Morsel-Driven Operator Wrapper

Automatically splits batches into cache-friendly morsels and processes them
either locally (C++ threads) or via network shuffle (Arrow Flight).

Decision logic:
- Small batches (<10K rows) → direct execution (no overhead)
- Stateless operators (map, filter) → local C++ parallel execution
- Stateful operators (join, groupBy) → network shuffle via Arrow Flight
"""

import cython
from sabot._cython.operators.base_operator cimport BaseOperator

# Constants
DEF MIN_BATCH_SIZE_FOR_MORSEL = 10000  # Only use morsels for batches >= 10K rows
DEF DEFAULT_MORSEL_SIZE_KB = 64


cdef class MorselDrivenOperator(BaseOperator):
    """
    Wrapper that enables morsel-driven parallel execution for any operator.

    Usage:
        # Wrap any operator
        map_op = CythonMapOperator(source, map_func)
        parallel_map = MorselDrivenOperator(map_op, num_workers=8)

        # Process batches - automatically uses morsel parallelism
        for batch in parallel_map:
            process(batch)
    """

    def __cinit__(
        self,
        object wrapped_operator,
        int num_workers = 0,
        long long morsel_size_kb = DEFAULT_MORSEL_SIZE_KB,
        bint enabled = True,
        **kwargs
    ):
        """
        Initialize morsel-driven operator wrapper.

        Args:
            wrapped_operator: Operator to wrap for parallel execution
            num_workers: Number of workers (0 = auto-detect)
            morsel_size_kb: Size of each morsel in KB
            enabled: Enable morsel execution (False = passthrough)
        """
        self._wrapped_operator = wrapped_operator
        self._num_workers = num_workers if num_workers > 0 else 4  # Default to 4 workers
        self._morsel_size_kb = morsel_size_kb
        self._enabled = enabled

        # Shuffle client for network morsels
        self._shuffle_client = None

        # Copy source and schema from wrapped operator
        # Use get_source() method since _source is a cdef attribute not accessible via getattr
        if hasattr(wrapped_operator, 'get_source'):
            self._source = wrapped_operator.get_source()
        else:
            self._source = getattr(wrapped_operator, '_source', kwargs.get('source'))

        if hasattr(wrapped_operator, 'get_schema'):
            self._schema = wrapped_operator.get_schema()
        else:
            self._schema = getattr(wrapped_operator, '_schema', kwargs.get('schema'))

        # Copy stateful metadata
        self._stateful = getattr(wrapped_operator, '_stateful', False)
        self._key_columns = getattr(wrapped_operator, '_key_columns', None)
        self._parallelism_hint = num_workers if num_workers > 0 else 1

    def __dealloc__(self):
        """Clean up executors."""
        # No cleanup needed (ThreadPoolExecutor is context-managed)
        pass

    cpdef bint should_use_morsel_execution(self, object batch):
        """
        Determine if morsel execution should be used for this batch.

        Heuristics:
        - Batch must have >= MIN_BATCH_SIZE_FOR_MORSEL rows
        - Morsel execution must be enabled
        - Must have multiple workers available

        Args:
            batch: RecordBatch to check

        Returns:
            True if morsel execution should be used
        """
        if not self._enabled:
            return False

        if self._num_workers <= 1:
            return False

        if batch is None or batch.num_rows < MIN_BATCH_SIZE_FOR_MORSEL:
            return False

        return True

    @cython.boundscheck(False)
    @cython.wraparound(False)
    cpdef object process_batch(self, object batch):
        """
        Process batch with optimal execution strategy.

        Decision tree:
        1. If batch is small (<10K rows) → direct processing (no overhead)
        2. If operator is stateful (join, groupBy) → network shuffle (Arrow Flight)
        3. If operator is stateless (map, filter) → local parallel (C++ threads)

        Args:
            batch: RecordBatch to process

        Returns:
            Processed RecordBatch
        """
        if batch is None:
            return None

        # Small batch - direct execution (no overhead)
        if not self.should_use_morsel_execution(batch):
            return self._wrapped_operator.process_batch(batch)

        # Large batch - decide LOCAL vs NETWORK
        if self._wrapped_operator.requires_shuffle():
            # Stateful operator (join, groupBy, window)
            # Needs network shuffle to co-locate keys across agents
            return self._process_with_network_shuffle(batch)
        else:
            # Stateless operator (map, filter, select)
            # Can process locally in parallel using C++ threads
            return self._process_with_local_morsels(batch)

    cdef object _process_with_local_morsels(self, object batch):
        """
        Process batch locally using TaskSlotManager (C++ work-stealing threads).

        Stateless operators (map, filter, select) process morsels in parallel
        using the agent's shared task slot pool (unified local + network processing).

        Steps:
        1. Get shared task slot manager from AgentContext
        2. Create Morsel objects (not tuples!) with proper attributes
        3. Submit morsels to task slot manager (releases GIL!)
        4. Workers process in parallel with work-stealing
        5. Reassemble results in order

        Returns:
            Processed RecordBatch
        """
        from sabot import cyarrow as pa

        # Try to use TaskSlotManager (C++ work-stealing)
        try:
            from sabot.cluster.agent_context import get_or_create_slot_manager

            # Get shared task slot manager (auto-initializes if needed)
            task_slot_manager = get_or_create_slot_manager(num_slots=self._num_workers)

            # Calculate rows per morsel
            morsel_size_bytes = self._morsel_size_kb * 1024
            avg_bytes_per_row = 100  # Conservative estimate
            rows_per_morsel = max(1000, morsel_size_bytes // avg_bytes_per_row)

            # Create morsel tuples (batch, start_row, num_rows, partition_id)
            morsels = []
            for start_row in range(0, batch.num_rows, rows_per_morsel):
                num_rows = min(rows_per_morsel, batch.num_rows - start_row)
                # TaskSlotManager expects tuples: (batch, start_row, num_rows, partition_id)
                morsels.append((batch, start_row, num_rows, 0))

            # If only one morsel, process directly
            if len(morsels) <= 1:
                return self._wrapped_operator.process_batch(batch)

            # Create callback that processes morsel slices
            def process_morsel_callback(morsel_batch):
                """Process single morsel using wrapped operator."""
                return self._wrapped_operator.process_batch(morsel_batch)

            # Execute using task slot manager (releases GIL, C++ threads!)
            result_batches = task_slot_manager.execute_morsels(morsels, process_morsel_callback)

            # Filter out None results
            result_batches = [rb for rb in result_batches if rb is not None and rb.num_rows > 0]

            # Reassemble results
            if not result_batches:
                return None

            if len(result_batches) == 1:
                return result_batches[0]

            # Multiple batches - concatenate
            table = pa.Table.from_batches(result_batches)
            combined_table = table.combine_chunks()
            batches = combined_table.to_batches()
            return batches[0] if batches else None

        except Exception as e:
            # Fallback to ThreadPoolExecutor if TaskSlotManager unavailable
            import logging
            import concurrent.futures
            logging.warning(f"TaskSlotManager unavailable ({e}), falling back to ThreadPoolExecutor")

            # Calculate rows per morsel
            morsel_size_bytes = self._morsel_size_kb * 1024
            avg_bytes_per_row = 100
            rows_per_morsel = max(1000, morsel_size_bytes // avg_bytes_per_row)

            # Split batch into slices
            morsel_slices = []
            for start_row in range(0, batch.num_rows, rows_per_morsel):
                end_row = min(start_row + rows_per_morsel, batch.num_rows)
                morsel_slices.append((start_row, end_row))

            # If only one morsel, process directly
            if len(morsel_slices) <= 1:
                return self._wrapped_operator.process_batch(batch)

            # Process morsels with ThreadPoolExecutor fallback
            def process_morsel(slice_info):
                start_row, end_row = slice_info
                morsel_batch = batch.slice(offset=start_row, length=(end_row - start_row))
                return self._wrapped_operator.process_batch(morsel_batch)

            with concurrent.futures.ThreadPoolExecutor(max_workers=self._num_workers) as executor:
                result_batches = list(executor.map(process_morsel, morsel_slices))

            # Filter and reassemble
            result_batches = [rb for rb in result_batches if rb is not None and rb.num_rows > 0]

            if not result_batches:
                return None
            if len(result_batches) == 1:
                return result_batches[0]

            table = pa.Table.from_batches(result_batches)
            combined_table = table.combine_chunks()
            batches = combined_table.to_batches()
            return batches[0] if batches else None

    cdef object _process_with_network_shuffle(self, object batch):
        """
        Process batch via network shuffle using Arrow Flight.

        Stateful operators (join, groupBy, window) require shuffling data
        across the network to co-locate records by key.

        Steps:
        1. Partition batch by key (hash partitioning)
        2. Send partitions to remote agents via Arrow Flight
        3. Remote agents process their partitions
        4. Collect results

        TODO: This currently uses Python asyncio. Migrate to C++ shuffle executor.

        Returns:
            Processed RecordBatch (after network shuffle)
        """
        # For now, fall back to Python-based shuffle
        # TODO: Replace with C++ shuffle executor
        import asyncio

        # Get or create event loop
        try:
            loop = asyncio.get_event_loop()
        except RuntimeError:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)

        # Process with network shuffle (async)
        if loop.is_running():
            future = asyncio.ensure_future(
                self._async_process_with_network_shuffle(batch)
            )
            return future  # Caller must await
        else:
            return loop.run_until_complete(
                self._async_process_with_network_shuffle(batch)
            )

    async def _async_process_with_network_shuffle(self, batch):
        """
        Async implementation of network shuffle.

        Uses existing Arrow Flight infrastructure for zero-copy network transfer.
        """
        # Initialize shuffle client if needed
        if self._shuffle_client is None:
            from sabot._cython.shuffle.shuffle_transport import ShuffleClient
            self._shuffle_client = ShuffleClient()

        # Get partition keys from operator
        partition_keys = self._wrapped_operator.get_partition_keys()
        if not partition_keys:
            # No keys - broadcast to all agents
            # For now, just process locally
            return self._wrapped_operator.process_batch(batch)

        # Partition batch by keys using hash partitioner
        from sabot._cython.shuffle.partitioner import HashPartitioner
        partitioner = HashPartitioner(partition_keys)
        partitions = partitioner.partition_batch(batch)

        # TODO: Send partitions via Arrow Flight to remote agents
        # For now, just process locally
        # This will be implemented when we have agent coordination

        return self._wrapped_operator.process_batch(batch)

    async def _async_process_with_morsels(self, batch):
        """
        Async implementation of morsel-driven processing.

        This method is called by tests and provides a unified async interface
        for morsel processing (either local or network-based).

        Delegates to appropriate processing strategy:
        - Stateful operators → network shuffle
        - Stateless operators → local parallel execution (C++ handles threading)

        Args:
            batch: RecordBatch to process

        Returns:
            Processed RecordBatch
        """
        if batch is None:
            return None

        # Decide processing strategy
        if self._wrapped_operator.requires_shuffle():
            # Stateful operator - use network shuffle
            return await self._async_process_with_network_shuffle(batch)
        else:
            # Stateless operator - use local morsels
            # Just call sync method directly - C++ TaskSlotManager handles all threading
            # No need for asyncio wrapper - that causes triple-threading conflicts
            return self._process_with_local_morsels(batch)

    def __iter__(self):
        """
        Iterate over batches from source and process with morsel execution.

        CRITICAL: Must iterate self._source (from wrapped operator), not assume
        self._source is set on this wrapper. This enables lazy/streaming sources.
        """
        # Get source from wrapped operator
        # Use get_source() method since _source is a cdef attribute not accessible via getattr
        source = self._source
        if source is None:
            if hasattr(self._wrapped_operator, 'get_source'):
                source = self._wrapped_operator.get_source()
            else:
                source = getattr(self._wrapped_operator, '_source', None)

        if source is None:
            return

        # Iterate and process each batch
        for batch in source:
            result = self.process_batch(batch)
            if result is not None and result.num_rows > 0:
                yield result

    def get_stats(self):
        """Get morsel processing statistics."""
        stats = {
            'num_workers': self._num_workers,
            'morsel_size_kb': self._morsel_size_kb,
            'enabled': self._enabled,
            'execution_mode': 'task_slot_manager'
        }

        # Try to get task slot manager stats
        try:
            from sabot.cluster.agent_context import get_agent_context
            ctx = get_agent_context()
            if ctx.task_slot_manager is not None:
                slot_stats = ctx.task_slot_manager.get_slot_stats()
                stats.update({
                    'num_slots': ctx.task_slot_manager.num_slots,
                    'available_slots': ctx.task_slot_manager.available_slots,
                    'queue_depth': ctx.task_slot_manager.queue_depth,
                    'slot_stats': slot_stats
                })
        except:
            # TaskSlotManager not available - using ThreadPoolExecutor fallback
            stats['execution_mode'] = 'thread_pool_executor (fallback)'

        return stats
