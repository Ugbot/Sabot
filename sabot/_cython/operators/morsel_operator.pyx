# cython: language_level=3, boundscheck=False, wraparound=False, cdivision=True, nonecheck=False
"""
Morsel-Driven Operator Wrapper

Automatically splits batches into cache-friendly morsels and processes them
in parallel using work-stealing for load balancing.
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
        self._num_workers = num_workers
        self._morsel_size_kb = morsel_size_kb
        self._enabled = enabled
        self._parallel_processor = None

        # Copy source and schema from wrapped operator
        self._source = getattr(wrapped_operator, '_source', kwargs.get('source'))
        self._schema = getattr(wrapped_operator, '_schema', kwargs.get('schema'))

        # Copy stateful metadata
        self._stateful = getattr(wrapped_operator, '_stateful', False)
        self._key_columns = getattr(wrapped_operator, '_key_columns', None)
        self._parallelism_hint = num_workers if num_workers > 0 else 1

    def __dealloc__(self):
        """Clean up parallel processor."""
        if self._parallel_processor is not None:
            import asyncio
            try:
                loop = asyncio.get_event_loop()
                if loop.is_running():
                    asyncio.create_task(self._parallel_processor.stop())
                else:
                    loop.run_until_complete(self._parallel_processor.stop())
            except:
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
        Process batch with optional morsel parallelism.

        Decision tree:
        1. If batch is small → direct processing (no overhead)
        2. If batch is large → split into morsels → parallel processing

        Args:
            batch: RecordBatch to process

        Returns:
            Processed RecordBatch
        """
        if batch is None:
            return None

        # Check if we should use morsel execution
        if not self.should_use_morsel_execution(batch):
            # Small batch or morsel disabled - direct processing
            return self._wrapped_operator.process_batch(batch)

        # Large batch - use morsel-driven parallelism
        return self._process_batch_with_morsels(batch)

    cpdef object _process_batch_with_morsels(self, object batch):
        """
        Process batch by splitting into morsels and executing in parallel.

        Steps:
        1. Initialize parallel processor (lazy)
        2. Split batch into morsels
        3. Submit morsels to workers
        4. Collect results
        5. Reassemble into batch
        """
        import asyncio

        # Lazy initialization of parallel processor
        if self._parallel_processor is None:
            from sabot.morsel_parallelism import ParallelProcessor
            self._parallel_processor = ParallelProcessor(
                num_workers=self._num_workers,
                morsel_size_kb=self._morsel_size_kb
            )

        # Get or create event loop
        try:
            loop = asyncio.get_event_loop()
        except RuntimeError:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)

        # Process with morsels (async)
        if loop.is_running():
            # We're in an async context - create task
            future = asyncio.ensure_future(
                self._async_process_with_morsels(batch)
            )
            return future  # Caller must await
        else:
            # Sync context - run until complete
            return loop.run_until_complete(
                self._async_process_with_morsels(batch)
            )

    async def _async_process_with_morsels(self, batch):
        """
        Async implementation of morsel processing.

        Args:
            batch: RecordBatch to process

        Returns:
            Reassembled RecordBatch
        """
        # Start processor if needed
        if not self._parallel_processor._running:
            await self._parallel_processor.start()

        # Create processing function that calls wrapped operator
        async def process_morsel_func(morsel):
            """Process a single morsel using wrapped operator."""
            return self._wrapped_operator.process_morsel(morsel)

        # Process batch with morsels
        results = await self._parallel_processor.process_with_function(
            data=batch,
            processor_func=process_morsel_func,
            partition_id=0
        )

        # Reassemble results
        if not results:
            return None

        # Extract batches from morsel results
        result_batches = []
        for result in results:
            if hasattr(result, 'data'):
                # It's a morsel
                if result.data is not None:
                    result_batches.append(result.data)
            elif hasattr(result, '__class__') and 'RecordBatch' in str(result.__class__):
                # Already a batch
                result_batches.append(result)

        if not result_batches:
            return None

        # Concatenate batches
        if len(result_batches) == 1:
            return result_batches[0]

        # Multiple batches - concatenate using Arrow
        import pyarrow as pa
        table = pa.Table.from_batches(result_batches)
        return table.combine_chunks()

    def get_stats(self):
        """Get morsel processing statistics."""
        if self._parallel_processor is None:
            return {'morsel_execution': 'not_initialized'}

        return self._parallel_processor.get_stats()
