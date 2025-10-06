# cython: language_level=3, boundscheck=False, wraparound=False, cdivision=True, nonecheck=False
"""
Base Operator for Sabot Streaming Engine

All operators inherit from BaseOperator and implement:
- process_batch(): Transform RecordBatch → RecordBatch
- process_morsel(): (Optional) Transform Morsel → Morsel for parallelism
"""

import cython
from typing import Iterator, Optional
from libc.stdint cimport int64_t

# Import CyArrow for type checking
cimport sabot._c.arrow_core as ca


cdef class BaseOperator:
    """
    Base class for all streaming operators.

    Contract:
    - Operators process RecordBatches (Arrow columnar format)
    - __iter__() always yields batches, never records
    - Operators are composable via chaining
    - Operators can be morsel-aware for parallelism
    - Operators declare if they need shuffle
    """

    def __cinit__(self, *args, **kwargs):
        """Initialize operator (allow subclass args)"""
        self._source = kwargs.get('source')
        self._schema = kwargs.get('schema')
        self._stateful = False
        self._key_columns = None
        self._parallelism_hint = 1

    # ========================================================================
    # ITERATION INTERFACE
    # ========================================================================

    def __iter__(self) -> Iterator:
        """
        Synchronous iteration over batches (batch mode).

        Use for finite sources (files, tables).
        Terminates when source exhausted.
        """
        if self._source is None:
            return

        for batch in self._source:
            result = self.process_batch(batch)
            if result is not None and result.num_rows > 0:
                yield result

    async def __aiter__(self):
        """
        Asynchronous iteration over batches (streaming mode).

        Use for infinite sources (Kafka, sockets).
        Runs forever until externally interrupted.
        """
        if self._source is None:
            return

        # Check if source is async
        if hasattr(self._source, '__aiter__'):
            # Async source (Kafka, etc)
            async for batch in self._source:
                result = self.process_batch(batch)
                if result is not None and result.num_rows > 0:
                    yield result
        else:
            # Sync source wrapped in async
            for batch in self._source:
                result = self.process_batch(batch)
                if result is not None and result.num_rows > 0:
                    yield result

    # ========================================================================
    # BATCH PROCESSING INTERFACE
    # ========================================================================

    cpdef object process_batch(self, object batch):
        """
        Process a single RecordBatch.

        Args:
            batch: PyArrow RecordBatch

        Returns:
            Transformed RecordBatch or None (for filtering)

        Override this in subclasses for actual logic.
        """
        return batch

    # ========================================================================
    # MORSEL-AWARE INTERFACE (for parallelism)
    # ========================================================================

    cpdef object process_morsel(self, object morsel):
        """
        Process a morsel (chunk of batch for parallel execution).

        Default implementation: extract batch from morsel, process it,
        and put result back in morsel.

        Override for morsel-specific optimizations:
        - NUMA-aware placement
        - Cache-aware access patterns
        - Vectorization hints

        Args:
            morsel: Morsel object containing RecordBatch data

        Returns:
            Processed Morsel or None
        """
        # Default: just process the batch inside the morsel
        if not hasattr(morsel, 'data'):
            # Not a proper morsel, try to process directly
            return self.process_batch(morsel)

        result_batch = self.process_batch(morsel.data)

        if result_batch is not None:
            # Update morsel with result
            morsel.data = result_batch
            if hasattr(morsel, 'mark_processed'):
                morsel.mark_processed()
            return morsel

        return None

    # ========================================================================
    # SHUFFLE INTERFACE (for distributed execution)
    # ========================================================================

    cpdef bint requires_shuffle(self):
        """
        Does this operator require network shuffle (repartitioning)?

        Returns True for stateful operators (joins, groupBy, windows).
        Shuffle is needed to co-locate data by key across the cluster.
        """
        return self._stateful

    cpdef list get_partition_keys(self):
        """
        Get columns to partition by for shuffle.

        Returns:
            List of column names for hash partitioning.
            None if no shuffle needed.
        """
        return self._key_columns if self._stateful else None

    cpdef int get_parallelism_hint(self):
        """
        Suggest parallelism for this operator.

        Used by scheduler to determine number of parallel tasks.
        """
        return self._parallelism_hint

    # ========================================================================
    # METADATA INTERFACE
    # ========================================================================

    cpdef object get_schema(self):
        """Get output schema (Arrow Schema object)"""
        return self._schema

    cpdef bint is_stateful(self):
        """Is this a stateful operator?"""
        return self._stateful

    cpdef str get_operator_name(self):
        """Get operator name for logging/debugging"""
        return self.__class__.__name__
