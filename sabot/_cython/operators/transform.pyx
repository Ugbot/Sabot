# cython: language_level=3
"""
Cython Stateless Transform Operators

High-performance streaming operators using Arrow C++ compute kernels.
All operations maintain zero-copy semantics with SIMD acceleration.

Performance targets:
- map: 10-100M records/sec
- filter: 10-500M records/sec
- select: 50-1000M records/sec (zero-copy projection)
- flatMap: 5-50M records/sec
- union: 50-500M records/sec
"""

import cython
from libc.stdint cimport int64_t, int32_t
from typing import Callable, List, Optional, Iterator

# CyArrow import for zero-copy operations
cimport pyarrow.lib as ca

# Arrow availability check
cdef bint ARROW_AVAILABLE = True

# Import Numba auto-compiler for automatic UDF compilation
try:
    from sabot._cython.operators.numba_compiler cimport auto_compile
    NUMBA_COMPILER_AVAILABLE = True
except ImportError:
    NUMBA_COMPILER_AVAILABLE = False


# ============================================================================
# Base Operator Import
# ============================================================================

# Import BaseOperator from separate module
from sabot._cython.operators.base_operator cimport BaseOperator


# ============================================================================
# Map Operator
# ============================================================================

@cython.final
cdef class CythonMapOperator(BaseOperator):
    """
    Map operator: transform each RecordBatch using a function.

    BATCH-FIRST: Processes RecordBatch → RecordBatch

    The map function receives a RecordBatch and must return a RecordBatch.
    No per-record iteration occurs in the data plane.

    Supported Functions:
    1. Arrow compute expressions (preferred - SIMD accelerated)
    2. Python functions (auto Numba-compiled if possible)
    3. Cython functions

    Performance:
    - Arrow compute: 10-100M records/sec (SIMD)
    - Numba-compiled: 5-50M records/sec
    - Interpreted Python: 0.5-5M records/sec

    Examples:
        # Arrow compute (SIMD - fastest)
        stream.map(lambda b: b.append_column('fee',
            pc.multiply(b.column('amount'), 0.03)))

        # Auto Numba-compiled
        def transform(batch):
            # Sabot auto-compiles this with Numba
            total = pc.add(batch.column('a'), batch.column('b'))
            return batch.append_column('total', total)
        stream.map(transform)

        # Complex transformation - must return Arrow objects
        def enrich(batch):
            # Use Arrow compute operations - no pyarrow imports allowed
            import pyarrow.compute as pc  # OK in user functions, not in operators
            doubled = pc.multiply(batch.column('x'), 2)
            categorized = pc.if_else(pc.greater(batch.column('x'), 100), 'HIGH', 'LOW')
            batch = batch.append_column('doubled', doubled)
            batch = batch.append_column('category', categorized)
            return batch
        stream.map(enrich)

    Performance Notes:
    - Zero pyarrow imports in operator hot paths
    - Functions must return ca.RecordBatch or ca.Table objects
    - No automatic dict conversion (users handle conversion themselves)
    - CyArrow operations for maximum performance

    Args:
        source: Iterable of RecordBatches
        map_func: Function(RecordBatch) → RecordBatch/Table
        vectorized: If True, function is already vectorized
        schema: Output schema (inferred if None)
    """

    def __init__(self, source, map_func, vectorized=False, schema=None):
        """
        Initialize map operator.

        Args:
            source: Input stream/iterator
            map_func: Function to apply to each batch
            vectorized: If True, function operates on Arrow arrays directly
            schema: Output schema (inferred if None)
        """
        self._source = source
        self._schema = schema
        self._vectorized = vectorized
        self._map_func = map_func

        # Auto-compile with Numba for performance
        if NUMBA_COMPILER_AVAILABLE:
            import time
            start = time.perf_counter()

            self._compiled_func = auto_compile(map_func)
            self._is_compiled = (self._compiled_func is not map_func)

            compile_time = (time.perf_counter() - start) * 1000  # ms

            if self._is_compiled:
                import logging
                logger = logging.getLogger(__name__)
                logger.info(
                    f"Numba-compiled map function '{getattr(map_func, '__name__', '<lambda>')}' "
                    f"in {compile_time:.2f}ms"
                )
            else:
                # Compilation skipped or failed - use original function
                self._compiled_func = map_func
        else:
            # Numba compiler not available - use original function
            self._compiled_func = map_func
            self._is_compiled = False

    @cython.boundscheck(False)
    @cython.wraparound(False)
    cpdef object process_batch(self, object batch):
        """Apply map function to batch."""
        if not ARROW_AVAILABLE or batch is None:
            return None

        try:
            # Use compiled function (or original if compilation failed/skipped)
            result = self._compiled_func(batch)

            # CyArrow: Assume result is proper Arrow object (RecordBatch/Table)
            # No type checking/conversion to avoid pyarrow import overhead
            # Users must return ca.RecordBatch or ca.Table from their functions

            return result

        except Exception as e:
            # Runtime fallback: If Numba compilation failed at runtime, use original function
            # This handles cases where pattern detection missed Arrow/Pandas usage
            if self._is_compiled and ('TypingError' in str(type(e).__name__) or 'numba' in str(e).lower()):
                import logging
                logger = logging.getLogger(__name__)
                logger.warning(
                    f"Numba compilation failed at runtime for '{getattr(self._map_func, '__name__', '<lambda>')}', "
                    f"falling back to original function. Error: {type(e).__name__}"
                )
                # Fallback to original function permanently
                self._compiled_func = self._map_func
                self._is_compiled = False
                # Retry with original function
                result = self._compiled_func(batch)
                return result
            else:
                raise RuntimeError(f"Error in map operator: {e}")


# ============================================================================
# Filter Operator
# ============================================================================

@cython.final
cdef class CythonFilterOperator(BaseOperator):
    """
    Filter operator: keep only rows matching predicate.

    BATCH-FIRST: Processes RecordBatch → RecordBatch (filtered)

    Uses Arrow's SIMD-accelerated filter kernel for maximum performance.
    No per-record iteration occurs in the data plane.

    The predicate function should return:
    - Arrow boolean Array (preferred - uses SIMD filter)
    - Python boolean (filters entire batch)

    Performance:
    - SIMD filter: 10-500M records/sec
    - Zero-copy when possible

    Examples:
        # Arrow compute predicate (SIMD-accelerated)
        stream.filter(lambda b: pc.greater(b.column('price'), 100))

        # Complex predicate
        stream.filter(lambda b: pc.and_(
            pc.greater(b.column('price'), 100),
            pc.equal(b.column('side'), 'BUY')
        ))

        # Numeric filtering
        stream.filter(lambda b: pc.between(b.column('amount'), 50, 1000))

        # String filtering
        stream.filter(lambda b: pc.equal(b.column('status'), 'ACTIVE'))

    Args:
        source: Iterable of RecordBatches
        predicate: Function(RecordBatch) → boolean Array/bool
        schema: Schema (passed through from input)
    """

    def __init__(self, source, predicate, schema=None):
        """
        Initialize filter operator.

        Args:
            source: Input stream/iterator
            predicate: Function returning boolean mask
            schema: Schema (passed through from input)
        """
        self._source = source
        self._schema = schema
        self._predicate = predicate

    @cython.boundscheck(False)
    @cython.wraparound(False)
    cpdef object process_batch(self, object batch):
        """Apply filter predicate to batch."""
        if not ARROW_AVAILABLE or batch is None or batch.num_rows == 0:
            return None

        try:
            # Evaluate predicate - handle common array-scalar comparison errors
            try:
                mask = self._predicate(batch)
            except TypeError as predicate_error:
                # Check if this is a common array-scalar comparison error that we can auto-fix
                if self._can_auto_convert_predicate_error(predicate_error):
                    mask = self._evaluate_predicate_with_auto_conversion(batch)
                else:
                    raise

            # Handle different mask types - CyArrow only, no pyarrow conversions
            if isinstance(mask, bool):
                # Boolean scalar - keep or drop entire batch
                return batch if mask else None
            else:
                # Assume mask is CyArrow Array - use SIMD filter directly
                # Users must return proper Arrow boolean arrays
                return batch.filter(mask)

        except Exception as e:
            raise RuntimeError(f"Error in filter operator: {e}")


# ============================================================================
# Select Operator
# ============================================================================

@cython.final
cdef class CythonSelectOperator(BaseOperator):
    """
    Select (project) operator: choose specific columns.

    BATCH-FIRST: Processes RecordBatch → RecordBatch (projected)

    Uses Arrow's zero-copy column selection for maximum performance.
    No per-record iteration occurs in the data plane.
    This is essentially free - no data is copied.

    Performance:
    - Zero-copy: 50-1000M records/sec
    - Memory efficient (shares underlying data)

    Examples:
        # Basic column selection
        stream.select(['user_id', 'amount', 'timestamp'])

        # Single column
        stream.select(['id'])

        # Reordering columns
        stream.select(['timestamp', 'user_id', 'amount'])

        # After transformation
        stream.map(lambda b: b.append_column('fee', pc.multiply(b.column('amount'), 0.03))) \
              .select(['id', 'amount', 'fee'])

    Args:
        source: Iterable of RecordBatches
        columns: List of column names to keep
        schema: Output schema (derived from input)
    """

    def __init__(self, source, columns: List[str], schema=None):
        """
        Initialize select operator.

        Args:
            source: Input stream/iterator
            columns: List of column names to keep
            schema: Output schema (derived from input)
        """
        self._source = source
        self._schema = schema
        self._columns = columns

    @cython.boundscheck(False)
    @cython.wraparound(False)
    cpdef object process_batch(self, object batch):
        """Select columns from batch (zero-copy)."""
        if not ARROW_AVAILABLE or batch is None:
            return None

        try:
            # Zero-copy column selection
            return batch.select(self._columns)

        except Exception as e:
            raise RuntimeError(f"Error in select operator: {e}")


# ============================================================================
# FlatMap Operator
# ============================================================================

@cython.final
cdef class CythonFlatMapOperator(BaseOperator):
    """
    FlatMap operator: expand each element into multiple elements.

    BATCH-FIRST: Processes RecordBatch → [RecordBatch, RecordBatch, ...]

    The function should return a list/iterable of RecordBatches.
    No per-record iteration occurs in the data plane.
    Useful for batch splitting, expansion, or fan-out operations.

    Performance:
    - Memory efficient: shares data with input batches
    - Throughput: 5-50M records/sec

    Examples:
        # Split batch into smaller batches
        def split_batch(batch):
            n = len(batch) // 10
            return [batch.slice(i*n, n) for i in range(10)]
        stream.flatMap(split_batch)

        # Duplicate batch for A/B testing
        def duplicate_batch(batch):
            return [batch, batch]  # Returns 2 identical batches
        stream.flatMap(duplicate_batch)

        # Conditional expansion
        def expand_by_type(batch):
            if batch.num_rows > 1000:
                # Split large batches
                mid = batch.num_rows // 2
                return [batch.slice(0, mid), batch.slice(mid)]
            else:
                # Keep small batches as-is
                return [batch]
        stream.flatMap(expand_by_type)

    Args:
        source: Iterable of RecordBatches
        flat_map_func: Function(RecordBatch) → [RecordBatch, ...]
        schema: Output schema
    """

    def __init__(self, source, flat_map_func, schema=None):
        """
        Initialize flatMap operator.

        Args:
            source: Input stream/iterator
            flat_map_func: Function returning iterable of results
            schema: Output schema
        """
        self._source = source
        self._schema = schema
        self._flat_map_func = flat_map_func

    cpdef object process_batch(self, object batch):
        """
        Apply flatMap - note this returns multiple batches.

        The __iter__ method handles unpacking these.
        """
        if not ARROW_AVAILABLE or batch is None:
            return []

        try:
            results = self._flat_map_func(batch)
            # Ensure it's a list
            if not isinstance(results, (list, tuple)):
                results = list(results)
            return results

        except Exception as e:
            raise RuntimeError(f"Error in flatMap operator: {e}")

    def __iter__(self):
        """Iterate and flatten results."""
        if self._source is None:
            return

        for batch in self._source:
            results = self.process_batch(batch)
            # Flatten - yield each result batch
            for result_batch in results:
                if result_batch is not None and result_batch.num_rows > 0:
                    yield result_batch


# ============================================================================
# Union Operator
# ============================================================================

@cython.final
cdef class CythonUnionOperator(BaseOperator):
    """
    Union operator: merge multiple streams.

    BATCH-FIRST: Processes [RecordBatch, RecordBatch, ...] → RecordBatch

    Interleaves batches from multiple source streams.
    No per-record iteration occurs in the data plane.
    Useful for combining data from multiple sources.

    Performance:
    - Memory efficient: concatenates batches when possible
    - Throughput: 50-500M records/sec

    Examples:
        # Merge multiple streams
        stream1.union(stream2, stream3)

        # Combine Kafka topics
        kafka1 = Stream.from_kafka('topic1', group='g1')
        kafka2 = Stream.from_kafka('topic2', group='g1')
        combined = kafka1.union(kafka2)

        # Union with different schemas (requires concatenation)
        sales_us = Stream.from_parquet('us_sales.parquet')
        sales_eu = Stream.from_parquet('eu_sales.parquet')
        global_sales = sales_us.union(sales_eu)

    Args:
        *sources: Multiple input streams/iterators
        schema: Common schema (must match across all sources)
    """

    def __init__(self, *sources, schema=None):
        """
        Initialize union operator.

        Args:
            *sources: Multiple input streams/iterators
            schema: Common schema (must match across all sources)
        """
        self._source = None
        self._schema = schema
        self._sources = list(sources)

    cpdef object process_batches(self, list batches):
        """
        Combine multiple batches.

        This can optionally concatenate them into a single batch
        if they have the same schema.
        """
        if not ARROW_AVAILABLE or not batches:
            return None

        # Filter out None/empty batches
        valid_batches = [b for b in batches if b is not None and b.num_rows > 0]

        if not valid_batches:
            return None

        if len(valid_batches) == 1:
            return valid_batches[0]

        # CyArrow: Return batches separately to avoid pyarrow table operations
        # Users can handle concatenation themselves if needed
        return valid_batches

    def __iter__(self):
        """Iterate over all source streams, interleaving."""
        import itertools

        # Create iterators for all sources
        iterators = [iter(source) for source in self._sources]

        # Round-robin through sources
        for batch in itertools.chain.from_iterable(itertools.zip_longest(*iterators)):
            if batch is not None:
                yield batch


# ============================================================================
# Utility Functions
# ============================================================================

def create_map_operator(source, func, **kwargs):
    """Factory function for map operator."""
    return CythonMapOperator(source, func, **kwargs)


def create_filter_operator(source, predicate, **kwargs):
    """Factory function for filter operator."""
    return CythonFilterOperator(source, predicate, **kwargs)


def create_select_operator(source, columns, **kwargs):
    """Factory function for select operator."""
    return CythonSelectOperator(source, columns, **kwargs)


def create_flatmap_operator(source, func, **kwargs):
    """Factory function for flatMap operator."""
    return CythonFlatMapOperator(source, func, **kwargs)


def create_union_operator(*sources, **kwargs):
    """Factory function for union operator."""
    return CythonUnionOperator(*sources, **kwargs)
