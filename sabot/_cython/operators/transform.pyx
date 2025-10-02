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

# Import Arrow for vectorized operations
try:
    import pyarrow as pa
    import pyarrow.compute as pc
    ARROW_AVAILABLE = True
except ImportError:
    ARROW_AVAILABLE = False
    pa = None
    pc = None


# ============================================================================
# Base Operator
# ============================================================================

cdef class BaseOperator:
    """
    Base class for all Cython streaming operators.

    Provides common infrastructure for operator chaining and
    zero-copy batch processing.
    """

    def __cinit__(self, *args, **kwargs):
        """Initialize base operator (allow subclass args)."""
        self._source = kwargs.get('source')
        self._schema = kwargs.get('schema')

    cpdef object process_batch(self, object batch):
        """Process a single RecordBatch. Override in subclasses."""
        return batch

    def __iter__(self):
        """Iterate over source, applying operator to each batch."""
        if self._source is None:
            return

        for batch in self._source:
            result = self.process_batch(batch)
            if result is not None and result.num_rows > 0:
                yield result


# ============================================================================
# Map Operator
# ============================================================================

@cython.final
cdef class CythonMapOperator(BaseOperator):
    """
    Map operator: transform each RecordBatch using a function.

    Supports both:
    1. User-defined Python functions (with optional Numba JIT)
    2. Arrow compute kernel names (e.g., "add", "multiply")

    Examples:
        # Python function
        stream.map(lambda b: pc.multiply(b.column('x'), 2))

        # Numba-accelerated function
        @numba.jit
        def double(x):
            return x * 2
        stream.map(double, vectorized=True)
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
        self._map_func = map_func
        self._vectorized = vectorized

    @cython.boundscheck(False)
    @cython.wraparound(False)
    cpdef object process_batch(self, object batch):
        """Apply map function to batch."""
        if not ARROW_AVAILABLE or batch is None:
            return None

        try:
            # Apply user function
            result = self._map_func(batch)

            # Ensure result is RecordBatch
            if not isinstance(result, pa.RecordBatch):
                # Try to convert
                if isinstance(result, pa.Table):
                    # Combine into single batch if possible
                    result = result.combine_chunks()
                elif isinstance(result, dict):
                    result = pa.RecordBatch.from_pydict(result)
                else:
                    raise TypeError(f"Map function must return RecordBatch, got {type(result)}")

            return result

        except Exception as e:
            raise RuntimeError(f"Error in map operator: {e}")


# ============================================================================
# Filter Operator
# ============================================================================

@cython.final
cdef class CythonFilterOperator(BaseOperator):
    """
    Filter operator: keep only rows matching predicate.

    Uses Arrow's SIMD-accelerated filter kernel for maximum performance.

    The predicate function should return:
    - Arrow boolean Array (preferred - uses SIMD filter)
    - Python boolean (filters entire batch)

    Examples:
        # Arrow compute predicate (SIMD-accelerated)
        stream.filter(lambda b: pc.greater(b.column('price'), 100))

        # Complex predicate
        stream.filter(lambda b: pc.and_(
            pc.greater(b.column('price'), 100),
            pc.equal(b.column('side'), 'BUY')
        ))
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
            # Evaluate predicate
            mask = self._predicate(batch)

            # Handle different mask types
            if isinstance(mask, pa.Array):
                # Arrow boolean array - use SIMD filter
                return batch.filter(mask)
            elif isinstance(mask, bool):
                # Boolean scalar - keep or drop entire batch
                return batch if mask else None
            else:
                raise TypeError(f"Predicate must return Array or bool, got {type(mask)}")

        except Exception as e:
            raise RuntimeError(f"Error in filter operator: {e}")


# ============================================================================
# Select Operator
# ============================================================================

@cython.final
cdef class CythonSelectOperator(BaseOperator):
    """
    Select (project) operator: choose specific columns.

    Uses Arrow's zero-copy column selection for maximum performance.
    This is essentially free - no data is copied.

    Examples:
        stream.select(['user_id', 'amount', 'timestamp'])
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

    The function should return a list/iterable of RecordBatches.

    Examples:
        # Split batch into smaller batches
        def split_batch(batch):
            n = len(batch) // 10
            return [batch.slice(i*n, n) for i in range(10)]
        stream.flatMap(split_batch)
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

    Interleaves batches from multiple source streams.

    Examples:
        stream1.union(stream2, stream3)
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

        try:
            # Try to concatenate into single batch
            # This works if schemas match
            tables = [pa.Table.from_batches([b]) for b in valid_batches]
            combined = pa.concat_tables(tables)
            return combined.to_batches()[0] if combined.num_rows > 0 else None

        except Exception:
            # If concat fails, just return batches separately
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
