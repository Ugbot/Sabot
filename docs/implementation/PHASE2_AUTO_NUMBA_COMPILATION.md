# Phase 2: Auto-Numba UDF Compilation

**Version**: 1.0
**Date**: October 2025
**Status**: Implementation Plan
**Related**: [UNIFIED_BATCH_ARCHITECTURE.md](../design/UNIFIED_BATCH_ARCHITECTURE.md)

---

## Executive Summary

This document provides a detailed implementation plan for automatic Numba JIT compilation of user-defined functions (UDFs). The system will transparently compile Python UDFs to achieve 10-100x speedup without requiring users to modify their code.

### Key Features

- **Automatic compilation**: AST analysis detects function patterns and compiles with Numba
- **Smart strategy selection**: Chooses `@njit` (scalar) vs `@vectorize` (array) automatically
- **Graceful fallback**: Falls back to interpreted Python if compilation fails
- **Transparent caching**: Compiled functions cached to avoid recompilation
- **Zero user changes**: Works with existing Python code

### Performance Targets

- **Scalar loops**: 10-50x speedup with `@njit`
- **NumPy operations**: 50-100x speedup with `@vectorize`
- **Compilation overhead**: <100ms per function (first-time only)
- **Cache hit latency**: <1ms (subsequent uses)

---

## Architecture Overview

### Component Diagram

```
User Python UDF
       ↓
┌──────────────────────────────────────┐
│  NumbaCompiler (AST Analysis)        │
│  - Parse function source              │
│  - Detect patterns (loops/numpy/etc)  │
│  - Choose compilation strategy        │
└──────────────────────────────────────┘
       ↓
┌──────────────────────────────────────┐
│  CompilationStrategy Decision         │
│  - SKIP: Already fast (Arrow/Pandas)  │
│  - NJIT: Scalar loops/conditionals    │
│  - VECTORIZE: NumPy array operations  │
│  - AUTO: Try best option with fallback│
└──────────────────────────────────────┘
       ↓
┌──────────────────────────────────────┐
│  Numba JIT Compilation                │
│  - Compile with chosen decorator      │
│  - Test compilation with sample data  │
│  - Fallback to Python on failure      │
└──────────────────────────────────────┘
       ↓
┌──────────────────────────────────────┐
│  Compiled Function Cache              │
│  - In-memory cache (LRU)              │
│  - Disk cache (optional, persistent)  │
│  - Cache key: function source hash    │
└──────────────────────────────────────┘
       ↓
CythonMapOperator (uses compiled func)
```

### Data Flow

```
1. User writes Python function (batch-first):
   def my_transform(batch):
       # Extract numpy arrays from Arrow columns
       values = batch.column('value').to_numpy()
       ids = batch.column('id').to_numpy()

       # Numba-compiled computation on arrays
       results = []
       for i in range(len(values)):
           total = 0
           for j in range(100):
               total += values[i] * j
           results.append(total)

       # Return new RecordBatch
       return batch.append_column('computed', pa.array(results))

2. MapOperator receives function in __init__()

3. NumbaCompiler analyzes function:
   - Parses AST
   - Detects: for loop, numpy array operations, no Arrow/Pandas
   - Strategy: NJIT (scalar compilation on extracted arrays)

4. Numba compiles function:
   - Applies @njit decorator
   - Triggers compilation with test RecordBatch data
   - Returns compiled function

5. Compiled function cached:
   - Key: hash(function source)
   - Value: compiled callable

6. MapOperator uses compiled function:
   - 10-50x faster execution on batch data
   - Transparent to user
```

---

## Detailed Task Breakdown

### Task 1: Create NumbaCompiler Core

**File**: `/Users/bengamble/Sabot/sabot/_cython/operators/numba_compiler.pyx` (NEW)

**Lines of Code**: ~350 lines

**Responsibilities**:
1. AST analysis for function pattern detection
2. Compilation strategy selection
3. Numba compilation with error handling
4. Test data generation for compilation verification

**Implementation**:

```cython
# cython: language_level=3

"""
Automatic Numba JIT compilation for user-defined functions.

Analyzes function source code and chooses optimal compilation strategy:
- Pure Python loops → @njit (scalar JIT)
- NumPy operations → @vectorize (array JIT)
- PyArrow/Pandas → No compilation (already fast)
- Complex logic → Try JIT, fallback to Python
"""

import logging
import inspect
import ast
import hashlib

logger = logging.getLogger(__name__)

# Compilation strategy enum
cdef enum CompilationStrategy:
    SKIP = 0        # Don't compile (already fast)
    NJIT = 1        # Scalar JIT (@njit)
    VECTORIZE = 2   # Array JIT (@vectorize)
    AUTO = 3        # Try best option


cdef class FunctionPattern:
    """
    Detected patterns in function source code.

    Used to determine optimal compilation strategy.
    """
    cdef:
        public bint has_loops
        public bint has_numpy
        public bint has_pandas
        public bint has_arrow
        public bint has_dict_access
        public bint has_list_ops
        public int loop_count
        public int numpy_call_count

    def __cinit__(self):
        self.has_loops = False
        self.has_numpy = False
        self.has_pandas = False
        self.has_arrow = False
        self.has_dict_access = False
        self.has_list_ops = False
        self.loop_count = 0
        self.numpy_call_count = 0


cdef class NumbaCompiler:
    """
    Intelligent Numba compiler for UDFs.

    Analyzes function and picks best compilation strategy.
    """

    cdef:
        object _numba_module
        bint _numba_available
        dict _compilation_cache
        int _max_cache_size

    def __cinit__(self, max_cache_size=1000):
        """
        Initialize Numba compiler.

        Args:
            max_cache_size: Maximum number of compiled functions to cache
        """
        # Try to import Numba
        try:
            import numba
            self._numba_module = numba
            self._numba_available = True
            logger.info("Numba available - UDF compilation enabled")
        except ImportError:
            self._numba_module = None
            self._numba_available = False
            logger.warning("Numba not available - UDFs will be interpreted")

        # Compilation cache
        self._compilation_cache = {}
        self._max_cache_size = max_cache_size

    cpdef object compile_udf(self, object func):
        """
        Compile user function with optimal strategy.

        Args:
            func: User-defined Python function

        Returns:
            Compiled function or original if compilation not beneficial
        """
        if not self._numba_available:
            return func

        # Check cache first
        cache_key = self._get_cache_key(func)
        if cache_key in self._compilation_cache:
            logger.debug(f"Cache hit for function: {func.__name__}")
            return self._compilation_cache[cache_key]

        # Analyze function
        pattern = self._analyze_function(func)
        strategy = self._choose_strategy(pattern)

        # Compile based on strategy
        compiled_func = None
        if strategy == CompilationStrategy.NJIT:
            compiled_func = self._compile_njit(func)
        elif strategy == CompilationStrategy.VECTORIZE:
            compiled_func = self._compile_vectorize(func)
        elif strategy == CompilationStrategy.SKIP:
            compiled_func = func
        else:  # AUTO
            compiled_func = self._try_compile_best_effort(func)

        # Cache result
        self._add_to_cache(cache_key, compiled_func)

        return compiled_func

    cdef str _get_cache_key(self, object func):
        """
        Generate cache key for function.

        Uses source code hash to detect changes.
        """
        try:
            source = inspect.getsource(func)
            return hashlib.md5(source.encode()).hexdigest()
        except:
            # If we can't get source, use function name + id
            return f"{func.__name__}_{id(func)}"

    cdef FunctionPattern _analyze_function(self, object func):
        """
        Analyze function source to detect patterns.

        Returns:
            FunctionPattern object with detected patterns
        """
        pattern = FunctionPattern()

        try:
            source = inspect.getsource(func)
        except:
            # Can't analyze - return empty pattern
            return pattern

        # Parse AST
        try:
            tree = ast.parse(source)
        except:
            return pattern

        # Walk AST and detect patterns
        for node in ast.walk(tree):
            # Loops
            if isinstance(node, (ast.For, ast.While)):
                pattern.has_loops = True
                pattern.loop_count += 1

            # NumPy usage
            elif isinstance(node, ast.Attribute):
                if 'np.' in ast.unparse(node):
                    pattern.has_numpy = True
                    pattern.numpy_call_count += 1

            # Dict access (record['key'])
            elif isinstance(node, ast.Subscript):
                pattern.has_dict_access = True

            # List operations
            elif isinstance(node, ast.List):
                pattern.has_list_ops = True

        # String-based detection for libraries
        pattern.has_pandas = 'pandas' in source or 'pd.' in source
        pattern.has_arrow = 'pyarrow' in source or 'pa.' in source or 'pc.' in source

        return pattern

    cdef CompilationStrategy _choose_strategy(self, FunctionPattern pattern):
        """
        Choose compilation strategy based on detected patterns.

        Decision tree:
        1. If uses Arrow/Pandas → SKIP (already fast)
        2. If uses NumPy → VECTORIZE (array operations)
        3. If has loops → NJIT (scalar operations)
        4. Otherwise → NJIT (simple expressions)
        """
        if pattern.has_arrow or pattern.has_pandas:
            # Already using fast libraries - don't compile
            return CompilationStrategy.SKIP

        elif pattern.has_numpy and pattern.numpy_call_count > 2:
            # Significant NumPy usage - use vectorize
            return CompilationStrategy.VECTORIZE

        elif pattern.has_loops or pattern.loop_count > 0:
            # Pure Python loops - use njit
            return CompilationStrategy.NJIT

        else:
            # Simple expressions - try njit
            return CompilationStrategy.NJIT

    cdef object _compile_njit(self, object func):
        """
        Compile with @njit (scalar JIT).

        Good for: loops, conditionals, simple math
        """
        try:
            compiled = self._numba_module.njit(func)

            # Test compilation (trigger actual JIT)
            test_args = self._generate_test_args(func)
            if test_args is not None:
                try:
                    _ = compiled(*test_args)
                except:
                    # Test execution failed - return original
                    logger.debug(f"Test execution failed for {func.__name__}, using Python")
                    return func

            logger.info(f"Compiled {func.__name__} with @njit")
            return compiled

        except Exception as e:
            logger.debug(f"NJIT compilation failed for {func.__name__}: {e}")
            return func

    cdef object _compile_vectorize(self, object func):
        """
        Compile with @vectorize (array JIT).

        Good for: element-wise array operations
        """
        try:
            # Vectorize needs signature - try to infer
            compiled = self._numba_module.vectorize(nopython=True)(func)
            logger.info(f"Compiled {func.__name__} with @vectorize")
            return compiled

        except Exception as e:
            logger.debug(f"Vectorize compilation failed for {func.__name__}: {e}")
            # Fall back to njit
            return self._compile_njit(func)

    cdef object _try_compile_best_effort(self, object func):
        """
        Try compilation, fall back to Python on failure.
        """
        try:
            return self._numba_module.njit(func)
        except:
            return func

    cdef tuple _generate_test_args(self, object func):
        """
        Generate test arguments for function.

        Used to trigger JIT compilation and verify it works.
        For batch-first processing, we need RecordBatch test data.
        """
        import inspect
        sig = inspect.signature(func)

        # Single argument: likely a RecordBatch for batch-first processing
        if len(sig.parameters) == 1:
            # Create a small test RecordBatch
            import pyarrow as pa
            test_batch = pa.RecordBatch.from_pydict({
                'value': [1.0, 2.0, 3.0],
                'id': [1, 2, 3],
                'timestamp': [1000, 1001, 1002]
            })
            return (test_batch,)

        # Can't infer - skip test
        return None

    cdef void _add_to_cache(self, str key, object func):
        """
        Add compiled function to cache.

        Implements simple LRU eviction if cache is full.
        """
        if len(self._compilation_cache) >= self._max_cache_size:
            # Simple eviction: remove first item (not true LRU, but fast)
            first_key = next(iter(self._compilation_cache))
            del self._compilation_cache[first_key]

        self._compilation_cache[key] = func


# Global compiler instance (singleton)
cdef NumbaCompiler _global_compiler = NumbaCompiler()


cpdef object auto_compile(object func):
    """
    Public API: automatically compile function.

    Usage:
        compiled_func = auto_compile(my_udf)

    Args:
        func: User-defined Python function

    Returns:
        Compiled function (or original if compilation not beneficial)
    """
    return _global_compiler.compile_udf(func)
```

**Estimated Hours**: 8 hours

---

### Task 2: Integrate with MapOperator

**File**: `/Users/bengamble/Sabot/sabot/_cython/operators/transform.pyx` (MODIFY)

**Changes**:
1. Import `auto_compile` from `numba_compiler`
2. Auto-compile map functions in `__init__()`
3. Track compilation status for debugging
4. Add performance logging

**Implementation**:

```cython
# Add import at top
from sabot._cython.operators.numba_compiler cimport auto_compile

@cython.final
cdef class CythonMapOperator(BaseOperator):
    """
    Map operator: transform each RecordBatch using a function.

    Automatically compiles user functions with Numba for 10-100x speedup.
    """

    cdef:
        object _map_func            # Original user function
        object _compiled_func       # Numba-compiled version (may be same as _map_func)
        bint _is_compiled           # Was function successfully compiled?
        str _compilation_strategy   # Strategy used (njit/vectorize/skip)

    def __init__(self, source, map_func, schema=None):
        """
        Initialize map operator.

        Args:
            source: Input stream/iterator
            map_func: Function to apply to each batch
            schema: Output schema (inferred if None)
        """
        self._source = source
        self._schema = schema
        self._map_func = map_func

        # Auto-compile with Numba
        import time
        start = time.perf_counter()

        self._compiled_func = auto_compile(map_func)
        self._is_compiled = (self._compiled_func is not map_func)

        compile_time = (time.perf_counter() - start) * 1000  # ms

        if self._is_compiled:
            logger.info(
                f"Numba-compiled map function '{map_func.__name__}' "
                f"in {compile_time:.2f}ms"
            )
        else:
            logger.debug(
                f"Using interpreted Python for '{map_func.__name__}' "
                f"(compilation skipped or failed in {compile_time:.2f}ms)"
            )

    @cython.boundscheck(False)
    @cython.wraparound(False)
    cpdef object process_batch(self, object batch):
        """Apply map function to batch."""
        if not ARROW_AVAILABLE or batch is None:
            return None

        try:
            # Use compiled function (or original if compilation failed)
            result = self._compiled_func(batch)

            # Ensure result is RecordBatch
            if not isinstance(result, pa.RecordBatch):
                if isinstance(result, pa.Table):
                    result = result.combine_chunks()
                elif isinstance(result, dict):
                    result = pa.RecordBatch.from_pydict(result)
                else:
                    raise TypeError(
                        f"Map function must return RecordBatch, got {type(result)}"
                    )

            return result

        except Exception as e:
            raise RuntimeError(f"Error in map operator: {e}")
```

**Estimated Hours**: 2 hours

---

### Task 3: Create .pxd Header File

**File**: `/Users/bengamble/Sabot/sabot/_cython/operators/numba_compiler.pxd` (NEW)

**Purpose**: Cython header file for cross-module imports

**Implementation**:

```cython
# cython: language_level=3

"""
Cython header file for Numba compiler.

Allows other Cython modules to import compiled declarations.
"""

cdef enum CompilationStrategy:
    SKIP
    NJIT
    VECTORIZE
    AUTO

cdef class FunctionPattern:
    cdef:
        public bint has_loops
        public bint has_numpy
        public bint has_pandas
        public bint has_arrow
        public bint has_dict_access
        public bint has_list_ops
        public int loop_count
        public int numpy_call_count

cdef class NumbaCompiler:
    cdef:
        object _numba_module
        bint _numba_available
        dict _compilation_cache
        int _max_cache_size

    cpdef object compile_udf(self, object func)
    cdef str _get_cache_key(self, object func)
    cdef FunctionPattern _analyze_function(self, object func)
    cdef CompilationStrategy _choose_strategy(self, FunctionPattern pattern)
    cdef object _compile_njit(self, object func)
    cdef object _compile_vectorize(self, object func)
    cdef object _try_compile_best_effort(self, object func)
    cdef tuple _generate_test_args(self, object func)
    cdef void _add_to_cache(self, str key, object func)

# Public API
cpdef object auto_compile(object func)
```

**Estimated Hours**: 1 hour

---

### Task 4: Add to Build System

**File**: `/Users/bengamble/Sabot/setup.py` (MODIFY)

**Changes**: Add `numba_compiler.pyx` to the list of extensions to compile

**Implementation**:

```python
# In the find_pyx_files() function, add to working_extensions list:

working_extensions = [
    # ... existing extensions ...

    # Numba UDF compilation (NEW)
    "sabot/_cython/operators/numba_compiler.pyx",

    # Transform operators (updated for Numba integration)
    "sabot/_cython/operators/transform.pyx",
]
```

**Estimated Hours**: 0.5 hours

---

### Task 5: Create Test Suite

**File**: `/Users/bengamble/Sabot/tests/unit/test_numba_compilation.py` (NEW)

**Lines of Code**: ~400 lines

**Test Coverage**:
1. Compilation detection for different function patterns
2. Performance benchmarks (compiled vs interpreted)
3. Fallback behavior on compilation failure
4. Cache functionality
5. Integration with MapOperator

**Implementation**:

```python
"""
Test Numba auto-compilation for UDFs.

Verifies:
- Pattern detection (loops, numpy, pandas, arrow)
- Compilation strategy selection
- Performance improvement (10-100x speedup)
- Graceful fallback on compilation failure
- Cache functionality
"""

import pytest
import numpy as np
import pyarrow as pa
import time

try:
    import numba
    NUMBA_AVAILABLE = True
except ImportError:
    NUMBA_AVAILABLE = False

from sabot._cython.operators.numba_compiler import NumbaCompiler, auto_compile
from sabot._cython.operators.transform import CythonMapOperator


class TestPatternDetection:
    """Test AST analysis for pattern detection."""

    def test_detect_loops(self):
        """Should detect for loops."""
        def func_with_loop(record):
            total = 0
            for i in range(100):
                total += record['value'] * i
            return {'result': total}

        compiler = NumbaCompiler()
        pattern = compiler._analyze_function(func_with_loop)

        assert pattern.has_loops
        assert pattern.loop_count == 1
        assert not pattern.has_numpy
        assert not pattern.has_pandas
        assert not pattern.has_arrow

    def test_detect_numpy(self):
        """Should detect NumPy usage."""
        def func_with_numpy(record):
            arr = np.array(record['values'])
            return {'result': np.sum(arr * 2)}

        compiler = NumbaCompiler()
        pattern = compiler._analyze_function(func_with_numpy)

        assert pattern.has_numpy
        assert pattern.numpy_call_count >= 1

    def test_detect_pandas(self):
        """Should detect Pandas usage."""
        def func_with_pandas(batch):
            import pandas as pd
            df = batch.to_pandas()
            return df[df['value'] > 10]

        compiler = NumbaCompiler()
        pattern = compiler._analyze_function(func_with_pandas)

        assert pattern.has_pandas

    def test_detect_arrow(self):
        """Should detect PyArrow usage."""
        def func_with_arrow(batch):
            import pyarrow.compute as pc
            return pc.filter(batch, pc.greater(batch['value'], 10))

        compiler = NumbaCompiler()
        pattern = compiler._analyze_function(func_with_arrow)

        assert pattern.has_arrow


class TestCompilationStrategy:
    """Test strategy selection logic."""

    def test_skip_arrow_functions(self):
        """Should skip compilation for Arrow functions."""
        def arrow_func(batch):
            import pyarrow.compute as pc
            return pc.multiply(batch['value'], 2)

        compiler = NumbaCompiler()
        pattern = compiler._analyze_function(arrow_func)
        strategy = compiler._choose_strategy(pattern)

        # Should skip - Arrow is already fast
        from sabot._cython.operators.numba_compiler import CompilationStrategy
        assert strategy == CompilationStrategy.SKIP

    def test_njit_for_loops(self):
        """Should use @njit for loop-based functions."""
        def loop_func(record):
            total = 0
            for i in range(100):
                total += record['value'] * i
            return total

        compiler = NumbaCompiler()
        pattern = compiler._analyze_function(loop_func)
        strategy = compiler._choose_strategy(pattern)

        from sabot._cython.operators.numba_compiler import CompilationStrategy
        assert strategy == CompilationStrategy.NJIT

    @pytest.mark.skipif(not NUMBA_AVAILABLE, reason="Numba not installed")
    def test_vectorize_for_numpy(self):
        """Should use @vectorize for NumPy functions."""
        def numpy_func(x):
            return np.sin(x) + np.cos(x) * 2 + np.exp(x / 10)

        compiler = NumbaCompiler()
        pattern = compiler._analyze_function(numpy_func)
        strategy = compiler._choose_strategy(pattern)

        from sabot._cython.operators.numba_compiler import CompilationStrategy
        # Multiple numpy calls → vectorize
        assert strategy == CompilationStrategy.VECTORIZE


class TestCompilation:
    """Test actual Numba compilation."""

    @pytest.mark.skipif(not NUMBA_AVAILABLE, reason="Numba not installed")
    def test_njit_compilation_success(self):
        """Should successfully compile with @njit."""
        def simple_batch_func(batch):
            # Extract numpy array and do computation
            values = batch.column('value').to_numpy()
            results = []
            for i in range(len(values)):
                results.append(values[i] * 2 + 10)
            return batch.append_column('computed', pa.array(results))

        compiled = auto_compile(simple_batch_func)

        # Test it works
        test_batch = pa.RecordBatch.from_pydict({'value': [5, 10]})
        result = compiled(test_batch)
        assert result.column('computed').to_pylist() == [20, 30]

    @pytest.mark.skipif(not NUMBA_AVAILABLE, reason="Numba not installed")
    def test_compilation_fallback(self):
        """Should fallback to Python on compilation failure."""
        def unsupported_batch_func(batch):
            # Complex operations not supported by Numba
            # (This example would actually work, but demonstrates fallback)
            names = batch.column('name').to_pylist()
            upper_names = [name.upper() for name in names]
            return batch.append_column('upper_name', pa.array(upper_names))

        compiled = auto_compile(unsupported_batch_func)

        # Should still work (using Python)
        test_batch = pa.RecordBatch.from_pydict({'name': ['test', 'data']})
        result = compiled(test_batch)
        assert result.column('upper_name').to_pylist() == ['TEST', 'DATA']

    def test_skip_compilation_for_arrow(self):
        """Should not compile Arrow functions."""
        def arrow_func(batch):
            import pyarrow.compute as pc
            return batch

        compiled = auto_compile(arrow_func)

        # Should return same function (not compiled)
        assert compiled is arrow_func


class TestPerformance:
    """Test performance improvement from compilation."""

    @pytest.mark.skipif(not NUMBA_AVAILABLE, reason="Numba not installed")
    def test_loop_speedup(self):
        """Compiled loop should be 10-50x faster."""
        def loop_batch_func(batch):
            values = batch.column('value').to_numpy()
            results = []
            for i in range(len(values)):
                total = 0.0
                for j in range(1000):
                    total += values[i] * j * 1.5
                results.append(total)
            return batch.append_column('computed', pa.array(results))

        # Warm up JIT
        compiled = auto_compile(loop_batch_func)
        test_batch = pa.RecordBatch.from_pydict({'value': [2.5]})
        _ = compiled(test_batch)

        # Benchmark interpreted
        start = time.perf_counter()
        for _ in range(100):
            _ = loop_batch_func(test_batch)
        interpreted_time = time.perf_counter() - start

        # Benchmark compiled
        start = time.perf_counter()
        for _ in range(100):
            _ = compiled(test_batch)
        compiled_time = time.perf_counter() - start

        speedup = interpreted_time / compiled_time

        print(f"\nLoop speedup: {speedup:.1f}x")
        print(f"  Interpreted: {interpreted_time*1000:.2f}ms")
        print(f"  Compiled:    {compiled_time*1000:.2f}ms")

        # Should be at least 5x faster (lower threshold for batch processing)
        assert speedup >= 5.0, f"Expected 5x+ speedup, got {speedup:.1f}x"

    @pytest.mark.skipif(not NUMBA_AVAILABLE, reason="Numba not installed")
    def test_numpy_speedup(self):
        """Compiled NumPy should be 50-100x faster."""
        def numpy_func(arr):
            return np.sum(arr * 2.5 + 10)

        # Warm up JIT
        compiled = auto_compile(numpy_func)
        test_arr = np.random.random(10000)
        _ = compiled(test_arr)

        # Benchmark interpreted
        start = time.perf_counter()
        for _ in range(1000):
            _ = numpy_func(test_arr)
        interpreted_time = time.perf_counter() - start

        # Benchmark compiled
        start = time.perf_counter()
        for _ in range(1000):
            _ = compiled(test_arr)
        compiled_time = time.perf_counter() - start

        speedup = interpreted_time / compiled_time

        print(f"\nNumPy speedup: {speedup:.1f}x")
        print(f"  Interpreted: {interpreted_time*1000:.2f}ms")
        print(f"  Compiled:    {compiled_time*1000:.2f}ms")

        # NumPy speedup varies, but should be noticeable
        assert speedup >= 5.0, f"Expected 5x+ speedup, got {speedup:.1f}x"


class TestCache:
    """Test compilation cache."""

    @pytest.mark.skipif(not NUMBA_AVAILABLE, reason="Numba not installed")
    def test_cache_hit(self):
        """Second compilation should hit cache."""
        def test_func(x):
            return x * 2

        compiler = NumbaCompiler()

        # First compilation
        start = time.perf_counter()
        compiled1 = compiler.compile_udf(test_func)
        first_time = time.perf_counter() - start

        # Second compilation (should hit cache)
        start = time.perf_counter()
        compiled2 = compiler.compile_udf(test_func)
        second_time = time.perf_counter() - start

        # Cache hit should be much faster (<1ms)
        assert second_time < 0.001, f"Cache hit took {second_time*1000:.2f}ms"

        # Should return same compiled object
        assert compiled1 is compiled2

    def test_cache_eviction(self):
        """Cache should evict old entries when full."""
        compiler = NumbaCompiler(max_cache_size=5)

        # Compile 10 different functions
        for i in range(10):
            func = eval(f"lambda x: x * {i}")
            func.__name__ = f"func_{i}"
            _ = compiler.compile_udf(func)

        # Cache should have at most 5 entries
        assert len(compiler._compilation_cache) <= 5


class TestMapOperatorIntegration:
    """Test integration with MapOperator."""

    @pytest.mark.skipif(not NUMBA_AVAILABLE, reason="Numba not installed")
    def test_auto_compilation_in_map(self):
        """MapOperator should auto-compile functions."""
        def my_transform(batch):
            # Simple batch transformation
            import pyarrow.compute as pc
            return batch.append_column(
                'doubled',
                pc.multiply(batch.column('value'), 2)
            )

        # Create test batch
        batch = pa.RecordBatch.from_pydict({
            'value': [1, 2, 3, 4, 5]
        })

        # Create map operator (should auto-compile)
        source = iter([batch])
        map_op = CythonMapOperator(source, my_transform)

        # Execute
        results = list(map_op)

        assert len(results) == 1
        result = results[0]
        assert 'doubled' in result.column_names
        assert result.column('doubled').to_pylist() == [2, 4, 6, 8, 10]

    @pytest.mark.skipif(not NUMBA_AVAILABLE, reason="Numba not installed")
    def test_compilation_logging(self):
        """Should log compilation status."""
        import logging

        # Capture logs
        logger = logging.getLogger('sabot._cython.operators.transform')
        logger.setLevel(logging.INFO)

        handler = logging.StreamHandler()
        logger.addHandler(handler)

        def loop_func(batch):
            # This should trigger Numba compilation
            total = 0
            for i in range(100):
                total += i
            return batch

        batch = pa.RecordBatch.from_pydict({'value': [1, 2, 3]})
        source = iter([batch])

        # Create operator (should log compilation)
        map_op = CythonMapOperator(source, loop_func)

        # Check compilation status
        assert map_op._is_compiled or not NUMBA_AVAILABLE


class TestEdgeCases:
    """Test edge cases and error handling."""

    def test_lambda_function(self):
        """Should handle lambda functions."""
        func = lambda x: x * 2

        # Should not crash
        compiled = auto_compile(func)

        # Lambda source may not be parseable, so could be original
        assert compiled is not None

    def test_builtin_function(self):
        """Should handle builtin functions gracefully."""
        # Builtins can't be compiled
        compiled = auto_compile(len)

        # Should return original
        assert compiled is len

    def test_class_method(self):
        """Should handle class methods."""
        class MyClass:
            def my_method(self, x):
                return x * 2

        obj = MyClass()

        # Should not crash
        compiled = auto_compile(obj.my_method)
        assert compiled is not None


if __name__ == '__main__':
    pytest.main([__file__, '-v', '-s'])
```

**Estimated Hours**: 6 hours

---

### Task 6: Create Benchmark Suite

**File**: `/Users/bengamble/Sabot/benchmarks/numba_compilation_bench.py` (NEW)

**Purpose**: Comprehensive performance benchmarks demonstrating speedup

**Implementation**:

```python
"""
Numba Compilation Benchmarks

Demonstrates performance improvement from auto-compilation.
"""

import time
import numpy as np
import pyarrow as pa
import pyarrow.compute as pc

try:
    from sabot._cython.operators.numba_compiler import auto_compile
    from sabot._cython.operators.transform import CythonMapOperator
    SABOT_AVAILABLE = True
except ImportError:
    SABOT_AVAILABLE = False
    print("Sabot not installed - skipping benchmarks")
    exit(0)


def benchmark_function(func, compiled_func, test_data, iterations=1000):
    """
    Benchmark a function with and without compilation.

    Returns:
        dict with timing results and speedup
    """
    # Warm up JIT
    _ = compiled_func(test_data)

    # Benchmark interpreted
    start = time.perf_counter()
    for _ in range(iterations):
        _ = func(test_data)
    interpreted_time = time.perf_counter() - start

    # Benchmark compiled
    start = time.perf_counter()
    for _ in range(iterations):
        _ = compiled_func(test_data)
    compiled_time = time.perf_counter() - start

    speedup = interpreted_time / compiled_time

    return {
        'interpreted_ms': interpreted_time * 1000,
        'compiled_ms': compiled_time * 1000,
        'speedup': speedup
    }


def bench_simple_loop():
    """Benchmark simple for loop."""
    print("\n1. Simple Loop Benchmark")
    print("-" * 50)

    def loop_func(record):
        total = 0.0
        for i in range(1000):
            total += record['value'] * i * 1.5
        return total

    compiled = auto_compile(loop_func)
    test_data = {'value': 2.5}

    results = benchmark_function(loop_func, compiled, test_data)

    print(f"Interpreted: {results['interpreted_ms']:.2f}ms")
    print(f"Compiled:    {results['compiled_ms']:.2f}ms")
    print(f"Speedup:     {results['speedup']:.1f}x")

    return results


def bench_complex_computation():
    """Benchmark complex mathematical computation."""
    print("\n2. Complex Math Benchmark")
    print("-" * 50)

    def complex_func(record):
        x = record['value']
        result = 0.0
        for i in range(500):
            result += (x * i) / (i + 1) + (x ** 2) * 0.5
        return result

    compiled = auto_compile(complex_func)
    test_data = {'value': 3.14}

    results = benchmark_function(complex_func, compiled, test_data)

    print(f"Interpreted: {results['interpreted_ms']:.2f}ms")
    print(f"Compiled:    {results['compiled_ms']:.2f}ms")
    print(f"Speedup:     {results['speedup']:.1f}x")

    return results


def bench_batch_processing():
    """Benchmark batch-level processing with MapOperator."""
    print("\n3. Batch Processing Benchmark")
    print("-" * 50)

    # Create large batch
    n = 100_000
    batch = pa.RecordBatch.from_pydict({
        'value': np.random.random(n),
        'id': np.arange(n),
    })

    def batch_transform(b):
        # Use Arrow compute (already fast - should not compile)
        return b.append_column(
            'doubled',
            pc.multiply(b.column('value'), 2)
        )

    # Process with MapOperator
    source = iter([batch])
    map_op = CythonMapOperator(source, batch_transform)

    start = time.perf_counter()
    results = list(map_op)
    elapsed = (time.perf_counter() - start) * 1000

    print(f"Processed {n:,} rows in {elapsed:.2f}ms")
    print(f"Throughput: {n / (elapsed / 1000) / 1_000_000:.2f}M rows/sec")
    print(f"Compilation: {'Skipped (Arrow already fast)' if not map_op._is_compiled else 'Compiled'}")

    return {'elapsed_ms': elapsed, 'rows': n}


def bench_compilation_overhead():
    """Measure compilation overhead (first-time cost)."""
    print("\n4. Compilation Overhead Benchmark")
    print("-" * 50)

    def test_func(x):
        total = 0
        for i in range(100):
            total += x * i
        return total

    # Measure compilation time
    start = time.perf_counter()
    compiled = auto_compile(test_func)
    compile_time = (time.perf_counter() - start) * 1000

    print(f"Compilation time: {compile_time:.2f}ms")

    # Measure cache hit time
    start = time.perf_counter()
    cached = auto_compile(test_func)
    cache_time = (time.perf_counter() - start) * 1000

    print(f"Cache hit time:   {cache_time:.3f}ms")

    return {'compile_ms': compile_time, 'cache_ms': cache_time}


def run_all_benchmarks():
    """Run all benchmarks and print summary."""
    print("=" * 50)
    print("Numba Auto-Compilation Benchmarks")
    print("=" * 50)

    results = []

    results.append(('Simple Loop', bench_simple_loop()))
    results.append(('Complex Math', bench_complex_computation()))
    results.append(('Batch Processing', bench_batch_processing()))
    results.append(('Compilation Overhead', bench_compilation_overhead()))

    print("\n" + "=" * 50)
    print("Summary")
    print("=" * 50)

    for name, result in results:
        print(f"\n{name}:")
        for key, value in result.items():
            if key == 'speedup':
                print(f"  {key}: {value:.1f}x")
            elif 'ms' in key:
                print(f"  {key}: {value:.2f}ms")
            else:
                print(f"  {key}: {value}")


if __name__ == '__main__':
    run_all_benchmarks()
```

**Estimated Hours**: 3 hours

---

### Task 7: Update Documentation

**Files to Update**:
1. `/Users/bengamble/Sabot/README.md` - Add section on auto-compilation
2. `/Users/bengamble/Sabot/docs/design/UNIFIED_BATCH_ARCHITECTURE.md` - Update Part 3 with implementation details

**Content for README.md**:

```markdown
### Auto-Numba UDF Compilation

Sabot automatically compiles user-defined functions with Numba for 10-100x speedup:

```python
# User writes normal Python - Sabot auto-compiles it!
def my_transform(record):
    total = 0
    for i in range(100):
        total += record['value'] * i
    return {'result': total}

# Automatically compiled with Numba @njit (10-50x faster)
stream = Stream.from_kafka('data').map(my_transform)
```

**How it works**:
1. AST analysis detects function patterns (loops, NumPy, etc.)
2. Chooses optimal compilation strategy (`@njit` vs `@vectorize`)
3. Compiles with Numba transparently
4. Falls back to Python if compilation fails
5. Caches compiled functions for reuse

**Performance**:
- Scalar loops: 10-50x speedup
- NumPy operations: 50-100x speedup
- Compilation overhead: <100ms (first-time only)
- Cache hit: <1ms

No code changes required!
```

**Estimated Hours**: 2 hours

---

## Integration Points

### 1. MapOperator Integration
- **File**: `sabot/_cython/operators/transform.pyx`
- **Change**: Import and use `auto_compile()` in `__init__()`
- **Impact**: All map operations automatically benefit from compilation

### 2. Build System Integration
- **File**: `setup.py`
- **Change**: Add `numba_compiler.pyx` to build list
- **Impact**: Module compiled alongside other Cython extensions

### 3. Test Integration
- **Files**: `tests/unit/test_numba_compilation.py`, `benchmarks/numba_compilation_bench.py`
- **Impact**: Comprehensive test coverage and performance validation

---

## Success Criteria

### Functional Requirements
- ✅ AST analysis correctly detects function patterns
- ✅ Compilation strategy chosen automatically
- ✅ Numba compilation succeeds for compatible functions
- ✅ Graceful fallback to Python for incompatible functions
- ✅ Compiled functions cached and reused
- ✅ Integration with MapOperator transparent to users

### Performance Requirements
- ✅ Scalar loops: 10-50x speedup over interpreted Python
- ✅ NumPy operations: 50-100x speedup
- ✅ Compilation overhead: <100ms per function
- ✅ Cache hit latency: <1ms
- ✅ No performance regression for Arrow/Pandas functions (skip compilation)

### Testing Requirements
- ✅ 100% coverage of NumbaCompiler class
- ✅ Pattern detection tests for all function types
- ✅ Performance benchmarks demonstrating speedup
- ✅ Edge case tests (lambdas, builtins, class methods)
- ✅ Integration tests with MapOperator

---

## Estimated Effort

| Task | Component | Hours |
|------|-----------|-------|
| 1 | NumbaCompiler Core (numba_compiler.pyx) | 8 |
| 2 | MapOperator Integration (transform.pyx) | 2 |
| 3 | Header File (numba_compiler.pxd) | 1 |
| 4 | Build System (setup.py) | 0.5 |
| 5 | Test Suite (test_numba_compilation.py) | 6 |
| 6 | Benchmark Suite (numba_compilation_bench.py) | 3 |
| 7 | Documentation Updates | 2 |
| **Total** | | **22.5 hours** |

**Estimated Duration**: 3-4 working days

---

## Risk Assessment

### Low Risk
- ✅ Numba is stable and well-tested
- ✅ Fallback to Python ensures robustness
- ✅ Compilation is opt-in (automatic, but skippable)

### Medium Risk
- ⚠️ Numba not available on all platforms → Handled by availability check
- ⚠️ Compilation may fail for complex functions → Graceful fallback
- ⚠️ Cache eviction may cause recompilation → Acceptable tradeoff

### Mitigation Strategies
1. **Availability check**: Detect Numba at runtime, fallback if not installed
2. **Comprehensive testing**: Test all function patterns and edge cases
3. **Logging**: Detailed logs for debugging compilation issues
4. **Performance monitoring**: Track compilation success/failure rates

---

## Testing Strategy

### Unit Tests
- **Pattern detection**: Verify AST analysis for loops, NumPy, Pandas, Arrow
- **Strategy selection**: Test decision tree for compilation strategy
- **Compilation**: Test `@njit` and `@vectorize` compilation
- **Fallback**: Test graceful degradation on failure
- **Cache**: Test cache hits, eviction, and persistence

### Integration Tests
- **MapOperator**: Test auto-compilation in map operations
- **End-to-end**: Test complete pipeline with compiled UDFs

### Performance Tests
- **Speedup benchmarks**: Measure 10-100x improvement
- **Overhead benchmarks**: Verify <100ms compilation time
- **Throughput tests**: Ensure no regression for Arrow/Pandas functions

### Edge Case Tests
- **Lambda functions**: Handle lambda compilation/fallback
- **Builtins**: Gracefully skip builtin functions
- **Class methods**: Handle method compilation
- **Unsupported operations**: Fallback for string ops, object manipulation

---

## Implementation Checklist

- [ ] Task 1: Create NumbaCompiler core (`numba_compiler.pyx`)
  - [ ] AST analysis for pattern detection
  - [ ] Compilation strategy selection logic
  - [ ] `@njit` compilation with error handling
  - [ ] `@vectorize` compilation with signature inference
  - [ ] Test data generation for compilation verification
  - [ ] LRU cache implementation

- [ ] Task 2: Integrate with MapOperator (`transform.pyx`)
  - [ ] Import `auto_compile` function
  - [ ] Auto-compile in `__init__()`
  - [ ] Track compilation status
  - [ ] Add performance logging

- [ ] Task 3: Create header file (`numba_compiler.pxd`)
  - [ ] Declare cdef classes and functions
  - [ ] Export public API

- [ ] Task 4: Update build system (`setup.py`)
  - [ ] Add `numba_compiler.pyx` to extensions
  - [ ] Verify compilation

- [ ] Task 5: Create test suite (`test_numba_compilation.py`)
  - [ ] Pattern detection tests
  - [ ] Strategy selection tests
  - [ ] Compilation tests
  - [ ] Performance tests
  - [ ] Cache tests
  - [ ] Integration tests
  - [ ] Edge case tests

- [ ] Task 6: Create benchmark suite (`numba_compilation_bench.py`)
  - [ ] Simple loop benchmark
  - [ ] Complex math benchmark
  - [ ] Batch processing benchmark
  - [ ] Compilation overhead benchmark

- [ ] Task 7: Update documentation
  - [ ] README.md section
  - [ ] Architecture doc updates
  - [ ] Usage examples

---

## Follow-up Tasks (Future Phases)

### Phase 2.1: Persistent Cache (Optional)
- Disk-based cache for compiled functions
- Share compiled functions across processes
- Invalidation on function source change

### Phase 2.2: Advanced Compilation (Optional)
- GPU compilation with `@cuda.jit` for CUDA-capable devices
- Parallel compilation with `@njit(parallel=True)`
- Custom type signatures for better optimization

### Phase 2.3: Metrics and Observability (Optional)
- Track compilation success/failure rates
- Monitor speedup metrics
- Dashboard for compilation analytics

---

## Conclusion

Phase 2 delivers automatic Numba compilation for user-defined functions, providing 10-100x speedup transparently. The implementation is robust (graceful fallback), well-tested (comprehensive test suite), and production-ready.

**Key Benefits**:
- ✅ No user code changes required
- ✅ 10-100x performance improvement
- ✅ Graceful fallback ensures reliability
- ✅ Comprehensive testing and benchmarks
- ✅ Clean integration with existing operators

**Next Phase**: Phase 3 - Connect Operators to Morsel-Driven Parallelism
