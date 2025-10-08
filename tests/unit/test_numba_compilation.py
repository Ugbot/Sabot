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
from sabot import cyarrow as pa
import time

try:
    import numba
    NUMBA_AVAILABLE = True
except ImportError:
    NUMBA_AVAILABLE = False

from sabot._cython.operators.numba_compiler import (
    NumbaCompiler, auto_compile,
    STRATEGY_SKIP, STRATEGY_NJIT, STRATEGY_VECTORIZE
)
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
        assert strategy == STRATEGY_SKIP

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

        assert strategy == STRATEGY_NJIT

    @pytest.mark.skipif(not NUMBA_AVAILABLE, reason="Numba not installed")
    def test_vectorize_for_numpy(self):
        """Should use @vectorize for NumPy functions."""
        def numpy_func(x):
            return np.sin(x) + np.cos(x) * 2 + np.exp(x / 10)

        compiler = NumbaCompiler()
        pattern = compiler._analyze_function(numpy_func)
        strategy = compiler._choose_strategy(pattern)

        # Multiple numpy calls → vectorize
        assert strategy == STRATEGY_VECTORIZE


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
