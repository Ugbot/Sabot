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

    print(".2f")
    print(".2f")
    print(".1f")

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

    print(".2f")
    print(".2f")
    print(".1f")

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
    print(f"Compilation: Auto-compilation enabled (Numba {'available' if SABOT_AVAILABLE else 'not available'})")

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

    print(f"Cache hit time: {cache_time:.3f}ms")

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
                print(".1f")
            elif 'ms' in key:
                print(".2f")
            else:
                print(f"  {key}: {value}")


if __name__ == '__main__':
    run_all_benchmarks()
