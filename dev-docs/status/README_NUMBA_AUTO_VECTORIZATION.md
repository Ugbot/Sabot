# Numba Auto-Vectorization Demo

**Automatic 100-200x speedup for streaming data analytics - zero code changes required!**

## What This Demonstrates

This example showcases Sabot's **automatic Numba JIT compilation** feature, which transparently accelerates compute-intensive user-defined functions (UDFs) by 10-200x with:

- ✅ **Zero code changes** - just write normal NumPy/loop code
- ✅ **Automatic pattern detection** - bytecode analysis finds optimization opportunities
- ✅ **Transparent compilation** - happens at runtime without user intervention
- ✅ **Graceful fallback** - falls back to interpreted Python if compilation fails

## Running the Demo

```bash
# From the Sabot root directory
.venv/bin/python examples/numba_auto_vectorization_demo.py
```

## Example Results

```
Moving Average (Loop-based):           194.5x speedup ✅
Exponential Moving Average:            113.4x speedup ✅
VWAP (Volume-Weighted Avg Price):      208.3x speedup ✅
```

## How It Works

### 1. **Automatic Pattern Detection**

The NumbaCompiler uses **bytecode + AST analysis** to detect:
- Python loops (`for`, `while`)
- NumPy array operations
- Numeric computations

### 2. **Smart Compilation Strategy**

Based on detected patterns, it chooses:
- **NJIT** - For loop-heavy code (50-200x faster)
- **VECTORIZE** - For NumPy array operations (10-50x faster)
- **SKIP** - For Arrow/Pandas operations (already fast)

### 3. **Lazy JIT Compilation**

Functions are compiled on **first call** with concrete types:
- Happens automatically at runtime
- No upfront compilation cost
- Works with inline functions, lambdas, file-defined functions

### 4. **Graceful Fallback**

If compilation fails (unsupported operations):
- Automatically falls back to interpreted Python
- No errors or crashes
- Still functions correctly (just slower)

## Use Cases Demonstrated

### 1. **Moving Average** (Loop-based calculation)
```python
def moving_average_with_loops(prices, window=20):
    result = np.zeros(len(prices))
    for i in range(window, len(prices)):
        total = 0.0
        for j in range(window):
            total += prices[i - j]
        result[i] = total / window
    return result
```
**Result:** 194.5x speedup

### 2. **VWAP** (Volume-Weighted Average Price)
```python
def calculate_vwap(prices, volumes):
    cumulative_tpv = 0.0
    cumulative_volume = 0.0
    vwap = np.zeros(len(prices))

    for i in range(len(prices)):
        cumulative_tpv += prices[i] * volumes[i]
        cumulative_volume += volumes[i]
        if cumulative_volume > 0:
            vwap[i] = cumulative_tpv / cumulative_volume

    return vwap
```
**Result:** 208.3x speedup

### 3. **Exponential Moving Average** (Smoothing)
```python
def exponential_moving_average(prices, alpha=0.1):
    ema = np.zeros(len(prices))
    ema[0] = prices[0]

    for i in range(1, len(prices)):
        ema[i] = alpha * prices[i] + (1 - alpha) * ema[i - 1]

    return ema
```
**Result:** 113.4x speedup

## Numba Compatibility

### ✅ Works Well With:
- NumPy arrays (`np.ndarray`)
- Numeric types (`int`, `float`)
- Basic loops (`for`, `while`)
- Simple NumPy functions (`np.zeros`, `np.mean`, `np.sum`)
- Array indexing and slicing

### ⚠️ Limited Support For:
- Complex NumPy functions (`np.diff`, `np.where` in some contexts)
- Multi-dimensional arrays (in `@vectorize` mode)
- Python dicts, lists, objects
- String operations

### 💡 Best Practices:
1. **Use NumPy arrays** instead of Python lists
2. **Use loops** for sequential operations (Numba makes them fast!)
3. **Keep functions focused** - single responsibility
4. **Profile first** - not all code needs optimization
5. **Test fallback** - ensure your code works without Numba

## Integration with Sabot Streaming

This auto-compilation works seamlessly with Sabot's streaming operators:

```python
from sabot.api import Stream
from sabot._cython.operators.numba_compiler import auto_compile

# Your UDF automatically gets compiled
def calculate_technical_indicators(prices_array):
    # This function will be auto-compiled with Numba
    result = np.zeros(len(prices_array))
    for i in range(20, len(prices_array)):
        result[i] = np.mean(prices_array[i-20:i])
    return result

# Use in streaming pipeline (compilation happens automatically)
stream = Stream.from_kafka('prices')
stream.map(calculate_technical_indicators).to_kafka('indicators')
```

## Performance Tips

### 1. **Warm-up Period**
First call triggers JIT compilation (adds latency):
```python
# Warm up the JIT compiler
_ = my_function(test_data)  # First call: ~100ms (compilation)
_ = my_function(real_data)  # Subsequent calls: <1ms
```

### 2. **Batch Processing**
Process data in batches for maximum speedup:
```python
# BAD: Process one-by-one
for price in prices:
    result = calculate(np.array([price]))  # JIT overhead per call

# GOOD: Process in batches
result = calculate(np.array(prices))  # JIT overhead once
```

### 3. **Type Stability**
Keep function signatures consistent:
```python
# BAD: Different types trigger recompilation
calculate(np.array([1, 2, 3], dtype=np.int32))   # Compile for int32
calculate(np.array([1.0, 2.0], dtype=np.float64)) # Compile for float64

# GOOD: Consistent types
calculate(np.array([1, 2, 3], dtype=np.float64))  # Compile once
calculate(np.array([4, 5, 6], dtype=np.float64))  # Reuse compiled version
```

## Troubleshooting

### Compilation Failed?
If you see `⚠️ Compilation failed`, check:
1. Are you using unsupported NumPy functions? (Try simpler alternatives)
2. Are you using Python dicts/lists? (Convert to NumPy arrays)
3. Is your function too complex? (Break it into smaller functions)

### No Speedup?
If speedup is minimal:
1. Function may be too simple (overhead > benefit)
2. May already be using fast NumPy operations
3. Check if pattern detection worked (look for "Strategy: NJIT")

### Debugging
Enable debug logging to see what's happening:
```python
import logging
logging.basicConfig(level=logging.DEBUG)

# Now you'll see compilation messages
result = auto_compile(my_function)
```

## Further Reading

- [Numba Documentation](https://numba.pydata.org/)
- [Sabot Auto-Compilation Design Doc](/docs/implementation/PHASE2_AUTO_NUMBA_COMPILATION.md)
- [NumPy Performance Tips](https://numpy.org/doc/stable/user/basics.performance.html)

## Summary

**Automatic Numba compilation in Sabot provides:**
- 🚀 **100-200x speedup** for compute-intensive streaming analytics
- 🎯 **Zero configuration** - works out of the box
- 🔧 **Transparent** - no code changes required
- 🛡️ **Robust** - graceful fallback if compilation fails

**Perfect for:** High-frequency trading, real-time analytics, sensor data processing, financial calculations, ML feature engineering in streaming pipelines.
