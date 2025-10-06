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


cdef class FunctionPattern:
    """
    Detected patterns in function source code.

    Used to determine optimal compilation strategy.
    """

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

    def __cinit__(self, max_cache_size=1000):
        """
        Initialize Numba compiler.

        Args:
            max_cache_size: Maximum number of compiled functions to cache
        """
        # Initialize attributes - we'll check for numba at runtime
        self._numba_module = None
        self._numba_available = False

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
        # Check numba availability at runtime
        if not self._numba_available:
            try:
                import numba
                self._numba_module = numba
                self._numba_available = True
            except ImportError:
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
