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
import dis

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

    cpdef FunctionPattern _analyze_function(self, object func):
        """
        Analyze function using hybrid bytecode + AST approach.

        Strategy:
        1. Always try bytecode analysis first (works for all functions)
        2. If AST analysis is available, merge results (more detailed)
        3. Return combined pattern

        Returns:
            FunctionPattern object with detected patterns
        """
        # Start with bytecode analysis (always works)
        pattern = self._analyze_function_bytecode(func)

        # Try to enhance with AST analysis (more detailed but limited availability)
        try:
            source = inspect.getsource(func)
        except:
            # Source not available - bytecode analysis is sufficient
            logger.debug(f"AST analysis unavailable for {getattr(func, '__name__', '<lambda>')}, using bytecode only")
            return pattern

        # Parse AST to get more detailed patterns
        try:
            tree = ast.parse(source)
        except:
            # AST parsing failed - bytecode analysis is sufficient
            return pattern

        # Walk AST and merge patterns with bytecode results
        # AST provides more accurate counts
        ast_loop_count = 0
        ast_numpy_call_count = 0

        for node in ast.walk(tree):
            # Loops
            if isinstance(node, (ast.For, ast.While)):
                pattern.has_loops = True
                ast_loop_count += 1

            # NumPy usage
            elif isinstance(node, ast.Attribute):
                if 'np.' in ast.unparse(node):
                    pattern.has_numpy = True
                    ast_numpy_call_count += 1

            # Dict access (record['key'])
            elif isinstance(node, ast.Subscript):
                pattern.has_dict_access = True

            # List operations
            elif isinstance(node, ast.List):
                pattern.has_list_ops = True

        # Use AST counts if available (more accurate)
        if ast_loop_count > 0:
            pattern.loop_count = ast_loop_count
        if ast_numpy_call_count > 0:
            pattern.numpy_call_count = ast_numpy_call_count

        # String-based detection for libraries (AST enhancement)
        if 'pandas' in source or 'pd.' in source:
            pattern.has_pandas = True
        if 'pyarrow' in source or 'pa.' in source or 'pc.' in source:
            pattern.has_arrow = True

        return pattern

    cdef FunctionPattern _analyze_function_bytecode(self, object func):
        """
        Analyze function bytecode to detect patterns.

        Works for ALL functions (inline, lambda, file-defined, __main__ scope).
        Uses Python bytecode inspection via dis module.

        Returns:
            FunctionPattern object with detected patterns
        """
        pattern = FunctionPattern()

        try:
            # Get bytecode instructions
            instructions = list(dis.get_instructions(func))
        except Exception as e:
            logger.debug(f"Bytecode analysis failed for {getattr(func, '__name__', '<lambda>')}: {e}")
            return pattern

        # Track imported modules
        imported_modules = set()

        # Analyze bytecode opcodes
        for instr in instructions:
            opname = instr.opname
            argval = instr.argval

            # Loop detection
            if opname == 'FOR_ITER':
                pattern.has_loops = True
                pattern.loop_count += 1
            elif opname == 'JUMP_BACKWARD':
                # While loops use JUMP_BACKWARD
                if not pattern.has_loops:
                    pattern.has_loops = True
                    pattern.loop_count += 1

            # Import detection
            elif opname == 'IMPORT_NAME':
                if argval:
                    imported_modules.add(str(argval))

            # Dict/subscript access
            elif opname in ('BINARY_SUBSCR', 'STORE_SUBSCR'):
                pattern.has_dict_access = True

            # List operations
            elif opname in ('BUILD_LIST', 'LIST_APPEND', 'LIST_EXTEND'):
                pattern.has_list_ops = True

            # NumPy detection (from loaded globals)
            elif opname == 'LOAD_GLOBAL' or opname == 'LOAD_FAST':
                if argval and isinstance(argval, str):
                    argval_lower = argval.lower()
                    if 'np' in argval_lower or 'numpy' in argval_lower:
                        pattern.has_numpy = True
                        pattern.numpy_call_count += 1
                    # PyArrow detection (pa, pc module aliases)
                    if argval in ('pa', 'pc') or 'pyarrow' in argval_lower:
                        pattern.has_arrow = True

        # Analyze imported modules
        for module in imported_modules:
            module_lower = module.lower()

            if 'numpy' in module_lower or module == 'np':
                pattern.has_numpy = True

            if 'pandas' in module_lower or module == 'pd':
                pattern.has_pandas = True

            if 'pyarrow' in module_lower or module in ('pa', 'pc'):
                pattern.has_arrow = True

        return pattern

    cpdef CompilationStrategy _choose_strategy(self, FunctionPattern pattern):
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

        Note: Numba uses lazy compilation - actual compilation happens
        on first call with concrete types. We don't test-execute here
        because we can't reliably infer argument types.
        """
        try:
            compiled = self._numba_module.njit(func)
            logger.info(f"Compiled {func.__name__} with @njit (lazy compilation)")
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


# Export CompilationStrategy enum values as module constants
#  (Python can't access cdef enums directly)
STRATEGY_SKIP = 0      # Skip compilation (already fast)
STRATEGY_NJIT = 1      # @njit (scalar JIT)
STRATEGY_VECTORIZE = 2 # @vectorize (array JIT)
STRATEGY_AUTO = 3      # Auto-detect best strategy
