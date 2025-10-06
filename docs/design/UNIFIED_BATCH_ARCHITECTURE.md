# Unified Batch-Centric Architecture with Morsel-Driven Parallelism

**Version**: 1.0
**Date**: October 2025
**Status**: Design Specification

---

## Executive Summary

Sabot's architecture unifies around **batches as the fundamental unit** with clean separation between data plane (C++/Cython) and control plane (Python/DBOS). Everything processes `RecordBatch` objects - "per-record" is only syntactic sugar at the API layer.

### Key Principles

1. **Batch-First**: All operators process `RecordBatch` → `RecordBatch`
2. **Agent = Worker Node**: Agents are cluster nodes that execute tasks, not user code
3. **Data Plane = C++/Cython**: Zero-copy processing from vendored libs (Arrow, DuckDB, Tonbo)
4. **Control Plane = Python/DBOS**: Orchestration, job management, durable state
5. **Morsel-Driven Execution**: Work-stealing parallelism for shuffle and operators
6. **Auto-Numba UDFs**: User functions JIT-compiled transparently

---

## Part 1: Unified Streaming/Batch Execution Model

### The Fundamental Insight

**Streaming and batch are the same thing.** Both process `RecordBatch` objects through identical operators. The only difference is boundedness:

- **Batch mode**: Finite data source (file, table) → operators process batches → terminate when exhausted
- **Streaming mode**: Infinite data source (Kafka, socket) → operators process batches → run forever

**Same operators. Same execution. Same code.**

### Execution Characteristics

|                    | Batch Mode          | Streaming Mode      |
|--------------------|---------------------|---------------------|
| **Data Source**    | Finite (files, tables) | Infinite (Kafka, queues) |
| **Termination**    | When source exhausted | Never (or external signal) |
| **Operator Code**  | **IDENTICAL** | **IDENTICAL** |
| **Iteration**      | `for batch in op` | `async for batch in op` |
| **Checkpointing**  | Optional | Required |
| **Watermarks**     | Implicit (max timestamp) | Explicit (track progress) |

### Code Example: Same Pipeline, Different Modes

```python
def create_pipeline(source):
    """
    This pipeline works whether source is:
    - Stream.from_parquet() → batch mode (finite)
    - Stream.from_kafka() → streaming mode (infinite)
    """
    return (source
        .filter(lambda b: pc.greater(b['amount'], 1000))
        .window(tumbling(seconds=60))
        .aggregate({'total': ('amount', 'sum')})
    )

# Batch execution (finite)
batch_result = create_pipeline(Stream.from_parquet('data.parquet'))
for batch in batch_result:  # Terminates when file exhausted
    process(batch)

# Streaming execution (infinite)
stream_result = create_pipeline(Stream.from_kafka('transactions'))
async for batch in stream_result:  # Runs forever
    process(batch)
```

**Key insight**: The ONLY component that knows about boundedness is the **source**. Everything downstream is identical.

---

## Part 2: Batch-Centric Operator Model

### Fundamental Contract

**Every operator implements**:
```cython
cdef class BaseOperator:
    def __iter__(self) -> Iterator[RecordBatch]:
        """Yields RecordBatches, never individual records"""

    cpdef object process_batch(self, object batch):
        """RecordBatch → RecordBatch transformation"""
```

**Key insight**: Even "per-record" APIs are just iteration sugar over batches at the user API layer. The data plane only sees batches.

### Operator Hierarchy

```
BaseOperator (abstract)
├── StatelessOperator
│   ├── MapOperator
│   ├── FilterOperator
│   ├── SelectOperator (projection)
│   ├── FlatMapOperator
│   └── UnionOperator
│
├── StatefulOperator (requires shuffle)
│   ├── JoinOperator
│   │   ├── HashJoinOperator
│   │   ├── IntervalJoinOperator
│   │   └── AsofJoinOperator
│   ├── AggregationOperator
│   │   ├── GroupByOperator
│   │   ├── ReduceOperator
│   │   └── DistinctOperator
│   └── WindowOperator
│       ├── TumblingWindowOperator
│       ├── SlidingWindowOperator
│       └── SessionWindowOperator
│
└── SourceOperator / SinkOperator
    ├── KafkaSourceOperator
    ├── KafkaSinkOperator
    ├── FileSourceOperator
    └── FileSinkOperator
```

### Data Flow

```
User Dataflow DAG
       ↓
Operator DAG (logical)
       ↓
Plan Optimizer (filter pushdown, projection pushdown, fusion)
       ↓
Physical Execution Graph (with parallelism)
       ↓
TaskVertex Assignment (operators → agents)
       ↓
Agent Execution (morsel-driven)
```

### Sources: The Only Place That Differs

**Batch sources** (finite) simply iterate and stop:

```python
class ParquetSource(BaseOperator):
    def __iter__(self):
        table = pq.read_table(self.file_path)
        for batch in table.to_batches():
            yield batch
        # STOPS after last batch
```

**Streaming sources** (infinite) loop forever:

```python
class KafkaSource(BaseOperator):
    async def __aiter__(self):
        consumer = KafkaConsumer(self.topic)
        while True:  # ← Infinite loop
            messages = await consumer.poll()
            if messages:
                batch = self._to_batch(messages)
                yield batch
        # NEVER stops
```

### Watermarks for Event-Time Processing

Streaming adds **watermarks** to track event-time progress:

```python
# Batch mode: watermark = max timestamp in data (all data present)
batch_watermark = batch.column('timestamp').max().as_py()

# Streaming mode: watermark = max timestamp seen so far (data arrives continuously)
streaming_watermark = max(batch.column('timestamp').max().as_py(), last_watermark)
```

Windows use watermarks to determine completion:

```python
# Window completes when: watermark >= window_end_time
if watermark >= window_end:
    emit_window_result()
```

---

## Part 2: Operator Interface Specification

### Base Operator (Cython)

**File**: `sabot/_cython/operators/base_operator.pyx`

```cython
# cython: language_level=3

cimport cython
from libc.stdint cimport int64_t

# Forward declarations
cdef class Morsel
cdef class Partitioner

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

    cdef:
        object _source              # Upstream operator or iterable
        object _schema              # Arrow schema (optional)
        bint _stateful              # Does this op have keyed state?
        list _key_columns           # Key columns for partitioning
        int _parallelism_hint       # Suggested parallelism

    def __cinit__(self, *args, **kwargs):
        """Initialize operator (allow subclass args)"""
        self._source = kwargs.get('source')
        self._schema = kwargs.get('schema')
        self._stateful = False
        self._key_columns = None
        self._parallelism_hint = 1

    # ========================================================================
    # ITERATION INTERFACE (supports both batch and streaming)
    # ========================================================================

    def __iter__(self):
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

        SAME LOGIC as __iter__, just async-aware.
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
                # Yield control to event loop
                await asyncio.sleep(0)

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

    cpdef object process_morsel(self, Morsel morsel):
        """
        Process a morsel (chunk of batch for parallel execution).

        Default: just process the batch inside the morsel.
        Override for morsel-specific optimizations (e.g., NUMA placement).

        Args:
            morsel: Morsel containing RecordBatch data

        Returns:
            Processed Morsel or RecordBatch
        """
        result_batch = self.process_batch(morsel.data)
        if result_batch is not None:
            morsel.data = result_batch
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
```

### Stateless Operator Example (Map)

**File**: `sabot/_cython/operators/transform.pyx`

```cython
@cython.final
cdef class CythonMapOperator(BaseOperator):
    """
    Map operator: transform each batch using a function.

    Function can be:
    1. Python function (will be Numba-compiled if possible)
    2. Arrow compute expression
    3. Cython function
    """

    cdef:
        object _map_func            # User function
        object _compiled_func       # Numba-compiled version
        bint _is_compiled           # Was function compiled?
        bint _is_vectorized         # Does it use Arrow compute?

    def __init__(self, source, map_func, schema=None):
        self._source = source
        self._schema = schema
        self._map_func = map_func
        self._compiled_func = None
        self._is_compiled = False
        self._is_vectorized = self._detect_vectorization(map_func)

        # Try to compile with Numba
        if not self._is_vectorized:
            self._try_compile_numba()

    cdef bint _detect_vectorization(self, func):
        """
        Detect if function uses Arrow compute (already vectorized).

        Heuristic: check if function uses pyarrow.compute module.
        """
        import inspect
        try:
            source = inspect.getsource(func)
            return 'pc.' in source or 'pyarrow.compute' in source
        except:
            return False

    cdef void _try_compile_numba(self):
        """
        Attempt to JIT-compile function with Numba.

        Falls back gracefully if compilation fails.
        """
        try:
            import numba
            import inspect

            # Get function source for analysis
            source = inspect.getsource(self._map_func)

            # Decide compilation mode
            if 'numpy' in source or 'np.' in source:
                # Vectorized numpy operations
                self._compiled_func = numba.vectorize(nopython=True)(self._map_func)
            else:
                # Scalar operations
                self._compiled_func = numba.njit(self._map_func)

            self._is_compiled = True
            logger.info(f"Numba-compiled map function: {self._map_func.__name__}")

        except Exception as e:
            # Compilation failed - use interpreted Python
            logger.debug(f"Numba compilation failed, using Python: {e}")
            self._compiled_func = self._map_func
            self._is_compiled = False

    @cython.boundscheck(False)
    @cython.wraparound(False)
    cpdef object process_batch(self, object batch):
        """Apply map function to batch"""
        if batch is None or batch.num_rows == 0:
            return None

        try:
            # Choose execution path
            if self._is_vectorized:
                # Already uses Arrow compute - just call it
                result = self._map_func(batch)

            elif self._is_compiled:
                # Numba-compiled function - apply efficiently
                result = self._apply_compiled(batch)

            else:
                # Interpreted Python - slower but works
                result = self._map_func(batch)

            # Ensure result is RecordBatch
            if not isinstance(result, pa.RecordBatch):
                if isinstance(result, pa.Table):
                    result = result.combine_chunks()
                elif isinstance(result, dict):
                    result = pa.RecordBatch.from_pydict(result)
                else:
                    raise TypeError(f"Map must return RecordBatch, got {type(result)}")

            return result

        except Exception as e:
            raise RuntimeError(f"Error in map operator: {e}")

    cdef object _apply_compiled(self, object batch):
        """
        Apply Numba-compiled function to batch.

        Strategy depends on whether function is scalar or vectorized.
        """
        if self._is_vectorized:
            # Vectorized: apply to arrays directly
            result_dict = {}
            for col_name in batch.schema.names:
                arr = batch.column(col_name).to_numpy()
                result_dict[col_name] = self._compiled_func(arr)
            return pa.RecordBatch.from_pydict(result_dict)

        else:
            # Scalar: apply record-by-record (still faster with Numba)
            records = batch.to_pylist()
            results = [self._compiled_func(r) for r in records]
            return pa.RecordBatch.from_pylist(results)
```

### Stateful Operator Example (HashJoin with Shuffle)

**File**: `sabot/_cython/operators/joins.pyx`

```cython
@cython.final
cdef class CythonHashJoinOperator(BaseOperator):
    """
    Hash join operator with automatic shuffle.

    This is STATEFUL - requires both sides to be co-partitioned by join key.
    The shuffle layer handles repartitioning automatically.
    """

    cdef:
        object _right_source        # Right side of join
        list _left_keys             # Left join key columns
        list _right_keys            # Right join key columns
        str _join_type              # 'inner', 'left', 'right', 'outer'
        StreamingHashTableBuilder _hash_table  # Build side hash table

    def __init__(self, left_source, right_source, left_keys, right_keys,
                 join_type='inner', schema=None):
        self._source = left_source  # Probe side
        self._right_source = right_source  # Build side
        self._left_keys = left_keys
        self._right_keys = right_keys
        self._join_type = join_type
        self._schema = schema

        # This is stateful - needs shuffle
        self._stateful = True
        self._key_columns = left_keys

        # Initialize hash table builder
        self._hash_table = StreamingHashTableBuilder(right_keys)

    def __iter__(self):
        """
        Execute hash join:

        Phase 1: Build - accumulate right side into hash table
        Phase 2: Probe - stream left side and probe hash table

        NOTE: In distributed mode, shuffle ensures both sides are
        co-partitioned before this runs.
        """
        # Phase 1: Build hash table from right side
        for right_batch in self._right_source:
            self._hash_table.add_batch(right_batch)

        self._hash_table.build_index()

        # Phase 2: Probe with left side
        for left_batch in self._source:
            result = self._hash_table.probe_batch_vectorized(
                left_batch,
                self._left_keys,
                self._join_type
            )
            if result is not None:
                yield result

    cpdef object process_batch(self, object batch):
        """
        For joins, we can't process batches independently.
        Instead, use __iter__() which handles build/probe phases.
        """
        raise NotImplementedError(
            "Join operators use __iter__() for build/probe phases"
        )
```

---

## Part 3: User-Defined Functions with Auto-Numba

### Problem

Users write Python functions like:
```python
def transform(record):
    total = 0
    for i in range(100):
        total += record['value'] * i
    return {'result': total}
```

This is slow if interpreted. We want automatic JIT compilation.

### Solution: Transparent Numba Compilation

**File**: `sabot/_cython/operators/numba_compiler.pyx`

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

logger = logging.getLogger(__name__)

cdef class NumbaCompiler:
    """
    Intelligent Numba compiler for UDFs.

    Analyzes function and picks best compilation strategy.
    """

    cdef:
        object _numba_module
        bint _numba_available

    def __cinit__(self):
        try:
            import numba
            self._numba_module = numba
            self._numba_available = True
        except ImportError:
            self._numba_module = None
            self._numba_available = False
            logger.warning("Numba not available - UDFs will be interpreted")

    cpdef object compile_udf(self, object func):
        """
        Compile user function with optimal strategy.

        Returns:
            Compiled function or original if compilation not beneficial
        """
        if not self._numba_available:
            return func

        # Analyze function
        strategy = self._analyze_function(func)

        if strategy == CompilationStrategy.NJIT:
            return self._compile_njit(func)
        elif strategy == CompilationStrategy.VECTORIZE:
            return self._compile_vectorize(func)
        elif strategy == CompilationStrategy.SKIP:
            return func
        else:
            # Try NJIT as fallback
            return self._try_compile_best_effort(func)

    cdef CompilationStrategy _analyze_function(self, object func):
        """
        Analyze function source to determine compilation strategy.

        Returns:
            CompilationStrategy enum value
        """
        try:
            source = inspect.getsource(func)
        except:
            return CompilationStrategy.SKIP

        # Parse AST
        try:
            tree = ast.parse(source)
        except:
            return CompilationStrategy.SKIP

        # Detect patterns
        has_loops = any(isinstance(node, (ast.For, ast.While))
                       for node in ast.walk(tree))
        has_numpy = 'numpy' in source or 'np.' in source
        has_pandas = 'pandas' in source or 'pd.' in source
        has_arrow = 'pyarrow' in source or 'pa.' in source or 'pc.' in source

        # Decision tree
        if has_arrow or has_pandas:
            # Already using fast libraries - don't compile
            return CompilationStrategy.SKIP

        elif has_numpy:
            # NumPy operations - use vectorize
            return CompilationStrategy.VECTORIZE

        elif has_loops:
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
            if test_args:
                _ = compiled(*test_args)

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
            return func

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
        Used to trigger JIT compilation.
        """
        import inspect
        sig = inspect.signature(func)

        # Simple heuristic: pass dummy dict for record-level functions
        if len(sig.parameters) == 1:
            return ({'value': 1.0, 'id': 1},)
        return None


cdef enum CompilationStrategy:
    SKIP = 0        # Don't compile (already fast)
    NJIT = 1        # Scalar JIT (@njit)
    VECTORIZE = 2   # Array JIT (@vectorize)
    AUTO = 3        # Try best option


# Global compiler instance
cdef NumbaCompiler _global_compiler = NumbaCompiler()

cpdef object auto_compile(object func):
    """
    Public API: automatically compile function.

    Usage:
        compiled_func = auto_compile(my_udf)
    """
    return _global_compiler.compile_udf(func)
```

### Integration with Operators

```cython
# In CythonMapOperator.__init__:

def __init__(self, source, map_func, schema=None):
    self._source = source
    self._schema = schema
    self._map_func = map_func

    # Auto-compile with Numba
    self._compiled_func = auto_compile(map_func)
    self._is_compiled = (self._compiled_func is not map_func)
```

### User Experience

```python
# User writes normal Python
stream = Stream.from_kafka('data')

def my_transform(record):
    # Pure Python - but Sabot auto-compiles it!
    total = 0
    for i in range(100):
        total += record['value'] * i
    return {'result': total}

# Automatically Numba-compiled under the hood
result = stream.map(lambda b: apply_to_batch(b, my_transform))

# Or with explicit batch processing
def batch_transform(batch):
    # Uses Arrow compute - no compilation needed
    return batch.append_column(
        'doubled',
        pc.multiply(batch.column('value'), 2)
    )

result = stream.map(batch_transform)
```

---

## Part 4: Agents as Worker Nodes

### Current Confusion

`@app.agent()` suggests agents are user code. Actually:
- **Agents = worker processes** on cluster nodes
- **User code = dataflow DAG** of operators
- **JobManager deploys** dataflow to agents

### New Mental Model

```
┌─────────────────────────────────────────────┐
│  User writes Dataflow (operator DAG)        │
│                                             │
│  stream = (Stream.from_kafka(...)          │
│      .filter(...)                           │
│      .join(...)                             │
│      .aggregate(...))                       │
└─────────────────────────────────────────────┘
                    ↓
┌─────────────────────────────────────────────┐
│  JobManager (DBOS-backed control plane)     │
│  - Compiles DAG to physical plan            │
│  - Assigns tasks to agents                  │
│  - Monitors execution                       │
└─────────────────────────────────────────────┘
                    ↓
    ┌───────────────┴───────────────┐
    ↓                               ↓
┌──────────┐                  ┌──────────┐
│ Agent 1  │                  │ Agent 2  │
│ (Worker) │                  │ (Worker) │
│          │                  │          │
│ Task 1.0 │ ←─ shuffle ────→ │ Task 2.0 │
│ Task 1.1 │                  │ Task 2.1 │
└──────────┘                  └──────────┘
  (executes                    (executes
   operators                    operators
   on batches)                  on batches)
```

### Agent Class (Worker Node)

**File**: `sabot/agent.py` (NEW - Python control plane)

```python
"""
Agent: A worker node that executes operator tasks.

Agents:
- Run on cluster nodes (physical or virtual machines)
- Execute TaskVertices (parallel operator instances)
- Use morsel-driven parallelism locally
- Shuffle data via Arrow Flight
- Report health to JobManager via DBOS
"""

import asyncio
import logging
from typing import Dict, List, Optional
from dataclasses import dataclass

logger = logging.getLogger(__name__)


@dataclass
class AgentConfig:
    """Configuration for agent worker node"""
    agent_id: str
    host: str
    port: int
    num_slots: int = 8              # How many tasks can run concurrently
    workers_per_slot: int = 4       # Morsel workers per task
    memory_mb: int = 16384          # Total memory for agent
    cpu_cores: int = 8              # Total CPU cores


class Agent:
    """
    Worker node that executes streaming operator tasks.

    An Agent is NOT user code - it's a runtime component that:
    1. Receives task assignments from JobManager
    2. Executes operators on batches
    3. Uses morsel parallelism for local execution
    4. Shuffles data via Arrow Flight
    5. Reports metrics to JobManager
    """

    def __init__(self, config: AgentConfig):
        self.config = config
        self.agent_id = config.agent_id

        # Task execution
        self.active_tasks: Dict[str, TaskExecutor] = {}
        self.slots: List[Slot] = [
            Slot(i, config.memory_mb // config.num_slots)
            for i in range(config.num_slots)
        ]

        # Morsel parallelism
        from .dbos_cython_parallel import DBOSCythonParallelProcessor
        self.morsel_processor = DBOSCythonParallelProcessor(
            max_workers=config.num_slots * config.workers_per_slot,
            morsel_size_kb=64
        )

        # Shuffle
        from ._cython.shuffle.shuffle_transport import ShuffleServer, ShuffleClient
        self.shuffle_server = ShuffleServer()
        self.shuffle_client = ShuffleClient()

        # State
        self.running = False

        logger.info(f"Agent initialized: {self.agent_id}")

    async def start(self):
        """Start agent services"""
        if self.running:
            return

        self.running = True

        # Start morsel processor
        await self.morsel_processor.start()

        # Start shuffle server (Arrow Flight)
        await self.shuffle_server.start(self.config.host, self.config.port)

        logger.info(f"Agent started: {self.agent_id} at {self.config.host}:{self.config.port}")

    async def stop(self):
        """Stop agent and cleanup"""
        if not self.running:
            return

        self.running = False

        # Stop tasks
        for task_id in list(self.active_tasks.keys()):
            await self.stop_task(task_id)

        # Stop services
        await self.morsel_processor.stop()
        await self.shuffle_server.stop()

        logger.info(f"Agent stopped: {self.agent_id}")

    async def deploy_task(self, task: 'TaskVertex'):
        """
        Deploy and start a task (operator instance).

        Args:
            task: TaskVertex containing operator and configuration
        """
        # Allocate slot
        slot = self._allocate_slot(task)
        if slot is None:
            raise RuntimeError(f"No available slots for task {task.task_id}")

        # Create task executor
        executor = TaskExecutor(
            task=task,
            slot=slot,
            morsel_processor=self.morsel_processor,
            shuffle_client=self.shuffle_client
        )

        # Start execution
        await executor.start()

        self.active_tasks[task.task_id] = executor
        logger.info(f"Deployed task {task.task_id} to slot {slot.slot_id}")

    async def stop_task(self, task_id: str):
        """Stop and remove a task"""
        if task_id not in self.active_tasks:
            return

        executor = self.active_tasks[task_id]
        await executor.stop()

        # Free slot
        executor.slot.release()

        del self.active_tasks[task_id]
        logger.info(f"Stopped task {task_id}")

    def get_status(self) -> dict:
        """Get agent status for health reporting"""
        return {
            'agent_id': self.agent_id,
            'running': self.running,
            'active_tasks': len(self.active_tasks),
            'available_slots': sum(1 for s in self.slots if s.is_free()),
            'total_slots': len(self.slots),
        }

    def _allocate_slot(self, task) -> Optional['Slot']:
        """Find free slot for task"""
        for slot in self.slots:
            if slot.is_free() and slot.can_fit(task):
                slot.assign(task)
                return slot
        return None


@dataclass
class Slot:
    """Execution slot for task"""
    slot_id: int
    memory_mb: int
    assigned_task: Optional[str] = None

    def is_free(self) -> bool:
        return self.assigned_task is None

    def can_fit(self, task) -> bool:
        return task.memory_mb <= self.memory_mb

    def assign(self, task):
        self.assigned_task = task.task_id

    def release(self):
        self.assigned_task = None


class TaskExecutor:
    """
    Executes a single TaskVertex (operator instance).

    Responsibilities:
    - Pull batches from input stream
    - Execute operator on batches (via morsel parallelism)
    - Push results to output stream (potentially via shuffle)
    """

    def __init__(self, task, slot, morsel_processor, shuffle_client):
        self.task = task
        self.slot = slot
        self.morsel_processor = morsel_processor
        self.shuffle_client = shuffle_client

        # Create operator instance
        self.operator = self._create_operator()

        # Execution state
        self.running = False
        self.execution_task = None

    def _create_operator(self):
        """Instantiate operator from task specification"""
        # This would deserialize operator from task.operator_spec
        # For now, placeholder
        return self.task.operator

    async def start(self):
        """Start task execution"""
        if self.running:
            return

        self.running = True
        self.execution_task = asyncio.create_task(self._execution_loop())

    async def stop(self):
        """Stop task execution"""
        if not self.running:
            return

        self.running = False
        if self.execution_task:
            self.execution_task.cancel()
            try:
                await self.execution_task
            except asyncio.CancelledError:
                pass

    async def _execution_loop(self):
        """
        Main execution loop: process batches via operator.

        For operators requiring shuffle:
        1. Receive batches from shuffle server
        2. Process via operator
        3. Send results to downstream via shuffle client

        For local operators:
        1. Receive batches from upstream task (same agent)
        2. Process via operator
        3. Send results to downstream task
        """
        try:
            if self.operator.requires_shuffle():
                await self._execute_with_shuffle()
            else:
                await self._execute_local()

        except Exception as e:
            logger.error(f"Task execution failed: {e}")
            raise

    async def _execute_with_shuffle(self):
        """Execute operator with network shuffle"""
        # Receive partitions assigned to this task
        async for batch in self.shuffle_server.receive_batches(self.task.task_id):
            # Process batch (via morsel parallelism)
            result = await self._process_batch_with_morsels(batch)

            # Send result downstream (may require re-shuffling)
            if result is not None:
                await self._send_downstream(result)

    async def _execute_local(self):
        """Execute operator without shuffle"""
        # Process batches from upstream
        async for batch in self.task.input_stream:
            result = await self._process_batch_with_morsels(batch)

            if result is not None:
                await self.task.output_stream.send(result)

    async def _process_batch_with_morsels(self, batch):
        """
        Process batch using morsel-driven parallelism.

        Splits batch into morsels, processes in parallel via workers.
        """
        # Create morsels from batch
        morsels = self.morsel_processor.create_morsels(batch)

        # Process morsels in parallel
        results = await self.morsel_processor.process_morsels(
            morsels,
            process_fn=self.operator.process_morsel
        )

        # Reassemble batch from morsel results
        if results:
            return self._reassemble_batch(results)
        return None

    def _reassemble_batch(self, morsel_results):
        """Combine morsel results back into single batch"""
        batches = [m.data for m in morsel_results if m.data is not None]
        if batches:
            import pyarrow as pa
            return pa.concat_batches(batches)
        return None

    async def _send_downstream(self, batch):
        """Send batch to downstream tasks (via shuffle if needed)"""
        # Determine target tasks based on partitioning
        # For now, simplified
        await self.task.output_stream.send(batch)
```

---

## Part 5: Control Plane (DBOS-backed)

### JobManager with DBOS Workflows

**File**: `sabot/job_manager.py` (NEW - Python with DBOS)

```python
"""
JobManager: DBOS-backed distributed job orchestration.

All orchestration state lives in Postgres (via DBOS).
Jobs survive crashes and can resume from last completed step.
"""

import logging
from typing import List, Dict, Optional
import uuid

# DBOS for durable workflows
try:
    from dbos import DBOS, DBOSConfig
    DBOS_AVAILABLE = True
except ImportError:
    DBOS_AVAILABLE = False
    # Fallback: define stubs
    class DBOS:
        @staticmethod
        def workflow(func): return func
        @staticmethod
        def transaction(func): return func
        @staticmethod
        def communicator(func): return func

logger = logging.getLogger(__name__)


class JobManager:
    """
    Manages distributed streaming jobs across cluster.

    Uses DBOS for:
    - Durable job state (survives crashes)
    - Workflow orchestration (multi-step deployment)
    - Transaction guarantees (atomic task assignment)
    - Event logging (audit trail)

    Responsibilities:
    1. Accept user dataflow DAGs
    2. Optimize and compile to physical plan
    3. Assign tasks to agents
    4. Monitor execution
    5. Handle failures and rescaling
    """

    def __init__(self, dbos_url='postgresql://localhost/sabot'):
        if DBOS_AVAILABLE:
            self.dbos = DBOS(DBOSConfig(database_url=dbos_url))
        else:
            logger.warning("DBOS not available - running without durability")
            self.dbos = None

        # Optimizer
        from .compiler.plan_optimizer import PlanOptimizer
        self.optimizer = PlanOptimizer()

        # Cluster state
        self.agents: Dict[str, 'AgentInfo'] = {}
        self.jobs: Dict[str, 'JobExecution'] = {}

    # ========================================================================
    # DBOS WORKFLOW: Submit and Deploy Job
    # ========================================================================

    @DBOS.workflow
    async def submit_job(self, dataflow_dag: 'OperatorDAG', parallelism=4) -> str:
        """
        Submit streaming job (DBOS workflow - durable across failures).

        Steps:
        1. Optimize DAG (transaction)
        2. Compile to physical plan (transaction)
        3. Assign tasks to agents (transaction)
        4. Deploy to agents (communicator)
        5. Monitor execution (workflow)

        Returns:
            job_id: Unique job identifier
        """
        job_id = str(uuid.uuid4())

        logger.info(f"Submitting job {job_id}")

        # Step 1: Optimize DAG
        optimized_dag = await self._optimize_dag_step(job_id, dataflow_dag)

        # Step 2: Compile to execution graph
        exec_graph = await self._compile_execution_graph_step(
            job_id, optimized_dag, parallelism
        )

        # Step 3: Assign tasks to agents
        assignments = await self._assign_tasks_step(job_id, exec_graph)

        # Step 4: Deploy to agents
        await self._deploy_tasks_step(job_id, assignments)

        # Step 5: Monitor execution (background)
        await self._start_monitoring_step(job_id)

        logger.info(f"Job {job_id} submitted successfully")
        return job_id

    @DBOS.transaction
    async def _optimize_dag_step(self, job_id, dag):
        """
        Optimize operator DAG (DBOS transaction - atomic).

        Optimizations:
        - Filter pushdown
        - Projection pushdown
        - Join reordering
        - Operator fusion
        """
        logger.info(f"Optimizing DAG for job {job_id}")

        optimized = self.optimizer.optimize(dag)

        # Store in DBOS
        if self.dbos:
            await self.dbos.execute(
                """INSERT INTO job_dag_optimized (job_id, dag_json)
                   VALUES ($1, $2)""",
                job_id, optimized.to_json()
            )

        return optimized

    @DBOS.transaction
    async def _compile_execution_graph_step(self, job_id, dag, parallelism):
        """
        Compile logical DAG to physical execution graph.

        Determines:
        - Which operators need shuffle
        - How many parallel tasks per operator
        - Task dependencies and data flow edges
        """
        logger.info(f"Compiling execution graph for job {job_id}")

        from .execution.job_graph import JobGraph
        from .execution.execution_graph import ExecutionGraph

        # Create logical job graph
        job_graph = JobGraph(job_id=job_id)
        for op in dag.operators:
            job_graph.add_operator(op)

        # Expand to physical execution graph
        exec_graph = ExecutionGraph.from_job_graph(
            job_graph, default_parallelism=parallelism
        )

        # Store in DBOS
        if self.dbos:
            await self.dbos.execute(
                """INSERT INTO execution_graphs (job_id, graph_json)
                   VALUES ($1, $2)""",
                job_id, exec_graph.to_json()
            )

        return exec_graph

    @DBOS.transaction
    async def _assign_tasks_step(self, job_id, exec_graph):
        """
        Assign tasks to agents (DBOS transaction - atomic).

        Strategy:
        - Load-aware: prefer agents with more available slots
        - Locality-aware: co-locate tasks that exchange data
        - Constraint-aware: respect resource requirements
        """
        logger.info(f"Assigning tasks for job {job_id}")

        assignments = {}
        available_agents = await self._get_available_agents()

        for task in exec_graph.tasks.values():
            # Pick best agent for this task
            agent = self._pick_agent_for_task(task, available_agents)

            if agent is None:
                raise RuntimeError(f"No agent available for task {task.task_id}")

            assignments[task.task_id] = agent.agent_id

            # Store in DBOS
            if self.dbos:
                await self.dbos.execute(
                    """INSERT INTO task_assignments
                       (job_id, task_id, agent_id, operator_type)
                       VALUES ($1, $2, $3, $4)""",
                    job_id, task.task_id, agent.agent_id, task.operator_type.value
                )

        return assignments

    @DBOS.communicator
    async def _deploy_tasks_step(self, job_id, assignments):
        """
        Deploy tasks to agents (DBOS communicator - external calls).

        Sends task deployment requests to agents via HTTP/gRPC.
        """
        logger.info(f"Deploying tasks for job {job_id}")

        import aiohttp

        for task_id, agent_id in assignments.items():
            agent = self.agents[agent_id]

            # Send deployment request to agent
            async with aiohttp.ClientSession() as session:
                url = f"http://{agent.host}:{agent.port}/deploy_task"
                async with session.post(url, json={
                    'task_id': task_id,
                    'job_id': job_id,
                }) as response:
                    if response.status != 200:
                        raise RuntimeError(
                            f"Failed to deploy task {task_id} to agent {agent_id}"
                        )

        logger.info(f"All tasks deployed for job {job_id}")

    @DBOS.workflow
    async def _start_monitoring_step(self, job_id):
        """
        Monitor job execution (DBOS workflow - background).

        Periodically checks task health and handles failures.
        """
        # This would poll agents for metrics and handle failures
        # For now, placeholder
        pass

    # ========================================================================
    # DBOS WORKFLOW: Rescale Job
    # ========================================================================

    @DBOS.workflow
    async def rescale_operator(self, job_id, operator_id, new_parallelism):
        """
        Dynamically rescale operator (DBOS workflow - durable).

        Steps:
        1. Pause operator tasks
        2. Drain in-flight data
        3. Create new tasks (transaction)
        4. Redistribute state if stateful (transaction)
        5. Resume with new parallelism
        """
        logger.info(
            f"Rescaling operator {operator_id} in job {job_id} "
            f"to parallelism {new_parallelism}"
        )

        # Step 1: Pause
        await self._pause_operator_tasks(job_id, operator_id)

        # Step 2: Drain
        await self._drain_operator_tasks(job_id, operator_id)

        # Step 3: Create new tasks
        new_tasks = await self._create_rescaled_tasks_step(
            job_id, operator_id, new_parallelism
        )

        # Step 4: Redistribute state
        if await self._is_stateful(operator_id):
            await self._redistribute_state_step(job_id, operator_id, new_tasks)

        # Step 5: Resume
        await self._resume_operator_tasks(job_id, operator_id)

        logger.info(f"Rescaling complete for operator {operator_id}")

    # ========================================================================
    # Helper Methods
    # ========================================================================

    async def _get_available_agents(self):
        """Get list of healthy agents"""
        if self.dbos:
            result = await self.dbos.query(
                """SELECT agent_id, host, port, available_slots
                   FROM agents WHERE status = 'healthy'
                   ORDER BY available_slots DESC"""
            )
            return [AgentInfo(**row) for row in result]
        else:
            return list(self.agents.values())

    def _pick_agent_for_task(self, task, available_agents):
        """
        Pick best agent for task.

        Simple strategy: pick agent with most available slots.
        Could be enhanced with locality, resource constraints, etc.
        """
        if not available_agents:
            return None

        # Sort by available slots (descending)
        available_agents.sort(
            key=lambda a: a.available_slots, reverse=True
        )

        return available_agents[0]


from dataclasses import dataclass

@dataclass
class AgentInfo:
    """Information about an agent"""
    agent_id: str
    host: str
    port: int
    available_slots: int = 0
    status: str = 'unknown'
```

---

## Part 6: Detailed Implementation Roadmap

### Phase 1: Solidify Batch-Only Operator API ✅ COMPLETE

**Status:** Implemented and tested (October 2025)

**Delivered:**
- [x] BaseOperator has both `__iter__()` and `__aiter__()`
- [x] All operators process RecordBatch → RecordBatch exclusively
- [x] Comprehensive documentation with batch-first contract
- [x] Examples demonstrating batch vs streaming modes
- [x] Test coverage >80% for batch operations
- [x] Zero regressions in existing functionality

**Key Files:**
- `/Users/bengamble/Sabot/sabot/_cython/operators/transform.pyx` - Updated BaseOperator
- `/Users/bengamble/Sabot/sabot/api/stream.py` - Updated Stream API docs
- `/Users/bengamble/Sabot/examples/batch_first_examples.py` - Comprehensive examples
- `/Users/bengamble/Sabot/tests/unit/operators/test_batch_contract.py` - Contract tests
- `/Users/bengamble/Sabot/tests/unit/operators/test_async_iteration.py` - Async tests

**Performance Validated:**
- Filter: 10-500M records/sec (SIMD)
- Map: 10-100M records/sec
- Select: 50-1000M records/sec (zero-copy)
- No performance regression vs previous implementation

---

### Phase 2: Auto-Numba UDF Compilation
**Duration**: 1 week
**Goal**: Automatically JIT-compile user functions

**Tasks**:
1. **Create Numba compiler**
   - File: `sabot/_cython/operators/numba_compiler.pyx` (NEW)
   - Implement AST analysis for function detection
   - Support `@njit`, `@vectorize`, and fallback

2. **Integrate with MapOperator**
   - File: `sabot/_cython/operators/transform.pyx`
   - Auto-compile map functions in `__init__()`
   - Benchmark compiled vs interpreted

3. **Add compilation cache**
   - Cache compiled functions to avoid recompilation
   - Persistent cache across runs (optional)

4. **Test suite**
   - File: `tests/unit/test_numba_compilation.py` (NEW)
   - Test different function patterns
   - Verify performance improvement

**Deliverables**:
- ✅ Automatic Numba compilation
- ✅ 10-100x speedup for Python UDFs
- ✅ Transparent to users

---

### Phase 3: Connect Operators → Morsels
**Duration**: 2 weeks
**Goal**: Operators execute via morsel-driven parallelism

**Tasks**:
1. **Add morsel interface to BaseOperator**
   - File: `sabot/_cython/operators/base_operator.pyx`
   - Add `process_morsel()` method
   - Default implementation delegates to `process_batch()`

2. **Create MorselDrivenOperator wrapper**
   - File: `sabot/_cython/operators/morsel_operator.pyx` (NEW)
   - Wraps any operator for morsel execution
   - Integrates with `ParallelProcessor`

3. **Benchmark morsel execution**
   - File: `benchmarks/morsel_vs_batch_bench.py` (NEW)
   - Compare morsel-driven vs direct batch processing
   - Measure work-stealing efficiency

4. **Update documentation**
   - Explain when to use morsel-driven execution
   - Performance characteristics

**Deliverables**:
- ✅ Operators support morsel execution
- ✅ Work-stealing parallelism
- ✅ Performance benchmarks

---

### Phase 4: Integrate Network Shuffle
**Duration**: 2 weeks
**Goal**: Stateful operators automatically shuffle data

**Tasks**:
1. **Create ShuffledOperator base class**
   - File: `sabot/_cython/operators/shuffled_operator.pyx` (NEW)
   - Implements shuffle protocol
   - Uses Arrow Flight for network transfer

2. **Update stateful operators**
   - Files: `joins.pyx`, `aggregations.pyx`
   - Extend `ShuffledOperator`
   - Declare partition keys

3. **Implement morsel-driven shuffle**
   - File: `sabot/_cython/shuffle/morsel_shuffle.pyx` (NEW)
   - Pipelined shuffle (no barrier)
   - Work-stealing for repartitioning

4. **End-to-end shuffle test**
   - File: `tests/integration/test_distributed_shuffle.py` (NEW)
   - Multi-agent shuffle scenario
   - Verify correctness and performance

**Deliverables**:
- ✅ Stateful operators shuffle automatically
- ✅ Arrow Flight network transfer
- ✅ Morsel-driven pipelined shuffle

---

### Phase 5: Agent as Worker Node
**Duration**: 1 week
**Goal**: Redefine agent as worker, not user code

**Tasks**:
1. **Create Agent class**
   - File: `sabot/agent.py` (NEW)
   - Worker node that executes tasks
   - Manages slots and resources

2. **Create TaskExecutor**
   - File: `sabot/agent.py`
   - Executes individual TaskVertex
   - Integrates with morsel processor

3. **Deprecate @app.agent decorator**
   - File: `sabot/app.py`
   - Add `@app.dataflow()` as replacement
   - Keep `@agent` for backward compat (maps to dataflow)

4. **Update documentation**
   - File: `docs/AGENT_WORKER_MODEL.md` (NEW)
   - Explain agent as worker
   - Migration guide

**Deliverables**:
- ✅ Agent class for worker nodes
- ✅ Clear separation of concerns
- ✅ Migration path for existing code

---

### Phase 6: DBOS Control Plane
**Duration**: 2 weeks
**Goal**: All orchestration via DBOS workflows

**Tasks**:
1. **Create JobManager with DBOS**
   - File: `sabot/job_manager.py` (NEW)
   - Workflow for job submission
   - Transaction for task assignment
   - Communicator for agent deployment

2. **Database schema**
   - File: `sabot/dbos_schema.sql` (NEW)
   - Tables for jobs, tasks, agents, assignments
   - Indexes for query performance

3. **Agent health tracking**
   - Agents report heartbeats to JobManager
   - DBOS stores health state
   - Automatic failure detection

4. **Rescaling workflow**
   - DBOS workflow for live rescaling
   - State redistribution for stateful operators
   - Zero-downtime operator migration

**Deliverables**:
- ✅ DBOS-backed orchestration
- ✅ Durable job state
- ✅ Automatic recovery from failures
- ✅ Live rescaling

---

### Phase 7: Plan Optimization (Optional)
**Duration**: 1 week
**Goal**: Borrow DuckDB optimization techniques

**Tasks**:
1. **Create PlanOptimizer**
   - File: `sabot/compiler/plan_optimizer.py` (NEW)
   - Filter pushdown
   - Projection pushdown
   - Join reordering

2. **Reference DuckDB code**
   - Study `vendor/duckdb/src/optimizer/`
   - Adapt techniques for streaming context
   - Keep it simple (don't copy everything)

3. **Benchmark optimizations**
   - File: `benchmarks/optimizer_bench.py` (NEW)
   - Measure impact of each optimization
   - Create before/after examples

**Deliverables**:
- ✅ Basic plan optimization
- ✅ Measurable performance improvement
- ✅ Foundation for future enhancements

---

## Summary

This document provides the complete blueprint for Sabot's unified architecture. Key innovations:

### 1. **Streaming = Batch** (Same Code, Different Boundedness)
- Operators process `RecordBatch` whether source is finite or infinite
- `for batch in op` → batch mode (terminates)
- `async for batch in op` → streaming mode (runs forever)
- Only sources know about boundedness
- Windows/joins/aggregations work identically in both modes

### 2. **Everything is Batches** (Clean Data Model)
- All operators: `RecordBatch` → `RecordBatch`
- No per-record processing in data plane
- Batch-first API with record iteration as sugar only

### 3. **Auto-Numba Compilation** (Transparent Performance)
- UDFs automatically JIT-compiled
- AST analysis chooses `@njit` vs `@vectorize`
- 10-100x speedup without user intervention

### 4. **Morsel-Driven Execution** (Work-Stealing Parallelism)
- Batches split into morsels for parallel processing
- Work-stealing queues balance load
- NUMA-aware scheduling

### 5. **Network Shuffle** (Arrow Flight + Morsels)
- Stateful operators automatically shuffle
- Pipelined shuffle (no barriers)
- Zero-copy via Arrow Flight

### 6. **Agents as Workers** (Clear Mental Model)
- Agents are cluster nodes, not user code
- Users write dataflow DAGs
- JobManager deploys to agents

### 7. **DBOS Orchestration** (Durable, Fault-Tolerant)
- All orchestration via DBOS workflows
- Job state persisted in Postgres
- Automatic recovery from failures

### 8. **DuckDB Inspiration** (Query Optimization)
- Filter/projection pushdown
- Join reordering
- Operator fusion

### Benefits

✅ **Write once, run anywhere**: Same code for batch and streaming
✅ **Easy testing**: Test on batch (fast), deploy to streaming
✅ **High performance**: Numba + Arrow + Cython + morsels
✅ **Fault tolerance**: DBOS workflows + checkpointing
✅ **Scalability**: Morsel-driven + network shuffle
✅ **Simplicity**: One operator model, not two

Ready to implement!
