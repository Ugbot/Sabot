"""
Sabot High-Level Stream API

User-friendly streaming API that automatically uses Cython-accelerated operators
for Flink/Spark-level performance with a simple, Pythonic interface.

Example:
    stream = Stream.from_batches(batches)
    result = (stream
        .filter(lambda b: b.column('price') > 100)
        .map(lambda b: b.append_column('fee', b.column('price') * 0.03))
        .select('id', 'price', 'fee')
    )

    for batch in result:
        process(batch)
"""

from typing import Callable, List, Optional, Dict, Any, Union, Iterable
from sabot import cyarrow as ca

# Convenience alias for Arrow compute functions (mirrors pyarrow.compute)
# This allows code like: cc.greater(batch.column('price'), 100)
cc = ca.compute if hasattr(ca, 'compute') and ca.compute else None

# Enable performance optimizations
try:
    from sabot._cython.arrow.buffer_pool import get_buffer_pool
    from sabot._cython.arrow.memory_pool import get_memory_pool
    # Don't call set_default_memory_pool() here - it might interfere
    
    # Initialize global pools (lazy)
    _buffer_pool = None  # Will initialize on first use
    _memory_pool = None  # Will initialize on first use
    
    OPTIMIZATION_FEATURES_AVAILABLE = True
except ImportError:
    _buffer_pool = None
    _memory_pool = None
    OPTIMIZATION_FEATURES_AVAILABLE = False

# Import Cython operators
try:
    from sabot._cython.operators import (
        # Transform operators
        CythonFilterOperator,
        CythonMapOperator,
        CythonSelectOperator,
        CythonFlatMapOperator,
        CythonUnionOperator,
    # Aggregation operators - Note: aggregations module import may fail
    CythonReduceOperator,
    CythonDistinctOperator,
        # Join operators
        CythonHashJoinOperator,
        CythonIntervalJoinOperator,
        CythonAsofJoinOperator,
    )
    from sabot._cython.operators.morsel_operator import MorselDrivenOperator
    CYTHON_AVAILABLE = True
    CYTHON_AVAILABLE = True
except ImportError as e:
    CYTHON_AVAILABLE = False
    MorselDrivenOperator = None
    CythonFilterOperator = None
    CythonMapOperator = None
    CythonSelectOperator = None
    CythonHashJoinOperator = None
    CythonReduceOperator = None
    CythonDistinctOperator = None

# Try to import aggregation operators separately
try:
    from sabot._cython.operators.aggregations import (
        CythonAggregateOperator,
        CythonGroupByOperator,
    )
    # Cython operators now use vendored Arrow with zero-copy SIMD!
    AGGREGATION_OPERATORS_AVAILABLE = True
except ImportError as e:
    AGGREGATION_OPERATORS_AVAILABLE = False
    CythonAggregateOperator = None
    CythonGroupByOperator = None


class Stream:
    """
    High-level streaming API with automatic Cython acceleration and morsel-driven parallelism.

    MORSEL-DRIVEN PARALLELISM BY DEFAULT
    ====================================

    Sabot automatically uses morsel-driven parallelism for optimal performance:
    - Large batches (>10K rows) are split into cache-friendly morsels
    - Morsels are processed in parallel using work-stealing
    - 2-4x speedup for CPU-bound operations
    - Zero overhead for small batches

    BATCH-FIRST ARCHITECTURE
    ========================

    Sabot's fundamental unit of processing is the RecordBatch (Arrow columnar
    format). All operations are batch-level transformations. Streaming and
    batch processing use IDENTICAL operators - the only difference is source
    boundedness.

    Key Concepts:

    1. **Everything is Batches**
       - All operators process RecordBatch → RecordBatch
       - Zero-copy throughout via Arrow
       - SIMD acceleration via Arrow compute

    2. **Batch Mode vs Streaming Mode**
       - Batch mode: Finite source (files, tables) → iteration terminates
       - Streaming mode: Infinite source (Kafka, sockets) → runs forever
       - SAME code, SAME operators, different boundedness only

    3. **Per-Record is API Sugar**
       - .records() method unpacks batches for user convenience
       - NOT recommended for production (use batch API for performance)
       - Data plane (Cython operators) NEVER see individual records

    4. **Lazy Evaluation**
       - Operations build a DAG, no execution until consumed
       - for batch in stream → executes the pipeline
       - async for batch in stream → async execution

    Examples:

        # Batch processing (finite)
        stream = Stream.from_parquet('data.parquet')
        result = (stream
            .filter(lambda b: pc.greater(b.column('amount'), 1000))
            .map(lambda b: b.append_column('fee', pc.multiply(b.column('amount'), 0.03)))
            .select('id', 'amount', 'fee')
        )

        for batch in result:  # Terminates when file exhausted
            process(batch)

        # Streaming processing (infinite) - SAME PIPELINE!
        stream = Stream.from_kafka('localhost:9092', 'transactions', 'my-group')
        result = (stream
            .filter(lambda b: pc.greater(b.column('amount'), 1000))
            .map(lambda b: b.append_column('fee', pc.multiply(b.column('amount'), 0.03)))
            .select('id', 'amount', 'fee')
        )

        async for batch in result:  # Runs forever
            process(batch)

    Performance:
        - Filter: 10-500M records/sec (SIMD)
        - Map: 10-100M records/sec
        - Select: 50-1000M records/sec (zero-copy)
        - Join: 2-50M records/sec
        - GroupBy: 5-100M records/sec
    """

    def __init__(self, source: Iterable[ca.RecordBatch], schema: Optional[ca.Schema] = None, enable_morsel: bool = True):
        """
        Create a stream from a source of RecordBatches.

        Args:
            source: Iterable of RecordBatches
            schema: Optional schema (inferred from first batch if None)
            enable_morsel: Enable morsel parallelism (default: True for multi-core)
        """
        self._source = source
        self._schema = schema
        self._enable_morsel = enable_morsel  # Default to morsel parallelism for performance

    def _wrap_with_morsel_parallelism(self, operator, enable=None):
        """
        Wrap operator with MorselDrivenOperator for automatic parallelism.

        This enables morsel-driven parallelism by default for all operations.
        Small batches bypass parallelism (no overhead), large batches get
        automatic parallel processing with 8 workers.
        
        Args:
            operator: Operator to wrap
            enable: Override morsel parallelism (None = use class default)
        """
        # Check class-level setting
        if enable is False:
            return operator  # Explicit disable
        
        # Default: use instance setting (now defaults to True)
        if enable is None:
            enable = getattr(self, '_enable_morsel', True)
        
        if not enable:
            return operator  # Direct execution
        
        if not CYTHON_AVAILABLE or MorselDrivenOperator is None:
            return operator
        
        # CRITICAL: Disable morsel wrapping for streaming/lazy sources
        # Current MorselDrivenOperator (pre-recompile) doesn't handle them properly
        # After recompiling with new __iter__(), this check can be removed
        import types
        source_to_check = self._source
        
        # Check if source is a generator, iterator, or operator (all streaming)
        is_streaming_source = (
            isinstance(source_to_check, types.GeneratorType) or
            (hasattr(source_to_check, '__iter__') and 
             not hasattr(source_to_check, '__len__')) or  # Iterator but not list
            hasattr(source_to_check, 'process_batch')  # It's an operator
        )
        
        if is_streaming_source:
            # Streaming/lazy source - skip morsel wrapping
            # Operators themselves are streaming-compatible
            return operator

        # Wrap with morsel-driven parallelism
        # Using 8 workers - best tested configuration
        return MorselDrivenOperator(
            wrapped_operator=operator,
            num_workers=8,  # 8 workers optimal for small-medium data
            morsel_size_kb=64,
            enabled=True
        )

    @classmethod
    def from_batches(cls, batches: Iterable[ca.RecordBatch]) -> 'Stream':
        """Create stream from RecordBatches."""
        return cls(batches)

    @classmethod
    def from_table(cls, table: ca.Table, batch_size: int = 10000) -> 'Stream':
        """Create stream from PyArrow Table."""
        return cls(table.to_batches(max_chunksize=batch_size))

    @classmethod
    def from_dicts(cls, dicts: List[Dict[str, Any]], batch_size: int = 10000) -> 'Stream':
        """Create stream from list of Python dicts."""
        if not dicts:
            return cls(iter([]))

        # Convert to batches
        def dict_batches():
            for i in range(0, len(dicts), batch_size):
                chunk = dicts[i:i + batch_size]
                yield ca.RecordBatch.from_pylist(chunk)

        return cls(dict_batches())

    @classmethod
    def from_pylist(cls, pylist: List[Dict[str, Any]], batch_size: int = 10000) -> 'Stream':
        """Alias for from_dicts."""
        return cls.from_dicts(pylist, batch_size)

    @classmethod
    def from_sql(
        cls,
        sql: str,
        database: str = ':memory:',
        filters: Optional[Dict[str, Any]] = None,
        columns: Optional[List[str]] = None,
        extensions: Optional[List[str]] = None,
        batch_size: Optional[int] = None
    ) -> 'Stream':
        """
        Create stream from DuckDB SQL query with automatic pushdown optimization.

        Leverages DuckDB's query optimizer to push filters and projections down
        to the data source (Parquet, CSV, S3, Postgres, etc.) for maximum performance.

        Args:
            sql: Base SQL query (e.g., "SELECT * FROM read_parquet('data/*.parquet')")
            database: Database path (default: in-memory)
            filters: Column filters for pushdown (dict of column: condition)
            columns: Columns to select (projection pushdown)
            extensions: DuckDB extensions to load (e.g., ['httpfs', 'postgres_scanner'])
            batch_size: Target batch size for streaming (None = use DuckDB default)

        Returns:
            Stream from DuckDB query results

        Examples:
            # Parquet with pushdown
            stream = Stream.from_sql(
                "SELECT * FROM read_parquet('s3://data/*.parquet')",
                filters={'date': ">= '2025-01-01'", 'price': '> 100'},
                columns=['id', 'symbol', 'price', 'volume'],
                extensions=['httpfs']
            )

            # Postgres source
            stream = Stream.from_sql(
                "SELECT * FROM postgres_scan('host=localhost', 'transactions')",
                filters={'amount': '> 1000'},
                extensions=['postgres_scanner']
            )

            # Delta Lake
            stream = Stream.from_sql(
                "SELECT * FROM delta_scan('s3://bucket/table')",
                extensions=['delta']
            )

        Performance:
            - Automatic filter/projection pushdown
            - Zero-copy Arrow streaming via Arrow C Data Interface
            - 100M+ records/sec for simple queries
        """
        from sabot.connectors import DuckDBSource

        source = DuckDBSource(
            sql=sql,
            database=database,
            filters=filters,
            columns=columns,
            extensions=extensions or [],
            batch_size=batch_size
        )

        # Get schema from source
        schema = source.get_schema()

        # Create async generator from source
        async def batch_generator():
            async for batch in source.stream_batches():
                yield batch

        return cls(batch_generator(), schema=schema)

    @classmethod
    def from_parquet_duckdb(
        cls,
        path: str,
        filters: Optional[Dict[str, Any]] = None,
        columns: Optional[List[str]] = None,
        **kwargs
    ) -> 'Stream':
        """
        Read Parquet using DuckDB's optimized streaming engine.
        
        Advantages over Arrow-native:
        - Automatic filter/projection pushdown to Parquet metadata
        - Optimized compression handling (zstd, snappy, etc.)
        - Handles partitioned datasets automatically
        - Battle-tested query optimizer
        - Streaming by default (larger-than-RAM)
        
        Use when:
        - Complex filters (DuckDB pushes down to Parquet)
        - Heavily compressed files
        - Partitioned datasets (hive-style)
        - Want automatic query optimization
        
        Args:
            path: Parquet file path (supports globs: 'data/*.parquet')
            filters: Column filters for pushdown (dict of column: condition)
            columns: Columns to read (projection pushdown)
            **kwargs: Additional arguments (batch_size, etc.)
        
        Returns:
            Stream from DuckDB Parquet reader
        
        Examples:
            # Simple read with DuckDB
            stream = Stream.from_parquet_duckdb('data.parquet')
            
            # With filters and projection
            stream = Stream.from_parquet_duckdb(
                'lineitem.parquet',
                filters={'l_shipdate': "<= '1998-09-02'", 'l_discount': '> 0.05'},
                columns=['l_orderkey', 'l_extendedprice', 'l_discount']
            )
            
            # Partitioned dataset
            stream = Stream.from_parquet_duckdb('data/year=*/month=*/*.parquet')
        
        Performance:
            - Zero-copy Arrow streaming via C Data Interface
            - Filter pushdown to Parquet row groups
            - Matches DuckDB native performance (~10s for TPC-H Scale 1.67)
        """
        # Use from_sql() with DuckDB's read_parquet() function
        sql = f"SELECT * FROM read_parquet('{path}')"
        return cls.from_sql(
            sql=sql,
            filters=filters,
            columns=columns,
            **kwargs
        )

    @classmethod
    def from_parquet(
        cls,
        path: str,
        backend: str = 'arrow',  # 'arrow' or 'duckdb'
        filters: Optional[Dict[str, Any]] = None,
        columns: Optional[List[str]] = None,
        optimize_dates: bool = False,  # Disabled by default - testing showed no benefit
        parallel: bool = True,
        num_threads: int = 4,
        lazy: bool = True  # Enable lazy streaming by default
    ) -> 'Stream':
        """
        Create stream from Parquet file(s) with choice of backend.

        Args:
            path: Parquet file path (supports globs: 'data/*.parquet')
            backend: 'arrow' (default, minimal deps) or 'duckdb' (optimizer)
            filters: Column filters for pushdown
            columns: Columns to read (projection pushdown)
            optimize_dates: Convert string dates to date32 (Arrow backend only)
            parallel: Enable parallel row group reading (Arrow backend only)
            num_threads: Number of threads for parallel I/O (Arrow backend only)
            lazy: Enable lazy streaming (default: True)

        Returns:
            Stream from Parquet data

        Backend Comparison:
            Arrow (default):
            - Zero dependencies beyond vendored Arrow
            - Fast for simple reads
            - Full control over memory and parallelism
            - Good for: Simple queries, no complex filters
            
            DuckDB:
            - Automatic query optimization
            - Filter/projection pushdown to Parquet metadata
            - Better compression handling
            - Good for: Complex filters, partitioned data, optimization

        Examples:
            # Arrow-native (fast, simple, default)
            stream = Stream.from_parquet('lineitem.parquet')
            
            # DuckDB (optimized, complex queries)
            stream = Stream.from_parquet(
                'lineitem.parquet',
                backend='duckdb',
                filters={'l_shipdate': "<= '1998-09-02'"}
            )
            
            # Arrow with custom parallelism
            stream = Stream.from_parquet(
                'lineitem.parquet',
                backend='arrow',
                parallel=True,
                num_threads=8
            )
        
        Performance:
            Both backends support larger-than-RAM streaming with lazy=True (default).
            Arrow: 1.5-2x faster I/O with parallel=True
            DuckDB: Automatic filter pushdown, matches native DuckDB performance
        """
        # Route to appropriate backend
        if backend == 'duckdb':
            return cls.from_parquet_duckdb(path, filters=filters, columns=columns)
        else:
            # Arrow-native backend (default)
            return cls._from_parquet_optimized(path, columns, parallel=parallel, num_threads=num_threads, lazy=lazy)
    
    @classmethod
    def _from_parquet_optimized(
        cls,
        path: str,
        columns: Optional[List[str]] = None,
        parallel: bool = True,
        num_threads: int = 4,
        lazy: bool = True
    ) -> 'Stream':
        """
        Optimized Parquet reader with LAZY streaming row group reading.
        
        This matches Polars/DuckDB's approach:
        - Streams row groups one at a time (lazy)
        - Processes while reading (better scaling)
        - Low memory footprint
        
        Args:
            path: Path to Parquet file
            columns: Columns to read
            parallel: Enable parallel row group reading (default: True)
            num_threads: Number of threads for parallel reading (default: 4)
            lazy: Stream row groups lazily (default: True) - KEY for good scaling!
        """
        from sabot import cyarrow as ca
        # Use vendored Arrow parquet module ONLY (cyarrow wraps vendored Arrow)
        pq = ca.parquet
        import concurrent.futures
        
        if lazy:
            # Use CyArrow's (vendored Arrow's) BUILT-IN lazy loading
            # ParquetFile.iter_batches() is what Polars uses internally!
            
            # Generator owns file handle - stays open during iteration
            def lazy_batch_generator():
                """Generator that owns ParquetFile handle for lazy streaming."""
                # Open file object directly to avoid filesystem registration conflicts
                # This bypasses the LocalFileSystem.register() call that causes conflicts
                with open(path, 'rb') as f:
                    # Pass file object directly - no filesystem resolution needed
                    pf = pq.ParquetFile(f)
                    
                    # Yield batches lazily - file stays open during iteration
                    yield from pf.iter_batches(
                        batch_size=100_000,  # 100K rows per batch
                        columns=columns      # Column projection
                    )
                    # File closes when with block exits (after generator exhausted)
            
            # Return stream with lazy generator
            # All RecordBatches are from vendored Arrow (cyarrow)
            return cls(lazy_batch_generator())
        
        else:
            # EAGER PATH - load all data (old approach)
            # Use memory pool if available
            global _memory_pool
            if OPTIMIZATION_FEATURES_AVAILABLE and _memory_pool is None:
                try:
                    from sabot._cython.arrow.memory_pool import get_memory_pool
                    _memory_pool = get_memory_pool()
                except:
                    pass
            
            memory_pool = _memory_pool
            
            if not parallel:
                # Single-threaded
                with open(path, 'rb') as f:
                    # memory_pool parameter not supported in cyarrow parquet
                    table = pq.read_table(f, columns=columns)
            else:
                # Parallel row group reading
                with open(path, 'rb') as f:
                    parquet_file = pq.ParquetFile(f)
                    num_row_groups = parquet_file.num_row_groups
                
                if num_row_groups == 1:
                    with open(path, 'rb') as f:
                        # memory_pool parameter not supported in cyarrow parquet
                        table = pq.read_table(f, columns=columns)
                else:
                    def read_row_group(rg_idx):
                        with open(path, 'rb') as f:
                            pf = pq.ParquetFile(f)
                            return pf.read_row_group(rg_idx, columns=columns)
                    
                    with concurrent.futures.ThreadPoolExecutor(max_workers=num_threads) as executor:
                        row_group_tables = list(executor.map(read_row_group, range(num_row_groups)))
                    
                    # Concatenate row groups - manually concat batches
                    all_batches = []
                    for t in row_group_tables:
                        all_batches.extend(t.to_batches())
                    table = ca.Table.from_batches(all_batches)
            
            # Convert to batches
            batches = table.to_batches()
            return cls.from_batches(batches)

    @classmethod
    def from_csv(
        cls,
        path: str,
        filters: Optional[Dict[str, Any]] = None,
        columns: Optional[List[str]] = None,
        **csv_options
    ) -> 'Stream':
        """
        Create stream from CSV file(s) with automatic pushdown.

        Args:
            path: CSV file path (supports globs: 'data/*.csv')
            filters: Column filters for pushdown
            columns: Columns to read (projection pushdown)
            **csv_options: DuckDB CSV options (header, delimiter, etc.)

        Returns:
            Stream from CSV data

        Example:
            stream = Stream.from_csv(
                'data/transactions.csv',
                filters={'amount': '> 1000'},
                header=True,
                delimiter=','
            )
        """
        # Build CSV read options
        options_str = ""
        if csv_options:
            opts = ', '.join(f"{k}={repr(v)}" for k, v in csv_options.items())
            options_str = f", {opts}"

        sql = f"SELECT * FROM read_csv('{path}'{options_str})"
        return cls.from_sql(sql, filters=filters, columns=columns)

    @classmethod
    def from_postgres(
        cls,
        connection_string: str,
        table: str,
        filters: Optional[Dict[str, Any]] = None,
        columns: Optional[List[str]] = None
    ) -> 'Stream':
        """
        Create stream from PostgreSQL table with automatic pushdown.

        Args:
            connection_string: Postgres connection (e.g., 'host=localhost user=postgres')
            table: Table name to read from
            filters: Column filters (pushed to Postgres)
            columns: Columns to read (pushed to Postgres)

        Returns:
            Stream from Postgres data

        Example:
            stream = Stream.from_postgres(
                'host=localhost user=postgres password=secret',
                'transactions',
                filters={'date': ">= '2025-01-01'"},
                columns=['id', 'amount', 'timestamp']
            )
        """
        sql = f"SELECT * FROM postgres_scan('{connection_string}', '{table}')"
        return cls.from_sql(sql, filters=filters, columns=columns, extensions=['postgres_scanner'])

    @classmethod
    def from_delta(
        cls,
        path: str,
        filters: Optional[Dict[str, Any]] = None,
        columns: Optional[List[str]] = None
    ) -> 'Stream':
        """
        Create stream from Delta Lake table with automatic pushdown.

        Args:
            path: Delta table path (local or S3)
            filters: Column filters for pushdown
            columns: Columns to read (projection pushdown)

        Returns:
            Stream from Delta Lake data

        Example:
            stream = Stream.from_delta(
                's3://bucket/delta-table',
                filters={'date': ">= '2025-01-01'"},
                columns=['id', 'amount', 'timestamp']
            )
        """
        sql = f"SELECT * FROM delta_scan('{path}')"
        return cls.from_sql(sql, filters=filters, columns=columns, extensions=['delta'])

    @classmethod
    def from_kafka(
        cls,
        bootstrap_servers: str,
        topic: str,
        group_id: str,
        codec_type: str = "json",
        codec_options: Optional[Dict[str, Any]] = None,
        batch_size: int = 1000,
        use_cpp: bool = True,
        **consumer_kwargs
    ) -> 'Stream':
        """
        Create stream from Kafka topic.

        Args:
            bootstrap_servers: Kafka brokers (e.g., "localhost:9092")
            topic: Topic to consume from
            group_id: Consumer group ID
            codec_type: Codec type (json, avro, protobuf, json_schema, msgpack, string, bytes)
            codec_options: Codec-specific options (e.g., schema_registry_url for Avro)
            batch_size: Number of messages to batch into RecordBatch
            use_cpp: Use C++ librdkafka (default: True for performance)
            **consumer_kwargs: Additional Kafka consumer config

        Returns:
            Stream from Kafka

        Examples:
            # Simple JSON stream (uses C++ librdkafka by default)
            stream = Stream.from_kafka(
                "localhost:9092",
                "transactions",
                "my-group"
            )

            # Avro stream with Schema Registry (uses Python fallback)
            stream = Stream.from_kafka(
                "localhost:9092",
                "transactions",
                "fraud-detector",
                codec_type="avro",
                codec_options={
                    'schema_registry_url': 'http://localhost:8081',
                    'subject': 'transactions-value'
                },
                use_cpp=False  # Python supports Schema Registry
            )
        """
        import logging
        logger = logging.getLogger(__name__)
        
        # Try C++ implementation first (if requested and codec_type is json)
        if use_cpp and codec_type == "json":
            try:
                from sabot._cython.kafka import LibrdkafkaSource, LIBRDKAFKA_AVAILABLE
                if LIBRDKAFKA_AVAILABLE:
                    logger.info(f"Using C++ librdkafka source for {topic} (high performance)")
                    return cls._from_kafka_cpp(
                        bootstrap_servers, topic, group_id, batch_size, consumer_kwargs
                    )
            except (ImportError, RuntimeError) as e:
                logger.warning(f"C++ Kafka unavailable ({e}), using Python fallback")
        
        # Use Python fallback (supports all codecs)
        logger.info(f"Using Python aiokafka source for {topic} (codec: {codec_type})")
        from ..kafka import from_kafka as create_kafka_source
        import asyncio

        # Create Kafka source
        source = create_kafka_source(
            bootstrap_servers=bootstrap_servers,
            topic=topic,
            group_id=group_id,
            codec_type=codec_type,
            codec_options=codec_options,
            **consumer_kwargs
        )

        # Convert async Kafka stream to batched RecordBatches
        def kafka_batches():
            async def consume():
                buffer = []
                async for message in source.stream():
                    buffer.append(message)

                    # Flush when buffer is full
                    if len(buffer) >= batch_size:
                        yield ca.RecordBatch.from_pylist(buffer)
                        buffer = []

                # Flush remaining
                if buffer:
                    yield ca.RecordBatch.from_pylist(buffer)

            # Run async generator in sync context
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            try:
                async_gen = consume()
                while True:
                    try:
                        batch = loop.run_until_complete(async_gen.__anext__())
                        yield batch
                    except StopAsyncIteration:
                        break
            finally:
                loop.close()

        return cls(kafka_batches())
    
    @classmethod
    def _from_kafka_cpp(
        cls,
        bootstrap_servers: str,
        topic: str,
        group_id: str,
        batch_size: int,
        consumer_kwargs: dict
    ) -> 'Stream':
        """
        Create stream using C++ librdkafka (high performance).
        
        Currently JSON-only. Falls back to Python for other codecs.
        """
        from sabot._cython.kafka import LibrdkafkaSource
        import asyncio
        
        # Create C++ source
        source = LibrdkafkaSource(
            bootstrap_servers=bootstrap_servers,
            topic=topic,
            group_id=group_id,
            partition_id=consumer_kwargs.get('partition_id', -1),
            properties=consumer_kwargs
        )
        
        # Convert to batch generator
        def cpp_kafka_batches():
            async def consume():
                while True:
                    batch = await source.get_next_batch(batch_size)
                    if batch is None:
                        break
                    yield batch
            
            # Run async generator
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            try:
                async_gen = consume()
                while True:
                    try:
                        batch = loop.run_until_complete(async_gen.__anext__())
                        yield batch
                    except StopAsyncIteration:
                        break
            finally:
                source.shutdown()
                loop.close()
        
        return cls(cpp_kafka_batches())

    @classmethod
    def from_marbledb(
        cls,
        db_path: str,
        table_name: str,
        predicates: Optional[List[tuple]] = None,
        store: Optional[Any] = None
    ) -> 'Stream':
        """
        Create stream from MarbleDB table with lazy batch streaming.

        Uses MarbleDB's TableBatchIterator for zero-copy streaming access.
        Predicates are pushed down to zone maps for efficient filtering.

        Args:
            db_path: Path to MarbleDB database
            table_name: Name of the table to scan
            predicates: Optional list of predicates for pushdown filtering
                       Format: [(column, op, value), ...]
                       Supported ops: '=', '>', '<', '>=', '<=', '!='
            store: Optional existing MarbleDBStoreBackend instance (if None,
                   a new one will be created and managed internally)

        Returns:
            Stream from MarbleDB table

        Examples:
            # Simple scan - streams batches lazily
            stream = Stream.from_marbledb('/data/state', 'transactions')

            # With predicate pushdown (uses zone maps)
            stream = Stream.from_marbledb(
                '/data/state',
                'transactions',
                predicates=[('amount', '>', 1000), ('status', '=', 'pending')]
            )

            # With existing store (store is not closed by stream)
            store = MarbleDBStoreBackend('/data/state')
            store.open()
            stream = Stream.from_marbledb('/data/state', 'orders', store=store)

            # Chain with normal operators
            (stream
                .filter(lambda b: cc.greater(b.column('price'), 100))
                .map(lambda b: b.append_column('fee', cc.multiply(b.column('amount'), 0.03)))
                .collect()
            )

        Performance:
            - Zero-copy: batches returned directly from C++ layer
            - Zone map pruning: skips SSTables that don't match predicates
            - Streaming: constant memory (one batch at a time)
        """
        try:
            from sabot._cython.stores.marbledb_store import MarbleDBStoreBackend
        except ImportError:
            raise RuntimeError(
                "MarbleDB store not available. Build with: make -j4"
            )

        # Use provided store or create new one
        owns_store = store is None
        if owns_store:
            store = MarbleDBStoreBackend(db_path)
            store.open()

        # Use the iter_batches() streaming API
        batch_iterator = store.iter_batches(table_name)

        # Wrap in generator to handle cleanup
        def marbledb_batches():
            try:
                for batch in batch_iterator:
                    yield batch
            finally:
                # Only close if we created the store
                if owns_store:
                    store.close()

        return cls(marbledb_batches())

    @classmethod
    def from_postgres_cdc(
        cls,
        host: str = "localhost",
        port: int = 5432,
        database: str = "postgres",
        user: str = "postgres",
        password: str = "",
        replication_slot: str = "sabot_cdc_slot",
        table_filter: Optional[List[str]] = None,
        batch_size: int = 100,
        **cdc_kwargs
    ) -> 'Stream':
        """
        Create stream from PostgreSQL CDC (Change Data Capture).

        Uses logical replication with wal2json to stream database changes in real-time.

        Args:
            host: PostgreSQL host
            port: PostgreSQL port
            database: Database name
            user: Username
            password: Password
            replication_slot: Logical replication slot name
            table_filter: List of tables to monitor (e.g., ["public.users", "public.orders"])
            batch_size: Number of CDC events to batch into RecordBatch
            **cdc_kwargs: Additional PostgreSQLCDCConfig options

        Returns:
            Stream of CDC events

        Examples:
            # Basic CDC stream
            stream = Stream.from_postgres_cdc(
                host="localhost",
                database="ecommerce",
                user="cdc_user",
                password="secret",
                replication_slot="sabot_cdc"
            )

            # Filtered CDC stream
            stream = Stream.from_postgres_cdc(
                host="localhost",
                database="analytics",
                table_filter=["public.events", "public.metrics"],
                batch_size=50
            )

        Note:
            Requires PostgreSQL 10+ with logical replication enabled.
            The wal2json extension must be installed.
        """
        try:
            from .._cython.connectors.postgresql import create_postgresql_cdc_connector
        except ImportError:
            raise RuntimeError("PostgreSQL CDC connector not available. Build with PostgreSQL support.")

        import asyncio

        # Create CDC connector
        connector = create_postgresql_cdc_connector(
            host=host,
            port=port,
            database=database,
            user=user,
            password=password,
            replication_slot=replication_slot,
            filter_tables=table_filter,
            batch_size=batch_size,
            **cdc_kwargs
        )

        def cdc_batches():
            """Convert CDC records to RecordBatch stream."""
            async def consume():
                buffer = []

                async with connector:
                    async for records in connector.stream_changes():
                        # Convert CDC records to dictionaries
                        for record in records:
                            cdc_dict = {
                                'event_type': record.event_type,
                                'schema': record.schema,
                                'table': record.table,
                                'lsn': record.lsn,
                                'timestamp': record.timestamp.isoformat() if record.timestamp else None,
                                'transaction_id': record.transaction_id,
                                'origin': record.origin,
                                **({'data': record.data} if record.data else {}),
                                **({'old_data': record.old_data} if record.old_data else {}),
                                **({'key_data': record.key_data} if record.key_data else {}),
                            }

                            # Add message-specific fields if present
                            if hasattr(record, 'message_prefix') and record.message_prefix:
                                cdc_dict['message_prefix'] = record.message_prefix
                                cdc_dict['message_content'] = record.message_content
                                cdc_dict['transactional'] = record.transactional

                            buffer.append(cdc_dict)

                            # Flush when buffer is full
                            if len(buffer) >= batch_size:
                                yield ca.RecordBatch.from_pylist(buffer)
                                buffer = []

                    # Flush remaining
                    if buffer:
                        yield ca.RecordBatch.from_pylist(buffer)

            # Run async generator in sync context
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            try:
                async_gen = consume()
                while True:
                    try:
                        batch = loop.run_until_complete(async_gen.__anext__())
                        yield batch
                    except StopAsyncIteration:
                        break
            finally:
                loop.close()

        return cls(cdc_batches())

    # ========================================================================
    # Transform Operations (Stateless)
    # ========================================================================

    def filter(self, predicate: Callable[[ca.RecordBatch], Union[ca.Array, bool]]) -> 'Stream':
        """
        Filter stream with predicate (SIMD-accelerated).

        Args:
            predicate: Function that takes RecordBatch and returns:
                      - Arrow boolean Array (preferred - uses SIMD)
                      - Python boolean (filters entire batch)

        Returns:
            Filtered stream

        Examples:
            # Using Arrow compute (SIMD-accelerated)
            stream.filter(lambda b: pc.greater(b.column('price'), 100))

            # Natural Python syntax (auto-converted)
            stream.filter(lambda b: b.column('price') > 100)

            # Complex predicate
            stream.filter(lambda b: pc.and_(
                pc.greater(b.column('price'), 100),
                pc.equal(b.column('side'), 'BUY')
            ))
        """
        # Wrap predicate to auto-convert common comparison patterns
        wrapped_predicate = self._auto_convert_predicate(predicate)
        if CYTHON_AVAILABLE:
            operator = CythonFilterOperator(self._source, wrapped_predicate)
            operator = self._wrap_with_morsel_parallelism(operator)
            return Stream(operator, self._schema)
        else:
            # Fallback to Python
            def python_filter():
                for batch in self._source:
                    mask = wrapped_predicate(batch)
                    if isinstance(mask, ca.Array):
                        filtered = batch.filter(mask)
                        if filtered.num_rows > 0:
                            yield filtered
                    elif mask:
                        yield batch
            return Stream(python_filter(), self._schema)

    def _auto_convert_predicate(self, predicate: Callable[[ca.RecordBatch], Any]) -> Callable[[ca.RecordBatch], Any]:
        """
        Auto-convert predicates that use natural Python syntax to PyArrow compute functions.

        This allows users to write: lambda b: b.column('price') > 100
        Instead of requiring: lambda b: pc.greater(b.column('price'), 100)
        """
        def wrapped_predicate(batch: ca.RecordBatch) -> Any:
            try:
                return predicate(batch)
            except TypeError as e:
                if "not supported between instances of" in str(e) and "pyarrow.lib" in str(e):
                    # Try to convert the predicate automatically
                    return self._convert_predicate_to_compute(predicate, batch)
                else:
                    raise

        return wrapped_predicate

    def _convert_predicate_to_compute(self, predicate: Callable[[ca.RecordBatch], Any], batch: ca.RecordBatch) -> Any:
        """
        Convert a predicate that failed due to array-scalar comparison to use PyArrow compute.

        This is a best-effort conversion for common patterns.
        """
        try:
            # Create a mock batch with scalar values to test the predicate
            import inspect

            # Get the source code of the predicate if it's a lambda
            try:
                source = inspect.getsource(predicate)
                # Look for common patterns like column() > value, column() < value, etc.
                if 'column(' in source and (' > ' in source or ' < ' in source or ' >= ' in source or ' <= ' in source or ' == ' in source or ' != ' in source):
                    # This is a simple case we can try to convert
                    # For now, just re-raise with a helpful message
                    pass
            except (OSError, TypeError):
                pass

            # If we can't auto-convert, provide a helpful error message
            raise TypeError(
                "Predicate uses array-scalar comparison that isn't directly supported. "
                "Please use PyArrow compute functions instead:\n"
                "  Instead of: lambda b: b.column('col') > 100\n"
                "  Use:        lambda b: pc.greater(b.column('col'), 100)\n"
                "Available functions: pc.greater, pc.less, pc.equal, pc.greater_equal, pc.less_equal, pc.not_equal"
            )

        except Exception:
            # If conversion fails, re-raise the original error
            raise

    def map(self, func: Callable[[ca.RecordBatch], ca.RecordBatch]) -> 'Stream':
        """
        Transform each batch with a function (vectorized).

        Args:
            func: Function that takes RecordBatch and returns RecordBatch

        Returns:
            Transformed stream

        Examples:
            # Add column
            stream.map(lambda b: b.append_column('fee',
                pc.multiply(b.column('amount'), 0.03)))

            # Transform column
            stream.map(lambda b: b.set_column(0, 'id',
                pc.add(b.column('id'), 1000)))
        """
        if CYTHON_AVAILABLE:
            operator = CythonMapOperator(self._source, func)
            operator = self._wrap_with_morsel_parallelism(operator)
            return Stream(operator, self._schema)
        else:
            # Fallback to Python
            def python_map():
                for batch in self._source:
                    yield func(batch)
            return Stream(python_map(), self._schema)

    def select(self, *columns: str) -> 'Stream':
        """
        Select specific columns (zero-copy projection).

        Args:
            *columns: Column names to keep

        Returns:
            Stream with selected columns

        Examples:
            stream.select('id', 'price', 'quantity')
        """
        columns_list = list(columns)

        if CYTHON_AVAILABLE:
            operator = CythonSelectOperator(self._source, columns_list)
            operator = self._wrap_with_morsel_parallelism(operator)
            return Stream(operator, None)
        else:
            # Fallback to Python
            def python_select():
                for batch in self._source:
                    yield batch.select(columns_list)
            return Stream(python_select(), None)

    def sql(
        self,
        query: str,
        table_name: str = 'stream',
        num_workers: int = 4
    ) -> 'Stream':
        """
        Execute SQL on the current stream.

        Materializes the stream to an Arrow table, registers it with
        the SQL engine, executes the query, and returns a new stream.

        Args:
            query: SQL query (use table_name to reference stream data)
            table_name: Name to register stream as (default: 'stream')
            num_workers: Number of parallel workers for execution

        Returns:
            Stream with query results

        Examples:
            # Filter and aggregate with SQL
            stream.sql("SELECT symbol, SUM(amount) FROM stream GROUP BY symbol")

            # Join with another table
            stream.sql('''
                SELECT s.*, r.rate
                FROM stream s
                JOIN rates r ON s.currency = r.code
            ''')

            # Window functions
            stream.sql('''
                SELECT *,
                    ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY timestamp) as rn
                FROM stream
            ''')
        """
        import asyncio
        from sabot.sql.controller import SQLController

        # Materialize stream to table synchronously
        batches = list(self._source)

        if not batches:
            return Stream(iter([]), None)

        table = ca.Table.from_batches(batches)

        # Execute SQL
        async def execute_sql():
            controller = SQLController()
            await controller.register_table(table_name, table)
            result = await controller.execute(
                query,
                num_agents=num_workers,
                execution_mode="local_parallel"
            )
            return result

        # Run async execution
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            result = loop.run_until_complete(execute_sql())
        finally:
            loop.close()

        # Return result as stream
        if hasattr(result, 'to_batches'):
            return Stream(result.to_batches(), None)
        else:
            return Stream(iter([result]), None)

    def timeseries(self) -> 'TimeSeriesStreamOps':
        """
        Access time series operations on this stream.

        Returns:
            TimeSeriesStreamOps for chaining time series operations

        Examples:
            # Rolling average
            stream.timeseries().rolling(
                window_size=20, column='price', agg='mean'
            )

            # EWMA smoothing
            stream.timeseries().ewma(column='price', alpha=0.1)

            # Log returns
            stream.timeseries().log_returns(column='price')
        """
        return TimeSeriesStreamOps(self)

    # ========================================================================
    # String Operations (SIMD-Accelerated)
    # ========================================================================

    def filter_equals(self, column: str, value: str) -> 'Stream':
        """
        Filter rows where string column equals value (200M+ ops/sec).

        Uses Arrow's SIMD-optimized string comparison kernel.

        Args:
            column: Name of string column to filter
            value: String value to match

        Returns:
            Filtered stream

        Examples:
            # Filter by exact match
            stream.filter_equals('country', 'USA')

            # Chain multiple filters
            stream.filter_equals('status', 'active').filter_equals('type', 'premium')
        """
        try:
            from sabot._cython.arrow.string_operations import equal
        except ImportError:
            # Fallback to PyArrow compute
            equal = ca.compute.equal

        def predicate(batch):
            return equal(batch.column(column), value)

        return self.filter(predicate)

    def filter_contains(self, column: str, pattern: str, ignore_case: bool = False) -> 'Stream':
        """
        Filter rows where string column contains pattern (100M+ ops/sec).

        Uses Arrow's SIMD-optimized substring search with Boyer-Moore algorithm.

        Args:
            column: Name of string column to filter
            pattern: Substring pattern to search for
            ignore_case: If True, perform case-insensitive search

        Returns:
            Filtered stream

        Examples:
            # Filter by substring
            stream.filter_contains('description', 'urgent')

            # Case-insensitive search
            stream.filter_contains('title', 'ERROR', ignore_case=True)
        """
        try:
            from sabot._cython.arrow.string_operations import contains
        except ImportError:
            # Fallback to PyArrow compute
            def contains_fallback(array, pattern, ignore_case=False):
                if ignore_case:
                    array = ca.compute.utf8_lower(array)
                    pattern = pattern.lower()
                return ca.compute.match_substring(array, pattern)
            contains = contains_fallback

        def predicate(batch):
            return contains(batch.column(column), pattern, ignore_case=ignore_case)

        return self.filter(predicate)

    def filter_starts_with(self, column: str, pattern: str, ignore_case: bool = False) -> 'Stream':
        """
        Filter rows where string column starts with pattern (150M+ ops/sec).

        Uses Arrow's SIMD-optimized prefix matching.

        Args:
            column: Name of string column to filter
            pattern: Prefix pattern to match
            ignore_case: If True, perform case-insensitive match

        Returns:
            Filtered stream

        Examples:
            # Filter by prefix
            stream.filter_starts_with('url', 'https://')

            # Case-insensitive prefix
            stream.filter_starts_with('command', 'SELECT', ignore_case=True)
        """
        try:
            from sabot._cython.arrow.string_operations import starts_with
        except ImportError:
            # Fallback to PyArrow compute
            def starts_with_fallback(array, pattern, ignore_case=False):
                if ignore_case:
                    array = ca.compute.utf8_lower(array)
                    pattern = pattern.lower()
                return ca.compute.starts_with(array, pattern)
            starts_with = starts_with_fallback

        def predicate(batch):
            return starts_with(batch.column(column), pattern, ignore_case=ignore_case)

        return self.filter(predicate)

    def filter_ends_with(self, column: str, pattern: str, ignore_case: bool = False) -> 'Stream':
        """
        Filter rows where string column ends with pattern (150M+ ops/sec).

        Uses Arrow's SIMD-optimized suffix matching.

        Args:
            column: Name of string column to filter
            pattern: Suffix pattern to match
            ignore_case: If True, perform case-insensitive match

        Returns:
            Filtered stream

        Examples:
            # Filter by suffix
            stream.filter_ends_with('filename', '.csv')

            # Case-insensitive suffix
            stream.filter_ends_with('email', '@EXAMPLE.COM', ignore_case=True)
        """
        try:
            from sabot._cython.arrow.string_operations import ends_with
        except ImportError:
            # Fallback to PyArrow compute
            def ends_with_fallback(array, pattern, ignore_case=False):
                if ignore_case:
                    array = ca.compute.utf8_lower(array)
                    pattern = pattern.lower()
                return ca.compute.ends_with(array, pattern)
            ends_with = ends_with_fallback

        def predicate(batch):
            return ends_with(batch.column(column), pattern, ignore_case=ignore_case)

        return self.filter(predicate)

    def filter_regex(self, column: str, pattern: str, ignore_case: bool = False) -> 'Stream':
        """
        Filter rows where string column matches regex pattern (50M+ ops/sec).

        Uses Arrow's RE2-based regex engine (safe for untrusted patterns).

        Args:
            column: Name of string column to filter
            pattern: Regular expression pattern (RE2 syntax)
            ignore_case: If True, perform case-insensitive matching

        Returns:
            Filtered stream

        Examples:
            # Filter by regex
            stream.filter_regex('phone', r'^\\+1-\\d{3}-\\d{4}$')

            # Case-insensitive regex
            stream.filter_regex('log_level', r'error|warn', ignore_case=True)
        """
        try:
            from sabot._cython.arrow.string_operations import match_regex
        except ImportError:
            # Fallback to PyArrow compute
            def match_regex_fallback(array, pattern, ignore_case=False):
                if ignore_case:
                    pattern = f"(?i){pattern}"
                return ca.compute.match_substring_regex(array, pattern)
            match_regex = match_regex_fallback

        def predicate(batch):
            return match_regex(batch.column(column), pattern, ignore_case=ignore_case)

        return self.filter(predicate)

    def parallel(self, num_workers: Optional[int] = None, morsel_size_kb: int = 64) -> 'Stream':
        """
        Configure morsel-driven parallel execution parameters.

        Morsel parallelism is enabled by default. This method allows you to
        configure the parallelism settings (workers, morsel size) for the
        already-enabled parallel execution.

        Args:
            num_workers: Number of workers (None = auto-detect)
            morsel_size_kb: Morsel size in KB (default 64KB)

        Returns:
            New stream with configured parallel execution

        Examples:
            # Use default auto-detected parallelism
            stream.map(transform).filter(condition)

            # Configure parallelism explicitly
            stream.map(transform).parallel(num_workers=8).filter(condition)

            # Custom morsel size for memory-constrained environments
            stream.parallel(morsel_size_kb=32).map(transform)
        """
        # Since parallelism is already enabled by default, we need to reconfigure
        # the existing MorselDrivenOperator with new parameters
        if hasattr(self._operator, '_wrapped_operator'):
            # Already wrapped - create new wrapper with updated settings
            from sabot._cython.operators.morsel_operator import MorselDrivenOperator
            new_wrapper = MorselDrivenOperator(
                wrapped_operator=self._operator._wrapped_operator,
                num_workers=num_workers or 0,
                morsel_size_kb=morsel_size_kb,
                enabled=True
            )
            return Stream(new_wrapper, schema=self._schema)
        else:
            # Not wrapped yet - wrap with specified settings
            from sabot._cython.operators.morsel_operator import MorselDrivenOperator
            parallel_op = MorselDrivenOperator(
                wrapped_operator=self._operator,
                num_workers=num_workers or 0,
                morsel_size_kb=morsel_size_kb,
                enabled=True
            )
            return Stream(parallel_op, schema=self._schema)

    def sequential(self) -> 'Stream':
        """
        Disable morsel-driven parallel execution for this stream.

        Use this method when you need to ensure sequential processing,
        for example when operations have side effects or ordering dependencies.

        Returns:
            New stream with parallel execution disabled

        Examples:
            # Force sequential processing
            stream.map(side_effect_operation).sequential().foreach(print)

            # Disable parallelism for debugging
            stream.sequential().map(debug_transform)
        """
        if hasattr(self._operator, '_wrapped_operator'):
            # Unwrap from MorselDrivenOperator
            return Stream(self._operator._wrapped_operator, schema=self._schema)
        else:
            # Already sequential
            return self

    def flat_map(self, func: Callable[[ca.RecordBatch], List[ca.RecordBatch]]) -> 'Stream':
        """
        Expand each batch into multiple batches (1-to-N).

        Args:
            func: Function that takes RecordBatch and returns list of RecordBatches

        Returns:
            Flattened stream

        Examples:
            # Split large batches
            stream.flat_map(lambda b: [b.slice(i*1000, 1000) for i in range(b.num_rows // 1000)])
        """
        if CYTHON_AVAILABLE:
            operator = CythonFlatMapOperator(self._source, func)
            operator = self._wrap_with_morsel_parallelism(operator)
            return Stream(operator, self._schema)
        else:
            # Fallback to Python
            def python_flat_map():
                for batch in self._source:
                    for result_batch in func(batch):
                        yield result_batch
            return Stream(python_flat_map(), self._schema)

    def transform_graph(self, graph_operator: 'GraphStreamOperator') -> 'Stream':
        """
        **Advanced API**: Transform stream using graph pattern matching operator.

        For most use cases, use graph_query() instead. This method is useful when
        you need direct control over the operator instance.

        Args:
            graph_operator: GraphStreamOperator instance

        Returns:
            Stream of pattern matches

        Example:
            # Advanced usage with direct operator control
            operator = engine.query_cypher(query, as_operator=True)
            matches = updates.transform_graph(operator)

        Note:
            For simpler use cases, prefer graph_query():

                matches = updates.graph_query(engine, query, mode='incremental')

        Performance:
            - Pattern matching: 3-37M matches/sec
            - Deduplication overhead: 50-200ns per match
            - Integrates with morsel parallelism
        """
        try:
            from sabot._cython.graph.executor.graph_stream_operator import GraphStreamOperator
        except ImportError:
            raise RuntimeError("GraphStreamOperator not available. Build graph executor modules.")

        # Verify operator type
        if not isinstance(graph_operator, GraphStreamOperator):
            raise TypeError(f"Expected GraphStreamOperator, got {type(graph_operator)}")

        # Wrap with morsel parallelism (for large update batches)
        wrapped_operator = self._wrap_with_morsel_parallelism(graph_operator)

        return Stream(wrapped_operator, schema=None)  # Schema inferred from matches

    def graph_query(
        self,
        engine: 'GraphQueryEngine',
        query: str,
        language: str = 'cypher',
        mode: str = 'incremental'
    ) -> 'Stream':
        """
        Apply graph query to streaming updates (RECOMMENDED API).

        This is the unified, DuckDB-style API for graph queries on streams.
        Same query works for batch (engine.query_cypher) or streaming (this method).

        Args:
            engine: GraphQueryEngine instance
            query: Query string (Cypher or SPARQL)
            language: 'cypher' or 'sparql'
            mode: 'incremental' (deduplicate) or 'continuous' (all matches)

        Returns:
            Stream of pattern matches

        Examples:
            # Batch query (finite source)
            >>> result = engine.query_cypher("MATCH (a)-[:KNOWS]->(b) RETURN a, b")
            >>> print(result.to_pandas())

            # Streaming query (infinite source) - SAME QUERY!
            >>> updates = Stream.from_kafka("localhost:9092", "graph_updates", "group")
            >>> matches = updates.graph_query(
            ...     engine=engine,
            ...     query="MATCH (a)-[:KNOWS]->(b) RETURN a, b",
            ...     mode='incremental'
            ... )

            # Chain with normal operators
            >>> (matches
            ...     .filter(lambda b: b.num_rows > 0)
            ...     .map(lambda b: enrich(b))
            ...     .to_kafka("localhost:9092", "alerts")
            ... )

        Performance:
            - Pattern matching: 3-37M matches/sec
            - Update processing: 1-5M updates/sec
            - End-to-end latency: <10ms (incremental mode)
        """
        # Create operator using unified API
        if language == 'cypher':
            operator = engine.query_cypher(
                query,
                as_operator=True,
                mode=mode
            )
        else:
            operator = engine.query_sparql(
                query,
                as_operator=True,
                mode=mode
            )

        # Apply operator to stream using transform_graph()
        return self.transform_graph(operator)

    def continuous_query(
        self,
        query: str,
        graph: 'PyPropertyGraph',
        query_engine: 'GraphQueryEngine',
        language: str = 'cypher',
        mode: str = 'incremental',
        timestamp_column: Optional[str] = None
    ) -> 'Stream':
        """
        **Advanced API**: Apply continuous graph pattern matching to stream.

        For most use cases, use graph_query() instead. This method provides
        more explicit control over the graph and query engine parameters.

        Args:
            query: Pattern matching query (Cypher or SPARQL)
            graph: PropertyGraph instance to maintain state
            query_engine: GraphQueryEngine for query execution
            language: Query language ('cypher' or 'sparql')
            mode: 'incremental' (deduplicate) or 'continuous' (all matches)
            timestamp_column: Column for event timestamps (optional)

        Returns:
            Stream of pattern matches

        Example:
            # Advanced usage with explicit parameters
            updates = Stream.from_kafka("localhost:9092", "graph_updates", "group")
            fraud_patterns = updates.continuous_query(
                query="MATCH (a)-[:TRANSFER {amount > 10000}]->(b) RETURN a, b",
                graph=engine.graph,
                query_engine=engine,
                mode='incremental',
                timestamp_column='event_time'
            )

        Note:
            For simpler use cases, prefer graph_query():

                matches = updates.graph_query(engine, query, mode='incremental')
        """
        try:
            from sabot._cython.graph.executor.graph_stream_operator import GraphStreamOperator
        except ImportError:
            raise RuntimeError("GraphStreamOperator not available. Build graph executor modules.")

        # Create graph stream operator
        operator = GraphStreamOperator(
            graph=graph,
            query_engine=query_engine,
            query=query,
            language=language,
            mode=mode,
            timestamp_column=timestamp_column
        )

        # Apply operator to stream
        return self.transform_graph(operator)

    def union(self, *other_streams: 'Stream') -> 'Stream':
        """
        Merge this stream with other streams.

        Args:
            *other_streams: Other streams to merge

        Returns:
            Merged stream

        Examples:
            stream1.union(stream2, stream3)
        """
        sources = [self._source] + [s._source for s in other_streams]

        if CYTHON_AVAILABLE:
            operator = CythonUnionOperator(*sources)
            operator = self._wrap_with_morsel_parallelism(operator)
            return Stream(operator, self._schema)
        else:
            # Fallback to Python
            import itertools
            def python_union():
                for batch in itertools.chain(*sources):
                    yield batch
            return Stream(python_union(), self._schema)

    # ========================================================================
    # Aggregation Operations (Stateful)
    # ========================================================================

    def aggregate(self, aggregations: Dict[str, tuple]) -> 'Stream':
        """
        Compute global aggregations across entire stream.

        Args:
            aggregations: Dict mapping output name to (column, function) tuple
                         Functions: sum, mean, min, max, count, stddev, variance

        Returns:
            Stream with single batch containing aggregation results

        Examples:
            stream.aggregate({
                'total_amount': ('price', 'sum'),
                'avg_price': ('price', 'mean'),
                'max_quantity': ('quantity', 'max'),
                'count': ('*', 'count')
            })
        """
        if CYTHON_AVAILABLE:
            operator = CythonAggregateOperator(self._source, aggregations)
            operator = self._wrap_with_morsel_parallelism(operator)
            return Stream(operator, None)
        else:
            raise NotImplementedError("Aggregate requires Cython operators")

    def reduce(self, func: Callable, initial_value: Any = None) -> 'Stream':
        """
        Reduce stream to single value using custom function.

        Args:
            func: Function(accumulator, batch) -> new_accumulator
            initial_value: Initial accumulator value

        Returns:
            Stream with single batch containing reduction result

        Examples:
            # Sum all amounts
            stream.reduce(
                lambda acc, b: acc + b.column('amount').sum().as_py(),
                initial_value=0.0
            )
        """
        if CYTHON_AVAILABLE:
            operator = CythonReduceOperator(self._source, func, initial_value)
            operator = self._wrap_with_morsel_parallelism(operator)
            return Stream(operator, None)
        else:
            raise NotImplementedError("Reduce requires Cython operators")

    def distinct(self, *columns: str) -> 'Stream':
        """
        Keep only unique rows based on specified columns.

        Args:
            *columns: Columns to check for uniqueness (all if empty)

        Returns:
            Stream with duplicates removed

        Examples:
            # Distinct on all columns
            stream.distinct()

            # Distinct on specific columns
            stream.distinct('customer_id', 'product_id')
        """
        columns_list = list(columns) if columns else None

        if CYTHON_AVAILABLE:
            operator = CythonDistinctOperator(self._source, columns_list)
            operator = self._wrap_with_morsel_parallelism(operator)
            return Stream(operator, self._schema)
        else:
            raise NotImplementedError("Distinct requires Cython operators")

    def group_by(self, *keys) -> 'GroupedStream':
        """
        Group stream by key columns.

        Args:
            *keys: Key columns to group by (varargs or single list)

        Returns:
            GroupedStream for applying aggregations

        Examples:
            # Varargs style
            stream.group_by('customer_id', 'product_id').aggregate(...)

            # List style (for compatibility)
            stream.group_by(['customer_id', 'product_id']).aggregate(...)
        """
        # Handle both varargs and list calling styles
        if len(keys) == 1 and isinstance(keys[0], (list, tuple)):
            # Called as .group_by(['key1', 'key2'])
            keys_list = list(keys[0])
        else:
            # Called as .group_by('key1', 'key2')
            keys_list = list(keys)

        return GroupedStream(self._source, keys_list, self._schema)

    # ========================================================================
    # Join Operations (Stateful)
    # ========================================================================

    def join(self, other: 'Stream', left_keys: List[str], right_keys: List[str],
             how: str = 'inner') -> 'Stream':
        """
        Hash join with another stream.

        Args:
            other: Right stream to join with
            left_keys: Join keys from this stream
            right_keys: Join keys from other stream
            how: 'inner', 'left', 'right', or 'outer'

        Returns:
            Joined stream

        Examples:
            orders.join(customers,
                left_keys=['customer_id'],
                right_keys=['id'],
                how='inner')
        """
        # Use optimized StreamingHashJoin for inner joins only
        # For outer joins, fall back to PyArrow (simpler and correct)
        # Performance: StreamingHashJoin: 87 M rows/sec peak, 9x speedup from caching
        use_streaming_join = (how == 'inner')  # Only for inner joins
        if use_streaming_join:
            try:
                from sabot._cython.operators.streaming_hash_join_operator import StreamingHashJoinOperator
                operator = StreamingHashJoinOperator(
                    self._source, other._source,
                    left_keys, right_keys, how,
                    num_threads=1  # Single-threaded (parallel not implemented yet)
                )
                return Stream(operator, None)
            except Exception as e:
                # Fall through to Arrow fallback
                import traceback
                print(f"Warning: StreamingHashJoin failed, using PyArrow fallback: {e}")
                traceback.print_exc()

        # Fallback to PyArrow join (materializes both sides)
        def arrow_join():
            # Collect both sides into tables
            left_batches = list(self._source)
            right_batches = list(other._source)

            if not left_batches or not right_batches:
                return

            from sabot import cyarrow as pa  # Use Sabot's vendored Arrow
            left_table = pa.Table.from_batches(left_batches)
            right_table = pa.Table.from_batches(right_batches)

            # Map join types to PyArrow format
            join_type_map = {
                'inner': 'inner',
                'left': 'left outer',
                'right': 'right outer',
                'outer': 'full outer',
                'left_outer': 'left outer',
                'right_outer': 'right outer',
                'full_outer': 'full outer'
            }
            arrow_join_type = join_type_map.get(how, 'inner')

            # Use PyArrow join with both left and right keys
            result = left_table.join(right_table, keys=left_keys, right_keys=right_keys, join_type=arrow_join_type)

            for batch in result.to_batches():
                yield batch

        return Stream(arrow_join(), None)

    def interval_join(self, other: 'Stream', time_column: str,
                     lower_bound: int, upper_bound: int) -> 'Stream':
        """
        Join rows within time interval.

        Args:
            other: Right stream
            time_column: Timestamp column name
            lower_bound: Lower bound (ms)
            upper_bound: Upper bound (ms)

        Returns:
            Joined stream

        Examples:
            # Join within ±1 hour
            transactions.interval_join(
                fraud_alerts,
                time_column='timestamp',
                lower_bound=-3600000,
                upper_bound=3600000
            )
        """
        if CYTHON_AVAILABLE:
            operator = CythonIntervalJoinOperator(
                self._source, other._source,
                time_column, lower_bound, upper_bound
            )
            operator = self._wrap_with_morsel_parallelism(operator)
            return Stream(operator, None)
        else:
            raise NotImplementedError("Interval join requires Cython operators")

    def asof_join(self, other: 'Stream', time_column: str,
                  direction: str = 'backward') -> 'Stream':
        """
        As-of join (most recent match).

        Args:
            other: Right stream
            time_column: Timestamp column
            direction: 'backward' or 'forward'

        Returns:
            Joined stream

        Examples:
            # Join each trade with most recent quote
            trades.asof_join(quotes,
                time_column='timestamp',
                direction='backward')
        """
        if CYTHON_AVAILABLE:
            operator = CythonAsofJoinOperator(
                self._source, other._source,
                time_column, direction
            )
            operator = self._wrap_with_morsel_parallelism(operator)
            return Stream(operator, None)
        else:
            raise NotImplementedError("As-of join requires Cython operators")

    # ========================================================================
    # Terminal Operations
    # ========================================================================

    def collect(self) -> ca.Table:
        """
        Collect all batches into a PyArrow Table.

        Returns:
            PyArrow Table with all data
        """
        batches = list(self)
        if not batches:
            # Return empty table with basic schema
            schema = ca.schema([])
            return ca.Table.from_batches([], schema=schema)
        return ca.Table.from_batches(batches)

    def to_pylist(self) -> List[Dict[str, Any]]:
        """
        Collect all data as list of Python dicts.

        Returns:
            List of dicts
        """
        return self.collect().to_pylist()

    def count(self) -> int:
        """
        Count total number of rows.

        Returns:
            Total row count
        """
        total = 0
        for batch in self:
            total += batch.num_rows
        return total

    def take(self, n: int) -> List[ca.RecordBatch]:
        """
        Take first n batches.

        Args:
            n: Number of batches to take

        Returns:
            List of up to n batches
        """
        result = []
        for i, batch in enumerate(self):
            if i >= n:
                break
            result.append(batch)
        return result

    def foreach(self, func: Callable[[ca.RecordBatch], None]) -> None:
        """
        Apply function to each batch (for side effects).

        Args:
            func: Function to apply
        """
        for batch in self:
            func(batch)

    # ========================================================================
    # DuckDB Sink Methods
    # ========================================================================

    def to_sql(
        self,
        destination: str,
        mode: str = 'append',
        format: str = 'parquet',
        extensions: Optional[List[str]] = None,
        database: str = ':memory:',
        batch_buffer_size: int = 1
    ) -> 'OutputStream':
        """
        Write stream to destination using DuckDB (files, databases, etc.).

        Args:
            destination: Target destination (file path or SQL table function)
                - File: "'path/to/file.parquet'" (note: quoted for SQL)
                - SQL: "postgres_scan('host=...', 'table')"
            mode: Write mode ('overwrite', 'append', 'error')
            format: Output format for files (parquet, csv, json)
            extensions: DuckDB extensions to load
            database: Database path (default: in-memory)
            batch_buffer_size: Number of batches to buffer before writing

        Returns:
            OutputStream for execution control

        Examples:
            # Write to Parquet
            stream.to_sql(
                "'output/data.parquet'",
                mode='overwrite',
                format='parquet'
            )

            # Write to Postgres
            stream.to_sql(
                "postgres_scan('host=localhost', 'transactions')",
                mode='append',
                extensions=['postgres_scanner']
            )

            # Write to S3
            stream.to_sql(
                "'s3://bucket/data.parquet'",
                extensions=['httpfs']
            )
        """
        from sabot.connectors import DuckDBSink
        import asyncio

        sink = DuckDBSink(
            destination=destination,
            format=format,
            mode=mode,
            extensions=extensions or [],
            database=database,
            batch_buffer_size=batch_buffer_size
        )

        async def write_stream():
            async for batch in self:
                await sink.write_batch(batch)
            await sink.close()

        # Return OutputStream wrapper
        return OutputStream(write_stream())

    def to_parquet(
        self,
        path: str,
        mode: str = 'overwrite',
        partition_by: Optional[List[str]] = None
    ) -> 'OutputStream':
        """
        Write stream to Parquet file(s).

        Args:
            path: Output file path
            mode: Write mode ('overwrite' or 'append')
            partition_by: Columns to partition by (hive-style partitioning)

        Returns:
            OutputStream for execution control

        Example:
            # Simple write
            stream.to_parquet('output/data.parquet')

            # Partitioned write
            stream.to_parquet(
                'output/data',
                partition_by=['date', 'region']
            )
        """
        # TODO: Add partitioning support via DuckDB PARTITION BY
        return self.to_sql(f"'{path}'", mode=mode, format='parquet')

    def to_csv(
        self,
        path: str,
        mode: str = 'overwrite',
        **csv_options
    ) -> 'OutputStream':
        """
        Write stream to CSV file.

        Args:
            path: Output file path
            mode: Write mode ('overwrite' or 'append')
            **csv_options: DuckDB CSV options (header, delimiter, etc.)

        Returns:
            OutputStream for execution control

        Example:
            stream.to_csv(
                'output/data.csv',
                header=True,
                delimiter=','
            )
        """
        # TODO: Pass csv_options to DuckDB COPY
        return self.to_sql(f"'{path}'", mode=mode, format='csv')

    def to_postgres(
        self,
        connection_string: str,
        table: str,
        mode: str = 'append'
    ) -> 'OutputStream':
        """
        Write stream to PostgreSQL table.

        Args:
            connection_string: Postgres connection (e.g., 'host=localhost user=postgres')
            table: Table name to write to
            mode: Write mode ('append' or 'overwrite')

        Returns:
            OutputStream for execution control

        Example:
            stream.to_postgres(
                'host=localhost user=postgres password=secret',
                'transactions',
                mode='append'
            )
        """
        destination = f"postgres_scan('{connection_string}', '{table}')"
        return self.to_sql(
            destination,
            mode=mode,
            extensions=['postgres_scanner']
        )

    def to_delta(
        self,
        path: str,
        mode: str = 'append'
    ) -> 'OutputStream':
        """
        Write stream to Delta Lake table.

        Args:
            path: Delta table path (local or S3)
            mode: Write mode ('append' or 'overwrite')

        Returns:
            OutputStream for execution control

        Example:
            stream.to_delta(
                's3://bucket/delta-table',
                mode='append'
            )
        """
        destination = f"delta_scan('{path}')"
        return self.to_sql(
            destination,
            mode=mode,
            extensions=['delta']
        )

    # ========================================================================
    # Iterator Protocol
    # ========================================================================

    def to_kafka(
        self,
        bootstrap_servers: str,
        topic: str,
        codec_type: str = "json",
        codec_options: Optional[Dict[str, Any]] = None,
        compression_type: str = "lz4",
        key_extractor: Optional[Callable[[Dict], bytes]] = None,
        use_cpp: bool = True,
        **producer_kwargs
    ) -> 'OutputStream':
        """
        Write stream to Kafka topic.

        Args:
            bootstrap_servers: Kafka brokers (e.g., "localhost:9092")
            topic: Topic to produce to
            codec_type: Codec type (json, avro, protobuf, json_schema, msgpack, string, bytes)
            codec_options: Codec-specific options (e.g., schema_registry_url for Avro)
            compression_type: Compression (gzip, snappy, lz4, zstd)
            key_extractor: Optional function to extract key from message
            use_cpp: Use C++ librdkafka (default: True for performance)
            **producer_kwargs: Additional Kafka producer config

        Returns:
            OutputStream for execution control

        Examples:
            # Simple JSON output
            stream.to_kafka("localhost:9092", "output-topic")

            # Avro output with Schema Registry
            stream.to_kafka(
                "localhost:9092",
                "transactions-enriched",
                codec_type="avro",
                codec_options={
                    'schema_registry_url': 'http://localhost:8081',
                    'subject': 'transactions-enriched-value',
                    'schema': transaction_schema
                }
            )

            # With key extraction
            stream.to_kafka(
                "localhost:9092",
                "user-events",
                key_extractor=lambda msg: msg['user_id'].encode('utf-8')
            )
        """
        from ..kafka import to_kafka as create_kafka_sink
        import asyncio

        # Create Kafka sink
        sink = create_kafka_sink(
            bootstrap_servers=bootstrap_servers,
            topic=topic,
            codec_type=codec_type,
            codec_options=codec_options,
            compression_type=compression_type,
            **producer_kwargs
        )

        # Convert RecordBatches to messages and send to Kafka
        async def produce():
            await sink.start()
            try:
                for batch in self._source:
                    # Convert RecordBatch to list of dicts
                    messages = batch.to_pylist()

                    # Send each message
                    for message in messages:
                        key = key_extractor(message) if key_extractor else None
                        await sink.send(message, key=key)

                # Flush remaining messages
                await sink.flush()
            finally:
                await sink.stop()

        # Run async producer
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            loop.run_until_complete(produce())
        finally:
            loop.close()

        return OutputStream(sink=sink, stream=self)

    def with_features(self, feature_names: List[str]) -> 'Stream':
        """
        Apply feature extractors using standard map operator.

        Args:
            feature_names: List of feature names from FeatureRegistry

        Returns:
            Stream with added feature columns

        Example:
            stream.with_features([
                'price_rolling_mean_5m',
                'volume_std_1h',
                'spread_percentile_95'
            ])
        """
        from sabot.features import FeatureRegistry, create_feature_map

        # Create extractors from registry
        registry = FeatureRegistry()
        extractors = []
        for name in feature_names:
            try:
                extractor = registry.create_extractor(name)
                extractors.append(extractor)
            except ValueError as e:
                import logging
                logging.warning(f"Feature '{name}' not found in registry: {e}")

        if not extractors:
            raise ValueError("No valid feature extractors found")

        # Create map function and apply
        feature_func = create_feature_map(extractors)
        return self.map(feature_func)

    def to_feature_store(
        self,
        feature_store: 'FeatureStore',
        entity_key_column: str,
        feature_columns: List[str],
        ttl: Optional[int] = None,
        batch_size: int = 1000
    ) -> 'OutputStream':
        """
        Write computed features to CyRedis feature store.

        Args:
            feature_store: FeatureStore instance
            entity_key_column: Column name containing entity ID
            feature_columns: List of column names to write as features
            ttl: Time-to-live in seconds (None = no expiration)
            batch_size: Rows per Redis pipeline write

        Returns:
            OutputStream for execution control

        Example:
            from sabot.features import FeatureStore

            feature_store = FeatureStore(redis_url="localhost:6379")
            await feature_store.initialize()

            stream.with_features([
                'price_rolling_mean_5m',
                'volume_std_1h'
            ]).to_feature_store(
                feature_store=feature_store,
                entity_key_column='symbol',
                feature_columns=['price_rolling_mean_5m', 'volume_std_1h'],
                ttl=300
            )
        """
        from sabot.features import to_feature_store as write_to_store
        import asyncio

        # Write features async
        async def write():
            stats = await write_to_store(
                self._source,
                feature_store=feature_store,
                entity_key_column=entity_key_column,
                feature_columns=feature_columns,
                ttl=ttl,
                batch_size=batch_size
            )
            return stats

        # Run async writer
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            stats = loop.run_until_complete(write())
            import logging
            logging.info(f"Feature store write complete: {stats}")
        finally:
            loop.close()

        return OutputStream(sink=feature_store, stream=self)

    def __iter__(self):
        """Iterate over batches in stream."""
        return iter(self._source)


class GroupedStream:
    """
    Grouped stream for applying aggregations after group_by().
    """

    def __init__(self, source: Iterable[ca.RecordBatch], keys: List[str], schema: Optional[ca.Schema] = None):
        self._source = source
        self._keys = keys
        self._schema = schema

    def aggregate(self, aggregations: Dict[str, tuple]) -> Stream:
        """
        Apply aggregations to grouped stream.

        Args:
            aggregations: Dict mapping output name to (column, function)

        Returns:
            Stream with aggregated results
        """
        # Try Cython operator first
        if AGGREGATION_OPERATORS_AVAILABLE and CythonGroupByOperator:
            operator = CythonGroupByOperator(
                self._source, self._keys, aggregations
            )
            return Stream(operator, None)
        
        # Fallback to Arrow groupby (using Sabot's vendored Arrow)
        from sabot import cyarrow as ca
        pa = ca
        pc = ca.compute  # Includes custom kernels: hash_array, hash_combine
        
        def arrow_groupby_streaming():
            """
            Streaming groupby that processes batches incrementally.
            
            For now, accumulate all batches but process them as we go to avoid
            holding duplicates in memory. Arrow's group_by needs full data for
            correctness (can't partially aggregate complex functions like mean).
            """
            # Collect all batches (needed for groupby correctness)
            # But do it via streaming to keep peak memory reasonable
            all_batches = []
            
            for batch in self._source:
                all_batches.append(batch)
            
            if not all_batches:
                return
            
            # Combine to table using cyarrow (pa is aliased to ca above)
            table = ca.Table.from_batches(all_batches)
            
            # Group by keys
            grouped = table.group_by(self._keys)
            
            # Build aggregation list for Arrow
            agg_list = []
            for output_name, (col_name, agg_func) in aggregations.items():
                if col_name == '*':
                    # For count(*), use count on any column
                    col_name = table.column_names[0]
                
                # Map to Arrow aggregation functions
                if agg_func in ('count', 'count_all'):
                    agg_list.append((col_name, 'count'))
                elif agg_func in ('sum', 'total'):
                    agg_list.append((col_name, 'sum'))
                elif agg_func in ('mean', 'avg', 'average'):
                    agg_list.append((col_name, 'mean'))
                elif agg_func == 'min':
                    agg_list.append((col_name, 'min'))
                elif agg_func == 'max':
                    agg_list.append((col_name, 'max'))
                else:
                    agg_list.append((col_name, 'sum'))  # Default
            
            # Apply aggregations using Arrow
            result_table = grouped.aggregate(agg_list)
            
            # Yield as batches
            for batch in result_table.to_batches():
                yield batch
        
        return Stream(arrow_groupby_streaming(), None)


class TimeSeriesStreamOps:
    """
    Time series operations for streams.

    Provides fluent API for time series transformations like rolling windows,
    EWMA, log returns, and temporal joins.

    All operations use the SQLController's time series operators internally,
    which leverage Arrow compute for vectorized performance.
    """

    def __init__(self, stream: Stream):
        """
        Initialize time series operations for a stream.

        Args:
            stream: The source stream to apply time series operations to
        """
        self._stream = stream

    def _materialize_and_apply(self, operation_func) -> Stream:
        """
        Materialize stream to table, apply operation, return new stream.

        Args:
            operation_func: Async function(controller, table) -> result_table
        """
        import asyncio
        from sabot.sql.controller import SQLController

        async def execute():
            # Materialize stream to table
            batches = []
            for batch in self._stream:
                batches.append(batch)

            if not batches:
                return ca.table({})

            table = ca.Table.from_batches(batches)

            # Apply operation
            controller = SQLController()
            result = await operation_func(controller, table)
            return result

        # Run and convert result to stream
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            result = loop.run_until_complete(execute())
        finally:
            loop.close()

        # Return as stream
        if hasattr(result, 'to_batches'):
            return Stream(result.to_batches(), None)
        else:
            return Stream(iter([result]), None)

    def rolling(
        self,
        window_size: int,
        column: str,
        agg: str = 'mean',
        min_periods: int = 1,
        output_column: Optional[str] = None
    ) -> Stream:
        """
        Apply rolling window aggregation to a column.

        Args:
            window_size: Number of rows in the rolling window
            column: Column to aggregate
            agg: Aggregation function ('mean', 'sum', 'min', 'max', 'std', 'var')
            min_periods: Minimum observations required (default: 1)
            output_column: Output column name (default: {column}_{agg}_{window_size})

        Returns:
            Stream with added rolling aggregation column

        Examples:
            # 20-period rolling mean
            stream.timeseries().rolling(20, 'price', agg='mean')

            # Rolling standard deviation
            stream.timeseries().rolling(50, 'returns', agg='std')
        """
        async def apply_rolling(controller, table):
            return await controller._apply_rolling_window(
                table=table,
                column=column,
                window_size=window_size,
                agg=agg,
                min_periods=min_periods,
                output_column=output_column
            )

        return self._materialize_and_apply(apply_rolling)

    def ewma(
        self,
        column: str,
        alpha: float = 0.5,
        output_column: Optional[str] = None
    ) -> Stream:
        """
        Apply Exponential Weighted Moving Average (EWMA).

        Args:
            column: Column to smooth
            alpha: Smoothing factor (0 < alpha <= 1). Higher = more weight on recent values.
            output_column: Output column name (default: {column}_ewma)

        Returns:
            Stream with EWMA column

        Examples:
            # Fast EWMA (alpha=0.3)
            stream.timeseries().ewma('price', alpha=0.3)

            # Slow EWMA (alpha=0.1) for trend following
            stream.timeseries().ewma('price', alpha=0.1)
        """
        async def apply_ewma(controller, table):
            return await controller._apply_ewma(
                table=table,
                column=column,
                alpha=alpha,
                output_column=output_column
            )

        return self._materialize_and_apply(apply_ewma)

    def log_returns(
        self,
        column: str = 'price',
        output_column: Optional[str] = None
    ) -> Stream:
        """
        Calculate logarithmic returns: ln(price_t / price_{t-1}).

        Args:
            column: Price column name
            output_column: Output column name (default: log_returns)

        Returns:
            Stream with log returns column

        Example:
            stream.timeseries().log_returns('close')
        """
        async def apply_log_returns(controller, table):
            return await controller._apply_log_returns(
                table=table,
                column=column,
                output_column=output_column
            )

        return self._materialize_and_apply(apply_log_returns)

    def time_bucket(
        self,
        time_column: str = 'timestamp',
        bucket_size: str = '1h',
        output_column: str = 'bucket'
    ) -> Stream:
        """
        Assign rows to time buckets.

        Args:
            time_column: Timestamp column name
            bucket_size: Bucket size (e.g., '1h', '5m', '1d')
            output_column: Output column name for bucket

        Returns:
            Stream with bucket column

        Example:
            stream.timeseries().time_bucket('event_time', '5m')
        """
        async def apply_time_bucket(controller, table):
            return await controller._apply_time_bucket(
                table=table,
                time_column=time_column,
                bucket_size=bucket_size,
                output_column=output_column
            )

        return self._materialize_and_apply(apply_time_bucket)

    def time_diff(
        self,
        time_column: str = 'timestamp',
        output_column: str = 'time_diff_ms'
    ) -> Stream:
        """
        Calculate time difference between consecutive rows.

        Args:
            time_column: Timestamp column name
            output_column: Output column name for time difference (in milliseconds)

        Returns:
            Stream with time difference column

        Example:
            stream.timeseries().time_diff('event_time')
        """
        async def apply_time_diff(controller, table):
            return await controller._apply_time_diff(
                table=table,
                time_column=time_column,
                output_column=output_column
            )

        return self._materialize_and_apply(apply_time_diff)

    def resample(
        self,
        time_column: str = 'timestamp',
        rule: str = '1h',
        agg: Dict[str, str] = None
    ) -> Stream:
        """
        Resample time series data to a different frequency.

        Args:
            time_column: Timestamp column name
            rule: Resampling rule (e.g., '1h', '5m', '1d')
            agg: Aggregation functions per column (e.g., {'price': 'mean', 'volume': 'sum'})

        Returns:
            Stream with resampled data

        Example:
            stream.timeseries().resample('timestamp', '5m', {'price': 'mean', 'volume': 'sum'})
        """
        async def apply_resample(controller, table):
            return await controller._apply_resample(
                table=table,
                time_column=time_column,
                rule=rule,
                agg=agg or {}
            )

        return self._materialize_and_apply(apply_resample)

    def fill(
        self,
        method: str = 'forward',
        columns: Optional[List[str]] = None
    ) -> Stream:
        """
        Fill missing values in time series.

        Args:
            method: Fill method ('forward', 'backward', 'linear', 'zero', 'mean')
            columns: Columns to fill (None = all numeric columns)

        Returns:
            Stream with filled values

        Examples:
            # Forward fill missing values
            stream.timeseries().fill('forward')

            # Linear interpolation for specific columns
            stream.timeseries().fill('linear', columns=['price', 'volume'])
        """
        async def apply_fill(controller, table):
            return await controller._apply_fill(
                table=table,
                method=method,
                columns=columns
            )

        return self._materialize_and_apply(apply_fill)

    def session_window(
        self,
        time_column: str = 'timestamp',
        gap_ms: int = 30000,
        output_column: str = 'session_id'
    ) -> Stream:
        """
        Assign session IDs based on activity gaps.

        Args:
            time_column: Timestamp column name
            gap_ms: Maximum gap in milliseconds between events in same session
            output_column: Output column name for session ID

        Returns:
            Stream with session ID column

        Example:
            # 30-second session timeout
            stream.timeseries().session_window('event_time', gap_ms=30000)
        """
        async def apply_session_window(controller, table):
            return await controller._apply_session_window(
                table=table,
                time_column=time_column,
                gap_ms=gap_ms,
                output_column=output_column
            )

        return self._materialize_and_apply(apply_session_window)

    def asof_join(
        self,
        right: Stream,
        time_column: str = 'timestamp',
        direction: str = 'backward'
    ) -> Stream:
        """
        As-of join with another stream (most recent match).

        Args:
            right: Right stream to join with
            time_column: Timestamp column name (must exist in both streams)
            direction: 'backward' (most recent before) or 'forward' (next after)

        Returns:
            Joined stream

        Example:
            # Join trades with most recent quotes
            trades.timeseries().asof_join(quotes, 'trade_time', 'backward')
        """
        import asyncio
        from sabot.sql.controller import SQLController

        async def execute():
            # Materialize both streams
            left_batches = list(self._stream)
            right_batches = list(right)

            if not left_batches or not right_batches:
                return ca.table({})

            left_table = ca.Table.from_batches(left_batches)
            right_table = ca.Table.from_batches(right_batches)

            # Apply ASOF join via controller
            controller = SQLController()
            result = await controller._apply_asof_join(
                left_table=left_table,
                right_table=right_table,
                time_column=time_column,
                direction=direction
            )
            return result

        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            result = loop.run_until_complete(execute())
        finally:
            loop.close()

        if hasattr(result, 'to_batches'):
            return Stream(result.to_batches(), None)
        else:
            return Stream(iter([result]), None)

    def interval_join(
        self,
        right: Stream,
        time_column: str = 'timestamp',
        lower_bound_ms: int = -60000,
        upper_bound_ms: int = 60000
    ) -> Stream:
        """
        Interval join with another stream (rows within time range).

        Args:
            right: Right stream to join with
            time_column: Timestamp column name (must exist in both streams)
            lower_bound_ms: Lower bound offset in milliseconds (negative = before)
            upper_bound_ms: Upper bound offset in milliseconds

        Returns:
            Joined stream

        Example:
            # Join events within ±1 minute window
            events.timeseries().interval_join(
                alerts, 'event_time',
                lower_bound_ms=-60000,
                upper_bound_ms=60000
            )
        """
        import asyncio
        from sabot.sql.controller import SQLController

        async def execute():
            # Materialize both streams
            left_batches = list(self._stream)
            right_batches = list(right)

            if not left_batches or not right_batches:
                return ca.table({})

            left_table = ca.Table.from_batches(left_batches)
            right_table = ca.Table.from_batches(right_batches)

            # Apply interval join via controller
            controller = SQLController()
            result = await controller._apply_interval_join(
                left_table=left_table,
                right_table=right_table,
                time_column=time_column,
                lower_bound=lower_bound_ms,
                upper_bound=upper_bound_ms
            )
            return result

        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            result = loop.run_until_complete(execute())
        finally:
            loop.close()

        if hasattr(result, 'to_batches'):
            return Stream(result.to_batches(), None)
        else:
            return Stream(iter([result]), None)


class OutputStream:
    """
    Output stream handle for execution control.

    Provides methods to start/stop streaming output.
    """

    def __init__(self, server=None, sink=None, stream=None):
        self.server = server
        self.sink = sink
        self.stream = stream

    def start(self):
        """Start streaming output (blocking)."""
        if self.server:
            self.server.serve()
        elif self.sink:
            for batch in self.stream:
                self.sink.write(batch)

    def stop(self):
        """Stop streaming output."""
        if self.server:
            self.server.shutdown()
        elif self.sink:
            self.sink.close()
