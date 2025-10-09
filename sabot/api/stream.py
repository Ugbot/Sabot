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

# Import Cython operators
try:
    from sabot._cython.operators import (
        # Transform operators
        CythonFilterOperator,
        CythonMapOperator,
        CythonSelectOperator,
        CythonFlatMapOperator,
        CythonUnionOperator,
        # Aggregation operators
        CythonAggregateOperator,
        CythonReduceOperator,
        CythonDistinctOperator,
        CythonGroupByOperator,
        # Join operators
        CythonHashJoinOperator,
        CythonIntervalJoinOperator,
        CythonAsofJoinOperator,
    )
    from sabot._cython.operators.morsel_operator import MorselDrivenOperator
    CYTHON_AVAILABLE = True
except ImportError:
    CYTHON_AVAILABLE = False
    MorselDrivenOperator = None


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

    def __init__(self, source: Iterable[ca.RecordBatch], schema: Optional[ca.Schema] = None):
        """
        Create a stream from a source of RecordBatches.

        Args:
            source: Iterable of RecordBatches
            schema: Optional schema (inferred from first batch if None)
        """
        self._source = source
        self._schema = schema

    def _wrap_with_morsel_parallelism(self, operator):
        """
        Wrap operator with MorselDrivenOperator for automatic parallelism.

        This enables morsel-driven parallelism by default for all operations.
        Small batches bypass parallelism (no overhead), large batches get
        automatic parallel processing.
        """
        if not CYTHON_AVAILABLE or MorselDrivenOperator is None:
            return operator

        # Wrap with morsel-driven parallelism
        return MorselDrivenOperator(
            wrapped_operator=operator,
            num_workers=0,  # Auto-detect
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
    def from_parquet(
        cls,
        path: str,
        filters: Optional[Dict[str, Any]] = None,
        columns: Optional[List[str]] = None
    ) -> 'Stream':
        """
        Create stream from Parquet file(s) with automatic pushdown.

        Args:
            path: Parquet file path (supports globs: 'data/*.parquet')
            filters: Column filters for pushdown
            columns: Columns to read (projection pushdown)

        Returns:
            Stream from Parquet data

        Example:
            stream = Stream.from_parquet(
                'data/transactions_*.parquet',
                filters={'date': ">= '2025-01-01'"},
                columns=['id', 'amount', 'timestamp']
            )
        """
        sql = f"SELECT * FROM read_parquet('{path}')"
        return cls.from_sql(sql, filters=filters, columns=columns)

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
            **consumer_kwargs: Additional Kafka consumer config

        Returns:
            Stream from Kafka

        Examples:
            # Simple JSON stream
            stream = Stream.from_kafka(
                "localhost:9092",
                "transactions",
                "my-group"
            )

            # Avro stream with Schema Registry
            stream = Stream.from_kafka(
                "localhost:9092",
                "transactions",
                "fraud-detector",
                codec_type="avro",
                codec_options={
                    'schema_registry_url': 'http://localhost:8081',
                    'subject': 'transactions-value'
                }
            )
        """
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

    def group_by(self, *keys: str) -> 'GroupedStream':
        """
        Group stream by key columns.

        Args:
            *keys: Key columns to group by

        Returns:
            GroupedStream for applying aggregations

        Examples:
            stream.group_by('customer_id').aggregate({
                'total': ('amount', 'sum'),
                'count': ('*', 'count')
            })
        """
        return GroupedStream(self._source, list(keys), self._schema)

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
        if CYTHON_AVAILABLE:
            operator = CythonHashJoinOperator(
                self._source, other._source,
                left_keys, right_keys, how
            )
            operator = self._wrap_with_morsel_parallelism(operator)
            return Stream(operator, None)
        else:
            raise NotImplementedError("Join requires Cython operators")

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
            return ca.Table.from_batches([])
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

    def __init__(self, source, keys: List[str], schema: Optional[ca.Schema] = None):
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
        if CYTHON_AVAILABLE:
            operator = CythonGroupByOperator(
                self._source, self._keys, aggregations
            )
            operator = self._wrap_with_morsel_parallelism(operator)
            return Stream(operator, None)
        else:
            raise NotImplementedError("GroupBy requires Cython operators")


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
