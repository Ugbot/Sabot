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
import pyarrow as pa
import pyarrow.compute as pc

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
    CYTHON_AVAILABLE = True
except ImportError:
    CYTHON_AVAILABLE = False


class Stream:
    """
    High-level streaming API with automatic Cython acceleration.

    All operations are lazy and zero-copy. Data is only processed
    when the stream is consumed (iterated).

    Examples:
        # From RecordBatches
        stream = Stream.from_batches(batches)

        # From PyArrow Table
        stream = Stream.from_table(table)

        # From Python dicts
        stream = Stream.from_dicts([{'a': 1}, {'a': 2}])

        # Chain operations
        result = (stream
            .filter(lambda b: pc.greater(b.column('amount'), 1000))
            .map(lambda b: b.append_column('fee', pc.multiply(b.column('amount'), 0.03)))
            .select('id', 'amount', 'fee')
        )

        # Consume (lazy evaluation)
        for batch in result:
            print(batch)
    """

    def __init__(self, source: Iterable[pa.RecordBatch], schema: Optional[pa.Schema] = None):
        """
        Create a stream from a source of RecordBatches.

        Args:
            source: Iterable of RecordBatches
            schema: Optional schema (inferred from first batch if None)
        """
        self._source = source
        self._schema = schema

    @classmethod
    def from_batches(cls, batches: Iterable[pa.RecordBatch]) -> 'Stream':
        """Create stream from RecordBatches."""
        return cls(batches)

    @classmethod
    def from_table(cls, table: pa.Table, batch_size: int = 10000) -> 'Stream':
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
                yield pa.RecordBatch.from_pylist(chunk)

        return cls(dict_batches())

    @classmethod
    def from_pylist(cls, pylist: List[Dict[str, Any]], batch_size: int = 10000) -> 'Stream':
        """Alias for from_dicts."""
        return cls.from_dicts(pylist, batch_size)

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
                        yield pa.RecordBatch.from_pylist(buffer)
                        buffer = []

                # Flush remaining
                if buffer:
                    yield pa.RecordBatch.from_pylist(buffer)

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

    # ========================================================================
    # Transform Operations (Stateless)
    # ========================================================================

    def filter(self, predicate: Callable[[pa.RecordBatch], Union[pa.Array, bool]]) -> 'Stream':
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

            # Complex predicate
            stream.filter(lambda b: pc.and_(
                pc.greater(b.column('price'), 100),
                pc.equal(b.column('side'), 'BUY')
            ))
        """
        if CYTHON_AVAILABLE:
            return Stream(CythonFilterOperator(self._source, predicate), self._schema)
        else:
            # Fallback to Python
            def python_filter():
                for batch in self._source:
                    mask = predicate(batch)
                    if isinstance(mask, pa.Array):
                        filtered = batch.filter(mask)
                        if filtered.num_rows > 0:
                            yield filtered
                    elif mask:
                        yield batch
            return Stream(python_filter(), self._schema)

    def map(self, func: Callable[[pa.RecordBatch], pa.RecordBatch]) -> 'Stream':
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
            return Stream(CythonMapOperator(self._source, func), self._schema)
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
            return Stream(CythonSelectOperator(self._source, columns_list), None)
        else:
            # Fallback to Python
            def python_select():
                for batch in self._source:
                    yield batch.select(columns_list)
            return Stream(python_select(), None)

    def flat_map(self, func: Callable[[pa.RecordBatch], List[pa.RecordBatch]]) -> 'Stream':
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
            return Stream(CythonFlatMapOperator(self._source, func), self._schema)
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
            return Stream(CythonUnionOperator(*sources), self._schema)
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
            return Stream(CythonAggregateOperator(self._source, aggregations), None)
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
            return Stream(CythonReduceOperator(self._source, func, initial_value), None)
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
            return Stream(CythonDistinctOperator(self._source, columns_list), self._schema)
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
            return Stream(CythonHashJoinOperator(
                self._source, other._source,
                left_keys, right_keys, how
            ), None)
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
            return Stream(CythonIntervalJoinOperator(
                self._source, other._source,
                time_column, lower_bound, upper_bound
            ), None)
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
            return Stream(CythonAsofJoinOperator(
                self._source, other._source,
                time_column, direction
            ), None)
        else:
            raise NotImplementedError("As-of join requires Cython operators")

    # ========================================================================
    # Terminal Operations
    # ========================================================================

    def collect(self) -> pa.Table:
        """
        Collect all batches into a PyArrow Table.

        Returns:
            PyArrow Table with all data
        """
        batches = list(self)
        if not batches:
            return pa.Table.from_batches([])
        return pa.Table.from_batches(batches)

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

    def take(self, n: int) -> List[pa.RecordBatch]:
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

    def foreach(self, func: Callable[[pa.RecordBatch], None]) -> None:
        """
        Apply function to each batch (for side effects).

        Args:
            func: Function to apply
        """
        for batch in self:
            func(batch)

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

    def __iter__(self):
        """Iterate over batches in stream."""
        return iter(self._source)


class GroupedStream:
    """
    Grouped stream for applying aggregations after group_by().
    """

    def __init__(self, source, keys: List[str], schema: Optional[pa.Schema] = None):
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
            return Stream(CythonGroupByOperator(
                self._source, self._keys, aggregations
            ), None)
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
