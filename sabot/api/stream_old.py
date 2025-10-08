"""
High-Level Stream API

Provides user-friendly stream processing operations while delegating to
zero-copy Cython operators underneath for Flink-level performance.
"""

from typing import Callable, Optional, Dict, Any, Iterator, Union
from collections.abc import Iterable
from .. import arrow as pa


class Stream:
    """
    High-level stream abstraction for Arrow RecordBatch processing.

    All operations maintain zero-copy semantics underneath while providing
    a clean Python API. Uses Cython operators for hot path performance.

    Example:
        # From source
        stream = Stream.from_kafka(topic="events", schema=schema)

        # Transform
        stream = stream.map(lambda batch: compute_features(batch))
        stream = stream.filter(lambda batch: batch.column('score') > 0.5)

        # Aggregate
        result = stream.aggregate(sum='amount', count='id')

        # Output
        stream.to_flight("grpc://localhost:8815")
    """

    def __init__(self, source: Union[Iterable, Callable], schema: pa.Schema):
        """
        Initialize stream.

        Args:
            source: Iterator/generator yielding RecordBatches or callable
            schema: Arrow schema for stream data
        """
        self.schema = schema
        self._source = source
        self._operators = []  # Chain of transformations

    @classmethod
    def from_iterator(cls, iterator: Iterable, schema: pa.Schema) -> 'Stream':
        """
        Create stream from Python iterator/generator.

        Args:
            iterator: Iterator yielding RecordBatches
            schema: Arrow schema

        Returns:
            Stream instance
        """
        return cls(source=iterator, schema=schema)

    @classmethod
    def from_batches(cls, batches: list, schema: Optional[pa.Schema] = None) -> 'Stream':
        """
        Create stream from list of RecordBatches.

        Args:
            batches: List of RecordBatches
            schema: Arrow schema (inferred from first batch if not provided)

        Returns:
            Stream instance
        """
        if not batches:
            raise ValueError("Cannot create stream from empty batch list")

        if schema is None:
            schema = batches[0].schema

        return cls(source=iter(batches), schema=schema)

    @classmethod
    def from_kafka(cls, topic: str, schema: pa.Schema, **kafka_config) -> 'Stream':
        """
        Create stream from Kafka topic.

        Args:
            topic: Kafka topic name
            schema: Arrow schema for deserialization
            **kafka_config: Kafka consumer configuration

        Returns:
            Stream instance
        """
        # Import here to avoid circular dependency
        from ..channels_flight import KafkaSource

        source = KafkaSource(topic=topic, schema=schema, **kafka_config)
        return cls(source=source, schema=schema)

    @classmethod
    def from_flight(cls, location: str, path: str) -> 'Stream':
        """
        Create stream from Arrow Flight server.

        Args:
            location: Flight server location (e.g., "grpc://localhost:8815")
            path: Stream path on server

        Returns:
            Stream instance
        """
        try:
            from .._cython.flight.flight_client import FlightClient
        except ImportError:
            raise ImportError("Flight not available. Rebuild with ARROW_FLIGHT=ON")

        client = FlightClient(location)
        reader = client.do_get(path)
        schema = reader.schema()

        return cls(source=reader, schema=schema)

    def map(self, func: Callable[[pa.RecordBatch], pa.RecordBatch]) -> 'Stream':
        """
        Transform each RecordBatch with a function.

        Args:
            func: Function taking RecordBatch and returning RecordBatch

        Returns:
            New Stream with transformation applied

        Example:
            stream.map(lambda batch: pa.RecordBatch.from_pydict({
                'doubled': batch.column('value') * 2
            }))
        """
        self._operators.append(('map', func))
        return self

    def filter(self, predicate: Callable[[pa.RecordBatch], pa.Array]) -> 'Stream':
        """
        Filter batches based on predicate.

        Args:
            predicate: Function taking RecordBatch and returning boolean Array

        Returns:
            New Stream with filter applied

        Example:
            stream.filter(lambda batch: batch.column('value') > 100)
        """
        self._operators.append(('filter', predicate))
        return self

    def flat_map(self, func: Callable[[pa.RecordBatch], Iterator[pa.RecordBatch]]) -> 'Stream':
        """
        Transform each batch into multiple batches.

        Args:
            func: Function taking RecordBatch and returning iterator of RecordBatches

        Returns:
            New Stream with flat_map applied

        Example:
            stream.flat_map(lambda batch: [batch.slice(0, 10), batch.slice(10, 10)])
        """
        self._operators.append(('flat_map', func))
        return self

    def aggregate(self, **agg_funcs) -> 'AggregatedStream':
        """
        Aggregate stream with named functions.

        Args:
            **agg_funcs: Aggregation functions (sum='col', count='*', etc.)

        Returns:
            AggregatedStream for further operations

        Example:
            stream.aggregate(sum='amount', count='*', avg='score')
        """
        return AggregatedStream(self, agg_funcs)

    def window(self, window_spec) -> 'WindowedStream':
        """
        Apply windowing to stream.

        Args:
            window_spec: Window specification (tumbling, sliding, session)

        Returns:
            WindowedStream for aggregation

        Example:
            stream.window(tumbling(seconds=60))
                  .aggregate(sum='amount')
        """
        return WindowedStream(self, window_spec)

    def key_by(self, key_func: Union[str, Callable]) -> 'KeyedStream':
        """
        Partition stream by key for stateful operations.

        Args:
            key_func: Column name or function extracting key from batch

        Returns:
            KeyedStream for stateful operations

        Example:
            stream.key_by('user_id')
                  .reduce(lambda acc, batch: acc + batch)
        """
        return KeyedStream(self, key_func)

    def to_flight(self, location: str, path: str = "/stream") -> 'OutputStream':
        """
        Send stream to Arrow Flight server.

        Args:
            location: Flight server location (e.g., "grpc://localhost:8815")
            path: Stream path on server

        Returns:
            OutputStream for execution control

        Example:
            stream.to_flight("grpc://localhost:8815", "/my_stream")
        """
        try:
            from .._cython.flight.flight_server import FlightServer
        except ImportError:
            raise ImportError("Flight not available. Rebuild with ARROW_FLIGHT=ON")

        server = FlightServer(location)
        server.register_stream(path, self.schema, self._execute_iterator())

        return OutputStream(server=server, stream=self)

    def to_kafka(self, topic: str, **kafka_config) -> 'OutputStream':
        """
        Send stream to Kafka topic.

        Args:
            topic: Kafka topic name
            **kafka_config: Kafka producer configuration

        Returns:
            OutputStream for execution control
        """
        from ..channels_flight import KafkaSink

        sink = KafkaSink(topic=topic, schema=self.schema, **kafka_config)
        return OutputStream(sink=sink, stream=self)

    def collect(self) -> pa.Table:
        """
        Execute stream and collect all batches into Arrow Table.

        Warning: Loads entire stream into memory.

        Returns:
            Arrow Table with all stream data

        Example:
            table = stream.filter(lambda b: b.column('value') > 100).collect()
        """
        batches = list(self._execute_iterator())
        if not batches:
            return pa.Table.from_batches([], schema=self.schema)
        return pa.Table.from_batches(batches)

    def take(self, n: int) -> list:
        """
        Execute stream and take first n batches.

        Args:
            n: Number of batches to take

        Returns:
            List of up to n RecordBatches
        """
        result = []
        for i, batch in enumerate(self._execute_iterator()):
            if i >= n:
                break
            result.append(batch)
        return result

    def for_each(self, func: Callable[[pa.RecordBatch], None]) -> None:
        """
        Execute stream and apply function to each batch (side effects).

        Args:
            func: Function to apply to each batch

        Example:
            stream.for_each(lambda batch: print(f"Processed {batch.num_rows} rows"))
        """
        for batch in self._execute_iterator():
            func(batch)

    def _execute_iterator(self) -> Iterator[pa.RecordBatch]:
        """
        Execute the stream pipeline and yield batches.

        Applies all operators in sequence while maintaining zero-copy semantics.
        """
        # Get source iterator
        if callable(self._source):
            source_iter = self._source()
        elif hasattr(self._source, '__iter__'):
            source_iter = iter(self._source)
        else:
            raise ValueError(f"Invalid source type: {type(self._source)}")

        # Apply operators in sequence
        current_iter = source_iter

        for op_type, op_func in self._operators:
            if op_type == 'map':
                current_iter = self._apply_map(current_iter, op_func)
            elif op_type == 'filter':
                current_iter = self._apply_filter(current_iter, op_func)
            elif op_type == 'flat_map':
                current_iter = self._apply_flat_map(current_iter, op_func)

        # Yield results
        for batch in current_iter:
            yield batch

    @staticmethod
    def _apply_map(iterator: Iterator, func: Callable) -> Iterator:
        """Apply map transformation."""
        for batch in iterator:
            result = func(batch)
            if result is not None:
                yield result

    @staticmethod
    def _apply_filter(iterator: Iterator, predicate: Callable) -> Iterator:
        """Apply filter transformation using Arrow compute."""
        from sabot.cyarrow import compute as pc

        for batch in iterator:
            mask = predicate(batch)
            if isinstance(mask, pa.Array):
                # Filter using Arrow compute (zero-copy where possible)
                filtered = batch.filter(mask)
                if filtered.num_rows > 0:
                    yield filtered
            elif mask:  # Boolean result for entire batch
                yield batch

    @staticmethod
    def _apply_flat_map(iterator: Iterator, func: Callable) -> Iterator:
        """Apply flat_map transformation."""
        for batch in iterator:
            for result_batch in func(batch):
                if result_batch is not None:
                    yield result_batch


class KeyedStream:
    """
    Partitioned stream for stateful operations.

    Maintains state per key and enables reduce, fold, etc.
    """

    def __init__(self, stream: Stream, key_func: Union[str, Callable]):
        self.stream = stream
        self.key_func = key_func
        self._state_backend = None

    def with_state(self, state_path: str) -> 'KeyedStream':
        """
        Configure state backend for this keyed stream.

        Args:
            state_path: Path for state storage

        Returns:
            Self for chaining
        """
        try:
            from .._cython.tonbo_store import TonboStateBackend
            self._state_backend = TonboStateBackend(state_path, "keyed_stream")
        except ImportError:
            # Fall back to in-memory state
            self._state_backend = {}

        return self

    def reduce(self, func: Callable[[pa.RecordBatch, pa.RecordBatch], pa.RecordBatch]) -> Stream:
        """
        Reduce batches per key using accumulator function.

        Args:
            func: Reduction function (accumulator, new_value) -> accumulator

        Returns:
            Stream with reduced values
        """
        # TODO: Implement with state backend
        raise NotImplementedError("KeyedStream.reduce() coming soon")

    def aggregate(self, **agg_funcs) -> 'AggregatedStream':
        """
        Aggregate per key.

        Args:
            **agg_funcs: Aggregation functions

        Returns:
            AggregatedStream
        """
        return AggregatedStream(self.stream, agg_funcs, key_func=self.key_func)


class WindowedStream:
    """
    Windowed stream for time-based aggregations.
    """

    def __init__(self, stream: Stream, window_spec):
        self.stream = stream
        self.window_spec = window_spec

    def aggregate(self, **agg_funcs) -> 'AggregatedStream':
        """
        Aggregate within windows.

        Args:
            **agg_funcs: Aggregation functions

        Returns:
            AggregatedStream with windowed results
        """
        return AggregatedStream(
            self.stream,
            agg_funcs,
            window_spec=self.window_spec
        )


class AggregatedStream:
    """
    Stream with aggregation applied.

    Provides access to aggregated results.
    """

    def __init__(self, stream: Stream, agg_funcs: Dict[str, str],
                 key_func=None, window_spec=None):
        self.stream = stream
        self.agg_funcs = agg_funcs
        self.key_func = key_func
        self.window_spec = window_spec

    def collect(self) -> pa.Table:
        """
        Execute aggregation and collect results.

        Returns:
            Arrow Table with aggregated data
        """
        # Import zero-copy operators
        try:
            from ..core import _ops
        except (ImportError, AttributeError):
            # Fallback if Cython modules not compiled or old sabot code has issues
            _ops = None

        # Collect all batches
        batches = list(self.stream._execute_iterator())
        if not batches:
            return pa.Table.from_batches([], schema=self.stream.schema)

        # Perform aggregation
        results = {}

        for agg_name, col_spec in self.agg_funcs.items():
            if col_spec == '*':
                # Count all rows
                results[agg_name] = sum(batch.num_rows for batch in batches)
            elif agg_name.startswith('sum'):
                # Sum column using zero-copy operator
                col_name = col_spec
                total = 0
                for batch in batches:
                    if _ops and batch.num_rows > 0:
                        # Use zero-copy Cython operator
                        total += _ops.sum_batch_column(batch, col_name)
                    else:
                        # Fallback to Arrow compute
                        from sabot.cyarrow import compute as pc
                        total += pc.sum(batch.column(col_name)).as_py()
                results[agg_name] = total
            elif agg_name.startswith('count'):
                # Count non-null values
                col_name = col_spec
                count = 0
                for batch in batches:
                    col = batch.column(col_name)
                    count += col.null_count
                results[agg_name] = count
            elif agg_name.startswith('avg') or agg_name.startswith('mean'):
                # Average
                col_name = col_spec
                total = 0
                count = 0
                for batch in batches:
                    if _ops and batch.num_rows > 0:
                        total += _ops.sum_batch_column(batch, col_name)
                    else:
                        from sabot.cyarrow import compute as pc
                        total += pc.sum(batch.column(col_name)).as_py()
                    count += batch.num_rows
                results[agg_name] = total / count if count > 0 else 0

        # Return as single-row table (wrap scalars in lists)
        results_wrapped = {k: [v] for k, v in results.items()}
        return pa.Table.from_pydict(results_wrapped)


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
            for batch in self.stream._execute_iterator():
                self.sink.write(batch)

    def stop(self):
        """Stop streaming output."""
        if self.server:
            self.server.shutdown()
        elif self.sink:
            self.sink.close()
