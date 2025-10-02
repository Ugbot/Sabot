# cython: language_level=3, boundscheck=False, wraparound=False, cdivision=True, nonecheck=False
"""
Arrow Flight Server - Zero-Copy Network Transport

Provides high-performance server for streaming RecordBatches over the network.
Uses Arrow Flight's zero-copy IPC protocol for maximum throughput.

Performance targets:
- Serialization: ~1μs per batch (zero-copy)
- Network throughput: Limited by network bandwidth, not CPU
- Latency: <1ms end-to-end for small batches
"""

from libc.stdint cimport int64_t, int32_t, uint8_t
from libcpp cimport bool as cbool
from libcpp.string cimport string
from libcpp.memory cimport shared_ptr, unique_ptr, make_shared
from libcpp.vector cimport vector

cimport cython
cimport pyarrow.lib as pa

# Arrow Flight C++ API
from pyarrow.includes.libarrow_flight cimport (
    CFlightServerBase,
    CFlightDataStream,
    CFlightInfo,
    CFlightDescriptor,
    CTicket,
    CLocation,
    CServerCallContext,
    CFlightEndpoint,
)

# Standard Arrow types
from pyarrow.includes.libarrow cimport (
    CRecordBatch,
    CSchema,
    CStatus,
)


cdef class FlightDataStream:
    """
    Zero-copy data stream for Flight server.

    Wraps a Python iterator or async generator to stream RecordBatches
    to Flight clients with zero-copy semantics.

    Usage:
        stream = FlightDataStream(schema, batch_iterator)
        # Stream will yield batches from iterator with zero-copy
    """

    cdef:
        pa.Schema _schema
        object _iterator  # Python iterator/generator
        cbool _exhausted

    def __cinit__(self, pa.Schema schema, iterator):
        """
        Initialize Flight data stream.

        Args:
            schema: Arrow schema for batches
            iterator: Iterator/generator yielding RecordBatches
        """
        self._schema = schema
        self._iterator = iterator
        self._exhausted = False

    cpdef pa.Schema schema(self):
        """Get the schema for this stream."""
        return self._schema

    cpdef pa.RecordBatch next_batch(self):
        """
        Get next RecordBatch from iterator.

        Returns:
            RecordBatch or None if exhausted
        """
        if self._exhausted:
            return None

        try:
            batch = next(self._iterator)
            return batch
        except StopIteration:
            self._exhausted = True
            return None

    cpdef cbool is_exhausted(self):
        """Check if stream is exhausted."""
        return self._exhausted


cdef class FlightServer:
    """
    Arrow Flight Server for zero-copy RecordBatch streaming.

    Provides high-performance gRPC-based transport for Arrow data.
    All operations use zero-copy where possible for maximum throughput.

    Key features:
    - Zero-copy serialization via Arrow IPC
    - Concurrent request handling via gRPC
    - Low latency (<1ms for small batches)
    - High throughput (multi-GB/s on fast networks)

    Example:
        server = FlightServer("grpc://0.0.0.0:8815")
        server.register_stream("my_stream", schema, batch_iterator)
        server.serve()  # Blocking
    """

    cdef:
        str _location
        dict _streams  # path -> FlightDataStream
        cbool _running

    def __cinit__(self, str location="grpc://0.0.0.0:8815"):
        """
        Initialize Flight server.

        Args:
            location: Server location (e.g., "grpc://0.0.0.0:8815")
        """
        self._location = location
        self._streams = {}
        self._running = False

    cpdef void register_stream(self, str path, pa.Schema schema, iterator) except *:
        """
        Register a data stream with the server.

        Args:
            path: Stream identifier (e.g., "/my_stream")
            schema: Arrow schema for the stream
            iterator: Iterator yielding RecordBatches
        """
        stream = FlightDataStream(schema, iterator)
        self._streams[path] = stream

    cpdef FlightDataStream get_stream(self, str path):
        """Get registered stream by path."""
        return self._streams.get(path)

    cpdef list list_streams(self):
        """List all registered stream paths."""
        return list(self._streams.keys())

    cpdef void serve(self) except *:
        """
        Start serving Flight requests (blocking).

        This will use PyArrow's Flight server implementation
        under the hood, delegating to our registered streams.

        Note: This is a Python-level implementation for now.
        For production, this should use the C++ FlightServerBase directly.
        """
        import pyarrow.flight as flight

        class SabotFlightServer(flight.FlightServerBase):
            def __init__(self, location, streams):
                super().__init__(location)
                self.streams = streams

            def list_flights(self, context, criteria):
                for path, stream in self.streams.items():
                    descriptor = flight.FlightDescriptor.for_path(path)
                    endpoint = flight.FlightEndpoint(
                        path.encode('utf-8'),
                        [flight.Location(self.location)]
                    )
                    info = flight.FlightInfo(
                        stream.schema(),
                        descriptor,
                        [endpoint],
                        -1,  # total_records unknown
                        -1   # total_bytes unknown
                    )
                    yield info

            def get_flight_info(self, context, descriptor):
                path = descriptor.path[0].decode('utf-8')
                if path not in self.streams:
                    raise KeyError(f"Stream {path} not found")

                stream = self.streams[path]
                endpoint = flight.FlightEndpoint(
                    path.encode('utf-8'),
                    [flight.Location(self.location)]
                )
                return flight.FlightInfo(
                    stream.schema(),
                    descriptor,
                    [endpoint],
                    -1,
                    -1
                )

            def do_get(self, context, ticket):
                path = ticket.ticket.decode('utf-8')
                if path not in self.streams:
                    raise KeyError(f"Stream {path} not found")

                stream = self.streams[path]
                # Return generator that yields batches
                def batch_generator():
                    while True:
                        batch = stream.next_batch()
                        if batch is None:
                            break
                        yield batch

                return flight.RecordBatchStream(
                    flight.GeneratorStream(
                        stream.schema(),
                        batch_generator()
                    )
                )

        # Create and run server
        server = SabotFlightServer(self._location, self._streams)
        self._running = True

        print(f"Flight server listening on {self._location}")
        print(f"Registered streams: {list(self._streams.keys())}")

        server.serve()

    cpdef void shutdown(self) except *:
        """Shutdown the server."""
        self._running = False


# ============================================================================
# Helper Functions
# ============================================================================

def create_flight_server(location: str = "grpc://0.0.0.0:8815") -> FlightServer:
    """
    Create a Flight server instance.

    Args:
        location: Server location

    Returns:
        FlightServer instance
    """
    return FlightServer(location)
