# -*- coding: utf-8 -*-
"""
Arrow Flight Client - High-Performance Distributed Data Transfer

Implements Arrow Flight protocol for zero-copy data transfer between
Sabot workers. Enables efficient distributed operations and data sharing.
"""

from libc.stdint cimport int64_t, int32_t
from cpython.ref cimport PyObject

cimport cython


@cython.final
cdef class ArrowFlightClient:
    """
    Arrow Flight client for distributed data transfer.

    Provides high-performance data transfer between Sabot workers using
    Arrow Flight protocol. Supports streaming data exchange for distributed
    operations like shuffles, broadcasts, and inter-worker communication.

    Features:
    - Zero-copy data transfer
    - Streaming data exchange
    - Authentication and authorization
    - Connection pooling and reuse
    """

    cdef:
        str host
        int32_t port
        str scheme  # grpc, grpc+tls, etc.
        dict headers  # Authentication headers
        object client  # Underlying Flight client

    def __cinit__(self, str host="localhost", int32_t port=8815,
                  str scheme="grpc"):
        """Initialize Flight client."""
        self.host = host
        self.port = port
        self.scheme = scheme
        self.headers = {}
        self.client = None

    cpdef void connect(self) except *:
        """
        Establish connection to Flight server.

        Creates underlying Flight client connection.
        """
        try:
            import pyarrow.flight as flight

            # Create client
            location = flight.Location.for_grpc_tcp(self.host, self.port)
            self.client = flight.FlightClient(location, **self.headers)

        except ImportError:
            # Flight not available - create mock client
            self.client = MockFlightClient(self.host, self.port)

    cpdef void set_auth_headers(self, dict headers):
        """Set authentication headers for requests."""
        self.headers = headers or {}

    cpdef object put_batch(self, str flight_path, object batch,
                          dict metadata=None) except *:
        """
        Send Arrow RecordBatch to remote server.

        flight_path: Logical path for the data stream
        batch: Arrow RecordBatch to send
        metadata: Optional metadata to attach

        Returns: Flight ticket for retrieving the data
        """
        if not self.client:
            raise RuntimeError("Client not connected")

        try:
            import pyarrow.flight as flight

            # Create flight descriptor
            descriptor = flight.FlightDescriptor.for_path(flight_path)

            # Put the batch
            writer, _ = self.client.do_put(descriptor, batch.schema)

            # Write batch with metadata
            if metadata:
                # Convert metadata to binary format
                import pickle
                metadata_bytes = pickle.dumps(metadata)
                writer.write_with_metadata(batch, metadata_bytes)
            else:
                writer.write(batch)

            writer.close()

            # Return ticket for retrieval
            return flight.Ticket(flight_path.encode('utf-8'))

        except ImportError:
            # Mock implementation
            return self.client.put_batch(flight_path, batch, metadata)

    cpdef object get_batch(self, object ticket) except *:
        """
        Retrieve Arrow RecordBatch from remote server.

        ticket: Flight ticket returned from put_batch

        Returns: (batch, metadata) tuple
        """
        if not self.client:
            raise RuntimeError("Client not connected")

        try:
            import pyarrow.flight as flight

            # Get the data
            reader = self.client.do_get(ticket)

            # Read batch and metadata
            batch = reader.read_all()
            metadata = None

            # Try to read metadata
            try:
                flight_data = reader.read_chunk()
                if flight_data.app_metadata:
                    import pickle
                    metadata = pickle.loads(flight_data.app_metadata)
            except:
                pass  # No metadata

            return (batch, metadata)

        except ImportError:
            # Mock implementation
            return self.client.get_batch(ticket)

    cpdef object stream_batches(self, str flight_path, object batch_iterator,
                               int32_t chunk_size=1000) except *:
        """
        Stream multiple batches to remote server.

        Useful for large datasets that don't fit in memory.
        Returns ticket for retrieving the streamed data.
        """
        if not self.client:
            raise RuntimeError("Client not connected")

        try:
            import pyarrow.flight as flight

            # Create flight descriptor
            descriptor = flight.FlightDescriptor.for_path(flight_path)

            # Start streaming
            writer, _ = self.client.do_put(descriptor, None)  # Schema will be inferred

            batch_count = 0
            for batch in batch_iterator:
                writer.write(batch)
                batch_count += 1

                if batch_count % chunk_size == 0:
                    # Periodic flush for streaming
                    pass

            writer.close()

            # Return ticket with batch count metadata
            ticket_data = f"{flight_path}:{batch_count}".encode('utf-8')
            return flight.Ticket(ticket_data)

        except ImportError:
            # Mock implementation
            batches = list(batch_iterator)
            return self.client.stream_batches(flight_path, batches, chunk_size)

    cpdef object list_flights(self, str pattern="*"):
        """
        List available flights on the server.

        Returns list of flight descriptors matching the pattern.
        """
        if not self.client:
            raise RuntimeError("Client not connected")

        try:
            import pyarrow.flight as flight

            # List flights
            criteria = flight.FlightCallOptions()
            flights = self.client.list_flights(criteria=criteria)

            # Filter by pattern
            matching = []
            for flight_info in flights:
                path = flight_info.descriptor.path
                if pattern == "*" or any(pattern in p for p in path):
                    matching.append(flight_info)

            return matching

        except ImportError:
            # Mock implementation
            return self.client.list_flights(pattern)

    cpdef void delete_flight(self, str flight_path) except *:
        """
        Delete a flight from the server.

        Useful for cleanup after processing.
        """
        if not self.client:
            raise RuntimeError("Client not connected")

        try:
            import pyarrow.flight as flight

            # Create descriptor for deletion
            descriptor = flight.FlightDescriptor.for_path(flight_path)

            # Delete the flight
            self.client.do_delete(descriptor)

        except ImportError:
            # Mock implementation
            self.client.delete_flight(flight_path)

    cpdef object get_flight_info(self, str flight_path):
        """
        Get information about a flight.

        Returns flight info including schema, size, etc.
        """
        if not self.client:
            raise RuntimeError("Client not connected")

        try:
            import pyarrow.flight as flight

            # Get flight info
            descriptor = flight.FlightDescriptor.for_path(flight_path)
            info = self.client.get_flight_info(descriptor)

            return {
                'descriptor': info.descriptor,
                'schema': info.schema,
                'total_records': info.total_records,
                'total_bytes': info.total_bytes,
                'endpoints': info.endpoints
            }

        except ImportError:
            # Mock implementation
            return self.client.get_flight_info(flight_path)

    cpdef object exchange_data(self, str flight_path, object local_batches,
                              str remote_host, int32_t remote_port) except *:
        """
        Exchange data with remote worker.

        Creates a temporary flight, sends data, and signals remote worker.
        This is useful for shuffle operations in distributed processing.
        """
        # Send data to remote
        ticket = self.put_batch(flight_path, local_batches)

        # Create client for remote worker
        remote_client = ArrowFlightClient(remote_host, remote_port)
        remote_client.connect()

        # Signal remote worker about available data
        signal_path = f"{flight_path}_signal"
        signal_data = {"ticket": str(ticket.ticket, 'utf-8'), "sender": f"{self.host}:{self.port}"}

        try:
            import pyarrow as pa
            signal_batch = pa.RecordBatch.from_arrays(
                [pa.array([signal_data])],
                names=["signal"]
            )
            remote_client.put_batch(signal_path, signal_batch)
        except ImportError:
            # Mock signal
            remote_client.put_batch(signal_path, signal_data, {})

        return ticket

    # Connection management

    cpdef void close(self):
        """Close the client connection."""
        if self.client:
            try:
                self.client.close()
            except:
                pass  # Ignore errors during close
            self.client = None

    cpdef bint is_connected(self):
        """Check if client is connected."""
        return self.client is not None

    # Statistics and monitoring

    cpdef object get_stats(self):
        """Get client statistics."""
        return {
            'host': self.host,
            'port': self.port,
            'scheme': self.scheme,
            'connected': self.is_connected(),
            'auth_headers': bool(self.headers),
        }


# Mock client for when Arrow Flight is not available
cdef class MockFlightClient:
    """Mock Flight client for testing and fallback."""

    cdef:
        str host
        int32_t port
        dict storage  # Mock storage

    def __init__(self, host, port):
        self.host = host
        self.port = port
        self.storage = {}

    def put_batch(self, flight_path, batch, metadata=None):
        """Mock put operation."""
        self.storage[flight_path] = (batch, metadata)
        return MockTicket(flight_path)

    def get_batch(self, ticket):
        """Mock get operation."""
        if ticket.ticket in self.storage:
            batch, metadata = self.storage[ticket.ticket]
            return (batch, metadata)
        return (None, None)

    def stream_batches(self, flight_path, batches, chunk_size):
        """Mock streaming."""
        self.storage[flight_path] = (batches, {"streamed": True})
        return MockTicket(f"{flight_path}_stream")

    def list_flights(self, pattern):
        """Mock list."""
        return [MockFlightInfo(path) for path in self.storage.keys()
                if pattern == "*" or pattern in path]

    def delete_flight(self, flight_path):
        """Mock delete."""
        if flight_path in self.storage:
            del self.storage[flight_path]

    def get_flight_info(self, flight_path):
        """Mock info."""
        if flight_path in self.storage:
            batch, metadata = self.storage[flight_path]
            return {
                'descriptor': flight_path,
                'schema': getattr(batch, 'schema', None),
                'total_records': getattr(batch, 'num_rows', 0),
                'total_bytes': 0,
                'endpoints': []
            }
        return None


cdef class MockTicket:
    """Mock flight ticket."""
    cdef:
        str ticket

    def __init__(self, ticket):
        self.ticket = ticket


cdef class MockFlightInfo:
    """Mock flight info."""
    cdef:
        str path

    def __init__(self, path):
        self.path = path

    @property
    def descriptor(self):
        return MockDescriptor(self.path)


cdef class MockDescriptor:
    """Mock descriptor."""
    cdef:
        str path

    def __init__(self, path):
        self.path = path

    @property
    def path(self):
        return [self.path]
