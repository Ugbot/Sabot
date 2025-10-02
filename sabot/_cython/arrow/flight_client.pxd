# -*- coding: utf-8 -*-
"""
Arrow Flight Client Cython Header

Header declarations for ArrowFlightClient.
"""

from libc.stdint cimport int32_t


cdef class ArrowFlightClient:
    cdef:
        str host
        int32_t port
        str scheme
        dict headers
        object client

    # Connection management
    cpdef void connect(self) except *
    cpdef void close(self)
    cpdef bint is_connected(self)
    cpdef void set_auth_headers(self, dict headers)

    # Data transfer
    cpdef object put_batch(self, str flight_path, object batch,
                          dict metadata=?) except *
    cpdef object get_batch(self, object ticket) except *
    cpdef object stream_batches(self, str flight_path, object batch_iterator,
                               int32_t chunk_size=?) except *

    # Flight management
    cpdef object list_flights(self, str pattern=?)
    cpdef void delete_flight(self, str flight_path) except *
    cpdef object get_flight_info(self, str flight_path)

    # Distributed operations
    cpdef object exchange_data(self, str flight_path, object local_batches,
                              str remote_host, int32_t remote_port) except *

    # Monitoring
    cpdef object get_stats(self)
