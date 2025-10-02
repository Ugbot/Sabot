# -*- coding: utf-8 -*-
"""
Unified Arrow API - Internal Cython Implementation

This module provides a PyArrow-compatible API using Sabot's internal Cython
implementations. Falls back to external PyArrow if needed for compatibility.

Usage:
    from sabot import arrow as pa  # Use internal implementation
    # or
    import sabot.arrow as pa  # Same thing
"""

import logging

logger = logging.getLogger(__name__)

# Try to use internal Cython implementation first
USING_INTERNAL = False
USING_EXTERNAL = False

try:
    from ._cython.arrow_core_simple import (
        Table,
        RecordBatch,
        Array,
        Schema,
        Field,
        create_table,
        create_record_batch,
    )
    USING_INTERNAL = True
    logger.info("Using internal Cython Arrow implementation")
except ImportError as e:
    logger.debug(f"Internal Arrow implementation not available: {e}")

    # Fall back to external pyarrow if available
    try:
        import pyarrow as _pa

        # Re-export pyarrow types
        Table = _pa.Table
        RecordBatch = _pa.RecordBatch
        Array = _pa.Array
        Schema = _pa.Schema
        Field = _pa.Field

        # Helper functions
        def create_table(*args, **kwargs):
            return _pa.table(*args, **kwargs)

        def create_record_batch(*args, **kwargs):
            return _pa.record_batch(*args, **kwargs)

        USING_EXTERNAL = True
        logger.info("Using external PyArrow")

    except ImportError:
        logger.warning("Neither internal nor external Arrow implementation available")

        # Provide stub classes for development
        class Table:
            """Stub Table class."""
            pass

        class RecordBatch:
            """Stub RecordBatch class."""
            pass

        class Array:
            """Stub Array class."""
            pass

        class Schema:
            """Stub Schema class."""
            pass

        class Field:
            """Stub Field class."""
            pass

        def create_table(*args, **kwargs):
            raise NotImplementedError("Arrow not available. Install: pip install -e .")

        def create_record_batch(*args, **kwargs):
            raise NotImplementedError("Arrow not available. Install: pip install -e .")


# Additional compute functions (use internal or fall back)
try:
    if USING_INTERNAL:
        from ._cython.arrow_core_simple import compute
    elif USING_EXTERNAL:
        import pyarrow.compute as compute
    else:
        compute = None
except ImportError:
    compute = None


# Export full PyArrow-compatible API
if USING_EXTERNAL:
    import pyarrow as _pa

    # Type constructors
    bool_ = _pa.bool_
    int8 = _pa.int8
    int16 = _pa.int16
    int32 = _pa.int32
    int64 = _pa.int64
    uint8 = _pa.uint8
    uint16 = _pa.uint16
    uint32 = _pa.uint32
    uint64 = _pa.uint64
    float16 = _pa.float16
    float32 = _pa.float32
    float64 = _pa.float64
    string = _pa.string
    binary = _pa.binary
    large_string = _pa.large_string
    large_binary = _pa.large_binary
    utf8 = _pa.utf8
    null = _pa.null
    timestamp = _pa.timestamp
    date32 = _pa.date32
    date64 = _pa.date64
    time32 = _pa.time32
    time64 = _pa.time64
    duration = _pa.duration
    list_ = _pa.list_
    large_list = _pa.large_list
    struct = _pa.struct
    dictionary = _pa.dictionary

    # Factory functions
    field = _pa.field
    schema = _pa.schema
    array = _pa.array
    record_batch = _pa.record_batch
    table = _pa.table

    # IPC and buffers
    ipc = _pa.ipc
    BufferOutputStream = _pa.BufferOutputStream
    py_buffer = _pa.py_buffer

    # Type class
    DataType = _pa.DataType

    # Cast
    cast = compute.cast if compute else None

elif USING_INTERNAL:
    # Use internal implementations
    # For now, these are stubs that will be implemented in Cython
    def bool_():
        """Return bool type."""
        raise NotImplementedError("Type constructors not yet in internal Arrow")

    def int64():
        """Return int64 type."""
        raise NotImplementedError("Type constructors not yet in internal Arrow")

    def float64():
        """Return float64 type."""
        raise NotImplementedError("Type constructors not yet in internal Arrow")

    def string():
        """Return string type."""
        raise NotImplementedError("Type constructors not yet in internal Arrow")

    def binary():
        """Return binary type."""
        raise NotImplementedError("Type constructors not yet in internal Arrow")

    def null():
        """Return null type."""
        raise NotImplementedError("Type constructors not yet in internal Arrow")

    def list_(value_type):
        """Return list type."""
        raise NotImplementedError("Type constructors not yet in internal Arrow")

    def struct(fields):
        """Return struct type."""
        raise NotImplementedError("Type constructors not yet in internal Arrow")

    def field(name, type):
        """Create field."""
        return Field(name, type)

    def schema(fields):
        """Create schema."""
        return Schema(fields)

    def array(data, type=None):
        """Create array."""
        raise NotImplementedError("array() not yet in internal Arrow")

    def record_batch(data, schema=None):
        """Create record batch."""
        return create_record_batch(data, schema)

    def table(data, schema=None):
        """Create table."""
        return create_table(data, schema)

    class DataType:
        """Stub DataType class."""
        pass

    # IPC stubs
    class _IPCStub:
        def new_file(self, *args, **kwargs):
            raise NotImplementedError("IPC not yet in internal Arrow")
        def open_file(self, *args, **kwargs):
            raise NotImplementedError("IPC not yet in internal Arrow")

    ipc = _IPCStub()

    def BufferOutputStream():
        raise NotImplementedError("BufferOutputStream not yet in internal Arrow")

    def py_buffer(data):
        raise NotImplementedError("py_buffer not yet in internal Arrow")

    cast = None

else:
    # No Arrow available - provide stubs
    def bool_():
        raise NotImplementedError("Arrow not available")

    def int64():
        raise NotImplementedError("Arrow not available")

    def float64():
        raise NotImplementedError("Arrow not available")

    def string():
        raise NotImplementedError("Arrow not available")

    def binary():
        raise NotImplementedError("Arrow not available")

    def null():
        raise NotImplementedError("Arrow not available")

    def list_(value_type):
        raise NotImplementedError("Arrow not available")

    def struct(fields):
        raise NotImplementedError("Arrow not available")

    def field(name, type):
        raise NotImplementedError("Arrow not available")

    def schema(fields):
        raise NotImplementedError("Arrow not available")

    def array(data, type=None):
        raise NotImplementedError("Arrow not available")

    def record_batch(data, schema=None):
        raise NotImplementedError("Arrow not available")

    def table(data, schema=None):
        raise NotImplementedError("Arrow not available")

    class DataType:
        pass

    class _IPCStub:
        def new_file(self, *args, **kwargs):
            raise NotImplementedError("Arrow not available")
        def open_file(self, *args, **kwargs):
            raise NotImplementedError("Arrow not available")

    ipc = _IPCStub()

    def BufferOutputStream():
        raise NotImplementedError("Arrow not available")

    def py_buffer(data):
        raise NotImplementedError("Arrow not available")

    cast = None


__all__ = [
    'Table',
    'RecordBatch',
    'Array',
    'Schema',
    'Field',
    'create_table',
    'create_record_batch',
    'compute',
    'USING_INTERNAL',
    'USING_EXTERNAL',
    # Type constructors
    'bool_', 'int8', 'int16', 'int32', 'int64',
    'uint8', 'uint16', 'uint32', 'uint64',
    'float16', 'float32', 'float64',
    'string', 'binary', 'utf8', 'null',
    'list_', 'struct', 'field', 'schema',
    'timestamp', 'date32', 'date64',
    # Factory functions
    'array', 'record_batch', 'table',
    # IPC
    'ipc', 'BufferOutputStream', 'py_buffer',
    # Type class
    'DataType',
    # Cast
    'cast',
]
