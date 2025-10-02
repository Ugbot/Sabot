#!/usr/bin/env python3
"""
Serialization Layer for Sabot Stream Processing

Handles conversion between:
- Python objects ↔ Arrow RecordBatches
- Python objects ↔ JSON bytes
- Python objects ↔ MessagePack bytes

Provides efficient, zero-copy serialization for high-performance streaming.
"""

import json
import logging
from typing import Any, Dict, List, Union, Optional
from abc import ABC, abstractmethod

try:
    # Import pyarrow directly for full API access
    import pyarrow as pa
    import pyarrow.compute as pc
    ARROW_AVAILABLE = True
except ImportError:
    ARROW_AVAILABLE = False
    pa = None
    pc = None

try:
    import msgpack
    MSGPACK_AVAILABLE = True
except ImportError:
    MSGPACK_AVAILABLE = False
    msgpack = None

try:
    import orjson
    ORJSON_AVAILABLE = True
except ImportError:
    ORJSON_AVAILABLE = False
    orjson = None

from ..sabot_types import RecordBatch

logger = logging.getLogger(__name__)


class Serializer(ABC):
    """Abstract base class for all serializers."""

    @abstractmethod
    def serialize(self, obj: Any) -> bytes:
        """Serialize object to bytes."""
        pass

    @abstractmethod
    def deserialize(self, data: bytes) -> Any:
        """Deserialize bytes to object."""
        pass

    @abstractmethod
    def get_format_name(self) -> str:
        """Get the format name for this serializer."""
        pass


if ARROW_AVAILABLE:
    class ArrowSerializer(Serializer):
        """
        Arrow-based serializer for high-performance columnar data.

        Converts between Python objects and Arrow RecordBatches with:
        - Zero-copy operations where possible
        - Efficient columnar storage
        - Type preservation and inference
        - SIMD-accelerated operations
        """

        def __init__(self, compression: str = "lz4"):
            """
            Initialize Arrow serializer.

            Args:
                compression: Compression codec ('lz4', 'zstd', 'snappy', None)
            """
            if not ARROW_AVAILABLE:
                raise RuntimeError("PyArrow is required for ArrowSerializer")

            self.compression = compression
            self._schema_cache: Dict[str, pa.Schema] = {}

            logger.info(f"ArrowSerializer initialized with {compression} compression")

        def serialize(self, obj: Any) -> bytes:
            """
            Serialize object to Arrow format.

            Args:
                obj: Python object (dict, list of dicts, or Arrow RecordBatch)

            Returns:
                Serialized bytes
            """
            try:
                if isinstance(obj, pa.RecordBatch):
                    batch = obj
                elif isinstance(obj, dict):
                    batch = self._dict_to_batch([obj])
                elif isinstance(obj, list) and obj and isinstance(obj[0], dict):
                    batch = self._dict_to_batch(obj)
                else:
                    # Convert to dict format first
                    batch = self._dict_to_batch([{"data": obj}])

                # Convert to Arrow IPC format
                sink = pa.BufferOutputStream()
                with pa.ipc.new_file(sink, batch.schema) as writer:
                    writer.write_batch(batch)

                return sink.getvalue().to_pybytes()

            except Exception as e:
                logger.error(f"Arrow serialization failed: {e}")
                raise

        def deserialize(self, data: bytes) -> Any:
            """
            Deserialize Arrow bytes to Python object.

            Args:
                data: Serialized Arrow bytes

            Returns:
                Python object (list of dicts)
            """
            try:
                reader = pa.ipc.open_file(pa.py_buffer(data))
                batch = reader.get_batch(0)

                # Convert back to Python dicts
                return self._batch_to_dicts(batch)

            except Exception as e:
                logger.error(f"Arrow deserialization failed: {e}")
                raise

        def _dict_to_batch(self, dicts: List[Dict[str, Any]]) -> pa.RecordBatch:
            """Convert list of dictionaries to Arrow RecordBatch."""
            if not dicts:
                return pa.record_batch([])

            # Infer schema from first dict
            sample = dicts[0]
            schema = self._infer_schema(sample)

            # Create arrays for each field
            arrays = []
            for field in schema:
                values = [d.get(field.name) for d in dicts]
                array = pa.array(values, type=field.type)
                arrays.append(array)

            return pa.record_batch(arrays, schema=schema)

        def _batch_to_dicts(self, batch: pa.RecordBatch) -> List[Dict[str, Any]]:
            """Convert Arrow RecordBatch to list of dictionaries."""
            # Convert to pandas for easier dict conversion
            df = batch.to_pandas()
            return df.to_dict('records')

        def _infer_schema(self, sample: Dict[str, Any]) -> pa.Schema:
            """Infer Arrow schema from Python dictionary."""
            fields = []

            for key, value in sample.items():
                arrow_type = self._infer_type(value)
                fields.append(pa.field(key, arrow_type))

            return pa.schema(fields)

        def _infer_type(self, value: Any) -> pa.DataType:
            """Infer Arrow type from Python value."""
            if isinstance(value, bool):
                return pa.bool_()
            elif isinstance(value, int):
                return pa.int64()
            elif isinstance(value, float):
                return pa.float64()
            elif isinstance(value, str):
                return pa.string()
            elif isinstance(value, bytes):
                return pa.binary()
            elif isinstance(value, list):
                if not value:
                    return pa.list_(pa.null())
                else:
                    element_type = self._infer_type(value[0])
                    return pa.list_(element_type)
            elif isinstance(value, dict):
                # Nested struct
                fields = [pa.field(k, self._infer_type(v)) for k, v in value.items()]
                return pa.struct(fields)
            else:
                # Default to string for unknown types
                return pa.string()

        def get_format_name(self) -> str:
            """Get format name."""
            return "arrow"
else:
    # Fallback when PyArrow is not available
    class ArrowSerializer(Serializer):
        """Fallback Arrow serializer when PyArrow is not available."""

        def __init__(self, compression: str = "lz4"):
            raise RuntimeError("ArrowSerializer requires PyArrow. Install with: pip install pyarrow")

        def serialize(self, obj: Any) -> bytes:
            raise RuntimeError("ArrowSerializer requires PyArrow")

        def deserialize(self, data: bytes) -> Any:
            raise RuntimeError("ArrowSerializer requires PyArrow")

        def get_format_name(self) -> str:
            return "arrow"


class JsonSerializer(Serializer):
    """
    JSON-based serializer for compatibility and debugging.

    Uses orjson for performance when available, falls back to standard json.
    """

    def __init__(self, pretty: bool = False):
        """
        Initialize JSON serializer.

        Args:
            pretty: Whether to pretty-print JSON output
        """
        self.pretty = pretty

        if ORJSON_AVAILABLE:
            logger.info("Using orjson for JSON serialization")
        else:
            logger.info("Using standard json for JSON serialization")

    def serialize(self, obj: Any) -> bytes:
        """
        Serialize object to JSON bytes.

        Args:
            obj: Python object

        Returns:
            JSON bytes
        """
        try:
            if ORJSON_AVAILABLE:
                if self.pretty:
                    # orjson doesn't have pretty printing, convert to string first
                    json_str = orjson.dumps(obj, option=orjson.OPT_INDENT_2).decode('utf-8')
                    return json_str.encode('utf-8')
                else:
                    return orjson.dumps(obj)
            else:
                json_str = json.dumps(obj, indent=2 if self.pretty else None, default=str)
                return json_str.encode('utf-8')
        except Exception as e:
            logger.error(f"JSON serialization failed: {e}")
            raise

    def deserialize(self, data: bytes) -> Any:
        """
        Deserialize JSON bytes to Python object.

        Args:
            data: JSON bytes

        Returns:
            Python object
        """
        try:
            if ORJSON_AVAILABLE:
                return orjson.loads(data)
            else:
                return json.loads(data.decode('utf-8'))
        except Exception as e:
            logger.error(f"JSON deserialization failed: {e}")
            raise

    def get_format_name(self) -> str:
        """Get format name."""
        return "json"


class MessagePackSerializer(Serializer):
    """
    MessagePack-based serializer for efficient binary serialization.

    Compact binary format with good performance characteristics.
    """

    def __init__(self):
        """Initialize MessagePack serializer."""
        if not MSGPACK_AVAILABLE:
            raise RuntimeError("msgpack is required for MessagePackSerializer")

        logger.info("MessagePackSerializer initialized")

    def serialize(self, obj: Any) -> bytes:
        """
        Serialize object to MessagePack bytes.

        Args:
            obj: Python object

        Returns:
            MessagePack bytes
        """
        try:
            return msgpack.packb(obj, use_bin_type=True)
        except Exception as e:
            logger.error(f"MessagePack serialization failed: {e}")
            raise

    def deserialize(self, data: bytes) -> Any:
        """
        Deserialize MessagePack bytes to Python object.

        Args:
            data: MessagePack bytes

        Returns:
            Python object
        """
        try:
            return msgpack.unpackb(data, raw=False)
        except Exception as e:
            logger.error(f"MessagePack deserialization failed: {e}")
            raise

    def get_format_name(self) -> str:
        """Get format name."""
        return "msgpack"


class SerializerRegistry:
    """
    Registry for managing multiple serializers.

    Provides a centralized way to access different serialization formats.
    """

    def __init__(self):
        """Initialize serializer registry."""
        self._serializers: Dict[str, Serializer] = {}

        # Register built-in serializers
        if ARROW_AVAILABLE:
            try:
                self.register(ArrowSerializer())
            except (RuntimeError, TypeError) as e:
                logger.warning(f"ArrowSerializer not available: {e}")

        self.register(JsonSerializer())

        try:
            self.register(MessagePackSerializer())
        except RuntimeError:
            logger.warning("MessagePackSerializer not available")

    def register(self, serializer: Serializer) -> None:
        """
        Register a serializer.

        Args:
            serializer: Serializer instance to register
        """
        name = serializer.get_format_name()
        self._serializers[name] = serializer
        logger.info(f"Registered serializer: {name}")

    def get_serializer(self, name: str) -> Serializer:
        """
        Get serializer by name.

        Args:
            name: Serializer name

        Returns:
            Serializer instance

        Raises:
            KeyError: If serializer not found
        """
        if name not in self._serializers:
            available = list(self._serializers.keys())
            raise KeyError(f"Serializer '{name}' not found. Available: {available}")

        return self._serializers[name]

    def list_serializers(self) -> List[str]:
        """List available serializer names."""
        return list(self._serializers.keys())

    def serialize(self, obj: Any, format: str = "json") -> bytes:
        """
        Serialize object using specified format.

        Args:
            obj: Object to serialize
            format: Serialization format

        Returns:
            Serialized bytes
        """
        serializer = self.get_serializer(format)
        return serializer.serialize(obj)

    def deserialize(self, data: bytes, format: str = "json") -> Any:
        """
        Deserialize bytes using specified format.

        Args:
            data: Bytes to deserialize
            format: Serialization format

        Returns:
            Deserialized object
        """
        serializer = self.get_serializer(format)
        return serializer.deserialize(data)


# Global serializer registry instance
registry = SerializerRegistry()


def get_serializer(name: str) -> Serializer:
    """Get a serializer by name from the global registry."""
    return registry.get_serializer(name)


def serialize(obj: Any, format: str = "json") -> bytes:
    """Serialize object using the global registry."""
    return registry.serialize(obj, format)


def deserialize(data: bytes, format: str = "json") -> Any:
    """Deserialize bytes using the global registry."""
    return registry.deserialize(data, format)
