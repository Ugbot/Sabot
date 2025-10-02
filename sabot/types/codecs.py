#!/usr/bin/env python3
"""
Codec types and definitions for Sabot channels.

Defines serialization/deserialization interfaces and types.
"""

from typing import Any, Union, Optional, Dict, List
from enum import Enum
from abc import ABC, abstractmethod


class CodecType(Enum):
    """Supported codec types."""
    JSON = "json"
    MSGPACK = "msgpack"
    ARROW = "arrow"
    AVRO = "avro"
    PROTOBUF = "protobuf"
    JSON_SCHEMA = "json_schema"
    STRING = "string"
    BYTES = "bytes"
    CUSTOM = "custom"


class CodecArg:
    """
    Codec argument specification.

    Defines how to configure and use different codecs for channel communication.
    """

    def __init__(self,
                 codec_type: CodecType,
                 options: Optional[Dict[str, Any]] = None):
        """
        Initialize codec argument.

        Args:
            codec_type: Type of codec to use
            options: Codec-specific options
        """
        self.codec_type = codec_type
        self.options = options or {}

    def __repr__(self) -> str:
        return f"CodecArg({self.codec_type.value}, {self.options})"


class Codec(ABC):
    """
    Abstract base class for all codecs.

    Codecs handle serialization and deserialization of messages
    for channel communication.
    """

    @abstractmethod
    def encode(self, message: Any) -> bytes:
        """
        Encode a message to bytes.

        Args:
            message: Message to encode

        Returns:
            Encoded bytes
        """
        pass

    @abstractmethod
    def decode(self, data: bytes) -> Any:
        """
        Decode bytes to a message.

        Args:
            data: Bytes to decode

        Returns:
            Decoded message
        """
        pass

    @property
    @abstractmethod
    def content_type(self) -> str:
        """
        Get the content type for this codec.

        Returns:
            MIME content type string
        """
        pass


class JSONCodec(Codec):
    """JSON codec for message serialization."""

    def __init__(self):
        """Initialize JSON codec."""
        try:
            import orjson
            self._dumps = orjson.dumps
            self._loads = orjson.loads
        except ImportError:
            import json
            self._dumps = json.dumps
            self._loads = json.loads

    def encode(self, message: Any) -> bytes:
        """Encode message to JSON bytes."""
        return self._dumps(message)

    def decode(self, data: bytes) -> Any:
        """Decode JSON bytes to message."""
        return self._loads(data)

    @property
    def content_type(self) -> str:
        return "application/json"


class MsgPackCodec(Codec):
    """MessagePack codec for efficient binary serialization."""

    def __init__(self):
        """Initialize MessagePack codec."""
        try:
            import msgpack
            self._packb = msgpack.packb
            self._unpackb = msgpack.unpackb
        except ImportError:
            raise RuntimeError("msgpack is required for MsgPackCodec")

    def encode(self, message: Any) -> bytes:
        """Encode message to MessagePack bytes."""
        return self._packb(message, use_bin_type=True)

    def decode(self, data: bytes) -> Any:
        """Decode MessagePack bytes to message."""
        return self._unpackb(data, raw=False)

    @property
    def content_type(self) -> str:
        return "application/msgpack"


class ArrowCodec(Codec):
    """Arrow codec for columnar data serialization."""

    def __init__(self, compression: str = "lz4"):
        """
        Initialize Arrow codec.

        Args:
            compression: Compression codec to use
        """
        try:
            import pyarrow as pa
            self._pa = pa
            self.compression = compression
        except ImportError:
            raise RuntimeError("pyarrow is required for ArrowCodec")

    def encode(self, message: Any) -> bytes:
        """Encode message to Arrow IPC format."""
        # Convert to RecordBatch if needed
        if isinstance(message, dict):
            # Convert dict to RecordBatch
            import pandas as pd
            df = pd.DataFrame([message])
            batch = self._pa.RecordBatch.from_pandas(df)
        elif isinstance(message, list) and message and isinstance(message[0], dict):
            # Convert list of dicts to RecordBatch
            import pandas as pd
            df = pd.DataFrame(message)
            batch = self._pa.RecordBatch.from_pandas(df)
        elif hasattr(message, 'to_pandas'):
            # Convert from pandas/polars
            batch = self._pa.RecordBatch.from_pandas(message.to_pandas())
        else:
            # Assume it's already a RecordBatch
            batch = message

        # Serialize to IPC format
        sink = self._pa.BufferOutputStream()
        with self._pa.ipc.new_file(sink, batch.schema) as writer:
            writer.write_batch(batch)

        return sink.getvalue().to_pybytes()

    def decode(self, data: bytes) -> Any:
        """Decode Arrow IPC bytes to RecordBatch."""
        reader = self._pa.ipc.open_file(self._pa.py_buffer(data))
        batch = reader.get_batch(0)

        # Convert to pandas for easier consumption
        return batch.to_pandas()

    @property
    def content_type(self) -> str:
        return "application/vnd.apache.arrow.file"


class AvroCodec(Codec):
    """
    Avro codec with Confluent Schema Registry integration.

    Implements Confluent wire format:
    [magic_byte(1)][schema_id(4)][avro_payload]

    Phase 1: Pure Python using avro-python3
    Phase 2: Will be Cythonized for performance
    """

    MAGIC_BYTE = 0

    def __init__(self, schema_registry_url: Optional[str] = None,
                 schema_str: Optional[str] = None, subject: Optional[str] = None):
        """
        Initialize Avro codec.

        Args:
            schema_registry_url: Schema Registry URL
            schema_str: Avro schema JSON string (if not using registry)
            subject: Subject name for schema registration
        """
        try:
            import avro.schema
            import avro.io
            import io as python_io
            self._avro_schema = avro.schema
            self._avro_io = avro.io
            self._python_io = python_io
        except ImportError:
            raise RuntimeError("avro is required for AvroCodec. Install with: pip install avro")

        self.schema_registry_url = schema_registry_url
        self.subject = subject
        self._schema = None
        self._schema_id = None
        self._registry_client = None

        # Initialize Schema Registry client if URL provided
        if schema_registry_url:
            from ..kafka.schema_registry import SchemaRegistryClient
            self._registry_client = SchemaRegistryClient(schema_registry_url)

            # Register or fetch schema
            if schema_str and subject:
                self._schema_id = self._registry_client.register_schema(subject, schema_str, "AVRO")
                self._schema = self._avro_schema.parse(schema_str)
            elif subject:
                # Fetch latest schema for subject
                registered = self._registry_client.get_latest_schema(subject)
                if registered:
                    self._schema_id = registered.schema_id
                    self._schema = self._avro_schema.parse(registered.schema_str)
        elif schema_str:
            # No registry, just use provided schema
            self._schema = self._avro_schema.parse(schema_str)

    def encode(self, message: Any) -> bytes:
        """
        Encode message to Avro format (Confluent wire format).

        Args:
            message: Python dict matching Avro schema

        Returns:
            Bytes in Confluent wire format
        """
        if not self._schema:
            raise RuntimeError("No Avro schema configured")

        # Serialize Avro payload
        writer = self._avro_io.DatumWriter(self._schema)
        bytes_writer = self._python_io.BytesIO()
        encoder = self._avro_io.BinaryEncoder(bytes_writer)
        writer.write(message, encoder)
        avro_payload = bytes_writer.getvalue()

        # If using Schema Registry, prepend magic byte + schema ID
        if self._schema_id is not None:
            import struct
            # Confluent wire format: [magic_byte(1)][schema_id(4)][payload]
            return struct.pack('>bI', self.MAGIC_BYTE, self._schema_id) + avro_payload
        else:
            # No registry, just return raw Avro
            return avro_payload

    def decode(self, data: bytes) -> Any:
        """
        Decode Avro bytes to Python dict.

        Args:
            data: Bytes in Confluent wire format or raw Avro

        Returns:
            Python dict
        """
        import struct

        # Check if this is Confluent wire format
        if len(data) >= 5 and data[0] == self.MAGIC_BYTE:
            # Parse magic byte + schema ID
            magic_byte, schema_id = struct.unpack('>bI', data[:5])
            avro_payload = data[5:]

            # Fetch schema if we don't have it
            if self._schema_id != schema_id:
                if not self._registry_client:
                    raise RuntimeError(f"Need Schema Registry to decode schema ID {schema_id}")

                registered = self._registry_client.get_schema_by_id(schema_id)
                if not registered:
                    raise RuntimeError(f"Schema ID {schema_id} not found in registry")

                self._schema = self._avro_schema.parse(registered.schema_str)
                self._schema_id = schema_id
        else:
            # Raw Avro (no wire format)
            avro_payload = data

        if not self._schema:
            raise RuntimeError("No Avro schema available for decoding")

        # Deserialize Avro payload
        reader = self._avro_io.DatumReader(self._schema)
        bytes_reader = self._python_io.BytesIO(avro_payload)
        decoder = self._avro_io.BinaryDecoder(bytes_reader)
        return reader.read(decoder)

    @property
    def content_type(self) -> str:
        return "application/vnd.confluent.avro"


class ProtobufCodec(Codec):
    """
    Protobuf codec with Confluent Schema Registry integration.

    Implements Confluent wire format:
    [magic_byte(1)][schema_id(4)][protobuf_payload]

    Uses google.protobuf which already has C++ backend.
    """

    MAGIC_BYTE = 0

    def __init__(self, schema_registry_url: Optional[str] = None,
                 message_class: Optional[type] = None, subject: Optional[str] = None):
        """
        Initialize Protobuf codec.

        Args:
            schema_registry_url: Schema Registry URL
            message_class: Protobuf message class (e.g., MyMessage)
            subject: Subject name for schema registration
        """
        try:
            from google.protobuf import message as pb_message
            self._pb_message = pb_message
        except ImportError:
            raise RuntimeError("protobuf is required for ProtobufCodec. Install with: pip install protobuf")

        self.schema_registry_url = schema_registry_url
        self.message_class = message_class
        self.subject = subject
        self._schema_id = None
        self._registry_client = None

        # Initialize Schema Registry client if URL provided
        if schema_registry_url:
            from ..kafka.schema_registry import SchemaRegistryClient
            self._registry_client = SchemaRegistryClient(schema_registry_url)

            # Register schema if subject provided
            if message_class and subject:
                # Get protobuf schema from message descriptor
                schema_str = str(message_class.DESCRIPTOR)
                self._schema_id = self._registry_client.register_schema(subject, schema_str, "PROTOBUF")

    def encode(self, message: Any) -> bytes:
        """
        Encode Protobuf message to bytes (Confluent wire format).

        Args:
            message: Protobuf message instance

        Returns:
            Bytes in Confluent wire format
        """
        if not isinstance(message, self._pb_message.Message):
            raise TypeError(f"Expected Protobuf message, got {type(message)}")

        # Serialize Protobuf (uses C++ backend)
        protobuf_payload = message.SerializeToString()

        # If using Schema Registry, prepend magic byte + schema ID
        if self._schema_id is not None:
            import struct
            return struct.pack('>bI', self.MAGIC_BYTE, self._schema_id) + protobuf_payload
        else:
            # No registry, just return raw Protobuf
            return protobuf_payload

    def decode(self, data: bytes) -> Any:
        """
        Decode Protobuf bytes to message instance.

        Args:
            data: Bytes in Confluent wire format or raw Protobuf

        Returns:
            Protobuf message instance
        """
        import struct

        # Check if this is Confluent wire format
        if len(data) >= 5 and data[0] == self.MAGIC_BYTE:
            # Parse magic byte + schema ID
            magic_byte, schema_id = struct.unpack('>bI', data[:5])
            protobuf_payload = data[5:]

            # Note: In full implementation, would fetch schema and dynamically
            # create message class. For now, use provided message_class.
            if self._schema_id != schema_id:
                # TODO: Fetch schema from registry and create dynamic message
                pass
        else:
            # Raw Protobuf (no wire format)
            protobuf_payload = data

        if not self.message_class:
            raise RuntimeError("No Protobuf message class configured")

        # Deserialize Protobuf (uses C++ backend)
        message = self.message_class()
        message.ParseFromString(protobuf_payload)
        return message

    @property
    def content_type(self) -> str:
        return "application/vnd.confluent.protobuf"


class JSONSchemaCodec(Codec):
    """
    JSON Schema codec with Confluent Schema Registry integration.

    Implements Confluent wire format:
    [magic_byte(1)][schema_id(4)][json_payload]

    Phase 1: Pure Python using jsonschema
    Phase 2: Will be optimized with Cython for validation performance
    """

    MAGIC_BYTE = 0

    def __init__(self, schema_registry_url: Optional[str] = None,
                 schema: Optional[Dict[str, Any]] = None, subject: Optional[str] = None):
        """
        Initialize JSON Schema codec.

        Args:
            schema_registry_url: Schema Registry URL
            schema: JSON Schema dict (if not using registry)
            subject: Subject name for schema registration
        """
        try:
            import jsonschema
            self._jsonschema = jsonschema
        except ImportError:
            raise RuntimeError("jsonschema is required for JSONSchemaCodec. Install with: pip install jsonschema")

        try:
            import orjson
            self._dumps = orjson.dumps
            self._loads = orjson.loads
        except ImportError:
            import json
            self._dumps = lambda obj: json.dumps(obj).encode('utf-8')
            self._loads = json.loads

        self.schema_registry_url = schema_registry_url
        self.subject = subject
        self._schema = schema
        self._schema_id = None
        self._registry_client = None
        self._validator = None

        # Initialize Schema Registry client if URL provided
        if schema_registry_url:
            from ..kafka.schema_registry import SchemaRegistryClient
            self._registry_client = SchemaRegistryClient(schema_registry_url)

            # Register or fetch schema
            if schema and subject:
                import json as stdlib_json
                schema_str = stdlib_json.dumps(schema)
                self._schema_id = self._registry_client.register_schema(subject, schema_str, "JSON")
                self._validator = self._jsonschema.Draft7Validator(schema)
            elif subject:
                # Fetch latest schema for subject
                registered = self._registry_client.get_latest_schema(subject)
                if registered:
                    import json as stdlib_json
                    self._schema_id = registered.schema_id
                    self._schema = stdlib_json.loads(registered.schema_str)
                    self._validator = self._jsonschema.Draft7Validator(self._schema)
        elif schema:
            # No registry, just use provided schema
            self._validator = self._jsonschema.Draft7Validator(schema)

    def encode(self, message: Any) -> bytes:
        """
        Encode message to JSON Schema format (Confluent wire format).

        Args:
            message: Python dict matching JSON Schema

        Returns:
            Bytes in Confluent wire format
        """
        if not self._schema:
            raise RuntimeError("No JSON Schema configured")

        # Validate against schema
        if self._validator:
            self._validator.validate(message)

        # Serialize JSON payload
        json_payload = self._dumps(message)

        # If using Schema Registry, prepend magic byte + schema ID
        if self._schema_id is not None:
            import struct
            # Confluent wire format: [magic_byte(1)][schema_id(4)][payload]
            return struct.pack('>bI', self.MAGIC_BYTE, self._schema_id) + json_payload
        else:
            # No registry, just return JSON
            return json_payload

    def decode(self, data: bytes) -> Any:
        """
        Decode JSON Schema bytes to Python dict.

        Args:
            data: Bytes in Confluent wire format or raw JSON

        Returns:
            Python dict
        """
        import struct

        # Check if this is Confluent wire format
        if len(data) >= 5 and data[0] == self.MAGIC_BYTE:
            # Parse magic byte + schema ID
            magic_byte, schema_id = struct.unpack('>bI', data[:5])
            json_payload = data[5:]

            # Fetch schema if we don't have it
            if self._schema_id != schema_id:
                if not self._registry_client:
                    raise RuntimeError(f"Need Schema Registry to decode schema ID {schema_id}")

                registered = self._registry_client.get_schema_by_id(schema_id)
                if not registered:
                    raise RuntimeError(f"Schema ID {schema_id} not found in registry")

                import json as stdlib_json
                self._schema = stdlib_json.loads(registered.schema_str)
                self._schema_id = schema_id
                self._validator = self._jsonschema.Draft7Validator(self._schema)
        else:
            # Raw JSON (no wire format)
            json_payload = data

        # Deserialize JSON
        message = self._loads(json_payload)

        # Validate against schema
        if self._validator:
            self._validator.validate(message)

        return message

    @property
    def content_type(self) -> str:
        return "application/vnd.confluent.json"


class StringCodec(Codec):
    """Plain UTF-8 string codec."""

    def encode(self, message: str) -> bytes:
        """Encode string to UTF-8 bytes."""
        if isinstance(message, bytes):
            return message
        return str(message).encode('utf-8')

    def decode(self, data: bytes) -> str:
        """Decode UTF-8 bytes to string."""
        return data.decode('utf-8')

    @property
    def content_type(self) -> str:
        return "text/plain; charset=utf-8"


class BytesCodec(Codec):
    """Raw bytes codec (no-op)."""

    def encode(self, message: bytes) -> bytes:
        """Return bytes as-is."""
        return bytes(message)

    def decode(self, data: bytes) -> bytes:
        """Return bytes as-is."""
        return data

    @property
    def content_type(self) -> str:
        return "application/octet-stream"


def create_codec(codec_arg: CodecArg) -> Codec:
    """
    Create a codec instance from CodecArg.

    Args:
        codec_arg: Codec argument specification

    Returns:
        Codec instance

    Raises:
        ValueError: If codec type is unsupported
    """
    codec_type = codec_arg.codec_type

    if codec_type == CodecType.JSON:
        return JSONCodec()
    elif codec_type == CodecType.MSGPACK:
        return MsgPackCodec()
    elif codec_type == CodecType.ARROW:
        compression = codec_arg.options.get('compression', 'lz4')
        return ArrowCodec(compression=compression)
    elif codec_type == CodecType.AVRO:
        schema_registry_url = codec_arg.options.get('schema_registry_url')
        schema_str = codec_arg.options.get('schema')
        subject = codec_arg.options.get('subject')
        return AvroCodec(
            schema_registry_url=schema_registry_url,
            schema_str=schema_str,
            subject=subject
        )
    elif codec_type == CodecType.PROTOBUF:
        schema_registry_url = codec_arg.options.get('schema_registry_url')
        message_class = codec_arg.options.get('message_class')
        subject = codec_arg.options.get('subject')
        return ProtobufCodec(
            schema_registry_url=schema_registry_url,
            message_class=message_class,
            subject=subject
        )
    elif codec_type == CodecType.JSON_SCHEMA:
        schema_registry_url = codec_arg.options.get('schema_registry_url')
        schema = codec_arg.options.get('schema')
        subject = codec_arg.options.get('subject')
        return JSONSchemaCodec(
            schema_registry_url=schema_registry_url,
            schema=schema,
            subject=subject
        )
    elif codec_type == CodecType.STRING:
        return StringCodec()
    elif codec_type == CodecType.BYTES:
        return BytesCodec()
    elif codec_type == CodecType.CUSTOM:
        # Custom codec would need to be specified in options
        custom_class = codec_arg.options.get('codec_class')
        if not custom_class:
            raise ValueError("Custom codec requires 'codec_class' in options")
        return custom_class(**codec_arg.options.get('codec_kwargs', {}))
    else:
        raise ValueError(f"Unsupported codec type: {codec_type}")


# Default codec instances (conditional on availability)
DEFAULT_JSON_CODEC = JSONCodec()

try:
    DEFAULT_MSGPACK_CODEC = MsgPackCodec()
except RuntimeError:
    DEFAULT_MSGPACK_CODEC = None

try:
    DEFAULT_ARROW_CODEC = ArrowCodec()
except RuntimeError:
    DEFAULT_ARROW_CODEC = None
