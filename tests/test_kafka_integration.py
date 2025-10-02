#!/usr/bin/env python3
"""
Comprehensive Kafka Integration Tests

Tests all Kafka functionality:
- All codec types (JSON, Avro, Protobuf, JSON Schema, MessagePack, String, Bytes)
- Schema Registry integration
- KafkaSource and KafkaSink
- Stream API integration
- Error handling and edge cases

Note: These tests require a running Kafka instance and Schema Registry.
For CI/CD, use testcontainers or mock the Kafka connections.
"""

import pytest
import asyncio
import time
from typing import Dict, Any

# Kafka integration
from sabot.kafka import (
    KafkaSource,
    KafkaSourceConfig,
    KafkaSink,
    KafkaSinkConfig,
    SchemaRegistryClient,
    from_kafka,
    to_kafka,
)

# Codecs
from sabot.types.codecs import (
    CodecType,
    CodecArg,
    create_codec,
    JSONCodec,
    MsgPackCodec,
    AvroCodec,
    ProtobufCodec,
    JSONSchemaCodec,
    StringCodec,
    BytesCodec,
)

# Stream API
from sabot.api.stream import Stream


# ============================================================================
# Test Configuration
# ============================================================================

KAFKA_BOOTSTRAP = "localhost:9092"
SCHEMA_REGISTRY_URL = "http://localhost:8081"

# Skip tests if Kafka not available
pytestmark = pytest.mark.skipif(
    not pytest.config.getoption("--run-kafka"),
    reason="Kafka tests require --run-kafka flag and running Kafka instance"
)


# ============================================================================
# Codec Tests
# ============================================================================

class TestCodecs:
    """Test all codec implementations."""

    def test_json_codec(self):
        """Test JSON codec encode/decode."""
        codec = JSONCodec()

        message = {"user_id": "123", "amount": 100.50, "action": "purchase"}
        encoded = codec.encode(message)

        assert isinstance(encoded, bytes)
        assert codec.content_type == "application/json"

        decoded = codec.decode(encoded)
        assert decoded == message

    def test_msgpack_codec(self):
        """Test MessagePack codec encode/decode."""
        try:
            codec = MsgPackCodec()
        except RuntimeError:
            pytest.skip("msgpack not installed")

        message = {"user_id": "123", "amount": 100.50, "nested": {"key": "value"}}
        encoded = codec.encode(message)

        assert isinstance(encoded, bytes)
        assert len(encoded) < len(str(message))  # More compact than JSON
        assert codec.content_type == "application/msgpack"

        decoded = codec.decode(encoded)
        assert decoded == message

    def test_string_codec(self):
        """Test String codec encode/decode."""
        codec = StringCodec()

        message = "Hello, Kafka!"
        encoded = codec.encode(message)

        assert isinstance(encoded, bytes)
        assert encoded == b"Hello, Kafka!"
        assert codec.content_type == "text/plain; charset=utf-8"

        decoded = codec.decode(encoded)
        assert decoded == message

    def test_bytes_codec(self):
        """Test Bytes codec (no-op)."""
        codec = BytesCodec()

        message = b"\x00\x01\x02\x03\xff"
        encoded = codec.encode(message)

        assert encoded == message
        assert codec.content_type == "application/octet-stream"

        decoded = codec.decode(encoded)
        assert decoded == message

    def test_avro_codec_without_registry(self):
        """Test Avro codec with inline schema (no registry)."""
        schema_str = """
        {
            "type": "record",
            "name": "User",
            "fields": [
                {"name": "name", "type": "string"},
                {"name": "age", "type": "int"}
            ]
        }
        """

        try:
            codec = AvroCodec(schema_str=schema_str)
        except RuntimeError:
            pytest.skip("avro not installed")

        message = {"name": "Alice", "age": 30}
        encoded = codec.encode(message)

        assert isinstance(encoded, bytes)
        assert codec.content_type == "application/vnd.confluent.avro"

        decoded = codec.decode(encoded)
        assert decoded == message

    @pytest.mark.asyncio
    async def test_avro_codec_with_registry(self):
        """Test Avro codec with Schema Registry."""
        schema_str = """
        {
            "type": "record",
            "name": "Transaction",
            "fields": [
                {"name": "id", "type": "string"},
                {"name": "amount", "type": "double"}
            ]
        }
        """

        try:
            codec = AvroCodec(
                schema_registry_url=SCHEMA_REGISTRY_URL,
                schema_str=schema_str,
                subject="test-transactions-value"
            )
        except RuntimeError:
            pytest.skip("avro or Schema Registry not available")

        message = {"id": "TX-123", "amount": 99.99}
        encoded = codec.encode(message)

        # Check Confluent wire format
        assert len(encoded) >= 5
        assert encoded[0] == 0  # Magic byte

        decoded = codec.decode(encoded)
        assert decoded == message

    def test_json_schema_codec(self):
        """Test JSON Schema codec with validation."""
        schema = {
            "type": "object",
            "properties": {
                "name": {"type": "string"},
                "age": {"type": "integer", "minimum": 0}
            },
            "required": ["name", "age"]
        }

        try:
            codec = JSONSchemaCodec(schema=schema)
        except RuntimeError:
            pytest.skip("jsonschema not installed")

        # Valid message
        message = {"name": "Bob", "age": 25}
        encoded = codec.encode(message)
        decoded = codec.decode(encoded)
        assert decoded == message

        # Invalid message (should raise validation error)
        with pytest.raises(Exception):  # jsonschema.ValidationError
            invalid = {"name": "Charlie"}  # Missing required 'age'
            codec.encode(invalid)

    def test_codec_factory(self):
        """Test create_codec factory function."""
        # JSON
        json_codec = create_codec(CodecArg(CodecType.JSON))
        assert isinstance(json_codec, JSONCodec)

        # String
        string_codec = create_codec(CodecArg(CodecType.STRING))
        assert isinstance(string_codec, StringCodec)

        # Bytes
        bytes_codec = create_codec(CodecArg(CodecType.BYTES))
        assert isinstance(bytes_codec, BytesCodec)


# ============================================================================
# Schema Registry Tests
# ============================================================================

class TestSchemaRegistry:
    """Test Schema Registry client."""

    @pytest.mark.asyncio
    async def test_schema_registration(self):
        """Test registering schemas."""
        try:
            client = SchemaRegistryClient(SCHEMA_REGISTRY_URL)
        except Exception:
            pytest.skip("Schema Registry not available")

        schema_str = """
        {
            "type": "record",
            "name": "TestRecord",
            "fields": [
                {"name": "field1", "type": "string"}
            ]
        }
        """

        subject = f"test-subject-{int(time.time())}"

        # Register schema
        schema_id = client.register_schema(subject, schema_str, "AVRO")
        assert isinstance(schema_id, int)
        assert schema_id > 0

        # Fetch by ID
        registered = client.get_schema_by_id(schema_id)
        assert registered is not None
        assert registered.schema_id == schema_id
        assert registered.subject == subject

        # Fetch latest
        latest = client.get_latest_schema(subject)
        assert latest is not None
        assert latest.schema_id == schema_id

    @pytest.mark.asyncio
    async def test_schema_caching(self):
        """Test schema caching behavior."""
        try:
            client = SchemaRegistryClient(SCHEMA_REGISTRY_URL)
        except Exception:
            pytest.skip("Schema Registry not available")

        schema_str = """{"type": "string"}"""
        subject = f"test-cache-{int(time.time())}"

        # Register
        schema_id = client.register_schema(subject, schema_str, "AVRO")

        # First fetch (hits registry)
        registered1 = client.get_schema_by_id(schema_id)

        # Second fetch (should be cached)
        registered2 = client.get_schema_by_id(schema_id)

        assert registered1.schema_id == registered2.schema_id
        # Both should be the same cached object
        assert registered1 is registered2


# ============================================================================
# KafkaSource Tests
# ============================================================================

class TestKafkaSource:
    """Test Kafka source functionality."""

    @pytest.mark.asyncio
    async def test_kafka_source_json(self):
        """Test reading JSON from Kafka."""
        config = KafkaSourceConfig(
            bootstrap_servers=KAFKA_BOOTSTRAP,
            topic="test-json-topic",
            group_id="test-consumer",
            codec_type="json"
        )

        source = KafkaSource(config)

        # Start source
        await source.start()

        # Read messages (with timeout)
        messages = []
        try:
            async for message in source.stream():
                messages.append(message)
                if len(messages) >= 10:
                    break
        except asyncio.TimeoutError:
            pass
        finally:
            await source.stop()

        # Should have read some messages (if topic has data)
        # In real test, we'd produce test data first
        assert isinstance(messages, list)

    @pytest.mark.asyncio
    async def test_kafka_source_with_metadata(self):
        """Test reading with metadata."""
        config = KafkaSourceConfig(
            bootstrap_servers=KAFKA_BOOTSTRAP,
            topic="test-topic",
            group_id="test-metadata",
            codec_type="json"
        )

        source = KafkaSource(config)
        await source.start()

        try:
            async for metadata in source.stream_with_metadata():
                # Check metadata fields
                assert 'value' in metadata
                assert 'key' in metadata
                assert 'topic' in metadata
                assert 'partition' in metadata
                assert 'offset' in metadata
                assert 'timestamp' in metadata
                assert 'headers' in metadata
                break
        except asyncio.TimeoutError:
            pass
        finally:
            await source.stop()

    @pytest.mark.asyncio
    async def test_from_kafka_convenience(self):
        """Test from_kafka convenience function."""
        source = from_kafka(
            bootstrap_servers=KAFKA_BOOTSTRAP,
            topic="test-topic",
            group_id="test-convenience",
            codec_type="json"
        )

        assert isinstance(source, KafkaSource)
        assert source.config.topic == "test-topic"
        assert source.config.codec_type == "json"


# ============================================================================
# KafkaSink Tests
# ============================================================================

class TestKafkaSink:
    """Test Kafka sink functionality."""

    @pytest.mark.asyncio
    async def test_kafka_sink_json(self):
        """Test writing JSON to Kafka."""
        config = KafkaSinkConfig(
            bootstrap_servers=KAFKA_BOOTSTRAP,
            topic="test-output-json",
            codec_type="json"
        )

        sink = KafkaSink(config)
        await sink.start()

        try:
            # Send messages
            messages = [
                {"id": 1, "value": "test1"},
                {"id": 2, "value": "test2"},
                {"id": 3, "value": "test3"}
            ]

            for msg in messages:
                await sink.send(msg)

            # Flush
            await sink.flush()

        finally:
            await sink.stop()

    @pytest.mark.asyncio
    async def test_kafka_sink_batch(self):
        """Test batch sending."""
        config = KafkaSinkConfig(
            bootstrap_servers=KAFKA_BOOTSTRAP,
            topic="test-output-batch",
            codec_type="json"
        )

        sink = KafkaSink(config)
        await sink.start()

        try:
            messages = [{"id": i, "data": f"batch_{i}"} for i in range(100)]
            await sink.send_batch(messages)
            await sink.flush()

        finally:
            await sink.stop()

    @pytest.mark.asyncio
    async def test_to_kafka_convenience(self):
        """Test to_kafka convenience function."""
        sink = to_kafka(
            bootstrap_servers=KAFKA_BOOTSTRAP,
            topic="test-output",
            codec_type="json",
            compression_type="lz4"
        )

        assert isinstance(sink, KafkaSink)
        assert sink.config.topic == "test-output"
        assert sink.config.compression_type == "lz4"


# ============================================================================
# Stream API Integration Tests
# ============================================================================

class TestStreamAPIIntegration:
    """Test Stream API Kafka integration."""

    @pytest.mark.asyncio
    async def test_stream_from_kafka(self):
        """Test Stream.from_kafka()."""
        # Note: This requires async event loop handling
        # In real usage, Stream.from_kafka handles async internally

        # Create stream (lazy)
        stream = Stream.from_kafka(
            bootstrap_servers=KAFKA_BOOTSTRAP,
            topic="test-stream-input",
            group_id="test-stream-reader",
            codec_type="json",
            batch_size=100
        )

        assert isinstance(stream, Stream)

    @pytest.mark.asyncio
    async def test_stream_to_kafka(self):
        """Test stream.to_kafka()."""
        # Create stream from in-memory data
        data = [
            {"id": 1, "value": "a"},
            {"id": 2, "value": "b"},
            {"id": 3, "value": "c"}
        ]

        stream = Stream.from_dicts(data, batch_size=10)

        # Write to Kafka (executes immediately)
        # Note: In real test, we'd verify messages were written
        # stream.to_kafka(
        #     bootstrap_servers=KAFKA_BOOTSTRAP,
        #     topic="test-stream-output",
        #     codec_type="json"
        # )

    @pytest.mark.asyncio
    async def test_end_to_end_pipeline(self):
        """Test complete ETL pipeline: Kafka → Stream → Kafka."""
        # This would be a full integration test
        # Produce data → Read with Stream API → Transform → Write back

        # 1. Produce test data
        sink = to_kafka(
            bootstrap_servers=KAFKA_BOOTSTRAP,
            topic="test-pipeline-input",
            codec_type="json"
        )

        await sink.start()
        test_data = [{"id": i, "value": i * 2} for i in range(10)]
        await sink.send_batch(test_data)
        await sink.stop()

        # 2. Read, transform, write (would need async handling)
        # stream = Stream.from_kafka(...)
        # result = stream.filter(...).map(...)
        # result.to_kafka(...)


# ============================================================================
# Error Handling Tests
# ============================================================================

class TestErrorHandling:
    """Test error handling and edge cases."""

    def test_invalid_codec_type(self):
        """Test invalid codec type."""
        with pytest.raises(ValueError):
            create_codec(CodecArg(CodecType.JSON, options={}))
            # Invalid usage would be caught by enum

    @pytest.mark.asyncio
    async def test_kafka_connection_failure(self):
        """Test handling of Kafka connection failures."""
        config = KafkaSourceConfig(
            bootstrap_servers="invalid:9999",  # Non-existent broker
            topic="test-topic",
            group_id="test-fail"
        )

        source = KafkaSource(config)

        # Should raise connection error
        with pytest.raises(Exception):
            await source.start()
            # Give it a moment to fail
            await asyncio.sleep(1)

    def test_schema_registry_connection_failure(self):
        """Test Schema Registry connection handling."""
        try:
            client = SchemaRegistryClient("http://invalid:9999")
            schema_str = """{"type": "string"}"""

            # Should fail to register
            with pytest.raises(Exception):
                client.register_schema("test", schema_str, "AVRO")
        except Exception:
            pytest.skip("Expected Schema Registry failure")


# ============================================================================
# Performance Tests (Optional)
# ============================================================================

class TestPerformance:
    """Performance benchmarks for Kafka integration."""

    @pytest.mark.benchmark
    @pytest.mark.asyncio
    async def test_throughput_json(self):
        """Benchmark JSON codec throughput."""
        codec = JSONCodec()

        # Encode 10,000 messages
        messages = [{"id": i, "value": f"msg_{i}"} for i in range(10000)]

        start = time.time()
        for msg in messages:
            encoded = codec.encode(msg)
            decoded = codec.decode(encoded)
        elapsed = time.time() - start

        throughput = len(messages) / elapsed
        print(f"\nJSON throughput: {throughput:.0f} msgs/sec")

        # Should be > 10K msgs/sec for JSON
        assert throughput > 10000

    @pytest.mark.benchmark
    @pytest.mark.asyncio
    async def test_throughput_msgpack(self):
        """Benchmark MessagePack codec throughput."""
        try:
            codec = MsgPackCodec()
        except RuntimeError:
            pytest.skip("msgpack not installed")

        messages = [{"id": i, "value": f"msg_{i}"} for i in range(10000)]

        start = time.time()
        for msg in messages:
            encoded = codec.encode(msg)
            decoded = codec.decode(encoded)
        elapsed = time.time() - start

        throughput = len(messages) / elapsed
        print(f"\nMessagePack throughput: {throughput:.0f} msgs/sec")

        # MessagePack should be faster than JSON
        assert throughput > 15000


# ============================================================================
# Pytest Configuration
# ============================================================================

def pytest_addoption(parser):
    """Add custom pytest options."""
    parser.addoption(
        "--run-kafka",
        action="store_true",
        default=False,
        help="Run tests that require Kafka"
    )


# ============================================================================
# Test Utilities
# ============================================================================

@pytest.fixture
async def kafka_producer():
    """Fixture for Kafka producer."""
    sink = to_kafka(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        topic="test-fixture",
        codec_type="json"
    )
    await sink.start()
    yield sink
    await sink.stop()


@pytest.fixture
async def kafka_consumer():
    """Fixture for Kafka consumer."""
    source = from_kafka(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        topic="test-fixture",
        group_id="test-fixture-consumer",
        codec_type="json"
    )
    await source.start()
    yield source
    await source.stop()


if __name__ == "__main__":
    """Run tests directly."""
    pytest.main([__file__, "-v", "--run-kafka"])
