#!/usr/bin/env python3
"""
Kafka Integration for Sabot

Provides Kafka-specific functionality:
- Schema Registry client
- Avro/Protobuf/JSON Schema codec support
- Kafka source (read from topics)
- Kafka sink (write to topics)
- Kafka channel integrations

This module will be optimized with Cython in Phase 2 for production performance.
"""

from .schema_registry import SchemaRegistryClient, RegisteredSchema, CompatibilityMode
from .source import KafkaSource, KafkaSourceConfig, from_kafka
from .sink import KafkaSink, KafkaSinkConfig, to_kafka, PartitionStrategy

__all__ = [
    'SchemaRegistryClient',
    'RegisteredSchema',
    'CompatibilityMode',
    'KafkaSource',
    'KafkaSourceConfig',
    'from_kafka',
    'KafkaSink',
    'KafkaSinkConfig',
    'to_kafka',
    'PartitionStrategy',
]
