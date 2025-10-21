"""
Kafka Integration for Sabot

Provides C++ librdkafka bindings (default) with Python aiokafka fallback.
"""

# Try to import C++ implementation
try:
    from .librdkafka_source import LibrdkafkaSource
    from .librdkafka_sink import LibrdkafkaSink
    LIBRDKAFKA_AVAILABLE = True
except ImportError:
    LIBRDKAFKA_AVAILABLE = False
    LibrdkafkaSource = None
    LibrdkafkaSink = None

__all__ = [
    'LibrdkafkaSource',
    'LibrdkafkaSink',
    'LIBRDKAFKA_AVAILABLE',
]

