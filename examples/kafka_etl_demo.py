#!/usr/bin/env python3
"""
Kafka ETL Demo - Full Integration Example

Demonstrates Sabot's complete Kafka integration with all codec types:
- Reading from Kafka (KafkaSource)
- Stream API transformations
- Writing to Kafka (KafkaSink)
- All serialization formats (JSON, Avro, Protobuf, JSON Schema, MessagePack)
- Schema Registry integration

This example shows a fraud detection pipeline:
1. Read transactions from Kafka (Avro with Schema Registry)
2. Detect fraudulent transactions using Stream API
3. Write alerts to Kafka (JSON for easy consumption)

Requirements:
- Kafka running on localhost:9092
- Schema Registry running on localhost:8081
- Install: pip install aiokafka avro protobuf jsonschema msgpack httpx
"""

import asyncio
import logging
from sabot.api.stream import Stream
from sabot.kafka import (
    from_kafka,
    to_kafka,
    KafkaSource,
    KafkaSink,
    SchemaRegistryClient
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# ============================================================================
# Example 1: JSON Kafka ETL (Simplest)
# ============================================================================

async def json_etl_example():
    """
    Simple JSON Kafka ETL pipeline.

    Reads JSON transactions, filters high-value ones, writes to output topic.
    """
    logger.info("=== Example 1: JSON Kafka ETL ===")

    # Create source
    source = from_kafka(
        bootstrap_servers="localhost:9092",
        topic="transactions-json",
        group_id="fraud-detector-json",
        codec_type="json"
    )

    # Start consuming
    await source.start()

    try:
        async for message in source.stream():
            # Process message (simple filter)
            if message.get('amount', 0) > 10000:
                logger.info(f"High-value transaction: {message}")

                # Could write to output topic here
                # sink.send(message)

    except KeyboardInterrupt:
        logger.info("Stopping JSON ETL")
    finally:
        await source.stop()


# ============================================================================
# Example 2: Avro Kafka ETL with Schema Registry
# ============================================================================

TRANSACTION_AVRO_SCHEMA = """
{
  "type": "record",
  "name": "Transaction",
  "namespace": "com.example.fraud",
  "fields": [
    {"name": "transaction_id", "type": "string"},
    {"name": "user_id", "type": "string"},
    {"name": "amount", "type": "double"},
    {"name": "merchant", "type": "string"},
    {"name": "timestamp", "type": "long"},
    {"name": "location", "type": ["null", "string"], "default": null}
  ]
}
"""

FRAUD_ALERT_AVRO_SCHEMA = """
{
  "type": "record",
  "name": "FraudAlert",
  "namespace": "com.example.fraud",
  "fields": [
    {"name": "alert_id", "type": "string"},
    {"name": "transaction_id", "type": "string"},
    {"name": "user_id", "type": "string"},
    {"name": "amount", "type": "double"},
    {"name": "reason", "type": "string"},
    {"name": "timestamp", "type": "long"}
  ]
}
"""


async def avro_etl_example():
    """
    Avro Kafka ETL with Schema Registry.

    Reads Avro transactions, detects fraud, writes Avro alerts.
    """
    logger.info("=== Example 2: Avro Kafka ETL with Schema Registry ===")

    # Schema Registry client
    registry = SchemaRegistryClient("http://localhost:8081")

    # Register schemas
    try:
        tx_schema_id = registry.register_schema(
            "transactions-value",
            TRANSACTION_AVRO_SCHEMA,
            "AVRO"
        )
        alert_schema_id = registry.register_schema(
            "fraud-alerts-value",
            FRAUD_ALERT_AVRO_SCHEMA,
            "AVRO"
        )
        logger.info(f"Registered schemas: TX={tx_schema_id}, Alert={alert_schema_id}")
    except Exception as e:
        logger.warning(f"Schema registration failed (may already exist): {e}")

    # Create Avro source
    source = from_kafka(
        bootstrap_servers="localhost:9092",
        topic="transactions-avro",
        group_id="fraud-detector-avro",
        codec_type="avro",
        codec_options={
            'schema_registry_url': 'http://localhost:8081',
            'subject': 'transactions-value'
        }
    )

    # Create Avro sink
    sink = to_kafka(
        bootstrap_servers="localhost:9092",
        topic="fraud-alerts-avro",
        codec_type="avro",
        codec_options={
            'schema_registry_url': 'http://localhost:8081',
            'subject': 'fraud-alerts-value',
            'schema': FRAUD_ALERT_AVRO_SCHEMA
        }
    )

    # Process
    await source.start()
    await sink.start()

    try:
        fraud_count = 0
        async for transaction in source.stream():
            # Fraud detection logic
            if is_fraudulent(transaction):
                alert = {
                    'alert_id': f"ALERT-{fraud_count}",
                    'transaction_id': transaction['transaction_id'],
                    'user_id': transaction['user_id'],
                    'amount': transaction['amount'],
                    'reason': detect_fraud_reason(transaction),
                    'timestamp': transaction['timestamp']
                }

                # Write alert
                await sink.send(alert)
                fraud_count += 1
                logger.info(f"Fraud detected: {alert}")

    except KeyboardInterrupt:
        logger.info("Stopping Avro ETL")
    finally:
        await source.stop()
        await sink.stop()


# ============================================================================
# Example 3: Stream API Integration (Most Powerful)
# ============================================================================

async def stream_api_kafka_example():
    """
    Use Stream API with Kafka for high-level transformations.

    This is the most powerful approach - combines Kafka I/O with
    Stream API transformations (filter, map, aggregate, etc.).
    """
    logger.info("=== Example 3: Stream API + Kafka Integration ===")

    # Create stream from Kafka
    stream = Stream.from_kafka(
        bootstrap_servers="localhost:9092",
        topic="transactions-json",
        group_id="fraud-detector-stream",
        codec_type="json",
        batch_size=1000  # Batch for Arrow processing
    )

    # Apply transformations using Stream API
    result = (stream
        .filter(lambda batch: batch.column('amount').to_pylist())  # Example filter
        .map(lambda batch: batch)  # Could enrich here
    )

    # Write to Kafka
    result.to_kafka(
        bootstrap_servers="localhost:9092",
        topic="processed-transactions",
        codec_type="json"
    )

    logger.info("Stream API + Kafka pipeline executed")


# ============================================================================
# Example 4: Multi-Codec Pipeline
# ============================================================================

async def multi_codec_example():
    """
    Demonstrate reading from one codec, writing to another.

    Reads Avro from Kafka, writes JSON (for downstream consumers).
    """
    logger.info("=== Example 4: Multi-Codec Pipeline ===")

    # Read Avro
    source = from_kafka(
        bootstrap_servers="localhost:9092",
        topic="transactions-avro",
        group_id="codec-converter",
        codec_type="avro",
        codec_options={
            'schema_registry_url': 'http://localhost:8081',
            'subject': 'transactions-value'
        }
    )

    # Write JSON (easier for downstream)
    sink = to_kafka(
        bootstrap_servers="localhost:9092",
        topic="transactions-json",
        codec_type="json"
    )

    # Convert
    await source.start()
    await sink.start()

    try:
        count = 0
        async for transaction in source.stream():
            await sink.send(transaction)
            count += 1

            if count % 1000 == 0:
                logger.info(f"Converted {count} messages (Avro → JSON)")

    except KeyboardInterrupt:
        logger.info("Stopping codec converter")
    finally:
        await source.stop()
        await sink.stop()


# ============================================================================
# Example 5: MessagePack for High Performance
# ============================================================================

async def msgpack_example():
    """
    Use MessagePack for high-performance binary serialization.

    MessagePack is faster and more compact than JSON.
    """
    logger.info("=== Example 5: MessagePack High-Performance Pipeline ===")

    source = from_kafka(
        bootstrap_servers="localhost:9092",
        topic="transactions-msgpack",
        group_id="msgpack-processor",
        codec_type="msgpack"
    )

    sink = to_kafka(
        bootstrap_servers="localhost:9092",
        topic="processed-msgpack",
        codec_type="msgpack"
    )

    await source.start()
    await sink.start()

    try:
        async for message in source.stream():
            # Process at wire speed
            processed = process_transaction(message)
            await sink.send(processed)

    except KeyboardInterrupt:
        logger.info("Stopping MessagePack pipeline")
    finally:
        await source.stop()
        await sink.stop()


# ============================================================================
# Helper Functions
# ============================================================================

def is_fraudulent(transaction: dict) -> bool:
    """
    Simple fraud detection rules.

    Production systems would use ML models.
    """
    amount = transaction.get('amount', 0)
    location = transaction.get('location')

    # Rule 1: Amount > $50,000
    if amount > 50000:
        return True

    # Rule 2: Missing location
    if location is None or location == "":
        return True

    # Rule 3: Known bad merchant
    bad_merchants = ["SCAM_CORP", "FRAUD_INC"]
    if transaction.get('merchant') in bad_merchants:
        return True

    return False


def detect_fraud_reason(transaction: dict) -> str:
    """Determine why transaction was flagged."""
    amount = transaction.get('amount', 0)
    location = transaction.get('location')
    merchant = transaction.get('merchant')

    if amount > 50000:
        return f"HIGH_AMOUNT: ${amount}"
    elif location is None or location == "":
        return "MISSING_LOCATION"
    elif merchant in ["SCAM_CORP", "FRAUD_INC"]:
        return f"KNOWN_BAD_MERCHANT: {merchant}"
    else:
        return "UNKNOWN"


def process_transaction(transaction: dict) -> dict:
    """Process transaction (placeholder)."""
    # Add processing timestamp
    import time
    transaction['processed_at'] = int(time.time() * 1000)
    return transaction


# ============================================================================
# Main
# ============================================================================

async def main():
    """Run all examples."""

    print("""
╔════════════════════════════════════════════════════════════════════╗
║                   Sabot Kafka ETL Demo                             ║
╚════════════════════════════════════════════════════════════════════╝

This demo showcases Sabot's full Kafka integration:
1. JSON Kafka ETL (simplest)
2. Avro with Schema Registry (production-grade)
3. Stream API integration (most powerful)
4. Multi-codec pipeline (Avro → JSON)
5. MessagePack (high performance)

Prerequisites:
- Kafka running on localhost:9092
- Schema Registry on localhost:8081 (for Avro examples)

Choose an example to run:
[1] JSON ETL
[2] Avro ETL with Schema Registry
[3] Stream API + Kafka
[4] Multi-codec (Avro → JSON)
[5] MessagePack pipeline
[0] Exit
    """)

    choice = input("Enter choice: ").strip()

    if choice == "1":
        await json_etl_example()
    elif choice == "2":
        await avro_etl_example()
    elif choice == "3":
        await stream_api_kafka_example()
    elif choice == "4":
        await multi_codec_example()
    elif choice == "5":
        await msgpack_example()
    else:
        print("Exiting")


if __name__ == "__main__":
    asyncio.run(main())
