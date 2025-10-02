#!/usr/bin/env python3
"""
Fraud Detection Demo - Data Producer
=====================================

Reads banking transaction CSVs and produces to Kafka.
Run this in a separate process from the Sabot fraud detector.

Usage:
    python flink_fraud_producer.py
"""

import asyncio
import csv
import json
import time
from pathlib import Path
from typing import List
from dataclasses import dataclass, asdict

# Kafka imports
try:
    from confluent_kafka import Producer
    from confluent_kafka.admin import AdminClient, NewTopic
    KAFKA_AVAILABLE = True
except ImportError:
    print("⚠️  confluent-kafka not installed. Install with: pip install confluent-kafka")
    KAFKA_AVAILABLE = False


@dataclass
class BankTransaction:
    """Bank transaction record."""
    transaction_id: int
    account_number: str
    user_id: str  # RUT or DNI
    transaction_date: str
    transaction_amount: float
    currency: str
    transaction_type: str
    branch_city: str
    payment_method: str
    country: str  # Chile or Peru
    timestamp_ms: int  # Event time in milliseconds


class KafkaSetup:
    """Setup Kafka topic."""

    def __init__(self, bootstrap_servers: str = "localhost:19092"):
        self.bootstrap_servers = bootstrap_servers
        self.topic_name = "bank-transactions"
        self.num_partitions = 3

    def create_topic(self):
        """Create Kafka topic if it doesn't exist."""
        if not KAFKA_AVAILABLE:
            print("⚠️  Kafka not available - exiting")
            return False

        try:
            admin = AdminClient({'bootstrap.servers': self.bootstrap_servers})

            topic = NewTopic(
                topic=self.topic_name,
                num_partitions=self.num_partitions,
                replication_factor=1
            )

            fs = admin.create_topics([topic])

            # Wait for operation to complete
            for topic, f in fs.items():
                try:
                    f.result()
                    print(f"✅ Created Kafka topic '{topic}' with {self.num_partitions} partitions")
                except Exception as e:
                    if "already exists" in str(e).lower():
                        print(f"✅ Kafka topic '{topic}' already exists")
                    else:
                        print(f"❌ Failed to create topic {topic}: {e}")
                        return False
            return True
        except Exception as e:
            print(f"❌ Failed to create Kafka topic: {e}")
            return False


class BankTransactionProducer:
    """Produces bank transactions to Kafka."""

    def __init__(self, bootstrap_servers: str = "localhost:19092"):
        self.bootstrap_servers = bootstrap_servers
        self.topic_name = "bank-transactions"
        self.producer = None
        self.transaction_count = 0

        if KAFKA_AVAILABLE:
            self.producer = Producer({
                'bootstrap.servers': bootstrap_servers,
                'compression.type': 'snappy',
                'batch.size': 16384,
                'linger.ms': 10
            })

    async def load_and_produce(self, csv_path: Path, country: str, rate_limit: int = 5000):
        """
        Load CSV and produce to Kafka at specified rate.

        Args:
            csv_path: Path to CSV file
            country: Chile or Peru
            rate_limit: Max transactions per second
        """
        print(f"\n📤 Loading {country} transactions from {csv_path.name}")
        start_time = time.time()
        batch = []
        batch_size = 100

        with open(csv_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)

            for row in reader:
                # Parse transaction
                txn = BankTransaction(
                    transaction_id=int(row['transaction_id']),
                    account_number=row['account_number'],
                    user_id=row.get('user_rut', row.get('user_dni', 'unknown')),
                    transaction_date=row['transaction_date'],
                    transaction_amount=float(row['transaction_amount']),
                    currency=row['currency'],
                    transaction_type=row['transaction_type'],
                    branch_city=row['branch_city'],
                    payment_method=row['payment_method'],
                    country=country,
                    timestamp_ms=int(time.time() * 1000)  # Current time as event time
                )

                batch.append(txn)

                # Send batch to Kafka
                if len(batch) >= batch_size:
                    await self._send_batch(batch)
                    batch = []

                    # Rate limiting
                    elapsed = time.time() - start_time
                    expected_time = self.transaction_count / rate_limit
                    if elapsed < expected_time:
                        await asyncio.sleep(expected_time - elapsed)

            # Send remaining batch
            if batch:
                await self._send_batch(batch)

        elapsed = time.time() - start_time
        rate = self.transaction_count / elapsed if elapsed > 0 else 0
        print(f"✅ {country}: Sent {self.transaction_count:,} transactions in {elapsed:.1f}s ({rate:.0f} txn/s)")

    async def _send_batch(self, batch: List[BankTransaction]):
        """Send batch of transactions to Kafka."""
        if self.producer is None:
            print("❌ No Kafka producer available")
            return

        for txn in batch:
            # Use account_number as key for partitioning
            key = txn.account_number.encode('utf-8')
            value = json.dumps(asdict(txn)).encode('utf-8')

            try:
                self.producer.produce(
                    topic=self.topic_name,
                    key=key,
                    value=value
                )
                self.transaction_count += 1
            except BufferError:
                # Queue is full, wait for messages to be delivered
                self.producer.poll(0.1)
                self.producer.produce(
                    topic=self.topic_name,
                    key=key,
                    value=value
                )
                self.transaction_count += 1

        # Flush every batch for low latency
        self.producer.poll(0)
        self.producer.flush()

    def close(self):
        """Close producer."""
        if self.producer:
            self.producer.flush()
            print("\n✅ Producer closed")


async def main():
    """Run the transaction producer."""
    print("=" * 70)
    print("🏦 Bank Transaction Producer")
    print("=" * 70)
    print("\nThis process reads CSV files and produces to Kafka.")
    print("Run the Sabot fraud detector in a separate process.\n")
    print("=" * 70)

    if not KAFKA_AVAILABLE:
        print("❌ confluent-kafka not installed!")
        print("   Install with: pip install confluent-kafka")
        return

    # Setup Kafka
    print("\n📡 Setting up Kafka...")
    kafka_setup = KafkaSetup()
    if not kafka_setup.create_topic():
        print("❌ Failed to setup Kafka - exiting")
        return

    # Data paths (configurable via environment or fallback to default)
    import os
    data_dir_str = os.getenv("SABOT_DATA_DIR", "/Users/bengamble/PycharmProjects/pythonProject/demoData/banking")
    data_dir = Path(data_dir_str)
    chilean_csv = data_dir / "chilean_bank_transactions.csv"
    peruvian_csv = data_dir / "peruvian_bank_transactions.csv"

    # Verify data files exist
    if not chilean_csv.exists():
        print(f"❌ Chilean data file not found: {chilean_csv}")
        print(f"   Set SABOT_DATA_DIR environment variable or place CSV files in {data_dir}")
        return
    if not peruvian_csv.exists():
        print(f"❌ Peruvian data file not found: {peruvian_csv}")
        print(f"   Set SABOT_DATA_DIR environment variable or place CSV files in {data_dir}")
        return

    # Create producer
    producer = BankTransactionProducer()

    print("\n" + "=" * 70)
    print("📥 Producing Transactions to Kafka")
    print("=" * 70)

    try:
        # Load both countries concurrently
        await asyncio.gather(
            producer.load_and_produce(chilean_csv, "Chile", rate_limit=3000),
            producer.load_and_produce(peruvian_csv, "Peru", rate_limit=3000)
        )

        print(f"\n✅ Total transactions produced: {producer.transaction_count:,}")
        print("\n🎉 Producer Complete!")
        print("   Now run the Sabot fraud detector: python flink_fraud_detector.py")

    except KeyboardInterrupt:
        print("\n🛑 Producer interrupted by user")
    finally:
        producer.close()


if __name__ == "__main__":
    asyncio.run(main())
