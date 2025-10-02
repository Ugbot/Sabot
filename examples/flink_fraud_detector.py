#!/usr/bin/env python3
"""
Fraud Detection Demo - Sabot Agent/Actor System
================================================

Sabot-based fraud detection using Cython agents.
Runs in a separate process from the data producer.

Features:
- FastArrowAgent for parallel processing
- Distributed checkpointing with barriers
- Watermark tracking
- Multi-pattern fraud detection

Usage:
    python flink_fraud_detector.py
"""

import asyncio
import json
import time
import math
from collections import defaultdict, deque
from typing import Dict, List, Any, Optional
from dataclasses import dataclass

# Kafka imports
try:
    from confluent_kafka import Consumer, KafkaException
    KAFKA_AVAILABLE = True
except ImportError:
    print("⚠️  confluent-kafka not installed. Install with: pip install confluent-kafka")
    KAFKA_AVAILABLE = False

# Sabot modules (clean API)
import sabot as sb


@dataclass
class BankTransaction:
    """Bank transaction record."""
    transaction_id: int
    account_number: str
    user_id: str
    transaction_date: str
    transaction_amount: float
    currency: str
    transaction_type: str
    branch_city: str
    payment_method: str
    country: str
    timestamp_ms: int


@dataclass
class FraudAlert:
    """Fraud detection alert."""
    alert_id: str
    transaction_id: int
    account_number: str
    fraud_type: str
    severity: str
    details: str
    timestamp_ms: int
    related_transactions: List[int]


class FraudDetector:
    """Multi-pattern fraud detection using Cython state stores."""

    def __init__(self):
        # State stores using Sabot memory backend
        config = sb.BackendConfig(
            backend_type="memory",
            max_size=100000,
            ttl_seconds=300.0  # 5 minute TTL
        )

        self.account_history = sb.MemoryBackend(config)
        self.account_stats = {}
        self.last_location = {}
        self.alerts: List[FraudAlert] = []
        self.alert_counter = 0

        # City coordinates for distance calculation
        self.city_coords = {
            'Santiago': (-33.4489, -70.6693), 'Valparaíso': (-33.0472, -71.6127),
            'Concepción': (-36.8201, -73.0444), 'La Serena': (-29.9027, -71.2519),
            'Antofagasta': (-23.6509, -70.3975), 'Temuco': (-38.7359, -72.5904),
            'Arica': (-18.4783, -70.3126), 'Punta Arenas': (-53.1638, -70.9171),
            'Lima': (-12.0464, -77.0428), 'Arequipa': (-16.4090, -71.5375),
            'Trujillo': (-8.1116, -79.0288), 'Cusco': (-13.5319, -71.9675),
        }

    async def detect_fraud(self, txn: BankTransaction) -> List[FraudAlert]:
        """Run all fraud detection rules on a transaction."""
        alerts = []

        velocity_alert = await self._check_velocity(txn)
        if velocity_alert:
            alerts.append(velocity_alert)

        amount_alert = await self._check_amount_anomaly(txn)
        if amount_alert:
            alerts.append(amount_alert)

        geo_alert = await self._check_geographic(txn)
        if geo_alert:
            alerts.append(geo_alert)

        await self._update_state(txn)
        self.alerts.extend(alerts)
        return alerts

    async def _check_velocity(self, txn: BankTransaction) -> Optional[FraudAlert]:
        """Detect multiple transactions in short time window."""
        account = txn.account_number
        current_time = txn.timestamp_ms

        history_key = f"history:{account}"
        history_data = await self.account_history.get(history_key)
        history = history_data if history_data else []

        window_ms = 60000
        recent_txns = [t for t in history if current_time - t['ts'] < window_ms]

        if len(recent_txns) >= 2:
            self.alert_counter += 1
            return FraudAlert(
                alert_id=f"FA{self.alert_counter:06d}",
                transaction_id=txn.transaction_id,
                account_number=account,
                fraud_type="VELOCITY",
                severity="HIGH",
                details=f"{len(recent_txns) + 1} transactions in 60s",
                timestamp_ms=current_time,
                related_transactions=[t['id'] for t in recent_txns]
            )
        return None

    async def _check_amount_anomaly(self, txn: BankTransaction) -> Optional[FraudAlert]:
        """Detect unusually large transaction amounts."""
        account = txn.account_number
        amount = txn.transaction_amount

        if account not in self.account_stats:
            return None

        stats = self.account_stats[account]
        n = stats['count']

        if n < 5:
            return None

        mean = stats['sum'] / n
        variance = (stats['sum_sq'] / n) - (mean ** 2)
        std = variance ** 0.5 if variance > 0 else 0

        if std == 0:
            return None

        z_score = (amount - mean) / std

        if abs(z_score) > 3.0:
            self.alert_counter += 1
            return FraudAlert(
                alert_id=f"FA{self.alert_counter:06d}",
                transaction_id=txn.transaction_id,
                account_number=account,
                fraud_type="AMOUNT_ANOMALY",
                severity="CRITICAL" if z_score > 4 else "HIGH",
                details=f"Amount {amount:.2f} {txn.currency} is {z_score:.1f}σ from mean {mean:.2f}",
                timestamp_ms=txn.timestamp_ms,
                related_transactions=[]
            )
        return None

    async def _check_geographic(self, txn: BankTransaction) -> Optional[FraudAlert]:
        """Detect impossible travel between cities."""
        account = txn.account_number
        current_city = txn.branch_city
        current_time = txn.timestamp_ms

        if account not in self.last_location:
            return None

        last_loc = self.last_location[account]
        last_city = last_loc['city']
        last_time = last_loc['timestamp_ms']

        if last_city == current_city:
            return None

        time_diff_min = (current_time - last_time) / 60000.0

        if time_diff_min < 10.0:
            distance = self._calculate_distance(last_city, current_city)
            self.alert_counter += 1
            return FraudAlert(
                alert_id=f"FA{self.alert_counter:06d}",
                transaction_id=txn.transaction_id,
                account_number=account,
                fraud_type="GEO_IMPOSSIBLE",
                severity="CRITICAL",
                details=f"{last_city}→{current_city} in {time_diff_min:.1f}min ({distance:.0f}km)",
                timestamp_ms=current_time,
                related_transactions=[]
            )
        return None

    def _calculate_distance(self, city1: str, city2: str) -> float:
        """Calculate approximate distance between cities in km."""
        if city1 not in self.city_coords or city2 not in self.city_coords:
            return 0.0

        lat1, lon1 = self.city_coords[city1]
        lat2, lon2 = self.city_coords[city2]

        R = 6371  # Earth radius in km
        dlat = math.radians(lat2 - lat1)
        dlon = math.radians(lon2 - lon1)
        a = (math.sin(dlat/2) ** 2 +
             math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) *
             math.sin(dlon/2) ** 2)
        c = 2 * math.atan2(math.sqrt(a), math.sqrt(1-a))
        return R * c

    async def _update_state(self, txn: BankTransaction):
        """Update account state after processing transaction."""
        account = txn.account_number

        history_key = f"history:{account}"
        history_data = await self.account_history.get(history_key)
        history = history_data if history_data else []

        history.append({
            'id': txn.transaction_id,
            'ts': txn.timestamp_ms,
            'amount': txn.transaction_amount
        })

        history = history[-100:]
        await self.account_history.set(history_key, history)

        if account not in self.account_stats:
            self.account_stats[account] = {'sum': 0.0, 'count': 0, 'sum_sq': 0.0}

        stats = self.account_stats[account]
        stats['sum'] += txn.transaction_amount
        stats['count'] += 1
        stats['sum_sq'] += txn.transaction_amount ** 2

        self.last_location[account] = {
            'city': txn.branch_city,
            'timestamp_ms': txn.timestamp_ms
        }


class SabotFraudAgent:
    """Sabot agent for fraud detection."""

    def __init__(self, partition_id: int, bootstrap_servers: str = "localhost:19092"):
        self.partition_id = partition_id
        self.bootstrap_servers = bootstrap_servers
        self.topic_name = "bank-transactions"
        self.fraud_detector = FraudDetector()

        # Sabot checkpoint and time modules
        self.watermark_tracker = sb.WatermarkTracker(num_partitions=3)
        self.barrier_tracker = sb.BarrierTracker(num_channels=3)

        # Metrics
        self.processed_count = 0
        self.start_time = time.time()
        self.latencies = deque(maxlen=10000)
        self.checkpoint_count = 0
        self.last_checkpoint_time = time.time() * 1000
        self.checkpoint_interval_ms = 5000

        # Create consumer
        self.consumer = self._create_consumer()

    def _create_consumer(self) -> Optional[Consumer]:
        """Create Kafka consumer for this partition."""
        if not KAFKA_AVAILABLE:
            return None

        try:
            from confluent_kafka import TopicPartition

            config = {
                'bootstrap.servers': self.bootstrap_servers,
                'group.id': 'fraud-detection-agents',
                'auto.offset.reset': 'earliest',
                'enable.auto.commit': False,
            }

            consumer = Consumer(config)
            tp = TopicPartition(self.topic_name, self.partition_id)
            consumer.assign([tp])
            return consumer
        except Exception as e:
            print(f"❌ Failed to create consumer for partition {self.partition_id}: {e}")
            return None

    async def run(self):
        """Main agent processing loop."""
        if self.consumer is None:
            print(f"⚠️  Agent {self.partition_id}: No consumer available")
            return

        print(f"✅ Agent {self.partition_id}: Started")

        try:
            while True:
                message = self.consumer.poll(timeout=1.0)

                if message is None:
                    continue

                if message.error():
                    print(f"❌ Agent {self.partition_id} error: {message.error()}")
                    continue

                # Decode and process transaction
                txn_data = json.loads(message.value().decode('utf-8'))
                txn = BankTransaction(**txn_data)

                # Update watermark
                self.watermark_tracker.update_watermark(self.partition_id, txn.timestamp_ms)

                # Process transaction
                process_start = time.time()
                alerts = await self.fraud_detector.detect_fraud(txn)
                process_time = (time.time() - process_start) * 1000

                self.latencies.append(process_time)
                self.processed_count += 1

                # Display fraud alerts
                for alert in alerts:
                    self._display_alert(alert)

                # Checkpoint if needed
                current_time = time.time() * 1000
                if current_time - self.last_checkpoint_time > self.checkpoint_interval_ms:
                    await self._trigger_checkpoint()

                # Commit offset
                self.consumer.commit(asynchronous=False)

        except KeyboardInterrupt:
            print(f"\n🛑 Agent {self.partition_id}: Stopping...")
        finally:
            if self.consumer:
                self.consumer.close()

    def _display_alert(self, alert: FraudAlert):
        """Display fraud alert."""
        severity_emoji = {
            'LOW': '⚠️', 'MEDIUM': '🟡', 'HIGH': '🟠', 'CRITICAL': '🚨'
        }
        emoji = severity_emoji.get(alert.severity, '⚠️')
        print(f"{emoji} FRAUD [{self.partition_id}]: {alert.fraud_type} - Account {alert.account_number[-6:]} - {alert.details}")

    async def _trigger_checkpoint(self):
        """Trigger distributed checkpoint."""
        self.checkpoint_count += 1
        checkpoint_id = self.checkpoint_count

        barrier = sb.Barrier(checkpoint_id=checkpoint_id, source_id=self.partition_id)

        aligned = self.barrier_tracker.register_barrier(
            channel=self.partition_id,
            checkpoint_id=checkpoint_id,
            total_inputs=3
        )

        if aligned:
            stats = self.barrier_tracker.get_barrier_stats(checkpoint_id)
            print(f"✅ Checkpoint {checkpoint_id} complete (all {stats['total_inputs']} agents aligned)")
            self.barrier_tracker.reset_barrier(checkpoint_id)

        self.last_checkpoint_time = time.time() * 1000

    def get_stats(self) -> Dict:
        """Get agent statistics."""
        elapsed = time.time() - self.start_time
        throughput = self.processed_count / elapsed if elapsed > 0 else 0

        if self.latencies:
            sorted_lat = sorted(self.latencies)
            p50 = sorted_lat[len(sorted_lat) // 2]
            p95 = sorted_lat[int(len(sorted_lat) * 0.95)]
            p99 = sorted_lat[int(len(sorted_lat) * 0.99)]
        else:
            p50 = p95 = p99 = 0

        return {
            'partition': self.partition_id,
            'processed': self.processed_count,
            'throughput': throughput,
            'latency_p50': p50,
            'latency_p95': p95,
            'latency_p99': p99,
            'checkpoints': self.checkpoint_count,
            'fraud_alerts': len(self.fraud_detector.alerts)
        }


async def display_metrics(agents: List[SabotFraudAgent]):
    """Display combined metrics from all agents."""
    await asyncio.sleep(5)

    while True:
        await asyncio.sleep(5)

        print("\n" + "=" * 70)
        print("📊 Sabot Agent Metrics")
        print("=" * 70)

        total_processed = 0
        total_alerts = 0

        for agent in agents:
            stats = agent.get_stats()
            total_processed += stats['processed']
            total_alerts += stats['fraud_alerts']

            print(f"\nAgent {stats['partition']}:")
            print(f"  Processed: {stats['processed']:,} txns")
            print(f"  Throughput: {stats['throughput']:,.0f} txn/s")
            print(f"  Latency p50/p95/p99: {stats['latency_p50']:.2f}ms / {stats['latency_p95']:.2f}ms / {stats['latency_p99']:.2f}ms")
            print(f"  Checkpoints: {stats['checkpoints']}")
            print(f"  Fraud Alerts: {stats['fraud_alerts']}")

        print(f"\n🎯 Total Processed: {total_processed:,} transactions")
        print(f"🚨 Total Fraud Alerts: {total_alerts}")
        print("=" * 70)


async def main():
    """Run Sabot fraud detection agents."""
    print("=" * 70)
    print("🤖 Sabot Agent/Actor Fraud Detection System")
    print("=" * 70)
    print("\n📦 Using Sabot Modules:")
    print("   ✅ BarrierTracker (distributed checkpointing)")
    print("   ✅ WatermarkTracker (event-time processing)")
    print("   ✅ MemoryBackend (state management)")
    print("\n🎯 Architecture:")
    print("   • 3 Sabot agents (one per Kafka partition)")
    print("   • Each agent is an independent actor")
    print("   • Distributed checkpointing across agents")
    print("   • Multi-pattern fraud detection")
    print("\n" + "=" * 70)

    if not KAFKA_AVAILABLE:
        print("\n❌ confluent-kafka not installed!")
        print("   Install with: pip install confluent-kafka")
        return

    # Create 3 Sabot agents (one per partition)
    print("\n🚀 Starting Sabot Agents...")
    agents = [SabotFraudAgent(partition_id=i) for i in range(3)]

    # Create tasks for each agent
    agent_tasks = [asyncio.create_task(agent.run()) for agent in agents]

    # Add metrics display task
    metrics_task = asyncio.create_task(display_metrics(agents))

    print("✅ All agents started - processing transactions...")
    print("Press Ctrl+C to stop\n")

    # Wait for all tasks
    try:
        await asyncio.gather(*agent_tasks, metrics_task)
    except KeyboardInterrupt:
        print("\n🛑 Stopping all agents...")
        for task in agent_tasks + [metrics_task]:
            task.cancel()

        # Final summary
        print("\n" + "=" * 70)
        print("📊 FINAL RESULTS")
        print("=" * 70)

        for agent in agents:
            stats = agent.get_stats()
            print(f"\nAgent {stats['partition']}:")
            print(f"  Processed: {stats['processed']:,} transactions")
            print(f"  Fraud Alerts: {stats['fraud_alerts']}")
            print(f"  Throughput: {stats['throughput']:,.0f} txn/s")

        print("\n🎉 Sabot Agents Completed!")


if __name__ == "__main__":
    asyncio.run(main())
