#!/usr/bin/env python3
"""
Fraud Detection Sabot App
==========================

CLI-compatible Sabot application for real-time fraud detection.

Usage:
    # Start with CLI
    sabot -A examples.fraud_app:app worker --loglevel=INFO

    # Or with concurrency
    sabot -A examples.fraud_app:app worker -c 3 --loglevel=INFO
"""

import asyncio
import json
import math
import time
from collections import defaultdict, deque
from typing import Dict, List, Any, Optional
from dataclasses import dataclass

# Sabot modules (clean API)
import sabot as sb

# Kafka imports
try:
    from confluent_kafka import Consumer
    KAFKA_AVAILABLE = True
except ImportError:
    print("⚠️  confluent-kafka not installed. Install with: pip install confluent-kafka")
    KAFKA_AVAILABLE = False


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
    """Multi-pattern fraud detection using Sabot state stores."""

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


# ============================================================================
# Sabot Application
# ============================================================================

# Create Sabot App
app = sb.App(
    'fraud-detection',
    broker='kafka://localhost:19092',
    value_serializer='json',
    key_serializer='raw'
)

# Shared fraud detector instance
detector = FraudDetector()

# Metrics tracking
metrics = {
    'processed_count': 0,
    'alert_count': 0,
    'start_time': time.time(),
    'latencies': deque(maxlen=10000)
}


@app.agent('bank-transactions')
async def fraud_detection_agent(stream):
    """
    Sabot agent for fraud detection.

    Consumes from 'bank-transactions' Kafka topic and detects fraud patterns.
    """
    print("✅ Fraud detection agent started")

    # Start metrics display on first message
    await _ensure_metrics_started()

    async for message in stream:
        try:
            # Parse transaction
            if isinstance(message, bytes):
                txn_data = json.loads(message.decode('utf-8'))
            elif isinstance(message, str):
                txn_data = json.loads(message)
            else:
                txn_data = message

            txn = BankTransaction(**txn_data)

            # Process transaction
            process_start = time.time()
            alerts = await detector.detect_fraud(txn)
            process_time = (time.time() - process_start) * 1000

            metrics['latencies'].append(process_time)
            metrics['processed_count'] += 1

            # Display fraud alerts
            for alert in alerts:
                metrics['alert_count'] += 1
                _display_alert(alert)

        except Exception as e:
            print(f"❌ Error processing transaction: {e}")
            continue


def _display_alert(alert: FraudAlert):
    """Display fraud alert."""
    severity_emoji = {
        'LOW': '⚠️', 'MEDIUM': '🟡', 'HIGH': '🟠', 'CRITICAL': '🚨'
    }
    emoji = severity_emoji.get(alert.severity, '⚠️')
    print(f"{emoji} FRAUD: {alert.fraud_type} - Account {alert.account_number[-6:]} - {alert.details}")


async def display_metrics_task():
    """Background task to display metrics periodically."""
    await asyncio.sleep(10)  # Wait for initial data

    while True:
        await asyncio.sleep(10)

        elapsed = time.time() - metrics['start_time']
        throughput = metrics['processed_count'] / elapsed if elapsed > 0 else 0

        if metrics['latencies']:
            sorted_lat = sorted(metrics['latencies'])
            p50 = sorted_lat[len(sorted_lat) // 2]
            p95 = sorted_lat[int(len(sorted_lat) * 0.95)]
            p99 = sorted_lat[int(len(sorted_lat) * 0.99)]
        else:
            p50 = p95 = p99 = 0

        print("\n" + "=" * 70)
        print("📊 Fraud Detection Metrics")
        print("=" * 70)
        print(f"  Processed: {metrics['processed_count']:,} transactions")
        print(f"  Throughput: {throughput:,.0f} txn/s")
        print(f"  Latency p50/p95/p99: {p50:.2f}ms / {p95:.2f}ms / {p99:.2f}ms")
        print(f"  Fraud Alerts: {metrics['alert_count']}")
        print("=" * 70 + "\n")


# Print startup banner
print("=" * 70)
print("🤖 Sabot Fraud Detection System")
print("=" * 70)
print("\n📦 Using Sabot Modules:")
print("   ✅ BarrierTracker (distributed checkpointing)")
print("   ✅ WatermarkTracker (event-time processing)")
print("   ✅ MemoryBackend (state management)")
print("\n🎯 Architecture:")
print("   • Sabot App API with @app.agent decorator")
print("   • Automatic Kafka consumer management")
print("   • Multi-pattern fraud detection")
print("   • Clean CLI integration")
print("\n" + "=" * 70)
print("✅ Initializing...")
print("💡 Use CLI: sabot -A examples.fraud_app:app worker\n")

# Note: metrics_task will be started automatically when first message arrives
_metrics_task_started = False

async def _ensure_metrics_started():
    """Start metrics display task on first message."""
    global _metrics_task_started
    if not _metrics_task_started:
        _metrics_task_started = True
        asyncio.create_task(display_metrics_task())
