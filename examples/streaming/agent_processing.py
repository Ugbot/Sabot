#!/usr/bin/env python3
"""
Agent-Based Processing Example

This example demonstrates Sabot's agent-based architecture for stateful,
asynchronous stream processing. It shows:
- Defining agents with state
- Processing messages asynchronously
- Maintaining state between messages
- Real-time fraud detection simulation

Prerequisites:
- Kafka/Redpanda running: docker compose up -d
- Sabot installed: pip install -e .

Usage:
    # Start the worker
    sabot -A examples.streaming.agent_processing:app worker

    # Send test data (separate terminal)
    python -c "
    from confluent_kafka import Producer
    import json, random, time

    producer = Producer({'bootstrap.servers': 'localhost:19092'})
    users = [f'user_{i}' for i in range(1, 6)]

    for i in range(50):
        # Occasional large/fraudulent transactions
        if random.random() < 0.05:
            amount = round(random.uniform(5000, 10000), 2)
        elif random.random() < 0.1:
            amount = round(random.uniform(500, 2000), 2)
        else:
            amount = round(random.uniform(10, 100), 2)

        txn = {
            'user_id': random.choice(users),
            'amount': amount,
            'timestamp': time.time(),
            'merchant': f'merchant_{random.randint(1, 50)}',
            'location': f'city_{random.randint(1, 20)}'
        }
        producer.produce('transactions', value=json.dumps(txn).encode())
        producer.flush()
        print(f'Sent: {txn}')
        time.sleep(0.5)
    "
"""

import sabot as sb
import time

# Create Sabot application
app = sb.App(
    'fraud-detection',
    broker='kafka://localhost:19092',
    value_serializer='json'
)


class FraudDetector:
    """Stateful fraud detection logic."""

    def __init__(self):
        self.user_transactions = {}
        self.suspicious_patterns = {}

    def check_fraud(self, user_id, amount, timestamp):
        """Check if transaction looks fraudulent."""
        if user_id not in self.user_transactions:
            self.user_transactions[user_id] = []

        # Keep last 10 transactions for this user
        transactions = self.user_transactions[user_id]
        transactions.append((amount, timestamp))

        if len(transactions) > 10:
            transactions.pop(0)

        # Simple fraud detection rules
        if len(transactions) >= 3:
            recent_amounts = [t[0] for t in transactions[-3:]]
            avg_recent = sum(recent_amounts) / len(recent_amounts)

            # Flag if amount is 3x higher than recent average
            if amount > avg_recent * 3:
                return "HIGH_RISK"

            # Flag if amount is 2x higher than recent average
            if amount > avg_recent * 2:
                return "MEDIUM_RISK"

        return "LOW_RISK"

    def get_stats(self):
        """Get detection statistics."""
        total_users = len(self.user_transactions)
        total_transactions = sum(len(txns) for txns in self.user_transactions.values())
        return {
            "total_users": total_users,
            "total_transactions": total_transactions
        }


# Create fraud detector instance (shared state across agent calls)
detector = FraudDetector()


@app.agent('transactions')
async def fraud_detection_agent(stream):
    """
    Agent that processes transactions and detects fraud.

    Maintains stateful fraud detection across all user transactions.
    """
    print("🕵️  Fraud Detection Agent started")
    print("💳 Monitoring transactions for suspicious patterns")
    print("🚨 High-risk transactions will trigger alerts\n")

    alert_count = 0
    processed_count = 0

    async for transaction in stream:
        try:
            user_id = transaction.get("user_id")
            amount = transaction.get("amount", 0)
            timestamp = transaction.get("timestamp", time.time())

            processed_count += 1

            # Use stateful fraud detection
            risk_level = detector.check_fraud(user_id, amount, timestamp)

            # Log all transactions
            print(f"💳 Transaction: {user_id} - ${amount:.2f} ({risk_level})")

            # Create alert for suspicious transactions
            if risk_level in ["HIGH_RISK", "MEDIUM_RISK"]:
                alert_count += 1

                alert = {
                    "alert_id": f"alert_{int(timestamp)}_{user_id}",
                    "user_id": user_id,
                    "amount": amount,
                    "risk_level": risk_level,
                    "timestamp": timestamp,
                    "merchant": transaction.get("merchant"),
                    "location": transaction.get("location")
                }

                emoji = "🚨" if risk_level == "HIGH_RISK" else "⚠️ "
                print(f"{emoji} FRAUD ALERT [{risk_level}]: "
                      f"{user_id} - ${amount:.2f} at {alert['merchant']}")

                yield alert

            # Periodic stats
            if processed_count % 20 == 0:
                stats = detector.get_stats()
                print(f"\n📊 Stats: {processed_count} transactions processed, "
                      f"{alert_count} alerts generated")
                print(f"   Tracking {stats['total_users']} users, "
                      f"{stats['total_transactions']} total transactions\n")

        except Exception as e:
            print(f"❌ Error processing transaction: {e}")
            continue


if __name__ == "__main__":
    print(__doc__)
