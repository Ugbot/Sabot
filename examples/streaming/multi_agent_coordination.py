#!/usr/bin/env python3
"""
Multi-Agent Coordination Example

This example demonstrates Sabot's multi-agent coordination capabilities.
It shows:
- Multiple agents working together in a pipeline
- Inter-agent communication via Kafka topics
- Distributed processing patterns
- Real-time collaborative analytics

Prerequisites:
- Kafka/Redpanda running: docker compose up -d
- Sabot installed: pip install -e .

Usage:
    # Start the worker (all agents run together)
    sabot -A examples.streaming.multi_agent_coordination:app worker

    # Send test data (separate terminal - producer at end of file)
    python examples/streaming/multi_agent_producer.py
"""

import sabot as sb
import time
import random

# Create Sabot application
app = sb.App(
    'customer-analytics',
    broker='kafka://localhost:19092',
    value_serializer='json'
)


# Processing coordinator for tracking stats
class ProcessingCoordinator:
    """Coordinates multiple processing agents."""

    def __init__(self):
        self.agent_stats = {
            "data_ingestion": {"processed": 0, "errors": 0},
            "validation": {"processed": 0, "valid": 0, "invalid": 0},
            "enrichment": {"processed": 0, "enriched": 0},
            "analytics": {"processed": 0, "insights": 0}
        }
        self.last_report = time.time()

    def update_stats(self, agent_name, **stats):
        """Update statistics for an agent."""
        if agent_name in self.agent_stats:
            for key, value in stats.items():
                if key in self.agent_stats[agent_name]:
                    self.agent_stats[agent_name][key] += value

    def report_status(self):
        """Report current processing status."""
        current_time = time.time()
        if current_time - self.last_report >= 10.0:  # Report every 10 seconds
            print("\n📊 Multi-Agent Processing Status:")
            for agent, stats in self.agent_stats.items():
                print(f"   {agent}: {stats}")
            print("-" * 50)
            self.last_report = current_time


coordinator = ProcessingCoordinator()


# Agent 1: Data Ingestion & Initial Processing
@app.agent('customer-interactions')
async def data_ingestion_agent(stream):
    """
    First agent: Ingest and validate incoming data.

    Outputs to: validated-interactions
    """
    print("📥 Data Ingestion agent started")

    async for interaction in stream:
        try:
            # Basic validation
            required_fields = ["customer_id", "interaction_type", "timestamp"]

            if all(field in interaction for field in required_fields):
                # Add processing metadata
                interaction["processed_at"] = time.time()
                interaction["processing_stage"] = "ingestion"

                coordinator.update_stats("data_ingestion", processed=1)

                print(f"📥 Ingested: {interaction['interaction_type']} from {interaction['customer_id']}")

                # Forward to validation agent
                yield interaction

            else:
                coordinator.update_stats("data_ingestion", errors=1)
                print(f"❌ Invalid interaction (missing fields): {interaction}")

        except Exception as e:
            coordinator.update_stats("data_ingestion", errors=1)
            print(f"❌ Error in ingestion: {e}")
            continue

        coordinator.report_status()


# Agent 2: Data Validation & Quality Assurance
@app.agent('customer-interactions')
async def validation_agent(stream):
    """
    Second agent: Validate data quality.

    Receives from: data_ingestion_agent (same topic)
    Outputs to: enriched-interactions
    """
    print("✅ Validation agent started")

    async for interaction in stream:
        try:
            coordinator.update_stats("validation", processed=1)

            # Quality checks
            is_valid = True
            issues = []

            # Timestamp validation (not in future)
            if interaction.get("timestamp", 0) > time.time() + 300:  # 5 min tolerance
                is_valid = False
                issues.append("future_timestamp")

            # Customer ID format
            if not str(interaction.get("customer_id", "")).startswith("customer_"):
                is_valid = False
                issues.append("invalid_customer_format")

            # Interaction type validation
            valid_types = ["login", "browse", "search", "purchase", "support", "feedback"]
            if interaction.get("interaction_type") not in valid_types:
                is_valid = False
                issues.append("invalid_interaction_type")

            if is_valid:
                coordinator.update_stats("validation", valid=1)
                interaction["validation_status"] = "valid"
                interaction["processing_stage"] = "validated"

                print(f"✅ Validated: {interaction['interaction_type']}")

                # Forward to enrichment
                yield interaction

            else:
                coordinator.update_stats("validation", invalid=1)
                print(f"❌ Invalid interaction: {issues}")

        except Exception as e:
            print(f"❌ Error in validation: {e}")
            continue

        coordinator.report_status()


# Agent 3: Data Enrichment & Business Logic
@app.agent('customer-interactions')
async def enrichment_agent(stream):
    """
    Third agent: Enrich data with business logic.

    Receives from: validation_agent
    Outputs to: analytics-ready
    """
    print("🎨 Enrichment agent started")

    async for interaction in stream:
        try:
            # Skip if not validated
            if interaction.get("validation_status") != "valid":
                continue

            coordinator.update_stats("enrichment", processed=1)

            # Business logic enrichment based on interaction type
            if interaction["interaction_type"] == "purchase":
                # Calculate revenue metrics
                amount = interaction.get("amount", 0)
                interaction["revenue_category"] = (
                    "high" if amount > 200 else
                    "medium" if amount > 50 else "low"
                )
                interaction["estimated_profit"] = amount * 0.3  # 30% margin

            elif interaction["interaction_type"] == "search":
                # Categorize search intent
                query = interaction.get("query", "").lower()
                if any(word in query for word in ["wireless", "bluetooth", "headphones"]):
                    interaction["search_category"] = "electronics"
                elif any(word in query for word in ["running", "yoga", "sports"]):
                    interaction["search_category"] = "sports"
                elif any(word in query for word in ["coffee", "kitchen"]):
                    interaction["search_category"] = "kitchen"
                else:
                    interaction["search_category"] = "other"

            elif interaction["interaction_type"] == "support":
                # Prioritize support issues
                issue_type = interaction.get("issue_type", "")
                priority_map = {
                    "payment_issue": "high",
                    "product_defect": "high",
                    "login_problem": "medium",
                    "shipping_delay": "medium",
                    "return_request": "low"
                }
                interaction["support_priority"] = priority_map.get(issue_type, "low")

            interaction["processing_stage"] = "enriched"
            interaction["enriched_at"] = time.time()

            coordinator.update_stats("enrichment", enriched=1)

            print(f"🎨 Enriched: {interaction['interaction_type']}")

            # Forward to analytics
            yield interaction

        except Exception as e:
            print(f"❌ Error in enrichment: {e}")
            continue

        coordinator.report_status()


# Agent 4: Analytics & Insights Generation
@app.agent('customer-interactions')
async def analytics_agent(stream):
    """
    Fourth agent: Generate analytics and insights.

    Receives from: enrichment_agent
    Final processing stage.
    """
    print("📊 Analytics agent started")

    analytics_insights = []

    async for interaction in stream:
        try:
            # Skip if not enriched
            if interaction.get("processing_stage") != "enriched":
                continue

            coordinator.update_stats("analytics", processed=1)

            # Generate insights based on interaction type
            if interaction["interaction_type"] == "purchase":
                insight = {
                    "type": "revenue",
                    "customer_id": interaction["customer_id"],
                    "amount": interaction.get("amount", 0),
                    "category": interaction.get("revenue_category"),
                    "timestamp": interaction["timestamp"]
                }
                analytics_insights.append(insight)
                coordinator.update_stats("analytics", insights=1)

                print(f"💰 Revenue insight: ${interaction.get('amount', 0)} "
                      f"({interaction.get('revenue_category')})")

            elif interaction["interaction_type"] == "support" and \
                 interaction.get("support_priority") == "high":
                insight = {
                    "type": "support_alert",
                    "customer_id": interaction["customer_id"],
                    "issue_type": interaction.get("issue_type"),
                    "priority": interaction["support_priority"],
                    "timestamp": interaction["timestamp"]
                }
                analytics_insights.append(insight)
                coordinator.update_stats("analytics", insights=1)

                print(f"🚨 HIGH PRIORITY SUPPORT: {interaction['customer_id']} - "
                      f"{interaction.get('issue_type')}")

            elif interaction["interaction_type"] == "search":
                insight = {
                    "type": "search_trend",
                    "query": interaction.get("query"),
                    "category": interaction.get("search_category"),
                    "timestamp": interaction["timestamp"]
                }
                analytics_insights.append(insight)
                coordinator.update_stats("analytics", insights=1)

            # Keep only recent insights (last 100)
            if len(analytics_insights) > 100:
                analytics_insights[:] = analytics_insights[-100:]

            # Yield insights for potential downstream processing
            yield {
                "insight_type": insight.get("type") if 'insight' in locals() else None,
                "customer_id": interaction["customer_id"],
                "interaction_type": interaction["interaction_type"]
            }

        except Exception as e:
            print(f"❌ Error in analytics: {e}")
            continue

        coordinator.report_status()


if __name__ == "__main__":
    print(__doc__)
    print("\n" + "="*60)
    print("PRODUCER CODE - Save as multi_agent_producer.py:")
    print("="*60)
    print("""
import random
import time
import json
from confluent_kafka import Producer

producer = Producer({'bootstrap.servers': 'localhost:19092'})

customers = [f"customer_{i}" for i in range(1, 21)]
interaction_types = ["login", "browse", "search", "purchase", "support", "feedback"]

print("🚀 Sending customer interaction data to Kafka...")

for i in range(50):
    customer_id = random.choice(customers)
    interaction_type = random.choice(interaction_types)

    interaction = {
        "interaction_id": f"int_{int(time.time() * 1000)}_{i}",
        "customer_id": customer_id,
        "interaction_type": interaction_type,
        "timestamp": time.time(),
        "session_id": f"session_{random.randint(10000, 99999)}",
        "user_agent": random.choice(["Chrome", "Safari", "Firefox", "Mobile"]),
        "ip_address": f"192.168.{random.randint(1, 255)}.{random.randint(1, 255)}"
    }

    # Add type-specific data
    if interaction_type == "purchase":
        interaction["amount"] = round(random.uniform(10.0, 500.0), 2)
        interaction["items"] = random.randint(1, 10)
    elif interaction_type == "search":
        interaction["query"] = random.choice([
            "wireless headphones", "running shoes", "coffee maker",
            "yoga mat", "bluetooth speaker", "laptop stand"
        ])
    elif interaction_type == "support":
        interaction["issue_type"] = random.choice([
            "login_problem", "payment_issue", "shipping_delay",
            "product_defect", "return_request"
        ])

    producer.produce('customer-interactions', value=json.dumps(interaction).encode())
    producer.flush()

    if i % 10 == 0:
        print(f"📨 Sent {i} interactions...")

    time.sleep(0.5)

print("✅ Producer complete!")
""")
