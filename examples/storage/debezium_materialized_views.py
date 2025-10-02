#!/usr/bin/env python3
import sys
import os

# Add sabot to path
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

"""
Debezium Materialized Views Demo - Native CDC Integration

This example demonstrates Sabot's native Debezium integration for
real-time materialized views that automatically stay in sync with
database changes via CDC (Change Data Capture).
"""

import asyncio
import json
import time
from typing import Dict, Any

import sabot as sb


async def simulate_debezium_events(app: sb.App) -> None:
    """Simulate Debezium change events from a database."""
    print("🔄 Simulating Debezium change events...")

    # Get materialized view manager
    mv_manager = app.materialized_views("./demo_mv_db")

    # Create materialized views
    await create_user_analytics_view(mv_manager)
    await create_inventory_view(mv_manager)

    # Start all views
    await mv_manager.start_all()

    print("📊 Materialized views initialized")

    # Simulate Debezium events (normally these would come from Kafka)
    await simulate_user_events(mv_manager)
    await simulate_inventory_events(mv_manager)

    # Query the materialized views
    await query_materialized_views(mv_manager)

    # Clean up
    await mv_manager.stop_all()


async def create_user_analytics_view(mv_manager: sb.MaterializedViewManager) -> None:
    """Create a user analytics materialized view."""
    print("📈 Creating user analytics materialized view...")

    user_view = await mv_manager.create_aggregation_view(
        name="user_analytics",
        source_topic="dbserver1.inventory.users",  # Debezium topic naming
        key_field="id",
        aggregations={
            "login_count": "count",
            "total_session_time": "sum",
            "last_login": "max"
        },
        group_by_fields=["user_id", "date"]
    )

    print("✅ User analytics view created")


async def create_inventory_view(mv_manager: sb.MaterializedViewManager) -> None:
    """Create an inventory tracking materialized view."""
    print("📦 Creating inventory tracking materialized view...")

    inventory_view = await mv_manager.create_aggregation_view(
        name="inventory_levels",
        source_topic="dbserver1.inventory.products",  # Debezium topic
        key_field="product_id",
        aggregations={
            "quantity": "sum",
            "total_value": "sum",
            "last_updated": "max"
        }
    )

    print("✅ Inventory tracking view created")


async def simulate_user_events(mv_manager: sb.MaterializedViewManager) -> None:
    """Simulate user login events as Debezium change events."""
    print("👤 Simulating user login events...")

    # Simulate Debezium messages that would come from Kafka
    user_events = [
        # User login events
        {
            "topic": "dbserver1.inventory.users",
            "value": {
                "payload": {
                    "op": "c",  # create
                    "after": {
                        "id": 1,
                        "user_id": "alice",
                        "login_count": 1,
                        "session_time": 45,
                        "last_login": "2024-01-15T10:30:00Z",
                        "date": "2024-01-15"
                    }
                }
            }
        },
        {
            "topic": "dbserver1.inventory.users",
            "value": {
                "payload": {
                    "op": "u",  # update
                    "before": {
                        "id": 1,
                        "user_id": "alice",
                        "login_count": 1,
                        "session_time": 45,
                        "last_login": "2024-01-15T10:30:00Z",
                        "date": "2024-01-15"
                    },
                    "after": {
                        "id": 1,
                        "user_id": "alice",
                        "login_count": 2,
                        "session_time": 90,
                        "last_login": "2024-01-15T11:15:00Z",
                        "date": "2024-01-15"
                    }
                }
            }
        },
        {
            "topic": "dbserver1.inventory.users",
            "value": {
                "payload": {
                    "op": "c",  # create
                    "after": {
                        "id": 2,
                        "user_id": "bob",
                        "login_count": 1,
                        "session_time": 30,
                        "last_login": "2024-01-15T14:20:00Z",
                        "date": "2024-01-15"
                    }
                }
            }
        }
    ]

    # Process each Debezium event
    for event in user_events:
        await mv_manager.process_debezium_message(event)
        await asyncio.sleep(0.1)  # Simulate processing time

    print("✅ User events processed")


async def simulate_inventory_events(mv_manager: sb.MaterializedViewManager) -> None:
    """Simulate inventory change events as Debezium change events."""
    print("📦 Simulating inventory change events...")

    inventory_events = [
        # Product inventory updates
        {
            "topic": "dbserver1.inventory.products",
            "value": {
                "payload": {
                    "op": "c",
                    "after": {
                        "product_id": "widget-a",
                        "name": "Widget A",
                        "quantity": 100,
                        "price": 19.99,
                        "total_value": 1999.00,
                        "last_updated": "2024-01-15T09:00:00Z"
                    }
                }
            }
        },
        {
            "topic": "dbserver1.inventory.products",
            "value": {
                "payload": {
                    "op": "u",
                    "before": {
                        "product_id": "widget-a",
                        "name": "Widget A",
                        "quantity": 100,
                        "price": 19.99,
                        "total_value": 1999.00,
                        "last_updated": "2024-01-15T09:00:00Z"
                    },
                    "after": {
                        "product_id": "widget-a",
                        "name": "Widget A",
                        "quantity": 80,  # Sold 20 units
                        "price": 19.99,
                        "total_value": 1599.20,
                        "last_updated": "2024-01-15T10:30:00Z"
                    }
                }
            }
        },
        {
            "topic": "dbserver1.inventory.products",
            "value": {
                "payload": {
                    "op": "c",
                    "after": {
                        "product_id": "gadget-x",
                        "name": "Gadget X",
                        "quantity": 50,
                        "price": 49.99,
                        "total_value": 2499.50,
                        "last_updated": "2024-01-15T11:00:00Z"
                    }
                }
            }
        }
    ]

    # Process each inventory event
    for event in inventory_events:
        await mv_manager.process_debezium_message(event)
        await asyncio.sleep(0.1)

    print("✅ Inventory events processed")


async def query_materialized_views(mv_manager: sb.MaterializedViewManager) -> None:
    """Query and display results from materialized views."""
    print("\n🔍 Querying materialized views...")

    # Query user analytics view
    user_view = mv_manager.get_view("user_analytics")
    if user_view:
        print("\n👤 User Analytics View:")
        user_data = await user_view.query()
        for record in user_data:
            print(f"  User {record.get('user_id')}: {record.get('login_count', 0)} logins, "
                  f"{record.get('total_session_time', 0)}s total session time")

        # Get view stats
        stats = await user_view.get_stats()
        print(f"  View stats: {stats['record_count']} records")

    # Query inventory view
    inventory_view = mv_manager.get_view("inventory_levels")
    if inventory_view:
        print("\n📦 Inventory Levels View:")
        inventory_data = await inventory_view.query()
        total_value = 0
        total_quantity = 0

        for record in inventory_data:
            qty = record.get('quantity', 0)
            value = record.get('total_value', 0)
            total_quantity += qty
            total_value += value
            print(f"  {record.get('name', 'Unknown')}: {qty} units, ${value:.2f}")

        print(f"  Total inventory: {total_quantity} units, ${total_value:.2f}")

        # Get view stats
        stats = await inventory_view.get_stats()
        print(f"  View stats: {stats['record_count']} records")

    # List all views
    print("\n📋 All Materialized Views:")
    all_views = await mv_manager.list_views()
    for view_info in all_views:
        print(f"  {view_info['name']}: {view_info['stats']['record_count']} records")


async def demonstrate_real_debezium_integration() -> None:
    """Show how to integrate with real Debezium/Kafka."""
    print("\n🔗 Real Debezium Integration Example:")
    print("""
# To integrate with real Debezium:

from sabot.materialized_views import DebeziumChangeStream

# 1. Create app and materialized views
app = sb.create_app("debezium-app")
mv_manager = app.materialized_views("./rocksdb_views")

# Create views (same as above)
user_view = await mv_manager.create_aggregation_view(
    name="user_analytics",
    source_topic="dbserver1.inventory.users",
    aggregations={"login_count": "count", "session_time": "sum"}
)

# 2. Set up Debezium change stream
change_stream = DebeziumChangeStream(
    topic="dbserver1.inventory.*",  # Wildcard for all tables
    view_manager=mv_manager
)

# 3. Start consuming from Kafka
await change_stream.start_consuming(
    bootstrap_servers="localhost:9092",
    group_id="sabot-debezium-consumer"
)

# Views automatically stay in sync with database changes!
# No manual ETL pipelines needed.
""")


async def demonstrate_custom_views() -> None:
    """Show how to create custom materialized view logic."""
    print("\n🛠️  Custom Materialized View Example:")
    print("""
# Create a custom view with business logic

from sabot.materialized_views import MaterializedView, ViewType

class CustomAnalyticsView(MaterializedView):
    async def process_change_event(self, event):
        # Custom logic for complex analytics
        if event['operation'] == 'INSERT':
            # Calculate user engagement score
            engagement = self._calculate_engagement(event['data'])
            event['data']['engagement_score'] = engagement

        # Call parent processing
        await super().process_change_event(event)

    def _calculate_engagement(self, user_data):
        # Complex business logic
        logins = user_data.get('login_count', 0)
        session_time = user_data.get('session_time', 0)
        return (logins * 0.3) + (session_time * 0.001)

# Use the custom view
custom_view = CustomAnalyticsView(
    name="custom_analytics",
    view_type=ViewType.CUSTOM,
    source_topic="user-events",
    db_path="./custom_views"
)
""")


async def main():
    """Run the Debezium materialized views demo."""
    print("🗄️  Sabot Debezium Materialized Views Demo")
    print("=" * 50)

    app = sb.create_app("debezium-mv-demo")

    try:
        await app.start()

        # Run the main demo
        await simulate_debezium_events(app)

        # Show integration examples
        await demonstrate_real_debezium_integration()
        await demonstrate_custom_views()

    except KeyboardInterrupt:
        print("\n🛑 Demo interrupted by user")
    except Exception as e:
        print(f"\n❌ Demo failed: {e}")
        import traceback
        traceback.print_exc()
    finally:
        await app.stop()

    print("\n✅ Debezium materialized views demo completed!")
    print("\nKey Features Demonstrated:")
    print("• 🗄️  RocksDB-backed materialized views")
    print("• 🔄 Native Debezium CDC integration")
    print("• ⚡ Real-time view maintenance")
    print("• 📊 Automatic aggregation updates")
    print("• 🔍 Efficient view querying")
    print("• 🏗️  Extensible custom view logic")


if __name__ == "__main__":
    asyncio.run(main())
