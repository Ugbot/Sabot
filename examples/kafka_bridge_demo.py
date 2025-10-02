#!/usr/bin/env python3
"""
Kafka Bridge Demo for Sabot

Demonstrates Kafka's role in bridging jobs/systems and acting as source/sink,
while using Tonbo for high-performance analytical storage.

This showcases:
- Kafka as a bridge between processing jobs
- Kafka as a durable source/sink for external systems
- Tonbo for analytical workloads requiring Arrow performance
- Seamless integration of multiple backends in a single pipeline
"""

import asyncio
import time
from pathlib import Path
import sys
import os

# Add sabot to path
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

try:
    from sabot.stores import StateStoreManager, BackendType
    from sabot.stores.base import StoreBackendConfig
    KAFKA_AVAILABLE = True
except ImportError as e:
    print(f"Missing dependencies: {e}")
    print("Install: pip install sabot aiokafka")
    KAFKA_AVAILABLE = False


async def simulate_external_system_producer(kafka_manager):
    """
    Simulate an external system producing data to Kafka.

    This represents a real-world scenario where external systems
    send data to Kafka as a source.
    """
    print("🔄 Simulating external system producing to Kafka...")

    # Simulate different types of events from external systems
    events = [
        {"event_type": "user_action", "user_id": "user_123", "action": "login", "timestamp": time.time()},
        {"event_type": "payment", "user_id": "user_456", "amount": 99.99, "currency": "USD"},
        {"event_type": "api_call", "service": "checkout", "response_time": 245, "status": 200},
        {"event_type": "system_metric", "metric": "cpu_usage", "value": 78.5, "host": "web-01"},
        {"event_type": "error_log", "level": "ERROR", "message": "Database timeout", "service": "payment"},
    ]

    for i, event in enumerate(events):
        topic = f"external.{event['event_type']}"
        key = f"event_{i}"

        await kafka_manager.send_to_topic(topic, key, event)
        print(f"   📤 Sent {event['event_type']} event to {topic}")
        await asyncio.sleep(0.1)  # Simulate realistic timing

    print("✅ External system simulation complete")


async def job_bridging_example(kafka_manager):
    """
    Demonstrate job-to-job bridging using Kafka.

    This shows how Kafka acts as the communication layer between
    different processing stages/jobs in a streaming pipeline.
    """
    print("\n🔗 Demonstrating job-to-job bridging...")

    # Job 1: Raw data ingestion and basic filtering
    async def ingestion_job():
        print("   👷 Job 1: Ingesting and filtering raw events...")

        filtered_events = []
        async for key, value, metadata in kafka_manager.consume_topic("external.user_action", timeout_ms=5000):
            # Filter for login events only
            if value.get("action") == "login":
                # Transform for next job
                processed_event = {
                    "user_id": value["user_id"],
                    "login_time": value["timestamp"],
                    "source_topic": metadata["topic"],
                    "processed_by": "ingestion_job"
                }
                filtered_events.append(processed_event)

        # Send to next job via Kafka bridge
        for event in filtered_events:
            await kafka_manager.send_to_topic("processed.logins", f"user_{event['user_id']}", event)
            print(f"      ✅ Processed login for {event['user_id']}")

    # Job 2: User session analysis
    async def analysis_job():
        print("   👷 Job 2: Analyzing user sessions...")

        user_sessions = {}
        async for key, value, metadata in kafka_manager.consume_topic("processed.logins", timeout_ms=5000):
            user_id = value["user_id"]
            login_time = value["login_time"]

            if user_id not in user_sessions:
                user_sessions[user_id] = {"logins": 0, "first_login": login_time, "last_login": login_time}

            user_sessions[user_id]["logins"] += 1
            user_sessions[user_id]["last_login"] = max(user_sessions[user_id]["last_login"], login_time)

            # Calculate session duration
            session_duration = user_sessions[user_id]["last_login"] - user_sessions[user_id]["first_login"]

            # Send analysis results
            analysis_result = {
                "user_id": user_id,
                "total_logins": user_sessions[user_id]["logins"],
                "session_duration_hours": session_duration / 3600,
                "analyzed_by": "analysis_job",
                "timestamp": time.time()
            }

            await kafka_manager.send_to_topic("analytics.user_sessions", f"session_{user_id}", analysis_result)
            print(f"      📊 Analyzed session for {user_id}: {analysis_result['total_logins']} logins")

    # Run jobs concurrently (simulating distributed job processing)
    await asyncio.gather(ingestion_job(), analysis_job())

    print("✅ Job bridging demonstration complete")


async def tonbo_analytical_processing(tonbo_manager):
    """
    Demonstrate high-performance analytical processing using Tonbo.

    This shows how analytical workloads benefit from Tonbo's Arrow integration.
    """
    print("\n⚡ Demonstrating Tonbo analytical processing...")

    # First, bridge data from Kafka to Tonbo for analysis
    print("   🔄 Bridging Kafka analytics data to Tonbo...")

    bridged_records = []
    async for key, value, metadata in tonbo_manager.consume_topic("analytics.user_sessions", timeout_ms=5000):
        bridged_records.append(value)

    print(f"   📊 Bridged {len(bridged_records)} user session records to Tonbo")

    # Store in Tonbo for Arrow-based analytics
    for record in bridged_records:
        await tonbo_manager.set(f"session_{record['user_id']}", record)

    # Perform Arrow-based analytics
    print("   📈 Running Arrow-based analytics on Tonbo data...")

    # Get data as Arrow table for columnar processing
    arrow_table = await tonbo_manager.scan_as_arrow_table("default")
    if arrow_table:
        print(f"      Retrieved {arrow_table.num_rows} rows, {arrow_table.column_names} columns")

        # Filter for high-activity users
        import pyarrow.compute as pc
        high_activity = pc.filter(arrow_table, pc.greater(arrow_table.column('total_logins'), 0))
        print(f"      High activity users: {high_activity.num_rows}")

        # Aggregate session analytics
        session_analytics = await tonbo_manager.arrow_aggregate(
            "default",
            group_by=["user_id"],
            aggregations={"session_duration_hours": "mean", "total_logins": "sum"}
        )

        if session_analytics:
            print("      Session analytics by user:")
            for batch in session_analytics.to_batches():
                for row in zip(*[batch.column(i).to_pylist() for i in range(batch.num_columns)]):
                    print(f"        User {row[0]}: {row[2]} total logins, {row[1]:.2f}h avg session")

    print("✅ Tonbo analytical processing complete")


async def end_to_end_pipeline(kafka_manager, tonbo_manager):
    """
    Demonstrate a complete pipeline: External Source → Kafka → Processing → Tonbo Analytics.
    """
    print("\n🚀 Demonstrating end-to-end pipeline...")

    # Step 1: External system produces data to Kafka
    await simulate_external_system_producer(kafka_manager)

    # Step 2: Job bridging between processing stages
    await job_bridging_example(kafka_manager)

    # Step 3: Analytical processing with Tonbo
    await tonbo_analytical_processing(tonbo_manager)

    # Step 4: Export results for external consumption
    print("   📤 Exporting final results as Arrow dataset...")
    with tempfile.TemporaryDirectory() as temp_dir:
        export_path = Path(temp_dir) / "pipeline_results"

        success = await tonbo_manager.arrow_export_dataset(
            "default",
            str(export_path),
            partitioning={"columns": ["user_id"]}
        )

        if success:
            print(f"      ✅ Results exported to {export_path}")
            # List exported files
            parquet_files = list(export_path.rglob("*.parquet"))
            print(f"      📁 Created {len(parquet_files)} partitioned Parquet files")
        else:
            print("      ❌ Export failed")

    print("✅ End-to-end pipeline complete")


async def performance_comparison(kafka_manager, tonbo_manager):
    """
    Compare Kafka vs Tonbo for different workloads.
    """
    print("\n⚖️  Performance comparison: Kafka vs Tonbo...")

    # Test data
    test_data = [{"id": i, "value": f"test_value_{i}", "timestamp": time.time() + i}
                 for i in range(1000)]

    # Kafka performance (messaging)
    print("   Testing Kafka (messaging) performance...")
    kafka_start = time.time()
    for item in test_data:
        await kafka_manager.send_to_topic("perf_test.kafka", f"key_{item['id']}", item)
    kafka_write_time = time.time() - kafka_start

    kafka_read_start = time.time()
    count = 0
    async for key, value, metadata in kafka_manager.consume_topic("perf_test.kafka", timeout_ms=1000):
        count += 1
        if count >= len(test_data):
            break
    kafka_read_time = time.time() - kafka_read_start

    # Tonbo performance (analytical)
    print("   Testing Tonbo (analytical) performance...")
    tonbo_write_start = time.time()
    for item in test_data:
        await tonbo_manager.set(f"perf_key_{item['id']}", item)
    tonbo_write_time = time.time() - tonbo_write_start

    tonbo_read_start = time.time()
    arrow_table = await tonbo_manager.scan_as_arrow_table("default")
    tonbo_read_time = time.time() - tonbo_read_start

    # Results
    print("
📊 Performance Results:"    print(".3f"    print(".3f"    print(".3f"    print(".3f"
    if arrow_table:
        print(f"      Tonbo Arrow processing: {arrow_table.num_rows} rows processed")

    print("
💡 Use Cases:"    print("      Kafka: Job bridging, external system integration, ordered event streams")
    print("      Tonbo: Analytical queries, Arrow operations, high-performance storage")


async def main():
    """Main demonstration of Kafka bridging and Tonbo analytics."""
    print("🚀 Sabot Kafka Bridge & Tonbo Analytics Demo")
    print("=" * 60)

    if not KAFKA_AVAILABLE:
        print("❌ Kafka dependencies not available")
        print("Install: pip install aiokafka")
        return

    # Check if Kafka is running
    try:
        import socket
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(1)
        result = sock.connect_ex(('localhost', 9092))
        sock.close()

        if result != 0:
            print("⚠️  Kafka not running on localhost:9092")
            print("Please start Kafka for full demo functionality")
            print("Continuing with simulated operations...\n")

    except Exception:
        pass

    # Configure Kafka backend for bridging
    kafka_config = StoreBackendConfig(
        backend_type="kafka",
        options={
            'bootstrap_servers': 'localhost:9092',
            'group_id': 'sabot-bridge-demo',
            'topic_prefix': 'demo.',
        }
    )

    # Configure Tonbo backend for analytics
    tonbo_config = StoreBackendConfig(
        backend_type="tonbo",
        path=Path("./demo_tonbo_analytics")
    )

    # Create managers for different backends
    kafka_manager = StateStoreManager({
        'backend_type': BackendType.KAFKA,
        'kafka_config': kafka_config
    })

    tonbo_manager = StateStoreManager({
        'backend_type': BackendType.TONBO,
        'tonbo_config': tonbo_config
    })

    try:
        # Initialize both backends
        print("🔧 Initializing backends...")
        await kafka_manager.start()
        await tonbo_manager.start()
        print("✅ Backends initialized")

        # Run demonstrations
        await end_to_end_pipeline(kafka_manager, tonbo_manager)
        await performance_comparison(kafka_manager, tonbo_manager)

        # Summary
        print("\n🎉 Demo completed successfully!")
        print("=" * 60)
        print("Key Takeaways:")
        print("• Kafka excels at bridging jobs/systems and external integration")
        print("• Tonbo provides superior performance for analytical workloads")
        print("• Both backends work together for complete streaming solutions")
        print("• Use Kafka when you need: ordering, durability, external connectivity")
        print("• Use Tonbo when you need: speed, Arrow analytics, embedded operation")

    except Exception as e:
        print(f"❌ Demo failed: {e}")
        import traceback
        traceback.print_exc()

    finally:
        # Clean up
        await kafka_manager.stop()
        await tonbo_manager.stop()


if __name__ == "__main__":
    asyncio.run(main())
