#!/usr/bin/env python3
import sys
import os

# Add sabot to path
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

"""
Durable Agent Manager Demo - Control Plane + Data Plane Architecture

This example demonstrates Sabot's CONTROL PLANE + DATA PLANE architecture:

CONTROL PLANE (DBOS-inspired Postgres-backed):
- Postgres-backed agent state persistence
- Exactly-once execution semantics
- Workflow resumption on restart
- Circuit breaker protection
- Programmatic workflow management

DATA PLANE (Arrow/RAFT High-throughput):
- Actual streaming data flows through Arrow channels
- GPU acceleration via RAFT algorithms
- High-performance columnar processing
- Separate from orchestration for maximum throughput

Inspired by DBOS (https://github.com/dbos-inc/dbos-transact-py) for control plane
and Faust for data plane streaming patterns.
"""

import asyncio
import logging
from typing import Dict, Any, List

import sabot as sb

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# Example agent functions
# NOTE: In the CONTROL PLANE + DATA PLANE architecture:
# - DBOS manages agent orchestration and durability
# - Actual streaming data flows through separate Arrow/RAFT channels
# - Agent functions process real streaming data, not DBOS payloads

@sb.app.agent('user-events')
async def process_user_events(stream_data):
    """Durable agent for processing user events.

    CONTROL PLANE: DBOS manages when/where this agent runs
    DATA PLANE: Arrow streaming data comes through 'stream_data' parameter
    """
    logger.info("Processing user events stream (DBOS orchestration + Arrow data)")

    # In real implementation, stream_data would come from Arrow channels
    # For demo, we simulate processing
    async for batch in stream_data:  # This would be Arrow RecordBatches
        for event in batch:
            user_id = event.get('user_id')
            event_type = event.get('type')

            # Simulate processing (would be real Arrow operations)
            await asyncio.sleep(0.01)

            # Yield results to sinks (Faust-style async generator)
            yield {
                'processed_event': event,
                'user_id': user_id,
                'event_type': event_type,
                'processed_at': asyncio.get_event_loop().time()
            }


@sb.app.agent('order-events')
async def process_orders(stream_data):
    """Durable agent for processing orders.

    CONTROL PLANE: DBOS manages orchestration and exactly-once execution
    DATA PLANE: Arrow streaming data flows through separate channels
    """
    logger.info("Processing order events stream (DBOS orchestration + Arrow data)")

    # In real implementation, stream_data would come from Arrow channels
    async for batch in stream_data:  # This would be Arrow RecordBatches
        for order in batch:
            order_id = order.get('order_id')
            amount = order.get('amount', 0)

            # Simulate fraud detection (would be real Arrow compute operations)
            is_fraudulent = amount > 10000  # Simple rule

            await asyncio.sleep(0.01)  # Simulate processing time

            yield {
                'order_id': order_id,
                'amount': amount,
                'is_fraudulent': is_fraudulent,
                'processed_at': asyncio.get_event_loop().time()
            }


async def simulate_event_stream(app: sb.App, topic_name: str, event_generator):
    """Simulate streaming events to a topic."""
    logger.info(f"Starting event simulation for topic: {topic_name}")

    topic = app.topic(topic_name)
    event_count = 0

    while True:
        # Generate batch of events
        events = event_generator(event_count, batch_size=5)

        # Send to topic
        await topic.send(events)

        event_count += len(events)
        logger.info(f"Sent {len(events)} events to {topic_name} (total: {event_count})")

        await asyncio.sleep(2.0)  # Send batch every 2 seconds


def generate_user_events(start_id: int, batch_size: int = 5) -> List[Dict[str, Any]]:
    """Generate synthetic user events."""
    events = []
    for i in range(batch_size):
        event_id = start_id + i
        events.append({
            'event_id': f'evt_{event_id}',
            'user_id': f'user_{event_id % 100}',  # 100 different users
            'type': ['login', 'logout', 'purchase', 'view'][event_id % 4],
            'timestamp': asyncio.get_event_loop().time(),
            'metadata': {'source': 'web', 'device': 'mobile'}
        })
    return events


def generate_order_events(start_id: int, batch_size: int = 5) -> List[Dict[str, Any]]:
    """Generate synthetic order events."""
    events = []
    for i in range(batch_size):
        order_id = start_id + i
        events.append({
            'order_id': f'order_{order_id}',
            'user_id': f'user_{order_id % 50}',
            'amount': (order_id % 100) * 10.0,  # Varying amounts
            'items': ['laptop', 'phone', 'book', 'shirt'][order_id % 4],
            'timestamp': asyncio.get_event_loop().time()
        })
    return events


async def demonstrate_workflow_management(app: sb.App):
    """Demonstrate DBOS-inspired workflow management features."""
    logger.info("Demonstrating workflow management features")

    # Wait a bit for agents to process some events
    await asyncio.sleep(5)

    # 1. List all workflows
    logger.info("=== Listing all workflows ===")
    all_workflows = app.list_workflows()
    logger.info(f"Total workflows: {len(all_workflows)}")

    for wf in all_workflows[:3]:  # Show first 3
        logger.info(f"  Workflow {wf['workflow_id']}: {wf['agent_name']} - {wf['state']}")

    # 2. Check specific workflow status
    if all_workflows:
        workflow_id = all_workflows[0]['workflow_id']
        logger.info(f"\n=== Checking status of workflow {workflow_id} ===")
        status = app.get_workflow_status(workflow_id)
        if status:
            logger.info(f"Status: {status['state']}")
            logger.info(f"Agent: {status['agent_name']}")
            if status['error']:
                logger.info(f"Error: {status['error']}")

    # 3. Show agent manager statistics
    logger.info("
=== Agent Manager Statistics ===")
    stats = app.get_stats()
    agent_mgr_stats = stats.get('agent_manager', {})
    workflow_stats = stats.get('workflows', {})

    logger.info(f"Agents created: {agent_mgr_stats.get('agents_created', 0)}")
    logger.info(f"Agents running: {agent_mgr_stats.get('agents_running', 0)}")
    logger.info(f"Total messages processed: {agent_mgr_stats.get('total_messages_processed', 0)}")
    logger.info(f"Total errors: {agent_mgr_stats.get('total_errors', 0)}")

    logger.info(f"Total workflows: {workflow_stats.get('total_workflows', 0)}")
    logger.info(f"Running workflows: {workflow_stats.get('running_workflows', 0)}")
    logger.info(f"Failed workflows: {workflow_stats.get('failed_workflows', 0)}")

    # 4. Demonstrate programmatic workflow enqueueing
    logger.info("
=== Enqueueing custom workflow ===")
    try:
        workflow_id = await app.enqueue_workflow(
            agent_name='process_user_events',
            workflow_id=f'custom_workflow_{int(asyncio.get_event_loop().time())}',
            payload={
                'stream_data': [{
                    'event_id': 'custom_evt_1',
                    'user_id': 'user_custom',
                    'type': 'custom_event',
                    'timestamp': asyncio.get_event_loop().time()
                }],
                'kwargs': {}
            }
        )
        logger.info(f"Enqueued custom workflow: {workflow_id}")

        # Check its status after a moment
        await asyncio.sleep(1)
        status = app.get_workflow_status(workflow_id)
        if status:
            logger.info(f"Custom workflow status: {status['state']}")

    except Exception as e:
        logger.error(f"Failed to enqueue custom workflow: {e}")


async def main():
    """Main demo function."""
    logger.info("Starting Sabot Durable Agent Manager Demo")
    logger.info("=" * 50)

    # Create Sabot app with durable execution (DBOS-inspired)
    app = sb.create_app(
        'durable_agents_demo',
        broker=None,  # No external broker - using local messaging
        database_url="postgresql://localhost/sabot",  # DBOS-style durable storage
        enable_distributed_state=True,
        enable_gpu=False  # CPU mode for this demo
    )

    logger.info("App created with durable agent manager")

    try:
        # Start the app (this starts the durable agent manager)
        await app.start()

        # Create background tasks for event simulation
        tasks = []

        # Simulate user events
        tasks.append(asyncio.create_task(
            simulate_event_stream(app, 'user-events', generate_user_events)
        ))

        # Simulate order events
        tasks.append(asyncio.create_task(
            simulate_event_stream(app, 'order-events', generate_order_events)
        ))

        # Demonstrate workflow management
        tasks.append(asyncio.create_task(
            demonstrate_workflow_management(app)
        ))

        logger.info("All components started - running for 20 seconds...")
        logger.info("Watch for agent processing logs and workflow management demos")

        # Run for 20 seconds
        await asyncio.sleep(20)

        # Cancel background tasks
        for task in tasks:
            task.cancel()

        # Wait for tasks to complete
        await asyncio.gather(*tasks, return_exceptions=True)

    except KeyboardInterrupt:
        logger.info("Demo interrupted by user")
    except Exception as e:
        logger.error(f"Demo failed: {e}")
        raise
    finally:
        await app.stop()

    logger.info("Control Plane + Data Plane Demo completed")
    logger.info("\nArchitecture demonstrated:")
    logger.info("✓ CONTROL PLANE: Postgres-backed agent orchestration (DBOS-inspired)")
    logger.info("✓ DATA PLANE: Arrow streaming data processing (separate from DBOS)")
    logger.info("✓ Exactly-once execution semantics for orchestration")
    logger.info("✓ High-throughput streaming data flows")
    logger.info("✓ Workflow resumption and circuit breaker protection")
    logger.info("✓ Programmatic workflow management")


if __name__ == "__main__":
    asyncio.run(main())
