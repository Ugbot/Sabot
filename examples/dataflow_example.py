# -*- coding: utf-8 -*-
"""
Example demonstrating the new @app.dataflow API.

This example shows:
1. The NEW way: @app.dataflow (recommended)
2. The OLD way: @app.agent (deprecated but still works)
3. Local mode execution with SQLite backend
"""

import asyncio
import logging
from sabot import App

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


async def main():
    """Run dataflow examples."""

    # Create app (local mode by default)
    app = App(id="dataflow_example")

    # ========================================================================
    # EXAMPLE 1: NEW @app.dataflow API (recommended)
    # ========================================================================

    @app.dataflow("order_processing")
    def order_pipeline():
        """
        Process orders: filter high-value orders, compute tax, output to Kafka.

        This defines a dataflow DAG that will be compiled to tasks
        and executed on agent worker nodes.
        """
        from sabot.api.stream import Stream
        from sabot.cyarrow import compute as pc

        return (Stream.from_kafka('localhost:9092', 'orders', 'fraud-detection')
            .filter(lambda b: pc.greater(b['amount'], 100.0))  # Filter high-value orders
            .map(lambda b: b.append_column('tax', pc.multiply(b['amount'], 0.08)))  # Add tax
            .map(lambda b: b.append_column('total', pc.add(b['amount'], b['tax'])))  # Add total
            .to_kafka('processed_orders'))

    # ========================================================================
    # EXAMPLE 2: OLD @app.agent API (deprecated but still works)
    # ========================================================================

    @app.agent("transactions")  # This will show a deprecation warning
    async def fraud_detector(transaction):
        """
        OLD WAY: Process transactions one record at a time.

        This still works for backward compatibility, but internally
        gets converted to a dataflow.
        """
        if transaction['amount'] > 10000:
            # Simulate async processing
            await asyncio.sleep(0.001)
            yield {
                'alert': 'high_value',
                'txn_id': transaction['id'],
                'amount': transaction['amount']
            }

    # ========================================================================
    # EXECUTION
    # ========================================================================

    logger.info("Starting dataflow examples...")

    # Start the new dataflow
    logger.info("Starting order processing dataflow...")
    await order_pipeline.start(parallelism=2)

    # The old agent would start automatically with the app
    # But since we're in local mode, we need to handle it differently

    # Wait a bit to let things run
    await asyncio.sleep(2)

    # Check job status
    from sabot.job_manager import JobManager
    job_manager = JobManager()

    jobs = await job_manager.list_jobs()
    logger.info(f"Active jobs: {len(jobs)}")

    for job in jobs:
        status = await job_manager.get_job_status(job['job_id'])
        logger.info(f"Job {job['job_id']}: {status['status']} - {status['tasks']['total_tasks']} tasks")

    # Clean up
    logger.info("Stopping dataflows...")
    await order_pipeline.stop()

    logger.info("Dataflow examples completed!")


if __name__ == "__main__":
    # Initialize local database
    from sabot.init_db import init_local_database
    init_local_database()

    # Run the example
    asyncio.run(main())
