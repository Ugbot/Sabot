#!/usr/bin/env python3
import sys
import os

# Add sabot to path
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

"""
GPU-Accelerated K-means Clustering with RAFT in Sabot

This example demonstrates how to use RAFT (GPU-accelerated ML) for real-time
clustering of streaming sensor data.
"""

import asyncio
import numpy as np
import pandas as pd
from typing import Dict, Any
import logging

# Sabot imports
from sabot.app import App
from sabot.stream import Stream

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def generate_sensor_data(n_samples: int = 1000) -> pd.DataFrame:
    """Generate synthetic sensor data for clustering."""
    np.random.seed(42)

    # Create three distinct clusters
    data = []

    # Cluster 1: Low temperature, low pressure
    cluster1 = pd.DataFrame({
        'temperature': np.random.normal(20, 2, n_samples // 3),
        'pressure': np.random.normal(1000, 50, n_samples // 3),
        'humidity': np.random.normal(60, 5, n_samples // 3),
        'sensor_id': ['sensor_1'] * (n_samples // 3)
    })

    # Cluster 2: High temperature, high pressure
    cluster2 = pd.DataFrame({
        'temperature': np.random.normal(35, 3, n_samples // 3),
        'pressure': np.random.normal(1020, 30, n_samples // 3),
        'humidity': np.random.normal(75, 8, n_samples // 3),
        'sensor_id': ['sensor_2'] * (n_samples // 3)
    })

    # Cluster 3: Medium temperature, low humidity
    cluster3 = pd.DataFrame({
        'temperature': np.random.normal(25, 2, n_samples - 2 * (n_samples // 3)),
        'pressure': np.random.normal(1010, 40, n_samples - 2 * (n_samples // 3)),
        'humidity': np.random.normal(40, 6, n_samples - 2 * (n_samples // 3)),
        'sensor_id': ['sensor_3'] * (n_samples - 2 * (n_samples // 3))
    })

    return pd.concat([cluster1, cluster2, cluster3], ignore_index=True)


async def simulate_sensor_stream(app: App, topic_name: str, batch_size: int = 100):
    """Simulate streaming sensor data."""
    logger.info(f"Starting sensor data simulation on topic: {topic_name}")

    # Create topic if it doesn't exist
    topic = app.topic(topic_name)

    # Generate continuous sensor data
    sample_count = 0
    while True:
        # Generate batch of sensor readings
        batch_data = generate_sensor_data(batch_size)

        # Add timestamp
        import time
        batch_data['timestamp'] = time.time()

        # Send batch to topic
        await topic.send(batch_data.to_dict('records'))

        sample_count += batch_size
        logger.info(f"Sent batch of {batch_size} sensor readings (total: {sample_count})")

        # Wait before next batch
        await asyncio.sleep(1.0)


async def gpu_clustering_agent(app: App):
    """Agent that performs GPU-accelerated clustering on sensor data."""
    logger.info("Starting GPU clustering agent")

    # Create RAFT stream for GPU acceleration
    raft_stream = app.raft_stream("gpu_clustering")

    # Create K-means clustering processor
    cluster_processor = raft_stream.kmeans_cluster(n_clusters=3, max_iter=100)

    # Create topic for input and output
    input_topic = app.topic("sensor_data")
    output_topic = app.topic("clustered_data")

    # Create stream from topic
    stream = app.stream(input_topic, batch_size=200)

    logger.info("GPU clustering agent ready - processing sensor data...")

    async for batch in stream:
        try:
            # Apply GPU-accelerated clustering
            clustered_data = cluster_processor(batch)

            # Add clustering metadata
            result = {
                'original_data': batch,
                'clustered_data': clustered_data,
                'cluster_count': len(clustered_data['cluster_id'].unique()),
                'processing_timestamp': asyncio.get_event_loop().time()
            }

            # Send results to output topic
            await output_topic.send(result)

            logger.info(f"Processed batch: {len(batch)} samples -> {result['cluster_count']} clusters")

        except Exception as e:
            logger.error(f"Error in clustering: {e}")
            continue


async def results_consumer(app: App):
    """Consume and display clustering results."""
    logger.info("Starting results consumer")

    # Create topic for clustered results
    results_topic = app.topic("clustered_data")
    stream = app.stream(results_topic)

    async for result in stream:
        clustered_data = result['clustered_data']

        # Display clustering statistics
        cluster_counts = clustered_data['cluster_id'].value_counts()
        logger.info(f"Clustering Results - Total samples: {len(clustered_data)}")
        for cluster_id, count in cluster_counts.items():
            logger.info(f"  Cluster {cluster_id}: {count} samples")

        # Show sample of clustered data
        sample = clustered_data.head(3)
        logger.info("Sample clustered data:")
        for _, row in sample.iterrows():
            logger.info(".1f")


async def main():
    """Main function demonstrating GPU-accelerated clustering."""
    logger.info("Starting Sabot GPU-Accelerated Clustering Demo")

    # Create Sabot app with GPU acceleration
    app = App(
        "gpu_clustering_demo",
        enable_gpu=True,  # Enable GPU acceleration
        gpu_device=0,     # Use GPU device 0
        redis_host="localhost",
        redis_port=6379,
        enable_distributed_state=True
    )

    logger.info("App initialized with GPU acceleration")

    # Check if GPU is available
    if not app.enable_gpu or app._gpu_resources is None:
        logger.warning("GPU acceleration not available - falling back to CPU")
        logger.warning("To enable GPU: install CUDA, cuDF, and pylibraft, then set enable_gpu=True")
        return

    try:
        # Start the app
        await app.start()

        # Create tasks for different components
        tasks = []

        # Sensor data simulator
        tasks.append(asyncio.create_task(
            simulate_sensor_stream(app, "sensor_data", batch_size=200)
        ))

        # GPU clustering agent
        tasks.append(asyncio.create_task(
            gpu_clustering_agent(app)
        ))

        # Results consumer
        tasks.append(asyncio.create_task(
            results_consumer(app)
        ))

        logger.info("All components started - running for 30 seconds...")

        # Run for 30 seconds
        await asyncio.sleep(30)

        # Cancel all tasks
        for task in tasks:
            task.cancel()

        # Wait for tasks to finish
        await asyncio.gather(*tasks, return_exceptions=True)

    except KeyboardInterrupt:
        logger.info("Demo interrupted by user")
    except Exception as e:
        logger.error(f"Demo failed: {e}")
        raise
    finally:
        await app.stop()

    logger.info("GPU clustering demo completed")


if __name__ == "__main__":
    # Run the demo
    asyncio.run(main())
