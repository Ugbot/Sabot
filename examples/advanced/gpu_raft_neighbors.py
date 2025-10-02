#!/usr/bin/env python3
import sys
import os

# Add sabot to path
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

"""
GPU-Accelerated Nearest Neighbors with RAFT in Sabot

This example demonstrates real-time similarity search using GPU-accelerated
nearest neighbors algorithms for recommendation systems.
"""

import asyncio
import numpy as np
import pandas as pd
from typing import Dict, Any, List
import logging

from sabot.app import App

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def generate_user_embeddings(n_users: int = 1000, embedding_dim: int = 128) -> pd.DataFrame:
    """Generate synthetic user embeddings for similarity search."""
    np.random.seed(42)

    # Create user embeddings with some clustering structure
    embeddings = []

    for i in range(n_users):
        # Create embedding with some structure (users with similar preferences cluster together)
        base_embedding = np.random.normal(0, 1, embedding_dim)

        # Add cluster bias (3 different user types)
        cluster = i % 3
        if cluster == 0:  # Tech enthusiasts
            base_embedding[:32] += np.random.normal(2, 0.5, 32)
        elif cluster == 1:  # Sports fans
            base_embedding[32:64] += np.random.normal(2, 0.5, 32)
        else:  # Music lovers
            base_embedding[64:96] += np.random.normal(2, 0.5, 32)

        embeddings.append({
            'user_id': f'user_{i}',
            'embedding': base_embedding.tolist(),
            'cluster': cluster,
            'age': np.random.randint(18, 65),
            'interests': ['tech', 'sports', 'music'][cluster]
        })

    return pd.DataFrame(embeddings)


def generate_query_embeddings(n_queries: int = 10, embedding_dim: int = 128) -> List[Dict[str, Any]]:
    """Generate query embeddings for similarity search."""
    np.random.seed(123)

    queries = []
    for i in range(n_queries):
        # Create query embedding similar to one of the clusters
        cluster = np.random.choice([0, 1, 2])
        embedding = np.random.normal(0, 1, embedding_dim)

        if cluster == 0:  # Tech query
            embedding[:32] += np.random.normal(2, 0.3, 32)
        elif cluster == 1:  # Sports query
            embedding[32:64] += np.random.normal(2, 0.3, 32)
        else:  # Music query
            embedding[64:96] += np.random.normal(2, 0.3, 32)

        queries.append({
            'query_id': f'query_{i}',
            'embedding': embedding.tolist(),
            'expected_interest': ['tech', 'sports', 'music'][cluster],
            'timestamp': asyncio.get_event_loop().time()
        })

    return queries


async def user_embedding_producer(app: App, topic_name: str):
    """Produce user embeddings to a topic."""
    logger.info(f"Starting user embedding producer on topic: {topic_name}")

    topic = app.topic(topic_name)

    # Generate and send user embeddings in batches
    embeddings_df = generate_user_embeddings(1000)
    batch_size = 100

    for i in range(0, len(embeddings_df), batch_size):
        batch = embeddings_df.iloc[i:i+batch_size]

        # Convert to records for streaming
        records = batch.to_dict('records')

        # Send batch
        await topic.send(records)

        logger.info(f"Sent batch of {len(records)} user embeddings")
        await asyncio.sleep(0.5)

    # Send completion signal
    await topic.send({'type': 'complete', 'total_users': len(embeddings_df)})
    logger.info("User embedding production completed")


async def gpu_neighbors_agent(app: App):
    """Agent that performs GPU-accelerated nearest neighbors search."""
    logger.info("Starting GPU nearest neighbors agent")

    # Create RAFT stream for GPU acceleration
    raft_stream = app.raft_stream("gpu_neighbors")

    # Create nearest neighbors processor (k=5)
    nn_processor = raft_stream.nearest_neighbors(k=5)

    # Topics
    input_topic = app.topic("user_embeddings")
    query_topic = app.topic("similarity_queries")
    results_topic = app.topic("neighbors_results")

    # Build user embedding index
    logger.info("Building user embedding index...")

    user_embeddings = []
    stream = app.stream(input_topic)

    # Collect all user embeddings
    async for batch in stream:
        if isinstance(batch, dict) and batch.get('type') == 'complete':
            logger.info(f"Collected {len(user_embeddings)} user embeddings")
            break

        # Accumulate embeddings
        for record in batch:
            if 'embedding' in record:
                user_embeddings.append(record)

    if not user_embeddings:
        logger.error("No user embeddings collected!")
        return

    # Convert to DataFrame for processing
    embeddings_df = pd.DataFrame(user_embeddings)
    logger.info(f"Built index with {len(embeddings_df)} users")

    # Listen for similarity queries
    query_stream = app.stream(query_topic)

    logger.info("Ready to process similarity queries...")

    async for query_batch in query_stream:
        try:
            for query in query_batch:
                if 'embedding' not in query:
                    continue

                # Prepare query data for NN search
                query_data = {
                    'data': embeddings_df[['embedding']].copy(),
                    'queries': pd.DataFrame([{
                        'embedding': query['embedding']
                    }])
                }

                # Perform GPU-accelerated nearest neighbors search
                neighbors_result = nn_processor(query_data)

                # Format results
                distances = neighbors_result['distances'][0] if hasattr(neighbors_result['distances'], '__getitem__') else neighbors_result['distances']
                indices = neighbors_result['indices'][0] if hasattr(neighbors_result['indices'], '__getitem__') else neighbors_result['indices']

                # Get similar users
                similar_users = []
                for idx, distance in zip(indices, distances):
                    if idx < len(embeddings_df):
                        user = embeddings_df.iloc[idx]
                        similar_users.append({
                            'user_id': user['user_id'],
                            'similarity_score': float(distance),
                            'interests': user.get('interests', 'unknown'),
                            'cluster': user.get('cluster', -1)
                        })

                result = {
                    'query_id': query['query_id'],
                    'expected_interest': query.get('expected_interest'),
                    'similar_users': similar_users,
                    'processing_time': asyncio.get_event_loop().time()
                }

                # Send results
                await results_topic.send(result)

                logger.info(f"Processed query {query['query_id']}: found {len(similar_users)} similar users")

        except Exception as e:
            logger.error(f"Error processing query: {e}")
            continue


async def query_producer(app: App, topic_name: str):
    """Produce similarity queries."""
    logger.info(f"Starting query producer on topic: {topic_name}")

    topic = app.topic(topic_name)

    # Generate queries
    queries = generate_query_embeddings(20)

    # Send queries with delays
    for query in queries:
        await topic.send(query)
        logger.info(f"Sent similarity query: {query['query_id']} (interest: {query['expected_interest']})")
        await asyncio.sleep(0.2)

    # Send completion
    await topic.send({'type': 'complete'})
    logger.info("Query production completed")


async def results_display_agent(app: App):
    """Display similarity search results."""
    logger.info("Starting results display agent")

    results_topic = app.topic("neighbors_results")
    stream = app.stream(results_topic)

    query_count = 0

    async for result in stream:
        if isinstance(result, dict) and result.get('type') == 'complete':
            break

        query_count += 1
        logger.info(f"\n--- Query {query_count} Results ---")
        logger.info(f"Query ID: {result['query_id']}")
        logger.info(f"Expected Interest: {result.get('expected_interest', 'unknown')}")

        logger.info("Top 5 Similar Users:")
        for i, user in enumerate(result['similar_users'][:5], 1):
            logger.info(".4f")

        # Calculate accuracy (if expected interest matches)
        if 'expected_interest' in result:
            expected = result['expected_interest']
            top_match = result['similar_users'][0]['interests'] if result['similar_users'] else 'none'
            accuracy = 1.0 if expected == top_match else 0.0
            logger.info(f"Top-1 Accuracy: {accuracy:.1f} (expected: {expected}, got: {top_match})")

        await asyncio.sleep(0.1)  # Small delay for readability


async def main():
    """Main function demonstrating GPU-accelerated nearest neighbors."""
    logger.info("Starting Sabot GPU-Accelerated Nearest Neighbors Demo")

    # Create Sabot app with GPU acceleration
    app = App(
        "gpu_neighbors_demo",
        enable_gpu=True,
        gpu_device=0,
        redis_host="localhost",
        redis_port=6379,
        enable_distributed_state=True
    )

    if not app.enable_gpu or app._gpu_resources is None:
        logger.warning("GPU acceleration not available - install CUDA and RAFT dependencies")
        return

    try:
        await app.start()

        # Create concurrent tasks
        tasks = []

        # User embedding producer
        tasks.append(asyncio.create_task(
            user_embedding_producer(app, "user_embeddings")
        ))

        # GPU nearest neighbors agent (wait a bit for embeddings to be ready)
        await asyncio.sleep(2)
        tasks.append(asyncio.create_task(
            gpu_neighbors_agent(app)
        ))

        # Query producer (wait for NN agent to be ready)
        await asyncio.sleep(1)
        tasks.append(asyncio.create_task(
            query_producer(app, "similarity_queries")
        ))

        # Results display
        tasks.append(asyncio.create_task(
            results_display_agent(app)
        ))

        logger.info("All components started - running similarity search demo...")

        # Run until all tasks complete or timeout
        done, pending = await asyncio.wait(tasks, timeout=60, return_when=asyncio.ALL_COMPLETED)

        # Cancel any remaining tasks
        for task in pending:
            task.cancel()

        # Check for exceptions
        for task in done:
            if task.exception():
                logger.error(f"Task failed: {task.exception()}")

    except KeyboardInterrupt:
        logger.info("Demo interrupted by user")
    except Exception as e:
        logger.error(f"Demo failed: {e}")
        raise
    finally:
        await app.stop()

    logger.info("GPU nearest neighbors demo completed")


if __name__ == "__main__":
    asyncio.run(main())
