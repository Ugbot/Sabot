"""
Feature Store Sink - Write computed features to CyRedis-backed feature store.
"""

import asyncio
import time
from typing import Dict, List, Optional, Any, Callable
import logging

from sabot import cyarrow as ca
from .store import FeatureStore

logger = logging.getLogger(__name__)


class FeatureStoreSink:
    """
    Sink that writes Arrow batches to CyRedis feature store.

    Each row in the batch is written as features keyed by entity_id.
    """

    def __init__(
        self,
        feature_store: FeatureStore,
        entity_key_column: str,
        feature_columns: List[str],
        ttl: Optional[int] = None,
        batch_size: int = 1000
    ):
        """
        Initialize feature store sink.

        Args:
            feature_store: FeatureStore instance
            entity_key_column: Column name containing entity ID
            feature_columns: List of column names to write as features
            ttl: Time-to-live in seconds (None = no expiration)
            batch_size: Number of rows to batch per Redis pipeline write
        """
        self.feature_store = feature_store
        self.entity_key_column = entity_key_column
        self.feature_columns = feature_columns
        self.ttl = ttl
        self.batch_size = batch_size

        self._stats = {
            'batches_processed': 0,
            'rows_written': 0,
            'features_written': 0,
            'errors': 0
        }

    async def write_batch(self, batch: ca.RecordBatch) -> None:
        """
        Write a single Arrow batch to feature store.

        Args:
            batch: Arrow RecordBatch with entity_key and feature columns
        """
        if not self.feature_store._initialized:
            await self.feature_store.initialize()

        num_rows = batch.num_rows()
        if num_rows == 0:
            return

        # Extract entity IDs
        try:
            entity_ids = batch.column(self.entity_key_column).to_pylist()
        except KeyError:
            logger.error(f"Entity key column '{self.entity_key_column}' not found in batch")
            self._stats['errors'] += 1
            return

        # Extract feature columns
        features_data = {}
        for feature_name in self.feature_columns:
            try:
                features_data[feature_name] = batch.column(feature_name).to_pylist()
            except KeyError:
                logger.warning(f"Feature column '{feature_name}' not found in batch, skipping")
                continue

        if not features_data:
            logger.warning("No feature columns found in batch")
            return

        # Write features for each entity
        timestamp = int(time.time())

        # Batch writes for performance
        for i in range(0, num_rows, self.batch_size):
            batch_end = min(i + self.batch_size, num_rows)

            # Prepare writes
            writes = []
            for row_idx in range(i, batch_end):
                entity_id = str(entity_ids[row_idx])

                # Extract features for this row
                row_features = {}
                for feature_name, values in features_data.items():
                    value = values[row_idx]
                    if value is not None:  # Skip None values
                        row_features[feature_name] = float(value)

                if row_features:
                    writes.append((entity_id, row_features))

            # Execute batch write
            try:
                for entity_id, row_features in writes:
                    await self.feature_store.set_features(
                        entity_id=entity_id,
                        features=row_features,
                        ttl=self.ttl,
                        timestamp=timestamp
                    )

                self._stats['rows_written'] += len(writes)
                self._stats['features_written'] += sum(len(f) for _, f in writes)

            except Exception as e:
                logger.error(f"Failed to write features to store: {e}")
                self._stats['errors'] += 1

        self._stats['batches_processed'] += 1

    async def sink(self, stream) -> None:
        """
        Consume stream and write to feature store.

        Args:
            stream: Iterable of Arrow RecordBatches
        """
        logger.info(f"Starting feature store sink: entity_key={self.entity_key_column}, "
                   f"features={self.feature_columns}, ttl={self.ttl}")

        try:
            async for batch in stream:
                await self.write_batch(batch)

                # Log progress periodically
                if self._stats['batches_processed'] % 100 == 0:
                    logger.info(
                        f"Feature sink progress: {self._stats['batches_processed']} batches, "
                        f"{self._stats['rows_written']} rows, "
                        f"{self._stats['features_written']} features, "
                        f"{self._stats['errors']} errors"
                    )

        except Exception as e:
            logger.error(f"Feature sink error: {e}")
            raise

        finally:
            logger.info(
                f"Feature sink completed: {self._stats['batches_processed']} batches, "
                f"{self._stats['rows_written']} rows, "
                f"{self._stats['features_written']} features written"
            )

    def get_stats(self) -> Dict[str, int]:
        """Get sink statistics."""
        return self._stats.copy()


async def to_feature_store(
    stream,
    feature_store: FeatureStore,
    entity_key_column: str,
    feature_columns: List[str],
    ttl: Optional[int] = None,
    batch_size: int = 1000
) -> Dict[str, int]:
    """
    Write stream to feature store.

    Args:
        stream: Iterable of Arrow RecordBatches
        feature_store: FeatureStore instance
        entity_key_column: Column name containing entity ID
        feature_columns: List of column names to write as features
        ttl: Time-to-live in seconds
        batch_size: Rows per Redis pipeline write

    Returns:
        Statistics dictionary

    Example:
        ```python
        feature_store = FeatureStore(redis_url="localhost:6379")
        await feature_store.initialize()

        stream = (
            Stream.from_kafka(...)
            .with_features(['price_ma_5m', 'volume_std_1h'])
        )

        stats = await to_feature_store(
            stream,
            feature_store=feature_store,
            entity_key_column='symbol',
            feature_columns=['price_ma_5m', 'volume_std_1h'],
            ttl=300
        )
        ```
    """
    sink = FeatureStoreSink(
        feature_store=feature_store,
        entity_key_column=entity_key_column,
        feature_columns=feature_columns,
        ttl=ttl,
        batch_size=batch_size
    )

    await sink.sink(stream)
    return sink.get_stats()
