"""
Base connector interfaces for Sabot data sources and sinks.

Defines abstract interfaces that all connectors must implement.
"""

from abc import ABC, abstractmethod
from typing import AsyncIterator, Optional, Dict, Any, List
from sabot import cyarrow as ca


class ConnectorSource(ABC):
    """
    Abstract base class for data source connectors.

    Sources stream Arrow RecordBatches into Sabot pipelines with optional
    filter and projection pushdown for optimal performance.

    Examples:
        >>> source = ParquetSource('data/*.parquet', columns=['id', 'price'])
        >>> async for batch in source.stream_batches():
        >>>     process(batch)
    """

    @abstractmethod
    def get_schema(self) -> ca.Schema:
        """
        Get the Arrow schema for this data source.

        Returns:
            ca.Schema: Arrow schema describing the data
        """
        pass

    @abstractmethod
    async def stream_batches(self) -> AsyncIterator[ca.RecordBatch]:
        """
        Stream Arrow RecordBatches from the data source.

        Yields:
            ca.RecordBatch: Zero-copy Arrow batches
        """
        pass

    def apply_filters(self, filters: Dict[str, Any]) -> 'ConnectorSource':
        """
        Apply filter predicates (optional, for pushdown optimization).

        Args:
            filters: Column filters (e.g., {'price': '> 100', 'date': ">= '2025-01-01'"})

        Returns:
            Self for chaining
        """
        # Default: no-op (connectors can override for pushdown)
        return self

    def select_columns(self, columns: List[str]) -> 'ConnectorSource':
        """
        Select specific columns (optional, for projection pushdown).

        Args:
            columns: Column names to read

        Returns:
            Self for chaining
        """
        # Default: no-op (connectors can override for pushdown)
        return self

    def close(self):
        """Close and cleanup resources (optional)."""
        pass


class ConnectorSink(ABC):
    """
    Abstract base class for data sink connectors.

    Sinks receive Arrow RecordBatches from Sabot pipelines and write
    to external systems with optional batching and compression.

    Examples:
        >>> sink = ParquetSink('output/*.parquet', partition_by='date')
        >>> await sink.write_batch(batch)
        >>> await sink.close()
    """

    @abstractmethod
    async def write_batch(self, batch: ca.RecordBatch):
        """
        Write a single Arrow RecordBatch to the sink.

        Args:
            batch: Arrow batch to write
        """
        pass

    async def write_stream(self, stream: AsyncIterator[ca.RecordBatch]):
        """
        Write a full stream of batches to the sink.

        Args:
            stream: AsyncIterator yielding Arrow batches
        """
        async for batch in stream:
            await self.write_batch(batch)

    @abstractmethod
    async def close(self):
        """Flush buffers and close the sink."""
        pass

    async def __aenter__(self):
        """Context manager entry."""
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        await self.close()
