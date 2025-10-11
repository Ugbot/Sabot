"""
Continuous Query Manager

Orchestrates continuous graph queries over streaming graph updates.

Architecture:
- Registers continuous queries
- Creates GraphStreamOperator instances
- Manages query lifecycle (start, stop, pause)
- Routes callbacks for new matches
- Coordinates with Stream API

Example:
    manager = ContinuousQueryManager(query_engine=engine)

    # Register continuous query
    query_id = manager.register_query(
        query="MATCH (a)-[:TRANSFER {amount > 10000}]->(b) RETURN a, b",
        callback=lambda result: print(f"High-value transfer: {result}"),
        mode='incremental'
    )

    # Create Stream operator
    operator = manager.create_operator(query_id)

    # Process updates through operator
    updates = Stream.from_kafka("localhost:9092", "graph_updates", "group")
    matches = updates.transform(operator)
"""

from typing import Dict, Callable, Optional, List, Any
import uuid
from dataclasses import dataclass, field
from enum import Enum

from sabot import cyarrow as pa


class QueryStatus(Enum):
    """Status of a continuous query."""
    REGISTERED = "registered"
    RUNNING = "running"
    PAUSED = "paused"
    STOPPED = "stopped"
    ERROR = "error"


@dataclass
class ContinuousQuery:
    """
    Continuous query registration.

    Attributes:
        query_id: Unique query identifier
        query_str: Query string (Cypher or SQL)
        language: Query language
        mode: 'incremental' or 'continuous'
        callback: Callback function for new matches
        timestamp_column: Column for event timestamps
        status: Current query status
        operator: Associated GraphStreamOperator
        stats: Query execution statistics
    """
    query_id: str
    query_str: str
    language: str
    mode: str
    callback: Optional[Callable[[pa.Table], None]]
    timestamp_column: Optional[str] = None
    status: QueryStatus = QueryStatus.REGISTERED
    operator: Optional[Any] = None
    stats: Dict[str, Any] = field(default_factory=dict)


class ContinuousQueryManager:
    """
    **Advanced API**: Manage continuous graph queries.

    For most use cases, use engine.query_cypher(..., as_operator=True) instead.
    This class provides fine-grained control for power users.

    Responsibilities:
    - Register/unregister continuous queries
    - Create GraphStreamOperator instances
    - Route match results to callbacks
    - Track query statistics
    - Handle query lifecycle
    """

    def __init__(
        self,
        query_engine,
        bloom_size: int = 1_000_000,
        max_exact_matches: int = 100_000
    ):
        """
        Initialize continuous query manager.

        Args:
            query_engine: GraphQueryEngine instance
            bloom_size: Bloom filter size for deduplication
            max_exact_matches: Max exact matches before eviction
        """
        self.query_engine = query_engine
        self.bloom_size = bloom_size
        self.max_exact_matches = max_exact_matches

        # Registry of continuous queries
        self.queries: Dict[str, ContinuousQuery] = {}

        # Statistics
        self.total_queries_registered = 0
        self.total_queries_active = 0

    def register_query(
        self,
        query: str,
        callback: Optional[Callable[[pa.Table], None]] = None,
        language: str = "cypher",
        mode: str = "incremental",
        timestamp_column: Optional[str] = None
    ) -> str:
        """
        Register a continuous query.

        Args:
            query: Query string (Cypher or SQL)
            callback: Callback function for new matches (optional)
            language: Query language ('cypher' or 'sql')
            mode: 'incremental' (deduplicate) or 'continuous' (all matches)
            timestamp_column: Column for event timestamps (optional)

        Returns:
            Query ID (UUID)
        """
        # Generate query ID
        query_id = str(uuid.uuid4())

        # Create query registration
        continuous_query = ContinuousQuery(
            query_id=query_id,
            query_str=query,
            language=language,
            mode=mode,
            callback=callback,
            timestamp_column=timestamp_column,
            status=QueryStatus.REGISTERED
        )

        # Register query
        self.queries[query_id] = continuous_query
        self.total_queries_registered += 1

        return query_id

    def unregister_query(self, query_id: str):
        """
        Unregister a continuous query.

        Args:
            query_id: Query identifier
        """
        if query_id in self.queries:
            query = self.queries[query_id]
            query.status = QueryStatus.STOPPED
            del self.queries[query_id]

            if query.status == QueryStatus.RUNNING:
                self.total_queries_active -= 1

    def create_operator(self, query_id: str) -> 'GraphStreamOperator':
        """
        Create GraphStreamOperator for a registered query.

        Args:
            query_id: Query identifier

        Returns:
            GraphStreamOperator instance
        """
        if query_id not in self.queries:
            raise ValueError(f"Query not found: {query_id}")

        query = self.queries[query_id]

        # Import here to avoid circular dependency
        from ..executor.graph_stream_operator import GraphStreamOperator

        # Create operator
        operator = GraphStreamOperator(
            graph=self.query_engine.graph,
            query_engine=self.query_engine,
            query=query.query_str,
            language=query.language,
            mode=query.mode,
            timestamp_column=query.timestamp_column,
            bloom_size=self.bloom_size,
            max_exact_matches=self.max_exact_matches
        )

        # Store operator reference
        query.operator = operator
        query.status = QueryStatus.RUNNING
        self.total_queries_active += 1

        # Wrap operator to route results to callback
        if query.callback is not None:
            operator = self._wrap_with_callback(operator, query.callback)

        return operator

    def _wrap_with_callback(
        self,
        operator: 'GraphStreamOperator',
        callback: Callable[[pa.Table], None]
    ) -> 'GraphStreamOperator':
        """
        Wrap operator to route results to callback.

        Args:
            operator: GraphStreamOperator instance
            callback: Callback function

        Returns:
            Wrapped operator
        """
        # Store original process_batch
        original_process_batch = operator.process_batch

        # Define wrapper
        def process_batch_with_callback(batch):
            # Process batch
            result = original_process_batch(batch)

            # Call callback if we have results
            if result is not None and result.num_rows > 0:
                # Convert to Table for callback
                result_table = pa.Table.from_batches([result])
                try:
                    callback(result_table)
                except Exception as e:
                    print(f"Error in continuous query callback: {e}")

            return result

        # Replace process_batch
        operator.process_batch = process_batch_with_callback

        return operator

    def pause_query(self, query_id: str):
        """
        Pause a running query.

        Args:
            query_id: Query identifier
        """
        if query_id in self.queries:
            query = self.queries[query_id]
            if query.status == QueryStatus.RUNNING:
                query.status = QueryStatus.PAUSED
                self.total_queries_active -= 1

    def resume_query(self, query_id: str):
        """
        Resume a paused query.

        Args:
            query_id: Query identifier
        """
        if query_id in self.queries:
            query = self.queries[query_id]
            if query.status == QueryStatus.PAUSED:
                query.status = QueryStatus.RUNNING
                self.total_queries_active += 1

    def get_query_stats(self, query_id: str) -> Dict[str, Any]:
        """
        Get statistics for a query.

        Args:
            query_id: Query identifier

        Returns:
            Dict with query statistics
        """
        if query_id not in self.queries:
            raise ValueError(f"Query not found: {query_id}")

        query = self.queries[query_id]

        stats = {
            'query_id': query.query_id,
            'query': query.query_str,
            'language': query.language,
            'mode': query.mode,
            'status': query.status.value
        }

        # Add operator stats if available
        if query.operator is not None:
            try:
                stats['operator'] = query.operator.get_stats()
            except Exception as e:
                stats['operator_error'] = str(e)

        return stats

    def get_all_queries(self) -> List[Dict[str, Any]]:
        """
        Get all registered queries.

        Returns:
            List of query metadata
        """
        return [
            {
                'query_id': q.query_id,
                'query': q.query_str,
                'language': q.language,
                'mode': q.mode,
                'status': q.status.value
            }
            for q in self.queries.values()
        ]

    def get_manager_stats(self) -> Dict[str, Any]:
        """
        Get manager statistics.

        Returns:
            Dict with manager statistics
        """
        return {
            'total_queries_registered': self.total_queries_registered,
            'total_queries_active': self.total_queries_active,
            'queries_by_status': {
                status.value: sum(1 for q in self.queries.values() if q.status == status)
                for status in QueryStatus
            }
        }
