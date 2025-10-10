"""
Query Result Formatting

Handles formatting and streaming of query results. Results are returned as
Arrow tables for zero-copy integration with the rest of Sabot.

Features:
- QueryResult wrapper for batch query results
- ResultStream for continuous query results
- Conversion to Pandas, JSON, CSV
- Pagination support for large result sets
"""

from sabot import cyarrow as pa
from typing import Optional, Iterator, Any, Dict, List
from dataclasses import dataclass
import json


@dataclass
class QueryMetadata:
    """Metadata about query execution."""
    query: str
    language: str  # 'cypher' or 'sparql'
    num_results: int
    execution_time_ms: float
    num_vertices_scanned: int = 0
    num_edges_scanned: int = 0
    optimized: bool = True


class QueryResult:
    """
    Result of a batch graph query.

    Wraps Arrow table with query metadata and provides convenient
    conversion methods.

    Example:
        >>> result = engine.query_cypher("MATCH (a)-[r]->(b) RETURN a, b")
        >>> print(result)
        QueryResult(num_results=1000, execution_time_ms=5.2)
        >>>
        >>> # Convert to Pandas
        >>> df = result.to_pandas()
        >>>
        >>> # Get Arrow table
        >>> table = result.to_arrow()
        >>>
        >>> # Iterate rows
        >>> for row in result:
        ...     print(row)
    """

    def __init__(
        self,
        table: pa.Table,
        metadata: Optional[QueryMetadata] = None
    ):
        """
        Initialize query result.

        Args:
            table: Arrow table with query results
            metadata: Query execution metadata
        """
        self.table = table
        self.metadata = metadata

    def to_arrow(self) -> pa.Table:
        """Return results as Arrow table."""
        return self.table

    def to_pandas(self):
        """
        Convert results to Pandas DataFrame.

        Returns:
            Pandas DataFrame with query results
        """
        return self.table.to_pandas()

    def to_dict(self) -> List[Dict[str, Any]]:
        """
        Convert results to list of dictionaries.

        Returns:
            List of row dictionaries
        """
        return self.table.to_pylist()

    def to_json(self, orient: str = 'records') -> str:
        """
        Convert results to JSON string.

        Args:
            orient: 'records' (list of dicts) or 'table' (schema + data)

        Returns:
            JSON string
        """
        if orient == 'records':
            return json.dumps(self.to_dict())
        elif orient == 'table':
            return json.dumps({
                'schema': self.table.schema.to_string(),
                'data': self.to_dict()
            })
        else:
            raise ValueError(f"Invalid orient: {orient}")

    def to_csv(self, include_header: bool = True) -> str:
        """
        Convert results to CSV string.

        Args:
            include_header: Include column names as first row

        Returns:
            CSV string
        """
        import io
        output = io.StringIO()

        # Write header
        if include_header:
            output.write(','.join(self.table.schema.names) + '\n')

        # Write rows
        for row in self.to_dict():
            values = [str(row.get(col, '')) for col in self.table.schema.names]
            output.write(','.join(values) + '\n')

        return output.getvalue()

    def __len__(self) -> int:
        """Return number of result rows."""
        return self.table.num_rows

    def __iter__(self) -> Iterator[Dict[str, Any]]:
        """Iterate over result rows as dictionaries."""
        return iter(self.to_dict())

    def __repr__(self) -> str:
        """String representation."""
        if self.metadata:
            return (
                f"QueryResult(num_results={self.metadata.num_results}, "
                f"execution_time_ms={self.metadata.execution_time_ms:.2f})"
            )
        else:
            return f"QueryResult(num_results={len(self)})"

    def __str__(self) -> str:
        """Human-readable string representation."""
        output = []
        output.append(repr(self))

        if self.metadata:
            output.append(f"\nQuery: {self.metadata.query}")
            output.append(f"Language: {self.metadata.language}")
            output.append(f"Vertices scanned: {self.metadata.num_vertices_scanned}")
            output.append(f"Edges scanned: {self.metadata.num_edges_scanned}")

        output.append(f"\nSchema: {self.table.schema}")
        output.append(f"\nFirst 5 rows:")
        output.append(str(self.table.slice(0, min(5, len(self))).to_pandas()))

        return '\n'.join(output)


class ResultStream:
    """
    Stream of query results for continuous queries.

    Yields QueryResult objects as new results become available.
    Supports both full results (all matches) and incremental results
    (only new matches since last update).

    Example:
        >>> # Register continuous query
        >>> stream = engine.query_continuous(
        ...     "MATCH (a)-[:TRANSFER {amount: > 10000}]->(b) RETURN a, b"
        ... )
        >>>
        >>> # Iterate results as they arrive
        >>> for result in stream:
        ...     print(f"New fraud pattern: {result.to_pandas()}")
        ...
        ...     if result.num_results > 10:
        ...         stream.stop()
        ...         break
    """

    def __init__(
        self,
        query_id: str,
        query: str,
        language: str = "cypher",
        mode: str = "incremental"
    ):
        """
        Initialize result stream.

        Args:
            query_id: Unique query identifier
            query: Query string
            language: 'cypher' or 'sparql'
            mode: 'continuous' (all results) or 'incremental' (only new)
        """
        self.query_id = query_id
        self.query = query
        self.language = language
        self.mode = mode

        # Result queue (filled by continuous query manager)
        self.result_queue: List[QueryResult] = []

        # Stream state
        self.stopped = False
        self.total_results = 0

    def __iter__(self) -> Iterator[QueryResult]:
        """
        Iterate over query results as they arrive.

        Yields:
            QueryResult objects with new matches
        """
        while not self.stopped:
            # Wait for new results (Phase 4.6 will implement async queue)
            if self.result_queue:
                result = self.result_queue.pop(0)
                self.total_results += len(result)
                yield result

    def stop(self) -> None:
        """Stop streaming results."""
        self.stopped = True

    def is_stopped(self) -> bool:
        """Check if stream is stopped."""
        return self.stopped

    def get_total_results(self) -> int:
        """Get total number of results streamed so far."""
        return self.total_results

    def __repr__(self) -> str:
        """String representation."""
        return (
            f"ResultStream(query_id={self.query_id}, "
            f"total_results={self.total_results}, "
            f"stopped={self.stopped})"
        )
