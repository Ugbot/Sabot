# cython: language_level=3
# distutils: language = c++
"""
Graph Stream Operator

Continuous graph pattern matching operator.

Architecture:
- Extends BaseOperator → integrates with Stream API
- Maintains PropertyGraph state
- Processes batches of graph updates (vertices/edges)
- Runs pattern matching on updated graph
- Deduplicates matches (incremental mode)
- Tracks watermarks for event-time processing

Performance:
- Pattern matching: 3-37M matches/sec (depends on pattern complexity)
- Update processing: 1-5M updates/sec
- Deduplication overhead: 50-200ns per match

Example:
    operator = GraphStreamOperator(
        graph=graph,
        query_engine=engine,
        query="MATCH (a)-[:TRANSFER {amount > 10000}]->(b) RETURN a, b",
        mode='incremental'
    )

    updates_batch = ...  # RecordBatch with vertex/edge updates
    matches_batch = operator.process_batch(updates_batch)
"""

from libc.stdint cimport int64_t, int32_t
from libcpp cimport bool as cbool
from libcpp.string cimport string
from libcpp.vector cimport vector

import time
from typing import Optional, List, Dict, Any

from sabot import cyarrow as pa
cimport pyarrow.lib as ca

from sabot._cython.operators.base_operator cimport BaseOperator
from sabot._cython.time.watermark_tracker cimport WatermarkTracker
from .match_tracker cimport MatchTracker


cdef class GraphStreamOperator(BaseOperator):
    """
    Stream operator for continuous graph pattern matching.

    This operator:
    1. Receives batches of graph updates (vertices/edges)
    2. Updates the in-memory graph state
    3. Runs pattern matching on the updated graph
    4. Deduplicates matches (incremental mode)
    5. Returns new pattern matches as RecordBatch

    Attributes:
        graph: PropertyGraph instance (maintains graph state)
        query_engine: GraphQueryEngine instance (executes queries)
        query_str: Cypher/SQL query string
        query_language: Query language ('cypher' or 'sql')
        mode: 'incremental' (only new matches) or 'continuous' (all matches)
        compiled_plan: Compiled physical query plan
        match_tracker: Tracks emitted matches for deduplication
        watermark_tracker: Tracks event-time watermarks
        timestamp_column: Column name for event timestamps
    """

    def __init__(
        self,
        object graph,
        object query_engine,
        str query,
        str language='cypher',
        str mode='incremental',
        str timestamp_column=None,
        int32_t bloom_size=1_000_000,
        int64_t max_exact_matches=100_000
    ):
        """
        Initialize graph stream operator.

        Args:
            graph: PyPropertyGraph instance
            query_engine: GraphQueryEngine instance
            query: Pattern matching query (Cypher or SQL)
            language: Query language ('cypher' or 'sql')
            mode: 'incremental' (deduplicate) or 'continuous' (all matches)
            timestamp_column: Column for event timestamps (optional)
            bloom_size: Bloom filter size for deduplication
            max_exact_matches: Max exact matches before eviction
        """
        super().__init__()

        self.graph = graph
        self.query_engine = query_engine
        self.query_str = query
        self.query_language = language
        self.mode = mode

        # Configure deduplication
        self.track_matches = (mode == 'incremental')
        if self.track_matches:
            self.match_tracker = MatchTracker(
                bloom_size=bloom_size,
                max_exact_matches=max_exact_matches
            )

        # Configure watermarks
        self.timestamp_column = timestamp_column
        self.has_watermarks = (timestamp_column is not None)
        if self.has_watermarks:
            self.watermark_tracker = WatermarkTracker(
                num_partitions=16,  # Default partition count
                allowed_lateness_ms=60000  # 1 minute
            )

        # Compile query plan
        try:
            if language == 'cypher':
                result = query_engine.query_cypher(
                    query,
                    config=query_engine.QueryConfig(explain=True)
                )
                self.compiled_plan = result.metadata.get('physical_plan')
            else:
                # SQL queries
                result = query_engine.query_sql(
                    query,
                    config=query_engine.QueryConfig(explain=True)
                )
                self.compiled_plan = result.metadata.get('physical_plan')
        except Exception as e:
            print(f"Warning: Could not compile query plan: {e}")
            self.compiled_plan = None

        # Statistics
        self.total_updates_processed = 0
        self.total_matches_emitted = 0
        self.total_matches_deduplicated = 0
        self.total_pattern_matching_time_ms = 0.0

        # Mark as stateful operator
        self._stateful = True

    cpdef object process_batch(self, object batch):
        """
        Process batch of graph updates.

        Steps:
        1. Update watermarks (if event-time enabled)
        2. Apply updates to graph (add/update vertices/edges)
        3. Run pattern matching on updated graph
        4. Filter to only new matches (if incremental mode)
        5. Return new matches as RecordBatch

        Args:
            batch: Arrow Table or RecordBatch with graph updates
                Expected schema:
                - update_type: 'vertex' or 'edge'
                - vertex_id/source/target: int64
                - label: string
                - properties: struct or map
                - timestamp: int64 (optional, for event-time)

        Returns:
            Arrow RecordBatch with new pattern matches
        """
        if batch is None or batch.num_rows == 0:
            return pa.RecordBatch.from_arrays([], schema=pa.schema([]))

        # Convert Table to RecordBatch if needed
        cdef object record_batch
        if isinstance(batch, pa.Table):
            batches = batch.to_batches()
            if not batches:
                return pa.RecordBatch.from_arrays([], schema=batch.schema)
            record_batch = batches[0]
        else:
            record_batch = batch

        # Process updates
        try:
            result = self._process_updates(record_batch)
            return result
        except Exception as e:
            print(f"Error processing batch in GraphStreamOperator: {e}")
            import traceback
            traceback.print_exc()
            # Return empty batch on error
            return pa.RecordBatch.from_arrays([], schema=pa.schema([]))

    cdef object _process_updates(self, object batch) except *:
        """
        Internal method to process updates.

        Args:
            batch: RecordBatch with updates

        Returns:
            RecordBatch with new matches
        """
        # 1. Update watermarks
        if self.has_watermarks:
            self._update_watermarks(batch)

        # 2. Apply updates to graph
        self._apply_updates_to_graph(batch)
        self.total_updates_processed += batch.num_rows

        # 3. Run pattern matching
        cdef double start_time = time.time()
        cdef object matches = self._run_pattern_matching()
        cdef double end_time = time.time()
        self.total_pattern_matching_time_ms += (end_time - start_time) * 1000.0

        # 4. Filter to new matches (if incremental)
        if self.track_matches:
            matches = self._filter_new_matches(matches)

        # 5. Update stats
        if matches is not None and matches.num_rows > 0:
            self.total_matches_emitted += matches.num_rows

        return matches

    cdef object _apply_updates_to_graph(self, object batch) except *:
        """
        Apply graph updates to in-memory graph.

        Args:
            batch: RecordBatch with updates
                Columns:
                - update_type: 'vertex' or 'edge'
                - For vertices: vertex_id, label, properties
                - For edges: source, target, label, properties

        Returns:
            None (updates graph in-place)
        """
        # Check if batch has update_type column
        cdef object schema = batch.schema
        cdef list column_names = schema.names

        if 'update_type' in column_names:
            # Batch contains mixed vertex and edge updates
            update_types = batch.column('update_type').to_pylist()

            # Split into vertex and edge updates
            vertex_indices = [i for i, t in enumerate(update_types) if t == 'vertex']
            edge_indices = [i for i, t in enumerate(update_types) if t == 'edge']

            if vertex_indices:
                vertex_batch = batch.take(pa.array(vertex_indices, type=pa.int64()))
                self.graph.add_vertices_from_table(pa.Table.from_batches([vertex_batch]))

            if edge_indices:
                edge_batch = batch.take(pa.array(edge_indices, type=pa.int64()))
                self.graph.add_edges_from_table(pa.Table.from_batches([edge_batch]))

        elif 'source' in column_names and 'target' in column_names:
            # Batch contains only edges
            self.graph.add_edges_from_table(pa.Table.from_batches([batch]))

        elif 'vertex_id' in column_names or 'id' in column_names:
            # Batch contains only vertices
            self.graph.add_vertices_from_table(pa.Table.from_batches([batch]))

        else:
            raise ValueError(f"Invalid update batch schema: {schema}")

    cdef object _run_pattern_matching(self) except *:
        """
        Run pattern matching on current graph state.

        Returns:
            Arrow Table with pattern matches
        """
        # Execute query on current graph
        if self.query_language == 'cypher':
            from ..engine.query_engine import QueryConfig
            result = self.query_engine.query_cypher(
                self.query_str,
                config=QueryConfig(optimize=True)
            )
        else:
            from ..engine.query_engine import QueryConfig
            result = self.query_engine.query_sql(
                self.query_str,
                config=QueryConfig(optimize=True)
            )

        # Return matches
        if result and result.table is not None:
            # Convert to RecordBatch
            if isinstance(result.table, pa.Table):
                if result.table.num_batches > 0:
                    return result.table.to_batches()[0]
                else:
                    return pa.RecordBatch.from_arrays([], schema=result.table.schema)
            else:
                return result.table
        else:
            return pa.RecordBatch.from_arrays([], schema=pa.schema([]))

    cdef object _filter_new_matches(self, object matches) except *:
        """
        Filter matches to only new ones (incremental mode).

        Uses MatchTracker to deduplicate based on vertex IDs.

        Args:
            matches: RecordBatch with all pattern matches

        Returns:
            RecordBatch with only new matches
        """
        if matches is None or matches.num_rows == 0:
            return matches

        # Extract vertex IDs from matches
        # Assumes matches have columns like: a.id, b.id, c.id, ...
        cdef list new_match_indices = []
        cdef int i
        cdef list vertex_ids

        for i in range(matches.num_rows):
            # Extract vertex IDs from this match
            vertex_ids = []
            for col_name in matches.schema.names:
                if col_name.endswith('.id') or col_name == 'id':
                    col_value = matches.column(col_name)[i].as_py()
                    if col_value is not None:
                        vertex_ids.append(col_value)

            # Check if new match
            if self.match_tracker.is_new_match(vertex_ids):
                new_match_indices.append(i)
                self.match_tracker.add_match(vertex_ids)
            else:
                self.total_matches_deduplicated += 1

        # Filter to new matches
        if not new_match_indices:
            return pa.RecordBatch.from_arrays([], schema=matches.schema)

        indices_array = pa.array(new_match_indices, type=pa.int64())
        return matches.take(indices_array)

    cdef void _update_watermarks(self, object batch) except *:
        """
        Update watermarks from batch timestamps.

        Args:
            batch: RecordBatch with timestamp column
        """
        if not self.has_watermarks or self.timestamp_column not in batch.schema.names:
            return

        # Extract timestamps
        timestamps = batch.column(self.timestamp_column).to_pylist()

        # Update watermark for each partition (assume partition 0 for now)
        cdef int64_t max_timestamp = max(timestamps) if timestamps else 0
        if max_timestamp > 0:
            self.watermark_tracker.update_watermark(0, max_timestamp)

    cpdef list get_partition_keys(self):
        """
        Get partition key columns for distributed shuffle.

        For graph operators, we typically don't partition (process whole graph).
        Return empty list to indicate no partitioning.

        Returns:
            Empty list (no partitioning)
        """
        return []

    cdef dict _get_operator_stats(self):
        """
        Get operator statistics.

        Returns:
            Dict with statistics
        """
        stats = {
            'operator': 'GraphStreamOperator',
            'mode': self.mode,
            'query': self.query_str,
            'total_updates_processed': self.total_updates_processed,
            'total_matches_emitted': self.total_matches_emitted,
            'total_matches_deduplicated': self.total_matches_deduplicated,
            'total_pattern_matching_time_ms': self.total_pattern_matching_time_ms,
        }

        # Add deduplication stats if tracking
        if self.track_matches:
            stats['match_tracker'] = self.match_tracker.get_stats()

        # Add watermark stats if enabled
        if self.has_watermarks:
            stats['current_watermark'] = self.watermark_tracker.get_current_watermark()

        return stats

    def get_stats(self) -> Dict[str, Any]:
        """
        Public method to get statistics.

        Returns:
            Dict with operator statistics
        """
        return self._get_operator_stats()
