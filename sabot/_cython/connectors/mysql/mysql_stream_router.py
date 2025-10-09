"""
Per-table stream routing for MySQL CDC.

Routes CDC events from different tables to separate output streams,
enabling per-table processing pipelines and distributed routing.
"""

from typing import Dict, List, Optional, Callable, Any
from dataclasses import dataclass, field
from collections import defaultdict
import logging

from sabot import cyarrow as ca

logger = logging.getLogger(__name__)


@dataclass
class TableRoute:
    """
    Route configuration for a table.

    Defines where events from a table should be sent.
    """
    database: str
    table: str
    output_key: str  # Routing key (e.g., "orders", "users", "events_click")
    predicate: Optional[Callable[[Any], bool]] = None  # Optional filter function

    @property
    def table_spec(self) -> str:
        """Get database.table specification."""
        return f"{self.database}.{self.table}"


class MySQLStreamRouter:
    """
    Route MySQL CDC events to separate streams per table.

    Enables per-table processing with different downstream operators:
    - Route orders to order processing pipeline
    - Route users to user enrichment pipeline
    - Route events to analytics pipeline

    Example:
        >>> router = MySQLStreamRouter()
        >>>
        >>> # Configure routes
        >>> router.add_route("ecommerce", "orders", output_key="orders")
        >>> router.add_route("ecommerce", "users", output_key="users")
        >>> router.add_route("ecommerce", "events_click", output_key="events")
        >>>
        >>> # Route a batch
        >>> for batch in cdc_batches:
        ...     routed_batches = router.route_batch(batch)
        ...     for key, batch in routed_batches.items():
        ...         print(f"Route {key}: {batch.num_rows} rows")
    """

    def __init__(self):
        """Initialize stream router."""
        self._routes: Dict[str, TableRoute] = {}  # table_spec -> TableRoute
        self._output_keys: Dict[str, List[str]] = defaultdict(list)  # output_key -> [table_specs]

    def add_route(
        self,
        database: str,
        table: str,
        output_key: str,
        predicate: Optional[Callable[[Any], bool]] = None
    ):
        """
        Add routing rule for table.

        Args:
            database: Database name
            table: Table name
            output_key: Output stream key
            predicate: Optional filter function (row -> bool)
        """
        route = TableRoute(
            database=database,
            table=table,
            output_key=output_key,
            predicate=predicate
        )

        self._routes[route.table_spec] = route
        self._output_keys[output_key].append(route.table_spec)

        logger.info(f"Added route: {route.table_spec} -> {output_key}")

    def remove_route(self, database: str, table: str):
        """
        Remove routing rule for table.

        Args:
            database: Database name
            table: Table name
        """
        table_spec = f"{database}.{table}"
        if table_spec in self._routes:
            route = self._routes.pop(table_spec)
            self._output_keys[route.output_key].remove(table_spec)

            if not self._output_keys[route.output_key]:
                del self._output_keys[route.output_key]

            logger.info(f"Removed route: {table_spec}")

    def get_route(self, database: str, table: str) -> Optional[TableRoute]:
        """
        Get route for table.

        Args:
            database: Database name
            table: Table name

        Returns:
            TableRoute if exists, None otherwise
        """
        return self._routes.get(f"{database}.{table}")

    def get_output_key(self, database: str, table: str) -> Optional[str]:
        """
        Get output key for table.

        Args:
            database: Database name
            table: Table name

        Returns:
            Output key if route exists, None otherwise
        """
        route = self.get_route(database, table)
        return route.output_key if route else None

    def route_batch(
        self,
        batch: ca.RecordBatch
    ) -> Dict[str, ca.RecordBatch]:
        """
        Route CDC batch to separate output batches per table.

        Args:
            batch: Input CDC batch with columns: event_type, schema, table, ...

        Returns:
            Dictionary mapping output_key -> RecordBatch

        Example:
            Input batch (3 rows):
                schema='ecommerce', table='orders'  -> output_key='orders'
                schema='ecommerce', table='users'   -> output_key='users'
                schema='ecommerce', table='orders'  -> output_key='orders'

            Output:
                {
                    'orders': RecordBatch (2 rows),
                    'users': RecordBatch (1 row)
                }
        """
        if batch.num_rows == 0:
            return {}

        # Get schema and table columns
        batch_dict = batch.to_pydict()
        schemas = batch_dict['schema']
        tables = batch_dict['table']

        # Group rows by output key
        output_batches: Dict[str, List[int]] = defaultdict(list)  # output_key -> row_indices

        for i in range(batch.num_rows):
            schema = schemas[i]
            table = tables[i]

            # Find route
            route = self.get_route(schema, table)
            if route:
                # Apply predicate if configured
                if route.predicate:
                    # Get row data for predicate
                    row_data = {col: batch_dict[col][i] for col in batch_dict.keys()}
                    if not route.predicate(row_data):
                        continue  # Skip this row

                output_batches[route.output_key].append(i)
            else:
                # No route configured - send to default output
                output_batches['default'].append(i)

        # Create output batches
        result = {}
        for output_key, row_indices in output_batches.items():
            # Filter batch to selected rows
            result[output_key] = self._filter_batch(batch, row_indices)

        logger.debug(f"Routed batch: {batch.num_rows} rows -> " +
                    f"{', '.join(f'{k}:{v.num_rows}' for k, v in result.items())}")

        return result

    def _filter_batch(
        self,
        batch: ca.RecordBatch,
        row_indices: List[int]
    ) -> ca.RecordBatch:
        """
        Filter batch to selected rows.

        Args:
            batch: Input batch
            row_indices: List of row indices to keep

        Returns:
            Filtered batch
        """
        if not row_indices:
            # Return empty batch with same schema
            return ca.RecordBatch.from_arrays(
                [ca.array([], type=field.type) for field in batch.schema],
                schema=batch.schema
            )

        # Use Arrow's take() to select rows
        import cyarrow.compute as pc
        indices_array = ca.array(row_indices, type=ca.int64())

        filtered_columns = []
        for column in batch:
            filtered_columns.append(pc.take(column, indices_array))

        return ca.RecordBatch.from_arrays(filtered_columns, schema=batch.schema)

    def get_routes(self) -> List[TableRoute]:
        """
        Get all configured routes.

        Returns:
            List of TableRoute objects
        """
        return list(self._routes.values())

    def get_output_keys(self) -> List[str]:
        """
        Get all output keys.

        Returns:
            List of output keys
        """
        return list(self._output_keys.keys())

    def get_tables_for_output(self, output_key: str) -> List[str]:
        """
        Get table specs routed to output key.

        Args:
            output_key: Output key

        Returns:
            List of table specs (database.table)
        """
        return self._output_keys.get(output_key, [])

    def clear_routes(self):
        """Clear all routes."""
        self._routes.clear()
        self._output_keys.clear()
        logger.info("Cleared all routes")


def create_table_router(
    table_routes: Dict[str, str]
) -> MySQLStreamRouter:
    """
    Create router from simple table->output_key mapping.

    Args:
        table_routes: Dictionary mapping "database.table" -> "output_key"

    Returns:
        Configured MySQLStreamRouter

    Example:
        >>> router = create_table_router({
        ...     "ecommerce.orders": "orders",
        ...     "ecommerce.users": "users",
        ...     "ecommerce.events_click": "events"
        ... })
    """
    router = MySQLStreamRouter()

    for table_spec, output_key in table_routes.items():
        if '.' not in table_spec:
            raise ValueError(f"Invalid table spec (must be database.table): {table_spec}")

        database, table = table_spec.split('.', 1)
        router.add_route(database, table, output_key)

    return router
