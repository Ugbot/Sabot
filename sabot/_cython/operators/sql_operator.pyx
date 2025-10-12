# cython: language_level=3, boundscheck=False, wraparound=False, cdivision=True, nonecheck=False
"""
SabotSQL Operator - Cython bindings for SQL execution

This operator enables SQL queries to be executed using Sabot's morsel-driven
execution engine with integrated Flink and QuestDB extensions.

Features:
- Standard SQL + Flink SQL (windows, watermarks) + QuestDB SQL (ASOF, SAMPLE BY)
- Sabot-only execution (no DuckDB physical runtime)
- Arrow-based zero-copy data flow
- Morsel-driven parallel execution
- Network shuffle for stateful operations
"""

import cython
from sabot._cython.operators.base_operator cimport BaseOperator


cdef class CythonSQLOperator(BaseOperator):
    """
    SQL operator that integrates with Sabot's execution engine.
    
    Usage:
        from sabot._cython.operators.sql_operator import CythonSQLOperator
        from sabot import cyarrow as ca
        
        # Create operator with SQL and source tables
        sql_op = CythonSQLOperator(
            sql="SELECT * FROM trades ASOF JOIN quotes ON trades.symbol = quotes.symbol AND trades.ts <= quotes.ts",
            sources={"trades": trades_table, "quotes": quotes_table}
        )
        
        # Execute and get results
        result = sql_op.process_table()
        
        # Can also wrap with MorselDrivenOperator for parallel execution
        from sabot._cython.operators.morsel_operator import MorselDrivenOperator
        parallel_op = MorselDrivenOperator(sql_op, num_workers=8)
    """
    
    def __cinit__(
        self,
        str sql_query,
        dict sources = None,
        int num_workers = 0,
        **kwargs
    ):
        """
        Initialize SQL operator.
        
        Args:
            sql_query: SQL query string (supports standard SQL, Flink SQL, QuestDB SQL)
            sources: Dict of table_name -> ca.Table sources
            num_workers: Number of workers for parallel execution
        """
        self._sql_query = sql_query
        self._source_tables = sources if sources is not None else {}
        self._executed = False
        self._result_table = None
        self._partition_keys = []
        self._requires_shuffle = False
        
        # Query flags
        self._has_joins = False
        self._has_asof_joins = False
        self._has_aggregates = False
        self._has_windows = False
        
        # Copy schema from source if only one source
        if len(self._source_tables) == 1:
            first_table = list(self._source_tables.values())[0]
            self._schema = first_table.schema
        else:
            self._schema = None
        
        # Initialize bridge
        self._initialize_bridge()
        
        # Analyze query plan
        self._analyze_query_plan()
    
    def _initialize_bridge(self):
        """Initialize SabotSQL bridge and register source tables."""
        try:
            # Import the Python SabotSQL module (backed by C++ dylib)
            from sabot_sql import create_sabot_sql_bridge
            
            self._bridge = create_sabot_sql_bridge()
            
            # Register all source tables
            for table_name, table in self._source_tables.items():
                # Convert cyarrow to pyarrow if needed
                import pyarrow as pa
                if hasattr(table, '_to_pyarrow'):
                    pa_table = table._to_pyarrow()
                else:
                    pa_table = table
                
                self._bridge.register_table(table_name, pa_table)
                
        except ImportError as e:
            raise ImportError(f"Failed to import sabot_sql module: {e}. Ensure SabotSQL is built.")
    
    cdef void _analyze_query_plan(self):
        """Analyze query plan to determine execution requirements."""
        try:
            # Parse and optimize SQL
            plan = self._bridge.parse_and_optimize(self._sql_query)
            
            # Extract query metadata
            self._has_joins = plan.get('has_joins', False)
            self._has_asof_joins = plan.get('has_asof_joins', False)
            self._has_aggregates = plan.get('has_aggregates', False)
            self._has_windows = plan.get('has_windows', False)
            
            # Determine if shuffle is required
            self._requires_shuffle = self._has_joins or self._has_aggregates or self._has_windows
            
            # Extract partition keys if available
            if 'join_key_columns' in plan and plan['join_key_columns']:
                # Extract just the column names (strip table prefixes if present)
                for key in plan['join_key_columns']:
                    col_name = key.split('.')[-1] if '.' in key else key
                    self._partition_keys.append(col_name)
            
        except Exception as e:
            import logging
            logging.warning(f"Failed to analyze SQL plan: {e}")
    
    cdef void _execute_query(self):
        """Execute the SQL query if not already executed."""
        if self._executed:
            return
        
        try:
            # Execute SQL via bridge
            result = self._bridge.execute_sql(self._sql_query)
            
            # Convert pyarrow to cyarrow if needed
            from sabot import cyarrow as ca
            if hasattr(ca.Table, 'from_pyarrow'):
                self._result_table = ca.Table.from_pyarrow(result)
            else:
                self._result_table = result
            
            self._executed = True
            
        except Exception as e:
            raise RuntimeError(f"SQL execution failed: {e}")
    
    cpdef object process_table(self):
        """
        Execute SQL query and return result table.
        
        Returns:
            ca.Table with query results
        """
        self._execute_query()
        return self._result_table
    
    cpdef object process_batch(self, object batch):
        """
        Process a single batch (for streaming integration).
        
        For SQL operators, we execute the entire query once and return
        the result. Subsequent calls return None.
        
        Args:
            batch: Input batch (ignored for SQL queries)
            
        Returns:
            RecordBatch from query results, or None if already returned
        """
        if not self._executed:
            self._execute_query()
            
            # Convert table to batch
            if self._result_table is not None and self._result_table.num_rows > 0:
                from sabot import cyarrow as ca
                # Return as single batch
                return self._result_table.combine_chunks().to_batches()[0]
        
        return None
    
    cpdef bint requires_shuffle(self):
        """
        Check if this operator requires network shuffle.
        
        SQL operators with joins, aggregations, or windows need shuffle
        to co-locate data by keys across agents.
        
        Returns:
            True if shuffle is required
        """
        return self._requires_shuffle
    
    cpdef list get_partition_keys(self):
        """
        Get partition keys for shuffle.
        
        Returns:
            List of column names to partition by
        """
        return self._partition_keys
    
    def get_sql(self):
        """Get the SQL query string."""
        return self._sql_query
    
    def get_plan_info(self):
        """Get query plan metadata."""
        return {
            'has_joins': self._has_joins,
            'has_asof_joins': self._has_asof_joins,
            'has_aggregates': self._has_aggregates,
            'has_windows': self._has_windows,
            'requires_shuffle': self._requires_shuffle,
            'partition_keys': self._partition_keys
        }
    
    def __repr__(self):
        return f"CythonSQLOperator(sql='{self._sql_query[:50]}...', sources={list(self._source_tables.keys())})"

