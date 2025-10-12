# cython: language_level=3
"""
SabotSQL Operator Cython declarations
"""

from sabot._cython.operators.base_operator cimport BaseOperator


cdef class CythonSQLOperator(BaseOperator):
    """
    Cython wrapper for SabotSQL engine.
    
    Executes SQL queries using the integrated SabotSQL C++ engine with:
    - Flink SQL extensions (TUMBLE, HOP, SESSION windows)
    - QuestDB SQL extensions (SAMPLE BY, LATEST BY, ASOF JOIN)
    - Sabot-only execution (no DuckDB physical runtime)
    - Arrow-based zero-copy data flow
    - Morsel-driven parallel execution
    """
    cdef:
        object _sql_query               # SQL query string
        dict _source_tables             # Dict of table_name -> ca.Table
        object _bridge                  # SabotSQLBridge instance (Python)
        object _result_table            # Cached result table
        bint _executed                  # Whether query has been executed
        list _partition_keys            # Partition keys for shuffle
        bint _requires_shuffle          # Whether this query needs network shuffle
        
        # Query metadata from plan
        bint _has_joins
        bint _has_asof_joins
        bint _has_aggregates
        bint _has_windows
        
    # Processing methods
    cpdef object process_table(self)
    cpdef object process_batch(self, object batch)
    cpdef bint requires_shuffle(self)
    cpdef list get_partition_keys(self)
    
    # Internal methods
    cdef void _execute_query(self)
    cdef void _analyze_query_plan(self)

