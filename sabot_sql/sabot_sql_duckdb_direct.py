"""
Direct DuckDB SQL Bridge (Temporary)

Since the Cython wrapper has build issues, use DuckDB directly for now.
This is REAL SQL execution, not a mock.
"""

import pyarrow as pa
from sabot import cyarrow as ca


class SabotSQLBridge:
    """SQL bridge using DuckDB directly for real SQL execution."""
    
    def __init__(self):
        import duckdb
        self.conn = duckdb.connect(':memory:')
        self.tables = {}
    
    def register_table(self, table_name: str, table: ca.Table):
        """Register an Arrow table."""
        # Convert cyarrow to pyarrow for DuckDB
        if hasattr(table, 'to_pyarrow'):
            arrow_table = table.to_pyarrow()
        else:
            arrow_table = table  # Already pyarrow
        
        # Register with DuckDB
        self.conn.register(table_name, arrow_table)
        self.tables[table_name] = table
        print(f"Registered table '{table_name}' with {arrow_table.num_rows} rows")
    
    def execute_sql(self, sql: str):
        """Execute SQL query using DuckDB parser and Sabot operators."""
        print(f"Executing SQL: {sql}")
        
        # For now, use DuckDB execution directly
        # TODO: Parse with DuckDB, translate to Sabot operators, execute with Arrow
        # This will happen when:
        # 1. DuckDB parser extracts logical plan
        # 2. Translator builds Sabot operator tree
        # 3. MorselExecutor runs operators
        
        # Execute with DuckDB
        result = self.conn.execute(sql).fetch_arrow_table()
        
        # Return as Arrow table (compatible with benchmarks)
        return result
    
    def __del__(self):
        if hasattr(self, 'conn'):
            self.conn.close()


def create_sabot_sql_bridge():
    """Create a SQL bridge instance."""
    return SabotSQLBridge()

