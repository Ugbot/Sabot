# distutils: language = c
# cython: language_level=3

"""
Ultra-minimal Cython wrapper using pure C API
"""

from libc.stdlib cimport free
from libc.stdint cimport int64_t
import pyarrow as pa

# C API declarations
cdef extern from "sabot_sql/sql/c_api.h":
    ctypedef void* SabotSQLBridgeHandle
    ctypedef void* ArrowTableHandle
    
    SabotSQLBridgeHandle sabot_sql_bridge_create()
    void sabot_sql_bridge_destroy(SabotSQLBridgeHandle handle)
    int sabot_sql_bridge_register_table(SabotSQLBridgeHandle handle, const char* table_name, ArrowTableHandle table)
    ArrowTableHandle sabot_sql_bridge_execute_sql(SabotSQLBridgeHandle handle, const char* sql, char** error)
    int sabot_sql_bridge_get_table_rows(ArrowTableHandle table)
    int sabot_sql_bridge_get_table_cols(ArrowTableHandle table)

cdef class SabotSQLBridge:
    """Minimal C API wrapper"""
    
    cdef SabotSQLBridgeHandle handle
    cdef dict _tables
    
    def __cinit__(self):
        self.handle = sabot_sql_bridge_create()
        if self.handle == NULL:
            raise RuntimeError("Failed to create bridge")
        self._tables = {}
    
    def __dealloc__(self):
        if self.handle != NULL:
            sabot_sql_bridge_destroy(self.handle)
    
    def register_table(self, str table_name, table):
        """Register table"""
        print(f"Registering table '{table_name}' with {table.num_rows} rows")
        
        # Keep Python reference
        self._tables[table_name] = table
        
        # Pass table address as handle
        cdef int64_t addr = id(table)
        cdef int result = sabot_sql_bridge_register_table(
            self.handle,
            table_name.encode('utf-8'),
            <ArrowTableHandle><void*>addr
        )
        
        if result != 0:
            raise RuntimeError(f"Failed to register table")
    
    def execute_sql(self, str sql):
        """Execute SQL"""
        print(f"Executing SQL: {sql}")
        
        cdef char* error_msg = NULL
        cdef ArrowTableHandle result = sabot_sql_bridge_execute_sql(
            self.handle,
            sql.encode('utf-8'),
            &error_msg
        )
        
        if result == NULL:
            err = error_msg.decode('utf-8') if error_msg != NULL else "Unknown error"
            if error_msg != NULL:
                free(error_msg)
            raise RuntimeError(f"SQL failed: {err}")
        
        # Get dimensions
        cdef int rows = sabot_sql_bridge_get_table_rows(result)
        cdef int cols = sabot_sql_bridge_get_table_cols(result)
        
        print(f"Got result: {rows} rows, {cols} cols")
        
        # TODO: Convert handle to PyArrow table
        # For now return empty table
        return pa.table({})

def create_sabot_sql_bridge():
    return SabotSQLBridge()


