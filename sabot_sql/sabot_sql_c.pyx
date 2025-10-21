# distutils: language = c++
# cython: language_level=3

"""
Minimal Cython wrapper using C API - no Arrow Python bridge needed
"""

from libc.stdlib cimport free
from libc.stdint cimport int64_t
import pyarrow as pa
from pyarrow.lib import Table

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

# Arrow C Data Interface
cdef extern from "arrow/c/abi.h":
    struct ArrowSchema:
        pass
    
    struct ArrowArray:
        pass

cdef extern from "arrow/c/bridge.h" namespace "arrow":
    cdef cppclass Result[T]:
        pass
    
    # Export Arrow table to C Data Interface
    Result[void] ExportTable(const Table& table, ArrowArray* out_array, ArrowSchema* out_schema) except +
    
    # Import from C Data Interface
    Result[Table] ImportTable(const ArrowArray* array, const ArrowSchema* schema) except +

cdef class SabotSQLBridge:
    """Minimal Cython wrapper using C API"""
    
    cdef SabotSQLBridgeHandle handle
    cdef dict _tables
    
    def __cinit__(self):
        self.handle = sabot_sql_bridge_create()
        if self.handle == NULL:
            raise RuntimeError("Failed to create SabotSQL bridge")
        self._tables = {}
    
    def __dealloc__(self):
        if self.handle != NULL:
            sabot_sql_bridge_destroy(self.handle)
    
    def register_table(self, str table_name, table):
        """Register an Arrow table"""
        print(f"Registering table '{table_name}' with {table.num_rows} rows")
        
        # Store Python reference to keep table alive
        self._tables[table_name] = table
        
        # Get C++ table pointer from PyArrow
        # Use ctypes to get the address
        cdef int64_t table_addr = <int64_t><void*>table
        
        # Pass as handle (simplified - just store Python object)
        cdef int result = sabot_sql_bridge_register_table(
            self.handle,
            table_name.encode('utf-8'),
            <ArrowTableHandle><void*>table_addr
        )
        
        if result != 0:
            raise RuntimeError(f"Failed to register table: {table_name}")
    
    def execute_sql(self, str sql):
        """Execute SQL query"""
        print(f"Executing SQL: {sql}")
        
        cdef char* error_msg = NULL
        cdef ArrowTableHandle result_handle = sabot_sql_bridge_execute_sql(
            self.handle,
            sql.encode('utf-8'),
            &error_msg
        )
        
        if result_handle == NULL:
            error_str = error_msg.decode('utf-8') if error_msg != NULL else "Unknown error"
            if error_msg != NULL:
                free(error_msg)
            raise RuntimeError(f"SQL execution failed: {error_str}")
        
        # Get table info
        cdef int num_rows = sabot_sql_bridge_get_table_rows(result_handle)
        cdef int num_cols = sabot_sql_bridge_get_table_cols(result_handle)
        
        print(f"Result: {num_rows} rows, {num_cols} columns")
        
        # For now, return a placeholder
        # TODO: Convert C++ table handle to Python Arrow table
        # This requires proper Arrow C Data Interface handling
        
        # Temporary: return empty table
        return pa.table({})

def create_sabot_sql_bridge():
    """Create a new SabotSQL bridge"""
    return SabotSQLBridge()

