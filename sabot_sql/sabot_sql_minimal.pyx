# distutils: language = c++
# cython: language_level=3

from libcpp.memory cimport shared_ptr
from libcpp.string cimport string
from libcpp cimport bool as cbool

# Arrow C++ types
cdef extern from "arrow/api.h" namespace "arrow" nogil:
    cdef cppclass CTable "arrow::Table":
        pass
    
    cdef cppclass CStatus "arrow::Status":
        cbool ok()
        string ToString()
    
    cdef cppclass CResult "arrow::Result"[T]:
        cbool ok()
        T ValueOrDie()
        CStatus status()

# PyArrow bridge functions
cdef extern from "arrow/python/pyarrow.h":
    shared_ptr[CTable] pyarrow_unwrap_table(object table) except *
    object pyarrow_wrap_table(const shared_ptr[CTable]& table)

# SabotSQL C++ bridge
cdef extern from "sabot_sql/sql/simple_sabot_sql_bridge.h" namespace "sabot_sql::sql":
    cdef cppclass CSabotSQLBridge "sabot_sql::sql::SabotSQLBridge":
        @staticmethod
        CResult[shared_ptr[CSabotSQLBridge]] Create()
        
        CStatus RegisterTable(const string& table_name, const shared_ptr[CTable]& table)
        CResult[shared_ptr[CTable]] ExecuteSQL(const string& sql)
        cbool TableExists(const string& table_name)

# Python wrapper
cdef class SabotSQLBridge:
    cdef shared_ptr[CSabotSQLBridge] c_bridge
    
    def __cinit__(self):
        cdef CResult[shared_ptr[CSabotSQLBridge]] result = CSabotSQLBridge.Create()
        if not result.ok():
            raise RuntimeError(b"Failed to create SabotSQL bridge")
        self.c_bridge = result.ValueOrDie()
    
    def register_table(self, str table_name, table):
        """Register an Arrow table"""
        print(f"Registering table '{table_name}'")
        
        cdef string c_name = table_name.encode('utf-8')
        cdef shared_ptr[CTable] c_table = pyarrow_unwrap_table(table)
        
        cdef CStatus status = self.c_bridge.get().RegisterTable(c_name, c_table)
        if not status.ok():
            raise RuntimeError(status.ToString().decode('utf-8'))
    
    def execute_sql(self, str sql):
        """Execute SQL query"""
        print(f"Executing SQL: {sql}")
        
        cdef string c_sql = sql.encode('utf-8')
        cdef CResult[shared_ptr[CTable]] result = self.c_bridge.get().ExecuteSQL(c_sql)
        
        if not result.ok():
            raise RuntimeError(result.status().ToString().decode('utf-8'))
        
        cdef shared_ptr[CTable] c_table = result.ValueOrDie()
        return pyarrow_wrap_table(c_table)

def create_sabot_sql_bridge():
    """Create a new SabotSQL bridge"""
    return SabotSQLBridge()

