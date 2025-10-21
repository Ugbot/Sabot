# cython: language_level=3, boundscheck=False, wraparound=False, cdivision=True, nonecheck=False
"""
Local Executor Cython Wrapper

When running locally, automatically creates and manages a C++ agent
behind the scenes for high-performance execution.
"""

from libc.stdint cimport int32_t, int64_t
from libcpp cimport bool as cbool
from libcpp.string cimport string
from libcpp.memory cimport shared_ptr, unique_ptr
from libcpp.unordered_map cimport unordered_map
from libcpp.vector cimport vector

# Import Arrow C++ types
cimport pyarrow.lib as ca
from pyarrow.includes.libarrow cimport (
    CRecordBatch as PCRecordBatch,
    CTable as PCTable,
    CSchema as PCSchema,
)

# Import C++ local executor classes
from sabot._c.local_executor cimport (
    LocalExecutor as CppLocalExecutor,
    LocalExecutorConfig as CppLocalExecutorConfig,
    LocalExecutorManager as CppLocalExecutorManager
)

# Import other C++ components
from sabot._c.task_slot_manager cimport TaskSlotManager
from sabot._cython.shuffle.shuffle_transport cimport ShuffleTransport

# Forward declarations
cdef class MarbleDBIntegration


# ============================================================================
# C++ Local Executor Wrapper
# ============================================================================

cdef class LocalExecutor:
    """
    C++ Local Executor wrapper for automatic agent management.
    
    When running locally, automatically creates and manages a C++ agent
    behind the scenes for high-performance execution.
    """
    
    cdef:
        unique_ptr[CppLocalExecutor] _cpp_executor
        CppLocalExecutorConfig _config
        cbool _initialized
    
    def __cinit__(self, str executor_id="default", int memory_mb=1024, 
                  int num_slots=4, int workers_per_slot=2):
        """Initialize C++ local executor."""
        self._config = CppLocalExecutorConfig(
            executor_id.encode('utf-8'),
            memory_mb,
            num_slots,
            workers_per_slot
        )
        
        self._cpp_executor = unique_ptr[CppLocalExecutor](new CppLocalExecutor(self._config))
        self._initialized = True
    
    def __dealloc__(self):
        """Cleanup C++ executor."""
        if self._initialized:
            self._cpp_executor.reset()
    
    cpdef ca.Status initialize(self):
        """Initialize local executor."""
        if not self._initialized:
            return ca.Status.Invalid("LocalExecutor not initialized")
        
        cdef CppLocalExecutor* executor = self._cpp_executor.get()
        cdef arrow.Status status = executor.Initialize()
        
        if not status.ok():
            return ca.Status.Invalid(status.message().decode('utf-8'))
        
        return ca.Status.OK()
    
    cpdef ca.Status shutdown(self):
        """Shutdown local executor."""
        if not self._initialized:
            return ca.Status.OK()
        
        cdef CppLocalExecutor* executor = self._cpp_executor.get()
        cdef arrow.Status status = executor.Shutdown()
        
        if not status.ok():
            return ca.Status.Invalid(status.message().decode('utf-8'))
        
        return ca.Status.OK()
    
    cpdef ca.Status execute_streaming_sql(self, str query, dict input_tables, object output_callback):
        """Execute a streaming SQL query locally."""
        if not self._initialized:
            return ca.Status.Invalid("LocalExecutor not initialized")
        
        # Convert input tables to C++ map
        cdef unordered_map[string, shared_ptr[PCTable]] cpp_tables
        for table_name, table in input_tables.items():
            if table is not None:
                cpp_tables[table_name.encode('utf-8')] = table.sp_table
        
        # Create output callback wrapper
        cdef auto cpp_callback = [output_callback](shared_ptr[PCRecordBatch] batch) -> void:
            if batch.get() != nullptr:
                # Wrap C++ RecordBatch as Python RecordBatch
                cdef ca.RecordBatch py_batch = ca.RecordBatch.__new__(ca.RecordBatch)
                py_batch.init(batch)
                output_callback(py_batch)
        
        cdef CppLocalExecutor* executor = self._cpp_executor.get()
        cdef arrow.Status status = executor.ExecuteStreamingSQL(
            query.encode('utf-8'),
            cpp_tables,
            cpp_callback
        )
        
        if not status.ok():
            return ca.Status.Invalid(status.message().decode('utf-8'))
        
        return ca.Status.OK()
    
    cpdef ca.Table execute_batch_sql(self, str query, dict input_tables):
        """Execute a batch SQL query locally."""
        if not self._initialized:
            return None
        
        # Convert input tables to C++ map
        cdef unordered_map[string, shared_ptr[PCTable]] cpp_tables
        for table_name, table in input_tables.items():
            if table is not None:
                cpp_tables[table_name.encode('utf-8')] = table.sp_table
        
        cdef CppLocalExecutor* executor = self._cpp_executor.get()
        cdef arrow.Result[shared_ptr[PCTable]] result = executor.ExecuteBatchSQL(
            query.encode('utf-8'),
            cpp_tables
        )
        
        if not result.ok():
            return None
        
        # Wrap C++ Table as Python Table
        cdef shared_ptr[PCTable] cpp_table = result.ValueOrDie()
        cdef ca.Table py_table = ca.Table.__new__(ca.Table)
        py_table.init(cpp_table)
        return py_table
    
    cpdef ca.Status register_dimension_table(self, str table_name, ca.Table table, cbool is_raft_replicated=False):
        """Register a dimension table."""
        if not self._initialized:
            return ca.Status.Invalid("LocalExecutor not initialized")
        
        cdef CppLocalExecutor* executor = self._cpp_executor.get()
        cdef arrow.Status status = executor.RegisterDimensionTable(
            table_name.encode('utf-8'),
            table.sp_table,
            is_raft_replicated
        )
        
        if not status.ok():
            return ca.Status.Invalid(status.message().decode('utf-8'))
        
        return ca.Status.OK()
    
    cpdef ca.Status register_streaming_source(self, str source_name, str connector_type, dict config):
        """Register a streaming source."""
        if not self._initialized:
            return ca.Status.Invalid("LocalExecutor not initialized")
        
        # Convert config to C++ map
        cdef unordered_map[string, string] cpp_config
        for key, value in config.items():
            cpp_config[key.encode('utf-8')] = str(value).encode('utf-8')
        
        cdef CppLocalExecutor* executor = self._cpp_executor.get()
        cdef arrow.Status status = executor.RegisterStreamingSource(
            source_name.encode('utf-8'),
            connector_type.encode('utf-8'),
            cpp_config
        )
        
        if not status.ok():
            return ca.Status.Invalid(status.message().decode('utf-8'))
        
        return ca.Status.OK()
    
    cpdef MarbleDBIntegration get_marbledb(self):
        """Get embedded MarbleDB instance."""
        if not self._initialized:
            return None
        
        cdef CppLocalExecutor* executor = self._cpp_executor.get()
        cdef shared_ptr[MarbleDBIntegration] marbledb = executor.GetMarbleDB()
        
        if marbledb.get() == nullptr:
            return None
        
        # Wrap C++ MarbleDBIntegration in Python wrapper
        cdef MarbleDBIntegration wrapper = MarbleDBIntegration.__new__(MarbleDBIntegration)
        wrapper._cpp_integration = marbledb
        return wrapper
    
    cpdef TaskSlotManager get_task_slot_manager(self):
        """Get task slot manager."""
        if not self._initialized:
            return None
        
        cdef CppLocalExecutor* executor = self._cpp_executor.get()
        cdef shared_ptr[TaskSlotManager] slot_manager = executor.GetTaskSlotManager()
        
        if slot_manager.get() == nullptr:
            return None
        
        # Wrap C++ TaskSlotManager in Python wrapper
        cdef TaskSlotManager wrapper = TaskSlotManager.__new__(TaskSlotManager)
        wrapper._cpp_manager = slot_manager
        return wrapper
    
    cpdef cbool is_initialized(self):
        """Check if initialized."""
        if not self._initialized:
            return False
        
        cdef CppLocalExecutor* executor = self._cpp_executor.get()
        return executor.IsInitialized()
    
    cpdef str get_executor_id(self):
        """Get executor ID."""
        if not self._initialized:
            return ""
        
        cdef CppLocalExecutor* executor = self._cpp_executor.get()
        return executor.GetExecutorId().decode('utf-8')


# ============================================================================
# Local Executor Manager Wrapper
# ============================================================================

cdef class LocalExecutorManager:
    """
    C++ LocalExecutorManager wrapper for singleton management.
    """
    
    cdef:
        CppLocalExecutorManager* _cpp_manager
        cbool _initialized
    
    def __cinit__(self):
        """Initialize local executor manager wrapper."""
        self._cpp_manager = &CppLocalExecutorManager.GetInstance()
        self._initialized = True
    
    def __dealloc__(self):
        """Cleanup C++ manager."""
        if self._initialized:
            self._cpp_manager.ShutdownAll()
    
    cpdef LocalExecutor get_or_create_executor(self, str executor_id="default"):
        """Get or create local executor."""
        if not self._initialized:
            return None
        
        cdef shared_ptr[CppLocalExecutor] cpp_executor = self._cpp_manager.GetOrCreateExecutor(executor_id.encode('utf-8'))
        
        if cpp_executor.get() == nullptr:
            return None
        
        # Wrap C++ LocalExecutor in Python wrapper
        cdef LocalExecutor wrapper = LocalExecutor.__new__(LocalExecutor)
        wrapper._cpp_executor = unique_ptr[CppLocalExecutor](cpp_executor.get())
        wrapper._initialized = True
        return wrapper
    
    cpdef void shutdown_all(self):
        """Shutdown all executors."""
        if self._initialized:
            self._cpp_manager.ShutdownAll()


# ============================================================================
# Global Local Executor Manager Instance
# ============================================================================

cdef LocalExecutorManager _global_local_executor_manager = LocalExecutorManager()

def get_local_executor_manager():
    """Get global local executor manager instance."""
    return _global_local_executor_manager

def get_or_create_local_executor(executor_id="default"):
    """Get or create local executor (convenience function)."""
    return _global_local_executor_manager.get_or_create_executor(executor_id)
