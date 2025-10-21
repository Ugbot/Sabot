# cython: language_level=3, boundscheck=False, wraparound=False, cdivision=True, nonecheck=False
"""
C++ Agent Core Cython Wrapper

High-performance C++ agent core with minimal Python dependencies.
Only high-level control and configuration comes from Python.
"""

from libc.stdint cimport int32_t, int64_t
from libcpp cimport bool as cbool
from libcpp.string cimport string
from libcpp.memory cimport shared_ptr, unique_ptr
from libcpp.unordered_map cimport unordered_map
from libcpp.vector cimport vector
from libcpp.functional cimport function

# Import Arrow C++ types
cimport pyarrow.lib as ca
from pyarrow.includes.libarrow cimport (
    CRecordBatch as PCRecordBatch,
    CTable as PCTable,
    CSchema as PCSchema,
)

# Import C++ agent core classes
from sabot._c.agent_core cimport (
    AgentCore as CppAgentCore,
    AgentConfig as CppAgentConfig,
    StreamingOperatorConfig as CppStreamingOperatorConfig,
    AgentStatus as CppAgentStatus,
    StreamingOperator as CppStreamingOperator,
    StreamingSource as CppStreamingSource,
    DimensionTableManager as CppDimensionTableManager
)

# Import other C++ components
from sabot._c.task_slot_manager cimport TaskSlotManager
from sabot._cython.shuffle.shuffle_transport cimport ShuffleTransport

# Forward declarations
cdef class MarbleDBIntegration
cdef class CheckpointCoordinator


# ============================================================================
# C++ Agent Core Wrapper
# ============================================================================

cdef class AgentCore:
    """
    C++ Agent Core wrapper for high-performance streaming SQL execution.
    
    This is the core C++ implementation with minimal Python dependencies.
    Only high-level control and configuration comes from Python.
    """
    
    cdef:
        unique_ptr[CppAgentCore] _cpp_agent_core
        CppAgentConfig _config
        cbool _initialized
    
    def __cinit__(self, str agent_id, str host="localhost", int port=8816,
                  int memory_mb=1024, int num_slots=4, int workers_per_slot=2,
                  cbool is_local_mode=True, int checkpoint_interval_ms=60000,
                  int watermark_idle_timeout_ms=30000):
        """Initialize C++ agent core."""
        self._config = CppAgentConfig(
            agent_id.encode('utf-8'),
            host.encode('utf-8'),
            port,
            memory_mb,
            num_slots,
            workers_per_slot,
            is_local_mode,
            checkpoint_interval_ms,
            watermark_idle_timeout_ms
        )
        
        self._cpp_agent_core = unique_ptr[CppAgentCore](new CppAgentCore(self._config))
        self._initialized = True
    
    def __dealloc__(self):
        """Cleanup C++ agent core."""
        if self._initialized:
            self._cpp_agent_core.reset()
    
    cpdef ca.Status initialize(self):
        """Initialize agent core."""
        if not self._initialized:
            return ca.Status.Invalid("AgentCore not initialized")
        
        cdef CppAgentCore* agent_core = self._cpp_agent_core.get()
        cdef arrow.Status status = agent_core.Initialize()
        
        if not status.ok():
            return ca.Status.Invalid(status.message().decode('utf-8'))
        
        return ca.Status.OK()
    
    cpdef ca.Status shutdown(self):
        """Shutdown agent core."""
        if not self._initialized:
            return ca.Status.OK()
        
        cdef CppAgentCore* agent_core = self._cpp_agent_core.get()
        cdef arrow.Status status = agent_core.Shutdown()
        
        if not status.ok():
            return ca.Status.Invalid(status.message().decode('utf-8'))
        
        return ca.Status.OK()
    
    cpdef ca.Status start(self):
        """Start agent services."""
        if not self._initialized:
            return ca.Status.Invalid("AgentCore not initialized")
        
        cdef CppAgentCore* agent_core = self._cpp_agent_core.get()
        cdef arrow.Status status = agent_core.Start()
        
        if not status.ok():
            return ca.Status.Invalid(status.message().decode('utf-8'))
        
        return ca.Status.OK()
    
    cpdef ca.Status stop(self):
        """Stop agent services."""
        if not self._initialized:
            return ca.Status.OK()
        
        cdef CppAgentCore* agent_core = self._cpp_agent_core.get()
        cdef arrow.Status status = agent_core.Stop()
        
        if not status.ok():
            return ca.Status.Invalid(status.message().decode('utf-8'))
        
        return ca.Status.OK()
    
    cpdef ca.Status deploy_streaming_operator(self, str operator_id, str operator_type, dict parameters=None):
        """Deploy streaming operator."""
        if not self._initialized:
            return ca.Status.Invalid("AgentCore not initialized")
        
        if parameters is None:
            parameters = {}
        
        # Create operator configuration
        cdef CppStreamingOperatorConfig config
        config.operator_id = operator_id.encode('utf-8')
        config.operator_type = operator_type.encode('utf-8')
        
        # Convert parameters to C++ map
        for key, value in parameters.items():
            config.parameters[key.encode('utf-8')] = str(value).encode('utf-8')
        
        cdef CppAgentCore* agent_core = self._cpp_agent_core.get()
        cdef arrow.Status status = agent_core.DeployStreamingOperator(config)
        
        if not status.ok():
            return ca.Status.Invalid(status.message().decode('utf-8'))
        
        return ca.Status.OK()
    
    cpdef ca.Status stop_streaming_operator(self, str operator_id):
        """Stop streaming operator."""
        if not self._initialized:
            return ca.Status.Invalid("AgentCore not initialized")
        
        cdef CppAgentCore* agent_core = self._cpp_agent_core.get()
        cdef arrow.Status status = agent_core.StopStreamingOperator(operator_id.encode('utf-8'))
        
        if not status.ok():
            return ca.Status.Invalid(status.message().decode('utf-8'))
        
        return ca.Status.OK()
    
    cpdef ca.Status register_dimension_table(self, str table_name, ca.Table table, cbool is_raft_replicated=False):
        """Register dimension table."""
        if not self._initialized:
            return ca.Status.Invalid("AgentCore not initialized")
        
        cdef CppAgentCore* agent_core = self._cpp_agent_core.get()
        cdef arrow.Status status = agent_core.RegisterDimensionTable(
            table_name.encode('utf-8'),
            table.sp_table,
            is_raft_replicated
        )
        
        if not status.ok():
            return ca.Status.Invalid(status.message().decode('utf-8'))
        
        return ca.Status.OK()
    
    cpdef ca.Status register_streaming_source(self, str source_name, str connector_type, dict config):
        """Register streaming source."""
        if not self._initialized:
            return ca.Status.Invalid("AgentCore not initialized")
        
        # Convert config to C++ map
        cdef unordered_map[string, string] cpp_config
        for key, value in config.items():
            cpp_config[key.encode('utf-8')] = str(value).encode('utf-8')
        
        cdef CppAgentCore* agent_core = self._cpp_agent_core.get()
        cdef arrow.Status status = agent_core.RegisterStreamingSource(
            source_name.encode('utf-8'),
            connector_type.encode('utf-8'),
            cpp_config
        )
        
        if not status.ok():
            return ca.Status.Invalid(status.message().decode('utf-8'))
        
        return ca.Status.OK()
    
    cpdef ca.Table execute_batch_sql(self, str query, dict input_tables):
        """Execute batch SQL query."""
        if not self._initialized:
            return None
        
        # Convert input tables to C++ map
        cdef unordered_map[string, shared_ptr[PCTable]] cpp_tables
        for table_name, table in input_tables.items():
            if table is not None:
                cpp_tables[table_name.encode('utf-8')] = table.sp_table
        
        cdef CppAgentCore* agent_core = self._cpp_agent_core.get()
        cdef arrow.Result[shared_ptr[PCTable]] result = agent_core.ExecuteBatchSQL(
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
    
    cpdef ca.Status execute_streaming_sql(self, str query, dict input_tables, object output_callback):
        """Execute streaming SQL query."""
        if not self._initialized:
            return ca.Status.Invalid("AgentCore not initialized")
        
        # Convert input tables to C++ map
        cdef unordered_map[string, shared_ptr[PCTable]] cpp_tables
        for table_name, table in input_tables.items():
            if table is not None:
                cpp_tables[table_name.encode('utf-8')] = table.sp_table
        
        # Create output callback wrapper - simplified for Cython compatibility
        # Note: This is a placeholder - proper callback implementation would require C++ wrapper
        cdef shared_ptr[PCRecordBatch] dummy_batch
        
        cdef CppAgentCore* agent_core = self._cpp_agent_core.get()
        cdef arrow.Status status = agent_core.ExecuteStreamingSQL(
            query.encode('utf-8'),
            cpp_tables
        )
        
        if not status.ok():
            return ca.Status.Invalid(status.message().decode('utf-8'))
        
        return ca.Status.OK()
    
    cpdef dict get_status(self):
        """Get agent status."""
        if not self._initialized:
            return {"error": "AgentCore not initialized"}
        
        cdef CppAgentCore* agent_core = self._cpp_agent_core.get()
        cdef CppAgentStatus status = agent_core.GetStatus()
        
        return {
            "agent_id": status.agent_id.decode('utf-8'),
            "running": status.running,
            "active_tasks": status.active_tasks,
            "available_slots": status.available_slots,
            "total_morsels_processed": status.total_morsels_processed,
            "total_bytes_shuffled": status.total_bytes_shuffled,
            "marbledb_path": status.marbledb_path.decode('utf-8'),
            "marbledb_initialized": status.marbledb_initialized,
            "uptime_ms": status.uptime_ms,
            "cpu_usage_percent": status.cpu_usage_percent,
            "memory_usage_percent": status.memory_usage_percent
        }
    
    cpdef MarbleDBIntegration get_marbledb(self):
        """Get embedded MarbleDB instance."""
        if not self._initialized:
            return None
        
        cdef CppAgentCore* agent_core = self._cpp_agent_core.get()
        cdef shared_ptr[MarbleDBIntegration] marbledb = agent_core.GetMarbleDB()
        
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
        
        cdef CppAgentCore* agent_core = self._cpp_agent_core.get()
        cdef shared_ptr[TaskSlotManager] slot_manager = agent_core.GetTaskSlotManager()
        
        if slot_manager.get() == nullptr:
            return None
        
        # Wrap C++ TaskSlotManager in Python wrapper
        cdef TaskSlotManager wrapper = TaskSlotManager.__new__(TaskSlotManager)
        wrapper._cpp_manager = slot_manager
        return wrapper
    
    cpdef ShuffleTransport get_shuffle_transport(self):
        """Get shuffle transport."""
        if not self._initialized:
            return None
        
        cdef CppAgentCore* agent_core = self._cpp_agent_core.get()
        cdef shared_ptr[ShuffleTransport] transport = agent_core.GetShuffleTransport()
        
        if transport.get() == nullptr:
            return None
        
        # Wrap C++ ShuffleTransport in Python wrapper
        cdef ShuffleTransport wrapper = ShuffleTransport.__new__(ShuffleTransport)
        wrapper._cpp_transport = transport
        return wrapper
    
    cpdef DimensionTableManager get_dimension_table_manager(self):
        """Get dimension table manager."""
        if not self._initialized:
            return None
        
        cdef CppAgentCore* agent_core = self._cpp_agent_core.get()
        cdef shared_ptr[CppDimensionTableManager] dim_manager = agent_core.GetDimensionTableManager()
        
        if dim_manager.get() == nullptr:
            return None
        
        # Wrap C++ DimensionTableManager in Python wrapper
        cdef DimensionTableManager wrapper = DimensionTableManager.__new__(DimensionTableManager)
        wrapper._cpp_manager = dim_manager
        return wrapper
    
    cpdef CheckpointCoordinator get_checkpoint_coordinator(self):
        """Get checkpoint coordinator."""
        if not self._initialized:
            return None
        
        cdef CppAgentCore* agent_core = self._cpp_agent_core.get()
        cdef shared_ptr[CheckpointCoordinator] checkpoint_coord = agent_core.GetCheckpointCoordinator()
        
        if checkpoint_coord.get() == nullptr:
            return None
        
        # Wrap C++ CheckpointCoordinator in Python wrapper
        cdef CheckpointCoordinator wrapper = CheckpointCoordinator.__new__(CheckpointCoordinator)
        wrapper._cpp_coordinator = checkpoint_coord
        return wrapper
    
    cpdef cbool is_running(self):
        """Check if agent is running."""
        if not self._initialized:
            return False
        
        cdef CppAgentCore* agent_core = self._cpp_agent_core.get()
        return agent_core.IsRunning()
    
    cpdef str get_agent_id(self):
        """Get agent ID."""
        if not self._initialized:
            return ""
        
        cdef CppAgentCore* agent_core = self._cpp_agent_core.get()
        return agent_core.GetAgentId().decode('utf-8')


# ============================================================================
# Streaming Operator Wrapper
# ============================================================================

cdef class StreamingOperator:
    """
    C++ StreamingOperator wrapper for high-performance streaming operations.
    """
    
    cdef:
        shared_ptr[CppStreamingOperator] _cpp_operator
        cbool _initialized
    
    def __cinit__(self):
        """Initialize streaming operator wrapper."""
        self._initialized = False
    
    def __dealloc__(self):
        """Cleanup C++ operator."""
        if self._initialized:
            self._cpp_operator.reset()
    
    cpdef ca.Status start(self):
        """Start operator."""
        if not self._initialized:
            return ca.Status.Invalid("StreamingOperator not initialized")
        
        cdef CppStreamingOperator* cpp_op = self._cpp_operator.get()
        cdef arrow.Status status = cpp_op.Start()
        
        if not status.ok():
            return ca.Status.Invalid(status.message().decode('utf-8'))
        
        return ca.Status.OK()
    
    cpdef ca.Status stop(self):
        """Stop operator."""
        if not self._initialized:
            return ca.Status.OK()
        
        cdef CppStreamingOperator* cpp_op = self._cpp_operator.get()
        cdef arrow.Status status = cpp_op.Stop()
        
        if not status.ok():
            return ca.Status.Invalid(status.message().decode('utf-8'))
        
        return ca.Status.OK()
    
    cpdef ca.RecordBatch process_batch(self, ca.RecordBatch batch):
        """Process batch."""
        if not self._initialized:
            return None
        
        cdef CppStreamingOperator* cpp_op = self._cpp_operator.get()
        cdef arrow.Result[shared_ptr[PCRecordBatch]] result = cpp_op.ProcessBatch(batch.sp_batch)
        
        if not result.ok():
            return None
        
        # Wrap C++ RecordBatch as Python RecordBatch
        cdef shared_ptr[PCRecordBatch] cpp_batch = result.ValueOrDie()
        cdef ca.RecordBatch py_batch = ca.RecordBatch.__new__(ca.RecordBatch)
        py_batch.init(cpp_batch)
        return py_batch
    
    cpdef str get_operator_id(self):
        """Get operator ID."""
        if not self._initialized:
            return ""
        
        cdef CppStreamingOperator* cpp_op = self._cpp_operator.get()
        return cpp_op.GetOperatorId().decode('utf-8')
    
    cpdef str get_operator_type(self):
        """Get operator type."""
        if not self._initialized:
            return ""
        
        cdef CppStreamingOperator* cpp_op = self._cpp_operator.get()
        return cpp_op.GetOperatorType().decode('utf-8')
    
    cpdef cbool is_running(self):
        """Check if running."""
        if not self._initialized:
            return False
        
        cdef CppStreamingOperator* cpp_op = self._cpp_operator.get()
        return cpp_op.IsRunning()


# ============================================================================
# Streaming Source Wrapper
# ============================================================================

cdef class StreamingSource:
    """
    C++ StreamingSource wrapper for high-performance streaming sources.
    """
    
    cdef:
        shared_ptr[CppStreamingSource] _cpp_source
        cbool _initialized
    
    def __cinit__(self):
        """Initialize streaming source wrapper."""
        self._initialized = False
    
    def __dealloc__(self):
        """Cleanup C++ source."""
        if self._initialized:
            self._cpp_source.reset()
    
    cpdef ca.Status start(self):
        """Start source."""
        if not self._initialized:
            return ca.Status.Invalid("StreamingSource not initialized")
        
        cdef CppStreamingSource* source = self._cpp_source.get()
        cdef arrow.Status status = source.Start()
        
        if not status.ok():
            return ca.Status.Invalid(status.message().decode('utf-8'))
        
        return ca.Status.OK()
    
    cpdef ca.Status stop(self):
        """Stop source."""
        if not self._initialized:
            return ca.Status.OK()
        
        cdef CppStreamingSource* source = self._cpp_source.get()
        cdef arrow.Status status = source.Stop()
        
        if not status.ok():
            return ca.Status.Invalid(status.message().decode('utf-8'))
        
        return ca.Status.OK()
    
    cpdef ca.RecordBatch fetch_next_batch(self):
        """Fetch next batch."""
        if not self._initialized:
            return None
        
        cdef CppStreamingSource* source = self._cpp_source.get()
        cdef arrow.Result[shared_ptr[PCRecordBatch]] result = source.FetchNextBatch()
        
        if not result.ok():
            return None
        
        # Wrap C++ RecordBatch as Python RecordBatch
        cdef shared_ptr[PCRecordBatch] cpp_batch = result.ValueOrDie()
        cdef ca.RecordBatch py_batch = ca.RecordBatch.__new__(ca.RecordBatch)
        py_batch.init(cpp_batch)
        return py_batch
    
    cpdef ca.Status commit_offset(self, int32_t partition, int64_t offset, int64_t timestamp):
        """Commit offset."""
        if not self._initialized:
            return ca.Status.Invalid("StreamingSource not initialized")
        
        cdef CppStreamingSource* source = self._cpp_source.get()
        cdef arrow.Status status = source.CommitOffset(partition, offset, timestamp)
        
        if not status.ok():
            return ca.Status.Invalid(status.message().decode('utf-8'))
        
        return ca.Status.OK()
    
    cpdef int64_t get_last_offset(self, int32_t partition):
        """Get last offset."""
        if not self._initialized:
            return -1
        
        cdef CppStreamingSource* source = self._cpp_source.get()
        cdef arrow.Result[int64_t] result = source.GetLastOffset(partition)
        
        if not result.ok():
            return -1
        
        return result.ValueOrDie()
    
    cpdef str get_source_name(self):
        """Get source name."""
        if not self._initialized:
            return ""
        
        cdef CppStreamingSource* source = self._cpp_source.get()
        return source.GetSourceName().decode('utf-8')
    
    cpdef cbool is_running(self):
        """Check if running."""
        if not self._initialized:
            return False
        
        cdef CppStreamingSource* source = self._cpp_source.get()
        return source.IsRunning()


# ============================================================================
# Dimension Table Manager Wrapper
# ============================================================================

cdef class DimensionTableManager:
    """
    C++ DimensionTableManager wrapper for dimension table management.
    """
    
    cdef:
        shared_ptr[CppDimensionTableManager] _cpp_manager
        cbool _initialized
    
    def __cinit__(self):
        """Initialize dimension table manager wrapper."""
        self._initialized = False
    
    def __dealloc__(self):
        """Cleanup C++ manager."""
        if self._initialized:
            self._cpp_manager.reset()
    
    cpdef ca.Status initialize(self):
        """Initialize manager."""
        if not self._initialized:
            return ca.Status.Invalid("DimensionTableManager not initialized")
        
        cdef CppDimensionTableManager* manager = self._cpp_manager.get()
        cdef arrow.Status status = manager.Initialize()
        
        if not status.ok():
            return ca.Status.Invalid(status.message().decode('utf-8'))
        
        return ca.Status.OK()
    
    cpdef ca.Status shutdown(self):
        """Shutdown manager."""
        if not self._initialized:
            return ca.Status.OK()
        
        cdef CppDimensionTableManager* manager = self._cpp_manager.get()
        cdef arrow.Status status = manager.Shutdown()
        
        if not status.ok():
            return ca.Status.Invalid(status.message().decode('utf-8'))
        
        return ca.Status.OK()
    
    cpdef ca.Status register_table(self, str table_name, ca.Table table, cbool is_raft_replicated=False):
        """Register table."""
        if not self._initialized:
            return ca.Status.Invalid("DimensionTableManager not initialized")
        
        cdef CppDimensionTableManager* manager = self._cpp_manager.get()
        cdef arrow.Status status = manager.RegisterTable(
            table_name.encode('utf-8'),
            table.sp_table,
            is_raft_replicated
        )
        
        if not status.ok():
            return ca.Status.Invalid(status.message().decode('utf-8'))
        
        return ca.Status.OK()
    
    cpdef ca.Table get_table(self, str table_name):
        """Get table."""
        if not self._initialized:
            return None
        
        cdef CppDimensionTableManager* manager = self._cpp_manager.get()
        cdef arrow.Result[shared_ptr[PCTable]] result = manager.GetTable(table_name.encode('utf-8'))
        
        if not result.ok():
            return None
        
        # Wrap C++ Table as Python Table
        cdef shared_ptr[PCTable] cpp_table = result.ValueOrDie()
        cdef ca.Table py_table = ca.Table.__new__(ca.Table)
        py_table.init(cpp_table)
        return py_table
    
    cpdef ca.Status update_table(self, str table_name, ca.Table table):
        """Update table."""
        if not self._initialized:
            return ca.Status.Invalid("DimensionTableManager not initialized")
        
        cdef CppDimensionTableManager* manager = self._cpp_manager.get()
        cdef arrow.Status status = manager.UpdateTable(
            table_name.encode('utf-8'),
            table.sp_table
        )
        
        if not status.ok():
            return ca.Status.Invalid(status.message().decode('utf-8'))
        
        return ca.Status.OK()
    
    cpdef ca.Status remove_table(self, str table_name):
        """Remove table."""
        if not self._initialized:
            return ca.Status.Invalid("DimensionTableManager not initialized")
        
        cdef CppDimensionTableManager* manager = self._cpp_manager.get()
        cdef arrow.Status status = manager.RemoveTable(table_name.encode('utf-8'))
        
        if not status.ok():
            return ca.Status.Invalid(status.message().decode('utf-8'))
        
        return ca.Status.OK()
    
    cpdef list list_tables(self):
        """List tables."""
        if not self._initialized:
            return []
        
        cdef CppDimensionTableManager* manager = self._cpp_manager.get()
        cdef vector[string] table_names = manager.ListTables()
        
        cdef list result = []
        for i in range(table_names.size()):
            result.append(table_names[i].decode('utf-8'))
        
        return result
    
    cpdef cbool is_raft_replicated(self, str table_name):
        """Check if table is RAFT replicated."""
        if not self._initialized:
            return False
        
        cdef CppDimensionTableManager* manager = self._cpp_manager.get()
        return manager.IsRaftReplicated(table_name.encode('utf-8'))


# ============================================================================
# Forward declarations for other wrappers
# ============================================================================

cdef class MarbleDBIntegration:
    """C++ MarbleDBIntegration wrapper (defined elsewhere)."""
    cdef shared_ptr[MarbleDBIntegration] _cpp_integration
    cdef cbool _initialized

cdef class TaskSlotManager:
    """C++ TaskSlotManager wrapper (defined elsewhere)."""
    cdef shared_ptr[TaskSlotManager] _cpp_manager
    cdef cbool _initialized

cdef class ShuffleTransport:
    """C++ ShuffleTransport wrapper (defined elsewhere)."""
    cdef shared_ptr[ShuffleTransport] _cpp_transport
    cdef cbool _initialized

cdef class CheckpointCoordinator:
    """C++ CheckpointCoordinator wrapper (defined elsewhere)."""
    cdef shared_ptr[CheckpointCoordinator] _cpp_coordinator
    cdef cbool _initialized
