# cython: language_level=3, boundscheck=False, wraparound=False, cdivision=True, nonecheck=False
"""
C++ Agent Cython Wrapper

High-performance C++ agent for streaming SQL execution with embedded MarbleDB.
No Python dependencies - pure C++/Cython for maximum performance.
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
    CSchema as PCSchema,
)

# Import C++ agent classes
from sabot._c.agent cimport Agent as CppAgent, AgentConfig as CppAgentConfig, TaskSpec as CppTaskSpec, AgentStatus as CppAgentStatus

# Import other C++ components
from sabot._c.task_slot_manager cimport TaskSlotManager
from sabot._cython.shuffle.shuffle_transport cimport ShuffleTransport

# Forward declarations
cdef class MarbleDBIntegration


# ============================================================================
# C++ Agent Wrapper
# ============================================================================

cdef class Agent:
    """
    C++ Agent wrapper for high-performance streaming SQL execution.
    
    Features:
    - Embedded MarbleDB for state management
    - TaskSlotManager for morsel parallelism
    - ShuffleTransport for network communication
    - Zero Python overhead in hot paths
    """
    
    cdef:
        unique_ptr[CppAgent] _cpp_agent
        CppAgentConfig _config
        cbool _initialized
    
    def __cinit__(self, str agent_id, str host="localhost", int port=8816,
                  int memory_mb=1024, int num_slots=4, int workers_per_slot=2,
                  cbool is_local_mode=True):
        """Initialize C++ agent."""
        self._config = CppAgentConfig(
            agent_id.encode('utf-8'),
            host.encode('utf-8'),
            port,
            memory_mb,
            num_slots,
            workers_per_slot,
            is_local_mode
        )
        
        self._cpp_agent = unique_ptr[CppAgent](new CppAgent(self._config))
        self._initialized = True
    
    def __dealloc__(self):
        """Cleanup C++ agent."""
        if self._initialized:
            self._cpp_agent.reset()
    
    cpdef ca.Status start(self):
        """Start agent services."""
        if not self._initialized:
            return ca.Status.Invalid("Agent not initialized")
        
        cdef CppAgent* agent = self._cpp_agent.get()
        cdef arrow.Status status = agent.Start()
        
        if not status.ok():
            return ca.Status.Invalid(status.message().decode('utf-8'))
        
        return ca.Status.OK()
    
    cpdef ca.Status stop(self):
        """Stop agent services."""
        if not self._initialized:
            return ca.Status.OK()
        
        cdef CppAgent* agent = self._cpp_agent.get()
        cdef arrow.Status status = agent.Stop()
        
        if not status.ok():
            return ca.Status.Invalid(status.message().decode('utf-8'))
        
        return ca.Status.OK()
    
    cpdef ca.Status deploy_task(self, str task_id, str operator_type, dict parameters=None):
        """Deploy and start a task."""
        if not self._initialized:
            return ca.Status.Invalid("Agent not initialized")
        
        if parameters is None:
            parameters = {}
        
        # Create task specification
        cdef CppTaskSpec task_spec
        task_spec.task_id = task_id.encode('utf-8')
        task_spec.operator_type = operator_type.encode('utf-8')
        
        # Convert parameters to C++ map
        for key, value in parameters.items():
            task_spec.parameters[key.encode('utf-8')] = str(value).encode('utf-8')
        
        cdef CppAgent* agent = self._cpp_agent.get()
        cdef arrow.Status status = agent.DeployTask(task_spec)
        
        if not status.ok():
            return ca.Status.Invalid(status.message().decode('utf-8'))
        
        return ca.Status.OK()
    
    cpdef ca.Status stop_task(self, str task_id):
        """Stop a task."""
        if not self._initialized:
            return ca.Status.Invalid("Agent not initialized")
        
        cdef CppAgent* agent = self._cpp_agent.get()
        cdef arrow.Status status = agent.StopTask(task_id.encode('utf-8'))
        
        if not status.ok():
            return ca.Status.Invalid(status.message().decode('utf-8'))
        
        return ca.Status.OK()
    
    cpdef dict get_status(self):
        """Get agent status."""
        if not self._initialized:
            return {"error": "Agent not initialized"}
        
        cdef CppAgent* agent = self._cpp_agent.get()
        cdef CppAgentStatus status = agent.GetStatus()
        
        return {
            "agent_id": status.agent_id.decode('utf-8'),
            "running": status.running,
            "active_tasks": status.active_tasks,
            "available_slots": status.available_slots,
            "total_morsels_processed": status.total_morsels_processed,
            "total_bytes_shuffled": status.total_bytes_shuffled,
            "marbledb_path": status.marbledb_path.decode('utf-8'),
            "marbledb_initialized": status.marbledb_initialized
        }
    
    cpdef MarbleDBIntegration get_marbledb(self):
        """Get embedded MarbleDB instance."""
        if not self._initialized:
            return None
        
        cdef CppAgent* agent = self._cpp_agent.get()
        cdef shared_ptr[MarbleDBIntegration] marbledb = agent.GetMarbleDB()
        
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
        
        cdef CppAgent* agent = self._cpp_agent.get()
        cdef shared_ptr[TaskSlotManager] slot_manager = agent.GetTaskSlotManager()
        
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
        
        cdef CppAgent* agent = self._cpp_agent.get()
        cdef shared_ptr[ShuffleTransport] transport = agent.GetShuffleTransport()
        
        if transport.get() == nullptr:
            return None
        
        # Wrap C++ ShuffleTransport in Python wrapper
        cdef ShuffleTransport wrapper = ShuffleTransport.__new__(ShuffleTransport)
        wrapper._cpp_transport = transport
        return wrapper
    
    cpdef cbool is_running(self):
        """Check if agent is running."""
        if not self._initialized:
            return False
        
        cdef CppAgent* agent = self._cpp_agent.get()
        return agent.IsRunning()
    
    cpdef str get_agent_id(self):
        """Get agent ID."""
        if not self._initialized:
            return ""
        
        cdef CppAgent* agent = self._cpp_agent.get()
        return agent.GetAgentId().decode('utf-8')


# ============================================================================
# MarbleDB Integration Wrapper
# ============================================================================

cdef class MarbleDBIntegration:
    """
    C++ MarbleDBIntegration wrapper for embedded MarbleDB access.
    """
    
    cdef:
        shared_ptr[MarbleDBIntegration] _cpp_integration
        cbool _initialized
    
    def __cinit__(self):
        """Initialize MarbleDB integration wrapper."""
        self._initialized = False
    
    def __dealloc__(self):
        """Cleanup C++ integration."""
        if self._initialized:
            self._cpp_integration.reset()
    
    cpdef ca.Status initialize(self, str integration_id, str db_path, cbool enable_raft=False):
        """Initialize embedded MarbleDB integration."""
        if not self._initialized:
            return ca.Status.Invalid("MarbleDB integration not initialized")
        
        # In real implementation, this would call the C++ Initialize method
        # For now, just return success
        return ca.Status.OK()
    
    cpdef ca.Status shutdown(self):
        """Shutdown MarbleDB integration."""
        if not self._initialized:
            return ca.Status.OK()
        
        # In real implementation, this would call the C++ Shutdown method
        # For now, just return success
        return ca.Status.OK()
    
    cpdef ca.Status create_table(self, str table_name, ca.Schema schema, cbool is_raft_replicated=False):
        """Create a table in MarbleDB."""
        if not self._initialized:
            return ca.Status.Invalid("MarbleDB integration not initialized")
        
        # In real implementation, this would call the C++ CreateTable method
        return ca.Status.OK()
    
    cpdef ca.RecordBatch read_table(self, str table_name):
        """Read a table from MarbleDB."""
        if not self._initialized:
            return ca.Status.Invalid("MarbleDB integration not initialized")
        
        # In real implementation, this would call the C++ ReadTable method
        return None
    
    cpdef ca.Status write_state(self, str key, str value):
        """Write state to MarbleDB."""
        if not self._initialized:
            return ca.Status.Invalid("MarbleDB integration not initialized")
        
        # In real implementation, this would call the C++ WriteState method
        return ca.Status.OK()
    
    cpdef str read_state(self, str key):
        """Read state from MarbleDB."""
        if not self._initialized:
            return None
        
        # In real implementation, this would call the C++ ReadState method
        return None
    
    cpdef ca.Status register_timer(self, str timer_name, int64_t trigger_time_ms, str callback_data):
        """Register a timer in MarbleDB."""
        if not self._initialized:
            return ca.Status.Invalid("MarbleDB integration not initialized")
        
        # In real implementation, this would call the C++ RegisterTimer method
        return ca.Status.OK()
    
    cpdef ca.Status set_watermark(self, int32_t partition_id, int64_t timestamp):
        """Set watermark for a partition."""
        if not self._initialized:
            return ca.Status.Invalid("MarbleDB integration not initialized")
        
        # In real implementation, this would call the C++ SetWatermark method
        return ca.Status.OK()
    
    cpdef int64_t get_watermark(self, int32_t partition_id):
        """Get watermark for a partition."""
        if not self._initialized:
            return -1
        
        # In real implementation, this would call the C++ GetWatermark method
        return -1


# ============================================================================
# TaskSlotManager Wrapper
# ============================================================================

cdef class TaskSlotManager:
    """
    C++ TaskSlotManager wrapper for morsel parallelism.
    """
    
    cdef:
        shared_ptr[TaskSlotManager] _cpp_manager
        cbool _initialized
    
    def __cinit__(self):
        """Initialize task slot manager wrapper."""
        self._initialized = False
    
    def __dealloc__(self):
        """Cleanup C++ manager."""
        if self._initialized:
            self._cpp_manager.reset()
    
    cpdef int get_num_slots(self):
        """Get number of slots."""
        if not self._initialized:
            return 0
        
        cdef TaskSlotManager* manager = self._cpp_manager.get()
        return manager.GetNumSlots()
    
    cpdef int get_available_slots(self):
        """Get available slots."""
        if not self._initialized:
            return 0
        
        cdef TaskSlotManager* manager = self._cpp_manager.get()
        return manager.GetAvailableSlots()
    
    cpdef int64_t get_queue_depth(self):
        """Get queue depth."""
        if not self._initialized:
            return 0
        
        cdef TaskSlotManager* manager = self._cpp_manager.get()
        return manager.GetQueueDepth()
    
    cpdef void shutdown(self):
        """Shutdown task slot manager."""
        if self._initialized:
            cdef TaskSlotManager* manager = self._cpp_manager.get()
            manager.Shutdown()
