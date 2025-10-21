# distutils: language = c++
# cython: language_level = 3

"""
Cython wrapper for MarbleDB Integration

Exposes C++ MarbleDBIntegration to Python for direct morsel access.
"""

from libcpp.string cimport string
from libcpp.vector cimport vector
from libcpp.unordered_map cimport unordered_map
from libcpp.memory cimport shared_ptr, unique_ptr
from libcpp cimport bool

import pyarrow as pa
from pyarrow.lib cimport CTable, CSchema, CRecordBatch
from pyarrow.lib import Table, Schema, RecordBatch

cdef extern from "sabot_sql/streaming/marbledb_integration.h" namespace "sabot_sql::streaming":
    cdef cppclass MarbleDBIntegration:
        MarbleDBIntegration() except +
        ~MarbleDBIntegration()
        
        # Lifecycle
        cdef object Initialize(string integration_id, string db_path, bool enable_raft)
        cdef object Shutdown()
        
        # Table Management
        cdef object CreateTable(string table_name, shared_ptr[CSchema] schema, bool is_raft_replicated)
        cdef object ReadTable(string table_name)
        cdef object DeleteTable(string table_name)
        cdef object TableExists(string table_name)
        cdef object GetTableSchema(string table_name)
        
        # State Management
        cdef object WriteState(string key, string value)
        cdef object ReadState(string key)
        cdef object DeleteState(string key)
        cdef object StateExists(string key)
        
        # Timer Management
        cdef object RegisterTimer(string timer_name, int64_t trigger_time_ms, string callback_data)
        cdef object CancelTimer(string timer_name)
        cdef object GetPendingTimers()
        
        # Watermark Management
        cdef object SetWatermark(int32_t partition_id, int64_t timestamp)
        cdef object GetWatermark(int32_t partition_id)
        cdef object GetGlobalWatermark()
        
        # Checkpoint Management
        cdef object CreateCheckpoint(int64_t checkpoint_id, int64_t timestamp, string state_data)
        cdef object CommitCheckpoint(int64_t checkpoint_id)
        cdef object GetLastCommittedCheckpoint()
        
        # Statistics
        cdef object GetStats()


cdef class MarbleDBIntegrationWrapper:
    """Python wrapper for C++ MarbleDBIntegration."""
    
    cdef MarbleDBIntegration* _cpp_integration
    cdef bool _initialized
    
    def __init__(self):
        """Initialize MarbleDB integration wrapper."""
        self._cpp_integration = new MarbleDBIntegration()
        self._initialized = False
    
    def __dealloc__(self):
        """Cleanup C++ integration."""
        if self._cpp_integration:
            del self._cpp_integration
    
    def Initialize(self, str integration_id, str db_path, bool enable_raft = False):
        """Initialize embedded MarbleDB integration."""
        cdef string cpp_integration_id = integration_id.encode('utf-8')
        cdef string cpp_db_path = db_path.encode('utf-8')
        
        try:
            result = self._cpp_integration.Initialize(cpp_integration_id, cpp_db_path, enable_raft)
            if result.ok():
                self._initialized = True
            return result
        except Exception as e:
            return Status(False, str(e))
    
    def Shutdown(self):
        """Shutdown MarbleDB integration."""
        if self._cpp_integration:
            try:
                result = self._cpp_integration.Shutdown()
                self._initialized = False
                return result
            except Exception as e:
                return Status(False, str(e))
        return Status(True, "Already shutdown")
    
    def CreateTable(self, str table_name, Schema schema, bool is_raft_replicated = False):
        """Create a table in MarbleDB."""
        if not self._initialized:
            return Status(False, "MarbleDB not initialized")
        
        cdef string cpp_table_name = table_name.encode('utf-8')
        cdef shared_ptr[CSchema] cpp_schema = schema.sp_schema
        
        try:
            return self._cpp_integration.CreateTable(cpp_table_name, cpp_schema, is_raft_replicated)
        except Exception as e:
            return Status(False, str(e))
    
    def ReadTable(self, str table_name):
        """Read a table from MarbleDB."""
        if not self._initialized:
            return Result(False, None, "MarbleDB not initialized")
        
        cdef string cpp_table_name = table_name.encode('utf-8')
        
        try:
            return self._cpp_integration.ReadTable(cpp_table_name)
        except Exception as e:
            return Result(False, None, str(e))
    
    def WriteState(self, str key, str value):
        """Write state to MarbleDB."""
        if not self._initialized:
            return Status(False, "MarbleDB not initialized")
        
        cdef string cpp_key = key.encode('utf-8')
        cdef string cpp_value = value.encode('utf-8')
        
        try:
            return self._cpp_integration.WriteState(cpp_key, cpp_value)
        except Exception as e:
            return Status(False, str(e))
    
    def ReadState(self, str key):
        """Read state from MarbleDB."""
        if not self._initialized:
            return Result(False, None, "MarbleDB not initialized")
        
        cdef string cpp_key = key.encode('utf-8')
        
        try:
            return self._cpp_integration.ReadState(cpp_key)
        except Exception as e:
            return Result(False, None, str(e))
    
    def RegisterTimer(self, str timer_name, int64_t trigger_time_ms, str callback_data):
        """Register a timer in MarbleDB."""
        if not self._initialized:
            return Status(False, "MarbleDB not initialized")
        
        cdef string cpp_timer_name = timer_name.encode('utf-8')
        cdef string cpp_callback_data = callback_data.encode('utf-8')
        
        try:
            return self._cpp_integration.RegisterTimer(cpp_timer_name, trigger_time_ms, cpp_callback_data)
        except Exception as e:
            return Status(False, str(e))
    
    def SetWatermark(self, int32_t partition_id, int64_t timestamp):
        """Set watermark for a partition."""
        if not self._initialized:
            return Status(False, "MarbleDB not initialized")
        
        try:
            return self._cpp_integration.SetWatermark(partition_id, timestamp)
        except Exception as e:
            return Status(False, str(e))
    
    def GetWatermark(self, int32_t partition_id):
        """Get watermark for a partition."""
        if not self._initialized:
            return Result(False, None, "MarbleDB not initialized")
        
        try:
            return self._cpp_integration.GetWatermark(partition_id)
        except Exception as e:
            return Result(False, None, str(e))


cdef class Status:
    """Status result for MarbleDB operations."""
    
    cdef bool _ok
    cdef string _message
    
    def __init__(self, bool ok, str message = ""):
        self._ok = ok
        self._message = message.encode('utf-8')
    
    def ok(self):
        """Check if operation was successful."""
        return self._ok
    
    def message(self):
        """Get status message."""
        return self._message.decode('utf-8')


cdef class Result:
    """Result container for MarbleDB operations."""
    
    cdef bool _ok
    cdef object _value
    cdef string _message
    
    def __init__(self, bool ok, object value, str message = ""):
        self._ok = ok
        self._value = value
        self._message = message.encode('utf-8')
    
    def ok(self):
        """Check if operation was successful."""
        return self._ok
    
    def ValueOrDie(self):
        """Get the result value."""
        if not self._ok:
            raise RuntimeError(f"Operation failed: {self._message.decode('utf-8')}")
        return self._value
    
    def message(self):
        """Get result message."""
        return self._message.decode('utf-8')
