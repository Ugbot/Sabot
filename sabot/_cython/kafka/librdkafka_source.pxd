# distutils: language = c++
# cython: language_level=3

from libcpp.string cimport string
from libcpp.memory cimport shared_ptr, unique_ptr
from libcpp.unordered_map cimport unordered_map
from libcpp cimport bool as cbool
from libc.stdint cimport int32_t, int64_t

cdef extern from "arrow/api.h" namespace "arrow" nogil:
    cdef cppclass CStatus "arrow::Status":
        cbool ok()
        string message()
    
    cdef cppclass CResult "arrow::Result"[T]:
        cbool ok()
        T ValueOrDie()

cdef extern from "arrow/c/bridge.h":
    pass

cdef extern from "sabot_sql/streaming/source_connector.h" namespace "sabot_sql::streaming":
    cdef struct Offset:
        int32_t partition
        int64_t offset
        int64_t timestamp
    
    cdef struct ConnectorConfig:
        string connector_id
        int partition_id
        unordered_map[string, string] properties

cdef extern from "sabot_sql/streaming/kafka_connector.h" namespace "sabot_sql::streaming":
    cdef cppclass KafkaConnector:
        KafkaConnector() except +
        CStatus Initialize(const ConnectorConfig& config) except +
        CStatus Shutdown() except +
        cbool HasMore() const
        size_t GetPartitionCount() const

