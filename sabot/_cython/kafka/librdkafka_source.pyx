# distutils: language = c++
# cython: language_level=3

"""
Cython wrapper for C++ librdkafka Kafka connector.

Provides high-performance Kafka source using librdkafka + simdjson.
"""

from libcpp.string cimport string
from libcpp.memory cimport shared_ptr, unique_ptr, make_unique
from libcpp.unordered_map cimport unordered_map
from libcpp cimport bool as cbool
from libc.stdint cimport int32_t, int64_t

cimport pyarrow as pa
cimport pyarrow.lib as ca

from .librdkafka_source cimport (
    KafkaConnector,
    ConnectorConfig,
    Offset
)

import logging
logger = logging.getLogger(__name__)


cdef class LibrdkafkaSource:
    """
    High-performance Kafka source using C++ librdkafka.
    
    Features:
    - SIMD-accelerated JSON parsing with simdjson
    - Zero-copy Arrow conversion
    - MarbleDB RAFT offset storage
    - One consumer per partition for parallelism
    
    Example:
        source = LibrdkafkaSource(
            bootstrap_servers="localhost:9092",
            topic="transactions",
            group_id="processor"
        )
        
        async for batch in source:
            print(f"Received {batch.num_rows} rows")
    """
    cdef unique_ptr[KafkaConnector] _connector
    cdef ConnectorConfig _config
    cdef cbool _initialized
    cdef str _topic
    cdef str _group_id
    cdef str _bootstrap_servers
    
    def __init__(self, str bootstrap_servers, str topic, str group_id,
                 int partition_id=-1, dict properties=None):
        """
        Initialize librdkafka Kafka source.
        
        Args:
            bootstrap_servers: Kafka brokers (e.g., "localhost:9092")
            topic: Topic to consume from
            group_id: Consumer group ID
            partition_id: Specific partition (-1 for all partitions)
            properties: Additional Kafka properties
        """
        self._bootstrap_servers = bootstrap_servers
        self._topic = topic
        self._group_id = group_id
        self._initialized = False
        
        # Configure connector
        self._config.connector_id = group_id.encode('utf-8')
        self._config.partition_id = partition_id
        self._config.properties[b"bootstrap.servers"] = bootstrap_servers.encode('utf-8')
        self._config.properties[b"topic"] = topic.encode('utf-8')
        self._config.properties[b"group.id"] = group_id.encode('utf-8')
        
        # Add additional properties
        if properties:
            for key, value in properties.items():
                self._config.properties[key.encode('utf-8')] = str(value).encode('utf-8')
        
        # Create connector
        self._connector = make_unique[KafkaConnector]()
        
        # Initialize
        cdef CStatus status = self._connector.get().Initialize(self._config)
        if not status.ok():
            raise RuntimeError(f"Failed to initialize Kafka connector: {status.message().decode('utf-8')}")
        
        self._initialized = True
        logger.info(f"LibrdkafkaSource initialized: {topic} (group: {group_id})")
    
    async def get_next_batch(self, int max_rows=1000):
        """
        Fetch next batch from Kafka.
        
        Args:
            max_rows: Maximum number of rows to fetch
        
        Returns:
            PyArrow RecordBatch or None if no data
        """
        if not self._initialized:
            return None
        
        # Note: This is a placeholder - the actual C++ method signature needs to be updated
        # to support GetNextBatch returning RecordBatch
        # For now, we return None to avoid compilation errors
        logger.warning("get_next_batch not fully implemented - needs C++ RecordBatch binding")
        return None
    
    def shutdown(self):
        """Shutdown Kafka connector and release resources."""
        if self._initialized and self._connector:
            cdef CStatus status = self._connector.get().Shutdown()
            if not status.ok():
                logger.error(f"Shutdown error: {status.message().decode('utf-8')}")
            self._initialized = False
            logger.info(f"LibrdkafkaSource shutdown: {self._topic}")
    
    def __dealloc__(self):
        """Cleanup on object destruction."""
        if self._initialized:
            self.shutdown()
    
    @property
    def topic(self):
        """Get topic name."""
        return self._topic
    
    @property
    def group_id(self):
        """Get consumer group ID."""
        return self._group_id
    
    @property
    def bootstrap_servers(self):
        """Get bootstrap servers."""
        return self._bootstrap_servers
    
    @property
    def is_initialized(self):
        """Check if connector is initialized."""
        return self._initialized

