#!/usr/bin/env python3
"""
C++ Agent Control Layer

Minimal Python control layer for the C++ agent core.
Only high-level control and configuration comes from Python.
"""

import asyncio
import logging
from typing import Optional, Dict, Any, List, Callable
from dataclasses import dataclass

logger = logging.getLogger(__name__)


@dataclass
class AgentConfig:
    """Agent configuration for C++ agent core."""
    agent_id: str
    host: str = "localhost"
    port: int = 8816
    memory_mb: int = 1024
    num_slots: int = 4
    workers_per_slot: int = 2
    is_local_mode: bool = True
    checkpoint_interval_ms: int = 60000
    watermark_idle_timeout_ms: int = 30000


class Agent:
    """
    C++ Agent Control Layer
    
    Minimal Python control layer for the high-performance C++ agent core.
    Only high-level control and configuration comes from Python.
    """
    
    def __init__(self, config: AgentConfig):
        self.config = config
        self.agent_core = None
        self.running = False
        
        # Initialize C++ agent core
        self._initialize_agent_core()
        
        logger.info(f"C++ Agent initialized: {self.config.agent_id} (local_mode={self.config.is_local_mode})")
    
    def _initialize_agent_core(self):
        """Initialize C++ agent core."""
        try:
            from ._cython.agent_core import AgentCore
            
            self.agent_core = AgentCore(
                agent_id=self.config.agent_id,
                host=self.config.host,
                port=self.config.port,
                memory_mb=self.config.memory_mb,
                num_slots=self.config.num_slots,
                workers_per_slot=self.config.workers_per_slot,
                is_local_mode=self.config.is_local_mode,
                checkpoint_interval_ms=self.config.checkpoint_interval_ms,
                watermark_idle_timeout_ms=self.config.watermark_idle_timeout_ms
            )
            
            logger.info("C++ agent core initialized successfully")
            
        except ImportError as e:
            logger.error(f"Failed to import C++ agent core: {e}")
            logger.error("Falling back to Python implementation")
            self.agent_core = None
    
    async def start(self):
        """Start agent services."""
        if self.running:
            return
        
        if not self.agent_core:
            logger.error("C++ agent core not available")
            return
        
        # Initialize C++ agent core
        status = self.agent_core.initialize()
        if not status.ok():
            logger.error(f"Failed to initialize C++ agent core: {status.message()}")
            return
        
        # Start C++ agent core
        status = self.agent_core.start()
        if not status.ok():
            logger.error(f"Failed to start C++ agent core: {status.message()}")
            return
        
        self.running = True
        logger.info(f"C++ Agent started: {self.config.agent_id}")
    
    async def stop(self):
        """Stop agent services."""
        if not self.running:
            return
        
        if not self.agent_core:
            return
        
        # Stop C++ agent core
        status = self.agent_core.stop()
        if not status.ok():
            logger.error(f"Failed to stop C++ agent core: {status.message()}")
        
        # Shutdown C++ agent core
        status = self.agent_core.shutdown()
        if not status.ok():
            logger.error(f"Failed to shutdown C++ agent core: {status.message()}")
        
        self.running = False
        logger.info(f"C++ Agent stopped: {self.config.agent_id}")
    
    def deploy_streaming_operator(self, operator_id: str, operator_type: str, parameters: Dict[str, Any] = None):
        """Deploy streaming operator."""
        if not self.agent_core:
            raise RuntimeError("C++ agent core not available")
        
        if parameters is None:
            parameters = {}
        
        status = self.agent_core.deploy_streaming_operator(operator_id, operator_type, parameters)
        if not status.ok():
            raise RuntimeError(f"Failed to deploy streaming operator: {status.message()}")
        
        logger.info(f"Streaming operator deployed: {operator_id} (type={operator_type})")
    
    def stop_streaming_operator(self, operator_id: str):
        """Stop streaming operator."""
        if not self.agent_core:
            raise RuntimeError("C++ agent core not available")
        
        status = self.agent_core.stop_streaming_operator(operator_id)
        if not status.ok():
            raise RuntimeError(f"Failed to stop streaming operator: {status.message()}")
        
        logger.info(f"Streaming operator stopped: {operator_id}")
    
    def register_dimension_table(self, table_name: str, table, is_raft_replicated: bool = False):
        """Register dimension table."""
        if not self.agent_core:
            raise RuntimeError("C++ agent core not available")
        
        status = self.agent_core.register_dimension_table(table_name, table, is_raft_replicated)
        if not status.ok():
            raise RuntimeError(f"Failed to register dimension table: {status.message()}")
        
        logger.info(f"Dimension table registered: {table_name} (RAFT={is_raft_replicated})")
    
    def register_streaming_source(self, source_name: str, connector_type: str, config: Dict[str, str]):
        """Register streaming source."""
        if not self.agent_core:
            raise RuntimeError("C++ agent core not available")
        
        status = self.agent_core.register_streaming_source(source_name, connector_type, config)
        if not status.ok():
            raise RuntimeError(f"Failed to register streaming source: {status.message()}")
        
        logger.info(f"Streaming source registered: {source_name} (type={connector_type})")
    
    def execute_batch_sql(self, query: str, input_tables: Dict[str, Any] = None):
        """Execute batch SQL query."""
        if not self.agent_core:
            raise RuntimeError("C++ agent core not available")
        
        if input_tables is None:
            input_tables = {}
        
        result = self.agent_core.execute_batch_sql(query, input_tables)
        if result is None:
            raise RuntimeError("Batch SQL execution failed")
        
        return result
    
    def execute_streaming_sql(self, query: str, input_tables: Dict[str, Any] = None, 
                            output_callback: Callable = None):
        """Execute streaming SQL query."""
        if not self.agent_core:
            raise RuntimeError("C++ agent core not available")
        
        if input_tables is None:
            input_tables = {}
        
        if output_callback is None:
            output_callback = lambda batch: None
        
        status = self.agent_core.execute_streaming_sql(query, input_tables, output_callback)
        if not status.ok():
            raise RuntimeError(f"Streaming SQL execution failed: {status.message()}")
    
    def get_status(self):
        """Get agent status."""
        if not self.agent_core:
            return {"error": "C++ agent core not available"}
        
        return self.agent_core.get_status()
    
    def get_marbledb(self):
        """Get embedded MarbleDB instance."""
        if not self.agent_core:
            return None
        
        return self.agent_core.get_marbledb()
    
    def get_task_slot_manager(self):
        """Get task slot manager."""
        if not self.agent_core:
            return None
        
        return self.agent_core.get_task_slot_manager()
    
    def get_shuffle_transport(self):
        """Get shuffle transport."""
        if not self.agent_core:
            return None
        
        return self.agent_core.get_shuffle_transport()
    
    def get_dimension_table_manager(self):
        """Get dimension table manager."""
        if not self.agent_core:
            return None
        
        return self.agent_core.get_dimension_table_manager()
    
    def get_checkpoint_coordinator(self):
        """Get checkpoint coordinator."""
        if not self.agent_core:
            return None
        
        return self.agent_core.get_checkpoint_coordinator()
    
    def is_running(self):
        """Check if agent is running."""
        if not self.agent_core:
            return False
        
        return self.agent_core.is_running()
    
    def get_agent_id(self):
        """Get agent ID."""
        return self.config.agent_id


class StreamingSQLExecutor:
    """
    High-level streaming SQL executor using C++ agent core.
    
    Provides a simple interface for streaming SQL execution with automatic
    C++ agent management behind the scenes.
    """
    
    def __init__(self, agent_id: str = "streaming_sql_executor", **kwargs):
        self.agent_id = agent_id
        self.agent = None
        self.initialized = False
        
        # Create agent configuration
        config = AgentConfig(
            agent_id=agent_id,
            **kwargs
        )
        
        # Create C++ agent
        self.agent = Agent(config)
        
        logger.info(f"StreamingSQLExecutor initialized: {agent_id}")
    
    async def initialize(self):
        """Initialize streaming SQL executor."""
        if self.initialized:
            return
        
        await self.agent.start()
        self.initialized = True
        
        logger.info("StreamingSQLExecutor initialized successfully")
    
    async def shutdown(self):
        """Shutdown streaming SQL executor."""
        if not self.initialized:
            return
        
        await self.agent.stop()
        self.initialized = False
        
        logger.info("StreamingSQLExecutor shutdown complete")
    
    def register_dimension_table(self, table_name: str, table, is_raft_replicated: bool = False):
        """Register dimension table."""
        if not self.initialized:
            raise RuntimeError("StreamingSQLExecutor not initialized")
        
        self.agent.register_dimension_table(table_name, table, is_raft_replicated)
    
    def register_streaming_source(self, source_name: str, connector_type: str, config: Dict[str, str]):
        """Register streaming source."""
        if not self.initialized:
            raise RuntimeError("StreamingSQLExecutor not initialized")
        
        self.agent.register_streaming_source(source_name, connector_type, config)
    
    def execute_batch_sql(self, query: str, input_tables: Dict[str, Any] = None):
        """Execute batch SQL query."""
        if not self.initialized:
            raise RuntimeError("StreamingSQLExecutor not initialized")
        
        return self.agent.execute_batch_sql(query, input_tables)
    
    def execute_streaming_sql(self, query: str, input_tables: Dict[str, Any] = None, 
                             output_callback: Callable = None):
        """Execute streaming SQL query."""
        if not self.initialized:
            raise RuntimeError("StreamingSQLExecutor not initialized")
        
        return self.agent.execute_streaming_sql(query, input_tables, output_callback)
    
    def get_status(self):
        """Get executor status."""
        if not self.agent:
            return {"error": "Agent not available"}
        
        return self.agent.get_status()
    
    def get_marbledb(self):
        """Get embedded MarbleDB instance."""
        if not self.agent:
            return None
        
        return self.agent.get_marbledb()
    
    def get_task_slot_manager(self):
        """Get task slot manager."""
        if not self.agent:
            return None
        
        return self.agent.get_task_slot_manager()
    
    def get_dimension_table_manager(self):
        """Get dimension table manager."""
        if not self.agent:
            return None
        
        return self.agent.get_dimension_table_manager()
    
    def get_checkpoint_coordinator(self):
        """Get checkpoint coordinator."""
        if not self.agent:
            return None
        
        return self.agent.get_checkpoint_coordinator()
    
    def is_running(self):
        """Check if executor is running."""
        if not self.agent:
            return False
        
        return self.agent.is_running()


# Convenience function for creating streaming SQL executor
def create_streaming_sql_executor(agent_id: str = "streaming_sql_executor", **kwargs):
    """Create a streaming SQL executor with C++ agent core."""
    return StreamingSQLExecutor(agent_id, **kwargs)
