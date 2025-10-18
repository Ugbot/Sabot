#!/usr/bin/env python3
"""
State Manager

Manages state backend selection and configuration.
Provides automatic backend selection based on configuration.
"""

import logging
from typing import Optional, Dict, Any
from pathlib import Path

from .interface import StateBackend, BackendType

logger = logging.getLogger(__name__)


class StateManager:
    """
    Manages state backend selection and lifecycle.
    
    Automatically selects and configures appropriate backend based on:
    - Configuration (explicit backend choice)
    - Mode (local vs distributed)
    - Available dependencies
    
    Example:
        # Auto-select backend
        manager = StateManager({'backend': 'marbledb', 'state_path': './data'})
        
        # Use backend
        await manager.backend.put('key', b'value')
        value = await manager.backend.get('key')
    """
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        """
        Initialize state manager.
        
        Args:
            config: Configuration dict with keys:
                - backend: 'marbledb', 'rocksdb', 'redis', 'memory', 'auto'
                - state_path: Path to state storage
                - redis_url: Redis connection URL (if backend='redis')
                - Other backend-specific options
        """
        self.config = config or {}
        self.backend = self._select_and_initialize_backend()
        
        logger.info(f"StateManager initialized with backend: {self.backend.get_backend_type().value}")
    
    def _select_and_initialize_backend(self) -> StateBackend:
        """
        Select and initialize appropriate backend.
        
        Returns:
            StateBackend: Initialized backend
        """
        backend_type = self.config.get('backend', 'auto')
        
        if backend_type == 'auto':
            backend_type = self._auto_select_backend()
        
        return self._create_backend(backend_type)
    
    def _auto_select_backend(self) -> str:
        """
        Automatically select best available backend.
        
        Priority:
        1. MarbleDB (if available) - best performance
        2. RocksDB (if available) - good fallback
        3. Memory - always available
        
        Returns:
            Backend type string
        """
        # Try MarbleDB first (primary backend)
        try:
            from .marble import MarbleDBBackend
            logger.info("Auto-selected MarbleDB backend (optimal)")
            return 'marbledb'
        except ImportError:
            logger.debug("MarbleDB not available")
        
        # Try RocksDB second
        try:
            from .rocksdb import RocksDBBackend
            logger.info("Auto-selected RocksDB backend (good)")
            return 'rocksdb'
        except ImportError:
            logger.debug("RocksDB not available")
        
        # Fallback to memory
        logger.warning("Using Memory backend (testing only, not persistent!)")
        return 'memory'
    
    def _create_backend(self, backend_type: str) -> StateBackend:
        """
        Create backend instance.
        
        Args:
            backend_type: Backend type string
            
        Returns:
            StateBackend: Initialized backend instance
            
        Raises:
            ValueError: If backend type unknown or unavailable
        """
        if backend_type == 'marbledb':
            return self._create_marbledb()
        elif backend_type == 'rocksdb':
            return self._create_rocksdb()
        elif backend_type == 'redis':
            return self._create_redis()
        elif backend_type == 'memory':
            return self._create_memory()
        elif backend_type == 'kafka':
            return self._create_kafka()
        else:
            raise ValueError(
                f"Unknown backend type: {backend_type}. "
                f"Valid options: marbledb, rocksdb, redis, memory, kafka, auto"
            )
    
    def _create_marbledb(self) -> StateBackend:
        """Create MarbleDB backend."""
        try:
            from .marble import MarbleDBBackend
            
            state_path = self.config.get('state_path', './sabot_state/marbledb')
            config = self.config.get('marbledb_config', {})
            
            backend = MarbleDBBackend(state_path, config)
            logger.info(f"Created MarbleDB backend at {state_path}")
            return backend
            
        except ImportError as e:
            raise ImportError(
                f"MarbleDB backend not available: {e}. "
                "Ensure MarbleDB is built and installed."
            ) from e
    
    def _create_rocksdb(self) -> StateBackend:
        """Create RocksDB backend."""
        try:
            from sabot.stores.rocksdb import RocksDBBackend
            
            state_path = self.config.get('state_path', './sabot_state/rocksdb')
            config = self.config.get('rocksdb_config', {})
            
            backend = RocksDBBackend(state_path, config)
            logger.info(f"Created RocksDB backend at {state_path}")
            return backend
            
        except ImportError as e:
            raise ImportError(
                f"RocksDB backend not available: {e}. "
                "Install python-rocksdb or use different backend."
            ) from e
    
    def _create_redis(self) -> StateBackend:
        """Create Redis backend."""
        try:
            from sabot.stores.redis import RedisBackend
            
            redis_url = self.config.get('redis_url', 'redis://localhost:6379')
            config = self.config.get('redis_config', {})
            
            backend = RedisBackend(redis_url, config)
            logger.info(f"Created Redis backend at {redis_url}")
            return backend
            
        except ImportError as e:
            raise ImportError(
                f"Redis backend not available: {e}. "
                "Install redis-py or use different backend."
            ) from e
    
    def _create_memory(self) -> StateBackend:
        """Create in-memory backend."""
        from sabot.stores.memory import MemoryBackend
        
        logger.info("Created Memory backend (not persistent)")
        return MemoryBackend()
    
    def _create_kafka(self) -> StateBackend:
        """Create Kafka backend."""
        try:
            from sabot.stores.kafka import KafkaBackend
            
            bootstrap_servers = self.config.get('kafka_bootstrap_servers', 'localhost:9092')
            topic = self.config.get('kafka_topic', 'sabot-state')
            config = self.config.get('kafka_config', {})
            
            backend = KafkaBackend(bootstrap_servers, topic, config)
            logger.info(f"Created Kafka backend with topic {topic}")
            return backend
            
        except ImportError as e:
            raise ImportError(
                f"Kafka backend not available: {e}. "
                "Install confluent-kafka or use different backend."
            ) from e
    
    def get_backend_info(self) -> Dict[str, Any]:
        """
        Get information about current backend.
        
        Returns:
            Dict with backend information
        """
        return {
            'type': self.backend.get_backend_type().value,
            'class': self.backend.__class__.__name__,
            'config': self.config
        }
    
    async def health_check(self) -> bool:
        """
        Check if backend is healthy.
        
        Returns:
            True if backend is responsive
        """
        try:
            # Try a simple operation
            test_key = '__health_check__'
            test_value = b'ok'
            
            await self.backend.put(test_key, test_value)
            result = await self.backend.get(test_key)
            await self.backend.delete(test_key)
            
            return result == test_value
            
        except Exception as e:
            logger.error(f"Backend health check failed: {e}")
            return False
    
    def close(self):
        """Close backend and cleanup."""
        if self.backend:
            try:
                self.backend.close()
                logger.info("State backend closed")
            except Exception as e:
                logger.error(f"Error closing backend: {e}")


def create_state_manager(config: Optional[Dict[str, Any]] = None) -> StateManager:
    """
    Create state manager with configuration.
    
    Args:
        config: Configuration dict
        
    Returns:
        StateManager: Initialized state manager
        
    Example:
        manager = create_state_manager({'backend': 'marbledb', 'state_path': './data'})
    """
    return StateManager(config)

