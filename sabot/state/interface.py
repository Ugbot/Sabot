#!/usr/bin/env python3
"""
State Backend Interface

Defines the unified interface that all state backends must implement.
Ensures consistency across MarbleDB, RocksDB, Redis, and Memory backends.
"""

from abc import ABC, abstractmethod
from typing import Optional, Any, AsyncIterator, Tuple, Dict, List
from enum import Enum


class BackendType(Enum):
    """Available state backend types."""
    MARBLEDB = "marbledb"    # Primary: High-performance Arrow LSM
    ROCKSDB = "rocksdb"      # Alternative: Embedded KV store
    REDIS = "redis"          # Alternative: Distributed cache
    MEMORY = "memory"        # Fallback: In-memory for testing
    KAFKA = "kafka"          # Optional: Messaging/changelog


class StateBackend(ABC):
    """
    Unified state backend interface.
    
    All state backends (MarbleDB, RocksDB, Redis, etc.) implement this interface,
    allowing transparent backend swapping and consistent API.
    
    Performance contract:
    - get/put: Must be async-capable
    - scan: Must support prefix iteration
    - checkpoint: Must be atomic
    - All operations work with bytes (serialization handled by caller)
    """
    
    @abstractmethod
    async def get(self, key: str) -> Optional[bytes]:
        """
        Get value for key.
        
        Args:
            key: State key
            
        Returns:
            Value as bytes, or None if not found
        """
        pass
    
    @abstractmethod
    async def put(self, key: str, value: bytes) -> None:
        """
        Put key-value pair.
        
        Args:
            key: State key
            value: Value as bytes
        """
        pass
    
    @abstractmethod
    async def delete(self, key: str) -> None:
        """
        Delete key.
        
        Args:
            key: State key to delete
        """
        pass
    
    @abstractmethod
    async def exists(self, key: str) -> bool:
        """
        Check if key exists.
        
        Args:
            key: State key
            
        Returns:
            True if key exists
        """
        pass
    
    @abstractmethod
    async def scan(self, prefix: str = "") -> AsyncIterator[Tuple[str, bytes]]:
        """
        Scan keys with prefix.
        
        Args:
            prefix: Key prefix to scan (empty = all keys)
            
        Yields:
            (key, value) tuples
        """
        pass
    
    @abstractmethod
    async def multi_get(self, keys: List[str]) -> Dict[str, Optional[bytes]]:
        """
        Batch get multiple keys.
        
        Args:
            keys: List of keys to retrieve
            
        Returns:
            Dict mapping keys to values (None if not found)
        """
        pass
    
    @abstractmethod
    async def multi_put(self, items: Dict[str, bytes]) -> None:
        """
        Batch put multiple key-value pairs.
        
        Args:
            items: Dict of key-value pairs
        """
        pass
    
    @abstractmethod
    async def clear(self) -> None:
        """Clear all data."""
        pass
    
    @abstractmethod
    async def items(self) -> List[Tuple[str, bytes]]:
        """
        Get all items.
        
        Returns:
            List of (key, value) tuples
        """
        pass
    
    @abstractmethod
    async def checkpoint(self) -> str:
        """
        Create checkpoint of current state.
        
        Returns:
            Checkpoint ID
        """
        pass
    
    @abstractmethod
    async def restore(self, checkpoint_id: str) -> None:
        """
        Restore state from checkpoint.
        
        Args:
            checkpoint_id: Checkpoint to restore from
        """
        pass
    
    @abstractmethod
    def close(self) -> None:
        """Close backend and cleanup resources."""
        pass
    
    # Optional: Performance-critical operations
    
    async def merge(self, key: str, value: bytes, merge_func: Optional[Any] = None) -> None:
        """
        Atomic merge operation (optional, for performance).
        
        Args:
            key: State key
            value: Value to merge
            merge_func: Optional merge function (backend-specific)
        """
        # Default implementation: read-modify-write
        current = await self.get(key)
        if current is None:
            await self.put(key, value)
        else:
            # Simple concatenation if no merge function
            if merge_func:
                merged = merge_func(current, value)
            else:
                merged = current + value
            await self.put(key, merged)
    
    async def delete_range(self, start_key: str, end_key: str) -> None:
        """
        Delete range of keys (optional, for performance).
        
        Args:
            start_key: Range start (inclusive)
            end_key: Range end (exclusive)
        """
        # Default implementation: scan and delete
        keys_to_delete = []
        async for key, _ in self.scan():
            if start_key <= key < end_key:
                keys_to_delete.append(key)
        
        for key in keys_to_delete:
            await self.delete(key)
    
    # Metadata and stats
    
    def get_backend_type(self) -> BackendType:
        """
        Get backend type.
        
        Returns:
            BackendType enum
        """
        return BackendType.MEMORY  # Override in subclasses
    
    async def get_stats(self) -> Dict[str, Any]:
        """
        Get backend statistics.
        
        Returns:
            Dict with backend stats (size, operation counts, etc.)
        """
        return {
            'backend_type': self.get_backend_type().value,
            'num_keys': len(await self.items())
        }


class TransactionalStateBackend(StateBackend):
    """
    Extended interface for backends that support transactions.
    
    Backends like MarbleDB and RocksDB can implement this for ACID guarantees.
    """
    
    @abstractmethod
    async def begin_transaction(self) -> 'Transaction':
        """
        Begin a transaction.
        
        Returns:
            Transaction context
        """
        pass
    
    @abstractmethod
    async def commit_transaction(self, txn: 'Transaction') -> None:
        """Commit transaction."""
        pass
    
    @abstractmethod
    async def rollback_transaction(self, txn: 'Transaction') -> None:
        """Rollback transaction."""
        pass


class Transaction(ABC):
    """Transaction context for transactional backends."""
    
    @abstractmethod
    async def get(self, key: str) -> Optional[bytes]:
        """Get within transaction (sees own writes)."""
        pass
    
    @abstractmethod
    async def put(self, key: str, value: bytes) -> None:
        """Put within transaction (buffered)."""
        pass
    
    @abstractmethod
    async def delete(self, key: str) -> None:
        """Delete within transaction."""
        pass


class DistributedStateBackend(StateBackend):
    """
    Extended interface for distributed backends.
    
    Backends like MarbleDB with Raft or Redis clusters implement this.
    """
    
    @abstractmethod
    async def get_cluster_stats(self) -> Dict[str, Any]:
        """
        Get cluster-wide statistics.
        
        Returns:
            Dict with cluster stats (nodes, replication, etc.)
        """
        pass
    
    @abstractmethod
    async def get_node_health(self) -> Dict[str, bool]:
        """
        Get health status of cluster nodes.
        
        Returns:
            Dict mapping node IDs to health status
        """
        pass

