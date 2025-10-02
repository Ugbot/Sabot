#!/usr/bin/env python3
"""
RocksDB store backend for Sabot tables.

High-performance, persistent key-value storage with:
- Embedded database (no server required)
- Excellent read/write performance
- Compression and optimization options
- ACID transactions and atomic operations

Good for production state management and local persistence.
"""

import asyncio
import os
import json
import pickle
from typing import Any, Dict, List, Optional, Iterator, Union
from pathlib import Path

from .base import StoreBackend, StoreBackendConfig, Transaction


class RocksDBTransaction(Transaction):
    """RocksDB transaction implementation with ACID properties."""

    def __init__(self, backend: 'RocksDBBackend'):
        self.backend = backend
        self.operations: List[tuple] = []  # (op_type, key, value) tuples
        self._snapshot_data: Dict[bytes, Optional[bytes]] = {}

    async def get(self, key: Any) -> Optional[Any]:
        """Get within transaction."""
        # Check uncommitted changes first
        key_bytes = self.backend._serialize_key(key)
        if key_bytes in self._snapshot_data:
            value_bytes = self._snapshot_data[key_bytes]
            if value_bytes is None:  # Deleted
                return None
            return self.backend._deserialize_value(value_bytes)

        # Fall back to backend
        return await self.backend.get(key)

    async def set(self, key: Any, value: Any) -> None:
        """Set within transaction."""
        key_bytes = self.backend._serialize_key(key)
        value_bytes = self.backend._serialize_value(value)
        self._snapshot_data[key_bytes] = value_bytes
        self.operations.append(('set', key, value))

    async def delete(self, key: Any) -> bool:
        """Delete within transaction."""
        key_bytes = self.backend._serialize_key(key)
        existed = key_bytes in self._snapshot_data or await self.backend.exists(key)
        self._snapshot_data[key_bytes] = None  # Mark as deleted
        self.operations.append(('delete', key, None))
        return existed

    async def commit(self) -> None:
        """Commit the transaction."""
        # Apply all changes atomically
        write_batch = self.backend._db.write_batch()

        for key_bytes, value_bytes in self._snapshot_data.items():
            if value_bytes is None:
                write_batch.delete(key_bytes)
            else:
                write_batch.put(key_bytes, value_bytes)

        write_batch.write()
        self.operations.clear()
        self._snapshot_data.clear()

    async def rollback(self) -> None:
        """Rollback the transaction."""
        self.operations.clear()
        self._snapshot_data.clear()


class RocksDBBackend(StoreBackend):
    """
    RocksDB store backend for persistent, high-performance storage.

    Features:
    - Embedded key-value database
    - ACID transactions
    - Compression options
    - Excellent read/write performance
    - Automatic compaction and optimization
    """

    def __init__(self, config: StoreBackendConfig):
        super().__init__(config)

        # Initialize RocksDB
        self._db = None
        self._db_path = config.path or Path("./sabot_rocksdb")
        self._db_options = self._create_db_options(config)

        # Serialization options
        self.serializer = config.options.get('serializer', 'json')  # 'json', 'pickle', or 'raw'
        self.key_serializer = config.options.get('key_serializer', 'str')  # 'str', 'json', 'pickle'

        # Thread safety
        self._lock = asyncio.Lock()

    def _create_db_options(self, config: StoreBackendConfig) -> Dict[str, Any]:
        """Create RocksDB options from config."""
        options = {
            'create_if_missing': True,
            'max_open_files': config.options.get('max_open_files', -1),
            'write_buffer_size': config.options.get('write_buffer_size', 64 * 1024 * 1024),  # 64MB
            'max_write_buffer_number': config.options.get('max_write_buffer_number', 3),
            'target_file_size_base': config.options.get('target_file_size_base', 64 * 1024 * 1024),  # 64MB
        }

        # Compression options
        compression_type = config.compression or config.options.get('compression', 'lz4')
        if compression_type == 'lz4':
            options['compression'] = 'lz4'
        elif compression_type == 'snappy':
            options['compression'] = 'snappy'
        elif compression_type == 'zlib':
            options['compression'] = 'zlib'
        elif compression_type == 'bz2':
            options['compression'] = 'bz2'

        return options

    async def start(self) -> None:
        """Initialize the RocksDB backend."""
        try:
            import rocksdb
        except ImportError:
            raise RuntimeError("RocksDB not available. Install with: pip install rocksdb")

        # Ensure directory exists
        self._db_path.mkdir(parents=True, exist_ok=True)

        # Create DB options
        db_options = rocksdb.Options(**self._db_options)

        # Open database
        self._db = rocksdb.DB(str(self._db_path), db_options)

        # Create column families if needed
        # For now, we use the default column family

    async def stop(self) -> None:
        """Clean up the RocksDB backend."""
        async with self._lock:
            if self._db:
                # RocksDB doesn't have an explicit close method
                # The database will be closed when the object is garbage collected
                self._db = None

    def _serialize_key(self, key: Any) -> bytes:
        """Serialize key for RocksDB storage."""
        if self.key_serializer == 'str':
            return str(key).encode('utf-8')
        elif self.key_serializer == 'json':
            return json.dumps(key, sort_keys=True).encode('utf-8')
        elif self.key_serializer == 'pickle':
            return pickle.dumps(key)
        else:
            return str(key).encode('utf-8')

    def _deserialize_key(self, key_bytes: bytes) -> Any:
        """Deserialize key from RocksDB storage."""
        if self.key_serializer == 'str':
            return key_bytes.decode('utf-8')
        elif self.key_serializer == 'json':
            return json.loads(key_bytes.decode('utf-8'))
        elif self.key_serializer == 'pickle':
            return pickle.loads(key_bytes)
        else:
            return key_bytes.decode('utf-8')

    def _serialize_value(self, value: Any) -> bytes:
        """Serialize value for RocksDB storage."""
        if self.serializer == 'json':
            return json.dumps(value, default=str).encode('utf-8')
        elif self.serializer == 'pickle':
            return pickle.dumps(value)
        elif self.serializer == 'raw':
            if isinstance(value, str):
                return value.encode('utf-8')
            elif isinstance(value, bytes):
                return value
            else:
                return str(value).encode('utf-8')
        else:
            return json.dumps(value, default=str).encode('utf-8')

    def _deserialize_value(self, value_bytes: bytes) -> Any:
        """Deserialize value from RocksDB storage."""
        if self.serializer == 'json':
            return json.loads(value_bytes.decode('utf-8'))
        elif self.serializer == 'pickle':
            return pickle.loads(value_bytes)
        elif self.serializer == 'raw':
            try:
                return value_bytes.decode('utf-8')
            except UnicodeDecodeError:
                return value_bytes
        else:
            return json.loads(value_bytes.decode('utf-8'))

    async def get(self, key: Any) -> Optional[Any]:
        """Get a value by key."""
        if not self._db:
            return None

        async with self._lock:
            try:
                key_bytes = self._serialize_key(key)
                value_bytes = self._db.get(key_bytes)
                if value_bytes is None:
                    return None
                return self._deserialize_value(value_bytes)
            except Exception as e:
                # Log error but don't crash
                print(f"RocksDB get error: {e}")
                return None

    async def set(self, key: Any, value: Any) -> None:
        """Set a value by key."""
        if not self._db:
            raise RuntimeError("RocksDB backend not initialized")

        async with self._lock:
            try:
                key_bytes = self._serialize_key(key)
                value_bytes = self._serialize_value(value)
                self._db.put(key_bytes, value_bytes)
            except Exception as e:
                raise RuntimeError(f"RocksDB set error: {e}")

    async def delete(self, key: Any) -> bool:
        """Delete a value by key."""
        if not self._db:
            return False

        async with self._lock:
            try:
                key_bytes = self._serialize_key(key)
                # Check if key exists before deletion
                exists = self._db.get(key_bytes) is not None
                if exists:
                    self._db.delete(key_bytes)
                return exists
            except Exception as e:
                print(f"RocksDB delete error: {e}")
                return False

    async def exists(self, key: Any) -> bool:
        """Check if a key exists."""
        if not self._db:
            return False

        async with self._lock:
            try:
                key_bytes = self._serialize_key(key)
                return self._db.get(key_bytes) is not None
            except Exception as e:
                print(f"RocksDB exists error: {e}")
                return False

    async def keys(self, prefix: Optional[str] = None) -> List[Any]:
        """Get all keys, optionally filtered by prefix."""
        if not self._db:
            return []

        async with self._lock:
            try:
                keys = []
                it = self._db.iterkeys()
                it.seek_to_first()

                for key_bytes in it:
                    try:
                        key = self._deserialize_key(key_bytes)
                        if prefix is None or str(key).startswith(prefix):
                            keys.append(key)
                    except Exception:
                        # Skip keys that can't be deserialized
                        continue

                return keys
            except Exception as e:
                print(f"RocksDB keys error: {e}")
                return []

    async def values(self) -> List[Any]:
        """Get all values."""
        if not self._db:
            return []

        async with self._lock:
            try:
                values = []
                it = self._db.itervalues()
                it.seek_to_first()

                for value_bytes in it:
                    try:
                        value = self._deserialize_value(value_bytes)
                        values.append(value)
                    except Exception:
                        # Skip values that can't be deserialized
                        continue

                return values
            except Exception as e:
                print(f"RocksDB values error: {e}")
                return []

    async def items(self, prefix: Optional[str] = None) -> List[tuple]:
        """Get all key-value pairs, optionally filtered by prefix."""
        if not self._db:
            return []

        async with self._lock:
            try:
                items = []
                it = self._db.iteritems()
                it.seek_to_first()

                for key_bytes, value_bytes in it:
                    try:
                        key = self._deserialize_key(key_bytes)
                        if prefix is None or str(key).startswith(prefix):
                            value = self._deserialize_value(value_bytes)
                            items.append((key, value))
                    except Exception:
                        # Skip items that can't be deserialized
                        continue

                return items
            except Exception as e:
                print(f"RocksDB items error: {e}")
                return []

    async def clear(self) -> None:
        """Clear all data."""
        if not self._db:
            return

        async with self._lock:
            try:
                # Delete all keys
                batch = self._db.write_batch()
                it = self._db.iterkeys()
                it.seek_to_first()

                for key_bytes in it:
                    batch.delete(key_bytes)

                batch.write()
            except Exception as e:
                print(f"RocksDB clear error: {e}")

    async def size(self) -> int:
        """Get number of items stored."""
        keys = await self.keys()
        return len(keys)

    async def scan(
        self,
        start_key: Optional[Any] = None,
        end_key: Optional[Any] = None,
        prefix: Optional[str] = None,
        limit: Optional[int] = None
    ) -> List[tuple]:
        """Scan key-value pairs with optional range and limit."""
        if not self._db:
            return []

        async with self._lock:
            try:
                items = []
                it = self._db.iteritems()

                # Set start position
                if start_key is not None:
                    start_key_bytes = self._serialize_key(start_key)
                    it.seek(start_key_bytes)
                else:
                    it.seek_to_first()

                count = 0
                for key_bytes, value_bytes in it:
                    if limit and count >= limit:
                        break

                    try:
                        key = self._deserialize_key(key_bytes)

                        # Check end condition
                        if end_key is not None:
                            if key > end_key:
                                break

                        # Check prefix condition
                        if prefix is not None and not str(key).startswith(prefix):
                            continue

                        value = self._deserialize_value(value_bytes)
                        items.append((key, value))
                        count += 1

                    except Exception:
                        # Skip items that can't be deserialized
                        continue

                return items
            except Exception as e:
                print(f"RocksDB scan error: {e}")
                return []

    async def batch_set(self, items: Dict[Any, Any]) -> None:
        """Set multiple key-value pairs in a batch."""
        if not self._db:
            raise RuntimeError("RocksDB backend not initialized")

        async with self._lock:
            try:
                batch = self._db.write_batch()
                for key, value in items.items():
                    key_bytes = self._serialize_key(key)
                    value_bytes = self._serialize_value(value)
                    batch.put(key_bytes, value_bytes)
                batch.write()
            except Exception as e:
                raise RuntimeError(f"RocksDB batch_set error: {e}")

    async def batch_delete(self, keys: List[Any]) -> int:
        """Delete multiple keys in a batch. Returns number deleted."""
        if not self._db:
            return 0

        async with self._lock:
            try:
                batch = self._db.write_batch()
                deleted_count = 0
                for key in keys:
                    key_bytes = self._serialize_key(key)
                    # Check if key exists before deletion
                    if self._db.get(key_bytes) is not None:
                        batch.delete(key_bytes)
                        deleted_count += 1
                batch.write()
                return deleted_count
            except Exception as e:
                print(f"RocksDB batch_delete error: {e}")
                return 0

    async def begin_transaction(self) -> RocksDBTransaction:
        """Begin a new transaction."""
        return RocksDBTransaction(self)

    async def backup(self, backup_path: Union[str, Path]) -> None:
        """Create a backup of the database."""
        if not self._db:
            raise RuntimeError("RocksDB backend not initialized")

        backup_path = Path(backup_path)
        backup_path.mkdir(parents=True, exist_ok=True)

        async with self._lock:
            try:
                # Create a checkpoint (simplified backup)
                checkpoint = self._db.checkpoint()
                checkpoint.create_checkpoint(str(backup_path))
            except Exception as e:
                raise RuntimeError(f"RocksDB backup failed: {e}")

    async def restore(self, backup_path: Union[str, Path]) -> None:
        """Restore database from backup."""
        backup_path = Path(backup_path)
        if not backup_path.exists():
            raise FileNotFoundError(f"Backup path does not exist: {backup_path}")

        # Close current database
        await self.stop()

        # Copy backup files to database path
        import shutil
        if backup_path.is_dir():
            # Remove existing database files
            if self._db_path.exists():
                shutil.rmtree(self._db_path)

            # Copy backup
            shutil.copytree(backup_path, self._db_path)
        else:
            raise ValueError("Backup path must be a directory")

        # Reinitialize
        await self.start()

    def get_stats(self) -> Dict[str, Any]:
        """Get RocksDB statistics."""
        if not self._db:
            return {"status": "not_initialized"}

        try:
            # Get basic stats
            stats = {
                "path": str(self._db_path),
                "size_on_disk": sum(
                    f.stat().st_size for f in self._db_path.rglob("*") if f.is_file()
                ),
                "serializer": self.serializer,
                "key_serializer": self.key_serializer,
                "compression": self._db_options.get('compression', 'none')
            }

            # Try to get RocksDB stats if available
            try:
                rocksdb_stats = self._db.get_property('rocksdb.stats')
                if rocksdb_stats:
                    stats['rocksdb_stats'] = rocksdb_stats
            except:
                pass

            return stats

        except Exception as e:
            return {"error": str(e)}

    async def compact(self) -> None:
        """Manually trigger database compaction."""
        if not self._db:
            return

        async with self._lock:
            try:
                # Compact the entire database
                self._db.compact_range()
            except Exception as e:
                print(f"RocksDB compaction error: {e}")

    async def optimize(self) -> None:
        """Optimize database performance."""
        await self.compact()

        # Could also adjust options for better performance
        # This is a simplified version
