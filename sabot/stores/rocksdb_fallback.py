# -*- coding: utf-8 -*-
"""
RocksDB State Backend - Python Implementation with SQLite Fallback

This is a Python implementation of the RocksDB state backend that can be used
while the Cython version is being developed. It provides the same interface
but uses SQLite as a fallback when RocksDB is not available.
"""

import os
import pickle
import sqlite3
import threading
import logging
import shutil
from pathlib import Path
from typing import Any, Dict, List, Optional, Iterator, Union, Tuple

from .base import StoreBackend, StoreBackendConfig

logger = logging.getLogger(__name__)

# RocksDB is not properly installed - using SQLite fallback only
ROCKSDB_AVAILABLE = False
rocksdb = None
logger.warning("RocksDB library not available - using SQLite fallback")


class RocksDBBackend(StoreBackend):
    """
    RocksDB-based store backend implementation.

    Provides persistent key-value storage using RocksDB when available,
    with SQLite fallback for development.
    """

    def __init__(self, config: StoreBackendConfig):
        super().__init__(config)
        self._db_path = str(config.path) if config.path else "./sabot_rocksdb_store"
        self._db = None
        self._fallback_conn = None
        self._fallback_lock = threading.Lock()
        self._is_open = False

    async def start(self) -> None:
        """Start the backend."""
        if self._is_open:
            return

        try:
            if ROCKSDB_AVAILABLE:
                # Use RocksDB
                opts = rocksdb.Options()
                opts.create_if_missing = True
                opts.write_buffer_size = 64 * 1024 * 1024  # 64MB write buffer
                opts.max_write_buffer_number = 3
                opts.target_file_size_base = 64 * 1024 * 1024  # 64MB SST files
                opts.max_background_compactions = 4
                opts.max_background_flushes = 2

                self._db = rocksdb.DB(self._db_path, opts)
                logger.info(f"RocksDB store backend opened at {self._db_path}")
            else:
                # Use SQLite fallback
                db_file = f"{self._db_path}.db"
                self._fallback_conn = sqlite3.connect(db_file, check_same_thread=False)
                self._fallback_conn.execute("""
                    CREATE TABLE IF NOT EXISTS kv_store (
                        key TEXT PRIMARY KEY,
                        value BLOB
                    )
                """)
                self._fallback_conn.commit()
                logger.info(f"SQLite fallback store backend opened at {db_file}")

            self._is_open = True

        except Exception as e:
            logger.error(f"Failed to open database: {e}")
            raise RuntimeError(f"Database open failed: {e}")

    async def stop(self) -> None:
        """Stop the backend."""
        if not self._is_open:
            return

        try:
            if ROCKSDB_AVAILABLE and self._db:
                self._db.close()
                self._db = None
                logger.info("RocksDB store backend closed")
            elif self._fallback_conn:
                self._fallback_conn.close()
                self._fallback_conn = None
                logger.info("SQLite fallback store backend closed")

            self._is_open = False

        except Exception as e:
            logger.error(f"Error closing database: {e}")

    async def get(self, key: Any) -> Optional[Any]:
        """Get value by key."""
        if not self._is_open:
            return None

        try:
            key_str = str(key)
            if ROCKSDB_AVAILABLE and self._db:
                value_bytes = self._db.get(key_str.encode('utf-8'))
            elif self._fallback_conn:
                with self._fallback_lock:
                    cursor = self._fallback_conn.execute(
                        "SELECT value FROM kv_store WHERE key = ?",
                        (key_str,)
                    )
                    row = cursor.fetchone()
                    value_bytes = row[0] if row else None
            else:
                return None

            if value_bytes is None:
                return None

            return pickle.loads(value_bytes)

        except Exception as e:
            logger.error(f"Failed to get key {key}: {e}")
            return None

    async def set(self, key: Any, value: Any) -> None:
        """Set key-value pair."""
        if not self._is_open:
            raise RuntimeError("Database not open")

        try:
            key_str = str(key)
            serialized_value = pickle.dumps(value)

            if ROCKSDB_AVAILABLE and self._db:
                self._db.put(key_str.encode('utf-8'), serialized_value)
            elif self._fallback_conn:
                with self._fallback_lock:
                    self._fallback_conn.execute(
                        "INSERT OR REPLACE INTO kv_store (key, value) VALUES (?, ?)",
                        (key_str, serialized_value)
                    )
            else:
                raise RuntimeError("No database backend available")

        except Exception as e:
            logger.error(f"Failed to set key {key}: {e}")
            raise

    async def delete(self, key: Any) -> bool:
        """Delete key-value pair. Returns True if deleted."""
        if not self._is_open:
            return False

        try:
            key_str = str(key)
            existed = await self.exists(key)

            if ROCKSDB_AVAILABLE and self._db:
                self._db.delete(key_str.encode('utf-8'))
            elif self._fallback_conn:
                with self._fallback_lock:
                    self._fallback_conn.execute(
                        "DELETE FROM kv_store WHERE key = ?",
                        (key_str,)
                    )

            return existed

        except Exception as e:
            logger.error(f"Failed to delete key {key}: {e}")
            return False

    async def exists(self, key: Any) -> bool:
        """Check if key exists."""
        return await self.get(key) is not None

    async def keys(self, prefix: Optional[str] = None) -> List[Any]:
        """Get all keys, optionally filtered by prefix."""
        if not self._is_open:
            return []

        try:
            keys = []
            if ROCKSDB_AVAILABLE and self._db:
                # RocksDB iterator
                it = self._db.iterkeys()
                it.seek_to_first()
                while it.valid():
                    key_bytes = it.key()
                    key = key_bytes.decode('utf-8')
                    if prefix is None or key.startswith(prefix):
                        keys.append(key)
                    it.next()
                it.close()
            elif self._fallback_conn:
                with self._fallback_lock:
                    query = "SELECT key FROM kv_store"
                    params = ()
                    if prefix:
                        query += " WHERE key LIKE ?"
                        params = (f"{prefix}%",)

                    cursor = self._fallback_conn.execute(query, params)
                    keys = [row[0] for row in cursor.fetchall()]

            return keys

        except Exception as e:
            logger.error(f"Failed to get keys: {e}")
            return []

    async def values(self) -> List[Any]:
        """Get all values."""
        if not self._is_open:
            return []

        try:
            values = []
            if ROCKSDB_AVAILABLE and self._db:
                it = self._db.itervalues()
                it.seek_to_first()
                while it.valid():
                    value_bytes = it.value()
                    values.append(pickle.loads(value_bytes))
                    it.next()
                it.close()
            elif self._fallback_conn:
                with self._fallback_lock:
                    cursor = self._fallback_conn.execute("SELECT value FROM kv_store")
                    values = [pickle.loads(row[0]) for row in cursor.fetchall()]

            return values

        except Exception as e:
            logger.error(f"Failed to get values: {e}")
            return []

    async def items(self, prefix: Optional[str] = None) -> List[tuple]:
        """Get all key-value pairs."""
        if not self._is_open:
            return []

        try:
            items = []
            if ROCKSDB_AVAILABLE and self._db:
                it = self._db.iteritems()
                it.seek_to_first()
                while it.valid():
                    key_bytes = it.key()
                    value_bytes = it.value()
                    key = key_bytes.decode('utf-8')
                    if prefix is None or key.startswith(prefix):
                        items.append((key, pickle.loads(value_bytes)))
                    it.next()
                it.close()
            elif self._fallback_conn:
                with self._fallback_lock:
                    query = "SELECT key, value FROM kv_store"
                    params = ()
                    if prefix:
                        query += " WHERE key LIKE ?"
                        params = (f"{prefix}%",)

                    cursor = self._fallback_conn.execute(query, params)
                    items = [(row[0], pickle.loads(row[1])) for row in cursor.fetchall()]

            return items

        except Exception as e:
            logger.error(f"Failed to get items: {e}")
            return []

    async def clear(self) -> None:
        """Clear all data."""
        if not self._is_open:
            return

        try:
            if ROCKSDB_AVAILABLE and self._db:
                # RocksDB doesn't have a simple clear, so we'd need to recreate
                # For now, just delete all keys
                keys_to_delete = await self.keys()
                for key in keys_to_delete:
                    self._db.delete(key.encode('utf-8'))
            elif self._fallback_conn:
                with self._fallback_lock:
                    self._fallback_conn.execute("DELETE FROM kv_store")
                    self._fallback_conn.commit()

        except Exception as e:
            logger.error(f"Failed to clear: {e}")

    async def size(self) -> int:
        """Get number of items stored."""
        if not self._is_open:
            return 0

        try:
            if ROCKSDB_AVAILABLE and self._db:
                # Approximate size using iterator
                count = 0
                it = self._db.iterkeys()
                it.seek_to_first()
                while it.valid():
                    count += 1
                    it.next()
                it.close()
                return count
            elif self._fallback_conn:
                with self._fallback_lock:
                    cursor = self._fallback_conn.execute("SELECT COUNT(*) FROM kv_store")
                    return cursor.fetchone()[0]
            else:
                return 0

        except Exception as e:
            logger.error(f"Failed to get size: {e}")
            return 0

    async def scan(
        self,
        start_key: Optional[Any] = None,
        end_key: Optional[Any] = None,
        prefix: Optional[str] = None,
        limit: Optional[int] = None
    ):
        """Scan/range query over keys."""
        if not self._is_open:
            return

        try:
            items = await self.items(prefix)
            # Apply start/end filtering
            if start_key is not None:
                start_str = str(start_key)
                items = [(k, v) for k, v in items if k >= start_str]
            if end_key is not None:
                end_str = str(end_key)
                items = [(k, v) for k, v in items if k < end_str]

            # Apply limit
            if limit is not None:
                items = items[:limit]

            for item in items:
                yield item

        except Exception as e:
            logger.error(f"Failed to scan: {e}")

    async def batch_set(self, items: Dict[Any, Any]) -> None:
        """Set multiple key-value pairs in a batch."""
        if not self._is_open:
            raise RuntimeError("Database not open")

        try:
            if ROCKSDB_AVAILABLE and self._db:
                # Use RocksDB batch
                batch = rocksdb.WriteBatch()
                for key, value in items.items():
                    key_str = str(key)
                    serialized_value = pickle.dumps(value)
                    batch.put(key_str.encode('utf-8'), serialized_value)
                self._db.write(batch)
            elif self._fallback_conn:
                with self._fallback_lock:
                    for key, value in items.items():
                        key_str = str(key)
                        serialized_value = pickle.dumps(value)
                        self._fallback_conn.execute(
                            "INSERT OR REPLACE INTO kv_store (key, value) VALUES (?, ?)",
                            (key_str, serialized_value)
                    )
                    self._fallback_conn.commit()
            else:
                raise RuntimeError("No database backend available")

        except Exception as e:
            logger.error(f"Failed to batch set: {e}")
            raise

    async def batch_delete(self, keys: List[Any]) -> int:
        """Delete multiple keys in a batch. Returns number deleted."""
        if not self._is_open:
            return 0

        try:
            deleted_count = 0
            if ROCKSDB_AVAILABLE and self._db:
                batch = rocksdb.WriteBatch()
                for key in keys:
                    key_str = str(key)
                    if await self.exists(key):
                        batch.delete(key_str.encode('utf-8'))
                        deleted_count += 1
                self._db.write(batch)
            elif self._fallback_conn:
                with self._fallback_lock:
                    for key in keys:
                        key_str = str(key)
                        cursor = self._fallback_conn.execute(
                            "SELECT 1 FROM kv_store WHERE key = ?",
                            (key_str,)
                        )
                        if cursor.fetchone():
                            self._fallback_conn.execute(
                                "DELETE FROM kv_store WHERE key = ?",
                                (key_str,)
                            )
                            deleted_count += 1
                    self._fallback_conn.commit()

            return deleted_count

        except Exception as e:
            logger.error(f"Failed to batch delete: {e}")
            return 0

    async def get_stats(self) -> Dict[str, Any]:
        """Get backend statistics."""
        size_count = await self.size()
        return {
            "backend_type": "rocksdb" if ROCKSDB_AVAILABLE else "sqlite_fallback",
            "is_open": self._is_open,
            "db_path": self._db_path,
            "size": size_count,
        }

    async def backup(self, path: Path) -> None:
        """Create a backup."""
        # Simplified implementation - just copy the database file
        try:
            if ROCKSDB_AVAILABLE and self._db:
                # Close and copy all SST files (simplified)
                self._db.close()
                # This is a very basic backup - real implementation would use RocksDB backup API
                shutil.copytree(self._db_path, str(path), dirs_exist_ok=True)
                # Reopen
                opts = rocksdb.Options()
                opts.create_if_missing = True
                self._db = rocksdb.DB(self._db_path, opts)
            elif self._fallback_conn:
                shutil.copy(f"{self._db_path}.db", str(path))
        except Exception as e:
            logger.error(f"Backup failed: {e}")
            raise

    async def restore(self, path: Path) -> None:
        """Restore from backup."""
        import shutil
        try:
            await self.stop()
            if ROCKSDB_AVAILABLE:
                shutil.copytree(str(path), self._db_path, dirs_exist_ok=True)
            else:
                shutil.copy(str(path), f"{self._db_path}.db")
            await self.start()
        except Exception as e:
            logger.error(f"Restore failed: {e}")
            raise
