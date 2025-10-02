# -*- coding: utf-8 -*-
"""Arrow files store backend for Sabot tables."""

import asyncio
import json
import os
from pathlib import Path
from typing import Any, Dict, List, Optional, Iterator

try:
    # Try internal Arrow implementation first
    from ..arrow import Table, RecordBatch, Array, Schema, Field, USING_INTERNAL, USING_EXTERNAL
    # Create pa-like namespace for compatibility
    class _ArrowCompat:
        Table = Table
        RecordBatch = RecordBatch
        Array = Array
        Schema = Schema
        Field = Field
    pa = _ArrowCompat()
    ARROW_AVAILABLE = USING_INTERNAL or USING_EXTERNAL
except ImportError:
    # Fall back to external pyarrow if available
    try:
        import pyarrow as pa
        ARROW_AVAILABLE = True
    except ImportError:
        ARROW_AVAILABLE = False
        pa = None

# Parquet support requires external pyarrow
try:
    import pyarrow.parquet as pq
    PARQUET_AVAILABLE = True
except ImportError:
    PARQUET_AVAILABLE = False
    pq = None

from .base import StoreBackend, StoreBackendConfig, Transaction


class ArrowFileTransaction(Transaction):
    """Arrow file transaction implementation."""

    def __init__(self, backend: 'ArrowFileBackend'):
        self.backend = backend
        self.changes: Dict[Any, Any] = {}
        self.deletions: set = set()

    async def get(self, key: Any) -> Optional[Any]:
        """Get within transaction."""
        if key in self.deletions:
            return None
        if key in self.changes:
            return self.changes[key]
        return await self.backend.get(key)

    async def set(self, key: Any, value: Any) -> None:
        """Set within transaction."""
        self.changes[key] = value
        if key in self.deletions:
            self.deletions.remove(key)

    async def delete(self, key: Any) -> bool:
        """Delete within transaction."""
        if key in self.backend._data:
            self.deletions.add(key)
            if key in self.changes:
                del self.changes[key]
            return True
        return False


class ArrowFileBackend(StoreBackend):
    """Arrow files store backend using Parquet files.

    Persistent, Arrow-native storage using Parquet files.
    Good for analytical workloads and large datasets.
    """

    def __init__(self, config: StoreBackendConfig):
        super().__init__(config)
        self.data_path = config.path or Path("./sabot_data")
        self.data_path.mkdir(parents=True, exist_ok=True)
        self._data: Dict[Any, Any] = {}  # In-memory cache
        self._lock = asyncio.Lock()
        self._loaded = False

    async def start(self) -> None:
        """Initialize the Arrow files backend."""
        await self._load_data()

    async def stop(self) -> None:
        """Clean up the Arrow files backend."""
        async with self._lock:
            await self._save_data()
            self._data.clear()

    async def _load_data(self) -> None:
        """Load data from Parquet files."""
        async with self._lock:
            if self._loaded:
                return

            # Load from Parquet file if it exists
            parquet_file = self.data_path / "data.parquet"
            if parquet_file.exists():
                try:
                    table = pq.read_table(parquet_file)

                    # Convert to dict for in-memory operations
                    # This assumes the data has 'key' and 'value' columns
                    if 'key' in table.column_names and 'value' in table.column_names:
                        for batch in table.to_batches():
                            for row in batch.to_pylist():
                                key = row['key']
                                value = json.loads(row['value'])  # Assume JSON serialization
                                self._data[key] = value

                except Exception as e:
                    # Log error but continue with empty data
                    pass

            self._loaded = True

    async def _save_data(self) -> None:
        """Save data to Parquet file."""
        if not self._data:
            return

        try:
            # Convert to Arrow table
            keys = list(self._data.keys())
            values = [json.dumps(v, default=str) for v in self._data.values()]

            table = pa.table({
                'key': keys,
                'value': values
            })

            # Write to Parquet
            parquet_file = self.data_path / "data.parquet"
            pq.write_table(table, parquet_file, compression='snappy')

        except Exception as e:
            # Log error but don't crash
            pass

    async def get(self, key: Any) -> Optional[Any]:
        """Get a value by key."""
        async with self._lock:
            if not self._loaded:
                await self._load_data()
            return self._data.get(key)

    async def set(self, key: Any, value: Any) -> None:
        """Set a value by key."""
        async with self._lock:
            if not self._loaded:
                await self._load_data()
            self._data[key] = value

            # Auto-save periodically (could be configurable)
            if len(self._data) % 100 == 0:  # Save every 100 operations
                await self._save_data()

    async def delete(self, key: Any) -> bool:
        """Delete a value by key."""
        async with self._lock:
            if not self._loaded:
                await self._load_data()
            if key in self._data:
                del self._data[key]
                await self._save_data()  # Save after delete
                return True
            return False

    async def exists(self, key: Any) -> bool:
        """Check if a key exists."""
        async with self._lock:
            if not self._loaded:
                await self._load_data()
            return key in self._data

    async def keys(self, prefix: Optional[str] = None) -> List[Any]:
        """Get all keys, optionally filtered by prefix."""
        async with self._lock:
            if not self._loaded:
                await self._load_data()
            keys = list(self._data.keys())
            if prefix is not None:
                keys = [k for k in keys if str(k).startswith(prefix)]
            return keys

    async def values(self) -> List[Any]:
        """Get all values."""
        async with self._lock:
            if not self._loaded:
                await self._load_data()
            return list(self._data.values())

    async def items(self, prefix: Optional[str] = None) -> List[tuple]:
        """Get all key-value pairs, optionally filtered by prefix."""
        async with self._lock:
            if not self._loaded:
                await self._load_data()
            items = list(self._data.items())
            if prefix is not None:
                items = [(k, v) for k, v in items if str(k).startswith(prefix)]
            return items

    async def clear(self) -> None:
        """Clear all data."""
        async with self._lock:
            self._data.clear()
            await self._save_data()

    async def size(self) -> int:
        """Get number of items stored."""
        async with self._lock:
            if not self._loaded:
                await self._load_data()
            return len(self._data)

    async def scan(
        self,
        start_key: Optional[Any] = None,
        end_key: Optional[Any] = None,
        prefix: Optional[str] = None,
        limit: Optional[int] = None
    ) -> Iterator[tuple]:
        """Scan/range query over keys."""
        async with self._lock:
            if not self._loaded:
                await self._load_data()

            # Sort keys for range queries
            sorted_keys = sorted(self._data.keys())
            results = []

            for key in sorted_keys:
                # Apply prefix filter
                if prefix and not str(key).startswith(prefix):
                    continue

                # Apply range filter
                if start_key is not None and key < start_key:
                    continue
                if end_key is not None and key >= end_key:
                    continue

                results.append((key, self._data[key]))

                # Apply limit
                if limit and len(results) >= limit:
                    break

            return iter(results)

    async def batch_set(self, items: Dict[Any, Any]) -> None:
        """Set multiple key-value pairs in a batch."""
        async with self._lock:
            if not self._loaded:
                await self._load_data()
            self._data.update(items)
            await self._save_data()

    async def batch_delete(self, keys: List[Any]) -> int:
        """Delete multiple keys in a batch."""
        async with self._lock:
            if not self._loaded:
                await self._load_data()
            deleted = 0
            for key in keys:
                if key in self._data:
                    del self._data[key]
                    deleted += 1
            if deleted > 0:
                await self._save_data()
            return deleted

    async def get_stats(self) -> Dict[str, Any]:
        """Get backend statistics."""
        async with self._lock:
            if not self._loaded:
                await self._load_data()

            # Calculate file size
            parquet_file = self.data_path / "data.parquet"
            file_size = parquet_file.stat().st_size if parquet_file.exists() else 0

            return {
                'backend_type': 'arrow_files',
                'size': len(self._data),
                'data_path': str(self.data_path),
                'file_size_bytes': file_size,
                'loaded': self._loaded,
                'compression': 'snappy',
                'max_size': self.config.max_size,
            }

    async def backup(self, path: Path) -> None:
        """Create a backup by copying the data directory."""
        import shutil
        backup_path = path / f"arrow_backup_{int(asyncio.get_event_loop().time())}"
        backup_path.mkdir(parents=True, exist_ok=True)

        # Copy all files
        for file_path in self.data_path.iterdir():
            if file_path.is_file():
                shutil.copy2(file_path, backup_path / file_path.name)

    async def restore(self, path: Path) -> None:
        """Restore from backup."""
        import shutil

        async with self._lock:
            # Clear current data
            self._data.clear()

            # Copy backup files
            for file_path in path.iterdir():
                if file_path.is_file():
                    shutil.copy2(file_path, self.data_path / file_path.name)

            # Reload data
            self._loaded = False
            await self._load_data()

    async def begin_transaction(self) -> Transaction:
        """Begin a transaction."""
        return ArrowFileTransaction(self)

    async def commit_transaction(self, transaction: ArrowFileTransaction) -> None:
        """Commit a transaction."""
        async with self._lock:
            if not self._loaded:
                await self._load_data()

            # Apply changes
            for key, value in transaction.changes.items():
                self._data[key] = value

            # Apply deletions
            for key in transaction.deletions:
                self._data.pop(key, None)

            # Save to disk
            await self._save_data()

    async def rollback_transaction(self, transaction: ArrowFileTransaction) -> None:
        """Rollback a transaction (no-op since changes aren't saved until commit)."""
        # Changes are only in the transaction object, so just discard it
        pass
