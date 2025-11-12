# -*- coding: utf-8 -*-
"""MarbleDB store backend for Sabot tables.

Provides Arrow-native table storage using MarbleDB's InsertBatch and ScanTable APIs.
"""

import asyncio
import logging
from typing import Any, Dict, List, Optional, Iterator
from pathlib import Path

from .base import StoreBackend, StoreBackendConfig, Transaction

logger = logging.getLogger(__name__)

# Try to import Cython backend
try:
    from sabot._cython.stores.marbledb_store import MarbleDBStoreBackend as CyMarbleDBStoreBackend
    MARBLEDB_STORE_AVAILABLE = True
    logger.info("MarbleDB Cython store backend loaded successfully")
except ImportError as e:
    MARBLEDB_STORE_AVAILABLE = False
    CyMarbleDBStoreBackend = None
    logger.warning(f"MarbleDB Cython store backend not available: {e}")


class MarbleDBStoreBackend(StoreBackend):
    """MarbleDB store backend using Arrow Batch API.
    
    Provides high-performance Arrow table storage with:
    - InsertBatch for bulk RecordBatch insertion
    - ScanTable for full table scans
    - NewIterator for range scans with sparse indexing
    - CreateColumnFamily for table creation with schemas
    """

    def __init__(self, config: StoreBackendConfig):
        super().__init__(config)
        self.db_path = Path(config.path) if config.path else Path("./sabot_marbledb_store")
        self.db_path.mkdir(parents=True, exist_ok=True)
        self._backend = None
        self._lock = asyncio.Lock()

        if not MARBLEDB_STORE_AVAILABLE:
            raise RuntimeError(
                "MarbleDB Cython store backend not available. "
                "Please build Cython extensions: cd /path/to/Sabot && uv run python setup.py build_ext --inplace"
            )

    async def start(self) -> None:
        """Initialize the MarbleDB store backend."""
        try:
            config_dict = self.config.options or {}
            self._backend = CyMarbleDBStoreBackend(str(self.db_path), config_dict)
            self._backend.open()
            logger.info(f"MarbleDB store backend initialized at {self.db_path}")
        except Exception as e:
            logger.error(f"Failed to initialize MarbleDB store backend: {e}")
            raise

    async def stop(self) -> None:
        """Clean up the MarbleDB store backend."""
        async with self._lock:
            if self._backend is not None:
                try:
                    self._backend.close()
                    logger.info("Closed MarbleDB store backend")
                except Exception as e:
                    logger.error(f"Error closing MarbleDB store backend: {e}")
                self._backend = None

    async def get(self, key: Any) -> Optional[Any]:
        """Get a value by key."""
        # For MarbleDB, we'd need to use point API or scan
        # For now, use scan_table and filter
        raise NotImplementedError("Point get not yet implemented - use scan with filter")

    async def set(self, key: Any, value: Any) -> None:
        """Set a value by key."""
        # For MarbleDB, convert to Arrow batch and insert
        import pyarrow as pa
        
        # Convert value to Arrow RecordBatch
        if isinstance(value, dict):
            # Convert dict to RecordBatch
            arrays = []
            fields = []
            for k, v in value.items():
                if isinstance(v, (int, float)):
                    arrays.append(pa.array([v]))
                    fields.append(pa.field(k, pa.int64() if isinstance(v, int) else pa.float64()))
                else:
                    arrays.append(pa.array([str(v)]))
                    fields.append(pa.field(k, pa.string()))
            
            schema = pa.schema(fields)
            batch = pa.RecordBatch.from_arrays(arrays, schema=schema)
            
            # Use a default table name based on key
            table_name = f"key_value_{hash(str(key)) % 1000}"
            self._backend.insert_batch(table_name, batch)
        else:
            raise ValueError(f"Unsupported value type: {type(value)}")

    async def delete(self, key: Any) -> bool:
        """Delete a value by key."""
        # Would need point API or scan+delete
        raise NotImplementedError("Point delete not yet implemented")

    async def exists(self, key: Any) -> bool:
        """Check if a key exists."""
        return await self.get(key) is not None

    async def keys(self, prefix: Optional[str] = None) -> List[Any]:
        """Get all keys, optionally filtered by prefix."""
        # Would need to scan tables
        raise NotImplementedError("Keys not yet implemented - use scan_table")

    async def values(self) -> List[Any]:
        """Get all values."""
        raise NotImplementedError("Values not yet implemented - use scan_table")

    async def items(self, prefix: Optional[str] = None) -> List[tuple]:
        """Get all key-value pairs."""
        raise NotImplementedError("Items not yet implemented - use scan_table")

    async def clear(self) -> None:
        """Clear all data."""
        # Would need to drop and recreate column families
        raise NotImplementedError("Clear not yet implemented")

    async def size(self) -> int:
        """Get number of items stored."""
        # Would need to scan all tables
        raise NotImplementedError("Size not yet implemented")

    async def scan(
        self,
        start_key: Optional[Any] = None,
        end_key: Optional[Any] = None,
        prefix: Optional[str] = None,
        limit: Optional[int] = None
    ) -> Iterator[tuple]:
        """Scan/range query over keys."""
        # Use scan_range_to_table
        table_name = prefix or "default"
        table = self._backend.scan_range_to_table(table_name, start_key, end_key)
        
        # Convert table to iterator of tuples
        for batch in table.to_batches():
            for row in batch.to_pylist():
                yield tuple(row.values())

    async def batch_set(self, items: Dict[Any, Any]) -> None:
        """Set multiple key-value pairs in a batch."""
        # Convert items to Arrow RecordBatch and insert
        import pyarrow as pa
        
        # Build arrays from items
        # This is simplified - would need proper schema handling
        raise NotImplementedError("Batch set needs proper Arrow schema handling")

    async def batch_delete(self, keys: List[Any]) -> int:
        """Delete multiple keys in a batch."""
        deleted = 0
        for key in keys:
            if await self.delete(key):
                deleted += 1
        return deleted

    async def get_stats(self) -> Dict[str, Any]:
        """Get backend statistics."""
        cf_list = self._backend.list_column_families()
        return {
            'backend_type': 'marbledb',
            'db_path': str(self.db_path),
            'column_families': cf_list,
            'num_column_families': len(cf_list)
        }

    async def backup(self, path: Path) -> None:
        """Create a backup."""
        import shutil
        backup_path = path / f"marbledb_backup_{int(asyncio.get_event_loop().time())}"
        shutil.copytree(self.db_path, backup_path)

    async def restore(self, path: Path) -> None:
        """Restore from backup."""
        import shutil
        if self.db_path.exists():
            shutil.rmtree(self.db_path)
        shutil.copytree(path, self.db_path)
        # Reopen backend
        await self.stop()
        await self.start()

    # Arrow-specific methods

    async def arrow_insert_batch(self, table_name: str, batch) -> None:
        """Insert Arrow RecordBatch into table."""
        if self._backend is None:
            raise RuntimeError("MarbleDB backend not initialized")
        self._backend.insert_batch(table_name, batch)
    
    async def arrow_create_table(self, table_name: str, schema) -> None:
        """Create table with Arrow schema."""
        if self._backend is None:
            raise RuntimeError("MarbleDB backend not initialized")
        self._backend.create_column_family(table_name, schema)

    async def arrow_scan_to_table(self, table_name: str, start_key: str = None,
                                 end_key: str = None, limit: int = 10000):
        """Scan table and return as Arrow Table."""
        if self._backend is None:
            raise RuntimeError("MarbleDB backend not initialized")
        return self._backend.scan_range_to_table(table_name, start_key, end_key)

    async def arrow_create_table(self, table_name: str, schema) -> None:
        """Create table with Arrow schema."""
        if self._backend is None:
            raise RuntimeError("MarbleDB backend not initialized")
        self._backend.create_column_family(table_name, schema)

