# -*- coding: utf-8 -*-
"""Tonbo store backend for Sabot tables."""

import asyncio
import tempfile
from pathlib import Path
from typing import Any, Dict, List, Optional, Iterator, Union, AsyncIterator

from .base import StoreBackend, StoreBackendConfig, Transaction

# Tonbo imports - will be available when installed
try:
    from tonbo import TonboDB, DbOption, Column, DataType, Record, Bound
    TONBO_AVAILABLE = True
except ImportError:
    TONBO_AVAILABLE = False
    TonboDB = None
    DbOption = None
    Column = None
    DataType = None
    Record = None
    Bound = None

# Cython Tonbo wrapper - provides high-performance operations
try:
    from .._cython.tonbo_wrapper import FastTonboBackend, TonboCythonWrapper
    CYTHON_TONBO_AVAILABLE = True
except ImportError:
    CYTHON_TONBO_AVAILABLE = False
    FastTonboBackend = None
    TonboCythonWrapper = None

# Arrow-integrated Tonbo store - provides columnar operations
try:
    from .._cython.tonbo_arrow import ArrowTonboStore, ArrowTonboIntegration
    ARROW_TONBO_AVAILABLE = True
except ImportError:
    ARROW_TONBO_AVAILABLE = False
    ArrowTonboStore = None
    ArrowTonboIntegration = None


class TonboTransaction(Transaction):
    """Tonbo transaction implementation using Tonbo's native transactions."""

    def __init__(self, backend: 'TonboBackend', tonbo_txn):
        self.backend = backend
        self.tonbo_txn = tonbo_txn  # Native Tonbo transaction

    async def get(self, key: Any) -> Optional[Any]:
        """Get within transaction using Tonbo's transaction API."""
        if not TONBO_AVAILABLE or not self.tonbo_txn:
            return None

        try:
            result = await self.tonbo_txn.get(key)
            return result
        except Exception as e:
            self.backend.logger.error(f"Tonbo transaction get error: {e}")
            return None

    async def set(self, key: Any, value: Any) -> None:
        """Set within transaction using Tonbo's transaction API."""
        if not TONBO_AVAILABLE or not self.tonbo_txn:
            return

        try:
            # Convert value to Tonbo record format
            tonbo_record = self.backend._value_to_tonbo_record(key, value)
            await self.tonbo_txn.insert(tonbo_record)
        except Exception as e:
            self.backend.logger.error(f"Tonbo transaction set error: {e}")
            raise

    async def delete(self, key: Any) -> bool:
        """Delete within transaction using Tonbo's transaction API."""
        if not TONBO_AVAILABLE or not self.tonbo_txn:
            return False

        try:
            # For Tonbo, we need to remove by primary key
            # This assumes the key is the primary key
            await self.tonbo_txn.remove(key)
            return True
        except Exception as e:
            self.backend.logger.error(f"Tonbo transaction delete error: {e}")
            return False


class TonboBackend(StoreBackend):
    """Tonbo store backend using the embedded Arrow database.

    High-performance, persistent storage with LSM tree and Arrow integration.
    Uses zero-copy operations and pushdown predicates.

    Automatically uses Cython optimizations when available for maximum performance.
    """

    def __init__(self, config: StoreBackendConfig):
        super().__init__(config)
        self.db_path = config.path or Path("./sabot_tonbo_db")
        self.db_path.mkdir(parents=True, exist_ok=True)
        self._db = None  # Will hold the Tonbo database instance
        self._schema = None  # Dynamic schema for key-value storage
        self._cython_backend = None  # High-performance Cython backend
        self._arrow_store = None  # Arrow-integrated store for columnar operations
        self._use_cython = CYTHON_TONBO_AVAILABLE  # Use Cython by default if available
        self._use_arrow = ARROW_TONBO_AVAILABLE  # Use Arrow integration if available
        self._lock = asyncio.Lock()

    async def start(self) -> None:
        """Initialize the Tonbo backend with optimal performance settings."""
        if not TONBO_AVAILABLE:
            raise RuntimeError(
                "Tonbo backend requires Tonbo Python bindings. "
                "Install from: pip install tonbo"
            )

        try:
            # Initialize Arrow store first (provides columnar capabilities)
            if self._use_arrow and ARROW_TONBO_AVAILABLE:
                self._arrow_store = ArrowTonboStore(str(self.db_path))
                await self._arrow_store.initialize()
                self.logger.info(f"Tonbo Arrow backend initialized at {self.db_path}")

            # Use Cython backend for maximum performance if available
            if self._use_cython and CYTHON_TONBO_AVAILABLE:
                self._cython_backend = FastTonboBackend(str(self.db_path))
                await self._cython_backend.initialize()
                self.logger.info(f"Tonbo Cython backend initialized at {self.db_path}")

            else:
                # Fallback to Python implementation
                # Create dynamic schema for key-value storage
                temp_dir = str(self.db_path)

                # For dynamic schemas in Tonbo Python bindings, we need to define a record class
                # Since Tonbo Python uses decorators, we'll create a simple key-value record
                class KVRecord:
                    def __init__(self, key: str, value: bytes):
                        self.key = key
                        self.value = value

                # Apply Tonbo decorators dynamically if possible
                if hasattr(KVRecord, '__annotations__'):
                    # Define the schema
                    KVRecord.key = Column(DataType.String, name="key", primary_key=True)
                    KVRecord.value = Column(DataType.Bytes, name="value")
                    KVRecord.__record__ = True  # Mark as record

                # Create database options
                options = DbOption(temp_dir)

                # Initialize database with the record schema
                self._db = TonboDB(options, KVRecord())
                self._schema = KVRecord

                self.logger.info(f"Tonbo Python backend initialized at {self.db_path}")

        except Exception as e:
            self.logger.error(f"Failed to initialize Tonbo backend: {e}")
            raise RuntimeError(f"Tonbo backend initialization failed: {e}")

    async def stop(self) -> None:
        """Clean up the Tonbo backend and Arrow resources."""
        async with self._lock:
            # Close Arrow store first
            if self._arrow_store:
                try:
                    await self._arrow_store.close()
                except Exception as e:
                    self.logger.error(f"Error closing Arrow store: {e}")
                self._arrow_store = None

            # Close Cython backend
            if self._cython_backend:
                try:
                    await self._cython_backend.close()
                except Exception as e:
                    self.logger.error(f"Error closing Cython backend: {e}")
                self._cython_backend = None

            # Close main database
            if self._db:
                # TODO: Close Tonbo database when API is available
                # await self._db.close()
                pass

    def _key_to_string(self, key: Any) -> str:
        """Convert key to string format for Tonbo."""
        if isinstance(key, str):
            return key
        elif isinstance(key, (int, float)):
            return str(key)
        else:
            # For complex keys, use repr
            return repr(key)

    def _value_to_bytes(self, value: Any) -> bytes:
        """Convert value to bytes for storage."""
        import pickle
        return pickle.dumps(value)

    def _bytes_to_value(self, data: bytes) -> Any:
        """Convert bytes back to value."""
        import pickle
        return pickle.loads(data)

    def _value_to_tonbo_record(self, key: Any, value: Any) -> Any:
        """Convert key-value pair to Tonbo record format."""
        if not self._schema:
            raise RuntimeError("Schema not initialized")

        key_str = self._key_to_string(key)
        value_bytes = self._value_to_bytes(value)

        # Create record instance
        return self._schema(key=key_str, value=value_bytes)

    async def get(self, key: Any) -> Optional[Any]:
        """Get a value by key using Tonbo's efficient lookup."""
        # Use Cython backend for maximum performance if available
        if self._cython_backend:
            try:
                key_str = self._key_to_string(key)
                value_bytes = await self._cython_backend.fast_get(key_str)
                return self._bytes_to_value(value_bytes) if value_bytes else None
            except Exception as e:
                self.logger.error(f"Tonbo Cython get error for key {key}: {e}")
                return None

        # Fallback to Python implementation
        if not TONBO_AVAILABLE or not self._db:
            return None

        async with self._lock:
            try:
                key_str = self._key_to_string(key)
                result = await self._db.get(key_str)

                if result and result.get('value'):
                    return self._bytes_to_value(result['value'])
                return None

            except Exception as e:
                self.logger.error(f"Tonbo get error for key {key}: {e}")
                return None

    async def set(self, key: Any, value: Any) -> None:
        """Set a value by key using Tonbo's insert operation."""
        # Use Cython backend for maximum performance if available
        if self._cython_backend:
            try:
                key_str = self._key_to_string(key)
                value_bytes = self._value_to_bytes(value)
                await self._cython_backend.fast_insert(key_str, value_bytes)
                return
            except Exception as e:
                self.logger.error(f"Tonbo Cython set error for key {key}: {e}")
                raise

        # Fallback to Python implementation
        if not TONBO_AVAILABLE or not self._db:
            return

        async with self._lock:
            try:
                tonbo_record = self._value_to_tonbo_record(key, value)
                await self._db.insert(tonbo_record)
            except Exception as e:
                self.logger.error(f"Tonbo set error for key {key}: {e}")
                raise

    async def delete(self, key: Any) -> bool:
        """Delete a value by key using Tonbo's remove operation."""
        # Use Cython backend for maximum performance if available
        if self._cython_backend:
            try:
                key_str = self._key_to_string(key)
                return await self._cython_backend.fast_delete(key_str)
            except Exception as e:
                self.logger.error(f"Tonbo Cython delete error for key {key}: {e}")
                return False

        # Fallback to Python implementation
        if not TONBO_AVAILABLE or not self._db:
            return False

        async with self._lock:
            try:
                key_str = self._key_to_string(key)
                await self._db.remove(key_str)
                return True
            except Exception as e:
                self.logger.error(f"Tonbo delete error for key {key}: {e}")
                return False

    async def exists(self, key: Any) -> bool:
        """Check if a key exists."""
        return await self.get(key) is not None

    async def keys(self, prefix: Optional[str] = None) -> List[Any]:
        """Get all keys, optionally filtered by prefix."""
        if not self._db:
            return []

        async with self._lock:
            try:
                # TODO: Implement Tonbo scan operation
                # keys = await self._db.scan_keys()
                keys = list(self._db.keys())  # Placeholder

                if prefix:
                    keys = [k for k in keys if str(k).startswith(prefix)]

                return keys
            except Exception:
                return []

    async def values(self) -> List[Any]:
        """Get all values."""
        if not self._db:
            return []

        async with self._lock:
            try:
                # TODO: Implement efficient values scan
                keys = await self.keys()
                values = []
                for key in keys:
                    value = await self.get(key)
                    if value is not None:
                        values.append(value)
                return values
            except Exception:
                return []

    async def items(self, prefix: Optional[str] = None) -> List[tuple]:
        """Get all key-value pairs, optionally filtered by prefix."""
        if not self._db:
            return []

        async with self._lock:
            try:
                # TODO: Implement efficient items scan with projection
                keys = await self.keys(prefix)
                items = []
                for key in keys:
                    value = await self.get(key)
                    if value is not None:
                        items.append((key, value))
                return items
            except Exception:
                return []

    async def clear(self) -> None:
        """Clear all data."""
        if not self._db:
            return

        async with self._lock:
            try:
                # TODO: Implement Tonbo clear operation
                # await self._db.clear()
                self._db.clear()  # Placeholder
            except Exception:
                pass

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
    ) -> Iterator[tuple]:
        """Scan/range query over keys with Tonbo's efficient pushdown capabilities."""
        # Use Cython backend for maximum performance if available
        if self._cython_backend:
            try:
                start_str = self._key_to_string(start_key) if start_key else None
                end_str = self._key_to_string(end_key) if end_key else None

                results = []
                async for key, value_bytes, timestamp in self._cython_backend.fast_scan_range(
                    start_str, end_str, limit or 1000
                ):
                    value = self._bytes_to_value(value_bytes)

                    # Apply prefix filter if specified
                    if prefix and not str(key).startswith(prefix):
                        continue

                    results.append((key, value))

                return iter(results)

            except Exception as e:
                self.logger.error(f"Tonbo Cython scan error: {e}")
                return iter([])

        # Fallback to Python implementation
        if not TONBO_AVAILABLE or not self._db:
            return iter([])

        async with self._lock:
            try:
                # Convert keys to Tonbo format
                lower_bound = None
                if start_key is not None:
                    start_str = self._key_to_string(start_key)
                    lower_bound = Bound.Included(start_str)

                upper_bound = None
                if end_key is not None:
                    end_str = self._key_to_string(end_key)
                    upper_bound = Bound.Excluded(end_str)

                # Use Tonbo's efficient scan with bounds and limit
                scan_stream = await self._db.scan(
                    lower_bound,
                    upper_bound,
                    limit=limit,
                    projection=["key", "value"]  # Only fetch what we need
                )

                results = []
                async for record in scan_stream:
                    if record and 'key' in record and 'value' in record:
                        key = record['key']
                        value = self._bytes_to_value(record['value'])

                        # Apply prefix filter if specified
                        if prefix and not str(key).startswith(prefix):
                            continue

                        results.append((key, value))

                return iter(results)

            except Exception as e:
                self.logger.error(f"Tonbo scan error: {e}")
                return iter([])

    async def batch_set(self, items: Dict[Any, Any]) -> None:
        """Set multiple key-value pairs in a batch."""
        if not self._db:
            return

        async with self._lock:
            try:
                # TODO: Implement Tonbo batch insert
                # await self._db.batch_insert(items)
                for key, value in items.items():
                    self._db[key] = value  # Placeholder
            except Exception:
                pass

    async def batch_delete(self, keys: List[Any]) -> int:
        """Delete multiple keys in a batch."""
        if not self._db:
            return 0

        async with self._lock:
            try:
                # TODO: Implement Tonbo batch delete
                # result = await self._db.batch_delete(keys)
                # return result.deleted_count
                deleted = 0
                for key in keys:
                    if key in self._db:
                        del self._db[key]
                        deleted += 1
                return deleted
            except Exception:
                return 0

    async def get_stats(self) -> Dict[str, Any]:
        """Get comprehensive backend statistics including Arrow metrics."""
        size = await self.size()

        # Get Arrow statistics if available
        arrow_stats = await self.get_arrow_stats()

        stats = {
            'backend_type': 'tonbo',
            'size': size,
            'db_path': str(self.db_path),
            'cython_enabled': self._use_cython and CYTHON_TONBO_AVAILABLE,
            'arrow_enabled': self._use_arrow and ARROW_TONBO_AVAILABLE,
            'lsm_tree_levels': 0,  # TODO: Get from Tonbo when API available
            'total_sst_files': 0,  # TODO: Get from Tonbo when API available
            'memory_usage': 0,     # TODO: Get from Tonbo when API available
            'compaction_backlog': 0,  # TODO: Get from Tonbo when API available
        }

        # Merge Arrow statistics
        if arrow_stats:
            stats.update(arrow_stats)

        return stats

    async def backup(self, path: Path) -> None:
        """Create a backup of the Tonbo database."""
        # TODO: Implement Tonbo backup
        # await self._db.backup(path)
        raise NotImplementedError("Tonbo backup not implemented yet")

    async def restore(self, path: Path) -> None:
        """Restore from a Tonbo database backup."""
        # TODO: Implement Tonbo restore
        # await self._db.restore(path)
        raise NotImplementedError("Tonbo restore not implemented yet")

    async def begin_transaction(self) -> Transaction:
        """Begin a transaction using Tonbo's native transaction support."""
        if not TONBO_AVAILABLE or not self._db:
            raise RuntimeError("Tonbo backend not available")

        try:
            # Start a Tonbo transaction
            tonbo_txn = await self._db.transaction()
            return TonboTransaction(self, tonbo_txn)
        except Exception as e:
            self.logger.error(f"Failed to begin Tonbo transaction: {e}")
            raise

    async def commit_transaction(self, transaction: TonboTransaction) -> None:
        """Commit a Tonbo transaction."""
        if not TONBO_AVAILABLE or not hasattr(transaction, 'tonbo_txn'):
            return

        try:
            await transaction.tonbo_txn.commit()
        except Exception as e:
            self.logger.error(f"Failed to commit Tonbo transaction: {e}")
            raise

    async def rollback_transaction(self, transaction: TonboTransaction) -> None:
        """Rollback a Tonbo transaction."""
        # Tonbo handles rollback automatically if commit fails
        # No explicit rollback needed
        pass

    # Arrow-integrated methods for columnar operations

    async def arrow_scan_to_table(self, start_key: Optional[str] = None,
                                end_key: Optional[str] = None,
                                limit: int = 10000) -> Optional[object]:
        """
        Scan data and return as Arrow table for columnar processing.

        Args:
            start_key: Start key for range scan
            end_key: End key for range scan
            limit: Maximum number of rows to return

        Returns:
            PyArrow Table or None if Arrow not available
        """
        if self._arrow_store:
            return await self._arrow_store.arrow_scan_to_table(start_key, end_key, limit)
        return None

    async def arrow_execute_query(self, query_type: str, **params) -> Optional[object]:
        """
        Execute Arrow-native queries directly on stored data.

        Args:
            query_type: Type of query ('filter', 'aggregate', 'sort', 'project', 'join')
            **params: Query-specific parameters

        Returns:
            PyArrow Table result or None
        """
        if self._arrow_store:
            return await self._arrow_store.execute_arrow_query(query_type, params)
        return None

    async def arrow_filter(self, condition: object) -> Optional[object]:
        """
        Filter data using Arrow compute expressions.

        Args:
            condition: PyArrow compute expression

        Returns:
            Filtered PyArrow Table
        """
        return await self.arrow_execute_query("filter", condition=condition)

    async def arrow_aggregate(self, group_by: List[str],
                            aggregations: Dict[str, str]) -> Optional[object]:
        """
        Perform aggregation using Arrow compute.

        Args:
            group_by: Columns to group by
            aggregations: Aggregation functions by column

        Returns:
            Aggregated PyArrow Table
        """
        return await self.arrow_execute_query("aggregate",
                                            group_by=group_by,
                                            aggregations=aggregations)

    async def arrow_join(self, other_table: object,
                        join_keys: List[str] = None,
                        join_type: str = "inner") -> Optional[object]:
        """
        Join with another Arrow table.

        Args:
            other_table: PyArrow Table to join with
            join_keys: Keys to join on
            join_type: Type of join ('inner', 'left', 'right', 'outer', 'asof')

        Returns:
            Joined PyArrow Table
        """
        return await self.arrow_execute_query("join",
                                            other_table=other_table,
                                            join_keys=join_keys or ["key"],
                                            join_type=join_type)

    async def arrow_streaming_aggregate(self, aggregation_type: str,
                                      group_by_cols: List[str],
                                      **params) -> AsyncIterator[object]:
        """
        Perform streaming aggregation operations.

        Args:
            aggregation_type: Type of aggregation ('groupby_count', 'time_window', 'rolling')
            group_by_cols: Columns to group by
            **params: Additional parameters for the aggregation

        Yields:
            PyArrow Tables with aggregation results
        """
        if self._arrow_store:
            async for result in self._arrow_store.arrow_streaming_aggregate(
                aggregation_type, group_by_cols, params
            ):
                yield result

    async def arrow_export_dataset(self, output_path: str,
                                 partitioning: Dict = None) -> bool:
        """
        Export data as Arrow Dataset for external processing.

        Args:
            output_path: Path to export dataset to
            partitioning: Partitioning configuration

        Returns:
            True if successful, False otherwise
        """
        if self._arrow_store:
            return await self._arrow_store.arrow_export_to_dataset(output_path, partitioning)
        return False

    async def get_arrow_stats(self) -> Dict[str, Any]:
        """
        Get Arrow-specific performance statistics.

        Returns:
            Dictionary with Arrow performance metrics
        """
        if self._arrow_store:
            return await self._arrow_store.get_arrow_stats()

        return {
            'arrow_available': ARROW_TONBO_AVAILABLE,
            'tonbo_available': TONBO_AVAILABLE,
            'cython_available': CYTHON_TONBO_AVAILABLE,
            'arrow_integration': False
        }
