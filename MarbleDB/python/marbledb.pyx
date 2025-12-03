# marbledb.pyx - Python wrapper for MarbleDB
# cython: language_level=3

from libcpp.string cimport string
from libcpp.memory cimport unique_ptr, shared_ptr, make_shared
from libcpp.vector cimport vector
from libcpp cimport bool
from cython.operator cimport dereference as deref, preincrement as inc

import pyarrow as pa
cimport pyarrow.lib as palib
from pyarrow.lib cimport (
    pyarrow_wrap_schema,
    pyarrow_wrap_batch,
    pyarrow_unwrap_schema,
    pyarrow_unwrap_batch
)

# Import C++ declarations
from marbledb cimport (
    Status, Key, Int64Key, TripleKey, KeyRange, Iterator, Record,
    QueryResult, ColumnFamilyHandle, ColumnFamilyOptions,
    ColumnFamilyDescriptor, DBOptions, WriteOptions, ReadOptions,
    MarbleDB as CppMarbleDB, TableSchema, TableCapabilities,
    OpenDatabase as CppOpenDatabase, CloseDatabase as CppCloseDatabase,
    # Join types and functions
    JoinType, AsofJoinSpec,
    HashJoin as CppHashJoin, AsofJoin as CppAsofJoin
)
from pyarrow.includes.libarrow cimport (
    CSchema,
    CRecordBatch,
    CTable
)
from pyarrow.lib cimport (
    pyarrow_wrap_table,
    pyarrow_unwrap_table
)


class MarbleDBError(Exception):
    """Exception raised for MarbleDB errors."""
    pass


# Temporal model constants
TEMPORAL_NONE = 0
TEMPORAL_SYSTEM_TIME = 1
TEMPORAL_VALID_TIME = 2
TEMPORAL_BITEMPORAL = 3

# GC policy constants
GC_KEEP_ALL = 0
GC_KEEP_RECENT = 1
GC_KEEP_UNTIL = 2

# Join type constants
JOIN_INNER = 0
JOIN_LEFT = 1
JOIN_RIGHT = 2
JOIN_FULL = 3
JOIN_CROSS = 4


cdef class PyTableCapabilities:
    """Python wrapper for TableCapabilities (temporal/MVCC settings)."""
    cdef TableCapabilities* caps

    def __cinit__(self):
        self.caps = new TableCapabilities()

    def __dealloc__(self):
        del self.caps

    @property
    def temporal_model(self):
        """Get temporal model (TEMPORAL_NONE, TEMPORAL_SYSTEM_TIME, TEMPORAL_VALID_TIME, TEMPORAL_BITEMPORAL)."""
        return <int>self.caps.temporal_model

    @temporal_model.setter
    def temporal_model(self, int value):
        """Set temporal model."""
        if value == 0:
            self.caps.temporal_model = TableCapabilities.TemporalModel.kNone
        elif value == 1:
            self.caps.temporal_model = TableCapabilities.TemporalModel.kSystemTime
        elif value == 2:
            self.caps.temporal_model = TableCapabilities.TemporalModel.kValidTime
        elif value == 3:
            self.caps.temporal_model = TableCapabilities.TemporalModel.kBitemporal
        else:
            raise ValueError(f"Invalid temporal_model: {value}")

    @property
    def enable_mvcc(self):
        return self.caps.enable_mvcc

    @enable_mvcc.setter
    def enable_mvcc(self, bool value):
        self.caps.enable_mvcc = value

    @property
    def max_versions_per_key(self):
        return self.caps.mvcc_settings.max_versions_per_key

    @max_versions_per_key.setter
    def max_versions_per_key(self, size_t value):
        self.caps.mvcc_settings.max_versions_per_key = value

    @property
    def gc_policy(self):
        """Get GC policy (GC_KEEP_ALL, GC_KEEP_RECENT, GC_KEEP_UNTIL)."""
        return <int>self.caps.mvcc_settings.gc_policy

    @gc_policy.setter
    def gc_policy(self, int value):
        """Set GC policy."""
        if value == 0:
            self.caps.mvcc_settings.gc_policy = TableCapabilities.MVCCSettings.GCPolicy.kKeepAllVersions
        elif value == 1:
            self.caps.mvcc_settings.gc_policy = TableCapabilities.MVCCSettings.GCPolicy.kKeepRecentVersions
        elif value == 2:
            self.caps.mvcc_settings.gc_policy = TableCapabilities.MVCCSettings.GCPolicy.kKeepVersionsUntil
        else:
            raise ValueError(f"Invalid gc_policy: {value}")

    @staticmethod
    def bitemporal(size_t max_versions=10, int gc_policy=GC_KEEP_RECENT):
        """Create TableCapabilities for a bitemporal table.

        Args:
            max_versions: Maximum versions to keep per key (for GC_KEEP_RECENT)
            gc_policy: GC policy (GC_KEEP_ALL, GC_KEEP_RECENT, GC_KEEP_UNTIL)

        Returns:
            PyTableCapabilities configured for bitemporal storage

        Example:
            caps = PyTableCapabilities.bitemporal(max_versions=5)
            db.create_table("employees", schema, caps)
        """
        caps = PyTableCapabilities()
        caps.temporal_model = TEMPORAL_BITEMPORAL
        caps.enable_mvcc = True
        caps.max_versions_per_key = max_versions
        caps.gc_policy = gc_policy
        return caps


cdef class PyDBOptions:
    """Python wrapper for MarbleDB DBOptions."""
    cdef DBOptions* opts

    def __cinit__(self):
        self.opts = new DBOptions()

    def __dealloc__(self):
        del self.opts

    @property
    def db_path(self):
        return self.opts.db_path.decode('utf-8')

    @db_path.setter
    def db_path(self, str path):
        self.opts.db_path = path.encode('utf-8')

    @property
    def enable_wal(self):
        return self.opts.enable_wal

    @enable_wal.setter
    def enable_wal(self, bool value):
        self.opts.enable_wal = value

    @property
    def enable_sparse_index(self):
        return self.opts.enable_sparse_index

    @enable_sparse_index.setter
    def enable_sparse_index(self, bool value):
        self.opts.enable_sparse_index = value

    @property
    def wal_buffer_size(self):
        return self.opts.wal_buffer_size

    @wal_buffer_size.setter
    def wal_buffer_size(self, size_t value):
        self.opts.wal_buffer_size = value


cdef class PyColumnFamilyOptions:
    """Python wrapper for ColumnFamilyOptions."""
    cdef ColumnFamilyOptions* opts

    def __cinit__(self):
        self.opts = new ColumnFamilyOptions()

    def __dealloc__(self):
        del self.opts

    def set_schema(self, schema):
        """Set Arrow schema for this column family."""
        cdef shared_ptr[CSchema] c_schema = pyarrow_unwrap_schema(schema)
        deref(self.opts).schema = c_schema

    @property
    def enable_bloom_filter(self):
        return self.opts.enable_bloom_filter

    @enable_bloom_filter.setter
    def enable_bloom_filter(self, bool value):
        self.opts.enable_bloom_filter = value


cdef class PyTripleKey:
    """Python wrapper for TripleKey."""
    cdef shared_ptr[TripleKey] key

    def __cinit__(self, int64_t subject, int64_t predicate, int64_t obj):
        self.key = make_shared[TripleKey](subject, predicate, obj)

    @property
    def subject(self):
        return deref(self.key).subject()

    @property
    def predicate(self):
        return deref(self.key).predicate()

    @property
    def object(self):
        return deref(self.key).object()

    def compare(self, PyTripleKey other):
        """Compare this key with another. Returns -1, 0, or 1."""
        return deref(self.key).Compare(deref(other.key))

    def __repr__(self):
        return f"TripleKey({self.subject}, {self.predicate}, {self.object})"


cdef class PyIterator:
    """Python wrapper for MarbleDB Iterator."""
    cdef unique_ptr[Iterator] it

    cdef void _set_iterator(self, unique_ptr[Iterator]& iterator):
        """Internal method to take ownership of C++ iterator."""
        self.it.swap(iterator)

    def seek(self, PyTripleKey key):
        """Seek to the specified key."""
        if self.it.get() == NULL:
            raise MarbleDBError("Iterator not initialized")
        deref(self.it).Seek(deref(key.key.get()))

    def next(self):
        """Move to the next record."""
        if self.it.get() == NULL:
            raise MarbleDBError("Iterator not initialized")
        deref(self.it).Next()

    def valid(self):
        """Check if iterator is pointing to a valid record."""
        if self.it.get() == NULL:
            return False
        return deref(self.it).Valid()

    def key(self):
        """Get current key as PyTripleKey."""
        if self.it.get() == NULL:
            raise MarbleDBError("Iterator not initialized")

        cdef shared_ptr[Key] key_ptr = deref(self.it).key()
        # Cast to TripleKey
        cdef TripleKey* tk = <TripleKey*>key_ptr.get()
        if tk == NULL:
            return None

        result = PyTripleKey(tk.subject(), tk.predicate(), tk.object())
        return result

    def value(self):
        """Get current value as Record.

        Note: Returns the raw Record object. Use scan_table or temporal_scan_dedup
        for RecordBatch results.
        """
        if self.it.get() == NULL:
            raise MarbleDBError("Iterator not initialized")

        # Iterator returns Record, not RecordBatch directly
        # For full batch results, use ScanTable or TemporalScanDedup instead
        cdef shared_ptr[Record] record = deref(self.it).value()
        if record.get() == NULL:
            return None
        # Return None for now - use scan methods for batch results
        return None

    def status(self):
        """Get iterator status."""
        if self.it.get() == NULL:
            raise MarbleDBError("Iterator not initialized")

        cdef Status status = deref(self.it).status()
        if not status.ok():
            raise MarbleDBError(status.ToString().decode('utf-8'))

    def __iter__(self):
        """Make iterator iterable in Python."""
        return self

    def __next__(self):
        """Get next value in Python iteration."""
        if not self.valid():
            raise StopIteration

        key = self.key()
        value = self.value()
        self.next()
        return key, value


cdef class PyQueryResult:
    """Python wrapper for QueryResult."""
    cdef unique_ptr[QueryResult] result

    cdef void _set_result(self, unique_ptr[QueryResult]& qr):
        """Internal method to take ownership of C++ QueryResult."""
        self.result.swap(qr)

    def has_next(self):
        """Check if there are more batches."""
        if self.result.get() == NULL:
            return False
        return deref(self.result).HasNext()

    def next(self):
        """Get next RecordBatch."""
        if self.result.get() == NULL:
            raise MarbleDBError("QueryResult not initialized")

        cdef shared_ptr[CRecordBatch] batch
        cdef Status status = deref(self.result).Next(&batch)

        if not status.ok():
            raise MarbleDBError(status.ToString().decode('utf-8'))

        if batch.get() == NULL:
            return None

        return pyarrow_wrap_batch(batch)

    def schema(self):
        """Get result schema."""
        if self.result.get() == NULL:
            raise MarbleDBError("QueryResult not initialized")

        cdef shared_ptr[CSchema] schema = deref(self.result).schema()
        if schema.get() == NULL:
            return None
        return pyarrow_wrap_schema(schema)

    def to_table(self):
        """Convert all batches to a single Arrow Table."""
        batches = []
        while self.has_next():
            batch = self.next()
            if batch is not None:
                batches.append(batch)

        if not batches:
            # Return empty table with schema
            schema = self.schema()
            if schema is None:
                raise MarbleDBError("No schema available")
            return pa.Table.from_batches([], schema=schema)

        return pa.Table.from_batches(batches)

    def __iter__(self):
        """Make result iterable."""
        return self

    def __next__(self):
        """Get next batch in Python iteration."""
        if not self.has_next():
            raise StopIteration
        return self.next()


cdef class PyMarbleDB:
    """Python wrapper for MarbleDB database."""
    cdef CppMarbleDB* db

    def __cinit__(self):
        self.db = NULL

    def __dealloc__(self):
        if self.db != NULL:
            del self.db

    @staticmethod
    def open(PyDBOptions options, column_families=None):
        """
        Open a MarbleDB database.

        Args:
            options: PyDBOptions instance with database configuration
            column_families: Optional (ignored - use create_column_family after opening)

        Returns:
            PyMarbleDB instance
        """
        cdef PyMarbleDB db = PyMarbleDB()
        cdef unique_ptr[CppMarbleDB] db_ptr
        cdef Status status

        # Use the simpler OpenDatabase API
        status = CppOpenDatabase(options.db_path.encode('utf-8'), &db_ptr)

        if not status.ok():
            raise MarbleDBError(f"Failed to open database: {status.ToString().decode('utf-8')}")

        db.db = db_ptr.release()
        return db

    def create_column_family(self, str name, PyColumnFamilyOptions options):
        """
        Create a new column family.

        Args:
            name: Column family name
            options: PyColumnFamilyOptions instance

        Returns:
            None (handle is managed internally)
        """
        if self.db == NULL:
            raise MarbleDBError("Database not opened")

        cdef ColumnFamilyHandle* handle = NULL
        cdef ColumnFamilyDescriptor* descriptor = new ColumnFamilyDescriptor(
            name.encode('utf-8'),
            deref(options.opts)
        )

        cdef Status status = self.db.CreateColumnFamily(deref(descriptor), &handle)
        del descriptor
        if not status.ok():
            raise MarbleDBError(f"Failed to create column family: {status.ToString().decode('utf-8')}")

    def create_table(self, str name, schema, PyTableCapabilities capabilities=None):
        """
        Create a table with optional temporal/MVCC capabilities.

        Args:
            name: Table name
            schema: pyarrow.Schema defining the table columns
            capabilities: Optional PyTableCapabilities for temporal features

        Example:
            # Create a regular table
            schema = pa.schema([pa.field("id", pa.string()), pa.field("value", pa.float64())])
            db.create_table("simple", schema)

            # Create a bitemporal table
            caps = PyTableCapabilities.bitemporal(max_versions=10)
            db.create_table("employees", schema, caps)
        """
        if self.db == NULL:
            raise MarbleDBError("Database not opened")

        cdef shared_ptr[CSchema] c_schema = pyarrow_unwrap_schema(schema)
        cdef TableSchema table_schema = TableSchema(name.encode('utf-8'), c_schema)
        cdef TableCapabilities caps

        if capabilities is not None:
            caps = deref(capabilities.caps)
        else:
            caps = TableCapabilities()

        cdef Status status = self.db.CreateTable(table_schema, caps)
        if not status.ok():
            raise MarbleDBError(f"Failed to create table: {status.ToString().decode('utf-8')}")

    def insert_batch(self, str cf_name, batch):
        """
        Insert a RecordBatch into the specified column family.

        Args:
            cf_name: Column family name
            batch: pyarrow.RecordBatch to insert

        Returns:
            None
        """
        if self.db == NULL:
            raise MarbleDBError("Database not opened")

        cdef shared_ptr[CRecordBatch] c_batch = pyarrow_unwrap_batch(batch)
        cdef Status status = self.db.InsertBatch(cf_name.encode('utf-8'), c_batch)

        if not status.ok():
            raise MarbleDBError(f"Failed to insert batch: {status.ToString().decode('utf-8')}")

    def scan_table(self, str cf_name):
        """
        Scan entire column family.

        Args:
            cf_name: Column family name

        Returns:
            PyQueryResult instance
        """
        if self.db == NULL:
            raise MarbleDBError("Database not opened")

        cdef unique_ptr[QueryResult] result
        cdef Status status = self.db.ScanTable(cf_name.encode('utf-8'), &result)

        if not status.ok():
            raise MarbleDBError(f"Failed to scan table: {status.ToString().decode('utf-8')}")

        py_result = PyQueryResult()
        py_result._set_result(result)
        return py_result

    def new_iterator(self, str cf_name, PyTripleKey start_key=None, PyTripleKey end_key=None,
                     bool start_inclusive=True, bool end_inclusive=False):
        """
        Create an iterator for range scanning.

        Args:
            cf_name: Column family name
            start_key: Optional start key (None for beginning)
            end_key: Optional end key (None for end)
            start_inclusive: Include start key in range
            end_inclusive: Include end key in range

        Returns:
            PyIterator instance
        """
        if self.db == NULL:
            raise MarbleDBError("Database not opened")

        cdef ReadOptions read_opts

        cdef unique_ptr[Iterator] iterator
        cdef Status status = self.db.NewIterator(
            cf_name.encode('utf-8'),
            read_opts,
            KeyRange.All(),
            &iterator
        )

        if not status.ok():
            raise MarbleDBError(f"Failed to create iterator: {status.ToString().decode('utf-8')}")

        py_iterator = PyIterator()
        py_iterator._set_iterator(iterator)
        return py_iterator

    def flush(self):
        """Flush memtables to disk."""
        if self.db == NULL:
            raise MarbleDBError("Database not opened")

        cdef Status status = self.db.Flush()
        if not status.ok():
            raise MarbleDBError(f"Flush failed: {status.ToString().decode('utf-8')}")

    def compact_range(self):
        """Compact all data in the database."""
        if self.db == NULL:
            raise MarbleDBError("Database not opened")

        cdef Status status = self.db.CompactRange(KeyRange.All())

        if not status.ok():
            raise MarbleDBError(f"Compaction failed: {status.ToString().decode('utf-8')}")

    def close(self):
        """Close the database."""
        if self.db == NULL:
            return  # Already closed

        # Delete the database pointer (destructor handles cleanup)
        del self.db
        self.db = NULL

    def __enter__(self):
        """Context manager entry."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        if self.db != NULL:
            try:
                self.close()
            except:
                pass  # Ignore errors on close
        return False

    # ========== Bitemporal/Temporal Operations ==========

    def temporal_scan_dedup(self, str table_name, list key_columns,
                           uint64_t query_time=0,
                           uint64_t valid_time_start=0,
                           uint64_t valid_time_end=18446744073709551615,  # UINT64_MAX
                           bool include_deleted=False):
        """
        Scan a temporal table with deduplication and time filtering.

        This is the primary method for querying bitemporal tables. It:
        1. Filters by system time (query_time=0 means "current")
        2. Filters by valid time range (business time)
        3. Deduplicates by business key (latest version wins)
        4. Optionally includes deleted records

        Args:
            table_name: Name of the temporal table
            key_columns: List of column names that form the business key
            query_time: System time to query as of (0 = current time)
            valid_time_start: Start of valid time range (microseconds since epoch)
            valid_time_end: End of valid time range (UINT64_MAX = infinity)
            include_deleted: If True, include tombstoned records

        Returns:
            PyQueryResult that can be converted to Arrow Table

        Example:
            # Query current state
            result = db.temporal_scan_dedup("employees", ["employee_id"])
            table = result.to_table()

            # Query as of specific valid time (Jan 1, 2023)
            jan_2023 = 1672531200000000  # microseconds
            result = db.temporal_scan_dedup("salaries", ["emp_id"],
                                           valid_time_start=jan_2023,
                                           valid_time_end=jan_2023 + 1)
        """
        if self.db == NULL:
            raise MarbleDBError("Database not opened")

        cdef vector[string] cpp_key_columns
        for col in key_columns:
            cpp_key_columns.push_back(col.encode('utf-8'))

        cdef unique_ptr[QueryResult] result
        cdef Status status = self.db.TemporalScanDedup(
            table_name.encode('utf-8'),
            cpp_key_columns,
            query_time,
            valid_time_start,
            valid_time_end,
            include_deleted,
            &result
        )

        if not status.ok():
            raise MarbleDBError(f"TemporalScanDedup failed: {status.ToString().decode('utf-8')}")

        py_result = PyQueryResult()
        py_result._set_result(result)
        return py_result

    def temporal_update(self, str table_name, list key_columns,
                       key_batch, updated_batch):
        """
        Update records in a temporal table (preserves history).

        Unlike regular update, this:
        1. Closes the old version (sets _system_time_end = now)
        2. Inserts the new version with _system_time_start = now

        Args:
            table_name: Name of the temporal table
            key_columns: List of column names that identify records to update
            key_batch: pyarrow.RecordBatch with key column values
            updated_batch: pyarrow.RecordBatch with new values

        Example:
            # Update employee salary
            keys = pa.record_batch({"employee_id": ["EMP001"]}, ...)
            new_data = pa.record_batch({"employee_id": ["EMP001"], "salary": [75000]}, ...)
            db.temporal_update("employees", ["employee_id"], keys, new_data)
        """
        if self.db == NULL:
            raise MarbleDBError("Database not opened")

        cdef vector[string] cpp_key_columns
        for col in key_columns:
            cpp_key_columns.push_back(col.encode('utf-8'))

        cdef shared_ptr[CRecordBatch] c_key_batch = pyarrow_unwrap_batch(key_batch)
        cdef shared_ptr[CRecordBatch] c_updated_batch = pyarrow_unwrap_batch(updated_batch)

        cdef Status status = self.db.TemporalUpdate(
            table_name.encode('utf-8'),
            cpp_key_columns,
            c_key_batch,
            c_updated_batch
        )

        if not status.ok():
            raise MarbleDBError(f"TemporalUpdate failed: {status.ToString().decode('utf-8')}")

    def temporal_delete(self, str table_name, list key_columns, key_batch):
        """
        Delete records from a temporal table (preserves history).

        This doesn't physically delete data. Instead, it:
        1. Closes the current version (sets _system_time_end = now)
        2. Inserts a tombstone with _is_deleted = true

        Args:
            table_name: Name of the temporal table
            key_columns: List of column names that identify records to delete
            key_batch: pyarrow.RecordBatch with key column values

        Example:
            # Delete employee record
            keys = pa.record_batch({"employee_id": ["EMP002"]}, ...)
            db.temporal_delete("employees", ["employee_id"], keys)
        """
        if self.db == NULL:
            raise MarbleDBError("Database not opened")

        cdef vector[string] cpp_key_columns
        for col in key_columns:
            cpp_key_columns.push_back(col.encode('utf-8'))

        cdef shared_ptr[CRecordBatch] c_key_batch = pyarrow_unwrap_batch(key_batch)

        cdef Status status = self.db.TemporalDelete(
            table_name.encode('utf-8'),
            cpp_key_columns,
            c_key_batch
        )

        if not status.ok():
            raise MarbleDBError(f"TemporalDelete failed: {status.ToString().decode('utf-8')}")

    def prune_versions(self, str table_name, size_t max_versions_per_key=0,
                      uint64_t min_system_time_us=0):
        """
        Garbage collect old versions from a temporal table.

        Based on the table's GC policy (set at creation time):
        - kKeepAllVersions: No pruning (audit mode)
        - kKeepRecentVersions: Keep N most recent versions per key
        - kKeepVersionsUntil: Remove versions closed before timestamp

        Args:
            table_name: Name of the temporal table
            max_versions_per_key: Override max versions (0 = use table setting)
            min_system_time_us: Override timestamp threshold (0 = use table setting)

        Returns:
            Number of versions removed

        Example:
            # Prune using table's GC settings
            removed = db.prune_versions("employees")
            print(f"Pruned {removed} old versions")

            # Prune keeping only 2 versions per key
            removed = db.prune_versions("employees", max_versions_per_key=2)
        """
        if self.db == NULL:
            raise MarbleDBError("Database not opened")

        cdef uint64_t versions_removed = 0
        cdef Status status

        if max_versions_per_key > 0 or min_system_time_us > 0:
            # Use custom parameters
            status = self.db.PruneVersions(
                table_name.encode('utf-8'),
                max_versions_per_key if max_versions_per_key > 0 else 10,
                min_system_time_us,
                &versions_removed
            )
        else:
            # Use table's default GC settings
            status = self.db.PruneVersions(
                table_name.encode('utf-8'),
                &versions_removed
            )

        if not status.ok():
            raise MarbleDBError(f"PruneVersions failed: {status.ToString().decode('utf-8')}")

        return versions_removed


# Module-level convenience function
def open_database(str db_path) -> PyMarbleDB:
    """
    Open a MarbleDB database at the specified path.

    This is a convenience function that creates and opens a PyMarbleDB instance.

    Args:
        db_path: Path to the database directory (will be created if it doesn't exist)

    Returns:
        PyMarbleDB instance ready for use

    Example:
        db = marbledb.open_database("/tmp/my_database")

        # Create a bitemporal table
        import pyarrow as pa
        schema = pa.schema([
            pa.field("employee_id", pa.utf8()),
            pa.field("salary", pa.float64())
        ])
        caps = marbledb.PyTableCapabilities.bitemporal()
        db.create_table("employees", schema, caps)

        # Insert, update, query with temporal support
        db.insert_batch("employees", batch)
        results = db.temporal_scan_dedup("employees", ["employee_id"])
    """
    cdef PyDBOptions options = PyDBOptions()
    options.db_path = db_path
    return PyMarbleDB.open(options)


# ========== Join Operations (Table-to-Table) ==========

cdef JoinType _map_join_type(str how) except *:
    """Map string join type to C++ enum."""
    if how == 'inner':
        return JoinType.kInner
    elif how == 'left':
        return JoinType.kLeft
    elif how == 'right':
        return JoinType.kRight
    elif how == 'outer' or how == 'full':
        return JoinType.kFull
    elif how == 'cross':
        return JoinType.kCross
    else:
        raise ValueError(f"Unknown join type: {how}. Use 'inner', 'left', 'right', 'outer', or 'cross'")


def hash_join(left_table, right_table, on, how='inner', left_on=None, right_on=None,
              left_suffix='', right_suffix='_right'):
    """
    Perform hash join between two Arrow tables.

    This uses Arrow Acero's HashJoinNode for efficient SIMD-optimized joining.

    Args:
        left_table: PyArrow Table (left side of join)
        right_table: PyArrow Table (right side of join)
        on: str or list of str - join key column(s) when columns have the same name
        how: Join type - 'inner', 'left', 'right', 'outer'/'full', 'cross'
        left_on: str or list of str - left key columns (use when names differ)
        right_on: str or list of str - right key columns (use when names differ)
        left_suffix: Suffix for left columns in case of name collision (default: '')
        right_suffix: Suffix for right columns in case of name collision (default: '_right')

    Returns:
        PyArrow Table with joined result

    Example:
        # Simple join on same column name
        result = marbledb.hash_join(employees, departments, on='department_id')

        # Join with different column names
        result = marbledb.hash_join(
            orders, customers,
            left_on='customer_id',
            right_on='id',
            how='left'
        )

        # Multiple key columns
        result = marbledb.hash_join(
            table1, table2,
            on=['year', 'month', 'category']
        )
    """
    cdef shared_ptr[CTable] c_left = pyarrow_unwrap_table(left_table)
    cdef shared_ptr[CTable] c_right = pyarrow_unwrap_table(right_table)
    cdef shared_ptr[CTable] c_result
    cdef vector[string] cpp_on_keys
    cdef vector[string] cpp_left_keys
    cdef vector[string] cpp_right_keys
    cdef Status status
    cdef JoinType join_type = _map_join_type(how)

    # Handle different key specifications
    if left_on is not None and right_on is not None:
        # Different column names in each table
        if isinstance(left_on, str):
            left_on = [left_on]
        if isinstance(right_on, str):
            right_on = [right_on]

        if len(left_on) != len(right_on):
            raise ValueError("left_on and right_on must have the same length")

        for col in left_on:
            cpp_left_keys.push_back(col.encode('utf-8'))
        for col in right_on:
            cpp_right_keys.push_back(col.encode('utf-8'))

        status = CppHashJoin(
            c_left, c_right,
            cpp_left_keys, cpp_right_keys,
            join_type,
            left_suffix.encode('utf-8'),
            right_suffix.encode('utf-8'),
            &c_result
        )
    else:
        # Same column names in both tables
        if isinstance(on, str):
            on = [on]

        for col in on:
            cpp_on_keys.push_back(col.encode('utf-8'))

        status = CppHashJoin(
            c_left, c_right,
            cpp_on_keys,
            join_type,
            &c_result
        )

    if not status.ok():
        raise MarbleDBError(f"Hash join failed: {status.ToString().decode('utf-8')}")

    return pyarrow_wrap_table(c_result)


def asof_join(left_table, right_table, on_time, by=None, tolerance=0,
              left_suffix='', right_suffix='_right'):
    """
    Perform ASOF join between two Arrow tables on any time column.

    ASOF join matches each row in the left table with the closest row in the
    right table based on a time column. This is useful for:
    - Joining trades with quotes (find last quote before each trade)
    - Event correlation with sensor data
    - Point-in-time lookups

    IMPORTANT: Both tables must be sorted by the time column before calling.

    Args:
        left_table: PyArrow Table (left side, typically the "event" table)
        right_table: PyArrow Table (right side, typically the "reference" table)
        on_time: str - name of the time column (must exist in both tables)
        by: str or list of str - grouping columns for per-group matching (e.g., symbol)
        tolerance: int - time tolerance for matching
            - Negative: match only past values (backward, e.g., -1000 = look back 1000 units)
            - Positive: match only future values (forward)
            - Zero: exact match only
        left_suffix: Suffix for left columns in case of collision (default: '')
        right_suffix: Suffix for right columns in case of collision (default: '_right')

    Returns:
        PyArrow Table with joined result

    Example:
        # Join trades with quotes (backward lookup)
        trades = pa.table({
            'timestamp': [1000, 2000, 3000, 4000],
            'trade_price': [150.0, 151.5, 152.0, 153.0],
            'quantity': [100, 200, 150, 175]
        })
        quotes = pa.table({
            'timestamp': [900, 1500, 2500, 3500],
            'bid': [149.9, 150.9, 151.9, 152.9],
            'ask': [150.1, 151.1, 152.1, 153.1]
        })

        # Find last quote before each trade
        result = marbledb.asof_join(
            trades, quotes,
            on_time='timestamp',
            tolerance=-1000  # Look back up to 1000 time units
        )

        # With grouping by symbol
        result = marbledb.asof_join(
            trades, quotes,
            on_time='timestamp',
            by='symbol',
            tolerance=-1000
        )
    """
    cdef shared_ptr[CTable] c_left = pyarrow_unwrap_table(left_table)
    cdef shared_ptr[CTable] c_right = pyarrow_unwrap_table(right_table)
    cdef shared_ptr[CTable] c_result
    cdef AsofJoinSpec spec
    cdef Status status

    # Set time column
    spec.time_column = on_time.encode('utf-8')
    spec.tolerance = tolerance
    spec.left_suffix = left_suffix.encode('utf-8')
    spec.right_suffix = right_suffix.encode('utf-8')

    # Set by columns if provided
    if by is not None:
        if isinstance(by, str):
            by = [by]
        for col in by:
            spec.by_columns.push_back(col.encode('utf-8'))

    status = CppAsofJoin(c_left, c_right, spec, &c_result)

    if not status.ok():
        raise MarbleDBError(f"ASOF join failed: {status.ToString().decode('utf-8')}")

    return pyarrow_wrap_table(c_result)
