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
    Status, Key, Int64Key, TripleKey, KeyRange, Iterator,
    QueryResult, ColumnFamilyHandle, ColumnFamilyOptions,
    ColumnFamilyDescriptor, DBOptions, WriteOptions, ReadOptions,
    MarbleDB as CppMarbleDB
)
from pyarrow.includes.libarrow cimport (
    CSchema,
    CRecordBatch
)


class MarbleDBError(Exception):
    """Exception raised for MarbleDB errors."""
    pass


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
        self.opts.schema = c_schema

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
        """Get current value as Arrow RecordBatch."""
        if self.it.get() == NULL:
            raise MarbleDBError("Iterator not initialized")

        cdef shared_ptr[CRecordBatch] batch = deref(self.it).value()
        if batch.get() == NULL:
            return None
        return pyarrow_wrap_batch(batch)

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
            column_families: Optional list of (name, PyColumnFamilyOptions) tuples

        Returns:
            PyMarbleDB instance
        """
        cdef PyMarbleDB db = PyMarbleDB()
        cdef CppMarbleDB* db_ptr = NULL
        cdef vector[ColumnFamilyDescriptor] cf_descriptors
        cdef Status status

        # Prepare column family descriptors if provided
        if column_families:
            for name, cf_opts in column_families:
                cf_descriptors.push_back(
                    ColumnFamilyDescriptor(
                        name.encode('utf-8'),
                        deref((<PyColumnFamilyOptions>cf_opts).opts)
                    )
                )
            status = CppMarbleDB.Open(deref(options.opts), &cf_descriptors, &db_ptr)
        else:
            status = CppMarbleDB.Open(deref(options.opts), NULL, &db_ptr)

        if not status.ok():
            raise MarbleDBError(f"Failed to open database: {status.ToString().decode('utf-8')}")

        db.db = db_ptr
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
        cdef KeyRange key_range

        if start_key is None and end_key is None:
            key_range = KeyRange.All()
        else:
            # Create range with keys
            # Note: This is simplified - actual KeyRange constructor may differ
            key_range = KeyRange.All()

        cdef unique_ptr[Iterator] iterator
        cdef Status status = self.db.NewIterator(
            cf_name.encode('utf-8'),
            read_opts,
            key_range,
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

        cdef KeyRange full_range = KeyRange.All()
        cdef Status status = self.db.CompactRange(full_range)

        if not status.ok():
            raise MarbleDBError(f"Compaction failed: {status.ToString().decode('utf-8')}")

    def close(self):
        """Close the database."""
        if self.db == NULL:
            raise MarbleDBError("Database not opened")

        cdef Status status = self.db.Close()
        if not status.ok():
            raise MarbleDBError(f"Close failed: {status.ToString().decode('utf-8')}")

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
