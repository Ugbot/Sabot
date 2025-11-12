# -*- coding: utf-8 -*-
"""Cython declarations for MarbleDBStoreBackend."""

from libcpp.memory cimport unique_ptr
from libcpp.string cimport string
from libcpp.unordered_map cimport unordered_map
from libcpp cimport bool as cbool

cdef extern from "marble/db.h" namespace "marble":
    cdef cppclass MarbleDB:
        pass
    cdef cppclass ColumnFamilyHandle:
        pass

cdef class MarbleDBStoreBackend:
    cdef unique_ptr[MarbleDB] _db
    cdef str _db_path
    cdef cbool _is_open
    cdef unordered_map[string, ColumnFamilyHandle*] _column_families
    
    cpdef void open(self)
    cpdef void close(self)
    cpdef void create_column_family(self, str table_name, object pyarrow_schema)
    cpdef void insert_batch(self, str table_name, object pyarrow_batch)
    cpdef object scan_table(self, str table_name)
    cpdef object scan_range_to_table(self, str table_name, str start_key=*, str end_key=*)
    cpdef void delete_range(self, str table_name, str start_key, str end_key)
    cpdef list list_column_families(self)
    cpdef void flush(self)

