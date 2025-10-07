# -*- coding: utf-8 -*-
"""
CyRocks Header File

Cython declarations for cyrocks module. Keep types opaque to avoid
exposing RocksDB C++ headers to importing modules.
"""

# Declare classes without internal details to avoid header dependency propagation
cdef class CyOptions:
    pass

cdef class CyDB:
    pass
