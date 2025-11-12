# -*- coding: utf-8 -*-
"""Materialized views and dimension tables for Sabot."""

try:
    from sabot._cython.materializations.marbledb_dimension import MarbleDBDimensionTable
    MARBLEDB_DIMENSIONS_AVAILABLE = True
except ImportError:
    MARBLEDB_DIMENSIONS_AVAILABLE = False
    MarbleDBDimensionTable = None

__all__ = ['MarbleDBDimensionTable', 'MARBLEDB_DIMENSIONS_AVAILABLE']

