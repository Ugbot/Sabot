# cython: language_level=3
"""
Graph Storage Cython Header

Declarations for Arrow-native graph storage with C++ backend.
"""

from libc.stdint cimport int64_t
from libcpp cimport bool as cbool
from libcpp.memory cimport shared_ptr
from libcpp.string cimport string
from libcpp.vector cimport vector

cimport pyarrow.lib as ca


# C++ class declarations (external)
cdef extern from "graph_storage.h" namespace "sabot::graph":
    # CSRAdjacency
    cdef cppclass CSRAdjacency:
        CSRAdjacency(shared_ptr[ca.CInt64Array] indptr,
                     shared_ptr[ca.CInt64Array] indices,
                     shared_ptr[ca.CStructArray] edge_data) except +

        int64_t num_vertices() const
        int64_t num_edges() const
        int64_t GetDegree(int64_t v) const

        shared_ptr[ca.CInt64Array] indptr() const
        shared_ptr[ca.CInt64Array] indices() const
        shared_ptr[ca.CStructArray] edge_data() const

    # VertexTable
    cdef cppclass VertexTable:
        VertexTable(shared_ptr[ca.CTable] table) except +

        int64_t num_vertices() const
        shared_ptr[ca.CSchema] schema() const
        shared_ptr[ca.CTable] table() const

    # EdgeTable
    cdef cppclass EdgeTable:
        EdgeTable(shared_ptr[ca.CTable] table) except +

        int64_t num_edges() const
        shared_ptr[ca.CSchema] schema() const
        shared_ptr[ca.CTable] table() const

    # PropertyGraph
    cdef cppclass PropertyGraph:
        PropertyGraph(shared_ptr[VertexTable] vertices,
                     shared_ptr[EdgeTable] edges) except +

        int64_t num_vertices() const
        int64_t num_edges() const

        shared_ptr[VertexTable] vertices() const
        shared_ptr[EdgeTable] edges() const
        shared_ptr[CSRAdjacency] csr() const
        shared_ptr[CSRAdjacency] csc() const

    # RDFTripleStore
    cdef cppclass RDFTripleStore:
        RDFTripleStore(shared_ptr[ca.CTable] triples,
                      shared_ptr[ca.CTable] term_dict) except +

        int64_t num_triples() const
        int64_t num_terms() const

        shared_ptr[ca.CTable] triples() const
        shared_ptr[ca.CTable] term_dict() const


# Cython wrapper classes
cdef class PyCSRAdjacency:
    cdef shared_ptr[CSRAdjacency] c_csr
    cdef ca.Int64Array _indptr
    cdef ca.Int64Array _indices
    cdef ca.StructArray _edge_data

    cpdef int64_t num_vertices(self)
    cpdef int64_t num_edges(self)
    cpdef int64_t get_degree(self, int64_t v)
    cpdef ca.Int64Array get_neighbors(self, int64_t v)
    cpdef ca.StructArray get_edge_data(self, int64_t v)
    cpdef ca.RecordBatch get_neighbors_batch(self, ca.Int64Array vertices)


cdef class PyVertexTable:
    cdef shared_ptr[VertexTable] c_table
    cdef ca.Table _table

    cpdef int64_t num_vertices(self)
    cpdef ca.Schema schema(self)
    cpdef ca.Table table(self)
    cpdef ca.Int64Array get_ids(self)
    cpdef ca.DictionaryArray get_labels(self)
    cpdef PyVertexTable filter_by_label(self, str label)
    cpdef PyVertexTable select(self, list columns)
    cpdef ca.ChunkedArray get_column(self, str name)


cdef class PyEdgeTable:
    cdef shared_ptr[EdgeTable] c_table
    cdef ca.Table _table

    cpdef int64_t num_edges(self)
    cpdef ca.Schema schema(self)
    cpdef ca.Table table(self)
    cpdef ca.Int64Array get_sources(self)
    cpdef ca.Int64Array get_destinations(self)
    cpdef ca.DictionaryArray get_types(self)
    cpdef PyEdgeTable filter_by_type(self, str edge_type)
    cpdef PyEdgeTable select(self, list columns)
    cpdef ca.ChunkedArray get_column(self, str name)
    cpdef PyCSRAdjacency build_csr(self, int64_t num_vertices)
    cpdef PyCSRAdjacency build_csc(self, int64_t num_vertices)


cdef class PyPropertyGraph:
    cdef shared_ptr[PropertyGraph] c_graph
    cdef PyVertexTable _vertices
    cdef PyEdgeTable _edges
    cdef PyCSRAdjacency _csr
    cdef PyCSRAdjacency _csc

    cpdef int64_t num_vertices(self)
    cpdef int64_t num_edges(self)
    cpdef PyVertexTable vertices(self)
    cpdef PyEdgeTable edges(self)
    cpdef PyCSRAdjacency csr(self)
    cpdef PyCSRAdjacency csc(self)
    cpdef void build_csr(self)
    cpdef void build_csc(self)
    cpdef ca.Int64Array get_neighbors(self, int64_t v)
    cpdef ca.Int64Array get_in_neighbors(self, int64_t v)
    cpdef ca.RecordBatch match_pattern(self, str src_label, str edge_type, str dst_label)
    cpdef void add_vertices_from_table(self, ca.Table new_vertices)
    cpdef void add_edges_from_table(self, ca.Table new_edges)


cdef class PyRDFTripleStore:
    cdef shared_ptr[RDFTripleStore] c_store
    cdef ca.Table _triples
    cdef ca.Table _term_dict

    cpdef int64_t num_triples(self)
    cpdef int64_t num_terms(self)
    cpdef ca.Table triples(self)
    cpdef ca.Table term_dict(self)
    cpdef int64_t lookup_term_id(self, str lex)
    cpdef str lookup_term(self, int64_t term_id)
    cpdef ca.Table filter_triples(self, int64_t s, int64_t p, int64_t o)
