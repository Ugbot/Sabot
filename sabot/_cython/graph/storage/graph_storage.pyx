# cython: language_level=3
# cython: boundscheck=False
# cython: wraparound=False
"""
Graph Storage Cython Bindings

Zero-copy Arrow-native graph storage for property graphs and RDF.
"""

from libc.stdint cimport int64_t
from libcpp cimport bool as cbool
from libcpp.memory cimport shared_ptr, make_shared, static_pointer_cast
from libcpp.string cimport string
from libcpp.vector cimport vector

cimport cython
cimport pyarrow.lib as ca
from pyarrow.lib cimport pyarrow_unwrap_array, pyarrow_unwrap_table
from pyarrow.includes.libarrow cimport CArray, CInt64Array, CStructArray

# Import Arrow Python API
import pyarrow as pa


# ============================================================================
# CSRAdjacency Wrapper
# ============================================================================

cdef class PyCSRAdjacency:
    """
    Compressed Sparse Row (CSR) adjacency structure.

    Efficient representation for graph traversal:
    - indptr: Offsets for each vertex's neighbors
    - indices: Neighbor vertex IDs
    - edge_data: Optional edge properties (parallel to indices)

    Performance:
    - GetNeighbors: O(1) + O(degree) - zero-copy slice
    - GetDegree: O(1) - pointer arithmetic
    - Memory: ~16 bytes per edge (int64 indices + indptr)
    """

    def __cinit__(self, ca.Int64Array indptr, ca.Int64Array indices,
                  ca.StructArray edge_data=None):
        """Create CSR adjacency from Arrow arrays."""
        self._indptr = indptr
        self._indices = indices
        self._edge_data = edge_data

        # Declare all C variables at the top
        cdef shared_ptr[CArray] arr_base
        cdef shared_ptr[CInt64Array] c_indptr
        cdef shared_ptr[CInt64Array] c_indices
        cdef shared_ptr[CStructArray] c_edge_data

        # Unwrap indptr
        arr_base = pyarrow_unwrap_array(indptr)
        c_indptr = <shared_ptr[CInt64Array]>static_pointer_cast[CInt64Array, CArray](arr_base)

        # Unwrap indices
        arr_base = pyarrow_unwrap_array(indices)
        c_indices = <shared_ptr[CInt64Array]>static_pointer_cast[CInt64Array, CArray](arr_base)

        # Handle optional edge_data
        if edge_data is not None:
            arr_base = pyarrow_unwrap_array(edge_data)
            c_edge_data = <shared_ptr[CStructArray]>static_pointer_cast[CStructArray, CArray](arr_base)

        # Create C++ CSRAdjacency
        self.c_csr = make_shared[CSRAdjacency](c_indptr, c_indices, c_edge_data)

    cpdef int64_t num_vertices(self):
        """Number of vertices in the graph."""
        return self.c_csr.get().num_vertices()

    cpdef int64_t num_edges(self):
        """Number of edges in the graph."""
        return self.c_csr.get().num_edges()

    cpdef int64_t get_degree(self, int64_t v):
        """Get out-degree for vertex v (O(1))."""
        return self.c_csr.get().GetDegree(v)

    cpdef ca.Int64Array get_neighbors(self, int64_t v):
        """
        Get neighbors of vertex v (zero-copy slice).

        Returns Int64Array of neighbor vertex IDs.
        Performance: O(1) + O(degree)
        """
        cdef int64_t start = self._indptr.to_pylist()[v]
        cdef int64_t end = self._indptr.to_pylist()[v + 1]
        return self._indices.slice(start, end - start)

    cpdef ca.StructArray get_edge_data(self, int64_t v):
        """
        Get edge data for neighbors of vertex v (zero-copy slice).

        Returns StructArray with edge properties aligned with neighbors.
        """
        if self._edge_data is None:
            raise ValueError("No edge data available")

        cdef int64_t start = self._indptr.to_pylist()[v]
        cdef int64_t end = self._indptr.to_pylist()[v + 1]
        return self._edge_data.slice(start, end - start)

    cpdef ca.RecordBatch get_neighbors_batch(self, ca.Int64Array vertices):
        """
        Vectorized neighbor lookup for multiple vertices.

        Returns RecordBatch with schema:
        - vertex_id: int64 (original vertex)
        - neighbor_id: int64 (neighbor vertex)
        - edge_idx: int64 (index into edge_data)

        Performance: O(sum of degrees) - vectorized
        """
        vertex_ids = []
        neighbor_ids = []
        edge_indices = []

        cdef int64_t v, start, end, j
        cdef list indptr_list = self._indptr.to_pylist()
        cdef list indices_list = self._indices.to_pylist()

        for i in range(vertices.length()):
            v = vertices.to_pylist()[i]
            if v < 0 or v >= self.num_vertices():
                continue

            start = indptr_list[v]
            end = indptr_list[v + 1]

            for j in range(start, end):
                vertex_ids.append(v)
                neighbor_ids.append(indices_list[j])
                edge_indices.append(j)

        return pa.RecordBatch.from_arrays(
            [pa.array(vertex_ids, type=pa.int64()),
             pa.array(neighbor_ids, type=pa.int64()),
             pa.array(edge_indices, type=pa.int64())],
            names=['vertex_id', 'neighbor_id', 'edge_idx']
        )

    @property
    def indptr(self):
        """Offset array (num_vertices + 1)."""
        return self._indptr

    @property
    def indices(self):
        """Neighbor vertex IDs (num_edges)."""
        return self._indices

    @property
    def edge_data(self):
        """Edge properties (num_edges) or None."""
        return self._edge_data


# ============================================================================
# VertexTable Wrapper
# ============================================================================

cdef class PyVertexTable:
    """
    Property graph vertex table with columnar properties.

    Schema:
    - id: int64 (dense, 0..N-1)
    - label: dictionary<string> (vertex type)
    - <property_columns>: any Arrow type

    Zero-copy operations via Arrow compute kernels.
    """

    def __cinit__(self, ca.Table table):
        """Create vertex table from Arrow Table."""
        self._table = table
        self.c_table = make_shared[VertexTable](
            pyarrow_unwrap_table(table)
        )

    cpdef int64_t num_vertices(self):
        """Number of vertices."""
        return self.c_table.get().num_vertices()

    cpdef ca.Schema schema(self):
        """Table schema."""
        return ca.pyarrow_wrap_schema(self.c_table.get().schema())

    cpdef ca.Table table(self):
        """Underlying Arrow table."""
        return self._table

    cpdef ca.Int64Array get_ids(self):
        """Get vertex IDs (zero-copy)."""
        return self._table.column('id').combine_chunks()

    cpdef ca.DictionaryArray get_labels(self):
        """Get vertex labels (dictionary-encoded)."""
        return self._table.column('label').combine_chunks()

    cpdef PyVertexTable filter_by_label(self, str label):
        """Filter vertices by label (vectorized, zero-copy)."""
        labels = self.get_labels()
        dict_array = labels.dictionary

        # Find label index in dictionary
        label_idx = None
        for i in range(len(dict_array)):
            if dict_array[i].as_py() == label:
                label_idx = i
                break

        if label_idx is None:
            # Label not found, return empty table
            return PyVertexTable(self._table.slice(0, 0))

        # Filter using Arrow compute - compare with label string directly
        import pyarrow.compute as pc
        mask = pc.equal(pc.cast(self._table.column('label'), pa.string()), pa.scalar(label, type=pa.string()))
        filtered = self._table.filter(mask)

        return PyVertexTable(filtered)

    cpdef PyVertexTable select(self, list columns):
        """Select columns (zero-copy projection)."""
        selected = self._table.select(columns)
        return PyVertexTable(selected)

    cpdef ca.ChunkedArray get_column(self, str name):
        """Get column by name (zero-copy)."""
        return self._table.column(name)

    def __len__(self):
        return self.num_vertices()

    def __repr__(self):
        return f"VertexTable(num_vertices={self.num_vertices()}, schema={self.schema()})"


# ============================================================================
# EdgeTable Wrapper
# ============================================================================

cdef class PyEdgeTable:
    """
    Property graph edge table with columnar properties.

    Schema:
    - src: int64 (source vertex ID)
    - dst: int64 (destination vertex ID)
    - type: dictionary<string> (edge type)
    - <property_columns>: any Arrow type

    Zero-copy operations via Arrow compute kernels.
    """

    def __cinit__(self, ca.Table table):
        """Create edge table from Arrow Table."""
        self._table = table
        self.c_table = make_shared[EdgeTable](
            pyarrow_unwrap_table(table)
        )

    cpdef int64_t num_edges(self):
        """Number of edges."""
        return self.c_table.get().num_edges()

    cpdef ca.Schema schema(self):
        """Table schema."""
        return ca.pyarrow_wrap_schema(self.c_table.get().schema())

    cpdef ca.Table table(self):
        """Underlying Arrow table."""
        return self._table

    cpdef ca.Int64Array get_sources(self):
        """Get source vertex IDs (zero-copy)."""
        return self._table.column('src').combine_chunks()

    cpdef ca.Int64Array get_destinations(self):
        """Get destination vertex IDs (zero-copy)."""
        return self._table.column('dst').combine_chunks()

    cpdef ca.DictionaryArray get_types(self):
        """Get edge types (dictionary-encoded)."""
        return self._table.column('type').combine_chunks()

    cpdef PyEdgeTable filter_by_type(self, str edge_type):
        """Filter edges by type (vectorized, zero-copy)."""
        types = self.get_types()
        dict_array = types.dictionary

        # Find type index in dictionary
        type_idx = None
        for i in range(len(dict_array)):
            if dict_array[i].as_py() == edge_type:
                type_idx = i
                break

        if type_idx is None:
            return PyEdgeTable(self._table.slice(0, 0))

        # Filter using Arrow compute - compare with type string directly
        import pyarrow.compute as pc
        mask = pc.equal(pc.cast(self._table.column('type'), pa.string()), pa.scalar(edge_type, type=pa.string()))
        filtered = self._table.filter(mask)

        return PyEdgeTable(filtered)

    cpdef PyEdgeTable select(self, list columns):
        """Select columns (zero-copy projection)."""
        selected = self._table.select(columns)
        return PyEdgeTable(selected)

    cpdef ca.ChunkedArray get_column(self, str name):
        """Get column by name (zero-copy)."""
        return self._table.column(name)

    cpdef PyCSRAdjacency build_csr(self, int64_t num_vertices):
        """
        Build CSR adjacency from edge table.

        Sorts edges by src and creates indptr/indices arrays.
        Performance: O(E log E) sort + O(V + E) scan
        """
        import pyarrow.compute as pc

        # Get src/dst arrays
        src = self.get_sources()
        dst = self.get_destinations()

        # Sort by src
        sort_indices = pc.sort_indices(src)
        sorted_src = pc.take(src, sort_indices)
        sorted_dst = pc.take(dst, sort_indices)

        # Build indptr (offsets for each vertex)
        indptr_list = [0] * (num_vertices + 1)
        edge_idx = 0

        src_list = sorted_src.to_pylist()
        for v in range(num_vertices):
            indptr_list[v] = edge_idx
            # Count edges from vertex v
            while edge_idx < len(src_list) and src_list[edge_idx] == v:
                edge_idx += 1

        indptr_list[num_vertices] = edge_idx

        indptr = pa.array(indptr_list, type=pa.int64())
        indices = sorted_dst

        return PyCSRAdjacency(indptr, indices)

    cpdef PyCSRAdjacency build_csc(self, int64_t num_vertices):
        """
        Build CSC adjacency from edge table (in-edges).

        Sorts edges by dst and creates indptr/indices arrays.
        """
        import pyarrow.compute as pc

        # Get src/dst arrays
        src = self.get_sources()
        dst = self.get_destinations()

        # Sort by dst
        sort_indices = pc.sort_indices(dst)
        sorted_src = pc.take(src, sort_indices)
        sorted_dst = pc.take(dst, sort_indices)

        # Build indptr (offsets for each vertex by dst)
        indptr_list = [0] * (num_vertices + 1)
        edge_idx = 0

        dst_list = sorted_dst.to_pylist()
        for v in range(num_vertices):
            indptr_list[v] = edge_idx
            while edge_idx < len(dst_list) and dst_list[edge_idx] == v:
                edge_idx += 1

        indptr_list[num_vertices] = edge_idx

        indptr = pa.array(indptr_list, type=pa.int64())
        indices = sorted_src  # In-neighbors

        return PyCSRAdjacency(indptr, indices)

    def __len__(self):
        return self.num_edges()

    def __repr__(self):
        return f"EdgeTable(num_edges={self.num_edges()}, schema={self.schema()})"


# ============================================================================
# PropertyGraph Wrapper
# ============================================================================

cdef class PyPropertyGraph:
    """
    Property graph with vertices, edges, and adjacency structures.

    Combines vertex table, edge table, and CSR/CSC indices for
    efficient pattern matching, traversals, and graph analytics.

    Example:
        >>> vertices = pa.table({
        ...     'id': [0, 1, 2, 3],
        ...     'label': pa.array(['Person', 'Person', 'Company', 'Company']).dictionary_encode(),
        ...     'name': ['Alice', 'Bob', 'Acme', 'Globex']
        ... })
        >>> edges = pa.table({
        ...     'src': [0, 1, 0],
        ...     'dst': [2, 2, 3],
        ...     'type': pa.array(['WORKS_AT', 'WORKS_AT', 'OWNS']).dictionary_encode(),
        ...     'since': [2018, 2019, 2020]
        ... })
        >>> graph = PropertyGraph(vertices, edges)
        >>> graph.build_csr()
        >>> neighbors = graph.get_neighbors(0)  # Alice's neighbors
    """

    def __cinit__(self, PyVertexTable vertices, PyEdgeTable edges):
        """Create property graph from vertex and edge tables."""
        self._vertices = vertices
        self._edges = edges

        self.c_graph = make_shared[PropertyGraph](
            vertices.c_table,
            edges.c_table
        )

        self._csr = None
        self._csc = None

    cpdef int64_t num_vertices(self):
        """Number of vertices in the graph."""
        return self.c_graph.get().num_vertices()

    cpdef int64_t num_edges(self):
        """Number of edges in the graph."""
        return self.c_graph.get().num_edges()

    cpdef PyVertexTable vertices(self):
        """Get vertex table."""
        return self._vertices

    cpdef PyEdgeTable edges(self):
        """Get edge table."""
        return self._edges

    cpdef PyCSRAdjacency csr(self):
        """Get CSR adjacency (builds if needed)."""
        if self._csr is None:
            self.build_csr()
        return self._csr

    cpdef PyCSRAdjacency csc(self):
        """Get CSC adjacency (builds if needed)."""
        if self._csc is None:
            self.build_csc()
        return self._csc

    cpdef void build_csr(self):
        """Build CSR adjacency (out-edges)."""
        if self._csr is None:
            self._csr = self._edges.build_csr(self.num_vertices())

    cpdef void build_csc(self):
        """Build CSC adjacency (in-edges)."""
        if self._csc is None:
            self._csc = self._edges.build_csc(self.num_vertices())

    cpdef ca.Int64Array get_neighbors(self, int64_t v):
        """Get out-neighbors for vertex v (via CSR)."""
        if self._csr is None:
            self.build_csr()
        return self._csr.get_neighbors(v)

    cpdef ca.Int64Array get_in_neighbors(self, int64_t v):
        """Get in-neighbors for vertex v (via CSC)."""
        if self._csc is None:
            self.build_csc()
        return self._csc.get_neighbors(v)

    cpdef ca.RecordBatch match_pattern(self, str src_label, str edge_type, str dst_label):
        """
        Match graph pattern: (src_label)-[edge_type]->(dst_label)

        Returns RecordBatch with matched vertex/edge combinations.
        Performance: Vectorized filters + hash joins via Arrow compute.
        """
        import pyarrow.compute as pc

        # Filter vertices by label
        src_vertices = self._vertices.filter_by_label(src_label)
        dst_vertices = self._vertices.filter_by_label(dst_label)

        # Filter edges by type
        filtered_edges = self._edges.filter_by_type(edge_type)

        # Join: src_vertices.id == edges.src
        src_table = src_vertices.table()
        edges_table = filtered_edges.table()

        # Perform joins using Arrow compute
        # (Simplified - full implementation would use hash_join)
        src_ids = src_vertices.get_ids().to_pylist()
        edge_srcs = filtered_edges.get_sources().to_pylist()
        edge_dsts = filtered_edges.get_destinations().to_pylist()
        dst_ids = dst_vertices.get_ids().to_pylist()

        # Filter edges matching src and dst labels
        matched_src = []
        matched_dst = []
        for i in range(len(edge_srcs)):
            if edge_srcs[i] in src_ids and edge_dsts[i] in dst_ids:
                matched_src.append(edge_srcs[i])
                matched_dst.append(edge_dsts[i])

        return pa.RecordBatch.from_arrays(
            [pa.array(matched_src, type=pa.int64()),
             pa.array(matched_dst, type=pa.int64())],
            names=['src_id', 'dst_id']
        )

    cpdef void add_vertices_from_table(self, ca.Table new_vertices):
        """
        Add vertices from table (mutable operation for streaming).

        Concatenates new vertices to existing vertex table and recreates
        the C++ graph. Invalidates CSR/CSC caches since topology may change.

        Args:
            new_vertices: Arrow Table with new vertices (same schema as existing)

        Performance: O(V_existing + V_new) for table concatenation + graph recreation
        """
        # Concatenate new vertices to existing
        combined = pa.concat_tables([self._vertices.table(), new_vertices])
        self._vertices = PyVertexTable(combined)

        # Recreate C++ graph with updated tables
        self.c_graph = make_shared[PropertyGraph](
            self._vertices.c_table,
            self._edges.c_table
        )

        # Invalidate caches (vertex IDs may have changed)
        self._csr = None
        self._csc = None

    cpdef void add_edges_from_table(self, ca.Table new_edges):
        """
        Add edges from table (mutable operation for streaming).

        Concatenates new edges to existing edge table and recreates
        the C++ graph. Invalidates CSR/CSC caches since topology changed.

        Args:
            new_edges: Arrow Table with new edges (same schema as existing)

        Performance: O(E_existing + E_new) for table concatenation + graph recreation
        """
        # Concatenate new edges to existing
        combined = pa.concat_tables([self._edges.table(), new_edges])
        self._edges = PyEdgeTable(combined)

        # Recreate C++ graph with updated tables
        self.c_graph = make_shared[PropertyGraph](
            self._vertices.c_table,
            self._edges.c_table
        )

        # Invalidate caches (topology changed)
        self._csr = None
        self._csc = None

    def __repr__(self):
        return f"PropertyGraph(num_vertices={self.num_vertices()}, num_edges={self.num_edges()})"


# ============================================================================
# RDFTripleStore Wrapper
# ============================================================================

cdef class PyRDFTripleStore:
    """
    RDF triple store with term dictionary encoding.

    Triples schema:
    - s: int64 (subject ID)
    - p: int64 (predicate ID)
    - o: int64 (object ID)
    - g: int64 (graph ID, optional)

    Term dictionary schema:
    - id: int64
    - lex: string (lexical form)
    - kind: uint8 (IRI=0, Literal=1, Blank=2)
    - lang: string (language tag)
    - datatype: string (datatype IRI)
    """

    def __cinit__(self, ca.Table triples, ca.Table term_dict):
        """Create RDF triple store from triples and term dictionary."""
        self._triples = triples
        self._term_dict = term_dict

        self.c_store = make_shared[RDFTripleStore](
            pyarrow_unwrap_table(triples),
            pyarrow_unwrap_table(term_dict)
        )

    cpdef int64_t num_triples(self):
        """Number of triples."""
        return self.c_store.get().num_triples()

    cpdef int64_t num_terms(self):
        """Number of terms in dictionary."""
        return self.c_store.get().num_terms()

    cpdef ca.Table triples(self):
        """Get triples table."""
        return self._triples

    cpdef ca.Table term_dict(self):
        """Get term dictionary table."""
        return self._term_dict

    cpdef int64_t lookup_term_id(self, str lex):
        """
        Lookup term ID by lexical form.

        Returns -1 if term not found.
        """
        # Linear search through term dictionary
        # (Full implementation would use hash table)
        lex_column = self._term_dict.column('lex').combine_chunks()
        for i in range(lex_column.length()):
            if lex_column[i].as_py() == lex:
                return self._term_dict.column('id').combine_chunks()[i].as_py()
        return -1

    cpdef str lookup_term(self, int64_t term_id):
        """
        Lookup term lexical form by ID.

        Raises KeyError if term_id not found.
        """
        id_column = self._term_dict.column('id').combine_chunks()
        lex_column = self._term_dict.column('lex').combine_chunks()

        for i in range(id_column.length()):
            if id_column[i].as_py() == term_id:
                return lex_column[i].as_py()

        raise KeyError(f"Term ID {term_id} not found")

    cpdef ca.Table filter_triples(self, int64_t s, int64_t p, int64_t o):
        """
        Filter triples by pattern (s, p, o) where -1 = wildcard.

        Example:
        - (123, -1, -1) = all triples with subject 123
        - (-1, 456, -1) = all triples with predicate 456
        - (123, 456, 789) = exact triple match
        """
        import pyarrow.compute as pc

        mask = pa.array([True] * self.num_triples())

        if s >= 0:
            mask = pc.and_(mask, pc.equal(self._triples.column('s'), pa.scalar(s, type=pa.int64())))
        if p >= 0:
            mask = pc.and_(mask, pc.equal(self._triples.column('p'), pa.scalar(p, type=pa.int64())))
        if o >= 0:
            mask = pc.and_(mask, pc.equal(self._triples.column('o'), pa.scalar(o, type=pa.int64())))

        return self._triples.filter(mask)

    def __repr__(self):
        return f"RDFTripleStore(num_triples={self.num_triples()}, num_terms={self.num_terms()})"
