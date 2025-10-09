"""
Unit Tests for Graph Storage Module

Tests CSRAdjacency, VertexTable, EdgeTable, and PropertyGraph classes.
"""

import pytest
import pyarrow as pa


# Skip all tests if graph module not built
pytestmark = pytest.mark.skipif(
    True,  # Will be False once module is built
    reason="Graph storage module not yet built"
)


class TestCSRAdjacency:
    """Test CSR adjacency structure."""

    def test_create_csr(self):
        """Test CSR creation from Arrow arrays."""
        from sabot._cython.graph import CSRAdjacency

        # Simple graph: 0 -> 1, 2; 1 -> 2; 2 -> (no neighbors)
        indptr = pa.array([0, 2, 3, 3], type=pa.int64())
        indices = pa.array([1, 2, 2], type=pa.int64())

        csr = CSRAdjacency(indptr, indices)

        assert csr.num_vertices() == 3
        assert csr.num_edges() == 3

    def test_get_neighbors(self):
        """Test neighbor retrieval."""
        from sabot._cython.graph import CSRAdjacency

        indptr = pa.array([0, 2, 3, 3], type=pa.int64())
        indices = pa.array([1, 2, 2], type=pa.int64())
        csr = CSRAdjacency(indptr, indices)

        # Vertex 0 neighbors: [1, 2]
        neighbors = csr.get_neighbors(0)
        assert neighbors.to_pylist() == [1, 2]

        # Vertex 1 neighbors: [2]
        neighbors = csr.get_neighbors(1)
        assert neighbors.to_pylist() == [2]

        # Vertex 2 neighbors: []
        neighbors = csr.get_neighbors(2)
        assert neighbors.to_pylist() == []

    def test_get_degree(self):
        """Test degree computation."""
        from sabot._cython.graph import CSRAdjacency

        indptr = pa.array([0, 2, 3, 3], type=pa.int64())
        indices = pa.array([1, 2, 2], type=pa.int64())
        csr = CSRAdjacency(indptr, indices)

        assert csr.get_degree(0) == 2
        assert csr.get_degree(1) == 1
        assert csr.get_degree(2) == 0

    def test_get_neighbors_batch(self):
        """Test vectorized neighbor lookup."""
        from sabot._cython.graph import CSRAdjacency

        indptr = pa.array([0, 2, 3, 3], type=pa.int64())
        indices = pa.array([1, 2, 2], type=pa.int64())
        csr = CSRAdjacency(indptr, indices)

        vertices = pa.array([0, 1], type=pa.int64())
        batch = csr.get_neighbors_batch(vertices)

        # Expected: (0, 1), (0, 2), (1, 2)
        assert batch.num_rows == 3
        assert batch.column('vertex_id').to_pylist() == [0, 0, 1]
        assert batch.column('neighbor_id').to_pylist() == [1, 2, 2]


class TestVertexTable:
    """Test vertex table operations."""

    def test_create_vertex_table(self):
        """Test vertex table creation."""
        from sabot._cython.graph import VertexTable

        vertices = pa.table({
            'id': pa.array([0, 1, 2, 3], type=pa.int64()),
            'label': pa.array(['Person', 'Person', 'Company', 'Company']).dictionary_encode(),
            'name': ['Alice', 'Bob', 'Acme', 'Globex']
        })

        vt = VertexTable(vertices)

        assert vt.num_vertices() == 4
        assert 'id' in vt.schema().names
        assert 'label' in vt.schema().names

    def test_get_ids(self):
        """Test ID retrieval."""
        from sabot._cython.graph import VertexTable

        vertices = pa.table({
            'id': pa.array([0, 1, 2, 3], type=pa.int64()),
            'label': pa.array(['Person', 'Person', 'Company', 'Company']).dictionary_encode(),
            'name': ['Alice', 'Bob', 'Acme', 'Globex']
        })

        vt = VertexTable(vertices)
        ids = vt.get_ids()

        assert ids.to_pylist() == [0, 1, 2, 3]

    def test_filter_by_label(self):
        """Test label filtering."""
        from sabot._cython.graph import VertexTable

        vertices = pa.table({
            'id': pa.array([0, 1, 2, 3], type=pa.int64()),
            'label': pa.array(['Person', 'Person', 'Company', 'Company']).dictionary_encode(),
            'name': ['Alice', 'Bob', 'Acme', 'Globex']
        })

        vt = VertexTable(vertices)
        persons = vt.filter_by_label('Person')

        assert persons.num_vertices() == 2
        assert persons.get_ids().to_pylist() == [0, 1]

        companies = vt.filter_by_label('Company')
        assert companies.num_vertices() == 2
        assert companies.get_ids().to_pylist() == [2, 3]

    def test_select_columns(self):
        """Test column projection."""
        from sabot._cython.graph import VertexTable

        vertices = pa.table({
            'id': pa.array([0, 1, 2, 3], type=pa.int64()),
            'label': pa.array(['Person', 'Person', 'Company', 'Company']).dictionary_encode(),
            'name': ['Alice', 'Bob', 'Acme', 'Globex'],
            'age': [30, 25, 0, 0]
        })

        vt = VertexTable(vertices)
        selected = vt.select(['id', 'name'])

        assert selected.schema().names == ['id', 'name']
        assert selected.num_vertices() == 4


class TestEdgeTable:
    """Test edge table operations."""

    def test_create_edge_table(self):
        """Test edge table creation."""
        from sabot._cython.graph import EdgeTable

        edges = pa.table({
            'src': pa.array([0, 1, 0], type=pa.int64()),
            'dst': pa.array([2, 2, 3], type=pa.int64()),
            'type': pa.array(['WORKS_AT', 'WORKS_AT', 'OWNS']).dictionary_encode(),
            'since': [2018, 2019, 2020]
        })

        et = EdgeTable(edges)

        assert et.num_edges() == 3
        assert 'src' in et.schema().names
        assert 'dst' in et.schema().names
        assert 'type' in et.schema().names

    def test_get_sources_destinations(self):
        """Test source/destination retrieval."""
        from sabot._cython.graph import EdgeTable

        edges = pa.table({
            'src': pa.array([0, 1, 0], type=pa.int64()),
            'dst': pa.array([2, 2, 3], type=pa.int64()),
            'type': pa.array(['WORKS_AT', 'WORKS_AT', 'OWNS']).dictionary_encode(),
        })

        et = EdgeTable(edges)

        assert et.get_sources().to_pylist() == [0, 1, 0]
        assert et.get_destinations().to_pylist() == [2, 2, 3]

    def test_filter_by_type(self):
        """Test edge type filtering."""
        from sabot._cython.graph import EdgeTable

        edges = pa.table({
            'src': pa.array([0, 1, 0], type=pa.int64()),
            'dst': pa.array([2, 2, 3], type=pa.int64()),
            'type': pa.array(['WORKS_AT', 'WORKS_AT', 'OWNS']).dictionary_encode(),
        })

        et = EdgeTable(edges)
        works_at = et.filter_by_type('WORKS_AT')

        assert works_at.num_edges() == 2
        assert works_at.get_sources().to_pylist() == [0, 1]

        owns = et.filter_by_type('OWNS')
        assert owns.num_edges() == 1
        assert owns.get_sources().to_pylist() == [0]

    def test_build_csr(self):
        """Test CSR construction from edge table."""
        from sabot._cython.graph import EdgeTable

        edges = pa.table({
            'src': pa.array([0, 1, 0, 2], type=pa.int64()),
            'dst': pa.array([1, 2, 2, 3], type=pa.int64()),
            'type': pa.array(['A', 'A', 'B', 'C']).dictionary_encode(),
        })

        et = EdgeTable(edges)
        csr = et.build_csr(num_vertices=4)

        assert csr.num_vertices() == 4
        assert csr.num_edges() == 4

        # Check degrees
        assert csr.get_degree(0) == 2
        assert csr.get_degree(1) == 1
        assert csr.get_degree(2) == 1
        assert csr.get_degree(3) == 0

    def test_build_csc(self):
        """Test CSC construction from edge table."""
        from sabot._cython.graph import EdgeTable

        edges = pa.table({
            'src': pa.array([0, 1, 0, 2], type=pa.int64()),
            'dst': pa.array([1, 2, 2, 3], type=pa.int64()),
            'type': pa.array(['A', 'A', 'B', 'C']).dictionary_encode(),
        })

        et = EdgeTable(edges)
        csc = et.build_csc(num_vertices=4)

        assert csc.num_vertices() == 4
        assert csc.num_edges() == 4

        # Check in-degrees
        assert csc.get_degree(0) == 0  # No in-edges
        assert csc.get_degree(1) == 1  # One in-edge
        assert csc.get_degree(2) == 2  # Two in-edges
        assert csc.get_degree(3) == 1  # One in-edge


class TestPropertyGraph:
    """Test property graph operations."""

    def test_create_property_graph(self):
        """Test property graph creation."""
        from sabot._cython.graph import VertexTable, EdgeTable, PropertyGraph

        vertices = pa.table({
            'id': pa.array([0, 1, 2, 3], type=pa.int64()),
            'label': pa.array(['Person', 'Person', 'Company', 'Company']).dictionary_encode(),
            'name': ['Alice', 'Bob', 'Acme', 'Globex']
        })

        edges = pa.table({
            'src': pa.array([0, 1, 0], type=pa.int64()),
            'dst': pa.array([2, 2, 3], type=pa.int64()),
            'type': pa.array(['WORKS_AT', 'WORKS_AT', 'OWNS']).dictionary_encode(),
        })

        vt = VertexTable(vertices)
        et = EdgeTable(edges)
        graph = PropertyGraph(vt, et)

        assert graph.num_vertices() == 4
        assert graph.num_edges() == 3

    def test_build_csr_csc(self):
        """Test CSR/CSC building."""
        from sabot._cython.graph import VertexTable, EdgeTable, PropertyGraph

        vertices = pa.table({
            'id': pa.array([0, 1, 2, 3], type=pa.int64()),
            'label': pa.array(['A', 'A', 'A', 'A']).dictionary_encode(),
        })

        edges = pa.table({
            'src': pa.array([0, 1, 0, 2], type=pa.int64()),
            'dst': pa.array([1, 2, 2, 3], type=pa.int64()),
            'type': pa.array(['E', 'E', 'E', 'E']).dictionary_encode(),
        })

        vt = VertexTable(vertices)
        et = EdgeTable(edges)
        graph = PropertyGraph(vt, et)

        graph.build_csr()
        graph.build_csc()

        assert graph.csr() is not None
        assert graph.csc() is not None

    def test_get_neighbors(self):
        """Test neighbor retrieval."""
        from sabot._cython.graph import VertexTable, EdgeTable, PropertyGraph

        vertices = pa.table({
            'id': pa.array([0, 1, 2, 3], type=pa.int64()),
            'label': pa.array(['A', 'A', 'A', 'A']).dictionary_encode(),
        })

        edges = pa.table({
            'src': pa.array([0, 0, 1, 2], type=pa.int64()),
            'dst': pa.array([1, 2, 2, 3], type=pa.int64()),
            'type': pa.array(['E', 'E', 'E', 'E']).dictionary_encode(),
        })

        vt = VertexTable(vertices)
        et = EdgeTable(edges)
        graph = PropertyGraph(vt, et)

        neighbors = graph.get_neighbors(0)
        assert set(neighbors.to_pylist()) == {1, 2}

        neighbors = graph.get_neighbors(1)
        assert neighbors.to_pylist() == [2]

    def test_get_in_neighbors(self):
        """Test in-neighbor retrieval."""
        from sabot._cython.graph import VertexTable, EdgeTable, PropertyGraph

        vertices = pa.table({
            'id': pa.array([0, 1, 2, 3], type=pa.int64()),
            'label': pa.array(['A', 'A', 'A', 'A']).dictionary_encode(),
        })

        edges = pa.table({
            'src': pa.array([0, 0, 1, 2], type=pa.int64()),
            'dst': pa.array([1, 2, 2, 3], type=pa.int64()),
            'type': pa.array(['E', 'E', 'E', 'E']).dictionary_encode(),
        })

        vt = VertexTable(vertices)
        et = EdgeTable(edges)
        graph = PropertyGraph(vt, et)

        in_neighbors = graph.get_in_neighbors(2)
        assert set(in_neighbors.to_pylist()) == {0, 1}

        in_neighbors = graph.get_in_neighbors(0)
        assert in_neighbors.to_pylist() == []

    def test_match_pattern(self):
        """Test simple pattern matching."""
        from sabot._cython.graph import VertexTable, EdgeTable, PropertyGraph

        vertices = pa.table({
            'id': pa.array([0, 1, 2, 3], type=pa.int64()),
            'label': pa.array(['Person', 'Person', 'Company', 'Company']).dictionary_encode(),
            'name': ['Alice', 'Bob', 'Acme', 'Globex']
        })

        edges = pa.table({
            'src': pa.array([0, 1, 0], type=pa.int64()),
            'dst': pa.array([2, 2, 3], type=pa.int64()),
            'type': pa.array(['WORKS_AT', 'WORKS_AT', 'OWNS']).dictionary_encode(),
        })

        vt = VertexTable(vertices)
        et = EdgeTable(edges)
        graph = PropertyGraph(vt, et)

        # Pattern: (Person)-[WORKS_AT]->(Company)
        result = graph.match_pattern('Person', 'WORKS_AT', 'Company')

        assert result.num_rows == 2
        assert set(result.column('src_id').to_pylist()) == {0, 1}
        assert result.column('dst_id').to_pylist() == [2, 2]


class TestRDFTripleStore:
    """Test RDF triple store operations."""

    def test_create_triple_store(self):
        """Test RDF triple store creation."""
        from sabot._cython.graph import RDFTripleStore

        triples = pa.table({
            's': pa.array([1, 1, 2], type=pa.int64()),
            'p': pa.array([10, 11, 10], type=pa.int64()),
            'o': pa.array([20, 30, 40], type=pa.int64()),
        })

        term_dict = pa.table({
            'id': pa.array([1, 2, 10, 11, 20, 30, 40], type=pa.int64()),
            'lex': ['http://alice', 'http://bob', 'http://knows', 'http://age',
                    'http://charlie', '25', '30'],
            'kind': pa.array([0, 0, 0, 0, 0, 1, 1], type=pa.uint8()),
        })

        store = RDFTripleStore(triples, term_dict)

        assert store.num_triples() == 3
        assert store.num_terms() == 7

    def test_lookup_term_id(self):
        """Test term ID lookup."""
        from sabot._cython.graph import RDFTripleStore

        triples = pa.table({
            's': pa.array([1], type=pa.int64()),
            'p': pa.array([10], type=pa.int64()),
            'o': pa.array([20], type=pa.int64()),
        })

        term_dict = pa.table({
            'id': pa.array([1, 10, 20], type=pa.int64()),
            'lex': ['http://alice', 'http://knows', 'http://bob'],
            'kind': pa.array([0, 0, 0], type=pa.uint8()),
        })

        store = RDFTripleStore(triples, term_dict)

        assert store.lookup_term_id('http://alice') == 1
        assert store.lookup_term_id('http://knows') == 10
        assert store.lookup_term_id('http://bob') == 20
        assert store.lookup_term_id('http://unknown') == -1

    def test_filter_triples(self):
        """Test triple filtering by pattern."""
        from sabot._cython.graph import RDFTripleStore

        triples = pa.table({
            's': pa.array([1, 1, 2, 3], type=pa.int64()),
            'p': pa.array([10, 11, 10, 11], type=pa.int64()),
            'o': pa.array([20, 30, 40, 50], type=pa.int64()),
        })

        term_dict = pa.table({
            'id': pa.array([1, 2, 3, 10, 11, 20, 30, 40, 50], type=pa.int64()),
            'lex': ['a', 'b', 'c', 'knows', 'age', 'd', 'e', 'f', 'g'],
            'kind': pa.array([0] * 9, type=pa.uint8()),
        })

        store = RDFTripleStore(triples, term_dict)

        # Filter: all triples with subject 1
        filtered = store.filter_triples(s=1, p=-1, o=-1)
        assert filtered.num_rows == 2

        # Filter: all triples with predicate 10 (knows)
        filtered = store.filter_triples(s=-1, p=10, o=-1)
        assert filtered.num_rows == 2

        # Filter: exact triple
        filtered = store.filter_triples(s=1, p=10, o=20)
        assert filtered.num_rows == 1


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
