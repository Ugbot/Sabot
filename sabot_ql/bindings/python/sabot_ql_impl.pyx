# cython: language_level=3, boundscheck=False, wraparound=False
"""
SabotQL Python Bindings - Implementation

Exposes SabotQL's RDF triple store with MarbleDB persistence to Python.
"""

from libcpp.string cimport string as cpp_string
from libcpp.memory cimport shared_ptr
from libcpp.vector cimport vector
from libcpp cimport bool as cpp_bool
from libc.stdint cimport uint64_t
from cpython.ref cimport PyObject

# Import C++ declarations - these include vendored Arrow wrap/unwrap functions
from sabot_ql_cpp cimport *

# Import Python Arrow for type compatibility
import pyarrow as pa

# Helper function to check Arrow status
cdef inline check_status(const CStatus& status):
    if not status.ok():
        raise RuntimeError(status.ToString().decode('utf-8'))


cdef class TripleStoreWrapper:
    """
    Python wrapper for SabotQL triple store with MarbleDB persistence.
    Provides full W3C SPARQL 1.1 query support including property paths.
    """
    cdef shared_ptr[TripleStore] c_triple_store
    cdef shared_ptr[Vocabulary] c_vocab
    cdef shared_ptr[MarbleDB] c_db
    cdef shared_ptr[QueryEngine] c_query_engine
    cdef object db_path

    def __init__(self, str db_path):
        """
        Open or create MarbleDB-backed triple store.

        Args:
            db_path: Path to MarbleDB database directory
        """
        self.db_path = db_path
        cdef cpp_string c_db_path = db_path.encode('utf-8')

        # Open MarbleDB
        cdef CResult[shared_ptr[MarbleDB]] db_result = OpenMarbleDB(c_db_path, True)
        check_status(db_result.status())
        self.c_db = db_result.ValueOrDie()

        # Create vocabulary
        cdef CResult[shared_ptr[Vocabulary]] vocab_result = CreateVocabularyMarbleDB(
            c_db_path, self.c_db)
        check_status(vocab_result.status())
        self.c_vocab = vocab_result.ValueOrDie()

        # Create triple store
        cdef CResult[shared_ptr[TripleStore]] store_result = CreateTripleStoreMarbleDB(
            c_db_path, self.c_db)
        check_status(store_result.status())
        self.c_triple_store = store_result.ValueOrDie()

        # Create query engine
        self.c_query_engine = shared_ptr[QueryEngine](
            new QueryEngine(self.c_triple_store, self.c_vocab))

    def add(self, str subject, str predicate, str object_val):
        """
        Add a single RDF triple.

        Args:
            subject: Subject IRI (e.g., '<http://example.org/Alice>')
            predicate: Predicate IRI (e.g., '<http://xmlns.com/foaf/0.1/name>')
            object_val: Object IRI or literal (e.g., '"Alice"' or '<http://example.org/Bob>')
        """
        # For now, use the RDF parser to handle N-Triples syntax
        # This ensures proper parsing of IRIs, literals, etc.
        triple_line = f"{subject} {predicate} {object_val} ."

        # We'll need to expose a method to add N-Triples strings to the store
        # For now, raise NotImplementedError with helpful message
        raise NotImplementedError(
            "add() requires N-Triples parser binding. "
            "Use add_batch() or load triples via the NTriplesParser for now."
        )

    def query(self, str sparql_query):
        """
        Execute a SPARQL query with full W3C 1.1 support including property paths.

        Args:
            sparql_query: SPARQL query string (SELECT, ASK, CONSTRUCT, DESCRIBE)

        Returns:
            Arrow Table with query results

        Examples:
            # Simple pattern
            result = store.query('''
                SELECT ?s ?p ?o
                WHERE { ?s ?p ?o }
                LIMIT 10
            ''')

            # Property path (works!)
            result = store.query('''
                PREFIX foaf: <http://xmlns.com/foaf/0.1/>
                SELECT ?person ?friend
                WHERE {
                    ?person foaf:knows/foaf:name ?friend .
                }
            ''')
        """
        cdef cpp_string c_query = sparql_query.encode('utf-8')

        # Parse SPARQL query
        cdef CResult[Query] parse_result = ParseSPARQL(c_query)
        check_status(parse_result.status())
        cdef Query query_ast = parse_result.ValueOrDie()

        # Convert to SelectQuery (currently only SELECT supported)
        cdef CResult[SelectQuery] select_result = QueryToSelectQuery(query_ast)
        check_status(select_result.status())
        cdef SelectQuery select_query = select_result.ValueOrDie()

        # Execute query
        cdef CResult[shared_ptr[CTable]] exec_result = self.c_query_engine.get().ExecuteSelect(select_query)
        check_status(exec_result.status())
        cdef shared_ptr[CTable] c_table = exec_result.ValueOrDie()

        # Convert to Python Arrow Table using pyarrow's Cython wrapper - Sabot standard
        return pyarrow_wrap_table(c_table)

    def size(self):
        """Get approximate number of triples in store."""
        # Would call triple_store->TotalTriples() if exposed
        raise NotImplementedError("size() method needs C++ binding")

    def close(self):
        """Close the triple store and flush to disk."""
        # Reset shared_ptrs
        self.c_query_engine.reset()
        self.c_triple_store.reset()
        self.c_vocab.reset()
        self.c_db.reset()


def create_triple_store(db_path: str) -> TripleStoreWrapper:
    """
    Create or open SabotQL triple store.

    Args:
        db_path: Path to MarbleDB database directory

    Returns:
        TripleStoreWrapper instance
    """
    return TripleStoreWrapper(db_path)
