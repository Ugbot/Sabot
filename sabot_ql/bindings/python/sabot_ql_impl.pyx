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

# Import cyarrow for Sabot compatibility
from sabot import cyarrow as pa

# Import C++ declarations
from sabot_ql_cpp cimport *

# Helper function to check Arrow status
cdef inline check_status(const CStatus& status):
    if not status.ok():
        raise RuntimeError(status.ToString().decode('utf-8'))


cdef class TripleStoreWrapper:
    """
    Python wrapper for SabotQL triple store with MarbleDB persistence.
    """
    cdef shared_ptr[TripleStore] c_triple_store
    cdef shared_ptr[Vocabulary] c_vocab
    cdef shared_ptr[MarbleDB] c_db
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

    def insert_triple(self, str subject, str predicate, str object_val):
        """
        Insert a single RDF triple.

        Args:
            subject: Subject IRI
            predicate: Predicate IRI
            object_val: Object IRI or literal
        """
        # Convert strings to Terms and get ValueIds
        cdef Term subject_term
        subject_term.lexical = subject.encode('utf-8')
        subject_term.kind = IRI
        subject_term.language = b""
        subject_term.datatype = b""

        cdef Term predicate_term
        predicate_term.lexical = predicate.encode('utf-8')
        predicate_term.kind = IRI
        predicate_term.language = b""
        predicate_term.datatype = b""

        cdef Term object_term
        object_term.lexical = object_val.encode('utf-8')
        object_term.kind = IRI
        object_term.language = b""
        object_term.datatype = b""

        # Add terms to vocabulary and get IDs
        # Note: This is a simplified version - full implementation would call
        # vocab->AddTerm() and handle the Result properly

        # For now, raise NotImplementedError since we need to expose more C++ methods
        raise NotImplementedError(
            "insert_triple() requires additional C++ bindings for Vocabulary::AddTerm(). "
            "Use insert_triples_batch() with Arrow data instead."
        )

    def size(self):
        """Get approximate number of triples in store."""
        # Would call triple_store->Size() if exposed
        raise NotImplementedError("size() method needs C++ binding")

    def close(self):
        """Close the triple store and flush to disk."""
        # Reset shared_ptrs
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
