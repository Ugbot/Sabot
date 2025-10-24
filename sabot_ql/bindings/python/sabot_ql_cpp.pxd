# cython: language_level=3
"""
C++ declarations for SabotQL bindings
"""

from libcpp.string cimport string as cpp_string
from libcpp.memory cimport shared_ptr, unique_ptr
from libcpp.vector cimport vector
from libcpp cimport bool as cpp_bool
from libc.stdint cimport uint64_t, int64_t

# C++ std::string for Arrow
cdef extern from "<string>" namespace "std":
    cdef cppclass string:
        const char* c_str()

# Import pyarrow's Cython bindings - Sabot standard pattern
cimport pyarrow.lib as ca
from pyarrow.lib cimport CStatus, pyarrow_wrap_table, pyarrow_unwrap_table
from pyarrow.includes.libarrow cimport CTable, CRecordBatch, CSchema

# Define CResult template
cdef extern from "arrow/result.h" namespace "arrow" nogil:
    cdef cppclass CResult "arrow::Result"[T]:
        CResult()
        CResult(T)
        CStatus status()
        T ValueOrDie() except +
        cpp_bool ok()

# MarbleDB declarations
cdef extern from "marble/db.h" namespace "marble":
    cdef cppclass MarbleDB:
        pass

# SabotQL declarations
cdef extern from "sabot_ql/storage/triple_store.h" namespace "sabot_ql":
    cdef cppclass TripleStore:
        pass

    cdef struct Triple:
        uint64_t subject
        uint64_t predicate
        uint64_t object

    cdef struct TriplePattern:
        pass

cdef extern from "sabot_ql/storage/vocabulary.h" namespace "sabot_ql":
    cdef cppclass Vocabulary:
        pass

    # TermKind enum
    ctypedef enum TermKind:
        IRI "sabot_ql::TermKind::IRI"
        Literal "sabot_ql::TermKind::Literal"
        BlankNode "sabot_ql::TermKind::BlankNode"

    cdef struct Term:
        cpp_string lexical
        TermKind kind
        cpp_string language
        cpp_string datatype

cdef extern from "sabot_ql/types/value_id.h" namespace "sabot_ql":
    cdef cppclass ValueId:
        @staticmethod
        ValueId fromBits(uint64_t bits)
        uint64_t getBits() const

# SPARQL AST declarations
cdef extern from "sabot_ql/sparql/ast.h" namespace "sabot_ql::sparql":
    cdef cppclass Query:
        pass

    cdef cppclass SelectQuery:
        pass

    cdef cppclass AskQuery:
        pass

# SPARQL Parser declarations
cdef extern from "sabot_ql/sparql/parser.h" namespace "sabot_ql::sparql":
    CResult[Query] ParseSPARQL(const cpp_string& query_text)

# SPARQL Query Engine declarations
cdef extern from "sabot_ql/sparql/query_engine.h" namespace "sabot_ql::sparql":
    cdef cppclass QueryEngine:
        QueryEngine(shared_ptr[TripleStore] store, shared_ptr[Vocabulary] vocab)
        CResult[shared_ptr[CTable]] ExecuteSelect(const SelectQuery& query)

# Bindings factory functions
cdef extern from "sabot_ql/bindings.h" namespace "sabot_ql::bindings":
    CResult[shared_ptr[MarbleDB]] OpenMarbleDB(const cpp_string& db_path, cpp_bool create_if_missing)

    CResult[shared_ptr[TripleStore]] CreateTripleStoreMarbleDB(
        const cpp_string& db_path,
        shared_ptr[MarbleDB] db)

    CResult[shared_ptr[Vocabulary]] CreateVocabularyMarbleDB(
        const cpp_string& db_path,
        shared_ptr[MarbleDB] db)

    # Helper to convert Query to SelectQuery (safe downcast)
    CResult[SelectQuery] QueryToSelectQuery(const Query& query)
