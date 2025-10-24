# cython: language_level=3
"""
C++ declarations for SabotQL bindings
"""

from libcpp.string cimport string as cpp_string
from libcpp.memory cimport shared_ptr, unique_ptr
from libcpp.vector cimport vector
from libcpp cimport bool as cpp_bool
from libc.stdint cimport uint64_t, int64_t

# Arrow C++ declarations
from pyarrow.lib cimport (
    CStatus,
    CTable,
    CRecordBatch,
    CSchema,
    shared_ptr as arrow_shared_ptr
)

# Define CResult template since pyarrow might not expose it fully
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

# Bindings factory functions
cdef extern from "sabot_ql/bindings.h" namespace "sabot_ql::bindings":
    CResult[shared_ptr[MarbleDB]] OpenMarbleDB(const cpp_string& db_path, cpp_bool create_if_missing)

    CResult[shared_ptr[TripleStore]] CreateTripleStoreMarbleDB(
        const cpp_string& db_path,
        shared_ptr[MarbleDB] db)

    CResult[shared_ptr[Vocabulary]] CreateVocabularyMarbleDB(
        const cpp_string& db_path,
        shared_ptr[MarbleDB] db)
