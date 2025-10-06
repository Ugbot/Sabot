# -*- coding: utf-8 -*-
"""
Tonbo FFI C function declarations

This file declares the C FFI interface from tonbo-ffi Rust library.
Generated from tonbo_ffi.h
"""

from libc.stdint cimport uint8_t, uintptr_t

cdef extern from "tonbo_ffi.h" nogil:
    # Opaque handles
    ctypedef struct TonboDb:
        pass

    ctypedef struct TonboIter:
        pass

    # Database operations
    TonboDb* tonbo_db_open(const char* path)

    int tonbo_db_get(
        TonboDb* db,
        const char* key,
        uintptr_t key_len,
        uint8_t** value_out,
        uintptr_t* value_len_out
    )

    int tonbo_db_insert(
        TonboDb* db,
        const char* key,
        uintptr_t key_len,
        const uint8_t* value,
        uintptr_t value_len
    )

    int tonbo_db_delete(
        TonboDb* db,
        const char* key,
        uintptr_t key_len
    )

    void tonbo_db_close(TonboDb* db)

    void tonbo_free_bytes(uint8_t* ptr, uintptr_t len)

    # Scan operations (stub implementations)
    TonboIter* tonbo_db_scan(
        TonboDb* db,
        const char* start_key,
        const char* end_key,
        int limit
    )

    int tonbo_iter_next(
        TonboIter* iter,
        char** key_out,
        uint8_t** value_out,
        uintptr_t* value_len_out
    )

    void tonbo_iter_free(TonboIter* iter)
