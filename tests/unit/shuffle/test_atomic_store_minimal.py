#!/usr/bin/env python3
"""Minimal test of AtomicPartitionStore."""

import sys
print("Importing AtomicPartitionStore...", flush=True)
from sabot._cython.shuffle.atomic_partition_store import AtomicPartitionStore
print("Import successful!", flush=True)

print("Creating store with size=128...", flush=True)
store = AtomicPartitionStore(size=128)
print(f"Store created: {store}", flush=True)

print(f"Store size: {store.size()}", flush=True)
print("✅ Test passed!")
