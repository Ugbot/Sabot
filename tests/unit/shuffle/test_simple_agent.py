#!/usr/bin/env python3
"""Simple agent shuffle test."""
import sys
print("1: Starting imports", flush=True)

from sabot._cython.shuffle.shuffle_transport import ShuffleServer, ShuffleClient
print("2: Imported", flush=True)

from sabot import cyarrow as pa
print("3: Imported PyArrow", flush=True)

# Create server
server = ShuffleServer(b"127.0.0.1", 9000)
print("4: Created server", flush=True)

# Create batch
schema = pa.schema([('id', pa.int64())])
batch = pa.RecordBatch.from_arrays([pa.array([1, 2, 3], type=pa.int64())], schema=schema)
print("5: Created batch", flush=True)

# Register
server.register_partition(b"test", 0, batch)
print("6: Registered partition", flush=True)

# Create client
client = ShuffleClient()
print("7: Created client", flush=True)

# Fetch
result = client.fetch_partition(b"127.0.0.1", 9000, b"test", 0)
print(f"8: Fetch result: {result}", flush=True)

print("✅ Test passed!", flush=True)
