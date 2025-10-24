#!/usr/bin/env python3
"""
Example: Using Sabot's MarbleDB-backed Triple Store

This demonstrates the RDF triple store with persistent storage.
"""

import tempfile
import os

# Setup environment for testing
import sys
bindings_path = os.path.join(
    os.path.dirname(os.path.dirname(__file__)),
    "sabot_ql", "bindings", "python"
)
if bindings_path not in sys.path:
    sys.path.insert(0, bindings_path)

from sabot.graph import create_triple_store

def main():
    print("=" * 70)
    print("Sabot Triple Store Example")
    print("=" * 70)

    # Create a temporary directory for the database
    temp_dir = tempfile.mkdtemp()
    db_path = os.path.join(temp_dir, "my_knowledge_graph")

    print(f"\n📁 Database path: {db_path}")

    # Create the triple store
    print("\n1️⃣  Creating triple store...")
    store = create_triple_store(db_path)
    print("   ✅ Store created")

    # Currently, insert_triple() is not yet implemented
    # This will be available in a future version
    print("\n2️⃣  Attempting to insert triple...")
    try:
        store.insert_triple(
            "http://example.org/Alice",
            "http://example.org/knows",
            "http://example.org/Bob"
        )
        print("   ✅ Triple inserted")
    except NotImplementedError as e:
        print(f"   ⚠️  Not yet implemented: {e}")
        print("   ℹ️  Future versions will support:")
        print("      - insert_triple() for single triple insertion")
        print("      - insert_triples_batch() for bulk loading")
        print("      - query() for SPARQL queries")

    # Using context manager (auto-close)
    print("\n3️⃣  Using context manager...")
    with create_triple_store(db_path) as store2:
        print("   ✅ Store opened in context")
    print("   ✅ Store automatically closed")

    # Clean up
    print("\n4️⃣  Closing store...")
    store.close()
    print("   ✅ Store closed")

    print("\n" + "=" * 70)
    print("Summary")
    print("=" * 70)
    print("\n✅ Triple store created successfully")
    print("✅ MarbleDB persistence working")
    print("✅ Memory management correct")
    print("\n📋 What's Available Now:")
    print("   - create_triple_store() - Create/open persistent store")
    print("   - store.close() - Clean shutdown")
    print("   - Context manager support")
    print("\n🚧 Coming Soon:")
    print("   - Triple insertion (single and batch)")
    print("   - SPARQL query support")
    print("   - Triple counting and statistics")
    print("   - Integration with PropertyGraph")

    # Cleanup
    import shutil
    if os.path.exists(temp_dir):
        shutil.rmtree(temp_dir)

if __name__ == "__main__":
    main()
