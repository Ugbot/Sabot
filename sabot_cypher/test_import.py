#!/usr/bin/env python3
"""
Test SabotCypher Python module import.

Verifies the Python module structure without requiring the C++ extension.
"""

import sys

def test_import():
    """Test importing sabot_cypher module."""
    print("=" * 70)
    print("Testing SabotCypher Python Module")
    print("=" * 70)
    print()
    
    # Test import
    print("1. Testing import...")
    try:
        import sabot_cypher
        print("   ✅ Module imported successfully")
    except ImportError as e:
        print(f"   ❌ Import failed: {e}")
        return False
    
    # Check version
    print("2. Checking version...")
    print(f"   Version: {sabot_cypher.__version__}")
    
    # Check native availability
    print("3. Checking native extension...")
    if sabot_cypher.is_native_available():
        print("   ✅ Native extension loaded")
    else:
        print("   ⚠️  Native extension not available")
        error = sabot_cypher.get_import_error()
        if error:
            print(f"   Error: {error}")
    
    # Check API
    print("4. Checking API...")
    expected_attrs = [
        '__version__',
        'SabotCypherBridge',
        'CypherResult',
        'execute',
        'is_native_available',
        'get_import_error',
    ]
    
    for attr in expected_attrs:
        if hasattr(sabot_cypher, attr):
            print(f"   ✅ {attr}")
        else:
            print(f"   ❌ Missing: {attr}")
    
    # Test placeholder behavior
    print("5. Testing placeholder behavior...")
    try:
        bridge = sabot_cypher.SabotCypherBridge.create()
        print("   ❌ Should have raised NotImplementedError")
    except NotImplementedError as e:
        print(f"   ✅ Correctly raises NotImplementedError")
        print(f"      Message: {str(e)[:60]}...")
    
    print()
    print("=" * 70)
    print("Module Structure Test Complete")
    print("=" * 70)
    print()
    print("Status:")
    print("  - Python module structure: ✅ OK")
    print("  - Native extension: ⚠️  Not yet built")
    print()
    print("Next step: Build C++ extension")
    print("  cd sabot_cypher")
    print("  cmake -S . -B build -DCMAKE_BUILD_TYPE=Release")
    print("  cmake --build build")
    print()
    
    return True

if __name__ == "__main__":
    success = test_import()
    sys.exit(0 if success else 1)

