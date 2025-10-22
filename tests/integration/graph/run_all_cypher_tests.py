"""
Comprehensive Cypher Query Test Suite

Runs all Cypher query tests to verify complete functionality.
"""

import sys
from test_cypher_queries import run_all_tests as run_basic_tests
from test_property_access import run_all_tests as run_property_tests
from test_2hop_patterns import run_all_tests as run_2hop_tests


def run_all_cypher_tests():
    """Run all Cypher query test suites"""
    print("=" * 60)
    print("COMPREHENSIVE CYPHER QUERY TEST SUITE")
    print("=" * 60)
    print()

    all_passed = True

    # Run basic query tests
    print("=" * 60)
    print("1. Basic Cypher Query Tests")
    print("=" * 60)
    print()
    if not run_basic_tests():
        all_passed = False
    print()

    # Run property access tests
    print("=" * 60)
    print("2. Property Access Tests")
    print("=" * 60)
    print()
    if not run_property_tests():
        all_passed = False
    print()

    # Run 2-hop pattern tests
    print("=" * 60)
    print("3. 2-Hop Pattern Tests")
    print("=" * 60)
    print()
    if not run_2hop_tests():
        all_passed = False
    print()

    # Final summary
    print("=" * 60)
    print("COMPREHENSIVE TEST SUITE SUMMARY")
    print("=" * 60)
    if all_passed:
        print("✅ ALL TESTS PASSED!")
        print()
        print("Features Verified:")
        print("  ✅ Basic edge patterns: (a)-[r]->(b)")
        print("  ✅ Edge type filtering: -[:KNOWS]->")
        print("  ✅ Node label filtering: (a:Person)")
        print("  ✅ Property access: RETURN a.name")
        print("  ✅ 2-hop patterns: (a)-[r1]->(b)-[r2]->(c)")
        print("  ✅ LIMIT clause")
        print("  ✅ Variable naming")
        print("  ✅ Empty graph handling")
        print()
        print("Total: 19 tests across 3 test suites")
    else:
        print("❌ SOME TESTS FAILED")
    print("=" * 60)

    return all_passed


if __name__ == '__main__':
    success = run_all_cypher_tests()
    sys.exit(0 if success else 1)
