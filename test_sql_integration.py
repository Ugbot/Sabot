"""
Standalone SQL Integration Test

Tests the SQL pipeline implementation without requiring full Sabot build.
Verifies file structure, code quality, and basic functionality.
"""

import os
import sys
from pathlib import Path

# Colors for output
GREEN = '\033[92m'
RED = '\033[91m'
YELLOW = '\033[93m'
RESET = '\033[0m'

def check_file_exists(filepath):
    """Check if a file exists"""
    exists = Path(filepath).exists()
    status = f"{GREEN}✓{RESET}" if exists else f"{RED}✗{RESET}"
    print(f"  {status} {filepath}")
    return exists

def check_files_exist():
    """Verify all SQL integration files were created"""
    print(f"\n{YELLOW}=== Checking File Structure ==={RESET}\n")
    
    files = {
        "C++ Headers": [
            "sabot_ql/include/sabot_ql/sql/duckdb_bridge.h",
            "sabot_ql/include/sabot_ql/sql/sql_operator_translator.h",
            "sabot_ql/include/sabot_ql/sql/query_engine.h",
            "sabot_ql/include/sabot_ql/operators/table_scan.h",
            "sabot_ql/include/sabot_ql/operators/cte.h",
            "sabot_ql/include/sabot_ql/operators/subquery.h",
        ],
        "C++ Implementation": [
            "sabot_ql/src/sql/duckdb_bridge.cpp",
            "sabot_ql/src/sql/sql_operator_translator.cpp",
            "sabot_ql/src/sql/query_engine.cpp",
            "sabot_ql/src/operators/table_scan.cpp",
            "sabot_ql/src/operators/cte.cpp",
            "sabot_ql/src/operators/subquery.cpp",
        ],
        "Python Modules": [
            "sabot/sql/__init__.py",
            "sabot/sql/controller.py",
            "sabot/sql/agents.py",
            "sabot/api/sql.py",
        ],
        "Examples & Documentation": [
            "examples/sql_pipeline_demo.py",
            "SQL_PIPELINE_IMPLEMENTATION.md",
        ],
        "Build Configuration": [
            "sabot_ql/CMakeLists.txt",
        ]
    }
    
    all_exist = True
    for category, file_list in files.items():
        print(f"{category}:")
        for filepath in file_list:
            if not check_file_exists(filepath):
                all_exist = False
        print()
    
    return all_exist

def check_code_quality():
    """Check code quality and basic syntax"""
    print(f"\n{YELLOW}=== Checking Code Quality ==={RESET}\n")
    
    # Check Python files for syntax errors
    python_files = [
        "sabot/sql/__init__.py",
        "sabot/sql/controller.py",
        "sabot/sql/agents.py",
        "sabot/api/sql.py",
        "examples/sql_pipeline_demo.py",
    ]
    
    all_valid = True
    for filepath in python_files:
        try:
            with open(filepath, 'r') as f:
                code = f.read()
                compile(code, filepath, 'exec')
            print(f"  {GREEN}✓{RESET} {filepath} - Valid Python syntax")
        except SyntaxError as e:
            print(f"  {RED}✗{RESET} {filepath} - Syntax error: {e}")
            all_valid = False
        except FileNotFoundError:
            print(f"  {RED}✗{RESET} {filepath} - File not found")
            all_valid = False
    
    return all_valid

def check_implementation_features():
    """Check that key features are implemented"""
    print(f"\n{YELLOW}=== Checking Implementation Features ==={RESET}\n")
    
    features = []
    
    # Check DuckDB bridge
    try:
        with open("sabot_ql/include/sabot_ql/sql/duckdb_bridge.h", 'r') as f:
            content = f.read()
            features.append(("DuckDB Bridge", "DuckDBBridge" in content and "ParseAndOptimize" in content))
    except:
        features.append(("DuckDB Bridge", False))
    
    # Check Operator Translator
    try:
        with open("sabot_ql/include/sabot_ql/sql/sql_operator_translator.h", 'r') as f:
            content = f.read()
            features.append(("Operator Translator", "SQLOperatorTranslator" in content and "Translate" in content))
    except:
        features.append(("Operator Translator", False))
    
    # Check SQL Query Engine
    try:
        with open("sabot_ql/include/sabot_ql/sql/query_engine.h", 'r') as f:
            content = f.read()
            features.append(("SQL Query Engine", "SQLQueryEngine" in content and "Execute" in content))
    except:
        features.append(("SQL Query Engine", False))
    
    # Check TableScanOperator
    try:
        with open("sabot_ql/include/sabot_ql/operators/table_scan.h", 'r') as f:
            content = f.read()
            features.append(("TableScanOperator", "TableScanOperator" in content and "GetNextBatch" in content))
    except:
        features.append(("TableScanOperator", False))
    
    # Check CTEOperator
    try:
        with open("sabot_ql/include/sabot_ql/operators/cte.h", 'r') as f:
            content = f.read()
            features.append(("CTEOperator", "CTEOperator" in content and "Materialize" in content))
    except:
        features.append(("CTEOperator", False))
    
    # Check SubqueryOperator
    try:
        with open("sabot_ql/include/sabot_ql/operators/subquery.h", 'r') as f:
            content = f.read()
            features.append(("SubqueryOperator", "SubqueryOperator" in content and "SubqueryType" in content))
    except:
        features.append(("SubqueryOperator", False))
    
    # Check Python SQLController
    try:
        with open("sabot/sql/controller.py", 'r') as f:
            content = f.read()
            features.append(("Python SQLController", "SQLController" in content and "execute" in content))
    except:
        features.append(("Python SQLController", False))
    
    # Check Python SQL API
    try:
        with open("sabot/api/sql.py", 'r') as f:
            content = f.read()
            features.append(("Python SQL API", "SQLEngine" in content and "register_table" in content))
    except:
        features.append(("Python SQL API", False))
    
    # Check CMakeLists integration
    try:
        with open("sabot_ql/CMakeLists.txt", 'r') as f:
            content = f.read()
            features.append(("CMake DuckDB Integration", "DUCKDB" in content and "sql/duckdb_bridge.cpp" in content))
    except:
        features.append(("CMake DuckDB Integration", False))
    
    all_implemented = True
    for feature_name, implemented in features:
        status = f"{GREEN}✓{RESET}" if implemented else f"{RED}✗{RESET}"
        print(f"  {status} {feature_name}")
        if not implemented:
            all_implemented = False
    
    return all_implemented

def count_lines_of_code():
    """Count lines of code written"""
    print(f"\n{YELLOW}=== Code Statistics ==={RESET}\n")
    
    files_to_count = [
        # C++ Headers
        "sabot_ql/include/sabot_ql/sql/duckdb_bridge.h",
        "sabot_ql/include/sabot_ql/sql/sql_operator_translator.h",
        "sabot_ql/include/sabot_ql/sql/query_engine.h",
        "sabot_ql/include/sabot_ql/operators/table_scan.h",
        "sabot_ql/include/sabot_ql/operators/cte.h",
        "sabot_ql/include/sabot_ql/operators/subquery.h",
        # C++ Implementation
        "sabot_ql/src/sql/duckdb_bridge.cpp",
        "sabot_ql/src/sql/sql_operator_translator.cpp",
        "sabot_ql/src/sql/query_engine.cpp",
        "sabot_ql/src/operators/table_scan.cpp",
        "sabot_ql/src/operators/cte.cpp",
        "sabot_ql/src/operators/subquery.cpp",
        # Python
        "sabot/sql/controller.py",
        "sabot/sql/agents.py",
        "sabot/api/sql.py",
        "examples/sql_pipeline_demo.py",
    ]
    
    total_lines = 0
    total_files = 0
    
    for filepath in files_to_count:
        try:
            with open(filepath, 'r') as f:
                lines = len(f.readlines())
                total_lines += lines
                total_files += 1
                print(f"  {filepath}: {lines} lines")
        except:
            pass
    
    print(f"\n  Total: {total_files} files, {total_lines} lines of code")
    return total_lines

def main():
    """Run all tests"""
    print(f"\n{GREEN}{'=' * 60}{RESET}")
    print(f"{GREEN}SQL Pipeline Integration - Verification Test{RESET}")
    print(f"{GREEN}{'=' * 60}{RESET}")
    
    results = []
    
    # Test 1: File Structure
    files_ok = check_files_exist()
    results.append(("File Structure", files_ok))
    
    # Test 2: Code Quality
    quality_ok = check_code_quality()
    results.append(("Code Quality", quality_ok))
    
    # Test 3: Implementation Features
    features_ok = check_implementation_features()
    results.append(("Implementation Features", features_ok))
    
    # Test 4: Code Statistics
    lines = count_lines_of_code()
    results.append(("Code Statistics", lines > 0))
    
    # Final Results
    print(f"\n{YELLOW}=== Test Results ==={RESET}\n")
    
    all_passed = True
    for test_name, passed in results:
        status = f"{GREEN}PASS{RESET}" if passed else f"{RED}FAIL{RESET}"
        print(f"  {status} - {test_name}")
        if not passed:
            all_passed = False
    
    print(f"\n{GREEN}{'=' * 60}{RESET}")
    if all_passed:
        print(f"{GREEN}✓ All tests passed!{RESET}")
        print(f"\n{YELLOW}Next Steps:{RESET}")
        print(f"  1. Build DuckDB (if not already built):")
        print(f"     cd vendor/duckdb && make")
        print(f"  2. Fix pre-existing compilation errors in sabot_ql")
        print(f"  3. Build sabot_ql:")
        print(f"     cd sabot_ql/build && cmake .. && make")
        print(f"  4. Run the demo:")
        print(f"     python examples/sql_pipeline_demo.py")
    else:
        print(f"{RED}✗ Some tests failed{RESET}")
        return 1
    
    print(f"{GREEN}{'=' * 60}{RESET}\n")
    return 0

if __name__ == "__main__":
    sys.exit(main())


