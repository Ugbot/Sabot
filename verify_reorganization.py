#!/usr/bin/env python3
"""Quick verification of SQL module reorganization"""

import os
from pathlib import Path

GREEN = '\033[92m'
RED = '\033[91m'
RESET = '\033[0m'

def check(condition, message):
    status = f"{GREEN}✓{RESET}" if condition else f"{RED}✗{RESET}"
    print(f"{status} {message}")
    return condition

print("\n" + "="*60)
print("SQL Module Reorganization Verification")
print("="*60 + "\n")

all_good = True

# Check sabot_sql structure
print("Checking sabot_sql/ structure:")
all_good &= check(Path("sabot_sql").exists(), "sabot_sql/ directory exists")
all_good &= check(Path("sabot_sql/CMakeLists.txt").exists(), "sabot_sql/CMakeLists.txt exists")
all_good &= check(Path("sabot_sql/README.md").exists(), "sabot_sql/README.md exists")
all_good &= check(Path("sabot_sql/include/sabot_sql/sql").exists(), "include/sabot_sql/sql/ exists")
all_good &= check(Path("sabot_sql/include/sabot_sql/operators").exists(), "include/sabot_sql/operators/ exists")
all_good &= check(Path("sabot_sql/src/sql").exists(), "src/sql/ exists")
all_good &= check(Path("sabot_sql/src/operators").exists(), "src/operators/ exists")

# Check SQL files moved
print("\nChecking SQL files in sabot_sql/:")
sql_files = [
    "include/sabot_sql/sql/duckdb_bridge.h",
    "include/sabot_sql/sql/sql_operator_translator.h",
    "include/sabot_sql/sql/query_engine.h",
    "include/sabot_sql/operators/table_scan.h",
    "include/sabot_sql/operators/cte.h",
    "include/sabot_sql/operators/subquery.h",
    "src/sql/duckdb_bridge.cpp",
    "src/sql/sql_operator_translator.cpp",
    "src/sql/query_engine.cpp",
    "src/operators/table_scan.cpp",
    "src/operators/cte.cpp",
    "src/operators/subquery.cpp",
]
for f in sql_files:
    all_good &= check(Path(f"sabot_sql/{f}").exists(), f)

# Check sabot_ql cleaned up
print("\nChecking sabot_ql/ cleanup:")
all_good &= check(not Path("sabot_ql/include/sabot_ql/sql").exists(), 
                  "sabot_ql/include/sabot_ql/sql/ removed")
all_good &= check(not Path("sabot_ql/src/sql").exists(), 
                  "sabot_ql/src/sql/ removed")

# Check namespace updates
print("\nChecking namespace updates:")
with open("sabot_sql/include/sabot_sql/sql/duckdb_bridge.h") as f:
    content = f.read()
    all_good &= check("namespace sabot_sql" in content, 
                     "namespace updated to sabot_sql")
    all_good &= check("sabot_sql/sql/duckdb_bridge.h" in content or 
                     "sabot_sql/" in content,
                     "include paths updated")

# Check Python layer unchanged
print("\nChecking Python layer (should be unchanged):")
all_good &= check(Path("sabot/sql/__init__.py").exists(), 
                  "sabot/sql/__init__.py exists")
all_good &= check(Path("sabot/sql/controller.py").exists(), 
                  "sabot/sql/controller.py exists")
all_good &= check(Path("sabot/api/sql.py").exists(), 
                  "sabot/api/sql.py exists")

print("\n" + "="*60)
if all_good:
    print(f"{GREEN}✅ All checks passed! Reorganization successful.{RESET}")
else:
    print(f"{RED}❌ Some checks failed!{RESET}")
print("="*60 + "\n")

exit(0 if all_good else 1)
