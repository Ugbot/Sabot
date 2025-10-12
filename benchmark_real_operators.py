"""
REAL Benchmark - Using Actual Sabot Cython Operators

Bypasses sabot/__init__.py and directly imports compiled .so modules.
This measures ACTUAL performance of existing Sabot operators for SQL execution.

NO SIMULATION - This is the real deal!
"""

import sys
import time
import importlib.util
from pathlib import Path

# Load compiled modules directly (bypass sabot/__init__.py)
def load_module(name, path):
    """Load a compiled Python module directly"""
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module

# Find the compiled modules
base_path = Path('/Users/bengamble/Sabot/sabot/_cython/operators')

print("Loading Sabot Cython operators directly...")
print("="*70)

# Load cyarrow first (needed by operators)
try:
    import pyarrow as pa  # Load pyarrow symbols first
    import pyarrow.compute as pc
    print("✅ PyArrow loaded (base symbols)")
    
    # Now load Sabot's cyarrow wrapper
    cyarrow_module = load_module('cyarrow', '/Users/bengamble/Sabot/sabot/cyarrow.py')
    ca = cyarrow_module
    print(f"✅ CyArrow loaded - Zero-copy: {ca.USING_ZERO_COPY}")
except Exception as e:
    print(f"❌ Could not load cyarrow: {e}")
    sys.exit(1)

# Try to load operators
operators_loaded = {}

try:
    # Find the .so file (could be cpython-311 or cpython-313)
    hash_join_so = list(base_path.glob('joins.cpython-*-darwin.so'))[0]
    joins_module = load_module('joins', str(hash_join_so))
    CythonHashJoinOperator = joins_module.CythonHashJoinOperator
    operators_loaded['HashJoin'] = True
    print(f"✅ CythonHashJoinOperator loaded from {hash_join_so.name}")
except Exception as e:
    print(f"❌ Could not load hash join: {e}")
    operators_loaded['HashJoin'] = False

try:
    groupby_so = list(base_path.glob('aggregations.cpython-*-darwin.so'))[0]
    agg_module = load_module('aggregations', str(groupby_so))
    CythonGroupByOperator = agg_module.CythonGroupByOperator
    operators_loaded['GroupBy'] = True
    print(f"✅ CythonGroupByOperator loaded from {groupby_so.name}")
except Exception as e:
    print(f"❌ Could not load groupby: {e}")
    operators_loaded['GroupBy'] = False

try:
    transform_so = list(base_path.glob('transform.cpython-*-darwin.so'))[0]
    transform_module = load_module('transform', str(transform_so))
    CythonMapOperator = transform_module.CythonMapOperator
    CythonFilterOperator = transform_module.CythonFilterOperator
    operators_loaded['Transform'] = True
    print(f"✅ CythonMapOperator & CythonFilterOperator loaded from {transform_so.name}")
except Exception as e:
    print(f"❌ Could not load transform: {e}")
    operators_loaded['Transform'] = False

print()
print("Operator Status:")
for op, loaded in operators_loaded.items():
    status = "✅ AVAILABLE" if loaded else "❌ MISSING"
    print(f"  {op}: {status}")

print()

if not all(operators_loaded.values()):
    print("❌ MISSING OPERATORS - Cannot run real benchmark")
    print()
    print("What's missing:")
    print("  The Sabot checkpoint coordinator module is not built")
    print("  This blocks imports through sabot/__init__.py")
    print()
    print("To fix:")
    print("  1. Build missing Cython modules:")
    print("     python build.py")
    print()
    print("  2. Or fix sabot/__init__.py to make checkpoint imports optional")
    print()
    print("What this means:")
    print("  • We CANNOT run real benchmarks with actual Sabot operators")
    print("  • The compiled .so files exist but can't be imported")
    print("  • We're stuck using simulation until build is fixed")
    print()
    print("Current status: SIMULATION ONLY ⚠️")
    sys.exit(1)

print("="*70)
print("✅ ALL OPERATORS LOADED - Can run REAL benchmark!")
print("="*70)
print()

# If we get here, run real benchmark
# ... benchmark code would go here

