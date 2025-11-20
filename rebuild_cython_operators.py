#!/usr/bin/env python3
"""
Rebuild Cython operators for Python 3.13
"""

import subprocess
import sys
from pathlib import Path

print()
print("="*70)
print("REBUILDING CYTHON OPERATORS FOR PYTHON 3.13")
print("="*70)
print()

# Check Python version
print(f"Python: {sys.version}")
print(f"Executable: {sys.executable}")
print()

# Build aggregations module
aggregations_dir = Path("sabot/_cython/operators")

print(f"Building aggregations module in {aggregations_dir}")
print("-"*70)

try:
    # Use setuptools to build the extension
    result = subprocess.run([
        sys.executable,
        "setup.py",
        "build_ext",
        "--inplace"
    ], capture_output=True, text=True, timeout=120)
    
    if result.returncode == 0:
        print("✅ Build successful!")
        print()
        if result.stdout:
            print("Output:")
            print(result.stdout[-500:])  # Last 500 chars
    else:
        print("❌ Build failed!")
        print()
        print("STDOUT:")
        print(result.stdout)
        print()
        print("STDERR:")
        print(result.stderr)
        sys.exit(1)
        
except subprocess.TimeoutExpired:
    print("❌ Build timed out after 120 seconds")
    sys.exit(1)
except Exception as e:
    print(f"❌ Error: {e}")
    sys.exit(1)

# Test import
print()
print("Testing import...")
print("-"*70)

try:
    from sabot._cython.operators.aggregations import CythonGroupByOperator
    print("✅ CythonGroupByOperator imported successfully!")
    print(f"   Module: {CythonGroupByOperator.__module__}")
    print()
except ImportError as e:
    print(f"❌ Import failed: {e}")
    sys.exit(1)

print("="*70)
print("✅ REBUILD COMPLETE!")
print("="*70)
print()
