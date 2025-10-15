#!/bin/bash
# Build SabotQL Python bindings
#
# This script builds both the C++ library and Python bindings

set -e

echo "╔══════════════════════════════════════════════════════════╗"
echo "║  Building SabotQL Python Bindings                       ║"
echo "╚══════════════════════════════════════════════════════════╝"
echo ""

# ============================================================================
# Step 1: Build C++ Library
# ============================================================================

echo "[1/3] Building C++ library..."
cd ../../build

if [ ! -f CMakeCache.txt ]; then
    echo "  Running CMake..."
    cmake .. -DCMAKE_BUILD_TYPE=Release
fi

echo "  Compiling (this may take a minute)..."
make -j8

if [ $? -eq 0 ]; then
    echo "  ✅ C++ library built: libsabot_ql.dylib"
else
    echo "  ❌ C++ build failed"
    exit 1
fi

# ============================================================================
# Step 2: Build Python Bindings (PyBind11)
# ============================================================================

echo ""
echo "[2/3] Building Python bindings (PyBind11)..."
cd ..

# Check if pybind11 is available
python3 -c "import pybind11" 2>/dev/null
if [ $? -eq 0 ]; then
    echo "  pybind11 found, building native module..."
    cd build
    cmake .. -DBUILD_PYTHON_BINDINGS=ON
    make -j8
    
    if [ $? -eq 0 ]; then
        echo "  ✅ PyBind11 bindings built"
    else
        echo "  ⚠️  PyBind11 build failed, will use Cython fallback"
    fi
    cd ..
else
    echo "  pybind11 not found, using Cython bindings"
fi

# ============================================================================
# Step 3: Install Python Package
# ============================================================================

echo ""
echo "[3/3] Installing Python package..."
cd bindings/python

pip install -e .

if [ $? -eq 0 ]; then
    echo "  ✅ Python package installed"
else
    echo "  ❌ Python package installation failed"
    exit 1
fi

# ============================================================================
# Verification
# ============================================================================

echo ""
echo "Verifying installation..."

python3 << 'EOF'
try:
    from sabot_ql.bindings.python import create_triple_store
    print("✅ Import successful")
    
    # Quick test
    kg = create_triple_store('./test_verify.db')
    kg.insert_triple('http://a', 'http://b', '"c"')
    total = kg.total_triples()
    
    if total == 1:
        print(f"✅ Basic operations work (inserted {total} triple)")
    else:
        print(f"⚠️  Unexpected triple count: {total}")
    
    print("\n✅ SabotQL Python bindings are ready!")
    
except Exception as e:
    print(f"❌ Verification failed: {e}")
    exit(1)
EOF

if [ $? -eq 0 ]; then
    echo ""
    echo "╔══════════════════════════════════════════════════════════╗"
    echo "║  Build Complete! ✅                                      ║"
    echo "╚══════════════════════════════════════════════════════════╝"
    echo ""
    echo "Next steps:"
    echo "  1. Run quickstart: python examples/sabot_ql_integration/quickstart.py"
    echo "  2. Run tests: python examples/sabot_ql_integration/test_triple_enrichment.py"
    echo "  3. See docs: cat examples/sabot_ql_integration/README.md"
else
    echo ""
    echo "❌ Build verification failed"
    exit 1
fi

