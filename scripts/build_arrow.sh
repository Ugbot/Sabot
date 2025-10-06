#!/usr/bin/env bash
# Build Apache Arrow C++ from vendored source
#
# This builds the Arrow C++ library that our Cython modules link against.
# After building, Cython modules will use vendored Arrow instead of pip pyarrow.

set -euo pipefail

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Get script directory and project root
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

ARROW_SOURCE_DIR="$PROJECT_ROOT/vendor/arrow/cpp"
ARROW_BUILD_DIR="$ARROW_SOURCE_DIR/build"
ARROW_INSTALL_DIR="$ARROW_BUILD_DIR/install"

echo -e "${GREEN}Building Apache Arrow C++ Library${NC}"
echo "=========================================="
echo "Source:  $ARROW_SOURCE_DIR"
echo "Build:   $ARROW_BUILD_DIR"
echo "Install: $ARROW_INSTALL_DIR"
echo

# Check if Arrow source exists
if [ ! -d "$ARROW_SOURCE_DIR" ]; then
    echo -e "${RED}ERROR: Arrow source not found at $ARROW_SOURCE_DIR${NC}"
    echo "Run: git submodule update --init --recursive"
    exit 1
fi

# Check for required build tools
echo "Checking build dependencies..."
if ! command -v cmake &> /dev/null; then
    echo -e "${RED}ERROR: CMake not found. Install: brew install cmake${NC}"
    exit 1
fi

if ! command -v c++ &> /dev/null; then
    echo -e "${RED}ERROR: C++ compiler not found.${NC}"
    exit 1
fi

echo -e "${GREEN}✅ Build tools OK${NC}"
echo

# Create build directory
mkdir -p "$ARROW_BUILD_DIR"
cd "$ARROW_BUILD_DIR"

# Configure Arrow build
echo "Configuring Arrow build (this takes ~5 minutes)..."
echo

# Minimal Arrow build - only what we need for Sabot
cmake "$ARROW_SOURCE_DIR" \
    -DCMAKE_BUILD_TYPE=Release \
    -DCMAKE_INSTALL_PREFIX="$ARROW_INSTALL_DIR" \
    -DARROW_BUILD_STATIC=OFF \
    -DARROW_BUILD_SHARED=ON \
    -DARROW_COMPUTE=ON \
    -DARROW_CSV=ON \
    -DARROW_DATASET=ON \
    -DARROW_FILESYSTEM=ON \
    -DARROW_FLIGHT=ON \
    -DARROW_IPC=ON \
    -DARROW_JSON=ON \
    -DARROW_PARQUET=ON \
    -DARROW_PYTHON=ON \
    -DARROW_WITH_ZLIB=ON \
    -DARROW_WITH_ZSTD=ON \
    -DARROW_WITH_LZ4=ON \
    -DARROW_WITH_SNAPPY=ON \
    -DARROW_WITH_BROTLI=ON \
    -DARROW_S3=OFF \
    -DARROW_GCS=OFF \
    -DARROW_HDFS=OFF \
    -DARROW_ORC=OFF \
    -DARROW_CUDA=OFF \
    -DARROW_GANDIVA=OFF \
    -DARROW_BUILD_TESTS=OFF \
    -DARROW_BUILD_BENCHMARKS=OFF \
    -DARROW_BUILD_EXAMPLES=OFF \
    -DARROW_BUILD_UTILITIES=ON \
    -DARROW_DEPENDENCY_SOURCE=AUTO

echo
echo -e "${GREEN}✅ Configuration complete${NC}"
echo

# Build Arrow (this takes 20-60 minutes depending on system)
echo "Building Arrow C++ (this takes 20-60 minutes)..."
echo "Using $(nproc 2>/dev/null || sysctl -n hw.ncpu 2>/dev/null || echo 4) parallel jobs"
echo

cmake --build . --config Release --parallel $(nproc 2>/dev/null || sysctl -n hw.ncpu 2>/dev/null || echo 4)

echo
echo -e "${GREEN}✅ Build complete${NC}"
echo

# Install Arrow to local prefix
echo "Installing Arrow to $ARROW_INSTALL_DIR..."
cmake --install .

echo
echo -e "${GREEN}✅ Installation complete${NC}"
echo

# Verify installation
echo "Verifying installation..."
if [ -f "$ARROW_INSTALL_DIR/lib/libarrow.dylib" ] || [ -f "$ARROW_INSTALL_DIR/lib/libarrow.so" ]; then
    echo -e "${GREEN}✅ libarrow found${NC}"
else
    echo -e "${RED}ERROR: libarrow not found in $ARROW_INSTALL_DIR/lib${NC}"
    exit 1
fi

if [ -f "$ARROW_INSTALL_DIR/lib/libarrow_flight.dylib" ] || [ -f "$ARROW_INSTALL_DIR/lib/libarrow_flight.so" ]; then
    echo -e "${GREEN}✅ libarrow_flight found${NC}"
else
    echo -e "${YELLOW}⚠️  libarrow_flight not found (Flight may not be available)${NC}"
fi

if [ -d "$ARROW_INSTALL_DIR/include/arrow" ]; then
    echo -e "${GREEN}✅ Arrow headers found${NC}"
else
    echo -e "${RED}ERROR: Arrow headers not found in $ARROW_INSTALL_DIR/include${NC}"
    exit 1
fi

# Print summary
echo
echo "=========================================="
echo -e "${GREEN}Arrow C++ Build Complete!${NC}"
echo "=========================================="
echo
echo "Libraries:   $ARROW_INSTALL_DIR/lib"
echo "Headers:     $ARROW_INSTALL_DIR/include"
echo
echo "Next steps:"
echo "1. Rebuild Sabot: uv pip install -e . --force-reinstall --no-deps"
echo "2. Verify: python -c 'from sabot import cyarrow; print(cyarrow.USING_ZERO_COPY)'"
echo
