#!/bin/bash
# Build Arrow C++ from vendored source
set -e

ARROW_DIR="/Users/bengamble/PycharmProjects/pythonProject/sabot/third-party/arrow/cpp"
BUILD_DIR="$ARROW_DIR/build"
INSTALL_DIR="$BUILD_DIR/install"

echo "Building Arrow C++ (minimal configuration)..."
cd "$BUILD_DIR"

cmake "$ARROW_DIR" \
  -DCMAKE_BUILD_TYPE=Release \
  -DCMAKE_INSTALL_PREFIX="$INSTALL_DIR" \
  -DARROW_PYTHON=ON \
  -DARROW_BUILD_SHARED=ON \
  -DARROW_BUILD_STATIC=OFF \
  -DARROW_COMPUTE=ON \
  -DARROW_CSV=ON \
  -DARROW_DATASET=ON \
  -DARROW_FILESYSTEM=ON \
  -DARROW_FLIGHT=ON \
  -DARROW_PARQUET=OFF \
  -DARROW_WITH_BROTLI=OFF \
  -DARROW_WITH_BZ2=OFF \
  -DARROW_WITH_LZ4=OFF \
  -DARROW_WITH_SNAPPY=OFF \
  -DARROW_WITH_ZLIB=OFF \
  -DARROW_WITH_ZSTD=OFF

echo "Running make (this will take a few minutes)..."
make -j$(sysctl -n hw.ncpu)

echo "Installing Arrow C++..."
make install

echo "Arrow C++ build complete!"
echo "Libraries installed to: $INSTALL_DIR"
