#!/usr/bin/env python3
"""
Build script for Sabot - builds vendored Apache Arrow C++ before Cython modules.

This script is called during `pip install` to ensure Arrow C++ is built first.
Compatible with Windows, Linux, and macOS.
"""

import os
import sys
import subprocess
import shutil
from pathlib import Path


def find_executable(name, alternatives=None):
    """Find executable in PATH."""
    alternatives = alternatives or []
    for exe_name in [name] + alternatives:
        if shutil.which(exe_name):
            return exe_name
    return None


def build_arrow_cpp():
    """Build Apache Arrow C++ library from vendored source."""

    print("=" * 60)
    print("Building Apache Arrow C++ Library")
    print("=" * 60)

    # Get paths
    project_root = Path(__file__).parent
    arrow_source_dir = project_root / "vendor" / "arrow" / "cpp"
    arrow_build_dir = arrow_source_dir / "build"
    arrow_install_dir = arrow_build_dir / "install"

    print(f"Source:  {arrow_source_dir}")
    print(f"Build:   {arrow_build_dir}")
    print(f"Install: {arrow_install_dir}")
    print()

    # Check if Arrow source exists
    if not arrow_source_dir.exists():
        print("ERROR: Arrow source not found at", arrow_source_dir)
        print("Run: git submodule update --init --recursive")
        sys.exit(1)

    # Check if already built
    lib_extensions = [".so", ".dylib", ".dll", ".a"]
    arrow_lib_files = [arrow_install_dir / "lib" / f"libarrow{ext}" for ext in lib_extensions]

    if any(f.exists() for f in arrow_lib_files):
        print("✅ Arrow C++ already built, skipping build")
        print()
        return arrow_install_dir

    # Check for CMake
    cmake = find_executable("cmake", ["cmake3"])
    if not cmake:
        print("ERROR: CMake not found. Install cmake:")
        if sys.platform == "win32":
            print("  - Download from https://cmake.org/download/")
        elif sys.platform == "darwin":
            print("  - brew install cmake")
        else:
            print("  - apt install cmake  (Debian/Ubuntu)")
            print("  - yum install cmake  (RHEL/CentOS)")
        sys.exit(1)

    print(f"✅ Found CMake: {cmake}")

    # Check for C++ compiler
    cxx_compilers = ["c++", "g++", "clang++", "cl.exe"]
    cxx = find_executable("CXX", cxx_compilers) or os.environ.get("CXX")
    if not cxx:
        cxx = find_executable(cxx_compilers[0], cxx_compilers[1:])

    if not cxx:
        print("ERROR: C++ compiler not found")
        sys.exit(1)

    print(f"✅ Found C++ compiler: {cxx}")
    print()

    # Create build directory
    arrow_build_dir.mkdir(parents=True, exist_ok=True)

    # Determine number of parallel jobs
    import multiprocessing
    num_jobs = multiprocessing.cpu_count()

    print(f"Configuring Arrow build (using {num_jobs} parallel jobs)...")
    print()

    # CMake configuration
    cmake_args = [
        cmake,
        str(arrow_source_dir),
        f"-DCMAKE_INSTALL_PREFIX={arrow_install_dir}",
        "-DCMAKE_BUILD_TYPE=Release",
        "-DARROW_BUILD_STATIC=OFF",
        "-DARROW_BUILD_SHARED=ON",
        "-DARROW_COMPUTE=ON",
        "-DARROW_CSV=ON",
        "-DARROW_DATASET=ON",
        "-DARROW_FILESYSTEM=ON",
        "-DARROW_FLIGHT=ON",
        "-DARROW_IPC=ON",
        "-DARROW_JSON=ON",
        "-DARROW_PARQUET=ON",
        "-DARROW_PYTHON=ON",
        "-DARROW_WITH_ZLIB=ON",
        "-DARROW_WITH_ZSTD=ON",
        "-DARROW_WITH_LZ4=ON",
        "-DARROW_WITH_SNAPPY=ON",
        "-DARROW_WITH_BROTLI=ON",
        "-DARROW_S3=OFF",
        "-DARROW_GCS=OFF",
        "-DARROW_HDFS=OFF",
        "-DARROW_ORC=OFF",
        "-DARROW_CUDA=OFF",
        "-DARROW_GANDIVA=OFF",
        "-DARROW_BUILD_TESTS=OFF",
        "-DARROW_BUILD_BENCHMARKS=OFF",
        "-DARROW_BUILD_EXAMPLES=OFF",
        "-DARROW_BUILD_UTILITIES=ON",
        "-DARROW_DEPENDENCY_SOURCE=AUTO",
    ]

    # Run CMake configure
    try:
        subprocess.run(cmake_args, cwd=arrow_build_dir, check=True)
    except subprocess.CalledProcessError as e:
        print(f"ERROR: CMake configuration failed: {e}")
        sys.exit(1)

    print()
    print("✅ Configuration complete")
    print()

    # Build Arrow
    print(f"Building Arrow C++ (this takes 20-60 minutes on first build)...")
    print()

    build_args = [
        cmake,
        "--build", ".",
        "--config", "Release",
        "--parallel", str(num_jobs),
    ]

    try:
        subprocess.run(build_args, cwd=arrow_build_dir, check=True)
    except subprocess.CalledProcessError as e:
        print(f"ERROR: Arrow build failed: {e}")
        sys.exit(1)

    print()
    print("✅ Build complete")
    print()

    # Install Arrow
    print(f"Installing Arrow to {arrow_install_dir}...")
    install_args = [cmake, "--install", "."]

    try:
        subprocess.run(install_args, cwd=arrow_build_dir, check=True)
    except subprocess.CalledProcessError as e:
        print(f"ERROR: Arrow installation failed: {e}")
        sys.exit(1)

    print()
    print("✅ Installation complete")
    print()

    # Verify installation
    print("Verifying installation...")
    if not any(f.exists() for f in arrow_lib_files):
        print("ERROR: Arrow library not found after build")
        sys.exit(1)

    print("✅ Arrow C++ library built successfully")
    print()

    return arrow_install_dir


if __name__ == "__main__":
    # Build Arrow when run directly
    build_arrow_cpp()
    print("=" * 60)
    print("Arrow C++ Build Complete!")
    print("=" * 60)
    print()
    print("Next steps:")
    print("1. Install Sabot: pip install -e .")
    print("2. Verify: python -c 'from sabot import cyarrow; print(cyarrow.USING_ZERO_COPY)'")
    print()
