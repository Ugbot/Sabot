#!/usr/bin/env python3
"""Build the hash_join module with cross-platform SIMD support."""

import sys
import os
from pathlib import Path
from setuptools import setup, Extension
from Cython.Build import cythonize
import numpy as np

# Import cross-platform build utilities
from build_utils import get_compile_args, print_platform_info

# Print platform info for debugging
print_platform_info()

# Module to build
module_path = "sabot/cyarrow/hash_join.pyx"

# Arrow paths
arrow_include = "/Users/bengamble/Sabot/vendor/arrow/cpp/build/install/include"
arrow_lib = "/Users/bengamble/Sabot/vendor/arrow/cpp/build/install/lib"

# sabot_ql paths
sabot_ql_include = str(Path("sabot_ql/include"))
sabot_ql_lib = str(Path("sabot_ql/build"))

# MarbleDB paths (sabot_ql depends on MarbleDB)
marbledb_include = str(Path("MarbleDB/include"))
marbledb_lib = str(Path("MarbleDB/build"))

# NumPy include
numpy_include = np.get_include()

# Include directories
include_dirs = [
    numpy_include,
    arrow_include,
    sabot_ql_include,
    marbledb_include,
    str(Path("vendor/arrow/python")),
    str(Path("vendor/arrow/python/pyarrow")),
    str(Path("vendor/arrow/python/pyarrow/src")),
]

# Library directories
library_dirs = [
    arrow_lib,
    sabot_ql_lib,
    marbledb_lib,
]

# Libraries to link
libraries = [
    "arrow",
    "sabot_ql",
    "marble",
]

# Runtime library search paths
rpath_args = [
    f"-Wl,-rpath,{arrow_lib}",
    f"-Wl,-rpath,{sabot_ql_lib}",
    f"-Wl,-rpath,{marbledb_lib}",
]

# Get platform-appropriate SIMD compile args
base_args = ["-std=c++17", "-Wno-unused-function", "-Wno-deprecated-declarations"]
compile_args = get_compile_args(base_args)

print(f"\nCompile args: {' '.join(compile_args)}")

# Create extension
ext = Extension(
    "sabot.cyarrow.hash_join",
    sources=[module_path],
    include_dirs=include_dirs,
    library_dirs=library_dirs,
    libraries=libraries,
    language="c++",
    extra_compile_args=compile_args,
    extra_link_args=rpath_args,
)

# Build
print("Building hash_join module (zero-copy sabot_ql wrapper)...")
print(f"  Source: {module_path}")
print(f"  sabot_ql library: {sabot_ql_lib}/libsabot_ql.dylib")
print(f"  MarbleDB library: {marbledb_lib}/libmarble.a")
print(f"  Arrow library: {arrow_lib}")

setup(
    name="hash_join",
    ext_modules=cythonize([ext], compiler_directives={"language_level": "3"}),
)

print("✅ Build complete!")
print("   Module: sabot.cyarrow.hash_join")
print("   Usage: from sabot.cyarrow.hash_join import hash_join")
