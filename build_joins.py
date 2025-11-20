#!/usr/bin/env python3
"""Build the joins module with cross-platform SIMD support."""

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
module_path = "sabot/_cython/operators/joins.pyx"

# Arrow paths (matching build.py exactly)
arrow_include = "/Users/bengamble/Sabot/vendor/arrow/cpp/build/install/include"
arrow_lib = "/Users/bengamble/Sabot/vendor/arrow/cpp/build/install/lib"

# NumPy include
numpy_include = np.get_include()

# Include directories (matching build.py)
include_dirs = [
    numpy_include,
    arrow_include,
    str(Path("vendor/arrow/python")),
    str(Path("vendor/arrow/python/pyarrow")),
    str(Path("vendor/arrow/python/pyarrow/src")),
    str(Path("sabot/_c")),
    str(Path("sabot/_cython")),
    str(Path("sabot/_cython/operators")),
]

# Get platform-appropriate SIMD compile args
base_args = ["-std=c++17", "-Wno-unused-function", "-Wno-deprecated-declarations"]
compile_args = get_compile_args(base_args)

print(f"\nCompile args: {' '.join(compile_args)}")

# Create extension
ext = Extension(
    "sabot._cython.operators.joins",
    sources=[module_path],
    include_dirs=include_dirs,
    library_dirs=[arrow_lib],
    libraries=["arrow"],
    language="c++",
    extra_compile_args=compile_args,
    extra_link_args=[f"-Wl,-rpath,{arrow_lib}"],
)

# Build
print("Building joins module...")
setup(
    name="joins",
    ext_modules=cythonize([ext], compiler_directives={"language_level": "3"}),
)
print("✅ Build complete!")
