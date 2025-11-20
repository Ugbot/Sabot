#!/usr/bin/env python3
"""
Build script for Sabot datetime kernels with direct C++ bindings.

This builds a Cython extension that calls vendored Arrow C++ directly,
bypassing PyArrow's Python API.
"""

import os
import sys
from pathlib import Path
from setuptools import Extension
from Cython.Build import cythonize
import pyarrow as pa

# Paths
SABOT_ROOT = Path(__file__).parent.resolve()
ARROW_CPP = SABOT_ROOT / "vendor/arrow/cpp"
ARROW_BUILD = ARROW_CPP / "build"
ARROW_INSTALL = ARROW_BUILD / "install"
CPP_DATETIME = SABOT_ROOT / "vendor/cpp-datetime"

# Verify vendored Arrow is built
arrow_lib = ARROW_INSTALL / "lib/libarrow_compute.dylib"
if not arrow_lib.exists():
    print(f"ERROR: Vendored Arrow not found at {arrow_lib}")
    print("Build Arrow first: cd vendor/arrow/cpp/build && cmake .. && make install")
    sys.exit(1)

# Include directories
include_dirs = [
    str(ARROW_INSTALL / "include"),          # Vendored Arrow C++ headers
    str(CPP_DATETIME / "src"),               # cpp-datetime headers
    str(pa.get_include()),                   # PyArrow Cython includes
]

# Library directories
library_dirs = [
    str(ARROW_INSTALL / "lib"),              # Vendored Arrow libraries
]

# Libraries to link
libraries = [
    "arrow",           # Core Arrow
    "arrow_compute",   # Arrow compute (contains our custom kernels)
]

# Compiler flags
extra_compile_args = [
    "-std=c++17",
    "-O3",
    "-march=native",     # Use CPU-specific optimizations
    "-DARROW_NO_DEPRECATED_API",
]

# Linker flags (rpath for finding vendored Arrow at runtime)
extra_link_args = [
    f"-Wl,-rpath,{ARROW_INSTALL / 'lib'}",
]

# Extension definition
extension = Extension(
    name="sabot._cython.arrow.datetime_kernels_direct",
    sources=["sabot/_cython/arrow/datetime_kernels_direct.pyx"],
    include_dirs=include_dirs,
    library_dirs=library_dirs,
    libraries=libraries,
    extra_compile_args=extra_compile_args,
    extra_link_args=extra_link_args,
    language="c++",
)

# Build
print("=" * 70)
print("Building Sabot datetime kernels (direct C++ bindings)")
print("=" * 70)
print(f"Arrow C++:      {ARROW_INSTALL}")
print(f"cpp-datetime:   {CPP_DATETIME}")
print(f"Include dirs:   {include_dirs}")
print(f"Library dirs:   {library_dirs}")
print(f"Libraries:      {libraries}")
print("=" * 70)

# Cythonize and build
extensions = cythonize(
    [extension],
    compiler_directives={
        'language_level': '3',
        'embedsignature': True,
        'boundscheck': False,
        'wraparound': False,
    },
    annotate=True,  # Generate HTML annotation
)

# Build using setuptools
from setuptools import setup
setup(
    name="datetime_kernels_direct",
    ext_modules=extensions,
    zip_safe=False,
)

print("\n" + "=" * 70)
print("Build complete!")
print("=" * 70)
print(f"Module: sabot/_cython/arrow/datetime_kernels_direct.*.so")
print("Usage:  from sabot._cython.arrow import datetime_kernels_direct as dt")
print("=" * 70)
