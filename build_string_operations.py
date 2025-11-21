#!/usr/bin/env python3
"""Build string_operations Cython module."""

import sys
import os
from pathlib import Path
from setuptools import setup, Extension
from Cython.Build import cythonize
import numpy as np

# Module to build
module_path = "sabot/_cython/arrow/string_operations.pyx"

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
    str(Path("sabot/_cython/arrow")),
]

# Create extension
ext = Extension(
    "sabot._cython.arrow.string_operations",
    sources=[module_path],
    include_dirs=include_dirs,
    library_dirs=[arrow_lib],
    libraries=["arrow"],
    language="c++",
    extra_compile_args=["-std=c++17", "-O3", "-Wno-unused-function", "-Wno-deprecated-declarations"],
    extra_link_args=[f"-Wl,-rpath,{arrow_lib}"],
)

# Build
setup(
    name="string_operations",
    ext_modules=cythonize([ext], compiler_directives={"language_level": "3"}),
)
