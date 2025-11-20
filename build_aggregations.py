#!/usr/bin/env python3
"""Build aggregations module for Python 3.13"""

from setuptools import setup, Extension
from Cython.Build import cythonize
import numpy as np
from pathlib import Path

# Arrow paths
arrow_include = "/Users/bengamble/Sabot/vendor/arrow/cpp/build/install/include"
arrow_lib = "/Users/bengamble/Sabot/vendor/arrow/cpp/build/install/lib"

# Include directories
include_dirs = [
    np.get_include(),
    arrow_include,
    str(Path("vendor/arrow/python")),
    str(Path("vendor/arrow/python/pyarrow")),
    str(Path("vendor/arrow/python/pyarrow/src")),
    str(Path("sabot/_c")),
]

# Create extension
ext = Extension(
    "sabot._cython.operators.aggregations",
    sources=["sabot/_cython/operators/aggregations.pyx"],
    include_dirs=include_dirs,
    library_dirs=[arrow_lib],
    libraries=["arrow"],
    language="c++",
    extra_compile_args=["-std=c++17", "-O3", "-Wno-unused-function", "-Wno-deprecated-declarations"],
    extra_link_args=[f"-Wl,-rpath,{arrow_lib}"],
)

# Build
setup(
    name="aggregations",
    ext_modules=cythonize([ext], compiler_directives={"language_level": "3"}),
    script_args=['build_ext', '--inplace'],
)
