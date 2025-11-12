#!/usr/bin/env python3
"""
Build script for sabot_ql graph Cython bindings.

Builds the graph.pyx module with CSR graph and property path operators.
"""

import sys
import os
from pathlib import Path
from setuptools import setup, Extension
from Cython.Build import cythonize
import pyarrow as pa

# Project paths
SABOT_ROOT = Path(__file__).parent.parent
SABOT_QL_ROOT = Path(__file__).parent
ARROW_ROOT = SABOT_ROOT / "vendor/arrow/cpp/build/install"
MARBLE_ROOT = SABOT_ROOT / "MarbleDB"

# Include directories
include_dirs = [
    str(SABOT_QL_ROOT / "include"),
    str(ARROW_ROOT / "include"),
    str(MARBLE_ROOT / "include"),
    pa.get_include(),
]

# Library directories
library_dirs = [
    str(SABOT_QL_ROOT / "build"),
    str(ARROW_ROOT / "lib"),
    str(MARBLE_ROOT / "build"),
]

# Libraries to link
libraries = [
    "sabot_ql",
    "arrow",
]

# Runtime library paths
runtime_library_dirs = [
    str(ARROW_ROOT / "lib"),
    str(SABOT_QL_ROOT / "build"),
]

# Extension definition
ext = Extension(
    name="sabot_ql.bindings.python.graph",
    sources=[str(SABOT_QL_ROOT / "bindings/python/graph.pyx")],
    include_dirs=include_dirs,
    library_dirs=library_dirs,
    libraries=libraries,
    runtime_library_dirs=runtime_library_dirs,
    language="c++",
    extra_compile_args=["-std=c++20", "-O3"],
    extra_link_args=["-Wl,-rpath,@loader_path/../../../vendor/arrow/cpp/build/install/lib"],
)

if __name__ == "__main__":
    setup(
        name="sabot_ql_graph",
        ext_modules=cythonize(
            [ext],
            compiler_directives={
                "language_level": "3",
                "embedsignature": True,
            }
        ),
        script_args=["build_ext", "--inplace"],
    )
