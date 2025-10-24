#!/usr/bin/env python3
"""
Setup script for SabotQL Python bindings
"""

import os
import sys
from pathlib import Path
from setuptools import setup, Extension
from Cython.Build import cythonize
import subprocess
import sysconfig
import numpy

# Get paths
# This file is at: Sabot/sabot_ql/bindings/python/setup_sabot_ql.py
# So REPO_ROOT is 4 levels up to get to Sabot/
REPO_ROOT = Path(__file__).resolve().parent.parent.parent.parent
SABOT_QL_ROOT = REPO_ROOT / "sabot_ql"
MARBLE_ROOT = REPO_ROOT / "MarbleDB"
ARROW_ROOT = REPO_ROOT / "vendor" / "arrow" / "cpp" / "build" / "install"
ARROW_PYTHON_SRC = REPO_ROOT / "vendor" / "arrow" / "python" / "pyarrow" / "src"

# Include directories
include_dirs = [
    str(SABOT_QL_ROOT / "include"),
    str(MARBLE_ROOT / "include"),
    str(ARROW_ROOT / "include"),
    str(ARROW_PYTHON_SRC),  # For arrow/python headers
    sysconfig.get_path('include'),  # Python headers
    numpy.get_include(),  # NumPy headers
]

# Library directories
library_dirs = [
    str(SABOT_QL_ROOT / "build"),
    str(MARBLE_ROOT / "build"),
    str(ARROW_ROOT / "lib"),
]

# Libraries to link
libraries = [
    "sabot_ql",
    "marble",
    "arrow",
]

# Compiler flags
extra_compile_args = [
    "-std=c++17",
    "-O3",
    "-DNDEBUG",
]

# Linker flags
extra_link_args = [
    f"-Wl,-rpath,{ARROW_ROOT / 'lib'}",
    f"-Wl,-rpath,{SABOT_QL_ROOT / 'build'}",
]

# Define extension
ext_modules = [
    Extension(
        name="sabot_ql_native",
        sources=["sabot_ql_impl.pyx"],
        include_dirs=include_dirs,
        library_dirs=library_dirs,
        libraries=libraries,
        language="c++",
        extra_compile_args=extra_compile_args,
        extra_link_args=extra_link_args,
    )
]

# Build
setup(
    name="sabot_ql_native",
    version="0.1.0",
    description="SabotQL Python bindings (Cython)",
    ext_modules=cythonize(
        ext_modules,
        compiler_directives={
            "language_level": "3",
            "boundscheck": False,
            "wraparound": False,
        },
        include_path=[str(Path(__file__).parent)],
    ),
    zip_safe=False,
)
