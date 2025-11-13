#!/usr/bin/env python3
"""
Build script for Sabot Storage Shim C++ library and Cython bindings.

Builds:
1. C++ storage shim library (libsabot_storage.a)
2. Cython bindings for the shim
"""

import sys
import os
from pathlib import Path
from setuptools import setup, Extension
from Cython.Build import cythonize
import pyarrow as pa
import numpy as np

# Project paths
SABOT_ROOT = Path(__file__).parent
STORAGE_DIR = SABOT_ROOT / "sabot" / "storage"
CYTHON_DIR = SABOT_ROOT / "sabot" / "_cython"
ARROW_ROOT = SABOT_ROOT / "vendor" / "arrow" / "cpp" / "build" / "install"
MARBLE_ROOT = SABOT_ROOT / "MarbleDB"

# Include directories
include_dirs = [
    str(SABOT_ROOT),  # Root for sabot/storage/interface.h
    str(SABOT_ROOT / "sabot"),
    str(STORAGE_DIR),
    str(CYTHON_DIR / "storage"),
    str(ARROW_ROOT / "include"),
    str(MARBLE_ROOT / "include"),
    pa.get_include(),
    np.get_include(),
]

# Library directories
library_dirs = [
    str(ARROW_ROOT / "lib") if ARROW_ROOT.exists() else "/opt/homebrew/lib",
    str(MARBLE_ROOT / "build"),
    str(STORAGE_DIR / "build"),  # Where we'll build libsabot_storage.a
]

# Libraries to link
libraries = [
    "sabot_storage",  # Our shim library
    "arrow",
    "marble",  # MarbleDB library is libmarble.a, not libmarbledb.a
]

# Runtime library paths
runtime_library_dirs = [
    str(ARROW_ROOT / "lib"),
    str(MARBLE_ROOT / "build"),
    str(STORAGE_DIR / "build"),
]

# Extension definitions
extensions = [
    Extension(
        name="sabot._cython.storage.storage_shim",
        sources=[
            str(CYTHON_DIR / "storage" / "storage_shim.pyx"),
        ],
        include_dirs=include_dirs,
        library_dirs=library_dirs,
        libraries=libraries,
        runtime_library_dirs=runtime_library_dirs,
        language="c++",
        extra_compile_args=["-std=c++17", "-O3", "-Wall"],
        extra_link_args=[],
    ),
]

if __name__ == "__main__":
    print("Building Sabot Storage Shim Cython bindings...")
    print(f"Include dirs: {include_dirs}")
    print(f"Library dirs: {library_dirs}")
    print(f"Libraries: {libraries}")
    
    setup(
        name="sabot_storage_shim",
        ext_modules=cythonize(
            extensions,
            compiler_directives={
                "language_level": "3",
                "embedsignature": True,
                "boundscheck": False,
                "wraparound": False,
            }
        ),
        script_args=["build_ext", "--inplace"],
    )

