#!/usr/bin/env python3
"""
Build script for MarbleDB Cython bindings.

Builds all MarbleDB-related Cython modules:
- State backend (marbledb_backend.pyx)
- Store backend (marbledb_store.pyx)
- Window state (marbledb_window_state.pyx)
- Join buffer (marbledb_join_buffer.pyx)
- Dimension tables (marbledb_dimension.pyx)
"""

import sys
import os
from pathlib import Path
from setuptools import setup, Extension
from Cython.Build import cythonize
import pyarrow as pa

# Project paths
SABOT_ROOT = Path(__file__).parent
CYTHON_DIR = SABOT_ROOT / "sabot" / "_cython"
ARROW_ROOT = SABOT_ROOT / "vendor" / "arrow" / "cpp" / "build" / "install"
MARBLE_ROOT = SABOT_ROOT / "MarbleDB"

# Include directories
include_dirs = [
    str(CYTHON_DIR / "state"),
    str(CYTHON_DIR / "stores"),
    str(ARROW_ROOT / "include"),
    str(MARBLE_ROOT / "include"),
    pa.get_include(),
]

# Library directories
library_dirs = [
    str(ARROW_ROOT / "lib"),
    str(MARBLE_ROOT / "build"),
]

# Libraries to link
libraries = [
    "arrow",
    "marbledb",
]

# Runtime library paths
runtime_library_dirs = [
    str(ARROW_ROOT / "lib"),
    str(MARBLE_ROOT / "build"),
]

# Extension definitions
extensions = [
    Extension(
        name="sabot._cython.state.marbledb_backend",
        sources=[str(CYTHON_DIR / "state" / "marbledb_backend.pyx")],
        include_dirs=include_dirs,
        library_dirs=library_dirs,
        libraries=libraries,
        runtime_library_dirs=runtime_library_dirs,
        language="c++",
        extra_compile_args=["-std=c++17", "-O3", "-Wall"],
        extra_link_args=[],
    ),
    Extension(
        name="sabot._cython.stores.marbledb_store",
        sources=[str(CYTHON_DIR / "stores" / "marbledb_store.pyx")],
        include_dirs=include_dirs,
        library_dirs=library_dirs,
        libraries=libraries,
        runtime_library_dirs=runtime_library_dirs,
        language="c++",
        extra_compile_args=["-std=c++17", "-O3", "-Wall"],
        extra_link_args=[],
    ),
    Extension(
        name="sabot._cython.windows.marbledb_window_state",
        sources=[str(CYTHON_DIR / "windows" / "marbledb_window_state.pyx")],
        include_dirs=include_dirs,
        library_dirs=library_dirs,
        libraries=libraries,
        runtime_library_dirs=runtime_library_dirs,
        language="c++",
        extra_compile_args=["-std=c++17", "-O3", "-Wall"],
        extra_link_args=[],
    ),
    Extension(
        name="sabot._cython.joins.marbledb_join_buffer",
        sources=[str(CYTHON_DIR / "joins" / "marbledb_join_buffer.pyx")],
        include_dirs=include_dirs,
        library_dirs=library_dirs,
        libraries=libraries,
        runtime_library_dirs=runtime_library_dirs,
        language="c++",
        extra_compile_args=["-std=c++17", "-O3", "-Wall"],
        extra_link_args=[],
    ),
    Extension(
        name="sabot._cython.materializations.marbledb_dimension",
        sources=[str(CYTHON_DIR / "materializations" / "marbledb_dimension.pyx")],
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
    setup(
        name="sabot_marbledb",
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

