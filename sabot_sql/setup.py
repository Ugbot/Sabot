#!/usr/bin/env python3
"""
Setup script for SabotSQL Cython bindings
"""

from setuptools import setup, Extension
from Cython.Build import cythonize
import os

# Base paths
SABOT_ROOT = os.path.dirname(os.path.dirname(__file__))
SABOT_SQL_DIR = os.path.dirname(__file__)

# Vendored library paths
ARROW_ROOT = os.path.join(SABOT_ROOT, "vendor/arrow/cpp/build/install")
DUCKDB_ROOT = os.path.join(SABOT_ROOT, "vendor/duckdb")

# Include directories
include_dirs = [
    os.path.join(SABOT_SQL_DIR, "include"),
    os.path.join(ARROW_ROOT, "include"),
    os.path.join(DUCKDB_ROOT, "src/include"),
]

# Library directories
library_dirs = [
    os.path.join(SABOT_SQL_DIR, "build"),
    os.path.join(ARROW_ROOT, "lib"),
    os.path.join(DUCKDB_ROOT, "build/release/src"),
]

# Common compile/link args
compile_args = ["-std=c++17", "-O3"]
link_args = ["-std=c++17"]

# macOS rpath for vendored libraries
if os.uname().sysname == 'Darwin':
    link_args.extend([
        f"-Wl,-rpath,{os.path.join(ARROW_ROOT, 'lib')}",
        f"-Wl,-rpath,{os.path.join(SABOT_SQL_DIR, 'build')}",
        f"-Wl,-rpath,{os.path.join(DUCKDB_ROOT, 'build/release/src')}",
    ])

# Define extensions
extensions = [
    # Simple SQL bridge (uses sabot_sql lib)
    Extension(
        "sabot_sql_ext",
        ["sabot_sql_simple.pyx"],
        include_dirs=include_dirs,
        libraries=["sabot_sql", "arrow"],
        library_dirs=library_dirs,
        language="c++",
        extra_compile_args=compile_args,
        extra_link_args=link_args
    ),
    # Plan Bridge (DuckDB plan parsing and stage partitioning)
    Extension(
        "plan_bridge",
        ["plan_bridge.pyx"],
        include_dirs=include_dirs,
        libraries=["sabot_sql", "arrow", "duckdb"],
        library_dirs=library_dirs,
        language="c++",
        extra_compile_args=compile_args,
        extra_link_args=link_args
    ),
]

setup(
    name="sabot_sql",
    version="0.1.0",
    description="SabotSQL Cython bindings for agent-based distributed execution",
    ext_modules=cythonize(extensions, language_level="3"),
    zip_safe=False,
)
