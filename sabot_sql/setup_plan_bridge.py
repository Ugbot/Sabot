"""
Build script for plan_bridge Cython extension.

Provides Python-accessible SQLPlanner class that uses:
- DuckDB C++ for SQL parsing/optimization
- C++ StagePartitioner for distributed plan creation
- C++ PlanSerializer for Cython-accessible serialization
"""

from setuptools import setup, Extension
from Cython.Build import cythonize
import os
import sys

# Paths
SABOT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
ARROW_ROOT = os.path.join(SABOT_ROOT, "vendor/arrow/cpp/build/install")
DUCKDB_ROOT = os.path.join(SABOT_ROOT, "vendor/duckdb")
SABOT_SQL_ROOT = os.path.dirname(os.path.abspath(__file__))

# Include directories
include_dirs = [
    os.path.join(SABOT_SQL_ROOT, "include"),
    os.path.join(ARROW_ROOT, "include"),
    os.path.join(DUCKDB_ROOT, "src/include"),
]

# Library directories
library_dirs = [
    os.path.join(SABOT_SQL_ROOT, "build"),
    os.path.join(ARROW_ROOT, "lib"),
    os.path.join(DUCKDB_ROOT, "build/release/src"),
]

# Libraries
libraries = [
    "sabot_sql",
    "arrow",
    "duckdb",
]

# Runtime library path
extra_link_args = [
    f"-Wl,-rpath,{os.path.join(SABOT_SQL_ROOT, 'build')}",
    f"-Wl,-rpath,{os.path.join(ARROW_ROOT, 'lib')}",
    f"-Wl,-rpath,{os.path.join(DUCKDB_ROOT, 'build/release/src')}",
]

# Extension
extensions = [
    Extension(
        "plan_bridge",
        sources=["plan_bridge.pyx"],
        language="c++",
        include_dirs=include_dirs,
        library_dirs=library_dirs,
        libraries=libraries,
        extra_compile_args=["-std=c++17", "-O3"],
        extra_link_args=extra_link_args,
    )
]

setup(
    name="plan_bridge",
    ext_modules=cythonize(
        extensions,
        language_level="3",
        compiler_directives={
            "boundscheck": False,
            "wraparound": False,
        },
    ),
)
