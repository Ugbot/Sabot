#!/usr/bin/env python3
"""
Setup script for SabotSQL Cython bindings
"""

from setuptools import setup, Extension
from Cython.Build import cythonize
import pyarrow as pa
import os

# Get Arrow paths
arrow_include_dir = pa.get_include()
arrow_lib_dir = os.path.join(os.path.dirname(pa.__file__), 'lib')

# SabotSQL include directory
sabot_sql_include_dir = os.path.join(os.path.dirname(__file__), 'include')

# Define the extension
extensions = [
    Extension(
        "sabot_sql",
        ["sabot_sql_simple.pyx"],
        include_dirs=[
            sabot_sql_include_dir,
            arrow_include_dir,
            "/Users/bengamble/Sabot/vendor/arrow/cpp/build/install/include"
        ],
        libraries=["sabot_sql", "arrow"],
        library_dirs=[
            "/Users/bengamble/Sabot/sabot_sql/build",
            "/Users/bengamble/Sabot/vendor/arrow/cpp/build/install/lib"
        ],
        language="c++",
        extra_compile_args=["-std=c++20"],
        extra_link_args=["-std=c++20"]
    )
]

setup(
    name="sabot_sql",
    version="0.1.0",
    description="SabotSQL Cython bindings for agent-based distributed execution",
    ext_modules=cythonize(extensions, language_level="3"),
    zip_safe=False,
)
