#!/usr/bin/env python3
"""
Setup script for SabotCypher Cython module

Builds the C++ extension using Cython (like rest of Sabot).
"""

from setuptools import setup, Extension
from Cython.Build import cythonize
import numpy as np
import pyarrow as pa
import os

# Get Arrow paths
arrow_include = pa.get_include()
arrow_lib = pa.get_library_dirs()[0] if pa.get_library_dirs() else None

# SabotCypher paths
sabot_cypher_dir = os.path.dirname(os.path.abspath(__file__))
include_dirs = [
    f"{sabot_cypher_dir}/include",
    f"{sabot_cypher_dir}/vendored/sabot_cypher_core/src/include",
    arrow_include,
    np.get_include(),
]

library_dirs = [
    f"{sabot_cypher_dir}/build",
]

if arrow_lib:
    library_dirs.append(arrow_lib)

# Define extension
extensions = [
    Extension(
        "sabot_cypher_native",
        sources=["sabot_cypher.pyx"],
        include_dirs=include_dirs,
        library_dirs=library_dirs,
        libraries=["sabot_cypher", "arrow", "arrow_python"],
        language="c++",
        extra_compile_args=["-std=c++20", "-O3"],
        extra_link_args=["-Wl,-rpath,@loader_path"],
    )
]

setup(
    name="sabot_cypher",
    version="0.1.0",
    description="High-Performance Cypher Query Engine with Arrow Execution",
    ext_modules=cythonize(
        extensions,
        compiler_directives={
            'language_level': "3",
            'boundscheck': False,
            'wraparound': False,
        }
    ),
    python_requires='>=3.8',
    install_requires=[
        'pyarrow>=10.0.0',
        'numpy>=1.20.0',
        'cython>=0.29.0',
    ],
)

