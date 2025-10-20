#!/usr/bin/env python3
"""
Quick setup script for query optimizer Cython module

This builds just the optimizer_bridge module with correct include paths.
"""

from setuptools import setup, Extension
from Cython.Build import cythonize
import numpy as np
import os

# Paths
sabot_core_include = os.path.join(os.path.dirname(__file__), 'sabot_core', 'include')
arrow_include = os.path.join(os.path.dirname(__file__), 'vendor', 'arrow', 'cpp', 'build', 'install', 'include')
sabot_core_lib = os.path.join(os.path.dirname(__file__), 'sabot_core', 'build', 'src', 'query')

# Extension for optimizer_bridge
optimizer_ext = Extension(
    'sabot._cython.query.optimizer_bridge',
    ['sabot/_cython/query/optimizer_bridge.pyx'],
    include_dirs=[
        sabot_core_include,
        arrow_include,
        np.get_include(),
    ],
    library_dirs=[sabot_core_lib],
    libraries=['sabot_query'],  # Link to our C++ library
    language='c++',
    extra_compile_args=['-std=c++17', '-O3'],
)

# Build
setup(
    name='sabot-query-optimizer',
    ext_modules=cythonize([optimizer_ext], compiler_directives={'language_level': '3'}),
)

print("✅ Query optimizer module built successfully!")

