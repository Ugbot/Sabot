#!/usr/bin/env python3
"""
Setup script for MarbleDB Python bindings.

Build with:
    python setup.py build_ext --inplace
"""

import os
import sys
from setuptools import setup, Extension
from Cython.Build import cythonize
import pyarrow as pa
import numpy as np

# Get Arrow paths from pyarrow
arrow_include = pa.get_include()
arrow_lib = pa.get_library_dirs()[0] if pa.get_library_dirs() else None
numpy_include = np.get_include()

# Paths relative to this file
MARBLEDB_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
SABOT_ROOT = os.path.abspath(os.path.join(MARBLEDB_ROOT, '..'))
ARROW_ROOT = os.path.join(SABOT_ROOT, 'vendor', 'arrow', 'cpp', 'build', 'install')

# Include directories
include_dirs = [
    os.path.join(MARBLEDB_ROOT, 'include'),  # MarbleDB headers
    os.path.join(ARROW_ROOT, 'include'),      # Arrow headers
    arrow_include,                             # PyArrow headers
    numpy_include,                             # NumPy headers
]

# Library directories
library_dirs = [
    os.path.join(MARBLEDB_ROOT, 'build'),     # libmarble.a
    os.path.join(ARROW_ROOT, 'lib'),          # Arrow libraries
]

if arrow_lib:
    library_dirs.append(arrow_lib)

# Libraries to link
libraries = [
    'arrow',         # Arrow C++ core
    'arrow_acero',   # Arrow Acero execution engine (for joins)
]

# Runtime library directories
runtime_library_dirs = [
    os.path.join(ARROW_ROOT, 'lib'),
]

# Extra objects (static libraries)
extra_objects = [
    os.path.join(MARBLEDB_ROOT, 'build', 'libmarble.a'),
]

# Compiler flags
extra_compile_args = [
    '-std=c++17',
    '-O3',
    '-DNDEBUG',
]

# Linker flags
extra_link_args = []

# Platform-specific settings
if sys.platform == 'darwin':
    # macOS
    extra_compile_args.extend([
        '-mmacosx-version-min=10.15',
    ])
    extra_link_args.extend([
        '-mmacosx-version-min=10.15',
    ])
elif sys.platform == 'linux':
    # Linux
    extra_link_args.extend([
        '-Wl,-rpath,$ORIGIN',
    ])

# Define the extension
extensions = [
    Extension(
        name='marbledb',
        sources=['marbledb.pyx'],
        include_dirs=include_dirs,
        library_dirs=library_dirs,
        libraries=libraries,
        runtime_library_dirs=runtime_library_dirs,
        extra_objects=extra_objects,
        extra_compile_args=extra_compile_args,
        extra_link_args=extra_link_args,
        language='c++',
    )
]

# Setup configuration
setup(
    name='marbledb',
    version='0.1.0',
    description='Python bindings for MarbleDB',
    author='Sabot Team',
    ext_modules=cythonize(
        extensions,
        compiler_directives={
            'language_level': '3',
            'embedsignature': True,
            'boundscheck': False,
            'wraparound': False,
        },
        annotate=True,  # Generate HTML annotation files
    ),
    install_requires=[
        'pyarrow>=10.0.0',
        'cython>=0.29.0',
    ],
    zip_safe=False,
)
