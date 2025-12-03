# -*- coding: utf-8 -*-
"""
Build script for spillable_buffer.pyx

Compiles the SpillableBuffer Cython module to a shared library.
SpillableBuffer uses marbledb Python bindings, so no C++ dependencies needed.
"""

import os
import sys
from setuptools import setup, Extension
from Cython.Build import cythonize

# Get the directory containing this script
BUILD_DIR = os.path.dirname(os.path.abspath(__file__))

# Extension definition - pure Python/Cython, no C++ deps
extensions = [
    Extension(
        "spillable_buffer",
        [os.path.join(BUILD_DIR, "spillable_buffer.pyx")],
        language="c",  # Pure Cython, no C++
        extra_compile_args=["-O3"],
    )
]

# Build
if __name__ == "__main__":
    # Change to build directory for output
    original_dir = os.getcwd()
    os.chdir(BUILD_DIR)

    try:
        setup(
            name="spillable_buffer",
            ext_modules=cythonize(
                extensions,
                compiler_directives={
                    'language_level': 3,
                    'embedsignature': True,
                    'boundscheck': False,
                    'wraparound': False,
                },
                build_dir=BUILD_DIR,
            ),
            script_args=['build_ext', '--inplace'],
        )
        print("Build successful!")
    finally:
        os.chdir(original_dir)
