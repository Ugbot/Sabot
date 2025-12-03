# -*- coding: utf-8 -*-
"""
Build script for streaming_groupby.pyx

Compiles the StreamingGroupBy Cython module.
"""

import os
from setuptools import setup, Extension
from Cython.Build import cythonize

# Get the directory containing this script
BUILD_DIR = os.path.dirname(os.path.abspath(__file__))

# Extension definition - pure Cython, no C++ deps
extensions = [
    Extension(
        "streaming_groupby",
        [os.path.join(BUILD_DIR, "streaming_groupby.pyx")],
        language="c",
        extra_compile_args=["-O3"],
    )
]

if __name__ == "__main__":
    original_dir = os.getcwd()
    os.chdir(BUILD_DIR)

    try:
        setup(
            name="streaming_groupby",
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
