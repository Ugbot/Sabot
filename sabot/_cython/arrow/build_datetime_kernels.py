"""
Build script for datetime_kernels.pyx

Compiles the Cython datetime kernels module to a shared library.
"""

import os
import sys
from setuptools import setup, Extension
from Cython.Build import cythonize

# Use vendored Arrow paths
SABOT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../.."))
ARROW_INCLUDE = os.path.join(SABOT_ROOT, "vendor/arrow/cpp/build/install/include")
ARROW_LIB = os.path.join(SABOT_ROOT, "vendor/arrow/cpp/build/install/lib")

# Verify paths exist
if not os.path.exists(ARROW_INCLUDE):
    raise RuntimeError(f"Arrow include path not found: {ARROW_INCLUDE}")
if not os.path.exists(ARROW_LIB):
    raise RuntimeError(f"Arrow lib path not found: {ARROW_LIB}")

# Extension definition
extensions = [
    Extension(
        "datetime_kernels",
        ["datetime_kernels.pyx"],
        include_dirs=[ARROW_INCLUDE],
        library_dirs=[ARROW_LIB],
        libraries=["arrow", "arrow_compute"],
        runtime_library_dirs=[ARROW_LIB],
        language="c++",
        extra_compile_args=["-std=c++17", "-O3"],
        extra_link_args=[f"-Wl,-rpath,{ARROW_LIB}"],
    )
]

# Build
setup(
    name="datetime_kernels",
    ext_modules=cythonize(
        extensions,
        compiler_directives={
            'language_level': 3,
            'embedsignature': True,
        }
    ),
)
