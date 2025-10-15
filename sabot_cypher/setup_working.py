#!/usr/bin/env python3
"""
Simple setup for SabotCypher Cython module.
Builds basic wrapper while we work on full FFI.
"""

from setuptools import setup, Extension
from Cython.Build import cythonize
import numpy as np
import pyarrow as pa

# Get Arrow paths
arrow_include = pa.get_include()

extensions = [
    Extension(
        "sabot_cypher_working",
        sources=["sabot_cypher_working.pyx"],
        include_dirs=[
            "include",
            "vendored/sabot_cypher_core/src/include",
            arrow_include,
            np.get_include(),
        ],
        library_dirs=["build"],
        libraries=["sabot_cypher"],
        language="c++",
        extra_compile_args=["-std=c++20"],
        extra_link_args=["-Wl,-rpath,@loader_path/build"],
    )
]

setup(
    name="sabot_cypher_working",
    version="0.1.0",
    ext_modules=cythonize(
        extensions,
        compiler_directives={'language_level': "3"}
    ),
)

