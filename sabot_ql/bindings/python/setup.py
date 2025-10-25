"""Setup script for SabotQL Python bindings."""

from setuptools import setup, Extension
from Cython.Build import cythonize
import pyarrow as pa
import numpy as np

# Arrow include directories (use vendored arrow)
arrow_include_vendored = "../../../vendor/arrow/cpp/build/install/include"
arrow_lib = "../../../vendor/arrow/cpp/build/install/lib"

# PyArrow Python headers (for pyarrow_wrap_table, etc.)
arrow_include_pyarrow = pa.get_include()

# NumPy include directory
numpy_include = np.get_include()

# SabotQL include directories
sabot_ql_include = "../../include"
marble_include = "../../../MarbleDB/include"

extensions = [
    Extension(
        "sabot_ql.bindings.python.sabot_ql",
        ["sabot_ql.pyx"],
        include_dirs=[
            arrow_include_vendored,
            arrow_include_pyarrow,
            numpy_include,
            sabot_ql_include,
            marble_include,
        ],
        library_dirs=[
            arrow_lib,
            "../../build",
            "../../../MarbleDB/build",
        ],
        libraries=[
            "sabot_ql",
            "marble",
            "arrow",
        ],
        language="c++",
        extra_compile_args=["-std=c++20", "-O3"],
    )
]

setup(
    name="sabot_ql",
    version="0.1.0",
    description="SabotQL Python bindings for Sabot integration",
    ext_modules=cythonize(extensions, language_level=3),
    install_requires=[
        "pyarrow>=22.0.0",
        "cython>=3.0.0",
    ],
)


