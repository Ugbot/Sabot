"""Setup script for SabotQL Python bindings."""

from setuptools import setup, Extension
from Cython.Build import cythonize
import pyarrow as pa

# Arrow include directories
arrow_include = pa.get_include()
arrow_lib = pa.get_library_dirs()[0]

# SabotQL include directories
sabot_ql_include = "../../include"
marble_include = "../../../MarbleDB/include"

extensions = [
    Extension(
        "sabot_ql.bindings.python.sabot_ql",
        ["sabot_ql.pyx"],
        include_dirs=[
            arrow_include,
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


