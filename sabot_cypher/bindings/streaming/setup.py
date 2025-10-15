from setuptools import setup, Extension
from Cython.Build import cythonize
import os
import sys
import pyarrow

# Determine if we are building on macOS
IS_MACOS = sys.platform == 'darwin'

# Define paths
SABOT_CYPHER_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '../..'))
SABOT_CYPHER_BUILD_DIR = os.path.join(SABOT_CYPHER_ROOT, 'build')
SABOT_CYPHER_INCLUDE_DIR = os.path.join(SABOT_CYPHER_ROOT, 'include')

# Arrow paths
ARROW_HOME = os.environ.get('ARROW_HOME', os.path.abspath(os.path.join(SABOT_CYPHER_ROOT, '..', 'vendor', 'arrow', 'cpp', 'build', 'install')))
ARROW_INCLUDE_DIR = os.path.join(ARROW_HOME, 'include')
ARROW_LIB_DIR = os.path.join(ARROW_HOME, 'lib')

# PyArrow include directory
PYARROW_INCLUDE_DIR = pyarrow.get_include()

# Define extension module
extensions = [
    Extension(
        "sabot_cypher_streaming",
        ["sabot_cypher_streaming.pyx"],
        include_dirs=[
            SABOT_CYPHER_INCLUDE_DIR,
            SABOT_CYPHER_BUILD_DIR,
            ARROW_INCLUDE_DIR,
            PYARROW_INCLUDE_DIR,
            os.path.join(SABOT_CYPHER_ROOT, 'vendored', 'sabot_cypher_core', 'src', 'include'),
        ],
        library_dirs=[
            SABOT_CYPHER_BUILD_DIR,
            ARROW_LIB_DIR
        ],
        libraries=['sabot_cypher', 'arrow', 'arrow_compute'],
        extra_compile_args=['-std=c++17', '-Wall', '-Wextra'],
        extra_link_args=['-Wl,-rpath,@loader_path'] if IS_MACOS else [],
        language="c++",
    )
]

setup(
    name="SabotCypherStreaming",
    version="0.1.0",
    ext_modules=cythonize(
        extensions,
        compiler_directives={'language_level': "3"}
    ),
    zip_safe=False,
)

