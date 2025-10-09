#!/usr/bin/env python3
"""
Quick setup script to compile libpq_conn.pyx for testing.
"""

import subprocess
from setuptools import setup, Extension
from Cython.Build import cythonize
import sys

# Get pg_config path
try:
    pg_config = subprocess.check_output(['which', 'pg_config']).decode().strip()
except:
    pg_config = '/opt/homebrew/bin/pg_config'

# Get PostgreSQL paths
libpq_include = subprocess.check_output([pg_config, '--includedir']).decode().strip()
libpq_lib = subprocess.check_output([pg_config, '--libdir']).decode().strip()

print(f"✅ Found PostgreSQL:")
print(f"   Include: {libpq_include}")
print(f"   Lib: {libpq_lib}")

# Arrow paths
arrow_include = '/Users/bengamble/Sabot/vendor/arrow/python'

ext_modules = [
    Extension(
        "sabot._cython.connectors.postgresql.libpq_conn",
        ["sabot/_cython/connectors/postgresql/libpq_conn.pyx"],
        include_dirs=[libpq_include, arrow_include],
        library_dirs=[libpq_lib],
        libraries=['pq'],
        language='c',
    )
]

setup(
    name="sabot-postgres-cdc",
    ext_modules=cythonize(
        ext_modules,
        compiler_directives={
            'language_level': '3',
            'boundscheck': False,
            'wraparound': False,
        }
    ),
)

print("✅ Compilation complete!")
