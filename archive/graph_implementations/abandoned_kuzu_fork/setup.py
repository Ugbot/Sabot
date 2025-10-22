#!/usr/bin/env python3
"""
Setup script for SabotCypher Python module

Builds the C++ extension using Cython and installs the Python package.
"""

from setuptools import setup, Extension, find_packages
from Cython.Build import cythonize
import numpy as np
import pyarrow as pa
import sys
import os

class CMakeExtension(Extension):
    def __init__(self, name, sourcedir=''):
        Extension.__init__(self, name, sources=[])
        self.sourcedir = os.path.abspath(sourcedir)

class CMakeBuild(build_ext):
    def run(self):
        try:
            subprocess.check_output(['cmake', '--version'])
        except OSError:
            raise RuntimeError("CMake must be installed to build sabot_cypher")

        for ext in self.extensions:
            self.build_extension(ext)

    def build_extension(self, ext):
        extdir = os.path.abspath(os.path.dirname(self.get_ext_fullpath(ext.name)))
        
        cmake_args = [
            f'-DCMAKE_LIBRARY_OUTPUT_DIRECTORY={extdir}',
            f'-DPYTHON_EXECUTABLE={sys.executable}',
            '-DCMAKE_BUILD_TYPE=Release',
        ]

        build_args = ['--config', 'Release']

        if not os.path.exists(self.build_temp):
            os.makedirs(self.build_temp)

        subprocess.check_call(
            ['cmake', ext.sourcedir] + cmake_args,
            cwd=self.build_temp
        )
        subprocess.check_call(
            ['cmake', '--build', '.'] + build_args,
            cwd=self.build_temp
        )

# Read README for long description
with open('README.md', 'r', encoding='utf-8') as f:
    long_description = f.read()

setup(
    name='sabot_cypher',
    version='0.1.0',
    author='Sabot Team',
    description='High-Performance Cypher Query Engine with Arrow Execution',
    long_description=long_description,
    long_description_content_type='text/markdown',
    url='https://github.com/yourusername/sabot',  # TODO: Update
    packages=find_packages(),
    ext_modules=[CMakeExtension('sabot_cypher.sabot_cypher_native')],
    cmdclass={'build_ext': CMakeBuild},
    python_requires='>=3.8',
    install_requires=[
        'pyarrow>=10.0.0',
        'numpy>=1.20.0',
    ],
    extras_require={
        'dev': [
            'pytest>=7.0.0',
            'pytest-cov>=3.0.0',
            'black>=22.0.0',
            'mypy>=0.950',
        ],
        'benchmarks': [
            'polars>=0.18.0',
            'pandas>=1.5.0',
        ],
    },
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Intended Audience :: Developers',
        'Topic :: Database',
        'Topic :: Software Development :: Libraries :: Python Modules',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: 3.9',
        'Programming Language :: Python :: 3.10',
        'Programming Language :: Python :: 3.11',
        'Programming Language :: Python :: 3.12',
        'Programming Language :: C++',
        'License :: OSI Approved :: MIT License',
    ],
    keywords='cypher graph query arrow kuzu database',
    project_urls={
        'Documentation': 'https://github.com/yourusername/sabot/blob/main/sabot_cypher/README.md',
        'Source': 'https://github.com/yourusername/sabot',
        'Tracker': 'https://github.com/yourusername/sabot/issues',
    },
)

