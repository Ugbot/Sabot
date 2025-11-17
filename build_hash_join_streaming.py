#!/usr/bin/env python3
"""Build the streaming hash join module with cross-platform SIMD support."""

import sys
import os
from pathlib import Path
from setuptools import setup, Extension
from Cython.Build import cythonize
import numpy as np

# Import cross-platform build utilities
from build_utils import get_compile_args, get_simd_config, print_platform_info, get_platform_info

# Print platform info for debugging
print_platform_info()

# Get platform configuration
platform_info = get_platform_info()
simd_config = get_simd_config()

# Module directory
module_dir = Path("sabot/_cython/operators")

# Source files
main_source = str(module_dir / "hash_join_streaming.pyx")

# SIMD implementation files (platform-specific)
if platform_info['is_arm']:
    # ARM: Include NEON implementation
    simd_sources = [
        str(module_dir / "hash_join_neon.cpp"),
    ]
    print("\n📦 Including ARM NEON implementation")
elif platform_info['is_x86']:
    # x86: Include AVX2 implementation
    simd_sources = [
        str(module_dir / "hash_join_avx2.cpp"),
    ]
    print("\n📦 Including x86 AVX2 implementation")
else:
    # Fallback: No SIMD
    simd_sources = []
    print("\n⚠️  No SIMD implementation available for this platform")

# All sources
sources = [main_source] + simd_sources

# Arrow paths
arrow_include = "/Users/bengamble/Sabot/vendor/arrow/cpp/build/install/include"
arrow_lib = "/Users/bengamble/Sabot/vendor/arrow/cpp/build/install/lib"

# NumPy include
numpy_include = np.get_include()

# Include directories
include_dirs = [
    numpy_include,
    arrow_include,
    str(Path("vendor/arrow/python")),
    str(Path("vendor/arrow/python/pyarrow")),
    str(Path("vendor/arrow/python/pyarrow/src")),
    str(module_dir),  # For hash_join_memory.pxd
]

# Get platform-appropriate SIMD compile args
base_args = ["-std=c++17", "-Wno-unused-function", "-Wno-deprecated-declarations"]
compile_args = get_compile_args(base_args)

print(f"\n🔧 Compile args: {' '.join(compile_args)}")
print(f"📁 Sources:")
for src in sources:
    print(f"   - {src}")

# Create extension with per-file compilation flags
# NOTE: For x86, we need to compile AVX2 files with special flags
if platform_info['is_x86'] and simd_config['avx2_flag']:
    # On x86, we need to apply AVX2 flags to AVX2-specific files
    # This is done via Extension extra_compile_args, but we need to compile separately
    # For now, apply AVX2 flags to all files (will fix with separate compilation later)
    print(f"\n⚠️  Note: AVX2 flags will be applied to all files for simplicity")
    print(f"   AVX2 flag: {simd_config['avx2_flag']}")
    # TODO: Implement per-file compilation with different flags
    # For now, just use common flags (AVX2 code is guarded by #ifdef)

# Create extension
ext = Extension(
    "sabot._cython.operators.hash_join_streaming",
    sources=sources,
    include_dirs=include_dirs,
    library_dirs=[arrow_lib],
    libraries=["arrow"],
    language="c++",
    extra_compile_args=compile_args,
    extra_link_args=[f"-Wl,-rpath,{arrow_lib}"],
)

# Build
print("\n🔨 Building streaming hash join module...")
print(f"   Module: sabot._cython.operators.hash_join_streaming")
print(f"   Platform: {'ARM NEON' if platform_info['is_arm'] else 'x86 AVX2' if platform_info['is_x86'] else 'Scalar'}")

setup(
    name="hash_join_streaming",
    ext_modules=cythonize([ext], compiler_directives={
        "language_level": "3",
        "boundscheck": False,
        "wraparound": False,
        "cdivision": True,
        "nonecheck": False,
    }),
)

print("\n✅ Build complete!")
print(f"   Usage: from sabot._cython.operators.hash_join_streaming import StreamingHashJoin")
print(f"   SIMD: {simd_config['common_flags']}")
