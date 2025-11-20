#!/usr/bin/env python3
"""
Cross-platform build utilities for Sabot.

Provides platform detection and SIMD flag configuration for:
- ARM64 (Apple Silicon): NEON support
- x86_64: AVX2 runtime dispatch

Based on Arrow's proven cross-platform patterns.
"""

import platform
import sys
from pathlib import Path


def get_platform_info():
    """Get current platform architecture."""
    machine = platform.machine().lower()
    system = platform.system().lower()

    return {
        'machine': machine,
        'system': system,
        'is_arm': machine in ('arm64', 'aarch64'),
        'is_x86': machine in ('x86_64', 'amd64', 'x64'),
        'is_darwin': system == 'darwin',
        'is_linux': system == 'linux',
    }


def get_simd_config():
    """
    Get platform-appropriate SIMD configuration.

    Returns dict with:
    - common_flags: Flags for all source files
    - avx2_files: List of .cpp files requiring AVX2 (x86 only)
    - neon_files: List of .cpp files requiring NEON (ARM only)
    - defines: Preprocessor defines
    - avx2_flag: Specific flag for AVX2 compilation (x86)
    """
    platform_info = get_platform_info()

    if platform_info['is_arm']:
        # ARM: Enable NEON (always available on aarch64)
        return {
            'common_flags': [
                '-march=armv8-a',
                '-mtune=native',
            ],
            'avx2_files': [],  # No AVX2 on ARM
            'neon_files': [
                # NEON implementations (to be created)
                'hash_join_neon.cpp',
                'bloom_filter_neon.cpp',
            ],
            'defines': [
                '-DARROW_HAVE_NEON',
            ],
            'avx2_flag': None,  # Not applicable on ARM
        }

    elif platform_info['is_x86']:
        # x86: Runtime dispatch for AVX2
        return {
            'common_flags': [
                '-O3',
                # No -march=native here - we want portable binary with runtime dispatch
            ],
            'avx2_files': [
                # AVX2 implementations (to be created)
                'hash_join_avx2.cpp',
                'bloom_filter_avx2.cpp',
            ],
            'neon_files': [],  # No NEON on x86
            'defines': [
                '-DARROW_HAVE_RUNTIME_AVX2',
            ],
            'avx2_flag': '-march=haswell -mavx2',  # For AVX2-specific files
        }

    else:
        # Fallback: Scalar only
        return {
            'common_flags': ['-O3'],
            'avx2_files': [],
            'neon_files': [],
            'defines': [],
            'avx2_flag': None,
        }


def get_compile_args(base_args=None):
    """
    Get platform-appropriate compile arguments.

    Args:
        base_args: Base arguments to include (e.g., ['-std=c++17'])

    Returns:
        List of compile arguments with SIMD flags added
    """
    base_args = base_args or []
    config = get_simd_config()

    # Combine base args + common flags + defines
    compile_args = base_args + config['common_flags'] + config['defines']

    return compile_args


def get_source_files_with_flags(sources, source_dir=None):
    """
    Get source files with appropriate per-file compilation flags.

    For x86: Returns list of (source_file, extra_flags) tuples
    For ARM: Returns list of (source_file, None) tuples (NEON always enabled)

    Args:
        sources: List of source file names
        source_dir: Directory containing source files (for existence checks)

    Returns:
        List of (source_path, extra_flags) tuples
    """
    config = get_simd_config()
    platform_info = get_platform_info()

    result = []

    for source in sources:
        source_name = Path(source).name

        # Check if this is an AVX2-specific file (x86 only)
        if platform_info['is_x86'] and source_name in config['avx2_files']:
            # This file needs AVX2 flags
            extra_flags = config['avx2_flag']

            # Check if file exists (don't fail if not created yet)
            if source_dir:
                source_path = Path(source_dir) / source_name
                if source_path.exists():
                    result.append((str(source), extra_flags))
                else:
                    print(f"Note: AVX2 file {source_name} not found yet (will use scalar fallback)")
            else:
                result.append((str(source), extra_flags))

        # Check if this is a NEON-specific file (ARM only)
        elif platform_info['is_arm'] and source_name in config['neon_files']:
            # NEON always enabled via -march=armv8-a, no per-file flags needed
            if source_dir:
                source_path = Path(source_dir) / source_name
                if source_path.exists():
                    result.append((str(source), None))
                else:
                    print(f"Note: NEON file {source_name} not found yet (will use scalar fallback)")
            else:
                result.append((str(source), None))

        else:
            # Regular source file
            result.append((str(source), None))

    return result


def print_platform_info():
    """Print platform detection info (for debugging)."""
    platform_info = get_platform_info()
    config = get_simd_config()

    print("=" * 60)
    print("Platform Detection:")
    print(f"  Machine: {platform_info['machine']}")
    print(f"  System: {platform_info['system']}")
    print(f"  Architecture: {'ARM64' if platform_info['is_arm'] else 'x86_64' if platform_info['is_x86'] else 'Unknown'}")

    print("\nSIMD Configuration:")
    if platform_info['is_arm']:
        print("  NEON: Enabled (compile-time, always available on aarch64)")
    elif platform_info['is_x86']:
        print("  AVX2: Runtime dispatch (fat binary)")
        print(f"  AVX2 flag: {config['avx2_flag']}")
    else:
        print("  SIMD: Disabled (scalar only)")

    print(f"\nCommon flags: {' '.join(config['common_flags'])}")
    print(f"Defines: {' '.join(config['defines'])}")
    print("=" * 60)


if __name__ == "__main__":
    # Test platform detection
    print_platform_info()

    print("\nExample compile args:")
    args = get_compile_args(base_args=['-std=c++17', '-Wno-unused-function'])
    print(f"  {' '.join(args)}")

    print("\nExample source files:")
    sources = ['hash_join.pyx', 'hash_join_avx2.cpp', 'hash_join_neon.cpp']
    files = get_source_files_with_flags(sources)
    for source, flags in files:
        flag_str = flags if flags else "(common flags)"
        print(f"  {source}: {flag_str}")
