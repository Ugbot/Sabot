# Apache Arrow Submodule

Sabot includes Apache Arrow as a Git submodule for development and building purposes.

## Overview

The Arrow submodule (`third-party/arrow`) provides:

- Complete Apache Arrow C++ source code and headers
- Python bindings and libraries
- Development headers for building against Arrow C API
- Integration testing capabilities

## Usage

### Initializing the Submodule

```bash
# Clone with submodules
git clone --recursive https://github.com/your-repo/sabot.git

# Or initialize submodules after cloning
git submodule update --init --recursive third-party/arrow
```

### Building Against Arrow

The setup.py automatically detects and uses Arrow headers:

1. **Priority 1**: Installed PyArrow (if available)
2. **Priority 2**: Arrow submodule headers (for development builds)

### Development Workflow

```bash
# Update Arrow to latest version
cd third-party/arrow
git checkout main
git pull origin main

# Build Arrow (if needed for development)
# Follow Arrow build instructions in third-party/arrow/README.md

# Build Sabot with Arrow integration
cd ../..
python setup.py build_ext --inplace
```

## Integration Points

Arrow is integrated into Sabot for:

- **Arrow-native Operations**: Zero-copy columnar processing
- **Flight Protocol**: High-performance data transfer
- **Parquet Integration**: Efficient file format support
- **Dataset API**: Advanced data processing capabilities

## Files

- `third-party/arrow/`: Complete Apache Arrow repository
- `sabot/_cython/arrow/`: Arrow-specific Cython extensions
- `setup.py`: Automatic Arrow header detection

## Dependencies

- Git (for submodule management)
- CMake (for building Arrow if needed)
- C++ compiler (for Arrow development builds)

## Contributing

When working with Arrow integration:

1. Test changes against both installed PyArrow and submodule headers
2. Update submodule when Arrow API changes
3. Ensure compatibility with Arrow release versions

## References

- [Apache Arrow Documentation](https://arrow.apache.org/docs/)
- [Arrow Python Bindings](https://arrow.apache.org/docs/python/)
- [Arrow Flight RPC](https://arrow.apache.org/docs/format/Flight.html)
