# MarbleDB Documentation

Welcome to the MarbleDB documentation! This directory contains all documentation for the MarbleDB project.

## 📂 Documentation Structure

```
docs/
├── README.md              # This file - documentation overview
├── api/                   # API reference documentation
│   └── API_SURFACE.md     # Complete API surface documentation
├── guides/                # User guides and tutorials
│   ├── README.md          # Main project README
│   ├── README_API.md      # API usage guide
│   ├── arctic_tonbo_analysis.md
│   ├── DUCKDB_INTEGRATION_PLAN.md
│   ├── example_usage_cmake.md
│   ├── implementation_plan.md
│   ├── MARBLEDB_ROADMAP_REVIEW.md
│   ├── TECHNICAL_PLAN.md
│   ├── tonbo_comparison.md
│   └── RAFT_INTEGRATION.md
└── [future additions]
```

## 🚀 Quick Start

### For Users
- **[API Surface Guide](api/API_SURFACE.md)**: Complete API reference with examples in C++, C, and Python
- **[Main README](../README.md)**: Project overview and getting started

### For Developers
- **[Technical Plan](guides/TECHNICAL_PLAN.md)**: Architecture and implementation details
- **[Implementation Plan](guides/implementation_plan.md)**: Development roadmap and phases

## 📖 Documentation Topics

### API Documentation
- **C++ API**: Modern C++ interface with RAII and type safety
- **C API**: Language-agnostic C interface for bindings
- **Language Bindings**: Examples for Python, Java, Rust, Go, etc.

### Architecture Guides
- **LSM Tree Implementation**: Storage engine architecture
- **Raft Consensus**: Distributed consensus implementation
- **Pushdown Optimization**: Query optimization techniques
- **Type-Safe Records**: Compile-time schema validation

### Integration Guides
- **CMake Integration**: Building and linking with MarbleDB
- **Language Bindings**: Creating bindings for new languages
- **Embedded Usage**: Using MarbleDB in applications

## 🔧 Development Documentation

### Internal Architecture
- **Storage Engine**: LSM tree, SSTables, compaction
- **Query Engine**: Pushdown optimization, vectorized execution
- **Consensus Layer**: Raft implementation for distributed systems
- **API Layers**: C++, C, and language binding architecture

### Performance Tuning
- **Indexing Strategies**: Bloom filters, skipping indexes
- **Memory Management**: Arrow integration, zero-copy operations
- **Query Optimization**: Predicate and projection pushdown

## 📚 Additional Resources

- **[Examples](../examples/)**: Code examples organized by complexity and feature
- **[Tests](../tests/)**: Comprehensive test suite with performance benchmarks
- **[Scripts](../scripts/)**: Build and demo scripts
- **[Benchmarks](../benchmarks/)**: Performance benchmarking tools

## 🤝 Contributing

When adding new documentation:

1. **API Documentation**: Add to `docs/api/` directory
2. **User Guides**: Add to `docs/guides/` directory
3. **Examples**: Add to appropriate `examples/` subdirectory
4. **Keep it Updated**: Update docs when APIs change

## 📋 Documentation Standards

- Use Markdown format for all documentation
- Include code examples where relevant
- Document both C++ and C APIs
- Provide performance characteristics and trade-offs
- Include build and usage instructions

---

**Need help?** Check the [examples](../examples/) directory for working code samples, or see the [API documentation](api/API_SURFACE.md) for detailed function references.
