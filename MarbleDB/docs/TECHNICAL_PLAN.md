# MarbleDB Technical Plan

## Overview
MarbleDB is a C++ columnar state store implementation inspired by Tonbo, designed for the Sabot project. It implements an LSM-tree using Apache Arrow Feather files for efficient columnar storage with fixed run lengths to enable clean merging operations.

## Core Architecture

### 1. LSM Tree Structure
- **Levels**: 7 levels (L0-L6) similar to Tonbo
- **Level 0**: Uncompacted SSTables from memtable flushes
- **Levels 1-6**: Compacted SSTables with increasing sizes
- **Fixed Run Lengths**: Each level has a fixed number of SSTables before compaction

### 2. Storage Components

#### MemTable
- **Type**: SkipList-based in-memory table
- **Format**: Arrow RecordBatch storage
- **Trigger**: Size-based flush trigger (configurable)
- **WAL**: Write-ahead logging for durability

#### SSTable (Sorted String Table)
- **Format**: Apache Arrow Feather files
- **Structure**: Columnar storage with metadata
- **Compression**: LZ4/ZSTD support via Arrow
- **Indexing**: Primary key indexing within files

#### WAL (Write-Ahead Log)
- **Format**: Binary log with record serialization
- **Sync**: Configurable sync behavior (every write, periodic, none)
- **Recovery**: Crash recovery from WAL files

### 3. Columnar Storage Design

#### Record Schema
```cpp
struct RecordSchema {
    std::vector<ArrowField> fields;
    size_t primary_key_index;
    std::vector<size_t> primary_key_indices; // for composite keys
};
```

#### Feather File Structure
```
[Header: Magic + Schema + Metadata]
[Column 0 Data]
[Column 1 Data]
...
[Column N Data]
[Footer: Statistics + Bloom Filter + Index]
```

#### Fixed Run Lengths
- **Level 0**: 4 SSTables max
- **Level 1**: 8 SSTables max
- **Level 2**: 16 SSTables max
- **Level 3+**: 32 SSTables max

### 4. Compaction Strategy

#### Leveled Compaction (Primary)
- **Trigger**: When level exceeds max SSTable count
- **Scope**: Key-range based compaction
- **Merge**: Clean merging due to fixed run lengths
- **Output**: Single SSTable per compaction

#### Compaction Process
1. Select overlapping SSTables in target level
2. Merge with overlapping SSTables from source level
3. Write new SSTable with sorted, merged data
4. Update manifest and delete old files

### 5. Key Components

#### Core Classes
```cpp
class MarbleDB {
    std::unique_ptr<MemTable> memtable_;
    std::unique_ptr<Manifest> manifest_;
    std::unique_ptr<Compactor> compactor_;
    std::unique_ptr<WAL> wal_;
};

class MemTable {
    arrow::RecordBatchVector batches_;
    size_t size_threshold_;
};

class SSTable {
    std::string path_;
    std::shared_ptr<arrow::Schema> schema_;
    std::unique_ptr<FeatherReader> reader_;
};

class Compactor {
    std::unique_ptr<LeveledCompactionStrategy> strategy_;
};
```

#### Key Abstractions
- **Record**: Abstract base for user records
- **Schema**: Column definitions and constraints
- **Key**: Primary key abstraction (supports composite keys)
- **Value**: Record value abstraction

### 6. Transaction Support

#### Transaction Types
- **Read Transactions**: Snapshot isolation
- **Write Transactions**: Optimistic concurrency control
- **Batch Writes**: Efficient bulk operations

#### Concurrency Control
- **Lock Map**: Key-level locking for write conflicts
- **Snapshot Isolation**: Read transactions see consistent state
- **Write Conflicts**: Detected via key-level conflict checking

### 7. Storage Backends

#### Local Filesystem
- **Implementation**: std::filesystem + memory mapping
- **Features**: Direct I/O, memory mapping for reads

#### S3/Object Storage (Future)
- **Implementation**: AWS SDK / MinIO client
- **Features**: Multipart uploads, range reads

### 8. API Design

#### Basic Operations
```cpp
class MarbleDB {
public:
    Status Put(const Key& key, std::shared_ptr<Record> record);
    Status Get(const Key& key, std::shared_ptr<Record>* record);
    Status Delete(const Key& key);

    std::unique_ptr<Iterator> Scan(const KeyRange& range);
    std::unique_ptr<Transaction> BeginTransaction();
};
```

#### Iterator Interface
```cpp
class Iterator {
public:
    virtual bool Valid() const = 0;
    virtual void Next() = 0;
    virtual Key key() const = 0;
    virtual std::shared_ptr<Record> value() const = 0;
    virtual Status status() const = 0;
};
```

### 9. Performance Optimizations

#### Read Optimizations
- **Bloom Filters**: False positive filtering for SSTable files
- **Block Cache**: LRU cache for compressed blocks
- **Column Projection**: Only read required columns

#### Write Optimizations
- **Batch Writes**: Group multiple writes into single I/O
- **Parallel Compaction**: Multi-threaded compaction
- **Write Buffering**: Buffer writes before flush

#### Memory Management
- **Arena Allocation**: Efficient memory reuse
- **Object Pooling**: Reuse common objects
- **Memory Mapping**: Map files into virtual memory

### 10. Configuration

#### Database Options
```cpp
struct DBOptions {
    std::string db_path;
    size_t memtable_size = 64 * 1024 * 1024; // 64MB
    size_t sstable_size = 256 * 1024 * 1024; // 256MB
    size_t block_size = 64 * 1024; // 64KB
    CompressionType compression = CompressionType::LZ4;
    bool use_wal = true;
    size_t wal_buffer_size = 64 * 1024 * 1024; // 64MB
};
```

#### Compaction Options
```cpp
struct CompactionOptions {
    size_t level0_max_files = 4;
    size_t level1_max_files = 8;
    size_t level_multiplier = 2;
    size_t max_compaction_threads = 4;
};
```

### 11. Error Handling

#### Status Codes
```cpp
enum class StatusCode {
    OK,
    NOT_FOUND,
    CORRUPTION,
    IO_ERROR,
    INVALID_ARGUMENT,
    WRITE_CONFLICT,
    COMPACTION_ERROR
};
```

#### Error Propagation
- **Status**: Return status for all operations
- **Exceptions**: Only for programming errors
- **Logging**: Structured logging for debugging

### 12. Testing Strategy

#### Unit Tests
- **Component Tests**: Individual class testing
- **Mock Objects**: Test isolation via mocks
- **Property Tests**: QuickCheck-style testing

#### Integration Tests
- **Full DB Tests**: End-to-end operations
- **Performance Tests**: Benchmarking suite
- **Stress Tests**: High load testing

#### Fuzz Testing
- **Input Fuzzing**: Random input generation
- **File Corruption**: Corrupted file handling
- **Race Conditions**: Concurrent operation testing

### 13. Build System

#### CMake Configuration
- **Modern CMake**: CMake 3.20+ features
- **Dependencies**: Arrow, Boost, etc.
- **Cross-platform**: Linux, macOS, Windows

#### Dependencies
- **Apache Arrow**: Columnar processing
- **Boost**: Utilities and data structures
- **Google Test**: Testing framework
- **Abseil**: C++ utilities
- **AWS SDK**: S3 support (optional)

### 14. Development Roadmap

#### Phase 1: Core Infrastructure (4 weeks)
- Basic LSM tree implementation
- MemTable and SSTable
- Simple read/write operations
- Basic testing

#### Phase 2: Compaction (3 weeks)
- Leveled compaction implementation
- Manifest management
- File merging logic
- Compaction testing

#### Phase 3: Transactions (3 weeks)
- Transaction support
- Concurrency control
- WAL implementation
- Recovery testing

#### Phase 4: Optimizations (3 weeks)
- Caching layers
- Performance tuning
- Memory management
- Benchmarking

#### Phase 5: Advanced Features (2 weeks)
- Column projection
- Secondary indexing
- Advanced compaction strategies
- Integration testing

### 15. File Structure

```
MarbleDB/
├── cmake/           # CMake build files
├── docs/            # Documentation
├── examples/        # Example usage
├── include/marble/  # Public headers
│   ├── db.h         # Main DB interface
│   ├── schema.h     # Schema definitions
│   ├── record.h     # Record abstractions
│   ├── iterator.h   # Iterator interface
│   ├── transaction.h # Transaction support
│   └── status.h     # Status codes
├── src/             # Implementation
│   ├── core/        # Core components
│   ├── storage/     # Storage layer
│   ├── compaction/  # Compaction logic
│   └── util/        # Utilities
└── tests/           # Test files
    ├── unit/        # Unit tests
    └── integration/ # Integration tests
```

## Integration with Sabot

### Data Flow
1. **Input**: Sabot operators produce columnar data
2. **Storage**: MarbleDB stores data in Feather format
3. **Query**: Sabot queries access data via MarbleDB iterators
4. **Output**: Results returned as Arrow RecordBatches

### Key Integration Points
- **Schema Compatibility**: Arrow schema alignment
- **Memory Management**: Shared memory pools
- **Threading**: Compatible threading models
- **Error Handling**: Unified error propagation

### Performance Requirements
- **Throughput**: 100K+ ops/sec for small records
- **Latency**: <1ms for point queries
- **Storage Efficiency**: <2x space amplification
- **Memory Usage**: Configurable memory limits
