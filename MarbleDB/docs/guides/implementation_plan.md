# MarbleDB: ArcticDB + Tonbo Feature Integration Plan

## Phase 4: ArcticDB-Style Bitemporal Features

### 4.1 Complete Valid Time Support
**Goal**: Implement full bitemporal queries with valid time ranges

```cpp
// Enhanced TemporalQuerySpec
struct TemporalQuerySpec {
    // System time (MVCC)
    SnapshotId as_of_snapshot;
    uint64_t system_time_start;
    uint64_t system_time_end;

    // Valid time (business validity)
    uint64_t valid_time_start;  // NEW: Business validity start
    uint64_t valid_time_end;    // NEW: Business validity end

    // Query options
    bool include_deleted = false;
    bool temporal_reconstruction = true;
};

// Temporal metadata per record
struct TemporalMetadata {
    SnapshotId created_snapshot;
    SnapshotId deleted_snapshot;
    uint64_t valid_from;  // Business validity start
    uint64_t valid_to;    // Business validity end
    bool is_deleted = false;
};
```

### 4.2 Temporal Reconstruction Engine
**Goal**: Reconstruct historical state at any point in time

```cpp
class TemporalReconstructor {
public:
    // AS OF queries
    Status ReconstructAsOf(const SnapshotId& snapshot,
                          const std::vector<arrow::RecordBatch>& versions,
                          arrow::RecordBatch* result);

    // Valid time queries
    Status ReconstructValidTime(uint64_t valid_start, uint64_t valid_end,
                               const std::vector<arrow::RecordBatch>& versions,
                               arrow::RecordBatch* result);

    // Full bitemporal queries
    Status ReconstructBitemporal(const TemporalQuerySpec& spec,
                                const std::vector<arrow::RecordBatch>& versions,
                                arrow::RecordBatch* result);

private:
    // Version chaining algorithm
    Status ChainVersions(const std::vector<arrow::RecordBatch>& versions,
                        std::vector<VersionChain>* chains);

    // Conflict resolution for overlapping valid times
    Status ResolveConflicts(const VersionChain& chain,
                           const TemporalQuerySpec& spec,
                           arrow::RecordBatch* result);
};
```

## Phase 5: Tonbo-Style LSM Integration

### 5.1 LSM Tree Architecture
**Goal**: Replace simple file storage with proper LSM tree

```cpp
class LSMTree {
public:
    struct Level {
        int level_number;
        std::vector<SSTable> tables;
        size_t max_size;
    };

    struct Options {
        size_t memtable_size = 64 * 1024 * 1024;  // 64MB
        size_t l0_compaction_trigger = 4;
        size_t level_multiplier = 10;
        CompactionStrategy strategy = CompactionStrategy::kLeveled;
    };

    Status Put(const std::string& key, const std::string& value);
    Status Get(const std::string& key, std::string* value);
    Status Delete(const std::string& key);

    Status Scan(const ScanSpec& spec, QueryResult* result);

private:
    MemTable memtable_;
    std::vector<MemTable> immutable_memtables_;
    std::vector<Level> levels_;
    std::unique_ptr<Compactor> compactor_;
};
```

### 5.2 Compaction Strategy Framework
**Goal**: Pluggable compaction algorithms (borrow from Tonbo)

```cpp
enum class CompactionStrategy {
    kLeveled,   // Size-tiered: L0->L1->L2->...
    kTiered,    // Level-tiered: each level has target size
    kUniversal  // Universal compaction
};

class Compactor {
public:
    virtual ~Compactor() = default;

    virtual Status PickCompaction(const LSMTree::Options& options,
                                 const std::vector<LSMTree::Level>& levels,
                                 CompactionTask* task) = 0;

    virtual Status ExecuteCompaction(const CompactionTask& task,
                                   LSMTree* tree) = 0;

    virtual CompactionStrategy GetStrategy() const = 0;
};

// Leveled compaction (like RocksDB)
class LeveledCompactor : public Compactor {
    Status PickCompaction(const LSMTree::Options& options,
                         const std::vector<LSMTree::Level>& levels,
                         CompactionTask* task) override;
};
```

### 5.3 SSTable Implementation
**Goal**: Immutable sorted tables with indexing

```cpp
class SSTable {
public:
    struct Metadata {
        std::string filename;
        uint64_t file_size;
        std::string min_key;
        std::string max_key;
        uint64_t num_entries;
        std::unique_ptr<BloomFilter> bloom_filter;
        std::unique_ptr<ZoneMap> zone_map;
    };

    static Status Create(const std::vector<KeyValue>& entries,
                        const std::string& filename,
                        SSTable** table);

    Status Open(const std::string& filename, SSTable** table);

    Status Get(const std::string& key, std::string* value);
    Status Scan(const ScanSpec& spec, QueryResult* result);

    const Metadata& GetMetadata() const { return metadata_; }

private:
    Metadata metadata_;
    std::unique_ptr<RandomAccessFile> data_file_;
    std::unique_ptr<Index> index_;  // Block index for fast seeks
};
```

## Phase 6: Advanced Query Features

### 6.1 Time Series Operations (ArcticDB-style)
**Goal**: Native time series query support

```cpp
class TimeSeriesEngine {
public:
    // Resampling operations
    Status Resample(const QuerySpec& spec,
                   const std::string& frequency,  // "1min", "1h", "1d"
                   const std::string& aggregation, // "mean", "sum", "count"
                   QueryResult* result);

    // Rolling window operations
    Status RollingWindow(const QuerySpec& spec,
                        const std::string& window,     // "10min", "1h"
                        const std::string& operation,  // "mean", "std"
                        QueryResult* result);

    // Time-based aggregations
    Status TimeBucket(const QuerySpec& spec,
                     const std::string& bucket_size,
                     const std::vector<std::string>& aggregations,
                     QueryResult* result);

    // ASOF joins
    Status AsOfJoin(const QuerySpec& left_spec,
                   const QuerySpec& right_spec,
                   const std::string& join_key,
                   QueryResult* result);
};
```

### 6.2 Symbol Library System (ArcticDB-style)
**Goal**: Hierarchical data organization

```cpp
class SymbolLibrary {
public:
    // Library hierarchy management
    Status CreateLibrary(const std::string& path);
    Status DeleteLibrary(const std::string& path);
    Status ListLibraries(const std::string& prefix,
                        std::vector<std::string>* libraries);

    // Symbol management
    Status CreateSymbol(const std::string& library_path,
                       const std::string& symbol_name,
                       const TableSchema& schema);

    Status WriteSymbol(const std::string& library_path,
                      const std::string& symbol_name,
                      const arrow::RecordBatch& data,
                      const WriteOptions& options);

    Status ReadSymbol(const std::string& library_path,
                     const std::string& symbol_name,
                     const ReadQuery& query,
                     QueryResult* result);

    // Version management
    Status ListVersions(const std::string& library_path,
                       const std::string& symbol_name,
                       std::vector<VersionInfo>* versions);

    Status DeleteVersion(const std::string& library_path,
                        const std::string& symbol_name,
                        int64_t version);
};
```

## Phase 7: Tonbo-Style Optimizations

### 7.1 Projection Pushdown
**Goal**: Only read required columns (borrow from Tonbo)

```cpp
// Enhanced ScanSpec with projection
struct AnalyticalScanSpec : public ScanSpec {
    std::vector<std::string> projection_columns;  // Columns to read
    std::vector<size_t> projection_indices;       // Column indices

    // Convert to Arrow ProjectionMask
    ProjectionMask ToProjectionMask(const arrow::Schema& schema) const;
};

// Optimized table scan with projection
class ProjectedTableScan {
public:
    Status ScanWithProjection(const AnalyticalScanSpec& spec,
                             QueryResult* result);

private:
    // Only read projected columns from storage
    Status ReadProjectedColumns(const std::vector<size_t>& indices,
                               const std::string& file_path,
                               arrow::RecordBatch* result);
};
```

### 7.2 Record Macro System
**Goal**: Type-safe record definitions (inspired by Tonbo)

```cpp
// Code generation approach for type safety
#define MARBLE_RECORD(ClassName, ...) \
    class ClassName { \
    public: \
        static const arrow::Schema& Schema(); \
        arrow::RecordBatch ToBatch() const; \
        static ClassName FromBatch(const arrow::RecordBatch& batch, size_t row); \
        \
    private: \
        __VA_ARGS__ \
    };

// Usage example:
MARBLE_RECORD(User,
    MARBLE_FIELD(std::string, name, primary_key);
    MARBLE_FIELD(std::optional<std::string>, email);
    MARBLE_FIELD(int32_t, age);
    MARBLE_FIELD(std::vector<uint8_t>, data);
);

// Generates:
class User {
public:
    static const arrow::Schema& Schema() {
        static arrow::Schema schema = arrow::schema({
            arrow::field("name", arrow::utf8(), false),
            arrow::field("email", arrow::utf8(), true),
            arrow::field("age", arrow::int32(), false),
            arrow::field("data", arrow::binary(), false)
        });
        return schema;
    }

    // Type-safe accessors
    const std::string& name() const { return name_; }
    void set_name(const std::string& name) { name_ = name; }

    // etc...
};
```

## Implementation Priority

### High Priority (Next 2-3 weeks):
1. **Fix compilation issues** in temporal and analytics code
2. **Complete bitemporal reconstruction** logic
3. **Add valid time query support**
4. **Implement basic LSM tree** structure

### Medium Priority (Next 1-2 months):
1. **Add projection pushdown** for column selection
2. **Implement compaction strategies** (leveled first)
3. **Add symbol library system** for data organization
4. **Time series operations** (resample, rolling windows)

### Lower Priority (Future phases):
1. **Advanced query optimizations**
2. **Multi-symbol joins and operations**
3. **Record macro system** for type safety
4. **Additional storage backends** (S3, etc.)

## Success Metrics

- **Bitemporal Queries**: Support AS OF and VALID_TIME BETWEEN queries
- **Performance**: LSM tree should provide 2-5x better write throughput
- **Storage Efficiency**: Compaction should reduce storage by 20-50%
- **Query Speed**: Projection pushdown should improve query performance by 3-10x
- **Developer Experience**: Type-safe records should reduce bugs by 30%

This plan transforms MarbleDB from a basic analytical database into a full-featured ArcticDB competitor with Tonbo's proven architectural patterns.
