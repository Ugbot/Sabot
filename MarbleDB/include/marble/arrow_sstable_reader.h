/************************************************************************
Copyright 2024 MarbleDB Contributors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    https://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
**************************************************************************/

#pragma once

#include "marble/sstable.h"
#include "marble/file_system.h"
#include "marble/status.h"
#include "marble/analytics.h"
#include "marble/hot_key_cache.h"
#include <memory>
#include <string>
#include <vector>
#include <arrow/api.h>
#include <arrow/ipc/api.h>

namespace marble {

/**
 * @brief Arrow IPC-based SSTable Reader with Full Optimization Stack
 *
 * Reads SSTables written by MmapSSTableWriter in Arrow IPC format.
 * Leverages ALL of MarbleDB's lookup infrastructure:
 * - HotKeyCache: Aerospike-inspired LRU cache (~100ns lookups)
 * - NegativeCache: Fast "definitely doesn't exist" checks
 * - BloomFilter: Membership testing (~500ns, avoid disk I/O)
 * - Sparse Index: ClickHouse-style binary search (~5μs)
 * - Zone Maps: Min/max pruning for range queries
 * - Arrow IPC: Standard columnar format for scans
 *
 * File Format (Written by MmapSSTableWriter):
 * [Arrow IPC Stream Data]  <- RecordBatches with schema [key: uint64, value: binary]
 * [Sparse Index Section]   <- [count(8)][key(8), batch_idx(8)] repeated
 * [Metadata Section]       <- [entry_count(8)][min_key(8)][max_key(8)][level(8)]
 * [Footer]                 <- [data_end(8)][index_end(8)][magic(8) = "ARROWSST"]
 */
class ArrowSSTableReader : public SSTable {
public:
    ArrowSSTableReader(const std::string& filepath,
                       const SSTableMetadata& metadata,
                       std::shared_ptr<FileSystem> fs);

    ~ArrowSSTableReader() override;

    // SSTable interface
    const SSTableMetadata& GetMetadata() const override;
    bool ContainsKey(uint64_t key) const override;
    Status Get(uint64_t key, std::string* value) const override;
    Status MultiGet(const std::vector<uint64_t>& keys,
                   std::vector<std::string>* values) const override;
    Status Scan(uint64_t start_key, uint64_t end_key,
               std::vector<std::pair<uint64_t, std::string>>* results) const override;
    Status ScanBatches(uint64_t start_key, uint64_t end_key,
                      std::vector<std::shared_ptr<arrow::RecordBatch>>* batches) const override;
    Status GetAllKeys(std::vector<uint64_t>* keys) const override;
    std::string GetFilePath() const override;
    uint64_t GetFileSize() const override;
    Status Validate() const override;

    /**
     * @brief Statistics for performance monitoring
     */
    struct Stats {
        uint64_t cache_hits = 0;
        uint64_t negative_cache_hits = 0;
        uint64_t bloom_filter_rejections = 0;
        uint64_t range_pruned = 0;
        uint64_t sparse_index_lookups = 0;
        uint64_t full_scans = 0;
        uint64_t cache_promotions = 0;
    };

    Stats GetStats() const { return stats_; }

private:
    // File metadata
    std::string filepath_;
    SSTableMetadata metadata_;
    std::shared_ptr<FileSystem> fs_;

    // Footer offsets (loaded from file)
    uint64_t data_section_end_ = 0;
    uint64_t index_section_end_ = 0;

    // Arrow IPC data
    std::vector<std::shared_ptr<arrow::RecordBatch>> batches_;
    std::shared_ptr<arrow::Schema> arrow_schema_;
    bool batches_loaded_ = false;

    // Sparse index (ClickHouse-inspired)
    std::vector<std::pair<uint64_t, uint64_t>> sparse_index_;  // key -> batch_idx
    bool sparse_index_loaded_ = false;

    // Bloom filter for fast "might contain"
    std::unique_ptr<BloomFilter> bloom_filter_;
    bool bloom_filter_loaded_ = false;

    // Hot key cache (Aerospike-inspired)
    std::shared_ptr<HotKeyCache> hot_key_cache_;

    // Negative cache (recent misses)
    std::shared_ptr<HotKeyNegativeCache> negative_cache_;

    // Statistics
    mutable Stats stats_;

    // Lazy loading methods
    Status LoadFooter();
    Status LoadSparseIndex();
    Status LoadBloomFilter();
    Status LoadArrowBatches();

    // Lookup methods
    Status GetFromHotCache(uint64_t key, std::string* value, size_t* batch_idx, size_t* row_idx) const;
    Status GetFromSparseIndex(uint64_t key, std::string* value) const;
    Status GetFromBatchIndex(size_t batch_idx, size_t row_idx, std::string* value) const;

    // Helper methods
    Status ReadFooter(int fd, uint64_t* data_end, uint64_t* index_end);
    Status ReadSparseIndexSection(int fd);
    bool IsKeyInRange(uint64_t key) const;
};

/**
 * @brief Factory function to create Arrow SSTable reader
 *
 * Detects Arrow format by reading footer magic number.
 * Returns nullptr if not Arrow format.
 */
std::unique_ptr<ArrowSSTableReader> OpenArrowSSTable(
    const std::string& filepath,
    std::shared_ptr<FileSystem> fs = nullptr);

} // namespace marble
