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

#include "marble/arrow_sstable_reader.h"
#include "marble/sstable.h"  // For kTombstoneMarker
#include "marble/logging.h"
#include <fcntl.h>
#include <unistd.h>
#include <iostream>
#include <algorithm>
#include <arrow/io/file.h>
#include <arrow/ipc/reader.h>

namespace marble {

// Define log tag for this component
MARBLE_LOG_TAG(ArrowReader);

ArrowSSTableReader::ArrowSSTableReader(const std::string& filepath,
                                       const SSTableMetadata& metadata,
                                       std::shared_ptr<FileSystem> fs)
    : filepath_(filepath)
    , metadata_(metadata)
    , fs_(fs) {

    MARBLE_LOG_DEBUG(ArrowReader) << "Constructor START";
    MARBLE_LOG_DEBUG(ArrowReader) << "  filepath=" << filepath;
    MARBLE_LOG_DEBUG(ArrowReader) << "  entries=" << metadata.record_count;
    MARBLE_LOG_DEBUG(ArrowReader) << "  key_range=[" << metadata.min_key << "," << metadata.max_key << "]";

    // Create hot key cache for this SSTable
    MARBLE_LOG_DEBUG(ArrowReader) << "Creating HotKeyCache...";
    HotKeyCacheConfig cache_config;
    cache_config.max_entries = 10000;
    cache_config.promotion_threshold = 3;
    hot_key_cache_ = CreateHotKeyCache(cache_config);
    MARBLE_LOG_DEBUG(ArrowReader) << "HotKeyCache created";

    // Create negative cache
    MARBLE_LOG_DEBUG(ArrowReader) << "Creating HotKeyNegativeCache...";
    negative_cache_ = CreateHotKeyNegativeCache(10000);
    MARBLE_LOG_DEBUG(ArrowReader) << "HotKeyNegativeCache created";

    MARBLE_LOG_DEBUG(ArrowReader) << "Constructor COMPLETE for " << filepath_;
}

ArrowSSTableReader::~ArrowSSTableReader() = default;

const SSTableMetadata& ArrowSSTableReader::GetMetadata() const {
    return metadata_;
}

bool ArrowSSTableReader::IsKeyInRange(uint64_t key) const {
    return key >= metadata_.min_key && key <= metadata_.max_key;
}

bool ArrowSSTableReader::ContainsKey(uint64_t key) const {
    // Fast path 1: Range check (O(1))
    if (!IsKeyInRange(key)) {
        return false;
    }

    // Fast path 2: Negative cache (now supports uint64_t)
    if (negative_cache_ && negative_cache_->DefinitelyNotExists(key)) {
        return false;
    }

    // Fast path 3: Bloom filter (raw format from MmapSSTableWriter)
    if (!bloom_filter_loaded_) {
        const_cast<ArrowSSTableReader*>(this)->LoadBloomFilter();
    }

    // Check raw bloom filter if loaded
    if (!bloom_bytes_.empty() && bloom_num_bits_ > 0) {
        // Hash the key
        std::hash<uint64_t> hasher;
        uint64_t hash = hasher(key);

        // Check all probe positions (same algorithm as MmapSSTableWriter)
        bool might_contain = true;
        for (int i = 0; i < bloom_num_probes_; ++i) {
            uint64_t h = hash + i * 0x9e3779b9ULL;  // Golden ratio constant
            size_t bit_index = h % bloom_num_bits_;

            // Check bit: bloom_bytes_[bit_index >> 3] & (1 << (bit_index & 7))
            uint8_t byte_val = bloom_bytes_[bit_index >> 3];
            uint8_t bit_mask = 1 << (bit_index & 7);

            if ((byte_val & bit_mask) == 0) {
                might_contain = false;
                break;
            }
        }

        if (!might_contain) {
            return false;  // Definitely doesn't exist
        }
    }

    // Conservative: might exist
    return true;
}

Status ArrowSSTableReader::Get(uint64_t key, std::string* value) const {
    // Debug logging disabled for benchmarking
    // std::cerr << "ArrowSSTableReader::Get called for key=" << key << "\n";
    // Fast path 1: Hot key cache (O(1), ~100ns)
    size_t cached_batch_idx, cached_row_idx;
    auto status = GetFromHotCache(key, value, &cached_batch_idx, &cached_row_idx);
    if (status.ok()) {
        stats_.cache_hits++;
        return status;
    }

    // Fast path 2: Negative cache (O(1), definitely doesn't exist)
    if (negative_cache_ && negative_cache_->DefinitelyNotExists(key)) {
        stats_.negative_cache_hits++;
        return Status::NotFound("Key not found (negative cache)");
    }

    // Fast path 3: Bloom filter (O(k), k=3-5 hash functions, ~500ns)
    if (!bloom_filter_loaded_) {
        const_cast<ArrowSSTableReader*>(this)->LoadBloomFilter();
    }

    // Check raw bloom filter (same algorithm as ContainsKey)
    if (!bloom_bytes_.empty() && bloom_num_bits_ > 0) {
        // Key is already an FNV-1a hash from Cython, so use directly
        // (std::hash<uint64_t> varies by platform and can cause bloom filter mismatch)
        uint64_t hash = key;

        bool might_contain = true;
        for (int i = 0; i < bloom_num_probes_; ++i) {
            // NOTE: Cast i to uint64_t to ensure 64-bit multiplication
            uint64_t h = hash + static_cast<uint64_t>(i) * 0x9e3779b9ULL;
            size_t bit_index = h % bloom_num_bits_;

            uint8_t byte_val = bloom_bytes_[bit_index >> 3];
            uint8_t bit_mask = 1 << (bit_index & 7);

            if ((byte_val & bit_mask) == 0) {
                might_contain = false;
                break;
            }
        }

        if (!might_contain) {
            stats_.bloom_filter_rejections++;
            return Status::NotFound("Key not found (bloom filter)");
        }
    }

    // Metadata range check (O(1))
    if (!IsKeyInRange(key)) {
        stats_.range_pruned++;
        return Status::NotFound("Key not in range");
    }

    // Sparse index lookup (O(log n) binary search, ~5μs)
    if (!sparse_index_loaded_) {
        const_cast<ArrowSSTableReader*>(this)->LoadSparseIndex();
    }

    if (!sparse_index_.empty()) {
        status = GetFromSparseIndex(key, value);
        if (status.ok()) {
            stats_.sparse_index_lookups++;
            // Promote to hot cache if accessed frequently
            if (hot_key_cache_ && hot_key_cache_->ShouldPromote(key)) {
                // Note: We would need batch_idx and row_idx to properly populate the cache
                // For now, just increment the counter - full caching requires additional state
                stats_.cache_promotions++;
            }
            return status;
        }
    }

    // Fallback: Full batch scan when sparse index is empty or doesn't contain key
    // This happens for small SSTables (<4096 entries) or between sparse index points
    if (!batches_loaded_) {
        const_cast<ArrowSSTableReader*>(this)->LoadArrowBatches();
    }

    stats_.full_scans++;
    for (const auto& batch : batches_) {
        auto key_array = std::static_pointer_cast<arrow::UInt64Array>(batch->column(0));
        auto value_array = std::static_pointer_cast<arrow::BinaryArray>(batch->column(1));

        // Check if we have the new tombstone column (column 3)
        // New format: [key, value, _ts, _tombstone]
        // Old format: [key, value]
        std::shared_ptr<arrow::BooleanArray> tombstone_array = nullptr;
        if (batch->num_columns() >= 4) {
            tombstone_array = std::static_pointer_cast<arrow::BooleanArray>(batch->column(3));
        }

        // Binary search within batch using raw pointer access
        const uint64_t* raw_keys = key_array->raw_values();
        int64_t len = key_array->length();
        auto lower = std::lower_bound(raw_keys, raw_keys + len, key);

        if (lower != raw_keys + len && *lower == key) {
            int64_t i = lower - raw_keys;

            // Check for tombstone - use column if available, else check marker
            bool is_tombstone = false;
            if (tombstone_array) {
                // Fast path: use Arrow boolean column (no string comparison)
                is_tombstone = tombstone_array->Value(i);
            } else {
                // Fallback for old SSTables: check value for tombstone marker
                int32_t length;
                const uint8_t* data_ptr = value_array->GetValue(i, &length);
                std::string val(reinterpret_cast<const char*>(data_ptr), length);
                is_tombstone = (val == kTombstoneMarker);
            }

            if (is_tombstone) {
                return Status::NotFound("Key was deleted");
            }

            int32_t length;
            const uint8_t* data_ptr = value_array->GetValue(i, &length);
            *value = std::string(reinterpret_cast<const char*>(data_ptr), length);
            return Status::OK();
        }
    }

    // Record miss in negative cache
    if (negative_cache_) {
        negative_cache_->RecordMiss(key);
    }

    return Status::NotFound("Key not found");
}

Status ArrowSSTableReader::GetFromHotCache(uint64_t key, std::string* value,
                                           size_t* batch_idx, size_t* row_idx) const {
    if (!hot_key_cache_) {
        return Status::NotFound("Hot cache not enabled");
    }

    // Hot cache lookup (now supports uint64_t)
    HotKeyEntry entry;
    if (hot_key_cache_->Get(key, &entry)) {
        // Cache hit - retrieve from batch
        *batch_idx = entry.sstable_offset;  // We store batch index here
        *row_idx = entry.row_index;
        return GetFromBatchIndex(*batch_idx, *row_idx, value);
    }

    return Status::NotFound("Not in hot cache");
}

Status ArrowSSTableReader::GetFromBatchIndex(size_t batch_idx, size_t row_idx,
                                              std::string* value) const {
    if (!batches_loaded_) {
        const_cast<ArrowSSTableReader*>(this)->LoadArrowBatches();
    }

    if (batch_idx >= batches_.size()) {
        return Status::InvalidArgument("Batch index out of range");
    }

    auto batch = batches_[batch_idx];
    if (row_idx >= static_cast<size_t>(batch->num_rows())) {
        return Status::InvalidArgument("Row index out of range");
    }

    // Check for tombstone - use column if available (fast path)
    if (batch->num_columns() >= 4) {
        auto tombstone_array = std::static_pointer_cast<arrow::BooleanArray>(batch->column(3));
        if (tombstone_array->Value(row_idx)) {
            return Status::NotFound("Key was deleted");
        }
    }

    auto value_array = std::static_pointer_cast<arrow::BinaryArray>(batch->column(1));
    int32_t length;
    const uint8_t* data_ptr = value_array->GetValue(row_idx, &length);
    *value = std::string(reinterpret_cast<const char*>(data_ptr), length);

    // Fallback tombstone check for old SSTables (without _tombstone column)
    if (batch->num_columns() < 4 && *value == kTombstoneMarker) {
        return Status::NotFound("Key was deleted");
    }

    return Status::OK();
}

Status ArrowSSTableReader::GetFromSparseIndex(uint64_t key, std::string* value) const {
    if (sparse_index_.empty()) {
        return Status::InvalidArgument("Sparse index not loaded");
    }

    // Binary search in sparse index to find batch
    auto it = std::lower_bound(sparse_index_.begin(), sparse_index_.end(), key,
        [](const auto& entry, uint64_t k) { return entry.first < k; });

    if (it == sparse_index_.end() || (it != sparse_index_.begin() && key < it->first)) {
        if (it != sparse_index_.begin()) --it;
    }

    size_t batch_idx = it->second;

    // Load batches if not already loaded
    if (!batches_loaded_) {
        const_cast<ArrowSSTableReader*>(this)->LoadArrowBatches();
    }

    if (batch_idx >= batches_.size()) {
        return Status::InvalidArgument("Batch index out of range");
    }

    // Scan the batch for the key (Arrow columnar access)
    auto batch = batches_[batch_idx];
    auto key_array = std::static_pointer_cast<arrow::UInt64Array>(batch->column(0));
    auto value_array = std::static_pointer_cast<arrow::BinaryArray>(batch->column(1));

    // Binary search within batch using raw pointer access
    const uint64_t* raw_keys = key_array->raw_values();
    int64_t len = key_array->length();
    auto lower = std::lower_bound(raw_keys, raw_keys + len, key);

    if (lower != raw_keys + len && *lower == key) {
        int64_t i = lower - raw_keys;
        int32_t length;
        const uint8_t* data_ptr = value_array->GetValue(i, &length);
        *value = std::string(reinterpret_cast<const char*>(data_ptr), length);
        return Status::OK();
    }

    return Status::NotFound("Key not found in batch");
}

Status ArrowSSTableReader::MultiGet(const std::vector<uint64_t>& keys,
                                     std::vector<std::string>* values) const {
    values->clear();
    values->reserve(keys.size());

    for (uint64_t key : keys) {
        std::string value;
        auto status = Get(key, &value);
        if (!status.ok()) {
            values->push_back("");  // Placeholder for missing keys
        } else {
            values->push_back(value);
        }
    }

    return Status::OK();
}

Status ArrowSSTableReader::Scan(uint64_t start_key, uint64_t end_key,
                                 std::vector<std::pair<uint64_t, std::string>>* results) const {
    // Metadata range pruning
    if (end_key < metadata_.min_key || start_key > metadata_.max_key) {
        return Status::OK();  // No overlap
    }

    // Load batches
    if (!batches_loaded_) {
        const_cast<ArrowSSTableReader*>(this)->LoadArrowBatches();
    }

    // Find starting batch via sparse index
    size_t start_batch = 0;
    if (!sparse_index_.empty()) {
        auto it = std::lower_bound(sparse_index_.begin(), sparse_index_.end(), start_key,
            [](const auto& entry, uint64_t k) { return entry.first < k; });
        if (it != sparse_index_.begin()) --it;
        start_batch = it->second;
    }

    // Scan batches in range
    for (size_t batch_idx = start_batch; batch_idx < batches_.size(); ++batch_idx) {
        auto batch = batches_[batch_idx];
        auto key_array = std::static_pointer_cast<arrow::UInt64Array>(batch->column(0));
        auto value_array = std::static_pointer_cast<arrow::BinaryArray>(batch->column(1));

        for (int64_t i = 0; i < key_array->length(); ++i) {
            uint64_t key = key_array->Value(i);

            if (key > end_key) return Status::OK();  // Done
            if (key >= start_key) {
                int32_t length;
                const uint8_t* data_ptr = value_array->GetValue(i, &length);
                results->emplace_back(key, std::string(reinterpret_cast<const char*>(data_ptr), length));
            }
        }
    }

    stats_.full_scans++;
    return Status::OK();
}

Status ArrowSSTableReader::ScanBatches(uint64_t start_key, uint64_t end_key,
                                        std::vector<std::shared_ptr<arrow::RecordBatch>>* batches) const {
    batches->clear();

    // Fast path 1: Metadata range pruning (SSTable-level zone map)
    if (end_key < metadata_.min_key || start_key > metadata_.max_key) {
        return Status::OK();  // No overlap
    }

    // Load batches from Arrow IPC file (one-time deserialization)
    if (!batches_loaded_) {
        const_cast<ArrowSSTableReader*>(this)->LoadArrowBatches();
    }

    // Find starting batch via sparse index (binary search)
    size_t start_batch = 0;
    if (!sparse_index_.empty() && !sparse_index_loaded_) {
        const_cast<ArrowSSTableReader*>(this)->LoadSparseIndex();
    }

    if (!sparse_index_.empty()) {
        auto it = std::lower_bound(sparse_index_.begin(), sparse_index_.end(), start_key,
            [](const auto& entry, uint64_t k) { return entry.first < k; });
        if (it != sparse_index_.begin()) --it;
        start_batch = it->second;
    }

    // Scan batches in range and collect RecordBatches
    // Key optimization: Return batches directly instead of extracting rows
    for (size_t batch_idx = start_batch; batch_idx < batches_.size(); ++batch_idx) {
        auto batch = batches_[batch_idx];
        auto key_array = std::static_pointer_cast<arrow::UInt64Array>(batch->column(0));

        // Check if this batch overlaps with scan range
        if (key_array->length() == 0) continue;

        uint64_t batch_min = key_array->Value(0);
        uint64_t batch_max = key_array->Value(key_array->length() - 1);

        // Fast path 2: Batch-level zone map pruning
        if (batch_max < start_key) continue;  // Batch is before range
        if (batch_min > end_key) break;       // Batch is after range, done

        // Batch overlaps with range - add it
        // Note: May contain keys outside range, caller must filter if needed
        batches->push_back(batch);
    }

    return Status::OK();
}

Status ArrowSSTableReader::GetAllKeys(std::vector<uint64_t>* keys) const {
    if (!batches_loaded_) {
        const_cast<ArrowSSTableReader*>(this)->LoadArrowBatches();
    }

    keys->clear();
    for (const auto& batch : batches_) {
        auto key_array = std::static_pointer_cast<arrow::UInt64Array>(batch->column(0));
        for (int64_t i = 0; i < key_array->length(); ++i) {
            keys->push_back(key_array->Value(i));
        }
    }

    return Status::OK();
}

std::string ArrowSSTableReader::GetFilePath() const {
    return filepath_;
}

uint64_t ArrowSSTableReader::GetFileSize() const {
    return metadata_.file_size;
}

Status ArrowSSTableReader::Validate() const {
    if (metadata_.record_count == 0) {
        return Status::InvalidArgument("SSTable has no records");
    }

    if (metadata_.min_key > metadata_.max_key) {
        return Status::InvalidArgument("SSTable key range is invalid");
    }

    return Status::OK();
}

Status ArrowSSTableReader::ReadFooter(int fd, uint64_t* data_end, uint64_t* index_end) {
    // Read last 24 bytes: [data_end(8)][index_end(8)][magic(8)]
    off_t file_size = lseek(fd, 0, SEEK_END);
    if (file_size < 24) {
        return Status::InvalidArgument("File too small for Arrow SSTable format");
    }

    uint8_t footer[24];
    if (lseek(fd, file_size - 24, SEEK_SET) == -1) {
        return Status::IOError("Failed to seek to footer");
    }

    if (read(fd, footer, 24) != 24) {
        return Status::IOError("Failed to read footer");
    }

    // Parse footer
    *data_end = *reinterpret_cast<uint64_t*>(&footer[0]);
    *index_end = *reinterpret_cast<uint64_t*>(&footer[8]);
    uint64_t magic = *reinterpret_cast<uint64_t*>(&footer[16]);

    if (magic != 0x4152524F57535354) {  // "ARROWSST"
        return Status::InvalidArgument("Invalid magic number - not Arrow SSTable format");
    }

    return Status::OK();
}

Status ArrowSSTableReader::LoadFooter() {
    int fd = open(filepath_.c_str(), O_RDONLY);
    if (fd < 0) {
        return Status::IOError("Failed to open file: " + filepath_);
    }

    auto status = ReadFooter(fd, &data_section_end_, &index_section_end_);
    close(fd);

    if (!status.ok()) {
        return status;
    }

    MARBLE_LOG_DEBUG(ArrowReader) << "Footer loaded - data_end=" << data_section_end_
                                   << ", index_end=" << index_section_end_;

    return Status::OK();
}

Status ArrowSSTableReader::LoadSparseIndex() {
    if (sparse_index_loaded_) {
        return Status::OK();
    }

    // Load footer if not already loaded
    if (data_section_end_ == 0) {
        auto status = LoadFooter();
        if (!status.ok()) return status;
    }

    int fd = open(filepath_.c_str(), O_RDONLY);
    if (fd < 0) {
        return Status::IOError("Failed to open file: " + filepath_);
    }

    // Seek to sparse index section (right after bloom filter)
    if (lseek(fd, bloom_section_end_, SEEK_SET) == -1) {
        close(fd);
        return Status::IOError("Failed to seek to sparse index");
    }

    // Read index count
    uint64_t index_count;
    if (read(fd, &index_count, sizeof(uint64_t)) != sizeof(uint64_t)) {
        close(fd);
        return Status::IOError("Failed to read sparse index count");
    }

    // Read index entries
    sparse_index_.reserve(index_count);
    for (uint64_t i = 0; i < index_count; ++i) {
        uint64_t key, batch_idx;
        if (read(fd, &key, sizeof(uint64_t)) != sizeof(uint64_t)) {
            close(fd);
            return Status::IOError("Failed to read sparse index key");
        }
        if (read(fd, &batch_idx, sizeof(uint64_t)) != sizeof(uint64_t)) {
            close(fd);
            return Status::IOError("Failed to read sparse index batch index");
        }
        sparse_index_.emplace_back(key, batch_idx);
    }

    close(fd);
    sparse_index_loaded_ = true;

    MARBLE_LOG_DEBUG(ArrowReader) << "Loaded sparse index with " << sparse_index_.size()
                                   << " entries";

    return Status::OK();
}

Status ArrowSSTableReader::LoadBloomFilter() {
    if (bloom_filter_loaded_) {
        return Status::OK();
    }

    // Check if bloom filter section exists
    if (bloom_section_end_ == data_section_end_) {
        // No bloom filter written
        bloom_filter_loaded_ = true;
        return Status::OK();
    }

    int fd = open(filepath_.c_str(), O_RDONLY);
    if (fd < 0) {
        return Status::IOError("Failed to open file for bloom filter: " + filepath_);
    }

    // Seek to bloom filter section (right after Arrow IPC data)
    if (lseek(fd, data_section_end_, SEEK_SET) == -1) {
        close(fd);
        return Status::IOError("Failed to seek to bloom filter section");
    }

    // Read bloom filter metadata: [NUM_BITS(8)][NUM_PROBES(4)]
    uint64_t num_bits;
    int num_probes;

    if (read(fd, &num_bits, sizeof(uint64_t)) != sizeof(uint64_t)) {
        close(fd);
        return Status::IOError("Failed to read bloom filter num_bits");
    }

    if (read(fd, &num_probes, sizeof(int)) != sizeof(int)) {
        close(fd);
        return Status::IOError("Failed to read bloom filter num_probes");
    }

    // Calculate number of bytes
    size_t num_bytes = (num_bits + 7) / 8;

    // Read bloom filter bytes
    bloom_bytes_.resize(num_bytes);
    ssize_t bytes_read = read(fd, bloom_bytes_.data(), num_bytes);
    close(fd);

    if (bytes_read != static_cast<ssize_t>(num_bytes)) {
        return Status::IOError("Failed to read bloom filter bytes");
    }

    // Store bloom filter parameters
    bloom_num_bits_ = num_bits;
    bloom_num_probes_ = num_probes;

    bloom_filter_loaded_ = true;
    MARBLE_LOG_DEBUG(ArrowReader) << "Loaded bloom filter: " << num_bits << " bits, "
                                   << num_probes << " probes, " << num_bytes << " bytes";

    return Status::OK();
}

Status ArrowSSTableReader::LoadArrowBatches() {
    if (batches_loaded_) {
        return Status::OK();
    }

    MARBLE_LOG_DEBUG(ArrowReader) << "Loading Arrow IPC batches from " << filepath_;

    // Open file with Arrow FileInputStream
    auto file_result = arrow::io::ReadableFile::Open(filepath_);
    if (!file_result.ok()) {
        return Status::IOError("Failed to open Arrow file: " + file_result.status().ToString());
    }
    auto arrow_file = file_result.ValueOrDie();

    // Create IPC StreamReader
    auto reader_result = arrow::ipc::RecordBatchStreamReader::Open(arrow_file);
    if (!reader_result.ok()) {
        return Status::IOError("Failed to create IPC reader: " + reader_result.status().ToString());
    }
    auto ipc_reader = reader_result.ValueOrDie();

    // Read all batches
    arrow_schema_ = ipc_reader->schema();
    while (true) {
        std::shared_ptr<arrow::RecordBatch> batch;
        auto read_result = ipc_reader->ReadNext(&batch);
        if (!read_result.ok()) {
            return Status::IOError("Failed to read RecordBatch: " + read_result.ToString());
        }
        if (!batch) break;  // End of stream

        batches_.push_back(batch);
    }

    batches_loaded_ = true;

    MARBLE_LOG_DEBUG(ArrowReader) << "Loaded " << batches_.size() << " RecordBatches";

    return Status::OK();
}

// Factory function
std::unique_ptr<ArrowSSTableReader> OpenArrowSSTable(
    const std::string& filepath,
    std::shared_ptr<FileSystem> fs) {

    MARBLE_LOG_DEBUG(ArrowReader) << "Opening " << filepath;

    if (!fs) {
        fs = FileSystem::CreateLocal();
    }

    // Read footer to get metadata
    int fd = open(filepath.c_str(), O_RDONLY);
    if (fd < 0) {
        MARBLE_LOG_ERROR(ArrowReader) << "Failed to open file";
        return nullptr;
    }

    // Read last 32 bytes for footer
    off_t file_size = lseek(fd, 0, SEEK_END);
    MARBLE_LOG_DEBUG(ArrowReader) << "file_size=" << file_size << " bytes";

    if (file_size < 32) {
        MARBLE_LOG_ERROR(ArrowReader) << "ERROR - file too small (need 32 bytes minimum)";
        close(fd);
        return nullptr;
    }

    uint8_t footer[32];
    if (lseek(fd, file_size - 32, SEEK_SET) == -1) {
        MARBLE_LOG_ERROR(ArrowReader) << "ERROR - failed to seek to footer";
        close(fd);
        return nullptr;
    }

    if (read(fd, footer, 32) != 32) {
        MARBLE_LOG_ERROR(ArrowReader) << "ERROR - failed to read footer";
        close(fd);
        return nullptr;
    }

    uint64_t data_end = *reinterpret_cast<uint64_t*>(&footer[0]);
    uint64_t bloom_end = *reinterpret_cast<uint64_t*>(&footer[8]);
    uint64_t metadata_start = *reinterpret_cast<uint64_t*>(&footer[16]);
    uint64_t magic = *reinterpret_cast<uint64_t*>(&footer[24]);

    MARBLE_LOG_DEBUG(ArrowReader) << "Footer - data_end=" << data_end
                                   << ", bloom_end=" << bloom_end
                                   << ", metadata_start=" << metadata_start
                                   << ", magic=0x" << std::hex << magic << std::dec;

    if (magic != 0x4152524F57535354) {  // "ARROWSST"
        MARBLE_LOG_ERROR(ArrowReader) << "ERROR - invalid magic number (expected 0x4152524F57535354)";
        close(fd);
        return nullptr;  // Not Arrow format
    }

    MARBLE_LOG_DEBUG(ArrowReader) << "Magic number verified - this is Arrow format";

    // Read metadata section (starts at metadata_start, ends at footer)
    MARBLE_LOG_DEBUG(ArrowReader) << "Seeking to metadata at offset " << metadata_start;
    if (lseek(fd, metadata_start, SEEK_SET) == -1) {
        MARBLE_LOG_ERROR(ArrowReader) << "ERROR - failed to seek to metadata section";
        close(fd);
        return nullptr;
    }

    SSTableMetadata metadata;
    uint64_t entry_count, min_key, max_key, level;

    MARBLE_LOG_DEBUG(ArrowReader) << "Reading metadata fields...";
    ssize_t bytes_read;

    bytes_read = read(fd, &entry_count, sizeof(uint64_t));
    if (bytes_read != sizeof(uint64_t)) {
        MARBLE_LOG_ERROR(ArrowReader) << "ERROR - failed to read entry_count (got " << bytes_read << " bytes)";
        close(fd);
        return nullptr;
    }

    bytes_read = read(fd, &min_key, sizeof(uint64_t));
    if (bytes_read != sizeof(uint64_t)) {
        MARBLE_LOG_ERROR(ArrowReader) << "ERROR - failed to read min_key (got " << bytes_read << " bytes)";
        close(fd);
        return nullptr;
    }

    bytes_read = read(fd, &max_key, sizeof(uint64_t));
    if (bytes_read != sizeof(uint64_t)) {
        MARBLE_LOG_ERROR(ArrowReader) << "ERROR - failed to read max_key (got " << bytes_read << " bytes)";
        close(fd);
        return nullptr;
    }

    bytes_read = read(fd, &level, sizeof(uint64_t));
    if (bytes_read != sizeof(uint64_t)) {
        MARBLE_LOG_ERROR(ArrowReader) << "ERROR - failed to read level (got " << bytes_read << " bytes)";
        close(fd);
        return nullptr;
    }

    // Read data column value ranges (for predicate pushdown)
    // These fields are optional - older SSTables won't have them
    uint64_t data_min_key = 0;
    uint64_t data_max_key = 0;
    bool has_data_range = false;

    bytes_read = read(fd, &data_min_key, sizeof(uint64_t));
    if (bytes_read == sizeof(uint64_t)) {
        bytes_read = read(fd, &data_max_key, sizeof(uint64_t));
        if (bytes_read == sizeof(uint64_t)) {
            uint8_t has_data_range_byte = 0;
            bytes_read = read(fd, &has_data_range_byte, sizeof(uint8_t));
            if (bytes_read == sizeof(uint8_t)) {
                has_data_range = (has_data_range_byte != 0);
            }
        }
    }

    close(fd);

    MARBLE_LOG_DEBUG(ArrowReader) << "Metadata - entries=" << entry_count
                                   << ", min_key=" << min_key
                                   << ", max_key=" << max_key
                                   << ", level=" << level;

    // Sanity checks
    if (entry_count == 0) {
        MARBLE_LOG_ERROR(ArrowReader) << "ERROR - zero entries in metadata";
        return nullptr;
    }
    if (min_key > max_key) {
        MARBLE_LOG_ERROR(ArrowReader) << "ERROR - invalid key range (min > max)";
        return nullptr;
    }

    metadata.filename = filepath;
    metadata.file_size = file_size;
    metadata.min_key = min_key;
    metadata.max_key = max_key;
    metadata.record_count = entry_count;
    metadata.level = level;

    // Data column value ranges (for predicate pushdown)
    metadata.data_min_key = data_min_key;
    metadata.data_max_key = data_max_key;
    metadata.has_data_range = has_data_range;

    MARBLE_LOG_DEBUG(ArrowReader) << "Creating ArrowSSTableReader...";
    try {
        auto reader = std::make_unique<ArrowSSTableReader>(filepath, metadata, fs);

        // Store footer offsets in the reader
        reader->SetFooterOffsets(data_end, bloom_end, metadata_start);

        MARBLE_LOG_DEBUG(ArrowReader) << "Successfully created reader";
        return reader;
    } catch (const std::exception& e) {
        MARBLE_LOG_ERROR(ArrowReader) << "EXCEPTION during construction: " << e.what();
        return nullptr;
    } catch (...) {
        MARBLE_LOG_ERROR(ArrowReader) << "UNKNOWN EXCEPTION during construction";
        return nullptr;
    }
}

} // namespace marble
