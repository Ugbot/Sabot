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
#include <fcntl.h>
#include <unistd.h>
#include <iostream>
#include <algorithm>
#include <arrow/io/file.h>
#include <arrow/ipc/reader.h>

namespace marble {

ArrowSSTableReader::ArrowSSTableReader(const std::string& filepath,
                                       const SSTableMetadata& metadata,
                                       std::shared_ptr<FileSystem> fs)
    : filepath_(filepath)
    , metadata_(metadata)
    , fs_(fs) {

    std::cerr << "ArrowSSTableReader: Constructor START\n";
    std::cerr << "ArrowSSTableReader:   filepath=" << filepath << "\n";
    std::cerr << "ArrowSSTableReader:   entries=" << metadata.record_count << "\n";
    std::cerr << "ArrowSSTableReader:   key_range=[" << metadata.min_key << "," << metadata.max_key << "]\n";

    // Create hot key cache for this SSTable
    std::cerr << "ArrowSSTableReader: Creating HotKeyCache...\n";
    HotKeyCacheConfig cache_config;
    cache_config.max_entries = 10000;
    cache_config.promotion_threshold = 3;
    hot_key_cache_ = CreateHotKeyCache(cache_config);
    std::cerr << "ArrowSSTableReader: HotKeyCache created\n";

    // Create negative cache
    std::cerr << "ArrowSSTableReader: Creating HotKeyNegativeCache...\n";
    negative_cache_ = CreateHotKeyNegativeCache(10000);
    std::cerr << "ArrowSSTableReader: HotKeyNegativeCache created\n";

    std::cerr << "ArrowSSTableReader: Constructor COMPLETE for " << filepath_ << "\n";
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

    // Fast path 2: Negative cache
    // TODO: NegativeCache uses Key type, need to adapt for uint64_t
    // if (negative_cache_ && negative_cache_->DefinitelyNotExists(key)) {
    //     return false;
    // }

    // Fast path 3: Bloom filter
    if (!bloom_filter_loaded_) {
        const_cast<ArrowSSTableReader*>(this)->LoadBloomFilter();
    }
    if (bloom_filter_ && !bloom_filter_->MightContain(std::to_string(key))) {
        return false;
    }

    // Conservative: might exist
    return true;
}

Status ArrowSSTableReader::Get(uint64_t key, std::string* value) const {
    std::cerr << "ArrowSSTableReader::Get called for key=" << key << "\n";
    // Fast path 1: Hot key cache (O(1), ~100ns)
    size_t cached_batch_idx, cached_row_idx;
    auto status = GetFromHotCache(key, value, &cached_batch_idx, &cached_row_idx);
    if (status.ok()) {
        stats_.cache_hits++;
        return status;
    }

    // Fast path 2: Negative cache (O(1), definitely doesn't exist)
    // TODO: NegativeCache uses Key type, need to adapt for uint64_t
    // if (negative_cache_ && negative_cache_->DefinitelyNotExists(key)) {
    //     stats_.negative_cache_hits++;
    //     return Status::NotFound("Key not found (negative cache)");
    // }

    // Fast path 3: Bloom filter (O(k), k=3-5 hash functions, ~500ns)
    if (!bloom_filter_loaded_) {
        const_cast<ArrowSSTableReader*>(this)->LoadBloomFilter();
    }
    if (bloom_filter_ && !bloom_filter_->MightContain(std::to_string(key))) {
        stats_.bloom_filter_rejections++;
        // TODO: NegativeCache uses Key type
        // if (negative_cache_) {
        //     negative_cache_->RecordMiss(key);
        // }
        return Status::NotFound("Key not found (bloom filter)");
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
            // TODO: Promote to hot cache if accessed frequently
            // HotKeyCache uses Key type, need to adapt for uint64_t
            // if (hot_key_cache_ && hot_key_cache_->ShouldPromote(key)) {
            //     stats_.cache_promotions++;
            // }
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

        // Binary search within batch
        for (int64_t i = 0; i < key_array->length(); ++i) {
            if (key_array->Value(i) == key) {
                int32_t length;
                const uint8_t* data_ptr = value_array->GetValue(i, &length);
                *value = std::string(reinterpret_cast<const char*>(data_ptr), length);
                return Status::OK();
            }
            if (key_array->Value(i) > key) break;  // Keys are sorted
        }
    }

    // TODO: Record miss in negative cache (uses Key type)
    // if (negative_cache_) {
    //     negative_cache_->RecordMiss(key);
    // }

    return Status::NotFound("Key not found");
}

Status ArrowSSTableReader::GetFromHotCache(uint64_t key, std::string* value,
                                           size_t* batch_idx, size_t* row_idx) const {
    if (!hot_key_cache_) {
        return Status::NotFound("Hot cache not enabled");
    }

    // TODO: HotKeyCache uses Key type, need to adapt for uint64_t
    // HotKeyEntry entry;
    // if (hot_key_cache_->Get(key, &entry)) {
    //     // Cache hit - retrieve from batch
    //     *batch_idx = entry.sstable_offset;  // We store batch index here
    //     *row_idx = entry.row_index;
    //     return GetFromBatchIndex(*batch_idx, *row_idx, value);
    // }

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

    auto value_array = std::static_pointer_cast<arrow::BinaryArray>(batch->column(1));
    int32_t length;
    const uint8_t* data_ptr = value_array->GetValue(row_idx, &length);
    *value = std::string(reinterpret_cast<const char*>(data_ptr), length);

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

    // Binary search within batch
    for (int64_t i = 0; i < key_array->length(); ++i) {
        if (key_array->Value(i) == key) {
            int32_t length;
            const uint8_t* data_ptr = value_array->GetValue(i, &length);
            *value = std::string(reinterpret_cast<const char*>(data_ptr), length);
            return Status::OK();
        }
        if (key_array->Value(i) > key) break;  // Keys are sorted
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

    std::cerr << "ArrowSSTableReader: Footer loaded - data_end=" << data_section_end_
              << ", index_end=" << index_section_end_ << "\n";

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

    // Seek to sparse index section (right after Arrow IPC data)
    if (lseek(fd, data_section_end_, SEEK_SET) == -1) {
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

    std::cerr << "ArrowSSTableReader: Loaded sparse index with " << sparse_index_.size()
              << " entries\n";

    return Status::OK();
}

Status ArrowSSTableReader::LoadBloomFilter() {
    // TODO: Read bloom filter from metadata section
    // For now, skip - the writer doesn't create one yet
    bloom_filter_loaded_ = true;
    return Status::OK();
}

Status ArrowSSTableReader::LoadArrowBatches() {
    if (batches_loaded_) {
        return Status::OK();
    }

    std::cerr << "ArrowSSTableReader: Loading Arrow IPC batches from " << filepath_ << "\n";

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

    std::cerr << "ArrowSSTableReader: Loaded " << batches_.size() << " RecordBatches\n";

    return Status::OK();
}

// Factory function
std::unique_ptr<ArrowSSTableReader> OpenArrowSSTable(
    const std::string& filepath,
    std::shared_ptr<FileSystem> fs) {

    std::cerr << "OpenArrowSSTable: Opening " << filepath << "\n";

    if (!fs) {
        fs = FileSystem::CreateLocal();
    }

    // Read footer to get metadata
    int fd = open(filepath.c_str(), O_RDONLY);
    if (fd < 0) {
        std::cerr << "OpenArrowSSTable: Failed to open file\n";
        return nullptr;
    }

    // Read last 24 bytes for footer
    off_t file_size = lseek(fd, 0, SEEK_END);
    std::cerr << "OpenArrowSSTable: file_size=" << file_size << " bytes\n";

    if (file_size < 24) {
        std::cerr << "OpenArrowSSTable: ERROR - file too small (need 24 bytes minimum)\n";
        close(fd);
        return nullptr;
    }

    uint8_t footer[24];
    if (lseek(fd, file_size - 24, SEEK_SET) == -1) {
        std::cerr << "OpenArrowSSTable: ERROR - failed to seek to footer\n";
        close(fd);
        return nullptr;
    }

    if (read(fd, footer, 24) != 24) {
        std::cerr << "OpenArrowSSTable: ERROR - failed to read footer\n";
        close(fd);
        return nullptr;
    }

    uint64_t data_end = *reinterpret_cast<uint64_t*>(&footer[0]);
    uint64_t index_end = *reinterpret_cast<uint64_t*>(&footer[8]);
    uint64_t magic = *reinterpret_cast<uint64_t*>(&footer[16]);

    std::cerr << "OpenArrowSSTable: Footer - data_end=" << data_end
              << ", index_end=" << index_end
              << ", magic=0x" << std::hex << magic << std::dec << "\n";

    if (magic != 0x4152524F57535354) {  // "ARROWSST"
        std::cerr << "OpenArrowSSTable: ERROR - invalid magic number (expected 0x4152524F57535354)\n";
        close(fd);
        return nullptr;  // Not Arrow format
    }

    std::cerr << "OpenArrowSSTable: Magic number verified - this is Arrow format\n";

    // Read metadata section (between index_end and footer)
    std::cerr << "OpenArrowSSTable: Seeking to metadata at offset " << index_end << "\n";
    if (lseek(fd, index_end, SEEK_SET) == -1) {
        std::cerr << "OpenArrowSSTable: ERROR - failed to seek to metadata section\n";
        close(fd);
        return nullptr;
    }

    SSTableMetadata metadata;
    uint64_t entry_count, min_key, max_key, level;

    std::cerr << "OpenArrowSSTable: Reading metadata fields...\n";
    ssize_t bytes_read;

    bytes_read = read(fd, &entry_count, sizeof(uint64_t));
    if (bytes_read != sizeof(uint64_t)) {
        std::cerr << "OpenArrowSSTable: ERROR - failed to read entry_count (got " << bytes_read << " bytes)\n";
        close(fd);
        return nullptr;
    }

    bytes_read = read(fd, &min_key, sizeof(uint64_t));
    if (bytes_read != sizeof(uint64_t)) {
        std::cerr << "OpenArrowSSTable: ERROR - failed to read min_key (got " << bytes_read << " bytes)\n";
        close(fd);
        return nullptr;
    }

    bytes_read = read(fd, &max_key, sizeof(uint64_t));
    if (bytes_read != sizeof(uint64_t)) {
        std::cerr << "OpenArrowSSTable: ERROR - failed to read max_key (got " << bytes_read << " bytes)\n";
        close(fd);
        return nullptr;
    }

    bytes_read = read(fd, &level, sizeof(uint64_t));
    if (bytes_read != sizeof(uint64_t)) {
        std::cerr << "OpenArrowSSTable: ERROR - failed to read level (got " << bytes_read << " bytes)\n";
        close(fd);
        return nullptr;
    }

    close(fd);

    std::cerr << "OpenArrowSSTable: Metadata - entries=" << entry_count
              << ", min_key=" << min_key
              << ", max_key=" << max_key
              << ", level=" << level << "\n";

    // Sanity checks
    if (entry_count == 0) {
        std::cerr << "OpenArrowSSTable: ERROR - zero entries in metadata\n";
        return nullptr;
    }
    if (min_key > max_key) {
        std::cerr << "OpenArrowSSTable: ERROR - invalid key range (min > max)\n";
        return nullptr;
    }

    metadata.filename = filepath;
    metadata.file_size = file_size;
    metadata.min_key = min_key;
    metadata.max_key = max_key;
    metadata.record_count = entry_count;
    metadata.level = level;

    std::cerr << "OpenArrowSSTable: Creating ArrowSSTableReader...\n";
    try {
        auto reader = std::make_unique<ArrowSSTableReader>(filepath, metadata, fs);
        std::cerr << "OpenArrowSSTable: Successfully created reader\n";
        return reader;
    } catch (const std::exception& e) {
        std::cerr << "OpenArrowSSTable: EXCEPTION during construction: " << e.what() << "\n";
        return nullptr;
    } catch (...) {
        std::cerr << "OpenArrowSSTable: UNKNOWN EXCEPTION during construction\n";
        return nullptr;
    }
}

} // namespace marble
