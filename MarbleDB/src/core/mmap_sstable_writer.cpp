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

#include "marble/mmap_sstable_writer.h"
#include "marble/sstable.h"
#include <sys/mman.h>
#include <unistd.h>
#include <fcntl.h>
#include <cstring>
#include <algorithm>
#include <iostream>
#include <arrow/io/file.h>
#include <arrow/io/memory.h>
#include <arrow/ipc/writer.h>
#include <arrow/ipc/reader.h>
#include <arrow/compute/api.h>

namespace marble {

MmapSSTableWriter::MmapSSTableWriter(const std::string& filepath,
                                     uint64_t level,
                                     std::shared_ptr<FileSystem> fs,
                                     SSTableManager* sstable_mgr,
                                     size_t zone_size,
                                     bool use_async_msync)
    : filepath_(filepath)
    , level_(level)
    , fs_(fs)
    , sstable_mgr_(sstable_mgr)
    , fd_(-1)
    , mapped_region_(nullptr)
    , zone_size_(zone_size)
    , current_file_size_(0)
    , write_offset_(0)
    , use_async_msync_(use_async_msync)
    , entry_count_(0)
    , min_key_(UINT64_MAX)
    , max_key_(0)
    , data_section_start_(0)
    , data_section_end_(0)
    , finished_(false)
    , bits_per_key_(10.0) {  // 10 bits per key (~1% false positive rate)

    // Create Arrow schema with full metadata columns for searchability
    // Column layout:
    //   0: key (uint64)    - Primary key / LSM storage key
    //   1: value (binary)  - Serialized RecordBatch data
    //   2: _ts (int64)     - MVCC timestamp for visibility/ordering
    //   3: _tombstone (bool) - Deletion marker for efficient filtering
    arrow_schema_ = arrow::schema({
        arrow::field("key", arrow::uint64()),
        arrow::field("value", arrow::binary()),
        arrow::field("_ts", arrow::int64()),
        arrow::field("_tombstone", arrow::boolean())
    });

    // Reserve batch buffers
    batch_keys_.reserve(kRecordBatchSize);
    batch_values_.reserve(kRecordBatchSize);
    batch_timestamps_.reserve(kRecordBatchSize);
    batch_tombstones_.reserve(kRecordBatchSize);

    // Reserve bloom filter hash storage
    key_hashes_.reserve(10000);  // Pre-allocate for typical SSTable size
}

MmapSSTableWriter::~MmapSSTableWriter() {
    if (mapped_region_ && !finished_) {
        // Cleanup if Finish() wasn't called
        munmap(mapped_region_, current_file_size_);
        mapped_region_ = nullptr;
    }

    if (fd_ >= 0) {
        close(fd_);
        fd_ = -1;
    }
}

Status MmapSSTableWriter::Add(uint64_t key, const std::string& value) {
    if (finished_) {
        return Status::InvalidArgument("Cannot add to finished SSTable");
    }

    // Initialize on first add
    if (fd_ < 0) {
        // Open file for read/write, create if doesn't exist
        fd_ = open(filepath_.c_str(), O_RDWR | O_CREAT | O_TRUNC, 0644);
        if (fd_ < 0) {
            return Status::IOError("Failed to open file: " + filepath_);
        }

        // Pre-allocate initial zone
        if (ftruncate(fd_, zone_size_) != 0) {
            close(fd_);
            fd_ = -1;
            return Status::IOError("Failed to truncate file to zone size");
        }

        // Memory map the initial zone
        mapped_region_ = mmap(nullptr, zone_size_, PROT_READ | PROT_WRITE,
                             MAP_SHARED, fd_, 0);
        if (mapped_region_ == MAP_FAILED) {
            close(fd_);
            fd_ = -1;
            return Status::IOError("Failed to mmap file");
        }

        current_file_size_ = zone_size_;
        write_offset_ = 0;
        data_section_start_ = 0;
    }

    // Buffer entry in Arrow RecordBatch buffer with default metadata
    batch_keys_.push_back(key);
    batch_values_.push_back(value);
    batch_timestamps_.push_back(0);  // Default: no timestamp
    batch_tombstones_.push_back(value == kTombstoneMarker);  // Detect tombstone from value
    entry_count_++;

    // Collect hash for deferred bloom filter construction (RocksDB-style)
    // Note: Key is already an FNV-1a hash from Cython, so use directly
    // (std::hash<uint64_t> varies by platform and can cause bloom filter mismatch)
    uint64_t hash = key;
    // Hash deduplication: only add if different from previous hash
    if (key_hashes_.empty() || hash != key_hashes_.back()) {
        key_hashes_.push_back(hash);
    }

    // Update key range
    min_key_ = std::min(min_key_, key);
    max_key_ = std::max(max_key_, key);

    // Add to sparse index every N entries (track batch number for now)
    if (entry_count_ % kSparseIndexInterval == 0) {
        // Store key and batch index - will update with file offset later
        sparse_index_.emplace_back(key, record_batches_.size());
    }

    // Flush batch when buffer is full
    if (batch_keys_.size() >= kRecordBatchSize) {
        return FlushBatchBuffer();
    }

    return Status::OK();
}

Status MmapSSTableWriter::AddWithMetadata(uint64_t key, const std::string& value,
                                          uint64_t timestamp, bool is_tombstone) {
    if (finished_) {
        return Status::InvalidArgument("Cannot add to finished SSTable");
    }

    // Initialize on first add (same as Add())
    if (fd_ < 0) {
        fd_ = open(filepath_.c_str(), O_RDWR | O_CREAT | O_TRUNC, 0644);
        if (fd_ < 0) {
            return Status::IOError("Failed to open file: " + filepath_);
        }

        if (ftruncate(fd_, zone_size_) != 0) {
            close(fd_);
            fd_ = -1;
            return Status::IOError("Failed to truncate file to zone size");
        }

        mapped_region_ = mmap(nullptr, zone_size_, PROT_READ | PROT_WRITE,
                             MAP_SHARED, fd_, 0);
        if (mapped_region_ == MAP_FAILED) {
            close(fd_);
            fd_ = -1;
            return Status::IOError("Failed to mmap file");
        }

        current_file_size_ = zone_size_;
        write_offset_ = 0;
        data_section_start_ = 0;
    }

    // Buffer entry with explicit metadata
    batch_keys_.push_back(key);
    batch_values_.push_back(value);
    batch_timestamps_.push_back(timestamp);
    batch_tombstones_.push_back(is_tombstone);
    entry_count_++;

    // Collect hash for bloom filter
    uint64_t hash = key;
    if (key_hashes_.empty() || hash != key_hashes_.back()) {
        key_hashes_.push_back(hash);
    }

    // Update key range
    min_key_ = std::min(min_key_, key);
    max_key_ = std::max(max_key_, key);

    // Sparse index
    if (entry_count_ % kSparseIndexInterval == 0) {
        sparse_index_.emplace_back(key, record_batches_.size());
    }

    // Flush batch when full
    if (batch_keys_.size() >= kRecordBatchSize) {
        return FlushBatchBuffer();
    }

    return Status::OK();
}

Status MmapSSTableWriter::ExtendAndRemap() {
    // Unmap current region
    if (munmap(mapped_region_, current_file_size_) != 0) {
        return Status::IOError("Failed to unmap region during growth");
    }

    // Grow file by one zone
    size_t new_size = current_file_size_ + zone_size_;
    if (ftruncate(fd_, new_size) != 0) {
        return Status::IOError("Failed to extend file");
    }

    // Remap to larger region
    mapped_region_ = mmap(nullptr, new_size, PROT_READ | PROT_WRITE,
                         MAP_SHARED, fd_, 0);
    if (mapped_region_ == MAP_FAILED) {
        return Status::IOError("Failed to remap extended file");
    }

    current_file_size_ = new_size;

    return Status::OK();
}

Status MmapSSTableWriter::Finish(std::unique_ptr<SSTable>* sstable) {
    if (finished_) {
        return Status::InvalidArgument("SSTable already finished");
    }

    if (entry_count_ == 0) {
        return Status::InvalidArgument("Cannot finish empty SSTable");
    }

    // Step 0: Build bloom filter ONCE from collected hashes (BEFORE writing to file)
    BuildBloomFilterFromHashes();

    // Step 1: Write RecordBatches using Arrow IPC
    auto status = WriteIndex();
    if (!status.ok()) {
        return status;
    }

    // Step 2: Write bloom filter (BEFORE metadata/footer so magic is at end of file)
    if (!bloom_filter_bytes_.empty()) {
        status = WriteBloomFilter();
        if (!status.ok()) {
            return status;
        }
    }

    // Step 3: Write per-column statistics (zone maps)
    status = WriteColumnStats();
    if (!status.ok()) {
        return status;
    }

    // Step 4: Write metadata and footer (footer contains magic at END of file)
    status = WriteMetadata();
    if (!status.ok()) {
        return status;
    }

    // Step 5: Unmap memory region (if still mapped) and close file descriptor
    if (mapped_region_) {
        if (munmap(mapped_region_, current_file_size_) != 0) {
            return Status::IOError("Failed to unmap memory region");
        }
        mapped_region_ = nullptr;
    }

    if (fd_ >= 0) {
        if (close(fd_) != 0) {
            return Status::IOError("Failed to close file descriptor");
        }
        fd_ = -1;
    }

    // Step 6: Mark as finished
    finished_ = true;

    // Step 7: Try to reopen SSTable for reading

    if (sstable_mgr_) {

        status = sstable_mgr_->OpenSSTable(filepath_, sstable);

        if (!status.ok()) {
            *sstable = nullptr;
            return Status::OK();  // Don't fail - data is on disk
        }
    } else {
        *sstable = nullptr;
    }

    return Status::OK();
}

Status MmapSSTableWriter::CreateRecordBatchFromBuffer(std::shared_ptr<arrow::RecordBatch>* batch) {
    if (batch_keys_.empty()) {
        return Status::InvalidArgument("Cannot create RecordBatch from empty buffer");
    }

    // Build key column (UInt64Array)
    arrow::UInt64Builder key_builder;
    auto status_arrow = key_builder.AppendValues(batch_keys_);
    if (!status_arrow.ok()) {
        return Status::IOError("Failed to append keys: " + status_arrow.ToString());
    }
    std::shared_ptr<arrow::Array> key_array;
    status_arrow = key_builder.Finish(&key_array);
    if (!status_arrow.ok()) {
        return Status::IOError("Failed to finish key array: " + status_arrow.ToString());
    }

    // Build value column (BinaryArray)
    arrow::BinaryBuilder value_builder;
    for (const auto& val : batch_values_) {
        status_arrow = value_builder.Append(val);
        if (!status_arrow.ok()) {
            return Status::IOError("Failed to append value: " + status_arrow.ToString());
        }
    }
    std::shared_ptr<arrow::Array> value_array;
    status_arrow = value_builder.Finish(&value_array);
    if (!status_arrow.ok()) {
        return Status::IOError("Failed to finish value array: " + status_arrow.ToString());
    }

    // Build timestamp column (Int64Array) for MVCC/ordering
    arrow::Int64Builder ts_builder;
    for (uint64_t ts : batch_timestamps_) {
        status_arrow = ts_builder.Append(static_cast<int64_t>(ts));
        if (!status_arrow.ok()) {
            return Status::IOError("Failed to append timestamp: " + status_arrow.ToString());
        }
    }
    std::shared_ptr<arrow::Array> ts_array;
    status_arrow = ts_builder.Finish(&ts_array);
    if (!status_arrow.ok()) {
        return Status::IOError("Failed to finish timestamp array: " + status_arrow.ToString());
    }

    // Build tombstone column (BooleanArray) for deletion filtering
    arrow::BooleanBuilder tombstone_builder;
    status_arrow = tombstone_builder.AppendValues(batch_tombstones_);
    if (!status_arrow.ok()) {
        return Status::IOError("Failed to append tombstones: " + status_arrow.ToString());
    }
    std::shared_ptr<arrow::Array> tombstone_array;
    status_arrow = tombstone_builder.Finish(&tombstone_array);
    if (!status_arrow.ok()) {
        return Status::IOError("Failed to finish tombstone array: " + status_arrow.ToString());
    }

    // Create RecordBatch with all 4 columns: key, value, _ts, _tombstone
    *batch = arrow::RecordBatch::Make(arrow_schema_, batch_keys_.size(),
                                       {key_array, value_array, ts_array, tombstone_array});

    return Status::OK();
}

Status MmapSSTableWriter::FlushBatchBuffer() {
    if (batch_keys_.empty()) {
        return Status::OK();  // Nothing to flush
    }

    // Create RecordBatch from buffer
    std::shared_ptr<arrow::RecordBatch> batch;
    auto status = CreateRecordBatchFromBuffer(&batch);
    if (!status.ok()) {
        return status;
    }

    // Compute per-column statistics from serialized RecordBatches in column 1
    // Column 0 is LSM storage keys, Column 1 is serialized Arrow RecordBatches with user data
    // This is done once per batch (every 4096 entries) for efficiency
    if (batch->num_columns() >= 2 && batch->num_rows() > 0) {
        auto value_column = batch->column(1);  // BinaryArray with serialized RecordBatches

        if (value_column->type()->id() == ::arrow::Type::BINARY) {
            auto binary_array = std::static_pointer_cast<::arrow::BinaryArray>(value_column);

            // Iterate over each serialized RecordBatch
            for (int64_t i = 0; i < binary_array->length(); ++i) {
                if (binary_array->IsNull(i)) continue;

                // Get the serialized RecordBatch data
                auto view = binary_array->GetView(i);
                auto buffer = std::make_shared<::arrow::Buffer>(
                    reinterpret_cast<const uint8_t*>(view.data()), view.size());

                // Deserialize the RecordBatch using Arrow IPC
                auto buffer_reader = std::make_shared<::arrow::io::BufferReader>(buffer);
                auto stream_reader_result = ::arrow::ipc::RecordBatchStreamReader::Open(buffer_reader);

                if (!stream_reader_result.ok()) {
                    continue;  // Skip this batch if deserialization fails
                }

                auto stream_reader = stream_reader_result.ValueOrDie();
                std::shared_ptr<::arrow::RecordBatch> inner_batch;
                auto read_status = stream_reader->ReadNext(&inner_batch);

                if (!read_status.ok() || !inner_batch) {
                    continue;  // Skip if cannot read batch
                }

                // Capture inner schema from first batch
                if (!inner_schema_ && inner_batch->schema()) {
                    inner_schema_ = inner_batch->schema();
                }

                // Update per-column statistics for zone maps
                UpdateColumnStats(inner_batch);

                // Legacy: Update first column min/max for backwards compatibility
                if (inner_batch->num_columns() > 0 && inner_batch->num_rows() > 0) {
                    auto data_column = inner_batch->column(0);

                    if (data_column->type()->id() == ::arrow::Type::UINT64) {
                        auto uint_array = std::static_pointer_cast<::arrow::UInt64Array>(data_column);

                        uint64_t batch_min = UINT64_MAX;
                        uint64_t batch_max = 0;

                        for (int64_t j = 0; j < uint_array->length(); ++j) {
                            if (!uint_array->IsNull(j)) {
                                uint64_t value = uint_array->Value(j);
                                if (value < batch_min) batch_min = value;
                                if (value > batch_max) batch_max = value;
                            }
                        }

                        if (batch_min != UINT64_MAX) {
                            if (!has_data_range_) {
                                data_min_key_ = batch_min;
                                data_max_key_ = batch_max;
                                has_data_range_ = true;
                            } else {
                                if (batch_min < data_min_key_) data_min_key_ = batch_min;
                                if (batch_max > data_max_key_) data_max_key_ = batch_max;
                            }
                        }
                    }
                }
            }
        }
    }

    // Store batch for later writing (will write in WriteIndex())
    record_batches_.push_back(batch);

    // Clear all buffers
    batch_keys_.clear();
    batch_values_.clear();
    batch_timestamps_.clear();
    batch_tombstones_.clear();

    return Status::OK();
}

Status MmapSSTableWriter::WriteIndex() {
    // Flush any remaining buffered entries
    if (!batch_keys_.empty()) {
        auto status = FlushBatchBuffer();
        if (!status.ok()) {
            return status;
        }
    }

    if (record_batches_.empty()) {
        return Status::OK();  // No data to write
    }

    // Now write RecordBatches to the mapped file using Arrow IPC
    // Create Arrow OutputStream that writes to our mapped region
    // We'll use lseek + write instead of mmap for Arrow IPC (simpler)

    // Close mmap and switch to file writes for Arrow IPC
    if (mapped_region_) {
        if (munmap(mapped_region_, current_file_size_) != 0) {
            return Status::IOError("Failed to unmap before Arrow IPC write");
        }
        mapped_region_ = nullptr;
    }

    // Truncate file to start fresh for Arrow IPC data
    if (ftruncate(fd_, 0) != 0) {
        return Status::IOError("Failed to truncate file for Arrow IPC");
    }

    // Create Arrow file output stream
    auto file_result = arrow::io::FileOutputStream::Open(filepath_);
    if (!file_result.ok()) {
        return Status::IOError("Failed to open Arrow output stream: " + file_result.status().ToString());
    }
    auto arrow_file = file_result.ValueOrDie();

    // Create IPC StreamWriter
    auto writer_result = arrow::ipc::MakeStreamWriter(arrow_file, arrow_schema_);
    if (!writer_result.ok()) {
        return Status::IOError("Failed to create IPC writer: " + writer_result.status().ToString());
    }
    auto ipc_writer = writer_result.ValueOrDie();

    // Write each RecordBatch
    size_t batch_offset = 0;
    for (const auto& batch : record_batches_) {
        // Update sparse index with actual file offsets
        // For now, we use batch index as stored in sparse_index_
        // This is approximate - good enough for sparse lookups

        auto write_status = ipc_writer->WriteRecordBatch(*batch);
        if (!write_status.ok()) {
            return Status::IOError("Failed to write RecordBatch: " + write_status.ToString());
        }
        batch_offset++;
    }

    // Close IPC writer to finalize
    auto close_status = ipc_writer->Close();
    if (!close_status.ok()) {
        return Status::IOError("Failed to close IPC writer: " + close_status.ToString());
    }

    // Get current file position (end of Arrow IPC data)
    auto tell_result = arrow_file->Tell();
    if (!tell_result.ok()) {
        return Status::IOError("Failed to get file position: " + tell_result.status().ToString());
    }
    data_section_end_ = tell_result.ValueOrDie();

    // Close Arrow file (we'll reopen with fd_ for metadata)
    auto file_close_status = arrow_file->Close();
    if (!file_close_status.ok()) {
        return Status::IOError("Failed to close Arrow file: " + file_close_status.ToString());
    }

    return Status::OK();
}

Status MmapSSTableWriter::WriteMetadata() {

    // Reopen file for appending metadata
    // fd_ is still valid, just seek to end
    // Current file layout at this point:
    //   [Arrow IPC data] | [Bloom filter] | [Column stats (zone maps)]
    // We are now at the end of column stats, about to write sparse index + metadata + footer
    off_t current_pos = lseek(fd_, 0, SEEK_END);
    if (current_pos == -1) {
        return Status::IOError("Failed to seek to end of file");
    }

    // Record position after bloom + column stats sections
    // (For backwards compat, this field in footer is called bloom_section_end)
    size_t bloom_section_end = current_pos;

    // Write sparse index
    // Format: [COUNT(8)][KEY1(8)][BATCH_IDX1(8)][KEY2(8)][BATCH_IDX2(8)]...
    uint64_t index_count = sparse_index_.size();
    if (write(fd_, &index_count, sizeof(uint64_t)) != sizeof(uint64_t)) {
        return Status::IOError("Failed to write sparse index count");
    }

    for (const auto& entry : sparse_index_) {
        uint64_t key = entry.first;
        uint64_t batch_idx = entry.second;
        if (write(fd_, &key, sizeof(uint64_t)) != sizeof(uint64_t)) {
            return Status::IOError("Failed to write sparse index key");
        }
        if (write(fd_, &batch_idx, sizeof(uint64_t)) != sizeof(uint64_t)) {
            return Status::IOError("Failed to write sparse index batch index");
        }
    }

    // This position marks the START of metadata section (after sparse index)
    // The reader will seek to this position to read metadata fields
    size_t metadata_start = lseek(fd_, 0, SEEK_CUR);

    // Write metadata
    // Format: [ENTRY_COUNT(8)][MIN_KEY(8)][MAX_KEY(8)][LEVEL(8)]
    //         [DATA_MIN_KEY(8)][DATA_MAX_KEY(8)][HAS_DATA_RANGE(1)]
    if (write(fd_, &entry_count_, sizeof(uint64_t)) != sizeof(uint64_t)) {
        return Status::IOError("Failed to write entry count");
    }
    if (write(fd_, &min_key_, sizeof(uint64_t)) != sizeof(uint64_t)) {
        return Status::IOError("Failed to write min key");
    }
    if (write(fd_, &max_key_, sizeof(uint64_t)) != sizeof(uint64_t)) {
        return Status::IOError("Failed to write max key");
    }
    if (write(fd_, &level_, sizeof(uint64_t)) != sizeof(uint64_t)) {
        return Status::IOError("Failed to write level");
    }

    // Write data column value ranges (for predicate pushdown)
    if (write(fd_, &data_min_key_, sizeof(uint64_t)) != sizeof(uint64_t)) {
        return Status::IOError("Failed to write data min key");
    }
    if (write(fd_, &data_max_key_, sizeof(uint64_t)) != sizeof(uint64_t)) {
        return Status::IOError("Failed to write data max key");
    }
    uint8_t has_data_range_byte = has_data_range_ ? 1 : 0;
    if (write(fd_, &has_data_range_byte, sizeof(uint8_t)) != sizeof(uint8_t)) {
        return Status::IOError("Failed to write has data range flag");
    }

    // Write footer: [DATA_END(8)][BLOOM_END(8)][METADATA_START(8)][MAGIC(8)]
    // Full file layout:
    //   [Arrow IPC data (ends at DATA_END)]
    //   [Bloom filter bytes]
    //   [Column statistics / zone maps (ends at BLOOM_END)]
    //   [Sparse index]
    //   [Metadata fields (starts at METADATA_START)]
    //   [Footer: DATA_END | BLOOM_END | METADATA_START | MAGIC]
    // Note: BLOOM_END now includes column stats section for backwards compat.
    uint64_t magic = 0x4152524F57535354;  // "ARROWSST" in hex
    if (write(fd_, &data_section_end_, sizeof(uint64_t)) != sizeof(uint64_t)) {
        return Status::IOError("Failed to write data section end");
    }
    if (write(fd_, &bloom_section_end, sizeof(uint64_t)) != sizeof(uint64_t)) {
        return Status::IOError("Failed to write bloom section end");
    }
    if (write(fd_, &metadata_start, sizeof(uint64_t)) != sizeof(uint64_t)) {
        return Status::IOError("Failed to write metadata start");
    }
    if (write(fd_, &magic, sizeof(uint64_t)) != sizeof(uint64_t)) {
        return Status::IOError("Failed to write magic number");
    }

    // Sync to disk to ensure durability before returning
    if (fsync(fd_) != 0) {
        return Status::IOError("Failed to sync SSTable to disk");
    }

    return Status::OK();
}

// Bloom filter helper methods (same as SSTableWriterImpl)

int MmapSSTableWriter::ChooseNumProbes(double bits_per_key) const {
    // Optimal: ln(2) * bits_per_key ≈ 0.693 * bits_per_key
    int num_probes = static_cast<int>(0.693 * bits_per_key);

    // Clamp to reasonable range [1, 30]
    if (num_probes < 1) num_probes = 1;
    if (num_probes > 30) num_probes = 30;

    return num_probes;
}

void MmapSSTableWriter::BuildBloomFilterFromHashes() {
    if (key_hashes_.empty()) {
        return;  // No keys, no bloom filter
    }

    // Calculate bloom filter size based on number of unique hashes
    size_t num_keys = key_hashes_.size();
    size_t num_bits = static_cast<size_t>(num_keys * bits_per_key_);

    // Round up to nearest byte boundary
    size_t num_bytes = (num_bits + 7) / 8;
    num_bits = num_bytes * 8;  // Actual bit count (byte-aligned)

    // Allocate bloom filter bytes (zero-initialized)
    bloom_filter_bytes_.resize(num_bytes, 0);

    // Calculate optimal number of hash functions
    int num_probes = ChooseNumProbes(bits_per_key_);

    // Build bloom filter from collected hashes
    for (uint64_t hash : key_hashes_) {
        // Double hashing: use multiple probe positions from single hash
        // NOTE: Cast i to uint64_t to ensure 64-bit multiplication
        for (int i = 0; i < num_probes; ++i) {
            uint64_t h = hash + static_cast<uint64_t>(i) * 0x9e3779b9ULL;  // Golden ratio constant
            size_t bit_index = h % num_bits;
            SetBit(bit_index);
        }
    }

    // Free hash storage (no longer needed)
    key_hashes_.clear();
    key_hashes_.shrink_to_fit();
}

void MmapSSTableWriter::SetBit(size_t bit_index) {
    bloom_filter_bytes_[bit_index >> 3] |= (1 << (bit_index & 7));
}

bool MmapSSTableWriter::CheckBit(size_t bit_index) const {
    return (bloom_filter_bytes_[bit_index >> 3] & (1 << (bit_index & 7))) != 0;
}

Status MmapSSTableWriter::WriteBloomFilter() {
    if (bloom_filter_bytes_.empty()) {
        return Status::OK();  // No bloom filter to write
    }

    // Append bloom filter to file after metadata using raw write() on fd_
    // Format: [NUM_BITS(8)] [NUM_PROBES(4)] [BLOOM_BYTES]

    size_t num_bits = bloom_filter_bytes_.size() * 8;
    int num_probes = ChooseNumProbes(bits_per_key_);

    // Check fd_ is valid
    if (fd_ < 0) {
        return Status::IOError("File descriptor not available for bloom filter write");
    }

    // Seek to end of file (after metadata)
    off_t pos = lseek(fd_, 0, SEEK_END);
    if (pos == -1) {
        return Status::IOError("Failed to seek to end of file for bloom filter");
    }

    // Write num_bits
    if (write(fd_, &num_bits, sizeof(num_bits)) != sizeof(num_bits)) {
        return Status::IOError("Failed to write bloom filter num_bits");
    }

    // Write num_probes
    if (write(fd_, &num_probes, sizeof(num_probes)) != sizeof(num_probes)) {
        return Status::IOError("Failed to write bloom filter num_probes");
    }

    // Write bloom filter bytes
    ssize_t bytes_written = write(fd_, bloom_filter_bytes_.data(), bloom_filter_bytes_.size());
    if (bytes_written != static_cast<ssize_t>(bloom_filter_bytes_.size())) {
        return Status::IOError("Failed to write bloom filter bytes");
    }

    return Status::OK();
}

// Helper to compare two Arrow Scalars of the same type
// Returns negative if a < b, 0 if equal, positive if a > b
static int CompareScalars(const std::shared_ptr<arrow::Scalar>& a,
                          const std::shared_ptr<arrow::Scalar>& b) {
    if (!a || !b || !a->is_valid || !b->is_valid) {
        // Handle nulls: non-null > null
        if (a && a->is_valid && (!b || !b->is_valid)) return 1;
        if (b && b->is_valid && (!a || !a->is_valid)) return -1;
        return 0;
    }

    // Type must match
    if (a->type->id() != b->type->id()) {
        return 0;  // Cannot compare different types
    }

    switch (a->type->id()) {
        case arrow::Type::INT8: {
            auto va = std::static_pointer_cast<arrow::Int8Scalar>(a)->value;
            auto vb = std::static_pointer_cast<arrow::Int8Scalar>(b)->value;
            return (va < vb) ? -1 : (va > vb) ? 1 : 0;
        }
        case arrow::Type::INT16: {
            auto va = std::static_pointer_cast<arrow::Int16Scalar>(a)->value;
            auto vb = std::static_pointer_cast<arrow::Int16Scalar>(b)->value;
            return (va < vb) ? -1 : (va > vb) ? 1 : 0;
        }
        case arrow::Type::INT32: {
            auto va = std::static_pointer_cast<arrow::Int32Scalar>(a)->value;
            auto vb = std::static_pointer_cast<arrow::Int32Scalar>(b)->value;
            return (va < vb) ? -1 : (va > vb) ? 1 : 0;
        }
        case arrow::Type::INT64: {
            auto va = std::static_pointer_cast<arrow::Int64Scalar>(a)->value;
            auto vb = std::static_pointer_cast<arrow::Int64Scalar>(b)->value;
            return (va < vb) ? -1 : (va > vb) ? 1 : 0;
        }
        case arrow::Type::UINT8: {
            auto va = std::static_pointer_cast<arrow::UInt8Scalar>(a)->value;
            auto vb = std::static_pointer_cast<arrow::UInt8Scalar>(b)->value;
            return (va < vb) ? -1 : (va > vb) ? 1 : 0;
        }
        case arrow::Type::UINT16: {
            auto va = std::static_pointer_cast<arrow::UInt16Scalar>(a)->value;
            auto vb = std::static_pointer_cast<arrow::UInt16Scalar>(b)->value;
            return (va < vb) ? -1 : (va > vb) ? 1 : 0;
        }
        case arrow::Type::UINT32: {
            auto va = std::static_pointer_cast<arrow::UInt32Scalar>(a)->value;
            auto vb = std::static_pointer_cast<arrow::UInt32Scalar>(b)->value;
            return (va < vb) ? -1 : (va > vb) ? 1 : 0;
        }
        case arrow::Type::UINT64: {
            auto va = std::static_pointer_cast<arrow::UInt64Scalar>(a)->value;
            auto vb = std::static_pointer_cast<arrow::UInt64Scalar>(b)->value;
            return (va < vb) ? -1 : (va > vb) ? 1 : 0;
        }
        case arrow::Type::FLOAT: {
            auto va = std::static_pointer_cast<arrow::FloatScalar>(a)->value;
            auto vb = std::static_pointer_cast<arrow::FloatScalar>(b)->value;
            return (va < vb) ? -1 : (va > vb) ? 1 : 0;
        }
        case arrow::Type::DOUBLE: {
            auto va = std::static_pointer_cast<arrow::DoubleScalar>(a)->value;
            auto vb = std::static_pointer_cast<arrow::DoubleScalar>(b)->value;
            return (va < vb) ? -1 : (va > vb) ? 1 : 0;
        }
        case arrow::Type::STRING: {
            auto va = std::static_pointer_cast<arrow::StringScalar>(a)->value->ToString();
            auto vb = std::static_pointer_cast<arrow::StringScalar>(b)->value->ToString();
            return va.compare(vb);
        }
        case arrow::Type::BINARY: {
            auto va = std::static_pointer_cast<arrow::BinaryScalar>(a)->value->ToString();
            auto vb = std::static_pointer_cast<arrow::BinaryScalar>(b)->value->ToString();
            return va.compare(vb);
        }
        case arrow::Type::TIMESTAMP: {
            auto va = std::static_pointer_cast<arrow::TimestampScalar>(a)->value;
            auto vb = std::static_pointer_cast<arrow::TimestampScalar>(b)->value;
            return (va < vb) ? -1 : (va > vb) ? 1 : 0;
        }
        case arrow::Type::DATE32: {
            auto va = std::static_pointer_cast<arrow::Date32Scalar>(a)->value;
            auto vb = std::static_pointer_cast<arrow::Date32Scalar>(b)->value;
            return (va < vb) ? -1 : (va > vb) ? 1 : 0;
        }
        case arrow::Type::DATE64: {
            auto va = std::static_pointer_cast<arrow::Date64Scalar>(a)->value;
            auto vb = std::static_pointer_cast<arrow::Date64Scalar>(b)->value;
            return (va < vb) ? -1 : (va > vb) ? 1 : 0;
        }
        default:
            return 0;  // Unknown type - cannot compare
    }
}

void MmapSSTableWriter::UpdateColumnStats(const std::shared_ptr<arrow::RecordBatch>& inner_batch) {
    if (!inner_batch || inner_batch->num_rows() == 0) {
        return;
    }

    // Iterate over all columns in the inner batch
    for (int col_idx = 0; col_idx < inner_batch->num_columns(); ++col_idx) {
        auto column = inner_batch->column(col_idx);
        auto field = inner_batch->schema()->field(col_idx);
        const std::string& col_name = field->name();

        // Compute min/max using Arrow Compute
        auto min_max_result = arrow::compute::MinMax(column);
        if (!min_max_result.ok()) {
            continue;  // Skip if computation fails
        }

        auto min_max_struct = min_max_result.ValueOrDie().scalar_as<arrow::StructScalar>();
        if (!min_max_struct.is_valid || min_max_struct.value.size() < 2) {
            continue;
        }

        auto batch_min = min_max_struct.value[0];
        auto batch_max = min_max_struct.value[1];

        // Null count for this batch
        int64_t null_count = column->null_count();
        int64_t row_count = column->length();

        // Merge with existing stats
        MergeColumnStats(col_name, batch_min, batch_max, null_count, row_count);
    }
}

void MmapSSTableWriter::MergeColumnStats(const std::string& column_name,
                                          const std::shared_ptr<arrow::Scalar>& min_val,
                                          const std::shared_ptr<arrow::Scalar>& max_val,
                                          int64_t null_count, int64_t row_count) {
    auto it = column_stats_map_.find(column_name);

    if (it == column_stats_map_.end()) {
        // Create new stats entry
        auto stats = std::make_shared<ColumnStatistics>(column_name);
        stats->min_value = min_val;
        stats->max_value = max_val;
        stats->null_count = null_count;
        stats->has_nulls = (null_count > 0);
        column_stats_map_[column_name] = stats;
    } else {
        // Merge with existing stats
        auto& stats = it->second;

        // Update min value
        if (min_val && min_val->is_valid) {
            if (!stats->min_value || !stats->min_value->is_valid ||
                CompareScalars(min_val, stats->min_value) < 0) {
                stats->min_value = min_val;
            }
        }

        // Update max value
        if (max_val && max_val->is_valid) {
            if (!stats->max_value || !stats->max_value->is_valid ||
                CompareScalars(max_val, stats->max_value) > 0) {
                stats->max_value = max_val;
            }
        }

        // Accumulate null count
        stats->null_count += null_count;
        stats->has_nulls = (stats->null_count > 0);
    }
}

Status MmapSSTableWriter::WriteColumnStats() {
    // Write column statistics section to file
    // Format:
    //   [NUM_COLUMNS (4 bytes)]
    //   For each column:
    //     [NAME_LEN (4 bytes)][NAME_BYTES]
    //     [HAS_NULLS (1 byte)][NULL_COUNT (8 bytes)]
    //     [HAS_MIN (1 byte)][MIN_TYPE (4 bytes)][MIN_DATA_LEN (4 bytes)][MIN_DATA]
    //     [HAS_MAX (1 byte)][MAX_TYPE (4 bytes)][MAX_DATA_LEN (4 bytes)][MAX_DATA]

    if (column_stats_map_.empty()) {
        // Write zero columns indicator
        uint32_t num_columns = 0;
        if (write(fd_, &num_columns, sizeof(uint32_t)) != sizeof(uint32_t)) {
            return Status::IOError("Failed to write column stats count");
        }
        return Status::OK();
    }

    // Write number of columns
    uint32_t num_columns = static_cast<uint32_t>(column_stats_map_.size());
    if (write(fd_, &num_columns, sizeof(uint32_t)) != sizeof(uint32_t)) {
        return Status::IOError("Failed to write column stats count");
    }

    // Write each column's statistics
    for (const auto& [col_name, stats] : column_stats_map_) {
        // Write column name
        uint32_t name_len = static_cast<uint32_t>(col_name.size());
        if (write(fd_, &name_len, sizeof(uint32_t)) != sizeof(uint32_t)) {
            return Status::IOError("Failed to write column name length");
        }
        if (write(fd_, col_name.data(), name_len) != static_cast<ssize_t>(name_len)) {
            return Status::IOError("Failed to write column name");
        }

        // Write has_nulls flag and null_count
        uint8_t has_nulls = stats->has_nulls ? 1 : 0;
        if (write(fd_, &has_nulls, sizeof(uint8_t)) != sizeof(uint8_t)) {
            return Status::IOError("Failed to write has_nulls flag");
        }
        if (write(fd_, &stats->null_count, sizeof(int64_t)) != sizeof(int64_t)) {
            return Status::IOError("Failed to write null_count");
        }

        // Serialize min value using Arrow IPC
        auto write_scalar = [this](const std::shared_ptr<arrow::Scalar>& scalar) -> Status {
            uint8_t has_value = (scalar && scalar->is_valid) ? 1 : 0;
            if (write(fd_, &has_value, sizeof(uint8_t)) != sizeof(uint8_t)) {
                return Status::IOError("Failed to write has_value flag");
            }

            if (has_value) {
                // Write type id
                int32_t type_id = static_cast<int32_t>(scalar->type->id());
                if (write(fd_, &type_id, sizeof(int32_t)) != sizeof(int32_t)) {
                    return Status::IOError("Failed to write scalar type");
                }

                // Serialize scalar value to string using Arrow's ToString
                std::string scalar_str = scalar->ToString();
                uint32_t data_len = static_cast<uint32_t>(scalar_str.size());
                if (write(fd_, &data_len, sizeof(uint32_t)) != sizeof(uint32_t)) {
                    return Status::IOError("Failed to write scalar data length");
                }
                if (data_len > 0) {
                    if (write(fd_, scalar_str.data(), data_len) != static_cast<ssize_t>(data_len)) {
                        return Status::IOError("Failed to write scalar data");
                    }
                }
            }
            return Status::OK();
        };

        // Write min value
        auto status = write_scalar(stats->min_value);
        if (!status.ok()) return status;

        // Write max value
        status = write_scalar(stats->max_value);
        if (!status.ok()) return status;
    }

    return Status::OK();
}

std::unique_ptr<SSTableWriter> CreateMmapSSTableWriter(
    const std::string& filepath,
    uint64_t level,
    std::shared_ptr<FileSystem> fs,
    SSTableManager* sstable_mgr,
    size_t zone_size,
    bool use_async_msync) {

    return std::make_unique<MmapSSTableWriter>(
        filepath, level, fs, sstable_mgr, zone_size, use_async_msync);
}

} // namespace marble
