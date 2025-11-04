#include "marble/sstable.h"
#include "marble/arrow_sstable_reader.h"
#include "marble/file_system.h"
#include "marble/lsm_tree.h"
#include "marble/analytics.h"  // For BloomFilter
#include <iostream>
#include <sstream>
#include <iomanip>
#include <cstring>
#include <algorithm>
#include <arrow/api.h>
#include <arrow/util/compression.h>
#include <arrow/io/api.h>
#include <nlohmann/json.hpp>

namespace marble {

// SSTableMetadata implementation - using efficient binary serialization
Status SSTableMetadata::SerializeToString(std::string* output) const {
    // Calculate required size
    size_t filename_size = filename.size();
    size_t bloom_filter_size = bloom_filter.size();
    size_t total_size = sizeof(uint32_t) + filename_size + // filename
                       sizeof(uint64_t) * 6 +             // numeric fields
                       sizeof(uint32_t) + bloom_filter_size; // bloom filter

    output->resize(total_size);
    char* ptr = &(*output)[0];

    // Write filename
    *reinterpret_cast<uint32_t*>(ptr) = filename_size;
    ptr += sizeof(uint32_t);
    memcpy(ptr, filename.data(), filename_size);
    ptr += filename_size;

    // Write numeric fields
    *reinterpret_cast<uint64_t*>(ptr) = file_size; ptr += sizeof(uint64_t);
    *reinterpret_cast<uint64_t*>(ptr) = min_key; ptr += sizeof(uint64_t);
    *reinterpret_cast<uint64_t*>(ptr) = max_key; ptr += sizeof(uint64_t);
    *reinterpret_cast<uint64_t*>(ptr) = record_count; ptr += sizeof(uint64_t);
    *reinterpret_cast<uint64_t*>(ptr) = created_timestamp; ptr += sizeof(uint64_t);
    *reinterpret_cast<uint64_t*>(ptr) = level; ptr += sizeof(uint64_t);

    // Write bloom filter
    *reinterpret_cast<uint32_t*>(ptr) = bloom_filter_size;
    ptr += sizeof(uint32_t);
    memcpy(ptr, bloom_filter.data(), bloom_filter_size);

    return Status::OK();
}

Status SSTableMetadata::DeserializeFromString(const std::string& input, SSTableMetadata* metadata) {
    if (input.size() < sizeof(uint32_t)) {
        return Status::InvalidArgument("Metadata too small");
    }

    const char* ptr = input.data();

    // Read filename
    uint32_t filename_size = *reinterpret_cast<const uint32_t*>(ptr);
    ptr += sizeof(uint32_t);
    if (ptr + filename_size > input.data() + input.size()) {
        return Status::InvalidArgument("Invalid filename size");
    }
    metadata->filename.assign(ptr, filename_size);
    ptr += filename_size;

    // Read numeric fields
    if (ptr + sizeof(uint64_t) * 6 > input.data() + input.size()) {
        return Status::InvalidArgument("Metadata truncated");
    }
    metadata->file_size = *reinterpret_cast<const uint64_t*>(ptr); ptr += sizeof(uint64_t);
    metadata->min_key = *reinterpret_cast<const uint64_t*>(ptr); ptr += sizeof(uint64_t);
    metadata->max_key = *reinterpret_cast<const uint64_t*>(ptr); ptr += sizeof(uint64_t);
    metadata->record_count = *reinterpret_cast<const uint64_t*>(ptr); ptr += sizeof(uint64_t);
    metadata->created_timestamp = *reinterpret_cast<const uint64_t*>(ptr); ptr += sizeof(uint64_t);
    metadata->level = *reinterpret_cast<const uint64_t*>(ptr); ptr += sizeof(uint64_t);

    // Read bloom filter
    if (ptr + sizeof(uint32_t) > input.data() + input.size()) {
        return Status::InvalidArgument("Metadata truncated");
    }
    uint32_t bloom_filter_size = *reinterpret_cast<const uint32_t*>(ptr);
    ptr += sizeof(uint32_t);
    if (ptr + bloom_filter_size != input.data() + input.size()) {
        return Status::InvalidArgument("Invalid bloom filter size");
    }
    metadata->bloom_filter.assign(ptr, bloom_filter_size);

    return Status::OK();
}

// SSTableWriter implementation
class SSTableWriterImpl : public SSTableWriter {
public:
    SSTableWriterImpl(const std::string& filepath, uint64_t level,
                     std::shared_ptr<FileSystem> fs);
    ~SSTableWriterImpl() override;

    Status Add(uint64_t key, const std::string& value) override;
    Status Finish(std::unique_ptr<SSTable>* sstable) override;
    size_t GetEntryCount() const override;
    size_t GetEstimatedSize() const override;

private:
    std::string filepath_;
    uint64_t level_;
    std::shared_ptr<FileSystem> fs_;

    // In-memory data structures
    std::vector<std::pair<uint64_t, std::string>> entries_;
    std::vector<SSTableIndexEntry> index_entries_;
    size_t estimated_size_;

    // Bloom filter for keys
    std::vector<bool> bloom_filter_bits_;
    size_t bloom_filter_size_;
    size_t hash_functions_;

    // File handle for writing
    std::unique_ptr<FileHandle> file_handle_;

    Status WriteHeader();
    Status WriteData();
    Status WriteIndex();
    Status WriteBloomFilter();
    Status WriteMetadata();
};

// SSTableReader implementation
class SSTableReaderImpl : public SSTableReader {
public:
    SSTableReaderImpl(std::shared_ptr<FileSystem> fs);
    ~SSTableReaderImpl() override;

    Status Open(const std::string& filepath,
               std::unique_ptr<SSTable>* sstable) override;
    Status CreateFromFile(const std::string& filepath,
                         const SSTableMetadata& metadata,
                         std::unique_ptr<SSTable>* sstable) override;

private:
    std::shared_ptr<FileSystem> fs_;
};

// SSTable implementation
class SSTableImpl : public SSTable {
public:
    SSTableImpl(const std::string& filepath, const SSTableMetadata& metadata,
               std::shared_ptr<FileSystem> fs);
    ~SSTableImpl() override;

    const SSTableMetadata& GetMetadata() const override;
    bool ContainsKey(uint64_t key) const override;
    Status Get(uint64_t key, std::string* value) const override;
    Status MultiGet(const std::vector<uint64_t>& keys,
                   std::vector<std::string>* values) const override;
    Status Scan(uint64_t start_key, uint64_t end_key,
               std::vector<std::pair<uint64_t, std::string>>* results) const override;
    Status GetAllKeys(std::vector<uint64_t>* keys) const override;
    std::string GetFilePath() const override;
    uint64_t GetFileSize() const override;
    Status Validate() const override;

private:
    std::string filepath_;
    SSTableMetadata metadata_;
    std::shared_ptr<FileSystem> fs_;

    // Cached data
    mutable std::vector<SSTableIndexEntry> index_cache_;
    mutable std::unique_ptr<BloomFilter> bloom_filter_cache_;
    mutable bool index_loaded_;
    mutable bool bloom_filter_loaded_;

    Status LoadIndex() const;
    Status LoadBloomFilter() const;
    Status BinarySearch(uint64_t key, size_t* index_pos) const;
    Status ReadValueAtOffset(uint64_t offset, uint32_t size, std::string* value) const;
};

// SSTableManager implementation
class SSTableManagerImpl : public SSTableManager {
public:
    SSTableManagerImpl(std::shared_ptr<FileSystem> fs);
    ~SSTableManagerImpl() override;

    Status CreateWriter(const std::string& filepath,
                       uint64_t level,
                       std::unique_ptr<SSTableWriter>* writer) override;
    Status OpenSSTable(const std::string& filepath,
                      std::unique_ptr<SSTable>* sstable) override;
    Status ListSSTables(const std::string& directory,
                       std::vector<std::string>* files) override;
    Status DeleteSSTable(const std::string& filepath) override;
    Status RepairSSTable(const std::string& filepath) override;

private:
    std::shared_ptr<FileSystem> fs_;
};

// SSTableWriterImpl implementation
SSTableWriterImpl::SSTableWriterImpl(const std::string& filepath, uint64_t level,
                                   std::shared_ptr<FileSystem> fs)
    : filepath_(filepath), level_(level), fs_(fs), estimated_size_(0),
      bloom_filter_size_(1024 * 8), // 1KB bloom filter (8192 bits)
      hash_functions_(3) { // 3 hash functions
    // Initialize bloom filter bits
    bloom_filter_bits_.resize(bloom_filter_size_, false);
}

SSTableWriterImpl::~SSTableWriterImpl() = default;

Status SSTableWriterImpl::Add(uint64_t key, const std::string& value) {
    // Add to entries (maintaining sorted order)
    auto entry = std::make_pair(key, value);
    entries_.push_back(entry);

    // Update bloom filter using multiple hash functions
    for (size_t i = 0; i < hash_functions_; ++i) {
        // Simple hash function combination
        size_t hash = std::hash<uint64_t>{}(key + i * 0x9e3779b9);
        size_t bit_index = hash % bloom_filter_size_;
        bloom_filter_bits_[bit_index] = true;
    }

    // Update estimated size
    estimated_size_ += sizeof(uint64_t) + value.size() + sizeof(uint32_t);

    return Status::OK();
}

Status SSTableWriterImpl::Finish(std::unique_ptr<SSTable>* sstable) {
    if (entries_.empty()) {
        return Status::InvalidArgument("Cannot create SSTable with no entries");
    }

    // Sort entries by key
    std::sort(entries_.begin(), entries_.end());

    // Create file handle
    auto status = fs_->OpenFile(filepath_,
                               static_cast<FileOpenFlags>(
                                   static_cast<int>(FileOpenFlags::kWrite) |
                                   static_cast<int>(FileOpenFlags::kCreate) |
                                   static_cast<int>(FileOpenFlags::kTruncate)),
                               &file_handle_);
    if (!status.ok()) {
        return status;
    }

    // Write SSTable components
    status = WriteHeader();
    if (!status.ok()) return status;

    status = WriteData();
    if (!status.ok()) return status;

    status = WriteIndex();
    if (!status.ok()) return status;

    status = WriteBloomFilter();
    if (!status.ok()) return status;

    status = WriteMetadata();
    if (!status.ok()) return status;

    // Create SSTable instance
    SSTableMetadata metadata;
    metadata.filename = filepath_;
    metadata.file_size = estimated_size_;
    metadata.min_key = entries_.front().first;
    metadata.max_key = entries_.back().first;
    metadata.record_count = entries_.size();
    metadata.created_timestamp = std::chrono::duration_cast<std::chrono::microseconds>(
        std::chrono::system_clock::now().time_since_epoch()).count();
    metadata.level = level_;

    // Serialize bloom filter
    // FIXME: Proper bloom filter serialization needed
    metadata.bloom_filter = "";

    auto sstable_impl = std::make_unique<SSTableImpl>(filepath_, metadata, fs_);
    *sstable = std::move(sstable_impl);

    return Status::OK();
}

size_t SSTableWriterImpl::GetEntryCount() const {
    return entries_.size();
}

size_t SSTableWriterImpl::GetEstimatedSize() const {
    return estimated_size_;
}

Status SSTableWriterImpl::WriteHeader() {
    // SSTable format:
    // [MAGIC(8)] [VERSION(4)] [INDEX_OFFSET(8)] [BLOOM_OFFSET(8)] [METADATA_OFFSET(8)]

    const uint64_t magic = 0x53535441424C4500; // "SSTABLE\0"
    const uint32_t version = 1;

    std::string header(32, '\0');
    memcpy(&header[0], &magic, 8);
    memcpy(&header[8], &version, 4);

    // Placeholder offsets (will be updated at end)
    uint64_t placeholder = 0;
    memcpy(&header[12], &placeholder, 8); // index_offset
    memcpy(&header[20], &placeholder, 8); // bloom_offset
    memcpy(&header[28], &placeholder, 8); // metadata_offset

    auto seek_status = file_handle_->Seek(0);
    if (!seek_status.ok()) return seek_status;
    return file_handle_->Write(header.data(), header.size(), nullptr);
}

Status SSTableWriterImpl::WriteData() {
    // Write key-value pairs
    // Format: [KEY(8)] [VALUE_SIZE(4)] [VALUE(N)]

    uint64_t offset = 32; // After header
    index_entries_.reserve(entries_.size());

    for (const auto& entry : entries_) {
        SSTableIndexEntry index_entry;
        index_entry.key = entry.first;
        index_entry.offset = offset;
        index_entry.size = entry.second.size();

        // Write key
        auto status = file_handle_->Write(&entry.first, sizeof(uint64_t), nullptr);
        if (!status.ok()) return status;
        offset += sizeof(uint64_t);

        // Write value size
        uint32_t value_size = entry.second.size();
        status = file_handle_->Write(&value_size, sizeof(uint32_t), nullptr);
        if (!status.ok()) return status;
        offset += sizeof(uint32_t);

        // Write value
        status = file_handle_->Write(entry.second.data(), value_size, nullptr);
        if (!status.ok()) return status;
        offset += value_size;

        index_entries_.push_back(index_entry);
    }

    return Status::OK();
}

Status SSTableWriterImpl::WriteIndex() {
    // Write index entries at the end of data
    // Update header with index offset

    size_t index_offset;
    auto size_status = file_handle_->GetSize(&index_offset);
    if (!size_status.ok()) return size_status;
    uint64_t index_size = index_entries_.size() * sizeof(SSTableIndexEntry);

    auto seek_status = file_handle_->Seek(index_offset);
    if (!seek_status.ok()) return seek_status;
    auto status = file_handle_->Write(index_entries_.data(), index_size, nullptr);
    if (!status.ok()) return status;

    // Update header
    seek_status = file_handle_->Seek(12);
    if (!seek_status.ok()) return seek_status;
    status = file_handle_->Write(&index_offset, sizeof(uint64_t), nullptr);
    if (!status.ok()) return status;

    return Status::OK();
}

Status SSTableWriterImpl::WriteBloomFilter() {
    // Serialize bloom filter bits
    size_t bloom_offset;
    auto size_status = file_handle_->GetSize(&bloom_offset);
    if (!size_status.ok()) return size_status;

    // Convert bool vector to bytes for storage
    size_t bytes_needed = (bloom_filter_size_ + 7) / 8; // Round up to bytes
    std::vector<uint8_t> bloom_bytes(bytes_needed, 0);

    for (size_t i = 0; i < bloom_filter_size_; ++i) {
        if (bloom_filter_bits_[i]) {
            size_t byte_index = i / 8;
            size_t bit_index = i % 8;
            bloom_bytes[byte_index] |= (1 << bit_index);
        }
    }

    // Write bloom filter size and data
    auto seek_status = file_handle_->Seek(bloom_offset);
    if (!seek_status.ok()) return seek_status;
    auto status = file_handle_->Write(reinterpret_cast<const char*>(&bloom_filter_size_), sizeof(size_t), nullptr);
    if (!status.ok()) return status;
    bloom_offset += sizeof(size_t);

    seek_status = file_handle_->Seek(bloom_offset);
    if (!seek_status.ok()) return seek_status;
    status = file_handle_->Write(reinterpret_cast<const char*>(&hash_functions_), sizeof(size_t), nullptr);
    if (!status.ok()) return status;
    bloom_offset += sizeof(size_t);

    seek_status = file_handle_->Seek(bloom_offset);
    if (!seek_status.ok()) return seek_status;
    status = file_handle_->Write(reinterpret_cast<const char*>(bloom_bytes.data()), bloom_bytes.size(), nullptr);
    if (!status.ok()) return status;

    // Update header with bloom filter offset
    size_t current_size;
    size_status = file_handle_->GetSize(&current_size);
    if (!size_status.ok()) return size_status;
    uint64_t final_bloom_offset = current_size - bloom_bytes.size() - 2 * sizeof(size_t);
    seek_status = file_handle_->Seek(20);
    if (!seek_status.ok()) return seek_status;
    status = file_handle_->Write(&final_bloom_offset, sizeof(uint64_t), nullptr);
    if (!status.ok()) return status;

    return Status::OK();
}

Status SSTableWriterImpl::WriteMetadata() {
    // Write metadata at the end
    SSTableMetadata metadata;
    metadata.filename = filepath_;
    size_t file_size;
    auto size_status = file_handle_->GetSize(&file_size);
    if (!size_status.ok()) return size_status;
    metadata.file_size = file_size;
    metadata.min_key = entries_.front().first;
    metadata.max_key = entries_.back().first;
    metadata.record_count = entries_.size();
    metadata.created_timestamp = std::chrono::duration_cast<std::chrono::microseconds>(
        std::chrono::system_clock::now().time_since_epoch()).count();
    metadata.level = level_;

    // Serialize bloom filter to metadata
    size_t bytes_needed = (bloom_filter_size_ + 7) / 8;
    metadata.bloom_filter.resize(bytes_needed);
    for (size_t i = 0; i < bloom_filter_size_; ++i) {
        if (bloom_filter_bits_[i]) {
            size_t byte_index = i / 8;
            size_t bit_index = i % 8;
            metadata.bloom_filter[byte_index] |= (1 << bit_index);
        }
    }

    std::string metadata_str;
    auto status = metadata.SerializeToString(&metadata_str);
    if (!status.ok()) return status;

    size_t metadata_offset;
    size_status = file_handle_->GetSize(&metadata_offset);
    if (!size_status.ok()) return size_status;
    auto seek_status = file_handle_->Seek(metadata_offset);
    if (!seek_status.ok()) return seek_status;
    status = file_handle_->Write(metadata_str.data(), metadata_str.size(), nullptr);
    if (!status.ok()) return status;

    // Update header
    seek_status = file_handle_->Seek(28);
    if (!seek_status.ok()) return seek_status;
    status = file_handle_->Write(&metadata_offset, sizeof(uint64_t), nullptr);
    if (!status.ok()) return status;

    return Status::OK();
}

// SSTableReaderImpl implementation
SSTableReaderImpl::SSTableReaderImpl(std::shared_ptr<FileSystem> fs)
    : fs_(fs) {}

SSTableReaderImpl::~SSTableReaderImpl() = default;

Status SSTableReaderImpl::Open(const std::string& filepath,
                              std::unique_ptr<SSTable>* sstable) {
    std::cerr << "SSTableReaderImpl::Open() START - filepath=" << filepath << "\n";
    std::cerr << std::flush;

    // First, try to detect Arrow format by checking footer magic
    // Read last 8 bytes to check for "ARROWSST" magic
    std::unique_ptr<FileHandle> file_handle;

    std::cerr << "SSTableReaderImpl::Open() - Opening file...\n";
    std::cerr << std::flush;

    auto status = fs_->OpenFile(filepath, FileOpenFlags::kRead, &file_handle);
    if (!status.ok()) {
        std::cerr << "SSTableReaderImpl::Open() - Failed to open file\n";
        return status;
    }
    std::cerr << "SSTableReaderImpl::Open() - File opened successfully\n";

    size_t file_size;
    std::cerr << "SSTableReaderImpl::Open() - Getting file size...\n";
    std::cerr << std::flush;

    auto size_status = file_handle->GetSize(&file_size);

    std::cerr << "SSTableReaderImpl::Open() - GetSize() returned\n";
    if (!size_status.ok()) {
        std::cerr << "SSTableReaderImpl::Open() - GetSize() failed\n";
        return size_status;
    }
    std::cerr << "SSTableReaderImpl::Open() - file_size=" << file_size << " bytes\n";

    if (file_size >= 24) {  // Need at least 24 bytes for Arrow footer
        uint64_t magic;
        std::cerr << "SSTableReaderImpl::Open() - Seeking to footer...\n";
        std::cerr << std::flush;

        auto seek_status = file_handle->Seek(file_size - 8);

        std::cerr << "SSTableReaderImpl::Open() - Seek() returned\n";
        if (seek_status.ok()) {
            std::cerr << "SSTableReaderImpl::Open() - Reading magic number...\n";
            std::cerr << std::flush;

            status = file_handle->Read(&magic, sizeof(uint64_t), nullptr);

            std::cerr << "SSTableReaderImpl::Open() - Read() returned\n";
            std::cerr << "SSTableReaderImpl::Open() - magic=0x" << std::hex << magic << std::dec << "\n";

            if (status.ok() && magic == 0x4152524F57535354) {  // "ARROWSST"
                // Arrow format detected - use ArrowSSTableReader
                std::cerr << "SSTableReaderImpl: Detected Arrow format for " << filepath << "\n";
                auto arrow_sstable = OpenArrowSSTable(filepath, fs_);
                if (arrow_sstable) {
                    *sstable = std::move(arrow_sstable);
                    return Status::OK();
                }
                return Status::IOError("Failed to open Arrow SSTable");
            } else {
                std::cerr << "SSTableReaderImpl::Open() - Not Arrow format (magic mismatch)\n";
            }
        } else {
            std::cerr << "SSTableReaderImpl::Open() - Seek() failed\n";
        }
    } else {
        std::cerr << "SSTableReaderImpl::Open() - File too small for Arrow format\n";
    }

    // Not Arrow format - try old format
    std::cerr << "SSTableReaderImpl: Using old format reader for " << filepath << "\n";

    // Reset file handle for old format reading
    status = fs_->OpenFile(filepath, FileOpenFlags::kRead, &file_handle);
    if (!status.ok()) return status;

    // Seek to metadata offset in header (28 bytes into header)
    auto seek_status = file_handle->Seek(28);
    if (!seek_status.ok()) return seek_status;

    // Read metadata offset from header
    uint64_t metadata_offset;
    status = file_handle->Read(&metadata_offset, sizeof(uint64_t), nullptr);
    if (!status.ok()) return status;

    // Read metadata - first read a reasonable chunk to find the end
    // For now, assume metadata is less than 1MB
    std::string metadata_buffer;
    metadata_buffer.resize(1024 * 1024); // 1MB buffer
    uint64_t bytes_to_read = file_size - metadata_offset;
    if (bytes_to_read > metadata_buffer.size()) {
        return Status::InvalidArgument("Metadata too large");
    }

    seek_status = file_handle->Seek(metadata_offset);
    if (!seek_status.ok()) return seek_status;
    status = file_handle->Read(&metadata_buffer[0], bytes_to_read, nullptr);
    if (!status.ok()) return status;

    metadata_buffer.resize(bytes_to_read);

    SSTableMetadata metadata;
    status = SSTableMetadata::DeserializeFromString(metadata_buffer, &metadata);
    if (!status.ok()) return status;

    return CreateFromFile(filepath, metadata, sstable);
}

Status SSTableReaderImpl::CreateFromFile(const std::string& filepath,
                                        const SSTableMetadata& metadata,
                                        std::unique_ptr<SSTable>* sstable) {
    auto sstable_impl = std::make_unique<SSTableImpl>(filepath, metadata, fs_);
    *sstable = std::move(sstable_impl);
    return Status::OK();
}

// SSTableImpl implementation
SSTableImpl::SSTableImpl(const std::string& filepath, const SSTableMetadata& metadata,
                        std::shared_ptr<FileSystem> fs)
    : filepath_(filepath), metadata_(metadata), fs_(fs),
      index_loaded_(false), bloom_filter_loaded_(false) {}

SSTableImpl::~SSTableImpl() = default;

const SSTableMetadata& SSTableImpl::GetMetadata() const {
    return metadata_;
}

bool SSTableImpl::ContainsKey(uint64_t key) const {
    // Check bloom filter first (if available)
    if (metadata_.bloom_filter.empty()) {
        return true; // Conservative: assume it might exist
    }

    // FIXME: Implement bloom filter checking
    return true;
}

Status SSTableImpl::Get(uint64_t key, std::string* value) const {
    if (!ContainsKey(key)) {
        return Status::NotFound("Key not found");
    }

    // Load index if needed
    auto status = LoadIndex();
    if (!status.ok()) return status;

    // Binary search in index
    size_t index_pos;
    status = BinarySearch(key, &index_pos);
    if (!status.ok()) return status;

    const auto& index_entry = index_cache_[index_pos];
    return ReadValueAtOffset(index_entry.offset, index_entry.size, value);
}

Status SSTableImpl::MultiGet(const std::vector<uint64_t>& keys,
                             std::vector<std::string>* values) const {
    values->clear();
    values->reserve(keys.size());

    for (uint64_t key : keys) {
        std::string value;
        auto status = Get(key, &value);
        if (!status.ok()) {
            values->push_back(""); // Placeholder for missing keys
        } else {
            values->push_back(value);
        }
    }

    return Status::OK();
}

Status SSTableImpl::Scan(uint64_t start_key, uint64_t end_key,
                        std::vector<std::pair<uint64_t, std::string>>* results) const {
    // Load index if needed
    auto status = LoadIndex();
    if (!status.ok()) return status;

    // Find range in index
    auto it = std::lower_bound(index_cache_.begin(), index_cache_.end(), start_key,
                              [](const SSTableIndexEntry& entry, uint64_t key) {
                                  return entry.key < key;
                              });

    for (; it != index_cache_.end() && it->key <= end_key; ++it) {
        std::string value;
        status = ReadValueAtOffset(it->offset, it->size, &value);
        if (!status.ok()) continue;

        results->emplace_back(it->key, value);
    }

    return Status::OK();
}

Status SSTableImpl::GetAllKeys(std::vector<uint64_t>* keys) const {
    auto status = LoadIndex();
    if (!status.ok()) return status;

    keys->reserve(index_cache_.size());
    for (const auto& entry : index_cache_) {
        keys->push_back(entry.key);
    }

    return Status::OK();
}

std::string SSTableImpl::GetFilePath() const {
    return filepath_;
}

uint64_t SSTableImpl::GetFileSize() const {
    return metadata_.file_size;
}

Status SSTableImpl::Validate() const {
    // Basic validation
    if (metadata_.record_count == 0) {
        return Status::InvalidArgument("SSTable has no records");
    }

    if (metadata_.min_key > metadata_.max_key) {
        return Status::InvalidArgument("SSTable key range is invalid");
    }

    // FIXME: More comprehensive validation
    return Status::OK();
}

Status SSTableImpl::LoadIndex() const {
    if (index_loaded_) return Status::OK();

    std::unique_ptr<FileHandle> file_handle;
    auto status = fs_->OpenFile(filepath_, FileOpenFlags::kRead, &file_handle);
    if (!status.ok()) return status;

    // Read index offset from header
    auto seek_status = file_handle->Seek(12);
    if (!seek_status.ok()) return seek_status;
    uint64_t index_offset;
    status = file_handle->Read(&index_offset, sizeof(uint64_t), nullptr);
    if (!status.ok()) return status;

    // Read index entries
    uint64_t index_size = metadata_.record_count * sizeof(SSTableIndexEntry);
    index_cache_.resize(metadata_.record_count);

    seek_status = file_handle->Seek(index_offset);
    if (!seek_status.ok()) return seek_status;
    status = file_handle->Read(index_cache_.data(), index_size, nullptr);
    if (!status.ok()) return status;

    index_loaded_ = true;
    return Status::OK();
}

Status SSTableImpl::LoadBloomFilter() const {
    if (bloom_filter_loaded_ || metadata_.bloom_filter.empty()) return Status::OK();

    // FIXME: Implement bloom filter deserialization
    bloom_filter_loaded_ = true;
    return Status::OK();
}

Status SSTableImpl::BinarySearch(uint64_t key, size_t* index_pos) const {
    auto it = std::lower_bound(index_cache_.begin(), index_cache_.end(), key,
                              [](const SSTableIndexEntry& entry, uint64_t key) {
                                  return entry.key < key;
                              });

    if (it == index_cache_.end() || it->key != key) {
        return Status::NotFound("Key not found in index");
    }

    *index_pos = std::distance(index_cache_.begin(), it);
    return Status::OK();
}

Status SSTableImpl::ReadValueAtOffset(uint64_t offset, uint32_t size, std::string* value) const {
    std::unique_ptr<FileHandle> file_handle;
    auto status = fs_->OpenFile(filepath_, FileOpenFlags::kRead, &file_handle);
    if (!status.ok()) return status;

    // Skip key and read value size
    uint64_t value_offset = offset + sizeof(uint64_t);
    auto seek_status = file_handle->Seek(value_offset);
    if (!seek_status.ok()) return seek_status;
    uint32_t stored_size;
    status = file_handle->Read(&stored_size, sizeof(uint32_t), nullptr);
    if (!status.ok()) return status;

    if (stored_size != size) {
        return Status::Corruption("Value size mismatch");
    }

    // Read value
    uint64_t data_offset = value_offset + sizeof(uint32_t);
    seek_status = file_handle->Seek(data_offset);
    if (!seek_status.ok()) return seek_status;
    value->resize(size);
    status = file_handle->Read(&(*value)[0], size, nullptr);
    if (!status.ok()) return status;

    return Status::OK();
}

// SSTableManagerImpl implementation
SSTableManagerImpl::SSTableManagerImpl(std::shared_ptr<FileSystem> fs)
    : fs_(fs) {}

SSTableManagerImpl::~SSTableManagerImpl() = default;

Status SSTableManagerImpl::CreateWriter(const std::string& filepath,
                                       uint64_t level,
                                       std::unique_ptr<SSTableWriter>* writer) {
    auto writer_impl = std::make_unique<SSTableWriterImpl>(filepath, level, fs_);
    *writer = std::move(writer_impl);
    return Status::OK();
}

Status SSTableManagerImpl::OpenSSTable(const std::string& filepath,
                                      std::unique_ptr<SSTable>* sstable) {
    SSTableReaderImpl reader(fs_);
    return reader.Open(filepath, sstable);
}

Status SSTableManagerImpl::ListSSTables(const std::string& directory,
                                       std::vector<std::string>* files) {
    std::vector<FileInfo> file_infos;
    auto status = fs_->ListFiles(directory, &file_infos);
    if (!status.ok()) return status;
    
    files->clear();
    for (const auto& info : file_infos) {
        files->push_back(info.path);
    }
    return Status::OK();
}

Status SSTableManagerImpl::DeleteSSTable(const std::string& filepath) {
    return fs_->RemoveFile(filepath);
}

Status SSTableManagerImpl::RepairSSTable(const std::string& filepath) {
    // FIXME: Implement SSTable repair functionality
    return Status::NotImplemented("SSTable repair not implemented");
}

// Factory functions
std::unique_ptr<SSTableManager> CreateSSTableManager(
    std::shared_ptr<FileSystem> fs) {
    if (!fs) {
        fs = FileSystem::CreateLocal();
    }
    return std::make_unique<SSTableManagerImpl>(fs);
}

} // namespace marble
