#include <marble/wal.h>
#include <marble/file_system.h>
#include <marble/task_scheduler.h>
#include <algorithm>
#include <chrono>
#include <sstream>
#include <iostream>
#include <iomanip>
#include <cstring>
#include <zlib.h>  // For compression (if enabled)

namespace marble {

// WalEntry implementation
WalEntry::WalEntry(uint64_t seq, uint64_t txn_id, WalEntryType type,
                   std::shared_ptr<Key> k, std::shared_ptr<Record> v,
                   uint64_t ts)
    : sequence_number(seq), transaction_id(txn_id), entry_type(type),
      key(std::move(k)), value(std::move(v)), timestamp(ts) {

    if (timestamp == 0) {
        timestamp = std::chrono::duration_cast<std::chrono::microseconds>(
            std::chrono::system_clock::now().time_since_epoch()).count();
    }

    checksum = ComputeChecksum(*this);
}

// WAL file implementation
class WalFileImpl : public WalFile {
public:
    WalFileImpl(const std::string& path, uint64_t file_id, const WalOptions& options);
    ~WalFileImpl() override { Close(); }

    Status WriteEntry(const WalEntry& entry) override;
    Status ReadEntries(std::vector<WalEntry>* entries,
                      uint64_t start_sequence = 0,
                      size_t max_entries = 0) override;
    Status Flush() override;
    Status Close() override;

    std::string GetFilePath() const override { return file_path_; }
    uint64_t GetFileId() const override { return file_id_; }
    size_t GetFileSize() const override;
    uint64_t GetFirstSequence() const override { return first_sequence_; }
    uint64_t GetLastSequence() const override { return last_sequence_; }
    bool IsFull() const override;

private:
    Status SerializeEntry(const WalEntry& entry, std::string* buffer);
    Status DeserializeEntry(const std::string& buffer, WalEntry* entry);

    std::string file_path_;
    uint64_t file_id_;
    WalOptions options_;
    std::unique_ptr<FileHandle> file_handle_;
    std::atomic<size_t> file_size_{0};
    std::atomic<uint64_t> first_sequence_{0};
    std::atomic<uint64_t> last_sequence_{0};
    std::vector<char> write_buffer_;
    bool is_open_{false};
};

// WAL manager implementation
class WalManagerImpl : public WalManager {
public:
    explicit WalManagerImpl(TaskScheduler* scheduler = nullptr);
    ~WalManagerImpl() override { Close(); }

    Status Open(const WalOptions& options) override;
    Status Close() override;
    Status WriteEntry(const WalEntry& entry) override;
    Status WriteBatch(const std::vector<WalEntry>& entries) override;
    uint64_t GetCurrentSequence() const override;
    Status Sync() override;
    Status Rotate() override;
    Status Recover(std::function<Status(const WalEntry&)> callback,
                  uint64_t start_sequence = 0) override;
    Status Truncate(uint64_t sequence_number) override;
    Status GetStats(std::string* stats) const override;
    Status ListFiles(std::vector<std::string>* files) const override;

private:
    Status CreateNewWalFile();
    Status CleanupOldFiles();
    std::string GetWalFilePath(uint64_t file_id) const;

    WalOptions options_;
    TaskScheduler* scheduler_;
    std::unique_ptr<WalFileImpl> current_file_;
    std::atomic<uint64_t> current_sequence_{0};
    std::atomic<uint64_t> current_file_id_{0};
    std::vector<std::unique_ptr<WalFileImpl>> wal_files_;
    bool is_open_{false};
    mutable std::mutex mutex_;  // For thread safety
};

// WalFileImpl implementation
WalFileImpl::WalFileImpl(const std::string& path, uint64_t file_id, const WalOptions& options)
    : file_path_(path), file_id_(file_id), options_(options), write_buffer_(options.buffer_size) {

    // Create or open the file
    std::unique_ptr<FileSystem> fs = FileSystem::CreateLocal();
    FileOpenFlags flags = static_cast<FileOpenFlags>(
        static_cast<int>(FileOpenFlags::kWrite) | static_cast<int>(FileOpenFlags::kCreate));

    Status status = fs->OpenFile(path, flags, &file_handle_);
    if (!status.ok()) {
        std::cerr << "Failed to open WAL file: " << path << " - " << status.ToString() << std::endl;
        return;
    }

    is_open_ = true;
}

Status WalFileImpl::WriteEntry(const WalEntry& entry) {
    if (!is_open_) {
        return Status::InvalidArgument("WAL file is not open");
    }

    std::string buffer;
    Status status = SerializeEntry(entry, &buffer);
    if (!status.ok()) {
        return status;
    }

    // Check if writing this entry would exceed file size limit
    if (file_size_.load() + buffer.size() > options_.max_file_size) {
        return Status::ResourceExhausted("WAL file size limit exceeded");
    }

    // Write to buffer first
    if (write_buffer_.size() + buffer.size() > write_buffer_.capacity()) {
        // Flush buffer first
        Status flush_status = Flush();
        if (!flush_status.ok()) {
            return flush_status;
        }
    }

    // Add to write buffer
    write_buffer_.insert(write_buffer_.end(), buffer.begin(), buffer.end());

    // Update metadata
    if (first_sequence_ == 0) {
        first_sequence_ = entry.sequence_number;
    }
    last_sequence_ = entry.sequence_number;
    file_size_ += buffer.size();

    // Auto-flush based on sync mode
    if (options_.sync_mode == WalOptions::SyncMode::kSync) {
        return Flush();
    }

    return Status::OK();
}

Status WalFileImpl::ReadEntries(std::vector<WalEntry>* entries,
                               uint64_t start_sequence,
                               size_t max_entries) {
    if (!is_open_) {
        return Status::InvalidArgument("WAL file is not open");
    }

    // For simplicity, this implementation assumes we can read the entire file
    // A full implementation would need proper streaming and seeking
    entries->clear();

    // Read entire file into buffer using file handle
    size_t file_size = GetFileSize();
    if (file_size == 0) {
        return Status::OK();
    }

    std::string buffer;
    buffer.resize(file_size);
    size_t bytes_read;
    Status status = file_handle_->Read(buffer.data(), file_size, &bytes_read);
    if (!status.ok()) {
        return status;
    }

    if (bytes_read != file_size) {
        buffer.resize(bytes_read);
    }

    size_t offset = 0;
    size_t entries_read = 0;

    while (offset < buffer.size() && (max_entries == 0 || entries_read < max_entries)) {
        // Read entry size (first 4 bytes)
        if (offset + 4 > buffer.size()) break;

        uint32_t entry_size;
        std::memcpy(&entry_size, buffer.data() + offset, 4);
        offset += 4;

        if (offset + entry_size > buffer.size()) break;

        // Deserialize entry
        std::string entry_buffer(buffer.data() + offset, entry_size);
        WalEntry entry;
        Status deserialize_status = DeserializeEntry(entry_buffer, &entry);
        if (!deserialize_status.ok()) {
            return deserialize_status;
        }

        offset += entry_size;

        // Check if this entry is within our range
        if (entry.sequence_number >= start_sequence) {
            entries->push_back(entry);
            entries_read++;
        }
    }

    return Status::OK();
}

Status WalFileImpl::Flush() {
    if (!is_open_ || write_buffer_.empty()) {
        return Status::OK();
    }

    // Write buffer to file
    size_t bytes_written;
    Status status = file_handle_->Write(write_buffer_.data(), write_buffer_.size(), &bytes_written);
    if (!status.ok()) {
        return status;
    }

    if (bytes_written != write_buffer_.size()) {
        return Status::IOError("Incomplete write to WAL file");
    }

    // Sync to disk based on sync mode
    if (options_.sync_mode != WalOptions::SyncMode::kAsync) {
        status = file_handle_->Sync();
        if (!status.ok()) {
            return status;
        }
    }

    write_buffer_.clear();
    return Status::OK();
}

Status WalFileImpl::Close() {
    if (!is_open_) {
        return Status::OK();
    }

    // Flush any remaining data
    Status status = Flush();
    if (!status.ok()) {
        return status;
    }

    // Close file
    file_handle_.reset();
    is_open_ = false;
    return Status::OK();
}

size_t WalFileImpl::GetFileSize() const {
    return file_size_.load();
}

bool WalFileImpl::IsFull() const {
    return file_size_.load() >= options_.max_file_size;
}

Status WalFileImpl::SerializeEntry(const WalEntry& entry, std::string* buffer) {
    // Simple binary serialization
    // Format: [size][sequence][txn_id][entry_type][timestamp][key_size][key_data][value_size][value_data][checksum]

    buffer->clear();

    // Reserve space for size field
    buffer->resize(4);

    // Write sequence number
    uint64_t seq = entry.sequence_number;
    buffer->append(reinterpret_cast<char*>(&seq), sizeof(seq));

    // Write transaction ID
    uint64_t txn_id = entry.transaction_id;
    buffer->append(reinterpret_cast<char*>(&txn_id), sizeof(txn_id));

    // Write entry type
    uint8_t entry_type = static_cast<uint8_t>(entry.entry_type);
    buffer->append(reinterpret_cast<char*>(&entry_type), sizeof(entry_type));

    // Write timestamp
    uint64_t timestamp = entry.timestamp;
    buffer->append(reinterpret_cast<char*>(&timestamp), sizeof(timestamp));

    // Write checksum
    uint32_t checksum = entry.checksum;
    buffer->append(reinterpret_cast<char*>(&checksum), sizeof(checksum));

    // Write key (simplified - assuming key can be serialized)
    if (entry.key) {
        std::string key_str = entry.key->ToString();
        uint32_t key_size = key_str.size();
        buffer->append(reinterpret_cast<char*>(&key_size), sizeof(key_size));
        buffer->append(key_str);
    } else {
        uint32_t key_size = 0;
        buffer->append(reinterpret_cast<char*>(&key_size), sizeof(key_size));
    }

    // Write value (simplified)
    if (entry.value) {
        // For demo, we'll just store a placeholder
        std::string value_str = "placeholder_record";
        uint32_t value_size = value_str.size();
        buffer->append(reinterpret_cast<char*>(&value_size), sizeof(value_size));
        buffer->append(value_str);
    } else {
        uint32_t value_size = 0;
        buffer->append(reinterpret_cast<char*>(&value_size), sizeof(value_size));
    }

    // Write the size at the beginning
    uint32_t total_size = buffer->size() - 4;
    std::memcpy(buffer->data(), &total_size, 4);

    return Status::OK();
}

Status WalFileImpl::DeserializeEntry(const std::string& buffer, WalEntry* entry) {
    if (buffer.size() < 4) {
        return Status::InvalidArgument("Buffer too small for WAL entry");
    }

    size_t offset = 0;

    // Read sequence number
    uint64_t seq;
    std::memcpy(&seq, buffer.data() + offset, sizeof(seq));
    entry->sequence_number = seq;
    offset += sizeof(seq);

    // Read transaction ID
    uint64_t txn_id;
    std::memcpy(&txn_id, buffer.data() + offset, sizeof(txn_id));
    entry->transaction_id = txn_id;
    offset += sizeof(txn_id);

    // Read entry type
    uint8_t entry_type;
    std::memcpy(&entry_type, buffer.data() + offset, sizeof(entry_type));
    entry->entry_type = static_cast<WalEntryType>(entry_type);
    offset += sizeof(entry_type);

    // Read timestamp
    uint64_t timestamp;
    std::memcpy(&timestamp, buffer.data() + offset, sizeof(timestamp));
    entry->timestamp = timestamp;
    offset += sizeof(timestamp);

    // Read checksum
    uint32_t checksum;
    std::memcpy(&checksum, buffer.data() + offset, sizeof(checksum));
    entry->checksum = checksum;
    offset += sizeof(checksum);

    // Read key
    uint32_t key_size;
    std::memcpy(&key_size, buffer.data() + offset, sizeof(key_size));
    offset += sizeof(key_size);

    if (key_size > 0) {
        // Simplified key reconstruction
        std::string key_str(buffer.data() + offset, key_size);
        // In a real implementation, you'd reconstruct the proper Key object
        entry->key = nullptr;  // Placeholder
        offset += key_size;
    }

    // Read value
    uint32_t value_size;
    std::memcpy(&value_size, buffer.data() + offset, sizeof(value_size));
    offset += sizeof(value_size);

    if (value_size > 0) {
        // Simplified value reconstruction
        std::string value_str(buffer.data() + offset, value_size);
        // In a real implementation, you'd reconstruct the proper Record object
        entry->value = nullptr;  // Placeholder
        offset += value_size;
    }

    return Status::OK();
}

// WalManagerImpl implementation
WalManagerImpl::WalManagerImpl(TaskScheduler* scheduler)
    : scheduler_(scheduler) {}

Status WalManagerImpl::Open(const WalOptions& options) {
    std::lock_guard<std::mutex> lock(mutex_);

    if (is_open_) {
        return Status::InvalidArgument("WAL manager is already open");
    }

    options_ = options;

    // Create WAL directory if it doesn't exist
    std::unique_ptr<FileSystem> fs = FileSystem::CreateLocal();
    Status status = fs->CreateDirectory(options.wal_path);
    if (!status.ok()) {
        // Check if directory already exists (simplified check)
        bool exists = false;
        Status exists_status = fs->FileExists(options.wal_path, &exists);
        if (!exists_status.ok() || !exists) {
            return status;  // Directory creation failed for other reasons
        }
        // Directory already exists, continue
    }

    // Create initial WAL file
    status = CreateNewWalFile();
    if (!status.ok()) {
        return status;
    }

    is_open_ = true;
    return Status::OK();
}

Status WalManagerImpl::Close() {
    std::lock_guard<std::mutex> lock(mutex_);

    if (!is_open_) {
        return Status::OK();
    }

    // Close all WAL files
    for (auto& file : wal_files_) {
        file->Close();
    }
    wal_files_.clear();
    current_file_.reset();

    is_open_ = false;
    return Status::OK();
}

Status WalManagerImpl::WriteEntry(const WalEntry& entry) {
    std::lock_guard<std::mutex> lock(mutex_);

    if (!is_open_) {
        return Status::InvalidArgument("WAL manager is not open");
    }

    // Check if we need to rotate the file
    if (current_file_ && current_file_->IsFull()) {
        Status status = Rotate();
        if (!status.ok()) {
            return status;
        }
    }

    // Create new file if needed
    if (!current_file_) {
        Status status = CreateNewWalFile();
        if (!status.ok()) {
            return status;
        }
    }

    // Write the entry
    Status status = current_file_->WriteEntry(entry);
    if (!status.ok()) {
        return status;
    }

    // Update sequence number
    current_sequence_ = entry.sequence_number;

    // Periodic sync for batch mode
    if (options_.sync_mode == WalOptions::SyncMode::kBatch &&
        current_sequence_ % options_.sync_interval == 0) {
        return current_file_->Flush();
    }

    return Status::OK();
}

Status WalManagerImpl::WriteBatch(const std::vector<WalEntry>& entries) {
    for (const auto& entry : entries) {
        Status status = WriteEntry(entry);
        if (!status.ok()) {
            return status;
        }
    }
    return Status::OK();
}

uint64_t WalManagerImpl::GetCurrentSequence() const {
    return current_sequence_.load();
}

Status WalManagerImpl::Sync() {
    std::lock_guard<std::mutex> lock(mutex_);

    if (!is_open_) {
        return Status::InvalidArgument("WAL manager is not open");
    }

    if (current_file_) {
        return current_file_->Flush();
    }

    return Status::OK();
}

Status WalManagerImpl::Rotate() {
    std::lock_guard<std::mutex> lock(mutex_);

    if (!is_open_) {
        return Status::InvalidArgument("WAL manager is not open");
    }

    // Close current file
    if (current_file_) {
        current_file_->Flush();
        wal_files_.push_back(std::move(current_file_));
    }

    // Create new file
    Status status = CreateNewWalFile();
    if (!status.ok()) {
        return status;
    }

    // Cleanup old files
    return CleanupOldFiles();
}

Status WalManagerImpl::Recover(std::function<Status(const WalEntry&)> callback,
                              uint64_t start_sequence) {
    std::lock_guard<std::mutex> lock(mutex_);

    if (!is_open_) {
        return Status::InvalidArgument("WAL manager is not open");
    }

    // Read all WAL files in order
    for (const auto& file : wal_files_) {
        std::vector<WalEntry> entries;
        Status status = file->ReadEntries(&entries, start_sequence);
        if (!status.ok()) {
            return status;
        }

        for (const auto& entry : entries) {
            Status callback_status = callback(entry);
            if (!callback_status.ok()) {
                return callback_status;
            }
        }
    }

    return Status::OK();
}

Status WalManagerImpl::Truncate(uint64_t sequence_number) {
    std::lock_guard<std::mutex> lock(mutex_);

    if (!is_open_) {
        return Status::InvalidArgument("WAL manager is not open");
    }

    // Remove WAL files that contain only entries before the sequence number
    auto it = wal_files_.begin();
    while (it != wal_files_.end()) {
        if ((*it)->GetLastSequence() < sequence_number) {
            // This file can be deleted
            (*it)->Close();
            it = wal_files_.erase(it);
        } else {
            ++it;
        }
    }

    return Status::OK();
}

Status WalManagerImpl::GetStats(std::string* stats) const {
    std::lock_guard<std::mutex> lock(mutex_);

    std::stringstream ss;
    ss << "WalStats{"
       << "current_sequence=" << current_sequence_.load()
       << ", num_files=" << wal_files_.size()
       << ", sync_mode=" << (options_.sync_mode == WalOptions::SyncMode::kAsync ? "async" :
                            options_.sync_mode == WalOptions::SyncMode::kSync ? "sync" : "batch")
       << ", compression=" << (options_.enable_compression ? "enabled" : "disabled")
       << ", checksum=" << (options_.enable_checksum ? "enabled" : "disabled")
       << "}";

    *stats = ss.str();
    return Status::OK();
}

Status WalManagerImpl::ListFiles(std::vector<std::string>* files) const {
    std::lock_guard<std::mutex> lock(mutex_);

    files->clear();
    for (const auto& file : wal_files_) {
        files->push_back(file->GetFilePath());
    }

    return Status::OK();
}

Status WalManagerImpl::CreateNewWalFile() {
    current_file_id_++;
    std::string file_path = GetWalFilePath(current_file_id_);

    current_file_ = std::make_unique<WalFileImpl>(file_path, current_file_id_, options_);

    return Status::OK();
}

Status WalManagerImpl::CleanupOldFiles() {
    // Keep only the most recent max_files WAL files
    while (wal_files_.size() >= options_.max_files) {
        wal_files_.front()->Close();
        wal_files_.erase(wal_files_.begin());
    }

    return Status::OK();
}

std::string WalManagerImpl::GetWalFilePath(uint64_t file_id) const {
    std::stringstream ss;
    ss << options_.wal_path << "/wal_" << std::setfill('0') << std::setw(10) << file_id << ".log";
    return ss.str();
}

// Utility functions
uint32_t ComputeChecksum(const WalEntry& entry) {
    // Simple checksum computation
    uint32_t checksum = 0;
    checksum ^= static_cast<uint32_t>(entry.sequence_number);
    checksum ^= static_cast<uint32_t>(entry.transaction_id);
    checksum ^= static_cast<uint32_t>(entry.entry_type);
    checksum ^= static_cast<uint32_t>(entry.timestamp);
    checksum ^= static_cast<uint32_t>(entry.part_index);
    checksum ^= static_cast<uint32_t>(entry.total_parts);

    if (entry.key) {
        std::string key_str = entry.key->ToString();
        for (char c : key_str) {
            checksum ^= static_cast<uint32_t>(c);
        }
    }

    return checksum;
}

bool ValidateChecksum(const WalEntry& entry) {
    return ComputeChecksum(entry) == entry.checksum;
}

// Factory functions
std::unique_ptr<WalManager> CreateWalManager(TaskScheduler* scheduler) {
    return std::make_unique<WalManagerImpl>(scheduler);
}

std::unique_ptr<WalFile> CreateWalFile(const std::string& path, uint64_t file_id) {
    WalOptions options;
    return std::make_unique<WalFileImpl>(path, file_id, options);
}

} // namespace marble
