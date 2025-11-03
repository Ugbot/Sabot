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
#include <fcntl.h>
#include <unistd.h>
#include <sys/mman.h>
#include <sys/stat.h>

namespace marble {

// Factory function to create WAL manager - moved to end of file after WalManagerImpl definition

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

// Simple key implementation for WAL demo
class SimpleKey : public Key {
public:
    explicit SimpleKey(const std::string& value) : value_(value) {}

    std::string ToString() const override { return value_; }
    int Compare(const Key& other) const override {
        const SimpleKey& other_key = static_cast<const SimpleKey&>(other);
        return value_.compare(other_key.value_);
    }
    arrow::Result<std::shared_ptr<arrow::Scalar>> ToArrowScalar() const override {
        return arrow::MakeScalar(value_);
    }
    std::shared_ptr<Key> Clone() const override {
        return std::make_shared<SimpleKey>(value_);
    }
    size_t Hash() const override { return std::hash<std::string>()(value_); }

private:
    std::string value_;
};

// Memory-mapped ring buffer WAL implementation
MMRingBufferWalFile::MMRingBufferWalFile(const std::string& path,
                                       const MMRingBufferWalOptions& options)
    : path_(path), options_(options), header_(nullptr), buffer_start_(nullptr),
      buffer_size_(options.buffer_size), write_offset_(sizeof(RingBufferHeader)),
      read_offset_(sizeof(RingBufferHeader))
#ifdef _WIN32
      // Windows initialization would go here
#else
      , fd_(-1)
#endif
      {}

MMRingBufferWalFile::~MMRingBufferWalFile() {
    Close();
}

Status MMRingBufferWalFile::InitializeRingBuffer() {
    // For this demo, use direct POSIX memory mapping to avoid FileHandle complications
#ifdef _WIN32
    // Windows implementation would go here
    return Status::NotImplemented("Windows memory mapping not implemented in demo");
#else
    // POSIX implementation
    int fd = -1;
    bool file_exists = false;

    // Check if file exists and get its size
    struct stat st;
    if (stat(path_.c_str(), &st) == 0) {
        file_exists = true;
        if (st.st_size != static_cast<off_t>(buffer_size_)) {
            return Status::InvalidArgument("Existing file size doesn't match buffer size");
        }
    }

    // Open/create the file
    fd = open(path_.c_str(), O_RDWR | O_CREAT, 0644);
    if (fd == -1) {
        return Status::IOError(std::string("Failed to open file: ") + strerror(errno));
    }

    if (!file_exists) {
        // Pre-allocate the file to the desired size
        if (ftruncate(fd, buffer_size_) != 0) {
            close(fd);
            return Status::IOError(std::string("Failed to resize file: ") + strerror(errno));
        }
    }

    // Memory map the file
    buffer_start_ = mmap(nullptr, buffer_size_, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
    if (buffer_start_ == MAP_FAILED) {
        close(fd);
        return Status::IOError(std::string("Failed to memory map file: ") + strerror(errno));
    }

    // Store file descriptor for cleanup
    fd_ = fd;
    header_ = reinterpret_cast<RingBufferHeader*>(buffer_start_);
#endif

    if (!file_exists) {
        // Initialize header for new file
        header_->magic = 0x4D4D57414C;  // "MMWAL"
        header_->version = 1;
        header_->first_sequence = 0;
        header_->last_sequence = 0;
        header_->write_offset = sizeof(RingBufferHeader);
        header_->read_offset = sizeof(RingBufferHeader);
        header_->checksum = ComputeChecksum(header_, sizeof(RingBufferHeader) - sizeof(uint32_t));
    } else {
        // Validate existing header
        if (header_->magic != 0x4D4D57414C) {
            return Status::Corruption("Invalid WAL file magic number");
        }
        if (header_->version != 1) {
            return Status::Corruption("Unsupported WAL file version");
        }

        // Restore offsets from header
        write_offset_ = header_->write_offset;
        read_offset_ = header_->read_offset;
    }

    return Status::OK();
}

Status MMRingBufferWalFile::WriteEntry(const WalEntry& entry) {
    std::lock_guard<std::mutex> lock(mutex_);

    // Validate buffer state
    if (!buffer_start_ || buffer_start_ == MAP_FAILED) {
        return Status::InternalError("Buffer not properly mapped");
    }

    if (!header_) {
        return Status::InternalError("Header is null");
    }

    // Serialize the entry
    std::string buffer;
    auto status = SerializeEntry(entry, &buffer);
    if (!status.ok()) return status;

    if (buffer.size() > options_.max_entry_size) {
        return Status::InvalidArgument("Entry size exceeds maximum allowed size");
    }

    // Check if we need to wrap around or advance read offset
    size_t current_write = write_offset_.load();
    size_t entry_size = buffer.size() + sizeof(uint32_t);  // Data + size prefix

    if (WouldWrap(current_write, entry_size)) {
        // Move to beginning after header
        size_t new_write_offset = sizeof(RingBufferHeader);

        // Check if this would overwrite unread data
        size_t current_read = read_offset_.load();
        if (new_write_offset <= current_read && current_write >= current_read) {
            // Need to advance read offset to make space
            status = AdvanceReadOffset(new_write_offset);
            if (!status.ok()) return status;
        }

        current_write = new_write_offset;
    } else if (GetNextOffset(current_write, entry_size) > buffer_size_) {
        // Wrap around
        current_write = sizeof(RingBufferHeader);

        // Check if this would overwrite unread data
        size_t current_read = read_offset_.load();
        if (current_write <= current_read) {
            // Need to advance read offset
            status = AdvanceReadOffset(current_write);
            if (!status.ok()) return status;
        }
    }

    // Write the entry size prefix
    uint32_t size_prefix = static_cast<uint32_t>(buffer.size());
    status = WriteData(current_write, &size_prefix, sizeof(uint32_t));
    if (!status.ok()) return status;
    current_write = GetNextOffset(current_write, sizeof(uint32_t));

    // Write the entry data
    status = WriteData(current_write, buffer.data(), buffer.size());
    if (!status.ok()) return status;
    current_write = GetNextOffset(current_write, buffer.size());

    // Update write offset
    status = AdvanceWriteOffset(current_write);
    if (!status.ok()) return status;

    // Update header
    header_->last_sequence = entry.sequence_number;
    if (header_->first_sequence == 0) {
        header_->first_sequence = entry.sequence_number;
    }

    return Status::OK();
}

Status MMRingBufferWalFile::ReadEntries(std::vector<WalEntry>* entries,
                                      uint64_t start_sequence,
                                      size_t max_entries) {
    std::lock_guard<std::mutex> lock(mutex_);

    size_t current_read = read_offset_.load();
    size_t current_write = write_offset_.load();
    size_t entries_read = 0;

    // Start from the read offset, but we need to find entries starting from start_sequence
    size_t read_pos = current_read;

    while (entries_read < max_entries || max_entries == 0) {
        // Check if we've caught up to write offset
        if (read_pos == current_write) {
            break;
        }

        // Read entry size
        uint32_t entry_size;
        auto status = ReadData(read_pos, &entry_size, sizeof(uint32_t));
        if (!status.ok()) return status;
        read_pos = GetNextOffset(read_pos, sizeof(uint32_t));

        if (entry_size > options_.max_entry_size) {
            return Status::Corruption("Invalid entry size in WAL");
        }

        // Read entry data
        std::vector<char> entry_buffer(entry_size);
        status = ReadData(read_pos, entry_buffer.data(), entry_size);
        if (!status.ok()) return status;

        // Deserialize entry
        WalEntry entry;
        status = DeserializeEntry(entry_buffer.data(), entry_size, &entry);
        if (!status.ok()) return status;

        if (entry.sequence_number >= start_sequence) {
            entries->push_back(std::move(entry));
            entries_read++;
        }

        read_pos = GetNextOffset(read_pos, entry_size);
    }

    return Status::OK();
}

Status MMRingBufferWalFile::Flush() {
    if (file_handle_) {
        return file_handle_->Sync();
    }
    return Status::OK();
}

Status MMRingBufferWalFile::Close() {
    if (header_) {
        // Update header with current offsets
        header_->write_offset = write_offset_.load();
        header_->read_offset = read_offset_.load();
        header_->checksum = ComputeChecksum(header_, sizeof(RingBufferHeader) - sizeof(uint32_t));
    }

#ifdef _WIN32
    // Windows cleanup would go here
    if (buffer_start_) {
        // UnmapViewOfFile, etc.
        buffer_start_ = nullptr;
        header_ = nullptr;
    }
#else
    // POSIX cleanup
    if (buffer_start_ && buffer_start_ != MAP_FAILED) {
        munmap(buffer_start_, buffer_size_);
        buffer_start_ = nullptr;
        header_ = nullptr;
    }

    if (fd_ != -1) {
        close(fd_);
        fd_ = -1;
    }
#endif

    if (file_handle_) {
        file_handle_->Close();
        file_handle_.reset();
    }

    return Status::OK();
}

uint64_t MMRingBufferWalFile::GetFirstSequence() const {
    return header_ ? header_->first_sequence : 0;
}

uint64_t MMRingBufferWalFile::GetLastSequence() const {
    return header_ ? header_->last_sequence : 0;
}

Status MMRingBufferWalFile::GetRingBufferStats(std::string* stats) const {
    std::stringstream ss;
    ss << "Ring Buffer Stats:\n";
    ss << "  Buffer size: " << buffer_size_ << " bytes\n";
    ss << "  Write offset: " << write_offset_.load() << "\n";
    ss << "  Read offset: " << read_offset_.load() << "\n";
    ss << "  First sequence: " << GetFirstSequence() << "\n";
    ss << "  Last sequence: " << GetLastSequence() << "\n";
    ss << "  Used space: " << (write_offset_.load() - read_offset_.load()) << " bytes\n";
    *stats = ss.str();
    return Status::OK();
}

Status MMRingBufferWalFile::SerializeEntry(const WalEntry& entry, std::string* buffer) {
    // Simple binary serialization - in production this would be more robust
    buffer->clear();
    buffer->reserve(1024);

    // Write sequence number
    buffer->append(reinterpret_cast<const char*>(&entry.sequence_number), sizeof(uint64_t));

    // Write transaction ID
    buffer->append(reinterpret_cast<const char*>(&entry.transaction_id), sizeof(uint64_t));

    // Write entry type
    buffer->append(reinterpret_cast<const char*>(&entry.entry_type), sizeof(WalEntryType));

    // Write key (simplified - assuming string keys for demo)
    if (entry.key) {
        std::string key_str = entry.key->ToString();
        uint32_t key_size = key_str.size();
        buffer->append(reinterpret_cast<const char*>(&key_size), sizeof(uint32_t));
        buffer->append(key_str);
    } else {
        uint32_t key_size = 0;
        buffer->append(reinterpret_cast<const char*>(&key_size), sizeof(uint32_t));
    }

    // Write value size and data
    if (entry.value) {
        // Simplified serialization - just store a placeholder for demo
        // In production, would properly serialize the Record
        std::string value_data = "record_placeholder";
        uint32_t value_size = static_cast<uint32_t>(value_data.size());
        buffer->append(reinterpret_cast<const char*>(&value_size), sizeof(uint32_t));
        buffer->append(value_data);
    } else {
        uint32_t value_size = 0;
        buffer->append(reinterpret_cast<const char*>(&value_size), sizeof(uint32_t));
    }

    // Write timestamp and other fields
    buffer->append(reinterpret_cast<const char*>(&entry.timestamp), sizeof(uint64_t));

    return Status::OK();
}

Status MMRingBufferWalFile::DeserializeEntry(const void* data, size_t size, WalEntry* entry) {
    // Simplified deserialization - mirror of SerializeEntry
    const char* ptr = static_cast<const char*>(data);

    // Read sequence number
    entry->sequence_number = *reinterpret_cast<const uint64_t*>(ptr);
    ptr += sizeof(uint64_t);

    // Read transaction ID
    entry->transaction_id = *reinterpret_cast<const uint64_t*>(ptr);
    ptr += sizeof(uint64_t);

    // Read entry type
    entry->entry_type = *reinterpret_cast<const WalEntryType*>(ptr);
    ptr += sizeof(WalEntryType);

    // Read key (simplified)
    uint32_t key_size = *reinterpret_cast<const uint32_t*>(ptr);
    ptr += sizeof(uint32_t);
    if (key_size > 0) {
        std::string key_str(ptr, key_size);
        entry->key = std::make_shared<SimpleKey>(key_str);
        ptr += key_size;
    }

    // Read value (placeholder)
    uint32_t value_size = *reinterpret_cast<const uint32_t*>(ptr);
    ptr += sizeof(uint32_t);
    if (value_size > 0) {
        // In production, would deserialize RecordBatch
        entry->value = nullptr;  // Placeholder
        ptr += value_size;
    }

    // Read timestamp
    entry->timestamp = *reinterpret_cast<const uint64_t*>(ptr);

    return Status::OK();
}

Status MMRingBufferWalFile::WriteData(size_t offset, const void* data, size_t size) {
    if (offset + size > buffer_size_) {
        return Status::InvalidArgument("Write would exceed buffer bounds");
    }

    if (!buffer_start_ || buffer_start_ == MAP_FAILED) {
        return Status::InternalError("Buffer start is invalid");
    }

    std::memcpy(static_cast<char*>(buffer_start_) + offset, data, size);
    return Status::OK();
}

Status MMRingBufferWalFile::ReadData(size_t offset, void* data, size_t size) {
    if (offset + size > buffer_size_) {
        return Status::InvalidArgument("Read would exceed buffer bounds");
    }

    std::memcpy(data, static_cast<const char*>(buffer_start_) + offset, size);
    return Status::OK();
}

size_t MMRingBufferWalFile::GetNextOffset(size_t current_offset, size_t data_size) const {
    return (current_offset + data_size) % buffer_size_;
}

bool MMRingBufferWalFile::WouldWrap(size_t current_offset, size_t data_size) const {
    return current_offset + data_size > buffer_size_;
}

Status MMRingBufferWalFile::AdvanceReadOffset(size_t new_offset) {
    read_offset_ = new_offset;
    header_->read_offset = new_offset;
    return Status::OK();
}

Status MMRingBufferWalFile::AdvanceWriteOffset(size_t new_offset) {
    write_offset_ = new_offset;
    header_->write_offset = new_offset;
    return Status::OK();
}

uint32_t MMRingBufferWalFile::ComputeChecksum(const void* data, size_t size) const {
    // Simple checksum - in production use CRC32
    uint32_t checksum = 0;
    const uint8_t* bytes = static_cast<const uint8_t*>(data);
    for (size_t i = 0; i < size; ++i) {
        checksum = (checksum << 5) + checksum + bytes[i];
    }
    return checksum;
}

// Memory-mapped ring buffer WAL manager implementation
MMRingBufferWalManager::MMRingBufferWalManager(TaskScheduler* scheduler)
    : scheduler_(scheduler), current_sequence_(0), is_open_(false) {}

MMRingBufferWalManager::~MMRingBufferWalManager() {
    Close();
}

Status MMRingBufferWalManager::Open(const WalOptions& options) {
    std::lock_guard<std::mutex> lock(mutex_);

    if (is_open_) {
        return Status::InvalidArgument("WAL manager is already open");
    }

    options_ = options;
    mm_options_ = static_cast<const MMRingBufferWalOptions&>(options);

    auto status = EnsureFileExists();
    if (!status.ok()) return status;

    wal_file_ = std::make_unique<MMRingBufferWalFile>(options_.wal_path, mm_options_);
    status = wal_file_->InitializeRingBuffer();
    if (!status.ok()) return status;

    // Recover existing entries
    status = RecoverFromFile(nullptr, 0);
    if (!status.ok()) return status;

    is_open_ = true;
    return Status::OK();
}

Status MMRingBufferWalManager::Close() {
    std::lock_guard<std::mutex> lock(mutex_);

    if (!is_open_) {
        return Status::OK();
    }

    if (wal_file_) {
        auto status = wal_file_->Close();
        if (!status.ok()) return status;
        wal_file_.reset();
    }

    is_open_ = false;
    return Status::OK();
}

Status MMRingBufferWalManager::WriteEntry(const WalEntry& entry) {
    if (!is_open_) {
        return Status::InvalidArgument("WAL manager is not open");
    }

    return wal_file_->WriteEntry(entry);
}

Status MMRingBufferWalManager::WriteBatch(const std::vector<WalEntry>& entries) {
    if (!is_open_) {
        return Status::InvalidArgument("WAL manager is not open");
    }

    for (const auto& entry : entries) {
        auto status = wal_file_->WriteEntry(entry);
        if (!status.ok()) return status;
    }

    return Status::OK();
}

uint64_t MMRingBufferWalManager::GetCurrentSequence() const {
    return current_sequence_.load();
}

Status MMRingBufferWalManager::Sync() {
    if (!is_open_) {
        return Status::InvalidArgument("WAL manager is not open");
    }

    return wal_file_->Flush();
}

Status MMRingBufferWalManager::Rotate() {
    // For ring buffer, rotation is automatic - no-op
    return Status::OK();
}

Status MMRingBufferWalManager::Recover(std::function<Status(const WalEntry&)> callback,
                                     uint64_t start_sequence) {
    if (!is_open_) {
        return Status::InvalidArgument("WAL manager is not open");
    }

    return RecoverFromFile(callback, start_sequence);
}

Status MMRingBufferWalManager::Truncate(uint64_t sequence_number) {
    // For ring buffer, truncation is automatic when buffer wraps
    // In a full implementation, we might need to advance read offset
    return Status::OK();
}

Status MMRingBufferWalManager::GetStats(std::string* stats) const {
    std::lock_guard<std::mutex> lock(mutex_);

    if (!is_open_) {
        return Status::InvalidArgument("WAL manager is not open");
    }

    std::string ring_stats;
    auto status = wal_file_->GetRingBufferStats(&ring_stats);
    if (!status.ok()) return status;

    std::stringstream ss;
    ss << "Memory-Mapped Ring Buffer WAL Stats:\n";
    ss << "  Current sequence: " << current_sequence_.load() << "\n";
    ss << "  Buffer size: " << mm_options_.buffer_size << " bytes\n";
    ss << ring_stats;

    *stats = ss.str();
    return Status::OK();
}

Status MMRingBufferWalManager::ListFiles(std::vector<std::string>* files) const {
    files->clear();
    files->push_back(options_.wal_path);
    return Status::OK();
}

Status MMRingBufferWalManager::EnsureFileExists() {
    auto filesystem = FileSystem::CreateLocal();
    bool exists;
    auto status = filesystem->FileExists(options_.wal_path, &exists);
    if (!status.ok()) return status;

    // For demo purposes, just check if file exists
    // In production, would create parent directories as needed
    return Status::OK();
}

Status MMRingBufferWalManager::RecoverFromFile(std::function<Status(const WalEntry&)> callback,
                                             uint64_t start_sequence) {
    std::vector<WalEntry> entries;
    auto status = wal_file_->ReadEntries(&entries, start_sequence);
    if (!status.ok()) return status;

    for (const auto& entry : entries) {
        current_sequence_ = std::max(current_sequence_.load(), entry.sequence_number);
        if (callback) {
            status = callback(entry);
            if (!status.ok()) return status;
        }
    }

    return Status::OK();
}

// Factory functions
std::unique_ptr<WalManager> CreateMMRingBufferWalManager(TaskScheduler* scheduler) {
    return std::make_unique<MMRingBufferWalManager>(scheduler);
}

std::unique_ptr<WalManager> CreateWalManager() {
    return std::make_unique<WalManagerImpl>();
}

std::unique_ptr<WalManager> CreateWalManager(TaskScheduler* scheduler) {
    return std::make_unique<WalManagerImpl>(scheduler);
}

std::unique_ptr<WalFile> CreateWalFile(const std::string& path, uint64_t file_id) {
    WalOptions options;
    return std::make_unique<WalFileImpl>(path, file_id, options);
}

} // namespace marble
