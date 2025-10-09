#include <marble/file_system.h>
#include <fstream>
#include <filesystem>
#include <system_error>
#include <iostream>
#include <fcntl.h>
#include <unistd.h>
#include <sys/mman.h>
#ifdef _WIN32
#include <windows.h>
#endif

namespace fs = std::filesystem;

namespace marble {

// FileHandle base implementation
FileHandle::FileHandle(const std::string& path, FileOpenFlags flags)
    : path_(path), flags_(flags) {}

FileHandle::~FileHandle() = default;

// Local file handle implementation
class LocalFileHandle : public FileHandle {
public:
    LocalFileHandle(const std::string& path, FileOpenFlags flags)
        : FileHandle(path, flags) {
        // Convert flags to std::ios flags
        std::ios::openmode mode = std::ios::binary;

        if (static_cast<int>(flags) & static_cast<int>(FileOpenFlags::kRead)) {
            mode |= std::ios::in;
        }
        if (static_cast<int>(flags) & static_cast<int>(FileOpenFlags::kWrite)) {
            mode |= std::ios::out;
        }
        if (static_cast<int>(flags) & static_cast<int>(FileOpenFlags::kAppend)) {
            mode |= std::ios::app;
        }
        if (static_cast<int>(flags) & static_cast<int>(FileOpenFlags::kTruncate)) {
            mode |= std::ios::trunc;
        }

        file_.open(path, mode);
        if (!file_.is_open()) {
            throw std::system_error(errno, std::system_category(),
                                   "Failed to open file: " + path);
        }
    }

    ~LocalFileHandle() override {
        if (file_.is_open()) {
            file_.close();
        }
    }

    Status Read(void* buffer, size_t nr_bytes, size_t* bytes_read) override {
        try {
            file_.read(static_cast<char*>(buffer), nr_bytes);
            *bytes_read = file_.gcount();
            if (file_.bad()) {
                return Status::IOError("Read error");
            }
            return Status::OK();
        } catch (const std::exception& e) {
            return Status::IOError(std::string("Read failed: ") + e.what());
        }
    }

    Status Write(const void* buffer, size_t nr_bytes, size_t* bytes_written) override {
        try {
            file_.write(static_cast<const char*>(buffer), nr_bytes);
            if (file_.bad()) {
                return Status::IOError("Write error");
            }
            *bytes_written = nr_bytes;
            return Status::OK();
        } catch (const std::exception& e) {
            return Status::IOError(std::string("Write failed: ") + e.what());
        }
    }

    Status Seek(size_t position) override {
        try {
            file_.seekg(position);
            file_.seekp(position);
            if (file_.bad()) {
                return Status::IOError("Seek error");
            }
            return Status::OK();
        } catch (const std::exception& e) {
            return Status::IOError(std::string("Seek failed: ") + e.what());
        }
    }

    Status GetPosition(size_t* position) const override {
        try {
            *position = file_.tellg();
            return Status::OK();
        } catch (const std::exception& e) {
            return Status::IOError(std::string("Get position failed: ") + e.what());
        }
    }

    Status GetSize(size_t* size) const override {
        try {
            auto current_pos = file_.tellg();
            file_.seekg(0, std::ios::end);
            *size = file_.tellg();
            file_.seekg(current_pos);
            return Status::OK();
        } catch (const std::exception& e) {
            return Status::IOError(std::string("Get size failed: ") + e.what());
        }
    }

    bool CanSeek() const override { return true; }

    Status Sync() override {
        try {
            file_.flush();
            return Status::OK();
        } catch (const std::exception& e) {
            return Status::IOError(std::string("Sync failed: ") + e.what());
        }
    }

    Status Truncate(size_t new_size) override {
        // Note: std::fstream doesn't support truncate directly
        // In production, this would use platform-specific APIs
        return Status::InvalidArgument("Truncate not implemented for LocalFileHandle");
    }

    Status MemoryMap(size_t offset, size_t size, void** mapped_memory) override {
#ifdef _WIN32
        // Windows memory mapping implementation
        HANDLE file_handle = CreateFileA(path_.c_str(), GENERIC_READ | GENERIC_WRITE,
                                        FILE_SHARE_READ | FILE_SHARE_WRITE, nullptr,
                                        OPEN_EXISTING, FILE_ATTRIBUTE_NORMAL, nullptr);
        if (file_handle == INVALID_HANDLE_VALUE) {
            return Status::IOError("Failed to open file for memory mapping");
        }

        HANDLE mapping_handle = CreateFileMappingA(file_handle, nullptr, PAGE_READWRITE,
                                                  0, 0, nullptr);
        if (mapping_handle == nullptr) {
            CloseHandle(file_handle);
            return Status::IOError("Failed to create file mapping");
        }

        *mapped_memory = MapViewOfFile(mapping_handle, FILE_MAP_ALL_ACCESS,
                                      static_cast<DWORD>(offset >> 32),
                                      static_cast<DWORD>(offset & 0xFFFFFFFF), size);
        if (*mapped_memory == nullptr) {
            CloseHandle(mapping_handle);
            CloseHandle(file_handle);
            return Status::IOError("Failed to map view of file");
        }

        // Store handles for cleanup
        mapping_handle_ = mapping_handle;
        file_handle_ = file_handle;
        mapped_memory_ = *mapped_memory;
        mapped_size_ = size;
        return Status::OK();
#else
        // POSIX memory mapping implementation
        int fd = open(path_.c_str(), O_RDWR);
        if (fd == -1) {
            return Status::IOError(std::string("Failed to open file for memory mapping: ") +
                                  strerror(errno));
        }

        *mapped_memory = mmap(nullptr, size, PROT_READ | PROT_WRITE, MAP_SHARED, fd, offset);
        if (*mapped_memory == MAP_FAILED) {
            close(fd);
            return Status::IOError(std::string("Failed to memory map file: ") +
                                  strerror(errno));
        }

        // Store file descriptor for cleanup
        fd_ = fd;
        mapped_memory_ = *mapped_memory;
        mapped_size_ = size;
        return Status::OK();
#endif
    }

    Status UnmapMemory(void* mapped_memory, size_t size) override {
#ifdef _WIN32
        if (mapped_memory == mapped_memory_ && mapping_handle_ != nullptr) {
            UnmapViewOfFile(mapped_memory);
            CloseHandle(mapping_handle_);
            CloseHandle(file_handle_);
            mapping_handle_ = nullptr;
            file_handle_ = nullptr;
            mapped_memory_ = nullptr;
            mapped_size_ = 0;
        }
        return Status::OK();
#else
        if (mapped_memory == mapped_memory_ && fd_ != -1) {
            munmap(mapped_memory, size);
            close(fd_);
            fd_ = -1;
            mapped_memory_ = nullptr;
            mapped_size_ = 0;
        }
        return Status::OK();
#endif
    }

    bool SupportsMemoryMapping() const override {
        return true;
    }

    Status Close() override {
        try {
            // Unmap memory if mapped
            if (mapped_memory_ != nullptr) {
                UnmapMemory(mapped_memory_, mapped_size_);
            }
            file_.close();
            return Status::OK();
        } catch (const std::exception& e) {
            return Status::IOError(std::string("Close failed: ") + e.what());
        }
    }

private:
    mutable std::fstream file_;
#ifdef _WIN32
    HANDLE file_handle_ = nullptr;
    HANDLE mapping_handle_ = nullptr;
#else
    int fd_ = -1;
#endif
    void* mapped_memory_ = nullptr;
    size_t mapped_size_ = 0;
};

// LocalFileSystem implementation
LocalFileSystem::LocalFileSystem() : default_compression_(FileCompressionType::kNoCompression) {}

Status LocalFileSystem::OpenFile(const std::string& path, FileOpenFlags flags,
                                std::unique_ptr<FileHandle>* handle) {
    try {
        *handle = std::make_unique<LocalFileHandle>(path, flags);
        return Status::OK();
    } catch (const std::exception& e) {
        return Status::IOError(std::string("Failed to open file: ") + e.what());
    }
}

Status LocalFileSystem::FileExists(const std::string& path, bool* exists) {
    try {
        *exists = fs::exists(path);
        return Status::OK();
    } catch (const std::exception& e) {
        return Status::IOError(std::string("File exists check failed: ") + e.what());
    }
}

Status LocalFileSystem::GetFileInfo(const std::string& path, FileInfo* info) {
    try {
        if (!fs::exists(path)) {
            return Status::NotFound("File does not exist: " + path);
        }

        auto status = fs::status(path);
        info->path = path;
        info->is_directory = fs::is_directory(status);
        info->is_regular_file = fs::is_regular_file(status);

        if (info->is_regular_file) {
            info->size = fs::file_size(path);
        }

        auto time = fs::last_write_time(path);
        info->modification_time = std::chrono::duration_cast<std::chrono::seconds>(
            time.time_since_epoch()).count();

        return Status::OK();
    } catch (const std::exception& e) {
        return Status::IOError(std::string("Get file info failed: ") + e.what());
    }
}

Status LocalFileSystem::ListFiles(const std::string& directory,
                                 std::vector<FileInfo>* files) {
    try {
        if (!fs::exists(directory) || !fs::is_directory(directory)) {
            return Status::InvalidArgument("Not a directory: " + directory);
        }

        files->clear();
        for (const auto& entry : fs::directory_iterator(directory)) {
            FileInfo info;
            info.path = entry.path().string();
            info.is_directory = entry.is_directory();
            info.is_regular_file = entry.is_regular_file();

            if (info.is_regular_file) {
                info.size = entry.file_size();
            }

            files->push_back(info);
        }

        return Status::OK();
    } catch (const std::exception& e) {
        return Status::IOError(std::string("List files failed: ") + e.what());
    }
}

Status LocalFileSystem::CreateDirectory(const std::string& path) {
    try {
        if (!fs::create_directories(path)) {
            if (!fs::exists(path)) {
                return Status::IOError("Failed to create directory: " + path);
            }
        }
        return Status::OK();
    } catch (const std::exception& e) {
        return Status::IOError(std::string("Create directory failed: ") + e.what());
    }
}

Status LocalFileSystem::RemoveFile(const std::string& path) {
    try {
        if (!fs::remove(path)) {
            return Status::NotFound("File not found: " + path);
        }
        return Status::OK();
    } catch (const std::exception& e) {
        return Status::IOError(std::string("Remove file failed: ") + e.what());
    }
}

Status LocalFileSystem::RemoveDirectory(const std::string& path) {
    try {
        if (!fs::remove(path)) {
            return Status::NotFound("Directory not found: " + path);
        }
        return Status::OK();
    } catch (const std::exception& e) {
        return Status::IOError(std::string("Remove directory failed: ") + e.what());
    }
}

Status LocalFileSystem::NormalizePath(const std::string& path, std::string* normalized) {
    try {
        *normalized = fs::path(path).lexically_normal().string();
        return Status::OK();
    } catch (const std::exception& e) {
        return Status::InvalidArgument(std::string("Invalid path: ") + e.what());
    }
}

Status LocalFileSystem::JoinPath(const std::string& base, const std::string& path,
                                std::string* result) {
    try {
        fs::path base_path(base);
        fs::path joined = base_path / path;
        *result = joined.string();
        return Status::OK();
    } catch (const std::exception& e) {
        return Status::InvalidArgument(std::string("Path join failed: ") + e.what());
    }
}

bool LocalFileSystem::SupportsCompression(FileCompressionType type) const {
    // Basic support - in production this would check available libraries
    return type == FileCompressionType::kNoCompression ||
           type == FileCompressionType::kLZ4 ||
           type == FileCompressionType::kZstd;
}

Status LocalFileSystem::SetDefaultCompression(FileCompressionType type) {
    if (!SupportsCompression(type)) {
        return Status::InvalidArgument("Unsupported compression type");
    }
    default_compression_ = type;
    return Status::OK();
}

Status LocalFileSystem::RegisterSubSystem(const std::string& prefix,
                                        std::unique_ptr<FileSystem> fs) {
    subsystems_[prefix] = std::move(fs);
    return Status::OK();
}

// Factory function
std::unique_ptr<FileSystem> FileSystem::CreateLocal() {
    return std::make_unique<LocalFileSystem>();
}

// CompressedFileSystem implementation (simplified)
CompressedFileSystem::CompressedFileSystem(std::unique_ptr<FileSystem> base_fs,
                                         FileCompressionType compression_type)
    : base_fs_(std::move(base_fs)), compression_type_(compression_type) {}

Status CompressedFileSystem::OpenFile(const std::string& path, FileOpenFlags flags,
                                    std::unique_ptr<FileHandle>* handle) {
    // For now, delegate to base filesystem
    // In production, this would wrap the handle with compression/decompression
    return base_fs_->OpenFile(path, flags, handle);
}

// Delegate other operations to base filesystem
Status CompressedFileSystem::FileExists(const std::string& path, bool* exists) {
    return base_fs_->FileExists(path, exists);
}

Status CompressedFileSystem::GetFileInfo(const std::string& path, FileInfo* info) {
    return base_fs_->GetFileInfo(path, info);
}

Status CompressedFileSystem::ListFiles(const std::string& directory,
                                     std::vector<FileInfo>* files) {
    return base_fs_->ListFiles(directory, files);
}

Status CompressedFileSystem::CreateDirectory(const std::string& path) {
    return base_fs_->CreateDirectory(path);
}

Status CompressedFileSystem::RemoveFile(const std::string& path) {
    return base_fs_->RemoveFile(path);
}

Status CompressedFileSystem::RemoveDirectory(const std::string& path) {
    return base_fs_->RemoveDirectory(path);
}

Status CompressedFileSystem::NormalizePath(const std::string& path, std::string* normalized) {
    return base_fs_->NormalizePath(path, normalized);
}

Status CompressedFileSystem::JoinPath(const std::string& base, const std::string& path,
                                    std::string* result) {
    return base_fs_->JoinPath(base, path, result);
}

bool CompressedFileSystem::SupportsCompression(FileCompressionType type) const {
    return type == compression_type_ || base_fs_->SupportsCompression(type);
}

Status CompressedFileSystem::SetDefaultCompression(FileCompressionType type) {
    return base_fs_->SetDefaultCompression(type);
}

Status CompressedFileSystem::RegisterSubSystem(const std::string& prefix,
                                             std::unique_ptr<FileSystem> fs) {
    return base_fs_->RegisterSubSystem(prefix, std::move(fs));
}

std::string CompressedFileSystem::GetName() const {
    return "CompressedFileSystem(" + base_fs_->GetName() + ")";
}

// FileOperations implementation
FileOperations::FileOperations(std::shared_ptr<FileSystem> fs) : fs_(std::move(fs)) {}

Status FileOperations::ReadFile(const std::string& path,
                               std::shared_ptr<arrow::Buffer>* buffer) {
    std::unique_ptr<FileHandle> handle;
    auto status = fs_->OpenFile(path, FileOpenFlags::kRead, &handle);
    if (!status.ok()) return status;

    size_t file_size;
    status = handle->GetSize(&file_size);
    if (!status.ok()) return status;

    auto result = arrow::AllocateBuffer(file_size);
    if (!result.ok()) {
        return Status::IOError("Failed to allocate buffer: " + result.status().ToString());
    }
    *buffer = std::move(result).ValueUnsafe();

    size_t bytes_read;
    status = handle->Read((*buffer)->mutable_data(), file_size, &bytes_read);
    if (!status.ok()) return status;

    if (bytes_read != file_size) {
        return Status::IOError("Incomplete read");
    }

    return Status::OK();
}

Status FileOperations::WriteFile(const std::string& path,
                                std::shared_ptr<arrow::Buffer> buffer) {
    std::unique_ptr<FileHandle> handle;
    auto status = fs_->OpenFile(path,
                               static_cast<FileOpenFlags>(
                                   static_cast<int>(FileOpenFlags::kWrite) |
                                   static_cast<int>(FileOpenFlags::kCreate) |
                                   static_cast<int>(FileOpenFlags::kTruncate)),
                               &handle);
    if (!status.ok()) return status;

    size_t bytes_written;
    status = handle->Write(buffer->data(), buffer->size(), &bytes_written);
    if (!status.ok()) return status;

    if (bytes_written != buffer->size()) {
        return Status::IOError("Incomplete write");
    }

    return Status::OK();
}

Status FileOperations::ReadFileChunked(const std::string& path,
                                      std::function<Status(std::shared_ptr<arrow::Buffer>)> callback,
                                      size_t chunk_size) {
    std::unique_ptr<FileHandle> handle;
    auto status = fs_->OpenFile(path, FileOpenFlags::kRead, &handle);
    if (!status.ok()) return status;

    size_t file_size;
    status = handle->GetSize(&file_size);
    if (!status.ok()) return status;

    size_t offset = 0;
    while (offset < file_size) {
        size_t remaining = file_size - offset;
        size_t read_size = std::min(chunk_size, remaining);

        auto result = arrow::AllocateBuffer(read_size);
        if (!result.ok()) {
            return Status::IOError("Failed to allocate chunk buffer: " + result.status().ToString());
        }
        auto buffer = std::move(result).ValueUnsafe();

        size_t bytes_read;
        status = handle->Read(buffer->mutable_data(), read_size, &bytes_read);
        if (!status.ok()) return status;

        if (bytes_read != read_size) {
            return Status::IOError("Incomplete chunk read");
        }

        status = callback(std::move(buffer));
        if (!status.ok()) return status;

        offset += read_size;
    }

    return Status::OK();
}

Status FileOperations::WriteFileChunked(const std::string& path,
                                       std::function<Status(std::shared_ptr<arrow::Buffer>*)> chunk_provider) {
    std::unique_ptr<FileHandle> handle;
    auto status = fs_->OpenFile(path,
                               static_cast<FileOpenFlags>(
                                   static_cast<int>(FileOpenFlags::kWrite) |
                                   static_cast<int>(FileOpenFlags::kCreate) |
                                   static_cast<int>(FileOpenFlags::kTruncate)),
                               &handle);
    if (!status.ok()) return status;

    while (true) {
        std::shared_ptr<arrow::Buffer> buffer;
        status = chunk_provider(&buffer);
        if (!status.ok()) return status;

        if (!buffer) break; // End of chunks

        size_t bytes_written;
        status = handle->Write(buffer->data(), buffer->size(), &bytes_written);
        if (!status.ok()) return status;

        if (bytes_written != buffer->size()) {
            return Status::IOError("Incomplete chunk write");
        }
    }

    return Status::OK();
}

// Memory filesystem implementation
class MemoryFileHandle : public FileHandle {
public:
    MemoryFileHandle(const std::string& path, FileOpenFlags flags,
                    std::unordered_map<std::string, std::vector<char>>* files,
                    std::unordered_map<std::string, uint64_t>* file_sizes)
        : FileHandle(path, flags), files_(files), file_sizes_(file_sizes), pos_(0) {}

    Status Read(void* buffer, size_t nr_bytes, size_t* bytes_read) override {
        if (!(static_cast<int>(flags_) & static_cast<int>(FileOpenFlags::kRead))) {
            return Status::InvalidArgument("File not opened for reading");
        }

        auto it = files_->find(path_);
        if (it == files_->end()) {
            return Status::NotFound("File not found");
        }

        const auto& content = it->second;
        size_t available = content.size() - pos_;
        size_t to_read = std::min(nr_bytes, available);

        if (to_read > 0) {
            std::memcpy(buffer, content.data() + pos_, to_read);
            pos_ += to_read;
        }

        *bytes_read = to_read;
        return Status::OK();
    }

    Status Write(const void* buffer, size_t nr_bytes, size_t* bytes_written) override {
        if (!(static_cast<int>(flags_) & static_cast<int>(FileOpenFlags::kWrite))) {
            return Status::InvalidArgument("File not opened for writing");
        }

        auto& content = (*files_)[path_];

        // Handle append vs overwrite
        if (static_cast<int>(flags_) & static_cast<int>(FileOpenFlags::kAppend)) {
            content.insert(content.end(),
                          static_cast<const char*>(buffer),
                          static_cast<const char*>(buffer) + nr_bytes);
        } else {
            // For simplicity, overwrite from current position
            if (pos_ + nr_bytes > content.size()) {
                content.resize(pos_ + nr_bytes);
            }
            std::memcpy(content.data() + pos_, buffer, nr_bytes);
        }

        pos_ += nr_bytes;
        (*file_sizes_)[path_] = content.size();
        *bytes_written = nr_bytes;
        return Status::OK();
    }

    Status Seek(size_t position) override {
        auto it = files_->find(path_);
        if (it == files_->end()) {
            return Status::NotFound("File not found");
        }

        const auto& content = it->second;
        if (position > content.size()) {
            return Status::InvalidArgument("Seek beyond file size");
        }

        pos_ = position;
        return Status::OK();
    }

    Status GetPosition(size_t* position) const override {
        *position = pos_;
        return Status::OK();
    }

    Status GetSize(size_t* size) const override {
        auto it = file_sizes_->find(path_);
        if (it != file_sizes_->end()) {
            *size = it->second;
        } else {
            *size = 0;
        }
        return Status::OK();
    }

    bool CanSeek() const override {
        return true;
    }

    Status Sync() override {
        // Memory filesystem is always "synced"
        return Status::OK();
    }

    Status Truncate(size_t new_size) override {
        auto& content = (*files_)[path_];
        content.resize(new_size);
        (*file_sizes_)[path_] = new_size;
        if (pos_ > new_size) {
            pos_ = new_size;
        }
        return Status::OK();
    }

    Status MemoryMap(size_t offset, size_t size, void** mapped_memory) override {
        auto it = files_->find(path_);
        if (it == files_->end()) {
            return Status::NotFound("File not found for memory mapping");
        }

        auto& content = it->second;
        if (offset + size > content.size()) {
            return Status::InvalidArgument("Memory mapping request exceeds file size");
        }

        // For memory filesystem, just return a pointer to the content
        // In a real implementation, this might involve more complex memory management
        *mapped_memory = content.data() + offset;
        return Status::OK();
    }

    Status UnmapMemory(void* mapped_memory, size_t size) override {
        // For memory filesystem, unmapping is a no-op since we just return pointers
        // In a real implementation, this would release any pinned memory regions
        return Status::OK();
    }

    bool SupportsMemoryMapping() const override {
        return true;
    }

    Status Close() override {
        // Nothing to do for memory filesystem
        return Status::OK();
    }

private:
    std::unordered_map<std::string, std::vector<char>>* files_;
    std::unordered_map<std::string, uint64_t>* file_sizes_;
    size_t pos_;
};

MemoryFileSystem::MemoryFileSystem()
    : default_compression_(FileCompressionType::kNoCompression) {}

Status MemoryFileSystem::OpenFile(const std::string& path, FileOpenFlags flags,
                                 std::unique_ptr<FileHandle>* handle) {
    // Check for subsystem delegation first
    for (const auto& [prefix, fs] : subsystems_) {
        if (path.find(prefix) == 0) {
            return fs->OpenFile(path, flags, handle);
        }
    }

    *handle = std::make_unique<MemoryFileHandle>(path, flags, &files_, &file_sizes_);
    return Status::OK();
}

Status MemoryFileSystem::FileExists(const std::string& path, bool* exists) {
    // Check subsystems first
    for (const auto& [prefix, fs] : subsystems_) {
        if (path.find(prefix) == 0) {
            return fs->FileExists(path, exists);
        }
    }

    *exists = files_.find(path) != files_.end();
    return Status::OK();
}

Status MemoryFileSystem::GetFileInfo(const std::string& path, FileInfo* info) {
    // Check subsystems first
    for (const auto& [prefix, fs] : subsystems_) {
        if (path.find(prefix) == 0) {
            return fs->GetFileInfo(path, info);
        }
    }

    auto size_it = file_sizes_.find(path);
    if (size_it == file_sizes_.end()) {
        return Status::NotFound("File not found");
    }

    info->path = path;
    info->size = size_it->second;
    info->is_directory = false;  // Memory FS doesn't have directories
    info->modification_time = 0;  // Not tracked in memory FS
    return Status::OK();
}

Status MemoryFileSystem::ListFiles(const std::string& directory,
                                  std::vector<FileInfo>* files) {
    files->clear();

    // Simple implementation - list all files (no directory structure)
    for (const auto& [path, size] : file_sizes_) {
        FileInfo info;
        info.path = path;
        info.size = size;
        info.is_directory = false;
        info.modification_time = 0;
        files->push_back(info);
    }

    return Status::OK();
}

Status MemoryFileSystem::CreateDirectory(const std::string& path) {
    // Memory filesystem doesn't need directories, but we can track them
    directories_.insert(path);
    return Status::OK();
}

Status MemoryFileSystem::RemoveFile(const std::string& path) {
    // Check subsystems first
    for (const auto& [prefix, fs] : subsystems_) {
        if (path.find(prefix) == 0) {
            return fs->RemoveFile(path);
        }
    }

    files_.erase(path);
    file_sizes_.erase(path);
    return Status::OK();
}

Status MemoryFileSystem::RemoveDirectory(const std::string& path) {
    directories_.erase(path);
    return Status::OK();
}

Status MemoryFileSystem::NormalizePath(const std::string& path, std::string* normalized) {
    *normalized = path;  // Simple normalization for memory FS
    return Status::OK();
}

Status MemoryFileSystem::JoinPath(const std::string& base, const std::string& path,
                                 std::string* result) {
    if (base.empty()) {
        *result = path;
    } else if (path.empty()) {
        *result = base;
    } else {
        *result = base + "/" + path;
    }
    return Status::OK();
}

bool MemoryFileSystem::SupportsCompression(FileCompressionType type) const {
    // Memory FS supports basic compression types
    return type == FileCompressionType::kNoCompression ||
           type == FileCompressionType::kLZ4 ||
           type == FileCompressionType::kSnappy;
}

Status MemoryFileSystem::SetDefaultCompression(FileCompressionType type) {
    if (!SupportsCompression(type)) {
        return Status::InvalidArgument("Compression type not supported");
    }
    default_compression_ = type;
    return Status::OK();
}

Status MemoryFileSystem::RegisterSubSystem(const std::string& prefix,
                                          std::unique_ptr<FileSystem> fs) {
    subsystems_[prefix] = std::move(fs);
    return Status::OK();
}

// S3 filesystem implementation
class S3FileHandle : public FileHandle {
public:
    S3FileHandle(const std::string& path, FileOpenFlags flags,
                const std::string& bucket, const std::string& region,
                const std::string& access_key, const std::string& secret_key)
        : FileHandle(path, flags), bucket_(bucket), region_(region),
          access_key_(access_key), secret_key_(secret_key), pos_(0) {}

    Status Read(void* buffer, size_t nr_bytes, size_t* bytes_read) override {
        // Simplified S3 read implementation
        // In a real implementation, this would use AWS SDK or HTTP requests
        *bytes_read = 0;
        return Status::NotImplemented("S3 read not implemented in demo");
    }

    Status Write(const void* buffer, size_t nr_bytes, size_t* bytes_written) override {
        // Simplified S3 write implementation
        *bytes_written = 0;
        return Status::NotImplemented("S3 write not implemented in demo");
    }

    Status Seek(size_t position) override {
        pos_ = position;
        return Status::OK();
    }

    Status GetPosition(size_t* position) const override {
        *position = pos_;
        return Status::OK();
    }

    Status GetSize(size_t* size) const override {
        // In a real implementation, this would query S3 for object metadata
        *size = 0;
        return Status::NotImplemented("S3 GetSize not implemented in demo");
    }

    bool CanSeek() const override {
        return false;  // S3 objects don't support random access in this demo
    }

    Status Sync() override {
        return Status::NotImplemented("S3 sync not implemented in demo");
    }

    Status Truncate(size_t new_size) override {
        return Status::NotImplemented("S3 truncate not implemented in demo");
    }

    Status MemoryMap(size_t offset, size_t size, void** mapped_memory) override {
        // S3 doesn't support direct memory mapping
        // In a real implementation, this might download the range and return a pointer
        return Status::NotImplemented("S3 filesystem does not support memory mapping");
    }

    Status UnmapMemory(void* mapped_memory, size_t size) override {
        return Status::NotImplemented("S3 filesystem does not support memory mapping");
    }

    bool SupportsMemoryMapping() const override {
        return false;
    }

    Status Close() override {
        return Status::OK();
    }

private:
    std::string bucket_;
    std::string region_;
    std::string access_key_;
    std::string secret_key_;
    size_t pos_;
};

S3FileSystem::S3FileSystem(const std::string& bucket, const std::string& region,
                          const std::string& access_key, const std::string& secret_key)
    : bucket_(bucket), region_(region), access_key_(access_key), secret_key_(secret_key),
      default_compression_(FileCompressionType::kNoCompression) {}

Status S3FileSystem::OpenFile(const std::string& path, FileOpenFlags flags,
                             std::unique_ptr<FileHandle>* handle) {
    // Check subsystems first
    for (const auto& [prefix, fs] : subsystems_) {
        if (path.find(prefix) == 0) {
            return fs->OpenFile(path, flags, handle);
        }
    }

    *handle = std::make_unique<S3FileHandle>(path, flags, bucket_, region_,
                                            access_key_, secret_key_);
    return Status::OK();
}

Status S3FileSystem::FileExists(const std::string& path, bool* exists) {
    // Check subsystems first
    for (const auto& [prefix, fs] : subsystems_) {
        if (path.find(prefix) == 0) {
            return fs->FileExists(path, exists);
        }
    }

    // In a real implementation, this would check S3 object existence
    *exists = false;  // Assume not exists for demo
    return Status::OK();
}

Status S3FileSystem::GetFileInfo(const std::string& path, FileInfo* info) {
    // Check subsystems first
    for (const auto& [prefix, fs] : subsystems_) {
        if (path.find(prefix) == 0) {
            return fs->GetFileInfo(path, info);
        }
    }

    // In a real implementation, this would query S3 object metadata
    return Status::NotImplemented("S3 GetFileInfo not implemented in demo");
}

Status S3FileSystem::ListFiles(const std::string& directory,
                              std::vector<FileInfo>* files) {
    files->clear();
    // In a real implementation, this would list S3 objects with prefix
    return Status::NotImplemented("S3 ListFiles not implemented in demo");
}

Status S3FileSystem::CreateDirectory(const std::string& path) {
    // S3 doesn't have directories, but we can ignore this
    return Status::OK();
}

Status S3FileSystem::RemoveFile(const std::string& path) {
    // Check subsystems first
    for (const auto& [prefix, fs] : subsystems_) {
        if (path.find(prefix) == 0) {
            return fs->RemoveFile(path);
        }
    }

    // In a real implementation, this would delete S3 object
    return Status::NotImplemented("S3 RemoveFile not implemented in demo");
}

Status S3FileSystem::RemoveDirectory(const std::string& path) {
    // S3 doesn't have directories
    return Status::OK();
}

Status S3FileSystem::NormalizePath(const std::string& path, std::string* normalized) {
    *normalized = path;  // Simple normalization for S3
    return Status::OK();
}

Status S3FileSystem::JoinPath(const std::string& base, const std::string& path,
                             std::string* result) {
    if (base.empty()) {
        *result = path;
    } else if (path.empty()) {
        *result = base;
    } else {
        *result = base + "/" + path;
    }
    return Status::OK();
}

bool S3FileSystem::SupportsCompression(FileCompressionType type) const {
    // S3 supports all compression types
    return true;
}

Status S3FileSystem::SetDefaultCompression(FileCompressionType type) {
    default_compression_ = type;
    return Status::OK();
}

Status S3FileSystem::RegisterSubSystem(const std::string& prefix,
                                      std::unique_ptr<FileSystem> fs) {
    subsystems_[prefix] = std::move(fs);
    return Status::OK();
}

// Factory function implementations
std::unique_ptr<FileSystem> FileSystem::CreateMemory() {
    return std::unique_ptr<FileSystem>(new MemoryFileSystem());
}

std::unique_ptr<FileSystem> FileSystem::CreateS3(const std::string& bucket,
                                               const std::string& region,
                                               const std::string& access_key,
                                               const std::string& secret_key) {
    return std::unique_ptr<FileSystem>(new S3FileSystem(bucket, region, access_key, secret_key));
}

} // namespace marble
