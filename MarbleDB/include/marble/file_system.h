#pragma once

#include <memory>
#include <string>
#include <vector>
#include <functional>
#include <unordered_map>
#include <unordered_set>
#include <marble/status.h>
#include <arrow/buffer.h>

namespace marble {

// File open flags
enum class FileOpenFlags {
    kRead = 1,
    kWrite = 2,
    kCreate = 4,
    kAppend = 8,
    kTruncate = 16,
    kDirect = 32,  // O_DIRECT on Linux
};

// File compression types
enum class FileCompressionType {
    kNoCompression,
    kGzip,
    kLZ4,
    kZstd,
    kSnappy,
    kBrotli
};

// File information
struct FileInfo {
    std::string path;
    size_t size = 0;
    bool is_directory = false;
    bool is_regular_file = false;
    uint64_t modification_time = 0;
};

// File handle interface - inspired by DuckDB's FileHandle
class FileHandle {
public:
    FileHandle(const std::string& path, FileOpenFlags flags);
    virtual ~FileHandle();

    // Basic I/O operations
    virtual Status Read(void* buffer, size_t nr_bytes, size_t* bytes_read) = 0;
    virtual Status Write(const void* buffer, size_t nr_bytes, size_t* bytes_written) = 0;

    // Seek operations
    virtual Status Seek(size_t position) = 0;
    virtual Status GetPosition(size_t* position) const = 0;

    // File properties
    virtual Status GetSize(size_t* size) const = 0;
    virtual bool CanSeek() const = 0;
    virtual bool IsPipe() const { return false; }

    // Advanced operations
    virtual Status Sync() = 0;
    virtual Status Truncate(size_t new_size) = 0;

    // Memory mapping support
    virtual Status MemoryMap(size_t offset, size_t size, void** mapped_memory) = 0;
    virtual Status UnmapMemory(void* mapped_memory, size_t size) = 0;
    virtual bool SupportsMemoryMapping() const = 0;

    // Close the file
    virtual Status Close() = 0;

    const std::string& GetPath() const { return path_; }
    FileOpenFlags GetFlags() const { return flags_; }

protected:
    std::string path_;
    FileOpenFlags flags_;
};

// Virtual filesystem interface - inspired by DuckDB's FileSystem
class FileSystem {
public:
    virtual ~FileSystem() = default;

    // File operations
    virtual Status OpenFile(const std::string& path, FileOpenFlags flags,
                           std::unique_ptr<FileHandle>* handle) = 0;

    // Utility functions
    virtual Status FileExists(const std::string& path, bool* exists) = 0;
    virtual Status GetFileInfo(const std::string& path, FileInfo* info) = 0;
    virtual Status ListFiles(const std::string& directory,
                            std::vector<FileInfo>* files) = 0;

    // Directory operations
    virtual Status CreateDirectory(const std::string& path) = 0;
    virtual Status RemoveFile(const std::string& path) = 0;
    virtual Status RemoveDirectory(const std::string& path) = 0;

    // Path operations
    virtual Status NormalizePath(const std::string& path, std::string* normalized) = 0;
    virtual Status JoinPath(const std::string& base, const std::string& path,
                           std::string* result) = 0;

    // Compression support
    virtual bool SupportsCompression(FileCompressionType type) const = 0;
    virtual Status SetDefaultCompression(FileCompressionType type) = 0;

    // Subsystem registration (for pluggable backends)
    virtual Status RegisterSubSystem(const std::string& prefix,
                                    std::unique_ptr<FileSystem> fs) = 0;

    // Factory method for local filesystem
    static std::unique_ptr<FileSystem> CreateLocal();

    // Factory methods for other storage backends
    static std::unique_ptr<FileSystem> CreateMemory();
    static std::unique_ptr<FileSystem> CreateS3(const std::string& bucket,
                                               const std::string& region = "us-east-1",
                                               const std::string& access_key = "",
                                               const std::string& secret_key = "");

    // Get filesystem name for diagnostics
    virtual std::string GetName() const = 0;
};

// Local filesystem implementation
class LocalFileSystem : public FileSystem {
public:
    LocalFileSystem();
    ~LocalFileSystem() override = default;

    // FileSystem interface
    Status OpenFile(const std::string& path, FileOpenFlags flags,
                   std::unique_ptr<FileHandle>* handle) override;

    Status FileExists(const std::string& path, bool* exists) override;
    Status GetFileInfo(const std::string& path, FileInfo* info) override;
    Status ListFiles(const std::string& directory,
                    std::vector<FileInfo>* files) override;

    Status CreateDirectory(const std::string& path) override;
    Status RemoveFile(const std::string& path) override;
    Status RemoveDirectory(const std::string& path) override;

    Status NormalizePath(const std::string& path, std::string* normalized) override;
    Status JoinPath(const std::string& base, const std::string& path,
                   std::string* result) override;

    bool SupportsCompression(FileCompressionType type) const override;
    Status SetDefaultCompression(FileCompressionType type) override;

    Status RegisterSubSystem(const std::string& prefix,
                            std::unique_ptr<FileSystem> fs) override;

    std::string GetName() const override { return "LocalFileSystem"; }

private:
    FileCompressionType default_compression_;
    std::unordered_map<std::string, std::unique_ptr<FileSystem>> subsystems_;
};

// Memory filesystem implementation (for testing)
class MemoryFileSystem : public FileSystem {
public:
    MemoryFileSystem();
    ~MemoryFileSystem() override = default;

    // FileSystem interface
    Status OpenFile(const std::string& path, FileOpenFlags flags,
                   std::unique_ptr<FileHandle>* handle) override;

    Status FileExists(const std::string& path, bool* exists) override;
    Status GetFileInfo(const std::string& path, FileInfo* info) override;
    Status ListFiles(const std::string& directory,
                    std::vector<FileInfo>* files) override;

    Status CreateDirectory(const std::string& path) override;
    Status RemoveFile(const std::string& path) override;
    Status RemoveDirectory(const std::string& path) override;

    Status NormalizePath(const std::string& path, std::string* normalized) override;
    Status JoinPath(const std::string& base, const std::string& path,
                   std::string* result) override;

    bool SupportsCompression(FileCompressionType type) const override;
    Status SetDefaultCompression(FileCompressionType type) override;

    Status RegisterSubSystem(const std::string& prefix,
                            std::unique_ptr<FileSystem> fs) override;

    std::string GetName() const override { return "MemoryFileSystem"; }

private:
    FileCompressionType default_compression_;
    std::unordered_map<std::string, std::unique_ptr<FileSystem>> subsystems_;
    std::unordered_map<std::string, std::vector<char>> files_;  // path -> content
    std::unordered_map<std::string, uint64_t> file_sizes_;     // path -> size
    std::unordered_set<std::string> directories_;              // existing directories
};

// S3-compatible filesystem implementation
class S3FileSystem : public FileSystem {
public:
    S3FileSystem(const std::string& bucket, const std::string& region,
                const std::string& access_key, const std::string& secret_key);
    ~S3FileSystem() override = default;

    // FileSystem interface
    Status OpenFile(const std::string& path, FileOpenFlags flags,
                   std::unique_ptr<FileHandle>* handle) override;

    Status FileExists(const std::string& path, bool* exists) override;
    Status GetFileInfo(const std::string& path, FileInfo* info) override;
    Status ListFiles(const std::string& directory,
                    std::vector<FileInfo>* files) override;

    Status CreateDirectory(const std::string& path) override;
    Status RemoveFile(const std::string& path) override;
    Status RemoveDirectory(const std::string& path) override;

    Status NormalizePath(const std::string& path, std::string* normalized) override;
    Status JoinPath(const std::string& base, const std::string& path,
                   std::string* result) override;

    bool SupportsCompression(FileCompressionType type) const override;
    Status SetDefaultCompression(FileCompressionType type) override;

    Status RegisterSubSystem(const std::string& prefix,
                            std::unique_ptr<FileSystem> fs) override;

    std::string GetName() const override { return "S3FileSystem"; }

private:
    std::string bucket_;
    std::string region_;
    std::string access_key_;
    std::string secret_key_;
    FileCompressionType default_compression_;
    std::unordered_map<std::string, std::unique_ptr<FileSystem>> subsystems_;
};

// Compressed filesystem wrapper
class CompressedFileSystem : public FileSystem {
public:
    CompressedFileSystem(std::unique_ptr<FileSystem> base_fs,
                        FileCompressionType compression_type);
    ~CompressedFileSystem() override = default;

    // FileSystem interface - delegates to base_fs with compression
    Status OpenFile(const std::string& path, FileOpenFlags flags,
                   std::unique_ptr<FileHandle>* handle) override;

    // Other methods delegate to base filesystem...
    Status FileExists(const std::string& path, bool* exists) override;
    Status GetFileInfo(const std::string& path, FileInfo* info) override;
    Status ListFiles(const std::string& directory,
                    std::vector<FileInfo>* files) override;
    Status CreateDirectory(const std::string& path) override;
    Status RemoveFile(const std::string& path) override;
    Status RemoveDirectory(const std::string& path) override;
    Status NormalizePath(const std::string& path, std::string* normalized) override;
    Status JoinPath(const std::string& base, const std::string& path,
                   std::string* result) override;

    bool SupportsCompression(FileCompressionType type) const override;
    Status SetDefaultCompression(FileCompressionType type) override;
    Status RegisterSubSystem(const std::string& prefix,
                            std::unique_ptr<FileSystem> fs) override;

    std::string GetName() const override;

private:
    std::unique_ptr<FileSystem> base_fs_;
    FileCompressionType compression_type_;
};

// High-level file operations using Arrow buffers
class FileOperations {
public:
    explicit FileOperations(std::shared_ptr<FileSystem> fs);

    // Read entire file into Arrow buffer
    Status ReadFile(const std::string& path,
                   std::shared_ptr<arrow::Buffer>* buffer);

    // Write Arrow buffer to file
    Status WriteFile(const std::string& path,
                    std::shared_ptr<arrow::Buffer> buffer);

    // Read file in chunks
    Status ReadFileChunked(const std::string& path,
                          std::function<Status(std::shared_ptr<arrow::Buffer>)> callback,
                          size_t chunk_size = 64 * 1024);

    // Write file in chunks
    Status WriteFileChunked(const std::string& path,
                           std::function<Status(std::shared_ptr<arrow::Buffer>*)> chunk_provider);

private:
    std::shared_ptr<FileSystem> fs_;
};

} // namespace marble
