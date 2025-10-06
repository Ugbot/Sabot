#include <marble/marble.h>
#include <iostream>
#include <memory>
#include <vector>

using namespace marble;

int main() {
    std::cout << "MarbleDB Multi-Level Storage Example" << std::endl;
    std::cout << "===================================" << std::endl;

    // Example 1: Memory filesystem for testing
    std::cout << "\n=== Example 1: Memory Filesystem ===" << std::endl;

    auto memory_fs = FileSystem::CreateMemory();
    std::cout << "Created memory filesystem: " << memory_fs->GetName() << std::endl;

    // Test basic file operations
    std::unique_ptr<FileHandle> mem_handle;
    Status status = memory_fs->OpenFile("/test.txt", FileOpenFlags::kWrite, &mem_handle);
    if (status.ok()) {
        std::cout << "Opened file for writing in memory filesystem" << std::endl;

        // Write some data
        const char* test_data = "Hello, Memory Storage!";
        size_t bytes_written;
        status = mem_handle->Write(test_data, strlen(test_data), &bytes_written);
        if (status.ok()) {
            std::cout << "Wrote " << bytes_written << " bytes to memory file" << std::endl;
        }

        // Close the file
        mem_handle->Close();
    }

    // Read the data back
    status = memory_fs->OpenFile("/test.txt", FileOpenFlags::kRead, &mem_handle);
    if (status.ok()) {
        char buffer[100];
        size_t bytes_read;
        status = mem_handle->Read(buffer, sizeof(buffer), &bytes_read);
        if (status.ok()) {
            buffer[bytes_read] = '\0';
            std::cout << "Read from memory file: " << buffer << std::endl;
        }
        mem_handle->Close();
    }

    // Check file existence and info
    bool exists;
    status = memory_fs->FileExists("/test.txt", &exists);
    if (status.ok() && exists) {
        std::cout << "File exists in memory filesystem" << std::endl;

        FileInfo info;
        status = memory_fs->GetFileInfo("/test.txt", &info);
        if (status.ok()) {
            std::cout << "File info: path=" << info.path
                      << ", size=" << info.size
                      << ", is_directory=" << info.is_directory << std::endl;
        }
    }

    // List files
    std::vector<FileInfo> files;
    status = memory_fs->ListFiles("/", &files);
    if (status.ok()) {
        std::cout << "Files in memory filesystem: " << files.size() << std::endl;
        for (const auto& file : files) {
            std::cout << "  - " << file.path << " (" << file.size << " bytes)" << std::endl;
        }
    }

    // Example 2: S3-compatible filesystem (demo)
    std::cout << "\n=== Example 2: S3-Compatible Filesystem ===" << std::endl;

    auto s3_fs = FileSystem::CreateS3("my-test-bucket", "us-west-2");
    std::cout << "Created S3 filesystem: " << s3_fs->GetName() << std::endl;

    // Note: S3 operations are not implemented in demo, but the interface is ready
    std::cout << "S3 filesystem interface is available (implementation would use AWS SDK)" << std::endl;

    // Example 3: Filesystem composition with subsystems
    std::cout << "\n=== Example 3: Filesystem Composition ===" << std::endl;

    // Create a local filesystem and register memory FS as a subsystem
    auto local_fs = FileSystem::CreateLocal();
    std::cout << "Created local filesystem: " << local_fs->GetName() << std::endl;

    // Register memory filesystem under "/memory" prefix
    status = local_fs->RegisterSubSystem("/memory", FileSystem::CreateMemory());
    if (status.ok()) {
        std::cout << "Registered memory filesystem as subsystem under /memory" << std::endl;
    }

    // Test subsystem delegation
    status = local_fs->OpenFile("/memory/subsystem_test.txt", FileOpenFlags::kWrite, &mem_handle);
    if (status.ok()) {
        std::cout << "Successfully opened file in memory subsystem through local FS" << std::endl;
        const char* subsystem_data = "Data in subsystem!";
        size_t bytes_written;
        status = mem_handle->Write(subsystem_data, strlen(subsystem_data), &bytes_written);
        if (status.ok()) {
            std::cout << "Wrote " << bytes_written << " bytes to subsystem file" << std::endl;
        }
        mem_handle->Close();
    }

    // Example 4: Compression support
    std::cout << "\n=== Example 4: Compression Support ===" << std::endl;

    // Test compression support on different filesystems
    std::vector<FileCompressionType> compression_types = {
        FileCompressionType::kNoCompression,
        FileCompressionType::kLZ4,
        FileCompressionType::kSnappy,
        FileCompressionType::kGzip,
        FileCompressionType::kZstd,
        FileCompressionType::kBrotli
    };

    std::cout << "Memory filesystem compression support:" << std::endl;
    for (auto type : compression_types) {
        bool supported = memory_fs->SupportsCompression(type);
        std::cout << "  " << (supported ? "✓" : "✗") << " ";
        switch (type) {
            case FileCompressionType::kNoCompression: std::cout << "No Compression"; break;
            case FileCompressionType::kLZ4: std::cout << "LZ4"; break;
            case FileCompressionType::kSnappy: std::cout << "Snappy"; break;
            case FileCompressionType::kGzip: std::cout << "Gzip"; break;
            case FileCompressionType::kZstd: std::cout << "Zstd"; break;
            case FileCompressionType::kBrotli: std::cout << "Brotli"; break;
        }
        std::cout << std::endl;
    }

    std::cout << "\nS3 filesystem compression support:" << std::endl;
    for (auto type : compression_types) {
        bool supported = s3_fs->SupportsCompression(type);
        std::cout << "  " << (supported ? "✓" : "✗") << " ";
        switch (type) {
            case FileCompressionType::kNoCompression: std::cout << "No Compression"; break;
            case FileCompressionType::kLZ4: std::cout << "LZ4"; break;
            case FileCompressionType::kSnappy: std::cout << "Snappy"; break;
            case FileCompressionType::kGzip: std::cout << "Gzip"; break;
            case FileCompressionType::kZstd: std::cout << "Zstd"; break;
            case FileCompressionType::kBrotli: std::cout << "Brotli"; break;
        }
        std::cout << std::endl;
    }

    // Test setting compression
    status = memory_fs->SetDefaultCompression(FileCompressionType::kLZ4);
    if (status.ok()) {
        std::cout << "Set default compression to LZ4 on memory filesystem" << std::endl;
    }

    // Example 5: File operations with compression
    std::cout << "\n=== Example 5: File Operations with Buffers ===" << std::endl;

    // Create file operations wrapper
    auto file_ops = std::make_shared<FileOperations>(memory_fs);

    // Create a test buffer
    std::string test_content = "This is test content for file operations with potential compression!";
    auto buffer_result = arrow::AllocateBuffer(test_content.size());
    if (buffer_result.ok()) {
        auto buffer = buffer_result.ValueUnsafe();
        std::memcpy(buffer->mutable_data(), test_content.data(), test_content.size());

        // Write buffer to file
        status = file_ops->WriteFile("/buffer_test.txt", buffer);
        if (status.ok()) {
            std::cout << "Wrote buffer to file: " << test_content.size() << " bytes" << std::endl;
        }

        // Read buffer back
        std::shared_ptr<arrow::Buffer> read_buffer;
        status = file_ops->ReadFile("/buffer_test.txt", &read_buffer);
        if (status.ok() && read_buffer) {
            std::string read_content(reinterpret_cast<const char*>(read_buffer->data()),
                                   read_buffer->size());
            std::cout << "Read buffer from file: " << read_content << std::endl;
            std::cout << "Content matches: " << (read_content == test_content ? "YES" : "NO") << std::endl;
        }
    }

    std::cout << "\nMulti-level storage example completed successfully!" << std::endl;
    std::cout << "\nKey features demonstrated:" << std::endl;
    std::cout << "• Memory filesystem for testing" << std::endl;
    std::cout << "• S3-compatible filesystem interface" << std::endl;
    std::cout << "• Filesystem composition with subsystems" << std::endl;
    std::cout << "• Compression type support" << std::endl;
    std::cout << "• File operations with Arrow buffers" << std::endl;
    std::cout << "• Pluggable storage backend architecture" << std::endl;

    return 0;
}
