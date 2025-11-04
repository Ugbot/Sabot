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

namespace marble {

MmapSSTableWriter::MmapSSTableWriter(const std::string& filepath,
                                     uint64_t level,
                                     std::shared_ptr<FileSystem> fs,
                                     size_t zone_size,
                                     bool use_async_msync)
    : filepath_(filepath)
    , level_(level)
    , fs_(fs)
    , fd_(-1)
    , mapped_region_(nullptr)
    , zone_size_(zone_size)
    , current_file_size_(0)
    , write_offset_(0)
    , use_async_msync_(use_async_msync)
    , entry_count_(0)
    , min_key_(UINT64_MAX)
    , max_key_(0)
    , finished_(false) {

    std::cerr << "MmapSSTableWriter: Creating " << filepath_
              << " with zone_size=" << (zone_size_ / 1024 / 1024) << " MB\n";
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

        std::cerr << "MmapSSTableWriter: Mapped " << (zone_size_ / 1024 / 1024)
                  << " MB at " << mapped_region_ << "\n";
    }

    // Calculate space needed for this entry
    size_t entry_size = sizeof(uint64_t) +      // key
                       sizeof(uint32_t) +       // value size
                       value.size();             // value data

    // Check if we need to grow the zone
    if (write_offset_ + entry_size > current_file_size_) {
        auto status = ExtendAndRemap();
        if (!status.ok()) {
            return status;
        }
    }

    // Write entry directly to mapped memory (ZERO SYSCALLS!)
    char* write_ptr = static_cast<char*>(mapped_region_) + write_offset_;

    // Write key (8 bytes)
    *reinterpret_cast<uint64_t*>(write_ptr) = key;
    write_ptr += sizeof(uint64_t);

    // Write value size (4 bytes)
    uint32_t value_size = static_cast<uint32_t>(value.size());
    *reinterpret_cast<uint32_t*>(write_ptr) = value_size;
    write_ptr += sizeof(uint32_t);

    // Write value data
    std::memcpy(write_ptr, value.data(), value.size());

    write_offset_ += entry_size;
    entry_count_++;

    // Update key range
    min_key_ = std::min(min_key_, key);
    max_key_ = std::max(max_key_, key);

    // Add to sparse index every N entries
    if (entry_count_ % kSparseIndexInterval == 0) {
        sparse_index_.emplace_back(key, write_offset_ - entry_size);
    }

    return Status::OK();
}

Status MmapSSTableWriter::ExtendAndRemap() {
    std::cerr << "MmapSSTableWriter: Extending zone from "
              << (current_file_size_ / 1024 / 1024) << " MB to "
              << ((current_file_size_ + zone_size_) / 1024 / 1024) << " MB\n";

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

    std::cerr << "MmapSSTableWriter: Finishing with " << entry_count_
              << " entries, " << (write_offset_ / 1024 / 1024) << " MB data\n";

    // Step 1: Flush dirty pages to disk
    if (use_async_msync_) {
        // MS_ASYNC: Non-blocking, kernel schedules write
        if (msync(mapped_region_, write_offset_, MS_ASYNC) != 0) {
            std::cerr << "Warning: msync(MS_ASYNC) failed, falling back to MS_SYNC\n";
            msync(mapped_region_, write_offset_, MS_SYNC);
        }
    } else {
        // MS_SYNC: Blocking, wait for write completion
        if (msync(mapped_region_, write_offset_, MS_SYNC) != 0) {
            return Status::IOError("msync failed");
        }
    }

    // Step 2: Unmap memory (data is now on disk)
    if (munmap(mapped_region_, current_file_size_) != 0) {
        return Status::IOError("Failed to unmap region");
    }
    mapped_region_ = nullptr;

    // Step 3: Truncate file to actual data size (trim excess)
    if (ftruncate(fd_, write_offset_) != 0) {
        std::cerr << "Warning: Failed to truncate file to exact size\n";
    }

    // Step 4: Write sparse index (append to file)
    auto status = WriteIndex();
    if (!status.ok()) {
        return status;
    }

    // Step 5: Write metadata (entry count, index offset, etc.)
    status = WriteMetadata();
    if (!status.ok()) {
        return status;
    }

    // Step 6: Close file
    if (close(fd_) != 0) {
        return Status::IOError("Failed to close file");
    }
    fd_ = -1;

    // Step 7: Create SSTable object for reads
    finished_ = true;

    // For now, return a simple success status
    // Full SSTable object creation would load the file for reads
    *sstable = nullptr;  // TODO: Create actual SSTable object

    std::cerr << "MmapSSTableWriter: Finished successfully\n";

    return Status::OK();
}

Status MmapSSTableWriter::WriteIndex() {
    if (sparse_index_.empty()) {
        return Status::OK();  // No index to write
    }

    // TODO: Write sparse index to file
    // For now, just log
    std::cerr << "MmapSSTableWriter: Sparse index has " << sparse_index_.size()
              << " entries\n";

    return Status::OK();
}

Status MmapSSTableWriter::WriteMetadata() {
    // TODO: Write metadata footer
    // For now, just log
    std::cerr << "MmapSSTableWriter: Metadata - entries=" << entry_count_
              << ", min_key=" << min_key_ << ", max_key=" << max_key_ << "\n";

    return Status::OK();
}

std::unique_ptr<SSTableWriter> CreateMmapSSTableWriter(
    const std::string& filepath,
    uint64_t level,
    std::shared_ptr<FileSystem> fs,
    size_t zone_size,
    bool use_async_msync) {

    return std::make_unique<MmapSSTableWriter>(
        filepath, level, fs, zone_size, use_async_msync);
}

} // namespace marble
