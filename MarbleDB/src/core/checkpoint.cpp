/************************************************************************
MarbleDB Checkpoint Implementation
**************************************************************************/

#include "marble/checkpoint.h"
#include "marble/db.h"
#include <filesystem>
#include <fstream>
#include <sstream>
#include <iomanip>

namespace fs = std::filesystem;

namespace marble {

// CheckpointMetadata implementation
std::string CheckpointMetadata::ToJson() const {
    std::ostringstream oss;
    oss << "{\n";
    oss << "  \"checkpoint_id\": \"" << checkpoint_id << "\",\n";
    
    // Format timestamp
    auto time_t_val = std::chrono::system_clock::to_time_t(created_at);
    oss << "  \"created_at\": \"" << std::put_time(std::localtime(&time_t_val), "%Y-%m-%d %H:%M:%S") << "\",\n";
    
    oss << "  \"sequence_number\": " << sequence_number << ",\n";
    oss << "  \"total_size_bytes\": " << total_size_bytes << ",\n";
    
    // SSTable files
    oss << "  \"sstable_files\": [";
    for (size_t i = 0; i < sstable_files.size(); ++i) {
        if (i > 0) oss << ", ";
        oss << "\"" << sstable_files[i] << "\"";
    }
    oss << "],\n";
    
    // WAL files
    oss << "  \"wal_files\": [";
    for (size_t i = 0; i < wal_files.size(); ++i) {
        if (i > 0) oss << ", ";
        oss << "\"" << wal_files[i] << "\"";
    }
    oss << "],\n";
    
    // Column families
    oss << "  \"column_families\": [";
    for (size_t i = 0; i < column_families.size(); ++i) {
        if (i > 0) oss << ", ";
        oss << "\"" << column_families[i] << "\"";
    }
    oss << "]\n";
    
    oss << "}\n";
    return oss.str();
}

Status CheckpointMetadata::FromJson(const std::string& json, CheckpointMetadata* metadata) {
    // Simple JSON parsing (in production, use nlohmann/json or similar)
    // For now, return NotImplemented
    return Status::NotImplemented("JSON parsing not implemented");
}

// Checkpoint implementation
Status Checkpoint::Create(
    MarbleDB* db,
    const std::string& checkpoint_dir,
    const CheckpointOptions& options) {
    
    if (!db) {
        return Status::InvalidArgument("Database is null");
    }
    
    // Create checkpoint directory
    try {
        fs::create_directories(checkpoint_dir);
    } catch (const fs::filesystem_error& e) {
        return Status::IOError("Failed to create checkpoint directory: " + std::string(e.what()));
    }
    
    CheckpointMetadata metadata;
    metadata.checkpoint_id = fs::path(checkpoint_dir).filename().string();
    metadata.created_at = std::chrono::system_clock::now();
    metadata.sequence_number = 0;  // TODO: Get from DB
    metadata.total_size_bytes = 0;
    
    // Step 1: Flush memtables if requested
    if (options.flush_memtables) {
        auto status = db->Flush();
        if (!status.ok()) {
            return status;
        }
    }
    
    // Step 2: Get all SSTable files
    std::vector<std::string> sstable_files;
    auto status = GetSSTableFiles(db, &sstable_files);
    if (!status.ok()) {
        return status;
    }
    
    metadata.sstable_files = sstable_files;
    
    // Step 3: Copy/link SSTable files
    for (const auto& file : sstable_files) {
        std::string dest = checkpoint_dir + "/" + fs::path(file).filename().string();
        status = CopyFile(file, dest, options.use_hard_links);
        if (!status.ok()) {
            return status;
        }
        
        // Track size
        try {
            metadata.total_size_bytes += fs::file_size(file);
        } catch (...) {
            // Ignore size errors
        }
    }
    
    // Step 4: Copy WAL files if requested
    if (options.include_wal) {
        std::vector<std::string> wal_files;
        status = GetWALFiles(db, &wal_files);
        if (!status.ok()) {
            return status;
        }
        
        metadata.wal_files = wal_files;
        
        for (const auto& file : wal_files) {
            std::string dest = checkpoint_dir + "/" + fs::path(file).filename().string();
            status = CopyFile(file, dest, false);  // Always copy WAL (can't hard link active files)
            if (!status.ok()) {
                return status;
            }
        }
    }
    
    // Step 5: Get column family names
    metadata.column_families = db->ListColumnFamilies();
    
    // Step 6: Write metadata
    std::string metadata_path = checkpoint_dir + "/CHECKPOINT_METADATA.json";
    std::ofstream metadata_file(metadata_path);
    if (!metadata_file) {
        return Status::IOError("Failed to write checkpoint metadata");
    }
    
    metadata_file << metadata.ToJson();
    metadata_file.close();
    
    return Status::OK();
}

Status Checkpoint::Restore(
    const std::string& checkpoint_dir,
    const std::string& db_path) {
    
    // Verify checkpoint exists
    if (!fs::exists(checkpoint_dir)) {
        return Status::NotFound("Checkpoint directory not found: " + checkpoint_dir);
    }
    
    // Read metadata
    CheckpointMetadata metadata;
    auto status = GetMetadata(checkpoint_dir, &metadata);
    if (!status.ok()) {
        return status;
    }
    
    // Create database directory
    try {
        fs::create_directories(db_path);
    } catch (const fs::filesystem_error& e) {
        return Status::IOError("Failed to create database directory: " + std::string(e.what()));
    }
    
    // Copy all checkpoint files to new database location
    for (const auto& entry : fs::directory_iterator(checkpoint_dir)) {
        if (entry.path().filename() == "CHECKPOINT_METADATA.json") {
            continue;  // Skip metadata file
        }
        
        std::string dest = db_path + "/" + entry.path().filename().string();
        fs::copy(entry.path(), dest, fs::copy_options::overwrite_existing);
    }
    
    return Status::OK();
}

std::vector<std::string> Checkpoint::List(const std::string& base_dir) {
    std::vector<std::string> checkpoints;
    
    if (!fs::exists(base_dir)) {
        return checkpoints;
    }
    
    for (const auto& entry : fs::directory_iterator(base_dir)) {
        if (entry.is_directory()) {
            // Check if it has CHECKPOINT_METADATA.json
            std::string metadata_path = entry.path().string() + "/CHECKPOINT_METADATA.json";
            if (fs::exists(metadata_path)) {
                checkpoints.push_back(entry.path().string());
            }
        }
    }
    
    return checkpoints;
}

Status Checkpoint::GetMetadata(
    const std::string& checkpoint_dir,
    CheckpointMetadata* metadata) {
    
    std::string metadata_path = checkpoint_dir + "/CHECKPOINT_METADATA.json";
    
    if (!fs::exists(metadata_path)) {
        return Status::NotFound("Checkpoint metadata not found");
    }
    
    std::ifstream file(metadata_path);
    if (!file) {
        return Status::IOError("Failed to read checkpoint metadata");
    }
    
    std::stringstream buffer;
    buffer << file.rdbuf();
    
    return CheckpointMetadata::FromJson(buffer.str(), metadata);
}

Status Checkpoint::Delete(const std::string& checkpoint_dir) {
    if (!fs::exists(checkpoint_dir)) {
        return Status::NotFound("Checkpoint not found: " + checkpoint_dir);
    }
    
    try {
        fs::remove_all(checkpoint_dir);
    } catch (const fs::filesystem_error& e) {
        return Status::IOError("Failed to delete checkpoint: " + std::string(e.what()));
    }
    
    return Status::OK();
}

Status Checkpoint::Verify(const std::string& checkpoint_dir) {
    // Read metadata
    CheckpointMetadata metadata;
    auto status = GetMetadata(checkpoint_dir, &metadata);
    if (!status.ok()) {
        return status;
    }
    
    // Verify all SSTable files exist
    for (const auto& file : metadata.sstable_files) {
        std::string file_path = checkpoint_dir + "/" + fs::path(file).filename().string();
        if (!fs::exists(file_path)) {
            return Status::Corruption("Missing SSTable file: " + file);
        }
    }
    
    // Verify WAL files
    for (const auto& file : metadata.wal_files) {
        std::string file_path = checkpoint_dir + "/" + fs::path(file).filename().string();
        if (!fs::exists(file_path)) {
            return Status::Corruption("Missing WAL file: " + file);
        }
    }
    
    return Status::OK();
}

Status Checkpoint::CopyFile(
    const std::string& src,
    const std::string& dst,
    bool use_hard_link) {
    
    try {
        if (use_hard_link) {
            fs::create_hard_link(src, dst);
        } else {
            fs::copy(src, dst, fs::copy_options::overwrite_existing);
        }
    } catch (const fs::filesystem_error& e) {
        return Status::IOError("Failed to copy file: " + std::string(e.what()));
    }
    
    return Status::OK();
}

Status Checkpoint::GetSSTableFiles(
    MarbleDB* db,
    std::vector<std::string>* files) {
    
    // TODO: Get list of SSTable files from DB
    // For now, return empty list
    files->clear();
    
    return Status::OK();
}

Status Checkpoint::GetWALFiles(
    MarbleDB* db,
    std::vector<std::string>* files) {
    
    // TODO: Get list of WAL files from DB
    // For now, return empty list
    files->clear();
    
    return Status::OK();
}

// IncrementalCheckpoint implementation
Status IncrementalCheckpoint::Create(
    MarbleDB* db,
    const std::string& checkpoint_dir,
    const std::string& previous_checkpoint) {
    
    if (previous_checkpoint.empty()) {
        // No previous checkpoint - create full checkpoint
        return Checkpoint::Create(db, checkpoint_dir);
    }
    
    // Get current files
    std::vector<std::string> current_files;
    auto status = Checkpoint::GetSSTableFiles(db, &current_files);
    if (!status.ok()) {
        return status;
    }
    
    // Find changed files
    std::vector<std::string> changed_files;
    status = GetChangedFiles(previous_checkpoint, current_files, &changed_files);
    if (!status.ok()) {
        return status;
    }
    
    // Create directory
    try {
        fs::create_directories(checkpoint_dir);
    } catch (const fs::filesystem_error& e) {
        return Status::IOError(e.what());
    }
    
    // Copy only changed files
    for (const auto& file : changed_files) {
        std::string dest = checkpoint_dir + "/" + fs::path(file).filename().string();
        status = Checkpoint::CopyFile(file, dest, true);
        if (!status.ok()) {
            return status;
        }
    }
    
    // Write metadata
    CheckpointMetadata metadata;
    metadata.checkpoint_id = fs::path(checkpoint_dir).filename().string();
    metadata.created_at = std::chrono::system_clock::now();
    metadata.sstable_files = changed_files;
    
    std::ofstream metadata_file(checkpoint_dir + "/CHECKPOINT_METADATA.json");
    metadata_file << metadata.ToJson();
    
    return Status::OK();
}

uint64_t IncrementalCheckpoint::GetSize(const std::string& checkpoint_dir) {
    uint64_t total_size = 0;
    
    try {
        for (const auto& entry : fs::recursive_directory_iterator(checkpoint_dir)) {
            if (entry.is_regular_file()) {
                total_size += entry.file_size();
            }
        }
    } catch (...) {
        return 0;
    }
    
    return total_size;
}

Status IncrementalCheckpoint::GetChangedFiles(
    const std::string& previous_checkpoint,
    const std::vector<std::string>& current_files,
    std::vector<std::string>* changed_files) {
    
    // Read previous checkpoint metadata
    CheckpointMetadata prev_metadata;
    auto status = Checkpoint::GetMetadata(previous_checkpoint, &prev_metadata);
    if (!status.ok()) {
        // No previous metadata - all files are "changed"
        *changed_files = current_files;
        return Status::OK();
    }
    
    // Find files not in previous checkpoint
    std::set<std::string> prev_files(prev_metadata.sstable_files.begin(), 
                                     prev_metadata.sstable_files.end());
    
    for (const auto& file : current_files) {
        std::string filename = fs::path(file).filename().string();
        if (prev_files.find(filename) == prev_files.end()) {
            changed_files->push_back(file);
        }
    }
    
    return Status::OK();
}

} // namespace marble

