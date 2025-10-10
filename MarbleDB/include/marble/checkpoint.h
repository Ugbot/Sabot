/************************************************************************
MarbleDB Checkpoint System
Inspired by RocksDB's checkpoint mechanism

Provides consistent snapshots for:
- Backups
- Replication
- Point-in-time recovery
**************************************************************************/

#pragma once

#include <marble/status.h>
#include <memory>
#include <string>
#include <vector>
#include <chrono>

namespace marble {

// Forward declarations
class MarbleDB;
class ColumnFamilyHandle;

/**
 * @brief Checkpoint metadata
 */
struct CheckpointMetadata {
    std::string checkpoint_id;
    std::chrono::system_clock::time_point created_at;
    uint64_t sequence_number;  // Latest committed sequence
    std::vector<std::string> sstable_files;
    std::vector<std::string> wal_files;
    std::vector<std::string> column_families;
    uint64_t total_size_bytes;
    
    /**
     * @brief Serialize metadata to JSON
     */
    std::string ToJson() const;
    
    /**
     * @brief Deserialize from JSON
     */
    static Status FromJson(const std::string& json, CheckpointMetadata* metadata);
};

/**
 * @brief Checkpoint options
 */
struct CheckpointOptions {
    // Whether to flush memtables before checkpoint
    bool flush_memtables = true;
    
    // Whether to create hard links (faster) or copy files
    bool use_hard_links = true;
    
    // Whether to include WAL files
    bool include_wal = true;
    
    // Compression for checkpoint files
    bool compress = false;
    
    CheckpointOptions() = default;
};

/**
 * @brief Checkpoint manager
 * 
 * Creates and manages database checkpoints for backup/recovery.
 */
class Checkpoint {
public:
    /**
     * @brief Create a checkpoint
     * 
     * Creates a consistent snapshot of the database at checkpoint_dir.
     * 
     * Process:
     * 1. Flush memtables (if requested)
     * 2. Get list of all SSTable files
     * 3. Hard link or copy files to checkpoint directory
     * 4. Copy WAL files (if requested)
     * 5. Write checkpoint metadata
     * 
     * @param db Database to checkpoint
     * @param checkpoint_dir Directory for checkpoint files
     * @param options Checkpoint options
     * @return Status OK on success
     */
    static Status Create(
        MarbleDB* db,
        const std::string& checkpoint_dir,
        const CheckpointOptions& options = CheckpointOptions()
    );
    
    /**
     * @brief Restore database from checkpoint
     * 
     * @param checkpoint_dir Checkpoint directory
     * @param db_path Path for restored database
     * @return Status OK on success
     */
    static Status Restore(
        const std::string& checkpoint_dir,
        const std::string& db_path
    );
    
    /**
     * @brief List available checkpoints
     * 
     * @param base_dir Base directory containing checkpoints
     * @return Vector of checkpoint directories
     */
    static std::vector<std::string> List(const std::string& base_dir);
    
    /**
     * @brief Get checkpoint metadata
     * 
     * @param checkpoint_dir Checkpoint directory
     * @param metadata Output: checkpoint metadata
     * @return Status OK if checkpoint exists and valid
     */
    static Status GetMetadata(
        const std::string& checkpoint_dir,
        CheckpointMetadata* metadata
    );
    
    /**
     * @brief Delete a checkpoint
     * 
     * @param checkpoint_dir Checkpoint directory to delete
     * @return Status OK on success
     */
    static Status Delete(const std::string& checkpoint_dir);
    
    /**
     * @brief Verify checkpoint integrity
     * 
     * Checks that all files exist and checksums are valid.
     * 
     * @param checkpoint_dir Checkpoint directory
     * @return Status OK if checkpoint is valid
     */
    static Status Verify(const std::string& checkpoint_dir);

private:
    /**
     * @brief Copy or hard link a file
     */
    static Status CopyFile(
        const std::string& src,
        const std::string& dst,
        bool use_hard_link
    );
    
    /**
     * @brief Get all SSTable files from database
     */
    static Status GetSSTableFiles(
        MarbleDB* db,
        std::vector<std::string>* files
    );
    
    /**
     * @brief Get all WAL files from database
     */
    static Status GetWALFiles(
        MarbleDB* db,
        std::vector<std::string>* files
    );
};

/**
 * @brief Incremental checkpoint manager
 * 
 * Optimizes checkpoints by only copying changed files.
 * Useful for frequent backups.
 */
class IncrementalCheckpoint {
public:
    /**
     * @brief Create incremental checkpoint
     * 
     * Only copies files that changed since last checkpoint.
     * 
     * @param db Database
     * @param checkpoint_dir New checkpoint directory
     * @param previous_checkpoint Previous checkpoint (or empty for full)
     * @return Status OK on success
     */
    static Status Create(
        MarbleDB* db,
        const std::string& checkpoint_dir,
        const std::string& previous_checkpoint = ""
    );
    
    /**
     * @brief Calculate checkpoint size
     * 
     * @param checkpoint_dir Checkpoint directory
     * @return Size in bytes
     */
    static uint64_t GetSize(const std::string& checkpoint_dir);

private:
    /**
     * @brief Find files that changed since previous checkpoint
     */
    static Status GetChangedFiles(
        const std::string& previous_checkpoint,
        const std::vector<std::string>& current_files,
        std::vector<std::string>* changed_files
    );
};

} // namespace marble

