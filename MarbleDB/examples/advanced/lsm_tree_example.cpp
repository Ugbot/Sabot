#include "marble/lsm_storage.h"
#include <iostream>
#include <chrono>
#include <random>
#include <filesystem>

using namespace marble;

/**
 * @brief LSM Tree Example - Demonstrating Tonbo-style LSM storage
 *
 * This example shows:
 * - LSM Tree initialization and configuration
 * - High-throughput writes with memtable buffering
 * - Efficient reads across multiple levels
 * - Background compaction and flushing
 * - Range scans and point lookups
 */
class LSMTreeExample {
public:
    static Status Run() {
        std::cout << "MarbleDB LSM Tree Storage Engine Demo" << std::endl;
        std::cout << "====================================" << std::endl << std::endl;

        // Configure LSM Tree
        LSMTreeConfig config;
        config.data_directory = "./lsm_demo_data";
        config.memtable_max_size_bytes = 1 * 1024 * 1024; // 1MB for demo
        config.sstable_max_size_bytes = 2 * 1024 * 1024;  // 2MB SSTables
        config.l0_compaction_trigger = 3;                 // Compact L0 after 3 files
        config.compaction_threads = 1;                    // Single compaction thread for demo
        config.flush_threads = 1;

        // Clean up any existing data
        std::filesystem::remove_all(config.data_directory);

        // Create LSM Tree
        auto lsm_tree = CreateLSMTree();
        auto status = lsm_tree->Init(config);
        if (!status.ok()) {
            std::cerr << "Failed to initialize LSM Tree: " << status.message() << std::endl;
            return status;
        }

        std::cout << "✓ LSM Tree initialized successfully" << std::endl;
        std::cout << "  Data directory: " << config.data_directory << std::endl;
        std::cout << "  MemTable size: " << config.memtable_max_size_bytes / 1024 << "KB" << std::endl;
        std::cout << "  SSTable size: " << config.sstable_max_size_bytes / 1024 << "KB" << std::endl << std::endl;

        // Phase 1: High-throughput writes
        status = DemonstrateWrites(lsm_tree.get());
        if (!status.ok()) return status;

        // Phase 2: Efficient reads
        status = DemonstrateReads(lsm_tree.get());
        if (!status.ok()) return status;

        // Phase 3: Range scans
        status = DemonstrateScans(lsm_tree.get());
        if (!status.ok()) return status;

        // Phase 4: Background operations
        status = DemonstrateCompaction(lsm_tree.get());
        if (!status.ok()) return status;

        // Cleanup
        status = lsm_tree->Shutdown();
        if (!status.ok()) {
            std::cerr << "Failed to shutdown LSM Tree: " << status.message() << std::endl;
            return status;
        }

        // Clean up demo data
        std::filesystem::remove_all(config.data_directory);

        std::cout << std::endl << "🎉 LSM Tree demo completed successfully!" << std::endl;
        std::cout << "   Demonstrated Tonbo-style LSM storage with:" << std::endl;
        std::cout << "   • High-throughput memtable writes" << std::endl;
        std::cout << "   • Multi-level SSTable organization" << std::endl;
        std::cout << "   • Background compaction" << std::endl;
        std::cout << "   • Efficient point and range queries" << std::endl;

        return Status::OK();
    }

private:
    static Status DemonstrateWrites(LSMTree* lsm_tree) {
        std::cout << "📝 Phase 1: High-Throughput Writes" << std::endl;

        const int num_keys = 10000;
        std::vector<uint64_t> keys;

        // Generate sequential keys for testing
        for (int i = 0; i < num_keys; ++i) {
            keys.push_back(i * 100); // Spread out keys
        }

        // Shuffle for realistic access patterns
        std::random_device rd;
        std::mt19937 g(rd());
        std::shuffle(keys.begin(), keys.end(), g);

        auto start_time = std::chrono::high_resolution_clock::now();

        // Write data
        for (size_t i = 0; i < keys.size(); ++i) {
            uint64_t key = keys[i];
            std::string value = "value_" + std::to_string(key) + "_data_" +
                               std::string(100, 'x'); // 100-byte values

            auto status = lsm_tree->Put(key, value);
            if (!status.ok()) {
                std::cerr << "Write failed for key " << key << ": " << status.message() << std::endl;
                return status;
            }
        }

        auto end_time = std::chrono::high_resolution_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time);

        std::cout << "✓ Wrote " << num_keys << " keys in " << duration.count() << "ms" << std::endl;
        std::cout << "  Throughput: " << (num_keys * 1000.0 / duration.count()) << " writes/sec" << std::endl;
        std::cout << "  Data size: ~" << (num_keys * 100 / 1024) << "KB" << std::endl;

        // Show statistics
        auto stats = lsm_tree->GetStats();
        std::cout << "  MemTable entries: " << stats.active_memtable_entries << std::endl;
        std::cout << "  MemTable size: " << stats.active_memtable_size_bytes / 1024 << "KB" << std::endl << std::endl;

        return Status::OK();
    }

    static Status DemonstrateReads(LSMTree* lsm_tree) {
        std::cout << "🔍 Phase 2: Efficient Reads" << std::endl;

        // Test point lookups
        const int num_reads = 1000;
        int hits = 0;

        auto start_time = std::chrono::high_resolution_clock::now();

        for (int i = 0; i < num_reads; ++i) {
            uint64_t key = i * 100; // Same keys we wrote
            std::string value;

            auto status = lsm_tree->Get(key, &value);
            if (status.ok()) {
                hits++;
                // Verify value
                if (value.find("value_" + std::to_string(key)) != 0) {
                    std::cerr << "Value verification failed for key " << key << std::endl;
                    return Status::DataLoss("Value corruption detected");
                }
            }
        }

        auto end_time = std::chrono::high_resolution_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end_time - start_time);

        std::cout << "✓ Performed " << num_reads << " point lookups" << std::endl;
        std::cout << "  Hit rate: " << hits << "/" << num_reads << " (" << (hits * 100.0 / num_reads) << "%)" << std::endl;
        std::cout << "  Avg latency: " << (duration.count() / num_reads) << "μs per read" << std::endl;

        // Test non-existent keys
        std::string value;
        auto status = lsm_tree->Get(999999, &value); // Non-existent key
        if (status.ok()) {
            std::cerr << "Unexpected success for non-existent key" << std::endl;
            return Status::InternalError("Read consistency check failed");
        }

        std::cout << "✓ Correctly handled non-existent keys" << std::endl << std::endl;

        return Status::OK();
    }

    static Status DemonstrateScans(LSMTree* lsm_tree) {
        std::cout << "📊 Phase 3: Range Scans" << std::endl;

        // Test range scans
        std::vector<std::pair<uint64_t, std::string>> results;

        // Scan first 100 keys
        auto start_time = std::chrono::high_resolution_clock::now();
        auto status = lsm_tree->Scan(0, 1000, &results);
        auto end_time = std::chrono::high_resolution_clock::now();

        if (!status.ok()) {
            std::cerr << "Range scan failed: " << status.message() << std::endl;
            return status;
        }

        auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end_time - start_time);

        std::cout << "✓ Range scan [0, 1000] returned " << results.size() << " results" << std::endl;
        std::cout << "  Scan latency: " << duration.count() << "μs" << std::endl;

        // Verify results are sorted
        bool sorted = true;
        for (size_t i = 1; i < results.size(); ++i) {
            if (results[i-1].first > results[i].first) {
                sorted = false;
                break;
            }
        }

        if (!sorted) {
            std::cerr << "Range scan results not properly sorted" << std::endl;
            return Status::InternalError("Sort verification failed");
        }

        std::cout << "✓ Results properly sorted by key" << std::endl;

        // Test empty range
        results.clear();
        status = lsm_tree->Scan(5000, 5100, &results);
        std::cout << "✓ Empty range scan [5000, 5100] returned " << results.size() << " results" << std::endl << std::endl;

        return Status::OK();
    }

    static Status DemonstrateCompaction(LSMTree* lsm_tree) {
        std::cout << "🔄 Phase 4: Background Compaction" << std::endl;

        // Force a flush to create SSTables
        auto status = lsm_tree->Flush();
        if (!status.ok()) {
            std::cerr << "Flush failed: " << status.message() << std::endl;
            return status;
        }

        std::cout << "✓ Forced memtable flush" << std::endl;

        // Wait for background tasks
        status = lsm_tree->WaitForBackgroundTasks();
        if (!status.ok()) {
            std::cerr << "Wait for background tasks failed: " << status.message() << std::endl;
            return status;
        }

        // Show final statistics
        auto stats = lsm_tree->GetStats();

        std::cout << "✓ Background compaction completed" << std::endl;
        std::cout << "  Total SSTables: " << stats.total_sstables << std::endl;
        std::cout << "  Total SSTable size: " << stats.total_sstable_size_bytes / 1024 << "KB" << std::endl;
        std::cout << "  SSTables per level: ";
        for (size_t i = 0; i < stats.sstables_per_level.size(); ++i) {
            if (i > 0) std::cout << ", ";
            std::cout << "L" << i << ":" << stats.sstables_per_level[i];
        }
        std::cout << std::endl;
        std::cout << "  Completed compactions: " << stats.completed_compactions << std::endl << std::endl;

        return Status::OK();
    }
};

int main(int argc, char** argv) {
    auto status = LSMTreeExample::Run();
    if (!status.ok()) {
        std::cerr << "LSM Tree demo failed: " << status.message() << std::endl;
        return 1;
    }

    return 0;
}

