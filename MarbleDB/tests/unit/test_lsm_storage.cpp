/**
 * LSM Tree Storage Unit Tests
 *
 * Tests for the core LSM tree implementation including:
 * - Put/Get/Delete operations
 * - MemTable flushing and SSTable creation
 * - Compaction logic
 * - WAL integration
 */

#include "marble/lsm_storage.h"
#include "marble/memtable.h"
#include "marble/sstable.h"
#include <gtest/gtest.h>
#include <filesystem>
#include <thread>
#include <chrono>

namespace fs = std::filesystem;

class LSMStorageTest : public ::testing::Test {
protected:
    void SetUp() override {
        test_path_ = "/tmp/lsm_test_" + std::to_string(std::time(nullptr));
        fs::create_directories(test_path_);
    }

    void TearDown() override {
        // Clean up test directory
        fs::remove_all(test_path_);
    }

    std::string test_path_;
};

// Test basic LSM tree initialization
TEST_F(LSMStorageTest, Initialization) {
    LSMTreeConfig config;
    config.data_directory = test_path_ + "/data";
    config.wal_directory = test_path_ + "/wal";
    config.memtable_max_size_bytes = 1024 * 1024; // 1MB for testing

    std::unique_ptr<LSMTree> lsm_tree;
    auto status = CreateStandardLSMTree(config, &lsm_tree);
    ASSERT_TRUE(status.ok());
    ASSERT_TRUE(lsm_tree != nullptr);

    // Test basic operations
    status = lsm_tree->Put(100ULL, "test_value_100");
    ASSERT_TRUE(status.ok());

    std::string value;
    status = lsm_tree->Get(100ULL, &value);
    ASSERT_TRUE(status.ok());
    ASSERT_EQ(value, "test_value_100");

    // Cleanup
    lsm_tree->Shutdown();
}

// Test MemTable flush to SSTable
TEST_F(LSMStorageTest, MemTableFlush) {
    LSMTreeConfig config;
    config.data_directory = test_path_ + "/data";
    config.wal_directory = test_path_ + "/wal";
    config.memtable_max_size_bytes = 1024; // Small size to trigger flush

    std::unique_ptr<LSMTree> lsm_tree;
    auto status = CreateStandardLSMTree(config, &lsm_tree);
    ASSERT_TRUE(status.ok());

    // Fill memtable to trigger flush
    for (int i = 0; i < 100; ++i) {
        std::string value = "value_" + std::to_string(i);
        status = lsm_tree->Put(static_cast<uint64_t>(i), value);
        ASSERT_TRUE(status.ok());
    }

    // Force flush
    status = lsm_tree->Flush();
    ASSERT_TRUE(status.ok());

    // Give background threads time to complete
    std::this_thread::sleep_for(std::chrono::milliseconds(200));

    // Verify all data can be read back
    for (int i = 0; i < 100; ++i) {
        std::string value;
        status = lsm_tree->Get(static_cast<uint64_t>(i), &value);
        ASSERT_TRUE(status.ok());
        ASSERT_EQ(value, "value_" + std::to_string(i));
    }

    // Cleanup
    lsm_tree->Shutdown();
}

// Test compaction logic
TEST_F(LSMStorageTest, Compaction) {
    LSMTreeConfig config;
    config.data_directory = test_path_ + "/data";
    config.wal_directory = test_path_ + "/wal";
    config.memtable_max_size_bytes = 1024;
    config.l0_compaction_trigger = 2; // Trigger compaction with 2 SSTables

    std::unique_ptr<LSMTree> lsm_tree;
    auto status = CreateStandardLSMTree(config, &lsm_tree);
    ASSERT_TRUE(status.ok());

    // Create multiple SSTables to trigger compaction
    for (int batch = 0; batch < 3; ++batch) {
        for (int i = 0; i < 50; ++i) {
            uint64_t key = batch * 100 + i;
            std::string value = "batch_" + std::to_string(batch) + "_value_" + std::to_string(i);
            status = lsm_tree->Put(key, value);
            ASSERT_TRUE(status.ok());
        }

        // Force flush
        status = lsm_tree->Flush();
        ASSERT_TRUE(status.ok());
    }

    // Wait for compaction
    std::this_thread::sleep_for(std::chrono::milliseconds(500));

    // Verify all data is still accessible
    for (int batch = 0; batch < 3; ++batch) {
        for (int i = 0; i < 50; ++i) {
            uint64_t key = batch * 100 + i;
            std::string value;
            status = lsm_tree->Get(key, &value);
            ASSERT_TRUE(status.ok());
            ASSERT_EQ(value, "batch_" + std::to_string(batch) + "_value_" + std::to_string(i));
        }
    }

    // Cleanup
    lsm_tree->Shutdown();
}

// Test WAL recovery
TEST_F(LSMStorageTest, WALRecovery) {
    std::string db_path = test_path_ + "/wal_recovery";

    // Phase 1: Create and populate database
    {
        LSMTreeConfig config;
        config.data_directory = db_path + "/data";
        config.wal_directory = db_path + "/wal";
        config.memtable_max_size_bytes = 1024;

        std::unique_ptr<LSMTree> lsm_tree;
        auto status = CreateStandardLSMTree(config, &lsm_tree);
        ASSERT_TRUE(status.ok());

        // Add data
        for (int i = 0; i < 20; ++i) {
            status = lsm_tree->Put(static_cast<uint64_t>(i),
                                  "recovery_value_" + std::to_string(i));
            ASSERT_TRUE(status.ok());
        }

        // Shutdown (should persist WAL)
        lsm_tree->Shutdown();
    }

    // Phase 2: Reopen and verify recovery
    {
        LSMTreeConfig config;
        config.data_directory = db_path + "/data";
        config.wal_directory = db_path + "/wal";

        std::unique_ptr<LSMTree> lsm_tree;
        auto status = CreateStandardLSMTree(config, &lsm_tree);
        ASSERT_TRUE(status.ok());

        // Verify recovered data
        for (int i = 0; i < 20; ++i) {
            std::string value;
            status = lsm_tree->Get(static_cast<uint64_t>(i), &value);
            ASSERT_TRUE(status.ok());
            ASSERT_EQ(value, "recovery_value_" + std::to_string(i));
        }

        lsm_tree->Shutdown();
    }
}

// Test concurrent operations
TEST_F(LSMStorageTest, ConcurrentOperations) {
    LSMTreeConfig config;
    config.data_directory = test_path_ + "/data";
    config.wal_directory = test_path_ + "/wal";
    config.memtable_max_size_bytes = 64 * 1024; // 64KB

    std::unique_ptr<LSMTree> lsm_tree;
    auto status = CreateStandardLSMTree(config, &lsm_tree);
    ASSERT_TRUE(status.ok());

    // Run concurrent writers
    std::vector<std::thread> writers;
    for (int t = 0; t < 4; ++t) {
        writers.emplace_back([&, t]() {
            for (int i = 0; i < 100; ++i) {
                uint64_t key = t * 1000 + i;
                std::string value = "thread_" + std::to_string(t) + "_value_" + std::to_string(i);
                lsm_tree->Put(key, value);
            }
        });
    }

    // Wait for writers
    for (auto& thread : writers) {
        thread.join();
    }

    // Force flush
    lsm_tree->Flush();

    // Wait for background operations
    std::this_thread::sleep_for(std::chrono::milliseconds(300));

    // Verify all data
    for (int t = 0; t < 4; ++t) {
        for (int i = 0; i < 100; ++i) {
            uint64_t key = t * 1000 + i;
            std::string value;
            status = lsm_tree->Get(key, &value);
            ASSERT_TRUE(status.ok());
            ASSERT_EQ(value, "thread_" + std::to_string(t) + "_value_" + std::to_string(i));
        }
    }

    lsm_tree->Shutdown();
}

// Test delete operations
TEST_F(LSMStorageTest, DeleteOperations) {
    LSMTreeConfig config;
    config.data_directory = test_path_ + "/data";
    config.wal_directory = test_path_ + "/wal";

    std::unique_ptr<LSMTree> lsm_tree;
    auto status = CreateStandardLSMTree(config, &lsm_tree);
    ASSERT_TRUE(status.ok());

    // Put some data
    for (int i = 0; i < 10; ++i) {
        status = lsm_tree->Put(static_cast<uint64_t>(i),
                              "value_" + std::to_string(i));
        ASSERT_TRUE(status.ok());
    }

    // Delete some records
    status = lsm_tree->Delete(3ULL);
    ASSERT_TRUE(status.ok());

    status = lsm_tree->Delete(7ULL);
    ASSERT_TRUE(status.ok());

    // Flush to persist deletes
    lsm_tree->Flush();
    std::this_thread::sleep_for(std::chrono::milliseconds(200));

    // Verify existing records
    for (int i = 0; i < 10; ++i) {
        if (i == 3 || i == 7) {
            // Should be deleted
            std::string value;
            status = lsm_tree->Get(static_cast<uint64_t>(i), &value);
            ASSERT_TRUE(status.IsNotFound());
        } else {
            // Should exist
            std::string value;
            status = lsm_tree->Get(static_cast<uint64_t>(i), &value);
            ASSERT_TRUE(status.ok());
            ASSERT_EQ(value, "value_" + std::to_string(i));
        }
    }

    lsm_tree->Shutdown();
}

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
