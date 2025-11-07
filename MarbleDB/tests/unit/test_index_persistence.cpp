/**
 * Test for Index Persistence
 *
 * Verifies that optimization indexes (bloom filters, skipping indexes)
 * can be persisted to disk and loaded on restart.
 */

#include <gtest/gtest.h>
#include <marble/index_persistence.h>
#include <marble/status.h>
#include <filesystem>
#include <fstream>

namespace fs = std::filesystem;

namespace marble {
namespace {

class IndexPersistenceTest : public ::testing::Test {
protected:
    void SetUp() override {
        test_dir_ = "/tmp/marble_test_index_persistence_" + std::to_string(getpid());
        // Clean up any existing test directory
        if (fs::exists(test_dir_)) {
            fs::remove_all(test_dir_);
        }
        fs::create_directories(test_dir_);
    }

    void TearDown() override {
        // Clean up test directory
        if (fs::exists(test_dir_)) {
            fs::remove_all(test_dir_);
        }
    }

    std::string test_dir_;
};

TEST_F(IndexPersistenceTest, InitializeCreatesDirectories) {
    IndexPersistenceManager manager(test_dir_);
    auto status = manager.Initialize();
    ASSERT_TRUE(status.ok()) << status.ToString();

    // Check that indexes directory was created
    std::string indexes_path = test_dir_ + "/indexes";
    ASSERT_TRUE(fs::exists(indexes_path));
    ASSERT_TRUE(fs::is_directory(indexes_path));
}

TEST_F(IndexPersistenceTest, RegisterColumnFamily) {
    IndexPersistenceManager manager(test_dir_);
    ASSERT_TRUE(manager.Initialize().ok());

    auto status = manager.RegisterColumnFamily(1, "test_cf");
    ASSERT_TRUE(status.ok()) << status.ToString();

    // Save manifest to disk
    status = manager.SaveManifest();
    ASSERT_TRUE(status.ok()) << status.ToString();

    // Check manifest file exists
    std::string manifest_path = test_dir_ + "/indexes/manifest.json";
    ASSERT_TRUE(fs::exists(manifest_path));
}

TEST_F(IndexPersistenceTest, SaveAndLoadBloomFilter) {
    IndexPersistenceManager manager(test_dir_);
    ASSERT_TRUE(manager.Initialize().ok());
    ASSERT_TRUE(manager.RegisterColumnFamily(1, "test_cf").ok());

    // Create some test bloom filter data
    std::vector<uint8_t> test_data = {0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08};

    // Save bloom filter
    auto save_status = manager.SaveBloomFilter(1, 100, test_data);
    ASSERT_TRUE(save_status.ok()) << save_status.ToString();

    // Check file was created
    std::string bloom_path = manager.GetBloomFilterPath(1, 100);
    ASSERT_TRUE(fs::exists(bloom_path)) << "Bloom filter file not found at: " << bloom_path;

    // Load bloom filter
    std::vector<uint8_t> loaded_data;
    auto load_status = manager.LoadBloomFilter(1, 100, &loaded_data);
    ASSERT_TRUE(load_status.ok()) << load_status.ToString();

    // Verify data matches
    ASSERT_EQ(test_data.size(), loaded_data.size());
    ASSERT_EQ(test_data, loaded_data);
}

TEST_F(IndexPersistenceTest, LoadNonExistentBloomFilter) {
    IndexPersistenceManager manager(test_dir_);
    ASSERT_TRUE(manager.Initialize().ok());

    std::vector<uint8_t> loaded_data;
    auto load_status = manager.LoadBloomFilter(1, 999, &loaded_data);
    ASSERT_TRUE(load_status.IsNotFound());
}

TEST_F(IndexPersistenceTest, LoadAllBloomFilters) {
    IndexPersistenceManager manager(test_dir_);
    ASSERT_TRUE(manager.Initialize().ok());
    ASSERT_TRUE(manager.RegisterColumnFamily(1, "test_cf").ok());

    // Save multiple bloom filters
    std::vector<uint8_t> data1 = {0x01, 0x02};
    std::vector<uint8_t> data2 = {0x03, 0x04};
    std::vector<uint8_t> data3 = {0x05, 0x06};

    ASSERT_TRUE(manager.SaveBloomFilter(1, 100, data1).ok());
    ASSERT_TRUE(manager.SaveBloomFilter(1, 101, data2).ok());
    ASSERT_TRUE(manager.SaveBloomFilter(1, 102, data3).ok());

    // Load all filters
    std::unordered_map<uint64_t, std::vector<uint8_t>> all_filters;
    auto status = manager.LoadAllBloomFilters(1, &all_filters);
    ASSERT_TRUE(status.ok()) << status.ToString();

    // Verify we got all 3 filters
    ASSERT_EQ(3, all_filters.size());
    ASSERT_EQ(data1, all_filters[100]);
    ASSERT_EQ(data2, all_filters[101]);
    ASSERT_EQ(data3, all_filters[102]);
}

TEST_F(IndexPersistenceTest, DeleteBloomFilter) {
    IndexPersistenceManager manager(test_dir_);
    ASSERT_TRUE(manager.Initialize().ok());
    ASSERT_TRUE(manager.RegisterColumnFamily(1, "test_cf").ok());

    // Save a bloom filter
    std::vector<uint8_t> test_data = {0x01, 0x02, 0x03};
    ASSERT_TRUE(manager.SaveBloomFilter(1, 100, test_data).ok());

    // Verify it exists
    std::string bloom_path = manager.GetBloomFilterPath(1, 100);
    ASSERT_TRUE(fs::exists(bloom_path));

    // Delete it
    auto status = manager.DeleteBloomFilter(1, 100);
    ASSERT_TRUE(status.ok()) << status.ToString();

    // Verify it's gone
    ASSERT_FALSE(fs::exists(bloom_path));

    // Loading should return NotFound
    std::vector<uint8_t> loaded_data;
    auto load_status = manager.LoadBloomFilter(1, 100, &loaded_data);
    ASSERT_TRUE(load_status.IsNotFound());
}

TEST_F(IndexPersistenceTest, GetStats) {
    IndexPersistenceManager manager(test_dir_);
    ASSERT_TRUE(manager.Initialize().ok());
    ASSERT_TRUE(manager.RegisterColumnFamily(1, "test_cf").ok());

    // Save some bloom filters
    std::vector<uint8_t> data(1024, 0xFF);  // 1KB
    ASSERT_TRUE(manager.SaveBloomFilter(1, 100, data).ok());
    ASSERT_TRUE(manager.SaveBloomFilter(1, 101, data).ok());

    // Get stats
    std::string stats_json = manager.GetStats();

    // Stats should be valid JSON with expected fields
    ASSERT_FALSE(stats_json.empty());
    ASSERT_NE(stats_json.find("total_column_families"), std::string::npos);
    ASSERT_NE(stats_json.find("total_bloom_filters"), std::string::npos);
    ASSERT_NE(stats_json.find("total_size_bytes"), std::string::npos);
}

TEST_F(IndexPersistenceTest, ManifestPersistence) {
    // Create manager and add data
    {
        IndexPersistenceManager manager(test_dir_);
        ASSERT_TRUE(manager.Initialize().ok());
        ASSERT_TRUE(manager.RegisterColumnFamily(1, "cf1").ok());
        ASSERT_TRUE(manager.RegisterColumnFamily(2, "cf2").ok());

        std::vector<uint8_t> data = {0x01, 0x02};
        ASSERT_TRUE(manager.SaveBloomFilter(1, 100, data).ok());
        ASSERT_TRUE(manager.SaveBloomFilter(2, 200, data).ok());
    }

    // Create new manager and load manifest
    {
        IndexPersistenceManager manager(test_dir_);
        ASSERT_TRUE(manager.Initialize().ok());
        ASSERT_TRUE(manager.LoadManifest().ok());

        // Should be able to load the bloom filters
        std::vector<uint8_t> loaded_data;
        ASSERT_TRUE(manager.LoadBloomFilter(1, 100, &loaded_data).ok());
        ASSERT_TRUE(manager.LoadBloomFilter(2, 200, &loaded_data).ok());
    }
}

}  // namespace
}  // namespace marble

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
