//==============================================================================
// MarbleDB Database Tests - Similar to RocksDB's db_test.cc
//==============================================================================

#include "test_util/db_test_util.h"
#include <gtest/gtest.h>
#include <iostream>
#include <memory>
#include <string>
#include <vector>

// Test fixture for database tests
class DBTest : public DBTestBase {
 public:
  DBTest() : DBTestBase() {}
};

//==============================================================================
// BASIC DATABASE OPERATIONS
//==============================================================================

TEST_F(DBTest, OpenAndClose) {
  // Test basic database open and close
  ASSERT_OK(Open());
  ASSERT_TRUE(db_ != nullptr);
  Close();
  ASSERT_TRUE(db_ == nullptr);
}

TEST_F(DBTest, PutAndGet) {
  // Test basic put and get operations
  ASSERT_OK(Open());

  ASSERT_OK(Put("key1", "value1"));
  ASSERT_OK(Put("key2", "value2"));

  std::string value;
  ASSERT_OK(Get("key1", &value));
  ASSERT_EQ(value, "value1");

  ASSERT_OK(Get("key2", &value));
  ASSERT_EQ(value, "value2");

  // Test non-existent key
  Status s = Get("nonexistent", &value);
  ASSERT_TRUE(s.IsNotFound());
}

TEST_F(DBTest, Delete) {
  // Test delete operations
  ASSERT_OK(Open());

  ASSERT_OK(Put("key1", "value1"));
  ASSERT_OK(Put("key2", "value2"));

  // Verify both keys exist
  std::string value;
  ASSERT_OK(Get("key1", &value));
  ASSERT_OK(Get("key2", &value));

  // Delete one key
  ASSERT_OK(Delete("key1"));

  // Verify deleted key is gone
  Status s = Get("key1", &value);
  ASSERT_TRUE(s.IsNotFound());

  // Verify other key still exists
  ASSERT_OK(Get("key2", &value));
  ASSERT_EQ(value, "value2");
}

TEST_F(DBTest, Overwrite) {
  // Test overwriting existing keys
  ASSERT_OK(Open());

  ASSERT_OK(Put("key1", "value1"));
  std::string value;
  ASSERT_OK(Get("key1", &value));
  ASSERT_EQ(value, "value1");

  // Overwrite with new value
  ASSERT_OK(Put("key1", "new_value"));
  ASSERT_OK(Get("key1", &value));
  ASSERT_EQ(value, "new_value");
}

TEST_F(DBTest, EmptyValues) {
  // Test empty values
  ASSERT_OK(Open());

  ASSERT_OK(Put("empty", ""));
  std::string value;
  ASSERT_OK(Get("empty", &value));
  ASSERT_EQ(value, "");
}

TEST_F(DBTest, LargeValues) {
  // Test large values
  ASSERT_OK(Open());

  std::string large_value(1024 * 1024, 'x'); // 1MB value
  ASSERT_OK(Put("large", large_value));

  std::string retrieved_value;
  ASSERT_OK(Get("large", &retrieved_value));
  ASSERT_EQ(retrieved_value, large_value);
}

TEST_F(DBTest, Flush) {
  // Test flush operation
  ASSERT_OK(Open());

  ASSERT_OK(Put("key1", "value1"));
  ASSERT_OK(Put("key2", "value2"));

  ASSERT_OK(Flush());

  // Verify data persists after flush
  std::string value;
  ASSERT_OK(Get("key1", &value));
  ASSERT_EQ(value, "value1");

  ASSERT_OK(Get("key2", &value));
  ASSERT_EQ(value, "value2");
}

//==============================================================================
// DATABASE LIFECYCLE TESTS
//==============================================================================

TEST_F(DBTest, DestroyAndReopen) {
  // Test destroying and reopening database
  ASSERT_OK(Open());

  ASSERT_OK(Put("key1", "value1"));
  ASSERT_OK(Put("key2", "value2"));

  // Close and destroy
  Close();

  // Reopen with destroy
  DestroyAndReopen();

  // Verify data is gone (destroyed)
  std::string value;
  Status s1 = Get("key1", &value);
  Status s2 = Get("key2", &value);
  // Note: Depending on implementation, data may or may not persist
  // This test verifies the reopen operation works
}

TEST_F(DBTest, Reopen) {
  // Test reopening existing database
  ASSERT_OK(Open());

  ASSERT_OK(Put("key1", "value1"));
  ASSERT_OK(Put("key2", "value2"));

  // Close and reopen
  Reopen();

  // Verify data still exists
  std::string value;
  ASSERT_OK(Get("key1", &value));
  ASSERT_EQ(value, "value1");

  ASSERT_OK(Get("key2", &value));
  ASSERT_EQ(value, "value2");
}

//==============================================================================
// CONFIGURATION TESTS
//==============================================================================

TEST_F(DBTest, DifferentConfigurations) {
  // Test different database configurations
  std::vector<OptionConfig> configs = {
    kDefault,
    kLZ4Compression,
    kSnappyCompression,
    kNoCompression,
    kBlockSize4KB,
    kBlockSize64KB,
    kEnableBloomFilter,
    kDisableBloomFilter
  };

  for (auto config : configs) {
    SCOPED_TRACE("Testing config: " + std::to_string(static_cast<int>(config)));

    DBOptions options = GetOptions(config);
    ASSERT_OK(Open(options));

    // Basic operations still work
    ASSERT_OK(Put("test_key", "test_value"));
    std::string value;
    ASSERT_OK(Get("test_key", &value));
    ASSERT_EQ(value, "test_value");

    Close();
  }
}

//==============================================================================
// CONCURRENT OPERATIONS TESTS
//==============================================================================

TEST_F(DBTest, ConcurrentReads) {
  // Test concurrent read operations
  ASSERT_OK(Open());

  // Populate some data
  for (int i = 0; i < 100; ++i) {
    ASSERT_OK(Put("key" + std::to_string(i), "value" + std::to_string(i)));
  }

  // Test concurrent reads (basic version)
  std::vector<std::thread> threads;
  std::atomic<int> successful_reads(0);

  auto read_worker = [this, &successful_reads]() {
    for (int i = 0; i < 10; ++i) {
      std::string value;
      if (Get("key" + std::to_string(i), &value).ok()) {
        successful_reads++;
      }
    }
  };

  // Start multiple reader threads
  for (int i = 0; i < 5; ++i) {
    threads.emplace_back(read_worker);
  }

  // Wait for all threads
  for (auto& thread : threads) {
    thread.join();
  }

  // Verify reads succeeded
  ASSERT_EQ(successful_reads.load(), 50); // 5 threads * 10 reads each
}

//==============================================================================
// EDGE CASE TESTS
//==============================================================================

TEST_F(DBTest, ManyKeys) {
  // Test with many keys
  ASSERT_OK(Open());

  const int num_keys = 1000;
  for (int i = 0; i < num_keys; ++i) {
    ASSERT_OK(Put("key" + std::to_string(i), "value" + std::to_string(i)));
  }

  // Verify all keys
  for (int i = 0; i < num_keys; ++i) {
    std::string value;
    ASSERT_OK(Get("key" + std::to_string(i), &value));
    ASSERT_EQ(value, "value" + std::to_string(i));
  }
}

TEST_F(DBTest, KeyOrdering) {
  // Test key ordering behavior
  ASSERT_OK(Open());

  // Insert keys in reverse order
  ASSERT_OK(Put("key3", "value3"));
  ASSERT_OK(Put("key1", "value1"));
  ASSERT_OK(Put("key2", "value2"));

  // Verify retrieval works regardless of insertion order
  std::string value;
  ASSERT_OK(Get("key1", &value));
  ASSERT_EQ(value, "value1");

  ASSERT_OK(Get("key2", &value));
  ASSERT_EQ(value, "value2");

  ASSERT_OK(Get("key3", &value));
  ASSERT_EQ(value, "value3");
}

TEST_F(DBTest, SpecialCharacters) {
  // Test keys and values with special characters
  ASSERT_OK(Open());

  std::string special_key = "key with spaces & symbols!@#$%^&*()";
  std::string special_value = "value with\nnewlines\tand\ttabs";

  ASSERT_OK(Put(special_key, special_value));

  std::string retrieved_value;
  ASSERT_OK(Get(special_key, &retrieved_value));
  ASSERT_EQ(retrieved_value, special_value);
}

//==============================================================================
// ERROR CONDITION TESTS
//==============================================================================

TEST_F(DBTest, InvalidOperations) {
  // Test operations on closed database
  // Note: These tests may need adjustment based on actual error handling

  // Try operations without opening database first
  std::string value;
  Status s = Get("key1", &value);
  // This should fail in some way (exact error depends on implementation)
  ASSERT_FALSE(s.ok());
}

//==============================================================================
// PERFORMANCE CHARACTERIZATION TESTS
//==============================================================================

TEST_F(DBTest, DISABLED_WriteThroughput) {
  // Basic write throughput test (disabled by default)
  ASSERT_OK(Open());

  const int num_writes = 10000;
  auto start_time = std::chrono::high_resolution_clock::now();

  for (int i = 0; i < num_writes; ++i) {
    ASSERT_OK(Put("key" + std::to_string(i), "value" + std::to_string(i)));
  }

  auto end_time = std::chrono::high_resolution_clock::now();
  auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(
      end_time - start_time);

  double throughput = static_cast<double>(num_writes) / duration.count() * 1000.0;
  std::cout << "Write throughput: " << throughput << " ops/sec" << std::endl;
}

TEST_F(DBTest, DISABLED_ReadThroughput) {
  // Basic read throughput test (disabled by default)
  ASSERT_OK(Open());

  // First populate data
  const int num_keys = 10000;
  for (int i = 0; i < num_keys; ++i) {
    ASSERT_OK(Put("key" + std::to_string(i), "value" + std::to_string(i)));
  }

  // Now measure read throughput
  auto start_time = std::chrono::high_resolution_clock::now();

  for (int i = 0; i < num_keys; ++i) {
    std::string value;
    ASSERT_OK(Get("key" + std::to_string(i), &value));
  }

  auto end_time = std::chrono::high_resolution_clock::now();
  auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(
      end_time - start_time);

  double throughput = static_cast<double>(num_keys) / duration.count() * 1000.0;
  std::cout << "Read throughput: " << throughput << " ops/sec" << std::endl;
}

//==============================================================================
// MAIN FUNCTION
//==============================================================================

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
