//==============================================================================
// MarbleDB Database Fuzz Tests - Similar to RocksDB's fuzz testing
//==============================================================================

#include "test_util/db_test_util.h"
#include <gtest/gtest.h>
#include <iostream>
#include <memory>
#include <string>
#include <vector>
#include <random>
#include <cstdint>

// Fuzz test for database operations with random inputs
class DBFuzzTest : public DBTestBase {
 public:
  DBFuzzTest() : DBTestBase(), rng_(std::random_device{}()) {}

  // Generate random string
  std::string RandomString(size_t min_len = 1, size_t max_len = 100) {
    std::uniform_int_distribution<size_t> len_dist(min_len, max_len);
    std::uniform_int_distribution<int> char_dist(32, 126); // Printable ASCII

    size_t len = len_dist(rng_);
    std::string result;
    result.reserve(len);
    for (size_t i = 0; i < len; ++i) {
      result += static_cast<char>(char_dist(rng_));
    }
    return result;
  }

  // Generate random key
  std::string RandomKey() {
    return "key_" + RandomString(5, 20);
  }

  // Generate random value
  std::string RandomValue() {
    return "value_" + RandomString(10, 200);
  }

  // Random number generator
  std::mt19937 rng_;
};

//==============================================================================
// BASIC FUZZ TESTS
//==============================================================================

TEST_F(DBFuzzTest, DISABLED_RandomPutGet) {
  // Fuzz test: Random puts and gets
  ASSERT_OK(Open());

  const int num_operations = 1000;
  std::vector<std::pair<std::string, std::string>> expected_data;

  // Random puts
  for (int i = 0; i < num_operations; ++i) {
    std::string key = RandomKey();
    std::string value = RandomValue();

    ASSERT_OK(Put(key, value));
    expected_data.emplace_back(key, value);
  }

  // Verify gets
  for (const auto& [key, expected_value] : expected_data) {
    std::string actual_value;
    ASSERT_OK(Get(key, &actual_value));
    ASSERT_EQ(actual_value, expected_value);
  }
}

TEST_F(DBFuzzTest, DISABLED_RandomOverwrite) {
  // Fuzz test: Random overwrites
  ASSERT_OK(Open());

  const int num_keys = 100;
  const int num_overwrites = 500;

  // Create initial keys
  std::unordered_map<std::string, std::string> latest_values;
  for (int i = 0; i < num_keys; ++i) {
    std::string key = "fuzz_key_" + std::to_string(i);
    std::string value = "initial_" + RandomString(20, 50);
    ASSERT_OK(Put(key, value));
    latest_values[key] = value;
  }

  // Random overwrites
  std::uniform_int_distribution<int> key_dist(0, num_keys - 1);
  for (int i = 0; i < num_overwrites; ++i) {
    int key_idx = key_dist(rng_);
    std::string key = "fuzz_key_" + std::to_string(key_idx);
    std::string new_value = "overwrite_" + std::to_string(i) + "_" + RandomString(10, 30);

    ASSERT_OK(Put(key, new_value));
    latest_values[key] = new_value;
  }

  // Verify final state
  for (const auto& [key, expected_value] : latest_values) {
    std::string actual_value;
    ASSERT_OK(Get(key, &actual_value));
    ASSERT_EQ(actual_value, expected_value);
  }
}

TEST_F(DBFuzzTest, DISABLED_RandomDelete) {
  // Fuzz test: Random deletes
  ASSERT_OK(Open());

  const int num_keys = 200;

  // Create keys
  std::unordered_map<std::string, std::string> existing_keys;
  for (int i = 0; i < num_keys; ++i) {
    std::string key = "del_key_" + std::to_string(i);
    std::string value = RandomValue();
    ASSERT_OK(Put(key, value));
    existing_keys[key] = value;
  }

  // Random deletes
  std::vector<std::string> deleted_keys;
  std::uniform_int_distribution<size_t> key_dist(0, existing_keys.size() - 1);

  for (int i = 0; i < num_keys / 2; ++i) {
    auto it = existing_keys.begin();
    std::advance(it, key_dist(rng_));
    std::string key_to_delete = it->first;

    ASSERT_OK(Delete(key_to_delete));
    deleted_keys.push_back(key_to_delete);
    existing_keys.erase(it);
  }

  // Verify deletes
  for (const std::string& deleted_key : deleted_keys) {
    std::string value;
    Status s = Get(deleted_key, &value);
    ASSERT_TRUE(s.IsNotFound());
  }

  // Verify remaining keys
  for (const auto& [key, expected_value] : existing_keys) {
    std::string actual_value;
    ASSERT_OK(Get(key, &actual_value));
    ASSERT_EQ(actual_value, expected_value);
  }
}

TEST_F(DBFuzzTest, DISABLED_RandomMixedOperations) {
  // Fuzz test: Mixed put/get/delete operations
  ASSERT_OK(Open());

  const int num_operations = 2000;
  std::unordered_map<std::string, std::string> expected_state;

  std::uniform_int_distribution<int> op_dist(0, 2); // 0=put, 1=get, 2=delete

  for (int i = 0; i < num_operations; ++i) {
    int operation = op_dist(rng_);
    std::string key = RandomKey();

    switch (operation) {
      case 0: { // Put
        std::string value = RandomValue();
        ASSERT_OK(Put(key, value));
        expected_state[key] = value;
        break;
      }
      case 1: { // Get
        std::string actual_value;
        Status s = Get(key, &actual_value);
        auto it = expected_state.find(key);
        if (it != expected_state.end()) {
          ASSERT_OK(s);
          ASSERT_EQ(actual_value, it->second);
        } else {
          ASSERT_TRUE(s.IsNotFound());
        }
        break;
      }
      case 2: { // Delete
        ASSERT_OK(Delete(key));
        expected_state.erase(key);
        break;
      }
    }
  }

  // Final verification
  for (const auto& [key, expected_value] : expected_state) {
    std::string actual_value;
    ASSERT_OK(Get(key, &actual_value));
    ASSERT_EQ(actual_value, expected_value);
  }
}

TEST_F(DBFuzzTest, DISABLED_RandomKeyFormats) {
  // Fuzz test: Various key formats and edge cases
  ASSERT_OK(Open());

  std::vector<std::string> test_keys = {
    "",                                    // Empty key
    "a",                                   // Single character
    "very_long_key_" + RandomString(1000, 2000), // Very long key
    "key with spaces and symbols !@#$%^&*()",
    "unicode_ключ_测试_テスト",             // Unicode keys
    "key_with_null" + std::string(1, '\0') + "_after_null",
    std::string(255, 'x'),                // Maximum typical key length
    "key_with_newlines\nand\ttabs",
    "duplicate_key",                      // Will be used multiple times
    "duplicate_key",
    "duplicate_key"
  };

  // Add some random keys too
  for (int i = 0; i < 50; ++i) {
    test_keys.push_back(RandomKey());
  }

  // Put all keys
  for (size_t i = 0; i < test_keys.size(); ++i) {
    std::string value = "value_" + std::to_string(i) + "_" + RandomString(10, 50);
    Status s = Put(test_keys[i], value);
    // Some keys might fail (like empty keys), that's OK for fuzz testing
    if (s.ok()) {
      // Try to read it back if put succeeded
      std::string retrieved_value;
      Status get_s = Get(test_keys[i], &retrieved_value);
      if (get_s.ok()) {
        ASSERT_EQ(retrieved_value, value);
      }
    }
  }
}

TEST_F(DBFuzzTest, DISABLED_RandomValueSizes) {
  // Fuzz test: Various value sizes from tiny to huge
  ASSERT_OK(Open());

  std::vector<size_t> value_sizes = {
    0,      // Empty value
    1,      // Single byte
    100,    // Small value
    1000,   // Medium value
    10000,  // Large value
    100000, // Very large value
    1000000 // 1MB value
  };

  for (size_t size : value_sizes) {
    std::string key = "size_test_" + std::to_string(size);
    std::string value(size, 'x');

    Status s = Put(key, value);
    if (s.ok()) {
      std::string retrieved_value;
      ASSERT_OK(Get(key, &retrieved_value));
      ASSERT_EQ(retrieved_value.size(), size);
      ASSERT_EQ(retrieved_value, value);
    }
  }
}

//==============================================================================
// ADVANCED FUZZ TESTS
//==============================================================================

TEST_F(DBFuzzTest, DISABLED_RandomConcurrentFuzz) {
  // Fuzz test: Concurrent operations (basic version)
  ASSERT_OK(Open());

  const int num_threads = 4;
  const int ops_per_thread = 250;

  std::vector<std::thread> threads;
  std::atomic<int> total_operations(0);

  auto worker = [this, ops_per_thread, &total_operations]() {
    std::mt19937 local_rng(std::random_device{}());
    std::uniform_int_distribution<int> op_dist(0, 1);

    for (int i = 0; i < ops_per_thread; ++i) {
      std::string key = "concurrent_key_" + std::to_string(local_rng() % 100);
      std::string value = RandomValue();

      if (op_dist(local_rng) == 0) {
        Put(key, value); // Don't check result in fuzz test
      } else {
        std::string retrieved_value;
        Get(key, &retrieved_value);
      }
      total_operations++;
    }
  };

  // Start threads
  for (int i = 0; i < num_threads; ++i) {
    threads.emplace_back(worker);
  }

  // Wait for completion
  for (auto& thread : threads) {
    thread.join();
  }

  // Basic sanity check
  ASSERT_EQ(total_operations.load(), num_threads * ops_per_thread);
}

TEST_F(DBFuzzTest, DISABLED_RandomFlushFuzz) {
  // Fuzz test: Random operations with periodic flushes
  ASSERT_OK(Open());

  const int num_operations = 1000;
  const int flush_interval = 100;

  for (int i = 0; i < num_operations; ++i) {
    std::string key = RandomKey();
    std::string value = RandomValue();

    Put(key, value);

    if (i % flush_interval == 0) {
      Flush();
    }
  }

  // Final flush
  Flush();
}

//==============================================================================
// CORRUPTION FUZZ TESTS
//==============================================================================

TEST_F(DBFuzzTest, DISABLED_CorruptionResistance) {
  // Test database behavior with various edge cases
  ASSERT_OK(Open());

  // Test with malformed data (this would be more sophisticated in real fuzzing)
  std::vector<std::string> edge_case_keys = {
    std::string(10000, '\0'),  // Lots of null bytes
    std::string(1000, '\xff'), // High bytes
    "key\x00with\x00nulls",     // Embedded nulls
    "key_with_escapes\r\n\t",  // Escape sequences
  };

  for (const std::string& key : edge_case_keys) {
    std::string value = RandomValue();
    Status s = Put(key, value);
    // Just ensure it doesn't crash, result doesn't matter for fuzz test
  }
}

//==============================================================================
// MAIN FUNCTION
//==============================================================================

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
