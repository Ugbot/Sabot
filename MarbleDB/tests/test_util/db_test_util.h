//==============================================================================
// MarbleDB Database Test Utilities - Similar to RocksDB's db_test_util.h
//==============================================================================

#pragma once

#include "testharness.h"
#include <marble/db.h>
#include <marble/status.h>
#include <memory>
#include <string>
#include <vector>

// Forward declarations
namespace marble {
class MarbleDB;
class DBOptions;
class WriteOptions;
class ReadOptions;
class Status;
}

// Bring types into scope for convenience
using marble::Status;
using marble::DBOptions;
using marble::WriteOptions;
using marble::ReadOptions;

// Database test base class - similar to RocksDB's DBTestBase
class DBTestBase : public MarbleDBTest {
public:
  // Option configurations to test (similar to RocksDB's OptionConfig)
  enum OptionConfig : int {
    kDefault = 0,
    kLZ4Compression,
    kSnappyCompression,
    kNoCompression,
    kBlockSize4KB,
    kBlockSize64KB,
    kEnableBloomFilter,
    kDisableBloomFilter,
    kSmallMemTable,
    kLargeMemTable,
    kEnableWAL,
    kDisableWAL,
    kEnableSecondaryIndex,
    kDisableSecondaryIndex,
    // Add more configurations as needed
    kEnd
  };

  DBTestBase();
  ~DBTestBase() override;

  // Database operations
  Status Open(const DBOptions& options = DBOptions{});
  void Close();
  Status Put(const std::string& key, const std::string& value,
             const WriteOptions& options = WriteOptions{});
  Status Get(const std::string& key, std::string* value,
             const ReadOptions& options = ReadOptions{});
  Status Delete(const std::string& key,
                const WriteOptions& options = WriteOptions{});
  Status Flush();

  // Test utilities
  void ResetDB();
  void DestroyAndReopen(const DBOptions& options = DBOptions{});
  void Reopen(const DBOptions& options = DBOptions{});

  // Configuration management
  DBOptions GetDefaultOptions();
  DBOptions GetOptions(OptionConfig config);

  // Database state
  std::unique_ptr<marble::MarbleDB> db_;
  std::shared_ptr<marble::Schema> schema_;

private:
  bool db_open_;
};

// Helper functions for creating test data
namespace test {

// Create a simple test schema
std::shared_ptr<marble::Schema> CreateSimpleSchema();

// Create test records
std::shared_ptr<marble::Record> CreateTestRecord(const std::string& key,
                                                 const std::string& value);

// Generate sequential test data
void GenerateTestData(std::vector<std::pair<std::string, std::string>>& data,
                      int num_entries, int key_size = 16, int value_size = 100);

} // namespace test
