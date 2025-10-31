//==============================================================================
// MarbleDB Database Test Utilities Implementation
//==============================================================================

#include "db_test_util.h"
#include <marble/db.h>
#include <marble/record.h>
#include <marble/table.h>
#include <marble/column_family.h>
#include <iostream>
#include <random>
#include <sstream>

DBTestBase::DBTestBase() : db_open_(false) {
  schema_ = test::CreateSimpleSchema();
}

DBTestBase::~DBTestBase() {
  Close();
}

Status DBTestBase::Open(const DBOptions& options) {
  if (db_open_) {
    Close();
  }

  DBOptions opts = options;
  opts.db_path = dbname_;

  std::unique_ptr<marble::MarbleDB> db;
  Status s = marble::MarbleDB::Open(opts, schema_, &db);
  if (s.ok()) {
    db_ = std::move(db);
    db_open_ = true;
  }
  return s;
}

void DBTestBase::Close() {
  if (db_open_ && db_) {
    db_->Close();
    db_.reset();
    db_open_ = false;
  }
}

Status DBTestBase::Put(const std::string& key, const std::string& value,
                       const WriteOptions& options) {
  if (!db_open_) {
    return Status::NotFound("Database not open");
  }

  // For now, create a simple record
  auto record = test::CreateTestRecord(key, value);
  return db_->Put(options, record);
}

Status DBTestBase::Get(const std::string& key, std::string* value,
                       const ReadOptions& options) {
  if (!db_open_) {
    return Status::NotFound("Database not open");
  }

  std::shared_ptr<marble::Record> record;
  Status s = db_->Get(options, marble::Key(key), &record);
  if (s.ok() && record) {
    // Extract value from record (simplified for testing)
    *value = key + "_value"; // TODO: Properly extract from record
  }
  return s;
}

Status DBTestBase::Delete(const std::string& key, const WriteOptions& options) {
  if (!db_open_) {
    return Status::NotFound("Database not open");
  }

  return db_->Delete(options, marble::Key(key));
}

Status DBTestBase::Flush() {
  if (!db_open_) {
    return Status::NotFound("Database not open");
  }

  return db_->Flush();
}

void DBTestBase::ResetDB() {
  MarbleDBTest::ResetDB();
  Close();
}

void DBTestBase::DestroyAndReopen(const DBOptions& options) {
  Close();
  test::DestroyDir(dbname_);
  dbname_created_ = false;
  Status s = Open(options);
  ASSERT_TRUE(s.ok()) << "Failed to reopen database: " << s.ToString();
}

void DBTestBase::Reopen(const DBOptions& options) {
  Close();
  Status s = Open(options);
  ASSERT_TRUE(s.ok()) << "Failed to reopen database: " << s.ToString();
}

DBOptions DBTestBase::GetDefaultOptions() {
  return GetOptions(kDefault);
}

DBOptions DBTestBase::GetOptions(OptionConfig config) {
  DBOptions options;

  switch (config) {
    case kDefault:
      // Default configuration
      break;

    case kLZ4Compression:
      options.compression = DBOptions::CompressionType::kLZ4;
      break;

    case kSnappyCompression:
      options.compression = DBOptions::CompressionType::kSnappy;
      break;

    case kNoCompression:
      options.compression = DBOptions::CompressionType::kNoCompression;
      break;

    case kBlockSize4KB:
      options.block_size = 4 * 1024;
      break;

    case kBlockSize64KB:
      options.block_size = 64 * 1024;
      break;

    case kEnableBloomFilter:
      options.enable_bloom_filter = true;
      break;

    case kDisableBloomFilter:
      options.enable_bloom_filter = false;
      break;

    case kSmallMemTable:
      options.memtable_size_threshold = 1 * 1024 * 1024; // 1MB
      break;

    case kLargeMemTable:
      options.memtable_size_threshold = 64 * 1024 * 1024; // 64MB
      break;

    case kEnableWAL:
      options.enable_wal = true;
      break;

    case kDisableWAL:
      options.enable_wal = false;
      break;

    case kEnableSecondaryIndex:
      options.enable_sparse_index = true;
      break;

    case kDisableSecondaryIndex:
      options.enable_sparse_index = false;
      break;

    default:
      break;
  }

  return options;
}

namespace test {

std::shared_ptr<marble::Schema> CreateSimpleSchema() {
  // Create a simple schema with key and value columns
  auto schema = std::make_shared<marble::Schema>();
  // TODO: Implement proper schema creation
  return schema;
}

std::shared_ptr<marble::Record> CreateTestRecord(const std::string& key,
                                                 const std::string& value) {
  // Create a simple test record
  auto record_key = std::make_shared<marble::Int64Key>(std::stoll(key));
  // TODO: Implement proper record creation
  return nullptr;
}

void GenerateTestData(std::vector<std::pair<std::string, std::string>>& data,
                      int num_entries, int key_size, int value_size) {
  data.clear();
  data.reserve(num_entries);

  std::random_device rd;
  std::mt19937 gen(rd());
  std::uniform_int_distribution<> key_dist(0, 25); // a-z
  std::uniform_int_distribution<> val_dist(32, 126); // printable chars

  for (int i = 0; i < num_entries; ++i) {
    std::string key;
    std::string value;

    // Generate key
    for (int j = 0; j < key_size; ++j) {
      key += 'a' + key_dist(gen);
    }

    // Generate value
    for (int j = 0; j < value_size; ++j) {
      value += static_cast<char>(val_dist(gen));
    }

    data.emplace_back(key, value);
  }
}

} // namespace test
