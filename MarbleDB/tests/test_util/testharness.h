//==============================================================================
// MarbleDB Test Harness - Similar to RocksDB's test infrastructure
//==============================================================================

#pragma once

#include <gtest/gtest.h>
#include <memory>
#include <string>

// MarbleDB test skip/bypass macros (similar to RocksDB)
#define MARBLEDB_GTEST_SKIP(m)          \
  do {                                   \
    fputs("SKIPPED: " m "\n", stderr);  \
    GTEST_SKIP_(m);                     \
  } while (false)

#define MARBLEDB_GTEST_BYPASS(m)         \
  do {                                    \
    fputs("BYPASSED: " m "\n", stderr);  \
    GTEST_SUCCESS_("BYPASSED: " m);      \
  } while (false)

// Test fixture base class for MarbleDB tests
class MarbleDBTest : public testing::Test {
protected:
  MarbleDBTest();
  ~MarbleDBTest();

  // Set up and tear down
  void SetUp() override;
  void TearDown() override;

  // Test database path
  std::string dbname_;
  bool dbname_created_;

  // Helper methods
  void DestroyDB(const std::string& dbname);
  void ResetDB();
};

// Test utilities
namespace test {

// Generate a unique test database name
std::string TmpDir();

// Clean up test database
void DestroyDir(const std::string& dir);

} // namespace test
