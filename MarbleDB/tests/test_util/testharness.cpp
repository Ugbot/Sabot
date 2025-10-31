//==============================================================================
// MarbleDB Test Harness Implementation
//==============================================================================

#include "testharness.h"
#include <filesystem>
#include <iostream>
#include <cstdlib>

namespace fs = std::filesystem;

MarbleDBTest::MarbleDBTest()
    : dbname_created_(false) {
  dbname_ = test::TmpDir() + "/marble_test";
}

MarbleDBTest::~MarbleDBTest() {
  if (dbname_created_) {
    test::DestroyDir(dbname_);
  }
}

void MarbleDBTest::SetUp() {
  ResetDB();
}

void MarbleDBTest::TearDown() {
  if (dbname_created_) {
    test::DestroyDir(dbname_);
    dbname_created_ = false;
  }
}

void MarbleDBTest::DestroyDB(const std::string& dbname) {
  test::DestroyDir(dbname);
}

void MarbleDBTest::ResetDB() {
  if (dbname_created_) {
    test::DestroyDir(dbname_);
  }
  dbname_created_ = true;
}

namespace test {

std::string TmpDir() {
  const char* tmpdir = std::getenv("TEST_TMPDIR");
  if (tmpdir == nullptr) {
    tmpdir = "/tmp";
  }
  return std::string(tmpdir);
}

void DestroyDir(const std::string& dir) {
  std::error_code ec;
  fs::remove_all(dir, ec);
  if (ec) {
    std::cerr << "Warning: Failed to destroy directory " << dir
              << ": " << ec.message() << std::endl;
  }
}

} // namespace test
