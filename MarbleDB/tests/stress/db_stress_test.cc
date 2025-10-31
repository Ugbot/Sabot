//==============================================================================
// MarbleDB Stress Test Tool - Similar to RocksDB's db_stress_tool
//==============================================================================

#include "test_util/db_test_util.h"
#include <gtest/gtest.h>
#include <iostream>
#include <memory>
#include <string>
#include <vector>
#include <thread>
#include <atomic>
#include <random>
#include <chrono>
#include <unordered_map>
#include <unordered_set>

// Stress test configuration
struct StressTestConfig {
  int num_threads = 4;
  int num_operations = 10000;
  int num_keys = 1000;
  int value_size = 100;
  int read_percentage = 70;
  int write_percentage = 20;
  int delete_percentage = 10;
  int duration_seconds = 60;
  bool enable_compaction = false;
  bool enable_verification = true;
  bool enable_statistics = true;
};

// Stress test statistics
struct StressTestStats {
  std::atomic<uint64_t> total_operations{0};
  std::atomic<uint64_t> successful_reads{0};
  std::atomic<uint64_t> successful_writes{0};
  std::atomic<uint64_t> successful_deletes{0};
  std::atomic<uint64_t> failed_operations{0};
  std::atomic<uint64_t> data_corruption_errors{0};

  void Print() const {
    uint64_t total = total_operations.load();
    uint64_t reads = successful_reads.load();
    uint64_t writes = successful_writes.load();
    uint64_t deletes = successful_deletes.load();
    uint64_t failures = failed_operations.load();

    std::cout << "\n=== Stress Test Results ===" << std::endl;
    std::cout << "Total operations: " << total << std::endl;
    std::cout << "Successful reads: " << reads << std::endl;
    std::cout << "Successful writes: " << writes << std::endl;
    std::cout << "Successful deletes: " << deletes << std::endl;
    std::cout << "Failed operations: " << failures << std::endl;
    std::cout << "Success rate: "
              << (total > 0 ? (100.0 * (total - failures) / total) : 0.0)
              << "%" << std::endl;
  }
};

// Stress test worker
class StressTestWorker {
 public:
  StressTestWorker(DBTestBase* db, const StressTestConfig& config,
                   StressTestStats* stats, int worker_id)
      : db_(db), config_(config), stats_(stats), worker_id_(worker_id),
        rng_(worker_id) {}

  void Run() {
    std::uniform_int_distribution<> op_dist(0, 99);
    std::uniform_int_distribution<> key_dist(0, config_.num_keys - 1);

    for (int i = 0; i < config_.num_operations; ++i) {
      int operation = op_dist(rng_);

      if (operation < config_.read_percentage) {
        PerformRead(key_dist(rng_));
      } else if (operation < config_.read_percentage + config_.write_percentage) {
        PerformWrite(key_dist(rng_));
      } else {
        PerformDelete(key_dist(rng_));
      }

      stats_->total_operations++;
    }
  }

 private:
  void PerformRead(int key_id) {
    std::string key = "stress_key_" + std::to_string(key_id);
    std::string value;

    Status s = db_->Get(key, &value);
    if (s.ok()) {
      stats_->successful_reads++;
      // Optional: Verify value content
      if (config_.enable_verification) {
        // Could verify value format/content
      }
    } else if (!s.IsNotFound()) {
      // Unexpected error (not just key not found)
      stats_->failed_operations++;
    }
  }

  void PerformWrite(int key_id) {
    std::string key = "stress_key_" + std::to_string(key_id);
    std::string value = GenerateValue(key_id);

    Status s = db_->Put(key, value);
    if (s.ok()) {
      stats_->successful_writes++;
      // Track expected values for verification
      expected_values_[key] = value;
    } else {
      stats_->failed_operations++;
    }
  }

  void PerformDelete(int key_id) {
    std::string key = "stress_key_" + std::to_string(key_id);

    Status s = db_->Delete(key);
    if (s.ok()) {
      stats_->successful_deletes++;
      expected_values_.erase(key);
    } else {
      stats_->failed_operations++;
    }
  }

  std::string GenerateValue(int key_id) {
    std::string value = "stress_value_" + std::to_string(key_id) + "_";
    value += std::string(config_.value_size - value.length(), 'x');
    return value;
  }

  DBTestBase* db_;
  const StressTestConfig& config_;
  StressTestStats* stats_;
  int worker_id_;
  std::mt19937 rng_;
  std::unordered_map<std::string, std::string> expected_values_;
};

//==============================================================================
// STRESS TEST SUITE
//==============================================================================

class DBStressTest : public DBTestBase {
 public:
  DBStressTest() : DBTestBase() {}

  void RunStressTest(const StressTestConfig& config = StressTestConfig()) {
    std::cout << "Starting stress test with " << config.num_threads
              << " threads, " << config.num_operations << " operations each"
              << std::endl;

    // Initialize database
    ASSERT_OK(Open());

    StressTestStats stats;

    // Pre-populate some data
    std::cout << "Pre-populating database..." << std::endl;
    for (int i = 0; i < config.num_keys / 2; ++i) {
      std::string key = "stress_key_" + std::to_string(i);
      std::string value = "initial_value_" + std::to_string(i) + "_" +
                         std::string(config.value_size - 20, 'x');
      ASSERT_OK(Put(key, value));
    }

    // Start worker threads
    std::vector<std::thread> workers;
    std::vector<std::unique_ptr<StressTestWorker>> worker_objects;

    std::cout << "Starting worker threads..." << std::endl;
    for (int i = 0; i < config.num_threads; ++i) {
      auto worker = std::make_unique<StressTestWorker>(this, config, &stats, i);
      workers.emplace_back(&StressTestWorker::Run, worker.get());
      worker_objects.push_back(std::move(worker));
    }

    // Wait for all workers to complete
    for (auto& worker : workers) {
      worker.join();
    }

    // Print results
    stats.Print();

    // Optional: Run verification
    if (config.enable_verification) {
      RunVerification();
    }
  }

  void RunVerification() {
    std::cout << "Running data verification..." << std::endl;
    // Basic verification - try to read some known keys
    for (int i = 0; i < 100; ++i) {
      std::string key = "stress_key_" + std::to_string(i);
      std::string value;
      Status s = Get(key, &value);
      if (s.ok()) {
        // Verify basic structure
        ASSERT_TRUE(value.find("stress_value_") == 0 ||
                   value.find("initial_value_") == 0);
      }
      // Not found is OK - keys may have been deleted
    }
  }
};

TEST_F(DBStressTest, BasicStressTest) {
  StressTestConfig config;
  config.num_threads = 2;
  config.num_operations = 1000;
  config.num_keys = 100;
  config.duration_seconds = 10;

  RunStressTest(config);
}

TEST_F(DBStressTest, HighConcurrencyStressTest) {
  StressTestConfig config;
  config.num_threads = 8;
  config.num_operations = 5000;
  config.num_keys = 500;
  config.duration_seconds = 30;

  RunStressTest(config);
}

TEST_F(DBStressTest, WriteHeavyStressTest) {
  StressTestConfig config;
  config.num_threads = 4;
  config.num_operations = 3000;
  config.num_keys = 200;
  config.read_percentage = 20;
  config.write_percentage = 70;
  config.delete_percentage = 10;
  config.duration_seconds = 20;

  RunStressTest(config);
}

TEST_F(DBStressTest, ReadHeavyStressTest) {
  StressTestConfig config;
  config.num_threads = 6;
  config.num_operations = 4000;
  config.num_keys = 300;
  config.read_percentage = 90;
  config.write_percentage = 5;
  config.delete_percentage = 5;
  config.duration_seconds = 25;

  RunStressTest(config);
}

TEST_F(DBStressTest, LargeValueStressTest) {
  StressTestConfig config;
  config.num_threads = 2;
  config.num_operations = 500;
  config.num_keys = 50;
  config.value_size = 1024 * 10; // 10KB values
  config.duration_seconds = 15;

  RunStressTest(config);
}

TEST_F(DBStressTest, DISABLED_LongRunningStressTest) {
  // Long-running stress test (disabled by default)
  StressTestConfig config;
  config.num_threads = 4;
  config.num_operations = 100000; // Much higher operation count
  config.num_keys = 1000;
  config.duration_seconds = 300; // 5 minutes

  auto start_time = std::chrono::high_resolution_clock::now();
  RunStressTest(config);
  auto end_time = std::chrono::high_resolution_clock::now();

  auto duration = std::chrono::duration_cast<std::chrono::seconds>(
      end_time - start_time);
  std::cout << "Long-running stress test completed in "
            << duration.count() << " seconds" << std::endl;
}

TEST_F(DBStressTest, DISABLED_MemoryPressureStressTest) {
  // Memory pressure stress test (disabled by default)
  StressTestConfig config;
  config.num_threads = 4;
  config.num_operations = 50000;
  config.num_keys = 10000; // Many keys
  config.value_size = 1024; // 1KB values
  config.duration_seconds = 120; // 2 minutes

  // This test stresses memory usage with many keys and values
  RunStressTest(config);
}

//==============================================================================
// CONCURRENT STRESS TESTS
//==============================================================================

class ConcurrentStressTest : public DBStressTest {
 public:
  void RunConcurrentStressTest(int num_db_instances = 2) {
    std::cout << "Running concurrent database stress test with "
              << num_db_instances << " database instances" << std::endl;

    std::vector<std::unique_ptr<DBStressTest>> db_instances;
    std::vector<std::thread> threads;

    // Create multiple database instances
    for (int i = 0; i < num_db_instances; ++i) {
      auto db = std::make_unique<DBStressTest>();
      db_instances.push_back(std::move(db));
    }

    // Run stress tests concurrently on each instance
    for (int i = 0; i < num_db_instances; ++i) {
      threads.emplace_back([this, &db_instances, i]() {
        StressTestConfig config;
        config.num_threads = 2;
        config.num_operations = 2000;
        config.num_keys = 100;
        config.duration_seconds = 15;

        db_instances[i]->RunStressTest(config);
      });
    }

    // Wait for all concurrent tests to complete
    for (auto& thread : threads) {
      thread.join();
    }

    std::cout << "Concurrent database stress test completed" << std::endl;
  }
};

TEST_F(ConcurrentStressTest, DISABLED_ConcurrentDatabases) {
  RunConcurrentStressTest(3);
}

//==============================================================================
// MAIN FUNCTION
//==============================================================================

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
