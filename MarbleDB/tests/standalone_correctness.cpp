//==============================================================================
// MarbleDB Standalone Correctness Test
//
// A self-contained correctness test for MarbleDB operations.
// No test framework dependencies - just raw correctness verification.
//==============================================================================

#include <iostream>
#include <chrono>
#include <random>
#include <vector>
#include <string>
#include <memory>
#include <filesystem>
#include <iomanip>
#include <cassert>
#include <unordered_map>
#include <algorithm>

// MarbleDB includes
#include "marble/lsm_storage.h"
#include "marble/file_system.h"

namespace fs = std::filesystem;

//==============================================================================
// Test Configuration
//==============================================================================

struct TestConfig {
    std::string test_path = "/tmp/marble_correctness_test";
    size_t small_test_keys = 100;
    size_t medium_test_keys = 1000;
    size_t large_test_keys = 10000;
    size_t value_size = 100;
};

//==============================================================================
// Test Results
//==============================================================================

struct TestResult {
    std::string name;
    bool passed;
    std::string message;
    double duration_ms;
};

class TestSuite {
public:
    void AddResult(const TestResult& result) {
        results_.push_back(result);
        if (result.passed) {
            passed_++;
        } else {
            failed_++;
        }
    }

    void PrintSummary() const {
        std::cout << "\n";
        std::cout << "============================================================" << std::endl;
        std::cout << "                   TEST RESULTS SUMMARY                     " << std::endl;
        std::cout << "============================================================" << std::endl;

        for (const auto& result : results_) {
            std::cout << (result.passed ? "[PASS]" : "[FAIL]") << " "
                      << std::left << std::setw(40) << result.name
                      << " (" << std::fixed << std::setprecision(2) << result.duration_ms << " ms)";
            if (!result.passed) {
                std::cout << "\n       Error: " << result.message;
            }
            std::cout << std::endl;
        }

        std::cout << "------------------------------------------------------------" << std::endl;
        std::cout << "Total: " << (passed_ + failed_) << " | Passed: " << passed_
                  << " | Failed: " << failed_ << std::endl;
        std::cout << "============================================================" << std::endl;
    }

    bool AllPassed() const { return failed_ == 0; }
    size_t GetPassed() const { return passed_; }
    size_t GetFailed() const { return failed_; }

private:
    std::vector<TestResult> results_;
    size_t passed_ = 0;
    size_t failed_ = 0;
};

//==============================================================================
// Utility Functions
//==============================================================================

void CleanupDirectory(const std::string& path) {
    if (fs::exists(path)) {
        fs::remove_all(path);
    }
    fs::create_directories(path);
}

std::string GenerateRandomValue(size_t size, std::mt19937& gen) {
    static const char charset[] = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
    std::uniform_int_distribution<> dist(0, sizeof(charset) - 2);
    std::string result(size, '\0');
    for (size_t i = 0; i < size; ++i) {
        result[i] = charset[dist(gen)];
    }
    return result;
}

std::unique_ptr<marble::LSMTree> CreateTestLSM(const std::string& path) {
    CleanupDirectory(path);

    marble::LSMTreeConfig config;
    config.data_directory = path + "/data";
    config.wal_directory = path + "/wal";
    config.temp_directory = path + "/temp";
    config.memtable_max_size_bytes = 4 * 1024 * 1024;  // 4MB memtable
    config.l0_compaction_trigger = 4;
    config.max_levels = 7;

    // Create directories
    fs::create_directories(config.data_directory);
    fs::create_directories(config.wal_directory);
    fs::create_directories(config.temp_directory);

    auto lsm = marble::CreateLSMTree();
    auto status = lsm->Init(config);
    if (!status.ok()) {
        std::cerr << "Failed to init MarbleDB: " << status.ToString() << std::endl;
        return nullptr;
    }
    return lsm;
}

//==============================================================================
// Correctness Tests
//==============================================================================

TestResult TestBasicPutGet(const TestConfig& config) {
    auto start = std::chrono::high_resolution_clock::now();
    std::string test_path = config.test_path + "/basic_put_get";

    auto lsm = CreateTestLSM(test_path);
    if (!lsm) {
        return {"Basic Put/Get", false, "Failed to create LSM tree", 0};
    }

    // Test single key
    uint64_t key = 12345;
    std::string value = "test_value";
    auto status = lsm->Put(key, value);
    if (!status.ok()) {
        return {"Basic Put/Get", false, "Put failed: " + status.ToString(), 0};
    }

    std::string retrieved;
    status = lsm->Get(key, &retrieved);
    if (!status.ok()) {
        return {"Basic Put/Get", false, "Get failed: " + status.ToString(), 0};
    }

    if (retrieved != value) {
        return {"Basic Put/Get", false,
                "Value mismatch: expected '" + value + "', got '" + retrieved + "'", 0};
    }

    lsm->Shutdown();

    auto end = std::chrono::high_resolution_clock::now();
    double duration = std::chrono::duration<double, std::milli>(end - start).count();
    return {"Basic Put/Get", true, "", duration};
}

TestResult TestMultiplePutGet(const TestConfig& config) {
    auto start = std::chrono::high_resolution_clock::now();
    std::string test_path = config.test_path + "/multiple_put_get";

    auto lsm = CreateTestLSM(test_path);
    if (!lsm) {
        return {"Multiple Put/Get", false, "Failed to create LSM tree", 0};
    }

    std::mt19937 gen(42);
    std::unordered_map<uint64_t, std::string> reference;

    // Write keys
    for (size_t i = 0; i < config.small_test_keys; ++i) {
        uint64_t key = i;
        std::string value = GenerateRandomValue(config.value_size, gen);
        reference[key] = value;

        auto status = lsm->Put(key, value);
        if (!status.ok()) {
            return {"Multiple Put/Get", false, "Put failed at key " + std::to_string(i), 0};
        }
    }

    // Verify all keys
    for (const auto& [key, expected_value] : reference) {
        std::string retrieved;
        auto status = lsm->Get(key, &retrieved);
        if (!status.ok()) {
            return {"Multiple Put/Get", false, "Get failed for key: " + std::to_string(key), 0};
        }
        if (retrieved != expected_value) {
            return {"Multiple Put/Get", false,
                    "Value mismatch for key: " + std::to_string(key), 0};
        }
    }

    lsm->Shutdown();

    auto end = std::chrono::high_resolution_clock::now();
    double duration = std::chrono::duration<double, std::milli>(end - start).count();
    return {"Multiple Put/Get (" + std::to_string(config.small_test_keys) + " keys)", true, "", duration};
}

TestResult TestOverwrite(const TestConfig& config) {
    auto start = std::chrono::high_resolution_clock::now();
    std::string test_path = config.test_path + "/overwrite";

    auto lsm = CreateTestLSM(test_path);
    if (!lsm) {
        return {"Overwrite", false, "Failed to create LSM tree", 0};
    }

    uint64_t key = 99999;

    // Write multiple versions
    for (int i = 0; i < 10; ++i) {
        std::string value = "value_version_" + std::to_string(i);
        auto status = lsm->Put(key, value);
        if (!status.ok()) {
            return {"Overwrite", false, "Put failed at version " + std::to_string(i), 0};
        }
    }

    // Should get the latest value
    std::string retrieved;
    auto status = lsm->Get(key, &retrieved);
    if (!status.ok()) {
        return {"Overwrite", false, "Get failed: " + status.ToString(), 0};
    }

    if (retrieved != "value_version_9") {
        return {"Overwrite", false,
                "Expected 'value_version_9', got '" + retrieved + "'", 0};
    }

    lsm->Shutdown();

    auto end = std::chrono::high_resolution_clock::now();
    double duration = std::chrono::duration<double, std::milli>(end - start).count();
    return {"Overwrite", true, "", duration};
}

TestResult TestDelete(const TestConfig& config) {
    auto start = std::chrono::high_resolution_clock::now();
    std::string test_path = config.test_path + "/delete";

    auto lsm = CreateTestLSM(test_path);
    if (!lsm) {
        return {"Delete", false, "Failed to create LSM tree", 0};
    }

    uint64_t key = 88888;
    std::string value = "delete_value";

    // Write
    auto status = lsm->Put(key, value);
    if (!status.ok()) {
        return {"Delete", false, "Put failed", 0};
    }

    // Verify it exists
    std::string retrieved;
    status = lsm->Get(key, &retrieved);
    if (!status.ok()) {
        return {"Delete", false, "Get before delete failed", 0};
    }

    // Delete
    status = lsm->Delete(key);
    if (!status.ok()) {
        return {"Delete", false, "Delete failed: " + status.ToString(), 0};
    }

    // Verify it's gone
    status = lsm->Get(key, &retrieved);
    if (!status.IsNotFound()) {
        return {"Delete", false,
                "Expected NotFound after delete, got: " + status.ToString(), 0};
    }

    lsm->Shutdown();

    auto end = std::chrono::high_resolution_clock::now();
    double duration = std::chrono::duration<double, std::milli>(end - start).count();
    return {"Delete", true, "", duration};
}

TestResult TestNotFound(const TestConfig& config) {
    auto start = std::chrono::high_resolution_clock::now();
    std::string test_path = config.test_path + "/not_found";

    auto lsm = CreateTestLSM(test_path);
    if (!lsm) {
        return {"Not Found", false, "Failed to create LSM tree", 0};
    }

    std::string retrieved;
    auto status = lsm->Get(9999999, &retrieved);
    if (!status.IsNotFound()) {
        return {"Not Found", false,
                "Expected NotFound, got: " + status.ToString(), 0};
    }

    lsm->Shutdown();

    auto end = std::chrono::high_resolution_clock::now();
    double duration = std::chrono::duration<double, std::milli>(end - start).count();
    return {"Not Found", true, "", duration};
}

TestResult TestEmptyValue(const TestConfig& config) {
    auto start = std::chrono::high_resolution_clock::now();
    std::string test_path = config.test_path + "/empty_value";

    auto lsm = CreateTestLSM(test_path);
    if (!lsm) {
        return {"Empty Value", false, "Failed to create LSM tree", 0};
    }

    uint64_t key = 77777;
    std::string value = "";

    auto status = lsm->Put(key, value);
    if (!status.ok()) {
        return {"Empty Value", false, "Put failed: " + status.ToString(), 0};
    }

    std::string retrieved;
    status = lsm->Get(key, &retrieved);
    if (!status.ok()) {
        return {"Empty Value", false, "Get failed: " + status.ToString(), 0};
    }

    if (retrieved != value) {
        return {"Empty Value", false,
                "Expected empty string, got '" + retrieved + "'", 0};
    }

    lsm->Shutdown();

    auto end = std::chrono::high_resolution_clock::now();
    double duration = std::chrono::duration<double, std::milli>(end - start).count();
    return {"Empty Value", true, "", duration};
}

TestResult TestLargeValue(const TestConfig& config) {
    auto start = std::chrono::high_resolution_clock::now();
    std::string test_path = config.test_path + "/large_value";

    auto lsm = CreateTestLSM(test_path);
    if (!lsm) {
        return {"Large Value", false, "Failed to create LSM tree", 0};
    }

    std::mt19937 gen(42);
    uint64_t key = 66666;
    std::string value = GenerateRandomValue(1024 * 100, gen);  // 100KB value

    auto status = lsm->Put(key, value);
    if (!status.ok()) {
        return {"Large Value", false, "Put failed: " + status.ToString(), 0};
    }

    std::string retrieved;
    status = lsm->Get(key, &retrieved);
    if (!status.ok()) {
        return {"Large Value", false, "Get failed: " + status.ToString(), 0};
    }

    if (retrieved != value) {
        return {"Large Value", false, "Value mismatch (length: " +
                std::to_string(retrieved.size()) + " vs " +
                std::to_string(value.size()) + ")", 0};
    }

    lsm->Shutdown();

    auto end = std::chrono::high_resolution_clock::now();
    double duration = std::chrono::duration<double, std::milli>(end - start).count();
    return {"Large Value (100KB)", true, "", duration};
}

TestResult TestScan(const TestConfig& config) {
    auto start = std::chrono::high_resolution_clock::now();
    std::string test_path = config.test_path + "/scan";

    auto lsm = CreateTestLSM(test_path);
    if (!lsm) {
        return {"Scan", false, "Failed to create LSM tree", 0};
    }

    std::mt19937 gen(42);

    // Insert keys
    for (size_t i = 0; i < config.small_test_keys; ++i) {
        uint64_t key = i;
        std::string value = GenerateRandomValue(config.value_size, gen);
        auto status = lsm->Put(key, value);
        if (!status.ok()) {
            return {"Scan", false, "Put failed at " + std::to_string(i), 0};
        }
    }

    // Scan and count
    std::vector<std::pair<uint64_t, std::string>> results;
    auto status = lsm->Scan(0, config.small_test_keys - 1, &results);
    if (!status.ok()) {
        return {"Scan", false, "Scan failed: " + status.ToString(), 0};
    }

    if (results.size() != config.small_test_keys) {
        return {"Scan", false, "Expected " + std::to_string(config.small_test_keys) +
                " keys, scanned " + std::to_string(results.size()), 0};
    }

    lsm->Shutdown();

    auto end = std::chrono::high_resolution_clock::now();
    double duration = std::chrono::duration<double, std::milli>(end - start).count();
    return {"Scan (" + std::to_string(config.small_test_keys) + " keys)", true, "", duration};
}

TestResult TestScanOrder(const TestConfig& config) {
    auto start = std::chrono::high_resolution_clock::now();
    std::string test_path = config.test_path + "/scan_order";

    auto lsm = CreateTestLSM(test_path);
    if (!lsm) {
        return {"Scan Order", false, "Failed to create LSM tree", 0};
    }

    std::mt19937 gen(42);

    // Insert keys in random order
    std::vector<uint64_t> indices(config.small_test_keys);
    std::iota(indices.begin(), indices.end(), 0);
    std::shuffle(indices.begin(), indices.end(), gen);

    for (uint64_t key : indices) {
        std::string value = "value_" + std::to_string(key);
        auto status = lsm->Put(key, value);
        if (!status.ok()) {
            return {"Scan Order", false, "Put failed", 0};
        }
    }

    // Scan and verify order
    std::vector<std::pair<uint64_t, std::string>> results;
    auto status = lsm->Scan(0, config.small_test_keys - 1, &results);
    if (!status.ok()) {
        return {"Scan Order", false, "Scan failed: " + status.ToString(), 0};
    }

    // Verify results are in order
    uint64_t prev_key = 0;
    bool first = true;
    for (const auto& [key, value] : results) {
        if (!first && key <= prev_key) {
            return {"Scan Order", false, "Keys not in order: " +
                    std::to_string(prev_key) + " >= " + std::to_string(key), 0};
        }
        prev_key = key;
        first = false;
    }

    if (results.size() != config.small_test_keys) {
        return {"Scan Order", false, "Count mismatch: expected " +
                std::to_string(config.small_test_keys) + ", got " +
                std::to_string(results.size()), 0};
    }

    lsm->Shutdown();

    auto end = std::chrono::high_resolution_clock::now();
    double duration = std::chrono::duration<double, std::milli>(end - start).count();
    return {"Scan Order (numeric)", true, "", duration};
}

TestResult TestMediumDataset(const TestConfig& config) {
    auto start = std::chrono::high_resolution_clock::now();
    std::string test_path = config.test_path + "/medium_dataset";

    auto lsm = CreateTestLSM(test_path);
    if (!lsm) {
        return {"Medium Dataset", false, "Failed to create LSM tree", 0};
    }

    std::mt19937 gen(42);
    std::unordered_map<uint64_t, std::string> reference;

    // Write medium dataset
    for (size_t i = 0; i < config.medium_test_keys; ++i) {
        uint64_t key = i;
        std::string value = GenerateRandomValue(config.value_size, gen);
        reference[key] = value;

        auto status = lsm->Put(key, value);
        if (!status.ok()) {
            return {"Medium Dataset", false, "Put failed at " + std::to_string(i), 0};
        }
    }

    // Sample verification (check 10% of keys randomly)
    std::vector<uint64_t> keys_to_check;
    for (const auto& [key, _] : reference) {
        keys_to_check.push_back(key);
    }
    std::shuffle(keys_to_check.begin(), keys_to_check.end(), gen);

    size_t check_count = config.medium_test_keys / 10;
    for (size_t i = 0; i < check_count; ++i) {
        uint64_t key = keys_to_check[i];
        std::string retrieved;
        auto status = lsm->Get(key, &retrieved);
        if (!status.ok()) {
            return {"Medium Dataset", false, "Get failed for " + std::to_string(key), 0};
        }
        if (retrieved != reference[key]) {
            return {"Medium Dataset", false, "Value mismatch for " + std::to_string(key), 0};
        }
    }

    lsm->Shutdown();

    auto end = std::chrono::high_resolution_clock::now();
    double duration = std::chrono::duration<double, std::milli>(end - start).count();
    return {"Medium Dataset (" + std::to_string(config.medium_test_keys) + " keys)", true, "", duration};
}

TestResult TestLargeDataset(const TestConfig& config) {
    auto start = std::chrono::high_resolution_clock::now();
    std::string test_path = config.test_path + "/large_dataset";

    auto lsm = CreateTestLSM(test_path);
    if (!lsm) {
        return {"Large Dataset", false, "Failed to create LSM tree", 0};
    }

    std::mt19937 gen(42);
    std::mt19937 verify_gen(42);  // Same seed for verification

    // Write large dataset
    for (size_t i = 0; i < config.large_test_keys; ++i) {
        uint64_t key = i;
        std::string value = GenerateRandomValue(config.value_size, gen);
        auto status = lsm->Put(key, value);
        if (!status.ok()) {
            return {"Large Dataset", false, "Put failed at " + std::to_string(i), 0};
        }
    }

    // Verify a sample
    size_t check_count = std::min(config.large_test_keys / 100, (size_t)100);
    for (size_t i = 0; i < config.large_test_keys; ++i) {
        std::string expected_value = GenerateRandomValue(config.value_size, verify_gen);
        if (i < check_count || (i % 100 == 0)) {
            uint64_t key = i;
            std::string retrieved;
            auto status = lsm->Get(key, &retrieved);
            if (!status.ok()) {
                return {"Large Dataset", false, "Get failed for " + std::to_string(key), 0};
            }
            if (retrieved != expected_value) {
                return {"Large Dataset", false, "Value mismatch at " + std::to_string(i), 0};
            }
        }
    }

    lsm->Shutdown();

    auto end = std::chrono::high_resolution_clock::now();
    double duration = std::chrono::duration<double, std::milli>(end - start).count();
    return {"Large Dataset (" + std::to_string(config.large_test_keys) + " keys)", true, "", duration};
}

TestResult TestContains(const TestConfig& config) {
    auto start = std::chrono::high_resolution_clock::now();
    std::string test_path = config.test_path + "/contains";

    auto lsm = CreateTestLSM(test_path);
    if (!lsm) {
        return {"Contains", false, "Failed to create LSM tree", 0};
    }

    // Insert some keys
    for (uint64_t i = 0; i < 10; ++i) {
        auto status = lsm->Put(i, "value_" + std::to_string(i));
        if (!status.ok()) {
            return {"Contains", false, "Put failed", 0};
        }
    }

    // Check contains for existing keys
    for (uint64_t i = 0; i < 10; ++i) {
        if (!lsm->Contains(i)) {
            return {"Contains", false, "Contains returned false for existing key " + std::to_string(i), 0};
        }
    }

    // Check contains for non-existing keys
    for (uint64_t i = 100; i < 110; ++i) {
        if (lsm->Contains(i)) {
            return {"Contains", false, "Contains returned true for non-existing key " + std::to_string(i), 0};
        }
    }

    lsm->Shutdown();

    auto end = std::chrono::high_resolution_clock::now();
    double duration = std::chrono::duration<double, std::milli>(end - start).count();
    return {"Contains", true, "", duration};
}

TestResult TestMultiGet(const TestConfig& config) {
    auto start = std::chrono::high_resolution_clock::now();
    std::string test_path = config.test_path + "/multiget";

    auto lsm = CreateTestLSM(test_path);
    if (!lsm) {
        return {"MultiGet", false, "Failed to create LSM tree", 0};
    }

    std::mt19937 gen(42);

    // Insert keys
    std::vector<uint64_t> keys;
    std::vector<std::string> expected_values;
    for (size_t i = 0; i < config.small_test_keys; ++i) {
        uint64_t key = i;
        std::string value = GenerateRandomValue(config.value_size, gen);
        keys.push_back(key);
        expected_values.push_back(value);
        auto status = lsm->Put(key, value);
        if (!status.ok()) {
            return {"MultiGet", false, "Put failed", 0};
        }
    }

    // MultiGet all keys
    std::vector<std::string> values;
    std::vector<marble::Status> statuses;
    auto status = lsm->MultiGet(keys, &values, &statuses);
    if (!status.ok()) {
        return {"MultiGet", false, "MultiGet failed: " + status.ToString(), 0};
    }

    // Verify results
    if (values.size() != keys.size()) {
        return {"MultiGet", false, "Values size mismatch", 0};
    }

    for (size_t i = 0; i < keys.size(); ++i) {
        if (!statuses[i].ok()) {
            return {"MultiGet", false, "Status not OK for key " + std::to_string(keys[i]), 0};
        }
        if (values[i] != expected_values[i]) {
            return {"MultiGet", false, "Value mismatch for key " + std::to_string(keys[i]), 0};
        }
    }

    lsm->Shutdown();

    auto end = std::chrono::high_resolution_clock::now();
    double duration = std::chrono::duration<double, std::milli>(end - start).count();
    return {"MultiGet (" + std::to_string(config.small_test_keys) + " keys)", true, "", duration};
}

TestResult TestDeleteRange(const TestConfig& config) {
    auto start = std::chrono::high_resolution_clock::now();
    std::string test_path = config.test_path + "/delete_range";

    auto lsm = CreateTestLSM(test_path);
    if (!lsm) {
        return {"Delete Range", false, "Failed to create LSM tree", 0};
    }

    // Insert 100 keys (0-99)
    for (uint64_t i = 0; i < 100; ++i) {
        auto status = lsm->Put(i, "value_" + std::to_string(i));
        if (!status.ok()) {
            return {"Delete Range", false, "Put failed", 0};
        }
    }

    // Delete range 30-60
    size_t deleted_count = 0;
    auto status = lsm->DeleteRange(30, 60, &deleted_count);
    if (!status.ok()) {
        return {"Delete Range", false, "DeleteRange failed: " + status.ToString(), 0};
    }

    // Verify deleted keys
    for (uint64_t i = 30; i <= 60; ++i) {
        std::string value;
        status = lsm->Get(i, &value);
        if (!status.IsNotFound()) {
            return {"Delete Range", false, "Key " + std::to_string(i) +
                    " should be deleted but got: " + status.ToString(), 0};
        }
    }

    // Verify remaining keys
    for (uint64_t i = 0; i < 30; ++i) {
        std::string value;
        status = lsm->Get(i, &value);
        if (!status.ok()) {
            return {"Delete Range", false, "Key " + std::to_string(i) +
                    " should exist but got: " + status.ToString(), 0};
        }
    }
    for (uint64_t i = 61; i < 100; ++i) {
        std::string value;
        status = lsm->Get(i, &value);
        if (!status.ok()) {
            return {"Delete Range", false, "Key " + std::to_string(i) +
                    " should exist but got: " + status.ToString(), 0};
        }
    }

    lsm->Shutdown();

    auto end = std::chrono::high_resolution_clock::now();
    double duration = std::chrono::duration<double, std::milli>(end - start).count();
    return {"Delete Range", true, "", duration};
}

//==============================================================================
// Main
//==============================================================================

int main(int argc, char** argv) {
    TestConfig config;

    // Parse command line args
    for (int i = 1; i < argc; ++i) {
        std::string arg = argv[i];
        if (arg == "--small-keys" && i + 1 < argc) {
            config.small_test_keys = std::stoull(argv[++i]);
        } else if (arg == "--medium-keys" && i + 1 < argc) {
            config.medium_test_keys = std::stoull(argv[++i]);
        } else if (arg == "--large-keys" && i + 1 < argc) {
            config.large_test_keys = std::stoull(argv[++i]);
        } else if (arg == "--help") {
            std::cout << "Usage: " << argv[0] << " [options]" << std::endl;
            std::cout << "  --small-keys N   Small test key count (default: 100)" << std::endl;
            std::cout << "  --medium-keys N  Medium test key count (default: 1000)" << std::endl;
            std::cout << "  --large-keys N   Large test key count (default: 10000)" << std::endl;
            return 0;
        }
    }

    std::cout << "\n";
    std::cout << "============================================================" << std::endl;
    std::cout << "         MarbleDB Standalone Correctness Test               " << std::endl;
    std::cout << "============================================================" << std::endl;
    std::cout << "Small keys:  " << config.small_test_keys << std::endl;
    std::cout << "Medium keys: " << config.medium_test_keys << std::endl;
    std::cout << "Large keys:  " << config.large_test_keys << std::endl;
    std::cout << "Value size:  " << config.value_size << " bytes" << std::endl;
    std::cout << "============================================================" << std::endl;

    TestSuite suite;

    std::cout << "\nRunning tests..." << std::endl;

    // Basic tests
    std::cout << "  Basic Put/Get..." << std::flush;
    suite.AddResult(TestBasicPutGet(config));
    std::cout << " done." << std::endl;

    std::cout << "  Multiple Put/Get..." << std::flush;
    suite.AddResult(TestMultiplePutGet(config));
    std::cout << " done." << std::endl;

    std::cout << "  Overwrite..." << std::flush;
    suite.AddResult(TestOverwrite(config));
    std::cout << " done." << std::endl;

    std::cout << "  Delete..." << std::flush;
    suite.AddResult(TestDelete(config));
    std::cout << " done." << std::endl;

    std::cout << "  Not Found..." << std::flush;
    suite.AddResult(TestNotFound(config));
    std::cout << " done." << std::endl;

    // Edge case tests
    std::cout << "  Empty Value..." << std::flush;
    suite.AddResult(TestEmptyValue(config));
    std::cout << " done." << std::endl;

    std::cout << "  Large Value..." << std::flush;
    suite.AddResult(TestLargeValue(config));
    std::cout << " done." << std::endl;

    std::cout << "  Contains..." << std::flush;
    suite.AddResult(TestContains(config));
    std::cout << " done." << std::endl;

    // Scan tests
    std::cout << "  Scan..." << std::flush;
    suite.AddResult(TestScan(config));
    std::cout << " done." << std::endl;

    std::cout << "  Scan Order..." << std::flush;
    suite.AddResult(TestScanOrder(config));
    std::cout << " done." << std::endl;

    // Batch tests
    std::cout << "  MultiGet..." << std::flush;
    suite.AddResult(TestMultiGet(config));
    std::cout << " done." << std::endl;

    std::cout << "  Delete Range..." << std::flush;
    suite.AddResult(TestDeleteRange(config));
    std::cout << " done." << std::endl;

    // Scale tests
    std::cout << "  Medium Dataset..." << std::flush;
    suite.AddResult(TestMediumDataset(config));
    std::cout << " done." << std::endl;

    std::cout << "  Large Dataset..." << std::flush;
    suite.AddResult(TestLargeDataset(config));
    std::cout << " done." << std::endl;

    // Print summary
    suite.PrintSummary();

    // Cleanup
    fs::remove_all(config.test_path);

    return suite.AllPassed() ? 0 : 1;
}
