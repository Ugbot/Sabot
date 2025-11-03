/**
 * MarbleDB Batch Write Benchmark
 *
 * Tests bulk insert operations with Arrow RecordBatches
 * Modeled after Tonbo's write_bench.rs patterns
 */

#include "common.h"
#include <iostream>
#include <vector>

using namespace marble;
using namespace marble::bench;

class BatchWriteBenchmark : public MarbleTestBase {
public:
    BatchWriteBenchmark() : MarbleTestBase("batch_write") {}

    void RunAll() {
        std::cout << "\n========================================" << std::endl;
        std::cout << "  MarbleDB Batch Write Benchmark" << std::endl;
        std::cout << "========================================\n" << std::endl;

        // Create test table
        auto status = CreateTestTable("benchmark");
        if (!status.ok()) {
            std::cerr << "Failed to create table: " << status.message() << std::endl;
            return;
        }

        // Test 1: Varying batch sizes (single-threaded)
        std::cout << "\n--- Test 1: Varying Batch Sizes (Single-threaded) ---" << std::endl;
        TestVaryingBatchSizes();

        // Test 2: Large bulk load
        std::cout << "\n--- Test 2: Large Bulk Load ---" << std::endl;
        TestLargeBulkLoad();

        // Test 3: Multiple small batches
        std::cout << "\n--- Test 3: Multiple Small Batches ---" << std::endl;
        TestManySmallBatches();

        std::cout << "\n========================================" << std::endl;
        std::cout << "  Benchmark Complete" << std::endl;
        std::cout << "========================================\n" << std::endl;
    }

private:
    void TestVaryingBatchSizes() {
        std::vector<size_t> batch_sizes = {100, 1000, 10000};

        for (auto batch_size : batch_sizes) {
            std::string name = "BatchWrite_" + std::to_string(batch_size) + "_rows";
            size_t num_batches = 100;

            BenchmarkRunner::RunAndReport(
                name,
                [this, batch_size, num_batches]() -> Status {
                    return WriteBatches(num_batches, batch_size);
                },
                num_batches,
                num_batches * batch_size
            );
        }
    }

    void TestLargeBulkLoad() {
        size_t batch_size = 10000;
        size_t num_batches = 100; // 1M rows total

        BenchmarkRunner::RunAndReport(
            "BulkLoad_1M_rows",
            [this, batch_size, num_batches]() -> Status {
                return WriteBatches(num_batches, batch_size);
            },
            num_batches,
            num_batches * batch_size
        );
    }

    void TestManySmallBatches() {
        size_t batch_size = 10;
        size_t num_batches = 10000; // 100K rows total

        BenchmarkRunner::RunAndReport(
            "ManySmallBatches_100K_rows",
            [this, batch_size, num_batches]() -> Status {
                return WriteBatches(num_batches, batch_size);
            },
            num_batches,
            num_batches * batch_size
        );
    }

    Status WriteBatches(size_t num_batches, size_t batch_size) {
        for (size_t i = 0; i < num_batches; ++i) {
            // Generate random batch
            auto batch = ArrowBatchBuilder::CreateTestBatch(batch_size, rng());
            if (!batch) {
                return Status::InternalError("Failed to create test batch");
            }

            // Insert batch
            auto status = InsertBatch("benchmark", batch);
            if (!status.ok()) {
                return status;
            }
        }

        return Status::OK();
    }
};

int main(int argc, char** argv) {
    std::string db_path = "/tmp/marble_batch_write_bench";
    bool clean = true;

    // Parse simple CLI args
    for (int i = 1; i < argc; ++i) {
        std::string arg(argv[i]);
        if (arg == "--path" && i + 1 < argc) {
            db_path = argv[++i];
        }
        else if (arg == "--no-clean") {
            clean = false;
        }
        else if (arg == "--help" || arg == "-h") {
            std::cout << "MarbleDB Batch Write Benchmark\n";
            std::cout << "Usage: " << argv[0] << " [options]\n";
            std::cout << "Options:\n";
            std::cout << "  --path PATH     Database path (default: /tmp/marble_batch_write_bench)\n";
            std::cout << "  --no-clean      Don't clean database on start\n";
            std::cout << "  --help, -h      Show this help\n";
            return 0;
        }
    }

    // Create and run benchmark
    BatchWriteBenchmark bench;
    bench.Initialize(clean);
    bench.RunAll();
    bench.Cleanup();

    return 0;
}
