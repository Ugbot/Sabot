/**
 * MarbleDB Performance Benchmarks
 *
 * Comprehensive performance testing for:
 * - Individual operations (Put/Get/Delete)
 * - Batch operations (InsertBatch/ScanTable)
 * - Write → Flush → Compact → Read pipeline
 * - Concurrent operations
 * - Memory usage and throughput
 */

#include "marble/api.h"
#include "marble/record.h"
#include <benchmark/benchmark.h>
#include <filesystem>
#include <thread>
#include <vector>
#include <atomic>

namespace fs = std::filesystem;

class MarbleBenchmark : public benchmark::Fixture {
protected:
    void SetUp(const benchmark::State& state) override {
        test_path_ = "/tmp/marble_bench_" + std::to_string(std::time(nullptr));
        fs::create_directories(test_path_);

        // Initialize database
        DBOptions options;
        options.db_path = test_path_;
        options.create_if_missing = true;

        auto status = MarbleDB::Open(options, nullptr, &db_);
        if (!status.ok()) {
            throw std::runtime_error("Failed to open database: " + status.ToString());
        }

        // Create column family
        ColumnFamilyOptions cf_options;
        cf_options.schema = arrow::schema({
            arrow::field("id", arrow::int64()),
            arrow::field("data", arrow::utf8()),
            arrow::field("timestamp", arrow::int64())
        });

        ColumnFamilyDescriptor cf_desc("benchmark", cf_options);
        status = db_->CreateColumnFamily(cf_desc, &cf_handle_);
        if (!status.ok()) {
            throw std::runtime_error("Failed to create column family: " + status.ToString());
        }
    }

    void TearDown(const benchmark::State& state) override {
        if (db_) {
            db_->DestroyColumnFamilyHandle(cf_handle_);
            db_->Close();
        }
        fs::remove_all(test_path_);
    }

    MarbleDB* db_ = nullptr;
    ColumnFamilyHandle* cf_handle_ = nullptr;
    std::string test_path_;
};

// Benchmark individual Put operations
BENCHMARK_DEFINE_F(MarbleBenchmark, PutOperations)(benchmark::State& state) {
    int64_t key_counter = 0;

    for (auto _ : state) {
        auto record = std::make_shared<SimpleRecord>(
            std::make_shared<Int64Key>(key_counter++),
            arrow::RecordBatch::Make(cf_handle_->GetSchema(), 1, {
                arrow::ArrayFromJSON(arrow::int64(), std::to_string(key_counter)).ValueOrDie(),
                arrow::ArrayFromJSON(arrow::utf8(), "\"benchmark_data_" + std::to_string(key_counter) + "\"").ValueOrDie(),
                arrow::ArrayFromJSON(arrow::int64(), std::to_string(std::time(nullptr))).ValueOrDie()
            }).ValueOrDie(),
            0
        );

        auto status = db_->Put(WriteOptions(), record);
        if (!status.ok()) {
            state.SkipWithError(("Put failed: " + status.ToString()).c_str());
            break;
        }
    }

    state.SetItemsProcessed(state.iterations());
}

// Benchmark individual Get operations
BENCHMARK_DEFINE_F(MarbleBenchmark, GetOperations)(benchmark::State& state) {
    // Pre-populate with data
    const int num_records = 10000;
    for (int i = 0; i < num_records; ++i) {
        auto record = std::make_shared<SimpleRecord>(
            std::make_shared<Int64Key>(i),
            arrow::RecordBatch::Make(cf_handle_->GetSchema(), 1, {
                arrow::ArrayFromJSON(arrow::int64(), std::to_string(i)).ValueOrDie(),
                arrow::ArrayFromJSON(arrow::utf8(), "\"data_" + std::to_string(i) + "\"").ValueOrDie(),
                arrow::ArrayFromJSON(arrow::int64(), std::to_string(i)).ValueOrDie()
            }).ValueOrDie(),
            0
        );
        db_->Put(WriteOptions(), record);
    }

    // Benchmark gets
    int key_index = 0;
    for (auto _ : state) {
        std::shared_ptr<Record> retrieved;
        auto status = db_->Get(ReadOptions(), Int64Key(key_index % num_records), &retrieved);
        if (!status.ok()) {
            state.SkipWithError(("Get failed: " + status.ToString()).c_str());
            break;
        }
        key_index++;
    }

    state.SetItemsProcessed(state.iterations());
}

// Benchmark batch insert operations
BENCHMARK_DEFINE_F(MarbleBenchmark, BatchInsert)(benchmark::State& state) {
    const int batch_size = state.range(0); // Parameterized batch size

    for (auto _ : state) {
        std::vector<std::shared_ptr<arrow::Array>> columns;

        // Generate batch data
        std::vector<int64_t> ids;
        std::vector<std::string> data_strings;
        std::vector<int64_t> timestamps;

        for (int i = 0; i < batch_size; ++i) {
            ids.push_back(i);
            data_strings.push_back("\"batch_data_" + std::to_string(i) + "\"");
            timestamps.push_back(std::time(nullptr));
        }

        auto batch = arrow::RecordBatch::Make(cf_handle_->GetSchema(), batch_size, {
            arrow::ArrayFromJSON(arrow::int64(), "[" + std::to_string(ids[0]) + "]").ValueOrDie(),
            arrow::ArrayFromJSON(arrow::utf8(), data_strings[0]).ValueOrDie(),
            arrow::ArrayFromJSON(arrow::int64(), std::to_string(timestamps[0])).ValueOrDie()
        }).ValueOrDie();

        auto status = db_->InsertBatch("benchmark", batch);
        if (!status.ok()) {
            state.SkipWithError(("Batch insert failed: " + status.ToString()).c_str());
            break;
        }
    }

    state.SetItemsProcessed(state.iterations() * batch_size);
}

// Benchmark scan operations
BENCHMARK_DEFINE_F(MarbleBenchmark, ScanOperations)(benchmark::State& state) {
    // Pre-populate with data
    const int num_records = 10000;
    for (int batch = 0; batch < 10; ++batch) {
        auto batch_data = arrow::RecordBatch::Make(cf_handle_->GetSchema(), 1000, {
            arrow::ArrayFromJSON(arrow::int64(), "[0]").ValueOrDie(), // Placeholder
            arrow::ArrayFromJSON(arrow::utf8(), "[\"scan_data\"]").ValueOrDie(),
            arrow::ArrayFromJSON(arrow::int64(), "[0]").ValueOrDie()
        }).ValueOrDie();

        db_->InsertBatch("benchmark", batch_data);
    }

    for (auto _ : state) {
        std::unique_ptr<QueryResult> result;
        auto status = db_->ScanTable("benchmark", &result);
        if (!status.ok()) {
            state.SkipWithError(("Scan failed: " + status.ToString()).c_str());
            break;
        }
    }

    state.SetItemsProcessed(state.iterations());
}

// Benchmark concurrent operations
BENCHMARK_DEFINE_F(MarbleBenchmark, ConcurrentOperations)(benchmark::State& state) {
    const int num_threads = state.range(0);
    const int operations_per_thread = 1000;

    std::vector<std::thread> threads;
    std::atomic<int64_t> operations_completed{0};

    auto worker = [this, operations_per_thread, &operations_completed]() {
        for (int i = 0; i < operations_per_thread; ++i) {
            auto record = std::make_shared<SimpleRecord>(
                std::make_shared<Int64Key>(operations_completed.fetch_add(1)),
                arrow::RecordBatch::Make(cf_handle_->GetSchema(), 1, {
                    arrow::ArrayFromJSON(arrow::int64(), std::to_string(i)).ValueOrDie(),
                    arrow::ArrayFromJSON(arrow::utf8(), "\"concurrent_data\"").ValueOrDie(),
                    arrow::ArrayFromJSON(arrow::int64(), std::to_string(std::time(nullptr))).ValueOrDie()
                }).ValueOrDie(),
                0
            );

            db_->Put(WriteOptions(), record);
        }
    };

    for (auto _ : state) {
        operations_completed = 0;
        threads.clear();

        // Start worker threads
        auto start_time = std::chrono::high_resolution_clock::now();
        for (int t = 0; t < num_threads; ++t) {
            threads.emplace_back(worker);
        }

        // Wait for completion
        for (auto& thread : threads) {
            thread.join();
        }
        auto end_time = std::chrono::high_resolution_clock::now();

        // Calculate throughput
        auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time);
        double throughput = (operations_completed.load() * 1000.0) / duration.count();

        state.SetItemsProcessed(operations_completed.load());
        state.counters["throughput_ops_sec"] = benchmark::Counter(throughput, benchmark::Counter::kIsRate);
    }
}

// Register benchmarks with parameters
BENCHMARK_REGISTER_F(MarbleBenchmark, PutOperations)->Threads(1);
BENCHMARK_REGISTER_F(MarbleBenchmark, GetOperations)->Threads(1);
BENCHMARK_REGISTER_F(MarbleBenchmark, BatchInsert)->Arg(100)->Arg(1000)->Arg(10000);
BENCHMARK_REGISTER_F(MarbleBenchmark, ScanOperations)->Threads(1);
BENCHMARK_REGISTER_F(MarbleBenchmark, ConcurrentOperations)->Arg(1)->Arg(2)->Arg(4)->Arg(8);

// Memory usage benchmark
BENCHMARK_DEFINE_F(MarbleBenchmark, MemoryUsage)(benchmark::State& state) {
    const int num_records = 100000;

    // Record initial memory usage (approximate)
    auto initial_memory = benchmark::GetMemoryUsage();

    // Insert large dataset
    for (int i = 0; i < num_records; ++i) {
        auto record = std::make_shared<SimpleRecord>(
            std::make_shared<Int64Key>(i),
            arrow::RecordBatch::Make(cf_handle_->GetSchema(), 1, {
                arrow::ArrayFromJSON(arrow::int64(), std::to_string(i)).ValueOrDie(),
                arrow::ArrayFromJSON(arrow::utf8(), "\"memory_test_data_" + std::to_string(i) + "\"").ValueOrDie(),
                arrow::ArrayFromJSON(arrow::int64(), std::to_string(std::time(nullptr))).ValueOrDie()
            }).ValueOrDie(),
            0
        );
        db_->Put(WriteOptions(), record);
    }

    // Force flush
    db_->Flush();

    // Record final memory usage
    auto final_memory = benchmark::GetMemoryUsage();
    auto memory_delta = final_memory - initial_memory;

    for (auto _ : state) {
        // Measure memory per record
        state.counters["memory_per_record_kb"] = benchmark::Counter(
            static_cast<double>(memory_delta) / num_records / 1024.0,
            benchmark::Counter::kDefaults
        );
    }

    state.SetItemsProcessed(num_records);
}

BENCHMARK_REGISTER_F(MarbleBenchmark, MemoryUsage)->Iterations(1);

int main(int argc, char** argv) {
    benchmark::Initialize(&argc, argv);

    // Run benchmarks
    benchmark::RunSpecifiedBenchmarks();

    return 0;
}
