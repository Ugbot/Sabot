#pragma once

#include <vector>
#include <string>
#include <memory>
#include <chrono>
#include <random>
#include <iostream>
#include <iomanip>
#include <marble/marble.h>

namespace marble_bench {

// Benchmark record structure
struct BenchRecord {
    int64_t id;
    std::string name;
    std::string value;
    int32_t category;

    BenchRecord() = default;
    BenchRecord(int64_t id, std::string name, std::string value, int32_t category)
        : id(id), name(std::move(name)), value(std::move(value)), category(category) {}
};

// Concrete implementation of marble interfaces for benchmarking
class BenchKey : public marble::Key {
public:
    explicit BenchKey(int64_t id) : id_(id) {}
    explicit BenchKey(const std::string& str) : id_(std::stoll(str)) {}

    int Compare(const marble::Key& other) const override {
        const auto& other_key = static_cast<const BenchKey&>(other);
        if (id_ < other_key.id_) return -1;
        if (id_ > other_key.id_) return 1;
        return 0;
    }

    std::shared_ptr<marble::Key> Clone() const override {
        return std::make_shared<BenchKey>(id_);
    }

    std::string ToString() const override {
        return std::to_string(id_);
    }

    size_t Hash() const override {
        return std::hash<int64_t>()(id_);
    }

private:
    int64_t id_;
};

class BenchRecordImpl : public marble::Record {
public:
    explicit BenchRecordImpl(BenchRecord record) : record_(std::move(record)) {}

    std::shared_ptr<marble::Key> GetKey() const override {
        return std::make_shared<BenchKey>(record_.id);
    }

    std::shared_ptr<arrow::Schema> GetArrowSchema() const override {
        return arrow::schema({
            arrow::field("id", arrow::int64()),
            arrow::field("name", arrow::utf8()),
            arrow::field("value", arrow::utf8()),
            arrow::field("category", arrow::int32())
        });
    }

    arrow::Result<std::shared_ptr<arrow::RecordBatch>> ToRecordBatch() const override {
        arrow::Int64Builder id_builder;
        arrow::StringBuilder name_builder;
        arrow::StringBuilder value_builder;
        arrow::Int32Builder category_builder;

        id_builder.Append(record_.id).ok();
        name_builder.Append(record_.name).ok();
        value_builder.Append(record_.value).ok();
        category_builder.Append(record_.category).ok();

        std::shared_ptr<arrow::Array> id_array, name_array, value_array, category_array;
        id_builder.Finish(&id_array).ok();
        name_builder.Finish(&name_array).ok();
        value_builder.Finish(&value_array).ok();
        category_builder.Finish(&category_array).ok();

        return arrow::RecordBatch::Make(GetArrowSchema(), 1,
            {id_array, name_array, value_array, category_array});
    }

    size_t Size() const override {
        return sizeof(BenchRecord) + record_.name.size() + record_.value.size();
    }

private:
    BenchRecord record_;
};

class BenchSchema : public marble::Schema {
public:
    std::shared_ptr<arrow::Schema> GetArrowSchema() const override {
        return arrow::schema({
            arrow::field("id", arrow::int64()),
            arrow::field("name", arrow::utf8()),
            arrow::field("value", arrow::utf8()),
            arrow::field("category", arrow::int32())
        });
    }

    const std::vector<size_t>& GetPrimaryKeyIndices() const override {
        static std::vector<size_t> indices = {0};
        return indices;
    }

    arrow::Result<std::shared_ptr<marble::Record>> RecordFromRow(
        const std::shared_ptr<arrow::RecordBatch>& batch,
        size_t row_index) const override {
        auto id_array = std::static_pointer_cast<arrow::Int64Array>(batch->column(0));
        auto name_array = std::static_pointer_cast<arrow::StringArray>(batch->column(1));
        auto value_array = std::static_pointer_cast<arrow::StringArray>(batch->column(2));
        auto category_array = std::static_pointer_cast<arrow::Int32Array>(batch->column(3));

        BenchRecord record;
        record.id = id_array->Value(row_index);
        record.name = name_array->GetString(row_index);
        record.value = value_array->GetString(row_index);
        record.category = category_array->Value(row_index);

        return std::make_shared<BenchRecordImpl>(record);
    }

    arrow::Result<std::shared_ptr<marble::Key>> KeyFromScalar(
        const std::shared_ptr<arrow::Scalar>& scalar) const override {
        if (scalar->type->id() == arrow::Type::INT64) {
            auto int_scalar = std::static_pointer_cast<arrow::Int64Scalar>(scalar);
            return std::make_shared<BenchKey>(int_scalar->value);
        }
        return arrow::Status::Invalid("Unsupported scalar type for key");
    }
};

// Benchmark utilities
class BenchTimer {
public:
    BenchTimer() : start_(std::chrono::high_resolution_clock::now()) {}

    void reset() {
        start_ = std::chrono::high_resolution_clock::now();
    }

    double elapsed_seconds() const {
        auto end = std::chrono::high_resolution_clock::now();
        return std::chrono::duration<double>(end - start_).count();
    }

    double elapsed_milliseconds() const {
        return elapsed_seconds() * 1000.0;
    }

    double elapsed_microseconds() const {
        return elapsed_seconds() * 1000000.0;
    }

private:
    std::chrono::high_resolution_clock::time_point start_;
};

// Random data generation
class BenchDataGenerator {
public:
    BenchDataGenerator() : rng_(std::random_device{}()), dist_(0, 999999) {}

    BenchRecord generate_record(int64_t id) {
        return BenchRecord{
            id,
            "name_" + std::to_string(dist_(rng_)),
            "value_" + std::to_string(dist_(rng_)),
            static_cast<int32_t>(dist_(rng_) % 100)
        };
    }

    std::vector<BenchRecord> generate_records(size_t count, int64_t start_id = 0) {
        std::vector<BenchRecord> records;
        records.reserve(count);
        for (size_t i = 0; i < count; ++i) {
            records.push_back(generate_record(start_id + i));
        }
        return records;
    }

private:
    std::mt19937 rng_;
    std::uniform_int_distribution<int> dist_;
};

// Benchmark result structure
struct BenchResult {
    std::string operation;
    size_t operations_count;
    double total_time_seconds;
    double ops_per_second;
    double avg_latency_us;

    void print() const {
        std::cout << std::fixed << std::setprecision(2)
                  << operation << ": "
                  << operations_count << " ops in "
                  << total_time_seconds << "s ("
                  << ops_per_second << " ops/sec, "
                  << avg_latency_us << " us/op)" << std::endl;
    }
};

class BenchmarkSuite {
public:
    template<typename Func>
    BenchResult run(const std::string& name, size_t iterations, Func&& func) {
        BenchTimer timer;

        for (size_t i = 0; i < iterations; ++i) {
            func();
        }

        double total_time = timer.elapsed_seconds();
        double ops_per_second = iterations / total_time;
        double avg_latency_us = (total_time * 1000000.0) / iterations;

        BenchResult result{name, iterations, total_time, ops_per_second, avg_latency_us};
        results_.push_back(result);
        return result;
    }

    void print_summary() const {
        std::cout << "\n=== Benchmark Results ===\n";
        for (const auto& result : results_) {
            result.print();
        }
    }

private:
    std::vector<BenchResult> results_;
};

// Common benchmark configurations
constexpr size_t SMALL_DATASET = 1000;
constexpr size_t MEDIUM_DATASET = 10000;
constexpr size_t LARGE_DATASET = 100000;
constexpr size_t XLARGE_DATASET = 1000000;

} // namespace marble_bench
