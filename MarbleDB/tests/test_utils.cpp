/**
 * Test Utilities Implementation
 *
 * Implementation of test utilities for MarbleDB testing framework.
 */

#include "test_utils.h"
#include <arrow/io/api.h>
#include <arrow/ipc/api.h>
#include <algorithm>
#include <sstream>

namespace marble {
namespace test {

//==============================================================================
// BenchmarkRunner::BenchmarkResult Implementation
//==============================================================================

void BenchmarkRunner::BenchmarkResult::Print() const {
    std::cout << "Benchmark: " << name << std::endl;
    std::cout << "  Operations: " << operations << std::endl;
    std::cout << "  Time: " << elapsed_seconds << " seconds" << std::endl;
    std::cout << "  Throughput: " << throughput << " ops/sec" << std::endl;
    std::cout << "  Latency: " << (elapsed_seconds / operations * 1000) << " ms/op" << std::endl;
    std::cout << std::endl;
}

//==============================================================================
// TestDataGenerator Implementation
//==============================================================================

std::shared_ptr<arrow::RecordBatch> TestDataGenerator::GenerateBatch(int64_t num_rows) {
    std::vector<std::shared_ptr<arrow::Array>> arrays;
    arrays.reserve(schema_->num_fields());

    for (int i = 0; i < schema_->num_fields(); ++i) {
        auto field = schema_->field(i);
        auto type = field->type();

        if (type->Equals(arrow::int64())) {
            arrow::Int64Builder builder;
            for (int64_t j = 0; j < num_rows; ++j) {
                builder.Append(int64_dist_(gen_));
            }
            std::shared_ptr<arrow::Array> array;
            builder.Finish(&array);
            arrays.push_back(array);
        } else if (type->Equals(arrow::float64())) {
            arrow::DoubleBuilder builder;
            for (int64_t j = 0; j < num_rows; ++j) {
                builder.Append(double_dist_(gen_));
            }
            std::shared_ptr<arrow::Array> array;
            builder.Finish(&array);
            arrays.push_back(array);
        } else if (type->Equals(arrow::utf8())) {
            arrow::StringBuilder builder;
            for (int64_t j = 0; j < num_rows; ++j) {
                int length = string_len_dist_(gen_);
                builder.Append(GenerateRandomString(length));
            }
            std::shared_ptr<arrow::Array> array;
            builder.Finish(&array);
            arrays.push_back(array);
        } else if (type->Equals(arrow::timestamp(arrow::TimeUnit::MICRO))) {
            arrow::TimestampBuilder builder(arrow::timestamp(arrow::TimeUnit::MICRO), arrow::default_memory_pool());
            for (int64_t j = 0; j < num_rows; ++j) {
                builder.Append(GenerateTimestamp());
            }
            std::shared_ptr<arrow::Array> array;
            builder.Finish(&array);
            arrays.push_back(array);
        } else {
            // For unsupported types, create null array
            auto array = arrow::MakeArrayOfNull(type, num_rows).ValueOrDie();
            arrays.push_back(array);
        }
    }

    return arrow::RecordBatch::Make(schema_, num_rows, arrays);
}

std::vector<std::shared_ptr<arrow::RecordBatch>> TestDataGenerator::GenerateBatches(
    int64_t num_batches, int64_t rows_per_batch) {

    std::vector<std::shared_ptr<arrow::RecordBatch>> batches;
    batches.reserve(num_batches);

    for (int64_t i = 0; i < num_batches; ++i) {
        batches.push_back(GenerateBatch(rows_per_batch));
    }

    return batches;
}

std::string TestDataGenerator::GenerateRandomString(int length) {
    static const char charset[] = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
    std::string result;
    result.reserve(length);

    for (int i = 0; i < length; ++i) {
        result += charset[choice_dist_(gen_) % (sizeof(charset) - 1)];
    }

    return result;
}

int64_t TestDataGenerator::GenerateTimestamp() {
    // Generate timestamps in the last year
    auto now = std::chrono::system_clock::now();
    auto one_year_ago = now - std::chrono::hours(24 * 365);
    auto random_duration = std::chrono::seconds(choice_dist_(gen_) * 86400); // Random day

    auto timestamp = one_year_ago + random_duration;
    return std::chrono::duration_cast<std::chrono::microseconds>(
        timestamp.time_since_epoch()).count();
}

//==============================================================================
// PatternedDataGenerator Implementation
//==============================================================================

std::shared_ptr<arrow::RecordBatch> PatternedDataGenerator::GeneratePatternedBatch(
    int64_t num_rows, const std::string& pattern) {

    if (pattern == "uniform") {
        return GenerateBatch(num_rows);
    } else if (pattern == "clustered") {
        // Create clustered data (groups of similar values)
        // Implementation would create clusters of similar data
        return GenerateBatch(num_rows);
    } else if (pattern == "skewed") {
        // Create skewed data (power-law distribution)
        // Implementation would create skewed distributions
        return GenerateBatch(num_rows);
    }

    // Default to uniform
    return GenerateBatch(num_rows);
}

std::shared_ptr<arrow::RecordBatch> PatternedDataGenerator::GenerateMatchingBatch(
    int64_t num_rows, const std::vector<std::pair<std::string, std::string>>& match_patterns) {

    // Generate data that matches specific patterns for testing predicates
    // This is a simplified implementation
    return GenerateBatch(num_rows);
}

//==============================================================================
// RecordBatchValidator Implementation
//==============================================================================

bool RecordBatchValidator::ValidateSchema(
    const std::shared_ptr<arrow::RecordBatch>& batch,
    const std::shared_ptr<arrow::Schema>& expected_schema) {

    if (!batch || !expected_schema) {
        return false;
    }

    return batch->schema()->Equals(expected_schema);
}

bool RecordBatchValidator::ValidateRowCount(
    const std::shared_ptr<arrow::RecordBatch>& batch, int64_t expected_rows) {

    if (!batch) {
        return false;
    }

    return batch->num_rows() == expected_rows;
}

bool RecordBatchValidator::ValidateColumnValues(
    const std::shared_ptr<arrow::RecordBatch>& batch,
    const std::string& column_name,
    const std::function<bool(const std::shared_ptr<arrow::Scalar>&)>& validator) {

    if (!batch) {
        return false;
    }

    auto column = batch->GetColumnByName(column_name);
    if (!column) {
        return false;
    }

    for (int64_t i = 0; i < batch->num_rows(); ++i) {
        auto scalar = arrow::MakeScalar(column->GetScalar(i).ValueOrDie()).ValueOrDie();
        if (!validator(scalar)) {
            return false;
        }
    }

    return true;
}

bool RecordBatchValidator::ValidateNoNulls(
    const std::shared_ptr<arrow::RecordBatch>& batch,
    const std::string& column_name) {

    if (!batch) {
        return false;
    }

    auto column = batch->GetColumnByName(column_name);
    if (!column) {
        return false;
    }

    // Check for nulls in the column
    auto null_count = arrow::compute::CountNull(column).ValueOrDie();
    return null_count->ViewAs<arrow::Int64Scalar>()->value == 0;
}

bool RecordBatchValidator::ValidateUniqueValues(
    const std::shared_ptr<arrow::RecordBatch>& batch,
    const std::string& column_name) {

    if (!batch) {
        return false;
    }

    auto column = batch->GetColumnByName(column_name);
    if (!column) {
        return false;
    }

    // Use Arrow's unique function to check for duplicates
    auto unique_result = arrow::compute::Unique(column).ValueOrDie();
    return unique_result->length() == batch->num_rows();
}

//==============================================================================
// QueryResultValidator Implementation
//==============================================================================

bool QueryResultValidator::ValidateResultCount(
    int64_t actual_count, int64_t expected_count, double tolerance) {

    if (tolerance == 0.0) {
        return actual_count == expected_count;
    }

    double diff = std::abs(static_cast<double>(actual_count) - expected_count);
    return diff <= (expected_count * tolerance);
}

bool QueryResultValidator::ValidateColumnProjection(
    const std::shared_ptr<arrow::RecordBatch>& batch,
    const std::vector<std::string>& expected_columns) {

    if (!batch) {
        return false;
    }

    if (batch->num_columns() != static_cast<int64_t>(expected_columns.size())) {
        return false;
    }

    for (const auto& col_name : expected_columns) {
        if (batch->GetColumnByName(col_name) == nullptr) {
            return false;
        }
    }

    return true;
}

bool QueryResultValidator::ValidatePredicateApplication(
    const std::shared_ptr<arrow::RecordBatch>& batch,
    const std::string& column_name,
    const std::function<bool(const std::shared_ptr<arrow::Scalar>&)>& predicate) {

    return RecordBatchValidator::ValidateColumnValues(batch, column_name, predicate);
}

//==============================================================================
// TestSchemaFactory Implementation
//==============================================================================

std::shared_ptr<arrow::Schema> TestSchemaFactory::CreateEmployeeSchema() {
    return arrow::schema({
        arrow::field("id", arrow::int64()),
        arrow::field("name", arrow::utf8()),
        arrow::field("age", arrow::int64()),
        arrow::field("salary", arrow::float64()),
        arrow::field("department", arrow::utf8()),
        arrow::field("hire_date", arrow::timestamp(arrow::TimeUnit::MICRO))
    });
}

std::shared_ptr<arrow::Schema> TestSchemaFactory::CreateEcommerceSchema() {
    return arrow::schema({
        arrow::field("id", arrow::int64()),
        arrow::field("user_id", arrow::int64()),
        arrow::field("product_id", arrow::int64()),
        arrow::field("category", arrow::utf8()),
        arrow::field("price", arrow::float64()),
        arrow::field("quantity", arrow::int32()),
        arrow::field("timestamp", arrow::timestamp(arrow::TimeUnit::MICRO)),
        arrow::field("region", arrow::utf8()),
        arrow::field("discount", arrow::float32()),
        arrow::field("tax_rate", arrow::float64())
    });
}

std::shared_ptr<arrow::Schema> TestSchemaFactory::CreateTimeSeriesSchema() {
    return arrow::schema({
        arrow::field("timestamp", arrow::timestamp(arrow::TimeUnit::MICRO)),
        arrow::field("metric_name", arrow::utf8()),
        arrow::field("value", arrow::float64()),
        arrow::field("tags", arrow::utf8())
    });
}

std::shared_ptr<arrow::Schema> TestSchemaFactory::CreateSimpleSchema() {
    return arrow::schema({
        arrow::field("id", arrow::int64()),
        arrow::field("name", arrow::utf8()),
        arrow::field("value", arrow::float64())
    });
}

//==============================================================================
// TestPredicateFactory Implementation
//==============================================================================

std::vector<ColumnPredicate> TestPredicateFactory::CreateNumericPredicates(
    const std::string& column_name, double min_val, double max_val) {

    return {
        ColumnPredicate(column_name, PredicateType::kGreaterThan,
                       arrow::MakeScalar(min_val)),
        ColumnPredicate(column_name, PredicateType::kLessThan,
                       arrow::MakeScalar(max_val))
    };
}

std::vector<ColumnPredicate> TestPredicateFactory::CreateStringPredicates(
    const std::string& column_name, const std::vector<std::string>& values) {

    std::vector<ColumnPredicate> predicates;
    for (const auto& value : values) {
        predicates.emplace_back(column_name, PredicateType::kEqual,
                               arrow::MakeScalar(value));
    }
    return predicates;
}

std::vector<ColumnPredicate> TestPredicateFactory::CreateComplexPredicates() {
    return {
        ColumnPredicate("age", PredicateType::kGreaterThan, arrow::MakeScalar<int64_t>(25)),
        ColumnPredicate("salary", PredicateType::kGreaterThan, arrow::MakeScalar<double>(50000.0)),
        ColumnPredicate("department", PredicateType::kEqual, arrow::MakeScalar<std::string>("Engineering"))
    };
}

//==============================================================================
// TestDataPersistence Implementation
//==============================================================================

Status TestDataPersistence::SaveBatchToFile(
    const std::shared_ptr<arrow::RecordBatch>& batch, const std::string& filepath) {

    if (!batch) {
        return Status::InvalidArgument("Batch is null");
    }

    ARROW_ASSIGN_OR_RAISE(auto output_stream, arrow::io::FileOutputStream::Open(filepath));
    ARROW_ASSIGN_OR_RAISE(auto writer, arrow::ipc::MakeFileWriter(output_stream, batch->schema()));

    ARROW_RETURN_NOT_OK(writer->WriteRecordBatch(*batch));
    ARROW_RETURN_NOT_OK(writer->Close());

    return Status::OK();
}

Status TestDataPersistence::LoadBatchFromFile(
    const std::string& filepath, std::shared_ptr<arrow::RecordBatch>* batch) {

    ARROW_ASSIGN_OR_RAISE(auto input_stream, arrow::io::ReadableFile::Open(filepath));
    ARROW_ASSIGN_OR_RAISE(auto reader, arrow::ipc::OpenFile(input_stream));

    ARROW_ASSIGN_OR_RAISE(*batch, reader->ReadRecordBatch(0));

    return Status::OK();
}

Status TestDataPersistence::SaveTestData(
    const std::vector<std::shared_ptr<arrow::RecordBatch>>& batches,
    const std::string& base_path) {

    for (size_t i = 0; i < batches.size(); ++i) {
        std::string filepath = base_path + "/batch_" + std::to_string(i) + ".arrow";
        ARROW_RETURN_NOT_OK(SaveBatchToFile(batches[i], filepath));
    }

    return Status::OK();
}

Status TestDataPersistence::LoadTestData(
    const std::string& base_path,
    std::vector<std::shared_ptr<arrow::RecordBatch>>* batches) {

    batches->clear();

    // Find all batch files in the directory
    for (const auto& entry : fs::directory_iterator(base_path)) {
        if (entry.path().extension() == ".arrow") {
            std::shared_ptr<arrow::RecordBatch> batch;
            ARROW_RETURN_NOT_OK(LoadBatchFromFile(entry.path().string(), &batch));
            batches->push_back(batch);
        }
    }

    return Status::OK();
}

} // namespace test
} // namespace marble
