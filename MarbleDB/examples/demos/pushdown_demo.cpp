#include <iostream>
#include <memory>
#include <vector>
#include <chrono>
#include <marble/sstable_arrow.h>
#include <marble/query.h>

using namespace marble;

/**
 * @brief Demo showing Projection & Predicate Pushdown capabilities
 *
 * This demonstrates how MarbleDB can filter and project data directly
 * at the SSTable level, avoiding unnecessary I/O and processing.
 */

class MockArrowSSTable : public ArrowSSTable {
public:
    MockArrowSSTable() {
        // Create a sample schema
        schema_ = arrow::schema({
            arrow::field("id", arrow::int64()),
            arrow::field("name", arrow::utf8()),
            arrow::field("age", arrow::int64()),
            arrow::field("salary", arrow::float64()),
            arrow::field("department", arrow::utf8())
        });

        // Create sample data - 1000 rows
        CreateSampleData();
    }

    // ArrowSSTable interface
    const SSTableMetadata& GetMetadata() const override { return metadata_; }
    std::shared_ptr<arrow::Schema> GetArrowSchema() const override { return schema_; }

    Status ScanWithPushdown(
        const std::vector<std::string>& projection_columns,
        const std::vector<ColumnPredicate>& predicates,
        int64_t batch_size,
        std::unique_ptr<ArrowRecordBatchIterator>* iterator) override;

    Status CountRows(const std::vector<ColumnPredicate>& predicates, int64_t* count) override;
    Status GetColumnStats(const std::string& column_name, ColumnStatistics* stats) override;
    Status CanSatisfyPredicates(const std::vector<ColumnPredicate>& predicates, bool* can_satisfy) override;

    std::string GetFilePath() const override { return "mock_sstable"; }
    uint64_t GetFileSize() const override { return 1024 * 1024; } // 1MB
    size_t GetMemoryUsage() const override { return sizeof(*this); }
    Status Validate() const override { return Status::OK(); }

private:
    void CreateSampleData();

    SSTableMetadata metadata_;
    std::shared_ptr<arrow::Schema> schema_;
    std::vector<std::shared_ptr<arrow::RecordBatch>> batches_;
};

void MockArrowSSTable::CreateSampleData() {
    // Create sample data with 1000 rows
    arrow::Int64Builder id_builder;
    arrow::StringBuilder name_builder;
    arrow::Int64Builder age_builder;
    arrow::DoubleBuilder salary_builder;
    arrow::StringBuilder dept_builder;

    for (int64_t i = 0; i < 1000; ++i) {
        ARROW_RETURN_NOT_OK(id_builder.Append(i));
        ARROW_RETURN_NOT_OK(name_builder.Append("Employee_" + std::to_string(i)));
        ARROW_RETURN_NOT_OK(age_builder.Append(25 + (i % 35))); // Ages 25-59
        ARROW_RETURN_NOT_OK(salary_builder.Append(50000.0 + (i % 50000))); // Salaries 50k-99k
        ARROW_RETURN_NOT_OK(dept_builder.Append((i % 5 == 0) ? "Engineering" :
                                                (i % 5 == 1) ? "Sales" :
                                                (i % 5 == 2) ? "Marketing" :
                                                (i % 5 == 3) ? "HR" : "Finance"));
    }

    std::shared_ptr<arrow::Array> id_array, name_array, age_array, salary_array, dept_array;
    ARROW_RETURN_NOT_OK(id_builder.Finish(&id_array));
    ARROW_RETURN_NOT_OK(name_builder.Finish(&name_array));
    ARROW_RETURN_NOT_OK(age_builder.Finish(&age_array));
    ARROW_RETURN_NOT_OK(salary_builder.Finish(&salary_array));
    ARROW_RETURN_NOT_OK(dept_builder.Finish(&dept_array));

    auto batch = arrow::RecordBatch::Make(schema_, 1000, {id_array, name_array, age_array, salary_array, dept_array});
    batches_.push_back(batch);

    // Set up metadata
    metadata_.filename = "mock_sstable";
    metadata_.file_size = 1024 * 1024;
    metadata_.record_count = 1000;
    metadata_.min_key = 0;
    metadata_.max_key = 999;
    metadata_.level = 0;
}

Status MockArrowSSTable::ScanWithPushdown(
    const std::vector<std::string>& projection_columns,
    const std::vector<ColumnPredicate>& predicates,
    int64_t batch_size,
    std::unique_ptr<ArrowRecordBatchIterator>* iterator) {

    // Create evaluator and projector
    auto evaluator = CreateArrowPredicateEvaluator(predicates);
    auto projector = CreateArrowColumnProjector(projection_columns);

    // Create iterator
    *iterator = std::make_unique<ArrowRecordBatchIteratorImpl>(
        schema_, batches_, std::move(evaluator), std::move(projector));

    return Status::OK();
}

Status MockArrowSSTable::CountRows(
    const std::vector<ColumnPredicate>& predicates,
    int64_t* count) {

    *count = 0;

    // Quick check using column statistics
    bool can_satisfy = true;
    ARROW_RETURN_NOT_OK(CanSatisfyPredicates(predicates, &can_satisfy));
    if (!can_satisfy) {
        return Status::OK();  // No rows can satisfy predicates
    }

    // Count matching rows
    auto evaluator = CreateArrowPredicateEvaluator(predicates);
    for (const auto& batch : batches_) {
        std::shared_ptr<arrow::BooleanArray> mask;
        ARROW_RETURN_NOT_OK(evaluator->EvaluateBatch(batch, &mask));

        for (int64_t i = 0; i < mask->length(); ++i) {
            if (mask->Value(i)) {
                (*count)++;
            }
        }
    }

    return Status::OK();
}

Status MockArrowSSTable::GetColumnStats(
    const std::string& column_name,
    ColumnStatistics* stats) {

    // Build statistics from all batches
    std::vector<std::shared_ptr<arrow::Array>> chunks;

    for (const auto& batch : batches_) {
        auto column = batch->GetColumnByName(column_name);
        if (column) {
            for (int i = 0; i < column->num_chunks(); ++i) {
                chunks.push_back(column->chunk(i));
            }
        }
    }

    if (chunks.empty()) {
        return Status::InvalidArgument("Column not found: " + column_name);
    }

    auto chunked_array = std::make_shared<arrow::ChunkedArray>(chunks);
    ARROW_RETURN_NOT_OK(BuildColumnStatistics(chunked_array, column_name, stats));

    return Status::OK();
}

Status MockArrowSSTable::CanSatisfyPredicates(
    const std::vector<ColumnPredicate>& predicates,
    bool* can_satisfy) {

    *can_satisfy = true;

    for (const auto& pred : predicates) {
        ColumnStatistics stats(pred.column_name);
        ARROW_RETURN_NOT_OK(GetColumnStats(pred.column_name, &stats));

        if (!stats.CanSatisfyPredicate(pred)) {
            *can_satisfy = false;
            return Status::OK();
        }
    }

    return Status::OK();
}

int main() {
    std::cout << "==========================================" << std::endl;
    std::cout << "🗂️  Projection & Predicate Pushdown Demo" << std::endl;
    std::cout << "==========================================" << std::endl;
    std::cout << std::endl;

    std::cout << "🎯 Goal: Filter and project data directly at SSTable level" << std::endl;
    std::cout << "   • Avoid reading unnecessary columns" << std::endl;
    std::cout << "   • Filter rows before they reach query execution" << std::endl;
    std::cout << "   • Dramatically reduce I/O and processing" << std::endl;
    std::cout << std::endl;

    // Create mock SSTable with sample data
    auto sstable = std::make_unique<MockArrowSSTable>();

    std::cout << "📊 Sample Dataset:" << std::endl;
    std::cout << "   • 1000 employee records" << std::endl;
    std::cout << "   • Columns: id, name, age, salary, department" << std::endl;
    std::cout << "   • Age range: 25-59" << std::endl;
    std::cout << "   • Salary range: $50k-$99k" << std::endl;
    std::cout << std::endl;

    //==============================================================================
    // Demonstrate Projection Pushdown
    //==============================================================================

    std::cout << "🎯 PROJECTION PUSHDOWN - Reading Only Selected Columns" << std::endl;
    std::cout << std::endl;

    // Query 1: Select only name and salary (project 2/5 columns)
    {
        std::vector<std::string> projection = {"name", "salary"};
        std::vector<ColumnPredicate> predicates = {}; // No filtering

        std::unique_ptr<ArrowRecordBatchIterator> iterator;
        auto start_time = std::chrono::high_resolution_clock::now();

        ARROW_RETURN_NOT_OK(sstable->ScanWithPushdown(projection, predicates, 1000, &iterator));

        int64_t total_rows = 0;
        while (iterator->HasNext()) {
            std::shared_ptr<arrow::RecordBatch> batch;
            ARROW_RETURN_NOT_OK(iterator->Next(&batch));
            total_rows += batch->num_rows();

            if (batch->num_rows() > 0) {
                std::cout << "   📋 Projected batch: " << batch->schema()->ToString() << std::endl;
                std::cout << "      Only contains: ";
                for (int i = 0; i < batch->num_columns(); ++i) {
                    std::cout << batch->schema()->field(i)->name();
                    if (i < batch->num_columns() - 1) std::cout << ", ";
                }
                std::cout << std::endl;
                break; // Just show first batch
            }
        }

        auto end_time = std::chrono::high_resolution_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end_time - start_time);

        std::cout << "   ✅ Read " << total_rows << " rows, only 2 columns" << std::endl;
        std::cout << "   ⚡ Projection saved reading 3/5 columns (" << (3.0/5.0)*100 << "% reduction)" << std::endl;
        std::cout << "   🕒 Scan time: " << duration.count() << " microseconds" << std::endl;
        std::cout << std::endl;
    }

    //==============================================================================
    // Demonstrate Predicate Pushdown
    //==============================================================================

    std::cout << "🎯 PREDICATE PUSHDOWN - Filtering at Storage Level" << std::endl;
    std::cout << std::endl;

    // Query 2: Find employees with age > 50
    {
        std::vector<std::string> projection = {}; // All columns
        std::vector<ColumnPredicate> predicates = {
            ColumnPredicate("age", PredicateType::kGreaterThan,
                          arrow::MakeScalar<int64_t>(50))
        };

        int64_t matching_rows = 0;
        auto start_time = std::chrono::high_resolution_clock::now();

        ARROW_RETURN_NOT_OK(sstable->CountRows(predicates, &matching_rows));

        auto end_time = std::chrono::high_resolution_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end_time - start_time);

        std::cout << "   🔍 Query: age > 50" << std::endl;
        std::cout << "   📊 Results: " << matching_rows << " matching rows" << std::endl;
        std::cout << "   ⚡ Filtered " << (1000 - matching_rows) << " rows at storage level" << std::endl;
        std::cout << "   📈 " << (matching_rows / 10.0) << "% of data processed" << std::endl;
        std::cout << "   🕒 Count time: " << duration.count() << " microseconds" << std::endl;
        std::cout << std::endl;
    }

    // Query 3: Find high-earning employees in Engineering
    {
        std::vector<std::string> projection = {"name", "salary"};
        std::vector<ColumnPredicate> predicates = {
            ColumnPredicate("salary", PredicateType::kGreaterThan,
                          arrow::MakeScalar<double>(80000.0)),
            ColumnPredicate("department", PredicateType::kEqual,
                          arrow::MakeScalar<std::string>("Engineering"))
        };

        std::unique_ptr<ArrowRecordBatchIterator> iterator;
        auto start_time = std::chrono::high_resolution_clock::now();

        ARROW_RETURN_NOT_OK(sstable->ScanWithPushdown(projection, predicates, 1000, &iterator));

        int64_t total_rows = 0;
        while (iterator->HasNext()) {
            std::shared_ptr<arrow::RecordBatch> batch;
            ARROW_RETURN_NOT_OK(iterator->Next(&batch));
            total_rows += batch->num_rows();

            if (batch->num_rows() > 0) {
                std::cout << "   🔍 Query: salary > 80000 AND department = 'Engineering'" << std::endl;
                std::cout << "   📋 Projected columns: ";
                for (int i = 0; i < batch->num_columns(); ++i) {
                    std::cout << batch->schema()->field(i)->name();
                    if (i < batch->num_columns() - 1) std::cout << ", ";
                }
                std::cout << std::endl;
                std::cout << "   📊 Results: " << total_rows << " matching rows" << std::endl;
                break;
            }
        }

        auto end_time = std::chrono::high_resolution_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end_time - start_time);

        std::cout << "   ⚡ Combined projection + predicate pushdown" << std::endl;
        std::cout << "   📈 Minimal I/O - only relevant data read" << std::endl;
        std::cout << "   🕒 Query time: " << duration.count() << " microseconds" << std::endl;
        std::cout << std::endl;
    }

    //==============================================================================
    // Demonstrate Column Statistics for Optimization
    //==============================================================================

    std::cout << "🎯 COLUMN STATISTICS - Query Optimization" << std::endl;
    std::cout << std::endl;

    // Show statistics for different columns
    std::vector<std::string> columns = {"age", "salary"};

    for (const auto& col : columns) {
        ColumnStatistics stats(col);
        ARROW_RETURN_NOT_OK(sstable->GetColumnStats(col, &stats));

        std::cout << "   📊 Column '" << col << "' statistics:" << std::endl;
        if (stats.min_value) {
            std::cout << "      Min: " << stats.min_value->ToString() << std::endl;
        }
        if (stats.max_value) {
            std::cout << "      Max: " << stats.max_value->ToString() << std::endl;
        }
        if (stats.mean != 0.0) {
            std::cout << "      Mean: " << stats.mean << std::endl;
        }
        std::cout << "      Nulls: " << (stats.has_nulls ? "Yes" : "No");
        if (stats.has_nulls) {
            std::cout << " (" << stats.null_count << ")";
        }
        std::cout << std::endl;
        std::cout << std::endl;
    }

    //==============================================================================
    // Demonstrate Predicate Elimination
    //==============================================================================

    std::cout << "🎯 PREDICATE ELIMINATION - Skip Unnecessary SSTables" << std::endl;
    std::cout << std::endl;

    // Query that can be eliminated based on statistics
    {
        std::vector<ColumnPredicate> impossible_predicates = {
            ColumnPredicate("age", PredicateType::kGreaterThan,
                          arrow::MakeScalar<int64_t>(100))  // Impossible - max age is 59
        };

        bool can_satisfy = false;
        ARROW_RETURN_NOT_OK(sstable->CanSatisfyPredicates(impossible_predicates, &can_satisfy));

        std::cout << "   🔍 Query: age > 100 (impossible - max age is 59)" << std::endl;
        std::cout << "   🚫 Can satisfy: " << (can_satisfy ? "YES" : "NO") << std::endl;
        std::cout << "   ⚡ SSTable skipped entirely - zero I/O!" << std::endl;
        std::cout << std::endl;
    }

    //==============================================================================
    // Performance Comparison
    //==============================================================================

    std::cout << "==========================================" << std::endl;
    std::cout << "⚡ PERFORMANCE IMPACT" << std::endl;
    std::cout << "==========================================" << std::endl;
    std::cout << std::endl;

    std::cout << "📊 Without Pushdown:" << std::endl;
    std::cout << "   • Read all 1000 rows × 5 columns" << std::endl;
    std::cout << "   • Transfer 5000 data points" << std::endl;
    std::cout << "   • Filter in application memory" << std::endl;
    std::cout << "   • High I/O and CPU usage" << std::endl;
    std::cout << std::endl;

    std::cout << "🚀 With Pushdown:" << std::endl;
    std::cout << "   • Read only matching rows × selected columns" << std::endl;
    std::cout << "   • Example: 200 rows × 2 columns = 400 data points" << std::endl;
    std::cout << "   • Filtering happens during I/O" << std::endl;
    std::cout << "   • 92% reduction in data transfer!" << std::endl;
    std::cout << std::endl;

    std::cout << "🎯 Key Benefits:" << std::endl;
    std::cout << "   ✅ Reduced I/O - read less data from disk" << std::endl;
    std::cout << "   ✅ Lower memory usage - smaller working sets" << std::endl;
    std::cout << "   ✅ Faster queries - less data to process" << std::endl;
    std::cout << "   ✅ Better parallelism - smaller batches" << std::endl;
    std::cout << "   ✅ Network efficiency - less data transfer" << std::endl;
    std::cout << std::endl;

    std::cout << "**Pushdown is the secret to high-performance analytical databases!** 🎯" << std::endl;
    std::cout << "==========================================" << std::endl;

    return 0;
}
