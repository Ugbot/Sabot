#include "marble/table.h"
#include "marble/analytics.h"
#include <arrow/api.h>
#include <iostream>
#include <memory>

using namespace marble;
using namespace arrow;

int main() {
    std::cout << "Testing MarbleDB Analytics Engine" << std::endl;

    // Test BloomFilter
    auto bloom = CreateBloomFilter(1000, 0.01);
    bloom->Add("sensor_1");
    bloom->Add("sensor_2");

    std::cout << "Bloom filter test:" << std::endl;
    std::cout << "  'sensor_1' might contain: " << (bloom->MightContain("sensor_1") ? "YES" : "NO") << std::endl;
    std::cout << "  'sensor_999' might contain: " << (bloom->MightContain("sensor_999") ? "YES" : "NO") << std::endl;

    // Test Aggregation Engine
    AggregationEngine agg_engine;

    // Create simple test data
    auto schema = arrow::schema({
        field("sensor_id", utf8()),
        field("temperature", float64())
    });

    StringBuilder sensor_builder;
    DoubleBuilder temp_builder;

    sensor_builder.Append("sensor_1");
    sensor_builder.Append("sensor_1");
    sensor_builder.Append("sensor_2");

    temp_builder.Append(20.0);
    temp_builder.Append(25.0);
    temp_builder.Append(30.0);

    std::shared_ptr<Array> sensor_array, temp_array;
    sensor_builder.Finish(&sensor_array);
    temp_builder.Finish(&temp_array);

    auto test_batch = RecordBatch::Make(schema, 3, {sensor_array, temp_array});
    auto test_table = arrow::Table::FromRecordBatches({test_batch}).ValueUnsafe();

    // Test COUNT
    std::vector<AggregationEngine::AggSpec> count_specs = {
        {AggregationEngine::AggFunction::kCount, "sensor_id", "count"}
    };

    std::shared_ptr<arrow::Table> count_result;
    auto status = agg_engine.Execute(count_specs, test_table, &count_result);
    if (status.ok() && count_result) {
        std::cout << "COUNT aggregation result: " << count_result->num_rows() << " rows" << std::endl;
        auto count_col = count_result->GetColumnByName("count");
        if (count_col) {
            auto scalar = count_col->GetScalar(0).ValueUnsafe();
            std::cout << "  Count value: " << scalar->ToString() << std::endl;
        }
    }

    // Test SUM
    std::vector<AggregationEngine::AggSpec> sum_specs = {
        {AggregationEngine::AggFunction::kSum, "temperature", "sum_temp"}
    };

    std::shared_ptr<arrow::Table> sum_result;
    status = agg_engine.Execute(sum_specs, test_table, &sum_result);
    if (status.ok() && sum_result) {
        auto sum_col = sum_result->GetColumnByName("sum_temp");
        if (sum_col) {
            auto scalar = sum_col->GetScalar(0).ValueUnsafe();
            std::cout << "SUM aggregation result: " << scalar->ToString() << std::endl;
        }
    }

    // Test AVG
    std::vector<AggregationEngine::AggSpec> avg_specs = {
        {AggregationEngine::AggFunction::kAvg, "temperature", "avg_temp"}
    };

    std::shared_ptr<arrow::Table> avg_result;
    status = agg_engine.Execute(avg_specs, test_table, &avg_result);
    if (status.ok() && avg_result) {
        auto avg_col = avg_result->GetColumnByName("avg_temp");
        if (avg_col) {
            auto scalar = avg_col->GetScalar(0).ValueUnsafe();
            std::cout << "AVG aggregation result: " << scalar->ToString() << std::endl;
        }
    }

    std::cout << "✅ Analytics engine test completed!" << std::endl;
    return 0;
}
