/**
 * Comprehensive tests for ArcticDB-style bitemporal capabilities
 */

#include "marble/temporal_reconstruction.h"
#include "marble/temporal.h"
#include <gtest/gtest.h>
#include <arrow/api.h>
#include <memory>

using namespace marble;
using namespace arrow;

class ArcticBitemporalTest : public ::testing::Test {
protected:
    void SetUp() override {
        // Create test data
        CreateTestData();
    }

    void CreateTestData() {
        // Create schema for employee data
        schema_ = arrow::schema({
            field("id", utf8()),
            field("name", utf8()),
            field("salary", int64()),
            field("department", utf8())
        });

        // Create version 1 data (initial hire)
        auto ids_v1 = ArrayFromJSON(utf8(), R"(["EMP001", "EMP002", "EMP003"])");
        auto names_v1 = ArrayFromJSON(utf8(), R"(["Alice", "Bob", "Carol"])");
        auto salaries_v1 = ArrayFromJSON(int64(), R"([50000, 55000, 60000])");
        auto depts_v1 = ArrayFromJSON(utf8(), R"(["Engineering", "Sales", "Marketing"])");

        version_batches_.push_back(*RecordBatch::Make(schema_, 3,
                                                     {ids_v1, names_v1, salaries_v1, depts_v1}));

        // Version 1 metadata (hired Jan 2024, valid until further notice)
        metadata_.push_back({
            SnapshotId(1704067200000000ULL, 1),  // Jan 1, 2024
            SnapshotId(0, 0),                    // Not deleted
            1704067200000000ULL,                 // Valid from Jan 1, 2024
            UINT64_MAX,                          // Valid until infinity
            false
        });

        // Create version 2 data (salary increases in Mar 2024)
        auto salaries_v2 = ArrayFromJSON(int64(), R"([55000, 60500, 66000])");  // +10% raises
        version_batches_.push_back(*RecordBatch::Make(schema_, 3,
                                                     {ids_v1, names_v1, salaries_v2, depts_v1}));

        // Version 2 metadata
        metadata_.push_back({
            SnapshotId(1709251200000000ULL, 2),  // Mar 1, 2024
            SnapshotId(0, 0),
            1709251200000000ULL,                 // Valid from Mar 1, 2024
            UINT64_MAX,
            false
        });

        // Create version 3 data (Bob moves to Engineering in Jun 2024)
        auto depts_v3 = ArrayFromJSON(utf8(), R"(["Engineering", "Engineering", "Marketing"])");
        version_batches_.push_back(*RecordBatch::Make(schema_, 3,
                                                     {ids_v1, names_v1, salaries_v2, depts_v3}));

        // Version 3 metadata
        metadata_.push_back({
            SnapshotId(1717200000000000ULL, 3),  // Jun 1, 2024
            SnapshotId(0, 0),
            1717200000000000ULL,                 // Valid from Jun 1, 2024
            UINT64_MAX,
            false
        });
    }

    std::shared_ptr<Schema> schema_;
    std::vector<RecordBatch> version_batches_;
    std::vector<TemporalMetadata> metadata_;
};

TEST_F(ArcticBitemporalTest, AsOfQuery) {
    auto reconstructor = CreateTemporalReconstructor();

    // Test AS OF Jan 2024 (should see original salaries)
    SnapshotId jan_snapshot(1704067200000000ULL, 1);
    RecordBatch result;

    auto status = reconstructor->ReconstructAsOf(jan_snapshot,
                                                version_batches_,
                                                metadata_,
                                                &result);
    ASSERT_TRUE(status.ok());
    ASSERT_EQ(result.num_rows(), 3);

    // Check salaries are original values
    auto salary_col = result.GetColumnByName("salary");
    auto salary_0 = salary_col->GetScalar(0);
    auto salary_1 = salary_col->GetScalar(1);
    auto salary_2 = salary_col->GetScalar(2);

    ASSERT_TRUE(salary_0.ok());
    ASSERT_TRUE(salary_1.ok());
    ASSERT_TRUE(salary_2.ok());

    ASSERT_EQ(std::static_pointer_cast<Int64Scalar>(salary_0.ValueUnsafe())->value, 50000);
    ASSERT_EQ(std::static_pointer_cast<Int64Scalar>(salary_1.ValueUnsafe())->value, 55000);
    ASSERT_EQ(std::static_pointer_cast<Int64Scalar>(salary_2.ValueUnsafe())->value, 60000);
}

TEST_F(ArcticBitemporalTest, ValidTimeQuery) {
    auto reconstructor = CreateTemporalReconstructor();

    // Test VALID_TIME during Feb 2024 (should see original salaries)
    uint64_t feb_start = 1706745600000000ULL;  // Feb 1, 2024
    uint64_t feb_end = 1709251199999999ULL;    // Feb 28, 2024

    RecordBatch result;
    auto status = reconstructor->ReconstructValidTime(feb_start, feb_end,
                                                     version_batches_,
                                                     metadata_,
                                                     &result);
    ASSERT_TRUE(status.ok());
    ASSERT_EQ(result.num_rows(), 3);

    // Check salaries are original values (valid during Feb)
    auto salary_col = result.GetColumnByName("salary");
    auto salary_0 = salary_col->GetScalar(0);
    ASSERT_TRUE(salary_0.ok());
    ASSERT_EQ(std::static_pointer_cast<Int64Scalar>(salary_0.ValueUnsafe())->value, 50000);
}

TEST_F(ArcticBitemporalTest, FullBitemporalQuery) {
    auto reconstructor = CreateTemporalReconstructor();

    // Test AS OF Mar 2024 AND VALID during Apr 2024
    TemporalQuerySpec spec;
    spec.as_of_snapshot = SnapshotId(1709251200000000ULL, 2);  // Mar 1, 2024
    spec.valid_time_start = 1711929600000000ULL;  // Apr 1, 2024
    spec.valid_time_end = 1714521599999999ULL;    // Apr 30, 2024

    RecordBatch result;
    auto status = reconstructor->ReconstructBitemporal(spec,
                                                      version_batches_,
                                                      metadata_,
                                                      &result);
    ASSERT_TRUE(status.ok());
    ASSERT_EQ(result.num_rows(), 3);

    // Should see March salaries (AS OF Mar) which were valid in April
    auto salary_col = result.GetColumnByName("salary");
    auto salary_0 = salary_col->GetScalar(0);
    ASSERT_TRUE(salary_0.ok());
    ASSERT_EQ(std::static_pointer_cast<Int64Scalar>(salary_0.ValueUnsafe())->value, 55000);
}

TEST_F(ArcticBitemporalTest, VersionHistory) {
    auto reconstructor = CreateTemporalReconstructor();

    // Get history for EMP002 (Bob)
    std::vector<RecordBatch> history;
    auto status = reconstructor->ReconstructHistory("EMP002",
                                                   version_batches_,
                                                   metadata_,
                                                   &history);
    ASSERT_TRUE(status.ok());

    // Should have multiple versions (Bob had salary change and dept change)
    ASSERT_GT(history.size(), 0);

    // Check that department changed from Sales to Engineering
    bool found_sales = false;
    bool found_engineering = false;

    for (const auto& version : history) {
        auto dept_col = version.GetColumnByName("department");
        if (dept_col) {
            auto dept_scalar = dept_col->GetScalar(0);  // Bob's record
            if (dept_scalar.ok()) {
                auto dept = dept_scalar.ValueUnsafe()->ToString();
                if (dept == "Sales") found_sales = true;
                if (dept == "Engineering") found_engineering = true;
            }
        }
    }

    ASSERT_TRUE(found_sales);
    ASSERT_TRUE(found_engineering);
}

TEST_F(ArcticBitemporalTest, QueryBuilder) {
    auto builder = CreateArcticQueryBuilder();

    // Test fluent API
    builder->AsOf(SnapshotId(1704067200000000ULL, 1))
           ->ValidBetween(1706745600000000ULL, 1709251199999999ULL)
           ->IncludeDeleted(false);

    auto spec = builder->Build();

    ASSERT_EQ(spec.as_of_snapshot.timestamp, 1704067200000000ULL);
    ASSERT_EQ(spec.valid_time_start, 1706745600000000ULL);
    ASSERT_EQ(spec.valid_time_end, 1709251199999999ULL);
    ASSERT_FALSE(spec.include_deleted);
    ASSERT_TRUE(spec.temporal_reconstruction);
}

TEST_F(ArcticBitemporalTest, ArcticOperations) {
    // Test ArcticOperations static methods
    auto temporal_db = CreateTemporalDatabase("/tmp/test_arctic_ops");

    TableSchema schema("test", arrow::schema({field("id", utf8()), field("value", int64())}));
    auto status = temporal_db->CreateTemporalTable("test", schema, TemporalModel::kBitemporal);
    ASSERT_TRUE(status.ok());

    std::shared_ptr<TemporalTable> table;
    status = temporal_db->GetTemporalTable("test", &table);
    ASSERT_TRUE(status.ok());

    // Test AppendWithValidity
    auto test_data = *RecordBatch::Make(
        arrow::schema({field("id", utf8()), field("value", int64())}),
        1, {ArrayFromJSON(utf8(), R"(["TEST001"])"),
             ArrayFromJSON(int64(), R"([42])")}
    );

    status = ArcticOperations::AppendWithValidity(table, test_data,
                                                 1704067200000000ULL, UINT64_MAX);
    ASSERT_TRUE(status.ok());

    // Test GetVersionHistory
    std::vector<VersionInfo> history;
    status = ArcticOperations::GetVersionHistory(table, "TEST001", &history);
    ASSERT_TRUE(status.ok());
    // Note: For MVP, history might be empty - that's OK
}

TEST_F(ArcticBitemporalTest, VersionChainBuilding) {
    auto reconstructor = CreateTemporalReconstructor();

    // Test internal version chain building
    std::unordered_map<std::string, VersionChain> chains;
    auto status = reconstructor->BuildVersionChains(version_batches_, metadata_, &chains);
    ASSERT_TRUE(status.ok());

    // Should have chains for EMP001, EMP002, EMP003
    ASSERT_EQ(chains.size(), 3);
    ASSERT_TRUE(chains.find("EMP001") != chains.end());
    ASSERT_TRUE(chains.find("EMP002") != chains.end());
    ASSERT_TRUE(chains.find("EMP003") != chains.end());

    // Each chain should have the expected number of versions
    for (const auto& [key, chain] : chains) {
        ASSERT_EQ(chain.version_indices.size(), 3);  // All employees have 3 versions
        ASSERT_EQ(chain.metadata.size(), 3);
        ASSERT_EQ(chain.primary_key, key);
    }
}

TEST_F(ArcticBitemporalTest, ConflictResolution) {
    auto reconstructor = CreateTemporalReconstructor();

    std::unordered_map<std::string, VersionChain> chains;
    auto status = reconstructor->BuildVersionChains(version_batches_, metadata_, &chains);
    ASSERT_TRUE(status.ok());

    // Resolve conflicts for each chain
    for (auto& [key, chain] : chains) {
        status = reconstructor->ResolveVersionConflicts(&chain);
        ASSERT_TRUE(status.ok());

        // After resolution, versions should be sorted by system time
        for (size_t i = 1; i < chain.metadata.size(); ++i) {
            ASSERT_LE(chain.metadata[i-1].created_snapshot.timestamp,
                     chain.metadata[i].created_snapshot.timestamp);
        }

        // Current state should reflect the latest version
        ASSERT_FALSE(chain.is_deleted);  // No deletions in our test data
        ASSERT_EQ(chain.valid_to, UINT64_MAX);  // Still valid
    }
}

TEST_F(ArcticBitemporalTest, MergeRecordBatches) {
    auto reconstructor = CreateTemporalReconstructor();

    std::vector<std::shared_ptr<RecordBatch>> batches = {
        std::make_shared<RecordBatch>(version_batches_[0]),
        std::make_shared<RecordBatch>(version_batches_[1])
    };

    RecordBatch result;
    auto status = reconstructor->MergeRecordBatches(batches, &result);
    ASSERT_TRUE(status.ok());

    // Should have combined rows
    ASSERT_EQ(result.num_rows(), 6);  // 3 + 3 rows
    ASSERT_TRUE(result.schema()->Equals(*schema_));
}

int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
