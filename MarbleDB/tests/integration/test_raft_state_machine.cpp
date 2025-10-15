#include <marble/raft.h>
#include <marble/db.h>
#include <gtest/gtest.h>
#include <memory>
#include <iostream>

namespace marble {
namespace test {

// Test fixture for RAFT state machine tests
class RaftStateMachineTest : public ::testing::Test {
protected:
    void SetUp() override {
        // Create a MarbleDB instance for testing
        DBOptions options;
        options.db_path = "/tmp/marble_raft_test_" + std::to_string(test_counter_++);
        options.create_if_missing = true;

        auto status = MarbleDB::Open(options, nullptr, &db_);
        ASSERT_TRUE(status.ok()) << "Failed to create test database: " << status.ToString();

        // Create RAFT state machine
        state_machine_ = CreateMarbleDBStateMachine(db_);
        ASSERT_TRUE(state_machine_ != nullptr);
    }

    void TearDown() override {
        state_machine_.reset();
        if (db_) {
            db_->Close();
        }
        db_.reset();
    }

    std::shared_ptr<MarbleDB> db_;
    std::unique_ptr<RaftStateMachine> state_machine_;
    static int test_counter_;
};

int RaftStateMachineTest::test_counter_ = 0;

// Test basic operation application
TEST_F(RaftStateMachineTest, ApplyCreateTableOperation) {
    // Create a simple table operation
    auto raft_op = CreateCreateTableOperation(
        TableSchema("test_table", arrow::schema({
            arrow::field("id", arrow::int64()),
            arrow::field("value", arrow::utf8())
        })),
        1  // sequence number
    );

    ASSERT_TRUE(raft_op != nullptr);

    // Apply the operation
    auto status = state_machine_->ApplyOperation(*raft_op);
    EXPECT_TRUE(status.ok()) << "Failed to apply CreateTable operation: " << status.ToString();

    // Verify operation was applied
    EXPECT_EQ(state_machine_->GetLastAppliedIndex(), 1u);
}

// Test InsertBatch operation
TEST_F(RaftStateMachineTest, ApplyInsertBatchOperation) {
    // First create a table
    auto create_op = CreateCreateTableOperation(
        TableSchema("batch_test", arrow::schema({
            arrow::field("id", arrow::int64()),
            arrow::field("data", arrow::utf8())
        })),
        1
    );

    auto status = state_machine_->ApplyOperation(*create_op);
    ASSERT_TRUE(status.ok());

    // Create test data
    arrow::Int64Builder id_builder;
    arrow::StringBuilder data_builder;

    for (int64_t i = 0; i < 10; ++i) {
        id_builder.Append(i).ok();
        data_builder.Append("value_" + std::to_string(i)).ok();
    }

    std::shared_ptr<arrow::Array> id_array, data_array;
    id_builder.Finish(&id_array).ok();
    data_builder.Finish(&data_array).ok();

    auto batch = arrow::RecordBatch::Make(
        arrow::schema({
            arrow::field("id", arrow::int64()),
            arrow::field("data", arrow::utf8())
        }),
        10,
        {id_array, data_array}
    ).ValueOrDie();

    // Create InsertBatch operation
    auto insert_op = CreateInsertBatchOperation("batch_test", batch, 2);

    // Apply the operation
    status = state_machine_->ApplyOperation(*insert_op);
    EXPECT_TRUE(status.ok()) << "Failed to apply InsertBatch operation: " << status.ToString();

    // Verify operation was applied
    EXPECT_EQ(state_machine_->GetLastAppliedIndex(), 2u);
}

// Test snapshot creation and restoration
TEST_F(RaftStateMachineTest, SnapshotOperations) {
    // Apply some operations first
    auto create_op = CreateCreateTableOperation(
        TableSchema("snapshot_test", arrow::schema({
            arrow::field("key", arrow::int64())
        })),
        1
    );

    auto status = state_machine_->ApplyOperation(*create_op);
    ASSERT_TRUE(status.ok());

    // Create snapshot
    status = state_machine_->CreateSnapshot(1);
    EXPECT_TRUE(status.ok()) << "Failed to create snapshot: " << status.ToString();

    // Apply more operations
    auto create_op2 = CreateCreateTableOperation(
        TableSchema("snapshot_test2", arrow::schema({
            arrow::field("key", arrow::int64())
        })),
        2
    );

    status = state_machine_->ApplyOperation(*create_op2);
    ASSERT_TRUE(status.ok());

    // Verify we have 2 applied operations
    EXPECT_EQ(state_machine_->GetLastAppliedIndex(), 2u);

    // Restore from snapshot (should only have operation 1 applied)
    status = state_machine_->RestoreFromSnapshot(1);
    EXPECT_TRUE(status.ok()) << "Failed to restore from snapshot: " << status.ToString();

    // After restore, we should be back to operation 1
    EXPECT_EQ(state_machine_->GetLastAppliedIndex(), 1u);
}

// Test operation serialization
TEST(RaftOperationTest, SerializeDeserialize) {
    // Create a test operation
    auto raft_op = CreateCreateTableOperation(
        TableSchema("test_table", arrow::schema({
            arrow::field("id", arrow::int64())
        })),
        42
    );

    ASSERT_TRUE(raft_op != nullptr);

    // Serialize to string
    std::string serialized = raft_op->Serialize();
    EXPECT_FALSE(serialized.empty());

    // Deserialize back
    auto deserialized = RaftOperation::Deserialize(serialized);
    ASSERT_TRUE(deserialized != nullptr);

    // Verify fields match
    EXPECT_EQ(deserialized->sequence_number, raft_op->sequence_number);
    EXPECT_EQ(deserialized->timestamp, raft_op->timestamp);
    EXPECT_EQ(deserialized->type, raft_op->type);
    EXPECT_EQ(deserialized->data, raft_op->data);
}

// Test multiple state machines
TEST_F(RaftStateMachineTest, MultipleOperations) {
    const size_t num_operations = 100;

    for (size_t i = 0; i < num_operations; ++i) {
        auto raft_op = CreateCreateTableOperation(
            TableSchema("table_" + std::to_string(i), arrow::schema({
                arrow::field("id", arrow::int64())
            })),
            i + 1
        );

        auto status = state_machine_->ApplyOperation(*raft_op);
        ASSERT_TRUE(status.ok()) << "Failed to apply operation " << i << ": " << status.ToString();
    }

    // Verify all operations were applied
    EXPECT_EQ(state_machine_->GetLastAppliedIndex(), num_operations);
}

} // namespace test
} // namespace marble
