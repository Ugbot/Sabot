/**
 * Record Operations Unit Tests
 *
 * Tests for record operations, key types, and record references:
 * - Key types (Int64Key, TripleKey, StringKey)
 * - Record serialization/deserialization
 * - RecordRef zero-copy access
 * - Schema validation
 */

#include "marble/record.h"
#include "marble/record_ref.h"
#include <gtest/gtest.h>
#include <arrow/api.h>

// Test Int64Key functionality
TEST(Int64KeyTest, BasicOperations) {
    Int64Key key1(42);
    Int64Key key2(100);
    Int64Key key3(42);

    // Test comparison
    EXPECT_LT(key1.Compare(key2), 0); // 42 < 100
    EXPECT_GT(key2.Compare(key1), 0); // 100 > 42
    EXPECT_EQ(key1.Compare(key3), 0); // 42 == 42

    // Test ToString
    EXPECT_EQ(key1.ToString(), "42");

    // Test Hash
    EXPECT_EQ(key1.Hash(), key3.Hash()); // Same values should hash the same
    EXPECT_NE(key1.Hash(), key2.Hash()); // Different values should hash differently

    // Test ToArrowScalar
    auto scalar_result = key1.ToArrowScalar();
    ASSERT_TRUE(scalar_result.ok());
    auto scalar = scalar_result.ValueOrDie();
    EXPECT_EQ(std::static_pointer_cast<arrow::Int64Scalar>(scalar)->value, 42);
}

// Test TripleKey functionality
TEST(TripleKeyTest, BasicOperations) {
    TripleKey key1(1, 2, 3);
    TripleKey key2(1, 2, 4);
    TripleKey key3(1, 2, 3);

    // Test comparison (lexicographic)
    EXPECT_LT(key1.Compare(key2), 0); // (1,2,3) < (1,2,4)
    EXPECT_EQ(key1.Compare(key3), 0); // (1,2,3) == (1,2,3)

    // Test ToString
    EXPECT_EQ(key1.ToString(), "1:2:3");

    // Test Hash
    EXPECT_EQ(key1.Hash(), key3.Hash());
    EXPECT_NE(key1.Hash(), key2.Hash());
}

// Test SimpleRecord functionality
TEST(SimpleRecordTest, BasicOperations) {
    // Create a test schema
    auto schema = arrow::schema({
        arrow::field("id", arrow::int64()),
        arrow::field("name", arrow::utf8()),
        arrow::field("score", arrow::float64())
    });

    // Create test data
    auto batch = arrow::RecordBatch::Make(schema, 1, {
        arrow::ArrayFromJSON(arrow::int64(), "[123]").ValueOrDie(),
        arrow::ArrayFromJSON(arrow::utf8(), "[\"test_name\"]").ValueOrDie(),
        arrow::ArrayFromJSON(arrow::float64(), "[95.5]").ValueOrDie()
    }).ValueOrDie();

    // Create record
    auto key = std::make_shared<Int64Key>(123);
    SimpleRecord record(key, batch, 0);

    // Test GetKey
    auto retrieved_key = record.GetKey();
    ASSERT_TRUE(retrieved_key != nullptr);
    EXPECT_EQ(std::static_pointer_cast<Int64Key>(retrieved_key)->value_, 123);

    // Test GetArrowSchema
    auto record_schema = record.GetArrowSchema();
    ASSERT_TRUE(record_schema != nullptr);
    EXPECT_TRUE(record_schema->Equals(schema));

    // Test ToRecordBatch
    auto record_batch_result = record.ToRecordBatch();
    ASSERT_TRUE(record_batch_result.ok());
    auto record_batch = record_batch_result.ValueOrDie();
    EXPECT_EQ(record_batch->num_rows(), 1);
    EXPECT_TRUE(record_batch->schema()->Equals(schema));

    // Test AsRecordRef
    auto record_ref = record.AsRecordRef();
    ASSERT_TRUE(record_ref != nullptr);
}

// Test ArrowRecordRef zero-copy access
TEST(ArrowRecordRefTest, ZeroCopyAccess) {
    // Create test data
    auto schema = arrow::schema({
        arrow::field("id", arrow::int64()),
        arrow::field("name", arrow::utf8()),
        arrow::field("active", arrow::boolean())
    });

    auto batch = arrow::RecordBatch::Make(schema, 2, {
        arrow::ArrayFromJSON(arrow::int64(), "[1, 2]").ValueOrDie(),
        arrow::ArrayFromJSON(arrow::utf8(), "[\"alice\", \"bob\"]").ValueOrDie(),
        arrow::ArrayFromJSON(arrow::boolean(), "[true, false]").ValueOrDie()
    }).ValueOrDie();

    // Create ArrowRecordRef for first row
    ArrowRecordRef record_ref(batch, 0);

    // Test field access
    auto id_result = record_ref.GetField("id");
    ASSERT_TRUE(id_result.ok());
    auto id_scalar = std::static_pointer_cast<arrow::Int64Scalar>(id_result.ValueOrDie());
    EXPECT_EQ(id_scalar->value, 1);

    auto name_result = record_ref.GetField("name");
    ASSERT_TRUE(name_result.ok());
    auto name_scalar = std::static_pointer_cast<arrow::StringScalar>(name_result.ValueOrDie());
    EXPECT_EQ(name_scalar->value->ToString(), "alice");

    auto active_result = record_ref.GetField("active");
    ASSERT_TRUE(active_result.ok());
    auto active_scalar = std::static_pointer_cast<arrow::BooleanScalar>(active_result.ValueOrDie());
    EXPECT_TRUE(active_scalar->value);

    // Test GetFields (all fields)
    auto all_fields_result = record_ref.GetFields();
    ASSERT_TRUE(all_fields_result.ok());
    auto all_fields = all_fields_result.ValueOrDie();
    EXPECT_EQ(all_fields.size(), 3);

    // Test invalid field access
    auto invalid_result = record_ref.GetField("nonexistent");
    EXPECT_FALSE(invalid_result.ok());
}

// Test SimpleRecordRef functionality
TEST(SimpleRecordRefTest, BasicOperations) {
    // Create test data
    auto schema = arrow::schema({
        arrow::field("user_id", arrow::int64()),
        arrow::field("username", arrow::utf8())
    });

    auto batch = arrow::RecordBatch::Make(schema, 1, {
        arrow::ArrayFromJSON(arrow::int64(), "[42]").ValueOrDie(),
        arrow::ArrayFromJSON(arrow::utf8(), "[\"testuser\"]").ValueOrDie()
    }).ValueOrDie();

    // Create SimpleRecordRef
    auto key = std::make_shared<Int64Key>(42);
    SimpleRecordRef record_ref(key, batch, 0);

    // Test key()
    auto retrieved_key = record_ref.key();
    ASSERT_TRUE(retrieved_key != nullptr);
    EXPECT_EQ(std::static_pointer_cast<Int64Key>(retrieved_key)->value_, 42);

    // Test GetField
    auto username_result = record_ref.GetField("username");
    ASSERT_TRUE(username_result.ok());
    auto username_scalar = std::static_pointer_cast<arrow::StringScalar>(username_result.ValueOrDie());
    EXPECT_EQ(username_scalar->value->ToString(), "testuser");

    // Test GetFields
    auto all_fields_result = record_ref.GetFields();
    ASSERT_TRUE(all_fields_result.ok());
    auto all_fields = all_fields_result.ValueOrDie();
    EXPECT_EQ(all_fields.size(), 2);
}

// Test schema validation
TEST(SchemaValidationTest, RecordSchema) {
    auto schema1 = arrow::schema({
        arrow::field("id", arrow::int64()),
        arrow::field("name", arrow::utf8())
    });

    auto schema2 = arrow::schema({
        arrow::field("id", arrow::int64()),
        arrow::field("name", arrow::utf8())
    });

    auto schema3 = arrow::schema({
        arrow::field("id", arrow::int32()), // Different type
        arrow::field("name", arrow::utf8())
    });

    // Test schema equality
    EXPECT_TRUE(schema1->Equals(schema2));
    EXPECT_FALSE(schema1->Equals(schema3));

    // Test field access
    auto field1 = schema1->GetFieldByName("id");
    ASSERT_TRUE(field1.ok());
    EXPECT_TRUE(field1.ValueOrDie()->type()->Equals(arrow::int64()));

    auto field2 = schema1->GetFieldByName("nonexistent");
    EXPECT_FALSE(field2.ok());
}

// Test key range functionality
TEST(KeyRangeTest, BasicOperations) {
    auto start_key = std::make_shared<Int64Key>(10);
    auto end_key = std::make_shared<Int64Key>(50);

    KeyRange range(start_key, true, end_key, false);

    // Test key containment (basic check)
    auto test_key1 = std::make_shared<Int64Key>(25); // Should be in range
    auto test_key2 = std::make_shared<Int64Key>(5);  // Should be below range
    auto test_key3 = std::make_shared<Int64Key>(55); // Should be above range

    // Note: Actual containment logic would depend on implementation
    // This is just testing the basic structure
    EXPECT_TRUE(range.start_ != nullptr);
    EXPECT_TRUE(range.end_ != nullptr);
    EXPECT_TRUE(range.start_inclusive_);
    EXPECT_FALSE(range.end_inclusive_);

    // Test static All() method
    KeyRange all_range = KeyRange::All();
    EXPECT_TRUE(all_range.start_ == nullptr);
    EXPECT_TRUE(all_range.end_ == nullptr);
}

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
