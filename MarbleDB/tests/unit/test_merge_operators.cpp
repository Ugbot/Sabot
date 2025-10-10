/************************************************************************
Unit Tests for Merge Operators
**************************************************************************/

#include "marble/merge_operator.h"
#include <gtest/gtest.h>
#include <vector>
#include <string>

using namespace marble;

// Test Int64AddOperator
TEST(MergeOperatorTest, Int64Add_Basic) {
    Int64AddOperator op;
    std::string result;
    
    // No existing value
    std::vector<std::string> operands = {"+5", "+3", "+2"};
    auto status = op.FullMerge("key1", nullptr, operands, &result);
    
    ASSERT_TRUE(status.ok());
    ASSERT_EQ(result, "10");
}

TEST(MergeOperatorTest, Int64Add_WithExisting) {
    Int64AddOperator op;
    std::string result;
    
    // With existing value
    std::string existing = "100";
    std::vector<std::string> operands = {"+5", "-3", "+8"};
    auto status = op.FullMerge("key1", &existing, operands, &result);
    
    ASSERT_TRUE(status.ok());
    ASSERT_EQ(result, "110");
}

TEST(MergeOperatorTest, Int64Add_PartialMerge) {
    Int64AddOperator op;
    std::string result;
    
    // Partial merge optimization
    std::vector<std::string> operands = {"+1", "+2", "+3", "+4", "+5"};
    auto status = op.PartialMerge("key1", operands, &result);
    
    ASSERT_TRUE(status.ok());
    ASSERT_EQ(result, "15");
}

TEST(MergeOperatorTest, Int64Add_Negative) {
    Int64AddOperator op;
    std::string result;
    
    std::string existing = "100";
    std::vector<std::string> operands = {"-10", "-20", "-30"};
    auto status = op.FullMerge("key1", &existing, operands, &result);
    
    ASSERT_TRUE(status.ok());
    ASSERT_EQ(result, "40");
}

// Test StringAppendOperator
TEST(MergeOperatorTest, StringAppend_NoDelimiter) {
    StringAppendOperator op("");
    std::string result;
    
    std::vector<std::string> operands = {"Hello", "World", "!"};
    auto status = op.FullMerge("key1", nullptr, operands, &result);
    
    ASSERT_TRUE(status.ok());
    ASSERT_EQ(result, "HelloWorld!");
}

TEST(MergeOperatorTest, StringAppend_WithDelimiter) {
    StringAppendOperator op(",");
    std::string result;
    
    std::string existing = "apple";
    std::vector<std::string> operands = {"banana", "cherry"};
    auto status = op.FullMerge("key1", &existing, operands, &result);
    
    ASSERT_TRUE(status.ok());
    ASSERT_EQ(result, "apple,banana,cherry");
}

// Test SetUnionOperator
TEST(MergeOperatorTest, SetUnion_Basic) {
    SetUnionOperator op;
    std::string result;
    
    std::vector<std::string> operands = {"a,b", "b,c", "c,d"};
    auto status = op.FullMerge("key1", nullptr, operands, &result);
    
    ASSERT_TRUE(status.ok());
    // Result should be sorted unique: a,b,c,d
    ASSERT_EQ(result, "a,b,c,d");
}

TEST(MergeOperatorTest, SetUnion_WithExisting) {
    SetUnionOperator op;
    std::string result;
    
    std::string existing = "x,y,z";
    std::vector<std::string> operands = {"y,z,a", "a,b"};
    auto status = op.FullMerge("key1", &existing, operands, &result);
    
    ASSERT_TRUE(status.ok());
    // Unique set: a,b,x,y,z (sorted)
    ASSERT_EQ(result, "a,b,x,y,z");
}

// Test MaxOperator
TEST(MergeOperatorTest, Max_Basic) {
    MaxOperator op;
    std::string result;
    
    std::vector<std::string> operands = {"10", "5", "20", "15"};
    auto status = op.FullMerge("key1", nullptr, operands, &result);
    
    ASSERT_TRUE(status.ok());
    ASSERT_EQ(result, "20");
}

TEST(MergeOperatorTest, Max_WithExisting) {
    MaxOperator op;
    std::string result;
    
    std::string existing = "50";
    std::vector<std::string> operands = {"10", "30", "40"};
    auto status = op.FullMerge("key1", &existing, operands, &result);
    
    ASSERT_TRUE(status.ok());
    ASSERT_EQ(result, "50");  // Existing is max
}

// Test MinOperator
TEST(MergeOperatorTest, Min_Basic) {
    MinOperator op;
    std::string result;
    
    std::vector<std::string> operands = {"10", "5", "20", "15"};
    auto status = op.FullMerge("key1", nullptr, operands, &result);
    
    ASSERT_TRUE(status.ok());
    ASSERT_EQ(result, "5");
}

TEST(MergeOperatorTest, Min_WithExisting) {
    MinOperator op;
    std::string result;
    
    std::string existing = "3";
    std::vector<std::string> operands = {"10", "5", "8"};
    auto status = op.FullMerge("key1", &existing, operands, &result);
    
    ASSERT_TRUE(status.ok());
    ASSERT_EQ(result, "3");  // Existing is min
}

// Test MergeOperatorFactory
TEST(MergeOperatorTest, Factory_Int64Add) {
    auto op = MergeOperatorFactory::Create(MergeOperatorFactory::Type::kInt64Add);
    ASSERT_NE(op, nullptr);
    ASSERT_STREQ(op->Name(), "Int64AddOperator");
    
    std::string result;
    std::vector<std::string> operands = {"+10"};
    auto status = op->FullMerge("key", nullptr, operands, &result);
    ASSERT_TRUE(status.ok());
    ASSERT_EQ(result, "10");
}

TEST(MergeOperatorTest, Factory_StringAppend) {
    auto op = MergeOperatorFactory::Create(
        MergeOperatorFactory::Type::kStringAppend, 
        ","  // delimiter
    );
    ASSERT_NE(op, nullptr);
    
    std::string result;
    std::vector<std::string> operands = {"a", "b"};
    auto status = op->FullMerge("key", nullptr, operands, &result);
    ASSERT_TRUE(status.ok());
    ASSERT_EQ(result, "a,b");
}

// Stress test - many concurrent merges
TEST(MergeOperatorTest, Stress_ConcurrentMerges) {
    Int64AddOperator op;
    std::string result;
    
    // Simulate 1000 concurrent +1 operations
    std::vector<std::string> operands;
    for (int i = 0; i < 1000; ++i) {
        operands.push_back("+1");
    }
    
    auto status = op.FullMerge("counter", nullptr, operands, &result);
    ASSERT_TRUE(status.ok());
    ASSERT_EQ(result, "1000");
}

// Edge case tests
TEST(MergeOperatorTest, Int64Add_InvalidOperand) {
    Int64AddOperator op;
    std::string result;
    
    std::vector<std::string> operands = {"not_a_number"};
    auto status = op.FullMerge("key", nullptr, operands, &result);
    
    ASSERT_FALSE(status.ok());
    ASSERT_EQ(status.code(), StatusCode::kInvalidArgument);
}

TEST(MergeOperatorTest, SetUnion_EmptyOperands) {
    SetUnionOperator op;
    std::string result;
    
    std::string existing = "a,b,c";
    std::vector<std::string> operands = {};
    auto status = op.FullMerge("key", &existing, operands, &result);
    
    ASSERT_TRUE(status.ok());
    ASSERT_EQ(result, "a,b,c");  // Just existing value
}

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}

