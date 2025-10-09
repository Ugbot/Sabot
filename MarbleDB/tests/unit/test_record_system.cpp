/**
 * Unit Tests for Type-Safe Record System
 *
 * Tests compile-time type safety, schema generation,
 * and record operations.
 */

#include <gtest/gtest.h>
#include <marble/record.h>
#include <marble/status.h>

namespace marble {

class RecordSystemTest : public ::testing::Test {
protected:
    void SetUp() override {}
    void TearDown() override {}
};

//==============================================================================
// Test Field Template Metaprogramming
//==============================================================================

TEST_F(RecordSystemTest, FieldCompileTimeAttributes) {
    // Test compile-time field attributes
    using TestField = Field<int64_t, "test_field", FieldAttribute::kPrimaryKey>;

    static_assert(TestField::is_primary_key == true, "Primary key attribute not set");
    static_assert(TestField::is_nullable == false, "Field should not be nullable");
    static_assert(std::is_same_v<TestField::value_type, int64_t>, "Value type mismatch");
    static_assert(std::string_view(TestField::name) == "test_field", "Field name mismatch");

    // Test field instance
    TestField field;
    field.value = 42;
    EXPECT_EQ(field.get(), 42);
}

TEST_F(RecordSystemTest, FieldTypeMapping) {
    // Test Arrow type mapping
    auto int_type = ArrowTypeMap<int64_t>::type();
    EXPECT_TRUE(int_type->Equals(arrow::int64()));

    auto string_type = ArrowTypeMap<std::string>::type();
    EXPECT_TRUE(string_type->Equals(arrow::utf8()));

    auto double_type = ArrowTypeMap<double>::type();
    EXPECT_TRUE(double_type->Equals(arrow::float64()));
}

TEST_F(RecordSystemTest, NullableFieldTypes) {
    // Test optional field types
    using OptionalIntField = Field<std::optional<int64_t>, "opt_int", FieldAttribute::kNullable>;

    static_assert(OptionalIntField::is_nullable == true, "Optional field should be nullable");

    auto field_type = ArrowTypeMap<std::optional<int64_t>>::type();
    EXPECT_TRUE(field_type->Equals(arrow::int64()));
}

//==============================================================================
// Test FieldList Template Metaprogramming
//==============================================================================

TEST_F(RecordSystemTest, FieldListOperations) {
    using TestFieldList = FieldList<
        Field<int64_t, "id", FieldAttribute::kPrimaryKey>,
        Field<std::string, "name", FieldAttribute::kIndexed>,
        Field<double, "value", FieldAttribute::kNone>
    >;

    static_assert(TestFieldList::size == 3, "Field list size incorrect");

    // Test schema generation
    auto schema = TestFieldList::to_arrow_schema();
    ASSERT_TRUE(schema != nullptr);
    EXPECT_EQ(schema->num_fields(), 3);

    // Test field names
    auto field_names = TestFieldList::field_names();
    EXPECT_EQ(field_names.size(), 3);
    EXPECT_EQ(field_names[0], "id");
    EXPECT_EQ(field_names[1], "name");
    EXPECT_EQ(field_names[2], "value");

    // Test primary key fields
    auto pk_fields = TestFieldList::primary_key_fields();
    EXPECT_EQ(pk_fields.size(), 1);
    EXPECT_EQ(pk_fields[0], "id");
}

TEST_F(RecordSystemTest, EmptyFieldList) {
    using EmptyFieldList = FieldList<>;

    static_assert(EmptyFieldList::size == 0, "Empty field list size should be 0");

    auto schema = EmptyFieldList::to_arrow_schema();
    EXPECT_EQ(schema->num_fields(), 0);

    auto field_names = EmptyFieldList::field_names();
    EXPECT_TRUE(field_names.empty());

    auto pk_fields = EmptyFieldList::primary_key_fields();
    EXPECT_TRUE(pk_fields.empty());
}

//==============================================================================
// Test TypedRecord Base Class
//==============================================================================

TEST_F(RecordSystemTest, TypedRecordSchemaGeneration) {
    using TestRecordFields = FieldList<
        Field<int64_t, "user_id", FieldAttribute::kPrimaryKey>,
        Field<std::string, "username", FieldAttribute::kIndexed>,
        Field<std::string, "email", FieldAttribute::kNone>
    >;

    class TestRecord : public TypedRecord<TestRecordFields> {};

    TestRecord record;

    // Test schema generation
    auto schema = record.schema();
    ASSERT_TRUE(schema != nullptr);
    EXPECT_EQ(schema->num_fields(), 3);

    // Test field access by name
    EXPECT_EQ(schema->GetFieldIndex("user_id"), 0);
    EXPECT_EQ(schema->GetFieldIndex("username"), 1);
    EXPECT_EQ(schema->GetFieldIndex("email"), 2);

    // Test field types
    EXPECT_TRUE(schema->field(0)->type()->Equals(arrow::int64()));
    EXPECT_TRUE(schema->field(1)->type()->Equals(arrow::utf8()));
    EXPECT_TRUE(schema->field(2)->type()->Equals(arrow::utf8()));
}

//==============================================================================
// Test Complete Record Implementation
//==============================================================================

// Define a test record using the pure template approach
constexpr const char user_id_name[] = "user_id";
constexpr const char username_name[] = "username";
constexpr const char email_name[] = "email";
constexpr const char age_name[] = "age";
constexpr const char balance_name[] = "balance";

using UserRecordFields = FieldList<
    Field<int64_t, user_id_name, FieldAttribute::kPrimaryKey>,
    Field<std::string, username_name, FieldAttribute::kIndexed>,
    Field<std::string, email_name, FieldAttribute::kNone>,
    Field<int32_t, age_name, FieldAttribute::kNone>,
    Field<double, balance_name, FieldAttribute::kNone>
>;

class UserRecord : public TypedRecord<UserRecordFields> {
public:
    // Individual field members (pure template approach)
    Field<int64_t, user_id_name, FieldAttribute::kPrimaryKey> user_id;
    Field<std::string, username_name, FieldAttribute::kIndexed> username;
    Field<std::string, email_name, FieldAttribute::kNone> email;
    Field<int32_t, age_name, FieldAttribute::kNone> age;
    Field<double, balance_name, FieldAttribute::kNone> balance;

    UserRecord() = default;

    // Constructor for easy initialization
    UserRecord(int64_t id, std::string uname, std::string mail, int32_t a, double bal)
        : user_id(id), username(std::move(uname)), email(std::move(mail)), age(a), balance(bal) {}

    Status to_record_batch(std::shared_ptr<arrow::RecordBatch>* batch) const override {
        return record_to_batch(*this, batch);
    }

    Status from_record_batch(const std::shared_ptr<arrow::RecordBatch>& batch, size_t row_index) override {
        return record_from_batch(*this, batch, row_index);
    }

    Status validate() const override {
        if (user_id.value <= 0) {
            return Status::InvalidArgument("User ID must be positive");
        }
        if (username.value.empty()) {
            return Status::InvalidArgument("Username cannot be empty");
        }
        if (email.value.find('@') == std::string::npos) {
            return Status::InvalidArgument("Invalid email format");
        }
        if (age.value < 0 || age.value > 150) {
            return Status::InvalidArgument("Age must be between 0 and 150");
        }
        return Status::OK();
    }
};

TEST_F(RecordSystemTest, CompleteRecordOperations) {
    // Test record creation
    UserRecord user(1, "john_doe", "john@example.com", 30, 1234.56);

    EXPECT_EQ(user.user_id.value, 1);
    EXPECT_EQ(user.username.value, "john_doe");
    EXPECT_EQ(user.email.value, "john@example.com");
    EXPECT_EQ(user.age.value, 30);
    EXPECT_EQ(user.balance.value, 1234.56);

    // Test validation
    auto valid_result = user.validate();
    EXPECT_TRUE(valid_result.ok());

    // Test invalid record
    UserRecord invalid_user(-1, "", "invalid-email", 200, 0.0);
    auto invalid_result = invalid_user.validate();
    EXPECT_FALSE(invalid_result.ok());
    EXPECT_EQ(invalid_result.code(), StatusCode::kInvalidArgument);
}

TEST_F(RecordSystemTest, RecordBatchConversion) {
    // Create a record
    UserRecord user(42, "test_user", "test@example.com", 25, 999.99);

    // Convert to RecordBatch
    std::shared_ptr<arrow::RecordBatch> batch;
    auto status = user.to_record_batch(&batch);
    ASSERT_TRUE(status.ok());
    ASSERT_TRUE(batch != nullptr);

    // Verify batch contents
    EXPECT_EQ(batch->num_rows(), 1);
    EXPECT_EQ(batch->num_columns(), 5);

    // Check individual columns
    auto id_array = std::static_pointer_cast<arrow::Int64Array>(batch->column(0));
    EXPECT_EQ(id_array->Value(0), 42);

    auto username_array = std::static_pointer_cast<arrow::StringArray>(batch->column(1));
    EXPECT_EQ(username_array->GetString(0), "test_user");

    auto email_array = std::static_pointer_cast<arrow::StringArray>(batch->column(2));
    EXPECT_EQ(email_array->GetString(0), "test@example.com");

    auto age_array = std::static_pointer_cast<arrow::Int32Array>(batch->column(3));
    EXPECT_EQ(age_array->Value(0), 25);

    auto balance_array = std::static_pointer_cast<arrow::DoubleArray>(batch->column(4));
    EXPECT_DOUBLE_EQ(balance_array->Value(0), 999.99);
}

TEST_F(RecordSystemTest, RecordFromBatchConversion) {
    // Create a RecordBatch manually
    arrow::Int64Builder id_builder;
    arrow::StringBuilder username_builder;
    arrow::StringBuilder email_builder;
    arrow::Int32Builder age_builder;
    arrow::DoubleBuilder balance_builder;

    ASSERT_TRUE(id_builder.Append(123).ok());
    ASSERT_TRUE(username_builder.Append("batch_user").ok());
    ASSERT_TRUE(email_builder.Append("batch@example.com").ok());
    ASSERT_TRUE(age_builder.Append(35).ok());
    ASSERT_TRUE(balance_builder.Append(777.77).ok());

    std::shared_ptr<arrow::Array> id_array, username_array, email_array, age_array, balance_array;
    ASSERT_TRUE(id_builder.Finish(&id_array).ok());
    ASSERT_TRUE(username_builder.Finish(&username_array).ok());
    ASSERT_TRUE(email_builder.Finish(&email_array).ok());
    ASSERT_TRUE(age_builder.Finish(&age_array).ok());
    ASSERT_TRUE(balance_builder.Finish(&balance_array).ok());

    auto schema = arrow::schema({
        arrow::field("user_id", arrow::int64()),
        arrow::field("username", arrow::utf8()),
        arrow::field("email", arrow::utf8()),
        arrow::field("age", arrow::int32()),
        arrow::field("balance", arrow::float64())
    });

    auto batch = arrow::RecordBatch::Make(schema, 1, {id_array, username_array, email_array, age_array, balance_array});

    // Convert back to record
    UserRecord user;
    auto status = user.from_record_batch(batch, 0);
    ASSERT_TRUE(status.ok());

    EXPECT_EQ(user.user_id.value, 123);
    EXPECT_EQ(user.username.value, "batch_user");
    EXPECT_EQ(user.email.value, "batch@example.com");
    EXPECT_EQ(user.age.value, 35);
    EXPECT_DOUBLE_EQ(user.balance.value, 777.77);
}

//==============================================================================
// Test Schema Registry
//==============================================================================

TEST_F(RecordSystemTest, SchemaRegistryOperations) {
    // Register a record type
    auto register_status = global_schema_registry.register_schema<UserRecord>("User");
    EXPECT_TRUE(register_status.ok());

    // Retrieve schema
    auto schema = global_schema_registry.get_schema("User");
    ASSERT_TRUE(schema != nullptr);
    EXPECT_EQ(schema->num_fields(), 5);

    // List schemas
    auto schema_names = global_schema_registry.list_schemas();
    EXPECT_EQ(schema_names.size(), 1);
    EXPECT_EQ(schema_names[0], "User");
}

//==============================================================================
// Test Record Factory
//==============================================================================

TEST_F(RecordSystemTest, RecordFactoryOperations) {
    // Create record via factory
    auto record = RecordFactory::create<UserRecord>();
    ASSERT_TRUE(record != nullptr);

    // Set some values
    record->user_id.value = 999;
    record->username.value = "factory_user";
    record->email.value = "factory@example.com";
    record->age.value = 40;
    record->balance.value = 555.55;

    // Verify values
    EXPECT_EQ(record->user_id.value, 999);
    EXPECT_EQ(record->username.value, "factory_user");
    EXPECT_EQ(record->email.value, "factory@example.com");
    EXPECT_EQ(record->age.value, 40);
    EXPECT_DOUBLE_EQ(record->balance.value, 555.55);
}

} // namespace marble
