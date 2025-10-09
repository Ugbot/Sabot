#include <iostream>
#include <memory>
#include <marble/record.h>

using namespace marble;

/**
 * @brief Simple demo showing pure template-based type-safe records (no macros!)
 */

int main() {
    std::cout << "==========================================" << std::endl;
    std::cout << "🛡️  Pure Template-Based Type-Safe Records" << std::endl;
    std::cout << "==========================================" << std::endl;
    std::cout << std::endl;

    std::cout << "🎯 NO MACROS - Pure C++ Templates Only!" << std::endl;
    std::cout << std::endl;

    //==============================================================================
    // Step 1: Define field name constants (compile-time string literals)
    //==============================================================================

    std::cout << "📝 Step 1: Define field name constants" << std::endl;
    constexpr const char id_name[] = "id";
    constexpr const char email_name[] = "email";
    constexpr const char age_name[] = "age";
    constexpr const char balance_name[] = "balance";

    std::cout << "   • id_name = \"" << id_name << "\"" << std::endl;
    std::cout << "   • email_name = \"" << email_name << "\"" << std::endl;
    std::cout << "   • age_name = \"" << age_name << "\"" << std::endl;
    std::cout << "   • balance_name = \"" << balance_name << "\"" << std::endl;
    std::cout << std::endl;

    //==============================================================================
    // Step 2: Define field list using templates only
    //==============================================================================

    std::cout << "🔧 Step 2: Define field list using templates only" << std::endl;
    using UserFields = FieldList<
        Field<std::string, id_name, FieldAttribute::kPrimaryKey>,
        Field<std::string, email_name, FieldAttribute::kIndexed>,
        Field<int64_t, age_name, FieldAttribute::kNone>,
        Field<double, balance_name, FieldAttribute::kNone>
    >;

    std::cout << "   ✅ UserFields defined with " << UserFields::size << " fields" << std::endl;
    std::cout << std::endl;

    //==============================================================================
    // Step 3: Define record class with individual field members
    //==============================================================================

    std::cout << "🏗️  Step 3: Define record class with individual field members" << std::endl;

    class User : public TypedRecord<UserFields> {
    public:
        // Individual field members (pure template approach)
        Field<std::string, id_name, FieldAttribute::kPrimaryKey> id;
        Field<std::string, email_name, FieldAttribute::kIndexed> email;
        Field<int64_t, age_name, FieldAttribute::kNone> age;
        Field<double, balance_name, FieldAttribute::kNone> balance;

        User() = default;

        Status to_record_batch(std::shared_ptr<arrow::RecordBatch>* batch) const override {
            return record_to_batch(*this, batch);
        }

        Status from_record_batch(const std::shared_ptr<arrow::RecordBatch>& batch, size_t row_index) override {
            return record_from_batch(*this, batch, row_index);
        }

        Status validate() const override {
            return validate_record(*this);
        }

    private:
        template<size_t I = 0>
        void set_field_values(const std::shared_ptr<arrow::RecordBatch>& batch, size_t row_index);

        template<size_t I = 0>
        void collect_field_values(std::vector<std::shared_ptr<arrow::Array>>& arrays) const;
    };

    std::cout << "   ✅ User record class defined" << std::endl;
    std::cout << std::endl;

    //==============================================================================
    // Compile-time features demonstration
    //==============================================================================

    std::cout << "🔬 Compile-Time Features:" << std::endl;
    std::cout << std::endl;

    User user;

    // Compile-time schema generation
    std::cout << "   📋 Auto-generated schema:" << std::endl;
    std::cout << "      " << user.schema()->ToString() << std::endl;
    std::cout << std::endl;

    // Compile-time field enumeration
    std::cout << "   📝 Field names: ";
    auto field_names = user.field_names();
    for (size_t i = 0; i < field_names.size(); ++i) {
        std::cout << field_names[i];
        if (i < field_names.size() - 1) std::cout << ", ";
    }
    std::cout << std::endl;

    // Compile-time primary key detection
    std::cout << "   🔑 Primary key fields: ";
    auto pk_fields = user.primary_key_fields();
    for (size_t i = 0; i < pk_fields.size(); ++i) {
        std::cout << pk_fields[i];
        if (i < pk_fields.size() - 1) std::cout << ", ";
    }
    std::cout << std::endl;
    std::cout << std::endl;

    //==============================================================================
    // Type safety demonstration
    //==============================================================================

    std::cout << "🛡️  Type Safety Demonstration:" << std::endl;
    std::cout << std::endl;

    std::cout << "   ✅ Setting valid values:" << std::endl;
    user.id.value = "user123";
    user.email.value = "user@example.com";
    user.age.value = 30;
    user.balance.value = 1234.56;

    std::cout << "      user.id.value = \"user123\" ✓" << std::endl;
    std::cout << "      user.email.value = \"user@example.com\" ✓" << std::endl;
    std::cout << "      user.age.value = 30 ✓" << std::endl;
    std::cout << "      user.balance.value = 1234.56 ✓" << std::endl;
    std::cout << std::endl;

    std::cout << "   ❌ These would cause COMPILE ERRORS:" << std::endl;
    std::cout << "      user.age.value = \"thirty\";    // ❌ Type mismatch!" << std::endl;
    std::cout << "      user.id.value = 123;            // ❌ Type mismatch!" << std::endl;
    std::cout << "      user.nonexistent = \"value\";    // ❌ Field doesn't exist!" << std::endl;
    std::cout << std::endl;

    //==============================================================================
    // Record validation
    //==============================================================================

    std::cout << "🔍 Record Validation:" << std::endl;
    std::cout << std::endl;

    // Valid record
    auto validation_result = user.validate();
    std::cout << "   ✅ Valid record: " << (validation_result.ok() ? "PASS" : "FAIL") << std::endl;

    // Create invalid record for testing
    User invalid_user;
    invalid_user.id.value = "";  // Empty primary key
    invalid_user.email.value = "invalid-email";  // No @ symbol
    invalid_user.age.value = -5;  // Negative age

    validation_result = invalid_user.validate();
    std::cout << "   ❌ Invalid record: " << (validation_result.ok() ? "PASS" : "FAIL") << std::endl;
    if (!validation_result.ok()) {
        std::cout << "      Error: " << validation_result.message() << std::endl;
    }
    std::cout << std::endl;

    //==============================================================================
    // Schema registry
    //==============================================================================

    std::cout << "📚 Schema Registry:" << std::endl;
    std::cout << std::endl;

    // Register the User schema
    auto registry_status = global_schema_registry.register_schema<User>("User");
    std::cout << "   ✅ Registered User schema: " << (registry_status.ok() ? "SUCCESS" : "FAILED") << std::endl;

    // List registered schemas
    auto schemas = global_schema_registry.list_schemas();
    std::cout << "   📋 Registered schemas: ";
    for (size_t i = 0; i < schemas.size(); ++i) {
        std::cout << schemas[i];
        if (i < schemas.size() - 1) std::cout << ", ";
    }
    std::cout << std::endl;
    std::cout << std::endl;

    //==============================================================================
    // Comparison with macro-based approaches
    //==============================================================================

    std::cout << "==========================================" << std::endl;
    std::cout << "🔄 Pure Templates vs Macros" << std::endl;
    std::cout << "==========================================" << std::endl;
    std::cout << std::endl;

    std::cout << "❌ Macro-based (like Tonbo's derive):" << std::endl;
    std::cout << "```cpp" << std::endl;
    std::cout << "MARBLE_RECORD(User, UserFields," << std::endl;
    std::cout << "    MARBLE_FIELD(std::string, id, kPrimaryKey)," << std::endl;
    std::cout << "    MARBLE_FIELD(std::string, email, kIndexed)," << std::endl;
    std::cout << "    MARBLE_FIELD(int64_t, age, kNone)," << std::endl;
    std::cout << "    MARBLE_FIELD(double, balance, kNone)" << std::endl;
    std::cout << ");" << std::endl;
    std::cout << "```" << std::endl;
    std::cout << std::endl;

    std::cout << "✅ Pure Template-based (what we have):" << std::endl;
    std::cout << "```cpp" << std::endl;
    std::cout << "constexpr const char id_name[] = \"id\";" << std::endl;
    std::cout << "using UserFields = FieldList<" << std::endl;
    std::cout << "    Field<std::string, id_name, kPrimaryKey>," << std::endl;
    std::cout << "    Field<std::string, email_name, kIndexed>," << std::endl;
    std::cout << "    Field<int64_t, age_name, kNone>," << std::endl;
    std::cout << "    Field<double, balance_name, kNone>" << std::endl;
    std::cout << ">;" << std::endl;
    std::cout << "" << std::endl;
    std::cout << "class User : public TypedRecord<UserFields> {" << std::endl;
    std::cout << "    Field<std::string, id_name, kPrimaryKey> id;" << std::endl;
    std::cout << "    Field<std::string, email_name, kIndexed> email;" << std::endl;
    std::cout << "    // ... explicit field declarations" << std::endl;
    std::cout << "};" << std::endl;
    std::cout << "```" << std::endl;
    std::cout << std::endl;

    std::cout << "🏆 Pure Template Advantages:" << std::endl;
    std::cout << "   • ✅ Zero preprocessor magic" << std::endl;
    std::cout << "   • ✅ Full compile-time type safety" << std::endl;
    std::cout << "   • ✅ Maximum performance (no macro overhead)" << std::endl;
    std::cout << "   • ✅ Complete IDE support and debugging" << std::endl;
    std::cout << "   • ✅ Template metaprogramming flexibility" << std::endl;
    std::cout << std::endl;

    std::cout << "⚖️  Trade-offs:" << std::endl;
    std::cout << "   • More verbose than macro-based approaches" << std::endl;
    std::cout << "   • Requires explicit field member declarations" << std::endl;
    std::cout << "   • No 'magic' - everything is explicit and visible" << std::endl;
    std::cout << std::endl;

    std::cout << "**Pure template-based type safety with ZERO C-style macros!** 🚀" << std::endl;
    std::cout << "==========================================" << std::endl;

    return 0;
}
