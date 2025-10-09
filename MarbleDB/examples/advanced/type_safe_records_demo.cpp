#include <iostream>
#include <memory>
#include <marble/record.h>

using namespace marble;

/**
 * @brief Demo showing MarbleDB's type-safe record system
 *
 * This demonstrates how MarbleDB provides compile-time type safety
 * and automatic schema generation, similar to Tonbo's macro system
 * but using C++ templates for better type safety.
 */

int main() {
    std::cout << "==========================================" << std::endl;
    std::cout << "🛡️  MarbleDB Type-Safe Records Demo" << std::endl;
    std::cout << "==========================================" << std::endl;
    std::cout << std::endl;

    std::cout << "🎯 Problem Solved: Compile-time type safety with automatic schema generation" << std::endl;
    std::cout << std::endl;

    //==============================================================================
    // BEFORE: Manual, error-prone schema creation
    //==============================================================================

    std::cout << "❌ BEFORE: Manual Schema Creation (Error-Prone)" << std::endl;
    std::cout << "   • Manual field definitions" << std::endl;
    std::cout << "   • Runtime type mismatches possible" << std::endl;
    std::cout << "   • No compile-time validation" << std::endl;
    std::cout << std::endl;

    // Old way: Manual schema creation
    auto old_schema = arrow::schema({
        arrow::field("id", arrow::utf8()),
        arrow::field("email", arrow::utf8()),
        arrow::field("age", arrow::int64()),
        arrow::field("balance", arrow::float64())
    });

    std::cout << "   Manual schema: " << old_schema->ToString() << std::endl;
    std::cout << std::endl;

    //==============================================================================
    // AFTER: Type-safe record definitions
    //==============================================================================

    std::cout << "✅ AFTER: Type-Safe Record Definitions (Compile-Time Safety)" << std::endl;
    std::cout << "   • Compile-time schema generation" << std::endl;
    std::cout << "   • Type-safe field access" << std::endl;
    std::cout << "   • Automatic Arrow integration" << std::endl;
    std::cout << std::endl;

    // New way: Pure template-based record definition (no macros!)
    // Step 1: Define field name constants (compile-time string literals)
    constexpr const char id_name[] = "id";
    constexpr const char email_name[] = "email";
    constexpr const char age_name[] = "age";
    constexpr const char balance_name[] = "balance";

    // Step 2: Define field list using templates only
    using UserFields = FieldList<
        Field<std::string, id_name, FieldAttribute::kPrimaryKey>,
        Field<std::string, email_name, FieldAttribute::kIndexed>,
        Field<int64_t, age_name, FieldAttribute::kNone>,
        Field<double, balance_name, FieldAttribute::kNone>
    >;

    // Step 3: Define record class inheriting from template base
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
    };

    // Create a user record
    User user;
    user.id.value = "user123";
    user.email.value = "user@example.com";
    user.age.value = 30;
    user.balance.value = 1234.56;

    std::cout << "   Created User record with type safety:" << std::endl;
    std::cout << "   • ID: " << user.id.value << std::endl;
    std::cout << "   • Email: " << user.email.value << std::endl;
    std::cout << "   • Age: " << user.age.value << std::endl;
    std::cout << "   • Balance: " << user.balance.value << std::endl;
    std::cout << std::endl;

    //==============================================================================
    // Compile-time features
    //==============================================================================

    std::cout << "🔧 Compile-Time Features:" << std::endl;
    std::cout << std::endl;

    // Compile-time schema generation
    std::cout << "   📋 Auto-generated schema: " << user.schema()->ToString() << std::endl;
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
    // Type safety benefits
    //==============================================================================

    std::cout << "🛡️  Type Safety Benefits:" << std::endl;
    std::cout << std::endl;

    std::cout << "   ✅ Compile-time field validation:" << std::endl;
    std::cout << "      user.id.value = \"user123\";        // ✓ Correct type" << std::endl;
    std::cout << "      user.age.value = 30;                // ✓ Correct type" << std::endl;
    std::cout << "      user.balance.value = 1234.56;       // ✓ Correct type" << std::endl;
    std::cout << std::endl;

    std::cout << "   ❌ Compile-time error prevention:" << std::endl;
    std::cout << "      user.age.value = \"thirty\";        // ❌ Compile error!" << std::endl;
    std::cout << "      user.id.value = 123;                // ❌ Compile error!" << std::endl;
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
    // Comparison with Tonbo
    //==============================================================================

    std::cout << "==========================================" << std::endl;
    std::cout << "🔄 MarbleDB vs Tonbo Comparison" << std::endl;
    std::cout << "==========================================" << std::endl;
    std::cout << std::endl;

    std::cout << "🎯 Tonbo's Approach (Rust macros):" << std::endl;
    std::cout << "```rust" << std::endl;
    std::cout << "#[derive(Record, Debug)]" << std::endl;
    std::cout << "pub struct User {" << std::endl;
    std::cout << "    #[record(primary_key)]" << std::endl;
    std::cout << "    name: String," << std::endl;
    std::cout << "    email: Option<String>," << std::endl;
    std::cout << "    age: u8," << std::endl;
    std::cout << "}" << std::endl;
    std::cout << "```" << std::endl;
    std::cout << std::endl;

    std::cout << "🎯 MarbleDB's Approach (Pure C++ Templates - No Macros!):" << std::endl;
    std::cout << "```cpp" << std::endl;
    std::cout << "// Step 1: Define field name constants" << std::endl;
    std::cout << "constexpr const char id_name[] = \"id\";" << std::endl;
    std::cout << "constexpr const char email_name[] = \"email\";" << std::endl;
    std::cout << "" << std::endl;
    std::cout << "// Step 2: Define field list using templates only" << std::endl;
    std::cout << "using UserFields = FieldList<" << std::endl;
    std::cout << "    Field<std::string, id_name, FieldAttribute::kPrimaryKey>," << std::endl;
    std::cout << "    Field<std::string, email_name, FieldAttribute::kIndexed>," << std::endl;
    std::cout << "    Field<int64_t, age_name, FieldAttribute::kNone>," << std::endl;
    std::cout << "    Field<double, balance_name, FieldAttribute::kNone>" << std::endl;
    std::cout << ">;" << std::endl;
    std::cout << "" << std::endl;
    std::cout << "// Step 3: Define record class with individual field members" << std::endl;
    std::cout << "class User : public TypedRecord<UserFields> {" << std::endl;
    std::cout << "public:" << std::endl;
    std::cout << "    Field<std::string, id_name, FieldAttribute::kPrimaryKey> id;" << std::endl;
    std::cout << "    Field<std::string, email_name, FieldAttribute::kIndexed> email;" << std::endl;
    std::cout << "    // ... field members" << std::endl;
    std::cout << "};" << std::endl;
    std::cout << "```" << std::endl;
    std::cout << std::endl;

    std::cout << "🏆 Pure Template Advantages:" << std::endl;
    std::cout << "   • ✅ Zero preprocessor magic - pure C++ only" << std::endl;
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

    std::cout << "🔧 Implementation Benefits:" << std::endl;
    std::cout << "   • Compile-time schema validation" << std::endl;
    std::cout << "   • Type-safe field access" << std::endl;
    std::cout << "   • Automatic Arrow integration" << std::endl;
    std::cout << "   • Runtime performance equivalent to hand-written code" << std::endl;
    std::cout << "   • IDE support and refactoring safety" << std::endl;
    std::cout << std::endl;

    std::cout << "**MarbleDB now has Tonbo-level type safety using PURE C++ TEMPLATES ONLY!** 🚀" << std::endl;
    std::cout << "**No C-style macros - just clean, powerful template metaprogramming!** ✨" << std::endl;
    std::cout << "==========================================" << std::endl;

    return 0;
}
