#include <sabot_ql/sparql/arrow_function_registry.h>
#include <sabot_ql/types/value_id.h>
#include <arrow/compute/cast.h>

namespace sabot_ql {
namespace sparql {

ArrowFunctionRegistry& ArrowFunctionRegistry::Instance() {
    static ArrowFunctionRegistry instance;
    return instance;
}

ArrowFunctionRegistry::ArrowFunctionRegistry() {
    InitializeStringFunctions();
    InitializeMathFunctions();
    InitializeTemporalFunctions();
    InitializeConditionalFunctions();
    InitializeTypeFunctions();
}

void ArrowFunctionRegistry::InitializeStringFunctions() {
    // String manipulation functions
    function_map_["UCASE"] = "utf8_upper";
    function_map_["LCASE"] = "utf8_lower";
    function_map_["STRLEN"] = "binary_length";
    function_map_["SUBSTR"] = "utf8_slice_codeunits";
    function_map_["STRSTARTS"] = "starts_with";
    function_map_["STRENDS"] = "ends_with";
    function_map_["CONTAINS"] = "match_substring";
    function_map_["STRBEFORE"] = "find_substring";  // Needs post-processing
    function_map_["STRAFTER"] = "find_substring";   // Needs post-processing
    function_map_["ENCODE_FOR_URI"] = "ascii_upper";  // Placeholder
    function_map_["CONCAT"] = "binary_join_element_wise";
    function_map_["REPLACE"] = "replace_substring_regex";

    // String regex functions
    function_map_["REGEX"] = "match_substring_regex";

    // Custom SPARQL functions (need special handling)
    custom_functions_.insert("STR");
    custom_functions_.insert("LANG");
    custom_functions_.insert("DATATYPE");
}

void ArrowFunctionRegistry::InitializeMathFunctions() {
    // Basic arithmetic
    function_map_["ABS"] = "abs";
    function_map_["CEIL"] = "ceil";
    function_map_["FLOOR"] = "floor";
    function_map_["ROUND"] = "round";

    // Exponential and logarithmic
    function_map_["SQRT"] = "sqrt";
    function_map_["EXP"] = "exp";
    function_map_["LN"] = "logb";  // Natural log
    function_map_["LOG10"] = "log10";

    // Trigonometric
    function_map_["SIN"] = "sin";
    function_map_["COS"] = "cos";
    function_map_["TAN"] = "tan";
    function_map_["ASIN"] = "asin";
    function_map_["ACOS"] = "acos";
    function_map_["ATAN"] = "atan";

    // Power
    function_map_["POW"] = "power";

    // Random
    function_map_["RAND"] = "random";
}

void ArrowFunctionRegistry::InitializeTemporalFunctions() {
    // Date/time extraction
    function_map_["YEAR"] = "year";
    function_map_["MONTH"] = "month";
    function_map_["DAY"] = "day";
    function_map_["HOURS"] = "hour";
    function_map_["MINUTES"] = "minute";
    function_map_["SECONDS"] = "second";

    // Date/time construction
    function_map_["NOW"] = "local_timestamp";

    // Timezone
    function_map_["TIMEZONE"] = "assume_timezone";
    function_map_["TZ"] = "assume_timezone";
}

void ArrowFunctionRegistry::InitializeConditionalFunctions() {
    // Conditional logic
    function_map_["IF"] = "if_else";
    function_map_["COALESCE"] = "coalesce";
}

void ArrowFunctionRegistry::InitializeTypeFunctions() {
    // Type checking functions (custom implementations)
    custom_functions_.insert("isIRI");
    custom_functions_.insert("isURI");  // Alias for isIRI
    custom_functions_.insert("isLiteral");
    custom_functions_.insert("isBLANK");
    custom_functions_.insert("isNumeric");
    custom_functions_.insert("BOUND");
    custom_functions_.insert("sameTerm");
}

arrow::Result<arrow::Datum> ArrowFunctionRegistry::EvaluateFunction(
    const std::string& function_name,
    const std::vector<arrow::Datum>& args,
    const std::shared_ptr<arrow::compute::FunctionOptions>& options) const {

    // Check if it's a custom function
    if (custom_functions_.count(function_name) > 0) {
        if (function_name == "STR") return EvaluateSTR(args);
        if (function_name == "LANG") return EvaluateLANG(args);
        if (function_name == "DATATYPE") return EvaluateDATATYPE(args);
        if (function_name == "BOUND") return EvaluateBOUND(args);
        if (function_name == "sameTerm") return EvaluatesameTerm(args);
        if (function_name == "isIRI" || function_name == "isURI") return EvaluateisIRI(args);
        if (function_name == "isLiteral") return EvaluateisLiteral(args);
        if (function_name == "isBLANK") return EvaluateisBLANK(args);
        if (function_name == "isNumeric") return EvaluateisNumeric(args);

        return arrow::Status::NotImplemented(
            "Custom function not yet implemented: " + function_name);
    }

    // Look up Arrow function
    auto it = function_map_.find(function_name);
    if (it == function_map_.end()) {
        return arrow::Status::Invalid("Unknown SPARQL function: " + function_name);
    }

    // Call Arrow compute function
    const std::string& arrow_func = it->second;
    if (options) {
        return arrow::compute::CallFunction(arrow_func, args, options.get());
    } else {
        return arrow::compute::CallFunction(arrow_func, args);
    }
}

bool ArrowFunctionRegistry::IsSupported(const std::string& function_name) const {
    return function_map_.count(function_name) > 0 || custom_functions_.count(function_name) > 0;
}

std::string ArrowFunctionRegistry::GetArrowFunctionName(const std::string& sparql_name) const {
    auto it = function_map_.find(sparql_name);
    return (it != function_map_.end()) ? it->second : "";
}

std::vector<std::string> ArrowFunctionRegistry::GetSupportedFunctions() const {
    std::vector<std::string> functions;
    functions.reserve(function_map_.size() + custom_functions_.size());

    for (const auto& pair : function_map_) {
        functions.push_back(pair.first);
    }
    for (const auto& func : custom_functions_) {
        functions.push_back(func);
    }

    std::sort(functions.begin(), functions.end());
    return functions;
}

// Custom SPARQL function implementations

arrow::Result<arrow::Datum> ArrowFunctionRegistry::EvaluateSTR(
    const std::vector<arrow::Datum>& args) const {
    if (args.size() != 1) {
        return arrow::Status::Invalid("STR() requires exactly 1 argument");
    }

    // STR() converts RDF term to its lexical form
    // For ValueIDs, we need to look up the lexical value in vocabulary
    // For now, just cast to string
    return arrow::compute::Cast(args[0], arrow::utf8());
}

arrow::Result<arrow::Datum> ArrowFunctionRegistry::EvaluateLANG(
    const std::vector<arrow::Datum>& args) const {
    if (args.size() != 1) {
        return arrow::Status::Invalid("LANG() requires exactly 1 argument");
    }

    // LANG() returns language tag of a literal
    // For ValueIDs, need to extract language from vocabulary metadata
    // For now, return empty string
    auto length = args[0].length();
    auto builder = arrow::StringBuilder();
    for (int64_t i = 0; i < length; ++i) {
        ARROW_RETURN_NOT_OK(builder.Append(""));  // Empty language tag
    }
    std::shared_ptr<arrow::Array> array;
    ARROW_RETURN_NOT_OK(builder.Finish(&array));
    return arrow::Datum(array);
}

arrow::Result<arrow::Datum> ArrowFunctionRegistry::EvaluateDATATYPE(
    const std::vector<arrow::Datum>& args) const {
    if (args.size() != 1) {
        return arrow::Status::Invalid("DATATYPE() requires exactly 1 argument");
    }

    // DATATYPE() returns datatype IRI of a literal
    // For ValueIDs, need to extract datatype from vocabulary metadata
    // For now, return xsd:string
    auto length = args[0].length();
    auto builder = arrow::StringBuilder();
    const std::string xsd_string = "http://www.w3.org/2001/XMLSchema#string";
    for (int64_t i = 0; i < length; ++i) {
        ARROW_RETURN_NOT_OK(builder.Append(xsd_string));
    }
    std::shared_ptr<arrow::Array> array;
    ARROW_RETURN_NOT_OK(builder.Finish(&array));
    return arrow::Datum(array);
}

arrow::Result<arrow::Datum> ArrowFunctionRegistry::EvaluateBOUND(
    const std::vector<arrow::Datum>& args) const {
    if (args.size() != 1) {
        return arrow::Status::Invalid("BOUND() requires exactly 1 argument");
    }

    // BOUND() checks if variable is bound (not null)
    return arrow::compute::CallFunction("is_valid", args);
}

arrow::Result<arrow::Datum> ArrowFunctionRegistry::EvaluatesameTerm(
    const std::vector<arrow::Datum>& args) const {
    if (args.size() != 2) {
        return arrow::Status::Invalid("sameTerm() requires exactly 2 arguments");
    }

    // sameTerm() checks if two terms are identical (not just equal)
    return arrow::compute::CallFunction("equal", args);
}

arrow::Result<arrow::Datum> ArrowFunctionRegistry::EvaluateisIRI(
    const std::vector<arrow::Datum>& args) const {
    if (args.size() != 1) {
        return arrow::Status::Invalid("isIRI() requires exactly 1 argument");
    }

    // isIRI() checks if value is an IRI
    // For ValueIDs: Check if high bit is set (>= 2^62)
    // IRIs have high bit set, literals don't
    const auto& datum = args[0];
    if (!datum.is_array()) {
        return arrow::Status::Invalid("isIRI() requires array input");
    }

    auto array = datum.make_array();
    auto int_array = std::static_pointer_cast<arrow::Int64Array>(array);

    arrow::BooleanBuilder builder;
    for (int64_t i = 0; i < int_array->length(); ++i) {
        if (int_array->IsNull(i)) {
            ARROW_RETURN_NOT_OK(builder.AppendNull());
        } else {
            int64_t value_id = int_array->Value(i);
            // Check if high bit set (IRI marker)
            bool is_iri = (value_id & (1LL << 62)) != 0;
            ARROW_RETURN_NOT_OK(builder.Append(is_iri));
        }
    }

    std::shared_ptr<arrow::Array> result;
    ARROW_RETURN_NOT_OK(builder.Finish(&result));
    return arrow::Datum(result);
}

arrow::Result<arrow::Datum> ArrowFunctionRegistry::EvaluateisLiteral(
    const std::vector<arrow::Datum>& args) const {
    if (args.size() != 1) {
        return arrow::Status::Invalid("isLiteral() requires exactly 1 argument");
    }

    // isLiteral() checks if value is a literal
    // For ValueIDs: Check if high bit is NOT set (< 2^62)
    const auto& datum = args[0];
    if (!datum.is_array()) {
        return arrow::Status::Invalid("isLiteral() requires array input");
    }

    auto array = datum.make_array();
    auto int_array = std::static_pointer_cast<arrow::Int64Array>(array);

    arrow::BooleanBuilder builder;
    for (int64_t i = 0; i < int_array->length(); ++i) {
        if (int_array->IsNull(i)) {
            ARROW_RETURN_NOT_OK(builder.AppendNull());
        } else {
            int64_t value_id = int_array->Value(i);
            // Check if high bit NOT set (literal marker)
            bool is_literal = (value_id & (1LL << 62)) == 0;
            ARROW_RETURN_NOT_OK(builder.Append(is_literal));
        }
    }

    std::shared_ptr<arrow::Array> result;
    ARROW_RETURN_NOT_OK(builder.Finish(&result));
    return arrow::Datum(result);
}

arrow::Result<arrow::Datum> ArrowFunctionRegistry::EvaluateisBLANK(
    const std::vector<arrow::Datum>& args) const {
    if (args.size() != 1) {
        return arrow::Status::Invalid("isBLANK() requires exactly 1 argument");
    }

    // isBLANK() checks if value is a blank node
    // For now, return false (blank nodes not yet implemented)
    auto length = args[0].length();
    auto builder = arrow::BooleanBuilder();
    for (int64_t i = 0; i < length; ++i) {
        ARROW_RETURN_NOT_OK(builder.Append(false));
    }
    std::shared_ptr<arrow::Array> array;
    ARROW_RETURN_NOT_OK(builder.Finish(&array));
    return arrow::Datum(array);
}

arrow::Result<arrow::Datum> ArrowFunctionRegistry::EvaluateisNumeric(
    const std::vector<arrow::Datum>& args) const {
    if (args.size() != 1) {
        return arrow::Status::Invalid("isNumeric() requires exactly 1 argument");
    }

    // isNumeric() checks if value is a numeric literal
    // Check if type is numeric
    const auto& datum = args[0];
    auto type = datum.type();

    bool is_numeric = arrow::is_integer(type->id()) ||
                     arrow::is_floating(type->id()) ||
                     arrow::is_decimal(type->id());

    if (is_numeric) {
        // All values are numeric
        auto length = datum.length();
        auto builder = arrow::BooleanBuilder();
        for (int64_t i = 0; i < length; ++i) {
            ARROW_RETURN_NOT_OK(builder.Append(true));
        }
        std::shared_ptr<arrow::Array> array;
        ARROW_RETURN_NOT_OK(builder.Finish(&array));
        return arrow::Datum(array);
    } else {
        // None are numeric
        auto length = datum.length();
        auto builder = arrow::BooleanBuilder();
        for (int64_t i = 0; i < length; ++i) {
            ARROW_RETURN_NOT_OK(builder.Append(false));
        }
        std::shared_ptr<arrow::Array> array;
        ARROW_RETURN_NOT_OK(builder.Finish(&array));
        return arrow::Datum(array);
    }
}

} // namespace sparql
} // namespace sabot_ql
