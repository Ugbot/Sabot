#pragma once

#include <arrow/api.h>
#include <arrow/compute/api.h>
#include <arrow/result.h>
#include <string>
#include <vector>
#include <unordered_map>
#include <unordered_set>
#include <memory>

namespace sabot_ql {
namespace sparql {

/**
 * Arrow Function Registry for SPARQL Functions
 *
 * Maps SPARQL 1.1 function names to Arrow compute function names,
 * enabling direct use of Arrow's optimized, vectorized compute kernels
 * for SPARQL expression evaluation.
 *
 * Coverage:
 * - String functions: REGEX, STR, LANG, DATATYPE, STRSTARTS, STRENDS, CONTAINS, etc.
 * - Math functions: ABS, CEIL, FLOOR, ROUND, SQRT, EXP, LOG, POW, SIN, COS, etc.
 * - Date/time functions: NOW, YEAR, MONTH, DAY, HOURS, MINUTES, SECONDS
 * - Conditional: COALESCE, IF
 * - Type checking: isIRI, isLiteral, isBLANK, isNumeric
 *
 * Benefits:
 * - 150+ functions available for free
 * - SIMD-accelerated execution
 * - Zero-copy where possible
 * - Production-tested Arrow kernels
 */
class ArrowFunctionRegistry {
public:
    /**
     * Get singleton instance of registry
     */
    static ArrowFunctionRegistry& Instance();

    /**
     * Evaluate a SPARQL function using Arrow compute
     *
     * @param function_name SPARQL function name (e.g., "UCASE", "ABS", "YEAR")
     * @param args Function arguments as Arrow Datums
     * @param options Optional function-specific options
     * @return Result Datum from function execution
     */
    arrow::Result<arrow::Datum> EvaluateFunction(
        const std::string& function_name,
        const std::vector<arrow::Datum>& args,
        const std::shared_ptr<arrow::compute::FunctionOptions>& options = nullptr) const;

    /**
     * Check if a SPARQL function is supported
     */
    bool IsSupported(const std::string& function_name) const;

    /**
     * Get Arrow function name for SPARQL function
     * Returns empty string if not supported
     */
    std::string GetArrowFunctionName(const std::string& sparql_name) const;

    /**
     * Get list of all supported SPARQL functions
     */
    std::vector<std::string> GetSupportedFunctions() const;

private:
    ArrowFunctionRegistry();

    // Initialize all function mappings
    void InitializeStringFunctions();
    void InitializeMathFunctions();
    void InitializeTemporalFunctions();
    void InitializeConditionalFunctions();
    void InitializeTypeFunctions();

    // Custom SPARQL-specific function implementations
    arrow::Result<arrow::Datum> EvaluateSTR(const std::vector<arrow::Datum>& args) const;
    arrow::Result<arrow::Datum> EvaluateLANG(const std::vector<arrow::Datum>& args) const;
    arrow::Result<arrow::Datum> EvaluateDATATYPE(const std::vector<arrow::Datum>& args) const;
    arrow::Result<arrow::Datum> EvaluateBOUND(const std::vector<arrow::Datum>& args) const;
    arrow::Result<arrow::Datum> EvaluatesameTerm(const std::vector<arrow::Datum>& args) const;
    arrow::Result<arrow::Datum> EvaluateisIRI(const std::vector<arrow::Datum>& args) const;
    arrow::Result<arrow::Datum> EvaluateisLiteral(const std::vector<arrow::Datum>& args) const;
    arrow::Result<arrow::Datum> EvaluateisBLANK(const std::vector<arrow::Datum>& args) const;
    arrow::Result<arrow::Datum> EvaluateisNumeric(const std::vector<arrow::Datum>& args) const;

    // Mapping: SPARQL function name → Arrow function name
    std::unordered_map<std::string, std::string> function_map_;

    // Functions requiring custom implementation
    std::unordered_set<std::string> custom_functions_;
};

} // namespace sparql
} // namespace sabot_ql
