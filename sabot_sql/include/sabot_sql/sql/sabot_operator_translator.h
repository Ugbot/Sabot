#pragma once

#include <memory>
#include <string>
#include <vector>
#include <unordered_map>
#include <arrow/api.h>
#include <arrow/result.h>
#include "sabot_sql/sql/common_types.h"

namespace sabot_sql {
namespace sql {

/**
 * @brief Translates SabotSQL logical plans to Sabot morsel operators
 * 
 * This translator bridges the gap between SabotSQL's logical planning
 * and Sabot's morsel-driven execution engine.
 */
class SabotOperatorTranslator {
public:
    /**
     * @brief Create a Sabot operator translator
     */
    static arrow::Result<std::shared_ptr<SabotOperatorTranslator>> Create();
    
    ~SabotOperatorTranslator() = default;
    
    /**
     * @brief Translate a logical plan to Sabot morsel operators
     * @param logical_plan The logical plan to translate
     * @return Sabot morsel execution plan
     */
    arrow::Result<std::shared_ptr<void>> 
        TranslateToMorselOperators(const LogicalPlan& logical_plan);
    
    /**
     * @brief Get the output schema of the translated plan
     * @param morsel_plan The morsel execution plan
     * @return Output schema
     */
    arrow::Result<std::shared_ptr<arrow::Schema>> 
        GetOutputSchema(std::shared_ptr<void> morsel_plan);
    
    /**
     * @brief Execute the morsel plan and return results
     * @param morsel_plan The morsel execution plan
     * @return Arrow table result
     */
    arrow::Result<std::shared_ptr<arrow::Table>> 
        ExecuteMorselPlan(std::shared_ptr<void> morsel_plan);

private:
    SabotOperatorTranslator() = default;
    
    // Sabot-only execution mapping helpers
    // These describe the expected Sabot operator stages; the actual construction
    // happens in the executor layer using Sabot's cyarrow-backed kernels.
    void AppendPartitionByKeys(struct MorselPlan& plan);
    void AppendHashJoin(struct MorselPlan& plan);
    void AppendTimeSortWithinPartition(struct MorselPlan& plan);
    void AppendAsOfMergeProbe(struct MorselPlan& plan);
    void AppendProjectDateTruncForWindows(struct MorselPlan& plan);
    void AppendGroupByWindowFrame(struct MorselPlan& plan);
    
    // Translation methods for different operator types
    arrow::Result<std::shared_ptr<void>> 
        TranslateTableScan(const std::shared_ptr<void>& logical_op);
    
    arrow::Result<std::shared_ptr<void>> 
        TranslateFilter(const std::shared_ptr<void>& logical_op);
    
    arrow::Result<std::shared_ptr<void>> 
        TranslateProjection(const std::shared_ptr<void>& logical_op);
    
    arrow::Result<std::shared_ptr<void>> 
        TranslateJoin(const std::shared_ptr<void>& logical_op);
    
    arrow::Result<std::shared_ptr<void>> 
        TranslateAggregate(const std::shared_ptr<void>& logical_op);
    
    arrow::Result<std::shared_ptr<void>> 
        TranslateOrderBy(const std::shared_ptr<void>& logical_op);
    
    arrow::Result<std::shared_ptr<void>> 
        TranslateLimit(const std::shared_ptr<void>& logical_op);
    
    // Helper methods
    std::string GetOperatorType(const std::shared_ptr<void>& logical_op);
    arrow::Result<std::vector<std::string>> 
        GetColumnNames(const std::shared_ptr<void>& logical_op);
    arrow::Result<std::shared_ptr<arrow::Schema>> 
        GetInputSchema(const std::shared_ptr<void>& logical_op);
};

} // namespace sql
} // namespace sabot_sql
