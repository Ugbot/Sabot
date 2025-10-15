#pragma once

#include <arrow/api.h>
#include <memory>
#include "sabot_cypher/cypher/logical_plan_translator.h"

namespace sabot_cypher {
namespace execution {

// Arrow executor: execute Arrow physical plan
class ArrowExecutor {
public:
    static arrow::Result<std::shared_ptr<ArrowExecutor>> Create();
    
    // Execute Arrow physical plan and return results
    arrow::Result<std::shared_ptr<arrow::Table>> Execute(
        const cypher::ArrowPlan& plan,
        std::shared_ptr<arrow::Table> vertices,
        std::shared_ptr<arrow::Table> edges);
    
private:
    ArrowExecutor() = default;
    
    // Execute individual operators
    arrow::Result<std::shared_ptr<arrow::Table>> ExecuteScan(
        const cypher::ArrowOperatorDesc& op,
        std::shared_ptr<arrow::Table> vertices,
        std::shared_ptr<arrow::Table> edges);
    
    arrow::Result<std::shared_ptr<arrow::Table>> ExecuteFilter(
        const cypher::ArrowOperatorDesc& op,
        std::shared_ptr<arrow::Table> input);
    
    arrow::Result<std::shared_ptr<arrow::Table>> ExecuteProject(
        const cypher::ArrowOperatorDesc& op,
        std::shared_ptr<arrow::Table> input);
    
    arrow::Result<std::shared_ptr<arrow::Table>> ExecuteJoin(
        const cypher::ArrowOperatorDesc& op,
        std::shared_ptr<arrow::Table> left,
        std::shared_ptr<arrow::Table> right);
    
    arrow::Result<std::shared_ptr<arrow::Table>> ExecuteAggregate(
        const cypher::ArrowOperatorDesc& op,
        std::shared_ptr<arrow::Table> input);
    
    arrow::Result<std::shared_ptr<arrow::Table>> ExecuteOrderBy(
        const cypher::ArrowOperatorDesc& op,
        std::shared_ptr<arrow::Table> input);
    
    arrow::Result<std::shared_ptr<arrow::Table>> ExecuteLimit(
        const cypher::ArrowOperatorDesc& op,
        std::shared_ptr<arrow::Table> input);
    
    // Pattern matching operators (using Sabot kernels)
    arrow::Result<std::shared_ptr<arrow::Table>> ExecuteMatch2Hop(
        const cypher::ArrowOperatorDesc& op,
        std::shared_ptr<arrow::Table> input,
        std::shared_ptr<arrow::Table> vertices,
        std::shared_ptr<arrow::Table> edges);
    
    arrow::Result<std::shared_ptr<arrow::Table>> ExecuteMatch3Hop(
        const cypher::ArrowOperatorDesc& op,
        std::shared_ptr<arrow::Table> input,
        std::shared_ptr<arrow::Table> vertices,
        std::shared_ptr<arrow::Table> edges);
    
    arrow::Result<std::shared_ptr<arrow::Table>> ExecuteMatchVariableLength(
        const cypher::ArrowOperatorDesc& op,
        std::shared_ptr<arrow::Table> input,
        std::shared_ptr<arrow::Table> vertices,
        std::shared_ptr<arrow::Table> edges);
    
    arrow::Result<std::shared_ptr<arrow::Table>> ExecuteMatchTriangle(
        const cypher::ArrowOperatorDesc& op,
        std::shared_ptr<arrow::Table> input,
        std::shared_ptr<arrow::Table> vertices,
        std::shared_ptr<arrow::Table> edges);
    
    // Property access operator
    arrow::Result<std::shared_ptr<arrow::Table>> ExecutePropertyAccess(
        const cypher::ArrowOperatorDesc& op,
        std::shared_ptr<arrow::Table> input,
        std::shared_ptr<arrow::Table> vertices,
        std::shared_ptr<arrow::Table> edges);
};

}}  // namespace sabot_cypher::execution

