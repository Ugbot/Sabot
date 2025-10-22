// Graph Plan Translator
//
// Translates Cypher/SPARQL logical plans to Sabot morsel operator descriptors.
// Pattern: Mirrors sabot_sql/include/sabot_sql/sql/sabot_operator_translator.h

#pragma once

#include "sabot_graph/graph/common_types.h"
#include <arrow/api.h>
#include <arrow/result.h>
#include <memory>
#include <string>
#include <vector>

namespace sabot_graph {
namespace graph {

// Translates graph logical plans to Sabot physical operators
//
// Cypher/SPARQL → Logical Plan → Morsel Operators → Distributed Execution
class GraphPlanTranslator {
public:
    GraphPlanTranslator();
    ~GraphPlanTranslator();
    
    // Translate logical plan to Sabot morsel plan
    arrow::Result<GraphPlan> Translate(const LogicalGraphPlan& logical_plan);
    
    // Translate specific Cypher operators
    arrow::Result<std::vector<GraphOperatorDescriptor>> TranslateCypherMatch(
        std::shared_ptr<void> match_clause);
    
    arrow::Result<GraphOperatorDescriptor> TranslateCypherFilter(
        std::shared_ptr<void> where_clause);
    
    arrow::Result<GraphOperatorDescriptor> TranslateCypherReturn(
        std::shared_ptr<void> return_clause);
    
    // Translate specific SPARQL operators
    arrow::Result<std::vector<GraphOperatorDescriptor>> TranslateSPARQLPattern(
        std::shared_ptr<void> basic_graph_pattern);
    
    arrow::Result<GraphOperatorDescriptor> TranslateSPARQLFilter(
        std::shared_ptr<void> filter_clause);
    
    // Helper: Detect pattern type (2-hop, 3-hop, variable-length)
    std::string DetectPatternType(std::shared_ptr<void> pattern);
    
    // Helper: Extract vertex labels and edge types
    std::vector<std::string> ExtractVertexLabels(std::shared_ptr<void> pattern);
    std::vector<std::string> ExtractEdgeTypes(std::shared_ptr<void> pattern);

private:
    // Convert operator to descriptor
    GraphOperatorDescriptor CreateOperatorDescriptor(
        const std::string& type,
        const std::unordered_map<std::string, std::string>& params);
    
    // Estimate cardinality for graph patterns
    size_t EstimatePatternCardinality(const std::string& pattern_type);
};

} // namespace graph
} // namespace sabot_graph

