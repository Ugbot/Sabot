#include "sabot_sql/sql/query_engine.h"
#include "sabot_sql/operators/table_scan.h"
#include <sstream>

namespace sabot_sql {
namespace sql {

SQLQueryEngine::SQLQueryEngine(
    const SQLQueryEngineConfig& config,
    std::shared_ptr<DuckDBBridge> bridge,
    std::unique_ptr<SQLOperatorTranslator> translator,
    std::unique_ptr<QueryExecutor> executor)
    : config_(config)
    , duck_bridge_(std::move(bridge))
    , translator_(std::move(translator))
    , executor_(std::move(executor)) {
}

SQLQueryEngine::~SQLQueryEngine() = default;

arrow::Result<std::shared_ptr<SQLQueryEngine>> 
SQLQueryEngine::Create(const SQLQueryEngineConfig& config) {
    // Create DuckDB bridge
    ARROW_ASSIGN_OR_RAISE(auto bridge, DuckDBBridge::Create(config.database_path));
    
    // Create operator translator
    auto translator = std::make_unique<SQLOperatorTranslator>();
    translator->SetUseMorselOperators(config.execution_mode != ExecutionMode::LOCAL);
    translator->SetMorselSizeKB(config.morsel_size_kb);
    translator->SetNumWorkers(config.num_workers);
    
    // Create query executor
    // Note: QueryExecutor is from existing SabotQL execution engine
    auto executor = std::make_unique<QueryExecutor>(nullptr, nullptr); // TODO: Pass proper storage/vocab
    
    return std::shared_ptr<SQLQueryEngine>(
        new SQLQueryEngine(config, std::move(bridge), 
                          std::move(translator), std::move(executor)));
}

arrow::Status SQLQueryEngine::RegisterTable(
    const std::string& table_name,
    const std::shared_ptr<arrow::Table>& table) {
    
    // Register in DuckDB bridge
    ARROW_RETURN_NOT_OK(duck_bridge_->RegisterTable(table_name, table));
    
    // Register in translator
    translator_->RegisterTable(table_name, table);
    
    // Store locally
    tables_[table_name] = table;
    
    return arrow::Status::OK();
}

arrow::Status SQLQueryEngine::RegisterTableFromFile(
    const std::string& table_name,
    const std::string& file_path) {
    
    // Load table from file using TableScanOperator
    ARROW_ASSIGN_OR_RAISE(auto scan_op, TableScanOperator::FromFile(file_path));
    ARROW_ASSIGN_OR_RAISE(auto table, scan_op->GetAllResults());
    
    return RegisterTable(table_name, table);
}

arrow::Result<std::shared_ptr<arrow::Table>> 
SQLQueryEngine::Execute(const std::string& sql) {
    // Create query plan
    ARROW_ASSIGN_OR_RAISE(auto root_op, CreateQueryPlan(sql));
    
    // Execute plan
    ARROW_ASSIGN_OR_RAISE(auto result, root_op->GetAllResults());
    
    // Collect statistics
    last_query_stats_.clear();
    const auto& stats = root_op->GetStats();
    last_query_stats_["rows_processed"] = std::to_string(stats.rows_processed);
    last_query_stats_["batches_processed"] = std::to_string(stats.batches_processed);
    last_query_stats_["execution_time_ms"] = std::to_string(stats.execution_time_ms);
    
    return result;
}

arrow::Result<std::shared_ptr<Operator>> 
SQLQueryEngine::ExecuteStreaming(const std::string& sql) {
    // Create query plan
    ARROW_ASSIGN_OR_RAISE(auto root_op, CreateQueryPlan(sql));
    
    // Return operator for streaming
    return root_op;
}

arrow::Result<std::string> 
SQLQueryEngine::Explain(const std::string& sql) {
    // Create query plan
    ARROW_ASSIGN_OR_RAISE(auto root_op, CreateQueryPlan(sql));
    
    // Generate explain output
    return GenerateExplainOutput(*root_op);
}

arrow::Result<std::string> 
SQLQueryEngine::ExplainAnalyze(const std::string& sql) {
    // Execute query to collect statistics
    ARROW_ASSIGN_OR_RAISE(auto root_op, CreateQueryPlan(sql));
    ARROW_ASSIGN_OR_RAISE(auto result, root_op->GetAllResults());
    
    // Generate explain output with stats
    std::string explain = GenerateExplainOutput(*root_op);
    
    // Add statistics
    std::ostringstream oss;
    oss << explain << "\n\n";
    oss << "Execution Statistics:\n";
    oss << "  Rows Processed: " << root_op->GetStats().rows_processed << "\n";
    oss << "  Batches Processed: " << root_op->GetStats().batches_processed << "\n";
    oss << "  Execution Time: " << root_op->GetStats().execution_time_ms << " ms\n";
    
    return oss.str();
}

bool SQLQueryEngine::HasTable(const std::string& table_name) const {
    return tables_.find(table_name) != tables_.end();
}

arrow::Result<std::shared_ptr<arrow::Schema>> 
SQLQueryEngine::GetTableSchema(const std::string& table_name) const {
    auto it = tables_.find(table_name);
    if (it != tables_.end()) {
        return it->second->schema();
    }
    return arrow::Status::Invalid("Table not found: " + table_name);
}

std::vector<std::string> SQLQueryEngine::GetTableNames() const {
    std::vector<std::string> names;
    names.reserve(tables_.size());
    for (const auto& [name, table] : tables_) {
        names.push_back(name);
    }
    return names;
}

std::unordered_map<std::string, std::string> 
SQLQueryEngine::GetLastQueryStats() const {
    return last_query_stats_;
}

arrow::Result<std::shared_ptr<Operator>> 
SQLQueryEngine::CreateQueryPlan(const std::string& sql) {
    // Step 1: Parse and optimize with DuckDB
    ARROW_ASSIGN_OR_RAISE(auto logical_plan, duck_bridge_->ParseAndOptimize(sql));
    
    // Step 2: Translate to Sabot operators
    TranslationContext context;
    context.tables = tables_;
    
    ARROW_ASSIGN_OR_RAISE(auto root_op, translator_->Translate(logical_plan, context));
    
    // Step 3: Optimize query plan (optional)
    ARROW_ASSIGN_OR_RAISE(auto optimized_op, OptimizeQueryPlan(root_op));
    
    return optimized_op;
}

arrow::Result<std::shared_ptr<Operator>> 
SQLQueryEngine::OptimizeQueryPlan(std::shared_ptr<Operator> plan) {
    // For now, return plan as-is
    // Future: Apply Sabot-specific optimizations
    // - Operator fusion
    // - Predicate pushdown
    // - Join reordering
    // - Morsel size tuning
    
    return plan;
}

std::string SQLQueryEngine::GenerateExplainOutput(const Operator& root_op) {
    // Generate tree-structured explanation
    std::ostringstream oss;
    oss << "Query Plan:\n";
    oss << "  " << root_op.ToString() << "\n";
    oss << "  Estimated Cardinality: " << root_op.EstimateCardinality() << "\n";
    
    // TODO: Walk operator tree and generate full plan
    
    return oss.str();
}

} // namespace sql
} // namespace sabot_sql

