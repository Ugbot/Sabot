#include "sabot_sql/sql/duckdb_bridge.h"
#include <arrow/io/memory.h>
#include <arrow/ipc/writer.h>
#include <arrow/ipc/reader.h>
#include <arrow/builder.h>
#include "duckdb/main/prepared_statement_data.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/planner.hpp"
#include "duckdb/parser/parser.hpp"
#include "duckdb/common/types/vector.hpp"

namespace sabot_sql {
namespace sql {

// Helper function to convert DuckDB Vector to Arrow Array
arrow::Result<std::shared_ptr<arrow::Array>> ConvertVectorToArrow(
    duckdb::Vector& vec, idx_t count) {

    auto& type = vec.GetType();
    using duckdb::LogicalTypeId;

    switch (type.id()) {
        case LogicalTypeId::BOOLEAN: {
            arrow::BooleanBuilder builder;
            auto data = duckdb::FlatVector::GetData<bool>(vec);
            auto& validity = duckdb::FlatVector::Validity(vec);
            for (idx_t i = 0; i < count; i++) {
                if (validity.RowIsValid(i)) {
                    ARROW_RETURN_NOT_OK(builder.Append(data[i]));
                } else {
                    ARROW_RETURN_NOT_OK(builder.AppendNull());
                }
            }
            return builder.Finish();
        }
        case LogicalTypeId::INTEGER: {
            arrow::Int32Builder builder;
            auto data = duckdb::FlatVector::GetData<int32_t>(vec);
            auto& validity = duckdb::FlatVector::Validity(vec);
            for (idx_t i = 0; i < count; i++) {
                if (validity.RowIsValid(i)) {
                    ARROW_RETURN_NOT_OK(builder.Append(data[i]));
                } else {
                    ARROW_RETURN_NOT_OK(builder.AppendNull());
                }
            }
            return builder.Finish();
        }
        case LogicalTypeId::BIGINT: {
            arrow::Int64Builder builder;
            auto data = duckdb::FlatVector::GetData<int64_t>(vec);
            auto& validity = duckdb::FlatVector::Validity(vec);
            for (idx_t i = 0; i < count; i++) {
                if (validity.RowIsValid(i)) {
                    ARROW_RETURN_NOT_OK(builder.Append(data[i]));
                } else {
                    ARROW_RETURN_NOT_OK(builder.AppendNull());
                }
            }
            return builder.Finish();
        }
        case LogicalTypeId::DOUBLE: {
            arrow::DoubleBuilder builder;
            auto data = duckdb::FlatVector::GetData<double>(vec);
            auto& validity = duckdb::FlatVector::Validity(vec);
            for (idx_t i = 0; i < count; i++) {
                if (validity.RowIsValid(i)) {
                    ARROW_RETURN_NOT_OK(builder.Append(data[i]));
                } else {
                    ARROW_RETURN_NOT_OK(builder.AppendNull());
                }
            }
            return builder.Finish();
        }
        case LogicalTypeId::FLOAT: {
            arrow::FloatBuilder builder;
            auto data = duckdb::FlatVector::GetData<float>(vec);
            auto& validity = duckdb::FlatVector::Validity(vec);
            for (idx_t i = 0; i < count; i++) {
                if (validity.RowIsValid(i)) {
                    ARROW_RETURN_NOT_OK(builder.Append(data[i]));
                } else {
                    ARROW_RETURN_NOT_OK(builder.AppendNull());
                }
            }
            return builder.Finish();
        }
        case LogicalTypeId::VARCHAR: {
            arrow::StringBuilder builder;
            auto& validity = duckdb::FlatVector::Validity(vec);
            for (idx_t i = 0; i < count; i++) {
                if (validity.RowIsValid(i)) {
                    auto str_val = vec.GetValue(i).ToString();
                    ARROW_RETURN_NOT_OK(builder.Append(str_val));
                } else {
                    ARROW_RETURN_NOT_OK(builder.AppendNull());
                }
            }
            return builder.Finish();
        }
        default:
            return arrow::Status::NotImplemented(
                "Vector type not yet supported: " + type.ToString());
    }
}

// TypeConverter implementation is in type_converter.cpp

// DuckDBBridge implementation
DuckDBBridge::DuckDBBridge(
    std::unique_ptr<duckdb::DuckDB> database,
    std::unique_ptr<duckdb::Connection> connection)
    : database_(std::move(database))
    , connection_(std::move(connection)) {
}

DuckDBBridge::~DuckDBBridge() = default;

arrow::Result<std::shared_ptr<DuckDBBridge>> 
DuckDBBridge::Create(const std::string& database_path) {
    try {
        auto database = std::make_unique<duckdb::DuckDB>(database_path);
        auto connection = std::make_unique<duckdb::Connection>(*database);
        
        return std::shared_ptr<DuckDBBridge>(
            new DuckDBBridge(std::move(database), std::move(connection)));
    } catch (const std::exception& e) {
        return arrow::Status::IOError(
            "Failed to create DuckDB bridge: " + std::string(e.what()));
    }
}

arrow::Status DuckDBBridge::RegisterTable(
    const std::string& table_name,
    const std::shared_ptr<arrow::Table>& table) {
    
    ARROW_RETURN_NOT_OK(RegisterArrowTableInternal(table_name, table));
    registered_tables_[table_name] = table;
    return arrow::Status::OK();
}

arrow::Status DuckDBBridge::RegisterArrowTableInternal(
    const std::string& table_name,
    const std::shared_ptr<arrow::Table>& table) {
    
    try {
        // Register Arrow table directly in DuckDB
        connection_->TableFunction("arrow_scan", {duckdb::Value::POINTER((uintptr_t)&table)})
            ->CreateView(table_name, true, true);
        
        return arrow::Status::OK();
    } catch (const std::exception& e) {
        return arrow::Status::IOError(
            "Failed to register table: " + std::string(e.what()));
    }
}

arrow::Result<LogicalPlan>
DuckDBBridge::ParseAndOptimize(const std::string& sql) {
    try {
        // Parse SQL
        duckdb::Parser parser;
        parser.ParseQuery(sql);

        if (parser.statements.empty()) {
            return arrow::Status::Invalid("Empty query");
        }

        // Create planner - it handles both binding and plan generation
        duckdb::Planner planner(*connection_->context);
        planner.CreatePlan(std::move(parser.statements[0]));

        // Extract logical plan
        LogicalPlan plan = ExtractLogicalPlan(std::move(planner.plan));

        // Copy column names and types from planner
        plan.column_names = planner.names;
        plan.column_types = planner.types;

        // Analyze plan features
        AnalyzePlanFeatures(plan);

        return plan;

    } catch (const std::exception& e) {
        return arrow::Status::Invalid(
            "Failed to parse/optimize SQL: " + std::string(e.what()));
    }
}

LogicalPlan DuckDBBridge::ExtractLogicalPlan(
    duckdb::unique_ptr<duckdb::LogicalOperator> root) {
    
    LogicalPlan plan;
    plan.root = std::move(root);
    
    // Extract schema information
    if (plan.root) {
        plan.root->ResolveOperatorTypes();
        plan.column_types = plan.root->types;
        
        // Get column names from bindings
        auto bindings = plan.root->GetColumnBindings();
        plan.column_names.reserve(bindings.size());
        for (size_t i = 0; i < bindings.size(); i++) {
            plan.column_names.push_back("col_" + std::to_string(i));
        }
        
        plan.estimated_cardinality = plan.root->estimated_cardinality;
    }
    
    return plan;
}

void DuckDBBridge::AnalyzePlanFeatures(LogicalPlan& plan) {
    if (!plan.root) {
        return;
    }
    
    // Walk tree and check for features
    LogicalOperatorWalker::Walk(*plan.root, [&](duckdb::LogicalOperator& op) {
        using duckdb::LogicalOperatorType;
        
        switch (op.type) {
            case LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY:
                plan.has_aggregates = true;
                break;
            case LogicalOperatorType::LOGICAL_COMPARISON_JOIN:
            case LogicalOperatorType::LOGICAL_CROSS_PRODUCT:
            case LogicalOperatorType::LOGICAL_ANY_JOIN:
                plan.has_joins = true;
                break;
            case LogicalOperatorType::LOGICAL_CTE_REF:
                plan.has_ctes = true;
                break;
            default:
                break;
        }
    });
}

arrow::Result<std::vector<PhysicalPlanInfo>> 
DuckDBBridge::ExtractPhysicalPlanStructure(const LogicalPlan& logical_plan) {
    // For now, return empty - we'll implement full physical plan extraction
    // when we need it for advanced optimization
    std::vector<PhysicalPlanInfo> info;
    return info;
}

arrow::Result<std::shared_ptr<arrow::Table>>
DuckDBBridge::ExecuteWithDuckDB(const std::string& sql) {
    try {
        auto result = connection_->Query(sql);

        if (result->HasError()) {
            return arrow::Status::Invalid(
                "Query error: " + result->GetError());
        }

        // Get schema from result
        ARROW_ASSIGN_OR_RAISE(
            auto schema,
            TypeConverter::DuckDBSchemaToArrow(result->types, result->names));

        // Collect all chunks and convert to Arrow
        std::vector<std::shared_ptr<arrow::RecordBatch>> batches;

        // Fetch all data chunks
        while (true) {
            auto chunk = result->Fetch();
            if (!chunk || chunk->size() == 0) {
                break;
            }

            // Convert DuckDB DataChunk to Arrow RecordBatch
            std::vector<std::shared_ptr<arrow::Array>> arrays;
            arrays.reserve(chunk->ColumnCount());

            for (idx_t col_idx = 0; col_idx < chunk->ColumnCount(); col_idx++) {
                auto& vec = chunk->data[col_idx];
                ARROW_ASSIGN_OR_RAISE(auto array, ConvertVectorToArrow(vec, chunk->size()));
                arrays.push_back(array);
            }

            auto batch = arrow::RecordBatch::Make(schema, chunk->size(), arrays);
            batches.push_back(batch);
        }

        if (batches.empty()) {
            // Empty result
            return arrow::Table::MakeEmpty(schema);
        }

        return arrow::Table::FromRecordBatches(schema, batches);

    } catch (const std::exception& e) {
        return arrow::Status::IOError(
            "Failed to execute query: " + std::string(e.what()));
    }
}

bool DuckDBBridge::TableExists(const std::string& table_name) {
    try {
        auto result = connection_->Query(
            "SELECT * FROM information_schema.tables WHERE table_name = '" + 
            table_name + "'");
        return result && result->RowCount() > 0;
    } catch (...) {
        return false;
    }
}

arrow::Result<std::shared_ptr<arrow::Schema>> 
DuckDBBridge::GetTableSchema(const std::string& table_name) {
    if (registered_tables_.find(table_name) != registered_tables_.end()) {
        return registered_tables_[table_name]->schema();
    }
    
    try {
        auto result = connection_->Query(
            "SELECT * FROM " + table_name + " LIMIT 0");
        
        if (result->HasError()) {
            return arrow::Status::Invalid(result->GetError());
        }
        
        return TypeConverter::DuckDBSchemaToArrow(result->types, result->names);
        
    } catch (const std::exception& e) {
        return arrow::Status::IOError(
            "Failed to get table schema: " + std::string(e.what()));
    }
}

// LogicalOperatorWalker implementation
void LogicalOperatorWalker::Walk(
    duckdb::LogicalOperator& root, 
    VisitorFunction visitor) {
    
    visitor(root);
    for (auto& child : root.children) {
        Walk(*child, visitor);
    }
}

std::vector<duckdb::LogicalOperator*> 
LogicalOperatorWalker::FindOperatorsByType(
    duckdb::LogicalOperator& root,
    duckdb::LogicalOperatorType type) {
    
    std::vector<duckdb::LogicalOperator*> operators;
    
    Walk(root, [&](duckdb::LogicalOperator& op) {
        if (op.type == type) {
            operators.push_back(&op);
        }
    });
    
    return operators;
}

} // namespace sql
} // namespace sabot_sql

