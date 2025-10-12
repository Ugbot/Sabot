#include "sabot_sql/sql/duckdb_bridge.h"
#include <arrow/io/memory.h>
#include <arrow/ipc/writer.h>
#include <arrow/ipc/reader.h>
#include "duckdb/main/prepared_statement_data.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/planner.hpp"
#include "duckdb/parser/parser.hpp"

namespace sabot_sql {
namespace sql {

// TypeConverter implementation
arrow::Result<std::shared_ptr<arrow::DataType>> 
TypeConverter::DuckDBToArrow(const duckdb::LogicalType& duck_type) {
    using duckdb::LogicalTypeId;
    
    switch (duck_type.id()) {
        case LogicalTypeId::BOOLEAN:
            return arrow::boolean();
        case LogicalTypeId::TINYINT:
            return arrow::int8();
        case LogicalTypeId::SMALLINT:
            return arrow::int16();
        case LogicalTypeId::INTEGER:
            return arrow::int32();
        case LogicalTypeId::BIGINT:
            return arrow::int64();
        case LogicalTypeId::UTINYINT:
            return arrow::uint8();
        case LogicalTypeId::USMALLINT:
            return arrow::uint16();
        case LogicalTypeId::UINTEGER:
            return arrow::uint32();
        case LogicalTypeId::UBIGINT:
            return arrow::uint64();
        case LogicalTypeId::FLOAT:
            return arrow::float32();
        case LogicalTypeId::DOUBLE:
            return arrow::float64();
        case LogicalTypeId::VARCHAR:
            return arrow::utf8();
        case LogicalTypeId::DATE:
            return arrow::date32();
        case LogicalTypeId::TIMESTAMP:
            return arrow::timestamp(arrow::TimeUnit::MICRO);
        case LogicalTypeId::TIME:
            return arrow::time64(arrow::TimeUnit::MICRO);
        case LogicalTypeId::BLOB:
            return arrow::binary();
        default:
            return arrow::Status::NotImplemented(
                "DuckDB type not yet supported: " + duck_type.ToString());
    }
}

arrow::Result<duckdb::LogicalType> 
TypeConverter::ArrowToDuckDB(const std::shared_ptr<arrow::DataType>& arrow_type) {
    switch (arrow_type->id()) {
        case arrow::Type::BOOL:
            return duckdb::LogicalType::BOOLEAN;
        case arrow::Type::INT8:
            return duckdb::LogicalType::TINYINT;
        case arrow::Type::INT16:
            return duckdb::LogicalType::SMALLINT;
        case arrow::Type::INT32:
            return duckdb::LogicalType::INTEGER;
        case arrow::Type::INT64:
            return duckdb::LogicalType::BIGINT;
        case arrow::Type::UINT8:
            return duckdb::LogicalType::UTINYINT;
        case arrow::Type::UINT16:
            return duckdb::LogicalType::USMALLINT;
        case arrow::Type::UINT32:
            return duckdb::LogicalType::UINTEGER;
        case arrow::Type::UINT64:
            return duckdb::LogicalType::UBIGINT;
        case arrow::Type::FLOAT:
            return duckdb::LogicalType::FLOAT;
        case arrow::Type::DOUBLE:
            return duckdb::LogicalType::DOUBLE;
        case arrow::Type::STRING:
            return duckdb::LogicalType::VARCHAR;
        case arrow::Type::DATE32:
            return duckdb::LogicalType::DATE;
        case arrow::Type::TIMESTAMP:
            return duckdb::LogicalType::TIMESTAMP;
        case arrow::Type::BINARY:
            return duckdb::LogicalType::BLOB;
        default:
            return arrow::Status::NotImplemented(
                "Arrow type not yet supported: " + arrow_type->ToString());
    }
}

arrow::Result<std::shared_ptr<arrow::Schema>> 
TypeConverter::DuckDBSchemaToArrow(
    const std::vector<duckdb::LogicalType>& duck_types,
    const std::vector<std::string>& column_names) {
    
    if (duck_types.size() != column_names.size()) {
        return arrow::Status::Invalid(
            "Type and name count mismatch");
    }
    
    std::vector<std::shared_ptr<arrow::Field>> fields;
    fields.reserve(duck_types.size());
    
    for (size_t i = 0; i < duck_types.size(); i++) {
        ARROW_ASSIGN_OR_RAISE(auto arrow_type, DuckDBToArrow(duck_types[i]));
        fields.push_back(arrow::field(column_names[i], arrow_type));
    }
    
    return arrow::schema(fields);
}

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
        
        // Bind and plan - Updated for DuckDB API changes
        auto binder = duckdb::Binder::CreateBinder(*connection_->context);
        binder->BindStatement(*parser.statements[0]);
        
        duckdb::Planner planner(*connection_->context);
        planner.CreatePlan(std::move(parser.statements[0]));
        
        // Extract logical plan
        LogicalPlan plan = ExtractLogicalPlan(std::move(planner.plan));
        
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
        
        // Convert to Arrow
        // Updated for DuckDB API changes - use Arrow conversion
        auto arrow_result = result->ToArrowTable();
        if (!arrow_result) {
            return arrow::Status::Invalid("Failed to fetch result");
        }
        
        // Collect all batches into a table
        std::vector<std::shared_ptr<arrow::RecordBatch>> batches;
        while (arrow_result) {
            batches.push_back(arrow_result);
            arrow_result = result->ToArrowTable();
        }
        
        if (batches.empty()) {
            // Empty result - create empty table with schema
            ARROW_ASSIGN_OR_RAISE(
                auto schema, 
                TypeConverter::DuckDBSchemaToArrow(
                    result->types, result->names));
            return arrow::Table::MakeEmpty(schema);
        }
        
        return arrow::Table::FromRecordBatches(batches);
        
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

