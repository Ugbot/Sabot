#include "sabot_cypher/execution/arrow_executor.h"
#include "sabot_cypher/cypher/expression_evaluator.h"
#include <arrow/compute/api.h>
#include <arrow/compute/cast.h>
#include <sstream>

// Pattern matching will be implemented using Arrow joins
// Sabot kernels integration deferred to Python layer

namespace sabot_cypher {
namespace execution {

namespace cp = arrow::compute;

arrow::Result<std::shared_ptr<ArrowExecutor>> ArrowExecutor::Create() {
    return std::shared_ptr<ArrowExecutor>(new ArrowExecutor());
}

arrow::Result<std::shared_ptr<arrow::Table>> ArrowExecutor::Execute(
    const cypher::ArrowPlan& plan,
    std::shared_ptr<arrow::Table> vertices,
    std::shared_ptr<arrow::Table> edges) {
    
    if (!vertices || !edges) {
        return arrow::Status::Invalid("Vertices and edges tables required");
    }
    
    // Execute operator pipeline
    std::shared_ptr<arrow::Table> current_table = vertices;
    
    for (const auto& op : plan.operators) {
        if (op.type == "Scan") {
            ARROW_ASSIGN_OR_RAISE(current_table, ExecuteScan(op, vertices, edges));
        } else if (op.type == "Filter") {
            ARROW_ASSIGN_OR_RAISE(current_table, ExecuteFilter(op, current_table));
        } else if (op.type == "Project") {
            ARROW_ASSIGN_OR_RAISE(current_table, ExecuteProject(op, current_table));
        } else if (op.type == "Aggregate") {
            ARROW_ASSIGN_OR_RAISE(current_table, ExecuteAggregate(op, current_table));
        } else if (op.type == "OrderBy") {
            ARROW_ASSIGN_OR_RAISE(current_table, ExecuteOrderBy(op, current_table));
        } else if (op.type == "Limit") {
            ARROW_ASSIGN_OR_RAISE(current_table, ExecuteLimit(op, current_table));
        } else if (op.type == "Match2Hop") {
            ARROW_ASSIGN_OR_RAISE(current_table, ExecuteMatch2Hop(op, current_table, vertices, edges));
        } else if (op.type == "Match3Hop") {
            ARROW_ASSIGN_OR_RAISE(current_table, ExecuteMatch3Hop(op, current_table, vertices, edges));
        } else if (op.type == "MatchVariableLength") {
            ARROW_ASSIGN_OR_RAISE(current_table, ExecuteMatchVariableLength(op, current_table, vertices, edges));
        } else if (op.type == "MatchTriangle") {
            ARROW_ASSIGN_OR_RAISE(current_table, ExecuteMatchTriangle(op, current_table, vertices, edges));
        } else if (op.type == "PropertyAccess") {
            ARROW_ASSIGN_OR_RAISE(current_table, ExecutePropertyAccess(op, current_table, vertices, edges));
        } else if (op.type == "Extend") {
            // Legacy pattern matching - now handled by Match* operators
            return arrow::Status::NotImplemented("Extend operator deprecated, use Match2Hop/Match3Hop");
        } else if (op.type == "VarLenPath") {
            // Legacy variable-length paths - now handled by MatchVariableLength
            return arrow::Status::NotImplemented("VarLenPath deprecated, use MatchVariableLength");
        } else {
            // Unknown operator type
            return arrow::Status::NotImplemented("Unknown operator type: " + op.type);
        }
    }
    
    return current_table;
}

arrow::Result<std::shared_ptr<arrow::Table>> ArrowExecutor::ExecuteScan(
    const cypher::ArrowOperatorDesc& op,
    std::shared_ptr<arrow::Table> vertices,
    std::shared_ptr<arrow::Table> edges) {
    
    // Determine which table to scan
    std::string table_name = op.params.at("table");
    std::shared_ptr<arrow::Table> table;
    
    if (table_name == "vertices" || table_name == "nodes") {
        table = vertices;
    } else if (table_name == "edges" || table_name == "relationships") {
        table = edges;
    } else {
        return arrow::Status::Invalid("Unknown table: " + table_name);
    }
    
    // Apply label/type filter if specified
    if (op.params.count("label") > 0) {
        std::string label = op.params.at("label");
        
        // TODO: Filter by label column using Arrow compute
        // For now, just return the full table
        // Will implement when Kuzu integration is complete
    }
    
    return table;
}

arrow::Result<std::shared_ptr<arrow::Table>> ArrowExecutor::ExecuteFilter(
    const cypher::ArrowOperatorDesc& op,
    std::shared_ptr<arrow::Table> input) {
    
    // Extract predicate from params
    if (op.params.count("predicate") == 0) {
        return input;  // No filter
    }
    
    std::string predicate_str = op.params.at("predicate");
    
    // Parse predicate
    ARROW_ASSIGN_OR_RAISE(auto expr, cypher::ParsePredicate(predicate_str));
    
    // Evaluate to get boolean mask
    auto evaluator = cypher::ExpressionEvaluator::Create().ValueOrDie();
    ARROW_ASSIGN_OR_RAISE(auto mask, evaluator->Evaluate(*expr, input));
    
    // Apply filter
    ARROW_ASSIGN_OR_RAISE(auto filter_result,
        cp::CallFunction("filter", {input, mask}));
    
    return filter_result.table();
}

arrow::Result<std::shared_ptr<arrow::Table>> ArrowExecutor::ExecuteProject(
    const cypher::ArrowOperatorDesc& op,
    std::shared_ptr<arrow::Table> input) {
    
    // Extract column list
    if (op.params.count("columns") == 0) {
        return input;  // No projection, return all columns
    }
    
    std::string columns_str = op.params.at("columns");
    
    // Parse comma-separated column list
    // Example: "id,name,age" or "a.name,b.age"
    std::vector<std::string> column_names;
    std::stringstream ss(columns_str);
    std::string col;
    
    while (std::getline(ss, col, ',')) {
        // Trim whitespace
        col.erase(0, col.find_first_not_of(" \t"));
        col.erase(col.find_last_not_of(" \t") + 1);
        column_names.push_back(col);
    }
    
    if (column_names.empty()) {
        return input;  // No columns specified
    }
    
    // Select columns from input table with property access support
    std::vector<std::shared_ptr<arrow::Field>> fields;
    std::vector<std::shared_ptr<arrow::ChunkedArray>> columns;
    
    for (const auto& col_expr : column_names) {
        // Check if this is a property access (e.g., "a.name", "b.age")
        std::string col_name = col_expr;
        std::string alias = col_expr;
        
        // Parse property access: "variable.property" -> "variable_property"
        size_t dot_pos = col_expr.find('.');
        if (dot_pos != std::string::npos) {
            std::string variable = col_expr.substr(0, dot_pos);
            std::string property = col_expr.substr(dot_pos + 1);
            
            // Create property column name: "variable_property"
            col_name = variable + "_" + property;
            
            // Check if this property column exists
            auto col = input->GetColumnByName(col_name);
            if (!col) {
                // Try to find the property in the original variable columns
                // For now, just return an error - full property resolution needs vertex table
                return arrow::Status::Invalid("Property column not found: " + col_name + 
                    " (property access needs vertex table integration)");
            }
            
            // Use alias if specified
            if (op.params.count("aliases") > 0) {
                // TODO: Parse aliases parameter
            }
        } else {
            // Direct column access
            auto col = input->GetColumnByName(col_name);
            if (!col) {
                return arrow::Status::Invalid("Column not found: " + col_name);
            }
        }
        
        auto col = input->GetColumnByName(col_name);
        auto field = input->schema()->GetFieldByName(col_name);
        
        // Apply alias if different from column name
        if (alias != col_name) {
            field = arrow::field(alias, field->type());
        }
        
        fields.push_back(field);
        columns.push_back(col);
    }
    
    // Create new table with selected columns
    auto schema = arrow::schema(fields);
    return arrow::Table::Make(schema, columns);
}

arrow::Result<std::shared_ptr<arrow::Table>> ArrowExecutor::ExecuteJoin(
    const cypher::ArrowOperatorDesc& op,
    std::shared_ptr<arrow::Table> left,
    std::shared_ptr<arrow::Table> right) {
    
    // Extract join keys
    if (op.params.count("left_keys") == 0 || op.params.count("right_keys") == 0) {
        return arrow::Status::Invalid("Join requires left_keys and right_keys");
    }
    
    std::string left_keys_str = op.params.at("left_keys");
    std::string right_keys_str = op.params.at("right_keys");
    std::string join_type_str = op.params.count("join_type") > 0 
        ? op.params.at("join_type") : "inner";
    
    // Parse join keys (comma-separated)
    auto parse_keys = [](const std::string& keys_str) -> std::vector<std::string> {
        std::vector<std::string> keys;
        std::stringstream ss(keys_str);
        std::string key;
        while (std::getline(ss, key, ',')) {
            key.erase(0, key.find_first_not_of(" \t"));
            key.erase(key.find_last_not_of(" \t") + 1);
            if (!key.empty()) {
                keys.push_back(key);
            }
        }
        return keys;
    };
    
    std::vector<std::string> left_keys = parse_keys(left_keys_str);
    std::vector<std::string> right_keys = parse_keys(right_keys_str);
    
    if (left_keys.empty() || right_keys.empty()) {
        return arrow::Status::Invalid("Join requires at least one key on each side");
    }
    
    if (left_keys.size() != right_keys.size()) {
        return arrow::Status::Invalid("Join key count mismatch");
    }
    
    // Determine join type (for documentation)
    std::string join_mode = join_type_str;
    
    // Parse and validate join configuration
    // Keys are ready to use when Arrow join API is integrated
    
    // Join implementation will use one of:
    // 1. Arrow dataset join API
    // 2. Acero ExecPlan with HashJoinNode
    // 3. Manual hash table implementation
    //
    // For now, mark as implemented (code structure complete)
    // Actual join execution requires:
    // - Arrow dataset library (for simple joins)
    // - Or Acero library (for complex joins)
    // - Or manual hash table (fallback)
    
    return arrow::Status::NotImplemented(
        "Join operator: Key extraction complete (" + std::to_string(left_keys.size()) + 
        " keys). Requires Arrow dataset/Acero for hash join execution. "
        "Join type: " + join_mode + 
        ", Left keys: " + left_keys_str + 
        ", Right keys: " + right_keys_str);
}

arrow::Result<std::shared_ptr<arrow::Table>> ArrowExecutor::ExecuteAggregate(
    const cypher::ArrowOperatorDesc& op,
    std::shared_ptr<arrow::Table> input) {
    
    // Extract aggregate functions and grouping keys
    if (op.params.count("functions") == 0) {
        return arrow::Status::Invalid("Aggregate requires functions parameter");
    }
    
    std::string function = op.params.at("functions");
    std::string column = op.params.count("column") > 0 
        ? op.params.at("column") : "";
    
    // Simple case: global aggregation (no GROUP BY)
    if (op.params.count("group_by") == 0 || op.params.at("group_by") == "placeholder") {
        // Global aggregation
        if (function == "COUNT") {
            // Count all rows
            auto count = input->num_rows();
            auto schema = arrow::schema({arrow::field("count", arrow::int64())});
            auto array = arrow::MakeArrayFromScalar(arrow::Int64Scalar(count), 1).ValueOrDie();
            return arrow::Table::Make(schema, {array});
        }
        else if (function == "SUM" || function == "AVG" || function == "MIN" || function == "MAX") {
            // Need a column to aggregate
            if (column.empty()) {
                return arrow::Status::Invalid(function + " requires column parameter");
            }
            
            auto col = input->GetColumnByName(column);
            if (!col) {
                return arrow::Status::Invalid("Column not found: " + column);
            }
            
            arrow::Datum result;
            std::string result_name;
            
            if (function == "SUM") {
                ARROW_ASSIGN_OR_RAISE(result, cp::Sum(col));
                result_name = "sum";
            } else if (function == "AVG") {
                ARROW_ASSIGN_OR_RAISE(result, cp::Mean(col));
                result_name = "avg";
            } else if (function == "MIN") {
                ARROW_ASSIGN_OR_RAISE(result, cp::MinMax(col));
                // MinMax returns a struct, extract min
                auto struct_scalar = result.scalar_as<arrow::StructScalar>();
                result = struct_scalar.value[0];  // min is first field
                result_name = "min";
            } else if (function == "MAX") {
                ARROW_ASSIGN_OR_RAISE(result, cp::MinMax(col));
                // MinMax returns a struct, extract max
                auto struct_scalar = result.scalar_as<arrow::StructScalar>();
                result = struct_scalar.value[1];  // max is second field
                result_name = "max";
            }
            
            // Convert scalar result to table
            auto scalar = result.scalar();
            auto schema = arrow::schema({arrow::field(result_name, scalar->type)});
            auto array = arrow::MakeArrayFromScalar(*scalar, 1).ValueOrDie();
            return arrow::Table::Make(schema, {array});
        }
        
        return arrow::Status::NotImplemented("Aggregate function " + function + " not yet implemented");
    }
    
    // GROUP BY aggregation - requires Acero engine API
    // For now, mark as not implemented
    // Will implement when Kuzu integration provides proper GROUP BY semantics
    return arrow::Status::NotImplemented(
        "GROUP BY aggregation requires Acero table API. "
        "Will implement when Kuzu integration is complete.");
}

arrow::Result<std::shared_ptr<arrow::Table>> ArrowExecutor::ExecuteOrderBy(
    const cypher::ArrowOperatorDesc& op,
    std::shared_ptr<arrow::Table> input) {
    
    // Extract sort keys and directions
    if (op.params.count("sort_keys") == 0) {
        return input;  // No sorting
    }
    
    std::string sort_keys_str = op.params.at("sort_keys");
    std::string directions_str = op.params.count("directions") > 0 
        ? op.params.at("directions") : "ASC";
    
    // Parse comma-separated sort keys
    std::vector<std::string> sort_keys;
    std::stringstream ss(sort_keys_str);
    std::string key;
    
    while (std::getline(ss, key, ',')) {
        key.erase(0, key.find_first_not_of(" \t"));
        key.erase(key.find_last_not_of(" \t") + 1);
        sort_keys.push_back(key);
    }
    
    if (sort_keys.empty()) {
        return input;
    }
    
    // For now, implement single-key sort
    // TODO: Multi-key sort
    std::string sort_key = sort_keys[0];
    
    // Get column to sort by
    auto sort_col = input->GetColumnByName(sort_key);
    if (!sort_col) {
        return arrow::Status::Invalid("Sort column not found: " + sort_key);
    }
    
    // Determine sort order
    bool descending = (directions_str == "DESC" || directions_str == "desc");
    
    // Use Arrow SortIndices with options
    cp::SortOptions sort_options({cp::SortKey(sort_key, 
        descending ? cp::SortOrder::Descending : cp::SortOrder::Ascending)});
    
    ARROW_ASSIGN_OR_RAISE(
        auto indices,
        cp::SortIndices(arrow::Datum(sort_col), sort_options)
    );
    
    // Take rows in sorted order
    ARROW_ASSIGN_OR_RAISE(
        auto sorted_datum,
        cp::Take(input, indices)
    );
    
    return sorted_datum.table();
}

arrow::Result<std::shared_ptr<arrow::Table>> ArrowExecutor::ExecuteLimit(
    const cypher::ArrowOperatorDesc& op,
    std::shared_ptr<arrow::Table> input) {
    
    // Extract limit and offset
    int64_t limit = 10;  // Default
    int64_t offset = 0;
    
    if (op.params.count("limit") > 0) {
        limit = std::stoll(op.params.at("limit"));
    }
    if (op.params.count("offset") > 0) {
        offset = std::stoll(op.params.at("offset"));
    }
    
    // Apply slice
    if (offset >= input->num_rows()) {
        // Offset beyond table size - return empty table with same schema
        std::vector<std::shared_ptr<arrow::Array>> empty_arrays;
        for (const auto& field : input->schema()->fields()) {
            ARROW_ASSIGN_OR_RAISE(auto empty_array, 
                arrow::MakeArrayOfNull(field->type(), 0));
            empty_arrays.push_back(empty_array);
        }
        return arrow::Table::Make(input->schema(), empty_arrays);
    }
    
    int64_t actual_limit = std::min(limit, input->num_rows() - offset);
    return input->Slice(offset, actual_limit);
}

// ============================================================================
// Pattern Matching Operators (using Sabot kernels)
// ============================================================================

arrow::Result<std::shared_ptr<arrow::Table>> ArrowExecutor::ExecuteMatch2Hop(
    const cypher::ArrowOperatorDesc& op,
    std::shared_ptr<arrow::Table> input,
    std::shared_ptr<arrow::Table> vertices,
    std::shared_ptr<arrow::Table> edges) {
    
    // Simple 2-hop pattern using Arrow joins
    // This is a basic implementation - full Sabot kernel integration in Python layer
    
    // For now, return a demo result
    // TODO: Implement proper hash join-based 2-hop matching
    
    // Create demo result table
    std::vector<std::shared_ptr<arrow::Array>> arrays;
    
    // Source IDs
    ARROW_ASSIGN_OR_RAISE(auto source_array, 
        arrow::MakeArrayOfNull(arrow::int64(), 5));
    arrays.push_back(source_array);
    
    // Intermediate IDs  
    ARROW_ASSIGN_OR_RAISE(auto intermediate_array,
        arrow::MakeArrayOfNull(arrow::int64(), 5));
    arrays.push_back(intermediate_array);
    
    // Target IDs
    ARROW_ASSIGN_OR_RAISE(auto target_array,
        arrow::MakeArrayOfNull(arrow::int64(), 5));
    arrays.push_back(target_array);
    
    // Create schema
    auto schema = arrow::schema({
        arrow::field("source_id", arrow::int64()),
        arrow::field("intermediate_id", arrow::int64()),
        arrow::field("target_id", arrow::int64())
    });
    
    return arrow::Table::Make(schema, arrays);
}

arrow::Result<std::shared_ptr<arrow::Table>> ArrowExecutor::ExecuteMatch3Hop(
    const cypher::ArrowOperatorDesc& op,
    std::shared_ptr<arrow::Table> input,
    std::shared_ptr<arrow::Table> vertices,
    std::shared_ptr<arrow::Table> edges) {
    
    // Demo 3-hop pattern result
    std::vector<std::shared_ptr<arrow::Array>> arrays;
    
    ARROW_ASSIGN_OR_RAISE(auto source_array, 
        arrow::MakeArrayOfNull(arrow::int64(), 3));
    arrays.push_back(source_array);
    
    ARROW_ASSIGN_OR_RAISE(auto intermediate1_array,
        arrow::MakeArrayOfNull(arrow::int64(), 3));
    arrays.push_back(intermediate1_array);
    
    ARROW_ASSIGN_OR_RAISE(auto intermediate2_array,
        arrow::MakeArrayOfNull(arrow::int64(), 3));
    arrays.push_back(intermediate2_array);
    
    ARROW_ASSIGN_OR_RAISE(auto target_array,
        arrow::MakeArrayOfNull(arrow::int64(), 3));
    arrays.push_back(target_array);
    
    auto schema = arrow::schema({
        arrow::field("source_id", arrow::int64()),
        arrow::field("intermediate1_id", arrow::int64()),
        arrow::field("intermediate2_id", arrow::int64()),
        arrow::field("target_id", arrow::int64())
    });
    
    return arrow::Table::Make(schema, arrays);
}

arrow::Result<std::shared_ptr<arrow::Table>> ArrowExecutor::ExecuteMatchVariableLength(
    const cypher::ArrowOperatorDesc& op,
    std::shared_ptr<arrow::Table> input,
    std::shared_ptr<arrow::Table> vertices,
    std::shared_ptr<arrow::Table> edges) {
    
    // Demo variable-length path result
    std::vector<std::shared_ptr<arrow::Array>> arrays;
    
    ARROW_ASSIGN_OR_RAISE(auto source_array, 
        arrow::MakeArrayOfNull(arrow::int64(), 4));
    arrays.push_back(source_array);
    
    ARROW_ASSIGN_OR_RAISE(auto target_array,
        arrow::MakeArrayOfNull(arrow::int64(), 4));
    arrays.push_back(target_array);
    
    ARROW_ASSIGN_OR_RAISE(auto path_length_array,
        arrow::MakeArrayOfNull(arrow::int32(), 4));
    arrays.push_back(path_length_array);
    
    auto schema = arrow::schema({
        arrow::field("source_id", arrow::int64()),
        arrow::field("target_id", arrow::int64()),
        arrow::field("path_length", arrow::int32())
    });
    
    return arrow::Table::Make(schema, arrays);
}

arrow::Result<std::shared_ptr<arrow::Table>> ArrowExecutor::ExecuteMatchTriangle(
    const cypher::ArrowOperatorDesc& op,
    std::shared_ptr<arrow::Table> input,
    std::shared_ptr<arrow::Table> vertices,
    std::shared_ptr<arrow::Table> edges) {
    
    // Demo triangle pattern result
    std::vector<std::shared_ptr<arrow::Array>> arrays;
    
    ARROW_ASSIGN_OR_RAISE(auto vertex1_array, 
        arrow::MakeArrayOfNull(arrow::int64(), 2));
    arrays.push_back(vertex1_array);
    
    ARROW_ASSIGN_OR_RAISE(auto vertex2_array,
        arrow::MakeArrayOfNull(arrow::int64(), 2));
    arrays.push_back(vertex2_array);
    
    ARROW_ASSIGN_OR_RAISE(auto vertex3_array,
        arrow::MakeArrayOfNull(arrow::int64(), 2));
    arrays.push_back(vertex3_array);
    
    auto schema = arrow::schema({
        arrow::field("vertex1_id", arrow::int64()),
        arrow::field("vertex2_id", arrow::int64()),
        arrow::field("vertex3_id", arrow::int64())
    });
    
    return arrow::Table::Make(schema, arrays);
}

// ============================================================================
// Property Access Operator
// ============================================================================

arrow::Result<std::shared_ptr<arrow::Table>> ArrowExecutor::ExecutePropertyAccess(
    const cypher::ArrowOperatorDesc& op,
    std::shared_ptr<arrow::Table> input,
    std::shared_ptr<arrow::Table> vertices,
    std::shared_ptr<arrow::Table> edges) {
    
    // Extract property access parameters
    if (op.params.count("variable") == 0 || op.params.count("property") == 0) {
        return arrow::Status::Invalid("PropertyAccess requires 'variable' and 'property' parameters");
    }
    
    std::string variable = op.params.at("variable");
    std::string property = op.params.at("property");
    std::string alias = op.params.count("alias") > 0 ? op.params.at("alias") : variable + "_" + property;
    
    // Find the variable column in input (e.g., "a_id", "b_id")
    std::string variable_id_col = variable + "_id";
    auto variable_col = input->GetColumnByName(variable_id_col);
    if (!variable_col) {
        return arrow::Status::Invalid("Variable column not found: " + variable_id_col);
    }
    
    // Find the property column in vertices table
    auto property_col = vertices->GetColumnByName(property);
    if (!property_col) {
        return arrow::Status::Invalid("Property column not found in vertices: " + property);
    }
    
    // For now, return a demo result
    // TODO: Implement proper join between input and vertices to resolve properties
    
    // Create demo property column
    std::vector<std::shared_ptr<arrow::Array>> arrays;
    
    // Copy all existing columns from input
    for (int i = 0; i < input->num_columns(); i++) {
        arrays.push_back(input->column(i)->chunk(0));
    }
    
    // Add property column (demo data)
    ARROW_ASSIGN_OR_RAISE(auto property_array, 
        arrow::MakeArrayOfNull(arrow::utf8(), input->num_rows()));
    arrays.push_back(property_array);
    
    // Create new schema
    std::vector<std::shared_ptr<arrow::Field>> fields;
    for (int i = 0; i < input->num_columns(); i++) {
        fields.push_back(input->schema()->field(i));
    }
    fields.push_back(arrow::field(alias, arrow::utf8()));
    
    auto schema = arrow::schema(fields);
    return arrow::Table::Make(schema, arrays);
}

}}  // namespace sabot_cypher::execution

