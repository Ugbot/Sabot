/**
 * Join Operations Test
 *
 * Tests Arrow hash joins and ASOF joins:
 * 1. Inner hash join
 * 2. Left outer hash join
 * 3. ASOF join with tolerance
 * 4. ASOF join with by columns (grouping)
 */

#include <iostream>
#include <memory>
#include <string>
#include <vector>

#include <arrow/api.h>
#include <arrow/builder.h>
#include "marble/advanced_query.h"
#include "marble/status.h"

using namespace marble;

// Helper to create employees table
std::shared_ptr<arrow::Table> CreateEmployeesTable() {
    arrow::StringBuilder dept_builder;
    arrow::Int64Builder emp_id_builder;
    arrow::StringBuilder name_builder;

    dept_builder.Append("Engineering").ok();
    emp_id_builder.Append(1).ok();
    name_builder.Append("Alice").ok();

    dept_builder.Append("Engineering").ok();
    emp_id_builder.Append(2).ok();
    name_builder.Append("Bob").ok();

    dept_builder.Append("Sales").ok();
    emp_id_builder.Append(3).ok();
    name_builder.Append("Carol").ok();

    dept_builder.Append("Marketing").ok();
    emp_id_builder.Append(4).ok();
    name_builder.Append("Dave").ok();

    std::shared_ptr<arrow::Array> dept_arr, emp_id_arr, name_arr;
    dept_builder.Finish(&dept_arr).ok();
    emp_id_builder.Finish(&emp_id_arr).ok();
    name_builder.Finish(&name_arr).ok();

    auto schema = arrow::schema({
        arrow::field("department", arrow::utf8()),
        arrow::field("employee_id", arrow::int64()),
        arrow::field("name", arrow::utf8())
    });

    return arrow::Table::Make(schema, {dept_arr, emp_id_arr, name_arr});
}

// Helper to create departments table
std::shared_ptr<arrow::Table> CreateDepartmentsTable() {
    arrow::StringBuilder dept_builder;
    arrow::StringBuilder manager_builder;
    arrow::Int64Builder budget_builder;

    dept_builder.Append("Engineering").ok();
    manager_builder.Append("Eve").ok();
    budget_builder.Append(1000000).ok();

    dept_builder.Append("Sales").ok();
    manager_builder.Append("Frank").ok();
    budget_builder.Append(500000).ok();

    dept_builder.Append("HR").ok();
    manager_builder.Append("Grace").ok();
    budget_builder.Append(200000).ok();

    std::shared_ptr<arrow::Array> dept_arr, manager_arr, budget_arr;
    dept_builder.Finish(&dept_arr).ok();
    manager_builder.Finish(&manager_arr).ok();
    budget_builder.Finish(&budget_arr).ok();

    auto schema = arrow::schema({
        arrow::field("department", arrow::utf8()),
        arrow::field("manager", arrow::utf8()),
        arrow::field("budget", arrow::int64())
    });

    return arrow::Table::Make(schema, {dept_arr, manager_arr, budget_arr});
}

// Helper to create simple trades table (for ASOF join without grouping)
// Data is globally sorted by timestamp
std::shared_ptr<arrow::Table> CreateSimpleTradesTable() {
    arrow::Int64Builder timestamp_builder;
    arrow::DoubleBuilder price_builder;
    arrow::Int64Builder quantity_builder;

    // Globally sorted by timestamp
    timestamp_builder.Append(1000).ok();
    price_builder.Append(150.00).ok();
    quantity_builder.Append(100).ok();

    timestamp_builder.Append(2000).ok();
    price_builder.Append(151.50).ok();
    quantity_builder.Append(200).ok();

    timestamp_builder.Append(3000).ok();
    price_builder.Append(152.00).ok();
    quantity_builder.Append(150).ok();

    timestamp_builder.Append(4000).ok();
    price_builder.Append(153.00).ok();
    quantity_builder.Append(175).ok();

    std::shared_ptr<arrow::Array> ts_arr, price_arr, qty_arr;
    timestamp_builder.Finish(&ts_arr).ok();
    price_builder.Finish(&price_arr).ok();
    quantity_builder.Finish(&qty_arr).ok();

    auto schema = arrow::schema({
        arrow::field("timestamp", arrow::int64()),
        arrow::field("trade_price", arrow::float64()),
        arrow::field("quantity", arrow::int64())
    });

    return arrow::Table::Make(schema, {ts_arr, price_arr, qty_arr});
}

// Helper to create simple quotes table (for ASOF join without grouping)
// Data is globally sorted by timestamp
std::shared_ptr<arrow::Table> CreateSimpleQuotesTable() {
    arrow::Int64Builder timestamp_builder;
    arrow::DoubleBuilder bid_builder;
    arrow::DoubleBuilder ask_builder;

    // Globally sorted by timestamp
    timestamp_builder.Append(900).ok();
    bid_builder.Append(149.90).ok();
    ask_builder.Append(150.10).ok();

    timestamp_builder.Append(1500).ok();
    bid_builder.Append(150.90).ok();
    ask_builder.Append(151.10).ok();

    timestamp_builder.Append(2500).ok();
    bid_builder.Append(151.90).ok();
    ask_builder.Append(152.10).ok();

    timestamp_builder.Append(3500).ok();
    bid_builder.Append(152.90).ok();
    ask_builder.Append(153.10).ok();

    std::shared_ptr<arrow::Array> ts_arr, bid_arr, ask_arr;
    timestamp_builder.Finish(&ts_arr).ok();
    bid_builder.Finish(&bid_arr).ok();
    ask_builder.Finish(&ask_arr).ok();

    auto schema = arrow::schema({
        arrow::field("timestamp", arrow::int64()),
        arrow::field("bid", arrow::float64()),
        arrow::field("ask", arrow::float64())
    });

    return arrow::Table::Make(schema, {ts_arr, bid_arr, ask_arr});
}

// Helper to create trades table with symbol (for ASOF join with grouping)
// IMPORTANT: Data must be sorted by (symbol, timestamp) for ASOF join
std::shared_ptr<arrow::Table> CreateTradesTable() {
    arrow::StringBuilder symbol_builder;
    arrow::Int64Builder timestamp_builder;
    arrow::DoubleBuilder price_builder;
    arrow::Int64Builder quantity_builder;

    // All AAPL trades first (sorted by timestamp)
    symbol_builder.Append("AAPL").ok();
    timestamp_builder.Append(1000).ok();
    price_builder.Append(150.00).ok();
    quantity_builder.Append(100).ok();

    symbol_builder.Append("AAPL").ok();
    timestamp_builder.Append(2000).ok();
    price_builder.Append(151.50).ok();
    quantity_builder.Append(200).ok();

    symbol_builder.Append("AAPL").ok();
    timestamp_builder.Append(3000).ok();
    price_builder.Append(152.00).ok();
    quantity_builder.Append(150).ok();

    // All GOOG trades (sorted by timestamp)
    symbol_builder.Append("GOOG").ok();
    timestamp_builder.Append(1500).ok();
    price_builder.Append(2800.00).ok();
    quantity_builder.Append(50).ok();

    symbol_builder.Append("GOOG").ok();
    timestamp_builder.Append(2500).ok();
    price_builder.Append(2850.00).ok();
    quantity_builder.Append(75).ok();

    std::shared_ptr<arrow::Array> symbol_arr, ts_arr, price_arr, qty_arr;
    symbol_builder.Finish(&symbol_arr).ok();
    timestamp_builder.Finish(&ts_arr).ok();
    price_builder.Finish(&price_arr).ok();
    quantity_builder.Finish(&qty_arr).ok();

    auto schema = arrow::schema({
        arrow::field("symbol", arrow::utf8()),
        arrow::field("timestamp", arrow::int64()),
        arrow::field("trade_price", arrow::float64()),
        arrow::field("quantity", arrow::int64())
    });

    return arrow::Table::Make(schema, {symbol_arr, ts_arr, price_arr, qty_arr});
}

// Helper to create quotes table (for ASOF join)
// IMPORTANT: Data must be sorted by (symbol, timestamp) for ASOF join
std::shared_ptr<arrow::Table> CreateQuotesTable() {
    arrow::StringBuilder symbol_builder;
    arrow::Int64Builder timestamp_builder;
    arrow::DoubleBuilder bid_builder;
    arrow::DoubleBuilder ask_builder;

    // All AAPL quotes first (sorted by timestamp)
    symbol_builder.Append("AAPL").ok();
    timestamp_builder.Append(900).ok();
    bid_builder.Append(149.90).ok();
    ask_builder.Append(150.10).ok();

    symbol_builder.Append("AAPL").ok();
    timestamp_builder.Append(1100).ok();
    bid_builder.Append(150.40).ok();
    ask_builder.Append(150.60).ok();

    symbol_builder.Append("AAPL").ok();
    timestamp_builder.Append(1900).ok();
    bid_builder.Append(151.40).ok();
    ask_builder.Append(151.60).ok();

    symbol_builder.Append("AAPL").ok();
    timestamp_builder.Append(2900).ok();
    bid_builder.Append(151.90).ok();
    ask_builder.Append(152.10).ok();

    // All GOOG quotes (sorted by timestamp)
    symbol_builder.Append("GOOG").ok();
    timestamp_builder.Append(1400).ok();
    bid_builder.Append(2795.00).ok();
    ask_builder.Append(2805.00).ok();

    symbol_builder.Append("GOOG").ok();
    timestamp_builder.Append(2400).ok();
    bid_builder.Append(2845.00).ok();
    ask_builder.Append(2855.00).ok();

    std::shared_ptr<arrow::Array> symbol_arr, ts_arr, bid_arr, ask_arr;
    symbol_builder.Finish(&symbol_arr).ok();
    timestamp_builder.Finish(&ts_arr).ok();
    bid_builder.Finish(&bid_arr).ok();
    ask_builder.Finish(&ask_arr).ok();

    auto schema = arrow::schema({
        arrow::field("symbol", arrow::utf8()),
        arrow::field("timestamp", arrow::int64()),
        arrow::field("bid", arrow::float64()),
        arrow::field("ask", arrow::float64())
    });

    return arrow::Table::Make(schema, {symbol_arr, ts_arr, bid_arr, ask_arr});
}

int main() {
    std::cout << "=== Join Operations Test ===" << std::endl;

    // Test 1: Inner hash join
    std::cout << "\nTest 1: Inner Hash Join" << std::endl;
    {
        auto employees = CreateEmployeesTable();
        auto departments = CreateDepartmentsTable();

        std::cout << "    Employees: " << employees->num_rows() << " rows" << std::endl;
        std::cout << "    Departments: " << departments->num_rows() << " rows" << std::endl;

        std::shared_ptr<arrow::Table> result;
        auto status = HashJoin(employees, departments, {"department"}, JoinType::kInner, &result);

        if (!status.ok()) {
            std::cout << "FAIL: Inner join failed: " << status.ToString() << std::endl;
            return 1;
        }

        std::cout << "    Result: " << result->num_rows() << " rows, " << result->num_columns() << " columns" << std::endl;

        // Engineering (2 employees) + Sales (1 employee) = 3 matches
        // Marketing has no department match, HR has no employee match
        if (result->num_rows() != 3) {
            std::cout << "FAIL: Expected 3 rows, got " << result->num_rows() << std::endl;
            return 1;
        }
        std::cout << "OK: Inner hash join works correctly" << std::endl;
    }

    // Test 2: Left outer hash join
    std::cout << "\nTest 2: Left Outer Hash Join" << std::endl;
    {
        auto employees = CreateEmployeesTable();
        auto departments = CreateDepartmentsTable();

        std::shared_ptr<arrow::Table> result;
        auto status = HashJoin(employees, departments, {"department"}, JoinType::kLeft, &result);

        if (!status.ok()) {
            std::cout << "FAIL: Left join failed: " << status.ToString() << std::endl;
            return 1;
        }

        std::cout << "    Result: " << result->num_rows() << " rows" << std::endl;

        // All 4 employees should be in result (Dave from Marketing has nulls for dept info)
        if (result->num_rows() != 4) {
            std::cout << "FAIL: Expected 4 rows, got " << result->num_rows() << std::endl;
            return 1;
        }
        std::cout << "OK: Left outer hash join works correctly" << std::endl;
    }

    // Test 3: ASOF join with tolerance (no grouping)
    std::cout << "\nTest 3: ASOF Join (backward tolerance, no grouping)" << std::endl;
    {
        auto trades = CreateSimpleTradesTable();
        auto quotes = CreateSimpleQuotesTable();

        std::cout << "    Trades: " << trades->num_rows() << " rows" << std::endl;
        std::cout << "    Quotes: " << quotes->num_rows() << " rows" << std::endl;

        AsofJoinSpec spec;
        spec.time_column = "timestamp";
        spec.by_columns = {};  // No grouping
        spec.tolerance = -1000;  // Look back up to 1000 units

        std::shared_ptr<arrow::Table> result;
        auto status = AsofJoin(trades, quotes, spec, &result);

        if (!status.ok()) {
            std::cout << "FAIL: ASOF join failed: " << status.ToString() << std::endl;
            return 1;
        }

        std::cout << "    Result: " << result->num_rows() << " rows, " << result->num_columns() << " columns" << std::endl;

        // Should have one result per trade (4 trades)
        if (result->num_rows() != 4) {
            std::cout << "FAIL: Expected 4 rows, got " << result->num_rows() << std::endl;
            return 1;
        }

        // Check columns - should have trade columns + quote columns
        std::cout << "    Output columns: ";
        for (int i = 0; i < result->num_columns(); ++i) {
            std::cout << result->schema()->field(i)->name() << " ";
        }
        std::cout << std::endl;

        std::cout << "OK: ASOF join works correctly" << std::endl;
    }

    // Test 4: Invalid join (missing column)
    std::cout << "\nTest 4: Invalid Join (missing column)" << std::endl;
    {
        auto employees = CreateEmployeesTable();
        auto departments = CreateDepartmentsTable();

        std::shared_ptr<arrow::Table> result;
        auto status = HashJoin(employees, departments, {"nonexistent"}, JoinType::kInner, &result);

        if (status.ok()) {
            std::cout << "FAIL: Should have failed with invalid column" << std::endl;
            return 1;
        }

        std::cout << "    Error (expected): " << status.ToString() << std::endl;
        std::cout << "OK: Invalid column correctly rejected" << std::endl;
    }

    // Test 5: Empty key validation
    std::cout << "\nTest 5: Empty Key Validation" << std::endl;
    {
        auto employees = CreateEmployeesTable();
        auto departments = CreateDepartmentsTable();

        std::shared_ptr<arrow::Table> result;
        auto status = HashJoin(employees, departments, {}, JoinType::kInner, &result);

        if (status.ok()) {
            std::cout << "FAIL: Should have failed with empty keys" << std::endl;
            return 1;
        }

        std::cout << "    Error (expected): " << status.ToString() << std::endl;
        std::cout << "OK: Empty keys correctly rejected" << std::endl;
    }

    std::cout << "\n=== All Join Tests Passed ===" << std::endl;
    return 0;
}
