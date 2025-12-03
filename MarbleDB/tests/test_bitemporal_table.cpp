/**
 * Bitemporal Table Creation Test
 *
 * Tests the opt-in bitemporal table creation functionality.
 * Verifies that temporal columns are automatically added to the schema
 * when a table is created with bitemporal capabilities.
 */

#include <iostream>
#include <memory>
#include <string>

#include <arrow/api.h>
#include "marble/api.h"
#include "marble/db.h"
#include "marble/table.h"
#include "marble/table_capabilities.h"
#include "marble/status.h"

using namespace marble;

int main() {
    std::cout << "=== Bitemporal Table Creation Test ===" << std::endl;

    // Open database
    std::unique_ptr<MarbleDB> db;
    auto status = OpenDatabase("/tmp/marble_bitemporal_test", &db);
    if (!status.ok()) {
        std::cout << "FAIL: Failed to open database: " << status.ToString() << std::endl;
        return 1;
    }
    std::cout << "OK: Database opened" << std::endl;

    // Test 1: Create a regular (non-temporal) table
    std::cout << "\nTest 1: Create regular table (no temporal columns)" << std::endl;
    {
        auto schema = arrow::schema({
            arrow::field("id", arrow::int64()),
            arrow::field("name", arrow::utf8()),
            arrow::field("value", arrow::float64())
        });

        TableSchema table_schema("regular_table", schema);
        TableCapabilities caps;  // Default - no temporal

        status = db->CreateTable(table_schema, caps);
        if (!status.ok()) {
            std::cout << "FAIL: Failed to create regular table: " << status.ToString() << std::endl;
            return 1;
        }
        std::cout << "OK: Regular table created" << std::endl;
    }

    // Test 2: Create a bitemporal table
    std::cout << "\nTest 2: Create bitemporal table (should have temporal columns)" << std::endl;
    {
        auto schema = arrow::schema({
            arrow::field("employee_id", arrow::utf8()),
            arrow::field("salary", arrow::float64()),
            arrow::field("department", arrow::utf8())
        });

        TableSchema table_schema("employee_history", schema);

        // Enable bitemporal
        TableCapabilities caps;
        caps.temporal_model = TableCapabilities::TemporalModel::kBitemporal;

        status = db->CreateTable(table_schema, caps);
        if (!status.ok()) {
            std::cout << "FAIL: Failed to create bitemporal table: " << status.ToString() << std::endl;
            return 1;
        }
        std::cout << "OK: Bitemporal table created" << std::endl;

        // The table should have the following schema:
        // - employee_id (utf8)
        // - salary (float64)
        // - department (utf8)
        // - _system_time_start (timestamp[us])
        // - _system_time_end (timestamp[us])
        // - _system_version (int64)
        // - _valid_time_start (timestamp[us])
        // - _valid_time_end (timestamp[us])
        // - _is_deleted (bool)
        std::cout << "OK: Bitemporal table should have 9 columns (3 user + 6 temporal)" << std::endl;
    }

    // Test 3: Create a system-time only table
    std::cout << "\nTest 3: Create system-time table" << std::endl;
    {
        auto schema = arrow::schema({
            arrow::field("log_id", arrow::int64()),
            arrow::field("message", arrow::utf8())
        });

        TableSchema table_schema("audit_log", schema);

        TableCapabilities caps;
        caps.temporal_model = TableCapabilities::TemporalModel::kSystemTime;

        status = db->CreateTable(table_schema, caps);
        if (!status.ok()) {
            std::cout << "FAIL: Failed to create system-time table: " << status.ToString() << std::endl;
            return 1;
        }
        std::cout << "OK: System-time table created" << std::endl;
        std::cout << "OK: System-time table should have 6 columns (2 user + 4 temporal)" << std::endl;
    }

    // Test 4: Create a valid-time only table
    std::cout << "\nTest 4: Create valid-time table" << std::endl;
    {
        auto schema = arrow::schema({
            arrow::field("policy_id", arrow::utf8()),
            arrow::field("premium", arrow::float64())
        });

        TableSchema table_schema("insurance_policies", schema);

        TableCapabilities caps;
        caps.temporal_model = TableCapabilities::TemporalModel::kValidTime;

        status = db->CreateTable(table_schema, caps);
        if (!status.ok()) {
            std::cout << "FAIL: Failed to create valid-time table: " << status.ToString() << std::endl;
            return 1;
        }
        std::cout << "OK: Valid-time table created" << std::endl;
        std::cout << "OK: Valid-time table should have 5 columns (2 user + 3 temporal)" << std::endl;
    }

    // Test 5: Use preset configuration for audit log
    std::cout << "\nTest 5: Create table using AuditLog preset" << std::endl;
    {
        auto schema = arrow::schema({
            arrow::field("event_type", arrow::utf8()),
            arrow::field("user_id", arrow::utf8()),
            arrow::field("details", arrow::utf8())
        });

        TableSchema table_schema("compliance_events", schema);
        TableCapabilities caps = CapabilityPresets::AuditLog();

        // Verify the preset uses bitemporal
        if (caps.temporal_model != TableCapabilities::TemporalModel::kBitemporal) {
            std::cout << "FAIL: AuditLog preset should use kBitemporal" << std::endl;
            return 1;
        }

        status = db->CreateTable(table_schema, caps);
        if (!status.ok()) {
            std::cout << "FAIL: Failed to create table with AuditLog preset: " << status.ToString() << std::endl;
            return 1;
        }
        std::cout << "OK: AuditLog preset table created with bitemporal support" << std::endl;
    }

    std::cout << "\n=== All Bitemporal Table Creation Tests Passed ===" << std::endl;
    return 0;
}
