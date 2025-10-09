/**
 * MarbleDB C API Example
 *
 * Demonstrates how to use the C API from C programs.
 * This example shows basic database operations using the
 * extern "C" interface.
 */

#include <marble/c_api.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdbool.h>

#define CHECK_STATUS(status, operation) \
    do { \
        if (!marble_status_ok(&status)) { \
            fprintf(stderr, "Error in %s: %s\n", operation, status.message); \
            return EXIT_FAILURE; \
        } \
    } while (0)

int main(int argc, char* argv[]) {
    (void)argc; // Suppress unused parameter warning
    (void)argv;

    printf("========================================\n");
    printf("🗂️  MarbleDB C API Example\n");
    printf("========================================\n\n");

    // Initialize the library
    MarbleDB_Status status = marble_init();
    CHECK_STATUS(status, "library initialization");

    printf("✅ Library initialized\n");

    // Open database
    MarbleDB_Handle* db_handle = NULL;
    status = marble_db_open("/tmp/marble_c_example", &db_handle);
    CHECK_STATUS(status, "database open");

    printf("✅ Database opened\n");

    // Create a table
    const char* schema_json =
        "{"
        "  \"fields\": ["
        "    {\"name\": \"id\", \"type\": \"int64\"},"
        "    {\"name\": \"name\", \"type\": \"string\"},"
        "    {\"name\": \"age\", \"type\": \"int64\"},"
        "    {\"name\": \"salary\", \"type\": \"float64\"}"
        "  ]"
        "}";

    status = marble_db_create_table(db_handle, "employees", schema_json);
    CHECK_STATUS(status, "table creation");

    printf("✅ Table 'employees' created\n");

    // Insert some records
    const char* records[] = {
        "{\"id\": 1, \"name\": \"Alice\", \"age\": 30, \"salary\": 75000.0}",
        "{\"id\": 2, \"name\": \"Bob\", \"age\": 25, \"salary\": 65000.0}",
        "{\"id\": 3, \"name\": \"Charlie\", \"age\": 35, \"salary\": 85000.0}",
        "{\"id\": 4, \"name\": \"Diana\", \"age\": 28, \"salary\": 70000.0}",
        "{\"id\": 5, \"name\": \"Eve\", \"age\": 32, \"salary\": 80000.0}"
    };

    for (size_t i = 0; i < sizeof(records) / sizeof(records[0]); ++i) {
        status = marble_db_insert_record(db_handle, "employees", records[i]);
        CHECK_STATUS(status, "record insertion");
    }

    printf("✅ Inserted %zu records\n", sizeof(records) / sizeof(records[0]));

    // Execute a query
    MarbleDB_QueryResult_Handle* result_handle = NULL;
    status = marble_db_execute_query(db_handle, "SELECT * FROM employees WHERE age > 30", &result_handle);
    CHECK_STATUS(status, "query execution");

    printf("✅ Query executed: SELECT * FROM employees WHERE age > 30\n");

    // Process results
    bool has_next = false;
    status = marble_db_query_result_has_next(result_handle, &has_next);
    CHECK_STATUS(status, "checking result availability");

    int64_t total_rows = 0;
    while (has_next) {
        char* batch_json = NULL;
        status = marble_db_query_result_next_json(result_handle, &batch_json);
        CHECK_STATUS(status, "getting next batch");

        printf("📋 Result batch:\n%s\n", batch_json);

        // Count rows in this batch (simple JSON parsing)
        const char* ptr = batch_json;
        int row_count = 0;
        while (*ptr) {
            if (*ptr == '{') row_count++;
            ptr++;
        }
        total_rows += row_count;

        marble_free_string(batch_json);

        // Check if there are more results
        status = marble_db_query_result_has_next(result_handle, &has_next);
        CHECK_STATUS(status, "checking for more results");
    }

    printf("📊 Query returned %lld rows\n", (long long)total_rows);

    // Get result schema
    char* schema_result = NULL;
    status = marble_db_query_result_get_schema(result_handle, &schema_result);
    CHECK_STATUS(status, "getting result schema");

    printf("📋 Result schema: %s\n", schema_result);
    marble_free_string(schema_result);

    // Clean up query result
    marble_db_close_query_result(&result_handle);

    // Get database statistics
    char* stats_json = NULL;
    status = marble_db_get_stats(db_handle, &stats_json);
    CHECK_STATUS(status, "getting database stats");

    printf("📊 Database stats: %s\n", stats_json);
    marble_free_string(stats_json);

    // List tables
    const char** table_names = NULL;
    size_t table_count = 0;
    status = marble_db_list_tables(db_handle, &table_names, &table_count);
    CHECK_STATUS(status, "listing tables");

    printf("📋 Tables (%zu):\n", table_count);
    for (size_t i = 0; i < table_count; ++i) {
        printf("  - %s\n", table_names[i]);
    }

    marble_free_string_array(table_names, table_count);

    // Get version information
    char* version = NULL;
    status = marble_db_get_version(&version);
    CHECK_STATUS(status, "getting version");

    printf("📋 MarbleDB version: %s\n", version);
    marble_free_string(version);

    // Close database
    marble_db_close(&db_handle);
    printf("✅ Database closed\n");

    // Cleanup library
    marble_cleanup();
    printf("✅ Library cleanup completed\n");

    printf("\n🎉 C API example completed successfully!\n");
    printf("========================================\n");

    return EXIT_SUCCESS;
}
