#pragma once

#ifdef __cplusplus
extern "C" {
#endif

// Opaque handle types
typedef void* SabotSQLBridgeHandle;
typedef void* ArrowTableHandle;

// Create/destroy bridge
SabotSQLBridgeHandle sabot_sql_bridge_create();
void sabot_sql_bridge_destroy(SabotSQLBridgeHandle handle);

// Register table (takes ownership of Arrow C Data Interface)
int sabot_sql_bridge_register_table(
    SabotSQLBridgeHandle handle,
    const char* table_name,
    ArrowTableHandle table_handle
);

// Execute SQL and return Arrow table handle
// Caller must free with arrow_release_table
ArrowTableHandle sabot_sql_bridge_execute_sql(
    SabotSQLBridgeHandle handle,
    const char* sql,
    char** error_message  // OUT: error message (caller must free)
);

// Helper to get table info
int sabot_sql_bridge_get_table_rows(ArrowTableHandle table);
int sabot_sql_bridge_get_table_cols(ArrowTableHandle table);

#ifdef __cplusplus
}
#endif


