#include "sabot_sql/sql/c_api.h"
#include "sabot_sql/sql/simple_sabot_sql_bridge.h"
#include <arrow/c/bridge.h>
#include <cstring>
#include <memory>

using namespace sabot_sql::sql;

extern "C" {

SabotSQLBridgeHandle sabot_sql_bridge_create() {
    auto result = SabotSQLBridge::Create();
    if (!result.ok()) {
        return nullptr;
    }
    
    // Return raw pointer (caller will manage via handle)
    return new std::shared_ptr<SabotSQLBridge>(result.ValueOrDie());
}

void sabot_sql_bridge_destroy(SabotSQLBridgeHandle handle) {
    if (handle) {
        delete static_cast<std::shared_ptr<SabotSQLBridge>*>(handle);
    }
}

int sabot_sql_bridge_register_table(
    SabotSQLBridgeHandle handle,
    const char* table_name,
    ArrowTableHandle table_handle) {
    
    if (!handle || !table_name || !table_handle) {
        return -1;
    }
    
    auto bridge = static_cast<std::shared_ptr<SabotSQLBridge>*>(handle);
    auto table = static_cast<std::shared_ptr<arrow::Table>*>(table_handle);
    
    auto status = (*bridge)->RegisterTable(table_name, *table);
    return status.ok() ? 0 : -1;
}

ArrowTableHandle sabot_sql_bridge_execute_sql(
    SabotSQLBridgeHandle handle,
    const char* sql,
    char** error_message) {
    
    if (!handle || !sql) {
        if (error_message) {
            *error_message = strdup("Invalid handle or SQL");
        }
        return nullptr;
    }
    
    auto bridge = static_cast<std::shared_ptr<SabotSQLBridge>*>(handle);
    
    auto result = (*bridge)->ExecuteSQL(sql);
    
    if (!result.ok()) {
        if (error_message) {
            std::string err = result.status().ToString();
            *error_message = strdup(err.c_str());
        }
        return nullptr;
    }
    
    // Return new shared_ptr to table
    return new std::shared_ptr<arrow::Table>(result.ValueOrDie());
}

int sabot_sql_bridge_get_table_rows(ArrowTableHandle table) {
    if (!table) return 0;
    auto t = static_cast<std::shared_ptr<arrow::Table>*>(table);
    return (*t)->num_rows();
}

int sabot_sql_bridge_get_table_cols(ArrowTableHandle table) {
    if (!table) return 0;
    auto t = static_cast<std::shared_ptr<arrow::Table>*>(table);
    return (*t)->num_columns();
}

} // extern "C"


