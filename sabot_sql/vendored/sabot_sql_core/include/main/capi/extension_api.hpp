#pragma once

#include "sabot_sql.h"

//===--------------------------------------------------------------------===//
// Function pointer struct
//===--------------------------------------------------------------------===//
typedef struct {
	// v1.2.0
	sabot_sql_state (*sabot_sql_open)(const char *path, sabot_sql_database *out_database);
	sabot_sql_state (*sabot_sql_open_ext)(const char *path, sabot_sql_database *out_database, sabot_sql_config config,
	                                char **out_error);
	void (*sabot_sql_close)(sabot_sql_database *database);
	sabot_sql_state (*sabot_sql_connect)(sabot_sql_database database, sabot_sql_connection *out_connection);
	void (*sabot_sql_interrupt)(sabot_sql_connection connection);
	sabot_sql_query_progress_type (*sabot_sql_query_progress)(sabot_sql_connection connection);
	void (*sabot_sql_disconnect)(sabot_sql_connection *connection);
	const char *(*sabot_sql_library_version)();
	sabot_sql_state (*sabot_sql_create_config)(sabot_sql_config *out_config);
	size_t (*sabot_sql_config_count)();
	sabot_sql_state (*sabot_sql_get_config_flag)(size_t index, const char **out_name, const char **out_description);
	sabot_sql_state (*sabot_sql_set_config)(sabot_sql_config config, const char *name, const char *option);
	void (*sabot_sql_destroy_config)(sabot_sql_config *config);
	sabot_sql_state (*sabot_sql_query)(sabot_sql_connection connection, const char *query, sabot_sql_result *out_result);
	void (*sabot_sql_destroy_result)(sabot_sql_result *result);
	const char *(*sabot_sql_column_name)(sabot_sql_result *result, idx_t col);
	sabot_sql_type (*sabot_sql_column_type)(sabot_sql_result *result, idx_t col);
	sabot_sql_statement_type (*sabot_sql_result_statement_type)(sabot_sql_result result);
	sabot_sql_logical_type (*sabot_sql_column_logical_type)(sabot_sql_result *result, idx_t col);
	idx_t (*sabot_sql_column_count)(sabot_sql_result *result);
	idx_t (*sabot_sql_rows_changed)(sabot_sql_result *result);
	const char *(*sabot_sql_result_error)(sabot_sql_result *result);
	sabot_sql_error_type (*sabot_sql_result_error_type)(sabot_sql_result *result);
	sabot_sql_result_type (*sabot_sql_result_return_type)(sabot_sql_result result);
	void *(*sabot_sql_malloc)(size_t size);
	void (*sabot_sql_free)(void *ptr);
	idx_t (*sabot_sql_vector_size)();
	bool (*sabot_sql_string_is_inlined)(sabot_sql_string_t string);
	uint32_t (*sabot_sql_string_t_length)(sabot_sql_string_t string);
	const char *(*sabot_sql_string_t_data)(sabot_sql_string_t *string);
	sabot_sql_date_struct (*sabot_sql_from_date)(sabot_sql_date date);
	sabot_sql_date (*sabot_sql_to_date)(sabot_sql_date_struct date);
	bool (*sabot_sql_is_finite_date)(sabot_sql_date date);
	sabot_sql_time_struct (*sabot_sql_from_time)(sabot_sql_time time);
	sabot_sql_time_tz (*sabot_sql_create_time_tz)(int64_t micros, int32_t offset);
	sabot_sql_time_tz_struct (*sabot_sql_from_time_tz)(sabot_sql_time_tz micros);
	sabot_sql_time (*sabot_sql_to_time)(sabot_sql_time_struct time);
	sabot_sql_timestamp_struct (*sabot_sql_from_timestamp)(sabot_sql_timestamp ts);
	sabot_sql_timestamp (*sabot_sql_to_timestamp)(sabot_sql_timestamp_struct ts);
	bool (*sabot_sql_is_finite_timestamp)(sabot_sql_timestamp ts);
	double (*sabot_sql_hugeint_to_double)(sabot_sql_hugeint val);
	sabot_sql_hugeint (*sabot_sql_double_to_hugeint)(double val);
	double (*sabot_sql_uhugeint_to_double)(sabot_sql_uhugeint val);
	sabot_sql_uhugeint (*sabot_sql_double_to_uhugeint)(double val);
	sabot_sql_decimal (*sabot_sql_double_to_decimal)(double val, uint8_t width, uint8_t scale);
	double (*sabot_sql_decimal_to_double)(sabot_sql_decimal val);
	sabot_sql_state (*sabot_sql_prepare)(sabot_sql_connection connection, const char *query,
	                               sabot_sql_prepared_statement *out_prepared_statement);
	void (*sabot_sql_destroy_prepare)(sabot_sql_prepared_statement *prepared_statement);
	const char *(*sabot_sql_prepare_error)(sabot_sql_prepared_statement prepared_statement);
	idx_t (*sabot_sql_nparams)(sabot_sql_prepared_statement prepared_statement);
	const char *(*sabot_sql_parameter_name)(sabot_sql_prepared_statement prepared_statement, idx_t index);
	sabot_sql_type (*sabot_sql_param_type)(sabot_sql_prepared_statement prepared_statement, idx_t param_idx);
	sabot_sql_logical_type (*sabot_sql_param_logical_type)(sabot_sql_prepared_statement prepared_statement, idx_t param_idx);
	sabot_sql_state (*sabot_sql_clear_bindings)(sabot_sql_prepared_statement prepared_statement);
	sabot_sql_statement_type (*sabot_sql_prepared_statement_type)(sabot_sql_prepared_statement statement);
	sabot_sql_state (*sabot_sql_bind_value)(sabot_sql_prepared_statement prepared_statement, idx_t param_idx, sabot_sql_value val);
	sabot_sql_state (*sabot_sql_bind_parameter_index)(sabot_sql_prepared_statement prepared_statement, idx_t *param_idx_out,
	                                            const char *name);
	sabot_sql_state (*sabot_sql_bind_boolean)(sabot_sql_prepared_statement prepared_statement, idx_t param_idx, bool val);
	sabot_sql_state (*sabot_sql_bind_int8)(sabot_sql_prepared_statement prepared_statement, idx_t param_idx, int8_t val);
	sabot_sql_state (*sabot_sql_bind_int16)(sabot_sql_prepared_statement prepared_statement, idx_t param_idx, int16_t val);
	sabot_sql_state (*sabot_sql_bind_int32)(sabot_sql_prepared_statement prepared_statement, idx_t param_idx, int32_t val);
	sabot_sql_state (*sabot_sql_bind_int64)(sabot_sql_prepared_statement prepared_statement, idx_t param_idx, int64_t val);
	sabot_sql_state (*sabot_sql_bind_hugeint)(sabot_sql_prepared_statement prepared_statement, idx_t param_idx,
	                                    sabot_sql_hugeint val);
	sabot_sql_state (*sabot_sql_bind_uhugeint)(sabot_sql_prepared_statement prepared_statement, idx_t param_idx,
	                                     sabot_sql_uhugeint val);
	sabot_sql_state (*sabot_sql_bind_decimal)(sabot_sql_prepared_statement prepared_statement, idx_t param_idx,
	                                    sabot_sql_decimal val);
	sabot_sql_state (*sabot_sql_bind_uint8)(sabot_sql_prepared_statement prepared_statement, idx_t param_idx, uint8_t val);
	sabot_sql_state (*sabot_sql_bind_uint16)(sabot_sql_prepared_statement prepared_statement, idx_t param_idx, uint16_t val);
	sabot_sql_state (*sabot_sql_bind_uint32)(sabot_sql_prepared_statement prepared_statement, idx_t param_idx, uint32_t val);
	sabot_sql_state (*sabot_sql_bind_uint64)(sabot_sql_prepared_statement prepared_statement, idx_t param_idx, uint64_t val);
	sabot_sql_state (*sabot_sql_bind_float)(sabot_sql_prepared_statement prepared_statement, idx_t param_idx, float val);
	sabot_sql_state (*sabot_sql_bind_double)(sabot_sql_prepared_statement prepared_statement, idx_t param_idx, double val);
	sabot_sql_state (*sabot_sql_bind_date)(sabot_sql_prepared_statement prepared_statement, idx_t param_idx, sabot_sql_date val);
	sabot_sql_state (*sabot_sql_bind_time)(sabot_sql_prepared_statement prepared_statement, idx_t param_idx, sabot_sql_time val);
	sabot_sql_state (*sabot_sql_bind_timestamp)(sabot_sql_prepared_statement prepared_statement, idx_t param_idx,
	                                      sabot_sql_timestamp val);
	sabot_sql_state (*sabot_sql_bind_timestamp_tz)(sabot_sql_prepared_statement prepared_statement, idx_t param_idx,
	                                         sabot_sql_timestamp val);
	sabot_sql_state (*sabot_sql_bind_interval)(sabot_sql_prepared_statement prepared_statement, idx_t param_idx,
	                                     sabot_sql_interval val);
	sabot_sql_state (*sabot_sql_bind_varchar)(sabot_sql_prepared_statement prepared_statement, idx_t param_idx, const char *val);
	sabot_sql_state (*sabot_sql_bind_varchar_length)(sabot_sql_prepared_statement prepared_statement, idx_t param_idx,
	                                           const char *val, idx_t length);
	sabot_sql_state (*sabot_sql_bind_blob)(sabot_sql_prepared_statement prepared_statement, idx_t param_idx, const void *data,
	                                 idx_t length);
	sabot_sql_state (*sabot_sql_bind_null)(sabot_sql_prepared_statement prepared_statement, idx_t param_idx);
	sabot_sql_state (*sabot_sql_execute_prepared)(sabot_sql_prepared_statement prepared_statement, sabot_sql_result *out_result);
	idx_t (*sabot_sql_extract_statements)(sabot_sql_connection connection, const char *query,
	                                   sabot_sql_extracted_statements *out_extracted_statements);
	sabot_sql_state (*sabot_sql_prepare_extracted_statement)(sabot_sql_connection connection,
	                                                   sabot_sql_extracted_statements extracted_statements, idx_t index,
	                                                   sabot_sql_prepared_statement *out_prepared_statement);
	const char *(*sabot_sql_extract_statements_error)(sabot_sql_extracted_statements extracted_statements);
	void (*sabot_sql_destroy_extracted)(sabot_sql_extracted_statements *extracted_statements);
	sabot_sql_state (*sabot_sql_pending_prepared)(sabot_sql_prepared_statement prepared_statement,
	                                        sabot_sql_pending_result *out_result);
	void (*sabot_sql_destroy_pending)(sabot_sql_pending_result *pending_result);
	const char *(*sabot_sql_pending_error)(sabot_sql_pending_result pending_result);
	sabot_sql_pending_state (*sabot_sql_pending_execute_task)(sabot_sql_pending_result pending_result);
	sabot_sql_pending_state (*sabot_sql_pending_execute_check_state)(sabot_sql_pending_result pending_result);
	sabot_sql_state (*sabot_sql_execute_pending)(sabot_sql_pending_result pending_result, sabot_sql_result *out_result);
	bool (*sabot_sql_pending_execution_is_finished)(sabot_sql_pending_state pending_state);
	void (*sabot_sql_destroy_value)(sabot_sql_value *value);
	sabot_sql_value (*sabot_sql_create_varchar)(const char *text);
	sabot_sql_value (*sabot_sql_create_varchar_length)(const char *text, idx_t length);
	sabot_sql_value (*sabot_sql_create_bool)(bool input);
	sabot_sql_value (*sabot_sql_create_int8)(int8_t input);
	sabot_sql_value (*sabot_sql_create_uint8)(uint8_t input);
	sabot_sql_value (*sabot_sql_create_int16)(int16_t input);
	sabot_sql_value (*sabot_sql_create_uint16)(uint16_t input);
	sabot_sql_value (*sabot_sql_create_int32)(int32_t input);
	sabot_sql_value (*sabot_sql_create_uint32)(uint32_t input);
	sabot_sql_value (*sabot_sql_create_uint64)(uint64_t input);
	sabot_sql_value (*sabot_sql_create_int64)(int64_t val);
	sabot_sql_value (*sabot_sql_create_hugeint)(sabot_sql_hugeint input);
	sabot_sql_value (*sabot_sql_create_uhugeint)(sabot_sql_uhugeint input);
	sabot_sql_value (*sabot_sql_create_float)(float input);
	sabot_sql_value (*sabot_sql_create_double)(double input);
	sabot_sql_value (*sabot_sql_create_date)(sabot_sql_date input);
	sabot_sql_value (*sabot_sql_create_time)(sabot_sql_time input);
	sabot_sql_value (*sabot_sql_create_time_tz_value)(sabot_sql_time_tz value);
	sabot_sql_value (*sabot_sql_create_timestamp)(sabot_sql_timestamp input);
	sabot_sql_value (*sabot_sql_create_interval)(sabot_sql_interval input);
	sabot_sql_value (*sabot_sql_create_blob)(const uint8_t *data, idx_t length);
	sabot_sql_value (*sabot_sql_create_bignum)(sabot_sql_bignum input);
	sabot_sql_value (*sabot_sql_create_decimal)(sabot_sql_decimal input);
	sabot_sql_value (*sabot_sql_create_bit)(sabot_sql_bit input);
	sabot_sql_value (*sabot_sql_create_uuid)(sabot_sql_uhugeint input);
	bool (*sabot_sql_get_bool)(sabot_sql_value val);
	int8_t (*sabot_sql_get_int8)(sabot_sql_value val);
	uint8_t (*sabot_sql_get_uint8)(sabot_sql_value val);
	int16_t (*sabot_sql_get_int16)(sabot_sql_value val);
	uint16_t (*sabot_sql_get_uint16)(sabot_sql_value val);
	int32_t (*sabot_sql_get_int32)(sabot_sql_value val);
	uint32_t (*sabot_sql_get_uint32)(sabot_sql_value val);
	int64_t (*sabot_sql_get_int64)(sabot_sql_value val);
	uint64_t (*sabot_sql_get_uint64)(sabot_sql_value val);
	sabot_sql_hugeint (*sabot_sql_get_hugeint)(sabot_sql_value val);
	sabot_sql_uhugeint (*sabot_sql_get_uhugeint)(sabot_sql_value val);
	float (*sabot_sql_get_float)(sabot_sql_value val);
	double (*sabot_sql_get_double)(sabot_sql_value val);
	sabot_sql_date (*sabot_sql_get_date)(sabot_sql_value val);
	sabot_sql_time (*sabot_sql_get_time)(sabot_sql_value val);
	sabot_sql_time_tz (*sabot_sql_get_time_tz)(sabot_sql_value val);
	sabot_sql_timestamp (*sabot_sql_get_timestamp)(sabot_sql_value val);
	sabot_sql_interval (*sabot_sql_get_interval)(sabot_sql_value val);
	sabot_sql_logical_type (*sabot_sql_get_value_type)(sabot_sql_value val);
	sabot_sql_blob (*sabot_sql_get_blob)(sabot_sql_value val);
	sabot_sql_bignum (*sabot_sql_get_bignum)(sabot_sql_value val);
	sabot_sql_decimal (*sabot_sql_get_decimal)(sabot_sql_value val);
	sabot_sql_bit (*sabot_sql_get_bit)(sabot_sql_value val);
	sabot_sql_uhugeint (*sabot_sql_get_uuid)(sabot_sql_value val);
	char *(*sabot_sql_get_varchar)(sabot_sql_value value);
	sabot_sql_value (*sabot_sql_create_struct_value)(sabot_sql_logical_type type, sabot_sql_value *values);
	sabot_sql_value (*sabot_sql_create_list_value)(sabot_sql_logical_type type, sabot_sql_value *values, idx_t value_count);
	sabot_sql_value (*sabot_sql_create_array_value)(sabot_sql_logical_type type, sabot_sql_value *values, idx_t value_count);
	idx_t (*sabot_sql_get_map_size)(sabot_sql_value value);
	sabot_sql_value (*sabot_sql_get_map_key)(sabot_sql_value value, idx_t index);
	sabot_sql_value (*sabot_sql_get_map_value)(sabot_sql_value value, idx_t index);
	bool (*sabot_sql_is_null_value)(sabot_sql_value value);
	sabot_sql_value (*sabot_sql_create_null_value)();
	idx_t (*sabot_sql_get_list_size)(sabot_sql_value value);
	sabot_sql_value (*sabot_sql_get_list_child)(sabot_sql_value value, idx_t index);
	sabot_sql_value (*sabot_sql_create_enum_value)(sabot_sql_logical_type type, uint64_t value);
	uint64_t (*sabot_sql_get_enum_value)(sabot_sql_value value);
	sabot_sql_value (*sabot_sql_get_struct_child)(sabot_sql_value value, idx_t index);
	sabot_sql_logical_type (*sabot_sql_create_logical_type)(sabot_sql_type type);
	char *(*sabot_sql_logical_type_get_alias)(sabot_sql_logical_type type);
	void (*sabot_sql_logical_type_set_alias)(sabot_sql_logical_type type, const char *alias);
	sabot_sql_logical_type (*sabot_sql_create_list_type)(sabot_sql_logical_type type);
	sabot_sql_logical_type (*sabot_sql_create_array_type)(sabot_sql_logical_type type, idx_t array_size);
	sabot_sql_logical_type (*sabot_sql_create_map_type)(sabot_sql_logical_type key_type, sabot_sql_logical_type value_type);
	sabot_sql_logical_type (*sabot_sql_create_union_type)(sabot_sql_logical_type *member_types, const char **member_names,
	                                                idx_t member_count);
	sabot_sql_logical_type (*sabot_sql_create_struct_type)(sabot_sql_logical_type *member_types, const char **member_names,
	                                                 idx_t member_count);
	sabot_sql_logical_type (*sabot_sql_create_enum_type)(const char **member_names, idx_t member_count);
	sabot_sql_logical_type (*sabot_sql_create_decimal_type)(uint8_t width, uint8_t scale);
	sabot_sql_type (*sabot_sql_get_type_id)(sabot_sql_logical_type type);
	uint8_t (*sabot_sql_decimal_width)(sabot_sql_logical_type type);
	uint8_t (*sabot_sql_decimal_scale)(sabot_sql_logical_type type);
	sabot_sql_type (*sabot_sql_decimal_internal_type)(sabot_sql_logical_type type);
	sabot_sql_type (*sabot_sql_enum_internal_type)(sabot_sql_logical_type type);
	uint32_t (*sabot_sql_enum_dictionary_size)(sabot_sql_logical_type type);
	char *(*sabot_sql_enum_dictionary_value)(sabot_sql_logical_type type, idx_t index);
	sabot_sql_logical_type (*sabot_sql_list_type_child_type)(sabot_sql_logical_type type);
	sabot_sql_logical_type (*sabot_sql_array_type_child_type)(sabot_sql_logical_type type);
	idx_t (*sabot_sql_array_type_array_size)(sabot_sql_logical_type type);
	sabot_sql_logical_type (*sabot_sql_map_type_key_type)(sabot_sql_logical_type type);
	sabot_sql_logical_type (*sabot_sql_map_type_value_type)(sabot_sql_logical_type type);
	idx_t (*sabot_sql_struct_type_child_count)(sabot_sql_logical_type type);
	char *(*sabot_sql_struct_type_child_name)(sabot_sql_logical_type type, idx_t index);
	sabot_sql_logical_type (*sabot_sql_struct_type_child_type)(sabot_sql_logical_type type, idx_t index);
	idx_t (*sabot_sql_union_type_member_count)(sabot_sql_logical_type type);
	char *(*sabot_sql_union_type_member_name)(sabot_sql_logical_type type, idx_t index);
	sabot_sql_logical_type (*sabot_sql_union_type_member_type)(sabot_sql_logical_type type, idx_t index);
	void (*sabot_sql_destroy_logical_type)(sabot_sql_logical_type *type);
	sabot_sql_state (*sabot_sql_register_logical_type)(sabot_sql_connection con, sabot_sql_logical_type type,
	                                             sabot_sql_create_type_info info);
	sabot_sql_data_chunk (*sabot_sql_create_data_chunk)(sabot_sql_logical_type *types, idx_t column_count);
	void (*sabot_sql_destroy_data_chunk)(sabot_sql_data_chunk *chunk);
	void (*sabot_sql_data_chunk_reset)(sabot_sql_data_chunk chunk);
	idx_t (*sabot_sql_data_chunk_get_column_count)(sabot_sql_data_chunk chunk);
	sabot_sql_vector (*sabot_sql_data_chunk_get_vector)(sabot_sql_data_chunk chunk, idx_t col_idx);
	idx_t (*sabot_sql_data_chunk_get_size)(sabot_sql_data_chunk chunk);
	void (*sabot_sql_data_chunk_set_size)(sabot_sql_data_chunk chunk, idx_t size);
	sabot_sql_logical_type (*sabot_sql_vector_get_column_type)(sabot_sql_vector vector);
	void *(*sabot_sql_vector_get_data)(sabot_sql_vector vector);
	uint64_t *(*sabot_sql_vector_get_validity)(sabot_sql_vector vector);
	void (*sabot_sql_vector_ensure_validity_writable)(sabot_sql_vector vector);
	void (*sabot_sql_vector_assign_string_element)(sabot_sql_vector vector, idx_t index, const char *str);
	void (*sabot_sql_vector_assign_string_element_len)(sabot_sql_vector vector, idx_t index, const char *str, idx_t str_len);
	sabot_sql_vector (*sabot_sql_list_vector_get_child)(sabot_sql_vector vector);
	idx_t (*sabot_sql_list_vector_get_size)(sabot_sql_vector vector);
	sabot_sql_state (*sabot_sql_list_vector_set_size)(sabot_sql_vector vector, idx_t size);
	sabot_sql_state (*sabot_sql_list_vector_reserve)(sabot_sql_vector vector, idx_t required_capacity);
	sabot_sql_vector (*sabot_sql_struct_vector_get_child)(sabot_sql_vector vector, idx_t index);
	sabot_sql_vector (*sabot_sql_array_vector_get_child)(sabot_sql_vector vector);
	bool (*sabot_sql_validity_row_is_valid)(uint64_t *validity, idx_t row);
	void (*sabot_sql_validity_set_row_validity)(uint64_t *validity, idx_t row, bool valid);
	void (*sabot_sql_validity_set_row_invalid)(uint64_t *validity, idx_t row);
	void (*sabot_sql_validity_set_row_valid)(uint64_t *validity, idx_t row);
	sabot_sql_scalar_function (*sabot_sql_create_scalar_function)();
	void (*sabot_sql_destroy_scalar_function)(sabot_sql_scalar_function *scalar_function);
	void (*sabot_sql_scalar_function_set_name)(sabot_sql_scalar_function scalar_function, const char *name);
	void (*sabot_sql_scalar_function_set_varargs)(sabot_sql_scalar_function scalar_function, sabot_sql_logical_type type);
	void (*sabot_sql_scalar_function_set_special_handling)(sabot_sql_scalar_function scalar_function);
	void (*sabot_sql_scalar_function_set_volatile)(sabot_sql_scalar_function scalar_function);
	void (*sabot_sql_scalar_function_add_parameter)(sabot_sql_scalar_function scalar_function, sabot_sql_logical_type type);
	void (*sabot_sql_scalar_function_set_return_type)(sabot_sql_scalar_function scalar_function, sabot_sql_logical_type type);
	void (*sabot_sql_scalar_function_set_extra_info)(sabot_sql_scalar_function scalar_function, void *extra_info,
	                                              sabot_sql_delete_callback_t destroy);
	void (*sabot_sql_scalar_function_set_function)(sabot_sql_scalar_function scalar_function,
	                                            sabot_sql_scalar_function_t function);
	sabot_sql_state (*sabot_sql_register_scalar_function)(sabot_sql_connection con, sabot_sql_scalar_function scalar_function);
	void *(*sabot_sql_scalar_function_get_extra_info)(sabot_sql_function_info info);
	void (*sabot_sql_scalar_function_set_error)(sabot_sql_function_info info, const char *error);
	sabot_sql_scalar_function_set (*sabot_sql_create_scalar_function_set)(const char *name);
	void (*sabot_sql_destroy_scalar_function_set)(sabot_sql_scalar_function_set *scalar_function_set);
	sabot_sql_state (*sabot_sql_add_scalar_function_to_set)(sabot_sql_scalar_function_set set, sabot_sql_scalar_function function);
	sabot_sql_state (*sabot_sql_register_scalar_function_set)(sabot_sql_connection con, sabot_sql_scalar_function_set set);
	sabot_sql_aggregate_function (*sabot_sql_create_aggregate_function)();
	void (*sabot_sql_destroy_aggregate_function)(sabot_sql_aggregate_function *aggregate_function);
	void (*sabot_sql_aggregate_function_set_name)(sabot_sql_aggregate_function aggregate_function, const char *name);
	void (*sabot_sql_aggregate_function_add_parameter)(sabot_sql_aggregate_function aggregate_function,
	                                                sabot_sql_logical_type type);
	void (*sabot_sql_aggregate_function_set_return_type)(sabot_sql_aggregate_function aggregate_function,
	                                                  sabot_sql_logical_type type);
	void (*sabot_sql_aggregate_function_set_functions)(sabot_sql_aggregate_function aggregate_function,
	                                                sabot_sql_aggregate_state_size state_size,
	                                                sabot_sql_aggregate_init_t state_init,
	                                                sabot_sql_aggregate_update_t update,
	                                                sabot_sql_aggregate_combine_t combine,
	                                                sabot_sql_aggregate_finalize_t finalize);
	void (*sabot_sql_aggregate_function_set_destructor)(sabot_sql_aggregate_function aggregate_function,
	                                                 sabot_sql_aggregate_destroy_t destroy);
	sabot_sql_state (*sabot_sql_register_aggregate_function)(sabot_sql_connection con,
	                                                   sabot_sql_aggregate_function aggregate_function);
	void (*sabot_sql_aggregate_function_set_special_handling)(sabot_sql_aggregate_function aggregate_function);
	void (*sabot_sql_aggregate_function_set_extra_info)(sabot_sql_aggregate_function aggregate_function, void *extra_info,
	                                                 sabot_sql_delete_callback_t destroy);
	void *(*sabot_sql_aggregate_function_get_extra_info)(sabot_sql_function_info info);
	void (*sabot_sql_aggregate_function_set_error)(sabot_sql_function_info info, const char *error);
	sabot_sql_aggregate_function_set (*sabot_sql_create_aggregate_function_set)(const char *name);
	void (*sabot_sql_destroy_aggregate_function_set)(sabot_sql_aggregate_function_set *aggregate_function_set);
	sabot_sql_state (*sabot_sql_add_aggregate_function_to_set)(sabot_sql_aggregate_function_set set,
	                                                     sabot_sql_aggregate_function function);
	sabot_sql_state (*sabot_sql_register_aggregate_function_set)(sabot_sql_connection con, sabot_sql_aggregate_function_set set);
	sabot_sql_table_function (*sabot_sql_create_table_function)();
	void (*sabot_sql_destroy_table_function)(sabot_sql_table_function *table_function);
	void (*sabot_sql_table_function_set_name)(sabot_sql_table_function table_function, const char *name);
	void (*sabot_sql_table_function_add_parameter)(sabot_sql_table_function table_function, sabot_sql_logical_type type);
	void (*sabot_sql_table_function_add_named_parameter)(sabot_sql_table_function table_function, const char *name,
	                                                  sabot_sql_logical_type type);
	void (*sabot_sql_table_function_set_extra_info)(sabot_sql_table_function table_function, void *extra_info,
	                                             sabot_sql_delete_callback_t destroy);
	void (*sabot_sql_table_function_set_bind)(sabot_sql_table_function table_function, sabot_sql_table_function_bind_t bind);
	void (*sabot_sql_table_function_set_init)(sabot_sql_table_function table_function, sabot_sql_table_function_init_t init);
	void (*sabot_sql_table_function_set_local_init)(sabot_sql_table_function table_function,
	                                             sabot_sql_table_function_init_t init);
	void (*sabot_sql_table_function_set_function)(sabot_sql_table_function table_function, sabot_sql_table_function_t function);
	void (*sabot_sql_table_function_supports_projection_pushdown)(sabot_sql_table_function table_function, bool pushdown);
	sabot_sql_state (*sabot_sql_register_table_function)(sabot_sql_connection con, sabot_sql_table_function function);
	void *(*sabot_sql_bind_get_extra_info)(sabot_sql_bind_info info);
	void (*sabot_sql_bind_add_result_column)(sabot_sql_bind_info info, const char *name, sabot_sql_logical_type type);
	idx_t (*sabot_sql_bind_get_parameter_count)(sabot_sql_bind_info info);
	sabot_sql_value (*sabot_sql_bind_get_parameter)(sabot_sql_bind_info info, idx_t index);
	sabot_sql_value (*sabot_sql_bind_get_named_parameter)(sabot_sql_bind_info info, const char *name);
	void (*sabot_sql_bind_set_bind_data)(sabot_sql_bind_info info, void *bind_data, sabot_sql_delete_callback_t destroy);
	void (*sabot_sql_bind_set_cardinality)(sabot_sql_bind_info info, idx_t cardinality, bool is_exact);
	void (*sabot_sql_bind_set_error)(sabot_sql_bind_info info, const char *error);
	void *(*sabot_sql_init_get_extra_info)(sabot_sql_init_info info);
	void *(*sabot_sql_init_get_bind_data)(sabot_sql_init_info info);
	void (*sabot_sql_init_set_init_data)(sabot_sql_init_info info, void *init_data, sabot_sql_delete_callback_t destroy);
	idx_t (*sabot_sql_init_get_column_count)(sabot_sql_init_info info);
	idx_t (*sabot_sql_init_get_column_index)(sabot_sql_init_info info, idx_t column_index);
	void (*sabot_sql_init_set_max_threads)(sabot_sql_init_info info, idx_t max_threads);
	void (*sabot_sql_init_set_error)(sabot_sql_init_info info, const char *error);
	void *(*sabot_sql_function_get_extra_info)(sabot_sql_function_info info);
	void *(*sabot_sql_function_get_bind_data)(sabot_sql_function_info info);
	void *(*sabot_sql_function_get_init_data)(sabot_sql_function_info info);
	void *(*sabot_sql_function_get_local_init_data)(sabot_sql_function_info info);
	void (*sabot_sql_function_set_error)(sabot_sql_function_info info, const char *error);
	void (*sabot_sql_add_replacement_scan)(sabot_sql_database db, sabot_sql_replacement_callback_t replacement, void *extra_data,
	                                    sabot_sql_delete_callback_t delete_callback);
	void (*sabot_sql_replacement_scan_set_function_name)(sabot_sql_replacement_scan_info info, const char *function_name);
	void (*sabot_sql_replacement_scan_add_parameter)(sabot_sql_replacement_scan_info info, sabot_sql_value parameter);
	void (*sabot_sql_replacement_scan_set_error)(sabot_sql_replacement_scan_info info, const char *error);
	sabot_sql_value (*sabot_sql_profiling_info_get_metrics)(sabot_sql_profiling_info info);
	idx_t (*sabot_sql_profiling_info_get_child_count)(sabot_sql_profiling_info info);
	sabot_sql_profiling_info (*sabot_sql_profiling_info_get_child)(sabot_sql_profiling_info info, idx_t index);
	sabot_sql_state (*sabot_sql_appender_create)(sabot_sql_connection connection, const char *schema, const char *table,
	                                       sabot_sql_appender *out_appender);
	sabot_sql_state (*sabot_sql_appender_create_ext)(sabot_sql_connection connection, const char *catalog, const char *schema,
	                                           const char *table, sabot_sql_appender *out_appender);
	idx_t (*sabot_sql_appender_column_count)(sabot_sql_appender appender);
	sabot_sql_logical_type (*sabot_sql_appender_column_type)(sabot_sql_appender appender, idx_t col_idx);
	const char *(*sabot_sql_appender_error)(sabot_sql_appender appender);
	sabot_sql_state (*sabot_sql_appender_flush)(sabot_sql_appender appender);
	sabot_sql_state (*sabot_sql_appender_close)(sabot_sql_appender appender);
	sabot_sql_state (*sabot_sql_appender_destroy)(sabot_sql_appender *appender);
	sabot_sql_state (*sabot_sql_appender_add_column)(sabot_sql_appender appender, const char *name);
	sabot_sql_state (*sabot_sql_appender_clear_columns)(sabot_sql_appender appender);
	sabot_sql_state (*sabot_sql_append_data_chunk)(sabot_sql_appender appender, sabot_sql_data_chunk chunk);
	sabot_sql_state (*sabot_sql_table_description_create)(sabot_sql_connection connection, const char *schema, const char *table,
	                                                sabot_sql_table_description *out);
	sabot_sql_state (*sabot_sql_table_description_create_ext)(sabot_sql_connection connection, const char *catalog,
	                                                    const char *schema, const char *table,
	                                                    sabot_sql_table_description *out);
	void (*sabot_sql_table_description_destroy)(sabot_sql_table_description *table_description);
	const char *(*sabot_sql_table_description_error)(sabot_sql_table_description table_description);
	sabot_sql_state (*sabot_sql_column_has_default)(sabot_sql_table_description table_description, idx_t index, bool *out);
	char *(*sabot_sql_table_description_get_column_name)(sabot_sql_table_description table_description, idx_t index);
	void (*sabot_sql_execute_tasks)(sabot_sql_database database, idx_t max_tasks);
	sabot_sql_task_state (*sabot_sql_create_task_state)(sabot_sql_database database);
	void (*sabot_sql_execute_tasks_state)(sabot_sql_task_state state);
	idx_t (*sabot_sql_execute_n_tasks_state)(sabot_sql_task_state state, idx_t max_tasks);
	void (*sabot_sql_finish_execution)(sabot_sql_task_state state);
	bool (*sabot_sql_task_state_is_finished)(sabot_sql_task_state state);
	void (*sabot_sql_destroy_task_state)(sabot_sql_task_state state);
	bool (*sabot_sql_execution_is_finished)(sabot_sql_connection con);
	sabot_sql_data_chunk (*sabot_sql_fetch_chunk)(sabot_sql_result result);
	sabot_sql_cast_function (*sabot_sql_create_cast_function)();
	void (*sabot_sql_cast_function_set_source_type)(sabot_sql_cast_function cast_function, sabot_sql_logical_type source_type);
	void (*sabot_sql_cast_function_set_target_type)(sabot_sql_cast_function cast_function, sabot_sql_logical_type target_type);
	void (*sabot_sql_cast_function_set_implicit_cast_cost)(sabot_sql_cast_function cast_function, int64_t cost);
	void (*sabot_sql_cast_function_set_function)(sabot_sql_cast_function cast_function, sabot_sql_cast_function_t function);
	void (*sabot_sql_cast_function_set_extra_info)(sabot_sql_cast_function cast_function, void *extra_info,
	                                            sabot_sql_delete_callback_t destroy);
	void *(*sabot_sql_cast_function_get_extra_info)(sabot_sql_function_info info);
	sabot_sql_cast_mode (*sabot_sql_cast_function_get_cast_mode)(sabot_sql_function_info info);
	void (*sabot_sql_cast_function_set_error)(sabot_sql_function_info info, const char *error);
	void (*sabot_sql_cast_function_set_row_error)(sabot_sql_function_info info, const char *error, idx_t row,
	                                           sabot_sql_vector output);
	sabot_sql_state (*sabot_sql_register_cast_function)(sabot_sql_connection con, sabot_sql_cast_function cast_function);
	void (*sabot_sql_destroy_cast_function)(sabot_sql_cast_function *cast_function);
	bool (*sabot_sql_is_finite_timestamp_s)(sabot_sql_timestamp_s ts);
	bool (*sabot_sql_is_finite_timestamp_ms)(sabot_sql_timestamp_ms ts);
	bool (*sabot_sql_is_finite_timestamp_ns)(sabot_sql_timestamp_ns ts);
	sabot_sql_value (*sabot_sql_create_timestamp_tz)(sabot_sql_timestamp input);
	sabot_sql_value (*sabot_sql_create_timestamp_s)(sabot_sql_timestamp_s input);
	sabot_sql_value (*sabot_sql_create_timestamp_ms)(sabot_sql_timestamp_ms input);
	sabot_sql_value (*sabot_sql_create_timestamp_ns)(sabot_sql_timestamp_ns input);
	sabot_sql_timestamp (*sabot_sql_get_timestamp_tz)(sabot_sql_value val);
	sabot_sql_timestamp_s (*sabot_sql_get_timestamp_s)(sabot_sql_value val);
	sabot_sql_timestamp_ms (*sabot_sql_get_timestamp_ms)(sabot_sql_value val);
	sabot_sql_timestamp_ns (*sabot_sql_get_timestamp_ns)(sabot_sql_value val);
	sabot_sql_state (*sabot_sql_append_value)(sabot_sql_appender appender, sabot_sql_value value);
	sabot_sql_profiling_info (*sabot_sql_get_profiling_info)(sabot_sql_connection connection);
	sabot_sql_value (*sabot_sql_profiling_info_get_value)(sabot_sql_profiling_info info, const char *key);
	sabot_sql_state (*sabot_sql_appender_begin_row)(sabot_sql_appender appender);
	sabot_sql_state (*sabot_sql_appender_end_row)(sabot_sql_appender appender);
	sabot_sql_state (*sabot_sql_append_default)(sabot_sql_appender appender);
	sabot_sql_state (*sabot_sql_append_bool)(sabot_sql_appender appender, bool value);
	sabot_sql_state (*sabot_sql_append_int8)(sabot_sql_appender appender, int8_t value);
	sabot_sql_state (*sabot_sql_append_int16)(sabot_sql_appender appender, int16_t value);
	sabot_sql_state (*sabot_sql_append_int32)(sabot_sql_appender appender, int32_t value);
	sabot_sql_state (*sabot_sql_append_int64)(sabot_sql_appender appender, int64_t value);
	sabot_sql_state (*sabot_sql_append_hugeint)(sabot_sql_appender appender, sabot_sql_hugeint value);
	sabot_sql_state (*sabot_sql_append_uint8)(sabot_sql_appender appender, uint8_t value);
	sabot_sql_state (*sabot_sql_append_uint16)(sabot_sql_appender appender, uint16_t value);
	sabot_sql_state (*sabot_sql_append_uint32)(sabot_sql_appender appender, uint32_t value);
	sabot_sql_state (*sabot_sql_append_uint64)(sabot_sql_appender appender, uint64_t value);
	sabot_sql_state (*sabot_sql_append_uhugeint)(sabot_sql_appender appender, sabot_sql_uhugeint value);
	sabot_sql_state (*sabot_sql_append_float)(sabot_sql_appender appender, float value);
	sabot_sql_state (*sabot_sql_append_double)(sabot_sql_appender appender, double value);
	sabot_sql_state (*sabot_sql_append_date)(sabot_sql_appender appender, sabot_sql_date value);
	sabot_sql_state (*sabot_sql_append_time)(sabot_sql_appender appender, sabot_sql_time value);
	sabot_sql_state (*sabot_sql_append_timestamp)(sabot_sql_appender appender, sabot_sql_timestamp value);
	sabot_sql_state (*sabot_sql_append_interval)(sabot_sql_appender appender, sabot_sql_interval value);
	sabot_sql_state (*sabot_sql_append_varchar)(sabot_sql_appender appender, const char *val);
	sabot_sql_state (*sabot_sql_append_varchar_length)(sabot_sql_appender appender, const char *val, idx_t length);
	sabot_sql_state (*sabot_sql_append_blob)(sabot_sql_appender appender, const void *data, idx_t length);
	sabot_sql_state (*sabot_sql_append_null)(sabot_sql_appender appender);
	// These functions have been deprecated and may be removed in future versions of SabotSQL

	idx_t (*sabot_sql_row_count)(sabot_sql_result *result);
	void *(*sabot_sql_column_data)(sabot_sql_result *result, idx_t col);
	bool *(*sabot_sql_nullmask_data)(sabot_sql_result *result, idx_t col);
	sabot_sql_data_chunk (*sabot_sql_result_get_chunk)(sabot_sql_result result, idx_t chunk_index);
	bool (*sabot_sql_result_is_streaming)(sabot_sql_result result);
	idx_t (*sabot_sql_result_chunk_count)(sabot_sql_result result);
	bool (*sabot_sql_value_boolean)(sabot_sql_result *result, idx_t col, idx_t row);
	int8_t (*sabot_sql_value_int8)(sabot_sql_result *result, idx_t col, idx_t row);
	int16_t (*sabot_sql_value_int16)(sabot_sql_result *result, idx_t col, idx_t row);
	int32_t (*sabot_sql_value_int32)(sabot_sql_result *result, idx_t col, idx_t row);
	int64_t (*sabot_sql_value_int64)(sabot_sql_result *result, idx_t col, idx_t row);
	sabot_sql_hugeint (*sabot_sql_value_hugeint)(sabot_sql_result *result, idx_t col, idx_t row);
	sabot_sql_uhugeint (*sabot_sql_value_uhugeint)(sabot_sql_result *result, idx_t col, idx_t row);
	sabot_sql_decimal (*sabot_sql_value_decimal)(sabot_sql_result *result, idx_t col, idx_t row);
	uint8_t (*sabot_sql_value_uint8)(sabot_sql_result *result, idx_t col, idx_t row);
	uint16_t (*sabot_sql_value_uint16)(sabot_sql_result *result, idx_t col, idx_t row);
	uint32_t (*sabot_sql_value_uint32)(sabot_sql_result *result, idx_t col, idx_t row);
	uint64_t (*sabot_sql_value_uint64)(sabot_sql_result *result, idx_t col, idx_t row);
	float (*sabot_sql_value_float)(sabot_sql_result *result, idx_t col, idx_t row);
	double (*sabot_sql_value_double)(sabot_sql_result *result, idx_t col, idx_t row);
	sabot_sql_date (*sabot_sql_value_date)(sabot_sql_result *result, idx_t col, idx_t row);
	sabot_sql_time (*sabot_sql_value_time)(sabot_sql_result *result, idx_t col, idx_t row);
	sabot_sql_timestamp (*sabot_sql_value_timestamp)(sabot_sql_result *result, idx_t col, idx_t row);
	sabot_sql_interval (*sabot_sql_value_interval)(sabot_sql_result *result, idx_t col, idx_t row);
	char *(*sabot_sql_value_varchar)(sabot_sql_result *result, idx_t col, idx_t row);
	sabot_sql_string (*sabot_sql_value_string)(sabot_sql_result *result, idx_t col, idx_t row);
	char *(*sabot_sql_value_varchar_internal)(sabot_sql_result *result, idx_t col, idx_t row);
	sabot_sql_string (*sabot_sql_value_string_internal)(sabot_sql_result *result, idx_t col, idx_t row);
	sabot_sql_blob (*sabot_sql_value_blob)(sabot_sql_result *result, idx_t col, idx_t row);
	bool (*sabot_sql_value_is_null)(sabot_sql_result *result, idx_t col, idx_t row);
	sabot_sql_state (*sabot_sql_execute_prepared_streaming)(sabot_sql_prepared_statement prepared_statement,
	                                                  sabot_sql_result *out_result);
	sabot_sql_state (*sabot_sql_pending_prepared_streaming)(sabot_sql_prepared_statement prepared_statement,
	                                                  sabot_sql_pending_result *out_result);
	sabot_sql_state (*sabot_sql_query_arrow)(sabot_sql_connection connection, const char *query, sabot_sql_arrow *out_result);
	sabot_sql_state (*sabot_sql_query_arrow_schema)(sabot_sql_arrow result, sabot_sql_arrow_schema *out_schema);
	sabot_sql_state (*sabot_sql_prepared_arrow_schema)(sabot_sql_prepared_statement prepared, sabot_sql_arrow_schema *out_schema);
	void (*sabot_sql_result_arrow_array)(sabot_sql_result result, sabot_sql_data_chunk chunk, sabot_sql_arrow_array *out_array);
	sabot_sql_state (*sabot_sql_query_arrow_array)(sabot_sql_arrow result, sabot_sql_arrow_array *out_array);
	idx_t (*sabot_sql_arrow_column_count)(sabot_sql_arrow result);
	idx_t (*sabot_sql_arrow_row_count)(sabot_sql_arrow result);
	idx_t (*sabot_sql_arrow_rows_changed)(sabot_sql_arrow result);
	const char *(*sabot_sql_query_arrow_error)(sabot_sql_arrow result);
	void (*sabot_sql_destroy_arrow)(sabot_sql_arrow *result);
	void (*sabot_sql_destroy_arrow_stream)(sabot_sql_arrow_stream *stream_p);
	sabot_sql_state (*sabot_sql_execute_prepared_arrow)(sabot_sql_prepared_statement prepared_statement,
	                                              sabot_sql_arrow *out_result);
	sabot_sql_state (*sabot_sql_arrow_scan)(sabot_sql_connection connection, const char *table_name, sabot_sql_arrow_stream arrow);
	sabot_sql_state (*sabot_sql_arrow_array_scan)(sabot_sql_connection connection, const char *table_name,
	                                        sabot_sql_arrow_schema arrow_schema, sabot_sql_arrow_array arrow_array,
	                                        sabot_sql_arrow_stream *out_stream);
	sabot_sql_data_chunk (*sabot_sql_stream_fetch_chunk)(sabot_sql_result result);
	// Exposing the instance cache

	sabot_sql_instance_cache (*sabot_sql_create_instance_cache)();
	sabot_sql_state (*sabot_sql_get_or_create_from_cache)(sabot_sql_instance_cache instance_cache, const char *path,
	                                                sabot_sql_database *out_database, sabot_sql_config config,
	                                                char **out_error);
	void (*sabot_sql_destroy_instance_cache)(sabot_sql_instance_cache *instance_cache);
	// New append functions that are added

	sabot_sql_state (*sabot_sql_append_default_to_chunk)(sabot_sql_appender appender, sabot_sql_data_chunk chunk, idx_t col,
	                                               idx_t row);
	sabot_sql_error_data (*sabot_sql_appender_error_data)(sabot_sql_appender appender);
	sabot_sql_state (*sabot_sql_appender_create_query)(sabot_sql_connection connection, const char *query, idx_t column_count,
	                                             sabot_sql_logical_type *types, const char *table_name,
	                                             const char **column_names, sabot_sql_appender *out_appender);
	// New arrow interface functions

	sabot_sql_error_data (*sabot_sql_to_arrow_schema)(sabot_sql_arrow_options arrow_options, sabot_sql_logical_type *types,
	                                            const char **names, idx_t column_count, struct ArrowSchema *out_schema);
	sabot_sql_error_data (*sabot_sql_data_chunk_to_arrow)(sabot_sql_arrow_options arrow_options, sabot_sql_data_chunk chunk,
	                                                struct ArrowArray *out_arrow_array);
	sabot_sql_error_data (*sabot_sql_schema_from_arrow)(sabot_sql_connection connection, struct ArrowSchema *schema,
	                                              sabot_sql_arrow_converted_schema *out_types);
	sabot_sql_error_data (*sabot_sql_data_chunk_from_arrow)(sabot_sql_connection connection, struct ArrowArray *arrow_array,
	                                                  sabot_sql_arrow_converted_schema converted_schema,
	                                                  sabot_sql_data_chunk *out_chunk);
	void (*sabot_sql_destroy_arrow_converted_schema)(sabot_sql_arrow_converted_schema *arrow_converted_schema);
	// New functions for sabot_sql error data

	sabot_sql_error_data (*sabot_sql_create_error_data)(sabot_sql_error_type type, const char *message);
	void (*sabot_sql_destroy_error_data)(sabot_sql_error_data *error_data);
	sabot_sql_error_type (*sabot_sql_error_data_error_type)(sabot_sql_error_data error_data);
	const char *(*sabot_sql_error_data_message)(sabot_sql_error_data error_data);
	bool (*sabot_sql_error_data_has_error)(sabot_sql_error_data error_data);
	// API to create and manipulate expressions

	void (*sabot_sql_destroy_expression)(sabot_sql_expression *expr);
	sabot_sql_logical_type (*sabot_sql_expression_return_type)(sabot_sql_expression expr);
	bool (*sabot_sql_expression_is_foldable)(sabot_sql_expression expr);
	sabot_sql_error_data (*sabot_sql_expression_fold)(sabot_sql_client_context context, sabot_sql_expression expr,
	                                            sabot_sql_value *out_value);
	// API to manage file system operations

	sabot_sql_file_system (*sabot_sql_client_context_get_file_system)(sabot_sql_client_context context);
	void (*sabot_sql_destroy_file_system)(sabot_sql_file_system *file_system);
	sabot_sql_state (*sabot_sql_file_system_open)(sabot_sql_file_system file_system, const char *path,
	                                        sabot_sql_file_open_options options, sabot_sql_file_handle *out_file);
	sabot_sql_error_data (*sabot_sql_file_system_error_data)(sabot_sql_file_system file_system);
	sabot_sql_file_open_options (*sabot_sql_create_file_open_options)();
	sabot_sql_state (*sabot_sql_file_open_options_set_flag)(sabot_sql_file_open_options options, sabot_sql_file_flag flag,
	                                                  bool value);
	void (*sabot_sql_destroy_file_open_options)(sabot_sql_file_open_options *options);
	void (*sabot_sql_destroy_file_handle)(sabot_sql_file_handle *file_handle);
	sabot_sql_error_data (*sabot_sql_file_handle_error_data)(sabot_sql_file_handle file_handle);
	sabot_sql_state (*sabot_sql_file_handle_close)(sabot_sql_file_handle file_handle);
	int64_t (*sabot_sql_file_handle_read)(sabot_sql_file_handle file_handle, void *buffer, int64_t size);
	int64_t (*sabot_sql_file_handle_write)(sabot_sql_file_handle file_handle, const void *buffer, int64_t size);
	sabot_sql_state (*sabot_sql_file_handle_seek)(sabot_sql_file_handle file_handle, int64_t position);
	int64_t (*sabot_sql_file_handle_tell)(sabot_sql_file_handle file_handle);
	sabot_sql_state (*sabot_sql_file_handle_sync)(sabot_sql_file_handle file_handle);
	int64_t (*sabot_sql_file_handle_size)(sabot_sql_file_handle file_handle);
	// New functions around the client context

	idx_t (*sabot_sql_client_context_get_connection_id)(sabot_sql_client_context context);
	void (*sabot_sql_destroy_client_context)(sabot_sql_client_context *context);
	void (*sabot_sql_connection_get_client_context)(sabot_sql_connection connection, sabot_sql_client_context *out_context);
	sabot_sql_value (*sabot_sql_get_table_names)(sabot_sql_connection connection, const char *query, bool qualified);
	void (*sabot_sql_connection_get_arrow_options)(sabot_sql_connection connection, sabot_sql_arrow_options *out_arrow_options);
	void (*sabot_sql_destroy_arrow_options)(sabot_sql_arrow_options *arrow_options);
	// API to get information about the results of a prepared statement

	idx_t (*sabot_sql_prepared_statement_column_count)(sabot_sql_prepared_statement prepared_statement);
	const char *(*sabot_sql_prepared_statement_column_name)(sabot_sql_prepared_statement prepared_statement, idx_t col_idx);
	sabot_sql_logical_type (*sabot_sql_prepared_statement_column_logical_type)(sabot_sql_prepared_statement prepared_statement,
	                                                                     idx_t col_idx);
	sabot_sql_type (*sabot_sql_prepared_statement_column_type)(sabot_sql_prepared_statement prepared_statement, idx_t col_idx);
	// New query execution functions

	sabot_sql_arrow_options (*sabot_sql_result_get_arrow_options)(sabot_sql_result *result);
	// New functions around scalar function binding

	void (*sabot_sql_scalar_function_set_bind)(sabot_sql_scalar_function scalar_function, sabot_sql_scalar_function_bind_t bind);
	void (*sabot_sql_scalar_function_bind_set_error)(sabot_sql_bind_info info, const char *error);
	void (*sabot_sql_scalar_function_get_client_context)(sabot_sql_bind_info info, sabot_sql_client_context *out_context);
	void (*sabot_sql_scalar_function_set_bind_data)(sabot_sql_bind_info info, void *bind_data,
	                                             sabot_sql_delete_callback_t destroy);
	void *(*sabot_sql_scalar_function_get_bind_data)(sabot_sql_function_info info);
	void *(*sabot_sql_scalar_function_bind_get_extra_info)(sabot_sql_bind_info info);
	idx_t (*sabot_sql_scalar_function_bind_get_argument_count)(sabot_sql_bind_info info);
	sabot_sql_expression (*sabot_sql_scalar_function_bind_get_argument)(sabot_sql_bind_info info, idx_t index);
	void (*sabot_sql_scalar_function_set_bind_data_copy)(sabot_sql_bind_info info, sabot_sql_copy_callback_t copy);
	// New string functions that are added

	char *(*sabot_sql_value_to_string)(sabot_sql_value value);
	// New functions around table function binding

	void (*sabot_sql_table_function_get_client_context)(sabot_sql_bind_info info, sabot_sql_client_context *out_context);
	// New value functions that are added

	sabot_sql_value (*sabot_sql_create_map_value)(sabot_sql_logical_type map_type, sabot_sql_value *keys, sabot_sql_value *values,
	                                        idx_t entry_count);
	sabot_sql_value (*sabot_sql_create_union_value)(sabot_sql_logical_type union_type, idx_t tag_index, sabot_sql_value value);
	sabot_sql_value (*sabot_sql_create_time_ns)(sabot_sql_time_ns input);
	sabot_sql_time_ns (*sabot_sql_get_time_ns)(sabot_sql_value val);
	// API to create and manipulate vector types

	sabot_sql_vector (*sabot_sql_create_vector)(sabot_sql_logical_type type, idx_t capacity);
	void (*sabot_sql_destroy_vector)(sabot_sql_vector *vector);
	void (*sabot_sql_slice_vector)(sabot_sql_vector vector, sabot_sql_selection_vector sel, idx_t len);
	void (*sabot_sql_vector_reference_value)(sabot_sql_vector vector, sabot_sql_value value);
	void (*sabot_sql_vector_reference_vector)(sabot_sql_vector to_vector, sabot_sql_vector from_vector);
	sabot_sql_selection_vector (*sabot_sql_create_selection_vector)(idx_t size);
	void (*sabot_sql_destroy_selection_vector)(sabot_sql_selection_vector sel);
	sel_t *(*sabot_sql_selection_vector_get_data_ptr)(sabot_sql_selection_vector sel);
	void (*sabot_sql_vector_copy_sel)(sabot_sql_vector src, sabot_sql_vector dst, sabot_sql_selection_vector sel, idx_t src_count,
	                               idx_t src_offset, idx_t dst_offset);
} sabot_sql_ext_api_v1;

//===--------------------------------------------------------------------===//
// Struct Create Method
//===--------------------------------------------------------------------===//
inline sabot_sql_ext_api_v1 CreateAPIv1() {
	sabot_sql_ext_api_v1 result;
	result.sabot_sql_open = sabot_sql_open;
	result.sabot_sql_open_ext = sabot_sql_open_ext;
	result.sabot_sql_close = sabot_sql_close;
	result.sabot_sql_connect = sabot_sql_connect;
	result.sabot_sql_interrupt = sabot_sql_interrupt;
	result.sabot_sql_query_progress = sabot_sql_query_progress;
	result.sabot_sql_disconnect = sabot_sql_disconnect;
	result.sabot_sql_library_version = sabot_sql_library_version;
	result.sabot_sql_create_config = sabot_sql_create_config;
	result.sabot_sql_config_count = sabot_sql_config_count;
	result.sabot_sql_get_config_flag = sabot_sql_get_config_flag;
	result.sabot_sql_set_config = sabot_sql_set_config;
	result.sabot_sql_destroy_config = sabot_sql_destroy_config;
	result.sabot_sql_query = sabot_sql_query;
	result.sabot_sql_destroy_result = sabot_sql_destroy_result;
	result.sabot_sql_column_name = sabot_sql_column_name;
	result.sabot_sql_column_type = sabot_sql_column_type;
	result.sabot_sql_result_statement_type = sabot_sql_result_statement_type;
	result.sabot_sql_column_logical_type = sabot_sql_column_logical_type;
	result.sabot_sql_column_count = sabot_sql_column_count;
	result.sabot_sql_rows_changed = sabot_sql_rows_changed;
	result.sabot_sql_result_error = sabot_sql_result_error;
	result.sabot_sql_result_error_type = sabot_sql_result_error_type;
	result.sabot_sql_result_return_type = sabot_sql_result_return_type;
	result.sabot_sql_malloc = sabot_sql_malloc;
	result.sabot_sql_free = sabot_sql_free;
	result.sabot_sql_vector_size = sabot_sql_vector_size;
	result.sabot_sql_string_is_inlined = sabot_sql_string_is_inlined;
	result.sabot_sql_string_t_length = sabot_sql_string_t_length;
	result.sabot_sql_string_t_data = sabot_sql_string_t_data;
	result.sabot_sql_from_date = sabot_sql_from_date;
	result.sabot_sql_to_date = sabot_sql_to_date;
	result.sabot_sql_is_finite_date = sabot_sql_is_finite_date;
	result.sabot_sql_from_time = sabot_sql_from_time;
	result.sabot_sql_create_time_tz = sabot_sql_create_time_tz;
	result.sabot_sql_from_time_tz = sabot_sql_from_time_tz;
	result.sabot_sql_to_time = sabot_sql_to_time;
	result.sabot_sql_from_timestamp = sabot_sql_from_timestamp;
	result.sabot_sql_to_timestamp = sabot_sql_to_timestamp;
	result.sabot_sql_is_finite_timestamp = sabot_sql_is_finite_timestamp;
	result.sabot_sql_hugeint_to_double = sabot_sql_hugeint_to_double;
	result.sabot_sql_double_to_hugeint = sabot_sql_double_to_hugeint;
	result.sabot_sql_uhugeint_to_double = sabot_sql_uhugeint_to_double;
	result.sabot_sql_double_to_uhugeint = sabot_sql_double_to_uhugeint;
	result.sabot_sql_double_to_decimal = sabot_sql_double_to_decimal;
	result.sabot_sql_decimal_to_double = sabot_sql_decimal_to_double;
	result.sabot_sql_prepare = sabot_sql_prepare;
	result.sabot_sql_destroy_prepare = sabot_sql_destroy_prepare;
	result.sabot_sql_prepare_error = sabot_sql_prepare_error;
	result.sabot_sql_nparams = sabot_sql_nparams;
	result.sabot_sql_parameter_name = sabot_sql_parameter_name;
	result.sabot_sql_param_type = sabot_sql_param_type;
	result.sabot_sql_param_logical_type = sabot_sql_param_logical_type;
	result.sabot_sql_clear_bindings = sabot_sql_clear_bindings;
	result.sabot_sql_prepared_statement_type = sabot_sql_prepared_statement_type;
	result.sabot_sql_bind_value = sabot_sql_bind_value;
	result.sabot_sql_bind_parameter_index = sabot_sql_bind_parameter_index;
	result.sabot_sql_bind_boolean = sabot_sql_bind_boolean;
	result.sabot_sql_bind_int8 = sabot_sql_bind_int8;
	result.sabot_sql_bind_int16 = sabot_sql_bind_int16;
	result.sabot_sql_bind_int32 = sabot_sql_bind_int32;
	result.sabot_sql_bind_int64 = sabot_sql_bind_int64;
	result.sabot_sql_bind_hugeint = sabot_sql_bind_hugeint;
	result.sabot_sql_bind_uhugeint = sabot_sql_bind_uhugeint;
	result.sabot_sql_bind_decimal = sabot_sql_bind_decimal;
	result.sabot_sql_bind_uint8 = sabot_sql_bind_uint8;
	result.sabot_sql_bind_uint16 = sabot_sql_bind_uint16;
	result.sabot_sql_bind_uint32 = sabot_sql_bind_uint32;
	result.sabot_sql_bind_uint64 = sabot_sql_bind_uint64;
	result.sabot_sql_bind_float = sabot_sql_bind_float;
	result.sabot_sql_bind_double = sabot_sql_bind_double;
	result.sabot_sql_bind_date = sabot_sql_bind_date;
	result.sabot_sql_bind_time = sabot_sql_bind_time;
	result.sabot_sql_bind_timestamp = sabot_sql_bind_timestamp;
	result.sabot_sql_bind_timestamp_tz = sabot_sql_bind_timestamp_tz;
	result.sabot_sql_bind_interval = sabot_sql_bind_interval;
	result.sabot_sql_bind_varchar = sabot_sql_bind_varchar;
	result.sabot_sql_bind_varchar_length = sabot_sql_bind_varchar_length;
	result.sabot_sql_bind_blob = sabot_sql_bind_blob;
	result.sabot_sql_bind_null = sabot_sql_bind_null;
	result.sabot_sql_execute_prepared = sabot_sql_execute_prepared;
	result.sabot_sql_extract_statements = sabot_sql_extract_statements;
	result.sabot_sql_prepare_extracted_statement = sabot_sql_prepare_extracted_statement;
	result.sabot_sql_extract_statements_error = sabot_sql_extract_statements_error;
	result.sabot_sql_destroy_extracted = sabot_sql_destroy_extracted;
	result.sabot_sql_pending_prepared = sabot_sql_pending_prepared;
	result.sabot_sql_destroy_pending = sabot_sql_destroy_pending;
	result.sabot_sql_pending_error = sabot_sql_pending_error;
	result.sabot_sql_pending_execute_task = sabot_sql_pending_execute_task;
	result.sabot_sql_pending_execute_check_state = sabot_sql_pending_execute_check_state;
	result.sabot_sql_execute_pending = sabot_sql_execute_pending;
	result.sabot_sql_pending_execution_is_finished = sabot_sql_pending_execution_is_finished;
	result.sabot_sql_destroy_value = sabot_sql_destroy_value;
	result.sabot_sql_create_varchar = sabot_sql_create_varchar;
	result.sabot_sql_create_varchar_length = sabot_sql_create_varchar_length;
	result.sabot_sql_create_bool = sabot_sql_create_bool;
	result.sabot_sql_create_int8 = sabot_sql_create_int8;
	result.sabot_sql_create_uint8 = sabot_sql_create_uint8;
	result.sabot_sql_create_int16 = sabot_sql_create_int16;
	result.sabot_sql_create_uint16 = sabot_sql_create_uint16;
	result.sabot_sql_create_int32 = sabot_sql_create_int32;
	result.sabot_sql_create_uint32 = sabot_sql_create_uint32;
	result.sabot_sql_create_uint64 = sabot_sql_create_uint64;
	result.sabot_sql_create_int64 = sabot_sql_create_int64;
	result.sabot_sql_create_hugeint = sabot_sql_create_hugeint;
	result.sabot_sql_create_uhugeint = sabot_sql_create_uhugeint;
	result.sabot_sql_create_float = sabot_sql_create_float;
	result.sabot_sql_create_double = sabot_sql_create_double;
	result.sabot_sql_create_date = sabot_sql_create_date;
	result.sabot_sql_create_time = sabot_sql_create_time;
	result.sabot_sql_create_time_tz_value = sabot_sql_create_time_tz_value;
	result.sabot_sql_create_timestamp = sabot_sql_create_timestamp;
	result.sabot_sql_create_interval = sabot_sql_create_interval;
	result.sabot_sql_create_blob = sabot_sql_create_blob;
	result.sabot_sql_create_bignum = sabot_sql_create_bignum;
	result.sabot_sql_create_decimal = sabot_sql_create_decimal;
	result.sabot_sql_create_bit = sabot_sql_create_bit;
	result.sabot_sql_create_uuid = sabot_sql_create_uuid;
	result.sabot_sql_get_bool = sabot_sql_get_bool;
	result.sabot_sql_get_int8 = sabot_sql_get_int8;
	result.sabot_sql_get_uint8 = sabot_sql_get_uint8;
	result.sabot_sql_get_int16 = sabot_sql_get_int16;
	result.sabot_sql_get_uint16 = sabot_sql_get_uint16;
	result.sabot_sql_get_int32 = sabot_sql_get_int32;
	result.sabot_sql_get_uint32 = sabot_sql_get_uint32;
	result.sabot_sql_get_int64 = sabot_sql_get_int64;
	result.sabot_sql_get_uint64 = sabot_sql_get_uint64;
	result.sabot_sql_get_hugeint = sabot_sql_get_hugeint;
	result.sabot_sql_get_uhugeint = sabot_sql_get_uhugeint;
	result.sabot_sql_get_float = sabot_sql_get_float;
	result.sabot_sql_get_double = sabot_sql_get_double;
	result.sabot_sql_get_date = sabot_sql_get_date;
	result.sabot_sql_get_time = sabot_sql_get_time;
	result.sabot_sql_get_time_tz = sabot_sql_get_time_tz;
	result.sabot_sql_get_timestamp = sabot_sql_get_timestamp;
	result.sabot_sql_get_interval = sabot_sql_get_interval;
	result.sabot_sql_get_value_type = sabot_sql_get_value_type;
	result.sabot_sql_get_blob = sabot_sql_get_blob;
	result.sabot_sql_get_bignum = sabot_sql_get_bignum;
	result.sabot_sql_get_decimal = sabot_sql_get_decimal;
	result.sabot_sql_get_bit = sabot_sql_get_bit;
	result.sabot_sql_get_uuid = sabot_sql_get_uuid;
	result.sabot_sql_get_varchar = sabot_sql_get_varchar;
	result.sabot_sql_create_struct_value = sabot_sql_create_struct_value;
	result.sabot_sql_create_list_value = sabot_sql_create_list_value;
	result.sabot_sql_create_array_value = sabot_sql_create_array_value;
	result.sabot_sql_get_map_size = sabot_sql_get_map_size;
	result.sabot_sql_get_map_key = sabot_sql_get_map_key;
	result.sabot_sql_get_map_value = sabot_sql_get_map_value;
	result.sabot_sql_is_null_value = sabot_sql_is_null_value;
	result.sabot_sql_create_null_value = sabot_sql_create_null_value;
	result.sabot_sql_get_list_size = sabot_sql_get_list_size;
	result.sabot_sql_get_list_child = sabot_sql_get_list_child;
	result.sabot_sql_create_enum_value = sabot_sql_create_enum_value;
	result.sabot_sql_get_enum_value = sabot_sql_get_enum_value;
	result.sabot_sql_get_struct_child = sabot_sql_get_struct_child;
	result.sabot_sql_create_logical_type = sabot_sql_create_logical_type;
	result.sabot_sql_logical_type_get_alias = sabot_sql_logical_type_get_alias;
	result.sabot_sql_logical_type_set_alias = sabot_sql_logical_type_set_alias;
	result.sabot_sql_create_list_type = sabot_sql_create_list_type;
	result.sabot_sql_create_array_type = sabot_sql_create_array_type;
	result.sabot_sql_create_map_type = sabot_sql_create_map_type;
	result.sabot_sql_create_union_type = sabot_sql_create_union_type;
	result.sabot_sql_create_struct_type = sabot_sql_create_struct_type;
	result.sabot_sql_create_enum_type = sabot_sql_create_enum_type;
	result.sabot_sql_create_decimal_type = sabot_sql_create_decimal_type;
	result.sabot_sql_get_type_id = sabot_sql_get_type_id;
	result.sabot_sql_decimal_width = sabot_sql_decimal_width;
	result.sabot_sql_decimal_scale = sabot_sql_decimal_scale;
	result.sabot_sql_decimal_internal_type = sabot_sql_decimal_internal_type;
	result.sabot_sql_enum_internal_type = sabot_sql_enum_internal_type;
	result.sabot_sql_enum_dictionary_size = sabot_sql_enum_dictionary_size;
	result.sabot_sql_enum_dictionary_value = sabot_sql_enum_dictionary_value;
	result.sabot_sql_list_type_child_type = sabot_sql_list_type_child_type;
	result.sabot_sql_array_type_child_type = sabot_sql_array_type_child_type;
	result.sabot_sql_array_type_array_size = sabot_sql_array_type_array_size;
	result.sabot_sql_map_type_key_type = sabot_sql_map_type_key_type;
	result.sabot_sql_map_type_value_type = sabot_sql_map_type_value_type;
	result.sabot_sql_struct_type_child_count = sabot_sql_struct_type_child_count;
	result.sabot_sql_struct_type_child_name = sabot_sql_struct_type_child_name;
	result.sabot_sql_struct_type_child_type = sabot_sql_struct_type_child_type;
	result.sabot_sql_union_type_member_count = sabot_sql_union_type_member_count;
	result.sabot_sql_union_type_member_name = sabot_sql_union_type_member_name;
	result.sabot_sql_union_type_member_type = sabot_sql_union_type_member_type;
	result.sabot_sql_destroy_logical_type = sabot_sql_destroy_logical_type;
	result.sabot_sql_register_logical_type = sabot_sql_register_logical_type;
	result.sabot_sql_create_data_chunk = sabot_sql_create_data_chunk;
	result.sabot_sql_destroy_data_chunk = sabot_sql_destroy_data_chunk;
	result.sabot_sql_data_chunk_reset = sabot_sql_data_chunk_reset;
	result.sabot_sql_data_chunk_get_column_count = sabot_sql_data_chunk_get_column_count;
	result.sabot_sql_data_chunk_get_vector = sabot_sql_data_chunk_get_vector;
	result.sabot_sql_data_chunk_get_size = sabot_sql_data_chunk_get_size;
	result.sabot_sql_data_chunk_set_size = sabot_sql_data_chunk_set_size;
	result.sabot_sql_vector_get_column_type = sabot_sql_vector_get_column_type;
	result.sabot_sql_vector_get_data = sabot_sql_vector_get_data;
	result.sabot_sql_vector_get_validity = sabot_sql_vector_get_validity;
	result.sabot_sql_vector_ensure_validity_writable = sabot_sql_vector_ensure_validity_writable;
	result.sabot_sql_vector_assign_string_element = sabot_sql_vector_assign_string_element;
	result.sabot_sql_vector_assign_string_element_len = sabot_sql_vector_assign_string_element_len;
	result.sabot_sql_list_vector_get_child = sabot_sql_list_vector_get_child;
	result.sabot_sql_list_vector_get_size = sabot_sql_list_vector_get_size;
	result.sabot_sql_list_vector_set_size = sabot_sql_list_vector_set_size;
	result.sabot_sql_list_vector_reserve = sabot_sql_list_vector_reserve;
	result.sabot_sql_struct_vector_get_child = sabot_sql_struct_vector_get_child;
	result.sabot_sql_array_vector_get_child = sabot_sql_array_vector_get_child;
	result.sabot_sql_validity_row_is_valid = sabot_sql_validity_row_is_valid;
	result.sabot_sql_validity_set_row_validity = sabot_sql_validity_set_row_validity;
	result.sabot_sql_validity_set_row_invalid = sabot_sql_validity_set_row_invalid;
	result.sabot_sql_validity_set_row_valid = sabot_sql_validity_set_row_valid;
	result.sabot_sql_create_scalar_function = sabot_sql_create_scalar_function;
	result.sabot_sql_destroy_scalar_function = sabot_sql_destroy_scalar_function;
	result.sabot_sql_scalar_function_set_name = sabot_sql_scalar_function_set_name;
	result.sabot_sql_scalar_function_set_varargs = sabot_sql_scalar_function_set_varargs;
	result.sabot_sql_scalar_function_set_special_handling = sabot_sql_scalar_function_set_special_handling;
	result.sabot_sql_scalar_function_set_volatile = sabot_sql_scalar_function_set_volatile;
	result.sabot_sql_scalar_function_add_parameter = sabot_sql_scalar_function_add_parameter;
	result.sabot_sql_scalar_function_set_return_type = sabot_sql_scalar_function_set_return_type;
	result.sabot_sql_scalar_function_set_extra_info = sabot_sql_scalar_function_set_extra_info;
	result.sabot_sql_scalar_function_set_function = sabot_sql_scalar_function_set_function;
	result.sabot_sql_register_scalar_function = sabot_sql_register_scalar_function;
	result.sabot_sql_scalar_function_get_extra_info = sabot_sql_scalar_function_get_extra_info;
	result.sabot_sql_scalar_function_set_error = sabot_sql_scalar_function_set_error;
	result.sabot_sql_create_scalar_function_set = sabot_sql_create_scalar_function_set;
	result.sabot_sql_destroy_scalar_function_set = sabot_sql_destroy_scalar_function_set;
	result.sabot_sql_add_scalar_function_to_set = sabot_sql_add_scalar_function_to_set;
	result.sabot_sql_register_scalar_function_set = sabot_sql_register_scalar_function_set;
	result.sabot_sql_create_aggregate_function = sabot_sql_create_aggregate_function;
	result.sabot_sql_destroy_aggregate_function = sabot_sql_destroy_aggregate_function;
	result.sabot_sql_aggregate_function_set_name = sabot_sql_aggregate_function_set_name;
	result.sabot_sql_aggregate_function_add_parameter = sabot_sql_aggregate_function_add_parameter;
	result.sabot_sql_aggregate_function_set_return_type = sabot_sql_aggregate_function_set_return_type;
	result.sabot_sql_aggregate_function_set_functions = sabot_sql_aggregate_function_set_functions;
	result.sabot_sql_aggregate_function_set_destructor = sabot_sql_aggregate_function_set_destructor;
	result.sabot_sql_register_aggregate_function = sabot_sql_register_aggregate_function;
	result.sabot_sql_aggregate_function_set_special_handling = sabot_sql_aggregate_function_set_special_handling;
	result.sabot_sql_aggregate_function_set_extra_info = sabot_sql_aggregate_function_set_extra_info;
	result.sabot_sql_aggregate_function_get_extra_info = sabot_sql_aggregate_function_get_extra_info;
	result.sabot_sql_aggregate_function_set_error = sabot_sql_aggregate_function_set_error;
	result.sabot_sql_create_aggregate_function_set = sabot_sql_create_aggregate_function_set;
	result.sabot_sql_destroy_aggregate_function_set = sabot_sql_destroy_aggregate_function_set;
	result.sabot_sql_add_aggregate_function_to_set = sabot_sql_add_aggregate_function_to_set;
	result.sabot_sql_register_aggregate_function_set = sabot_sql_register_aggregate_function_set;
	result.sabot_sql_create_table_function = sabot_sql_create_table_function;
	result.sabot_sql_destroy_table_function = sabot_sql_destroy_table_function;
	result.sabot_sql_table_function_set_name = sabot_sql_table_function_set_name;
	result.sabot_sql_table_function_add_parameter = sabot_sql_table_function_add_parameter;
	result.sabot_sql_table_function_add_named_parameter = sabot_sql_table_function_add_named_parameter;
	result.sabot_sql_table_function_set_extra_info = sabot_sql_table_function_set_extra_info;
	result.sabot_sql_table_function_set_bind = sabot_sql_table_function_set_bind;
	result.sabot_sql_table_function_set_init = sabot_sql_table_function_set_init;
	result.sabot_sql_table_function_set_local_init = sabot_sql_table_function_set_local_init;
	result.sabot_sql_table_function_set_function = sabot_sql_table_function_set_function;
	result.sabot_sql_table_function_supports_projection_pushdown = sabot_sql_table_function_supports_projection_pushdown;
	result.sabot_sql_register_table_function = sabot_sql_register_table_function;
	result.sabot_sql_bind_get_extra_info = sabot_sql_bind_get_extra_info;
	result.sabot_sql_bind_add_result_column = sabot_sql_bind_add_result_column;
	result.sabot_sql_bind_get_parameter_count = sabot_sql_bind_get_parameter_count;
	result.sabot_sql_bind_get_parameter = sabot_sql_bind_get_parameter;
	result.sabot_sql_bind_get_named_parameter = sabot_sql_bind_get_named_parameter;
	result.sabot_sql_bind_set_bind_data = sabot_sql_bind_set_bind_data;
	result.sabot_sql_bind_set_cardinality = sabot_sql_bind_set_cardinality;
	result.sabot_sql_bind_set_error = sabot_sql_bind_set_error;
	result.sabot_sql_init_get_extra_info = sabot_sql_init_get_extra_info;
	result.sabot_sql_init_get_bind_data = sabot_sql_init_get_bind_data;
	result.sabot_sql_init_set_init_data = sabot_sql_init_set_init_data;
	result.sabot_sql_init_get_column_count = sabot_sql_init_get_column_count;
	result.sabot_sql_init_get_column_index = sabot_sql_init_get_column_index;
	result.sabot_sql_init_set_max_threads = sabot_sql_init_set_max_threads;
	result.sabot_sql_init_set_error = sabot_sql_init_set_error;
	result.sabot_sql_function_get_extra_info = sabot_sql_function_get_extra_info;
	result.sabot_sql_function_get_bind_data = sabot_sql_function_get_bind_data;
	result.sabot_sql_function_get_init_data = sabot_sql_function_get_init_data;
	result.sabot_sql_function_get_local_init_data = sabot_sql_function_get_local_init_data;
	result.sabot_sql_function_set_error = sabot_sql_function_set_error;
	result.sabot_sql_add_replacement_scan = sabot_sql_add_replacement_scan;
	result.sabot_sql_replacement_scan_set_function_name = sabot_sql_replacement_scan_set_function_name;
	result.sabot_sql_replacement_scan_add_parameter = sabot_sql_replacement_scan_add_parameter;
	result.sabot_sql_replacement_scan_set_error = sabot_sql_replacement_scan_set_error;
	result.sabot_sql_profiling_info_get_metrics = sabot_sql_profiling_info_get_metrics;
	result.sabot_sql_profiling_info_get_child_count = sabot_sql_profiling_info_get_child_count;
	result.sabot_sql_profiling_info_get_child = sabot_sql_profiling_info_get_child;
	result.sabot_sql_appender_create = sabot_sql_appender_create;
	result.sabot_sql_appender_create_ext = sabot_sql_appender_create_ext;
	result.sabot_sql_appender_column_count = sabot_sql_appender_column_count;
	result.sabot_sql_appender_column_type = sabot_sql_appender_column_type;
	result.sabot_sql_appender_error = sabot_sql_appender_error;
	result.sabot_sql_appender_flush = sabot_sql_appender_flush;
	result.sabot_sql_appender_close = sabot_sql_appender_close;
	result.sabot_sql_appender_destroy = sabot_sql_appender_destroy;
	result.sabot_sql_appender_add_column = sabot_sql_appender_add_column;
	result.sabot_sql_appender_clear_columns = sabot_sql_appender_clear_columns;
	result.sabot_sql_append_data_chunk = sabot_sql_append_data_chunk;
	result.sabot_sql_table_description_create = sabot_sql_table_description_create;
	result.sabot_sql_table_description_create_ext = sabot_sql_table_description_create_ext;
	result.sabot_sql_table_description_destroy = sabot_sql_table_description_destroy;
	result.sabot_sql_table_description_error = sabot_sql_table_description_error;
	result.sabot_sql_column_has_default = sabot_sql_column_has_default;
	result.sabot_sql_table_description_get_column_name = sabot_sql_table_description_get_column_name;
	result.sabot_sql_execute_tasks = sabot_sql_execute_tasks;
	result.sabot_sql_create_task_state = sabot_sql_create_task_state;
	result.sabot_sql_execute_tasks_state = sabot_sql_execute_tasks_state;
	result.sabot_sql_execute_n_tasks_state = sabot_sql_execute_n_tasks_state;
	result.sabot_sql_finish_execution = sabot_sql_finish_execution;
	result.sabot_sql_task_state_is_finished = sabot_sql_task_state_is_finished;
	result.sabot_sql_destroy_task_state = sabot_sql_destroy_task_state;
	result.sabot_sql_execution_is_finished = sabot_sql_execution_is_finished;
	result.sabot_sql_fetch_chunk = sabot_sql_fetch_chunk;
	result.sabot_sql_create_cast_function = sabot_sql_create_cast_function;
	result.sabot_sql_cast_function_set_source_type = sabot_sql_cast_function_set_source_type;
	result.sabot_sql_cast_function_set_target_type = sabot_sql_cast_function_set_target_type;
	result.sabot_sql_cast_function_set_implicit_cast_cost = sabot_sql_cast_function_set_implicit_cast_cost;
	result.sabot_sql_cast_function_set_function = sabot_sql_cast_function_set_function;
	result.sabot_sql_cast_function_set_extra_info = sabot_sql_cast_function_set_extra_info;
	result.sabot_sql_cast_function_get_extra_info = sabot_sql_cast_function_get_extra_info;
	result.sabot_sql_cast_function_get_cast_mode = sabot_sql_cast_function_get_cast_mode;
	result.sabot_sql_cast_function_set_error = sabot_sql_cast_function_set_error;
	result.sabot_sql_cast_function_set_row_error = sabot_sql_cast_function_set_row_error;
	result.sabot_sql_register_cast_function = sabot_sql_register_cast_function;
	result.sabot_sql_destroy_cast_function = sabot_sql_destroy_cast_function;
	result.sabot_sql_is_finite_timestamp_s = sabot_sql_is_finite_timestamp_s;
	result.sabot_sql_is_finite_timestamp_ms = sabot_sql_is_finite_timestamp_ms;
	result.sabot_sql_is_finite_timestamp_ns = sabot_sql_is_finite_timestamp_ns;
	result.sabot_sql_create_timestamp_tz = sabot_sql_create_timestamp_tz;
	result.sabot_sql_create_timestamp_s = sabot_sql_create_timestamp_s;
	result.sabot_sql_create_timestamp_ms = sabot_sql_create_timestamp_ms;
	result.sabot_sql_create_timestamp_ns = sabot_sql_create_timestamp_ns;
	result.sabot_sql_get_timestamp_tz = sabot_sql_get_timestamp_tz;
	result.sabot_sql_get_timestamp_s = sabot_sql_get_timestamp_s;
	result.sabot_sql_get_timestamp_ms = sabot_sql_get_timestamp_ms;
	result.sabot_sql_get_timestamp_ns = sabot_sql_get_timestamp_ns;
	result.sabot_sql_append_value = sabot_sql_append_value;
	result.sabot_sql_get_profiling_info = sabot_sql_get_profiling_info;
	result.sabot_sql_profiling_info_get_value = sabot_sql_profiling_info_get_value;
	result.sabot_sql_appender_begin_row = sabot_sql_appender_begin_row;
	result.sabot_sql_appender_end_row = sabot_sql_appender_end_row;
	result.sabot_sql_append_default = sabot_sql_append_default;
	result.sabot_sql_append_bool = sabot_sql_append_bool;
	result.sabot_sql_append_int8 = sabot_sql_append_int8;
	result.sabot_sql_append_int16 = sabot_sql_append_int16;
	result.sabot_sql_append_int32 = sabot_sql_append_int32;
	result.sabot_sql_append_int64 = sabot_sql_append_int64;
	result.sabot_sql_append_hugeint = sabot_sql_append_hugeint;
	result.sabot_sql_append_uint8 = sabot_sql_append_uint8;
	result.sabot_sql_append_uint16 = sabot_sql_append_uint16;
	result.sabot_sql_append_uint32 = sabot_sql_append_uint32;
	result.sabot_sql_append_uint64 = sabot_sql_append_uint64;
	result.sabot_sql_append_uhugeint = sabot_sql_append_uhugeint;
	result.sabot_sql_append_float = sabot_sql_append_float;
	result.sabot_sql_append_double = sabot_sql_append_double;
	result.sabot_sql_append_date = sabot_sql_append_date;
	result.sabot_sql_append_time = sabot_sql_append_time;
	result.sabot_sql_append_timestamp = sabot_sql_append_timestamp;
	result.sabot_sql_append_interval = sabot_sql_append_interval;
	result.sabot_sql_append_varchar = sabot_sql_append_varchar;
	result.sabot_sql_append_varchar_length = sabot_sql_append_varchar_length;
	result.sabot_sql_append_blob = sabot_sql_append_blob;
	result.sabot_sql_append_null = sabot_sql_append_null;
	result.sabot_sql_row_count = sabot_sql_row_count;
	result.sabot_sql_column_data = sabot_sql_column_data;
	result.sabot_sql_nullmask_data = sabot_sql_nullmask_data;
	result.sabot_sql_result_get_chunk = sabot_sql_result_get_chunk;
	result.sabot_sql_result_is_streaming = sabot_sql_result_is_streaming;
	result.sabot_sql_result_chunk_count = sabot_sql_result_chunk_count;
	result.sabot_sql_value_boolean = sabot_sql_value_boolean;
	result.sabot_sql_value_int8 = sabot_sql_value_int8;
	result.sabot_sql_value_int16 = sabot_sql_value_int16;
	result.sabot_sql_value_int32 = sabot_sql_value_int32;
	result.sabot_sql_value_int64 = sabot_sql_value_int64;
	result.sabot_sql_value_hugeint = sabot_sql_value_hugeint;
	result.sabot_sql_value_uhugeint = sabot_sql_value_uhugeint;
	result.sabot_sql_value_decimal = sabot_sql_value_decimal;
	result.sabot_sql_value_uint8 = sabot_sql_value_uint8;
	result.sabot_sql_value_uint16 = sabot_sql_value_uint16;
	result.sabot_sql_value_uint32 = sabot_sql_value_uint32;
	result.sabot_sql_value_uint64 = sabot_sql_value_uint64;
	result.sabot_sql_value_float = sabot_sql_value_float;
	result.sabot_sql_value_double = sabot_sql_value_double;
	result.sabot_sql_value_date = sabot_sql_value_date;
	result.sabot_sql_value_time = sabot_sql_value_time;
	result.sabot_sql_value_timestamp = sabot_sql_value_timestamp;
	result.sabot_sql_value_interval = sabot_sql_value_interval;
	result.sabot_sql_value_varchar = sabot_sql_value_varchar;
	result.sabot_sql_value_string = sabot_sql_value_string;
	result.sabot_sql_value_varchar_internal = sabot_sql_value_varchar_internal;
	result.sabot_sql_value_string_internal = sabot_sql_value_string_internal;
	result.sabot_sql_value_blob = sabot_sql_value_blob;
	result.sabot_sql_value_is_null = sabot_sql_value_is_null;
	result.sabot_sql_execute_prepared_streaming = sabot_sql_execute_prepared_streaming;
	result.sabot_sql_pending_prepared_streaming = sabot_sql_pending_prepared_streaming;
	result.sabot_sql_query_arrow = sabot_sql_query_arrow;
	result.sabot_sql_query_arrow_schema = sabot_sql_query_arrow_schema;
	result.sabot_sql_prepared_arrow_schema = sabot_sql_prepared_arrow_schema;
	result.sabot_sql_result_arrow_array = sabot_sql_result_arrow_array;
	result.sabot_sql_query_arrow_array = sabot_sql_query_arrow_array;
	result.sabot_sql_arrow_column_count = sabot_sql_arrow_column_count;
	result.sabot_sql_arrow_row_count = sabot_sql_arrow_row_count;
	result.sabot_sql_arrow_rows_changed = sabot_sql_arrow_rows_changed;
	result.sabot_sql_query_arrow_error = sabot_sql_query_arrow_error;
	result.sabot_sql_destroy_arrow = sabot_sql_destroy_arrow;
	result.sabot_sql_destroy_arrow_stream = sabot_sql_destroy_arrow_stream;
	result.sabot_sql_execute_prepared_arrow = sabot_sql_execute_prepared_arrow;
	result.sabot_sql_arrow_scan = sabot_sql_arrow_scan;
	result.sabot_sql_arrow_array_scan = sabot_sql_arrow_array_scan;
	result.sabot_sql_stream_fetch_chunk = sabot_sql_stream_fetch_chunk;
	result.sabot_sql_create_instance_cache = sabot_sql_create_instance_cache;
	result.sabot_sql_get_or_create_from_cache = sabot_sql_get_or_create_from_cache;
	result.sabot_sql_destroy_instance_cache = sabot_sql_destroy_instance_cache;
	result.sabot_sql_append_default_to_chunk = sabot_sql_append_default_to_chunk;
	result.sabot_sql_appender_error_data = sabot_sql_appender_error_data;
	result.sabot_sql_appender_create_query = sabot_sql_appender_create_query;
	result.sabot_sql_to_arrow_schema = sabot_sql_to_arrow_schema;
	result.sabot_sql_data_chunk_to_arrow = sabot_sql_data_chunk_to_arrow;
	result.sabot_sql_schema_from_arrow = sabot_sql_schema_from_arrow;
	result.sabot_sql_data_chunk_from_arrow = sabot_sql_data_chunk_from_arrow;
	result.sabot_sql_destroy_arrow_converted_schema = sabot_sql_destroy_arrow_converted_schema;
	result.sabot_sql_create_error_data = sabot_sql_create_error_data;
	result.sabot_sql_destroy_error_data = sabot_sql_destroy_error_data;
	result.sabot_sql_error_data_error_type = sabot_sql_error_data_error_type;
	result.sabot_sql_error_data_message = sabot_sql_error_data_message;
	result.sabot_sql_error_data_has_error = sabot_sql_error_data_has_error;
	result.sabot_sql_destroy_expression = sabot_sql_destroy_expression;
	result.sabot_sql_expression_return_type = sabot_sql_expression_return_type;
	result.sabot_sql_expression_is_foldable = sabot_sql_expression_is_foldable;
	result.sabot_sql_expression_fold = sabot_sql_expression_fold;
	result.sabot_sql_client_context_get_file_system = sabot_sql_client_context_get_file_system;
	result.sabot_sql_destroy_file_system = sabot_sql_destroy_file_system;
	result.sabot_sql_file_system_open = sabot_sql_file_system_open;
	result.sabot_sql_file_system_error_data = sabot_sql_file_system_error_data;
	result.sabot_sql_create_file_open_options = sabot_sql_create_file_open_options;
	result.sabot_sql_file_open_options_set_flag = sabot_sql_file_open_options_set_flag;
	result.sabot_sql_destroy_file_open_options = sabot_sql_destroy_file_open_options;
	result.sabot_sql_destroy_file_handle = sabot_sql_destroy_file_handle;
	result.sabot_sql_file_handle_error_data = sabot_sql_file_handle_error_data;
	result.sabot_sql_file_handle_close = sabot_sql_file_handle_close;
	result.sabot_sql_file_handle_read = sabot_sql_file_handle_read;
	result.sabot_sql_file_handle_write = sabot_sql_file_handle_write;
	result.sabot_sql_file_handle_seek = sabot_sql_file_handle_seek;
	result.sabot_sql_file_handle_tell = sabot_sql_file_handle_tell;
	result.sabot_sql_file_handle_sync = sabot_sql_file_handle_sync;
	result.sabot_sql_file_handle_size = sabot_sql_file_handle_size;
	result.sabot_sql_client_context_get_connection_id = sabot_sql_client_context_get_connection_id;
	result.sabot_sql_destroy_client_context = sabot_sql_destroy_client_context;
	result.sabot_sql_connection_get_client_context = sabot_sql_connection_get_client_context;
	result.sabot_sql_get_table_names = sabot_sql_get_table_names;
	result.sabot_sql_connection_get_arrow_options = sabot_sql_connection_get_arrow_options;
	result.sabot_sql_destroy_arrow_options = sabot_sql_destroy_arrow_options;
	result.sabot_sql_prepared_statement_column_count = sabot_sql_prepared_statement_column_count;
	result.sabot_sql_prepared_statement_column_name = sabot_sql_prepared_statement_column_name;
	result.sabot_sql_prepared_statement_column_logical_type = sabot_sql_prepared_statement_column_logical_type;
	result.sabot_sql_prepared_statement_column_type = sabot_sql_prepared_statement_column_type;
	result.sabot_sql_result_get_arrow_options = sabot_sql_result_get_arrow_options;
	result.sabot_sql_scalar_function_set_bind = sabot_sql_scalar_function_set_bind;
	result.sabot_sql_scalar_function_bind_set_error = sabot_sql_scalar_function_bind_set_error;
	result.sabot_sql_scalar_function_get_client_context = sabot_sql_scalar_function_get_client_context;
	result.sabot_sql_scalar_function_set_bind_data = sabot_sql_scalar_function_set_bind_data;
	result.sabot_sql_scalar_function_get_bind_data = sabot_sql_scalar_function_get_bind_data;
	result.sabot_sql_scalar_function_bind_get_extra_info = sabot_sql_scalar_function_bind_get_extra_info;
	result.sabot_sql_scalar_function_bind_get_argument_count = sabot_sql_scalar_function_bind_get_argument_count;
	result.sabot_sql_scalar_function_bind_get_argument = sabot_sql_scalar_function_bind_get_argument;
	result.sabot_sql_scalar_function_set_bind_data_copy = sabot_sql_scalar_function_set_bind_data_copy;
	result.sabot_sql_value_to_string = sabot_sql_value_to_string;
	result.sabot_sql_table_function_get_client_context = sabot_sql_table_function_get_client_context;
	result.sabot_sql_create_map_value = sabot_sql_create_map_value;
	result.sabot_sql_create_union_value = sabot_sql_create_union_value;
	result.sabot_sql_create_time_ns = sabot_sql_create_time_ns;
	result.sabot_sql_get_time_ns = sabot_sql_get_time_ns;
	result.sabot_sql_create_vector = sabot_sql_create_vector;
	result.sabot_sql_destroy_vector = sabot_sql_destroy_vector;
	result.sabot_sql_slice_vector = sabot_sql_slice_vector;
	result.sabot_sql_vector_reference_value = sabot_sql_vector_reference_value;
	result.sabot_sql_vector_reference_vector = sabot_sql_vector_reference_vector;
	result.sabot_sql_create_selection_vector = sabot_sql_create_selection_vector;
	result.sabot_sql_destroy_selection_vector = sabot_sql_destroy_selection_vector;
	result.sabot_sql_selection_vector_get_data_ptr = sabot_sql_selection_vector_get_data_ptr;
	result.sabot_sql_vector_copy_sel = sabot_sql_vector_copy_sel;
	return result;
}

#define SABOT_SQL_EXTENSION_API_VERSION_MAJOR  1
#define SABOT_SQL_EXTENSION_API_VERSION_MINOR  2
#define SABOT_SQL_EXTENSION_API_VERSION_PATCH  0
#define SABOT_SQL_EXTENSION_API_VERSION_STRING "v1.2.0"
