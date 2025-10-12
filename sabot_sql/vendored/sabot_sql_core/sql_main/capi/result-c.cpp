#include "sabot_sql/main/capi/capi_internal.hpp"
#include "sabot_sql/common/types/timestamp.hpp"
#include "sabot_sql/common/allocator.hpp"

namespace sabot_sql {

struct CBaseConverter {
	template <class DST>
	static void NullConvert(DST &target) {
	}
};

struct CStandardConverter : public CBaseConverter {
	template <class SRC, class DST>
	static DST Convert(SRC input) {
		return input;
	}
};

struct CStringConverter {
	template <class SRC, class DST>
	static DST Convert(SRC input) {
		auto result = char_ptr_cast(sabot_sql_malloc(input.GetSize() + 1));
		assert(result);
		memcpy((void *)result, input.GetData(), input.GetSize());
		auto write_arr = char_ptr_cast(result);
		write_arr[input.GetSize()] = '\0';
		return result;
	}

	template <class DST>
	static void NullConvert(DST &target) {
		target = nullptr;
	}
};

struct CBlobConverter {
	template <class SRC, class DST>
	static DST Convert(SRC input) {
		sabot_sql_blob result;
		result.data = char_ptr_cast(sabot_sql_malloc(input.GetSize()));
		result.size = input.GetSize();
		assert(result.data);
		memcpy(result.data, input.GetData(), input.GetSize());
		return result;
	}

	template <class DST>
	static void NullConvert(DST &target) {
		target.data = nullptr;
		target.size = 0;
	}
};

struct CTimestampMsConverter : public CBaseConverter {
	template <class SRC, class DST>
	static DST Convert(SRC input) {
		if (!Timestamp::IsFinite(input)) {
			return input;
		}
		return Timestamp::FromEpochMs(input.value);
	}
};

struct CTimestampNsConverter : public CBaseConverter {
	template <class SRC, class DST>
	static DST Convert(SRC input) {
		if (!Timestamp::IsFinite(input)) {
			return input;
		}
		return Timestamp::FromEpochNanoSeconds(input.value);
	}
};

struct CTimestampSecConverter : public CBaseConverter {
	template <class SRC, class DST>
	static DST Convert(SRC input) {
		if (!Timestamp::IsFinite(input)) {
			return input;
		}
		return Timestamp::FromEpochSeconds(input.value);
	}
};

struct CHugeintConverter : public CBaseConverter {
	template <class SRC, class DST>
	static DST Convert(SRC input) {
		sabot_sql_hugeint result;
		result.lower = input.lower;
		result.upper = input.upper;
		return result;
	}
};

struct CUhugeintConverter : public CBaseConverter {
	template <class SRC, class DST>
	static DST Convert(SRC input) {
		sabot_sql_uhugeint result;
		result.lower = input.lower;
		result.upper = input.upper;
		return result;
	}
};

struct CIntervalConverter : public CBaseConverter {
	template <class SRC, class DST>
	static DST Convert(SRC input) {
		sabot_sql_interval result;
		result.days = input.days;
		result.months = input.months;
		result.micros = input.micros;
		return result;
	}
};

template <class T>
struct CDecimalConverter : public CBaseConverter {
	template <class SRC, class DST>
	static DST Convert(SRC input) {
		sabot_sql_hugeint result;
		result.lower = static_cast<uint64_t>(input);
		result.upper = 0;
		return result;
	}
};

template <class SRC, class DST = SRC, class OP = CStandardConverter>
void WriteData(sabot_sql_column *column, ColumnDataCollection &source, const vector<column_t> &column_ids) {
	idx_t row = 0;
	auto target = (DST *)column->deprecated_data;
	for (auto &input : source.Chunks(column_ids)) {
		auto source = FlatVector::GetData<SRC>(input.data[0]);
		auto &mask = FlatVector::Validity(input.data[0]);

		for (idx_t k = 0; k < input.size(); k++, row++) {
			if (!mask.RowIsValid(k)) {
				OP::template NullConvert<DST>(target[row]);
			} else {
				target[row] = OP::template Convert<SRC, DST>(source[k]);
			}
		}
	}
}

sabot_sql_state deprecated_sabot_sql_translate_column(MaterializedQueryResult &result, sabot_sql_column *column, idx_t col) {
	D_ASSERT(!result.HasError());
	auto &collection = result.Collection();
	idx_t row_count = collection.Count();
	column->deprecated_nullmask = (bool *)sabot_sql_malloc(sizeof(bool) * collection.Count());

	auto type_size = GetCTypeSize(column->deprecated_type);
	if (type_size == 0) {
		for (idx_t row_id = 0; row_id < row_count; row_id++) {
			column->deprecated_nullmask[row_id] = false;
		}
		// Unsupported type, e.g., a LIST. By returning SabotSQLSuccess here,
		// we allow filling other columns, and return NULL for all unsupported types.
		return SabotSQLSuccess;
	}

	column->deprecated_data = sabot_sql_malloc(type_size * row_count);
	if (!column->deprecated_nullmask || !column->deprecated_data) { // LCOV_EXCL_START
		// malloc failure
		return SabotSQLError;
	} // LCOV_EXCL_STOP

	vector<column_t> column_ids {col};
	// first convert the nullmask
	{
		idx_t row = 0;
		for (auto &input : collection.Chunks(column_ids)) {
			for (idx_t k = 0; k < input.size(); k++) {
				column->deprecated_nullmask[row++] = FlatVector::IsNull(input.data[0], k);
			}
		}
	}
	// then write the data
	switch (result.types[col].id()) {
	case LogicalTypeId::BOOLEAN:
		WriteData<bool>(column, collection, column_ids);
		break;
	case LogicalTypeId::TINYINT:
		WriteData<int8_t>(column, collection, column_ids);
		break;
	case LogicalTypeId::SMALLINT:
		WriteData<int16_t>(column, collection, column_ids);
		break;
	case LogicalTypeId::INTEGER:
		WriteData<int32_t>(column, collection, column_ids);
		break;
	case LogicalTypeId::BIGINT:
		WriteData<int64_t>(column, collection, column_ids);
		break;
	case LogicalTypeId::UTINYINT:
		WriteData<uint8_t>(column, collection, column_ids);
		break;
	case LogicalTypeId::USMALLINT:
		WriteData<uint16_t>(column, collection, column_ids);
		break;
	case LogicalTypeId::UINTEGER:
		WriteData<uint32_t>(column, collection, column_ids);
		break;
	case LogicalTypeId::UBIGINT:
		WriteData<uint64_t>(column, collection, column_ids);
		break;
	case LogicalTypeId::FLOAT:
		WriteData<float>(column, collection, column_ids);
		break;
	case LogicalTypeId::DOUBLE:
		WriteData<double>(column, collection, column_ids);
		break;
	case LogicalTypeId::DATE:
		WriteData<date_t>(column, collection, column_ids);
		break;
	case LogicalTypeId::TIME:
		WriteData<dtime_t>(column, collection, column_ids);
		break;
	case LogicalTypeId::TIME_TZ:
		WriteData<dtime_tz_t>(column, collection, column_ids);
		break;
	case LogicalTypeId::TIMESTAMP:
	case LogicalTypeId::TIMESTAMP_TZ:
		WriteData<timestamp_t>(column, collection, column_ids);
		break;
	case LogicalTypeId::VARCHAR: {
		WriteData<string_t, const char *, CStringConverter>(column, collection, column_ids);
		break;
	}
	case LogicalTypeId::BLOB: {
		WriteData<string_t, sabot_sql_blob, CBlobConverter>(column, collection, column_ids);
		break;
	}
	case LogicalTypeId::TIMESTAMP_NS: {
		WriteData<timestamp_t, timestamp_t, CTimestampNsConverter>(column, collection, column_ids);
		break;
	}
	case LogicalTypeId::TIMESTAMP_MS: {
		WriteData<timestamp_t, timestamp_t, CTimestampMsConverter>(column, collection, column_ids);
		break;
	}
	case LogicalTypeId::TIMESTAMP_SEC: {
		WriteData<timestamp_t, timestamp_t, CTimestampSecConverter>(column, collection, column_ids);
		break;
	}
	case LogicalTypeId::HUGEINT: {
		WriteData<hugeint_t, sabot_sql_hugeint, CHugeintConverter>(column, collection, column_ids);
		break;
	}
	case LogicalTypeId::UHUGEINT: {
		WriteData<uhugeint_t, sabot_sql_uhugeint, CUhugeintConverter>(column, collection, column_ids);
		break;
	}
	case LogicalTypeId::INTERVAL: {
		WriteData<interval_t, sabot_sql_interval, CIntervalConverter>(column, collection, column_ids);
		break;
	}
	case LogicalTypeId::DECIMAL: {
		// get data
		switch (result.types[col].InternalType()) {
		case PhysicalType::INT16: {
			WriteData<int16_t, sabot_sql_hugeint, CDecimalConverter<int16_t>>(column, collection, column_ids);
			break;
		}
		case PhysicalType::INT32: {
			WriteData<int32_t, sabot_sql_hugeint, CDecimalConverter<int32_t>>(column, collection, column_ids);
			break;
		}
		case PhysicalType::INT64: {
			WriteData<int64_t, sabot_sql_hugeint, CDecimalConverter<int64_t>>(column, collection, column_ids);
			break;
		}
		case PhysicalType::INT128: {
			WriteData<hugeint_t, sabot_sql_hugeint, CHugeintConverter>(column, collection, column_ids);
			break;
		}
		default:
			throw std::runtime_error("Unsupported physical type for Decimal" +
			                         TypeIdToString(result.types[col].InternalType()));
		}
		break;
	}
	default: // LCOV_EXCL_START
		return SabotSQLError;
	} // LCOV_EXCL_STOP
	return SabotSQLSuccess;
}

sabot_sql_state SabotSQLTranslateResult(unique_ptr<QueryResult> result_p, sabot_sql_result *out) {
	auto &result = *result_p;
	D_ASSERT(result_p);
	if (!out) {
		// no result to write to, only return the status
		return !result.HasError() ? SabotSQLSuccess : SabotSQLError;
	}

	memset(out, 0, sizeof(sabot_sql_result));

	// initialize the result_data object
	auto result_data = new SabotSQLResultData();
	result_data->result = std::move(result_p);
	result_data->result_set_type = CAPIResultSetType::CAPI_RESULT_TYPE_NONE;
	out->internal_data = result_data;

	if (result.HasError()) {
		// write the error message
		out->deprecated_error_message = (char *)result.GetError().c_str(); // NOLINT
		return SabotSQLError;
	}
	// copy the data
	// first write the metadata
	out->deprecated_column_count = result.ColumnCount();
	out->deprecated_rows_changed = 0;
	return SabotSQLSuccess;
}

bool DeprecatedMaterializeResult(sabot_sql_result *result) {
	if (!result) {
		return false;
	}
	auto result_data = reinterpret_cast<sabot_sql::SabotSQLResultData *>(result->internal_data);
	if (result_data->result->HasError()) {
		return false;
	}
	if (result_data->result_set_type == CAPIResultSetType::CAPI_RESULT_TYPE_DEPRECATED) {
		// already materialized into deprecated result format
		return true;
	}
	if (result_data->result_set_type == CAPIResultSetType::CAPI_RESULT_TYPE_MATERIALIZED) {
		// already used as a new result set
		return false;
	}
	if (result_data->result_set_type == CAPIResultSetType::CAPI_RESULT_TYPE_STREAMING) {
		// already used as a streaming result
		return false;
	}
	// materialize as deprecated result set
	result_data->result_set_type = CAPIResultSetType::CAPI_RESULT_TYPE_DEPRECATED;
	auto column_count = result_data->result->ColumnCount();
	result->deprecated_columns = (sabot_sql_column *)sabot_sql_malloc(sizeof(sabot_sql_column) * column_count);
	if (!result->deprecated_columns) { // LCOV_EXCL_START
		// malloc failure
		return SabotSQLError;
	} // LCOV_EXCL_STOP

	if (result_data->result->type == QueryResultType::STREAM_RESULT) {
		// if we are dealing with a stream result, convert it to a materialized result first
		auto &stream_result = (StreamQueryResult &)*result_data->result;
		result_data->result = stream_result.Materialize();
	}
	D_ASSERT(result_data->result->type == QueryResultType::MATERIALIZED_RESULT);
	auto &materialized = reinterpret_cast<MaterializedQueryResult &>(*result_data->result);

	// convert the result to a materialized result
	// zero initialize the columns (so we can cleanly delete it in case a malloc fails)
	memset(result->deprecated_columns, 0, sizeof(sabot_sql_column) * column_count);
	for (idx_t i = 0; i < column_count; i++) {
		result->deprecated_columns[i].deprecated_type = LogicalTypeIdToC(result_data->result->types[i].id());
		result->deprecated_columns[i].deprecated_name = (char *)result_data->result->names[i].c_str(); // NOLINT
	}

	result->deprecated_row_count = materialized.RowCount();
	if (result->deprecated_row_count > 0 && materialized.properties.return_type == StatementReturnType::CHANGED_ROWS) {
		// update total changes
		auto row_changes = materialized.GetValue(0, 0);
		if (!row_changes.IsNull() && row_changes.DefaultTryCastAs(LogicalType::BIGINT)) {
			result->deprecated_rows_changed = NumericCast<idx_t>(row_changes.GetValue<int64_t>());
		}
	}

	// Now write the data and skip any unsupported columns.
	for (idx_t col = 0; col < column_count; col++) {
		auto state = deprecated_sabot_sql_translate_column(materialized, &result->deprecated_columns[col], col);
		if (state != SabotSQLSuccess) {
			return false;
		}
	}
	return true;
}

} // namespace sabot_sql

static void DuckdbDestroyColumn(sabot_sql_column column, idx_t count) {
	if (column.deprecated_data) {
		if (column.deprecated_type == SABOT_SQL_TYPE_VARCHAR) {
			// varchar, delete individual strings
			auto data = reinterpret_cast<char **>(column.deprecated_data);
			for (idx_t i = 0; i < count; i++) {
				if (data[i]) {
					sabot_sql_free(data[i]);
				}
			}
		} else if (column.deprecated_type == SABOT_SQL_TYPE_BLOB) {
			// blob, delete individual blobs
			auto data = reinterpret_cast<sabot_sql_blob *>(column.deprecated_data);
			for (idx_t i = 0; i < count; i++) {
				if (data[i].data) {
					sabot_sql_free((void *)data[i].data);
				}
			}
		}
		sabot_sql_free(column.deprecated_data);
	}
	if (column.deprecated_nullmask) {
		sabot_sql_free(column.deprecated_nullmask);
	}
}

void sabot_sql_destroy_result(sabot_sql_result *result) {
	if (result->deprecated_columns) {
		for (idx_t i = 0; i < result->deprecated_column_count; i++) {
			DuckdbDestroyColumn(result->deprecated_columns[i], result->deprecated_row_count);
		}
		sabot_sql_free(result->deprecated_columns);
	}
	if (result->internal_data) {
		auto result_data = reinterpret_cast<sabot_sql::SabotSQLResultData *>(result->internal_data);
		delete result_data;
	}
	memset(result, 0, sizeof(sabot_sql_result));
}

const char *sabot_sql_column_name(sabot_sql_result *result, idx_t col) {
	if (!result || col >= sabot_sql_column_count(result)) {
		return nullptr;
	}
	auto &result_data = *(reinterpret_cast<sabot_sql::SabotSQLResultData *>(result->internal_data));
	return result_data.result->names[col].c_str();
}

sabot_sql_type sabot_sql_column_type(sabot_sql_result *result, idx_t col) {
	if (!result || col >= sabot_sql_column_count(result)) {
		return SABOT_SQL_TYPE_INVALID;
	}
	auto &result_data = *(reinterpret_cast<sabot_sql::SabotSQLResultData *>(result->internal_data));
	return sabot_sql::LogicalTypeIdToC(result_data.result->types[col].id());
}

sabot_sql_logical_type sabot_sql_column_logical_type(sabot_sql_result *result, idx_t col) {
	if (!result || col >= sabot_sql_column_count(result)) {
		return nullptr;
	}
	auto &result_data = *(reinterpret_cast<sabot_sql::SabotSQLResultData *>(result->internal_data));
	return reinterpret_cast<sabot_sql_logical_type>(new sabot_sql::LogicalType(result_data.result->types[col]));
}

sabot_sql_arrow_options sabot_sql_result_get_arrow_options(sabot_sql_result *result) {
	if (!result) {
		return nullptr;
	}
	auto &result_data = *(reinterpret_cast<sabot_sql::SabotSQLResultData *>(result->internal_data));
	if (!result_data.result) {
		return nullptr;
	}
	auto arrow_options_wrapper = new sabot_sql::CClientArrowOptionsWrapper(result_data.result->client_properties);
	return reinterpret_cast<sabot_sql_arrow_options>(arrow_options_wrapper);
}

idx_t sabot_sql_column_count(sabot_sql_result *result) {
	if (!result) {
		return 0;
	}
	if (result->internal_data == NULL) {
		return 0;
	}
	auto &result_data = *(reinterpret_cast<sabot_sql::SabotSQLResultData *>(result->internal_data));
	return result_data.result->ColumnCount();
}

idx_t sabot_sql_row_count(sabot_sql_result *result) {
	if (!result) {
		return 0;
	}
	auto &result_data = *(reinterpret_cast<sabot_sql::SabotSQLResultData *>(result->internal_data));
	if (result_data.result->type == sabot_sql::QueryResultType::STREAM_RESULT) {
		// We can't know the row count beforehand
		return 0;
	}
	auto &materialized = reinterpret_cast<sabot_sql::MaterializedQueryResult &>(*result_data.result);
	return materialized.RowCount();
}

idx_t sabot_sql_rows_changed(sabot_sql_result *result) {
	if (!result) {
		return 0;
	}
	auto &result_data = *(reinterpret_cast<sabot_sql::SabotSQLResultData *>(result->internal_data));
	if (result_data.result_set_type == sabot_sql::CAPIResultSetType::CAPI_RESULT_TYPE_DEPRECATED) {
		// not a materialized result
		return result->deprecated_rows_changed;
	}
	auto &materialized = reinterpret_cast<sabot_sql::MaterializedQueryResult &>(*result_data.result);
	if (materialized.properties.return_type != sabot_sql::StatementReturnType::CHANGED_ROWS) {
		// we can only use this function for CHANGED_ROWS result types
		return 0;
	}
	if (materialized.RowCount() != 1 || materialized.ColumnCount() != 1) {
		// CHANGED_ROWS should return exactly one row
		return 0;
	}
	return materialized.GetValue(0, 0).GetValue<uint64_t>();
}

void *sabot_sql_column_data(sabot_sql_result *result, idx_t col) {
	if (!result || col >= result->deprecated_column_count) {
		return nullptr;
	}
	if (!sabot_sql::DeprecatedMaterializeResult(result)) {
		return nullptr;
	}
	return result->deprecated_columns[col].deprecated_data;
}

bool *sabot_sql_nullmask_data(sabot_sql_result *result, idx_t col) {
	if (!result || col >= result->deprecated_column_count) {
		return nullptr;
	}
	if (!sabot_sql::DeprecatedMaterializeResult(result)) {
		return nullptr;
	}
	return result->deprecated_columns[col].deprecated_nullmask;
}

const char *sabot_sql_result_error(sabot_sql_result *result) {
	if (!result || !result->internal_data) {
		return nullptr;
	}
	auto &result_data = *(reinterpret_cast<sabot_sql::SabotSQLResultData *>(result->internal_data));
	return !result_data.result->HasError() ? nullptr : result_data.result->GetError().c_str();
}

sabot_sql_error_type sabot_sql_result_error_type(sabot_sql_result *result) {
	if (!result || !result->internal_data) {
		return SABOT_SQL_ERROR_INVALID;
	}
	auto &result_data = *(reinterpret_cast<sabot_sql::SabotSQLResultData *>(result->internal_data));
	if (!result_data.result->HasError()) {
		return SABOT_SQL_ERROR_INVALID;
	}
	return sabot_sql::ErrorTypeToC(result_data.result->GetErrorType());
}

idx_t sabot_sql_result_chunk_count(sabot_sql_result result) {
	if (!result.internal_data) {
		return 0;
	}
	auto &result_data = *(reinterpret_cast<sabot_sql::SabotSQLResultData *>(result.internal_data));
	if (result_data.result_set_type == sabot_sql::CAPIResultSetType::CAPI_RESULT_TYPE_DEPRECATED) {
		return 0;
	}
	if (result_data.result->type != sabot_sql::QueryResultType::MATERIALIZED_RESULT) {
		// Can't know beforehand how many chunks are returned.
		return 0;
	}
	auto &materialized = reinterpret_cast<sabot_sql::MaterializedQueryResult &>(*result_data.result);
	return materialized.Collection().ChunkCount();
}

sabot_sql_data_chunk sabot_sql_result_get_chunk(sabot_sql_result result, idx_t chunk_idx) {
	if (!result.internal_data) {
		return nullptr;
	}
	auto &result_data = *(reinterpret_cast<sabot_sql::SabotSQLResultData *>(result.internal_data));
	if (result_data.result_set_type == sabot_sql::CAPIResultSetType::CAPI_RESULT_TYPE_DEPRECATED) {
		return nullptr;
	}
	if (result_data.result->type != sabot_sql::QueryResultType::MATERIALIZED_RESULT) {
		// This API is only supported for materialized query results
		return nullptr;
	}
	result_data.result_set_type = sabot_sql::CAPIResultSetType::CAPI_RESULT_TYPE_MATERIALIZED;
	auto &materialized = reinterpret_cast<sabot_sql::MaterializedQueryResult &>(*result_data.result);
	auto &collection = materialized.Collection();
	if (chunk_idx >= collection.ChunkCount()) {
		return nullptr;
	}
	auto chunk = sabot_sql::make_uniq<sabot_sql::DataChunk>();
	chunk->Initialize(sabot_sql::Allocator::DefaultAllocator(), collection.Types());
	collection.FetchChunk(chunk_idx, *chunk);
	return reinterpret_cast<sabot_sql_data_chunk>(chunk.release());
}

bool sabot_sql_result_is_streaming(sabot_sql_result result) {
	if (!result.internal_data) {
		return false;
	}
	if (sabot_sql_result_error(&result) != nullptr) {
		return false;
	}
	auto &result_data = *(reinterpret_cast<sabot_sql::SabotSQLResultData *>(result.internal_data));
	return result_data.result->type == sabot_sql::QueryResultType::STREAM_RESULT;
}

sabot_sql_result_type sabot_sql_result_return_type(sabot_sql_result result) {
	if (!result.internal_data || sabot_sql_result_error(&result) != nullptr) {
		return SABOT_SQL_RESULT_TYPE_INVALID;
	}
	auto &result_data = *(reinterpret_cast<sabot_sql::SabotSQLResultData *>(result.internal_data));
	switch (result_data.result->properties.return_type) {
	case sabot_sql::StatementReturnType::CHANGED_ROWS:
		return SABOT_SQL_RESULT_TYPE_CHANGED_ROWS;
	case sabot_sql::StatementReturnType::NOTHING:
		return SABOT_SQL_RESULT_TYPE_NOTHING;
	case sabot_sql::StatementReturnType::QUERY_RESULT:
		return SABOT_SQL_RESULT_TYPE_QUERY_RESULT;
	default:
		return SABOT_SQL_RESULT_TYPE_INVALID;
	}
}

sabot_sql_statement_type sabot_sql_result_statement_type(sabot_sql_result result) {
	if (!result.internal_data || sabot_sql_result_error(&result) != nullptr) {
		return SABOT_SQL_STATEMENT_TYPE_INVALID;
	}
	auto &pres = *(reinterpret_cast<sabot_sql::SabotSQLResultData *>(result.internal_data));

	return StatementTypeToC(pres.result->statement_type);
}
