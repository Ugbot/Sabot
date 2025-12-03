#include "sabot_sql/sql/duckdb_bridge.h"

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

} // namespace sql
} // namespace sabot_sql
