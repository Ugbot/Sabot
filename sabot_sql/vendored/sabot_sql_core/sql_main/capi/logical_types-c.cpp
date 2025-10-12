#include "sabot_sql/main/capi/capi_internal.hpp"
#include "sabot_sql/parser/parsed_data/create_type_info.hpp"
#include "sabot_sql/common/type_visitor.hpp"
#include "sabot_sql/common/helper.hpp"

namespace sabot_sql {

struct CCustomType {
	unique_ptr<LogicalType> base_type;
	string name;
};

} // namespace sabot_sql

static bool AssertLogicalTypeId(sabot_sql_logical_type type, sabot_sql::LogicalTypeId type_id) {
	if (!type) {
		return false;
	}
	auto &logical_type = *(reinterpret_cast<sabot_sql::LogicalType *>(type));
	if (logical_type.id() != type_id) {
		return false;
	}
	return true;
}

static bool AssertInternalType(sabot_sql_logical_type type, sabot_sql::PhysicalType physical_type) {
	if (!type) {
		return false;
	}
	auto &logical_type = *(reinterpret_cast<sabot_sql::LogicalType *>(type));
	if (logical_type.InternalType() != physical_type) {
		return false;
	}
	return true;
}

sabot_sql_logical_type sabot_sql_create_logical_type(sabot_sql_type type) {
	if (type == SABOT_SQL_TYPE_DECIMAL || type == SABOT_SQL_TYPE_ENUM || type == SABOT_SQL_TYPE_LIST ||
	    type == SABOT_SQL_TYPE_STRUCT || type == SABOT_SQL_TYPE_MAP || type == SABOT_SQL_TYPE_ARRAY ||
	    type == SABOT_SQL_TYPE_UNION) {
		type = SABOT_SQL_TYPE_INVALID;
	}

	auto type_id = sabot_sql::LogicalTypeIdFromC(type);
	return reinterpret_cast<sabot_sql_logical_type>(new sabot_sql::LogicalType(type_id));
}

sabot_sql_logical_type sabot_sql_create_list_type(sabot_sql_logical_type type) {
	if (!type) {
		return nullptr;
	}
	try {
		sabot_sql::LogicalType *logical_type =
		    new sabot_sql::LogicalType(sabot_sql::LogicalType::LIST(*reinterpret_cast<sabot_sql::LogicalType *>(type)));
		return reinterpret_cast<sabot_sql_logical_type>(logical_type);
	} catch (...) {
		return nullptr;
	}
}

sabot_sql_logical_type sabot_sql_create_array_type(sabot_sql_logical_type type, idx_t array_size) {
	if (!type) {
		return nullptr;
	}
	if (array_size >= sabot_sql::ArrayType::MAX_ARRAY_SIZE) {
		return nullptr;
	}
	try {
		sabot_sql::LogicalType *ltype = new sabot_sql::LogicalType(
		    sabot_sql::LogicalType::ARRAY(*reinterpret_cast<sabot_sql::LogicalType *>(type), array_size));
		return reinterpret_cast<sabot_sql_logical_type>(ltype);
	} catch (...) {
		return nullptr;
	}
}

sabot_sql_logical_type sabot_sql_create_union_type(sabot_sql_logical_type *member_types_p, const char **member_names,
                                             idx_t member_count) {
	if (!member_types_p || !member_names) {
		return nullptr;
	}
	sabot_sql::LogicalType **member_types = reinterpret_cast<sabot_sql::LogicalType **>(member_types_p);
	try {
		sabot_sql::child_list_t<sabot_sql::LogicalType> members;

		for (idx_t i = 0; i < member_count; i++) {
			members.push_back(make_pair(member_names[i], *member_types[i]));
		}
		sabot_sql::LogicalType *mtype = new sabot_sql::LogicalType(sabot_sql::LogicalType::UNION(members));
		return reinterpret_cast<sabot_sql_logical_type>(mtype);
	} catch (...) {
		return nullptr;
	}
}

sabot_sql_logical_type sabot_sql_create_struct_type(sabot_sql_logical_type *member_types_p, const char **member_names,
                                              idx_t member_count) {
	if (!member_types_p || !member_names) {
		return nullptr;
	}
	sabot_sql::LogicalType **member_types = (sabot_sql::LogicalType **)member_types_p;
	for (idx_t i = 0; i < member_count; i++) {
		if (!member_names[i] || !member_types[i]) {
			return nullptr;
		}
	}

	try {
		sabot_sql::child_list_t<sabot_sql::LogicalType> members;

		for (idx_t i = 0; i < member_count; i++) {
			members.push_back(make_pair(member_names[i], *member_types[i]));
		}
		sabot_sql::LogicalType *mtype = new sabot_sql::LogicalType(sabot_sql::LogicalType::STRUCT(members));
		return reinterpret_cast<sabot_sql_logical_type>(mtype);
	} catch (...) {
		return nullptr;
	}
}

sabot_sql_logical_type sabot_sql_create_enum_type(const char **member_names, idx_t member_count) {
	if (!member_names) {
		return nullptr;
	}
	sabot_sql::Vector enum_vector(sabot_sql::LogicalType::VARCHAR, member_count);
	auto enum_vector_ptr = sabot_sql::FlatVector::GetData<sabot_sql::string_t>(enum_vector);

	for (idx_t i = 0; i < member_count; i++) {
		if (!member_names[i]) {
			return nullptr;
		}
		enum_vector_ptr[i] = sabot_sql::StringVector::AddStringOrBlob(enum_vector, member_names[i]);
	}

	try {
		sabot_sql::LogicalType *mtype = new sabot_sql::LogicalType(sabot_sql::LogicalType::ENUM(enum_vector, member_count));
		return reinterpret_cast<sabot_sql_logical_type>(mtype);
	} catch (...) {
		return nullptr;
	}
}

sabot_sql_logical_type sabot_sql_create_map_type(sabot_sql_logical_type key_type, sabot_sql_logical_type value_type) {
	if (!key_type || !value_type) {
		return nullptr;
	}
	try {
		sabot_sql::LogicalType *mtype = new sabot_sql::LogicalType(sabot_sql::LogicalType::MAP(
		    *reinterpret_cast<sabot_sql::LogicalType *>(key_type), *reinterpret_cast<sabot_sql::LogicalType *>(value_type)));
		return reinterpret_cast<sabot_sql_logical_type>(mtype);
	} catch (...) {
		return nullptr;
	}
}

sabot_sql_logical_type sabot_sql_create_decimal_type(uint8_t width, uint8_t scale) {
	return reinterpret_cast<sabot_sql_logical_type>(new sabot_sql::LogicalType(sabot_sql::LogicalType::DECIMAL(width, scale)));
}

sabot_sql_type sabot_sql_get_type_id(sabot_sql_logical_type type) {
	if (!type) {
		return SABOT_SQL_TYPE_INVALID;
	}
	auto logical_type = reinterpret_cast<sabot_sql::LogicalType *>(type);
	return sabot_sql::LogicalTypeIdToC(logical_type->id());
}

void sabot_sql_destroy_logical_type(sabot_sql_logical_type *type) {
	if (type && *type) {
		auto logical_type = reinterpret_cast<sabot_sql::LogicalType *>(*type);
		delete logical_type;
		*type = nullptr;
	}
}

uint8_t sabot_sql_decimal_width(sabot_sql_logical_type type) {
	if (!AssertLogicalTypeId(type, sabot_sql::LogicalTypeId::DECIMAL)) {
		return 0;
	}
	auto &logical_type = *(reinterpret_cast<sabot_sql::LogicalType *>(type));
	return sabot_sql::DecimalType::GetWidth(logical_type);
}

uint8_t sabot_sql_decimal_scale(sabot_sql_logical_type type) {
	if (!AssertLogicalTypeId(type, sabot_sql::LogicalTypeId::DECIMAL)) {
		return 0;
	}
	auto &logical_type = *(reinterpret_cast<sabot_sql::LogicalType *>(type));
	return sabot_sql::DecimalType::GetScale(logical_type);
}

sabot_sql_type sabot_sql_decimal_internal_type(sabot_sql_logical_type type) {
	if (!AssertLogicalTypeId(type, sabot_sql::LogicalTypeId::DECIMAL)) {
		return SABOT_SQL_TYPE_INVALID;
	}
	auto &logical_type = *(reinterpret_cast<sabot_sql::LogicalType *>(type));
	switch (logical_type.InternalType()) {
	case sabot_sql::PhysicalType::INT16:
		return SABOT_SQL_TYPE_SMALLINT;
	case sabot_sql::PhysicalType::INT32:
		return SABOT_SQL_TYPE_INTEGER;
	case sabot_sql::PhysicalType::INT64:
		return SABOT_SQL_TYPE_BIGINT;
	case sabot_sql::PhysicalType::INT128:
		return SABOT_SQL_TYPE_HUGEINT;
	default:
		return SABOT_SQL_TYPE_INVALID;
	}
}

sabot_sql_type sabot_sql_enum_internal_type(sabot_sql_logical_type type) {
	if (!AssertLogicalTypeId(type, sabot_sql::LogicalTypeId::ENUM)) {
		return SABOT_SQL_TYPE_INVALID;
	}
	auto &logical_type = *(reinterpret_cast<sabot_sql::LogicalType *>(type));
	switch (logical_type.InternalType()) {
	case sabot_sql::PhysicalType::UINT8:
		return SABOT_SQL_TYPE_UTINYINT;
	case sabot_sql::PhysicalType::UINT16:
		return SABOT_SQL_TYPE_USMALLINT;
	case sabot_sql::PhysicalType::UINT32:
		return SABOT_SQL_TYPE_UINTEGER;
	default:
		return SABOT_SQL_TYPE_INVALID;
	}
}

uint32_t sabot_sql_enum_dictionary_size(sabot_sql_logical_type type) {
	if (!AssertLogicalTypeId(type, sabot_sql::LogicalTypeId::ENUM)) {
		return 0;
	}
	auto &logical_type = *(reinterpret_cast<sabot_sql::LogicalType *>(type));
	return sabot_sql::NumericCast<uint32_t>(sabot_sql::EnumType::GetSize(logical_type));
}

char *sabot_sql_enum_dictionary_value(sabot_sql_logical_type type, idx_t index) {
	if (!AssertLogicalTypeId(type, sabot_sql::LogicalTypeId::ENUM)) {
		return nullptr;
	}
	auto &logical_type = *(reinterpret_cast<sabot_sql::LogicalType *>(type));
	auto &vector = sabot_sql::EnumType::GetValuesInsertOrder(logical_type);
	auto value = vector.GetValue(index);
	return strdup(sabot_sql::StringValue::Get(value).c_str());
}

sabot_sql_logical_type sabot_sql_list_type_child_type(sabot_sql_logical_type type) {
	if (!AssertLogicalTypeId(type, sabot_sql::LogicalTypeId::LIST) &&
	    !AssertLogicalTypeId(type, sabot_sql::LogicalTypeId::MAP)) {
		return nullptr;
	}
	auto &logical_type = *(reinterpret_cast<sabot_sql::LogicalType *>(type));
	if (logical_type.id() != sabot_sql::LogicalTypeId::LIST && logical_type.id() != sabot_sql::LogicalTypeId::MAP) {
		return nullptr;
	}
	return reinterpret_cast<sabot_sql_logical_type>(new sabot_sql::LogicalType(sabot_sql::ListType::GetChildType(logical_type)));
}

sabot_sql_logical_type sabot_sql_array_type_child_type(sabot_sql_logical_type type) {
	if (!AssertLogicalTypeId(type, sabot_sql::LogicalTypeId::ARRAY)) {
		return nullptr;
	}
	auto &logical_type = *(reinterpret_cast<sabot_sql::LogicalType *>(type));
	if (logical_type.id() != sabot_sql::LogicalTypeId::ARRAY) {
		return nullptr;
	}
	return reinterpret_cast<sabot_sql_logical_type>(
	    new sabot_sql::LogicalType(sabot_sql::ArrayType::GetChildType(logical_type)));
}

idx_t sabot_sql_array_type_array_size(sabot_sql_logical_type type) {
	if (!AssertLogicalTypeId(type, sabot_sql::LogicalTypeId::ARRAY)) {
		return 0;
	}
	auto &logical_type = *(reinterpret_cast<sabot_sql::LogicalType *>(type));
	if (logical_type.id() != sabot_sql::LogicalTypeId::ARRAY) {
		return 0;
	}
	return sabot_sql::ArrayType::GetSize(logical_type);
}

sabot_sql_logical_type sabot_sql_map_type_key_type(sabot_sql_logical_type type) {
	if (!AssertLogicalTypeId(type, sabot_sql::LogicalTypeId::MAP)) {
		return nullptr;
	}
	auto &mtype = *(reinterpret_cast<sabot_sql::LogicalType *>(type));
	if (mtype.id() != sabot_sql::LogicalTypeId::MAP) {
		return nullptr;
	}
	return reinterpret_cast<sabot_sql_logical_type>(new sabot_sql::LogicalType(sabot_sql::MapType::KeyType(mtype)));
}

sabot_sql_logical_type sabot_sql_map_type_value_type(sabot_sql_logical_type type) {
	if (!AssertLogicalTypeId(type, sabot_sql::LogicalTypeId::MAP)) {
		return nullptr;
	}
	auto &mtype = *(reinterpret_cast<sabot_sql::LogicalType *>(type));
	if (mtype.id() != sabot_sql::LogicalTypeId::MAP) {
		return nullptr;
	}
	return reinterpret_cast<sabot_sql_logical_type>(new sabot_sql::LogicalType(sabot_sql::MapType::ValueType(mtype)));
}

idx_t sabot_sql_struct_type_child_count(sabot_sql_logical_type type) {
	if (!AssertInternalType(type, sabot_sql::PhysicalType::STRUCT)) {
		return 0;
	}
	auto &logical_type = *(reinterpret_cast<sabot_sql::LogicalType *>(type));
	return sabot_sql::StructType::GetChildCount(logical_type);
}

idx_t sabot_sql_union_type_member_count(sabot_sql_logical_type type) {
	if (!AssertLogicalTypeId(type, sabot_sql::LogicalTypeId::UNION)) {
		return 0;
	}
	idx_t member_count = sabot_sql_struct_type_child_count(type);
	if (member_count != 0) {
		member_count--;
	}
	return member_count;
}

char *sabot_sql_union_type_member_name(sabot_sql_logical_type type, idx_t index) {
	if (!AssertInternalType(type, sabot_sql::PhysicalType::STRUCT)) {
		return nullptr;
	}
	if (!AssertLogicalTypeId(type, sabot_sql::LogicalTypeId::UNION)) {
		return nullptr;
	}
	auto &logical_type = *(reinterpret_cast<sabot_sql::LogicalType *>(type));
	return strdup(sabot_sql::UnionType::GetMemberName(logical_type, index).c_str());
}

sabot_sql_logical_type sabot_sql_union_type_member_type(sabot_sql_logical_type type, idx_t index) {
	if (!AssertInternalType(type, sabot_sql::PhysicalType::STRUCT)) {
		return nullptr;
	}
	if (!AssertLogicalTypeId(type, sabot_sql::LogicalTypeId::UNION)) {
		return nullptr;
	}
	auto &logical_type = *(reinterpret_cast<sabot_sql::LogicalType *>(type));
	return reinterpret_cast<sabot_sql_logical_type>(
	    new sabot_sql::LogicalType(sabot_sql::UnionType::GetMemberType(logical_type, index)));
}

char *sabot_sql_struct_type_child_name(sabot_sql_logical_type type, idx_t index) {
	if (!AssertInternalType(type, sabot_sql::PhysicalType::STRUCT)) {
		return nullptr;
	}
	auto &logical_type = *(reinterpret_cast<sabot_sql::LogicalType *>(type));
	return strdup(sabot_sql::StructType::GetChildName(logical_type, index).c_str());
}

char *sabot_sql_logical_type_get_alias(sabot_sql_logical_type type) {
	auto &logical_type = *(reinterpret_cast<sabot_sql::LogicalType *>(type));
	return logical_type.HasAlias() ? strdup(logical_type.GetAlias().c_str()) : nullptr;
}

void sabot_sql_logical_type_set_alias(sabot_sql_logical_type type, const char *alias) {
	auto &logical_type = *(reinterpret_cast<sabot_sql::LogicalType *>(type));
	logical_type.SetAlias(alias);
}

sabot_sql_logical_type sabot_sql_struct_type_child_type(sabot_sql_logical_type type, idx_t index) {
	if (!AssertInternalType(type, sabot_sql::PhysicalType::STRUCT)) {
		return nullptr;
	}
	auto &logical_type = *(reinterpret_cast<sabot_sql::LogicalType *>(type));
	if (logical_type.InternalType() != sabot_sql::PhysicalType::STRUCT) {
		return nullptr;
	}
	return reinterpret_cast<sabot_sql_logical_type>(
	    new sabot_sql::LogicalType(sabot_sql::StructType::GetChildType(logical_type, index)));
}

sabot_sql_state sabot_sql_register_logical_type(sabot_sql_connection connection, sabot_sql_logical_type type,
                                          sabot_sql_create_type_info info) {
	if (!connection || !type) {
		return SabotSQLError;
	}

	// Unused for now
	(void)info;

	const auto &base_type = *reinterpret_cast<sabot_sql::LogicalType *>(type);
	if (!base_type.HasAlias()) {
		return SabotSQLError;
	}

	if (sabot_sql::TypeVisitor::Contains(base_type, sabot_sql::LogicalTypeId::INVALID) ||
	    sabot_sql::TypeVisitor::Contains(base_type, sabot_sql::LogicalTypeId::ANY)) {
		return SabotSQLError;
	}

	try {
		const auto con = reinterpret_cast<sabot_sql::Connection *>(connection);
		con->context->RunFunctionInTransaction([&]() {
			auto &catalog = sabot_sql::Catalog::GetSystemCatalog(*con->context);
			sabot_sql::CreateTypeInfo info(base_type.GetAlias(), base_type);
			info.temporary = true;
			info.internal = true;
			catalog.CreateType(*con->context, info);
		});
	} catch (...) {
		return SabotSQLError;
	}
	return SabotSQLSuccess;
}
