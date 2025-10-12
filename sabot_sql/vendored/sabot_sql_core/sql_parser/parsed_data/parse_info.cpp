#include "sabot_sql/parser/parsed_data/parse_info.hpp"
#include "sabot_sql/common/enums/catalog_type.hpp"
#include "sabot_sql/common/enum_util.hpp"
#include "sabot_sql/parser/keyword_helper.hpp"

namespace sabot_sql {

string ParseInfo::TypeToString(CatalogType type) {
	switch (type) {
	case CatalogType::TABLE_ENTRY:
		return "TABLE";
	case CatalogType::SCALAR_FUNCTION_ENTRY:
		return "FUNCTION";
	case CatalogType::INDEX_ENTRY:
		return "INDEX";
	case CatalogType::SCHEMA_ENTRY:
		return "SCHEMA";
	case CatalogType::TYPE_ENTRY:
		return "TYPE";
	case CatalogType::VIEW_ENTRY:
		return "VIEW";
	case CatalogType::SEQUENCE_ENTRY:
		return "SEQUENCE";
	case CatalogType::MACRO_ENTRY:
		return "MACRO";
	case CatalogType::TABLE_MACRO_ENTRY:
		return "MACRO TABLE";
	case CatalogType::SECRET_ENTRY:
		return "SECRET";
	default:
		throw InternalException("ParseInfo::TypeToString for CatalogType with type: %s not implemented",
		                        EnumUtil::ToString(type));
	}
}

string ParseInfo::QualifierToString(const string &catalog, const string &schema, const string &name) {
	string result;
	if (!catalog.empty()) {
		result += KeywordHelper::WriteOptionallyQuoted(catalog) + ".";
		if (!schema.empty()) {
			result += KeywordHelper::WriteOptionallyQuoted(schema) + ".";
		}
	} else if (!schema.empty() && schema != DEFAULT_SCHEMA) {
		result += KeywordHelper::WriteOptionallyQuoted(schema) + ".";
	}
	result += KeywordHelper::WriteOptionallyQuoted(name);
	return result;
}

} // namespace sabot_sql
