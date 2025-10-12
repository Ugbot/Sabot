#include "sabot_sql/parser/tableref/table_function_ref.hpp"
#include "sabot_sql/common/vector.hpp"
#include "sabot_sql/common/serializer/serializer.hpp"
#include "sabot_sql/common/serializer/deserializer.hpp"

namespace sabot_sql {

TableFunctionRef::TableFunctionRef() : TableRef(TableReferenceType::TABLE_FUNCTION) {
}

string TableFunctionRef::ToString() const {
	auto result = function->ToString();
	if (with_ordinality == OrdinalityType::WITH_ORDINALITY) {
		result += " WITH ORDINALITY";
	}
	return BaseToString(result, column_name_alias);
}

bool TableFunctionRef::Equals(const TableRef &other_p) const {
	if (!TableRef::Equals(other_p)) {
		return false;
	}
	auto &other = other_p.Cast<TableFunctionRef>();
	return function->Equals(*other.function);
}

unique_ptr<TableRef> TableFunctionRef::Copy() {
	auto copy = make_uniq<TableFunctionRef>();

	copy->function = function->Copy();
	copy->column_name_alias = column_name_alias;
	copy->with_ordinality = with_ordinality;
	CopyProperties(*copy);

	return std::move(copy);
}

} // namespace sabot_sql
