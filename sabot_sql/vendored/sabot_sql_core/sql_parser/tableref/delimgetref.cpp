#include "sabot_sql/parser/tableref/delimgetref.hpp"
#include "sabot_sql/common/serializer/deserializer.hpp"
#include "sabot_sql/common/serializer/serializer.hpp"

namespace sabot_sql {

string DelimGetRef::ToString() const {
	return "";
}

bool DelimGetRef::Equals(const TableRef &other) const {
	return TableRef::Equals(other);
}

unique_ptr<TableRef> DelimGetRef::Copy() {
	return make_uniq<DelimGetRef>(types);
}

void DelimGetRef::Serialize(Serializer &serializer) const {
	TableRef::Serialize(serializer);
	serializer.WriteProperty<vector<LogicalType>>(105, "chunk_types", types);
}

unique_ptr<TableRef> DelimGetRef::Deserialize(Deserializer &deserializer) {
	vector<LogicalType> types = deserializer.ReadProperty<vector<LogicalType>>(105, "chunk_types");
	auto result = sabot_sql::unique_ptr<DelimGetRef>(new DelimGetRef(types));
	return std::move(result);
}

} // namespace sabot_sql
