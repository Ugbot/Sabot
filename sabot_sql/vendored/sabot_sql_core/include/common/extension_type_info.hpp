#pragma once

#include "sabot_sql/common/string.hpp"
#include "sabot_sql/common/types/value.hpp"

namespace sabot_sql {

class Serializer;
class Deserializer;

struct LogicalTypeModifier {
public:
	explicit LogicalTypeModifier(Value value_p) : value(std::move(value_p)) {
	}
	string ToString() const {
		return label.empty() ? value.ToString() : label;
	}

public:
	Value value;
	string label;

	void Serialize(Serializer &serializer) const;
	static LogicalTypeModifier Deserialize(Deserializer &source);
};

struct ExtensionTypeInfo {
	vector<LogicalTypeModifier> modifiers;
	unordered_map<string, Value> properties;

public:
	void Serialize(Serializer &serializer) const;
	static unique_ptr<ExtensionTypeInfo> Deserialize(Deserializer &source);
	static bool Equals(optional_ptr<ExtensionTypeInfo> rhs, optional_ptr<ExtensionTypeInfo> lhs);
};

} // namespace sabot_sql
