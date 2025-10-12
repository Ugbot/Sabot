//===----------------------------------------------------------------------===//
//                         SabotSQL
//
// sabot_sql/parser/parsed_data/create_sequence_info.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sabot_sql/parser/parsed_data/create_info.hpp"
#include "sabot_sql/common/limits.hpp"

namespace sabot_sql {

enum class SequenceInfo : uint8_t {
	// Sequence start
	SEQ_START,
	// Sequence increment
	SEQ_INC,
	// Sequence minimum value
	SEQ_MIN,
	// Sequence maximum value
	SEQ_MAX,
	// Sequence cycle option
	SEQ_CYCLE,
	// Sequence owner table
	SEQ_OWN
};

struct CreateSequenceInfo : public CreateInfo {
	CreateSequenceInfo();

	//! Sequence name to create
	string name;
	//! Usage count of the sequence
	uint64_t usage_count;
	//! The increment value
	int64_t increment;
	//! The minimum value of the sequence
	int64_t min_value;
	//! The maximum value of the sequence
	int64_t max_value;
	//! The start value of the sequence
	int64_t start_value;
	//! Whether or not the sequence cycles
	bool cycle;

public:
	unique_ptr<CreateInfo> Copy() const override;

public:
	SABOT_SQL_API void Serialize(Serializer &serializer) const override;
	SABOT_SQL_API static unique_ptr<CreateInfo> Deserialize(Deserializer &deserializer);

	string ToString() const override;
};

} // namespace sabot_sql
