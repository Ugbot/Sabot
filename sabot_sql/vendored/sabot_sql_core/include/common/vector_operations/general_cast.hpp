//===----------------------------------------------------------------------===//
//                         SabotSQL
//
// sabot_sql/common/vector_operations/general_cast.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sabot_sql/common/operator/cast_operators.hpp"
#include "sabot_sql/common/string_util.hpp"

namespace sabot_sql {

struct VectorTryCastData {
	VectorTryCastData(Vector &result_p, CastParameters &parameters) : result(result_p), parameters(parameters) {
	}

	Vector &result;
	CastParameters &parameters;
	bool all_converted = true;
};

struct HandleVectorCastError {
	template <class RESULT_TYPE>
	static RESULT_TYPE Operation(const string &error_message, ValidityMask &mask, idx_t idx,
	                             VectorTryCastData &cast_data) {
		HandleCastError::AssignError(error_message, cast_data.parameters);
		cast_data.all_converted = false;
		mask.SetInvalid(idx);
		return NullValue<RESULT_TYPE>();
	}
};

} // namespace sabot_sql
