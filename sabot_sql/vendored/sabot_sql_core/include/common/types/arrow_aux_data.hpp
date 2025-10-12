//===----------------------------------------------------------------------===//
//                         SabotSQL
//
// sabot_sql/common/types/arrow_aux_data.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sabot_sql/common/types/vector_buffer.hpp"
#include "sabot_sql/common/arrow/arrow_wrapper.hpp"

namespace sabot_sql {

struct ArrowAuxiliaryData : public VectorAuxiliaryData {
	static constexpr const VectorAuxiliaryDataType TYPE = VectorAuxiliaryDataType::ARROW_AUXILIARY;
	explicit ArrowAuxiliaryData(shared_ptr<ArrowArrayWrapper> arrow_array_p)
	    : VectorAuxiliaryData(VectorAuxiliaryDataType::ARROW_AUXILIARY), arrow_array(std::move(arrow_array_p)) {
	}
	~ArrowAuxiliaryData() override {
	}

	shared_ptr<ArrowArrayWrapper> arrow_array;
};

} // namespace sabot_sql
