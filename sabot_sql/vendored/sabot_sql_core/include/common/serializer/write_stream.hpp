//===----------------------------------------------------------------------===//
//                         SabotSQL
//
// sabot_sql/common/serializer/write_stream.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sabot_sql/catalog/catalog.hpp"
#include "sabot_sql/common/common.hpp"
#include "sabot_sql/common/exception.hpp"
#include "sabot_sql/common/vector.hpp"
#include <type_traits>

namespace sabot_sql {

class WriteStream {
public:
	// Writes a set amount of data from the specified buffer into the stream and moves the stream forward accordingly
	virtual void WriteData(const_data_ptr_t buffer, idx_t write_size) = 0;

	// Writes a type into the stream and moves the stream forward sizeof(T) bytes
	// The type must be a standard layout type
	template <class T>
	void Write(T element) {
		static_assert(std::is_standard_layout<T>(), "Write element must be a standard layout data type");
		WriteData(const_data_ptr_cast(&element), sizeof(T));
	}

	virtual ~WriteStream() {
	}
};

} // namespace sabot_sql
