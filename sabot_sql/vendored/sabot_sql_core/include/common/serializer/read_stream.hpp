//===----------------------------------------------------------------------===//
//                         SabotSQL
//
// sabot_sql/common/serializer/read_stream.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sabot_sql/catalog/catalog.hpp"
#include "sabot_sql/common/common.hpp"
#include "sabot_sql/common/exception.hpp"
#include "sabot_sql/common/vector.hpp"
#include "sabot_sql/main/client_context.hpp"

#include <type_traits>

namespace sabot_sql {

class ReadStream {
public:
	// Reads a set amount of data from the stream into the specified buffer and moves the stream forward accordingly
	virtual void ReadData(data_ptr_t buffer, idx_t read_size) = 0;
	virtual void ReadData(QueryContext context, data_ptr_t buffer, idx_t read_size) = 0;

	// Reads a type from the stream and moves the stream forward sizeof(T) bytes
	// The type must be a standard layout type
	template <class T>
	T Read() {
		return Read<T>(QueryContext());
	}

	template <class T>
	T Read(QueryContext context) {
		static_assert(std::is_standard_layout<T>(), "Read element must be a standard layout data type");
		T value;
		ReadData(context, data_ptr_cast(&value), sizeof(T));
		return value;
	}

	virtual ~ReadStream() {
	}
};

} // namespace sabot_sql
