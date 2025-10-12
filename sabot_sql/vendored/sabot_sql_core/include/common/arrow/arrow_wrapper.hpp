//===----------------------------------------------------------------------===//
//                         SabotSQL
//
// sabot_sql/common/arrow/arrow_wrapper.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once
#include "sabot_sql/common/arrow/arrow.hpp"
#include "sabot_sql/common/helper.hpp"

//! Here we have the internal sabot_sql classes that interact with Arrow's Internal Header (i.e., sabot_sql/commons/arrow.hpp)
namespace sabot_sql {

class ArrowSchemaWrapper {
public:
	ArrowSchema arrow_schema;

	ArrowSchemaWrapper() {
		arrow_schema.release = nullptr;
	}

	~ArrowSchemaWrapper();
};
class ArrowArrayWrapper {
public:
	ArrowArray arrow_array;
	ArrowArrayWrapper() {
		arrow_array.length = 0;
		arrow_array.release = nullptr;
	}
	ArrowArrayWrapper(ArrowArrayWrapper &&other) noexcept : arrow_array(other.arrow_array) {
		other.arrow_array.release = nullptr;
	}
	ArrowArrayWrapper &operator=(ArrowArrayWrapper &&other) noexcept {
		if (this != &other) {
			if (arrow_array.release) {
				arrow_array.release(&arrow_array);
			}
			arrow_array = other.arrow_array;
			other.arrow_array.release = nullptr;
		}
		return *this;
	}
	~ArrowArrayWrapper();
};

class ArrowArrayStreamWrapper {
public:
	ArrowArrayStream arrow_array_stream;
	int64_t number_of_rows;

public:
	void GetSchema(ArrowSchemaWrapper &schema);

	virtual shared_ptr<ArrowArrayWrapper> GetNextChunk();

	const char *GetError();

	virtual ~ArrowArrayStreamWrapper();
	ArrowArrayStreamWrapper() {
		arrow_array_stream.release = nullptr;
	}
};

} // namespace sabot_sql
