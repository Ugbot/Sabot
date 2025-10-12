//===----------------------------------------------------------------------===//
//                         SabotSQL
//
// sabot_sql/storage/checkpoint/table_data_reader.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sabot_sql/storage/checkpoint_manager.hpp"

namespace sabot_sql {
struct BoundCreateTableInfo;

//! The table data reader is responsible for reading the data of a table from the block manager
class TableDataReader {
public:
	TableDataReader(MetadataReader &reader, BoundCreateTableInfo &info, MetaBlockPointer table_pointer);

	void ReadTableData();

private:
	MetadataReader &reader;
	BoundCreateTableInfo &info;
};

} // namespace sabot_sql
