//===----------------------------------------------------------------------===//
//                         SabotSQL
//
// sabot_sql/storage/magic_bytes.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sabot_sql/common/common.hpp"

namespace sabot_sql {
class FileSystem;
class QueryContext;

enum class DataFileType : uint8_t {
	FILE_DOES_NOT_EXIST, // file does not exist
	SABOT_SQL_FILE,         // sabot_sql database file
	SQLITE_FILE,         // sqlite database file
	PARQUET_FILE,        // parquet file
	UNKNOWN_FILE         // unknown file type
};

class MagicBytes {
public:
	static DataFileType CheckMagicBytes(QueryContext context, FileSystem &fs, const string &path);
};

} // namespace sabot_sql
