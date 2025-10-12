//===----------------------------------------------------------------------===//
//                         SabotSQL
//
// sabot_sql/common/multi_file/multi_file_options.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sabot_sql/common/case_insensitive_map.hpp"
#include "sabot_sql/common/hive_partitioning.hpp"
#include "sabot_sql/common/types.hpp"
#include "sabot_sql/main/client_context.hpp"

namespace sabot_sql {
struct BindInfo;
class MultiFileList;

enum class MultiFileColumnMappingMode : uint8_t { BY_NAME, BY_FIELD_ID };

struct MultiFileOptions {
	bool filename = false;
	bool hive_partitioning = false;
	bool auto_detect_hive_partitioning = true;
	bool union_by_name = false;
	bool hive_types_autocast = true;
	MultiFileColumnMappingMode mapping = MultiFileColumnMappingMode::BY_NAME;

	case_insensitive_map_t<LogicalType> hive_types_schema;

	// Default/configurable name of the column containing the file names
	static constexpr const char *DEFAULT_FILENAME_COLUMN = "filename";
	string filename_column = DEFAULT_FILENAME_COLUMN;
	// These are used to pass options through custom multifilereaders
	case_insensitive_map_t<Value> custom_options;

	SABOT_SQL_API void Serialize(Serializer &serializer) const;
	SABOT_SQL_API static MultiFileOptions Deserialize(Deserializer &source);
	SABOT_SQL_API void AddBatchInfo(BindInfo &bind_info) const;
	SABOT_SQL_API void AutoDetectHivePartitioning(MultiFileList &files, ClientContext &context);
	SABOT_SQL_API static bool AutoDetectHivePartitioningInternal(MultiFileList &files, ClientContext &context);
	SABOT_SQL_API void AutoDetectHiveTypesInternal(MultiFileList &files, ClientContext &context);
	SABOT_SQL_API void VerifyHiveTypesArePartitions(const std::map<string, string> &partitions) const;
	SABOT_SQL_API LogicalType GetHiveLogicalType(const string &hive_partition_column) const;
	SABOT_SQL_API Value GetHivePartitionValue(const string &base, const string &entry, ClientContext &context) const;
	SABOT_SQL_API bool AnySet() const;
};

} // namespace sabot_sql
