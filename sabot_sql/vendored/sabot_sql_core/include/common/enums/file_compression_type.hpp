//===----------------------------------------------------------------------===//
//                         SabotSQL
//
// sabot_sql/common/enums/file_compression_type.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sabot_sql/common/constants.hpp"

namespace sabot_sql {

enum class FileCompressionType : uint8_t { AUTO_DETECT = 0, UNCOMPRESSED = 1, GZIP = 2, ZSTD = 3 };

FileCompressionType FileCompressionTypeFromString(const string &input);

string CompressionExtensionFromType(const FileCompressionType type);

bool IsFileCompressed(string path, FileCompressionType type);

} // namespace sabot_sql
