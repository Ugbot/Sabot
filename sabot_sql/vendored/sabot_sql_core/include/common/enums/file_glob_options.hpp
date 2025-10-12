//===----------------------------------------------------------------------===//
//                         SabotSQL
//
// sabot_sql/common/enums/file_glob_options.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sabot_sql/common/constants.hpp"

namespace sabot_sql {

enum class FileGlobOptions : uint8_t { DISALLOW_EMPTY = 0, ALLOW_EMPTY = 1, FALLBACK_GLOB = 2 };

struct FileGlobInput {
	FileGlobInput(FileGlobOptions options) // NOLINT: allow implicit conversion from FileGlobOptions
	    : behavior(options) {
	}
	FileGlobInput(FileGlobOptions options, string extension_p) : behavior(options), extension(std::move(extension_p)) {
	}

	FileGlobOptions behavior;
	string extension;
};

} // namespace sabot_sql
