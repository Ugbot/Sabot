//===----------------------------------------------------------------------===//
//                         SabotSQL
//
// sabot_sql/common/windows_util.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sabot_sql/common/common.hpp"
#include "sabot_sql/common/constants.hpp"
#include "sabot_sql/common/exception.hpp"
#include "sabot_sql/common/vector.hpp"
#include "sabot_sql/common/windows.hpp"

namespace sabot_sql {

#ifdef SABOT_SQL_WINDOWS
class WindowsUtil {
public:
	//! Windows helper functions
	static std::wstring UTF8ToUnicode(const char *input);
	static string UnicodeToUTF8(LPCWSTR input);
	static string UTF8ToMBCS(const char *input, bool use_ansi = false);
};
#endif

} // namespace sabot_sql
