//===----------------------------------------------------------------------===//
//                         SabotSQL
//
// sabot_sql/logging/file_system_logger.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

namespace sabot_sql {

#define SABOT_SQL_LOG_FILE_SYSTEM_BYTES(HANDLE, OP, BYTES, POS)                                                           \
	{                                                                                                                  \
		if (HANDLE.logger) {                                                                                           \
			SABOT_SQL_LOG(HANDLE.logger, FileSystemLogType, HANDLE, OP, BYTES, POS)                                       \
		}                                                                                                              \
	}
#define SABOT_SQL_LOG_FILE_SYSTEM(HANDLE, OP)                                                                             \
	{                                                                                                                  \
		if (HANDLE.logger) {                                                                                           \
			SABOT_SQL_LOG(HANDLE.logger, FileSystemLogType, HANDLE, OP)                                                   \
		}                                                                                                              \
	}

// Macros for logging to file handles
#define SABOT_SQL_LOG_FILE_SYSTEM_READ(HANDLE, BYTES, POS)  SABOT_SQL_LOG_FILE_SYSTEM_BYTES(HANDLE, "READ", BYTES, POS);
#define SABOT_SQL_LOG_FILE_SYSTEM_WRITE(HANDLE, BYTES, POS) SABOT_SQL_LOG_FILE_SYSTEM_BYTES(HANDLE, "WRITE", BYTES, POS);
#define SABOT_SQL_LOG_FILE_SYSTEM_OPEN(HANDLE)              SABOT_SQL_LOG_FILE_SYSTEM(HANDLE, "OPEN");
#define SABOT_SQL_LOG_FILE_SYSTEM_CLOSE(HANDLE)             SABOT_SQL_LOG_FILE_SYSTEM(HANDLE, "CLOSE");

} // namespace sabot_sql
