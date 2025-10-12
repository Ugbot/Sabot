//===----------------------------------------------------------------------===//
//                         SabotSQL
//
// sabot_sql/logging/log_manager.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sabot_sql/logging/logger.hpp"
#include "sabot_sql/logging/log_storage.hpp"
#include "sabot_sql/common/types/timestamp.hpp"
#include "sabot_sql/common/case_insensitive_map.hpp"

namespace sabot_sql {
class LogType;

// Holds global logging state
// - Handles configuration changes
// - Creates Loggers with cached configuration
// - Main sink for logs (either by logging directly into this, or by syncing a pre-cached set of log entries)
// - Holds the log storage
class LogManager : public enable_shared_from_this<LogManager> {
	friend class ThreadSafeLogger;
	friend class ThreadLocalLogger;
	friend class MutableLogger;

public:
	// Note: two step initialization because Logger needs shared pointer to log manager TODO: can we clean up?
	explicit LogManager(DatabaseInstance &db, LogConfig config = LogConfig());
	~LogManager();
	void Initialize();

	SABOT_SQL_API static LogManager &Get(ClientContext &context);
	unique_ptr<Logger> CreateLogger(LoggingContext context, bool thread_safe = true, bool mutable_settings = false);

	RegisteredLoggingContext RegisterLoggingContext(LoggingContext &context);

	SABOT_SQL_API bool RegisterLogStorage(const string &name, shared_ptr<LogStorage> &storage);

	//! The global logger can be used whe
	SABOT_SQL_API Logger &GlobalLogger();
	SABOT_SQL_API shared_ptr<Logger> GlobalLoggerReference();

	//! Flush everything
	SABOT_SQL_API void Flush();

	//! Get a shared_ptr to the log storage (For example, to scan it)
	SABOT_SQL_API shared_ptr<LogStorage> GetLogStorage();
	SABOT_SQL_API bool CanScan(LoggingTargetTable table);

	SABOT_SQL_API void SetConfig(DatabaseInstance &db, const LogConfig &config);
	SABOT_SQL_API void SetEnableLogging(bool enable);
	SABOT_SQL_API void SetLogMode(LogMode mode);
	SABOT_SQL_API void SetLogLevel(LogLevel level);
	SABOT_SQL_API void SetEnabledLogTypes(optional_ptr<unordered_set<string>> enabled_log_types);
	SABOT_SQL_API void SetDisabledLogTypes(optional_ptr<unordered_set<string>> disabled_log_types);
	SABOT_SQL_API void SetLogStorage(DatabaseInstance &db, const string &storage_name);

	SABOT_SQL_API void UpdateLogStorageConfig(DatabaseInstance &db, case_insensitive_map_t<Value> &config_value);

	SABOT_SQL_API void SetEnableStructuredLoggers(vector<string> &enabled_logger_types);

	SABOT_SQL_API void TruncateLogStorage();

	SABOT_SQL_API LogConfig GetConfig();

	SABOT_SQL_API void RegisterLogType(unique_ptr<LogType> type);
	SABOT_SQL_API optional_ptr<const LogType> LookupLogType(const string &type);
	SABOT_SQL_API void RegisterDefaultLogTypes();

protected:
	RegisteredLoggingContext RegisterLoggingContextInternal(LoggingContext &context);

	// This is to be called by the Loggers only, it does not verify log_level and log_type
	void WriteLogEntry(timestamp_t, const char *log_type, LogLevel log_level, const char *log_message,
	                   const RegisteredLoggingContext &context);
	// This allows efficiently pushing a cached set of log entries into the log manager
	void FlushCachedLogEntries(DataChunk &chunk, const RegisteredLoggingContext &context);

	void SetLogStorageInternal(DatabaseInstance &db, const string &storage_name);

	optional_ptr<const LogType> LookupLogTypeInternal(const string &type);

	void SetConfigInternal(LogConfig config);

	mutex lock;
	LogConfig config;

	shared_ptr<Logger> global_logger;

	shared_ptr<LogStorage> log_storage;

	idx_t next_registered_logging_context_index = 0;

	// Any additional LogStorages registered (by extensions for example)
	case_insensitive_map_t<shared_ptr<LogStorage>> registered_log_storages;
	case_insensitive_map_t<unique_ptr<LogType>> registered_log_types;
};

} // namespace sabot_sql
