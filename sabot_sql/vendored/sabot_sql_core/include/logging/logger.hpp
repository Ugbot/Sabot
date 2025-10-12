//===----------------------------------------------------------------------===//
//                         SabotSQL
//
// sabot_sql/logging/logger.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sabot_sql/logging/logging.hpp"
#include "sabot_sql/logging/log_type.hpp"
#include "sabot_sql/common/types.hpp"
#include "sabot_sql/common/string_util.hpp"

namespace sabot_sql {
class TableDescription;
class DatabaseInstance;
class DataChunk;
class LogManager;
class ColumnDataCollection;
class ThreadContext;
class FileOpener;
class LogStorage;
class ExecutionContext;
struct FileHandle;

//! Internal
#define SABOT_SQL_LOG_INTERNAL(SOURCE, TYPE, LEVEL, ...)                                                                  \
	{                                                                                                                  \
		auto &logger_ref_ = Logger::Get(SOURCE);                                                                       \
		if (logger_ref_.ShouldLog(TYPE, LEVEL)) {                                                                      \
			logger_ref_.WriteLog(TYPE, LEVEL, __VA_ARGS__);                                                            \
		}                                                                                                              \
	}

//! Default Loggers
#define SABOT_SQL_LOG_TRACE(SOURCE, ...)                                                                                  \
	SABOT_SQL_LOG_INTERNAL(SOURCE, DefaultLogType::NAME, LogLevel::LOG_TRACE, __VA_ARGS__)
#define SABOT_SQL_LOG_DEBUG(SOURCE, ...)                                                                                  \
	SABOT_SQL_LOG_INTERNAL(SOURCE, DefaultLogType::NAME, LogLevel::LOG_DEBUG, __VA_ARGS__)
#define SABOT_SQL_LOG_INFO(SOURCE, ...) SABOT_SQL_LOG_INTERNAL(SOURCE, DefaultLogType::NAME, LogLevel::LOG_INFO, __VA_ARGS__)
#define SABOT_SQL_LOG_WARN(SOURCE, ...) SABOT_SQL_LOG_INTERNAL(SOURCE, DefaultLogType::NAME, LogLevel::LOG_WARN, __VA_ARGS__)
#define SABOT_SQL_LOG_ERROR(SOURCE, ...)                                                                                  \
	SABOT_SQL_LOG_INTERNAL(SOURCE, DefaultLogType::NAME, LogLevel::LOG_ERROR, __VA_ARGS__)
#define SABOT_SQL_LOG_FATAL(SOURCE, ...)                                                                                  \
	SABOT_SQL_LOG_INTERNAL(SOURCE, DefaultLogType::NAME, LogLevel::LOG_FATAL, __VA_ARGS__)

//! LogType based loggers
#define SABOT_SQL_LOG(SOURCE, LOG_TYPE_CLASS, ...)                                                                        \
	SABOT_SQL_LOG_INTERNAL(SOURCE, LOG_TYPE_CLASS::NAME, LOG_TYPE_CLASS::LEVEL,                                           \
	                    LOG_TYPE_CLASS::ConstructLogMessage(__VA_ARGS__))

//! Main logging interface
class Logger {
public:
	SABOT_SQL_API explicit Logger(LogManager &manager) : manager(manager) {
	}

	SABOT_SQL_API virtual ~Logger() = default;

	// Main Logging interface. In most cases the macros above should be used instead of calling these directly
	SABOT_SQL_API virtual bool ShouldLog(const char *log_type, LogLevel log_level) = 0;
	SABOT_SQL_API virtual void WriteLog(const char *log_type, LogLevel log_level, const char *message) = 0;

	// Some more string types for easy logging
	SABOT_SQL_API void WriteLog(const char *log_type, LogLevel log_level, const string &message);
	SABOT_SQL_API void WriteLog(const char *log_type, LogLevel log_level, const string_t &message);

	// Syntactic sugar for formatted strings
	template <typename... ARGS>
	void WriteLog(const char *log_type, LogLevel log_level, const char *format_string, ARGS... params) {
		auto formatted_string = StringUtil::Format(format_string, params...);
		WriteLog(log_type, log_level, formatted_string.c_str());
	}

	SABOT_SQL_API virtual void Flush() = 0;

	// Get the Logger to write log messages to. In decreasing order of preference(!) so the ThreadContext getter is the
	// most preferred way of fetching the logger and the DatabaseInstance getter the least preferred. This has to do
	// both with logging performance and level of detail of logging context that is provided.
	SABOT_SQL_API static Logger &Get(const ThreadContext &thread_context);
	SABOT_SQL_API static Logger &Get(const ExecutionContext &execution_context);
	SABOT_SQL_API static Logger &Get(const ClientContext &client_context);
	SABOT_SQL_API static Logger &Get(const FileOpener &opener);
	SABOT_SQL_API static Logger &Get(const DatabaseInstance &db);
	SABOT_SQL_API static Logger &Get(const shared_ptr<Logger> &logger);

	template <class T>
	static void Flush(T &log_context_source) {
		Get(log_context_source).Flush();
	}

	SABOT_SQL_API virtual bool IsThreadSafe() = 0;
	SABOT_SQL_API virtual bool IsMutable() {
		return false;
	};
	SABOT_SQL_API virtual void UpdateConfig(LogConfig &new_config) {
		throw InternalException("Cannot update the config of this logger!");
	}
	SABOT_SQL_API virtual const LogConfig &GetConfig() const = 0;

protected:
	LogManager &manager;
};

// Thread-safe logger
class ThreadSafeLogger : public Logger {
public:
	explicit ThreadSafeLogger(LogConfig &config_p, LoggingContext &context_p, LogManager &manager);
	explicit ThreadSafeLogger(LogConfig &config_p, RegisteredLoggingContext context_p, LogManager &manager);

	// Main Logger API
	bool ShouldLog(const char *log_type, LogLevel log_level) override;
	void WriteLog(const char *log_type, LogLevel log_level, const char *message) override;

	void Flush() override;
	bool IsThreadSafe() override {
		return true;
	}
	const LogConfig &GetConfig() const override {
		return config;
	}

protected:
	const LogConfig config;
	mutex lock;
	const RegisteredLoggingContext context;
};

// Non Thread-safe logger
// - will cache log entries locally
class ThreadLocalLogger : public Logger {
public:
	explicit ThreadLocalLogger(LogConfig &config_p, LoggingContext &context_p, LogManager &manager);
	explicit ThreadLocalLogger(LogConfig &config_p, RegisteredLoggingContext context_p, LogManager &manager);

	// Main Logger API
	bool ShouldLog(const char *log_type, LogLevel log_level) override;
	void WriteLog(const char *log_type, LogLevel log_level, const char *message) override;
	void Flush() override;

	bool IsThreadSafe() override {
		return false;
	}
	const LogConfig &GetConfig() const override {
		return config;
	}

protected:
	const LogConfig config;
	const RegisteredLoggingContext context;
};

// Thread-safe Logger with mutable log settings
class MutableLogger : public Logger {
public:
	explicit MutableLogger(LogConfig &config_p, LoggingContext &context_p, LogManager &manager);
	explicit MutableLogger(LogConfig &config_p, RegisteredLoggingContext context_p, LogManager &manager);

	// Main Logger API
	bool ShouldLog(const char *log_type, LogLevel log_level) override;
	void WriteLog(const char *log_type, LogLevel log_level, const char *message) override;

	void Flush() override;
	bool IsThreadSafe() override {
		return true;
	}
	bool IsMutable() override {
		return true;
	}
	const LogConfig &GetConfig() const override {
		return config;
	}
	void UpdateConfig(LogConfig &new_config) override;

protected:
	// Atomics for lock-free log setting checks
	atomic<bool> enabled;
	atomic<LogMode> mode;
	atomic<LogLevel> level;

	mutex lock;
	LogConfig config;
	const RegisteredLoggingContext context;
};

// For when logging is disabled: NOPs everything
class NopLogger : public Logger {
public:
	explicit NopLogger(LogManager &manager) : Logger(manager) {
	}
	bool ShouldLog(const char *log_type, LogLevel log_level) override {
		return false;
	}
	void WriteLog(const char *log_type, LogLevel log_level, const char *message) override {};
	void Flush() override {
	}
	bool IsThreadSafe() override {
		return true;
	}
	const LogConfig &GetConfig() const override {
		throw InternalException("Called GetConfig on NopLogger");
	}
};

} // namespace sabot_sql
