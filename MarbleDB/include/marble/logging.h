/**
 * Compile-Time Logging System for MarbleDB
 *
 * Features:
 * - Zero-cost abstraction when disabled
 * - Per-component log level control
 * - Tag-based filtering
 * - Single centralized output point
 *
 * Usage:
 *   // Define component tag
 *   MARBLE_LOG_TAG(ArrowReader);
 *
 *   // Log with component-specific level control
 *   MARBLE_LOG_DEBUG(ArrowReader) << "Loading batch " << batch_id;
 *   MARBLE_LOG_INFO(LSMStorage) << "Compaction complete";
 *
 * Build configuration:
 *   cmake -DMARBLE_ENABLE_DEBUG_LOGGING=ON ..   # Enable all logging
 *   cmake -DMARBLE_MIN_LOG_LEVEL=2 ..           # Set minimum level (INFO)
 */

#pragma once

#include <string>
#include <sstream>
#include <iostream>
#include <mutex>

namespace marble {
namespace logging {

// ============================================================================
// Log Levels
// ============================================================================

enum class LogLevel : int {
    TRACE = 0,
    DEBUG = 1,
    INFO  = 2,
    WARN  = 3,
    ERROR = 4,
    FATAL = 5,
    OFF   = 6
};

// ============================================================================
// Log Component Tags
// ============================================================================

struct LogTag {
    const char* name;
    LogLevel min_level;

    constexpr LogTag(const char* n, LogLevel level = LogLevel::DEBUG)
        : name(n), min_level(level) {}
};

// Global minimum log level (compile-time configurable)
#ifndef MARBLE_MIN_LOG_LEVEL
#define MARBLE_MIN_LOG_LEVEL 2  // INFO by default
#endif

constexpr LogLevel kGlobalMinLevel = static_cast<LogLevel>(MARBLE_MIN_LOG_LEVEL);

// ============================================================================
// Centralized Log Output
// ============================================================================

class LogOutput {
public:
    static LogOutput& Instance() {
        static LogOutput instance;
        return instance;
    }

    void Write(const LogTag& tag, LogLevel level, const std::string& message) {
        if (!ShouldLog(tag, level)) return;

        std::lock_guard<std::mutex> lock(mutex_);
        std::cerr << "[" << LevelToString(level) << "] "
                  << "[" << tag.name << "] "
                  << message << "\n";
    }

    bool ShouldLog(const LogTag& tag, LogLevel level) const {
        if (level < kGlobalMinLevel) return false;
        if (level < tag.min_level) return false;
        return enabled_;
    }

    void SetEnabled(bool enabled) {
        std::lock_guard<std::mutex> lock(mutex_);
        enabled_ = enabled;
    }

private:
    LogOutput() : enabled_(true) {}

    static const char* LevelToString(LogLevel level) {
        switch (level) {
            case LogLevel::TRACE: return "TRACE";
            case LogLevel::DEBUG: return "DEBUG";
            case LogLevel::INFO:  return "INFO ";
            case LogLevel::WARN:  return "WARN ";
            case LogLevel::ERROR: return "ERROR";
            case LogLevel::FATAL: return "FATAL";
            default: return "?????";
        }
    }

    std::mutex mutex_;
    bool enabled_;
};

// ============================================================================
// Stream-like Logger Helper
// ============================================================================

#ifdef MARBLE_ENABLE_DEBUG_LOGGING

template<LogLevel Level>
class LogStream {
public:
    LogStream(const LogTag& tag) : tag_(tag), enabled_(false) {
        if constexpr (Level >= kGlobalMinLevel) {
            enabled_ = LogOutput::Instance().ShouldLog(tag, Level);
        }
    }

    ~LogStream() {
        if (enabled_) {
            LogOutput::Instance().Write(tag_, Level, oss_.str());
        }
    }

    template<typename T>
    LogStream& operator<<(const T& value) {
        if (enabled_) {
            oss_ << value;
        }
        return *this;
    }

private:
    const LogTag& tag_;
    bool enabled_;
    std::ostringstream oss_;
};

#else

// Zero-cost: Empty class that compiles away
template<LogLevel Level>
class LogStream {
public:
    LogStream(const LogTag&) {}

    template<typename T>
    LogStream& operator<<(const T&) {
        return *this;
    }
};

#endif // MARBLE_ENABLE_DEBUG_LOGGING

} // namespace logging
} // namespace marble

// ============================================================================
// Convenience Macros
// ============================================================================

// Define a log component tag
#define MARBLE_LOG_TAG(name) \
    static constexpr ::marble::logging::LogTag name##Tag(#name)

// Define a log component tag with custom level
#define MARBLE_LOG_TAG_LEVEL(name, level) \
    static constexpr ::marble::logging::LogTag name##Tag(#name, ::marble::logging::LogLevel::level)

// Main logging macros (return LogStream for << chaining)
#define MARBLE_LOG_TRACE(component) \
    ::marble::logging::LogStream<::marble::logging::LogLevel::TRACE>(component##Tag)

#define MARBLE_LOG_DEBUG(component) \
    ::marble::logging::LogStream<::marble::logging::LogLevel::DEBUG>(component##Tag)

#define MARBLE_LOG_INFO(component) \
    ::marble::logging::LogStream<::marble::logging::LogLevel::INFO>(component##Tag)

#define MARBLE_LOG_WARN(component) \
    ::marble::logging::LogStream<::marble::logging::LogLevel::WARN>(component##Tag)

#define MARBLE_LOG_ERROR(component) \
    ::marble::logging::LogStream<::marble::logging::LogLevel::ERROR>(component##Tag)

#define MARBLE_LOG_FATAL(component) \
    ::marble::logging::LogStream<::marble::logging::LogLevel::FATAL>(component##Tag)
