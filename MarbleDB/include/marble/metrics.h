#pragma once

#include <atomic>
#include <chrono>
#include <memory>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>
#include <mutex>

namespace marble {

/**
 * @brief Metrics system for MarbleDB monitoring and observability
 *
 * Provides comprehensive metrics collection, health monitoring, and performance tracking
 * for production deployments.
 */
class MetricsCollector {
public:
    // Metric types
    enum class MetricType {
        COUNTER,      // Monotonically increasing counter
        GAUGE,        // Point-in-time value
        HISTOGRAM,    // Distribution of values
        TIMER         // Duration measurements
    };

    // Predefined metric names
    static constexpr const char* READ_OPERATIONS = "marble.read.operations";
    static constexpr const char* WRITE_OPERATIONS = "marble.write.operations";
    static constexpr const char* READ_LATENCY_MS = "marble.read.latency_ms";
    static constexpr const char* WRITE_LATENCY_MS = "marble.write.latency_ms";
    static constexpr const char* ACTIVE_CONNECTIONS = "marble.connections.active";
    static constexpr const char* TOTAL_CONNECTIONS = "marble.connections.total";
    static constexpr const char* SSTABLE_COUNT = "marble.sstable.count";
    static constexpr const char* MEMTABLE_SIZE_BYTES = "marble.memtable.size_bytes";
    static constexpr const char* COMPACTION_COUNT = "marble.compaction.count";
    static constexpr const char* COMPACTION_LATENCY_MS = "marble.compaction.latency_ms";
    static constexpr const char* BLOOM_FILTER_HITS = "marble.bloom.hits";
    static constexpr const char* BLOOM_FILTER_MISSES = "marble.bloom.misses";
    static constexpr const char* CACHE_HITS = "marble.cache.hits";
    static constexpr const char* CACHE_MISSES = "marble.cache.misses";
    static constexpr const char* RAFT_OPERATIONS = "marble.raft.operations";
    static constexpr const char* RAFT_LATENCY_MS = "marble.raft.latency_ms";
    static constexpr const char* ERROR_COUNT = "marble.errors.count";
    static constexpr const char* TRIPLE_STORE_QUERIES = "marble.triple_store.queries";
    static constexpr const char* TRIPLE_STORE_LATENCY_MS = "marble.triple_store.latency_ms";

    MetricsCollector();
    ~MetricsCollector() = default;

    // Counter operations
    void incrementCounter(const std::string& name, int64_t value = 1);
    void setGauge(const std::string& name, int64_t value);
    void recordHistogram(const std::string& name, int64_t value);
    void recordTimer(const std::string& name, std::chrono::milliseconds duration);

    // Timer helpers
    class Timer {
    public:
        Timer(MetricsCollector& collector, const std::string& metric_name);
        ~Timer();

    private:
        MetricsCollector& collector_;
        std::string metric_name_;
        std::chrono::steady_clock::time_point start_time_;
    };

    // Get metrics
    int64_t getCounter(const std::string& name) const;
    int64_t getGauge(const std::string& name) const;
    std::vector<int64_t> getHistogram(const std::string& name) const;

    // Health checks
    void recordHealthCheck(const std::string& component, bool healthy);
    bool isHealthy(const std::string& component) const;
    std::unordered_map<std::string, bool> getAllHealthChecks() const;

    // Export metrics (for monitoring systems)
    std::string exportPrometheus() const;
    std::string exportJSON() const;

    // Reset metrics (for testing)
    void reset();

private:
    mutable std::mutex mutex_;

    // Metric storage
    std::unordered_map<std::string, std::atomic<int64_t>> counters_;
    std::unordered_map<std::string, std::atomic<int64_t>> gauges_;
    std::unordered_map<std::string, std::vector<int64_t>> histograms_;
    std::unordered_map<std::string, bool> health_checks_;

    // Histogram buckets for percentile calculations
    static constexpr size_t HISTOGRAM_BUCKETS = 100;
};

/**
 * @brief Health check interface for components
 */
class HealthCheck {
public:
    virtual ~HealthCheck() = default;
    virtual bool checkHealth() const = 0;
    virtual std::string getComponentName() const = 0;
    virtual std::string getHealthDetails() const = 0;
};

/**
 * @brief Logging interface for structured logging
 */
class Logger {
public:
    enum class Level {
        TRACE = 0,
        DEBUG = 1,
        INFO = 2,
        WARN = 3,
        ERROR = 4,
        FATAL = 5
    };

    struct LogEntry {
        Level level;
        std::string component;
        std::string message;
        std::string details;
        std::chrono::system_clock::time_point timestamp;
        std::thread::id thread_id;
    };

    virtual ~Logger() = default;

    virtual void log(Level level, const std::string& component,
                    const std::string& message,
                    const std::string& details = "") = 0;

    // Convenience methods
    void trace(const std::string& component, const std::string& message,
              const std::string& details = "") {
        log(Level::TRACE, component, message, details);
    }

    void debug(const std::string& component, const std::string& message,
              const std::string& details = "") {
        log(Level::DEBUG, component, message, details);
    }

    void info(const std::string& component, const std::string& message,
             const std::string& details = "") {
        log(Level::INFO, component, message, details);
    }

    void warn(const std::string& component, const std::string& message,
             const std::string& details = "") {
        log(Level::WARN, component, message, details);
    }

    void error(const std::string& component, const std::string& message,
              const std::string& details = "") {
        log(Level::ERROR, component, message, details);
    }

    void fatal(const std::string& component, const std::string& message,
              const std::string& details = "") {
        log(Level::FATAL, component, message, details);
    }
};

/**
 * @brief Standard logger implementation
 */
class StandardLogger : public Logger {
public:
    StandardLogger(Level min_level = Level::INFO);
    ~StandardLogger() override = default;

    void log(Level level, const std::string& component,
            const std::string& message, const std::string& details) override;

    void setMinLevel(Level level) { min_level_ = level; }

private:
    Level min_level_;
    mutable std::mutex mutex_;

    std::string levelToString(Level level) const;
    std::string formatTimestamp(std::chrono::system_clock::time_point ts) const;
};

/**
 * @brief Error context for detailed error reporting
 */
class ErrorContext {
public:
    ErrorContext(const std::string& operation, const std::string& component);

    void addDetail(const std::string& key, const std::string& value);
    void addDetail(const std::string& key, int64_t value);
    void setErrorCode(int code) { error_code_ = code; }

    std::string toString() const;
    int getErrorCode() const { return error_code_; }

private:
    std::string operation_;
    std::string component_;
    int error_code_;
    std::unordered_map<std::string, std::string> details_;
    std::chrono::system_clock::time_point timestamp_;
};

/**
 * @brief Enhanced Status with error context and metrics
 */
class StatusWithMetrics {
public:
    enum class Code {
        OK = 0,
        NOT_FOUND = 1,
        CORRUPTION = 2,
        NOT_SUPPORTED = 3,
        INVALID_ARGUMENT = 4,
        IO_ERROR = 5,
        NETWORK_ERROR = 6,
        TIMEOUT = 7,
        BUSY = 8,
        RUNTIME_ERROR = 9,
        ABORTED = 10,
        UNKNOWN = 11
    };

    StatusWithMetrics();
    StatusWithMetrics(Code code, const std::string& message);
    StatusWithMetrics(Code code, const std::string& message,
                     std::unique_ptr<ErrorContext> context);

    // Status interface
    bool ok() const { return code_ == Code::OK; }
    Code code() const { return code_; }
    const std::string& message() const { return message_; }

    // Enhanced features
    const ErrorContext* context() const { return context_.get(); }
    std::string toString() const;
    void recordMetrics(MetricsCollector& collector, const std::string& operation) const;

    // Factory methods
    static StatusWithMetrics OK();
    static StatusWithMetrics NotFound(const std::string& message = "Not found");
    static StatusWithMetrics Corruption(const std::string& message = "Data corruption");
    static StatusWithMetrics NotSupported(const std::string& message = "Operation not supported");
    static StatusWithMetrics InvalidArgument(const std::string& message = "Invalid argument");
    static StatusWithMetrics IOError(const std::string& message = "I/O error");
    static StatusWithMetrics NetworkError(const std::string& message = "Network error");
    static StatusWithMetrics Timeout(const std::string& message = "Operation timeout");
    static StatusWithMetrics Busy(const std::string& message = "System busy");
    static StatusWithMetrics RuntimeError(const std::string& message = "Runtime error");
    static StatusWithMetrics Aborted(const std::string& message = "Operation aborted");
    static StatusWithMetrics Unknown(const std::string& message = "Unknown error");

private:
    Code code_;
    std::string message_;
    std::unique_ptr<ErrorContext> context_;
};

// Global metrics and logging instances
extern std::unique_ptr<MetricsCollector> global_metrics_collector;
extern std::unique_ptr<Logger> global_logger;

// Initialization functions
void initializeMetrics();
void initializeLogging(Logger::Level level = Logger::Level::INFO);
void shutdownMonitoring();

// Convenience macros for metrics
#define MARBLE_METRICS_INCREMENT(name) \
    if (::marble::global_metrics_collector) { \
        ::marble::global_metrics_collector->incrementCounter(name); \
    }

#define MARBLE_METRICS_SET_GAUGE(name, value) \
    if (::marble::global_metrics_collector) { \
        ::marble::global_metrics_collector->setGauge(name, value); \
    }

#define MARBLE_METRICS_RECORD_HISTOGRAM(name, value) \
    if (::marble::global_metrics_collector) { \
        ::marble::global_metrics_collector->recordHistogram(name, value); \
    }

#define MARBLE_METRICS_TIMER(name) \
    ::marble::MetricsCollector::Timer timer(*::marble::global_metrics_collector, name);

// Convenience macros for logging
#define MARBLE_LOG_TRACE(component, message) \
    if (::marble::global_logger) { \
        ::marble::global_logger->trace(component, message); \
    }

#define MARBLE_LOG_DEBUG(component, message) \
    if (::marble::global_logger) { \
        ::marble::global_logger->debug(component, message); \
    }

#define MARBLE_LOG_INFO(component, message) \
    if (::marble::global_logger) { \
        ::marble::global_logger->info(component, message); \
    }

#define MARBLE_LOG_WARN(component, message) \
    if (::marble::global_logger) { \
        ::marble::global_logger->warn(component, message); \
    }

#define MARBLE_LOG_ERROR(component, message) \
    if (::marble::global_logger) { \
        ::marble::global_logger->error(component, message); \
    }

#define MARBLE_LOG_FATAL(component, message) \
    if (::marble::global_logger) { \
        ::marble::global_logger->fatal(component, message); \
    }

}  // namespace marble
