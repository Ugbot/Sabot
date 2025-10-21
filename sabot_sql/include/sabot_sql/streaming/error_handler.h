#pragma once

#include <arrow/api.h>
#include <string>
#include <memory>
#include <unordered_map>
#include <functional>
#include <chrono>

namespace sabot_sql {
namespace streaming {

/**
 * Error types for streaming SQL operations
 */
enum class ErrorType {
    CONNECTOR_ERROR,      // Kafka connection, partition assignment
    STATE_ERROR,         // MarbleDB state operations
    WATERMARK_ERROR,     // Watermark processing, late data
    CHECKPOINT_ERROR,    // Checkpoint coordination, recovery
    OPERATOR_ERROR,      // Window aggregation, join operations
    ORCHESTRATOR_ERROR,  // Sabot orchestrator communication
    NETWORK_ERROR,       // Network connectivity issues
    TIMEOUT_ERROR,       // Operation timeouts
    VALIDATION_ERROR,    // Data validation failures
    UNKNOWN_ERROR        // Unexpected errors
};

/**
 * Error severity levels
 */
enum class ErrorSeverity {
    INFO,       // Informational, no action needed
    WARNING,    // Recoverable, may impact performance
    ERROR,      // Recoverable, impacts functionality
    CRITICAL,   // Non-recoverable, requires intervention
    FATAL       // System failure, requires restart
};

/**
 * Error context for debugging and recovery
 */
struct ErrorContext {
    std::string component;           // Component where error occurred
    std::string operation;           // Operation being performed
    std::string execution_id;        // Streaming execution ID
    std::string operator_id;         // Operator ID if applicable
    std::string partition_id;        // Partition ID if applicable
    int64_t timestamp;               // Error timestamp
    std::unordered_map<std::string, std::string> metadata;  // Additional context
    
    ErrorContext() : timestamp(std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::system_clock::now().time_since_epoch()).count()) {}
};

/**
 * Error recovery strategies
 */
enum class RecoveryStrategy {
    RETRY,              // Retry operation with backoff
    SKIP,               // Skip current batch/record
    FAIL_FAST,          // Fail immediately
    DEGRADE,            // Degrade functionality
    RESTART_COMPONENT,  // Restart component
    RESTART_EXECUTION,  // Restart entire execution
    MANUAL_INTERVENTION // Require manual intervention
};

/**
 * Error handler interface for streaming SQL
 */
class ErrorHandler {
public:
    virtual ~ErrorHandler() = default;
    
    /**
     * Handle an error with context
     */
    virtual arrow::Status HandleError(
        ErrorType type,
        ErrorSeverity severity,
        const std::string& message,
        const ErrorContext& context,
        RecoveryStrategy strategy = RecoveryStrategy::RETRY
    ) = 0;
    
    /**
     * Check if error is recoverable
     */
    virtual bool IsRecoverable(ErrorType type, ErrorSeverity severity) const = 0;
    
    /**
     * Get recovery strategy for error type
     */
    virtual RecoveryStrategy GetRecoveryStrategy(ErrorType type, ErrorSeverity severity) const = 0;
    
    /**
     * Register error callback
     */
    virtual void RegisterErrorCallback(
        ErrorType type,
        std::function<arrow::Status(const ErrorContext&)> callback
    ) = 0;
    
    /**
     * Get error statistics
     */
    virtual std::unordered_map<std::string, int64_t> GetErrorStats() const = 0;
    
    /**
     * Clear error statistics
     */
    virtual void ClearErrorStats() = 0;
};

/**
 * Default error handler implementation
 */
class DefaultErrorHandler : public ErrorHandler {
public:
    DefaultErrorHandler();
    ~DefaultErrorHandler() override;
    
    arrow::Status HandleError(
        ErrorType type,
        ErrorSeverity severity,
        const std::string& message,
        const ErrorContext& context,
        RecoveryStrategy strategy = RecoveryStrategy::RETRY
    ) override;
    
    bool IsRecoverable(ErrorType type, ErrorSeverity severity) const override;
    RecoveryStrategy GetRecoveryStrategy(ErrorType type, ErrorSeverity severity) const override;
    
    void RegisterErrorCallback(
        ErrorType type,
        std::function<arrow::Status(const ErrorContext&)> callback
    ) override;
    
    std::unordered_map<std::string, int64_t> GetErrorStats() const override;
    void ClearErrorStats() override;
    
    /**
     * Configure retry parameters
     */
    void SetRetryConfig(int max_retries, int base_delay_ms, int max_delay_ms);
    
    /**
     * Configure error thresholds
     */
    void SetErrorThresholds(int warning_threshold, int error_threshold, int critical_threshold);
    
private:
    struct ErrorStats {
        int64_t count = 0;
        int64_t last_occurrence = 0;
        ErrorSeverity max_severity = ErrorSeverity::INFO;
    };
    
    std::unordered_map<ErrorType, ErrorStats> error_stats_;
    std::unordered_map<ErrorType, std::function<arrow::Status(const ErrorContext&)>> callbacks_;
    
    // Retry configuration
    int max_retries_ = 3;
    int base_delay_ms_ = 1000;
    int max_delay_ms_ = 30000;
    
    // Error thresholds
    int warning_threshold_ = 10;
    int error_threshold_ = 100;
    int critical_threshold_ = 1000;
    
    // Helper methods
    arrow::Status LogError(ErrorType type, ErrorSeverity severity, const std::string& message, const ErrorContext& context);
    arrow::Status ExecuteRecovery(RecoveryStrategy strategy, const ErrorContext& context);
    void UpdateErrorStats(ErrorType type, ErrorSeverity severity);
    int CalculateBackoffDelay(int attempt) const;
};

/**
 * Circuit breaker for error handling
 */
class CircuitBreaker {
public:
    CircuitBreaker(int failure_threshold = 5, int timeout_ms = 60000);
    ~CircuitBreaker() = default;
    
    /**
     * Check if circuit is open (should fail fast)
     */
    bool IsOpen() const;
    
    /**
     * Record successful operation
     */
    void RecordSuccess();
    
    /**
     * Record failed operation
     */
    void RecordFailure();
    
    /**
     * Get circuit breaker state
     */
    std::string GetState() const;
    
    /**
     * Reset circuit breaker
     */
    void Reset();
    
private:
    int failure_threshold_;
    int timeout_ms_;
    int failure_count_;
    int64_t last_failure_time_;
    bool is_open_;
    
    bool ShouldAttemptReset() const;
};

/**
 * Error recovery utilities
 */
class ErrorRecoveryUtils {
public:
    /**
     * Retry operation with exponential backoff
     */
    static arrow::Status RetryWithBackoff(
        std::function<arrow::Status()> operation,
        int max_retries = 3,
        int base_delay_ms = 1000,
        int max_delay_ms = 30000
    );
    
    /**
     * Validate error context
     */
    static bool ValidateErrorContext(const ErrorContext& context);
    
    /**
     * Format error message
     */
    static std::string FormatErrorMessage(
        ErrorType type,
        ErrorSeverity severity,
        const std::string& message,
        const ErrorContext& context
    );
    
    /**
     * Get error type from Arrow status
     */
    static ErrorType GetErrorTypeFromStatus(const arrow::Status& status);
    
    /**
     * Get error severity from error type
     */
    static ErrorSeverity GetErrorSeverity(ErrorType type);
};

} // namespace streaming
} // namespace sabot_sql
