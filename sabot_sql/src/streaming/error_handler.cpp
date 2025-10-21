#include "sabot_sql/streaming/error_handler.h"
#include <iostream>
#include <sstream>
#include <thread>
#include <chrono>
#include <algorithm>

namespace sabot_sql {
namespace streaming {

// ============================================================================
// DefaultErrorHandler Implementation
// ============================================================================

DefaultErrorHandler::DefaultErrorHandler() {
    // Initialize default error statistics
    for (int i = 0; i < static_cast<int>(ErrorType::UNKNOWN_ERROR) + 1; ++i) {
        error_stats_[static_cast<ErrorType>(i)] = ErrorStats{};
    }
}

DefaultErrorHandler::~DefaultErrorHandler() = default;

arrow::Status DefaultErrorHandler::HandleError(
    ErrorType type,
    ErrorSeverity severity,
    const std::string& message,
    const ErrorContext& context,
    RecoveryStrategy strategy
) {
    // Log the error
    ARROW_RETURN_NOT_OK(LogError(type, severity, message, context));
    
    // Update error statistics
    UpdateErrorStats(type, severity);
    
    // Execute recovery strategy
    ARROW_RETURN_NOT_OK(ExecuteRecovery(strategy, context));
    
    // Call registered callback if available
    auto callback_it = callbacks_.find(type);
    if (callback_it != callbacks_.end()) {
        ARROW_RETURN_NOT_OK(callback_it->second(context));
    }
    
    return arrow::Status::OK();
}

bool DefaultErrorHandler::IsRecoverable(ErrorType type, ErrorSeverity severity) const {
    // Most errors are recoverable except fatal ones
    if (severity == ErrorSeverity::FATAL) {
        return false;
    }
    
    // Some error types are inherently non-recoverable
    switch (type) {
        case ErrorType::VALIDATION_ERROR:
            return false;  // Data validation errors are not recoverable
        case ErrorType::UNKNOWN_ERROR:
            return false;  // Unknown errors are not recoverable
        default:
            return true;
    }
}

RecoveryStrategy DefaultErrorHandler::GetRecoveryStrategy(ErrorType type, ErrorSeverity severity) const {
    // Determine recovery strategy based on error type and severity
    switch (severity) {
        case ErrorSeverity::FATAL:
            return RecoveryStrategy::MANUAL_INTERVENTION;
        case ErrorSeverity::CRITICAL:
            return RecoveryStrategy::RESTART_EXECUTION;
        case ErrorSeverity::ERROR:
            switch (type) {
                case ErrorType::CONNECTOR_ERROR:
                case ErrorType::NETWORK_ERROR:
                    return RecoveryStrategy::RETRY;
                case ErrorType::STATE_ERROR:
                    return RecoveryStrategy::RESTART_COMPONENT;
                case ErrorType::CHECKPOINT_ERROR:
                    return RecoveryStrategy::RESTART_EXECUTION;
                default:
                    return RecoveryStrategy::RETRY;
            }
        case ErrorSeverity::WARNING:
            return RecoveryStrategy::SKIP;
        case ErrorSeverity::INFO:
            return RecoveryStrategy::SKIP;
        default:
            return RecoveryStrategy::RETRY;
    }
}

void DefaultErrorHandler::RegisterErrorCallback(
    ErrorType type,
    std::function<arrow::Status(const ErrorContext&)> callback
) {
    callbacks_[type] = callback;
}

std::unordered_map<std::string, int64_t> DefaultErrorHandler::GetErrorStats() const {
    std::unordered_map<std::string, int64_t> stats;
    
    for (const auto& [type, error_stats] : error_stats_) {
        std::string type_name = "error_type_" + std::to_string(static_cast<int>(type));
        stats[type_name + "_count"] = error_stats.count;
        stats[type_name + "_last_occurrence"] = error_stats.last_occurrence;
        stats[type_name + "_max_severity"] = static_cast<int64_t>(error_stats.max_severity);
    }
    
    return stats;
}

void DefaultErrorHandler::ClearErrorStats() {
    for (auto& [type, stats] : error_stats_) {
        stats = ErrorStats{};
    }
}

void DefaultErrorHandler::SetRetryConfig(int max_retries, int base_delay_ms, int max_delay_ms) {
    max_retries_ = max_retries;
    base_delay_ms_ = base_delay_ms;
    max_delay_ms_ = max_delay_ms;
}

void DefaultErrorHandler::SetErrorThresholds(int warning_threshold, int error_threshold, int critical_threshold) {
    warning_threshold_ = warning_threshold;
    error_threshold_ = error_threshold;
    critical_threshold_ = critical_threshold;
}

arrow::Status DefaultErrorHandler::LogError(
    ErrorType type,
    ErrorSeverity severity,
    const std::string& message,
    const ErrorContext& context
) {
    // Format error message
    std::string formatted_message = ErrorRecoveryUtils::FormatErrorMessage(type, severity, message, context);
    
    // Log based on severity
    switch (severity) {
        case ErrorSeverity::FATAL:
        case ErrorSeverity::CRITICAL:
            std::cerr << "[FATAL] " << formatted_message << std::endl;
            break;
        case ErrorSeverity::ERROR:
            std::cerr << "[ERROR] " << formatted_message << std::endl;
            break;
        case ErrorSeverity::WARNING:
            std::cout << "[WARNING] " << formatted_message << std::endl;
            break;
        case ErrorSeverity::INFO:
            std::cout << "[INFO] " << formatted_message << std::endl;
            break;
    }
    
    return arrow::Status::OK();
}

arrow::Status DefaultErrorHandler::ExecuteRecovery(
    RecoveryStrategy strategy,
    const ErrorContext& context
) {
    switch (strategy) {
        case RecoveryStrategy::RETRY:
            // Retry logic is handled by the caller
            break;
        case RecoveryStrategy::SKIP:
            // Skip current operation
            break;
        case RecoveryStrategy::FAIL_FAST:
            return arrow::Status::Invalid("Fail fast strategy triggered");
        case RecoveryStrategy::DEGRADE:
            // Degrade functionality
            break;
        case RecoveryStrategy::RESTART_COMPONENT:
            // Restart component logic would be implemented here
            break;
        case RecoveryStrategy::RESTART_EXECUTION:
            // Restart execution logic would be implemented here
            break;
        case RecoveryStrategy::MANUAL_INTERVENTION:
            return arrow::Status::Invalid("Manual intervention required");
    }
    
    return arrow::Status::OK();
}

void DefaultErrorHandler::UpdateErrorStats(ErrorType type, ErrorSeverity severity) {
    auto& stats = error_stats_[type];
    stats.count++;
    stats.last_occurrence = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::system_clock::now().time_since_epoch()
    ).count();
    
    if (severity > stats.max_severity) {
        stats.max_severity = severity;
    }
}

int DefaultErrorHandler::CalculateBackoffDelay(int attempt) const {
    // Exponential backoff with jitter
    int delay = base_delay_ms_ * (1 << attempt);  // 2^attempt
    delay = std::min(delay, max_delay_ms_);
    
    // Add jitter (±25%)
    int jitter = delay * 0.25;
    delay += (rand() % (2 * jitter + 1)) - jitter;
    
    return std::max(0, delay);
}

// ============================================================================
// CircuitBreaker Implementation
// ============================================================================

CircuitBreaker::CircuitBreaker(int failure_threshold, int timeout_ms)
    : failure_threshold_(failure_threshold)
    , timeout_ms_(timeout_ms)
    , failure_count_(0)
    , last_failure_time_(0)
    , is_open_(false) {
}

bool CircuitBreaker::IsOpen() const {
    if (is_open_) {
        return !ShouldAttemptReset();
    }
    return false;
}

void CircuitBreaker::RecordSuccess() {
    failure_count_ = 0;
    is_open_ = false;
}

void CircuitBreaker::RecordFailure() {
    failure_count_++;
    last_failure_time_ = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::system_clock::now().time_since_epoch()
    ).count();
    
    if (failure_count_ >= failure_threshold_) {
        is_open_ = true;
    }
}

std::string CircuitBreaker::GetState() const {
    if (is_open_) {
        return "OPEN";
    } else if (failure_count_ > 0) {
        return "HALF_OPEN";
    } else {
        return "CLOSED";
    }
}

void CircuitBreaker::Reset() {
    failure_count_ = 0;
    is_open_ = false;
    last_failure_time_ = 0;
}

bool CircuitBreaker::ShouldAttemptReset() const {
    if (!is_open_) {
        return false;
    }
    
    int64_t current_time = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::system_clock::now().time_since_epoch()
    ).count();
    
    return (current_time - last_failure_time_) >= timeout_ms_;
}

// ============================================================================
// ErrorRecoveryUtils Implementation
// ============================================================================

arrow::Status ErrorRecoveryUtils::RetryWithBackoff(
    std::function<arrow::Status()> operation,
    int max_retries,
    int base_delay_ms,
    int max_delay_ms
) {
    int attempt = 0;
    
    while (attempt <= max_retries) {
        auto status = operation();
        
        if (status.ok()) {
            return arrow::Status::OK();
        }
        
        if (attempt == max_retries) {
            return status;
        }
        
        // Calculate backoff delay
        int delay = base_delay_ms * (1 << attempt);
        delay = std::min(delay, max_delay_ms);
        
        // Add jitter
        int jitter = delay * 0.25;
        delay += (rand() % (2 * jitter + 1)) - jitter;
        
        // Wait before retry
        std::this_thread::sleep_for(std::chrono::milliseconds(delay));
        
        attempt++;
    }
    
    return arrow::Status::Invalid("Max retries exceeded");
}

bool ErrorRecoveryUtils::ValidateErrorContext(const ErrorContext& context) {
    return !context.component.empty() && !context.operation.empty();
}

std::string ErrorRecoveryUtils::FormatErrorMessage(
    ErrorType type,
    ErrorSeverity severity,
    const std::string& message,
    const ErrorContext& context
) {
    std::ostringstream oss;
    
    oss << "ErrorType=" << static_cast<int>(type)
        << " Severity=" << static_cast<int>(severity)
        << " Component=" << context.component
        << " Operation=" << context.operation;
    
    if (!context.execution_id.empty()) {
        oss << " ExecutionID=" << context.execution_id;
    }
    
    if (!context.operator_id.empty()) {
        oss << " OperatorID=" << context.operator_id;
    }
    
    if (!context.partition_id.empty()) {
        oss << " PartitionID=" << context.partition_id;
    }
    
    oss << " Message=" << message;
    
    return oss.str();
}

ErrorType ErrorRecoveryUtils::GetErrorTypeFromStatus(const arrow::Status& status) {
    if (status.IsIOError()) {
        return ErrorType::NETWORK_ERROR;
    } else if (status.IsInvalid()) {
        return ErrorType::VALIDATION_ERROR;
    } else if (status.IsNotImplemented()) {
        return ErrorType::OPERATOR_ERROR;
    } else if (status.IsKeyError()) {
        return ErrorType::STATE_ERROR;
    } else {
        return ErrorType::UNKNOWN_ERROR;
    }
}

ErrorSeverity ErrorRecoveryUtils::GetErrorSeverity(ErrorType type) {
    switch (type) {
        case ErrorType::CONNECTOR_ERROR:
        case ErrorType::NETWORK_ERROR:
            return ErrorSeverity::ERROR;
        case ErrorType::STATE_ERROR:
        case ErrorType::CHECKPOINT_ERROR:
            return ErrorSeverity::CRITICAL;
        case ErrorType::WATERMARK_ERROR:
        case ErrorType::OPERATOR_ERROR:
            return ErrorSeverity::WARNING;
        case ErrorType::ORCHESTRATOR_ERROR:
            return ErrorSeverity::ERROR;
        case ErrorType::TIMEOUT_ERROR:
            return ErrorSeverity::WARNING;
        case ErrorType::VALIDATION_ERROR:
            return ErrorSeverity::ERROR;
        case ErrorType::UNKNOWN_ERROR:
            return ErrorSeverity::CRITICAL;
        default:
            return ErrorSeverity::ERROR;
    }
}

} // namespace streaming
} // namespace sabot_sql
