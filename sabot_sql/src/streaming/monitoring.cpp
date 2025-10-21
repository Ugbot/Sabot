#include "sabot_sql/streaming/monitoring.h"
#include <iostream>
#include <sstream>
#include <algorithm>
#include <iomanip>
#include <random>

#ifdef __APPLE__
#include <mach/mach.h>
#include <sys/sysctl.h>
#elif __linux__
#include <sys/sysinfo.h>
#include <fstream>
#endif

namespace sabot_sql {
namespace streaming {

// ============================================================================
// DefaultMetricsCollector Implementation
// ============================================================================

DefaultMetricsCollector::DefaultMetricsCollector() = default;

void DefaultMetricsCollector::RecordCounter(const std::string& name, int64_t value) {
    std::lock_guard<std::mutex> lock(mutex_);
    counters_[name] += value;
}

void DefaultMetricsCollector::RecordGauge(const std::string& name, double value) {
    std::lock_guard<std::mutex> lock(mutex_);
    gauges_[name] = value;
}

void DefaultMetricsCollector::RecordHistogram(const std::string& name, double value) {
    std::lock_guard<std::mutex> lock(mutex_);
    histograms_[name].push_back(value);
}

void DefaultMetricsCollector::RecordTimer(const std::string& name, int64_t duration_ms) {
    std::lock_guard<std::mutex> lock(mutex_);
    timers_[name].push_back(duration_ms);
}

std::unordered_map<std::string, double> DefaultMetricsCollector::GetAllMetrics() const {
    std::lock_guard<std::mutex> lock(mutex_);
    std::unordered_map<std::string, double> metrics;
    
    // Add counters
    for (const auto& [name, counter] : counters_) {
        metrics[name] = counter.load();
    }
    
    // Add gauges
    for (const auto& [name, gauge] : gauges_) {
        metrics[name] = gauge.load();
    }
    
    // Add histogram statistics
    for (const auto& [name, values] : histograms_) {
        if (!values.empty()) {
            double sum = 0.0;
            double min_val = values[0];
            double max_val = values[0];
            
            for (double val : values) {
                sum += val;
                min_val = std::min(min_val, val);
                max_val = std::max(max_val, val);
            }
            
            metrics[name + "_count"] = values.size();
            metrics[name + "_sum"] = sum;
            metrics[name + "_avg"] = sum / values.size();
            metrics[name + "_min"] = min_val;
            metrics[name + "_max"] = max_val;
        }
    }
    
    // Add timer statistics
    for (const auto& [name, values] : timers_) {
        if (!values.empty()) {
            int64_t sum = 0;
            int64_t min_val = values[0];
            int64_t max_val = values[0];
            
            for (int64_t val : values) {
                sum += val;
                min_val = std::min(min_val, val);
                max_val = std::max(max_val, val);
            }
            
            metrics[name + "_count"] = values.size();
            metrics[name + "_sum"] = sum;
            metrics[name + "_avg"] = static_cast<double>(sum) / values.size();
            metrics[name + "_min"] = min_val;
            metrics[name + "_max"] = max_val;
        }
    }
    
    return metrics;
}

void DefaultMetricsCollector::Reset() {
    std::lock_guard<std::mutex> lock(mutex_);
    counters_.clear();
    gauges_.clear();
    histograms_.clear();
    timers_.clear();
}

double DefaultMetricsCollector::GetMetric(const std::string& name) const {
    std::lock_guard<std::mutex> lock(mutex_);
    
    // Check counters
    auto counter_it = counters_.find(name);
    if (counter_it != counters_.end()) {
        return counter_it->second.load();
    }
    
    // Check gauges
    auto gauge_it = gauges_.find(name);
    if (gauge_it != gauges_.end()) {
        return gauge_it->second.load();
    }
    
    return 0.0;
}

bool DefaultMetricsCollector::HasMetric(const std::string& name) const {
    std::lock_guard<std::mutex> lock(mutex_);
    return counters_.find(name) != counters_.end() ||
           gauges_.find(name) != gauges_.end() ||
           histograms_.find(name) != histograms_.end() ||
           timers_.find(name) != timers_.end();
}

// ============================================================================
// DefaultHealthChecker Implementation
// ============================================================================

DefaultHealthChecker::DefaultHealthChecker() = default;

arrow::Status DefaultHealthChecker::CheckHealth() {
    std::lock_guard<std::mutex> lock(mutex_);
    bool all_healthy = true;
    
    for (auto& [name, component] : components_) {
        if (component.check_func) {
            auto status = component.check_func();
            component.is_healthy = status.ok();
            component.last_check_time = std::chrono::duration_cast<std::chrono::milliseconds>(
                std::chrono::system_clock::now().time_since_epoch()
            ).count();
            
            if (!status.ok()) {
                component.last_error = status.message();
                all_healthy = false;
            }
        }
    }
    
    overall_healthy_ = all_healthy;
    
    if (all_healthy) {
        return arrow::Status::OK();
    } else {
        return arrow::Status::Invalid("One or more components are unhealthy");
    }
}

std::string DefaultHealthChecker::GetHealthStatus() const {
    return overall_healthy_ ? "HEALTHY" : "UNHEALTHY";
}

std::unordered_map<std::string, std::string> DefaultHealthChecker::GetHealthDetails() const {
    std::lock_guard<std::mutex> lock(mutex_);
    std::unordered_map<std::string, std::string> details;
    
    details["overall_status"] = GetHealthStatus();
    
    for (const auto& [name, component] : components_) {
        details[name + "_status"] = component.is_healthy ? "HEALTHY" : "UNHEALTHY";
        details[name + "_last_check"] = std::to_string(component.last_check_time);
        
        if (!component.is_healthy && !component.last_error.empty()) {
            details[name + "_error"] = component.last_error;
        }
    }
    
    return details;
}

void DefaultHealthChecker::RegisterComponent(
    const std::string& name,
    std::function<arrow::Status()> check_func
) {
    std::lock_guard<std::mutex> lock(mutex_);
    components_[name] = ComponentHealth{check_func, true, 0, ""};
}

void DefaultHealthChecker::UpdateComponentHealth(const std::string& name, bool is_healthy) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = components_.find(name);
    if (it != components_.end()) {
        it->second.is_healthy = is_healthy;
        it->second.last_check_time = std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::system_clock::now().time_since_epoch()
        ).count();
    }
}

// ============================================================================
// ObservabilityManager Implementation
// ============================================================================

ObservabilityManager::ObservabilityManager()
    : metrics_collector_(std::make_shared<DefaultMetricsCollector>())
    , health_checker_(std::make_shared<DefaultHealthChecker>())
    , streaming_metrics_(std::make_shared<StreamingMetrics>()) {
}

arrow::Status ObservabilityManager::Initialize(const std::string& execution_id) {
    std::lock_guard<std::mutex> lock(mutex_);
    
    if (initialized_) {
        return arrow::Status::Invalid("ObservabilityManager already initialized");
    }
    
    execution_id_ = execution_id;
    execution_start_time_ = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::system_clock::now().time_since_epoch()
    ).count();
    
    initialized_ = true;
    
    // Record initialization metrics
    metrics_collector_->RecordCounter("execution_started", 1);
    metrics_collector_->RecordGauge("execution_start_time", execution_start_time_);
    
    return arrow::Status::OK();
}

arrow::Status ObservabilityManager::Shutdown() {
    std::lock_guard<std::mutex> lock(mutex_);
    
    if (!initialized_) {
        return arrow::Status::Invalid("ObservabilityManager not initialized");
    }
    
    execution_end_time_ = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::system_clock::now().time_since_epoch()
    ).count();
    
    // Record shutdown metrics
    metrics_collector_->RecordCounter("execution_ended", 1);
    metrics_collector_->RecordGauge("execution_end_time", execution_end_time_);
    
    if (execution_start_time_ > 0) {
        int64_t duration = execution_end_time_ - execution_start_time_;
        metrics_collector_->RecordTimer("execution_duration", duration);
    }
    
    initialized_ = false;
    
    return arrow::Status::OK();
}

std::shared_ptr<MetricsCollector> ObservabilityManager::GetMetricsCollector() const {
    return metrics_collector_;
}

std::shared_ptr<HealthChecker> ObservabilityManager::GetHealthChecker() const {
    return health_checker_;
}

std::shared_ptr<StreamingMetrics> ObservabilityManager::GetStreamingMetrics() const {
    return streaming_metrics_;
}

void ObservabilityManager::RecordExecutionStart(const std::string& execution_id) {
    metrics_collector_->RecordCounter("execution_started", 1);
    metrics_collector_->RecordGauge("active_executions", 1);
}

void ObservabilityManager::RecordExecutionEnd(const std::string& execution_id, bool success) {
    metrics_collector_->RecordCounter("execution_ended", 1);
    metrics_collector_->RecordCounter(success ? "execution_success" : "execution_failure", 1);
    metrics_collector_->RecordGauge("active_executions", -1);
}

void ObservabilityManager::RecordBatchProcessed(int64_t batch_size, int64_t latency_ms) {
    streaming_metrics_->records_processed += batch_size;
    streaming_metrics_->batches_processed += 1;
    
    // Update latency metrics
    int64_t current_avg = streaming_metrics_->avg_batch_latency_ms.load();
    int64_t new_avg = (current_avg + latency_ms) / 2;
    streaming_metrics_->avg_batch_latency_ms = new_avg;
    
    int64_t current_max = streaming_metrics_->max_batch_latency_ms.load();
    if (latency_ms > current_max) {
        streaming_metrics_->max_batch_latency_ms = latency_ms;
    }
    
    streaming_metrics_->last_batch_time = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::system_clock::now().time_since_epoch()
    ).count();
    
    // Record metrics
    metrics_collector_->RecordCounter("batches_processed", 1);
    metrics_collector_->RecordCounter("records_processed", batch_size);
    metrics_collector_->RecordTimer("batch_latency", latency_ms);
}

void ObservabilityManager::RecordWindowEmitted(int64_t window_size, int64_t latency_ms) {
    streaming_metrics_->windows_emitted += 1;
    
    // Update latency metrics
    int64_t current_avg = streaming_metrics_->avg_window_latency_ms.load();
    int64_t new_avg = (current_avg + latency_ms) / 2;
    streaming_metrics_->avg_window_latency_ms = new_avg;
    
    int64_t current_max = streaming_metrics_->max_window_latency_ms.load();
    if (latency_ms > current_max) {
        streaming_metrics_->max_window_latency_ms = latency_ms;
    }
    
    streaming_metrics_->last_window_time = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::system_clock::now().time_since_epoch()
    ).count();
    
    // Record metrics
    metrics_collector_->RecordCounter("windows_emitted", 1);
    metrics_collector_->RecordTimer("window_latency", latency_ms);
}

void ObservabilityManager::RecordCheckpointCompleted(int64_t checkpoint_id, int64_t latency_ms) {
    streaming_metrics_->checkpoints_completed += 1;
    streaming_metrics_->last_checkpoint_time = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::system_clock::now().time_since_epoch()
    ).count();
    
    // Record metrics
    metrics_collector_->RecordCounter("checkpoints_completed", 1);
    metrics_collector_->RecordTimer("checkpoint_latency", latency_ms);
}

void ObservabilityManager::RecordError(const std::string& error_type, const std::string& message) {
    metrics_collector_->RecordCounter("errors_total", 1);
    metrics_collector_->RecordCounter("errors_" + error_type, 1);
    
    // Update specific error counters
    if (error_type == "connector") {
        streaming_metrics_->connector_errors += 1;
    } else if (error_type == "state") {
        streaming_metrics_->state_errors += 1;
    } else if (error_type == "watermark") {
        streaming_metrics_->watermark_errors += 1;
    } else if (error_type == "checkpoint") {
        streaming_metrics_->checkpoint_errors += 1;
    }
}

std::unordered_map<std::string, std::string> ObservabilityManager::GetExecutionSummary() const {
    std::unordered_map<std::string, std::string> summary;
    
    summary["execution_id"] = execution_id_;
    summary["initialized"] = initialized_ ? "true" : "false";
    summary["start_time"] = std::to_string(execution_start_time_);
    summary["end_time"] = std::to_string(execution_end_time_);
    summary["success"] = execution_success_ ? "true" : "false";
    
    if (execution_start_time_ > 0 && execution_end_time_ > 0) {
        int64_t duration = execution_end_time_ - execution_start_time_;
        summary["duration_ms"] = std::to_string(duration);
    }
    
    // Add streaming metrics
    summary["records_processed"] = std::to_string(streaming_metrics_->records_processed.load());
    summary["batches_processed"] = std::to_string(streaming_metrics_->batches_processed.load());
    summary["windows_emitted"] = std::to_string(streaming_metrics_->windows_emitted.load());
    summary["checkpoints_completed"] = std::to_string(streaming_metrics_->checkpoints_completed.load());
    
    return summary;
}

std::string ObservabilityManager::ExportMetricsToJson() const {
    auto metrics = metrics_collector_->GetAllMetrics();
    
    std::ostringstream oss;
    oss << "{\n";
    oss << "  \"execution_id\": \"" << execution_id_ << "\",\n";
    oss << "  \"timestamp\": " << std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::system_clock::now().time_since_epoch()
    ).count() << ",\n";
    oss << "  \"metrics\": {\n";
    
    bool first = true;
    for (const auto& [name, value] : metrics) {
        if (!first) oss << ",\n";
        oss << "    \"" << name << "\": " << std::fixed << std::setprecision(2) << value;
        first = false;
    }
    
    oss << "\n  }\n";
    oss << "}";
    
    return oss.str();
}

std::string ObservabilityManager::ExportHealthToJson() const {
    auto health_details = health_checker_->GetHealthDetails();
    
    std::ostringstream oss;
    oss << "{\n";
    oss << "  \"execution_id\": \"" << execution_id_ << "\",\n";
    oss << "  \"timestamp\": " << std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::system_clock::now().time_since_epoch()
    ).count() << ",\n";
    oss << "  \"health\": {\n";
    
    bool first = true;
    for (const auto& [name, value] : health_details) {
        if (!first) oss << ",\n";
        oss << "    \"" << name << "\": \"" << value << "\"";
        first = false;
    }
    
    oss << "\n  }\n";
    oss << "}";
    
    return oss.str();
}

// ============================================================================
// PerformanceProfiler Implementation
// ============================================================================

PerformanceProfiler::PerformanceProfiler() = default;

void PerformanceProfiler::StartProfile(const std::string& operation_name) {
    std::lock_guard<std::mutex> lock(mutex_);
    active_profiles_[operation_name] = {
        std::chrono::steady_clock::now(),
        true
    };
}

void PerformanceProfiler::EndProfile(const std::string& operation_name) {
    std::lock_guard<std::mutex> lock(mutex_);
    
    auto it = active_profiles_.find(operation_name);
    if (it != active_profiles_.end() && it->second.is_active) {
        auto end_time = std::chrono::steady_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::microseconds>(
            end_time - it->second.start_time
        ).count();
        
        double duration_ms = duration / 1000.0;
        
        // Update operation statistics
        auto& stats = operation_stats_[operation_name];
        stats.call_count++;
        stats.total_time_ms += duration_ms;
        stats.avg_time_ms = stats.total_time_ms / stats.call_count;
        
        if (stats.call_count == 1) {
            stats.min_time_ms = duration_ms;
            stats.max_time_ms = duration_ms;
        } else {
            stats.min_time_ms = std::min(stats.min_time_ms, duration_ms);
            stats.max_time_ms = std::max(stats.max_time_ms, duration_ms);
        }
        
        // Remove from active profiles
        active_profiles_.erase(it);
    }
}

std::unordered_map<std::string, double> PerformanceProfiler::GetResults() const {
    std::lock_guard<std::mutex> lock(mutex_);
    std::unordered_map<std::string, double> results;
    
    for (const auto& [name, stats] : operation_stats_) {
        results[name + "_call_count"] = stats.call_count;
        results[name + "_total_time_ms"] = stats.total_time_ms;
        results[name + "_avg_time_ms"] = stats.avg_time_ms;
        results[name + "_min_time_ms"] = stats.min_time_ms;
        results[name + "_max_time_ms"] = stats.max_time_ms;
    }
    
    return results;
}

void PerformanceProfiler::Reset() {
    std::lock_guard<std::mutex> lock(mutex_);
    active_profiles_.clear();
    operation_stats_.clear();
}

std::unordered_map<std::string, PerformanceProfiler::OperationStats> PerformanceProfiler::GetOperationStats() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return operation_stats_;
}

// ============================================================================
// MonitoringUtils Implementation
// ============================================================================

std::unordered_map<std::string, double> MonitoringUtils::GetSystemResourceUsage() {
    std::unordered_map<std::string, double> usage;
    
    usage["memory_mb"] = GetMemoryUsageMB();
    usage["cpu_percent"] = GetCpuUsagePercent();
    
    return usage;
}

double MonitoringUtils::GetMemoryUsageMB() {
#ifdef __APPLE__
    struct mach_task_basic_info info;
    mach_msg_type_number_t size = MACH_TASK_BASIC_INFO_COUNT;
    kern_return_t kerr = task_info(mach_task_self(), MACH_TASK_BASIC_INFO, (task_info_t)&info, &size);
    
    if (kerr == KERN_SUCCESS) {
        return info.resident_size / (1024.0 * 1024.0);
    }
#elif __linux__
    std::ifstream status("/proc/self/status");
    std::string line;
    
    while (std::getline(status, line)) {
        if (line.substr(0, 6) == "VmRSS:") {
            std::istringstream iss(line);
            std::string key, value, unit;
            iss >> key >> value >> unit;
            return std::stod(value) / 1024.0;  // Convert KB to MB
        }
    }
#endif
    
    return 0.0;
}

double MonitoringUtils::GetCpuUsagePercent() {
    // Simplified CPU usage calculation
    // In a real implementation, this would use more sophisticated methods
    static auto last_time = std::chrono::steady_clock::now();
    static auto last_cpu_time = std::chrono::duration_cast<std::chrono::microseconds>(
        std::chrono::steady_clock::now().time_since_epoch()
    ).count();
    
    auto current_time = std::chrono::steady_clock::now();
    auto current_cpu_time = std::chrono::duration_cast<std::chrono::microseconds>(
        current_time.time_since_epoch()
    ).count();
    
    auto time_diff = std::chrono::duration_cast<std::chrono::microseconds>(
        current_time - last_time
    ).count();
    
    auto cpu_diff = current_cpu_time - last_cpu_time;
    
    last_time = current_time;
    last_cpu_time = current_cpu_time;
    
    if (time_diff > 0) {
        return (cpu_diff * 100.0) / time_diff;
    }
    
    return 0.0;
}

std::string MonitoringUtils::FormatMetrics(const std::unordered_map<std::string, double>& metrics) {
    std::ostringstream oss;
    
    for (const auto& [name, value] : metrics) {
        oss << name << "=" << std::fixed << std::setprecision(2) << value << " ";
    }
    
    return oss.str();
}

std::string MonitoringUtils::CreateMetricsSummary(const StreamingMetrics& metrics) {
    std::ostringstream oss;
    
    oss << "StreamingMetrics Summary:\n";
    oss << "  Records Processed: " << metrics.records_processed.load() << "\n";
    oss << "  Batches Processed: " << metrics.batches_processed.load() << "\n";
    oss << "  Windows Emitted: " << metrics.windows_emitted.load() << "\n";
    oss << "  Checkpoints Completed: " << metrics.checkpoints_completed.load() << "\n";
    oss << "  Avg Batch Latency: " << metrics.avg_batch_latency_ms.load() << "ms\n";
    oss << "  Max Batch Latency: " << metrics.max_batch_latency_ms.load() << "ms\n";
    oss << "  Avg Window Latency: " << metrics.avg_window_latency_ms.load() << "ms\n";
    oss << "  Max Window Latency: " << metrics.max_window_latency_ms.load() << "ms\n";
    oss << "  Connector Errors: " << metrics.connector_errors.load() << "\n";
    oss << "  State Errors: " << metrics.state_errors.load() << "\n";
    oss << "  Watermark Errors: " << metrics.watermark_errors.load() << "\n";
    oss << "  Checkpoint Errors: " << metrics.checkpoint_errors.load() << "\n";
    oss << "  CPU Usage: " << std::fixed << std::setprecision(1) << metrics.cpu_usage_percent.load() << "%\n";
    oss << "  Memory Usage: " << std::fixed << std::setprecision(1) << metrics.memory_usage_mb.load() << "MB\n";
    
    return oss.str();
}

bool MonitoringUtils::ValidateMetrics(const std::unordered_map<std::string, double>& metrics) {
    for (const auto& [name, value] : metrics) {
        if (std::isnan(value) || std::isinf(value)) {
            return false;
        }
    }
    return true;
}

} // namespace streaming
} // namespace sabot_sql
