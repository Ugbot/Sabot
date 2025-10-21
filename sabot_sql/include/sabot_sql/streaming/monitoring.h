#pragma once

#include <arrow/api.h>
#include <string>
#include <memory>
#include <unordered_map>
#include <vector>
#include <chrono>
#include <atomic>
#include <mutex>

namespace sabot_sql {
namespace streaming {

/**
 * Metrics collection for streaming SQL operations
 */
class MetricsCollector {
public:
    virtual ~MetricsCollector() = default;
    
    /**
     * Record a counter metric
     */
    virtual void RecordCounter(const std::string& name, int64_t value = 1) = 0;
    
    /**
     * Record a gauge metric
     */
    virtual void RecordGauge(const std::string& name, double value) = 0;
    
    /**
     * Record a histogram metric
     */
    virtual void RecordHistogram(const std::string& name, double value) = 0;
    
    /**
     * Record a timer metric
     */
    virtual void RecordTimer(const std::string& name, int64_t duration_ms) = 0;
    
    /**
     * Get all metrics as a map
     */
    virtual std::unordered_map<std::string, double> GetAllMetrics() const = 0;
    
    /**
     * Reset all metrics
     */
    virtual void Reset() = 0;
};

/**
 * Default metrics collector implementation
 */
class DefaultMetricsCollector : public MetricsCollector {
public:
    DefaultMetricsCollector();
    ~DefaultMetricsCollector() override = default;
    
    void RecordCounter(const std::string& name, int64_t value = 1) override;
    void RecordGauge(const std::string& name, double value) override;
    void RecordHistogram(const std::string& name, double value) override;
    void RecordTimer(const std::string& name, int64_t duration_ms) override;
    
    std::unordered_map<std::string, double> GetAllMetrics() const override;
    void Reset() override;
    
    /**
     * Get specific metric value
     */
    double GetMetric(const std::string& name) const;
    
    /**
     * Check if metric exists
     */
    bool HasMetric(const std::string& name) const;
    
private:
    mutable std::mutex mutex_;
    std::unordered_map<std::string, std::atomic<int64_t>> counters_;
    std::unordered_map<std::string, std::atomic<double>> gauges_;
    std::unordered_map<std::string, std::vector<double>> histograms_;
    std::unordered_map<std::string, std::vector<int64_t>> timers_;
};

/**
 * Performance metrics for streaming operations
 */
struct StreamingMetrics {
    // Throughput metrics
    std::atomic<int64_t> records_processed{0};
    std::atomic<int64_t> batches_processed{0};
    std::atomic<int64_t> windows_emitted{0};
    std::atomic<int64_t> checkpoints_completed{0};
    
    // Latency metrics
    std::atomic<int64_t> avg_batch_latency_ms{0};
    std::atomic<int64_t> max_batch_latency_ms{0};
    std::atomic<int64_t> avg_window_latency_ms{0};
    std::atomic<int64_t> max_window_latency_ms{0};
    
    // Error metrics
    std::atomic<int64_t> connector_errors{0};
    std::atomic<int64_t> state_errors{0};
    std::atomic<int64_t> watermark_errors{0};
    std::atomic<int64_t> checkpoint_errors{0};
    
    // Resource metrics
    std::atomic<double> cpu_usage_percent{0.0};
    std::atomic<double> memory_usage_mb{0.0};
    std::atomic<int64_t> active_connections{0};
    std::atomic<int64_t> queued_batches{0};
    
    // Timestamps
    std::atomic<int64_t> last_batch_time{0};
    std::atomic<int64_t> last_window_time{0};
    std::atomic<int64_t> last_checkpoint_time{0};
    std::atomic<int64_t> start_time{0};
    
    StreamingMetrics() {
        start_time = std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::system_clock::now().time_since_epoch()
        ).count();
    }
};

/**
 * Health check interface
 */
class HealthChecker {
public:
    virtual ~HealthChecker() = default;
    
    /**
     * Perform health check
     */
    virtual arrow::Status CheckHealth() = 0;
    
    /**
     * Get health status
     */
    virtual std::string GetHealthStatus() const = 0;
    
    /**
     * Get detailed health information
     */
    virtual std::unordered_map<std::string, std::string> GetHealthDetails() const = 0;
};

/**
 * Default health checker implementation
 */
class DefaultHealthChecker : public HealthChecker {
public:
    DefaultHealthChecker();
    ~DefaultHealthChecker() override = default;
    
    arrow::Status CheckHealth() override;
    std::string GetHealthStatus() const override;
    std::unordered_map<std::string, std::string> GetHealthDetails() const override;
    
    /**
     * Register health check component
     */
    void RegisterComponent(
        const std::string& name,
        std::function<arrow::Status()> check_func
    );
    
    /**
     * Update component health
     */
    void UpdateComponentHealth(const std::string& name, bool is_healthy);
    
private:
    struct ComponentHealth {
        std::function<arrow::Status()> check_func;
        bool is_healthy = true;
        int64_t last_check_time = 0;
        std::string last_error;
    };
    
    mutable std::mutex mutex_;
    std::unordered_map<std::string, ComponentHealth> components_;
    std::atomic<bool> overall_healthy_{true};
};

/**
 * Observability manager for streaming SQL
 */
class ObservabilityManager {
public:
    ObservabilityManager();
    ~ObservabilityManager() = default;
    
    /**
     * Initialize observability
     */
    arrow::Status Initialize(const std::string& execution_id);
    
    /**
     * Shutdown observability
     */
    arrow::Status Shutdown();
    
    /**
     * Get metrics collector
     */
    std::shared_ptr<MetricsCollector> GetMetricsCollector() const;
    
    /**
     * Get health checker
     */
    std::shared_ptr<HealthChecker> GetHealthChecker() const;
    
    /**
     * Get streaming metrics
     */
    std::shared_ptr<StreamingMetrics> GetStreamingMetrics() const;
    
    /**
     * Record execution start
     */
    void RecordExecutionStart(const std::string& execution_id);
    
    /**
     * Record execution end
     */
    void RecordExecutionEnd(const std::string& execution_id, bool success);
    
    /**
     * Record batch processing
     */
    void RecordBatchProcessed(int64_t batch_size, int64_t latency_ms);
    
    /**
     * Record window emission
     */
    void RecordWindowEmitted(int64_t window_size, int64_t latency_ms);
    
    /**
     * Record checkpoint completion
     */
    void RecordCheckpointCompleted(int64_t checkpoint_id, int64_t latency_ms);
    
    /**
     * Record error
     */
    void RecordError(const std::string& error_type, const std::string& message);
    
    /**
     * Get execution summary
     */
    std::unordered_map<std::string, std::string> GetExecutionSummary() const;
    
    /**
     * Export metrics to JSON
     */
    std::string ExportMetricsToJson() const;
    
    /**
     * Export health status to JSON
     */
    std::string ExportHealthToJson() const;
    
private:
    std::shared_ptr<DefaultMetricsCollector> metrics_collector_;
    std::shared_ptr<DefaultHealthChecker> health_checker_;
    std::shared_ptr<StreamingMetrics> streaming_metrics_;
    
    std::string execution_id_;
    std::atomic<bool> initialized_{false};
    std::atomic<int64_t> execution_start_time_{0};
    std::atomic<int64_t> execution_end_time_{0};
    std::atomic<bool> execution_success_{false};
    
    mutable std::mutex mutex_;
};

/**
 * Performance profiler for streaming operations
 */
class PerformanceProfiler {
public:
    PerformanceProfiler();
    ~PerformanceProfiler() = default;
    
    /**
     * Start profiling an operation
     */
    void StartProfile(const std::string& operation_name);
    
    /**
     * End profiling an operation
     */
    void EndProfile(const std::string& operation_name);
    
    /**
     * Get profiling results
     */
    std::unordered_map<std::string, double> GetResults() const;
    
    /**
     * Reset profiling data
     */
    void Reset();
    
    /**
     * Get operation statistics
     */
    struct OperationStats {
        int64_t call_count = 0;
        double total_time_ms = 0.0;
        double avg_time_ms = 0.0;
        double min_time_ms = 0.0;
        double max_time_ms = 0.0;
    };
    
    std::unordered_map<std::string, OperationStats> GetOperationStats() const;
    
private:
    struct ProfileData {
        std::chrono::steady_clock::time_point start_time;
        bool is_active = false;
    };
    
    mutable std::mutex mutex_;
    std::unordered_map<std::string, ProfileData> active_profiles_;
    std::unordered_map<std::string, OperationStats> operation_stats_;
};

/**
 * Monitoring utilities
 */
class MonitoringUtils {
public:
    /**
     * Get system resource usage
     */
    static std::unordered_map<std::string, double> GetSystemResourceUsage();
    
    /**
     * Get memory usage in MB
     */
    static double GetMemoryUsageMB();
    
    /**
     * Get CPU usage percentage
     */
    static double GetCpuUsagePercent();
    
    /**
     * Format metrics for logging
     */
    static std::string FormatMetrics(const std::unordered_map<std::string, double>& metrics);
    
    /**
     * Create metrics summary
     */
    static std::string CreateMetricsSummary(const StreamingMetrics& metrics);
    
    /**
     * Validate metrics data
     */
    static bool ValidateMetrics(const std::unordered_map<std::string, double>& metrics);
};

} // namespace streaming
} // namespace sabot_sql
