#include "marble/metrics.h"
#include <algorithm>
#include <sstream>
#include <iomanip>
#include <iostream>
#include <thread>
#include <nlohmann/json.hpp>

namespace marble {

// Global instances
std::unique_ptr<MetricsCollector> global_metrics_collector;
std::unique_ptr<Logger> global_logger;

void initializeMetrics() {
    global_metrics_collector = std::make_unique<MetricsCollector>();
}

void initializeLogging(Logger::Level level) {
    global_logger = std::make_unique<StandardLogger>(level);
}

void shutdownMonitoring() {
    global_logger.reset();
    global_metrics_collector.reset();
}

// MetricsCollector implementation
MetricsCollector::MetricsCollector() = default;

void MetricsCollector::incrementCounter(const std::string& name, int64_t value) {
    std::lock_guard<std::mutex> lock(mutex_);
    counters_[name].fetch_add(value, std::memory_order_relaxed);
}

void MetricsCollector::setGauge(const std::string& name, int64_t value) {
    std::lock_guard<std::mutex> lock(mutex_);
    gauges_[name].store(value, std::memory_order_relaxed);
}

void MetricsCollector::recordHistogram(const std::string& name, int64_t value) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto& hist = histograms_[name];
    hist.push_back(value);

    // Keep only recent values for memory efficiency
    if (hist.size() > HISTOGRAM_BUCKETS) {
        hist.erase(hist.begin());
    }
}

void MetricsCollector::recordTimer(const std::string& name, std::chrono::milliseconds duration) {
    recordHistogram(name, duration.count());
}

int64_t MetricsCollector::getCounter(const std::string& name) const {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = counters_.find(name);
    return it != counters_.end() ? it->second.load(std::memory_order_relaxed) : 0;
}

int64_t MetricsCollector::getGauge(const std::string& name) const {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = gauges_.find(name);
    return it != gauges_.end() ? it->second.load(std::memory_order_relaxed) : 0;
}

std::vector<int64_t> MetricsCollector::getHistogram(const std::string& name) const {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = histograms_.find(name);
    return it != histograms_.end() ? it->second : std::vector<int64_t>();
}

void MetricsCollector::recordHealthCheck(const std::string& component, bool healthy) {
    std::lock_guard<std::mutex> lock(mutex_);
    health_checks_[component] = healthy;
}

bool MetricsCollector::isHealthy(const std::string& component) const {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = health_checks_.find(component);
    return it != health_checks_.end() ? it->second : false;
}

std::unordered_map<std::string, bool> MetricsCollector::getAllHealthChecks() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return health_checks_;
}

std::string MetricsCollector::exportPrometheus() const {
    std::lock_guard<std::mutex> lock(mutex_);
    std::stringstream ss;

    // Counters
    for (const auto& [name, value] : counters_) {
        ss << "# TYPE " << name << " counter\n";
        ss << name << " " << value.load(std::memory_order_relaxed) << "\n";
    }

    // Gauges
    for (const auto& [name, value] : gauges_) {
        ss << "# TYPE " << name << " gauge\n";
        ss << name << " " << value.load(std::memory_order_relaxed) << "\n";
    }

    // Histograms (simplified)
    for (const auto& [name, values] : histograms_) {
        if (!values.empty()) {
            ss << "# TYPE " << name << " histogram\n";
            ss << name << "_count " << values.size() << "\n";
            ss << name << "_sum " << std::accumulate(values.begin(), values.end(), 0LL) << "\n";

            // Simple percentiles
            std::vector<int64_t> sorted = values;
            std::sort(sorted.begin(), sorted.end());
            size_t p50_idx = sorted.size() * 50 / 100;
            size_t p95_idx = sorted.size() * 95 / 100;
            size_t p99_idx = sorted.size() * 99 / 100;

            if (p50_idx < sorted.size()) ss << name << "_50 " << sorted[p50_idx] << "\n";
            if (p95_idx < sorted.size()) ss << name << "_95 " << sorted[p95_idx] << "\n";
            if (p99_idx < sorted.size()) ss << name << "_99 " << sorted[p99_idx] << "\n";
        }
    }

    return ss.str();
}

std::string MetricsCollector::exportJSON() const {
    std::lock_guard<std::mutex> lock(mutex_);

    nlohmann::json json;

    // Counters
    nlohmann::json counters_json;
    for (const auto& [name, value] : counters_) {
        counters_json[name] = value.load(std::memory_order_relaxed);
    }
    json["counters"] = counters_json;

    // Gauges
    nlohmann::json gauges_json;
    for (const auto& [name, value] : gauges_) {
        gauges_json[name] = value.load(std::memory_order_relaxed);
    }
    json["gauges"] = gauges_json;

    // Histograms
    nlohmann::json histograms_json;
    for (const auto& [name, values] : histograms_) {
        nlohmann::json hist_json;
        hist_json["count"] = values.size();
        hist_json["sum"] = std::accumulate(values.begin(), values.end(), 0LL);

        if (!values.empty()) {
            std::vector<int64_t> sorted = values;
            std::sort(sorted.begin(), sorted.end());
            hist_json["min"] = sorted.front();
            hist_json["max"] = sorted.back();
            hist_json["median"] = sorted[sorted.size() / 2];

            size_t p95_idx = sorted.size() * 95 / 100;
            if (p95_idx < sorted.size()) hist_json["p95"] = sorted[p95_idx];

            size_t p99_idx = sorted.size() * 99 / 100;
            if (p99_idx < sorted.size()) hist_json["p99"] = sorted[p99_idx];
        }

        histograms_json[name] = hist_json;
    }
    json["histograms"] = histograms_json;

    // Health checks
    json["health_checks"] = health_checks_;

    json["timestamp"] = std::chrono::duration_cast<std::chrono::seconds>(
        std::chrono::system_clock::now().time_since_epoch()).count();

    return json.dump(2);
}

void MetricsCollector::reset() {
    std::lock_guard<std::mutex> lock(mutex_);
    counters_.clear();
    gauges_.clear();
    histograms_.clear();
    health_checks_.clear();
}

// MetricsCollector::Timer implementation
MetricsCollector::Timer::Timer(MetricsCollector& collector, const std::string& metric_name)
    : collector_(collector), metric_name_(metric_name),
      start_time_(std::chrono::steady_clock::now()) {}

MetricsCollector::Timer::~Timer() {
    auto end_time = std::chrono::steady_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time_);
    collector_.recordTimer(metric_name_, duration);
}

// StandardLogger implementation
StandardLogger::StandardLogger(Level min_level) : min_level_(min_level) {}

void StandardLogger::log(Level level, const std::string& component,
                        const std::string& message, const std::string& details) {
    if (level < min_level_) return;

    std::lock_guard<std::mutex> lock(mutex_);

    auto now = std::chrono::system_clock::now();
    std::stringstream ss;

    ss << "[" << formatTimestamp(now) << "] "
       << "[" << levelToString(level) << "] "
       << "[" << component << "] "
       << "[" << std::this_thread::get_id() << "] "
       << message;

    if (!details.empty()) {
        ss << " | " << details;
    }

    std::cout << ss.str() << std::endl;

    // Also log errors to stderr for visibility
    if (level >= Level::ERROR) {
        std::cerr << ss.str() << std::endl;
    }
}

std::string StandardLogger::levelToString(Level level) const {
    switch (level) {
        case Level::TRACE: return "TRACE";
        case Level::DEBUG: return "DEBUG";
        case Level::INFO: return "INFO";
        case Level::WARN: return "WARN";
        case Level::ERROR: return "ERROR";
        case Level::FATAL: return "FATAL";
        default: return "UNKNOWN";
    }
}

std::string StandardLogger::formatTimestamp(std::chrono::system_clock::time_point ts) const {
    auto time_t = std::chrono::system_clock::to_time_t(ts);
    auto tm = *std::localtime(&time_t);

    std::stringstream ss;
    ss << std::put_time(&tm, "%Y-%m-%d %H:%M:%S");

    auto duration = ts.time_since_epoch();
    auto millis = std::chrono::duration_cast<std::chrono::milliseconds>(duration).count() % 1000;
    ss << "." << std::setfill('0') << std::setw(3) << millis;

    return ss.str();
}

// ErrorContext implementation
ErrorContext::ErrorContext(const std::string& operation, const std::string& component)
    : operation_(operation), component_(component), error_code_(0),
      timestamp_(std::chrono::system_clock::now()) {}

void ErrorContext::addDetail(const std::string& key, const std::string& value) {
    details_[key] = value;
}

void ErrorContext::addDetail(const std::string& key, int64_t value) {
    details_[key] = std::to_string(value);
}

std::string ErrorContext::toString() const {
    nlohmann::json json;
    json["operation"] = operation_;
    json["component"] = component_;
    json["error_code"] = error_code_;
    json["timestamp"] = std::chrono::duration_cast<std::chrono::seconds>(
        timestamp_.time_since_epoch()).count();
    json["details"] = details_;

    return json.dump();
}

// StatusWithMetrics implementation
StatusWithMetrics::StatusWithMetrics() : code_(Code::OK) {}

StatusWithMetrics::StatusWithMetrics(Code code, const std::string& message)
    : code_(code), message_(message) {}

StatusWithMetrics::StatusWithMetrics(Code code, const std::string& message,
                                   std::unique_ptr<ErrorContext> context)
    : code_(code), message_(message), context_(std::move(context)) {}

std::string StatusWithMetrics::toString() const {
    std::stringstream ss;
    ss << "Status(";

    switch (code_) {
        case Code::OK: ss << "OK"; break;
        case Code::NOT_FOUND: ss << "NOT_FOUND"; break;
        case Code::CORRUPTION: ss << "CORRUPTION"; break;
        case Code::NOT_SUPPORTED: ss << "NOT_SUPPORTED"; break;
        case Code::INVALID_ARGUMENT: ss << "INVALID_ARGUMENT"; break;
        case Code::IO_ERROR: ss << "IO_ERROR"; break;
        case Code::NETWORK_ERROR: ss << "NETWORK_ERROR"; break;
        case Code::TIMEOUT: ss << "TIMEOUT"; break;
        case Code::BUSY: ss << "BUSY"; break;
        case Code::RUNTIME_ERROR: ss << "RUNTIME_ERROR"; break;
        case Code::ABORTED: ss << "ABORTED"; break;
        case Code::UNKNOWN: ss << "UNKNOWN"; break;
    }

    if (!message_.empty()) {
        ss << ": " << message_;
    }

    if (context_) {
        ss << " [" << context_->toString() << "]";
    }

    ss << ")";
    return ss.str();
}

void StatusWithMetrics::recordMetrics(MetricsCollector& collector, const std::string& operation) const {
    if (!ok()) {
        collector.incrementCounter(MetricsCollector::ERROR_COUNT);
        MARBLE_LOG_ERROR("StatusWithMetrics", "Operation failed: " + toString());
    }

    // Record operation-specific metrics
    if (operation == "read") {
        collector.incrementCounter(MetricsCollector::READ_OPERATIONS);
    } else if (operation == "write") {
        collector.incrementCounter(MetricsCollector::WRITE_OPERATIONS);
    } else if (operation == "raft") {
        collector.incrementCounter(MetricsCollector::RAFT_OPERATIONS);
    } else if (operation == "triple_store") {
        collector.incrementCounter(MetricsCollector::TRIPLE_STORE_QUERIES);
    }
}

// Factory methods
StatusWithMetrics StatusWithMetrics::OK() {
    return StatusWithMetrics(Code::OK, "");
}

StatusWithMetrics StatusWithMetrics::NotFound(const std::string& message) {
    return StatusWithMetrics(Code::NOT_FOUND, message);
}

StatusWithMetrics StatusWithMetrics::Corruption(const std::string& message) {
    return StatusWithMetrics(Code::CORRUPTION, message);
}

StatusWithMetrics StatusWithMetrics::NotSupported(const std::string& message) {
    return StatusWithMetrics(Code::NOT_SUPPORTED, message);
}

StatusWithMetrics StatusWithMetrics::InvalidArgument(const std::string& message) {
    return StatusWithMetrics(Code::INVALID_ARGUMENT, message);
}

StatusWithMetrics StatusWithMetrics::IOError(const std::string& message) {
    return StatusWithMetrics(Code::IO_ERROR, message);
}

StatusWithMetrics StatusWithMetrics::NetworkError(const std::string& message) {
    return StatusWithMetrics(Code::NETWORK_ERROR, message);
}

StatusWithMetrics StatusWithMetrics::Timeout(const std::string& message) {
    return StatusWithMetrics(Code::TIMEOUT, message);
}

StatusWithMetrics StatusWithMetrics::Busy(const std::string& message) {
    return StatusWithMetrics(Code::BUSY, message);
}

StatusWithMetrics StatusWithMetrics::RuntimeError(const std::string& message) {
    return StatusWithMetrics(Code::RUNTIME_ERROR, message);
}

StatusWithMetrics StatusWithMetrics::Aborted(const std::string& message) {
    return StatusWithMetrics(Code::ABORTED, message);
}

StatusWithMetrics StatusWithMetrics::Unknown(const std::string& message) {
    return StatusWithMetrics(Code::UNKNOWN, message);
}

}  // namespace marble
