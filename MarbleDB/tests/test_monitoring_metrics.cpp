/**
 * MarbleDB Monitoring and Metrics Test Suite
 *
 * Tests comprehensive monitoring, metrics collection, error handling,
 * and observability features for production deployments.
 */

#include <gtest/gtest.h>
#include <gmock/gmock.h>

#include <marble/metrics.h>
#include <marble/status.h>
#include <marble/db.h>

#include <thread>
#include <chrono>
#include <filesystem>

namespace marble {
namespace test {

using ::testing::_;
using ::testing::Return;
using ::testing::Invoke;
using ::testing::NiceMock;

// Mock Logger for testing
class MockLogger : public Logger {
public:
    MOCK_METHOD(void, log, (Level level, const std::string& component,
                           const std::string& message, const std::string& details), (override));
};

// Mock HealthCheck for testing
class MockHealthCheck : public HealthCheck {
public:
    MOCK_METHOD(bool, checkHealth, (), (const override));
    MOCK_METHOD(std::string, getComponentName, (), (const override));
    MOCK_METHOD(std::string, getHealthDetails, (), (const override));
};

class MonitoringMetricsTest : public ::testing::Test {
protected:
    void SetUp() override {
        // Initialize global monitoring
        initializeMetrics();
        initializeLogging(Logger::Level::DEBUG);

        // Create a fresh metrics collector for each test
        metrics_ = std::make_shared<MetricsCollector>();
    }

    void TearDown() override {
        // Clean up global state
        shutdownMonitoring();
    }

    std::shared_ptr<MetricsCollector> metrics_;
};

/**
 * Test MetricsCollector basic functionality
 */
TEST_F(MonitoringMetricsTest, MetricsCollectorBasicOperations) {
    // Test counter operations
    metrics_->incrementCounter("test_counter", 5);
    EXPECT_EQ(metrics_->getCounter("test_counter"), 5);

    metrics_->incrementCounter("test_counter", 3);
    EXPECT_EQ(metrics_->getCounter("test_counter"), 8);

    // Test gauge operations
    metrics_->setGauge("test_gauge", 42);
    EXPECT_EQ(metrics_->getGauge("test_gauge"), 42);

    metrics_->setGauge("test_gauge", 100);
    EXPECT_EQ(metrics_->getGauge("test_gauge"), 100);

    // Test histogram operations
    metrics_->recordHistogram("test_histogram", 10);
    metrics_->recordHistogram("test_histogram", 20);
    metrics_->recordHistogram("test_histogram", 30);

    auto hist = metrics_->getHistogram("test_histogram");
    EXPECT_EQ(hist.size(), 3);
    EXPECT_EQ(hist[0], 10);
    EXPECT_EQ(hist[1], 20);
    EXPECT_EQ(hist[2], 30);
}

/**
 * Test MetricsCollector timer functionality
 */
TEST_F(MonitoringMetricsTest, MetricsCollectorTimer) {
    // Test timer RAII wrapper
    {
        MetricsCollector::Timer timer(*metrics_, "test_timer");
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
        // Timer automatically records duration when it goes out of scope
    }

    auto hist = metrics_->getHistogram("test_timer");
    EXPECT_EQ(hist.size(), 1);
    EXPECT_GE(hist[0], 10);  // Should be at least 10ms
}

/**
 * Test health check functionality
 */
TEST_F(MonitoringMetricsTest, HealthChecks) {
    metrics_->recordHealthCheck("storage", true);
    metrics_->recordHealthCheck("network", false);
    metrics_->recordHealthCheck("cache", true);

    EXPECT_TRUE(metrics_->isHealthy("storage"));
    EXPECT_FALSE(metrics_->isHealthy("network"));
    EXPECT_TRUE(metrics_->isHealthy("cache"));
    EXPECT_FALSE(metrics_->isHealthy("nonexistent"));

    auto health_status = metrics_->getAllHealthChecks();
    EXPECT_EQ(health_status.size(), 3);
    EXPECT_TRUE(health_status["storage"]);
    EXPECT_FALSE(health_status["network"]);
    EXPECT_TRUE(health_status["cache"]);
}

/**
 * Test Prometheus metrics export
 */
TEST_F(MonitoringMetricsTest, PrometheusExport) {
    // Setup some test data
    metrics_->incrementCounter("requests_total", 100);
    metrics_->setGauge("active_connections", 5);
    metrics_->recordHistogram("request_duration_ms", 50);
    metrics_->recordHistogram("request_duration_ms", 75);
    metrics_->recordHealthCheck("database", true);

    std::string prometheus_output = metrics_->exportPrometheus();

    // Check that output contains expected metric types
    EXPECT_THAT(prometheus_output, ::testing::HasSubstr("# TYPE requests_total counter"));
    EXPECT_THAT(prometheus_output, ::testing::HasSubstr("requests_total 100"));
    EXPECT_THAT(prometheus_output, ::testing::HasSubstr("# TYPE active_connections gauge"));
    EXPECT_THAT(prometheus_output, ::testing::HasSubstr("active_connections 5"));
    EXPECT_THAT(prometheus_output, ::testing::HasSubstr("# TYPE request_duration_ms histogram"));
}

/**
 * Test JSON metrics export
 */
TEST_F(MonitoringMetricsTest, JSONExport) {
    // Setup some test data
    metrics_->incrementCounter("requests_total", 100);
    metrics_->setGauge("active_connections", 5);
    metrics_->recordHistogram("request_duration_ms", 50);
    metrics_->recordHealthCheck("database", true);

    std::string json_output = metrics_->exportJSON();

    // Parse JSON and verify structure
    nlohmann::json parsed = nlohmann::json::parse(json_output);

    EXPECT_TRUE(parsed.contains("counters"));
    EXPECT_TRUE(parsed.contains("gauges"));
    EXPECT_TRUE(parsed.contains("histograms"));
    EXPECT_TRUE(parsed.contains("health_checks"));
    EXPECT_TRUE(parsed.contains("timestamp"));

    EXPECT_EQ(parsed["counters"]["requests_total"], 100);
    EXPECT_EQ(parsed["gauges"]["active_connections"], 5);
    EXPECT_TRUE(parsed["health_checks"]["database"]);
}

/**
 * Test ErrorContext functionality
 */
TEST_F(MonitoringMetricsTest, ErrorContext) {
    ErrorContext context("database_operation", "storage_engine");

    context.addDetail("file_path", "/data/db/file.sst");
    context.addDetail("operation_offset", 1024LL);
    context.addDetail("error_code", 42);

    std::string context_str = context.toString();
    nlohmann::json parsed = nlohmann::json::parse(context_str);

    EXPECT_EQ(parsed["operation"], "database_operation");
    EXPECT_EQ(parsed["component"], "storage_engine");
    EXPECT_EQ(parsed["error_code"], 42);
    EXPECT_EQ(parsed["details"]["file_path"], "/data/db/file.sst");
    EXPECT_EQ(parsed["details"]["operation_offset"], 1024);
}

/**
 * Test StatusWithMetrics functionality
 */
TEST_F(MonitoringMetricsTest, StatusWithMetrics) {
    // Test OK status
    auto ok_status = StatusWithMetrics::OK();
    EXPECT_TRUE(ok_status.ok());
    EXPECT_EQ(ok_status.code(), StatusWithMetrics::Code::OK);

    // Test error status with context
    auto context = std::make_unique<ErrorContext>("file_read", "io_manager");
    context->addDetail("file_path", "/data/db/manifest");
    context->setErrorCode(13);

    auto error_status = StatusWithMetrics(
        StatusWithMetrics::Code::IO_ERROR,
        "Failed to read manifest file",
        std::move(context)
    );

    EXPECT_FALSE(error_status.ok());
    EXPECT_EQ(error_status.code(), StatusWithMetrics::Code::IO_ERROR);
    EXPECT_EQ(error_status.message(), "Failed to read manifest file");
    EXPECT_NE(error_status.context(), nullptr);

    // Test string representation
    std::string status_str = error_status.toString();
    EXPECT_THAT(status_str, ::testing::HasSubstr("IO_ERROR"));
    EXPECT_THAT(status_str, ::testing::HasSubstr("Failed to read manifest file"));

    // Test metrics recording
    error_status.recordMetrics(*metrics_, "read");
    EXPECT_EQ(metrics_->getCounter(MetricsCollector::ERROR_COUNT), 1);
    EXPECT_EQ(metrics_->getCounter(MetricsCollector::READ_OPERATIONS), 1);
}

/**
 * Test Logger functionality
 */
TEST_F(MonitoringMetricsTest, Logger) {
    MockLogger mock_logger;

    // Expect specific log calls
    EXPECT_CALL(mock_logger, log(Logger::Level::INFO, "test_component", "test message", ""))
        .Times(1);

    EXPECT_CALL(mock_logger, log(Logger::Level::ERROR, "test_component", "error occurred", "details"))
        .Times(1);

    // Test logging methods
    mock_logger.info("test_component", "test message");
    mock_logger.error("test_component", "error occurred", "details");
}

/**
 * Test StandardLogger implementation
 */
TEST_F(MonitoringMetricsTest, StandardLogger) {
    StandardLogger logger(Logger::Level::INFO);

    // This should not crash - testing basic functionality
    logger.info("test_component", "test message");
    logger.warn("test_component", "warning message", "with details");
    logger.error("test_component", "error message");

    // Test level filtering
    logger.setMinLevel(Logger::Level::ERROR);
    // INFO and WARN should be filtered out
    logger.info("test_component", "filtered message");  // Should not log
    logger.error("test_component", "error message");    // Should log
}

/**
 * Test global monitoring functions
 */
TEST_F(MonitoringMetricsTest, GlobalMonitoringFunctions) {
    // Test initialization
    initializeMetrics();
    initializeLogging(Logger::Level::INFO);

    EXPECT_NE(global_metrics_collector, nullptr);
    EXPECT_NE(global_logger, nullptr);

    // Test metrics macros
    MARBLE_METRICS_INCREMENT("global_test_counter");
    EXPECT_EQ(global_metrics_collector->getCounter("global_test_counter"), 1);

    MARBLE_METRICS_SET_GAUGE("global_test_gauge", 42);
    EXPECT_EQ(global_metrics_collector->getGauge("global_test_gauge"), 42);

    // Test logging macros
    MARBLE_LOG_INFO("test_component", "global logging test");

    // Test timer macro
    {
        MARBLE_METRICS_TIMER("global_test_timer");
        std::this_thread::sleep_for(std::chrono::milliseconds(5));
    }

    auto hist = global_metrics_collector->getHistogram("global_test_timer");
    EXPECT_EQ(hist.size(), 1);
    EXPECT_GE(hist[0], 5);

    // Test cleanup
    shutdownMonitoring();
    EXPECT_EQ(global_metrics_collector, nullptr);
    EXPECT_EQ(global_logger, nullptr);
}

/**
 * Test concurrent metrics access
 */
TEST_F(MonitoringMetricsTest, ConcurrentMetricsAccess) {
    const int num_threads = 10;
    const int increments_per_thread = 100;

    std::vector<std::thread> threads;

    // Launch multiple threads incrementing the same counter
    for (int i = 0; i < num_threads; ++i) {
        threads.emplace_back([this]() {
            for (int j = 0; j < increments_per_thread; ++j) {
                metrics_->incrementCounter("concurrent_counter");
                std::this_thread::sleep_for(std::chrono::microseconds(1));  // Small delay
            }
        });
    }

    // Wait for all threads to complete
    for (auto& thread : threads) {
        thread.join();
    }

    // Verify final counter value
    EXPECT_EQ(metrics_->getCounter("concurrent_counter"),
              num_threads * increments_per_thread);
}

/**
 * Test comprehensive health check scenario
 */
TEST_F(MonitoringMetricsTest, ComprehensiveHealthCheck) {
    // Simulate a full health check scenario
    metrics_->recordHealthCheck("storage_engine", true);
    metrics_->recordHealthCheck("cache_system", true);
    metrics_->recordHealthCheck("compaction_threads", false);  // Failed
    metrics_->recordHealthCheck("network_connectivity", true);
    metrics_->recordHealthCheck("memory_usage", false);        // Failed

    auto health_status = metrics_->getAllHealthChecks();

    // Verify individual components
    EXPECT_TRUE(metrics_->isHealthy("storage_engine"));
    EXPECT_TRUE(metrics_->isHealthy("cache_system"));
    EXPECT_FALSE(metrics_->isHealthy("compaction_threads"));
    EXPECT_TRUE(metrics_->isHealthy("network_connectivity"));
    EXPECT_FALSE(metrics_->isHealthy("memory_usage"));

    // Verify overall health (at least some components are healthy)
    bool has_healthy_components = false;
    bool has_failed_components = false;

    for (const auto& [component, healthy] : health_status) {
        if (healthy) has_healthy_components = true;
        else has_failed_components = true;
    }

    EXPECT_TRUE(has_healthy_components);
    EXPECT_TRUE(has_failed_components);
}

/**
 * Test metrics reset functionality
 */
TEST_F(MonitoringMetricsTest, MetricsReset) {
    // Add some metrics
    metrics_->incrementCounter("test_counter", 100);
    metrics_->setGauge("test_gauge", 50);
    metrics_->recordHistogram("test_histogram", 25);
    metrics_->recordHealthCheck("test_component", true);

    // Verify metrics exist
    EXPECT_EQ(metrics_->getCounter("test_counter"), 100);
    EXPECT_EQ(metrics_->getGauge("test_gauge"), 50);
    EXPECT_EQ(metrics_->getHistogram("test_histogram").size(), 1);
    EXPECT_TRUE(metrics_->isHealthy("test_component"));

    // Reset metrics
    metrics_->reset();

    // Verify metrics are cleared
    EXPECT_EQ(metrics_->getCounter("test_counter"), 0);
    EXPECT_EQ(metrics_->getGauge("test_gauge"), 0);
    EXPECT_EQ(metrics_->getHistogram("test_histogram").size(), 0);
    EXPECT_FALSE(metrics_->isHealthy("test_component"));
}

/**
 * Test metrics export edge cases
 */
TEST_F(MonitoringMetricsTest, MetricsExportEdgeCases) {
    // Test empty metrics export
    std::string prometheus_empty = metrics_->exportPrometheus();
    std::string json_empty = metrics_->exportJSON();

    // Should not crash and should produce valid output
    EXPECT_FALSE(prometheus_empty.empty());
    EXPECT_FALSE(json_empty.empty());

    // Parse JSON to ensure it's valid
    nlohmann::json parsed = nlohmann::json::parse(json_empty);
    EXPECT_TRUE(parsed.contains("counters"));
    EXPECT_TRUE(parsed.contains("gauges"));
    EXPECT_TRUE(parsed.contains("histograms"));
    EXPECT_TRUE(parsed.contains("health_checks"));
    EXPECT_TRUE(parsed.contains("timestamp"));
}

}  // namespace test
}  // namespace marble

// Main test runner
int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    ::testing::InitGoogleMock(&argc, argv);

    return RUN_ALL_TESTS();
}

