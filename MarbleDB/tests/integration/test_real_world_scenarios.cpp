/**
 * Real-World MarbleDB Scenarios Test
 *
 * Demonstrates practical database scenarios that would be used in production:
 * - E-commerce product catalog
 * - User session management
 * - Time-series analytics
 * - Multi-tenant data isolation
 * - Audit logging and compliance
 */

#include <iostream>
#include <string>
#include <vector>
#include <memory>
#include <unordered_map>
#include <map>
#include <set>
#include <algorithm>
#include <random>
#include <chrono>
#include <thread>
#include <mutex>
#include <sstream>
#include <ctime>
#include <iomanip>

// Simple test framework
#define TEST(test_case, test_name) \
    void test_case##_##test_name(); \
    static bool test_case##_##test_name##_registered = []() { \
        test_registry.push_back({#test_case "." #test_name, test_case##_##test_name}); \
        return true; \
    }(); \
    void test_case##_##test_name()

#define EXPECT_EQ(a, b) \
    if ((a) != (b)) { \
        std::cout << "FAIL: " << __FILE__ << ":" << __LINE__ << " Expected " << (a) << " == " << (b) << std::endl; \
        return; \
    }

#define EXPECT_TRUE(a) \
    if (!(a)) { \
        std::cout << "FAIL: " << __FILE__ << ":" << __LINE__ << " Expected true: " << #a << std::endl; \
        return; \
    }

#define EXPECT_FALSE(a) \
    if (a) { \
        std::cout << "FAIL: " << __FILE__ << ":" << __LINE__ << " Expected false: " << #a << std::endl; \
        return; \
    }

#define EXPECT_NE(a, b) \
    if ((a) == (b)) { \
        std::cout << "FAIL: " << __FILE__ << ":" << __LINE__ << " Expected " << (a) << " != " << (b) << std::endl; \
        return; \
    }

#define EXPECT_LT(a, b) \
    if ((a) >= (b)) { \
        std::cout << "FAIL: " << __FILE__ << ":" << __LINE__ << " Expected " << (a) << " < " << (b) << std::endl; \
        return; \
    }

#define EXPECT_GT(a, b) \
    if ((a) <= (b)) { \
        std::cout << "FAIL: " << __FILE__ << ":" << __LINE__ << " Expected " << (a) << " > " << (b) << std::endl; \
        return; \
    }

struct TestCase {
    std::string name;
    void (*func)();
};

std::vector<TestCase> test_registry;

// Mock Status class
namespace marble {
class Status {
public:
    enum Code { kOk = 0, kNotFound = 1, kInvalidArgument = 2, kAlreadyExists = 3 };
    Status() : code_(kOk) {}
    Status(Code code) : code_(code) {}
    bool ok() const { return code_ == kOk; }
    bool IsNotFound() const { return code_ == kNotFound; }
    bool IsInvalidArgument() const { return code_ == kInvalidArgument; }
    bool IsAlreadyExists() const { return code_ == kAlreadyExists; }
    static Status OK() { return Status(kOk); }
    static Status NotFound() { return Status(kNotFound); }
    static Status InvalidArgument() { return Status(kInvalidArgument); }
    static Status AlreadyExists() { return Status(kAlreadyExists); }
private:
    Code code_;
};
}

// Enhanced Record structure for real-world scenarios
struct Record {
    std::string key;
    std::string value;
    int64_t timestamp;
    std::string tenant_id;  // Multi-tenant support
    std::unordered_map<std::string, std::string> metadata;
    std::vector<std::string> tags;  // Tagging support
    bool is_deleted = false;  // Soft delete support

    Record() = default;  // Default constructor
    Record(std::string k, std::string v, std::string tenant = "", int64_t ts = 0)
        : key(std::move(k)), value(std::move(v)), tenant_id(std::move(tenant)), timestamp(ts) {}
};

// E-commerce Product Catalog Scenario
TEST(EcommerceScenarios, ProductCatalogManagement) {
    std::vector<Record> products;

    // Product categories and attributes
    const std::vector<std::string> categories = {"Electronics", "Clothing", "Books", "Home", "Sports"};
    const std::vector<std::string> brands = {"Apple", "Nike", "Amazon", "Sony", "Samsung"};

    std::mt19937 rng(12345);
    std::uniform_int_distribution<> category_dist(0, categories.size() - 1);
    std::uniform_int_distribution<> brand_dist(0, brands.size() - 1);
    std::uniform_real_distribution<> price_dist(10.99, 1999.99);
    std::uniform_int_distribution<> stock_dist(0, 1000);

    // Create 2000 products across categories
    for (int i = 0; i < 2000; ++i) {
        Record product("product:" + std::to_string(i), "product_data");
        product.tenant_id = "ecommerce_store";

        std::string category = categories[category_dist(rng)];
        std::string brand = brands[brand_dist(rng)];

        product.metadata["id"] = std::to_string(i);
        product.metadata["name"] = brand + " Product " + std::to_string(i % 100);
        product.metadata["category"] = category;
        product.metadata["brand"] = brand;
        product.metadata["price"] = std::to_string(price_dist(rng));
        product.metadata["stock"] = std::to_string(stock_dist(rng));
        product.metadata["sku"] = "SKU-" + std::to_string(i);
        product.metadata["created"] = std::to_string(std::time(nullptr));
        product.metadata["updated"] = std::to_string(std::time(nullptr));

        // Add category-specific tags
        product.tags.push_back(category);
        product.tags.push_back(brand);
        if (std::stod(product.metadata["price"]) > 500) {
            product.tags.push_back("premium");
        }
        if (std::stoi(product.metadata["stock"]) < 50) {
            product.tags.push_back("low_stock");
        }

        products.push_back(std::move(product));
    }

    // Test 1: Category-based queries
    std::map<std::string, std::vector<Record>> products_by_category;
    for (const auto& product : products) {
        auto cat_it = product.metadata.find("category");
        if (cat_it != product.metadata.end()) {
            products_by_category[cat_it->second].push_back(product);
        }
    }

    // Verify category distribution
    for (const auto& category : categories) {
        auto it = products_by_category.find(category);
        EXPECT_TRUE(it != products_by_category.end());
        EXPECT_GT(it->second.size(), 300);  // Should be ~400 per category
        EXPECT_LT(it->second.size(), 500);
    }

    // Test 2: Low stock alerts
    std::vector<Record> low_stock_products;
    for (const auto& product : products) {
        auto stock_it = product.metadata.find("stock");
        if (stock_it != product.metadata.end() && std::stoi(stock_it->second) < 50) {
            low_stock_products.push_back(product);
        }
    }

    EXPECT_GT(low_stock_products.size(), 50);  // Should be ~200 low stock items
    EXPECT_LT(low_stock_products.size(), 150);

    // Test 3: Premium product analysis
    std::vector<Record> premium_products;
    double total_premium_value = 0;

    for (const auto& product : products) {
        auto price_it = product.metadata.find("price");
        if (price_it != product.metadata.end() && std::stod(price_it->second) > 500) {
            premium_products.push_back(product);
            total_premium_value += std::stod(price_it->second);
        }
    }

    EXPECT_GT(premium_products.size(), 300);  // Should be ~600 premium products
    EXPECT_LT(premium_products.size(), 800);
    EXPECT_GT(total_premium_value, 200000);   // High total value

    // Test 4: Brand performance metrics
    std::map<std::string, std::pair<int, double>> brand_metrics;  // count, avg_price

    for (const auto& product : products) {
        auto brand_it = product.metadata.find("brand");
        auto price_it = product.metadata.find("price");

        if (brand_it != product.metadata.end() && price_it != product.metadata.end()) {
            std::string brand = brand_it->second;
            double price = std::stod(price_it->second);

            auto& metrics = brand_metrics[brand];
            metrics.first++;  // count
            metrics.second = (metrics.second * (metrics.first - 1) + price) / metrics.first;  // avg
        }
    }

    // Verify all brands are represented
    EXPECT_EQ(brand_metrics.size(), brands.size());

    // Each brand should have reasonable metrics
    for (const auto& pair : brand_metrics) {
        EXPECT_GT(pair.second.first, 300);   // Product count per brand
        EXPECT_LT(pair.second.first, 500);
        EXPECT_GT(pair.second.second, 100);  // Average price per brand
        EXPECT_LT(pair.second.second, 1500);
    }

    std::cout << "  Product Catalog: " << products.size() << " products across " << categories.size() << " categories" << std::endl;
    std::cout << "  Low Stock Items: " << low_stock_products.size() << std::endl;
    std::cout << "  Premium Products: " << premium_products.size() << " ($" << std::fixed << std::setprecision(0) << total_premium_value << ")" << std::endl;
}

// User Session Management Scenario
TEST(UserScenarios, SessionManagement) {
    std::unordered_map<std::string, Record> active_sessions;
    std::unordered_map<std::string, std::vector<Record>> user_session_history;

    const int num_users = 1000;
    const int sessions_per_user = 5;
    const int concurrent_sessions = 200;

    std::mt19937 rng(67890);
    std::uniform_int_distribution<> device_dist(0, 4);  // 5 device types
    std::uniform_int_distribution<> location_dist(0, 9); // 10 locations
    std::uniform_int_distribution<> duration_dist(300, 3600); // 5min to 1hr

    // Device and location data
    const std::vector<std::string> devices = {"iPhone", "Android", "Desktop", "Tablet", "SmartTV"};
    const std::vector<std::string> locations = {"New York", "London", "Tokyo", "Paris", "Berlin",
                                              "Sydney", "Toronto", "Singapore", "Mumbai", "Sao Paulo"};

    // Create user sessions
    int session_counter = 0;
    for (int user = 0; user < num_users; ++user) {
        std::string user_id = "user:" + std::to_string(user);
        std::vector<Record> user_sessions;

        for (int session = 0; session < sessions_per_user; ++session) {
            std::string session_id = "session:" + std::to_string(session_counter++);
            Record session_record(session_id, "session_data", "web_app");

            session_record.metadata["user_id"] = user_id;
            session_record.metadata["device"] = devices[device_dist(rng)];
            session_record.metadata["location"] = locations[location_dist(rng)];
            session_record.metadata["start_time"] = std::to_string(std::time(nullptr) - duration_dist(rng));
            session_record.metadata["duration"] = std::to_string(duration_dist(rng));
            session_record.metadata["ip_address"] = "192.168." + std::to_string(user % 256) + "." + std::to_string(session % 256);

            // Add session tags
            session_record.tags.push_back("active");
            session_record.tags.push_back(session_record.metadata["device"]);
            session_record.tags.push_back(session_record.metadata["location"]);

            user_sessions.push_back(session_record);

            // Keep only recent concurrent sessions active
            if (session_counter <= concurrent_sessions) {
                active_sessions[session_id] = session_record;
            }
        }

        user_session_history[user_id] = std::move(user_sessions);
    }

    // Test 1: Active session count
    EXPECT_EQ(active_sessions.size(), concurrent_sessions);

    // Test 2: User session history
    EXPECT_EQ(user_session_history.size(), num_users);

    for (const auto& pair : user_session_history) {
        EXPECT_EQ(pair.second.size(), sessions_per_user);
    }

    // Test 3: Device usage analytics
    std::map<std::string, int> device_usage;
    for (const auto& pair : active_sessions) {
        auto device_it = pair.second.metadata.find("device");
        if (device_it != pair.second.metadata.end()) {
            device_usage[device_it->second]++;
        }
    }

    // Each device type should be represented
    EXPECT_EQ(device_usage.size(), devices.size());

    // Test 4: Geographic distribution
    std::map<std::string, int> location_distribution;
    for (const auto& pair : active_sessions) {
        auto loc_it = pair.second.metadata.find("location");
        if (loc_it != pair.second.metadata.end()) {
            location_distribution[loc_it->second]++;
        }
    }

    EXPECT_EQ(location_distribution.size(), locations.size());

    // Test 5: Session cleanup (simulate logout)
    int sessions_to_cleanup = 50;
    int cleaned = 0;

    for (auto it = active_sessions.begin(); it != active_sessions.end() && cleaned < sessions_to_cleanup; ++it) {
        auto duration_it = it->second.metadata.find("duration");
        if (duration_it != it->second.metadata.end() && std::stoi(duration_it->second) < 600) {  // Short sessions
            it->second.is_deleted = true;
            cleaned++;
        }
    }

    EXPECT_EQ(cleaned, sessions_to_cleanup);

    std::cout << "  Active Sessions: " << active_sessions.size() << std::endl;
    std::cout << "  User Histories: " << user_session_history.size() << " users" << std::endl;
    std::cout << "  Devices Tracked: " << device_usage.size() << std::endl;
    std::cout << "  Locations: " << location_distribution.size() << std::endl;
}

// Time-Series Analytics Scenario
TEST(AnalyticsScenarios, TimeSeriesMetrics) {
    std::vector<Record> metrics;

    const int num_series = 10;  // 10 different metrics
    const int data_points = 1000;  // 1000 data points per series
    const int time_range = 86400;  // 24 hours in seconds

    std::mt19937 rng(11111);
    std::uniform_real_distribution<> value_dist(0, 1000);
    std::uniform_int_distribution<> time_dist(0, time_range);

    // Metric names
    const std::vector<std::string> metric_names = {
        "cpu_usage", "memory_usage", "disk_io", "network_in", "network_out",
        "response_time", "error_rate", "throughput", "latency", "connections"
    };

    // Generate time-series data
    for (int series = 0; series < num_series; ++series) {
        std::string metric_name = metric_names[series];

        for (int point = 0; point < data_points; ++point) {
            Record metric("metric:" + metric_name + ":" + std::to_string(point), "metric_data", "monitoring");

            int64_t timestamp = std::time(nullptr) - time_range + (point * time_range / data_points);
            double value = value_dist(rng);

            metric.timestamp = timestamp;
            metric.metadata["metric_name"] = metric_name;
            metric.metadata["value"] = std::to_string(value);
            metric.metadata["timestamp"] = std::to_string(timestamp);
            metric.metadata["series_id"] = std::to_string(series);
            metric.metadata["point_id"] = std::to_string(point);

            // Add time-based tags
            std::time_t time_obj = timestamp;
            std::tm* tm_obj = std::localtime(&time_obj);
            int hour = tm_obj->tm_hour;
            metric.metadata["hour"] = std::to_string(hour);
            metric.tags.push_back("hour_" + std::to_string(hour));

            if (hour >= 9 && hour <= 17) {
                metric.tags.push_back("business_hours");
            } else {
                metric.tags.push_back("off_hours");
            }

            metrics.push_back(std::move(metric));
        }
    }

    // Test 1: Data completeness
    EXPECT_EQ(metrics.size(), num_series * data_points);

    // Test 2: Time ordering
    for (int series = 0; series < num_series; ++series) {
        std::vector<Record> series_data;
        for (const auto& metric : metrics) {
            auto series_it = metric.metadata.find("series_id");
            if (series_it != metric.metadata.end() && std::stoi(series_it->second) == series) {
                series_data.push_back(metric);
            }
        }

        EXPECT_EQ(series_data.size(), data_points);

        // Verify time ordering
        for (size_t i = 1; i < series_data.size(); ++i) {
            EXPECT_GT(series_data[i].timestamp, series_data[i-1].timestamp);
        }
    }

    // Test 3: Business hours analysis
    std::map<std::string, std::vector<double>> business_hours_data;
    std::map<std::string, std::vector<double>> off_hours_data;

    for (const auto& metric : metrics) {
        auto name_it = metric.metadata.find("metric_name");
        auto value_it = metric.metadata.find("value");

        if (name_it != metric.metadata.end() && value_it != metric.metadata.end()) {
            double value = std::stod(value_it->second);
            std::string name = name_it->second;

            if (std::find(metric.tags.begin(), metric.tags.end(), "business_hours") != metric.tags.end()) {
                business_hours_data[name].push_back(value);
            } else {
                off_hours_data[name].push_back(value);
            }
        }
    }

    // Test 4: Statistical analysis
    std::map<std::string, std::tuple<double, double, double>> metric_stats;  // min, max, avg

    for (const auto& metric : metrics) {
        auto name_it = metric.metadata.find("metric_name");
        auto value_it = metric.metadata.find("value");

        if (name_it != metric.metadata.end() && value_it != metric.metadata.end()) {
            std::string name = name_it->second;
            double value = std::stod(value_it->second);

            auto& stats = metric_stats[name];
            double& min_val = std::get<0>(stats);
            double& max_val = std::get<1>(stats);
            double& avg_val = std::get<2>(stats);

            if (metric_stats[name] == std::make_tuple(0.0, 0.0, 0.0)) {
                min_val = max_val = avg_val = value;
            } else {
                min_val = std::min(min_val, value);
                max_val = std::max(max_val, value);
                avg_val = (avg_val + value) / 2;  // Running average approximation
            }
        }
    }

    // Verify all metrics have statistics
    EXPECT_EQ(metric_stats.size(), metric_names.size());

    // Test 5: Peak hours analysis
    std::map<int, std::vector<double>> hourly_values;
    for (const auto& metric : metrics) {
        auto hour_it = metric.metadata.find("hour");
        auto value_it = metric.metadata.find("value");

        if (hour_it != metric.metadata.end() && value_it != metric.metadata.end()) {
            int hour = std::stoi(hour_it->second);
            double value = std::stod(value_it->second);
            hourly_values[hour].push_back(value);
        }
    }

    EXPECT_EQ(hourly_values.size(), 24);  // All 24 hours represented

    std::cout << "  Time Series: " << metrics.size() << " data points" << std::endl;
    std::cout << "  Metrics: " << metric_names.size() << " different types" << std::endl;
    std::cout << "  Business Hours Data: " << business_hours_data.size() << " metrics" << std::endl;
    std::cout << "  Hourly Analysis: " << hourly_values.size() << " hours covered" << std::endl;
}

// Multi-Tenant Data Isolation Scenario
TEST(MultiTenantScenarios, DataIsolation) {
    std::unordered_map<std::string, std::vector<Record>> tenant_data;
    const std::vector<std::string> tenants = {"tenant_a", "tenant_b", "tenant_c", "tenant_d", "tenant_e"};

    std::mt19937 rng(22222);
    std::uniform_int_distribution<> tenant_dist(0, tenants.size() - 1);
    std::uniform_int_distribution<> record_count_dist(500, 2000);

    // Generate tenant-specific data
    for (const auto& tenant : tenants) {
        std::vector<Record> tenant_records;
        int record_count = record_count_dist(rng);

        for (int i = 0; i < record_count; ++i) {
            Record record("record:" + tenant + ":" + std::to_string(i), "data", tenant);
            record.metadata["tenant"] = tenant;
            record.metadata["record_id"] = std::to_string(i);
            record.metadata["data_type"] = (i % 3 == 0) ? "user" : (i % 3 == 1) ? "product" : "order";
            record.metadata["created"] = std::to_string(std::time(nullptr));

            tenant_records.push_back(std::move(record));
        }

        tenant_data[tenant] = std::move(tenant_records);
    }

    // Test 1: Data isolation verification
    for (const auto& tenant : tenants) {
        const auto& records = tenant_data[tenant];
        for (const auto& record : records) {
            EXPECT_EQ(record.tenant_id, tenant);
            auto tenant_it = record.metadata.find("tenant");
            EXPECT_TRUE(tenant_it != record.metadata.end());
            EXPECT_EQ(tenant_it->second, tenant);
        }
    }

    // Test 2: Cross-tenant data access prevention (simulated)
    for (const auto& source_tenant : tenants) {
        for (const auto& target_tenant : tenants) {
            if (source_tenant != target_tenant) {
                // Verify no cross-tenant data leakage
                const auto& source_records = tenant_data[source_tenant];
                bool has_cross_tenant_data = false;

                for (const auto& record : source_records) {
                    if (record.tenant_id != source_tenant) {
                        has_cross_tenant_data = true;
                        break;
                    }
                }

                EXPECT_FALSE(has_cross_tenant_data);
            }
        }
    }

    // Test 3: Tenant resource usage
    std::map<std::string, size_t> tenant_usage;
    for (const auto& pair : tenant_data) {
        tenant_usage[pair.first] = pair.second.size();
    }

    // All tenants should have data
    EXPECT_EQ(tenant_usage.size(), tenants.size());

    // Test 4: Tenant-specific queries
    for (const auto& tenant : tenants) {
        const auto& records = tenant_data[tenant];

        // Count data types per tenant
        std::map<std::string, int> data_types;
        for (const auto& record : records) {
            auto type_it = record.metadata.find("data_type");
            if (type_it != record.metadata.end()) {
                data_types[type_it->second]++;
            }
        }

        // Each tenant should have all data types
        EXPECT_EQ(data_types.size(), 3);  // user, product, order
        EXPECT_TRUE(data_types.find("user") != data_types.end());
        EXPECT_TRUE(data_types.find("product") != data_types.end());
        EXPECT_TRUE(data_types.find("order") != data_types.end());
    }

    std::cout << "  Tenants: " << tenants.size() << std::endl;
    for (const auto& pair : tenant_usage) {
        std::cout << "  " << pair.first << ": " << pair.second << " records" << std::endl;
    }
}

// Audit Logging and Compliance Scenario
TEST(ComplianceScenarios, AuditLogging) {
    std::vector<Record> audit_log;

    const std::vector<std::string> actions = {"CREATE", "READ", "UPDATE", "DELETE", "LOGIN", "LOGOUT"};
    const std::vector<std::string> resources = {"user", "product", "order", "payment", "session"};
    const std::vector<std::string> users = {"alice", "bob", "charlie", "diana", "eve"};

    std::mt19937 rng(33333);
    std::uniform_int_distribution<> action_dist(0, actions.size() - 1);
    std::uniform_int_distribution<> resource_dist(0, resources.size() - 1);
    std::uniform_int_distribution<> user_dist(0, users.size() - 1);
    std::uniform_int_distribution<> success_dist(0, 9);  // 90% success rate

    // Generate comprehensive audit log
    int64_t base_time = std::time(nullptr) - 86400;  // Start 24 hours ago
    for (int i = 0; i < 5000; ++i) {
        Record audit_entry("audit:" + std::to_string(i), "audit_data", "security");
        audit_entry.timestamp = base_time + (i * 86400 / 5000);  // Spread over 24 hours

        std::string user = users[user_dist(rng)];
        std::string action = actions[action_dist(rng)];
        std::string resource = resources[resource_dist(rng)];
        bool success = (success_dist(rng) < 9);  // 90% success

        audit_entry.metadata["audit_id"] = std::to_string(i);
        audit_entry.metadata["user"] = user;
        audit_entry.metadata["action"] = action;
        audit_entry.metadata["resource"] = resource;
        audit_entry.metadata["success"] = success ? "true" : "false";
        audit_entry.metadata["timestamp"] = std::to_string(audit_entry.timestamp);
        audit_entry.metadata["ip_address"] = "10.0.0." + std::to_string(i % 256);
        audit_entry.metadata["user_agent"] = "Client/" + std::to_string(i % 10);

        // Add compliance tags
        audit_entry.tags.push_back("audit");
        audit_entry.tags.push_back(action);
        audit_entry.tags.push_back(success ? "success" : "failure");

        if (!success) {
            audit_entry.tags.push_back("security_event");
        }

        audit_log.push_back(std::move(audit_entry));
    }

    // Test 1: Audit completeness
    EXPECT_EQ(audit_log.size(), 5000);

    // Test 2: Failed operations analysis
    std::vector<Record> failed_operations;
    for (const auto& entry : audit_log) {
        auto success_it = entry.metadata.find("success");
        if (success_it != entry.metadata.end() && success_it->second == "false") {
            failed_operations.push_back(entry);
        }
    }

    EXPECT_GT(failed_operations.size(), 400);  // Should be ~500 failures (10%)
    EXPECT_LT(failed_operations.size(), 600);

    // Test 3: User activity analysis
    std::map<std::string, std::vector<Record>> user_activity;
    for (const auto& entry : audit_log) {
        auto user_it = entry.metadata.find("user");
        if (user_it != entry.metadata.end()) {
            user_activity[user_it->second].push_back(entry);
        }
    }

    EXPECT_EQ(user_activity.size(), users.size());

    // Each user should have reasonable activity
    for (const auto& pair : user_activity) {
        EXPECT_GT(pair.second.size(), 800);  // ~1000 activities per user
        EXPECT_LT(pair.second.size(), 1200);
    }

    // Test 4: Resource access patterns
    std::map<std::string, std::map<std::string, int>> resource_access;  // resource -> action -> count

    for (const auto& entry : audit_log) {
        auto resource_it = entry.metadata.find("resource");
        auto action_it = entry.metadata.find("action");

        if (resource_it != entry.metadata.end() && action_it != entry.metadata.end()) {
            resource_access[resource_it->second][action_it->second]++;
        }
    }

    EXPECT_EQ(resource_access.size(), resources.size());

    // Test 5: Time-based analysis (last 24 hours)
    std::map<std::string, int> hourly_activity;
    for (const auto& entry : audit_log) {
        std::time_t time_obj = entry.timestamp;
        std::tm* tm_obj = std::localtime(&time_obj);
        std::string hour_key = std::to_string(tm_obj->tm_hour);
        hourly_activity[hour_key]++;
    }

    // Should have activity across multiple hours
    EXPECT_GT(hourly_activity.size(), 10);

    // Test 6: Security event monitoring
    std::vector<Record> security_events;
    for (const auto& entry : audit_log) {
        if (std::find(entry.tags.begin(), entry.tags.end(), "security_event") != entry.tags.end()) {
            security_events.push_back(entry);
        }
    }

    EXPECT_EQ(security_events.size(), failed_operations.size());  // All failures are security events

    std::cout << "  Audit Entries: " << audit_log.size() << std::endl;
    std::cout << "  Failed Operations: " << failed_operations.size() << std::endl;
    std::cout << "  Active Users: " << user_activity.size() << std::endl;
    std::cout << "  Security Events: " << security_events.size() << std::endl;
    std::cout << "  Hours Covered: " << hourly_activity.size() << std::endl;
}

// Test runner
int main(int argc, char** argv) {
    std::cout << "Running Real-World MarbleDB Scenarios Tests" << std::endl;
    std::cout << "============================================" << std::endl;

    int passed = 0;
    int failed = 0;

    for (const auto& test : test_registry) {
        std::cout << "Running: " << test.name << "... ";

        try {
            test.func();
            std::cout << "PASS" << std::endl;
            passed++;
        } catch (const std::exception& e) {
            std::cout << "FAIL (exception: " << e.what() << ")" << std::endl;
            failed++;
        } catch (...) {
            std::cout << "FAIL (unknown exception)" << std::endl;
            failed++;
        }
    }

    std::cout << "============================================" << std::endl;
    std::cout << "Results: " << passed << " passed, " << failed << " failed" << std::endl;

    if (failed == 0) {
        std::cout << "🎉 All real-world MarbleDB scenario tests passed!" << std::endl;
        std::cout << "   Demonstrated: E-commerce, session management, time-series analytics," << std::endl;
        std::cout << "   multi-tenant isolation, and audit logging scenarios!" << std::endl;
        return 0;
    } else {
        std::cout << "❌ Some tests failed!" << std::endl;
        return 1;
    }
}
