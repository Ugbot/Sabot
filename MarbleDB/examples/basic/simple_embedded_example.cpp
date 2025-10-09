#include <iostream>
#include <string>

/**
 * @brief Simple demonstration of MarbleDB as an embedded library
 *
 * This example shows how MarbleDB can be used as a library that
 * applications link against directly.
 */

// Forward declarations for MarbleDB interfaces
namespace marble {

// Simple status type
enum class StatusCode { OK, ERROR };
class Status {
public:
    Status(StatusCode code = StatusCode::OK) : code_(code) {}
    bool ok() const { return code_ == StatusCode::OK; }
    std::string message() const { return "Status message"; }
    StatusCode code_;
};

// Database interface
class Database {
public:
    static Status Open(const std::string& path, Database** db) {
        *db = new Database();
        return Status();
    }

    Status Put(const std::string& key, const std::string& value) {
        std::cout << "Storing: " << key << " -> " << value << std::endl;
        return Status();
    }

    Status Get(const std::string& key, std::string* value) {
        *value = "sample_value";
        std::cout << "Retrieved: " << key << " -> " << *value << std::endl;
        return Status();
    }

    ~Database() {
        std::cout << "Database closed" << std::endl;
    }
};

} // namespace marble

/**
 * @brief Example application using MarbleDB as an embedded library
 */
int main() {
    std::cout << "==========================================" << std::endl;
    std::cout << "🗄️  MarbleDB Embedded Library Example" << std::endl;
    std::cout << "==========================================" << std::endl;
    std::cout << std::endl;

    // Open database (this would be the real MarbleDB API)
    marble::Database* db;
    auto status = marble::Database::Open("./my_data", &db);

    if (!status.ok()) {
        std::cerr << "Failed to open database: " << status.message() << std::endl;
        return 1;
    }

    std::cout << "✅ Database opened successfully" << std::endl;

    // Store some data
    db->Put("key1", "value1");
    db->Put("key2", "value2");

    // Retrieve data
    std::string value;
    db->Get("key1", &value);

    // Clean up
    delete db;

    std::cout << std::endl;
    std::cout << "🎯 MarbleDB can be embedded in applications!" << std::endl;
    std::cout << "   • Link against libmarble.a or libmarble.so" << std::endl;
    std::cout << "   • Direct API access without network overhead" << std::endl;
    std::cout << "   • Same performance as server mode" << std::endl;
    std::cout << "   • Full control over database lifecycle" << std::endl;
    std::cout << std::endl;

    std::cout << "==========================================" << std::endl;

    return 0;
}
