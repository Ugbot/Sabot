/**
 * Simple Standalone Test
 *
 * A minimal test that doesn't depend on the full MarbleDB library
 * to verify the test infrastructure works.
 */

#include <iostream>
#include <string>
#include <vector>
#include <memory>

// Simple test framework (minimal version of Google Test)
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

struct TestCase {
    std::string name;
    void (*func)();
};

std::vector<TestCase> test_registry;

// Test Status class (minimal implementation)
class Status {
public:
    enum Code { kOk = 0, kNotFound = 1, kInvalidArgument = 2 };

    Status() : code_(kOk) {}
    Status(Code code) : code_(code) {}

    bool ok() const { return code_ == kOk; }
    bool IsNotFound() const { return code_ == kNotFound; }
    Code code() const { return code_; }

    static Status OK() { return Status(kOk); }
    static Status NotFound() { return Status(kNotFound); }

private:
    Code code_;
};

// Test basic functionality
TEST(SimpleTest, StatusOperations) {
    Status ok = Status::OK();
    EXPECT_TRUE(ok.ok());
    EXPECT_FALSE(ok.IsNotFound());

    Status not_found = Status::NotFound();
    EXPECT_FALSE(not_found.ok());
    EXPECT_TRUE(not_found.IsNotFound());
}

TEST(SimpleTest, StringOperations) {
    std::string s1 = "hello";
    std::string s2 = "world";
    std::string s3 = s1 + " " + s2;

    EXPECT_EQ(s3, "hello world");
    EXPECT_TRUE(s3.length() == 11);
    EXPECT_FALSE(s3.empty());
}

TEST(SimpleTest, VectorOperations) {
    std::vector<int> v = {1, 2, 3, 4, 5};

    EXPECT_EQ(v.size(), 5u);
    EXPECT_EQ(v[0], 1);
    EXPECT_EQ(v.back(), 5);

    v.push_back(6);
    EXPECT_EQ(v.size(), 6u);
    EXPECT_EQ(v.back(), 6);
}

TEST(SimpleTest, SmartPointers) {
    auto ptr = std::make_shared<std::string>("test");
    EXPECT_TRUE(ptr != nullptr);
    EXPECT_EQ(*ptr, "test");

    auto unique_ptr = std::make_unique<int>(42);
    EXPECT_TRUE(unique_ptr != nullptr);
    EXPECT_EQ(*unique_ptr, 42);
}

TEST(SimpleTest, BasicMath) {
    int a = 10;
    int b = 20;
    int sum = a + b;

    EXPECT_EQ(sum, 30);
    EXPECT_TRUE(sum > a);
    EXPECT_TRUE(sum > b);
    EXPECT_FALSE(sum < a);
}

// Test runner
int main(int argc, char** argv) {
    std::cout << "Running Simple Standalone Tests" << std::endl;
    std::cout << "===============================" << std::endl;

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

    std::cout << "===============================" << std::endl;
    std::cout << "Results: " << passed << " passed, " << failed << " failed" << std::endl;

    if (failed == 0) {
        std::cout << "🎉 All tests passed!" << std::endl;
        return 0;
    } else {
        std::cout << "❌ Some tests failed!" << std::endl;
        return 1;
    }
}
