# MarbleDB Test Suite

Comprehensive testing framework for MarbleDB components and functionality.

## 📂 Test Organization

```
tests/
├── unit/                    # Unit tests for individual components
│   ├── test_record_system.cpp     # Type-safe record system tests
│   └── test_pushdown.cpp          # Pushdown functionality tests
├── integration/             # Integration tests across components
│   └── test_query_execution.cpp   # End-to-end query execution tests
├── performance/             # Performance benchmarks and profiling
│   └── test_pushdown_performance.cpp # Pushdown performance tests
├── test_utils.h             # Test utilities and helpers
├── test_utils.cpp           # Test utilities implementation
├── CMakeLists.txt           # Test build configuration
└── README.md               # This file
```

## 🧪 Test Categories

### Unit Tests (`unit/`)
- **Focus**: Individual component functionality
- **Scope**: Isolated unit testing
- **Coverage**: Template metaprogramming, data structures, algorithms
- **Examples**: Record system, pushdown evaluators, column projectors

### Integration Tests (`integration/`)
- **Focus**: Component interaction and end-to-end workflows
- **Scope**: Multi-component testing
- **Coverage**: Query execution, data flow, error handling
- **Examples**: Full query pipelines, pushdown optimization

### Performance Tests (`performance/`)
- **Focus**: Performance benchmarks and scalability
- **Scope**: Large dataset testing and profiling
- **Coverage**: Throughput, latency, memory usage
- **Examples**: 100k+ row datasets, comparative benchmarks

### Legacy Tests (root level)
- **Status**: Being migrated to organized structure
- **Contents**: Existing tests for core functionality
- **Migration**: Gradually moving to appropriate categories

## 🛠️ Test Utilities

### Core Testing Framework
```cpp
#include "test_utils.h"

// Base test fixture with common setup/teardown
class MyTest : public marble::test::MarbleTestBase {
    // Automatic temp directory management
    // std::string test_path_ available
};

// Arrow-specific test fixture
class MyArrowTest : public marble::test::ArrowTestBase {
    // std::shared_ptr<arrow::Schema> schema_ available
    // std::vector<std::shared_ptr<arrow::RecordBatch>> test_batches_
};
```

### Data Generation
```cpp
// Generate random test data
marble::test::TestDataGenerator generator(schema);
auto batch = generator.GenerateBatch(1000);

// Generate patterned data for specific test cases
marble::test::PatternedDataGenerator patterned_gen(schema);
auto clustered_batch = patterned_gen.GeneratePatternedBatch(1000, "clustered");
```

### Performance Measurement
```cpp
marble::test::PerformanceTimer timer;
marble::test::BenchmarkRunner runner;

// Measure function execution time
timer.Start();
// ... code to measure ...
timer.Stop();
double elapsed = timer.ElapsedSeconds();

// Benchmark with throughput calculation
auto result = runner.Run("my_operation", 1000, []() {
    // Operation to benchmark
});
result.Print(); // Shows ops/sec, latency, etc.
```

### Validation Helpers
```cpp
// Validate RecordBatch contents
EXPECT_TRUE(marble::test::RecordBatchValidator::ValidateSchema(batch, expected_schema));
EXPECT_TRUE(marble::test::RecordBatchValidator::ValidateRowCount(batch, 1000));
EXPECT_TRUE(marble::test::RecordBatchValidator::ValidateNoNulls(batch, "id"));

// Validate query results
EXPECT_TRUE(marble::test::QueryResultValidator::ValidateResultCount(actual_count, expected_count, 0.1));
```

## 🚀 Running Tests

### Build Tests
```bash
# Build all tests
make run_all_tests

# Build specific test categories
make run_unit_tests       # Unit tests only
make run_integration_tests # Integration tests only
make run_performance_tests # Performance tests only
```

### Run Individual Tests
```bash
# Run specific test executables
./build/test_record_system
./build/test_pushdown
./build/test_query_execution
./build/test_pushdown_performance
```

### Run with CTest
```bash
# Run all tests
ctest

# Run tests by label
ctest -L unit
ctest -L integration
ctest -L performance

# Run specific tests
ctest -R test_record_system
ctest -R test_pushdown_performance
```

### Run Tests with Coverage
```bash
# Enable coverage in CMake (requires GCC/Clang)
cmake -DENABLE_COVERAGE=ON ..
make coverage

# View coverage report in browser
open coverage_report/index.html
```

## 📊 Test Examples

### Unit Test Example
```cpp
TEST_F(RecordSystemTest, FieldCompileTimeAttributes) {
    using TestField = Field<int64_t, "test_field", FieldAttribute::kPrimaryKey>;

    // Compile-time assertions
    static_assert(TestField::is_primary_key == true);
    static_assert(TestField::is_nullable == false);

    // Runtime tests
    TestField field;
    field.value = 42;
    EXPECT_EQ(field.get(), 42);
}
```

### Integration Test Example
```cpp
TEST_F(QueryExecutionTest, EndToEndProjectionPushdown) {
    // Generate test data
    CreateTestData(); // 1000 employee records

    // Execute projection query
    std::vector<std::string> projection = {"name", "salary"};
    auto evaluator = CreateArrowPredicateEvaluator({});
    auto projector = CreateArrowColumnProjector(projection);

    // Process all batches
    for (const auto& batch : batches_) {
        std::shared_ptr<arrow::RecordBatch> projected;
        ASSERT_STATUS_OK(projector->ProjectBatch(batch, &projected));
        EXPECT_EQ(projected->num_columns(), 2);
    }
}
```

### Performance Test Example
```cpp
TEST_F(PushdownPerformanceTest, CombinedPushdownPerformance) {
    // Test with 100k rows of e-commerce data
    std::vector<std::string> projection = {"id", "user_id", "price"};
    std::vector<ColumnPredicate> predicates = {
        ColumnPredicate("price", PredicateType::kGreaterThan, arrow::MakeScalar<double>(100.0)),
        ColumnPredicate("region", PredicateType::kEqual, arrow::MakeScalar<std::string>("North"))
    };

    // Measure performance
    PerformanceTimer timer;
    timer.Start();

    // Execute query
    auto evaluator = CreateArrowPredicateEvaluator(predicates);
    auto projector = CreateArrowColumnProjector(projection);

    int64_t total_rows = 0;
    for (const auto& batch : batches_) {
        std::shared_ptr<arrow::RecordBatch> filtered, final;
        evaluator->FilterBatch(batch, &filtered);
        projector->ProjectBatch(filtered, &final);
        total_rows += final->num_rows();
    }

    timer.Stop();

    // Report results
    std::cout << "Combined Pushdown Performance:" << std::endl;
    std::cout << "  Processed: " << total_rows << " rows" << std::endl;
    std::cout << "  Time: " << timer.ElapsedSeconds() << " seconds" << std::endl;
    std::cout << "  Throughput: " << (total_rows / timer.ElapsedSeconds()) << " rows/sec" << std::endl;
}
```

## 📈 Test Coverage Goals

### Unit Tests
- ✅ Template metaprogramming (Field, FieldList, TypedRecord)
- ✅ Arrow type mapping and conversion
- ✅ Predicate evaluation logic
- ✅ Column projection logic
- ✅ Column statistics computation
- ✅ Schema validation and management

### Integration Tests
- ✅ End-to-end query execution with pushdown
- ✅ Multi-batch processing pipelines
- ✅ Error handling and edge cases
- ✅ Memory efficiency validation
- ✅ Streaming query execution

### Performance Tests
- ✅ Large dataset scalability (100k+ rows)
- ✅ Pushdown vs traditional comparison
- ✅ Memory usage profiling
- ✅ Throughput and latency measurement
- ✅ Comparative performance analysis

## 🔧 Test Infrastructure

### Build System Integration
- **CMake**: Comprehensive test configuration
- **CTest**: Native test runner integration
- **CTest Labels**: Test categorization and filtering
- **Custom Targets**: Convenient test execution commands

### Continuous Integration
```yaml
# Example GitHub Actions workflow
- name: Run Tests
  run: |
    make run_unit_tests
    make run_integration_tests

- name: Performance Tests
  run: make run_performance_tests

- name: Coverage Report
  run: make coverage
```

### Test Data Management
- **Automatic Cleanup**: Temp directories removed after tests
- **Data Persistence**: Save/load test data for debugging
- **Deterministic Generation**: Reproducible random data
- **Patterned Data**: Specific data distributions for testing

## 🎯 Best Practices

### Writing Tests
1. **Use Appropriate Fixtures**: Extend `MarbleTestBase` or `ArrowTestBase`
2. **Descriptive Test Names**: `TEST_F(TestClass, DescriptiveTestName)`
3. **ASSERT vs EXPECT**: Use ASSERT for critical failures, EXPECT for validation
4. **Test Edge Cases**: Empty data, null values, boundary conditions
5. **Performance Tests**: Include baseline measurements and comparisons

### Test Organization
1. **One Concept Per Test**: Each test should validate one specific behavior
2. **Independent Tests**: Tests should not depend on each other's execution order
3. **Fast Tests**: Unit tests should be < 1 second, integration < 10 seconds
4. **Clear Assertions**: Use descriptive assertion messages
5. **Resource Cleanup**: Automatic cleanup via test fixtures

### Performance Testing
1. **Realistic Data**: Use representative data sizes and distributions
2. **Multiple Runs**: Run performance tests multiple times for stability
3. **Baseline Comparison**: Compare against known good implementations
4. **Resource Monitoring**: Track memory, CPU, and I/O usage
5. **Scalability Testing**: Test with different data sizes

## 📋 Test Status

### ✅ Completed Tests
- **Unit Tests**: Record system, pushdown functionality
- **Integration Tests**: Query execution pipelines
- **Performance Tests**: Pushdown scalability and efficiency
- **Test Infrastructure**: Utilities, fixtures, data generators

### 🚧 In Progress
- **Migration**: Moving legacy tests to organized structure
- **Coverage**: Expanding test coverage for new features
- **CI/CD**: Integrating with continuous integration

### 📅 Planned
- **Distributed Tests**: Multi-node testing scenarios
- **Stress Tests**: High load and edge case testing
- **Fuzz Testing**: Random input generation and validation
- **Benchmark Suite**: Standardized performance comparisons

## 🎉 Impact

The comprehensive test suite ensures:

- **✅ Reliability**: Extensive coverage prevents regressions
- **✅ Performance**: Benchmarks validate optimization effectiveness
- **✅ Maintainability**: Well-organized tests ease development
- **✅ Confidence**: Thorough testing enables rapid development cycles

**Testing is the foundation of reliable, high-performance database systems!** 🧪✨
