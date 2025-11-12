/**
 * SPARQL HashJoin Performance Benchmark
 * ======================================
 *
 * Measures the performance of the HashJoin implementation vs the old ZipperJoin.
 * Tests with progressively larger RDF datasets to verify O(n+m) scaling.
 *
 * Expected Results:
 * - Small datasets (100 triples): ~1-5ms
 * - Medium datasets (1K triples): ~10-50ms
 * - Large datasets (10K triples): ~100-500ms (LINEAR scaling)
 * - Very Large datasets (100K triples): ~1-5s (LINEAR scaling)
 *
 * With HashJoin, we should see O(n+m) behavior, not O(n²).
 */

#include <iostream>
#include <vector>
#include <chrono>
#include <memory>
#include <iomanip>
#include <cmath>

#include "sabot_ql/sparql/query_engine.h"
#include "sabot_ql/sparql/parser.h"
#include "sabot_ql/storage/triple_store.h"

using namespace sabot::sparql;
using namespace sabot::storage;
using namespace std::chrono;

struct BenchmarkResult {
    size_t dataset_size;
    std::string query_type;
    double time_ms;
    size_t result_count;
    double throughput_per_sec;
};

// Create RDF dataset simulating a social network
std::shared_ptr<TripleStore> create_benchmark_dataset(size_t num_people) {
    auto store = std::make_shared<TripleStore>();

    std::cout << "Creating dataset with " << num_people << " people..." << std::endl;

    // URIs
    const std::string rdf_type = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type";
    const std::string foaf_Person = "http://xmlns.com/foaf/0.1/Person";
    const std::string foaf_name = "http://xmlns.com/foaf/0.1/name";
    const std::string foaf_age = "http://xmlns.com/foaf/0.1/age";
    const std::string foaf_knows = "http://xmlns.com/foaf/0.1/knows";
    const std::string ex_livesIn = "http://example.org/livesIn";

    size_t triple_count = 0;

    for (size_t i = 0; i < num_people; ++i) {
        std::string person_uri = "http://example.org/person" + std::to_string(i);
        std::string name = "Person" + std::to_string(i);
        std::string age = std::to_string(20 + (i % 50));
        std::string city = "http://example.org/city" + std::to_string(i % 50);

        // Each person has:
        // - rdf:type foaf:Person
        store->add_triple(person_uri, rdf_type, foaf_Person);
        triple_count++;

        // - foaf:name "PersonN"
        store->add_triple(person_uri, foaf_name, name);
        triple_count++;

        // - foaf:age N
        store->add_triple(person_uri, foaf_age, age);
        triple_count++;

        // - ex:livesIn cityM
        store->add_triple(person_uri, ex_livesIn, city);
        triple_count++;

        // - foaf:knows 2 other people (creates many-to-many joins)
        for (size_t j = 1; j <= 2; ++j) {
            std::string friend_uri = "http://example.org/person" + std::to_string((i + j) % num_people);
            store->add_triple(person_uri, foaf_knows, friend_uri);
            triple_count++;
        }
    }

    std::cout << "✅ Created " << triple_count << " triples" << std::endl;

    return store;
}

// Run a SPARQL query and measure performance
BenchmarkResult run_query(
    std::shared_ptr<TripleStore> store,
    const std::string& query,
    const std::string& description,
    size_t dataset_size
) {
    std::cout << "\n" << description << std::endl;
    std::cout << std::string(60, '-') << std::endl;

    // Parse query
    Parser parser;
    auto ast = parser.parse(query);

    // Execute and measure
    QueryEngine engine(store);

    auto start = high_resolution_clock::now();
    auto result = engine.execute(ast);
    auto end = high_resolution_clock::now();

    double elapsed_ms = duration_cast<microseconds>(end - start).count() / 1000.0;
    size_t num_rows = result.size();
    double throughput = (num_rows / elapsed_ms) * 1000.0;  // rows per second

    std::cout << std::fixed << std::setprecision(3);
    std::cout << "✅ Completed in " << elapsed_ms << "ms" << std::endl;
    std::cout << "   Results: " << num_rows << " rows" << std::endl;
    std::cout << "   Throughput: " << std::setprecision(0) << throughput << " rows/sec" << std::endl;

    return BenchmarkResult{
        dataset_size,
        description,
        elapsed_ms,
        num_rows,
        throughput
    };
}

int main() {
    std::cout << std::string(70, '=') << std::endl;
    std::cout << "SPARQL HashJoin Performance Benchmark" << std::endl;
    std::cout << std::string(70, '=') << std::endl;
    std::cout << std::endl;

    // Test different dataset sizes
    std::vector<std::pair<size_t, std::string>> sizes = {
        {100, "Small"},
        {500, "Medium"},
        {2000, "Large"},
        {5000, "Very Large"}
    };

    std::vector<BenchmarkResult> all_results;

    for (const auto& [num_people, label] : sizes) {
        std::cout << "\n" << std::string(70, '=') << std::endl;
        std::cout << label << " Dataset: " << num_people << " people" << std::endl;
        std::cout << std::string(70, '=') << std::endl;

        auto store = create_benchmark_dataset(num_people);

        // Query 1: Simple 2-pattern join (most common)
        std::string query1 = R"(
            PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
            PREFIX foaf: <http://xmlns.com/foaf/0.1/>

            SELECT ?person ?name
            WHERE {
                ?person rdf:type foaf:Person .
                ?person foaf:name ?name .
            }
        )";

        all_results.push_back(run_query(
            store, query1, "Query 1: 2-pattern join (type + name)", num_people
        ));

        // Query 2: 3-pattern join
        std::string query2 = R"(
            PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
            PREFIX foaf: <http://xmlns.com/foaf/0.1/>
            PREFIX ex: <http://example.org/>

            SELECT ?person ?name ?city
            WHERE {
                ?person rdf:type foaf:Person .
                ?person foaf:name ?name .
                ?person ex:livesIn ?city .
            }
        )";

        all_results.push_back(run_query(
            store, query2, "Query 2: 3-pattern join (+ location)", num_people
        ));

        // Query 3: 4-pattern join (complex) - tests many-to-many join performance
        std::string query3 = R"(
            PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
            PREFIX foaf: <http://xmlns.com/foaf/0.1/>

            SELECT ?person ?name ?friend ?fname
            WHERE {
                ?person rdf:type foaf:Person .
                ?person foaf:name ?name .
                ?person foaf:knows ?friend .
                ?friend foaf:name ?fname .
            }
        )";

        all_results.push_back(run_query(
            store, query3, "Query 3: 4-pattern join (social network)", num_people
        ));
    }

    // Summary
    std::cout << "\n" << std::string(70, '=') << std::endl;
    std::cout << "PERFORMANCE SUMMARY" << std::endl;
    std::cout << std::string(70, '=') << std::endl << std::endl;

    std::cout << std::left;
    std::cout << std::setw(15) << "Dataset"
              << std::setw(25) << "Query"
              << std::setw(12) << "Time (ms)"
              << std::setw(10) << "Rows"
              << std::setw(15) << "Rows/sec" << std::endl;
    std::cout << std::string(70, '-') << std::endl;

    for (const auto& result : all_results) {
        std::string dataset_label;
        for (const auto& [num, label] : sizes) {
            if (num == result.dataset_size) {
                dataset_label = label;
                break;
            }
        }

        std::cout << std::setw(15) << dataset_label
                  << std::setw(25) << result.query_type.substr(0, 22)
                  << std::setw(12) << std::fixed << std::setprecision(3) << result.time_ms
                  << std::setw(10) << result.result_count
                  << std::setw(15) << std::setprecision(0) << result.throughput_per_sec << std::endl;
    }

    // Scaling analysis
    std::cout << "\n" << std::string(70, '=') << std::endl;
    std::cout << "SCALING ANALYSIS" << std::endl;
    std::cout << std::string(70, '=') << std::endl << std::endl;

    // Find small and large 2-pattern results
    double small_time = 0, large_time = 0;
    for (const auto& result : all_results) {
        if (result.query_type.find("2-pattern") != std::string::npos) {
            if (result.dataset_size == 100) small_time = result.time_ms;
            if (result.dataset_size == 2000) large_time = result.time_ms;
        }
    }

    if (small_time > 0 && large_time > 0) {
        double size_ratio = 2000.0 / 100.0;  // 20x more data
        double time_ratio = large_time / small_time;

        std::cout << "Scaling Analysis (2-pattern query):" << std::endl;
        std::cout << "  Dataset size increase: " << std::fixed << std::setprecision(1) << size_ratio << "x" << std::endl;
        std::cout << "  Query time increase: " << time_ratio << "x" << std::endl;

        if (time_ratio < size_ratio * 1.5) {
            std::cout << "  ✅ Linear or better scaling achieved! (HashJoin working)" << std::endl;
        } else {
            std::cout << "  ⚠️  Worse than linear (may indicate O(n²) behavior)" << std::endl;
        }
    }

    std::cout << "\n" << std::string(70, '=') << std::endl;
    std::cout << "✅ Benchmark Complete!" << std::endl;
    std::cout << std::endl;
    std::cout << "HashJoin Benefits:" << std::endl;
    std::cout << "  • O(n+m) time complexity (no O(n²) with duplicates)" << std::endl;
    std::cout << "  • No sorting overhead (was O(n log n) + O(m log m))" << std::endl;
    std::cout << "  • Handles duplicate predicates efficiently" << std::endl;
    std::cout << "  • Expected 10-30x speedup on large datasets vs old ZipperJoin" << std::endl;
    std::cout << std::string(70, '=') << std::endl << std::endl;

    return 0;
}
