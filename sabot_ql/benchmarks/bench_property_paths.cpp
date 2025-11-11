/**
 * SPARQL 1.1 Property Path Performance Benchmark
 *
 * Tests transitive closure performance on chains and trees.
 */

#include <iostream>
#include <chrono>
#include <iomanip>
#include <cstdlib>
#include <sabot_ql/storage/triple_store.h>
#include <sabot_ql/storage/vocabulary.h>
#include <sabot_ql/sparql/query_engine.h>
#include <sabot_ql/sparql/parser.h>

using namespace sabot_ql;
using namespace sabot_ql::sparql;
using namespace std::chrono;

// Timing helper
struct Timer {
    steady_clock::time_point start;

    Timer() : start(steady_clock::now()) {}

    double elapsed_ms() const {
        auto end = steady_clock::now();
        return duration_cast<duration<double, std::milli>>(end - start).count();
    }
};

// Create chain: 0 -> 1 -> 2 -> ... -> n-1
// Returns (store, vocab) tuple
std::pair<std::shared_ptr<TripleStore>, std::shared_ptr<Vocabulary>>
create_chain(int n, const std::string& db_path) {
    // Clean up any existing database
    std::system(("rm -rf " + db_path).c_str());

    // Create store using factory
    auto store_result = CreateTripleStore(db_path);
    if (!store_result.ok()) {
        std::cerr << "Failed to create store: " << store_result.status().ToString() << "\n";
        return {nullptr, nullptr};
    }
    auto store = *store_result;

    // Create vocabulary using factory
    auto vocab_result = CreateVocabulary(db_path);
    if (!vocab_result.ok()) {
        std::cerr << "Failed to create vocabulary: " << vocab_result.status().ToString() << "\n";
        return {nullptr, nullptr};
    }
    auto vocab = *vocab_result;

    std::string knows = "http://xmlns.com/foaf/0.1/knows";

    for (int i = 0; i < n - 1; i++) {
        std::string subject = "http://example.org/person" + std::to_string(i);
        std::string object = "http://example.org/person" + std::to_string(i + 1);

        auto s = vocab->AddTerm(Term::IRI(subject));
        auto p = vocab->AddTerm(Term::IRI(knows));
        auto o = vocab->AddTerm(Term::IRI(object));

        if (!s.ok() || !p.ok() || !o.ok()) {
            std::cerr << "Failed to add terms\n";
            return {nullptr, nullptr};
        }

        Triple triple;
        triple.subject = *s;
        triple.predicate = *p;
        triple.object = *o;

        std::vector<Triple> triples = {triple};
        auto status = store->InsertTriples(triples);
        if (!status.ok()) {
            std::cerr << "Failed to insert triple: " << status.ToString() << "\n";
            return {nullptr, nullptr};
        }
    }

    auto flush_status = store->Flush();
    if (!flush_status.ok()) {
        std::cerr << "Failed to flush: " << flush_status.ToString() << "\n";
        return {nullptr, nullptr};
    }

    return {store, vocab};
}

// Benchmark transitive closure p+
void benchmark_transitive_plus(int chain_size) {
    std::string db_path = "/tmp/bench_property_path_plus_" + std::to_string(chain_size);

    // Create dataset
    Timer create_timer;
    auto [store, vocab] = create_chain(chain_size, db_path);
    if (!store || !vocab) {
        std::cerr << "Failed to create chain\n";
        return;
    }
    double create_ms = create_timer.elapsed_ms();

    // Run query
    std::string query = R"(
        PREFIX foaf: <http://xmlns.com/foaf/0.1/>
        SELECT ?person WHERE {
            <http://example.org/person0> foaf:knows+ ?person .
        }
    )";

    QueryEngine engine(store, vocab);

    Timer query_timer;
    auto parse_result = ParseSPARQL(query);
    if (!parse_result.ok()) {
        std::cerr << "Parse failed: " << parse_result.status().ToString() << "\n";
        return;
    }
    auto parsed_query = *parse_result;
    auto select_query = std::get<SelectQuery>(parsed_query.query_body);

    auto result = engine.ExecuteSelect(select_query);
    double query_ms = query_timer.elapsed_ms();

    if (!result.ok()) {
        std::cerr << "Query failed: " << result.status().ToString() << "\n";
        return;
    }

    int num_rows = (*result)->num_rows();

    // Calculate throughput
    double triples_per_sec = (chain_size / query_ms) * 1000.0;

    std::cout << std::setw(10) << chain_size
              << std::setw(12) << "p+"
              << std::setw(12) << num_rows
              << std::setw(15) << std::fixed << std::setprecision(2) << create_ms
              << std::setw(15) << std::fixed << std::setprecision(2) << query_ms
              << std::setw(20) << std::fixed << std::setprecision(1) << (triples_per_sec / 1000.0)
              << "\n";

    // Cleanup
    std::system(("rm -rf " + db_path).c_str());
}

// Benchmark transitive closure p*
void benchmark_transitive_star(int chain_size) {
    std::string db_path = "/tmp/bench_property_path_star_" + std::to_string(chain_size);

    auto [store, vocab] = create_chain(chain_size, db_path);
    if (!store || !vocab) return;

    std::string query = R"(
        PREFIX foaf: <http://xmlns.com/foaf/0.1/>
        SELECT ?person WHERE {
            <http://example.org/person0> foaf:knows* ?person .
        }
    )";

    QueryEngine engine(store, vocab);

    Timer query_timer;
    auto parse_result = ParseSPARQL(query);
    if (!parse_result.ok()) {
        std::cerr << "Parse failed: " << parse_result.status().ToString() << "\n";
        return;
    }
    auto parsed_query = *parse_result;
    auto select_query = std::get<SelectQuery>(parsed_query.query_body);

    auto result = engine.ExecuteSelect(select_query);
    double query_ms = query_timer.elapsed_ms();

    if (!result.ok()) {
        std::cerr << "Query failed: " << result.status().ToString() << "\n";
        return;
    }

    int num_rows = (*result)->num_rows();
    double triples_per_sec = (chain_size / query_ms) * 1000.0;

    std::cout << std::setw(10) << chain_size
              << std::setw(12) << "p*"
              << std::setw(12) << num_rows
              << std::setw(15) << "-"
              << std::setw(15) << std::fixed << std::setprecision(2) << query_ms
              << std::setw(20) << std::fixed << std::setprecision(1) << (triples_per_sec / 1000.0)
              << "\n";

    // Cleanup
    std::system(("rm -rf " + db_path).c_str());
}

// Benchmark bounded paths p{2,5}
void benchmark_bounded(int chain_size) {
    std::string db_path = "/tmp/bench_property_path_bounded_" + std::to_string(chain_size);

    auto [store, vocab] = create_chain(chain_size, db_path);
    if (!store || !vocab) return;

    std::string query = R"(
        PREFIX foaf: <http://xmlns.com/foaf/0.1/>
        SELECT ?person WHERE {
            <http://example.org/person0> foaf:knows{2,5} ?person .
        }
    )";

    QueryEngine engine(store, vocab);

    Timer query_timer;
    auto parse_result = ParseSPARQL(query);
    if (!parse_result.ok()) {
        std::cerr << "Parse failed: " << parse_result.status().ToString() << "\n";
        return;
    }
    auto parsed_query = *parse_result;
    auto select_query = std::get<SelectQuery>(parsed_query.query_body);

    auto result = engine.ExecuteSelect(select_query);
    double query_ms = query_timer.elapsed_ms();

    if (!result.ok()) {
        std::cerr << "Query failed: " << result.status().ToString() << "\n";
        return;
    }

    int num_rows = (*result)->num_rows();
    double triples_per_sec = (chain_size / query_ms) * 1000.0;

    std::cout << std::setw(10) << chain_size
              << std::setw(12) << "p{2,5}"
              << std::setw(12) << num_rows
              << std::setw(15) << "-"
              << std::setw(15) << std::fixed << std::setprecision(2) << query_ms
              << std::setw(20) << std::fixed << std::setprecision(1) << (triples_per_sec / 1000.0)
              << "\n";

    // Cleanup
    std::system(("rm -rf " + db_path).c_str());
}

int main() {
    std::cout << "======================================================================\n";
    std::cout << "SPARQL 1.1 Property Path Performance Benchmark\n";
    std::cout << "======================================================================\n\n";

    std::cout << "Transitive Closure on Chains\n";
    std::cout << "----------------------------------------------------------------------\n";
    std::cout << std::setw(10) << "Chain Size"
              << std::setw(12) << "Operator"
              << std::setw(12) << "Results"
              << std::setw(15) << "Create (ms)"
              << std::setw(15) << "Query (ms)"
              << std::setw(20) << "Throughput (K/s)"
              << "\n";
    std::cout << "----------------------------------------------------------------------\n";

    // Test increasing chain sizes
    for (int n : {100, 500, 1000, 2000, 5000}) {
        benchmark_transitive_plus(n);
        benchmark_transitive_star(n);
        benchmark_bounded(n);
        std::cout << "\n";
    }

    std::cout << "======================================================================\n";
    std::cout << "Benchmark Complete!\n";
    std::cout << "======================================================================\n";

    return 0;
}
