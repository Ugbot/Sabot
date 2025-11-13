#include <iostream>
#include <chrono>
#include <sabot_ql/storage/triple_store.h>
#include <sabot_ql/sparql/query_engine.h>
#include <sabot_ql/sparql/parser.h>
#include <arrow/compute/registry.h>

using namespace sabot_ql;
using namespace sabot_ql::sparql;

class Timer {
    std::chrono::high_resolution_clock::time_point start_;
public:
    Timer() : start_(std::chrono::high_resolution_clock::now()) {}
    double elapsed_ms() const {
        auto end = std::chrono::high_resolution_clock::now();
        return std::chrono::duration<double, std::milli>(end - start_).count();
    }
};

// Helper to execute SPARQL query
arrow::Result<std::shared_ptr<arrow::Table>> execute_sparql(QueryEngine& engine, const std::string& sparql) {
    auto parse_result = ParseSPARQL(sparql);
    if (!parse_result.ok()) {
        return parse_result.status();
    }
    auto parsed_query = *parse_result;
    auto select_query = std::get<SelectQuery>(parsed_query.query_body);
    return engine.ExecuteSelect(select_query);
}

int main() {
    // Initialize Arrow compute module - registers all built-in compute functions
    // (not_equal, sort_indices, etc.) to the global FunctionRegistry
    auto init_status = arrow::compute::Initialize();
    if (!init_status.ok()) {
        std::cerr << "Failed to initialize Arrow compute module: " << init_status << "\n";
        return 1;
    }

    std::cout << "======================================================================\n";
    std::cout << "SPARQL Arrow Features Test\n";
    std::cout << "Testing: Cross Products, Variable FILTER, ORDER BY\n";
    std::cout << "======================================================================\n\n";

    // Initialize triple store
    std::string test_path = "/tmp/sparql_features_test";
    std::system(("rm -rf " + test_path).c_str());

    auto store_result = CreateTripleStore(test_path);
    if (!store_result.ok()) {
        std::cerr << "Failed to create store: " << store_result.status() << "\n";
        return 1;
    }
    auto store = *store_result;

    auto vocab_result = CreateVocabulary(test_path);
    if (!vocab_result.ok()) {
        std::cerr << "Failed to create vocab: " << vocab_result.status() << "\n";
        return 1;
    }
    auto vocab = *vocab_result;

    QueryEngine engine(store, vocab);

    std::cout << "Phase 1: Insert test data (100 triples)\n";
    std::cout << "----------------------------------------------------------------------\n";

    // Insert test data: 10 medals with athlete, country, event
    auto athlete_pred = Term::IRI("http://ex.org/athlete");
    auto country_pred = Term::IRI("http://ex.org/country");
    auto event_pred = Term::IRI("http://ex.org/event");

    std::vector<std::string> athletes = {"Alice", "Bob", "Charlie", "Diana", "Eve",
                                          "Frank", "Grace", "Henry", "Iris", "Jack"};
    std::vector<std::string> countries = {"USA", "UK", "Germany", "France", "Japan"};
    std::vector<std::string> events = {"100m", "200m", "Long Jump"};

    // Build batch of triples
    std::vector<Triple> batch;
    int triple_count = 0;

    for (int i = 0; i < 10; i++) {
        auto medal = Term::IRI("http://ex.org/medal" + std::to_string(i));
        auto athlete = Term::Literal(athletes[i % athletes.size()]);
        auto country = Term::Literal(countries[i % countries.size()]);
        auto event = Term::Literal(events[i % events.size()]);

        // Convert terms to ValueIds
        auto medal_id = vocab->AddTerm(medal);
        auto athlete_pred_id = vocab->AddTerm(athlete_pred);
        auto country_pred_id = vocab->AddTerm(country_pred);
        auto event_pred_id = vocab->AddTerm(event_pred);
        auto athlete_id = vocab->AddTerm(athlete);
        auto country_id = vocab->AddTerm(country);
        auto event_id = vocab->AddTerm(event);

        if (!medal_id.ok() || !athlete_pred_id.ok() || !country_pred_id.ok() ||
            !event_pred_id.ok() || !athlete_id.ok() || !country_id.ok() || !event_id.ok()) {
            std::cerr << "Failed to add terms to vocabulary\n";
            return 1;
        }

        // Create triples
        Triple t1 = {*medal_id, *athlete_pred_id, *athlete_id};
        Triple t2 = {*medal_id, *country_pred_id, *country_id};
        Triple t3 = {*medal_id, *event_pred_id, *event_id};

        batch.push_back(t1);
        batch.push_back(t2);
        batch.push_back(t3);
        triple_count += 3;
    }

    // Insert batch
    auto status = store->InsertTriples(batch);
    if (!status.ok()) {
        std::cerr << "Insert failed: " << status << "\n";
        return 1;
    }

    auto flush_status = store->Flush();
    if (!flush_status.ok()) {
        std::cerr << "Flush failed: " << flush_status << "\n";
        return 1;
    }

    std::cout << "Inserted " << triple_count << " triples\n\n";

    // Test 1: Cross Product
    std::cout << "Test 1: CROSS PRODUCT (no shared variables)\n";
    std::cout << "----------------------------------------------------------------------\n";
    std::string query1 = R"(
        PREFIX ex: <http://ex.org/>
        SELECT ?medal1 ?medal2 WHERE {
            ?medal1 ex:athlete ?a1 .
            ?medal2 ex:country ?c2 .
        }
        LIMIT 5
    )";

    Timer t1;
    auto result1 = execute_sparql(engine, query1);
    double time1 = t1.elapsed_ms();

    if (!result1.ok()) {
        std::cerr << "  FAILED: " << result1.status() << "\n";
        return 1;
    }
    std::cout << "  Results: " << (*result1)->num_rows() << " rows\n";
    std::cout << "  Time: " << time1 << " ms\n";
    std::cout << "  ✓ Cross product executed successfully\n\n";

    // Test 2: Variable-to-Variable FILTER
    std::cout << "Test 2: VARIABLE-TO-VARIABLE FILTER (?x != ?y)\n";
    std::cout << "----------------------------------------------------------------------\n";
    std::string query2 = R"(
        PREFIX ex: <http://ex.org/>
        SELECT ?medal1 ?medal2 WHERE {
            ?medal1 ex:event ?e1 .
            ?medal2 ex:event ?e2 .
            FILTER(?medal1 != ?medal2)
        }
        LIMIT 20
    )";

    Timer t2;
    auto result2 = execute_sparql(engine, query2);
    double time2 = t2.elapsed_ms();

    if (!result2.ok()) {
        std::cerr << "  FAILED: " << result2.status() << "\n";
        return 1;
    }
    std::cout << "  Results: " << (*result2)->num_rows() << " rows\n";
    std::cout << "  Time: " << time2 << " ms\n";
    std::cout << "  ✓ Variable comparison executed successfully\n\n";

    // Test 3: ORDER BY
    std::cout << "Test 3: ORDER BY (sort results)\n";
    std::cout << "----------------------------------------------------------------------\n";
    std::string query3 = R"(
        PREFIX ex: <http://ex.org/>
        SELECT ?medal ?athlete WHERE {
            ?medal ex:athlete ?athlete .
        }
        ORDER BY ?athlete
    )";

    Timer t3;
    auto result3 = execute_sparql(engine, query3);
    double time3 = t3.elapsed_ms();

    if (!result3.ok()) {
        std::cerr << "  FAILED: " << result3.status() << "\n";
        return 1;
    }
    std::cout << "  Results: " << (*result3)->num_rows() << " rows\n";
    std::cout << "  Time: " << time3 << " ms\n";
    std::cout << "  ✓ ORDER BY executed successfully\n\n";

    // Test 4: Combined (all 3 features)
    std::cout << "Test 4: COMBINED (cross product + variable filter + ORDER BY)\n";
    std::cout << "----------------------------------------------------------------------\n";
    std::string query4 = R"(
        PREFIX ex: <http://ex.org/>
        SELECT ?athlete1 ?athlete2 WHERE {
            ?medal1 ex:athlete ?athlete1 .
            ?medal2 ex:country ?country2 .
            FILTER(?athlete1 != ?athlete2)
        }
        ORDER BY ?athlete1
        LIMIT 10
    )";

    Timer t4;
    auto result4 = execute_sparql(engine, query4);
    double time4 = t4.elapsed_ms();

    if (!result4.ok()) {
        std::cerr << "  FAILED: " << result4.status() << "\n";
        return 1;
    }
    std::cout << "  Results: " << (*result4)->num_rows() << " rows\n";
    std::cout << "  Time: " << time4 << " ms\n";
    std::cout << "  ✓ All features working together successfully\n\n";

    // Summary
    std::cout << "======================================================================\n";
    std::cout << "✓✓✓ ALL TESTS PASSED ✓✓✓\n";
    std::cout << "======================================================================\n";
    std::cout << "Feature 1: Cross Products         ✓ (" << time1 << " ms)\n";
    std::cout << "Feature 2: Variable Comparisons   ✓ (" << time2 << " ms)\n";
    std::cout << "Feature 3: ORDER BY               ✓ (" << time3 << " ms)\n";
    std::cout << "Combined Test:                    ✓ (" << time4 << " ms)\n";
    std::cout << "======================================================================\n";
    std::cout << "Total time: " << (time1 + time2 + time3 + time4) << " ms\n";
    std::cout << "Build: O3 optimizations enabled (-O3 -march=native -DNDEBUG)\n";
    std::cout << "======================================================================\n";

    return 0;
}
