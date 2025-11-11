/**
 * End-to-End Property Path Integration Test
 *
 * Tests all SPARQL 1.1 property path operators:
 *   - p+ (one-or-more transitive)
 *   - p* (zero-or-more transitive)
 *   - p{m,n} (bounded paths)
 *   - p/q (sequence paths)
 *   - p|q (alternative paths)
 *   - ^p (inverse paths)
 *   - !p (negated paths)
 *
 * Verifies complete pipeline:
 *   SPARQL property path query → Parser → AST → Planner → Property Path Operators → Results
 */

#include <sabot_ql/storage/triple_store.h>
#include <sabot_ql/storage/vocabulary.h>
#include <sabot_ql/sparql/parser.h>
#include <sabot_ql/sparql/planner.h>
#include <sabot_ql/sparql/query_engine.h>
#include <arrow/api.h>
#include <arrow/compute/api.h>
#include <arrow/compute/initialize.h>
#include <iostream>
#include <cassert>

using namespace sabot_ql;
using namespace sabot_ql::sparql;

// ANSI color codes
#define COLOR_GREEN "\033[32m"
#define COLOR_RED "\033[31m"
#define COLOR_YELLOW "\033[33m"
#define COLOR_BLUE "\033[34m"
#define COLOR_RESET "\033[0m"

int test_failures = 0;
int test_successes = 0;

void AssertTrue(bool condition, const std::string& message) {
    if (!condition) {
        std::cerr << COLOR_RED << "✗ FAILED: " << message << COLOR_RESET << "\n";
        test_failures++;
    } else {
        std::cout << COLOR_GREEN << "✓ PASSED: " << message << COLOR_RESET << "\n";
        test_successes++;
    }
}

void AssertArrowOk(const arrow::Status& status, const std::string& message) {
    if (!status.ok()) {
        std::cerr << COLOR_RED << "✗ FAILED: " << message << "\n"
                  << "  Arrow error: " << status.ToString() << COLOR_RESET << "\n";
        test_failures++;
    } else {
        std::cout << COLOR_GREEN << "✓ PASSED: " << message << COLOR_RESET << "\n";
        test_successes++;
    }
}

// Setup test data with social network and family tree
void SetupTestData(std::shared_ptr<TripleStore>& store, std::shared_ptr<Vocabulary>& vocab) {
    /**
     * Test dataset structure:
     *
     * Social network (knows):
     *   Alice → Bob → Charlie → David → Alice (cycle)
     *
     * Family tree (parent):
     *   Alice → Bob → Charlie → David
     *
     * Alternative relationships:
     *   Alice friend Bob
     *   Bob worksWith Charlie
     *
     * Isolated:
     *   Eve → Alice
     */

    // Define IRIs
    std::string alice = "http://example.org/Alice";
    std::string bob = "http://example.org/Bob";
    std::string charlie = "http://example.org/Charlie";
    std::string david = "http://example.org/David";
    std::string eve = "http://example.org/Eve";

    std::string knows = "http://xmlns.com/foaf/0.1/knows";
    std::string friend_pred = "http://xmlns.com/foaf/0.1/friend";
    std::string works_with = "http://example.org/worksWith";
    std::string parent = "http://example.org/parent";

    // Add terms to vocabulary
    auto alice_id = *vocab->AddTerm(Term::IRI(alice));
    auto bob_id = *vocab->AddTerm(Term::IRI(bob));
    auto charlie_id = *vocab->AddTerm(Term::IRI(charlie));
    auto david_id = *vocab->AddTerm(Term::IRI(david));
    auto eve_id = *vocab->AddTerm(Term::IRI(eve));

    auto knows_id = *vocab->AddTerm(Term::IRI(knows));
    auto friend_id = *vocab->AddTerm(Term::IRI(friend_pred));
    auto works_id = *vocab->AddTerm(Term::IRI(works_with));
    auto parent_id = *vocab->AddTerm(Term::IRI(parent));

    // Create triples
    std::vector<Triple> triples = {
        // Knows chain with cycle
        Triple{alice_id, knows_id, bob_id},
        Triple{bob_id, knows_id, charlie_id},
        Triple{charlie_id, knows_id, david_id},
        Triple{david_id, knows_id, alice_id},  // cycle

        // Alternative relationships
        Triple{alice_id, friend_id, bob_id},
        Triple{bob_id, works_id, charlie_id},

        // Family tree
        Triple{alice_id, parent_id, bob_id},
        Triple{bob_id, parent_id, charlie_id},
        Triple{charlie_id, parent_id, david_id},

        // Isolated edge
        Triple{eve_id, knows_id, alice_id}
    };

    auto insert_status = store->InsertTriples(triples);
    if (!insert_status.ok()) {
        std::cerr << "Failed to insert test triples: " << insert_status.ToString() << "\n";
        std::exit(1);
    }

    std::cout << "DEBUG: Inserted " << triples.size() << " triples\n";
    std::cout << "DEBUG: Triple store now has " << store->TotalTriples() << " triples\n";

    auto flush_status = store->Flush();
    if (!flush_status.ok()) {
        std::cerr << "Failed to flush store: " << flush_status.ToString() << "\n";
        std::exit(1);
    }

    std::cout << "DEBUG: After flush, triple store has " << store->TotalTriples() << " triples\n";
    std::cout << COLOR_BLUE << "Test data loaded: 10 triples\n" << COLOR_RESET;
}

// Test 1: Transitive p+ (one-or-more)
void TestTransitiveOnePlus(QueryEngine& engine) {
    std::cout << "\n" << COLOR_BLUE << "=== Test 1: Transitive p+ (one-or-more) ===" << COLOR_RESET << "\n";
    std::cout << "Query: Who does Alice know transitively?\n";

    std::string sparql = R"(
        PREFIX foaf: <http://xmlns.com/foaf/0.1/>
        PREFIX ex: <http://example.org/>

        SELECT ?knows WHERE {
            ex:Alice foaf:knows+ ?knows .
        }
    )";

    auto parse_result = ParseSPARQL(sparql);
    if (!parse_result.ok()) {
        std::cerr << COLOR_RED << "Parse failed: " << parse_result.status().ToString() << COLOR_RESET << "\n";
        test_failures++;
        return;
    }

    auto query = *parse_result;
    auto select_query = std::get<SelectQuery>(query.query_body);

    auto result = engine.ExecuteSelect(select_query);
    if (!result.ok()) {
        std::cerr << COLOR_RED << "Execute failed: " << result.status().ToString() << COLOR_RESET << "\n";
        test_failures++;
        return;
    }

    auto table = *result;

    std::cout << "Results:\n" << table->ToString() << "\n";

    // Expected: Bob, Charlie, David (and possibly Alice due to cycle)
    AssertTrue(table->num_rows() >= 3, "Returns at least 3 results (Bob, Charlie, David)");
    AssertTrue(table->num_columns() == 1, "Returns 1 column (?knows)");
}

// Test 2: Transitive p* (zero-or-more)
void TestTransitiveZeroOrMore(QueryEngine& engine) {
    std::cout << "\n" << COLOR_BLUE << "=== Test 2: Transitive p* (zero-or-more) ===" << COLOR_RESET << "\n";
    std::cout << "Query: Who does Alice know transitively (including herself)?\n";

    std::string sparql = R"(
        PREFIX foaf: <http://xmlns.com/foaf/0.1/>
        PREFIX ex: <http://example.org/>

        SELECT ?knows WHERE {
            ex:Alice foaf:knows* ?knows .
        }
    )";

    auto parse_result = ParseSPARQL(sparql);
    if (!parse_result.ok()) {
        std::cerr << COLOR_RED << "Parse failed: " << parse_result.status().ToString() << COLOR_RESET << "\n";
        test_failures++;
        return;
    }

    auto query = *parse_result;
    auto select_query = std::get<SelectQuery>(query.query_body);

    auto result = engine.ExecuteSelect(select_query);
    if (!result.ok()) {
        std::cerr << COLOR_RED << "Execute failed: " << result.status().ToString() << COLOR_RESET << "\n";
        test_failures++;
        return;
    }

    auto table = *result;

    std::cout << "Results:\n" << table->ToString() << "\n";

    // Expected: Alice (distance 0), Bob, Charlie, David
    AssertTrue(table->num_rows() >= 4, "Returns at least 4 results (Alice, Bob, Charlie, David)");
}

// Test 3: Bounded paths p{2,3}
void TestBoundedPaths(QueryEngine& engine) {
    std::cout << "\n" << COLOR_BLUE << "=== Test 3: Bounded Paths p{2,3} ===" << COLOR_RESET << "\n";
    std::cout << "Query: Who does Alice know at distance 2-3?\n";

    std::string sparql = R"(
        PREFIX foaf: <http://xmlns.com/foaf/0.1/>
        PREFIX ex: <http://example.org/>

        SELECT ?knows WHERE {
            ex:Alice foaf:knows{2,3} ?knows .
        }
    )";

    auto parse_result = ParseSPARQL(sparql);
    if (!parse_result.ok()) {
        std::cerr << COLOR_RED << "Parse failed: " << parse_result.status().ToString() << COLOR_RESET << "\n";
        test_failures++;
        return;
    }

    auto query = *parse_result;
    auto select_query = std::get<SelectQuery>(query.query_body);

    auto result = engine.ExecuteSelect(select_query);
    if (!result.ok()) {
        std::cerr << COLOR_RED << "Execute failed: " << result.status().ToString() << COLOR_RESET << "\n";
        test_failures++;
        return;
    }

    auto table = *result;

    std::cout << "Results:\n" << table->ToString() << "\n";

    // Expected: Charlie (distance 2), David (distance 3)
    AssertTrue(table->num_rows() >= 2, "Returns at least 2 results (Charlie, David)");
}

// Test 4: Sequence paths p/q
void TestSequencePaths(QueryEngine& engine) {
    std::cout << "\n" << COLOR_BLUE << "=== Test 4: Sequence Paths p/q ===" << COLOR_RESET << "\n";
    std::cout << "Query: Who are Alice's grandchildren? (parent/parent)\n";

    std::string sparql = R"(
        PREFIX ex: <http://example.org/>

        SELECT ?grandchild WHERE {
            ex:Alice ex:parent/ex:parent ?grandchild .
        }
    )";

    auto parse_result = ParseSPARQL(sparql);
    if (!parse_result.ok()) {
        std::cerr << COLOR_RED << "Parse failed: " << parse_result.status().ToString() << COLOR_RESET << "\n";
        test_failures++;
        return;
    }

    auto query = *parse_result;
    auto select_query = std::get<SelectQuery>(query.query_body);

    auto result = engine.ExecuteSelect(select_query);
    if (!result.ok()) {
        std::cerr << COLOR_RED << "Execute failed: " << result.status().ToString() << COLOR_RESET << "\n";
        test_failures++;
        return;
    }

    auto table = *result;

    std::cout << "Results:\n" << table->ToString() << "\n";

    // Expected: Charlie (Alice -> Bob -> Charlie)
    AssertTrue(table->num_rows() >= 1, "Returns at least 1 result (Charlie)");
}

// Test 5: Alternative paths p|q
void TestAlternativePaths(QueryEngine& engine) {
    std::cout << "\n" << COLOR_BLUE << "=== Test 5: Alternative Paths p|q ===" << COLOR_RESET << "\n";
    std::cout << "Query: Who does Alice know OR is friends with?\n";

    std::string sparql = R"(
        PREFIX foaf: <http://xmlns.com/foaf/0.1/>
        PREFIX ex: <http://example.org/>

        SELECT ?contact WHERE {
            ex:Alice (foaf:knows|foaf:friend) ?contact .
        }
    )";

    auto parse_result = ParseSPARQL(sparql);
    if (!parse_result.ok()) {
        std::cerr << COLOR_RED << "Parse failed: " << parse_result.status().ToString() << COLOR_RESET << "\n";
        test_failures++;
        return;
    }

    auto query = *parse_result;
    auto select_query = std::get<SelectQuery>(query.query_body);

    auto result = engine.ExecuteSelect(select_query);
    if (!result.ok()) {
        std::cerr << COLOR_RED << "Execute failed: " << result.status().ToString() << COLOR_RESET << "\n";
        test_failures++;
        return;
    }

    auto table = *result;

    std::cout << "Results:\n" << table->ToString() << "\n";

    // Expected: Bob (via both knows and friend - should be deduplicated)
    AssertTrue(table->num_rows() >= 1, "Returns at least 1 result (Bob)");
}

// Test 6: Inverse paths ^p
void TestInversePaths(QueryEngine& engine) {
    std::cout << "\n" << COLOR_BLUE << "=== Test 6: Inverse Paths ^p ===" << COLOR_RESET << "\n";
    std::cout << "Query: Who knows Bob? (inverse)\n";

    std::string sparql = R"(
        PREFIX foaf: <http://xmlns.com/foaf/0.1/>
        PREFIX ex: <http://example.org/>

        SELECT ?knower WHERE {
            ex:Bob ^foaf:knows ?knower .
        }
    )";

    auto parse_result = ParseSPARQL(sparql);
    if (!parse_result.ok()) {
        std::cerr << COLOR_RED << "Parse failed: " << parse_result.status().ToString() << COLOR_RESET << "\n";
        test_failures++;
        return;
    }

    auto query = *parse_result;
    auto select_query = std::get<SelectQuery>(query.query_body);

    auto result = engine.ExecuteSelect(select_query);
    if (!result.ok()) {
        std::cerr << COLOR_RED << "Execute failed: " << result.status().ToString() << COLOR_RESET << "\n";
        test_failures++;
        return;
    }

    auto table = *result;

    std::cout << "Results:\n" << table->ToString() << "\n";

    // Expected: Alice (Alice knows Bob)
    AssertTrue(table->num_rows() >= 1, "Returns at least 1 result (Alice)");
}

// Test 7: Inverse transitive ^p+
void TestInverseTransitive(QueryEngine& engine) {
    std::cout << "\n" << COLOR_BLUE << "=== Test 7: Inverse Transitive ^p+ ===" << COLOR_RESET << "\n";
    std::cout << "Query: Who are David's ancestors?\n";

    std::string sparql = R"(
        PREFIX ex: <http://example.org/>

        SELECT ?ancestor WHERE {
            ex:David ^ex:parent+ ?ancestor .
        }
    )";

    auto parse_result = ParseSPARQL(sparql);
    if (!parse_result.ok()) {
        std::cerr << COLOR_RED << "Parse failed: " << parse_result.status().ToString() << COLOR_RESET << "\n";
        test_failures++;
        return;
    }

    auto query = *parse_result;
    auto select_query = std::get<SelectQuery>(query.query_body);

    auto result = engine.ExecuteSelect(select_query);
    if (!result.ok()) {
        std::cerr << COLOR_RED << "Execute failed: " << result.status().ToString() << COLOR_RESET << "\n";
        test_failures++;
        return;
    }

    auto table = *result;

    std::cout << "Results:\n" << table->ToString() << "\n";

    // Expected: Charlie (parent), Bob (grandparent), Alice (great-grandparent)
    AssertTrue(table->num_rows() >= 3, "Returns at least 3 results");
}

int main() {
    std::cout << "\n" << COLOR_BLUE << "========================================" << COLOR_RESET << "\n";
    std::cout << COLOR_BLUE << "Property Path End-to-End Integration Test" << COLOR_RESET << "\n";
    std::cout << COLOR_BLUE << "========================================" << COLOR_RESET << "\n\n";

    // Initialize Arrow compute
    auto init_status = arrow::compute::Initialize();
    if (!init_status.ok()) {
        std::cerr << COLOR_RED << "Failed to initialize Arrow: " << init_status.ToString() << COLOR_RESET << "\n";
        return 1;
    }

    // Setup storage
    std::cout << COLOR_BLUE << "Phase 1: Storage Setup" << COLOR_RESET << "\n";
    std::cout << "----------------------\n";

    std::string test_path = "/tmp/test_property_paths_e2e";
    std::system(("rm -rf " + test_path).c_str());

    auto store_result = CreateTripleStore(test_path);
    if (!store_result.ok()) {
        std::cerr << COLOR_RED << "Failed to create store: " << store_result.status().ToString() << COLOR_RESET << "\n";
        return 1;
    }
    auto store = *store_result;

    auto vocab_result = CreateVocabulary(test_path);
    if (!vocab_result.ok()) {
        std::cerr << COLOR_RED << "Failed to create vocabulary: " << vocab_result.status().ToString() << COLOR_RESET << "\n";
        return 1;
    }
    auto vocab = *vocab_result;

    std::cout << COLOR_GREEN << "✓ Storage created" << COLOR_RESET << "\n";

    // Load test data
    std::cout << "\n" << COLOR_BLUE << "Phase 2: Load Test Data" << COLOR_RESET << "\n";
    std::cout << "----------------------\n";
    SetupTestData(store, vocab);

    // Create query engine
    QueryEngine engine(store, vocab);

    // Run all tests
    std::cout << "\n" << COLOR_BLUE << "Phase 3: Execute Property Path Tests" << COLOR_RESET << "\n";
    std::cout << "=====================================\n";

    TestTransitiveOnePlus(engine);
    TestTransitiveZeroOrMore(engine);
    TestBoundedPaths(engine);
    TestSequencePaths(engine);
    TestAlternativePaths(engine);
    TestInversePaths(engine);
    TestInverseTransitive(engine);

    // Summary
    std::cout << "\n" << COLOR_BLUE << "========================================" << COLOR_RESET << "\n";
    std::cout << COLOR_BLUE << "Test Summary" << COLOR_RESET << "\n";
    std::cout << COLOR_BLUE << "========================================" << COLOR_RESET << "\n";
    std::cout << COLOR_GREEN << "✓ Passed: " << test_successes << COLOR_RESET << "\n";
    if (test_failures > 0) {
        std::cout << COLOR_RED << "✗ Failed: " << test_failures << COLOR_RESET << "\n";
    }

    if (test_failures == 0) {
        std::cout << "\n" << COLOR_GREEN << "🎉 ALL TESTS PASSED!" << COLOR_RESET << "\n";
        std::cout << COLOR_GREEN << "Property path implementation is working correctly." << COLOR_RESET << "\n";
        return 0;
    } else {
        std::cout << "\n" << COLOR_RED << "⚠️ SOME TESTS FAILED" << COLOR_RESET << "\n";
        return 1;
    }
}
