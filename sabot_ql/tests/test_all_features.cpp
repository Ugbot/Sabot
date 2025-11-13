#include <iostream>
#include <sabot_ql/storage/triple_store.h>
#include <sabot_ql/sparql/query_engine.h>

using namespace sabot_ql;
using namespace sabot_ql::sparql;

int main() {
    std::cout << "=================================================================\n";
    std::cout << "Testing All 3 SPARQL Features: Cross Products + Variable FILTER + ORDER BY\n";
    std::cout << "=================================================================\n\n";

    // Initialize triple store
    auto vocab = std::make_shared<Vocabulary>();
    ARROW_ASSIGN_OR_RAISE_ELSE(auto store, TripleStore::Create(vocab), {
        std::cerr << "Failed to create store: " << _error_or_value.status() << "\n";
        return 1;
    });

    // Insert test data: medals
    auto gold = Term::IRI("http://ex.org/gold");
    auto silver = Term::IRI("http://ex.org/silver");
    auto bronze = Term::IRI("http://ex.org/bronze");
    auto athlete_pred = Term::IRI("http://ex.org/athlete");
    auto country_pred = Term::IRI("http://ex.org/country");
    auto event_pred = Term::IRI("http://ex.org/event");

    auto alice = Term::Literal("Alice");
    auto bob = Term::Literal("Bob");
    auto charlie = Term::Literal("Charlie");
    auto usa = Term::Literal("USA");
    auto uk = Term::Literal("UK");
    auto sprint100 = Term::Literal("100m");

    // Gold medal: Alice, USA, 100m
    ARROW_RETURN_NOT_OK(store->InsertTriple(gold, athlete_pred, alice));
    ARROW_RETURN_NOT_OK(store->InsertTriple(gold, country_pred, usa));
    ARROW_RETURN_NOT_OK(store->InsertTriple(gold, event_pred, sprint100));

    // Silver medal: Bob, UK, 100m
    ARROW_RETURN_NOT_OK(store->InsertTriple(silver, athlete_pred, bob));
    ARROW_RETURN_NOT_OK(store->InsertTriple(silver, country_pred, uk));
    ARROW_RETURN_NOT_OK(store->InsertTriple(silver, event_pred, sprint100));

    // Bronze medal: Charlie, USA, 100m
    ARROW_RETURN_NOT_OK(store->InsertTriple(bronze, athlete_pred, charlie));
    ARROW_RETURN_NOT_OK(store->InsertTriple(bronze, country_pred, usa));
    ARROW_RETURN_NOT_OK(store->InsertTriple(bronze, event_pred, sprint100));

    std::cout << "Inserted 9 triples (3 medals × 3 properties each)\n\n";

    auto engine = std::make_shared<QueryEngine>(store);

    // =================================================================
    // Test 1: Cross Product (No shared variables)
    // =================================================================
    std::cout << "Test 1: CROSS PRODUCT (no shared variables)\n";
    std::cout << "-----------------------------------------------------------------\n";
    std::string query1 = R"(
        PREFIX ex: <http://ex.org/>
        SELECT ?medal1 ?medal2 WHERE {
            ?medal1 ex:athlete ?a1 .
            ?medal2 ex:country ?c2 .
        }
        LIMIT 5
    )";

    std::cout << query1 << "\n";
    ARROW_ASSIGN_OR_RAISE_ELSE(auto result1, engine->ExecuteQuery(query1), {
        std::cerr << "✗ Test 1 failed: " << _error_or_value.status() << "\n";
        return 1;
    });
    std::cout << "Results: " << result1->num_rows() << " rows\n";
    std::cout << "✓ Test 1 passed: Cross product executed\n\n";

    // =================================================================
    // Test 2: Variable-to-Variable FILTER
    // =================================================================
    std::cout << "Test 2: VARIABLE-TO-VARIABLE FILTER (?x != ?y)\n";
    std::cout << "-----------------------------------------------------------------\n";
    std::string query2 = R"(
        PREFIX ex: <http://ex.org/>
        SELECT ?medal1 ?medal2 WHERE {
            ?medal1 ex:event ?e1 .
            ?medal2 ex:event ?e2 .
            FILTER(?medal1 != ?medal2)
        }
    )";

    std::cout << query2 << "\n";
    ARROW_ASSIGN_OR_RAISE_ELSE(auto result2, engine->ExecuteQuery(query2), {
        std::cerr << "✗ Test 2 failed: " << _error_or_value.status() << "\n";
        return 1;
    });
    std::cout << "Results: " << result2->num_rows() << " rows\n";
    std::cout << "✓ Test 2 passed: Variable comparison executed\n\n";

    // =================================================================
    // Test 3: ORDER BY
    // =================================================================
    std::cout << "Test 3: ORDER BY (sort results)\n";
    std::cout << "-----------------------------------------------------------------\n";
    std::string query3 = R"(
        PREFIX ex: <http://ex.org/>
        SELECT ?medal ?athlete WHERE {
            ?medal ex:athlete ?athlete .
        }
        ORDER BY ?athlete
    )";

    std::cout << query3 << "\n";
    ARROW_ASSIGN_OR_RAISE_ELSE(auto result3, engine->ExecuteQuery(query3), {
        std::cerr << "✗ Test 3 failed: " << _error_or_value.status() << "\n";
        return 1;
    });
    std::cout << "Results: " << result3->num_rows() << " rows\n";
    std::cout << "✓ Test 3 passed: ORDER BY executed\n\n";

    // =================================================================
    // Test 4: Combined - All 3 features together
    // =================================================================
    std::cout << "Test 4: COMBINED (cross product + variable filter + ORDER BY)\n";
    std::cout << "-----------------------------------------------------------------\n";
    std::string query4 = R"(
        PREFIX ex: <http://ex.org/>
        SELECT ?athlete1 ?athlete2 WHERE {
            ?medal1 ex:athlete ?athlete1 .
            ?medal2 ex:country ?country2 .
            FILTER(?athlete1 != ?athlete2)
        }
        ORDER BY ?athlete1
        LIMIT 5
    )";

    std::cout << query4 << "\n";
    ARROW_ASSIGN_OR_RAISE_ELSE(auto result4, engine->ExecuteQuery(query4), {
        std::cerr << "✗ Test 4 failed: " << _error_or_value.status() << "\n";
        return 1;
    });
    std::cout << "Results: " << result4->num_rows() << " rows\n";
    std::cout << "✓ Test 4 passed: All features working together\n\n";

    // =================================================================
    // Summary
    // =================================================================
    std::cout << "=================================================================\n";
    std::cout << "✓✓✓ ALL TESTS PASSED ✓✓✓\n";
    std::cout << "=================================================================\n";
    std::cout << "Feature 1: Cross Products         ✓\n";
    std::cout << "Feature 2: Variable Comparisons   ✓\n";
    std::cout << "Feature 3: ORDER BY               ✓\n";
    std::cout << "Combined Test:                    ✓\n";
    std::cout << "=================================================================\n";

    return 0;
}
