/**
 * End-to-end integration test for SPARQL query engine
 *
 * Tests the complete pipeline:
 *   SPARQL text → Parser → AST → Planner → Operators → Executor → Results
 *
 * Verifies:
 * 1. Layer 1 (Storage): TripleStore + Vocabulary
 * 2. Layer 2 (Operators): ScanOperator, ZipperJoinOperator, ExpressionFilterOperator
 * 3. Layer 3 (Planning): QueryPlanner converts AST to operator trees
 * 4. Execution: QueryExecutor runs operators and produces Arrow results
 */

#include <sabot_ql/storage/triple_store.h>
#include <sabot_ql/storage/vocabulary.h>
#include <sabot_ql/sparql/parser.h>
#include <sabot_ql/sparql/planner.h>
#include <sabot_ql/sparql/query_engine.h>
#include <sabot_ql/execution/executor.h>
#include <arrow/api.h>
#include <arrow/compute/api.h>
#include <arrow/compute/initialize.h>
#include <iostream>
#include <cassert>

using namespace sabot_ql;
using namespace sabot_ql::sparql;

// ANSI color codes for output
#define COLOR_GREEN "\033[32m"
#define COLOR_RED "\033[31m"
#define COLOR_RESET "\033[0m"

void AssertTrue(bool condition, const std::string& message) {
    if (!condition) {
        std::cerr << COLOR_RED << "✗ FAILED: " << message << COLOR_RESET << "\n";
        std::exit(1);
    }
    std::cout << COLOR_GREEN << "✓ PASSED: " << message << COLOR_RESET << "\n";
}

void AssertArrowOk(const arrow::Status& status, const std::string& message) {
    if (!status.ok()) {
        std::cerr << COLOR_RED << "✗ FAILED: " << message << "\n"
                  << "  Arrow error: " << status.ToString() << COLOR_RESET << "\n";
        std::exit(1);
    }
    std::cout << COLOR_GREEN << "✓ PASSED: " << message << COLOR_RESET << "\n";
}

int main() {
    std::cout << "\n========================================\n";
    std::cout << "SPARQL End-to-End Integration Test\n";
    std::cout << "========================================\n\n";

    // Initialize Arrow compute functions
    auto init_status = arrow::compute::Initialize();
    if (!init_status.ok()) {
        std::cerr << COLOR_RED << "Failed to initialize Arrow compute: "
                  << init_status.ToString() << COLOR_RESET << "\n";
        return 1;
    }

    // ================================================================
    // Setup: Create store, vocabulary, and load test data
    // ================================================================

    std::cout << "Phase 1: Storage Setup\n";
    std::cout << "----------------------\n";

    // Create temporary storage
    std::string test_path = "/tmp/test_sparql_e2e";
    std::string cmd = "rm -rf " + test_path;
    std::system(cmd.c_str());

    // Create triple store
    auto store_result = CreateTripleStore(test_path);
    AssertArrowOk(store_result.status(), "Create triple store");
    auto store = *store_result;

    // Create vocabulary
    auto vocab_result = CreateVocabulary(test_path);
    AssertArrowOk(vocab_result.status(), "Create vocabulary");
    auto vocab = *vocab_result;

    // Add test data:
    //   <Alice> <knows> <Bob>
    //   <Alice> <age> "30"
    //   <Bob> <age> "25"
    //   <Charlie> <age> "35"

    std::cout << "\nPhase 2: Load Test Data\n";
    std::cout << "----------------------\n";

    // Add terms to vocabulary
    auto alice_id = vocab->AddTerm(Term::IRI("http://example.org/Alice"));
    auto bob_id = vocab->AddTerm(Term::IRI("http://example.org/Bob"));
    auto charlie_id = vocab->AddTerm(Term::IRI("http://example.org/Charlie"));
    auto knows_id = vocab->AddTerm(Term::IRI("http://example.org/knows"));
    auto age_id = vocab->AddTerm(Term::IRI("http://example.org/age"));

    AssertArrowOk(alice_id.status(), "Add Alice to vocabulary");
    AssertArrowOk(bob_id.status(), "Add Bob to vocabulary");
    AssertArrowOk(charlie_id.status(), "Add Charlie to vocabulary");
    AssertArrowOk(knows_id.status(), "Add 'knows' to vocabulary");
    AssertArrowOk(age_id.status(), "Add 'age' to vocabulary");

    // Add literal values (ages)
    auto age_30 = vocab->AddTerm(Term::Literal("30", "", "http://www.w3.org/2001/XMLSchema#integer"));
    auto age_25 = vocab->AddTerm(Term::Literal("25", "", "http://www.w3.org/2001/XMLSchema#integer"));
    auto age_35 = vocab->AddTerm(Term::Literal("35", "", "http://www.w3.org/2001/XMLSchema#integer"));

    AssertArrowOk(age_30.status(), "Add age 30 to vocabulary");
    AssertArrowOk(age_25.status(), "Add age 25 to vocabulary");
    AssertArrowOk(age_35.status(), "Add age 35 to vocabulary");

    // Insert triples
    std::cout << "\nInserting triples with IDs:\n";
    std::cout << "  Alice ID: " << alice_id->getBits() << "\n";
    std::cout << "  Bob ID: " << bob_id->getBits() << "\n";
    std::cout << "  Charlie ID: " << charlie_id->getBits() << "\n";
    std::cout << "  knows ID: " << knows_id->getBits() << "\n";
    std::cout << "  age ID: " << age_id->getBits() << "\n";
    std::cout << "  age 30 ID: " << age_30->getBits() << "\n";
    std::cout << "  age 25 ID: " << age_25->getBits() << "\n";
    std::cout << "  age 35 ID: " << age_35->getBits() << "\n";

    std::vector<Triple> triples = {
        Triple{*alice_id, *knows_id, *bob_id},
        Triple{*alice_id, *age_id, *age_30},
        Triple{*bob_id, *age_id, *age_25},
        Triple{*charlie_id, *age_id, *age_35}
    };

    std::cout << "\nTriples being inserted:\n";
    std::cout << "  Triple 1: " << alice_id->getBits() << " " << knows_id->getBits() << " " << bob_id->getBits() << "\n";
    std::cout << "  Triple 2: " << alice_id->getBits() << " " << age_id->getBits() << " " << age_30->getBits() << "\n";
    std::cout << "  Triple 3: " << bob_id->getBits() << " " << age_id->getBits() << " " << age_25->getBits() << "\n";
    std::cout << "  Triple 4: " << charlie_id->getBits() << " " << age_id->getBits() << " " << age_35->getBits() << "\n";

    auto insert_status = store->InsertTriples(triples);
    AssertArrowOk(insert_status, "Insert 4 test triples");

    auto flush_status = store->Flush();
    AssertArrowOk(flush_status, "Flush store to disk");

    std::cout << "\nTest data loaded:\n";
    std::cout << "  <Alice> <knows> <Bob>\n";
    std::cout << "  <Alice> <age> \"30\"\n";
    std::cout << "  <Bob> <age> \"25\"\n";
    std::cout << "  <Charlie> <age> \"35\"\n";

    // ================================================================
    // Test 1: Simple scan - SELECT ?s ?o WHERE { ?s <knows> ?o }
    // ================================================================

    std::cout << "\n\nPhase 3: Test 1 - Simple Scan\n";
    std::cout << "------------------------------\n";
    std::cout << "Query: SELECT ?s ?o WHERE { ?s <knows> ?o }\n";
    std::cout << "Expected: 1 result (Alice knows Bob)\n\n";

    std::string sparql1 = R"(
        SELECT ?s ?o WHERE {
            ?s <http://example.org/knows> ?o
        }
    )";

    // Parse query
    std::cout << "  Parsing query...\n";
    auto parse_result1 = ParseSPARQL(sparql1);
    AssertArrowOk(parse_result1.status(), "Parse query 1");
    auto query1 = *parse_result1;

    // Extract SELECT query
    std::cout << "  Extracting SELECT query...\n";
    auto select1 = std::get<SelectQuery>(query1.query_body);

    // Create query engine and execute
    std::cout << "  Creating query engine...\n";
    QueryEngine engine(store, vocab);

    std::cout << "  Executing query...\n";
    auto result1 = engine.ExecuteSelect(select1);
    AssertArrowOk(result1.status(), "Execute query 1");

    std::cout << "  Getting result table...\n";
    auto table1 = *result1;

    // Verify results
    AssertTrue(table1->num_rows() == 1, "Query 1 returns 1 row");
    AssertTrue(table1->num_columns() == 2, "Query 1 returns 2 columns (s, o)");

    std::cout << "Result table:\n";
    std::cout << table1->ToString() << "\n";

    // ================================================================
    // Test 2: Filter - SELECT ?s ?a WHERE { ?s <age> ?a FILTER(?a > 25) }
    // ================================================================

    std::cout << "\n\nPhase 4: Test 2 - Scan + Filter\n";
    std::cout << "--------------------------------\n";
    std::cout << "Query: SELECT ?s ?a WHERE { ?s <age> ?a FILTER(?a > 25) }\n";
    std::cout << "Expected: 2 results (Alice:30, Charlie:35)\n\n";

    std::string sparql2 = R"(
        SELECT ?s ?a WHERE {
            ?s <http://example.org/age> ?a .
            FILTER(?a > 25)
        }
    )";

    auto parse_result2 = ParseSPARQL(sparql2);
    AssertArrowOk(parse_result2.status(), "Parse query 2");
    auto query2 = *parse_result2;

    auto select2 = std::get<SelectQuery>(query2.query_body);

    auto result2 = engine.ExecuteSelect(select2);
    AssertArrowOk(result2.status(), "Execute query 2");
    auto table2 = *result2;

    // Verify results
    AssertTrue(table2->num_rows() == 2, "Query 2 returns 2 rows");
    AssertTrue(table2->num_columns() == 2, "Query 2 returns 2 columns (s, a)");

    std::cout << "Result table:\n";
    std::cout << table2->ToString() << "\n";

    // ================================================================
    // Test 3: Join - SELECT ?person ?age WHERE { ?person <knows> ?friend . ?person <age> ?age }
    // ================================================================

    std::cout << "\n\nPhase 5: Test 3 - Join\n";
    std::cout << "----------------------\n";
    std::cout << "Query: SELECT ?person ?age WHERE { ?person <knows> ?friend . ?person <age> ?age }\n";
    std::cout << "Expected: 1 result (Alice:30)\n\n";

    std::string sparql3 = R"(
        SELECT ?person ?age WHERE {
            ?person <http://example.org/knows> ?friend .
            ?person <http://example.org/age> ?age
        }
    )";

    auto parse_result3 = ParseSPARQL(sparql3);
    AssertArrowOk(parse_result3.status(), "Parse query 3");
    auto query3 = *parse_result3;

    auto select3 = std::get<SelectQuery>(query3.query_body);

    auto result3 = engine.ExecuteSelect(select3);
    AssertArrowOk(result3.status(), "Execute query 3");
    auto table3 = *result3;

    // Verify results
    AssertTrue(table3->num_rows() == 1, "Query 3 returns 1 row");
    AssertTrue(table3->num_columns() == 2, "Query 3 returns 2 columns (person, age)");

    std::cout << "Result table:\n";
    std::cout << table3->ToString() << "\n";

    // ================================================================
    // Test 4: Complex filter - SELECT ?s WHERE { ?s <age> ?a FILTER(?a >= 30 && ?a < 35) }
    // ================================================================

    std::cout << "\n\nPhase 6: Test 4 - Complex Filter (AND)\n";
    std::cout << "---------------------------------------\n";
    std::cout << "Query: SELECT ?s WHERE { ?s <age> ?a FILTER(?a >= 30 && ?a < 35) }\n";
    std::cout << "Expected: 1 result (Alice:30)\n\n";

    std::string sparql4 = R"(
        SELECT ?s WHERE {
            ?s <http://example.org/age> ?a .
            FILTER(?a >= 30 && ?a < 35)
        }
    )";

    auto parse_result4 = ParseSPARQL(sparql4);
    AssertArrowOk(parse_result4.status(), "Parse query 4");
    auto query4 = *parse_result4;

    auto select4 = std::get<SelectQuery>(query4.query_body);

    auto result4 = engine.ExecuteSelect(select4);
    AssertArrowOk(result4.status(), "Execute query 4");
    auto table4 = *result4;

    // Verify results
    AssertTrue(table4->num_rows() == 1, "Query 4 returns 1 row");
    AssertTrue(table4->num_columns() == 1, "Query 4 returns 1 column (s)");

    std::cout << "Result table:\n";
    std::cout << table4->ToString() << "\n";

    // ================================================================
    // Test 5: All ages - SELECT ?s ?a WHERE { ?s <age> ?a }
    // ================================================================

    std::cout << "\n\nPhase 7: Test 5 - Scan All Ages\n";
    std::cout << "--------------------------------\n";
    std::cout << "Query: SELECT ?s ?a WHERE { ?s <age> ?a }\n";
    std::cout << "Expected: 3 results (Alice:30, Bob:25, Charlie:35)\n\n";

    std::string sparql5 = R"(
        SELECT ?s ?a WHERE {
            ?s <http://example.org/age> ?a
        }
    )";

    auto parse_result5 = ParseSPARQL(sparql5);
    AssertArrowOk(parse_result5.status(), "Parse query 5");
    auto query5 = *parse_result5;

    auto select5 = std::get<SelectQuery>(query5.query_body);

    auto result5 = engine.ExecuteSelect(select5);
    AssertArrowOk(result5.status(), "Execute query 5");
    auto table5 = *result5;

    // Verify results
    AssertTrue(table5->num_rows() == 3, "Query 5 returns 3 rows");
    AssertTrue(table5->num_columns() == 2, "Query 5 returns 2 columns (s, a)");

    std::cout << "Result table:\n";
    std::cout << table5->ToString() << "\n";

    // ================================================================
    // Success!
    // ================================================================

    std::cout << "\n========================================\n";
    std::cout << COLOR_GREEN << "✓ ALL TESTS PASSED!" << COLOR_RESET << "\n";
    std::cout << "========================================\n\n";

    std::cout << "Verified components:\n";
    std::cout << "  ✓ Layer 1: TripleStore + Vocabulary (storage)\n";
    std::cout << "  ✓ Layer 2: ScanOperator (triple pattern scans)\n";
    std::cout << "  ✓ Layer 2: ExpressionFilterOperator (FILTER clauses)\n";
    std::cout << "  ✓ Layer 2: ZipperJoinOperator (joins)\n";
    std::cout << "  ✓ Layer 3: QueryPlanner (AST → operators)\n";
    std::cout << "  ✓ Parser: SPARQL text → AST\n";
    std::cout << "  ✓ Executor: Operator tree → Arrow results\n";
    std::cout << "\n";

    return 0;
}
