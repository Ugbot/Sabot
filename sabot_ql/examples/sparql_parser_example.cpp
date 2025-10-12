// Example: SPARQL Text Parser
//
// This demonstrates how to parse SPARQL query strings into AST structures:
// - Simple SELECT queries
// - Queries with FILTER clauses
// - Queries with OPTIONAL clauses
// - Queries with UNION
// - Queries with ORDER BY
// - Complex multi-clause queries
//
// The parser converts text queries into the same AST structures used
// by the programmatic query builder, so you can use either approach.

#include <sabot_ql/sparql/parser.h>
#include <sabot_ql/sparql/query_engine.h>
#include <sabot_ql/storage/triple_store.h>
#include <sabot_ql/storage/vocabulary.h>
#include <marble/db.h>
#include <iostream>
#include <string>

using namespace sabot_ql;
using namespace sabot_ql::sparql;

void PrintParseResult(const std::string& query_name, const std::string& query_text) {
    std::cout << "\n=== " << query_name << " ===" << std::endl;
    std::cout << "Query text:\n" << query_text << std::endl;

    auto parse_result = ParseSPARQL(query_text);
    if (!parse_result.ok()) {
        std::cerr << "Parse error: " << parse_result.status().ToString() << std::endl;
        return;
    }

    auto query = parse_result.ValueOrDie();
    std::cout << "\nParsed AST:\n" << query.ToString() << std::endl;
    std::cout << "✅ Parse successful!" << std::endl;
}

int main() {
    std::cout << "SPARQL Text Parser Examples" << std::endl;
    std::cout << "============================" << std::endl;

    // ========================================================================
    // Example 1: Simple SELECT query
    // ========================================================================
    PrintParseResult(
        "Simple SELECT",
        R"(
            SELECT ?person ?name
            WHERE {
                ?person <http://schema.org/name> ?name
            }
        )"
    );

    // ========================================================================
    // Example 2: SELECT * (all variables)
    // ========================================================================
    PrintParseResult(
        "SELECT *",
        R"(
            SELECT *
            WHERE {
                ?s ?p ?o
            }
        )"
    );

    // ========================================================================
    // Example 3: FILTER with comparison
    // ========================================================================
    PrintParseResult(
        "FILTER with comparison",
        R"(
            SELECT ?person ?age
            WHERE {
                ?person <http://schema.org/age> ?age .
                FILTER (?age > 30)
            }
        )"
    );

    // ========================================================================
    // Example 4: FILTER with logical operators
    // ========================================================================
    PrintParseResult(
        "FILTER with AND/OR",
        R"(
            SELECT ?person ?age ?name
            WHERE {
                ?person <http://schema.org/name> ?name .
                ?person <http://schema.org/age> ?age .
                FILTER (?age > 18 && ?age < 65)
            }
        )"
    );

    // ========================================================================
    // Example 5: OPTIONAL clause
    // ========================================================================
    PrintParseResult(
        "OPTIONAL clause",
        R"(
            SELECT ?person ?name ?phone
            WHERE {
                ?person <http://schema.org/name> ?name .
                OPTIONAL { ?person <http://schema.org/phone> ?phone }
            }
        )"
    );

    // ========================================================================
    // Example 6: Multiple OPTIONAL clauses
    // ========================================================================
    PrintParseResult(
        "Multiple OPTIONAL",
        R"(
            SELECT ?person ?name ?phone ?email
            WHERE {
                ?person <http://schema.org/name> ?name .
                OPTIONAL { ?person <http://schema.org/phone> ?phone }
                OPTIONAL { ?person <http://schema.org/email> ?email }
            }
        )"
    );

    // ========================================================================
    // Example 7: ORDER BY ascending
    // ========================================================================
    PrintParseResult(
        "ORDER BY ascending",
        R"(
            SELECT ?person ?age
            WHERE {
                ?person <http://schema.org/age> ?age
            }
            ORDER BY ASC(?age)
        )"
    );

    // ========================================================================
    // Example 8: ORDER BY descending
    // ========================================================================
    PrintParseResult(
        "ORDER BY descending",
        R"(
            SELECT ?person ?age
            WHERE {
                ?person <http://schema.org/age> ?age
            }
            ORDER BY DESC(?age)
        )"
    );

    // ========================================================================
    // Example 9: LIMIT and OFFSET
    // ========================================================================
    PrintParseResult(
        "LIMIT and OFFSET",
        R"(
            SELECT ?person ?name
            WHERE {
                ?person <http://schema.org/name> ?name
            }
            ORDER BY ?name
            LIMIT 10
            OFFSET 20
        )"
    );

    // ========================================================================
    // Example 10: DISTINCT
    // ========================================================================
    PrintParseResult(
        "DISTINCT",
        R"(
            SELECT DISTINCT ?name
            WHERE {
                ?person <http://schema.org/name> ?name
            }
        )"
    );

    // ========================================================================
    // Example 11: Built-in functions
    // ========================================================================
    PrintParseResult(
        "Built-in functions",
        R"(
            SELECT ?person ?name
            WHERE {
                ?person <http://schema.org/name> ?name .
                FILTER (BOUND(?name) && STR(?name) != "")
            }
        )"
    );

    // ========================================================================
    // Example 12: Complex query with everything
    // ========================================================================
    PrintParseResult(
        "Complex query",
        R"(
            SELECT DISTINCT ?person ?name ?age ?phone
            WHERE {
                ?person <http://schema.org/name> ?name .
                ?person <http://schema.org/age> ?age .
                FILTER (?age >= 18 && ?age <= 65)
                OPTIONAL {
                    ?person <http://schema.org/phone> ?phone .
                    FILTER (STR(?phone) >= "555-0000")
                }
            }
            ORDER BY DESC(?age)
            LIMIT 100
        )"
    );

    // ========================================================================
    // Example 13: Execute a parsed query against real data
    // ========================================================================
    std::cout << "\n=== Example 13: Execute parsed query ===" << std::endl;

    // 1. Open database
    marble::DBOptions db_opts;
    db_opts.db_path = "/tmp/sabot_ql_parser_example";
    auto db_result = marble::MarbleDB::Open(db_opts);
    if (!db_result.ok()) {
        std::cerr << "Failed to open database: " << db_result.status().ToString() << std::endl;
        return 1;
    }
    auto db = db_result.value();

    // 2. Create triple store and vocabulary
    auto store_result = TripleStore::Create("/tmp/sabot_ql_parser_example", db);
    if (!store_result.ok()) {
        std::cerr << "Failed to create triple store: " << store_result.status().ToString() << std::endl;
        return 1;
    }
    auto store = store_result.ValueOrDie();

    auto vocab_result = Vocabulary::Create("/tmp/sabot_ql_parser_example", db);
    if (!vocab_result.ok()) {
        std::cerr << "Failed to create vocabulary: " << vocab_result.status().ToString() << std::endl;
        return 1;
    }
    auto vocab = vocab_result.ValueOrDie();

    // 3. Load sample data
    std::vector<Triple> triples;

    auto hasName = vocab->AddTerm(Term::IRI("http://schema.org/name")).ValueOrDie();
    auto hasAge = vocab->AddTerm(Term::IRI("http://schema.org/age")).ValueOrDie();

    auto alice = vocab->AddTerm(Term::IRI("http://example.org/Alice")).ValueOrDie();
    auto alice_name = vocab->AddTerm(Term::Literal("Alice")).ValueOrDie();
    auto alice_age = vocab->AddTerm(Term::Literal("35", "", "http://www.w3.org/2001/XMLSchema#integer")).ValueOrDie();

    auto bob = vocab->AddTerm(Term::IRI("http://example.org/Bob")).ValueOrDie();
    auto bob_name = vocab->AddTerm(Term::Literal("Bob")).ValueOrDie();
    auto bob_age = vocab->AddTerm(Term::Literal("28", "", "http://www.w3.org/2001/XMLSchema#integer")).ValueOrDie();

    auto carol = vocab->AddTerm(Term::IRI("http://example.org/Carol")).ValueOrDie();
    auto carol_name = vocab->AddTerm(Term::Literal("Carol")).ValueOrDie();
    auto carol_age = vocab->AddTerm(Term::Literal("42", "", "http://www.w3.org/2001/XMLSchema#integer")).ValueOrDie();

    triples.push_back({alice, hasName, alice_name});
    triples.push_back({alice, hasAge, alice_age});
    triples.push_back({bob, hasName, bob_name});
    triples.push_back({bob, hasAge, bob_age});
    triples.push_back({carol, hasName, carol_name});
    triples.push_back({carol, hasAge, carol_age});

    auto status = store->InsertTriples(triples);
    if (!status.ok()) {
        std::cerr << "Failed to insert triples: " << status.ToString() << std::endl;
        return 1;
    }

    std::cout << "Loaded " << triples.size() << " triples" << std::endl;

    // 4. Parse and execute a query
    std::string query_text = R"(
        SELECT ?person ?name ?age
        WHERE {
            ?person <http://schema.org/name> ?name .
            ?person <http://schema.org/age> ?age .
            FILTER (?age > 30)
        }
        ORDER BY DESC(?age)
    )";

    std::cout << "\nQuery text:\n" << query_text << std::endl;

    auto parse_result = ParseSPARQL(query_text);
    if (!parse_result.ok()) {
        std::cerr << "Parse error: " << parse_result.status().ToString() << std::endl;
        return 1;
    }

    auto query = parse_result.ValueOrDie();
    std::cout << "\nParsed successfully!" << std::endl;

    // 5. Execute query
    QueryEngine engine(store, vocab);
    auto result = engine.ExecuteSelect(query.select_query);
    if (!result.ok()) {
        std::cerr << "Query execution failed: " << result.status().ToString() << std::endl;
        return 1;
    }

    std::cout << "\nQuery results:\n" << result.ValueOrDie()->ToString() << std::endl;

    // ========================================================================
    // Example 14: Parser error handling
    // ========================================================================
    std::cout << "\n=== Example 14: Parser error handling ===" << std::endl;

    std::vector<std::string> invalid_queries = {
        "SELECT ?x WHERE",  // Missing braces
        "SELECT WHERE { ?s ?p ?o }",  // Missing variable
        "SELECT ?x WHERE { ?x }",  // Incomplete triple pattern
        "SELECT ?x WHERE { ?x ?p ?o } ORDER BY",  // Incomplete ORDER BY
    };

    for (const auto& invalid_query : invalid_queries) {
        std::cout << "\nInvalid query: " << invalid_query << std::endl;
        auto result = ParseSPARQL(invalid_query);
        if (!result.ok()) {
            std::cout << "Expected error: " << result.status().ToString() << std::endl;
        } else {
            std::cout << "❌ Should have failed to parse!" << std::endl;
        }
    }

    std::cout << "\n=== All parser examples completed! ===" << std::endl;

    return 0;
}
