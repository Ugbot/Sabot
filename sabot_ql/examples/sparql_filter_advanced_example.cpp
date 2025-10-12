// Example: Advanced SPARQL FILTER Functions
//
// This demonstrates the new FILTER built-in functions:
// - LANG(?var) - Extract language tag from literals
// - DATATYPE(?var) - Extract datatype IRI from literals
// - REGEX(?text, pattern) - Regular expression matching
//
// These functions complete the SPARQL 1.1 FILTER built-in function set.

#include <sabot_ql/sparql/parser.h>
#include <sabot_ql/sparql/query_engine.h>
#include <sabot_ql/storage/triple_store.h>
#include <sabot_ql/storage/vocabulary.h>
#include <marble/db.h>
#include <iostream>
#include <string>

using namespace sabot_ql;
using namespace sabot_ql::sparql;

void PrintQueryResult(const std::string& query_name,
                     const std::string& query_text,
                     QueryEngine& engine) {
    std::cout << "\n=== " << query_name << " ===" << std::endl;
    std::cout << "Query text:\n" << query_text << std::endl;

    auto parse_result = ParseSPARQL(query_text);
    if (!parse_result.ok()) {
        std::cerr << "Parse error: " << parse_result.status().ToString() << std::endl;
        return;
    }

    auto query = parse_result.ValueOrDie();
    std::cout << "\n✅ Parse successful!" << std::endl;

    auto result = engine.ExecuteSelect(query.select_query);
    if (!result.ok()) {
        std::cerr << "Query execution failed: " << result.status().ToString() << std::endl;
        return;
    }

    std::cout << "\nQuery results:\n" << result.ValueOrDie()->ToString() << std::endl;
}

int main() {
    std::cout << "Advanced SPARQL FILTER Functions Examples" << std::endl;
    std::cout << "==========================================" << std::endl;

    // ========================================================================
    // Setup: Create database and load multilingual data
    // ========================================================================

    marble::DBOptions db_opts;
    db_opts.db_path = "/tmp/sabot_ql_filter_advanced";
    auto db_result = marble::MarbleDB::Open(db_opts);
    if (!db_result.ok()) {
        std::cerr << "Failed to open database: " << db_result.status().ToString() << std::endl;
        return 1;
    }
    auto db = db_result.value();

    auto store_result = TripleStore::Create("/tmp/sabot_ql_filter_advanced", db);
    if (!store_result.ok()) {
        std::cerr << "Failed to create triple store: " << store_result.status().ToString() << std::endl;
        return 1;
    }
    auto store = store_result.ValueOrDie();

    auto vocab_result = Vocabulary::Create("/tmp/sabot_ql_filter_advanced", db);
    if (!vocab_result.ok()) {
        std::cerr << "Failed to create vocabulary: " << vocab_result.status().ToString() << std::endl;
        return 1;
    }
    auto vocab = vocab_result.ValueOrDie();

    // ========================================================================
    // Load multilingual sample data
    // ========================================================================

    std::vector<Triple> triples;

    auto hasName = vocab->AddTerm(Term::IRI("http://schema.org/name")).ValueOrDie();
    auto hasDescription = vocab->AddTerm(Term::IRI("http://schema.org/description")).ValueOrDie();
    auto hasAge = vocab->AddTerm(Term::IRI("http://schema.org/age")).ValueOrDie();
    auto hasPrice = vocab->AddTerm(Term::IRI("http://schema.org/price")).ValueOrDie();
    auto hasEmail = vocab->AddTerm(Term::IRI("http://schema.org/email")).ValueOrDie();

    // People with multilingual names
    auto alice = vocab->AddTerm(Term::IRI("http://example.org/Alice")).ValueOrDie();
    auto alice_name_en = vocab->AddTerm(Term::Literal("Alice", "en", "")).ValueOrDie();
    auto alice_name_fr = vocab->AddTerm(Term::Literal("Alice", "fr", "")).ValueOrDie();
    auto alice_desc_en = vocab->AddTerm(Term::Literal("Software engineer", "en", "")).ValueOrDie();
    auto alice_age = vocab->AddTerm(Term::Literal("35", "", "http://www.w3.org/2001/XMLSchema#integer")).ValueOrDie();
    auto alice_email = vocab->AddTerm(Term::Literal("alice@example.com")).ValueOrDie();

    auto bob = vocab->AddTerm(Term::IRI("http://example.org/Bob")).ValueOrDie();
    auto bob_name_en = vocab->AddTerm(Term::Literal("Bob", "en", "")).ValueOrDie();
    auto bob_name_es = vocab->AddTerm(Term::Literal("Roberto", "es", "")).ValueOrDie();
    auto bob_desc_es = vocab->AddTerm(Term::Literal("Ingeniero de datos", "es", "")).ValueOrDie();
    auto bob_age = vocab->AddTerm(Term::Literal("28", "", "http://www.w3.org/2001/XMLSchema#integer")).ValueOrDie();
    auto bob_email = vocab->AddTerm(Term::Literal("bob.smith@company.com")).ValueOrDie();

    auto carol = vocab->AddTerm(Term::IRI("http://example.org/Carol")).ValueOrDie();
    auto carol_name_de = vocab->AddTerm(Term::Literal("Karola", "de", "")).ValueOrDie();
    auto carol_desc_de = vocab->AddTerm(Term::Literal("Projektmanagerin", "de", "")).ValueOrDie();
    auto carol_age = vocab->AddTerm(Term::Literal("42", "", "http://www.w3.org/2001/XMLSchema#integer")).ValueOrDie();
    auto carol_email = vocab->AddTerm(Term::Literal("carol_johnson@example.org")).ValueOrDie();

    // Products with typed prices
    auto product1 = vocab->AddTerm(Term::IRI("http://example.org/Product1")).ValueOrDie();
    auto product1_name = vocab->AddTerm(Term::Literal("Laptop")).ValueOrDie();
    auto product1_price = vocab->AddTerm(Term::Literal("999.99", "", "http://www.w3.org/2001/XMLSchema#decimal")).ValueOrDie();

    auto product2 = vocab->AddTerm(Term::IRI("http://example.org/Product2")).ValueOrDie();
    auto product2_name = vocab->AddTerm(Term::Literal("Mouse")).ValueOrDie();
    auto product2_price = vocab->AddTerm(Term::Literal("29.99", "", "http://www.w3.org/2001/XMLSchema#decimal")).ValueOrDie();

    // Build triples
    triples.push_back({alice, hasName, alice_name_en});
    triples.push_back({alice, hasName, alice_name_fr});
    triples.push_back({alice, hasDescription, alice_desc_en});
    triples.push_back({alice, hasAge, alice_age});
    triples.push_back({alice, hasEmail, alice_email});

    triples.push_back({bob, hasName, bob_name_en});
    triples.push_back({bob, hasName, bob_name_es});
    triples.push_back({bob, hasDescription, bob_desc_es});
    triples.push_back({bob, hasAge, bob_age});
    triples.push_back({bob, hasEmail, bob_email});

    triples.push_back({carol, hasName, carol_name_de});
    triples.push_back({carol, hasDescription, carol_desc_de});
    triples.push_back({carol, hasAge, carol_age});
    triples.push_back({carol, hasEmail, carol_email});

    triples.push_back({product1, hasName, product1_name});
    triples.push_back({product1, hasPrice, product1_price});

    triples.push_back({product2, hasName, product2_name});
    triples.push_back({product2, hasPrice, product2_price});

    auto status = store->InsertTriples(triples);
    if (!status.ok()) {
        std::cerr << "Failed to insert triples: " << status.ToString() << std::endl;
        return 1;
    }

    std::cout << "\nLoaded " << triples.size() << " triples" << std::endl;

    // Create query engine
    QueryEngine engine(store, vocab);

    // ========================================================================
    // Example 1: LANG(?var) - Filter by language tag
    // ========================================================================
    PrintQueryResult(
        "Example 1: Filter English names using LANG",
        R"(
            PREFIX schema: <http://schema.org/>

            SELECT ?person ?name
            WHERE {
                ?person schema:name ?name .
                FILTER (LANG(?name) = "en")
            }
        )",
        engine
    );

    // ========================================================================
    // Example 2: LANG(?var) - Filter Spanish descriptions
    // ========================================================================
    PrintQueryResult(
        "Example 2: Filter Spanish descriptions using LANG",
        R"(
            PREFIX schema: <http://schema.org/>

            SELECT ?person ?desc
            WHERE {
                ?person schema:description ?desc .
                FILTER (LANG(?desc) = "es")
            }
        )",
        engine
    );

    // ========================================================================
    // Example 3: LANG(?var) with BOUND - Handle optional translations
    // ========================================================================
    PrintQueryResult(
        "Example 3: German names or fallback to any language",
        R"(
            PREFIX schema: <http://schema.org/>

            SELECT ?person ?name
            WHERE {
                ?person schema:name ?name .
                FILTER (LANG(?name) = "de" || LANG(?name) = "en")
            }
        )",
        engine
    );

    // ========================================================================
    // Example 4: DATATYPE(?var) - Filter integer ages
    // ========================================================================
    PrintQueryResult(
        "Example 4: Filter integer ages using DATATYPE",
        R"(
            PREFIX schema: <http://schema.org/>
            PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>

            SELECT ?person ?age
            WHERE {
                ?person schema:age ?age .
                FILTER (DATATYPE(?age) = xsd:integer)
            }
        )",
        engine
    );

    // ========================================================================
    // Example 5: DATATYPE(?var) - Filter decimal prices
    // ========================================================================
    PrintQueryResult(
        "Example 5: Filter decimal prices using DATATYPE",
        R"(
            PREFIX schema: <http://schema.org/>
            PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>

            SELECT ?product ?price
            WHERE {
                ?product schema:price ?price .
                FILTER (DATATYPE(?price) = xsd:decimal)
            }
        )",
        engine
    );

    // ========================================================================
    // Example 6: REGEX(?text, pattern) - Email pattern matching
    // ========================================================================
    PrintQueryResult(
        "Example 6: Filter emails from example.com using REGEX",
        R"(
            PREFIX schema: <http://schema.org/>

            SELECT ?person ?email
            WHERE {
                ?person schema:email ?email .
                FILTER (REGEX(?email, "@example\\.com$"))
            }
        )",
        engine
    );

    // ========================================================================
    // Example 7: REGEX(?text, pattern) - Name pattern matching
    // ========================================================================
    PrintQueryResult(
        "Example 7: Filter names starting with 'A' using REGEX",
        R"(
            PREFIX schema: <http://schema.org/>

            SELECT ?person ?name
            WHERE {
                ?person schema:name ?name .
                FILTER (REGEX(?name, "^A"))
            }
        )",
        engine
    );

    // ========================================================================
    // Example 8: REGEX(?text, pattern) - Complex email validation
    // ========================================================================
    PrintQueryResult(
        "Example 8: Complex email pattern (underscore or domain)",
        R"(
            PREFIX schema: <http://schema.org/>

            SELECT ?person ?email
            WHERE {
                ?person schema:email ?email .
                FILTER (REGEX(?email, "_|@company\\.com$"))
            }
        )",
        engine
    );

    // ========================================================================
    // Example 9: Combined LANG and REGEX
    // ========================================================================
    PrintQueryResult(
        "Example 9: English names containing 'o' using LANG + REGEX",
        R"(
            PREFIX schema: <http://schema.org/>

            SELECT ?person ?name
            WHERE {
                ?person schema:name ?name .
                FILTER (LANG(?name) = "en" && REGEX(?name, "o"))
            }
        )",
        engine
    );

    // ========================================================================
    // Example 10: Combined DATATYPE and comparison
    // ========================================================================
    PrintQueryResult(
        "Example 10: Integer ages over 30 using DATATYPE + comparison",
        R"(
            PREFIX schema: <http://schema.org/>
            PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>

            SELECT ?person ?age
            WHERE {
                ?person schema:age ?age .
                FILTER (DATATYPE(?age) = xsd:integer && ?age > 30)
            }
        )",
        engine
    );

    // ========================================================================
    // Example 11: All three functions combined
    // ========================================================================
    PrintQueryResult(
        "Example 11: Complex query with LANG, DATATYPE, and REGEX",
        R"(
            PREFIX schema: <http://schema.org/>
            PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>

            SELECT ?person ?name ?age ?email
            WHERE {
                ?person schema:name ?name .
                ?person schema:age ?age .
                ?person schema:email ?email .
                FILTER (
                    LANG(?name) = "en" &&
                    DATATYPE(?age) = xsd:integer &&
                    REGEX(?email, "@example\\.com$")
                )
            }
        )",
        engine
    );

    std::cout << "\n=== All advanced FILTER examples completed! ===" << std::endl;
    std::cout << "\n📊 Summary of new built-in functions:" << std::endl;
    std::cout << "  • LANG(?var) - Extract language tag from literals" << std::endl;
    std::cout << "  • DATATYPE(?var) - Extract datatype IRI from literals" << std::endl;
    std::cout << "  • REGEX(?text, pattern) - Regular expression matching" << std::endl;
    std::cout << "\n✅ SabotQL now supports complete SPARQL 1.1 FILTER built-ins!" << std::endl;

    return 0;
}
