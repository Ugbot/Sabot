// Example: SPARQL OPTIONAL clause
//
// This demonstrates how to use OPTIONAL for left outer joins:
// - Simple OPTIONAL (optional attributes)
// - OPTIONAL with FILTER
// - Multiple OPTIONAL clauses
// - OPTIONAL with no join variables (cross product)
//
// OPTIONAL keeps all results from the main pattern,
// filling in NULL/UNDEF values when the optional pattern doesn't match

#include <sabot_ql/storage/triple_store.h>
#include <sabot_ql/storage/vocabulary.h>
#include <sabot_ql/sparql/query_engine.h>
#include <marble/db.h>
#include <iostream>

using namespace sabot_ql;
using namespace sabot_ql::sparql;

int main() {
    // 1. Open/create database
    marble::DBOptions db_opts;
    db_opts.db_path = "/tmp/sabot_ql_optional_example";
    auto db_result = marble::MarbleDB::Open(db_opts);
    if (!db_result.ok()) {
        std::cerr << "Failed to open database: " << db_result.status().ToString() << std::endl;
        return 1;
    }
    auto db = db_result.value();

    // 2. Create triple store and vocabulary
    auto store_result = TripleStore::Create("/tmp/sabot_ql_optional_example", db);
    if (!store_result.ok()) {
        std::cerr << "Failed to create triple store: " << store_result.status().ToString() << std::endl;
        return 1;
    }
    auto store = store_result.ValueOrDie();

    auto vocab_result = Vocabulary::Create("/tmp/sabot_ql_optional_example", db);
    if (!vocab_result.ok()) {
        std::cerr << "Failed to create vocabulary: " << vocab_result.status().ToString() << std::endl;
        return 1;
    }
    auto vocab = vocab_result.ValueOrDie();

    // 3. Load sample data (people with optional phone numbers and addresses)
    std::cout << "Loading sample data (people with optional contact info)..." << std::endl;

    std::vector<Triple> triples;

    // Predicates
    auto hasName = vocab->AddTerm(Term::IRI("http://schema.org/name")).ValueOrDie();
    auto hasEmail = vocab->AddTerm(Term::IRI("http://schema.org/email")).ValueOrDie();
    auto hasPhone = vocab->AddTerm(Term::IRI("http://schema.org/phone")).ValueOrDie();
    auto hasAddress = vocab->AddTerm(Term::IRI("http://schema.org/address")).ValueOrDie();
    auto hasAge = vocab->AddTerm(Term::IRI("http://schema.org/age")).ValueOrDie();

    // Person 1: Alice (has name, email, phone, address)
    auto alice = vocab->AddTerm(Term::IRI("http://example.org/Alice")).ValueOrDie();
    auto alice_name = vocab->AddTerm(Term::Literal("Alice")).ValueOrDie();
    auto alice_email = vocab->AddTerm(Term::Literal("alice@example.com")).ValueOrDie();
    auto alice_phone = vocab->AddTerm(Term::Literal("555-1234")).ValueOrDie();
    auto alice_address = vocab->AddTerm(Term::Literal("123 Main St")).ValueOrDie();
    auto alice_age = vocab->AddTerm(Term::Literal("35", "", "http://www.w3.org/2001/XMLSchema#integer")).ValueOrDie();

    triples.push_back({alice, hasName, alice_name});
    triples.push_back({alice, hasEmail, alice_email});
    triples.push_back({alice, hasPhone, alice_phone});
    triples.push_back({alice, hasAddress, alice_address});
    triples.push_back({alice, hasAge, alice_age});

    // Person 2: Bob (has name, email, phone, no address)
    auto bob = vocab->AddTerm(Term::IRI("http://example.org/Bob")).ValueOrDie();
    auto bob_name = vocab->AddTerm(Term::Literal("Bob")).ValueOrDie();
    auto bob_email = vocab->AddTerm(Term::Literal("bob@example.com")).ValueOrDie();
    auto bob_phone = vocab->AddTerm(Term::Literal("555-5678")).ValueOrDie();
    auto bob_age = vocab->AddTerm(Term::Literal("28", "", "http://www.w3.org/2001/XMLSchema#integer")).ValueOrDie();

    triples.push_back({bob, hasName, bob_name});
    triples.push_back({bob, hasEmail, bob_email});
    triples.push_back({bob, hasPhone, bob_phone});
    triples.push_back({bob, hasAge, bob_age});

    // Person 3: Carol (has name, email, no phone, no address)
    auto carol = vocab->AddTerm(Term::IRI("http://example.org/Carol")).ValueOrDie();
    auto carol_name = vocab->AddTerm(Term::Literal("Carol")).ValueOrDie();
    auto carol_email = vocab->AddTerm(Term::Literal("carol@example.com")).ValueOrDie();
    auto carol_age = vocab->AddTerm(Term::Literal("42", "", "http://www.w3.org/2001/XMLSchema#integer")).ValueOrDie();

    triples.push_back({carol, hasName, carol_name});
    triples.push_back({carol, hasEmail, carol_email});
    triples.push_back({carol, hasAge, carol_age});

    // Person 4: Dave (has name, email only - minimal info)
    auto dave = vocab->AddTerm(Term::IRI("http://example.org/Dave")).ValueOrDie();
    auto dave_name = vocab->AddTerm(Term::Literal("Dave")).ValueOrDie();
    auto dave_email = vocab->AddTerm(Term::Literal("dave@example.com")).ValueOrDie();

    triples.push_back({dave, hasName, dave_name});
    triples.push_back({dave, hasEmail, dave_email});

    // Insert triples
    auto status = store->InsertTriples(triples);
    if (!status.ok()) {
        std::cerr << "Failed to insert triples: " << status.ToString() << std::endl;
        return 1;
    }

    std::cout << "Loaded " << triples.size() << " triples" << std::endl;

    // 4. Create SPARQL query engine
    QueryEngine engine(store, vocab);

    // ========================================================================
    // Example 1: Simple OPTIONAL (phone number)
    // ========================================================================
    std::cout << "\n=== Example 1: Simple OPTIONAL (people with optional phone) ===" << std::endl;

    // SPARQL equivalent:
    // SELECT ?person ?name ?phone WHERE {
    //   ?person <hasName> ?name .
    //   OPTIONAL { ?person <hasPhone> ?phone }
    // }

    SPARQLBuilder builder1;

    // Main pattern (required)
    BasicGraphPattern bgp1;
    bgp1.triples = {
        TriplePattern(Var("person"), Iri("http://schema.org/name"), Var("name"))
    };

    // Optional pattern
    auto optional1 = std::make_shared<QueryPattern>();
    BasicGraphPattern optional_bgp1;
    optional_bgp1.triples = {
        TriplePattern(Var("person"), Iri("http://schema.org/phone"), Var("phone"))
    };
    optional1->bgp = optional_bgp1;

    // Build query
    SelectQuery query1;
    query1.select.variables = {Variable("person"), Variable("name"), Variable("phone")};
    query1.where.bgp = bgp1;
    query1.where.optionals = {OptionalPattern(optional1)};

    std::cout << "Query: SELECT ?person ?name ?phone WHERE { required { optional } }" << std::endl;

    auto result1 = engine.ExecuteSelect(query1);
    if (!result1.ok()) {
        std::cerr << "Query 1 failed: " << result1.status().ToString() << std::endl;
    } else {
        std::cout << "\nResults (all people, phone if available):\n";
        std::cout << result1.ValueOrDie()->ToString() << std::endl;
        std::cout << "Note: Dave has NULL for phone (no phone triple exists)\n";
    }

    // ========================================================================
    // Example 2: Multiple OPTIONAL clauses
    // ========================================================================
    std::cout << "\n=== Example 2: Multiple OPTIONAL (phone AND address) ===" << std::endl;

    // SPARQL equivalent:
    // SELECT ?person ?name ?phone ?address WHERE {
    //   ?person <hasName> ?name .
    //   OPTIONAL { ?person <hasPhone> ?phone }
    //   OPTIONAL { ?person <hasAddress> ?address }
    // }

    // Main pattern
    BasicGraphPattern bgp2;
    bgp2.triples = {
        TriplePattern(Var("person"), Iri("http://schema.org/name"), Var("name"))
    };

    // Optional pattern 1 (phone)
    auto optional2a = std::make_shared<QueryPattern>();
    BasicGraphPattern optional_bgp2a;
    optional_bgp2a.triples = {
        TriplePattern(Var("person"), Iri("http://schema.org/phone"), Var("phone"))
    };
    optional2a->bgp = optional_bgp2a;

    // Optional pattern 2 (address)
    auto optional2b = std::make_shared<QueryPattern>();
    BasicGraphPattern optional_bgp2b;
    optional_bgp2b.triples = {
        TriplePattern(Var("person"), Iri("http://schema.org/address"), Var("address"))
    };
    optional2b->bgp = optional_bgp2b;

    // Build query
    SelectQuery query2;
    query2.select.variables = {Variable("person"), Variable("name"), Variable("phone"), Variable("address")};
    query2.where.bgp = bgp2;
    query2.where.optionals = {OptionalPattern(optional2a), OptionalPattern(optional2b)};

    std::cout << "Query: SELECT ?person ?name ?phone ?address WHERE { required { opt1 } { opt2 } }" << std::endl;

    auto result2 = engine.ExecuteSelect(query2);
    if (!result2.ok()) {
        std::cerr << "Query 2 failed: " << result2.status().ToString() << std::endl;
    } else {
        std::cout << "\nResults (all people with optional phone and address):\n";
        std::cout << result2.ValueOrDie()->ToString() << std::endl;
        std::cout << "Note: Alice has both, Bob has phone only, Carol/Dave have neither\n";
    }

    // ========================================================================
    // Example 3: OPTIONAL with FILTER
    // ========================================================================
    std::cout << "\n=== Example 3: OPTIONAL with FILTER (only get phones starting with 555-1) ===" << std::endl;

    // SPARQL equivalent:
    // SELECT ?person ?name ?phone WHERE {
    //   ?person <hasName> ?name .
    //   OPTIONAL {
    //     ?person <hasPhone> ?phone .
    //     FILTER (STR(?phone) >= "555-1")
    //   }
    // }

    // Main pattern
    BasicGraphPattern bgp3;
    bgp3.triples = {
        TriplePattern(Var("person"), Iri("http://schema.org/name"), Var("name"))
    };

    // Optional pattern with filter
    auto optional3 = std::make_shared<QueryPattern>();
    BasicGraphPattern optional_bgp3;
    optional_bgp3.triples = {
        TriplePattern(Var("person"), Iri("http://schema.org/phone"), Var("phone"))
    };
    optional3->bgp = optional_bgp3;
    optional3->filters = {
        FilterClause(expr::GreaterThanEqual(
            expr::Var("phone"),
            expr::Lit("555-1")
        ))
    };

    // Build query
    SelectQuery query3;
    query3.select.variables = {Variable("person"), Variable("name"), Variable("phone")};
    query3.where.bgp = bgp3;
    query3.where.optionals = {OptionalPattern(optional3)};

    std::cout << "Query: SELECT ?person ?name ?phone WHERE { required OPTIONAL { filter } }" << std::endl;

    auto result3 = engine.ExecuteSelect(query3);
    if (!result3.ok()) {
        std::cerr << "Query 3 failed: " << result3.status().ToString() << std::endl;
    } else {
        std::cout << "\nResults (all people, phone if >= '555-1'):\n";
        std::cout << result3.ValueOrDie()->ToString() << std::endl;
        std::cout << "Note: Only Alice's phone matches the filter\n";
    }

    // ========================================================================
    // Example 4: OPTIONAL with age filter (people with optional age > 30)
    // ========================================================================
    std::cout << "\n=== Example 4: OPTIONAL with age filter (age > 30) ===" << std::endl;

    // Main pattern
    BasicGraphPattern bgp4;
    bgp4.triples = {
        TriplePattern(Var("person"), Iri("http://schema.org/name"), Var("name"))
    };

    // Optional pattern with age filter
    auto optional4 = std::make_shared<QueryPattern>();
    BasicGraphPattern optional_bgp4;
    optional_bgp4.triples = {
        TriplePattern(Var("person"), Iri("http://schema.org/age"), Var("age"))
    };
    optional4->bgp = optional_bgp4;
    optional4->filters = {
        FilterClause(expr::GreaterThan(
            expr::Var("age"),
            expr::Lit("30")
        ))
    };

    // Build query
    SelectQuery query4;
    query4.select.variables = {Variable("person"), Variable("name"), Variable("age")};
    query4.where.bgp = bgp4;
    query4.where.optionals = {OptionalPattern(optional4)};

    std::cout << "Query: SELECT ?person ?name ?age WHERE { required OPTIONAL { age > 30 } }" << std::endl;

    auto result4 = engine.ExecuteSelect(query4);
    if (!result4.ok()) {
        std::cerr << "Query 4 failed: " << result4.status().ToString() << std::endl;
    } else {
        std::cout << "\nResults (all people, age if > 30):\n";
        std::cout << result4.ValueOrDie()->ToString() << std::endl;
        std::cout << "Note: Only Alice (35) and Carol (42) have age > 30\n";
    }

    // ========================================================================
    // Example 5: EXPLAIN query plan with OPTIONAL
    // ========================================================================
    std::cout << "\n=== Example 5: EXPLAIN OPTIONAL query ===" << std::endl;

    std::cout << engine.Explain(query1) << std::endl;

    // ========================================================================
    // Example 6: EXPLAIN ANALYZE (with statistics)
    // ========================================================================
    std::cout << "\n=== Example 6: EXPLAIN ANALYZE ===" << std::endl;

    auto analyze_result = engine.ExplainAnalyze(query1);
    if (analyze_result.ok()) {
        std::cout << analyze_result.ValueOrDie() << std::endl;
    }

    std::cout << "\n=== All OPTIONAL examples completed! ===" << std::endl;

    return 0;
}
