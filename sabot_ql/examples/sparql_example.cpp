// Example: Using SabotQL's SPARQL query engine
//
// This demonstrates how to:
// 1. Load RDF data
// 2. Build SPARQL queries programmatically using SPARQLBuilder
// 3. Execute queries and get results
// 4. Use EXPLAIN and EXPLAIN ANALYZE

#include <sabot_ql/storage/triple_store.h>
#include <sabot_ql/storage/vocabulary.h>
#include <sabot_ql/parser/rdf_parser.h>
#include <sabot_ql/sparql/query_engine.h>
#include <marble/db.h>
#include <iostream>

using namespace sabot_ql;
using namespace sabot_ql::sparql;

int main() {
    // 1. Open/create database
    marble::DBOptions db_opts;
    db_opts.db_path = "/tmp/sabot_ql_sparql_example";
    auto db_result = marble::MarbleDB::Open(db_opts);
    if (!db_result.ok()) {
        std::cerr << "Failed to open database: " << db_result.status().ToString() << std::endl;
        return 1;
    }
    auto db = db_result.value();

    // 2. Create triple store and vocabulary
    auto store_result = TripleStore::Create("/tmp/sabot_ql_sparql_example", db);
    if (!store_result.ok()) {
        std::cerr << "Failed to create triple store: " << store_result.status().ToString() << std::endl;
        return 1;
    }
    auto store = store_result.ValueOrDie();

    auto vocab_result = Vocabulary::Create("/tmp/sabot_ql_sparql_example", db);
    if (!vocab_result.ok()) {
        std::cerr << "Failed to create vocabulary: " << vocab_result.status().ToString() << std::endl;
        return 1;
    }
    auto vocab = vocab_result.ValueOrDie();

    // 3. Load sample RDF data programmatically
    std::cout << "Loading sample RDF data..." << std::endl;

    // Add some sample triples: (Alice, hasName, "Alice"), (Alice, hasAge, 35), etc.
    std::vector<Triple> triples;

    // Alice
    auto alice_iri = vocab->AddTerm(Term::IRI("http://example.org/Alice")).ValueOrDie();
    auto hasName = vocab->AddTerm(Term::IRI("http://schema.org/name")).ValueOrDie();
    auto hasAge = vocab->AddTerm(Term::IRI("http://schema.org/age")).ValueOrDie();
    auto livesIn = vocab->AddTerm(Term::IRI("http://schema.org/livesIn")).ValueOrDie();

    auto alice_name = vocab->AddTerm(Term::Literal("Alice")).ValueOrDie();
    auto alice_age = vocab->AddTerm(Term::Literal("35", "", "http://www.w3.org/2001/XMLSchema#integer")).ValueOrDie();
    auto sf_city = vocab->AddTerm(Term::IRI("http://example.org/SanFrancisco")).ValueOrDie();

    triples.push_back({alice_iri, hasName, alice_name});
    triples.push_back({alice_iri, hasAge, alice_age});
    triples.push_back({alice_iri, livesIn, sf_city});

    // Bob
    auto bob_iri = vocab->AddTerm(Term::IRI("http://example.org/Bob")).ValueOrDie();
    auto bob_name = vocab->AddTerm(Term::Literal("Bob")).ValueOrDie();
    auto bob_age = vocab->AddTerm(Term::Literal("28", "", "http://www.w3.org/2001/XMLSchema#integer")).ValueOrDie();
    auto ny_city = vocab->AddTerm(Term::IRI("http://example.org/NewYork")).ValueOrDie();

    triples.push_back({bob_iri, hasName, bob_name});
    triples.push_back({bob_iri, hasAge, bob_age});
    triples.push_back({bob_iri, livesIn, ny_city});

    // Carol
    auto carol_iri = vocab->AddTerm(Term::IRI("http://example.org/Carol")).ValueOrDie();
    auto carol_name = vocab->AddTerm(Term::Literal("Carol")).ValueOrDie();
    auto carol_age = vocab->AddTerm(Term::Literal("42", "", "http://www.w3.org/2001/XMLSchema#integer")).ValueOrDie();

    triples.push_back({carol_iri, hasName, carol_name});
    triples.push_back({carol_iri, hasAge, carol_age});
    triples.push_back({carol_iri, livesIn, sf_city});

    // Insert triples
    auto status = store->InsertTriples(triples);
    if (!status.ok()) {
        std::cerr << "Failed to insert triples: " << status.ToString() << std::endl;
        return 1;
    }

    std::cout << "Loaded " << triples.size() << " triples" << std::endl;
    std::cout << "Total triples in store: " << store->TotalTriples() << std::endl;

    // 4. Create SPARQL query engine
    QueryEngine engine(store, vocab);

    // 5. Example Query 1: Simple SELECT with triple patterns
    // SPARQL equivalent:
    //   SELECT ?person ?name WHERE {
    //       ?person <http://schema.org/name> ?name .
    //   }
    //   LIMIT 10

    std::cout << "\n=== Query 1: Find all people with names ===" << std::endl;

    SPARQLBuilder builder1;
    auto query1 = builder1
        .Select({"person", "name"})
        .Where()
            .Triple(
                Var("person"),
                Iri("http://schema.org/name"),
                Var("name")
            )
        .EndWhere()
        .Limit(10)
        .Build();

    std::cout << "Query:\n" << query1.ToString() << std::endl;

    auto result1 = engine.ExecuteSelect(query1);
    if (!result1.ok()) {
        std::cerr << "Query 1 failed: " << result1.status().ToString() << std::endl;
    } else {
        std::cout << "Results: " << result1.ValueOrDie()->num_rows() << " rows" << std::endl;
        std::cout << result1.ValueOrDie()->ToString() << std::endl;
    }

    // 6. Example Query 2: JOIN two triple patterns
    // SPARQL equivalent:
    //   SELECT ?person ?name ?city WHERE {
    //       ?person <http://schema.org/name> ?name .
    //       ?person <http://schema.org/livesIn> ?city .
    //   }

    std::cout << "\n=== Query 2: Find people and their cities ===" << std::endl;

    SPARQLBuilder builder2;
    auto query2 = builder2
        .Select({"person", "name", "city"})
        .Where()
            .Triple(Var("person"), Iri("http://schema.org/name"), Var("name"))
            .Triple(Var("person"), Iri("http://schema.org/livesIn"), Var("city"))
        .EndWhere()
        .Build();

    std::cout << "Query:\n" << query2.ToString() << std::endl;

    auto result2 = engine.ExecuteSelect(query2);
    if (!result2.ok()) {
        std::cerr << "Query 2 failed: " << result2.status().ToString() << std::endl;
    } else {
        std::cout << "Results: " << result2.ValueOrDie()->num_rows() << " rows" << std::endl;
        std::cout << result2.ValueOrDie()->ToString() << std::endl;
    }

    // 7. Example Query 3: SELECT *
    // SPARQL equivalent:
    //   SELECT * WHERE {
    //       ?person <http://schema.org/name> ?name .
    //   }
    //   LIMIT 5

    std::cout << "\n=== Query 3: SELECT * (all variables) ===" << std::endl;

    SPARQLBuilder builder3;
    auto query3 = builder3
        .SelectAll()
        .Where()
            .Triple(Var("person"), Iri("http://schema.org/name"), Var("name"))
        .EndWhere()
        .Limit(5)
        .Build();

    std::cout << "Query:\n" << query3.ToString() << std::endl;

    auto result3 = engine.ExecuteSelect(query3);
    if (!result3.ok()) {
        std::cerr << "Query 3 failed: " << result3.status().ToString() << std::endl;
    } else {
        std::cout << "Results: " << result3.ValueOrDie()->num_rows() << " rows" << std::endl;
        std::cout << result3.ValueOrDie()->ToString() << std::endl;
    }

    // 8. Example Query 4: DISTINCT
    // SPARQL equivalent:
    //   SELECT DISTINCT ?city WHERE {
    //       ?person <http://schema.org/livesIn> ?city .
    //   }

    std::cout << "\n=== Query 4: DISTINCT cities ===" << std::endl;

    SPARQLBuilder builder4;
    auto query4 = builder4
        .SelectDistinct({"city"})
        .Where()
            .Triple(Var("person"), Iri("http://schema.org/livesIn"), Var("city"))
        .EndWhere()
        .Build();

    std::cout << "Query:\n" << query4.ToString() << std::endl;

    auto result4 = engine.ExecuteSelect(query4);
    if (!result4.ok()) {
        std::cerr << "Query 4 failed: " << result4.status().ToString() << std::endl;
    } else {
        std::cout << "Results: " << result4.ValueOrDie()->num_rows() << " rows" << std::endl;
        std::cout << result4.ValueOrDie()->ToString() << std::endl;
    }

    // 9. EXPLAIN query plan
    std::cout << "\n=== EXPLAIN Query 2 ===" << std::endl;

    std::cout << engine.Explain(query2) << std::endl;

    // 10. EXPLAIN ANALYZE (execute and show statistics)
    std::cout << "\n=== EXPLAIN ANALYZE Query 2 ===" << std::endl;

    auto analyze_result = engine.ExplainAnalyze(query2);
    if (analyze_result.ok()) {
        std::cout << analyze_result.ValueOrDie() << std::endl;
    }

    // 11. Example with FILTER (will be implemented in next phase)
    std::cout << "\n=== Query 5: With FILTER (not yet implemented) ===" << std::endl;

    SPARQLBuilder builder5;
    auto query5 = builder5
        .Select({"person", "name", "age"})
        .Where()
            .Triple(Var("person"), Iri("http://schema.org/name"), Var("name"))
            .Triple(Var("person"), Iri("http://schema.org/age"), Var("age"))
            // Filter expressions will be added in next phase
            // .Filter(expr::GreaterThan(expr::Var("age"), expr::Lit("30")))
        .EndWhere()
        .Build();

    std::cout << "Query:\n" << query5.ToString() << std::endl;

    std::cout << "\n=== All queries completed! ===" << std::endl;

    return 0;
}
