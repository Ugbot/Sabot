// Example: SPARQL FILTER clauses with expression evaluation
//
// This demonstrates how to use FILTER clauses with:
// - Comparison operators (=, !=, <, >, <=, >=)
// - Logical operators (&&, ||, !)
// - Arithmetic operators (+, -, *, /)
// - Built-in functions (BOUND, isIRI, isLiteral, STR)

#include <sabot_ql/storage/triple_store.h>
#include <sabot_ql/storage/vocabulary.h>
#include <sabot_ql/sparql/query_engine.h>
#include <marble/db.h>
#include <iostream>

using namespace sabot_ql;
using namespace sabot_ql::sparql;
using namespace sabot_ql::sparql::expr;

int main() {
    // 1. Open/create database
    marble::DBOptions db_opts;
    db_opts.db_path = "/tmp/sabot_ql_filter_example";
    auto db_result = marble::MarbleDB::Open(db_opts);
    if (!db_result.ok()) {
        std::cerr << "Failed to open database: " << db_result.status().ToString() << std::endl;
        return 1;
    }
    auto db = db_result.value();

    // 2. Create triple store and vocabulary
    auto store_result = TripleStore::Create("/tmp/sabot_ql_filter_example", db);
    if (!store_result.ok()) {
        std::cerr << "Failed to create triple store: " << store_result.status().ToString() << std::endl;
        return 1;
    }
    auto store = store_result.ValueOrDie();

    auto vocab_result = Vocabulary::Create("/tmp/sabot_ql_filter_example", db);
    if (!vocab_result.ok()) {
        std::cerr << "Failed to create vocabulary: " << vocab_result.status().ToString() << std::endl;
        return 1;
    }
    auto vocab = vocab_result.ValueOrDie();

    // 3. Load sample data
    std::cout << "Loading sample data..." << std::endl;

    std::vector<Triple> triples;

    // Add people with ages
    auto hasAge = vocab->AddTerm(Term::IRI("http://schema.org/age")).ValueOrDie();
    auto hasName = vocab->AddTerm(Term::IRI("http://schema.org/name")).ValueOrDie();
    auto hasSalary = vocab->AddTerm(Term::IRI("http://schema.org/salary")).ValueOrDie();

    // Alice, age 35, salary 75000
    auto alice = vocab->AddTerm(Term::IRI("http://example.org/Alice")).ValueOrDie();
    auto alice_name = vocab->AddTerm(Term::Literal("Alice")).ValueOrDie();
    auto alice_age = vocab->AddTerm(Term::Literal("35", "", "http://www.w3.org/2001/XMLSchema#integer")).ValueOrDie();
    auto alice_salary = vocab->AddTerm(Term::Literal("75000", "", "http://www.w3.org/2001/XMLSchema#integer")).ValueOrDie();

    triples.push_back({alice, hasName, alice_name});
    triples.push_back({alice, hasAge, alice_age});
    triples.push_back({alice, hasSalary, alice_salary});

    // Bob, age 28, salary 60000
    auto bob = vocab->AddTerm(Term::IRI("http://example.org/Bob")).ValueOrDie();
    auto bob_name = vocab->AddTerm(Term::Literal("Bob")).ValueOrDie();
    auto bob_age = vocab->AddTerm(Term::Literal("28", "", "http://www.w3.org/2001/XMLSchema#integer")).ValueOrDie();
    auto bob_salary = vocab->AddTerm(Term::Literal("60000", "", "http://www.w3.org/2001/XMLSchema#integer")).ValueOrDie();

    triples.push_back({bob, hasName, bob_name});
    triples.push_back({bob, hasAge, bob_age});
    triples.push_back({bob, hasSalary, bob_salary});

    // Carol, age 42, salary 90000
    auto carol = vocab->AddTerm(Term::IRI("http://example.org/Carol")).ValueOrDie();
    auto carol_name = vocab->AddTerm(Term::Literal("Carol")).ValueOrDie();
    auto carol_age = vocab->AddTerm(Term::Literal("42", "", "http://www.w3.org/2001/XMLSchema#integer")).ValueOrDie();
    auto carol_salary = vocab->AddTerm(Term::Literal("90000", "", "http://www.w3.org/2001/XMLSchema#integer")).ValueOrDie();

    triples.push_back({carol, hasName, carol_name});
    triples.push_back({carol, hasAge, carol_age});
    triples.push_back({carol, hasSalary, carol_salary});

    // Dave, age 25, salary 55000
    auto dave = vocab->AddTerm(Term::IRI("http://example.org/Dave")).ValueOrDie();
    auto dave_name = vocab->AddTerm(Term::Literal("Dave")).ValueOrDie();
    auto dave_age = vocab->AddTerm(Term::Literal("25", "", "http://www.w3.org/2001/XMLSchema#integer")).ValueOrDie();
    auto dave_salary = vocab->AddTerm(Term::Literal("55000", "", "http://www.w3.org/2001/XMLSchema#integer")).ValueOrDie();

    triples.push_back({dave, hasName, dave_name});
    triples.push_back({dave, hasAge, dave_age});
    triples.push_back({dave, hasSalary, dave_salary});

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
    // Example 1: Simple comparison filter (age > 30)
    // ========================================================================
    std::cout << "\n=== Example 1: Filter with comparison (age > 30) ===" << std::endl;

    SPARQLBuilder builder1;
    auto query1 = builder1
        .Select({"person", "name", "age"})
        .Where()
            .Triple(Var("person"), Iri("http://schema.org/name"), Var("name"))
            .Triple(Var("person"), Iri("http://schema.org/age"), Var("age"))
            .Filter(
                GreaterThan(
                    Term(Var("age")),
                    Lit("30")
                )
            )
        .EndWhere()
        .Build();

    std::cout << "Query:\n" << query1.ToString() << std::endl;

    auto result1 = engine.ExecuteSelect(query1);
    if (!result1.ok()) {
        std::cerr << "Query 1 failed: " << result1.status().ToString() << std::endl;
    } else {
        std::cout << "Results: " << result1.ValueOrDie()->num_rows() << " rows" << std::endl;
        std::cout << result1.ValueOrDie()->ToString() << std::endl;
    }

    std::cout << "\nExplain:\n" << engine.Explain(query1) << std::endl;

    // ========================================================================
    // Example 2: Range filter (25 <= age <= 35)
    // ========================================================================
    std::cout << "\n=== Example 2: Range filter (25 <= age <= 35) ===" << std::endl;

    SPARQLBuilder builder2;
    auto query2 = builder2
        .Select({"person", "name", "age"})
        .Where()
            .Triple(Var("person"), Iri("http://schema.org/name"), Var("name"))
            .Triple(Var("person"), Iri("http://schema.org/age"), Var("age"))
            .Filter(
                And(
                    GreaterThanEqual(Term(Var("age")), Lit("25")),
                    LessThanEqual(Term(Var("age")), Lit("35"))
                )
            )
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

    // ========================================================================
    // Example 3: Salary filter (salary >= 70000)
    // ========================================================================
    std::cout << "\n=== Example 3: Salary filter (salary >= 70000) ===" << std::endl;

    SPARQLBuilder builder3;
    auto query3 = builder3
        .Select({"person", "name", "salary"})
        .Where()
            .Triple(Var("person"), Iri("http://schema.org/name"), Var("name"))
            .Triple(Var("person"), Iri("http://schema.org/salary"), Var("salary"))
            .Filter(
                GreaterThanEqual(
                    Term(Var("salary")),
                    Lit("70000")
                )
            )
        .EndWhere()
        .Build();

    std::cout << "Query:\n" << query3.ToString() << std::endl;

    auto result3 = engine.ExecuteSelect(query3);
    if (!result3.ok()) {
        std::cerr << "Query 3 failed: " << result3.status().ToString() << std::endl;
    } else {
        std::cout << "Results: " << result3.ValueOrDie()->num_rows() << " rows" << std::endl;
        std::cout << result3.ValueOrDie()->ToString() << std::endl;
    }

    // ========================================================================
    // Example 4: Complex logical filter ((age > 30 OR salary >= 60000) AND age != 42)
    // ========================================================================
    std::cout << "\n=== Example 4: Complex logical filter ===" << std::endl;
    std::cout << "((age > 30 OR salary >= 60000) AND age != 42)" << std::endl;

    SPARQLBuilder builder4;
    auto query4 = builder4
        .Select({"person", "name", "age", "salary"})
        .Where()
            .Triple(Var("person"), Iri("http://schema.org/name"), Var("name"))
            .Triple(Var("person"), Iri("http://schema.org/age"), Var("age"))
            .Triple(Var("person"), Iri("http://schema.org/salary"), Var("salary"))
            .Filter(
                And(
                    Or(
                        GreaterThan(Term(Var("age")), Lit("30")),
                        GreaterThanEqual(Term(Var("salary")), Lit("60000"))
                    ),
                    NotEqual(Term(Var("age")), Lit("42"))
                )
            )
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

    // ========================================================================
    // Example 5: NOT filter (NOT age < 30)
    // ========================================================================
    std::cout << "\n=== Example 5: NOT filter (NOT age < 30) ===" << std::endl;

    SPARQLBuilder builder5;
    auto query5 = builder5
        .Select({"person", "name", "age"})
        .Where()
            .Triple(Var("person"), Iri("http://schema.org/name"), Var("name"))
            .Triple(Var("person"), Iri("http://schema.org/age"), Var("age"))
            .Filter(
                Not(
                    LessThan(Term(Var("age")), Lit("30"))
                )
            )
        .EndWhere()
        .Build();

    std::cout << "Query:\n" << query5.ToString() << std::endl;

    auto result5 = engine.ExecuteSelect(query5);
    if (!result5.ok()) {
        std::cerr << "Query 5 failed: " << result5.status().ToString() << std::endl;
    } else {
        std::cout << "Results: " << result5.ValueOrDie()->num_rows() << " rows" << std::endl;
        std::cout << result5.ValueOrDie()->ToString() << std::endl;
    }

    // ========================================================================
    // Example 6: Equality filter (name = "Alice")
    // ========================================================================
    std::cout << "\n=== Example 6: Equality filter (name = \"Alice\") ===" << std::endl;

    SPARQLBuilder builder6;
    auto query6 = builder6
        .Select({"person", "name", "age"})
        .Where()
            .Triple(Var("person"), Iri("http://schema.org/name"), Var("name"))
            .Triple(Var("person"), Iri("http://schema.org/age"), Var("age"))
            .Filter(
                Equal(
                    Term(Var("name")),
                    Lit("Alice")
                )
            )
        .EndWhere()
        .Build();

    std::cout << "Query:\n" << query6.ToString() << std::endl;

    auto result6 = engine.ExecuteSelect(query6);
    if (!result6.ok()) {
        std::cerr << "Query 6 failed: " << result6.status().ToString() << std::endl;
    } else {
        std::cout << "Results: " << result6.ValueOrDie()->num_rows() << " rows" << std::endl;
        std::cout << result6.ValueOrDie()->ToString() << std::endl;
    }

    // ========================================================================
    // Example 7: EXPLAIN ANALYZE for performance profiling
    // ========================================================================
    std::cout << "\n=== Example 7: EXPLAIN ANALYZE ===" << std::endl;

    auto analyze_result = engine.ExplainAnalyze(query4);
    if (analyze_result.ok()) {
        std::cout << analyze_result.ValueOrDie() << std::endl;
    }

    std::cout << "\n=== All FILTER examples completed! ===" << std::endl;

    return 0;
}
