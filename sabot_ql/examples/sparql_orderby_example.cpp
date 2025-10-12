// Example: SPARQL ORDER BY clause
//
// This demonstrates how to use ORDER BY with:
// - Single column sorting (ASC/DESC)
// - Multiple column sorting
// - Combined with FILTER and LIMIT
// - OrderByAsc() and OrderByDesc() convenience methods

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
    db_opts.db_path = "/tmp/sabot_ql_orderby_example";
    auto db_result = marble::MarbleDB::Open(db_opts);
    if (!db_result.ok()) {
        std::cerr << "Failed to open database: " << db_result.status().ToString() << std::endl;
        return 1;
    }
    auto db = db_result.value();

    // 2. Create triple store and vocabulary
    auto store_result = TripleStore::Create("/tmp/sabot_ql_orderby_example", db);
    if (!store_result.ok()) {
        std::cerr << "Failed to create triple store: " << store_result.status().ToString() << std::endl;
        return 1;
    }
    auto store = store_result.ValueOrDie();

    auto vocab_result = Vocabulary::Create("/tmp/sabot_ql_orderby_example", db);
    if (!vocab_result.ok()) {
        std::cerr << "Failed to create vocabulary: " << vocab_result.status().ToString() << std::endl;
        return 1;
    }
    auto vocab = vocab_result.ValueOrDie();

    // 3. Load sample data (employees with name, age, salary, department)
    std::cout << "Loading sample employee data..." << std::endl;

    std::vector<Triple> triples;

    auto hasName = vocab->AddTerm(Term::IRI("http://schema.org/name")).ValueOrDie();
    auto hasAge = vocab->AddTerm(Term::IRI("http://schema.org/age")).ValueOrDie();
    auto hasSalary = vocab->AddTerm(Term::IRI("http://schema.org/salary")).ValueOrDie();
    auto hasDept = vocab->AddTerm(Term::IRI("http://schema.org/department")).ValueOrDie();

    // Engineering team
    auto alice = vocab->AddTerm(Term::IRI("http://example.org/Alice")).ValueOrDie();
    auto alice_name = vocab->AddTerm(Term::Literal("Alice")).ValueOrDie();
    auto alice_age = vocab->AddTerm(Term::Literal("35", "", "http://www.w3.org/2001/XMLSchema#integer")).ValueOrDie();
    auto alice_salary = vocab->AddTerm(Term::Literal("95000", "", "http://www.w3.org/2001/XMLSchema#integer")).ValueOrDie();
    auto eng_dept = vocab->AddTerm(Term::Literal("Engineering")).ValueOrDie();

    triples.push_back({alice, hasName, alice_name});
    triples.push_back({alice, hasAge, alice_age});
    triples.push_back({alice, hasSalary, alice_salary});
    triples.push_back({alice, hasDept, eng_dept});

    auto bob = vocab->AddTerm(Term::IRI("http://example.org/Bob")).ValueOrDie();
    auto bob_name = vocab->AddTerm(Term::Literal("Bob")).ValueOrDie();
    auto bob_age = vocab->AddTerm(Term::Literal("28", "", "http://www.w3.org/2001/XMLSchema#integer")).ValueOrDie();
    auto bob_salary = vocab->AddTerm(Term::Literal("75000", "", "http://www.w3.org/2001/XMLSchema#integer")).ValueOrDie();

    triples.push_back({bob, hasName, bob_name});
    triples.push_back({bob, hasAge, bob_age});
    triples.push_back({bob, hasSalary, bob_salary});
    triples.push_back({bob, hasDept, eng_dept});

    auto carol = vocab->AddTerm(Term::IRI("http://example.org/Carol")).ValueOrDie();
    auto carol_name = vocab->AddTerm(Term::Literal("Carol")).ValueOrDie();
    auto carol_age = vocab->AddTerm(Term::Literal("42", "", "http://www.w3.org/2001/XMLSchema#integer")).ValueOrDie();
    auto carol_salary = vocab->AddTerm(Term::Literal("110000", "", "http://www.w3.org/2001/XMLSchema#integer")).ValueOrDie();

    triples.push_back({carol, hasName, carol_name});
    triples.push_back({carol, hasAge, carol_age});
    triples.push_back({carol, hasSalary, carol_salary});
    triples.push_back({carol, hasDept, eng_dept});

    // Sales team
    auto dave = vocab->AddTerm(Term::IRI("http://example.org/Dave")).ValueOrDie();
    auto dave_name = vocab->AddTerm(Term::Literal("Dave")).ValueOrDie();
    auto dave_age = vocab->AddTerm(Term::Literal("31", "", "http://www.w3.org/2001/XMLSchema#integer")).ValueOrDie();
    auto dave_salary = vocab->AddTerm(Term::Literal("65000", "", "http://www.w3.org/2001/XMLSchema#integer")).ValueOrDie();
    auto sales_dept = vocab->AddTerm(Term::Literal("Sales")).ValueOrDie();

    triples.push_back({dave, hasName, dave_name});
    triples.push_back({dave, hasAge, dave_age});
    triples.push_back({dave, hasSalary, dave_salary});
    triples.push_back({dave, hasDept, sales_dept});

    auto eve = vocab->AddTerm(Term::IRI("http://example.org/Eve")).ValueOrDie();
    auto eve_name = vocab->AddTerm(Term::Literal("Eve")).ValueOrDie();
    auto eve_age = vocab->AddTerm(Term::Literal("29", "", "http://www.w3.org/2001/XMLSchema#integer")).ValueOrDie();
    auto eve_salary = vocab->AddTerm(Term::Literal("68000", "", "http://www.w3.org/2001/XMLSchema#integer")).ValueOrDie();

    triples.push_back({eve, hasName, eve_name});
    triples.push_back({eve, hasAge, eve_age});
    triples.push_back({eve, hasSalary, eve_salary});
    triples.push_back({eve, hasDept, sales_dept});

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
    // Example 1: ORDER BY single column (age ascending)
    // ========================================================================
    std::cout << "\n=== Example 1: ORDER BY age ASC ===" << std::endl;

    SPARQLBuilder builder1;
    auto query1 = builder1
        .Select({"person", "name", "age"})
        .Where()
            .Triple(Var("person"), Iri("http://schema.org/name"), Var("name"))
            .Triple(Var("person"), Iri("http://schema.org/age"), Var("age"))
        .EndWhere()
        .OrderByAsc("age")  // Convenience method
        .Build();

    std::cout << "Query:\n" << query1.ToString() << std::endl;

    auto result1 = engine.ExecuteSelect(query1);
    if (!result1.ok()) {
        std::cerr << "Query 1 failed: " << result1.status().ToString() << std::endl;
    } else {
        std::cout << "Results (sorted by age ascending):\n";
        std::cout << result1.ValueOrDie()->ToString() << std::endl;
    }

    // ========================================================================
    // Example 2: ORDER BY single column (salary descending)
    // ========================================================================
    std::cout << "\n=== Example 2: ORDER BY salary DESC ===" << std::endl;

    SPARQLBuilder builder2;
    auto query2 = builder2
        .Select({"person", "name", "salary"})
        .Where()
            .Triple(Var("person"), Iri("http://schema.org/name"), Var("name"))
            .Triple(Var("person"), Iri("http://schema.org/salary"), Var("salary"))
        .EndWhere()
        .OrderByDesc("salary")  // Convenience method
        .Build();

    std::cout << "Query:\n" << query2.ToString() << std::endl;

    auto result2 = engine.ExecuteSelect(query2);
    if (!result2.ok()) {
        std::cerr << "Query 2 failed: " << result2.status().ToString() << std::endl;
    } else {
        std::cout << "Results (sorted by salary descending):\n";
        std::cout << result2.ValueOrDie()->ToString() << std::endl;
    }

    // ========================================================================
    // Example 3: ORDER BY multiple columns (department ASC, salary DESC)
    // ========================================================================
    std::cout << "\n=== Example 3: ORDER BY department ASC, salary DESC ===" << std::endl;

    SPARQLBuilder builder3;
    auto query3 = builder3
        .Select({"person", "name", "department", "salary"})
        .Where()
            .Triple(Var("person"), Iri("http://schema.org/name"), Var("name"))
            .Triple(Var("person"), Iri("http://schema.org/department"), Var("department"))
            .Triple(Var("person"), Iri("http://schema.org/salary"), Var("salary"))
        .EndWhere()
        .OrderByAsc("department")    // First sort by department
        .OrderByDesc("salary")       // Then by salary (within each department)
        .Build();

    std::cout << "Query:\n" << query3.ToString() << std::endl;

    auto result3 = engine.ExecuteSelect(query3);
    if (!result3.ok()) {
        std::cerr << "Query 3 failed: " << result3.status().ToString() << std::endl;
    } else {
        std::cout << "Results (grouped by department, sorted by salary):\n";
        std::cout << result3.ValueOrDie()->ToString() << std::endl;
    }

    // ========================================================================
    // Example 4: ORDER BY with FILTER and LIMIT (top 3 highest paid)
    // ========================================================================
    std::cout << "\n=== Example 4: ORDER BY with FILTER and LIMIT (top 3 earners) ===" << std::endl;

    SPARQLBuilder builder4;
    auto query4 = builder4
        .Select({"person", "name", "salary"})
        .Where()
            .Triple(Var("person"), Iri("http://schema.org/name"), Var("name"))
            .Triple(Var("person"), Iri("http://schema.org/salary"), Var("salary"))
        .EndWhere()
        .OrderByDesc("salary")
        .Limit(3)
        .Build();

    std::cout << "Query:\n" << query4.ToString() << std::endl;

    auto result4 = engine.ExecuteSelect(query4);
    if (!result4.ok()) {
        std::cerr << "Query 4 failed: " << result4.status().ToString() << std::endl;
    } else {
        std::cout << "Results (top 3 highest paid):\n";
        std::cout << result4.ValueOrDie()->ToString() << std::endl;
    }

    // ========================================================================
    // Example 5: ORDER BY with FILTER (Engineering dept, sorted by age)
    // ========================================================================
    std::cout << "\n=== Example 5: ORDER BY with FILTER (Engineering only) ===" << std::endl;

    SPARQLBuilder builder5;
    auto query5 = builder5
        .Select({"person", "name", "age", "department"})
        .Where()
            .Triple(Var("person"), Iri("http://schema.org/name"), Var("name"))
            .Triple(Var("person"), Iri("http://schema.org/age"), Var("age"))
            .Triple(Var("person"), Iri("http://schema.org/department"), Var("department"))
            .Filter(
                expr::Equal(
                    expr::Term(Var("department")),
                    expr::Lit("Engineering")
                )
            )
        .EndWhere()
        .OrderByAsc("age")
        .Build();

    std::cout << "Query:\n" << query5.ToString() << std::endl;

    auto result5 = engine.ExecuteSelect(query5);
    if (!result5.ok()) {
        std::cerr << "Query 5 failed: " << result5.status().ToString() << std::endl;
    } else {
        std::cout << "Results (Engineering dept, sorted by age):\n";
        std::cout << result5.ValueOrDie()->ToString() << std::endl;
    }

    // ========================================================================
    // Example 6: EXPLAIN query plan with ORDER BY
    // ========================================================================
    std::cout << "\n=== Example 6: EXPLAIN ORDER BY query ===" << std::endl;

    std::cout << engine.Explain(query2) << std::endl;

    // ========================================================================
    // Example 7: EXPLAIN ANALYZE (with statistics)
    // ========================================================================
    std::cout << "\n=== Example 7: EXPLAIN ANALYZE ===" << std::endl;

    auto analyze_result = engine.ExplainAnalyze(query3);
    if (analyze_result.ok()) {
        std::cout << analyze_result.ValueOrDie() << std::endl;
    }

    std::cout << "\n=== All ORDER BY examples completed! ===" << std::endl;

    return 0;
}
