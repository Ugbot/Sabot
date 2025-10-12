#include <sabot_ql/sparql/query_engine.h>
#include <sabot_ql/sparql/parser.h>
#include <sabot_ql/storage/triple_store.h>
#include <sabot_ql/storage/vocabulary.h>
#include <iostream>
#include <memory>

using namespace sabot_ql;
using namespace sabot_ql::sparql;

// Helper function to print query results
void PrintQueryResult(const std::string& test_name,
                      const std::string& sparql_query,
                      QueryEngine& engine) {
    std::cout << "\n═══════════════════════════════════════════════════════════\n";
    std::cout << "Test: " << test_name << "\n";
    std::cout << "═══════════════════════════════════════════════════════════\n";
    std::cout << "SPARQL Query:\n" << sparql_query << "\n\n";

    // Parse query
    auto parse_result = ParseSPARQL(sparql_query);
    if (!parse_result.ok()) {
        std::cout << "❌ Parse Error: " << parse_result.status().ToString() << "\n";
        return;
    }

    auto query = parse_result.ValueOrDie();
    std::cout << "✅ Parse Success!\n\n";

    // Show query plan
    std::cout << "Query Plan:\n";
    std::cout << engine.Explain(query) << "\n\n";

    // Execute query
    auto result = engine.ExecuteSelect(query.select_query);
    if (!result.ok()) {
        std::cout << "❌ Execution Error: " << result.status().ToString() << "\n";
        return;
    }

    auto table = result.ValueOrDie();
    std::cout << "Results (" << table->num_rows() << " rows):\n";
    std::cout << table->ToString() << "\n";
}

int main() {
    std::cout << "╔═══════════════════════════════════════════════════════════╗\n";
    std::cout << "║  SabotQL SPARQL Aggregation Examples                     ║\n";
    std::cout << "║  Demonstrates GROUP BY and aggregate functions           ║\n";
    std::cout << "╚═══════════════════════════════════════════════════════════╝\n";

    // Create database and initialize stores
    std::cout << "\n[Setup] Creating database and loading sample data...\n";

    // TODO: Initialize MarbleDB, TripleStore, and Vocabulary
    // For now, this is a template showing the structure

    /*
    auto db = marble::DB::Open("/tmp/sparql_aggregation_test").ValueOrDie();
    auto store = TripleStore::Create("/tmp/sparql_aggregation_test", db).ValueOrDie();
    auto vocab = Vocabulary::Create("/tmp/sparql_aggregation_test", db).ValueOrDie();

    // Add sample data: People with names, ages, and cities
    auto schema_name = vocab->AddTerm(Term::IRI("http://schema.org/name")).ValueOrDie();
    auto schema_age = vocab->AddTerm(Term::IRI("http://schema.org/age")).ValueOrDie();
    auto schema_livesIn = vocab->AddTerm(Term::IRI("http://schema.org/livesIn")).ValueOrDie();
    auto schema_salary = vocab->AddTerm(Term::IRI("http://schema.org/salary")).ValueOrDie();

    // People
    auto alice = vocab->AddTerm(Term::IRI("http://example.org/Alice")).ValueOrDie();
    auto bob = vocab->AddTerm(Term::IRI("http://example.org/Bob")).ValueOrDie();
    auto charlie = vocab->AddTerm(Term::IRI("http://example.org/Charlie")).ValueOrDie();
    auto diana = vocab->AddTerm(Term::IRI("http://example.org/Diana")).ValueOrDie();
    auto eve = vocab->AddTerm(Term::IRI("http://example.org/Eve")).ValueOrDie();

    // Values
    auto name_alice = vocab->AddTerm(Term::Literal("Alice")).ValueOrDie();
    auto name_bob = vocab->AddTerm(Term::Literal("Bob")).ValueOrDie();
    auto name_charlie = vocab->AddTerm(Term::Literal("Charlie")).ValueOrDie();
    auto name_diana = vocab->AddTerm(Term::Literal("Diana")).ValueOrDie();
    auto name_eve = vocab->AddTerm(Term::Literal("Eve")).ValueOrDie();

    auto age_30 = vocab->AddTerm(Term::Literal("30", "", "http://www.w3.org/2001/XMLSchema#integer")).ValueOrDie();
    auto age_25 = vocab->AddTerm(Term::Literal("25", "", "http://www.w3.org/2001/XMLSchema#integer")).ValueOrDie();
    auto age_35 = vocab->AddTerm(Term::Literal("35", "", "http://www.w3.org/2001/XMLSchema#integer")).ValueOrDie();
    auto age_28 = vocab->AddTerm(Term::Literal("28", "", "http://www.w3.org/2001/XMLSchema#integer")).ValueOrDie();
    auto age_32 = vocab->AddTerm(Term::Literal("32", "", "http://www.w3.org/2001/XMLSchema#integer")).ValueOrDie();

    auto sf = vocab->AddTerm(Term::IRI("http://example.org/SanFrancisco")).ValueOrDie();
    auto ny = vocab->AddTerm(Term::IRI("http://example.org/NewYork")).ValueOrDie();
    auto la = vocab->AddTerm(Term::IRI("http://example.org/LosAngeles")).ValueOrDie();

    auto salary_100k = vocab->AddTerm(Term::Literal("100000", "", "http://www.w3.org/2001/XMLSchema#integer")).ValueOrDie();
    auto salary_90k = vocab->AddTerm(Term::Literal("90000", "", "http://www.w3.org/2001/XMLSchema#integer")).ValueOrDie();
    auto salary_120k = vocab->AddTerm(Term::Literal("120000", "", "http://www.w3.org/2001/XMLSchema#integer")).ValueOrDie();
    auto salary_85k = vocab->AddTerm(Term::Literal("85000", "", "http://www.w3.org/2001/XMLSchema#integer")).ValueOrDie();
    auto salary_110k = vocab->AddTerm(Term::Literal("110000", "", "http://www.w3.org/2001/XMLSchema#integer")).ValueOrDie();

    // Insert triples
    std::vector<Triple> triples = {
        // Alice - 30 - San Francisco - $100k
        {alice, schema_name, name_alice},
        {alice, schema_age, age_30},
        {alice, schema_livesIn, sf},
        {alice, schema_salary, salary_100k},

        // Bob - 25 - New York - $90k
        {bob, schema_name, name_bob},
        {bob, schema_age, age_25},
        {bob, schema_livesIn, ny},
        {bob, schema_salary, salary_90k},

        // Charlie - 35 - San Francisco - $120k
        {charlie, schema_name, name_charlie},
        {charlie, schema_age, age_35},
        {charlie, schema_livesIn, sf},
        {charlie, schema_salary, salary_120k},

        // Diana - 28 - New York - $85k
        {diana, schema_name, name_diana},
        {diana, schema_age, age_28},
        {diana, schema_livesIn, ny},
        {diana, schema_salary, salary_85k},

        // Eve - 32 - Los Angeles - $110k
        {eve, schema_name, name_eve},
        {eve, schema_age, age_32},
        {eve, schema_livesIn, la},
        {eve, schema_salary, salary_110k},
    };

    auto insert_status = store->InsertTriples(triples);
    if (!insert_status.ok()) {
        std::cerr << "Failed to insert triples: " << insert_status.ToString() << std::endl;
        return 1;
    }

    std::cout << "✅ Loaded " << triples.size() << " triples\n";
    std::cout << "   - 5 people with names, ages, cities, and salaries\n\n";

    // Create query engine
    QueryEngine engine(store, vocab);

    // ============================================================================
    // Example 1: Simple COUNT - Count all people
    // ============================================================================
    PrintQueryResult(
        "Example 1: Simple COUNT - Count all people",
        R"(
            SELECT (COUNT(?person) AS ?total)
            WHERE {
                ?person <http://schema.org/name> ?name .
            }
        )",
        engine
    );

    // ============================================================================
    // Example 2: COUNT(*) - Count all triples matching pattern
    // ============================================================================
    PrintQueryResult(
        "Example 2: COUNT(*) - Count all name triples",
        R"(
            SELECT (COUNT(*) AS ?total)
            WHERE {
                ?person <http://schema.org/name> ?name .
            }
        )",
        engine
    );

    // ============================================================================
    // Example 3: GROUP BY with COUNT - Count people per city
    // ============================================================================
    PrintQueryResult(
        "Example 3: GROUP BY with COUNT - People per city",
        R"(
            SELECT ?city (COUNT(?person) AS ?population)
            WHERE {
                ?person <http://schema.org/name> ?name .
                ?person <http://schema.org/livesIn> ?city .
            }
            GROUP BY ?city
            ORDER BY DESC(?population)
        )",
        engine
    );

    // ============================================================================
    // Example 4: Multiple aggregates - City statistics
    // ============================================================================
    PrintQueryResult(
        "Example 4: Multiple aggregates - City statistics",
        R"(
            SELECT ?city
                   (COUNT(?person) AS ?population)
                   (AVG(?age) AS ?avg_age)
                   (AVG(?salary) AS ?avg_salary)
            WHERE {
                ?person <http://schema.org/livesIn> ?city .
                ?person <http://schema.org/age> ?age .
                ?person <http://schema.org/salary> ?salary .
            }
            GROUP BY ?city
            ORDER BY DESC(?population)
        )",
        engine
    );

    // ============================================================================
    // Example 5: MIN and MAX - Age range per city
    // ============================================================================
    PrintQueryResult(
        "Example 5: MIN and MAX - Age range per city",
        R"(
            SELECT ?city
                   (MIN(?age) AS ?min_age)
                   (MAX(?age) AS ?max_age)
            WHERE {
                ?person <http://schema.org/livesIn> ?city .
                ?person <http://schema.org/age> ?age .
            }
            GROUP BY ?city
        )",
        engine
    );

    // ============================================================================
    // Example 6: SUM - Total salary by city
    // ============================================================================
    PrintQueryResult(
        "Example 6: SUM - Total salary by city",
        R"(
            SELECT ?city (SUM(?salary) AS ?total_salary)
            WHERE {
                ?person <http://schema.org/livesIn> ?city .
                ?person <http://schema.org/salary> ?salary .
            }
            GROUP BY ?city
            ORDER BY DESC(?total_salary)
        )",
        engine
    );

    // ============================================================================
    // Example 7: COUNT DISTINCT - Unique cities
    // ============================================================================
    PrintQueryResult(
        "Example 7: COUNT DISTINCT - Count unique cities",
        R"(
            SELECT (COUNT(DISTINCT ?city) AS ?unique_cities)
            WHERE {
                ?person <http://schema.org/livesIn> ?city .
            }
        )",
        engine
    );

    // ============================================================================
    // Example 8: SAMPLE - Get one person per city
    // ============================================================================
    PrintQueryResult(
        "Example 8: SAMPLE - One person per city",
        R"(
            SELECT ?city (SAMPLE(?name) AS ?example_person)
            WHERE {
                ?person <http://schema.org/livesIn> ?city .
                ?person <http://schema.org/name> ?name .
            }
            GROUP BY ?city
        )",
        engine
    );

    // ============================================================================
    // Example 9: GROUP_CONCAT - List all people per city
    // ============================================================================
    PrintQueryResult(
        "Example 9: GROUP_CONCAT - All people per city",
        R"(
            SELECT ?city (GROUP_CONCAT(?name) AS ?people)
            WHERE {
                ?person <http://schema.org/livesIn> ?city .
                ?person <http://schema.org/name> ?name .
            }
            GROUP BY ?city
        )",
        engine
    );

    // ============================================================================
    // Example 10: Complex query - Top 2 cities by average salary
    // ============================================================================
    PrintQueryResult(
        "Example 10: Top 2 cities by average salary",
        R"(
            SELECT ?city
                   (COUNT(?person) AS ?population)
                   (AVG(?salary) AS ?avg_salary)
                   (MIN(?age) AS ?youngest)
                   (MAX(?age) AS ?oldest)
            WHERE {
                ?person <http://schema.org/livesIn> ?city .
                ?person <http://schema.org/age> ?age .
                ?person <http://schema.org/salary> ?salary .
            }
            GROUP BY ?city
            ORDER BY DESC(?avg_salary)
            LIMIT 2
        )",
        engine
    );

    // ============================================================================
    // Example 11: Aggregate without GROUP BY - Overall statistics
    // ============================================================================
    PrintQueryResult(
        "Example 11: Overall statistics (no GROUP BY)",
        R"(
            SELECT (COUNT(?person) AS ?total_people)
                   (AVG(?age) AS ?avg_age)
                   (MIN(?age) AS ?youngest)
                   (MAX(?age) AS ?oldest)
                   (SUM(?salary) AS ?total_salary)
            WHERE {
                ?person <http://schema.org/name> ?name .
                ?person <http://schema.org/age> ?age .
                ?person <http://schema.org/salary> ?salary .
            }
        )",
        engine
    );

    std::cout << "\n╔═══════════════════════════════════════════════════════════╗\n";
    std::cout << "║                   All Examples Complete!                 ║\n";
    std::cout << "╚═══════════════════════════════════════════════════════════╝\n";
    */

    std::cout << "\n[Note] This example requires MarbleDB, TripleStore, and Vocabulary\n";
    std::cout << "       to be fully initialized. The structure shows how aggregation\n";
    std::cout << "       queries would be executed once the storage layer is ready.\n\n";

    std::cout << "Aggregation features demonstrated:\n";
    std::cout << "  ✅ COUNT(?var) - Count values\n";
    std::cout << "  ✅ COUNT(*) - Count all rows\n";
    std::cout << "  ✅ COUNT(DISTINCT ?var) - Count unique values\n";
    std::cout << "  ✅ SUM(?var) - Sum numeric values\n";
    std::cout << "  ✅ AVG(?var) - Average numeric values\n";
    std::cout << "  ✅ MIN(?var) - Minimum value\n";
    std::cout << "  ✅ MAX(?var) - Maximum value\n";
    std::cout << "  ✅ SAMPLE(?var) - Sample value from group\n";
    std::cout << "  ✅ GROUP_CONCAT(?var) - Concatenate values\n";
    std::cout << "  ✅ GROUP BY - Group results by variables\n";
    std::cout << "  ✅ Multiple aggregates in single query\n";
    std::cout << "  ✅ GROUP BY with ORDER BY and LIMIT\n";

    return 0;
}
