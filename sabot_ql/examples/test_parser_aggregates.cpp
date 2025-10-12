#include <sabot_ql/sparql/parser.h>
#include <sabot_ql/sparql/ast.h>
#include <iostream>
#include <string>

using namespace sabot_ql::sparql;

void PrintParseResult(const std::string& test_name, const std::string& query_text) {
    std::cout << "\n========================================\n";
    std::cout << "Test: " << test_name << "\n";
    std::cout << "========================================\n";
    std::cout << "Query:\n" << query_text << "\n\n";

    auto result = ParseSPARQL(query_text);
    if (!result.ok()) {
        std::cout << "❌ Parse Error: " << result.status().ToString() << "\n";
        return;
    }

    auto query = result.ValueOrDie();
    std::cout << "✅ Parse Success!\n\n";
    std::cout << "Parsed Query:\n" << query.ToString() << "\n";
}

int main() {
    std::cout << "╔════════════════════════════════════════════════════════════╗\n";
    std::cout << "║  SabotQL SPARQL Parser - Aggregate Functions Test Suite   ║\n";
    std::cout << "╚════════════════════════════════════════════════════════════╝\n";

    // Test 1: Simple COUNT aggregate
    PrintParseResult(
        "Simple COUNT aggregate",
        R"(
            SELECT (COUNT(?person) AS ?count)
            WHERE {
                ?person <http://schema.org/name> ?name .
            }
        )"
    );

    // Test 2: COUNT(*) aggregate
    PrintParseResult(
        "COUNT(*) aggregate",
        R"(
            SELECT (COUNT(*) AS ?total)
            WHERE {
                ?person <http://schema.org/name> ?name .
            }
        )"
    );

    // Test 3: COUNT DISTINCT aggregate
    PrintParseResult(
        "COUNT DISTINCT aggregate",
        R"(
            SELECT (COUNT(DISTINCT ?city) AS ?unique_cities)
            WHERE {
                ?person <http://schema.org/address> ?address .
                ?address <http://schema.org/city> ?city .
            }
        )"
    );

    // Test 4: GROUP BY with COUNT
    PrintParseResult(
        "GROUP BY with COUNT",
        R"(
            SELECT ?city (COUNT(?person) AS ?population)
            WHERE {
                ?person <http://schema.org/address> ?address .
                ?address <http://schema.org/city> ?city .
            }
            GROUP BY ?city
        )"
    );

    // Test 5: Multiple aggregates with GROUP BY
    PrintParseResult(
        "Multiple aggregates with GROUP BY",
        R"(
            SELECT ?department (COUNT(?employee) AS ?count) (AVG(?salary) AS ?avg_salary)
            WHERE {
                ?employee <http://schema.org/department> ?department .
                ?employee <http://schema.org/salary> ?salary .
            }
            GROUP BY ?department
        )"
    );

    // Test 6: SUM aggregate
    PrintParseResult(
        "SUM aggregate",
        R"(
            SELECT (SUM(?amount) AS ?total_revenue)
            WHERE {
                ?transaction <http://schema.org/amount> ?amount .
            }
        )"
    );

    // Test 7: MIN and MAX aggregates
    PrintParseResult(
        "MIN and MAX aggregates",
        R"(
            SELECT (MIN(?price) AS ?min_price) (MAX(?price) AS ?max_price)
            WHERE {
                ?product <http://schema.org/price> ?price .
            }
        )"
    );

    // Test 8: GROUP BY with multiple variables
    PrintParseResult(
        "GROUP BY with multiple variables",
        R"(
            SELECT ?city ?country (COUNT(?person) AS ?count)
            WHERE {
                ?person <http://schema.org/address> ?address .
                ?address <http://schema.org/city> ?city .
                ?address <http://schema.org/country> ?country .
            }
            GROUP BY ?city, ?country
        )"
    );

    // Test 9: GROUP BY with ORDER BY and LIMIT
    PrintParseResult(
        "GROUP BY with ORDER BY and LIMIT",
        R"(
            SELECT ?category (COUNT(?product) AS ?count)
            WHERE {
                ?product <http://schema.org/category> ?category .
            }
            GROUP BY ?category
            ORDER BY DESC(?count)
            LIMIT 10
        )"
    );

    // Test 10: SAMPLE aggregate
    PrintParseResult(
        "SAMPLE aggregate",
        R"(
            SELECT ?country (SAMPLE(?capital) AS ?example_capital)
            WHERE {
                ?country <http://schema.org/capital> ?capital .
            }
            GROUP BY ?country
        )"
    );

    // Test 11: GROUP_CONCAT aggregate
    PrintParseResult(
        "GROUP_CONCAT aggregate",
        R"(
            SELECT ?author (GROUP_CONCAT(?book) AS ?books)
            WHERE {
                ?author <http://schema.org/wrote> ?book .
            }
            GROUP BY ?author
        )"
    );

    // Test 12: Mix of simple variables and aggregates (should error - invalid SPARQL)
    PrintParseResult(
        "Invalid: Mix of simple variables and aggregates without GROUP BY",
        R"(
            SELECT ?person (COUNT(?friend) AS ?friend_count)
            WHERE {
                ?person <http://schema.org/knows> ?friend .
            }
        )"
    );

    std::cout << "\n╔════════════════════════════════════════════════════════════╗\n";
    std::cout << "║                     Tests Complete                         ║\n";
    std::cout << "╚════════════════════════════════════════════════════════════╝\n";

    return 0;
}
