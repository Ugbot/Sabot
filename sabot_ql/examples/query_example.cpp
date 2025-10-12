// Example: Using SabotQL's operator infrastructure to execute queries
//
// This demonstrates how to:
// 1. Load RDF data from N-Triples
// 2. Build operator pipelines programmatically
// 3. Execute queries and get results
// 4. Use the QueryBuilder fluent API

#include <sabot_ql/storage/triple_store.h>
#include <sabot_ql/storage/vocabulary.h>
#include <sabot_ql/parser/rdf_parser.h>
#include <sabot_ql/operators/operator.h>
#include <sabot_ql/operators/join.h>
#include <sabot_ql/operators/aggregate.h>
#include <sabot_ql/execution/executor.h>
#include <marble/db.h>
#include <iostream>

using namespace sabot_ql;

int main() {
    // 1. Open/create database
    marble::DBOptions db_opts;
    db_opts.db_path = "/tmp/sabot_ql_example";
    auto db_result = marble::MarbleDB::Open(db_opts);
    if (!db_result.ok()) {
        std::cerr << "Failed to open database: " << db_result.status().ToString() << std::endl;
        return 1;
    }
    auto db = db_result.value();

    // 2. Create triple store and vocabulary
    auto store_result = TripleStore::Create("/tmp/sabot_ql_example", db);
    if (!store_result.ok()) {
        std::cerr << "Failed to create triple store: " << store_result.status().ToString() << std::endl;
        return 1;
    }
    auto store = store_result.ValueOrDie();

    auto vocab_result = Vocabulary::Create("/tmp/sabot_ql_example", db);
    if (!vocab_result.ok()) {
        std::cerr << "Failed to create vocabulary: " << vocab_result.status().ToString() << std::endl;
        return 1;
    }
    auto vocab = vocab_result.ValueOrDie();

    // 3. Load RDF data from N-Triples file
    std::cout << "Loading RDF data..." << std::endl;

    RdfParserConfig config;
    config.batch_size = 100000;  // 100K triples per batch
    config.skip_invalid_triples = false;

    auto load_status = LoadRdfFile("data.nt", store, vocab, config);
    if (!load_status.ok()) {
        std::cerr << "Failed to load RDF: " << load_status.ToString() << std::endl;
        return 1;
    }

    std::cout << "Total triples: " << store->TotalTriples() << std::endl;

    // 4. Example Query 1: Simple pattern scan with filter
    // SPARQL equivalent:
    //   SELECT ?name ?age WHERE {
    //       ?person <hasName> ?name .
    //       ?person <hasAge> ?age .
    //       FILTER(?age > 30)
    //   }

    std::cout << "\n=== Query 1: Find people over 30 ===" << std::endl;

    // Build pattern: (?person, hasName, ?name)
    auto hasName_term = Term::IRI("http://example.org/hasName");
    auto hasName_id_result = vocab->GetValueId(hasName_term);
    if (!hasName_id_result.ok() || !hasName_id_result.ValueOrDie().has_value()) {
        std::cerr << "hasName predicate not found" << std::endl;
        return 1;
    }
    auto hasName_id = hasName_id_result.ValueOrDie().value();

    TriplePattern pattern1;
    pattern1.subject = std::nullopt;  // ?person
    pattern1.predicate = hasName_id;  // hasName
    pattern1.object = std::nullopt;   // ?name

    // Create scan operator
    auto scan1 = std::make_shared<TripleScanOperator>(store, vocab, pattern1);

    // Create filter: age > 30
    auto filter_predicate = [](const std::shared_ptr<arrow::RecordBatch>& batch)
        -> arrow::Result<std::shared_ptr<arrow::BooleanArray>> {

        // Find 'age' column
        int age_col_idx = batch->schema()->GetFieldIndex("object");
        if (age_col_idx < 0) {
            return arrow::Status::Invalid("Age column not found");
        }

        auto age_array = batch->column(age_col_idx);

        // Build boolean mask: age > 30
        arrow::Int64Builder mask_builder;
        for (int64_t i = 0; i < batch->num_rows(); ++i) {
            if (age_array->IsNull(i)) {
                ARROW_RETURN_NOT_OK(mask_builder.Append(false));
            } else {
                auto scalar_result = age_array->GetScalar(i);
                if (!scalar_result.ok()) {
                    ARROW_RETURN_NOT_OK(mask_builder.Append(false));
                    continue;
                }

                // Decode ValueId to get integer value
                auto value_id = std::static_pointer_cast<arrow::Int64Scalar>(
                    scalar_result.ValueOrDie()
                )->value;

                auto kind = value_id::GetType(value_id);
                if (kind == TermKind::Int) {
                    auto int_value = value_id::DecodeInt(value_id);
                    ARROW_RETURN_NOT_OK(mask_builder.Append(int_value.has_value() && *int_value > 30));
                } else {
                    ARROW_RETURN_NOT_OK(mask_builder.Append(false));
                }
            }
        }

        ARROW_ASSIGN_OR_RAISE(auto mask_array, mask_builder.Finish());
        return std::static_pointer_cast<arrow::BooleanArray>(mask_array);
    };

    auto filter1 = std::make_shared<FilterOperator>(scan1, filter_predicate, "?age > 30");

    // Limit to 10 results
    auto limit1 = std::make_shared<LimitOperator>(filter1, 10);

    // Execute query
    QueryExecutor executor(store, vocab);

    auto result1 = executor.Execute(limit1);
    if (!result1.ok()) {
        std::cerr << "Query 1 failed: " << result1.status().ToString() << std::endl;
        return 1;
    }

    std::cout << "Results: " << result1.ValueOrDie()->num_rows() << " rows" << std::endl;
    std::cout << result1.ValueOrDie()->ToString() << std::endl;
    std::cout << executor.GetStats().ToString() << std::endl;

    // 5. Example Query 2: Join two patterns
    // SPARQL equivalent:
    //   SELECT ?person ?name ?city WHERE {
    //       ?person <hasName> ?name .
    //       ?person <livesIn> ?city .
    //   }
    //   LIMIT 5

    std::cout << "\n=== Query 2: Find people and their cities ===" << std::endl;

    // Pattern 1: (?person, hasName, ?name)
    auto scan2a = std::make_shared<TripleScanOperator>(store, vocab, pattern1);

    // Pattern 2: (?person, livesIn, ?city)
    auto livesIn_term = Term::IRI("http://example.org/livesIn");
    auto livesIn_id_result = vocab->GetValueId(livesIn_term);
    if (!livesIn_id_result.ok() || !livesIn_id_result.ValueOrDie().has_value()) {
        std::cerr << "livesIn predicate not found" << std::endl;
        return 1;
    }
    auto livesIn_id = livesIn_id_result.ValueOrDie().value();

    TriplePattern pattern2;
    pattern2.subject = std::nullopt;  // ?person
    pattern2.predicate = livesIn_id;  // livesIn
    pattern2.object = std::nullopt;   // ?city

    auto scan2b = std::make_shared<TripleScanOperator>(store, vocab, pattern2);

    // Join on ?person (subject column)
    auto join2 = CreateJoin(
        scan2a,
        scan2b,
        {"subject"},  // Left join key
        {"subject"},  // Right join key
        JoinType::Inner,
        JoinAlgorithm::Hash
    );

    // Limit to 5 results
    auto limit2 = std::make_shared<LimitOperator>(join2, 5);

    auto result2 = executor.Execute(limit2);
    if (!result2.ok()) {
        std::cerr << "Query 2 failed: " << result2.status().ToString() << std::endl;
        return 1;
    }

    std::cout << "Results: " << result2.ValueOrDie()->num_rows() << " rows" << std::endl;
    std::cout << result2.ValueOrDie()->ToString() << std::endl;

    // 6. Example Query 3: Aggregation
    // SPARQL equivalent:
    //   SELECT ?city (COUNT(?person) AS ?count) WHERE {
    //       ?person <livesIn> ?city .
    //   }
    //   GROUP BY ?city

    std::cout << "\n=== Query 3: Count people per city ===" << std::endl;

    auto scan3 = std::make_shared<TripleScanOperator>(store, vocab, pattern2);

    // Group by city, count persons
    std::vector<AggregateSpec> aggregates = {
        AggregateSpec(AggregateFunction::Count, "subject", "count", false)
    };

    auto groupby3 = std::make_shared<GroupByOperator>(
        scan3,
        std::vector<std::string>{"object"},  // Group by ?city (object column)
        aggregates
    );

    auto result3 = executor.Execute(groupby3);
    if (!result3.ok()) {
        std::cerr << "Query 3 failed: " << result3.status().ToString() << std::endl;
        return 1;
    }

    std::cout << "Results: " << result3.ValueOrDie()->num_rows() << " groups" << std::endl;
    std::cout << result3.ValueOrDie()->ToString() << std::endl;

    // 7. Example Query 4: Using QueryBuilder fluent API
    std::cout << "\n=== Query 4: Using QueryBuilder ===" << std::endl;

    QueryBuilder builder(store, vocab);

    auto result4 = builder
        .Scan(pattern1)  // Scan for hasName triples
        .Limit(10)       // Limit to 10 results
        .Execute();

    if (!result4.ok()) {
        std::cerr << "Query 4 failed: " << result4.status().ToString() << std::endl;
        return 1;
    }

    std::cout << "Results: " << result4.ValueOrDie()->num_rows() << " rows" << std::endl;
    std::cout << result4.ValueOrDie()->ToString() << std::endl;

    // 8. EXPLAIN plan
    std::cout << "\n=== EXPLAIN Query 4 ===" << std::endl;

    QueryBuilder builder2(store, vocab);
    builder2.Scan(pattern1).Limit(10);

    std::cout << builder2.Explain() << std::endl;

    // 9. EXPLAIN ANALYZE (execute and show stats)
    std::cout << "\n=== EXPLAIN ANALYZE Query 4 ===" << std::endl;

    QueryBuilder builder3(store, vocab);
    auto analyze_result = builder3
        .Scan(pattern1)
        .Limit(10)
        .ExplainAnalyze();

    if (analyze_result.ok()) {
        std::cout << analyze_result.ValueOrDie() << std::endl;
    }

    std::cout << "\n=== All queries executed successfully! ===" << std::endl;

    return 0;
}
