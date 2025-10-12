// Example: SPARQL UNION clause
//
// This demonstrates how to use UNION to combine results from multiple graph patterns:
// - Simple UNION (combining two patterns)
// - UNION with FILTER
// - Schema unification (patterns with different variables)
// - EXPLAIN to see query plan
//
// UNION performs deduplication by default (use UNION ALL to keep duplicates)

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
    db_opts.db_path = "/tmp/sabot_ql_union_example";
    auto db_result = marble::MarbleDB::Open(db_opts);
    if (!db_result.ok()) {
        std::cerr << "Failed to open database: " << db_result.status().ToString() << std::endl;
        return 1;
    }
    auto db = db_result.value();

    // 2. Create triple store and vocabulary
    auto store_result = TripleStore::Create("/tmp/sabot_ql_union_example", db);
    if (!store_result.ok()) {
        std::cerr << "Failed to create triple store: " << store_result.status().ToString() << std::endl;
        return 1;
    }
    auto store = store_result.ValueOrDie();

    auto vocab_result = Vocabulary::Create("/tmp/sabot_ql_union_example", db);
    if (!vocab_result.ok()) {
        std::cerr << "Failed to create vocabulary: " << vocab_result.status().ToString() << std::endl;
        return 1;
    }
    auto vocab = vocab_result.ValueOrDie();

    // 3. Load sample data (books and movies)
    std::cout << "Loading sample data (books and movies)..." << std::endl;

    std::vector<Triple> triples;

    // Predicates
    auto hasTitle = vocab->AddTerm(Term::IRI("http://schema.org/title")).ValueOrDie();
    auto hasAuthor = vocab->AddTerm(Term::IRI("http://schema.org/author")).ValueOrDie();
    auto hasDirector = vocab->AddTerm(Term::IRI("http://schema.org/director")).ValueOrDie();
    auto hasYear = vocab->AddTerm(Term::IRI("http://schema.org/year")).ValueOrDie();
    auto hasGenre = vocab->AddTerm(Term::IRI("http://schema.org/genre")).ValueOrDie();

    // Books
    auto book1 = vocab->AddTerm(Term::IRI("http://example.org/book1")).ValueOrDie();
    auto book1_title = vocab->AddTerm(Term::Literal("1984")).ValueOrDie();
    auto book1_author = vocab->AddTerm(Term::Literal("George Orwell")).ValueOrDie();
    auto book1_year = vocab->AddTerm(Term::Literal("1949", "", "http://www.w3.org/2001/XMLSchema#integer")).ValueOrDie();
    auto book1_genre = vocab->AddTerm(Term::Literal("Dystopian")).ValueOrDie();

    triples.push_back({book1, hasTitle, book1_title});
    triples.push_back({book1, hasAuthor, book1_author});
    triples.push_back({book1, hasYear, book1_year});
    triples.push_back({book1, hasGenre, book1_genre});

    auto book2 = vocab->AddTerm(Term::IRI("http://example.org/book2")).ValueOrDie();
    auto book2_title = vocab->AddTerm(Term::Literal("The Great Gatsby")).ValueOrDie();
    auto book2_author = vocab->AddTerm(Term::Literal("F. Scott Fitzgerald")).ValueOrDie();
    auto book2_year = vocab->AddTerm(Term::Literal("1925", "", "http://www.w3.org/2001/XMLSchema#integer")).ValueOrDie();
    auto book2_genre = vocab->AddTerm(Term::Literal("Fiction")).ValueOrDie();

    triples.push_back({book2, hasTitle, book2_title});
    triples.push_back({book2, hasAuthor, book2_author});
    triples.push_back({book2, hasYear, book2_year});
    triples.push_back({book2, hasGenre, book2_genre});

    // Movies
    auto movie1 = vocab->AddTerm(Term::IRI("http://example.org/movie1")).ValueOrDie();
    auto movie1_title = vocab->AddTerm(Term::Literal("Inception")).ValueOrDie();
    auto movie1_director = vocab->AddTerm(Term::Literal("Christopher Nolan")).ValueOrDie();
    auto movie1_year = vocab->AddTerm(Term::Literal("2010", "", "http://www.w3.org/2001/XMLSchema#integer")).ValueOrDie();
    auto movie1_genre = vocab->AddTerm(Term::Literal("Sci-Fi")).ValueOrDie();

    triples.push_back({movie1, hasTitle, movie1_title});
    triples.push_back({movie1, hasDirector, movie1_director});
    triples.push_back({movie1, hasYear, movie1_year});
    triples.push_back({movie1, hasGenre, movie1_genre});

    auto movie2 = vocab->AddTerm(Term::IRI("http://example.org/movie2")).ValueOrDie();
    auto movie2_title = vocab->AddTerm(Term::Literal("The Matrix")).ValueOrDie();
    auto movie2_director = vocab->AddTerm(Term::Literal("The Wachowskis")).ValueOrDie();
    auto movie2_year = vocab->AddTerm(Term::Literal("1999", "", "http://www.w3.org/2001/XMLSchema#integer")).ValueOrDie();
    auto movie2_genre = vocab->AddTerm(Term::Literal("Sci-Fi")).ValueOrDie();

    triples.push_back({movie2, hasTitle, movie2_title});
    triples.push_back({movie2, hasDirector, movie2_director});
    triples.push_back({movie2, hasYear, movie2_year});
    triples.push_back({movie2, hasGenre, movie2_genre});

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
    // Example 1: Simple UNION (books OR movies)
    // ========================================================================
    std::cout << "\n=== Example 1: UNION books and movies ===" << std::endl;
    std::cout << "Find all items that are either books (have author) OR movies (have director)\n" << std::endl;

    // SPARQL equivalent:
    // SELECT ?item ?title WHERE {
    //   { ?item <hasTitle> ?title . ?item <hasAuthor> ?author }
    //   UNION
    //   { ?item <hasTitle> ?title . ?item <hasDirector> ?director }
    // }

    SPARQLBuilder builder1;

    // Build first pattern (books)
    auto pattern1 = std::make_shared<QueryPattern>();
    BasicGraphPattern bgp1;
    bgp1.triples = {
        TriplePattern(Var("item"), Iri("http://schema.org/title"), Var("title")),
        TriplePattern(Var("item"), Iri("http://schema.org/author"), Var("author"))
    };
    pattern1->bgp = bgp1;

    // Build second pattern (movies)
    auto pattern2 = std::make_shared<QueryPattern>();
    BasicGraphPattern bgp2;
    bgp2.triples = {
        TriplePattern(Var("item"), Iri("http://schema.org/title"), Var("title")),
        TriplePattern(Var("item"), Iri("http://schema.org/director"), Var("director"))
    };
    pattern2->bgp = bgp2;

    // Create UNION pattern
    UnionPattern union1({pattern1, pattern2});

    // Build query manually (SPARQLBuilder doesn't have UNION support yet)
    SelectQuery query1;
    query1.select.variables = {Variable("item"), Variable("title")};
    query1.where.unions = {union1};

    std::cout << "Query: SELECT ?item ?title WHERE { { books } UNION { movies } }" << std::endl;

    auto result1 = engine.ExecuteSelect(query1);
    if (!result1.ok()) {
        std::cerr << "Query 1 failed: " << result1.status().ToString() << std::endl;
    } else {
        std::cout << "\nResults (all books and movies):\n";
        std::cout << result1.ValueOrDie()->ToString() << std::endl;
    }

    // ========================================================================
    // Example 2: UNION with FILTER (recent works)
    // ========================================================================
    std::cout << "\n=== Example 2: UNION with FILTER (works after 2000) ===" << std::endl;

    // SPARQL equivalent:
    // SELECT ?item ?title ?year WHERE {
    //   {
    //     ?item <hasTitle> ?title .
    //     ?item <hasAuthor> ?author .
    //     ?item <hasYear> ?year .
    //     FILTER (?year >= 2000)
    //   }
    //   UNION
    //   {
    //     ?item <hasTitle> ?title .
    //     ?item <hasDirector> ?director .
    //     ?item <hasYear> ?year .
    //     FILTER (?year >= 2000)
    //   }
    // }

    // Build first pattern (books with filter)
    auto pattern3 = std::make_shared<QueryPattern>();
    BasicGraphPattern bgp3;
    bgp3.triples = {
        TriplePattern(Var("item"), Iri("http://schema.org/title"), Var("title")),
        TriplePattern(Var("item"), Iri("http://schema.org/author"), Var("author")),
        TriplePattern(Var("item"), Iri("http://schema.org/year"), Var("year"))
    };
    pattern3->bgp = bgp3;
    pattern3->filters = {
        FilterClause(expr::GreaterThanEqual(
            expr::Var("year"),
            expr::Lit("2000")
        ))
    };

    // Build second pattern (movies with filter)
    auto pattern4 = std::make_shared<QueryPattern>();
    BasicGraphPattern bgp4;
    bgp4.triples = {
        TriplePattern(Var("item"), Iri("http://schema.org/title"), Var("title")),
        TriplePattern(Var("item"), Iri("http://schema.org/director"), Var("director")),
        TriplePattern(Var("item"), Iri("http://schema.org/year"), Var("year"))
    };
    pattern4->bgp = bgp4;
    pattern4->filters = {
        FilterClause(expr::GreaterThanEqual(
            expr::Var("year"),
            expr::Lit("2000")
        ))
    };

    // Create UNION pattern
    UnionPattern union2({pattern3, pattern4});

    // Build query
    SelectQuery query2;
    query2.select.variables = {Variable("item"), Variable("title"), Variable("year")};
    query2.where.unions = {union2};

    std::cout << "Query: SELECT ?item ?title ?year WHERE { { books year>=2000 } UNION { movies year>=2000 } }" << std::endl;

    auto result2 = engine.ExecuteSelect(query2);
    if (!result2.ok()) {
        std::cerr << "Query 2 failed: " << result2.status().ToString() << std::endl;
    } else {
        std::cout << "\nResults (recent works):\n";
        std::cout << result2.ValueOrDie()->ToString() << std::endl;
    }

    // ========================================================================
    // Example 3: UNION with different schemas (demonstrates schema unification)
    // ========================================================================
    std::cout << "\n=== Example 3: UNION with different variables (schema unification) ===" << std::endl;

    // SPARQL equivalent:
    // SELECT ?item ?title ?creator WHERE {
    //   { ?item <hasTitle> ?title . ?item <hasAuthor> ?creator }
    //   UNION
    //   { ?item <hasTitle> ?title . ?item <hasDirector> ?creator }
    // }

    // Build first pattern (books - author as creator)
    auto pattern5 = std::make_shared<QueryPattern>();
    BasicGraphPattern bgp5;
    bgp5.triples = {
        TriplePattern(Var("item"), Iri("http://schema.org/title"), Var("title")),
        TriplePattern(Var("item"), Iri("http://schema.org/author"), Var("creator"))
    };
    pattern5->bgp = bgp5;

    // Build second pattern (movies - director as creator)
    auto pattern6 = std::make_shared<QueryPattern>();
    BasicGraphPattern bgp6;
    bgp6.triples = {
        TriplePattern(Var("item"), Iri("http://schema.org/title"), Var("title")),
        TriplePattern(Var("item"), Iri("http://schema.org/director"), Var("creator"))
    };
    pattern6->bgp = bgp6;

    // Create UNION pattern
    UnionPattern union3({pattern5, pattern6});

    // Build query
    SelectQuery query3;
    query3.select.variables = {Variable("item"), Variable("title"), Variable("creator")};
    query3.where.unions = {union3};

    std::cout << "Query: SELECT ?item ?title ?creator WHERE { { books } UNION { movies } }" << std::endl;

    auto result3 = engine.ExecuteSelect(query3);
    if (!result3.ok()) {
        std::cerr << "Query 3 failed: " << result3.status().ToString() << std::endl;
    } else {
        std::cout << "\nResults (all creators):\n";
        std::cout << result3.ValueOrDie()->ToString() << std::endl;
    }

    // ========================================================================
    // Example 4: EXPLAIN query plan with UNION
    // ========================================================================
    std::cout << "\n=== Example 4: EXPLAIN UNION query ===" << std::endl;

    std::cout << engine.Explain(query1) << std::endl;

    // ========================================================================
    // Example 5: EXPLAIN ANALYZE (with statistics)
    // ========================================================================
    std::cout << "\n=== Example 5: EXPLAIN ANALYZE ===" << std::endl;

    auto analyze_result = engine.ExplainAnalyze(query1);
    if (analyze_result.ok()) {
        std::cout << analyze_result.ValueOrDie() << std::endl;
    }

    std::cout << "\n=== All UNION examples completed! ===" << std::endl;

    return 0;
}
