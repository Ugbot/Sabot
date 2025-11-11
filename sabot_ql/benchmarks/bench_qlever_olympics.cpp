/**
 * QLever Olympics Benchmark
 *
 * Runs the actual QLever example queries on the Olympic dataset.
 * Tests real-world SPARQL queries including property paths and aggregations.
 *
 * Based on: https://github.com/ad-freiburg/qlever/blob/master/docs/quickstart.md
 */

#include <iostream>
#include <chrono>
#include <iomanip>
#include <cstdlib>
#include <sabot_ql/storage/triple_store.h>
#include <sabot_ql/storage/vocabulary.h>
#include <sabot_ql/sparql/query_engine.h>
#include <sabot_ql/sparql/parser.h>

using namespace sabot_ql;
using namespace sabot_ql::sparql;
using namespace std::chrono;

// Timing helper
struct Timer {
    steady_clock::time_point start;
    Timer() : start(steady_clock::now()) {}
    double elapsed_sec() const {
        auto end = steady_clock::now();
        return duration_cast<duration<double>>(end - start).count();
    }
};

// Simple N-Triples parser (inline)
arrow::Result<std::tuple<Term, Term, Term>> parse_ntriple_line(const std::string& line) {
    if (line.empty() || line[0] == '#') {
        return arrow::Status::Invalid("Comment or empty line");
    }

    size_t pos = 0;
    auto skip_ws = [&]() {
        while (pos < line.size() && (line[pos] == ' ' || line[pos] == '\t')) pos++;
    };

    // Parse subject
    skip_ws();
    if (pos >= line.size()) return arrow::Status::Invalid("Empty line");

    Term subject;
    if (line[pos] == '<') {
        size_t end = line.find('>', pos);
        if (end == std::string::npos) return arrow::Status::Invalid("Unclosed IRI");
        subject = Term::IRI(line.substr(pos + 1, end - pos - 1));
        pos = end + 1;
    } else {
        return arrow::Status::Invalid("Subject must be IRI");
    }

    // Parse predicate
    skip_ws();
    if (line[pos] != '<') return arrow::Status::Invalid("Predicate must be IRI");
    size_t end = line.find('>', pos);
    if (end == std::string::npos) return arrow::Status::Invalid("Unclosed IRI");
    Term predicate = Term::IRI(line.substr(pos + 1, end - pos - 1));
    pos = end + 1;

    // Parse object
    skip_ws();
    Term object;
    if (line[pos] == '<') {
        end = line.find('>', pos);
        if (end == std::string::npos) return arrow::Status::Invalid("Unclosed IRI");
        object = Term::IRI(line.substr(pos + 1, end - pos - 1));
        pos = end + 1;
    } else if (line[pos] == '"') {
        end = line.find('"', pos + 1);
        if (end == std::string::npos) return arrow::Status::Invalid("Unclosed literal");
        std::string lit = line.substr(pos + 1, end - pos - 1);
        pos = end + 1;

        skip_ws();
        if (pos < line.size() && line[pos] == '@') {
            pos++;
            size_t lang_end = pos;
            while (lang_end < line.size() && (isalnum(line[lang_end]) || line[lang_end] == '-')) lang_end++;
            std::string lang = line.substr(pos, lang_end - pos);
            object = Term::Literal(lit, lang);
            pos = lang_end;
        } else if (pos < line.size() && line[pos] == '^' && pos + 1 < line.size() && line[pos+1] == '^') {
            pos += 2;
            skip_ws();
            if (line[pos] != '<') return arrow::Status::Invalid("Datatype must be IRI");
            end = line.find('>', pos);
            if (end == std::string::npos) return arrow::Status::Invalid("Unclosed datatype IRI");
            std::string datatype = line.substr(pos + 1, end - pos - 1);
            object = Term::Literal(lit, "", datatype);
            pos = end + 1;
        } else {
            object = Term::Literal(lit);
        }
    } else {
        return arrow::Status::Invalid("Object must be IRI or literal");
    }

    return std::make_tuple(subject, predicate, object);
}

// Load Olympics dataset
bool load_olympics_data(std::shared_ptr<TripleStore>& store,
                        std::shared_ptr<Vocabulary>& vocab,
                        const std::string& data_path) {
    std::cout << "Loading Olympics dataset from: " << data_path << "\n";

    std::string cmd = "xzcat " + data_path + " 2>/dev/null";
    FILE* pipe = popen(cmd.c_str(), "r");
    if (!pipe) {
        std::cerr << "Failed to decompress dataset\n";
        return false;
    }

    Timer load_timer;
    int64_t triples_loaded = 0;
    char buffer[8192];

    std::vector<Triple> batch;
    batch.reserve(10000);
    const size_t BATCH_SIZE = 10000;

    while (fgets(buffer, sizeof(buffer), pipe)) {
        auto parse_result = parse_ntriple_line(buffer);
        if (!parse_result.ok()) continue;

        auto [subj_term, pred_term, obj_term] = *parse_result;

        auto s_result = vocab->AddTerm(subj_term);
        auto p_result = vocab->AddTerm(pred_term);
        auto o_result = vocab->AddTerm(obj_term);

        if (!s_result.ok() || !p_result.ok() || !o_result.ok()) continue;

        Triple triple;
        triple.subject = *s_result;
        triple.predicate = *p_result;
        triple.object = *o_result;

        batch.push_back(triple);

        if (batch.size() >= BATCH_SIZE) {
            auto status = store->InsertTriples(batch);
            if (!status.ok()) {
                std::cerr << "Failed to insert batch\n";
                return false;
            }
            triples_loaded += batch.size();
            batch.clear();

            if (triples_loaded % 100000 == 0) {
                double elapsed = load_timer.elapsed_sec();
                double rate = triples_loaded / elapsed;
                std::cout << "  Loaded " << triples_loaded << " triples "
                          << "(" << std::fixed << std::setprecision(0) << rate << " triples/sec)\n";
            }
        }
    }

    if (!batch.empty()) {
        auto status = store->InsertTriples(batch);
        if (!status.ok()) return false;
        triples_loaded += batch.size();
    }

    pclose(pipe);

    auto flush_status = store->Flush();
    if (!flush_status.ok()) return false;

    double load_time = load_timer.elapsed_sec();
    double load_rate = triples_loaded / load_time;

    std::cout << "\nLoad complete:\n";
    std::cout << "  Triples loaded: " << triples_loaded << "\n";
    std::cout << "  Load time: " << std::fixed << std::setprecision(2) << load_time << " sec\n";
    std::cout << "  Load rate: " << std::fixed << std::setprecision(0) << load_rate << " triples/sec\n\n";

    return triples_loaded > 0;
}

// Run a SPARQL query and time it
void run_query(QueryEngine& engine, const std::string& name, const std::string& sparql) {
    std::cout << "Query: " << name << "\n";
    std::cout << "----------------------------------------------------------------------\n";

    Timer parse_timer;
    auto parse_result = ParseSPARQL(sparql);
    double parse_time = parse_timer.elapsed_sec();

    if (!parse_result.ok()) {
        std::cerr << "  Parse failed: " << parse_result.status().ToString() << "\n\n";
        return;
    }

    auto parsed_query = *parse_result;
    auto select_query = std::get<SelectQuery>(parsed_query.query_body);

    Timer exec_timer;
    auto result = engine.ExecuteSelect(select_query);
    double exec_time = exec_timer.elapsed_sec();

    if (!result.ok()) {
        std::cerr << "  Execution failed: " << result.status().ToString() << "\n\n";
        return;
    }

    auto table = *result;
    int64_t num_rows = table->num_rows();

    std::cout << "  Parse time: " << std::fixed << std::setprecision(3) << parse_time * 1000 << " ms\n";
    std::cout << "  Execution time: " << std::fixed << std::setprecision(3) << exec_time * 1000 << " ms\n";
    std::cout << "  Results: " << num_rows << " rows\n";

    // Show first few results
    if (num_rows > 0) {
        std::cout << "  Sample results:\n";
        std::cout << table->ToString() << "\n";
    }

    std::cout << "\n";
}

int main(int argc, char* argv[]) {
    std::cout << "\n";
    std::cout << "======================================================================\n";
    std::cout << "QLever Olympics Benchmark\n";
    std::cout << "======================================================================\n";
    std::cout << "\n";

    std::string data_path = "/Users/bengamble/Sabot/vendor/qlever/examples/olympics.nt.xz";
    if (argc > 1) {
        data_path = argv[1];
    }

    // Setup storage
    std::cout << "Phase 1: Storage Setup\n";
    std::cout << "----------------------\n";

    std::string test_path = "/tmp/qlever_olympics_bench";
    std::system(("rm -rf " + test_path).c_str());

    auto store_result = CreateTripleStore(test_path);
    if (!store_result.ok()) {
        std::cerr << "Failed to create store\n";
        return 1;
    }
    auto store = *store_result;

    auto vocab_result = CreateVocabulary(test_path);
    if (!vocab_result.ok()) {
        std::cerr << "Failed to create vocabulary\n";
        return 1;
    }
    auto vocab = *vocab_result;

    std::cout << "Storage created\n\n";

    // Load dataset
    std::cout << "Phase 2: Load Olympics Dataset\n";
    std::cout << "-------------------------------\n";

    if (!load_olympics_data(store, vocab, data_path)) {
        std::cerr << "Failed to load dataset\n";
        return 1;
    }

    // Run QLever benchmark queries
    std::cout << "Phase 3: Execute QLever Benchmark Queries\n";
    std::cout << "==========================================\n\n";

    QueryEngine engine(store, vocab);

    // Query 1: Top 10 athletes with most gold medals (from QLever docs)
    run_query(engine, "Top 10 athletes with most gold medals",
        R"(
        PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
        PREFIX medal: <http://wallscope.co.uk/resource/olympics/medal/>
        PREFIX olympics: <http://wallscope.co.uk/ontology/olympics/>
        SELECT ?athlete (COUNT(?medal) as ?count_medal)
        WHERE {
            ?medal olympics:medal medal:Gold .
            ?medal olympics:athlete/rdfs:label ?athlete .
        }
        GROUP BY ?athlete
        ORDER BY DESC(?count_medal)
        LIMIT 10
        )");

    // Query 2: Property path - athletes connected through medals
    run_query(engine, "Athletes connected through medals (property path)",
        R"(
        PREFIX olympics: <http://wallscope.co.uk/ontology/olympics/>
        PREFIX medal: <http://wallscope.co.uk/resource/olympics/medal/>
        SELECT ?athlete1 ?athlete2
        WHERE {
            ?medal1 olympics:athlete ?athlete1 .
            ?medal1 olympics:event ?event .
            ?medal2 olympics:event ?event .
            ?medal2 olympics:athlete ?athlete2 .
            FILTER(?athlete1 != ?athlete2)
        }
        LIMIT 100
        )");

    // Query 3: Count athletes by country
    run_query(engine, "Count athletes by country",
        R"(
        PREFIX olympics: <http://wallscope.co.uk/ontology/olympics/>
        PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
        SELECT ?country (COUNT(DISTINCT ?athlete) as ?num_athletes)
        WHERE {
            ?athlete olympics:country ?country_id .
            ?country_id rdfs:label ?country .
        }
        GROUP BY ?country
        ORDER BY DESC(?num_athletes)
        LIMIT 20
        )");

    // Query 4: All medals for a specific event
    run_query(engine, "All medals for 100m sprint events",
        R"(
        PREFIX olympics: <http://wallscope.co.uk/ontology/olympics/>
        PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
        SELECT ?event ?athlete ?medal_type
        WHERE {
            ?medal olympics:event ?event_id .
            ?event_id rdfs:label ?event .
            ?medal olympics:athlete ?athlete_id .
            ?athlete_id rdfs:label ?athlete .
            ?medal olympics:medal ?medal_type .
            FILTER(CONTAINS(LCASE(STR(?event)), "100"))
        }
        LIMIT 50
        )");

    std::cout << "======================================================================\n";
    std::cout << "Benchmark Complete!\n";
    std::cout << "======================================================================\n";

    return 0;
}
