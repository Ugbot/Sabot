/**
 * Olympic Dataset Property Path Benchmark (Multi-Agent)
 *
 * Tests SPARQL 1.1 property paths on the full Olympic dataset (~1.8M triples).
 * Uses multiple threads to simulate distributed agent execution.
 *
 * Dataset: QLever Olympics dataset
 * - 1.8M triples
 * - 12,000 athletes
 * - 1,000 events
 * - Olympic Games 1896-2014
 */

#include <iostream>
#include <chrono>
#include <iomanip>
#include <cstdlib>
#include <thread>
#include <vector>
#include <atomic>
#include <mutex>
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

// Shared state for multi-threaded benchmark
struct BenchmarkContext {
    std::shared_ptr<TripleStore> store;
    std::shared_ptr<Vocabulary> vocab;
    std::atomic<int> queries_completed{0};
    std::atomic<int64_t> total_results{0};
    std::atomic<double> total_query_time{0.0};
    std::mutex cout_mutex;
};

// Simple N-Triples line parser (inline for pipe input)
arrow::Result<std::tuple<Term, Term, Term>> parse_ntriple_line(const std::string& line) {
    // Skip comments and empty lines
    if (line.empty() || line[0] == '#') {
        return arrow::Status::Invalid("Comment or empty line");
    }

    // Very basic parser - splits on <> and ".
    // Example: <subj> <pred> <obj> .
    //          <subj> <pred> "literal" .

    size_t pos = 0;
    auto skip_ws = [&]() {
        while (pos < line.size() && (line[pos] == ' ' || line[pos] == '\t')) pos++;
    };

    // Parse subject (IRI or blank node)
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

    // Parse predicate (IRI)
    skip_ws();
    if (line[pos] != '<') return arrow::Status::Invalid("Predicate must be IRI");
    size_t end = line.find('>', pos);
    if (end == std::string::npos) return arrow::Status::Invalid("Unclosed IRI");
    Term predicate = Term::IRI(line.substr(pos + 1, end - pos - 1));
    pos = end + 1;

    // Parse object (IRI or literal)
    skip_ws();
    Term object;
    if (line[pos] == '<') {
        end = line.find('>', pos);
        if (end == std::string::npos) return arrow::Status::Invalid("Unclosed IRI");
        object = Term::IRI(line.substr(pos + 1, end - pos - 1));
        pos = end + 1;
    } else if (line[pos] == '"') {
        // Parse literal (simplified - doesn't handle escapes properly)
        end = line.find('"', pos + 1);
        if (end == std::string::npos) return arrow::Status::Invalid("Unclosed literal");
        std::string lit = line.substr(pos + 1, end - pos - 1);
        pos = end + 1;

        // Check for language tag or datatype
        skip_ws();
        if (pos < line.size() && line[pos] == '@') {
            // Language tag
            pos++;
            size_t lang_end = pos;
            while (lang_end < line.size() && (isalnum(line[lang_end]) || line[lang_end] == '-')) lang_end++;
            std::string lang = line.substr(pos, lang_end - pos);
            object = Term::Literal(lit, lang);
            pos = lang_end;
        } else if (pos < line.size() && line[pos] == '^' && pos + 1 < line.size() && line[pos+1] == '^') {
            // Datatype
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

// Load Olympics dataset from N-Triples file
bool load_olympics_data(std::shared_ptr<TripleStore>& store,
                        std::shared_ptr<Vocabulary>& vocab,
                        const std::string& data_path) {
    std::cout << "Loading Olympics dataset from: " << data_path << "\n";
    std::cout << "Decompressing and parsing N-Triples...\n";

    // Decompress and parse using system command
    std::string cmd = "xzcat " + data_path + " 2>/dev/null";
    FILE* pipe = popen(cmd.c_str(), "r");
    if (!pipe) {
        std::cerr << "Failed to decompress dataset\n";
        return false;
    }

    Timer load_timer;
    int64_t triples_loaded = 0;
    int64_t lines_parsed = 0;
    char buffer[8192];

    // Batch accumulator
    std::vector<Triple> batch;
    batch.reserve(10000);
    const size_t BATCH_SIZE = 10000;

    while (fgets(buffer, sizeof(buffer), pipe)) {
        lines_parsed++;
        std::string line(buffer);

        // Parse N-Triple line
        auto parse_result = parse_ntriple_line(line);
        if (!parse_result.ok()) {
            // Skip parse errors (malformed triples, comments, blank lines)
            continue;
        }

        auto [subj_term, pred_term, obj_term] = *parse_result;

        // Add to vocabulary
        auto s_result = vocab->AddTerm(subj_term);
        auto p_result = vocab->AddTerm(pred_term);
        auto o_result = vocab->AddTerm(obj_term);

        if (!s_result.ok() || !p_result.ok() || !o_result.ok()) {
            std::cerr << "Failed to add terms to vocabulary\n";
            continue;
        }

        // Create triple
        Triple triple;
        triple.subject = *s_result;
        triple.predicate = *p_result;
        triple.object = *o_result;

        // Add to batch
        batch.push_back(triple);

        // Insert batch when full
        if (batch.size() >= BATCH_SIZE) {
            auto status = store->InsertTriples(batch);
            if (!status.ok()) {
                std::cerr << "Failed to insert batch: " << status.ToString() << "\n";
                return false;
            }
            triples_loaded += batch.size();
            batch.clear();

            // Progress report every 100K triples
            if (triples_loaded % 100000 == 0) {
                double elapsed = load_timer.elapsed_sec();
                double rate = triples_loaded / elapsed;
                std::cout << "  Loaded " << triples_loaded << " triples "
                          << "(" << std::fixed << std::setprecision(0) << rate << " triples/sec)\n";
            }
        }
    }

    // Insert remaining triples
    if (!batch.empty()) {
        auto status = store->InsertTriples(batch);
        if (!status.ok()) {
            std::cerr << "Failed to insert final batch: " << status.ToString() << "\n";
            return false;
        }
        triples_loaded += batch.size();
    }

    pclose(pipe);

    // Flush to disk
    std::cout << "Flushing to disk...\n";
    auto flush_status = store->Flush();
    if (!flush_status.ok()) {
        std::cerr << "Failed to flush: " << flush_status.ToString() << "\n";
        return false;
    }

    double load_time = load_timer.elapsed_sec();
    double load_rate = triples_loaded / load_time;

    std::cout << "\n";
    std::cout << "Load complete:\n";
    std::cout << "  Triples loaded: " << triples_loaded << "\n";
    std::cout << "  Lines parsed: " << lines_parsed << "\n";
    std::cout << "  Load time: " << std::fixed << std::setprecision(2) << load_time << " sec\n";
    std::cout << "  Load rate: " << std::fixed << std::setprecision(0) << load_rate << " triples/sec\n";

    return triples_loaded > 0;
}

// Query worker function (simulates an agent)
void query_worker(BenchmarkContext* ctx, int agent_id, const std::string& query_name,
                  const std::string& sparql_query) {
    {
        std::lock_guard<std::mutex> lock(ctx->cout_mutex);
        std::cout << "[Agent " << agent_id << "] Starting query: " << query_name << "\n";
    }

    QueryEngine engine(ctx->store, ctx->vocab);

    Timer query_timer;

    // Parse query
    auto parse_result = ParseSPARQL(sparql_query);
    if (!parse_result.ok()) {
        std::lock_guard<std::mutex> lock(ctx->cout_mutex);
        std::cerr << "[Agent " << agent_id << "] Parse failed: " << parse_result.status().ToString() << "\n";
        return;
    }

    auto parsed_query = *parse_result;
    auto select_query = std::get<SelectQuery>(parsed_query.query_body);

    // Execute query
    auto result = engine.ExecuteSelect(select_query);
    double query_time = query_timer.elapsed_sec();

    if (!result.ok()) {
        std::lock_guard<std::mutex> lock(ctx->cout_mutex);
        std::cerr << "[Agent " << agent_id << "] Query failed: " << result.status().ToString() << "\n";
        return;
    }

    int64_t num_rows = (*result)->num_rows();

    // Update shared metrics
    ctx->queries_completed++;
    ctx->total_results += num_rows;
    ctx->total_query_time += query_time;

    {
        std::lock_guard<std::mutex> lock(ctx->cout_mutex);
        std::cout << "[Agent " << agent_id << "] Completed: " << query_name << "\n";
        std::cout << "  Results: " << num_rows << " rows\n";
        std::cout << "  Time: " << std::fixed << std::setprecision(3) << query_time << " sec\n";
        std::cout << "\n";
    }
}

int main(int argc, char* argv[]) {
    std::cout << "\n";
    std::cout << "======================================================================\n";
    std::cout << "Olympic Dataset Property Path Benchmark (Multi-Agent)\n";
    std::cout << "======================================================================\n";
    std::cout << "\n";

    // Parse command-line args
    std::string data_path = "/Users/bengamble/Sabot/vendor/qlever/examples/olympics.nt.xz";
    int num_agents = 4;

    if (argc > 1) {
        data_path = argv[1];
    }
    if (argc > 2) {
        num_agents = std::atoi(argv[2]);
    }

    std::cout << "Configuration:\n";
    std::cout << "  Dataset: " << data_path << "\n";
    std::cout << "  Agents: " << num_agents << "\n";
    std::cout << "\n";

    // Setup storage
    std::cout << "Phase 1: Storage Setup\n";
    std::cout << "----------------------\n";

    std::string test_path = "/tmp/olympics_property_paths_bench";
    std::system(("rm -rf " + test_path).c_str());

    auto store_result = CreateTripleStore(test_path);
    if (!store_result.ok()) {
        std::cerr << "Failed to create store: " << store_result.status().ToString() << "\n";
        return 1;
    }
    auto store = *store_result;

    auto vocab_result = CreateVocabulary(test_path);
    if (!vocab_result.ok()) {
        std::cerr << "Failed to create vocabulary: " << vocab_result.status().ToString() << "\n";
        return 1;
    }
    auto vocab = *vocab_result;

    std::cout << "Storage created\n\n";

    // Load dataset
    std::cout << "Phase 2: Load Olympics Dataset (~1.8M triples)\n";
    std::cout << "----------------------------------------------\n";

    if (!load_olympics_data(store, vocab, data_path)) {
        std::cerr << "Failed to load dataset\n";
        return 1;
    }

    // Setup benchmark context
    BenchmarkContext ctx;
    ctx.store = store;
    ctx.vocab = vocab;

    // Define property path queries
    struct Query {
        std::string name;
        std::string sparql;
    };

    std::vector<Query> queries = {
        {
            "Transitive hierarchy p+",
            R"(
                PREFIX wd: <http://www.wikidata.org/entity/>
                PREFIX wdt: <http://www.wikidata.org/prop/direct/>
                SELECT ?entity WHERE {
                    wd:Q5389 wdt:P279+ ?entity .
                }
                LIMIT 100
            )"
        },
        {
            "Transitive reflexive p*",
            R"(
                PREFIX wd: <http://www.wikidata.org/entity/>
                PREFIX wdt: <http://www.wikidata.org/prop/direct/>
                SELECT ?entity WHERE {
                    wd:Q5389 wdt:P279* ?entity .
                }
                LIMIT 100
            )"
        },
        {
            "Inverse path ^p",
            R"(
                PREFIX wd: <http://www.wikidata.org/entity/>
                PREFIX wdt: <http://www.wikidata.org/prop/direct/>
                SELECT ?country WHERE {
                    ?person ^wdt:P27 ?country .
                }
                LIMIT 100
            )"
        },
        {
            "Sequence path p/q",
            R"(
                PREFIX wd: <http://www.wikidata.org/entity/>
                PREFIX wdt: <http://www.wikidata.org/prop/direct/>
                PREFIX p: <http://www.wikidata.org/prop/>
                SELECT ?value WHERE {
                    ?athlete wdt:P106/p:P1532 ?value .
                }
                LIMIT 100
            )"
        },
        {
            "Alternative path p|q",
            R"(
                PREFIX wd: <http://www.wikidata.org/entity/>
                PREFIX wdt: <http://www.wikidata.org/prop/direct/>
                PREFIX p: <http://www.wikidata.org/prop/>
                SELECT ?related WHERE {
                    ?person (wdt:P166|wdt:P1344) ?related .
                }
                LIMIT 100
            )"
        },
        {
            "Bounded path p{1,3}",
            R"(
                PREFIX wd: <http://www.wikidata.org/entity/>
                PREFIX wdt: <http://www.wikidata.org/prop/direct/>
                SELECT ?entity WHERE {
                    wd:Q5389 wdt:P279{1,3} ?entity .
                }
                LIMIT 100
            )"
        },
        {
            "Complex: transitive + filter",
            R"(
                PREFIX wd: <http://www.wikidata.org/entity/>
                PREFIX wdt: <http://www.wikidata.org/prop/direct/>
                PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
                SELECT ?entity ?label WHERE {
                    wd:Q5389 wdt:P279+ ?entity .
                    ?entity rdfs:label ?label .
                }
                LIMIT 50
            )"
        },
        {
            "Complex: inverse transitive",
            R"(
                PREFIX wd: <http://www.wikidata.org/entity/>
                PREFIX wdt: <http://www.wikidata.org/prop/direct/>
                SELECT ?subclass WHERE {
                    ?subclass ^wdt:P279+ wd:Q5389 .
                }
                LIMIT 100
            )"
        }
    };

    // Run queries in parallel using multiple agents
    std::cout << "Phase 3: Execute Property Path Queries (Multi-Agent)\n";
    std::cout << "====================================================\n\n";

    Timer total_timer;

    std::vector<std::thread> agents;
    agents.reserve(queries.size());

    // Launch agent threads
    for (size_t i = 0; i < queries.size(); i++) {
        int agent_id = i % num_agents;
        agents.emplace_back(query_worker, &ctx, agent_id, queries[i].name, queries[i].sparql);

        // Stagger launches slightly
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }

    // Wait for all agents to complete
    for (auto& agent : agents) {
        agent.join();
    }

    double total_time = total_timer.elapsed_sec();

    // Print summary
    std::cout << "======================================================================\n";
    std::cout << "Benchmark Summary\n";
    std::cout << "======================================================================\n";
    std::cout << "\n";
    std::cout << "Queries completed: " << ctx.queries_completed << " / " << queries.size() << "\n";
    std::cout << "Total results: " << ctx.total_results.load() << " rows\n";
    std::cout << "Total query time: " << std::fixed << std::setprecision(2)
              << ctx.total_query_time.load() << " sec\n";
    std::cout << "Wall clock time: " << std::fixed << std::setprecision(2) << total_time << " sec\n";
    std::cout << "Speedup: " << std::fixed << std::setprecision(2)
              << (ctx.total_query_time.load() / total_time) << "x\n";
    std::cout << "\n";
    std::cout << "Average query time: " << std::fixed << std::setprecision(3)
              << (ctx.total_query_time.load() / ctx.queries_completed) << " sec\n";
    std::cout << "Queries per second: " << std::fixed << std::setprecision(2)
              << (ctx.queries_completed.load() / total_time) << "\n";
    std::cout << "\n";

    if (ctx.queries_completed == queries.size()) {
        std::cout << "✓ All queries completed successfully!\n";
        return 0;
    } else {
        std::cout << "⚠ Some queries failed\n";
        return 1;
    }
}
