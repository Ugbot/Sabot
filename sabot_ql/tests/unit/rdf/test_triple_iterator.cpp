/**
 * Test 3.7: Iterator-Based Triple Retrieval (DEBUGGING)
 *
 * This test creates an iterator directly and walks through the storage layer
 * with extensive debugging output to identify why scans return 0 rows.
 */

#include <sabot_ql/storage/triple_store.h>
#include <sabot_ql/storage/vocabulary.h>
#include <marble/db.h>
#include <iostream>
#include <cstdlib>

using namespace sabot_ql;

void Fail(const std::string& msg) {
    std::cerr << "\n❌ FAILED: " << msg << "\n";
    exit(1);
}

void Pass(const std::string& msg) {
    std::cout << "✅ " << msg << "\n";
}

void Debug(const std::string& msg) {
    std::cout << "[DEBUG] " << msg << "\n";
}

int main() {
    std::cout << "========================================\n";
    std::cout << "Test 3.7: Iterator Debug Test\n";
    std::cout << "========================================\n\n";

    // Clean up
    std::string test_path = "/tmp/test_triple_iterator";
    std::system(("rm -rf " + test_path).c_str());

    std::cout << "=== Phase 1: Setup ===\n\n";

    Debug("Creating triple store...");
    auto store_result = CreateTripleStore(test_path);
    if (!store_result.ok()) {
        Fail("Triple store creation failed: " + store_result.status().ToString());
    }
    auto store = *store_result;
    Pass("Triple store created");

    Debug("Creating vocabulary...");
    auto vocab_result = CreateVocabulary(test_path);
    if (!vocab_result.ok()) {
        Fail("Vocabulary creation failed: " + vocab_result.status().ToString());
    }
    auto vocab = *vocab_result;
    Pass("Vocabulary created");

    std::cout << "\n=== Phase 2: Insert Test Data ===\n\n";

    // Add real terms to vocabulary
    Debug("Adding terms to vocabulary...");
    auto alice_id = vocab->AddTerm(Term::IRI("http://example.org/Alice"));
    auto bob_id = vocab->AddTerm(Term::IRI("http://example.org/Bob"));
    auto knows_id = vocab->AddTerm(Term::IRI("http://example.org/knows"));

    if (!alice_id.ok() || !bob_id.ok() || !knows_id.ok()) {
        Fail("Failed to add terms to vocabulary");
    }

    std::cout << "  Alice ID: " << alice_id->getBits() << "\n";
    std::cout << "  Bob ID: " << bob_id->getBits() << "\n";
    std::cout << "  knows ID: " << knows_id->getBits() << "\n";
    Pass("Terms added to vocabulary");

    // Insert triple
    Debug("Inserting triple (Alice, knows, Bob)...");
    std::vector<Triple> triples = {
        Triple{*alice_id, *knows_id, *bob_id}
    };

    auto insert_status = store->InsertTriples(triples);
    if (!insert_status.ok()) {
        Fail("Insert failed: " + insert_status.ToString());
    }
    Pass("Triple inserted");

    std::cout << "  Inserted: (" << alice_id->getBits() << ", "
              << knows_id->getBits() << ", " << bob_id->getBits() << ")\n";

    // Flush
    Debug("Flushing to disk...");
    auto flush_status = store->Flush();
    if (!flush_status.ok()) {
        Fail("Flush failed: " + flush_status.ToString());
    }
    Pass("Flush completed");

    std::cout << "\n=== Phase 3: Test Scans ===\n\n";

    // Test 1: Full scan
    Debug("Test 1: Full scan (?, ?, ?)");
    TriplePattern full_pattern{std::nullopt, std::nullopt, std::nullopt};
    auto full_result = store->ScanPattern(full_pattern);
    if (!full_result.ok()) {
        Fail("Full scan failed: " + full_result.status().ToString());
    }
    auto full_table = *full_result;
    std::cout << "  Result: " << full_table->num_rows() << " rows, "
              << full_table->num_columns() << " columns\n";

    if (full_table->num_rows() > 0) {
        Pass("Full scan found data!");
        std::cout << "  Schema: " << full_table->schema()->ToString() << "\n";
        std::cout << "  Data:\n" << full_table->ToString() << "\n";
    } else {
        std::cout << "  ⚠️  Full scan returned 0 rows\n";
    }

    // Test 2: Subject-bound scan
    Debug("Test 2: Subject-bound scan (Alice, ?, ?)");
    TriplePattern subject_pattern{alice_id->getBits(), std::nullopt, std::nullopt};
    auto subject_result = store->ScanPattern(subject_pattern);
    if (!subject_result.ok()) {
        Fail("Subject scan failed: " + subject_result.status().ToString());
    }
    auto subject_table = *subject_result;
    std::cout << "  Result: " << subject_table->num_rows() << " rows, "
              << subject_table->num_columns() << " columns\n";

    if (subject_table->num_rows() > 0) {
        Pass("Subject-bound scan found data!");
    } else {
        std::cout << "  ⚠️  Subject scan returned 0 rows\n";
    }

    // Test 3: Predicate-bound scan
    Debug("Test 3: Predicate-bound scan (?, knows, ?)");
    TriplePattern predicate_pattern{std::nullopt, knows_id->getBits(), std::nullopt};
    auto predicate_result = store->ScanPattern(predicate_pattern);
    if (!predicate_result.ok()) {
        Fail("Predicate scan failed: " + predicate_result.status().ToString());
    }
    auto predicate_table = *predicate_result;
    std::cout << "  Result: " << predicate_table->num_rows() << " rows, "
              << predicate_table->num_columns() << " columns\n";

    if (predicate_table->num_rows() > 0) {
        Pass("Predicate-bound scan found data!");
    } else {
        std::cout << "  ⚠️  Predicate scan returned 0 rows\n";
    }

    std::cout << "\n=== Phase 4: Statistics ===\n\n";

    Debug("Checking triple store statistics...");
    size_t total = store->TotalTriples();
    std::cout << "  Total triples reported: " << total << "\n";

    if (total == 0) {
        std::cout << "  ⚠️  Triple store reports 0 triples!\n";
        std::cout << "  This means InsertTriples() did not update statistics.\n";
    } else {
        Pass("Triple store statistics updated");
    }

    // Cardinality estimates
    Debug("Testing cardinality estimation...");
    auto card_result = store->EstimateCardinality(full_pattern);
    if (card_result.ok()) {
        std::cout << "  Estimated cardinality (full scan): " << *card_result << "\n";
    }

    std::cout << "\n=== Summary ===\n\n";

    bool all_scans_empty = (full_table->num_rows() == 0 &&
                            subject_table->num_rows() == 0 &&
                            predicate_table->num_rows() == 0);

    if (all_scans_empty) {
        std::cout << "❌ CRITICAL: All scans returned 0 rows\n\n";
        std::cout << "Diagnostic information:\n";
        std::cout << "  - Insert reported success: YES\n";
        std::cout << "  - Flush reported success: YES\n";
        std::cout << "  - Scan operations succeeded: YES\n";
        std::cout << "  - Data returned from scans: NO\n";
        std::cout << "  - Total triples stat: " << total << "\n";
        std::cout << "\nThis indicates one of:\n";
        std::cout << "  1. Data is written but Iterator doesn't find it\n";
        std::cout << "  2. Flush() doesn't actually persist to MarbleDB\n";
        std::cout << "  3. KeyRange construction is preventing matches\n";
        std::cout << "  4. InsertBatch() to MarbleDB is failing silently\n";
        std::cout << "\nRecommended next steps:\n";
        std::cout << "  1. Add debug logging to triple_store_impl.cpp ScanIndex()\n";
        std::cout << "  2. Verify MarbleDB::InsertBatch() actually works\n";
        std::cout << "  3. Test MarbleDB Iterator API independently\n";
        std::cout << "  4. Check if column families were created correctly\n";
        exit(1);
    } else {
        std::cout << "✅ SUCCESS: Scans are retrieving data!\n\n";
        std::cout << "Results:\n";
        std::cout << "  - Full scan: " << full_table->num_rows() << " rows\n";
        std::cout << "  - Subject scan: " << subject_table->num_rows() << " rows\n";
        std::cout << "  - Predicate scan: " << predicate_table->num_rows() << " rows\n";
    }

    std::cout << "\n========================================\n";
    std::cout << "Test complete\n";
    std::cout << "========================================\n";

    return (all_scans_empty ? 1 : 0);
}
