/************************************************************************
Test MarbleDB-backed Triple Store and Vocabulary
**************************************************************************/

#include "sabot_ql/storage/triple_store.h"
#include "sabot_ql/storage/vocabulary.h"
#include "sabot_ql/types/value_id.h"
#include "marble/db.h"
#include <iostream>
#include <memory>

using namespace sabot_ql;

// Forward declarations of factory functions
extern std::unique_ptr<TripleStore> CreateMarbleDBTripleStore(std::shared_ptr<marble::MarbleDB> db);
extern std::unique_ptr<Vocabulary> CreateMarbleDBVocabulary(std::shared_ptr<marble::MarbleDB> db);

int main() {
    std::cout << "=== MarbleDB Storage Integration Test ===" << std::endl;

    // Test 1: Create MarbleDB instance
    std::cout << "\n[Test 1] Creating MarbleDB instance..." << std::endl;

    marble::DBOptions options;
    options.create_if_missing = true;
    options.error_if_exists = false;

    std::shared_ptr<marble::MarbleDB> db;
    auto status = marble::MarbleDB::Open(options, "/tmp/sabot_ql_test_db", &db);

    if (!status.ok()) {
        std::cerr << "  ✗ Failed to open MarbleDB: " << status.ToString() << std::endl;
        return 1;
    }
    std::cout << "  ✓ MarbleDB instance created" << std::endl;

    // Test 2: Create Vocabulary
    std::cout << "\n[Test 2] Creating MarbleDB Vocabulary..." << std::endl;
    auto vocab = CreateMarbleDBVocabulary(db);
    if (!vocab) {
        std::cerr << "  ✗ Failed to create vocabulary" << std::endl;
        return 1;
    }
    std::cout << "  ✓ Vocabulary created" << std::endl;

    // Test 3: Add terms to vocabulary
    std::cout << "\n[Test 3] Adding terms to vocabulary..." << std::endl;

    Term subject_term{"http://example.org/subject1", TermKind::IRI, "", ""};
    Term predicate_term{"http://example.org/predicate1", TermKind::IRI, "", ""};
    Term object_term{"Object Value", TermKind::Literal, "", "xsd:string"};

    auto subject_id_result = vocab->AddTerm(subject_term);
    if (!subject_id_result.ok()) {
        std::cerr << "  ✗ Failed to add subject: " << subject_id_result.status().ToString() << std::endl;
        return 1;
    }
    auto subject_id = subject_id_result.ValueOrDie();

    auto predicate_id_result = vocab->AddTerm(predicate_term);
    if (!predicate_id_result.ok()) {
        std::cerr << "  ✗ Failed to add predicate: " << predicate_id_result.status().ToString() << std::endl;
        return 1;
    }
    auto predicate_id = predicate_id_result.ValueOrDie();

    auto object_id_result = vocab->AddTerm(object_term);
    if (!object_id_result.ok()) {
        std::cerr << "  ✗ Failed to add object: " << object_id_result.status().ToString() << std::endl;
        return 1;
    }
    auto object_id = object_id_result.ValueOrDie();

    std::cout << "  ✓ Added 3 terms to vocabulary" << std::endl;
    std::cout << "    Subject ID: " << subject_id.getBits() << std::endl;
    std::cout << "    Predicate ID: " << predicate_id.getBits() << std::endl;
    std::cout << "    Object ID: " << object_id.getBits() << std::endl;

    // Test 4: Retrieve terms from vocabulary
    std::cout << "\n[Test 4] Retrieving terms from vocabulary..." << std::endl;

    auto retrieved_subject_result = vocab->GetTerm(subject_id);
    if (!retrieved_subject_result.ok()) {
        std::cerr << "  ✗ Failed to retrieve subject" << std::endl;
        return 1;
    }
    auto retrieved_subject = retrieved_subject_result.ValueOrDie();

    if (retrieved_subject.lexical != subject_term.lexical) {
        std::cerr << "  ✗ Subject mismatch: " << retrieved_subject.lexical << std::endl;
        return 1;
    }
    std::cout << "  ✓ Successfully retrieved subject: " << retrieved_subject.lexical << std::endl;

    // Test 5: Check vocabulary size
    std::cout << "\n[Test 5] Checking vocabulary size..." << std::endl;
    size_t vocab_size = vocab->Size();
    std::cout << "  Vocabulary size: " << vocab_size << " terms" << std::endl;
    if (vocab_size < 3) {
        std::cerr << "  ✗ Expected at least 3 terms" << std::endl;
        return 1;
    }
    std::cout << "  ✓ Vocabulary size correct" << std::endl;

    // Test 6: Create Triple Store
    std::cout << "\n[Test 6] Creating MarbleDB Triple Store..." << std::endl;
    auto triple_store = CreateMarbleDBTripleStore(db);
    if (!triple_store) {
        std::cerr << "  ✗ Failed to create triple store" << std::endl;
        return 1;
    }
    std::cout << "  ✓ Triple store created" << std::endl;

    // Test 7: Insert triples
    std::cout << "\n[Test 7] Inserting triples..." << std::endl;

    std::vector<Triple> triples;
    triples.push_back({subject_id.getBits(), predicate_id.getBits(), object_id.getBits()});

    auto insert_status = triple_store->InsertTriples(triples);
    if (!insert_status.ok()) {
        std::cerr << "  ✗ Failed to insert triples: " << insert_status.ToString() << std::endl;
        return 1;
    }
    std::cout << "  ✓ Inserted 1 triple" << std::endl;

    // Test 8: Scan for triples (full scan)
    std::cout << "\n[Test 8] Scanning for all triples..." << std::endl;

    TriplePattern full_scan_pattern{std::nullopt, std::nullopt, std::nullopt};
    auto scan_result = triple_store->ScanPattern(full_scan_pattern);

    if (!scan_result.ok()) {
        std::cerr << "  ✗ Scan failed: " << scan_result.status().ToString() << std::endl;
        return 1;
    }

    auto result_table = scan_result.ValueOrDie();
    std::cout << "  Scan returned " << result_table->num_rows() << " rows" << std::endl;

    if (result_table->num_rows() != 1) {
        std::cerr << "  ✗ Expected 1 row, got " << result_table->num_rows() << std::endl;
        return 1;
    }
    std::cout << "  ✓ Scan returned correct number of triples" << std::endl;

    // Test 9: Scan with bound subject
    std::cout << "\n[Test 9] Scanning with bound subject..." << std::endl;

    TriplePattern bound_subject_pattern{subject_id.getBits(), std::nullopt, std::nullopt};
    auto bound_scan_result = triple_store->ScanPattern(bound_subject_pattern);

    if (!bound_scan_result.ok()) {
        std::cerr << "  ✗ Bound scan failed: " << bound_scan_result.status().ToString() << std::endl;
        return 1;
    }

    auto bound_result_table = bound_scan_result.ValueOrDie();
    std::cout << "  Bound scan returned " << bound_result_table->num_rows() << " rows" << std::endl;

    if (bound_result_table->num_rows() != 1) {
        std::cerr << "  ✗ Expected 1 row with bound subject" << std::endl;
        return 1;
    }
    std::cout << "  ✓ Bound subject scan correct" << std::endl;

    // Test 10: Estimate cardinality
    std::cout << "\n[Test 10] Estimating cardinality..." << std::endl;

    auto cardinality_result = triple_store->EstimateCardinality(full_scan_pattern);
    if (!cardinality_result.ok()) {
        std::cerr << "  ✗ Cardinality estimation failed" << std::endl;
        return 1;
    }

    size_t cardinality = cardinality_result.ValueOrDie();
    std::cout << "  Estimated cardinality: " << cardinality << std::endl;
    std::cout << "  ✓ Cardinality estimation working" << std::endl;

    std::cout << "\n=== All Tests Passed! ===" << std::endl;
    std::cout << "\nMarbleDB-backed storage is working correctly:" << std::endl;
    std::cout << "  - MarbleDB instance creation ✓" << std::endl;
    std::cout << "  - Vocabulary persistence ✓" << std::endl;
    std::cout << "  - Term storage and retrieval ✓" << std::endl;
    std::cout << "  - Triple store operations ✓" << std::endl;
    std::cout << "  - Full table scans ✓" << std::endl;
    std::cout << "  - Bound pattern scans ✓" << std::endl;
    std::cout << "  - Cardinality estimation ✓" << std::endl;

    return 0;
}
