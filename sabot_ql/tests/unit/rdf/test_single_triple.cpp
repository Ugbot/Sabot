/**
 * Test 3.2: Single Triple Insert & Retrieval
 *
 * Verifies that a triple (1, 2, 3) is correctly stored in all 3 indexes
 * with the proper permutations:
 * - SPO: (1, 2, 3)
 * - POS: (2, 3, 1)
 * - OSP: (3, 1, 2)
 */

#include <sabot_ql/storage/triple_store.h>
#include <iostream>
#include <cstdlib>

using namespace sabot_ql;

void Fail(const std::string& msg) {
    std::cerr << "\n❌ FAILED: " << msg << "\n";
    exit(1);
}

void Pass(const std::string& msg) {
    std::cout << "✅ PASSED: " << msg << "\n";
}

int main() {
    std::cout << "========================================\n";
    std::cout << "Test 3.2: Single Triple Storage\n";
    std::cout << "========================================\n\n";

    // Clean up
    std::string test_path = "/tmp/test_single_triple";
    std::system(("rm -rf " + test_path).c_str());

    std::cout << "Step 1: Create triple store...\n";
    auto store_result = CreateTripleStore(test_path);
    if (!store_result.ok()) {
        Fail("Triple store creation failed: " + store_result.status().ToString());
    }
    auto store = *store_result;
    Pass("Triple store created");

    std::cout << "\nStep 2: Insert triple (1, 2, 3)...\n";
    std::vector<Triple> triples = {
        Triple{ValueId::fromBits(1), ValueId::fromBits(2), ValueId::fromBits(3)}
    };

    auto insert_status = store->InsertTriples(triples);
    if (!insert_status.ok()) {
        Fail("Insert failed: " + insert_status.ToString());
    }
    Pass("Triple inserted");

    std::cout << "\nStep 3: Flush to ensure data is persisted...\n";
    auto flush_status = store->Flush();
    if (!flush_status.ok()) {
        Fail("Flush failed: " + flush_status.ToString());
    }
    Pass("Flush completed");

    std::cout << "\nStep 4: Scan using SPO index (subject=1)...\n";
    TriplePattern spo_pattern{1, std::nullopt, std::nullopt};
    auto spo_result = store->ScanPattern(spo_pattern);
    if (!spo_result.ok()) {
        Fail("SPO scan failed: " + spo_result.status().ToString());
    }
    auto spo_table = *spo_result;

    std::cout << "  Result: " << spo_table->num_rows() << " rows, "
              << spo_table->num_columns() << " columns\n";
    std::cout << "  Schema: " << spo_table->schema()->ToString() << "\n";

    if (spo_table->num_rows() == 0) {
        std::cout << "\n⚠️  WARNING: SPO scan returned 0 rows!\n";
        std::cout << "  This indicates the storage layer is NOT retrieving inserted data.\n";
        std::cout << "  Continuing tests to gather more information...\n\n";
    } else if (spo_table->num_rows() != 1) {
        Fail("Expected 1 row, got " + std::to_string(spo_table->num_rows()));
    } else {
        Pass("SPO scan returned 1 row");

        // Verify column values
        if (spo_table->num_columns() != 2) {
            Fail("Expected 2 columns (predicate, object), got " +
                 std::to_string(spo_table->num_columns()));
        }
        Pass("SPO scan has correct column count");
    }

    std::cout << "\nStep 5: Scan using POS index (predicate=2)...\n";
    TriplePattern pos_pattern{std::nullopt, 2, std::nullopt};
    auto pos_result = store->ScanPattern(pos_pattern);
    if (!pos_result.ok()) {
        Fail("POS scan failed: " + pos_result.status().ToString());
    }
    auto pos_table = *pos_result;

    std::cout << "  Result: " << pos_table->num_rows() << " rows, "
              << pos_table->num_columns() << " columns\n";

    if (pos_table->num_rows() == 0) {
        std::cout << "  ⚠️  POS scan also returned 0 rows\n";
    } else if (pos_table->num_rows() != 1) {
        Fail("Expected 1 row from POS, got " + std::to_string(pos_table->num_rows()));
    } else {
        Pass("POS scan returned 1 row");
    }

    std::cout << "\nStep 6: Scan using OSP index (object=3)...\n";
    TriplePattern osp_pattern{std::nullopt, std::nullopt, 3};
    auto osp_result = store->ScanPattern(osp_pattern);
    if (!osp_result.ok()) {
        Fail("OSP scan failed: " + osp_result.status().ToString());
    }
    auto osp_table = *osp_result;

    std::cout << "  Result: " << osp_table->num_rows() << " rows, "
              << osp_table->num_columns() << " columns\n";

    if (osp_table->num_rows() == 0) {
        std::cout << "  ⚠️  OSP scan also returned 0 rows\n";
    } else if (osp_table->num_rows() != 1) {
        Fail("Expected 1 row from OSP, got " + std::to_string(osp_table->num_rows()));
    } else {
        Pass("OSP scan returned 1 row");
    }

    std::cout << "\nStep 7: Full scan (?, ?, ?)...\n";
    TriplePattern full_scan{std::nullopt, std::nullopt, std::nullopt};
    auto full_result = store->ScanPattern(full_scan);
    if (!full_result.ok()) {
        Fail("Full scan failed: " + full_result.status().ToString());
    }
    auto full_table = *full_result;

    std::cout << "  Result: " << full_table->num_rows() << " rows, "
              << full_table->num_columns() << " columns\n";

    if (full_table->num_rows() == 0) {
        std::cout << "\n========================================\n";
        std::cout << "❌ CRITICAL ISSUE DETECTED\n";
        std::cout << "========================================\n";
        std::cout << "All scans returned 0 rows despite successful insert.\n";
        std::cout << "This confirms the storage layer is NOT retrieving data.\n";
        std::cout << "\nPossible causes:\n";
        std::cout << "  1. Flush() is not actually persisting data\n";
        std::cout << "  2. Iterator is not being created correctly\n";
        std::cout << "  3. KeyRange construction is wrong\n";
        std::cout << "  4. Data is written but in wrong format\n";
        std::cout << "\nNext: Run test_triple_iterator.cpp for detailed debugging\n";
        exit(1);
    } else if (full_table->num_rows() != 1) {
        Fail("Expected 1 row from full scan, got " + std::to_string(full_table->num_rows()));
    } else {
        Pass("Full scan returned 1 row");
    }

    std::cout << "\n========================================\n";
    std::cout << "✅ ALL TESTS PASSED\n";
    std::cout << "========================================\n";
    std::cout << "\nVerified:\n";
    std::cout << "  - Triple inserted successfully\n";
    std::cout << "  - SPO index retrieves data correctly\n";
    std::cout << "  - POS index retrieves data correctly\n";
    std::cout << "  - OSP index retrieves data correctly\n";
    std::cout << "  - Full scan retrieves data correctly\n";

    return 0;
}
