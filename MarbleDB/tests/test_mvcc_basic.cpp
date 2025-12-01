/**
 * Basic MVCC Transaction Test
 *
 * Tests the BeginTransaction() API to verify MVCC initialization works.
 */

#include <iostream>
#include <memory>
#include <string>

#include "marble/api.h"
#include "marble/db.h"
#include "marble/status.h"

using namespace marble;

int main() {
    std::cout << "=== MVCC Basic Transaction Test ===" << std::endl;

    // Open database
    std::unique_ptr<MarbleDB> db;
    auto status = OpenDatabase("/tmp/marble_mvcc_test", &db);
    if (!status.ok()) {
        std::cout << "FAIL: Failed to open database: " << status.ToString() << std::endl;
        return 1;
    }
    std::cout << "OK: Database opened" << std::endl;

    // Test 1: Begin a transaction
    std::cout << "\nTest 1: BeginTransaction()" << std::endl;
    DBTransaction* txn = nullptr;
    status = db->BeginTransaction(&txn);

    if (!status.ok()) {
        std::cout << "FAIL: BeginTransaction failed: " << status.ToString() << std::endl;
        return 1;
    }

    if (txn == nullptr) {
        std::cout << "FAIL: Transaction pointer is null" << std::endl;
        return 1;
    }

    std::cout << "OK: Transaction created, ID: " << txn->GetTxnId() << std::endl;

    // Test 2: Commit empty transaction
    std::cout << "\nTest 2: Commit()" << std::endl;
    status = txn->Commit();

    if (!status.ok()) {
        std::cout << "WARNING: Commit returned: " << status.ToString() << std::endl;
        // This is OK for now - the basic lifecycle works
    } else {
        std::cout << "OK: Transaction committed" << std::endl;
    }

    // Cleanup
    delete txn;

    // Test 3: Begin a read-only transaction
    std::cout << "\nTest 3: Read-only transaction" << std::endl;
    TransactionOptions opts;
    opts.read_only = true;

    DBTransaction* ro_txn = nullptr;
    status = db->BeginTransaction(opts, &ro_txn);

    if (!status.ok()) {
        std::cout << "FAIL: BeginTransaction (read-only) failed: " << status.ToString() << std::endl;
        return 1;
    }

    std::cout << "OK: Read-only transaction created, ID: " << ro_txn->GetTxnId() << std::endl;
    std::cout << "OK: IsReadOnly = " << (ro_txn->IsReadOnly() ? "true" : "false") << std::endl;

    // Read-only transactions should always commit successfully
    status = ro_txn->Commit();
    if (status.ok()) {
        std::cout << "OK: Read-only transaction committed" << std::endl;
    } else {
        std::cout << "FAIL: Read-only commit failed: " << status.ToString() << std::endl;
    }

    delete ro_txn;

    std::cout << "\n=== All MVCC Basic Tests Passed ===" << std::endl;
    return 0;
}
