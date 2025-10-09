#include <marble/marble.h>
#include <iostream>
#include <memory>

using namespace marble;

int main() {
    std::cout << "MarbleDB Version Management Example" << std::endl;
    std::cout << "===================================" << std::endl;

    // Version management is under development
    std::cout << "Version management support is currently under development." << std::endl;
    std::cout << "This example demonstrates the interface structure." << std::endl;
    return 0;

    // Create version set
    auto version_set = CreateVersionSet();
    std::cout << "Created version set" << std::endl;

    // Create MVCC manager
    auto timestamp_provider = CreateDefaultTimestampProvider();
    auto mvcc_manager = CreateMVCCManager(std::move(timestamp_provider));
    std::cout << "Created MVCC manager" << std::endl;

    // Get initial version
    auto initial_version = version_set->Current();
    std::cout << "Initial version timestamp: " << initial_version->GetTimestamp() << std::endl;

    // Test timestamp generation
    std::cout << "Current timestamp: " << mvcc_manager->GetReadTimestamp() << std::endl;
    std::cout << "Next write timestamp: " << mvcc_manager->GetWriteTimestamp() << std::endl;

    // Create a snapshot
    auto snapshot = version_set->CreateSnapshot();
    std::cout << "Created snapshot at timestamp: " << snapshot->GetTimestamp() << std::endl;

    // Test version statistics
    std::string version_stats, mvcc_stats;
    version_set->GetStats(&version_stats);
    mvcc_manager->GetStats(&mvcc_stats);

    std::cout << "\nStatistics:" << std::endl;
    std::cout << "Version Set: " << version_stats << std::endl;
    std::cout << "MVCC Manager: " << mvcc_stats << std::endl;

    std::cout << "\nVersion management example completed successfully!" << std::endl;
    std::cout << "\nKey features demonstrated:" << std::endl;
    std::cout << "• Version set management" << std::endl;
    std::cout << "• MVCC manager with timestamp generation" << std::endl;
    std::cout << "• Snapshot creation" << std::endl;
    std::cout << "• Version statistics" << std::endl;

    return 0;
}

    // Apply edits to create new version
    std::shared_ptr<Version> version1;
    Status status = version_set->ApplyEdits(edits, &version1);
    if (!status.ok()) {
        std::cerr << "Failed to apply edits: " << status.ToString() << std::endl;
        return 1;
    }

    std::cout << "\nCreated version 1 with timestamp: " << version1->GetTimestamp() << std::endl;

    // Test reading from version 1
    SimpleKey test_key("user_123");
    std::shared_ptr<Record> record;
    status = version1->Get(test_key, version1->GetTimestamp(), &record);
    if (status.ok()) {
        std::cout << "Found record in version 1: " << record->GetKey()->ToString() << std::endl;
    } else {
        std::cout << "Record not found in version 1: " << status.ToString() << std::endl;
    }

    // Create another version with more changes
    edits.clear();

    // Add another SSTable to level 0
    VersionEdit add_edit2(VersionEditType::kAddSSTable);
    add_edit2.level = 0;
    add_edit2.sstable = std::make_shared<MockSSTable>("sstable_002.sst", "product");
    edits.push_back(add_edit2);

    // Move first SSTable to level 1 (simulating compaction)
    VersionEdit delete_edit(VersionEditType::kDeleteSSTable);
    delete_edit.level = 0;
    delete_edit.sstable = std::make_shared<MockSSTable>("sstable_001.sst", "user");
    edits.push_back(delete_edit);

    VersionEdit add_to_l1(VersionEditType::kAddSSTable);
    add_to_l1.level = 1;
    add_to_l1.sstable = std::make_shared<MockSSTable>("sstable_001.sst", "user");
    edits.push_back(add_to_l1);

    std::shared_ptr<Version> version2;
    status = version_set->ApplyEdits(edits, &version2);
    if (!status.ok()) {
        std::cerr << "Failed to apply edits for version 2: " << status.ToString() << std::endl;
        return 1;
    }

    std::cout << "\nCreated version 2 with timestamp: " << version2->GetTimestamp() << std::endl;

    // Test reading from both versions
    std::cout << "\nTesting version isolation:" << std::endl;

    // Test user key in both versions
    status = version1->Get(test_key, version1->GetTimestamp(), &record);
    std::cout << "User key in version 1: " << (status.ok() ? "found" : "not found") << std::endl;

    status = version2->Get(test_key, version2->GetTimestamp(), &record);
    std::cout << "User key in version 2: " << (status.ok() ? "found" : "not found") << std::endl;

    // Test product key (only in version 2)
    SimpleKey product_key("product_456");
    status = version1->Get(product_key, version1->GetTimestamp(), &record);
    std::cout << "Product key in version 1: " << (status.ok() ? "found" : "not found") << std::endl;

    status = version2->Get(product_key, version2->GetTimestamp(), &record);
    std::cout << "Product key in version 2: " << (status.ok() ? "found" : "not found") << std::endl;

    // Test snapshots
    std::cout << "\nTesting snapshots:" << std::endl;

    std::shared_ptr<Version> snapshot1;
    status = snapshot_manager->CreateSnapshot("checkpoint_1", &snapshot1);
    if (status.ok()) {
        std::cout << "Created snapshot 'checkpoint_1' at timestamp: " << snapshot1->GetTimestamp() << std::endl;
    }

    // Create another version
    edits.clear();
    VersionEdit add_edit3(VersionEditType::kAddSSTable);
    add_edit3.level = 2;
    add_edit3.sstable = std::make_shared<MockSSTable>("sstable_003.sst", "order");
    edits.push_back(add_edit3);

    std::shared_ptr<Version> version3;
    status = version_set->ApplyEdits(edits, &version3);
    if (status.ok()) {
        std::cout << "Created version 3 with timestamp: " << version3->GetTimestamp() << std::endl;
    }

    // Test retrieving snapshot
    std::shared_ptr<Version> retrieved_snapshot;
    status = snapshot_manager->GetSnapshot("checkpoint_1", &retrieved_snapshot);
    if (status.ok()) {
        std::cout << "Retrieved snapshot 'checkpoint_1' with timestamp: " << retrieved_snapshot->GetTimestamp() << std::endl;
    }

    // Test time travel - get version at specific timestamp
    auto historical_version = version_set->GetVersionAt(snapshot1->GetTimestamp());
    std::cout << "Time travel to timestamp " << snapshot1->GetTimestamp()
              << " returned version with timestamp: " << historical_version->GetTimestamp() << std::endl;

    // Test MVCC transactions
    std::cout << "\nTesting MVCC transactions:" << std::endl;

    TransactionId txn1, txn2;
    status = mvcc_manager->BeginTransaction(&txn1);
    if (status.ok()) {
        std::cout << "Started transaction 1 with ID: " << txn1 << std::endl;
    }

    status = mvcc_manager->BeginTransaction(&txn2);
    if (status.ok()) {
        std::cout << "Started transaction 2 with ID: " << txn2 << std::endl;
    }

    // Add some write operations
    VersionedKey write_key{std::make_shared<SimpleKey>("txn_test"), version_set->CurrentTimestamp(), txn1};
    VersionedRecord write_record{std::make_shared<SimpleRecord>("txn_data"), version_set->CurrentTimestamp(), txn1};

    status = mvcc_manager->AddWriteOperation(txn1, write_key, write_record);
    if (status.ok()) {
        std::cout << "Added write operation to transaction 1" << std::endl;
    }

    // Commit transaction 1
    status = mvcc_manager->CommitTransaction(txn1);
    if (status.ok()) {
        std::cout << "Committed transaction 1" << std::endl;
    }

    // Rollback transaction 2
    status = mvcc_manager->RollbackTransaction(txn2);
    if (status.ok()) {
        std::cout << "Rolled back transaction 2" << std::endl;
    }

    // Get statistics
    std::string version_stats, mvcc_stats;
    version_set->GetStats(&version_stats);
    mvcc_manager->GetStats(&mvcc_stats);

    std::cout << "\nFinal Statistics:" << std::endl;
    std::cout << "Version Set: " << version_stats << std::endl;
    std::cout << "MVCC Manager: " << mvcc_stats << std::endl;

    // List snapshots
    std::vector<std::string> snapshots;
    status = snapshot_manager->ListSnapshots(&snapshots);
    if (status.ok()) {
        std::cout << "Active snapshots: " << snapshots.size() << std::endl;
        for (const auto& name : snapshots) {
            std::cout << "  - " << name << std::endl;
        }
    }

    std::cout << "\nVersion management example completed successfully!" << std::endl;
    std::cout << "\nKey features demonstrated:" << std::endl;
    std::cout << "• Version isolation with timestamps" << std::endl;
    std::cout << "• Atomic version updates with edits" << std::endl;
    std::cout << "• Snapshot creation and retrieval" << std::endl;
    std::cout << "• Time travel queries" << std::endl;
    std::cout << "• MVCC transaction management" << std::endl;
    std::cout << "• Write conflict detection" << std::endl;
    std::cout << "• Reference counting for memory management" << std::endl;

    return 0;
}
