/**
 * MVCC Transaction Implementation
 *
 * Concrete implementation of DBTransaction with snapshot isolation.
 */

#include "marble/db.h"
#include "marble/mvcc.h"
#include "marble/lsm_storage.h"
#include "marble/version.h"
#include "marble/status.h"
#include <memory>
#include <mutex>

namespace marble {

class MVCCTransactionImpl : public DBTransaction {
public:
    MVCCTransactionImpl(MVCCManager* mvcc_manager,
                       LSMTree* lsm_tree,
                       WalManager* wal_manager,
                       uint64_t txn_id,
                       const Snapshot& snapshot,
                       bool read_only,
                       Timestamp start_time)
        : mvcc_manager_(mvcc_manager)
        , lsm_tree_(lsm_tree)
        , wal_manager_(wal_manager)
        , txn_id_(txn_id)
        , snapshot_(snapshot)
        , read_only_(read_only)
        , start_time_(start_time)
        , committed_(false)
        , rolled_back_(false) {}

    ~MVCCTransactionImpl() override {
        if (!committed_ && !rolled_back_) {
            // Auto-rollback on destruction
            Rollback();
        }
    }

    // Read operations (snapshot isolation)
    Status Get(const ReadOptions& options, const Key& key,
              std::shared_ptr<Record>* record) override {
        if (rolled_back_) {
            return Status::InvalidArgument("Transaction already rolled back");
        }
        if (committed_) {
            return Status::InvalidArgument("Transaction already committed");
        }

        // Check write buffer first (read-your-writes)
        std::shared_ptr<Record> buffered_record;
        if (write_buffer_.Get(key, &buffered_record)) {
            *record = buffered_record;  // May be nullptr for tombstones
            return Status::OK();
        }

        // For now, delegate to MVCC manager for snapshot reads
        // In a full implementation, this would read from LSM tree with timestamp filtering
        return mvcc_manager_->GetForSnapshot(key, snapshot_.timestamp(), record);
    }

    // Write operations (buffered until commit)
    Status Put(const WriteOptions& options, std::shared_ptr<Record> record) override {
        if (read_only_) {
            return Status::InvalidArgument("Cannot write to read-only transaction");
        }
        if (rolled_back_) {
            return Status::InvalidArgument("Transaction already rolled back");
        }
        if (committed_) {
            return Status::InvalidArgument("Transaction already committed");
        }

        // TODO: Validate record against schema
        write_buffer_.Put(record);
        return Status::OK();
    }

    Status Delete(const WriteOptions& options, const Key& key) override {
        if (read_only_) {
            return Status::InvalidArgument("Cannot write to read-only transaction");
        }
        if (rolled_back_) {
            return Status::InvalidArgument("Transaction already rolled back");
        }
        if (committed_) {
            return Status::InvalidArgument("Transaction already committed");
        }

        write_buffer_.Delete(key.Clone());
        return Status::OK();
    }

    // Column family operations (simplified - delegate to main operations)
    Status Put(const WriteOptions& options, ColumnFamilyHandle* cf,
              std::shared_ptr<Record> record) override {
        // For now, ignore column family - just use main operation
        return Put(options, record);
    }

    Status Delete(const WriteOptions& options, ColumnFamilyHandle* cf,
                 const Key& key) override {
        // For now, ignore column family - just use main operation
        return Delete(options, key);
    }

    // Transaction control
    Status Commit() override {
        if (read_only_) {
            committed_ = true;
            return Status::OK();  // Read-only transactions always succeed
        }

        if (rolled_back_) {
            return Status::InvalidArgument("Transaction already rolled back");
        }
        if (committed_) {
            return Status::InvalidArgument("Transaction already committed");
        }

        // Create transaction context for the MVCC manager
        MVCCManager::TransactionContext ctx;
        ctx.txn_id = txn_id_;
        ctx.snapshot = snapshot_;
        ctx.write_buffer = &write_buffer_;
        ctx.read_only = read_only_;
        ctx.start_time = start_time_;

        // Use real MVCC manager to commit the transaction
        auto status = mvcc_manager_->CommitTransaction(ctx);

        if (status.ok()) {
            committed_ = true;
        }

        return status;
    }

    Status Rollback() override {
        if (committed_) {
            return Status::InvalidArgument("Transaction already committed");
        }

        // Create transaction context for rollback
        MVCCManager::TransactionContext ctx;
        ctx.txn_id = txn_id_;
        ctx.snapshot = snapshot_;
        ctx.write_buffer = &write_buffer_;
        ctx.read_only = read_only_;
        ctx.start_time = start_time_;

        // Use MVCC manager to rollback
        auto status = mvcc_manager_->RollbackTransaction(ctx);

        if (status.ok()) {
            rolled_back_ = true;
        }

        return status;
    }

    // Transaction metadata
    uint64_t GetTxnId() const override {
        return txn_id_;
    }

    Snapshot GetSnapshot() const override {
        return snapshot_;
    }

    bool IsReadOnly() const override {
        return read_only_;
    }

private:
    MVCCManager* mvcc_manager_;
    LSMTree* lsm_tree_;
    WalManager* wal_manager_;
    uint64_t txn_id_;
    Snapshot snapshot_;
    WriteBuffer write_buffer_;
    bool read_only_;
    Timestamp start_time_;
    bool committed_;
    bool rolled_back_;
};

// Factory function to create MVCC transactions
Status CreateMVCCTransaction(MVCCManager* mvcc_manager,
                           LSMTree* lsm_tree,
                           WalManager* wal_manager,
                           const TransactionOptions& options,
                           DBTransaction** txn) {
    if (!mvcc_manager || !lsm_tree) {
        return Status::InvalidArgument("MVCC manager and LSM tree required");
    }

    auto ctx = mvcc_manager->BeginTransaction(options.read_only);

    *txn = new MVCCTransactionImpl(mvcc_manager, lsm_tree, wal_manager,
                                  ctx.txn_id, ctx.snapshot, options.read_only, ctx.start_time);

    return Status::OK();
}

} // namespace marble
