#pragma once

#include <memory>
#include <vector>
#include <marble/status.h>
#include <marble/record.h>

namespace marble {

// Transaction options
struct TransactionOptions {
    // Transaction timeout in milliseconds (0 = no timeout)
    uint64_t timeout_ms = 0;

    // Maximum number of retries on write conflicts
    size_t max_retries = 3;

    // Snapshot isolation level
    enum class IsolationLevel {
        kReadCommitted,
        kSnapshot
    };
    IsolationLevel isolation_level = IsolationLevel::kSnapshot;
};

// Transaction interface (forward declaration - defined later)
class Transaction;

// Transaction factory and management
class TransactionFactory {
public:
    virtual ~TransactionFactory() = default;

    virtual Status BeginTransaction(const TransactionOptions& options,
                                    std::unique_ptr<Transaction>* transaction) = 0;
};

// Snapshot isolation support
class Snapshot {
public:
    virtual ~Snapshot() = default;

    virtual uint64_t GetTimestamp() const = 0;
    virtual Status Release() = 0;
};

// Write conflict information
struct WriteConflict {
    std::shared_ptr<Key> key;
    std::string description;
};

// Conflict resolution strategies
enum class ConflictResolution {
    kAbort,           // Abort the transaction
    kRetry,           // Retry the operation
    kOverwrite,       // Overwrite the conflicting value
    kMerge            // Merge the values (user-defined logic)
};

// Transaction support
class Transaction {
public:
    virtual ~Transaction() = default;

    virtual Status Put(std::shared_ptr<Record> record) = 0;
    virtual Status Get(const Key& key, std::shared_ptr<Record>* record) = 0;
    virtual Status Delete(const Key& key) = 0;

    virtual Status Commit() = 0;
    virtual Status Rollback() = 0;
};

// Advanced transaction with conflict resolution
class AdvancedTransaction : public Transaction {
public:
    // Get list of write conflicts
    virtual Status GetWriteConflicts(std::vector<WriteConflict>* conflicts) = 0;

    // Resolve conflicts manually
    virtual Status ResolveConflict(const Key& key, ConflictResolution resolution) = 0;

    // Set conflict resolver callback
    using ConflictResolver = std::function<Status(const std::vector<WriteConflict>&, std::vector<ConflictResolution>*)>;
    virtual Status SetConflictResolver(ConflictResolver resolver) = 0;
};

} // namespace marble
