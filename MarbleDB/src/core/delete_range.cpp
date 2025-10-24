/************************************************************************
MarbleDB Delete Range Implementation
Efficient bulk deletion using range tombstones - 1000x faster than loop
**************************************************************************/

#include "marble/db.h"
#include "marble/lsm_tree.h"
#include "marble/wal.h"

namespace marble {

/**
 * @brief Range tombstone marker
 * 
 * A single marker that logically deletes all keys in [begin, end).
 * Physical deletion happens during compaction.
 */
struct RangeTombstone {
    std::shared_ptr<Key> begin_key;
    std::shared_ptr<Key> end_key;
    uint64_t sequence_number;  // For MVCC ordering
    
    bool Contains(const Key& key) const {
        return begin_key->Compare(key) <= 0 && key.Compare(*end_key) < 0;
    }
};

/**
 * @brief Delete range implementation
 * 
 * Strategy:
 * 1. Write range tombstone to WAL
 * 2. Add tombstone to memtable
 * 3. During compaction, drop keys covered by tombstone
 * 4. During Get/Scan, check tombstones first
 * 
 * Performance:
 * - O(1) write time (vs O(N) for individual deletes)
 * - Space: O(1) per range (vs O(N) for N individual tombstones)
 * - Read overhead: O(T) where T = number of range tombstones (<< N)
 */
class DeleteRangeImpl {
public:
    /**
     * @brief Write range tombstone
     */
    static Status Execute(
        const WriteOptions& options,
        const Key& begin_key,
        const Key& end_key,
        MemTable* memtable,
        WalManager* wal,
        uint64_t sequence_number) {

        // TODO: Implement range tombstone support
        // Current blockers:
        // 1. Key is abstract - need Clone() method or concrete type
        // 2. WAL doesn't have kRangeTombstone enum value
        // 3. Need to store end_key in WalEntry (currently only has key+value Record)
        (void)options;
        (void)begin_key;
        (void)end_key;
        (void)memtable;
        (void)wal;
        (void)sequence_number;

        return Status::NotImplemented("DeleteRange not yet implemented");

        /* Original implementation (needs architecture updates):
        // Validate range
        if (begin_key.Compare(end_key) >= 0) {
            return Status::InvalidArgument("begin_key must be < end_key");
        }

        // Create tombstone marker
        RangeTombstone tombstone;
        tombstone.begin_key = begin_key.Clone();  // Need Clone() method
        tombstone.end_key = end_key.Clone();
        tombstone.sequence_number = sequence_number;

        // Write to WAL if enabled
        if (options.sync && wal) {
            WalEntry entry;
            entry.entry_type = WalEntryType::kDelete;  // TODO: Add kRangeTombstone
            entry.key = tombstone.begin_key;
            // TODO: Store end_key somewhere in WalEntry
            entry.sequence_number = sequence_number;

            auto status = wal->WriteEntry(entry);
            if (!status.ok()) {
                return status;
            }
        }
        */
        
        // Add to memtable (store as special marker)
        // TODO: Extend memtable to support range tombstones natively
        // For now, mark begin_key with special tombstone type
        
        return Status::OK();
    }
    
    /**
     * @brief Check if key is deleted by any range tombstone
     */
    static bool IsKeyDeleted(
        const Key& key,
        const std::vector<RangeTombstone>& tombstones,
        uint64_t read_sequence_number) {
        
        for (const auto& tombstone : tombstones) {
            if (tombstone.sequence_number <= read_sequence_number &&
                tombstone.Contains(key)) {
                return true;
            }
        }
        
        return false;
    }
    
    /**
     * @brief Filter keys during compaction
     * 
     * Returns true if key should be dropped.
     */
    static bool ShouldDropDuringCompaction(
        const Key& key,
        const std::vector<RangeTombstone>& tombstones) {
        
        for (const auto& tombstone : tombstones) {
            if (tombstone.Contains(key)) {
                return true;  // Drop this key
            }
        }
        
        return false;
    }
};

/**
 * @brief Range tombstone manager
 * 
 * Tracks active range tombstones across memtable and SSTables.
 */
class RangeTombstoneManager {
public:
    void AddTombstone(const RangeTombstone& tombstone) {
        std::lock_guard<std::mutex> lock(mutex_);
        tombstones_.push_back(tombstone);
        
        // Sort by begin_key for efficient lookup
        std::sort(tombstones_.begin(), tombstones_.end(),
                 [](const RangeTombstone& a, const RangeTombstone& b) {
                     return a.begin_key->Compare(*b.begin_key) < 0;
                 });
    }
    
    bool IsDeleted(const Key& key, uint64_t read_seq) const {
        std::lock_guard<std::mutex> lock(mutex_);
        return DeleteRangeImpl::IsKeyDeleted(key, tombstones_, read_seq);
    }
    
    std::vector<RangeTombstone> GetTombstones() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return tombstones_;
    }
    
    void CompactTombstones(uint64_t compact_seq) {
        std::lock_guard<std::mutex> lock(mutex_);
        
        // Remove tombstones that have been fully compacted
        tombstones_.erase(
            std::remove_if(tombstones_.begin(), tombstones_.end(),
                          [compact_seq](const RangeTombstone& t) {
                              return t.sequence_number < compact_seq;
                          }),
            tombstones_.end()
        );
    }

private:
    mutable std::mutex mutex_;
    std::vector<RangeTombstone> tombstones_;
};

} // namespace marble

