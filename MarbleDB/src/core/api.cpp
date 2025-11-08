/**
 * MarbleDB Public API Implementation
 *
 * Implementation of the clean public API that wraps internal complexity
 * and provides a stable interface for external use.
 */

#include "marble/api.h"
#include "marble/db.h"
#include "marble/table.h"
#include "marble/query.h"
#include "marble/analytics.h"
#include "marble/skipping_index.h"
#include "marble/status.h"
#include "marble/ttl.h"
#include "marble/lsm_storage.h"
#include "marble/mvcc.h"
#include "marble/wal.h"
#include "marble/version.h"
#include "marble/range_iterator.h"
#include "marble/arrow_serialization.h"
#include "marble/optimization_factory.h"
#include "marble/optimization_strategy.h"
#include "marble/table_capabilities.h"
#include "marble/index_persistence.h"
#include "marble/arrow/reader.h"
#include "marble/arrow/lsm_iterator.h"
#include <nlohmann/json.hpp>
#include <arrow/api.h>
#include <arrow/ipc/api.h>
#include <arrow/io/api.h>
#include <unordered_map>
#include <mutex>
#include <sstream>
#include <iostream>

namespace marble {

//==============================================================================
// Internal Helper Functions
//==============================================================================
namespace {
// Simple MarbleDB implementation for API
class SimpleMarbleDB : public MarbleDB {
public:
    SimpleMarbleDB(const std::string& path) : path_(path) {
        // Initialize LSM tree for persistent storage
        lsm_tree_ = std::make_unique<StandardLSMTree>();

        LSMTreeConfig config;
        config.data_directory = path + "/lsm";
        config.memtable_max_size_bytes = 64 * 1024 * 1024;  // 64MB
        config.l0_compaction_trigger = 4;
        config.compaction_threads = 2;
        config.flush_threads = 1;

        auto status = lsm_tree_->Init(config);
        if (!status.ok()) {
            throw std::runtime_error("Failed to initialize LSM tree: " + status.ToString());
        }

        // Initialize WAL manager
        wal_manager_ = CreateWalManager(nullptr);
        WalOptions wal_options;
        wal_options.wal_path = path + "/wal";
        wal_options.max_file_size = 64 * 1024 * 1024; // 64MB
        wal_options.sync_mode = WalOptions::SyncMode::kAsync;

        status = wal_manager_->Open(wal_options);
        if (!status.ok()) {
            throw std::runtime_error("Failed to initialize WAL manager: " + status.ToString());
        }

        // Initialize index persistence manager
        index_persistence_manager_ = std::make_unique<IndexPersistenceManager>(path);
        status = index_persistence_manager_->Initialize();
        if (!status.ok()) {
            throw std::runtime_error("Failed to initialize index persistence manager: " + status.ToString());
        }

        // Initialize MVCC manager
        initializeMVCC();
        // TODO: Fix global_mvcc_manager - MVCCManager is forward-declared so can't use unique_ptr
        mvcc_manager_ = nullptr;  // global_mvcc_manager.get();

        // MVCC manager is initialized in its constructor

        // ★★★ CREATE DEFAULT COLUMN FAMILY ★★★
        // Create a default column family with a generic schema so that Get() without
        // a column family handle will work
        auto default_schema = arrow::schema({
            arrow::field("key", arrow::uint64()),
            arrow::field("value", arrow::binary())
        });

        ColumnFamilyOptions default_options;
        default_options.schema = default_schema;
        ColumnFamilyDescriptor default_descriptor("default", default_options);
        ColumnFamilyHandle* default_handle = nullptr;

        // Create the default column family (ignore errors - it might already exist)
        auto create_status = CreateColumnFamily(default_descriptor, &default_handle);
        if (create_status.ok() && default_handle) {
            delete default_handle;  // We don't need the handle
        }
    }

    ~SimpleMarbleDB() {
        // Cleanup MVCC on shutdown
        shutdownMVCC();
    }

    // Column Family management
    struct ColumnFamilyInfo {
        std::string name;
        std::shared_ptr<arrow::Schema> schema;
        // Note: data is now stored in LSM tree, not in memory
        uint32_t id;
        uint64_t next_batch_id = 0;  // Sequential batch ID for LSM storage
        std::shared_ptr<SkippingIndex> skipping_index;
        std::shared_ptr<BloomFilter> bloom_filter;

        // Secondary index: maps row keys to (batch_id, row_offset) pairs
        // This enables individual row lookups via Get/Put/Delete operations
        struct RowLocation {
            uint64_t batch_id;
            uint32_t row_offset;
            RowLocation() : batch_id(0), row_offset(0) {}
            RowLocation(uint64_t b, uint32_t r) : batch_id(b), row_offset(r) {}
        };
        std::unordered_map<uint64_t, RowLocation> row_index;  // key_hash -> (batch_id, row_offset)

        // ★★★ BATCH CACHE - Avoid repeated deserialization ★★★
        struct BatchCacheEntry {
            std::shared_ptr<arrow::RecordBatch> batch;
            uint64_t last_access_time;  // For LRU eviction

            BatchCacheEntry() : last_access_time(0) {}
            BatchCacheEntry(std::shared_ptr<arrow::RecordBatch> b, uint64_t t)
                : batch(std::move(b)), last_access_time(t) {}
        };
        std::unordered_map<uint64_t, BatchCacheEntry> batch_cache;  // batch_id -> cached batch
        size_t batch_cache_max_size = 100;  // Cache last 100 batches (~100MB)
        uint64_t cache_access_counter = 0;  // Monotonic counter for LRU

        // ★★★ HOT KEY CACHE - Avoid secondary index lookups for hot keys ★★★
        struct HotKeyCacheEntry {
            RowLocation location;           // (batch_id, row_offset)
            uint64_t last_access_time;      // For LRU eviction

            HotKeyCacheEntry() : last_access_time(0) {}
            HotKeyCacheEntry(const RowLocation& loc, uint64_t t)
                : location(loc), last_access_time(t) {}
        };
        std::unordered_map<uint64_t, HotKeyCacheEntry> hot_key_cache;  // key_hash -> location
        size_t hot_key_cache_max_size = 100;  // Cache last 100 hot keys (~10KB)
        uint64_t hot_key_access_counter = 0;  // Monotonic counter for LRU

        // Buffering for Put operations (flush when full)
        std::vector<std::shared_ptr<Record>> put_buffer;
        static constexpr size_t PUT_BATCH_SIZE = 1024;  // Flush every 1024 records

        // ★★★ OPTIMIZATION PIPELINE - Pluggable optimization strategies ★★★
        std::unique_ptr<OptimizationPipeline> optimization_pipeline;

        ColumnFamilyInfo(std::string n, std::shared_ptr<arrow::Schema> s, uint32_t i)
            : name(std::move(n)), schema(std::move(s)), id(i) {
            // Auto-configure optimizations based on schema
            TableCapabilities caps;  // Use default capabilities
            WorkloadHints hints;
            // Let factory auto-detect from schema
            optimization_pipeline = OptimizationFactory::CreateForSchema(s, caps, hints);
        }

        // Flush buffered records to LSM tree and update secondary index
        Status FlushPutBuffer(SimpleMarbleDB* db) {
            if (put_buffer.empty()) return Status::OK();

            // Convert buffered records to RecordBatch
            std::vector<std::shared_ptr<arrow::RecordBatch>> batches;
            for (const auto& record : put_buffer) {
                auto batch_result = record->ToRecordBatch();
                if (!batch_result.ok()) {
                    return Status::InternalError("Failed to convert record to batch: " +
                                               batch_result.status().ToString());
                }
                batches.push_back(batch_result.ValueOrDie());
            }

            // Combine into single batch using ConcatenateRecordBatches
            // This properly preserves buffer lifetimes for binary data
            auto combined_result = arrow::ConcatenateRecordBatches(batches);
            if (!combined_result.ok()) {
                return Status::InternalError("Failed to concatenate batches: " +
                                           combined_result.status().ToString());
            }
            auto batch = combined_result.ValueOrDie();

            // Assign batch ID and store in LSM
            uint64_t batch_id = next_batch_id++;
            std::string serialized_batch;
            auto serialize_status = SerializeArrowBatch(batch, &serialized_batch);
            if (!serialize_status.ok()) return serialize_status;

            uint64_t batch_key = db->EncodeBatchKey(id, batch_id);
            auto put_status = db->lsm_tree_->Put(batch_key, serialized_batch);
            if (!put_status.ok()) return put_status;

            // Update secondary index for each record
            for (size_t i = 0; i < put_buffer.size(); ++i) {
                const auto& record = put_buffer[i];
                auto key = record->GetKey();
                // Use the key's built-in hash function
                size_t key_hash = key->Hash();
                row_index[key_hash] = RowLocation{batch_id, static_cast<uint32_t>(i)};
            }

            // ★★★ OPTIMIZATION PIPELINE - Notify of flush (serialize bloom filters, etc.) ★★★
            if (optimization_pipeline) {
                FlushContext flush_ctx{batch, static_cast<size_t>(batch->num_rows())};

                auto opt_status = optimization_pipeline->OnFlush(&flush_ctx);
                if (!opt_status.ok()) {
                    // Log warning but don't fail the flush
                    std::cerr << "Optimization OnFlush failed: " << opt_status.ToString() << std::endl;
                }

                // Store bloom filter metadata if requested
                if (flush_ctx.include_bloom_filter && !flush_ctx.metadata.empty()) {
                    // TODO: Store bloom filter metadata in LSM tree metadata
                }
            }

            // ★★★ PERSIST INDEXES TO DISK ★★★
            auto save_indexes_status = SaveIndexes(db, batch_id);
            if (!save_indexes_status.ok()) {
                std::cerr << "Failed to save indexes: " << save_indexes_status.ToString() << std::endl;
            }

            // Clear buffer
            put_buffer.clear();
            return Status::OK();
        }

        // ★★★ SAVE INDEXES TO DISK ★★★
        Status SaveIndexes(SimpleMarbleDB* db, uint64_t batch_id) {
            if (!db->index_persistence_manager_) {
                return Status::OK();  // No persistence manager configured
            }

            // Save bloom filter if available
            if (bloom_filter) {
                std::vector<uint8_t> serialized = bloom_filter->Serialize();
                auto save_status = db->index_persistence_manager_->SaveBloomFilter(
                    id, batch_id, serialized);
                if (!save_status.ok()) {
                    std::cerr << "Failed to save bloom filter: " << save_status.ToString() << std::endl;
                }
            }

            return Status::OK();
        }

        // ★★★ LOAD INDEXES FROM DISK ★★★
        Status LoadIndexes(SimpleMarbleDB* db) {
            if (!db->index_persistence_manager_) {
                return Status::OK();  // No persistence manager configured
            }

            // Load all bloom filters for this column family
            std::unordered_map<uint64_t, std::vector<uint8_t>> bloom_filters;
            auto load_status = db->index_persistence_manager_->LoadAllBloomFilters(id, &bloom_filters);
            if (!load_status.ok()) {
                if (load_status.IsNotFound()) {
                    // No persisted bloom filters - this is OK for new databases
                    return Status::OK();
                }
                return load_status;
            }

            // Deserialize bloom filters into optimization_pipeline
            if (!bloom_filters.empty() && optimization_pipeline) {
                // Get the BloomFilter strategy from the pipeline
                auto* bloom_strategy = optimization_pipeline->GetStrategy("BloomFilter");
                if (bloom_strategy) {
                    // Use the most recent bloom filter (highest batch_id)
                    // In future, we could merge multiple filters
                    uint64_t max_batch_id = 0;
                    const std::vector<uint8_t>* most_recent_filter = nullptr;

                    for (const auto& [batch_id, filter_data] : bloom_filters) {
                        if (batch_id > max_batch_id) {
                            max_batch_id = batch_id;
                            most_recent_filter = &filter_data;
                        }
                    }

                    if (most_recent_filter) {
                        auto deserialize_status = bloom_strategy->Deserialize(*most_recent_filter);
                        if (!deserialize_status.ok()) {
                            std::cerr << "Warning: Failed to deserialize bloom filter for CF " << id
                                     << ": " << deserialize_status.ToString() << std::endl;
                        } else {
                            std::cerr << "Loaded bloom filter (batch " << max_batch_id
                                     << ") for CF " << id << std::endl;
                        }
                    }
                } else {
                    std::cerr << "Warning: Found persisted bloom filters for CF " << id
                             << " but no BloomFilter strategy in pipeline" << std::endl;
                }
            }

            // ★★★ LOAD SKIPPING INDEXES FROM DISK ★★★
            std::unordered_map<uint64_t, std::vector<uint8_t>> skipping_indexes;
            auto skip_load_status = db->index_persistence_manager_->LoadAllSkippingIndexes(id, &skipping_indexes);
            if (!skip_load_status.ok()) {
                if (skip_load_status.IsNotFound()) {
                    // No persisted skipping indexes - this is OK for new databases
                    // Don't need to log anything here
                } else {
                    // Log warning but continue (non-fatal)
                    std::cerr << "Warning: Failed to load skipping indexes for CF " << id
                             << ": " << skip_load_status.ToString() << std::endl;
                }
            }

            // Deserialize skipping indexes into optimization_pipeline
            if (!skipping_indexes.empty() && optimization_pipeline) {
                // Get the SkippingIndex strategy from the pipeline
                auto* skip_strategy = optimization_pipeline->GetStrategy("SkippingIndex");
                if (skip_strategy) {
                    // Use the most recent skipping index (highest batch_id)
                    uint64_t max_batch_id = 0;
                    const std::vector<uint8_t>* most_recent_index = nullptr;

                    for (const auto& [batch_id, index_data] : skipping_indexes) {
                        if (batch_id > max_batch_id) {
                            max_batch_id = batch_id;
                            most_recent_index = &index_data;
                        }
                    }

                    if (most_recent_index) {
                        auto deserialize_status = skip_strategy->Deserialize(*most_recent_index);
                        if (!deserialize_status.ok()) {
                            std::cerr << "Warning: Failed to deserialize skipping index for CF " << id
                                     << ": " << deserialize_status.ToString() << std::endl;
                        } else {
                            std::cerr << "Loaded skipping index (batch " << max_batch_id
                                     << ") for CF " << id << std::endl;
                        }
                    }
                } else {
                    std::cerr << "Warning: Found persisted skipping indexes for CF " << id
                             << " but no SkippingIndex strategy in pipeline" << std::endl;
                }
            }

            return Status::OK();
        }

        // ★★★ GET BATCH WITH CACHING ★★★
        // Checks batch cache before deserializing from LSM
        Status GetBatch(SimpleMarbleDB* db, uint64_t batch_id, std::shared_ptr<arrow::RecordBatch>* batch) {
            // Check cache first
            cache_access_counter++;
            auto cache_it = batch_cache.find(batch_id);
            if (cache_it != batch_cache.end()) {
                // Cache hit! Update access time and return
                cache_it->second.last_access_time = cache_access_counter;
                *batch = cache_it->second.batch;
                return Status::OK();
            }

            // Cache miss - read from LSM and deserialize
            uint64_t batch_key = db->EncodeBatchKey(id, batch_id);
            std::string serialized_batch;
            auto get_status = db->lsm_tree_->Get(batch_key, &serialized_batch);
            if (!get_status.ok()) {
                return get_status;
            }

            // Deserialize the batch
            std::shared_ptr<arrow::RecordBatch> deserialized_batch;
            auto deserialize_status = DeserializeArrowBatch(serialized_batch, &deserialized_batch);
            if (!deserialize_status.ok()) {
                return deserialize_status;
            }

            // Add to cache (with eviction if needed)
            if (batch_cache.size() >= batch_cache_max_size) {
                EvictOldestBatch();
            }
            batch_cache[batch_id] = BatchCacheEntry(deserialized_batch, cache_access_counter);

            *batch = deserialized_batch;
            return Status::OK();
        }

        // ★★★ LRU EVICTION ★★★
        // Evicts the least recently used batch from cache
        void EvictOldestBatch() {
            if (batch_cache.empty()) return;

            // Find entry with smallest last_access_time
            uint64_t oldest_batch_id = 0;
            uint64_t oldest_access_time = UINT64_MAX;

            for (const auto& entry : batch_cache) {
                if (entry.second.last_access_time < oldest_access_time) {
                    oldest_access_time = entry.second.last_access_time;
                    oldest_batch_id = entry.first;
                }
            }

            // Evict it
            batch_cache.erase(oldest_batch_id);
        }

        // ★★★ HOT KEY CACHE OPERATIONS ★★★
        // Check if key is in hot key cache (avoids secondary index lookup)
        bool GetHotKey(uint64_t key_hash, RowLocation* location) {
            hot_key_access_counter++;
            auto cache_it = hot_key_cache.find(key_hash);
            if (cache_it != hot_key_cache.end()) {
                // Cache hit! Update access time and return location
                cache_it->second.last_access_time = hot_key_access_counter;
                *location = cache_it->second.location;
                return true;
            }
            return false;
        }

        // Add key to hot key cache (after secondary index lookup)
        void PutHotKey(uint64_t key_hash, const RowLocation& location) {
            // Add to cache (with eviction if needed)
            if (hot_key_cache.size() >= hot_key_cache_max_size) {
                EvictOldestHotKey();
            }
            hot_key_cache[key_hash] = HotKeyCacheEntry(location, hot_key_access_counter);
        }

        // Evict the least recently used hot key from cache
        void EvictOldestHotKey() {
            if (hot_key_cache.empty()) return;

            // Find entry with smallest last_access_time
            uint64_t oldest_key_hash = 0;
            uint64_t oldest_access_time = UINT64_MAX;

            for (const auto& entry : hot_key_cache) {
                if (entry.second.last_access_time < oldest_access_time) {
                    oldest_access_time = entry.second.last_access_time;
                    oldest_key_hash = entry.first;
                }
            }

            // Evict it
            hot_key_cache.erase(oldest_key_hash);
        }

        // Invalidate hot key cache entry (on update/delete)
        void InvalidateHotKey(uint64_t key_hash) {
            hot_key_cache.erase(key_hash);
        }

        // Build indexes from current data
        // BuildIndexes() removed - indexes are now built incrementally during InsertBatch()
    };

    // LSM-backed iterator that reads batches from LSM tree
    class LSMBatchIterator : public Iterator {
    public:
        LSMBatchIterator(StandardLSMTree* lsm, uint32_t table_id, std::shared_ptr<arrow::Schema> schema)
            : lsm_(lsm)
            , schema_(schema)
            , current_batch_idx_(0)
            , current_row_idx_(0)
            , valid_(false) {

            // Scan all batches for this table from LSM
            uint64_t start_key = EncodeBatchKey(table_id, 0);
            uint64_t end_key = EncodeBatchKey(table_id + 1, 0);  // Next table's range

            std::vector<std::pair<uint64_t, std::string>> scan_results;
            auto status = lsm_->Scan(start_key, end_key, &scan_results);

            if (status.ok()) {
                // Deserialize all batches
                for (const auto& [batch_key, serialized_batch] : scan_results) {
                    std::shared_ptr<arrow::RecordBatch> batch;
                    auto deser_status = DeserializeArrowBatch(serialized_batch, &batch);
                    if (deser_status.ok() && batch && batch->num_rows() > 0) {
                        batches_.push_back(batch);
                    }
                }

                // Position at first row if we have data
                if (!batches_.empty() && batches_[0]->num_rows() > 0) {
                    valid_ = true;
                    current_batch_idx_ = 0;
                    current_row_idx_ = 0;
                }
            }
        }

        bool Valid() const override {
            return valid_;
        }

        void Next() override {
            if (!valid_) return;

            current_row_idx_++;

            // Move to next batch if needed
            while (current_batch_idx_ < batches_.size()) {
                if (current_row_idx_ < batches_[current_batch_idx_]->num_rows()) {
                    return;  // Still valid
                }
                // Move to next batch
                current_batch_idx_++;
                current_row_idx_ = 0;
            }

            // No more data
            valid_ = false;
        }

        std::shared_ptr<Key> key() const override {
            if (!valid_) return nullptr;

            // Extract key from current row
            // Assume first column is the key (or use row index as key)
            uint64_t key_value = current_batch_idx_ * 10000 + current_row_idx_;
            return std::make_shared<Int64Key>(key_value);
        }

        std::shared_ptr<Record> value() const override {
            if (!valid_) return nullptr;

            auto batch = batches_[current_batch_idx_];
            auto key_ptr = key();
            return std::make_shared<SimpleRecord>(key_ptr, batch, current_row_idx_);
        }

        std::unique_ptr<RecordRef> value_ref() const override {
            if (!valid_) return nullptr;

            // Use the record's AsRecordRef() method
            auto record = value();
            if (!record) return nullptr;
            return record->AsRecordRef();
        }

        marble::Status status() const override {
            return Status::OK();
        }

        void Seek(const Key& target) override {
            // Simple linear search for now
            // TODO: Binary search within batches
            SeekToFirst();
            while (valid_) {
                auto current_key = key();
                if (current_key->Compare(target) >= 0) {
                    return;
                }
                Next();
            }
        }

        void SeekForPrev(const Key& target) override {
            // Find last key <= target
            SeekToFirst();
            std::shared_ptr<Key> last_valid_key = nullptr;
            size_t last_batch = 0, last_row = 0;

            while (valid_) {
                auto current_key = key();
                if (current_key->Compare(target) <= 0) {
                    last_valid_key = current_key;
                    last_batch = current_batch_idx_;
                    last_row = current_row_idx_;
                }
                Next();
            }

            if (last_valid_key) {
                current_batch_idx_ = last_batch;
                current_row_idx_ = last_row;
                valid_ = true;
            } else {
                valid_ = false;
            }
        }

        void SeekToFirst() {
            if (!batches_.empty() && batches_[0]->num_rows() > 0) {
                valid_ = true;
                current_batch_idx_ = 0;
                current_row_idx_ = 0;
            } else {
                valid_ = false;
            }
        }

        void SeekToLast() override {
            if (batches_.empty()) {
                valid_ = false;
                return;
            }

            // Find last non-empty batch
            for (int i = batches_.size() - 1; i >= 0; i--) {
                if (batches_[i]->num_rows() > 0) {
                    current_batch_idx_ = i;
                    current_row_idx_ = batches_[i]->num_rows() - 1;
                    valid_ = true;
                    return;
                }
            }

            valid_ = false;
        }

        void Prev() override {
            if (!valid_) return;

            if (current_row_idx_ > 0) {
                current_row_idx_--;
                return;
            }

            // Move to previous batch
            while (current_batch_idx_ > 0) {
                current_batch_idx_--;
                if (batches_[current_batch_idx_]->num_rows() > 0) {
                    current_row_idx_ = batches_[current_batch_idx_]->num_rows() - 1;
                    return;
                }
            }

            // No more data
            valid_ = false;
        }

    private:
        StandardLSMTree* lsm_;
        std::shared_ptr<arrow::Schema> schema_;
        std::vector<std::shared_ptr<arrow::RecordBatch>> batches_;
        size_t current_batch_idx_;
        size_t current_row_idx_;
        bool valid_;
    };

    std::unordered_map<std::string, std::unique_ptr<ColumnFamilyInfo>> column_families_;
    std::unordered_map<uint32_t, ColumnFamilyInfo*> id_to_cf_;
    uint32_t next_cf_id_ = 1;
    mutable std::mutex cf_mutex_;

    // LSM tree for persistent storage
    std::unique_ptr<StandardLSMTree> lsm_tree_;

    // Key encoding helpers
    // Batch keys: bit 63 = 0, format: [0][table_id: 16 bits][batch_id: 47 bits]
    // Row keys:   bit 63 = 1, format: [1][table_id: 16 bits][row_hash: 47 bits]
    static uint64_t EncodeBatchKey(uint32_t table_id, uint64_t batch_id) {
        return ((uint64_t)table_id << 48) | (batch_id & 0x0000FFFFFFFFFFFF);
    }

    static void DecodeBatchKey(uint64_t key, uint32_t* table_id, uint64_t* batch_id) {
        *table_id = (key >> 48) & 0xFFFF;
        *batch_id = key & 0x0000FFFFFFFFFFFF;
    }

    static uint64_t EncodeRowKey(uint32_t table_id, uint64_t row_hash) {
        return (1ULL << 63) | ((uint64_t)table_id << 48) | (row_hash & 0x0000FFFFFFFFFFFF);
    }

    static void DecodeRowKey(uint64_t key, uint32_t* table_id, uint64_t* row_hash) {
        *table_id = (key >> 48) & 0xFFFF;
        *row_hash = key & 0x0000FFFFFFFFFFFF;
    }

    // Basic operations
    Status Put(const WriteOptions& options, std::shared_ptr<Record> record) override {
        std::lock_guard<std::mutex> lock(cf_mutex_);

        // For now, assume default column family (table_name = "default")
        // TODO: Add column family parameter support
        auto it = column_families_.find("default");
        if (it == column_families_.end()) {
            return Status::InvalidArgument("Default column family does not exist. Create it first.");
        }

        auto* cf_info = it->second.get();

        // Validate record schema matches column family
        auto record_schema = record->GetArrowSchema();
        if (!record_schema->Equals(cf_info->schema)) {
            return Status::InvalidArgument("Record schema does not match column family schema");
        }

        // ★★★ OPTIMIZATION PIPELINE - Notify of write (add to bloom filter, etc.) ★★★
        if (cf_info->optimization_pipeline) {
            auto key = record->GetKey();
            auto batch_result = record->ToRecordBatch();
            if (batch_result.ok()) {
                auto record_batch = batch_result.ValueOrDie();
                WriteContext write_ctx{*key, record_batch, 0};  // row_offset = 0 for single record

                auto opt_status = cf_info->optimization_pipeline->OnWrite(&write_ctx);
                if (!opt_status.ok()) {
                    // Log warning but don't fail the write
                    std::cerr << "Optimization OnWrite failed: " << opt_status.ToString() << std::endl;
                }
            }
        }

        // Add to buffer
        cf_info->put_buffer.push_back(record);

        // Flush if buffer is full
        if (cf_info->put_buffer.size() >= cf_info->PUT_BATCH_SIZE) {
            return cf_info->FlushPutBuffer(this);
        }

        return Status::OK();
    }

    Status Get(const ReadOptions& options, const Key& key, std::shared_ptr<Record>* record) override {
        std::lock_guard<std::mutex> lock(cf_mutex_);

        // For now, assume default column family
        auto it = column_families_.find("default");
        if (it == column_families_.end()) {
            return Status::InvalidArgument("Default column family does not exist");
        }

        auto* cf_info = it->second.get();

        // ★★★ OPTIMIZATION PIPELINE - Check if we can skip this read ★★★
        if (cf_info->optimization_pipeline) {
            ReadContext read_ctx{key};
            read_ctx.is_point_lookup = true;
            read_ctx.is_range_scan = false;

            auto opt_status = cf_info->optimization_pipeline->OnRead(&read_ctx);

            // If bloom filter says key definitely doesn't exist, short-circuit
            if (read_ctx.definitely_not_found) {
                return Status::NotFound("Key not found (bloom filter)");
            }
        }

        // ★★★ CHECK HOT KEY CACHE FIRST - Avoid secondary index lookup ★★★
        size_t key_hash = key.Hash();
        ColumnFamilyInfo::RowLocation location;

        if (!cf_info->GetHotKey(key_hash, &location)) {
            // Hot key cache miss - fall back to secondary index
            auto index_it = cf_info->row_index.find(key_hash);
            if (index_it == cf_info->row_index.end()) {
                return Status::NotFound("Key not found");
            }

            location = index_it->second;

            // Add to hot key cache for future accesses
            cf_info->PutHotKey(key_hash, location);
        }

        // ★★★ USE BATCH CACHE - Avoid repeated deserialization ★★★
        std::shared_ptr<arrow::RecordBatch> batch;
        auto get_status = cf_info->GetBatch(this, location.batch_id, &batch);
        if (!get_status.ok()) {
            return get_status;
        }

        // Extract the specific row
        if (location.row_offset >= static_cast<uint32_t>(batch->num_rows())) {
            return Status::InternalError("Invalid row offset in secondary index");
        }

        // Create a Record from the row
        // Use SimpleRecord which wraps a RecordBatch row
        *record = std::make_shared<SimpleRecord>(std::make_shared<Int64Key>(key_hash), batch, location.row_offset);

        return Status::OK();
    }

    Status Delete(const WriteOptions& options, const Key& key) override {
        std::lock_guard<std::mutex> lock(cf_mutex_);

        // For now, assume default column family
        auto it = column_families_.find("default");
        if (it == column_families_.end()) {
            return Status::InvalidArgument("Default column family does not exist");
        }

        auto* cf_info = it->second.get();

        // Look up key in secondary index
        size_t key_hash = key.Hash();
        auto index_it = cf_info->row_index.find(key_hash);
        if (index_it == cf_info->row_index.end()) {
            return Status::NotFound("Key not found");
        }

        // For now, implement delete as a tombstone
        // TODO: Implement proper deletion with compaction
        // Remove from secondary index (tombstone)
        cf_info->row_index.erase(index_it);

        // Invalidate hot key cache
        cf_info->InvalidateHotKey(key_hash);

        // Write tombstone to LSM tree
        uint64_t row_key = EncodeRowKey(cf_info->id, key_hash);
        std::string tombstone_value = "__TOMBSTONE__";
        auto put_status = lsm_tree_->Put(row_key, tombstone_value);
        if (!put_status.ok()) {
            return put_status;
        }

        return Status::OK();
    }
    
    // Merge operations
    Status Merge(const WriteOptions& options, const Key& key, const std::string& value) override {
        return Status::NotImplemented("Merge not implemented");
    }
    
    Status Merge(const WriteOptions& options, ColumnFamilyHandle* cf, const Key& key, const std::string& value) override {
        return Status::NotImplemented("CF Merge not implemented");
    }

    Status WriteBatch(const WriteOptions& options, const std::vector<std::shared_ptr<Record>>& records) override {
        return Status::NotImplemented("WriteBatch not implemented");
    }

    // Arrow batch operations
    Status InsertBatch(const std::string& table_name, const std::shared_ptr<arrow::RecordBatch>& batch) override {
        // Chunk large batches to prevent memory exhaustion
        // Use zero-copy slicing for efficient processing
        const int64_t CHUNK_SIZE = 5000;  // Process 5K rows at a time

        if (batch->num_rows() <= CHUNK_SIZE) {
            // Small batch, process directly
            return InsertBatchInternal(table_name, batch);
        }

        // Large batch, chunk and process
        for (int64_t offset = 0; offset < batch->num_rows(); offset += CHUNK_SIZE) {
            int64_t length = std::min(CHUNK_SIZE, batch->num_rows() - offset);

            // Safety check
            if (offset + length > batch->num_rows()) {
                return Status::InvalidArgument("Slice bounds exceed batch size");
            }

            auto chunk = batch->Slice(offset, length);  // Zero-copy slice
            if (!chunk) {
                return Status::InvalidArgument("Failed to slice batch");
            }

            auto status = InsertBatchInternal(table_name, chunk);
            if (!status.ok()) {
                return status;
            }
        }

        return Status::OK();
    }

private:
    Status InsertBatchInternal(const std::string& table_name, const std::shared_ptr<arrow::RecordBatch>& batch) {
        std::lock_guard<std::mutex> lock(cf_mutex_);

        // Find column family
        auto it = column_families_.find(table_name);
        if (it == column_families_.end()) {
            return Status::InvalidArgument("Column family '" + table_name + "' does not exist");
        }

        auto* cf_info = it->second.get();

        // Validate batch schema matches column family schema
        if (!batch->schema()->Equals(cf_info->schema)) {
            return Status::InvalidArgument("Batch schema does not match column family schema for '" + table_name + "'");
        }

        // Assign sequential batch ID
        uint64_t batch_id = cf_info->next_batch_id++;

        // Serialize RecordBatch using Arrow IPC
        std::string serialized_batch;
        auto serialize_status = SerializeArrowBatch(batch, &serialized_batch);
        if (!serialize_status.ok()) {
            return serialize_status;
        }

        // Encode key for LSM tree storage
        uint64_t batch_key = EncodeBatchKey(cf_info->id, batch_id);

        // Write to LSM tree for persistence
        auto put_status = lsm_tree_->Put(batch_key, serialized_batch);
        if (!put_status.ok()) {
            return put_status;
        }

        // ★★★ BUILD ROW_INDEX FOR POINT LOOKUPS ★★★
        // Extract keys from first column and build row_index mapping
        // This enables Get() to work with data inserted via InsertBatch()
        if (batch->num_columns() > 0 && batch->num_rows() > 0) {
            auto key_column = batch->column(0);  // Assume first column is the primary key

            for (int64_t i = 0; i < batch->num_rows(); ++i) {
                size_t key_hash = 0;
                bool key_extracted = false;

                // Extract key based on column type
                switch (key_column->type()->id()) {
                    case arrow::Type::INT64: {
                        auto arr = std::static_pointer_cast<arrow::Int64Array>(key_column);
                        if (!arr->IsNull(i)) {
                            key_hash = std::hash<int64_t>{}(arr->Value(i));
                            key_extracted = true;
                        }
                        break;
                    }
                    case arrow::Type::UINT64: {
                        auto arr = std::static_pointer_cast<arrow::UInt64Array>(key_column);
                        if (!arr->IsNull(i)) {
                            key_hash = std::hash<uint64_t>{}(arr->Value(i));
                            key_extracted = true;
                        }
                        break;
                    }
                    case arrow::Type::INT32: {
                        auto arr = std::static_pointer_cast<arrow::Int32Array>(key_column);
                        if (!arr->IsNull(i)) {
                            key_hash = std::hash<int32_t>{}(arr->Value(i));
                            key_extracted = true;
                        }
                        break;
                    }
                    case arrow::Type::STRING: {
                        auto arr = std::static_pointer_cast<arrow::StringArray>(key_column);
                        if (!arr->IsNull(i)) {
                            key_hash = std::hash<std::string>{}(arr->GetString(i));
                            key_extracted = true;
                        }
                        break;
                    }
                    default:
                        // Unsupported key type, skip this row
                        continue;
                }

                // Add to row_index if we successfully extracted a key
                if (key_extracted) {
                    cf_info->row_index[key_hash] = ColumnFamilyInfo::RowLocation{batch_id, static_cast<uint32_t>(i)};
                }
            }
        }

        // Incrementally update indexes (O(m) where m = batch size, not O(n) where n = total data)
        // Initialize indexes on first insert
        if (cf_info->next_batch_id == 1) {
            cf_info->skipping_index = std::make_shared<InMemorySkippingIndex>();
            // Start with 100K capacity, will auto-grow to 200K, 400K, 800K, etc. as needed
            cf_info->bloom_filter = std::make_shared<BloomFilter>(100000, 0.01);
        }

        // Update skipping index with new batch only
        if (cf_info->skipping_index) {
            auto single_batch_table_result = arrow::Table::FromRecordBatches(cf_info->schema, {batch});
            if (single_batch_table_result.ok()) {
                std::shared_ptr<arrow::Table> single_batch_table = single_batch_table_result.ValueOrDie();
                // Add block stats for this batch (incremental, not full rebuild)
                cf_info->skipping_index->BuildFromTable(single_batch_table, 8192);
            }
        }

        // Update bloom filter with new batch only (incremental)
        if (cf_info->bloom_filter && batch->num_columns() >= 3) {
            auto subject_col = std::static_pointer_cast<arrow::Int64Array>(batch->column(0));
            auto predicate_col = std::static_pointer_cast<arrow::Int64Array>(batch->column(1));
            auto object_col = std::static_pointer_cast<arrow::Int64Array>(batch->column(2));

            for (int64_t i = 0; i < batch->num_rows(); ++i) {
                std::string key_str = std::to_string(subject_col->Value(i)) + "," +
                                    std::to_string(predicate_col->Value(i)) + "," +
                                    std::to_string(object_col->Value(i));
                cf_info->bloom_filter->Add(key_str);
            }
        }

        return Status::OK();
    }

public:
    // Table operations
    Status CreateTable(const TableSchema& schema) override {
        // Create column family with provided schema
        ColumnFamilyOptions options;
        options.schema = schema.arrow_schema;

        ColumnFamilyDescriptor descriptor(schema.table_name, options);

        ColumnFamilyHandle* handle = nullptr;
        auto status = CreateColumnFamily(descriptor, &handle);

        // Clean up handle (we don't need to return it for CreateTable)
        if (handle) {
            delete handle;
        }

        return status;
    }

    Status ScanTable(const std::string& table_name, std::unique_ptr<QueryResult>* result) override {
        std::lock_guard<std::mutex> lock(cf_mutex_);

        // Find column family
        auto it = column_families_.find(table_name);
        if (it == column_families_.end()) {
            return Status::InvalidArgument("Column family '" + table_name + "' does not exist");
        }

        auto* cf_info = it->second.get();

        // Read all batches from LSM tree
        std::vector<std::shared_ptr<arrow::RecordBatch>> record_batches;

        // Compute key range for this table's batches (bit 63 = 0 for batch keys)
        uint64_t start_key = EncodeBatchKey(cf_info->id, 0);
        uint64_t end_key = EncodeBatchKey(cf_info->id, 0x0000FFFFFFFFFFFF);

        // Scan LSM tree for all batches
        std::vector<std::pair<uint64_t, std::string>> lsm_results;
        auto scan_status = lsm_tree_->Scan(start_key, end_key, &lsm_results);
        if (!scan_status.ok()) {
            return scan_status;
        }

        // Deserialize each batch
        for (const auto& [key, serialized_batch] : lsm_results) {
            std::shared_ptr<arrow::RecordBatch> batch;
            auto deserialize_status = DeserializeArrowBatch(
                serialized_batch.data(),
                serialized_batch.size(),
                &batch);
            if (!deserialize_status.ok()) {
                return deserialize_status;
            }
            record_batches.push_back(batch);
        }

        // Handle empty case
        if (record_batches.empty()) {
            std::vector<std::shared_ptr<arrow::ChunkedArray>> empty_columns;
            for (int i = 0; i < cf_info->schema->num_fields(); ++i) {
                auto field = cf_info->schema->field(i);
                auto empty_array = arrow::MakeArrayOfNull(field->type(), 0).ValueOrDie();
                auto chunked_array = std::make_shared<arrow::ChunkedArray>(empty_array);
                empty_columns.push_back(chunked_array);
            }
            auto empty_table = arrow::Table::Make(cf_info->schema, empty_columns);
            return TableQueryResult::Create(empty_table, result);
        }

        // Concatenate all record batches
        ARROW_ASSIGN_OR_RAISE(auto combined_table,
                             arrow::Table::FromRecordBatches(cf_info->schema, record_batches));

        // Create query result
        return TableQueryResult::Create(combined_table, result);
    }

    /**
     * @brief Create Arrow-optimized query builder
     */
    std::unique_ptr<QueryBuilder> Query(const std::string& table_name) {
        return std::make_unique<QueryBuilder>(this, table_name);
    }

    /**
     * @brief Helper for QueryExecutor to read batches from LSM
     * Used by QueryBuilder::Execute()
     */
    Status ReadBatchesFromLSM(
        const std::string& table_name,
        uint64_t start_key,
        uint64_t end_key,
        std::vector<std::shared_ptr<arrow::RecordBatch>>* batches
    ) {
        std::lock_guard<std::mutex> lock(cf_mutex_);

        auto it = column_families_.find(table_name);
        if (it == column_families_.end()) {
            return Status::InvalidArgument("Table not found: " + table_name);
        }

        auto* cf_info = it->second.get();

        // Encode key range for this table's batches
        uint64_t lsm_start = EncodeBatchKey(cf_info->id, start_key);
        uint64_t lsm_end = EncodeBatchKey(cf_info->id, end_key);

        // Scan LSM tree
        std::vector<std::pair<uint64_t, std::string>> lsm_results;
        auto scan_status = lsm_tree_->Scan(lsm_start, lsm_end, &lsm_results);
        if (!scan_status.ok()) return scan_status;

        // Deserialize batches
        for (const auto& [key, serialized] : lsm_results) {
            std::shared_ptr<arrow::RecordBatch> batch;
            auto status = DeserializeArrowBatch(
                serialized.data(),
                serialized.size(),
                &batch
            );
            if (!status.ok()) return status;

            batches->push_back(batch);
        }

        return Status::OK();
    }

    // Column Family operations
    Status CreateColumnFamily(const ColumnFamilyDescriptor& descriptor, ColumnFamilyHandle** handle) override {
        std::lock_guard<std::mutex> lock(cf_mutex_);

        // Check if column family already exists
        if (column_families_.find(descriptor.name) != column_families_.end()) {
            return Status::InvalidArgument("Column family '" + descriptor.name + "' already exists");
        }

        // Validate schema
        if (!descriptor.options.schema) {
            return Status::InvalidArgument("Column family must have a valid Arrow schema");
        }

        // Create column family info
        uint32_t cf_id = next_cf_id_++;
        auto cf_info = std::make_unique<ColumnFamilyInfo>(
            descriptor.name, descriptor.options.schema, cf_id);

        // Store in registry
        auto* cf_ptr = cf_info.get();
        column_families_[descriptor.name] = std::move(cf_info);
        id_to_cf_[cf_id] = cf_ptr;

        // Register column family with index persistence manager
        if (index_persistence_manager_) {
            auto register_status = index_persistence_manager_->RegisterColumnFamily(cf_id, descriptor.name);
            if (!register_status.ok()) {
                std::cerr << "Failed to register column family with index persistence: "
                         << register_status.ToString() << std::endl;
            }

            // Load any persisted indexes for this column family
            auto load_status = cf_ptr->LoadIndexes(this);
            if (!load_status.ok()) {
                std::cerr << "Failed to load persisted indexes: "
                         << load_status.ToString() << std::endl;
            }
        }

        // Create handle (we'll create a simple handle implementation)
        *handle = new ColumnFamilyHandle(descriptor.name, cf_id);

        // Configure advanced features from capabilities
        if (global_advanced_features_manager) {
            auto config_status = global_advanced_features_manager->ConfigureFromCapabilities(
                descriptor.name,
                descriptor.options.capabilities);
            if (!config_status.ok()) {
                // Log warning but don't fail column family creation
                if (global_logger) {
                    global_logger->warn("CreateColumnFamily",
                        "Failed to configure advanced features for " + descriptor.name +
                        ": " + config_status.ToString());
                }
            }
        }

        return Status::OK();
    }
    
    Status DropColumnFamily(ColumnFamilyHandle* handle) override {
        return Status::NotImplemented("DropColumnFamily not implemented");
    }
    
    std::vector<std::string> ListColumnFamilies() const override {
        std::lock_guard<std::mutex> lock(cf_mutex_);
        std::vector<std::string> names;
        names.reserve(column_families_.size());
        for (const auto& pair : column_families_) {
            names.push_back(pair.first);
        }
        return names;
    }
    
    // CF-specific operations
    Status Put(const WriteOptions& options, ColumnFamilyHandle* cf, std::shared_ptr<Record> record) override {
        return Status::NotImplemented("CF Put not implemented");
    }
    
    Status Get(const ReadOptions& options, ColumnFamilyHandle* cf, const Key& key, std::shared_ptr<Record>* record) override {                                                                                                                        
        return Status::NotImplemented("CF Get not implemented");
    }
    
    Status Delete(const WriteOptions& options, ColumnFamilyHandle* cf, const Key& key) override {
        return Status::NotImplemented("CF Delete not implemented");
    }
    
    // Multi-Get
    Status MultiGet(const ReadOptions& options, const std::vector<Key>& keys, std::vector<std::shared_ptr<Record>>* records) override {                                                                                                               
        return Status::NotImplemented("MultiGet not implemented");
    }
    
    Status MultiGet(const ReadOptions& options, ColumnFamilyHandle* cf, const std::vector<Key>& keys, std::vector<std::shared_ptr<Record>>* records) override {                                                                                       
        return Status::NotImplemented("CF MultiGet not implemented");
    }
    
    // Delete Range
    Status DeleteRange(const WriteOptions& options, const Key& begin_key, const Key& end_key) override {
        return Status::NotImplemented("DeleteRange not implemented");
    }
    
    Status DeleteRange(const WriteOptions& options, ColumnFamilyHandle* cf, const Key& begin_key, const Key& end_key) override {                                                                                                                      
        return Status::NotImplemented("CF DeleteRange not implemented");
    }

    // Scanning
    Status NewIterator(const ReadOptions& options, const KeyRange& range, std::unique_ptr<Iterator>* iterator) override {
        std::lock_guard<std::mutex> lock(cf_mutex_);

        // For now, use the default column family (assuming it's the first one)
        if (column_families_.empty()) {
            return Status::InvalidArgument("No column families available");
        }

        // Get the first column family (simplified - in practice would need column family selection)
        auto it = column_families_.begin();
        auto* cf_info = it->second.get();

        // Create LSM-backed iterator
        auto lsm_iterator = std::make_unique<LSMBatchIterator>(
            lsm_tree_.get(), cf_info->id, cf_info->schema);

        *iterator = std::move(lsm_iterator);
        return Status::OK();
    }

    // Scanning with specific column family
    Status NewIterator(const std::string& table_name, const ReadOptions& options, const KeyRange& range, std::unique_ptr<Iterator>* iterator) override {
        std::lock_guard<std::mutex> lock(cf_mutex_);

        // Find the specified column family
        auto it = column_families_.find(table_name);
        if (it == column_families_.end()) {
            return Status::InvalidArgument("Column family '" + table_name + "' does not exist");
        }

        auto* cf_info = it->second.get();

        // Create LSM-backed iterator
        auto lsm_iterator = std::make_unique<LSMBatchIterator>(
            lsm_tree_.get(), cf_info->id, cf_info->schema);

        *iterator = std::move(lsm_iterator);
        return Status::OK();
    }

    // Arrow integration (protected method called by arrow_api factory)
    Status CreateRecordBatchReader(
        const std::string& table_name,
        const std::vector<std::string>& projection,
        const std::vector<ColumnPredicate>& predicates,
        std::shared_ptr<::arrow::RecordBatchReader>* reader) override {

        std::lock_guard<std::mutex> lock(cf_mutex_);

        // Find the column family (table)
        auto it = column_families_.find(table_name);
        if (it == column_families_.end()) {
            return Status::InvalidArgument("Table '" + table_name + "' does not exist");
        }

        auto* cf_info = it->second.get();

        // Get schema from table
        auto schema = cf_info->schema;
        if (!schema) {
            return Status::InvalidArgument("Table '" + table_name + "' has no schema");
        }

        // Create fully initialized MarbleRecordBatchReader
        // NOTE: We can't use shared_from_this() since SimpleMarbleDB doesn't inherit from enable_shared_from_this
        // The reader takes a raw pointer wrapped in shared_ptr with custom deleter (no-op)
        auto marble_reader = std::make_shared<marble::arrow_api::MarbleRecordBatchReader>(
            std::shared_ptr<MarbleDB>(this, [](MarbleDB*){}),  // Non-owning shared_ptr
            table_name, projection, predicates);

        *reader = marble_reader;
        return Status::OK();
    }

    // Get SSTables (internal protected method for Arrow integration)
    Status GetSSTablesInternal(
        const std::string& table_name,
        std::vector<std::vector<std::shared_ptr<SSTable>>>* sstables) override {

        std::lock_guard<std::mutex> lock(cf_mutex_);

        // Find the column family
        auto it = column_families_.find(table_name);
        if (it == column_families_.end()) {
            return Status::InvalidArgument("Table '" + table_name + "' does not exist");
        }

        // Get SSTables from LSM tree
        if (lsm_tree_) {
            std::shared_ptr<Version> version;
            auto status = lsm_tree_->GetCurrentVersion(&version);
            if (!status.ok()) {
                return status;
            }

            // Get table info for key range filtering
            auto* cf_info = it->second.get();
            uint32_t table_id = cf_info->id;

            // Calculate key range for this table
            // Keys are encoded as: [table_id: 16 bits][batch_id: 48 bits]
            uint64_t min_key = EncodeBatchKey(table_id, 0);
            uint64_t max_key = EncodeBatchKey(table_id + 1, 0) - 1;

            // Convert Version's SSTable levels to output format with filtering
            const auto& all_levels = version->GetAllLevels();
            sstables->resize(all_levels.size());

            for (size_t level = 0; level < all_levels.size(); ++level) {
                for (const auto& sstable : all_levels[level]) {
                    const auto& meta = sstable->GetMetadata();

                    // Only include SSTable if key range overlaps with table's range
                    if (meta.max_key >= min_key && meta.min_key <= max_key) {
                        (*sstables)[level].push_back(sstable);
                    }
                }
            }

            return Status::OK();
        }

        // No LSM tree - return empty
        sstables->clear();
        return Status::OK();
    }

    // Get table schema (internal protected method for Arrow integration)
    Status GetTableSchemaInternal(
        const std::string& table_name,
        std::shared_ptr<::arrow::Schema>* schema) override {

        std::lock_guard<std::mutex> lock(cf_mutex_);

        // Find the column family (table)
        auto it = column_families_.find(table_name);
        if (it == column_families_.end()) {
            return Status::InvalidArgument("Table '" + table_name + "' does not exist");
        }

        // Get schema from column family info
        auto* cf_info = it->second.get();
        if (!cf_info || !cf_info->schema) {
            return Status::InvalidArgument("Table '" + table_name + "' has no schema");
        }

        *schema = cf_info->schema;
        return Status::OK();
    }

    // Maintenance operations
    Status Flush() override {
        std::lock_guard<std::mutex> lock(cf_mutex_);

        // Flush all column families' put buffers
        for (const auto& cf_pair : column_families_) {
            auto* cf_info = cf_pair.second.get();
            auto status = cf_info->FlushPutBuffer(this);
            if (!status.ok()) {
                return status;
            }
        }

        // Flush LSM tree to create SSTables from memtable
        auto lsm_status = lsm_tree_->Flush();
        if (!lsm_status.ok()) {
            return lsm_status;
        }

        // Wait for background flush to complete (flush is asynchronous)
        auto wait_status = lsm_tree_->WaitForBackgroundTasks();
        if (!wait_status.ok()) {
            return wait_status;
        }

        return Status::OK();
    }

    Status CompactRange(const KeyRange& range) override {
        return Status::NotImplemented("CompactRange not implemented");
    }

    Status Destroy() override {
        return Status::NotImplemented("Destroy not implemented");
    }

    // Checkpoint operations
    Status CreateCheckpoint(const std::string& checkpoint_path) override {
        return Status::NotImplemented("CreateCheckpoint not implemented");
    }

    Status RestoreFromCheckpoint(const std::string& checkpoint_path) override {
        return Status::NotImplemented("RestoreFromCheckpoint not implemented");
    }

    Status GetCheckpointMetadata(std::string* metadata) const override {
        return Status::NotImplemented("GetCheckpointMetadata not implemented");
    }

    // Property access
    std::string GetProperty(const std::string& property) const override {
        return ""; // Return empty string for unimplemented properties
    }

    Status GetApproximateSizes(const std::vector<KeyRange>& ranges,
                              std::vector<uint64_t>* sizes) const override {
        return Status::NotImplemented("GetApproximateSizes not implemented");
    }

    // Streaming interfaces
    Status CreateStream(const std::string& stream_name,
                       std::unique_ptr<Stream>* stream) override {
        return Status::NotImplemented("CreateStream not implemented");
    }

    Status GetStream(const std::string& stream_name,
                    std::unique_ptr<Stream>* stream) override {
        return Status::NotImplemented("GetStream not implemented");
    }

    // Monitoring and metrics (production features)
    std::shared_ptr<MetricsCollector> GetMetricsCollector() const override {
        // Return a simple metrics collector for demo purposes
        static std::shared_ptr<MetricsCollector> collector = std::make_shared<MetricsCollector>();
        return collector;
    }

    std::string ExportMetricsPrometheus() const override {
        auto collector = GetMetricsCollector();
        return collector->exportPrometheus();
    }

    std::string ExportMetricsJSON() const override {
        auto collector = GetMetricsCollector();
        return collector->exportJSON();
    }

    std::unordered_map<std::string, bool> GetHealthStatus() const override {
        return {{"storage", true}, {"memory", true}, {"disk", true}};
    }

    StatusWithMetrics PerformHealthCheck() const override {
        return StatusWithMetrics::OK();
    }

    std::string GetSystemInfo() const override {
        return "SimpleMarbleDB v1.0 - In-memory implementation for testing";
    }

    // Transaction support (MVCC with snapshot isolation)
    Status BeginTransaction(const TransactionOptions& options,
                          DBTransaction** txn) override {
        if (!mvcc_manager_) {
            return Status::NotImplemented("MVCC not initialized");
        }

        return CreateMVCCTransaction(mvcc_manager_, lsm_tree_.get(),
                                   wal_manager_.get(), options, txn);
    }

    Status BeginTransaction(DBTransaction** txn) override {
        TransactionOptions default_options;
        return BeginTransaction(default_options, txn);
    }

private:
    std::string path_;
    std::unique_ptr<WalManager> wal_manager_;
    MVCCManager* mvcc_manager_;
    std::unique_ptr<IndexPersistenceManager> index_persistence_manager_;
};

// Convert JSON string to Arrow Schema
Status JsonToArrowSchema(const std::string& schema_json, std::shared_ptr<arrow::Schema>* schema) {
    try {
        auto json = nlohmann::json::parse(schema_json);
        std::vector<std::shared_ptr<arrow::Field>> fields;

        for (const auto& field_json : json["fields"]) {
            std::string name = field_json["name"];
            std::string type_str = field_json["type"];
            bool nullable = field_json.value("nullable", false);

            std::shared_ptr<arrow::DataType> arrow_type;
            if (type_str == "int64") arrow_type = arrow::int64();
            else if (type_str == "int32") arrow_type = arrow::int32();
            else if (type_str == "float64") arrow_type = arrow::float64();
            else if (type_str == "float32") arrow_type = arrow::float32();
            else if (type_str == "string" || type_str == "utf8") arrow_type = arrow::utf8();
            else if (type_str == "bool" || type_str == "boolean") arrow_type = arrow::boolean();
            else if (type_str == "timestamp") arrow_type = arrow::timestamp(arrow::TimeUnit::MICRO);
            else return Status::InvalidArgument("Unsupported type: " + type_str);

            fields.push_back(arrow::field(name, arrow_type, nullable));
        }

        *schema = arrow::schema(fields);
        return Status::OK();
    } catch (const std::exception& e) {
        return Status::InvalidArgument("Invalid JSON schema: " + std::string(e.what()));
    }
}

// Convert Arrow Schema to JSON string
Status ArrowSchemaToJson(const std::shared_ptr<arrow::Schema>& schema, std::string* schema_json) {
    try {
        nlohmann::json json_schema;
        json_schema["fields"] = nlohmann::json::array();

        for (int i = 0; i < schema->num_fields(); ++i) {
            const auto& field = schema->field(i);
            nlohmann::json field_json;
            field_json["name"] = field->name();
            field_json["nullable"] = field->nullable();

            // Convert Arrow type to string
            auto type = field->type();
            if (type->Equals(arrow::int64())) field_json["type"] = "int64";
            else if (type->Equals(arrow::int32())) field_json["type"] = "int32";
            else if (type->Equals(arrow::float64())) field_json["type"] = "float64";
            else if (type->Equals(arrow::float32())) field_json["type"] = "float32";
            else if (type->Equals(arrow::utf8())) field_json["type"] = "string";
            else if (type->Equals(arrow::boolean())) field_json["type"] = "boolean";
            else if (type->id() == arrow::Type::TIMESTAMP) field_json["type"] = "timestamp";
            else field_json["type"] = "unknown";

            json_schema["fields"].push_back(field_json);
        }

        *schema_json = json_schema.dump(2);
        return Status::OK();
    } catch (const std::exception& e) {
        return Status::InternalError("Failed to convert schema to JSON: " + std::string(e.what()));
    }
}

// Convert JSON record to Arrow RecordBatch
Status JsonToArrowRecord(const std::string& record_json, const std::shared_ptr<arrow::Schema>& schema,
                        std::shared_ptr<arrow::RecordBatch>* batch) {
    try {
        auto json = nlohmann::json::parse(record_json);
        std::vector<std::shared_ptr<arrow::Array>> arrays;

        for (int i = 0; i < schema->num_fields(); ++i) {
            const auto& field = schema->field(i);
            std::string field_name = field->name();

            if (json.contains(field_name)) {
                // Create array with single value
                auto value = json[field_name];
                // This is simplified - in practice would need proper type conversion
                // For now, assume string values
                arrow::StringBuilder builder;
                builder.Append(value.dump());
                std::shared_ptr<arrow::Array> array;
                builder.Finish(&array);
                arrays.push_back(array);
            } else {
                // Null value
                auto array = arrow::MakeArrayOfNull(field->type(), 1).ValueOrDie();
                arrays.push_back(array);
            }
        }

        *batch = arrow::RecordBatch::Make(schema, 1, arrays);
        return Status::OK();
    } catch (const std::exception& e) {
        return Status::InvalidArgument("Invalid JSON record: " + std::string(e.what()));
    }
}

// Convert Arrow RecordBatch to JSON string
Status ArrowRecordBatchToJson(const std::shared_ptr<arrow::RecordBatch>& batch, std::string* json_str) {
    try {
        nlohmann::json json_array = nlohmann::json::array();

        for (int64_t row = 0; row < batch->num_rows(); ++row) {
            nlohmann::json json_record;

            for (int col = 0; col < batch->num_columns(); ++col) {
                const auto& column = batch->column(col);
                const auto& field = batch->schema()->field(col);
                std::string field_name = field->name();

                if (column->IsValid(row)) {
                    // Extract value based on type
                    auto type_id = column->type()->id();
                    if (type_id == arrow::Type::INT64) {
                        auto int_array = std::static_pointer_cast<arrow::Int64Array>(column);
                        json_record[field_name] = int_array->Value(row);
                    } else if (type_id == arrow::Type::STRING || type_id == arrow::Type::LARGE_STRING) {
                        auto str_array = std::static_pointer_cast<arrow::StringArray>(column);
                        json_record[field_name] = str_array->GetString(row);
                    } else if (type_id == arrow::Type::DOUBLE) {
                        auto double_array = std::static_pointer_cast<arrow::DoubleArray>(column);
                        json_record[field_name] = double_array->Value(row);
                    } else if (type_id == arrow::Type::BOOL) {
                        auto bool_array = std::static_pointer_cast<arrow::BooleanArray>(column);
                        json_record[field_name] = bool_array->Value(row);
                    } else {
                        json_record[field_name] = "unsupported_type";
                    }
                } else {
                    json_record[field_name] = nullptr;
                }
            }

            json_array.push_back(json_record);
        }

        *json_str = json_array.dump(2);
        return Status::OK();
    } catch (const std::exception& e) {
        return Status::InternalError("Failed to convert RecordBatch to JSON: " + std::string(e.what()));
    }
}

} // anonymous namespace

//==============================================================================
// Database Management API Implementation
//==============================================================================

Status CreateDatabase(const std::string& path, std::unique_ptr<MarbleDB>* db) {
    try {
        *db = std::make_unique<SimpleMarbleDB>(path);
        return Status::OK();
    } catch (const std::exception& e) {
        return Status::InternalError("Failed to create database: " + std::string(e.what()));
    }
}

Status OpenDatabase(const std::string& path, std::unique_ptr<MarbleDB>* db) {
    try {
        return CreateDatabase(path, db);
    } catch (const std::exception& e) {
        return Status::InternalError("Failed to open database: " + std::string(e.what()));
    }
}

void CloseDatabase(std::unique_ptr<MarbleDB>* db) {
    db->reset();
}

//==============================================================================
// MarbleDB Static Factory Method
//==============================================================================

// Static factory method for MarbleDB class (required by header declaration)
Status MarbleDB::Open(const DBOptions& options,
                     std::shared_ptr<Schema> schema,
                     std::unique_ptr<MarbleDB>* db) {
    // Delegate to the existing OpenDatabase function
    // Note: schema parameter is ignored for now (SimpleMarbleDB is schema-agnostic)
    try {
        *db = std::make_unique<SimpleMarbleDB>(options.db_path);
        return Status::OK();
    } catch (const std::exception& e) {
        return Status::InternalError("Failed to open MarbleDB: " + std::string(e.what()));
    }
}

//==============================================================================
// Table Management API Implementation
//==============================================================================

Status CreateTable(MarbleDB* db, const std::string& table_name, const std::string& schema_json) {
    if (!db || table_name.empty()) {
        return Status::InvalidArgument("Invalid database handle or table name");
    }

    try {
        std::shared_ptr<arrow::Schema> schema;
        ARROW_RETURN_NOT_OK(JsonToArrowSchema(schema_json, &schema));

        TableSchema table_schema(table_name, schema);
        return db->CreateTable(table_schema);
    } catch (const std::exception& e) {
        return Status::InternalError("Failed to create table: " + std::string(e.what()));
    }
}

Status DropTable(MarbleDB* db, const std::string& table_name) {
    if (!db || table_name.empty()) {
        return Status::InvalidArgument("Invalid database handle or table name");
    }

    try {
        // This would need to be implemented in the MarbleDB class
        // For now, return not implemented
        return Status::NotImplemented("DropTable not yet implemented");
    } catch (const std::exception& e) {
        return Status::InternalError("Failed to drop table: " + std::string(e.what()));
    }
}

Status ListTables(MarbleDB* db, std::vector<std::string>* table_names) {
    if (!db || !table_names) {
        return Status::InvalidArgument("Invalid database handle or output parameter");
    }

    try {
        // This would need to be implemented in the MarbleDB class
        // For now, return empty list
        table_names->clear();
        return Status::OK();
    } catch (const std::exception& e) {
        return Status::InternalError("Failed to list tables: " + std::string(e.what()));
    }
}

Status GetTableSchema(MarbleDB* db, const std::string& table_name, std::string* schema_json) {
    if (!db || table_name.empty() || !schema_json) {
        return Status::InvalidArgument("Invalid parameters");
    }

    try {
        // This would need to be implemented in the MarbleDB class
        // For now, return empty schema
        *schema_json = "{}";
        return Status::OK();
    } catch (const std::exception& e) {
        return Status::InternalError("Failed to get table schema: " + std::string(e.what()));
    }
}

//==============================================================================
// Data Ingestion API Implementation
//==============================================================================

Status InsertRecord(MarbleDB* db, const std::string& table_name, const std::string& record_json) {
    if (!db || table_name.empty() || record_json.empty()) {
        return Status::InvalidArgument("Invalid parameters");
    }

    try {
        // Get table schema (simplified)
        std::string schema_json;
        ARROW_RETURN_NOT_OK(GetTableSchema(db, table_name, &schema_json));

        std::shared_ptr<arrow::Schema> schema;
        ARROW_RETURN_NOT_OK(JsonToArrowSchema(schema_json, &schema));

        std::shared_ptr<arrow::RecordBatch> batch;
        ARROW_RETURN_NOT_OK(JsonToArrowRecord(record_json, schema, &batch));

        // Insert the batch
        return db->InsertBatch(table_name, batch);
    } catch (const std::exception& e) {
        return Status::InternalError("Failed to insert record: " + std::string(e.what()));
    }
}

Status InsertRecordsBatch(MarbleDB* db, const std::string& table_name, const std::string& records_json) {
    if (!db || table_name.empty() || records_json.empty()) {
        return Status::InvalidArgument("Invalid parameters");
    }

    try {
        // This would parse JSON array and create larger RecordBatch
        // For now, simplified to single record
        return InsertRecord(db, table_name, records_json);
    } catch (const std::exception& e) {
        return Status::InternalError("Failed to insert records batch: " + std::string(e.what()));
    }
}

Status InsertArrowBatch(MarbleDB* db, const std::string& table_name,
                       const void* record_batch_data, size_t record_batch_size) {
    if (!db || table_name.empty() || !record_batch_data || record_batch_size == 0) {
        return Status::InvalidArgument("Invalid parameters");
    }

    try {
        std::shared_ptr<arrow::RecordBatch> batch;
        ARROW_RETURN_NOT_OK(marble::DeserializeArrowBatch(record_batch_data, record_batch_size, &batch));

        return db->InsertBatch(table_name, batch);
    } catch (const std::exception& e) {
        return Status::InternalError("Failed to insert Arrow batch: " + std::string(e.what()));
    }
}

//==============================================================================
// Query API Implementation
//==============================================================================

Status ExecuteQuery(MarbleDB* db, const std::string& query_sql, std::unique_ptr<QueryResult>* result) {
    if (!db || query_sql.empty() || !result) {
        return Status::InvalidArgument("Invalid parameters");
    }

    try {
        // Parse SQL query (simplified - would need real SQL parser)
        // For now, assume simple SELECT * FROM table format

        std::string table_name;
        size_t from_pos = query_sql.find("FROM");
        if (from_pos != std::string::npos) {
            size_t table_start = from_pos + 4;
            while (table_start < query_sql.size() && isspace(query_sql[table_start])) table_start++;
            size_t table_end = table_start;
            while (table_end < query_sql.size() && !isspace(query_sql[table_end])) table_end++;
            table_name = query_sql.substr(table_start, table_end - table_start);
        }

        if (table_name.empty()) {
            return Status::InvalidArgument("Could not parse table name from query");
        }

        // Execute scan
        return db->ScanTable(table_name, result);
    } catch (const std::exception& e) {
        return Status::InternalError("Failed to execute query: " + std::string(e.what()));
    }
}

Status ExecuteQueryWithOptions(MarbleDB* db, const std::string& query_sql,
                              const std::string& options_json, std::unique_ptr<QueryResult>* result) {
    // Simplified - ignore options for now
    return ExecuteQuery(db, query_sql, result);
}

//==============================================================================
// Query Result API Implementation
//==============================================================================

Status QueryResultHasNext(QueryResult* result, bool* has_next) {
    if (!result || !has_next) {
        return Status::InvalidArgument("Invalid parameters");
    }

    try {
        *has_next = result->HasNext();
        return Status::OK();
    } catch (const std::exception& e) {
        return Status::InternalError("Failed to check if result has next: " + std::string(e.what()));
    }
}

Status QueryResultNextJson(QueryResult* result, std::string* batch_json) {
    if (!result || !batch_json) {
        return Status::InvalidArgument("Invalid parameters");
    }

    try {
        std::shared_ptr<arrow::RecordBatch> batch;
        ARROW_RETURN_NOT_OK(result->Next(&batch));

        return ArrowRecordBatchToJson(batch, batch_json);
    } catch (const std::exception& e) {
        return Status::InternalError("Failed to get next result batch as JSON: " + std::string(e.what()));
    }
}

Status QueryResultNextArrow(QueryResult* result, void** arrow_data, size_t* arrow_size) {
    if (!result || !arrow_data || !arrow_size) {
        return Status::InvalidArgument("Invalid parameters");
    }

    try {
        std::shared_ptr<arrow::RecordBatch> batch;
        ARROW_RETURN_NOT_OK(result->Next(&batch));

        std::string serialized_data;
        ARROW_RETURN_NOT_OK(marble::SerializeArrowBatch(batch, &serialized_data));

        // Allocate memory for the caller (they must free it with FreeArrowData)
        void* data_copy = malloc(serialized_data.size());
        if (!data_copy) {
            return Status::InternalError("Failed to allocate memory for Arrow data");
        }

        memcpy(data_copy, serialized_data.data(), serialized_data.size());
        *arrow_data = data_copy;
        *arrow_size = serialized_data.size();

        return Status::OK();
    } catch (const std::exception& e) {
        return Status::InternalError("Failed to get next result batch as Arrow: " + std::string(e.what()));
    }
}

void FreeArrowData(void* arrow_data) {
    if (arrow_data) {
        free(arrow_data);
    }
}

Status QueryResultGetSchema(QueryResult* result, std::string* schema_json) {
    if (!result || !schema_json) {
        return Status::InvalidArgument("Invalid parameters");
    }

    try {
        auto schema = result->schema();
        return ArrowSchemaToJson(schema, schema_json);
    } catch (const std::exception& e) {
        return Status::InternalError("Failed to get result schema: " + std::string(e.what()));
    }
}

Status QueryResultGetRowCount(QueryResult* result, int64_t* row_count) {
    if (!result || !row_count) {
        return Status::InvalidArgument("Invalid parameters");
    }

    try {
        *row_count = result->num_rows();
        return Status::OK();
    } catch (const std::exception& e) {
        return Status::InternalError("Failed to get row count: " + std::string(e.what()));
    }
}

void CloseQueryResult(std::unique_ptr<QueryResult>* result) {
    result->reset();
}

//==============================================================================
// Transaction API Implementation
//==============================================================================

Status BeginTransaction(MarbleDB* db, uint64_t* transaction_id) {
    if (!db || !transaction_id) {
        return Status::InvalidArgument("Invalid parameters");
    }

    try {
        // This would need to be implemented in the MarbleDB class
        // For now, return not implemented
        *transaction_id = 1; // Dummy ID
        return Status::NotImplemented("Transactions not yet implemented");
    } catch (const std::exception& e) {
        return Status::InternalError("Failed to begin transaction: " + std::string(e.what()));
    }
}

Status CommitTransaction(MarbleDB* db, uint64_t transaction_id) {
    if (!db) {
        return Status::InvalidArgument("Invalid database handle");
    }

    try {
        // This would need to be implemented in the MarbleDB class
        return Status::NotImplemented("Transactions not yet implemented");
    } catch (const std::exception& e) {
        return Status::InternalError("Failed to commit transaction: " + std::string(e.what()));
    }
}

Status RollbackTransaction(MarbleDB* db, uint64_t transaction_id) {
    if (!db) {
        return Status::InvalidArgument("Invalid database handle");
    }

    try {
        // This would need to be implemented in the MarbleDB class
        return Status::NotImplemented("Transactions not yet implemented");
    } catch (const std::exception& e) {
        return Status::InternalError("Failed to rollback transaction: " + std::string(e.what()));
    }
}

//==============================================================================
// Administrative API Implementation
//==============================================================================

Status GetDatabaseStats(MarbleDB* db, std::string* stats_json) {
    if (!db || !stats_json) {
        return Status::InvalidArgument("Invalid parameters");
    }

    try {
        // Create basic stats JSON
        nlohmann::json stats;
        stats["status"] = "operational";
        stats["tables"] = 0; // Would need to implement actual counting
        stats["total_rows"] = 0; // Would need to implement actual counting

        *stats_json = stats.dump(2);
        return Status::OK();
    } catch (const std::exception& e) {
        return Status::InternalError("Failed to get database stats: " + std::string(e.what()));
    }
}

Status OptimizeDatabase(MarbleDB* db, const std::string& options_json) {
    if (!db) {
        return Status::InvalidArgument("Invalid database handle");
    }

    try {
        // This would trigger compaction and optimization
        return Status::NotImplemented("Database optimization not yet implemented");
    } catch (const std::exception& e) {
        return Status::InternalError("Failed to optimize database: " + std::string(e.what()));
    }
}

Status BackupDatabase(MarbleDB* db, const std::string& backup_path) {
    if (!db || backup_path.empty()) {
        return Status::InvalidArgument("Invalid parameters");
    }

    try {
        // This would create a backup of the database
        return Status::NotImplemented("Database backup not yet implemented");
    } catch (const std::exception& e) {
        return Status::InternalError("Failed to backup database: " + std::string(e.what()));
    }
}

Status RestoreDatabase(MarbleDB* db, const std::string& backup_path) {
    if (!db || backup_path.empty()) {
        return Status::InvalidArgument("Invalid parameters");
    }

    try {
        // This would restore database from backup
        return Status::NotImplemented("Database restore not yet implemented");
    } catch (const std::exception& e) {
        return Status::InternalError("Failed to restore database: " + std::string(e.what()));
    }
}

//==============================================================================
// Configuration API Implementation
//==============================================================================

Status SetConfig(MarbleDB* db, const std::string& key, const std::string& value) {
    if (!db || key.empty()) {
        return Status::InvalidArgument("Invalid parameters");
    }

    try {
        // This would set configuration options
        return Status::NotImplemented("Configuration not yet implemented");
    } catch (const std::exception& e) {
        return Status::InternalError("Failed to set config: " + std::string(e.what()));
    }
}

Status GetConfig(MarbleDB* db, const std::string& key, std::string* value) {
    if (!db || key.empty() || !value) {
        return Status::InvalidArgument("Invalid parameters");
    }

    try {
        // This would get configuration options
        *value = ""; // Default empty
        return Status::NotImplemented("Configuration not yet implemented");
    } catch (const std::exception& e) {
        return Status::InternalError("Failed to get config: " + std::string(e.what()));
    }
}

//==============================================================================
// Error Handling Implementation
//==============================================================================

Status GetLastError(MarbleDB* db, std::string* error_msg) {
    if (!error_msg) {
        return Status::InvalidArgument("Invalid output parameter");
    }

    try {
        // This would return the last error message
        *error_msg = "No error"; // Default
        return Status::OK();
    } catch (const std::exception& e) {
        return Status::InternalError("Failed to get last error: " + std::string(e.what()));
    }
}

void ClearLastError(MarbleDB* db) {
    // Clear any stored error state
}

//==============================================================================
// Version and Information Implementation
//==============================================================================

Status GetVersion(std::string* version) {
    if (!version) {
        return Status::InvalidArgument("Invalid output parameter");
    }

    try {
        *version = "0.1.0"; // Version would be set during build
        return Status::OK();
    } catch (const std::exception& e) {
        return Status::InternalError("Failed to get version: " + std::string(e.what()));
    }
}

Status GetBuildInfo(std::string* build_info) {
    if (!build_info) {
        return Status::InvalidArgument("Invalid output parameter");
    }

    try {
        nlohmann::json info;
        info["version"] = "0.1.0";
        info["build_type"] = "development";
        info["features"] = {"arrow", "pushdown", "analytics"};

        *build_info = info.dump(2);
        return Status::OK();
    } catch (const std::exception& e) {
        return Status::InternalError("Failed to get build info: " + std::string(e.what()));
    }
}

} // namespace marble
