#include <marble/marble.h>
#include <iostream>
#include <thread>
#include <chrono>
#include <vector>
#include <memory>

using namespace marble;

// Simple SSTable mock for demonstration
class MockSSTable : public SSTable {
public:
    MockSSTable(const std::string& filename, uint64_t file_size, int level)
        : filename_(filename), file_size_(file_size), level_(level) {}

    const Metadata& GetMetadata() const override {
        // Create a simple mock key for demonstration
        class MockKey : public Key {
        public:
            explicit MockKey(const std::string& value) : value_(value) {}
            std::string ToString() const override { return value_; }
            int Compare(const Key& other) const override {
                const MockKey& other_mock = static_cast<const MockKey&>(other);
                return value_.compare(other_mock.value_);
            }
            arrow::Result<std::shared_ptr<arrow::Scalar>> ToArrowScalar() const override {
                return arrow::MakeScalar(value_);
            }
            std::shared_ptr<Key> Clone() const override {
                return std::make_shared<MockKey>(value_);
            }
            size_t Hash() const override { return std::hash<std::string>()(value_); }
            MockKey as_key_ref() const { return *this; }
        private:
            std::string value_;
        };

        static Metadata meta;
        meta.filename = filename_;
        meta.file_size = file_size_;
        meta.level = level_;
        meta.smallest_key = std::make_shared<MockKey>("key_000");
        meta.largest_key = std::make_shared<MockKey>("key_999");
        meta.num_entries = file_size_ / 100;  // Rough estimate
        return meta;
    }

    Status Get(const Key& key, std::shared_ptr<Record>* record) const override {
        return Status::NotImplemented("Mock SSTable");
    }

    std::unique_ptr<MemTable::Iterator> NewIterator() override {
        return nullptr;  // Not implemented for demo
    }

    std::unique_ptr<MemTable::Iterator> NewIterator(const std::vector<ColumnPredicate>& predicates) override {
        return nullptr;  // Not implemented for demo
    }

    bool KeyMayMatch(const Key& key) const override {
        return true;  // Always say yes for demo
    }

    Status GetBloomFilter(std::string* bloom_filter) const override {
        return Status::NotImplemented("Mock SSTable");
    }

    Status ReadRecordBatch(std::shared_ptr<arrow::RecordBatch>* batch) const override {
        return Status::NotImplemented("Mock SSTable");
    }

private:
    std::string filename_;
    uint64_t file_size_;
    int level_;
};

void demonstrateCompactionStrategy(const std::string& strategy_name,
                                 std::unique_ptr<CompactionStrategy> strategy) {
    std::cout << "\n=== Demonstrating " << strategy_name << " Strategy ===" << std::endl;
    std::cout << "Strategy Options: " << strategy->GetOptions() << std::endl;

    // Create mock LSM tree levels with SST files
    std::vector<std::vector<std::shared_ptr<SSTable>>> levels;

    // Level 0: Many small files (triggering L0 compaction)
    std::vector<std::shared_ptr<SSTable>> l0_files;
    for (int i = 0; i < 6; ++i) {  // More than typical max_l0_files
        l0_files.push_back(std::make_shared<MockSSTable>(
            "l0_file_" + std::to_string(i), 32 * 1024 * 1024, 0));  // 32MB each
    }
    levels.push_back(l0_files);

    // Level 1: Some files that might trigger compaction
    std::vector<std::shared_ptr<SSTable>> l1_files;
    for (int i = 0; i < 8; ++i) {
        l1_files.push_back(std::make_shared<MockSSTable>(
            "l1_file_" + std::to_string(i), 64 * 1024 * 1024, 1));  // 64MB each
    }
    levels.push_back(l1_files);

    // Level 2: Large files
    std::vector<std::shared_ptr<SSTable>> l2_files;
    for (int i = 0; i < 4; ++i) {
        l2_files.push_back(std::make_shared<MockSSTable>(
            "l2_file_" + std::to_string(i), 128 * 1024 * 1024, 2));  // 128MB each
    }
    levels.push_back(l2_files);

    // No immutable memtables for this demo
    std::vector<std::unique_ptr<ImmutableMemTable>> immutable_memtables;

    // Check what compactions are needed
    std::vector<CompactionTask> tasks;
    Status status = strategy->CheckCompactionNeeded(levels, immutable_memtables, &tasks);

    if (!status.ok()) {
        std::cout << "Error checking compaction: " << status.ToString() << std::endl;
        return;
    }

    std::cout << "Found " << tasks.size() << " compaction tasks:" << std::endl;

    for (size_t i = 0; i < tasks.size(); ++i) {
        const auto& task = tasks[i];
        std::cout << "  Task " << i << ": ";
        switch (task.type) {
            case CompactionTask::Type::kMinor:
                std::cout << "MINOR compaction (memtables -> L0)";
                break;
            case CompactionTask::Type::kMajor:
                std::cout << "MAJOR compaction (L" << task.source_level
                         << " -> L" << task.target_level << ")";
                break;
            case CompactionTask::Type::kTierMerge:
                std::cout << "TIER MERGE (L" << task.source_level
                         << " tier " << task.source_tier
                         << " -> tier " << task.target_tier << ")";
                break;
        }
        std::cout << " - " << task.input_tables.size() << " input files";
        if (!task.input_memtables.empty()) {
            std::cout << " + " << task.input_memtables.size() << " memtables";
        }
        std::cout << std::endl;
    }

    if (tasks.empty()) {
        std::cout << "  No compaction needed at this time." << std::endl;
    }
}

int main() {
    std::cout << "MarbleDB Compaction Strategies Demo" << std::endl;
    std::cout << "===================================" << std::endl;

    // Demonstrate Leveled Compaction Strategy
    {
        LeveledCompactionOptions leveled_options;
        leveled_options.max_l0_files = 4;  // Lower threshold for demo
        auto leveled_strategy = CreateLeveledCompactionStrategy(leveled_options);
        demonstrateCompactionStrategy("Leveled Compaction", std::move(leveled_strategy));
    }

    // Demonstrate Tiered Compaction Strategy
    {
        TieredCompactionOptions tiered_options;
        tiered_options.max_ssts_per_tier = 4;
        tiered_options.min_ssts_for_compaction = 6;  // Lower threshold for demo
        auto tiered_strategy = CreateTieredCompactionStrategy(tiered_options);
        demonstrateCompactionStrategy("Tiered Compaction", std::move(tiered_strategy));
    }

    // Demonstrate Universal Compaction Strategy
    {
        UniversalCompactionOptions universal_options;
        universal_options.min_merge_width = 2;
        universal_options.max_merge_width = 8;
        auto universal_strategy = CreateUniversalCompactionStrategy(universal_options);
        demonstrateCompactionStrategy("Universal Compaction", std::move(universal_strategy));
    }

    std::cout << "\n=== Compaction Strategy Comparison ===" << std::endl;
    std::cout << "Each strategy has different trade-offs:" << std::endl;
    std::cout << "• Leveled: Good for read-heavy workloads, predictable performance" << std::endl;
    std::cout << "• Tiered: Good for write-heavy workloads, better write amplification" << std::endl;
    std::cout << "• Universal: Adaptive strategy that balances read/write performance" << std::endl;

    std::cout << "\nCompaction strategies demo completed!" << std::endl;
    std::cout << "\nKey takeaways:" << std::endl;
    std::cout << "• Leveled compaction: Good for read-heavy workloads, predictable performance" << std::endl;
    std::cout << "• Tiered compaction: Good for write-heavy workloads, better write amplification" << std::endl;
    std::cout << "• Universal compaction: Adaptive strategy that balances read/write performance" << std::endl;
    std::cout << "• Each strategy has different trade-offs for space, read/write amplification" << std::endl;

    return 0;
}
