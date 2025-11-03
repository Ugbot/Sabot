/**
 * RangeIterator - Iterator over RecordBatch data with key range filtering
 *
 * Iterator implementation that scans through Arrow RecordBatches within a specified key range.
 * Supports bidirectional iteration, range filtering, and optional skipping index optimization.
 */

#pragma once

#include <memory>
#include <vector>
#include <arrow/api.h>
#include "marble/record.h"
#include "marble/skipping_index.h"
#include "marble/status.h"

namespace marble {

/**
 * @brief Iterator that scans through Arrow RecordBatches within a key range
 *
 * Supports:
 * - Range filtering (start/end bounds)
 * - Bidirectional iteration
 * - Skipping index for pruning batches
 * - Future: Bloom filters for existence checks (P0.3)
 */
class RangeIterator : public Iterator {
public:
    RangeIterator(const std::vector<std::shared_ptr<arrow::RecordBatch>>& data,
                  const KeyRange& range,
                  std::shared_ptr<arrow::Schema> schema,
                  std::shared_ptr<SkippingIndex> skipping_index = nullptr)
        : data_(data), range_(range), schema_(schema),
          skipping_index_(skipping_index),
          current_batch_(0), current_row_(0) {
        // Find first valid position
        SeekToStart();
    }

    bool Valid() const override {
        return current_batch_ < data_.size() &&
               current_row_ < static_cast<int64_t>(data_[current_batch_]->num_rows());
    }

    void Next() override {
        if (!Valid()) return;

        current_row_++;
        if (current_row_ >= static_cast<int64_t>(data_[current_batch_]->num_rows())) {
            current_batch_++;
            current_row_ = 0;
        }

        // Skip records that don't match the range
        while (Valid() && !InRange()) {
            current_row_++;
            if (current_row_ >= static_cast<int64_t>(data_[current_batch_]->num_rows())) {
                current_batch_++;
                current_row_ = 0;
            }
        }
    }

    std::shared_ptr<Key> key() const override {
        if (!Valid()) return nullptr;

        auto batch = data_[current_batch_];
        auto subject_col = std::static_pointer_cast<arrow::Int64Array>(batch->column(0));
        auto predicate_col = std::static_pointer_cast<arrow::Int64Array>(batch->column(1));
        auto object_col = std::static_pointer_cast<arrow::Int64Array>(batch->column(2));

        return std::make_shared<TripleKey>(
            subject_col->Value(current_row_),
            predicate_col->Value(current_row_),
            object_col->Value(current_row_)
        );
    }

    std::shared_ptr<Record> value() const override {
        if (!Valid()) return nullptr;
        return std::make_shared<SimpleRecord>(key(), data_[current_batch_], current_row_);
    }

    std::unique_ptr<RecordRef> value_ref() const override {
        if (!Valid()) return nullptr;
        return std::make_unique<SimpleRecordRef>(key(), data_[current_batch_], current_row_);
    }

    marble::Status status() const override {
        return marble::Status::OK();
    }

    void Seek(const Key& target) override {
        const TripleKey* target_key = dynamic_cast<const TripleKey*>(&target);
        if (!target_key) return;

        // Find the first record >= target
        for (size_t batch_idx = 0; batch_idx < data_.size(); ++batch_idx) {
            const auto& batch = data_[batch_idx];
            auto subject_col = std::static_pointer_cast<arrow::Int64Array>(batch->column(0));
            auto predicate_col = std::static_pointer_cast<arrow::Int64Array>(batch->column(1));
            auto object_col = std::static_pointer_cast<arrow::Int64Array>(batch->column(2));

            for (int64_t row_idx = 0; row_idx < static_cast<int64_t>(batch->num_rows()); ++row_idx) {
                TripleKey current_key(subject_col->Value(row_idx),
                                    predicate_col->Value(row_idx),
                                    object_col->Value(row_idx));

                if (current_key.Compare(*target_key) >= 0) {
                    current_batch_ = batch_idx;
                    current_row_ = row_idx;
                    return;
                }
            }
        }

        // If not found, go to end
        current_batch_ = data_.size();
        current_row_ = 0;
    }

    void SeekForPrev(const Key& target) override {
        const TripleKey* target_key = dynamic_cast<const TripleKey*>(&target);
        if (!target_key) return;

        // Find the last record <= target
        size_t found_batch = data_.size();
        int64_t found_row = -1;

        for (size_t batch_idx = 0; batch_idx < data_.size(); ++batch_idx) {
            const auto& batch = data_[batch_idx];
            auto subject_col = std::static_pointer_cast<arrow::Int64Array>(batch->column(0));
            auto predicate_col = std::static_pointer_cast<arrow::Int64Array>(batch->column(1));
            auto object_col = std::static_pointer_cast<arrow::Int64Array>(batch->column(2));

            for (int64_t row_idx = 0; row_idx < static_cast<int64_t>(batch->num_rows()); ++row_idx) {
                TripleKey current_key(subject_col->Value(row_idx),
                                    predicate_col->Value(row_idx),
                                    object_col->Value(row_idx));

                if (current_key.Compare(*target_key) <= 0) {
                    found_batch = batch_idx;
                    found_row = row_idx;
                } else if (found_row >= 0) {
                    // We've passed the target, stop
                    break;
                }
            }
        }

        if (found_row >= 0) {
            current_batch_ = found_batch;
            current_row_ = found_row;
        } else {
            // Not found, go to beginning
            current_batch_ = 0;
            current_row_ = 0;
        }
    }

    void SeekToFirst() {
        SeekToStart();
    }

    void SeekToLast() override {
        if (data_.empty()) {
            current_batch_ = 0;
            current_row_ = 0;
            return;
        }
        current_batch_ = data_.size() - 1;
        current_row_ = static_cast<int64_t>(data_[current_batch_]->num_rows()) - 1;

        // Move backwards to find last record in range
        while (Valid() && !InRange()) {
            Prev();
        }
    }

    void Prev() override {
        if (!Valid()) return;

        if (current_row_ > 0) {
            current_row_--;
        } else if (current_batch_ > 0) {
            current_batch_--;
            current_row_ = static_cast<int64_t>(data_[current_batch_]->num_rows()) - 1;
        } else {
            // At beginning, make invalid
            current_batch_ = data_.size();
            current_row_ = 0;
        }

        // Skip records that don't match the range
        while (Valid() && !InRange()) {
            if (current_row_ > 0) {
                current_row_--;
            } else if (current_batch_ > 0) {
                current_batch_--;
                current_row_ = static_cast<int64_t>(data_[current_batch_]->num_rows()) - 1;
            } else {
                current_batch_ = data_.size();
                current_row_ = 0;
                break;
            }
        }
    }

    std::unique_ptr<Iterator> Clone() const {
        RangeIterator* clone_raw = new RangeIterator(data_, range_, schema_, skipping_index_);
        clone_raw->current_batch_ = current_batch_;
        clone_raw->current_row_ = current_row_;
        return std::unique_ptr<Iterator>(clone_raw);
    }

    std::shared_ptr<arrow::RecordBatch> GetCurrentBatch() const {
        if (!Valid()) return nullptr;
        return data_[current_batch_];
    }

    int64_t GetCurrentRowInBatch() const {
        if (!Valid()) return -1;
        return current_row_;
    }

private:
    void SeekToStart() {
        current_batch_ = 0;
        current_row_ = 0;

        // Skip records that don't match the range
        while (Valid() && !InRange()) {
            current_row_++;
            if (current_row_ >= static_cast<int64_t>(data_[current_batch_]->num_rows())) {
                current_batch_++;
                current_row_ = 0;
            }
        }
    }

    // TODO: Re-add bloom filter check in P0.3
    // bool CanSkipWithBloomFilter() const { ... }

    bool InRange() const {
        if (!Valid()) return false;

        auto current_key = key();
        if (!current_key) return false;

        const TripleKey* triple_key = dynamic_cast<const TripleKey*>(current_key.get());
        if (!triple_key) return false;

        // Check start bound
        if (range_.start()) {
            const TripleKey* start_key = dynamic_cast<const TripleKey*>(range_.start().get());
            if (start_key) {
                int cmp = triple_key->Compare(*start_key);
                if (range_.start_inclusive()) {
                    if (cmp < 0) return false;
                } else {
                    if (cmp <= 0) return false;
                }
            }
        }

        // Check end bound
        if (range_.end()) {
            const TripleKey* end_key = dynamic_cast<const TripleKey*>(range_.end().get());
            if (end_key) {
                int cmp = triple_key->Compare(*end_key);
                if (range_.end_inclusive()) {
                    if (cmp > 0) return false;
                } else {
                    if (cmp >= 0) return false;
                }
            }
        }

        return true;
    }

    const std::vector<std::shared_ptr<arrow::RecordBatch>>& data_;
    KeyRange range_;
    std::shared_ptr<arrow::Schema> schema_;
    std::shared_ptr<SkippingIndex> skipping_index_;
    // TODO: Re-add in P0.3: std::shared_ptr<BloomFilter> bloom_filter_;
    size_t current_batch_;
    int64_t current_row_;
};

} // namespace marble
