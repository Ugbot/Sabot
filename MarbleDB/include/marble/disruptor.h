#pragma once

#include <atomic>
#include <vector>
#include <memory>
#include <functional>
#include <thread>
#include <string>
#include <marble/status.h>
#include <marble/record.h>
#include <marble/stream.h>

namespace marble {

// Sequence number type for tracking positions in the ring buffer
using Sequence = uint64_t;

// Event interface for items stored in the disruptor
template<typename T>
class Event {
public:
    virtual ~Event() = default;

    // Get the data
    virtual T& data() = 0;
    virtual const T& data() const = 0;

    // Sequence number
    virtual Sequence sequence() const = 0;

    // Reset for reuse
    virtual void reset() = 0;
};

// Concrete event implementation
template<typename T>
class RingBufferEvent : public Event<T> {
public:
    RingBufferEvent() : sequence_(0) {}

    T& data() override { return data_; }
    const T& data() const override { return data_; }

    Sequence sequence() const override { return sequence_; }
    void set_sequence(Sequence seq) { sequence_ = seq; }

    void reset() override {
        // Reset the data to default state
        data_ = T{};
        sequence_ = 0;
    }

private:
    T data_;
    Sequence sequence_;
};

// Wait strategy interface
class WaitStrategy {
public:
    virtual ~WaitStrategy() = default;

    // Wait for the given sequence to be available
    virtual Sequence wait_for(Sequence sequence,
                             Sequence cursor,
                             const std::function<bool()>& is_available) = 0;

    // Signal that new sequences are available
    virtual void signal_all_when_blocking() = 0;
};

// Busy spin wait strategy (lowest latency, highest CPU usage)
class BusySpinWaitStrategy : public WaitStrategy {
public:
    Sequence wait_for(Sequence sequence,
                     Sequence cursor,
                     const std::function<bool()>& is_available) override;

    void signal_all_when_blocking() override {
        // No-op for busy spin
    }
};

// Yielding wait strategy (balance between latency and CPU usage)
class YieldingWaitStrategy : public WaitStrategy {
public:
    Sequence wait_for(Sequence sequence,
                     Sequence cursor,
                     const std::function<bool()>& is_available) override;

    void signal_all_when_blocking() override {
        // No-op for yielding
    }
};

// Sleeping wait strategy (lowest CPU usage, higher latency)
class SleepingWaitStrategy : public WaitStrategy {
public:
    Sequence wait_for(Sequence sequence,
                     Sequence cursor,
                     const std::function<bool()>& is_available) override;

    void signal_all_when_blocking() override;

private:
    int retries_ = 200;
};

// Sequence barrier for coordinating producers and consumers
class SequenceBarrier {
public:
    virtual ~SequenceBarrier() = default;

    // Wait for the given sequence
    virtual Sequence wait_for(Sequence sequence) = 0;

    // Get the current cursor value
    virtual Sequence cursor() const = 0;

    // Check if alert has been raised
    virtual bool is_alerted() const = 0;

    // Alert the barrier
    virtual void alert() = 0;

    // Clear alert
    virtual void clear_alert() = 0;
};

// Event processor interface
template<typename T>
class EventHandler {
public:
    virtual ~EventHandler() = default;

    // Process an event
    virtual void on_event(T& event, Sequence sequence, bool end_of_batch) = 0;

    // Called when processing starts
    virtual void on_start() {}

    // Called when processing stops
    virtual void on_shutdown() {}

    // Get the sequence this handler is up to
    virtual Sequence sequence() const = 0;
};

// Batch event processor for handling multiple events at once
template<typename T>
class BatchEventProcessor {
public:
    BatchEventProcessor(std::shared_ptr<SequenceBarrier> barrier,
                       std::shared_ptr<EventHandler<T>> handler)
        : barrier_(std::move(barrier))
        , handler_(std::move(handler))
        , running_(false) {}

    // Start processing
    void start() {
        if (running_.exchange(true)) return;

        handler_->on_start();
        processor_thread_ = std::thread([this]() {
            process_events();
        });
    }

    // Stop processing
    void stop() {
        running_.store(false);
        barrier_->alert();
        if (processor_thread_.joinable()) {
            processor_thread_.join();
        }
        handler_->on_shutdown();
    }

    // Get current sequence
    Sequence sequence() const {
        return handler_->sequence();
    }

private:
    void process_events() {
        Sequence next_sequence = handler_->sequence() + 1;

        while (running_.load(std::memory_order_acquire)) {
            Sequence available_sequence = barrier_->wait_for(next_sequence);

            while (next_sequence <= available_sequence) {
                bool end_of_batch = next_sequence == available_sequence;
                handler_->on_event(*event_, next_sequence, end_of_batch);
                next_sequence++;
            }

            handler_->sequence() = next_sequence - 1;
        }
    }

    std::shared_ptr<SequenceBarrier> barrier_;
    std::shared_ptr<EventHandler<T>> handler_;
    std::atomic<bool> running_;
    std::thread processor_thread_;
    // Note: event_ would need to be set up to point to the ring buffer
    RingBufferEvent<T>* event_ = nullptr;
};

// Main Disruptor class
template<typename T>
class Disruptor {
public:
    static constexpr Sequence INITIAL_CURSOR_VALUE = -1;

    Disruptor(size_t buffer_size,
              std::unique_ptr<WaitStrategy> wait_strategy = nullptr)
        : buffer_size_(buffer_size)
        , wait_strategy_(wait_strategy ? std::move(wait_strategy)
                                       : std::make_unique<BusySpinWaitStrategy>())
        , ring_buffer_(buffer_size)
        , cursor_(INITIAL_CURSOR_VALUE)
        , gating_sequences_() {

        // Initialize ring buffer
        for (size_t i = 0; i < buffer_size; ++i) {
            ring_buffer_[i] = std::make_unique<RingBufferEvent<T>>();
        }
    }

    ~Disruptor() {
        // Stop all processors
        for (auto& processor : event_processors_) {
            processor->stop();
        }
    }

    // Get event at sequence
    RingBufferEvent<T>* get(Sequence sequence) {
        return ring_buffer_[sequence & (buffer_size_ - 1)].get();
    }

    // Get current cursor value
    Sequence cursor() const {
        return cursor_.load(std::memory_order_acquire);
    }

    // Publish an event to the ring buffer
    void publish_event(std::function<void(T&, Sequence)> event_translator) {
        Sequence sequence = next();
        try {
            event_translator(get(sequence)->data(), sequence);
        } catch (...) {
            // Handle exception - in real implementation might need to handle rollback
            throw;
        }
        publish(sequence);
    }

    // Create event publisher
    class EventPublisher {
    public:
        EventPublisher(Disruptor<T>* disruptor) : disruptor_(disruptor) {}

        template<typename... Args>
        void publish_event(Args&&... args) {
            disruptor_->publish_event([&](T& event, Sequence sequence) {
                // Initialize event with args
                // This would need to be customized based on T
                event = T(std::forward<Args>(args)...);
            });
        }

    private:
        Disruptor<T>* disruptor_;
    };

    std::unique_ptr<EventPublisher> create_publisher() {
        return std::make_unique<EventPublisher>(this);
    }

    // Add a gating sequence (consumer sequence)
    void add_gating_sequence(std::shared_ptr<std::atomic<Sequence>> gating_sequence) {
        gating_sequences_.push_back(gating_sequence);
    }

    // Get minimum sequence from all gating sequences
    Sequence get_minimum_gating_sequence() const {
        Sequence minimum = std::numeric_limits<Sequence>::max();

        for (const auto& seq : gating_sequences_) {
            minimum = std::min(minimum, seq->load(std::memory_order_acquire));
        }

        return minimum;
    }

private:
    Sequence next() {
        Sequence current;
        Sequence next;

        do {
            current = cursor_.load(std::memory_order_acquire);
            next = current + 1;

            Sequence wrap_point = next - buffer_size_;
            Sequence cached_gating_sequence = get_minimum_gating_sequence();

            if (wrap_point > cached_gating_sequence) {
                // Wait for gating sequences to advance
                Sequence min_sequence;
                while ((min_sequence = get_minimum_gating_sequence()) < wrap_point) {
                    wait_strategy_->wait_for(wrap_point, min_sequence,
                                           [&]() { return get_minimum_gating_sequence() >= wrap_point; });
                }
            }
        } while (!cursor_.compare_exchange_weak(current, next,
                                               std::memory_order_acq_rel,
                                               std::memory_order_acquire));

        return next;
    }

    void publish(Sequence sequence) {
        get(sequence)->set_sequence(sequence);
        cursor_.store(sequence, std::memory_order_release);
        wait_strategy_->signal_all_when_blocking();
    }

    const size_t buffer_size_;
    std::unique_ptr<WaitStrategy> wait_strategy_;
    std::vector<std::unique_ptr<RingBufferEvent<T>>> ring_buffer_;
    std::atomic<Sequence> cursor_;
    std::vector<std::shared_ptr<std::atomic<Sequence>>> gating_sequences_;
    std::vector<std::unique_ptr<BatchEventProcessor<T>>> event_processors_;
};

// Disruptor-based streaming implementation
class DisruptorStream : public marble::Stream {
public:
    DisruptorStream(size_t buffer_size = 1024)
        : disruptor_(buffer_size)
        , publisher_(disruptor_.create_publisher())
        , consumer_sequence_(std::make_shared<std::atomic<Sequence>>(Disruptor<StreamEvent>::INITIAL_CURSOR_VALUE)) {

        // Add consumer sequence to disruptor
        disruptor_.add_gating_sequence(consumer_sequence_);
    }

    // Producer interface
    Status SendRecord(std::shared_ptr<Record> record) {
        publisher_->publish_event(StreamEvent{StreamEvent::Type::RECORD, record, {}, {}});
        return Status::OK();
    }

    Status SendRecords(const std::vector<std::shared_ptr<Record>>& records) {
        for (const auto& record : records) {
            auto status = SendRecord(record);
            if (!status.ok()) return status;
        }
        return Status::OK();
    }

    Status SendChange(const ChangeRecord& change) {
        publisher_->publish_event(StreamEvent{StreamEvent::Type::CHANGE, {}, change, {}});
        return Status::OK();
    }

    Status SendChanges(const std::vector<ChangeRecord>& changes) {
        for (const auto& change : changes) {
            auto status = SendChange(change);
            if (!status.ok()) return status;
        }
        return Status::OK();
    }

    Status SendRecordBatch(std::shared_ptr<arrow::RecordBatch> batch) {
        publisher_->publish_event(StreamEvent{StreamEvent::Type::BATCH, {}, {}, batch});
        return Status::OK();
    }

    // Consumer interface
    Status ReceiveRecord(std::shared_ptr<Record>* record, bool blocking = true) {
        Sequence available_sequence = consumer_sequence_->load(std::memory_order_acquire);
        Sequence next_sequence = available_sequence + 1;

        if (!blocking && next_sequence > get_cursor()) {
            return Status::NotFound("No data available");
        }

        // Wait for data (simplified - real implementation would use proper waiting)
        while (next_sequence > get_cursor()) {
            if (!blocking) return Status::NotFound("No data available");
            std::this_thread::yield();
        }

        auto event = disruptor_.get(next_sequence);
        if (event->data().type == StreamEvent::Type::RECORD && event->data().record) {
            *record = event->data().record;
            consumer_sequence_->store(next_sequence, std::memory_order_release);
            return Status::OK();
        }

        return Status::InvalidArgument("Event is not a record");
    }

    Status ReceiveRecords(std::vector<std::shared_ptr<Record>>* records,
                         size_t max_count = 0, bool blocking = true) {
        records->clear();

        if (max_count == 0) max_count = 100;  // Default batch size

        for (size_t i = 0; i < max_count; ++i) {
            std::shared_ptr<Record> record;
            auto status = ReceiveRecord(&record, blocking && i == 0);
            if (!status.ok()) {
                if (status.IsNotFound() && !records->empty()) {
                    return Status::OK();
                }
                return status;
            }
            records->push_back(record);
        }

        return Status::OK();
    }

    Status ReceiveChange(ChangeRecord* change, bool blocking = true) {
        Sequence available_sequence = consumer_sequence_->load(std::memory_order_acquire);
        Sequence next_sequence = available_sequence + 1;

        if (!blocking && next_sequence > get_cursor()) {
            return Status::NotFound("No change available");
        }

        while (next_sequence > get_cursor()) {
            if (!blocking) return Status::NotFound("No change available");
            std::this_thread::yield();
        }

        auto event = disruptor_.get(next_sequence);
        if (event->data().type == StreamEvent::Type::CHANGE) {
            *change = event->data().change_record;
            consumer_sequence_->store(next_sequence, std::memory_order_release);
            return Status::OK();
        }

        return Status::InvalidArgument("Event is not a change record");
    }

    Status ReceiveChanges(std::vector<ChangeRecord>* changes,
                         size_t max_count = 0, bool blocking = true) {
        changes->clear();

        if (max_count == 0) max_count = 100;

        for (size_t i = 0; i < max_count; ++i) {
            ChangeRecord change;
            auto status = ReceiveChange(&change, blocking && i == 0);
            if (!status.ok()) {
                if (status.IsNotFound() && !changes->empty()) {
                    return Status::OK();
                }
                return status;
            }
            changes->push_back(change);
        }

        return Status::OK();
    }

    Status ReceiveRecordBatch(std::shared_ptr<arrow::RecordBatch>* batch, bool blocking = true) {
        Sequence available_sequence = consumer_sequence_->load(std::memory_order_acquire);
        Sequence next_sequence = available_sequence + 1;

        if (!blocking && next_sequence > get_cursor()) {
            return Status::NotFound("No batch available");
        }

        while (next_sequence > get_cursor()) {
            if (!blocking) return Status::NotFound("No batch available");
            std::this_thread::yield();
        }

        auto event = disruptor_.get(next_sequence);
        if (event->data().type == StreamEvent::Type::BATCH && event->data().batch) {
            *batch = event->data().batch;
            consumer_sequence_->store(next_sequence, std::memory_order_release);
            return Status::OK();
        }

        return Status::InvalidArgument("Event is not a record batch");
    }

    // Control methods
    Status Flush() {
        // Disruptor is always flushed (events are immediately available)
        return Status::OK();
    }

    Status Close() {
        closed_.store(true, std::memory_order_release);
        return Status::OK();
    }

    // Status methods
    bool IsOpen() const {
        return !closed_.load(std::memory_order_acquire);
    }

    size_t GetPendingRecords() const {
        Sequence cursor = get_cursor();
        Sequence consumer = consumer_sequence_->load(std::memory_order_acquire);
        return cursor - consumer;
    }

    uint64_t GetLastSequenceNumber() const {
        return consumer_sequence_->load(std::memory_order_acquire);
    }

    // Metadata
    const std::string& GetName() const { return name_; }
    const StreamOptions& GetOptions() const { return options_; }

    void SetName(const std::string& name) { name_ = name; }

private:
    // Stream event that can hold different types of data
    struct StreamEvent {
        enum class Type { RECORD, CHANGE, BATCH };

        Type type;
        std::shared_ptr<Record> record;
        ChangeRecord change_record;
        std::shared_ptr<arrow::RecordBatch> batch;

        StreamEvent() : type(Type::RECORD) {}
        StreamEvent(Type t, std::shared_ptr<Record> r, ChangeRecord c, std::shared_ptr<arrow::RecordBatch> b)
            : type(t), record(std::move(r)), change_record(std::move(c)), batch(std::move(b)) {}
    };

    Sequence get_cursor() const {
        return disruptor_.cursor();
    }

    Disruptor<StreamEvent> disruptor_;
    std::unique_ptr<typename Disruptor<StreamEvent>::EventPublisher> publisher_;
    std::shared_ptr<std::atomic<Sequence>> consumer_sequence_;
    std::atomic<bool> closed_{false};
    std::string name_;
    StreamOptions options_;
};

// Factory functions for creating disruptor-based streams
std::unique_ptr<Stream> CreateDisruptorStream(size_t buffer_size = 1024);

} // namespace marble
