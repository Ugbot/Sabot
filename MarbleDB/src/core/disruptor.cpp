#include <marble/disruptor.h>
#include <chrono>
#include <thread>

namespace marble {

// Busy spin wait strategy implementation
Sequence BusySpinWaitStrategy::wait_for(Sequence sequence,
                                       Sequence cursor,
                                       const std::function<bool()>& is_available) {
    Sequence available_sequence = cursor;

    while (!is_available()) {
        // Busy spin - do nothing, just check again
        std::atomic_thread_fence(std::memory_order_acquire);
    }

    return available_sequence;
}

// Yielding wait strategy implementation
Sequence YieldingWaitStrategy::wait_for(Sequence sequence,
                                       Sequence cursor,
                                       const std::function<bool()>& is_available) {
    Sequence available_sequence = cursor;
    int counter = 100;  // Yield after 100 spins

    while (!is_available()) {
        std::atomic_thread_fence(std::memory_order_acquire);

        if (counter == 0) {
            std::this_thread::yield();
            counter = 100;
        } else {
            counter--;
        }
    }

    return available_sequence;
}

// Sleeping wait strategy implementation
Sequence SleepingWaitStrategy::wait_for(Sequence sequence,
                                       Sequence cursor,
                                       const std::function<bool()>& is_available) {
    Sequence available_sequence = cursor;

    while (!is_available()) {
        std::atomic_thread_fence(std::memory_order_acquire);

        if (retries_ > 100) {
            std::this_thread::sleep_for(std::chrono::microseconds(1));
        } else if (retries_ > 0) {
            std::this_thread::yield();
        }

        if (retries_ > 0) {
            retries_--;
        }
    }

    retries_ = 200;  // Reset for next wait
    return available_sequence;
}

void SleepingWaitStrategy::signal_all_when_blocking() {
    // Reset retries to wake up sleeping threads
    retries_ = 200;
}

// Factory function
std::unique_ptr<Stream> CreateDisruptorStream(size_t buffer_size) {
    return std::make_unique<DisruptorStream>(buffer_size);
}

} // namespace marble
