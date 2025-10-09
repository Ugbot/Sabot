#include <marble/marble.h>
#include <iostream>
#include <thread>
#include <chrono>
#include <atomic>
#include <vector>
#include <memory>

using namespace marble;

// Simple demo event for disruptor
struct DemoEvent {
    int value;
    std::string message;
    std::chrono::steady_clock::time_point timestamp;
};

// Producer thread function - demonstrates disruptor event publishing
void producer_thread(Disruptor<DemoEvent>* disruptor, int producer_id, int num_events) {
    std::cout << "Producer " << producer_id << " starting, will send " << num_events << " events" << std::endl;

    auto publisher = disruptor->create_publisher();

    for (int i = 0; i < num_events; ++i) {
        publisher->publish_event(DemoEvent{
            producer_id * 1000 + i,  // unique value per producer
            "Event from producer " + std::to_string(producer_id),
            std::chrono::steady_clock::now()
        });

        // Small delay to simulate real-world processing
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }

    std::cout << "Producer " << producer_id << " finished sending " << num_events << " events" << std::endl;
}

// Consumer thread function - demonstrates event processing
void consumer_thread(Disruptor<DemoEvent>* disruptor, std::string consumer_name, std::atomic<int>& total_processed, std::atomic<bool>& producers_done) {
    std::cout << consumer_name << " starting..." << std::endl;

    int processed_count = 0;
    Sequence last_sequence = -1;

    // Consumer loop - process events as they become available
    while (true) {
        Sequence current_sequence = disruptor->cursor();

        if (current_sequence > last_sequence) {
            // Process all available events
            for (Sequence seq = last_sequence + 1; seq <= current_sequence; ++seq) {
                auto event = disruptor->get(seq);
                processed_count++;

                // Simulate processing time
                std::this_thread::sleep_for(std::chrono::milliseconds(2));

                // In a real application, you'd do something with the event
                if (processed_count % 25 == 0) {  // Log every 25 events
                    std::cout << consumer_name << " processed " << processed_count << " events, last value: " << event->data().value << std::endl;
                }
            }
            last_sequence = current_sequence;
        } else {
            // If producers are done and we've processed all available events, exit
            if (producers_done.load() && current_sequence == last_sequence) {
                break;
            }

            // Wait a bit before checking again
            std::this_thread::sleep_for(std::chrono::milliseconds(5));
        }
    }

    std::cout << consumer_name << " finished, processed " << processed_count << " events" << std::endl;
    total_processed += processed_count;
}

int main() {
    std::cout << "MarbleDB Disruptor Example" << std::endl;
    std::cout << "=========================" << std::endl;

    // Create a disruptor with a ring buffer size of 1024
    Disruptor<DemoEvent> disruptor(1024);

    std::cout << "Created disruptor with ring buffer size 1024" << std::endl;

    // Track total events processed and producer completion
    std::atomic<int> total_processed(0);
    std::atomic<bool> producers_done(false);

    // Start consumer threads
    const int num_consumers = 2;
    std::vector<std::thread> consumer_threads;

    for (int i = 0; i < num_consumers; ++i) {
        consumer_threads.emplace_back(consumer_thread, &disruptor,
                                    "Consumer-" + std::to_string(i + 1),
                                    std::ref(total_processed),
                                    std::ref(producers_done));
    }

    // Give consumers a moment to start
    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    // Start producer threads
    const int num_producers = 2;
    const int events_per_producer = 100;
    std::vector<std::thread> producer_threads;

    for (int i = 0; i < num_producers; ++i) {
        producer_threads.emplace_back(producer_thread, &disruptor,
                                    i + 1, events_per_producer);
    }

    // Wait for all producers to finish
    for (auto& producer : producer_threads) {
        producer.join();
    }

    // Signal that producers are done
    producers_done.store(true);

    std::cout << "All producers finished. Waiting for consumers to process remaining events..." << std::endl;

    // Wait a bit more for consumers to finish processing
    std::this_thread::sleep_for(std::chrono::milliseconds(2000));

    // Wait for all consumers to finish
    for (auto& consumer : consumer_threads) {
        consumer.join();
    }

    std::cout << "All consumers finished." << std::endl;
    std::cout << "Final disruptor status:" << std::endl;
    std::cout << "  - Current cursor: " << disruptor.cursor() << std::endl;
    std::cout << "  - Total events processed: " << total_processed.load() << std::endl;

    std::cout << "\nDisruptor example completed successfully!" << std::endl;
    std::cout << "This demonstrates:" << std::endl;
    std::cout << "  - Lock-free communication between producers and consumers" << std::endl;
    std::cout << "  - High-performance ring buffer for inter-thread messaging" << std::endl;
    std::cout << "  - LMAX Disruptor pattern implementation" << std::endl;
    std::cout << "  - Atomic operations replacing traditional mutexes" << std::endl;
    std::cout << "  - Wait-free event processing with sequence barriers" << std::endl;

    return 0;
}
