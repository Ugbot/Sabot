#pragma once

#include <memory>
#include <functional>
#include <vector>
#include <atomic>
#include <thread>
#include <chrono>
#include <queue>
#include <mutex>
#include <condition_variable>
#include <marble/status.h>

namespace marble {

// Forward declarations for DuckDB-inspired interfaces
class Task;
class TaskScheduler;
class ProducerToken;

// Simplified concurrent queue implementation
// In production, this would use DuckDB's ConcurrentQueue
class ConcurrentQueue {
public:
    void Push(std::shared_ptr<Task> task);
    bool TryPop(std::shared_ptr<Task>* task);
    bool Pop(std::shared_ptr<Task>* task, std::chrono::milliseconds timeout);
    size_t Size() const;

private:
    std::queue<std::shared_ptr<Task>> queue_;
    mutable std::mutex mutex_;
    std::condition_variable cv_;
};

// Task interface - represents a unit of work
class Task {
public:
    virtual ~Task() = default;
    virtual void Execute() = 0;
    virtual bool IsFinished() const { return true; }
};

// Producer token for efficient task submission
class ProducerToken {
public:
    ProducerToken(TaskScheduler& scheduler);
    ~ProducerToken();

    TaskScheduler& GetScheduler() { return scheduler_; }

private:
    TaskScheduler& scheduler_;
    // DuckDB-inspired producer token would go here
};

// Main task scheduler class - inspired by DuckDB's TaskScheduler
class TaskScheduler {
public:
    explicit TaskScheduler(size_t thread_count = 0); // 0 = auto-detect
    ~TaskScheduler();

    // Disable copying
    TaskScheduler(const TaskScheduler&) = delete;
    TaskScheduler& operator=(const TaskScheduler&) = delete;

    // Producer token management
    std::unique_ptr<ProducerToken> CreateProducer();

    // Task scheduling
    void ScheduleTask(ProducerToken& producer, std::shared_ptr<Task> task);
    void ScheduleTasks(ProducerToken& producer, std::vector<std::shared_ptr<Task>>& tasks);

    // Execute tasks directly (for testing/development)
    Status ExecuteTask(std::shared_ptr<Task> task);
    Status ExecuteTasks(std::vector<std::shared_ptr<Task>> tasks);

    // Thread management
    void SetThreadCount(size_t total_threads, size_t external_threads = 0);
    size_t GetThreadCount() const;

    // Task statistics
    size_t GetPendingTaskCount() const;
    size_t GetActiveTaskCount() const;

    // Synchronization
    void WaitForCompletion();
    bool WaitForCompletion(std::chrono::milliseconds timeout);

    // Shutdown
    void Shutdown();
    bool IsShutdown() const;

private:
    // Thread management
    void LaunchThreads(size_t count);
    void JoinThreads();

    // Internal execution
    void ExecuteForever(std::atomic<bool>* marker);
    size_t ExecuteTasksInternal(std::atomic<bool>* marker, size_t max_tasks);

    // Member variables
    std::atomic<bool> shutdown_;
    std::vector<std::thread> threads_;
    std::mutex thread_mutex_;

    // Task queue - simplified version of DuckDB's ConcurrentQueue
    std::unique_ptr<ConcurrentQueue> queue_;

    // Configuration
    size_t total_threads_;
    size_t external_threads_;

    // Statistics
    std::atomic<size_t> active_tasks_;
};

// Convenience function for running tasks
Status RunTask(TaskScheduler& scheduler, std::shared_ptr<Task> task);
Status RunTasks(TaskScheduler& scheduler, std::vector<std::shared_ptr<Task>> tasks);

// Helper class for lambda-based tasks
class LambdaTask : public Task {
public:
    explicit LambdaTask(std::function<void()> func) : func_(std::move(func)) {}
    void Execute() override { func_(); }

private:
    std::function<void()> func_;
};

// Factory function for lambda tasks
inline std::shared_ptr<Task> MakeTask(std::function<void()> func) {
    return std::make_shared<LambdaTask>(std::move(func));
}

} // namespace marble
