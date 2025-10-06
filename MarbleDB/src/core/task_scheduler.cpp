#include <marble/task_scheduler.h>
#include <iostream>

namespace marble {

// ConcurrentQueue implementation
void ConcurrentQueue::Push(std::shared_ptr<Task> task) {
    std::unique_lock<std::mutex> lock(mutex_);
    queue_.push(std::move(task));
    cv_.notify_one();
}

bool ConcurrentQueue::TryPop(std::shared_ptr<Task>* task) {
    std::unique_lock<std::mutex> lock(mutex_);
    if (queue_.empty()) {
        return false;
    }
    *task = std::move(queue_.front());
    queue_.pop();
    return true;
}

bool ConcurrentQueue::Pop(std::shared_ptr<Task>* task, std::chrono::milliseconds timeout) {
    std::unique_lock<std::mutex> lock(mutex_);
    if (!cv_.wait_for(lock, timeout, [this] { return !queue_.empty(); })) {
        return false;
    }
    *task = std::move(queue_.front());
    queue_.pop();
    return true;
}

size_t ConcurrentQueue::Size() const {
    std::unique_lock<std::mutex> lock(mutex_);
    return queue_.size();
}

// ProducerToken implementation
ProducerToken::ProducerToken(TaskScheduler& scheduler) : scheduler_(scheduler) {}
ProducerToken::~ProducerToken() = default;

// TaskScheduler implementation
TaskScheduler::TaskScheduler(size_t thread_count) : shutdown_(false), active_tasks_(0) {
    // Auto-detect thread count if not specified
    if (thread_count == 0) {
        thread_count = std::thread::hardware_concurrency();
        if (thread_count == 0) thread_count = 4; // fallback
    }

    total_threads_ = thread_count;
    external_threads_ = 0;
    queue_ = std::make_unique<ConcurrentQueue>();

    LaunchThreads(thread_count);
}

TaskScheduler::~TaskScheduler() {
    Shutdown();
    JoinThreads();
}

std::unique_ptr<ProducerToken> TaskScheduler::CreateProducer() {
    return std::make_unique<ProducerToken>(*this);
}

void TaskScheduler::ScheduleTask(ProducerToken& producer, std::shared_ptr<Task> task) {
    if (shutdown_) return;
    queue_->Push(std::move(task));
}

void TaskScheduler::ScheduleTasks(ProducerToken& producer, std::vector<std::shared_ptr<Task>>& tasks) {
    if (shutdown_) return;
    for (auto& task : tasks) {
        queue_->Push(std::move(task));
    }
    tasks.clear();
}

Status TaskScheduler::ExecuteTask(std::shared_ptr<Task> task) {
    if (!task) return Status::InvalidArgument("Task cannot be null");

    active_tasks_.fetch_add(1);
    try {
        task->Execute();
        active_tasks_.fetch_sub(1);
        return Status::OK();
    } catch (const std::exception& e) {
        active_tasks_.fetch_sub(1);
        return Status::Corruption(std::string("Task execution failed: ") + e.what());
    }
}

Status TaskScheduler::ExecuteTasks(std::vector<std::shared_ptr<Task>> tasks) {
    for (auto& task : tasks) {
        auto status = ExecuteTask(task);
        if (!status.ok()) return status;
    }
    return Status::OK();
}

void TaskScheduler::SetThreadCount(size_t total_threads, size_t external_threads) {
    std::unique_lock<std::mutex> lock(thread_mutex_);
    total_threads_ = total_threads;
    external_threads_ = external_threads;

    // Adjust thread pool size
    size_t worker_threads = total_threads - external_threads;
    if (worker_threads != threads_.size()) {
        JoinThreads();
        LaunchThreads(worker_threads);
    }
}

size_t TaskScheduler::GetThreadCount() const {
    return total_threads_;
}

size_t TaskScheduler::GetPendingTaskCount() const {
    return queue_->Size();
}

size_t TaskScheduler::GetActiveTaskCount() const {
    return active_tasks_.load();
}

void TaskScheduler::WaitForCompletion() {
    while (GetPendingTaskCount() > 0 || GetActiveTaskCount() > 0) {
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
}

bool TaskScheduler::WaitForCompletion(std::chrono::milliseconds timeout) {
    auto start = std::chrono::steady_clock::now();
    while (GetPendingTaskCount() > 0 || GetActiveTaskCount() > 0) {
        auto elapsed = std::chrono::steady_clock::now() - start;
        if (elapsed >= timeout) return false;
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    return true;
}

void TaskScheduler::Shutdown() {
    shutdown_ = true;
    // Wake up all waiting threads
    queue_->Push(nullptr); // sentinel value to wake threads
}

bool TaskScheduler::IsShutdown() const {
    return shutdown_;
}

void TaskScheduler::LaunchThreads(size_t count) {
    for (size_t i = 0; i < count; ++i) {
        threads_.emplace_back([this]() {
            ExecuteForever(&shutdown_);
        });
    }
}

void TaskScheduler::JoinThreads() {
    for (auto& thread : threads_) {
        if (thread.joinable()) {
            thread.join();
        }
    }
    threads_.clear();
}

void TaskScheduler::ExecuteForever(std::atomic<bool>* marker) {
    while (!*marker) {
        std::shared_ptr<Task> task;
        if (queue_->Pop(&task, std::chrono::milliseconds(100))) {
            if (!task) break; // sentinel value
            ExecuteTask(task);
        }
    }
}

size_t TaskScheduler::ExecuteTasksInternal(std::atomic<bool>* marker, size_t max_tasks) {
    size_t completed = 0;
    while (!*marker && completed < max_tasks) {
        std::shared_ptr<Task> task;
        if (!queue_->TryPop(&task)) break;
        ExecuteTask(task);
        ++completed;
    }
    return completed;
}

// Global functions
Status RunTask(TaskScheduler& scheduler, std::shared_ptr<Task> task) {
    return scheduler.ExecuteTask(std::move(task));
}

Status RunTasks(TaskScheduler& scheduler, std::vector<std::shared_ptr<Task>> tasks) {
    return scheduler.ExecuteTasks(std::move(tasks));
}

} // namespace marble
