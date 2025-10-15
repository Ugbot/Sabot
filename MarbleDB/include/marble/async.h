#pragma once

#include <boost/asio.hpp>
#include <boost/asio/experimental/coro.hpp>
#include <coroutine>
#include <memory>
#include <functional>
#include <arrow/api.h>
#include <marble/status.h>
#include <marble/db.h>
#include <marble/record.h>
#include <marble/query.h>

namespace marble {

//==============================================================================
// Async Infrastructure
//==============================================================================

/**
 * @brief AsyncResult<T> - Future-based async result
 *
 * Provides async operations using std::future for initial implementation.
 * Can be upgraded to full coroutine support later.
 */
template<typename T>
class AsyncResult {
public:
    AsyncResult(std::future<T> future) : future_(std::move(future)) {}

    // Check if result is ready
    bool is_ready() const {
        return future_.wait_for(std::chrono::seconds(0)) == std::future_status::ready;
    }

    // Get result (blocking)
    T get() {
        return future_.get();
    }

    // Wait for completion
    void wait() {
        future_.wait();
    }

private:
    std::future<T> future_;
};

/**
 * @brief AsyncResult<void> specialization
 */
template<>
class AsyncResult<void> {
public:
    AsyncResult(std::future<void> future) : future_(std::move(future)) {}

    bool is_ready() const {
        return future_.wait_for(std::chrono::seconds(0)) == std::future_status::ready;
    }

    void get() {
        future_.get();
    }

    void wait() {
        future_.wait();
    }

private:
    std::future<void> future_;
};

//==============================================================================
// Async MarbleDB API
//==============================================================================

/**
 * @brief AsyncMarbleDB - Future-based async API
 *
 * Provides non-blocking operations using std::future.
 * All operations return AsyncResult<T> for async operations.
 */
class AsyncMarbleDB {
public:
    AsyncMarbleDB();
    ~AsyncMarbleDB();

    // Disable copy/move
    AsyncMarbleDB(const AsyncMarbleDB&) = delete;
    AsyncMarbleDB& operator=(const AsyncMarbleDB&) = delete;
    AsyncMarbleDB(AsyncMarbleDB&&) = delete;
    AsyncMarbleDB& operator=(AsyncMarbleDB&&) = delete;

    // Async database operations
    AsyncResult<Status> Open(const std::string& path);
    AsyncResult<Status> Close();

    // Async column family operations
    AsyncResult<Status> CreateColumnFamily(const std::string& name,
                                          const std::shared_ptr<arrow::Schema>& schema);

    // Async data operations
    AsyncResult<Status> InsertBatch(const std::string& table_name,
                                   const std::shared_ptr<arrow::RecordBatch>& batch);

    AsyncResult<Status> ScanTable(const std::string& table_name,
                                 std::unique_ptr<QueryResult>* result);

    // Async iterator operations
    AsyncResult<std::unique_ptr<Iterator>> NewIterator(const KeyRange& range);

    // Utility
    std::vector<std::string> ListColumnFamilies() const;

private:
    std::unique_ptr<class MarbleDB> db_;  // Underlying synchronous DB
    std::unique_ptr<std::thread> worker_thread_;  // For async operations
};

//==============================================================================
// Async Iterator - Coroutine-based iteration
//==============================================================================

/**
 * @brief AsyncIterator - Future-based range iterator
 *
 * Provides async iteration over key-value pairs using futures.
 */
class AsyncIterator {
public:
    explicit AsyncIterator(std::unique_ptr<Iterator> iterator);
    ~AsyncIterator();

    // Async iterator operations
    AsyncResult<bool> Valid();
    AsyncResult<void> Next();
    AsyncResult<std::shared_ptr<Key>> Key();
    AsyncResult<std::shared_ptr<Record>> Value();
    AsyncResult<Status> Status();

private:
    std::unique_ptr<Iterator> iterator_;
};

//==============================================================================
// Async Query Operations
//==============================================================================

/**
 * @brief AsyncQueryResult - Streaming query results
 *
 * Provides future-based streaming of large result sets.
 */
class AsyncQueryResult {
public:
    virtual ~AsyncQueryResult() = default;

    // Get next batch asynchronously
    virtual AsyncResult<Status> NextBatch(std::shared_ptr<arrow::RecordBatch>* batch) = 0;

    // Check if more data available
    virtual AsyncResult<bool> HasMore() = 0;

    // Get schema
    virtual std::shared_ptr<arrow::Schema> Schema() const = 0;
};

/**
 * @brief Create async query result from synchronous result
 */
std::unique_ptr<AsyncQueryResult> MakeAsyncQueryResult(
    std::unique_ptr<QueryResult> sync_result);

} // namespace marble
