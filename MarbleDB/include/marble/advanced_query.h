#pragma once

#include <memory>
#include <string>
#include <vector>
#include <unordered_map>
#include <functional>
#include <marble/status.h>
#include <marble/query.h>
#include <arrow/api.h>

namespace marble {

// Forward declarations
class QueryResult;

// Forward declarations and basic types for query planning
enum class JoinType {
    kInner,
    kLeft,
    kRight,
    kFull,
    kCross
};

struct OrderBySpec {
    std::string column;
    bool ascending = true;
};

struct JoinCondition {
    std::string left_column;
    std::string right_column;
    std::string operator_;  // e.g., "=", "<", ">"
};

/**
 * @brief Specification for ASOF join operations on any time column
 *
 * ASOF joins find the closest match in the right table for each row in the left
 * table based on a time column. This is commonly used for:
 * - Joining trades with quotes (financial data)
 * - Event correlation with sensor readings
 * - Point-in-time lookups
 */
struct AsofJoinSpec {
    /// Time column name (must exist in both tables)
    std::string time_column;

    /// Grouping columns for per-group ASOF matching (e.g., symbol, sensor_id)
    std::vector<std::string> by_columns;

    /// Tolerance for matching in the same units as the time column
    /// - Negative: match only past values (right.time - left.time <= 0)
    /// - Positive: match only future values (right.time - left.time >= 0)
    /// - Zero: exact match only
    int64_t tolerance = 0;

    /// Suffixes for output columns from each table
    std::string left_suffix = "";
    std::string right_suffix = "_right";
};

class QueryPlan {
public:
    virtual ~QueryPlan() = default;
    virtual Status Execute(std::shared_ptr<arrow::Table>* result) = 0;
};

/**
 * @brief Time series window function types
 */
enum class WindowFunction {
    kRowNumber,     // ROW_NUMBER()
    kRank,         // RANK()
    kDenseRank,    // DENSE_RANK()
    kPercentRank,  // PERCENT_RANK()
    kCumeDist,     // CUME_DIST()
    kNtile,        // NTILE(n)
    kLag,          // LAG(column, offset)
    kLead,         // LEAD(column, offset)
    kFirstValue,   // FIRST_VALUE(column)
    kLastValue,    // LAST_VALUE(column)
    kNthValue      // NTH_VALUE(column, n)
};

/**
 * @brief Time series aggregation function types
 */
enum class TimeAggregation {
    kTimeWeightedAverage,  // Time-weighted average
    kExponentialMovingAvg, // EMA with half-life
    kCumulativeSum,        // Running total over time
    kRateOfChange,         // Rate of change (derivative)
    kTimeBucket,          // Group by time buckets
    kMovingAverage,       // Moving average over time window
    kBollingerBands,      // Bollinger bands calculation
    kVolumeWeightedAvg    // VWAP (Volume Weighted Average Price)
};

/**
 * @brief Query symbol - cached computation result
 */
struct QuerySymbol {
    std::string name;
    std::string sql_query;
    std::shared_ptr<arrow::Table> cached_result;
    uint64_t last_updated;
    uint64_t ttl_seconds;  // Time to live
    std::vector<std::string> dependencies; // Other symbols this depends on

    bool IsExpired() const {
        return (time(nullptr) - last_updated) > ttl_seconds;
    }
};

/**
 * @brief Symbol library - cache of pre-computed query results
 */
class SymbolLibrary {
public:
    virtual ~SymbolLibrary() = default;

    /**
     * @brief Register a symbol with its query
     */
    virtual Status RegisterSymbol(const std::string& name,
                                 const std::string& sql_query,
                                 uint64_t ttl_seconds = 3600) = 0;

    /**
     * @brief Get cached result for a symbol
     */
    virtual Status GetSymbol(const std::string& name,
                           std::shared_ptr<arrow::Table>* result) = 0;

    /**
     * @brief Invalidate a symbol (force recomputation)
     */
    virtual Status InvalidateSymbol(const std::string& name) = 0;

    /**
     * @brief Get all registered symbols
     */
    virtual Status ListSymbols(std::vector<std::string>* symbols) const = 0;

    /**
     * @brief Clean up expired symbols
     */
    virtual Status CleanupExpired() = 0;
};

/**
 * @brief Window specification for window functions
 */
struct WindowSpec {
    std::vector<std::string> partition_by;  // PARTITION BY columns
    std::vector<OrderBySpec> order_by;     // ORDER BY specification
    std::string frame_clause;              // ROWS/RANGE frame specification

    // Frame boundaries
    enum class FrameType {
        kRows,    // Physical row-based frames
        kRange    // Logical range-based frames
    };

    FrameType frame_type = FrameType::kRows;
    int64_t frame_start = 0;  // UNBOUNDED PRECEDING = INT64_MIN
    int64_t frame_end = 0;    // UNBOUNDED FOLLOWING = INT64_MAX
};

/**
 * @brief Time series query specification
 */
struct TimeSeriesSpec {
    std::string time_column;
    std::string value_column;
    uint64_t window_size_seconds;
    TimeAggregation aggregation_type;

    // For moving averages, EMAs, etc.
    uint64_t period_count = 0;
    double alpha = 0.0;  // For EMA (1-alpha smoothing factor)

    // For Bollinger bands
    double standard_deviations = 2.0;
};

/**
 * @brief Advanced query execution engine
 */
class AdvancedQueryEngine {
public:
    virtual ~AdvancedQueryEngine() = default;

    /**
     * @brief Execute window function query
     */
    virtual Status ExecuteWindowFunction(const std::string& table_name,
                                       const std::vector<std::string>& select_columns,
                                       const WindowSpec& window_spec,
                                       WindowFunction window_func,
                                       const std::string& window_func_column,
                                       std::unique_ptr<QueryResult>* result) = 0;

    /**
     * @brief Execute time series aggregation
     */
    virtual Status ExecuteTimeAggregation(const std::string& table_name,
                                        const TimeSeriesSpec& time_spec,
                                        std::unique_ptr<QueryResult>* result) = 0;

    /**
     * @brief Execute complex join query
     */
    virtual Status ExecuteJoin(const std::string& left_table,
                             const std::string& right_table,
                             const std::vector<JoinCondition>& join_conditions,
                             JoinType join_type,
                             const std::vector<std::string>& select_columns,
                             std::unique_ptr<QueryResult>* result) = 0;

    /**
     * @brief Execute subquery
     */
    virtual Status ExecuteSubquery(const std::string& outer_query,
                                 const std::string& subquery,
                                 std::unique_ptr<QueryResult>* result) = 0;

    /**
     * @brief Execute analytical query using symbols
     */
    virtual Status ExecuteWithSymbols(const std::string& query_with_symbols,
                                    std::unique_ptr<QueryResult>* result) = 0;

    /**
     * @brief Register a user-defined function
     */
    virtual Status RegisterUDF(const std::string& name,
                             std::function<arrow::Result<std::shared_ptr<arrow::Array>>(
                                 const std::vector<std::shared_ptr<arrow::Array>>&)> udf) = 0;
};

/**
 * @brief Time series analytics engine
 */
class TimeSeriesAnalytics {
public:
    virtual ~TimeSeriesAnalytics() = default;

    /**
     * @brief Calculate time-weighted average price (TWAP)
     */
    virtual Status CalculateTWAP(const std::string& table_name,
                               const std::string& time_column,
                               const std::string& price_column,
                               const std::string& volume_column,
                               uint64_t window_seconds,
                               std::shared_ptr<arrow::Table>* result) = 0;

    /**
     * @brief Calculate volume-weighted average price (VWAP)
     */
    virtual Status CalculateVWAP(const std::string& table_name,
                               const std::string& time_column,
                               const std::string& price_column,
                               const std::string& volume_column,
                               std::shared_ptr<arrow::Table>* result) = 0;

    /**
     * @brief Calculate exponential moving average
     */
    virtual Status CalculateEMA(const std::string& table_name,
                              const std::string& time_column,
                              const std::string& value_column,
                              double alpha,
                              std::shared_ptr<arrow::Table>* result) = 0;

    /**
     * @brief Detect anomalies using statistical methods
     */
    virtual Status DetectAnomalies(const std::string& table_name,
                                 const std::string& time_column,
                                 const std::string& value_column,
                                 double threshold_sigma,
                                 std::shared_ptr<arrow::Table>* result) = 0;

    /**
     * @brief Calculate correlation between time series
     */
    virtual Status CalculateCorrelation(const std::string& table1,
                                      const std::string& table2,
                                      const std::string& time_column,
                                      const std::string& value_column1,
                                      const std::string& value_column2,
                                      std::shared_ptr<arrow::Table>* result) = 0;
};

/**
 * @brief Query optimization hints
 */
struct QueryHint {
    enum Type {
        kUseIndex,
        kForceMergeJoin,
        kForceHashJoin,
        kPreferSortMerge,
        kDisableParallel,
        kEnableParallel,
        kUseSymbolCache,
        kBypassCache
    };

    Type type;
    std::string value;
};

/**
 * @brief Advanced query planner
 */
class AdvancedQueryPlanner {
public:
    virtual ~AdvancedQueryPlanner() = default;

    /**
     * @brief Plan a complex analytical query
     */
    virtual Status PlanQuery(const std::string& query_sql,
                           const std::vector<QueryHint>& hints,
                           std::unique_ptr<QueryPlan>* plan) = 0;

    /**
     * @brief Optimize query plan for time series operations
     */
    virtual Status OptimizeTimeSeriesPlan(const TimeSeriesSpec& time_spec,
                                        std::unique_ptr<QueryPlan>* optimized_plan) = 0;

    /**
     * @brief Cache query plan for reuse
     */
    virtual Status CachePlan(const std::string& query_hash,
                           std::unique_ptr<QueryPlan> plan) = 0;

    /**
     * @brief Get cached plan if available
     */
    virtual Status GetCachedPlan(const std::string& query_hash,
                               std::unique_ptr<QueryPlan>* plan) = 0;
};

/**
 * @brief Complex query builder with fluent API
 */
class ComplexQueryBuilder {
public:
    ComplexQueryBuilder();
    ~ComplexQueryBuilder();

    // Table selection
    ComplexQueryBuilder& From(const std::string& table_name);

    // Joins
    ComplexQueryBuilder& Join(const std::string& table_name,
                             const std::string& condition,
                             JoinType join_type = JoinType::kInner);

    // Window functions
    ComplexQueryBuilder& WithWindow(const std::string& window_name,
                                   const WindowSpec& window_spec);
    ComplexQueryBuilder& AddWindowFunction(WindowFunction func,
                                          const std::string& column,
                                          const std::string& window_name = "");

    // Time series operations
    ComplexQueryBuilder& WithTimeSeries(const TimeSeriesSpec& time_spec);
    ComplexQueryBuilder& AddTimeAggregation(TimeAggregation agg);

    // Subqueries
    ComplexQueryBuilder& WithSubquery(const std::string& subquery_alias,
                                     const std::string& subquery_sql);

    // Symbols
    ComplexQueryBuilder& UseSymbol(const std::string& symbol_name);

    // Selection and filtering
    ComplexQueryBuilder& Select(const std::vector<std::string>& columns);
    ComplexQueryBuilder& Where(const std::string& condition);
    ComplexQueryBuilder& GroupBy(const std::vector<std::string>& columns);
    ComplexQueryBuilder& OrderBy(const std::vector<OrderBySpec>& order_specs);
    ComplexQueryBuilder& Limit(int64_t limit);
    ComplexQueryBuilder& Offset(int64_t offset);

    // Hints
    ComplexQueryBuilder& WithHint(QueryHint hint);

    // Execution
    Status Execute(AdvancedQueryEngine* engine, std::unique_ptr<QueryResult>* result);

private:
    struct QueryComponents {
        std::string from_table;
        std::vector<std::pair<std::string, std::pair<std::string, JoinType>>> joins;
        std::vector<std::string> select_columns;
        std::string where_clause;
        std::vector<std::string> group_by_columns;
        std::vector<OrderBySpec> order_by_specs;
        int64_t limit_count = -1;
        int64_t offset_count = 0;
        std::vector<QueryHint> hints;

        // Advanced features
        std::unordered_map<std::string, WindowSpec> windows;
        std::vector<std::pair<WindowFunction, std::pair<std::string, std::string>>> window_functions;
        TimeSeriesSpec time_series_spec;
        std::unordered_map<std::string, std::string> subqueries;
        std::vector<std::string> symbols;
    };

    QueryComponents components_;
};

// Factory functions
std::unique_ptr<AdvancedQueryEngine> CreateAdvancedQueryEngine();
std::unique_ptr<SymbolLibrary> CreateSymbolLibrary();
std::unique_ptr<TimeSeriesAnalytics> CreateTimeSeriesAnalytics();
std::unique_ptr<AdvancedQueryPlanner> CreateAdvancedQueryPlanner();
ComplexQueryBuilder CreateComplexQueryBuilder();

//==============================================================================
// Standalone Join Functions (Table-to-Table Operations)
//==============================================================================

/**
 * @brief Perform hash join between two Arrow tables
 *
 * Joins two tables on specified key columns using hash-based algorithm.
 * Powered by Arrow Acero execution engine.
 *
 * @param left Left input table
 * @param right Right input table
 * @param left_keys Key column names from left table
 * @param right_keys Key column names from right table (must match left_keys length)
 * @param join_type Type of join (kInner, kLeft, kRight, kFull)
 * @param left_suffix Suffix for left columns in case of name collision
 * @param right_suffix Suffix for right columns in case of name collision
 * @param result Output table
 * @return Status::OK() on success
 */
Status HashJoin(
    std::shared_ptr<arrow::Table> left,
    std::shared_ptr<arrow::Table> right,
    const std::vector<std::string>& left_keys,
    const std::vector<std::string>& right_keys,
    JoinType join_type,
    const std::string& left_suffix,
    const std::string& right_suffix,
    std::shared_ptr<arrow::Table>* result);

/**
 * @brief Simplified hash join with same key names in both tables
 *
 * @param left Left input table
 * @param right Right input table
 * @param on_keys Key column names (must exist in both tables)
 * @param join_type Type of join (default: kInner)
 * @param result Output table
 * @return Status::OK() on success
 */
Status HashJoin(
    std::shared_ptr<arrow::Table> left,
    std::shared_ptr<arrow::Table> right,
    const std::vector<std::string>& on_keys,
    JoinType join_type,
    std::shared_ptr<arrow::Table>* result);

/**
 * @brief Perform ASOF join between two Arrow tables
 *
 * ASOF join matches each row in the left table with the closest row in the
 * right table based on a time column. This is useful for:
 * - Joining trades with quotes (find last quote before each trade)
 * - Event correlation with sensor data
 * - Point-in-time lookups
 *
 * Both tables must be sorted by the time column before calling this function.
 *
 * @param left Left input table (must be sorted by time_column)
 * @param right Right input table (must be sorted by time_column)
 * @param spec ASOF join specification (time column, by columns, tolerance)
 * @param result Output table
 * @return Status::OK() on success
 */
Status AsofJoin(
    std::shared_ptr<arrow::Table> left,
    std::shared_ptr<arrow::Table> right,
    const AsofJoinSpec& spec,
    std::shared_ptr<arrow::Table>* result);

} // namespace marble

