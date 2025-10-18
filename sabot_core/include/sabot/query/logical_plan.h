#pragma once

#include <memory>
#include <string>
#include <vector>
#include <unordered_map>
#include <arrow/api.h>

namespace sabot {
namespace query {

/**
 * @brief Logical operator types
 */
enum class LogicalOperatorType {
    SCAN,
    FILTER,
    PROJECT,
    MAP,
    AGGREGATE,
    GROUP_BY,
    JOIN,
    WINDOW,
    SORT,
    LIMIT,
    UNION,
    DISTINCT
};

/**
 * @brief Base class for logical plan nodes
 * 
 * All query APIs (Stream, SQL, Graph) translate to logical plans,
 * enabling shared optimization and composition.
 */
class LogicalPlan {
public:
    virtual ~LogicalPlan() = default;
    
    /**
     * @brief Get operator type
     */
    virtual LogicalOperatorType GetType() const = 0;
    
    /**
     * @brief Get child operators
     */
    virtual std::vector<std::shared_ptr<LogicalPlan>> GetChildren() const = 0;
    
    /**
     * @brief Validate plan correctness
     */
    virtual bool Validate() const = 0;
    
    /**
     * @brief Get output schema (if known)
     */
    virtual arrow::Result<std::shared_ptr<arrow::Schema>> GetSchema() const = 0;
    
    /**
     * @brief Estimate output cardinality
     */
    virtual int64_t EstimateCardinality() const = 0;
    
    /**
     * @brief Get plan metadata
     */
    virtual std::unordered_map<std::string, std::string> GetMetadata() const = 0;
    
    /**
     * @brief Clone the plan node
     */
    virtual std::shared_ptr<LogicalPlan> Clone() const = 0;
};

/**
 * @brief Scan operator (data source)
 */
class ScanPlan : public LogicalPlan {
public:
    ScanPlan(const std::string& source_type, const std::string& source_name);
    
    LogicalOperatorType GetType() const override { return LogicalOperatorType::SCAN; }
    std::vector<std::shared_ptr<LogicalPlan>> GetChildren() const override { return {}; }
    bool Validate() const override;
    arrow::Result<std::shared_ptr<arrow::Schema>> GetSchema() const override;
    int64_t EstimateCardinality() const override;
    std::unordered_map<std::string, std::string> GetMetadata() const override;
    std::shared_ptr<LogicalPlan> Clone() const override;
    
    const std::string& GetSourceType() const { return source_type_; }
    const std::string& GetSourceName() const { return source_name_; }
    
    void SetSchema(std::shared_ptr<arrow::Schema> schema) { schema_ = schema; }
    void SetEstimatedRows(int64_t rows) { estimated_rows_ = rows; }
    
private:
    std::string source_type_;  // 'kafka', 'parquet', 'table', etc.
    std::string source_name_;  // Topic name, file path, table name
    std::shared_ptr<arrow::Schema> schema_;
    int64_t estimated_rows_ = -1;
};

/**
 * @brief Filter operator
 */
class FilterPlan : public LogicalPlan {
public:
    FilterPlan(std::shared_ptr<LogicalPlan> input, const std::string& predicate);
    
    LogicalOperatorType GetType() const override { return LogicalOperatorType::FILTER; }
    std::vector<std::shared_ptr<LogicalPlan>> GetChildren() const override { return {input_}; }
    bool Validate() const override;
    arrow::Result<std::shared_ptr<arrow::Schema>> GetSchema() const override;
    int64_t EstimateCardinality() const override;
    std::unordered_map<std::string, std::string> GetMetadata() const override;
    std::shared_ptr<LogicalPlan> Clone() const override;
    
    const std::shared_ptr<LogicalPlan>& GetInput() const { return input_; }
    const std::string& GetPredicate() const { return predicate_; }
    
    void SetSelectivity(double selectivity) { selectivity_ = selectivity; }
    
private:
    std::shared_ptr<LogicalPlan> input_;
    std::string predicate_;
    double selectivity_ = 0.5;  // Default 50% selectivity
};

/**
 * @brief Project operator (column selection)
 */
class ProjectPlan : public LogicalPlan {
public:
    ProjectPlan(std::shared_ptr<LogicalPlan> input, const std::vector<std::string>& columns);
    
    LogicalOperatorType GetType() const override { return LogicalOperatorType::PROJECT; }
    std::vector<std::shared_ptr<LogicalPlan>> GetChildren() const override { return {input_}; }
    bool Validate() const override;
    arrow::Result<std::shared_ptr<arrow::Schema>> GetSchema() const override;
    int64_t EstimateCardinality() const override;
    std::unordered_map<std::string, std::string> GetMetadata() const override;
    std::shared_ptr<LogicalPlan> Clone() const override;
    
    const std::shared_ptr<LogicalPlan>& GetInput() const { return input_; }
    const std::vector<std::string>& GetColumns() const { return columns_; }
    
private:
    std::shared_ptr<LogicalPlan> input_;
    std::vector<std::string> columns_;
};

/**
 * @brief Join operator
 */
class JoinPlan : public LogicalPlan {
public:
    enum class JoinType {
        INNER,
        LEFT,
        RIGHT,
        FULL,
        SEMI,
        ANTI,
        ASOF
    };
    
    JoinPlan(
        std::shared_ptr<LogicalPlan> left,
        std::shared_ptr<LogicalPlan> right,
        const std::vector<std::string>& left_keys,
        const std::vector<std::string>& right_keys,
        JoinType join_type = JoinType::INNER
    );
    
    LogicalOperatorType GetType() const override { return LogicalOperatorType::JOIN; }
    std::vector<std::shared_ptr<LogicalPlan>> GetChildren() const override { return {left_, right_}; }
    bool Validate() const override;
    arrow::Result<std::shared_ptr<arrow::Schema>> GetSchema() const override;
    int64_t EstimateCardinality() const override;
    std::unordered_map<std::string, std::string> GetMetadata() const override;
    std::shared_ptr<LogicalPlan> Clone() const override;
    
    const std::shared_ptr<LogicalPlan>& GetLeft() const { return left_; }
    const std::shared_ptr<LogicalPlan>& GetRight() const { return right_; }
    const std::vector<std::string>& GetLeftKeys() const { return left_keys_; }
    const std::vector<std::string>& GetRightKeys() const { return right_keys_; }
    JoinType GetJoinType() const { return join_type_; }
    
private:
    std::shared_ptr<LogicalPlan> left_;
    std::shared_ptr<LogicalPlan> right_;
    std::vector<std::string> left_keys_;
    std::vector<std::string> right_keys_;
    JoinType join_type_;
};

/**
 * @brief Group by operator
 */
class GroupByPlan : public LogicalPlan {
public:
    GroupByPlan(
        std::shared_ptr<LogicalPlan> input,
        const std::vector<std::string>& group_keys,
        const std::unordered_map<std::string, std::string>& aggregations
    );
    
    LogicalOperatorType GetType() const override { return LogicalOperatorType::GROUP_BY; }
    std::vector<std::shared_ptr<LogicalPlan>> GetChildren() const override { return {input_}; }
    bool Validate() const override;
    arrow::Result<std::shared_ptr<arrow::Schema>> GetSchema() const override;
    int64_t EstimateCardinality() const override;
    std::unordered_map<std::string, std::string> GetMetadata() const override;
    std::shared_ptr<LogicalPlan> Clone() const override;
    
    const std::shared_ptr<LogicalPlan>& GetInput() const { return input_; }
    const std::vector<std::string>& GetGroupKeys() const { return group_keys_; }
    const std::unordered_map<std::string, std::string>& GetAggregations() const { 
        return aggregations_; 
    }
    
private:
    std::shared_ptr<LogicalPlan> input_;
    std::vector<std::string> group_keys_;
    std::unordered_map<std::string, std::string> aggregations_;  // column -> function
};

} // namespace query
} // namespace sabot

