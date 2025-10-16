// Batch Graph Operators
//
// Batch processing operators for graph analytics (like Spark GraphX).

#pragma once

#include <arrow/api.h>
#include <arrow/result.h>
#include <memory>
#include <string>

namespace sabot_graph {
namespace execution {

// Batch graph operator for large-scale analytics
class BatchGraphOperator {
public:
    virtual ~BatchGraphOperator() = default;
    
    // Execute batch operation on graph
    virtual arrow::Result<std::shared_ptr<arrow::Table>> Execute(
        std::shared_ptr<arrow::Table> vertices,
        std::shared_ptr<arrow::Table> edges) = 0;
    
    // Get operator name
    virtual std::string GetName() const = 0;
};

// PageRank batch operator
class PageRankOperator : public BatchGraphOperator {
public:
    PageRankOperator(int max_iterations = 20, double damping = 0.85);
    
    arrow::Result<std::shared_ptr<arrow::Table>> Execute(
        std::shared_ptr<arrow::Table> vertices,
        std::shared_ptr<arrow::Table> edges) override;
    
    std::string GetName() const override { return "PageRank"; }

private:
    int max_iterations_;
    double damping_;
};

// Connected Components batch operator
class ConnectedComponentsOperator : public BatchGraphOperator {
public:
    ConnectedComponentsOperator();
    
    arrow::Result<std::shared_ptr<arrow::Table>> Execute(
        std::shared_ptr<arrow::Table> vertices,
        std::shared_ptr<arrow::Table> edges) override;
    
    std::string GetName() const override { return "ConnectedComponents"; }
};

// Triangle Count batch operator
class TriangleCountOperator : public BatchGraphOperator {
public:
    TriangleCountOperator();
    
    arrow::Result<std::shared_ptr<arrow::Table>> Execute(
        std::shared_ptr<arrow::Table> vertices,
        std::shared_ptr<arrow::Table> edges) override;
    
    std::string GetName() const override { return "TriangleCount"; }
};

// Shortest Path batch operator
class ShortestPathOperator : public BatchGraphOperator {
public:
    ShortestPathOperator(int64_t source_id, int64_t target_id);
    
    arrow::Result<std::shared_ptr<arrow::Table>> Execute(
        std::shared_ptr<arrow::Table> vertices,
        std::shared_ptr<arrow::Table> edges) override;
    
    std::string GetName() const override { return "ShortestPath"; }

private:
    int64_t source_id_;
    int64_t target_id_;
};

} // namespace execution
} // namespace sabot_graph

