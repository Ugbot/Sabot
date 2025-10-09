// graph_storage.h - Minimal Arrow-Native Graph Storage
//
// Core C++ primitives for CSR operations and table wrappers.
// High-level operations implemented in Cython using pyarrow.

#ifndef SABOT_GRAPH_STORAGE_H
#define SABOT_GRAPH_STORAGE_H

#include <arrow/api.h>
#include <arrow/table.h>
#include <arrow/array.h>
#include <memory>

namespace sabot {
namespace graph {

// ============================================================================
// CSR (Compressed Sparse Row) Adjacency Structure
// ============================================================================

/// CSR adjacency structure backed by Arrow arrays for zero-copy operations.
class CSRAdjacency {
 public:
  CSRAdjacency(std::shared_ptr<arrow::Int64Array> indptr,
               std::shared_ptr<arrow::Int64Array> indices,
               std::shared_ptr<arrow::StructArray> edge_data = nullptr);

  ~CSRAdjacency() = default;

  // Core operations
  int64_t num_vertices() const;
  int64_t num_edges() const;
  int64_t GetDegree(int64_t v) const;

  // Accessors
  std::shared_ptr<arrow::Int64Array> indptr() const { return indptr_; }
  std::shared_ptr<arrow::Int64Array> indices() const { return indices_; }
  std::shared_ptr<arrow::StructArray> edge_data() const { return edge_data_; }

 private:
  std::shared_ptr<arrow::Int64Array> indptr_;
  std::shared_ptr<arrow::Int64Array> indices_;
  std::shared_ptr<arrow::StructArray> edge_data_;
};

// ============================================================================
// Vertex Table - Minimal wrapper
// ============================================================================

class VertexTable {
 public:
  explicit VertexTable(std::shared_ptr<arrow::Table> table);
  ~VertexTable() = default;

  int64_t num_vertices() const;
  std::shared_ptr<arrow::Schema> schema() const;
  std::shared_ptr<arrow::Table> table() const;

 private:
  std::shared_ptr<arrow::Table> table_;
};

// ============================================================================
// Edge Table - Minimal wrapper
// ============================================================================

class EdgeTable {
 public:
  explicit EdgeTable(std::shared_ptr<arrow::Table> table);
  ~EdgeTable() = default;

  int64_t num_edges() const;
  std::shared_ptr<arrow::Schema> schema() const;
  std::shared_ptr<arrow::Table> table() const;

 private:
  std::shared_ptr<arrow::Table> table_;
};

// ============================================================================
// Property Graph - Minimal wrapper
// ============================================================================

class PropertyGraph {
 public:
  PropertyGraph(std::shared_ptr<VertexTable> vertices,
                std::shared_ptr<EdgeTable> edges);
  ~PropertyGraph() = default;

  int64_t num_vertices() const;
  int64_t num_edges() const;
  std::shared_ptr<VertexTable> vertices() const;
  std::shared_ptr<EdgeTable> edges() const;
  std::shared_ptr<CSRAdjacency> csr() const;
  std::shared_ptr<CSRAdjacency> csc() const;

 private:
  std::shared_ptr<VertexTable> vertices_;
  std::shared_ptr<EdgeTable> edges_;
  std::shared_ptr<CSRAdjacency> csr_;
  std::shared_ptr<CSRAdjacency> csc_;
};

// ============================================================================
// RDF Triple Store - Minimal wrapper
// ============================================================================

class RDFTripleStore {
 public:
  RDFTripleStore(std::shared_ptr<arrow::Table> triples,
                 std::shared_ptr<arrow::Table> term_dict);
  ~RDFTripleStore() = default;

  int64_t num_triples() const;
  int64_t num_terms() const;
  std::shared_ptr<arrow::Table> triples() const;
  std::shared_ptr<arrow::Table> term_dict() const;

 private:
  std::shared_ptr<arrow::Table> triples_;
  std::shared_ptr<arrow::Table> term_dict_;
};

}  // namespace graph
}  // namespace sabot

#endif  // SABOT_GRAPH_STORAGE_H
