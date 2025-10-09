// graph_storage_impl.cpp - Minimal Arrow-Native Graph Storage

#include "graph_storage.h"
#include <algorithm>

namespace sabot {
namespace graph {

// ============================================================================
// CSRAdjacency Implementation - Core CSR operations only
// ============================================================================

CSRAdjacency::CSRAdjacency(std::shared_ptr<arrow::Int64Array> indptr,
                           std::shared_ptr<arrow::Int64Array> indices,
                           std::shared_ptr<arrow::StructArray> edge_data)
    : indptr_(std::move(indptr)),
      indices_(std::move(indices)),
      edge_data_(std::move(edge_data)) {}

int64_t CSRAdjacency::num_vertices() const {
  return indptr_->length() - 1;
}

int64_t CSRAdjacency::num_edges() const {
  return indices_->length();
}

int64_t CSRAdjacency::GetDegree(int64_t v) const {
  if (v < 0 || v >= num_vertices()) {
    return 0;
  }
  return indptr_->Value(v + 1) - indptr_->Value(v);
}

// ============================================================================
// VertexTable Implementation - Minimal wrapper
// ============================================================================

VertexTable::VertexTable(std::shared_ptr<arrow::Table> table)
    : table_(std::move(table)) {}

int64_t VertexTable::num_vertices() const {
  return table_->num_rows();
}

std::shared_ptr<arrow::Schema> VertexTable::schema() const {
  return table_->schema();
}

std::shared_ptr<arrow::Table> VertexTable::table() const {
  return table_;
}

// ============================================================================
// EdgeTable Implementation - Minimal wrapper
// ============================================================================

EdgeTable::EdgeTable(std::shared_ptr<arrow::Table> table)
    : table_(std::move(table)) {}

int64_t EdgeTable::num_edges() const {
  return table_->num_rows();
}

std::shared_ptr<arrow::Schema> EdgeTable::schema() const {
  return table_->schema();
}

std::shared_ptr<arrow::Table> EdgeTable::table() const {
  return table_;
}

// ============================================================================
// PropertyGraph Implementation - Minimal wrapper
// ============================================================================

PropertyGraph::PropertyGraph(std::shared_ptr<VertexTable> vertices,
                             std::shared_ptr<EdgeTable> edges)
    : vertices_(std::move(vertices)),
      edges_(std::move(edges)),
      csr_(nullptr),
      csc_(nullptr) {}

int64_t PropertyGraph::num_vertices() const {
  return vertices_->num_vertices();
}

int64_t PropertyGraph::num_edges() const {
  return edges_->num_edges();
}

std::shared_ptr<VertexTable> PropertyGraph::vertices() const {
  return vertices_;
}

std::shared_ptr<EdgeTable> PropertyGraph::edges() const {
  return edges_;
}

std::shared_ptr<CSRAdjacency> PropertyGraph::csr() const {
  return csr_;
}

std::shared_ptr<CSRAdjacency> PropertyGraph::csc() const {
  return csc_;
}

// ============================================================================
// RDFTripleStore Implementation - Minimal wrapper
// ============================================================================

RDFTripleStore::RDFTripleStore(std::shared_ptr<arrow::Table> triples,
                               std::shared_ptr<arrow::Table> term_dict)
    : triples_(std::move(triples)),
      term_dict_(std::move(term_dict)) {}

int64_t RDFTripleStore::num_triples() const {
  return triples_->num_rows();
}

int64_t RDFTripleStore::num_terms() const {
  return term_dict_->num_rows();
}

std::shared_ptr<arrow::Table> RDFTripleStore::triples() const {
  return triples_;
}

std::shared_ptr<arrow::Table> RDFTripleStore::term_dict() const {
  return term_dict_;
}

}  // namespace graph
}  // namespace sabot
