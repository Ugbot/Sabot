#include <pybind11/pybind11.h>
#include <pybind11/stl.h>
#include <pybind11/functional.h>
#include "sabot_cypher/cypher/sabot_cypher_bridge.h"
#include "sabot_cypher/cypher/logical_plan_translator.h"

// PyArrow integration
#include <arrow/python/pyarrow.h>

namespace py = pybind11;
using namespace sabot_cypher::cypher;

// Helper to convert Arrow Result to Python (raises exception on error)
template<typename T>
T unwrap_result(arrow::Result<T>&& result) {
    if (!result.ok()) {
        throw std::runtime_error(result.status().ToString());
    }
    return std::move(result).ValueOrDie();
}

PYBIND11_MODULE(sabot_cypher_native, m) {
    m.doc() = "SabotCypher: High-Performance Cypher Query Engine with Arrow Execution";
    
    // Import PyArrow
    if (arrow::py::import_pyarrow() != 0) {
        throw std::runtime_error("Failed to import pyarrow");
    }
    
    // CypherResult struct
    py::class_<CypherResult>(m, "CypherResult", "Result from Cypher query execution")
        .def_readonly("query", &CypherResult::query, "Original query string")
        .def_readonly("execution_time_ms", &CypherResult::execution_time_ms, "Execution time in milliseconds")
        .def_readonly("num_rows", &CypherResult::num_rows, "Number of result rows")
        .def_property_readonly("table", [](const CypherResult& self) {
            // Convert Arrow table to PyArrow table
            return arrow::py::wrap_table(self.table);
        }, "Result table as PyArrow Table");
    
    // SabotCypherBridge class
    py::class_<SabotCypherBridge, std::shared_ptr<SabotCypherBridge>>(
        m, "SabotCypherBridge", 
        "Main interface for SabotCypher query execution")
        
        .def_static("create", []() {
            return unwrap_result(SabotCypherBridge::Create());
        }, "Create a new SabotCypher bridge instance")
        
        .def("execute", [](SabotCypherBridge& self, 
                          const std::string& query,
                          const std::map<std::string, std::string>& params) {
            return unwrap_result(self.ExecuteCypher(query, params));
        }, 
        py::arg("query"),
        py::arg("params") = std::map<std::string, std::string>(),
        "Execute a Cypher query and return results (requires Kuzu integration)")
        
        .def("execute_plan", [](SabotCypherBridge& self, py::dict plan_dict) {
            // Convert Python dict to ArrowPlan
            ArrowPlan plan;
            
            // Extract operators list
            if (plan_dict.contains("operators")) {
                py::list ops = plan_dict["operators"];
                
                for (auto op_obj : ops) {
                    py::dict op = op_obj.cast<py::dict>();
                    
                    ArrowOperatorDesc desc;
                    desc.type = op["type"].cast<std::string>();
                    
                    // Extract params
                    if (op.contains("params")) {
                        py::dict params = op["params"];
                        for (auto item : params) {
                            std::string key = item.first.cast<std::string>();
                            std::string val = py::str(item.second).cast<std::string>();
                            desc.params[key] = val;
                        }
                    }
                    
                    plan.operators.push_back(desc);
                }
            }
            
            // Set flags
            if (plan_dict.contains("has_joins")) {
                plan.has_joins = plan_dict["has_joins"].cast<bool>();
            }
            if (plan_dict.contains("has_aggregates")) {
                plan.has_aggregates = plan_dict["has_aggregates"].cast<bool>();
            }
            if (plan_dict.contains("has_filters")) {
                plan.has_filters = plan_dict["has_filters"].cast<bool>();
            }
            
            return unwrap_result(self.ExecutePlan(plan));
        },
        py::arg("plan"),
        "Execute an ArrowPlan directly (for integration with external parsers like Lark)")
        
        .def("explain", [](SabotCypherBridge& self, const std::string& query) {
            return unwrap_result(self.Explain(query));
        },
        py::arg("query"),
        "Get query execution plan (EXPLAIN)")
        
        .def("register_graph", [](SabotCypherBridge& self,
                                  py::object vertices_py,
                                  py::object edges_py) {
            // Convert PyArrow tables to Arrow tables
            auto vertices = arrow::py::unwrap_table(vertices_py.ptr()).ValueOrDie();
            auto edges = arrow::py::unwrap_table(edges_py.ptr()).ValueOrDie();
            
            auto status = self.RegisterGraph(vertices, edges);
            if (!status.ok()) {
                throw std::runtime_error(status.ToString());
            }
        },
        py::arg("vertices"),
        py::arg("edges"),
        R"(
        Register graph data for querying.
        
        Parameters
        ----------
        vertices : pyarrow.Table
            Vertex table with columns: id (int64), label (utf8), properties...
        edges : pyarrow.Table
            Edge table with columns: source (int64), target (int64), type (utf8), properties...
        
        Examples
        --------
        >>> import pyarrow as pa
        >>> vertices = pa.table({'id': [1, 2, 3], 'label': ['Person', 'Person', 'City'], 'name': ['Alice', 'Bob', 'NYC']})
        >>> edges = pa.table({'source': [1, 2], 'target': [2, 3], 'type': ['KNOWS', 'LIVES_IN']})
        >>> bridge.register_graph(vertices, edges)
        )");
    
    // Module-level convenience function
    m.def("execute", [](const std::string& query,
                       py::object vertices_py,
                       py::object edges_py,
                       const std::map<std::string, std::string>& params) {
        auto bridge = unwrap_result(SabotCypherBridge::Create());
        
        auto vertices = arrow::py::unwrap_table(vertices_py.ptr()).ValueOrDie();
        auto edges = arrow::py::unwrap_table(edges_py.ptr()).ValueOrDie();
        
        auto status = bridge->RegisterGraph(vertices, edges);
        if (!status.ok()) {
            throw std::runtime_error(status.ToString());
        }
        
        return unwrap_result(bridge->ExecuteCypher(query, params));
    },
    py::arg("query"),
    py::arg("vertices"),
    py::arg("edges"),
    py::arg("params") = std::map<std::string, std::string>(),
    R"(
    Convenience function: execute a Cypher query without creating a bridge explicitly.
    
    Parameters
    ----------
    query : str
        Cypher query string
    vertices : pyarrow.Table
        Vertex table
    edges : pyarrow.Table
        Edge table
    params : dict, optional
        Query parameters
    
    Returns
    -------
    CypherResult
        Query results
    
    )");
    
    // Version info
    m.attr("__version__") = "0.1.0";
    m.attr("__kuzu_version__") = "0.11.2 (vendored frontend only)";
}

