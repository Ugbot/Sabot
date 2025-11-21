/**
 * String Predicate Optimization Strategy Implementation
 *
 * Integrates Sabot's SIMD-accelerated string operations into MarbleDB.
 */

#include "marble/optimizations/string_predicate_strategy.h"
#include "marble/table_capabilities.h"
#include <Python.h>
#include <arrow/python/pyarrow.h>
#include <chrono>
#include <sstream>
#include <iomanip>

namespace marble {

//==============================================================================
// StringOperationsWrapper - C++ wrapper around Cython string operations
//==============================================================================

StringOperationsWrapper::StringOperationsWrapper() : is_available_(false) {
    // Initialize Python interpreter if not already initialized
    if (!Py_IsInitialized()) {
        Py_Initialize();
    }

    // Initialize PyArrow
    arrow::py::import_pyarrow();

    // Try to import sabot._cython.arrow.string_operations module
    PyObject* module = PyImport_ImportModule("sabot._cython.arrow.string_operations");
    if (module != nullptr) {
        is_available_ = true;
        Py_DECREF(module);
    } else {
        // Module not available (not built or import error)
        PyErr_Clear();
        is_available_ = false;
    }
}

StringOperationsWrapper::~StringOperationsWrapper() {
    // Don't finalize Python (other code may be using it)
}

arrow::Result<std::shared_ptr<arrow::BooleanArray>>
StringOperationsWrapper::CallPythonStringOp(
    const std::string& func_name,
    const std::shared_ptr<arrow::StringArray>& array,
    const std::string& pattern,
    bool ignore_case) {

    if (!is_available_) {
        return arrow::Status::NotImplemented(
            "Sabot string operations module not available");
    }

    // Acquire GIL
    PyGILState_STATE gstate = PyGILState_Ensure();

    PyObject* result = nullptr;
    std::shared_ptr<arrow::BooleanArray> bool_array;

    try {
        // Import module
        PyObject* module = PyImport_ImportModule("sabot._cython.arrow.string_operations");
        if (module == nullptr) {
            PyErr_Print();
            PyGILState_Release(gstate);
            return arrow::Status::IOError("Failed to import string_operations module");
        }

        // Get function
        PyObject* func = PyObject_GetAttrString(module, func_name.c_str());
        Py_DECREF(module);
        if (func == nullptr) {
            PyErr_Print();
            PyGILState_Release(gstate);
            return arrow::Status::IOError("Failed to get function: " + func_name);
        }

        // Convert Arrow array to PyArrow
        PyObject* py_array = arrow::py::wrap_array(array);
        if (py_array == nullptr) {
            Py_DECREF(func);
            PyErr_Print();
            PyGILState_Release(gstate);
            return arrow::Status::IOError("Failed to convert array to PyArrow");
        }

        // Create arguments tuple
        PyObject* args;
        if (func_name == "length") {
            // length() takes only array
            args = PyTuple_Pack(1, py_array);
        } else {
            // Other functions take (array, pattern, ignore_case)
            PyObject* py_pattern = PyUnicode_FromString(pattern.c_str());
            PyObject* py_ignore_case = ignore_case ? Py_True : Py_False;
            Py_INCREF(py_ignore_case);  // PyTuple_Pack steals reference

            if (func_name == "equal" || func_name == "not_equal") {
                // equal/not_equal don't have ignore_case parameter
                args = PyTuple_Pack(2, py_array, py_pattern);
                Py_DECREF(py_ignore_case);
            } else {
                // contains, starts_with, ends_with, match_regex have ignore_case
                args = PyTuple_Pack(3, py_array, py_pattern, py_ignore_case);
            }
            Py_DECREF(py_pattern);
        }
        Py_DECREF(py_array);

        if (args == nullptr) {
            Py_DECREF(func);
            PyErr_Print();
            PyGILState_Release(gstate);
            return arrow::Status::IOError("Failed to create args tuple");
        }

        // Call function
        result = PyObject_CallObject(func, args);
        Py_DECREF(args);
        Py_DECREF(func);

        if (result == nullptr) {
            PyErr_Print();
            PyGILState_Release(gstate);
            return arrow::Status::IOError("Python function call failed");
        }

        // Convert result back to Arrow array
        arrow::Result<std::shared_ptr<arrow::Array>> unwrap_result =
            arrow::py::unwrap_array(result);
        Py_DECREF(result);

        if (!unwrap_result.ok()) {
            PyGILState_Release(gstate);
            return arrow::Status::IOError("Failed to unwrap result array");
        }

        bool_array = std::static_pointer_cast<arrow::BooleanArray>(unwrap_result.ValueOrDie());

    } catch (const std::exception& e) {
        if (result) Py_DECREF(result);
        PyGILState_Release(gstate);
        return arrow::Status::IOError("Exception in Python call: " + std::string(e.what()));
    }

    PyGILState_Release(gstate);
    return bool_array;
}

arrow::Result<std::shared_ptr<arrow::BooleanArray>>
StringOperationsWrapper::Equal(
    const std::shared_ptr<arrow::StringArray>& array,
    const std::string& pattern) {
    return CallPythonStringOp("equal", array, pattern, false);
}

arrow::Result<std::shared_ptr<arrow::BooleanArray>>
StringOperationsWrapper::NotEqual(
    const std::shared_ptr<arrow::StringArray>& array,
    const std::string& pattern) {
    return CallPythonStringOp("not_equal", array, pattern, false);
}

arrow::Result<std::shared_ptr<arrow::BooleanArray>>
StringOperationsWrapper::Contains(
    const std::shared_ptr<arrow::StringArray>& array,
    const std::string& pattern,
    bool ignore_case) {
    return CallPythonStringOp("contains", array, pattern, ignore_case);
}

arrow::Result<std::shared_ptr<arrow::BooleanArray>>
StringOperationsWrapper::StartsWith(
    const std::shared_ptr<arrow::StringArray>& array,
    const std::string& pattern,
    bool ignore_case) {
    return CallPythonStringOp("starts_with", array, pattern, ignore_case);
}

arrow::Result<std::shared_ptr<arrow::BooleanArray>>
StringOperationsWrapper::EndsWith(
    const std::shared_ptr<arrow::StringArray>& array,
    const std::string& pattern,
    bool ignore_case) {
    return CallPythonStringOp("ends_with", array, pattern, ignore_case);
}

arrow::Result<std::shared_ptr<arrow::BooleanArray>>
StringOperationsWrapper::MatchRegex(
    const std::shared_ptr<arrow::StringArray>& array,
    const std::string& pattern,
    bool ignore_case) {
    return CallPythonStringOp("match_regex", array, pattern, ignore_case);
}

arrow::Result<std::shared_ptr<arrow::Int32Array>>
StringOperationsWrapper::Length(
    const std::shared_ptr<arrow::StringArray>& array) {

    if (!is_available_) {
        return arrow::Status::NotImplemented(
            "Sabot string operations module not available");
    }

    // Similar to CallPythonStringOp but returns Int32Array
    PyGILState_STATE gstate = PyGILState_Ensure();

    PyObject* module = PyImport_ImportModule("sabot._cython.arrow.string_operations");
    if (module == nullptr) {
        PyErr_Print();
        PyGILState_Release(gstate);
        return arrow::Status::IOError("Failed to import module");
    }

    PyObject* func = PyObject_GetAttrString(module, "length");
    Py_DECREF(module);
    if (func == nullptr) {
        PyErr_Print();
        PyGILState_Release(gstate);
        return arrow::Status::IOError("Failed to get length function");
    }

    PyObject* py_array = arrow::py::wrap_array(array);
    if (py_array == nullptr) {
        Py_DECREF(func);
        PyErr_Print();
        PyGILState_Release(gstate);
        return arrow::Status::IOError("Failed to wrap array");
    }

    PyObject* args = PyTuple_Pack(1, py_array);
    Py_DECREF(py_array);

    PyObject* result = PyObject_CallObject(func, args);
    Py_DECREF(args);
    Py_DECREF(func);

    if (result == nullptr) {
        PyErr_Print();
        PyGILState_Release(gstate);
        return arrow::Status::IOError("Length function call failed");
    }

    arrow::Result<std::shared_ptr<arrow::Array>> unwrap_result =
        arrow::py::unwrap_array(result);
    Py_DECREF(result);
    PyGILState_Release(gstate);

    if (!unwrap_result.ok()) {
        return arrow::Status::IOError("Failed to unwrap result");
    }

    return std::static_pointer_cast<arrow::Int32Array>(unwrap_result.ValueOrDie());
}

//==============================================================================
// StringPredicateStrategy Implementation
//==============================================================================

StringPredicateStrategy::StringPredicateStrategy()
    : string_ops_(std::make_unique<StringOperationsWrapper>()) {
}

StringPredicateStrategy::~StringPredicateStrategy() = default;

Status StringPredicateStrategy::OnTableCreate(const TableCapabilities& caps) {
    // TableCapabilities doesn't contain schema
    // Auto-configuration must be done using AutoConfigure() factory method
    // or by manually calling EnableColumn() for each column

    // If no columns enabled yet, log a warning but don't fail
    // (user should use AutoConfigure or manually enable columns)

    return Status::OK();
}

Status StringPredicateStrategy::OnRead(ReadContext* ctx) {
    // This is a placeholder - actual predicate information needs to be passed
    // via extended ReadContext or a separate mechanism
    //
    // TODO: Integrate with MarbleDB's query planner to pass string predicates
    // For now, this is a no-op until we add predicate passing infrastructure

    stats_.reads_intercepted++;
    return Status::OK();
}

void StringPredicateStrategy::OnReadComplete(const Key& key, const Record& record) {
    // Update access statistics
    // Could track popular search patterns here
}

Status StringPredicateStrategy::OnWrite(WriteContext* ctx) {
    // Could build vocabulary cache here for RDF optimization
    // For now, no-op
    return Status::OK();
}

Status StringPredicateStrategy::OnCompaction(CompactionContext* ctx) {
    // Could rebuild string statistics here
    return Status::OK();
}

Status StringPredicateStrategy::OnFlush(FlushContext* ctx) {
    // Serialize vocabulary cache if we have one
    if (!vocabulary_cache_.empty()) {
        // Serialize vocabulary to metadata
        std::stringstream ss;
        ss << vocabulary_cache_.size();
        for (const auto& [str, id] : vocabulary_cache_) {
            ss << "\n" << str << "\t" << id;
        }
        std::string serialized = ss.str();
        ctx->metadata["vocabulary_cache"] = std::vector<uint8_t>(
            serialized.begin(), serialized.end());
        ctx->include_cache_hints = true;
    }

    return Status::OK();
}

size_t StringPredicateStrategy::MemoryUsage() const {
    size_t total = 0;

    // Vocabulary cache
    for (const auto& [str, id] : vocabulary_cache_) {
        total += str.size() + sizeof(int64_t);
    }

    // Overhead
    total += sizeof(*this);

    return total;
}

void StringPredicateStrategy::Clear() {
    vocabulary_cache_.clear();
    stats_ = Stats();
}

std::vector<uint8_t> StringPredicateStrategy::Serialize() const {
    std::stringstream ss;

    // Serialize stats
    ss << stats_.reads_intercepted << " "
       << stats_.reads_short_circuited << " "
       << stats_.rows_filtered << " "
       << stats_.rows_passed << " "
       << stats_.total_filter_time_ms << " "
       << stats_.vocab_cache_hits << " "
       << stats_.vocab_cache_misses << "\n";

    // Serialize vocabulary cache
    ss << vocabulary_cache_.size() << "\n";
    for (const auto& [str, id] : vocabulary_cache_) {
        ss << str << "\t" << id << "\n";
    }

    std::string serialized = ss.str();
    return std::vector<uint8_t>(serialized.begin(), serialized.end());
}

Status StringPredicateStrategy::Deserialize(const std::vector<uint8_t>& data) {
    std::string str(data.begin(), data.end());
    std::stringstream ss(str);

    // Deserialize stats
    ss >> stats_.reads_intercepted
       >> stats_.reads_short_circuited
       >> stats_.rows_filtered
       >> stats_.rows_passed
       >> stats_.total_filter_time_ms
       >> stats_.vocab_cache_hits
       >> stats_.vocab_cache_misses;

    if (stats_.reads_intercepted > 0) {
        stats_.avg_filter_time_ms = stats_.total_filter_time_ms / stats_.reads_intercepted;
    }

    // Deserialize vocabulary cache
    size_t cache_size;
    ss >> cache_size;
    ss.ignore();  // Skip newline

    vocabulary_cache_.clear();
    for (size_t i = 0; i < cache_size; ++i) {
        std::string key;
        int64_t value;
        if (std::getline(ss, key, '\t')) {
            ss >> value;
            ss.ignore();  // Skip newline
            vocabulary_cache_[key] = value;
        }
    }

    stats_.vocab_cache_size = vocabulary_cache_.size();

    return Status::OK();
}

std::string StringPredicateStrategy::GetStats() const {
    std::stringstream ss;
    ss << "StringPredicateStrategy:\n";
    ss << "  Reads intercepted: " << stats_.reads_intercepted << "\n";
    ss << "  Reads short-circuited: " << stats_.reads_short_circuited << "\n";
    ss << "  Rows filtered: " << stats_.rows_filtered << "\n";
    ss << "  Rows passed: " << stats_.rows_passed << "\n";
    ss << "  Total filter time: " << std::fixed << std::setprecision(2)
       << stats_.total_filter_time_ms << "ms\n";
    ss << "  Avg filter time: " << stats_.avg_filter_time_ms << "ms\n";
    ss << "  Vocabulary cache: " << stats_.vocab_cache_size << " entries\n";
    ss << "  Vocab cache hits: " << stats_.vocab_cache_hits << "\n";
    ss << "  Vocab cache misses: " << stats_.vocab_cache_misses << "\n";
    ss << "  SIMD module available: " << (string_ops_->IsAvailable() ? "yes" : "no") << "\n";
    return ss.str();
}

void StringPredicateStrategy::EnableColumn(const std::string& column_name) {
    enabled_columns_.insert(column_name);
}

void StringPredicateStrategy::DisableColumn(const std::string& column_name) {
    enabled_columns_.erase(column_name);
}

bool StringPredicateStrategy::IsColumnEnabled(const std::string& column_name) const {
    return enabled_columns_.find(column_name) != enabled_columns_.end();
}

std::unique_ptr<StringPredicateStrategy>
StringPredicateStrategy::AutoConfigure(const std::shared_ptr<arrow::Schema>& schema) {
    auto strategy = std::make_unique<StringPredicateStrategy>();

    // Enable all string columns
    for (int i = 0; i < schema->num_fields(); ++i) {
        auto field = schema->field(i);
        if (field->type()->id() == arrow::Type::STRING ||
            field->type()->id() == arrow::Type::LARGE_STRING) {
            strategy->EnableColumn(field->name());
        }
    }

    return strategy;
}

void StringPredicateStrategy::CacheVocabulary(const std::string& str, int64_t id) {
    if (vocabulary_cache_.size() >= max_vocabulary_cache_size_) {
        // Simple LRU: clear cache when full
        // TODO: Implement proper LRU eviction
        vocabulary_cache_.clear();
    }
    vocabulary_cache_[str] = id;
    stats_.vocab_cache_size = vocabulary_cache_.size();
}

int64_t StringPredicateStrategy::LookupVocabulary(const std::string& str) const {
    auto it = vocabulary_cache_.find(str);
    if (it != vocabulary_cache_.end()) {
        const_cast<Stats&>(stats_).vocab_cache_hits++;
        return it->second;
    }
    const_cast<Stats&>(stats_).vocab_cache_misses++;
    return -1;
}

void StringPredicateStrategy::ClearVocabulary() {
    vocabulary_cache_.clear();
    stats_.vocab_cache_size = 0;
}

Status StringPredicateStrategy::ApplyStringPredicate(ReadContext* ctx) {
    // This will be implemented once we have predicate passing infrastructure
    // For now, placeholder
    return Status::OK();
}

void StringPredicateStrategy::UpdateStats(
    const StringPredicateInfo& pred_info,
    double time_ms) {

    stats_.total_filter_time_ms += time_ms;
    stats_.rows_filtered += pred_info.rows_filtered;
    if (pred_info.can_skip_read) {
        stats_.reads_short_circuited++;
    }

    if (stats_.reads_intercepted > 0) {
        stats_.avg_filter_time_ms = stats_.total_filter_time_ms / stats_.reads_intercepted;
    }

    // Track predicate type usage
    std::string type_name;
    switch (pred_info.type) {
        case StringPredicateInfo::Type::EQUAL: type_name = "EQUAL"; break;
        case StringPredicateInfo::Type::NOT_EQUAL: type_name = "NOT_EQUAL"; break;
        case StringPredicateInfo::Type::CONTAINS: type_name = "CONTAINS"; break;
        case StringPredicateInfo::Type::STARTS_WITH: type_name = "STARTS_WITH"; break;
        case StringPredicateInfo::Type::ENDS_WITH: type_name = "ENDS_WITH"; break;
        case StringPredicateInfo::Type::REGEX: type_name = "REGEX"; break;
        default: type_name = "NONE"; break;
    }
    stats_.predicate_type_counts[type_name]++;
}

}  // namespace marble
