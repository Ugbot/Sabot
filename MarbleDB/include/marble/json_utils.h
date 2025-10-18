/************************************************************************
MarbleDB JSON Utilities
High-performance JSON parsing and serialization using simdjson

All JSON operations in MarbleDB use simdjson for maximum performance:
- Parsing: 4GB/s+ (4x faster than RapidJSON)
- Validation: 13GB/s UTF-8 validation
- SIMD-accelerated, zero-copy where possible

Copyright 2024 MarbleDB Contributors
Licensed under the Apache License, Version 2.0
**************************************************************************/

#pragma once

#include "../../vendor/simdjson/singleheader/simdjson.h"
#include <marble/status.h>
#include <string>
#include <vector>
#include <cstdint>

namespace marble {
namespace json {

/**
 * @brief JSON parser wrapper around simdjson
 *
 * Provides convenient parsing with error handling
 */
class Parser {
public:
    Parser() = default;
    ~Parser() = default;

    /**
     * @brief Parse JSON string
     *
     * @param json_str JSON string to parse
     * @param doc Output document
     * @return Status OK on success
     */
    Status Parse(const std::string& json_str, simdjson::ondemand::document& doc) {
        try {
            padded_string_ = simdjson::padded_string(json_str);
            auto result = parser_.iterate(padded_string_);
            if (result.error()) {
                return Status::InvalidArgument("Failed to parse JSON: " +
                    std::string(simdjson::error_message(result.error())));
            }
            doc = std::move(result.value_unsafe());
            return Status::OK();
        } catch (const simdjson::simdjson_error& e) {
            return Status::InvalidArgument("JSON parse error: " + std::string(e.what()));
        }
    }

    /**
     * @brief Parse JSON from buffer
     */
    Status Parse(const char* json_data, size_t length, simdjson::ondemand::document& doc) {
        try {
            padded_string_ = simdjson::padded_string(json_data, length);
            auto result = parser_.iterate(padded_string_);
            if (result.error()) {
                return Status::InvalidArgument("Failed to parse JSON: " +
                    std::string(simdjson::error_message(result.error())));
            }
            doc = std::move(result.value_unsafe());
            return Status::OK();
        } catch (const simdjson::simdjson_error& e) {
            return Status::InvalidArgument("JSON parse error: " + std::string(e.what()));
        }
    }

private:
    simdjson::ondemand::parser parser_;
    simdjson::padded_string padded_string_;
};

/**
 * @brief JSON builder for serialization
 *
 * Lightweight JSON builder that generates valid JSON strings
 */
class Builder {
public:
    Builder() {
        buffer_.reserve(1024);  // Pre-allocate 1KB
    }

    void StartObject() {
        buffer_ += "{";
        needs_comma_ = false;
    }

    void EndObject() {
        buffer_ += "}";
        needs_comma_ = true;
    }

    void StartArray() {
        buffer_ += "[";
        needs_comma_ = false;
    }

    void EndArray() {
        buffer_ += "]";
        needs_comma_ = true;
    }

    void Key(const std::string& key) {
        if (needs_comma_) buffer_ += ",";
        buffer_ += "\"" + key + "\":";
        needs_comma_ = false;
    }

    void String(const std::string& value) {
        if (needs_comma_) buffer_ += ",";
        buffer_ += "\"" + EscapeString(value) + "\"";
        needs_comma_ = true;
    }

    void Int(int64_t value) {
        if (needs_comma_) buffer_ += ",";
        buffer_ += std::to_string(value);
        needs_comma_ = true;
    }

    void UInt(uint64_t value) {
        if (needs_comma_) buffer_ += ",";
        buffer_ += std::to_string(value);
        needs_comma_ = true;
    }

    void Double(double value) {
        if (needs_comma_) buffer_ += ",";
        buffer_ += std::to_string(value);
        needs_comma_ = true;
    }

    void Bool(bool value) {
        if (needs_comma_) buffer_ += ",";
        buffer_ += value ? "true" : "false";
        needs_comma_ = true;
    }

    void Null() {
        if (needs_comma_) buffer_ += ",";
        buffer_ += "null";
        needs_comma_ = true;
    }

    std::string ToString() const {
        return buffer_;
    }

    void Clear() {
        buffer_.clear();
        needs_comma_ = false;
    }

private:
    std::string EscapeString(const std::string& str) const {
        std::string result;
        result.reserve(str.size());
        for (char c : str) {
            switch (c) {
                case '"':  result += "\\\""; break;
                case '\\': result += "\\\\"; break;
                case '\b': result += "\\b"; break;
                case '\f': result += "\\f"; break;
                case '\n': result += "\\n"; break;
                case '\r': result += "\\r"; break;
                case '\t': result += "\\t"; break;
                default:   result += c; break;
            }
        }
        return result;
    }

    std::string buffer_;
    bool needs_comma_ = false;
};

/**
 * @brief Helper functions for common JSON operations
 */

// Get string value from JSON object
inline Status GetString(simdjson::ondemand::object& obj, const char* key, std::string* out) {
    try {
        auto value = obj[key];
        if (value.error()) {
            return Status::NotFound(std::string("Key not found: ") + key);
        }
        auto str = value.get_string();
        if (str.error()) {
            return Status::InvalidArgument(std::string("Not a string: ") + key);
        }
        *out = std::string(str.value_unsafe());
        return Status::OK();
    } catch (const simdjson::simdjson_error& e) {
        return Status::InvalidArgument(std::string("JSON error: ") + e.what());
    }
}

// Get int64 value from JSON object
inline Status GetInt64(simdjson::ondemand::object& obj, const char* key, int64_t* out) {
    try {
        auto value = obj[key];
        if (value.error()) {
            return Status::NotFound(std::string("Key not found: ") + key);
        }
        auto num = value.get_int64();
        if (num.error()) {
            return Status::InvalidArgument(std::string("Not an int64: ") + key);
        }
        *out = num.value_unsafe();
        return Status::OK();
    } catch (const simdjson::simdjson_error& e) {
        return Status::InvalidArgument(std::string("JSON error: ") + e.what());
    }
}

// Get uint64 value from JSON object
inline Status GetUInt64(simdjson::ondemand::object& obj, const char* key, uint64_t* out) {
    try {
        auto value = obj[key];
        if (value.error()) {
            return Status::NotFound(std::string("Key not found: ") + key);
        }
        auto num = value.get_uint64();
        if (num.error()) {
            return Status::InvalidArgument(std::string("Not a uint64: ") + key);
        }
        *out = num.value_unsafe();
        return Status::OK();
    } catch (const simdjson::simdjson_error& e) {
        return Status::InvalidArgument(std::string("JSON error: ") + e.what());
    }
}

// Get bool value from JSON object
inline Status GetBool(simdjson::ondemand::object& obj, const char* key, bool* out) {
    try {
        auto value = obj[key];
        if (value.error()) {
            return Status::NotFound(std::string("Key not found: ") + key);
        }
        auto boolean = value.get_bool();
        if (boolean.error()) {
            return Status::InvalidArgument(std::string("Not a bool: ") + key);
        }
        *out = boolean.value_unsafe();
        return Status::OK();
    } catch (const simdjson::simdjson_error& e) {
        return Status::InvalidArgument(std::string("JSON error: ") + e.what());
    }
}

// Get array of strings from JSON object
inline Status GetStringArray(simdjson::ondemand::object& obj, const char* key, std::vector<std::string>* out) {
    try {
        auto value = obj[key];
        if (value.error()) {
            return Status::NotFound(std::string("Key not found: ") + key);
        }
        auto array = value.get_array();
        if (array.error()) {
            return Status::InvalidArgument(std::string("Not an array: ") + key);
        }

        out->clear();
        for (auto element : array.value_unsafe()) {
            auto str = element.get_string();
            if (str.error()) {
                return Status::InvalidArgument("Array element is not a string");
            }
            out->push_back(std::string(str.value_unsafe()));
        }
        return Status::OK();
    } catch (const simdjson::simdjson_error& e) {
        return Status::InvalidArgument(std::string("JSON error: ") + e.what());
    }
}

// Try to get optional value (returns OK if key doesn't exist)
template<typename T>
inline Status TryGet(simdjson::ondemand::object& obj, const char* key,
                     Status (*getter)(simdjson::ondemand::object&, const char*, T*), T* out) {
    auto status = getter(obj, key, out);
    if (status.IsNotFound()) {
        return Status::OK();  // Optional field, ignore
    }
    return status;
}

} // namespace json
} // namespace marble
