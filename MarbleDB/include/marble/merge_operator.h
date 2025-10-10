/************************************************************************
MarbleDB Merge Operators
Inspired by RocksDB's merge operators for efficient aggregations

Enables custom merge logic for counters, sets, lists without read-modify-write.
**************************************************************************/

#pragma once

#include <marble/status.h>
#include <memory>
#include <string>
#include <vector>

namespace marble {

/**
 * @brief Base class for merge operators
 * 
 * Merge operators allow associative operations on values without
 * reading the existing value first, enabling better performance for
 * counters, sets, and aggregations.
 * 
 * Inspired by RocksDB's MergeOperator design.
 */
class MergeOperator {
public:
    virtual ~MergeOperator() = default;
    
    /**
     * @brief Merge a list of operands into a final value
     * 
     * @param key The key being merged
     * @param existing_value Current value (may be nullptr if key doesn't exist)
     * @param operands List of merge operands to apply
     * @param result Output: merged result
     * @return Status OK on success
     * 
     * Example (counter):
     *   existing_value = "10"
     *   operands = ["+5", "+3", "+2"]
     *   result = "20"
     */
    virtual Status FullMerge(const std::string& key,
                            const std::string* existing_value,
                            const std::vector<std::string>& operands,
                            std::string* result) = 0;
    
    /**
     * @brief Partial merge of operands (optimization)
     * 
     * Combine multiple operands before full merge.
     * Default: no partial merge.
     */
    virtual Status PartialMerge(const std::string& key,
                               const std::vector<std::string>& operands,
                               std::string* result) {
        // Default: no partial merge optimization
        return Status::NotImplemented("PartialMerge not implemented");
    }
    
    /**
     * @brief Name of this merge operator
     */
    virtual const char* Name() const = 0;
};

/**
 * @brief Numeric counter merge operator (Int64)
 * 
 * Supports: +N, -N operations
 * Example: Merge(["+5", "+3", "-2"]) = +6
 */
class Int64AddOperator : public MergeOperator {
public:
    Status FullMerge(const std::string& key,
                    const std::string* existing_value,
                    const std::vector<std::string>& operands,
                    std::string* result) override;
    
    Status PartialMerge(const std::string& key,
                       const std::vector<std::string>& operands,
                       std::string* result) override;
    
    const char* Name() const override { return "Int64AddOperator"; }
};

/**
 * @brief String append merge operator
 * 
 * Concatenates strings with optional delimiter.
 */
class StringAppendOperator : public MergeOperator {
public:
    explicit StringAppendOperator(const std::string& delimiter = "")
        : delimiter_(delimiter) {}
    
    Status FullMerge(const std::string& key,
                    const std::string* existing_value,
                    const std::vector<std::string>& operands,
                    std::string* result) override;
    
    const char* Name() const override { return "StringAppendOperator"; }

private:
    std::string delimiter_;
};

/**
 * @brief Set union merge operator
 * 
 * Maintains unique set of strings (comma-separated).
 * Example: Merge(["a,b", "b,c", "c,d"]) = "a,b,c,d"
 */
class SetUnionOperator : public MergeOperator {
public:
    Status FullMerge(const std::string& key,
                    const std::string* existing_value,
                    const std::vector<std::string>& operands,
                    std::string* result) override;
    
    const char* Name() const override { return "SetUnionOperator"; }
};

/**
 * @brief Max value merge operator
 * 
 * Keeps the maximum value (numeric comparison).
 */
class MaxOperator : public MergeOperator {
public:
    Status FullMerge(const std::string& key,
                    const std::string* existing_value,
                    const std::vector<std::string>& operands,
                    std::string* result) override;
    
    const char* Name() const override { return "MaxOperator"; }
};

/**
 * @brief Min value merge operator
 */
class MinOperator : public MergeOperator {
public:
    Status FullMerge(const std::string& key,
                    const std::string* existing_value,
                    const std::vector<std::string>& operands,
                    std::string* result) override;
    
    const char* Name() const override { return "MinOperator"; }
};

/**
 * @brief JSON merge operator
 * 
 * Merges JSON objects by updating fields.
 * Requires JSON library (optional).
 */
class JsonMergeOperator : public MergeOperator {
public:
    Status FullMerge(const std::string& key,
                    const std::string* existing_value,
                    const std::vector<std::string>& operands,
                    std::string* result) override;
    
    const char* Name() const override { return "JsonMergeOperator"; }
};

/**
 * @brief Factory for creating built-in merge operators
 */
class MergeOperatorFactory {
public:
    enum class Type {
        kInt64Add,
        kStringAppend,
        kSetUnion,
        kMax,
        kMin,
        kJsonMerge
    };
    
    static std::shared_ptr<MergeOperator> Create(Type type, const std::string& config = "");
};

} // namespace marble

