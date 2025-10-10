/************************************************************************
MarbleDB Merge Operator Implementations
**************************************************************************/

#include "marble/merge_operator.h"
#include <algorithm>
#include <sstream>
#include <unordered_set>
#include <cstdlib>

namespace marble {

// Int64AddOperator implementation
Status Int64AddOperator::FullMerge(const std::string& key,
                                   const std::string* existing_value,
                                   const std::vector<std::string>& operands,
                                   std::string* result) {
    int64_t total = 0;
    
    // Start with existing value
    if (existing_value) {
        try {
            total = std::stoll(*existing_value);
        } catch (...) {
            return Status::InvalidArgument("Existing value is not a valid int64");
        }
    }
    
    // Apply all operands
    for (const auto& operand : operands) {
        try {
            int64_t delta = std::stoll(operand);
            total += delta;
        } catch (...) {
            return Status::InvalidArgument("Operand is not a valid int64: " + operand);
        }
    }
    
    *result = std::to_string(total);
    return Status::OK();
}

Status Int64AddOperator::PartialMerge(const std::string& key,
                                      const std::vector<std::string>& operands,
                                      std::string* result) {
    // Optimize by summing all operands first
    int64_t sum = 0;
    
    for (const auto& operand : operands) {
        try {
            sum += std::stoll(operand);
        } catch (...) {
            return Status::InvalidArgument("Operand is not a valid int64: " + operand);
        }
    }
    
    *result = std::to_string(sum);
    return Status::OK();
}

// StringAppendOperator implementation
Status StringAppendOperator::FullMerge(const std::string& key,
                                       const std::string* existing_value,
                                       const std::vector<std::string>& operands,
                                       std::string* result) {
    std::ostringstream oss;
    
    // Start with existing value
    if (existing_value && !existing_value->empty()) {
        oss << *existing_value;
    }
    
    // Append all operands
    for (const auto& operand : operands) {
        if (oss.tellp() > 0 && !delimiter_.empty()) {
            oss << delimiter_;
        }
        oss << operand;
    }
    
    *result = oss.str();
    return Status::OK();
}

// SetUnionOperator implementation
Status SetUnionOperator::FullMerge(const std::string& key,
                                   const std::string* existing_value,
                                   const std::vector<std::string>& operands,
                                   std::string* result) {
    std::unordered_set<std::string> unique_items;
    
    // Parse existing value (comma-separated)
    if (existing_value && !existing_value->empty()) {
        std::istringstream iss(*existing_value);
        std::string item;
        while (std::getline(iss, item, ',')) {
            unique_items.insert(item);
        }
    }
    
    // Add items from operands
    for (const auto& operand : operands) {
        std::istringstream iss(operand);
        std::string item;
        while (std::getline(iss, item, ',')) {
            unique_items.insert(item);
        }
    }
    
    // Build result (sorted for determinism)
    std::vector<std::string> sorted_items(unique_items.begin(), unique_items.end());
    std::sort(sorted_items.begin(), sorted_items.end());
    
    std::ostringstream oss;
    for (size_t i = 0; i < sorted_items.size(); ++i) {
        if (i > 0) oss << ",";
        oss << sorted_items[i];
    }
    
    *result = oss.str();
    return Status::OK();
}

// MaxOperator implementation
Status MaxOperator::FullMerge(const std::string& key,
                              const std::string* existing_value,
                              const std::vector<std::string>& operands,
                              std::string* result) {
    int64_t max_val = INT64_MIN;
    bool has_value = false;
    
    // Check existing value
    if (existing_value) {
        try {
            max_val = std::stoll(*existing_value);
            has_value = true;
        } catch (...) {
            return Status::InvalidArgument("Existing value is not numeric");
        }
    }
    
    // Check all operands
    for (const auto& operand : operands) {
        try {
            int64_t val = std::stoll(operand);
            if (!has_value || val > max_val) {
                max_val = val;
                has_value = true;
            }
        } catch (...) {
            return Status::InvalidArgument("Operand is not numeric: " + operand);
        }
    }
    
    if (!has_value) {
        return Status::InvalidArgument("No valid values to merge");
    }
    
    *result = std::to_string(max_val);
    return Status::OK();
}

// MinOperator implementation
Status MinOperator::FullMerge(const std::string& key,
                              const std::string* existing_value,
                              const std::vector<std::string>& operands,
                              std::string* result) {
    int64_t min_val = INT64_MAX;
    bool has_value = false;
    
    // Check existing value
    if (existing_value) {
        try {
            min_val = std::stoll(*existing_value);
            has_value = true;
        } catch (...) {
            return Status::InvalidArgument("Existing value is not numeric");
        }
    }
    
    // Check all operands
    for (const auto& operand : operands) {
        try {
            int64_t val = std::stoll(operand);
            if (!has_value || val < min_val) {
                min_val = val;
                has_value = true;
            }
        } catch (...) {
            return Status::InvalidArgument("Operand is not numeric: " + operand);
        }
    }
    
    if (!has_value) {
        return Status::InvalidArgument("No valid values to merge");
    }
    
    *result = std::to_string(min_val);
    return Status::OK();
}

// JsonMergeOperator implementation (placeholder)
Status JsonMergeOperator::FullMerge(const std::string& key,
                                    const std::string* existing_value,
                                    const std::vector<std::string>& operands,
                                    std::string* result) {
    // TODO: Implement JSON merge using nlohmann/json or similar
    // For now, return last operand
    if (operands.empty()) {
        if (existing_value) {
            *result = *existing_value;
        } else {
            *result = "{}";
        }
    } else {
        *result = operands.back();
    }
    
    return Status::OK();
}

// MergeOperatorFactory implementation
std::shared_ptr<MergeOperator> MergeOperatorFactory::Create(Type type, const std::string& config) {
    switch (type) {
        case Type::kInt64Add:
            return std::make_shared<Int64AddOperator>();
        case Type::kStringAppend: {
            // Config is delimiter
            return std::make_shared<StringAppendOperator>(config);
        }
        case Type::kSetUnion:
            return std::make_shared<SetUnionOperator>();
        case Type::kMax:
            return std::make_shared<MaxOperator>();
        case Type::kMin:
            return std::make_shared<MinOperator>();
        case Type::kJsonMerge:
            return std::make_shared<JsonMergeOperator>();
        default:
            return nullptr;
    }
}

} // namespace marble

