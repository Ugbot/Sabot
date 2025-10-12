#pragma once

#include <cstdint>
#include <string>
#include <optional>

namespace sabot_ql {

// ValueId: 64-bit encoded value for RDF terms
// Inspired by QLever's design:
// - 4 bits for datatype tag
// - 60 bits for data/index
//
// This allows efficient storage and comparison of RDF terms
// while maintaining type information.

using ValueId = uint64_t;

// RDF term types
enum class TermKind : uint8_t {
    IRI = 0,          // <http://example.org/...>
    Literal = 1,      // "string literal"
    BlankNode = 2,    // _:b1
    Int = 3,          // Inline integer
    Double = 4,       // Inline double
    Bool = 5,         // Inline boolean
    Undefined = 15    // Special undefined value
};

// ValueId encoding/decoding utilities
namespace value_id {

constexpr uint64_t TYPE_BITS = 4;
constexpr uint64_t DATA_BITS = 64 - TYPE_BITS;
constexpr uint64_t DATA_MASK = (1ULL << DATA_BITS) - 1;

// Encode a vocabulary index (IRI, Literal, BlankNode)
inline ValueId EncodeVocabIndex(uint64_t vocab_index, TermKind kind) {
    return (static_cast<uint64_t>(kind) << DATA_BITS) | (vocab_index & DATA_MASK);
}

// Encode an inline integer
inline ValueId EncodeInt(int64_t value) {
    return (static_cast<uint64_t>(TermKind::Int) << DATA_BITS) |
           (static_cast<uint64_t>(value) & DATA_MASK);
}

// Encode an inline double (with precision loss)
inline ValueId EncodeDouble(double value) {
    uint64_t bits = *reinterpret_cast<uint64_t*>(&value);
    uint64_t data = bits >> TYPE_BITS;  // Lose some precision
    return (static_cast<uint64_t>(TermKind::Double) << DATA_BITS) | data;
}

// Encode an inline boolean
inline ValueId EncodeBool(bool value) {
    return (static_cast<uint64_t>(TermKind::Bool) << DATA_BITS) |
           (value ? 1ULL : 0ULL);
}

// Special undefined value (for OPTIONAL, unbound variables)
inline ValueId EncodeUndefined() {
    return (static_cast<uint64_t>(TermKind::Undefined) << DATA_BITS);
}

// Get the type of a ValueId
inline TermKind GetType(ValueId id) {
    return static_cast<TermKind>(id >> DATA_BITS);
}

// Get the data portion of a ValueId
inline uint64_t GetData(ValueId id) {
    return id & DATA_MASK;
}

// Check if ValueId is undefined
inline bool IsUndefined(ValueId id) {
    return GetType(id) == TermKind::Undefined;
}

// Decode vocabulary index
inline std::optional<uint64_t> DecodeVocabIndex(ValueId id) {
    TermKind kind = GetType(id);
    if (kind == TermKind::IRI || kind == TermKind::Literal || kind == TermKind::BlankNode) {
        return GetData(id);
    }
    return std::nullopt;
}

// Decode inline integer
inline std::optional<int64_t> DecodeInt(ValueId id) {
    if (GetType(id) == TermKind::Int) {
        return static_cast<int64_t>(GetData(id));
    }
    return std::nullopt;
}

// Decode inline double
inline std::optional<double> DecodeDouble(ValueId id) {
    if (GetType(id) == TermKind::Double) {
        uint64_t data = GetData(id) << TYPE_BITS;
        return *reinterpret_cast<double*>(&data);
    }
    return std::nullopt;
}

// Decode inline boolean
inline std::optional<bool> DecodeBool(ValueId id) {
    if (GetType(id) == TermKind::Bool) {
        return GetData(id) != 0;
    }
    return std::nullopt;
}

} // namespace value_id

} // namespace sabot_ql
