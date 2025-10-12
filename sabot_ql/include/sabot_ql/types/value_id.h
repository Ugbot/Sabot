#pragma once

#include <cstdint>
#include <cstring>
#include <limits>
#include <stdexcept>
#include <string>
#include <string_view>

namespace sabot_ql {

// Datatype enum - simplified from QLever's implementation
// Uses 4 bits for type encoding (16 possible types)
enum class Datatype : uint8_t {
    Undefined = 0,
    Bool = 1,
    Int = 2,
    Double = 3,
    VocabIndex = 4,        // IRI and Literal strings in vocabulary
    LocalVocabIndex = 5,   // Local vocabulary (query-specific)
    TextRecordIndex = 6,   // Full-text search records
    Date = 7,
    GeoPoint = 8,
    WordVocabIndex = 9,
    BlankNodeIndex = 10,
    EncodedVal = 11,
    MaxValue = EncodedVal
};

// Convert Datatype to string
constexpr std::string_view toString(Datatype type) {
    switch (type) {
        case Datatype::Undefined: return "Undefined";
        case Datatype::Bool: return "Bool";
        case Datatype::Int: return "Int";
        case Datatype::Double: return "Double";
        case Datatype::VocabIndex: return "VocabIndex";
        case Datatype::LocalVocabIndex: return "LocalVocabIndex";
        case Datatype::TextRecordIndex: return "TextRecordIndex";
        case Datatype::Date: return "Date";
        case Datatype::GeoPoint: return "GeoPoint";
        case Datatype::WordVocabIndex: return "WordVocabIndex";
        case Datatype::BlankNodeIndex: return "BlankNodeIndex";
        case Datatype::EncodedVal: return "EncodedVal";
    }
    return "Unknown";
}

// Strong-typed index wrappers (simplified from QLever)
struct VocabIndex {
    uint64_t value;

    static VocabIndex make(uint64_t v) { return {v}; }
    uint64_t get() const { return value; }

    bool operator==(const VocabIndex& other) const { return value == other.value; }
    bool operator<(const VocabIndex& other) const { return value < other.value; }
};

struct BlankNodeIndex {
    uint64_t value;

    static BlankNodeIndex make(uint64_t v) { return {v}; }
    uint64_t get() const { return value; }

    bool operator==(const BlankNodeIndex& other) const { return value == other.value; }
    bool operator<(const BlankNodeIndex& other) const { return value < other.value; }
};

// ValueId: 64-bit encoded value with 4 bits for type, 60 bits for data
// Simplified from QLever's implementation, standalone with no external dependencies
class ValueId {
public:
    using T = uint64_t;
    static constexpr T numDatatypeBits = 4;
    static constexpr T numDataBits = 64 - numDatatypeBits;  // 60 bits

    // Maximum value for unsigned index types
    static constexpr T maxIndex = (1ull << numDataBits) - 1;

    // Maximum representable signed integer (60-bit range)
    static constexpr int64_t maxInt = (1ll << (numDataBits - 1)) - 1;
    static constexpr int64_t minInt = -(1ll << (numDataBits - 1));

private:
    T _bits;  // The actual 64-bit representation

public:
    // Default construction
    ValueId() = default;

    // Get underlying bit representation
    constexpr T getBits() const noexcept { return _bits; }

    // Construct from bits
    static constexpr ValueId fromBits(T bits) noexcept { return ValueId(bits); }

    // Get the datatype (top 4 bits)
    constexpr Datatype getDatatype() const noexcept {
        return static_cast<Datatype>(_bits >> numDataBits);
    }

    // ===== Factory methods for creating ValueIds =====

    // Undefined value
    static constexpr ValueId makeUndefined() noexcept { return ValueId(0); }
    bool isUndefined() const noexcept { return _bits == 0; }

    // Bool values
    static constexpr ValueId makeFromBool(bool b) noexcept {
        return addDatatypeBits(static_cast<T>(b), Datatype::Bool);
    }

    bool getBool() const noexcept {
        return static_cast<bool>(removeDatatypeBits(_bits) & 1);
    }

    // Integer values (60-bit signed)
    static ValueId makeFromInt(int64_t i) {
        // Check range
        if (i < minInt || i > maxInt) {
            throw std::overflow_error("Integer value outside 60-bit range");
        }

        // Convert to 60-bit two's complement representation
        uint64_t bits = static_cast<uint64_t>(i) & ((1ull << numDataBits) - 1);
        return addDatatypeBits(bits, Datatype::Int);
    }

    int64_t getInt() const noexcept {
        uint64_t bits = removeDatatypeBits(_bits);

        // Check sign bit (bit 59)
        if (bits & (1ull << (numDataBits - 1))) {
            // Negative number - sign extend
            return static_cast<int64_t>(bits | ~((1ull << numDataBits) - 1));
        } else {
            // Positive number
            return static_cast<int64_t>(bits);
        }
    }

    // Double values (reduced precision: 60 bits instead of 64)
    static ValueId makeFromDouble(double d) {
        uint64_t double_bits;
        std::memcpy(&double_bits, &d, sizeof(double));

        // Shift right 4 bits to make room for datatype
        uint64_t shifted = double_bits >> numDatatypeBits;
        return addDatatypeBits(shifted, Datatype::Double);
    }

    double getDouble() const noexcept {
        // Shift left 4 bits to restore double format (loses 4 bits of precision)
        uint64_t shifted = _bits << numDatatypeBits;
        double result;
        std::memcpy(&result, &shifted, sizeof(double));
        return result;
    }

    // Vocabulary index (for IRI and Literal strings)
    static ValueId makeFromVocabIndex(VocabIndex index) {
        return makeFromIndex(index.get(), Datatype::VocabIndex);
    }

    constexpr VocabIndex getVocabIndex() const noexcept {
        return VocabIndex::make(removeDatatypeBits(_bits));
    }

    // Blank node index
    static ValueId makeFromBlankNodeIndex(BlankNodeIndex index) {
        return makeFromIndex(index.get(), Datatype::BlankNodeIndex);
    }

    constexpr BlankNodeIndex getBlankNodeIndex() const noexcept {
        return BlankNodeIndex::make(removeDatatypeBits(_bits));
    }

    // ===== Comparison operators =====

    bool operator==(const ValueId& other) const noexcept {
        return _bits == other._bits;
    }

    bool operator!=(const ValueId& other) const noexcept {
        return _bits != other._bits;
    }

    bool operator<(const ValueId& other) const noexcept {
        return _bits < other._bits;
    }

    bool operator<=(const ValueId& other) const noexcept {
        return _bits <= other._bits;
    }

    bool operator>(const ValueId& other) const noexcept {
        return _bits > other._bits;
    }

    bool operator>=(const ValueId& other) const noexcept {
        return _bits >= other._bits;
    }

private:
    // Private constructor from bits
    constexpr explicit ValueId(T bits) : _bits(bits) {}

    // Add datatype bits (top 4 bits)
    static constexpr ValueId addDatatypeBits(T bits, Datatype type) noexcept {
        T mask = static_cast<T>(type) << numDataBits;
        return ValueId(bits | mask);
    }

    // Remove datatype bits (extract bottom 60 bits)
    static constexpr T removeDatatypeBits(T bits) noexcept {
        T mask = (1ull << numDataBits) - 1;
        return bits & mask;
    }

    // Helper for index types
    static ValueId makeFromIndex(T id, Datatype type) {
        if (id > maxIndex) {
            throw std::overflow_error("Index value exceeds 60-bit maximum");
        }
        return addDatatypeBits(id, type);
    }
};

// TermKind enum - identifies RDF term kinds
enum class TermKind : uint8_t {
    Undefined = 0,
    IRI = 1,
    Literal = 2,
    BlankNode = 3
};

// Compatibility layer with old value_id namespace
namespace value_id {
    inline Datatype GetType(ValueId id) {
        return id.getDatatype();
    }

    inline bool IsUndefined(ValueId id) {
        return id.isUndefined();
    }

    inline uint64_t GetVocabIndex(ValueId id) {
        return id.getVocabIndex().get();
    }

    inline uint64_t GetBlankNodeIndex(ValueId id) {
        return id.getBlankNodeIndex().get();
    }

    inline int64_t GetInt(ValueId id) {
        return id.getInt();
    }

    inline double GetDouble(ValueId id) {
        return id.getDouble();
    }

    inline bool GetBool(ValueId id) {
        return id.getBool();
    }
}

} // namespace sabot_ql

// Hash function for ValueId (for std::unordered_map)
namespace std {
template <>
struct hash<sabot_ql::ValueId> {
    size_t operator()(const sabot_ql::ValueId& id) const noexcept {
        return std::hash<uint64_t>{}(id.getBits());
    }
};
} // namespace std
