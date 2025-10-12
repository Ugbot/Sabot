//===----------------------------------------------------------------------===//
//                         SabotSQL
//
// sabot_sql/function/compression/compression.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sabot_sql/function/compression_function.hpp"
#include "sabot_sql/function/function_set.hpp"

namespace sabot_sql {

struct ConstantFun {
	static CompressionFunction GetFunction(PhysicalType type);
	static bool TypeIsSupported(const PhysicalType physical_type);
};

struct UncompressedFun {
	static CompressionFunction GetFunction(PhysicalType type);
	static bool TypeIsSupported(const PhysicalType physical_type);
};

struct RLEFun {
	static CompressionFunction GetFunction(PhysicalType type);
	static bool TypeIsSupported(const PhysicalType physical_type);
};

struct BitpackingFun {
	static CompressionFunction GetFunction(PhysicalType type);
	static bool TypeIsSupported(const PhysicalType physical_type);
};

struct DictionaryCompressionFun {
	static CompressionFunction GetFunction(PhysicalType type);
	static bool TypeIsSupported(const PhysicalType physical_type);
};

struct DictFSSTCompressionFun {
	static CompressionFunction GetFunction(PhysicalType type);
	static bool TypeIsSupported(const PhysicalType physical_type);
};

struct ChimpCompressionFun {
	static CompressionFunction GetFunction(PhysicalType type);
	static bool TypeIsSupported(const PhysicalType physical_type);
};

struct PatasCompressionFun {
	static CompressionFunction GetFunction(PhysicalType type);
	static bool TypeIsSupported(const PhysicalType physical_type);
};

struct AlpCompressionFun {
	static CompressionFunction GetFunction(PhysicalType type);
	static bool TypeIsSupported(const PhysicalType physical_type);
};

struct AlpRDCompressionFun {
	static CompressionFunction GetFunction(PhysicalType type);
	static bool TypeIsSupported(const PhysicalType physical_type);
};

struct FSSTFun {
	static CompressionFunction GetFunction(PhysicalType type);
	static bool TypeIsSupported(const PhysicalType physical_type);
};

struct ZSTDFun {
	static CompressionFunction GetFunction(PhysicalType type);
	static bool TypeIsSupported(PhysicalType type);
};

struct RoaringCompressionFun {
	static CompressionFunction GetFunction(PhysicalType type);
	static bool TypeIsSupported(const PhysicalType physical_type);
};

struct EmptyValidityCompressionFun {
	static CompressionFunction GetFunction(PhysicalType type);
	static bool TypeIsSupported(const PhysicalType physical_type);
};

} // namespace sabot_sql
