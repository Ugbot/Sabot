//===----------------------------------------------------------------------===//
//                         SabotSQL
//
// sabot_sql/common/filename_pattern.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sabot_sql/common/file_system.hpp"
#include "sabot_sql/common/types/uuid.hpp"

namespace sabot_sql {

class Serializer;
class Deserializer;

enum class FileNameSegmentType : uint8_t { LITERAL, UUID_V4, UUID_V7, OFFSET };

struct FileNameSegment {
	FileNameSegment() = default;
	explicit FileNameSegment(string data);
	explicit FileNameSegment(FileNameSegmentType type);

	FileNameSegmentType type;
	string data;

public:
	void Serialize(Serializer &serializer) const;
	static FileNameSegment Deserialize(Deserializer &deserializer);
};

class FilenamePattern {
public:
	FilenamePattern();
	FilenamePattern(string base, idx_t pos, bool uuid, vector<FileNameSegment> segments);

public:
	void SetFilenamePattern(const string &pattern);
	string CreateFilename(FileSystem &fs, const string &path, const string &extension, idx_t offset) const;

	void Serialize(Serializer &serializer) const;
	static FilenamePattern Deserialize(Deserializer &deserializer);

	bool HasUUID() const;

public:
	// serialization code for backwards compatibility
	string SerializeBase() const;
	idx_t SerializePos() const;
	vector<FileNameSegment> SerializeSegments() const;

private:
	vector<FileNameSegment> segments;
};

} // namespace sabot_sql
