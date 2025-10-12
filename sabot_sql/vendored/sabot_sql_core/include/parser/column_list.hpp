//===----------------------------------------------------------------------===//
//                         SabotSQL
//
// sabot_sql/parser/column_list.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sabot_sql/parser/column_definition.hpp"

namespace sabot_sql {

//! A set of column definitions
class ColumnList {
public:
	class ColumnListIterator;

public:
	SABOT_SQL_API explicit ColumnList(bool allow_duplicate_names = false);
	SABOT_SQL_API explicit ColumnList(vector<ColumnDefinition> columns, bool allow_duplicate_names = false);

	SABOT_SQL_API void AddColumn(ColumnDefinition column);
	void Finalize();

	SABOT_SQL_API const ColumnDefinition &GetColumn(LogicalIndex index) const;
	SABOT_SQL_API const ColumnDefinition &GetColumn(PhysicalIndex index) const;
	SABOT_SQL_API const ColumnDefinition &GetColumn(const string &name) const;
	SABOT_SQL_API ColumnDefinition &GetColumnMutable(LogicalIndex index);
	SABOT_SQL_API ColumnDefinition &GetColumnMutable(PhysicalIndex index);
	SABOT_SQL_API ColumnDefinition &GetColumnMutable(const string &name);
	SABOT_SQL_API vector<string> GetColumnNames() const;
	SABOT_SQL_API vector<LogicalType> GetColumnTypes() const;

	SABOT_SQL_API bool ColumnExists(const string &name) const;

	SABOT_SQL_API LogicalIndex GetColumnIndex(string &column_name) const;
	SABOT_SQL_API PhysicalIndex LogicalToPhysical(LogicalIndex index) const;
	SABOT_SQL_API LogicalIndex PhysicalToLogical(PhysicalIndex index) const;

	idx_t LogicalColumnCount() const {
		return columns.size();
	}
	idx_t PhysicalColumnCount() const {
		return physical_columns.size();
	}
	bool empty() const { // NOLINT: match stl API
		return columns.empty();
	}

	ColumnList Copy() const;
	void Serialize(Serializer &serializer) const;
	static ColumnList Deserialize(Deserializer &deserializer);

	SABOT_SQL_API ColumnListIterator Logical() const;
	SABOT_SQL_API ColumnListIterator Physical() const;

	void SetAllowDuplicates(bool allow_duplicates) {
		allow_duplicate_names = allow_duplicates;
	}

private:
	vector<ColumnDefinition> columns;
	//! A map of column name to column index
	case_insensitive_map_t<column_t> name_map;
	//! The set of physical columns
	vector<idx_t> physical_columns;
	//! Allow duplicate names or not
	bool allow_duplicate_names;

private:
	void AddToNameMap(ColumnDefinition &column);

public:
	// logical iterator
	class ColumnListIterator {
	public:
		ColumnListIterator(const ColumnList &list, bool physical) : list(list), physical(physical) {
		}

	private:
		const ColumnList &list;
		bool physical;

	private:
		class ColumnLogicalIteratorInternal {
		public:
			ColumnLogicalIteratorInternal(const ColumnList &list, bool physical, idx_t pos, idx_t end)
			    : list(list), physical(physical), pos(pos), end(end) {
			}

			const ColumnList &list;
			bool physical;
			idx_t pos;
			idx_t end;

		public:
			ColumnLogicalIteratorInternal &operator++() {
				pos++;
				return *this;
			}
			bool operator!=(const ColumnLogicalIteratorInternal &other) const {
				return pos != other.pos || end != other.end || &list != &other.list;
			}
			const ColumnDefinition &operator*() const {
				if (physical) {
					return list.GetColumn(PhysicalIndex(pos));
				} else {
					return list.GetColumn(LogicalIndex(pos));
				}
			}
		};

	public:
		idx_t Size() {
			return physical ? list.PhysicalColumnCount() : list.LogicalColumnCount();
		}

		ColumnLogicalIteratorInternal begin() { // NOLINT: match stl API
			return ColumnLogicalIteratorInternal(list, physical, 0, Size());
		}
		ColumnLogicalIteratorInternal end() { // NOLINT: match stl API
			return ColumnLogicalIteratorInternal(list, physical, Size(), Size());
		}
	};
};

} // namespace sabot_sql
