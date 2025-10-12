//===----------------------------------------------------------------------===//
//                         SabotSQL
//
// sabot_sql/catalog/catalog_entry/column_dependency_manager.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sabot_sql/common/case_insensitive_map.hpp"
#include "sabot_sql/common/string.hpp"
#include "sabot_sql/parser/column_list.hpp"
#include "sabot_sql/common/set.hpp"
#include "sabot_sql/common/stack.hpp"
#include "sabot_sql/common/index_map.hpp"

namespace sabot_sql {

//! Dependency Manager local to a table, responsible for keeping track of generated column dependencies

class ColumnDependencyManager {
public:
	SABOT_SQL_API ColumnDependencyManager();
	SABOT_SQL_API ~ColumnDependencyManager();
	ColumnDependencyManager(ColumnDependencyManager &&other) = default;
	ColumnDependencyManager(const ColumnDependencyManager &other) = delete;

public:
	//! Get the bind order that ensures dependencies are resolved before dependents are
	stack<LogicalIndex> GetBindOrder(const ColumnList &columns);

	//! Adds a connection between the dependent and its dependencies
	void AddGeneratedColumn(LogicalIndex index, const vector<LogicalIndex> &indices, bool root = true);
	//! Add a generated column from a column definition
	void AddGeneratedColumn(const ColumnDefinition &column, const ColumnList &list);

	//! Removes the column(s) and outputs the new column indices
	vector<LogicalIndex> RemoveColumn(LogicalIndex index, idx_t column_amount);

	bool IsDependencyOf(LogicalIndex dependent, LogicalIndex dependency) const;
	bool HasDependencies(LogicalIndex index) const;
	const logical_index_set_t &GetDependencies(LogicalIndex index) const;

	bool HasDependents(LogicalIndex index) const;
	const logical_index_set_t &GetDependents(LogicalIndex index) const;

private:
	void RemoveStandardColumn(LogicalIndex index);
	void RemoveGeneratedColumn(LogicalIndex index);

	void AdjustSingle(LogicalIndex idx, idx_t offset);
	// Clean up the gaps created by a Remove operation
	vector<LogicalIndex> CleanupInternals(idx_t column_amount);

private:
	//! A map of column dependency to generated column(s)
	logical_index_map_t<logical_index_set_t> dependencies_map;
	//! A map of generated column name to (potentially generated)column dependencies
	logical_index_map_t<logical_index_set_t> dependents_map;
	//! For resolve-order purposes, keep track of the 'direct' (not inherited) dependencies of a generated column
	logical_index_map_t<logical_index_set_t> direct_dependencies;
	logical_index_set_t deleted_columns;
};

} // namespace sabot_sql
