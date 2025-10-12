//===----------------------------------------------------------------------===//
//                         SabotSQL
//
// sabot_sql/catalog/catalog_entry/table_catalog_entry.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sabot_sql/catalog/standard_entry.hpp"
#include "sabot_sql/common/unordered_map.hpp"
#include "sabot_sql/parser/column_list.hpp"
#include "sabot_sql/parser/constraint.hpp"
#include "sabot_sql/planner/bound_constraint.hpp"
#include "sabot_sql/storage/table/table_statistics.hpp"
#include "sabot_sql/planner/expression.hpp"
#include "sabot_sql/common/case_insensitive_map.hpp"
#include "sabot_sql/catalog/catalog_entry/table_column_type.hpp"
#include "sabot_sql/catalog/catalog_entry/column_dependency_manager.hpp"
#include "sabot_sql/common/table_column.hpp"

namespace sabot_sql {

class DataTable;

struct RenameColumnInfo;
struct RenameFieldInfo;
struct AddColumnInfo;
struct AddFieldInfo;
struct RemoveColumnInfo;
struct RemoveFieldInfo;
struct SetDefaultInfo;
struct ChangeColumnTypeInfo;
struct AlterForeignKeyInfo;
struct SetNotNullInfo;
struct DropNotNullInfo;
struct SetColumnCommentInfo;
struct CreateTableInfo;
struct BoundCreateTableInfo;

class TableFunction;
struct FunctionData;
struct EntryLookupInfo;

class Binder;
struct ColumnSegmentInfo;
class TableStorageInfo;

class LogicalGet;
class LogicalProjection;
class LogicalUpdate;

//! A table catalog entry
class TableCatalogEntry : public StandardEntry {
public:
	static constexpr const CatalogType Type = CatalogType::TABLE_ENTRY;
	static constexpr const char *Name = "table";

public:
	//! Create a TableCatalogEntry and initialize storage for it
	SABOT_SQL_API TableCatalogEntry(Catalog &catalog, SchemaCatalogEntry &schema, CreateTableInfo &info);

public:
	SABOT_SQL_API unique_ptr<CreateInfo> GetInfo() const override;

	SABOT_SQL_API bool HasGeneratedColumns() const;

	//! Returns whether or not a column with the given name exists
	SABOT_SQL_API bool ColumnExists(const string &name) const;
	//! Returns a reference to the column of the specified name. Throws an
	//! exception if the column does not exist.
	SABOT_SQL_API const ColumnDefinition &GetColumn(const string &name) const;
	//! Returns a reference to the column of the specified logical index. Throws an
	//! exception if the column does not exist.
	SABOT_SQL_API const ColumnDefinition &GetColumn(LogicalIndex idx) const;
	//! Returns a list of types of the table, excluding generated columns
	SABOT_SQL_API vector<LogicalType> GetTypes() const;
	//! Returns a list of the columns of the table
	SABOT_SQL_API const ColumnList &GetColumns() const;
	//! Returns the underlying storage of the table
	virtual DataTable &GetStorage();

	//! Returns a list of the constraints of the table
	SABOT_SQL_API const vector<unique_ptr<Constraint>> &GetConstraints() const;
	SABOT_SQL_API string ToSQL() const override;

	//! Get statistics of a column (physical or virtual) within the table
	virtual unique_ptr<BaseStatistics> GetStatistics(ClientContext &context, column_t column_id) = 0;

	virtual unique_ptr<BlockingSample> GetSample();

	//! Returns the column index of the specified column name.
	//! If the column does not exist:
	//! If if_column_exists is true, returns DConstants::INVALID_INDEX
	//! If if_column_exists is false, throws an exception
	SABOT_SQL_API LogicalIndex GetColumnIndex(string &name, bool if_exists = false) const;

	//! Returns the scan function that can be used to scan the given table
	virtual TableFunction GetScanFunction(ClientContext &context, unique_ptr<FunctionData> &bind_data) = 0;
	virtual TableFunction GetScanFunction(ClientContext &context, unique_ptr<FunctionData> &bind_data,
	                                      const EntryLookupInfo &lookup_info);

	virtual bool IsDuckTable() const {
		return false;
	}

	SABOT_SQL_API static string ColumnsToSQL(const ColumnList &columns, const vector<unique_ptr<Constraint>> &constraints);

	//! Returns the expression string list of the column names e.g. (col1, col2, col3)
	static string ColumnNamesToSQL(const ColumnList &columns);

	//! Returns a list of segment information for this table, if exists
	virtual vector<ColumnSegmentInfo> GetColumnSegmentInfo(const QueryContext &context);

	//! Returns the storage info of this table
	virtual TableStorageInfo GetStorageInfo(ClientContext &context) = 0;

	virtual void BindUpdateConstraints(Binder &binder, LogicalGet &get, LogicalProjection &proj, LogicalUpdate &update,
	                                   ClientContext &context);

	//! Returns a pointer to the table's primary key, if exists, else nullptr.
	optional_ptr<Constraint> GetPrimaryKey() const;
	//! Returns true, if the table has a primary key, else false.
	bool HasPrimaryKey() const;

	//! Returns the virtual columns for this table
	virtual virtual_column_map_t GetVirtualColumns() const;

	virtual vector<column_t> GetRowIdColumns() const;

protected:
	//! A list of columns that are part of this table
	ColumnList columns;
	//! A list of constraints that are part of this table
	vector<unique_ptr<Constraint>> constraints;
};
} // namespace sabot_sql
