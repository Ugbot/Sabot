//===----------------------------------------------------------------------===//
//                         SabotSQL
//
// sabot_sql/parser/column_definition.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sabot_sql/common/common.hpp"
#include "sabot_sql/common/types/value.hpp"
#include "sabot_sql/parser/parsed_expression.hpp"
#include "sabot_sql/common/enums/compression_type.hpp"
#include "sabot_sql/catalog/catalog_entry/table_column_type.hpp"
#include "sabot_sql/common/case_insensitive_map.hpp"

namespace sabot_sql {

struct RenameColumnInfo;
struct RenameTableInfo;

class ColumnDefinition;

//! A column of a table.
class ColumnDefinition {
public:
	SABOT_SQL_API ColumnDefinition(string name, LogicalType type);
	SABOT_SQL_API ColumnDefinition(string name, LogicalType type, unique_ptr<ParsedExpression> expression,
	                            TableColumnType category);

public:
	//! default_value
	const ParsedExpression &DefaultValue() const;
	bool HasDefaultValue() const;
	void SetDefaultValue(unique_ptr<ParsedExpression> default_value);

	//! type
	SABOT_SQL_API const LogicalType &Type() const;
	LogicalType &TypeMutable();
	void SetType(const LogicalType &type);

	//! name
	SABOT_SQL_API const string &Name() const;
	void SetName(const string &name);

	//! comment
	SABOT_SQL_API const Value &Comment() const;
	void SetComment(const Value &comment);

	//! compression_type
	const sabot_sql::CompressionType &CompressionType() const;
	void SetCompressionType(sabot_sql::CompressionType compression_type);

	//! storage_oid
	const storage_t &StorageOid() const;
	void SetStorageOid(storage_t storage_oid);

	LogicalIndex Logical() const;
	PhysicalIndex Physical() const;

	//! oid
	const column_t &Oid() const;
	void SetOid(column_t oid);

	//! category
	const TableColumnType &Category() const;
	//! Whether this column is a Generated Column
	bool Generated() const;
	SABOT_SQL_API ColumnDefinition Copy() const;

	SABOT_SQL_API void Serialize(Serializer &serializer) const;
	SABOT_SQL_API static ColumnDefinition Deserialize(Deserializer &deserializer);

	//===--------------------------------------------------------------------===//
	// Generated Columns (VIRTUAL)
	//===--------------------------------------------------------------------===//

	ParsedExpression &GeneratedExpressionMutable();
	const ParsedExpression &GeneratedExpression() const;
	void SetGeneratedExpression(unique_ptr<ParsedExpression> expression);
	void ChangeGeneratedExpressionType(const LogicalType &type);
	void GetListOfDependencies(vector<string> &dependencies) const;

	string GetName() const;

	LogicalType GetType() const;

private:
	//! The name of the entry
	string name;
	//! The type of the column
	LogicalType type;
	//! Compression Type used for this column
	sabot_sql::CompressionType compression_type = sabot_sql::CompressionType::COMPRESSION_AUTO;
	//! The index of the column in the storage of the table
	storage_t storage_oid = DConstants::INVALID_INDEX;
	//! The index of the column in the table
	idx_t oid = DConstants::INVALID_INDEX;
	//! The category of the column
	TableColumnType category = TableColumnType::STANDARD;
	//! The default value of the column (for non-generated columns)
	//! The generated column expression (for generated columns)
	unique_ptr<ParsedExpression> expression;
	//! Comment on this column
	Value comment;
	//! Tags on this column
	unordered_map<string, string> tags;
};

} // namespace sabot_sql
