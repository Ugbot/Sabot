#include "sabot_sql/common/types/data_chunk.hpp"
#include "sabot_sql/common/types/string_type.hpp"
#include "sabot_sql/main/capi/capi_internal.hpp"
#include "sabot_sql/common/type_visitor.hpp"

#include <string.h>

sabot_sql_data_chunk sabot_sql_create_data_chunk(sabot_sql_logical_type *column_types, idx_t column_count) {
	if (!column_types) {
		return nullptr;
	}
	sabot_sql::vector<sabot_sql::LogicalType> types;
	for (idx_t i = 0; i < column_count; i++) {
		auto logical_type = reinterpret_cast<sabot_sql::LogicalType *>(column_types[i]);
		if (sabot_sql::TypeVisitor::Contains(*logical_type, sabot_sql::LogicalTypeId::INVALID) ||
		    sabot_sql::TypeVisitor::Contains(*logical_type, sabot_sql::LogicalTypeId::ANY)) {
			return nullptr;
		}
		types.push_back(*logical_type);
	}

	auto result = new sabot_sql::DataChunk();
	try {
		result->Initialize(sabot_sql::Allocator::DefaultAllocator(), types);
	} catch (...) {
		delete result;
		return nullptr;
	}

	return reinterpret_cast<sabot_sql_data_chunk>(result);
}

void sabot_sql_destroy_data_chunk(sabot_sql_data_chunk *chunk) {
	if (chunk && *chunk) {
		auto data_chunk = reinterpret_cast<sabot_sql::DataChunk *>(*chunk);
		delete data_chunk;
		*chunk = nullptr;
	}
}

void sabot_sql_data_chunk_reset(sabot_sql_data_chunk chunk) {
	if (!chunk) {
		return;
	}
	auto dchunk = reinterpret_cast<sabot_sql::DataChunk *>(chunk);
	dchunk->Reset();
}

sabot_sql_vector sabot_sql_create_vector(sabot_sql_logical_type type, idx_t capacity) {
	auto dtype = reinterpret_cast<sabot_sql::LogicalType *>(type);
	try {
		auto vector = new sabot_sql::Vector(*dtype, capacity);
		return reinterpret_cast<sabot_sql_vector>(vector);
	} catch (...) {
		return nullptr;
	}
}

void sabot_sql_destroy_vector(sabot_sql_vector *vector) {
	if (vector && *vector) {
		auto dvector = reinterpret_cast<sabot_sql::Vector *>(*vector);
		delete dvector;
		*vector = nullptr;
	}
}

idx_t sabot_sql_data_chunk_get_column_count(sabot_sql_data_chunk chunk) {
	if (!chunk) {
		return 0;
	}
	auto dchunk = reinterpret_cast<sabot_sql::DataChunk *>(chunk);
	return dchunk->ColumnCount();
}

sabot_sql_vector sabot_sql_data_chunk_get_vector(sabot_sql_data_chunk chunk, idx_t col_idx) {
	if (!chunk || col_idx >= sabot_sql_data_chunk_get_column_count(chunk)) {
		return nullptr;
	}
	auto dchunk = reinterpret_cast<sabot_sql::DataChunk *>(chunk);
	return reinterpret_cast<sabot_sql_vector>(&dchunk->data[col_idx]);
}

idx_t sabot_sql_data_chunk_get_size(sabot_sql_data_chunk chunk) {
	if (!chunk) {
		return 0;
	}
	auto dchunk = reinterpret_cast<sabot_sql::DataChunk *>(chunk);
	return dchunk->size();
}

void sabot_sql_data_chunk_set_size(sabot_sql_data_chunk chunk, idx_t size) {
	if (!chunk) {
		return;
	}
	auto dchunk = reinterpret_cast<sabot_sql::DataChunk *>(chunk);
	dchunk->SetCardinality(size);
}

sabot_sql_logical_type sabot_sql_vector_get_column_type(sabot_sql_vector vector) {
	if (!vector) {
		return nullptr;
	}
	auto v = reinterpret_cast<sabot_sql::Vector *>(vector);
	return reinterpret_cast<sabot_sql_logical_type>(new sabot_sql::LogicalType(v->GetType()));
}

void *sabot_sql_vector_get_data(sabot_sql_vector vector) {
	if (!vector) {
		return nullptr;
	}
	auto v = reinterpret_cast<sabot_sql::Vector *>(vector);
	return sabot_sql::FlatVector::GetData(*v);
}

uint64_t *sabot_sql_vector_get_validity(sabot_sql_vector vector) {
	if (!vector) {
		return nullptr;
	}
	auto v = reinterpret_cast<sabot_sql::Vector *>(vector);
	switch (v->GetVectorType()) {
	case sabot_sql::VectorType::CONSTANT_VECTOR:
		return sabot_sql::ConstantVector::Validity(*v).GetData();
	case sabot_sql::VectorType::FLAT_VECTOR:
		return sabot_sql::FlatVector::Validity(*v).GetData();
	default:
		return nullptr;
	}
}

void sabot_sql_vector_ensure_validity_writable(sabot_sql_vector vector) {
	if (!vector) {
		return;
	}
	auto v = reinterpret_cast<sabot_sql::Vector *>(vector);
	auto &validity = sabot_sql::FlatVector::Validity(*v);
	validity.EnsureWritable();
}

void sabot_sql_vector_assign_string_element(sabot_sql_vector vector, idx_t index, const char *str) {
	sabot_sql_vector_assign_string_element_len(vector, index, str, strlen(str));
}

void sabot_sql_vector_assign_string_element_len(sabot_sql_vector vector, idx_t index, const char *str, idx_t str_len) {
	if (!vector) {
		return;
	}
	auto v = reinterpret_cast<sabot_sql::Vector *>(vector);
	auto data = sabot_sql::FlatVector::GetData<sabot_sql::string_t>(*v);
	data[index] = sabot_sql::StringVector::AddStringOrBlob(*v, str, str_len);
}

sabot_sql_vector sabot_sql_list_vector_get_child(sabot_sql_vector vector) {
	if (!vector) {
		return nullptr;
	}
	auto v = reinterpret_cast<sabot_sql::Vector *>(vector);
	return reinterpret_cast<sabot_sql_vector>(&sabot_sql::ListVector::GetEntry(*v));
}

idx_t sabot_sql_list_vector_get_size(sabot_sql_vector vector) {
	if (!vector) {
		return 0;
	}
	auto v = reinterpret_cast<sabot_sql::Vector *>(vector);
	return sabot_sql::ListVector::GetListSize(*v);
}

sabot_sql_state sabot_sql_list_vector_set_size(sabot_sql_vector vector, idx_t size) {
	if (!vector) {
		return sabot_sql_state::SabotSQLError;
	}
	auto v = reinterpret_cast<sabot_sql::Vector *>(vector);
	sabot_sql::ListVector::SetListSize(*v, size);
	return sabot_sql_state::SabotSQLSuccess;
}

sabot_sql_state sabot_sql_list_vector_reserve(sabot_sql_vector vector, idx_t required_capacity) {
	if (!vector) {
		return sabot_sql_state::SabotSQLError;
	}
	auto v = reinterpret_cast<sabot_sql::Vector *>(vector);
	sabot_sql::ListVector::Reserve(*v, required_capacity);
	return sabot_sql_state::SabotSQLSuccess;
}

sabot_sql_vector sabot_sql_struct_vector_get_child(sabot_sql_vector vector, idx_t index) {
	if (!vector) {
		return nullptr;
	}
	auto v = reinterpret_cast<sabot_sql::Vector *>(vector);
	return reinterpret_cast<sabot_sql_vector>(sabot_sql::StructVector::GetEntries(*v)[index].get());
}

sabot_sql_vector sabot_sql_array_vector_get_child(sabot_sql_vector vector) {
	if (!vector) {
		return nullptr;
	}
	auto v = reinterpret_cast<sabot_sql::Vector *>(vector);
	return reinterpret_cast<sabot_sql_vector>(&sabot_sql::ArrayVector::GetEntry(*v));
}

bool sabot_sql_validity_row_is_valid(uint64_t *validity, idx_t row) {
	if (!validity) {
		return true;
	}
	idx_t entry_idx = row / 64;
	idx_t idx_in_entry = row % 64;
	return validity[entry_idx] & ((idx_t)1 << idx_in_entry);
}

void sabot_sql_validity_set_row_validity(uint64_t *validity, idx_t row, bool valid) {
	if (valid) {
		sabot_sql_validity_set_row_valid(validity, row);
	} else {
		sabot_sql_validity_set_row_invalid(validity, row);
	}
}

void sabot_sql_validity_set_row_invalid(uint64_t *validity, idx_t row) {
	if (!validity) {
		return;
	}
	idx_t entry_idx = row / 64;
	idx_t idx_in_entry = row % 64;
	validity[entry_idx] &= ~((uint64_t)1 << idx_in_entry);
}

void sabot_sql_validity_set_row_valid(uint64_t *validity, idx_t row) {
	if (!validity) {
		return;
	}
	idx_t entry_idx = row / 64;
	idx_t idx_in_entry = row % 64;
	validity[entry_idx] |= (uint64_t)1 << idx_in_entry;
}

sabot_sql_selection_vector sabot_sql_create_selection_vector(idx_t size) {
	return reinterpret_cast<sabot_sql_selection_vector>(new sabot_sql::SelectionVector(size));
}

void sabot_sql_destroy_selection_vector(sabot_sql_selection_vector sel) {
	delete reinterpret_cast<sabot_sql::SelectionVector *>(sel);
}

sel_t *sabot_sql_selection_vector_get_data_ptr(sabot_sql_selection_vector sel) {
	return reinterpret_cast<sabot_sql::SelectionVector *>(sel)->data();
}

void sabot_sql_slice_vector(sabot_sql_vector dict, sabot_sql_selection_vector sel, idx_t len) {
	auto d_dict = reinterpret_cast<sabot_sql::Vector *>(dict);
	auto d_sel = reinterpret_cast<sabot_sql::SelectionVector *>(sel);
	d_dict->Slice(*d_sel, len);
}

void sabot_sql_vector_copy_sel(sabot_sql_vector src, sabot_sql_vector dst, sabot_sql_selection_vector sel, idx_t src_count,
                            idx_t src_offset, idx_t dst_offset) {
	auto d_src = reinterpret_cast<sabot_sql::Vector *>(src);
	auto d_dst = reinterpret_cast<sabot_sql::Vector *>(dst);
	auto d_sel = reinterpret_cast<sabot_sql::SelectionVector *>(sel);
	sabot_sql::VectorOperations::Copy(*d_src, *d_dst, *d_sel, src_count, src_offset, dst_offset);
}

void sabot_sql_vector_reference_value(sabot_sql_vector vector, sabot_sql_value value) {
	auto dvector = reinterpret_cast<sabot_sql::Vector *>(vector);
	auto dvalue = reinterpret_cast<sabot_sql::Value *>(value);
	dvector->Reference(*dvalue);
}

void sabot_sql_vector_reference_vector(sabot_sql_vector to_vector, sabot_sql_vector from_vector) {
	auto dto_vector = reinterpret_cast<sabot_sql::Vector *>(to_vector);
	auto dfrom_vector = reinterpret_cast<sabot_sql::Vector *>(from_vector);
	dto_vector->Reference(*dfrom_vector);
}
