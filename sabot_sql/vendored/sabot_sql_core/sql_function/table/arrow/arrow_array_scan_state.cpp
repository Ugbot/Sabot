#include "sabot_sql/function/table/arrow.hpp"
#include "sabot_sql/common/printer.hpp"
#include "sabot_sql/common/types/arrow_aux_data.hpp"

namespace sabot_sql {

ArrowArrayScanState::ArrowArrayScanState(ClientContext &context) : context(context) {
	arrow_dictionary = nullptr;
}

ArrowArrayScanState &ArrowArrayScanState::GetChild(idx_t child_idx) {
	auto it = children.find(child_idx);
	if (it == children.end()) {
		auto child_p = make_uniq<ArrowArrayScanState>(context);
		auto &child = *child_p;
		child.owned_data = owned_data;
		children.emplace(child_idx, std::move(child_p));
		return child;
	}
	if (!it->second->owned_data) {
		// Propagate down the ownership, for dictionaries in children
		D_ASSERT(owned_data);
		it->second->owned_data = owned_data;
	}
	return *it->second;
}

void ArrowArrayScanState::AddDictionary(unique_ptr<Vector> dictionary_p, ArrowArray *arrow_dict) {
	dictionary = std::move(dictionary_p);
	D_ASSERT(owned_data);
	D_ASSERT(arrow_dict);
	arrow_dictionary = arrow_dict;
	// Make sure the data referenced by the dictionary stays alive
	dictionary->GetBuffer()->SetAuxiliaryData(make_uniq<ArrowAuxiliaryData>(owned_data));
}

bool ArrowArrayScanState::HasDictionary() const {
	return dictionary != nullptr;
}

bool ArrowArrayScanState::CacheOutdated(ArrowArray *dictionary) const {
	if (!dictionary) {
		// Not cached
		return true;
	}
	if (dictionary == arrow_dictionary.get()) {
		// Already cached, not outdated
		return false;
	}
	return true;
}

Vector &ArrowArrayScanState::GetDictionary() {
	D_ASSERT(HasDictionary());
	return *dictionary;
}

} // namespace sabot_sql
