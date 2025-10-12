#include "sabot_sql/common/fsst.hpp"

namespace sabot_sql {

string FSSTPrimitives::DecompressValue(void *sabot_sql_fsst_decoder, const char *compressed_string,
                                       const idx_t compressed_string_len, vector<unsigned char> &decompress_buffer) {
	auto compressed_string_ptr = reinterpret_cast<const unsigned char *>(compressed_string);
	auto fsst_decoder = static_cast<sabot_sql_fsst_decoder_t *>(sabot_sql_fsst_decoder);
	auto decompressed_string_size = sabot_sql_fsst_decompress(fsst_decoder, compressed_string_len, compressed_string_ptr,
	                                                       decompress_buffer.size(), decompress_buffer.data());

	D_ASSERT(!decompress_buffer.empty());
	D_ASSERT(decompressed_string_size <= decompress_buffer.size() - 1);
	return string(char_ptr_cast(decompress_buffer.data()), decompressed_string_size);
}

} // namespace sabot_sql
