//===----------------------------------------------------------------------===//
//                         SabotSQL
//
// mbedtls_wrapper.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "sabot_sql/common/optional_ptr.hpp"
#include "sabot_sql/common/typedefs.hpp"
#include "sabot_sql/common/encryption_state.hpp"

#include <string>

typedef struct mbedtls_cipher_context_t mbedtls_cipher_context_t;
typedef struct mbedtls_cipher_info_t mbedtls_cipher_info_t;

namespace sabot_sql_mbedtls {



class MbedTlsWrapper {
public:
	static void ComputeSha256Hash(const char *in, size_t in_len, char *out);
	static std::string ComputeSha256Hash(const std::string &file_content);
	static bool IsValidSha256Signature(const std::string &pubkey, const std::string &signature,
	                                   const std::string &sha256_hash);
	static void Hmac256(const char *key, size_t key_len, const char *message, size_t message_len, char *out);
	static void ToBase16(char *in, char *out, size_t len);

	static constexpr size_t SHA256_HASH_LENGTH_BYTES = 32;
	static constexpr size_t SHA256_HASH_LENGTH_TEXT = 64;

	class SHA256State {
	public:
		SHA256State();
		~SHA256State();
		void AddString(const std::string &str);
		void AddBytes(sabot_sql::data_ptr_t input_bytes, sabot_sql::idx_t len);
		void AddBytes(sabot_sql::const_data_ptr_t input_bytes, sabot_sql::idx_t len);
		void AddSalt(unsigned char *salt, size_t salt_len);
		std::string Finalize();
		void FinishHex(char *out);
		void FinalizeDerivedKey(sabot_sql::data_ptr_t hash);

	private:
		void *sha_context;
	};

	static constexpr size_t SHA1_HASH_LENGTH_BYTES = 20;
	static constexpr size_t SHA1_HASH_LENGTH_TEXT = 40;

	class SHA1State {
	public:
		SHA1State();
		~SHA1State();
		void AddString(const std::string &str);
		std::string Finalize();
		void FinishHex(char *out);

	private:
		void *sha_context;
	};

class AESStateMBEDTLS : public sabot_sql::EncryptionState {
	public:
		SABOT_SQL_API explicit AESStateMBEDTLS(sabot_sql::EncryptionTypes::CipherType cipher_p, sabot_sql::idx_t key_len);
		SABOT_SQL_API ~AESStateMBEDTLS() override;

	public:
		SABOT_SQL_API void InitializeEncryption(sabot_sql::const_data_ptr_t iv, sabot_sql::idx_t iv_len, sabot_sql::const_data_ptr_t key, sabot_sql::idx_t key_len, sabot_sql::const_data_ptr_t aad, sabot_sql::idx_t aad_len) override;
		SABOT_SQL_API void InitializeDecryption(sabot_sql::const_data_ptr_t iv, sabot_sql::idx_t iv_len, sabot_sql::const_data_ptr_t key, sabot_sql::idx_t key_len, sabot_sql::const_data_ptr_t aad, sabot_sql::idx_t aad_len) override;

		SABOT_SQL_API size_t Process(sabot_sql::const_data_ptr_t in, sabot_sql::idx_t in_len, sabot_sql::data_ptr_t out,
		                          sabot_sql::idx_t out_len) override;
		SABOT_SQL_API size_t Finalize(sabot_sql::data_ptr_t out, sabot_sql::idx_t out_len, sabot_sql::data_ptr_t tag, sabot_sql::idx_t tag_len) override;

		SABOT_SQL_API static void GenerateRandomDataStatic(sabot_sql::data_ptr_t data, sabot_sql::idx_t len);
		SABOT_SQL_API void GenerateRandomData(sabot_sql::data_ptr_t data, sabot_sql::idx_t len) override;
		SABOT_SQL_API void FinalizeGCM(sabot_sql::data_ptr_t tag, sabot_sql::idx_t tag_len);
		SABOT_SQL_API const mbedtls_cipher_info_t *GetCipher(size_t key_len);

	private:
		SABOT_SQL_API void InitializeInternal(sabot_sql::const_data_ptr_t iv, sabot_sql::idx_t iv_len, sabot_sql::const_data_ptr_t aad, sabot_sql::idx_t aad_len);

	private:
		sabot_sql::EncryptionTypes::Mode mode;
		sabot_sql::unique_ptr<mbedtls_cipher_context_t> context;
	};

	class AESStateMBEDTLSFactory : public sabot_sql::EncryptionUtil {

	public:
		sabot_sql::shared_ptr<sabot_sql::EncryptionState> CreateEncryptionState(sabot_sql::EncryptionTypes::CipherType cipher_p, sabot_sql::idx_t key_len = 0) const override {
			return sabot_sql::make_shared_ptr<MbedTlsWrapper::AESStateMBEDTLS>(cipher_p, key_len);
		}

		~AESStateMBEDTLSFactory() override {} //
	};
};

} // namespace sabot_sql_mbedtls
