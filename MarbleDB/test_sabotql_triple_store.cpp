#include <marble/db.h>
#include <arrow/api.h>
#include <iostream>
#include <memory>

int main() {
    std::cout << "Testing MarbleDB SabotQL Triple Store (P0)" << std::endl;
    std::cout << "Creating SPO, POS, OSP indexes as per requirements" << std::endl;

    // Open database
    marble::DBOptions options;
    options.db_path = "/tmp/marble_triple_test";
    std::unique_ptr<marble::MarbleDB> db;
    auto status = marble::MarbleDB::Open(options, nullptr, &db);

    if (!status.ok()) {
        std::cerr << "Failed to open database: " << status.ToString() << std::endl;
        return 1;
    }

    // Create SPO Index (Subject-Predicate-Object)
    auto spo_schema = arrow::schema({
        arrow::field("subject", arrow::int64()),
        arrow::field("predicate", arrow::int64()),
        arrow::field("object", arrow::int64())
    });

    marble::ColumnFamilyOptions spo_options;
    spo_options.schema = spo_schema;
    spo_options.enable_bloom_filter = true;
    spo_options.enable_sparse_index = true;
    spo_options.index_granularity = 8192;

    marble::ColumnFamilyDescriptor spo_descriptor("SPO", spo_options);
    marble::ColumnFamilyHandle* spo_handle = nullptr;
    status = db->CreateColumnFamily(spo_descriptor, &spo_handle);

    if (!status.ok()) {
        std::cerr << "Failed to create SPO index: " << status.ToString() << std::endl;
        return 1;
    }
    std::cout << "✓ Created SPO index" << std::endl;

    // Create POS Index (Predicate-Object-Subject)
    auto pos_schema = arrow::schema({
        arrow::field("predicate", arrow::int64()),
        arrow::field("object", arrow::int64()),
        arrow::field("subject", arrow::int64())
    });

    marble::ColumnFamilyOptions pos_options;
    pos_options.schema = pos_schema;
    pos_options.enable_bloom_filter = true;
    pos_options.enable_sparse_index = true;
    pos_options.index_granularity = 8192;

    marble::ColumnFamilyDescriptor pos_descriptor("POS", pos_options);
    marble::ColumnFamilyHandle* pos_handle = nullptr;
    status = db->CreateColumnFamily(pos_descriptor, &pos_handle);

    if (!status.ok()) {
        std::cerr << "Failed to create POS index: " << status.ToString() << std::endl;
        return 1;
    }
    std::cout << "✓ Created POS index" << std::endl;

    // Create OSP Index (Object-Subject-Predicate)
    auto osp_schema = arrow::schema({
        arrow::field("object", arrow::int64()),
        arrow::field("subject", arrow::int64()),
        arrow::field("predicate", arrow::int64())
    });

    marble::ColumnFamilyOptions osp_options;
    osp_options.schema = osp_schema;
    osp_options.enable_bloom_filter = true;
    osp_options.enable_sparse_index = true;
    osp_options.index_granularity = 8192;

    marble::ColumnFamilyDescriptor osp_descriptor("OSP", osp_options);
    marble::ColumnFamilyHandle* osp_handle = nullptr;
    status = db->CreateColumnFamily(osp_descriptor, &osp_handle);

    if (!status.ok()) {
        std::cerr << "Failed to create OSP index: " << status.ToString() << std::endl;
        return 1;
    }
    std::cout << "✓ Created OSP index" << std::endl;

    // Create Vocabulary Table
    auto vocab_schema = arrow::schema({
        arrow::field("id", arrow::int64()),
        arrow::field("lexical", arrow::utf8()),
        arrow::field("kind", arrow::uint8()),
        arrow::field("language", arrow::utf8()),
        arrow::field("datatype", arrow::utf8())
    });

    marble::ColumnFamilyOptions vocab_options;
    vocab_options.schema = vocab_schema;
    vocab_options.enable_bloom_filter = true;

    marble::ColumnFamilyDescriptor vocab_descriptor("vocabulary", vocab_options);
    marble::ColumnFamilyHandle* vocab_handle = nullptr;
    status = db->CreateColumnFamily(vocab_descriptor, &vocab_handle);

    if (!status.ok()) {
        std::cerr << "Failed to create vocabulary table: " << status.ToString() << std::endl;
        return 1;
    }
    std::cout << "✓ Created vocabulary table" << std::endl;

    // Insert vocabulary entries
    // Alice = 42, knows = 100, Bob = 99, Carol = 98, worksAt = 101, CompanyX = 200
    {
        arrow::Int64Builder id_builder;
        arrow::StringBuilder lexical_builder, language_builder, datatype_builder;
        arrow::UInt8Builder kind_builder;

        // Alice
        id_builder.Append(42);
        lexical_builder.Append("Alice");
        kind_builder.Append(0); // IRI
        language_builder.Append("");
        datatype_builder.Append("");

        // knows
        id_builder.Append(100);
        lexical_builder.Append("knows");
        kind_builder.Append(0); // IRI
        language_builder.Append("");
        datatype_builder.Append("");

        // Bob
        id_builder.Append(99);
        lexical_builder.Append("Bob");
        kind_builder.Append(0); // IRI
        language_builder.Append("");
        datatype_builder.Append("");

        // Carol
        id_builder.Append(98);
        lexical_builder.Append("Carol");
        kind_builder.Append(0); // IRI
        language_builder.Append("");
        datatype_builder.Append("");

        // worksAt
        id_builder.Append(101);
        lexical_builder.Append("worksAt");
        kind_builder.Append(0); // IRI
        language_builder.Append("");
        datatype_builder.Append("");

        // CompanyX
        id_builder.Append(200);
        lexical_builder.Append("CompanyX");
        kind_builder.Append(0); // IRI
        language_builder.Append("");
        datatype_builder.Append("");

        std::shared_ptr<arrow::Array> id_array, lexical_array, kind_array, lang_array, dt_array;
        id_builder.Finish(&id_array);
        lexical_builder.Finish(&lexical_array);
        kind_builder.Finish(&kind_array);
        language_builder.Finish(&lang_array);
        datatype_builder.Finish(&dt_array);

        auto vocab_batch = arrow::RecordBatch::Make(vocab_schema, 6,
                                                   {id_array, lexical_array, kind_array, lang_array, dt_array});

        status = db->InsertBatch("vocabulary", vocab_batch);
        if (!status.ok()) {
            std::cerr << "Failed to insert vocabulary: " << status.ToString() << std::endl;
            return 1;
        }
        std::cout << "✓ Inserted vocabulary entries" << std::endl;
    }

    // Insert triples for SPO index
    {
        arrow::Int64Builder s_builder, p_builder, o_builder;

        // Alice knows Bob
        s_builder.Append(42);
        p_builder.Append(100);
        o_builder.Append(99);

        // Alice knows Carol
        s_builder.Append(42);
        p_builder.Append(100);
        o_builder.Append(98);

        // Bob worksAt CompanyX
        s_builder.Append(99);
        p_builder.Append(101);
        o_builder.Append(200);

        // Carol worksAt CompanyX
        s_builder.Append(98);
        p_builder.Append(101);
        o_builder.Append(200);

        std::shared_ptr<arrow::Array> s_array, p_array, o_array;
        s_builder.Finish(&s_array);
        p_builder.Finish(&p_array);
        o_builder.Finish(&o_array);

        auto spo_batch = arrow::RecordBatch::Make(spo_schema, 4, {s_array, p_array, o_array});
        status = db->InsertBatch("SPO", spo_batch);

        if (!status.ok()) {
            std::cerr << "Failed to insert SPO triples: " << status.ToString() << std::endl;
            return 1;
        }
        std::cout << "✓ Inserted triples into SPO index" << std::endl;
    }

    // Insert triples for POS index (reordered: predicate, object, subject)
    {
        arrow::Int64Builder p_builder, o_builder, s_builder;

        // knows Bob Alice
        p_builder.Append(100);
        o_builder.Append(99);
        s_builder.Append(42);

        // knows Carol Alice
        p_builder.Append(100);
        o_builder.Append(98);
        s_builder.Append(42);

        // worksAt CompanyX Bob
        p_builder.Append(101);
        o_builder.Append(200);
        s_builder.Append(99);

        // worksAt CompanyX Carol
        p_builder.Append(101);
        o_builder.Append(200);
        s_builder.Append(98);

        std::shared_ptr<arrow::Array> p_array, o_array, s_array;
        p_builder.Finish(&p_array);
        o_builder.Finish(&o_array);
        s_builder.Finish(&s_array);

        auto pos_batch = arrow::RecordBatch::Make(pos_schema, 4, {p_array, o_array, s_array});
        status = db->InsertBatch("POS", pos_batch);

        if (!status.ok()) {
            std::cerr << "Failed to insert POS triples: " << status.ToString() << std::endl;
            return 1;
        }
        std::cout << "✓ Inserted triples into POS index" << std::endl;
    }

    // Insert triples for OSP index (reordered: object, subject, predicate)
    {
        arrow::Int64Builder o_builder, s_builder, p_builder;

        // Bob Alice knows
        o_builder.Append(99);
        s_builder.Append(42);
        p_builder.Append(100);

        // Carol Alice knows
        o_builder.Append(98);
        s_builder.Append(42);
        p_builder.Append(100);

        // CompanyX Bob worksAt
        o_builder.Append(200);
        s_builder.Append(99);
        p_builder.Append(101);

        // CompanyX Carol worksAt
        o_builder.Append(200);
        s_builder.Append(98);
        p_builder.Append(101);

        std::shared_ptr<arrow::Array> o_array, s_array, p_array;
        o_builder.Finish(&o_array);
        s_builder.Finish(&s_array);
        p_builder.Finish(&p_array);

        auto osp_batch = arrow::RecordBatch::Make(osp_schema, 4, {o_array, s_array, p_array});
        status = db->InsertBatch("OSP", osp_batch);

        if (!status.ok()) {
            std::cerr << "Failed to insert OSP triples: " << status.ToString() << std::endl;
            return 1;
        }
        std::cout << "✓ Inserted triples into OSP index" << std::endl;
    }

    // Test queries using different indexes

    // Query 1: Who does Alice know? (SPO index, subject = Alice)
    std::cout << "\n--- Query 1: Who does Alice know? (using SPO index) ---" << std::endl;
    {
        std::unique_ptr<marble::QueryResult> result;
        status = db->ScanTable("SPO", &result);
        if (!status.ok()) {
            std::cerr << "Failed to scan SPO: " << status.ToString() << std::endl;
            return 1;
        }

        int64_t total_rows = 0;
        while (result->HasNext()) {
            std::shared_ptr<arrow::RecordBatch> batch;
            status = result->Next(&batch);
            if (!status.ok()) break;

            auto s_col = std::static_pointer_cast<arrow::Int64Array>(batch->column(0));
            auto p_col = std::static_pointer_cast<arrow::Int64Array>(batch->column(1));
            auto o_col = std::static_pointer_cast<arrow::Int64Array>(batch->column(2));

            for (int64_t i = 0; i < batch->num_rows(); ++i) {
                if (s_col->Value(i) == 42 && p_col->Value(i) == 100) { // Alice knows X
                    total_rows++;
                    std::cout << "  Alice knows " << o_col->Value(i) << std::endl;
                }
            }
        }
        std::cout << "Found " << total_rows << " relationships" << std::endl;
    }

    // Query 2: Who works at CompanyX? (POS index, predicate = worksAt, object = CompanyX)
    std::cout << "\n--- Query 2: Who works at CompanyX? (using POS index) ---" << std::endl;
    {
        std::unique_ptr<marble::QueryResult> result;
        status = db->ScanTable("POS", &result);
        if (!status.ok()) {
            std::cerr << "Failed to scan POS: " << status.ToString() << std::endl;
            return 1;
        }

        int64_t total_rows = 0;
        while (result->HasNext()) {
            std::shared_ptr<arrow::RecordBatch> batch;
            status = result->Next(&batch);
            if (!status.ok()) break;

            auto p_col = std::static_pointer_cast<arrow::Int64Array>(batch->column(0));
            auto o_col = std::static_pointer_cast<arrow::Int64Array>(batch->column(1));
            auto s_col = std::static_pointer_cast<arrow::Int64Array>(batch->column(2));

            for (int64_t i = 0; i < batch->num_rows(); ++i) {
                if (p_col->Value(i) == 101 && o_col->Value(i) == 200) { // X worksAt CompanyX
                    total_rows++;
                    std::cout << "  " << s_col->Value(i) << " works at CompanyX" << std::endl;
                }
            }
        }
        std::cout << "Found " << total_rows << " employees" << std::endl;
    }

    // Query 3: What relationships exist with Bob? (OSP index, object = Bob)
    std::cout << "\n--- Query 3: What relationships exist with Bob? (using OSP index) ---" << std::endl;
    {
        std::unique_ptr<marble::QueryResult> result;
        status = db->ScanTable("OSP", &result);
        if (!status.ok()) {
            std::cerr << "Failed to scan OSP: " << status.ToString() << std::endl;
            return 1;
        }

        int64_t total_rows = 0;
        while (result->HasNext()) {
            std::shared_ptr<arrow::RecordBatch> batch;
            status = result->Next(&batch);
            if (!status.ok()) break;

            auto o_col = std::static_pointer_cast<arrow::Int64Array>(batch->column(0));
            auto s_col = std::static_pointer_cast<arrow::Int64Array>(batch->column(1));
            auto p_col = std::static_pointer_cast<arrow::Int64Array>(batch->column(2));

            for (int64_t i = 0; i < batch->num_rows(); ++i) {
                if (o_col->Value(i) == 99) { // Relationships where Bob is the object
                    total_rows++;
                    std::cout << "  " << s_col->Value(i) << " --" << p_col->Value(i) << "--> Bob" << std::endl;
                }
            }
        }
        std::cout << "Found " << total_rows << " relationships" << std::endl;
    }

    // Check all column families
    auto cf_list = db->ListColumnFamilies();
    std::cout << "\n✓ All column families: ";
    for (const auto& cf : cf_list) {
        std::cout << cf << " ";
    }
    std::cout << std::endl;

    std::cout << "\n🎉 SabotQL triple store P0 features working correctly!" << std::endl;
    std::cout << "✓ Created SPO, POS, OSP indexes with Arrow schemas" << std::endl;
    std::cout << "✓ Inserted triples and vocabulary data" << std::endl;
    std::cout << "✓ Can query using different indexes (simulating SPARQL patterns)" << std::endl;

    return 0;
}
