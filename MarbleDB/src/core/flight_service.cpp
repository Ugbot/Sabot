/************************************************************************
Copyright 2024 MarbleDB Contributors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    https://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
**************************************************************************/

#include "marble/flight_service.h"
#include <arrow/flight/server.h>
#include <arrow/flight/client.h>
#include <arrow/util/logging.h>
#include <nlohmann/json.hpp>
#include <sstream>
#include <iostream>

using json = nlohmann::json;

namespace marble {

// Flight service implementation

MarbleFlightService::MarbleFlightService(std::shared_ptr<Database> db)
    : db_(std::move(db)) {}

arrow::Status MarbleFlightService::DoPut(
    const arrow::flight::ServerCallContext& context,
    std::unique_ptr<arrow::flight::FlightMessageReader> reader,
    std::unique_ptr<arrow::flight::FlightMetadataWriter> writer) {

    // Extract table name from descriptor
    auto descriptor = reader->descriptor();
    if (!descriptor) {
        return arrow::Status::Invalid("Missing flight descriptor");
    }

    std::string table_name = descriptor->path[0];  // Assume path[0] is table name

    // Get or create table
    std::shared_ptr<Table> table;
    auto status = db_->GetTable(table_name, &table);
    if (!status.ok()) {
        return ToArrowStatus(status);
    }

    // Read and ingest record batches
    while (true) {
        arrow::flight::FlightStreamChunk chunk;
        ARROW_ASSIGN_OR_RAISE(chunk, reader->Next());

        if (!chunk.data) {
            break;  // End of stream
        }

        // Convert FlightData to RecordBatch
        ARROW_ASSIGN_OR_RAISE(auto batch, chunk.data->ToRecordBatch());

        // Append to table
        status = table->Append(*batch);
        if (!status.ok()) {
            return ToArrowStatus(status);
        }
    }

    return arrow::Status::OK();
}

arrow::Status MarbleFlightService::DoGet(
    const arrow::flight::ServerCallContext& context,
    const arrow::flight::Ticket& request,
    std::unique_ptr<arrow::flight::FlightDataStream>* stream) {

    // Parse ticket as JSON command
    std::string ticket_data(reinterpret_cast<const char*>(request.ticket.data()),
                           request.ticket.size());

    try {
        auto cmd = ScanCommand::FromJson(ticket_data);
        return HandleScan(*cmd, stream);
    } catch (const std::exception& e) {
        return arrow::Status::Invalid("Invalid scan command: ", e.what());
    }
}

arrow::Status MarbleFlightService::DoAction(
    const arrow::flight::ServerCallContext& context,
    const arrow::flight::Action& action,
    std::unique_ptr<arrow::flight::ResultStream>* result) {

    if (action.type == "create_table") {
        try {
            auto cmd = CreateTableCommand::FromJson(action.body->ToString());
            auto status = HandleCreateTable(*cmd);
            if (!status.ok()) {
                return status;
            }

            // Return empty result
            *result = std::unique_ptr<arrow::flight::ResultStream>(
                new arrow::flight::SimpleResultStream({}));
            return arrow::Status::OK();

        } catch (const std::exception& e) {
            return arrow::Status::Invalid("Invalid create_table command: ", e.what());
        }
    }

    return arrow::Status::NotImplemented("Unknown action: ", action.type);
}

arrow::Status MarbleFlightService::ListActions(
    const arrow::flight::ServerCallContext& context,
    std::unique_ptr<arrow::flight::ActionTypeStream>* actions) {

    std::vector<arrow::flight::ActionType> action_types = {
        {"create_table", "Create a new table"},
        {"scan", "Scan/query a table"}
    };

    *actions = std::unique_ptr<arrow::flight::ActionTypeStream>(
        new arrow::flight::SimpleActionTypeStream(action_types));
    return arrow::Status::OK();
}

// Command implementations

std::unique_ptr<MarbleFlightService::CreateTableCommand>
MarbleFlightService::CreateTableCommand::FromJson(const std::string& json_str) {
    auto cmd = std::make_unique<CreateTableCommand>();
    auto j = json::parse(json_str);

    cmd->table_name = j["table_name"];
    cmd->time_column = j.value("time_column", "timestamp");

    std::string partition_str = j.value("time_partition", "daily");
    if (partition_str == "hourly") {
        cmd->time_partition = TimePartition::kHourly;
    } else if (partition_str == "monthly") {
        cmd->time_partition = TimePartition::kMonthly;
    } else {
        cmd->time_partition = TimePartition::kDaily;
    }

    // Parse Arrow schema from JSON
    // This is a simplified implementation - in practice you'd need
    // proper Arrow schema serialization
    std::vector<std::shared_ptr<arrow::Field>> fields;
    for (const auto& field_json : j["schema"]["fields"]) {
        std::string name = field_json["name"];
        std::string type_str = field_json["type"];

        std::shared_ptr<arrow::DataType> type;
        if (type_str == "int64") {
            type = arrow::int64();
        } else if (type_str == "float64") {
            type = arrow::float64();
        } else if (type_str == "utf8") {
            type = arrow::utf8();
        } else if (type_str == "timestamp[us]") {
            type = arrow::timestamp(arrow::TimeUnit::MICRO);
        } else {
            type = arrow::utf8();  // Default to string
        }

        fields.push_back(arrow::field(name, type));
    }

    cmd->schema = arrow::schema(fields);
    return cmd;
}

std::string MarbleFlightService::CreateTableCommand::ToJson() const {
    json j = {
        {"table_name", table_name},
        {"time_column", time_column},
        {"time_partition", time_partition == TimePartition::kHourly ? "hourly" :
                          time_partition == TimePartition::kMonthly ? "monthly" : "daily"}
    };
    return j.dump();
}

std::unique_ptr<MarbleFlightService::ScanCommand>
MarbleFlightService::ScanCommand::FromJson(const std::string& json_str) {
    auto cmd = std::make_unique<ScanCommand>();
    auto j = json::parse(json_str);

    cmd->table_name = j["table_name"];
    cmd->limit = j.value("limit", -1);

    if (j.contains("columns")) {
        for (const auto& col : j["columns"]) {
            cmd->columns.push_back(col);
        }
    }

    if (j.contains("filters")) {
        for (const auto& [key, value] : j["filters"].items()) {
            cmd->filters[key] = value;
        }
    }

    if (j.contains("time_filter")) {
        cmd->time_filter = j["time_filter"];
    }

    return cmd;
}

std::string MarbleFlightService::ScanCommand::ToJson() const {
    json j = {
        {"table_name", table_name},
        {"limit", limit}
    };

    if (!columns.empty()) {
        j["columns"] = columns;
    }

    if (!filters.empty()) {
        j["filters"] = filters;
    }

    if (!time_filter.empty()) {
        j["time_filter"] = time_filter;
    }

    return j.dump();
}

arrow::Status MarbleFlightService::HandleCreateTable(const CreateTableCommand& cmd) {
    TableSchema schema(cmd.table_name, cmd.schema);
    schema.time_partition = cmd.time_partition;
    schema.time_column = cmd.time_column;

    auto status = db_->CreateTable(schema);
    return ToArrowStatus(status);
}

arrow::Status MarbleFlightService::HandleScan(
    const ScanCommand& cmd,
    std::unique_ptr<arrow::flight::FlightDataStream>* stream) {

    // Get table
    std::shared_ptr<Table> table;
    auto status = db_->GetTable(cmd.table_name, &table);
    if (!status.ok()) {
        return ToArrowStatus(status);
    }

    // Build scan spec
    ScanSpec spec;
    spec.columns = cmd.columns;
    spec.filters = cmd.filters;
    spec.time_filter = cmd.time_filter;
    spec.limit = cmd.limit;

    // Execute scan
    QueryResult result;
    status = table->Scan(spec, &result);
    if (!status.ok()) {
        return ToArrowStatus(status);
    }

    // Create flight data stream from result
    if (result.table) {
        *stream = std::unique_ptr<arrow::flight::FlightDataStream>(
            new arrow::flight::RecordBatchStream(result.table));
    } else {
        // Empty result
        auto empty_table = arrow::Table::MakeEmpty(table->GetSchema().arrow_schema).ValueOrDie();
        *stream = std::unique_ptr<arrow::flight::FlightDataStream>(
            new arrow::flight::RecordBatchStream(empty_table));
    }

    return arrow::Status::OK();
}

arrow::Status MarbleFlightService::ToArrowStatus(const Status& status) {
    if (status.ok()) {
        return arrow::Status::OK();
    }

    switch (status.code()) {
        case StatusCode::kNotFound:
            return arrow::Status::NotFound(status.message());
        case StatusCode::kInvalidArgument:
            return arrow::Status::Invalid(status.message());
        case StatusCode::kIoError:
            return arrow::Status::IOError(status.message());
        case StatusCode::kAlreadyExists:
            return arrow::Status::AlreadyExists(status.message());
        default:
            return arrow::Status::UnknownError(status.message());
    }
}

// Flight server implementation

MarbleFlightServer::MarbleFlightServer(std::shared_ptr<Database> db,
                                     const std::string& host, int port)
    : db_(std::move(db)), host_(host), port_(port), running_(false) {
    service_ = std::make_unique<MarbleFlightService>(db_);
}

MarbleFlightServer::~MarbleFlightServer() {
    if (running_) {
        Stop();
    }
}

Status MarbleFlightServer::Start() {
    if (running_) {
        return Status::OK();
    }

    try {
        arrow::flight::Location location = arrow::flight::Location::ForGrpcTcp(host_, port_);
        arrow::flight::FlightServerOptions options(location);

        server_ = std::make_unique<arrow::flight::FlightServer>(options, service_.get());

        auto status = server_->Init();
        if (!status.ok()) {
            return Status::FromArrowStatus(status);
        }

        status = server_->Serve();
        if (!status.ok()) {
            return Status::FromArrowStatus(status);
        }

        running_ = true;
        return Status::OK();

    } catch (const std::exception& e) {
        return Status::InternalError(std::string("Failed to start Flight server: ") + e.what());
    }
}

Status MarbleFlightServer::Stop() {
    if (!running_) {
        return Status::OK();
    }

    try {
        if (server_) {
            auto status = server_->Shutdown();
            if (!status.ok()) {
                return Status::FromArrowStatus(status);
            }
        }

        running_ = false;
        return Status::OK();

    } catch (const std::exception& e) {
        return Status::InternalError(std::string("Failed to stop Flight server: ") + e.what());
    }
}

Status MarbleFlightServer::Wait() {
    if (server_) {
        server_->Wait();
    }
    return Status::OK();
}

// Flight client implementation

MarbleFlightClient::MarbleFlightClient(const std::string& host, int port)
    : host_(host), port_(port), connected_(false) {}

MarbleFlightClient::~MarbleFlightClient() {
    if (connected_) {
        Disconnect();
    }
}

Status MarbleFlightClient::Connect() {
    if (connected_) {
        return Status::OK();
    }

    try {
        arrow::flight::Location location = arrow::flight::Location::ForGrpcTcp(host_, port_);
        auto client_result = arrow::flight::FlightClient::Connect(location);
        if (!client_result.ok()) {
            return Status::FromArrowStatus(client_result.status());
        }

        client_ = std::move(client_result.ValueUnsafe());
        connected_ = true;
        return Status::OK();

    } catch (const std::exception& e) {
        return Status::InternalError(std::string("Failed to connect to Flight server: ") + e.what());
    }
}

Status MarbleFlightClient::Disconnect() {
    if (!connected_) {
        return Status::OK();
    }

    client_.reset();
    connected_ = false;
    return Status::OK();
}

Status MarbleFlightClient::PutRecordBatch(const std::string& table_name,
                                        const arrow::RecordBatch& batch) {
    if (!connected_) {
        return Status::FailedPrecondition("Client not connected");
    }

    try {
        // Create flight descriptor
        arrow::flight::FlightDescriptor descriptor;
        descriptor.type = arrow::flight::FlightDescriptor::PATH;
        descriptor.path = {table_name};

        // Create writer
        auto writer_result = client_->DoPut(descriptor, batch.schema());
        if (!writer_result.ok()) {
            return Status::FromArrowStatus(writer_result.status());
        }

        auto writer = std::move(writer_result.ValueUnsafe());

        // Write the batch
        auto write_result = writer.writer->WriteRecordBatch(batch);
        if (!write_result.ok()) {
            return Status::FromArrowStatus(write_result);
        }

        // Close writer
        auto close_result = writer.writer->Close();
        if (!close_result.ok()) {
            return Status::FromArrowStatus(close_result);
        }

        return Status::OK();

    } catch (const std::exception& e) {
        return Status::InternalError(std::string("Failed to put record batch: ") + e.what());
    }
}

Status MarbleFlightClient::ScanTable(const std::string& table_name,
                                   const ScanSpec& spec,
                                   std::shared_ptr<arrow::Table>* result) {
    if (!connected_) {
        return Status::FailedPrecondition("Client not connected");
    }

    try {
        // Create scan command
        ScanCommand cmd;
        cmd.table_name = table_name;
        cmd.columns = spec.columns;
        cmd.filters = spec.filters;
        cmd.time_filter = spec.time_filter;
        cmd.limit = spec.limit;

        std::string ticket_data = cmd.ToJson();

        // Create ticket
        arrow::flight::Ticket ticket;
        ticket.ticket = arrow::Buffer::FromString(ticket_data).ValueOrDie();

        // Execute query
        auto stream_result = client_->DoGet(ticket);
        if (!stream_result.ok()) {
            return Status::FromArrowStatus(stream_result.status());
        }

        auto stream = std::move(stream_result.ValueUnsafe());

        // Read all data
        std::vector<std::shared_ptr<arrow::RecordBatch>> batches;
        while (true) {
            arrow::flight::FlightStreamChunk chunk;
            auto next_result = stream->Next();
            if (!next_result.ok()) {
                return Status::FromArrowStatus(next_result.status());
            }

            chunk = std::move(next_result.ValueUnsafe());
            if (!chunk.data) {
                break;
            }

            ARROW_ASSIGN_OR_RAISE(auto batch, chunk.data->ToRecordBatch());
            batches.push_back(batch);
        }

        // Convert to table
        if (batches.empty()) {
            return Status::InternalError("No data returned from scan");
        }

        ARROW_ASSIGN_OR_RAISE(*result, arrow::Table::FromRecordBatches(batches));
        return Status::OK();

    } catch (const std::exception& e) {
        return Status::InternalError(std::string("Failed to scan table: ") + e.what());
    }
}

Status MarbleFlightClient::CreateTable(const TableSchema& schema) {
    if (!connected_) {
        return Status::FailedPrecondition("Client not connected");
    }

    try {
        // Create command
        CreateTableCommand cmd;
        cmd.table_name = schema.table_name;
        cmd.schema = schema.arrow_schema;
        cmd.time_partition = schema.time_partition;
        cmd.time_column = schema.time_column;

        std::string body = cmd.ToJson();

        // Execute action
        arrow::flight::Action action{"create_table", arrow::Buffer::FromString(body).ValueOrDie()};
        auto result_stream = client_->DoAction(action);
        if (!result_stream.ok()) {
            return Status::FromArrowStatus(result_stream.status());
        }

        // Consume results (should be empty for create_table)
        auto stream = std::move(result_stream.ValueUnsafe());
        while (true) {
            auto next_result = stream->Next();
            if (!next_result.ok()) {
                return Status::FromArrowStatus(next_result.status());
            }

            auto result = std::move(next_result.ValueUnsafe());
            if (!result) {
                break;
            }
        }

        return Status::OK();

    } catch (const std::exception& e) {
        return Status::InternalError(std::string("Failed to create table: ") + e.what());
    }
}

// Factory functions

std::unique_ptr<MarbleFlightServer> CreateFlightServer(std::shared_ptr<Database> db,
                                                     const std::string& host,
                                                     int port) {
    return std::make_unique<MarbleFlightServer>(db, host, port);
}

std::unique_ptr<MarbleFlightClient> CreateFlightClient(const std::string& host,
                                                     int port) {
    return std::make_unique<MarbleFlightClient>(host, port);
}

} // namespace marble

