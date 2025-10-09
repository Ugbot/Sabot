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

#pragma once

#include <memory>
#include <string>
#include <arrow/flight/server.h>
#include <marble/status.h>
#include <marble/table.h>

namespace marble {

/**
 * @brief Flight service for MarbleDB ingestion and querying
 *
 * Provides Arrow Flight endpoints for:
 * - DoPut: Ingest Arrow RecordBatches into tables
 * - DoGet: Query tables and return Arrow data
 * - DoAction: Administrative operations (create table, etc.)
 */
class MarbleFlightService : public arrow::flight::FlightServerBase {
public:
    explicit MarbleFlightService(std::shared_ptr<Database> db);
    ~MarbleFlightService() override = default;

    // Flight server implementation
    arrow::Status DoPut(const arrow::flight::ServerCallContext& context,
                       std::unique_ptr<arrow::flight::FlightMessageReader> reader,
                       std::unique_ptr<arrow::flight::FlightMetadataWriter> writer) override;

    arrow::Status DoGet(const arrow::flight::ServerCallContext& context,
                       const arrow::flight::Ticket& request,
                       std::unique_ptr<arrow::flight::FlightDataStream>*) override;

    arrow::Status DoAction(const arrow::flight::ServerCallContext& context,
                          const arrow::flight::Action& action,
                          std::unique_ptr<arrow::flight::ResultStream>* result) override;

    arrow::Status ListActions(const arrow::flight::ServerCallContext& context,
                             std::unique_ptr<arrow::flight::ActionTypeStream>* actions) override;

private:
    // Command structures for Flight actions
    struct CreateTableCommand {
        std::string table_name;
        std::shared_ptr<arrow::Schema> schema;
        TimePartition time_partition = TimePartition::kDaily;
        std::string time_column = "timestamp";

        static std::unique_ptr<CreateTableCommand> FromJson(const std::string& json);
        std::string ToJson() const;
    };

    struct ScanCommand {
        std::string table_name;
        std::vector<std::string> columns;
        std::unordered_map<std::string, std::string> filters;
        std::string time_filter;
        int64_t limit = -1;

        static std::unique_ptr<ScanCommand> FromJson(const std::string& json);
        std::string ToJson() const;
    };

    // Helper methods
    arrow::Status HandleCreateTable(const CreateTableCommand& cmd);
    arrow::Status HandleScan(const ScanCommand& cmd,
                            std::unique_ptr<arrow::flight::FlightDataStream>* stream);

    // Convert Status to arrow::Status
    static arrow::Status ToArrowStatus(const Status& status);

    std::shared_ptr<Database> db_;
};

/**
 * @brief Flight server wrapper for MarbleDB
 */
class MarbleFlightServer {
public:
    MarbleFlightServer(std::shared_ptr<Database> db, const std::string& host, int port);
    ~MarbleFlightServer();

    // Server lifecycle
    Status Start();
    Status Stop();
    Status Wait();

    // Server info
    std::string GetHost() const { return host_; }
    int GetPort() const { return port_; }
    bool IsRunning() const { return running_; }

private:
    std::shared_ptr<Database> db_;
    std::string host_;
    int port_;
    std::unique_ptr<arrow::flight::FlightServerBase> service_;
    std::unique_ptr<arrow::flight::FlightServer> server_;
    bool running_;
};

/**
 * @brief Flight client for interacting with MarbleDB
 */
class MarbleFlightClient {
public:
    MarbleFlightClient(const std::string& host, int port);
    ~MarbleFlightClient();

    // Client lifecycle
    Status Connect();
    Status Disconnect();

    // Data operations
    Status PutRecordBatch(const std::string& table_name,
                         const arrow::RecordBatch& batch);

    Status ScanTable(const std::string& table_name,
                    const ScanSpec& spec,
                    std::shared_ptr<arrow::Table>* result);

    // Administrative operations
    Status CreateTable(const TableSchema& schema);

private:
    std::string host_;
    int port_;
    std::unique_ptr<arrow::flight::FlightClient> client_;
    bool connected_;
};

// Factory functions
std::unique_ptr<MarbleFlightServer> CreateFlightServer(std::shared_ptr<Database> db,
                                                      const std::string& host = "localhost",
                                                      int port = 50051);

std::unique_ptr<MarbleFlightClient> CreateFlightClient(const std::string& host = "localhost",
                                                      int port = 50051);

} // namespace marble

