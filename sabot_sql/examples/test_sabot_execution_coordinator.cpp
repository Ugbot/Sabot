#include <iostream>
#include <arrow/api.h>
#include <arrow/table.h>
#include <arrow/array.h>
#include <arrow/builder.h>
#include "sabot_sql/streaming/sabot_execution_coordinator.h"
#include "sabot_sql/streaming/marbledb_integration.h"
#include "sabot_sql/streaming/checkpoint_coordinator.h"

using namespace sabot_sql::streaming;

int main() {
    std::cout << "=== Testing Sabot Execution Coordinator ===" << std::endl;

    try {
        // 1. Test SabotExecutionCoordinator initialization
        std::cout << "\n1. Testing SabotExecutionCoordinator initialization..." << std::endl;
        
        SabotExecutionCoordinator coordinator;
        
        SabotExecutionCoordinator::ExecutionConfig config;
        config.orchestrator_host = "localhost";
        config.orchestrator_port = 8080;
        config.max_parallelism = 4;
        config.checkpoint_interval = "30s";
        config.state_backend = "marbledb";
        config.timer_backend = "marbledb";
        config.enable_checkpointing = true;
        config.enable_watermarks = true;
        config.watermark_idle_timeout_ms = 30000;
        
        // Initialize embedded MarbleDB integration
        auto marbledb = std::make_shared<sabot_sql::streaming::MarbleDBIntegration>();
        auto marbledb_status = marbledb->Initialize("test_coordinator", "./test_coordinator_marbledb", false);
        if (!marbledb_status.ok()) {
            std::cerr << "❌ Failed to initialize embedded MarbleDB: " << marbledb_status.message() << std::endl;
            return 1;
        }
        
        // Initialize checkpoint coordinator
        auto checkpoint_coordinator = std::make_shared<sabot_sql::streaming::CheckpointCoordinator>();
        auto checkpoint_status = checkpoint_coordinator->Initialize("test_coordinator", 30000, nullptr);
        if (!checkpoint_status.ok()) {
            std::cerr << "❌ Failed to initialize checkpoint coordinator: " << checkpoint_status.message() << std::endl;
            return 1;
        }
        
        auto status = coordinator.Initialize(config);
        if (!status.ok()) {
            std::cerr << "❌ Failed to initialize coordinator: " << status.message() << std::endl;
            return 1;
        }
        std::cout << "✅ Coordinator initialized successfully" << std::endl;

        // 2. Test agent management
        std::cout << "\n2. Testing agent management..." << std::endl;
        
        // Register some mock agents
        SabotExecutionCoordinator::AgentConfig agent1;
        agent1.agent_id = "agent_1";
        agent1.host = "localhost";
        agent1.port = 9001;
        agent1.available_slots = 4;
        agent1.cpu_percent = 50;
        agent1.memory_percent = 60;
        agent1.status = "alive";
        agent1.last_heartbeat = std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::system_clock::now().time_since_epoch()
        ).count();
        
        status = coordinator.RegisterAgent(agent1);
        if (!status.ok()) {
            std::cerr << "❌ Failed to register agent: " << status.message() << std::endl;
            return 1;
        }
        std::cout << "✅ Agent registered successfully" << std::endl;
        
        // Get available agents
        auto available_agents_result = coordinator.GetAvailableAgents();
        if (!available_agents_result.ok()) {
            std::cerr << "❌ Failed to get available agents: " << available_agents_result.status().message() << std::endl;
            return 1;
        }
        
        auto available_agents = available_agents_result.ValueOrDie();
        std::cout << "✅ Found " << available_agents.size() << " available agents" << std::endl;

        // 3. Test streaming job submission
        std::cout << "\n3. Testing streaming job submission..." << std::endl;
        
        // Create a mock streaming plan
        StreamingMorselPlan plan;
        plan.query_id = "test_query_123";
        plan.query_text = "SELECT symbol, COUNT(*) FROM trades GROUP BY symbol";
        plan.is_streaming = true;
        plan.max_parallelism = 2;
        plan.checkpoint_interval = "30s";
        plan.state_backend = "marbledb";
        plan.timer_backend = "marbledb";
        
        // Add a Kafka source
        StreamingMorselPlan::StreamingSource kafka_source;
        kafka_source.source_type = "kafka";
        kafka_source.topic = "trades";
        kafka_source.partition = 0;
        kafka_source.max_parallelism = 2;
        kafka_source.batch_size = 10000;
        kafka_source.watermark_column = "timestamp";
        kafka_source.properties["bootstrap.servers"] = "localhost:9092";
        kafka_source.properties["group.id"] = "sabot_sql_group";
        plan.streaming_sources.push_back(kafka_source);
        
        // Add a window operator
        StreamingMorselPlan::OperatorDescriptor window_op;
        window_op.operator_id = "window_agg_1";
        window_op.operator_type = "WindowAggregateOperator";
        window_op.is_stateful = true;
        window_op.requires_shuffle = false;
        window_op.parameters["window_type"] = "TUMBLE";
        window_op.parameters["window_size"] = "1h";
        window_op.parameters["timestamp_column"] = "timestamp";
        plan.operators.push_back(window_op);
        
        // Submit job
        auto execution_id_result = coordinator.SubmitStreamingJob(plan, "test_streaming_job");
        if (!execution_id_result.ok()) {
            std::cerr << "❌ Failed to submit streaming job: " << execution_id_result.status().message() << std::endl;
            return 1;
        }
        
        std::string execution_id = execution_id_result.ValueOrDie();
        std::cout << "✅ Streaming job submitted with execution ID: " << execution_id << std::endl;

        // 4. Test checkpoint coordination
        std::cout << "\n4. Testing checkpoint coordination..." << std::endl;
        
        auto checkpoint_id_result = coordinator.TriggerCheckpoint(execution_id);
        if (!checkpoint_id_result.ok()) {
            std::cerr << "❌ Failed to trigger checkpoint: " << checkpoint_id_result.status().message() << std::endl;
            return 1;
        }
        
        int64_t checkpoint_id = checkpoint_id_result.ValueOrDie();
        std::cout << "✅ Checkpoint triggered with ID: " << checkpoint_id << std::endl;
        
        // Wait for checkpoint completion
        status = coordinator.WaitForCheckpointCompletion(execution_id, checkpoint_id, 5000);
        if (!status.ok()) {
            std::cerr << "❌ Checkpoint completion failed: " << status.message() << std::endl;
            return 1;
        }
        std::cout << "✅ Checkpoint completed successfully" << std::endl;

        // 5. Test execution monitoring
        std::cout << "\n5. Testing execution monitoring..." << std::endl;
        
        auto status_result = coordinator.GetExecutionStatus(execution_id);
        if (!status_result.ok()) {
            std::cerr << "❌ Failed to get execution status: " << status_result.status().message() << std::endl;
            return 1;
        }
        
        auto execution_status = status_result.ValueOrDie();
        std::cout << "✅ Execution status retrieved:" << std::endl;
        for (const auto& [key, value] : execution_status) {
            std::cout << "   " << key << ": " << value << std::endl;
        }

        // 6. Test result collection
        std::cout << "\n6. Testing result collection..." << std::endl;
        
        auto results_result = coordinator.CollectResults(execution_id, "window_agg_1");
        if (!results_result.ok()) {
            std::cerr << "❌ Failed to collect results: " << results_result.status().message() << std::endl;
            return 1;
        }
        
        auto results = results_result.ValueOrDie();
        std::cout << "✅ Results collected: " << results->num_rows() << " rows, " << results->num_columns() << " columns" << std::endl;

        // 7. Test execution completion
        std::cout << "\n7. Testing execution completion..." << std::endl;
        
        auto completion_result = coordinator.WaitForCompletion(execution_id, 10000);
        if (!completion_result.ok()) {
            std::cerr << "❌ Failed to wait for completion: " << completion_result.status().message() << std::endl;
            return 1;
        }
        
        auto execution_result = completion_result.ValueOrDie();
        std::cout << "✅ Execution completed:" << std::endl;
        std::cout << "   Success: " << (execution_result.success ? "true" : "false") << std::endl;
        std::cout << "   Total rows processed: " << execution_result.total_rows_processed << std::endl;
        std::cout << "   Execution time: " << execution_result.execution_time_ms << " ms" << std::endl;
        std::cout << "   Completed tasks: " << execution_result.completed_tasks.size() << std::endl;

        // 8. Test cleanup
        std::cout << "\n8. Testing cleanup..." << std::endl;
        
        status = coordinator.CancelExecution(execution_id);
        if (!status.ok()) {
            std::cerr << "❌ Failed to cancel execution: " << status.message() << std::endl;
            return 1;
        }
        std::cout << "✅ Execution cancelled successfully" << std::endl;

        // 9. Test shutdown
        std::cout << "\n9. Testing shutdown..." << std::endl;
        
        status = coordinator.Shutdown();
        if (!status.ok()) {
            std::cerr << "❌ Failed to shutdown coordinator: " << status.message() << std::endl;
            return 1;
        }
        std::cout << "✅ Coordinator shutdown successfully" << std::endl;

        std::cout << "\n=== Sabot Execution Coordinator Test Complete ===" << std::endl;
        std::cout << "✅ All tests passed!" << std::endl;

    } catch (const std::exception& e) {
        std::cerr << "❌ Test failed with exception: " << e.what() << std::endl;
        return 1;
    }

    return 0;
}
