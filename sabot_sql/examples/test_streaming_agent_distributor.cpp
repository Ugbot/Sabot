#include <iostream>
#include <arrow/api.h>
#include <arrow/table.h>
#include <arrow/array.h>
#include <arrow/builder.h>
#include "sabot_sql/streaming/streaming_agent_distributor.h"
#include "sabot_sql/streaming/marbledb_integration.h"
#include "sabot_sql/streaming/checkpoint_barrier_injector.h"

using namespace sabot_sql::streaming;

int main() {
    std::cout << "=== Testing Streaming Agent Distributor ===" << std::endl;

    try {
        // 1. Test StreamingAgentDistributor initialization
        std::cout << "\n1. Testing StreamingAgentDistributor initialization..." << std::endl;
        
        StreamingAgentDistributor distributor;
        
        // Initialize embedded MarbleDB integration
        auto marbledb = std::make_shared<MarbleDBIntegration>();
        auto status = marbledb->Initialize("test_agent_distributor", "./test_distributor_marbledb", false);
        if (!status.ok()) {
            std::cerr << "❌ Failed to initialize embedded MarbleDB: " << status.message() << std::endl;
            return 1;
        }
        std::cout << "✅ MarbleDB integration initialized" << std::endl;
        
        // Initialize checkpoint barrier injector
        auto barrier_injector = std::make_shared<CheckpointBarrierInjector>();
        status = barrier_injector->Initialize(nullptr, marbledb);
        if (!status.ok()) {
            std::cerr << "❌ Failed to initialize barrier injector: " << status.message() << std::endl;
            return 1;
        }
        std::cout << "✅ Barrier injector initialized" << std::endl;
        
        // Initialize distributor
        status = distributor.Initialize(marbledb, barrier_injector);
        if (!status.ok()) {
            std::cerr << "❌ Failed to initialize distributor: " << status.message() << std::endl;
            return 1;
        }
        std::cout << "✅ Agent distributor initialized successfully" << std::endl;

        // 2. Test agent registration
        std::cout << "\n2. Testing agent registration..." << std::endl;
        
        // Register multiple agents
        for (int i = 0; i < 4; ++i) {
            StreamingAgentDistributor::AgentConfig agent;
            agent.agent_id = "agent_" + std::to_string(i);
            agent.host = "localhost";
            agent.port = 9000 + i;
            agent.available_slots = 4;
            agent.cpu_percent = 50 + i * 10;
            agent.memory_percent = 60 + i * 5;
            agent.status = "alive";
            agent.last_heartbeat = std::chrono::duration_cast<std::chrono::milliseconds>(
                std::chrono::system_clock::now().time_since_epoch()
            ).count();
            
            status = distributor.RegisterAgent(agent);
            if (!status.ok()) {
                std::cerr << "❌ Failed to register agent " << i << ": " << status.message() << std::endl;
                return 1;
            }
        }
        std::cout << "✅ 4 agents registered successfully" << std::endl;

        // 3. Test agent management
        std::cout << "\n3. Testing agent management..." << std::endl;
        
        // Get available agents
        auto available_agents_result = distributor.GetAvailableAgents();
        if (!available_agents_result.ok()) {
            std::cerr << "❌ Failed to get available agents: " << available_agents_result.status().message() << std::endl;
            return 1;
        }
        
        auto available_agents = available_agents_result.ValueOrDie();
        std::cout << "✅ Found " << available_agents.size() << " available agents" << std::endl;
        
        // Get specific agent
        auto agent_result = distributor.GetAgent("agent_1");
        if (!agent_result.ok()) {
            std::cerr << "❌ Failed to get agent: " << agent_result.status().message() << std::endl;
            return 1;
        }
        
        auto agent = agent_result.ValueOrDie();
        std::cout << "✅ Agent retrieved: " << agent.agent_id << " (CPU: " << agent.cpu_percent << "%, Memory: " << agent.memory_percent << "%)" << std::endl;
        
        // Update agent status
        status = distributor.UpdateAgentStatus("agent_1", "busy");
        if (!status.ok()) {
            std::cerr << "❌ Failed to update agent status: " << status.message() << std::endl;
            return 1;
        }
        std::cout << "✅ Agent status updated" << std::endl;

        // 4. Test distribution plan creation
        std::cout << "\n4. Testing distribution plan creation..." << std::endl;
        
        // Create a mock streaming plan
        StreamingMorselPlan plan;
        plan.query_id = "test_query_456";
        plan.query_text = "SELECT symbol, COUNT(*) FROM trades GROUP BY symbol";
        plan.is_streaming = true;
        plan.max_parallelism = 4;
        plan.checkpoint_interval = "30s";
        plan.state_backend = "marbledb";
        plan.timer_backend = "marbledb";
        
        // Add Kafka sources
        for (int i = 0; i < 2; ++i) {
            StreamingMorselPlan::StreamingSource kafka_source;
            kafka_source.source_type = "kafka";
            kafka_source.topic = "trades";
            kafka_source.partition = i;
            kafka_source.max_parallelism = 2;
            kafka_source.batch_size = 10000;
            kafka_source.watermark_column = "timestamp";
            kafka_source.properties["bootstrap.servers"] = "localhost:9092";
            kafka_source.properties["group.id"] = "sabot_sql_group";
            plan.streaming_sources.push_back(kafka_source);
        }
        
        // Add file source
        StreamingMorselPlan::StreamingSource file_source;
        file_source.source_type = "file";
        file_source.topic = "/data/streaming/file1.csv";
        file_source.max_parallelism = 1;
        file_source.batch_size = 5000;
        file_source.properties["format"] = "csv";
        plan.streaming_sources.push_back(file_source);
        
        // Add operators
        StreamingMorselPlan::OperatorDescriptor window_op;
        window_op.operator_id = "window_agg_1";
        window_op.operator_type = "WindowAggregateOperator";
        window_op.is_stateful = true;
        window_op.requires_shuffle = false;
        window_op.parameters["window_type"] = "TUMBLE";
        window_op.parameters["window_size"] = "1h";
        plan.operators.push_back(window_op);
        
        std::string execution_id = "test_execution_456";
        
        // Create distribution plan
        auto distribution_plan_result = distributor.CreateDistributionPlan(plan, execution_id);
        if (!distribution_plan_result.ok()) {
            std::cerr << "❌ Failed to create distribution plan: " << distribution_plan_result.status().message() << std::endl;
            return 1;
        }
        
        auto distribution_plan = distribution_plan_result.ValueOrDie();
        std::cout << "✅ Distribution plan created:" << std::endl;
        std::cout << "   Execution ID: " << distribution_plan.execution_id << std::endl;
        std::cout << "   Total partitions: " << distribution_plan.total_partitions << std::endl;
        std::cout << "   Assigned agents: " << distribution_plan.assigned_agents << std::endl;
        std::cout << "   Partition assignments: " << distribution_plan.partition_assignments.size() << std::endl;

        // 5. Test task deployment
        std::cout << "\n5. Testing task deployment..." << std::endl;
        
        status = distributor.DeployTasksToAgents(distribution_plan);
        if (!status.ok()) {
            std::cerr << "❌ Failed to deploy tasks: " << status.message() << std::endl;
            return 1;
        }
        std::cout << "✅ Tasks deployed to agents" << std::endl;
        
        // Start execution
        status = distributor.StartExecution(execution_id);
        if (!status.ok()) {
            std::cerr << "❌ Failed to start execution: " << status.message() << std::endl;
            return 1;
        }
        std::cout << "✅ Execution started" << std::endl;

        // 6. Test partition management
        std::cout << "\n6. Testing partition management..." << std::endl;
        
        // Assign partition to agent
        status = distributor.AssignPartitionToAgent(execution_id, "trades_partition_0", "agent_2");
        if (!status.ok()) {
            std::cerr << "❌ Failed to assign partition: " << status.message() << std::endl;
            return 1;
        }
        std::cout << "✅ Partition assigned to agent" << std::endl;
        
        // Reassign partition
        status = distributor.ReassignPartition(execution_id, "trades_partition_0", "agent_3");
        if (!status.ok()) {
            std::cerr << "❌ Failed to reassign partition: " << status.message() << std::endl;
            return 1;
        }
        std::cout << "✅ Partition reassigned" << std::endl;
        
        // Rebalance partitions
        status = distributor.RebalancePartitions(execution_id);
        if (!status.ok()) {
            std::cerr << "❌ Failed to rebalance partitions: " << status.message() << std::endl;
            return 1;
        }
        std::cout << "✅ Partitions rebalanced" << std::endl;

        // 7. Test result collection
        std::cout << "\n7. Testing result collection..." << std::endl;
        
        // Create mock result batch
        arrow::StringBuilder key_builder;
        arrow::Int64Builder count_builder;
        arrow::DoubleBuilder sum_builder;
        
        if (!key_builder.Append("AAPL").ok()) return 1;
        if (!count_builder.Append(1000).ok()) return 1;
        if (!sum_builder.Append(150000.0).ok()) return 1;
        
        auto key_array_result = key_builder.Finish();
        if (!key_array_result.ok()) return 1;
        auto key_array = key_array_result.ValueOrDie();
        
        auto count_array_result = count_builder.Finish();
        if (!count_array_result.ok()) return 1;
        auto count_array = count_array_result.ValueOrDie();
        
        auto sum_array_result = sum_builder.Finish();
        if (!sum_array_result.ok()) return 1;
        auto sum_array = sum_array_result.ValueOrDie();
        
        auto batch = arrow::RecordBatch::Make(
            arrow::schema({
                arrow::field("key", arrow::utf8()),
                arrow::field("count", arrow::int64()),
                arrow::field("sum", arrow::float64())
            }),
            1,
            {key_array, count_array, sum_array}
        );
        
        // Aggregate results
        status = distributor.AggregateResults(execution_id, "window_agg_1", batch);
        if (!status.ok()) {
            std::cerr << "❌ Failed to aggregate results: " << status.message() << std::endl;
            return 1;
        }
        std::cout << "✅ Results aggregated" << std::endl;
        
        // Collect results
        auto results_result = distributor.CollectResults(execution_id, "window_agg_1");
        if (!results_result.ok()) {
            std::cerr << "❌ Failed to collect results: " << results_result.status().message() << std::endl;
            return 1;
        }
        
        auto results = results_result.ValueOrDie();
        std::cout << "✅ Results collected: " << results->num_rows() << " rows, " << results->num_columns() << " columns" << std::endl;

        // 8. Test execution metrics
        std::cout << "\n8. Testing execution metrics..." << std::endl;
        
        StreamingAgentDistributor::ExecutionMetrics metrics;
        metrics.execution_id = execution_id;
        metrics.total_rows_processed = 1000000;
        metrics.total_bytes_processed = 50000000;
        metrics.execution_time_ms = 5000;
        metrics.agent_metrics["agent_0"] = 250000;
        metrics.agent_metrics["agent_1"] = 250000;
        metrics.agent_metrics["agent_2"] = 250000;
        metrics.agent_metrics["agent_3"] = 250000;
        metrics.partition_metrics["trades_partition_0"] = 500000;
        metrics.partition_metrics["trades_partition_1"] = 500000;
        metrics.last_updated = std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::system_clock::now().time_since_epoch()
        ).count();
        
        status = distributor.UpdateExecutionMetrics(execution_id, metrics);
        if (!status.ok()) {
            std::cerr << "❌ Failed to update execution metrics: " << status.message() << std::endl;
            return 1;
        }
        std::cout << "✅ Execution metrics updated" << std::endl;
        
        // Get execution metrics
        auto metrics_result = distributor.GetExecutionMetrics(execution_id);
        if (!metrics_result.ok()) {
            std::cerr << "❌ Failed to get execution metrics: " << metrics_result.status().message() << std::endl;
            return 1;
        }
        
        auto retrieved_metrics = metrics_result.ValueOrDie();
        std::cout << "✅ Execution metrics retrieved:" << std::endl;
        std::cout << "   Total rows processed: " << retrieved_metrics.total_rows_processed << std::endl;
        std::cout << "   Execution time: " << retrieved_metrics.execution_time_ms << " ms" << std::endl;
        std::cout << "   Agent count: " << retrieved_metrics.agent_metrics.size() << std::endl;
        std::cout << "   Partition count: " << retrieved_metrics.partition_metrics.size() << std::endl;

        // 9. Test active executions
        std::cout << "\n9. Testing active executions..." << std::endl;
        
        auto active_executions_result = distributor.GetActiveExecutions();
        if (!active_executions_result.ok()) {
            std::cerr << "❌ Failed to get active executions: " << active_executions_result.status().message() << std::endl;
            return 1;
        }
        
        auto active_executions = active_executions_result.ValueOrDie();
        std::cout << "✅ Found " << active_executions.size() << " active executions" << std::endl;

        // 10. Test failure handling
        std::cout << "\n10. Testing failure handling..." << std::endl;
        
        // Handle agent failure
        status = distributor.HandleAgentFailure("agent_1");
        if (!status.ok()) {
            std::cerr << "❌ Failed to handle agent failure: " << status.message() << std::endl;
            return 1;
        }
        std::cout << "✅ Agent failure handled" << std::endl;
        
        // Recover from agent failure
        status = distributor.RecoverFromAgentFailure(execution_id, "agent_1");
        if (!status.ok()) {
            std::cerr << "❌ Failed to recover from agent failure: " << status.message() << std::endl;
            return 1;
        }
        std::cout << "✅ Recovery from agent failure completed" << std::endl;

        // 11. Test execution control
        std::cout << "\n11. Testing execution control..." << std::endl;
        
        // Stop execution
        status = distributor.StopExecution(execution_id);
        if (!status.ok()) {
            std::cerr << "❌ Failed to stop execution: " << status.message() << std::endl;
            return 1;
        }
        std::cout << "✅ Execution stopped" << std::endl;
        
        // Cancel execution
        status = distributor.CancelExecution(execution_id);
        if (!status.ok()) {
            std::cerr << "❌ Failed to cancel execution: " << status.message() << std::endl;
            return 1;
        }
        std::cout << "✅ Execution cancelled" << std::endl;

        // 12. Test agent unregistration
        std::cout << "\n12. Testing agent unregistration..." << std::endl;
        
        status = distributor.UnregisterAgent("agent_0");
        if (!status.ok()) {
            std::cerr << "❌ Failed to unregister agent: " << status.message() << std::endl;
            return 1;
        }
        std::cout << "✅ Agent unregistered" << std::endl;

        // 13. Test shutdown
        std::cout << "\n13. Testing shutdown..." << std::endl;
        
        status = distributor.Shutdown();
        if (!status.ok()) {
            std::cerr << "❌ Failed to shutdown distributor: " << status.message() << std::endl;
            return 1;
        }
        std::cout << "✅ Distributor shutdown successfully" << std::endl;

        std::cout << "\n=== Streaming Agent Distributor Test Complete ===" << std::endl;
        std::cout << "✅ All tests passed!" << std::endl;

    } catch (const std::exception& e) {
        std::cerr << "❌ Test failed with exception: " << e.what() << std::endl;
        return 1;
    }

    return 0;
}
