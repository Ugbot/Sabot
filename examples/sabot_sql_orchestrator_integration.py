#!/usr/bin/env python3
"""
SabotSQL Orchestrator Integration Example

This example demonstrates how to integrate SabotSQL with Sabot's orchestrator
for agent-based distributed SQL execution.
"""

import os
import sys
import time
import asyncio
from typing import List, Dict, Any, Optional
import pyarrow as pa
import pyarrow.parquet as pq
import numpy as np

# Add Sabot to path
sys.path.insert(0, '/Users/bengamble/Sabot')

# Import SabotSQL
try:
    from sabot_sql import (
        SabotSQLBridge,
        SabotOperatorTranslator,
        FlinkSQLExtension,
        QuestDBSQLExtension,
        execute_sql_on_agent,
        distribute_sql_query
    )
    SABOT_SQL_AVAILABLE = True
except ImportError:
    print("SabotSQL not available, using mock implementation")
    SABOT_SQL_AVAILABLE = False

class SabotSQLAgent:
    """Represents a SabotSQL agent for distributed execution"""
    
    def __init__(self, agent_id: str, bridge: Optional[SabotSQLBridge] = None):
        self.agent_id = agent_id
        self.bridge = bridge or SabotSQLBridge()
        self.tables = {}
        self.query_count = 0
        self.total_execution_time = 0.0
    
    def register_table(self, table_name: str, table: pa.Table):
        """Register a table with this agent"""
        self.tables[table_name] = table
        self.bridge.register_table(table_name, table)
        print(f"Agent {self.agent_id}: Registered table '{table_name}' with {table.num_rows} rows")
    
    def execute_query(self, sql_query: str) -> Dict[str, Any]:
        """Execute a SQL query on this agent"""
        start_time = time.time()
        
        try:
            # Parse and optimize
            plan = self.bridge.parse_and_optimize(sql_query)
            
            # Execute
            result = self.bridge.execute_sql(sql_query)
            
            execution_time = time.time() - start_time
            self.query_count += 1
            self.total_execution_time += execution_time
            
            return {
                'agent_id': self.agent_id,
                'query': sql_query,
                'plan': plan,
                'result': result,
                'execution_time': execution_time,
                'status': 'success'
            }
            
        except Exception as e:
            execution_time = time.time() - start_time
            return {
                'agent_id': self.agent_id,
                'query': sql_query,
                'error': str(e),
                'execution_time': execution_time,
                'status': 'error'
            }
    
    def get_stats(self) -> Dict[str, Any]:
        """Get agent statistics"""
        avg_time = self.total_execution_time / self.query_count if self.query_count > 0 else 0
        return {
            'agent_id': self.agent_id,
            'query_count': self.query_count,
            'total_execution_time': self.total_execution_time,
            'average_execution_time': avg_time,
            'tables': list(self.tables.keys())
        }

class SabotSQLOrchestrator:
    """Orchestrator for distributed SabotSQL execution"""
    
    def __init__(self):
        self.agents = {}
        self.flink_extension = FlinkSQLExtension() if SABOT_SQL_AVAILABLE else None
        self.questdb_extension = QuestDBSQLExtension() if SABOT_SQL_AVAILABLE else None
        self.operator_translator = SabotOperatorTranslator() if SABOT_SQL_AVAILABLE else None
    
    def add_agent(self, agent_id: str) -> SabotSQLAgent:
        """Add a new agent to the orchestrator"""
        agent = SabotSQLAgent(agent_id)
        self.agents[agent_id] = agent
        print(f"Added agent {agent_id} to orchestrator")
        return agent
    
    def remove_agent(self, agent_id: str):
        """Remove an agent from the orchestrator"""
        if agent_id in self.agents:
            del self.agents[agent_id]
            print(f"Removed agent {agent_id} from orchestrator")
    
    def distribute_table(self, table_name: str, table: pa.Table, strategy: str = "round_robin"):
        """Distribute a table across agents"""
        if not self.agents:
            raise ValueError("No agents available")
        
        if strategy == "round_robin":
            # Distribute rows evenly across agents
            num_agents = len(self.agents)
            rows_per_agent = table.num_rows // num_agents
            remainder = table.num_rows % num_agents
            
            start_row = 0
            for i, (agent_id, agent) in enumerate(self.agents.items()):
                # Calculate rows for this agent
                agent_rows = rows_per_agent + (1 if i < remainder else 0)
                end_row = start_row + agent_rows
                
                if start_row < table.num_rows:
                    # Slice table for this agent
                    agent_table = table.slice(start_row, agent_rows)
                    agent.register_table(f"{table_name}_{agent_id}", agent_table)
                
                start_row = end_row
        
        elif strategy == "replicate":
            # Replicate table to all agents
            for agent in self.agents.values():
                agent.register_table(table_name, table)
        
        else:
            raise ValueError(f"Unknown distribution strategy: {strategy}")
    
    def execute_distributed_query(self, sql_query: str, agent_ids: Optional[List[str]] = None) -> List[Dict[str, Any]]:
        """Execute a query across multiple agents"""
        if agent_ids is None:
            agent_ids = list(self.agents.keys())
        
        if not agent_ids:
            raise ValueError("No agents specified")
        
        # Check if query contains Flink constructs
        if self.flink_extension and self.flink_extension.contains_flink_constructs(sql_query):
            print("Detected Flink SQL constructs, preprocessing...")
            sql_query = self.flink_extension.preprocess_flink_sql(sql_query)
            print(f"Preprocessed SQL: {sql_query}")
        
        # Check if query contains QuestDB constructs
        if self.questdb_extension and self.questdb_extension.contains_questdb_constructs(sql_query):
            print("Detected QuestDB SQL constructs, preprocessing...")
            sql_query = self.questdb_extension.preprocess_questdb_sql(sql_query)
            print(f"Preprocessed SQL: {sql_query}")
        
        # Execute on each agent
        results = []
        for agent_id in agent_ids:
            if agent_id in self.agents:
                result = self.agents[agent_id].execute_query(sql_query)
                results.append(result)
            else:
                results.append({
                    'agent_id': agent_id,
                    'query': sql_query,
                    'error': f"Agent {agent_id} not found",
                    'status': 'error'
                })
        
        return results
    
    def execute_parallel_query(self, sql_query: str, agent_ids: Optional[List[str]] = None) -> List[Dict[str, Any]]:
        """Execute a query in parallel across agents"""
        if agent_ids is None:
            agent_ids = list(self.agents.keys())
        
        if not agent_ids:
            raise ValueError("No agents specified")
        
        # Use asyncio for parallel execution
        async def execute_on_agent(agent_id: str):
            if agent_id in self.agents:
                return self.agents[agent_id].execute_query(sql_query)
            else:
                return {
                    'agent_id': agent_id,
                    'query': sql_query,
                    'error': f"Agent {agent_id} not found",
                    'status': 'error'
                }
        
        async def run_parallel():
            tasks = [execute_on_agent(agent_id) for agent_id in agent_ids]
            return await asyncio.gather(*tasks)
        
        return asyncio.run(run_parallel())
    
    def get_orchestrator_stats(self) -> Dict[str, Any]:
        """Get orchestrator statistics"""
        agent_stats = {}
        total_queries = 0
        total_time = 0.0
        
        for agent_id, agent in self.agents.items():
            stats = agent.get_stats()
            agent_stats[agent_id] = stats
            total_queries += stats['query_count']
            total_time += stats['total_execution_time']
        
        return {
            'total_agents': len(self.agents),
            'total_queries': total_queries,
            'total_execution_time': total_time,
            'average_execution_time': total_time / total_queries if total_queries > 0 else 0,
            'agents': agent_stats
        }

def create_test_data(num_rows: int = 10000) -> pa.Table:
    """Create test data for demonstration"""
    np.random.seed(42)
    
    data = {
        'id': np.arange(num_rows),
        'user_id': np.random.randint(1, 1000, num_rows),
        'product_id': np.random.randint(1, 100, num_rows),
        'price': np.random.uniform(10.0, 1000.0, num_rows),
        'quantity': np.random.randint(1, 10, num_rows),
        'category': np.random.choice(['electronics', 'clothing', 'books', 'home'], num_rows),
        'rating': np.random.uniform(1.0, 5.0, num_rows)
    }
    
    return pa.Table.from_pydict(data)

def main():
    """Main demonstration function"""
    print("🚀 SabotSQL Orchestrator Integration Demo")
    print("=" * 50)
    
    if not SABOT_SQL_AVAILABLE:
        print("❌ SabotSQL not available. Please build the Cython bindings first.")
        return 1
    
    # Create orchestrator
    orchestrator = SabotSQLOrchestrator()
    
    # Add agents
    agents = ['agent_1', 'agent_2', 'agent_3', 'agent_4']
    for agent_id in agents:
        orchestrator.add_agent(agent_id)
    
    # Create test data
    print("\n📊 Creating test data...")
    test_data = create_test_data(10000)
    print(f"Created test data: {test_data.num_rows} rows, {test_data.num_columns} columns")
    
    # Distribute data across agents
    print("\n🔄 Distributing data across agents...")
    orchestrator.distribute_table("sales", test_data, strategy="round_robin")
    
    # Test queries
    test_queries = [
        "SELECT COUNT(*) FROM sales",
        "SELECT category, COUNT(*) FROM sales GROUP BY category",
        "SELECT user_id, AVG(price) FROM sales GROUP BY user_id LIMIT 10",
        "SELECT * FROM sales WHERE price > 500 ORDER BY price DESC LIMIT 100"
    ]
    
    print("\n🔍 Testing distributed query execution...")
    for i, query in enumerate(test_queries, 1):
        print(f"\nQuery {i}: {query}")
        print("-" * 40)
        
        # Execute on all agents
        results = orchestrator.execute_distributed_query(query)
        
        # Show results
        for result in results:
            if result['status'] == 'success':
                print(f"✅ Agent {result['agent_id']}: {result['execution_time']:.3f}s")
                if 'result' in result:
                    print(f"   Result: {result['result'].num_rows} rows, {result['result'].num_columns} columns")
            else:
                print(f"❌ Agent {result['agent_id']}: {result['error']}")
    
    # Test Flink SQL extensions
    print("\n🌊 Testing Flink SQL extensions...")
    flink_query = """
    SELECT 
        user_id,
        TUMBLE(timestamp, INTERVAL '1' HOUR) as window_start,
        COUNT(*) as event_count
    FROM sales
    WHERE CURRENT_TIMESTAMP > timestamp
    GROUP BY user_id, TUMBLE(timestamp, INTERVAL '1' HOUR)
    """
    
    if orchestrator.flink_extension:
        try:
            results = orchestrator.execute_distributed_query(flink_query)
            print("✅ Flink SQL query executed successfully")
        except Exception as e:
            print(f"❌ Flink SQL query failed: {e}")
    
    # Test QuestDB SQL extensions
    print("\n📈 Testing QuestDB SQL extensions...")
    questdb_query = """
    SELECT product_id, price, timestamp
    FROM sales
    SAMPLE BY 1h
    LATEST BY product_id
    """
    
    if orchestrator.questdb_extension:
        try:
            results = orchestrator.execute_distributed_query(questdb_query)
            print("✅ QuestDB SQL query executed successfully")
        except Exception as e:
            print(f"❌ QuestDB SQL query failed: {e}")
    
    # Show orchestrator statistics
    print("\n📊 Orchestrator Statistics:")
    print("-" * 30)
    stats = orchestrator.get_orchestrator_stats()
    print(f"Total agents: {stats['total_agents']}")
    print(f"Total queries: {stats['total_queries']}")
    print(f"Total execution time: {stats['total_execution_time']:.3f}s")
    print(f"Average execution time: {stats['average_execution_time']:.3f}s")
    
    print("\n🎉 SabotSQL Orchestrator integration demo completed!")
    return 0

if __name__ == "__main__":
    sys.exit(main())
