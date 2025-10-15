#!/usr/bin/env python3
"""
SabotCypher Streaming Graph Queries Benchmark

Benchmark streaming graph query performance.
"""

import sys
import os
import pyarrow as pa
import time
from pathlib import Path
from datetime import datetime, timedelta
import random

# Add streaming module to path
sys.path.append(str(Path(__file__).parent.parent))

from streaming import StreamingGraphProcessor


class StreamingBenchmark:
    """Benchmark streaming graph query performance."""
    
    def __init__(self):
        """Initialize benchmark."""
        self.processor = StreamingGraphProcessor(
            window_size=timedelta(minutes=5),
            slide_interval=timedelta(seconds=1),
            ttl=timedelta(hours=1)
        )
        
        self.results = {
            'ingestion_times': [],
            'query_times': [],
            'total_vertices': 0,
            'total_edges': 0
        }
    
    def benchmark_ingestion(self, num_batches: int = 100, batch_size: int = 1000):
        """Benchmark ingestion throughput."""
        print(f"\n📊 Benchmarking Ingestion:")
        print(f"   Batches: {num_batches}")
        print(f"   Batch size: {batch_size}")
        print(f"   Total events: {num_batches * batch_size:,}")
        
        total_start = time.time()
        
        for batch_num in range(num_batches):
            # Generate batch
            vertices = pa.table({
                'id': pa.array([batch_num * batch_size + i for i in range(batch_size)], type=pa.int64()),
                'name': pa.array([f"User_{batch_num * batch_size + i}" for i in range(batch_size)]),
                'type': pa.array(['Person'] * batch_size),
                'timestamp': pa.array([datetime.now()] * batch_size)
            })
            
            edges = pa.table({
                'source': pa.array([batch_num * batch_size + i for i in range(batch_size)], type=pa.int64()),
                'target': pa.array([batch_num * batch_size + ((i + 1) % batch_size) for i in range(batch_size)], type=pa.int64()),
                'type': pa.array(['FOLLOWS'] * batch_size),
                'timestamp': pa.array([datetime.now()] * batch_size)
            })
            
            # Measure ingestion time
            start_time = time.time()
            self.processor.ingest_batch(vertices, edges)
            end_time = time.time()
            
            ingestion_time = (end_time - start_time) * 1000
            self.results['ingestion_times'].append(ingestion_time)
            self.results['total_vertices'] += vertices.num_rows
            self.results['total_edges'] += edges.num_rows
            
            if (batch_num + 1) % 10 == 0:
                print(f"   Batch {batch_num + 1}/{num_batches}: {ingestion_time:.2f}ms")
        
        total_end = time.time()
        total_time = (total_end - total_start) * 1000
        
        avg_ingestion = sum(self.results['ingestion_times']) / len(self.results['ingestion_times'])
        events_per_sec = (num_batches * batch_size * 1000) / total_time
        
        print(f"\n✅ Ingestion Benchmark:")
        print(f"   Total time: {total_time:.2f}ms")
        print(f"   Avg batch time: {avg_ingestion:.2f}ms")
        print(f"   Throughput: {events_per_sec:,.0f} events/sec")
        print(f"   Total events: {self.results['total_vertices'] + self.results['total_edges']:,}")
    
    def benchmark_queries(self):
        """Benchmark query execution."""
        print(f"\n📊 Benchmarking Queries:")
        
        queries = {
            'Simple_Scan': "MATCH (a:Person) RETURN a.name LIMIT 10",
            'Aggregation': "MATCH (a:Person) RETURN count(*) as total",
            'Pattern_Match': "MATCH (a:Person)-[:FOLLOWS]->(b:Person) RETURN a.name, b.name LIMIT 10",
            'Multi_Hop': "MATCH (a:Person)-[:FOLLOWS]->(b:Person)-[:FOLLOWS]->(c:Person) RETURN count(*) as paths"
        }
        
        # Get current graph
        vertices, edges = self.processor.get_current_graph()
        print(f"   Current window: {vertices.num_rows} vertices, {edges.num_rows} edges")
        
        for query_name, query in queries.items():
            times = []
            
            for i in range(10):
                start_time = time.time()
                result = self.processor.query_current_window(query)
                end_time = time.time()
                
                query_time = (end_time - start_time) * 1000
                times.append(query_time)
            
            avg_time = sum(times) / len(times)
            min_time = min(times)
            max_time = max(times)
            
            print(f"   {query_name}: {avg_time:.4f}ms avg ({min_time:.4f}-{max_time:.4f}ms)")
            
            self.results['query_times'].append({
                'query': query_name,
                'avg_time_ms': avg_time,
                'min_time_ms': min_time,
                'max_time_ms': max_time
            })
    
    def print_summary(self):
        """Print benchmark summary."""
        print(f"\n{'='*60}")
        print("STREAMING BENCHMARK SUMMARY")
        print(f"{'='*60}")
        
        # Ingestion summary
        avg_ingestion = sum(self.results['ingestion_times']) / len(self.results['ingestion_times']) if self.results['ingestion_times'] else 0
        total_events = self.results['total_vertices'] + self.results['total_edges']
        total_time = sum(self.results['ingestion_times'])
        throughput = (total_events * 1000) / total_time if total_time > 0 else 0
        
        print(f"\n📊 Ingestion Performance:")
        print(f"   Total events: {total_events:,}")
        print(f"   Avg batch time: {avg_ingestion:.2f}ms")
        print(f"   Throughput: {throughput:,.0f} events/sec")
        
        # Query summary
        if self.results['query_times']:
            avg_query_time = sum(q['avg_time_ms'] for q in self.results['query_times']) / len(self.results['query_times'])
            
            print(f"\n📊 Query Performance:")
            print(f"   Avg query time: {avg_query_time:.4f}ms")
            print(f"   Queries tested: {len(self.results['query_times'])}")
        
        # Storage stats
        stats = self.processor.get_stats()
        print(f"\n📊 Storage Statistics:")
        print(f"   Stored vertices: {stats['store']['total_vertices']:,}")
        print(f"   Stored edges: {stats['store']['total_edges']:,}")
        print(f"   Vertex buckets: {stats['store']['num_vertex_buckets']}")
        print(f"   Edge buckets: {stats['store']['num_edge_buckets']}")
        
        print(f"\n{'='*60}")
        print("BENCHMARK COMPLETE!")
        print(f"{'='*60}")


def main():
    """Run streaming benchmark."""
    print("SABOT_CYPHER STREAMING GRAPH QUERIES BENCHMARK")
    print("=" * 60)
    
    benchmark = StreamingBenchmark()
    
    # Benchmark ingestion
    benchmark.benchmark_ingestion(num_batches=100, batch_size=1000)
    
    # Benchmark queries
    benchmark.benchmark_queries()
    
    # Print summary
    benchmark.print_summary()
    
    return 0


if __name__ == "__main__":
    sys.exit(main())

