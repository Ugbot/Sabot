#!/usr/bin/env python3
"""
Complete Streaming SQL Example

This example demonstrates the full streaming SQL capabilities of SabotSQL:
- Kafka source with watermark processing
- Stateful window aggregation
- Dimension table broadcast join
- Checkpointing and error recovery
- Comprehensive monitoring and observability

Requirements:
- Kafka running on localhost:9092
- Topics: 'trades', 'securities'
- MarbleDB state backend
"""

import asyncio
import time
import json
from typing import Dict, Any, List
import pyarrow as pa
import pyarrow.compute as pc

from sabot_sql import StreamingSQLExecutor


class StreamingSQLDemo:
    """Complete streaming SQL demonstration"""
    
    def __init__(self):
        self.executor = None
        self.metrics = {}
        self.error_count = 0
        self.processed_batches = 0
        self.start_time = None
        
    async def initialize_executor(self):
        """Initialize the streaming SQL executor with comprehensive configuration"""
        print("🚀 Initializing StreamingSQLExecutor...")
        
        self.executor = StreamingSQLExecutor(
            # State management
            state_backend='marbledb',
            timer_backend='marbledb',
            state_path='./streaming_state_demo',
            
            # Execution configuration
            checkpoint_interval_seconds=30,  # 30 second checkpoints
            max_parallelism=8,              # Up to 8 Kafka partitions
            watermark_idle_timeout_ms=10000, # 10 second watermark timeout
            
            # Error handling
            enable_error_recovery=True,
            max_retries=3,
            retry_delay_ms=1000,
            
            # Monitoring
            enable_metrics=True,
            metrics_export_interval_ms=5000,
            enable_health_checks=True
        )
        
        print("✅ Executor initialized successfully")
        
    async def setup_dimension_tables(self):
        """Set up dimension tables for broadcast joins"""
        print("\n📊 Setting up dimension tables...")
        
        # Securities dimension table
        securities_schema = pa.schema([
            ('symbol', pa.string()),
            ('company_name', pa.string()),
            ('sector', pa.string()),
            ('industry', pa.string()),
            ('market_cap', pa.float64()),
            ('shares_outstanding', pa.int64())
        ])
        
        # Create columnar data
        symbols = ['AAPL', 'MSFT', 'GOOGL', 'AMZN', 'TSLA', 'NVDA', 'META', 'NFLX']
        companies = ['Apple Inc.', 'Microsoft Corporation', 'Alphabet Inc.', 'Amazon.com Inc.', 
                    'Tesla Inc.', 'NVIDIA Corporation', 'Meta Platforms Inc.', 'Netflix Inc.']
        sectors = ['Technology', 'Technology', 'Technology', 'Consumer Discretionary', 
                  'Consumer Discretionary', 'Technology', 'Technology', 'Communication Services']
        industries = ['Consumer Electronics', 'Software', 'Internet Services', 'E-commerce',
                     'Automotive', 'Semiconductors', 'Social Media', 'Streaming']
        market_caps = [3000000000000.0, 2800000000000.0, 1800000000000.0, 1600000000000.0,
                      800000000000.0, 1200000000000.0, 700000000000.0, 200000000000.0]
        shares_outstanding = [15000000000, 7500000000, 3000000000, 500000000,
                             1000000000, 2500000000, 2700000000, 450000000]
        
        securities_table = pa.table({
            'symbol': symbols,
            'company_name': companies,
            'sector': sectors,
            'industry': industries,
            'market_cap': market_caps,
            'shares_outstanding': shares_outstanding
        })
        
        # Register securities dimension table (RAFT replicated)
        self.executor.register_dimension_table(
            'securities',
            securities_table,
            is_raft_replicated=True
        )
        
        print(f"✅ Registered securities dimension table: {securities_table.num_rows} rows")
        
        # Market indicators dimension table
        indicators_schema = pa.schema([
            ('indicator', pa.string()),
            ('value', pa.float64()),
            ('threshold', pa.float64()),
            ('description', pa.string())
        ])
        
        # Create columnar data for indicators
        indicators = ['VIX', 'DXY', 'TNX', 'GOLD']
        values = [20.5, 103.2, 4.5, 2000.0]
        thresholds = [30.0, 100.0, 5.0, 1800.0]
        descriptions = ['Volatility Index', 'Dollar Index', '10-Year Treasury Yield', 'Gold Price per Ounce']
        
        indicators_table = pa.table({
            'indicator': indicators,
            'value': values,
            'threshold': thresholds,
            'description': descriptions
        })
        
        # Register indicators dimension table (RAFT replicated)
        self.executor.register_dimension_table(
            'market_indicators',
            indicators_table,
            is_raft_replicated=True
        )
        
        print(f"✅ Registered market indicators dimension table: {indicators_table.num_rows} rows")
        
    async def setup_streaming_sources(self):
        """Set up streaming data sources"""
        print("\n📡 Setting up streaming sources...")
        
        # Trades streaming source
        self.executor.execute_ddl("""
            CREATE TABLE trades (
                symbol VARCHAR,
                price DOUBLE,
                volume BIGINT,
                bid_price DOUBLE,
                ask_price DOUBLE,
                trade_type VARCHAR,
                ts TIMESTAMP
            ) WITH (
                'connector' = 'kafka',
                'topic' = 'trades',
                'bootstrap.servers' = 'localhost:9092',
                'group.id' = 'sabot_streaming_demo',
                'auto.offset.reset' = 'earliest',
                'enable.auto.commit' = 'false'
            )
        """)
        
        # Market data streaming source
        self.executor.execute_ddl("""
            CREATE TABLE market_data (
                symbol VARCHAR,
                open_price DOUBLE,
                high_price DOUBLE,
                low_price DOUBLE,
                close_price DOUBLE,
                volume BIGINT,
                ts TIMESTAMP
            ) WITH (
                'connector' = 'kafka',
                'topic' = 'market_data',
                'bootstrap.servers' = 'localhost:9092',
                'group.id' = 'sabot_streaming_demo',
                'auto.offset.reset' = 'earliest',
                'enable.auto.commit' = 'false'
            )
        """)
        
        print("✅ Streaming sources configured")
        
    async def execute_streaming_queries(self):
        """Execute comprehensive streaming SQL queries"""
        print("\n🔄 Executing streaming SQL queries...")
        
        self.start_time = time.time()
        
        # Query 1: Real-time trading analytics with dimension joins
        print("\n📈 Query 1: Real-time trading analytics")
        await self.execute_trading_analytics()
        
        # Query 2: Market volatility analysis
        print("\n📊 Query 2: Market volatility analysis")
        await self.execute_volatility_analysis()
        
        # Query 3: Cross-asset correlation analysis
        print("\n🔗 Query 3: Cross-asset correlation analysis")
        await self.execute_correlation_analysis()
        
    async def execute_trading_analytics(self):
        """Execute real-time trading analytics query"""
        query = """
            SELECT 
                t.symbol,
                s.company_name,
                s.sector,
                s.industry,
                TUMBLE(t.ts, INTERVAL '1' MINUTE) as window_start,
                TUMBLE_END(t.ts, INTERVAL '1' MINUTE) as window_end,
                COUNT(*) as trade_count,
                SUM(t.volume) as total_volume,
                AVG(t.price) as avg_price,
                MIN(t.price) as min_price,
                MAX(t.price) as max_price,
                AVG(t.bid_price) as avg_bid,
                AVG(t.ask_price) as avg_ask,
                AVG(t.ask_price - t.bid_price) as avg_spread,
                SUM(CASE WHEN t.trade_type = 'BUY' THEN t.volume ELSE 0 END) as buy_volume,
                SUM(CASE WHEN t.trade_type = 'SELL' THEN t.volume ELSE 0 END) as sell_volume
            FROM trades t
            LEFT JOIN securities s ON t.symbol = s.symbol
            GROUP BY t.symbol, s.company_name, s.sector, s.industry, 
                     TUMBLE(t.ts, INTERVAL '1' MINUTE)
            HAVING COUNT(*) > 0
        """
        
        try:
            async for batch in self.executor.execute_streaming_sql(query):
                await self.process_trading_batch(batch)
                
        except Exception as e:
            print(f"❌ Trading analytics query failed: {e}")
            self.error_count += 1
            
    async def execute_volatility_analysis(self):
        """Execute market volatility analysis query"""
        query = """
            SELECT 
                symbol,
                TUMBLE(ts, INTERVAL '5' MINUTE) as window_start,
                COUNT(*) as data_points,
                AVG(price) as avg_price,
                STDDEV(price) as price_volatility,
                (MAX(price) - MIN(price)) / AVG(price) as price_range_pct,
                SUM(volume) as total_volume,
                AVG(volume) as avg_volume,
                STDDEV(volume) as volume_volatility
            FROM trades
            GROUP BY symbol, TUMBLE(ts, INTERVAL '5' MINUTE)
            HAVING COUNT(*) > 10
        """
        
        try:
            async for batch in self.executor.execute_streaming_sql(query):
                await self.process_volatility_batch(batch)
                
        except Exception as e:
            print(f"❌ Volatility analysis query failed: {e}")
            self.error_count += 1
            
    async def execute_correlation_analysis(self):
        """Execute cross-asset correlation analysis query"""
        query = """
            SELECT 
                TUMBLE(t.ts, INTERVAL '10' MINUTE) as window_start,
                COUNT(DISTINCT t.symbol) as symbol_count,
                AVG(t.price) as market_avg_price,
                STDDEV(t.price) as market_volatility,
                SUM(t.volume) as total_market_volume,
                COUNT(*) as total_trades,
                AVG(t.ask_price - t.bid_price) as avg_market_spread
            FROM trades t
            GROUP BY TUMBLE(t.ts, INTERVAL '10' MINUTE)
            HAVING COUNT(DISTINCT t.symbol) > 1
        """
        
        try:
            async for batch in self.executor.execute_streaming_sql(query):
                await self.process_correlation_batch(batch)
                
        except Exception as e:
            print(f"❌ Correlation analysis query failed: {e}")
            self.error_count += 1
            
    async def process_trading_batch(self, batch):
        """Process trading analytics batch"""
        self.processed_batches += 1
        
        print(f"📈 Trading Analytics Batch #{self.processed_batches}:")
        print(f"   Rows: {batch.num_rows}")
        
        if batch.num_rows > 0:
            # Extract key metrics
            symbols = batch.column('symbol').to_pylist()
            companies = batch.column('company_name').to_pylist()
            sectors = batch.column('sector').to_pylist()
            trade_counts = batch.column('trade_count').to_pylist()
            total_volumes = batch.column('total_volume').to_pylist()
            avg_prices = batch.column('avg_price').to_pylist()
            avg_spreads = batch.column('avg_spread').to_pylist()
            
            # Display top performers
            for i in range(min(3, len(symbols))):
                symbol = symbols[i]
                company = companies[i]
                sector = sectors[i]
                trades = trade_counts[i]
                volume = total_volumes[i]
                price = avg_prices[i]
                spread = avg_spreads[i]
                
                print(f"   {symbol} ({company}, {sector}): "
                      f"{trades} trades, {volume:,} shares, "
                      f"${price:.2f}, spread ${spread:.4f}")
        
        # Update metrics
        self.metrics['trading_batches'] = self.metrics.get('trading_batches', 0) + 1
        self.metrics['total_rows_processed'] = self.metrics.get('total_rows_processed', 0) + batch.num_rows
        
    async def process_volatility_batch(self, batch):
        """Process volatility analysis batch"""
        self.processed_batches += 1
        
        print(f"📊 Volatility Analysis Batch #{self.processed_batches}:")
        print(f"   Rows: {batch.num_rows}")
        
        if batch.num_rows > 0:
            # Extract volatility metrics
            symbols = batch.column('symbol').to_pylist()
            volatilities = batch.column('price_volatility').to_pylist()
            price_ranges = batch.column('price_range_pct').to_pylist()
            volumes = batch.column('total_volume').to_pylist()
            
            # Display volatility leaders
            for i in range(min(3, len(symbols))):
                symbol = symbols[i]
                volatility = volatilities[i]
                price_range = price_ranges[i]
                volume = volumes[i]
                
                print(f"   {symbol}: Volatility ${volatility:.4f}, "
                      f"Range {price_range:.2%}, Volume {volume:,}")
        
        # Update metrics
        self.metrics['volatility_batches'] = self.metrics.get('volatility_batches', 0) + 1
        
    async def process_correlation_batch(self, batch):
        """Process correlation analysis batch"""
        self.processed_batches += 1
        
        print(f"🔗 Correlation Analysis Batch #{self.processed_batches}:")
        print(f"   Rows: {batch.num_rows}")
        
        if batch.num_rows > 0:
            # Extract market metrics
            symbol_counts = batch.column('symbol_count').to_pylist()
            market_prices = batch.column('market_avg_price').to_pylist()
            market_volatilities = batch.column('market_volatility').to_pylist()
            total_volumes = batch.column('total_market_volume').to_pylist()
            total_trades = batch.column('total_trades').to_pylist()
            avg_spreads = batch.column('avg_market_spread').to_pylist()
            
            # Display market overview
            for i in range(len(symbol_counts)):
                symbols = symbol_counts[i]
                price = market_prices[i]
                volatility = market_volatilities[i]
                volume = total_volumes[i]
                trades = total_trades[i]
                spread = avg_spreads[i]
                
                print(f"   Market: {symbols} symbols, ${price:.2f} avg, "
                      f"${volatility:.4f} volatility, {volume:,} volume, "
                      f"{trades} trades, ${spread:.4f} spread")
        
        # Update metrics
        self.metrics['correlation_batches'] = self.metrics.get('correlation_batches', 0) + 1
        
    async def monitor_execution(self):
        """Monitor execution metrics and health"""
        print("\n📊 Monitoring execution...")
        
        while True:
            try:
                # Get execution metrics
                execution_time = time.time() - self.start_time if self.start_time else 0
                
                # Display metrics
                print(f"\n📈 Execution Metrics (t={execution_time:.1f}s):")
                print(f"   Processed Batches: {self.processed_batches}")
                print(f"   Error Count: {self.error_count}")
                print(f"   Total Rows: {self.metrics.get('total_rows_processed', 0)}")
                print(f"   Trading Batches: {self.metrics.get('trading_batches', 0)}")
                print(f"   Volatility Batches: {self.metrics.get('volatility_batches', 0)}")
                print(f"   Correlation Batches: {self.metrics.get('correlation_batches', 0)}")
                
                # Calculate throughput
                if execution_time > 0:
                    throughput = self.metrics.get('total_rows_processed', 0) / execution_time
                    print(f"   Throughput: {throughput:.1f} rows/sec")
                
                # Check for errors
                if self.error_count > 0:
                    print(f"   ⚠️  Errors detected: {self.error_count}")
                
                # Wait before next check
                await asyncio.sleep(10)
                
            except Exception as e:
                print(f"❌ Monitoring error: {e}")
                break
                
    async def run_demo(self):
        """Run the complete streaming SQL demonstration"""
        try:
            print("🚀 Starting Streaming SQL Complete Example")
            print("=" * 60)
            
            # Initialize components
            await self.initialize_executor()
            await self.setup_dimension_tables()
            await self.setup_streaming_sources()
            
            # Start monitoring task
            monitoring_task = asyncio.create_task(self.monitor_execution())
            
            # Execute streaming queries
            await self.execute_streaming_queries()
            
            # Cancel monitoring task
            monitoring_task.cancel()
            
            # Final summary
            execution_time = time.time() - self.start_time if self.start_time else 0
            print(f"\n✅ Demo completed successfully!")
            print(f"   Total execution time: {execution_time:.1f} seconds")
            print(f"   Processed batches: {self.processed_batches}")
            print(f"   Total rows: {self.metrics.get('total_rows_processed', 0)}")
            print(f"   Errors: {self.error_count}")
            
            if execution_time > 0:
                throughput = self.metrics.get('total_rows_processed', 0) / execution_time
                print(f"   Average throughput: {throughput:.1f} rows/sec")
            
        except Exception as e:
            print(f"❌ Demo failed: {e}")
            self.error_count += 1
            
        finally:
            # Cleanup
            if self.executor:
                try:
                    await self.executor.shutdown()
                    print("🧹 Executor shutdown completed")
                except Exception as e:
                    print(f"⚠️  Shutdown warning: {e}")


async def main():
    """Main entry point"""
    demo = StreamingSQLDemo()
    await demo.run_demo()


if __name__ == "__main__":
    asyncio.run(main())
