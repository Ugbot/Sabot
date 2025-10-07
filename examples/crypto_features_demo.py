#!/usr/bin/env python3
"""
Crypto Feature Engineering Demo

Demonstrates real-time feature engineering pipeline using Sabot:
1. Coinbase ticker data → Kafka
2. Feature extraction (rolling stats, percentiles)
3. Feature storage in CyRedis

Features computed:
- price_rolling_mean_5m: 5-minute rolling average price
- volume_std_1h: 1-hour volume standard deviation
- spread_percentile_95: 95th percentile bid-ask spread

Usage:
    # Start Coinbase producer (in separate terminal):
    python examples/coinbase2parquet.py -k

    # Run feature pipeline:
    python examples/crypto_features_demo.py
"""

import asyncio
import time
from collections import defaultdict
import json

# Sabot imports
from sabot import Stream
from sabot.features import FeatureStore


async def run_feature_pipeline():
    """
    Feature engineering pipeline using standard Sabot operators.
    """
    print("🚀 Starting Crypto Feature Engineering Demo")
    print("=" * 60)

    # Initialize feature store
    feature_store = FeatureStore(redis_url="localhost:6379", db=0)
    await feature_store.initialize()
    print("✅ Feature store initialized (CyRedis @ localhost:6379)")

    # Create stream from Kafka
    print("\n📊 Connecting to Kafka (coinbase-ticker topic)...")

    # Use aiokafka directly for this demo (Stream API integration pending)
    from aiokafka import AIOKafkaConsumer

    consumer = AIOKafkaConsumer(
        "coinbase-ticker",
        bootstrap_servers="localhost:19092",
        group_id="crypto-features",
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        auto_offset_reset='latest'
    )

    await consumer.start()
    print("✅ Connected to Kafka")

    # Stats tracking
    stats = {
        "messages_processed": 0,
        "features_computed": 0,
        "features_written": 0,
        "errors": 0,
        "start_time": time.time(),
        "last_print": time.time()
    }

    # Simple feature computation (demonstration)
    # In production, would use .with_features() with Cython extractors
    feature_buffers = defaultdict(lambda: {"prices": [], "volumes": [], "spreads": []})

    print("\n🎯 Feature Pipeline Active")
    print("-" * 60)
    print("Features being computed:")
    print("  • price_rolling_mean_5m (5-min rolling avg)")
    print("  • volume_std_1h (1-hr std dev)")
    print("  • spread_percentile_95 (95th percentile)")
    print("-" * 60)

    try:
        async for msg in consumer:
            data = msg.value
            stats["messages_processed"] += 1

            # Extract fields
            product_id = data.get("product_id", "UNKNOWN")
            price = data.get("price", 0.0)
            volume_24h = data.get("volume_24h", 0.0)
            best_bid = data.get("best_bid", 0.0)
            best_ask = data.get("best_ask", 0.0)

            # Skip invalid data
            if price == 0 or product_id == "UNKNOWN":
                continue

            # Calculate spread
            spread = best_ask - best_bid if best_bid > 0 and best_ask > 0 else 0.0

            # Update feature buffers (simple rolling window)
            buffer = feature_buffers[product_id]
            buffer["prices"].append(price)
            buffer["volumes"].append(volume_24h)
            buffer["spreads"].append(spread)

            # Keep last 300 values (5 min window at 1 msg/sec)
            if len(buffer["prices"]) > 300:
                buffer["prices"] = buffer["prices"][-300:]
                buffer["volumes"] = buffer["volumes"][-300:]
                buffer["spreads"] = buffer["spreads"][-300:]

            # Compute features if we have enough data
            if len(buffer["prices"]) >= 10:
                import statistics

                # Feature 1: Rolling mean price (5 min)
                price_ma_5m = statistics.mean(buffer["prices"][-60:])

                # Feature 2: Volume std dev (1 hr window)
                volume_std_1h = statistics.stdev(buffer["volumes"]) if len(buffer["volumes"]) > 1 else 0.0

                # Feature 3: Spread 95th percentile
                sorted_spreads = sorted(buffer["spreads"])
                idx_95 = int(len(sorted_spreads) * 0.95)
                spread_p95 = sorted_spreads[idx_95] if sorted_spreads else 0.0

                # Write features to Redis
                try:
                    await feature_store.set_features(
                        entity_id=product_id,
                        features={
                            "price_rolling_mean_5m": price_ma_5m,
                            "volume_std_1h": volume_std_1h,
                            "spread_percentile_95": spread_p95
                        },
                        ttl=300  # 5 minute expiration
                    )

                    stats["features_computed"] += 3
                    stats["features_written"] += 3

                except Exception as e:
                    print(f"❌ Feature write error: {e}")
                    stats["errors"] += 1

            # Print stats every 5 seconds
            now = time.time()
            if now - stats["last_print"] >= 5:
                elapsed = now - stats["start_time"]
                throughput = stats["messages_processed"] / elapsed if elapsed > 0 else 0

                print(f"\n📈 Pipeline Stats (t={int(elapsed)}s)")
                print(f"   Messages:  {stats['messages_processed']:,}")
                print(f"   Features:  {stats['features_written']:,}")
                print(f"   Throughput: {throughput:.1f} msgs/sec")
                print(f"   Errors:    {stats['errors']}")

                # Show sample features for a symbol
                if product_id != "UNKNOWN":
                    try:
                        features = await feature_store.get_features(
                            entity_id=product_id,
                            feature_names=[
                                "price_rolling_mean_5m",
                                "volume_std_1h",
                                "spread_percentile_95"
                            ]
                        )
                        print(f"\n   {product_id} Features:")
                        for fname, fval in features.items():
                            if fval is not None:
                                print(f"     {fname}: {fval:.6f}")
                    except Exception as e:
                        print(f"   Feature retrieval error: {e}")

                stats["last_print"] = now

    except KeyboardInterrupt:
        print("\n\n⏸️  Pipeline stopped by user")

    finally:
        await consumer.stop()

        # Final stats
        elapsed = time.time() - stats["start_time"]
        print("\n" + "=" * 60)
        print("📊 Final Statistics")
        print("=" * 60)
        print(f"Runtime:          {elapsed:.1f}s")
        print(f"Messages:         {stats['messages_processed']:,}")
        print(f"Features written: {stats['features_written']:,}")
        print(f"Errors:           {stats['errors']}")
        print(f"Avg throughput:   {stats['messages_processed'] / elapsed:.1f} msgs/sec")
        print("=" * 60)


if __name__ == "__main__":
    print(__doc__)
    asyncio.run(run_feature_pipeline())
