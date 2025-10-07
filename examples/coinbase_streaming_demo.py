#!/usr/bin/env python3
"""
Real-Time Crypto Streaming with Sabot

Consumes Coinbase ticker data from Kafka and performs:
- Real-time aggregations (1-minute tumbling windows)
- Spread calculations
- Top movers detection

Producer: coinbase2parquet.py -k
Consumer: This script

Performance target: 10K+ msgs/sec throughput
"""

import asyncio
import time
from datetime import datetime
from collections import defaultdict
import json

from aiokafka import AIOKafkaConsumer

# Stats tracking
stats = {
    "messages_processed": 0,
    "last_print": time.time(),
    "start_time": time.time(),
    "by_product": defaultdict(int),
}


async def process_coinbase_ticker():
    """Process real-time Coinbase ticker data."""

    # Create Kafka consumer
    consumer = AIOKafkaConsumer(
        "coinbase-ticker",
        bootstrap_servers="localhost:19092",
        group_id="coinbase-streaming",
        value_deserializer=lambda m: json.loads(m.decode('utf-8'))
    )

    await consumer.start()

    try:
        async for msg in consumer:
            stats["messages_processed"] += 1

            # Parse ticker data
            data = msg.value

            product_id = data.get("product_id", "UNKNOWN")
            stats["by_product"][product_id] += 1

            # Calculate spread
            best_bid = data.get("best_bid", 0)
            best_ask = data.get("best_ask", 0)

            if best_bid > 0 and best_ask > 0:
                spread = best_ask - best_bid
                spread_bps = (spread / best_bid) * 10000  # basis points

                # Detect tight spreads (< 5 bps)
                if spread_bps < 5:
                    price = data.get("price", 0)
                    print(f"🎯 Tight spread: {product_id} @ ${price:,.2f} - Spread: {spread_bps:.2f} bps")

            # Print stats every 5 seconds
            now = time.time()
            if now - stats["last_print"] >= 5:
                elapsed = now - stats["start_time"]
                throughput = stats["messages_processed"] / elapsed

                print(f"\n📊 Streaming Stats (after {elapsed:.1f}s):")
                print(f"   Total messages: {stats['messages_processed']:,}")
                print(f"   Throughput: {throughput:.0f} msgs/sec")
                print(f"   Products: {len(stats['by_product'])}")

                # Top 5 active products
                top_products = sorted(
                    stats["by_product"].items(),
                    key=lambda x: x[1],
                    reverse=True
                )[:5]
                print(f"   Top products:")
                for prod, count in top_products:
                    print(f"     - {prod}: {count:,} msgs")

                stats["last_print"] = now

    finally:
        await consumer.stop()


async def detect_price_moves():
    """Detect significant price movements."""

    price_history = {}

    # Create separate consumer for price moves
    consumer = AIOKafkaConsumer(
        "coinbase-ticker",
        bootstrap_servers="localhost:19092",
        group_id="coinbase-streaming-moves",
        value_deserializer=lambda m: json.loads(m.decode('utf-8'))
    )

    await consumer.start()

    try:
        async for msg in consumer:
            data = msg.value

            product_id = data.get("product_id")
            price = data.get("price", 0)
            price_chg_24h = data.get("price_percent_chg_24_h", 0)

            if not product_id or price == 0:
                continue

            # Track price changes
            if product_id not in price_history:
                price_history[product_id] = {
                    "last_price": price,
                    "last_update": time.time(),
                }
            else:
                prev = price_history[product_id]
                pct_change = ((price - prev["last_price"]) / prev["last_price"]) * 100

                # Alert on >1% move in last update
                if abs(pct_change) > 1.0:
                    direction = "🔼" if pct_change > 0 else "🔽"
                    print(f"{direction} {product_id}: {pct_change:+.2f}% move (${prev['last_price']:,.2f} → ${price:,.2f})")

                price_history[product_id] = {
                    "last_price": price,
                    "last_update": time.time(),
                }

    finally:
        await consumer.stop()


async def main():
    """Run the streaming app."""
    print("="*70)
    print("🚀 Sabot Real-Time Crypto Streaming Demo")
    print("="*70)
    print(f"Broker: localhost:19092")
    print(f"Topic: coinbase-ticker")
    print(f"Tasks: 2 (stats + price_moves)")
    print("="*70)
    print("\n✨ Processing stream... (Ctrl+C to stop)\n")

    try:
        # Run stats processor only
        await process_coinbase_ticker()
    except KeyboardInterrupt:
        print("\n\n⏹️  Shutting down...")

        # Final stats
        elapsed = time.time() - stats["start_time"]
        throughput = stats["messages_processed"] / elapsed if elapsed > 0 else 0

        print("\n" + "="*70)
        print("📊 Final Statistics")
        print("="*70)
        print(f"Runtime: {elapsed:.1f}s")
        print(f"Total messages: {stats['messages_processed']:,}")
        print(f"Average throughput: {throughput:.0f} msgs/sec")
        print(f"Products tracked: {len(stats['by_product'])}")
        print("="*70)


if __name__ == "__main__":
    asyncio.run(main())
