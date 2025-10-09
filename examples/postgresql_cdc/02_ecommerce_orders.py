#!/usr/bin/env python3
"""
02: E-commerce Order Processing with CDC.

Demonstrates real-world e-commerce order processing:
- Order state machine (pending → paid → shipped → delivered)
- Inventory management
- Real-time metrics
- Order fulfillment pipeline

Setup:
    1. Start PostgreSQL:
       docker compose up -d postgres

    2. Initialize CDC:
       docker compose exec postgres psql -U sabot -d sabot -f /docker-entrypoint-initdb.d/01_init_cdc.sql

    3. Create schema:
       python examples/postgresql_cdc/02_ecommerce_orders.py --setup

    4. Run processor:
       python examples/postgresql_cdc/02_ecommerce_orders.py

    5. Simulate orders (in another terminal):
       python examples/postgresql_cdc/02_ecommerce_orders.py --simulate
"""

import asyncio
import logging
import sys
from pathlib import Path
import json
from datetime import datetime
from collections import defaultdict
from enum import Enum

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from sabot._cython.connectors.postgresql import (
    PostgreSQLCDCConnector,
    PostgreSQLCDCConfig
)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class OrderStatus(Enum):
    """Order status states."""
    PENDING = "pending"
    PAID = "paid"
    SHIPPED = "shipped"
    DELIVERED = "delivered"
    CANCELLED = "cancelled"


class OrderProcessor:
    """Process order CDC events."""

    def __init__(self):
        self.orders = {}  # order_id -> order_data
        self.metrics = {
            'total_orders': 0,
            'total_revenue': 0.0,
            'orders_by_status': defaultdict(int),
            'avg_order_value': 0.0,
        }

    async def process_order_event(self, event_type: str, data: dict, old_data: dict = None):
        """
        Process order event.

        Args:
            event_type: 'insert', 'update', or 'delete'
            data: Current order data
            old_data: Previous order data (for updates)
        """
        order_id = data.get('id')

        if event_type == 'insert':
            # New order created
            await self.handle_new_order(order_id, data)

        elif event_type == 'update':
            # Order updated
            await self.handle_order_update(order_id, data, old_data)

        elif event_type == 'delete':
            # Order cancelled/deleted
            await self.handle_order_cancelled(order_id, data)

    async def handle_new_order(self, order_id: int, data: dict):
        """Handle new order creation."""
        self.orders[order_id] = data
        self.metrics['total_orders'] += 1

        amount = float(data.get('total_amount', 0))
        self.metrics['total_revenue'] += amount
        self.metrics['avg_order_value'] = self.metrics['total_revenue'] / self.metrics['total_orders']

        status = data.get('status', 'pending')
        self.metrics['orders_by_status'][status] += 1

        logger.info(f"\n🆕 NEW ORDER #{order_id}")
        logger.info(f"   Customer: {data.get('customer_id')}")
        logger.info(f"   Amount: ${amount:.2f}")
        logger.info(f"   Status: {status}")
        logger.info(f"\n📊 Metrics:")
        logger.info(f"   Total Orders: {self.metrics['total_orders']}")
        logger.info(f"   Total Revenue: ${self.metrics['total_revenue']:.2f}")
        logger.info(f"   Avg Order Value: ${self.metrics['avg_order_value']:.2f}")

        # Trigger order fulfillment workflow
        await self.trigger_fulfillment(order_id, data)

    async def handle_order_update(self, order_id: int, data: dict, old_data: dict):
        """Handle order status change."""
        old_status = old_data.get('status') if old_data else None
        new_status = data.get('status')

        if old_status != new_status:
            # Status changed
            if old_status:
                self.metrics['orders_by_status'][old_status] -= 1
            self.metrics['orders_by_status'][new_status] += 1

            logger.info(f"\n🔄 ORDER #{order_id} STATUS CHANGED")
            logger.info(f"   {old_status} → {new_status}")

            # Handle state transitions
            if new_status == OrderStatus.PAID.value:
                await self.process_payment(order_id, data)
            elif new_status == OrderStatus.SHIPPED.value:
                await self.ship_order(order_id, data)
            elif new_status == OrderStatus.DELIVERED.value:
                await self.complete_order(order_id, data)

            # Print status distribution
            logger.info(f"\n📊 Order Status Distribution:")
            for status, count in self.metrics['orders_by_status'].items():
                logger.info(f"   {status}: {count}")

        # Update cache
        self.orders[order_id] = data

    async def handle_order_cancelled(self, order_id: int, data: dict):
        """Handle order cancellation."""
        logger.info(f"\n❌ ORDER #{order_id} CANCELLED")

        # Update metrics
        if order_id in self.orders:
            old_data = self.orders[order_id]
            amount = float(old_data.get('total_amount', 0))
            self.metrics['total_revenue'] -= amount
            self.metrics['total_orders'] -= 1

            if self.metrics['total_orders'] > 0:
                self.metrics['avg_order_value'] = self.metrics['total_revenue'] / self.metrics['total_orders']

            status = old_data.get('status')
            self.metrics['orders_by_status'][status] -= 1

            del self.orders[order_id]

        # Refund and restore inventory
        await self.process_refund(order_id, data)

    async def trigger_fulfillment(self, order_id: int, data: dict):
        """Trigger order fulfillment workflow."""
        logger.info(f"   ✅ Fulfillment workflow triggered for order #{order_id}")
        # In production: Send to fulfillment service, update inventory, etc.

    async def process_payment(self, order_id: int, data: dict):
        """Process order payment."""
        amount = float(data.get('total_amount', 0))
        logger.info(f"   💰 Payment processed: ${amount:.2f}")
        # In production: Update payment gateway, send receipt, etc.

    async def ship_order(self, order_id: int, data: dict):
        """Ship order."""
        logger.info(f"   📦 Order shipped - tracking will be generated")
        # In production: Generate tracking number, notify customer, etc.

    async def complete_order(self, order_id: int, data: dict):
        """Complete order delivery."""
        logger.info(f"   ✅ Order delivered successfully")
        # In production: Request review, update loyalty points, etc.

    async def process_refund(self, order_id: int, data: dict):
        """Process order refund."""
        amount = float(data.get('total_amount', 0))
        logger.info(f"   💸 Refund processed: ${amount:.2f}")
        # In production: Process refund, restore inventory, etc.


async def run_processor():
    """Run the order processor."""
    logger.info("\n" + "="*70)
    logger.info("E-commerce Order Processing CDC")
    logger.info("="*70)

    processor = OrderProcessor()

    config = PostgreSQLCDCConfig(
        host="localhost",
        port=5433,
        user="sabot",
        password="sabot",
        database="sabot",
        slot_name="sabot_orders_slot",
        publication_name="sabot_cdc",
        tables=["public.orders"],
        batch_size=10,
        max_poll_interval=2.0,
    )

    connector = PostgreSQLCDCConnector(config)

    logger.info("\n📡 Listening for order events...\n")

    try:
        async with connector:
            async for batch in connector.stream_batches():
                batch_dict = batch.to_pydict()

                for i in range(batch.num_rows):
                    event_type = batch_dict['event_type'][i]
                    data_json = batch_dict['data'][i]
                    old_data_json = batch_dict['old_data'][i]

                    data = json.loads(data_json)
                    old_data = json.loads(old_data_json) if old_data_json else {}

                    await processor.process_order_event(event_type, data, old_data)

    except KeyboardInterrupt:
        logger.info("\n\n⏹️  Stopping processor...")
    finally:
        logger.info(f"\n{'='*70}")
        logger.info("Final Metrics")
        logger.info(f"{'='*70}")
        logger.info(f"Total Orders: {processor.metrics['total_orders']}")
        logger.info(f"Total Revenue: ${processor.metrics['total_revenue']:.2f}")
        logger.info(f"Avg Order Value: ${processor.metrics['avg_order_value']:.2f}")
        logger.info(f"{'='*70}\n")


def setup_schema():
    """Setup database schema."""
    import psycopg2

    logger.info("Setting up e-commerce schema...")

    conn = psycopg2.connect(
        host="localhost",
        port=5433,
        user="sabot",
        password="sabot",
        database="sabot"
    )

    try:
        with conn.cursor() as cursor:
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS orders (
                    id SERIAL PRIMARY KEY,
                    customer_id INT NOT NULL,
                    total_amount DECIMAL(10,2) NOT NULL,
                    status VARCHAR(20) DEFAULT 'pending',
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );

                CREATE INDEX IF NOT EXISTS idx_customer ON orders(customer_id);
                CREATE INDEX IF NOT EXISTS idx_status ON orders(status);
            """)
            conn.commit()
            logger.info("✅ Schema created")
    finally:
        conn.close()


def simulate_orders():
    """Simulate order creation and updates."""
    import psycopg2
    import time
    import random

    logger.info("Simulating orders...")

    conn = psycopg2.connect(
        host="localhost",
        port=5433,
        user="sabot",
        password="sabot",
        database="sabot"
    )

    try:
        with conn.cursor() as cursor:
            # Create orders
            for i in range(5):
                customer_id = random.randint(1, 100)
                amount = random.uniform(10, 500)

                cursor.execute("""
                    INSERT INTO orders (customer_id, total_amount, status)
                    VALUES (%s, %s, 'pending')
                """, (customer_id, amount))
                conn.commit()

                logger.info(f"Created order for customer {customer_id}: ${amount:.2f}")
                time.sleep(1)

            # Update order statuses
            cursor.execute("SELECT id FROM orders WHERE status = 'pending' LIMIT 3")
            orders = cursor.fetchall()

            for (order_id,) in orders:
                for status in ['paid', 'shipped', 'delivered']:
                    cursor.execute("""
                        UPDATE orders SET status = %s, updated_at = CURRENT_TIMESTAMP
                        WHERE id = %s
                    """, (status, order_id))
                    conn.commit()
                    logger.info(f"Order {order_id} → {status}")
                    time.sleep(1)

            logger.info("✅ Simulation complete")

    finally:
        conn.close()


async def main():
    """Main entry point."""
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument('--setup', action='store_true', help='Setup database schema')
    parser.add_argument('--simulate', action='store_true', help='Simulate orders')
    args = parser.parse_args()

    if args.setup:
        setup_schema()
    elif args.simulate:
        simulate_orders()
    else:
        await run_processor()


if __name__ == "__main__":
    if '--simulate' in sys.argv or '--setup' in sys.argv:
        import argparse
        parser = argparse.ArgumentParser()
        parser.add_argument('--setup', action='store_true')
        parser.add_argument('--simulate', action='store_true')
        args = parser.parse_args()

        if args.setup:
            setup_schema()
        elif args.simulate:
            simulate_orders()
    else:
        asyncio.run(main())
