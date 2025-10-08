#!/usr/bin/env python3
"""
Stateful Processing Example - Running Totals
=============================================

**What this demonstrates:**
- Stateful stream processing (maintaining state across batches)
- Using MemoryBackend for state storage
- Running aggregations (cumulative sum, count)
- Per-key state management
- Local execution (no distributed agents)

**Prerequisites:** Completed window_aggregation.py

**Runtime:** ~5 seconds

**Next steps:**
- See 02_optimization/ for automatic optimization
- See 04_production_patterns/fraud_detection/ for stateful pattern matching

**Pattern:**
Stream → Update State → Emit Results

"""

import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

import pyarrow as pa
import pyarrow.compute as pc
from typing import Iterator, Dict
import time

# Import Sabot state backend
from sabot._cython.state.memory_backend import MemoryBackend


def transaction_stream(num_batches: int = 10, batch_size: int = 10) -> Iterator[pa.Table]:
    """
    Generate transaction batches.

    Each transaction has:
    - account_id: Account identifier
    - amount: Transaction amount
    - timestamp: Event time
    """
    print(f"\n📡 Transaction Stream Generator")
    print(f"   Batches: {num_batches}, Batch size: {batch_size}")
    print("=" * 50)

    for batch_num in range(num_batches):
        # Generate batch with 3 accounts
        account_ids = []
        amounts = []
        timestamps = []

        for i in range(batch_size):
            account_id = f"ACC_{i % 3}"  # 3 accounts: ACC_0, ACC_1, ACC_2
            amount = float((batch_num * batch_size + i) * 10)
            timestamp = time.time() + i * 0.001

            account_ids.append(account_id)
            amounts.append(amount)
            timestamps.append(timestamp)

        batch = pa.table({
            'account_id': account_ids,
            'amount': amounts,
            'timestamp': timestamps
        })

        print(f"Batch {batch_num + 1}/{num_batches}: {batch.num_rows} transactions")

        yield batch

        time.sleep(0.1)


class StatefulAggregator:
    """
    Stateful aggregator using MemoryBackend.

    Maintains running totals per account:
    - Total amount
    - Transaction count
    """

    def __init__(self):
        """Initialize with MemoryBackend."""
        self.state_backend = MemoryBackend()

        # State keys
        self.TOTAL_KEY = "total_amount"
        self.COUNT_KEY = "txn_count"

    def process_batch(self, batch: pa.Table) -> Dict[str, dict]:
        """
        Process batch and update state.

        Returns current state for all accounts in batch.
        """
        results = {}

        # Process each transaction in batch
        for i in range(batch.num_rows):
            account_id = batch['account_id'][i].as_py()
            amount = batch['amount'][i].as_py()

            # Get current state
            total_key = f"{account_id}:{self.TOTAL_KEY}"
            count_key = f"{account_id}:{self.COUNT_KEY}"

            current_total = self.state_backend.get(total_key) or 0.0
            current_count = self.state_backend.get(count_key) or 0

            # Update state
            new_total = current_total + amount
            new_count = current_count + 1

            self.state_backend.put(total_key, new_total)
            self.state_backend.put(count_key, new_count)

            # Compute average
            avg_amount = new_total / new_count

            # Store result
            results[account_id] = {
                'account_id': account_id,
                'total_amount': new_total,
                'txn_count': new_count,
                'avg_amount': avg_amount
            }

        return results

    def get_all_state(self) -> Dict[str, dict]:
        """Get state for all accounts."""
        # Get all keys
        all_keys = [k for k in self.state_backend._store.keys()]

        # Group by account
        accounts = {}
        for key in all_keys:
            if ':' in key:
                account_id, metric = key.rsplit(':', 1)
                if account_id not in accounts:
                    accounts[account_id] = {'account_id': account_id}

                accounts[account_id][metric] = self.state_backend.get(key)

        # Compute averages
        for account_id, state in accounts.items():
            if 'total_amount' in state and 'txn_count' in state:
                state['avg_amount'] = state['total_amount'] / state['txn_count']

        return accounts


def main():
    print("📊 Stateful Processing Example")
    print("=" * 50)
    print("\nKey insight: State persists across batches")
    print("Each account has running total, count, average")

    # Configuration
    num_batches = 10
    batch_size = 10

    print(f"\nConfiguration:")
    print(f"  Batches: {num_batches}")
    print(f"  Batch size: {batch_size}")
    print(f"  Total transactions: {num_batches * batch_size}")
    print(f"  Accounts: 3 (ACC_0, ACC_1, ACC_2)")

    # Create stateful aggregator
    aggregator = StatefulAggregator()

    # Process stream
    print("\n\n⚙️  Processing Transaction Stream...")
    print("=" * 50)

    for batch_num, batch in enumerate(transaction_stream(num_batches, batch_size), 1):
        # Process batch
        results = aggregator.process_batch(batch)

        # Print batch results (every 2 batches)
        if batch_num % 2 == 0:
            print(f"\nBatch {batch_num} - Current State:")
            for account_id, state in sorted(results.items()):
                print(f"  {account_id}: "
                      f"Count={state['txn_count']}, "
                      f"Total=${state['total_amount']:.2f}, "
                      f"Avg=${state['avg_amount']:.2f}")

    # Final summary
    print("\n\n📊 Final State Summary")
    print("=" * 50)

    final_state = aggregator.get_all_state()

    print(f"{'Account':<12} {'Txn Count':<12} {'Total Amount':<15} {'Avg Amount':<15}")
    print("-" * 65)

    for account_id, state in sorted(final_state.items()):
        print(f"{account_id:<12} {state['txn_count']:<12} "
              f"${state['total_amount']:<14.2f} ${state['avg_amount']:<14.2f}")

    # Verify totals
    total_txns = sum(s['txn_count'] for s in final_state.values())
    total_amount = sum(s['total_amount'] for s in final_state.values())

    print("\n" + "=" * 65)
    print(f"{'TOTAL':<12} {total_txns:<12} ${total_amount:<14.2f}")

    print("\n\n💡 Key Takeaways:")
    print("=" * 50)
    print("1. Stateful processing maintains state across batches")
    print("2. MemoryBackend stores key-value state in-memory")
    print("3. State is per-key (per account in this example)")
    print("4. Running aggregations: total, count, average")
    print("5. State survives batch boundaries")

    print("\n\n🔗 State Backend Options:")
    print("=" * 50)
    print("MemoryBackend:   Fast, volatile, <1GB state")
    print("RocksDBBackend:  Persistent, large state (>100GB)")
    print("RedisBackend:    Distributed, shared state")

    print("\n\n🔗 Next Steps:")
    print("=" * 50)
    print("- See 02_optimization/ for automatic optimization")
    print("- See 04_production_patterns/fraud_detection/ for stateful patterns")
    print("- See docs/USER_WORKFLOW.md for state backends")


if __name__ == "__main__":
    main()
