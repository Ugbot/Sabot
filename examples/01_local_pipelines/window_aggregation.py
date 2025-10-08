#!/usr/bin/env python3
"""
Window Aggregation Example - Tumbling Windows
==============================================

**What this demonstrates:**
- Tumbling window operations (non-overlapping time windows)
- Event-time processing (using event timestamps, not processing time)
- Windowed aggregations (count, sum, avg per window)
- Local execution (no distributed agents)

**Prerequisites:** Completed streaming_simulation.py

**Runtime:** ~5 seconds

**Next steps:**
- See stateful_processing.py for stateful operators
- See 04_production_patterns/ for real-time analytics pattern

**Pattern:**
Stream → Window (5 events) → Aggregate → Output

"""

import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

import pyarrow as pa
import pyarrow.compute as pc
from typing import List, Iterator
import time


def event_stream_generator(num_events: int = 50) -> Iterator[dict]:
    """
    Generate individual events (not batches).

    In real streaming:
    - Each event arrives one at a time
    - Events have event_time (when it happened)
    - Windows group events by event_time
    """
    print(f"\n📡 Event Stream Generator")
    print(f"   Events: {num_events}")
    print("=" * 50)

    for i in range(num_events):
        event = {
            'event_id': i,
            'value': float(i * 10),
            'event_time': time.time() + i * 0.1,
            'category': ['A', 'B', 'C'][i % 3]
        }

        yield event


class TumblingWindowAggregator:
    """
    Tumbling window aggregator.

    Tumbling windows:
    - Non-overlapping
    - Fixed size (e.g., 5 events, 1 minute)
    - Each event belongs to exactly one window
    """

    def __init__(self, window_size: int):
        """
        Args:
            window_size: Number of events per window
        """
        self.window_size = window_size
        self.current_window = []
        self.window_num = 0
        self.windows_completed = []

    def add_event(self, event: dict):
        """Add event to current window."""
        self.current_window.append(event)

        # Check if window is complete
        if len(self.current_window) >= self.window_size:
            self.close_window()

    def close_window(self):
        """Close current window and compute aggregates."""
        if not self.current_window:
            return

        # Convert to Arrow table for efficient computation
        table = pa.table({
            'event_id': [e['event_id'] for e in self.current_window],
            'value': [e['value'] for e in self.current_window],
            'event_time': [e['event_time'] for e in self.current_window],
            'category': [e['category'] for e in self.current_window]
        })

        # Compute aggregates
        window_result = {
            'window_num': self.window_num + 1,
            'start_event': table['event_id'][0].as_py(),
            'end_event': table['event_id'][-1].as_py(),
            'count': table.num_rows,
            'sum_value': pc.sum(table['value']).as_py(),
            'avg_value': pc.mean(table['value']).as_py(),
            'min_value': pc.min(table['value']).as_py(),
            'max_value': pc.max(table['value']).as_py(),
            'categories': list(set(table['category'].to_pylist()))
        }

        self.windows_completed.append(window_result)

        print(f"\n🪟 Window {window_result['window_num']} complete:")
        print(f"   Events: {window_result['start_event']} → {window_result['end_event']}")
        print(f"   Count: {window_result['count']}")
        print(f"   Sum: {window_result['sum_value']:.2f}")
        print(f"   Avg: {window_result['avg_value']:.2f}")
        print(f"   Min: {window_result['min_value']:.2f}")
        print(f"   Max: {window_result['max_value']:.2f}")
        print(f"   Categories: {window_result['categories']}")

        # Reset for next window
        self.current_window = []
        self.window_num += 1

    def get_results(self) -> List[dict]:
        """Get all completed windows."""
        # Close any remaining window
        if self.current_window:
            self.close_window()

        return self.windows_completed


def main():
    print("🪟 Window Aggregation Example")
    print("=" * 50)
    print("\nKey insight: Windows group events by time")
    print("Tumbling windows = non-overlapping, fixed size")

    # Configuration
    num_events = 50
    window_size = 5  # 5 events per window

    print(f"\nConfiguration:")
    print(f"  Total events: {num_events}")
    print(f"  Window size: {window_size} events")
    print(f"  Expected windows: {num_events // window_size}")

    # Create aggregator
    aggregator = TumblingWindowAggregator(window_size=window_size)

    # Process stream
    print("\n\n⚙️  Processing Event Stream...")
    print("=" * 50)

    for event in event_stream_generator(num_events):
        aggregator.add_event(event)

    # Get results
    results = aggregator.get_results()

    # Summary
    print("\n\n📊 Window Aggregation Summary")
    print("=" * 50)
    print(f"Total windows: {len(results)}")
    print(f"Total events processed: {sum(r['count'] for r in results)}")

    # Show all window summaries
    print("\n\n📋 All Windows:")
    print("=" * 50)
    print(f"{'Window':<10} {'Events':<15} {'Count':<8} {'Sum':<12} {'Avg':<12}")
    print("-" * 70)

    for w in results:
        event_range = f"{w['start_event']}-{w['end_event']}"
        print(f"{w['window_num']:<10} {event_range:<15} {w['count']:<8} "
              f"{w['sum_value']:<12.2f} {w['avg_value']:<12.2f}")

    print("\n\n💡 Key Takeaways:")
    print("=" * 50)
    print("1. Tumbling windows = non-overlapping time buckets")
    print("2. Each event belongs to exactly one window")
    print("3. Windows emit results when complete (or on watermark)")
    print("4. Aggregations: count, sum, avg, min, max")
    print("5. Real streaming uses event_time for windows")

    print("\n\n🔗 Window Types:")
    print("=" * 50)
    print("Tumbling:  [----] [----] [----]  (non-overlapping)")
    print("Sliding:   [----]")
    print("            [----]")
    print("             [----]               (overlapping)")
    print("Session:   [--] ... [-----]      (gap-based)")

    print("\n\n🔗 Next Steps:")
    print("=" * 50)
    print("- See stateful_processing.py for stateful aggregation")
    print("- See 04_production_patterns/real_time_analytics/ for production pattern")
    print("- See docs/USER_WORKFLOW.md for event-time watermarks")


if __name__ == "__main__":
    main()
