# -*- coding: utf-8 -*-
"""Parallel streaming operators using Morsel-Driven Parallelism."""

import asyncio
from typing import Any, Callable, List, Optional, TypeVar
from .morsel_parallelism import (
    ParallelProcessor, FanOutOperator, FanInOperator,
    Morsel, create_parallel_processor
)

T = TypeVar('T')

class ParallelStreamProcessor:
    """Streaming processor with built-in parallel processing capabilities."""

    def __init__(self, num_workers: Optional[int] = None, morsel_size_kb: int = 64):
        self.processor = create_parallel_processor(num_workers, morsel_size_kb)
        self._running = False

    async def start(self):
        """Start the parallel processor."""
        if not self._running:
            await self.processor.start()
            self._running = True

    async def stop(self):
        """Stop the parallel processor."""
        if self._running:
            await self.processor.stop()
            self._running = False

    async def process_stream(
        self,
        input_stream: asyncio.Queue,
        processor_func: Callable[[Morsel], Any],
        output_stream: Optional[asyncio.Queue] = None
    ) -> asyncio.Queue:
        """Process a stream using morsel-driven parallelism."""

        if output_stream is None:
            output_stream = asyncio.Queue()

        async def stream_processor():
            """Process items from input stream."""
            while True:
                try:
                    # Get item from input stream
                    item = await input_stream.get()

                    # Process item using parallel morsel processing
                    results = await self.processor.process_with_function(
                        item, processor_func
                    )

                    # Put results to output stream
                    for result in results:
                        if result is not None:
                            await output_stream.put(result)

                    input_stream.task_done()

                except asyncio.CancelledError:
                    break
                except Exception as e:
                    print(f"Stream processing error: {e}")

        # Start stream processor
        processor_task = asyncio.create_task(stream_processor())

        return output_stream

class ParallelGroupBy:
    """Parallel group-by operation using morsel-driven parallelism."""

    def __init__(self, key_func: Callable[[Any], Any], num_workers: Optional[int] = None):
        self.key_func = key_func
        self.processor = create_parallel_processor(num_workers)
        self._groups: dict = {}
        self._lock = asyncio.Lock()

    async def process_stream(self, input_stream: asyncio.Queue) -> asyncio.Queue:
        """Process stream with parallel grouping."""

        output_stream = asyncio.Queue()

        async def group_processor(morsel: Morsel) -> dict:
            """Process a morsel and return grouped results."""
            local_groups = {}

            # Process items in morsel
            if hasattr(morsel.data, '__iter__') and not isinstance(morsel.data, (str, bytes)):
                for item in morsel.data:
                    key = self.key_func(item)
                    if key not in local_groups:
                        local_groups[key] = []
                    local_groups[key].append(item)
            else:
                # Single item
                key = self.key_func(morsel.data)
                local_groups[key] = [morsel.data]

            return local_groups

        async def merge_groups():
            """Merge results from parallel processing."""
            while True:
                try:
                    # Get grouped results from processing
                    grouped_results = await output_stream.get()

                    # Merge into global groups
                    async with self._lock:
                        for key, items in grouped_results.items():
                            if key not in self._groups:
                                self._groups[key] = []
                            self._groups[key].extend(items)

                    output_stream.task_done()

                except asyncio.CancelledError:
                    break

        # Start parallel processing
        await self.processor.start()

        # Create processing pipeline
        processed_stream = await self.processor.process_with_function(
            input_stream, group_processor
        )

        # Start merge task
        merge_task = asyncio.create_task(merge_groups())

        # Put processed results to output stream
        for result in processed_stream:
            await output_stream.put(result)

        return output_stream

    def get_groups(self) -> dict:
        """Get current grouped results."""
        return self._groups.copy()

class ParallelJoin:
    """Parallel join operation using morsel-driven parallelism."""

    def __init__(self,
                 left_key_func: Callable[[Any], Any],
                 right_key_func: Callable[[Any], Any],
                 join_type: str = "inner",
                 num_workers: Optional[int] = None):
        self.left_key_func = left_key_func
        self.right_key_func = right_key_func
        self.join_type = join_type.lower()
        self.processor = create_parallel_processor(num_workers)

        # Build tables for join
        self.left_table = {}
        self.right_table = {}
        self._lock = asyncio.Lock()

    async def build_left_table(self, left_stream: asyncio.Queue):
        """Build left table from stream."""

        async def build_processor(morsel: Morsel) -> dict:
            """Process morsel and extract key-value pairs."""
            local_table = {}

            items = morsel.data if hasattr(morsel.data, '__iter__') and not isinstance(morsel.data, (str, bytes)) else [morsel.data]

            for item in items:
                key = self.left_key_func(item)
                if key not in local_table:
                    local_table[key] = []
                local_table[key].append(item)

            return local_table

        # Process left stream in parallel
        results = await self.processor.process_with_function(left_stream, build_processor)

        # Merge results
        async with self._lock:
            for result in results:
                for key, items in result.items():
                    if key not in self.left_table:
                        self.left_table[key] = []
                    self.left_table[key].extend(items)

    async def build_right_table(self, right_stream: asyncio.Queue):
        """Build right table from stream."""

        async def build_processor(morsel: Morsel) -> dict:
            """Process morsel and extract key-value pairs."""
            local_table = {}

            items = morsel.data if hasattr(morsel.data, '__iter__') and not isinstance(morsel.data, (str, bytes)) else [morsel.data]

            for item in items:
                key = self.right_key_func(item)
                if key not in local_table:
                    local_table[key] = []
                local_table[key].append(item)

            return local_table

        # Process right stream in parallel
        results = await self.processor.process_with_function(right_stream, build_processor)

        # Merge results
        async with self._lock:
            for result in results:
                for key, items in result.items():
                    if key not in self.right_table:
                        self.right_table[key] = []
                    self.right_table[key].extend(items)

    async def join(self, output_stream: Optional[asyncio.Queue] = None) -> asyncio.Queue:
        """Perform the join operation."""

        if output_stream is None:
            output_stream = asyncio.Queue()

        async def join_processor(morsel: Morsel) -> List[tuple]:
            """Process morsel and perform join."""
            results = []

            # Get keys from morsel (assuming it's from one of the tables)
            keys = set()
            if hasattr(morsel.data, '__iter__') and not isinstance(morsel.data, (str, bytes)):
                for item in morsel.data:
                    if item in self.left_table:
                        keys.update(self.left_table.keys())
                    elif item in self.right_table:
                        keys.update(self.right_table.keys())
                    break  # Just need keys from one item
            else:
                # Check which table this item belongs to
                if morsel.data in self.left_table:
                    keys = set(self.left_table.keys())
                elif morsel.data in self.right_table:
                    keys = set(self.right_table.keys())

            # Perform join for these keys
            for key in keys:
                left_items = self.left_table.get(key, [])
                right_items = self.right_table.get(key, [])

                if self.join_type == "inner":
                    if left_items and right_items:
                        for left in left_items:
                            for right in right_items:
                                results.append((left, right))
                elif self.join_type == "left":
                    if left_items:
                        for left in left_items:
                            if right_items:
                                for right in right_items:
                                    results.append((left, right))
                            else:
                                results.append((left, None))
                elif self.join_type == "right":
                    if right_items:
                        for right in right_items:
                            if left_items:
                                for left in left_items:
                                    results.append((left, right))
                            else:
                                results.append((None, right))

            return results

        # Create morsels from join keys
        all_keys = set(self.left_table.keys()) | set(self.right_table.keys())
        morsels = self.processor.create_morsels(list(all_keys))

        # Process joins in parallel
        join_results = await self.processor.process_with_function(morsels, join_processor)

        # Flatten and output results
        for result_list in join_results:
            for result in result_list:
                await output_stream.put(result)

        return output_stream

class FanOutFanInPipeline:
    """Complete fan-out/fan-in pipeline for stream processing."""

    def __init__(self,
                 processor_func: Callable[[Morsel], Any],
                 num_branches: int = 4,
                 num_workers: Optional[int] = None,
                 morsel_size_kb: int = 64):
        self.processor_func = processor_func
        self.num_branches = num_branches
        self.processor = create_parallel_processor(num_workers, morsel_size_kb)
        self.fan_out = FanOutOperator(num_branches, self.processor)
        self.fan_in = FanInOperator(num_branches)

    async def process_stream(self,
                           input_stream: asyncio.Queue,
                           output_stream: Optional[asyncio.Queue] = None) -> asyncio.Queue:
        """Process stream through fan-out/fan-in pipeline."""

        if output_stream is None:
            output_stream = asyncio.Queue()

        # Create fan-out branches
        branch_queues = await self.fan_out.process_stream(input_stream)

        # Process each branch in parallel
        processed_queues = []

        async def process_branch(branch_id: int, branch_queue: asyncio.Queue) -> asyncio.Queue:
            """Process a single branch."""
            processed_queue = asyncio.Queue()

            async def branch_processor():
                while True:
                    try:
                        morsel = await branch_queue.get()

                        # Apply processing function
                        result = await self.processor_func(morsel)

                        # Mark morsel as processed
                        morsel.mark_processed()

                        await processed_queue.put(result)
                        branch_queue.task_done()

                    except asyncio.CancelledError:
                        break
                    except Exception as e:
                        print(f"Branch {branch_id} processing error: {e}")

            # Start branch processor
            asyncio.create_task(branch_processor())
            return processed_queue

        # Create processing tasks for all branches
        branch_tasks = []
        for branch_id, branch_queue in enumerate(branch_queues):
            task = asyncio.create_task(process_branch(branch_id, branch_queue))
            branch_tasks.append(task)

        # Wait for all branch processing setups to complete
        processed_queues = await asyncio.gather(*branch_tasks)

        # Merge results with fan-in
        final_output = await self.fan_in.merge_streams(processed_queues)

        # Copy results to output stream
        async def copy_results():
            while True:
                try:
                    result = await final_output.get()
                    await output_stream.put(result)
                    final_output.task_done()
                except asyncio.CancelledError:
                    break

        asyncio.create_task(copy_results())

        return output_stream

# Convenience functions

async def parallel_map_stream(
    input_stream: asyncio.Queue,
    map_func: Callable[[Any], Any],
    num_workers: Optional[int] = None,
    output_stream: Optional[asyncio.Queue] = None
) -> asyncio.Queue:
    """Apply function to each item in stream using parallel processing."""

    if output_stream is None:
        output_stream = asyncio.Queue()

    processor = ParallelStreamProcessor(num_workers)

    async def morsel_mapper(morsel: Morsel) -> List[Any]:
        """Map function over morsel data."""
        results = []

        items = morsel.data if hasattr(morsel.data, '__iter__') and not isinstance(morsel.data, (str, bytes)) else [morsel.data]

        for item in items:
            try:
                result = await map_func(item)
                results.append(result)
            except Exception as e:
                print(f"Map function error: {e}")
                results.append(None)

        return results

    await processor.start()
    try:
        result_stream = await processor.process_stream(input_stream, morsel_mapper, output_stream)
        return result_stream
    finally:
        await processor.stop()

async def parallel_filter_stream(
    input_stream: asyncio.Queue,
    filter_func: Callable[[Any], bool],
    num_workers: Optional[int] = None,
    output_stream: Optional[asyncio.Queue] = None
) -> asyncio.Queue:
    """Filter stream items using parallel processing."""

    if output_stream is None:
        output_stream = asyncio.Queue()

    processor = ParallelStreamProcessor(num_workers)

    async def morsel_filter(morsel: Morsel) -> List[Any]:
        """Filter morsel data."""
        results = []

        items = morsel.data if hasattr(morsel.data, '__iter__') and not isinstance(morsel.data, (str, bytes)) else [morsel.data]

        for item in items:
            try:
                if await filter_func(item):
                    results.append(item)
            except Exception as e:
                print(f"Filter function error: {e}")

        return results

    await processor.start()
    try:
        result_stream = await processor.process_stream(input_stream, morsel_filter, output_stream)
        return result_stream
    finally:
        await processor.stop()

async def parallel_group_by_stream(
    input_stream: asyncio.Queue,
    key_func: Callable[[Any], Any],
    num_workers: Optional[int] = None
) -> ParallelGroupBy:
    """Create parallel group-by operation on stream."""

    group_by = ParallelGroupBy(key_func, num_workers)
    await group_by.processor.start()

    # Process the stream
    await group_by.process_stream(input_stream)

    return group_by
