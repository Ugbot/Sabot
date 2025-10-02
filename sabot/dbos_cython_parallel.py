# -*- coding: utf-8 -*-
"""High-level DBOS-controlled Cython parallel processing interface."""

import asyncio
import logging
from typing import Any, Callable, List, Optional, Dict, TypeVar
from .dbos_parallel_controller import DBOSParallelController, get_system_resource_summary

logger = logging.getLogger(__name__)

T = TypeVar('T')

class DBOSCythonParallelProcessor:
    """High-level interface combining DBOS control with Cython parallel execution."""

    def __init__(self,
                 max_workers: Optional[int] = None,
                 morsel_size_kb: int = 64,
                 target_utilization: float = 0.8):
        self.max_workers = max_workers
        self.morsel_size_kb = morsel_size_kb
        self.target_utilization = target_utilization

        # DBOS controller (decision making)
        self.dbos_controller = DBOSParallelController(max_workers, target_utilization)

        # Cython processor (actual execution) - lazy loaded
        self._cython_processor = None
        self._is_running = False

    async def _ensure_cython_processor(self):
        """Ensure Cython processor is initialized."""
        if self._cython_processor is None:
            try:
                from ._cython.morsel_parallelism import create_fast_parallel_processor
                # Get optimal worker count from DBOS
                optimal_workers = self.dbos_controller._calculate_optimal_worker_count(0)
                actual_workers = min(optimal_workers, self.max_workers or optimal_workers)

                self._cython_processor = create_fast_parallel_processor(
                    actual_workers, self.morsel_size_kb, self.dbos_controller
                )
                logger.info(f"Initialized Cython processor with {actual_workers} workers")

            except ImportError:
                logger.warning("Cython processor not available, falling back to Python implementation")
                from .morsel_parallelism import ParallelProcessor
                optimal_workers = self.dbos_controller._calculate_optimal_worker_count(0)
                actual_workers = min(optimal_workers, self.max_workers or optimal_workers)

                self._cython_processor = ParallelProcessor(actual_workers, self.morsel_size_kb)

    async def start(self):
        """Start the DBOS-controlled parallel processor."""
        if self._is_running:
            return

        await self._ensure_cython_processor()

        if hasattr(self._cython_processor, 'start'):
            await self._cython_processor.start()

        await self.dbos_controller.processor_started(
            getattr(self._cython_processor, '_num_workers', 1)
        )

        self._is_running = True
        logger.info("Started DBOS-controlled Cython parallel processor")

    async def stop(self):
        """Stop the parallel processor."""
        if not self._is_running:
            return

        if hasattr(self._cython_processor, 'stop'):
            await self._cython_processor.stop()

        await self.dbos_controller.processor_stopped()
        self._is_running = False
        logger.info("Stopped DBOS-controlled Cython parallel processor")

    async def process_data(self,
                          data: Any,
                          processor_func: Callable[[Any], Any],
                          partition_id: int = 0) -> List[Any]:
        """Process data using DBOS-controlled Cython parallel execution."""

        if not self._is_running:
            await self.start()

        # Use DBOS to decide how to process
        if hasattr(self._cython_processor, 'process_data'):
            # Cython path
            morsels = await self._cython_processor.process_data(data, processor_func, partition_id)
            return morsels
        else:
            # Python fallback
            results = await self._cython_processor.process_with_function(data, processor_func)
            return results

    async def process_stream(self,
                           input_stream: asyncio.Queue,
                           processor_func: Callable[[Any], Any],
                           output_stream: Optional[asyncio.Queue] = None) -> asyncio.Queue:
        """Process a stream using parallel execution."""

        if output_stream is None:
            output_stream = asyncio.Queue()

        async def stream_processor():
            while True:
                try:
                    # Get item from input stream
                    item = await input_stream.get()

                    # Process item in parallel
                    results = await self.process_data(item, processor_func)

                    # Put results to output stream
                    for result in results:
                        if result is not None:
                            await output_stream.put(result)

                    input_stream.task_done()

                except asyncio.CancelledError:
                    break
                except Exception as e:
                    logger.error(f"Stream processing error: {e}")

        # Start stream processor
        asyncio.create_task(stream_processor())
        return output_stream

    def get_stats(self) -> Dict[str, Any]:
        """Get comprehensive processing statistics."""
        stats = {
            'dbos_controller': self.dbos_controller.get_performance_stats(),
            'system_resources': get_system_resource_summary(),
        }

        if self._cython_processor:
            if hasattr(self._cython_processor, 'get_stats'):
                stats['cython_processor'] = self._cython_processor.get_stats()
            else:
                stats['python_processor'] = {
                    'type': 'python_fallback',
                    'morsel_size_kb': self.morsel_size_kb,
                }

        stats['is_running'] = self._is_running
        stats['max_workers'] = self.max_workers
        stats['target_utilization'] = self.target_utilization

        return stats

class FanOutFanInCythonPipeline:
    """DBOS-controlled fan-out/fan-in pipeline using Cython execution."""

    def __init__(self,
                 processor_func: Callable[[Any], Any],
                 num_branches: int = 4,
                 morsel_size_kb: int = 64,
                 max_workers: Optional[int] = None):
        self.processor_func = processor_func
        self.num_branches = num_branches
        self.morsel_size_kb = morsel_size_kb
        self.max_workers = max_workers

        # Create DBOS-controlled processors for each branch
        self.branch_processors = [
            DBOSCythonParallelProcessor(max_workers, morsel_size_kb)
            for _ in range(num_branches)
        ]

        self.fan_in_queue = asyncio.Queue()

    async def start(self):
        """Start all branch processors."""
        start_tasks = [processor.start() for processor in self.branch_processors]
        await asyncio.gather(*start_tasks)

    async def stop(self):
        """Stop all branch processors."""
        stop_tasks = [processor.stop() for processor in self.branch_processors]
        await asyncio.gather(*stop_tasks)

    async def process_stream(self,
                           input_stream: asyncio.Queue,
                           output_stream: Optional[asyncio.Queue] = None) -> asyncio.Queue:
        """Process stream through fan-out/fan-in pipeline."""

        if output_stream is None:
            output_stream = asyncio.Queue()

        # Start branch processors
        await self.start()

        # Fan-out: distribute work to branches
        async def fan_out_worker(branch_id: int):
            """Worker for a specific branch."""
            processor = self.branch_processors[branch_id]

            while True:
                try:
                    # Get item from input stream
                    item = await input_stream.get()

                    # Process through branch
                    branch_results = await processor.process_data(item, self.processor_func)

                    # Send to fan-in
                    for result in branch_results:
                        if result is not None:
                            await self.fan_in_queue.put((branch_id, result))

                    input_stream.task_done()

                except asyncio.CancelledError:
                    break
                except Exception as e:
                    logger.error(f"Fan-out branch {branch_id} error: {e}")

        # Start fan-out workers
        fan_out_tasks = [
            asyncio.create_task(fan_out_worker(branch_id))
            for branch_id in range(self.num_branches)
        ]

        # Fan-in: merge results
        async def fan_in_worker():
            """Merge results from all branches."""
            while True:
                try:
                    branch_id, result = await self.fan_in_queue.get()

                    # Apply any final processing (e.g., aggregation)
                    final_result = result  # Could add aggregation logic here

                    await output_stream.put(final_result)
                    self.fan_in_queue.task_done()

                except asyncio.CancelledError:
                    break
                except Exception as e:
                    logger.error(f"Fan-in error: {e}")

        # Start fan-in worker
        fan_in_task = asyncio.create_task(fan_in_worker())

        # Return output stream
        return output_stream

    def get_stats(self) -> Dict[str, Any]:
        """Get pipeline statistics."""
        return {
            'num_branches': self.num_branches,
            'morsel_size_kb': self.morsel_size_kb,
            'branch_stats': {
                i: processor.get_stats()
                for i, processor in enumerate(self.branch_processors)
            }
        }

# Convenience functions for easy usage

def create_dbos_cython_parallel_processor(
    max_workers: Optional[int] = None,
    morsel_size_kb: int = 64,
    target_utilization: float = 0.8
) -> DBOSCythonParallelProcessor:
    """Create a DBOS-controlled Cython parallel processor."""
    return DBOSCythonParallelProcessor(max_workers, morsel_size_kb, target_utilization)

def create_fan_out_fan_in_cython_pipeline(
    processor_func: Callable[[Any], Any],
    num_branches: int = 4,
    morsel_size_kb: int = 64,
    max_workers: Optional[int] = None
) -> FanOutFanInCythonPipeline:
    """Create a fan-out/fan-in pipeline with Cython execution."""
    return FanOutFanInCythonPipeline(
        processor_func, num_branches, morsel_size_kb, max_workers
    )

async def parallel_process_with_dbos(
    data: Any,
    processor_func: Callable[[Any], Any],
    max_workers: Optional[int] = None,
    morsel_size_kb: int = 64
) -> List[Any]:
    """High-level function for DBOS-controlled parallel processing."""
    processor = create_dbos_cython_parallel_processor(max_workers, morsel_size_kb)

    try:
        await processor.start()
        results = await processor.process_data(data, processor_func)
        return results
    finally:
        await processor.stop()

async def parallel_stream_process_with_dbos(
    input_stream: asyncio.Queue,
    processor_func: Callable[[Any], Any],
    max_workers: Optional[int] = None,
    morsel_size_kb: int = 64,
    output_stream: Optional[asyncio.Queue] = None
) -> asyncio.Queue:
    """High-level function for DBOS-controlled parallel stream processing."""
    processor = create_dbos_cython_parallel_processor(max_workers, morsel_size_kb)

    try:
        await processor.start()
        result_stream = await processor.process_stream(
            input_stream, processor_func, output_stream
        )
        return result_stream
    finally:
        await processor.stop()

# Performance monitoring utilities

def get_parallel_performance_summary(processor) -> Dict[str, Any]:
    """Get a performance summary for parallel processing."""
    if hasattr(processor, 'get_stats'):
        stats = processor.get_stats()
    else:
        return {'error': 'Processor does not support stats'}

    summary = {
        'is_running': stats.get('is_running', False),
        'total_processed': 0,
        'processing_time': 0,
        'throughput': 0,
        'worker_count': 0,
        'system_cpu_percent': 0,
        'system_memory_percent': 0,
    }

    # Extract from DBOS controller stats
    dbos_stats = stats.get('dbos_controller', {})
    summary['total_processed'] = dbos_stats.get('total_processed', 0)
    summary['processing_time'] = dbos_stats.get('processing_time_seconds', 0)
    summary['throughput'] = dbos_stats.get('throughput_items_per_second', 0)
    summary['worker_count'] = dbos_stats.get('active_workers', 0)

    # Extract system metrics
    system_stats = stats.get('system_resources', {})
    summary['system_cpu_percent'] = system_stats.get('cpu_percent', 0)
    summary['system_memory_percent'] = system_stats.get('memory_percent', 0)

    return summary
