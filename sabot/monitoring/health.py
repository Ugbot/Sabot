#!/usr/bin/env python3
"""
Health Check System for Sabot

Provides comprehensive health monitoring with:
- Component health checks
- Dependency validation
- Performance thresholds
- Automated health assessment
- Integration with monitoring dashboard
"""

import asyncio
import time
import logging
from typing import Dict, List, Any, Optional, Callable, Awaitable
from dataclasses import dataclass, field
from enum import Enum

logger = logging.getLogger(__name__)


class HealthStatus(Enum):
    """Health status levels."""
    HEALTHY = "healthy"
    DEGRADED = "degraded"
    UNHEALTHY = "unhealthy"
    UNKNOWN = "unknown"


@dataclass
class HealthCheck:
    """Individual health check definition."""
    name: str
    check_function: Callable[[], Awaitable[Dict[str, Any]]]
    interval_seconds: float = 30.0
    timeout_seconds: float = 10.0
    critical: bool = False  # If True, failure marks system as unhealthy
    tags: List[str] = field(default_factory=list)


@dataclass
class HealthResult:
    """Result of a health check."""
    name: str
    status: HealthStatus
    timestamp: float
    duration: float
    details: Dict[str, Any] = field(default_factory=dict)
    error_message: Optional[str] = None


class HealthChecker:
    """
    Comprehensive health checker for Sabot applications.

    Monitors system components and dependencies to ensure operational health.
    """

    def __init__(self):
        self.checks: Dict[str, HealthCheck] = {}
        self.results: Dict[str, HealthResult] = {}
        self._running = False
        self._check_task: Optional[asyncio.Task] = None

        # Initialize built-in health checks
        self._initialize_builtin_checks()

    def _initialize_builtin_checks(self):
        """Initialize built-in health checks."""
        # System resource checks
        self.add_check(HealthCheck(
            name="memory_usage",
            check_function=self._check_memory_usage,
            interval_seconds=60.0,
            critical=True
        ))

        self.add_check(HealthCheck(
            name="disk_space",
            check_function=self._check_disk_space,
            interval_seconds=300.0,  # 5 minutes
            critical=True
        ))

        # Process checks
        self.add_check(HealthCheck(
            name="process_health",
            check_function=self._check_process_health,
            interval_seconds=30.0,
            critical=True
        ))

    def add_check(self, check: HealthCheck) -> None:
        """Add a health check."""
        self.checks[check.name] = check

    def remove_check(self, name: str) -> None:
        """Remove a health check."""
        if name in self.checks:
            del self.checks[name]
        if name in self.results:
            del self.results[name]

    async def run_check(self, name: str) -> Optional[HealthResult]:
        """Run a specific health check."""
        if name not in self.checks:
            return None

        check = self.checks[name]
        start_time = time.time()

        try:
            # Run the check with timeout
            result = await asyncio.wait_for(
                check.check_function(),
                timeout=check.timeout_seconds
            )

            duration = time.time() - start_time

            # Determine status from result
            status = result.get('status', 'unknown')
            if isinstance(status, str):
                status = HealthStatus(status.lower())
            elif isinstance(status, bool):
                status = HealthStatus.HEALTHY if status else HealthStatus.UNHEALTHY

            health_result = HealthResult(
                name=name,
                status=status,
                timestamp=start_time,
                duration=duration,
                details=result
            )

        except asyncio.TimeoutError:
            duration = time.time() - start_time
            health_result = HealthResult(
                name=name,
                status=HealthStatus.UNHEALTHY,
                timestamp=start_time,
                duration=duration,
                error_message=f"Check timed out after {check.timeout_seconds}s"
            )

        except Exception as e:
            duration = time.time() - start_time
            health_result = HealthResult(
                name=name,
                status=HealthStatus.UNHEALTHY,
                timestamp=start_time,
                duration=duration,
                error_message=str(e)
            )

        self.results[name] = health_result
        return health_result

    async def run_all_checks(self) -> Dict[str, HealthResult]:
        """Run all health checks."""
        tasks = [self.run_check(name) for name in self.checks.keys()]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        check_results = {}
        for name, result in zip(self.checks.keys(), results):
            if isinstance(result, Exception):
                # Handle exceptions in check execution
                check_results[name] = HealthResult(
                    name=name,
                    status=HealthStatus.UNHEALTHY,
                    timestamp=time.time(),
                    duration=0.0,
                    error_message=str(result)
                )
            else:
                check_results[name] = result

        return check_results

    def get_overall_health(self) -> Dict[str, Any]:
        """Get overall system health status."""
        if not self.results:
            return {
                'status': HealthStatus.UNKNOWN.value,
                'message': 'No health checks have been run'
            }

        # Count statuses
        status_counts = {}
        critical_failures = []
        degraded_components = []

        for result in self.results.values():
            status_str = result.status.value
            status_counts[status_str] = status_counts.get(status_str, 0) + 1

            # Check for critical failures
            if (result.status == HealthStatus.UNHEALTHY and
                self.checks[result.name].critical):
                critical_failures.append(result.name)

            # Check for degraded components
            if result.status == HealthStatus.DEGRADED:
                degraded_components.append(result.name)

        # Determine overall status
        if critical_failures:
            overall_status = HealthStatus.UNHEALTHY
            message = f"Critical components unhealthy: {', '.join(critical_failures)}"
        elif status_counts.get(HealthStatus.UNHEALTHY.value, 0) > 0:
            overall_status = HealthStatus.DEGRADED
            message = f"Some components unhealthy: {status_counts.get(HealthStatus.UNHEALTHY.value, 0)}"
        elif status_counts.get(HealthStatus.DEGRADED.value, 0) > 0:
            overall_status = HealthStatus.DEGRADED
            message = f"Some components degraded: {', '.join(degraded_components)}"
        else:
            overall_status = HealthStatus.HEALTHY
            message = "All components healthy"

        return {
            'status': overall_status.value,
            'message': message,
            'timestamp': time.time(),
            'checks': {
                name: {
                    'status': result.status.value,
                    'timestamp': result.timestamp,
                    'duration': result.duration,
                    'error_message': result.error_message
                }
                for name, result in self.results.items()
            },
            'summary': status_counts
        }

    def start_monitoring(self) -> None:
        """Start continuous health monitoring."""
        if self._running:
            return

        self._running = True
        self._check_task = asyncio.create_task(self._monitoring_loop())
        logger.info("Health checker monitoring started")

    def stop_monitoring(self) -> None:
        """Stop continuous health monitoring."""
        self._running = False
        if self._check_task:
            self._check_task.cancel()
        logger.info("Health checker monitoring stopped")

    async def _monitoring_loop(self) -> None:
        """Continuous monitoring loop."""
        while self._running:
            try:
                # Run all checks
                await self.run_all_checks()

                # Calculate next check time (run checks at their individual intervals)
                if self.checks:
                    next_check = min(check.interval_seconds for check in self.checks.values())
                    await asyncio.sleep(next_check)
                else:
                    await asyncio.sleep(30.0)  # Default interval

            except Exception as e:
                logger.error(f"Error in health monitoring loop: {e}")
                await asyncio.sleep(10.0)  # Brief pause before retrying

    # Built-in health check implementations

    async def _check_memory_usage(self) -> Dict[str, Any]:
        """Check system memory usage."""
        try:
            import psutil
            memory = psutil.virtual_memory()

            usage_percent = memory.percent
            available_gb = memory.available / (1024**3)

            if usage_percent > 90:
                status = HealthStatus.UNHEALTHY
            elif usage_percent > 80:
                status = HealthStatus.DEGRADED
            else:
                status = HealthStatus.HEALTHY

            return {
                'status': status.value,
                'usage_percent': usage_percent,
                'available_gb': available_gb,
                'total_gb': memory.total / (1024**3)
            }

        except Exception as e:
            return {
                'status': HealthStatus.UNKNOWN.value,
                'error': str(e)
            }

    async def _check_disk_space(self) -> Dict[str, Any]:
        """Check disk space availability."""
        try:
            import psutil
            disk = psutil.disk_usage('/')

            free_percent = 100 - disk.percent
            free_gb = disk.free / (1024**3)

            if free_percent < 5:  # Less than 5% free
                status = HealthStatus.UNHEALTHY
            elif free_percent < 10:  # Less than 10% free
                status = HealthStatus.DEGRADED
            else:
                status = HealthStatus.HEALTHY

            return {
                'status': status.value,
                'free_percent': free_percent,
                'free_gb': free_gb,
                'total_gb': disk.total / (1024**3)
            }

        except Exception as e:
            return {
                'status': HealthStatus.UNKNOWN.value,
                'error': str(e)
            }

    async def _check_process_health(self) -> Dict[str, Any]:
        """Check process health."""
        try:
            import psutil
            process = psutil.Process()

            # Check if process is running
            if not process.is_running():
                return {
                    'status': HealthStatus.UNHEALTHY.value,
                    'error': 'Process is not running'
                }

            # Check CPU usage (should not be constantly high)
            cpu_percent = process.cpu_percent(interval=1.0)

            # Check memory usage
            memory_info = process.memory_info()
            memory_mb = memory_info.rss / (1024**2)

            # Basic health criteria
            if cpu_percent > 95:  # Consistently high CPU
                status = HealthStatus.DEGRADED
            elif memory_mb > 1000:  # Using more than 1GB (arbitrary threshold)
                status = HealthStatus.DEGRADED
            else:
                status = HealthStatus.HEALTHY

            return {
                'status': status.value,
                'cpu_percent': cpu_percent,
                'memory_mb': memory_mb,
                'pid': process.pid,
                'threads': process.num_threads()
            }

        except Exception as e:
            return {
                'status': HealthStatus.UNHEALTHY.value,
                'error': str(e)
            }

    # Component-specific health checks

    async def check_stream_engine(self, stream_engine) -> Dict[str, Any]:
        """Check stream engine health."""
        try:
            health = await stream_engine.health_check()

            # Convert health status to our enum
            status_map = {
                'healthy': HealthStatus.HEALTHY,
                'degraded': HealthStatus.DEGRADED,
                'error': HealthStatus.UNHEALTHY
            }

            status = status_map.get(health.get('status', 'unknown'), HealthStatus.UNKNOWN)

            return {
                'status': status.value,
                'active_streams': health.get('active_streams', 0),
                'processing_tasks': health.get('processing_tasks', 0),
                'messages_processed': health.get('total_messages_processed', 0),
                'errors': health.get('total_errors', 0)
            }

        except Exception as e:
            return {
                'status': HealthStatus.UNHEALTHY.value,
                'error': str(e)
            }

    async def check_state_manager(self, state_manager) -> Dict[str, Any]:
        """Check state manager health."""
        try:
            health = await state_manager.health_check()

            status_map = {
                'healthy': HealthStatus.HEALTHY,
                'degraded': HealthStatus.DEGRADED,
                'error': HealthStatus.UNHEALTHY
            }

            status = status_map.get(health.get('overall_status', 'unknown'), HealthStatus.UNKNOWN)

            return {
                'status': status.value,
                'tables': health.get('tables', {}),
                'backends': health.get('backends', {})
            }

        except Exception as e:
            return {
                'status': HealthStatus.UNHEALTHY.value,
                'error': str(e)
            }

    async def check_agent_manager(self, agent_manager) -> Dict[str, Any]:
        """Check agent manager health."""
        try:
            # Basic agent manager health check
            stats = agent_manager.get_manager_stats()

            active_agents = stats.get('active_agents', 0)
            failed_agents = stats.get('failed_agents', 0)

            if failed_agents > 0:
                status = HealthStatus.DEGRADED
            elif active_agents == 0:
                status = HealthStatus.UNHEALTHY
            else:
                status = HealthStatus.HEALTHY

            return {
                'status': status.value,
                'active_agents': active_agents,
                'failed_agents': failed_agents,
                'restarts': stats.get('total_restarts', 0)
            }

        except Exception as e:
            return {
                'status': HealthStatus.UNHEALTHY.value,
                'error': str(e)
            }
