#!/usr/bin/env python3
"""
Configuration management for Sabot.

Provides centralized configuration for all Sabot components including
telemetry, performance tuning, and operational settings.
"""

import os
import json
from typing import Dict, Any, Optional
from dataclasses import dataclass, field
from pathlib import Path


@dataclass
class TelemetryConfig:
    """OpenTelemetry configuration."""
    enabled: bool = False
    service_name: str = "sabot"
    jaeger_endpoint: Optional[str] = None
    otlp_endpoint: Optional[str] = None
    prometheus_port: int = 8000
    console_tracing: bool = False
    sample_rate: float = 1.0
    custom_attributes: Dict[str, str] = field(default_factory=dict)


@dataclass
class PerformanceConfig:
    """Performance tuning configuration."""
    max_memory_mb: int = 1024
    thread_pool_size: int = 4
    batch_size: int = 1000
    buffer_size: int = 10000
    enable_cython: bool = True
    enable_arrow: bool = True
    enable_gpu: bool = False


@dataclass
class OperationalConfig:
    """Operational configuration."""
    log_level: str = "INFO"
    enable_metrics: bool = True
    enable_health_checks: bool = True
    health_check_interval: int = 30
    database_url: Optional[str] = None
    broker_url: str = "memory://"


@dataclass
class SabotConfig:
    """Main Sabot configuration."""
    telemetry: TelemetryConfig = field(default_factory=TelemetryConfig)
    performance: PerformanceConfig = field(default_factory=PerformanceConfig)
    operational: OperationalConfig = field(default_factory=OperationalConfig)

    def __post_init__(self):
        """Load configuration from environment and files."""
        self._load_from_environment()
        self._load_from_config_files()

    def _load_from_environment(self):
        """Load configuration from environment variables."""
        # Telemetry
        self.telemetry.enabled = os.getenv('SABOT_TELEMETRY_ENABLED', 'false').lower() == 'true'
        self.telemetry.service_name = os.getenv('SABOT_SERVICE_NAME', self.telemetry.service_name)
        self.telemetry.jaeger_endpoint = os.getenv('SABOT_JAEGER_ENDPOINT', self.telemetry.jaeger_endpoint)
        self.telemetry.otlp_endpoint = os.getenv('SABOT_OTLP_ENDPOINT', self.telemetry.otlp_endpoint)
        self.telemetry.prometheus_port = int(os.getenv('SABOT_PROMETHEUS_PORT', self.telemetry.prometheus_port))
        self.telemetry.console_tracing = os.getenv('SABOT_CONSOLE_TRACING', 'false').lower() == 'true'
        self.telemetry.sample_rate = float(os.getenv('SABOT_SAMPLE_RATE', self.telemetry.sample_rate))

        # Performance
        self.performance.max_memory_mb = int(os.getenv('SABOT_MAX_MEMORY_MB', self.performance.max_memory_mb))
        self.performance.thread_pool_size = int(os.getenv('SABOT_THREAD_POOL_SIZE', self.performance.thread_pool_size))
        self.performance.batch_size = int(os.getenv('SABOT_BATCH_SIZE', self.performance.batch_size))
        self.performance.buffer_size = int(os.getenv('SABOT_BUFFER_SIZE', self.performance.buffer_size))
        self.performance.enable_cython = os.getenv('SABOT_ENABLE_CYTHON', 'true').lower() == 'true'
        self.performance.enable_arrow = os.getenv('SABOT_ENABLE_ARROW', 'true').lower() == 'true'
        self.performance.enable_gpu = os.getenv('SABOT_ENABLE_GPU', 'false').lower() == 'true'

        # Operational
        self.operational.log_level = os.getenv('SABOT_LOG_LEVEL', self.operational.log_level)
        self.operational.enable_metrics = os.getenv('SABOT_ENABLE_METRICS', 'true').lower() == 'true'
        self.operational.enable_health_checks = os.getenv('SABOT_ENABLE_HEALTH_CHECKS', 'true').lower() == 'true'
        self.operational.health_check_interval = int(os.getenv('SABOT_HEALTH_CHECK_INTERVAL', self.operational.health_check_interval))
        self.operational.database_url = os.getenv('SABOT_DATABASE_URL', self.operational.database_url)
        self.operational.broker_url = os.getenv('SABOT_BROKER_URL', self.operational.broker_url)

    def _load_from_config_files(self):
        """Load configuration from config files."""
        config_paths = [
            Path.home() / '.sabot' / 'config.json',
            Path.cwd() / 'sabot.json',
            Path.cwd() / 'sabot-config.json',
            Path(os.getenv('SABOT_CONFIG_FILE', ''))
        ]

        for config_path in config_paths:
            if config_path and config_path.exists():
                try:
                    with open(config_path, 'r') as f:
                        config_data = json.load(f)

                    # Update configuration from file
                    self._update_from_dict(config_data)
                    break  # Use first found config file
                except Exception as e:
                    print(f"Warning: Failed to load config from {config_path}: {e}")

    def _update_from_dict(self, data: Dict[str, Any]):
        """Update configuration from dictionary."""
        # Update telemetry
        if 'telemetry' in data:
            telemetry_data = data['telemetry']
            for key, value in telemetry_data.items():
                if hasattr(self.telemetry, key):
                    setattr(self.telemetry, key, value)

        # Update performance
        if 'performance' in data:
            perf_data = data['performance']
            for key, value in perf_data.items():
                if hasattr(self.performance, key):
                    setattr(self.performance, key, value)

        # Update operational
        if 'operational' in data:
            op_data = data['operational']
            for key, value in op_data.items():
                if hasattr(self.operational, key):
                    setattr(self.operational, key, value)

    def save_to_file(self, path: Path):
        """Save configuration to file."""
        config_dict = {
            'telemetry': {
                'enabled': self.telemetry.enabled,
                'service_name': self.telemetry.service_name,
                'jaeger_endpoint': self.telemetry.jaeger_endpoint,
                'otlp_endpoint': self.telemetry.otlp_endpoint,
                'prometheus_port': self.telemetry.prometheus_port,
                'console_tracing': self.telemetry.console_tracing,
                'sample_rate': self.telemetry.sample_rate,
                'custom_attributes': self.telemetry.custom_attributes
            },
            'performance': {
                'max_memory_mb': self.performance.max_memory_mb,
                'thread_pool_size': self.performance.thread_pool_size,
                'batch_size': self.performance.batch_size,
                'buffer_size': self.performance.buffer_size,
                'enable_cython': self.performance.enable_cython,
                'enable_arrow': self.performance.enable_arrow,
                'enable_gpu': self.performance.enable_gpu
            },
            'operational': {
                'log_level': self.operational.log_level,
                'enable_metrics': self.operational.enable_metrics,
                'enable_health_checks': self.operational.enable_health_checks,
                'health_check_interval': self.operational.health_check_interval,
                'database_url': self.operational.database_url,
                'broker_url': self.operational.broker_url
            }
        }

        path.parent.mkdir(parents=True, exist_ok=True)
        with open(path, 'w') as f:
            json.dump(config_dict, f, indent=2)

    def to_dict(self) -> Dict[str, Any]:
        """Convert configuration to dictionary."""
        return {
            'telemetry': {
                'enabled': self.telemetry.enabled,
                'service_name': self.telemetry.service_name,
                'jaeger_endpoint': self.telemetry.jaeger_endpoint,
                'otlp_endpoint': self.telemetry.otlp_endpoint,
                'prometheus_port': self.telemetry.prometheus_port,
                'console_tracing': self.telemetry.console_tracing,
                'sample_rate': self.telemetry.sample_rate
            },
            'performance': {
                'max_memory_mb': self.performance.max_memory_mb,
                'thread_pool_size': self.performance.thread_pool_size,
                'batch_size': self.performance.batch_size,
                'buffer_size': self.performance.buffer_size,
                'enable_cython': self.performance.enable_cython,
                'enable_arrow': self.performance.enable_arrow,
                'enable_gpu': self.performance.enable_gpu
            },
            'operational': {
                'log_level': self.operational.log_level,
                'enable_metrics': self.operational.enable_metrics,
                'enable_health_checks': self.operational.enable_health_checks,
                'health_check_interval': self.operational.health_check_interval,
                'database_url': self.operational.database_url,
                'broker_url': self.operational.broker_url
            }
        }


# Global configuration instance
_config: Optional[SabotConfig] = None

def get_config() -> SabotConfig:
    """Get the global configuration instance."""
    global _config
    if _config is None:
        _config = SabotConfig()
    return _config

def init_config(config_file: Optional[str] = None) -> SabotConfig:
    """Initialize global configuration."""
    global _config
    _config = SabotConfig()

    if config_file:
        config_path = Path(config_file)
        if config_path.exists():
            try:
                with open(config_path, 'r') as f:
                    config_data = json.load(f)
                _config._update_from_dict(config_data)
            except Exception as e:
                print(f"Warning: Failed to load config from {config_file}: {e}")

    return _config

def save_config(path: Path):
    """Save current configuration to file."""
    config = get_config()
    config.save_to_file(path)
